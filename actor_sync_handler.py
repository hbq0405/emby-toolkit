# actor_sync_handler.py (V2 - 按媒体库筛选版)

from typing import Optional, List, Dict, Any
import threading
import emby_handler
import logging
from db_handler import get_db_connection as get_central_db_connection, get_all_emby_person_ids_from_map, delete_persons_by_emby_ids
from db_handler import ActorDBManager

logger = logging.getLogger(__name__)

class UnifiedSyncHandler:
    def __init__(self, emby_url: str, emby_api_key: str, emby_user_id: Optional[str], tmdb_api_key: str, config: Dict[str, Any]):
        self.actor_db_manager = ActorDBManager()
        self.emby_url = emby_url
        self.emby_api_key = emby_api_key
        self.emby_user_id = emby_user_id
        self.tmdb_api_key = tmdb_api_key
        self.config = config  # ★★★ 存储完整的 config，用于获取媒体库列表 ★★★
        
        logger.trace(f"UnifiedSyncHandler (按媒体库筛选版) 初始化完成。")

    def sync_emby_person_map_to_db(self, update_status_callback: Optional[callable] = None, stop_event: Optional[threading.Event] = None):
        """
        【V6 - 按媒体库筛选最终版】
        重构演员同步逻辑，只处理在用户选定媒体库中出现过的演员，并保留所有安全机制。
        """
        logger.info("--- 开始执行'同步演员映射表 (按媒体库筛选)'任务 ---")
        if update_status_callback: update_status_callback(0, "阶段 1/4: 获取选定媒体库项目...")

        try:
            # ======================================================================
            # 阶段一：获取用户选定的媒体库中的所有电影和剧集
            # ======================================================================
            libs_to_process_ids = self.config.get("libraries_to_process", [])
            if not libs_to_process_ids:
                logger.warning("未在配置中指定要处理的媒体库，任务中止。")
                if update_status_callback: update_status_callback(100, "未配置媒体库")
                return

            # ★★★ 核心修改：获取媒体项目时，必须请求 'People' 字段！ ★★★
            movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, fields="People") or []
            series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, fields="People") or []
            all_media_items = movies + series

            if not all_media_items:
                logger.info("在选定的媒体库中未找到任何电影或剧集，任务完成。")
                if update_status_callback: update_status_callback(100, "媒体库为空")
                return

            # ======================================================================
            # 阶段二：从媒体项目中提取所有不重复的演员ID
            # ======================================================================
            if update_status_callback: update_status_callback(25, f"从 {len(all_media_items)} 个项目中提取演员...")

            actor_ids_to_sync = set()
            for item in all_media_items:
                if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                for person in item.get("People", []):
                    if person.get("Type") == "Actor" and person.get("Id"):
                        actor_ids_to_sync.add(person.get("Id"))
            
            total_actors_found = len(actor_ids_to_sync)
            logger.info(f"  -> 从选定媒体库中，共提取出 {total_actors_found} 位独立演员。")

            # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
            # ★★★ 核心修改：将安全检查 (熔断机制) 移植到这里 ★★★
            # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
            if total_actors_found == 0:
                logger.warning("从选定媒体库中获取到 0 个演员条目，正在执行安全检查...")
                try:
                    pids_in_db = get_all_emby_person_ids_from_map()
                    db_count = len(pids_in_db)
                    SAFETY_THRESHOLD = 100 
                    
                    if db_count > SAFETY_THRESHOLD:
                        error_message = f"安全中止：从媒体库提取到 0 个演员，但数据库中存在 {db_count} 条记录。这可能是Emby连接问题或媒体库刮削不完整。为防止数据误删，同步任务已中止。"
                        logger.error(error_message)
                        if update_status_callback: update_status_callback(-1, "安全中止：无法获取演员")
                        return
                    else:
                        logger.info(f"数据库中记录数 ({db_count}) 低于安全阈值，将按预期继续执行清理。")
                except Exception as e_check:
                    logger.error(f"执行安全检查时发生数据库错误: {e_check}", exc_info=True)
                    if update_status_callback: update_status_callback(-1, "安全检查失败")
                    return

            # ======================================================================
            # 阶段三：使用新工具，分批获取这些演员的详情
            # ======================================================================
            if update_status_callback: update_status_callback(50, f"阶段 2/4: 获取 {total_actors_found} 位演员详情...")

            filtered_persons_from_emby = []
            if total_actors_found > 0:
                person_generator = emby_handler.get_persons_by_ids_in_batches(
                    base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id,
                    person_ids=list(actor_ids_to_sync), stop_event=stop_event
                )
                for person_batch in person_generator:
                    if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                    filtered_persons_from_emby.extend(person_batch)
                    
                    progress = 50 + int((len(filtered_persons_from_emby) / total_actors_found) * 25)
                    if update_status_callback: update_status_callback(progress, f"已获取 {len(filtered_persons_from_emby)}/{total_actors_found} 位演员详情")

            # ======================================================================
            # 阶段四：处理与写入数据库 (复用您原有的健壮逻辑)
            # ======================================================================
            stats = { "total": len(filtered_persons_from_emby), "processed": 0, "inserted": 0, "updated": 0, "unchanged": 0, "skipped": 0, "errors": 0, "deleted": 0 }
            if update_status_callback: update_status_callback(75, "阶段 3/4: 同步数据到数据库...")
            
            all_emby_pids_from_sync = {str(p.get("Id", "")).strip() for p in filtered_persons_from_emby if p.get("Id")}

            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}

                # --- Upsert 阶段 ---
                for person_emby in filtered_persons_from_emby:
                    if stop_event and stop_event.is_set(): raise InterruptedError("任务在写入阶段被中止")
                    # ... (这部分 upsert 逻辑与您原版完全相同，直接复用) ...
                    stats["processed"] += 1
                    emby_pid = str(person_emby.get("Id", "")).strip()
                    person_name = str(person_emby.get("Name", "")).strip()

                    if not emby_pid or not person_name:
                        stats["skipped"] += 1
                        continue
                    
                    provider_ids = person_emby.get("ProviderIds", {})
                    person_data_for_db = {
                        "emby_id": emby_pid, "name": person_name,
                        "tmdb_id": provider_ids.get("Tmdb"),
                        "imdb_id": provider_ids.get("Imdb"),
                        "douban_id": provider_ids.get("Douban"),
                    }
                    
                    try:
                        map_id, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db, emby_config=emby_config_for_upsert)
                        if status == "INSERTED": stats['inserted'] += 1
                        elif status == "UPDATED": stats['updated'] += 1
                        elif status == "UNCHANGED": stats['unchanged'] += 1
                        elif status == "SKIPPED": stats['skipped'] += 1
                        else: stats['errors'] += 1
                    except Exception as e_upsert:
                        logger.error(f"同步时写入数据库失败 for EmbyPID {emby_pid}: {e_upsert}")
                        stats['errors'] += 1

                conn.commit()

                # --- 清理阶段 ---
                if update_status_callback: update_status_callback(98, "阶段 4/4: 对比数据进行清理...")
                pids_in_db = get_all_emby_person_ids_from_map()
                pids_to_delete = list(pids_in_db - all_emby_pids_from_sync)

                if pids_to_delete:
                    logger.warning(f"  -> 发现 {len(pids_to_delete)} 条失效记录需要删除 (这些演员已不在您选定的媒体库中)。")
                    deleted_count = delete_persons_by_emby_ids(pids_to_delete)
                    stats['deleted'] = deleted_count
                else:
                    logger.info("  -> 数据库与选定媒体库的演员数据一致，无需清理。")

        except InterruptedError as e:
            logger.warning(str(e))
            if 'conn' in locals() and conn and not conn.closed: conn.rollback()
            if update_status_callback: update_status_callback(-1, "任务已中止")
            return
        except Exception as e_main:
            logger.error(f"演员同步主流程发生严重错误: {e_main}", exc_info=True)
            if 'conn' in locals() and conn and not conn.closed: conn.rollback()
            if update_status_callback: update_status_callback(-1, "数据库操作失败")
            return

        # ... (最终的统计日志输出，保持不变) ...
        total_changed = stats['inserted'] + stats['updated']
        total_failed = stats['skipped'] + stats['errors']

        logger.info("--- 同步演员映射完成 ---")
        logger.info(f"📊 媒体库演员总数: {stats['total']} 条")
        logger.info(f"⚙️ 已处理: {stats['processed']} 条")
        logger.info(f"✅ 成功写入/更新: {total_changed} 条 (新增: {stats['inserted']}, 更新: {stats['updated']})")
        logger.info(f"➖ 无需变动: {stats['unchanged']} 条")
        logger.info(f"🗑️ 清理失效数据: {stats['deleted']} 条")
        if total_failed > 0:
            logger.warning(f"⚠️ 跳过或错误: {total_failed} 条 (跳过: {stats['skipped']}, 错误: {stats['errors']})")
        logger.info("----------------------")

        if update_status_callback:
            final_message = f"同步完成！新增 {stats['inserted']}，更新 {stats['updated']}，清理 {stats['deleted']}。"
            update_status_callback(100, final_message)