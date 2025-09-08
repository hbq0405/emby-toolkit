# actor_sync_handler.py (V2 - 按媒体库筛选版)

from typing import Optional, List, Dict, Any
import threading
import time
import concurrent.futures
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
        self.config = config
        
        logger.trace(f"UnifiedSyncHandler (终极兼容版) 初始化完成。")

    def _get_persons_from_selected_libraries(self, update_status_callback, stop_event) -> Optional[List[Dict[str, Any]]]:
        """【辅助函数】尝试以高效、可靠的方式从选定媒体库提取演员。如果失败则返回 None。"""
        # 阶段一：获取ID
        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids: return []
        movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, fields="Id") or []
        series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, fields="Id") or []
        all_media_items_ids = [item['Id'] for item in (movies + series) if item.get('Id')]
        if not all_media_items_ids: return []

        # 阶段二：并发提取演员ID
        total_items = len(all_media_items_ids)
        if update_status_callback: update_status_callback(15, f"阶段 2/5: 准备从 {total_items} 个项目中提取演员...")
        actor_ids_to_sync = set()
        processed_count = 0
        lock = threading.Lock()

        def fetch_and_extract(item_id):
            nonlocal processed_count
            try:
                if stop_event and stop_event.is_set(): return
                details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields="People")
                if details and details.get("People"):
                    with lock:
                        for p in details["People"]:
                            if p.get("Type") == "Actor" and p.get("Id"): actor_ids_to_sync.add(p.get("Id"))
            finally:
                with lock:
                    processed_count += 1
                    if processed_count % 20 == 0 or processed_count == total_items:
                        progress = 15 + int((processed_count / total_items) * 45)
                        if update_status_callback: update_status_callback(progress, f"阶段 2/5: 已处理 {processed_count}/{total_items} 个项目")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(fetch_and_extract, all_media_items_ids)) # 使用 map 简化
        
        if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")

        # 阶段三：并发获取演员详情
        total_actors_found = len(actor_ids_to_sync)
        logger.info(f"  -> 按库筛选模式：共提取出 {total_actors_found} 位独立演员。")
        if update_status_callback: update_status_callback(60, f"阶段 3/5: 获取 {total_actors_found} 位演员详情...")
        
        final_persons_list = []
        if total_actors_found > 0:
            person_generator = emby_handler.get_persons_by_ids_in_batches(
                base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id,
                person_ids=list(actor_ids_to_sync), stop_event=stop_event
            )
            for batch in person_generator:
                final_persons_list.extend(batch)
        
        return final_persons_list

    def sync_emby_person_map_to_db(self, update_status_callback: Optional[callable] = None, stop_event: Optional[threading.Event] = None):
        logger.info("--- 开始执行'同步演员映射表 (终极兼容版)'任务 ---")
        final_persons_list = []
        
        try:
            # ======================================================================
            # 步骤 1: 优先尝试高效的“按库筛选”模式
            # ======================================================================
            if update_status_callback: update_status_callback(5, "阶段 1/5: 尝试按媒体库筛选模式...")
            final_persons_list = self._get_persons_from_selected_libraries(update_status_callback, stop_event)

            # ======================================================================
            # ★★★ 核心修改：兼容性回退逻辑 ★★★
            # ======================================================================
            if final_persons_list is not None and len(final_persons_list) == 0:
                logger.warning("按库筛选模式未提取到任何演员，这可能由 Emby Beta 版的 API 限制导致。")
                logger.warning("将自动回退到全局同步模式以确保数据安全...")
                if update_status_callback: update_status_callback(70, "兼容性回退：切换到全局同步模式...")
                
                final_persons_list = [] # 重置列表
                person_generator = emby_handler.get_all_persons_from_emby(self.emby_url, self.emby_api_key, self.emby_user_id, stop_event)
                for person_batch in person_generator:
                    if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                    final_persons_list.extend(person_batch)
                logger.info(f"  -> 全局同步模式：共获取到 {len(final_persons_list)} 个演员条目。")

            # ======================================================================
            # 步骤 2: 写入数据库 (逻辑不变)
            # ======================================================================
            stats = { "total": len(final_persons_list), "processed": 0, "inserted": 0, "updated": 0, "unchanged": 0, "skipped": 0, "errors": 0, "deleted": 0 }
            if update_status_callback: update_status_callback(85, "阶段 4/5: 同步数据到数据库...")
            
            all_emby_pids_from_sync = {str(p.get("Id", "")).strip() for p in final_persons_list if p.get("Id")}

            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                for person_emby in final_persons_list:
                    # ... (upsert 逻辑完全不变) ...
                    stats["processed"] += 1
                    emby_pid = str(person_emby.get("Id", "")).strip()
                    person_name = str(person_emby.get("Name", "")).strip()
                    if not emby_pid or not person_name:
                        stats["skipped"] += 1
                        continue
                    provider_ids = person_emby.get("ProviderIds", {})
                    person_data_for_db = { "emby_id": emby_pid, "name": person_name, "tmdb_id": provider_ids.get("Tmdb"), "imdb_id": provider_ids.get("Imdb"), "douban_id": provider_ids.get("Douban") }
                    try:
                        _, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db, emby_config=emby_config_for_upsert)
                        if status in stats: stats[status.lower()] += 1
                    except Exception: stats['errors'] += 1
                conn.commit()

                # ======================================================================
                # 步骤 3: 清理前的终极安全检查 (熔断机制)
                # ======================================================================
                if update_status_callback: update_status_callback(98, "阶段 5/5: 执行安全检查并清理...")
                
                if not all_emby_pids_from_sync:
                    pids_in_db_before_delete = get_all_emby_person_ids_from_map()
                    db_count = len(pids_in_db_before_delete)
                    SAFETY_THRESHOLD = 100
                    if db_count > SAFETY_THRESHOLD:
                        error_message = f"终极安全检查失败：准备清空数据库，但数据库中仍有 {db_count} 条记录。清理操作已强制中止！"
                        logger.error(error_message)
                        raise RuntimeError(error_message)

                # --- 清理阶段 ---
                pids_in_db = get_all_emby_person_ids_from_map()
                pids_to_delete = list(pids_in_db - all_emby_pids_from_sync)
                if pids_to_delete:
                    stats['deleted'] = delete_persons_by_emby_ids(pids_to_delete)

        except (InterruptedError, RuntimeError) as e:
            logger.warning(f"任务安全中止: {e}")
            if update_status_callback: update_status_callback(-1, f"任务已中止: {e}")
            return
        except Exception as e_main:
            logger.error(f"演员同步主流程发生严重错误: {e_main}", exc_info=True)
            if update_status_callback: update_status_callback(-1, "同步失败，发生未知错误")
            return

        # ... (统计日志输出) ...
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