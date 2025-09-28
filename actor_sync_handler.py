# actor_sync_handler.py (最终版)

from typing import Optional, Callable
import threading
# 导入必要的模块
import emby_handler
from database.actor_db import ActorDBManager
from database import connection, actor_db
import logging
logger = logging.getLogger(__name__)

class UnifiedSyncHandler:
    def __init__(self, emby_url: str, emby_api_key: str, emby_user_id: Optional[str], tmdb_api_key: str):
        self.actor_db_manager = ActorDBManager()
        self.emby_url = emby_url
        self.emby_api_key = emby_api_key
        self.emby_user_id = emby_user_id
        self.tmdb_api_key = tmdb_api_key
        logger.trace(f"UnifiedSyncHandler 初始化完成。")
        
    def sync_emby_person_map_to_db(self, update_status_callback: Optional[Callable] = None, stop_event: Optional[threading.Event] = None):
        """
        【V2 - 流式处理 & 进度反馈版】
        """
        logger.info("--- 开始执行演员数据单向同步任务 (Emby -> 本地数据库) ---")
        
        stats = { "total_from_emby": 0, "processed": 0, "db_inserted": 0, "db_updated": 0, 
                  "unchanged": 0, "skipped": 0, "errors": 0, "deleted": 0 }

        try:
            # --- 阶段一：流式处理 Emby 数据并同步到数据库 ---
            if update_status_callback: update_status_callback(0, "阶段 1/2: 从 Emby 扫描并同步演员...")
            
            all_emby_pids_from_sync = set()
            
            person_generator = emby_handler.get_all_persons_from_emby(
                self.emby_url, self.emby_api_key, self.emby_user_id, stop_event,
                update_status_callback=update_status_callback # 传递回调
            )
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                    
                    for person_batch in person_generator:
                        if stop_event and stop_event.is_set(): raise InterruptedError("任务在处理批次时被中止")
                        
                        for person_emby in person_batch:
                            if stop_event and stop_event.is_set(): raise InterruptedError("任务在处理批次时被中止")
                            
                            stats["total_from_emby"] += 1
                            emby_pid = str(person_emby.get("Id", "")).strip()
                            
                            # 从 Emby 获取的是原始英文名
                            original_name = str(person_emby.get("Name", "")).strip()
                            
                            if not emby_pid or not original_name:
                                stats["skipped"] += 1
                                continue
                            
                            # --- ★★★ 核心逻辑重构：预先确定最终姓名 ★★★ ---
                            final_name = original_name
                            is_chinese = False

                            # 尝试从翻译缓存中获取中文名
                            translated_info = self.actor_db_manager.get_translation_from_db(cursor, original_name)
                            if translated_info and translated_info.get('translated_text'):
                                chinese_name = translated_info['translated_text']
                                # 确认中文名有效且与原名不同
                                if chinese_name != original_name:
                                    final_name = chinese_name # 优先使用中文名
                                    is_chinese = True
                                    logger.trace(f"演员 '{original_name}' 已找到中文翻译 '{final_name}'，将用于更新。")

                            # --- 准备提交到数据库的数据 ---
                            provider_ids = person_emby.get("ProviderIds", {})
                            tmdb_id = provider_ids.get("Tmdb")
                            person_data_for_db = {
                                "emby_id": emby_pid,
                                "name": final_name,  # ★ 使用我们最终确定的名字
                                "tmdb_id": tmdb_id,
                                "imdb_id": provider_ids.get("Imdb"),
                                "douban_id": provider_ids.get("Douban"),
                            }
                            
                            try:
                                # --- 步骤 1: 更新 person_identity_map ---
                                # 现在 upsert_person 会正确更新名字了
                                _, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db, emby_config=emby_config_for_upsert)
                                if status == "INSERTED": stats['db_inserted'] += 1
                                elif status == "UPDATED": stats['db_updated'] += 1
                                elif status == "UNCHANGED": stats['unchanged'] += 1
                                elif status == "SKIPPED": stats['skipped'] += 1

                                # --- 步骤 2: 如果名字是中文，则同步更新 media_metadata ---
                                # 只有在名字是中文且数据库操作成功时才执行
                                if is_chinese and tmdb_id and status in ("INSERTED", "UPDATED", "UNCHANGED"):
                                    self.actor_db_manager.update_actor_name_in_media_metadata(cursor, tmdb_id, final_name)

                            except Exception as e_upsert:
                                stats['errors'] += 1
                                logger.error(f"处理演员 {original_name} (ID: {emby_pid}) 的 upsert 时失败: {e_upsert}")
                conn.commit()

            # --- 阶段二：清理本地数据库中多余的条目 ---
            if update_status_callback: update_status_callback(90, "阶段 2/2: 正在清理本地陈旧数据...")
            
            pids_in_db_before_sync = actor_db.get_all_emby_person_ids_from_map()
            pids_to_delete = list(pids_in_db_before_sync - all_emby_pids_from_sync)
            if pids_to_delete:
                deleted_count = actor_db.delete_persons_by_emby_ids(pids_to_delete)
                stats['deleted'] = deleted_count

        except InterruptedError:
            if 'conn' in locals() and conn: conn.rollback()
            if update_status_callback: update_status_callback(100, "任务已中止")
            return
        except Exception as e_main:
            if 'conn' in locals() and conn: conn.rollback()
            logger.error(f"演员同步任务发生严重错误: {e_main}", exc_info=True)
            if update_status_callback: update_status_callback(-1, "数据库操作失败")
            return

        # --- 最终统计 ---
        logger.info("--- 单向同步演员数据完成 ---")
        logger.info(f"📊 : 新增 {stats['db_inserted']}, 更新 {stats['db_updated']}, 清理 {stats['deleted']}")
        logger.info("--------------------------")

        if update_status_callback:
            final_message = f"同步完成！新增 {stats['db_inserted']}, 更新 {stats['db_updated']}, 清理 {stats['deleted']}。"
            update_status_callback(100, final_message)