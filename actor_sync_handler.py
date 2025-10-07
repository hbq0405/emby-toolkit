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
        【V4 - 数据补全修复版】
        - 在将数据发送到 upsert_person 之前，主动获取每个演员的完整详情，确保 TMDb ID 不会丢失。
        """
        logger.trace("  ➜ 开始执行演员数据单向同步任务 (Emby -> 本地数据库) ")
        
        stats = { "total_from_emby": 0, "processed": 0, "db_inserted": 0, "db_updated": 0, 
                  "unchanged": 0, "skipped": 0, "errors": 0 }

        try:
            if update_status_callback: update_status_callback(0, "正在从 Emby 扫描并同步演员...")
            
            person_generator = emby_handler.get_all_persons_from_emby(
                self.emby_url, self.emby_api_key, self.emby_user_id, stop_event,
                update_status_callback=update_status_callback
            )
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for person_batch in person_generator:
                        if stop_event and stop_event.is_set(): 
                            raise InterruptedError("任务在处理批次时被中止")
                        
                        for person_emby_basic in person_batch:
                            stats["total_from_emby"] += 1
                            emby_pid = str(person_emby_basic.get("Id", "")).strip()

                            if not emby_pid:
                                stats["skipped"] += 1
                                continue
                            
                            person_emby_full = emby_handler.get_emby_item_details(
                                item_id=emby_pid,
                                emby_server_url=self.emby_url,
                                emby_api_key=self.emby_api_key,
                                user_id=self.emby_user_id,
                                fields="ProviderIds,Name" 
                            )

                            if not person_emby_full:
                                stats["skipped"] += 1
                                logger.warning(f"无法获取演员 (ID: {emby_pid}) 的完整详情，已跳过。")
                                continue

                            person_name = str(person_emby_full.get("Name", "")).strip()
                            provider_ids = person_emby_full.get("ProviderIds", {})
                            
                            person_data_for_db = { 
                                "emby_id": emby_pid, 
                                "name": person_name, 
                                "tmdb_id": provider_ids.get("Tmdb"), 
                                "imdb_id": provider_ids.get("Imdb"), 
                                "douban_id": provider_ids.get("Douban"), 
                            }
                            
                            try:
                                _, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db)
                                if status == "INSERTED": stats['db_inserted'] += 1
                                elif status == "UPDATED": stats['db_updated'] += 1
                                elif status == "SKIPPED_NO_TMDB_ID": stats['skipped'] += 1 # 明确统计被跳过的
                                else: stats['unchanged'] += 1
                            except Exception as e_upsert:
                                stats['errors'] += 1
                                logger.error(f"处理演员 {person_name} (ID: {emby_pid}) 的 upsert 时失败: {e_upsert}")
                conn.commit()

        except InterruptedError:
            # 使用 'conn' in locals() and conn 来安全地检查连接对象是否存在
            if 'conn' in locals() and conn: 
                conn.rollback()
            if update_status_callback: 
                update_status_callback(100, "任务已中止")
            return
        except Exception as e_main:
            if 'conn' in locals() and conn: 
                conn.rollback()
            logger.error(f"演员同步任务发生严重错误: {e_main}", exc_info=True)
            if update_status_callback: 
                update_status_callback(-1, "数据库操作失败")
            return

        # --- 最终统计 ---
        logger.info("  ➜ 单向同步演员数据完成")
        # 日志输出中移除了 '清理'
        logger.info(f"  📊 : 新增 {stats['db_inserted']}, 更新 {stats['db_updated']}.")

        if update_status_callback:
            # 最终消息中移除了 '清理'
            final_message = f"同步完成！新增 {stats['db_inserted']}, 更新 {stats['db_updated']}。"
            update_status_callback(100, final_message)