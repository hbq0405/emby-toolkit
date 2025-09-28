# actor_sync_handler.py (最终版)

from typing import Optional, Callable
import threading
# 导入必要的模块
import emby_handler
from database.actor_db import ActorDBManager
from database import connection, actor_db
import logging
# 导入新增的工具函数
from utils import contains_chinese

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
        【V4 - 性能优化版】
        1. 移除了清理逻辑。
        2. 增加了中文名双重同步。
        3. ★ 优化：仅在演员名实际发生变化时，才触发 media_metadata 的更新。
        """
        logger.info("--- 开始执行演员数据单向同步任务 (Emby -> 本地数据库) ---")
        
        stats = { "total_from_emby": 0, "processed": 0, "db_inserted": 0, "db_updated": 0, 
                  "unchanged": 0, "skipped": 0, "errors": 0 }

        try:
            if update_status_callback: update_status_callback(0, "从 Emby 扫描并同步演员...")
            
            person_generator = emby_handler.get_all_persons_from_emby(
                self.emby_url, self.emby_api_key, self.emby_user_id, stop_event,
                update_status_callback=update_status_callback
            )
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                    
                    for person_batch in person_generator:
                        if stop_event and stop_event.is_set(): raise InterruptedError("任务在处理批次时被中止")
                        
                        for person_emby in person_batch:
                            stats["total_from_emby"] += 1
                            emby_pid = str(person_emby.get("Id", "")).strip()
                            person_name = str(person_emby.get("Name", "")).strip()
                            
                            if not emby_pid or not person_name:
                                stats["skipped"] += 1
                                continue
                            
                            provider_ids = person_emby.get("ProviderIds", {})
                            person_data_for_db = { "emby_id": emby_pid, "name": person_name, "tmdb_id": provider_ids.get("Tmdb"), "imdb_id": provider_ids.get("Imdb"), "douban_id": provider_ids.get("Douban"), }
                            
                            try:
                                # 步骤 1: 更新或插入到 person_identity_map 表
                                _, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db, emby_config=emby_config_for_upsert)
                                if status == "INSERTED": stats['db_inserted'] += 1
                                elif status == "UPDATED": stats['db_updated'] += 1
                                elif status == "UNCHANGED": stats['unchanged'] += 1
                                elif status == "SKIPPED": stats['skipped'] += 1

                                # ★★★ 性能优化核心 ★★★
                                # 仅当名字被实际更新(status=='UPDATED')，且新名字是中文时，才触发媒体库更新
                                tmdb_id = provider_ids.get("Tmdb")
                                if status == "UPDATED" and contains_chinese(person_name) and tmdb_id:
                                    try:
                                        # 步骤 2: 调用函数，将中文名同步到 media_metadata.actors_json
                                        updated_media_count = self.actor_db_manager.update_actor_name_in_media_metadata(
                                            cursor, int(tmdb_id), person_name
                                        )
                                        if updated_media_count > 0:
                                            logger.info(f"  -> 媒体库演员名同步: '{person_name}' (TMDb ID: {tmdb_id}) 的姓名已更新，影响了 {updated_media_count} 条媒体记录。")
                                    except Exception as e_media_update:
                                        logger.error(f"更新 media_metadata 中演员名 '{person_name}' (TMDb ID: {tmdb_id}) 时失败: {e_media_update}")

                            except Exception as e_upsert:
                                stats['errors'] += 1
                                logger.error(f"处理演员 {person_name} (ID: {emby_pid}) 的 upsert 时失败: {e_upsert}")
                conn.commit()

        except InterruptedError:
            if 'conn' in locals() and conn: conn.rollback()
            if update_status_callback: update_status_callback(100, "任务已中止")
            return
        except Exception as e_main:
            if 'conn' in locals() and conn: conn.rollback()
            logger.error(f"演员同步任务发生严重错误: {e_main}", exc_info=True)
            if update_status_callback: update_status_callback(-1, "数据库操作失败")
            return

        logger.info("--- 单向同步演员数据完成 ---")
        logger.info(f"📊 : 新增 {stats['db_inserted']}, 更新 {stats['db_updated']}。")
        logger.info("--------------------------")

        if update_status_callback:
            final_message = f"同步完成！新增 {stats['db_inserted']}, 更新 {stats['db_updated']}。"
            update_status_callback(100, final_message)