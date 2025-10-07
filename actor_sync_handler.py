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
        【V5.1 - 修正调用方式】
        - 实现了完整的“清理、重链、同步”三阶段流程。
        - 确保在调用 emby_handler 时强制使用全局扫描，以覆盖所有演员。
        """
        logger.info("  ➜ 开始执行演员数据同步任务 (人事审计模式)...")
        
        stats = { "db_cleaned": 0, "db_relinked": 0, "db_inserted": 0, "db_updated": 0, "errors": 0 }

        try:
            # --- 数据准备阶段：获取 Emby 和本地数据库的全量数据 ---
            if update_status_callback: update_status_callback(0, "准备阶段: 正在获取 Emby 全量演员...")
            
            emby_persons_by_emby_id = {}
            emby_persons_by_tmdb_id = {}
            
            # ★★★ 核心修正：在这里传递 force_full_scan=True ★★★
            person_generator = emby_handler.get_all_persons_from_emby(
                self.emby_url, self.emby_api_key, self.emby_user_id, stop_event,
                update_status_callback=update_status_callback,
                force_full_scan=True, # 强制全局扫描，不错过任何“休假”员工
                start_progress=5
            )
            
            for person_batch in person_generator:
                for person in person_batch:
                    emby_id = person.get("Id")
                    tmdb_id = (person.get("ProviderIds", {}) or {}).get("Tmdb")
                    if emby_id:
                        emby_persons_by_emby_id[emby_id] = person
                    if tmdb_id:
                        emby_persons_by_tmdb_id[tmdb_id] = person

            if update_status_callback: update_status_callback(30, "准备阶段: 正在获取本地数据库全部演员...")
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT map_id, emby_person_id, tmdb_person_id FROM person_identity_map")
                    local_persons = cursor.fetchall()

            # --- 阶段一：清理“离职员工” ---
            if update_status_callback: update_status_callback(40, "阶段 1/3: 正在清理已下线的演员...")
            
            ids_to_clean = [
                p['emby_person_id'] for p in local_persons 
                if p['emby_person_id'] and p['emby_person_id'] not in emby_persons_by_emby_id
            ]
            
            if ids_to_clean:
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cleanup_query = "UPDATE person_identity_map SET emby_person_id = NULL WHERE emby_person_id = ANY(%s)"
                        cursor.execute(cleanup_query, (ids_to_clean,))
                        stats['db_cleaned'] = cursor.rowcount
                logger.info(f"  ➜ [清理] 成功标记 {stats['db_cleaned']} 位演员为“未关联”状态。")

            # --- 阶段二：智能重链“返聘员工” ---
            if update_status_callback: update_status_callback(50, "阶段 2/3: 正在为重新上线的演员智能重链...")
            
            relink_candidates = [
                p for p in local_persons 
                if not p['emby_person_id'] and p['tmdb_person_id'] and str(p['tmdb_person_id']) in emby_persons_by_tmdb_id
            ]

            if relink_candidates:
                relinked_count = 0
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        relink_query = "UPDATE person_identity_map SET emby_person_id = %s WHERE tmdb_person_id = %s"
                        for candidate in relink_candidates:
                            tmdb_id = str(candidate['tmdb_person_id'])
                            new_emby_id = emby_persons_by_tmdb_id[tmdb_id].get("Id")
                            if new_emby_id:
                                cursor.execute(relink_query, (new_emby_id, tmdb_id))
                                relinked_count += 1
                stats['db_relinked'] = relinked_count
                logger.info(f"  ➜ 成功为 {stats['db_relinked']} 位“重新上线”演员恢复了 Emby 关联。")

            # --- 阶段三：同步所有在职员工信息 ---
            if update_status_callback: update_status_callback(60, "阶段 3/3: 正在同步所有在线演员信息...")
            
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                    
                    total_emby_persons = len(emby_persons_by_emby_id)
                    processed_count = 0
                    for emby_pid, person_emby in emby_persons_by_emby_id.items():
                        if stop_event and stop_event.is_set(): 
                            raise InterruptedError("任务在处理时被中止")
                        
                        provider_ids = person_emby.get("ProviderIds", {})
                        person_data_for_db = { 
                            "emby_id": emby_pid, 
                            "name": person_emby.get("Name", "Unknown"), 
                            "tmdb_id": provider_ids.get("Tmdb"), 
                            "imdb_id": provider_ids.get("Imdb"), 
                            "douban_id": provider_ids.get("Douban"), 
                        }
                        
                        try:
                            _, status = self.actor_db_manager.upsert_person(cursor, person_data_for_db, emby_config=emby_config_for_upsert)
                            if status == "INSERTED": stats['db_inserted'] += 1
                            elif status == "UPDATED": stats['db_updated'] += 1
                        except Exception as e_upsert:
                            stats['errors'] += 1
                            logger.error(f"处理演员 {person_emby.get('Name')} (ID: {emby_pid}) 的 upsert 时失败: {e_upsert}")
                        
                        processed_count += 1
                        if update_status_callback and processed_count % 100 == 0:
                            progress = 60 + int((processed_count / total_emby_persons) * 40)
                            update_status_callback(progress, f"同步中: {processed_count}/{total_emby_persons}")
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

        # --- 最终统计 ---
        logger.info("  ➜ 演员数据人事审计完成")
        logger.info(f"  📊 : 清理 {stats['db_cleaned']}, 重链 {stats['db_relinked']}, 新增 {stats['db_inserted']}, 更新 {stats['db_updated']}.")

        if update_status_callback:
            final_message = f"审计完成！清理 {stats['db_cleaned']}, 重链 {stats['db_relinked']}, 新增 {stats['db_inserted']}, 更新 {stats['db_updated']}。"
            update_status_callback(100, final_message)