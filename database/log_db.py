# database/log_db.py
import psycopg2
import logging
from typing import Optional, List, Tuple, Dict

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 日志数据访问
# ======================================================================

class LogDBManager:
    """专门负责与日志相关的数据库表 (processed_log, failed_log) 进行交互的类。"""
    def __init__(self):
        pass

    def save_to_processed_log(self, cursor: psycopg2.extensions.cursor, item_id: str, item_name: str, score: float = 10.0):
        
        try:
            sql = """
                INSERT INTO processed_log (item_id, item_name, processed_at, score)
                VALUES (%s, %s, NOW(), %s)
                ON CONFLICT (item_id) DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    processed_at = NOW(),
                    score = EXCLUDED.score;
            """
            cursor.execute(sql, (item_id, item_name, score))
        except Exception as e:
            logger.error(f"写入已处理 失败 (Item ID: {item_id}): {e}")
    
    def remove_from_processed_log(self, cursor: psycopg2.extensions.cursor, item_id: str):
        
        try:
            logger.debug(f"正在从已处理日志中删除 Item ID: {item_id}...")
            cursor.execute("DELETE FROM processed_log WHERE item_id = %s", (item_id,))
        except Exception as e:
            logger.error(f"从已处理日志删除失败 for item {item_id}: {e}", exc_info=True)

    def remove_from_failed_log(self, cursor: psycopg2.extensions.cursor, item_id: str):
        
        try:
            cursor.execute("DELETE FROM failed_log WHERE item_id = %s", (item_id,))
        except Exception as e:
            logger.error(f"从 failed_log 删除失败 (Item ID: {item_id}): {e}")

    def save_to_failed_log(self, cursor: psycopg2.extensions.cursor, item_id: str, item_name: str, reason: str, item_type: str, score: Optional[float] = None):
        
        try:
            sql = """
                INSERT INTO failed_log (item_id, item_name, reason, item_type, score, failed_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (item_id) DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    reason = EXCLUDED.reason,
                    item_type = EXCLUDED.item_type,
                    score = EXCLUDED.score,
                    failed_at = NOW();
            """
            cursor.execute(sql, (item_id, item_name, reason, item_type, score))
        except Exception as e:
            logger.error(f"写入 failed_log 失败 (Item ID: {item_id}): {e}")
    
    def mark_assets_as_synced(self, cursor, item_id: str, sync_timestamp_iso: str):
        """在 processed_log 中标记一个项目的资源文件已同步。"""
        
        logger.trace(f"  ➜ 正在更新 Item ID {item_id} 的备份状态和时间戳...")
        sql = """
            INSERT INTO processed_log (item_id, assets_synced_at)
            VALUES (%s, %s)
            ON CONFLICT (item_id) DO UPDATE SET
                assets_synced_at = EXCLUDED.assets_synced_at;
        """
        try:
            cursor.execute(sql, (item_id, sync_timestamp_iso))
        except Exception as e:
            logger.error(f"更新资源同步时间戳时失败 for item {item_id}: {e}", exc_info=True)

def get_item_name_from_failed_log(item_id: str) -> Optional[str]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name FROM failed_log WHERE item_id = %s", (item_id,))
            result = cursor.fetchone()
            return result['item_name'] if result else None
    except Exception as e:
        logger.error(f"从 failed_log 获取 item_name 时出错: {e}")
        return None

def get_review_items_paginated(page: int, per_page: int, query_filter: str) -> Tuple[List, int]:
    
    offset = (page - 1) * per_page
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            where_clause = ""
            sql_params = []
            if query_filter:
                where_clause = "WHERE item_name ILIKE %s"
                sql_params.append(f"%{query_filter}%")

            count_sql = f"SELECT COUNT(*) as total FROM failed_log {where_clause}"
            cursor.execute(count_sql, tuple(sql_params))
            total_matching_items = cursor.fetchone()['total']

            items_sql = f"""
                SELECT item_id, item_name, failed_at, reason, item_type, score 
                FROM failed_log {where_clause}
                ORDER BY failed_at DESC 
                LIMIT %s OFFSET %s
            """
            cursor.execute(items_sql, tuple(sql_params + [per_page, offset]))
            items_to_review = [dict(row) for row in cursor.fetchall()]
            
        return items_to_review, total_matching_items
    except Exception as e:
        logger.error(f"DB: 获取待复核列表失败: {e}", exc_info=True)
        raise

def mark_review_item_as_processed(item_id: str) -> bool:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            with conn.cursor() as cursor:
                cursor.execute("SELECT item_name, item_type, score FROM failed_log WHERE item_id = %s", (item_id,))
                failed_item_info = cursor.fetchone()
                if not failed_item_info: return False

                cursor.execute("DELETE FROM failed_log WHERE item_id = %s", (item_id,))
                
                score_to_save = failed_item_info["score"] if failed_item_info["score"] is not None else 10.0
                
                upsert_sql = """
                    INSERT INTO processed_log (item_id, item_name, processed_at, score) 
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (item_id) DO UPDATE SET
                        item_name = EXCLUDED.item_name,
                        processed_at = NOW(),
                        score = EXCLUDED.score;
                """
                cursor.execute(upsert_sql, (item_id, failed_item_info["item_name"], score_to_save))
            conn.commit()
            logger.info(f"DB: 项目 {item_id} 已成功移至已处理日志。")
            return True
    except Exception as e:
        logger.error(f"DB: 标记项目 {item_id} 为已处理时失败: {e}", exc_info=True)
        raise

def clear_all_review_items() -> List[Dict[str, str]]:
    """将所有待复核项移至已处理。"""
    
    moved_items = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT item_id, item_name, score FROM failed_log")
                items_to_move = cursor.fetchall()
                
                if not items_to_move:
                    return []

                for item in items_to_move:
                    score_to_save = item["score"] if item["score"] is not None else 10.0
                    cursor.execute(
                        "INSERT INTO processed_log (item_id, item_name, processed_at, score) VALUES (%s, %s, NOW(), %s) "
                        "ON CONFLICT (item_id) DO UPDATE SET item_name = EXCLUDED.item_name, processed_at = NOW(), score = EXCLUDED.score",
                        (item['item_id'], item['item_name'], score_to_save)
                    )
                    moved_items.append(dict(item))

                cursor.execute("DELETE FROM failed_log")
                conn.commit()
                
            logger.info(f"成功移动 {len(moved_items)} 条记录从待复核到已处理。")
            return moved_items
    except Exception as e:
        logger.error(f"清空并标记待复核列表时发生异常：{e}", exc_info=True)
        return []