# database/cleanup_db.py
import logging
import json
from typing import List, Dict, Any
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_all_cleanup_index() -> List[Dict[str, Any]]:
    """获取所有待处理的清理索引。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cleanup_index WHERE status = 'pending' ORDER BY id")
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取清理索引列表失败: {e}", exc_info=True)
        return []

def batch_upsert_cleanup_index(tasks: List[Dict[str, Any]]):
    """
    批量插入或更新清理索引。使用 (tmdb_id, item_type) 作为唯一标识。
    """
    if not tasks:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                columns = list(tasks[0].keys())
                update_cols = [col for col in columns if col not in ['tmdb_id', 'item_type']]
                
                sql_query = sql.SQL("""
                    INSERT INTO cleanup_index ({cols})
                    VALUES %s
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        {updates},
                        status = 'pending',
                        last_updated_at = NOW()
                    WHERE cleanup_index.status != 'ignored'
                """).format(
                    cols=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    updates=sql.SQL(', ').join(
                        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in update_cols
                    )
                )

                values_to_insert = []
                for task in tasks:
                    row = []
                    for col in columns:
                        val = task.get(col)
                        if col in ['versions_info_json', 'best_version_json']:
                            row.append(Json(val) if val is not None else None)
                        else:
                            row.append(val)
                    
                    values_to_insert.append(tuple(row))
            
                execute_values(cursor, sql_query, values_to_insert, page_size=500)
                conn.commit()
                logger.info(f"  ➜ 成功批量写入/更新 {len(tasks)} 条媒体清理索引。")

    except Exception as e:
        logger.error(f"  ➜ 批量写入/更新媒体清理索引时失败: {e}", exc_info=True)
        raise

def get_cleanup_index_by_ids(task_ids: List[int]) -> List[Dict[str, Any]]:
    """根据ID获取清理索引。"""
    if not task_ids: return []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM cleanup_index WHERE id = ANY(%s)", (task_ids,))
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 根据ID获取清理索引时失败: {e}", exc_info=True)
        return []

def batch_update_cleanup_index_status(task_ids: List[int], new_status: str) -> int:
    """批量更新清理索引的状态。"""
    if not task_ids: return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE cleanup_index SET status = %s, last_updated_at = NOW() WHERE id = ANY(%s)", (new_status, task_ids))
                updated_count = cursor.rowcount
                conn.commit()
                return updated_count
    except Exception as e:
        logger.error(f"DB: 批量更新清理索引状态时失败: {e}", exc_info=True)
        return 0

def batch_delete_cleanup_index(task_ids: List[int]) -> int:
    """批量删除清理索引。"""
    if not task_ids: return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cleanup_index WHERE id = ANY(%s)", (task_ids,))
                deleted_count = cursor.rowcount
                conn.commit()
                return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除清理索引时失败: {e}", exc_info=True)
        return 0

def clear_pending_cleanup_tasks():
    """清空所有状态为 'pending' 的清理索引。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cleanup_index WHERE status = 'pending';")
                conn.commit()
                logger.info("  ➜ 已清空所有待处理的媒体清理索引。")
    except Exception as e:
        logger.error(f"清空待处理的媒体清理索引时失败: {e}", exc_info=True)