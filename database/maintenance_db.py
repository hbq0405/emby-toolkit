# database/maintenance_db.py
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# --- 媒体去重模块 ---
def get_all_cleanup_tasks() -> List[Dict[str, Any]]:
    # ... (函数体与原文件相同)
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_cleanup_tasks WHERE status = 'pending' ORDER BY item_name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取媒体去重任务列表失败: {e}", exc_info=True)
        return []

def batch_insert_cleanup_tasks(tasks: List[Dict[str, Any]]):
    # ... (函数体与原文件相同)
    if not tasks:
        logger.info("没有发现需要清理的媒体项，无需更新数据库。")
        return

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            logger.warning("正在清空旧的媒体去重任务列表...")
            cursor.execute("TRUNCATE TABLE media_cleanup_tasks RESTART IDENTITY;")

            sql = """
                INSERT INTO media_cleanup_tasks (
                    task_type, tmdb_id, item_name, versions_info_json, 
                    status, best_version_id, created_at
                ) VALUES %s
            """
            
            values_to_insert = [
                (
                    task.get('task_type'),
                    task.get('tmdb_id'),
                    task.get('item_name'),
                    Json(task.get('versions_info_json')),
                    task.get('status', 'pending'),
                    task.get('best_version_id'),
                    datetime.now(timezone.utc)
                ) for task in tasks
            ]
            
            execute_values(cursor, sql, values_to_insert, page_size=500)
            conn.commit()
            logger.info(f"DB: 成功批量插入 {len(tasks)} 条新的媒体去重任务。")

    except Exception as e:
        logger.error(f"DB: 批量插入媒体去重任务时失败: {e}", exc_info=True)
        raise

def get_cleanup_tasks_by_ids(task_ids: List[int]) -> List[Dict[str, Any]]:
    # ... (函数体与原文件相同)
    if not task_ids:
        return []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM media_cleanup_tasks WHERE id = ANY(%s)"
            cursor.execute(sql, (task_ids,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 根据ID获取清理任务时失败: {e}", exc_info=True)
        return []

def batch_update_cleanup_task_status(task_ids: List[int], new_status: str) -> int:
    # ... (函数体与原文件相同)
    if not task_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE media_cleanup_tasks SET status = %s WHERE id = ANY(%s)"
            cursor.execute(sql, (new_status, task_ids))
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 成功将 {updated_count} 个清理任务的状态更新为 '{new_status}'。")
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量更新清理任务状态时失败: {e}", exc_info=True)
        return 0

def batch_delete_cleanup_tasks(task_ids: List[int]) -> int:
    # ... (函数体与原文件相同)
    if not task_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM media_cleanup_tasks WHERE id = ANY(%s)"
            cursor.execute(sql, (task_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 成功删除了 {deleted_count} 个清理任务。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除清理任务时失败: {e}", exc_info=True)
        return 0

# --- 通用维护函数 ---
def clear_table(table_name: str) -> int:
    """清空指定的数据库表，返回删除的行数。"""
    # ... (函数体与原文件相同)
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))
            cursor.execute(query)
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"清空表 {table_name}，删除了 {deleted_count} 行。")
            return deleted_count
    except Exception as e:
        logger.error(f"清空表 {table_name} 时发生错误: {e}", exc_info=True)
        raise

def correct_all_sequences() -> list:
    """【V2 - 最终修正版】自动查找并校准所有表的自增序列。"""
    # ... (函数体与原文件相同)
    corrected_tables = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT c.table_name, c.column_name
                FROM information_schema.columns c
                WHERE c.table_schema = 'public' AND c.column_default LIKE 'nextval%';
            """)
            tables_with_sequences = cursor.fetchall()

            if not tables_with_sequences:
                logger.info("未找到任何使用自增序列的表，无需校准。")
                return []

            logger.info(f"开始校准 {len(tables_with_sequences)} 个表的自增序列...")

            for row in tables_with_sequences:
                table_name = row['table_name']
                column_name = row['column_name']
                
                query = sql.SQL("""
                    SELECT setval(
                        pg_get_serial_sequence({table}, {column}),
                        COALESCE((SELECT MAX({id_col}) FROM {table_ident}), 0) + 1,
                        false
                    )
                """).format(
                    table=sql.Literal(table_name),
                    column=sql.Literal(column_name),
                    id_col=sql.Identifier(column_name),
                    table_ident=sql.Identifier(table_name)
                )
                
                cursor.execute(query)
                logger.info(f"  -> 已成功校准表 '{table_name}' 的序列。")
                corrected_tables.append(table_name)
            
            conn.commit()
            return corrected_tables

        except Exception as e:
            conn.rollback()
            logger.error(f"校准自增序列时发生严重错误: {e}", exc_info=True)
            raise