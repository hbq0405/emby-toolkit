# database/maintenance_db.py
import psycopg2
import re
import json
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from .connection import get_db_connection
from .log_db import LogDBManager
from .collection_db import remove_emby_id_from_all_collections

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 维护数据访问
# ======================================================================

# --- 媒体去重模块 ---
def get_all_cleanup_tasks() -> List[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_cleanup_tasks WHERE status = 'pending' ORDER BY item_name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取媒体去重任务列表失败: {e}", exc_info=True)
        return []

def batch_insert_cleanup_tasks(tasks: List[Dict[str, Any]]):
    
    if not tasks:
        logger.info("没有发现需要清理的媒体项，无需更新数据库。")
        return

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            logger.warning("  ➜ 正在清空旧的媒体去重任务列表...")
            cursor.execute("TRUNCATE TABLE media_cleanup_tasks RESTART IDENTITY;")

            sql = """
                INSERT INTO media_cleanup_tasks (
                    task_type, tmdb_id, item_name, item_type, versions_info_json, 
                    status, best_version_id, created_at
                ) VALUES %s
            """
            
            values_to_insert = [
                (
                    task.get('task_type'),
                    task.get('tmdb_id'),
                    task.get('item_name'),
                    task.get('item_type'), # 修正 item_type 的位置
                    Json(task.get('versions_info_json')),
                    task.get('status', 'pending'),
                    task.get('best_version_id'),
                    datetime.now(timezone.utc)
                ) for task in tasks
            ]
            
            execute_values(cursor, sql, values_to_insert, page_size=500)
            conn.commit()
            logger.info(f"  ➜ 成功批量插入 {len(tasks)} 条新的媒体去重任务。")

    except Exception as e:
        logger.error(f"  ➜ 批量插入媒体去重任务时失败: {e}", exc_info=True)
        raise

def get_cleanup_tasks_by_ids(task_ids: List[int]) -> List[Dict[str, Any]]:
    
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
                logger.info(f"  ➜ 已成功校准表 '{table_name}' 的序列。")
                corrected_tables.append(table_name)
            
            conn.commit()
            return corrected_tables

        except Exception as e:
            conn.rollback()
            logger.error(f"校准自增序列时发生严重错误: {e}", exc_info=True)
            raise

def get_dashboard_stats() -> dict:
    """
    执行一个聚合查询，获取数据看板所需的所有统计数据。
    """
    # 这个函数是从 database_admin.py 迁移过来的 _get_all_stats_in_one_query
    sql = """
    SELECT
        (SELECT COUNT(*) FROM media_metadata) AS media_cached_total,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = TRUE) AS media_in_library_total,
        COUNT(*) FILTER (WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        COUNT(*) FILTER (WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = FALSE) AS media_missing_total,
        (SELECT COUNT(*) FROM emby_users) AS emby_users_total,
        (SELECT COUNT(*) FROM emby_users_extended WHERE status = 'active') AS emby_users_active,
        (SELECT COUNT(*) FROM emby_users_extended WHERE status = 'disabled') AS emby_users_disabled,
        (SELECT COUNT(*) FROM media_cleanup_tasks WHERE status = 'pending') AS cleanup_tasks_pending,
        (SELECT COUNT(*) FROM resubscribe_rules WHERE enabled = TRUE) AS resubscribe_rules_enabled,
        (SELECT COUNT(*) FROM collections_info) AS collections_tmdb_total,
        (SELECT COUNT(*) FROM collections_info WHERE has_missing = TRUE) AS collections_with_missing,
        (SELECT COUNT(*) FROM custom_collections WHERE status = 'active') AS collections_custom_active,
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Watching') AS watchlist_active,
        (SELECT COUNT(*) FROM watchlist WHERE status = 'Paused') AS watchlist_paused,
        (SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active') AS actor_subscriptions_active,
        (SELECT COUNT(*) FROM tracked_actor_media) AS tracked_media_total,
        (SELECT COUNT(*) FROM tracked_actor_media WHERE status = 'IN_LIBRARY') AS tracked_media_in_library,
        (SELECT COUNT(*) FROM resubscribe_cache WHERE status ILIKE 'needed') AS resubscribe_pending,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NOT NULL) AS actor_mappings_linked,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NULL) AS actor_mappings_unlinked,
        (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
        (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
        (SELECT COUNT(*) FROM failed_log) AS failed_log_count
    FROM media_metadata
    LIMIT 1;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                return dict(result) if result else {}
    except psycopg2.Error as e:
        logger.error(f"执行聚合统计查询时出错: {e}")
        return {}

def get_all_table_names() -> List[str]:
    """
    使用 information_schema 获取数据库中所有表的名称。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cursor.execute(query)
                return [row['table_name'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取 PostgreSQL 表列表时出错: {e}", exc_info=True)
        raise

def export_tables_data(tables_to_export: List[str]) -> Dict[str, List[Dict]]:
    """
    从指定的多个表中导出所有数据。
    """
    exported_data = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for table_name in tables_to_export:
                    if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                        logger.warning(f"检测到无效的表名 '{table_name}'，已跳过导出。")
                        continue
                    
                    query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(table_name))
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    exported_data[table_name] = [dict(row) for row in rows]
        return exported_data
    except Exception as e:
        logger.error(f"导出数据库表时发生错误: {e}", exc_info=True)
        raise

def prepare_for_library_rebuild() -> Dict[str, Dict]:
    """
    【高危】执行为 Emby 媒体库重建做准备的所有数据库操作。
    """
    tables_to_truncate = [
        'emby_users', 'emby_users_extended', 'user_media_data', 'user_collection_cache',
        'collections_info', 'watchlist', 'resubscribe_cache', 'media_cleanup_tasks'
    ]
    columns_to_reset = {
        'media_metadata': 'emby_item_id', 'person_identity_map': 'emby_person_id',
        'custom_collections': 'emby_collection_id', 'tracked_actor_media': 'emby_item_id'
    }
    results = {"truncated_tables": {}, "updated_columns": {}}

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("第一步：开始清空 Emby 专属数据表...")
                for table_name in tables_to_truncate:
                    logger.warning(f"  ➜ 正在清空表: {table_name}")
                    query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(table=sql.Identifier(table_name))
                    cursor.execute(query)
                    results["truncated_tables"][table_name] = "清空成功"
                
                logger.info("第二步：开始断开元数据与 Emby ID 的关联...")
                for table_name, column_name in columns_to_reset.items():
                    logger.warning(f"  ➜ 正在重置表 '{table_name}' 中的 '{column_name}' 字段...")
                    query = sql.SQL("UPDATE {table} SET {column} = NULL WHERE {column} IS NOT NULL;").format(
                        table=sql.Identifier(table_name), column=sql.Identifier(column_name)
                    )
                    cursor.execute(query)
                    affected_rows = cursor.rowcount
                    results["updated_columns"][f"{table_name}.{column_name}"] = f"重置了 {affected_rows} 行"
                    logger.info(f"    ➜ 操作完成，影响了 {affected_rows} 行。")
        return results
    except Exception as e:
        logger.error(f"执行 prepare_for_library_rebuild 时发生严重错误: {e}", exc_info=True)
        raise

def cleanup_deleted_media_item(item_id: str, item_name: str, item_type: str, series_id_from_webhook: Optional[str] = None):
    """
    【高危】处理一个从 Emby 中被删除的媒体项，并清理所有相关的数据库记录。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                log_manager = LogDBManager()
                
                id_to_cleanup = None
                cleanup_item_name = item_name

                if item_type in ["Series", "Movie"]:
                    id_to_cleanup = item_id
                    cursor.execute("SELECT title FROM media_metadata WHERE emby_item_id = %s", (id_to_cleanup,))
                    db_record = cursor.fetchone()
                    if db_record and db_record['title']:
                        cleanup_item_name = db_record['title']
                    logger.warning(f"  ➜ 媒体项 '{cleanup_item_name}' (ID: {id_to_cleanup}) 本身被删除，将执行完整清理。")

                elif item_type == "Episode":
                    series_id = series_id_from_webhook
                    if not series_id:
                        logger.warning(f"  ➜ 分集 {item_id} 被删除，但无SeriesId，无法处理。")
                        return

                    cursor.execute("SELECT title, emby_children_details_json FROM media_metadata WHERE emby_item_id = %s", (series_id,))
                    record = cursor.fetchone()
                    
                    if record and record['emby_children_details_json']:
                        series_name = record['title'] or series_id
                        current_children = record['emby_children_details_json']
                        remaining_children = [child for child in current_children if child.get("Id") != item_id]
                        
                        remaining_children_json = json.dumps(remaining_children)
                        cursor.execute(
                            "UPDATE media_metadata SET emby_children_details_json = %s::jsonb WHERE emby_item_id = %s",
                            (remaining_children_json, series_id)
                        )
                        
                        has_remaining_episodes = any(child.get("Type") == "Episode" for child in remaining_children)
                        if not has_remaining_episodes:
                            logger.warning(f"  ➜ 剧集 '{series_name}' (ID: {series_id}) 的最后一个分集 '{item_name}' 已被删除，判断该剧集已离线，将执行完整清理。")
                            id_to_cleanup = series_id
                            cleanup_item_name = series_name
                        else:
                            logger.info(f"  ➜ 已从剧集 '{series_name}' 的记录中移除分集 '{item_name}'。不执行清理。")
                    else:
                        logger.warning(f"  ➜ 未在数据库中找到剧集 {series_id} 的子项目记录，无法更新。")
                
                # --- 统一的清理操作 ---
                if id_to_cleanup:
                    log_manager.remove_from_processed_log(cursor, id_to_cleanup)
                    logger.info(f"  ➜ 已从处理/失败日志中移除项目 '{cleanup_item_name}' (ID: {id_to_cleanup})。")

                    cursor.execute(
                        "UPDATE media_metadata SET in_library = FALSE, emby_item_id = NULL, emby_children_details_json = NULL WHERE emby_item_id = %s",
                        (id_to_cleanup,)
                    )
                    if cursor.rowcount > 0: logger.info(f"  ➜ 已在 media_metadata 缓存中将项目 '{cleanup_item_name}' 标记为“不在库中”。")
                    cursor.execute("DELETE FROM watchlist WHERE item_id = %s", (id_to_cleanup,))
                    if cursor.rowcount > 0: logger.info(f"  ➜ 已从智能追剧列表中移除项目 '{cleanup_item_name}'。")
                    cursor.execute("DELETE FROM resubscribe_cache WHERE item_id = %s", (id_to_cleanup,))
                    if cursor.rowcount > 0: logger.info(f"  ➜ 已从媒体洗版缓存中移除项目 '{cleanup_item_name}'。")
                    
                    # 注意：remove_emby_id_from_all_collections 内部自己管理事务，所以在这里调用是安全的
                    # 但为了在一个事务内完成，我们可以把它的逻辑也合并进来，或者暂时接受它开启一个新事务
                    # 为了简单起见，我们暂时接受它独立运行
                    conn.commit() # 在调用下一个独立事务函数前，先提交当前事务
                    remove_emby_id_from_all_collections(id_to_cleanup, cleanup_item_name)
                    return # 提前返回，因为后续不再需要 commit

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生数据库错误: {e}", exc_info=True)
        raise