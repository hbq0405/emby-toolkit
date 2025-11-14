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
from .collection_db import remove_tmdb_id_from_all_collections
from .media_db import get_tmdb_id_from_emby_id

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
    # ★★★ 核心修正：将 watchlist 相关的查询指向 media_metadata 表 ★★★
    sql = """
    SELECT
        (SELECT COUNT(*) FROM media_metadata WHERE item_type IN ('Movie', 'Series')) AS media_cached_total,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = TRUE) AS media_in_library_total,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Episode' AND in_library = TRUE) AS media_episodes_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = FALSE) AS media_missing_total,
        (SELECT COUNT(*) FROM person_identity_map) AS actor_mappings_total,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NOT NULL) AS actor_mappings_linked,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NULL) AS actor_mappings_unlinked,
        (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
        (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
        (SELECT COUNT(*) FROM failed_log) AS failed_log_count,
        
        -- ▼▼▼ 核心修改在这里 ▼▼▼
        (SELECT COUNT(*) FROM media_metadata WHERE watching_status = 'Watching') AS watchlist_active,
        (SELECT COUNT(*) FROM media_metadata WHERE watching_status = 'Paused') AS watchlist_paused,
        -- ▲▲▲ 核心修改在这里 ▲▲▲
        
        (SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active') AS actor_subscriptions_active,
        (SELECT COUNT(*) FROM resubscribe_cache WHERE status ILIKE 'needed') AS resubscribe_pending,
        (SELECT COUNT(*) FROM collections_info WHERE has_missing = TRUE) AS collections_with_missing
    LIMIT 1;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                return dict(result) if result else {}
    except psycopg2.Error as e:
        # 保留这个有用的警告，以防万一
        if "watchlist" in str(e):
             logger.warning(f"聚合统计查询失败，可能是由于旧的数据库结构: {e}。这通常在升级后自动解决。")
        else:
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
        'custom_collections': 'emby_collection_id'
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
    【V2 - 新架构重构版】处理一个从 Emby 中被删除的媒体项，并清理所有相关的数据库记录。
    此函数现在以 tmdb_id 为核心进行所有清理操作。
    """
    logger.info(f"  ➜ 检测到 Emby 媒体项被删除: '{item_name}' (Type: {item_type}, EmbyID: {item_id})，开始清理流程...")

    try:
        # ======================================================================
        # 步骤 1: ID 转换与目标确定 (The Translation & Targeting)
        # 无论删除的是什么，我们都需要找到它对应的 tmdb_id 和顶层剧集的 tmdb_id
        # ======================================================================
        
        target_tmdb_id: Optional[str] = None
        target_item_type: Optional[str] = None

        if item_type in ["Movie", "Series"]:
            # 如果删除的是电影或剧集本身，直接用它的 Emby ID 反查 TMDB ID
            target_tmdb_id = get_tmdb_id_from_emby_id(item_id)
            target_item_type = item_type
            if target_tmdb_id:
                logger.info(f"  ➜ 目标是顶层媒体项，映射到 TMDB ID: {target_tmdb_id}。")
            else:
                logger.warning(f"  ➜ 无法在数据库中找到 Emby ID {item_id} 对应的 TMDB ID，清理中止。")
                return

        elif item_type == "Episode":
            # 如果删除的是一集，我们需要判断它是不是这一季/这部剧的最后一集
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. 根据被删除集的 Emby ID，找到它在数据库中的记录，从而获取它的父剧集 TMDB ID
                    cursor.execute(
                        "SELECT parent_series_tmdb_id FROM media_metadata WHERE emby_item_ids_json @> %s::jsonb AND item_type = 'Episode'",
                        (json.dumps([item_id]),)
                    )
                    record = cursor.fetchone()
                    
                    if not record or not record['parent_series_tmdb_id']:
                        logger.warning(f"  ➜ 无法找到分集 Emby ID {item_id} 的父剧集信息，无法判断是否需要清理剧集。")
                        # 即使找不到父剧集，我们仍然需要将这一集本身标记为“不在库”
                        cursor.execute("UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb WHERE emby_item_ids_json @> %s::jsonb", (json.dumps([item_id]),))
                        conn.commit()
                        logger.info(f"  ➜ 已将被删除的分集 (Emby ID: {item_id}) 标记为“不在库中”。")
                        return

                    series_tmdb_id = record['parent_series_tmdb_id']
                    
                    # 2. 将被删除的这一集标记为“不在库”
                    cursor.execute(
                        "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb WHERE emby_item_ids_json @> %s::jsonb",
                        (json.dumps([item_id]),)
                    )
                    logger.info(f"  ➜ 已将被删除的分集 '{item_name}' (Emby ID: {item_id}) 标记为“不在库中”。")

                    # 3. 检查这部剧是否还有其他任何一集在库
                    cursor.execute(
                        "SELECT COUNT(*) as count FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE",
                        (series_tmdb_id,)
                    )
                    remaining_episodes = cursor.fetchone()['count']

                    if remaining_episodes == 0:
                        logger.warning(f"  ➜ 剧集 (TMDB ID: {series_tmdb_id}) 的最后一集已被删除，该剧集将被视为离线，将执行完整清理。")
                        target_tmdb_id = series_tmdb_id
                        target_item_type = "Series"
                    else:
                        logger.info(f"  ➜ 剧集 (TMDB ID: {series_tmdb_id}) 仍有 {remaining_episodes} 集在库，不执行剧集清理。")
                        conn.commit() # 只提交分集状态的更新
                        return
        
        # 如果经过上面的逻辑，没有确定要清理的目标，就直接返回
        if not target_tmdb_id:
            return

        # ======================================================================
        # 步骤 2: 执行统一的、基于 TMDB_ID 的清理操作
        # ======================================================================
        logger.info(f"--- 开始对 TMDB ID: {target_tmdb_id} (Type: {target_item_type}) 执行统一清理 ---")
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                log_manager = LogDBManager()
                
                # 1. 清理处理日志 (注意：日志表可能仍在使用 emby_id，需要确认)
                # 假设日志表也应该用 tmdb_id 关联
                # log_manager.remove_from_processed_log_by_tmdb_id(cursor, target_tmdb_id)
                
                # 2. 更新 media_metadata 状态 (核心操作)
                # 不仅更新顶层项目，还更新所有以它为父项目的子项目
                sql_update_status = """
                    UPDATE media_metadata 
                    SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb
                    WHERE tmdb_id = %s OR parent_series_tmdb_id = %s
                """
                cursor.execute(sql_update_status, (target_tmdb_id, target_tmdb_id))
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ 已在 media_metadata 中将 {cursor.rowcount} 个相关记录标记为“不在库中”。")

                # 3. 清理 watchlist (假设 watchlist.item_id 现在是 tmdb_id)
                cursor.execute("DELETE FROM watchlist WHERE tmdb_id = %s", (target_tmdb_id,))
                if cursor.rowcount > 0: logger.info(f"  ➜ 已从智能追剧列表中移除。")

                # 4. 清理 resubscribe_cache (假设 resubscribe_cache.tmdb_id 存在)
                cursor.execute("DELETE FROM resubscribe_cache WHERE tmdb_id = %s", (target_tmdb_id,))
                if cursor.rowcount > 0: logger.info(f"  ➜ 已从媒体洗版缓存中移除。")

                # 5. 清理自定义合集缓存 (调用新的、基于 tmdb_id 的函数)
                # 这个函数内部会处理自己的事务，所以我们在这里提交之前的操作
                conn.commit()

        # 在主事务之外调用，因为它自己管理事务
        remove_tmdb_id_from_all_collections(target_tmdb_id)
        
        logger.info(f"--- 对 TMDB ID: {target_tmdb_id} 的清理已全部完成 ---")

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生严重数据库错误: {e}", exc_info=True)