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
import constants

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 维护数据访问
# ======================================================================

# --- 媒体去重模块 ---
def get_all_cleanup_index() -> List[Dict[str, Any]]:
    """获取所有待处理的清理索引。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 表名已修改 ★★★
                cursor.execute("SELECT * FROM cleanup_index WHERE status = 'pending' ORDER BY id")
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取清理索引列表失败: {e}", exc_info=True)
        return []

def batch_upsert_cleanup_index(tasks: List[Dict[str, Any]]):
    """
    【V4 - 状态保持版 Upsert】
    批量插入或更新清理索引。使用 (tmdb_id, item_type) 作为唯一标识来处理冲突。
    ★★★ 核心修复：当发生冲突时，只有在现有记录的状态不是 'ignored' 的情况下才执行更新。★★★
    """
    if not tasks:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                columns = list(tasks[0].keys())
                update_cols = [col for col in columns if col not in ['tmdb_id', 'item_type']]
                
                # ★★★ 修改后的 SQL 查询 ★★★
                sql_query = sql.SQL("""
                    INSERT INTO cleanup_index ({cols})
                    VALUES %s
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        {updates},
                        status = 'pending',
                        last_updated_at = NOW()
                    WHERE cleanup_index.status != 'ignored' -- <-- 新增的条件
                """).format(
                    cols=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    updates=sql.SQL(', ').join(
                        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in update_cols
                    )
                )

                values_to_insert = []
                for task in tasks:
                    row = [task.get(col) for col in columns]
                    # 找到 versions_info_json 的索引并用 Json() 包装
                    if 'versions_info_json' in columns:
                        idx = columns.index('versions_info_json')
                        row[idx] = Json(row[idx])
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
    """清空所有状态为 'pending' 的清理任务 (现在是索引)。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM cleanup_index WHERE status = 'pending';")
                conn.commit()
                logger.info("已清空所有待处理的媒体清理索引。")
    except Exception as e:
        logger.error(f"清空待处理的媒体清理索引时失败: {e}", exc_info=True)

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
                logger.info("  ➜ 未找到任何使用自增序列的表，无需校准。")
                return []

            logger.info(f"  ➜ 开始校准 {len(tables_with_sequences)} 个表的自增序列...")

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
            logger.error(f"  ➜ 校准自增序列时发生严重错误: {e}", exc_info=True)
            raise

def get_dashboard_stats() -> dict:
    """
    执行一个聚合查询，获取数据看板所需的所有统计数据。
    """
    # ★★★ 核心修正：将 watchlist 相关的查询指向 media_metadata 表 ★★★
    sql = """
    SELECT
        -- 核心数据: 已缓存媒体 (只统计顶层项目)
        (SELECT COUNT(*) FROM media_metadata WHERE item_type IN ('Movie', 'Series')) AS media_cached_total,
        
        -- 核心数据: 已归档演员 (逻辑不变)
        (SELECT COUNT(*) FROM person_identity_map) AS actor_mappings_total,
        
        -- 媒体细分: 在库电影数 (逻辑不变)
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        
        -- 媒体细分: 在库剧集数 (逻辑不变)
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        
        -- 媒体细分: 在库总集数 (逻辑不变)
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Episode' AND in_library = TRUE) AS media_episodes_in_library,
        
        -- 媒体细分: 预缓存 (修正：只统计不在库的顶层项目)
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = FALSE AND item_type IN ('Movie', 'Series')) AS media_missing_total,
        
        -- 演员细分 (逻辑不变)
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NOT NULL) AS actor_mappings_linked,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NULL) AS actor_mappings_unlinked,
        
        -- 系统日志与缓存 (逻辑不变)
        (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
        (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
        (SELECT COUNT(*) FROM failed_log) AS failed_log_count,
        
        -- 智能订阅 (逻辑不变)
        (SELECT COUNT(*) FROM media_metadata WHERE watching_status = 'Watching') AS watchlist_active,
        (SELECT COUNT(*) FROM media_metadata WHERE watching_status = 'Paused') AS watchlist_paused,
        (SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active') AS actor_subscriptions_active,
        (SELECT COUNT(*) FROM media_metadata WHERE subscription_sources_json @> '[{"type": "actor_subscription"}]'::jsonb) AS actor_works_total,
        (SELECT COUNT(*) FROM media_metadata WHERE subscription_sources_json @> '[{"type": "actor_subscription"}]'::jsonb AND in_library = TRUE) AS actor_works_in_library,
        (SELECT COUNT(*) FROM resubscribe_index WHERE status ILIKE 'needed') AS resubscribe_pending,
        
        -- ★★★ 原生合集统计 (原 collections_info 表) - 新增总数统计 ★★★
        (SELECT COUNT(*) FROM collections_info) AS native_collections_total,
        (SELECT COUNT(*) FROM collections_info WHERE has_missing = TRUE) AS native_collections_with_missing,
        (SELECT SUM(jsonb_array_length(missing_movies_json)) FROM collections_info WHERE has_missing = TRUE) AS native_collections_missing_items,
        
        -- ★★★ 自建合集统计 (custom_collections 表) - 新增总数统计 ★★★
        (SELECT COUNT(*) FROM custom_collections) AS custom_collections_total,
        (SELECT COUNT(*) FROM custom_collections WHERE missing_count > 0) AS custom_collections_with_missing,
        (SELECT SUM(missing_count) FROM custom_collections WHERE missing_count > 0) AS custom_collections_missing_items;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                if result:
                    stats_dict = dict(result)
                    stats_dict['resolution_distribution'] = get_resolution_distribution()
                    return stats_dict
                else:
                    return {}
    except psycopg2.Error as e:
        # 保留这个有用的警告，以防万一
        if "watchlist" in str(e):
             logger.warning(f"聚合统计查询失败，可能是由于旧的数据库结构: {e}。这通常在升级后自动解决。")
        else:
            logger.error(f"执行聚合统计查询时出错: {e}")
        return {}
    
def get_resolution_distribution() -> List[Dict[str, Any]]:
    """获取在库媒体的分辨率分布，用于生成图表。"""
    sql = """
        SELECT 
            -- 提取 asset_details_json 数组中第一个元素的 resolution_display 字段
            (jsonb_array_elements(asset_details_json) ->> 'resolution_display') as resolution,
            COUNT(*) as count
        FROM 
            media_metadata
        WHERE 
            in_library = TRUE 
            AND item_type IN ('Movie', 'Episode')
            AND asset_details_json IS NOT NULL
            AND jsonb_array_length(asset_details_json) > 0
        GROUP BY 
            resolution
        ORDER BY 
            count DESC;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取分辨率分布数据失败: {e}", exc_info=True)
        return []

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
        'collections_info', 'watchlist', 'resubscribe_index', 'media_cleanup_tasks'
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
                sql_reset_watchlist = """
                    UPDATE media_metadata
                    SET watching_status = 'NONE'
                    WHERE 
                        tmdb_id = %s 
                        AND item_type = 'Series' 
                        AND watching_status != 'NONE'
                """
                cursor.execute(sql_reset_watchlist, (target_tmdb_id,))
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ 已将该剧集的智能追剧状态重置为'NONE'。")

                # 4. 清理resubscribe_index 
                cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s", (target_tmdb_id,))
                if cursor.rowcount > 0: logger.info(f"  ➜ 已从媒体洗版缓存中移除。")

                # 5. 清理用户权限缓存 
                sql_cleanup_user_cache = """
                    UPDATE user_collection_cache
                    SET 
                        visible_emby_ids_json = (
                            SELECT jsonb_agg(elem)
                            FROM jsonb_array_elements_text(visible_emby_ids_json) AS elem
                            WHERE elem != %s
                        ),
                        total_count = total_count - 1
                    WHERE 
                        visible_emby_ids_json @> %s::jsonb;
                """
                # 使用被删除项的 Emby Item ID (函数入口传入的 item_id) 来执行清理
                cursor.execute(sql_cleanup_user_cache, (item_id, json.dumps([item_id])))
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ 已从 {cursor.rowcount} 条用户权限缓存中移除了已被删除的 Emby ID: {item_id}。")

                conn.commit()

        # 在主事务之外调用，因为它自己管理事务
        remove_tmdb_id_from_all_collections(target_tmdb_id)
        
        logger.info(f"--- 对 TMDB ID: {target_tmdb_id} 的清理已全部完成 ---")

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生严重数据库错误: {e}", exc_info=True)

def get_release_group_ranking(limit: int = 5) -> list: # 默认值也改成5
    """
    统计【当天入库】的发布组作品（文件）数量，并返回排名前N的列表。
    """
    query = f"""
        SELECT
            release_group,
            COUNT(*) AS count
        FROM (
            SELECT jsonb_array_elements_text(asset -> 'release_group_raw') AS release_group
            FROM (
                SELECT jsonb_array_elements(asset_details_json) AS asset
                FROM media_metadata
                WHERE 
                    in_library = TRUE 
                    AND asset_details_json IS NOT NULL 
                    AND jsonb_array_length(asset_details_json) > 0
                    AND (date_added AT TIME ZONE 'UTC' AT TIME ZONE %(timezone)s)::date = (NOW() AT TIME ZONE %(timezone)s)::date
            ) AS assets
        ) AS release_groups
        WHERE release_group IS NOT NULL AND release_group != ''
        GROUP BY release_group
        ORDER BY count DESC
        LIMIT %(limit)s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 2/2: 核心修正 - 使用您已有的 TIMEZONE 常量 ★★★
                params = {'timezone': constants.TIMEZONE, 'limit': limit}
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row] in results]
    except Exception as e:
        logger.error(f"获取【每日】发布组排行时发生数据库错误: {e}", exc_info=True)
        return []