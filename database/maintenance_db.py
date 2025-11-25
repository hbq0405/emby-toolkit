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

# ======================================================================
# 模块: 数据看板统计 (拆分版)
# ======================================================================

def _execute_single_row_query(sql_query: str) -> dict:
    """辅助函数：执行返回单行结果的查询"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchone()
                return dict(result) if result else {}
    except Exception as e:
        logger.error(f"统计查询出错: {e}")
        return {}

def get_stats_core() -> dict:
    """1. 核心头部数据 (极快)"""
    sql = """
    SELECT
        (SELECT COUNT(*) FROM media_metadata WHERE item_type IN ('Movie', 'Series')) AS media_cached_total,
        (SELECT COUNT(*) FROM person_identity_map) AS actor_mappings_total
    """
    return _execute_single_row_query(sql)

def get_stats_library() -> dict:
    """2. 媒体库概览 (较快)"""
    sql = """
    SELECT
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Episode' AND in_library = TRUE) AS media_episodes_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE in_library = FALSE AND item_type IN ('Movie', 'Series')) AS media_missing_total
    """
    data = _execute_single_row_query(sql)
    data['resolution_stats'] = get_resolution_distribution() # 复用现有的分辨率函数
    return data

def get_stats_system() -> dict:
    """3. 系统日志与缓存 (快)"""
    sql = """
    SELECT
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NOT NULL) AS actor_mappings_linked,
        (SELECT COUNT(*) FROM person_identity_map WHERE emby_person_id IS NULL) AS actor_mappings_unlinked,
        (SELECT COUNT(*) FROM translation_cache) AS translation_cache_count,
        (SELECT COUNT(*) FROM processed_log) AS processed_log_count,
        (SELECT COUNT(*) FROM failed_log) AS failed_log_count
    """
    return _execute_single_row_query(sql)

def get_stats_subscription():
    """
    获取订阅相关的统计数据 (最终修正：限制为 Series 类型，防止统计季层级)
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 追剧统计
                # 修正：增加 AND item_type = 'Series'，只统计剧集层级，排除季和集
                cursor.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE TRIM(watching_status) ILIKE 'Watching') as watching,
                        COUNT(*) FILTER (WHERE TRIM(watching_status) ILIKE 'Paused') as paused,
                        COUNT(*) FILTER (WHERE TRIM(watching_status) ILIKE 'Completed') as completed
                    FROM media_metadata
                    WHERE watching_status IS NOT NULL 
                      AND watching_status NOT ILIKE 'NONE'
                      AND item_type = 'Series'
                """)
                watchlist_row = cursor.fetchone()
                
                # 2. 演员订阅统计
                cursor.execute("SELECT COUNT(*) FROM actor_subscriptions WHERE status = 'active'")
                actor_sub_count = cursor.fetchone()['count']

                # 保持修复：使用 @> 操作符，避免 SQL 报错
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE in_library = TRUE) as in_lib
                    FROM media_metadata 
                    WHERE subscription_sources_json @> '["Actor"]'::jsonb
                """)
                actor_works_row = cursor.fetchone()

                # 3. 洗版统计
                cursor.execute("SELECT COUNT(*) FROM resubscribe_index WHERE status = 'needed'")
                resub_pending = cursor.fetchone()['count']

                # 4. 原生合集统计
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE has_missing = TRUE) as with_missing,
                        COALESCE(SUM((jsonb_array_length(missing_movies_json))), 0) as missing_items
                    FROM collections_info
                """)
                native_col_row = cursor.fetchone()

                # 5. 自建合集统计
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE missing_count > 0) as with_missing,
                        COALESCE(SUM(missing_count), 0) as missing_items
                    FROM custom_collections
                    WHERE status = 'active'
                """)
                custom_col_row = cursor.fetchone()

                return {
                    'watchlist_active': watchlist_row['watching'],
                    'watchlist_paused': watchlist_row['paused'],
                    'watchlist_completed': watchlist_row['completed'],
                    
                    'actor_subscriptions_active': actor_sub_count,
                    'actor_works_total': actor_works_row['total'],
                    'actor_works_in_library': actor_works_row['in_lib'],
                    
                    'resubscribe_pending': resub_pending,
                    
                    'native_collections_total': native_col_row['total'],
                    'native_collections_with_missing': native_col_row['with_missing'],
                    'native_collections_missing_items': native_col_row['missing_items'],
                    
                    'custom_collections_total': custom_col_row['total'],
                    'custom_collections_with_missing': custom_col_row['with_missing'],
                    'custom_collections_missing_items': custom_col_row['missing_items']
                }
    except Exception as e:
        logger.error(f"获取订阅统计失败: {e}", exc_info=True)
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

def get_release_group_ranking(limit: int = 5) -> list:
    """
    统计【当天入库】的发布组作品（文件）数量，并返回排名前N的列表。
    """
    query = """
        SELECT
            release_group,
            COUNT(*) AS count
        FROM (
            SELECT
                jsonb_array_elements_text(asset -> 'release_group_raw') AS release_group,
                ((asset ->> 'date_added_to_library')::timestamp AT TIME ZONE 'UTC') AS asset_added_at_utc
            FROM (
                SELECT jsonb_array_elements(asset_details_json) AS asset
                FROM media_metadata
                WHERE
                    in_library = TRUE
                    AND asset_details_json IS NOT NULL
                    AND jsonb_array_length(asset_details_json) > 0
                    AND asset_details_json::text LIKE %s
            ) AS assets
        ) AS release_groups
        WHERE
            release_group IS NOT NULL AND release_group != ''
            AND (asset_added_at_utc AT TIME ZONE %s)::date = (NOW() AT TIME ZONE %s)::date
        GROUP BY release_group
        ORDER BY count DESC
        LIMIT %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                like_pattern = '%date_added_to_library%'
                params = (like_pattern, constants.TIMEZONE, constants.TIMEZONE, limit)
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"获取【每日】发布组排行时发生数据库错误: {e}", exc_info=True)
        return []
    
def get_historical_release_group_ranking(limit: int = 5) -> list:
    """
    统计【历史入库】的所有发布组作品（文件）数量，并返回总排名前N的列表。
    """
    # 这个查询与 get_release_group_ranking 几乎一样，但没有按“当天”过滤
    query = """
        SELECT
            release_group,
            COUNT(*) AS count
        FROM (
            SELECT 
                jsonb_array_elements_text(asset -> 'release_group_raw') AS release_group
            FROM (
                SELECT jsonb_array_elements(asset_details_json) AS asset
                FROM media_metadata
                WHERE 
                    in_library = TRUE 
                    AND asset_details_json IS NOT NULL 
                    AND jsonb_array_length(asset_details_json) > 0
                    -- 仍然检查 date_added_to_library 字段是否存在，以确保是有效入库记录
                    AND asset_details_json::text LIKE %s
            ) AS assets
        ) AS release_groups
        WHERE 
            release_group IS NOT NULL AND release_group != ''
        GROUP BY release_group
        ORDER BY count DESC
        LIMIT %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 参数减少了，因为不再需要时区
                like_pattern = '%date_added_to_library%'
                params = (like_pattern, limit)
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"获取【历史】发布组排行时发生数据库错误: {e}", exc_info=True)
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

    # media_metadata 对应字段改成列表，其他保持字符串方便兼容
    columns_to_reset = {
        'media_metadata': ['emby_item_ids_json', 'asset_details_json'],
        'person_identity_map': 'emby_person_id',
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

                for table_name, columns in columns_to_reset.items():
                    # 判断 columns 是否为列表，如果是则循环，否则直接处理
                    if not isinstance(columns, (list, tuple)):
                        columns = [columns]

                    for column_name in columns:
                        logger.warning(f"  ➜ 正在重置表 '{table_name}' 中的 '{column_name}' 字段...")
                        query = sql.SQL("UPDATE {table} SET {column} = NULL WHERE {column} IS NOT NULL;").format(
                            table=sql.Identifier(table_name), column=sql.Identifier(column_name)
                        )
                        cursor.execute(query)
                        affected_rows = cursor.rowcount
                        key = f"{table_name}.{column_name}"
                        results["updated_columns"][key] = f"重置了 {affected_rows} 行"
                        logger.info(f"    ➜ 操作完成，影响了 {affected_rows} 行。")
        return results
    except Exception as e:
        logger.error(f"执行 prepare_for_library_rebuild 时发生严重错误: {e}", exc_info=True)
        raise

def cleanup_deleted_media_item(item_id: str, item_name: str, item_type: str, series_id_from_webhook: Optional[str] = None):
    """
    处理一个从 Emby 中被删除的媒体项，同步清除所有相关的数据。
    """
    logger.info(f"  ➜ 检测到 Emby 媒体项被删除: '{item_name}' (Type: {item_type}, EmbyID: {item_id})，开始清理流程...")

    try:
        # ======================================================================
        # 辅助函数：执行外科手术式移除，并返回剩余的 ID 数量
        # ======================================================================
        def remove_id_from_metadata(cursor, target_emby_id):
            """
            从 media_metadata 的 JSON 数组中移除指定的 Emby ID。
            返回: (remaining_count, tmdb_id, item_type, parent_tmdb_id, season_number)
            """
            # ★★★ 修正 SQL：匹配 asset_details_json 中的 'emby_item_id' 键 ★★★
            sql_remove = """
                UPDATE media_metadata
                SET 
                    -- 1. 从 ID 列表中移除
                    emby_item_ids_json = COALESCE((
                        SELECT jsonb_agg(elem)
                        FROM jsonb_array_elements_text(emby_item_ids_json) elem
                        WHERE elem != %s
                    ), '[]'::jsonb),
                    
                    -- 2. 从详情列表中移除 (匹配 emby_item_id)
                    asset_details_json = COALESCE((
                        SELECT jsonb_agg(elem)
                        FROM jsonb_array_elements(COALESCE(asset_details_json, '[]'::jsonb)) elem
                        WHERE (elem->>'emby_item_id') IS NULL OR (elem->>'emby_item_id') != %s
                    ), '[]'::jsonb),
                    
                    last_updated_at = NOW()
                WHERE emby_item_ids_json @> %s::jsonb
                RETURNING tmdb_id, item_type, parent_series_tmdb_id, season_number, jsonb_array_length(emby_item_ids_json) as remaining_len;
            """
            # 注意参数传递顺序：ID列表移除用, 详情移除用, WHERE条件匹配用
            cursor.execute(sql_remove, (target_emby_id, target_emby_id, json.dumps([target_emby_id])))
            row = cursor.fetchone()
            
            if row:
                return row['remaining_len'], row['tmdb_id'], row['item_type'], row['parent_series_tmdb_id'], row['season_number']
            return None, None, None, None, None

        # ======================================================================
        # 开始处理
        # ======================================================================
        
        target_tmdb_id_for_full_cleanup: Optional[str] = None
        target_item_type_for_full_cleanup: Optional[str] = None

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                
                # --- 执行移除操作 ---
                remaining_count, tmdb_id, db_item_type, parent_tmdb_id, season_num = remove_id_from_metadata(cursor, item_id)

                if remaining_count is None:
                    logger.warning(f"  ➜ 在数据库中未找到包含 Emby ID {item_id} 的记录，无需清理。")
                    return

                # --- 情况 A: 还有其他版本存在 ---
                if remaining_count > 0:
                    logger.info(f"  ➜ 媒体项 '{item_name}' (TMDB: {tmdb_id}) 移除了一个版本，但仍有 {remaining_count} 个版本在库中。")
                    logger.info(f"  ➜ 仅更新了元数据，不执行下架操作。")
                    conn.commit()
                    return

                # --- 情况 B: 所有版本都已删除 (remaining_count == 0) ---
                logger.info(f"  ➜ 媒体项 '{item_name}' (TMDB: {tmdb_id}) 的所有版本均已删除，标记为“不在库中”。")
                
                # 1. 标记当前项为不在库
                cursor.execute(
                    "UPDATE media_metadata SET in_library = FALSE WHERE tmdb_id = %s AND item_type = %s",
                    (tmdb_id, db_item_type)
                )

                # 2. 根据类型决定后续逻辑
                if db_item_type in ['Movie', 'Series']:
                    # 电影或剧集整部删除了 -> 触发完全清理
                    target_tmdb_id_for_full_cleanup = tmdb_id
                    target_item_type_for_full_cleanup = db_item_type

                elif db_item_type == 'Season':
                    # 季删除了 -> 检查父剧集是否还有其他内容
                    logger.info(f"  ➜ 第 {season_num} 季已完全删除，正在检查父剧集 (TMDB: {parent_tmdb_id})...")
                    
                    # 顺便把该季下的所有 Episode 也标记为不在库 (级联处理)
                    # 注意：这里也要清空 asset_details_json，因为季都没了，集肯定也没了
                    cursor.execute(
                        "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = NULL WHERE parent_series_tmdb_id = %s AND season_number = %s AND item_type = 'Episode'",
                        (parent_tmdb_id, season_num)
                    )
                    
                    # 删除该季的洗版规则
                    cursor.execute(
                        "DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Season' AND season_number = %s",
                        (parent_tmdb_id, season_num)
                    )

                    # 检查剧集是否还有剩余集数
                    cursor.execute(
                        "SELECT COUNT(*) as count FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE",
                        (parent_tmdb_id,)
                    )
                    if cursor.fetchone()['count'] == 0:
                        logger.warning(f"  ➜ 父剧集已无任何在库分集，将触发整剧清理。")
                        target_tmdb_id_for_full_cleanup = parent_tmdb_id
                        target_item_type_for_full_cleanup = 'Series'

                elif db_item_type == 'Episode':
                    # 分集删除了 -> 检查父剧集
                    logger.info(f"  ➜ 分集已完全删除，正在检查父剧集 (TMDB: {parent_tmdb_id})...")
                    cursor.execute(
                        "SELECT COUNT(*) as count FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE",
                        (parent_tmdb_id,)
                    )
                    if cursor.fetchone()['count'] == 0:
                        logger.warning(f"  ➜ 父剧集已无任何在库分集，将触发整剧清理。")
                        target_tmdb_id_for_full_cleanup = parent_tmdb_id
                        target_item_type_for_full_cleanup = 'Series'

                conn.commit()

        # ======================================================================
        # 步骤 2: 执行统一的“完全清理” (针对整部剧/电影离线)
        # ======================================================================
        if target_tmdb_id_for_full_cleanup:
            logger.info(f"--- 开始对 TMDB ID: {target_tmdb_id_for_full_cleanup} (Type: {target_item_type_for_full_cleanup}) 执行统一清理 ---")
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. 再次确保主记录状态 (双重保险)
                    cursor.execute(
                        "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = NULL WHERE tmdb_id = %s AND item_type = %s",
                        (target_tmdb_id_for_full_cleanup, target_item_type_for_full_cleanup)
                    )

                    # 2. 清理 watchlist
                    sql_reset_watchlist = """
                        UPDATE media_metadata
                        SET watching_status = 'NONE'
                        WHERE 
                            tmdb_id = %s 
                            AND item_type = 'Series' 
                            AND watching_status != 'NONE'
                    """
                    cursor.execute(sql_reset_watchlist, (target_tmdb_id_for_full_cleanup,))
                    if cursor.rowcount > 0:
                        logger.info(f"  ➜ 已将该剧集的智能追剧状态重置为'NONE'。")

                    # 3. 清理 resubscribe_index
                    if target_item_type_for_full_cleanup == 'Movie':
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Movie'", (target_tmdb_id_for_full_cleanup,))
                    else:
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Season'", (target_tmdb_id_for_full_cleanup,))
                    
                    if cursor.rowcount > 0: 
                        logger.info(f"  ➜ 已从媒体洗版缓存中移除 {cursor.rowcount} 条记录。")

                    # 4. 清理用户权限缓存 (针对已删除的 Emby ID)
                    # 注意：这里只清理触发 webhook 的那个 ID，防止误伤
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
                    cursor.execute(sql_cleanup_user_cache, (item_id, json.dumps([item_id])))
                    
                    conn.commit()

            remove_tmdb_id_from_all_collections(target_tmdb_id_for_full_cleanup)
            logger.info(f"--- 对 TMDB ID: {target_tmdb_id_for_full_cleanup} 的完全清理已完成 ---")

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生严重数据库错误: {e}", exc_info=True)

