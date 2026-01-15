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
                # 增加 AND item_type = 'Series'，只统计剧集层级，排除季和集
                cursor.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE TRIM(watching_status) ILIKE 'Watching' OR TRIM(watching_status) ILIKE 'Pending') as watching,
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

                # 只统计 in_library = TRUE 的项目，不再计算总数
                cursor.execute("""
                    SELECT 
                        COUNT(*) as in_lib
                    FROM media_metadata 
                    WHERE subscription_sources_json @> '[{"type": "actor_subscription"}]'::jsonb
                      AND in_library = TRUE
                """)
                actor_works_row = cursor.fetchone()

                # 3. 洗版统计
                cursor.execute("SELECT COUNT(*) FROM resubscribe_index WHERE status IN ('needed', 'auto_subscribed')")
                resub_pending = cursor.fetchone()['count']

                # 4. 原生合集统计 (实时计算)
                # 逻辑：展开 collections_info 中的 TMDB ID -> 关联 media_metadata -> 筛选不在库且无订阅状态的电影
                cursor.execute("""
                    WITH expanded_ids AS (
                        -- 1. 展开所有合集的 TMDB ID，并确保是数组类型
                        SELECT 
                            emby_collection_id,
                            jsonb_array_elements_text(all_tmdb_ids_json) AS tmdb_id
                        FROM collections_info
                        WHERE all_tmdb_ids_json IS NOT NULL AND jsonb_typeof(all_tmdb_ids_json) = 'array'
                    ),
                    missing_pairs AS (
                        -- 2. 关联媒体表，找出真正缺失的项目 (Collection ID, TMDB ID) 对
                        -- 使用 LEFT JOIN 包含那些在 media_metadata 表中完全不存在的记录
                        SELECT 
                            e.emby_collection_id,
                            e.tmdb_id
                        FROM expanded_ids e
                        LEFT JOIN media_metadata m ON e.tmdb_id = m.tmdb_id AND m.item_type = 'Movie'
                        WHERE 
                            -- 核心修改：只要不在库（记录为NULL 或 in_library=FALSE），就算缺失
                            -- 不再判断 subscription_status，无论是否订阅/忽略，只要没入库都算
                            (m.in_library IS NULL OR m.in_library = FALSE)
                    )
                    SELECT 
                        (SELECT COUNT(*) FROM collections_info) as total,
                        -- 统计有多少个合集存在缺失 (按合集ID去重)
                        (SELECT COUNT(DISTINCT emby_collection_id) FROM missing_pairs) as with_missing,
                        -- 统计总共缺失多少部电影 (按TMDB ID去重，避免一部电影在多个合集中被重复计算)
                        (SELECT COUNT(DISTINCT tmdb_id) FROM missing_pairs) as missing_items;
                """)
                native_col_row = cursor.fetchone()

                # 5. 自建合集统计
                cursor.execute("""
                    SELECT id, type, generated_media_info_json 
                    FROM custom_collections 
                    WHERE status = 'active'
                """)
                active_collections = cursor.fetchall()
                
                custom_total = len(active_collections)
                custom_with_missing = 0
                custom_missing_items_set = set() # 存储 "{id}_{type}" 字符串去重

                # 5.2 收集所有需要检查的 ID (SQL查询只需要ID)
                all_tmdb_ids_to_check = set()
                for col in active_collections:
                    if col['type'] not in ['list', 'ai_recommendation_global']:
                        continue
                        
                    media_list = col['generated_media_info_json']
                    if not media_list: continue
                    
                    if isinstance(media_list, str):
                        try: media_list = json.loads(media_list)
                        except: continue
                    
                    if isinstance(media_list, list):
                        for item in media_list:
                            tid = None
                            if isinstance(item, dict): tid = item.get('tmdb_id')
                            elif isinstance(item, str): tid = item
                            
                            if tid: all_tmdb_ids_to_check.add(str(tid))

                # 5.3 批量查询在库状态 (★ 必须查 item_type ★)
                in_library_status_map = {}
                if all_tmdb_ids_to_check:
                    cursor.execute("""
                        SELECT tmdb_id, item_type, in_library 
                        FROM media_metadata 
                        WHERE tmdb_id = ANY(%s)
                    """, (list(all_tmdb_ids_to_check),))
                    
                    for row in cursor.fetchall():
                        # ★ 构造组合键：12345_Movie
                        key = f"{row['tmdb_id']}_{row['item_type']}"
                        in_library_status_map[key] = row['in_library']

                # 5.4 计算缺失 (★ 精确比对 ★)
                for col in active_collections:
                    if col['type'] not in ['list', 'ai_recommendation_global']:
                        continue
                        
                    media_list = col['generated_media_info_json']
                    if not media_list: continue
                    if isinstance(media_list, str):
                        try: media_list = json.loads(media_list)
                        except: continue
                    
                    has_missing_in_this_col = False
                    
                    for item in media_list:
                        tid = None
                        media_type = 'Movie' # 默认类型

                        if isinstance(item, dict): 
                            tid = item.get('tmdb_id')
                            media_type = item.get('media_type') or 'Movie'
                        elif isinstance(item, str): 
                            tid = item
                        
                        if not tid or str(tid).lower() == 'none': 
                            # 没有ID算缺失
                            has_missing_in_this_col = True
                            continue
                        
                        # ★ 构造目标键：12345_Series
                        target_key = f"{tid}_{media_type}"
                        
                        # 查字典：必须 ID 和 类型 都匹配，且 in_library 为 True 才算在库
                        is_in_lib = in_library_status_map.get(target_key, False)
                        
                        if not is_in_lib:
                            has_missing_in_this_col = True
                            # 加入缺失集合去重 (带类型)
                            custom_missing_items_set.add(target_key)
                    
                    if has_missing_in_this_col:
                        custom_with_missing += 1

                return {
                    'watchlist_active': watchlist_row['watching'],
                    'watchlist_paused': watchlist_row['paused'],
                    'watchlist_completed': watchlist_row['completed'],
                    
                    'actor_subscriptions_active': actor_sub_count,
                    'actor_works_in_library': actor_works_row['in_lib'],
                    
                    'resubscribe_pending': resub_pending,
                    
                    'native_collections_total': native_col_row['total'],
                    'native_collections_with_missing': native_col_row['with_missing'],
                    'native_collections_missing_items': native_col_row['missing_items'],
                    
                    'custom_collections_total': custom_total,
                    'custom_collections_with_missing': custom_with_missing,
                    'custom_collections_missing_items': len(custom_missing_items_set)
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
    【高危 - 修复版】执行为 Emby 媒体库重建做准备的所有数据库操作。
    1. 清空 Emby 专属数据表 (用户、播放状态、缓存)。
    2. 重置核心元数据表中的 Emby 关联字段 (ID、资产详情、在库状态)。
    3. 重置追剧状态。
    """
    # 1. 需要被 TRUNCATE (清空) 的表
    tables_to_truncate = [
        'emby_users', 
        'emby_users_extended', 
        'user_media_data', 
        'collections_info', 
        'resubscribe_index', 
        'cleanup_index' 
    ]

    results = {"truncated_tables": [], "updated_rows": {}}
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("第一步：开始清空 Emby 专属数据表...")
                for table_name in tables_to_truncate:
                    # 检查表是否存在，防止报错
                    cursor.execute("SELECT to_regclass(%s)", (table_name,))
                    result = cursor.fetchone()
                    if result and result.get('to_regclass'):
                        logger.warning(f"  ➜ 正在清空表: {table_name}")
                        query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(table=sql.Identifier(table_name))
                        cursor.execute(query)
                        results["truncated_tables"].append(table_name)
                    else:
                        logger.warning(f"  ➜ 表 {table_name} 不存在，跳过清空。")

                logger.info("第二步：重置 media_metadata 表中的 Emby 关联字段...")
                # ★★★ 核心修复：针对 JSONB 字段设置 '[]'，针对状态字段重置 ★★★
                cursor.execute("""
                    UPDATE media_metadata
                    SET 
                        -- 1. 核心关联字段
                        in_library = FALSE,
                        emby_item_ids_json = '[]'::jsonb,  -- 设置为空数组，而不是 NULL
                        asset_details_json = NULL,         -- 资产详情可以为 NULL
                        date_added = NULL,
                        
                        -- 2. 追剧状态重置 (库都没了，追剧状态自然要重置)
                        watching_status = 'NONE',
                        paused_until = NULL,
                        force_ended = FALSE,
                        watchlist_is_airing = FALSE,
                        watchlist_next_episode_json = NULL,
                        watchlist_missing_info_json = NULL,
                        
                        -- 3. 更新时间戳
                        last_updated_at = NOW()
                    WHERE 
                        in_library = TRUE 
                        OR emby_item_ids_json::text != '[]'
                        OR watching_status != 'NONE';
                """)
                results["updated_rows"]["media_metadata"] = cursor.rowcount
                logger.info(f"  ➜ media_metadata 表重置完成，影响了 {cursor.rowcount} 行。")

                logger.info("第三步：重置 演员映射表 (person_identity_map)...")
                cursor.execute("""
                    UPDATE person_identity_map 
                    SET emby_person_id = NULL 
                    WHERE emby_person_id IS NOT NULL;
                """)
                results["updated_rows"]["person_identity_map"] = cursor.rowcount

                logger.info("第四步：重置 自建合集表 (custom_collections)...")
                cursor.execute("""
                    UPDATE custom_collections 
                    SET 
                        emby_collection_id = NULL,
                        in_library_count = 0,
                        missing_count = 0
                    WHERE emby_collection_id IS NOT NULL;
                """)
                results["updated_rows"]["custom_collections"] = cursor.rowcount

            conn.commit()
            logger.info("  ➜ 数据库重置操作全部完成。")
            
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
                    # --- 1. 检查该季是否还有剩余集数 ---
                    cursor.execute(
                        """
                        SELECT 1 
                        FROM media_metadata 
                        WHERE parent_series_tmdb_id = %s 
                          AND season_number = %s 
                          AND item_type = 'Episode' 
                          AND in_library = TRUE
                        LIMIT 1
                        """,
                        (parent_tmdb_id, season_num)
                    )
                    has_episodes_in_season = cursor.fetchone()

                    if not has_episodes_in_season:
                        logger.info(f"  ➜ 第 {season_num} 季已无任何在库分集，标记该季为离线。")
                        
                        # A. 标记季为离线 (清空 asset_details_json)
                        cursor.execute(
                            """
                            UPDATE media_metadata 
                            SET in_library = FALSE, asset_details_json = NULL 
                            WHERE parent_series_tmdb_id = %s 
                              AND season_number = %s 
                              AND item_type = 'Season'
                            """,
                            (parent_tmdb_id, season_num)
                        )
                        
                        # B. 清理该季的洗版规则 (如果有)
                        cursor.execute(
                            """
                            DELETE FROM resubscribe_index 
                            WHERE tmdb_id = %s 
                              AND item_type = 'Season' 
                              AND season_number = %s
                            """,
                            (parent_tmdb_id, season_num)
                        )

                    # --- 2. 检查整部剧是否还有剩余集数 (原有逻辑) ---
                    logger.info(f"  ➜ 正在检查父剧集 (TMDB: {parent_tmdb_id}) 是否已空...")
                    cursor.execute(
                        """
                        SELECT 1 
                        FROM media_metadata 
                        WHERE parent_series_tmdb_id = %s 
                          AND item_type = 'Episode' 
                          AND in_library = TRUE
                        LIMIT 1
                        """,
                        (parent_tmdb_id,)
                    )
                    has_episodes_in_series = cursor.fetchone()

                    if not has_episodes_in_series:
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
                    # 1. 标记主条目离线
                    cursor.execute(
                        """
                        UPDATE media_metadata 
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL
                        WHERE tmdb_id = %s AND item_type = %s
                        """,
                        (target_tmdb_id_for_full_cleanup, target_item_type_for_full_cleanup)
                    )

                    # ★★★ 如果是剧集，必须级联标记所有子项（季、集）离线 ★★★
                    if target_item_type_for_full_cleanup == 'Series':
                        cursor.execute(
                            """
                            UPDATE media_metadata 
                            SET in_library = FALSE, 
                                emby_item_ids_json = '[]'::jsonb, 
                                asset_details_json = NULL
                            WHERE parent_series_tmdb_id = %s AND item_type IN ('Season', 'Episode')
                            """,
                            (target_tmdb_id_for_full_cleanup,)
                        )
                        logger.info(f"  ➜ 已级联标记该剧集下的 {cursor.rowcount} 个子项(季/集)为离线。")

                    # 2. 清理 watchlist (仅针对剧集)
                    if target_item_type_for_full_cleanup == 'Series':
                        sql_reset_watchlist = """
                            UPDATE media_metadata
                            SET watching_status = 'NONE'
                            WHERE tmdb_id = %s AND item_type = 'Series' AND watching_status != 'NONE'
                        """
                        cursor.execute(sql_reset_watchlist, (target_tmdb_id_for_full_cleanup,))
                        if cursor.rowcount > 0:
                            logger.info(f"  ➜ 已将该剧集从智能追剧列表移除。")

                    # 3. 清理 resubscribe_index
                    if target_item_type_for_full_cleanup == 'Movie':
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Movie'", (target_tmdb_id_for_full_cleanup,))
                    else:
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Season'", (target_tmdb_id_for_full_cleanup,))
                    
                    if cursor.rowcount > 0: 
                        logger.info(f"  ➜ 已从媒体洗版缓存中移除 {cursor.rowcount} 条记录。")

                    # 5. 清理原生合集
                    if target_item_type_for_full_cleanup == 'Movie':
                        # 1. 找出包含这部电影的所有原生合集
                        cursor.execute("""
                            SELECT emby_collection_id, name, all_tmdb_ids_json
                            FROM collections_info
                            WHERE all_tmdb_ids_json @> %s::jsonb
                        """, (json.dumps([target_tmdb_id_for_full_cleanup]),))
                        
                        affected_collections = cursor.fetchall()
                        
                        for col in affected_collections:
                            c_id = col['emby_collection_id']
                            c_name = col['name']
                            tmdb_ids = col['all_tmdb_ids_json'] # 这是一个 list
                            
                            if not tmdb_ids: continue

                            # 2. 检查该合集里是否还有其他“在库”的电影
                            # 注意：当前电影刚刚已经被标记为 in_library=FALSE 了，所以不会被统计进去
                            cursor.execute("""
                                SELECT 1 
                                FROM media_metadata 
                                WHERE tmdb_id = ANY(%s) 
                                  AND in_library = TRUE
                                LIMIT 1
                            """, (tmdb_ids,))
                            
                            has_remaining_items = cursor.fetchone()
                            
                            # 3. 如果没有剩下的了，说明合集全灭，删除记录
                            if not has_remaining_items:
                                logger.info(f"  🗑️ 原生合集 '{c_name}' (ID: {c_id}) 内所有媒体均已离线，正在自动清理该合集记录...")
                                cursor.execute("DELETE FROM collections_info WHERE emby_collection_id = %s", (c_id,))
                    
                    conn.commit()

            logger.info(f"--- 对 TMDB ID: {target_tmdb_id_for_full_cleanup} 的完全清理已完成 ---")

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生严重数据库错误: {e}", exc_info=True)

def cleanup_offline_media() -> Dict[str, int]:
    """
    【新增】清理所有“不在库”且“无订阅/追剧状态”的媒体元数据。
    用于给数据库瘦身，移除不再需要的离线缓存。
    """
    results = {
        "media_metadata_deleted": 0,
        "resubscribe_index_cleaned": 0,
        "cleanup_index_cleaned": 0
    }
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 核心清理：删除 media_metadata 中符合条件的记录
                # 条件：
                #   - in_library = FALSE (不在库)
                #   - subscription_status = 'NONE' (无订阅)
                #   - watching_status = 'NONE' (无追剧状态 - 防止误删正在追但暂时缺集的内容)
                logger.info("正在执行离线媒体清理任务...")
                
                cursor.execute("""
                    DELETE FROM media_metadata
                    WHERE in_library = FALSE
                      AND subscription_status = 'NONE'
                      AND (watching_status IS NULL OR watching_status = 'NONE');
                """)
                results["media_metadata_deleted"] = cursor.rowcount
                logger.info(f"  ➜ 已从 media_metadata 删除 {results['media_metadata_deleted']} 条无效离线记录。")

                # 2. 级联清理：清理 resubscribe_index 中的孤儿记录
                # (即：主表中已经不存在，但洗版表中还残留的记录)
                cursor.execute("""
                    DELETE FROM resubscribe_index ri
                    WHERE NOT EXISTS (
                        SELECT 1 FROM media_metadata mm
                        WHERE mm.tmdb_id = ri.tmdb_id AND mm.item_type = ri.item_type
                    );
                """)
                results["resubscribe_index_cleaned"] = cursor.rowcount
                
                # 3. 级联清理：清理 cleanup_index 中的孤儿记录
                cursor.execute("""
                    DELETE FROM cleanup_index ci
                    WHERE NOT EXISTS (
                        SELECT 1 FROM media_metadata mm
                        WHERE mm.tmdb_id = ci.tmdb_id AND mm.item_type = ci.item_type
                    );
                """)
                results["cleanup_index_cleaned"] = cursor.rowcount

            conn.commit()
            logger.info(f"离线媒体清理完成。统计: {results}")
            return results

    except Exception as e:
        logger.error(f"执行离线媒体清理时发生错误: {e}", exc_info=True)
        raise

def clear_all_vectors() -> int:
    """
    清空所有已生成的向量数据。
    场景：用户更换了 Embedding 模型，旧的向量数据不再适用，必须清除。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 仅清空 embedding 字段，保留其他元数据
            cursor.execute("UPDATE media_metadata SET overview_embedding = NULL WHERE overview_embedding IS NOT NULL")
            count = cursor.rowcount
            conn.commit()
            logger.info(f"  ✅ 已清空 {count} 条向量数据。")
            return count
    except Exception as e:
        logger.error(f"清空向量数据失败: {e}", exc_info=True)
        raise