# database/maintenance_db.py
import psycopg2
import re
import json
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import threading
from gevent import spawn_later, spawn

from .connection import get_db_connection
from .log_db import LogDBManager
from .media_db import get_tmdb_id_from_emby_id
import constants

logger = logging.getLogger(__name__)

# ======================================================================
# ★★★ 智能网盘删除缓冲池 (防并发冲突) ★★★
# ======================================================================
_SMART_DELETE_BUFFER = set()
_SMART_DELETE_TIMER = None
_SMART_DELETE_LOCK = threading.Lock()

def _queue_smart_delete(pickcodes: list, item_name: str, item_type: str):
    """将待删除的 PC 码列表加入缓冲池，5秒后统一执行智能剪枝删除"""
    global _SMART_DELETE_TIMER
    with _SMART_DELETE_LOCK:
        if pickcodes:
            for pc in pickcodes:
                if pc: _SMART_DELETE_BUFFER.add((pc, item_name, item_type))

def _flush_smart_delete():
    """执行缓冲池中的所有删除任务"""
    global _SMART_DELETE_TIMER
    with _SMART_DELETE_LOCK:
        items = list(_SMART_DELETE_BUFFER)
        _SMART_DELETE_BUFFER.clear()
        _SMART_DELETE_TIMER = None

    if not items: return

    pickcodes = [i[0] for i in items]
    # 取第一个作为日志代表
    sample_name = items[0][1]
    sample_type = items[0][2]
    
    _execute_smart_115_deletion(pickcodes, sample_name, sample_type)

def _execute_smart_115_deletion(pickcodes: List[str], sample_item_name: str, sample_item_type: str):
    """
    【核心魔法】自底向上智能剪枝删除算法。
    利用本地 p115_filesystem_cache 目录树，判断删除文件后父目录是否为空。
    如果为空，则直接删除父目录（甚至爷爷目录），只需 1 次 API 调用！
    """
    if not pickcodes: return

    try:
        from handler.p115_service import P115Service, get_config
        
        client = P115Service.get_client()
        if not client: return

        # 1. 获取防火墙 (受保护的目录 CID，绝对不能删)
        config = get_config()
        protected_cids = {'0'}
        if config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID):
            protected_cids.add(str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID)))
        if config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID):
            protected_cids.add(str(config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)))
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (constants.DB_KEY_115_SORTING_RULES,))
                row = cursor.fetchone()
                if row and row['value_json']:
                    rules = row['value_json'] if isinstance(row['value_json'], list) else json.loads(row['value_json'])
                    for r in rules:
                        if r.get('cid'): protected_cids.add(str(r['cid']))

        # 2. 自底向上寻找最优删除目标
        nodes_to_delete = set()
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 获取初始文件
                cursor.execute("SELECT id, parent_id, name FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (pickcodes,))
                files = cursor.fetchall()
                
                if not files:
                    logger.warning(f"  ⚠️ [网盘清理] 未能在本地缓存中找到对应的 PC 码，放弃本地智能删除。")
                    return

                for f in files:
                    nodes_to_delete.add(f['id'])

                parents_to_check = {f['parent_id'] for f in files}

                # 向上追溯，寻找空目录
                while parents_to_check:
                    next_parents = set()
                    for pid in parents_to_check:
                        if pid in protected_cids or pid == '0':
                            continue
                        
                        # 检查该目录下是否还有【不属于本次删除计划】的其他文件/目录
                        cursor.execute("""
                            SELECT id FROM p115_filesystem_cache 
                            WHERE parent_id = %s AND id != ALL(%s) 
                            LIMIT 1
                        """, (pid, list(nodes_to_delete)))
                        
                        has_others = cursor.fetchone()
                        
                        if not has_others:
                            # 目录空了！将父目录加入删除计划
                            nodes_to_delete.add(pid)
                            # 优化：既然要删父目录，子节点就不需要单独发 API 删了 (115是递归删除)
                            cursor.execute("SELECT id FROM p115_filesystem_cache WHERE parent_id = %s", (pid,))
                            for child in cursor.fetchall():
                                nodes_to_delete.discard(child['id'])
                                
                            # 继续向上检查爷爷目录
                            cursor.execute("SELECT parent_id FROM p115_filesystem_cache WHERE id = %s", (pid,))
                            p_info = cursor.fetchone()
                            if p_info and p_info['parent_id']:
                                next_parents.add(p_info['parent_id'])
                                
                    parents_to_check = next_parents

        # 3. 执行物理删除
        if nodes_to_delete:
            # 格式化人类友好日志
            log_title = sample_item_name.split(' - ')[0] if ' - ' in sample_item_name else sample_item_name
            if len(pickcodes) > 1:
                log_msg = f"已同步删除网盘媒体《{log_title}》等相关文件/目录 (共 {len(pickcodes)} 个视频)"
            else:
                log_msg = f"已同步删除网盘媒体《{sample_item_name}》的相关文件/目录"

            logger.info(f"  💥 [网盘清理] 锁定 {len(nodes_to_delete)} 个网盘节点(含自动追溯的空目录)，正在发送删除指令...")
            resp = client.fs_delete(list(nodes_to_delete))
            
            if resp.get('state'):
                logger.info(f"  ✅ [网盘清理] {log_msg}")
                # 4. 清理本地缓存与整理记录
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 清理整理记录
                        cursor.execute("DELETE FROM p115_organize_records WHERE pick_code = ANY(%s)", (pickcodes,))
                        # 清理目录树缓存
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (pickcodes,))
                        for nid in nodes_to_delete:
                            cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s OR parent_id = %s", (nid, nid))
                        conn.commit()
            else:
                logger.error(f"  ❌ [网盘清理] 删除失败: {resp}")

    except Exception as e:
        logger.error(f"  ❌ [网盘清理] 智能删除执行异常: {e}", exc_info=True)


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
        (SELECT COUNT(*) FROM p115_mediainfo_cache) AS mediainfo_backed_up_total,
        (SELECT COALESCE(SUM(hit_count), 0) FROM p115_mediainfo_cache) AS mediainfo_hits_total,
        (SELECT COUNT(*) FROM person_identity_map) AS actor_mappings_total
    """
    return _execute_single_row_query(sql)

def get_stats_library() -> dict:
    """2. 媒体库概览 (较快)"""
    sql = """
    SELECT
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Movie' AND in_library = TRUE) AS media_movies_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Series' AND in_library = TRUE) AS media_series_in_library,
        (SELECT COUNT(*) FROM media_metadata WHERE item_type = 'Episode' AND in_library = TRUE) AS media_episodes_in_library
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
    2. 重置核心元数据表中的 Emby 关联字段 (ID、资产详情、在库状态、指纹信息)。
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
                cursor.execute("""
                    UPDATE media_metadata
                    SET 
                        -- 1. 核心关联字段
                        in_library = FALSE,
                        emby_item_ids_json = '[]'::jsonb,  
                        file_sha1_json = '[]'::jsonb,      
                        file_pickcode_json = '[]'::jsonb,  
                        asset_details_json = NULL,         
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
                        in_library_count = 0
                    WHERE emby_collection_id IS NOT NULL;
                """)
                results["updated_rows"]["custom_collections"] = cursor.rowcount

            conn.commit()
            logger.info("  ➜ 数据库重置操作全部完成。")
            
        return results
    except Exception as e:
        logger.error(f"执行 prepare_for_library_rebuild 时发生严重错误: {e}", exc_info=True)
        raise

def cleanup_deleted_media_item(item_id: str, item_name: str, item_type: str, series_id_from_webhook: Optional[str] = None, webhook_pcs: List[str] = None) -> Optional[Dict[str, Any]]:
    """
    处理一个从 Emby 中被删除的媒体项，同步清除所有相关的数据。
    """
    logger.info(f"  ➜ 检测到 Emby 媒体项被删除: '{item_name}' (Type: {item_type}, EmbyID: {item_id})，开始清理流程...")
    collected_pcs = set(webhook_pcs) if webhook_pcs else set()

    try:
        # ======================================================================
        # ★★★ 核心解绑：第一步，无脑清理该特定版本的专属日志和内存缓存 ★★★
        # ======================================================================
        try:
            import extensions
            processor = extensions.media_processor_instance
            if processor:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    processor.log_db_manager.remove_from_processed_log(cursor, item_id)
                    processor.log_db_manager.remove_from_failed_log(cursor, item_id)
                    conn.commit()
                
                if item_id in processor.processed_items_cache:
                    del processor.processed_items_cache[item_id]
                    logger.debug(f"  🧹 [深度删除] 已精准抹除该版本的专属日志与内存缓存: EmbyID {item_id}")
        except Exception as e:
            logger.error(f"  ❌ 清理专属日志缓存失败: {e}")
            
        # ======================================================================
        # 辅助函数：执行外科手术式移除，并返回剩余的 ID 数量和被删除的 PC 码
        # ======================================================================
        def remove_id_from_metadata(cursor, target_emby_id):
            cursor.execute("""
                SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number,
                       emby_item_ids_json, asset_details_json, file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE emby_item_ids_json @> %s::jsonb
                FOR UPDATE
            """, (json.dumps([target_emby_id]),))
            row = cursor.fetchone()

            if not row:
                return None, None, None, None, None, None

            emby_ids = row['emby_item_ids_json'] if isinstance(row['emby_item_ids_json'], list) else json.loads(row['emby_item_ids_json'] or '[]')
            
            if not isinstance(emby_ids, list) or target_emby_id not in emby_ids:
                return None, None, None, None, None, None

            assets = row['asset_details_json'] if isinstance(row['asset_details_json'], list) else json.loads(row['asset_details_json'] or '[]')
            sha1s = row['file_sha1_json'] if isinstance(row['file_sha1_json'], list) else json.loads(row['file_sha1_json'] or '[]')
            pcs = row['file_pickcode_json'] if isinstance(row['file_pickcode_json'], list) else json.loads(row['file_pickcode_json'] or '[]')

            idx = emby_ids.index(target_emby_id)
            emby_ids.pop(idx)
            
            if isinstance(assets, list) and idx < len(assets): assets.pop(idx)
            if isinstance(sha1s, list) and idx < len(sha1s): sha1s.pop(idx)
            
            # ★ 核心：捕获被删除的 PC 码
            deleted_pc = None
            if isinstance(pcs, list) and idx < len(pcs): 
                deleted_pc = pcs.pop(idx)

            cursor.execute("""
                UPDATE media_metadata
                SET emby_item_ids_json = %s::jsonb,
                    asset_details_json = %s::jsonb,
                    file_sha1_json = %s::jsonb,
                    file_pickcode_json = %s::jsonb,
                    last_updated_at = NOW()
                WHERE tmdb_id = %s AND item_type = %s
            """, (
                json.dumps(emby_ids, ensure_ascii=False),
                json.dumps(assets, ensure_ascii=False) if assets else None, 
                json.dumps(sha1s, ensure_ascii=False),
                json.dumps(pcs, ensure_ascii=False),
                row['tmdb_id'], row['item_type']
            ))

            return len(emby_ids), row['tmdb_id'], row['item_type'], row['parent_series_tmdb_id'], row['season_number'], deleted_pc

        # ======================================================================
        # 开始处理
        # ======================================================================
        
        target_tmdb_id_for_full_cleanup: Optional[str] = None
        target_item_type_for_full_cleanup: Optional[str] = None
        cascaded_cleanup_info = None
        captured_deleted_pc = None
        collected_pcs = set()

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                
                # --- 执行移除操作 ---
                remaining_count, tmdb_id, db_item_type, parent_tmdb_id, season_num, captured_deleted_pc = remove_id_from_metadata(cursor, item_id)
                if captured_deleted_pc:
                    collected_pcs.add(captured_deleted_pc)

                if remaining_count is None:
                    logger.warning(f"  ➜ 在数据库中未找到包含 Emby ID {item_id} 的记录，无需清理。")
                    return None

                # --- 情况 A: 还有其他版本存在 ---
                if remaining_count > 0:
                    logger.info(f"  ➜ 媒体项 '{item_name}' (TMDB: {tmdb_id}) 移除了一个版本，但仍有 {remaining_count} 个版本在库中。")
                    conn.commit()
                    # ★ 如果开启了联动删除，即使只是删了一个版本，也要把这个版本的网盘文件删掉
                    if captured_deleted_pc:
                        from handler.p115_service import get_config
                        if get_config().get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False):
                            _queue_smart_delete(captured_deleted_pc, item_name, item_type)
                    return None

                # --- 情况 B: 所有版本都已删除 (remaining_count == 0) ---
                logger.info(f"  ➜ 媒体项 '{item_name}' (TMDB: {tmdb_id}) 的所有版本均已删除，标记为“不在库中”。")
                
                # 1. 标记当前项为不在库
                cursor.execute(
                    "UPDATE media_metadata SET in_library = FALSE WHERE tmdb_id = %s AND item_type = %s",
                    (tmdb_id, db_item_type)
                )

                # 2. 根据类型决定后续逻辑
                if db_item_type in ['Movie', 'Series']:
                    target_tmdb_id_for_full_cleanup = tmdb_id
                    target_item_type_for_full_cleanup = db_item_type

                elif db_item_type == 'Season':
                    logger.info(f"  ➜ 第 {season_num} 季已完全删除，正在检查父剧集 (TMDB: {parent_tmdb_id})...")
                    
                    # ★ 提前捞取该季所有分集的 PC 码
                    cursor.execute("SELECT file_pickcode_json FROM media_metadata WHERE parent_series_tmdb_id = %s AND season_number = %s AND item_type = 'Episode'", (parent_tmdb_id, season_num))
                    for r in cursor.fetchall():
                        pcs = r['file_pickcode_json'] if isinstance(r['file_pickcode_json'], list) else json.loads(r['file_pickcode_json'] or '[]')
                        collected_pcs.update(pcs)
                    
                    # ★ 终极兜底：从目录树缓存中按 TMDB ID 暴力捞取
                    cursor.execute("SELECT pick_code FROM p115_filesystem_cache WHERE local_path LIKE %s AND pick_code IS NOT NULL", (f"%{{tmdb={parent_tmdb_id}}}%",))
                    for r in cursor.fetchall():
                        collected_pcs.add(r['pick_code'])

                    cursor.execute(
                        """
                        UPDATE media_metadata 
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL,
                            file_sha1_json = '[]'::jsonb,
                            file_pickcode_json = '[]'::jsonb
                        WHERE parent_series_tmdb_id = %s AND season_number = %s AND item_type = 'Episode'
                        """,
                        (parent_tmdb_id, season_num)
                    )
                    
                    cursor.execute(
                        "DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Season' AND season_number = %s",
                        (parent_tmdb_id, season_num)
                    )

                    cursor.execute(
                        "SELECT COUNT(*) as count FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE",
                        (parent_tmdb_id,)
                    )
                    if cursor.fetchone()['count'] == 0:
                        logger.warning(f"  ➜ 父剧集已无任何在库分集，将触发整剧清理。")
                        target_tmdb_id_for_full_cleanup = parent_tmdb_id
                        target_item_type_for_full_cleanup = 'Series'

                elif db_item_type == 'Episode':
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
                        cursor.execute(
                            """
                            UPDATE media_metadata 
                            SET in_library = FALSE, 
                                asset_details_json = NULL,
                                file_sha1_json = '[]'::jsonb,
                                file_pickcode_json = '[]'::jsonb
                            WHERE parent_series_tmdb_id = %s 
                              AND season_number = %s 
                              AND item_type = 'Season'
                            """,
                            (parent_tmdb_id, season_num)
                        )
                        cursor.execute(
                            """
                            DELETE FROM resubscribe_index 
                            WHERE tmdb_id = %s 
                              AND item_type = 'Season' 
                              AND season_number = %s
                            """,
                            (parent_tmdb_id, season_num)
                        )

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

                # ======================================================================
                # 步骤 2: 执行统一的“完全清理” (针对整部剧/电影离线)
                # ======================================================================
                if target_tmdb_id_for_full_cleanup:
                    logger.info(f"--- 开始对 TMDB ID: {target_tmdb_id_for_full_cleanup} (Type: {target_item_type_for_full_cleanup}) 执行统一清理 ---")
                    
                    cursor.execute(
                        "SELECT title, emby_item_ids_json FROM media_metadata WHERE tmdb_id = %s AND item_type = %s",
                        (target_tmdb_id_for_full_cleanup, target_item_type_for_full_cleanup)
                    )
                    row = cursor.fetchone()
                    item_title = row['title'] if row and row['title'] else "未知标题"
                    parent_emby_ids = []
                    if row and row['emby_item_ids_json']:
                        raw_ids = row['emby_item_ids_json']
                        if isinstance(raw_ids, list):
                            parent_emby_ids = raw_ids
                        elif isinstance(raw_ids, str):
                            try:
                                parent_emby_ids = json.loads(raw_ids)
                            except Exception as e:
                                logger.warning(f"解析 Emby IDs JSON 失败: {e}")
                    
                    if not isinstance(parent_emby_ids, list):
                        parent_emby_ids = []
                    
                    cascaded_cleanup_info = {
                        'tmdb_id': target_tmdb_id_for_full_cleanup,
                        'item_type': target_item_type_for_full_cleanup,
                        'item_name': item_title,
                        'emby_ids': parent_emby_ids
                    }

                    cursor.execute(
                        """
                        UPDATE media_metadata 
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL,
                            file_sha1_json = '[]'::jsonb,
                            file_pickcode_json = '[]'::jsonb
                        WHERE tmdb_id = %s AND item_type = %s
                        """,
                        (target_tmdb_id_for_full_cleanup, target_item_type_for_full_cleanup)
                    )

                    if target_item_type_for_full_cleanup == 'Series':
                        # ★ 提前捞取全剧所有分集的 PC 码
                        cursor.execute("SELECT file_pickcode_json FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode'", (target_tmdb_id_for_full_cleanup,))
                        for r in cursor.fetchall():
                            pcs = r['file_pickcode_json'] if isinstance(r['file_pickcode_json'], list) else json.loads(r['file_pickcode_json'] or '[]')
                            collected_pcs.update(pcs)
                            
                        # ★ 终极兜底：从目录树缓存中按 TMDB ID 暴力捞取
                        cursor.execute("SELECT pick_code FROM p115_filesystem_cache WHERE local_path LIKE %s AND pick_code IS NOT NULL", (f"%{{tmdb={target_tmdb_id_for_full_cleanup}}}%",))
                        for r in cursor.fetchall():
                            collected_pcs.add(r['pick_code'])

                        cursor.execute(
                            """
                            UPDATE media_metadata 
                            SET in_library = FALSE, 
                                emby_item_ids_json = '[]'::jsonb, 
                                asset_details_json = NULL,
                                file_sha1_json = '[]'::jsonb,
                                file_pickcode_json = '[]'::jsonb
                            WHERE parent_series_tmdb_id = %s AND item_type IN ('Season', 'Episode')
                            """,
                            (target_tmdb_id_for_full_cleanup,)
                        )
                        logger.info(f"  ➜ 已级联标记该剧集下的 {cursor.rowcount} 个子项(季/集)为离线。")

                    if target_item_type_for_full_cleanup == 'Series':
                        sql_reset_watchlist = """
                            UPDATE media_metadata
                            SET watching_status = 'NONE'
                            WHERE tmdb_id = %s AND item_type = 'Series' AND watching_status != 'NONE'
                        """
                        cursor.execute(sql_reset_watchlist, (target_tmdb_id_for_full_cleanup,))
                        if cursor.rowcount > 0:
                            logger.info(f"  ➜ 已将该剧集从智能追剧列表移除。")

                    if target_item_type_for_full_cleanup == 'Movie':
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Movie'", (target_tmdb_id_for_full_cleanup,))
                    else:
                        cursor.execute("DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = 'Season'", (target_tmdb_id_for_full_cleanup,))
                    
                    if cursor.rowcount > 0: 
                        logger.info(f"  ➜ 已从媒体洗版缓存中移除 {cursor.rowcount} 条记录。")

                    if target_item_type_for_full_cleanup == 'Movie':
                        cursor.execute("""
                            SELECT emby_collection_id, name, all_tmdb_ids_json
                            FROM collections_info
                            WHERE all_tmdb_ids_json @> %s::jsonb
                        """, (json.dumps([target_tmdb_id_for_full_cleanup]),))
                        
                        affected_collections = cursor.fetchall()
                        
                        for col in affected_collections:
                            c_id = col['emby_collection_id']
                            c_name = col['name']
                            tmdb_ids = col['all_tmdb_ids_json']
                            
                            if not tmdb_ids: continue

                            cursor.execute("""
                                SELECT 1 
                                FROM media_metadata 
                                WHERE tmdb_id = ANY(%s) 
                                  AND in_library = TRUE
                                LIMIT 1
                            """, (tmdb_ids,))
                            
                            has_remaining_items = cursor.fetchone()
                            
                            if not has_remaining_items:
                                logger.info(f"  🗑️ 原生合集 '{c_name}' (ID: {c_id}) 内所有媒体均已离线，正在自动清理该合集记录...")
                                cursor.execute("DELETE FROM collections_info WHERE emby_collection_id = %s", (c_id,))
                    
                    if target_item_type_for_full_cleanup in ['Movie', 'Series']:
                        try:
                            import config_manager
                            from handler.custom_collection import RecommendationEngine
                            
                            if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_ENABLED) and config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AI_VECTOR):
                                spawn(RecommendationEngine.refresh_cache)
                                logger.debug(f"  ➜ [智能推荐] 检测到顶层媒体完全删除，已触发向量缓存刷新。")
                        except Exception as e:
                            logger.error(f"  ❌ 刷新向量缓存失败: {e}", exc_info=True)

                    logger.info(f"--- 对 TMDB ID: {target_tmdb_id_for_full_cleanup} 的完全清理已完成 ---")

                conn.commit()

        # ======================================================================
        # ★★★ 终极联动：将捕获到的所有 PC 码送入智能网盘删除缓冲池 ★★★
        # ======================================================================
        if collected_pcs:
            from handler.p115_service import get_config
            if get_config().get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False):
                _queue_smart_delete(list(collected_pcs), item_name, item_type)

        return cascaded_cleanup_info

    except Exception as e:
        logger.error(f"清理被删除的媒体项 {item_id} 时发生严重数据库错误: {e}", exc_info=True)
        return None

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
                logger.info("正在执行离线媒体清理任务...")
                
                cursor.execute("""
                    DELETE FROM media_metadata
                    WHERE in_library = FALSE
                      AND subscription_status = 'NONE'
                      AND (watching_status IS NULL OR watching_status = 'NONE');
                """)
                results["media_metadata_deleted"] = cursor.rowcount
                logger.info(f"  ➜ 已从 media_metadata 删除 {results['media_metadata_deleted']} 条无效离线记录。")

                cursor.execute("""
                    DELETE FROM resubscribe_index ri
                    WHERE NOT EXISTS (
                        SELECT 1 FROM media_metadata mm
                        WHERE mm.tmdb_id = ri.tmdb_id AND mm.item_type = ri.item_type
                    );
                """)
                results["resubscribe_index_cleaned"] = cursor.rowcount
                
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
            cursor.execute("UPDATE media_metadata SET overview_embedding = NULL WHERE overview_embedding IS NOT NULL")
            count = cursor.rowcount
            conn.commit()
            logger.info(f"  ✅ 已清空 {count} 条向量数据。")
            return count
    except Exception as e:
        logger.error(f"清空向量数据失败: {e}", exc_info=True)
        raise