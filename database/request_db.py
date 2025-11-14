# database/request_db.py
import psycopg2
import logging
from typing import List, Dict, Any, Optional, Tuple
import json

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 媒体订阅管理 (基于 media_metadata 表)
# ======================================================================

def add_subscription_request(
    tmdb_id: str, 
    item_type: str, 
    user_info: Dict[str, Any], 
    media_info: Dict[str, Any]
) -> bool:
    """
    【新】为媒体项目添加一个用户订阅请求。
    这会创建一个新的 media_metadata 条目（如果不存在），
    然后将用户信息追加到 subscription_sources_json 数组，
    并将状态设置为 'REQUESTED'。

    :param tmdb_id: 媒体的 TMDB ID。
    :param item_type: 媒体类型 ('Movie', 'Series')。
    :param user_info: 请求用户的字典，例如 {'user_id': '...', 'username': '...'}。
    :param media_info: 媒体的基本信息，用于创建条目，例如 {'title': '...', 'release_year': 2023}。
    :return: 操作是否成功。
    """
    # 确保核心媒体信息存在
    title = media_info.get('title', 'N/A')
    release_year = media_info.get('release_year')

    # SQL UPSERT (INSERT ON CONFLICT UPDATE)
    # 1. 尝试插入新记录。
    # 2. 如果 (tmdb_id, item_type) 已存在，则执行更新。
    # 3. 更新逻辑：
    #    - 将新用户追加到 subscription_sources_json 数组 (仅当用户不存在时)。
    #    - 将 subscription_status 设置为 'REQUESTED'。
    #    - 如果是第一次被请求，则设置 first_requested_at。
    sql = """
        INSERT INTO media_metadata (
            tmdb_id, item_type, title, release_year,
            subscription_status, subscription_sources_json, first_requested_at, last_synced_at
        )
        VALUES (%s, %s, %s, %s, 'REQUESTED', %s, NOW(), NOW())
        ON CONFLICT (tmdb_id, item_type) DO UPDATE
        SET
            subscription_status = 'REQUESTED',
            -- 使用 jsonb_set 和 COALESCE 来安全地追加到 JSONB 数组
            subscription_sources_json = COALESCE(media_metadata.subscription_sources_json, '[]'::jsonb) || %s,
            -- 只有当 first_requested_at 为空时才更新它
            first_requested_at = COALESCE(media_metadata.first_requested_at, NOW()),
            last_synced_at = NOW()
        -- WHERE子句防止重复添加同一个用户
        WHERE NOT (media_metadata.subscription_sources_json @> %s);
    """
    user_json = json.dumps([user_info])
    user_check_json = json.dumps([{"user_id": user_info.get("user_id")}])

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_id, item_type, title, release_year, user_json, user_json, user_check_json))
                conn.commit()
                return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 创建订阅请求 (TMDb ID: {tmdb_id}) 失败: {e}", exc_info=True)
        raise

def get_pending_subscription_requests() -> List[Dict[str, Any]]:
    """
    【新】查询所有待处理的订阅请求。
    一个媒体项目可能被多个用户请求，这里会将它们展开成独立的请求记录。
    """
    # 使用 jsonb_to_recordset 将 JSON 数组中的每个对象展开为一行
    sql = """
        SELECT
            m.tmdb_id,
            m.item_type,
            m.title AS item_name,
            u.user_id AS emby_user_id,
            u.username,
            u.requested_at
        FROM
            media_metadata m,
            jsonb_to_recordset(m.subscription_sources_json) AS u(user_id TEXT, username TEXT, requested_at TIMESTAMP WITH TIME ZONE)
        WHERE
            m.subscription_status = 'REQUESTED'
        ORDER BY
            u.requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询待审订阅列表失败: {e}", exc_info=True)
        raise

def update_media_subscription_status(
    tmdb_id: str, 
    item_type: str, 
    status: str, 
    reason: Optional[str] = None
) -> bool:
    """
    【新】更新媒体的订阅状态 (例如：批准或拒绝)。
    - 批准: 'SUBSCRIBED'
    - 拒绝: 'IGNORED'
    """
    sql_parts = ["UPDATE media_metadata SET subscription_status = %s, last_synced_at = NOW()"]
    params = [status]

    if status == 'SUBSCRIBED':
        sql_parts.append("last_subscribed_at = NOW()")
    elif status == 'IGNORED' and reason:
        sql_parts.append("ignore_reason = %s")
        params.append(reason)
        # 拒绝时清空请求者列表
        sql_parts.append("subscription_sources_json = '[]'::jsonb")

    sql_parts.append("WHERE tmdb_id = %s AND item_type = %s")
    params.extend([tmdb_id, item_type])
    
    sql = ", ".join(sql_parts)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                conn.commit()
                return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 更新媒体 (TMDb ID: {tmdb_id}) 状态失败: {e}", exc_info=True)
        raise

def batch_update_media_subscription_status(
    media_keys: List[Tuple[str, str]], 
    status: str, 
    reason: Optional[str] = None
) -> int:
    """
    【新】批量更新多个媒体的订阅状态。
    :param media_keys: 一个元组列表，每个元组为 (tmdb_id, item_type)。
    """
    if not media_keys:
        return 0

    # PostgreSQL 不支持直接在 WHERE IN 中使用元组列表，需要转换
    # WHERE (tmdb_id, item_type) IN (('1', 'Movie'), ('2', 'Series'))
    placeholders = ','.join(['%s'] * len(media_keys))
    flat_params = [item for tpl in media_keys for item in tpl]

    sql_parts = ["UPDATE media_metadata SET subscription_status = %s, last_synced_at = NOW()"]
    params = [status]

    if status == 'IGNORED' and reason:
        sql_parts.append("ignore_reason = %s")
        params.append(reason)
        sql_parts.append("subscription_sources_json = '[]'::jsonb")

    # 构建 WHERE (col1, col2) IN (...) 子句
    where_clause = f"WHERE (tmdb_id, item_type) IN (VALUES {placeholders})"
    sql_parts.append(where_clause)
    params.extend(flat_params)

    sql = ", ".join(sql_parts)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                updated_count = cursor.rowcount
                conn.commit()
                return updated_count
    except Exception as e:
        logger.error(f"DB: 批量更新媒体状态失败: {e}", exc_info=True)
        raise

def get_user_subscription_history(user_id: str, page: int = 1, page_size: int = 10) -> Tuple[List[Dict[str, Any]], int]:
    """
    【新】获取指定用户的订阅请求历史，支持分页。
    通过查询 subscription_sources_json 字段实现。
    """
    offset = (page - 1) * page_size
    
    # 使用 JSONB 操作符 @> 来查找包含特定用户ID的记录
    user_filter = json.dumps([{'user_id': user_id}])

    count_sql = f"""
        SELECT COUNT(*) FROM media_metadata
        WHERE subscription_sources_json @> '{user_filter}';
    """
    
    data_sql = f"""
        SELECT 
            tmdb_id, 
            item_type, 
            title as item_name, 
            subscription_status as status, 
            first_requested_at as requested_at, 
            ignore_reason as notes
        FROM media_metadata
        WHERE subscription_sources_json @> '{user_filter}'
        ORDER BY first_requested_at DESC
        LIMIT %s OFFSET %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(count_sql)
                total_records = cursor.fetchone()['count']
                
                cursor.execute(data_sql, (page_size, offset))
                history = [dict(row) for row in cursor.fetchall()]
                
                return history, total_records
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅历史失败: {e}", exc_info=True)
        raise

def get_user_subscription_statuses_by_tmdb_ids(tmdb_ids: List[str], user_id: str) -> Dict[str, str]:
    """
    【新增】根据 TMDb ID 列表和指定的用户ID，查询该用户对每个媒体的订阅状态。
    返回一个字典，键为 tmdb_id，值为该用户的具体状态 ('REQUESTED', 'SUBSCRIBED', 'NONE' 等)。
    """
    if not tmdb_ids or not user_id:
        return {}

    # 这个查询稍微复杂一些：
    # 1. 我们只关心那些 subscription_sources_json 字段包含我们目标 user_id 的行。
    #    这通过 WHERE subscription_sources_json @> ... 来高效筛选。
    # 2. 对于筛选出的行，我们直接返回它的 tmdb_id 和全局的 subscription_status。
    #    因为如果一个用户在请求列表里，那么这个媒体的状态 ('REQUESTED', 'SUBSCRIBED' 等) 对他就是有效的。
    sql = """
        SELECT
            m.tmdb_id,
            m.subscription_status
        FROM
            media_metadata m
        WHERE
            m.tmdb_id = ANY(%s)
            AND m.subscription_sources_json @> %s::jsonb;
    """
    
    status_map = {}
    # 构建用于 JSONB 查询的用户过滤器
    user_filter = json.dumps([{'user_id': user_id}])

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_ids, user_filter))
                rows = cursor.fetchall()
                for row in rows:
                    status_map[row['tmdb_id']] = row['subscription_status']
    except Exception as e:
        logger.error(f"DB: 批量查询用户 {user_id} 的订阅状态失败: {e}", exc_info=True)
    
    return status_map

def get_global_subscription_statuses_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, str]:
    """
    【新】根据 TMDb ID 列表，高效查询每个ID的订阅状态。
    返回一个字典，键为 tmdb_id，值为简化后的状态 ('SUBSCRIBED', 'REQUESTED', 'NONE')。
    """
    if not tmdb_ids:
        return {}

    sql = """
        SELECT tmdb_id, subscription_status
        FROM media_metadata
        WHERE tmdb_id = ANY(%s);
    """
    status_map = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_ids,))
                rows = cursor.fetchall()
                for row in rows:
                    status_map[row['tmdb_id']] = row['subscription_status']
    except Exception as e:
        logger.error(f"DB: 批量查询 TMDb IDs 的全局状态失败: {e}", exc_info=True)
    
    return status_map

def get_subscribers_by_tmdb_id(tmdb_id: str, item_type: str) -> List[Dict[str, Any]]:
    """
    【新】根据 TMDb ID 和类型查询所有订阅了该媒体的用户信息。
    """
    if not tmdb_id or not item_type:
        return []
    
    sql = """
        SELECT subscription_sources_json
        FROM media_metadata
        WHERE tmdb_id = %s AND item_type = %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_id, item_type))
                result = cursor.fetchone()
                if result and result['subscription_sources_json']:
                    return result['subscription_sources_json']
                return []
    except Exception as e:
        logger.error(f"DB: 根据 TMDb ID [{tmdb_id}] 查询订阅者失败: {e}", exc_info=True)
        return []

def get_global_subscription_status_by_tmdb_id(tmdb_id: str, item_type: str) -> Optional[str]:
    """
    【新】查询单个 TMDb ID 的订阅状态。
    """
    if not tmdb_id or not item_type:
        return None

    sql = """
        SELECT subscription_status
        FROM media_metadata
        WHERE tmdb_id = %s AND item_type = %s;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_id, item_type))
                result = cursor.fetchone()
                return result['subscription_status'] if result else 'NONE'
    except Exception as e:
        logger.error(f"DB: 查询 TMDb ID {tmdb_id} 的全局状态失败: {e}", exc_info=True)
        return None