# database/request_db.py
import psycopg2
import logging
from typing import List, Dict, Any, Optional, Union, Tuple, Set
import json

from .connection import get_db_connection
import utils

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 媒体订阅管理 (基于 media_metadata 表)
# ======================================================================
def _prepare_media_data_for_upsert(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None,
    ignore_reason: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    内部辅助函数：标准化输入并准备用于批量插入/更新的数据。
    """
    if isinstance(tmdb_ids, str):
        id_list = [tmdb_ids]
    elif isinstance(tmdb_ids, list):
        id_list = tmdb_ids
    else:
        logger.warning("准备数据失败：tmdb_ids 必须是字符串或列表。")
        return []

    data_to_upsert = []
    media_info_map = {info['tmdb_id']: info for info in media_info_list} if media_info_list else {}
    
    for tmdb_id in id_list:
        media_info = media_info_map.get(tmdb_id, {})
        data_to_upsert.append({
            "tmdb_id": tmdb_id, "item_type": item_type,
            "source": json.dumps([source]) if source else '[]',
            "reason": ignore_reason, "title": media_info.get('title'),
            "original_title": media_info.get('original_title'),
            "release_date": media_info.get('release_date') or None,
            "poster_path": media_info.get('poster_path'),
            "season_number": media_info.get('season_number') or None,
            "parent_series_tmdb_id": media_info.get('parent_series_tmdb_id') or None,
            "overview": media_info.get('overview')
        })
    return data_to_upsert

def set_media_status_requested(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None
):
    """
    将媒体状态设置为 'REQUESTED'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'REQUESTED'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at, 
                        title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, 'REQUESTED', %(source)s::jsonb, NOW(),
                        %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'REQUESTED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id
                    WHERE media_metadata.in_library = FALSE AND media_metadata.subscription_status = 'NONE';
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'REQUESTED' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_wanted(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None,
    force_unignore: bool = False
):
    """
    将媒体状态设置为 'WANTED'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'WANTED'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                if force_unignore:
                    sql = """
                        UPDATE media_metadata SET 
                            subscription_status = 'WANTED',
                            subscription_sources_json = subscription_sources_json || %(source)s::jsonb,
                            ignore_reason = NULL, last_synced_at = NOW()
                        WHERE tmdb_id = %(tmdb_id)s AND item_type = %(item_type)s AND subscription_status = 'IGNORED';
                    """
                    execute_batch(cursor, sql, data_to_upsert)
                else:
                    sql = """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at,
                            title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview
                        ) VALUES (
                            %(tmdb_id)s, %(item_type)s, 'WANTED', %(source)s::jsonb, NOW(),
                            %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'WANTED',
                            subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                            first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                            ignore_reason = NULL,
                            parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id
                        WHERE (media_metadata.in_library = FALSE OR (media_metadata.item_type = 'Series' AND EXCLUDED.subscription_sources_json->0->>'reason' LIKE 'missing_%%season'))
                          AND media_metadata.subscription_status NOT IN ('SUBSCRIBED', 'IGNORED');
                    """
                    execute_batch(cursor, sql, data_to_upsert)
                
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'WANTED' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_pending_release(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None
):
    """
    将媒体状态设置为 'PENDING_RELEASE'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'PENDING_RELEASE'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at,
                        title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, 'PENDING_RELEASE', %(source)s::jsonb, NOW(),
                        %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'PENDING_RELEASE',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        ignore_reason = NULL,
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id
                    WHERE media_metadata.in_library = FALSE AND media_metadata.subscription_status NOT IN ('SUBSCRIBED', 'WANTED');
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'PENDING_RELEASE' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_subscribed(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None
):
    """
    将媒体状态设置为 'SUBSCRIBED'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'SUBSCRIBED'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at,
                        title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, 'SUBSCRIBED', %(source)s::jsonb, NOW(),
                        %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'SUBSCRIBED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        last_synced_at = NOW(),
                        ignore_reason = NULL,
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id
                    WHERE media_metadata.in_library = FALSE;
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'SUBSCRIBED' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_ignored(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None,
    ignore_reason: Optional[str] = None
):
    """
    将媒体状态设置为 'IGNORED'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list, ignore_reason)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'IGNORED'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, subscription_status, subscription_sources_json, ignore_reason,
                        title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, 'IGNORED', %(source)s::jsonb, %(reason)s,
                        %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'IGNORED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        ignore_reason = EXCLUDED.ignore_reason,
                        last_synced_at = NOW(),
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id
                    WHERE media_metadata.in_library = FALSE;
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'IGNORED' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_none(
    tmdb_ids: Union[str, List[str]], 
    item_type: str
):
    """
    将媒体状态设置为 'NONE'。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'NONE'...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    UPDATE media_metadata
                    SET
                        subscription_status = 'NONE',
                        subscription_sources_json = '[]'::jsonb,
                        ignore_reason = NULL,
                        last_synced_at = NOW()
                    WHERE
                        tmdb_id = %(tmdb_id)s AND item_type = %(item_type)s;
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [状态执行] 成功，影响了 {cursor.rowcount} 行。")
                else:
                    logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'NONE' 时发生错误: {e}", exc_info=True)
        raise

def get_pending_requests_for_admin() -> List[Dict[str, Any]]:
    """获取所有待审批 (REQUESTED) 的订阅请求，并关联用户名。"""
    sql = """
        SELECT 
            m.tmdb_id, m.item_type, m.title, m.release_date, m.poster_path,
            m.first_requested_at as requested_at,
            (m.subscription_sources_json -> 0 ->> 'user_id') as emby_user_id,
            u.name as username
        FROM media_metadata m
        LEFT JOIN emby_users u ON (m.subscription_sources_json -> 0 ->> 'user_id') = u.id
        WHERE m.subscription_status = 'REQUESTED'
        ORDER BY m.first_requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询待审订阅列表失败: {e}", exc_info=True)
        return []
    
def get_global_request_status_by_tmdb_id(tmdb_id: str) -> Optional[str]:
    """查询单个 TMDb ID 的全局请求/订阅状态。"""
    sql = "SELECT subscription_status FROM media_metadata WHERE tmdb_id = %s LIMIT 1;"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_id,))
            result = cursor.fetchone()
            if not result: return None
            
            status = result['subscription_status']
            if status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE']: return 'approved'
            if status == 'REQUESTED': return 'pending'
            return None
    except Exception as e:
        logger.error(f"DB: 查询 TMDb ID {tmdb_id} 的全局状态失败: {e}", exc_info=True)
        return None

def get_all_in_library_media_for_actor_sync() -> Tuple[Dict[str, str], Dict[str, Set[int]], Dict[str, str]]:
    """
    为演员订阅任务，一次性从 media_metadata 表中提取所有需要的数据。
    返回三个核心映射:
    1. emby_media_map: {tmdb_id: emby_id}
    2. emby_series_seasons_map: {series_tmdb_id: {season_number, ...}}
    3. emby_series_name_to_tmdb_id_map: {normalized_name: tmdb_id}
    """
    emby_media_map = {}
    emby_series_seasons_map = {}
    emby_series_name_to_tmdb_id_map = {}

    # SQL 查询所有在库的、顶层的电影和剧集
    sql = """
        SELECT tmdb_id, item_type, title, emby_item_ids_json 
        FROM media_metadata 
        WHERE in_library = TRUE AND item_type IN ('Movie', 'Series');
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                top_level_items = cursor.fetchall()

                series_tmdb_ids = []
                for item in top_level_items:
                    tmdb_id = str(item['tmdb_id'])
                    emby_ids = item.get('emby_item_ids_json')
                    
                    # 我们只需要第一个 Emby ID 用于映射
                    if emby_ids and len(emby_ids) > 0:
                        emby_media_map[tmdb_id] = emby_ids[0]

                    if item['item_type'] == 'Series':
                        series_tmdb_ids.append(tmdb_id)
                        # 构建剧名到 ID 的映射
                        normalized_name = utils.normalize_name_for_matching(item.get('title', ''))
                        if normalized_name:
                            emby_series_name_to_tmdb_id_map[normalized_name] = tmdb_id
                
                # 如果有剧集，再批量查询所有在库的季信息
                if series_tmdb_ids:
                    cursor.execute(
                        """
                        SELECT parent_series_tmdb_id, season_number 
                        FROM media_metadata 
                        WHERE in_library = TRUE AND item_type = 'Season' AND parent_series_tmdb_id = ANY(%s)
                        """,
                        (series_tmdb_ids,)
                    )
                    for row in cursor.fetchall():
                        parent_id = str(row['parent_series_tmdb_id'])
                        season_num = row['season_number']
                        if parent_id not in emby_series_seasons_map:
                            emby_series_seasons_map[parent_id] = set()
                        emby_series_seasons_map[parent_id].add(season_num)

        return emby_media_map, emby_series_seasons_map, emby_series_name_to_tmdb_id_map

    except Exception as e:
        logger.error(f"DB: 为演员同步任务准备在库媒体数据时失败: {e}", exc_info=True)
        # 即使失败也返回空字典，避免上层任务崩溃
        return {}, {}, {}
    
def remove_subscription_source(tmdb_id: str, item_type: str, source_to_remove: Dict[str, Any]):
    """
    从单个媒体项的订阅源列表中移除一个指定的源。
    如果移除后列表为空，则将订阅状态重置为 'NONE'。
    """
    if not all([tmdb_id, item_type, source_to_remove]):
        return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 步骤 1: 先拉取当前的 sources_json
                cursor.execute(
                    "SELECT subscription_sources_json FROM media_metadata WHERE tmdb_id = %s AND item_type = %s",
                    (tmdb_id, item_type)
                )
                row = cursor.fetchone()
                if not row or not row['subscription_sources_json']:
                    return # 没有源，无需操作

                current_sources = row['subscription_sources_json']
                
                # 步骤 2: 在 Python 中过滤掉要移除的源
                # 注意：这里需要精确匹配，所以直接比较字典
                updated_sources = [s for s in current_sources if s != source_to_remove]
                
                # 步骤 3: 根据过滤后的结果决定如何更新
                new_status = 'NONE' if not updated_sources else None # 如果列表空了，就重置状态
                
                sql = "UPDATE media_metadata SET subscription_sources_json = %s"
                params = [json.dumps(updated_sources)]
                
                if new_status:
                    sql += ", subscription_status = %s"
                    params.append(new_status)
                
                sql += " WHERE tmdb_id = %s AND item_type = %s"
                params.extend([tmdb_id, item_type])
                
                cursor.execute(sql, tuple(params))
                logger.info(f"  ➜ 已从媒体 {tmdb_id} ({item_type}) 移除订阅源: {source_to_remove}")

    except Exception as e:
        logger.error(f"移除媒体 {tmdb_id} 的订阅源时出错: {e}", exc_info=True)
        raise

def add_subscription_source(tmdb_id: str, item_type: str, source: Dict[str, Any]):
    """
    【新增】为一个已存在的媒体记录，安全地追加一个新的订阅源。
    - 使用 JSONB 操作符，确保源的唯一性，防止重复添加。
    """
    if not all([tmdb_id, item_type, source]):
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 核心逻辑：
                # 1. 先用 @> 检查这个 source 是否已经存在于数组中。
                # 2. 如果不存在，再用 || 操作符将新的 source 对象追加到数组末尾。
                sql = """
                    UPDATE media_metadata
                    SET subscription_sources_json = subscription_sources_json || %s::jsonb
                    WHERE 
                        tmdb_id = %s 
                        AND item_type = %s
                        AND NOT (subscription_sources_json @> %s::jsonb);
                """
                # source 需要被转换成 JSON 字符串，然后包装成一个单元素的数组
                source_jsonb_array = json.dumps([source])
                
                cursor.execute(sql, (source_jsonb_array, tmdb_id, item_type, source_jsonb_array))
                # with conn: 会自动提交
    except Exception as e:
        logger.error(f"DB: 为 {tmdb_id} ({item_type}) 追加订阅源失败: {e}", exc_info=True)
        # 向上抛出异常，以便外部事务可以回滚
        raise

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