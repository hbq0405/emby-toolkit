# database/media_db.py
import logging
from typing import List, Dict, Optional, Any, Union, Tuple, Set
import json
import psycopg2
from .connection import get_db_connection
import utils

logger = logging.getLogger(__name__)

def check_tmdb_ids_in_library(tmdb_ids: List[str], item_type: str) -> Dict[str, str]:
    """
    接收 TMDb ID 列表，返回一个字典，映射 TMDb ID 到 Emby Item ID。
    """
    if not tmdb_ids:
        return {}

    sql = "SELECT tmdb_id, emby_item_ids_json FROM media_metadata WHERE item_type = %s AND tmdb_id = ANY(%s)"

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (item_type, tmdb_ids))
            result = {}
            for row in cursor.fetchall():
                tmdb_id = row['tmdb_id']
                emby_ids = row['emby_item_ids_json']  # 这已经是个列表
                if emby_ids:   # 只保存非空列表
                    result[tmdb_id] = emby_ids
            return result
    except Exception as e:
        logger.error(f"DB: 检查 TMDb ID 是否在库时失败: {e}", exc_info=True)
        return {}
    
def does_series_have_valid_actor_cache(tmdb_id: str) -> bool:
    """
    检查一个剧集是否在 media_metadata 中存在有效的演员缓存。
    "有效"定义为 actors_json 字段存在且不为空数组 '[]'。
    """
    if not tmdb_id:
        return False
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM media_metadata 
                    WHERE tmdb_id = %s AND item_type = 'Series'
                      AND actors_json IS NOT NULL AND actors_json::text != '[]'
                """, (tmdb_id,))
                # 如果能查询到一行，说明缓存存在且有效
                return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"检查剧集 {tmdb_id} 演员缓存时出错: {e}", exc_info=True)
        # 安全起见，如果查询失败，我们假定缓存不存在，以便触发深度处理
        return False
    
def get_tmdb_id_from_emby_id(emby_id: str) -> Optional[str]:
    """
    根据 Emby ID，从 media_metadata 表中反查出对应的 TMDB ID。
    """
    if not emby_id:
        return None
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 @> 操作符高效查询 JSONB 数组是否包含指定的 Emby ID
            sql = "SELECT tmdb_id FROM media_metadata WHERE emby_item_ids_json @> %s::jsonb"
            cursor.execute(sql, (json.dumps([emby_id]),))
            row = cursor.fetchone()
            return row['tmdb_id'] if row else None
    except psycopg2.Error as e:
        logger.error(f"根据 Emby ID {emby_id} 反查 TMDB ID 时出错: {e}", exc_info=True)
        return None
    
def get_media_details_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    【V3 - 新增核心工具】根据 TMDB ID 列表，批量获取 media_metadata 表中的完整记录。
    返回一个以 tmdb_id 为键，整行记录字典为值的 map，方便快速查找。
    """
    if not tmdb_ids:
        return {}
    
    media_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM media_metadata WHERE tmdb_id = ANY(%s)"
            cursor.execute(sql, (tmdb_ids,))
            rows = cursor.fetchall()
            for row in rows:
                media_map[row['tmdb_id']] = dict(row)
        return media_map
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID列表批量获取媒体详情时出错: {e}", exc_info=True)
        return {}

def get_all_media_metadata(item_type: str = 'Movie') -> List[Dict[str, Any]]:
    """从媒体元数据缓存表中获取指定类型的所有记录。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE item_type = %s AND in_library = TRUE", (item_type,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有媒体元数据时出错 (类型: {item_type}): {e}", exc_info=True)
        return []

def get_media_in_library_status_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, bool]:
    """ 根据 TMDB ID 列表，批量查询媒体的在库状态。"""
    if not tmdb_ids: return {}
    in_library_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT tmdb_id FROM media_metadata WHERE tmdb_id = ANY(%s) AND in_library = TRUE AND item_type IN ('Movie', 'Series')"
            cursor.execute(sql, (tmdb_ids,))
            for row in cursor.fetchall():
                in_library_map[row['tmdb_id']] = True
        return in_library_map
    except psycopg2.Error as e:
        logger.error(f"批量获取媒体在库状态时出错: {e}", exc_info=True)
        return {}
    
def get_all_wanted_media() -> List[Dict[str, Any]]:
    """
    【V2 - 增加父剧信息版】获取所有状态为 'WANTED' 的媒体项。
    为 Season 类型的项目额外提供 parent_series_tmdb_id。
    """
    sql = """
        SELECT 
            tmdb_id, item_type, title, release_date, poster_path, overview,
            -- ★★★ 核心修改：把这两个关键字段也查出来 ★★★
            parent_series_tmdb_id, 
            season_number, 
            subscription_sources_json
        FROM media_metadata
        WHERE subscription_status = 'WANTED'
        ORDER BY first_requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取所有待订阅(WANTED)媒体失败: {e}", exc_info=True)
        return []
    
def promote_pending_to_wanted() -> int:
    """
    【新增】检查所有状态为 'PENDING_RELEASE' 的媒体项。
    如果其发行日期已到或已过，则将其状态更新为 'WANTED'。
    返回被成功晋升状态的媒体项数量。
    """
    sql = """
        UPDATE media_metadata
        SET 
            subscription_status = 'WANTED',
            -- 可以选择性地在这里也更新一个时间戳字段，用于追踪状态变更
            last_synced_at = NOW()
        WHERE 
            subscription_status = 'PENDING_RELEASE' 
            AND release_date <= NOW();
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                promoted_count = cursor.rowcount
                conn.commit()
                return promoted_count
    except Exception as e:
        logger.error(f"DB: 晋升 PENDING_RELEASE 状态失败: {e}", exc_info=True)
        return 0

def ensure_media_record_exists(media_info_list: List[Dict[str, Any]]):
    """
    【V1 - 职责单一版】
    确保媒体元数据记录存在于数据库中。
    - 如果记录不存在，则创建它，订阅状态默认为 'NONE'。
    - 如果记录已存在，则只更新其基础元数据（标题、海报、父子关系等）。
    - ★★★ 这个函数【绝不】会修改已存在的订阅状态 ★★★
    """
    if not media_info_list:
        return

    logger.info(f"  ➜ [元数据注册] 准备为 {len(media_info_list)} 个媒体项目确保记录存在...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, title, original_title, release_date, poster_path, 
                        overview, season_number, parent_series_tmdb_id
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s,
                        %(overview)s, %(season_number)s, %(parent_series_tmdb_id)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        title = EXCLUDED.title,
                        original_title = EXCLUDED.original_title,
                        release_date = EXCLUDED.release_date,
                        poster_path = EXCLUDED.poster_path,
                        overview = EXCLUDED.overview,
                        season_number = EXCLUDED.season_number,
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        last_synced_at = NOW();
                """
                
                # 准备数据，确保所有 key 都存在，避免 psycopg2 报错
                data_for_batch = []
                for info in media_info_list:
                    data_for_batch.append({
                        "tmdb_id": info.get("tmdb_id"),
                        "item_type": info.get("item_type"),
                        "title": info.get("title"),
                        "original_title": info.get("original_title"),
                        "release_date": info.get("release_date") or None,
                        "poster_path": info.get("poster_path"),
                        "overview": info.get("overview"),
                        "season_number": info.get("season_number"),
                        "parent_series_tmdb_id": info.get("parent_series_tmdb_id")
                    })

                execute_batch(cursor, sql, data_for_batch)
                logger.info(f"  ➜ [元数据注册] 成功，影响了 {cursor.rowcount} 行。")

    except Exception as e:
        logger.error(f"  ➜ [元数据注册] 确保媒体记录存在时发生错误: {e}", exc_info=True)
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

def get_all_non_library_media() -> List[Dict[str, Any]]:
    """
    【V3 - 智能标题拼接版】获取所有不在媒体库中的媒体项，用于前端统一管理。
    当项目类型为 Season 时，会自动查询并拼接父剧集的标题。
    """
    # ★★★ 核心修改：使用 LEFT JOIN 和 CASE 语句来智能构建标题 ★★★
    sql = """
        SELECT 
            m1.tmdb_id, 
            m1.item_type, 
            -- ★★★ 核心修改：无论原始季名是什么，都拼接成“第 X 季” ★★★
            CASE 
                WHEN m1.item_type = 'Season' THEN COALESCE(m2.title, '未知剧集') || ' 第 ' || m1.season_number || '季'
                ELSE m1.title 
            END AS title,
            m1.release_date, 
            m1.poster_path, 
            m1.subscription_status, 
            m1.ignore_reason, 
            m1.subscription_sources_json,
            m1.first_requested_at
        FROM 
            media_metadata AS m1
        LEFT JOIN 
            media_metadata AS m2 
        ON 
            m1.parent_series_tmdb_id = m2.tmdb_id AND m2.item_type = 'Series'
        WHERE 
            m1.in_library = FALSE 
            AND m1.subscription_status IN ('WANTED', 'PENDING_RELEASE', 'IGNORED', 'SUBSCRIBED')
        ORDER BY 
            m1.first_requested_at DESC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取所有非在库媒体失败: {e}", exc_info=True)
        return []
    
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

def get_user_request_history(user_id: str, page: int = 1, page_size: int = 10) -> tuple[List[Dict[str, Any]], int]:
    """
    【V2 - 分页功能修复版】
    获取指定用户的订阅请求历史，支持分页，并返回总记录数。
    """
    offset = (page - 1) * page_size
    
    # ★★★ 核心修改 1/3: 准备用于查询的 source filter ★★★
    source_filter = json.dumps([{"type": "user_request", "user_id": user_id}])

    # ★★★ 核心修改 2/3: 增加一个查询总记录数的 SQL ★★★
    count_sql = """
        SELECT COUNT(*) 
        FROM media_metadata
        WHERE subscription_sources_json @> %s::jsonb;
    """
    
    # ★★★ 核心修改 3/3: 在查询数据的 SQL 中加入 LIMIT 和 OFFSET ★★★
    data_sql = """
        SELECT 
            tmdb_id, item_type, title, 
            subscription_status as status, 
            in_library, -- <--- 把这个字段也拿出来
            first_requested_at as requested_at, 
            ignore_reason as notes
        FROM media_metadata
        WHERE subscription_sources_json @> %s::jsonb
        ORDER BY first_requested_at DESC
        LIMIT %s OFFSET %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 先获取总数
            cursor.execute(count_sql, (source_filter,))
            total_records = cursor.fetchone()['count']
            
            # 再获取分页数据
            cursor.execute(data_sql, (source_filter, page_size, offset))
            rows = cursor.fetchall()
            history = []
            for row in rows:
                # 复制一份，避免修改原始数据
                history_item = dict(row)
                
                # 规则1: 只要在库里了，就是“已完成”，这是最高优先级
                if history_item.get('in_library'):
                    history_item['status'] = 'completed'
                # 规则2: 如果不在库，且状态是 IGNORED，那就是“已拒绝”
                elif history_item.get('status') == 'IGNORED':
                    history_item['status'] = 'rejected'
                # 其他状态 (WANTED, SUBSCRIBED, REQUESTED) 直接使用，前端 statusMap 能识别
                
                history.append(history_item)
            
            return history, total_records
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅历史失败: {e}", exc_info=True)
        # 保持与旧函数一致的返回类型，即使出错
        return [], 0

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
    
def sync_series_children_metadata(parent_tmdb_id: str, seasons: List[Dict], episodes: List[Dict], local_in_library_info: Dict[int, set]):
    """
    根据从 TMDB 获取的最新数据，批量同步一个剧集的所有季和集到 media_metadata 表。
    使用 ON CONFLICT DO UPDATE 实现高效的“插入或更新”。
    """
    if not parent_tmdb_id:
        return

    records_to_upsert = []

    # 1. 准备所有季的记录
    for season in seasons:
        season_num = season.get('season_number')
        # ★★★ 核心修改：直接从 TMDb 数据中获取真实的季 ID ★★★
        season_tmdb_id = season.get('id')

        # 如果季号或真实 ID 不存在，则跳过此记录，保证数据完整性
        if season_num is None or season_num == 0 or not season_tmdb_id:
            continue
        
        # 判断本季是否在库的逻辑保持不变
        is_season_in_library = season_num in local_in_library_info
        
        records_to_upsert.append({
            "tmdb_id": str(season_tmdb_id), "item_type": "Season", # <-- 使用修正后的真实 ID
            "parent_series_tmdb_id": parent_tmdb_id, "title": season.get('name'),
            "overview": season.get('overview'), "release_date": season.get('air_date'),
            "poster_path": season.get('poster_path'), "season_number": season_num,
            "in_library": is_season_in_library
        })

    # 2. 准备所有集的记录
    for episode in episodes:
        episode_tmdb_id = episode.get('id')
        if not episode_tmdb_id: continue

        season_num = episode.get('season_number')
        episode_num = episode.get('episode_number')

        # ★★★ 核心修改 2/4: 判断本集是否在库 ★★★
        is_episode_in_library = season_num in local_in_library_info and episode_num in local_in_library_info.get(season_num, set())

        records_to_upsert.append({
            "tmdb_id": str(episode_tmdb_id), "item_type": "Episode",
            "parent_series_tmdb_id": parent_tmdb_id, "title": episode.get('name'),
            "overview": episode.get('overview'), "release_date": episode.get('air_date'),
            "season_number": season_num, "episode_number": episode_num,
            "in_library": is_episode_in_library # <-- 使用判断结果
        })

    if not records_to_upsert:
        return

    # 3. 执行批量“插入或更新”
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                # ★★★ 核心修改 3/4: SQL语句的 ON CONFLICT 部分，也要更新 in_library 状态 ★★★
                sql = """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, parent_series_tmdb_id, title, overview, 
                        release_date, poster_path, season_number, episode_number, in_library
                    ) VALUES (
                        %(tmdb_id)s, %(item_type)s, %(parent_series_tmdb_id)s, %(title)s, %(overview)s,
                        %(release_date)s, %(poster_path)s, %(season_number)s, %(episode_number)s, %(in_library)s
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        title = EXCLUDED.title,
                        overview = EXCLUDED.overview,
                        release_date = EXCLUDED.release_date,
                        poster_path = EXCLUDED.poster_path,
                        season_number = EXCLUDED.season_number,
                        episode_number = EXCLUDED.episode_number,
                        in_library = EXCLUDED.in_library, -- <-- 关键！确保更新时也同步在库状态
                        last_synced_at = NOW();
                """
                
                # ★★★ 核心修改 4/4: 确保 in_library 字段被正确填充 ★★★
                data_for_batch = []
                for rec in records_to_upsert:
                    data_for_batch.append({
                        "tmdb_id": rec.get("tmdb_id"), "item_type": rec.get("item_type"),
                        "parent_series_tmdb_id": rec.get("parent_series_tmdb_id"),
                        "title": rec.get("title"), "overview": rec.get("overview"),
                        "release_date": rec.get("release_date"), "poster_path": rec.get("poster_path"),
                        "season_number": rec.get("season_number"), "episode_number": rec.get("episode_number"),
                        "in_library": rec.get("in_library", False) # <-- 确保这个值被正确传入
                    })

                execute_batch(cursor, sql, data_for_batch)
                logger.info(f"  ➜ [追剧联动] 成功为剧集 {parent_tmdb_id} 智能同步了 {len(data_for_batch)} 个子项目的元数据和在库状态。")

    except Exception as e:
        logger.error(f"  ➜ [追剧联动] 在同步剧集 {parent_tmdb_id} 的子项目时发生错误: {e}", exc_info=True)

def get_series_title_by_tmdb_id(tmdb_id: str) -> Optional[str]:
    """根据 TMDB ID 精确查询剧集的标题。"""
    if not tmdb_id:
        return None
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT title FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series' LIMIT 1"
            cursor.execute(sql, (tmdb_id,))
            row = cursor.fetchone()
            return row['title'] if row else None
    except psycopg2.Error as e:
        logger.error(f"根据 TMDB ID {tmdb_id} 查询剧集标题时出错: {e}", exc_info=True)
        return None
    
def get_in_library_status_for_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, bool]:
    """
    给定一个 TMDB ID 列表，批量查询它们在 media_metadata 中的 in_library 状态。
    返回一个字典，键是 TMDB ID，值是布尔值 (True/False)。
    """
    if not tmdb_ids:
        return {}
    
    sql = """
        SELECT tmdb_id, in_library 
        FROM media_metadata 
        WHERE tmdb_id = ANY(%s);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_ids,))
                # 使用字典推导式高效地构建返回结果
                return {str(row['tmdb_id']): row['in_library'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"DB: 批量查询 TMDB ID 的在库状态失败: {e}", exc_info=True)
        return {}
    
def get_all_in_library_media_for_actor_sync() -> Tuple[Dict[str, str], Dict[str, Set[int]], Dict[str, str]]:
    """
    【新增】为演员订阅任务，一次性从 media_metadata 表中提取所有需要的数据。
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
