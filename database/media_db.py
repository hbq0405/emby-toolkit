# database/media_db.py
import logging
from typing import List, Dict, Optional, Any, Union
import json
import psycopg2
from .connection import get_db_connection

logger = logging.getLogger(__name__)

def check_tmdb_ids_in_library(tmdb_ids: List[str], item_type: str) -> Dict[str, str]:
    """
    【V3 - 返回 Emby ID 版】
    接收 TMDb ID 列表，返回一个字典，映射 TMDb ID 到 Emby Item ID。
    """
    if not tmdb_ids:
        return {}
    
    # ★ 核心修改：同时查询 tmdb_id 和 emby_item_id
    sql = "SELECT tmdb_id, emby_item_id FROM media_metadata WHERE item_type = %s AND tmdb_id = ANY(%s)"
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (item_type, tmdb_ids))
            # ★ 返回一个 {tmdb_id: emby_item_id} 格式的字典
            return {row['tmdb_id']: row['emby_item_id'] for row in cursor.fetchall() if row['emby_item_id']}
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
    
def update_subscription_status(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    new_status: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None,
    ignore_reason: Optional[str] = None  
):
    """
    【V2 - 终极五合一版】
    统一处理单个或批量的媒体订阅状态更新。这是所有状态变更的唯一入口。

    Args:
        tmdb_ids (Union[str, List[str]]): 单个 TMDb ID 或 TMDb ID 列表。
        item_type (str): 媒体类型 ('Movie' or 'Series')。
        new_status (str): 新的状态 ('WANTED', 'SUBSCRIBED', 'IGNORED', 'NONE', 'MISSING')。
        source (Optional[Dict]): 订阅来源，用于 WANTED/SUBSCRIBED 状态。
        media_info_list (Optional[List[Dict]]): 媒体元数据列表，用于在首次订阅时创建记录。
                                                列表中的每个字典应包含 'tmdb_id' 和其他元数据。
    """
    # 1. 标准化输入
    if isinstance(tmdb_ids, str):
        id_list = [tmdb_ids]
    elif isinstance(tmdb_ids, list):
        id_list = tmdb_ids
    else:
        logger.warning("更新状态失败：tmdb_ids 必须是字符串或列表。")
        return

    if not all([id_list, item_type, new_status]):
        logger.warning("更新状态失败：缺少 tmdb_ids, item_type 或 new_status。")
        return

    new_status_upper = new_status.upper()
    if new_status_upper == 'MISSING':
        new_status_upper = 'NONE'

    logger.info(f"  ➜ [统一状态更新] 准备将 {len(id_list)} 个媒体 (类型: {item_type}) 的状态更新为 '{new_status_upper}'。")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                
                # 2. 根据不同状态，选择不同的SQL逻辑
                
                if new_status_upper == 'WANTED':
                    # WANTED 状态：表示用户想要订阅，加入待订阅列表，不更新 last_synced_at
                    sql = """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, subscription_status, subscription_sources_json, 
                            first_requested_at, title, original_title, release_date, poster_path, season_number, overview
                        ) VALUES (
                            %(tmdb_id)s, %(item_type)s, 'WANTED', %(source)s::jsonb,
                            NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(overview)s
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'WANTED',
                            subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                            first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at)
                        WHERE media_metadata.in_library = FALSE;
                    """
                    
                    # 准备批量数据
                    data_to_upsert = []
                    media_info_map = {info['tmdb_id']: info for info in media_info_list} if media_info_list else {}
                    
                    for tmdb_id in id_list:
                        media_info = media_info_map.get(tmdb_id, {})
                        data_to_upsert.append({
                            "tmdb_id": tmdb_id,
                            "item_type": item_type,
                            "source": json.dumps([source]) if source else '[]',
                            "title": media_info.get('title'),
                            "original_title": media_info.get('original_title'),
                            "release_date": media_info.get('release_date') or None,
                            "poster_path": media_info.get('poster_path'),
                            "season_number": media_info.get('season_number') or None,
                            "overview": media_info.get('overview')
                        })
                    
                    from psycopg2.extras import execute_batch
                    execute_batch(cursor, sql, data_to_upsert)

                elif new_status_upper == 'SUBSCRIBED':
                    # SUBSCRIBED 状态：表示已成功订阅，更新 last_synced_at
                    sql = """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, subscription_status, subscription_sources_json, 
                            first_requested_at, title, original_title, release_date, poster_path, season_number, overview
                        ) VALUES (
                            %(tmdb_id)s, %(item_type)s, 'SUBSCRIBED', %(source)s::jsonb,
                            NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(overview)s
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'SUBSCRIBED',
                            subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                            first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                            last_synced_at = NOW();
                    """
                    
                    # 准备批量数据
                    data_to_upsert = []
                    media_info_map = {info['tmdb_id']: info for info in media_info_list} if media_info_list else {}
                    
                    for tmdb_id in id_list:
                        media_info = media_info_map.get(tmdb_id, {})
                        data_to_upsert.append({
                            "tmdb_id": tmdb_id,
                            "item_type": item_type,
                            "source": json.dumps([source]) if source else '[]',
                            "title": media_info.get('title'),
                            "original_title": media_info.get('original_title'),
                            "release_date": media_info.get('release_date') or None,
                            "poster_path": media_info.get('poster_path'),
                            "season_number": media_info.get('season_number') or None,
                            "overview": media_info.get('overview')
                        })
                    
                    from psycopg2.extras import execute_batch
                    execute_batch(cursor, sql, data_to_upsert)

                elif new_status_upper == 'IGNORED':
                    # ★★★ 全新重构的 IGNORED 逻辑 ★★★
                    sql = """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, subscription_status, subscription_sources_json, 
                            ignore_reason, title, original_title, release_date, poster_path, season_number, overview
                        ) VALUES (
                            %(tmdb_id)s, %(item_type)s, 'IGNORED', %(source)s::jsonb, 
                            %(reason)s, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(overview)s
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'IGNORED',
                            -- 核心修正：追加订阅源，而不是清空！
                            subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                            ignore_reason = EXCLUDED.ignore_reason,
                            last_synced_at = NOW()
                        -- 增加一个 WHERE 条件，防止重复添加同一个订阅源
                        WHERE NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json);
                    """
                    
                    # 数据准备逻辑也需要像 WANTED 一样完整
                    data_to_upsert = []
                    media_info_map = {info['tmdb_id']: info for info in media_info_list} if media_info_list else {}
                    
                    for tmdb_id in id_list:
                        media_info = media_info_map.get(tmdb_id, {})
                        data_to_upsert.append({
                            "tmdb_id": tmdb_id,
                            "item_type": item_type,
                            "source": json.dumps([source]) if source else '[]',
                            "reason": ignore_reason,
                            "title": media_info.get('title'),
                            "original_title": media_info.get('original_title'),
                            "release_date": media_info.get('release_date') or None,
                            "poster_path": media_info.get('poster_path'),
                            "season_number": media_info.get('season_number') or None,
                            "overview": media_info.get('overview')
                        })
                    
                    from psycopg2.extras import execute_batch
                    execute_batch(cursor, sql, data_to_upsert)

                elif new_status_upper == 'NONE':
                    sql = """
                        UPDATE media_metadata
                        SET subscription_status = 'NONE',
                            subscription_sources_json = '[]'::jsonb
                        WHERE tmdb_id = ANY(%s) AND item_type = %s;
                    """
                    cursor.execute(sql, (id_list, item_type))
                
                else:
                    logger.warning(f"  ➜ [统一状态更新] 未知的状态 '{new_status_upper}'，操作已跳过。")
                    return

                if cursor.rowcount > 0:
                    logger.info(f"  ➜ [统一状态更新] 成功，影响了 {cursor.rowcount} 行。")

    except Exception as e:
        logger.error(f"  ➜ [统一状态更新] 更新媒体状态时发生错误: {e}", exc_info=True)
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
