# database/request_db.py
import logging
from typing import List, Dict, Any, Optional, Union
import json

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 媒体订阅管理 (基于 media_metadata 表)
# ======================================================================
def _prepare_media_data_for_upsert(
    tmdb_ids: Union[str, List[str]], 
    item_type: str, 
    source: Optional[Dict[str, Any]] = None,
    media_info_list: Optional[List[Dict[str, Any]]] = None,
    ignore_reason: Optional[str] = None # 这个参数我们保留，作为备用
) -> List[Dict[str, Any]]:
    """
    内部辅助函数：标准化输入并准备用于批量插入/更新的数据。
    【V2 - 智能原因版】
    """
    if isinstance(tmdb_ids, str):
        id_list = [tmdb_ids]
    elif isinstance(tmdb_ids, list):
        id_list = tmdb_ids
    else:
        logger.warning("准备数据失败：tmdb_ids 必须是字符串或列表。")
        return []

    data_to_upsert = []
    # ★★★ 核心修改 1/2: 将 media_info_list 转换成更方便查找的字典 ★★★
    media_info_map = {info['tmdb_id']: info for info in media_info_list} if media_info_list else {}
    
    for tmdb_id in id_list:
        media_info = media_info_map.get(tmdb_id, {})
        
        # ★★★ 核心修改 2/2: 智能决定 reason 的来源 ★★★
        # 优先使用 media_info 字典中自带的 'reason' 键。
        # 如果 media_info 中没有，再使用函数传入的全局 ignore_reason。
        final_reason = media_info.get('reason', ignore_reason)

        data_to_upsert.append({
            "tmdb_id": tmdb_id, "item_type": item_type,
            "source": json.dumps([source]) if source else '[]',
            "reason": final_reason, # <--- 使用我们智能判断出的 final_reason
            "title": media_info.get('title'),
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
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                    VALUES (%(tmdb_id)s, %(item_type)s, 'REQUESTED', %(source)s::jsonb, NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'REQUESTED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id)
                    WHERE media_metadata.in_library = FALSE AND media_metadata.subscription_status = 'NONE'
                      AND (EXCLUDED.subscription_sources_json = '[]'::jsonb OR NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json));
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount <= 0: logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
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
    【核心修复 V2】智能防御逻辑：
    1. 绝对不覆盖已订阅 (SUBSCRIBED) 的项目。
    2. 默认不覆盖已入库 (in_library=TRUE) 的项目，★除非是洗版请求 (resubscribe)★。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                if force_unignore:
                    # 强制模式（用户手动点击）：允许覆盖
                    sql = """
                        UPDATE media_metadata 
                        SET subscription_status = 'WANTED', 
                            subscription_sources_json = subscription_sources_json || %(source)s::jsonb, 
                            ignore_reason = NULL, 
                            last_synced_at = NOW()
                        WHERE tmdb_id = %(tmdb_id)s 
                          AND item_type = %(item_type)s 
                          AND subscription_status = 'IGNORED' 
                          -- 用户手动点击时，通常也允许覆盖已入库状态（比如手动洗版）
                          -- 所以这里不加 in_library 限制，或者根据需求加
                          AND NOT (subscription_sources_json @> %(source)s::jsonb);
                    """
                    execute_batch(cursor, sql, data_to_upsert)
                else:
                    # 自动模式：
                    sql = """
                        INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                        VALUES (%(tmdb_id)s, %(item_type)s, 'WANTED', %(source)s::jsonb, NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'WANTED',
                            subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                            first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                            ignore_reason = NULL, 
                            parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id),
                            poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path)
                        WHERE
                            -- 1. 绝对不覆盖已订阅/已完成的项目
                            media_metadata.subscription_status != 'SUBSCRIBED'
                            
                            -- 2. ★★★ 智能防线：已入库项目保护 ★★★
                            -- 规则：如果已入库，则禁止更新，除非是 洗版 或 缺集扫描
                            AND (
                                media_metadata.in_library = FALSE
                                OR
                                EXCLUDED.subscription_sources_json->0->>'type' IN ('resubscribe', 'gap_scan')
                            )
                            
                            AND (
                                -- 3. 防止重复添加完全相同的源
                                NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json)
                                
                                AND (
                                    -- 4.A 正常情况：非忽略状态，允许更新
                                    media_metadata.subscription_status != 'IGNORED'
                                    
                                    -- 4.B 特殊豁免：如果是 gap_scan (缺集扫描)，拥有最高权限，允许复活
                                    OR EXCLUDED.subscription_sources_json->0->>'type' = 'gap_scan'
                                    
                                    -- 4.C 特殊豁免：如果是 resubscribe (洗版)，也允许复活
                                    OR EXCLUDED.subscription_sources_json->0->>'type' = 'resubscribe'

                                    -- 4.D 智能复活：软忽略允许复活
                                    OR (
                                        media_metadata.subscription_status = 'IGNORED'
                                        AND (media_metadata.ignore_reason IS NULL OR media_metadata.ignore_reason != '手动忽略')
                                    )
                                )
                            );
                    """
                    execute_batch(cursor, sql, data_to_upsert)
                
                if cursor.rowcount <= 0: logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为已入库，或不满足前置条件）。")
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
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                    VALUES (%(tmdb_id)s, %(item_type)s, 'PENDING_RELEASE', %(source)s::jsonb, NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'PENDING_RELEASE',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        ignore_reason = NULL, parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id)
                    WHERE media_metadata.in_library = FALSE AND media_metadata.subscription_status NOT IN ('SUBSCRIBED', 'WANTED')
                      AND (EXCLUDED.subscription_sources_json = '[]'::jsonb OR NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json));
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount <= 0: logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
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
    允许洗版和缺集扫描更新已入库项目的状态。
    """
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, source, media_info_list)
    if not data_to_upsert: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, first_requested_at, last_subscribed_at, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                    VALUES (%(tmdb_id)s, %(item_type)s, 'SUBSCRIBED', %(source)s::jsonb, NOW(), NOW(), %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'SUBSCRIBED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        first_requested_at = COALESCE(media_metadata.first_requested_at, EXCLUDED.first_requested_at),
                        last_subscribed_at = NOW(), 
                        last_synced_at = NOW(), 
                        ignore_reason = NULL, 
                        parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id)
                    WHERE 
                        (
                            -- 1. 正常情况：不在库
                            media_metadata.in_library = FALSE 
                            
                            -- 2. 特殊情况：季 (Season) 往往是虚拟容器，允许更新
                            OR media_metadata.item_type = 'Season'
                            
                            -- 3. 洗版豁免：如果是洗版或缺集扫描 (传入了新的 source)，允许更新已入库项目
                            OR EXCLUDED.subscription_sources_json->0->>'type' IN ('resubscribe', 'gap_scan')

                            -- 4. 状态流转豁免
                            -- 如果当前已经是 WANTED 状态，说明之前已通过检查，允许流转到 SUBSCRIBED
                            OR media_metadata.subscription_status = 'WANTED'
                        )
                        AND (
                            (
                                EXCLUDED.subscription_sources_json = '[]'::jsonb 
                                OR NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json)
                            )
                            OR media_metadata.subscription_status != 'SUBSCRIBED'
                        );
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount <= 0: logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为已入库且非洗版，或状态已是 SUBSCRIBED）。")
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
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                sql = """
                    INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, ignore_reason, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                    VALUES (%(tmdb_id)s, %(item_type)s, 'IGNORED', %(source)s::jsonb, %(reason)s, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        subscription_status = 'IGNORED',
                        subscription_sources_json = media_metadata.subscription_sources_json || EXCLUDED.subscription_sources_json,
                        ignore_reason = EXCLUDED.ignore_reason, last_synced_at = NOW(), parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id)
                    WHERE (EXCLUDED.subscription_sources_json = '[]'::jsonb OR NOT (media_metadata.subscription_sources_json @> EXCLUDED.subscription_sources_json));
                """
                execute_batch(cursor, sql, data_to_upsert)
                if cursor.rowcount <= 0: logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能因为不满足前置条件）。")
    except Exception as e:
        logger.error(f"  ➜ [状态执行] 更新媒体状态为 'IGNORED' 时发生错误: {e}", exc_info=True)
        raise

def set_media_status_none(
    tmdb_ids: Union[str, List[str]], 
    item_type: str,
    media_info_list: Optional[List[Dict[str, Any]]] = None # ★★★ 新增参数 ★★★
):
    """
    将媒体状态设置为 'NONE'。
    如果提供了 media_info_list，则执行 UPSERT (不存在则插入，存在则更新)。
    如果没有提供 media_info_list，则仅执行 UPDATE (仅更新已存在的记录)。
    """
    # 准备数据 (注意：这里我们不传递 source，因为 NONE 状态通常意味着清空 source)
    data_to_upsert = _prepare_media_data_for_upsert(tmdb_ids, item_type, None, media_info_list)
    if not data_to_upsert: return

    logger.info(f"  ➜ [状态执行] 准备将 {len(data_to_upsert)} 个媒体 (类型: {item_type}) 的状态更新为 'NONE'...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                
                # ★★★ 策略分叉 ★★★
                if media_info_list:
                    # 模式 A: UPSERT (用于创建占位符或更新)
                    # 必须包含 title 等必填字段，由调用方保证 media_info_list 的完整性
                    sql = """
                        INSERT INTO media_metadata (tmdb_id, item_type, subscription_status, subscription_sources_json, title, original_title, release_date, poster_path, season_number, parent_series_tmdb_id, overview)
                        VALUES (%(tmdb_id)s, %(item_type)s, 'NONE', '[]'::jsonb, %(title)s, %(original_title)s, %(release_date)s, %(poster_path)s, %(season_number)s, %(parent_series_tmdb_id)s, %(overview)s)
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            subscription_status = 'NONE',
                            subscription_sources_json = '[]'::jsonb,
                            ignore_reason = NULL,
                            last_synced_at = NOW(),
                            -- 可选：更新元数据
                            title = COALESCE(EXCLUDED.title, media_metadata.title),
                            poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
                            parent_series_tmdb_id = COALESCE(EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id)
                    """
                    execute_batch(cursor, sql, data_to_upsert)
                    logger.info(f"  ➜ [状态执行] (UPSERT) 成功处理了 {len(data_to_upsert)} 行。")
                    
                else:
                    # 模式 B: UPDATE ONLY (用于取消订阅)
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
                        logger.info(f"  ➜ [状态执行] (UPDATE) 成功，影响了 {cursor.rowcount} 行。")
                    else:
                        logger.info(f"  ➜ [状态执行] 操作完成，但没有行受到影响（可能记录不存在）。")
                        
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

def get_global_subscription_statuses_by_tmdb_ids(tmdb_ids: List[str], item_type: str) -> Dict[str, str]:
    """
    根据 TMDb ID 列表和类型，高效查询每个ID的订阅状态。
    返回一个字典，键为 tmdb_id，值为简化后的状态 ('SUBSCRIBED', 'REQUESTED', 'NONE')。
    """
    if not tmdb_ids or not item_type:
        return {}

    sql = """
        SELECT tmdb_id, subscription_status
        FROM media_metadata
        WHERE tmdb_id = ANY(%s) AND item_type = %s;
    """
    status_map = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (tmdb_ids, item_type))
                rows = cursor.fetchall()
                for row in rows:
                    status_map[str(row['tmdb_id'])] = row['subscription_status']
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
    
def get_stale_subscribed_media(movie_search_window: int, protection_days: int) -> List[Dict[str, Any]]:
    """
    获取所有状态为 'SUBSCRIBED' 且满足以下条件的媒体项：
    1. 订阅时间超过 threshold_days (超时)
    2. 发行时间超过 protection_days (已过新片保护期)
    """
    if movie_search_window <= 0:
        return []

    # 确保保护期至少为 0
    protection_days = max(0, protection_days)

    sql = f"""
        SELECT 
            tmdb_id, 
            item_type, 
            title,
            parent_series_tmdb_id,
            season_number
        FROM 
            media_metadata
        WHERE 
            subscription_status = 'SUBSCRIBED'
            AND last_subscribed_at IS NOT NULL
            AND NOW() - last_subscribed_at > INTERVAL '{movie_search_window} days'
            AND release_date IS NOT NULL
            AND release_date < NOW() - INTERVAL '{protection_days} days';
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB: 查询超时的订阅媒体时失败: {e}", exc_info=True)
        return []
    
def remove_sources_by_type(tmdb_id: str, item_type: str, target_source_type: str):
    """
    从指定媒体的 subscription_sources_json 中移除所有 type 等于 target_source_type 的条目。
    用于确保自动洗版请求是覆盖更新，而不是追加。
    """
    if not all([tmdb_id, item_type, target_source_type]):
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 使用 PostgreSQL 的 JSONB 函数进行过滤重建
                # 逻辑：将数组展开 -> 过滤掉 type 匹配的项 -> 重新聚合 -> 如果结果为空则返回空数组
                sql = """
                    UPDATE media_metadata
                    SET subscription_sources_json = COALESCE(
                        (
                            SELECT jsonb_agg(elem)
                            FROM jsonb_array_elements(subscription_sources_json) elem
                            WHERE elem->>'type' != %s
                        ),
                        '[]'::jsonb
                    )
                    WHERE tmdb_id = %s AND item_type = %s;
                """
                cursor.execute(sql, (target_source_type, tmdb_id, item_type))
                # 这里的更新不需要改变 subscription_status，因为紧接着就会调用 set_media_status_wanted
                
    except Exception as e:
        logger.error(f"DB: 移除媒体 {tmdb_id} 的 '{target_source_type}' 类型来源时出错: {e}", exc_info=True)
        raise

def get_season_tmdb_id(parent_tmdb_id: str, season_number: int) -> Optional[str]:
    """
    根据父剧集ID和季号，查询该季在 media_metadata 表中的 tmdb_id。
    用于在订阅任务中定位具体的季记录，以便更新状态。
    """
    if not parent_tmdb_id or season_number is None:
        return None
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT tmdb_id 
                FROM media_metadata 
                WHERE parent_series_tmdb_id = %s 
                  AND season_number = %s 
                  AND item_type = 'Season' 
                LIMIT 1
            """
            cursor.execute(sql, (parent_tmdb_id, season_number))
            row = cursor.fetchone()
            
            if row and row.get('tmdb_id'):
                return str(row['tmdb_id'])
            return None
            
    except Exception as e:
        logger.error(f"DB: 查询季 ID (Series:{parent_tmdb_id} S{season_number}) 失败: {e}", exc_info=True)
        return None
    
def get_movies_to_pause(search_window_days: int, protection_days: int) -> List[Dict[str, Any]]:
    """
    获取需要暂停搜索的电影。
    条件：
    1. 状态为 SUBSCRIBED
    2. 未入库
    3. 订阅时间超过 search_window_days (搜索窗口期)
    4. 发行日期在 protection_days 内 (保护期内的新片才进行呼吸式搜索，老片直接由超时规则取消)
    """
    # 防止参数为0导致SQL错误
    search_window_days = max(1, search_window_days)
    protection_days = max(30, protection_days)

    sql = f"""
        SELECT tmdb_id, title
        FROM media_metadata
        WHERE item_type = 'Movie'
          AND subscription_status = 'SUBSCRIBED'
          AND in_library = FALSE
          AND last_subscribed_at < NOW() - INTERVAL '{search_window_days} days'
          AND release_date > NOW() - INTERVAL '{protection_days} days';
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB: 获取待暂停电影失败: {e}")
        return []

def get_movies_to_revive() -> List[Dict[str, Any]]:
    """
    获取需要复活的电影。
    条件：
    1. 状态为 PAUSED
    2. 暂停结束时间 (paused_until) 已过
    """
    sql = """
        SELECT tmdb_id, title
        FROM media_metadata
        WHERE item_type = 'Movie'
          AND subscription_status = 'PAUSED'
          AND paused_until <= NOW();
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB: 获取待复活电影失败: {e}")
        return []

def update_movie_status_paused(tmdb_ids: List[str], pause_days: int):
    """
    批量将电影状态设为 PAUSED，并设定复活时间。
    ★ 优化：引入随机抖动，防止同一天大规模复活。
    """
    if not tmdb_ids: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 核心修改：增加随机偏移量 ★★★
                # 逻辑：基础暂停天数 + (0 到 2 天之间的随机时间)
                # 这样复活时间会分散在 [pause_days, pause_days + 2] 这个区间内
                sql = f"""
                    UPDATE media_metadata
                    SET subscription_status = 'PAUSED',
                        paused_until = NOW() + INTERVAL '{pause_days} days' + (random() * INTERVAL '2 days')
                    WHERE tmdb_id = ANY(%s) AND item_type = 'Movie'
                """
                cursor.execute(sql, (tmdb_ids,))
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量暂停电影失败: {e}")

def update_movie_status_revived(tmdb_ids: List[str]):
    """批量将电影状态设为 SUBSCRIBED，并重置订阅时间"""
    if not tmdb_ids: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    UPDATE media_metadata
                    SET subscription_status = 'SUBSCRIBED',
                        last_subscribed_at = NOW(), -- 重置时间，开启新一轮搜索窗口
                        paused_until = NULL
                    WHERE tmdb_id = ANY(%s) AND item_type = 'Movie'
                """
                cursor.execute(sql, (tmdb_ids,))
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量复活电影失败: {e}")