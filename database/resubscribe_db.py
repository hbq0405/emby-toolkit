# database/resubscribe_db.py
import psycopg2
from psycopg2.extras import Json, execute_values
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 洗版数据访问
# ======================================================================

# --- 规则管理 (Rules Management) ---
def _prepare_rule_data_for_db(rule_data: Dict[str, Any]) -> Dict[str, Any]:
    
    data_to_save = rule_data.copy()
    jsonb_fields = [
        'target_library_ids', 'resubscribe_audio_missing_languages',
        'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
        'resubscribe_effect_include'
    ]
    for field in jsonb_fields:
        if field in data_to_save and data_to_save[field] is not None:
            data_to_save[field] = Json(data_to_save[field])
    return data_to_save

def get_all_resubscribe_rules() -> List[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_rules ORDER BY sort_order ASC, id ASC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 获取所有洗版规则时失败: {e}", exc_info=True)
        return []

def create_resubscribe_rule(rule_data: Dict[str, Any]) -> int:
    
    try:
        prepared_data = _prepare_rule_data_for_db(rule_data)
        columns = prepared_data.keys()
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO resubscribe_rules ({', '.join(columns)}) VALUES ({placeholders}) RETURNING id"
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, list(prepared_data.values()))
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建的规则ID。")
            new_id = result['id']
            conn.commit()
            logger.info(f"  ➜ 成功创建洗版规则 '{rule_data.get('name')}' (ID: {new_id})。")
            return new_id
    except psycopg2.IntegrityError as e:
        logger.warning(f"  ➜ 创建洗版规则失败，可能名称 '{rule_data.get('name')}' 已存在: {e}")
        raise
    except Exception as e:
        logger.error(f"  ➜ 创建洗版规则时发生未知错误: {e}", exc_info=True)
        raise

def update_resubscribe_rule(rule_id: int, rule_data: Dict[str, Any]) -> bool:
    
    try:
        prepared_data = _prepare_rule_data_for_db(rule_data)
        set_clauses = [f"{key} = %s" for key in prepared_data.keys()]
        sql = f"UPDATE resubscribe_rules SET {', '.join(set_clauses)} WHERE id = %s"
        values = list(prepared_data.values())
        values.append(rule_id)
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            if cursor.rowcount == 0:
                logger.warning(f"  ➜ 尝试更新洗版规则ID {rule_id}，但在数据库中未找到。")
                return False
            conn.commit()
            logger.info(f"  ➜ 成功更新洗版规则ID {rule_id}。")
            return True
    except Exception as e:
        logger.error(f"  ➜ 更新洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def delete_resubscribe_rule(rule_id: int) -> bool:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_rules WHERE id = %s", (rule_id,))
            if cursor.rowcount == 0:
                logger.warning(f"  ➜ 尝试删除洗版规则ID {rule_id}，但在数据库中未找到。")
                return False
            conn.commit()
            logger.info(f"  ➜ 成功删除洗版规则ID {rule_id}。")
            return True
    except Exception as e:
        logger.error(f"  ➜ 删除洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def update_resubscribe_rules_order(ordered_ids: List[int]) -> bool:
    
    if not ordered_ids:
        return True
    data_to_update = [(index, rule_id) for index, rule_id in enumerate(ordered_ids)]
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE resubscribe_rules SET sort_order = data.sort_order FROM (VALUES %s) AS data(sort_order, id) WHERE resubscribe_rules.id = data.id;"
            execute_values(cursor, sql, data_to_update)
            conn.commit()
            logger.info(f"  ➜ 成功更新了 {len(ordered_ids)} 个洗版规则的顺序。")
            return True
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版规则顺序时失败: {e}", exc_info=True)
        raise

# --- 缓存管理 (Cache Management) ---
def get_all_resubscribe_cache() -> List[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache ORDER BY item_name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 获取洗版缓存失败: {e}", exc_info=True)
        return []

def upsert_resubscribe_cache_batch(items_data: List[Dict[str, Any]]):
    """
    【V3 - 剧集分季修复版】
    批量插入或更新洗版缓存表。
    - 核心修复：确保 SQL 语句和数据准备过程都包含了 series_id 和 season_number 字段。
    - 使用更现代的 execute_batch 方法以提高兼容性。
    """
    if not items_data:
        return

    # ★★★ 1. 更新 SQL 语句以包含新字段 ★★★
    sql = """
        INSERT INTO resubscribe_cache (
            item_id, emby_item_id, item_name, tmdb_id, item_type, status, reason,
            resolution_display, quality_display, effect_display, audio_display, subtitle_display,
            audio_languages_raw, subtitle_languages_raw, last_checked_at,
            matched_rule_id, matched_rule_name, source_library_id, path, filename,
            series_id, season_number
        )
        VALUES (
            %(item_id)s, %(emby_item_id)s, %(item_name)s, %(tmdb_id)s, %(item_type)s, %(status)s, %(reason)s,
            %(resolution_display)s, %(quality_display)s, %(effect_display)s, %(audio_display)s, %(subtitle_display)s,
            %(audio_languages_raw)s, %(subtitle_languages_raw)s, NOW(),
            %(matched_rule_id)s, %(matched_rule_name)s, %(source_library_id)s, %(path)s, %(filename)s,
            %(series_id)s, %(season_number)s
        )
        ON CONFLICT (item_id) DO UPDATE SET
            emby_item_id = EXCLUDED.emby_item_id,
            item_name = EXCLUDED.item_name,
            tmdb_id = EXCLUDED.tmdb_id,
            item_type = EXCLUDED.item_type,
            status = EXCLUDED.status,
            reason = EXCLUDED.reason,
            resolution_display = EXCLUDED.resolution_display,
            quality_display = EXCLUDED.quality_display,
            effect_display = EXCLUDED.effect_display,
            audio_display = EXCLUDED.audio_display,
            subtitle_display = EXCLUDED.subtitle_display,
            audio_languages_raw = EXCLUDED.audio_languages_raw,
            subtitle_languages_raw = EXCLUDED.subtitle_languages_raw,
            last_checked_at = NOW(),
            matched_rule_id = EXCLUDED.matched_rule_id,
            matched_rule_name = EXCLUDED.matched_rule_name,
            source_library_id = EXCLUDED.source_library_id,
            path = EXCLUDED.path,
            filename = EXCLUDED.filename,
            series_id = EXCLUDED.series_id,          -- 新增字段
            season_number = EXCLUDED.season_number;   -- 新增字段
    """
    
    # ★★★ 2. 准备数据，确保每个字典都包含所有键，即使是 None ★★★
    # 这是为了让 execute_batch 能够正常工作
    prepared_items = []
    for item in items_data:
        # 确保所有可能的键都存在，为缺失的键提供默认值 None
        full_item = {
            'item_id': item.get('item_id'),
            'emby_item_id': item.get('emby_item_id'),
            'item_name': item.get('item_name'),
            'tmdb_id': item.get('tmdb_id'),
            'item_type': item.get('item_type'),
            'status': item.get('status'),
            'reason': item.get('reason'),
            'resolution_display': item.get('resolution_display'),
            'quality_display': item.get('quality_display'),
            'effect_display': item.get('effect_display'),
            'audio_display': item.get('audio_display'),
            'subtitle_display': item.get('subtitle_display'),
            'audio_languages_raw': Json(item.get('audio_languages_raw')),
            'subtitle_languages_raw': Json(item.get('subtitle_languages_raw')),
            'matched_rule_id': item.get('matched_rule_id'),
            'matched_rule_name': item.get('matched_rule_name'),
            'source_library_id': item.get('source_library_id'),
            'path': item.get('path'),
            'filename': item.get('filename'),
            'series_id': item.get('series_id'), 
            'season_number': item.get('season_number') 
        }
        prepared_items.append(full_item)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 3. 使用更现代、更推荐的 execute_batch 方法 ★★★
                from psycopg2.extras import execute_batch
                execute_batch(cursor, sql, prepared_items, page_size=500)
            conn.commit()
        logger.info(f"成功向 resubscribe_cache 表中写入/更新了 {len(items_data)} 条记录。")
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版缓存失败: {e}", exc_info=True)
        raise

def update_resubscribe_item_status(item_id: str, new_status: str) -> bool:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE resubscribe_cache SET status = %s WHERE item_id = %s",
                (new_status, item_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 更新洗版缓存状态失败 for item {item_id}: {e}", exc_info=True)
        return False

def delete_resubscribe_cache_by_rule_id(rule_id: int) -> int:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_cache WHERE matched_rule_id = %s", (rule_id,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"  ➜ 联动删除了 {deleted_count} 条与规则ID {rule_id} 关联的洗版缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 根据规则ID {rule_id} 删除洗版缓存时失败: {e}", exc_info=True)
        raise

def delete_resubscribe_cache_for_unwatched_libraries(watched_library_ids: List[str]) -> int:
    
    if not watched_library_ids:
        sql = "DELETE FROM resubscribe_cache"
        params = []
    else:
        sql = "DELETE FROM resubscribe_cache WHERE source_library_id IS NOT NULL AND source_library_id NOT IN %s"
        params = [tuple(watched_library_ids)]
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"  ➜ 成功删除了 {deleted_count} 条来自无效媒体库的陈旧洗版缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 清理无效洗版缓存时失败: {e}", exc_info=True)
        raise

def get_resubscribe_cache_item(item_id: str) -> Optional[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache WHERE item_id = %s", (item_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"  ➜ 获取单个洗版缓存项 {item_id} 失败: {e}", exc_info=True)
        return None

def get_resubscribe_rule_by_id(rule_id: int) -> Optional[Dict[str, Any]]:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_rules WHERE id = %s", (rule_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"  ➜ 获取单个洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return None
    
def delete_resubscribe_cache_item(item_id: str) -> bool:
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_cache WHERE item_id = %s", (item_id,))
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 删除单条洗版缓存项 {item_id} 失败: {e}", exc_info=True)
        return False
    
def batch_update_resubscribe_cache_status(item_ids: List[str], new_status: str) -> int:
    
    if not item_ids or not new_status:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE resubscribe_cache SET status = %s WHERE item_id = ANY(%s)"
            cursor.execute(sql, (new_status, item_ids))
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"  ➜ 成功将 {updated_count} 个洗版缓存项的状态批量更新为 '{new_status}'。")
            return updated_count
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版缓存状态时失败: {e}", exc_info=True)
        return 0
    
def delete_resubscribe_cache_items_batch(item_ids: List[str]) -> int:
    """ 根据 Item ID 列表批量删除缓存记录 """
    if not item_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 ANY(%s) 语法高效处理列表
            sql = "DELETE FROM resubscribe_cache WHERE item_id = ANY(%s)"
            cursor.execute(sql, (item_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"  ➜ [快速扫描清理] 成功批量删除了 {deleted_count} 条陈旧的洗版缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 批量删除洗版缓存时失败: {e}", exc_info=True)
        return 0
    
def clear_resubscribe_cache_except_ignored() -> int:
    """
    【深度扫描专用】清空洗版缓存表中所有不是 'ignored' 状态的记录。
    返回被删除的记录数。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM resubscribe_cache WHERE status != 'ignored'"
            cursor.execute(sql)
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"  ➜ [深度扫描] 成功清空了 {deleted_count} 条非忽略状态的旧缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ [深度扫描] 清空洗版缓存时失败: {e}", exc_info=True)
        return 0