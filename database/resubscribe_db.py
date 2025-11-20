# database/resubscribe_db.py
import psycopg2
from psycopg2.extras import Json, execute_values
import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 洗版数据访问 (V2 - 兼容修复版)
# ======================================================================

def _parse_item_id(item_id: str) -> Optional[Tuple[str, str, int]]:
    """【内部辅助】将前端的 item_id 字符串解析为数据库主键元组。"""
    try:
        parts = item_id.split('-')
        tmdb_id = parts[0]
        item_type = parts[1]
        season_number = -1
        if item_type == 'Season' and len(parts) > 2:
            season_number = int(parts[2].replace('S',''))
        return (tmdb_id, item_type, season_number)
    except (IndexError, ValueError):
        logger.error(f"无法解析 item_id: '{item_id}'")
        return None

# --- 规则管理 (Rules Management) ---
def _prepare_rule_data_for_db(rule_data: Dict[str, Any]) -> Dict[str, Any]:
    data_to_save = rule_data.copy()
    jsonb_fields = [
        'target_library_ids', 'resubscribe_audio_missing_languages',
        'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
        'resubscribe_effect_include',
        'resubscribe_codec_include'
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

# ★★★ 新增函数 ★★★
def get_resubscribe_rule_by_id(rule_id: int) -> Optional[Dict[str, Any]]:
    """根据ID获取单个洗版规则。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_rules WHERE id = %s", (rule_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"  ➜ 获取洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        return None

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
            if not result: raise psycopg2.Error("数据库未能返回新创建的规则ID。")
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
            if cursor.rowcount == 0: return False
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"  ➜ 更新洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def delete_resubscribe_rule(rule_id: int) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_rules WHERE id = %s", (rule_id,))
            if cursor.rowcount == 0: return False
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"  ➜ 删除洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def update_resubscribe_rules_order(ordered_ids: List[int]) -> bool:
    if not ordered_ids: return True
    data_to_update = [(index, rule_id) for index, rule_id in enumerate(ordered_ids)]
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE resubscribe_rules SET sort_order = data.sort_order FROM (VALUES %s) AS data(sort_order, id) WHERE resubscribe_rules.id = data.id;"
            execute_values(cursor, sql, data_to_update)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版规则顺序时失败: {e}", exc_info=True)
        raise

# --- 索引管理 (Index Management) ---

def upsert_resubscribe_index_batch(items_data: List[Dict[str, Any]]):
    if not items_data: return
    sql = """
        INSERT INTO resubscribe_index (tmdb_id, item_type, season_number, status, reason, matched_rule_id, last_checked_at)
        VALUES (%(tmdb_id)s, %(item_type)s, %(season_number)s, %(status)s, %(reason)s, %(matched_rule_id)s, NOW())
        ON CONFLICT (tmdb_id, item_type, season_number) DO UPDATE SET
            status = EXCLUDED.status, reason = EXCLUDED.reason,
            matched_rule_id = EXCLUDED.matched_rule_id, last_checked_at = NOW();
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                execute_batch(cursor, sql, items_data, page_size=500)
            conn.commit()
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版索引失败: {e}", exc_info=True)
        raise

def _format_library_status_results(rows: List[Dict]) -> List[Dict]:
    """【内部辅助】格式化海报墙查询结果。"""
    results = []
    for row in rows:
        row_dict = dict(row)
        asset = row_dict.pop('asset_details') or {}
        series_emby_ids = row_dict.pop('series_emby_ids', None) or []
        series_emby_id = series_emby_ids[0] if series_emby_ids else None
        season_num = row_dict['season_number']
        
        # 兼容旧的 item_id 格式
        item_id_suffix = f"-S{season_num}" if row_dict['item_type'] == 'Season' else ""
        item_id = f"{row_dict['tmdb_id']}-{row_dict['item_type']}{item_id_suffix}"

        final_item = {
            "item_id": item_id,
            "tmdb_id": row_dict['tmdb_id'],
            "item_type": row_dict['item_type'],
            "season_number": season_num if season_num != -1 else None,
            "status": row_dict['status'],
            "reason": row_dict['reason'],
            "matched_rule_id": row_dict['matched_rule_id'],
            "item_name": row_dict['item_name'],
            "poster_path": row_dict['poster_path'],
            "resolution_display": asset.get('resolution_display', 'Unknown'),
            "quality_display": asset.get('quality_display', 'Unknown'),
            "release_group_raw": asset.get('release_group_raw', '无'),
            "codec_display": asset.get('codec_display', 'unknown'),
            "effect_display": asset.get('effect_display', ['SDR']),
            "audio_display": asset.get('audio_display', '无'),
            "subtitle_display": asset.get('subtitle_display', '无'),
            "filename": os.path.basename(asset.get('path', '')) if asset.get('path') else None,
            "emby_item_id": asset.get('emby_item_id'),
            "series_emby_id": series_emby_id
        }
        results.append(final_item)
    return results

def get_resubscribe_library_status(where_clause: str = "", params: tuple = ()) -> List[Dict[str, Any]]:
    """【V9 - 通用查询引擎】根据指定的 WHERE 条件获取海报墙数据。"""
    sql = f"""
    SELECT
        idx.tmdb_id, idx.item_type, idx.season_number, idx.status, idx.reason, idx.matched_rule_id,
        CASE
            WHEN idx.item_type = 'Season' THEN series_meta.title || ' - 第 ' || idx.season_number || ' 季'
            ELSE movie_meta.title
        END AS item_name,
        CASE
            WHEN idx.item_type = 'Season' THEN COALESCE(season_meta.poster_path, series_meta.poster_path)
            ELSE movie_meta.poster_path
        END AS poster_path,
        series_meta.emby_item_ids_json AS series_emby_ids,
        COALESCE(movie_meta.asset_details_json, episode_meta.asset_details_json) -> 0 AS asset_details
    FROM resubscribe_index AS idx
    LEFT JOIN media_metadata AS movie_meta ON idx.tmdb_id = movie_meta.tmdb_id AND idx.item_type = 'Movie' AND movie_meta.item_type = 'Movie'
    LEFT JOIN media_metadata AS series_meta ON idx.tmdb_id = series_meta.tmdb_id AND idx.item_type = 'Season' AND series_meta.item_type = 'Series'
    LEFT JOIN media_metadata AS season_meta ON idx.tmdb_id = season_meta.parent_series_tmdb_id AND idx.season_number = season_meta.season_number AND idx.item_type = 'Season' AND season_meta.item_type = 'Season'
    LEFT JOIN LATERAL (
        SELECT asset_details_json FROM media_metadata
        WHERE parent_series_tmdb_id = idx.tmdb_id AND season_number = idx.season_number AND item_type = 'Episode'
        ORDER BY episode_number ASC LIMIT 1
    ) AS episode_meta ON idx.item_type = 'Season'
    {where_clause}
    ORDER BY item_name;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return _format_library_status_results(cursor.fetchall())
    except Exception as e:
        logger.error(f"  ➜ 获取洗版海报墙状态失败: {e}", exc_info=True)
        return []

# ★★★ 新增函数 (兼容旧API) ★★★
def get_resubscribe_cache_item(item_id: str) -> Optional[Dict[str, Any]]:
    """根据前端 item_id 获取单个项目的完整信息。"""
    key_tuple = _parse_item_id(item_id)
    if not key_tuple: return None
    
    where_clause = "WHERE idx.tmdb_id = %s AND idx.item_type = %s AND idx.season_number = %s"
    results = get_resubscribe_library_status(where_clause, key_tuple)
    return results[0] if results else None

# ★★★ 新增函数 (兼容旧API) ★★★
def update_resubscribe_item_status(item_id: str, new_status: str) -> bool:
    """根据前端 item_id 更新单个项目的状态。"""
    key_tuple = _parse_item_id(item_id)
    if not key_tuple: return False
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE resubscribe_index SET status = %s WHERE tmdb_id = %s AND item_type = %s AND season_number = %s"
            cursor.execute(sql, (new_status, key_tuple[0], key_tuple[1], key_tuple[2]))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 更新项目 {item_id} 状态时失败: {e}", exc_info=True)
        return False

# ★★★ 新增函数 (兼容旧API) ★★★
def delete_resubscribe_cache_item(item_id: str) -> bool:
    """根据前端 item_id 删除单个索引项。"""
    key_tuple = _parse_item_id(item_id)
    if not key_tuple: return False
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM resubscribe_index WHERE tmdb_id = %s AND item_type = %s AND season_number = %s"
            cursor.execute(sql, key_tuple)
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"  ➜ 删除项目 {item_id} 时失败: {e}", exc_info=True)
        return False

def delete_resubscribe_index_by_rule_id(rule_id: int) -> int:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_index WHERE matched_rule_id = %s", (rule_id,))
            deleted_count = cursor.rowcount
            conn.commit()
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 根据规则ID {rule_id} 删除洗版索引时失败: {e}", exc_info=True)
        raise

def batch_update_resubscribe_index_status(item_keys: List[Tuple[str, str, int]], new_status: str) -> int:
    """根据复合主键列表，批量更新索引状态。"""
    if not item_keys or not new_status: return 0
    
    # 准备数据，确保 season_number 是整数
    data_to_update = [(new_status, key[0], key[1], key[2]) for key in item_keys]

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE resubscribe_index t SET status = data.new_status
                FROM (VALUES %s) AS data(new_status, tmdb_id, item_type, season_number)
                WHERE t.tmdb_id = data.tmdb_id 
                  AND t.item_type = data.item_type
                  AND t.season_number = data.season_number;
            """
            execute_values(cursor, sql, data_to_update, template="(%s, %s, %s, %s)")
            updated_count = cursor.rowcount
            conn.commit()
            return updated_count
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版索引状态时失败: {e}", exc_info=True)
        return 0
    
def get_all_resubscribe_index_keys() -> set:
    sql = "SELECT tmdb_id, item_type, season_number FROM resubscribe_index;"
    keys = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                if row['item_type'] == 'Movie':
                    keys.add(row['tmdb_id'])
                elif row['item_type'] == 'Season':
                    keys.add(f"{row['tmdb_id']}-S{row['season_number']}")
        return keys
    except Exception as e:
        logger.error(f"  ➜ 获取所有洗版索引键时失败: {e}", exc_info=True)
        return set()

def delete_resubscribe_index_by_keys(keys: List[str]) -> int:
    if not keys: return 0
    records_to_delete = []
    for key in keys:
        if '-S' in key:
            parts = key.split('-S')
            if len(parts) == 2: records_to_delete.append((parts[0], 'Season', int(parts[1])))
        else:
            records_to_delete.append((key, 'Movie', -1))
    if not records_to_delete: return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = "DELETE FROM resubscribe_index WHERE (tmdb_id, item_type, season_number) IN %s"
                execute_values(cursor, sql, records_to_delete, page_size=500)
                deleted_count = cursor.rowcount
                conn.commit()
                return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 批量删除洗版索引时失败: {e}", exc_info=True)
        return 0