# database/resubscribe_db.py
import psycopg2
from psycopg2.extras import Json, execute_values
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
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

def upsert_resubscribe_index_batch(items_data: List[Dict[str, Any]]):
    """批量插入或更新洗版索引表。"""
    if not items_data:
        return

    sql = """
        INSERT INTO resubscribe_index (
            tmdb_id, item_type, season_number, status, reason, matched_rule_id, last_checked_at
        )
        VALUES (
            %(tmdb_id)s, %(item_type)s, %(season_number)s, %(status)s, %(reason)s, %(matched_rule_id)s, NOW()
        )
        ON CONFLICT (tmdb_id, item_type, season_number) DO UPDATE SET
            status = EXCLUDED.status,
            reason = EXCLUDED.reason,
            matched_rule_id = EXCLUDED.matched_rule_id,
            last_checked_at = NOW();
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                from psycopg2.extras import execute_batch
                execute_batch(cursor, sql, items_data, page_size=500)
            conn.commit()
        logger.info(f"成功向 resubscribe_index 表中写入/更新了 {len(items_data)} 条记录。")
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版索引失败: {e}", exc_info=True)
        raise

# ★★★ 为前端获取完整海报墙数据的函数 ★★★
def get_resubscribe_library_status() -> List[Dict[str, Any]]:
    """
    【V8 - 智能海报回退最终版】
    - 为季优先获取自己的海报，如果不存在，则回退到父剧集的海报。
    """
    sql = """
    SELECT
        idx.tmdb_id,
        idx.item_type,
        idx.season_number,
        idx.status,
        idx.reason,
        idx.matched_rule_id,
        
        CASE
            WHEN idx.item_type = 'Season' THEN series_meta.title || ' - 第 ' || idx.season_number || ' 季'
            ELSE movie_meta.title
        END AS item_name,
        
        -- ★★★ 核心修复 1/2: 实现智能海报回退逻辑 ★★★
        CASE
            WHEN idx.item_type = 'Season' THEN COALESCE(season_meta.poster_path, series_meta.poster_path)
            ELSE movie_meta.poster_path
        END AS poster_path,
        
        COALESCE(movie_meta.asset_details_json, episode_meta.asset_details_json) -> 0 AS asset_details

    FROM resubscribe_index AS idx
    
    -- 用于获取电影信息
    LEFT JOIN media_metadata AS movie_meta 
        ON idx.tmdb_id = movie_meta.tmdb_id AND idx.item_type = 'Movie' AND movie_meta.item_type = 'Movie'
        
    -- 用于获取剧集信息 (剧集名和备用海报)
    LEFT JOIN media_metadata AS series_meta
        ON idx.tmdb_id = series_meta.tmdb_id AND idx.item_type = 'Season' AND series_meta.item_type = 'Series'

    -- ★★★ 新增JOIN: 专门用于获取“季”本身的信息 (首选海报) ★★★
    LEFT JOIN media_metadata AS season_meta
        ON idx.tmdb_id = season_meta.parent_series_tmdb_id
        AND idx.season_number = season_meta.season_number
        AND idx.item_type = 'Season'
        AND season_meta.item_type = 'Season'

    -- 用于获取季的代表性资产信息
    LEFT JOIN LATERAL (
        SELECT asset_details_json
        FROM media_metadata
        WHERE parent_series_tmdb_id = idx.tmdb_id
          AND season_number = idx.season_number
          AND item_type = 'Episode'
        ORDER BY episode_number ASC
        LIMIT 1
    ) AS episode_meta ON idx.item_type = 'Season'
    
    ORDER BY item_name;
    """
    
    results = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            for row in rows:
                row_dict = dict(row)
                asset = row_dict.pop('asset_details') or {}
                
                final_item = {
                    "item_id": f"{row_dict['tmdb_id']}-{row_dict['item_type']}-{row_dict['season_number'] if row_dict['season_number'] != -1 else ''}".rstrip('-'),
                    "tmdb_id": row_dict['tmdb_id'],
                    "item_type": row_dict['item_type'],
                    "season_number": row_dict['season_number'] if row_dict['season_number'] != -1 else None,
                    "status": row_dict['status'],
                    "reason": row_dict['reason'],
                    "matched_rule_id": row_dict['matched_rule_id'],
                    "item_name": row_dict['item_name'],
                    
                    # ★★★ 核心修复 2/2: poster_path 现在是智能选择的结果 ★★★
                    "poster_path": row_dict['poster_path'],
                    
                    "resolution_display": asset.get('resolution_display', 'Unknown'),
                    "quality_display": asset.get('quality_display', 'Unknown'),
                    "effect_display": asset.get('effect_display', ['SDR']),
                    "audio_display": asset.get('audio_display', '无'),
                    "subtitle_display": asset.get('subtitle_display', '无'),
                }
                results.append(final_item)
        return results
    except Exception as e:
        logger.error(f"  ➜ 获取洗版海报墙状态失败: {e}", exc_info=True)
        return []
    
def delete_resubscribe_index_by_rule_id(rule_id: int) -> int:
    """【新】删除规则时，联动删除其关联的洗版索引。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_index WHERE matched_rule_id = %s", (rule_id,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"  ➜ 联动删除了 {deleted_count} 条与规则ID {rule_id} 关联的洗版索引。")
            return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 根据规则ID {rule_id} 删除洗版索引时失败: {e}", exc_info=True)
        raise

def batch_update_resubscribe_index_status(item_keys: List[Tuple[str, str, Optional[int]]], new_status: str) -> int:
    """【新】根据复合主键列表，批量更新索引状态。"""
    if not item_keys or not new_status:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 VALUES 子句和 JOIN 来进行高效的批量更新
            sql = """
                UPDATE resubscribe_index AS t
                SET status = %s
                FROM (VALUES %s) AS v(tmdb_id, item_type, season_number)
                WHERE t.tmdb_id = v.tmdb_id 
                  AND t.item_type = v.item_type
                  AND (t.season_number = v.season_number OR (t.season_number IS NULL AND v.season_number IS NULL));
            """
            from psycopg2.extras import execute_values
            execute_values(cursor, sql, item_keys, template=None, page_size=500)
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"  ➜ 成功将 {updated_count} 个洗版索引项的状态批量更新为 '{new_status}'。")
            return updated_count
    except Exception as e:
        logger.error(f"  ➜ 批量更新洗版索引状态时失败: {e}", exc_info=True)
        return 0
    
def get_all_resubscribe_index_keys() -> set:
    """【新】高效获取所有已索引项目的唯一键集合，用于清理比对。"""
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
    """【新】根据统一格式的键列表，批量删除索引记录。"""
    if not keys:
        return 0
    
    # 将 'tmdb_id-S_num' 格式的键解析回 (tmdb_id, item_type, season_number) 的元组
    records_to_delete = []
    for key in keys:
        if '-S' in key:
            parts = key.split('-S')
            if len(parts) == 2:
                records_to_delete.append((parts[0], 'Season', int(parts[1])))
        else:
            records_to_delete.append((key, 'Movie', None))

    deleted_count = 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 使用 DELETE ... WHERE (cols) IN ... 语法进行高效批量删除
                sql = "DELETE FROM resubscribe_index WHERE (tmdb_id, item_type, season_number) IN %s"
                from psycopg2.extras import execute_values
                # execute_values 会自动处理 NULL 的情况
                deleted_count = execute_values(cursor, sql, records_to_delete, page_size=500)
        if deleted_count > 0:
            logger.info(f"  ➜ 成功清理了 {deleted_count} 条陈旧的洗版索引。")
        return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 批量删除洗版索引时失败: {e}", exc_info=True)
        return 0
    
def delete_resubscribe_index_by_rule_id(rule_id: int) -> int:
    """
    【新】当删除一个规则时，联动删除 resubscribe_index 表中所有与之关联的索引记录。
    返回被删除的记录数。
    """
    if not rule_id:
        return 0
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = "DELETE FROM resubscribe_index WHERE matched_rule_id = %s"
                cursor.execute(sql, (rule_id,))
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    logger.info(f"  ➜ 联动删除了 {deleted_count} 条与规则ID {rule_id} 关联的洗版索引。")
                return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 根据规则ID {rule_id} 删除洗版索引时失败: {e}", exc_info=True)
        # 发生错误时抛出异常，让上层调用者知道操作失败
        raise