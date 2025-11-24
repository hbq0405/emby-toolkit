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

def get_resubscribe_library_status(where_clause: str = "", params: tuple = ()) -> List[Dict[str, Any]]:
    """
    【V12 - SQL原生性能优化版】
    将 Python 内存中的数据拼接逻辑下沉到数据库层。
    使用 SQL JOIN 和子查询直接获取所需数据，避免传输大量无用的分集元数据。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # 核心优化思路：
            # 1. 主表 resubscribe_index (idx)
            # 2. 关联 media_metadata (m) 获取自身信息
            # 3. 关联 media_metadata (parent) 获取父剧集信息 (仅当 item_type='Season' 时有用)
            # 4. 使用子查询高效获取季的第一集 asset_details，无需加载所有分集
            
            sql = f"""
                SELECT 
                    idx.tmdb_id, 
                    idx.item_type, 
                    idx.season_number, 
                    idx.status, 
                    idx.reason, 
                    idx.matched_rule_id,
                    
                    -- 智能获取名称：如果是季，拼接 '剧集名 - 季名'；否则直接用标题
                    COALESCE(
                        CASE WHEN idx.item_type = 'Season' THEN parent.title || ' - ' || m.title
                             ELSE m.title 
                        END, 
                        '未知项目'
                    ) as item_name,

                    -- 智能获取海报：优先用自己的，没有则用父剧集的
                    COALESCE(m.poster_path, parent.poster_path) as poster_path,

                    -- 获取 Emby ID (取数组第一个)
                    m.emby_item_ids_json->>0 as emby_item_id,
                    parent.emby_item_ids_json->>0 as series_emby_id,

                    -- ★★★ 核心优化：直接在数据库层提取资产详情 ★★★
                    CASE 
                        -- 电影：直接取 asset_details_json 第一个元素
                        WHEN idx.item_type = 'Movie' THEN m.asset_details_json->0
                        
                        -- 季：使用子查询找到该季的第一集 (episode_number 最小的)，取其资产详情
                        WHEN idx.item_type = 'Season' THEN (
                            SELECT ep.asset_details_json->0
                            FROM media_metadata ep
                            WHERE ep.parent_series_tmdb_id = m.parent_series_tmdb_id
                              AND ep.season_number = m.season_number
                              AND ep.item_type = 'Episode'
                            ORDER BY ep.episode_number ASC
                            LIMIT 1
                        )
                    END as asset_details

                FROM resubscribe_index idx
                -- 关联自身元数据
                LEFT JOIN media_metadata m 
                    ON idx.tmdb_id = m.tmdb_id AND idx.item_type = m.item_type
                -- 关联父剧集元数据 (用于季的海报和标题回退)
                LEFT JOIN media_metadata parent 
                    ON m.parent_series_tmdb_id = parent.tmdb_id AND parent.item_type = 'Series'
                
                {where_clause}
            """
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Python 侧只负责极其轻量的格式化，不再进行循环查找
            final_results = []
            for row in rows:
                asset = row.get('asset_details') or {}
                
                # 构建前端需要的对象
                item = {
                    "item_id": f"{row['tmdb_id']}-{row['item_type']}" + (f"-S{row['season_number']}" if row['item_type'] == 'Season' else ""),
                    "tmdb_id": row['tmdb_id'],
                    "item_type": row['item_type'],
                    "conceptual_type": "Series" if row['item_type'] == 'Season' else "Movie",
                    "season_number": row['season_number'] if row['item_type'] == 'Season' else None,
                    "status": row['status'],
                    "reason": row['reason'],
                    "matched_rule_id": row['matched_rule_id'],
                    "item_name": row['item_name'],
                    "poster_path": row['poster_path'],
                    "emby_item_id": row['emby_item_id'],
                    "series_emby_id": row['series_emby_id'],
                    
                    # 资产详情解包
                    "resolution_display": asset.get('resolution_display', 'Unknown'),
                    "quality_display": asset.get('quality_display', 'Unknown'),
                    "release_group_raw": asset.get('release_group_raw', '无'),
                    "codec_display": asset.get('codec_display', 'unknown'),
                    "effect_display": asset.get('effect_display', ['SDR']),
                    "audio_display": asset.get('audio_display', '无'),
                    "subtitle_display": asset.get('subtitle_display', '无'),
                    "filename": os.path.basename(asset.get('path', '')) if asset.get('path') else None
                }
                final_results.append(item)

            # 排序 (Python 的 Timsort 非常快，这里排序开销很小)
            final_results.sort(key=lambda x: x['item_name'] or "")
            return final_results

    except Exception as e:
        logger.error(f"  ➜ 获取洗版海报墙状态失败 (SQL优化版): {e}", exc_info=True)
        return []

def get_resubscribe_cache_item(item_id: str) -> Optional[Dict[str, Any]]:
    """根据前端 item_id 获取单个项目的完整信息。"""
    key_tuple = _parse_item_id(item_id)
    if not key_tuple: return None
    
    where_clause = "WHERE idx.tmdb_id = %s AND idx.item_type = %s AND idx.season_number = %s"
    results = get_resubscribe_library_status(where_clause, key_tuple)
    return results[0] if results else None

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
                sql = """
                    DELETE FROM resubscribe_index t
                    USING (VALUES %s) AS v(tmdb_id, item_type, season_number)
                    WHERE t.tmdb_id = v.tmdb_id 
                      AND t.item_type = v.item_type 
                      AND t.season_number = v.season_number
                """
                execute_values(cursor, sql, records_to_delete, page_size=500)
                # ★★★ 修改结束 ★★★
                
                deleted_count = cursor.rowcount
                conn.commit()
                return deleted_count
    except Exception as e:
        logger.error(f"  ➜ 批量删除洗版索引时失败: {e}", exc_info=True)
        return 0
    
def get_resubscribe_items_by_ids(item_ids: List[str]) -> List[Dict[str, Any]]:
    """根据前端 item_id 列表，批量获取项目的完整信息。"""
    if not item_ids:
        return []
    
    key_tuples = [key for item_id in item_ids if (key := _parse_item_id(item_id))]
    if not key_tuples:
        return []

    where_clause = "WHERE (idx.tmdb_id, idx.item_type, idx.season_number) IN %s"
    params = (tuple(key_tuples),)
    return get_resubscribe_library_status(where_clause, params)

def get_all_needed_resubscribe_items() -> List[Dict[str, Any]]:
    """获取所有状态为 'needed' 的项目的完整信息。"""
    where_clause = "WHERE idx.status = 'needed'"
    return get_resubscribe_library_status(where_clause)

def get_current_index_statuses() -> Dict[Tuple[str, str, int], str]:
    """获取所有索引项的当前状态，用于保留用户操作。"""
    sql = "SELECT tmdb_id, item_type, season_number, status FROM resubscribe_index;"
    statuses = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                key = (str(row['tmdb_id']), row['item_type'], int(row['season_number']))
                statuses[key] = row['status']
        return statuses
    except Exception as e:
        logger.error(f"  ➜ 获取所有洗版索引状态时失败: {e}", exc_info=True)
        return {}