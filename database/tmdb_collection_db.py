# database/tmdb_collection_db.py
# TMDb 原生合集数据访问模块

import logging
import json
from typing import Optional, Dict, Any, List

from .connection import get_db_connection

logger = logging.getLogger(__name__)

def upsert_native_collection(data: Dict[str, Any]):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            sql = """
                INSERT INTO collections_info (
                    tmdb_collection_id, emby_collection_id, name, overview,
                    poster_path, all_tmdb_ids_json, last_checked_at
                )
                VALUES (%(tmdb_id)s, %(emby_id)s, %(name)s, %(overview)s, %(poster)s, %(ids_json)s, NOW())
                ON CONFLICT (tmdb_collection_id) DO UPDATE SET
                    -- 如果新数据里有 emby_id，则更新它；否则保留原有的 emby_id
                    emby_collection_id = COALESCE(EXCLUDED.emby_collection_id, collections_info.emby_collection_id),
                    name = EXCLUDED.name,
                    overview = COALESCE(EXCLUDED.overview, collections_info.overview),
                    poster_path = EXCLUDED.poster_path,
                    all_tmdb_ids_json = EXCLUDED.all_tmdb_ids_json,
                    last_checked_at = NOW();
            """
            
            params = {
                'tmdb_id': str(data.get('tmdb_collection_id')),
                'emby_id': data.get('emby_collection_id'),
                'name': data.get('name'),
                'overview': data.get('overview'),
                'poster': data.get('poster_path'),
                'ids_json': json.dumps(data.get('all_tmdb_ids', []))
            }
            
            cursor.execute(sql, params)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Upsert 原生合集失败: {e}")
        return False

def get_all_native_collections() -> List[Dict[str, Any]]:
    """ 获取所有原生合集的基础信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM collections_info ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 读取所有原生合集时发生错误: {e}", exc_info=True)
        return []

def get_all_missing_movies_in_collections() -> List[Dict[str, Any]]:
    """
    1. 展开所有原生合集中的 TMDB ID 和 合集名称。
    2. 关联 media_metadata 表。
    3. 筛选缺失电影，并聚合其所属的合集名称 (处理一部电影属于多个合集的情况)。
    """
    sql = """
        WITH expanded_collections AS (
            -- 1. 展开：将合集 ID 列表炸开成多行 (合集名, tmdb_id)
            SELECT 
                name,
                jsonb_array_elements_text(all_tmdb_ids_json) AS tmdb_id
            FROM collections_info
            WHERE all_tmdb_ids_json IS NOT NULL
        ),
        aggregated_names AS (
            -- 2. 聚合：按 tmdb_id 分组，把合集名拼起来
            SELECT 
                tmdb_id,
                STRING_AGG(DISTINCT name, ', ') as collection_names
            FROM expanded_collections
            GROUP BY tmdb_id
        )
        -- 3. 查询：关联媒体表，不再需要 GROUP BY
        SELECT 
            m.tmdb_id, 
            m.title, 
            m.original_title, 
            m.release_date, 
            m.poster_path, 
            m.overview,
            an.collection_names
        FROM media_metadata m
        JOIN aggregated_names an ON m.tmdb_id = an.tmdb_id
        WHERE 
            m.item_type = 'Movie' 
            AND m.in_library = FALSE 
            AND m.subscription_status = 'NONE';
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"查询合集缺失电影时发生数据库错误: {e}", exc_info=True)
        return []
    
def delete_native_collection_by_emby_id(emby_collection_id: str):
    """
    当 Emby 中删除了合集时，同步删除本地数据库记录。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM collections_info WHERE emby_collection_id = %s",
                (emby_collection_id,)
            )
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"  ➜ [同步删除] 已从数据库移除原生合集记录 (Emby ID: {emby_collection_id})")
            else:
                logger.debug(f"  ➜ [同步删除] 数据库中未找到 Emby ID 为 {emby_collection_id} 的合集，无需删除。")
            return deleted_count > 0
    except Exception as e:
        logger.error(f"删除原生合集记录失败: {e}", exc_info=True)
        return False

def get_native_collection_by_tmdb_id(tmdb_collection_id: str) -> Optional[Dict[str, Any]]:
    """
    根据 TMDb 合集 ID 查找本地数据库中的原生合集记录。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM collections_info WHERE tmdb_collection_id = %s",
                (str(tmdb_collection_id),)
            )
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"查询原生合集 (TMDb ID: {tmdb_collection_id}) 失败: {e}")

def touch_native_collection_by_child_id(tmdb_id: str) -> bool:
    """
    检查给定的 TMDb ID 是否在某个原生合集中。
    如果存在，顺便把该合集的 last_checked_at 更新为当前时间。
    返回: True (存在且已更新) / False (不存在)
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 直接尝试更新，如果匹配到了，rowcount 就会 > 0
            cursor.execute("""
                UPDATE collections_info
                SET last_checked_at = NOW()
                WHERE all_tmdb_ids_json @> %s::jsonb
            """, (json.dumps([str(tmdb_id)]),))
            
            updated = cursor.rowcount > 0
            conn.commit()
            return updated
    except Exception as e:
        logger.error(f"更新合集时间戳 (Child TMDb ID: {tmdb_id}) 失败: {e}")
        return False
    
def get_collection_by_movie_tmdb_id(movie_tmdb_id: str) -> Optional[Dict[str, Any]]:
    """
    【极速反查】根据电影的 TMDb ID，反查它所属的合集信息。
    利用 JSONB 的 @> 包含操作符，查询速度极快。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM collections_info
                WHERE all_tmdb_ids_json @> %s::jsonb
                LIMIT 1
            """, (json.dumps([str(movie_tmdb_id)]),))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"反查电影所属合集失败: {e}")
        return None