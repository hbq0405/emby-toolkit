# database/media_db.py
import logging
from typing import List, Set, Dict

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
    【短事务】检查一个剧集是否在 media_metadata 中存在有效的演员缓存。
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