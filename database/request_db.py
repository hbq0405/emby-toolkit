# database/request_db.py
import psycopg2
import logging
from typing import List, Dict, Any, Optional, Tuple
import json

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 媒体订阅管理 (基于 media_metadata 表)
# ======================================================================

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