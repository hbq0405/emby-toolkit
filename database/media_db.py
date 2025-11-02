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
    
def get_subscription_statuses(tmdb_ids: List[str], emby_user_id: str) -> Dict[str, str]:
    """
    根据 TMDb ID 列表和用户 ID，查询这些媒体的订阅状态。
    返回一个字典，格式为 {tmdb_id: status}。
    """
    if not tmdb_ids or not emby_user_id:
        return {}

    # 我们只关心 'pending' 和 'approved' 状态，因为其他的相当于未订阅
    sql = """
        SELECT tmdb_id, status 
        FROM subscription_requests 
        WHERE emby_user_id = %s 
          AND tmdb_id = ANY(%s)
          AND status IN ('pending', 'approved')
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (emby_user_id, tmdb_ids))
            # 将查询结果直接转换为 {tmdb_id: status} 的字典
            return {row['tmdb_id']: row['status'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"DB: 查询订阅状态失败: {e}", exc_info=True)
        return {}