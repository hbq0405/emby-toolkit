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
    
def get_global_subscription_statuses_by_tmdb_ids(tmdb_ids: List[str]) -> Dict[str, str]:
    """
    【V1 - 全局批量查询】根据一个TMDb ID列表，高效查询每个ID的最高优先级订阅状态。
    此函数不关心用户，只返回全局状态。
    返回一个字典，键为 tmdb_id，值为状态 ('approved' 或 'pending')。
    """
    if not tmdb_ids:
        return {}

    # 使用 PostgreSQL 的 DISTINCT ON 功能，可以非常高效地为每个 tmdb_id 找到优先级最高的那条记录
    sql = """
        SELECT DISTINCT ON (tmdb_id)
            tmdb_id,
            status
        FROM subscription_requests
        WHERE tmdb_id = ANY(%s) AND status != 'rejected'
        ORDER BY
            tmdb_id,
            CASE status
                WHEN 'approved' THEN 1
                WHEN 'processing' THEN 2
                WHEN 'completed' THEN 3
                WHEN 'pending' THEN 4
                ELSE 5
            END;
    """
    
    status_map = {}
    try:
        # 假设您有一个 get_db_connection 的函数
        from .connection import get_db_connection 
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_ids,))
            rows = cursor.fetchall()
            
            for row in rows:
                status = row['status']
                tmdb_id = row['tmdb_id']
                # 简化返回给前端的状态
                if status in ['approved', 'processing', 'completed']:
                    status_map[tmdb_id] = 'approved'
                elif status == 'pending':
                    status_map[tmdb_id] = 'pending'
    except Exception as e:
        # 假设您有 logger
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"DB: 批量查询 TMDb IDs 的全局状态失败: {e}", exc_info=True)
    
    return status_map