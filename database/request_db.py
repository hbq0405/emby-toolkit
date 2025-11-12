# database/request_db.py
import psycopg2
import logging
from typing import List, Dict, Any, Optional, Tuple

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 用户订阅请求 (Subscription Requests)
# ======================================================================

def create_subscription_request(**kwargs) -> int:
    """
    在 subscription_requests 表中创建一条新的申请记录。
    """
    columns = kwargs.keys()
    values = kwargs.values()
    
    sql = f"""
        INSERT INTO subscription_requests ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        RETURNING id;
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            new_id = cursor.fetchone()['id']
            conn.commit()
            return new_id
    except Exception as e:
        logger.error(f"DB: 创建订阅请求失败: {e}", exc_info=True)
        raise


    
def get_pending_subscription_requests() -> List[Dict[str, Any]]:
    """查询所有状态为 'pending' 的订阅请求，并关联用户名。"""
    sql = """
        SELECT sr.*, u.name as username
        FROM subscription_requests sr
        JOIN emby_users u ON sr.emby_user_id = u.id
        WHERE sr.status = 'pending'
        ORDER BY sr.requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询待审订阅列表失败: {e}", exc_info=True)
        raise

def get_approved_subscription_requests() -> List[Dict[str, Any]]:
    """查询所有状态为 'approved' 的订阅请求。"""
    sql = """
        SELECT * FROM subscription_requests
        WHERE status = 'approved'
        ORDER BY requested_at ASC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询已批准的订阅列表失败: {e}", exc_info=True)
        raise

def update_subscription_request_status(request_id: int, status: str, processed_by: str = 'admin', notes: Optional[str] = None) -> bool:
    """更新指定订阅请求的状态、处理人和备注信息。"""
    sql = """
        UPDATE subscription_requests
        SET status = %s, processed_by = %s, processed_at = NOW(), notes = %s
        WHERE id = %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (status, processed_by, notes, request_id))
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 更新订阅请求 {request_id} 状态失败: {e}", exc_info=True)
        raise

def get_multiple_subscription_request_details(request_ids: list) -> list:
    """根据ID列表获取多个订阅请求的详情。"""
    if not request_ids:
        return []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # 使用 ANY 操作符来查询列表中的所有ID
        cursor.execute("SELECT * FROM subscription_requests WHERE id = ANY(%s)", (request_ids,))
        return [dict(row) for row in cursor.fetchall()]

def batch_reject_subscription_requests(request_ids: List[int], reason: Optional[str] = None, processed_by: str = 'admin') -> int:
    """
    批量拒绝订阅请求。
    """
    if not request_ids:
        return 0

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE subscription_requests
                SET status = 'rejected', processed_by = %s, processed_at = NOW(), notes = %s
                WHERE id = ANY(%s) AND status = 'pending';
            """
            cursor.execute(sql, (processed_by, reason, request_ids))
            updated_count = cursor.rowcount
            conn.commit()
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量拒绝订阅请求失败: {e}", exc_info=True)
        raise



def get_user_subscription_history(user_id: str, page: int = 1, page_size: int = 10) -> Tuple[List[Dict[str, Any]], int]:
    """获取指定用户的订阅请求历史，支持分页，并返回总记录数。"""
    offset = (page - 1) * page_size
    
    # 查询总记录数
    count_sql = """
        SELECT COUNT(*) FROM subscription_requests
        WHERE emby_user_id = %s;
    """
    
    # 查询分页数据
    data_sql = """
        SELECT id, item_name, item_type, status, requested_at, notes
        FROM subscription_requests
        WHERE emby_user_id = %s
        ORDER BY requested_at DESC
        LIMIT %s OFFSET %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 获取总数
            cursor.execute(count_sql, (user_id,))
            total_records = cursor.fetchone()['count']
            
            # 获取分页数据
            cursor.execute(data_sql, (user_id, page_size, offset))
            history = [dict(row) for row in cursor.fetchall()]
            
            return history, total_records
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅历史失败: {e}", exc_info=True)
        raise

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

def get_subscribers_by_tmdb_id(tmdb_id: str) -> List[Dict[str, Any]]:
    """根据 TMDb ID 查询所有订阅了该媒体的用户 (状态为 pending 或 approved)。"""
    if not tmdb_id:
        return []
    
    sql = """
        SELECT DISTINCT emby_user_id
        FROM subscription_requests
        WHERE tmdb_id = %s AND status IN ('pending', 'approved');
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_id,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 根据 TMDb ID [{tmdb_id}] 查询订阅者失败: {e}", exc_info=True)
        return []
    
def get_global_subscription_status_by_tmdb_id(tmdb_id: str) -> Optional[str]:
    """
    【V1 - 全局状态查询】查询单个 TMDb ID 的最高优先级订阅状态。
    优先级: approved > processing > pending.
    """
    if not tmdb_id:
        return None

    sql = """
        SELECT status
        FROM subscription_requests
        WHERE tmdb_id = %s AND status != 'rejected'
        ORDER BY
            CASE status
                WHEN 'approved' THEN 1
                WHEN 'processing' THEN 2
                WHEN 'completed' THEN 3
                WHEN 'pending' THEN 4
                ELSE 5
            END
        LIMIT 1;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (tmdb_id,))
            result = cursor.fetchone()
            if not result:
                return None
            
            status = result['status']
            # 简化返回给前端的状态
            if status in ['approved', 'processing', 'completed']:
                return 'approved'
            if status == 'pending':
                return 'pending'
            return None
            
    except Exception as e:
        logger.error(f"DB: 查询 TMDb ID {tmdb_id} 的全局状态失败: {e}", exc_info=True)
        return None
    