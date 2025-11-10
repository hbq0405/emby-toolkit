# database/requests_db.py (新文件)

import logging
from typing import List, Dict, Optional

from .connection import get_db_connection

logger = logging.getLogger(__name__)

def get_pending_requests_with_username() -> List[Dict]:
    """
    获取所有待审批的媒体请求，并关联请求者的用户名。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT r.*, u.name as requested_by_username
                    FROM media_requests r
                    LEFT JOIN emby_users u ON r.requested_by_user_id = u.id
                    WHERE r.status = 'pending'
                    ORDER BY r.requested_at ASC
                """)
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取待审批请求列表时出错: {e}", exc_info=True)
        raise

def get_request_by_id(request_id: int) -> Optional[Dict]:
    """
    根据ID获取单个媒体请求的详细信息。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM media_requests WHERE id = %s", (request_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.error(f"根据ID {request_id} 获取请求详情时出错: {e}", exc_info=True)
        raise

def update_request_status(request_id: int, status: str, admin_notes: Optional[str] = None) -> int:
    """
    更新单个媒体请求的状态和可选的管理员备注。
    返回受影响的行数。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE media_requests SET status = %s, admin_notes = %s WHERE id = %s",
                    (status, admin_notes, request_id)
                )
                return cursor.rowcount
    except Exception as e:
        logger.error(f"更新请求 {request_id} 状态时出错: {e}", exc_info=True)
        raise