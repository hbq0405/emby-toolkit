# database/session_db.py
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 播放会话数据访问 (用于并发控制)
# ======================================================================

def start_session(session_data: Dict[str, Any]) -> bool:
    """
    记录一个新的播放会话开始。
    使用 ON CONFLICT DO UPDATE 来处理Emby可能重复发送start事件的极端情况。
    """
    session_id = session_data.get("session_id")
    if not session_id:
        logger.error("DB: 尝试开始一个没有 session_id 的会话，已忽略。")
        return False

    now = datetime.now(timezone.utc)
    session_data['started_at'] = now
    session_data['last_updated_at'] = now
    
    columns = session_data.keys()
    columns_str = ', '.join(columns)
    placeholders_str = ', '.join([f"%({key})s" for key in columns])
    
    # 如果冲突，只更新心跳时间
    sql = f"""
        INSERT INTO active_sessions ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (session_id) DO UPDATE SET
            last_updated_at = EXCLUDED.last_updated_at;
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, session_data)
            conn.commit()
            logger.info(f"  ➜ DB: 已记录用户 '{session_data.get('emby_user_id')}' 的新播放会话 (ID: {session_id})。")
            return True
    except Exception as e:
        logger.error(f"DB: 记录新播放会话 {session_id} 时失败: {e}", exc_info=True)
        return False

def stop_session(session_id: str) -> bool:
    """
    根据 session_id 删除一个已结束的播放会话。
    """
    if not session_id:
        return False
        
    sql = "DELETE FROM active_sessions WHERE session_id = %s"
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (session_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"  ➜ DB: 已移除播放会话 (ID: {session_id})。")
            return True
    except Exception as e:
        logger.error(f"DB: 移除播放会话 {session_id} 时失败: {e}", exc_info=True)
        return False

def get_active_session_count(user_id: str) -> int:
    """
    获取指定用户当前活跃的播放会话数量。
    这是并发检查的核心函数。
    """
    sql = "SELECT COUNT(*) as count FROM active_sessions WHERE emby_user_id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return result['count'] if result else 0
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的活跃会话数时失败: {e}", exc_info=True)
        # 在出错的情况下，返回一个很大的数，倾向于阻止播放，更安全
        return 999

def get_user_stream_limit(user_id: str) -> Optional[int]:
    """
    获取用户的最大并发流限制。
    数据源自用户关联的模板。
    """
    # 这个查询稍微复杂，需要 JOIN emby_users_extended 和 user_templates
    sql = """
        SELECT t.max_concurrent_streams
        FROM emby_users_extended e
        JOIN user_templates t ON e.template_id = t.id
        WHERE e.emby_user_id = %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            # 如果用户没有模板或模板没有设置，返回 None 代表“无限制”
            return result['max_concurrent_streams'] if result else None
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的并发限制时失败: {e}", exc_info=True)
        # 出错时返回 0，也会阻止播放
        return 0