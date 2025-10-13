# database/session_db.py
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 播放会话数据访问 (用于并发控制)
# ======================================================================

def start_session(session_data: Dict[str, Any]) -> bool:
    """【V2】记录一个新的播放会话开始，使用 device_id 作为主键。"""
    device_id = session_data.get("device_id")
    if not device_id:
        logger.error("DB: 尝试开始一个没有 device_id 的会话，已忽略。")
        return False

    now = datetime.now(timezone.utc)
    session_data['started_at'] = now
    session_data['last_updated_at'] = now
    
    columns = session_data.keys()
    columns_str = ', '.join(columns)
    placeholders_str = ', '.join([f"%({key})s" for key in columns])
    
    # ON CONFLICT 的目标改为 device_id
    sql = f"""
        INSERT INTO active_sessions ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (device_id) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            item_id = EXCLUDED.item_id,
            item_name = EXCLUDED.item_name,
            last_updated_at = EXCLUDED.last_updated_at;
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, session_data)
            conn.commit()
            logger.info(f"  ➜ DB: 已记录/更新 用户 '{session_data.get('emby_user_id')}' 在设备 '{device_id}' 上的播放会话。")
            return True
    except Exception as e:
        logger.error(f"DB: 记录播放会话 (设备ID: {device_id}) 时失败: {e}", exc_info=True)
        return False

def stop_session(device_id: str) -> bool:
    """【V2】根据 device_id 删除一个已结束的播放会话。"""
    if not device_id:
        return False
        
    sql = "DELETE FROM active_sessions WHERE device_id = %s"
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (device_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"  ➜ DB: 已移除播放会话 (设备ID: {device_id})。")
            return True
    except Exception as e:
        logger.error(f"DB: 移除播放会话 (设备ID: {device_id}) 时失败: {e}", exc_info=True)
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
    
def get_active_sessions(user_id: str) -> list:
    """
    获取指定用户的所有活动会话的详细列表，并按开始时间升序排序。
    """
    # ★★★ 核心修正：查询出所有关键信息，并按时间排序 ★★★
    sql = "SELECT device_id, session_id, started_at FROM active_sessions WHERE emby_user_id = %s ORDER BY started_at ASC"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            results = cursor.fetchall()
            return results if results else []
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的活跃会话列表时失败: {e}", exc_info=True)
        return []

def get_user_stream_limit(user_id: str) -> Optional[int]:
    """
    【V2 - 健壮版】获取用户的最大并发流限制。
    - 优先处理管理员，直接给予无限制。
    - 使用 LEFT JOIN 保证即使没有模板信息也能查询到用户。
    """
    # 这个查询会同时获取用户的管理员状态和模板并发数
    sql = """
        SELECT 
            u.is_administrator,
            t.max_concurrent_streams
        FROM emby_users u
        LEFT JOIN emby_users_extended e ON u.id = e.emby_user_id
        LEFT JOIN user_templates t ON e.template_id = t.id
        WHERE u.id = %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()

            if not result:
                # 如果连 emby_users 表里都找不到，说明是幽灵用户，直接禁止
                logger.warning(f"DB: 尝试查询一个不存在的用户 {user_id} 的并发限制。")
                return 1 # 返回一个严格的限制

            # 1. 首先判断是不是管理员
            if result.get('is_administrator'):
                logger.trace(f"用户 {user_id} 是管理员，并发无限制。")
                return 0  # 0 代表无限制

            # 2. 如果不是管理员，再看模板的设置
            limit = result.get('max_concurrent_streams')
            
            # 如果 limit 是 NULL (比如用户有扩展信息但没绑模板)，也返回 None
            return limit

    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的并发限制时失败: {e}", exc_info=True)
        # 出错时返回 1，倾向于阻止播放，更安全
        return 1
    
def get_active_session_details(user_id: str) -> List[Dict[str, Any]]:
    """获取指定用户所有活跃会话的详细信息。"""
    if not user_id:
        return []
    sql = "SELECT session_id, device_id, client_name FROM sessions WHERE user_id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取用户 {user_id} 的活跃会话详情失败: {e}", exc_info=True)
        return []

def delete_sessions_by_ids(session_ids: List[str]):
    """根据会话ID列表，批量删除会话记录。"""
    if not session_ids:
        return
    sql = "DELETE FROM sessions WHERE session_id = ANY(%s)"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (session_ids,))
            conn.commit()
            logger.info(f"  ➜ [实时清理] 成功从数据库中清除了 {cursor.rowcount} 个僵尸会话。")
    except Exception as e:
        logger.error(f"DB: 批量删除会话失败: {e}", exc_info=True)