# tasks/session_tasks.py
import logging
from datetime import datetime, timedelta, timezone
from database.connection import get_db_connection

logger = logging.getLogger(__name__)

def task_cleanup_stale_sessions():
    """
    【后台任务】定期清理陈旧的、卡住的播放会话。
    这是一个保证并发控制系统健壮性的保险丝。
    """
    # 定义一个会话的最大存活时间，例如 15 分钟
    # 意味着如果一个会话的心跳在15分钟内没有被任何方式更新，就认为它已经失效
    STALE_THRESHOLD_MINUTES = 15
    
    threshold_time = datetime.now(timezone.utc) - timedelta(minutes=STALE_THRESHOLD_MINUTES)
    
    logger.info("  ➜ 正在执行陈旧播放会话清理任务...")
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM active_sessions WHERE last_updated_at < %s"
            cursor.execute(sql, (threshold_time,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.warning(f"  ➜ 清理任务完成：移除了 {deleted_count} 个陈旧的播放会话。")
            else:
                logger.info("  ➜ 清理任务完成：未发现需要清理的陈旧会话。")
                
    except Exception as e:
        logger.error(f"执行陈旧会话清理任务时发生数据库错误: {e}", exc_info=True)