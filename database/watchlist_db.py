# database/watchlist_db.py
import psycopg2
import logging
from typing import List, Dict, Any, Optional

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 追剧数据访问
# ======================================================================

def get_all_watchlist_items() -> List[Dict[str, Any]]:
    """获取所有追剧列表中的项目。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM watchlist ORDER BY added_at DESC")
            items = [dict(row) for row in cursor.fetchall()]
            return items
    except Exception as e:
        logger.error(f"DB: 获取追剧列表失败: {e}", exc_info=True)
        raise

def get_watchlist_item_name(item_id: str) -> Optional[str]:
    """根据 item_id 获取单个追剧项目的名称。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name FROM watchlist WHERE item_id = %s", (item_id,))
            row = cursor.fetchone()
            return row['item_name'] if row else None
    except Exception as e:
        logger.warning(f"DB: 获取项目 {item_id} 名称时出错: {e}")
        return None

def add_item_to_watchlist(item_id: str, tmdb_id: str, item_name: str, item_type: str) -> bool:
    """【V2 - PG语法修复版】添加一个新项目到追剧列表。"""
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO watchlist (item_id, tmdb_id, item_name, item_type, status, last_checked_at)
                    VALUES (%s, %s, %s, %s, 'Watching', NULL)
                    ON CONFLICT (item_id) DO UPDATE SET
                        tmdb_id = EXCLUDED.tmdb_id,
                        item_name = EXCLUDED.item_name,
                        item_type = EXCLUDED.item_type,
                        status = EXCLUDED.status,
                        last_checked_at = EXCLUDED.last_checked_at;
                """
                cursor.execute(sql, (item_id, tmdb_id, item_name, item_type))
            conn.commit()
            logger.info(f"DB: 项目 '{item_name}' (ID: {item_id}) 已成功添加/更新到追剧列表。")
            return True
    except Exception as e:
        logger.error(f"DB: 手动添加项目到追剧列表时发生错误: {e}", exc_info=True)
        raise

def update_watchlist_item_status(item_id: str, new_status: str) -> bool:
    """更新追剧列表中某个项目的状态。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE watchlist SET status = %s WHERE item_id = %s",
                (new_status, item_id)
            )
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"DB: 项目 {item_id} 的追剧状态已更新为 '{new_status}'。")
                return True
            else:
                logger.warning(f"DB: 尝试更新追剧状态，但未在列表中找到项目 {item_id}。")
                return False
    except Exception as e:
        logger.error(f"DB: 更新追剧状态时发生错误: {e}", exc_info=True)
        raise

def remove_item_from_watchlist(item_id: str) -> bool:
    """从追剧列表中移除一个项目。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watchlist WHERE item_id = %s", (item_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.trace(f"DB: 项目 {item_id} 已从追剧列表移除。")
                return True
            else:
                logger.warning(f"DB: 尝试删除项目 {item_id}，但在追剧列表中未找到。")
                return False
    except psycopg2.OperationalError as e:
        if "database is locked" in str(e).lower():
            logger.error(f"DB: 从追剧列表移除项目时发生数据库锁定错误: {e}", exc_info=True)
        else:
            logger.error(f"DB: 从追剧列表移除项目时发生数据库操作错误: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"DB: 从追剧列表移除项目时发生未知错误: {e}", exc_info=True)
        raise

def batch_force_end_watchlist_items(item_ids: List[str]) -> int:
    """【V2】批量将追剧项目标记为“强制完结”。"""
    
    if not item_ids:
        return 0
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('%s' for _ in item_ids)
            sql = f"UPDATE watchlist SET status = 'Completed', force_ended = TRUE WHERE item_id IN ({placeholders})"
            
            cursor.execute(sql, item_ids)
            conn.commit()
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"DB: 批量强制完结了 {updated_count} 个追剧项目。")
            else:
                logger.warning(f"DB: 尝试批量强制完结，但提供的ID在列表中均未找到。")
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量强制完结追剧项目时发生错误: {e}", exc_info=True)
        raise

def batch_update_watchlist_status(item_ids: list, new_status: str) -> int:
    """【V2 - 时间格式修复版】批量更新指定项目ID列表的追剧状态。"""
    
    if not item_ids:
        return 0
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            updates = { "status": new_status }
            
            if new_status == 'Watching':
                updates["paused_until"] = None
                updates["force_ended"] = False
            
            set_clauses = [f"{key} = %s" for key in updates.keys()]
            set_clauses.append("last_checked_at = NOW()")
            
            values = list(updates.values())
            
            placeholders = ', '.join(['%s'] * len(item_ids))
            sql = f"UPDATE watchlist SET {', '.join(set_clauses)} WHERE item_id IN ({placeholders})"
            
            values.extend(item_ids)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            logger.info(f"DB: 成功将 {cursor.rowcount} 个项目的状态批量更新为 '{new_status}'。")
            return cursor.rowcount
            
    except Exception as e:
        logger.error(f"批量更新项目状态时数据库出错: {e}", exc_info=True)
        raise

def get_watching_tmdb_ids() -> set:
    """获取所有正在追看（状态为 'Watching'）的剧集的 TMDB ID 集合。"""
    
    watching_ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id FROM watchlist WHERE status = 'Watching'")
            rows = cursor.fetchall()
            for row in rows:
                watching_ids.add(str(row['tmdb_id']))
    except Exception as e:
        logger.error(f"从数据库获取正在追看的TMDB ID时出错: {e}", exc_info=True)
    return watching_ids

def update_resubscribe_info(item_id: str, season_number: int, timestamp: str):
    """
    更新或插入特定季的最后一次洗版订阅时间。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 使用 PostgreSQL 的 jsonb_set 函数来更新 JSONB 字段
                update_query = """
                    UPDATE watchlist
                    SET resubscribe_info_json = jsonb_set(
                        COALESCE(resubscribe_info_json, '{}'::jsonb),
                        %s,
                        %s::jsonb,
                        true
                    )
                    WHERE item_id = %s
                """
                cursor.execute(update_query, ([str(season_number)], f'"{timestamp}"', item_id))
            conn.commit()
            logger.info(f"  ➜ 已记录 ItemID {item_id} 第 {season_number} 季的洗版订阅时间。")
    except Exception as e:
        logger.error(f"更新 ItemID {item_id} 第 {season_number} 季的洗版订阅时间时出错: {e}", exc_info=True)