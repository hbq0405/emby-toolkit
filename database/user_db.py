# database/user_db.py
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 用户数据访问
# ======================================================================

def upsert_user_media_data(data: Dict[str, Any]):
    """【V1】根据Webhook传入的数据，更新或插入单条用户媒体状态。"""
    
    user_id = data.get('user_id')
    item_id = data.get('item_id')
    if not user_id or not item_id:
        return

    data['last_updated_at'] = datetime.now(timezone.utc)
    set_clauses = [f"{key} = EXCLUDED.{key}" for key in data.keys() if key not in ['user_id', 'item_id']]
    
    columns = list(data.keys())
    columns_str = ', '.join(columns)
    placeholders_str = ', '.join(['%s'] * len(columns))
    
    sql = f"""
        INSERT INTO user_media_data ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (user_id, item_id) DO UPDATE SET
            {', '.join(set_clauses)};
    """
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(data.values()))
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 更新用户媒体数据失败 for user {user_id}, item {item_id}: {e}", exc_info=True)
        raise

def upsert_user_media_data_batch(user_id: str, items_data: List[Dict[str, Any]]):
    """【V1】为一个指定用户，批量更新或插入其所有媒体的状态。"""
    
    if not user_id or not items_data:
        return

    sql = """
        INSERT INTO user_media_data (
            user_id, item_id, is_favorite, played, playback_position_ticks, 
            play_count, last_played_date, last_updated_at
        ) VALUES %s
        ON CONFLICT (user_id, item_id) DO UPDATE SET
            is_favorite = EXCLUDED.is_favorite,
            played = EXCLUDED.played,
            playback_position_ticks = EXCLUDED.playback_position_ticks,
            play_count = EXCLUDED.play_count,
            last_played_date = EXCLUDED.last_played_date,
            last_updated_at = EXCLUDED.last_updated_at;
    """
    
    values_to_insert = []
    now_utc = datetime.now(timezone.utc)
    for item in items_data:
        user_data = item.get('UserData', {})
        values_to_insert.append((
            user_id,
            item.get('Id'),
            user_data.get('IsFavorite', False),
            user_data.get('Played', False),
            user_data.get('PlaybackPositionTicks', 0),
            user_data.get('PlayCount', 0),
            user_data.get('LastPlayedDate'),
            now_utc
        ))

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            execute_values(cursor, sql, values_to_insert, page_size=1000)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新用户 {user_id} 的媒体数据时失败: {e}", exc_info=True)
        raise

def get_all_emby_users() -> List[Dict[str, Any]]:
    """获取本地缓存的所有Emby用户信息。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM emby_users ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取本地Emby用户缓存失败: {e}", exc_info=True)
        return []

def upsert_emby_users_batch(users_data: List[Dict[str, Any]]):
    """批量更新或插入Emby用户信息到本地缓存。"""
    
    if not users_data:
        return

    sql = """
        INSERT INTO emby_users (id, name, is_administrator, last_seen_at, profile_image_tag, last_updated_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            is_administrator = EXCLUDED.is_administrator,
            last_seen_at = EXCLUDED.last_seen_at,
            profile_image_tag = EXCLUDED.profile_image_tag,
            last_updated_at = EXCLUDED.last_updated_at;
    """
    
    values_to_insert = []
    now_utc = datetime.now(timezone.utc)
    for user in users_data:
        values_to_insert.append((
            user.get('Id'),
            user.get('Name'),
            user.get('Policy', {}).get('IsAdministrator', False),
            user.get('LastActivityDate'),
            user.get('PrimaryImageTag'),
            now_utc
        ))

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            execute_values(cursor, sql, values_to_insert, page_size=100)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新Emby用户缓存失败: {e}", exc_info=True)
        raise

def get_item_ids_by_dynamic_rules(user_id: str, rules: List[Dict[str, Any]]) -> Optional[List[str]]:
    """【V2 - 时间维度版】根据动态筛选规则获取匹配的媒体项ID列表。"""
    
    if not user_id or not rules:
        return []

    base_sql = "SELECT item_id FROM user_media_data WHERE user_id = %s"
    where_clauses = []
    params = [user_id]

    for rule in rules:
        field = rule.get("field")
        op = rule.get("operator", "is")
        value = rule.get("value")

        if field == 'is_favorite':
            if op == 'is': where_clauses.append("is_favorite = %s")
            elif op == 'is_not': where_clauses.append("is_favorite != %s")
            params.append(value)
        
        elif field == 'playback_status':
            condition = ""
            if value == 'played': condition = "played = TRUE"
            elif value == 'in_progress': condition = "(played = FALSE AND playback_position_ticks > 0)"
            elif value == 'unplayed': condition = "(played = FALSE AND (playback_position_ticks = 0 OR playback_position_ticks IS NULL))"
            else: continue
            
            if op == 'is': where_clauses.append(condition)
            elif op == 'is_not': where_clauses.append(f"NOT ({condition})")

    if not where_clauses:
        return []

    final_sql = f"{base_sql} AND {' AND '.join(where_clauses)}"
    
    logger.trace(f"执行动态筛选SQL: {final_sql} with params: {params}")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(final_sql, tuple(params))
            rows = cursor.fetchall()
            return [row['item_id'] for row in rows]
    except Exception as e:
        logger.error(f"DB: 根据动态规则获取媒体ID时失败 for user {user_id}: {e}", exc_info=True)
        return None
    
def get_all_local_emby_user_ids() -> set:
    """获取本地数据库中所有 emby_users 的 ID 集合。"""
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM emby_users")
            return {row['id'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"DB: 获取所有本地用户ID时失败: {e}", exc_info=True)
        return set()

def delete_emby_users_by_ids(user_ids: List[str]) -> int:
    """根据用户ID列表，从 emby_users 表中批量删除用户。"""
    
    if not user_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM emby_users WHERE id = ANY(%s)"
            cursor.execute(sql, (user_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"  ➜ 从本地数据库中同步删除了 {deleted_count} 个陈旧的用户记录。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除陈旧用户时失败: {e}", exc_info=True)
        raise

def upsert_user_media_data_batch_no_date(user_id: str, items_data: List[Dict[str, Any]]):
    """【V1 - 精准夺权版】批量更新用户媒体状态，但排除 last_played_date。"""
    
    if not user_id or not items_data:
        return

    sql = """
        INSERT INTO user_media_data (
            user_id, item_id, is_favorite, played, playback_position_ticks, 
            play_count, last_updated_at
        ) VALUES %s
        ON CONFLICT (user_id, item_id) DO UPDATE SET
            is_favorite = EXCLUDED.is_favorite,
            played = EXCLUDED.played,
            playback_position_ticks = EXCLUDED.playback_position_ticks,
            play_count = EXCLUDED.play_count,
            last_updated_at = EXCLUDED.last_updated_at;
    """
    
    values_to_insert = []
    now_utc = datetime.now(timezone.utc)
    for item in items_data:
        user_data = item.get('UserData', {})
        values_to_insert.append((
            user_id,
            item.get('Id'),
            user_data.get('IsFavorite', False),
            user_data.get('Played', False),
            user_data.get('PlaybackPositionTicks', 0),
            user_data.get('PlayCount', 0),
            now_utc
        ))

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            execute_values(cursor, sql, values_to_insert, page_size=1000)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新用户 {user_id} 的媒体数据时失败 (no_date): {e}", exc_info=True)
        raise

def get_user_display_name(user_id: str) -> str:
    """
    根据用户ID，从数据库中快速获取其显示名称。
    """
    sql = "SELECT name FROM emby_users WHERE id = %s"
    try:
        from .connection import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return result['name'] if result else None
    except Exception:
        # 如果查询失败，静默处理，返回 None
        return None