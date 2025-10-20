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
    """
    【V3 - 健壮修复版】根据动态筛选规则获取匹配的媒体项ID列表。
    - 修复了当规则为空时，错误地返回空列表导致虚拟库变空的问题。
    - 增强了日志，便于问题排查。
    """
    
    # --- 核心修复 #1: 如果没有提供规则，应返回 None 来告知调用方“跳过筛选” ---
    if not user_id or not rules:
        return None

    base_sql = "SELECT item_id FROM user_media_data WHERE user_id = %s"
    where_clauses = []
    params = [user_id]

    for rule in rules:
        field = rule.get("field")
        op = rule.get("operator")
        value = rule.get("value")

        # 跳过不完整的规则
        if not all([field, op]):
            continue

        if field == 'is_favorite':
            # is_favorite 的值必须是布尔值
            if not isinstance(value, bool):
                logger.warning(f"动态筛选规则'is_favorite'的值不是布尔值，已跳过: {rule}")
                continue
            
            if op == 'is': where_clauses.append("is_favorite = %s")
            elif op == 'is_not': where_clauses.append("is_favorite != %s")
            else: continue # 无效的操作符
            params.append(value)
        
        elif field == 'playback_status':
            condition = ""
            if value == 'played': condition = "played = TRUE"
            elif value == 'in_progress': condition = "(played = FALSE AND playback_position_ticks > 0)"
            elif value == 'unplayed': condition = "(played = FALSE AND (playback_position_ticks = 0 OR playback_position_ticks IS NULL))"
            else:
                logger.warning(f"动态筛选规则'playback_status'的值无效，已跳过: {rule}")
                continue
            
            if op == 'is': where_clauses.append(condition)
            elif op == 'is_not': where_clauses.append(f"NOT ({condition})")

    # --- 核心修复 #2: 如果所有规则都无效，也应返回 None 来“跳过筛选” ---
    if not where_clauses:
        logger.warning(f"为用户 {user_id} 提供了动态规则，但无法解析为有效的SQL条件。规则: {rules}")
        return None

    final_sql = f"{base_sql} AND {' AND '.join(where_clauses)}"
    
    logger.debug(f"执行动态筛选SQL: {final_sql} with params: {params}")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(final_sql, tuple(params))
            rows = cursor.fetchall()
            item_ids = [row['item_id'] for row in rows]
            logger.debug(f"动态筛选为用户 {user_id} 找到了 {len(item_ids)} 个匹配的媒体项。")
            return item_ids
    except Exception as e:
        logger.error(f"DB: 根据动态规则获取媒体ID时失败 for user {user_id}: {e}", exc_info=True)
        return None # 发生错误时返回 None
    
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

def get_username_by_id(user_id: str) -> Optional[str]:
    """根据用户ID从本地缓存中获取用户名。"""
    
    if not user_id:
        return None
    
    sql = "SELECT name FROM emby_users WHERE id = %s"
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            if result:
                return result['name']
            return None
    except Exception as e:
        logger.error(f"DB: 根据ID '{user_id}' 获取用户名失败: {e}", exc_info=True)
        return None
    
def get_all_emby_users_with_template_info() -> List[Dict[str, Any]]:
    """
    【V2 - 智能关联版】获取所有Emby用户信息，并通过 LEFT JOIN 关联扩展表来获取模板信息。
    这个版本不再依赖 emby_users 表自身有 template_user_id 字段。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = """
                SELECT
                    u.id,
                    u.name,
                    ue.template_id
                FROM
                    emby_users u
                LEFT JOIN
                    emby_users_extended ue ON u.id = ue.emby_user_id
                ORDER BY
                    u.name;
            """
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取带模板信息的Emby用户列表失败 (V2): {e}", exc_info=True)
        return []

def expand_template_user_ids(selected_user_ids: List[str]) -> List[str]:
    """
    【V2 - 智能关联版】接收一个用户ID列表，自动展开其中的模板源用户。
    这个版本通过查询 user_templates 和 emby_users_extended 表来实现。
    """
    if not selected_user_ids:
        return []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 找出 selected_user_ids 中哪些是“模板源”用户
            # “模板源”用户是指其 ID 被记录在 user_templates.source_emby_user_id 字段中的用户
            sql_find_sources = "SELECT id FROM user_templates WHERE source_emby_user_id = ANY(%s)"
            cursor.execute(sql_find_sources, (selected_user_ids,))
            source_template_ids = {row['id'] for row in cursor.fetchall()}

            final_user_ids = set(selected_user_ids)

            # 2. 如果找到了模板源，就去查找所有绑定了这些模板的“子用户”
            if source_template_ids:
                sql_find_children = "SELECT emby_user_id FROM emby_users_extended WHERE template_id = ANY(%s)"
                cursor.execute(sql_find_children, (list(source_template_ids),))
                child_user_ids = {row['emby_user_id'] for row in cursor.fetchall()}
                final_user_ids.update(child_user_ids)
            
            logger.debug(f"模板用户展开： 原始选择 {len(selected_user_ids)} 人, 展开后共 {len(final_user_ids)} 人。")
            return list(final_user_ids)
            
    except Exception as e:
        logger.error(f"DB: 展开模板用户ID时失败 (V2): {e}", exc_info=True)
        # 出错时，保守地返回原始列表
        return selected_user_ids