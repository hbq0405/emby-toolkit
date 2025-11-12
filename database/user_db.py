# database/user_db.py
import psycopg2
import uuid
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Any, Optional, Tuple # 导入 Tuple
from datetime import datetime, timezone, timedelta

from .connection import get_db_connection

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 用户数据访问
# ======================================================================

def upsert_user_media_data(data: Dict[str, Any]):
    """【V2 - 健壮版】根据Webhook传入的数据，更新或插入单条用户媒体状态。
    - 明确列出所有要更新的字段，避免因 webhook 负载变化而遗漏数据。
    """
    user_id = data.get('user_id')
    item_id = data.get('item_id')
    if not user_id or not item_id:
        return

    # 准备所有可能更新的字段
    update_data = {
        'is_favorite': data.get('is_favorite'),
        'played': data.get('played'),
        'playback_position_ticks': data.get('playback_position_ticks'),
        'play_count': data.get('play_count'), # 确保 play_count 在这里
        'last_played_date': data.get('last_played_date'),
        'last_updated_at': datetime.now(timezone.utc)
    }

    # 过滤掉值为 None 的字段，这样就不会用 None 覆盖掉数据库中已有的值
    update_data = {k: v for k, v in update_data.items() if v is not None}

    if not update_data:
        logger.warning(f"Webhook 为 user {user_id}, item {item_id} 传来的数据为空，跳过更新。")
        return

    # 动态构建 SQL
    columns = ['user_id', 'item_id'] + list(update_data.keys())
    set_clauses = [f"{key} = EXCLUDED.{key}" for key in update_data.keys()]
    
    columns_str = ', '.join(columns)
    placeholders_str = ', '.join(['%s'] * len(columns))
    
    sql = f"""
        INSERT INTO user_media_data ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (user_id, item_id) DO UPDATE SET
            {', '.join(set_clauses)};
    """
    
    values = (user_id, item_id) + tuple(update_data.values())

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
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
            # ★★★ 正确的位置：直接从顶层 item 对象里获取 LastPlayedDate ★★★
            item.get('LastPlayedDate'), 
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
    
def get_admin_telegram_chat_ids():
    """
    查询数据库，获取所有在emby_users表中标记为管理员，
    且在emby_users_extended表中配置了Telegram Chat ID的用户ID列表。
    
    Returns:
        list: 一个包含所有符合条件的管理员Telegram Chat ID的字符串列表。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 使用 JOIN 联结两张表，高效查询
                query = """
                    SELECT ext.telegram_chat_id
                    FROM emby_users AS base
                    JOIN emby_users_extended AS ext ON base.id = ext.emby_user_id
                    WHERE base.is_administrator = TRUE
                      AND ext.telegram_chat_id IS NOT NULL AND ext.telegram_chat_id != ''
                """
                cursor.execute(query)
                
                # RealDictCursor 返回的是字典列表，我们提取 'telegram_chat_id' 的值
                admin_ids = [row['telegram_chat_id'] for row in cursor.fetchall()]
                
                if admin_ids:
                    logger.debug(f"  ➜ 查询到 {len(admin_ids)} 个管理员的 Telegram Chat ID。")
                else:
                    logger.debug("  ➜ 未查询到任何已配置Telegram的管理员账户。")
                    
                return admin_ids
                
    except Exception as e:
        logger.error(f"查询管理员Telegram Chat ID时出错: {e}", exc_info=True)
        return [] # 出错时返回空列表，保证安全
    
def is_user_admin(user_id: str) -> bool:
    """
    检查一个指定的用户ID是否为Emby管理员。
    """
    if not user_id:
        return False
        
    # 这个函数遵循“用完即走”的原则
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 我们需要从 emby_users 表中查询 is_administrator 标志
                cursor.execute("SELECT is_administrator FROM emby_users WHERE id = %s", (user_id,))
                user_record = cursor.fetchone()
                
                # 如果找到了记录，并且 is_administrator 为 True，则返回 True
                if user_record and user_record['is_administrator']:
                    return True
                
                # 其他所有情况都返回 False
                return False
    except Exception as e:
        logger.error(f"检查用户 {user_id} 管理员权限时发生数据库错误: {e}", exc_info=True)
        # 发生任何错误时，都应安全地返回 False
        return False
    
def get_user_count() -> int:
    """
    获取 users 表中的用户总数。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM users")
                result = cursor.fetchone()
                return result['count'] if result else 0
    except Exception as e:
        logger.error(f"获取用户总数时出错: {e}", exc_info=True)
        return 0 # 出错时安全返回0

def create_initial_admin_user(username: str, password_hash: str):
    """
    创建一个初始的本地管理员用户。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    (username, password_hash)
                )
    except Exception as e:
        logger.error(f"创建初始管理员 '{username}' 时出错: {e}", exc_info=True)
        raise # 将异常向上抛出，让调用者知道操作失败

def get_local_user_by_username(username: str) -> Optional[Dict]:
    """
    根据用户名从 users 表获取本地用户信息。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
                user = cursor.fetchone()
                return dict(user) if user else None
    except Exception as e:
        logger.error(f"根据用户名 '{username}' 获取本地用户时出错: {e}", exc_info=True)
        return None

def get_local_user_by_id(user_id: int) -> Optional[Dict]:
    """
    根据ID从 users 表获取本地用户信息。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
                user = cursor.fetchone()
                return dict(user) if user else None
    except Exception as e:
        logger.error(f"根据ID {user_id} 获取本地用户时出错: {e}", exc_info=True)
        return None

def update_local_user_password(user_id: int, new_password_hash: str):
    """
    更新本地用户的密码哈希。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (new_password_hash, user_id))
    except Exception as e:
        logger.error(f"更新用户 {user_id} 密码时出错: {e}", exc_info=True)
        raise

def get_user_subscription_permission(user_id: str) -> bool:
    """
    根据用户ID，查询其所属模板是否允许免审订阅。
    """
    sql = """
        SELECT t.allow_unrestricted_subscriptions
        FROM emby_users_extended ue
        JOIN user_templates t ON ue.template_id = t.id
        WHERE ue.emby_user_id = %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            # 如果能查到记录，并且值为 True，则返回 True
            return result['allow_unrestricted_subscriptions'] if result else False
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅权限失败: {e}", exc_info=True)
        return False # 出错时，保守地返回 False
    
# ======================================================================
# 模块: 用户通知
# ======================================================================
def update_user_telegram_chat_id(user_id: str, chat_id: str) -> bool:
    """更新或设置用户的 Telegram Chat ID"""
    # 确保空字符串存为 NULL，方便处理
    chat_id_to_save = chat_id if chat_id and chat_id.strip() else None
    sql = "UPDATE emby_users_extended SET telegram_chat_id = %s WHERE emby_user_id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (chat_id_to_save, user_id))
            # 如果用户在 extended 表里不存在，则需要插入一条新记录
            if cursor.rowcount == 0:
                insert_sql = "INSERT INTO emby_users_extended (emby_user_id, telegram_chat_id) VALUES (%s, %s) ON CONFLICT (emby_user_id) DO UPDATE SET telegram_chat_id = EXCLUDED.telegram_chat_id"
                cursor.execute(insert_sql, (user_id, chat_id_to_save))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"DB: 更新用户 {user_id} 的 Telegram Chat ID 失败: {e}")
        return False

def get_user_telegram_chat_id(user_id: str) -> Optional[str]:
    """根据用户ID获取其 Telegram Chat ID"""
    sql = "SELECT telegram_chat_id FROM emby_users_extended WHERE emby_user_id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return result['telegram_chat_id'] if result else None
    except Exception as e:
        logger.error(f"DB: 获取用户 {user_id} 的 Telegram Chat ID 失败: {e}")
        return None

# ======================================================================
# 模块: 用户模板管理 (User Templates)
# ======================================================================
def get_template_source_user_ids() -> set:
    """
    从 user_templates 表中获取所有被用作模板源用户的ID集合。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT source_emby_user_id FROM user_templates WHERE source_emby_user_id IS NOT NULL")
                # 返回一个集合(set)，用于实现 O(1) 的高效查找
                return {row['source_emby_user_id'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"获取模板源用户ID列表时出错: {e}", exc_info=True)
        return set() # 出错时返回空集合，确保安全

def get_all_user_templates() -> List[Dict]:
    """获取所有用户模板。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, name, description, default_expiration_days, source_emby_user_id, allow_unrestricted_subscriptions
                    FROM user_templates ORDER BY name
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取所有用户模板时出错: {e}", exc_info=True)
        raise

def create_user_template(name, description, policy_json, default_expiration_days, source_emby_user_id, configuration_json, allow_unrestricted_subscriptions) -> int:
    """创建一个新的用户模板。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO user_templates (name, description, emby_policy_json, default_expiration_days, source_emby_user_id, emby_configuration_json, allow_unrestricted_subscriptions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """,
                    (name, description, policy_json, default_expiration_days, source_emby_user_id, configuration_json, allow_unrestricted_subscriptions)
                )
                new_row = cursor.fetchone()
                if not new_row:
                    raise Exception("数据库 INSERT 后未能返回新模板的ID。")
                return new_row['id']
    except Exception as e:
        logger.error(f"创建用户模板 '{name}' 时出错: {e}", exc_info=True)
        raise

def get_template_for_sync(template_id: int) -> Optional[Dict]:
    """获取用于同步的模板信息。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT source_emby_user_id, name, emby_configuration_json IS NOT NULL as has_config FROM user_templates WHERE id = %s", (template_id,))
                template = cursor.fetchone()
                return dict(template) if template else None
    except Exception as e:
        logger.error(f"获取待同步模板 {template_id} 信息时出错: {e}", exc_info=True)
        raise

def update_template_from_sync(template_id: int, new_policy_json: str, new_config_json: Optional[str]):
    """从源用户同步后，更新模板的 policy 和 configuration。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE user_templates SET emby_policy_json = %s, emby_configuration_json = %s WHERE id = %s",
                    (new_policy_json, new_config_json, template_id)
                )
    except Exception as e:
        logger.error(f"同步更新模板 {template_id} 时出错: {e}", exc_info=True)
        raise

def get_users_associated_with_template(template_id: int) -> List[Dict]:
    """获取所有使用指定模板的用户列表。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT u.id, u.name FROM emby_users_extended uex JOIN emby_users u ON uex.emby_user_id = u.id WHERE uex.template_id = %s",
                    (template_id,)
                )
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取模板 {template_id} 关联用户时出错: {e}", exc_info=True)
        raise

def delete_user_template(template_id: int) -> int:
    """删除一个用户模板，返回受影响行数。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM user_templates WHERE id = %s", (template_id,))
                return cursor.rowcount
    except Exception as e:
        logger.error(f"删除模板 {template_id} 时出错: {e}", exc_info=True)
        raise

def update_user_template_details(template_id, name, description, default_expiration_days, allow_unrestricted_subscriptions) -> int:
    """更新用户模板的详细信息，返回受影响行数。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE user_templates
                    SET name = %s, description = %s, default_expiration_days = %s, allow_unrestricted_subscriptions = %s
                    WHERE id = %s
                    """,
                    (name, description, default_expiration_days, allow_unrestricted_subscriptions, template_id)
                )
                return cursor.rowcount
    except Exception as e:
        logger.error(f"更新模板 {template_id} 时出错: {e}", exc_info=True)
        raise

# ======================================================================
# 模块: 邀请链接管理 (Invitations)
# ======================================================================

def create_invitation_link(template_id, expiration_days, link_expires_in_days) -> str:
    """创建一个新的邀请链接，并返回生成的token。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                final_expiration_days = expiration_days
                if final_expiration_days is None:
                    cursor.execute("SELECT default_expiration_days FROM user_templates WHERE id = %s", (template_id,))
                    template = cursor.fetchone()
                    if not template:
                        raise ValueError("模板不存在")
                    final_expiration_days = template['default_expiration_days']
                
                token = str(uuid.uuid4())
                expires_at = datetime.now(timezone.utc) + timedelta(days=link_expires_in_days)
                
                cursor.execute(
                    "INSERT INTO invitations (token, template_id, expiration_days, expires_at, status) VALUES (%s, %s, %s, %s, 'active')",
                    (token, template_id, final_expiration_days, expires_at)
                )
                return token
    except Exception as e:
        logger.error(f"创建邀请链接时出错: {e}", exc_info=True)
        raise

def get_all_invitation_links() -> List[Dict]:
    """获取所有邀请链接及其关联的模板名称。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT i.*, t.name as template_name 
                    FROM invitations i JOIN user_templates t ON i.template_id = t.id
                    ORDER BY i.created_at DESC
                """)
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"获取邀请链接列表时出错: {e}", exc_info=True)
        raise

def delete_invitation_link(invitation_id: int) -> int:
    """删除一个邀请链接，返回受影响行数。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM invitations WHERE id = %s", (invitation_id,))
                return cursor.rowcount
    except Exception as e:
        logger.error(f"删除邀请链接 {invitation_id} 时出错: {e}", exc_info=True)
        raise

# ======================================================================
# 模块: 用户管理 (User Management)
# ======================================================================

def get_all_extended_user_info() -> Dict[str, Dict]:
    """获取所有用户的扩展信息，并以用户ID为键返回一个字典。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT eue.*, ut.name as template_name 
                    FROM emby_users_extended eue
                    LEFT JOIN user_templates ut ON eue.template_id = ut.id
                """)
                return {row['emby_user_id']: dict(row) for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"获取所有扩展用户信息时出错: {e}", exc_info=True)
        raise

def change_user_template_and_get_names(user_id: str, new_template_id: int) -> tuple:
    """切换用户模板，并返回用户名和模板名用于日志。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                user_name, new_template_name = user_id, f"ID:{new_template_id}"
                cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
                user_record = cursor.fetchone()
                if user_record: user_name = user_record['name']
                
                cursor.execute("SELECT emby_policy_json, emby_configuration_json, name FROM user_templates WHERE id = %s", (new_template_id,))
                template_record = cursor.fetchone()
                if not template_record:
                    raise ValueError("模板不存在")
                new_template_name = template_record['name']
                
                upsert_sql = """
                    INSERT INTO emby_users_extended (emby_user_id, template_id, status, created_by)
                    VALUES (%s, %s, 'active', 'admin-assigned')
                    ON CONFLICT (emby_user_id) DO UPDATE SET template_id = EXCLUDED.template_id;
                """
                cursor.execute(upsert_sql, (user_id, new_template_id))
                return user_name, new_template_name, dict(template_record)
    except Exception as e:
        logger.error(f"切换用户 {user_id} 模板时出错: {e}", exc_info=True)
        raise

def set_user_status_in_db(user_id: str, new_status: str):
    """在数据库中更新用户的状态。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE emby_users_extended SET status = %s WHERE emby_user_id = %s", (new_status, user_id))
    except Exception as e:
        logger.error(f"更新用户 {user_id} 状态时出错: {e}", exc_info=True)
        raise

def set_user_expiration_in_db(user_id: str, expiration_date: Optional[str]):
    """在数据库中设置或清除用户的有效期。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM emby_users_extended WHERE emby_user_id = %s", (user_id,))
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO emby_users_extended (emby_user_id, status, created_by) VALUES (%s, 'active', 'admin-assigned')",
                        (user_id,)
                    )
                cursor.execute("UPDATE emby_users_extended SET expiration_date = %s WHERE emby_user_id = %s", (expiration_date, user_id))
    except Exception as e:
        logger.error(f"更新用户 {user_id} 有效期时出错: {e}", exc_info=True)
        raise

def delete_user_from_db(user_id: str) -> int:
    """从本地数据库删除一个用户，返回受影响行数。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM emby_users WHERE id = %s", (user_id,))
                return cursor.rowcount
    except Exception as e:
        logger.error(f"从数据库删除用户 {user_id} 时出错: {e}", exc_info=True)
        raise

def get_username_by_id(user_id: str) -> Optional[str]:
    """根据用户ID获取用户名。"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
                record = cursor.fetchone()
                return record['name'] if record else None
    except Exception:
        return None