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
    
# ★★★ 新增：获取用户观影历史并关联媒体元数据 ★★★
def get_user_history_with_metadata(user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    获取指定用户的观影历史，并 JOIN 媒体元数据表以获取标题、年份等信息。
    """
    sql = """
        SELECT
            umd.item_id,
            umd.last_played_date,
            umd.play_count,
            mm.title,
            mm.original_title,
            mm.release_year,
            mm.item_type,
            mm.rating
        FROM
            user_media_data umd
        LEFT JOIN
            media_metadata mm ON umd.item_id = mm.emby_item_id
        WHERE
            umd.user_id = %s
            AND umd.last_played_date IS NOT NULL
        ORDER BY
            umd.last_played_date DESC
        LIMIT %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id, limit))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 为用户 {user_id} 查询观影历史失败: {e}", exc_info=True)
        raise

# ★★★ 新增：获取全局播放次数排行榜 ★★★
def get_global_play_count_rankings(limit: int = 20) -> List[Dict[str, Any]]:
    """
    统计所有用户的播放数据，按总播放次数进行全局排名。
    """
    sql = """
        SELECT
            umd.item_id,
            SUM(umd.play_count) as total_play_count,
            COUNT(DISTINCT umd.user_id) as total_viewers,
            mm.title,
            mm.original_title,
            mm.release_year,
            mm.item_type
        FROM
            user_media_data umd
        LEFT JOIN
            media_metadata mm ON umd.item_id = mm.emby_item_id
        WHERE
            umd.play_count > 0
        GROUP BY
            umd.item_id, mm.title, mm.original_title, mm.release_year, mm.item_type
        ORDER BY
            total_play_count DESC, total_viewers DESC
        LIMIT %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询全局排行榜失败: {e}", exc_info=True)
        raise

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

def get_subscription_request_details(request_id: int) -> Optional[Dict[str, Any]]:
    """根据ID获取单条订阅请求的完整信息。"""
    sql = "SELECT * FROM subscription_requests WHERE id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (request_id,))
            return dict(cursor.fetchone()) if cursor.rowcount > 0 else None
    except Exception as e:
        logger.error(f"DB: 查询订阅请求 {request_id} 详情失败: {e}", exc_info=True)
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

def get_user_account_details(user_id: str) -> Optional[Dict[str, Any]]:
    """
    根据用户ID，查询其在 emby_users_extended 表中的信息，并关联 user_templates 表获取模板详情。
    """
    sql = """
        SELECT
            ue.status,
            ue.registration_date,
            ue.expiration_date,
            ue.telegram_chat_id,
            ut.name as template_name,
            ut.description as template_description,
            ut.allow_unrestricted_subscriptions
        FROM
            emby_users_extended ue
        LEFT JOIN
            user_templates ut ON ue.template_id = ut.id
        WHERE
            ue.emby_user_id = %s;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的账户详情失败: {e}", exc_info=True)
        raise

def get_user_subscription_history(user_id: str) -> List[Dict[str, Any]]:
    """获取指定用户的所有订阅请求历史。"""
    sql = """
        SELECT id, item_name, item_type, status, requested_at, notes
        FROM subscription_requests
        WHERE emby_user_id = %s
        ORDER BY requested_at DESC;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 查询用户 {user_id} 的订阅历史失败: {e}", exc_info=True)
        raise

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