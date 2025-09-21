# db_handler.py
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, Json
import json
import pytz
import logging
from typing import Optional, Dict, Any, List, Tuple
from flask import jsonify
from datetime import datetime, timezone
# 核心模块导入
import config_manager
import emby_handler
import constants # 确保常量模块被导入
from utils import contains_chinese

logger = logging.getLogger(__name__)

# --- 初始化数据库 ---
def init_db():
    """
    【PostgreSQL版】初始化数据库，创建所有表的最终结构。
    """
    logger.info("正在初始化 PostgreSQL 数据库，创建/验证所有表的结构...")
    
    # get_central_db_connection 应该就是 db_handler.get_db_connection
    # 确保它现在调用的是无参数版本
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("  -> 数据库连接成功，开始建表...")

                # --- 1. 创建基础表 (日志、缓存、用户) ---
                logger.trace("  -> 正在创建基础表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processed_log (
                        item_id TEXT PRIMARY KEY, 
                        item_name TEXT, 
                        processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), 
                        score REAL,
                        assets_synced_at TIMESTAMP WITH TIME ZONE,
                        last_emby_modified_at TIMESTAMP WITH TIME ZONE
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS failed_log (
                        item_id TEXT PRIMARY KEY, 
                        item_name TEXT, 
                        reason TEXT, 
                        failed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), 
                        error_message TEXT, 
                        item_type TEXT, 
                        score REAL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY, 
                        username TEXT UNIQUE NOT NULL, 
                        password_hash TEXT NOT NULL, 
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS translation_cache (
                        original_text TEXT PRIMARY KEY, 
                        translated_text TEXT, 
                        engine_used TEXT, 
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS app_settings (
                        setting_key TEXT PRIMARY KEY,
                        value_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                logger.trace("  -> 正在创建 'emby_users' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS emby_users (
                        id TEXT PRIMARY KEY, name TEXT NOT NULL, is_administrator BOOLEAN,
                        last_seen_at TIMESTAMP WITH TIME ZONE, profile_image_tag TEXT,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                logger.trace("  -> 正在创建 'user_media_data' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_media_data (
                        user_id TEXT NOT NULL,
                        item_id TEXT NOT NULL,
                        is_favorite BOOLEAN DEFAULT FALSE,
                        played BOOLEAN DEFAULT FALSE,
                        playback_position_ticks BIGINT DEFAULT 0,
                        play_count INTEGER DEFAULT 0,
                        last_played_date TIMESTAMP WITH TIME ZONE,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (user_id, item_id)
                    )
                """)
                # 为常用查询创建索引
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_umd_user_id ON user_media_data (user_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_umd_last_updated ON user_media_data (last_updated_at);")

                logger.trace("  -> 正在创建 'collections_info' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS collections_info (
                        emby_collection_id TEXT PRIMARY KEY,
                        name TEXT,
                        tmdb_collection_id TEXT,
                        status TEXT,
                        has_missing BOOLEAN, 
                        missing_movies_json JSONB,
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        poster_path TEXT,
                        item_type TEXT DEFAULT 'Movie' NOT NULL,
                        in_library_count INTEGER DEFAULT 0
                    )
                """)

                logger.trace("  -> 正在创建 'custom_collections' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS custom_collections (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        type TEXT NOT NULL,
                        definition_json JSONB NOT NULL,
                        status TEXT DEFAULT 'active',
                        emby_collection_id TEXT,
                        last_synced_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        health_status TEXT,
                        item_type TEXT,
                        in_library_count INTEGER DEFAULT 0,
                        missing_count INTEGER DEFAULT 0,
                        generated_media_info_json JSONB,
                        poster_path TEXT,
                        sort_order INTEGER NOT NULL DEFAULT 0
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cc_type ON custom_collections (type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cc_status ON custom_collections (status)")

                logger.trace("  -> 正在创建 'media_metadata' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS media_metadata (
                        tmdb_id TEXT,
                        item_type TEXT NOT NULL,
                        emby_item_id TEXT,
                        title TEXT,
                        original_title TEXT,
                        release_year INTEGER,
                        rating REAL,
                        genres_json JSONB,
                        actors_json JSONB,
                        directors_json JSONB,
                        studios_json JSONB,
                        countries_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        release_date DATE,
                        date_added TIMESTAMP WITH TIME ZONE,
                        tags_json JSONB,
                        last_synced_at TIMESTAMP WITH TIME ZONE,
                        PRIMARY KEY (tmdb_id, item_type)
                    )
                """)

                logger.trace("  -> 正在创建 'watchlist' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS watchlist (
                        item_id TEXT PRIMARY KEY,
                        tmdb_id TEXT NOT NULL,
                        item_name TEXT,
                        item_type TEXT DEFAULT 'Series',
                        status TEXT DEFAULT 'Watching',
                        added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        tmdb_status TEXT,
                        next_episode_to_air_json JSONB,
                        missing_info_json JSONB,
                        paused_until DATE DEFAULT NULL,
                        force_ended BOOLEAN DEFAULT FALSE NOT NULL
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_status ON watchlist (status)")

                logger.trace("  -> 正在创建 'person_identity_map' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS person_identity_map (
                        map_id SERIAL PRIMARY KEY, 
                        primary_name TEXT NOT NULL, 
                        emby_person_id TEXT NOT NULL UNIQUE,
                        tmdb_person_id INTEGER UNIQUE, 
                        imdb_id TEXT UNIQUE, 
                        douban_celebrity_id TEXT UNIQUE,
                        last_synced_at TIMESTAMP WITH TIME ZONE, 
                        last_updated_at TIMESTAMP WITH TIME ZONE
                    )
                """)

                logger.trace("  -> 正在创建 'actor_metadata' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS actor_metadata (
                        tmdb_id INTEGER PRIMARY KEY, 
                        profile_path TEXT, 
                        gender INTEGER, 
                        adult BOOLEAN,
                        popularity REAL, 
                        original_name TEXT, 
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        FOREIGN KEY(tmdb_id) REFERENCES person_identity_map(tmdb_person_id) ON DELETE CASCADE
                    )
                """)

                logger.trace("  -> 正在创建 'actor_subscriptions' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS actor_subscriptions (
                        id SERIAL PRIMARY KEY,
                        tmdb_person_id INTEGER NOT NULL UNIQUE,
                        actor_name TEXT NOT NULL,
                        profile_path TEXT,
                        config_start_year INTEGER DEFAULT 1900,
                        config_media_types TEXT DEFAULT 'Movie,TV',
                        config_genres_include_json JSONB,
                        config_genres_exclude_json JSONB,
                        status TEXT DEFAULT 'active',
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        config_min_rating REAL DEFAULT 6.0
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_as_status ON actor_subscriptions (status)")

                logger.trace("  -> 正在创建 'tracked_actor_media' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tracked_actor_media (
                        id SERIAL PRIMARY KEY,
                        subscription_id INTEGER NOT NULL,
                        tmdb_media_id INTEGER NOT NULL,
                        media_type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        release_date DATE,
                        poster_path TEXT,
                        status TEXT NOT NULL,
                        emby_item_id TEXT,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        FOREIGN KEY(subscription_id) REFERENCES actor_subscriptions(id) ON DELETE CASCADE,
                        UNIQUE(subscription_id, tmdb_media_id)
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tam_subscription_id ON tracked_actor_media (subscription_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tam_status ON tracked_actor_media (status)")

                logger.trace("  -> 正在创建 'resubscribe_rules' 表 (多规则洗版)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resubscribe_rules (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        enabled BOOLEAN DEFAULT TRUE,
                        
                        -- ★ 新增：规则应用的目标媒体库ID列表
                        target_library_ids JSONB, 
                        
                        -- ★ 新增：洗版成功后是否删除Emby媒体项
                        delete_after_resubscribe BOOLEAN DEFAULT FALSE,
                        
                        -- ★ 新增：规则优先级，数字越小越优先
                        sort_order INTEGER DEFAULT 0,

                        -- ▼ 下面是原来 settings 表里的所有字段
                        resubscribe_resolution_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_resolution_threshold INT DEFAULT 1920,
                        resubscribe_audio_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_audio_missing_languages JSONB,
                        resubscribe_subtitle_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_subtitle_missing_languages JSONB,
                        resubscribe_quality_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_quality_include JSONB,
                        resubscribe_effect_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_effect_include JSONB
                    )
                """)

                logger.trace("  -> 正在创建 'resubscribe_cache' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resubscribe_cache (
                        item_id TEXT PRIMARY KEY,
                        item_name TEXT,
                        tmdb_id TEXT,
                        item_type TEXT,
                        status TEXT DEFAULT 'unknown', -- 新增状态字段: 'ok', 'needed', 'subscribed'
                        reason TEXT,
                        resolution_display TEXT,
                        quality_display TEXT,
                        effect_display TEXT,
                        audio_display TEXT,
                        subtitle_display TEXT,
                        audio_languages_raw JSONB,
                        subtitle_languages_raw JSONB,
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        source_library_id TEXT
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_resubscribe_cache_status ON resubscribe_cache (status);")

                logger.trace("  -> 正在创建 'media_cleanup_tasks' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS media_cleanup_tasks (
                        id SERIAL PRIMARY KEY,
                        task_type TEXT NOT NULL, -- 'multi_version' or 'duplicate'
                        tmdb_id TEXT,
                        item_name TEXT,
                        versions_info_json JSONB,
                        status TEXT DEFAULT 'pending', -- 'pending', 'processed', 'ignored'
                        best_version_id TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cleanup_task_type ON media_cleanup_tasks (task_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cleanup_task_status ON media_cleanup_tasks (status);")

                logger.trace("  -> 正在创建 'user_templates' 表 (用户权限模板)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_templates (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        description TEXT,
                        -- 核心字段：存储一个完整的 Emby 用户策略 JSON 对象
                        emby_policy_json JSONB NOT NULL,
                        -- 模板默认的有效期（天数），0 表示永久
                        default_expiration_days INTEGER DEFAULT 30,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                logger.trace("  -> 正在创建 'invitations' 表 (邀请码)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS invitations (
                        id SERIAL PRIMARY KEY,
                        -- 核心字段：独一无二的邀请码
                        token TEXT NOT NULL UNIQUE,
                        -- 关联到使用的模板
                        template_id INTEGER NOT NULL,
                        -- 本次邀请的有效期，可以覆盖模板的默认值
                        expiration_days INTEGER NOT NULL,
                        status TEXT NOT NULL DEFAULT 'active', -- 状态: active(可用), used(已用), expired(过期)
                        -- 邀请链接本身的有效期
                        expires_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        -- 记录被哪个新用户使用了
                        used_by_user_id TEXT,
                        FOREIGN KEY(template_id) REFERENCES user_templates(id) ON DELETE CASCADE
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations (token);")

                logger.trace("  -> 正在创建 'emby_users_extended' 表 (用户扩展信息)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS emby_users_extended (
                        emby_user_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending', -- 状态: pending(待审批), active(激活), expired(过期), disabled(禁用)
                        registration_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        expiration_date TIMESTAMP WITH TIME ZONE, -- 核心字段：用户的到期时间
                        notes TEXT,
                        created_by TEXT DEFAULT 'self-registered', -- 'self-registered' 或 'admin'
                        template_id INTEGER,
                        FOREIGN KEY(emby_user_id) REFERENCES emby_users(id) ON DELETE CASCADE,
                        FOREIGN KEY(template_id) REFERENCES user_templates(id) ON DELETE SET NULL
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_eue_status ON emby_users_extended (status);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_eue_expiration_date ON emby_users_extended (expiration_date);")

                # --- 2. 执行平滑升级检查 ---
                logger.info("  -> 开始执行数据库表结构平滑升级检查...")
                try:
                    # --- 2.1 检查所有表的列 ---
                    # 查询 information_schema 获取所有表的列信息
                    cursor.execute("""
                        SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = current_schema();
                    """)
                    
                    # 将结果组织成一个字典，方便查询: {'table_name': {'col1', 'col2'}, ...}
                    all_existing_columns = {}
                    for row in cursor.fetchall():
                        table = row['table_name']
                        if table not in all_existing_columns:
                            all_existing_columns[table] = set()
                        all_existing_columns[table].add(row['column_name'])

                    # --- 2.2 定义所有需要检查和添加的新列 ---
                    # 格式: {'table_name': {'column_name': 'COLUMN_TYPE'}}
                    schema_upgrades = {
                        'media_metadata': {
                            "official_rating": "TEXT",
                            "unified_rating": "TEXT",
                            "emby_item_id": "TEXT"
                        },
                        'watchlist': {
                            "last_episode_to_air_json": "JSONB"
                        },
                        'resubscribe_cache': {
                            "matched_rule_id": "INTEGER",
                            "matched_rule_name": "TEXT",
                            "source_library_id": "TEXT"
                        },
                        'resubscribe_rules': {
                            "resubscribe_subtitle_effect_only": "BOOLEAN DEFAULT FALSE"
                        },
                        'custom_collections': {
                            "generated_emby_ids_json": "JSONB DEFAULT '[]'::jsonb NOT NULL"
                        },
                        'custom_collections': {
                            "generated_emby_ids_json": "JSONB DEFAULT '[]'::jsonb NOT NULL",
                            "allowed_user_ids": "JSONB" 
                        }
                    }

                    # --- 2.3 遍历并执行升级 ---
                    for table, columns_to_add in schema_upgrades.items():
                        # 检查表是否存在于我们查询到的信息中
                        if table in all_existing_columns:
                            existing_cols_for_table = all_existing_columns[table]
                            for col_name, col_type in columns_to_add.items():
                                # 如果新列不存在，则添加它
                                if col_name not in existing_cols_for_table:
                                    logger.info(f"    -> [数据库升级] 检测到 '{table}' 表缺少 '{col_name}' 字段，正在添加...")
                                    # 使用 ALTER TABLE ... ADD COLUMN ... IF NOT EXISTS 语法，双重保险
                                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
                                    logger.info(f"    -> [数据库升级] 字段 '{col_name}' 添加成功。")
                                else:
                                    logger.trace(f"    -> 字段 '{table}.{col_name}' 已存在，跳过。")
                        else:
                            # 这种情况理论上不会发生，因为前面的 CREATE TABLE IF NOT EXISTS 已经保证了表的存在
                            logger.warning(f"    -> [数据库升级] 检查表 '{table}' 时发现该表不存在，跳过升级。")

                except Exception as e_alter:
                    logger.error(f"  -> [数据库升级] 检查或添加新字段时出错: {e_alter}", exc_info=True)
                    # 即使升级失败，也继续执行，不中断主程序启动
                
                try:
                    logger.trace("  -> 正在为 'media_metadata.emby_item_id' 创建索引...")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_emby_item_id ON media_metadata (emby_item_id);")
                except Exception as e_index:
                    logger.error(f"  -> 创建 'emby_item_id' 索引时出错: {e_index}", exc_info=True)

                logger.info("  -> 数据库平滑升级检查完成。")

            conn.commit()
            logger.info("✅ PostgreSQL 数据库初始化完成，所有表结构已创建/验证。")

    except psycopg2.Error as e_pg:
        logger.error(f"数据库初始化时发生 PostgreSQL 错误: {e_pg}", exc_info=True)
        raise
    except Exception as e_global:
        logger.error(f"数据库初始化时发生未知错误: {e_global}", exc_info=True)
        raise

# ======================================================================
# 模块 1: 数据库管理器 (The Unified Data Access Layer)
# ======================================================================

# --- 状态中文翻译字典 ---
STATUS_TRANSLATION_MAP = {
    'in_library': '已入库',
    'subscribed': '已订阅',
    'missing': '缺失',
    'unreleased': '未上映',
    'pending_release': '未上映' # 确保这个状态也有翻译
}

def get_db_connection() -> psycopg2.extensions.connection:
    """
    【中央函数】获取一个配置好 RealDictCursor 的 PostgreSQL 数据库连接。
    这是整个应用获取数据库连接的唯一入口。
    """
    try:
        # 从全局配置中获取连接参数
        cfg = config_manager.APP_CONFIG
        conn = psycopg2.connect(
            host=cfg.get(constants.CONFIG_OPTION_DB_HOST),
            port=cfg.get(constants.CONFIG_OPTION_DB_PORT),
            user=cfg.get(constants.CONFIG_OPTION_DB_USER),
            password=cfg.get(constants.CONFIG_OPTION_DB_PASSWORD),
            dbname=cfg.get(constants.CONFIG_OPTION_DB_NAME),
            cursor_factory=RealDictCursor  # ★★★ 关键：让返回的每一行都是字典
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"获取 PostgreSQL 数据库连接失败: {e}", exc_info=True)
        raise

# ======================================================================
# 模块 2: 演员数据访问层 (Actor Data Access Layer)
# ======================================================================

class ActorDBManager:
    """
    一个专门负责与演员身份相关的数据库表进行交互的类。
    """
    def __init__(self):
        # PostgreSQL 连接信息从全局配置读取
        logger.trace("ActorDBManager 初始化 (PostgreSQL mode)。")

    def get_translation_from_db(self, cursor: psycopg2.extensions.cursor, text: str, by_translated_text: bool = False) -> Optional[Dict[str, Any]]:
        """
        【PostgreSQL版】从数据库获取翻译缓存，并自我净化坏数据。
        """
        try:
            if by_translated_text:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE translated_text = %s"
            else:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE original_text = %s"

            cursor.execute(sql, (text,))
            row = cursor.fetchone()

            if not row:
                return None

            translated_text = row['translated_text']
            
            if translated_text and not contains_chinese(translated_text):
                original_text_key = row['original_text']
                logger.warning(f"  -> 发现无效的历史翻译缓存: '{original_text_key}' -> '{translated_text}'。将自动销毁此记录。")
                try:
                    cursor.execute("DELETE FROM translation_cache WHERE original_text = %s", (original_text_key,))
                except Exception as e_delete:
                    logger.error(f"销毁无效缓存 '{original_text_key}' 时失败: {e_delete}")
                return None
            
            return dict(row)

        except Exception as e:
            logger.error(f"DB读取翻译缓存时发生错误 for '{text}': {e}", exc_info=True)
            return None

    def save_translation_to_db(self, cursor: psycopg2.extensions.cursor, original_text: str, translated_text: Optional[str], engine_used: Optional[str]):
        """
        【PostgreSQL版】将翻译结果保存到数据库，增加中文校验。
        """
        if translated_text and translated_text.strip() and not contains_chinese(translated_text):
            logger.warning(f"翻译结果 '{translated_text}' 不含中文，已丢弃。原文: '{original_text}'")
            return

        try:
            # PostgreSQL 使用 ON CONFLICT ... DO UPDATE 来实现 upsert
            sql = """
                INSERT INTO translation_cache (original_text, translated_text, engine_used, last_updated_at) 
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (original_text) DO UPDATE SET
                    translated_text = EXCLUDED.translated_text,
                    engine_used = EXCLUDED.engine_used,
                    last_updated_at = NOW();
            """
            cursor.execute(sql, (original_text, translated_text, engine_used))
            logger.trace(f"翻译缓存存DB: '{original_text}' -> '{translated_text}' (引擎: {engine_used})")
        except Exception as e:
            logger.error(f"DB保存翻译缓存失败 for '{original_text}': {e}", exc_info=True)

    def find_person_by_any_id(self, cursor: psycopg2.extensions.cursor, **kwargs) -> Optional[dict]:
        search_criteria = [
            ("tmdb_person_id", kwargs.get("tmdb_id")),
            ("emby_person_id", kwargs.get("emby_id")),
            ("imdb_id", kwargs.get("imdb_id")),
            ("douban_celebrity_id", kwargs.get("douban_id")),
        ]
        for column, value in search_criteria:
            if not value: continue
            try:
                cursor.execute(f"SELECT * FROM person_identity_map WHERE {column} = %s", (value,))
                result = cursor.fetchone()
                if result:
                    logger.debug(f"通过 {column}='{value}' 找到了演员记录 (map_id: {result['map_id']})。")
                    return result
            except psycopg2.Error as e:
                logger.error(f"查询 person_identity_map 时出错 ({column}={value}): {e}")
        return None

    def find_person_by_any_id(self, cursor: psycopg2.extensions.cursor, **kwargs) -> Optional[dict]:
        search_criteria = [
            ("tmdb_person_id", kwargs.get("tmdb_id")),
            ("emby_person_id", kwargs.get("emby_id")),
            ("imdb_id", kwargs.get("imdb_id")),
            ("douban_celebrity_id", kwargs.get("douban_id")),
        ]
        for column, value in search_criteria:
            if not value: continue
            try:
                cursor.execute(f"SELECT * FROM person_identity_map WHERE {column} = %s", (value,))
                result = cursor.fetchone()
                if result:
                    logger.debug(f"通过 {column}='{value}' 找到了演员记录 (map_id: {result['map_id']})。")
                    return result
            except psycopg2.Error as e:
                logger.error(f"查询 person_identity_map 时出错 ({column}={value}): {e}")
        return None

    def upsert_person(
        self, 
        cursor: psycopg2.extensions.cursor, 
        person_data: Dict[str, Any],
        emby_config: Dict[str, str] 
    ) -> Tuple[int, str]:
        """
        【V4.1 - 类型安全修复版】
        - 修复了当 tmdb_id 为 'None' 字符串时导致的 int() 转换错误。
        """
        id_field_map = {
            "tmdb_person_id": "Tmdb",
            "imdb_id": "Imdb",
            "douban_celebrity_id": "Douban"
        }

        cursor.execute("SAVEPOINT actor_upsert")

        try:
            # ★★★ 核心修复：更安全地处理 tmdb_id ★★★
            tmdb_id_raw = person_data.get("tmdb_id")
            tmdb_id_int = None
            if tmdb_id_raw and str(tmdb_id_raw).isdigit():
                try:
                    tmdb_id_int = int(tmdb_id_raw)
                except (ValueError, TypeError):
                    pass # 如果转换失败，保持为 None
            
            new_data = {
                "primary_name": str(person_data.get("name") or '').strip(),
                "emby_person_id": str(person_data.get("emby_id") or '').strip() or None,
                "tmdb_person_id": tmdb_id_int,
                "imdb_id": str(person_data.get("imdb_id") or '').strip() or None,
                "douban_celebrity_id": str(person_data.get("douban_id") or '').strip() or None,
            }

            if not new_data["emby_person_id"]:
                logger.warning("缺失 emby_person_id，无法执行 upsert")
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return -1, "SKIPPED"

            # --- 冲突检查 (逻辑不变) ---
            conflict_detected = False
            for db_column, emby_provider_key in id_field_map.items():
                id_value = new_data.get(db_column)
                if id_value is None:
                    continue

                cursor.execute(
                    f"SELECT emby_person_id FROM person_identity_map WHERE {db_column} = %s",
                    (id_value,)
                )
                conflicting_records = cursor.fetchall()
                
                if conflicting_records:
                    conflicting_emby_pids = [rec['emby_person_id'] for rec in conflicting_records]
                    if new_data["emby_person_id"] not in conflicting_emby_pids:
                        conflicting_emby_pids.append(new_data["emby_person_id"])
                    
                    if len(conflicting_emby_pids) > 1:
                        logger.warning(
                            f"检测到ID冲突: {db_column} = '{id_value}' 被多个Emby PID共享: {conflicting_emby_pids}。"
                            "将执行智能合并..."
                        )
                        
                        # 1. 找到目标记录 (通常是列表中第一个，或者最完善的那个)
                        # 我们以第一个找到的记录作为合并的目标
                        target_emby_pid = conflicting_emby_pids[0]
                        cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (target_emby_pid,))
                        target_actor = cursor.fetchone()
                        
                        if not target_actor:
                            logger.error(f"合并失败：无法找到目标演员记录 (Emby PID: {target_emby_pid})。")
                            continue # 继续检查下一个ID字段

                        target_map_id = target_actor['map_id']
                        
                        # 2. 遍历所有其他的冲突记录，将它们的信息合并到目标记录中，然后删除它们
                        for source_emby_pid in conflicting_emby_pids[1:]:
                            cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (source_emby_pid,))
                            source_actor = cursor.fetchone()
                            
                            if not source_actor or source_actor['map_id'] == target_map_id:
                                continue # 如果源记录不存在或就是目标记录本身，则跳过

                            source_map_id = source_actor['map_id']
                            logger.info(f"  -> 正在将源记录 (Emby PID: {source_emby_pid}, map_id: {source_map_id}) 合并到目标记录 (Emby PID: {target_emby_pid}, map_id: {target_map_id})")

                            # 核心合并逻辑：如果目标记录的某个ID字段为空，就用源记录的来填充
                            if source_actor.get('tmdb_person_id') and not target_actor.get('tmdb_person_id'):
                                cursor.execute("UPDATE person_identity_map SET tmdb_person_id = %s WHERE map_id = %s", (source_actor['tmdb_person_id'], target_map_id))
                            if source_actor.get('imdb_id') and not target_actor.get('imdb_id'):
                                cursor.execute("UPDATE person_identity_map SET imdb_id = %s WHERE map_id = %s", (source_actor['imdb_id'], target_map_id))
                            if source_actor.get('douban_celebrity_id') and not target_actor.get('douban_celebrity_id'):
                                cursor.execute("UPDATE person_identity_map SET douban_celebrity_id = %s WHERE map_id = %s", (source_actor['douban_celebrity_id'], target_map_id))

                            # 3. 删除现在已经多余的源记录
                            cursor.execute("DELETE FROM person_identity_map WHERE map_id = %s", (source_map_id,))
                            logger.info(f"  -> 合并完成，已删除多余的源记录 (map_id: {source_map_id})。")

                        # 4. 在本次 upsert 操作中，确保 new_data 使用的是合并后的目标ID
                        new_data["emby_person_id"] = target_emby_pid

            if conflict_detected:
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return -1, "SKIPPED"

            # --- Upsert 逻辑 (逻辑不变) ---
            cursor.execute("SELECT * FROM person_identity_map WHERE emby_person_id = %s", (new_data["emby_person_id"],))
            existing_record = cursor.fetchone()

            if existing_record:
                existing_record = dict(existing_record)
                update_fields = {}
                
                id_fields = id_field_map.keys()
                for f in id_fields:
                    new_val = new_data.get(f)
                    if new_val is not None and not existing_record.get(f):
                        update_fields[f] = new_val

                new_name = new_data["primary_name"]
                old_name = existing_record.get("primary_name") or ""
                if new_name and new_name != old_name:
                    update_fields["primary_name"] = new_name

                if update_fields:
                    set_clauses = [f"{k} = %s" for k in update_fields.keys()]
                    set_clauses.append("last_updated_at = NOW()")
                    sql = f"UPDATE person_identity_map SET {', '.join(set_clauses)} WHERE map_id = %s"
                    cursor.execute(sql, tuple(update_fields.values()) + (existing_record["map_id"],))
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return existing_record["map_id"], "UPDATED"
                else:
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return existing_record["map_id"], "UNCHANGED"
            else:
                insert_fields = [k for k, v in new_data.items() if v is not None]
                if not insert_fields:
                    cursor.execute("RELEASE SAVEPOINT actor_upsert")
                    return -1, "SKIPPED"

                insert_placeholders = ["%s"] * len(insert_fields)
                insert_values = [new_data[k] for k in insert_fields]

                sql_insert = f"""
                    INSERT INTO person_identity_map ({', '.join(insert_fields)}, last_updated_at) 
                    VALUES ({', '.join(insert_placeholders)}, NOW())
                    RETURNING map_id
                """
                cursor.execute(sql_insert, tuple(insert_values))
                result = cursor.fetchone()
                cursor.execute("RELEASE SAVEPOINT actor_upsert")
                return (result["map_id"], "INSERTED") if result else (-1, "ERROR")

        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT actor_upsert")
            logger.error(f"upsert_person 发生异常，emby_person_id={person_data.get('emby_id')}: {e}", exc_info=True)
            cursor.execute("RELEASE SAVEPOINT actor_upsert")
            return -1, "ERROR"

# --- 演员映射表清理 ---
def get_all_emby_person_ids_from_map() -> set:
    """从 person_identity_map 表中获取所有 emby_person_id 的集合。"""
    ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_person_id FROM person_identity_map")
            rows = cursor.fetchall()
            for row in rows:
                ids.add(row['emby_person_id'])
        return ids
    except Exception as e:
        logger.error(f"DB: 获取所有演员映射Emby ID时失败: {e}", exc_info=True)
        raise

def delete_persons_by_emby_ids(emby_ids: list) -> int:
    """根据 Emby Person ID 列表，从 person_identity_map 表中批量删除记录。"""
    if not emby_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 ANY(%s) 语法可以高效地处理列表删除
            sql = "DELETE FROM person_identity_map WHERE emby_person_id = ANY(%s)"
            cursor.execute(sql, (emby_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"  -> 从演员映射表中删除了 {deleted_count} 条陈旧记录。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除陈旧演员映射时失败: {e}", exc_info=True)
        raise
# ======================================================================
# 模块 3: 日志表数据访问 (Log Tables Data Access)
# ======================================================================
class LogDBManager:
    """
    专门负责与日志相关的数据库表 (processed_log, failed_log) 进行交互的类。
    """
    def __init__(self):
        pass

    def save_to_processed_log(self, cursor: psycopg2.extensions.cursor, item_id: str, item_name: str, score: float = 10.0):
        try:
            sql = """
                INSERT INTO processed_log (item_id, item_name, processed_at, score)
                VALUES (%s, %s, NOW(), %s)
                ON CONFLICT (item_id) DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    processed_at = NOW(),
                    score = EXCLUDED.score;
            """
            cursor.execute(sql, (item_id, item_name, score))
        except Exception as e:
            logger.error(f"写入已处理 失败 (Item ID: {item_id}): {e}")
    
    def remove_from_processed_log(self, cursor: psycopg2.extensions.cursor, item_id: str):
        try:
            logger.debug(f"正在从已处理日志中删除 Item ID: {item_id}...")
            cursor.execute("DELETE FROM processed_log WHERE item_id = %s", (item_id,))
        except Exception as e:
            logger.error(f"从已处理日志删除失败 for item {item_id}: {e}", exc_info=True)

    def remove_from_failed_log(self, cursor: psycopg2.extensions.cursor, item_id: str):
        try:
            cursor.execute("DELETE FROM failed_log WHERE item_id = %s", (item_id,))
        except Exception as e:
            logger.error(f"从 failed_log 删除失败 (Item ID: {item_id}): {e}")

    def save_to_failed_log(self, cursor: psycopg2.extensions.cursor, item_id: str, item_name: str, reason: str, item_type: str, score: Optional[float] = None):
        try:
            sql = """
                INSERT INTO failed_log (item_id, item_name, reason, item_type, score, failed_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (item_id) DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    reason = EXCLUDED.reason,
                    item_type = EXCLUDED.item_type,
                    score = EXCLUDED.score,
                    failed_at = NOW();
            """
            cursor.execute(sql, (item_id, item_name, reason, item_type, score))
        except Exception as e:
            logger.error(f"写入 failed_log 失败 (Item ID: {item_id}): {e}")
    
    def mark_assets_as_synced(self, cursor, item_id: str, sync_timestamp_iso: str):
        """
        在 processed_log 中标记一个项目的资源文件已同步，并记录确切的同步时间。
        如果条目不存在，会创建一个新条目。
        """
        logger.debug(f"正在更新 Item ID {item_id} 的备份状态和时间戳...")
        sql = """
            INSERT INTO processed_log (item_id, assets_synced_at)
            VALUES (%s, %s)
            ON CONFLICT (item_id) DO UPDATE SET
                assets_synced_at = EXCLUDED.assets_synced_at;
        """
        try:
            # 将 ISO 格式的时间戳字符串直接传递给数据库
            cursor.execute(sql, (item_id, sync_timestamp_iso))
        except Exception as e:
            logger.error(f"更新资源同步时间戳时失败 for item {item_id}: {e}", exc_info=True)

def get_item_name_from_failed_log(item_id: str) -> Optional[str]:
    """根据 item_id 从 failed_log 表中获取 item_name。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name FROM failed_log WHERE item_id = %s", (item_id,))
            result = cursor.fetchone()
            return result['item_name'] if result else None
    except Exception as e:
        logger.error(f"从 failed_log 获取 item_name 时出错: {e}")
        return None

def get_review_items_paginated(page: int, per_page: int, query_filter: str) -> Tuple[List, int]:
    offset = (page - 1) * per_page
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            where_clause = ""
            sql_params = []
            if query_filter:
                where_clause = "WHERE item_name ILIKE %s" # ILIKE for case-insensitive search
                sql_params.append(f"%{query_filter}%")

            count_sql = f"SELECT COUNT(*) as total FROM failed_log {where_clause}"
            cursor.execute(count_sql, tuple(sql_params))
            total_matching_items = cursor.fetchone()['total']

            items_sql = f"""
                SELECT item_id, item_name, failed_at, reason, item_type, score 
                FROM failed_log {where_clause}
                ORDER BY failed_at DESC 
                LIMIT %s OFFSET %s
            """
            cursor.execute(items_sql, tuple(sql_params + [per_page, offset]))
            items_to_review = [dict(row) for row in cursor.fetchall()]
            
        return items_to_review, total_matching_items
    except Exception as e:
        logger.error(f"DB: 获取待复核列表失败: {e}", exc_info=True)
        raise

def mark_review_item_as_processed(item_id: str) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            with conn.cursor() as cursor:
                cursor.execute("SELECT item_name, item_type, score FROM failed_log WHERE item_id = %s", (item_id,))
                failed_item_info = cursor.fetchone()
                if not failed_item_info: return False

                cursor.execute("DELETE FROM failed_log WHERE item_id = %s", (item_id,))
                
                score_to_save = failed_item_info["score"] if failed_item_info["score"] is not None else 10.0
                
                upsert_sql = """
                    INSERT INTO processed_log (item_id, item_name, processed_at, score) 
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (item_id) DO UPDATE SET
                        item_name = EXCLUDED.item_name,
                        processed_at = NOW(),
                        score = EXCLUDED.score;
                """
                cursor.execute(upsert_sql, (item_id, failed_item_info["item_name"], score_to_save))
            conn.commit()
            logger.info(f"DB: 项目 {item_id} 已成功移至已处理日志。")
            return True
    except Exception as e:
        logger.error(f"DB: 标记项目 {item_id} 为已处理时失败: {e}", exc_info=True)
        raise

def clear_all_review_items() -> List[Dict[str, str]]:
    """将所有待复核项移至已处理，并返回被移动的项的列表。"""
    moved_items = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 选出所有待复核的项
                cursor.execute("SELECT item_id, item_name, score FROM failed_log")
                items_to_move = cursor.fetchall()
                
                if not items_to_move:
                    return []

                # 2. 插入或更新到 processed_log
                for item in items_to_move:
                    score_to_save = item["score"] if item["score"] is not None else 10.0
                    cursor.execute(
                        "INSERT INTO processed_log (item_id, item_name, processed_at, score) VALUES (%s, %s, NOW(), %s) "
                        "ON CONFLICT (item_id) DO UPDATE SET item_name = EXCLUDED.item_name, processed_at = NOW(), score = EXCLUDED.score",
                        (item['item_id'], item['item_name'], score_to_save)
                    )
                    # 将字典格式的 item 添加到返回列表
                    moved_items.append(dict(item))

                # 3. 清空 failed_log
                cursor.execute("DELETE FROM failed_log")
                conn.commit()
                
            logger.info(f"成功移动 {len(moved_items)} 条记录从待复核到已处理。")
            return moved_items
    except Exception as e:
        logger.error(f"清空并标记待复核列表时发生异常：{e}", exc_info=True)
        # 发生错误时回滚事务（with conn 会自动处理）并返回空列表
        return []
# ======================================================================
# 模块 4: 智能追剧列表数据访问 (Watchlist Data Access)
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
    """
    【V2 - PG语法修复版】
    添加一个新项目到追剧列表。
    - 修复了因使用 SQLite 特有的 INSERT OR REPLACE 语法导致的错误。
    - 改为使用 PostgreSQL 标准的 ON CONFLICT ... DO UPDATE 语法。
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # ★★★ 核心修复：使用 PostgreSQL 的 ON CONFLICT 语法 ★★★
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
    """
    更新追剧列表中某个项目的状态。
    返回 True 表示成功，False 表示项目未找到。
    """
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
    """
    从追剧列表中移除一个项目。
    返回 True 表示成功，False 表示项目未找到。
    """
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

# 批量强制完结的逻辑
def batch_force_end_watchlist_items(item_ids: List[str]) -> int:
    """
    【V2】批量将追剧项目标记为“强制完结”。
    这会将项目状态设置为 'Ended'，并将 'force_ended' 标志位设为 True。
    这样可以防止常规刷新错误地复活剧集，但允许新一季的检查使其复活。
    返回成功更新的行数。
    """
    if not item_ids:
        return 0
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('%s' for _ in item_ids)
            # 将状态更新为 Ended，并设置 force_ended 标志
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
# ★★★ 批量更新追剧状态的数据库函数 ★★★
def batch_update_watchlist_status(item_ids: list, new_status: str) -> int:
    """
    【V2 - 时间格式修复版】
    批量更新指定项目ID列表的追剧状态。
    """
    if not item_ids:
        return 0
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 准备要更新的字段和值，但不包括时间
            updates = { "status": new_status }
            
            # 2. 核心逻辑：如果是“重新追剧”，则需要重置相关状态
            if new_status == 'Watching':
                updates["paused_until"] = None
                updates["force_ended"] = False # 使用布尔值更标准
            
            # 3. ★★★ 核心修正：将 last_checked_at 的更新直接写入SQL ★★★
            set_clauses = [f"{key} = %s" for key in updates.keys()]
            set_clauses.append("last_checked_at = NOW()") # 直接使用数据库的 NOW() 函数
            
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

# --- 水印 ---
def get_watching_tmdb_ids() -> set:
    """
    获取所有正在追看（状态为 'Watching'）的剧集的 TMDB ID 集合。
    返回一个集合(set)以便进行高效查询。
    """
    watching_ids = set()
    try:
        # 复用1: 使用标准的数据库连接获取方式
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 复用2: 借鉴现有的SQL查询逻辑
            cursor.execute("SELECT tmdb_id FROM watchlist WHERE status = 'Watching'")
            rows = cursor.fetchall()
            # 复用3: 借鉴数据处理方式，但适配新需求
            for row in rows:
                # 我们只需要 tmdb_id，并将其转换为字符串放入集合
                watching_ids.add(str(row['tmdb_id']))
    except Exception as e:
        # 使用你项目中已有的 logger
        logger.error(f"从数据库获取正在追看的TMDB ID时出错: {e}", exc_info=True)
        # 即使出错也返回空集合，保证上层逻辑的健壮性
    return watching_ids

# ======================================================================
# 模块 5: 电影合集数据访问 (Collections Data Access)
# ======================================================================

def get_all_collections() -> List[Dict[str, Any]]:
    """获取数据库中所有电影合集的信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM collections_info WHERE tmdb_collection_id IS NOT NULL ORDER BY name")
            
            final_results = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                
                # --- ▼▼▼ 核心修改点 ▼▼▼ ---
                # 1. 直接获取 psycopg2 已经解析好的 Python 对象 (列表)
                missing_movies_data = row_dict.get('missing_movies_json')
                
                # 2. 做一个健壮性检查，确保它是一个列表
                if isinstance(missing_movies_data, list):
                    row_dict['missing_movies'] = missing_movies_data
                else:
                    # 如果数据格式不正确或为NULL，则提供一个安全的默认值
                    row_dict['missing_movies'] = []
                # --- ▲▲▲ 修改结束 ▲▲▲ ---

                del row_dict['missing_movies_json'] # 删除原始json字段，保持API响应干净
                final_results.append(row_dict)
                
            return final_results
    except Exception as e:
        logger.error(f"DB: 读取合集状态时发生严重错误: {e}", exc_info=True)
        raise

def get_all_custom_collection_emby_ids() -> set:
    """
    从 custom_collections 表中获取所有非空的 emby_collection_id。
    返回一个集合(set)以便进行高效的成员资格检查和集合运算。
    """
    ids = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 只选择非NULL的ID
            cursor.execute("SELECT emby_collection_id FROM custom_collections WHERE emby_collection_id IS NOT NULL")
            rows = cursor.fetchall()
            for row in rows:
                ids.add(row['emby_collection_id'])
        logger.debug(f"从数据库中获取到 {len(ids)} 个由本程序管理的自定义合集ID。")
        return ids
    except psycopg2.Error as e:
        logger.error(f"获取所有自定义合集Emby ID时发生数据库错误: {e}", exc_info=True)
        return ids # 即使出错也返回一个空集合，保证上层逻辑不会崩溃

def get_collections_with_missing_movies() -> List[Dict[str, Any]]:
    """获取所有包含缺失电影的合集信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_collection_id, name, missing_movies_json FROM collections_info WHERE has_missing = TRUE")
            # 返回原始行，让业务逻辑层处理 JSON
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取有缺失电影的合集时失败: {e}", exc_info=True)
        raise


def update_collection_movies(collection_id: str, movies: List[Dict[str, Any]]):
    """
    更新指定合集的电影列表和缺失状态。
    """
    try:
        with get_db_connection() as conn:
            # 业务逻辑：根据更新后的电影列表，重新判断是否还有缺失
            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = %s, has_missing = %s WHERE emby_collection_id = %s",
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 的电影列表。")
    except Exception as e:
        logger.error(f"DB: 更新合集 {collection_id} 的电影列表时失败: {e}", exc_info=True)
        raise


def update_single_movie_status_in_collection(collection_id: str, movie_tmdb_id: str, new_status: str) -> bool:
    """【修复 #2】更新合集中单个电影的状态。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = %s", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            movies = row.get('missing_movies_json')
            if not isinstance(movies, list):
                movies = [] # 安全兜底

            movie_found = False
            for movie in movies:
                if str(movie.get('tmdb_id')) == str(movie_tmdb_id):
                    movie['status'] = new_status
                    movie_found = True
                    break
            
            if not movie_found:
                conn.rollback()
                return False

            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = %s, has_missing = %s WHERE emby_collection_id = %s", 
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 中电影 {movie_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新电影状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

# ★★★ 批量将指定合集中的'missing'电影状态更新为'subscribed' ★★★
def batch_mark_movies_as_subscribed_in_collections(collection_ids: List[str]) -> int:
    """
    【V2 - PG 兼容修复版】
    批量将指定合集列表中的所有'missing'状态的电影更新为'subscribed'。
    这是一个纯粹的数据库操作，不会触发任何外部订阅。

    :param collection_ids: 需要操作的合集 Emby ID 列表。
    :return: 成功更新状态的电影总数。
    """
    if not collection_ids:
        return 0

    total_updated_movies = 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            placeholders = ','.join('%s' for _ in collection_ids)
            sql_select = f"SELECT emby_collection_id, missing_movies_json FROM collections_info WHERE emby_collection_id IN ({placeholders})"
            cursor.execute(sql_select, collection_ids)
            collections_to_process = cursor.fetchall()

            if not collections_to_process:
                return 0

            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in collections_to_process:
                    collection_id = collection_row['emby_collection_id']
                    
                    # --- ▼▼▼ 核心修改 1: 修复 TypeError ▼▼▼
                    # 直接使用 psycopg2 解析好的列表，不再使用 json.loads()
                    movies = collection_row.get('missing_movies_json')
                    
                    # 增加健壮性检查，如果数据不是列表则跳过
                    if not isinstance(movies, list):
                        continue
                    # --- ▲▲▲ 修改结束 ▲▲▲ ---

                    movies_changed_in_this_collection = False
                    for movie in movies:
                        if movie.get('status') == 'missing':
                            movie['status'] = 'subscribed'
                            total_updated_movies += 1
                            movies_changed_in_this_collection = True
                    
                    if movies_changed_in_this_collection:
                        new_missing_json = json.dumps(movies, ensure_ascii=False)
                        
                        # --- ▼▼▼ 核心修改 2: 修复逻辑错误 ▼▼▼
                        # 既然所有 missing 都被标记了，has_missing 状态应该变为 False
                        cursor.execute(
                            "UPDATE collections_info SET missing_movies_json = %s, has_missing = FALSE WHERE emby_collection_id = %s",
                            (new_missing_json, collection_id)
                        )
                        # --- ▲▲▲ 修改结束 ▲▲▲ ---
                
                conn.commit()
                logger.info(f"DB: 成功将 {len(collection_ids)} 个合集中的 {total_updated_movies} 部缺失电影标记为已订阅。")

            except Exception as e_trans:
                conn.rollback()
                logger.error(f"批量标记已订阅的数据库事务失败，已回滚: {e_trans}", exc_info=True)
                raise
        
        return total_updated_movies

    except Exception as e:
        logger.error(f"DB: 批量标记电影为已订阅时发生错误: {e}", exc_info=True)
        raise
# ======================================================================
# 模块 6: 演员订阅数据访问 (Actor Subscriptions Data Access)
# ======================================================================

def get_all_actor_subscriptions() -> List[Dict[str, Any]]:
    """获取所有演员订阅的简略列表。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 只选择列表页需要展示的核心字段
            cursor.execute("SELECT id, tmdb_person_id, actor_name, profile_path, status, last_checked_at FROM actor_subscriptions ORDER BY added_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取演员订阅列表失败: {e}", exc_info=True)
        raise


def get_single_subscription_details(subscription_id: int) -> Optional[Dict[str, Any]]:
    """
    【V2 - 格式化修复版】
    获取单个订阅的完整详情，并确保返回给前端的数据格式正确。
    - 将配置项嵌套在 'config' 对象中，使API结构更清晰。
    - 将JSON字符串字段 (genres) 解析为Python列表。
    - 将逗号分隔的字符串字段 (media_types) 拆分为Python列表。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. 获取订阅主信息
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            sub_row = cursor.fetchone()
            if not sub_row:
                return None
            
            # 2. 获取关联的已追踪媒体
            cursor.execute("SELECT * FROM tracked_actor_media WHERE subscription_id = %s ORDER BY release_date DESC", (subscription_id,))
            tracked_media = [dict(row) for row in cursor.fetchall()]
            
            # 3. ★★★ 核心修复：构建一个结构化、类型正确的响应对象 ★★★

            # 辅助函数，用于安全地解析JSON字符串 (仅用于 TEXT 字段，JSONB 字段 psycopg2 会自动解析)
            def _safe_json_loads(json_string, default_value=None):
                if default_value is None:
                    default_value = []
                # 仅当输入是字符串时才尝试解析
                if isinstance(json_string, str):
                    try:
                        return json.loads(json_string)
                    except json.JSONDecodeError:
                        return default_value
                # 如果不是字符串 (例如已经是列表/字典，如JSONB字段)，则直接返回
                return json_string if json_string is not None else default_value

            # 将数据库行组装成前端期望的格式
            response_data = {
                "id": sub_row['id'],
                "tmdb_person_id": sub_row['tmdb_person_id'],
                "actor_name": sub_row['actor_name'],
                "profile_path": sub_row['profile_path'],
                "status": sub_row['status'],
                "last_checked_at": sub_row['last_checked_at'],
                "added_at": sub_row['added_at'],
                
                # 将所有配置项聚合到一个 'config' 对象中
                "config": {
                    "start_year": sub_row.get('config_start_year'),
                    # 将 'Movie,TV' 字符串拆分为 ['Movie', 'TV'] 数组
                    "media_types": [t.strip() for t in (sub_row.get('config_media_types') or '').split(',') if t.strip()],
                    # ★★★ 修复 2: 对于 JSONB 字段，直接使用其值，psycopg2 已自动解析为列表 ★★★
                    "genres_include_json": sub_row.get('config_genres_include_json') or [],
                    "genres_exclude_json": sub_row.get('config_genres_exclude_json') or [],
                    # 确保评分是浮点数
                    "min_rating": float(sub_row.get('config_min_rating', 0.0))
                },
                
                "tracked_media": tracked_media
            }
            
            return response_data
            
    except Exception as e:
        logger.error(f"DB: 获取订阅详情 {subscription_id} 失败: {e}", exc_info=True)
        raise


def safe_json_dumps(value):
    """
    将 Python 对象转换成 JSON 字符串。
    如果传入的是字符串且能被解析成合法 JSON，则先解析再序列化，避免重复转义。
    否则按字符串处理。
    """
    if isinstance(value, str):
        try:
            # 尝试解析字符串（可能是JSON字符串）
            parsed = json.loads(value)
            # 重新序列化，保证仅一层转义
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            # 解析失败，按字符串序列化
            return json.dumps(value, ensure_ascii=False)
    else:
        # 普通Python对象，正常序列化
        return json.dumps(value, ensure_ascii=False)

def add_actor_subscription(tmdb_person_id: int, actor_name: str, profile_path: str, config: dict) -> int:
    """
    【V3 - 最终修复版】
    新增一个演员订阅。
    - 修复了因 INSERT 语句缺少 RETURNING id 导致的 psycopg2.ProgrammingError。
    - 调整了 commit() 的位置，确保在获取ID后再提交事务。
    - ★★★ 新增：为新订阅设置默认的 'active' 状态，解决编辑失败问题 ★★★
    """
    start_year = config.get('start_year', 1900)
    media_types_list = config.get('media_types', ['Movie','TV'])
    if isinstance(media_types_list, list):
        media_types = ','.join(media_types_list)
    else:
        media_types = str(media_types_list)

    genres_include = safe_json_dumps(config.get('genres_include_json', []))
    genres_exclude = safe_json_dumps(config.get('genres_exclude_json', []))
    min_rating = config.get('min_rating', 6.0)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            sql = """
                INSERT INTO actor_subscriptions 
                (tmdb_person_id, actor_name, profile_path, status, config_start_year, config_media_types, config_genres_include_json, config_genres_exclude_json, config_min_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            cursor.execute(
                sql,
                (tmdb_person_id, actor_name, profile_path, 'active', start_year, media_types, genres_include, genres_exclude, min_rating)
            )
            
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建的演员订阅ID。")
            
            new_id = result['id']
            conn.commit()
            
            logger.info(f"DB: 成功添加演员订阅 '{actor_name}' (ID: {new_id})。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except Exception as e:
        logger.error(f"DB: 添加演员订阅 '{actor_name}' 时失败: {e}", exc_info=True)
        raise

def update_actor_subscription(subscription_id: int, data: dict) -> bool:
    """
    【V6 - 逻辑重构最终修复版】
    更新一个演员订阅的状态或配置。
    - 采用更健壮的“准备->覆盖->格式化”模式，确保任何更新路径下写入数据库的都是正确格式的字符串。
    - 彻底解决因部分更新（如只更新状态）导致配置被错误格式化的根本问题。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            current_sub = cursor.fetchone()
            if not current_sub:
                return False

            # --- 步骤 1: 使用数据库的当前值作为所有字段的“底稿” ---
            # 普通字段
            new_status = current_sub['status']
            new_start_year = current_sub['config_start_year']
            new_min_rating = current_sub['config_min_rating']
            # 需要在内存中当作列表处理的字段
            # 注意：psycopg2 已经将 JSONB 字段自动转为 Python 列表
            new_genres_include_list = current_sub.get('config_genres_include_json') or []
            new_genres_exclude_list = current_sub.get('config_genres_exclude_json') or []
            # 将逗号分隔的字符串也转为列表，统一处理
            new_media_types_list = [t.strip() for t in (current_sub.get('config_media_types') or '').split(',') if t.strip()]


            # --- 步骤 2: 检查前端传入的新数据，并用它来“覆盖”底稿 ---
            # 覆盖状态
            new_status = data.get('status', new_status)

            # 如果前端传了 config 对象，则覆盖所有相关的配置项
            config = data.get('config')
            if config is not None:
                new_start_year = config.get('start_year', new_start_year)
                new_min_rating = config.get('min_rating', new_min_rating)

                # 如果传了 media_types (必须是列表)，则覆盖
                if 'media_types' in config and isinstance(config['media_types'], list):
                    new_media_types_list = config['media_types']
                
                # 如果传了 genres (必须是列表)，则覆盖
                if 'genres_include_json' in config and isinstance(config['genres_include_json'], list):
                    new_genres_include_list = config['genres_include_json']
                if 'genres_exclude_json' in config and isinstance(config['genres_exclude_json'], list):
                    new_genres_exclude_list = config['genres_exclude_json']

            # --- 步骤 3: 在执行SQL前，将所有变量进行最终的格式化，确保它们都是字符串 ---
            # 将列表转换为逗号分隔的字符串
            final_media_types_str = ','.join(new_media_types_list)
            # 将列表转换为JSON字符串
            final_genres_include_json = json.dumps(new_genres_include_list, ensure_ascii=False)
            final_genres_exclude_json = json.dumps(new_genres_exclude_list, ensure_ascii=False)

            # --- 步骤 4: 使用准备好的、格式完全正确的变量执行数据库更新 ---
            cursor.execute("""
                UPDATE actor_subscriptions SET
                status = %s, config_start_year = %s, config_media_types = %s, 
                config_genres_include_json = %s, config_genres_exclude_json = %s, config_min_rating = %s
                WHERE id = %s
            """, (new_status, new_start_year, final_media_types_str, final_genres_include_json, final_genres_exclude_json, new_min_rating, subscription_id))
            
            conn.commit()
            logger.info(f"DB: 成功更新订阅ID {subscription_id}。")
            return True
            
    except Exception as e:
        logger.error(f"DB: 更新订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise


def delete_actor_subscription(subscription_id: int) -> bool:
    """
    删除一个演员订阅及其所有追踪的媒体。
    返回 True 表示成功。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 由于外键设置了 ON DELETE CASCADE，我们只需要删除主表记录即可
            cursor.execute("DELETE FROM actor_subscriptions WHERE id = %s", (subscription_id,))
            conn.commit()
            logger.info(f"DB: 成功删除订阅ID {subscription_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 删除订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

# ★★★ 根据 tracked_actor_media 的主键ID获取单条记录 ★★★
def get_tracked_media_by_id(media_id: int) -> Optional[Dict[str, Any]]:
    """根据 tracked_actor_media 表的主键 ID 获取单个媒体项的完整信息。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tracked_actor_media WHERE id = %s", (media_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB: 获取已追踪媒体项 {media_id} 失败: {e}", exc_info=True)
        raise

# ★★★ 更新单个已追踪媒体项的状态 ★★★
def update_tracked_media_status(media_id: int, new_status: str) -> bool:
    """根据 tracked_actor_media 表的主键 ID 更新单个媒体项的状态。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE tracked_actor_media SET status = %s, last_updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (new_status, media_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 更新已追踪媒体项 {media_id} 状态失败: {e}", exc_info=True)
        raise

# --- 清空指定表的函数，返回受影响的行数 ---
def clear_table(table_name: str) -> int:
    """
    清空指定的数据库表，返回删除的行数。
    注意：请确保传入的表名是受信任的，避免SQL注入风险。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 psycopg2.sql 模块安全构造SQL
            from psycopg2 import sql
            query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))
            cursor.execute(query)
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"清空表 {table_name}，删除了 {deleted_count} 行。")
            return deleted_count
    except Exception as e:
        logger.error(f"清空表 {table_name} 时发生错误: {e}", exc_info=True)
        raise

# --- 一键矫正自增序列 ---
def correct_all_sequences() -> list:
    """
    【V2 - 最终修正版】
    自动查找所有使用 SERIAL/IDENTITY 列的表，并校准它们的序列。
    通过使用 setval 的第三个参数 (is_called=false)，完美处理空表的情况。
    """
    corrected_tables = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    c.table_name,
                    c.column_name
                FROM
                    information_schema.columns c
                WHERE
                    c.table_schema = 'public' AND c.column_default LIKE 'nextval%';
            """)
            tables_with_sequences = cursor.fetchall()

            if not tables_with_sequences:
                logger.info("未找到任何使用自增序列的表，无需校准。")
                return []

            logger.info(f"开始校准 {len(tables_with_sequences)} 个表的自增序列...")

            for row in tables_with_sequences:
                table_name = row['table_name']
                column_name = row['column_name']
                
                # ★★★ 核心修正：改变 setval 的用法 ★★★
                # 1. 我们计算出 (最大ID + 1)，这代表下一个应该被发出的ID。
                # 2. 我们将 setval 的第三个参数设为 false，
                #    这告诉数据库：“我给你的这个值，是下一个要用的值，而不是已经被用掉的值。”
                query = sql.SQL("""
                    SELECT setval(
                        pg_get_serial_sequence({table}, {column}),
                        COALESCE((SELECT MAX({id_col}) FROM {table_ident}), 0) + 1,
                        false
                    )
                """).format(
                    table=sql.Literal(table_name),
                    column=sql.Literal(column_name),
                    id_col=sql.Identifier(column_name),
                    table_ident=sql.Identifier(table_name)
                )
                
                cursor.execute(query)
                logger.info(f"  -> 已成功校准表 '{table_name}' 的序列。")
                corrected_tables.append(table_name)
            
            conn.commit()
            return corrected_tables

        except Exception as e:
            conn.rollback()
            logger.error(f"校准自增序列时发生严重错误: {e}", exc_info=True)
            raise

# ======================================================================
# 模块 6: 自定义合集数据访问 (custom_collections Data Access)
# ======================================================================

def create_custom_collection(name: str, type: str, definition_json: str, allowed_user_ids_json: Optional[str] = None) -> int:
    """【V2 - 权限系统版】创建一个新的自定义合集，包含权限控制。"""
    # ★★★ 核心修复 1/2: 增加 allowed_user_ids 列 ★★★
    sql = """
        INSERT INTO custom_collections (name, type, definition_json, status, created_at, allowed_user_ids)
        VALUES (%s, %s, %s, 'active', NOW(), %s) 
        RETURNING id
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 核心修复 2/2: 增加 allowed_user_ids_json 参数 ★★★
            cursor.execute(sql, (name, type, definition_json, allowed_user_ids_json))
            
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建行的ID。")
            new_id = result['id']

            conn.commit()
            logger.info(f"成功创建自定义合集 '{name}' (类型: {type})。")
            return new_id
    except psycopg2.IntegrityError:
        raise
    except psycopg2.Error as e:
        logger.error(f"创建自定义合集 '{name}' 时发生非预期的数据库错误: {e}", exc_info=True)
        raise
def get_all_custom_collections() -> List[Dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM custom_collections
                ORDER BY sort_order ASC, id ASC
            """)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 获取所有已启用的自定义合集，供“一键生成”任务使用 ★★★
def get_all_active_custom_collections() -> List[Dict[str, Any]]:
    """获取所有状态为 'active' 的自定义合集"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE status = 'active' ORDER BY sort_order ASC, id ASC")
            rows = cursor.fetchall()
            logger.trace(f"  -> 从数据库找到 {len(rows)} 个已启用的自定义合集。")
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有已启用的自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

def get_custom_collection_by_id(collection_id: int) -> Optional[Dict[str, Any]]:
    """
    根据ID获取单个自定义合集的详细信息。
    :param collection_id: 自定义合集的ID。
    :return: 包含合集信息的字典，如果未找到则返回None。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE id = %s", (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except psycopg2.Error as e:
        logger.error(f"根据ID {collection_id} 获取自定义合集时发生数据库错误: {e}", exc_info=True)
        return None

def update_custom_collection(collection_id: int, name: str, type: str, definition_json: str, status: str, allowed_user_ids_json: Optional[str] = None) -> bool:
    """【V3 - 权限系统版】更新一个自定义合集，包含权限控制。"""
    # ★★★ 核心修复 1/2: 在 SET 子句中增加 allowed_user_ids ★★★
    sql = """
        UPDATE custom_collections
        SET name = %s, type = %s, definition_json = %s, status = %s, allowed_user_ids = %s
        WHERE id = %s
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复 2/2: 在参数元组中增加 allowed_user_ids_json ★★★
            cursor.execute(sql, (name, type, definition_json, status, allowed_user_ids_json, collection_id))
            
            if cursor.rowcount > 0:
                conn.commit()
                logger.info(f"成功更新自定义合集 ID: {collection_id}。")
                return True
            else:
                logger.warning(f"尝试更新自定义合集 ID {collection_id}，但在数据库中未找到该记录。")
                conn.rollback()
                return False

    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 ID {collection_id} 时发生数据库错误: {e}", exc_info=True)
        return False

def delete_custom_collection(collection_id: int) -> bool:
    """
    【V5 - 职责单一版】从数据库中删除一个自定义合集定义。
    此函数只负责数据库删除操作，不再与任何其他表或外部服务交互。
    联动删除Emby实体的逻辑应由调用方（API层）处理。
    
    :param collection_id: 要删除的自定义合集的数据库ID。
    :return: 如果成功删除了记录，返回 True；如果未找到记录或发生错误，返回 False。
    """
    sql = "DELETE FROM custom_collections WHERE id = %s"
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            conn.commit()
            # cursor.rowcount > 0 确保确实有一行被删除了
            if cursor.rowcount > 0:
                logger.info(f"  -> ✅ 成功从数据库中删除了自定义合集定义 (ID: {collection_id})。")
                return True
            else:
                logger.warning(f"尝试删除自定义合集 (ID: {collection_id})，但在数据库中未找到该记录。")
                return False # 虽然不是错误，但操作未产生效果
    except psycopg2.Error as e:
        logger.error(f"删除自定义合集 (ID: {collection_id}) 时发生数据库错误: {e}", exc_info=True)
        raise # 向上抛出异常，让API层可以捕获并返回500错误

# ★★★ 更新自定义合集排序的函数 ★★★
def update_custom_collections_order(ordered_ids: List[int]) -> bool:
    """
    根据提供的ID列表，批量更新自定义合集的 sort_order。
    :param ordered_ids: 按新顺序排列的合集ID列表。
    :return: 操作是否成功。
    """
    if not ordered_ids:
        return True

    sql = "UPDATE custom_collections SET sort_order = %s WHERE id = %s"
    # 创建一个元组列表，每个元组是 (sort_order, id)
    data_to_update = [(index, collection_id) for index, collection_id in enumerate(ordered_ids)]

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany(sql, data_to_update)
            conn.commit()
            logger.info(f"成功更新了 {len(ordered_ids)} 个自定义合集的顺序。")
            return True
    except psycopg2.Error as e:
        logger.error(f"批量更新自定义合集顺序时发生数据库错误: {e}", exc_info=True)
        # 发生错误时，事务会自动回滚
        return False

# +++ 自定义合集筛选引擎所需函数 +++
def get_media_metadata_by_tmdb_id(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """
    根据TMDb ID从媒体元数据缓存表中获取单条记录。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = %s", (tmdb_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID {tmdb_id} 获取媒体元数据时出错: {e}", exc_info=True)
        return None
    
# ★★★ 获取所有媒体元数据 ★★★
def get_all_media_metadata(item_type: str = 'Movie') -> List[Dict[str, Any]]:
    """
    从媒体元数据缓存表中获取指定类型的所有记录。
    :param item_type: 'Movie' 或 'Series'。默认为 'Movie'，因为合集主要是电影。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE item_type = %s", (item_type,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"获取所有媒体元数据时出错 (类型: {item_type}): {e}", exc_info=True)
        return []
    
# ★★★ 从元数据表中提取所有唯一的类型 ★★★
def get_unique_genres() -> List[str]:
    """
    【V2 - PG JSON 兼容版】
    从 media_metadata 表中扫描所有电影，提取出所有不重复的类型(genres)。
    """
    unique_genres = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT genres_json FROM media_metadata WHERE item_type = 'Movie'")
            rows = cursor.fetchall()
            
            for row in rows:
                # ★★★ 核心修复：直接使用已经是列表的 genres_json 字段 ★★★
                genres = row['genres_json']
                if genres: # 确保它不是 None 或空列表
                    try:
                        for genre in genres:
                            if genre:
                                unique_genres.add(genre.strip())
                    except TypeError:
                        # 增加保护，以防万一某行数据格式真的有问题
                        logger.warning(f"处理 genres_json 时遇到意外的类型错误，内容: {genres}")
                        continue
                        
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"从数据库中成功提取出 {len(sorted_genres)} 个唯一的电影类型。")
        return sorted_genres
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一电影类型时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 从元数据表中提取所有唯一的工作室 ★★★
def get_unique_studios() -> List[str]:
    """
    【V3 - PG JSON 兼容版】
    从 media_metadata 表中扫描所有媒体项，提取出所有不重复的工作室(studios)。
    """
    unique_studios = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT studios_json FROM media_metadata")
            rows = cursor.fetchall()
            
            for row in rows:
                # ★★★ 核心修复：直接使用已经是列表的 studios_json 字段 ★★★
                studios = row['studios_json']
                if studios:
                    try:
                        for studio in studios:
                            if studio:
                                unique_studios.add(studio.strip())
                    except TypeError:
                        logger.warning(f"处理 studios_json 时遇到意外的类型错误，内容: {studios}")
                        continue
                        
        sorted_studios = sorted(list(unique_studios))
        logger.trace(f"从数据库中成功提取出 {len(sorted_studios)} 个跨电影和电视剧的唯一工作室。")
        return sorted_studios
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一工作室时发生数据库错误: {e}", exc_info=True)
        return []
    
# ★★★ 从元数据表中提取所有唯一的标签 ★★★
def get_unique_tags() -> List[str]:
    """
    【V2 - PG JSON 兼容版】
    从 media_metadata 表中扫描所有媒体项，提取出所有不重复的标签(tags)。
    """
    unique_tags = set()
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tags_json FROM media_metadata")
            rows = cursor.fetchall()
            
            for row in rows:
                # ★★★ 核心修复：直接使用已经是列表的 tags_json 字段 ★★★
                tags = row['tags_json']
                if tags:
                    try:
                        for tag in tags:
                            if tag:
                                unique_tags.add(tag.strip())
                    except TypeError:
                        logger.warning(f"处理 tags_json 时遇到意外的类型错误，内容: {tags}")
                        continue
                        
        sorted_tags = sorted(list(unique_tags))
        logger.trace(f"从数据库中成功提取出 {len(sorted_tags)} 个唯一的标签。")
        return sorted_tags
        
    except psycopg2.Error as e:
        logger.error(f"提取唯一标签时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 根据关键词搜索唯一的工作室 ★★★
def search_unique_studios(search_term: str, limit: int = 20) -> List[str]:
    """
    (V3 - 智能排序版)
    从数据库中搜索工作室，并优先返回名称以 search_term 开头的结果。
    (此函数无需修改，因为它依赖的 get_unique_studios() 已被修复)
    """
    if not search_term:
        return []
    
    all_studios = get_unique_studios()
    
    if not all_studios:
        return []

    search_term_lower = search_term.lower()
    
    starts_with_matches = []
    contains_matches = []
    
    for studio in all_studios:
        studio_lower = studio.lower()
        if studio_lower.startswith(search_term_lower):
            starts_with_matches.append(studio)
        elif search_term_lower in studio_lower:
            contains_matches.append(studio)
            
    final_matches = starts_with_matches + contains_matches
    
    logger.trace(f"智能搜索 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
    
    return final_matches[:limit]

# --- 搜索演员 ---
def search_unique_actors(search_term: str, limit: int = 20) -> List[str]:
    """
    【V6.1 - PG JSON 兼容版】
    - 修复了因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    - 不再对从数据库中获取的 actors_json 字段执行 json.loads()。
    """
    if not search_term:
        return []
    
    unique_actors_map = {}
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT actors_json FROM media_metadata")
            rows = cursor.fetchall()
            
            for row in rows:
                # ★★★ 核心修复：直接使用已经是列表的 actors_json 字段 ★★★
                actors = row['actors_json']
                if actors: # 确保它不是 None 或空列表
                    try:
                        for actor in actors:
                            actor_name = actor.get('name')
                            original_name = actor.get('original_name')
                            
                            if actor_name and actor_name.strip():
                                if actor_name not in unique_actors_map:
                                    unique_actors_map[actor_name.strip()] = (original_name or '').strip()
                    except TypeError:
                        # 增加一个保护，以防万一某行数据格式真的有问题
                        logger.warning(f"处理 actors_json 时遇到意外的类型错误，内容: {actors}")
                        continue
        
        if not unique_actors_map:
            return []

        search_term_lower = search_term.lower()
        starts_with_matches = []
        contains_matches = []
        
        for name, original_name in sorted(unique_actors_map.items()):
            name_lower = name.lower()
            original_name_lower = original_name.lower()

            if name_lower.startswith(search_term_lower) or (original_name_lower and original_name_lower.startswith(search_term_lower)):
                starts_with_matches.append(name)
            elif search_term_lower in name_lower or (original_name_lower and search_term_lower in original_name_lower):
                contains_matches.append(name)
        
        final_matches = starts_with_matches + contains_matches
        logger.trace(f"双语搜索演员 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
        
        return final_matches[:limit]
        
    except psycopg2.Error as e:
        logger.error(f"提取并搜索唯一演员时发生数据库错误: {e}", exc_info=True)
        return []

# --- 搜索分级 ---
def get_unique_official_ratings():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT split_part(official_rating, '-', 2) as rating
            FROM media_metadata
            WHERE official_rating IS NOT NULL AND official_rating LIKE '%-%'
            ORDER BY rating;
        """)
        return [row['rating'] for row in cursor.fetchall()]

# ★★★ 新增：写入或更新一条完整的合集检查信息 ★★★
def upsert_collection_info(collection_data: Dict[str, Any]):
    """
    使用 INSERT OR REPLACE 写入或更新一条合集信息到 collections_info 表。
    """
    # PostgreSQL 没有 INSERT OR REPLACE，需要使用 ON CONFLICT DO UPDATE
    sql = """
        INSERT INTO collections_info 
        (emby_collection_id, name, tmdb_collection_id, item_type, status, has_missing, 
        missing_movies_json, last_checked_at, poster_path, in_library_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (emby_collection_id) DO UPDATE SET
            name = EXCLUDED.name,
            tmdb_collection_id = EXCLUDED.tmdb_collection_id,
            item_type = EXCLUDED.item_type,
            status = EXCLUDED.status,
            has_missing = EXCLUDED.has_missing,
            missing_movies_json = EXCLUDED.missing_movies_json,
            last_checked_at = EXCLUDED.last_checked_at,
            poster_path = EXCLUDED.poster_path,
            in_library_count = EXCLUDED.in_library_count;
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (
                collection_data.get('emby_collection_id'),
                collection_data.get('name'),
                collection_data.get('tmdb_collection_id'),
                collection_data.get('item_type'),
                collection_data.get('status'),
                collection_data.get('has_missing'),
                collection_data.get('missing_movies_json'),
                collection_data.get('last_checked_at'),
                collection_data.get('poster_path'),
                collection_data.get('in_library_count')
            ))
            conn.commit()
            logger.info(f"成功写入/更新合集检查信息到数据库 (ID: {collection_data.get('emby_collection_id')})。")
    except psycopg2.Error as e:
        logger.error(f"写入合集检查信息时发生数据库错误: {e}", exc_info=True)
        raise

def update_custom_collection_after_sync(collection_id: int, update_data: Dict[str, Any]) -> bool:
    """
    在同步任务完成后，使用一个包含多个字段的字典来更新自定义合集的状态。
    这是一个灵活的函数，可以动态构建SQL语句。
    """
    if not update_data:
        logger.warning(f"尝试更新自定义合集 {collection_id}，但没有提供任何更新数据。")
        return False

    # 动态构建 SET 子句
    set_clauses = [f"{key} = %s" for key in update_data.keys()]
    values = list(update_data.values())
    
    sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = %s"
    values.append(collection_id)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 的同步后状态。")
            return True
    except psycopg2.Error as e:
        logger.error(f"更新自定义合集 {collection_id} 同步后状态时出错: {e}", exc_info=True)
        return False

def update_single_media_status_in_custom_collection(collection_id: int, media_tmdb_id: str, new_status: str) -> bool:
    """ 更新自定义合集中单个媒体项的状态。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT generated_media_info_json FROM custom_collections WHERE id = %s", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            # --- ▼▼▼ 核心修复：移除 json.loads，直接使用已解析的对象 ▼▼▼ ---
            media_items = row.get('generated_media_info_json')
            if not isinstance(media_items, list):
                media_items = [] # 安全兜底
            # --- ▲▲▲ 修复结束 ▲▲▲ ---

            item_found = False
            for item in media_items:
                if str(item.get('tmdb_id')) == str(media_tmdb_id):
                    item['status'] = new_status
                    item_found = True
                    break
            
            if not item_found:
                conn.rollback()
                return False

            missing_count = sum(1 for item in media_items if item.get('status') == 'missing')
            new_health_status = 'has_missing' if missing_count > 0 else 'ok'
            
            update_data = {
                "generated_media_info_json": json.dumps(media_items, ensure_ascii=False),
                "missing_count": missing_count,
                "health_status": new_health_status
            }
            
            set_clauses = [f"{key} = %s" for key in update_data.keys()]
            values = list(update_data.values())
            sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = %s"
            values.append(collection_id)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 中媒体 {media_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新自定义合集中媒体状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

# --- 更新榜单合集 ---
def match_and_update_list_collections_on_item_add(new_item_tmdb_id: str, new_item_emby_id: str, new_item_name: str) -> List[Dict[str, Any]]:
    """
    【V3 - PG JSONB 查询修复版】
    当新媒体入库时，查找所有匹配的'list'类型合集，更新其内部状态，并返回需要被操作的Emby合集信息。
    - 修复了因对 JSONB 字段使用 LIKE 操作符导致的数据库错误。
    - 改为使用 PostgreSQL 高效的 @> (contains) 操作符进行查询。
    """
    collections_to_update_in_emby = []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复 1/2：修改 SQL 查询语句 ★★★
            # 不再使用 LIKE，而是使用 @> 操作符，并对参数进行 ::jsonb 类型转换
            sql_find = """
                SELECT * FROM custom_collections 
                WHERE type = 'list' 
                  AND status = 'active' 
                  AND emby_collection_id IS NOT NULL
                  AND generated_media_info_json @> %s::jsonb
            """
            
            # ★★★ 核心修复 2/2：构建一个符合 @> 操作符要求的 JSON 字符串作为参数 ★★★
            # 我们要查找的是一个数组，这个数组里包含一个 tmdb_id 是目标ID的对象
            search_payload = json.dumps([{'tmdb_id': str(new_item_tmdb_id)}])
            
            cursor.execute(sql_find, (search_payload,))
            candidate_collections = cursor.fetchall()

            if not candidate_collections:
                logger.debug(f"  -> 未在任何榜单合集中找到 TMDb ID: {new_item_tmdb_id}。")
                return []

            # --- 后续逻辑保持不变 ---
            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in candidate_collections:
                    collection = dict(collection_row)
                    collection_id = collection['id']
                    collection_name = collection['name']
                    
                    try:
                        # psycopg2 会自动将 jsonb 转为 list/dict，所以这里直接用
                        media_list = collection.get('generated_media_info_json') or []
                        item_found_and_updated = False
                        
                        for media_item in media_list:
                            if str(media_item.get('tmdb_id')) == str(new_item_tmdb_id) and media_item.get('status') != 'in_library':
                                old_status_key = media_item.get('status', 'unknown')
                                new_status_key = 'in_library'
                                old_status_cn = STATUS_TRANSLATION_MAP.get(old_status_key, old_status_key)
                                new_status_cn = STATUS_TRANSLATION_MAP.get(new_status_key, new_status_key)

                                logger.info(f"  -> 数据库状态更新：项目《{new_item_name}》在合集《{collection_name}》中的状态将从【{old_status_cn}】更新为【{new_status_cn}】。")
                                
                                media_item['status'] = new_status_key
                                media_item['emby_id'] = new_item_emby_id 
                                item_found_and_updated = True
                                break
                        
                        if item_found_and_updated:
                            new_in_library_count = sum(1 for m in media_list if m.get('status') == 'in_library')
                            new_missing_count = sum(1 for m in media_list if m.get('status') == 'missing')
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            new_json_data = json.dumps(media_list, ensure_ascii=False)
                            
                            cursor.execute("""
                                UPDATE custom_collections
                                SET generated_media_info_json = %s,
                                    in_library_count = %s,
                                    missing_count = %s,
                                    health_status = %s
                                WHERE id = %s
                            """, (new_json_data, new_in_library_count, new_missing_count, new_health_status, collection_id))
                            
                            collections_to_update_in_emby.append({
                                'emby_collection_id': collection['emby_collection_id'],
                                'name': collection_name
                            })

                    except (json.JSONDecodeError, TypeError) as e_json:
                        logger.warning(f"解析或处理榜单合集《{collection_name}》的数据时出错: {e_json}，跳过。")
                        continue
                
                conn.commit()
                
            except Exception as e_trans:
                conn.rollback()
                logger.error(f"在更新榜单合集数据库状态的事务中发生错误: {e_trans}", exc_info=True)
                raise

        return collections_to_update_in_emby

    except psycopg2.Error as e_db:
        logger.error(f"匹配和更新榜单合集时发生数据库错误: {e_db}", exc_info=True)
        raise
def get_media_metadata_by_tmdb_ids(tmdb_ids: List[str], item_type: str) -> List[Dict[str, Any]]:
    """
    根据一个 TMDb ID 列表，从媒体元数据缓存表中批量获取记录。
    """
    if not tmdb_ids:
        return []
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 ANY(%s) 语法可以高效地查询数组中的成员
            sql = "SELECT * FROM media_metadata WHERE item_type = %s AND tmdb_id = ANY(%s)"
            cursor.execute(sql, (item_type, tmdb_ids))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"根据TMDb ID列表批量获取媒体元数据时出错: {e}", exc_info=True)
        return []
# ★★★ 新增函数：为规则筛选类合集在数据库中追加一个媒体项 ★★★
def append_item_to_filter_collection_db(collection_id: int, new_item_tmdb_id: str, new_item_emby_id: str) -> bool:
    """
    当一个新媒体项匹配规则筛选合集时，更新数据库中的状态。
    这包括向 generated_media_info_json 追加精简信息，并更新 in_library_count。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 开启事务并锁定行，防止并发写入冲突
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("SELECT generated_media_info_json, in_library_count FROM custom_collections WHERE id = %s FOR UPDATE", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                logger.warning(f"尝试向规则合集 (DB ID: {collection_id}) 追加媒体项，但未找到该合集。")
                return False

            # 获取当前JSON，如果为NULL或无效，则初始化为空列表
            media_list = row.get('generated_media_info_json') or []
            if not isinstance(media_list, list):
                media_list = []
            
            # 检查是否已存在，避免重复添加
            if any(item.get('emby_id') == new_item_emby_id for item in media_list):
                conn.rollback()
                logger.debug(f"媒体项 {new_item_emby_id} 已存在于合集 {collection_id} 的JSON缓存中，跳过追加。")
                return True

            # 追加新的精简媒体信息
            media_list.append({
                'tmdb_id': new_item_tmdb_id,
                'emby_id': new_item_emby_id
            })
            
            # 更新库内数量
            new_in_library_count = (row.get('in_library_count') or 0) + 1
            
            # 将更新后的数据写回数据库
            new_json_data = json.dumps(media_list, ensure_ascii=False)
            cursor.execute(
                "UPDATE custom_collections SET generated_media_info_json = %s, in_library_count = %s WHERE id = %s",
                (new_json_data, new_in_library_count, collection_id)
            )
            conn.commit()
            logger.info(f"  -> 数据库状态同步：已将新媒体项 {new_item_emby_id} 追加到规则合集 (DB ID: {collection_id}) 的JSON缓存中。")
            return True

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
        logger.error(f"向规则合集 {collection_id} 的JSON缓存追加媒体项时发生数据库错误: {e}", exc_info=True)
        return False
# ======================================================================
# 模块 7: 应用设置数据访问 (Application Settings Data Access)
# ======================================================================

def get_setting(setting_key: str) -> Optional[Any]:
    """
    从 app_settings 表中获取一个设置项的值。
    :param setting_key: 设置项的键。
    :return: 设置项的值 (通常是字典)，如果未找到则返回 None。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = %s", (setting_key,))
            row = cursor.fetchone()
            # psycopg2 会自动将 JSONB 字段解析为 Python 对象
            return row['value_json'] if row else None
    except Exception as e:
        logger.error(f"DB: 获取设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise

def _save_setting_with_cursor(cursor, setting_key: str, value: Dict[str, Any]):
    """
    【内部函数】使用一个已有的数据库游标来保存设置。
    这个函数不开启或提交事务，由调用方负责。
    """
    sql = """
        INSERT INTO app_settings (setting_key, value_json, last_updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (setting_key) DO UPDATE SET
            value_json = EXCLUDED.value_json,
            last_updated_at = NOW();
    """
    value_as_json = json.dumps(value, ensure_ascii=False)
    cursor.execute(sql, (setting_key, value_as_json))

def save_setting(setting_key: str, value: Dict[str, Any]):
    """
    【V2 - 重构版】向 app_settings 表中保存或更新一个设置项。
    现在它调用内部函数来完成工作。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 核心修改：调用新的内部函数 ★★★
            _save_setting_with_cursor(cursor, setting_key, value)
            conn.commit()
            logger.info(f"DB: 成功保存设置 '{setting_key}'。")
    except Exception as e:
        logger.error(f"DB: 保存设置 '{setting_key}' 时失败: {e}", exc_info=True)
        raise
# ======================================================================
# 模块 8: 媒体洗版设置数据访问 (Resubscribe Settings Data Access) - ★★★ 新增模块 ★★★
# ======================================================================
# --- 规则管理 (Rules Management) ---

def _prepare_rule_data_for_db(rule_data: Dict[str, Any]) -> Dict[str, Any]:
    """【内部函数】准备规则数据以便存入数据库，自动包装JSONB字段。"""
    data_to_save = rule_data.copy()
    jsonb_fields = [
        'target_library_ids', 'resubscribe_audio_missing_languages',
        'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
        'resubscribe_effect_include'
    ]
    for field in jsonb_fields:
        if field in data_to_save and data_to_save[field] is not None:
            data_to_save[field] = Json(data_to_save[field])
    return data_to_save

def get_all_resubscribe_rules() -> List[Dict[str, Any]]:
    """获取所有洗版规则，按优先级排序。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_rules ORDER BY sort_order ASC, id ASC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取所有洗版规则时失败: {e}", exc_info=True)
        return []

def create_resubscribe_rule(rule_data: Dict[str, Any]) -> int:
    """创建一条新的洗版规则，并返回其ID。"""
    try:
        prepared_data = _prepare_rule_data_for_db(rule_data)
        columns = prepared_data.keys()
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO resubscribe_rules ({', '.join(columns)}) VALUES ({placeholders}) RETURNING id"
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, list(prepared_data.values()))
            result = cursor.fetchone()
            if not result:
                raise psycopg2.Error("数据库未能返回新创建的规则ID。")
            new_id = result['id']
            conn.commit()
            logger.info(f"DB: 成功创建洗版规则 '{rule_data.get('name')}' (ID: {new_id})。")
            return new_id
    except psycopg2.IntegrityError as e:
        logger.warning(f"DB: 创建洗版规则失败，可能名称 '{rule_data.get('name')}' 已存在: {e}")
        raise
    except Exception as e:
        logger.error(f"DB: 创建洗版规则时发生未知错误: {e}", exc_info=True)
        raise

def update_resubscribe_rule(rule_id: int, rule_data: Dict[str, Any]) -> bool:
    """更新指定ID的洗版规则。"""
    try:
        prepared_data = _prepare_rule_data_for_db(rule_data)
        set_clauses = [f"{key} = %s" for key in prepared_data.keys()]
        sql = f"UPDATE resubscribe_rules SET {', '.join(set_clauses)} WHERE id = %s"
        values = list(prepared_data.values())
        values.append(rule_id)
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            if cursor.rowcount == 0:
                logger.warning(f"DB: 尝试更新洗版规则ID {rule_id}，但在数据库中未找到。")
                return False
            conn.commit()
            logger.info(f"DB: 成功更新洗版规则ID {rule_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def delete_resubscribe_rule(rule_id: int) -> bool:
    """删除指定ID的洗版规则。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_rules WHERE id = %s", (rule_id,))
            if cursor.rowcount == 0:
                logger.warning(f"DB: 尝试删除洗版规则ID {rule_id}，但在数据库中未找到。")
                return False
            conn.commit()
            logger.info(f"DB: 成功删除洗版规则ID {rule_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 删除洗版规则ID {rule_id} 时失败: {e}", exc_info=True)
        raise

def update_resubscribe_rules_order(ordered_ids: List[int]) -> bool:
    """根据ID列表批量更新洗版规则的排序。"""
    if not ordered_ids:
        return True
    data_to_update = [(index, rule_id) for index, rule_id in enumerate(ordered_ids)]
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            from psycopg2.extras import execute_values
            sql = "UPDATE resubscribe_rules SET sort_order = data.sort_order FROM (VALUES %s) AS data(sort_order, id) WHERE resubscribe_rules.id = data.id;"
            execute_values(cursor, sql, data_to_update)
            conn.commit()
            logger.info(f"DB: 成功更新了 {len(ordered_ids)} 个洗版规则的顺序。")
            return True
    except Exception as e:
        logger.error(f"DB: 批量更新洗版规则顺序时失败: {e}", exc_info=True)
        raise

# --- 缓存管理 (Cache Management) ---

def get_all_resubscribe_cache() -> List[Dict[str, Any]]:
    """获取所有洗版缓存数据。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache ORDER BY item_name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取洗版缓存失败: {e}", exc_info=True)
        return []

def upsert_resubscribe_cache_batch(items_data: List[Dict[str, Any]]):
    """【V5 - 增加来源库ID】批量更新或插入洗版缓存数据。"""
    if not items_data:
        return

    sql = """
        INSERT INTO resubscribe_cache (
            item_id, item_name, tmdb_id, item_type, status, reason,
            resolution_display, quality_display, effect_display, audio_display, subtitle_display,
            audio_languages_raw, subtitle_languages_raw, last_checked_at,
            matched_rule_id, matched_rule_name, source_library_id
        ) VALUES %s
        ON CONFLICT (item_id) DO UPDATE SET
            item_name = EXCLUDED.item_name, tmdb_id = EXCLUDED.tmdb_id,
            item_type = EXCLUDED.item_type, status = EXCLUDED.status,
            reason = EXCLUDED.reason, resolution_display = EXCLUDED.resolution_display,
            quality_display = EXCLUDED.quality_display, effect_display = EXCLUDED.effect_display,
            audio_display = EXCLUDED.audio_display, subtitle_display = EXCLUDED.subtitle_display,
            audio_languages_raw = EXCLUDED.audio_languages_raw,
            subtitle_languages_raw = EXCLUDED.subtitle_languages_raw,
            last_checked_at = EXCLUDED.last_checked_at,
            matched_rule_id = EXCLUDED.matched_rule_id,
            matched_rule_name = EXCLUDED.matched_rule_name,
            source_library_id = EXCLUDED.source_library_id;
    """
    values_to_insert = []
    for item in items_data:
        values_to_insert.append((
            item.get('item_id'), item.get('item_name'), item.get('tmdb_id'),
            item.get('item_type'), item.get('status'), item.get('reason'),
            item.get('resolution_display'), item.get('quality_display'), item.get('effect_display'),
            item.get('audio_display'), item.get('subtitle_display'),
            json.dumps(item.get('audio_languages_raw', [])),
            json.dumps(item.get('subtitle_languages_raw', [])),
            datetime.now(timezone.utc),
            item.get('matched_rule_id'),
            item.get('matched_rule_name'),
            item.get('source_library_id')
        ))
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            from psycopg2.extras import execute_values
            execute_values(cursor, sql, values_to_insert, page_size=500)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新洗版缓存失败: {e}", exc_info=True)
        raise

def update_resubscribe_item_status(item_id: str, new_status: str) -> bool:
    """更新单个洗版缓存条目的状态。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE resubscribe_cache SET status = %s WHERE item_id = %s",
                (new_status, item_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 更新洗版缓存状态失败 for item {item_id}: {e}", exc_info=True)
        return False

def delete_resubscribe_cache_by_rule_id(rule_id: int) -> int:
    """根据规则ID，删除所有关联的洗版缓存项。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_cache WHERE matched_rule_id = %s", (rule_id,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 联动删除了 {deleted_count} 条与规则ID {rule_id} 关联的洗版缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 根据规则ID {rule_id} 删除洗版缓存时失败: {e}", exc_info=True)
        raise

def delete_resubscribe_cache_for_unwatched_libraries(watched_library_ids: List[str]) -> int:
    """删除所有来自“未被任何规则监控的”媒体库的缓存项。"""
    if not watched_library_ids:
        sql = "DELETE FROM resubscribe_cache"
        params = []
    else:
        # 使用元组作为IN子句的参数
        sql = "DELETE FROM resubscribe_cache WHERE source_library_id IS NOT NULL AND source_library_id NOT IN %s"
        params = [tuple(watched_library_ids)]
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"DB: [自愈清理] 成功删除了 {deleted_count} 条来自无效媒体库的陈旧洗版缓存。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: [自愈清理] 清理无效洗版缓存时失败: {e}", exc_info=True)
        raise

def get_resubscribe_cache_item(item_id: str) -> Optional[Dict[str, Any]]:
    """根据 item_id 获取单个洗版缓存项。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache WHERE item_id = %s", (item_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB: 获取单个洗版缓存项 {item_id} 失败: {e}", exc_info=True)
        return None

def get_resubscribe_rule_by_id(rule_id: int) -> Optional[Dict[str, Any]]:
    """根据 rule_id 获取单个洗版规则。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_rules WHERE id = %s", (rule_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB: 获取单个洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return None
    
def delete_resubscribe_cache_item(item_id: str) -> bool:
    """根据 item_id 从洗版缓存中删除单条记录。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM resubscribe_cache WHERE item_id = %s", (item_id,))
            conn.commit()
            # 确认是否真的删掉了一行
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB: 删除单条洗版缓存项 {item_id} 失败: {e}", exc_info=True)
        return False
    
def batch_update_resubscribe_cache_status(item_ids: List[str], new_status: str) -> int:
    """批量更新一组洗版缓存项的状态。"""
    if not item_ids or not new_status:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE resubscribe_cache SET status = %s WHERE item_id = ANY(%s)"
            cursor.execute(sql, (new_status, item_ids))
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 成功将 {updated_count} 个洗版缓存项的状态批量更新为 '{new_status}'。")
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量更新洗版缓存状态时失败: {e}", exc_info=True)
        return 0

# ======================================================================
# 模块 9: 全局订阅配额管理器 (Subscription Quota Manager) -          ★★★
# ======================================================================

def get_subscription_quota() -> int:
    """
    【V3 - 终极健壮版】获取当前可用的订阅配额。
    - 彻底修复了因每日上限下调导致剩余配额>总配额、已用配额为负数的BUG。
    - 增加了最终安全校验，确保返回的剩余配额永远不会超过当前设定的每日上限。
    - 能够自动修复和迁移旧的、不一致的数据库状态。
    """
    try:
        # 1. 永远从主配置中读取最新的最大配额，这是当前的“权威”值
        current_max_quota = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP, 200)
        
        # 获取今天的日期字符串
        today_str = datetime.now(pytz.timezone(constants.TIMEZONE)).strftime('%Y-%m-%d')

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 2. 从数据库获取配额状态
            state = get_setting('subscription_quota_state') or {}
            last_reset_date = state.get('last_reset_date')
            
            # --- 逻辑分支 A：新的一天，执行重置 ---
            if last_reset_date != today_str:
                logger.info(f"检测到新的一天 ({today_str})，正在重置订阅配额为 {current_max_quota}。")
                new_state = {
                    'current_quota': current_max_quota,
                    'last_reset_date': today_str,
                    'max_quota_on_reset': current_max_quota
                }
                save_setting('subscription_quota_state', new_state)
                return current_max_quota

            # --- 逻辑分支 B：仍在当天，进行动态计算和校验 ---
            else:
                max_quota_on_reset = state.get('max_quota_on_reset', -1) # 使用-1作为哨兵，表示状态是旧版的
                current_quota_in_db = state.get('current_quota', 0)
                
                effective_remaining_quota = 0
                
                # 如果是现代状态（有重置记录）且配置发生了变化
                if max_quota_on_reset != -1 and current_max_quota != max_quota_on_reset:
                    # 精确计算已消耗数量
                    consumed = max_quota_on_reset - current_quota_in_db
                    # 基于新上限，计算理论上应剩余的数量
                    effective_remaining_quota = max(0, current_max_quota - consumed)
                else:
                    # 如果是旧版状态，或配置未变，则先沿用数据库中的值
                    effective_remaining_quota = current_quota_in_db
                    
                # ★★★ 终极安全校验 ★★★
                # 无论经过何种计算，剩余配额绝对不能超过当前的总上限。
                final_remaining_quota = min(effective_remaining_quota, current_max_quota)
                
                # 如果计算结果与数据库不符（说明需要修正），或者状态是旧版的，就更新数据库
                if final_remaining_quota != current_quota_in_db or max_quota_on_reset == -1:
                    logger.info(f"动态调整或修正了当日订阅配额。旧剩余: {current_quota_in_db}, 新剩余: {final_remaining_quota}, 当前上限: {current_max_quota}")
                    new_state = {
                        'current_quota': final_remaining_quota,
                        'last_reset_date': today_str,
                        'max_quota_on_reset': current_max_quota # 将基准更新为当前上限
                    }
                    save_setting('subscription_quota_state', new_state)
                
                return final_remaining_quota

    except Exception as e:
        logger.error(f"获取订阅配额时发生严重错误，将返回0以确保安全: {e}", exc_info=True)
        return 0

def decrement_subscription_quota() -> bool:
    """
    将当前订阅配额减一。
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN;")
            try:
                cursor.execute("SELECT value_json FROM app_settings WHERE setting_key = 'subscription_quota_state' FOR UPDATE")
                row = cursor.fetchone()
                
                if not row or not row.get('value_json'):
                    conn.rollback()
                    logger.warning("尝试减少配额，但未找到配额状态记录。")
                    return False

                state = row['value_json']
                current_quota = state.get('current_quota', 0)

                if current_quota > 0:
                    state['current_quota'] = current_quota - 1
                    # ★★★ 核心修改：直接调用内部函数，在同一个事务中完成所有操作 ★★★
                    _save_setting_with_cursor(cursor, 'subscription_quota_state', state)
                    logger.debug(f"  -> 配额已消耗，剩余: {state['current_quota']}")
                
                conn.commit()
                return True
            except Exception as e_trans:
                conn.rollback()
                logger.error(f"减少配额的数据库事务失败: {e_trans}", exc_info=True)
                return False
    except Exception as e:
        logger.error(f"减少订阅配额时发生严重错误: {e}", exc_info=True)
        return False
# ======================================================================
# 模块 10: 媒体去重模块数据访问 (Media Cleanup Data Access) - ★★★ 新增模块 ★★★
# ======================================================================

def get_all_cleanup_tasks() -> List[Dict[str, Any]]:
    """获取所有待处理的媒体去重任务。"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 只获取待处理的任务，并按名称排序
            cursor.execute("SELECT * FROM media_cleanup_tasks WHERE status = 'pending' ORDER BY item_name")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取媒体去重任务列表失败: {e}", exc_info=True)
        return []

def batch_insert_cleanup_tasks(tasks: List[Dict[str, Any]]):
    """批量插入新的清理任务，插入前会清空旧表。"""
    if not tasks:
        logger.info("没有发现需要清理的媒体项，无需更新数据库。")
        return

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 TRUNCATE RESTART IDENTITY 清空表并重置ID序列
            logger.warning("正在清空旧的媒体去重任务列表...")
            cursor.execute("TRUNCATE TABLE media_cleanup_tasks RESTART IDENTITY;")

            # 准备批量插入
            from psycopg2.extras import execute_values
            sql = """
                INSERT INTO media_cleanup_tasks (
                    task_type, tmdb_id, item_name, versions_info_json, 
                    status, best_version_id, created_at
                ) VALUES %s
            """
            
            values_to_insert = [
                (
                    task.get('task_type'),
                    task.get('tmdb_id'),
                    task.get('item_name'),
                    Json(task.get('versions_info_json')), # 使用Json适配器
                    task.get('status', 'pending'),
                    task.get('best_version_id'),
                    datetime.now(timezone.utc)
                ) for task in tasks
            ]
            
            execute_values(cursor, sql, values_to_insert, page_size=500)
            conn.commit()
            logger.info(f"DB: 成功批量插入 {len(tasks)} 条新的媒体去重任务。")

    except Exception as e:
        logger.error(f"DB: 批量插入媒体去重任务时失败: {e}", exc_info=True)
        raise

def get_cleanup_tasks_by_ids(task_ids: List[int]) -> List[Dict[str, Any]]:
    """根据任务ID列表获取清理任务详情。"""
    if not task_ids:
        return []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM media_cleanup_tasks WHERE id = ANY(%s)"
            cursor.execute(sql, (task_ids,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 根据ID获取清理任务时失败: {e}", exc_info=True)
        return []

def batch_update_cleanup_task_status(task_ids: List[int], new_status: str) -> int:
    """批量更新一组清理任务的状态。"""
    if not task_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "UPDATE media_cleanup_tasks SET status = %s WHERE id = ANY(%s)"
            cursor.execute(sql, (new_status, task_ids))
            updated_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 成功将 {updated_count} 个清理任务的状态更新为 '{new_status}'。")
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量更新清理任务状态时失败: {e}", exc_info=True)
        return 0

def batch_delete_cleanup_tasks(task_ids: List[int]) -> int:
    """批量删除一组清理任务。"""
    if not task_ids:
        return 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "DELETE FROM media_cleanup_tasks WHERE id = ANY(%s)"
            cursor.execute(sql, (task_ids,))
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"DB: 成功删除了 {deleted_count} 个清理任务。")
            return deleted_count
    except Exception as e:
        logger.error(f"DB: 批量删除清理任务时失败: {e}", exc_info=True)
        return 0
    
# ======================================================================
# 模块 11: 用户媒体数据访问 (User Media Data Access) - ★★★ 新增模块 ★★★
# ======================================================================

def upsert_user_media_data(data: Dict[str, Any]):
    """
    【V1】根据Webhook传入的数据，更新或插入单条用户媒体状态。
    """
    user_id = data.get('user_id')
    item_id = data.get('item_id')
    if not user_id or not item_id:
        return

    # 动态构建 SET 子句
    data['last_updated_at'] = datetime.now(timezone.utc)
    set_clauses = [f"{key} = EXCLUDED.{key}" for key in data.keys() if key not in ['user_id', 'item_id']]
    
    # 准备列名和占位符
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
    """
    【V1】为一个指定用户，批量更新或插入其所有媒体的状态。
    """
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
            from psycopg2.extras import execute_values
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
            from psycopg2.extras import execute_values
            execute_values(cursor, sql, values_to_insert, page_size=100)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新Emby用户缓存失败: {e}", exc_info=True)
        raise

def get_item_ids_by_dynamic_rules(user_id: str, rules: List[Dict[str, Any]]) -> Optional[List[str]]:
    """
    【V2 - 时间维度版】
    根据动态筛选规则，从 user_media_data 表中获取匹配的媒体项ID列表。
    - 新增对 `last_played_date` 字段的筛选支持。
    """
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
    
    logger.debug(f"执行动态筛选SQL: {final_sql} with params: {params}")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(final_sql, tuple(params))
            rows = cursor.fetchall()
            return [row['item_id'] for row in rows]
    except Exception as e:
        logger.error(f"DB: 根据动态规则获取媒体ID时失败 for user {user_id}: {e}", exc_info=True)
        return None
def upsert_user_media_data_batch_no_date(user_id: str, items_data: List[Dict[str, Any]]):
    """
    【V1 - 精准夺权版】
    为一个指定用户，批量更新或插入其媒体状态，但【明确排除】last_played_date 字段。
    此函数专为“同步用户数据”任务设计，防止它覆盖由Webhook记录的精确时间戳。
    """
    if not user_id or not items_data:
        return

    # ★★★ 核心：SQL语句里，完全没有 last_played_date 的身影 ★★★
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
            from psycopg2.extras import execute_values
            execute_values(cursor, sql, values_to_insert, page_size=1000)
            conn.commit()
    except Exception as e:
        logger.error(f"DB: 批量更新用户 {user_id} 的媒体数据时失败 (no_date): {e}", exc_info=True)
        raise