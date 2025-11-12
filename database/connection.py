# database/connection.py
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

import config_manager
import constants

logger = logging.getLogger(__name__)

# ======================================================================
# 模块: 中央数据访问 
# ======================================================================

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

def init_db():
    """
    【PostgreSQL版】初始化数据库，创建所有表的最终结构。
    """
    logger.debug("  ➜ 正在初始化 PostgreSQL 数据库，创建/验证所有表的结构...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.trace("  ➜ 数据库连接成功，开始建表...")

                # --- 1. 创建基础表 (日志、缓存、用户) ---
                logger.trace("  ➜ 正在创建基础表...")
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

                logger.trace("  ➜ 正在创建 'emby_users' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS emby_users (
                        id TEXT PRIMARY KEY, name TEXT NOT NULL, is_administrator BOOLEAN,
                        last_seen_at TIMESTAMP WITH TIME ZONE, profile_image_tag TEXT,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                logger.trace("  ➜ 正在创建 'user_media_data' 表...")
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

                logger.trace("  ➜ 正在创建 'collections_info' 表 ...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS collections_info (
                        emby_collection_id TEXT PRIMARY KEY,
                        name TEXT,
                        tmdb_collection_id TEXT,
                        status TEXT,
                        has_missing BOOLEAN, 
                        -- ★★★ 只存储缺失电影的 TMDB ID 列表, e.g., ["123", "456"] ★★★
                        missing_movies_json JSONB,
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        poster_path TEXT,
                        item_type TEXT DEFAULT 'Movie' NOT NULL,
                        in_library_count INTEGER DEFAULT 0
                    );
                """)

                logger.trace("  ➜ 正在创建 'custom_collections' 表 (适配新架构)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS custom_collections (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        type TEXT NOT NULL,
                        definition_json JSONB NOT NULL,
                        status TEXT DEFAULT 'active',
                        emby_collection_id TEXT,
                        allowed_user_ids JSONB,
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
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cc_type ON custom_collections (type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cc_status ON custom_collections (status);")

                logger.trace("  ➜ 正在创建 'user_collection_cache' 表 (虚拟库权限预计算)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_collection_cache (
                        user_id TEXT NOT NULL,
                        collection_id INTEGER NOT NULL,
                        visible_emby_ids_json JSONB,
                        total_count INTEGER DEFAULT 0,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (user_id, collection_id)
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_ucc_user_coll ON user_collection_cache (user_id, collection_id);")

                logger.trace("  ➜ 正在创建 'media_metadata' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS media_metadata (
                        -- 核心标识符
                        tmdb_id TEXT NOT NULL,
                        item_type TEXT NOT NULL, -- 'Movie', 'Series', 'Season', 'Episode'
                        imdb_id TEXT UNIQUE,
                        tvdb_id TEXT,

                        -- 媒体库状态
                        in_library BOOLEAN DEFAULT FALSE NOT NULL,
                        emby_item_id TEXT, -- ★★★ 保留旧字段，用于平滑升级 ★★★
                        emby_item_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        date_added TIMESTAMP WITH TIME ZONE,
                        paths_json JSONB,

                        -- 订阅与状态管理
                        subscription_status TEXT NOT NULL DEFAULT 'NONE', -- 'NONE', 'WANTED', 'SUBSCRIBED', 'IGNORED'
                        subscription_sources_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        first_requested_at TIMESTAMP WITH TIME ZONE,
                        last_subscribed_at TIMESTAMP WITH TIME ZONE,

                        -- 预处理与缓存
                        pre_processed_at TIMESTAMP WITH TIME ZONE,
                        translated_title TEXT,
                        translated_overview TEXT,
                        pre_cached_tags_json JSONB,
                        pre_cached_extra_json JSONB,

                        -- 核心与扩展元数据
                        title TEXT,
                        original_title TEXT,
                        overview TEXT,
                        release_date DATE,
                        release_year INTEGER,
                        poster_path TEXT,
                        backdrop_path TEXT,
                        runtime_minutes INTEGER,
                        rating REAL,
                        vote_count INTEGER,
                        popularity REAL,
                        official_rating TEXT,
                        genres_json JSONB,
                        actors_json JSONB,
                        directors_json JSONB,
                        studios_json JSONB,
                        countries_json JSONB,
                        keywords_json JSONB,
                        ignore_reason TEXT,

                        -- 剧集专属与层级数据
                        tmdb_status TEXT,
                        total_seasons INTEGER,
                        total_episodes INTEGER,
                        next_episode_to_air_json JSONB,
                        last_episode_to_air_json JSONB,
                        is_airing BOOLEAN DEFAULT FALSE NOT NULL,
                        parent_series_tmdb_id TEXT,
                        season_number INTEGER,
                        episode_number INTEGER,

                        -- 内部管理字段
                        last_synced_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        -- 主键
                        PRIMARY KEY (tmdb_id, item_type)
                    )
                """)

                logger.trace("  ➜ 正在创建 'watchlist' 表...")
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
                        force_ended BOOLEAN DEFAULT FALSE NOT NULL,
                        resubscribe_info_json JSONB,
                        is_airing BOOLEAN DEFAULT FALSE NOT NULL
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_status ON watchlist (status)")

                logger.trace("  ➜ 正在创建 'person_identity_map' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS person_identity_map (
                        map_id SERIAL PRIMARY KEY, 
                        primary_name TEXT NOT NULL, 
                        emby_person_id TEXT UNIQUE,
                        tmdb_person_id INTEGER UNIQUE, 
                        imdb_id TEXT UNIQUE, 
                        douban_celebrity_id TEXT UNIQUE,
                        last_synced_at TIMESTAMP WITH TIME ZONE, 
                        last_updated_at TIMESTAMP WITH TIME ZONE
                    )
                """)

                logger.trace("  ➜ 正在创建 'actor_metadata' 表...")
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

                logger.trace("  ➜ 正在创建 'actor_subscriptions' 表...")
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
                        config_min_rating REAL DEFAULT 6.0,
                        config_main_role_only BOOLEAN NOT NULL DEFAULT FALSE,
                        config_min_vote_count INTEGER NOT NULL DEFAULT 10
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_as_status ON actor_subscriptions (status)")

                logger.trace("  ➜ 正在创建 'resubscribe_rules' 表 (多规则洗版)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resubscribe_rules (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        enabled BOOLEAN DEFAULT TRUE,
                        target_library_ids JSONB, 
                        delete_after_resubscribe BOOLEAN DEFAULT FALSE,
                        sort_order INTEGER DEFAULT 0,
                        resubscribe_resolution_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_resolution_threshold INT DEFAULT 1920,
                        resubscribe_audio_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_audio_missing_languages JSONB,
                        resubscribe_subtitle_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_subtitle_missing_languages JSONB,
                        resubscribe_quality_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_quality_include JSONB,
                        resubscribe_effect_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_effect_include JSONB,
                        resubscribe_filesize_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_filesize_operator TEXT DEFAULT 'lt', 
                        resubscribe_filesize_threshold_gb REAL DEFAULT 10.0 
                    )
                """)

                logger.trace("  ➜ 正在创建 'resubscribe_cache' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resubscribe_cache (
                        item_id TEXT PRIMARY KEY,
                        emby_item_id TEXT,
                        series_id TEXT,  
                        season_number INTEGER,
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
                        source_library_id TEXT,
                        path TEXT,
                        filename TEXT
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_resubscribe_cache_status ON resubscribe_cache (status);")

                logger.trace("  ➜ 正在创建 'media_cleanup_tasks' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS media_cleanup_tasks (
                        id SERIAL PRIMARY KEY,
                        task_type TEXT NOT NULL, -- 'multi_version' or 'duplicate'
                        tmdb_id TEXT,
                        item_name TEXT,
                        item_type TEXT, -- 新增 item_type 列
                        versions_info_json JSONB,
                        status TEXT DEFAULT 'pending', -- 'pending', 'processed', 'ignored'
                        best_version_id TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cleanup_task_type ON media_cleanup_tasks (task_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_cleanup_task_status ON media_cleanup_tasks (status);")

                logger.trace("  ➜ 正在创建 'user_templates' 表 (用户权限模板)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_templates (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        description TEXT,
                        -- 核心字段：存储一个完整的 Emby 用户策略 JSON 对象
                        emby_policy_json JSONB NOT NULL,
                        -- 模板默认的有效期（天数），0 表示永久
                        default_expiration_days INTEGER DEFAULT 30,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        allow_unrestricted_subscriptions BOOLEAN DEFAULT FALSE NOT NULL
                    )
                """)

                logger.trace("  ➜ 正在创建 'invitations' 表 (邀请码)...")
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

                logger.trace("  ➜ 正在创建 'emby_users_extended' 表 (用户扩展信息)...")
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

                logger.trace("  ➜ 正在创建 'subscription_requests' 表 (TMDb探索)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS subscription_requests (
                        id SERIAL PRIMARY KEY,
                        emby_user_id TEXT NOT NULL,
                        tmdb_id TEXT NOT NULL,
                        item_type TEXT NOT NULL, -- 'Movie' or 'Series'
                        item_name TEXT,
                        parent_tmdb_id TEXT,        -- 解析后的父剧集TMDb ID
                        parsed_series_name TEXT,    -- 解析后的父剧集名称
                        parsed_season_number INTEGER, -- 解析后的季号
                        status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'approved', 'rejected', 'processing', 'completed'
                        requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        processed_at TIMESTAMP WITH TIME ZONE,
                        processed_by TEXT, -- 'admin' or 'auto' (for VIPs)
                        notes TEXT, -- 管理员可以填写拒绝理由等
                        FOREIGN KEY(emby_user_id) REFERENCES emby_users(id) ON DELETE CASCADE
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sr_status ON subscription_requests (status);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sr_user_id ON subscription_requests (emby_user_id);")

                # --- 2. 执行平滑升级检查 ---
                logger.trace("  ➜ 开始执行数据库表结构升级检查...")
                
                # --- 2.1 移除 actor_metadata 的外键约束 (如果存在) ---
                try:
                    logger.trace("  ➜ [数据库升级] 正在检查并移除 'actor_metadata' 的外键约束...")
                    cursor.execute("""
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = 'actor_metadata'::regclass
                          AND confrelid = 'person_identity_map'::regclass
                          AND contype = 'f';
                    """)
                    constraint = cursor.fetchone()
                    if constraint:
                        constraint_name = constraint['conname']
                        logger.info(f"    ➜ [数据库升级] 检测到旧的外键约束 '{constraint_name}'，正在移除...")
                        cursor.execute(f"ALTER TABLE actor_metadata DROP CONSTRAINT IF EXISTS {constraint_name};")
                        logger.info(f"    ➜ [数据库升级] 约束 '{constraint_name}' 移除成功。")
                    else:
                        logger.trace("    ➜ 'actor_metadata' 表无外键约束，无需升级。")
                except Exception as e_fk:
                    logger.error(f"  ➜ [数据库升级] 检查或移除外键时出错: {e_fk}", exc_info=True)

                # --- 2.2 移除 person_identity_map.emby_person_id 的 NOT NULL 约束 (如果存在) ---
                try:
                    logger.trace("  ➜ [数据库升级] 正在检查 'person_identity_map.emby_person_id' 的 NOT NULL 约束...")
                    cursor.execute("""
                        SELECT is_nullable 
                        FROM information_schema.columns 
                        WHERE table_name = 'person_identity_map' AND column_name = 'emby_person_id';
                    """)
                    column_info = cursor.fetchone()
                    if column_info and column_info['is_nullable'] == 'NO':
                        logger.trace("    ➜ [数据库升级] 检测到 'emby_person_id' 字段存在 NOT NULL 约束，正在移除...")
                        cursor.execute("ALTER TABLE person_identity_map ALTER COLUMN emby_person_id DROP NOT NULL;")
                        logger.trace("    ➜ [数据库升级] 约束移除成功。")
                    else:
                        logger.trace("    ➜ 'emby_person_id' 字段已允许为空，无需升级。")
                except Exception as e_not_null:
                    logger.error(f"  ➜ [数据库升级] 检查或移除 NOT NULL 约束时出错: {e_not_null}", exc_info=True)

                # --- 2.3 检查并添加所有缺失的列 ---
                try:
                    cursor.execute("""
                        SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = current_schema();
                    """)
                    all_existing_columns = {}
                    for row in cursor.fetchall():
                        table = row['table_name']
                        if table not in all_existing_columns:
                            all_existing_columns[table] = set()
                        all_existing_columns[table].add(row['column_name'])

                    schema_upgrades = {
                        'media_metadata': {
                            "overview": "TEXT",
                            "official_rating": "TEXT",
                            "unified_rating": "TEXT",
                            "emby_item_id": "TEXT",
                            "keywords_json": "JSONB",
                            "in_library": "BOOLEAN DEFAULT FALSE NOT NULL",
                            "emby_item_ids_json": "JSONB NOT NULL DEFAULT '[]'::jsonb",
                            "subscription_status": "TEXT NOT NULL DEFAULT 'NONE'",
                            "subscription_sources_json": "JSONB NOT NULL DEFAULT '[]'::jsonb",
                            "first_requested_at": "TIMESTAMP WITH TIME ZONE",
                            "last_subscribed_at": "TIMESTAMP WITH TIME ZONE",
                            "pre_processed_at": "TIMESTAMP WITH TIME ZONE",
                            "translated_title": "TEXT",
                            "translated_overview": "TEXT",
                            "pre_cached_tags_json": "JSONB",
                            "pre_cached_extra_json": "JSONB",
                            "tmdb_status": "TEXT",
                            "total_seasons": "INTEGER",
                            "total_episodes": "INTEGER",
                            "next_episode_to_air_json": "JSONB",
                            "last_episode_to_air_json": "JSONB",
                            "is_airing": "BOOLEAN DEFAULT FALSE NOT NULL",
                            "parent_series_tmdb_id": "TEXT",
                            "season_number": "INTEGER",
                            "episode_number": "INTEGER",
                            "ignore_reason": "TEXT"
                        },
                        'watchlist': {
                            "last_episode_to_air_json": "JSONB",
                            "resubscribe_info_json": "JSONB",
                            "is_airing": "BOOLEAN DEFAULT FALSE NOT NULL"
                        },
                        'resubscribe_cache': {
                            "emby_item_id": "TEXT",
                            "series_id": "TEXT",  
                            "season_number": "INTEGER",  
                            "matched_rule_id": "INTEGER",
                            "matched_rule_name": "TEXT",
                            "source_library_id": "TEXT",
                            "path": "TEXT",
                            "filename": "TEXT"
                        },
                        'resubscribe_rules': {
                            "resubscribe_subtitle_effect_only": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_filesize_enabled": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_filesize_operator": "TEXT DEFAULT 'lt'",
                            "resubscribe_filesize_threshold_gb": "REAL DEFAULT 10.0"
                        },
                        'media_cleanup_tasks': { # 添加 media_cleanup_tasks 的升级
                            "item_type": "TEXT"
                        },
                        'user_templates': {
                            "source_emby_user_id": "TEXT",
                            "emby_configuration_json": "JSONB",
                            "allow_unrestricted_subscriptions": "BOOLEAN DEFAULT FALSE NOT NULL"
                        },
                        'emby_users_extended': {
                            "template_id": "INTEGER",
                            "telegram_chat_id": "TEXT"
                        },
                        'actor_subscriptions': {
                            "config_main_role_only": "BOOLEAN NOT NULL DEFAULT FALSE",
                            "config_min_vote_count": "INTEGER NOT NULL DEFAULT 10"
                        }
                    }

                    for table, columns_to_add in schema_upgrades.items():
                        if table in all_existing_columns:
                            existing_cols_for_table = all_existing_columns[table]
                            for col_name, col_type in columns_to_add.items():
                                if col_name not in existing_cols_for_table:
                                    logger.info(f"    ➜ [数据库升级] 检测到 '{table}' 表缺少 '{col_name}' 字段，正在添加...")
                                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
                                    logger.info(f"    ➜ [数据库升级] 字段 '{col_name}' 添加成功。")
                                else:
                                    logger.trace(f"    ➜ 字段 '{table}.{col_name}' 已存在，跳过。")
                        else:
                            logger.warning(f"    ➜ [数据库升级] 检查表 '{table}' 时发现该表不存在，跳过升级。")

                except Exception as e_alter:
                    logger.error(f"  ➜ [数据库升级] 检查或添加新字段时出错: {e_alter}", exc_info=True)
                
                # --- 2.4 确保索引存在 ---
                logger.trace("  ➜ 正在创建/验证所有索引...")
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_in_library ON media_metadata (in_library);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_release_year ON media_metadata (release_year);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_series_status ON media_metadata (tmdb_status) WHERE item_type = 'Series';")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_parent_series ON media_metadata (parent_series_tmdb_id);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_subscription_status ON media_metadata (subscription_status) WHERE in_library = FALSE;")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_pre_processing_needed ON media_metadata (pre_processed_at) WHERE subscription_status = 'SUBSCRIBED' AND pre_processed_at IS NULL;")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_emby_ids_gin ON media_metadata USING GIN(emby_item_ids_json);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_subscription_sources_gin ON media_metadata USING GIN(subscription_sources_json);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_emby_item_id ON media_metadata (emby_item_id);")
                except Exception as e_index:
                    logger.error(f"  ➜ 创建 'emby_item_id' 索引时出错: {e_index}", exc_info=True)

                logger.trace("  ➜ 数据库升级检查完成。")

                # ======================================================================
                # ★★★ 数据库自动修正补丁 (START) ★★★
                # 修正 'media_metadata.in_library' 字段错误的默认值
                # ======================================================================
                logger.trace("  ➜ [数据库修正] 正在检查并修正 'media_metadata.in_library' 的默认值...")
                try:
                    # 查询 information_schema 来获取列的当前默认值
                    cursor.execute("""
                        SELECT column_default
                        FROM information_schema.columns
                        WHERE table_schema = current_schema()
                          AND table_name = 'media_metadata'
                          AND column_name = 'in_library';
                    """)
                    result = cursor.fetchone()
                    current_default = result['column_default'] if result else None

                    # 如果默认值是 'true' 或包含 'true' (例如 'true::boolean')，则修正它
                    if current_default and 'true' in current_default.lower():
                        logger.warning(f"    ➜ [数据库修正] 检测到 'in_library' 字段的默认值为不正确的 '{current_default}'。正在修正...")
                        
                        # 执行 ALTER COLUMN 命令来设置正确的默认值
                        cursor.execute("ALTER TABLE media_metadata ALTER COLUMN in_library SET DEFAULT FALSE;")
                        
                        logger.info("    ➜ [数据库修正] 成功将 'in_library' 的默认值修正为 FALSE。")
                    else:
                        logger.trace("    ➜ 'in_library' 字段的默认值正确，无需修正。")

                except Exception as e_fix:
                    logger.error(f"  ➜ [数据库修正] 修正 'in_library' 默认值时出错: {e_fix}", exc_info=True)
                # ======================================================================
                # ★★★ 数据库自动修正补丁 (END) ★★★

                logger.trace("  ➜ 数据库升级检查完成。")

            conn.commit()
            logger.info("  ➜ PostgreSQL 数据库初始化完成，所有表结构已创建/验证。")

    except psycopg2.Error as e_pg:
        logger.error(f"数据库初始化时发生 PostgreSQL 错误: {e_pg}", exc_info=True)
        raise
    except Exception as e_global:
        logger.error(f"数据库初始化时发生未知错误: {e_global}", exc_info=True)
        raise

# --- 临时迁移数据 过段时间删除---
def run_database_migrations():
    """
    【启动时任务】执行所有必要的数据迁移。
    这个函数应该是幂等的，即多次运行不会产生副作用。
    """
    logger.info("  ➜ 正在检查并执行数据库数据迁移...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                
                # --- 迁移任务 1: 将 media_metadata.emby_item_id 迁移到 emby_item_ids_json ---
                logger.trace("  ➜ [数据迁移] 正在处理 'emby_item_id' -> 'emby_item_ids_json'...")
                
                # 这个查询非常安全：
                # 1. 只找旧字段有值的行
                # 2. 并且新字段还是空的（jsonb_array_length = 0），防止重复迁移
                cursor.execute("""
                    UPDATE media_metadata
                    SET emby_item_ids_json = jsonb_build_array(emby_item_id)
                    WHERE 
                        emby_item_id IS NOT NULL 
                        AND emby_item_id != ''
                        AND jsonb_array_length(emby_item_ids_json) = 0;
                """)
                
                migrated_count = cursor.rowcount
                if migrated_count > 0:
                    conn.commit()
                    logger.info(f"    ✅ [数据迁移] 成功将 {migrated_count} 条 'emby_item_id' 数据迁移到新格式。")
                else:
                    logger.trace("    ➜ 'emby_item_id' 数据无需迁移。")

                # --- 在这里可以添加未来的其他迁移任务 ---
                # logger.trace("  ➜ [数据迁移] 正在处理其他任务...")

        logger.info("  ✅ 数据库数据迁移检查完成。")

    except psycopg2.Error as e_pg:
        logger.error(f"数据迁移时发生 PostgreSQL 错误: {e_pg}", exc_info=True)
        # 发生错误时最好不要 raise，避免应用启动失败，但要记录严重错误
    except Exception as e_global:
        logger.error(f"数据迁移时发生未知错误: {e_global}", exc_info=True)