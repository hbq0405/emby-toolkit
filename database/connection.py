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

                logger.trace("  ➜ 正在创建 'collections_info' 表...")
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

                logger.trace("  ➜ 正在创建 'custom_collections' 表...")
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
                        keywords_json JSONB,
                        countries_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        release_date DATE,
                        date_added TIMESTAMP WITH TIME ZONE,
                        tags_json JSONB,
                        last_synced_at TIMESTAMP WITH TIME ZONE,
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
                        config_main_role_only BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_as_status ON actor_subscriptions (status)")

                logger.trace("  ➜ 正在创建 'tracked_actor_media' 表...")
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
                        ignore_reason TEXT,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        FOREIGN KEY(subscription_id) REFERENCES actor_subscriptions(id) ON DELETE CASCADE,
                        UNIQUE(subscription_id, tmdb_media_id)
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tam_subscription_id ON tracked_actor_media (subscription_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_tam_status ON tracked_actor_media (status)")

                logger.trace("  ➜ 正在创建 'resubscribe_rules' 表 (多规则洗版)...")
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

                logger.trace("  ➜ 正在创建 'resubscribe_cache' 表...")
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

                logger.trace("  ➜ 正在创建 'media_cleanup_tasks' 表...")
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
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
                        'tracked_actor_media': { 
                            "ignore_reason": "TEXT"
                        },
                        'media_metadata': {
                            "official_rating": "TEXT",
                            "unified_rating": "TEXT",
                            "emby_item_id": "TEXT",
                            "in_library": "BOOLEAN DEFAULT TRUE NOT NULL",
                            "emby_children_details_json": "JSONB",
                            "keywords_json": "JSONB"
                        },
                        'watchlist': {
                            "last_episode_to_air_json": "JSONB",
                            "resubscribe_info_json": "JSONB",
                            "is_airing": "BOOLEAN DEFAULT FALSE NOT NULL"
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
                            "generated_emby_ids_json": "JSONB DEFAULT '[]'::jsonb NOT NULL",
                            "allowed_user_ids": "JSONB" 
                        },
                        'user_templates': {
                            "source_emby_user_id": "TEXT",
                            "emby_configuration_json": "JSONB"
                        },
                        'emby_users_extended': {
                            "template_id": "INTEGER"
                        },
                        'actor_subscriptions': {
                            "config_main_role_only": "BOOLEAN NOT NULL DEFAULT FALSE"
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
                try:
                    logger.trace("  ➜ 正在为 'media_metadata.emby_item_id' 创建索引...")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_emby_item_id ON media_metadata (emby_item_id);")
                except Exception as e_index:
                    logger.error(f"  ➜ 创建 'emby_item_id' 索引时出错: {e_index}", exc_info=True)

                logger.trace("  ➜ 数据库升级检查完成。")

            conn.commit()
            logger.info("  ➜ PostgreSQL 数据库初始化完成，所有表结构已创建/验证。")

    except psycopg2.Error as e_pg:
        logger.error(f"数据库初始化时发生 PostgreSQL 错误: {e_pg}", exc_info=True)
        raise
    except Exception as e_global:
        logger.error(f"数据库初始化时发生未知错误: {e_global}", exc_info=True)
        raise