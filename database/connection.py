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
            cursor_factory=RealDictCursor
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

                logger.trace("  ➜ 正在创建 'collections_info' 表 ...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS collections_info (
                        emby_collection_id TEXT PRIMARY KEY,
                        name TEXT,
                        tmdb_collection_id TEXT,
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        item_type TEXT DEFAULT 'Movie' NOT NULL,
                        all_tmdb_ids_json JSONB
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

                logger.trace("  ➜ 正在创建 'media_metadata' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS media_metadata (
                        -- 核心标识符
                        tmdb_id TEXT NOT NULL,
                        item_type TEXT NOT NULL, -- 'Movie', 'Series', 'Season', 'Episode'

                        -- 媒体库状态
                        in_library BOOLEAN DEFAULT FALSE NOT NULL,
                        emby_item_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        date_added TIMESTAMP WITH TIME ZONE,
                        asset_details_json JSONB,

                        -- 订阅与状态管理
                        subscription_status TEXT NOT NULL DEFAULT 'NONE', -- 'NONE', 'WANTED', 'SUBSCRIBED', 'IGNORED', 'PENDING_RELEASE', 'REQUESTED', 'PAUSED'
                        subscription_sources_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        first_requested_at TIMESTAMP WITH TIME ZONE,
                        last_subscribed_at TIMESTAMP WITH TIME ZONE,

                        -- 核心与扩展元数据
                        title TEXT,
                        original_title TEXT,
                        original_language TEXT,
                        overview TEXT,
                        overview_embedding JSONB,
                        release_date DATE,
                        release_year INTEGER,
                        poster_path TEXT,
                        runtime_minutes INTEGER,
                        rating REAL,
                        official_rating TEXT,
                        genres_json JSONB,
                        actors_json JSONB,
                        directors_json JSONB,
                        studios_json JSONB,
                        countries_json JSONB,
                        keywords_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        ignore_reason TEXT,
                        pre_cached_tags_json JSONB,

                        -- 剧集专属与层级数据
                        parent_series_tmdb_id TEXT,
                        season_number INTEGER,
                        episode_number INTEGER,
                               
                        -- 追剧专属字段
                        watching_status TEXT DEFAULT 'NONE', -- 'NONE', 'Watching', 'Paused', 'Completed', 'Pending'
                        paused_until DATE,
                        force_ended BOOLEAN DEFAULT FALSE,
                        watchlist_last_checked_at TIMESTAMP WITH TIME ZONE,
                        watchlist_tmdb_status TEXT,
                        watchlist_next_episode_json JSONB,
                        watchlist_missing_info_json JSONB,
                        watchlist_is_airing BOOLEAN DEFAULT FALSE,
                        last_episode_to_air_json JSONB,
                        total_episodes INTEGER DEFAULT 0,
                        total_episodes_locked BOOLEAN DEFAULT FALSE,

                        -- 内部管理字段
                        last_synced_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        -- 主键
                        PRIMARY KEY (tmdb_id, item_type)
                    )
                """)

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
                        last_scanned_tmdb_ids_json JSONB,
                        last_checked_at TIMESTAMP WITH TIME ZONE,
                        added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        config_min_rating REAL DEFAULT 6.0,
                        config_main_role_only BOOLEAN NOT NULL DEFAULT FALSE,
                        config_min_vote_count INTEGER NOT NULL DEFAULT 10
                    )
                """)

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
                        resubscribe_filesize_threshold_gb REAL DEFAULT 10.0,
                        resubscribe_codec_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_codec_include JSONB,
                        resubscribe_subtitle_skip_if_audio_exists BOOLEAN DEFAULT FALSE,
                        auto_resubscribe BOOLEAN DEFAULT FALSE, 
                        custom_resubscribe_enabled BOOLEAN DEFAULT FALSE, 
                        consistency_check_enabled BOOLEAN DEFAULT FALSE,
                        consistency_must_match_resolution BOOLEAN DEFAULT FALSE,
                        consistency_must_match_group BOOLEAN DEFAULT FALSE,
                        consistency_must_match_code BOOLEAN DEFAULT FALSE,
                        rule_type TEXT DEFAULT 'resubscribe',           
                        filter_rating_enabled BOOLEAN DEFAULT FALSE, 
                        filter_rating_min REAL DEFAULT 0,  
                        delete_mode TEXT DEFAULT 'episode',          
                        delete_delay_seconds INTEGER DEFAULT 0,
                        filter_rating_ignore_zero BOOLEAN DEFAULT FALSE,
                        filter_missing_episodes_enabled BOOLEAN DEFAULT FALSE         
                    )
                """)

                logger.trace("  ➜ 正在创建 'resubscribe_index' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS resubscribe_index (
                        tmdb_id TEXT NOT NULL,
                        item_type TEXT NOT NULL,
                        season_number INTEGER NOT NULL DEFAULT -1, -- 对于电影，我们将使用-1作为占位符

                        status TEXT NOT NULL,
                        reason TEXT,
                        matched_rule_id INTEGER,
                        last_checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        -- 主键保持不变，但现在 season_number 有了默认值，不会再插入 NULL
                        PRIMARY KEY (tmdb_id, item_type, season_number)
                    )
                """)

                logger.trace("  ➜ 正在创建 'cleanup_index' 表 ...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cleanup_index (
                        id SERIAL PRIMARY KEY,
                        
                        -- 核心指针：指向 media_metadata 表的复合主键
                        tmdb_id TEXT NOT NULL,
                        item_type TEXT NOT NULL,

                        -- 任务元数据
                        status TEXT DEFAULT 'pending', -- 'pending', 'processed', 'ignored'
                        
                        -- 决策结果
                        versions_info_json JSONB,
                        best_version_id TEXT,
                        
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        -- 复合唯一约束，确保每个媒体项只有一个待办任务
                        UNIQUE (tmdb_id, item_type)
                    )
                """)

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

                logger.trace("  ➜ 正在创建 'emby_users_extended' 表 (用户扩展信息)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS emby_users_extended (
                        emby_user_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'active', -- active(激活), expired(过期), disabled(禁用)
                        registration_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        expiration_date TIMESTAMP WITH TIME ZONE, -- 核心字段：用户的到期时间
                        notes TEXT,
                        created_by TEXT DEFAULT 'self-registered', -- 'self-registered' 或 'admin'
                        template_id INTEGER,
                        FOREIGN KEY(emby_user_id) REFERENCES emby_users(id) ON DELETE CASCADE,
                        FOREIGN KEY(template_id) REFERENCES user_templates(id) ON DELETE SET NULL
                    )
                """)

                # ======================================================================
                # ★★★ 数据库平滑升级 (START) ★★★
                # 此处代码用于新增在新版本中添加的列。
                # ======================================================================
                logger.trace("  ➜ 开始执行数据库表结构升级检查...")
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
                            "original_language": "TEXT",
                            "asset_details_json": "JSONB",
                            "last_updated_at": "TIMESTAMP WITH TIME ZONE",
                            "overview": "TEXT",
                            "overview_embedding": "JSONB",
                            "official_rating": "TEXT",
                            "unified_rating": "TEXT",
                            "keywords_json": "JSONB",
                            "in_library": "BOOLEAN DEFAULT FALSE NOT NULL",
                            "emby_item_ids_json": "JSONB NOT NULL DEFAULT '[]'::jsonb",
                            "subscription_status": "TEXT NOT NULL DEFAULT 'NONE'",
                            "subscription_sources_json": "JSONB NOT NULL DEFAULT '[]'::jsonb",
                            "first_requested_at": "TIMESTAMP WITH TIME ZONE",
                            "last_subscribed_at": "TIMESTAMP WITH TIME ZONE",
                            "created_at": "TIMESTAMP WITH TIME ZONE",
                            "pre_cached_tags_json": "JSONB",
                            "poster_path": "TEXT",
                            "runtime_minutes": "INTEGER",
                            "last_episode_to_air_json": "JSONB",
                            "parent_series_tmdb_id": "TEXT",
                            "season_number": "INTEGER",
                            "episode_number": "INTEGER",
                            "ignore_reason": "TEXT",
                            "watching_status": "TEXT DEFAULT 'NONE'",
                            "paused_until": "DATE",
                            "force_ended": "BOOLEAN DEFAULT FALSE",
                            "watchlist_last_checked_at": "TIMESTAMP WITH TIME ZONE",
                            "watchlist_tmdb_status": "TEXT",
                            "watchlist_next_episode_json": "JSONB",
                            "watchlist_missing_info_json": "JSONB",
                            "watchlist_is_airing": "BOOLEAN DEFAULT FALSE",
                            "total_episodes": "INTEGER DEFAULT 0",
                            "total_episodes_locked": "BOOLEAN DEFAULT FALSE"
                        },
                        'resubscribe_rules': {
                            "resubscribe_subtitle_effect_only": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_filesize_enabled": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_filesize_operator": "TEXT DEFAULT 'lt'",
                            "resubscribe_filesize_threshold_gb": "REAL DEFAULT 10.0",
                            "resubscribe_codec_enabled": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_codec_include": "JSONB",
                            "resubscribe_subtitle_skip_if_audio_exists": "BOOLEAN DEFAULT FALSE",
                            "auto_resubscribe": "BOOLEAN DEFAULT FALSE",
                            "custom_resubscribe_enabled": "BOOLEAN DEFAULT FALSE",
                            "consistency_check_enabled": "BOOLEAN DEFAULT FALSE",
                            "consistency_must_match_resolution": "BOOLEAN DEFAULT FALSE", 
                            "consistency_must_match_group": "BOOLEAN DEFAULT FALSE",
                            "consistency_must_match_codec": "BOOLEAN DEFAULT FALSE",
                            "rule_type": "TEXT DEFAULT 'resubscribe'",           
                            "filter_rating_enabled": "BOOLEAN DEFAULT FALSE",    
                            "filter_rating_min": "REAL DEFAULT 0",               
                            "delete_mode": "TEXT DEFAULT 'episode'",             
                            "delete_delay_seconds": "INTEGER DEFAULT 0",  
                            "filter_rating_ignore_zero": "BOOLEAN DEFAULT FALSE",
                            "filter_missing_episodes_enabled": "BOOLEAN DEFAULT FALSE"
                        },
                        'collections_info': {
                            "all_tmdb_ids_json": "JSONB" 
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
                            "config_min_vote_count": "INTEGER NOT NULL DEFAULT 10",
                            "last_scanned_tmdb_ids_json": "JSONB"
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
                
                # ======================================================================
                # ★★★ 修复：用户状态默认值修正补丁 ★★★
                # ======================================================================
                try:
                    # 1. 修改列的默认值定义
                    cursor.execute("ALTER TABLE emby_users_extended ALTER COLUMN status SET DEFAULT 'active'")
                    
                    # 2. 将现有的 'pending' 状态全部刷为 'active'
                    cursor.execute("UPDATE emby_users_extended SET status = 'active' WHERE status = 'pending'")
                    
                    # 3. 确保 registration_date 列存在 (虽然之前应该有了，但为了保险)
                    # Emby API 返回的是 DateCreated，我们需要存入 registration_date
                except Exception as e_fix_status:
                    logger.trace(f"  ➜ [数据库修正] 尝试修改用户默认状态失败 (可能是已修改): {e_fix_status}")

                # ======================================================================
                # ★★★ 统一创建验证所有索引 ★★★
                # 此处代码用于集中创建所有表需要的索引。
                # ======================================================================
                logger.trace("  ➜ 正在创建/验证所有索引...")
                try:
                    # 1. 【核心状态】用于快速筛选“库内存在”和“不在库”的项目
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_in_library ON media_metadata (in_library);")
                    
                    # 2. 【排序与筛选】用于海报墙按年份排序
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_release_year ON media_metadata (release_year);")
                    
                    # 3. 【层级关系】查找某部剧的所有季和集 (非常重要！)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_parent_series ON media_metadata (parent_series_tmdb_id);")
                    
                    # 4. 【订阅系统】查找“想看”或“待发布”的项目
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_subscription_status ON media_metadata (subscription_status) WHERE in_library = FALSE;")
                    
                    # 5. 【JSON加速】用于根据 Emby ID 反查 TMDb ID (GIN 索引)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_emby_ids_gin ON media_metadata USING GIN(emby_item_ids_json);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_subscription_sources_gin ON media_metadata USING GIN(subscription_sources_json);")

                    # 6. 【洗版/性能优化】(这是我们刚加的“增肌”部分)
                    # 加速 "查找某部剧的第几季" (JOIN 优化)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_parent_series_season ON media_metadata (parent_series_tmdb_id, season_number);")
                    # 加速 "只看电影" 或 "只看剧集" 的筛选
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_item_type ON media_metadata (item_type);")
                    # 加速 resubscribe_index 表的查询
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ri_tmdb_type ON resubscribe_index (tmdb_id, item_type);")

                except Exception as e_index:
                    logger.error(f"  ➜ 创建索引时出错: {e_index}", exc_info=True)
                logger.trace("  ➜ 数据库升级检查完成。")

                # ======================================================================
                # ★★★ 数据库废弃对象清理补丁 (START) ★★★
                # 此处代码用于移除在新版本中已废弃的表和列，保持数据库整洁。
                # ======================================================================
                logger.trace("  ➜ [数据库清理] 正在检查并移除已废弃的数据库对象...")
                try:
                    # --- 3.1 清理废弃的表 ---
                    deprecated_tables = [
                        'watchlist',
                        'tracked_actor_media',
                        'subscription_requests',
                        'media_cleanup_tasks',
                        'resubscribe_cache',
                        'users'
                    ]
                    for table in deprecated_tables:
                        logger.trace(f"    ➜ [数据库清理] 正在尝试移除废弃的表: '{table}'...")
                        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                        logger.trace(f"    ➜ [数据库清理] 移除 '{table}' 表的操作已执行。")

                    # ★★★ 核心修复：使用字典来管理多个表的废弃列 ★★★
                    deprecated_columns_map = {
                        'media_metadata': [
                            'emby_item_id',
                            'emby_children_details_json',
                            'tags_json',
                            'paths_json',
                            'backdrop_path',
                            'vote_count',
                            'popularity',
                            'imdb_id',
                            'tvdb_id',
                            'pre_processed_at',
                            'pre_cached_extra_json',
                            'translated_title',
                            'translated_overview',
                            'tmdb_status',
                            'next_episode_to_air_json',
                            'is_airing',
                            'total_seasons'
                        ],
                        'collections_info': [
                            'status', 
                            'has_missing', 
                            'missing_movies_json', 
                            'in_library_count'
                        ],
                        'custom_collections': [
                            'generated_emby_ids_json' # <-- 在这里添加了废弃的列！
                        ]
                    }

                    for table_name, columns_to_drop in deprecated_columns_map.items():
                        for column_name in columns_to_drop:
                            logger.trace(f"    ➜ [数据库清理] 正在尝试从 '{table_name}' 表中移除废弃的列: '{column_name}'...")
                            cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name};")
                            logger.trace(f"    ➜ [数据库清理] 移除 '{table_name}.{column_name}' 列的操作已执行。")

                    logger.trace("  ➜ [数据库清理] 废弃对象清理完成。")

                except Exception as e_cleanup:
                    logger.error(f"  ➜ [数据库清理] 清理废弃对象时发生错误: {e_cleanup}", exc_info=True)
                # ======================================================================

            conn.commit()
            logger.info("  ➜ PostgreSQL 数据库初始化完成，所有表结构已创建/验证。")

    except psycopg2.Error as e_pg:
        logger.error(f"数据库初始化时发生 PostgreSQL 错误: {e_pg}", exc_info=True)
        raise
    except Exception as e_global:
        logger.error(f"数据库初始化时发生未知错误: {e_global}", exc_info=True)
        raise