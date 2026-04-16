# database/connection.py
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging

import config_manager
import constants

logger = logging.getLogger(__name__)


# ======================================================================
# 模块: 中央数据访问 (线程安全连接池版)
# ======================================================================

_db_pool = None

def _init_pool():
    global _db_pool
    if _db_pool is None:
        try:
            cfg = config_manager.APP_CONFIG
            # 初始化连接池 (最小1个，最大50个并发连接)
            _db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=50,
                host=cfg.get(constants.CONFIG_OPTION_DB_HOST),
                port=cfg.get(constants.CONFIG_OPTION_DB_PORT),
                user=cfg.get(constants.CONFIG_OPTION_DB_USER),
                password=cfg.get(constants.CONFIG_OPTION_DB_PASSWORD),
                dbname=cfg.get(constants.CONFIG_OPTION_DB_NAME)
            )
            logger.info("  ➜ PostgreSQL 线程安全连接池初始化成功 (Max: 50)")
        except Exception as e:
            logger.error(f"  ➜ 初始化数据库连接池失败: {e}", exc_info=True)
            raise

@contextmanager
def get_db_connection():
    """
    【中央函数】获取一个配置好 RealDictCursor 的 PostgreSQL 数据库连接。
    使用上下文管理器，确保高并发下连接用完瞬间回收，绝不泄漏。
    """
    global _db_pool
    if _db_pool is None:
        _init_pool()
        
    # 从连接池借出一个连接
    conn = _db_pool.getconn()
    try:
        conn.cursor_factory = RealDictCursor
        yield conn
    except Exception:
        # 如果发生异常，回滚未提交的事务，保证归还给池子的连接是干净的
        conn.rollback()
        raise
    finally:
        # ★ 核心：无论成功失败，用完立刻归还给连接池
        _db_pool.putconn(conn)

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
                        id TEXT PRIMARY KEY, 
                        name TEXT NOT NULL, 
                        is_administrator BOOLEAN,
                        last_seen_at TIMESTAMP WITH TIME ZONE, 
                        profile_image_tag TEXT,
                        policy_json JSONB, 
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
                        poster_path TEXT,
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
                        item_type TEXT,
                        in_library_count INTEGER DEFAULT 0,
                        generated_media_info_json JSONB,
                        sort_order INTEGER NOT NULL DEFAULT 0
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
                        file_sha1_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        file_pickcode_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        date_added TIMESTAMP WITH TIME ZONE,
                        asset_details_json JSONB,

                        -- 订阅与状态管理
                        subscription_status TEXT NOT NULL DEFAULT 'NONE', -- 'NONE', 'WANTED', 'SUBSCRIBED', 'IGNORED', 'PENDING_RELEASE', 'REQUESTED', 'PAUSED'
                        subscription_sources_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                        first_requested_at TIMESTAMP WITH TIME ZONE,
                        last_subscribed_at TIMESTAMP WITH TIME ZONE,

                        -- 核心与扩展元数据
                        imdb_id TEXT,
                        title TEXT,
                        original_title TEXT,
                        original_language TEXT,
                        overview TEXT,
                        tagline TEXT,
                        overview_embedding JSONB,
                        release_date DATE,
                        release_year INTEGER,
                        last_air_date DATE,
                        poster_path TEXT,
                        backdrop_path TEXT, 
                        homepage TEXT,
                        runtime_minutes INTEGER,
                        rating REAL,
                        official_rating_json JSONB,
                        custom_rating TEXT,
                        genres_json JSONB,
                        actors_json JSONB,
                        directors_json JSONB,
                        production_companies_json JSONB, 
                        networks_json JSONB,
                        countries_json JSONB,
                        keywords_json JSONB,
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        ignore_reason TEXT,
                        tags_json JSONB,

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
                        waiting_for_completed_pack BOOLEAN DEFAULT FALSE,
                        active_washing BOOLEAN DEFAULT FALSE,

                        -- 内部管理字段
                        last_synced_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        -- 主键
                        PRIMARY KEY (tmdb_id, item_type)
                    )
                """)

                logger.trace("  ➜ 正在创建 'person_metadata' 表...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS person_metadata (
                        -- 核心身份标识 (建议以 TMDb ID 为主键，因为影视刮削高度依赖 TMDb)
                        tmdb_person_id INTEGER PRIMARY KEY, 
                        
                        -- 其他平台的 ID 映射
                        imdb_id TEXT UNIQUE, 
                        douban_celebrity_id TEXT UNIQUE,
                        
                        -- 基础元数据
                        primary_name TEXT NOT NULL,       -- 主要译名 (原 map 表字段)
                        original_name TEXT,               -- 原名 (原 metadata 表字段)
                        profile_path TEXT,                -- 头像路径
                        gender INTEGER,                   -- 性别
                        adult BOOLEAN,                    -- 是否成人影星
                        popularity REAL,                  -- 热度
                        
                        -- 时间戳
                        last_synced_at TIMESTAMP WITH TIME ZONE, 
                        last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
                        scope_rules JSONB DEFAULT '[]'::jsonb,
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
                        resubscribe_subtitle_effect_only BOOLEAN DEFAULT FALSE,
                        resubscribe_filesize_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_filesize_operator TEXT DEFAULT 'lt', 
                        resubscribe_filesize_threshold_gb REAL DEFAULT 10.0,
                        resubscribe_codec_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_codec_include JSONB,
                        resubscribe_subtitle_skip_if_audio_exists BOOLEAN DEFAULT FALSE,
                        custom_resubscribe_enabled BOOLEAN DEFAULT FALSE, 
                        consistency_check_enabled BOOLEAN DEFAULT FALSE,
                        consistency_must_match_resolution BOOLEAN DEFAULT FALSE,
                        consistency_must_match_group BOOLEAN DEFAULT FALSE,
                        consistency_must_match_code BOOLEAN DEFAULT FALSE,
                        consistency_must_match_codec BOOLEAN DEFAULT FALSE,
                        rule_type TEXT DEFAULT 'resubscribe',           
                        filter_rating_enabled BOOLEAN DEFAULT FALSE, 
                        filter_rating_min REAL DEFAULT 0,  
                        delete_mode TEXT DEFAULT 'episode',          
                        delete_delay_seconds INTEGER DEFAULT 0,
                        filter_rating_ignore_zero BOOLEAN DEFAULT FALSE,
                        filter_missing_episodes_enabled BOOLEAN DEFAULT FALSE,
                        resubscribe_source TEXT DEFAULT 'moviepilot',
                        resubscribe_entire_season BOOLEAN DEFAULT FALSE,
                        inbound_quality_check_enabled BOOLEAN DEFAULT FALSE,
                        inbound_abort_on_probe_fail BOOLEAN DEFAULT FALSE
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
                        best_version_json JSONB,
                        
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

                logger.trace("  ➜ 正在创建 'p115_filesystem_cache' 表 (目录树缓存)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS p115_filesystem_cache (
                        id TEXT PRIMARY KEY,           -- 115 的 cid (文件夹) 或 fid (文件)
                        parent_id TEXT NOT NULL,       -- 父目录 ID (根目录为 '0')
                        name TEXT NOT NULL,            -- 文件/文件夹名称
                        local_path TEXT,               -- 本地映射路径 (如果已同步到本地)
                        sha1 TEXT,
                        pick_code TEXT,
                        size BIGINT DEFAULT 0,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- 最后同步时间
                        
                        -- 复合唯一约束：同一个父目录下不能有同名文件 (用于快速查找)
                        -- 注意：115 实际上允许同名，但在我们的管理逻辑中通常假设唯一，或者只缓存最新的
                        CONSTRAINT uniq_p115_parent_name UNIQUE (parent_id, name)
                    )
                """)

                logger.trace("  ➜ 正在创建 'p115_mediainfo_cache' 表 (独立媒体信息指纹库)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS p115_mediainfo_cache (
                        sha1 TEXT PRIMARY KEY,
                        mediainfo_json JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        hit_count INTEGER DEFAULT 0
                    )
                """)

                logger.trace("  ➜ 正在创建 'p115_organize_records' 表 (115整理记录)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS p115_organize_records (
                        id SERIAL PRIMARY KEY,
                        file_id TEXT UNIQUE NOT NULL,  -- 115 原始文件/文件夹ID (使用UNIQUE防止重复记录)
                        pick_code TEXT UNIQUE,
                        original_name TEXT NOT NULL,   -- 原始名称
                        renamed_name TEXT,             -- 整理后的名称
                        status TEXT NOT NULL,          -- 'success' 或 'unrecognized'
                        fail_reason TEXT,              -- 识别失败原因
                        tmdb_id TEXT,
                        media_type TEXT,
                        target_cid TEXT,               -- 目标分类CID
                        category_name TEXT,            -- 目标分类名称
                        processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        is_center_cached BOOLEAN DEFAULT FALSE,
                        season_number INTEGER
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
                        'p115_filesystem_cache': {
                            "local_path": "TEXT",
                            "sha1": "TEXT",
                            "pick_code": "TEXT",
                            "size": "BIGINT DEFAULT 0"
                        },
                        'p115_organize_records': {
                            "is_center_cached": "BOOLEAN DEFAULT FALSE",
                            "pick_code": "TEXT UNIQUE",
                            "season_number": "INTEGER",
                            "fail_reason": "TEXT"
                        },
                        'emby_users': {
                            "policy_json": "JSONB"  
                        },
                        'cleanup_index': {
                            "best_version_json": "JSONB"
                        },
                        'media_metadata': {
                            "imdb_id": "TEXT",
                            "tagline": "TEXT"
                        },
                        'resubscribe_rules': {
                            "filter_missing_episodes_enabled": "BOOLEAN DEFAULT FALSE",
                            "resubscribe_source": "TEXT DEFAULT 'moviepilot'", 
                            "resubscribe_entire_season": "BOOLEAN DEFAULT FALSE",
                            "inbound_quality_check_enabled": "BOOLEAN DEFAULT FALSE",
                            "inbound_abort_on_probe_fail": "BOOLEAN DEFAULT FALSE"
                        },
                        'collections_info': {
                            "poster_path": "TEXT",
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
                # ★★★ 存量数据清洗：为旧的 115 整理记录提取并写入季号 ★★★
                # ======================================================================
                try:
                    cursor.execute("SELECT id, original_name, renamed_name FROM p115_organize_records WHERE media_type = 'tv' AND season_number IS NULL")
                    records_to_upgrade = cursor.fetchall()
                    
                    if records_to_upgrade:
                        logger.info(f"  ➜ [数据清洗] 发现 {len(records_to_upgrade)} 条旧的 115 整理记录缺少季号，正在执行正则提取与升级...")
                        import re
                        update_data = []
                        for row in records_to_upgrade:
                            name_to_check = row['renamed_name'] or row['original_name'] or ""
                            s_num = None
                            
                            m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', name_to_check)
                            m2 = re.search(r'Season\s*(\d{1,4})\b', name_to_check, re.IGNORECASE)
                            m3 = re.search(r'第(\d{1,4})季', name_to_check)

                            if m1: s_num = int(m1.group(1))
                            elif m2: s_num = int(m2.group(1))
                            elif m3: s_num = int(m3.group(1))
                            
                            if s_num is not None:
                                update_data.append((s_num, row['id']))
                        
                        if update_data:
                            from psycopg2.extras import execute_batch
                            execute_batch(cursor, "UPDATE p115_organize_records SET season_number = %s WHERE id = %s", update_data)
                            logger.info(f"  ➜ [数据清洗] 成功为 {len(update_data)} 条记录补充了季号！")
                except Exception as e_clean:
                    logger.error(f"  ➜ [数据清洗] 提取季号时出错: {e_clean}")
                
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

                    # 7. 【权限系统核心】加速 ancestor_ids 和 source_library_id 的检索 (GIN 索引)
                    # 这是解决“灰块”和“权限拦截”性能的关键
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_asset_details_gin ON media_metadata USING GIN(asset_details_json);")

                    # 8. 【JSONB 数组筛选】加速 类型、标签、国家、制片厂、关键词的查询
                    # 使用 GIN 索引配合 jsonb_path_ops，查询速度比普通 GIN 更快
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_genres_gin ON media_metadata USING GIN(genres_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_tags_gin ON media_metadata USING GIN(tags_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_countries_gin ON media_metadata USING GIN(countries_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_companies_gin ON media_metadata USING GIN(production_companies_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_networks_gin ON media_metadata USING GIN(networks_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_keywords_gin ON media_metadata USING GIN(keywords_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_asset_details_gin ON media_metadata USING GIN(asset_details_json);")

                    # 9. 【复杂对象筛选】加速 导演 和 演员 的 ID 匹配 (@> 运算符)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_directors_gin ON media_metadata USING GIN(directors_json jsonb_path_ops);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_actors_gin ON media_metadata USING GIN(actors_json jsonb_path_ops);")

                    # 10. 【排序优化】加速海报墙的各种常用排序方式
                    # 加上 DESC 优化，因为大部分海报墙都是“从新到旧”
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_date_added_desc ON media_metadata (date_added DESC);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_rating_desc ON media_metadata (rating DESC);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_release_date_desc ON media_metadata (release_date DESC);")

                    # 11. 【跟播系统】加速“正在连载”剧集的筛选
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_watchlist_airing ON media_metadata (watchlist_is_airing) WHERE item_type = 'Series';")

                    # 12. 【115 目录缓存】加速本地目录树查找
                    # 加速 "列出某目录下的所有文件"
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_p115_parent_id ON p115_filesystem_cache (parent_id);")
                    # 加速 "全局搜索某个文件"
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_p115_name ON p115_filesystem_cache (name);")

                    # 13. 【海量数据优化】加速追剧列表的聚合查询
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_type_parent ON media_metadata (item_type, parent_series_tmdb_id);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mm_watching_status ON media_metadata (watching_status) WHERE watching_status != 'NONE';")

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
                        'tracked_actor_media'
                    ]
                    for table in deprecated_tables:
                        logger.trace(f"    ➜ [数据库清理] 正在尝试移除废弃的表: '{table}'...")
                        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                        logger.trace(f"    ➜ [数据库清理] 移除 '{table}' 表的操作已执行。")

                    # ★★★ 核心修复：使用字典来管理多个表的废弃列 ★★★
                    deprecated_columns_map = {
                        'media_metadata': [
                            'emby_item_id',
                            'tvdb_id'
                        ],
                        'cleanup_index': [
                            'best_version_id'
                        ],
                        'collections_info': [
                            'status'
                        ],
                        'resubscribe_rules': [
                            'target_library_ids',
                            'delete_after_resubscribe',
                            'auto_resubscribe'
                        ],
                        'custom_collections': [
                            'missing_count'
                        ],
                        'person_metadata': [
                            'emby_person_id'
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