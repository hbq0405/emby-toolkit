# tasks/maintenance.py
# 维护性任务模块：数据库导入

import json
import logging
from typing import List, Dict, Any

# 导入需要的底层模块和共享实例
from database import connection, maintenance_db
from psycopg2 import sql
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)

# --- 辅助函数 1: 数据清洗与准备 ---
def _prepare_data_for_insert(table_name: str, table_data: List[Dict[str, Any]]) -> tuple[List[str], List[tuple]]:
    """
    【V2 - 健壮性修复版】一个更强大的数据准备函数。
    - 核心功能：将需要存入 JSONB 列的数据包装成 psycopg2 的 Json 对象。
    - 新增健壮性：如果一个非 JSONB 列意外地收到了字典或列表，
      它会自动将其转换为 JSON 字符串，而不是让程序崩溃。
    """
    JSONB_COLUMNS = {
        'app_settings': {'value_json'},
        'cleanup_index': {'versions_info_json', 'best_version_json', 'additional_info_json'},
        'translation_cache': {'translated_text_json'},
        'collections_info': {'all_tmdb_ids_json'},
        'custom_collections': {'definition_json', 'allowed_user_ids', 'generated_media_info_json'},
        'emby_users': {'policy_json'},
        'media_metadata': {
            'emby_item_ids_json', 'file_sha1_json', 'file_pickcode_json', 'subscription_sources_json', 
            'tags_json', 'genres_json', 'official_rating_json', 
            'actors_json', 'directors_json', 'production_companies_json', 'networks_json', 'countries_json', 
            'keywords_json', 'last_episode_to_air_json',
            'watchlist_next_episode_json', 'watchlist_missing_info_json', 'asset_details_json',
            'overview_embedding'
        },
        'actor_subscriptions': {'config_genres_include_json', 'config_genres_exclude_json', 'last_scanned_tmdb_ids_json'},
        'resubscribe_rules': {
            'scope_rules', 'resubscribe_audio_missing_languages',
            'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
            'resubscribe_effect_include', 'resubscribe_codec_include'
        },
        'user_templates': {'emby_policy_json', 'emby_configuration_json'},
        'p115_mediainfo_cache': {'mediainfo_json', 'raw_ffprobe_json'},
        'washing_priority_groups': {'target_cids', 'priorities'},
        # 共享资源相关表：这些表大量使用 raw_json 保存接口回包/任务上下文，
        # 需要显式按 JSONB 处理，否则导入 PostgreSQL 时会把 dict/list 当普通字符串插入失败。
        'shared_virtual_items': {'raw_json'},
        'shared_share_records': {'raw_json'},
        'shared_share_items': {'raw_json'},
        'shared_credit_snapshot': {'raw_json'},
        'shared_credit_ledger_local': {'raw_json'},
        'shared_maintenance_state': {'raw_json'},
    }

    LIST_TO_STRING_COLUMNS = {
        'actor_subscriptions': {'config_media_types'}
    }

    if not table_data:
        return [], []

    columns = list(table_data[0].keys())
    table_json_rules = JSONB_COLUMNS.get(table_name.lower(), set())
    table_list_to_string_rules = LIST_TO_STRING_COLUMNS.get(table_name.lower(), set())
    
    prepared_rows = []
    for row_dict in table_data:
        row_values = []
        for col_name in columns:
            value = row_dict.get(col_name)
            
            if col_name in table_json_rules and value is not None:
                # 1. 如果是指定的 JSONB 列，使用 Json() 包装器
                value = Json(value)
            elif col_name in table_list_to_string_rules and isinstance(value, list):
                # 2. 如果是指定的需要转为字符串的列表列
                value = ','.join(map(str, value))
            # ★★★ 核心修复：在这里添加对意外字典/列表的处理 ★★★
            elif isinstance(value, (dict, list)):
                # 3. 如果它不是指定的 JSONB 列，但值依然是字典或列表
                #    这通常意味着数据不一致。我们发出警告，并将其序列化为字符串以避免崩溃。
                logger.warning(
                    f"  ➜ [数据清洗] 在表 '{table_name}' 的非JSONB列 '{col_name}' "
                    f"中发现了一个字典/列表类型的值。已自动将其转换为JSON字符串。"
                )
                value = json.dumps(value, ensure_ascii=False)
            
            row_values.append(value)
        prepared_rows.append(tuple(row_values))
        
    return columns, prepared_rows

# --- 辅助函数 2: 数据库覆盖操作 (保持不变，但现在更可靠) ---
def _overwrite_table_data(cursor, table_name: str, columns: List[str], data: List[tuple]):
    """安全地清空并批量插入数据。"""
    db_table_name = table_name.lower()

    logger.warning(f"  ➜ 执行覆盖模式：将清空表 '{db_table_name}' 中的所有数据！")
    truncate_query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(
        table=sql.Identifier(db_table_name)
    )
    cursor.execute(truncate_query)

    insert_query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
        table=sql.Identifier(db_table_name),
        cols=sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    execute_values(cursor, insert_query, data, page_size=500)
    logger.info(f"  ➜ 成功向表 '{db_table_name}' 插入 {len(data)} 条记录。")

# ★★★ 辅助函数 3: 数据库共享导入操作 ★★★
def _share_import_table_data(cursor, table_name: str, columns: List[str], data: List[tuple]):
    """
    安全地合并数据，使用 ON CONFLICT DO NOTHING 策略。
    这会尝试插入新行，如果主键或唯一约束冲突，则静默忽略。
    """
    CONFLICT_TARGETS = {
        'person_metadata': 'tmdb_person_id',
        'translation_cache': 'original_text',
        'media_metadata': 'tmdb_id, item_type',
        'p115_mediainfo_cache': 'sha1',
        # 共享资源本地状态表默认不进入“共享导入”模式；这里补上冲突目标，
        # 防止后续扩展或误选共享模式时直接报 Conflict target not defined。
        'shared_virtual_items': 'virtual_id',
        'shared_share_records': 'share_code',
        'shared_share_items': 'share_record_id, fid',
        'shared_credit_snapshot': 'id',
        'shared_maintenance_state': 'task_name'
    }
    
    db_table_name = table_name.lower()
    conflict_target = CONFLICT_TARGETS.get(db_table_name)

    if not conflict_target:
        logger.error(f"  ➜ 共享导入失败：表 '{db_table_name}' 未定义冲突目标，无法执行合并操作。")
        raise ValueError(f"Conflict target not defined for table {db_table_name}")

    logger.info(f"  ➜ 执行共享模式：将合并数据到表 '{db_table_name}'，冲突项将被忽略。")
    
    insert_query = sql.SQL("""
        INSERT INTO {table} ({cols}) VALUES %s
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """).format(
        table=sql.Identifier(db_table_name),
        cols=sql.SQL(', ').join(map(sql.Identifier, columns)),
        conflict_cols=sql.SQL(', ').join(map(sql.Identifier, [c.strip() for c in conflict_target.split(',')]))
    )

    execute_values(cursor, insert_query, data, page_size=500)
    inserted_count = cursor.rowcount
    logger.info(f"  ➜ 成功向表 '{db_table_name}' 合并 {inserted_count} 条新记录（总共尝试 {len(data)} 条）。")
    return inserted_count


def _sanitize_shared_raw_ffprobe_json(raw_ffprobe_json):
    """
    共享导入 p115_mediainfo_cache 时只保留可复用的 ffprobe 原始结构。
    老版本 raw_ffprobe_json 可能带有 format.filename 里的 115 临时直链，导入前必须剔除。
    """
    if not raw_ffprobe_json:
        return None

    if isinstance(raw_ffprobe_json, str):
        try:
            raw_ffprobe_json = json.loads(raw_ffprobe_json)
        except Exception:
            return None

    if not isinstance(raw_ffprobe_json, dict):
        return None

    # 深拷贝，避免修改备份对象本身。
    cleaned = json.loads(json.dumps(raw_ffprobe_json, ensure_ascii=False))

    fmt = cleaned.get('format')
    if isinstance(fmt, dict):
        # 115cdn 直链是临时签名链接，既没共享价值，也不应该跨账号导入。
        fmt.pop('filename', None)

    return cleaned


def _merge_p115_mediainfo_raw_cache(cursor, columns: List[str], data: List[tuple]) -> int:
    """
    共享模式下专门合并 p115_mediainfo_cache。
    只导入 raw_ffprobe_json，不导入 mediainfo_json，避免把个人默认音轨/字幕偏好共享给别人。
    冲突时只补齐缺失 raw；如果本地 raw 没有 _etk 而导入 raw 有 _etk，则用导入 raw 补强身份信息。
    """
    if not data:
        return 0

    allowed_columns = {'sha1', 'raw_ffprobe_json'}
    if set(columns) != allowed_columns:
        raise ValueError("共享导入 p115_mediainfo_cache 只允许 sha1 与 raw_ffprobe_json 两列")

    insert_query = sql.SQL("""
        INSERT INTO p115_mediainfo_cache (sha1, raw_ffprobe_json, created_at, hit_count)
        VALUES %s
        ON CONFLICT (sha1) DO UPDATE SET
            raw_ffprobe_json = CASE
                WHEN p115_mediainfo_cache.raw_ffprobe_json IS NULL THEN EXCLUDED.raw_ffprobe_json
                WHEN NOT (p115_mediainfo_cache.raw_ffprobe_json ? '_etk')
                     AND (EXCLUDED.raw_ffprobe_json ? '_etk') THEN EXCLUDED.raw_ffprobe_json
                ELSE p115_mediainfo_cache.raw_ffprobe_json
            END
    """)

    # data 只有 sha1/raw 两列，这里补 created_at/hit_count 默认值。
    values = [(row[columns.index('sha1')], row[columns.index('raw_ffprobe_json')], 0) for row in data]
    execute_values(
        cursor,
        insert_query,
        values,
        template="(%s, %s, NOW(), %s)",
        page_size=500
    )
    affected_count = cursor.rowcount
    logger.info(
        f"  ➜ 成功向表 'p115_mediainfo_cache' 合并/补齐 {affected_count} 条 raw_ffprobe_json "
        f"（总共尝试 {len(data)} 条，mediainfo_json 已忽略）。"
    )
    return affected_count

# ★★★ 辅助函数 4: 专门用于合并 person_metadata 的智能函数 ★★★
def _merge_person_metadata_data(cursor, table_name: str, columns: List[str], data: List[tuple]) -> dict:
    """
    【重构版】为 person_metadata 表提供一个健壮的合并策略。
    """
    logger.info(f"  ➜ 执行智能合并模式：将合并数据到表 '{table_name}'...")
    
    stats = {'inserted': 0, 'updated': 0, 'merged_and_deleted': 0}
    data_dicts = [dict(zip(columns, row)) for row in data]

    for row_to_merge in data_dicts:
        tmdb_id = row_to_merge.get('tmdb_person_id')
        if not tmdb_id:
            continue

        # 1. 检查是否存在
        cursor.execute("SELECT * FROM person_metadata WHERE tmdb_person_id = %s", (tmdb_id,))
        existing = cursor.fetchone()

        if not existing:
            # 插入前，检查唯一键冲突 (imdb_id, douban_celebrity_id)
            for unique_col in ['imdb_id', 'douban_celebrity_id']:
                val = row_to_merge.get(unique_col)
                if val:
                    cursor.execute(sql.SQL("SELECT tmdb_person_id FROM person_metadata WHERE {} = %s").format(sql.Identifier(unique_col)), (val,))
                    if cursor.fetchone():
                        row_to_merge[unique_col] = None # 发生冲突，置空以保证主数据能插入

            cols = [c for c in columns]
            vals = [row_to_merge.get(c) for c in cols]
            insert_sql = sql.SQL("INSERT INTO person_metadata ({}) VALUES ({})").format(
                sql.SQL(', ').join(map(sql.Identifier, cols)),
                sql.SQL(', ').join(sql.Placeholder() * len(cols))
            )
            cursor.execute(insert_sql, vals)
            stats['inserted'] += 1
        else:
            # 2. 如果存在，则用新数据填补空缺 (COALESCE 逻辑)
            updates = {}
            for col in columns:
                if col == 'tmdb_person_id': continue
                new_val = row_to_merge.get(col)
                old_val = existing[col]
                
                if new_val and not old_val:
                    # 同样需要检查唯一键冲突
                    if col in ['imdb_id', 'douban_celebrity_id']:
                        cursor.execute(sql.SQL("SELECT tmdb_person_id FROM person_metadata WHERE {} = %s").format(sql.Identifier(col)), (new_val,))
                        if cursor.fetchone():
                            continue # 冲突，跳过更新此字段
                    updates[col] = new_val

            if updates:
                set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in updates.keys()]
                update_sql = sql.SQL("UPDATE person_metadata SET {} WHERE tmdb_person_id = %s").format(sql.SQL(', ').join(set_clauses))
                cursor.execute(update_sql, tuple(updates.values()) + (tmdb_id,))
                stats['updated'] += 1
            
    logger.info(f"  ➜ 智能合并完成：新增 {stats['inserted']} 条，更新 {stats['updated']} 条记录。")
    return stats

# ★★★ 辅助函数 5: 同步主键序列 ★★★
def _resync_primary_key_sequence(cursor, table_name: str):
    """
    在执行插入前，同步表的主键序列生成器。
    """
    # ★★★ 在这里注册新表的 SERIAL 主键 ★★★
    PRIMARY_KEY_COLUMNS = {
        'custom_collections': 'id',
        'actor_subscriptions': 'id',
        'resubscribe_rules': 'id',
        'media_cleanup_tasks': 'id',
        'user_templates': 'id',
        'invitations': 'id',
        'shared_share_records': 'id',
        'shared_share_items': 'id',
        'shared_credit_ledger_local': 'id'
    }
    
    pk_column = PRIMARY_KEY_COLUMNS.get(table_name.lower())
    if not pk_column:
        logger.debug(f"  ➜ 表 '{table_name}' 未在主键序列同步列表中定义 (或其主键非SERIAL类型)，跳过。")
        return

    try:
        resync_sql = sql.SQL("""
            SELECT setval(
                pg_get_serial_sequence({table}, {pk_col}),
                GREATEST(
                    (SELECT COALESCE(MAX({pk_identifier}), 0) FROM {table_identifier}),
                    1
                )
            );
        """).format(
            table=sql.Literal(table_name.lower()),
            pk_col=sql.Literal(pk_column),
            pk_identifier=sql.Identifier(pk_column),
            table_identifier=sql.Identifier(table_name.lower())
        )
        
        cursor.execute(resync_sql)
        logger.info(f"  ➜ 已成功同步表 '{table_name}' 的主键序列。")
    except Exception as e:
        logger.warning(f"  ➜ 同步表 '{table_name}' 的主键序列时发生非致命错误: {e}")

# --- 主任务函数 ---
def task_import_database(processor, file_content: str, tables_to_import: List[str], import_strategy: str):
    """
    - 导入数据库备份主任务函数。
    """
    task_name = f"数据库恢复 ({'覆盖模式' if import_strategy == 'overwrite' else '共享模式'})"
    logger.info(f"  ➜ 后台任务开始：{task_name}，将恢复表: {tables_to_import}。")
    
    # ★★★ 共享白名单 ★★★
    # 共享导入白名单：只允许真正可跨实例复用的公共数据。
    # shared_* 是本机共享资源运行状态/分享记录/贡献值快照，包含本机 share_code、
    # virtual_id、中心 source_id、贡献值流水等账号相关状态，不应作为“共享数据”导入别人实例。
    SHARABLE_TABLES = {'person_metadata', 'translation_cache', 'media_metadata', 'p115_mediainfo_cache'}
    
    # ★★★ 为新表添加中文名 ★★★
    TABLE_TRANSLATIONS = {
        'person_metadata': '人物元数据', 
        'translation_cache': '翻译缓存',
        'actor_subscriptions': '演员订阅配置', 
        'collections_info': '电影合集信息', 
        'processed_log': '已处理列表', 
        'failed_log': '待复核列表',
        'custom_collections': '自建合集', 
        'media_metadata': '媒体元数据',
        'app_settings': '应用设置', 
        'emby_users': 'Emby用户', 
        'user_media_data': '用户媒体数据',
        'resubscribe_rules': '洗版规则', 
        'resubscribe_index': '洗版缓存', 
        'media_cleanup_tasks': '媒体清理任务',
        'user_templates': '用户权限模板', 
        'invitations': '邀请码', 
        'emby_users_extended': 'Emby用户扩展信息',
        'p115_mediainfo_cache': '115媒体信息缓存',
        'p115_organize_records': '115整理记录',
        'washing_priority_groups': '115洗版规则',
        'p115_filesystem_cache': '115目录缓存',
        # 共享资源模块新增表
        'shared_virtual_items': '虚拟入库记录',
        'shared_share_records': '我的分享记录',
        'shared_share_items': '分享文件明细',
        'shared_credit_snapshot': '贡献值快照',
        'shared_credit_ledger_local': '贡献值明细',
        'shared_maintenance_state': '共享维护状态'
    }
    summary_lines = []
    conn = None
    try:
        backup = json.loads(file_content)
        backup_data = backup.get("data", {})

        def get_table_sort_key(table_name):
            # ★★★ 设置导入顺序 ★★★
            order = {
                # --- 级别 0: 无任何依赖的核心表 ---
                'person_metadata': 0,
                'user_templates': 1,
                'emby_users': 2,

                # --- 级别 1: 依赖级别 0 的表 ---
                'emby_users_extended': 3,
                'invitations': 4,
                'actor_subscriptions': 10,
                # 共享资源表导入顺序：分享主表必须早于分享明细表。
                'shared_share_records': 20,
                'shared_share_items': 21,
                'shared_virtual_items': 22,
                'shared_credit_snapshot': 23,
                'shared_credit_ledger_local': 24,
                'shared_maintenance_state': 25
            }
            return order.get(table_name.lower(), 100)

        actual_tables_to_import = [t for t in tables_to_import if t in backup_data]
        sorted_tables_to_import = sorted(actual_tables_to_import, key=get_table_sort_key)
        
        logger.info(f"  ➜ 调整后的导入顺序：{sorted_tables_to_import}")

        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("  ➜ 数据库事务已开始。")

                logger.info("  ➜ 正在同步所有相关表的主键ID序列...")
                for table_name in sorted_tables_to_import:
                    _resync_primary_key_sequence(cursor, table_name)
                logger.info("  ➜ 主键ID序列同步完成。")

                for table_name in sorted_tables_to_import:
                    cn_name = TABLE_TRANSLATIONS.get(table_name.lower(), table_name)
                    table_data = backup_data.get(table_name, [])
                    if not table_data:
                        logger.debug(f"表 '{cn_name}' 在备份中没有数据，跳过。")
                        summary_lines.append(f"  - 表 '{cn_name}': 跳过 (备份中无数据)。")
                        continue

                    logger.info(f"  ➜ 正在处理表: '{cn_name}'，共 {len(table_data)} 行。")

                    if import_strategy == 'share':
                        if table_name.lower() not in SHARABLE_TABLES:
                            logger.warning(f"共享模式下跳过非共享表: '{cn_name}'")
                            summary_lines.append(f"  - 表 '{cn_name}': 跳过 (非共享数据)。")
                            continue
                        
                        cleaned_data = []
                        for row in table_data:
                            new_row = row.copy()
                            table_lower = table_name.lower()

                            if table_lower == 'person_metadata':
                                new_row.pop('map_id', None)

                            elif table_lower == 'media_metadata':
                                new_row.pop('emby_item_ids_json', None)
                                new_row.pop('asset_details_json', None)
                                new_row['in_library'] = False

                            elif table_lower == 'p115_mediainfo_cache':
                                # 共享导入只认 raw_ffprobe_json。
                                # mediainfo_json 带有个人默认音轨/字幕偏好，不具备共享价值，必须丢弃。
                                raw_ffprobe_json = _sanitize_shared_raw_ffprobe_json(new_row.get('raw_ffprobe_json'))
                                sha1 = str(new_row.get('sha1') or '').strip().upper()

                                if not sha1 or not raw_ffprobe_json:
                                    continue

                                new_row = {
                                    'sha1': sha1,
                                    'raw_ffprobe_json': raw_ffprobe_json
                                }

                            cleaned_data.append(new_row)
                        
                        columns, prepared_data = _prepare_data_for_insert(table_name, cleaned_data)
                        if not prepared_data:
                            summary_lines.append(f"  - 表 '{cn_name}': 跳过 (没有可共享的数据)。")
                            continue
                        
                        if table_name.lower() == 'person_metadata':
                            merge_stats = _merge_person_metadata_data(cursor, table_name, columns, prepared_data)
                            summary_lines.append(f"  - 表 '{cn_name}': 智能合并完成 (新增 {merge_stats['inserted']}, 更新 {merge_stats['updated']})。")
                        elif table_name.lower() == 'p115_mediainfo_cache':
                            inserted_count = _merge_p115_mediainfo_raw_cache(cursor, columns, prepared_data)
                            summary_lines.append(f"  - 表 '{cn_name}': 成功合并/补齐 {inserted_count} / {len(prepared_data)} 条 raw_ffprobe_json，已忽略 mediainfo_json。")
                        else:
                            inserted_count = _share_import_table_data(cursor, table_name, columns, prepared_data)
                            summary_lines.append(f"  - 表 '{cn_name}': 成功合并 {inserted_count} / {len(prepared_data)} 条新记录。")

                    else: # import_strategy == 'overwrite'
                        columns, prepared_data = _prepare_data_for_insert(table_name, table_data)
                        if not prepared_data: continue

                        _overwrite_table_data(cursor, table_name, columns, prepared_data)
                        summary_lines.append(f"  - 表 '{cn_name}': 成功覆盖 {len(prepared_data)} 条记录。")
                
                logger.info("="*11 + " 数据库恢复摘要 " + "="*11)
                for line in summary_lines: logger.info(line)
                logger.info("="*36)
                conn.commit()
                logger.info(f"  ➜  数据库事务已成功提交！任务 '{task_name}' 完成。")
                # --- 触发自动校准任务 ---
                try:
                    logger.info("  ➜ 数据导入成功，将自动触发ID计数器校准任务以确保数据一致性...")
                    # 直接调用校准任务函数
                    maintenance_db.correct_all_sequences()
                    logger.info("  ➜ ID计数器校准任务已完成。")
                except Exception as e_resync:
                    logger.error(f"  ➜ 在导入后自动执行ID校准时失败: {e_resync}", exc_info=True)
                    # 这是一个非关键步骤的失败，不应该影响主任务的成功状态，只记录错误即可。
    except Exception as e:
        logger.error(f"数据库恢复任务发生严重错误，所有更改将回滚: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
                logger.warning("数据库事务已回滚。")
            except Exception as rollback_e:
                logger.error(f"尝试回滚事务时发生额外错误: {rollback_e}")