# tasks/maintenance.py
# 维护性任务模块：数据库导入、媒体去重等

import json
import os
import logging
import collections
from functools import cmp_to_key
from typing import List, Dict, Any, Tuple, Optional

# 导入需要的底层模块和共享实例
import task_manager
import handler.emby as emby
from database import connection, maintenance_db, settings_db
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from .helpers import _get_standardized_effect, _extract_quality_tag_from_filename

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
        'collections_info': {'missing_movies_json'},
        'custom_collections': {'definition_json', 'allowed_user_ids', 'generated_media_info_json'},
        'user_collection_cache': {'visible_emby_ids_json'},
        'media_metadata': {
            'emby_item_ids_json', 'paths_json', 'subscription_sources_json', 
            'pre_cached_tags_json', 'pre_cached_extra_json', 'genres_json', 
            'actors_json', 'directors_json', 'studios_json', 'countries_json', 
            'keywords_json', 'next_episode_to_air_json', 'last_episode_to_air_json',
            'watchlist_next_episode_json', 'watchlist_missing_info_json'
        },
        'actor_subscriptions': {'config_genres_include_json', 'config_genres_exclude_json', 'last_scanned_tmdb_ids_json'},
        'resubscribe_rules': {
            'target_library_ids', 'resubscribe_audio_missing_languages',
            'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
            'resubscribe_effect_include'
        },
        'resubscribe_cache': {'audio_languages_raw', 'subtitle_languages_raw'},
        'media_cleanup_tasks': {'versions_info_json'},
        'user_templates': {'emby_policy_json', 'emby_configuration_json'}
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
        'person_identity_map': 'tmdb_person_id',
        'actor_metadata': 'tmdb_id',
        'translation_cache': 'original_text',
        'media_metadata': 'tmdb_id, item_type', 
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

# ★★★ 辅助函数 4: 专门用于合并 person_identity_map 的智能函数 ★★★
def _merge_person_identity_map_data(cursor, table_name: str, columns: List[str], data: List[tuple]) -> dict:
    """
    【V2 - 终极修复版】为 person_identity_map 表提供一个健壮的合并策略。
    """
    logger.info(f"  ➜ 执行智能合并模式：将合并数据到表 '{table_name}'...")
    
    stats = {'inserted': 0, 'updated': 0, 'merged_and_deleted': 0}
    
    data_dicts = [dict(zip(columns, row)) for row in data]

    for row_to_merge in data_dicts:
        ids_to_check = {
            'tmdb_person_id': row_to_merge.get('tmdb_person_id'),
            'imdb_id': row_to_merge.get('imdb_id'),
            'douban_celebrity_id': row_to_merge.get('douban_celebrity_id')
        }
        
        query_parts = []
        params = []
        for key, value in ids_to_check.items():
            if value:
                query_parts.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
                params.append(value)
            
        if not query_parts:
            continue

        find_sql = sql.SQL("SELECT * FROM person_identity_map WHERE {}").format(sql.SQL(' OR ').join(query_parts))
        cursor.execute(find_sql, tuple(params))
        existing_records = cursor.fetchall()

        if not existing_records:
            all_cols_in_order = [col for col in columns if col != 'map_id']
            values_to_insert = [row_to_merge.get(col) for col in all_cols_in_order]

            insert_sql = sql.SQL("INSERT INTO person_identity_map ({}) VALUES ({})").format(
                sql.SQL(', ').join(map(sql.Identifier, all_cols_in_order)),
                sql.SQL(', ').join(sql.Placeholder() * len(all_cols_in_order))
            )
            cursor.execute(insert_sql, values_to_insert)
            stats['inserted'] += 1
        else:
            sorted_records = sorted(existing_records, key=lambda r: r['map_id'])
            master_record_original = dict(sorted_records[0])
            records_to_delete = sorted_records[1:]
            
            merged_data = master_record_original.copy()
            all_sources = records_to_delete + [row_to_merge]
            
            for source in all_sources:
                for key, value in source.items():
                    if key in ['tmdb_person_id', 'imdb_id', 'douban_celebrity_id', 'primary_name'] and value and not merged_data.get(key):
                        merged_data[key] = value

            if records_to_delete:
                ids_to_delete = [r['map_id'] for r in records_to_delete]
                delete_sql = sql.SQL("DELETE FROM person_identity_map WHERE map_id = ANY(%s)")
                cursor.execute(delete_sql, (ids_to_delete,))
                stats['merged_and_deleted'] += len(ids_to_delete)

            updates = {
                k: v for k, v in merged_data.items() 
                if k != 'map_id' and v != master_record_original.get(k)
            }
            
            if updates:
                set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in updates.keys()]
                update_sql = sql.SQL("UPDATE person_identity_map SET {} WHERE map_id = %s").format(sql.SQL(', ').join(set_clauses))
                cursor.execute(update_sql, tuple(updates.values()) + (merged_data['map_id'],))
                stats['updated'] += 1
            
    logger.info(f"  ➜ 智能合并完成：新增 {stats['inserted']} 条，更新 {stats['updated']} 条，合并删除 {stats['merged_and_deleted']} 条记录。")
    return stats

# ★★★ 辅助函数 5: 同步主键序列 ★★★
def _resync_primary_key_sequence(cursor, table_name: str):
    """
    在执行插入前，同步表的主键序列生成器。
    """
    # ★★★ 在这里注册新表的 SERIAL 主键 ★★★
    PRIMARY_KEY_COLUMNS = {
        'users': 'id',
        'custom_collections': 'id',
        'person_identity_map': 'map_id',
        'actor_subscriptions': 'id',
        'resubscribe_rules': 'id',
        'media_cleanup_tasks': 'id',
        'user_templates': 'id',
        'invitations': 'id'
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
    
    SHARABLE_TABLES = {'person_identity_map', 'actor_metadata', 'translation_cache', 'media_metadata'}
    
    # ★★★ 为新表添加中文名 ★★★
    TABLE_TRANSLATIONS = {
        'person_identity_map': '演员映射表', 
        'actor_metadata': '演员元数据', 
        'translation_cache': '翻译缓存',
        'actor_subscriptions': '演员订阅配置', 
        'collections_info': '电影合集信息', 
        'processed_log': '已处理列表', 
        'failed_log': '待复核列表',
        'users': '用户账户', 
        'custom_collections': '自建合集', 
        'media_metadata': '媒体元数据',
        'app_settings': '应用设置', 
        'emby_users': 'Emby用户', 
        'user_media_data': '用户媒体数据',
        'resubscribe_rules': '洗版规则', 
        'resubscribe_cache': '洗版缓存', 
        'media_cleanup_tasks': '媒体清理任务',
        'user_templates': '用户权限模板', 
        'invitations': '邀请码', 
        'emby_users_extended': 'Emby用户扩展信息',
        'user_collection_cache': 'Emby用户权限缓存'
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
                'person_identity_map': 0,
                'users': 1,
                'user_templates': 2,
                'emby_users': 3,

                # --- 级别 1: 依赖级别 0 的表 ---
                'emby_users_extended': 4,
                'invitations': 5,
                'actor_subscriptions': 10,

                # --- 级别 2: 依赖更早级别的表 ---
                'actor_metadata': 11
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
                            if table_name.lower() == 'person_identity_map':
                                new_row.pop('map_id', None)
                                new_row.pop('emby_person_id', None)
                            elif table_name.lower() == 'media_metadata':
                                new_row.pop('emby_item_id', None)
                                new_row['in_library'] = False
                            cleaned_data.append(new_row)
                        
                        columns, prepared_data = _prepare_data_for_insert(table_name, cleaned_data)
                        if not prepared_data: continue
                        
                        if table_name.lower() == 'person_identity_map':
                            merge_stats = _merge_person_identity_map_data(cursor, table_name, columns, prepared_data)
                            summary_lines.append(f"  - 表 '{cn_name}': 智能合并完成 (新增 {merge_stats['inserted']}, 更新 {merge_stats['updated']}, 清理 {merge_stats['merged_and_deleted']})。")
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

# ======================================================================
# ★★★ 媒体去重模块 (Media Cleanup Module) - 新增 ★★★
# ======================================================================

def _get_properties_for_comparison(version: Dict) -> Dict:
    """
    【V2.2 - 最终数据结构修正版】
    彻底修正了对 'media_cleanup_rules' 数据结构（List of Dicts）的错误处理。
    """
    if not version or not isinstance(version, dict):
        return {'id': None, 'quality': 'unknown', 'resolution': 'unknown', 'effect': 'sdr', 'filesize': 0}

    # ★★★ 核心修正：正确处理规则列表 ★★★
    # 1. 正确获取规则列表，如果不存在则为空列表
    all_rules_list = settings_db.get_setting('media_cleanup_rules') or []
    
    # 2. 从规则列表中找到 'effect' 规则的字典
    effect_rule = next((rule for rule in all_rules_list if rule.get('id') == 'effect'), {})
    
    # 3. 从找到的 'effect' 规则字典中安全地获取优先级列表
    effect_priority = effect_rule.get('priority', 
        ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"])
    
    effect_list = version.get("effect_display", [])
    best_effect = 'sdr'
    
    if effect_list:
        standardized_effects = [_get_standardized_effect(e, None) for e in effect_list]
        best_effect = min(standardized_effects, key=lambda e: effect_priority.index(e) if e in effect_priority else 999)

    return {
        "id": version.get("emby_item_id"),
        "quality": str(version.get("quality_display", "unknown")).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl"),
        "resolution": version.get("resolution_display", "unknown"),
        "effect": best_effect,
        "filesize": version.get("size_bytes", 0)
    }

def _determine_best_version_by_rules(versions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    【V2 - 重构版】
    接收从 asset_details_json 来的版本列表，并根据规则决定最佳版本。
    """
    rules = settings_db.get_setting('media_cleanup_rules')
    if not rules: # 如果没有设置，使用默认规则
        rules = [
            {"id": "quality", "enabled": True, "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            {"id": "resolution", "enabled": True, "priority": ["2160p", "1080p", "720p"]},
            {"id": "effect", "enabled": True, "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]},
            {"id": "filesize", "enabled": True, "priority": "desc"}
        ]

    # 预处理规则，确保格式统一
    processed_rules = []
    for rule in rules:
        new_rule = rule.copy()
        if rule.get("id") == "quality" and "priority" in new_rule:
            new_rule["priority"] = [str(p).lower().replace("bluray", "blu-ray").replace("webdl", "web-dl") for p in new_rule["priority"]]
        elif rule.get("id") == "effect" and "priority" in new_rule:
            new_rule["priority"] = [str(p).lower().replace(" ", "_") for p in new_rule["priority"]]
        processed_rules.append(new_rule)
    
    # 将数据库来的版本信息，转换为用于比较的标准化属性字典列表
    version_properties = [_get_properties_for_comparison(v) for v in versions if v]

    def compare_versions(item1_props, item2_props):
        for rule in processed_rules:
            if not rule.get("enabled", True): continue
            
            rule_id = rule.get("id")
            val1 = item1_props.get(rule_id)
            val2 = item2_props.get(rule_id)

            if rule_id == "filesize":
                if val1 > val2: return -1
                if val1 < val2: return 1
                continue

            priority_list = rule.get("priority", [])
            try:
                index1 = priority_list.index(val1) if val1 in priority_list else 999
                index2 = priority_list.index(val2) if val2 in priority_list else 999
                
                if index1 < index2: return -1
                if index1 > index2: return 1
            except (ValueError, TypeError):
                continue
        return 0

    sorted_versions = sorted(version_properties, key=cmp_to_key(compare_versions))
    
    best_version_id = sorted_versions[0]['id'] if sorted_versions else None
    
    # 返回原始版本信息和最佳ID
    return versions, best_version_id

def task_scan_for_cleanup_issues(processor):
    """
    【V4 - 最终统一标准版】
    直接读取 media_metadata 中已分析好的数据，并格式化为前端需要的格式。
    """
    task_name = "扫描媒体库重复项 (统一标准模式)"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    task_manager.update_status_from_thread(0, "正在从数据库准备扫描...")

    try:
        sql_query = """
            SELECT tmdb_id, item_type, title, asset_details_json
            FROM media_metadata
            WHERE in_library = TRUE AND jsonb_array_length(asset_details_json) > 1;
        """
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                multi_version_items = cursor.fetchall()

        total_items = len(multi_version_items)
        if total_items == 0:
            task_manager.update_status_from_thread(100, "扫描完成：未在数据库中发现多版本媒体。")
            return

        task_manager.update_status_from_thread(10, f"发现 {total_items} 组多版本媒体，开始分析...")
        
        cleanup_tasks = []
        for i, item in enumerate(multi_version_items):
            progress = 10 + int((i / total_items) * 80)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_items}) 正在分析: {item['title']}")

            versions_from_db = item['asset_details_json']
            
            # 1. 决策函数依然用于找出最佳ID
            _, best_id = _determine_best_version_by_rules(versions_from_db)

            # 2. 格式化：将数据库中的标准数据，转换为前端需要的最终格式
            versions_for_frontend = []
            for v in versions_from_db:
                # _get_properties_for_comparison 用于获取“比较”用的标准化值
                props = _get_properties_for_comparison(v)
                
                # 特效需要特殊处理一下，从列表变成字符串
                effect_value = "SDR" # 默认值
                if effect_list := v.get('effect_display'):
                    # 如果有多个特效，比如 ["Dolby Vision", "HDR"]，我们只取最重要的那个给 props.get('effect')
                    # _get_properties_for_comparison 已经帮我们做了这件事
                    effect_value = props.get('effect')

                versions_for_frontend.append({
                    'id': v.get('emby_item_id'),
                    'Path': v.get('path'),
                    'filesize': v.get('size_bytes', 0),
                    'quality': v.get('quality_display'),
                    'resolution': v.get('resolution_display'),
                    'effect': effect_value # 使用我们处理过的单个特效值
                })

            # 3. 创建清理任务
            cleanup_tasks.append({
                "task_type": "Multi-version",
                "tmdb_id": item['tmdb_id'], 
                "item_type": item['item_type'],
                "item_name": item['title'], 
                "versions_info_json": versions_for_frontend,
                "best_version_id": best_id
            })

        task_manager.update_status_from_thread(90, f"分析完成，正在将 {len(cleanup_tasks)} 组任务写入数据库...")
        
        if cleanup_tasks:
            maintenance_db.batch_insert_cleanup_tasks(cleanup_tasks)

        final_message = f"扫描完成！共发现 {len(cleanup_tasks)} 组需要清理的多版本媒体。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_execute_cleanup(processor, task_ids: List[int], **kwargs):
    """
    后台任务：执行指定的一批媒体去重任务（删除多余文件）。
    (此函数逻辑不变)
    """
    if not task_ids or not isinstance(task_ids, list):
        logger.error("执行媒体去重任务失败：缺少有效的 'task_ids' 参数。")
        task_manager.update_status_from_thread(-1, "任务失败：缺少任务ID")
        return

    task_name = "执行媒体去重"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (任务ID: {task_ids}) ---")
    
    try:
        tasks_to_execute = maintenance_db.get_cleanup_tasks_by_ids(task_ids)
        total = len(tasks_to_execute)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：未找到指定的清理任务。")
            return

        deleted_count = 0
        processed_task_ids = []
        for i, task in enumerate(tasks_to_execute):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break
            
            item_name = task['item_name']
            best_version_id = task['best_version_id']
            versions = task['versions_info_json']
            task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在清理: {item_name}")

            for version in versions:
                version_id_to_check = version.get('id')
                if version_id_to_check != best_version_id:
                    logger.warning(f"  ➜ 准备删除劣质版本 (ID: {version_id_to_check}): {version.get('path')}")
                    
                    # ★★★ 核心修复：移除 mediasource_ 前缀，确保传递纯粹的 Emby GUID ★★★
                    emby_item_id_to_delete = version_id_to_check
                    if isinstance(version_id_to_check, str) and version_id_to_check.startswith('mediasource_'):
                        emby_item_id_to_delete = version_id_to_check.replace('mediasource_', '')
                        logger.debug(f"    ➜ 检测到 'mediasource_' 前缀，已移除。实际删除ID: {emby_item_id_to_delete}")

                    success = emby.delete_item(
                        item_id=emby_item_id_to_delete,
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    )
                    if success:
                        deleted_count += 1
                        logger.info(f"    ➜ 成功删除 ID: {version_id_to_check}")
                    else:
                        logger.error(f"    ➜ 删除 ID: {version_id_to_check} 失败！")
            
            processed_task_ids.append(task['id'])

        if processed_task_ids:
            maintenance_db.batch_update_cleanup_task_status(processed_task_ids, 'processed')

        final_message = f"清理完成！共处理 {len(processed_task_ids)} 个任务，尝试删除了 {deleted_count} 个多余版本。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
