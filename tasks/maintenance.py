# tasks/maintenance.py
# 维护性任务模块：数据库导入、媒体去重等

import json
import logging
import collections
from functools import cmp_to_key
from typing import List, Dict, Any, Tuple, Optional

# 导入需要的底层模块和共享实例
import task_manager
import emby_handler
from database import connection, maintenance_db, settings_db
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from .helpers import _get_standardized_effect

logger = logging.getLogger(__name__)

# --- 辅助函数 1: 数据清洗与准备 ---
def _prepare_data_for_insert(table_name: str, table_data: List[Dict[str, Any]]) -> tuple[List[str], List[tuple]]:
    """
    一个极简的数据准备函数，专为 PG-to-PG 流程设计。
    - 它只做一件事：将需要存入 JSONB 列的数据包装成 psycopg2 的 Json 对象。
    """
    # 定义哪些列是 JSONB 类型，需要特殊处理
    JSONB_COLUMNS = {
        'actor_subscriptions': {
            'config_genres_include_json', 'config_genres_exclude_json', 
            'config_tags_include_json', 'config_tags_exclude_json'
        },
        'custom_collections': {'definition_json', 'generated_media_info_json'},
        'media_metadata': {
            'genres_json', 'actors_json', 'directors_json', 
            'studios_json', 'countries_json', 'tags_json'
        },
        'watchlist': {'next_episode_to_air_json', 'missing_info_json'},
        'collections_info': {'missing_movies_json'},
        'resubscribe_rules': { # Add resubscribe_rules as it has JSONB fields
            'target_library_ids', 'resubscribe_audio_missing_languages',
            'resubscribe_subtitle_missing_languages', 'resubscribe_quality_include',
            'resubscribe_effect_include'
        },
        'media_cleanup_tasks': { # Add media_cleanup_tasks as it has JSONB fields
            'versions_info_json'
        },
        'app_settings': { # Add app_settings as it has JSONB fields
            'value_json'
        }
    }

    # Add specific non-JSONB columns that might be lists and need string conversion
    LIST_TO_STRING_COLUMNS = {
        'actor_subscriptions': {'config_media_types'}
    }

    if not table_data:
        return [], []

    columns = list(table_data[0].keys())
    # 使用小写表名来匹配规则
    table_json_rules = JSONB_COLUMNS.get(table_name.lower(), set())
    table_list_to_string_rules = LIST_TO_STRING_COLUMNS.get(table_name.lower(), set())
    
    prepared_rows = []
    for row_dict in table_data:
        row_values = []
        for col_name in columns:
            value = row_dict.get(col_name)
            
            # ★ 核心逻辑: 如果列是 JSONB 类型且值非空，使用 Json 适配器包装 ★
            if col_name in table_json_rules and value is not None:
                # Json() 会告诉 psycopg2: "请将这个 Python 对象作为 JSON 处理"
                value = Json(value)
            elif col_name in table_list_to_string_rules and isinstance(value, list):
                # 如果是需要转换为字符串的列表，则进行转换
                value = ','.join(map(str, value))
            
            row_values.append(value)
        prepared_rows.append(tuple(row_values))
        
    return columns, prepared_rows

# --- 辅助函数 2: 数据库覆盖操作 (保持不变，但现在更可靠) ---
def _overwrite_table_data(cursor, table_name: str, columns: List[str], data: List[tuple]):
    """安全地清空并批量插入数据。"""
    db_table_name = table_name.lower()

    logger.warning(f"执行覆盖模式：将清空表 '{db_table_name}' 中的所有数据！")
    truncate_query = sql.SQL("TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;").format(
        table=sql.Identifier(db_table_name)
    )
    cursor.execute(truncate_query)

    insert_query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
        table=sql.Identifier(db_table_name),
        cols=sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    execute_values(cursor, insert_query, data, page_size=500)
    logger.info(f"成功向表 '{db_table_name}' 插入 {len(data)} 条记录。")

# ★★★ 辅助函数 3: 数据库共享导入操作 ★★★
def _share_import_table_data(cursor, table_name: str, columns: List[str], data: List[tuple]):
    """
    安全地合并数据，使用 ON CONFLICT DO NOTHING 策略。
    这会尝试插入新行，如果主键或唯一约束冲突，则静默忽略。
    """
    # 定义每个表用于冲突检测的列（通常是主键或唯一键）
    CONFLICT_TARGETS = {
        'person_identity_map': 'tmdb_person_id',
        'actor_metadata': 'tmdb_id',
        'translation_cache': 'original_text',
        # media_metadata 的主键是复合主键
        'media_metadata': 'tmdb_id, item_type', 
    }
    
    db_table_name = table_name.lower()
    conflict_target = CONFLICT_TARGETS.get(db_table_name)

    if not conflict_target:
        logger.error(f"共享导入失败：表 '{db_table_name}' 未定义冲突目标，无法执行合并操作。")
        raise ValueError(f"Conflict target not defined for table {db_table_name}")

    logger.info(f"执行共享模式：将合并数据到表 '{db_table_name}'，冲突项将被忽略。")
    
    # 构造带有 ON CONFLICT 子句的 SQL
    insert_query = sql.SQL("""
        INSERT INTO {table} ({cols}) VALUES %s
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """).format(
        table=sql.Identifier(db_table_name),
        cols=sql.SQL(', ').join(map(sql.Identifier, columns)),
        conflict_cols=sql.SQL(', ').join(map(sql.Identifier, [c.strip() for c in conflict_target.split(',')]))
    )

    execute_values(cursor, insert_query, data, page_size=500)
    # cursor.rowcount 在 ON CONFLICT DO NOTHING 后返回的是实际插入的行数
    inserted_count = cursor.rowcount
    logger.info(f"成功向表 '{db_table_name}' 合并 {inserted_count} 条新记录（总共尝试 {len(data)} 条）。")
    return inserted_count

# --- 主任务函数 (V4 - 纯PG重构版) ---
def task_import_database(processor, file_content: str, tables_to_import: List[str], import_strategy: str):
    """
    【V5 - 共享导入重构版】
    - 根据 `import_strategy` ('overwrite' 或 'share') 决定导入行为。
    - 'share' 模式下，仅导入可共享数据，并对特定表进行清洗。
    - 使用 `ON CONFLICT DO NOTHING` 实现数据合并。
    """
    task_name = f"数据库恢复 ({'覆盖模式' if import_strategy == 'overwrite' else '共享模式'})"
    logger.info(f"后台任务开始：{task_name}，将恢复表: {tables_to_import}。")
    
    # 定义哪些表是可共享的。在共享模式下，只有这些表会被处理。
    SHARABLE_TABLES = {'person_identity_map', 'actor_metadata', 'translation_cache', 'media_metadata'}
    
    TABLE_TRANSLATIONS = {
        'person_identity_map': '演员映射表',
        'actor_metadata': '演员元数据',
        'translation_cache': '翻译缓存',
        'watchlist': '智能追剧列表',
        'actor_subscriptions': '演员订阅配置',
        'tracked_actor_media': '已追踪的演员作品',
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
    }
    summary_lines = []
    conn = None
    try:
        backup = json.loads(file_content)
        backup_data = backup.get("data", {})

        # --- 新增的逻辑: 强制排序 tables_to_import ---
        # 定义表的依赖顺序。排在前面的表是父表或没有依赖的表。
        # 这里只列出明确需要优先处理的表。
        # 未列出的表将保持其在原始列表中的相对顺序，但会排在已定义依赖的表之后。
        # 例如：person_identity_map 必须在 actor_metadata 之前
        # 你可以根据实际情况添加更多依赖关系
        
        # 建立一个排序键函数
        def get_table_sort_key(table_name):
            table_name_lower = table_name.lower()
            if table_name_lower == 'person_identity_map':
                return 0  # 演员身份映射表，是 actor_metadata 和 actor_subscriptions 的基础
            elif table_name_lower == 'users':
                return 1  # 用户表，通常也是基础
            elif table_name_lower == 'actor_subscriptions':
                return 10 # 演员订阅配置，依赖于 person_identity_map (语义上)，被 tracked_actor_media 依赖
            elif table_name_lower == 'actor_metadata':
                return 11 # 演员元数据，依赖于 person_identity_map (外键)
            elif table_name_lower == 'tracked_actor_media':
                return 20 # 已追踪的演员作品，依赖于 actor_subscriptions (外键)
            # 其他表，目前没有明确的外键依赖，可以放在后面
            # 它们的相对顺序将由原始 tables_to_import 列表决定，如果它们有相同的默认权重
            elif table_name_lower in [
                'processed_log', 'failed_log', 'translation_cache',
                'collections_info', 'custom_collections', 'media_metadata', 'watchlist'
            ]:
                return 100 # 默认权重
            else:
                return 999 # 未知表，确保它排在最后，以防万一

        # 核心排序逻辑
        actual_tables_to_import = [
            t for t in tables_to_import if t in backup_data
        ]
        
        sorted_tables_to_import = sorted(actual_tables_to_import, key=get_table_sort_key)
        
        logger.info(f"调整后的导入顺序：{sorted_tables_to_import}")
        # --- 结束更新逻辑 ---

        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                logger.info("数据库事务已开始。")
                for table_name in sorted_tables_to_import:
                    cn_name = TABLE_TRANSLATIONS.get(table_name.lower(), table_name)
                    table_data = backup_data.get(table_name, [])
                    if not table_data:
                        logger.debug(f"表 '{cn_name}' 在备份中没有数据，跳过。")
                        summary_lines.append(f"  - 表 '{cn_name}': 跳过 (备份中无数据)。")
                        continue

                    logger.info(f"正在处理表: '{cn_name}'，共 {len(table_data)} 行。")

                    # ★★★ 核心逻辑分支 ★★★
                    if import_strategy == 'share':
                        # --- 共享模式 ---
                        if table_name.lower() not in SHARABLE_TABLES:
                            logger.warning(f"共享模式下跳过非共享表: '{cn_name}'")
                            summary_lines.append(f"  - 表 '{cn_name}': 跳过 (非共享数据)。")
                            continue
                        
                        # ★ 数据清洗：根据表名移除或修改特定字段
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
                        
                        inserted_count = _share_import_table_data(cursor, table_name, columns, prepared_data)
                        summary_lines.append(f"  - 表 '{cn_name}': 成功合并 {inserted_count} / {len(prepared_data)} 条新记录。")

                    else: # import_strategy == 'overwrite'
                        # --- 覆盖模式 (保持原有逻辑) ---
                        columns, prepared_data = _prepare_data_for_insert(table_name, table_data)
                        if not prepared_data: continue

                        _overwrite_table_data(cursor, table_name, columns, prepared_data)
                        summary_lines.append(f"  - 表 '{cn_name}': 成功覆盖 {len(prepared_data)} 条记录。")
                
                logger.info("="*11 + " 数据库恢复摘要 " + "="*11)
                for line in summary_lines: logger.info(line)
                logger.info("="*36)
                conn.commit()
                logger.info(f"✅ 数据库事务已成功提交！任务 '{task_name}' 完成。")
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

def _get_version_properties(version: Optional[Dict]) -> Dict:
    """【V4 - 杜比Profile细分版】从单个版本信息中提取并计算属性，增加特效标准化。"""
    if not version or not isinstance(version, dict):
        return {
            'id': 'unknown_or_invalid', 'path': '', 'quality': 'unknown',
            'resolution': 'unknown', 'effect': 'sdr', 'filesize': 0
        }

    path_lower = version.get("Path", "").lower()
    
    # --- 质量标准化 (逻辑不变) ---
    QUALITY_ALIASES = {
        "remux": "remux", "bluray": "blu-ray", "blu-ray": "blu-ray",
        "web-dl": "web-dl", "webdl": "web-dl", "webrip": "webrip",
        "hdtv": "hdtv", "dvdrip": "dvdrip"
    }
    QUALITY_HIERARCHY = ["remux", "blu-ray", "web-dl", "webrip", "hdtv", "dvdrip"]
    quality = "unknown"
    for alias, official_name in QUALITY_ALIASES.items():
        if (f".{alias}." in path_lower or f" {alias} " in path_lower or
            f"-{alias}-" in path_lower or f"·{alias}·" in path_lower):
            current_priority = QUALITY_HIERARCHY.index(quality) if quality in QUALITY_HIERARCHY else 999
            new_priority = QUALITY_HIERARCHY.index(official_name)
            if new_priority < current_priority:
                quality = official_name

    # --- 分辨率标准化 (逻辑不变) ---
    resolution_tag = "unknown"
    resolution_wh = version.get("resolution_wh", (0, 0))
    width = resolution_wh[0]
    if width >= 3840: resolution_tag = "2160p"
    elif width >= 1920: resolution_tag = "1080p"
    elif width >= 1280: resolution_tag = "720p"

    # --- ★★★ 核心修改：调用新的辅助函数来标准化特效 ★★★ ---
    video_stream = version.get("video_stream")
    effect_tag = _get_standardized_effect(path_lower, video_stream)

    return {
        "id": version.get("id"),
        "quality": quality,
        "resolution": resolution_tag,
        "effect": effect_tag, # <-- 使用新字段
        "filesize": version.get("filesize", 0)
    }

def _determine_best_version_by_rules(versions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """【V6 - 杜比Profile细分版】"""
    
    rules = settings_db.get_setting('media_cleanup_rules')
    if not rules:
        # 更新默认规则，加入 effect
        rules = [
            {"id": "quality", "priority": ["remux", "blu-ray", "web-dl", "hdtv"]},
            {"id": "resolution", "priority": ["2160p", "1080p", "720p"]},
            # ★★★ 核心修改：更新默认特效规则，细化杜比视界 ★★★
            {"id": "effect", "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]},
            {"id": "filesize", "priority": "desc"}
        ]

    processed_rules = []
    for rule in rules:
        new_rule = rule.copy()
        if rule.get("id") == "quality" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            normalized_priority = []
            for p in new_rule["priority"]:
                p_lower = str(p).lower()
                if p_lower == "bluray": p_lower = "blu-ray"
                if p_lower == "webdl": p_lower = "web-dl"
                normalized_priority.append(p_lower)
            new_rule["priority"] = normalized_priority
        # ★★★ 新增：对特效规则也进行标准化处理 ★★★
        elif rule.get("id") == "effect" and "priority" in new_rule and isinstance(new_rule["priority"], list):
            new_rule["priority"] = [str(p).lower().replace(" ", "_") for p in new_rule["priority"]]

        processed_rules.append(new_rule)
    
    version_properties = [_get_version_properties(v) for v in versions if v is not None]

    from functools import cmp_to_key
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
    【V15 - 特效支持版】
    在构造 versions_info 时，将 video_stream 传递下去。
    """
    task_name = "扫描媒体库重复项"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    task_manager.update_status_from_thread(0, "正在准备扫描媒体库...")

    try:
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        task_manager.update_status_from_thread(5, f"正在从 {len(libs_to_process_ids)} 个媒体库获取项目...")
        all_emby_items = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            # 确保请求了 MediaStreams
            fields="ProviderIds,Name,Type,MediaSources,Path,ProductionYear,MediaStreams"
        ) or []

        if not all_emby_items:
            task_manager.update_status_from_thread(100, "任务完成：在指定媒体库中未找到任何项目。")
            return

        task_manager.update_status_from_thread(30, f"已获取 {len(all_emby_items)} 个项目，正在分析...")
        
        media_map = collections.defaultdict(list)
        for item in all_emby_items:
            tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
            item_type = item.get("Type")
            if tmdb_id and item_type:
                media_map[(tmdb_id, item_type)].append(item)

        duplicate_tasks = []
        for (tmdb_id, item_type), items in media_map.items():
            if len(items) > 1:
                logger.info(f"  -> [发现重复] TMDB ID {tmdb_id} (类型: {item_type}) 关联了 {len(items)} 个独立的媒体项。")
                versions_info = []
                for item in items:
                    source = item.get("MediaSources", [{}])[0]
                    video_stream = next((s for s in source.get("MediaStreams", []) if s.get("Type") == "Video"), None)
                    versions_info.append({
                        "id": item.get("Id"),
                        "path": source.get("Path") or item.get("Path") or "",
                        "size": source.get("Size", 0),
                        "resolution_wh": (video_stream.get("Width", 0), video_stream.get("Height", 0)) if video_stream else (0, 0),
                        # ★★★ 核心修改：把 video_stream 整个传下去 ★★★
                        "video_stream": video_stream
                    })
                
                analyzed_versions, best_id = _determine_best_version_by_rules(versions_info)
                best_item_name = next((item.get("Name") for item in items if item.get("Id") == best_id), items[0].get("Name"))
                
                duplicate_tasks.append({
                    "task_type": "duplicate",
                    "tmdb_id": tmdb_id,
                    "item_type": item_type,
                    "item_name": best_item_name,
                    "versions_info_json": analyzed_versions, "best_version_id": best_id
                })

        task_manager.update_status_from_thread(90, f"分析完成，正在将 {len(duplicate_tasks)} 组重复项写入数据库...")
        maintenance_db.batch_insert_cleanup_tasks(duplicate_tasks)

        final_message = f"扫描完成！共发现 {len(duplicate_tasks)} 组重复项，待清理。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_execute_cleanup(processor, task_ids: List[int], **kwargs):
    """
    后台任务：执行指定的一批媒体去重任务（删除多余文件）。
    这是一个高危的写操作。
    """
    # ★★★ 核心修复：这个函数签名现在可以正确接收 processor 和 task_ids 两个位置参数，
    # 同时用 **kwargs 忽略掉 task_manager 传来的其他所有参数。

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
        for i, task in enumerate(tasks_to_execute):
            if processor.is_stop_requested():
                logger.warning("任务被用户中止。")
                break
            
            task_id = task['id']
            item_name = task['item_name']
            best_version_id = task['best_version_id']
            versions = task['versions_info_json']

            task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在清理: {item_name}")

            for version in versions:
                version_id_to_check = version.get('id')
                if version_id_to_check != best_version_id:
                    logger.warning(f"  -> 准备删除劣质版本: {version.get('path')}")
                    
                    id_to_delete = version_id_to_check
                    
                    success = emby_handler.delete_item(
                        item_id=id_to_delete,
                        emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id
                    )
                    if success:
                        deleted_count += 1
                        logger.info(f"    -> 成功删除 ID: {id_to_delete}")
                    else:
                        logger.error(f"    -> 删除 ID: {id_to_delete} 失败！")
            
            maintenance_db.batch_update_cleanup_task_status([task_id], 'processed')

        final_message = f"清理完成！共处理 {total} 个任务，删除了 {deleted_count} 个多余版本/文件。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")