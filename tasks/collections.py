# tasks/collections.py
# 原生合集与自建合集任务模块

import json
import logging
import pytz
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Set

# 导入需要的底层模块和共享实例
import handler.emby as emby
import task_manager
import handler.tmdb as tmdb
from database import collection_db, connection, settings_db, media_db, request_db
from handler.custom_collection import ListImporter, FilterEngine
from handler import collections
from services.cover_generator import CoverGeneratorService

logger = logging.getLogger(__name__)

# 辅助函数应用修正
def _apply_id_corrections(tmdb_items: list, definition: dict, collection_name: str) -> tuple[list, dict]:
    """
    【V2 - 完整修正版】
    应用合集定义中的ID修正规则，并完整保留季号信息。
    :return: 一个元组，包含 (修正后的tmdb_items列表, 新ID到旧ID的映射字典)
    """
    corrections = definition.get('corrections', {})
    corrected_id_to_original_id_map = {}
    if corrections:
        logger.info(f"  -> 检测到合集 '{collection_name}' 存在 {len(corrections)} 条修正规则，正在应用...")
        for item in tmdb_items:
            original_id_str = str(item.get('id'))
            if original_id_str in corrections:
                corrected_value = corrections[original_id_str]
                logger.info(f"    -> 应用修正: 将源 ID {original_id_str} 替换为 {corrected_value}")
                
                new_id = None
                # ▼▼▼ 核心修复 ▼▼▼
                if isinstance(corrected_value, dict):
                    # 同时提取 tmdb_id 和 season
                    new_id = corrected_value.get('tmdb_id')
                    new_season = corrected_value.get('season')
                    
                    if new_id:
                        item['id'] = new_id
                    # 如果有季号信息，也更新到 item 中
                    if new_season is not None:
                        item['season'] = new_season
                else:
                    # 兼容旧的纯ID修正
                    new_id = corrected_value
                    item['id'] = new_id
                # ▲▲▲ 修复结束 ▲▲▲
                
                if new_id:
                    corrected_id_to_original_id_map[str(new_id)] = original_id_str
                    
    return tmdb_items, corrected_id_to_original_id_map

# 辅助函数榜单健康检查
def _perform_list_collection_health_check(
    tmdb_items: list, 
    tmdb_to_emby_item_map: dict, 
    corrected_id_to_original_id_map: dict, 
    collection_db_record: dict, 
    tmdb_api_key: str
) -> dict:
    """
    榜单健康检查 (修复版)
    适配 {tmdb_id}_{item_type} 格式的映射键。
    """
    collection_id = collection_db_record.get('id')
    collection_name = collection_db_record.get('name', '未知合集')
    logger.info(f"  ➜ 榜单合集 '{collection_name}'，开始进行健康度分析...")

    # 获取上一次同步时生成的媒体列表 
    old_media_map = {}
    historical_data = collection_db_record.get('generated_media_info_json')
    
    if historical_data:
        try:
            old_items = []
            if isinstance(historical_data, str):
                old_items = json.loads(historical_data)
            elif isinstance(historical_data, list):
                old_items = historical_data
            
            if old_items:
                old_media_map = {str(item['tmdb_id']): item['media_type'] for item in old_items}
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"  -> 解析合集 '{collection_name}' 的历史媒体列表时失败: {e}")

    # 提前加载所有在库的“季”的信息
    in_library_seasons_set = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT parent_series_tmdb_id, season_number FROM media_metadata WHERE item_type = 'Season' AND in_library = TRUE")
            for row in cursor.fetchall():
                in_library_seasons_set.add((row['parent_series_tmdb_id'], row['season_number']))
    except Exception as e_db:
        logger.error(f"  -> 获取在库季列表时发生数据库错误: {e_db}", exc_info=True)

    # ★★★ 核心修复：获取所有在库的 Key 集合 (格式: id_type) ★★★
    in_library_keys = set(tmdb_to_emby_item_map.keys())
    
    missing_released_items = []
    missing_unreleased_items = []
    parent_series_to_ensure_exist = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for item_def in tmdb_items:
        tmdb_id = str(item_def.get('tmdb_id'))
        media_type = item_def.get('media_type')
        season_num = item_def.get('season')

        is_in_library = False
        
        # 1. 如果 FilterEngine 已经直接给出了 emby_id，那肯定在库
        if item_def.get('emby_id'):
            is_in_library = True
        
        # 2. 季的判断逻辑 (保持不变)
        elif season_num is not None and media_type == 'Series':
            if (tmdb_id, season_num) in in_library_seasons_set:
                is_in_library = True
        
        # 3. 顶层项目 (电影/剧集) 的判断逻辑
        else:
            # ★★★ 修复点：构造组合键进行查找 ★★★
            current_key = f"{tmdb_id}_{media_type}"
            
            if current_key in in_library_keys:
                is_in_library = True
            else:
                # 尝试检查修正前的原始 ID
                original_id = corrected_id_to_original_id_map.get(tmdb_id)
                if original_id:
                    original_key = f"{original_id}_{media_type}"
                    if original_key in in_library_keys:
                        is_in_library = True
        
        if is_in_library:
            continue

        try:
            details = None
            item_type_for_db = media_type

            if season_num is not None and media_type == 'Series':
                details = tmdb.get_tv_season_details(tmdb_id, season_num, tmdb_api_key)
                if details:
                    item_type_for_db = 'Season'
                    parent_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
                    details['parent_series_tmdb_id'] = tmdb_id
                    details['parent_title'] = parent_details.get('name', '')
                    details['parent_poster_path'] = parent_details.get('poster_path')

                    # ★★★ 当发现缺失的季时，立即登记父剧集 ★★★
                    parent_series_to_ensure_exist.append({
                        'tmdb_id': tmdb_id,
                        'item_type': 'Series',
                        'title': parent_details.get('name'),
                        'original_title': parent_details.get('original_name'),
                        'release_date': parent_details.get('first_air_date'),
                        'release_year': parent_details.get('first_air_date', '----').split('-')[0],
                        'poster_path': parent_details.get('poster_path')
                    })
            else:
                details = tmdb.get_movie_details(tmdb_id, tmdb_api_key) if media_type == 'Movie' else tmdb.get_tv_details(tmdb_id, tmdb_api_key)

            if not details: continue
            
            release_date = details.get("air_date") or details.get("release_date") or details.get("first_air_date", '')
            
            item_details_for_db = {
                'tmdb_id': str(details.get('id')),
                'item_type': item_type_for_db,
                'title': details.get('name') or f"第 {season_num} 季" if item_type_for_db == 'Season' else details.get('title') or details.get('name'),
                'release_date': release_date,
                'poster_path': details.get('poster_path') or details.get('parent_poster_path'),
                'parent_series_tmdb_id': tmdb_id if item_type_for_db == 'Season' else None,
                'season_number': details.get('season_number'),
                'source': { "type": "collection", "id": collection_db_record.get('id'), "name": collection_name }
            }

            if release_date and release_date > today_str:
                missing_unreleased_items.append(item_details_for_db)
            else:
                missing_released_items.append(item_details_for_db)

        except Exception as e:
            logger.error(f"为合集 '{collection_name}' 获取 {tmdb_id} (季: {season_num}) 详情时发生异常: {e}", exc_info=True)

    source_for_subscription = {"type": "collection", "id": collection_db_record.get('id'), "name": collection_name}

    # 在处理订阅请求之前，先把所有父剧集的档案建立好
    if parent_series_to_ensure_exist:
        # 去重，防止重复处理
        unique_parents = {p['tmdb_id']: p for p in parent_series_to_ensure_exist}.values()
        logger.info(f"  -> 检测到 {len(unique_parents)} 个缺失的父剧集元数据，正在创建占位记录...")
        request_db.set_media_status_none(
            tmdb_ids=[p['tmdb_id'] for p in unique_parents],
            item_type='Series',
            media_info_list=list(unique_parents)
        )

    # 分组并更新季的订阅状态
    def group_and_update(items_list, status):
        if not items_list: return
        logger.info(f"  -> 检测到 {len(items_list)} 个缺失媒体，将订阅状态设为 '{status}'...")
        
        requests_by_type = {}
        for item in items_list:
            item_type = item['item_type']
            if item_type not in requests_by_type:
                requests_by_type[item_type] = []
            requests_by_type[item_type].append(item)
            
        for item_type, requests in requests_by_type.items():
            logger.info(f"    -> 正在为 {len(requests)} 个 '{item_type}' 类型的项目更新状态...")
            if status == 'WANTED':
                request_db.set_media_status_wanted(
                    tmdb_ids=[req['tmdb_id'] for req in requests],
                    item_type=item_type,
                    media_info_list=requests,
                    source=source_for_subscription
                )
            elif status == 'PENDING_RELEASE':
                request_db.set_media_status_pending_release(
                    tmdb_ids=[req['tmdb_id'] for req in requests],
                    item_type=item_type,
                    media_info_list=requests,
                    source=source_for_subscription
                )

    group_and_update(missing_released_items, 'WANTED')
    group_and_update(missing_unreleased_items, 'PENDING_RELEASE')

    # 清理掉出榜单的媒体项
    if old_media_map:
        # 获取本次新生成的媒体ID集合
        new_tmdb_ids = {str(item['tmdb_id']) for item in tmdb_items}
        
        # 计算差异，找出那些在旧列表里，但不在新列表里的ID
        removed_tmdb_ids = set(old_media_map.keys()) - new_tmdb_ids

        if removed_tmdb_ids:
            logger.warning(f"  -> 检测到 {len(removed_tmdb_ids)} 个媒体已从合集 '{collection_name}' 中移除，正在清理其订阅来源...")
            
            # 准备要移除的来源信息字典，这个对于本次清理的所有媒体都是一样的
            source_to_remove = {
                "type": "collection", 
                "id": collection_id, 
                "name": collection_name
            }
            
            # 遍历所有被移除的媒体，并调用数据库函数执行清理
            for tmdb_id in removed_tmdb_ids:
                # 从我们之前保存的旧媒体 map 中获取它的 item_type
                item_type = old_media_map.get(tmdb_id)
                if item_type:
                    try:
                        request_db.remove_subscription_source(tmdb_id, item_type, source_to_remove)
                    except Exception as e_remove:
                        logger.error(f"  -> 清理媒体 {tmdb_id} ({item_type}) 的来源时发生错误: {e_remove}", exc_info=True)
    
    total_missing = len(missing_released_items) + len(missing_unreleased_items)
    return {
        "health_status": "has_missing" if total_missing > 0 else "ok", 
        "missing_count": total_missing, 
        "generated_media_info_json": json.dumps(tmdb_items, ensure_ascii=False)
    }

# --- 精准权限检查辅助函数 ---
def _check_user_access_batch(base_url: str, api_key: str, user_id: str, item_ids: List[str]) -> Set[str]:
    """
    精准查询：检查指定用户对一组特定 Item ID 的访问权限。
    比查询用户全量权限快得多。
    """
    if not item_ids:
        return set()
    
    # 如果 ID 数量太多（例如超过 200），URL 可能会过长，分批处理
    chunk_size = 150
    accessible_ids = set()
    
    # 简单的分批逻辑
    for i in range(0, len(item_ids), chunk_size):
        chunk = item_ids[i:i + chunk_size]
        ids_str = ",".join(chunk)
        
        # 构造请求：只查询这些 ID，且只返回 Id 字段
        url = f"{base_url}/Users/{user_id}/Items"
        params = {
            'Ids': ids_str,
            'Fields': 'Id',
            'Recursive': 'true'
        }
        headers = {'X-Emby-Token': api_key}
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Emby 返回的 Items 列表就是该用户能看到的那些
                for item in data.get('Items', []):
                    accessible_ids.add(item['Id'])
            else:
                logger.warning(f"查询用户 {user_id} 权限失败: {response.status_code}")
        except Exception as e:
            logger.error(f"查询用户 {user_id} 权限时出错: {e}")
            
    return accessible_ids

# ★★★ 刷新合集的后台任务函数 ★★★
def task_refresh_collections(processor):
    """
    后台任务：启动原生合集扫描。
    职责：只负责调用 handler 层的总指挥函数。
    """
    task_name = "刷新原生合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (新架构) ---")
    try:
        task_manager.update_status_from_thread(0, "正在扫描原生合集并检查缺失...")
        
        # ★★★ 核心修正：所有复杂逻辑都已封装到 handler 中 ★★★
        collections.sync_and_subscribe_native_collections()
        
        task_manager.update_status_from_thread(100, "原生合集扫描与订阅任务完成。")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")
    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 用于统一处理自定义合集的角标逻辑 ★★★
def _get_cover_badge_text_for_collection(collection_db_info: Dict[str, Any]) -> Any:
    """
    根据自定义合集的数据库信息，智能判断并返回用于封面角标的参数。
    - 如果是特定类型的榜单，返回对应的中文字符串。
    - 否则，返回该合集在媒体库中实际包含的项目数量。
    """
    # 默认情况下，角标就是媒体库中的项目数量
    item_count_to_pass = collection_db_info.get('in_library_count', 0)
    
    collection_type = collection_db_info.get('type')
    definition = collection_db_info.get('definition_json', {})

    # 只有榜单(list)类型才需要特殊处理角标文字
    if collection_type == 'list':
        url = definition.get('url', '')
        # 根据URL或其他特征判断榜单来源
        if url.startswith('maoyan://'):
            return '猫眼'
        elif 'douban.com/doulist' in url:
            return '豆列'
        elif 'themoviedb.org/discover/' in url:
            return '探索'
        else:
            # 对于其他所有榜单类型，统一显示为'榜单'
            return '榜单'
            
    # 如果不是榜单类型，或者榜单类型不匹配任何特殊规则，则返回数字角标
    return item_count_to_pass

# --- 可复用的权限更新函数 ---
def update_user_permissions_for_collection(collection_id: int, global_ordered_emby_ids: list, user_permissions_map: dict):
    """
    为单个自定义合集，计算所有用户的专属可见媒体列表，并批量更新到 user_collection_cache 表。
    这是一个可被多处调用的独立、可复用函数。
    """
    logger.debug(f"  ➜ 正在为合集ID {collection_id} 计算并更新用户权限缓存...")
    if not user_permissions_map:
        logger.warning("  ➜ 未提供用户权限映射，跳过权限计算。")
        return

    user_collection_cache_data_to_upsert = []
    for user_id, permission_set in user_permissions_map.items():
        # 计算交集，得到该用户在此合集中可见的媒体ID
        visible_emby_ids = [emby_id for emby_id in global_ordered_emby_ids if emby_id in permission_set]
        user_collection_cache_data_to_upsert.append({
            "user_id": user_id,
            "collection_id": collection_id,
            "visible_emby_ids_json": json.dumps(visible_emby_ids),
            "total_count": len(visible_emby_ids),
        })

    if not user_collection_cache_data_to_upsert:
        logger.debug(f"  ➜ 合集ID {collection_id} 无需更新任何用户权限缓存。")
        return

    # 批量写入数据库
    with connection.get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cols = ["user_id", "collection_id", "visible_emby_ids_json", "total_count"]
            cols_str = ", ".join(cols)
            placeholders_str = ", ".join([f"%({k})s" for k in cols])
            update_cols = [f"{col} = EXCLUDED.{col}" for col in cols]
            update_str = ", ".join(update_cols)
            sql = f"""
                INSERT INTO user_collection_cache ({cols_str}, last_updated_at)
                VALUES ({placeholders_str}, NOW())
                ON CONFLICT (user_id, collection_id) DO UPDATE SET {update_str}, last_updated_at = NOW()
            """
            from psycopg2.extras import execute_batch
            execute_batch(cursor, sql, user_collection_cache_data_to_upsert)
            conn.commit()
            logger.info(f"  ✅ 成功为 {len(user_permissions_map)} 个用户更新了合集ID {collection_id} 的权限缓存。")
        except Exception as e_db:
            logger.error(f"批量写入用户合集权限缓存 (合集ID: {collection_id}) 时发生数据库错误: {e_db}", exc_info=True)
            conn.rollback()

# ★★★ 一键生成所有合集的后台任务 ★★★
def task_process_all_custom_collections(processor):
    """
    【V6 - 批量任务修复版】一键生成所有合集的后台任务。
    修复了 emby_id 透传丢失和组合键匹配的问题。
    """
    task_name = "生成所有自建合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # 1. 获取用户列表 (优先查本地 DB)
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户...")
        all_emby_users = collection_db.get_all_local_emby_users()
        if not all_emby_users:
            logger.info("  ➜ 本地数据库未找到用户数据，回退到 Emby API 获取...")
            all_emby_users = emby.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
        if not all_emby_users: raise RuntimeError("无法从Emby获取用户列表")

        # 2. 获取合集定义
        task_manager.update_status_from_thread(10, "正在获取所有启用的合集定义...")
        active_collections = collection_db.get_all_active_custom_collections()
        if not active_collections:
            task_manager.update_status_from_thread(100, "没有已启用的合集。")
            return

        # 3. 加载全量映射 (带类型)
        task_manager.update_status_from_thread(12, "正在从本地数据库加载全量媒体映射...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        # 获取全量映射 (Key 是 "id_type")
        tmdb_to_emby_item_map = media_db.get_tmdb_to_emby_map(library_ids=libs_to_process_ids)
        
        # 4. 获取现有合集列表 (用于增量更新判断)
        task_manager.update_status_from_thread(15, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}

        # 5. 初始化封面生成器
        cover_service = None
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
        except Exception: pass

        total_collections = len(active_collections)

        # ======================================================================
        # 遍历所有合集
        # ======================================================================
        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested(): break

            collection_id = collection['id']
            collection_name = collection['name']
            progress = 20 + int((i / total_collections) * 75)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_collections}) 正在计算: {collection_name}")

            try:
                definition = collection['definition_json']
                
                # --- A. 计算并标准化媒体列表 ---
                raw_tmdb_items = []
                if collection['type'] == 'list':
                    importer = ListImporter(processor.tmdb_api_key)
                    raw_tmdb_items, _ = importer.process(definition)
                elif collection['type'] == 'filter':
                    engine = FilterEngine()
                    raw_tmdb_items = engine.execute_filter(definition)

                raw_tmdb_items, corrected_id_to_original_id_map = _apply_id_corrections(raw_tmdb_items, definition, collection_name)
                
                tmdb_items = [
                    {
                        'tmdb_id': str(item.get('id')),
                        'media_type': item.get('type'),
                        'emby_id': item.get('emby_id'), 
                        **({'season': item['season']} if 'season' in item and item.get('season') is not None else {})
                    }
                    for item in raw_tmdb_items
                ]

                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未生成任何媒体ID，跳过。")
                    continue

                # --- B. 映射 Emby ID (适配组合键) ---
                global_ordered_emby_ids = []
                for item in tmdb_items:
                    # 1. 优先用 FilterEngine 自带的 (筛选类合集)
                    if item.get('emby_id'):
                        global_ordered_emby_ids.append(item['emby_id'])
                    else:
                        # 2. 查全量映射表 (榜单类合集) - ★★★ 使用组合键 ★★★
                        key = f"{item['tmdb_id']}_{item['media_type']}"
                        if key in tmdb_to_emby_item_map:
                            global_ordered_emby_ids.append(tmdb_to_emby_item_map[key]['Id'])

                # --- C. 更新 Emby 合集 ---
                emby_collection_id = emby.create_or_update_collection_with_emby_ids(
                    collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
                    base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
                    prefetched_collection_map=prefetched_collection_map
                )

                # --- D. 按需计算用户权限 ---
                current_collection_permissions = {}
                if not global_ordered_emby_ids:
                    for user in all_emby_users:
                        current_collection_permissions[user['Id']] = set()
                else:
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        future_to_user = {}
                        for user in all_emby_users:
                            user_id = user['Id']
                            is_admin = user.get('Policy', {}).get('IsAdministrator', False)
                            
                            if is_admin:
                                current_collection_permissions[user_id] = set(global_ordered_emby_ids)
                            else:
                                future = executor.submit(
                                    _check_user_access_batch, 
                                    processor.emby_url, 
                                    processor.emby_api_key, 
                                    user_id, 
                                    global_ordered_emby_ids
                                )
                                future_to_user[future] = user
                        
                        for future in as_completed(future_to_user):
                            user = future_to_user[future]
                            try:
                                current_collection_permissions[user['Id']] = future.result()
                            except Exception as e:
                                current_collection_permissions[user['Id']] = set()

                update_user_permissions_for_collection(collection_id, global_ordered_emby_ids, current_collection_permissions)

                # --- E. 更新数据库状态 ---
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc),
                    "in_library_count": len(global_ordered_emby_ids),
                }

                if collection['type'] == 'list':
                    health_check_results = _perform_list_collection_health_check(
                        tmdb_items=tmdb_items,
                        tmdb_to_emby_item_map=tmdb_to_emby_item_map, 
                        corrected_id_to_original_id_map=corrected_id_to_original_id_map,
                        collection_db_record=collection,
                        tmdb_api_key=processor.tmdb_api_key
                    )
                    update_data.update(health_check_results)
                else:
                    update_data.update({
                        "health_status": "ok", 
                        "missing_count": 0,
                        "generated_media_info_json": json.dumps(tmdb_items, ensure_ascii=False)
                    })

                collection_db.update_custom_collection_sync_results(collection_id, update_data)

                # --- F. 封面生成 ---
                if cover_service and emby_collection_id:
                    try:
                        library_info = emby.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                        if library_info:
                            latest_collection_info = collection_db.get_custom_collection_by_id(collection_id)
                            item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                            cover_service.generate_for_library(
                                emby_server_id='main_emby', library=library_info,
                                item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                            )
                    except Exception as e_cover:
                        logger.error(f"为合集 '{collection_name}' 生成封面时出错: {e_cover}", exc_info=True)

                if collection['type'] == 'list' and collection['definition_json'].get('url', '').startswith('maoyan://'):
                    time.sleep(10)
                
            except Exception as e_coll:
                logger.error(f"处理合集 '{collection_name}' (ID: {collection_id}) 时发生错误: {e_coll}", exc_info=True)
                continue
        
        final_message = "所有自建合集均已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 处理单个自定义合集的核心任务 ---
def process_single_custom_collection(processor, custom_collection_id: int):
    """
    - 【极致优化版】处理单个自定义合集的核心任务
    - 优化点：权限计算后移，管理员免查，普通用户按需查。
    """
    task_name = f"生成单个自建合集 (ID: {custom_collection_id})"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ------------------------------------------------------------------
        # 步骤 1: 获取用户列表 (从本地 DB)
        # ------------------------------------------------------------------
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户...")
        all_emby_users = collection_db.get_all_local_emby_users()
        
        if not all_emby_users:
            logger.info("  ➜ 本地数据库未找到用户数据，回退到 Emby API 获取...")
            all_emby_users = emby.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
            
        if not all_emby_users: raise RuntimeError("无法获取用户列表")
        
        logger.info(f"  ➜ 成功从本地数据库加载了 {len(all_emby_users)} 个用户。")

        # ★★★ 注意：这里不再预先计算所有用户的全量权限，直接跳过耗时的步骤 ★★★

        # ------------------------------------------------------------------
        # 步骤 2: 读取合集定义
        # ------------------------------------------------------------------
        task_manager.update_status_from_thread(10, "正在读取合集定义...")
        collection = collection_db.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        collection_name = collection['name']
        
        # ------------------------------------------------------------------
        # 步骤 3: 计算媒体列表
        # ------------------------------------------------------------------
        task_manager.update_status_from_thread(20, f"正在为《{collection_name}》计算媒体列表...")
        definition = collection['definition_json']
        
        # 3.1 获取原始数据
        raw_tmdb_items = []
        if collection['type'] == 'list':
            importer = ListImporter(processor.tmdb_api_key)
            raw_tmdb_items, _ = importer.process(definition)
        elif collection['type'] == 'filter':
            engine = FilterEngine()
            raw_tmdb_items = engine.execute_filter(definition)

        # 3.2 应用修正
        raw_tmdb_items, corrected_id_to_original_id_map = _apply_id_corrections(raw_tmdb_items, definition, collection_name)
        
        tmdb_items = [
            {
                'tmdb_id': str(item.get('id')),
                'media_type': item.get('type'),
                'emby_id': item.get('emby_id'),
                **({'season': item['season']} if 'season' in item and item.get('season') is not None else {})
            }
            for item in raw_tmdb_items
        ]

        if not tmdb_items:
            collection_db.update_custom_collection_sync_results(custom_collection_id, {"emby_collection_id": None})
            task_manager.update_status_from_thread(100, "该合集未匹配到任何媒体。")
            return

        # ------------------------------------------------------------------
        # 步骤 4: 映射到 Emby ID
        # ------------------------------------------------------------------
        # 找出哪些项目还缺 Emby ID
        # 1. 找出哪些项目还缺 Emby ID (主要是榜单类合集)
        items_needing_lookup = [
            item for item in tmdb_items 
            if not item.get('emby_id')
        ]
        
        lookup_map = {}
        if items_needing_lookup:
            # ★★★ 调用修正后的函数 ★★★
            lookup_map = media_db.get_emby_ids_for_items(items_needing_lookup)
            
        global_ordered_emby_ids = []
        for item in tmdb_items:
            # 情况 A: FilterEngine 直接给出的
            if item.get('emby_id'):
                global_ordered_emby_ids.append(item['emby_id'])
            
            # 情况 B: 榜单类，查库找到的
            else:
                # ★★★ 使用组合键查找 ★★★
                key = f"{item['tmdb_id']}_{item['media_type']}"
                if key in lookup_map:
                    global_ordered_emby_ids.append(lookup_map[key]['Id'])

        # ------------------------------------------------------------------
        # 步骤 5: 在 Emby 中创建/更新合集
        # ------------------------------------------------------------------
        task_manager.update_status_from_thread(60, "正在Emby中创建/更新合集...")
        emby_collection_id = emby.create_or_update_collection_with_emby_ids(
            collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id
        )

        # ------------------------------------------------------------------
        # 步骤 6: 【优化核心】按需计算用户权限
        # ------------------------------------------------------------------
        task_manager.update_status_from_thread(80, "正在智能更新用户权限缓存...")
        
        user_permissions_map = {}
        
        # 如果合集是空的，所有用户都看空列表，不需要查 API
        if not global_ordered_emby_ids:
            for user in all_emby_users:
                user_permissions_map[user['Id']] = set()
        else:
            # 并发处理权限检查
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_user = {}
                
                for user in all_emby_users:
                    user_id = user['Id']
                    is_admin = user.get('Policy', {}).get('IsAdministrator', False)
                    
                    if is_admin:
                        # ★★★ 优化 A: 管理员直接拥有全部权限，无需 API 请求 ★★★
                        logger.debug(f"    -> 用户 '{user['Name']}' 是管理员，自动授予全部权限。")
                        user_permissions_map[user_id] = set(global_ordered_emby_ids)
                    else:
                        # ★★★ 优化 B: 普通用户只查这几十个 ID 的权限 ★★★
                        future = executor.submit(
                            _check_user_access_batch, 
                            processor.emby_url, 
                            processor.emby_api_key, 
                            user_id, 
                            global_ordered_emby_ids
                        )
                        future_to_user[future] = user

                for future in as_completed(future_to_user):
                    user = future_to_user[future]
                    try:
                        allowed_ids = future.result()
                        user_permissions_map[user['Id']] = allowed_ids
                    except Exception as e:
                        logger.error(f"检查用户 '{user['Name']}' 的权限时出错: {e}")
                        # 出错时保守处理：认为无权限
                        user_permissions_map[user['Id']] = set()

        update_user_permissions_for_collection(custom_collection_id, global_ordered_emby_ids, user_permissions_map)

        # ------------------------------------------------------------------
        # 步骤 7: 更新数据库状态 & 封面生成 (保持不变)
        # ------------------------------------------------------------------
        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(definition.get('item_type', ['Movie'])),
            "last_synced_at": datetime.now(pytz.utc),
            "in_library_count": len(global_ordered_emby_ids),
        }

        if collection['type'] == 'list':
            health_check_results = _perform_list_collection_health_check(
                tmdb_items=tmdb_items,
                tmdb_to_emby_item_map=lookup_map,
                corrected_id_to_original_id_map=corrected_id_to_original_id_map,
                collection_db_record=collection,
                tmdb_api_key=processor.tmdb_api_key
            )
            update_data.update(health_check_results)
        else:
            update_data.update({
                "health_status": "ok", 
                "missing_count": 0,
                "generated_media_info_json": json.dumps(tmdb_items, ensure_ascii=False)
            })

        collection_db.update_custom_collection_sync_results(custom_collection_id, update_data)

        # 封面生成逻辑
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled") and emby_collection_id:
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    latest_collection_info = collection_db.get_custom_collection_by_id(custom_collection_id)
                    item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                    cover_service.generate_for_library(
                        emby_server_id='main_emby', library=library_info,
                        item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                    )
        except Exception as e_cover:
            logger.error(f"为合集 '{collection_name}' 生成封面时发生错误: {e_cover}", exc_info=True)
        
        task_manager.update_status_from_thread(100, "单个自定义合集同步完成！")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
