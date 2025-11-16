# tasks/collections.py
# 原生合集与自建合集任务模块

import json
import logging
import pytz
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

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
    榜单健康检查
    """
    collection_id = collection_db_record.get('id')
    collection_name = collection_db_record.get('name', '未知合集')
    logger.info(f"  ➜ 榜单合集 '{collection_name}'，开始进行健康度分析...")

    # 获取上一次同步时生成的媒体列表 
    old_media_map = {}
    # 从数据库记录中获取历史数据
    historical_data = collection_db_record.get('generated_media_info_json')
    
    if historical_data:
        try:
            old_items = []
            # ★★★ 核心修正：检查数据类型 ★★★
            # 如果是字符串，说明是旧格式或者意外情况，我们手动解析
            if isinstance(historical_data, str):
                old_items = json.loads(historical_data)
            # 如果已经是列表，说明数据库驱动已经帮我们解析好了，直接用
            elif isinstance(historical_data, list):
                old_items = historical_data
            
            if old_items:
                # 我们需要 TMDB ID 和 item_type 来执行清理
                old_media_map = {str(item['tmdb_id']): item['media_type'] for item in old_items}

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            # 这里的日志现在能更精确地反映问题
            logger.warning(f"  -> 解析合集 '{collection_name}' 的历史媒体列表时失败或格式不兼容: {e}，将跳过来源清理。")

    # 提前加载所有在库的“季”的信息，用于快速比对
    in_library_seasons_set = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT parent_series_tmdb_id, season_number FROM media_metadata WHERE item_type = 'Season' AND in_library = TRUE")
            for row in cursor.fetchall():
                in_library_seasons_set.add((row['parent_series_tmdb_id'], row['season_number']))
    except Exception as e_db:
        logger.error(f"  -> 获取在库季列表时发生数据库错误: {e_db}", exc_info=True)

    in_library_top_level_tmdb_ids = set(tmdb_to_emby_item_map.keys())
    missing_released_items = []
    missing_unreleased_items = []
    # 新增一个列表，专门存放需要确保存在的父剧集信息 
    parent_series_to_ensure_exist = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for item_def in tmdb_items:
        tmdb_id = str(item_def.get('tmdb_id'))
        media_type = item_def.get('media_type')
        season_num = item_def.get('season')

        is_in_library = False
        if season_num is not None and media_type == 'Series':
            if (tmdb_id, season_num) in in_library_seasons_set:
                is_in_library = True
        else:
            original_id = corrected_id_to_original_id_map.get(tmdb_id, tmdb_id)
            if tmdb_id in in_library_top_level_tmdb_ids or original_id in in_library_top_level_tmdb_ids:
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

# ✨ 辅助函数，并发刷新合集使用
def _process_single_collection_concurrently(collection_data: dict, tmdb_api_key: str) -> dict:
    """
    【V7 - 增加垃圾数据过滤器】
    """
    collection_id = collection_data['Id']
    collection_name = collection_data.get('Name', '')
    today_str = datetime.now().strftime('%Y-%m-%d')
    item_type = 'Movie'
    
    emby_movie_tmdb_ids = {str(id) for id in collection_data.get("ExistingMovieTmdbIds", [])}
    
    in_library_count = len(emby_movie_tmdb_ids)
    status, has_missing = "ok", False
    provider_ids = collection_data.get("ProviderIds", {})
    all_movies_with_status = []
    
    tmdb_id = provider_ids.get("TmdbCollection") or provider_ids.get("TmdbCollectionId") or provider_ids.get("Tmdb")

    if not tmdb_id:
        status = "unlinked"
    else:
        details = tmdb.get_collection_details(int(tmdb_id), tmdb_api_key)
        if not details or "parts" not in details:
            status = "tmdb_error"
        else:
            previous_movies_map = {}
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = %s", (collection_id,))
                row = cursor.fetchone()
                if row and row.get('missing_movies_json'):
                    try:
                        previous_movies_map = {str(m['tmdb_id']): m for m in row['missing_movies_json']}
                    except (TypeError, KeyError): 
                        logger.warning(f"解析合集 '{collection_name}' 的历史数据时格式不兼容，将忽略。")
            
            for movie in details.get("parts", []):
                # ★★★ 核心过滤点 3/3: 在这个辅助函数中也加入过滤器 ★★★
                if not movie.get("release_date") or not movie.get("poster_path"): 
                    continue

                movie_tmdb_id = str(movie.get("id"))
                
                movie_status = "unknown"
                if movie_tmdb_id in emby_movie_tmdb_ids:
                    movie_status = "in_library"
                elif movie.get("release_date", '') > today_str:
                    movie_status = "unreleased"
                elif previous_movies_map.get(movie_tmdb_id, {}).get('status') == 'subscribed':
                    movie_status = "subscribed"
                else:
                    movie_status = "missing"

                all_movies_with_status.append({
                    "tmdb_id": movie_tmdb_id, "title": movie.get("title", ""), 
                    "release_date": movie.get("release_date"), "poster_path": movie.get("poster_path"), 
                    "status": movie_status
                })
            
            if any(m['status'] == 'missing' for m in all_movies_with_status):
                has_missing = True
                status = "has_missing"

    image_tag = collection_data.get("ImageTags", {}).get("Primary")
    poster_path = f"/Items/{collection_id}/Images/Primary?tag={image_tag}" if image_tag else None

    return {
        "emby_collection_id": collection_id, "name": collection_name, 
        "tmdb_collection_id": tmdb_id, "item_type": item_type,
        "status": status, "has_missing": has_missing, 
        "missing_movies_json": json.dumps(all_movies_with_status, ensure_ascii=False), 
        "last_checked_at": datetime.now(timezone.utc), 
        "poster_path": poster_path, 
        "in_library_count": in_library_count
    }

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
    - 【最终修正版】一键生成所有合集的后台任务。
    """
    task_name = "生成所有自建合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # ... (步骤 1 & 2: 获取权限和合集数据 - 不变) ...
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户及权限...")
        all_emby_users = emby.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
        if not all_emby_users: raise RuntimeError("无法从Emby获取用户列表")
        
        user_permissions_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {executor.submit(emby.get_all_accessible_item_ids_for_user_optimized, processor.emby_url, processor.emby_api_key, user['Id']): user for user in all_emby_users}
            for future in as_completed(future_to_user):
                user = future_to_user[future]
                try:
                    permission_set = future.result()
                    if permission_set is not None: user_permissions_map[user['Id']] = permission_set
                except Exception as e: logger.error(f"为用户 '{user['Name']}' 获取权限时出错: {e}")
        
        task_manager.update_status_from_thread(10, "正在获取所有启用的合集定义...")
        active_collections = collection_db.get_all_active_custom_collections()
        if not active_collections:
            task_manager.update_status_from_thread(100, "没有已启用的合集。")
            return

        total_collections = len(active_collections)
        task_manager.update_status_from_thread(12, "正在从Emby获取全库媒体数据...")
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        all_emby_items = emby.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        task_manager.update_status_from_thread(15, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}

        cover_service = None
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
                logger.trace("  ➜ 封面生成器已启用。")
        except Exception as e_cover_init:
            logger.error(f"初始化封面生成器时失败: {e_cover_init}", exc_info=True)

        # ======================================================================
        # 步骤 3: 遍历所有合集，执行核心逻辑
        # ======================================================================
        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested(): break

            collection_id = collection['id']
            collection_name = collection['name']
            progress = 20 + int((i / total_collections) * 75)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_collections}) 正在计算: {collection_name}")

            try:
                definition = collection['definition_json']
                
                # --- A. 计算并【立即标准化】媒体列表 ---
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
                        # ★★★ 核心修正：将所有源头获取的ID强制转换为字符串 ★★★
                        'tmdb_id': str(item.get('id')),
                        'media_type': item.get('type'),
                        # 使用字典解包的技巧，如果 'season' 存在，就把它加进去
                        **({'season': item['season']} if 'season' in item and item.get('season') is not None else {})
                    }
                    for item in raw_tmdb_items
                ]

                if not tmdb_items:
                    logger.warning(f"合集 '{collection_name}' 未生成任何媒体ID，跳过。")
                    continue

                global_ordered_emby_ids = [tmdb_to_emby_item_map[item['tmdb_id']]['Id'] for item in tmdb_items if item['tmdb_id'] in tmdb_to_emby_item_map]

                emby_collection_id = emby.create_or_update_collection_with_emby_ids(
                    collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
                    base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
                    prefetched_collection_map=prefetched_collection_map
                )

                update_user_permissions_for_collection(collection_id, global_ordered_emby_ids, user_permissions_map)

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

                # ... (后续封面生成和延时逻辑 - 不变) ...
                
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
    - 【最终修正版】处理单个自定义合集的核心任务
    """
    task_name = f"生成单个自建合集 (ID: {custom_collection_id})"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # ... (步骤 1 & 2: 获取用户权限和合集定义 - 这部分代码不变)
        task_manager.update_status_from_thread(0, "正在获取所有Emby用户及权限...")
        all_emby_users = emby.get_all_emby_users_from_server(processor.emby_url, processor.emby_api_key)
        if not all_emby_users: raise RuntimeError("无法获取用户列表")
        
        user_permissions_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {executor.submit(emby.get_all_accessible_item_ids_for_user_optimized, processor.emby_url, processor.emby_api_key, user['Id']): user for user in all_emby_users}
            for future in as_completed(future_to_user):
                user = future_to_user[future]
                try:
                    permission_set = future.result()
                    if permission_set is not None: user_permissions_map[user['Id']] = permission_set
                except Exception as e: logger.error(f"为用户 '{user['Name']}' 获取权限时出错: {e}")
        
        task_manager.update_status_from_thread(20, "正在读取合集定义...")
        collection = collection_db.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        collection_name = collection['name']
        
        # ======================================================================
        # 步骤 3: 计算媒体列表并【立即标准化】
        # ======================================================================
        task_manager.update_status_from_thread(30, f"正在为《{collection_name}》计算媒体列表...")
        definition = collection['definition_json']
        
        # 3.1 获取原始数据 (键名为 id, type)
        raw_tmdb_items = []
        if collection['type'] == 'list':
            importer = ListImporter(processor.tmdb_api_key)
            raw_tmdb_items, _ = importer.process(definition)
        elif collection['type'] == 'filter':
            engine = FilterEngine()
            raw_tmdb_items = engine.execute_filter(definition)

        # 3.2 应用修正 (修正会保留 id, type 键名)
        raw_tmdb_items, corrected_id_to_original_id_map = _apply_id_corrections(raw_tmdb_items, definition, collection_name)
        
        tmdb_items = [
            {
                # ★★★ 核心修正：将所有源头获取的ID强制转换为字符串 ★★★
                'tmdb_id': str(item.get('id')),
                'media_type': item.get('type'),
                # ...
            }
            for item in raw_tmdb_items
        ]

        if not tmdb_items:
            collection_db.update_custom_collection_sync_results(custom_collection_id, {"emby_collection_id": None})
            task_manager.update_status_from_thread(100, "该合集未匹配到任何媒体。")
            return

        # ... (后续步骤使用标准化的 tmdb_items)
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        all_emby_items = emby.get_emby_library_items(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, media_type_filter="Movie,Series", library_ids=libs_to_process_ids) or []
        tmdb_to_emby_item_map = {item['ProviderIds']['Tmdb']: item for item in all_emby_items if item.get('ProviderIds', {}).get('Tmdb')}
        
        # 在这里，我们使用 tmdb_items 里的 'tmdb_id' 键
        global_ordered_emby_ids = [tmdb_to_emby_item_map[item['tmdb_id']]['Id'] for item in tmdb_items if item['tmdb_id'] in tmdb_to_emby_item_map]

        task_manager.update_status_from_thread(70, "正在Emby中创建/更新合集...")
        emby_collection_id = emby.create_or_update_collection_with_emby_ids(
            collection_name=collection_name, emby_ids_in_library=global_ordered_emby_ids, 
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id
        )

        task_manager.update_status_from_thread(90, "正在为所有用户更新此合集的权限缓存...")
        update_user_permissions_for_collection(custom_collection_id, global_ordered_emby_ids, user_permissions_map)

        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(definition.get('item_type', ['Movie'])),
            "last_synced_at": datetime.now(pytz.utc),
            "in_library_count": len(global_ordered_emby_ids),
        }

        if collection['type'] == 'list':
            # 将【标准化的 tmdb_items】传入健康检查函数，这样就不会再有 KeyError
            health_check_results = _perform_list_collection_health_check(
                tmdb_items=tmdb_items,
                tmdb_to_emby_item_map=tmdb_to_emby_item_map,
                corrected_id_to_original_id_map=corrected_id_to_original_id_map,
                collection_db_record=collection,
                tmdb_api_key=processor.tmdb_api_key
            )
            update_data.update(health_check_results)
        else:
            # 对于筛选合集，也保存标准化的列表
            update_data.update({
                "health_status": "ok", 
                "missing_count": 0,
                "generated_media_info_json": json.dumps(tmdb_items, ensure_ascii=False)
            })

        collection_db.update_custom_collection_sync_results(custom_collection_id, update_data)

        # ... (步骤 6: 封面生成 - 不变) ...
        
        task_manager.update_status_from_thread(100, "单个自定义合集同步完成！")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")
