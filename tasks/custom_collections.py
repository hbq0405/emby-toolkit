# tasks/custom_collections.py
# 自建合集任务模块 (V5 - 实时架构适配版)

import json
import logging
import pytz
import time
import random
from datetime import datetime
from typing import Dict, Any, List, Set

# 导入需要的底层模块和共享实例
import handler.emby as emby
import task_manager
import handler.tmdb as tmdb
from database import connection, custom_collection_db, settings_db, media_db, request_db, queries_db
from handler.custom_collection import ListImporter
from services.cover_generator import CoverGeneratorService
from handler.poster_generator import cleanup_placeholder, sync_all_subscription_posters
import constants
import config_manager

logger = logging.getLogger(__name__)

# 辅助函数应用修正
def _apply_id_corrections(tmdb_items: list, definition: dict, collection_name: str) -> tuple[list, dict]:
    """
    应用合集定义中的修正规则 (支持 ID 修正和 标题 修正)。
    """
    corrections = definition.get('corrections', {})
    corrected_id_to_original_id_map = {}
    
    if corrections:
        logger.info(f"  -> 检测到合集 '{collection_name}' 存在 {len(corrections)} 条修正规则，正在应用...")
        
        for item in tmdb_items:
            original_id_str = str(item.get('id')) if item.get('id') else None
            original_title = item.get('title')
            
            correction_found = None
            
            # 1. 优先尝试 ID 匹配
            if original_id_str and original_id_str in corrections:
                correction_found = corrections[original_id_str]
            # 2. 如果没有 ID 匹配，尝试 标题 匹配
            elif original_title:
                title_key = f"title:{original_title}"
                if title_key in corrections:
                    correction_found = corrections[title_key]

            # 3. 应用修正
            if correction_found:
                new_id = None
                new_season = None
                
                if isinstance(correction_found, dict):
                    new_id = correction_found.get('tmdb_id')
                    new_season = correction_found.get('season')
                else:
                    new_id = correction_found
                
                if new_id:
                    item['id'] = new_id
                    if original_id_str:
                        corrected_id_to_original_id_map[str(new_id)] = original_id_str
                
                if new_season is not None:
                    item['season'] = new_season

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
    榜单健康检查 (仅用于 List 类型)
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
                old_media_map = {str(item['tmdb_id']): item['media_type'] for item in old_items if item.get('tmdb_id')}
        except Exception as e:
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

    # 获取所有在库的 Key 集合 (格式: id_type)
    in_library_keys = set(tmdb_to_emby_item_map.keys())

    subscribed_or_paused_keys = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id, item_type FROM media_metadata WHERE subscription_status IN ('SUBSCRIBED', 'PAUSED', 'WANTED', 'IGNORED', 'PENDING_RELEASE')")
            for row in cursor.fetchall():
                subscribed_or_paused_keys.add(f"{row['tmdb_id']}_{row['item_type']}")
    except Exception as e_sub:
        logger.error(f"  -> 获取订阅状态时发生数据库错误: {e_sub}", exc_info=True)
    
    missing_released_items = []
    missing_unreleased_items = []
    parent_series_to_ensure_exist = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for item_def in tmdb_items:
        tmdb_id = str(item_def.get('tmdb_id'))
        media_type = item_def.get('media_type')
        season_num = item_def.get('season')

        is_in_library = False
        
        if item_def.get('emby_id'):
            is_in_library = True
        elif season_num is not None and media_type == 'Series':
            if (tmdb_id, season_num) in in_library_seasons_set:
                is_in_library = True
        else:
            current_key = f"{tmdb_id}_{media_type}"
            if current_key in in_library_keys:
                is_in_library = True
            else:
                original_id = corrected_id_to_original_id_map.get(tmdb_id)
                if original_id:
                    original_key = f"{original_id}_{media_type}"
                    if original_key in in_library_keys:
                        is_in_library = True
        
        if is_in_library:
            continue

        check_sub_key = f"{tmdb_id}_{media_type}"
        if check_sub_key in subscribed_or_paused_keys:
            continue
        
        if not tmdb_id or tmdb_id == 'None':
            continue

        try:
            details = None
            # 标记是否需要将该条目本身加入待订阅列表
            # 默认为 True，如果是整剧拆解模式，则设为 False (因为我们会添加具体的季)
            add_self_to_missing_list = True 

            if season_num is not None and media_type == 'Series':
                # ... (原有单季逻辑保持不变) ...
                details = tmdb.get_tv_season_details(tmdb_id, season_num, tmdb_api_key)
                if details:
                    item_type_for_db = 'Season'
                    parent_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
                    details['parent_series_tmdb_id'] = tmdb_id
                    details['parent_title'] = parent_details.get('name', '')
                    details['parent_poster_path'] = parent_details.get('poster_path')

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
                # 获取详情
                details = tmdb.get_movie_details(tmdb_id, tmdb_api_key) if media_type == 'Movie' else tmdb.get_tv_details(tmdb_id, tmdb_api_key)
                
                # ★★★ 新增逻辑：如果是整剧，立刻拆解为季 ★★★
                if details and media_type == 'Series':
                    # 1. 既然我们要添加具体的季，就不需要订阅 Series 本体了
                    add_self_to_missing_list = False
                    
                    # 2. 确保父剧集元数据存在 (占位)
                    parent_series_to_ensure_exist.append({
                        'tmdb_id': tmdb_id,
                        'item_type': 'Series',
                        'title': details.get('name'),
                        'original_title': details.get('original_name'),
                        'release_date': details.get('first_air_date'),
                        'release_year': details.get('first_air_date', '----').split('-')[0],
                        'poster_path': details.get('poster_path')
                    })

                    # 3. 遍历所有季
                    seasons = details.get('seasons', [])
                    series_name = details.get('name')
                    series_poster = details.get('poster_path')
                    
                    for season in seasons:
                        s_num = season.get('season_number')
                        # 跳过特别篇 (Season 0) 和 已经在库的季
                        if s_num is None or s_num == 0: continue
                        if (tmdb_id, s_num) in in_library_seasons_set: continue
                        
                        # 4. 构建季的订阅请求
                        s_air_date = season.get('air_date')
                        s_poster = season.get('poster_path') or series_poster
                        
                        season_item_for_db = {
                            'tmdb_id': str(season.get('id')), # 季的 TMDb ID
                            'item_type': 'Season',
                            'title': season.get('name') or f"第 {s_num} 季",
                            'release_date': s_air_date,
                            'release_year': int(s_air_date.split('-')[0]) if s_air_date else None,
                            'overview': season.get('overview'),
                            'poster_path': s_poster,
                            'parent_series_tmdb_id': tmdb_id,
                            'season_number': s_num,
                            'source': { "type": "collection", "id": collection_db_record.get('id'), "name": collection_name }
                        }
                        
                        # 5. 分流：已上映 vs 未上映
                        if s_air_date and s_air_date > today_str:
                            missing_unreleased_items.append(season_item_for_db)
                        else:
                            missing_released_items.append(season_item_for_db)
                    
                    logger.info(f"  -> [智能拆解] 已将剧集《{series_name}》拆解为缺少的季进行订阅。")

            if not details: continue
            
            # 如果 add_self_to_missing_list 为 True，说明是电影或者单季，走原有逻辑
            if add_self_to_missing_list:
                release_date = details.get("air_date") or details.get("release_date") or details.get("first_air_date", '')
                
                release_year = None
                if release_date and '-' in release_date:
                    try:
                        release_year = int(release_date.split('-')[0])
                    except:
                        pass

                item_details_for_db = {
                    'tmdb_id': str(details.get('id')),
                    'item_type': item_type_for_db,
                    'title': details.get('name') or f"第 {season_num} 季" if item_type_for_db == 'Season' else details.get('title') or details.get('name'),
                    'release_date': release_date,
                    'release_year': release_year, 
                    'overview': details.get('overview'),
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

    if parent_series_to_ensure_exist:
        unique_parents = {p['tmdb_id']: p for p in parent_series_to_ensure_exist}.values()
        logger.info(f"  -> 检测到 {len(unique_parents)} 个缺失的父剧集元数据，正在创建占位记录...")
        request_db.set_media_status_none(
            tmdb_ids=[p['tmdb_id'] for p in unique_parents],
            item_type='Series',
            media_info_list=list(unique_parents)
        )

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

    if old_media_map:
        new_tmdb_ids = {str(item['tmdb_id']) for item in tmdb_items}
        removed_tmdb_ids = set(old_media_map.keys()) - new_tmdb_ids

        if removed_tmdb_ids:
            logger.warning(f"  -> 检测到 {len(removed_tmdb_ids)} 个媒体已从合集 '{collection_name}' 中移除，正在清理其订阅来源...")
            source_to_remove = {
                "type": "collection", 
                "id": collection_id, 
                "name": collection_name
            }
            for tmdb_id in removed_tmdb_ids:
                item_type = old_media_map.get(tmdb_id)
                if item_type:
                    try:
                        request_db.remove_subscription_source(tmdb_id, item_type, source_to_remove)
                        cleanup_placeholder(tmdb_id)
                    except Exception as e_remove:
                        logger.error(f"  -> 清理媒体 {tmdb_id} ({item_type}) 的来源时发生错误: {e_remove}", exc_info=True)
    return 

def _get_cover_badge_text_for_collection(collection_db_info: Dict[str, Any]) -> Any:
    """
    根据自定义合集的数据库信息，智能判断并返回用于封面角标的参数。
    """
    item_count_to_pass = collection_db_info.get('in_library_count', 0)
    collection_type = collection_db_info.get('type')
    definition = collection_db_info.get('definition_json', {})
    
    if collection_type == 'list':
        raw_url = definition.get('url', '')
        urls = raw_url if isinstance(raw_url, list) else [str(raw_url)]
        types_found = set()
        for u in urls:
            if not isinstance(u, str): continue
            if u.startswith('maoyan://'): types_found.add('猫眼')
            elif 'douban.com/doulist' in u: types_found.add('豆列')
            elif 'themoviedb.org/discover/' in u: types_found.add('探索')
            else: types_found.add('未知')

        if len(types_found) == 1 and '未知' not in types_found:
            return types_found.pop()
        else:
            if types_found == {'未知'}: return '榜单'
            return '混合'    
            
    if collection_type == 'ai_recommendation_global':
        return '热榜'
    if collection_type == 'ai_recommendation':
        return '推荐'
    
    return item_count_to_pass

# ★★★ 一键生成所有合集的后台任务 (重构版) ★★★
def task_process_all_custom_collections(processor):
    """
    一键生成所有合集的后台任务 (轻量化版 - 仅刷新外部数据源)。
    - 仅处理 List (榜单) 和 AI Recommendation Global (全局AI)。
    - 跳过 Filter (筛选) 和 AI Recommendation (个人AI)，因为它们是实时计算的，无需后台刷新。
    """
    task_name = "生成所有自建合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")

    try:
        # 1. 获取合集定义
        task_manager.update_status_from_thread(10, "正在获取所有启用的合集定义...")
        all_collections = custom_collection_db.get_all_active_custom_collections()
        
        # --- 过滤逻辑：只保留需要从外部获取数据的类型 ---
        target_types = {'list', 'ai_recommendation_global'}
        active_collections = [c for c in all_collections if c['type'] in target_types]
        
        skipped_count = len(all_collections) - len(active_collections)
        if skipped_count > 0:
            logger.info(f"  -> 已跳过 {skipped_count} 个本地筛选/个人AI类合集 (无需定时刷新)。")

        if not active_collections:
            task_manager.update_status_from_thread(100, "没有需要刷新的榜单或全局推荐合集。")
            return

        # 2. 加载全量映射 (用于匹配本地媒体)
        task_manager.update_status_from_thread(12, "正在从本地数据库加载全量媒体映射...")
        tmdb_to_emby_item_map = media_db.get_tmdb_to_emby_map(library_ids=None)
        
        # 3. 获取现有合集列表 (用于 Emby 实体合集同步)
        task_manager.update_status_from_thread(15, "正在从Emby获取现有合集列表...")
        all_emby_collections = emby.get_all_collections_from_emby_generic(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id) or []
        prefetched_collection_map = {coll.get('Name', '').lower(): coll for coll in all_emby_collections}

        # 4. 初始化封面生成器
        cover_service = None
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled"):
                cover_service = CoverGeneratorService(config=cover_config)
        except Exception: pass

        total_collections = len(active_collections)

        for i, collection in enumerate(active_collections):
            if processor.is_stop_requested(): break

            collection_id = collection['id']
            collection_name = collection['name']
            collection_type = collection['type']
            definition = collection['definition_json']
            
            progress = 20 + int((i / total_collections) * 75)
            task_manager.update_status_from_thread(progress, f"({i+1}/{total_collections}) 正在处理: {collection_name}")

            try:
                global_ordered_emby_ids = [] # 用于同步给 Emby 实体合集 (封面素材)
                items_for_db = []            # 用于存入 generated_media_info_json
                total_count = 0              # 用于角标

                # 榜单/推荐类 (List/AI Global) - 全量模式
                raw_tmdb_items = []
                if collection_type == 'list':
                    importer = ListImporter(processor.tmdb_api_key)
                    raw_tmdb_items, _ = importer.process(definition)
                else:
                    # ai_recommendation_global
                    from handler.custom_collection import RecommendationEngine
                    rec_engine = RecommendationEngine(processor.tmdb_api_key)
                    raw_tmdb_items = rec_engine.generate(definition)

                # 应用修正
                raw_tmdb_items, corrected_id_to_original_id_map = _apply_id_corrections(raw_tmdb_items, definition, collection_name)
                
                # 映射 Emby ID
                tmdb_items = []
                for item in raw_tmdb_items:
                    tmdb_id = str(item.get('id')) if item.get('id') else None
                    media_type = item.get('type')
                    emby_id = item.get('emby_id')
                    
                    if not emby_id and tmdb_id:
                        key = f"{tmdb_id}_{media_type}"
                        if key in tmdb_to_emby_item_map:
                            emby_id = tmdb_to_emby_item_map[key]['Id']
                    
                    processed_item = {
                        'tmdb_id': tmdb_id,
                        'media_type': media_type,
                        'emby_id': emby_id,
                        'title': item.get('title'),
                        **({'season': item['season']} if 'season' in item and item.get('season') is not None else {})
                    }
                    tmdb_items.append(processed_item)
                    
                    if emby_id:
                        global_ordered_emby_ids.append(emby_id)

                # 榜单/全局AI类需要全量存储，因为反向代理层无法实时爬虫
                items_for_db = tmdb_items
                total_count = len(global_ordered_emby_ids)

                # 执行健康检查 (榜单类和全局AI推荐都需要)
                # 作用：对比 TMDB 列表和本地库，自动订阅缺失的媒体
                if collection_type in ['list', 'ai_recommendation_global']:
                    _perform_list_collection_health_check(
                        tmdb_items=tmdb_items, 
                        tmdb_to_emby_item_map=tmdb_to_emby_item_map, 
                        corrected_id_to_original_id_map=corrected_id_to_original_id_map,
                        collection_db_record=collection,
                        tmdb_api_key=processor.tmdb_api_key
                    )

                # 后续处理
                # 1. 更新 Emby 实体合集 (用于封面)
                emby_collection_id = emby.create_or_update_collection_with_emby_ids(
                    collection_name=collection_name, 
                    emby_ids_in_library=global_ordered_emby_ids,
                    base_url=processor.emby_url, 
                    api_key=processor.emby_api_key, 
                    user_id=processor.emby_user_id,
                    prefetched_collection_map=prefetched_collection_map,
                    allow_empty=False 
                )

                # 2. 更新数据库状态
                update_data = {
                    "emby_collection_id": emby_collection_id,
                    "item_type": json.dumps(definition.get('item_type', ['Movie'])),
                    "last_synced_at": datetime.now(pytz.utc),
                    "in_library_count": total_count, # 保存真实总数
                    "generated_media_info_json": json.dumps(items_for_db, ensure_ascii=False)
                }
                custom_collection_db.update_custom_collection_sync_results(collection_id, update_data)

                # 3. 封面生成
                if cover_service and emby_collection_id:
                    try:
                        library_info = emby.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                        if library_info:
                            # 重新获取一次最新的 info 以确保 count 准确
                            latest_collection_info = custom_collection_db.get_custom_collection_by_id(collection_id)
                            item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                            cover_service.generate_for_library(
                                emby_server_id='main_emby', library=library_info,
                                item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                            )
                    except Exception as e_cover:
                        logger.error(f"为合集 '{collection_name}' 生成封面时出错: {e_cover}", exc_info=True)

                # 防封控休眠 (仅针对猫眼榜单)
                is_maoyan = False
                raw_url = definition.get('url', '')
                urls = raw_url if isinstance(raw_url, list) else [str(raw_url)]
                for u in urls:
                    if isinstance(u, str) and u.startswith('maoyan://'):
                        is_maoyan = True
                        break
                if collection_type == 'list' and is_maoyan:
                    time.sleep(10)
                
            except Exception as e_coll:
                logger.error(f"处理合集 '{collection_name}' (ID: {collection_id}) 时发生错误: {e_coll}", exc_info=True)
                continue
        
        final_message = "所有外部源合集(List/Global AI)均已处理完毕！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        
        try:
            if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_SHOW_MISSING_PLACEHOLDERS, False):
                sync_all_subscription_posters()
            else:
                logger.info("  ➜ 检测到占位海报功能已关闭，跳过海报同步。")
        except Exception as e:
            logger.error(f"全量同步占位海报失败: {e}")

        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 处理单个自定义合集的核心任务 ---
def process_single_custom_collection(processor, custom_collection_id: int):
    """
    处理单个自定义合集 (逻辑与批量任务一致，已适配轻量化架构)。
    """
    task_name = f"生成单个自建合集 (ID: {custom_collection_id})"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # 1. 读取合集定义
        task_manager.update_status_from_thread(10, "正在读取合集定义...")
        collection = custom_collection_db.get_custom_collection_by_id(custom_collection_id)
        if not collection: raise ValueError(f"未找到ID为 {custom_collection_id} 的自定义合集。")
        
        collection_name = collection['name']
        collection_type = collection['type']
        definition = collection['definition_json']
        
        task_manager.update_status_from_thread(20, f"正在处理《{collection_name}》...")

        global_ordered_emby_ids = []
        items_for_db = []
        total_count = 0

        # ==================================================================
        # 分支 A: 筛选类 (Filter) - 极速模式
        # ==================================================================
        if collection_type == 'filter':
            admin_user_id = processor.emby_user_id
            target_library_ids = definition.get('target_library_ids', [])
            sample_items, total_count = queries_db.query_virtual_library_items(
                rules=definition.get('rules', []),
                logic=definition.get('logic', 'AND'),
                user_id=admin_user_id,
                limit=9,
                offset=0,
                item_types=definition.get('item_type', ['Movie']),
                target_library_ids=target_library_ids
            )
            global_ordered_emby_ids = [item['Id'] for item in sample_items]
            items_for_db = [{'emby_id': item['Id']} for item in sample_items]

        # ==================================================================
        # 分支 B: 榜单/推荐类 (List/AI) - 全量模式
        # ==================================================================
        elif collection_type in ['list', 'ai_recommendation_global']:
            raw_tmdb_items = []
            if collection_type == 'list':
                importer = ListImporter(processor.tmdb_api_key)
                raw_tmdb_items, _ = importer.process(definition)
            else:
                from handler.custom_collection import RecommendationEngine
                rec_engine = RecommendationEngine(processor.tmdb_api_key)
                raw_tmdb_items = rec_engine.generate(definition)

            raw_tmdb_items, corrected_id_to_original_id_map = _apply_id_corrections(raw_tmdb_items, definition, collection_name)
            
            # 映射 Emby ID (需要全量映射表)
            task_manager.update_status_from_thread(15, "正在加载媒体映射表...")
            # 放弃使用 get_emby_ids_for_items，改用批量任务同款函数
            tmdb_to_emby_item_map = media_db.get_tmdb_to_emby_map()

            tmdb_items = []
            for item in raw_tmdb_items:
                tmdb_id = str(item.get('id'))
                media_type = item.get('type')
                emby_id = None
                
                # 统一使用 key 匹配
                key = f"{tmdb_id}_{media_type}"
                if key in tmdb_to_emby_item_map:
                    emby_id = tmdb_to_emby_item_map[key]['Id']
                
                processed_item = {
                    'tmdb_id': tmdb_id,
                    'media_type': media_type,
                    'emby_id': emby_id,
                    'title': item.get('title'),
                    **({'season': item['season']} if 'season' in item and item.get('season') is not None else {})
                }
                tmdb_items.append(processed_item)
                
                if emby_id:
                    global_ordered_emby_ids.append(emby_id)

            items_for_db = tmdb_items
            total_count = len(global_ordered_emby_ids)

            if collection_type == 'list':
                # 构造一个临时的 map 传给健康检查
                tmdb_to_emby_map_full = tmdb_to_emby_item_map # 复用
                _perform_list_collection_health_check(
                    tmdb_items=tmdb_items,
                    tmdb_to_emby_item_map=tmdb_to_emby_map_full,
                    corrected_id_to_original_id_map=corrected_id_to_original_id_map,
                    collection_db_record=collection,
                    tmdb_api_key=processor.tmdb_api_key
                )

        # ==================================================================
        # 分支 C: 个人推荐类 (AI) - 封面快车道 (遵守前端定义的库和类型)
        # ==================================================================
        elif collection_type == 'ai_recommendation':
            # 💡 核心思路：后台任务仅为生成封面，不调用 LLM 浪费 Tokens。
            # 我们直接根据前端定义的 [媒体库] 和 [内容类型] 捞取高分片作为门面。
            
            admin_user_id = processor.emby_user_id
            # 1. 提取前端定义的规则
            target_library_ids = definition.get('target_library_ids', [])
            item_types = definition.get('item_type', ['Movie'])
            
            logger.info(f"  ➜ 正在为《{collection_name}》筛选封面素材 (类型: {item_types})...")

            # 2. 调用查询引擎：遵守前端规则 + 评分 > 7 (保证封面质量)
            sample_items, _ = queries_db.query_virtual_library_items(
                rules=[{"field": "rating", "operator": "gte", "value": 7}],
                logic='AND',
                user_id=admin_user_id,
                limit=20, 
                offset=0,
                item_types=item_types,         # 👈 遵守前端选的内容类型
                target_library_ids=target_library_ids, # 👈 遵守前端选的媒体库
                sort_by='random'               # 👈 随机排序，让封面每次更新都有新鲜感
            )
            
            # 3. 兜底逻辑：如果高分片太少（比如新库），则放宽条件纯随机抓取
            if len(sample_items) < 9:
                logger.debug(f"  ➜ 高分素材不足，放宽条件抓取...")
                sample_items, _ = queries_db.query_virtual_library_items(
                    rules=[], 
                    user_id=admin_user_id,
                    limit=20,
                    item_types=item_types,
                    target_library_ids=target_library_ids,
                    sort_by='random'
                )

            # 4. 填充数据
            global_ordered_emby_ids = [item['Id'] for item in sample_items]
            # 数据库里存个简单的占位，反代层实时访问时会动态生成真正的 AI 列表
            items_for_db = [{'emby_id': item['Id']} for item in sample_items]
            total_count = 0 # 个人推荐类在后台任务中不计总数

        if not global_ordered_emby_ids and collection_type != 'ai_recommendation':
             # 如果没找到任何东西，且不是AI推荐（AI推荐允许空），则清空 Emby 实体合集
             # 但为了封面生成器不报错，我们还是走正常流程，只是列表为空
             pass

        # 5. 在 Emby 中创建/更新合集
        task_manager.update_status_from_thread(60, "正在Emby中创建/更新合集...")
        should_allow_empty = (collection_type == 'ai_recommendation')
        emby_collection_id = emby.create_or_update_collection_with_emby_ids(
            collection_name=collection_name, 
            emby_ids_in_library=global_ordered_emby_ids, 
            base_url=processor.emby_url, 
            api_key=processor.emby_api_key, 
            user_id=processor.emby_user_id,
            allow_empty=should_allow_empty
        )

        # 6. 更新数据库状态
        update_data = {
            "emby_collection_id": emby_collection_id,
            "item_type": json.dumps(definition.get('item_type', ['Movie'])),
            "last_synced_at": datetime.now(pytz.utc),
            "in_library_count": total_count,
            "generated_media_info_json": json.dumps(items_for_db, ensure_ascii=False)
        }
        custom_collection_db.update_custom_collection_sync_results(custom_collection_id, update_data)

        # 7. 封面生成
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}
            if cover_config.get("enabled") and emby_collection_id:
                cover_service = CoverGeneratorService(config=cover_config)
                library_info = emby.get_emby_item_details(emby_collection_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if library_info:
                    latest_collection_info = custom_collection_db.get_custom_collection_by_id(custom_collection_id)
                    item_count_to_pass = _get_cover_badge_text_for_collection(latest_collection_info)
                    cover_service.generate_for_library(
                        emby_server_id='main_emby', library=library_info,
                        item_count=item_count_to_pass, content_types=definition.get('item_type', ['Movie'])
                    )
        except Exception as e_cover:
            logger.error(f"为合集 '{collection_name}' 生成封面时发生错误: {e_cover}", exc_info=True)
        
        try:
            if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_SHOW_MISSING_PLACEHOLDERS, False):
                sync_all_subscription_posters()
            else:
                logger.info("  ➜ 检测到占位海报功能已关闭，跳过海报同步。")
        except Exception as e:
            logger.error(f"全量同步占位海报失败: {e}")

        task_manager.update_status_from_thread(100, "自建合集及海报同步完毕！")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")