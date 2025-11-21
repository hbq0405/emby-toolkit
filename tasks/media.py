# tasks/media.py
# 核心媒体处理、元数据、资产同步等

import time
import json
import logging
import psycopg2
from typing import Optional, List
from datetime import datetime, timezone
import concurrent.futures
from collections import defaultdict

# 导入需要的底层模块和共享实例
import task_manager
import handler.tmdb as tmdb
import handler.emby as emby
import handler.telegram as telegram
from database import connection
from utils import translate_country_list, get_unified_rating
from .helpers import parse_full_asset_details

logger = logging.getLogger(__name__)

# ★★★ 中文化角色名 ★★★
def task_role_translation(processor, force_full_update: bool = False):
    """
    根据传入的 force_full_update 参数，决定是执行标准扫描还是深度更新。
    """
    # 1. 根据参数决定日志信息
    if force_full_update:
        logger.info("  ➜ 即将执行深度模式，将处理所有媒体项并从TMDb获取最新数据...")
    else:
        logger.info("  ➜ 即将执行快速模式，将跳过已处理项...")


    # 3. 调用核心处理函数，并将 force_full_update 参数透传下去
    processor.process_full_library(
        update_status_callback=task_manager.update_status_from_thread,
        force_full_update=force_full_update 
    )

# --- 使用手动编辑的结果处理媒体项 ---
def task_manual_update(processor, item_id: str, manual_cast_list: list, item_name: str):
    """任务：使用手动编辑的结果处理媒体项"""
    processor.process_item_with_manual_cast(
        item_id=item_id,
        manual_cast_list=manual_cast_list,
        item_name=item_name
    )

def task_sync_metadata_cache(processor, item_id: str, item_name: str, episode_ids_to_add: Optional[List[str]] = None):
    """
    任务：为单个媒体项同步元数据到 media_metadata 数据库表。
    可根据是否传入 episode_ids_to_add 来决定执行模式。
    """
    sync_mode = "精准分集追加" if episode_ids_to_add else "常规元数据刷新"
    logger.trace(f"  ➜ 任务开始：同步媒体元数据缓存 ({sync_mode}) for '{item_name}' (ID: {item_id})")
    try:
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name, episode_ids_to_add=episode_ids_to_add)
        logger.trace(f"  ➜ 任务成功：同步媒体元数据缓存 for '{item_name}'")
    except Exception as e:
        logger.error(f"  ➜ 任务失败：同步媒体元数据缓存 for '{item_name}' 时发生错误: {e}", exc_info=True)
        raise

def task_sync_images(processor, item_id: str, update_description: str, sync_timestamp_iso: str):
    """
    任务：为单个媒体项同步图片和元数据文件到本地 override 目录。
    """
    logger.trace(f"任务开始：图片备份 for ID: {item_id} (原因: {update_description})")
    try:
        # --- ▼▼▼ 核心修复 ▼▼▼ ---
        # 1. 根据 item_id 获取完整的媒体详情
        item_details = emby.get_emby_item_details(
            item_id, 
            processor.emby_url, 
            processor.emby_api_key, 
            processor.emby_user_id
        )
        if not item_details:
            logger.error(f"任务失败：无法获取 ID: {item_id} 的媒体详情，跳过图片备份。")
            return

        # 2. 使用获取到的 item_details 字典来调用
        processor.sync_item_images(
            item_details=item_details, 
            update_description=update_description
            # episode_ids_to_sync 参数这里不需要，sync_item_images 会自己处理
        )
        # --- ▲▲▲ 修复结束 ▲▲▲ ---

        logger.trace(f"任务成功：图片备份 for ID: {item_id}")
    except Exception as e:
        logger.error(f"任务失败：图片备份 for ID: {item_id} 时发生错误: {e}", exc_info=True)
        raise

def task_sync_all_metadata(processor, item_id: str, item_name: str):
    """
    【任务：全能元数据同步器。
    当收到 metadata.update Webhook 时，此任务会：
    1. 从 Emby 获取最新数据。
    2. 将更新持久化到 override 覆盖缓存文件。
    3. 将更新同步到 media_metadata 数据库缓存。
    """
    log_prefix = f"全能元数据同步 for '{item_name}'"
    logger.trace(f"  ➜ 任务开始：{log_prefix}")
    try:
        # 步骤 1: 获取包含了用户修改的、最新的完整媒体详情
        item_details = emby.get_emby_item_details(
            item_id, 
            processor.emby_url, 
            processor.emby_api_key, 
            processor.emby_user_id,
            # 请求所有可能被用户修改的字段
            fields="ProviderIds,Type,Name,OriginalTitle,Overview,Tagline,CommunityRating,OfficialRating,Genres,Studios,Tags,PremiereDate"
        )
        if not item_details:
            logger.error(f"  ➜ {log_prefix} 失败：无法获取项目 {item_id} 的最新详情。")
            return

        # 步骤 2: 调用施工队，更新 override 文件
        processor.sync_emby_updates_to_override_files(item_details)

        # 步骤 3: 调用另一个施工队，更新数据库缓存
        # 注意：这里我们复用现有的 task_sync_metadata_cache 逻辑
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name)

        logger.trace(f"  ➜ 任务成功：{log_prefix}")
    except Exception as e:
        logger.error(f"  ➜ 任务失败：{log_prefix} 时发生错误: {e}", exc_info=True)
        raise

# ★★★ 重新处理单个项目 ★★★
def task_reprocess_single_item(processor, item_id: str, item_name_for_ui: str):
    """
    【最终版 - 职责分离】后台任务。
    此版本负责在任务开始时设置“正在处理”的状态，并执行核心逻辑。
    """
    logger.trace(f"  ➜ 后台任务开始执行 ({item_name_for_ui})")
    
    try:
        # ✨ 关键修改：任务一开始，就用“正在处理”的状态覆盖掉旧状态
        task_manager.update_status_from_thread(0, f"正在处理: {item_name_for_ui}")

        # 现在才开始真正的工作
        processor.process_single_item(
            item_id, 
            force_full_update=True
        )
        # 任务成功完成后的状态更新会自动由任务队列处理，我们无需关心
        logger.trace(f"  ➜ 后台任务完成 ({item_name_for_ui})")

    except Exception as e:
        logger.error(f"后台任务处理 '{item_name_for_ui}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"处理失败: {item_name_for_ui}")

# ★★★ 重新处理所有待复核项 ★★★
def task_reprocess_all_review_items(processor):
    """
    【已升级】后台任务：遍历所有待复核项并逐一以“强制在线获取”模式重新处理。
    """
    logger.trace("--- 开始执行“重新处理所有待复核项”任务 [强制在线获取模式] ---")
    try:
        # +++ 核心修改 1：同时查询 item_id 和 item_name +++
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 从 failed_log 中同时获取 ID 和 Name
            cursor.execute("SELECT item_id, item_name FROM failed_log")
            # 将结果保存为一个字典列表，方便后续使用
            all_items = [{'id': row['item_id'], 'name': row['item_name']} for row in cursor.fetchall()]
        
        total = len(all_items)
        if total == 0:
            logger.info("待复核列表中没有项目，任务结束。")
            task_manager.update_status_from_thread(100, "待复核列表为空。")
            return

        logger.info(f"共找到 {total} 个待复核项需要以“强制在线获取”模式重新处理。")

        # +++ 核心修改 2：在循环中解包 item_id 和 item_name +++
        for i, item in enumerate(all_items):
            if processor.is_stop_requested():
                logger.info("任务被中止。")
                break
            
            item_id = item['id']
            item_name = item['name'] or f"ItemID: {item_id}" # 如果名字为空，提供一个备用名

            task_manager.update_status_from_thread(int((i/total)*100), f"正在重新处理 {i+1}/{total}: {item_name}")
            
            # +++ 核心修改 3：传递所有必需的参数 +++
            task_reprocess_single_item(processor, item_id, item_name)
            
            # 每个项目之间稍作停顿
            time.sleep(2) 

    except Exception as e:
        logger.error(f"重新处理所有待复核项时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")

# ★★★ 重量级的元数据缓存填充任务 ★★★
def task_populate_metadata_cache(processor, batch_size: int = 50, force_full_update: bool = False):
    """
    【V9 - 季ID获取修复版】
    - 核心重构：不再使用 get_series_children，而是统一使用 get_all_library_versions 的结果来处理分集。
    - 实现了对“单集多版本”的完美支持，能够将同一分集的多个文件版本的 ID 和资产信息聚合到数据库的同一条记录中。
    - 电影、剧集、分集的多版本处理逻辑完全统一，数据源一致。
    - 修复了无法直接获取“季”的 Emby Item ID 的问题，改为从已获取的 Season 对象中直接查找，提高了准确性和稳定性。
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    try:
        task_manager.update_status_from_thread(0, f"阶段1/2: 计算媒体库差异 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        emby_items_index = emby.get_all_library_versions(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series,Season,Episode",
            library_ids=libs_to_process_ids,
            fields="ProviderIds,Type,DateCreated,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,Tags,DateModified,OfficialRating,ProductionYear,Path,PrimaryImageAspectRatio,Overview,MediaStreams,Container,Size,SeriesId,ParentIndexNumber,IndexNumber,ParentId",
            update_status_callback=task_manager.update_status_from_thread
        ) or []
        
        top_level_items_map = defaultdict(list)
        series_to_episode_versions_map = defaultdict(list)
        # +++ 核心修复 1：新增一个字典，用于按剧集ID存储所有“季”对象 +++
        series_to_seasons_map = defaultdict(list)
        
        for item in emby_items_index:
            item_type = item.get("Type")
            if item_type in ["Movie", "Series"]:
                if tmdb_id := item.get("ProviderIds", {}).get("Tmdb"):
                    composite_key = (tmdb_id, item_type)
                    top_level_items_map[composite_key].append(item)
            # +++ 核心修复 2：将“季”对象存入新的字典中 +++
            elif item_type == 'Season' and item.get('SeriesId'):
                series_to_seasons_map[item['SeriesId']].append(item)
            elif item_type == 'Episode' and item.get('SeriesId'):
                series_to_episode_versions_map[item['SeriesId']].append(item)

        emby_composite_keys = set(top_level_items_map.keys())
        logger.info(f"  ➜ 从 Emby 获取到 {len(emby_composite_keys)} 个有效的顶层媒体项 (电影/剧集)。")

        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id, item_type FROM media_metadata WHERE in_library = TRUE AND item_type IN ('Movie', 'Series')")
            db_composite_keys = {(row["tmdb_id"], row["item_type"]) for row in cursor.fetchall()}
        logger.info(f"  ➜ 从本地数据库获取到 {len(db_composite_keys)} 个【仍在库中】的顶层媒体项。")

        if processor.is_stop_requested(): return

        keys_to_delete = db_composite_keys - emby_composite_keys
        
        if force_full_update:
            keys_to_process = emby_composite_keys
        else:
            keys_to_process = emby_composite_keys - db_composite_keys
        
        logger.info(f"  ➜ 同步计划：处理 {len(keys_to_process)} 项, 标记离线 {len(keys_to_delete)} 项。")

        if keys_to_delete:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                tmdb_ids_to_delete = {key[0] for key in keys_to_delete}
                ids_to_delete_list = list(tmdb_ids_to_delete)
                sql = "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb WHERE tmdb_id = ANY(%s) OR parent_series_tmdb_id = ANY(%s)"
                cursor.execute(sql, (ids_to_delete_list, ids_to_delete_list))
                conn.commit()
            logger.info(f"  ➜ {len(keys_to_delete)} 个离线项目标记完成。")

        if processor.is_stop_requested(): return

        items_to_process = [top_level_items_map[key] for key in keys_to_process]
        total_to_process = len(items_to_process)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "数据库已是最新，无需同步。")
            return

        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested(): break
            batch_item_groups = items_to_process[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            task_manager.update_status_from_thread(10 + int((processed_count / total_to_process) * 90), f"处理批次 {batch_number}...")

            tmdb_details_map = {}
            def fetch_tmdb_details(item_group):
                item = item_group[0]
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                item_type = item.get("Type")
                if not tmdb_id: return None, None
                details = None
                if item_type == 'Movie': details = tmdb.get_movie_details(tmdb_id, processor.tmdb_api_key)
                elif item_type == 'Series': details = tmdb.get_tv_details(tmdb_id, processor.tmdb_api_key)
                return tmdb_id, details
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_tmdb_id = {executor.submit(fetch_tmdb_details, item_group): item_group[0].get("ProviderIds", {}).get("Tmdb") for item_group in batch_item_groups}
                for future in concurrent.futures.as_completed(future_to_tmdb_id):
                    if processor.is_stop_requested(): break
                    tmdb_id_res, details = future.result()
                    if tmdb_id_res and details: tmdb_details_map[tmdb_id_res] = details
            if processor.is_stop_requested(): break

            metadata_batch = []
            for item_group in batch_item_groups:
                if processor.is_stop_requested(): break
                item = item_group[0]
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not tmdb_id: continue
                item_type = item.get("Type")
                
                asset_details_list = []
                if item_type == "Movie":
                    asset_details_list = [parse_full_asset_details(v) for v in item_group]
                
                tmdb_details = tmdb_details_map.get(tmdb_id)
                premiere_date_str = item.get('PremiereDate')
                release_date = premiere_date_str.split('T')[0] if premiere_date_str else None
                top_level_record = {
                    "tmdb_id": tmdb_id, "item_type": item_type, "title": item.get('Name'),
                    "original_title": item.get('OriginalTitle'), "release_year": item.get('ProductionYear'),
                    "rating": item.get('CommunityRating'), "overview": item.get('Overview'), "release_date": release_date,
                    "date_added": item.get('DateCreated'), "genres_json": json.dumps(item.get('Genres', []), ensure_ascii=False),
                    "in_library": True, "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "subscription_status": "NONE" if item_type == "Movie" else None, "ignore_reason": None,
                    "emby_item_ids_json": json.dumps([v.get('Id') for v in item_group if v.get('Id')], ensure_ascii=False),
                    "official_rating": item.get('OfficialRating'), "unified_rating": get_unified_rating(item.get('OfficialRating'))
                }
                if tmdb_details:
                    top_level_record['overview'] = tmdb_details.get('overview') or item.get('Overview')
                    top_level_record['poster_path'] = tmdb_details.get('poster_path')
                    if item_type == 'Movie':
                        top_level_record['original_language'] = tmdb_details.get('original_language')
                    elif item_type == 'Series':
                        # 从 languages 数组中取第一个作为主要语言
                        languages_list = tmdb_details.get('languages', [])
                        top_level_record['original_language'] = languages_list[0] if languages_list else None
                    top_level_record['studios_json'] = json.dumps([s['name'] for s in tmdb_details.get('production_companies', [])], ensure_ascii=False)
                    directors, countries, keywords = [], [], []
                    if item_type == 'Movie':
                        credits_data = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                        directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        country_codes = [c.get('iso_3166_1') for c in tmdb_details.get('production_countries', [])]
                        countries = translate_country_list(country_codes)
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('keywords', []) if isinstance(keywords_data, dict) else []
                        keywords = [k['name'] for k in keyword_list if k.get('name')]
                    elif item_type == 'Series':
                        directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                        countries = translate_country_list(tmdb_details.get('origin_country', []))
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('results', []) if isinstance(keywords_data, dict) else []
                        keywords = [k['name'] for k in keyword_list if k.get('name')]
                    top_level_record['directors_json'] = json.dumps(directors, ensure_ascii=False)
                    top_level_record['countries_json'] = json.dumps(countries, ensure_ascii=False)
                    top_level_record['keywords_json'] = json.dumps(keywords, ensure_ascii=False)
                else:
                    top_level_record['poster_path'] = None
                    top_level_record['studios_json'] = json.dumps([s['Name'] for s in item.get('Studios', [])], ensure_ascii=False)
                    top_level_record['directors_json'] = '[]'; top_level_record['countries_json'] = '[]'; top_level_record['keywords_json'] = '[]'

                metadata_batch.append(top_level_record)

                if item.get("Type") == "Series":
                    series_tmdb_id = tmdb_id
                    all_series_emby_ids = [v.get('Id') for v in item_group if v.get('Id')]
                    
                    # 聚合该剧集（包括其所有版本）下的所有分集和季
                    all_episode_versions = []
                    for s_id in all_series_emby_ids:
                        all_episode_versions.extend(series_to_episode_versions_map.get(s_id, []))
                    
                    # +++ 核心修复 3：聚合该剧集下的所有“季”对象 +++
                    all_season_items = []
                    for s_id in all_series_emby_ids:
                        all_season_items.extend(series_to_seasons_map.get(s_id, []))
                    
                    # +++ 核心修复 4：创建一个从季号到“季”对象的快速查找字典 +++
                    emby_seasons_by_number = defaultdict(list)
                    for season_item in all_season_items:
                        if season_item.get('IndexNumber') is not None:
                            emby_seasons_by_number[season_item.get('IndexNumber')].append(season_item)

                    if not all_episode_versions and not all_season_items: continue
                    
                    tmdb_series_details = tmdb_details_map.get(series_tmdb_id)
                    tmdb_children_map = {}
                    if tmdb_series_details and 'seasons' in tmdb_series_details:
                        for season_info in tmdb_series_details.get('seasons', []):
                            s_num_map = season_info.get('season_number')
                            if s_num_map is None or s_num_map == 0: continue
                            
                            season_tmdb_id = season_info.get('id')
                            if not season_tmdb_id: continue

                            # +++ 核心修复 5：使用新的查找字典直接获取“季”的Emby ID +++
                            matching_emby_seasons = emby_seasons_by_number.get(s_num_map, [])
                            season_emby_ids = [s.get('Id') for s in matching_emby_seasons if s.get('Id')]

                            season_record = {
                                "tmdb_id": str(season_tmdb_id),
                                "item_type": "Season",
                                "parent_series_tmdb_id": series_tmdb_id,
                                "season_number": s_num_map,
                                "title": season_info.get('name'),
                                "overview": season_info.get('overview'),
                                "release_date": season_info.get('air_date'),
                                "poster_path": season_info.get('poster_path'),
                                "in_library": True,
                                "emby_item_ids_json": json.dumps(season_emby_ids), 
                                "ignore_reason": None
                            }
                            metadata_batch.append(season_record)
                            
                            tmdb_children_map[f"S{s_num_map}"] = season_info
                            tmdb_season_details = tmdb.get_tv_season_details(series_tmdb_id, s_num_map, processor.tmdb_api_key)
                            if tmdb_season_details and 'episodes' in tmdb_season_details:
                                for episode_info in tmdb_season_details['episodes']:
                                    e_num_map = episode_info.get('episode_number')
                                    if e_num_map is None: continue
                                    tmdb_children_map[f"S{s_num_map}E{e_num_map}"] = episode_info
                    
                    if not all_episode_versions: continue

                    episodes_grouped_by_number = defaultdict(list)
                    for ep_version in all_episode_versions:
                        s_num, e_num = ep_version.get("ParentIndexNumber"), ep_version.get("IndexNumber")
                        if s_num is not None and e_num is not None:
                            episodes_grouped_by_number[(s_num, e_num)].append(ep_version)

                    for (s_num, e_num), versions_of_episode in episodes_grouped_by_number.items():
                        representative_episode = versions_of_episode[0]
                        all_emby_ids = [v.get('Id') for v in versions_of_episode]
                        all_asset_details = [parse_full_asset_details(v) for v in versions_of_episode]

                        child_record = {
                            "in_library": True, "ignore_reason": None, 
                            "emby_item_ids_json": json.dumps(all_emby_ids),
                            "asset_details_json": json.dumps(all_asset_details, ensure_ascii=False)
                        }
                        
                        lookup_key = f"S{s_num}E{e_num}"
                        tmdb_child_info = tmdb_children_map.get(lookup_key)

                        if tmdb_child_info and tmdb_child_info.get('id'):
                            child_record.update({ "tmdb_id": str(tmdb_child_info.get('id')), "title": tmdb_child_info.get('name'), "release_date": tmdb_child_info.get('air_date'), "overview": tmdb_child_info.get('overview'), "poster_path": tmdb_child_info.get('poster_path') })
                        else:
                            child_record.update({ "tmdb_id": f"{series_tmdb_id}-{lookup_key}", "title": representative_episode.get('Name'), "overview": representative_episode.get('Overview') })

                        child_record.update({ "item_type": "Episode", "parent_series_tmdb_id": series_tmdb_id, "season_number": s_num, "episode_number": e_num })
                        metadata_batch.append(child_record)

            if processor.is_stop_requested(): break

            if metadata_batch:
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    for idx, metadata in enumerate(metadata_batch):
                        if processor.is_stop_requested(): break
                        savepoint_name = f"sp_{idx}"
                        try:
                            cursor.execute(f"SAVEPOINT {savepoint_name};")
                            columns = [k for k, v in metadata.items() if v is not None]
                            values = [v for v in metadata.values() if v is not None]
                            columns_str = ', '.join(columns)
                            placeholders_str = ', '.join(['%s'] * len(values))
                            
                            update_clauses = []
                            columns_to_update = [c for c in columns if c not in ('tmdb_id', 'item_type')]
                            for col in columns_to_update:
                                if col in ['subscription_sources_json']: continue
                                
                                if col == 'emby_item_ids_json':
                                    update_clauses.append("""
                                        emby_item_ids_json = (
                                            SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb)
                                            FROM (
                                                SELECT DISTINCT elem
                                                FROM (
                                                    SELECT jsonb_array_elements_text(COALESCE(media_metadata.emby_item_ids_json, '[]'::jsonb)) AS elem
                                                    UNION ALL
                                                    SELECT jsonb_array_elements_text(EXCLUDED.emby_item_ids_json) AS elem
                                                ) AS combined
                                                WHERE elem IS NOT NULL AND elem != 'null'
                                            ) AS filtered
                                        )
                                    """)
                                else:
                                    update_clauses.append(f"{col} = EXCLUDED.{col}")
                            update_clauses.append("ignore_reason = NULL")
                            update_str = ', '.join(update_clauses)
                            sql = f"INSERT INTO media_metadata ({columns_str}, last_synced_at) VALUES ({placeholders_str}, NOW()) ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update_str}, last_synced_at = NOW()"
                            cursor.execute(sql, tuple(values))
                        except psycopg2.Error as e:
                            logger.error(f"写入 TMDB ID {metadata.get('tmdb_id')} 的元数据时发生数据库错误: {e}")
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                    if not processor.is_stop_requested():
                        conn.commit()
                    else:
                        conn.rollback()
            processed_count += len(batch_item_groups)

        final_message = f"同步完成！本次处理 {processed_count}/{total_to_process} 项, 标记离线 {len(keys_to_delete)} 项。"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_apply_main_cast_to_episodes(processor, series_id: str, episode_ids: list):
    """
    【V2 - 文件中心化重构版】
    轻量级任务：当剧集追更新增分集时，将主项目的完美演员表注入到新分集的 override 元数据文件中。
    此任务不再读写 Emby API，而是委托核心处理器的 sync_single_item_assets 方法执行精准的文件同步操作。
    """
    try:
        if not episode_ids:
            logger.info(f"  ➜ 剧集 {series_id} 追更任务跳过：未提供需要更新的分集ID。")
            return
        
        series_details_for_log = emby.get_emby_item_details(series_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, fields="Name,ProviderIds")
        series_name = series_details_for_log.get("Name", f"ID:{series_id}") if series_details_for_log else f"ID:{series_id}"

        logger.info(f"  ➜ 追更任务启动：准备为剧集 《{series_name}》 的 {len(episode_ids)} 个新分集同步元数据...")

        processor.sync_single_item_assets(
            item_id=series_id,
            update_description=f"追更新增 {len(episode_ids)} 个分集",
            sync_timestamp_iso=datetime.now(timezone.utc).isoformat(),
            episode_ids_to_sync=episode_ids
        )

        logger.info(f"  ➜ 处理完成，正在通知 Emby 刷新...")
        emby.refresh_emby_item_metadata(
            item_emby_id=series_id,
            emby_server_url=processor.emby_url,
            emby_api_key=processor.emby_api_key,
            user_id_for_ops=processor.emby_user_id,
            replace_all_metadata_param=True,
            item_name_for_log=series_name
        )

        # TG通知
        if series_details_for_log:
            logger.info(f"  ➜ 正在为《{series_name}》触发追更通知...")
            telegram.send_media_notification(
                item_details=series_details_for_log,
                notification_type='update',
                new_episode_ids=episode_ids
            )

        # 步骤 3: 更新父剧集在元数据缓存中的 last_synced_at 时间戳 (这个逻辑可以保留)
        if series_details_for_log:
            tmdb_id = series_details_for_log.get("ProviderIds", {}).get("Tmdb")
            if tmdb_id:
                try:
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "UPDATE media_metadata SET last_synced_at = %s WHERE tmdb_id = %s AND item_type = 'Series'",
                                (datetime.now(timezone.utc), tmdb_id)
                            )
                except Exception as db_e:
                    logger.error(f"  ➜ 更新剧集《{series_name}》的时间戳时发生数据库错误: {db_e}", exc_info=True)

    except Exception as e:
        logger.error(f"  ➜ 为剧集 {series_id} 的新分集应用主演员表时发生错误: {e}", exc_info=True)
        raise