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
    - 重量级的元数据缓存填充任务。
    - 逻辑升级：
      1. 记录本次扫描到的所有 Emby ID。
      2. 计算 (库内已知 ID - 本次扫描 ID) = 已删除的 ID。
      3. 如果发现已删除的 ID，反查其所属的父级剧集，并将该剧集标记为“待更新”。
      4. 这样即使只删了一集，该剧集也会进入处理队列，从而触发子集离线清理逻辑。
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    # --- 统计计数器 ---
    total_updated_count = 0
    total_offline_count = 0

    try:
        task_manager.update_status_from_thread(0, f"阶段1/3: 建立差异基准 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        # 1. 获取数据库中所有已知的 Emby ID (用于比对)
        known_emby_ids = set()
        if not force_full_update:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT jsonb_array_elements_text(emby_item_ids_json) AS emby_id
                    FROM media_metadata 
                    WHERE in_library = TRUE
                """)
                known_emby_ids = set(row['emby_id'] for row in cursor.fetchall())
            logger.info(f"  ➜ 基准建立完成，库内已知 {len(known_emby_ids)} 个 Emby 媒体项 ID。")

        # 2. 扫描 Emby
        task_manager.update_status_from_thread(10, f"阶段2/3: 扫描 Emby 并计算差异...")
        emby_items_index = emby.get_all_library_versions(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series,Season,Episode",
            library_ids=libs_to_process_ids,
            fields="ProviderIds,Type,DateCreated,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,Tags,DateModified,OfficialRating,ProductionYear,Path,PrimaryImageAspectRatio,Overview,MediaStreams,Container,Size,SeriesId,ParentIndexNumber,IndexNumber,ParentId,RunTimeTicks",
            update_status_callback=task_manager.update_status_from_thread
        ) or []
        
        # 3. 构建索引 & 识别变动
        top_level_items_map = defaultdict(list)       
        series_to_seasons_map = defaultdict(list)     
        series_to_episode_map = defaultdict(list)     
        
        emby_top_level_keys = set() 
        
        # 记录哪些剧集(TMDb ID)需要刷新
        dirty_series_tmdb_ids = set()
        emby_sid_to_tmdb_id = {}
        
        # 记录本次扫描到的所有 Emby ID (用于反向比对删除)
        current_scan_emby_ids = set()

        # 先遍历一遍建立 Series ID 映射
        for item in emby_items_index:
            # 记录 ID
            if item.get("Id"):
                current_scan_emby_ids.add(str(item.get("Id")))

            if item.get("Type") == "Series":
                t_id = item.get("ProviderIds", {}).get("Tmdb")
                e_id = str(item.get("Id"))
                if t_id and e_id:
                    emby_sid_to_tmdb_id[e_id] = str(t_id)

        scan_count = len(emby_items_index)
        
        for item in emby_items_index:
            item_type = item.get("Type")
            item_emby_id = str(item.get("Id"))
            tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
            
            # --- 正向差异检测 (新增) ---
            is_new_item = False
            if not force_full_update:
                if item_emby_id not in known_emby_ids:
                    is_new_item = True
            
            # A. 顶层媒体
            if item_type in ["Movie", "Series"]:
                if tmdb_id:
                    composite_key = (str(tmdb_id), item_type)
                    top_level_items_map[composite_key].append(item)
                    emby_top_level_keys.add(composite_key)
                    
                    if item_type == "Series" and is_new_item:
                        dirty_series_tmdb_ids.add(str(tmdb_id))

            # B. 子集媒体 (Season)
            elif item_type == 'Season':
                s_id = str(item.get('SeriesId') or item.get('ParentId'))
                if s_id: 
                    series_to_seasons_map[s_id].append(item)
                    if is_new_item and s_id in emby_sid_to_tmdb_id:
                        dirty_series_tmdb_ids.add(emby_sid_to_tmdb_id[s_id])

            # C. 子集媒体 (Episode)
            elif item_type == 'Episode':
                s_id = str(item.get('SeriesId'))
                if s_id: 
                    series_to_episode_map[s_id].append(item)
                    if is_new_item and s_id in emby_sid_to_tmdb_id:
                        dirty_series_tmdb_ids.add(emby_sid_to_tmdb_id[s_id])

        # ★★★ 反向差异检测 (删除) - V15 核心新增 ★★★
        if not force_full_update:
            # 算出哪些 ID 在库里有，但 Emby 没扫到
            missing_emby_ids = known_emby_ids - current_scan_emby_ids
            if missing_emby_ids:
                logger.info(f"  ➜ 检测到 {len(missing_emby_ids)} 个 Emby ID 已消失，正在反查所属剧集...")
                missing_ids_list = list(missing_emby_ids)
                
                # 从数据库反查这些消失的 ID 属于哪些剧集
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    # 查找包含这些 ID 的 Season/Episode 的 parent_series_tmdb_id
                    cursor.execute("""
                        SELECT DISTINCT parent_series_tmdb_id AS pid
                        FROM media_metadata 
                        WHERE item_type IN ('Season', 'Episode') 
                          AND in_library = TRUE 
                          AND EXISTS (
                              SELECT 1 
                              FROM jsonb_array_elements_text(emby_item_ids_json) as eid 
                              WHERE eid = ANY(%s)
                          )
                    """, (missing_ids_list,))
                    
                    affected_parents = set(row['pid'] for row in cursor.fetchall() if row['pid'])
                    
                    if affected_parents:
                        logger.info(f"  ➜ 因内容删除，{len(affected_parents)} 部剧集被标记为待刷新。")
                        dirty_series_tmdb_ids.update(affected_parents)

        logger.info(f"  ➜ Emby 扫描完成，共 {scan_count} 个项。共 {len(dirty_series_tmdb_ids)} 部剧集涉及变更(新增/删除)。")

        # 4. 数据库比对 (用于检测顶层离线)
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id, item_type FROM media_metadata WHERE in_library = TRUE AND item_type IN ('Movie', 'Series')")
            db_top_level_keys = {(row["tmdb_id"], row["item_type"]) for row in cursor.fetchall()}
        
        # 5. 处理顶层离线 (整部剧或电影被删)
        keys_to_delete = db_top_level_keys - emby_top_level_keys
        if keys_to_delete:
            count_top_offline = len(keys_to_delete)
            total_offline_count += count_top_offline
            logger.info(f"  ➜ 发现 {count_top_offline} 个顶层项目已完全离线，正在清理...")
            
            ids_to_del = defaultdict(list)
            for t_id, t_type in keys_to_delete:
                ids_to_del[t_type].append(t_id)
            
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                for i_type, id_list in ids_to_del.items():
                    cursor.execute(
                        "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = '[]'::jsonb WHERE item_type = %s AND tmdb_id = ANY(%s)",
                        (i_type, id_list)
                    )
                    if i_type == 'Series':
                        cursor.execute(
                            "UPDATE media_metadata SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = '[]'::jsonb WHERE parent_series_tmdb_id = ANY(%s)",
                            (id_list,)
                        )
                conn.commit()

        if processor.is_stop_requested(): return

        # 6. 确定处理队列 (精准过滤)
        items_to_process = []
        if force_full_update:
            items_to_process = [items for items in top_level_items_map.values()]
        else:
            keys_new_tmdb = emby_top_level_keys - db_top_level_keys
            
            for composite_key, items in top_level_items_map.items():
                tmdb_id, item_type = composite_key
                
                should_process = False
                # 规则1: 顶层本身是新的
                if composite_key in keys_new_tmdb:
                    should_process = True
                # 规则2: 剧集被标记为脏 (含新增子集 或 删除子集)
                elif item_type == 'Series' and tmdb_id in dirty_series_tmdb_ids:
                    should_process = True
                
                if should_process:
                    items_to_process.append(items)

        total_to_process = len(items_to_process)
        task_manager.update_status_from_thread(20, f"阶段3/3: 正在同步 {total_to_process} 个变更项目...")
        logger.info(f"  ➜ 最终处理队列: {total_to_process} 个顶层项目")

        # 7. 批量处理
        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested(): break
            batch_item_groups = items_to_process[i:i + batch_size]
            
            # --- 并发获取 TMDB 详情 ---
            tmdb_details_map = {}
            def fetch_tmdb_details(item_group):
                item = item_group[0]
                t_id = item.get("ProviderIds", {}).get("Tmdb")
                i_type = item.get("Type")
                if not t_id: return None, None
                details = None
                try:
                    if i_type == 'Movie': details = tmdb.get_movie_details(t_id, processor.tmdb_api_key)
                    elif i_type == 'Series': details = tmdb.get_tv_details(t_id, processor.tmdb_api_key)
                except Exception: pass
                return str(t_id), details

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_tmdb_details, grp): grp for grp in batch_item_groups}
                for future in concurrent.futures.as_completed(futures):
                    t_id_str, details = future.result()
                    if t_id_str and details: tmdb_details_map[t_id_str] = details

            metadata_batch = []
            series_ids_processed_in_batch = set()

            for item_group in batch_item_groups:
                item = item_group[0]
                tmdb_id_str = str(item.get("ProviderIds", {}).get("Tmdb"))
                item_type = item.get("Type")
                tmdb_details = tmdb_details_map.get(tmdb_id_str)
                
                # --- 1. 构建顶层记录 ---
                asset_details_list = []
                if item_type == "Movie":
                    asset_details_list = [parse_full_asset_details(v) for v in item_group]

                emby_runtime = round(item['RunTimeTicks'] / 600000000) if item.get('RunTimeTicks') else None

                top_record = {
                    "tmdb_id": tmdb_id_str, "item_type": item_type, "title": item.get('Name'),
                    "original_title": item.get('OriginalTitle'), "release_year": item.get('ProductionYear'),
                    "in_library": True, 
                    "emby_item_ids_json": json.dumps(list(set(v.get('Id') for v in item_group if v.get('Id'))), ensure_ascii=False),
                    "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "rating": item.get('CommunityRating'),
                    "date_added": item.get('DateCreated'),
                    "genres_json": json.dumps(item.get('Genres', []), ensure_ascii=False),
                    "official_rating": item.get('OfficialRating'), 
                    "unified_rating": get_unified_rating(item.get('OfficialRating')),
                    "runtime_minutes": emby_runtime if (item_type == 'Movie' and emby_runtime) else tmdb_details.get('runtime') if (item_type == 'Movie' and tmdb_details) else None
                }
                if tmdb_details:
                    top_record['poster_path'] = tmdb_details.get('poster_path')
                    top_record['overview'] = tmdb_details.get('overview')
                    top_record['studios_json'] = json.dumps([s['name'] for s in tmdb_details.get('production_companies', [])], ensure_ascii=False)
                    if item_type == 'Movie':
                        top_record['runtime_minutes'] = tmdb_details.get('runtime')
                    
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
                    top_record['directors_json'] = json.dumps(directors, ensure_ascii=False)
                    top_record['countries_json'] = json.dumps(countries, ensure_ascii=False)
                    top_record['keywords_json'] = json.dumps(keywords, ensure_ascii=False)
                else:
                    top_record['poster_path'] = None
                    top_record['studios_json'] = '[]'
                    top_record['directors_json'] = '[]'; top_record['countries_json'] = '[]'; top_record['keywords_json'] = '[]'

                metadata_batch.append(top_record)

                # --- 2. 处理 Series 的子集 ---
                if item_type == "Series":
                    series_ids_processed_in_batch.add(tmdb_id_str)
                    
                    series_emby_ids = [str(v.get('Id')) for v in item_group if v.get('Id')]
                    my_seasons = []
                    my_episodes = []
                    for s_id in series_emby_ids:
                        my_seasons.extend(series_to_seasons_map.get(s_id, []))
                        my_episodes.extend(series_to_episode_map.get(s_id, []))
                    
                    tmdb_children_map = {}
                    
                    if tmdb_details and 'seasons' in tmdb_details:
                        for s_info in tmdb_details.get('seasons', []):
                            s_num = s_info.get('season_number')
                            if s_num is None: continue
                            matched_emby_seasons = [s for s in my_seasons if s.get('IndexNumber') == s_num]
                            
                            if matched_emby_seasons:
                                real_season_tmdb_id = str(s_info.get('id'))

                                # 优先使用季海报，如果没有则回退使用父剧集海报
                                season_poster = s_info.get('poster_path')
                                if not season_poster and tmdb_details:
                                    season_poster = tmdb_details.get('poster_path')

                                season_record = {
                                    "tmdb_id": real_season_tmdb_id,
                                    "item_type": "Season",
                                    "parent_series_tmdb_id": tmdb_id_str,
                                    "season_number": s_num,
                                    "title": s_info.get('name'),
                                    "overview": s_info.get('overview'),
                                    "poster_path": season_poster,
                                    "in_library": True,
                                    "emby_item_ids_json": json.dumps([s.get('Id') for s in matched_emby_seasons]),
                                    "ignore_reason": None
                                }
                                metadata_batch.append(season_record)
                                tmdb_children_map[f"S{s_num}"] = s_info

                                has_eps = any(e.get('ParentIndexNumber') == s_num for e in my_episodes)
                                if has_eps:
                                    try:
                                        s_details = tmdb.get_tv_season_details(tmdb_id_str, s_num, processor.tmdb_api_key)
                                        if s_details and 'episodes' in s_details:
                                            for ep in s_details['episodes']:
                                                if ep.get('episode_number') is not None:
                                                    tmdb_children_map[f"S{s_num}E{ep.get('episode_number')}"] = ep
                                    except: pass

                    ep_grouped = defaultdict(list)
                    for ep in my_episodes:
                        s_n, e_n = ep.get('ParentIndexNumber'), ep.get('IndexNumber')
                        if s_n is not None and e_n is not None:
                            ep_grouped[(s_n, e_n)].append(ep)
                    
                    for (s_n, e_n), versions in ep_grouped.items():
                        emby_ep = versions[0]
                        emby_ep_runtime = round(emby_ep['RunTimeTicks'] / 600000000) if emby_ep.get('RunTimeTicks') else None
                        lookup_key = f"S{s_n}E{e_n}"
                        tmdb_ep_info = tmdb_children_map.get(lookup_key)
                        
                        child_record = {
                            "item_type": "Episode",
                            "parent_series_tmdb_id": tmdb_id_str,
                            "season_number": s_n,
                            "episode_number": e_n,
                            "in_library": True,
                            "emby_item_ids_json": json.dumps([v.get('Id') for v in versions]),
                            "asset_details_json": json.dumps([parse_full_asset_details(v) for v in versions], ensure_ascii=False),
                            "ignore_reason": None
                        }

                        if tmdb_ep_info and tmdb_ep_info.get('id'):
                            child_record['tmdb_id'] = str(tmdb_ep_info.get('id'))
                            child_record['title'] = tmdb_ep_info.get('name')
                            child_record['overview'] = tmdb_ep_info.get('overview')
                            child_record['poster_path'] = tmdb_ep_info.get('still_path')
                            child_record['runtime_minutes'] = emby_ep_runtime if emby_ep_runtime else tmdb_ep_info.get('runtime')
                        else:
                            child_record['tmdb_id'] = f"{tmdb_id_str}-S{s_n}E{e_n}"
                            child_record['title'] = versions[0].get('Name')
                            child_record['overview'] = versions[0].get('Overview')
                            child_record['runtime_minutes'] = emby_ep_runtime
                        
                        metadata_batch.append(child_record)

            # 7. 写入数据库 & 子集离线对账
            if metadata_batch:
                total_updated_count += len(metadata_batch)

                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    
                    # --- A. 执行写入 ---
                    for idx, metadata in enumerate(metadata_batch):
                        savepoint_name = f"sp_{idx}"
                        try:
                            cursor.execute(f"SAVEPOINT {savepoint_name};")
                            columns = [k for k, v in metadata.items() if v is not None]
                            values = [v for v in metadata.values() if v is not None]
                            cols_str = ', '.join(columns)
                            vals_str = ', '.join(['%s'] * len(values))
                            
                            update_clauses = []
                            for col in columns:
                                if col in ('tmdb_id', 'item_type', 'subscription_sources_json'): continue
                                update_clauses.append(f"{col} = EXCLUDED.{col}")
                            
                            sql = f"""
                                INSERT INTO media_metadata ({cols_str}, last_synced_at) 
                                VALUES ({vals_str}, NOW()) 
                                ON CONFLICT (tmdb_id, item_type) 
                                DO UPDATE SET {', '.join(update_clauses)}, last_synced_at = NOW()
                            """
                            cursor.execute(sql, tuple(values))
                        except Exception as e:
                            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
                            logger.error(f"写入失败 {metadata.get('tmdb_id')}: {e}")
                    
                    # --- B. 执行子集离线对账 ---
                    if series_ids_processed_in_batch:
                        active_child_ids = {
                            m['tmdb_id'] for m in metadata_batch 
                            if m['item_type'] in ('Season', 'Episode')
                        }
                        active_child_ids_list = list(active_child_ids)
                        
                        if active_child_ids_list:
                            cursor.execute("""
                                UPDATE media_metadata
                                SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = '[]'::jsonb
                                WHERE parent_series_tmdb_id = ANY(%s)
                                  AND item_type IN ('Season', 'Episode')
                                  AND in_library = TRUE
                                  AND tmdb_id != ALL(%s)
                            """, (list(series_ids_processed_in_batch), active_child_ids_list))
                            total_offline_count += cursor.rowcount
                        else:
                            cursor.execute("""
                                UPDATE media_metadata
                                SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = '[]'::jsonb
                                WHERE parent_series_tmdb_id = ANY(%s)
                                  AND item_type IN ('Season', 'Episode')
                                  AND in_library = TRUE
                            """, (list(series_ids_processed_in_batch),))
                            total_offline_count += cursor.rowcount

                    conn.commit()

            processed_count += len(batch_item_groups)
            task_manager.update_status_from_thread(20 + int((processed_count / total_to_process) * 80), f"处理进度 {processed_count}/{total_to_process}...")

        # 最终日志
        final_msg = f"同步完成！新增/更新: {total_updated_count} 个媒体项, 标记离线: {total_offline_count} 个媒体项。"
        logger.info(f"  ✅ {final_msg}")
        task_manager.update_status_from_thread(100, final_msg)

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