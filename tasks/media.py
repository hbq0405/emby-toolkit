# tasks/media.py
# 核心媒体处理、元数据、资产同步等

import time
import json
import gc
import logging
from typing import List
import concurrent.futures
from collections import defaultdict

# 导入需要的底层模块和共享实例
import task_manager
import utils
import handler.tmdb as tmdb
import handler.emby as emby
import handler.telegram as telegram
from database import connection, settings_db, media_db
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
                logger.info("  🚫 任务被中止。")
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

# 提取标签
def extract_tag_names(item_data):
    """
    兼容新旧版 Emby API 提取标签名。
    """
    tags_set = set()

    # 1. 尝试提取 TagItems (新版/详细版)
    tag_items = item_data.get('TagItems')
    if isinstance(tag_items, list):
        for t in tag_items:
            if isinstance(t, dict):
                name = t.get('Name')
                if name:
                    tags_set.add(name)
            elif isinstance(t, str) and t:
                tags_set.add(t)
    
    # 2. 尝试提取 Tags (旧版/简略版)
    tags = item_data.get('Tags')
    if isinstance(tags, list):
        for t in tags:
            if t:
                tags_set.add(str(t))
    
    return list(tags_set)

# --- 提取原始分级数据，不进行任何计算 ---
def _extract_and_map_tmdb_ratings(tmdb_details, item_type):
    """
    从 TMDb 详情中提取所有国家的分级数据，并执行智能映射（补全 US 分级）。
    返回: 字典 { 'US': 'R', 'CN': 'PG-13', ... }
    """
    if not tmdb_details:
        return {}

    ratings_map = {}
    origin_country = None

    # 1. 提取原始数据
    if item_type == 'Movie':
        # 电影：在 release_dates 中查找
        results = tmdb_details.get('release_dates', {}).get('results', [])
        for r in results:
            country = r.get('iso_3166_1')
            if not country: continue
            cert = None
            for release in r.get('release_dates', []):
                if release.get('certification'):
                    cert = release.get('certification')
                    break 
            if cert:
                ratings_map[country] = cert
        
        # 获取原产国
        p_countries = tmdb_details.get('production_countries', [])
        if p_countries:
            origin_country = p_countries[0].get('iso_3166_1')

    elif item_type == 'Series':
        # 剧集：在 content_ratings 中查找
        results = tmdb_details.get('content_ratings', {}).get('results', [])
        for r in results:
            country = r.get('iso_3166_1')
            rating = r.get('rating')
            if country and rating:
                ratings_map[country] = rating
        
        # 获取原产国
        o_countries = tmdb_details.get('origin_country', [])
        if o_countries:
            origin_country = o_countries[0]

    # 2. ★★★ 执行映射逻辑 (核心修复) ★★★
    # 如果已经有 US 分级，直接返回，不做映射（以 TMDb 原生 US 为准，或者你可以选择覆盖）
    # 这里我们选择：如果原生没有 US，或者我们想强制检查映射，就执行映射。
    # 为了保险，我们总是尝试计算映射值，如果计算出来了，就补全进去。
    
    target_us_code = None
    
    # 加载配置
    rating_mapping = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
    priority_list = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY

    # 按优先级查找
    for p_country in priority_list:
        search_country = origin_country if p_country == 'ORIGIN' else p_country
        if not search_country: continue
        
        if search_country in ratings_map:
            source_rating = ratings_map[search_country]
            
            # 尝试映射
            if isinstance(rating_mapping, dict) and search_country in rating_mapping and 'US' in rating_mapping:
                current_val = None
                # 找源分级对应的 Value
                for rule in rating_mapping[search_country]:
                    if str(rule['code']).strip().upper() == str(source_rating).strip().upper():
                        current_val = rule.get('emby_value')
                        break
                
                # 找 US 对应的 Code
                if current_val is not None:
                    valid_us_rules = []
                    for rule in rating_mapping['US']:
                        r_code = rule.get('code', '')
                        # 简单的类型过滤
                        if item_type == 'Movie' and r_code.startswith('TV-'): continue
                        valid_us_rules.append(rule)
                    
                    for rule in valid_us_rules:
                        # 尝试精确匹配
                        try:
                            if int(rule.get('emby_value')) == int(current_val):
                                target_us_code = rule['code']
                                break
                        except: continue
                    
                    # 如果没精确匹配，尝试向上兼容 (+1)
                    if not target_us_code:
                        for rule in valid_us_rules:
                            try:
                                if int(rule.get('emby_value')) == int(current_val) + 1:
                                    target_us_code = rule['code']
                                    break
                            except: continue

            if target_us_code:
                break
            # 如果没映射成功，但这是高优先级国家，且没有 US 分级，也可以考虑直接用它的分级做兜底（视需求而定）
            # 这里我们只做映射补全

    # 3. 补全 US 分级
    if target_us_code:
        # 强制覆盖/添加 US 分级
        ratings_map['US'] = target_us_code

    return ratings_map

# ★★★ 重量级的元数据缓存填充任务 (内存优化版) ★★★
def task_populate_metadata_cache(processor, batch_size: int = 50, force_full_update: bool = False):
    """
    - 重量级的元数据缓存填充任务 (类型安全版)。
    - 修复：彻底解决 TMDb ID 在电影和剧集间冲突的问题。
    - 修复：完善离线检测逻辑，确保消失的电影/剧集能被正确标记为离线。
    - 优化：增加详细的跳过统计，解释数量差异。
    """
    task_name = "同步媒体元数据"
    sync_mode = "深度同步 (全量)" if force_full_update else "快速同步 (增量)"
    logger.info(f"--- 模式: {sync_mode} (分批大小: {batch_size}) ---")
    
    total_updated_count = 0
    total_offline_count = 0

    try:
        task_manager.update_status_from_thread(0, f"阶段1/3: 建立差异基准 ({sync_mode})...")
        
        libs_to_process_ids = processor.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            raise ValueError("未在配置中指定要处理的媒体库。")

        # --- 1. 准备基础数据 ---
        known_emby_status = {}      # {emby_id: is_online}
        emby_sid_to_tmdb_id = {}    # {emby_series_id: tmdb_id}
        tmdb_key_to_emby_ids = defaultdict(set) 
        
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # A. 预加载映射
            cursor.execute("""
                SELECT tmdb_id, item_type, jsonb_array_elements_text(emby_item_ids_json) as eid 
                FROM media_metadata 
                WHERE item_type IN ('Movie', 'Series')
            """)
            for row in cursor.fetchall():
                e_id, t_id, i_type = row['eid'], row['tmdb_id'], row['item_type']
                if i_type == 'Series':
                    emby_sid_to_tmdb_id[e_id] = t_id
                if t_id:
                    tmdb_key_to_emby_ids[(t_id, i_type)].add(e_id)

            # B. 获取在线状态
            if not force_full_update:
                cursor.execute("""
                    SELECT jsonb_array_elements_text(emby_item_ids_json) AS emby_id, in_library
                    FROM media_metadata 
                """)
                known_emby_status = {row['emby_id']: row['in_library'] for row in cursor.fetchall()}
                
                cursor.execute("""
                    SELECT COUNT(*) as total, SUM(CASE WHEN in_library THEN 1 ELSE 0 END) as online 
                    FROM media_metadata
                """)
                stat_row = cursor.fetchone()
                total_items = stat_row['total'] if stat_row else 0
                online_items = stat_row['online'] if stat_row and stat_row['online'] is not None else 0
                
                logger.info(f"  ➜ 本地数据库共存储 {total_items} 个媒体项 (其中在线: {online_items}, 离线: {total_items - online_items})。")

        logger.info("  ➜ 正在预加载 Emby 文件夹路径映射...")
        folder_map = emby.get_all_folder_mappings(processor.emby_url, processor.emby_api_key)
        logger.info(f"  ➜ 加载了 {len(folder_map)} 个文件夹路径节点。")

        # --- 2. 扫描 Emby (流式处理) ---
        task_manager.update_status_from_thread(10, f"阶段2/3: 扫描 Emby 并计算差异...")
        
        top_level_items_map = defaultdict(list)       
        series_to_seasons_map = defaultdict(list)     
        series_to_episode_map = defaultdict(list)     
        emby_id_to_lib_id = {}
        id_to_parent_map = {}
        lib_id_to_guid_map = {}
        
        try:
            import requests
            lib_resp = requests.get(f"{processor.emby_url}/Library/VirtualFolders", params={"api_key": processor.emby_api_key})
            if lib_resp.status_code == 200:
                for lib in lib_resp.json():
                    l_id = str(lib.get('ItemId'))
                    l_guid = str(lib.get('Guid'))
                    if l_id and l_guid:
                        lib_id_to_guid_map[l_id] = l_guid
        except Exception as e:
            logger.error(f"获取库 GUID 映射失败: {e}")

        dirty_keys = set() 
        current_scan_emby_ids = set() 
        pending_children = [] 

        # ★★★ 新增计数器 ★★★
        scan_count = 0
        skipped_no_tmdb = 0
        skipped_other_type = 0
        skipped_clean = 0

        req_fields = "ProviderIds,Type,DateCreated,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,Tags,TagItems,DateModified,OfficialRating,ProductionYear,Path,PrimaryImageAspectRatio,Overview,MediaStreams,Container,Size,SeriesId,ParentIndexNumber,IndexNumber,ParentId,RunTimeTicks,_SourceLibraryId"

        item_generator = emby.fetch_all_emby_items_generator(
            base_url=processor.emby_url, 
            api_key=processor.emby_api_key, 
            library_ids=libs_to_process_ids, 
            fields=req_fields
        )

        for item in item_generator:
            scan_count += 1
            if scan_count % 5000 == 0:
                task_manager.update_status_from_thread(10, f"正在索引 Emby 库 ({scan_count} 已扫描)...")
            
            item_id = str(item.get("Id"))
            parent_id = str(item.get("ParentId"))
            if item_id and parent_id:
                id_to_parent_map[item_id] = parent_id
            
            if not item_id: 
                continue

            emby_id_to_lib_id[item_id] = item.get('_SourceLibraryId')
            
            item_type = item.get("Type")
            tmdb_id = item.get("ProviderIds", {}).get("Tmdb")

            # 1. 记录所有扫描到的 ID (用于反向检测离线)
            # 注意：只有我们关心的类型才记录，否则会误判离线
            if item_type in ["Movie", "Series", "Season", "Episode"]:
                current_scan_emby_ids.add(item_id)
            else:
                skipped_other_type += 1
                continue # 跳过非媒体类型 (Folder, BoxSet等)

            # 实时更新映射
            if item_type == "Series" and tmdb_id:
                emby_sid_to_tmdb_id[item_id] = str(tmdb_id)
            
            if item_type in ["Movie", "Series"] and tmdb_id:
                tmdb_key_to_emby_ids[(str(tmdb_id), item_type)].add(item_id)

            # 跳过判断 (已存在且在线)
            is_clean = False
            if not force_full_update:
                if known_emby_status.get(item_id) is True:
                    is_clean = True
            
            if is_clean:
                skipped_clean += 1
                continue 

            # ★★★ 脏数据处理 ★★★
            
            # A. 顶层媒体
            if item_type in ["Movie", "Series"]:
                if tmdb_id:
                    composite_key = (str(tmdb_id), item_type)
                    top_level_items_map[composite_key].append(item)
                    dirty_keys.add(composite_key)
                else:
                    skipped_no_tmdb += 1 # 记录无 TMDb ID 的项目

            # B. 子集媒体
            elif item_type in ['Season', 'Episode']:
                s_id = str(item.get('SeriesId') or item.get('ParentId')) if item_type == 'Season' else str(item.get('SeriesId'))
                
                if item_type == 'Season':
                    if s_id: series_to_seasons_map[s_id].append(item)
                else:
                    if s_id: series_to_episode_map[s_id].append(item)

                if s_id and s_id in emby_sid_to_tmdb_id:
                    dirty_keys.add((emby_sid_to_tmdb_id[s_id], 'Series'))
                elif s_id:
                    pending_children.append((s_id, item_type))

        # 处理孤儿分集
        for s_id, _ in pending_children:
            if s_id in emby_sid_to_tmdb_id:
                dirty_keys.add((emby_sid_to_tmdb_id[s_id], 'Series'))

        gc.collect()

        # --- 3. 反向差异检测 (删除) ---
        if not force_full_update:
            active_db_ids = {k for k, v in known_emby_status.items() if v is True}
            missing_emby_ids = active_db_ids - current_scan_emby_ids
            
            del known_emby_status
            del active_db_ids
            del current_scan_emby_ids
            gc.collect()

            if missing_emby_ids:
                logger.info(f"  ➜ 检测到 {len(missing_emby_ids)} 个 Emby ID 已消失，正在处理离线标记...")
                missing_ids_list = list(missing_emby_ids)
                
                with connection.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT tmdb_id, item_type, parent_series_tmdb_id
                        FROM media_metadata 
                        WHERE in_library = TRUE 
                          AND EXISTS (
                              SELECT 1 
                              FROM jsonb_array_elements_text(emby_item_ids_json) as eid 
                              WHERE eid = ANY(%s)
                          )
                    """, (missing_ids_list,))
                    
                    rows = cursor.fetchall()
                    direct_offline_tmdb_ids = []
                    affected_parent_ids = set()
                    
                    for row in rows:
                        r_type = row['item_type']
                        r_tmdb = row['tmdb_id']
                        r_parent = row['parent_series_tmdb_id']
                        
                        if r_type in ['Movie', 'Series']:
                            direct_offline_tmdb_ids.append(r_tmdb)
                        elif r_type in ['Season', 'Episode'] and r_parent:
                            affected_parent_ids.add(r_parent)

                    if direct_offline_tmdb_ids:
                        logger.info(f"  ➜ 正在标记 {len(direct_offline_tmdb_ids)} 个顶层项目为离线...")
                        cursor.execute("""
                            UPDATE media_metadata
                            SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb, asset_details_json = '[]'::jsonb
                            WHERE tmdb_id = ANY(%s) AND item_type IN ('Movie', 'Series')
                        """, (direct_offline_tmdb_ids,))
                        total_offline_count += cursor.rowcount
                        
                    if affected_parent_ids:
                        logger.info(f"  ➜ 因分集消失，将 {len(affected_parent_ids)} 个父剧集加入刷新队列...")
                        for pid in affected_parent_ids:
                            dirty_keys.add((pid, 'Series'))
                    
                    conn.commit()

        # ★★★ 打印详细统计日志 ★★★
        logger.info(f"  ➜ Emby 扫描完成，共扫描 {scan_count} 个项。")
        logger.info(f"    - 已入库: {skipped_clean}")
        logger.info(f"    - 已跳过: {skipped_no_tmdb + skipped_other_type} (含 {skipped_no_tmdb} 个无ID, {skipped_other_type} 个非媒体)")
        logger.info(f"    - 需同步: {len(dirty_keys)}")

        # --- 4. 确定处理队列 (无需猜测类型) ---
        items_to_process = []
        
        # 直接遍历 dirty_keys，里面已经包含了准确的 (ID, Type)
        for (tmdb_id, item_type) in dirty_keys:
            
            # 使用复合键查找关联的 Emby IDs
            related_emby_ids = tmdb_key_to_emby_ids.get((tmdb_id, item_type), set())
            
            if not related_emby_ids:
                continue

            items_to_process.append({
                'tmdb_id': tmdb_id,
                'emby_ids': list(related_emby_ids),
                'type': item_type, # 直接使用 key 里的 type，绝对准确
                'refetch': True 
            })

        total_to_process = len(items_to_process)
        task_manager.update_status_from_thread(20, f"阶段3/3: 正在同步 {total_to_process} 个变更项目...")
        logger.info(f"  ➜ 最终处理队列: {total_to_process} 个顶层项目")

        # --- 5. 批量处理 ---
        processed_count = 0
        for i in range(0, total_to_process, batch_size):
            if processor.is_stop_requested(): break
            batch_tasks = items_to_process[i:i + batch_size]
            
            batch_item_groups = []
            
            # 预处理：拉取 refetch 的数据
            for task in batch_tasks:
                try:
                    target_emby_ids = task['emby_ids']
                    item_type = task['type']
                    
                    # 1. 批量获取这些 Emby ID 的详情
                    top_items = emby.get_emby_items_by_id(
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id,
                        item_ids=target_emby_ids,
                        fields=req_fields
                    )
                    
                    if not top_items: continue

                    # 因为 get_emby_items_by_id 重新拉取的数据没有这个字段，我们需要从之前的映射中补回去
                    for item in top_items:
                        eid = str(item.get('Id'))
                        if eid in emby_id_to_lib_id:
                            item['_SourceLibraryId'] = emby_id_to_lib_id[eid]

                    # 2. 如果是剧集，还需要拉取每个剧集的子集
                    if item_type == 'Series':
                        full_group = []
                        full_group.extend(top_items)
                        
                        # 清空旧的子集缓存，防止重复
                        for e_id in target_emby_ids:
                            series_to_seasons_map[e_id] = []
                            series_to_episode_map[e_id] = []
                        
                        children_gen = emby.fetch_all_emby_items_generator(
                            base_url=processor.emby_url,
                            api_key=processor.emby_api_key,
                            library_ids=target_emby_ids, 
                            fields=req_fields
                        )
                        
                        children_list = list(children_gen)
                        for child in children_list:
                            parent_series_id = str(child.get('SeriesId') or child.get('ParentId'))
                            if parent_series_id and parent_series_id in emby_id_to_lib_id:
                                real_lib_id = emby_id_to_lib_id[parent_series_id]
                                child['_SourceLibraryId'] = real_lib_id 
                        full_group.extend(children_list)
                        
                        # 重新填充 map
                        for child in children_list:
                            ct = child.get('Type')
                            pid = str(child.get('SeriesId') or child.get('ParentId'))
                            if pid:
                                if ct == 'Season': series_to_seasons_map[pid].append(child)
                                elif ct == 'Episode': series_to_episode_map[pid].append(child)
                        
                        batch_item_groups.append(full_group)
                    
                    else:
                        # 电影直接添加
                        batch_item_groups.append(top_items)

                except Exception as e:
                    logger.error(f"处理项目 {task.get('tmdb_id')} 失败: {e}")

            # --- 以下逻辑保持不变 (并发获取 TMDB 和 写入 DB) ---
            
            tmdb_details_map = {}
            def fetch_tmdb_details(item_group):
                if not item_group: return None, None
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
                if not item_group: continue
                item = item_group[0]
                tmdb_id_str = str(item.get("ProviderIds", {}).get("Tmdb"))
                item_type = item.get("Type")
                tmdb_details = tmdb_details_map.get(tmdb_id_str)
                
                # --- 1. 构建顶层记录 ---
                asset_details_list = []
                if item_type in ["Movie", "Series"]:
                    for v in item_group:
                        # 仅处理当前类型的项目 (防止 Series 组里混入 Season/Episode)
                        if v.get('Type') != item_type:
                            continue
                            
                        source_lib_id = str(v.get('_SourceLibraryId'))
                        current_lib_guid = lib_id_to_guid_map.get(source_lib_id)

                        details = parse_full_asset_details(
                            v, 
                            id_to_parent_map=id_to_parent_map, 
                            library_guid=current_lib_guid
                        )
                        details['source_library_id'] = source_lib_id
                        asset_details_list.append(details)

                emby_runtime = round(item['RunTimeTicks'] / 600000000) if item.get('RunTimeTicks') else None

                # 提取发行日期 
                emby_date = item.get('PremiereDate')
                tmdb_date = None
                if tmdb_details:
                    if item_type == 'Movie': 
                        tmdb_date = tmdb_details.get('release_date')
                    elif item_type == 'Series': 
                        tmdb_date = tmdb_details.get('first_air_date')
                
                final_release_date = emby_date or tmdb_date
                # 提取全量分级数据
                raw_ratings_map = _extract_and_map_tmdb_ratings(tmdb_details, item_type)
                # 序列化为 JSON 字符串，准备存入数据库
                rating_json_str = json.dumps(raw_ratings_map, ensure_ascii=False)
                top_record = {
                    "tmdb_id": tmdb_id_str, "item_type": item_type, "title": item.get('Name'),
                    "original_title": item.get('OriginalTitle'), "release_year": item.get('ProductionYear'),
                    "original_language": tmdb_details.get('original_language') if tmdb_details else None,
                    "in_library": True, 
                    "subscription_status": "NONE",
                    "emby_item_ids_json": json.dumps(list(set(v.get('Id') for v in item_group if v.get('Id') and v.get('Type') == item_type)), ensure_ascii=False),
                    "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "rating": item.get('CommunityRating'),
                    "date_added": item.get('DateCreated'),
                    "release_date": final_release_date,
                    "genres_json": json.dumps(item.get('Genres', []), ensure_ascii=False),
                    "tags_json": json.dumps(extract_tag_names(item), ensure_ascii=False),
                    "official_rating_json": rating_json_str,
                    "runtime_minutes": emby_runtime if (item_type == 'Movie' and emby_runtime) else tmdb_details.get('runtime') if (item_type == 'Movie' and tmdb_details) else None
                }
                if tmdb_details:
                    top_record['poster_path'] = tmdb_details.get('poster_path')
                    top_record['overview'] = tmdb_details.get('overview')
                    if tmdb_details.get('vote_average') is not None:
                        top_record['rating'] = tmdb_details.get('vote_average')
                    # 1. 获取基础制作公司
                    raw_studios = tmdb_details.get('production_companies', []) or []

                    # 2. 如果是电视剧，追加 Networks (电视台/流媒体平台)
                    if item_type == 'Series':
                        networks = tmdb_details.get('networks', []) or []
                        raw_studios.extend(networks)

                    # 3. 去重 (使用字典以 ID 为键进行去重) 并格式化
                    unique_studios_map = {}
                    for s in raw_studios:
                        s_id = s.get('id')
                        s_name = s.get('name')
                        if s_name:
                            # 如果 ID 冲突，后来的覆盖前面的（通常 Networks 在后，保留 Networks 更合理）
                            unique_studios_map[s_id] = {'id': s_id, 'name': s_name}

                    top_record['studios_json'] = json.dumps(list(unique_studios_map.values()), ensure_ascii=False)
                    if item_type == 'Movie':
                        top_record['runtime_minutes'] = tmdb_details.get('runtime')
                    
                    directors, countries, keywords = [], [], []
                    if item_type == 'Movie':
                        credits_data = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                        directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                        countries = [c.get('iso_3166_1') for c in tmdb_details.get('production_countries', []) if c.get('iso_3166_1')]
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('keywords', []) if isinstance(keywords_data, dict) else []
                        keywords = [{'id': k.get('id'), 'name': k.get('name')} for k in keyword_list if k.get('name')]
                    elif item_type == 'Series':
                        directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                        countries = tmdb_details.get('origin_country', [])
                        keywords_data = tmdb_details.get('keywords', {})
                        keyword_list = keywords_data.get('results', []) if isinstance(keywords_data, dict) else []
                        keywords = [{'id': k.get('id'), 'name': k.get('name')} for k in keyword_list if k.get('name')]
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
                    processed_season_numbers = set()
                    
                    if tmdb_details and 'seasons' in tmdb_details:
                        for s_info in tmdb_details.get('seasons', []):
                            try:
                                s_num = int(s_info.get('season_number'))
                            except (ValueError, TypeError):
                                continue
                            
                            matched_emby_seasons = []
                            for s in my_seasons:
                                try:
                                    if int(s.get('IndexNumber')) == s_num:
                                        matched_emby_seasons.append(s)
                                except (ValueError, TypeError):
                                    continue
                            
                            if matched_emby_seasons:
                                processed_season_numbers.add(s_num)
                                real_season_tmdb_id = str(s_info.get('id'))
                                season_poster = s_info.get('poster_path')
                                if not season_poster and tmdb_details:
                                    season_poster = tmdb_details.get('poster_path')

                                # 提取季发行日期
                                s_release_date = s_info.get('air_date')
                                
                                if not s_release_date and matched_emby_seasons:
                                    s_release_date = matched_emby_seasons[0].get('PremiereDate')
                                
                                # 核心逻辑：如果还没找到，遍历该季下的分集找最早的
                                if not s_release_date:
                                    # 筛选出属于当前季(s_num)且有日期的分集
                                    ep_dates = [
                                        e.get('PremiereDate') for e in my_episodes 
                                        if e.get('ParentIndexNumber') == s_num and e.get('PremiereDate')
                                    ]
                                    if ep_dates:
                                        # 取最早的日期作为季的发行日期
                                        s_release_date = min(ep_dates)
                                season_record = {
                                    "tmdb_id": real_season_tmdb_id,
                                    "item_type": "Season",
                                    "parent_series_tmdb_id": tmdb_id_str,
                                    "season_number": s_num,
                                    "title": s_info.get('name'),
                                    "overview": s_info.get('overview'),
                                    "poster_path": season_poster,
                                    "rating": s_info.get('vote_average'),
                                    "in_library": True,
                                    "release_date": s_release_date,
                                    "subscription_status": "NONE",
                                    "emby_item_ids_json": json.dumps([s.get('Id') for s in matched_emby_seasons]),
                                    "tags_json": json.dumps(extract_tag_names(matched_emby_seasons[0]) if matched_emby_seasons else [], ensure_ascii=False),
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

                    # B. 兜底处理
                    for s in my_seasons:
                        try:
                            s_num = int(s.get('IndexNumber'))
                        except (ValueError, TypeError):
                            continue

                        if s_num not in processed_season_numbers:
                            # 兜底逻辑也加上分集日期推断 
                            s_release_date = s.get('PremiereDate')
                            if not s_release_date:
                                ep_dates = [
                                    e.get('PremiereDate') for e in my_episodes 
                                    if e.get('ParentIndexNumber') == s_num and e.get('PremiereDate')
                                ]
                                if ep_dates:
                                    s_release_date = min(ep_dates)
                            fallback_season_tmdb_id = f"{tmdb_id_str}-S{s_num}"
                            season_record = {
                                "tmdb_id": fallback_season_tmdb_id,
                                "item_type": "Season",
                                "parent_series_tmdb_id": tmdb_id_str,
                                "season_number": s_num,
                                "title": s.get('Name') or f"Season {s_num}",
                                "overview": None,
                                "poster_path": tmdb_details.get('poster_path') if tmdb_details else None,
                                "in_library": True,
                                "release_date": s_release_date,
                                "subscription_status": "NONE",
                                "emby_item_ids_json": json.dumps([s.get('Id')]),
                                "tags_json": json.dumps(extract_tag_names(s), ensure_ascii=False),
                                "ignore_reason": "Local Season Only"
                            }
                            metadata_batch.append(season_record)
                            processed_season_numbers.add(s_num)

                    # C. 处理分集
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
                        
                        ep_asset_details_list = []
                        for v in versions:
                            details = parse_full_asset_details(v) 
                            ep_asset_details_list.append(details)

                        # 提取分集发行日期 
                        ep_release_date = emby_ep.get('PremiereDate')
                        if not ep_release_date and tmdb_ep_info:
                            ep_release_date = tmdb_ep_info.get('air_date')
                        child_record = {
                            "item_type": "Episode",
                            "parent_series_tmdb_id": tmdb_id_str,
                            "season_number": s_n,
                            "episode_number": e_n,
                            "in_library": True,
                            "release_date": ep_release_date,
                            "rating": emby_ep.get('CommunityRating'),
                            "emby_item_ids_json": json.dumps([v.get('Id') for v in versions]),
                            "asset_details_json": json.dumps(ep_asset_details_list, ensure_ascii=False),
                            "tags_json": json.dumps(extract_tag_names(versions[0]), ensure_ascii=False),
                            "ignore_reason": None
                        }

                        if tmdb_ep_info and tmdb_ep_info.get('id'):
                            child_record['tmdb_id'] = str(tmdb_ep_info.get('id'))
                            child_record['title'] = tmdb_ep_info.get('name')
                            child_record['overview'] = tmdb_ep_info.get('overview')
                            child_record['poster_path'] = tmdb_ep_info.get('still_path')
                            child_record['runtime_minutes'] = emby_ep_runtime if emby_ep_runtime else tmdb_ep_info.get('runtime')
                            if tmdb_ep_info.get('vote_average') is not None:
                                child_record['rating'] = tmdb_ep_info.get('vote_average')
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
                                # 在 UPDATE 时排除 订阅状态和订阅来源
                                if col in ('tmdb_id', 'item_type', 'subscription_sources_json', 'subscription_status'): 
                                    continue
                                
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
            
            del batch_item_groups
            del tmdb_details_map
            del metadata_batch
            gc.collect()

            processed_count += len(batch_tasks)
            task_manager.update_status_from_thread(20 + int((processed_count / total_to_process) * 80), f"处理进度 {processed_count}/{total_to_process}...")

        final_msg = f"同步完成！新增/更新: {total_updated_count} 个媒体项, 标记离线: {total_offline_count} 个媒体项。"
        logger.info(f"  ✅ {final_msg}")
        task_manager.update_status_from_thread(100, final_msg)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 自动打标 ---
def task_bulk_auto_tag(processor, library_ids: List[str], tags: List[str]):
    """
    后台任务：支持为多个媒体库批量打标签。
    """
    try:
        total_libs = len(library_ids)
        for lib_idx, lib_id in enumerate(library_ids):
            task_manager.update_status_from_thread(int((lib_idx/total_libs)*100), f"正在扫描第 {lib_idx+1}/{total_libs} 个媒体库...")
            
            items = emby.get_emby_library_items(
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                library_ids=[lib_id],
                media_type_filter="Movie,Series,Episode",
                user_id=processor.emby_user_id
            )
            
            if not items: continue

            for i, item in enumerate(items):
                if processor.is_stop_requested(): return
                
                # 进度显示优化：显示当前库的进度
                task_manager.update_status_from_thread(
                    int((lib_idx/total_libs)*100 + (i/len(items))*(100/total_libs)), 
                    f"库({lib_idx+1}/{total_libs}) 正在打标: {item.get('Name')}"
                )
                
                emby.add_tags_to_item(item.get("Id"), tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        
        task_manager.update_status_from_thread(100, "所有选定库批量打标完成")
    except Exception as e:
        logger.error(f"批量打标任务失败: {e}")
        task_manager.update_status_from_thread(-1, "任务异常中止")

def task_bulk_remove_tags(processor, library_ids: List[str], tags: List[str]):
    """
    后台任务：从指定媒体库中批量移除特定标签。
    """
    try:
        total_libs = len(library_ids)
        for lib_idx, lib_id in enumerate(library_ids):
            items = emby.get_emby_library_items(
                base_url=processor.emby_url, api_key=processor.emby_api_key,
                library_ids=[lib_id], media_type_filter="Movie,Series,Episode",
                user_id=processor.emby_user_id
            )
            if not items: continue

            for i, item in enumerate(items):
                if processor.is_stop_requested(): return
                task_manager.update_status_from_thread(
                    int((lib_idx/total_libs)*100 + (i/len(items))*(100/total_libs)), 
                    f"正在移除标签({lib_idx+1}/{total_libs}): {item.get('Name')}"
                )
                emby.remove_tags_from_item(item.get("Id"), tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        
        task_manager.update_status_from_thread(100, "批量标签移除完成")
    except Exception as e:
        logger.error(f"批量清理任务失败: {e}")
        task_manager.update_status_from_thread(-1, "清理任务异常中止")

# ★★★ 分级同步特种部队 ★★★
def task_sync_ratings_to_emby(processor, force_full_update: bool = False):
    """
    【分级同步任务】
    force_full_update=True: 同步 CustomRating + OfficialRating (单向强制: DB US -> Emby)。
    force_full_update=False: 仅同步 CustomRating (双向互补: 有覆盖无)。
    """
    mode = 'deep' if force_full_update else 'fast'
    logger.info(f"--- 开始执行分级同步任务 (模式: {mode}) ---")
    
    # 1. 从数据库获取所有在库项目
    # 我们只需要查那些确实在库里的，不在库的同步了也没意义
    with connection.get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT tmdb_id, item_type, emby_item_ids_json, custom_rating, official_rating_json 
            FROM media_metadata 
            WHERE in_library = TRUE 
              AND emby_item_ids_json IS NOT NULL 
              AND jsonb_array_length(emby_item_ids_json) > 0
        """)
        all_items = cursor.fetchall()

    total_items = len(all_items)
    logger.info(f"  ➜ 扫描到 {total_items} 个在库项目，准备进行差异比对...")
    
    # 分批处理，避免内存爆炸
    BATCH_SIZE = 200
    updated_emby_count = 0
    updated_db_count = 0
    
    for i in range(0, total_items, BATCH_SIZE):
        if processor.is_stop_requested(): break
        
        batch = all_items[i : i + BATCH_SIZE]
        
        # 提取这一批的 Emby ID
        emby_id_map = {} # {emby_id: db_row}
        emby_ids_to_fetch = []
        
        for row in batch:
            try:
                e_ids = row['emby_item_ids_json']
                if e_ids:
                    # 通常取第一个 ID 即可
                    eid = e_ids[0]
                    emby_id_map[eid] = row
                    emby_ids_to_fetch.append(eid)
            except: continue

        if not emby_ids_to_fetch: continue

        # 批量获取 Emby 侧的现状
        # 我们只需要 OfficialRating, CustomRating, LockedFields
        emby_items = emby.get_emby_items_by_id(
            base_url=processor.emby_url,
            api_key=processor.emby_api_key,
            user_id=processor.emby_user_id,
            item_ids=emby_ids_to_fetch,
            fields="OfficialRating,CustomRating,LockedFields,Name"
        )
        
        for e_item in emby_items:
            eid = e_item['Id']
            db_row = emby_id_map.get(eid)
            if not db_row: continue
            
            tmdb_id = db_row['tmdb_id']
            item_type = db_row['item_type']
            item_name = e_item.get('Name', tmdb_id)
            
            # --- 数据准备 ---
            db_custom = db_row['custom_rating']
            emby_custom = e_item.get('CustomRating')
            
            db_official_json = db_row['official_rating_json'] or {}
            # 这里的 json 可能是 dict 也可能是 str，psycopg2 cursor_factory=RealDictCursor 通常会自动转 dict
            # 但为了保险，如果是 str 就 load 一下
            if isinstance(db_official_json, str):
                try: db_official_json = json.loads(db_official_json)
                except: db_official_json = {}
            
            # 提取 DB 里的 US 分级 (这是我们的真理标准)
            db_us_rating = db_official_json.get('US')
            emby_official = e_item.get('OfficialRating')

            changes_to_emby = {}
            changes_to_db = {}

            # =========================================================
            # 逻辑 A: CustomRating (双向互补 - 有覆盖无)
            # =========================================================
            # 1. DB 有，Emby 无 -> 推给 Emby (恢复丢失的数据)
            if db_custom and not emby_custom:
                changes_to_emby['CustomRating'] = db_custom
            
            # 2. Emby 有，DB 无 -> 拉回 DB (保存用户在前端的操作)
            elif emby_custom and not db_custom:
                changes_to_db['custom_rating'] = emby_custom
            
            # 3. 都有，但不一致 -> 以 DB 为准 (防止 Emby 瞎改，或者用户想回滚)
            # 这里你也可以选择以 Emby 为准，看你觉得哪边更权威。
            # 既然你说 "Emby一刷新就没了"，说明 DB 是避风港，所以冲突时信 DB。
            elif db_custom and emby_custom and db_custom != emby_custom:
                changes_to_emby['CustomRating'] = db_custom

            # =========================================================
            # 逻辑 B: OfficialRating (深度模式 - 单向强制 DB->Emby)
            # =========================================================
            if mode == 'deep':
                # 只有当 DB 里明确有 US 分级，且 Emby 当前分级不一致时，才覆盖
                # 这样能解决 "虚拟库看得到(因为读DB)，Emby看不到(因为Emby分级错)" 的灰块问题
                if db_us_rating and db_us_rating != emby_official:
                    changes_to_emby['OfficialRating'] = db_us_rating
                    
                    # 如果 Emby 锁定了 OfficialRating，我们需要解锁吗？
                    # update_emby_item_details 内部逻辑通常不处理解锁，
                    # 如果需要强行覆盖，最好把 LockedFields 也处理一下
                    locked = e_item.get('LockedFields', [])
                    if 'OfficialRating' in locked:
                        locked.remove('OfficialRating')
                        changes_to_emby['LockedFields'] = locked

            # =========================================================
            # 执行更新
            # =========================================================
            
            # 1. 更新 Emby
            if changes_to_emby:
                success = emby.update_emby_item_details(
                    item_id=eid,
                    new_data=changes_to_emby,
                    emby_server_url=processor.emby_url,
                    emby_api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
                if success:
                    updated_emby_count += 1
                    logger.trace(f"  ➜ [同步->Emby] {item_name}: {changes_to_emby}")

            # 2. 更新 DB
            if changes_to_db:
                media_db.update_media_metadata_fields(tmdb_id, item_type, changes_to_db)
                updated_db_count += 1
                logger.trace(f"  ➜ [同步->DB] {item_name}: {changes_to_db}")

        # 进度汇报
        progress = int((i / total_items) * 100)
        task_manager.update_status_from_thread(progress, f"分级同步({mode}): 已处理 {i}/{total_items}...")

    logger.info(f"--- 分级同步完成 ({mode}) ---")
    logger.info(f"  ➜ 推送给 Emby 的更新: {updated_emby_count} 条")
    logger.info(f"  ➜ 拉取回 DB 的更新: {updated_db_count} 条")
    task_manager.update_status_from_thread(100, f"分级同步完成: Emby更新{updated_emby_count}, DB更新{updated_db_count}")
