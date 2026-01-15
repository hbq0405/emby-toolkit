# tasks/media.py
# 核心媒体处理、元数据、资产同步等

import time
import json
import gc
import os
import re
import logging
from typing import List, Optional
import concurrent.futures
from collections import defaultdict
from gevent import spawn_later
# 导入需要的底层模块和共享实例
import task_manager
import utils
import constants
import handler.tmdb as tmdb
import handler.emby as emby
import handler.telegram as telegram
from database import connection, settings_db, media_db, queries_db
from .helpers import parse_full_asset_details, reconstruct_metadata_from_db
from extensions import UPDATING_METADATA

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

def _wait_for_items_recovery(processor, item_ids: list, max_retries=60, interval=10) -> bool:
    """
    轮询检查指定的一组 Emby ID 是否都已具备有效的视频流数据。
    用于等待神医插件处理网盘文件。
    """
    if not item_ids:
        return True

    logger.info(f"  ⏳ 开始轮询监控 {len(item_ids)} 个项目的修复进度 (最大等待 {max_retries*interval}秒)...")
    
    # 使用集合来管理还需要等待的ID，修复一个移除一个
    pending_ids = set(item_ids)
    
    for i in range(max_retries):
        if processor.is_stop_requested(): return False
        
        # 复制一份当前待处理列表进行遍历
        current_check_list = list(pending_ids)
        
        for eid in current_check_list:
            try:
                # 获取详情 (只查 MediaSources 即可)
                item_details = emby.get_emby_item_details(
                    eid, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                    fields="MediaSources"
                )
                
                is_healed = False
                if item_details:
                    media_sources = item_details.get("MediaSources", [])
                    for source in media_sources:
                        # 排除未分析的 strm
                        if not source.get("Container") and not source.get("Path", "").endswith(".strm"):
                            continue
                            
                        for stream in source.get("MediaStreams", []):
                            if stream.get("Type") == "Video":
                                w = stream.get("Width")
                                h = stream.get("Height")
                                c = stream.get("Codec")
                                # 使用严格标准检查
                                valid, _ = utils.check_stream_validity(w, h, c)
                                if valid:
                                    is_healed = True
                                    break
                        if is_healed: break
                
                if is_healed:
                    logger.debug(f"    ✔ 项目 {eid} 已检测到完整媒体信息，移除监控队列。")
                    pending_ids.remove(eid)
                    
            except Exception:
                pass # 网络错误暂时忽略，下次重试
        
        if not pending_ids:
            logger.info(f"  ✅ 所有目标项目均已修复完成 (耗时 {i*interval}秒)！")
            return True
            
        if i % 2 == 0: # 每20秒打印一次进度
            logger.info(f"  ⏳ 等待神医提取媒体信息中... 剩余 {len(pending_ids)}/{len(item_ids)} 个项目 (轮询 {i+1}/{max_retries})")
            
        time.sleep(interval)

    logger.warning(f"  ⚠️ 等待超时！仍有 {len(pending_ids)} 个项目未获取到完整信息，将强制继续处理。")
    return False

# --- 重新处理单个项目 ---
def task_reprocess_single_item(processor, item_id: str, item_name_for_ui: str):
    """
    【最终版 - 神医联动 & 智能轮询】后台任务。
    流程：ID纠错 -> 查找坏分集 -> 触发神医 -> 轮询等待(最长10分钟) -> 核心处理(入库)。
    """
    logger.trace(f"  ➜ 后台任务开始执行 ({item_name_for_ui})")
    
    try:
        task_manager.update_status_from_thread(0, f"正在处理: {item_name_for_ui}")

        # 2. 神医插件联动流程
        try:
            item_basic = emby.get_emby_item_details(
                item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                fields="Type,ProviderIds"
            )
            
            if item_basic:
                item_type = item_basic.get('Type')
                tmdb_id = item_basic.get('ProviderIds', {}).get('Tmdb')
                
                ids_to_heal = []
                
                # A. 确定需要治疗的目标 ID 列表
                if item_type == 'Movie':
                    ids_to_heal.append(item_id)
                elif item_type == 'Series' and tmdb_id:
                    logger.info(f"  ➜ [神医联动] 正在扫描剧集 '{item_name_for_ui}' 下的异常分集...")
                    bad_episode_ids = media_db.get_bad_episode_emby_ids(str(tmdb_id))
                    if bad_episode_ids:
                        logger.info(f"  ➜ [神医联动] 发现 {len(bad_episode_ids)} 个分集缺失媒体信息。")
                        ids_to_heal.extend(bad_episode_ids)
                    else:
                        logger.info(f"  ➜ [神医联动] 未发现明显的坏分集，将跳过触发步骤。")

                # B. 执行治疗与等待
                if ids_to_heal:
                    # 1. 触发
                    task_manager.update_status_from_thread(10, f"正在触发神医插件修复 {len(ids_to_heal)} 个文件...")
                    for eid in ids_to_heal:
                        emby.trigger_media_info_refresh(
                            eid, processor.emby_url, processor.emby_api_key, processor.emby_user_id
                        )
                        time.sleep(0.2) # 稍微间隔
                    
                    # 2. 轮询等待 (关键修改)
                    task_manager.update_status_from_thread(20, f"等待媒体信息提取 (最长10分钟)...")
                    _wait_for_items_recovery(processor, ids_to_heal, max_retries=60, interval=10)
                    
        except Exception as e_heal:
            logger.warning(f"  ⚠️ [神医联动] 流程出现小插曲 (不影响后续重扫): {e_heal}")

        # 3. 执行标准处理流程 (验收成果)
        task_manager.update_status_from_thread(50, f"正在重新刮削元数据: {item_name_for_ui}")
        
        processor.process_single_item(
            item_id, 
            force_full_update=True
        )
        
        logger.trace(f"  ➜ 后台任务完成 ({item_name_for_ui})")

    except Exception as e:
        logger.error(f"后台任务处理 '{item_name_for_ui}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"处理失败: {item_name_for_ui}")

# --- 重新处理所有待复核项 ---
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

    # 无论原始数据里有没有 US 分级，只要 TMDb 说是成人，就必须是 AO
    if tmdb_details.get('adult') is True:
        ratings_map['US'] = 'XXX'
        return ratings_map # 既然是成人，直接返回，不需要后续的映射逻辑了

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
                        
                        is_tv_code = r_code.upper().startswith('TV-') or r_code.upper() == 'TV-Y7' # 确保涵盖所有TV格式
                        
                        # 1. 如果是电影，跳过 TV 分级
                        if item_type == 'Movie' and is_tv_code:
                            continue
                            
                        # 2. 如果是剧集，跳过非 TV 分级 (强制要求 TV- 开头)
                        # 注意：US分级中，电视剧通常严格使用 TV-Y, TV-G, TV-14 等
                        if item_type == 'Series' and not is_tv_code:
                            continue

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
def task_populate_metadata_cache(processor, batch_size: int = 10, force_full_update: bool = False):
    """
    - 重量级的元数据缓存填充任务 (类型安全版)。
    - 修复：彻底解决 TMDb ID 在电影和剧集间冲突的问题。
    - 修复：完善离线检测逻辑，确保消失的电影/剧集能被正确标记为离线。
    - 优化：移除无用的中间数据缓存，大幅降低内存占用。
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
        # ★★★ 内存优化 1: 改用 Set 只存 ID，不存 True/False，节省一半内存 ★★★
        known_online_emby_ids = set() 
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
                # ★★★ 内存优化 1: 只查询在线的 ID ★★★
                cursor.execute("""
                    SELECT jsonb_array_elements_text(emby_item_ids_json) AS emby_id
                    FROM media_metadata 
                    WHERE in_library = TRUE
                """)
                for row in cursor.fetchall():
                    known_online_emby_ids.add(row['emby_id'])
                
                cursor.execute("""
                    SELECT COUNT(*) as total, SUM(CASE WHEN in_library THEN 1 ELSE 0 END) as online 
                    FROM media_metadata
                """)
                stat_row = cursor.fetchone()
                total_items = stat_row['total'] if stat_row else 0
                online_items = stat_row['online'] if stat_row and stat_row['online'] is not None else 0
                
                logger.info(f"  ➜ 本地数据库共存储 {total_items} 个媒体项 (其中在线: {online_items})。")

        logger.info("  ➜ 正在预加载 Emby 文件夹路径映射...")
        folder_map = emby.get_all_folder_mappings(processor.emby_url, processor.emby_api_key)
        logger.info(f"  ➜ 加载了 {len(folder_map)} 个文件夹路径节点。")

        # --- 2. 扫描 Emby (流式处理) ---
        task_manager.update_status_from_thread(10, f"阶段2/3: 扫描 Emby 并计算差异...")
        
        # ★★★ 内存优化 2: 彻底移除无用的累积字典 (top_level_items_map 等) ★★★
        # 这些字典之前只存不取，是导致爆内存的元凶
        
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
            if item_type in ["Movie", "Series", "Season", "Episode"]:
                current_scan_emby_ids.add(item_id)
            else:
                skipped_other_type += 1
                continue 

            # 实时更新映射
            if item_type == "Series" and tmdb_id:
                emby_sid_to_tmdb_id[item_id] = str(tmdb_id)
            
            if item_type in ["Movie", "Series"] and tmdb_id:
                tmdb_key_to_emby_ids[(str(tmdb_id), item_type)].add(item_id)

            # 跳过判断 (已存在且在线)
            is_clean = False
            if not force_full_update:
                # ★★★ 内存优化 1: 使用 Set 查找 ★★★
                if item_id in known_online_emby_ids:
                    is_clean = True
            
            if is_clean:
                skipped_clean += 1
                continue 

            # ★★★ 脏数据处理 (内存优化版) ★★★
            # 不再存储 item 对象，只记录 ID 关系
            
            # A. 顶层媒体
            if item_type in ["Movie", "Series"]:
                if tmdb_id:
                    composite_key = (str(tmdb_id), item_type)
                    # top_level_items_map[composite_key].append(item) # <--- 删除这行
                    dirty_keys.add(composite_key)
                else:
                    skipped_no_tmdb += 1 

            # B. 子集媒体
            elif item_type in ['Season', 'Episode']:
                s_id = str(item.get('SeriesId') or item.get('ParentId')) if item_type == 'Season' else str(item.get('SeriesId'))
                
                # series_to_seasons_map/series_to_episode_map 也不需要了，因为后面会重新 fetch
                
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
            # known_online_emby_ids 本身就是 active_db_ids
            missing_emby_ids = known_online_emby_ids - current_scan_emby_ids
            
            del known_online_emby_ids # 释放内存
            del current_scan_emby_ids
            gc.collect()

            if missing_emby_ids:
                # ... (保留原有的离线处理逻辑) ...
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

            series_to_seasons_map = defaultdict(list)
            series_to_episode_map = defaultdict(list)
            
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
                    if i_type == 'Movie': 
                        details = tmdb.get_movie_details(t_id, processor.tmdb_api_key)
                    elif i_type == 'Series': 
                        # 使用聚合函数，并发获取所有季信息
                        # 注意：外层已经是并发了，这里 max_workers 设小一点（如 3），防止瞬间请求过多触发 429
                        details = tmdb.aggregate_full_series_data_from_tmdb(t_id, processor.tmdb_api_key, max_workers=2)
                except Exception: pass
                return str(t_id), details

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
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

                full_aggregated_data = tmdb_details_map.get(tmdb_id_str)
                tmdb_details = None
                pre_fetched_episodes = {} # 用于存储预获取的分集信息

                if item_type == 'Series' and full_aggregated_data:
                    # 如果是 Series，full_aggregated_data 是一个包含 series_details, seasons_details, episodes_details 的字典
                    tmdb_details = full_aggregated_data.get('series_details')
                    pre_fetched_episodes = full_aggregated_data.get('episodes_details', {})
                else:
                    # Movie 或其他情况，保持原样
                    tmdb_details = full_aggregated_data
                
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
                emby_date = item.get('PremiereDate') or None
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
                # 构建 Genres 数据 
                # 默认使用 Emby 数据 (格式化为对象列表)
                final_genres_list = []
                for g in item.get('Genres', []):
                    name = g
                    if name in utils.GENRE_TRANSLATION_PATCH:
                        name = utils.GENRE_TRANSLATION_PATCH[name]
                    final_genres_list.append({"id": 0, "name": name})
                
                # 如果有 TMDb 详情，优先使用 TMDb 的 Genres (带 ID)
                if tmdb_details and tmdb_details.get('genres'):
                    final_genres_list = []
                    for g in tmdb_details.get('genres', []):
                        if isinstance(g, dict):
                            name = g.get('name')
                            if name in utils.GENRE_TRANSLATION_PATCH:
                                name = utils.GENRE_TRANSLATION_PATCH[name]
                            final_genres_list.append({"id": g.get('id', 0), "name": name})
                        elif isinstance(g, str):
                            name = g
                            if name in utils.GENRE_TRANSLATION_PATCH:
                                name = utils.GENRE_TRANSLATION_PATCH[name]
                            final_genres_list.append({"id": 0, "name": name})
                top_record = {
                    "tmdb_id": tmdb_id_str, "item_type": item_type, "title": item.get('Name'),
                    "original_title": item.get('OriginalTitle'), "release_year": item.get('ProductionYear'),
                    "original_language": tmdb_details.get('original_language') if tmdb_details else None,
                    "in_library": True, 
                    "subscription_status": "NONE",
                    "emby_item_ids_json": json.dumps(list(set(v.get('Id') for v in item_group if v.get('Id') and v.get('Type') == item_type)), ensure_ascii=False),
                    "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "rating": item.get('CommunityRating'),
                    "date_added": item.get('DateCreated') or None,
                    "release_date": final_release_date,
                    "genres_json": json.dumps(final_genres_list, ensure_ascii=False),
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
                    raw_studios = []
                    if item_type == 'Series':
                        # 剧集：只要播出平台 (Networks)，不要制作公司
                        raw_studios = tmdb_details.get('networks') or []
                    else:
                        # 电影：保留制作公司
                        raw_studios = tmdb_details.get('production_companies') or []
                    
                    # 确保是列表
                    if not isinstance(raw_studios, list):
                        raw_studios = []

                    # 2. 去重 (使用字典以 ID 为键进行去重) 并格式化
                    unique_studios_map = {}
                    for s in raw_studios:
                        s_id = s.get('id')
                        s_name = s.get('name')
                        if s_name:
                            # 如果 ID 冲突，后来的覆盖前面的
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
                                s_release_date = s_info.get('air_date') or None
                                
                                if not s_release_date and matched_emby_seasons:
                                    s_release_date = matched_emby_seasons[0].get('PremiereDate') or None
                                
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

                                for key, ep_data in pre_fetched_episodes.items():
                                    # key 格式为 S1E1
                                    if key.startswith(f"S{s_num}E"):
                                        tmdb_children_map[key] = ep_data

                    # B. 兜底处理
                    for s in my_seasons:
                        try:
                            s_num = int(s.get('IndexNumber'))
                        except (ValueError, TypeError):
                            continue

                        if s_num not in processed_season_numbers:
                            # 兜底逻辑也加上分集日期推断 
                            s_release_date = s.get('PremiereDate') or None
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
                            ep_release_date = tmdb_ep_info.get('air_date') or None
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
                            current_type = metadata.get('item_type')
                        
                            for col in columns:
                                # ★★★ 2. 定义基础排除列表 ★★★
                                # 这些字段永远不更新
                                exclude_cols = {'tmdb_id', 'item_type', 'subscription_sources_json', 'subscription_status'}
                                
                                # ★★★ 3. 动态判断是否排除标题 ★★★
                                # 只有当类型是 电影(Movie) 或 剧集(Series) 时，才排除 title
                                # 这样 季(Season) 和 集(Episode) 的标题依然可以正常同步更新
                                if current_type in ['Movie', 'Series']:
                                    exclude_cols.add('title')

                                if col in exclude_cols: 
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

        # 8. 执行大扫除：物理删除废弃的内部 ID 条目
        logger.info("  ➜ [自动维护] 正在清理废弃的内部ID兜底记录...")
        cleaned_zombies = media_db.cleanup_offline_internal_ids()
        if cleaned_zombies > 0:
            logger.info(f"  🧹 [大扫除] 成功物理删除了 {cleaned_zombies} 条已废弃的内部ID记录 (如 xxx-S1E1)。")
            
        final_msg = f"同步完成！新增/更新: {total_updated_count} 个媒体项, 标记离线: {total_offline_count} 个媒体项。"
        logger.info(f"  ✅ {final_msg}")
        # 自动触发分级同步 
        logger.info("  ➜ [自动触发] 元数据更新完毕，开始同步分级信息到 Emby...")
        # 为了防止分级同步的进度条覆盖掉主任务的进度，我们可以选择不传 update_status_callback 或者接受它重置进度
        # 这里直接调用，日志会记录过程
        try:
            task_sync_ratings_to_emby(processor)
        except Exception as e:
            logger.error(f"  ⚠️ 自动分级同步失败 (不影响主任务完成): {e}")
        task_manager.update_status_from_thread(100, final_msg)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 辅助函数：检查分级是否匹配 (带日志调试版) ---
def _is_rating_match(item_name: str, item_rating: str, rating_filters: List[str]) -> bool:
    """
    检查 Emby 的 OfficialRating 是否匹配指定的中分级标签列表。
    """
    if not rating_filters:
        return True # 未设置过滤器，默认匹配所有
    
    # 1. 如果项目没有分级，直接不匹配
    if not item_rating:
        # logger.trace(f"  [分级过滤] '{item_name}' 无分级信息 -> 跳过")
        return False 

    # 2. 将中文标签（如"限制级"）展开为所有可能的代码（如"R", "NC-17"）
    target_codes = queries_db._expand_rating_labels(rating_filters)
    
    # 3. 检查匹配
    # Emby 的 OfficialRating 可能是 "R" 也可能是 "US: R"，这里做宽松匹配
    is_match = item_rating in target_codes or \
               (item_rating.split(':')[-1].strip() in target_codes)
    
    # logger.trace(f"  [分级过滤] '{item_name}' 分级: {item_rating} | 目标: {target_codes} | 匹配: {is_match}")
    return is_match

# --- 执行自动打标规则任务 ---
def task_execute_auto_tagging_rules(processor):
    """
    任务：读取数据库中的自动打标规则，并依次执行。
    """
    rules = settings_db.get_setting('auto_tagging_rules') or []
    if not rules:
        logger.info("  ➜ [自动打标] 未配置任何规则，任务结束。")
        return

    total_rules = len(rules)
    logger.info(f"  ➜ [自动打标] 开始执行 {total_rules} 条规则...")

    for idx, rule in enumerate(rules):
        if processor.is_stop_requested(): 
            logger.info("  🚫 任务被中止。")
            break

        tags = rule.get('tags')
        if not tags: continue
        
        library_ids = rule.get('library_ids', [])
        rating_filters = rule.get('rating_filters', [])
        
        # 直接调用现有的批量打标逻辑
        # 注意：task_bulk_auto_tag 内部会处理进度更新和异常捕获
        task_bulk_auto_tag(processor, library_ids, tags, rating_filters)

    task_manager.update_status_from_thread(100, "自动打标规则执行完毕")

# --- 自动打标 (修复进度条卡顿版) ---
def task_bulk_auto_tag(processor, library_ids: List[str], tags: List[str], rating_filters: Optional[List[str]] = None):
    """
    后台任务：支持为多个媒体库批量打标签 (支持分级过滤，优先使用自定义分级)。
    """
    try:
        if not library_ids:
            logger.info("  ➜ 未指定媒体库，将扫描所有库...")
            all_libs = emby.get_emby_libraries(processor.emby_url, processor.emby_api_key, processor.emby_user_id)
            if all_libs:
                # 过滤掉合集、播放列表等非内容库
                library_ids = [l['Id'] for l in all_libs if l.get('CollectionType') not in ['boxsets', 'playlists', 'music']]
        
        total_libs = len(library_ids)
        filter_msg = f" (分级限制: {','.join(rating_filters)})" if rating_filters else ""
        
        for lib_idx, lib_id in enumerate(library_ids):
            # 初始状态更新
            task_manager.update_status_from_thread(int((lib_idx/total_libs)*100), f"正在读取第 {lib_idx+1}/{total_libs} 个媒体库...")
            
            # ★★★ 2. 请求 OfficialRating 和 CustomRating 字段 ★★★
            items = emby.get_emby_library_items(
                base_url=processor.emby_url,
                api_key=processor.emby_api_key,
                library_ids=[lib_id],
                media_type_filter="Movie,Series,Episode",
                user_id=processor.emby_user_id,
                fields="Id,Name,OfficialRating,CustomRating" 
            )
            
            if not items: 
                logger.info(f"  媒体库 {lib_id} 为空或无法访问。")
                continue

            total_items = len(items)
            logger.info(f"  媒体库 {lib_id} 扫描到 {total_items} 个项目，开始过滤...")
            
            processed_count = 0
            skipped_count = 0

            for i, item in enumerate(items):
                if processor.is_stop_requested(): return
                
                item_name = item.get('Name', '未知')
                
                # ★★★ 修复点：将进度更新移到过滤逻辑之前，并提高频率 ★★★
                if i % 5 == 0:
                    # 计算全局进度
                    current_progress = int((lib_idx/total_libs)*100 + (i/total_items)*(100/total_libs))
                    task_manager.update_status_from_thread(
                        current_progress, 
                        f"库({lib_idx+1}/{total_libs}) 正在扫描: {item_name}"
                    )

                # ★★★ 3. 分级过滤逻辑 (自定义分级优先) ★★★
                if rating_filters:
                    # 优先取 CustomRating，如果没有则取 OfficialRating
                    item_rating = item.get('CustomRating') or item.get('OfficialRating')
                    
                    if not _is_rating_match(item_name, item_rating, rating_filters):
                        skipped_count += 1
                        continue # 分级不匹配，跳过

                
                # 执行打标
                success = emby.add_tags_to_item(item.get("Id"), tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if success:
                    processed_count += 1

            logger.info(f"  媒体库 {lib_id} 处理完成: 打标 {processed_count} 个, 跳过 {skipped_count} 个 (不符分级)。")
        
        task_manager.update_status_from_thread(100, "所有选定库批量打标完成")
    except Exception as e:
        logger.error(f"批量打标任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务异常中止")

def task_bulk_remove_tags(processor, library_ids: List[str], tags: List[str], rating_filters: Optional[List[str]] = None):
    """
    后台任务：从指定媒体库中批量移除特定标签 (支持分级过滤，优先使用自定义分级)。
    """
    try:
        if not library_ids:
            logger.info("  ➜ 未指定媒体库，将扫描所有库...")
            all_libs = emby.get_emby_libraries(processor.emby_url, processor.emby_api_key, processor.emby_user_id)
            if all_libs:
                library_ids = [l['Id'] for l in all_libs if l.get('CollectionType') not in ['boxsets', 'playlists', 'music']]
        logger.info(f"启动批量移除任务 | 目标库: {len(library_ids)}个 | 标签: {tags} | 分级限制: {rating_filters if rating_filters else '无 (全量)'}")
        
        total_libs = len(library_ids)
        filter_msg = f" (分级限制: {','.join(rating_filters)})" if rating_filters else ""

        for lib_idx, lib_id in enumerate(library_ids):
            # 初始状态更新
            task_manager.update_status_from_thread(int((lib_idx/total_libs)*100), f"正在读取第 {lib_idx+1}/{total_libs} 个媒体库...")

            items = emby.get_emby_library_items(
                base_url=processor.emby_url, api_key=processor.emby_api_key,
                library_ids=[lib_id], media_type_filter="Movie,Series,Episode",
                user_id=processor.emby_user_id,
                fields="Id,Name,OfficialRating,CustomRating" 
            )
            if not items: continue

            total_items = len(items)
            processed_count = 0
            skipped_count = 0

            for i, item in enumerate(items):
                if processor.is_stop_requested(): return
                
                item_name = item.get('Name', '未知')

                # ★★★ 修复点：将进度更新移到过滤逻辑之前，并提高频率 ★★★
                if i % 5 == 0:
                    current_progress = int((lib_idx/total_libs)*100 + (i/total_items)*(100/total_libs))
                    task_manager.update_status_from_thread(
                        current_progress, 
                        f"库({lib_idx+1}/{total_libs}) 正在扫描: {item_name}"
                    )

                # ★★★ 分级过滤逻辑 (自定义分级优先) ★★★
                if rating_filters:
                    # 优先取 CustomRating，如果没有则取 OfficialRating
                    item_rating = item.get('CustomRating') or item.get('OfficialRating')
                    
                    if not _is_rating_match(item.get('Name'), item_rating, rating_filters):
                        skipped_count += 1
                        continue 

                
                # 执行移除 
                success = emby.remove_tags_from_item(item.get("Id"), tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if success:
                    processed_count += 1
            
            logger.info(f"  媒体库 {lib_id} 处理完成: 移除 {processed_count} 个, 跳过 {skipped_count} 个。")
        
        task_manager.update_status_from_thread(100, "批量标签移除完成")
    except Exception as e:
        logger.error(f"批量清理任务失败: {e}")
        task_manager.update_status_from_thread(-1, "清理任务异常中止")

# --- 分级同步任务 ---
def task_sync_ratings_to_emby(processor):
    """
    【分级同步任务】
    不再区分模式，每次执行都确保：
    1. CustomRating: 双向互补 (以DB为准)。
    2. OfficialRating: 单向强制 (DB US -> Emby)。
    """
    logger.info(f"--- 开始执行分级同步任务 (全量比对) ---")
    
    # 1. 从数据库获取所有在库项目
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
    
    BATCH_SIZE = 200
    updated_emby_count = 0
    updated_db_count = 0
    
    for i in range(0, total_items, BATCH_SIZE):
        if processor.is_stop_requested(): break
        
        batch = all_items[i : i + BATCH_SIZE]
        
        emby_id_map = {} 
        emby_ids_to_fetch = []
        
        for row in batch:
            try:
                e_ids = row['emby_item_ids_json']
                if e_ids:
                    eid = e_ids[0]
                    emby_id_map[eid] = row
                    emby_ids_to_fetch.append(eid)
            except: continue

        if not emby_ids_to_fetch: continue

        # 批量获取 Emby 现状
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
            
            db_custom = db_row['custom_rating']
            emby_custom = e_item.get('CustomRating')
            
            db_official_json = db_row['official_rating_json'] or {}
            if isinstance(db_official_json, str):
                try: db_official_json = json.loads(db_official_json)
                except: db_official_json = {}
            
            db_us_rating = db_official_json.get('US')
            emby_official = e_item.get('OfficialRating')

            changes_to_emby = {}
            changes_to_db = {}

            # --- 1. CustomRating (双向互补) ---
            if db_custom and not emby_custom:
                changes_to_emby['CustomRating'] = db_custom
            elif emby_custom and not db_custom:
                changes_to_db['custom_rating'] = emby_custom
            elif db_custom and emby_custom and db_custom != emby_custom:
                changes_to_emby['CustomRating'] = db_custom

            # --- 2. OfficialRating (单向强制) ---
            # 只要 DB 有 US 分级，且 Emby 不一致，就覆盖！
            if db_us_rating and db_us_rating != emby_official:
                changes_to_emby['OfficialRating'] = db_us_rating
                
                # 自动解锁 OfficialRating 防止修改失败
                locked = e_item.get('LockedFields', [])
                if 'OfficialRating' in locked:
                    locked.remove('OfficialRating')
                    changes_to_emby['LockedFields'] = locked

            # --- 执行更新 ---
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
                    # logger.trace(f"  ➜ [同步->Emby] {item_name}: {changes_to_emby}")

            if changes_to_db:
                media_db.update_media_metadata_fields(tmdb_id, item_type, changes_to_db)
                updated_db_count += 1
                # logger.trace(f"  ➜ [同步->DB] {item_name}: {changes_to_db}")

        progress = int((i / total_items) * 100)
        task_manager.update_status_from_thread(progress, f"分级同步: 已处理 {i}/{total_items}...")

    logger.info(f"--- 分级同步完成 ---")
    logger.info(f"  ➜ Emby 修正: {updated_emby_count} 条")
    logger.info(f"  ➜ DB 回写: {updated_db_count} 条")
    task_manager.update_status_from_thread(100, f"分级同步完成: Emby修正{updated_emby_count}, DB回写{updated_db_count}")

# --- 扫描监控目录查漏补缺 ---
def task_scan_monitor_folders(processor):
    """
    任务：扫描配置的监控目录，查找数据库中不存在的媒体（漏网之鱼），并触发主动处理。
    优化：
    1. 回溯时间可配置。
    2. 优先检查时间戳，极速过滤旧文件。
    3. 【新增】查库比对文件名，确保只处理真正未入库的文件。
    """
    # 1. 获取配置
    monitor_enabled = processor.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED)
    monitor_paths = processor.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
    monitor_extensions = processor.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)
    lookback_days = processor.config.get(constants.CONFIG_OPTION_MONITOR_SCAN_LOOKBACK_DAYS, constants.DEFAULT_MONITOR_SCAN_LOOKBACK_DAYS)
    
    logger.info(f"--- 开始执行监控目录查漏扫描 (回溯 {lookback_days} 天) ---")

    if not monitor_enabled or not monitor_paths:
        logger.info("  ➜ 实时监控未启用或未配置路径，跳过扫描。")
        return

    valid_exts = set(ext.lower() for ext in monitor_extensions)

    # 2. 获取已知 TMDb ID (白名单)
    # ... (保留原有白名单逻辑，虽然有了新逻辑，这个白名单依然可以用于快速判断是否是完全陌生的剧) ...
    known_tmdb_ids = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT tmdb_id FROM media_metadata WHERE tmdb_id IS NOT NULL")
            for row in cursor.fetchall():
                known_tmdb_ids.add(str(row['tmdb_id']))
        logger.info(f"  ➜ 加载了 {len(known_tmdb_ids)} 个已知 TMDb ID (白名单)。")
    except Exception as e:
        logger.error(f"  🚫 无法读取数据库白名单，任务中止: {e}")
        return
    
    tmdb_regex = r'(?:tmdb|tmdbid)[-_=\s]*(\d+)'
    processed_in_this_run = set()
    
    # ★★★ 新增：本次运行的资产缓存，避免重复查库 ★★★
    # Key: tmdb_id, Value: Set[filenames]
    db_assets_cache = {}

    scan_count = 0
    trigger_count = 0
    skipped_old_count = 0
    skipped_exists_count = 0 # 新增统计
    
    now = time.time()
    cutoff_time = now - (lookback_days * 24 * 3600)

    for root_path in monitor_paths:
        if not os.path.exists(root_path):
            logger.warning(f"  ⚠️ 监控路径不存在: {root_path}")
            continue

        logger.info(f"  ➜ 正在扫描目录: {root_path}")
        
        for dirpath, dirnames, filenames in os.walk(root_path):
            folder_name = os.path.basename(dirpath)
            match_folder = re.search(tmdb_regex, folder_name, re.IGNORECASE)
            
            # 提取当前目录可能的 ID (优先用文件夹ID)
            folder_tmdb_id = match_folder.group(1) if match_folder else None

            for filename in filenames:
                if filename.startswith('.'): continue
                _, ext = os.path.splitext(filename)
                if ext.lower() not in valid_exts: continue
                
                file_path = os.path.join(dirpath, filename)
                
                # ★★★ 第一道防线：时间过滤 (极速) ★★★
                try:
                    stat = os.stat(file_path)
                    file_time = max(stat.st_mtime, stat.st_ctime)
                    
                    if lookback_days > 0 and file_time < cutoff_time:
                        skipped_old_count += 1
                        continue 
                except OSError:
                    continue 

                scan_count += 1
                scan_count += 1
                if scan_count % 300 == 0:
                    # ★★★ 核心优化：主动休眠 0.05 秒 ★★★
                    # 这会释放 GIL 锁，让 Web 服务器有时间把进度推送到前端，防止界面假死
                    time.sleep(0.05)
                    
                    # 计算一个动态进度值 (50% ~ 90%)，让进度条看起来在动
                    # 假设大概有 30000 个文件，动态计算一下视觉效果更好
                    # 如果不知道总数，就固定 50 也可以，或者让它在 50-80 之间循环
                    dynamic_progress = 50 + int((scan_count % 10000) / 10000 * 30)
                    
                    task_manager.update_status_from_thread(
                        dynamic_progress, 
                        f"扫描中... (已扫 {scan_count}, 跳过旧文件 {skipped_old_count}, 跳过已存 {skipped_exists_count})"
                    )

                # --- ID 提取 ---
                target_id = folder_tmdb_id
                
                # 如果文件夹没ID，查爷爷文件夹
                if not target_id:
                    grandparent_path = os.path.dirname(dirpath)
                    grandparent_name = os.path.basename(grandparent_path)
                    match_grand = re.search(tmdb_regex, grandparent_name, re.IGNORECASE)
                    if match_grand:
                        target_id = match_grand.group(1)
                
                # 如果还没ID，查文件名
                if not target_id:
                    match_file = re.search(tmdb_regex, filename, re.IGNORECASE)
                    if match_file:
                        target_id = match_file.group(1)
                
                # --- 判定逻辑 ---
                if target_id:
                    # 1. 如果 ID 已经在本次运行处理过，跳过
                    if target_id in processed_in_this_run:
                        continue

                    # 2. ★★★ 第二道防线：查库比对文件名 ★★★
                    # 如果 ID 在缓存里没有，先去数据库查一次并缓存
                    if target_id not in db_assets_cache:
                        # 调用 media_db 新增的函数
                        db_assets_cache[target_id] = media_db.get_known_filenames_by_tmdb_id(target_id)
                    
                    # 检查当前文件名是否已在数据库记录中
                    if filename in db_assets_cache[target_id]:
                        skipped_exists_count += 1
                        # logger.trace(f"  ➜ [跳过] 文件已入库: {filename}")
                        continue

                    # 3. 确实是新文件，触发处理
                    logger.info(f"  🔍 发现未入库文件: {filename} (ID: {target_id})，触发检查...")
                    try:
                        processor.process_file_actively(file_path)
                        
                        # 标记该 ID 本次已触发
                        processed_in_this_run.add(target_id)
                        
                        # 乐观更新缓存：假设处理成功，将该文件名加入缓存，防止同一次扫描重复触发
                        if target_id in db_assets_cache:
                            db_assets_cache[target_id].add(filename)

                        trigger_count += 1
                        time.sleep(1) 
                    except Exception as e:
                        logger.error(f"  🚫 处理文件失败: {e}")

    logger.info(f"--- 监控目录扫描完成。扫描: {scan_count}, 跳过旧文件: {skipped_old_count}, 跳过已入库: {skipped_exists_count}, 触发处理: {trigger_count} ---")
    task_manager.update_status_from_thread(100, f"扫描完成，处理了 {trigger_count} 个新项目")

# --- 从数据库恢复本地覆盖缓存 ---
def task_restore_local_cache_from_db(processor):
    """
    【灾难恢复】从数据库读取元数据，重新生成本地 override JSON 文件。
    用于误删 cache 目录或迁移环境后的数据恢复。
    """
    task_name = "恢复覆盖缓存"
    logger.info(f"--- 开始执行 '{task_name}' ---")
    
    try:
        # 1. 获取所有顶层项目 (Movie, Series)
        task_manager.update_status_from_thread(5, "正在读取数据库...")
        
        items_to_restore = []
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM media_metadata 
                WHERE item_type IN ('Movie', 'Series') 
                  AND tmdb_id IS NOT NULL 
                  AND tmdb_id != '0'
            """)
            items_to_restore = [dict(row) for row in cursor.fetchall()]

        total = len(items_to_restore)
        if total == 0:
            task_manager.update_status_from_thread(100, "数据库中没有可恢复的项目。")
            return

        logger.info(f"  ➜ 发现 {total} 个项目需要恢复缓存。")
        
        success_count = 0
        
        for i, item in enumerate(items_to_restore):
            if processor.is_stop_requested():
                logger.warning("  🚫 任务被中止。")
                break

            # 每处理50个文件，暂停 0.01 秒，防止 IO/CPU 100% 卡死系统
            if i % 50 == 0:
                time.sleep(0.01)

            tmdb_id = item['tmdb_id']
            item_type = item['item_type']
            title = item.get('title', tmdb_id)
            
            # 更新进度
            if i % 5 == 0:
                progress = int((i / total) * 100)
                task_manager.update_status_from_thread(progress, f"正在恢复 ({i+1}/{total}): {title}")

            try:
                # --- A. 准备演员数据 ---
                db_actors = []
                if item.get('actors_json'):
                    try:
                        raw_actors = item['actors_json']
                        actors_link = json.loads(raw_actors) if isinstance(raw_actors, str) else raw_actors
                        
                        actor_tmdb_ids = [a['tmdb_id'] for a in actors_link if 'tmdb_id' in a]
                        
                        if actor_tmdb_ids:
                            with connection.get_db_connection() as conn:
                                cursor = conn.cursor()
                                # 批量查询演员详情
                                placeholders = ','.join(['%s'] * len(actor_tmdb_ids))
                                sql = f"""
                                    SELECT am.*, pim.primary_name as name
                                    FROM actor_metadata am
                                    LEFT JOIN person_identity_map pim ON am.tmdb_id = pim.tmdb_person_id
                                    WHERE am.tmdb_id IN ({placeholders})
                                """
                                cursor.execute(sql, tuple(actor_tmdb_ids))
                                actor_rows = cursor.fetchall()
                                actor_map = {r['tmdb_id']: dict(r) for r in actor_rows}
                                
                                # 组装回有序列表
                                for link in actors_link:
                                    tid = link.get('tmdb_id')
                                    if tid in actor_map:
                                        full_actor = actor_map[tid].copy()
                                        full_actor['character'] = link.get('character')
                                        full_actor['order'] = link.get('order')
                                        db_actors.append(full_actor)
                                        
                                db_actors.sort(key=lambda x: x.get('order', 999))
                    except Exception as e_actor:
                        logger.warning(f"  ⚠️ 解析演员数据失败 ({title}): {e_actor}")

                # --- B. 重建主 Payload ---
                payload = reconstruct_metadata_from_db(item, db_actors)

                # --- C. 如果是剧集，注入分季/分集数据 ---
                if item_type == "Series":
                    with connection.get_db_connection() as conn:
                        cursor = conn.cursor()
                        
                        # 查分季
                        cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Season'", (tmdb_id,))
                        seasons_rows = cursor.fetchall()
                        seasons_data = []
                        for s_row in seasons_rows:
                            s_data = {
                                "id": int(s_row['tmdb_id']) if s_row['tmdb_id'].isdigit() else 0,
                                "name": s_row['title'],
                                "overview": s_row['overview'],
                                "season_number": s_row['season_number'],
                                "air_date": str(s_row['release_date']) if s_row['release_date'] else None,
                                "poster_path": s_row['poster_path']
                            }
                            seasons_data.append(s_data)
                        
                        # 查分集
                        cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode'", (tmdb_id,))
                        episodes_rows = cursor.fetchall()
                        episodes_data = {} 
                        
                        for e_row in episodes_rows:
                            s_num = e_row['season_number']
                            e_num = e_row['episode_number']
                            key = f"S{s_num}E{e_num}"
                            
                            e_data = {
                                "id": int(e_row['tmdb_id']) if e_row['tmdb_id'].isdigit() else 0,
                                "name": e_row['title'],
                                "overview": e_row['overview'],
                                "season_number": s_num,
                                "episode_number": e_num,
                                "air_date": str(e_row['release_date']) if e_row['release_date'] else None,
                                "vote_average": e_row['rating'],
                            }
                            episodes_data[key] = e_data

                        if seasons_data: payload['seasons_details'] = seasons_data
                        if episodes_data: payload['episodes_details'] = episodes_data

                # --- D. 写入文件 ---
                # 构造上下文对象 (Id='pending' 避免触发 Emby API 请求)
                fake_item_details = {
                    "Id": "pending", 
                    "Name": title, 
                    "Type": item_type, 
                    "ProviderIds": {"Tmdb": tmdb_id}
                }
                
                processor.sync_item_metadata(
                    item_details=fake_item_details,
                    tmdb_id=tmdb_id,
                    metadata_override=payload
                )
                success_count += 1
                
            except Exception as e_item:
                logger.error(f"  🚫 恢复项目 '{title}' 失败: {e_item}")

        final_msg = f"恢复完成！成功生成 {success_count}/{total} 个项目的本地缓存文件。"
        logger.info(f"  ✅ {final_msg}")
        task_manager.update_status_from_thread(100, final_msg)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_scan_incomplete_assets(processor):
    """
    【新任务 - 优化版】全库扫描资产数据不完整的项目。
    直接利用 SQL Join 获取所需的 Emby ID，无需二次查询。
    """
    logger.info("--- 开始执行全库资产完整性扫描 ---")
    
    try:
        # 1. 从数据库获取“嫌疑人” (已包含父级信息)
        bad_items = media_db.get_items_with_potentially_bad_assets()
        total = len(bad_items)
        
        if total == 0:
            logger.info("  ✅ 未发现媒体信息异常的项目。")
            task_manager.update_status_from_thread(100, "媒体信息扫描完成：无异常")
            return

        logger.info(f"  ⚠️ 发现 {total} 个项目的媒体信息可能不完整，正在复核并标记...")
        
        marked_count = 0
        
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            for i, item in enumerate(bad_items):
                # 解析资产
                raw_assets = item['asset_details_json']
                assets = json.loads(raw_assets) if isinstance(raw_assets, str) else (raw_assets if isinstance(raw_assets, list) else [])
                
                # 复核 (虽然 SQL 已经筛过，但 Python 再确认一次更稳妥)
                is_valid = False
                fail_reason = "未知原因"
                
                for asset in assets:
                    w = asset.get('width')
                    h = asset.get('height')
                    c = asset.get('video_codec')
                    valid, reason = utils.check_stream_validity(w, h, c)
                    if valid:
                        is_valid = True
                        break
                    fail_reason = reason
                
                if not is_valid:
                    # =========================================================
                    # ★★★ 核心优化：直接从 item 字典中提取 Emby ID ★★★
                    # =========================================================
                    target_log_id = None
                    target_name = item['title']
                    target_type = item['item_type']
                    final_reason = fail_reason
                    
                    if item['item_type'] == 'Movie':
                        # 电影：取自己的 Emby ID
                        e_ids = item.get('emby_item_ids_json')
                        if e_ids and len(e_ids) > 0:
                            target_log_id = e_ids[0]
                            
                    elif item['item_type'] == 'Episode':
                        # 分集：取父剧集的 Emby ID (SQL 已经 Join 好了)
                        p_ids = item.get('parent_emby_ids_json')
                        if p_ids and len(p_ids) > 0:
                            target_log_id = p_ids[0]
                        
                        # 优先使用父剧集标题
                        if item.get('parent_title'):
                            target_name = item['parent_title']
                            
                        target_type = 'Series'
                        final_reason = f"[S{item['season_number']}E{item['episode_number']}] {fail_reason}"

                    # 兜底：如果实在没有 Emby ID (极少见)，回退到 TMDb ID
                    if not target_log_id:
                        target_log_id = item['parent_series_tmdb_id'] if item['item_type'] == 'Episode' else item['tmdb_id']

                    # 写入日志
                    processor.log_db_manager.save_to_failed_log(
                        cursor, target_log_id, target_name, 
                        f"全库扫描发现异常: {final_reason}", 
                        target_type, score=0.0
                    )
                    
                    processor.log_db_manager.save_to_processed_log(cursor, target_log_id, target_name, score=0.0)
                    
                    marked_count += 1
                    
                    if i % 10 == 0:
                        logger.info(f"  ➜ [标记] {target_name} (ID: {target_log_id}): {final_reason}")

            conn.commit()

        msg = f"扫描完成。共发现 {total} 个异常项，已将 {marked_count} 个(归并后)加入待复核列表。"
        logger.info(f"  ✅ {msg}")
        task_manager.update_status_from_thread(100, msg)

    except Exception as e:
        logger.error(f"执行资产扫描任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")