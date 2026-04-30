# tasks/media.py
# 核心媒体处理、元数据、资产同步等

import time
import json
import copy
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
from .helpers import parse_full_asset_details, reconstruct_metadata_from_db, translate_tmdb_metadata_recursively
from extensions import UPDATING_METADATA

logger = logging.getLogger(__name__)

# --- 辅助函数：严格校验 TMDb ID ---
def is_valid_tmdb_id(tmdb_id) -> bool:
    """
    严格校验 TMDb ID 是否有效。
    拦截 None, '', '0', 'None', 'null' 以及非纯数字。
    """
    if not tmdb_id:
        return False
    id_str = str(tmdb_id).strip()
    if id_str in ['0', 'None', 'null', '']:
        return False
    if not id_str.isdigit():
        return False
    if int(id_str) <= 0:
        return False
    return True

# --- 中文化角色名 ---
def task_role_translation(processor, force_full_update: bool = False):
    """
    根据传入的 force_full_update 参数，决定是执行标准扫描还是深度更新。
    """
    actor = processor.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE)

    if not actor:
        logger.info("  ➜ AI翻译功能未启用，跳过任务。")
        return

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

# --- 全能元数据同步器 ---
def task_sync_all_metadata(processor, item_id: str, item_name: str):
    """
    【任务：全能元数据同步器】
    当收到 metadata.update Webhook 时，此任务会：
    1. 从 Emby 获取最新数据。
    2. 将更新同步到 media_metadata 数据库缓存。
    (注：NFO 模式下，物理文件的修改由 Emby 自身负责)
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
            fields="ProviderIds,Type,Name,OriginalTitle,Overview,Tagline,CommunityRating,OfficialRating,Genres,Studios,Tags,PremiereDate"
        )
        if not item_details:
            logger.error(f"  ➜ {log_prefix} 失败：无法获取项目 {item_id} 的最新详情。")
            return

        # 步骤 2: 调用施工队，更新数据库缓存
        processor.sync_single_item_to_metadata_cache(item_id, item_name=item_name)

        logger.trace(f"  ➜ 任务成功：{log_prefix}")
    except Exception as e:
        logger.error(f"  ➜ 任务失败：{log_prefix} 时发生错误: {e}", exc_info=True)
        raise

# --- 核心辅助函数：轮询等待媒体信息修复完成 ---
def _wait_for_items_recovery(processor, item_ids: list, max_retries=6, interval=10) -> bool:
    """
    轮询检查指定的一组 Emby ID 是否都已具备有效的媒体信息文件 (-mediainfo.json)。
    用于等待神医插件处理网盘文件。
    """
    if not item_ids:
        return True

    logger.info(f"  ➜ 开始轮询监控 {len(item_ids)} 个项目的修复进度 (最大等待 {max_retries*interval}秒)...")
    
    # 使用集合来管理还需要等待的ID，修复一个移除一个
    pending_ids = set(item_ids)
    
    for i in range(max_retries):
        if processor.is_stop_requested(): return False
        
        # 复制一份当前待处理列表进行遍历
        current_check_list = list(pending_ids)
        
        for eid in current_check_list:
            try:
                # 获取详情 (请求 Path 字段)
                item_details = emby.get_emby_item_details(
                    eid, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                    fields="Path,MediaSources"
                )
                
                is_healed = False
                if item_details:
                    # ★★★ 核心修改：直接查找物理文件 ★★★
                    file_path = item_details.get("Path")
                    media_sources = item_details.get("MediaSources", [])
                    if not file_path and media_sources:
                        file_path = media_sources[0].get("Path")
                    
                    if file_path:
                        mediainfo_path = os.path.splitext(file_path)[0] + "-mediainfo.json"
                        if os.path.exists(mediainfo_path):
                            is_healed = True
                            
                    # ★★★ 补充检查：如果没有检测到物理文件，检查 MediaSources 是否有分辨率数据 ★★★
                    if not is_healed and media_sources:
                        for source in media_sources:
                            for stream in source.get("MediaStreams", []):
                                if stream.get("Type") == "Video" and (stream.get("Width") or stream.get("Height")):
                                    is_healed = True
                                    break
                            if is_healed:
                                break
                
                if is_healed:
                    logger.debug(f"    ✔ 项目 {eid} 已检测到媒体信息文件，移除监控队列。")
                    pending_ids.remove(eid)
                    
            except Exception:
                pass # 网络错误暂时忽略，下次重试
        
        if not pending_ids:
            logger.info(f"  ➜ 所有目标项目媒体信息均已提取完成 (耗时 {i*interval}秒)！")
            return True
            
        if i % 2 == 0: # 每20秒打印一次进度
            logger.info(f"  ➜ 等待神医提取媒体信息中... 剩余 {len(pending_ids)}/{len(item_ids)} 个项目 (轮询 {i+1}/{max_retries})")
            
        time.sleep(interval)

    logger.warning(f"  ➜ 等待超时！仍有 {len(pending_ids)} 个项目未获取到完整信息，将强制继续处理。")
    return False

# --- 重新处理单个项目 ---
def task_reprocess_single_item(processor, item_id: str, item_name_for_ui: str, failure_reason: Optional[str] = None):
    """
    重新处理单个项目的后台任务。
    逻辑重构：
    1. 如果是手动强制重扫（非缺失媒体信息），则清空所有媒体信息缓存（物理文件+数据库）。
    2. 统一调用本地提取器 (task_restore_mediainfo) 补齐/重新提取媒体信息。
    3. 执行标准的全量元数据刮削流程。
    """
    logger.trace(f"  ➜ 后台任务开始执行 ({item_name_for_ui})")
    
    try:
        task_manager.update_status_from_thread(0, f"正在处理: {item_name_for_ui}")
        
        # 判断是否是因为“缺失媒体信息”触发的
        is_missing_info = failure_reason and "缺失媒体信息" in failure_reason
        
        # =================================================================
        # 分支 1：手动强制重处理 -> 丢弃所有缓存，强制后续走 ffprobe 在线提取
        # =================================================================
        if not is_missing_info:
            logger.info(f"  ➜ 检测到强制重新处理请求，准备清除旧的媒体信息缓存...")
            task_manager.update_status_from_thread(5, "正在清除旧的媒体信息缓存...")
            
            paths_to_clean = []
            sha1s_to_clean = set()
            
            # 1. 获取 Emby 详情，收集所有相关路径 (包含多版本和分集)
            item_basic = emby.get_emby_item_details(
                item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                fields="Type,Path,MediaSources"
            )
            
            if item_basic:
                item_type = item_basic.get('Type')
                
                def _collect_paths(item_data):
                    collected = []
                    if item_data.get("Path"): collected.append(item_data.get("Path"))
                    for source in item_data.get("MediaSources", []):
                        if source.get("Path"): collected.append(source.get("Path"))
                    return collected

                if item_type in ['Movie', 'Episode']:
                    paths_to_clean.extend(_collect_paths(item_basic))
                elif item_type == 'Series':
                    episodes = emby.get_all_library_versions(
                        base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
                        media_type_filter="Episode", parent_id=item_id,
                        fields="Path,MediaSources"
                    )
                    if episodes:
                        for ep in episodes:
                            paths_to_clean.extend(_collect_paths(ep))
                            
            paths_to_clean = list(set(paths_to_clean))
            
            # 2. 执行清理
            deleted_files = 0
            for path in paths_to_clean:
                if not path: continue
                
                # A. 删物理文件 (处理 HTTP 挂载路径转换)
                local_path = path
                if path.startswith('http'):
                    pc, _ = processor._extract_115_fingerprints(path)
                    if pc:
                        db_local_path = processor._get_local_path_by_pickcode(pc)
                        if db_local_path:
                            local_root = processor.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT, "")
                            local_path = os.path.join(local_root, db_local_path.lstrip('/\\'))
                
                if local_path and not local_path.startswith('http'):
                    json_path = os.path.splitext(local_path)[0] + "-mediainfo.json"
                    if os.path.exists(json_path):
                        try:
                            os.remove(json_path)
                            deleted_files += 1
                            logger.debug(f"    - 已删除本地指纹文件: {json_path}")
                        except Exception as e:
                            logger.warning(f"    - 删除本地指纹文件失败 {json_path}: {e}")
                            
                # B. 收集 SHA1
                pc, sha1 = processor._extract_115_fingerprints(path)
                if not sha1 and pc:
                    sha1 = processor._get_sha1_by_pickcode(pc)
                if sha1:
                    sha1s_to_clean.add(sha1)
                    
            # C. 清理与重新格式化媒体信息
            if sha1s_to_clean:
                try:
                    with connection.get_db_connection() as conn:
                        cursor = conn.cursor()
                        # 1. 只清空 mediainfo_json，保留 raw_ffprobe_json
                        cursor.execute(
                            "UPDATE p115_mediainfo_cache SET mediainfo_json = NULL WHERE sha1 = ANY(%s)", 
                            (list(sha1s_to_clean),)
                        )
                        conn.commit()
                        
                    # 2. 尝试用 raw_ffprobe_json 重新格式化
                    from handler.p115_service import P115CacheManager, SmartOrganizer
                    for sha1 in sha1s_to_clean:
                        raw_ffprobe = P115CacheManager.get_raw_ffprobe_cache(sha1)
                        if raw_ffprobe:
                            logger.info(f"  ➜ 发现原始 ffprobe 数据，正在重新格式化: {sha1[:8]}")
                            dummy_node = {"fn": "unknown.mkv"} 
                            # ▼▼▼ 修改这里：正确调用解析器的方法 ▼▼▼
                            analyzer = SmartOrganizer.__new__(SmartOrganizer)
                            new_emby_json = analyzer._build_emby_mediainfo_from_ffprobe(raw_ffprobe, dummy_node, sha1)
                            if new_emby_json:
                                # 重新保存时，把 raw_ffprobe 也带上，防止丢失
                                P115CacheManager.save_mediainfo_cache(sha1, new_emby_json, raw_ffprobe)
                                logger.info(f"  ➜ 重新格式化成功，已写回缓存。")
                except Exception as e:
                    logger.warning(f"  ➜ 处理数据库媒体信息缓存失败: {e}")
                    
            # D. 调用神医接口清除 Emby 内部缓存
            try:
                emby.clear_item_media_info(item_id, processor.emby_url, processor.emby_api_key)
            except Exception:
                pass
                
            logger.info(f"  ➜ 媒体信息清除完毕 (删除了 {deleted_files} 个物理文件)。")
            
        # =================================================================
        # 分支 2：缺失媒体信息重扫 -> 保留缓存，仅做查漏补缺
        # =================================================================
        else:
            logger.info(f"  ➜ 失败原因为缺失媒体信息，保留现有缓存，准备查漏补缺。")

        # =================================================================
        # 统一执行：调用本地提取器 (恢复/在线提取)
        # =================================================================
        logger.info(f"  ➜ 进行媒体信息恢复/在线提取...")
        task_manager.update_status_from_thread(10, "正在提取底层媒体信息...")
        task_restore_mediainfo(processor)

        # =================================================================
        # 统一执行：标准处理流程 (验收成果 & 刮削元数据)
        # =================================================================
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
def task_reprocess_all_review_items(processor, reason_filter: Optional[str] = None):
    """
    后台任务：遍历待复核项并逐一重新处理。支持按原因筛选。
    """
    logger.trace(f"--- 开始执行“重新处理待复核项”任务 [筛选原因: {reason_filter or '无'}] ---")
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            if reason_filter:
                cursor.execute("SELECT item_id, item_name, reason FROM failed_log WHERE reason LIKE %s", (f"{reason_filter}%",))
            else:
                cursor.execute("SELECT item_id, item_name, reason FROM failed_log")
            
            all_items = [{'id': row['item_id'], 'name': row['item_name'], 'reason': row['reason']} for row in cursor.fetchall()]
        
        total = len(all_items)
        if total == 0:
            logger.info("没有符合条件的项目，任务结束。")
            task_manager.update_status_from_thread(100, "列表为空。")
            return

        logger.info(f"共找到 {total} 个项目需要重新处理。")

        for i, item in enumerate(all_items):
            if processor.is_stop_requested():
                logger.info("  ➜ 任务被中止。")
                break
            
            item_id = item['id']
            item_name = item['name'] or f"ItemID: {item_id}"
            failure_reason = item['reason']

            task_manager.update_status_from_thread(int((i/total)*100), f"正在重新处理 {i+1}/{total}: {item_name}")
            
            task_reprocess_single_item(processor, item_id, item_name, failure_reason=failure_reason)
            time.sleep(2) 

    except Exception as e:
        logger.error(f"重新处理待复核项时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")

# --- 提取标签 ---
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

# --- 翻译前裁剪 TMDb 数据 ---
def prune_tmdb_payload_for_translation(item_type: str, tmdb_data):
    """
    翻译前裁剪无用字段，减少 tokens。
    目标：
    1. Movie / Series 不翻译标题
    2. Movie / Series 不翻译任何人物相关信息（演员 / 导演 / 主创 / 客串 / 剧组）
    """

    if not tmdb_data or item_type not in ("Movie", "Series"):
        return tmdb_data

    data = copy.deepcopy(tmdb_data)

    PEOPLE_KEYS = {
        "cast",
        "crew",
        "guest_stars",
        "created_by",
    }

    CONTAINER_KEYS = {
        "credits",
        "casts",
        "aggregate_credits",
    }

    def _strip_people(obj):
        if isinstance(obj, dict):
            # 先删直接人物字段
            for key in list(obj.keys()):
                if key in PEOPLE_KEYS:
                    obj.pop(key, None)

            # 再删整块人物容器
            for key in list(obj.keys()):
                if key in CONTAINER_KEYS:
                    obj.pop(key, None)

            # 递归处理剩余字段
            for v in obj.values():
                _strip_people(v)

        elif isinstance(obj, list):
            for item in obj:
                _strip_people(item)

    # 1) 掐标题
    if item_type == "Movie":
        data.pop("title", None)
        data.pop("original_title", None)
    elif item_type == "Series":
        # 聚合结构下真正的剧集详情通常在 series_details
        if isinstance(data.get("series_details"), dict):
            data["series_details"].pop("name", None)
            data["series_details"].pop("original_name", None)
        # 兜底
        data.pop("name", None)
        data.pop("original_name", None)

    # 2) 全递归掐掉所有人物相关字段
    _strip_people(data)

    return data

# --- 重量级的元数据缓存填充任务 ---
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

            # B. 获取在线状态 (★ 修复：无论是否全量更新，都必须获取在线状态，否则无法检测离线)
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
            raw_tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
            
            # ★ 严格校验 TMDb ID
            tmdb_id = raw_tmdb_id if is_valid_tmdb_id(raw_tmdb_id) else None

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
        missing_emby_ids = known_online_emby_ids - current_scan_emby_ids
        
        del known_online_emby_ids # 释放内存
        del current_scan_emby_ids
        gc.collect()

        if missing_emby_ids:
            logger.info(f"  ➜ 检测到 {len(missing_emby_ids)} 个 Emby ID 已消失，正在执行外科手术式清理...")
            missing_ids_list = list(missing_emby_ids)
            missing_ids_set = set(missing_emby_ids) # 用于快速查找
            
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 1. 查出包含这些消失 ID 的所有记录
                cursor.execute("""
                    SELECT tmdb_id, item_type, parent_series_tmdb_id, 
                           emby_item_ids_json, asset_details_json, file_sha1_json, file_pickcode_json
                    FROM media_metadata 
                    WHERE in_library = TRUE 
                      AND EXISTS (
                          SELECT 1 
                          FROM jsonb_array_elements_text(emby_item_ids_json) as eid 
                          WHERE eid = ANY(%s)
                      )
                """, (missing_ids_list,))
                
                rows = cursor.fetchall()
                
                # 分类收集处理结果
                partial_update_records = [] # 还有其他版本存活的记录
                dead_movies_and_series = [] # 彻底死绝的顶层项目
                dead_seasons_and_episodes = [] # 彻底死绝的子集项目
                affected_parent_ids = set() # 需要重刷的父剧集
                
                # 2. 在内存中进行精准的数组元素剔除
                for row in rows:
                    r_tmdb = row['tmdb_id']
                    r_type = row['item_type']
                    r_parent = row['parent_series_tmdb_id']
                    
                    # 安全解析 JSON 数组
                    def _safe_parse(val):
                        if isinstance(val, list): return val
                        if isinstance(val, str):
                            try: return json.loads(val)
                            except: return []
                        return []

                    emby_ids = _safe_parse(row['emby_item_ids_json'])
                    assets = _safe_parse(row['asset_details_json'])
                    sha1s = _safe_parse(row['file_sha1_json'])
                    pcs = _safe_parse(row['file_pickcode_json'])
                    
                    # 倒序遍历，方便安全地 pop 元素
                    for i in range(len(emby_ids) - 1, -1, -1):
                        if emby_ids[i] in missing_ids_set:
                            # 发现消失的 ID，从四个数组中同步剔除
                            emby_ids.pop(i)
                            if i < len(assets): assets.pop(i)
                            if i < len(sha1s): sha1s.pop(i)
                            if i < len(pcs): pcs.pop(i)
                    
                    # 判断生死
                    if len(emby_ids) > 0:
                        # 还有其他版本存活，加入部分更新列表
                        partial_update_records.append((
                            json.dumps(emby_ids, ensure_ascii=False),
                            json.dumps(assets, ensure_ascii=False) if assets else None,
                            json.dumps(sha1s, ensure_ascii=False),
                            json.dumps(pcs, ensure_ascii=False),
                            r_tmdb, r_type
                        ))
                    else:
                        # 死绝了，分类加入死亡名单
                        if r_type in ['Movie', 'Series']:
                            dead_movies_and_series.append(r_tmdb)
                        elif r_type in ['Season', 'Episode']:
                            dead_seasons_and_episodes.append(r_tmdb)
                            if r_parent:
                                affected_parent_ids.add(r_parent)

                # 3. 执行数据库更新
                
                # A. 更新部分存活的记录 (只更新数组，不改变 in_library 状态)
                if partial_update_records:
                    logger.info(f"  ➜ 正在更新 {len(partial_update_records)} 个多版本媒体项 (剔除失效版本)...")
                    from psycopg2.extras import execute_values
                    update_sql = """
                        UPDATE media_metadata AS m
                        SET emby_item_ids_json = v.emby_ids::jsonb,
                            asset_details_json = v.assets::jsonb,
                            file_sha1_json = v.sha1s::jsonb,
                            file_pickcode_json = v.pcs::jsonb,
                            last_updated_at = NOW()
                        FROM (VALUES %s) AS v(emby_ids, assets, sha1s, pcs, tmdb_id, item_type)
                        WHERE m.tmdb_id = v.tmdb_id AND m.item_type = v.item_type
                    """
                    execute_values(cursor, update_sql, partial_update_records)

                # B. 彻底枪毙死绝的顶层项目 (Movie, Series)
                if dead_movies_and_series:
                    logger.info(f"  ➜ 正在标记 {len(dead_movies_and_series)} 个彻底消失的顶层项目为离线...")
                    cursor.execute("""
                        UPDATE media_metadata
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL,
                            file_sha1_json = '[]'::jsonb,
                            file_pickcode_json = '[]'::jsonb
                        WHERE tmdb_id = ANY(%s) AND item_type IN ('Movie', 'Series')
                    """, (dead_movies_and_series,))
                    total_offline_count += cursor.rowcount
                    
                    # 级联枪毙：顶层剧集死了，它下面的所有季和集必须陪葬！
                    cursor.execute("""
                        UPDATE media_metadata
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL,
                            file_sha1_json = '[]'::jsonb,
                            file_pickcode_json = '[]'::jsonb
                        WHERE parent_series_tmdb_id = ANY(%s) AND item_type IN ('Season', 'Episode')
                    """, (dead_movies_and_series,))
                    total_offline_count += cursor.rowcount

                # C. 彻底枪毙死绝的子集 (Season, Episode)
                if dead_seasons_and_episodes:
                    logger.info(f"  ➜ 正在标记 {len(dead_seasons_and_episodes)} 个彻底消失的子集(季/集)为离线...")
                    cursor.execute("""
                        UPDATE media_metadata
                        SET in_library = FALSE, 
                            emby_item_ids_json = '[]'::jsonb, 
                            asset_details_json = NULL,
                            file_sha1_json = '[]'::jsonb,
                            file_pickcode_json = '[]'::jsonb
                        WHERE tmdb_id = ANY(%s) AND item_type IN ('Season', 'Episode')
                    """, (dead_seasons_and_episodes,))
                    total_offline_count += cursor.rowcount
                    
                # D. 善后：如果子集死了，但父剧集还活着，让父剧集刷新一下状态
                if affected_parent_ids:
                    # 过滤掉已经死透的父剧集
                    valid_parent_ids = [pid for pid in affected_parent_ids if pid not in dead_movies_and_series]
                    if valid_parent_ids:
                        logger.info(f"  ➜ 将 {len(valid_parent_ids)} 个受影响的存活父剧集加入刷新队列...")
                        for pid in valid_parent_ids:
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

            # 在写入数据库之前，对获取到的 TMDb 数据进行翻译 (大一统引擎)
            if processor.ai_translator:
                for item_group in batch_item_groups:
                    if not item_group:
                        continue

                    item = item_group[0]
                    t_id = str(item.get("ProviderIds", {}).get("Tmdb"))
                    i_type = item.get("Type")

                    data_to_translate = tmdb_details_map.get(t_id)
                    if data_to_translate:
                        pruned_data = prune_tmdb_payload_for_translation(i_type, data_to_translate)

                        translate_tmdb_metadata_recursively(
                            item_type=i_type,
                            tmdb_data=pruned_data,
                            ai_translator=processor.ai_translator,
                            item_name='' if i_type in ('Movie', 'Series') else item.get('Name', ''),
                            tmdb_api_key=processor.tmdb_api_key,
                            config=processor.config
                        )

            metadata_batch = []
            series_ids_processed_in_batch = set()

            for item_group in batch_item_groups:
                if not item_group: continue
                item = item_group[0]
                
                # ★ 严格校验，防止 str(None) 变成 "None"
                raw_tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not is_valid_tmdb_id(raw_tmdb_id):
                    logger.warning(f"  ➜ [批量同步拦截] 发现无效的 TMDb ID: '{raw_tmdb_id}'，跳过该项目: {item.get('Name')}")
                    continue
                    
                tmdb_id_str = str(raw_tmdb_id)
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
                tmdb_last_date = None
                if tmdb_details:
                    if item_type == 'Movie': 
                        tmdb_date = tmdb_details.get('release_date') or None  # 强制过滤空字符串
                    elif item_type == 'Series': 
                        tmdb_date = tmdb_details.get('first_air_date') or None # 强制过滤空字符串
                        tmdb_last_date = tmdb_details.get('last_air_date') or None # 强制过滤空字符串
                
                final_release_date = emby_date or tmdb_date
                # 提取全量分级数据
                raw_ratings_map = _extract_and_map_tmdb_ratings(tmdb_details, item_type)
                if not raw_ratings_map and item.get('OfficialRating'):
                    raw_ratings_map['US'] = item.get('OfficialRating')
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
                # 1. 处理制作公司 & 2. 处理电视网 
                fmt_companies = []
                fmt_networks = []
                
                if tmdb_details:
                    raw_companies = tmdb_details.get('production_companies') or []
                    fmt_companies = [{'id': c.get('id'), 'name': c.get('name')} for c in raw_companies if c.get('name')]
                    
                    raw_networks = tmdb_details.get('networks') or []
                    fmt_networks = [{'id': n.get('id'), 'name': n.get('name')} for n in raw_networks if n.get('name')]
                top_record = {
                    "tmdb_id": tmdb_id_str,
                    "item_type": item_type,
                    "release_year": item.get('ProductionYear'),
                    "imdb_id": item.get("ProviderIds", {}).get("Imdb") or (tmdb_details.get("imdb_id") if tmdb_details else None),
                    "original_language": tmdb_details.get('original_language') if tmdb_details else None,
                    "watchlist_tmdb_status": tmdb_details.get('status') if tmdb_details else None,
                    "in_library": True,
                    "subscription_status": "NONE",
                    "emby_item_ids_json": json.dumps(list(set(v.get('Id') for v in item_group if v.get('Id') and v.get('Type') == item_type)), ensure_ascii=False),
                    "asset_details_json": json.dumps(asset_details_list, ensure_ascii=False),
                    "rating": item.get('CommunityRating'),
                    "date_added": item.get('DateCreated') or None,
                    "release_date": final_release_date or None,
                    "last_air_date": tmdb_last_date or None,
                    "genres_json": json.dumps(final_genres_list, ensure_ascii=False),
                    "production_companies_json": json.dumps(fmt_companies, ensure_ascii=False),
                    "networks_json": json.dumps(fmt_networks, ensure_ascii=False),
                    "tags_json": json.dumps(extract_tag_names(item), ensure_ascii=False),
                    "official_rating_json": rating_json_str,
                    "custom_rating": item.get('CustomRating'),
                    "runtime_minutes": emby_runtime if (item_type == 'Movie' and emby_runtime) else tmdb_details.get('runtime') if (item_type == 'Movie' and tmdb_details) else None,
                    "tagline": tmdb_details.get('tagline') if tmdb_details else None
                }
                if tmdb_details:
                    top_record['poster_path'] = tmdb_details.get('poster_path')
                    top_record['backdrop_path'] = tmdb_details.get('backdrop_path') 
                    top_record['homepage'] = tmdb_details.get('homepage')
                    top_record['overview'] = tmdb_details.get('overview')
                    if tmdb_details.get('vote_average') is not None:
                        top_record['rating'] = tmdb_details.get('vote_average')
                    # 采集总集数
                    if item_type == 'Series':
                        top_record['total_episodes'] = tmdb_details.get('number_of_episodes', 0)
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
                    top_record['backdrop_path'] = None 
                    top_record['homepage'] = None
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
                                    "imdb_id": matched_emby_seasons[0].get("ProviderIds", {}).get("Imdb") if matched_emby_seasons else None, 
                                    "parent_series_tmdb_id": tmdb_id_str,
                                    "season_number": s_num,
                                    "title": s_info.get('name'),
                                    "overview": s_info.get('overview'),
                                    "poster_path": season_poster,
                                    "rating": s_info.get('vote_average'),
                                    "total_episodes": s_info.get('episode_count', 0),
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
                                "imdb_id": s.get("ProviderIds", {}).get("Imdb"), 
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
                        ep_release_date = emby_ep.get('PremiereDate') or None # 强制过滤空字符串
                        if not ep_release_date and tmdb_ep_info:
                            ep_release_date = tmdb_ep_info.get('air_date') or None
                        child_record = {
                            "item_type": "Episode",
                            "imdb_id": emby_ep.get("ProviderIds", {}).get("Imdb"), 
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
                            child_record['backdrop_path'] = tmdb_ep_info.get('still_path')
                            child_record['runtime_minutes'] = emby_ep_runtime if emby_ep_runtime else tmdb_ep_info.get('runtime')
                            if tmdb_ep_info.get('vote_average') is not None:
                                child_record['rating'] = tmdb_ep_info.get('vote_average')
                        else:
                            child_record['tmdb_id'] = f"{tmdb_id_str}-S{s_n}E{e_n}"
                            child_record['title'] = versions[0].get('Name')
                            child_record['overview'] = versions[0].get('Overview')
                            child_record['runtime_minutes'] = emby_ep_runtime
                            child_record['poster_path'] = None   # ★★★ 兜底
                            child_record['backdrop_path'] = None # ★★★ 兜底
                        
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
                                
                                # 针对 total_episodes 字段，检查锁定状态
                                # 逻辑：如果 total_episodes_locked 为 TRUE，则保持原值；否则使用新值 (EXCLUDED.total_episodes)
                                if col == 'total_episodes':
                                    update_clauses.append(
                                        "total_episodes = CASE WHEN media_metadata.total_episodes_locked IS TRUE THEN media_metadata.total_episodes ELSE EXCLUDED.total_episodes END"
                                    )
                                else:
                                    # 其他字段正常更新
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
                                SET in_library = FALSE, 
                                    emby_item_ids_json = '[]'::jsonb, 
                                    asset_details_json = NULL,
                                    file_sha1_json = '[]'::jsonb,
                                    file_pickcode_json = '[]'::jsonb
                                WHERE parent_series_tmdb_id = ANY(%s)
                                  AND item_type IN ('Season', 'Episode')
                                  AND in_library = TRUE
                                  AND tmdb_id != ALL(%s)
                            """, (list(series_ids_processed_in_batch), active_child_ids_list))
                            total_offline_count += cursor.rowcount
                        else:
                            cursor.execute("""
                                UPDATE media_metadata
                                SET in_library = FALSE, 
                                    emby_item_ids_json = '[]'::jsonb, 
                                    asset_details_json = NULL,
                                    file_sha1_json = '[]'::jsonb,
                                    file_pickcode_json = '[]'::jsonb
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
            logger.info(f"  ➜ [大扫除] 成功物理删除了 {cleaned_zombies} 条已废弃的内部ID记录 (如 xxx-S1E1)。")
            
        final_msg = f"同步完成！新增/更新: {total_updated_count} 个媒体项, 标记离线: {total_offline_count} 个媒体项。"
        logger.info(f"  ➜ {final_msg}")
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
            logger.info("  ➜ 任务被中止。")
            break

        tags = rule.get('tags')
        if not tags: continue
        
        library_ids = rule.get('library_ids', [])
        rating_filters = rule.get('rating_filters', [])
        
        # 直接调用现有的批量打标逻辑
        # 注意：task_bulk_auto_tag 内部会处理进度更新和异常捕获
        task_bulk_auto_tag(processor, library_ids, tags, rating_filters)

    task_manager.update_status_from_thread(100, "自动打标规则执行完毕")

# --- 自动打标 ---
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
                logger.info(f"  ➜ 媒体库 {lib_id} 为空或无法访问。")
                continue

            total_items = len(items)
            logger.info(f"  ➜ 媒体库 {lib_id} 扫描到 {total_items} 个项目，开始过滤...")
            
            processed_count = 0
            skipped_count = 0

            for i, item in enumerate(items):
                if processor.is_stop_requested(): return
                
                item_name = item.get('Name', '未知')
                
                # ★★★ 修复点：将进度更新移到过滤逻辑之前，并提高频率 ★★★
                if i % 50 == 0:
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

            logger.info(f"  ➜ 媒体库 {lib_id} 处理完成: 打标 {processed_count} 个, 跳过 {skipped_count} 个 (不符分级)。")
        
        task_manager.update_status_from_thread(100, "所有选定库批量打标完成")
    except Exception as e:
        logger.error(f"  ➜ 批量打标任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务异常中止")

# --- 批量移除标签 ---
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

# --- 扫描监控目录查漏补缺 ---
def task_scan_monitor_folders(processor):
    """
    任务：扫描配置的监控目录，查找数据库中不存在的媒体（漏网之鱼），并触发主动处理。
    优化：
    1. 回溯时间可配置。
    2. 优先检查时间戳，极速过滤旧文件。
    3. 查库比对文件名，确保只处理真正未入库的文件。
    4. 【修正】命中排除路径时，直接跳过处理（不刷新），防止因无法入库导致的死循环刷新。
    """
    # 1. 获取配置
    monitor_enabled = processor.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED)
    monitor_paths = processor.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
    monitor_extensions = processor.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)
    lookback_days = processor.config.get(constants.CONFIG_OPTION_MONITOR_SCAN_LOOKBACK_DAYS, constants.DEFAULT_MONITOR_SCAN_LOOKBACK_DAYS)
    
    # 获取排除路径配置并规范化
    monitor_exclude_dirs = processor.config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, constants.DEFAULT_MONITOR_EXCLUDE_DIRS)
    exclude_paths = [os.path.normpath(d).lower() for d in (monitor_exclude_dirs or [])]

    logger.info(f"  ➜ 开始执行监控目录查漏扫描 (回溯 {lookback_days} 天)")

    if not monitor_enabled or not monitor_paths:
        logger.info("  ➜ 实时监控未启用或未配置路径，跳过扫描。")
        return

    valid_exts = set(ext.lower() for ext in monitor_extensions)

    # 2. 获取已知 TMDb ID (白名单)
    known_tmdb_ids = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT tmdb_id FROM media_metadata WHERE tmdb_id IS NOT NULL")
            for row in cursor.fetchall():
                known_tmdb_ids.add(str(row['tmdb_id']))
        logger.info(f"  ➜ 加载了 {len(known_tmdb_ids)} 个已知 TMDb ID (白名单)。")
    except Exception as e:
        logger.error(f"  ➜ 无法读取数据库白名单，任务中止: {e}")
        return
    
    tmdb_regex = r'(?:tmdb|tmdbid)[-_=\s]*(\d+)'
    processed_in_this_run = set()
    
    # Key: tmdb_id, Value: Set[filenames]
    db_assets_cache = {}

    scan_count = 0
    trigger_count = 0
    skipped_old_count = 0
    skipped_exists_count = 0 
    
    now = time.time()
    cutoff_time = now - (lookback_days * 24 * 3600)

    for root_path in monitor_paths:
        if not os.path.exists(root_path):
            logger.warning(f"  ➜ 监控路径不存在: {root_path}")
            continue

        logger.info(f"  ➜ 正在扫描目录: {root_path}")
        
        for dirpath, dirnames, filenames in os.walk(root_path):
            # ★★★ 修正：排除路径检查逻辑 ★★★
            norm_dirpath = os.path.normpath(dirpath).lower()
            hit_exclude = False
            
            for exc_path in exclude_paths:
                if norm_dirpath.startswith(exc_path):
                    hit_exclude = True
                    break
            
            if hit_exclude:
                # ★★★ 关键修改：直接静默跳过，不执行刷新 ★★★
                # 原因：排除的文件永远不会入库。如果在这里刷新，每次定时任务运行（只要在回溯期内）
                # 都会重复刷新这些文件，导致死循环和日志刷屏。
                # 排除目录的刷新应完全依赖“实时监控”或 Emby 自身的计划任务。
                
                # logger.debug(f"  ➜ [扫描跳过] 命中排除目录: {os.path.basename(dirpath)}")
                dirnames[:] = [] # 停止向下递归
                continue 

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
                if scan_count % 300 == 0:
                    time.sleep(0.05)
                    dynamic_progress = 50 + int((scan_count % 10000) / 10000 * 30)
                    task_manager.update_status_from_thread(
                        dynamic_progress, 
                        f"扫描中... (已扫 {scan_count}, 跳过旧文件 {skipped_old_count}, 跳过已存 {skipped_exists_count})"
                    )

                # --- ID 提取 ---
                target_id = folder_tmdb_id
                
                if not target_id:
                    grandparent_path = os.path.dirname(dirpath)
                    grandparent_name = os.path.basename(grandparent_path)
                    match_grand = re.search(tmdb_regex, grandparent_name, re.IGNORECASE)
                    if match_grand:
                        target_id = match_grand.group(1)
                
                if not target_id:
                    match_file = re.search(tmdb_regex, filename, re.IGNORECASE)
                    if match_file:
                        target_id = match_file.group(1)
                
                # --- 判定逻辑 ---
                if target_id:
                    if target_id in processed_in_this_run:
                        continue

                    if target_id not in db_assets_cache:
                        db_assets_cache[target_id] = media_db.get_known_filenames_by_tmdb_id(target_id)
                    
                    name_without_ext, _ = os.path.splitext(filename)
                    
                    if name_without_ext in db_assets_cache[target_id]:
                        skipped_exists_count += 1
                        continue

                    logger.info(f"  ➜ 发现未入库文件: {filename} (ID: {target_id})，触发检查...")
                    try:
                        processor.process_file_actively(file_path)
                        processed_in_this_run.add(target_id)
                        if target_id in db_assets_cache:
                            # ★ 存入缓存时也存无扩展名的版本
                            db_assets_cache[target_id].add(name_without_ext)
                        trigger_count += 1
                        time.sleep(1) 
                    except Exception as e:
                        logger.error(f"  ➜ 处理文件失败: {e}")

    logger.info(f"  ➜ 监控目录扫描完成。扫描: {scan_count}, 触发处理: {trigger_count}")
    task_manager.update_status_from_thread(100, f"扫描完成，处理了 {trigger_count} 个新项目")

# --- 终极媒体信息备份任务 ---
def task_backup_mediainfo(processor):
    """
    【终极媒体信息备份任务】(精准过滤版 + 待复核标记)
    1. 通过高级 SQL 精准获取真正缺失 SHA1 或 缺失指纹缓存 的媒体项。
    2. 检查 SHA1，缺失的通过解析 链接/STRM 提取 PC 码 -> 换取 FID -> 115 API 补齐并写入 media_metadata。
    3. 检查本地 -mediainfo.json，如果缺失则将该项目标记为“待复核”。
    4. 如果存在，则读取并写入指纹库 p115_mediainfo_cache。
    """
    logger.info("--- 开始执行媒体信息备份任务 ---")
    
    task_manager.update_status_from_thread(0, "正在扫描需要备份的媒体项，请稍候...")
    time.sleep(1)  # 增加短暂停顿，确保前端能渲染出初始状态
    
    items = media_db.get_missing_mediainfo_assets()
    total = len(items)
    
    if total == 0:
        logger.info("  ➜ 所有媒体信息均已备份，无需处理。")
        task_manager.update_status_from_thread(100, "所有媒体信息均已备份，无需处理")
        time.sleep(1)  # 增加短暂停顿，确保前端能渲染出完成状态
        return

    logger.info(f"  ➜ 共扫描到 {total} 个项目需要补充 SHA1 或备份媒体信息...")
    
    from handler.p115_service import P115Service, P115CacheManager
    client = P115Service.get_client()
    
    sha1_fixed_count = 0
    mediainfo_backed_up_count = 0
    
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            for i, item in enumerate(items):
                if processor.is_stop_requested(): break
                
                if i % 50 == 0: 
                    task_manager.update_status_from_thread(int((i/total)*100), f"正在处理 ({i+1}/{total}): {item['title']}...")
                
                tmdb_id = item['tmdb_id']
                item_type = item['item_type']
                title = item['title']
                
                # ==========================================
                # ★ 智能定位 Emby ID (分集报错需定位到父剧集)
                # ==========================================
                emby_ids = item.get('emby_item_ids_json', [])
                if isinstance(emby_ids, str):
                    try: emby_ids = json.loads(emby_ids)
                    except: emby_ids = []
                target_emby_id = emby_ids[0] if emby_ids else None
                
                target_log_type = item_type
                log_title = title
                
                if item_type == 'Episode':
                    parent_ids = item.get('parent_emby_ids_json')
                    if isinstance(parent_ids, str):
                        try: parent_ids = json.loads(parent_ids)
                        except: parent_ids = []
                    elif not parent_ids:
                        parent_ids = []
                        
                    if parent_ids:
                        target_emby_id = parent_ids[0]
                        target_log_type = 'Series'
                        log_title = item.get('parent_title') or '未知剧集'
                    else:
                        # ★ 核心修复：如果找不到父剧集ID，强制设为 None，防止把分集ID写进待复核列表
                        target_emby_id = None
                
                def _safe_parse_json_list(data):
                    if isinstance(data, list): return data
                    if isinstance(data, str):
                        try:
                            parsed = json.loads(data)
                            return parsed if isinstance(parsed, list) else []
                        except: return []
                    return []

                pcs = _safe_parse_json_list(item.get('file_pickcode_json'))
                sha1s = _safe_parse_json_list(item.get('file_sha1_json'))
                assets = _safe_parse_json_list(item.get('asset_details_json'))
                
                needs_db_update = False
                
                for idx, asset in enumerate(assets):
                    if not isinstance(asset, dict):
                        continue
                        
                    current_path = asset.get('path')
                    if not current_path: continue
                    
                    current_sha1 = sha1s[idx] if idx < len(sha1s) else None
                    current_pc = pcs[idx] if idx < len(pcs) else None
                    
                    # ★★★ 核心修复：直接调用核心处理器的万能双指纹提取器 (完美兼容 STRM 和 挂载模式) ★★★
                    extracted_pc, extracted_sha1 = processor._extract_115_fingerprints(current_path)
                            
                    # 兜底安检：确保提取出来的是合法的 PC 码 (纯字母数字且长度合理)
                    if extracted_pc and not (extracted_pc.isalnum() and 10 < len(extracted_pc) < 25):
                        extracted_pc = None
                            
                    actual_pc = extracted_pc or current_pc

                    # 如果提取到了 PC 码，且数据库里没有，则更新 PC 数组
                    if extracted_pc and current_pc != extracted_pc:
                        while len(pcs) <= idx: pcs.append(None)
                        pcs[idx] = extracted_pc
                        needs_db_update = True
                        
                    # ★★★ 意外惊喜：如果万能提取器(通过挂载路径匹配)顺手把 SHA1 也查出来了，直接用！★★★
                    if extracted_sha1 and not current_sha1:
                        current_sha1 = extracted_sha1
                        while len(sha1s) <= idx: sha1s.append(None)
                        sha1s[idx] = current_sha1
                        needs_db_update = True
                        sha1_fixed_count += 1
                        logger.info(f"  ➜ 成功通过本地路径匹配获取 SHA1: {current_sha1}")
                    
                    # 阶段 1: 补齐缺失的 SHA1 (如果万能提取器没拿到 SHA1，再调 API)
                    if not current_sha1 and actual_pc:
                        logger.info(f"  ➜ [{title}] 缺失 SHA1，正在通过本地计算 FID 并请求 115 API 补齐 (PC: {actual_pc})...")
                        fid = None
                        try:
                            # 优先使用 p115pickcode 库本地计算，无需查库，速度极快
                            from p115pickcode import to_id
                            fid = to_id(actual_pc)
                        except (ImportError, ValueError, TypeError): # ★ 增加异常捕获，防止奇葩字符串搞崩任务
                            try:
                                from p115client.tool.iterdir import to_id
                                fid = to_id(actual_pc)
                            except (ImportError, ValueError, TypeError):
                                # 兜底查库
                                fid = P115CacheManager.get_fid_by_pickcode(actual_pc)
                                
                        if fid and client:
                            try:
                                info_res = client.fs_get_info(fid)
                                if info_res and info_res.get('state'):
                                    fetched_sha1 = info_res['data'].get('sha1')
                                    if fetched_sha1:
                                        current_sha1 = fetched_sha1
                                        while len(sha1s) <= idx:
                                            sha1s.append(None)
                                        sha1s[idx] = current_sha1
                                        needs_db_update = True
                                        sha1_fixed_count += 1
                                        logger.info(f"  ➜ 成功获取 SHA1: {current_sha1}")
                            except Exception as e:
                                logger.warning(f"  ➜ 获取 SHA1 失败: {e}")
                                
                    # 阶段 2: 备份媒体信息到指纹库 & 缺失检查
                    if current_path and not current_path.startswith('http'):
                        local_root = processor.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT, '')
                        mediainfo_path = None
                        
                        # ★★★ 核心修复：解决挂载模式下找不到 JSON 的问题 ★★★
                        # Emby 的 current_path 可能是挂载路径，而 JSON 实际在本地 STRM 目录
                        if current_sha1 or actual_pc:
                            query_val = current_sha1 if current_sha1 else actual_pc
                            query_col = "sha1" if current_sha1 else "pick_code"
                            cursor.execute(f"SELECT local_path FROM p115_filesystem_cache WHERE {query_col} = %s AND local_path IS NOT NULL LIMIT 1", (query_val,))
                            cache_row = cursor.fetchone()
                            if cache_row and cache_row['local_path']:
                                # 拼接出真实的本地 JSON 路径
                                base_local = os.path.join(local_root, str(cache_row['local_path']).lstrip('\\/'))
                                mediainfo_path = os.path.splitext(base_local)[0] + "-mediainfo.json"
                        
                        # 兜底：如果没查到，或者本来就是本地 STRM 路径，直接替换后缀
                        if not mediainfo_path:
                            mediainfo_path = os.path.splitext(current_path)[0] + "-mediainfo.json"
                        
                        if not os.path.exists(mediainfo_path):
                            # ★★★ 缺失 mediainfo.json，标记待复核 (仅限电影和剧集) ★★★
                            if target_emby_id and target_log_type in ['Movie', 'Series']:
                                filename = os.path.basename(mediainfo_path)
                                match = re.search(r'(S\d{1,2}E\d{1,3})', filename, re.IGNORECASE)
                                reason = f"缺失媒体信息: {match.group(1).upper()}" if match else "缺失媒体信息"
                                    
                                processor.log_db_manager.save_to_failed_log(
                                    cursor, target_emby_id, log_title, reason, target_log_type, score=0.0
                                )
                                logger.warning(f"  ➜ [{log_title}] 缺失本地 JSON，已标记为待复核: {reason}")
                        else:
                            # 文件存在，如果有 SHA1 且未缓存，则备份
                            if current_sha1 and not media_db.is_mediainfo_cached(current_sha1):
                                try:
                                    with open(mediainfo_path, 'r', encoding='utf-8') as f:
                                        raw_info = json.load(f)
                                        
                                    if raw_info and isinstance(raw_info, list):
                                        cursor.execute("""
                                            INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json)
                                            VALUES (%s, %s::jsonb)
                                            ON CONFLICT (sha1) DO NOTHING
                                        """, (current_sha1, json.dumps(raw_info, ensure_ascii=False)))
                                        
                                        if cursor.rowcount > 0:
                                            mediainfo_backed_up_count += 1
                                            logger.info(f"  ➜ [{log_title}] 媒体信息已成功备份至数据库。")
                                except Exception as e:
                                    logger.warning(f"  ➜ 读取本地 JSON 失败 {mediainfo_path}: {e}")
                
                if needs_db_update:
                    media_db.update_media_sha1_and_pc_json(tmdb_id, item_type, sha1s, pcs)
                
                if i > 0 and i % 50 == 0:
                    conn.commit()
            
            conn.commit()
            
        msg = f"备份任务完成！补齐 SHA1: {sha1_fixed_count} 个，成功备份媒体信息: {mediainfo_backed_up_count} 个。"
        logger.info(f"  ➜ {msg}")
        task_manager.update_status_from_thread(100, msg)
        
    except Exception as e:
        logger.error(f"执行备份任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务失败")

# --- 终极媒体信息还原任务 ---
def task_restore_mediainfo(processor, force_full_update: bool = False):
    """
    【恢复媒体信息任务】(万能指纹提取 + I/O 节流优化版)
    force_full_update=False: 仅遍历本地查找缺失 -mediainfo.json 的 .strm 文件并补齐。
    force_full_update=True: 全量模式。查库获取所有在库项，逐个清除 Emby 缓存、清空数据库缓存，并重新生成。
    """
    mode_str = "全量强制刷新" if force_full_update else "增量查漏补缺"
    logger.info(f"--- 开始执行媒体信息还原任务 ({mode_str}) ---")
    task_manager.update_status_from_thread(0, f"正在准备数据 ({mode_str})...")
    time.sleep(1)  # 增加短暂停顿，确保前端能渲染出初始状态
    
    local_root = processor.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    if not local_root or not os.path.exists(local_root):
        logger.warning("  ➜ 未配置本地媒体库目录或目录不存在。")
        task_manager.update_status_from_thread(100, "未配置本地媒体库目录或目录不存在")
        time.sleep(1)
        return

    from handler.p115_service import P115Service, P115CacheManager, SmartOrganizer
    client = P115Service.get_client()
        
    # 1. 收集所有需要恢复的 strm 文件路径及对应的 Emby ID
    items_to_restore = []
    
    if force_full_update:
        logger.info("  ➜ [全量模式] 正在从数据库获取所有在库媒体项...")
        rows = media_db.get_all_in_library_physical_paths()
        for row in rows:
            path = row.get('path')
            if not path: continue
            
            # 提取 Emby ID 列表 (处理多版本情况)
            emby_ids = row.get('emby_item_ids_json')
            if isinstance(emby_ids, str):
                try: emby_ids = json.loads(emby_ids)
                except: emby_ids = []
            if not isinstance(emby_ids, list):
                emby_ids = []

            # 转换路径为本地路径 (兼容 HTTP 挂载模式)
            local_path = path
            if path.startswith('http'):
                pc, _ = processor._extract_115_fingerprints(path)
                if pc:
                    db_local_path = processor._get_local_path_by_pickcode(pc)
                    if db_local_path:
                        local_path = os.path.join(local_root, db_local_path.lstrip('/\\'))

            if local_path and not local_path.startswith('http') and local_path.lower().endswith('.strm'):
                items_to_restore.append({'path': local_path, 'emby_ids': emby_ids})
    else:
        logger.info("  ➜ [增量模式] 正在扫描本地媒体库目录查找缺失项...")
        for root, _, files in os.walk(local_root):
            if processor.is_stop_requested(): return
            for file in files:
                if file.lower().endswith('.strm'):
                    strm_path = os.path.join(root, file)
                    json_path = os.path.splitext(strm_path)[0] + "-mediainfo.json"
                    # 增量模式只处理缺失的
                    if not os.path.exists(json_path):
                        items_to_restore.append({'path': strm_path, 'emby_ids': []})
                    
    total = len(items_to_restore)
    if total == 0:
        logger.info("  ➜ 没有需要还原媒体信息的项目。")
        task_manager.update_status_from_thread(100, "没有需要还原媒体信息的项目")
        time.sleep(1)  
        return
        
    logger.info(f"  ➜ 发现 {total} 个媒体项，准备处理...")
    
    restored_count = 0
    failed_count = 0
    cleared_emby_count = 0
    
    # ★★★ 新增：用于收集成功生成的路径，最后统一通知 Emby 扫描 ★★★
    successfully_restored_paths = []

    def _probe_and_cache_mediainfo_online(pc, sha1, filename):
        if not client or not pc or not sha1:
            return None

        try:
            probe_helper = SmartOrganizer.__new__(SmartOrganizer)
            probe_helper.client = client

            file_node = {"pick_code": pc, "pc": pc, "file_name": filename, "fn": filename}

            emby_json, raw_ffprobe = probe_helper._probe_mediainfo_with_ffprobe(
                file_node=file_node,
                sha1=sha1,
                silent_log=False
            ) or (None, None)

            if emby_json:
                P115CacheManager.save_mediainfo_cache(sha1, emby_json, raw_ffprobe)
                return emby_json

        except Exception as e:
            logger.warning(f"  ➜ [媒体信息还原] 在线 ffprobe 提取失败 {filename}: {e}")

        return None
    
    for i, item in enumerate(items_to_restore):
        if processor.is_stop_requested(): break
        
        strm_path = item['path']
        emby_ids = item['emby_ids']
        
        # ★ 优化 1：动态调整进度推送频率
        update_interval = 1 if total <= 50 else (10 if total <= 500 else 50)
        if i % update_interval == 0 or i == total - 1:
            task_manager.update_status_from_thread(int((i/total)*100), f"正在处理 ({i+1}/{total})...")
            time.sleep(0.1) # 强制让出 CPU 时间片，让前端喘口气

        filename = os.path.basename(strm_path)
        strm_content_path = None
        
        try:
            with open(strm_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                strm_content_path = content.replace('\\', '/')
        except Exception as e:
            logger.warning(f"  ➜ 读取 STRM 失败 {strm_path}: {e}")
            failed_count += 1
            continue

        # 从 STRM 内容中提取真实的视频扩展名
        real_ext = ".mkv" 
        if strm_content_path:
            parsed_ext = os.path.splitext(strm_content_path)[1].lower()
            if parsed_ext in ['.mkv', '.mp4', '.ts', '.avi', '.rmvb', '.wmv', '.mov', '.m2ts', '.flv', '.mpg', '.iso']:
                real_ext = parsed_ext
                
        real_filename = filename.replace('.strm', real_ext)
        if real_filename == filename: 
            real_filename = os.path.splitext(filename)[0] + real_ext

        # 2. 调用核心处理器的万能指纹提取器
        pc, sha1 = processor._extract_115_fingerprints(strm_content_path)
        
        if not sha1 and pc:
            sha1 = processor._get_sha1_by_pickcode(pc)

        # =================================================================
        # ★ 全量模式特有逻辑：流水线式清除 Emby 缓存和数据库缓存
        # =================================================================
        if force_full_update:
            # 1. 清除 Emby 内部缓存 (神医接口会自动删除本地 JSON)
            for eid in emby_ids:
                try:
                    emby.clear_item_media_info(eid, processor.emby_url, processor.emby_api_key)
                    cleared_emby_count += 1
                except Exception:
                    pass
            
            # 2. 清除数据库中的 mediainfo_json (保留 raw_ffprobe_json)
            if sha1:
                media_db.clear_mediainfo_json_by_sha1(sha1)

        # 3. 获取或生成媒体信息
        mediainfo = None

        # 如果不是强制全量，先尝试直接从数据库获取格式化好的 mediainfo
        if not force_full_update and sha1:
            mediainfo = media_db.get_mediainfo_by_sha1(sha1)

        # 如果没有获取到 (或者是强制全量被清空了)，则尝试重新生成或在线提取
        if not mediainfo and sha1 and pc:
            # 先看有没有 raw_ffprobe_json 可以用来极速重新格式化
            raw_ffprobe = P115CacheManager.get_raw_ffprobe_cache(sha1)
            if raw_ffprobe:
                try:
                    analyzer = SmartOrganizer.__new__(SmartOrganizer)
                    dummy_node = {"fn": real_filename}
                    mediainfo = analyzer._build_emby_mediainfo_from_ffprobe(raw_ffprobe, dummy_node, sha1)
                    if mediainfo:
                        P115CacheManager.save_mediainfo_cache(sha1, mediainfo, raw_ffprobe)
                except Exception as e:
                    logger.warning(f"  ➜ 重新格式化 raw_ffprobe 失败: {e}")

            # 如果还是没有，只能在线提取了
            if not mediainfo:
                mediainfo = _probe_and_cache_mediainfo_online(pc, sha1, real_filename)
        
        # 4. 写入本地文件
        if mediainfo:
            json_path = os.path.splitext(strm_path)[0] + "-mediainfo.json"
            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(mediainfo, f, ensure_ascii=False)
                restored_count += 1
                successfully_restored_paths.append(strm_path) # ★★★ 收集成功生成的路径 ★★★
            except Exception as e:
                logger.error(f"  ➜ 写入 JSON 失败 {json_path}: {e}")
                failed_count += 1
        else:
            failed_count += 1
            
        # ★ 优化 2：每个文件处理完微小休眠，进行 I/O 节流
        time.sleep(0.005)
            
    # =================================================================
    # ★★★ 终极收尾：统一通知 Emby 对STRM根目录扫描 ★★★
    # =================================================================
    if successfully_restored_paths:
        logger.info(f"  ➜ 成功生成了 {len(successfully_restored_paths)} 个文件，正在通知 Emby 扫描 STRM 根目录...")
        task_manager.update_status_from_thread(99, "正在通知 Emby 扫描新生成的媒体信息...")
        try:
            # 既然 STRM 都在同一个根目录下，直接对这个根目录触发一次递归扫描即可！
            # 既避免了全库扫描打扰其他媒体库，又只发 1 次 API 请求，完美！
            emby.notify_emby_file_changes(local_root, processor.emby_url, processor.emby_api_key)
            logger.info(f"  ➜ 已成功触发 Emby 对目录 '{local_root}' 的递归扫描！")
        except Exception as e:
            logger.error(f"  ➜ 触发 Emby 目录扫描失败: {e}")

    msg = f"任务完成！成功生成: {restored_count} 个，失败: {failed_count} 个。"
    if force_full_update:
        msg += f" (共清除 Emby 缓存 {cleared_emby_count} 次)"
    logger.info(f"  ➜ {msg}")
    task_manager.update_status_from_thread(100, msg)

# --- 终极媒体信息反哺中心服务器任务 ---
def task_contribute_mediainfo_to_center(processor):
    """
    【人人为我，我为人人】专属反哺任务 (批量上传优化版)
    专门扫描本地已有的 SHA1，批量对比中心服务器，
    将中心服务器缺失的媒体信息，通过神医接口提取，并使用批量接口反哺上传。
    """
    logger.info("--- 开始执行媒体信息反哺中心服务器任务 (批量模式) ---")
    
    if not getattr(processor, 'p115_enabled', False) or not processor.p115_center:
        logger.warning("  ➜ P115Center 未启用或未配置，无法执行反哺任务。")
        task_manager.update_status_from_thread(100, "P115Center 未启用")
        return

    task_manager.update_status_from_thread(0, "正在收集本地媒体资产数据...")
    
    # 调用 media_db 获取数据
    raw_items = media_db.get_local_mediainfo_assets_with_sha1()
    
    items_to_check = []
    for row in raw_items:
        if row.get('emby_id') and row.get('sha1_val'):
            items_to_check.append({
                'title': row['title'],
                'emby_id': row['emby_id'],
                'sha1': row['sha1_val'].upper() # 统一转大写防坑
            })

    total_items = len(items_to_check)
    if total_items == 0:
        task_manager.update_status_from_thread(100, "没有找到可反哺的媒体资产")
        return
        
    logger.info(f"  ➜ 共收集到 {total_items} 个包含 SHA1 的媒体资产，准备与中心服务器比对...")
    
    # 分批查询中心服务器 (每次 500 个，极速过滤)
    BATCH_SIZE = 500
    missing_in_center = []
    
    for i in range(0, total_items, BATCH_SIZE):
        if processor.is_stop_requested(): break
        batch = items_to_check[i:i+BATCH_SIZE]
        sha1_list = [item['sha1'] for item in batch]
        
        task_manager.update_status_from_thread(
            int((i/total_items)*30), 
            f"正在比对中心服务器 ({i}/{total_items})..."
        )
        
        try:
            resp = processor.p115_center.download_emby_mediainfo_data(sha1_list)
            for item in batch:
                if not resp.get(item['sha1']):
                    missing_in_center.append(item)
        except Exception as e:
            logger.warning(f"  ➜ 批量查询中心服务器失败: {e}")
            time.sleep(2)
            
    total_missing = len(missing_in_center)
    logger.info(f"  ➜ 比对完成！发现中心服务器缺失 {total_missing} 个项目的媒体信息。")
    
    if total_missing == 0:
        task_manager.update_status_from_thread(100, "中心服务器数据已是最新，无需反哺")
        return
        
    # ==========================================
    # ★ 批量提取与上传逻辑
    # ==========================================
    UPLOAD_BATCH_SIZE = 100  # 每凑够 50 个执行一次批量上传
    payload_batch = []      # 用于存放 [(sha1, data), ...]
    success_count = 0
    
    for i, item in enumerate(missing_in_center):
        if processor.is_stop_requested(): break
        
        title = item['title']
        emby_id = item['emby_id']
        sha1 = item['sha1']
        
        progress = 30 + int((i/total_missing)*70)
        task_manager.update_status_from_thread(progress, f"正在提取 ({i+1}/{total_missing}): {title}")
        
        try:
            # 1. 逐个调神医提取 (必须逐个，因为要查本地 Emby)
            extracted_data = emby.sync_item_media_info(
                item_id=emby_id,
                media_data=None,
                base_url=processor.emby_url,
                api_key=processor.emby_api_key
            )
            
            if extracted_data:
                # 加入批量上传队列
                payload_batch.append((sha1, extracted_data))
                logger.debug(f"  ➜ [加入队列] {title} (当前队列: {len(payload_batch)}/{UPLOAD_BATCH_SIZE})")
            else:
                logger.debug(f"  ➜ [提取失败] {title} 无法获取媒体信息")
                
        except Exception as e:
            logger.warning(f"  ➜ 提取 {title} 时发生异常: {e}")
            
        # 稍微限速，保护本地 Emby
        time.sleep(0.5) 
        
        # 2. 达到批量阈值，或已经是最后一个项目时，执行批量上传
        is_last_item = (i == total_missing - 1)
        if len(payload_batch) >= UPLOAD_BATCH_SIZE or (is_last_item and payload_batch):
            logger.info(f"  ➜ [批量反哺] 正在将 {len(payload_batch)} 条媒体信息打包上传至中心服务器...")
            try:
                # 调用中心服务器的批量上传接口
                processor.p115_center.upload_emby_mediainfo_data_bulk(payload_batch)
                
                success_count += len(payload_batch)
                logger.info(f"  ➜ [批量反哺] 成功上传 {len(payload_batch)} 条数据！")
                
            except Exception as e:
                logger.error(f"  ➜ [批量反哺] 批量上传失败: {e}")
            finally:
                # 清空队列，准备下一批
                payload_batch = []
        
    msg = f"反哺任务完成！成功为中心服务器贡献了 {success_count} 条媒体信息。"
    logger.info(f"  🎉 {msg}")
    task_manager.update_status_from_thread(100, msg)

# --- 从数据库恢复物理 NFO 和图片 ---
def task_restore_nfo_and_images(processor):
    """
    【灾难恢复】从数据库读取元数据，重新生成物理目录下的 NFO 文件并补齐图片。
    用于 NFO 丢失、图片丢失或洗版后的数据恢复。
    """
    task_name = "恢复NFO与图片"
    logger.trace(f"--- 开始执行 '{task_name}' ---")
    
    try:
        task_manager.update_status_from_thread(5, "正在读取数据库...")
        
        items_to_restore = []
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 只恢复当前在库的项目
            cursor.execute("""
                SELECT * FROM media_metadata 
                WHERE item_type IN ('Movie', 'Series') 
                  AND tmdb_id IS NOT NULL 
                  AND tmdb_id NOT IN ('0', 'None', 'null', '')
                  AND in_library = TRUE
            """)
            items_to_restore = [dict(row) for row in cursor.fetchall()]

        total = len(items_to_restore)
        if total == 0:
            task_manager.update_status_from_thread(100, "数据库中没有可恢复的在线项目。")
            return

        logger.info(f"  ➜ 发现 {total} 个项目需要恢复 NFO 和图片。")
        success_count = 0
        
        for i, item in enumerate(items_to_restore):
            if processor.is_stop_requested():
                logger.warning("  ➜ 任务被中止。")
                break

            if i % 10 == 0: time.sleep(0.1)

            tmdb_id = item['tmdb_id']
            item_type = item['item_type']
            title = item.get('title', tmdb_id)
            
            if i % 5 == 0:
                progress = int((i / total) * 100)
                task_manager.update_status_from_thread(progress, f"正在恢复 ({i+1}/{total}): {title}")

            try:
                # ★★★ 1. 获取真实的物理路径 (直接从数据库提取，免 API 请求) ★★★
                asset_details_str = item.get('asset_details_json')
                assets = []
                if asset_details_str:
                    assets = json.loads(asset_details_str) if isinstance(asset_details_str, str) else asset_details_str
                
                if not assets or not isinstance(assets, list) or not assets[0].get('path'):
                    logger.warning(f"  ➜ 无法从数据库获取项目 '{title}' 的物理路径，跳过。")
                    continue
                
                # 构造一个伪装的 item_details 供后续生成函数使用
                item_details = {
                    "Path": assets[0].get('path'),
                    "Type": item_type
                }

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
                                    placeholders = ','.join(['%s'] * len(actor_tmdb_ids))
                                    sql = f"""
                                        SELECT *, primary_name AS name, tmdb_person_id AS tmdb_id
                                        FROM person_metadata
                                        WHERE tmdb_person_id IN ({placeholders})
                                    """
                                    cursor.execute(sql, tuple(actor_tmdb_ids))
                                    actor_rows = cursor.fetchall()
                                actor_map = {r['tmdb_id']: dict(r) for r in actor_rows}
                                for link in actors_link:
                                    tid = link.get('tmdb_id')
                                    if tid in actor_map:
                                        full_actor = actor_map[tid].copy()
                                        full_actor['character'] = link.get('character')
                                        full_actor['order'] = link.get('order')
                                        db_actors.append(full_actor)
                                db_actors.sort(key=lambda x: x.get('order', 999))
                    except Exception as e_actor:
                        logger.warning(f"  ➜ 解析演员数据失败 ({title}): {e_actor}")

                # --- B. 重建主 Payload ---
                payload = reconstruct_metadata_from_db(item, db_actors)

                # --- C. 如果是剧集，注入分季/分集数据 ---
                if item_type == "Series":
                    with connection.get_db_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Season'", (tmdb_id,))
                        seasons_data = []
                        for s_row in cursor.fetchall():
                            if not str(s_row['tmdb_id']).isdigit(): continue
                            seasons_data.append({
                                "id": int(s_row['tmdb_id']), "name": s_row['title'], "overview": s_row['overview'],
                                "season_number": s_row['season_number'], "air_date": str(s_row['release_date']) if s_row['release_date'] else None,
                                "poster_path": s_row['poster_path']
                            })
                        
                        cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode'", (tmdb_id,))
                        episodes_data = {} 
                        for e_row in cursor.fetchall():
                            if not str(e_row['tmdb_id']).isdigit(): continue
                            s_num, e_num = e_row['season_number'], e_row['episode_number']
                            episodes_data[f"S{s_num}E{e_num}"] = {
                                "id": int(e_row['tmdb_id']), "name": e_row['title'], "overview": e_row['overview'],
                                "season_number": s_num, "episode_number": e_num,
                                "air_date": str(e_row['release_date']) if e_row['release_date'] else None,
                                "vote_average": e_row['rating'], "still_path": e_row['poster_path']
                            }
                        if seasons_data: payload['seasons_details'] = seasons_data
                        if episodes_data: payload['episodes_details'] = episodes_data

                # --- D. 写入 NFO ---
                logger.info(f"  ➜ 正在为 '{title}' 生成物理 NFO 文件...")
                processor.sync_item_metadata(
                    item_details=item_details, # 传入真实的 item_details (包含 Path)
                    tmdb_id=tmdb_id,
                    final_cast_override=db_actors,
                    metadata_override=payload
                )
                
                # --- E. 下载图片 ---
                logger.info(f"  ➜ 正在为 '{title}' 补齐缺失图片...")
                processor.download_images_from_tmdb(
                    tmdb_id=tmdb_id,
                    item_type=item_type,
                    aggregated_tmdb_data=payload,
                    item_details=item_details
                )
                
                success_count += 1
                
            except Exception as e_item:
                logger.error(f"  ➜ 恢复项目 '{title}' 失败: {e_item}", exc_info=True)

        final_msg = f"恢复完成！成功为 {success_count}/{total} 个项目生成了 NFO 和图片。"
        logger.info(f"  ➜ {final_msg}")
        task_manager.update_status_from_thread(100, final_msg)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")