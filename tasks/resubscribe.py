# tasks/resubscribe.py
# 媒体洗版专属任务模块

import os
import re 
import time
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed 
from collections import defaultdict

# 导入需要的底层模块
import task_manager
import handler.emby as emby
import handler.moviepilot as moviepilot
import config_manager 
import constants  
from database import resubscribe_db, settings_db, media_db

# 从 helpers 导入的辅助函数和常量
from .helpers import (
    analyze_media_asset, 
    _get_resolution_tier, 
    _get_detected_languages_from_streams, 
    _get_standardized_effect, 
    _extract_quality_tag_from_filename,
    AUDIO_SUBTITLE_KEYWORD_MAP
)

logger = logging.getLogger(__name__)

# ======================================================================
# 核心任务：刷新洗版状态
# ======================================================================

def task_update_resubscribe_cache(processor): # <--- 移除 force_full_update 参数
    """
    【V6 - 最终统一扫描版】
    废除快速/深度模式，每次都执行全量、高效的数据库中心化扫描。
    """
    task_name = "刷新媒体洗版状态" # <--- 简化任务名
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        # --- 步骤 1 & 2: 加载规则和Emby索引 (保持不变) ---
        task_manager.update_status_from_thread(0, "正在加载规则并确定扫描范围...")
        all_enabled_rules = [rule for rule in resubscribe_db.get_all_resubscribe_rules() if rule.get('enabled')]
        
        library_to_rule_map = {}
        all_target_lib_ids = set()
        for rule in reversed(all_enabled_rules):
            if target_libs := rule.get('target_library_ids'):
                all_target_lib_ids.update(target_libs)
                for lib_id in target_libs:
                    library_to_rule_map[lib_id] = rule
        
        if not all_target_lib_ids:
            task_manager.update_status_from_thread(100, "任务跳过：没有规则指定任何媒体库")
            return

        task_manager.update_status_from_thread(10, f"正在从 {len(all_target_lib_ids)} 个目标库中建立媒体索引...")
        emby_index = emby.get_all_library_versions(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series,Episode", library_ids=list(all_target_lib_ids),
            fields="Id,Type,ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,_SourceLibraryId,Name"
        ) or []

        # --- 步骤 3: 加载数据库元数据 (保持不变) ---
        tmdb_ids_in_scope = {str(item['ProviderIds']['Tmdb']) for item in emby_index if item.get('ProviderIds', {}).get('Tmdb')}
        if not tmdb_ids_in_scope:
            task_manager.update_status_from_thread(100, "任务完成：目标媒体库为空。")
            return

        logger.info(f"  ➜ 正在从本地数据库批量获取 {len(tmdb_ids_in_scope)} 个媒体项的详细元数据...")
        metadata_map = media_db.get_media_details_by_tmdb_ids(list(tmdb_ids_in_scope))
        
        series_tmdb_ids = {meta['tmdb_id'] for meta in metadata_map.values() if meta.get('item_type') == 'Series'}
        all_episodes_from_db = media_db.get_episodes_for_series(list(series_tmdb_ids))
        episodes_map = defaultdict(list)
        for ep in all_episodes_from_db:
            episodes_map[ep['parent_series_tmdb_id']].append(ep)

        # ★★★ 步骤 4: 清理Emby中已删除的旧索引 ★★★
        logger.info("  ➜ 正在比对并清理陈旧的洗版索引...")
        indexed_keys = resubscribe_db.get_all_resubscribe_index_keys()
        
        current_emby_keys = set()
        for item in emby_index:
            tmdb_id = item.get('ProviderIds', {}).get('Tmdb')
            if not tmdb_id: continue
            
            if item.get('Type') == 'Movie':
                current_emby_keys.add(str(tmdb_id))
            elif item.get('Type') == 'Episode' and item.get('ParentIndexNumber') is not None:
                current_emby_keys.add(f"{tmdb_id}-S{item['ParentIndexNumber']}")
        
        deleted_keys = indexed_keys - current_emby_keys
        if deleted_keys:
            resubscribe_db.delete_resubscribe_index_by_keys(list(deleted_keys))

        # ★★★ 步骤 5: 全量处理所有项目 ★★★
        items_to_process_index = emby_index
        total = len(items_to_process_index)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：无需处理任何项目。")
            return

        logger.info(f"  ➜ 将对 {total} 个媒体索引项按规则检查洗版状态...")
        index_update_batch = []
        processed_count = 0
        
        # +++ 添加一个计数器用于调试 +++
        debug_skip_counter = defaultdict(int)

        # 将索引项按电影和剧集分组
        movies_to_process = [item for item in items_to_process_index if item.get('Type') == 'Movie']
        series_episodes_map = defaultdict(list)
        series_metadata_map = {} # 用于存储剧集本身的元数据，避免重复查找

        for item in items_to_process_index:
            # 我们只关心分集，因为它们代表了实际的文件
            if item.get('Type') == 'Episode' and item.get('SeriesId'):
                series_id = item.get('SeriesId')
                series_episodes_map[series_id].append(item)
                
                # 顺便存储剧集本身的索引信息（只需要一次）
                if series_id not in series_metadata_map:
                    # 从原始索引中找到这个剧集的顶层信息
                    series_index_item = next((s for s in emby_index if s.get('Id') == series_id and s.get('Type') == 'Series'), None)
                    if series_index_item:
                        series_metadata_map[series_id] = series_index_item

        # --- 处理电影 ---
        for movie_index in movies_to_process:
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80)
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {movie_index.get('Name')}")

            # +++ 添加详细的电影调试日志 +++
            movie_name_for_log = movie_index.get('Name', '未知电影')
            source_lib_id = movie_index.get('_SourceLibraryId')
            tmdb_id = str(movie_index.get('ProviderIds', {}).get('Tmdb'))
            
            rule = library_to_rule_map.get(source_lib_id)
            if not rule:
                debug_skip_counter['movie_no_rule'] += 1
                continue

            metadata = metadata_map.get(tmdb_id)
            if not metadata:
                debug_skip_counter['movie_no_metadata'] += 1
                continue
                
            if not metadata.get('asset_details_json'):
                debug_skip_counter['movie_no_asset_details'] += 1
                continue
            # +++ 调试日志结束 +++

            tmdb_id = movie_index.get('ProviderIds', {}).get('Tmdb')
            metadata = metadata_map.get(tmdb_id)
            if not metadata or not metadata.get('asset_details_json'): continue
            
            # 假设我们只分析第一个版本
            asset = metadata['asset_details_json'][0]
            rule = library_to_rule_map.get(movie_index.get('_SourceLibraryId'))
            if not rule: continue

            needs, reason = _item_needs_resubscribe(asset, rule, metadata)
            status = 'needed' if needs else 'ok'
            
            index_update_batch.append({
                "tmdb_id": tmdb_id,
                "item_type": "Movie",
                "season_number": -1,
                "status": status,
                "reason": reason,
                "matched_rule_id": rule.get('id')
            })

        # --- 处理剧集 ---
        for series_id, series_index in series_metadata_map.items():
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80)
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {series_index.get('Name')}")

            # +++ 添加详细的剧集调试日志 +++
            series_name_for_log = series_index.get('Name', '未知剧集')
            source_lib_id = series_index.get('_SourceLibraryId')
            tmdb_id = str(series_index.get('ProviderIds', {}).get('Tmdb'))

            rule = library_to_rule_map.get(source_lib_id)
            if not rule:
                debug_skip_counter['series_no_rule'] += 1
                continue

            series_metadata = metadata_map.get(tmdb_id)
            if not series_metadata:
                debug_skip_counter['series_no_metadata'] += 1
                continue

            episodes_for_series = episodes_map.get(tmdb_id)
            if not episodes_for_series:
                debug_skip_counter['series_no_episodes_in_map'] += 1
                continue
            # +++ 调试日志结束 +++

            tmdb_id = series_index.get('ProviderIds', {}).get('Tmdb')
            series_metadata = metadata_map.get(tmdb_id)
            episodes_for_series = episodes_map.get(tmdb_id)
            if not series_metadata or not episodes_for_series: continue

            rule = library_to_rule_map.get(series_index.get('_SourceLibraryId'))
            if not rule: continue

            episodes_by_season = defaultdict(list)
            for ep in episodes_for_series:
                episodes_by_season[ep.get('season_number')].append(ep)

            for season_num, episodes_in_season in episodes_by_season.items():
                if season_num is None or not episodes_in_season: continue
                
                # 选取第一集作为代表
                representative_episode = episodes_in_season[0]
                if not representative_episode.get('asset_details_json'): continue
                
                asset = representative_episode['asset_details_json'][0]
                needs, reason = _item_needs_resubscribe(asset, rule, series_metadata)
                status = 'needed' if needs else 'ok'

                season_item_id = f"{series_id}-S{season_num}"
                season_emby_id = next((item.get('Id') for item in emby_index if item.get('Type') == 'Season' and item.get('ParentId') == series_id and item.get('IndexNumber') == season_num), None)

                index_update_batch.append({
                    "tmdb_id": tmdb_id,
                    "item_type": "Season",
                    "season_number": season_num,
                    "status": status,
                    "reason": reason,
                    "matched_rule_id": rule.get('id')
                })

        if index_update_batch:
            resubscribe_db.upsert_resubscribe_index_batch(index_update_batch)

        # +++ 添加最终的调试统计信息输出 +++
        if debug_skip_counter:
            logger.warning("--- 洗版扫描跳过项统计 ---")
            for reason, count in debug_skip_counter.items():
                logger.warning(f"  ➜ 原因: '{reason}', 跳过数量: {count}")
        logger.warning("--------------------------")
        # +++ 调试统计结束 +++
            
        final_message = "媒体洗版状态刷新完成！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ======================================================================
# 核心任务：执行洗版订阅
# ======================================================================

def task_resubscribe_library(processor):
    """【V2 - 独立重构版】一键洗版所有状态为 'needed' 的项目。"""
    _execute_resubscribe(processor, "一键媒体洗版", "needed")

def task_resubscribe_batch(processor, item_ids: List[str]):
    """【V2 - 独立重构版】精准洗版指定的项目。"""
    _execute_resubscribe(processor, "批量媒体洗版", item_ids)

# ======================================================================
# 核心任务：批量删除
# ======================================================================

def task_delete_batch(processor, item_ids: List[str]):
    """【V2 - 独立重构版】精准删除指定的项目。"""
    task_name = "批量删除媒体"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_delete = resubscribe_db.get_resubscribe_cache_by_ids(item_ids)
    total = len(items_to_delete)
    if total == 0:
        task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有可删除的项。")
        return

    deleted_count = 0
    for i, item in enumerate(items_to_delete):
        if processor.is_stop_requested(): break
        
        item_id = item.get('item_id')
        item_name = item.get('item_name')
        task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在删除: {item_name}")
        
        id_to_delete = item.get('emby_item_id') or item_id
        
        if emby.delete_item(id_to_delete, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
            resubscribe_db.delete_resubscribe_cache_item(item_id)
            deleted_count += 1
        
        time.sleep(0.5)

    final_message = f"批量删除任务完成！成功删除了 {deleted_count} 个媒体项。"
    task_manager.update_status_from_thread(100, final_message)

# ======================================================================
# 内部辅助函数
# ======================================================================

def _process_single_item_for_cache(processor, item_base_info: dict, library_to_rule_map: dict) -> Optional[List[dict]]:
    """在线程中处理单个媒体项（电影或剧集）的分析逻辑。"""
    item_id = item_base_info.get('Id')
    item_name = item_base_info.get('Name')
    source_lib_id = item_base_info.get('_SourceLibraryId')

    try:
        applicable_rule = library_to_rule_map.get(source_lib_id)
        if not applicable_rule:
            return [{"item_id": item_id, "status": 'ok', "reason": "无匹配规则"}]
        
        item_details = emby.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not item_details: return None
        
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        media_metadata = media_db.get_media_details_by_tmdb_ids([tmdb_id]) if tmdb_id else None
        item_type = item_details.get('Type')

        if item_type == 'Series':
            seasons = emby.get_series_seasons(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
            if not seasons: return None

            season_cache_results = []
            for season in seasons:
                season_number = season.get('IndexNumber')
                season_id = season.get('Id')
                if season_number is None or season_id is None: continue

                season_item_id = f"{item_id}-S{season_number}"
                
                first_episode_details = None
                first_episode_list = emby.get_season_children(season_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, fields="Id", limit=1)
                if first_episode_list and (first_episode_id := first_episode_list[0].get('Id')):
                    first_episode_details = emby.get_emby_item_details(first_episode_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)

                if not first_episode_details:
                    needs_resubscribe, reason, analysis_data = False, "季内容为空", {}
                else:
                    needs_resubscribe, reason = _item_needs_resubscribe(first_episode_details, applicable_rule, media_metadata)
                    analysis_data = analyze_media_asset(first_episode_details)

                new_status = 'needed' if needs_resubscribe else 'ok'
                
                season_cache_item = {
                    "item_id": season_item_id, "emby_item_id": season_id, "series_id": item_id,
                    "season_number": season_number, "item_name": f"{item_name} - 第 {season_number} 季",
                    "tmdb_id": tmdb_id, "item_type": "Season", "status": new_status, "reason": reason,
                    **analysis_data,
                    "matched_rule_id": applicable_rule.get('id'), "matched_rule_name": applicable_rule.get('name'),
                    "source_library_id": source_lib_id,
                    "path": first_episode_details.get('Path') if first_episode_details else None,
                    "filename": os.path.basename(first_episode_details.get('Path', '')) if first_episode_details else None
                }
                season_cache_results.append(season_cache_item)
            return season_cache_results
        else: # Movie
            needs_resubscribe, reason = _item_needs_resubscribe(item_details, applicable_rule, media_metadata)
            new_status = 'needed' if needs_resubscribe else 'ok'
            analysis_data = analyze_media_asset(item_details)
            
            return [{
                "item_id": item_id, "emby_item_id": item_id, "item_name": item_name, "tmdb_id": tmdb_id,
                "item_type": item_type, "status": new_status, "reason": reason,
                **analysis_data,
                "matched_rule_id": applicable_rule.get('id'), "matched_rule_name": applicable_rule.get('name'),
                "source_library_id": source_lib_id,
                "path": item_details.get('Path'), "filename": os.path.basename(item_details.get('Path', ''))
            }]
    except Exception as e:
        logger.error(f"  ➜ 处理项目 '{item_name}' (ID: {item_id}) 时线程内发生错误: {e}", exc_info=True)
        return None

def _item_needs_resubscribe(asset_details: dict, rule: dict, media_metadata: Optional[dict]) -> tuple[bool, str]:
    """
    【V5 - 终极修正版】
    完全依赖 asset_details 中预先分析好的数据进行判断，不再进行任何二次解析。
    """
    item_name = media_metadata.get('title', '未知项目')
    reasons = []

    # --- 1. 分辨率检查 (直接使用 resolution_display) ---
    try:
        if rule.get("resubscribe_resolution_enabled"):
            # 定义清晰度等级的顺序
            RESOLUTION_ORDER = {
                "2160p": 4,
                "1080p": 3,
                "720p": 2,
                # 其他较低的分辨率都视为等级 1
            }
            
            # 获取当前媒体的清晰度等级
            current_res_str = asset_details.get('resolution_display', 'Unknown')
            current_tier = RESOLUTION_ORDER.get(current_res_str, 1)

            # 获取规则要求的清晰度等级
            required_width = int(rule.get("resubscribe_resolution_threshold", 1920))
            required_tier = 1
            if required_width >= 3800: required_tier = 4
            elif required_width >= 1900: required_tier = 3
            elif required_width >= 1200: required_tier = 2

            if current_tier < required_tier:
                reasons.append("分辨率不达标")
    except (ValueError, TypeError) as e:
        logger.warning(f"  ➜ [分辨率检查] 处理时发生错误: {e}")

    # --- 2. 质量检查 (直接使用 quality_display) ---
    try:
        if rule.get("resubscribe_quality_enabled"):
            required_list = rule.get("resubscribe_quality_include", [])
            if isinstance(required_list, list) and required_list:
                required_list_lower = [str(q).lower() for q in required_list]
                current_quality = asset_details.get('quality_display', '').lower()
                if not any(term in current_quality for term in required_list_lower):
                    reasons.append("质量不符")
    except Exception as e:
        logger.warning(f"  ➜ [质量检查] 处理时发生错误: {e}")

    # --- 3. 特效检查 (直接使用 effect_display) ---
    try:
        if rule.get("resubscribe_effect_enabled"):
            # 规则中存储的是 'dovi', 'hdr', 'hdr10+' 等
            required_effects = set(rule.get("resubscribe_effect_include", []))
            if required_effects:
                # asset_details.effect_display 中是 ['Dolby Vision', 'HDR']
                current_effects_raw = asset_details.get('effect_display', [])
                
                # 将 asset_details 中的显示名，标准化为与规则中一致的关键字
                current_effects_normalized = set()
                for effect in current_effects_raw:
                    eff_lower = effect.lower()
                    if 'dolby' in eff_lower or 'dovi' in eff_lower:
                        current_effects_normalized.add('dovi')
                    elif 'hdr10+' in eff_lower:
                        current_effects_normalized.add('hdr10+')
                    elif 'hdr' in eff_lower:
                        current_effects_normalized.add('hdr')
                
                # 检查当前媒体的特效集合，是否与规则要求的特效集合有任何交集
                # 如果没有任何交集，说明不满足规则
                if not current_effects_normalized.intersection(required_effects):
                    reasons.append("特效不符")
    except Exception as e:
        logger.warning(f"  ➜ [特效检查] 处理时发生错误: {e}")

    # --- 4. 文件大小检查 (直接使用 size_bytes) ---
    try:
        if rule.get("resubscribe_filesize_enabled"):
            file_size_bytes = asset_details.get('size_bytes')
            if file_size_bytes:
                operator = rule.get("resubscribe_filesize_operator", 'lt')
                threshold_gb = float(rule.get("resubscribe_filesize_threshold_gb", 10.0))
                file_size_gb = file_size_bytes / (1024**3)
                needs_resubscribe = False
                reason_text = ""
                if operator == 'lt' and file_size_gb < threshold_gb:
                    needs_resubscribe = True
                    reason_text = f"文件 < {threshold_gb} GB"
                elif operator == 'gt' and file_size_gb > threshold_gb:
                    needs_resubscribe = True
                    reason_text = f"文件 > {threshold_gb} GB"
                if needs_resubscribe:
                    reasons.append(reason_text)
    except (ValueError, TypeError, IndexError) as e:
        logger.warning(f"  ➜ [文件大小检查] 处理时发生错误: {e}")

    # --- 5. 音轨和字幕检查 (豁免逻辑) ---
    is_exempted = _is_exempted_from_chinese_check(asset_details.get('media_streams', []), media_metadata)
    
    # --- 6. 音轨检查 (直接使用 audio_languages_raw) ---
    try:
        if rule.get("resubscribe_audio_enabled") and not is_exempted:
            required_langs = set(rule.get("resubscribe_audio_missing_languages", []))
            if 'chi' in required_langs or 'yue' in required_langs:
                detected_audio_langs = set(asset_details.get('audio_languages_raw', []))
                if 'chi' not in detected_audio_langs and 'yue' not in detected_audio_langs:
                    reasons.append("缺中文音轨")
    except Exception as e:
        logger.warning(f"  ➜ [音轨检查] 处理时发生未知错误: {e}")

    # --- 7. 字幕检查 (直接使用 subtitle_languages_raw) ---
    try:
        if rule.get("resubscribe_subtitle_enabled") and not is_exempted:
            required_langs = set(rule.get("resubscribe_subtitle_missing_languages", []))
            if 'chi' in required_langs:
                detected_subtitle_langs = set(asset_details.get('subtitle_languages_raw', []))
                if 'chi' not in detected_subtitle_langs and 'yue' not in detected_subtitle_langs:
                    reasons.append("缺中文字幕")
    except Exception as e:
        logger.warning(f"  ➜ [字幕检查] 处理时发生未知错误: {e}")
                 
    if reasons:
        final_reason = "; ".join(sorted(list(set(reasons))))
        logger.info(f"  ➜ 《{item_name}》需要洗版。原因: {final_reason}")
        return True, final_reason
    else:
        logger.debug(f"  ➜ 《{item_name}》质量达标。")
        return False, ""

def _is_exempted_from_chinese_check(media_streams: list, media_metadata: Optional[dict]) -> bool:
    """
    判断一个媒体是否应该免除中文音轨/字幕的检查（例如，本身就是国产影视剧）。
    这个函数保持原样，因为它依赖的是媒体元数据，而不是文件技术细节。
    """
    import re
    CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}
    if media_metadata and media_metadata.get('countries_json'):
        if not set(media_metadata['countries_json']).isdisjoint(CHINESE_SPEAKING_REGIONS): return True
    if media_metadata and (original_title := media_metadata.get('original_title')):
        if len(re.findall(r'[\u4e00-\u9fff]', original_title)) >= 2: return True
    
    # 即使元数据不明确，也最后检查一下媒体流自身是否包含中文信息
    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    if 'chi' in detected_audio_langs or 'yue' in detected_audio_langs: return True
    detected_subtitle_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
    if 'chi' in detected_subtitle_langs or 'yue' in detected_subtitle_langs: return True
    
    return False

def build_resubscribe_payload(item_details: dict, rule: Optional[dict]) -> Optional[dict]:
    """构建发送给 MoviePilot 的订阅 payload。"""
    from .subscriptions import AUDIO_SUBTITLE_KEYWORD_MAP
    from datetime import date, datetime

    item_name = item_details.get('item_name')
    tmdb_id_str = str(item_details.get('tmdb_id', '')).strip()
    item_type = item_details.get('item_type')

    if not all([item_name, tmdb_id_str, item_type]):
        logger.error(f"构建Payload失败：缺少核心媒体信息。来源: {item_details}")
        return None
    
    try:
        tmdb_id = int(tmdb_id_str)
    except (ValueError, TypeError):
        logger.error(f"构建Payload失败：TMDB ID '{tmdb_id_str}' 无效。")
        return None

    base_series_name = item_name.split(' - 第')[0]
    media_type_for_payload = "电视剧" if item_type in ["Series", "Season"] else "电影"

    payload = {
        "name": base_series_name,
        "tmdbid": tmdb_id,
        "type": media_type_for_payload,
        "best_version": 1
    }

    if item_type == "Season":
        season_num = item_details.get('season_number')
        if season_num is not None:
            payload['season'] = int(season_num)
        else:
            logger.error(f"严重错误：项目 '{item_name}' 类型为 'Season' 但未找到 'season_number'！")

    # ★★★ 核心修改：直接从 item_details 获取预先分析好的发布组 ★★★
    # 不再调用 _extract_exclusion_keywords_from_filename 函数
    exclusion_keywords_list = item_details.get('release_group_raw', [])
    
    if exclusion_keywords_list:
        # 使用正向先行断言实现 AND 逻辑
        and_regex_parts = [f"(?=.*{re.escape(k)})" for k in exclusion_keywords_list]
        payload['exclude'] = "".join(and_regex_parts)
        logger.info(f"  ➜ 精准排除模式：已为《{item_name}》生成 AND 逻辑正则: {payload['exclude']}")
    else:
        logger.info(f"  ✅ 未找到预分析的发布组，不添加排除规则。")
    # ★★★ 修改结束 ★★★

    use_custom_subscribe = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_USE_CUSTOM_RESUBSCRIBE, False)
    if not use_custom_subscribe or not rule:
        return payload

    rule_name = rule.get('name', '未知规则')
    final_include_lookaheads = []

    # --- 分辨率、质量 (逻辑不变) ---
    if rule.get("resubscribe_resolution_enabled"):
        threshold = rule.get("resubscribe_resolution_threshold")
        target_resolution = None
        if threshold == 3840: target_resolution = "4k"
        elif threshold == 1920: target_resolution = "1080p"
        if target_resolution:
            payload['resolution'] = target_resolution
            logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 分辨率: {target_resolution}")
    if rule.get("resubscribe_quality_enabled"):
        quality_list = rule.get("resubscribe_quality_include")
        if isinstance(quality_list, list) and quality_list:
            payload['quality'] = ",".join(quality_list)
            logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加过滤器 - 质量: {payload['quality']}")
    
    # --- 特效订阅逻辑 (实战优化) ---
    if rule.get("resubscribe_effect_enabled"):
        effect_list = rule.get("resubscribe_effect_include", [])
        if isinstance(effect_list, list) and effect_list:
            simple_effects_for_payload = set()
            
            EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
            # ★★★ 核心修改：将 "dv" 加入正则 ★★★
            EFFECT_PARAM_MAP = {
                "dovi_p8": ("(?=.*(dovi|dolby|dv))(?=.*hdr)", "dovi"),
                "dovi_p7": ("(?=.*(dovi|dolby|dv))(?=.*(p7|profile.?7))", "dovi"),
                "dovi_p5": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "dovi_other": ("(?=.*(dovi|dolby|dv))", "dovi"),
                "hdr10+": ("(?=.*(hdr10\+|hdr10plus))", "hdr10+"),
                "hdr": ("(?=.*hdr)", "hdr")
            }
            OLD_EFFECT_MAP = {"杜比视界": "dovi_other", "HDR": "hdr"}

            highest_req_priority = 999
            best_effect_choice = None
            for choice in effect_list:
                normalized_choice = OLD_EFFECT_MAP.get(choice, choice)
                try:
                    priority = EFFECT_HIERARCHY.index(normalized_choice)
                    if priority < highest_req_priority:
                        highest_req_priority = priority
                        best_effect_choice = normalized_choice
                except ValueError: continue
            
            if best_effect_choice:
                regex_pattern, simple_effect = EFFECT_PARAM_MAP.get(best_effect_choice, (None, None))
                if regex_pattern:
                    final_include_lookaheads.append(regex_pattern)
                if simple_effect:
                    simple_effects_for_payload.add(simple_effect)

            if simple_effects_for_payload:
                 payload['effect'] = ",".join(simple_effects_for_payload)

    # --- 音轨、字幕处理 (逻辑不变) ---
    if rule.get("resubscribe_audio_enabled"):
        audio_langs = rule.get("resubscribe_audio_missing_languages", [])
        if isinstance(audio_langs, list) and audio_langs:
            audio_keywords = [k for lang in audio_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(lang, [])]
            if audio_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(audio_keywords)), key=len, reverse=True))}))")

    if rule.get("resubscribe_subtitle_effect_only"):
        final_include_lookaheads.append("(?=.*特效)")
    elif rule.get("resubscribe_subtitle_enabled"):
        subtitle_langs = rule.get("resubscribe_subtitle_missing_languages", [])
        if isinstance(subtitle_langs, list) and subtitle_langs:
            subtitle_keywords = [k for lang in subtitle_langs for k in AUDIO_SUBTITLE_KEYWORD_MAP.get(f"sub_{lang}", [])]
            if subtitle_keywords:
                final_include_lookaheads.append(f"(?=.*({'|'.join(sorted(list(set(subtitle_keywords)), key=len, reverse=True))}))")

    if final_include_lookaheads:
        payload['include'] = "".join(final_include_lookaheads)
        logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 生成的 AND 正则过滤器(精筛): {payload['include']}")

    return payload

def _execute_resubscribe(processor, task_name: str, target):
    """执行洗版订阅的通用函数。"""
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    if isinstance(target, str) and target == "needed":
        items_to_subscribe = resubscribe_db.get_all_needed_resubscribe_items()
    elif isinstance(target, list):
        items_to_subscribe = resubscribe_db.get_resubscribe_cache_by_ids(target)
    else:
        task_manager.update_status_from_thread(-1, "任务失败：无效的目标参数")
        return

    total = len(items_to_subscribe)
    if total == 0:
        task_manager.update_status_from_thread(100, "任务完成：没有需要洗版的项目。")
        return

    all_rules = resubscribe_db.get_all_resubscribe_rules()
    config = processor.config
    delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))
    resubscribed_count, deleted_count = 0, 0

    for i, item in enumerate(items_to_subscribe):
        if processor.is_stop_requested(): break
        
        current_quota = settings_db.get_subscription_quota()
        if current_quota <= 0:
            logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
            break

        item_id = item.get('item_id')
        item_name = item.get('item_name')
        task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) [配额:{current_quota}] 正在订阅: {item_name}")

        rule = next((r for r in all_rules if r['id'] == item.get('matched_rule_id')), None)
        payload = build_resubscribe_payload(item, rule)
        if not payload: continue

        if moviepilot.subscribe_with_custom_payload(payload, config):
            settings_db.decrement_subscription_quota()
            resubscribed_count += 1
            
            if rule and rule.get('delete_after_resubscribe'):
                id_to_delete = item.get('emby_item_id') or item_id
                if emby.delete_item(id_to_delete, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
                    resubscribe_db.delete_resubscribe_cache_item(item_id)
                    deleted_count += 1
                else:
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
            else:
                resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
            
            if i < total - 1: time.sleep(delay)

    final_message = f"任务完成！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
    task_manager.update_status_from_thread(100, final_message)