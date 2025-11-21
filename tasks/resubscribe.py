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
        # --- 步骤 1: 加载规则和确定扫描范围 (逻辑不变) ---
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

        # --- 步骤 2: 获取Emby全量数据 (逻辑不变) ---
        task_manager.update_status_from_thread(10, f"正在从 {len(all_target_lib_ids)} 个目标库中建立媒体索引...")
        emby_index = emby.get_all_library_versions(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series,Episode", library_ids=list(all_target_lib_ids),
            fields="Id,Type,ProviderIds,SeriesId,ParentIndexNumber,_SourceLibraryId,Name",
            update_status_callback=task_manager.update_status_from_thread
        ) or []

        # ★★★ 步骤 3: 预处理Emby数据，清晰分类 ★★★
        movies_to_process = []
        series_to_process = []
        # 使用 series_emby_id 作为键，值为该剧集的所有分集列表
        series_episodes_map = defaultdict(list)
        
        for item in emby_index:
            item_type = item.get('Type')
            if item_type == 'Movie':
                movies_to_process.append(item)
            elif item_type == 'Series':
                series_to_process.append(item)
            elif item_type == 'Episode' and item.get('SeriesId'):
                series_episodes_map[item['SeriesId']].append(item)

        # --- 步骤 4: 批量获取数据库元数据 (逻辑优化) ---
        tmdb_ids_in_scope = {str(item['ProviderIds']['Tmdb']) for item in movies_to_process + series_to_process if item.get('ProviderIds', {}).get('Tmdb')}
        if not tmdb_ids_in_scope:
            task_manager.update_status_from_thread(100, "任务完成：目标媒体库为空。")
            return

        logger.info(f"  ➜ 正在从本地数据库批量获取 {len(tmdb_ids_in_scope)} 个媒体项的详细元数据...")
        metadata_map = media_db.get_media_details_by_tmdb_ids(list(tmdb_ids_in_scope))
        
        series_tmdb_ids = {meta['tmdb_id'] for meta in metadata_map.values() if meta.get('item_type') == 'Series'}
        all_episodes_from_db = media_db.get_episodes_for_series(list(series_tmdb_ids))
        episodes_metadata_map = defaultdict(list)
        for ep in all_episodes_from_db:
            episodes_metadata_map[ep['parent_series_tmdb_id']].append(ep)

        # --- 步骤 5: 清理Emby中已删除的旧索引 (逻辑不变) ---
        logger.info("  ➜ 正在比对并清理陈旧的洗版索引...")
        indexed_keys = resubscribe_db.get_all_resubscribe_index_keys()
        current_emby_keys = set()
        for item in movies_to_process:
            if tmdb_id := item.get('ProviderIds', {}).get('Tmdb'):
                current_emby_keys.add(str(tmdb_id))
        for series_item in series_to_process:
            if tmdb_id := series_item.get('ProviderIds', {}).get('Tmdb'):
                # 确定这部剧实际存在哪些季
                seasons_in_series = {ep.get('ParentIndexNumber') for ep in series_episodes_map.get(series_item.get('Id'), []) if ep.get('ParentIndexNumber') is not None}
                for season_num in seasons_in_series:
                    current_emby_keys.add(f"{tmdb_id}-S{season_num}")
        
        deleted_keys = indexed_keys - current_emby_keys
        if deleted_keys:
            resubscribe_db.delete_resubscribe_index_by_keys(list(deleted_keys))

        # ★★★ 步骤 6: 全新、高效的全量处理流程 ★★★
        total = len(movies_to_process) + len(series_to_process)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：无需处理任何项目。")
            return

        logger.info(f"  ➜ 将对 {len(movies_to_process)} 部电影和 {len(series_to_process)} 部剧集按规则检查洗版状态...")
        index_update_batch = []
        processed_count = 0

        # ★★★在处理前，预先加载所有已存在的状态 ★★★
        logger.info("  ➜ 正在获取当前所有项目的状态以保留用户操作...")
        current_statuses = resubscribe_db.get_current_index_statuses()

        # --- 6a. 处理所有电影 ---
        for movie_index in movies_to_process:
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80)
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析电影: {movie_index.get('Name')}")

            tmdb_id = movie_index.get('ProviderIds', {}).get('Tmdb')
            metadata = metadata_map.get(tmdb_id)
            rule = library_to_rule_map.get(movie_index.get('_SourceLibraryId'))

            if not all([tmdb_id, metadata, rule]) or not metadata.get('asset_details_json'):
                continue
            
            if metadata.get('item_type') != 'Movie':
                logger.warning(f"  ➜ 检测到项目 '{metadata.get('title')}' (TMDB ID: {tmdb_id}) 被Emby错误识别为电影，实际类型为 '{metadata.get('item_type')}'。已跳过。")
                continue

            asset = metadata['asset_details_json'][0]
            needs, reason = _item_needs_resubscribe(asset, rule, metadata)
            status = 'needed' if needs else 'ok'

            # ★★★ 检查现有状态，如果已被用户操作，则跳过 ★★★
            item_key = (str(tmdb_id), "Movie", -1)
            if current_statuses.get(item_key) in ['ignored', 'subscribed']:
                continue # 尊重用户的忽略或已订阅状态，不进行覆盖
            
            index_update_batch.append({
                "tmdb_id": tmdb_id, "item_type": "Movie", "season_number": -1,
                "status": status, "reason": reason, "matched_rule_id": rule.get('id')
            })

        # --- 6b. 处理所有剧集 ---
        for series_index in series_to_process:
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80)
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析剧集: {series_index.get('Name')}")

            tmdb_id = series_index.get('ProviderIds', {}).get('Tmdb')
            series_metadata = metadata_map.get(tmdb_id)
            rule = library_to_rule_map.get(series_index.get('_SourceLibraryId'))
            
            # 获取该剧集在数据库中所有分集的元数据
            episodes_for_series_from_db = episodes_metadata_map.get(tmdb_id)

            if not all([tmdb_id, series_metadata, rule, episodes_for_series_from_db]):
                continue

            # 按季号对数据库中的分集元数据进行分组
            episodes_by_season = defaultdict(list)
            for ep_meta in episodes_for_series_from_db:
                episodes_by_season[ep_meta.get('season_number')].append(ep_meta)

            for season_num, episodes_in_season_meta in episodes_by_season.items():
                if season_num is None or not episodes_in_season_meta: continue
                
                # 选取第一集作为代表进行分析
                representative_episode_meta = episodes_in_season_meta[0]
                if not representative_episode_meta.get('asset_details_json'): continue
                
                asset = representative_episode_meta['asset_details_json'][0]
                needs, reason = _item_needs_resubscribe(asset, rule, series_metadata)
                status = 'needed' if needs else 'ok'

                # ★★★ 检查现有状态，如果已被用户操作，则跳过 ★★★
                item_key = (str(tmdb_id), "Season", int(season_num))
                if current_statuses.get(item_key) in ['ignored', 'subscribed']:
                    continue # 尊重用户的忽略或已订阅状态，不进行覆盖

                index_update_batch.append({
                    "tmdb_id": tmdb_id, "item_type": "Season", "season_number": season_num,
                    "status": status, "reason": reason, "matched_rule_id": rule.get('id')
                })

        if index_update_batch:
            resubscribe_db.upsert_resubscribe_index_batch(index_update_batch)
            
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
    """【V3 - ID安全版】精准删除指定的项目，并增加ID有效性检查。"""
    task_name = "批量删除媒体"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_delete = resubscribe_db.get_resubscribe_items_by_ids(item_ids)
    total = len(items_to_delete)
    if total == 0:
        task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有可删除的项。")
        return

    deleted_count = 0
    for i, item in enumerate(items_to_delete):
        if processor.is_stop_requested(): break
        
        internal_item_id = item.get('item_id') # 这是我们内部的ID，用于日志和数据库操作
        item_name = item.get('item_name')
        task_manager.update_status_from_thread(int((i / total) * 100), f"({i+1}/{total}) 正在删除: {item_name}")
        
        # 只使用 emby_item_id，并进行严格检查 
        id_to_delete = item.get('emby_item_id')
        
        # 健壮性检查：如果ID无效或格式错误，则跳过并记录错误
        if not id_to_delete or not str(id_to_delete).isdigit():
            logger.error(f"  ➜ 无法删除 '{item_name}'：获取到的Emby ID无效 ('{id_to_delete}')。跳过此项。")
            continue
        
        if emby.delete_item(id_to_delete, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
            # 删除成功后，使用内部ID从我们的索引中移除记录
            resubscribe_db.delete_resubscribe_cache_item(internal_item_id)
            deleted_count += 1
        
        time.sleep(0.5)

    final_message = f"批量删除任务完成！成功删除了 {deleted_count} 个媒体项。"
    task_manager.update_status_from_thread(100, final_message)

# ======================================================================
# 内部辅助函数
# ======================================================================
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
        # 检查规则是否启用了质量洗版
        if rule.get("resubscribe_quality_enabled"):
            # 获取规则中要求的质量列表，例如 ['BluRay', 'WEB-DL']
            required_qualities = rule.get("resubscribe_quality_include", [])
            
            # 仅当规则中明确配置了要求时，才执行检查
            if required_qualities:
                # 1. 定义权威的“质量金字塔”等级（数字越大，等级越高）
                QUALITY_HIERARCHY = {
                    'remux': 6,
                    'bluray': 5,
                    'web-dl': 4,
                    'webrip': 3,
                    'hdtv': 2,
                    'dvdrip': 1,
                    'unknown': 0
                }

                # 2. 计算规则要求的“最高目标等级”
                #    例如，如果规则是 ['BluRay', 'WEB-DL']，那么目标就是达到 BluRay (等级5)
                highest_required_tier = 0
                for req_quality in required_qualities:
                    highest_required_tier = max(highest_required_tier, QUALITY_HIERARCHY.get(req_quality.lower(), 0))

                # 3. 获取当前文件经过分析后得出的“质量标签”
                current_quality_tag = asset_details.get('quality_display', 'Unknown').lower()
                
                # 4. 计算当前文件所处的“实际质量等级”
                current_actual_tier = QUALITY_HIERARCHY.get(current_quality_tag, 0)

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("质量不符")
    except Exception as e:
        logger.warning(f"  ➜ [质量检查] 处理时发生错误: {e}")

    # --- 3. 特效检查 (直接使用 effect_display) ---
    try:
        # 检查规则是否启用了特效洗版
        if rule.get("resubscribe_effect_enabled"):
            # 获取规则中要求的特效列表，例如 ['dovi_p8', 'hdr10+']
            required_effects = rule.get("resubscribe_effect_include", [])
            
            # 仅当规则中明确配置了要求时，才执行检查
            if required_effects:
                # 1. 定义权威的“特效金字塔”等级（数字越大，等级越高）
                #    这个层级严格对应 helpers.py 中 _get_standardized_effect 的输出
                EFFECT_HIERARCHY = {
                    "dovi_p8": 7,
                    "dovi_p7": 6,
                    "dovi_p5": 5,
                    "dovi_other": 4,
                    "hdr10+": 3,
                    "hdr": 2,
                    "sdr": 1
                }

                # 2. 计算规则要求的“最高目标等级”
                #    例如，如果规则是 ['hdr', 'dovi_p5']，那么目标就是达到 d_p5 (等级5)
                highest_required_tier = 0
                for req_effect in required_effects:
                    highest_required_tier = max(highest_required_tier, EFFECT_HIERARCHY.get(req_effect.lower(), 0))

                # 3. 获取当前文件经过 helpers.py 分析后得出的“权威特效标识”
                #    asset_details['effect_display'] 现在存储的是 'dovi_p8' 这样的精确字符串
                current_effect_tag = asset_details.get('effect_display', 'sdr')
                
                # 4. 计算当前文件所处的“实际特效等级”
                current_actual_tier = EFFECT_HIERARCHY.get(current_effect_tag.lower(), 1) # 默认为sdr等级

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("特效不达标")
    except Exception as e:
        logger.warning(f"  ➜ [特效检查] 处理时发生错误: {e}")

    # --- 4. 编码检查 ---
    try:
        # 检查规则是否启用了编码洗版
        if rule.get("resubscribe_codec_enabled"):
            # 获取规则中要求的编码列表，例如 ['hevc']
            required_codecs = rule.get("resubscribe_codec_include", [])
            
            if required_codecs:
                # 1. 定义“编码金字塔”等级（数字越大，等级越高）
                #    为常见别名设置相同等级，增强兼容性
                CODEC_HIERARCHY = {
                    'hevc': 2, 'h265': 2,
                    'h264': 1, 'avc': 1,
                    'unknown': 0
                }

                # 2. 计算规则要求的“最高目标等级”
                highest_required_tier = 0
                for req_codec in required_codecs:
                    highest_required_tier = max(highest_required_tier, CODEC_HIERARCHY.get(req_codec.lower(), 0))

                # 3. 获取当前文件经过分析后得出的“编码标签”
                current_codec_tag = asset_details.get('codec_display', 'unknown').lower()
                
                # 4. 计算当前文件所处的“实际编码等级”
                current_actual_tier = CODEC_HIERARCHY.get(current_codec_tag, 0)

                # 5. 最终裁决：如果文件的实际等级 < 规则的最高目标等级，则判定为不达标
                if current_actual_tier < highest_required_tier:
                    reasons.append("编码不符")
    except Exception as e:
        logger.warning(f"  ➜ [编码检查] 处理时发生错误: {e}")

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
        if rule.get("resubscribe_audio_enabled"):
            required_langs = rule.get("resubscribe_audio_missing_languages", [])
            if required_langs:
                current_audio_display = asset_details.get('audio_display', '')
                # 1. 净化：将 "国语, 英语" 或 "国语，英语" 都拆分成干净的列表 ['国语', '英语']
                #    使用正则表达式替换所有可能的逗号和多个空格
                existing_langs_set = set(re.split(r'[,\s，]+', current_audio_display))
                
                AUDIO_DISPLAY_MAP = {'chi': '国语', 'yue': '粤语', 'eng': '英语', 'jpn': '日语'}
                
                for lang_code in required_langs:
                    if lang_code in ['chi', 'yue'] and is_exempted:
                        continue
                    
                    display_name = AUDIO_DISPLAY_MAP.get(lang_code)
                    # 2. 比对：在净化后的集合中进行绝对可靠的比对
                    if display_name and display_name not in existing_langs_set:
                        reasons.append(f"缺{display_name}音轨")
    except Exception as e:
        logger.warning(f"  ➜ [音轨检查] 处理时发生未知错误: {e}")

    # --- 7. 字幕检查 (装甲版) ---
    try:
        if rule.get("resubscribe_subtitle_enabled"):
            required_langs = rule.get("resubscribe_subtitle_missing_languages", [])
            if required_langs:
                current_subtitle_display = asset_details.get('subtitle_display', '')
                existing_langs_set = set(re.split(r'[,\s，]+', current_subtitle_display))
                SUB_DISPLAY_MAP = {'chi': '简体', 'yue': '繁体', 'eng': '英文', 'jpn': '日文'}
                
                for lang_code in required_langs:
                    if lang_code in ['chi', 'yue'] and is_exempted:
                        continue
                        
                    display_name = SUB_DISPLAY_MAP.get(lang_code)
                    
                    if display_name and display_name not in existing_langs_set:
                        reasons.append(f"缺{display_name}字幕")

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
    【V2 - 修正版】
    判断一个媒体是否应该免除中文音轨/字幕的检查（例如，本身就是国产影视剧）。
    此判断【严格只依赖】媒体的元数据，避免因文件内容变化导致逻辑悖论。
    """
    import re
    # 定义华语地区集合，用于判断出品国家
    CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}
    
    # 1. 如果出品国家/地区在华语地区列表中，则豁免检查
    if media_metadata and media_metadata.get('countries_json'):
        # 使用 isdisjoint 判断两个集合是否有交集，比循环更高效
        if not set(media_metadata['countries_json']).isdisjoint(CHINESE_SPEAKING_REGIONS):
            return True
            
    # 2. 如果原始标题中包含至少两个汉字，则豁免检查
    if media_metadata and (original_title := media_metadata.get('original_title')):
        # 使用正则表达式查找中文字符
        if len(re.findall(r'[\u4e00-\u9fff]', original_title)) >= 2:
            return True
    
    # 如果以上条件都不满足，则不豁免，必须进行中文音轨/字幕检查
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

    # --- 排除原发布组 ---
    exclusion_keywords_list = item_details.get('release_group_raw', [])
    if exclusion_keywords_list:
        # 使用正向先行断言实现 AND 逻辑
        and_regex_parts = [f"(?=.*{re.escape(k)})" for k in exclusion_keywords_list]
        payload['exclude'] = "".join(and_regex_parts)
        logger.info(f"  ➜ 精准排除模式：已为《{item_name}》生成 AND 逻辑正则: {payload['exclude']}")
    else:
        logger.info(f"  ✅ 未找到预分析的发布组，不添加排除规则。")

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

    # --- 编码订阅逻辑 ---
    try:
        if rule.get("resubscribe_codec_enabled"):
            codec_list = rule.get("resubscribe_codec_include", [])
            if isinstance(codec_list, list) and codec_list:
                # 定义编码到正则表达式关键字的映射，增强匹配成功率
                CODEC_REGEX_MAP = {
                    'hevc': ['hevc', 'h265', 'x265'],
                    'h264': ['h264', 'avc', 'x264']
                }
                
                # 根据用户选择，构建一个大的 OR 正则组
                # 例如，如果用户选了 'hevc'，最终会生成 (hevc|h265|x265)
                regex_parts = []
                for codec in codec_list:
                    if codec.lower() in CODEC_REGEX_MAP:
                        regex_parts.extend(CODEC_REGEX_MAP[codec.lower()])
                
                if regex_parts:
                    # 将所有关键字用 | 连接，并放入一个正向先行断言中
                    # 这意味着“标题中必须包含这些关键字中的任意一个”
                    include_regex = f"(?=.*({'|'.join(regex_parts)}))"
                    final_include_lookaheads.append(include_regex)
                    logger.info(f"  ➜ 《{item_name}》按规则 '{rule_name}' 追加编码过滤器: {include_regex}")
    except Exception as e:
        logger.warning(f"  ➜ [编码订阅] 构建正则时发生错误: {e}")
    
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
        items_to_subscribe = resubscribe_db.get_resubscribe_items_by_ids(target)
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