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
from database import connection, resubscribe_db, settings_db, media_db

# ★★★ 4. 补上所有从 helpers 导入的辅助函数和常量 ★★★
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
# ★★★ 新增：本地数据库查询辅助函数 ★★★
# ======================================================================

def _get_media_details_from_db(tmdb_ids: List[str]) -> List[Dict]:
    """
    直接从数据库批量获取媒体元数据，以修复对外部模块的错误依赖。
    """
    if not tmdb_ids:
        return []
    try:
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 查询洗版检查所需的所有字段
                sql = """
                    SELECT 
                        tmdb_id, item_type, title, original_title, 
                        countries_json, asset_details_json 
                    FROM media_metadata 
                    WHERE tmdb_id = ANY(%s)
                """
                cursor.execute(sql, (tmdb_ids,))
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 在 resubscribe 任务中直接查询媒体元数据失败: {e}", exc_info=True)
        return []

def _get_episodes_from_db(series_tmdb_ids: List[str]) -> List[Dict]:
    """
    直接从数据库批量获取指定剧集的所有分集元数据。
    """
    if not series_tmdb_ids:
        return []
    try:
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT 
                        parent_series_tmdb_id, season_number, asset_details_json
                    FROM media_metadata 
                    WHERE item_type = 'Episode' AND parent_series_tmdb_id = ANY(%s)
                    ORDER BY season_number, episode_number
                """
                cursor.execute(sql, (series_tmdb_ids,))
                return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"  ➜ 在 resubscribe 任务中直接查询分集元数据失败: {e}", exc_info=True)
        return []

# ======================================================================
# 核心任务：刷新洗版状态
# ======================================================================

def task_update_resubscribe_cache(processor, force_full_update: bool = False):
    """
    【V4 - 数据库中心化重构版】
    扫描指定的媒体库，完全依赖本地 media_metadata 缓存进行分析，实现极速扫描。
    """
    scan_mode = "深度模式" if force_full_update else "快速模式"
    task_name = f"刷新洗版状态 ({scan_mode})"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
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
            fields="Id,Type,ProviderIds,SeriesId,ParentId,ParentIndexNumber,IndexNumber,_SourceLibraryId,Name"
        ) or []

        tmdb_ids_in_scope = {item['ProviderIds'].get('Tmdb') for item in emby_index if item.get('ProviderIds', {}).get('Tmdb')}
        
        if not tmdb_ids_in_scope:
            task_manager.update_status_from_thread(100, "任务完成：目标媒体库为空。")
            return

        logger.info(f"  ➜ 正在从本地数据库批量获取 {len(tmdb_ids_in_scope)} 个媒体项的详细元数据...")
        
        # ★★★ 核心修复：调用本地实现的辅助函数 ★★★
        all_metadata_from_db = _get_media_details_from_db(list(tmdb_ids_in_scope))
        
        if not all_metadata_from_db:
            logger.warning("  ➜ 未能从数据库获取到任何元数据，扫描可能不准确。")
            metadata_map = {}
        else:
            # 这里的代码现在是安全的了
            metadata_map = {meta['tmdb_id']: meta for meta in all_metadata_from_db}

        series_tmdb_ids = {meta['tmdb_id'] for meta in all_metadata_from_db if meta['item_type'] == 'Series'}
        
        # ★★★ 核心修复：调用本地实现的辅助函数 ★★★
        all_episodes_from_db = _get_episodes_from_db(list(series_tmdb_ids))
        
        episodes_map = defaultdict(list)
        for ep in all_episodes_from_db:
            episodes_map[ep['parent_series_tmdb_id']].append(ep)

        movies_to_process = []
        series_to_process_map = defaultdict(list)

        # 筛选出所有顶层项目（电影和剧集）
        top_level_items = [item for item in emby_index if item.get('Type') in ['Movie', 'Series']]

        if force_full_update:
            logger.info("  ➜ [深度模式] 将对所有项目进行全面分析。")
            resubscribe_db.clear_resubscribe_cache_except_ignored()
            for item in top_level_items:
                if item.get('Type') == 'Movie':
                    movies_to_process.append(item)
                else:
                    series_to_process_map[item.get('Id')] = item
        else:
            logger.info("  ➜ [快速模式] 将进行增量扫描...")
            cached_items = resubscribe_db.get_all_resubscribe_cache()
            cached_ids = {item['item_id'] for item in cached_items}

            # 清理已删除的项目
            current_emby_item_ids = {item.get('Id') for item in top_level_items if item.get('Type') == 'Movie'}
            seasons_in_emby = {f"{item['SeriesId']}-S{item['ParentIndexNumber']}" for item in emby_index if item.get('Type') == 'Episode' and item.get('SeriesId')}
            current_emby_item_ids.update(seasons_in_emby)
            deleted_ids = list(cached_ids - current_emby_item_ids)
            if deleted_ids:
                resubscribe_db.delete_resubscribe_cache_items_batch(deleted_ids)

            # 找出需要处理的新项目
            for item in top_level_items:
                if item.get('Type') == 'Movie':
                    if item.get('Id') not in cached_ids:
                        movies_to_process.append(item)
                elif item.get('Type') == 'Series':
                    # 只要剧集的任何一季不在缓存中，就认为整个剧集需要重新处理
                    seasons_for_series = {f"{item.get('Id')}-S{ep.get('ParentIndexNumber')}" for ep in emby_index if ep.get('SeriesId') == item.get('Id')}
                    if not seasons_for_series.issubset(cached_ids):
                        series_to_process_map[item.get('Id')] = item
        
        total = len(movies_to_process) + len(series_to_process_map)

        if total == 0:
            task_manager.update_status_from_thread(100, f"任务完成：({scan_mode}) 无需处理任何新项目。")
            return

        logger.info(f"  ➜ 将对 {len(movies_to_process)} 个电影和 {len(series_to_process_map)} 个剧集按规则检查洗版状态...")
        cache_update_batch = []
        processed_count = 0

        for movie_index in movies_to_process:
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80) if total > 0 else 100
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {movie_index.get('Name')}")

            tmdb_id = movie_index.get('ProviderIds', {}).get('Tmdb')
            metadata = metadata_map.get(tmdb_id)
            if not metadata or not metadata.get('asset_details_json'): continue
            
            asset = metadata['asset_details_json'][0]
            rule = library_to_rule_map.get(movie_index.get('_SourceLibraryId'))
            if not rule: continue

            needs, reason = _item_needs_resubscribe(asset, rule, metadata)
            status = 'needed' if needs else 'ok'
            
            cache_update_batch.append({
                "item_id": movie_index.get('Id'), "emby_item_id": movie_index.get('Id'),
                "item_name": movie_index.get('Name'), "tmdb_id": tmdb_id, "item_type": "Movie",
                "status": status, "reason": reason, **analyze_media_asset(asset),
                "matched_rule_id": rule.get('id'), "matched_rule_name": rule.get('name'),
                "source_library_id": movie_index.get('_SourceLibraryId'),
                "path": asset.get('path'), "filename": os.path.basename(asset.get('path', ''))
            })

        for series_id, series_index in series_to_process_map.items():
            if processor.is_stop_requested(): break
            processed_count += 1
            progress = int(20 + (processed_count / total) * 80) if total > 0 else 100
            task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {series_index.get('Name')}")

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
                
                representative_episode = episodes_in_season[0]
                if not representative_episode.get('asset_details_json'): continue
                
                asset = representative_episode['asset_details_json'][0]
                needs, reason = _item_needs_resubscribe(asset, rule, series_metadata)
                status = 'needed' if needs else 'ok'

                season_item_id = f"{series_id}-S{season_num}"
                season_emby_id = next((item.get('Id') for item in emby_index if item.get('Type') == 'Season' and item.get('ParentId') == series_id and item.get('IndexNumber') == season_num), None)

                cache_update_batch.append({
                    "item_id": season_item_id, "emby_item_id": season_emby_id, "series_id": series_id,
                    "season_number": season_num, "item_name": f"{series_index.get('Name')} - 第 {season_num} 季",
                    "tmdb_id": tmdb_id, "item_type": "Season", "status": status, "reason": reason,
                    **analyze_media_asset(asset),
                    "matched_rule_id": rule.get('id'), "matched_rule_name": rule.get('name'),
                    "source_library_id": series_index.get('_SourceLibraryId'),
                    "path": asset.get('path'), "filename": os.path.basename(asset.get('path', ''))
                })

        if cache_update_batch:
            resubscribe_db.upsert_resubscribe_cache_batch(cache_update_batch)
            
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
    
    # ★★★ 修正：需要从数据库获取缓存项，而不是直接用ID ★★★
    items_to_delete = []
    if item_ids:
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)", (item_ids,))
                items_to_delete = [dict(row) for row in cursor.fetchall()]

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

def _item_needs_resubscribe(asset_details: dict, rule: dict, media_metadata: Optional[dict]) -> tuple[bool, str]:
    """
    【V4 - 数据库中心化重构版】
    判断单个媒体资产是否需要洗版的核心逻辑，数据源为数据库缓存。
    """
    item_name = media_metadata.get('title', '未知项目')
    logger.trace(f"  ➜ [洗版检查] 开始为《{item_name}》检查洗版需求...")
    
    media_streams = asset_details.get('media_streams', [])
    file_path = asset_details.get('path', '')
    video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)

    reasons = []

    # 1. 分辨率检查
    try:
        if rule.get("resubscribe_resolution_enabled"):
            if not video_stream:
                reasons.append("无视频流信息")
            else:
                threshold_width = int(rule.get("resubscribe_resolution_threshold") or 1920)
                required_tier, required_tier_name = _get_resolution_tier(threshold_width, 0)
                current_width = int(video_stream.get('width') or 0)
                current_height = int(video_stream.get('height') or 0)
                current_tier, _ = _get_resolution_tier(current_width, current_height)
                if current_tier < required_tier:
                    reasons.append(f"分辨率 < {required_tier_name}")
    except (ValueError, TypeError) as e:
        logger.warning(f"  ➜ [分辨率检查] 处理时发生类型错误: {e}")

    # 2. 质量检查
    try:
        if rule.get("resubscribe_quality_enabled"):
            required_list = rule.get("resubscribe_quality_include", [])
            if isinstance(required_list, list) and required_list:
                required_list_lower = [str(q).lower() for q in required_list]
                current_quality = asset_details.get('quality_display', '').lower()
                if not any(term in current_quality for term in required_list_lower):
                    reasons.append("质量不符")
    except Exception as e:
        logger.warning(f"  ➜ [质量检查] 处理时发生未知错误: {e}")

    # 3. 特效检查
    try:
        if rule.get("resubscribe_effect_enabled"):
            user_choices = rule.get("resubscribe_effect_include", [])
            if isinstance(user_choices, list) and user_choices:
                EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
                OLD_EFFECT_MAP = {"杜比视界": "dovi_other", "HDR": "hdr"}
                highest_req_priority = 999
                for choice in user_choices:
                    normalized_choice = OLD_EFFECT_MAP.get(choice, choice)
                    try:
                        priority = EFFECT_HIERARCHY.index(normalized_choice)
                        if priority < highest_req_priority:
                            highest_req_priority = priority
                    except ValueError: continue
                
                if highest_req_priority < 999:
                    current_effect_list = asset_details.get('effect_display', [])
                    # 将 effect_display 中的中文转为 key
                    standardized_effects = []
                    for e in current_effect_list:
                        e_lower = str(e).lower()
                        if 'dolby vision' in e_lower: standardized_effects.append('dovi_other')
                        elif 'hdr10+' in e_lower: standardized_effects.append('hdr10+')
                        elif 'hdr' in e_lower: standardized_effects.append('hdr')

                    current_best_effect = min(standardized_effects, key=lambda e: EFFECT_HIERARCHY.index(e) if e in EFFECT_HIERARCHY else 999) if standardized_effects else 'sdr'
                    current_priority = EFFECT_HIERARCHY.index(current_best_effect)
                    if current_priority > highest_req_priority:
                        reasons.append("特效不符")
    except Exception as e:
        logger.warning(f"  ➜ [特效检查] 处理时发生未知错误: {e}")

    # 4. 文件大小检查
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

    # 5. 音轨和字幕检查
    def _is_exempted_from_chinese_check(media_streams: list, media_metadata: Optional[dict]) -> bool:
        import re
        CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}
        if media_metadata and media_metadata.get('countries_json'):
            if not set(media_metadata['countries_json']).isdisjoint(CHINESE_SPEAKING_REGIONS): return True
        if media_metadata and (original_title := media_metadata.get('original_title')):
            if len(re.findall(r'[\u4e00-\u9fff]', original_title)) >= 2: return True
        detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
        if 'chi' in detected_audio_langs or 'yue' in detected_audio_langs: return True
        detected_subtitle_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
        if 'chi' in detected_subtitle_langs or 'yue' in detected_subtitle_langs: return True
        return False

    is_exempted = _is_exempted_from_chinese_check(media_streams, media_metadata)
    
    try:
        if rule.get("resubscribe_audio_enabled") and not is_exempted:
            required_langs = set(rule.get("resubscribe_audio_missing_languages", []))
            if 'chi' in required_langs or 'yue' in required_langs:
                detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
                if 'chi' not in detected_audio_langs and 'yue' not in detected_audio_langs:
                    reasons.append("缺中文音轨")
    except Exception as e:
        logger.warning(f"  ➜ [音轨检查] 处理时发生未知错误: {e}")

    try:
        if rule.get("resubscribe_subtitle_enabled") and not is_exempted:
            required_langs = set(rule.get("resubscribe_subtitle_missing_languages", []))
            if 'chi' in required_langs:
                detected_subtitle_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
                if 'chi' not in detected_subtitle_langs and 'yue' not in detected_subtitle_langs:
                    if any(s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
                        detected_subtitle_langs.add('chi')
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

def _build_resubscribe_payload(item_details: dict, rule: Optional[dict]) -> Optional[dict]:
    """构建发送给 MoviePilot 的订阅 payload。"""
    from .subscriptions import _extract_exclusion_keywords_from_filename
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
        "name": base_series_name, "tmdbid": tmdb_id,
        "type": media_type_for_payload, "best_version": 1
    }

    if item_type == "Season":
        season_num = item_details.get('season_number')
        if season_num is not None:
            payload['season'] = int(season_num)
        else:
            logger.error(f"  ➜ 严重错误：项目 '{item_name}' 类型为 'Season' 但未找到 'season_number'！")

    original_filename = item_details.get('filename')
    if original_filename:
        exclusion_keywords_list = _extract_exclusion_keywords_from_filename(original_filename)
        if exclusion_keywords_list:
            and_regex_parts = [f"(?=.*{re.escape(k)})" for k in exclusion_keywords_list]
            payload['exclude'] = "".join(and_regex_parts)
    
    use_custom_subscribe = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_USE_CUSTOM_RESUBSCRIBE, False)
    if not use_custom_subscribe or not rule:
        return payload

    rule_name = rule.get('name', '未知规则')
    final_include_lookaheads = []

    if rule.get("resubscribe_resolution_enabled"):
        threshold = rule.get("resubscribe_resolution_threshold")
        target_resolution = None
        if threshold == 3840: target_resolution = "4k"
        elif threshold == 1920: target_resolution = "1080p"
        if target_resolution: payload['resolution'] = target_resolution
    
    if rule.get("resubscribe_quality_enabled"):
        quality_list = rule.get("resubscribe_quality_include")
        if isinstance(quality_list, list) and quality_list:
            payload['quality'] = ",".join(quality_list)
    
    if rule.get("resubscribe_effect_enabled"):
        effect_list = rule.get("resubscribe_effect_include", [])
        if isinstance(effect_list, list) and effect_list:
            simple_effects_for_payload = set()
            EFFECT_HIERARCHY = ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
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
                if regex_pattern: final_include_lookaheads.append(regex_pattern)
                if simple_effect: simple_effects_for_payload.add(simple_effect)
            if simple_effects_for_payload: payload['effect'] = ",".join(simple_effects_for_payload)

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

    return payload

def _execute_resubscribe(processor, task_name: str, target):
    """执行洗版订阅的通用函数。"""
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    items_to_subscribe = []
    with connection.get_db_connection() as conn:
        with conn.cursor() as cursor:
            if isinstance(target, str) and target == "needed":
                cursor.execute("SELECT * FROM resubscribe_cache WHERE status = 'needed'")
                items_to_subscribe = [dict(row) for row in cursor.fetchall()]
            elif isinstance(target, list) and target:
                cursor.execute("SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)", (target,))
                items_to_subscribe = [dict(row) for row in cursor.fetchall()]

    if not items_to_subscribe:
        task_manager.update_status_from_thread(100, "任务完成：没有需要洗版的项目。")
        return

    total = len(items_to_subscribe)
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
        payload = _build_resubscribe_payload(item, rule)
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