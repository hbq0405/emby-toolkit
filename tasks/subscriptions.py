# tasks/subscriptions.py
# 智能订阅与媒体洗版任务模块

import os
import json
import time
import logging
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed # <--- 就是加上这一行！

# 导入需要的底层模块和共享实例
import config_manager
import constants
import emby_handler
import tmdb_handler
import moviepilot_handler
import task_manager
from database import connection, settings_db, resubscribe_db, collection_db
from .helpers import _get_standardized_effect, _extract_quality_tag_from_filename

logger = logging.getLogger(__name__)

def _get_detected_languages_from_streams(
    media_streams: List[dict], 
    stream_type: str, 
    lang_keyword_map: dict
) -> set:
    """
    【V2 - 智能识别版】
    从媒体流中检测指定类型（Audio/Subtitle）的语言。
    - 优先检查标准的 'Language' 字段。
    - 然后检查 'Title' 和 'DisplayTitle' 字段中的关键词。
    - 返回一个包含标准化语言代码的集合 (例如 {'chi', 'eng'})。
    """
    detected_langs = set()
    
    # 1. 优先从标准的 Language 字段获取信息
    standard_chinese_codes = {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn', 'yue'}
    standard_english_codes = {'eng'}
    standard_japanese_codes = {'jpn'}
    
    for stream in media_streams:
        if stream.get('Type') == stream_type and (lang_code := str(stream.get('Language', '')).lower()):
            if lang_code in standard_chinese_codes:
                detected_langs.add('chi')
            elif lang_code in standard_english_codes:
                detected_langs.add('eng')
            elif lang_code in standard_japanese_codes:
                detected_langs.add('jpn')

    # 2. 扫描 Title 和 DisplayTitle 作为补充
    for stream in media_streams:
        if stream.get('Type') == stream_type:
            # 将标题和显示标题合并，并转为小写，以便搜索
            title_string = (stream.get('Title', '') + stream.get('DisplayTitle', '')).lower()
            if not title_string:
                continue
            
            # 检查是否包含关键词
            for lang_key, keywords in lang_keyword_map.items():
                # lang_key 可能是 'chi', 'sub_chi', 'eng' 等
                normalized_lang_key = lang_key.replace('sub_', '')
                
                if any(keyword.lower() in title_string for keyword in keywords):
                    detected_langs.add(normalized_lang_key)

    return detected_langs

EFFECT_KEYWORD_MAP = {
    "杜比视界": ["dolby vision", "dovi"],
    "HDR": ["hdr", "hdr10", "hdr10+", "hlg"]
}

AUDIO_SUBTITLE_KEYWORD_MAP = {
    # 音轨关键词
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    # 字幕关键词 (可以和音轨共用，也可以分开定义)
    "sub_chi": ["CHS", "CHT", "中字", "简中", "繁中", "简", "繁"],
    "sub_eng": ["ENG", "英字"],
}

# ★★★ 定义分辨率等级辅助函数 ★★★
def _get_resolution_tier(width: int, height: int) -> tuple[int, str]:
    """根据视频的宽或高，将其归类到对应的分辨率等级。"""
    if width >= 3800 or height >= 2100:
        return 4, "4K"
    if width >= 1900 or height >= 1000:
        return 3, "1080p"
    if width >= 1200 or height >= 700:
        return 2, "720p"
    if height > 0:
        return 1, f"{height}p"
    return 0, "未知"

# --- 辅助函数：检查剧集或特定季是否完结，并返回洗版标志 ---
def _check_and_get_series_best_version_flag(series_tmdb_id: int, tmdb_api_key: str, season_number: Optional[int] = None, series_name: str = "未知剧集") -> Optional[int]:
    """
    辅助函数：检查剧集或特定季是否完结，并返回洗版标志。
    """
    if not tmdb_api_key:
        return None
    
    today = date.today()
    try:
        if season_number is not None:
            # 检查单季是否完结
            season_details = tmdb_handler.get_tv_details(series_tmdb_id, season_number, tmdb_api_key)
            if season_details and season_details.get('episodes'):
                last_episode = season_details['episodes'][-1]
                last_air_date_str = last_episode.get('air_date')
                if last_air_date_str:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    if last_air_date <= today:
                        logger.info(f"  ➜ 《{series_name}》第 {season_number} 季已完结，将以洗版模式订阅。")
                        return 1
        else:
            series_details = tmdb_handler.get_tv_details(series_tmdb_id, tmdb_api_key)
            if series_details and (last_episode_to_air := series_details.get('last_episode_to_air')):
                last_air_date_str = last_episode_to_air.get('air_date')
                if last_air_date_str:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    if last_air_date <= today:
                        logger.info(f"  ➜ 剧集《{series_name}》的最后一集已播出，将以洗版模式订阅。")
                        return 1
                        
    except Exception as e_tmdb:
        logger.warning(f"  ➜ 获取《{series_name}》详情失败: {e_tmdb}，将以普通模式订阅。")
    
    return None

# ★★★ 自动订阅任务 ★★★
def task_auto_subscribe(processor):
    """
    - 现在此任务会依次处理：原生合集、追剧、自定义合集、演员订阅，最后处理媒体洗版。
    - 一个任务搞定所有日常自动化订阅需求。
    """
    task_name = "缺失洗版订阅"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在启动缺失洗版订阅任务...")
    
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("  ➜ 订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        today = date.today()
        tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)

        task_manager.update_status_from_thread(10, "缺失洗版订阅已启动...")
        successfully_subscribed_items = []
        resubscribed_count = 0
        deleted_count = 0
        quota_exhausted = False

        with connection.get_db_connection() as conn:
            cursor = conn.cursor()

            # --- 1. 处理原生电影合集  ---
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(15, "正在检查原生电影合集...")
                sql_query_native_movies = "SELECT * FROM collections_info WHERE status = 'has_missing' AND missing_movies_json IS NOT NULL AND missing_movies_json != '[]'"
                cursor.execute(sql_query_native_movies)
                native_collections_to_check = cursor.fetchall()
                logger.info(f"  ➜ 找到 {len(native_collections_to_check)} 个有缺失影片的原生合集。")
                
                for collection in native_collections_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    
                    movies_to_keep = []
                    all_movies = collection['missing_movies_json']
                    movies_changed = False
                    
                    for movie in all_movies:
                        if processor.is_stop_requested(): break
                        
                        if movie.get('status') == 'missing':
                            release_date_str = movie.get('release_date')
                            if not release_date_str:
                                movies_to_keep.append(movie)
                                continue
                            try:
                                release_date = datetime.strptime(release_date_str.strip(), '%Y-%m-%d').date()
                            except (ValueError, TypeError):
                                movies_to_keep.append(movie)
                                continue

                            if release_date <= today:
                                current_quota = settings_db.get_subscription_quota()
                                if current_quota <= 0:
                                    quota_exhausted = True
                                    logger.warning("  ➜ 每日订阅配额已用尽，原生合集检查提前结束。")
                                    movies_to_keep.append(movie)
                                    break

                                if moviepilot_handler.subscribe_movie_to_moviepilot(movie, config_manager.APP_CONFIG):
                                    settings_db.decrement_subscription_quota()
                                    successfully_subscribed_items.append(f"电影《{movie['title']}》")
                                    movies_changed = True
                                    movie['status'] = 'subscribed'
                                movies_to_keep.append(movie)
                            else:
                                movies_to_keep.append(movie)
                        else:
                            movies_to_keep.append(movie)
                            
                    if movies_changed:
                        new_missing_json = json.dumps(movies_to_keep)
                        new_status = 'ok' if not any(m.get('status') == 'missing' for m in movies_to_keep) else 'has_missing'
                        cursor.execute("UPDATE collections_info SET missing_movies_json = %s, status = %s WHERE emby_collection_id = %s", (new_missing_json, new_status, collection['emby_collection_id']))

            # --- 2. 处理智能追剧 ---
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(30, "正在检查缺失的剧集...")
                sql_query = "SELECT * FROM watchlist WHERE status IN ('Watching', 'Paused') AND missing_info_json IS NOT NULL AND missing_info_json != '[]'"
                cursor.execute(sql_query)
                series_to_check = cursor.fetchall()
                
                for series in series_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    series_name = series['item_name']
                    series_tmdb_id = series['tmdb_id']
                    logger.info(f"    ├─ 正在检查: 《{series_name}》")
                    try:
                        missing_info = series['missing_info_json']
                        missing_seasons = missing_info.get('missing_seasons', [])
                        if not missing_seasons: continue
                        
                        seasons_to_keep = []
                        seasons_changed = False
                        for season in missing_seasons:
                            if processor.is_stop_requested() or quota_exhausted: break
                            
                            air_date_str = season.get('air_date')
                            if not air_date_str: seasons_to_keep.append(season); continue
                            try: season_date = datetime.strptime(air_date_str.strip(), '%Y-%m-%d').date()
                            except (ValueError, TypeError): seasons_to_keep.append(season); continue

                            if season_date <= today:
                                resubscribe_info = series.get('resubscribe_info_json') or {}
                                last_subscribed_str = resubscribe_info.get(str(season['season_number']))
                                if last_subscribed_str:
                                    try:
                                        cooldown_hours = 24 
                                        last_subscribed_time = datetime.fromisoformat(last_subscribed_str.replace('Z', '+00:00'))
                                        if datetime.now(timezone.utc) < last_subscribed_time + timedelta(hours=cooldown_hours):
                                            seasons_to_keep.append(season)
                                            continue
                                    except (ValueError, TypeError): pass
                                current_quota = settings_db.get_subscription_quota()
                                if current_quota <= 0:
                                    quota_exhausted = True; seasons_to_keep.append(season); break

                                # --- 检查剧集是否完结 ---
                                best_version_flag = _check_and_get_series_best_version_flag(
                                    series_tmdb_id=series_tmdb_id,
                                    tmdb_api_key=tmdb_api_key,
                                    season_number=season['season_number'],
                                    series_name=series_name
                                )
                                
                                success = moviepilot_handler.subscribe_series_to_moviepilot(
                                    series_info=dict(series), season_number=season['season_number'], 
                                    config=config_manager.APP_CONFIG, best_version=best_version_flag
                                )
                                
                                if success:
                                    settings_db.decrement_subscription_quota()
                                    cursor.execute("""
                                        UPDATE watchlist SET resubscribe_info_json = jsonb_set(
                                            COALESCE(resubscribe_info_json, '{}'::jsonb), %s, %s::jsonb, true)
                                        WHERE item_id = %s
                                    """, ([str(season['season_number'])], f'"{datetime.now(timezone.utc).isoformat()}"', series['item_id']))
                                    successfully_subscribed_items.append(f"《{series_name}》第 {season['season_number']} 季")
                                    seasons_changed = True
                                else:
                                    seasons_to_keep.append(season)
                            else:
                                seasons_to_keep.append(season)
                                
                        if seasons_changed:
                            missing_info['missing_seasons'] = seasons_to_keep
                            cursor.execute("UPDATE watchlist SET missing_info_json = %s WHERE item_id = %s", (json.dumps(missing_info), series['item_id']))
                    except Exception as e_series:
                        logger.error(f"  ➜ 【智能订阅-剧集】处理剧集 '{series_name}' 时出错: {e_series}")

            # --- 3. 处理自定义合集 ---
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(45, "正在检查自定义榜单合集...")
                sql_query_custom_collections = "SELECT * FROM custom_collections WHERE type = 'list' AND health_status = 'has_missing' AND generated_media_info_json IS NOT NULL AND generated_media_info_json != '[]'"
                cursor.execute(sql_query_custom_collections)
                custom_collections_to_check = cursor.fetchall()
                
                for collection in custom_collections_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    try:
                        all_media = collection['generated_media_info_json']
                        media_to_keep = []
                        media_changed = False
                        for media_item in all_media:
                            if processor.is_stop_requested(): break
                            
                            if media_item.get('status') == 'missing':
                                release_date_str = media_item.get('release_date')
                                if not release_date_str: media_to_keep.append(media_item); continue
                                try: release_date = datetime.strptime(release_date_str.strip(), '%Y-%m-%d').date()
                                except (ValueError, TypeError): media_to_keep.append(media_item); continue

                                if release_date <= today:
                                    current_quota = settings_db.get_subscription_quota()
                                    if current_quota <= 0:
                                        quota_exhausted = True; media_to_keep.append(media_item); break
                                        
                                    success = False
                                    media_title = media_item.get('title', '未知标题')
                                    media_tmdb_id = media_item.get('tmdb_id')
                                    authoritative_type = 'Series' if media_item.get('media_type') == 'Series' else 'Movie'

                                    if authoritative_type == 'Movie':
                                        success = moviepilot_handler.subscribe_movie_to_moviepilot(media_item, config_manager.APP_CONFIG)
                                    elif authoritative_type == 'Series':
                                        # --- 检查剧集是否完结 ---
                                        best_version_flag = _check_and_get_series_best_version_flag(
                                            series_tmdb_id=media_tmdb_id,
                                            tmdb_api_key=tmdb_api_key,
                                            series_name=media_title
                                        )
                                        series_info = { "item_name": media_title, "tmdb_id": media_tmdb_id }
                                        success = moviepilot_handler.subscribe_series_to_moviepilot(
                                            series_info, season_number=None, 
                                            config=config_manager.APP_CONFIG, best_version=best_version_flag
                                        )
                                    
                                    if success:
                                        settings_db.decrement_subscription_quota()
                                        successfully_subscribed_items.append(f"{authoritative_type}《{media_title}》")
                                        media_changed = True
                                        media_item['status'] = 'subscribed'
                                    media_to_keep.append(media_item)
                                else:
                                    media_to_keep.append(media_item)
                            else:
                                media_to_keep.append(media_item)
                                
                        if media_changed:
                            new_missing_json = json.dumps(media_to_keep, ensure_ascii=False)
                            new_missing_count = sum(1 for m in media_to_keep if m.get('status') == 'missing')
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            cursor.execute(
                                "UPDATE custom_collections SET generated_media_info_json = %s, health_status = %s, missing_count = %s WHERE id = %s", 
                                (new_missing_json, new_health_status, new_missing_count, collection['id'])
                            )
                    except Exception as e_coll:
                        logger.error(f"  ➜ 处理自定义合集 '{collection['name']}' 时发生错误: {e_coll}", exc_info=True)

            # --- 4. 处理演员订阅 ---
            if not processor.is_stop_requested() and not quota_exhausted:
                task_manager.update_status_from_thread(60, "正在检查演员订阅的缺失作品...")
                sql_query_actors = "SELECT * FROM tracked_actor_media WHERE status = 'MISSING'"
                cursor.execute(sql_query_actors)
                actor_media_to_check = cursor.fetchall()
                
                for media_item in actor_media_to_check:
                    if processor.is_stop_requested() or quota_exhausted: break
                    
                    release_date = media_item.get('release_date')
                    if not release_date or release_date > today: continue

                    current_quota = settings_db.get_subscription_quota()
                    if current_quota <= 0:
                        quota_exhausted = True; break
                    
                    success = False
                    media_title = media_item.get('title', '未知标题')
                    media_tmdb_id = media_item.get('tmdb_media_id')
                    
                    if media_item['media_type'] == 'Movie':
                        movie_info = {'title': media_title, 'tmdb_id': media_tmdb_id}
                        success = moviepilot_handler.subscribe_movie_to_moviepilot(movie_info, config_manager.APP_CONFIG)
                    elif media_item['media_type'] == 'Series':
                        # --- 检查剧集是否完结 ---
                        best_version_flag = _check_and_get_series_best_version_flag(
                            series_tmdb_id=media_tmdb_id,
                            tmdb_api_key=tmdb_api_key,
                            series_name=media_title
                        )
                        series_info = {"item_name": media_title, "tmdb_id": media_tmdb_id}
                        success = moviepilot_handler.subscribe_series_to_moviepilot(
                            series_info, season_number=None, 
                            config=config_manager.APP_CONFIG, best_version=best_version_flag
                        )
                    
                    if success:
                        settings_db.decrement_subscription_quota()
                        successfully_subscribed_items.append(f"演员作品《{media_title}》")
                        cursor.execute("UPDATE tracked_actor_media SET status = 'SUBSCRIBED' WHERE id = %s", (media_item['id'],))

            conn.commit()

        # --- 5. 处理媒体洗版 ---
        logger.info("--- 智能订阅缺失已完成，开始执行媒体洗版任务 ---")
        task_manager.update_status_from_thread(85, "缺失订阅完成，正在启动媒体洗版...") # 更新一个过渡状态
        
        # 直接调用洗版任务函数
        task_resubscribe_library(processor)

    except Exception as e:
        logger.error(f"智能订阅与洗版任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# ★★★ 媒体洗版任务 (基于精确API模型重构) ★★★
def build_resubscribe_payload(item_details: dict, rule: Optional[dict]) -> Optional[dict]:
    """
    - 根据PT站点的实际命名约定，优化了杜比视界Profile 8的正则表达式。
    - 现在，当订阅 Profile 8 时，会生成一个匹配 "dovi" 和 "hdr" 两个关键词同时存在的正则，
      这完美符合了现实世界中的文件命名习惯。
    """
    item_name = item_details.get('Name') or item_details.get('item_name')
    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb") or item_details.get('tmdb_id')
    item_type = item_details.get("Type") or item_details.get('item_type')

    if not all([item_name, tmdb_id, item_type]):
        logger.error(f"构建Payload失败：缺少核心媒体信息 {item_details}")
        return None

    payload = {
        "name": item_name, "tmdbid": int(tmdb_id),
        "type": "电影" if item_type == "Movie" else "电视剧",
        "best_version": 1
    }

    use_custom_subscribe = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_USE_CUSTOM_RESUBSCRIBE, False)
    if not use_custom_subscribe or not rule:
        log_reason = "自定义洗版未开启" if not use_custom_subscribe else "未匹配到规则"
        logger.info(f"  ➜ 《{item_name}》将使用全局洗版 ({log_reason})。")
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

def _item_needs_resubscribe(item_details: dict, config: dict, media_metadata: Optional[dict] = None) -> tuple[bool, str]:
    """
    【V12 - 功能完整·最终版】
    - 恢复了所有检查逻辑，包括：分辨率、质量、特效、音轨和字幕。
    - 此版本调用全局的、最新的 _get_standardized_effect 函数来做决策。
    """
    item_name = item_details.get('Name', '未知项目')
    logger.trace(f"  ➜ 开始为《{item_name}》检查洗版需求 ---")
    
    media_streams = item_details.get('MediaStreams', [])
    file_path = item_details.get('Path', '')
    file_name_lower = os.path.basename(file_path).lower() if file_path else ""

    reasons = []
    video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)

    CHINESE_LANG_CODES = {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn', 'yue'}
    CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}

    # 1. 分辨率检查
    try:
        if config.get("resubscribe_resolution_enabled"):
            if not video_stream:
                reasons.append("无视频流信息")
            else:
                # ★★★ 2. (修改) 使用等级系统进行判断 ★★★
                
                # 从配置中获取用户设置的宽度阈值 (例如 1920)
                threshold_width = int(config.get("resubscribe_resolution_threshold") or 1920)
                
                # 获取用户要求的等级
                required_tier, required_tier_name = _get_resolution_tier(threshold_width, 0)

                # 获取当前视频的实际等级
                current_width = int(video_stream.get('Width') or 0)
                current_height = int(video_stream.get('Height') or 0)
                current_tier, _ = _get_resolution_tier(current_width, current_height)

                # 只有当前等级严格小于要求等级时，才标记
                if current_tier < required_tier:
                    reasons.append(f"分辨率低于{required_tier_name}")

    except (ValueError, TypeError) as e:
        logger.warning(f"  ➜ [分辨率检查] 处理时发生类型错误: {e}")

    # 2. 质量检查
    try:
        if config.get("resubscribe_quality_enabled"):
            required_list = config.get("resubscribe_quality_include", [])
            if isinstance(required_list, list) and required_list:
                required_list_lower = [str(q).lower() for q in required_list]
                if not any(term in file_name_lower for term in required_list_lower):
                    reasons.append("质量不达标")
    except Exception as e:
        logger.warning(f"  ➜ [质量检查] 处理时发生未知错误: {e}")

    # 3. 特效检查 (调用最新的全局函数)
    try:
        if config.get("resubscribe_effect_enabled"):
            user_choices = config.get("resubscribe_effect_include", [])
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
                    except ValueError:
                        continue
                
                if highest_req_priority < 999:
                    current_effect = _get_standardized_effect(file_name_lower, video_stream)
                    current_priority = EFFECT_HIERARCHY.index(current_effect)
                    if current_priority > highest_req_priority:
                        reasons.append("特效不达标")
    except Exception as e:
        logger.warning(f"  ➜ [特效检查] 处理时发生未知错误: {e}")

    # 4. & 5. 音轨和字幕检查
    def _is_exempted_from_chinese_check(item_details: dict) -> bool:
        """
        【V5 - 原始标题终极版】
        - 采纳用户的绝佳建议，使用 TMDB 的 original_title 作为核心判断依据。
        - 这是目前最精准、最能抵抗本地化命名干扰的方案。
        - 豁免条件 (按优先级顺序检查):
          1. (最高) 媒体的制片国家/地区是华语区。
          2. (次高) 媒体的原始标题 (original_title) 是中文。
          3. 媒体已包含中文音轨。
          4. 媒体已包含中文字幕。
        """
        import re
        
        # 准备关键词和语言代码
        CHINESE_LANG_CODES = {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn', 'yue'}
        CHINESE_SPEAKING_REGIONS = {'中国', '中国大陆', '香港', '中国香港', '台湾', '中国台湾', '新加坡'}

        # 优先级 1: 检查制片国家/地区 (依然是最可靠的依据之一)
        if media_metadata and media_metadata.get('countries_json'):
            if not set(media_metadata['countries_json']).isdisjoint(CHINESE_SPEAKING_REGIONS):
                return True

        # ★★★ 优先级 2: 检查 TMDB 的原始标题 (核心修改) ★★★
        if media_metadata and (original_title := media_metadata.get('original_title')):
            # 匹配中文字符的 Unicode 范围
            chinese_chars = re.findall(r'[\u4e00-\u9fff]', original_title)
            # 如果原始标题中包含2个或以上的中文字符，就认定为华语内容
            if len(chinese_chars) >= 2:
                return True

        # 优先级 3: 检查现有音轨
        detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio', AUDIO_SUBTITLE_KEYWORD_MAP)
        if 'chi' in detected_audio_langs or 'yue' in detected_audio_langs:
            return True
            
        # 优先级 4: 检查现有字幕
        detected_subtitle_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle', AUDIO_SUBTITLE_KEYWORD_MAP)
        if 'chi' in detected_subtitle_langs or 'yue' in detected_subtitle_langs:
            return True

        # 注意：我们已经彻底移除了对本地显示名称 (item_details['Name']) 的检查，因为它会造成误判
        return False

    is_exempted = _is_exempted_from_chinese_check(item_details)
    
    try:
        if config.get("resubscribe_audio_enabled") and not is_exempted:
            required_langs = set(config.get("resubscribe_audio_missing_languages", []))
            if 'chi' in required_langs or 'yue' in required_langs:
                # ★★★ 让音轨判断也使用智能函数 ★★★
                detected_audio_langs = _get_detected_languages_from_streams(
                    media_streams, 'Audio', AUDIO_SUBTITLE_KEYWORD_MAP
                )
                if 'chi' not in detected_audio_langs and 'yue' not in detected_audio_langs:
                    reasons.append("缺中文音轨")
    except Exception as e:
        logger.warning(f"  ➜ [音轨检查] 处理时发生未知错误: {e}")

    try:
        if config.get("resubscribe_subtitle_enabled") and not is_exempted:
            required_langs = set(config.get("resubscribe_subtitle_missing_languages", []))
            if 'chi' in required_langs:
                detected_subtitle_langs = _get_detected_languages_from_streams(
                    media_streams, 'Subtitle', AUDIO_SUBTITLE_KEYWORD_MAP
                )
                
                # ★★★ 新增的核心逻辑：外挂字幕豁免规则 ★★★
                # 如果通过常规方式没找到中字，则检查是否存在外挂字幕
                if 'chi' not in detected_subtitle_langs and 'yue' not in detected_subtitle_langs:
                    if any(s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
                        # 如果存在外挂字幕，就默认它是中文，并加入到检测结果中
                        detected_subtitle_langs.add('chi')

                # 最终检查
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

# ★★★ 精准批量订阅的后台任务 ★★★
def task_resubscribe_batch(processor, item_ids: List[str]):
    """【精准批量版】后台任务：只订阅列表中指定的一批媒体项。"""
    task_name = "批量媒体洗版"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_subscribe = []
    
    try:
        # 1. 从数据库中精确获取需要处理的项目
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)"
            cursor.execute(sql, (item_ids,))
            items_to_subscribe = cursor.fetchall()

        total_to_process = len(items_to_subscribe)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有需要订阅的项。")
            return

        logger.info(f"  ➜ 精准任务：共找到 {total_to_process} 个项目待处理，将开始订阅...")
        
        # 2. 后续的订阅、删除、配额检查逻辑和“一键洗版”完全一致
        all_rules = resubscribe_db.get_all_resubscribe_rules()
        config = processor.config
        delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))
        resubscribed_count = 0
        deleted_count = 0

        for i, item in enumerate(items_to_subscribe):
            if processor.is_stop_requested():
                logger.info("  ➜ 任务被用户中止。")
                break
            
            current_quota = settings_db.get_subscription_quota()
            if current_quota <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            item_id = item.get('item_id')
            item_name = item.get('item_name')
            task_manager.update_status_from_thread(
                int((i / total_to_process) * 100), 
                f"({i+1}/{total_to_process}) [配额:{current_quota}] 正在订阅: {item_name}"
            )

            # 1. 获取当前项目匹配的规则
            matched_rule_id = item.get('matched_rule_id')
            rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

            # 2. 让“智能荷官”配牌 (item 字典本身就包含了需要的信息)
            payload = build_resubscribe_payload(item, rule)

            if not payload:
                logger.warning(f"为《{item.get('item_name')}》构建订阅Payload失败，已跳过。")
                continue # 跳过这个项目，继续下一个

            # 3. 发送订阅
            success = moviepilot_handler.subscribe_with_custom_payload(payload, config)
            
            if success:
                settings_db.decrement_subscription_quota()
                resubscribed_count += 1
                
                matched_rule_id = item.get('matched_rule_id')
                rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

                if rule and rule.get('delete_after_resubscribe'):
                    delete_success = emby_handler.delete_item(
                        item_id=item_id, emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                    )
                    if delete_success:
                        resubscribe_db.delete_resubscribe_cache_item(item_id)
                        deleted_count += 1
                    else:
                        resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                else:
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                
                if i < total_to_process - 1: time.sleep(delay)

        final_message = f"批量任务完成！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# --- 一键洗版 ---
def task_resubscribe_library(processor):
    """ 后台任务：订阅成功后，根据规则删除或更新缓存。"""
    task_name = "媒体洗版"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    config = processor.config
    
    try:
        all_rules = resubscribe_db.get_all_resubscribe_rules()
        delay = float(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5))

        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resubscribe_cache WHERE status = 'needed'")
            items_to_resubscribe = cursor.fetchall()

        total_needed = len(items_to_resubscribe)
        if total_needed == 0:
            task_manager.update_status_from_thread(100, "任务完成：没有发现需要洗版的项目。")
            return

        logger.info(f"  ➜ 共找到 {total_needed} 个项目待处理，将开始订阅...")
        resubscribed_count = 0
        deleted_count = 0

        for i, item in enumerate(items_to_resubscribe):
            if processor.is_stop_requested(): break
            
            current_quota = settings_db.get_subscription_quota()
            if current_quota <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            item_name = item.get('item_name')
            item_id = item.get('item_id')
            task_manager.update_status_from_thread(
                int((i / total_needed) * 100), 
                f"({i+1}/{total_needed}) [配额:{current_quota}] 正在订阅: {item_name}"
            )

            # 1. 获取当前项目匹配的规则
            matched_rule_id = item.get('matched_rule_id')
            rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

            # 2. 让“智能荷官”配牌 (item 字典本身就包含了需要的信息)
            payload = build_resubscribe_payload(item, rule)

            if not payload:
                logger.warning(f"为《{item.get('item_name')}》构建订阅Payload失败，已跳过。")
                continue # 跳过这个项目，继续下一个

            # 3. 发送订阅
            success = moviepilot_handler.subscribe_with_custom_payload(payload, config)
            
            if success:
                settings_db.decrement_subscription_quota()
                resubscribed_count += 1
                
                matched_rule_id = item.get('matched_rule_id')
                rule = next((r for r in all_rules if r['id'] == matched_rule_id), None) if matched_rule_id else None

                # --- ★★★ 核心逻辑改造：根据规则决定是“删除”还是“更新” ★★★ ---
                if rule and rule.get('delete_after_resubscribe'):
                    logger.warning(f"  ➜ 规则 '{rule['name']}' 要求删除源文件，正在删除 Emby 项目: {item_name} (ID: {item_id})")
                    delete_success = emby_handler.delete_item(
                        item_id=item_id, emby_server_url=processor.emby_url,
                        emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                    )
                    if delete_success:
                        # 如果 Emby 项删除成功，就从我们的缓存里也删除
                        resubscribe_db.delete_resubscribe_cache_item(item_id)
                        deleted_count += 1
                    else:
                        # 如果 Emby 项删除失败，那我们只更新状态，让用户知道订阅成功了但删除失败
                        resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                else:
                    # 如果没有删除规则，就正常更新状态
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                
                if i < total_needed - 1: time.sleep(delay)

        final_message = f"任务完成！成功提交 {resubscribed_count} 个订阅，并根据规则删除了 {deleted_count} 个媒体项。"
        if not processor.is_stop_requested() and current_quota <= 0:
             final_message = f"配额用尽！成功提交 {resubscribed_count} 个订阅，删除 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 精准批量删除的后台任务 ★★★
def task_delete_batch(processor, item_ids: List[str]):
    """【精准批量版】后台任务：只删除列表中指定的一批媒体项。"""
    task_name = "批量删除媒体"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (精准模式) ---")
    
    items_to_delete = []
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT * FROM resubscribe_cache WHERE item_id = ANY(%s)"
            cursor.execute(sql, (item_ids,))
            items_to_delete = cursor.fetchall()

        total_to_process = len(items_to_delete)
        if total_to_process == 0:
            task_manager.update_status_from_thread(100, "任务完成：选中的项目中没有可删除的项。")
            return

        logger.info(f"  ➜ 精准删除：共找到 {total_to_process} 个项目待处理...")
        deleted_count = 0

        for i, item in enumerate(items_to_delete):
            if processor.is_stop_requested(): break
            
            item_id = item.get('item_id')
            item_name = item.get('item_name')
            task_manager.update_status_from_thread(
                int((i / total_to_process) * 100), 
                f"({i+1}/{total_to_process}) 正在删除: {item_name}"
            )
            
            delete_success = emby_handler.delete_item(
                item_id=item_id, emby_server_url=processor.emby_url,
                emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
            )
            if delete_success:
                resubscribe_db.delete_resubscribe_cache_item(item_id)
                deleted_count += 1
            
            time.sleep(0.5) # 避免请求过快

        final_message = f"批量删除任务完成！成功删除了 {deleted_count} 个媒体项。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

def task_update_resubscribe_cache(processor):
    """
    【V-Final Pro - 架构恢复最终版】
    - 恢复了简洁的函数结构，所有业务逻辑都通过调用正确的全局辅助函数完成。
    """
    task_name = "刷新洗版状态 (架构恢复最终版)"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        task_manager.update_status_from_thread(0, "正在加载规则并确定扫描范围...")
        all_enabled_rules = [rule for rule in resubscribe_db.get_all_resubscribe_rules() if rule.get('enabled')]
        library_ids_to_scan = set()
        for rule in all_enabled_rules:
            target_libs = rule.get('target_library_ids')
            if isinstance(target_libs, list):
                library_ids_to_scan.update(target_libs)
        libs_to_process_ids = list(library_ids_to_scan)

        if not libs_to_process_ids:
            task_manager.update_status_from_thread(100, "任务跳过：没有规则指定媒体库")
            return
        
        task_manager.update_status_from_thread(10, f"正在从 {len(libs_to_process_ids)} 个目标库中获取项目...")
        all_items_base_info = emby_handler.get_emby_library_items(
            base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id,
            media_type_filter="Movie,Series", library_ids=libs_to_process_ids,
            fields="ProviderIds,Name,Type,ChildCount,_SourceLibraryId"
        ) or []
        
        current_db_status_map = {item['item_id']: item['status'] for item in resubscribe_db.get_all_resubscribe_cache()}
        total = len(all_items_base_info)
        if total == 0:
            task_manager.update_status_from_thread(100, "任务完成：在目标媒体库中未找到任何项目。")
            return

        logger.info(f"  ➜ 将为 {total} 个媒体项目获取详情并按规则检查洗版状态...")
        cache_update_batch = []
        processed_count = 0
        library_to_rule_map = {}
        for rule in reversed(all_enabled_rules):
            target_libs = rule.get('target_library_ids')
            if isinstance(target_libs, list):
                for lib_id in target_libs:
                    library_to_rule_map[lib_id] = rule

        def process_item_for_cache(item_base_info):
            item_id = item_base_info.get('Id')
            item_name = item_base_info.get('Name')
            source_lib_id = item_base_info.get('_SourceLibraryId')

            if current_db_status_map.get(item_id) == 'ignored': return None
        
            try:
                applicable_rule = library_to_rule_map.get(source_lib_id)
                if not applicable_rule:
                    return { "item_id": item_id, "status": 'ok', "reason": "无匹配规则" }
                
                item_details = emby_handler.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                if not item_details: return None
                
                tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                media_metadata = collection_db.get_media_metadata_by_tmdb_id(tmdb_id) if tmdb_id else None
                item_type = item_details.get('Type')
                if item_type == 'Series' and item_details.get('ChildCount', 0) > 0:
                    # 步骤 1: 仅获取第一集的 ID，这是高效且轻量的
                    first_episode_list = emby_handler.get_series_children(
                        series_id=item_id,
                        base_url=processor.emby_url,
                        api_key=processor.emby_api_key,
                        user_id=processor.emby_user_id,
                        include_item_types="Episode",
                        fields="Id"  # 只需要 ID
                    )
                    
                    # 步骤 2: 如果找到了分集，就用它的 ID 去获取完整详情
                    if first_episode_list:
                        first_episode_id = first_episode_list[0].get('Id')
                        if first_episode_id:
                            # 这个调用会返回包含完整 MediaStreams 和 Path 的详细信息
                            first_episode_details = emby_handler.get_emby_item_details(
                                first_episode_id, 
                                processor.emby_url, 
                                processor.emby_api_key, 
                                processor.emby_user_id
                            )
                            
                            # 步骤 3: 用获取到的完整详情来代表整个剧集的质量
                            if first_episode_details:
                                item_details['MediaStreams'] = first_episode_details.get('MediaStreams', [])
                                item_details['Path'] = first_episode_details.get('Path', '')
                
                needs_resubscribe, reason = _item_needs_resubscribe(item_details, applicable_rule, media_metadata)
                old_status = current_db_status_map.get(item_id)
                new_status = 'ok' if not needs_resubscribe else ('subscribed' if old_status == 'subscribed' else 'needed')
                
                media_streams = item_details.get('MediaStreams', [])
                video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)
                file_name_lower = os.path.basename(item_details.get('Path', '')).lower()
                
                raw_effect_tag = _get_standardized_effect(file_name_lower, video_stream)
                
                EFFECT_DISPLAY_MAP = {'dovi_p8': 'DoVi P8', 'dovi_p7': 'DoVi P7', 'dovi_p5': 'DoVi P5', 'dovi_other': 'DoVi (Other)', 'hdr10+': 'HDR10+', 'hdr': 'HDR', 'sdr': 'SDR'}
                effect_str = EFFECT_DISPLAY_MAP.get(raw_effect_tag, raw_effect_tag.upper())

                resolution_str = "未知"
                if video_stream:
                    # ★★★ 3. (修改) 使用等级系统生成显示名称 ★★★
                    width = int(video_stream.get('Width') or 0)
                    height = int(video_stream.get('Height') or 0)
                    _ , resolution_str = _get_resolution_tier(width, height)
                
                quality_str = _extract_quality_tag_from_filename(file_name_lower, video_stream)
                
                detected_audio_langs = _get_detected_languages_from_streams(
                    media_streams, 'Audio', AUDIO_SUBTITLE_KEYWORD_MAP
                )

                # 定义显示名称的映射
                AUDIO_DISPLAY_MAP = {'chi': '国语', 'yue': '粤语', 'eng': '英语', 'jpn': '日语'}

                # 生成显示字符串
                display_audio_list = sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs])
                audio_str = ', '.join(display_audio_list) or '无'

                # 将原始检测结果也存入数据库
                audio_langs_raw = list(detected_audio_langs)

                detected_sub_langs = _get_detected_languages_from_streams(
                    media_streams, 'Subtitle', AUDIO_SUBTITLE_KEYWORD_MAP
                )

                # ★★★ 新增的核心逻辑：外挂字幕显示规则 ★★★
                if 'chi' not in detected_sub_langs and 'yue' not in detected_sub_langs:
                    if any(s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
                        detected_sub_langs.add('chi')

                SUB_DISPLAY_MAP = {'chi': '中字', 'yue': '粤字', 'eng': '英文', 'jpn': '日文'}
                display_subtitle_list = sorted([SUB_DISPLAY_MAP.get(lang, lang) for lang in detected_sub_langs])
                subtitle_str = ', '.join(display_subtitle_list) or '无'
                subtitle_langs_raw = list(detected_sub_langs)
                
                return {
                    "item_id": item_id, "item_name": item_details.get('Name'), "tmdb_id": tmdb_id, "item_type": item_type, "status": new_status, 
                    "reason": reason, "resolution_display": resolution_str, "quality_display": quality_str, "effect_display": effect_str,
                    "audio_display": audio_str, "subtitle_display": subtitle_str,
                    "audio_languages_raw": audio_langs_raw, "subtitle_languages_raw": subtitle_langs_raw,
                    "matched_rule_id": applicable_rule.get('id'), "matched_rule_name": applicable_rule.get('name'), "source_library_id": source_lib_id
                }
            except Exception as e:
                logger.error(f"  ➜ 处理项目 '{item_name}' (ID: {item_id}) 时线程内发生错误: {e}", exc_info=True)
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(process_item_for_cache, item): item for item in all_items_base_info}
            for future in as_completed(future_to_item):
                if processor.is_stop_requested(): break
                result = future.result()
                if result: cache_update_batch.append(result)
                processed_count += 1
                progress = int(20 + (processed_count / (total or 1)) * 80)
                task_manager.update_status_from_thread(progress, f"({processed_count}/{total}) 正在分析: {future_to_item[future].get('Name')}")

        if cache_update_batch:
            logger.info(f"  ➜ 分析完成，正在将 {len(cache_update_batch)} 条记录写入缓存表...")
            resubscribe_db.upsert_resubscribe_cache_batch(cache_update_batch)
            
            task_manager.update_status_from_thread(99, "缓存写入完成，即将刷新...")
            time.sleep(1) # 给前端一点反应时间，确保信号被接收

        final_message = "媒体洗版状态刷新完成！"
        if processor.is_stop_requested(): final_message = "任务已中止。"
        task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")