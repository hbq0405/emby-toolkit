# tasks/subscriptions.py
# 智能订阅模块
import time
import re
from datetime import datetime, timedelta
import logging
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed 

# 导入需要的底层模块和共享实例
import config_manager
import constants
import handler.tmdb as tmdb
import handler.moviepilot as moviepilot
import task_manager
from handler import telegram
from database import settings_db, request_db, user_db, media_db, watchlist_db
from .helpers import is_movie_subscribable, check_series_completion, parse_series_title_and_season, should_mark_as_pending
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive

logger = logging.getLogger(__name__)

EFFECT_KEYWORD_MAP = {
    "杜比视界": ["dolby vision", "dovi"],
    "HDR": ["hdr", "hdr10", "hdr10+", "hlg"]
}

AUDIO_SUBTITLE_KEYWORD_MAP = {
    # --- 音轨关键词 ---
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    "kor": ["Korean", "KOR", "韩语"], 
    
    # --- 字幕关键词 ---
    # 注意：resubscribe.py 会通过 "sub_" + 语言代码 来查找这里
    "sub_chi": ["CHS", "CHT", "中字", "简中", "繁中", "简", "繁", "Chinese"],
    "sub_eng": ["ENG", "英字", "English"],
    "sub_jpn": ["JPN", "日字", "日文", "Japanese"], 
    "sub_kor": ["KOR", "韩字", "韩文", "Korean"],   
    "sub_yue": ["CHT", "繁中", "繁体", "Cantonese"], 
}
# 解析影巢返回的体积字符串
def _parse_size_to_gb(size_str):
    """将影巢返回的体积字符串 (如 '58.3 GB', '1.77TB', '50G') 转换为 GB 浮点数"""
    if not size_str:
        return 0.0
    
    # 确保转换为字符串，去除空格并转大写
    size_str = str(size_str).upper().replace(' ', '')
    
    # 匹配数字(含小数点)和后面的字母单位
    match = re.search(r'([\d\.]+)([A-Z]*)', size_str)
    if not match:
        return 0.0
        
    try:
        val = float(match.group(1))
    except ValueError:
        return 0.0
        
    unit = match.group(2)
    
    # 兼容 G, GB, GiB 等各种写法
    if 'T' in unit: return val * 1024
    if 'G' in unit: return val
    if 'M' in unit: return val / 1024
    if 'K' in unit: return val / (1024 * 1024)
    
    return 0.0

# ★★★ 内部辅助函数：处理整部剧集的精细化订阅 ★★★
# ==============================================================================
def _subscribe_full_series_with_logic(tmdb_id: int, series_name: str, config: Dict, tmdb_api_key: str, source: Dict = None) -> bool:
    """
    处理整部剧集的订阅：
    1. 查询 TMDb 获取所有季。
    2. 遍历所有季。
    3. 检查是否未上映 -> 设为 PENDING_RELEASE。
    5. 检查是否完结/配置开启 -> 决定 best_version。
    6. 逐季提交订阅并更新本地数据库。
    """
    watchlist_config = settings_db.get_setting('watchlist_config') or {}
    tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)

    try:
        # 1. 获取剧集详情
        series_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
        if not series_details:
            logger.error(f"  ➜ 无法获取剧集 ID {tmdb_id} 的详情，跳过订阅。")
            return False

        # 规范化名称
        final_series_name = series_details.get('name', series_name)
        # ★★★ 新增：获取剧集海报作为兜底 ★★★
        series_poster = series_details.get('poster_path')
        
        # 2. 获取所有有效季 (Season > 0)
        seasons = series_details.get('seasons', [])
        valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])
        
        if not valid_seasons:
            logger.warning(f"  ➜ 剧集《{final_series_name}》没有有效的季信息，尝试直接订阅整剧。")
            # 兜底：直接订阅 ID
            return moviepilot.subscribe_with_custom_payload({"name": final_series_name, "tmdbid": tmdb_id, "type": "电视剧"}, config)

        # 3. 确定最后一季的季号
        last_season_num = valid_seasons[-1]['season_number']
        any_success = False

        # ★★★ 关键步骤 1：先激活父剧集 ★★★
        watchlist_db.add_item_to_watchlist(str(tmdb_id), final_series_name)

        logger.info(f"  ➜ 正在处理《{final_series_name}》的 {len(valid_seasons)} 个季 (S{valid_seasons[0]['season_number']} - S{last_season_num})...")

        # 4. 遍历逐个订阅
        for season in valid_seasons:
            s_num = season['season_number']
            s_id = season.get('id') # 季的 TMDb ID
            air_date_str = season.get('air_date')
            
            # 优先使用季海报，没有则使用剧集海报
            season_poster = season.get('poster_path')
            # 如果概要中缺失日期，强制获取季详情 
            if not air_date_str:
                logger.debug(f"  ➜ S{s_num} 概要信息缺失发行日期，正在获取详细信息...")
                season_details_deep = tmdb.get_tv_season_details(tmdb_id, s_num, tmdb_api_key)
                
                if season_details_deep:
                    # 1. 尝试直接获取季日期
                    air_date_str = season_details_deep.get('air_date')
                    
                    # 2. ★★★ 新增：如果季日期仍为空，遍历分集找最早的日期 ★★★
                    if not air_date_str and 'episodes' in season_details_deep:
                        episodes = season_details_deep['episodes']
                        # 提取所有有效的 air_date
                        valid_dates = [e.get('air_date') for e in episodes if e.get('air_date')]
                        if valid_dates:
                            # 取最早的一个日期
                            air_date_str = min(valid_dates)
                            logger.debug(f"  ➜ 从分集数据中推断出 S{s_num} 发行日期: {air_date_str}")

                    # 补全海报和简介
                    if not season_poster: season_poster = season_details_deep.get('poster_path')
                    if not season.get('overview'): season['overview'] = season_details_deep.get('overview')
            final_poster = season_poster if season_poster else series_poster

            # ==============================================================
            # 逻辑 A: 检查是否未上映 (Pending Release)
            # ==============================================================
            is_future_season = False
            # 如果有日期且大于今天，或者干脆没有日期(视为待定/未上映)，都标记为未上映
            if air_date_str:
                try:
                    air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
                    if air_date > datetime.now().date():
                        is_future_season = True
                except ValueError:
                    pass
            else:
                # 如果深挖了详情还是没有日期，通常意味着 TBD (To Be Determined)，也应视为未上映，防止错误订阅
                is_future_season = True
                logger.info(f"  ➜ 季《{final_series_name}》S{s_num} 无发行日期，视为 '待上映'。")
            
            if is_future_season:
                logger.info(f"  ➜ 《{final_series_name}》第 {s_num} 季 尚未播出 ({air_date_str})，已加入待上映列表。")
                
                media_info = {
                    'tmdb_id': str(s_id) if s_id else f"{tmdb_id}_S{s_num}",
                    'title': season.get('name', f"第 {s_num} 季"),
                    'season_number': s_num,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'release_date': air_date_str,
                    'poster_path': final_poster, # 使用处理后的海报
                    'backdrop_path': season.get('backdrop_path') or series_details.get('backdrop_path'),
                    'overview': season.get('overview')
                }
                
                request_db.set_media_status_pending_release(
                    tmdb_ids=media_info['tmdb_id'],
                    item_type='Season',
                    source=source,
                    media_info_list=[media_info]
                    
                )
                any_success = True
                continue 

            # ==============================================================
            # 逻辑 B: 自动待定检查 (Auto Pending) 
            # ==============================================================
            # 针对刚上映但集数信息不全的剧集，我们需要将其在 MP 中标记为 'P' (待定)
            # 并设置一个虚假的总集数，防止 MP 下载完现有集数后直接完结订阅。
            is_pending_logic, fake_total_episodes = should_mark_as_pending(tmdb_id, s_num, tmdb_api_key)
            
            if is_pending_logic:
                logger.info(f"  ➜ 季《{final_series_name}》S{s_num} 满足自动待定条件，将执行 [订阅 -> 转待定] 流程。")

            # ==============================================================
            # 逻辑 C: 准备订阅 Payload
            # ==============================================================
            mp_payload = {
                "name": final_series_name,
                "tmdbid": tmdb_id,
                "type": "电视剧",
                "season": s_num,
                "best_version": 0
            }
            
            # ==============================================================
            # 逻辑 D: 决定 Best Version (洗版/完结检测)
            # ==============================================================
            # 只有在【不满足】待定条件时，才去检查完结状态。
            # 如果已经是待定状态，说明肯定没完结，不需要检查，也不应该开启洗版。
            is_completed = False # ★★★ 新增一个标志位
            if not is_pending_logic:
                if check_series_completion(tmdb_id, tmdb_api_key, season_number=s_num, series_name=final_series_name):
                    mp_payload["best_version"] = 1
                    is_completed = True # ★★★ 标记为已完结
                    logger.info(f"  ➜ S{s_num} 已完结，启用洗版模式订阅。")
                else:
                    logger.info(f"  ➜ S{s_num} 未完结，使用追更模式订阅。")
            else:
                logger.info(f"  ➜ S{s_num} 处于待定模式，使用追更模式订阅。")

            # ==============================================================
            # 逻辑 E: 提交订阅 & 后置状态修正
            # ==============================================================
            # ★★★ 修改开始：拦截 TG 频道追更 ★★★
            if tg_channel_tracking and not is_completed:
                logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{s_num} 的订阅。")
                mp_submit_success = True # 模拟成功，以便更新本地数据库状态为已订阅
                is_pending_logic = False # 既然没提交给MP，就不需要去MP改待定状态了
            else:
                mp_submit_success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            if mp_submit_success:
                any_success = True
                
                # ★★★ 核心修复：如果是待定逻辑，订阅成功后立即修改 MP 状态 ★★★
                if is_pending_logic:
                    logger.info(f"  ➜ [后置操作] 正在将 S{s_num} 的状态修改为 'P' (待定)，并将总集数修正为 {fake_total_episodes}...")
                    # 调用 moviepilot.py 中的 update_subscription_status
                    # 注意：这里传入 fake_total_episodes 以防止 MP 自动完结
                    mp_update_success = moviepilot.update_subscription_status(
                        tmdb_id=tmdb_id,
                        season=s_num,
                        status='P', # P = Pending
                        config=config,
                        total_episodes=fake_total_episodes
                    )
                    if mp_update_success:
                        logger.info(f"  ➜ S{s_num} 已成功转为待定状态。")
                    else:
                        logger.warning(f"  ➜ S{s_num} 订阅成功，但转待定状态失败。")

                # 订阅成功后，更新本地数据库状态为 SUBSCRIBED
                # (即使 MP 是 Pending，对于本地请求队列来说，它也算是“已处理/已订阅”)
                target_s_id = str(s_id) if s_id else f"{tmdb_id}_S{s_num}"
                media_info = {
                    'tmdb_id': target_s_id,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'season_number': s_num,
                    'title': season.get('name'),
                    'poster_path': final_poster,
                    'backdrop_path': season.get('backdrop_path') or series_details.get('backdrop_path'),
                    'release_date': air_date_str
                }
                request_db.set_media_status_subscribed(
                    tmdb_ids=[target_s_id],
                    item_type='Season',
                    source=source,
                    media_info_list=[media_info]
                )
                    
        return any_success

    except Exception as e:
        logger.error(f"处理整剧订阅逻辑时出错: {e}", exc_info=True)
        return False

# ★★★ 手动动订阅任务 ★★★
def task_manual_subscribe_batch(processor, subscribe_requests: List[Dict]):
    """
    手动订阅任务
    """
    total_items = len(subscribe_requests)
    task_name = f"手动订阅 {total_items} 个项目"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在准备手动订阅任务...")

    if not subscribe_requests:
        task_manager.update_status_from_thread(100, "任务完成：没有需要处理的项目。")
        return

    try:
        config = config_manager.APP_CONFIG
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        watchlist_config = settings_db.get_setting('watchlist_config') or {}
        tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)
        
        processed_count = 0

        for i, req in enumerate(subscribe_requests):
            tmdb_id = req.get('tmdb_id')
            item_type = req.get('item_type')
            item_title_for_log = req.get('title', f"ID: {tmdb_id}")
            season_number = req.get('season_number')
            user_id = req.get('user_id')

            # 构建来源信息 (用于后续通知)
            source = None
            if user_id:
                source = {'type': 'user_request', 'user_id': user_id}

            if not tmdb_id or not item_type:
                logger.warning(f"跳过一个无效的订阅请求: {req}")
                continue

            task_manager.update_status_from_thread(
                int((i / total_items) * 100),
                f"({i+1}/{total_items}) 正在处理: {item_title_for_log}"
            )

            # 检查配额
            if settings_db.get_subscription_quota() <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            success = False
            
            # ==================================================================
            # 逻辑分支 1: 剧集 / 季
            # ==================================================================
            if item_type == 'Series' or item_type == 'Season':
                # 1. ★★★ 核心修复：ID 和 季号 修正 ★★★
                if item_type == 'Season':
                    # 尝试从请求中获取父剧集 ID (统一订阅页面传过来的是 series_tmdb_id 或 parent_series_tmdb_id)
                    parent_id = req.get('series_tmdb_id') or req.get('parent_series_tmdb_id')
                    
                    # 如果请求里没有，去数据库查 (说明传入的 tmdb_id 可能是季 ID)
                    if not parent_id:
                        season_info = media_db.get_media_details(str(tmdb_id), 'Season')
                        if season_info:
                            parent_id = season_info.get('parent_series_tmdb_id')
                            if season_number is None:
                                season_number = season_info.get('season_number')
                    
                    # 如果找到了父剧集 ID，且与当前 tmdb_id 不同，说明传入的是季 ID
                    # 必须将其替换为父剧集 ID，因为后续的 check_series_completion 和 MP 订阅都需要剧集 ID
                    if parent_id and str(parent_id) != str(tmdb_id):
                        logger.debug(f"  ➜ [ID修正] 将季 ID {tmdb_id} 替换为父剧集 ID {parent_id}")
                        tmdb_id = parent_id
                
                # 2. 处理单季订阅 (最常见情况)
                if season_number is not None:
                    series_name = media_db.get_series_title_by_tmdb_id(str(tmdb_id))
                    if not series_name: series_name = item_title_for_log

                    mp_payload = {
                        "name": series_name,
                        "tmdbid": int(tmdb_id),
                        "type": "电视剧",
                        "season": int(season_number)
                    }

                    # B. ★★★ 核心：完结状态检查 ★★★
                    is_completed = check_series_completion(
                        int(tmdb_id), 
                        tmdb_api_key, 
                        season_number=season_number, 
                        series_name=series_name
                    )

                    if is_completed:
                        mp_payload["best_version"] = 1
                        logger.info(f"  ➜ [手动交互] S{season_number} 已完结，启用洗版模式 (best_version=1)。")
                    else:
                        logger.info(f"  ➜ [手动交互] S{season_number} 尚未完结 (连载中)，使用普通追更模式。")
                    
                    # ★★★ 拦截 TG 频道追更 ★★★
                    if tg_channel_tracking and not is_completed:
                        logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{season_number} 的订阅。")
                        success = True # 模拟成功
                    else:
                        success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

                # 3. 处理整剧订阅 (Series)
                elif item_type == 'Series':
                    # 调用整剧处理逻辑 (内部会遍历所有季)
                    success = _subscribe_full_series_with_logic(
                        tmdb_id=int(tmdb_id),
                        series_name=item_title_for_log,
                        config=config,
                        tmdb_api_key=tmdb_api_key,
                        source=source
                    )
                    if success:
                        request_db.set_media_status_none(str(tmdb_id), 'Series')
                
                else:
                    logger.error(f"  ➜ 订阅失败：季《{item_title_for_log}》缺少季号信息。")
                    continue
            
            # ==================================================================
            # 逻辑分支 2: 电影
            # ==================================================================
            elif item_type == 'Movie':
                if not is_movie_subscribable(int(tmdb_id), tmdb_api_key, config): 
                    logger.warning(f"  ➜ 电影《{item_title_for_log}》不满足发行日期条件，跳过订阅。")
                    continue
                
                mp_payload = {"name": item_title_for_log, "tmdbid": int(tmdb_id), "type": "电影"}
                # 电影手动订阅，通常意味着用户现在就想看，且电影一般没有“连载”概念
                # 可以默认开启 best_version=1 以获取更好质量，或者保持默认 0
                # 这里保持默认 0 比较稳妥，除非用户明确是洗版操作，但为了简化，这里不设 best_version
                success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            # ==================================================================
            # 结果处理
            # ==================================================================
            if success:
                logger.info(f"  ➜ 《{item_title_for_log}》订阅成功！")
                settings_db.decrement_subscription_quota()
                
                # 更新数据库状态 (Series 类型在 _subscribe_full_series_with_logic 里处理了)
                if item_type != 'Series':
                    # 如果是季，需要构建正确的 ID (例如 tmdbid_S1)
                    # 这里的 tmdb_id 已经被修正为 Series ID，所以需要重新构建 Season ID
                    target_id_for_update = str(tmdb_id)
                    if item_type == 'Season' and season_number is not None:
                         # 尝试查询真实的季 ID，查不到则用拼接 ID
                         real_season_id = request_db.get_season_tmdb_id(str(tmdb_id), season_number)
                         target_id_for_update = real_season_id if real_season_id else f"{tmdb_id}_S{season_number}"

                    request_db.set_media_status_subscribed(
                        tmdb_ids=[target_id_for_update],
                        item_type=item_type, 
                    )

                processed_count += 1
            else:
                logger.error(f"  ➜ 订阅《{item_title_for_log}》失败，请检查 MoviePilot 日志。")
        
        final_message = f"  ➜ 手动订阅任务完成，成功处理 {processed_count}/{total_items} 个项目。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务执行完毕 ---")

    except Exception as e:
        logger.error(f"  ➜ {task_name} 任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# ★★★ 自动订阅任务 ★★★
def task_auto_subscribe(processor):
    """
    【V2 - 统一订阅处理器】
    """
    task_name = "统一订阅处理"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在加载订阅策略...")
    config = config_manager.APP_CONFIG
    
    # 1. 加载策略配置 (优先从数据库读取，如果没有则使用默认值)
    strategy_config = settings_db.get_setting('subscription_strategy_config') or {}
    
    # 默认策略参数
    movie_protection_days = int(strategy_config.get('movie_protection_days', 180))    # 默认半年新片保护
    movie_search_window = int(strategy_config.get('movie_search_window_days', 1))     # 默认搜索1天
    movie_pause_days = int(strategy_config.get('movie_pause_days', 7))                # 默认暂停7天
    timeout_revive_days = int(strategy_config.get('timeout_revive_days', 0))          # 默认不复活超时订阅
    
    # 2. 读取请求延迟配置
    try:
        request_delay = int(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 0))
    except:
        request_delay = 0

    try:
        # ======================================================================
        # 阶段 1 - 清理超时订阅 
        # ======================================================================
        if movie_search_window > 0:
            logger.info(f"  ➜ 正在检查超过 {movie_search_window} 天仍未入库的订阅...")
            task_manager.update_status_from_thread(2, "正在清理超时订阅...")
            
            stale_items = request_db.get_stale_subscribed_media(movie_search_window, movie_protection_days)
            
            if stale_items:
                logger.warning(f"  ➜ 发现 {len(stale_items)} 个超时订阅，准备处理。")
                cancelled_ids_map = {} 
                cancelled_for_report = []

                for item in stale_items:
                    tmdb_id_to_cancel = item['tmdb_id']
                    item_type = item['item_type']
                    title = item['title']
                    season_to_cancel = None

                    if item_type == 'Season':
                        if item['parent_series_tmdb_id']:
                            tmdb_id_to_cancel = item['parent_series_tmdb_id']
                            season_to_cancel = item['season_number']
                        else:
                            logger.error(f"  ➜ 无法取消季《{item['title']}》，因为它缺少父剧集ID。")
                            continue

                    # --- 取消 MP 订阅 ---
                    success = moviepilot.cancel_subscription(
                        tmdb_id=tmdb_id_to_cancel,
                        item_type=item_type,
                        config=config,
                        season=season_to_cancel
                    )
                    
                    if success:
                        if item_type not in cancelled_ids_map:
                            cancelled_ids_map[item_type] = []
                        cancelled_ids_map[item_type].append(item['tmdb_id'])
                        
                        display_title = title
                        if item_type == 'Season':
                            parent_id = item.get('parent_series_tmdb_id')
                            s_num = item.get('season_number')
                            if parent_id:
                                series_title = media_db.get_series_title_by_tmdb_id(str(parent_id))
                                if series_title and s_num is not None:
                                    display_title = f"{series_title} 第 {s_num} 季"
                        
                        cancelled_for_report.append(f"《{display_title}》")

                # 1. 批量更新数据库状态 
                for item_type, tmdb_ids in cancelled_ids_map.items():
                    if tmdb_ids:
                        request_db.set_media_status_ignored(
                            tmdb_ids=tmdb_ids, 
                            item_type=item_type,
                            source={"type": "auto_ignored", "reason": "stale_subscription"},
                            ignore_reason="订阅超时"
                        )
                
                # 2. 发送取消通知
                if cancelled_for_report:
                    admin_chat_ids = user_db.get_admin_telegram_chat_ids()
                    if admin_chat_ids:
                        items_list_str = "\n".join([f"· `{item}`" for item in cancelled_for_report])
                        message_text = (f"➜ *自动取消了 {len(cancelled_for_report)} 个超时订阅*\n\n"
                                        f"下列项目因超过 {movie_search_window} 天未入库而被自动取消：\n{items_list_str}")
                        for admin_id in admin_chat_ids:
                            telegram.send_telegram_message(admin_id, message_text, disable_notification=True)

            else:
                logger.info("  ➜ 未发现超时订阅。")

        # ======================================================================
        # 阶段 1.5 - 清理下载超时并重新订阅
        # ======================================================================
        download_timeout_hours = int(strategy_config.get('download_timeout_hours', 0))
        if download_timeout_hours > 0:
            logger.info(f"  ➜ [策略] 检查下载超时超过 {download_timeout_hours} 小时的任务...")
            task_manager.update_status_from_thread(5, "正在检查下载超时任务...")
            
            downloading_tasks = moviepilot.get_downloading_tasks(config)
            if downloading_tasks:
                all_subs = media_db.get_all_subscriptions()
                
                # 获取带本地时区的当前时间
                now = datetime.now().astimezone()
                timeout_threshold = now - timedelta(hours=download_timeout_hours)

                for item in all_subs:
                    if item.get('subscription_status') != 'SUBSCRIBED':
                        continue

                    last_sub_str = item.get('last_subscribed_at')
                    if not last_sub_str:
                        continue

                    # 健壮的时间解析：处理带毫秒和时区的字符串 (如 2026-03-21 17:51:17.554 +0800)
                    if isinstance(last_sub_str, datetime):
                        last_sub_time = last_sub_str
                        if last_sub_time.tzinfo is None:
                            last_sub_time = last_sub_time.astimezone()
                    else:
                        try:
                            # 尝试标准化 ISO 格式
                            clean_str = str(last_sub_str).replace(" ", "T", 1)
                            if " +" in clean_str or " -" in clean_str:
                                clean_str = clean_str.replace(" +", "+").replace(" -", "-")
                            if re.search(r'[+-]\d{4}$', clean_str):
                                clean_str = clean_str[:-2] + ":" + clean_str[-2:]
                            last_sub_time = datetime.fromisoformat(clean_str)
                        except Exception:
                            try:
                                # 降级处理：去掉毫秒和时区，当做本地时间
                                last_sub_time = datetime.strptime(str(last_sub_str).split('.')[0], "%Y-%m-%d %H:%M:%S").astimezone()
                            except Exception:
                                continue

                    # 如果订阅时间早于超时阈值，说明超时了
                    if last_sub_time < timeout_threshold:
                        tmdb_id = item.get('tmdb_id')
                        item_type = item.get('item_type')
                        season_num = item.get('season_number')

                        # 确定要比对的真实 TMDb ID
                        target_tmdb_id = int(item.get('parent_series_tmdb_id') or tmdb_id)

                        for task in downloading_tasks:
                            task_media = task.get('media', {})
                            if not task_media:
                                continue

                            task_tmdbid = task_media.get('tmdb_id') or task_media.get('tmdbid')
                            task_season = task_media.get('season')

                            # 匹配 TMDb ID 和 季号
                            if str(task_tmdbid) == str(target_tmdb_id):
                                if item_type == 'Season' and str(task_season) != str(season_num):
                                    continue

                                task_hash = task.get('hash')
                                
                                # MP的下载列表中，'title' 是原始种子名，'name' 是洗白后的媒体名
                                raw_title = task.get('title', '')
                                clean_media_name = task.get('name', '')
                                
                                # 优先使用 title 作为种子名来精准排除
                                torrent_name = raw_title if raw_title else clean_media_name

                                logger.warning(f"  ➜ 发现超时下载任务: 《{clean_media_name}》 (已订阅超过 {download_timeout_hours} 小时)")

                                # 1. 提取要排除的关键词（去除容易引起正则错误的括号，保留核心文件名）
                                exclude_keywords = set()
                                # 去除扩展名
                                clean_torrent_name = re.sub(r'\.(mkv|mp4|ts|avi|torrent)$', '', torrent_name, flags=re.IGNORECASE).strip()
                                # 去除开头的 [xxx] 或 【xxx】 这种容易让 MP 正则引擎懵逼的符号
                                clean_torrent_name = re.sub(r'^\[[^\]]+\]|^【[^】]+】', '', clean_torrent_name).strip()
                                # 去除开头可能残留的点或空格 (例如 "[狂怒].Fury" 变成 "Fury")
                                clean_torrent_name = clean_torrent_name.lstrip('. ')
                                
                                if clean_torrent_name:
                                    exclude_keywords.add(clean_torrent_name)

                                # 2. 删除下载器中的任务
                                if moviepilot.delete_download_tasks("dummy", config, hashes=[task_hash]):
                                    logger.info(f"    - 已删除超时下载任务: {task_hash[:8]}...")

                                    # 3. 更新 MP 订阅规则，排除该死种
                                    sub_info = moviepilot.get_subscription_by_tmdbid(target_tmdb_id, season_num if item_type == 'Season' else None, config)
                                    
                                    if sub_info and sub_info.get('id'):
                                        # 剧集未完结时，订阅通常还在，直接更新现有订阅
                                        if exclude_keywords:
                                            current_exclude = sub_info.get('exclude') or ""
                                            exclude_list = [e.strip() for e in current_exclude.split(',') if e.strip()]
                                            added_any = False
                                            for kw in exclude_keywords:
                                                if kw not in exclude_list:
                                                    exclude_list.append(kw)
                                                    added_any = True
                                            
                                            if added_any:
                                                sub_info['exclude'] = ",".join(exclude_list)
                                                if moviepilot.update_subscription(sub_info, config):
                                                    logger.info(f"    - 已更新现有订阅规则，排除死种: {', '.join(exclude_keywords)}")

                                        # 4. 触发重新搜索
                                        moviepilot.search_subscription(sub_info['id'], config)
                                        logger.info(f"    - 已触发重新搜索")
                                    else:
                                        # 电影或已完结剧集，MP 会在下载开始后删除订阅，因此需要重新提交
                                        logger.info(f"    - MP 中订阅已自动移除(正常现象)，正在重新提交订阅并追加排除规则...")
                                        
                                        payload = {
                                            "tmdbid": int(target_tmdb_id),
                                            "type": "电影" if item_type == 'Movie' else "电视剧"
                                        }
                                        
                                        if item_type == 'Season' and season_num is not None:
                                            payload['season'] = int(season_num)
                                            series_name = media_db.get_series_title_by_tmdb_id(str(target_tmdb_id))
                                            if series_name:
                                                payload['name'] = series_name
                                        elif item_type == 'Movie':
                                            payload['name'] = item.get('title', '')
                                            
                                        if exclude_keywords:
                                            payload['exclude'] = ",".join(exclude_keywords)
                                            
                                        if moviepilot.subscribe_with_custom_payload(payload, config):
                                            logger.info(f"    - 重新订阅成功，并已排除死种: {', '.join(exclude_keywords)}")
                                        else:
                                            logger.error(f"    - 重新订阅失败！")

                                    # 5. 更新本地订阅时间，防止无限循环
                                    request_db.set_media_status_subscribed(
                                        tmdb_ids=[tmdb_id],
                                        item_type=item_type
                                    )
                                break # 跳出内层循环，处理下一个 item

        # ======================================================================
        # 阶段 2 - 电影间歇性订阅搜索
        # ======================================================================
        # 仅当配置有效时执行
        if movie_protection_days > 0 and movie_pause_days > 0:
            logger.info(f"  ➜ [策略] 执行电影间歇性订阅搜索维护...")
            
            # 2.1 复活 (Revive: PAUSED -> SUBSCRIBED)
            # 对应 MP 状态: 'S' -> 'R'
            movies_to_revive = request_db.get_movies_to_revive()
            if movies_to_revive:
                revived_ids = []
                for movie in movies_to_revive:
                    tmdb_id = movie['tmdb_id']
                    title = movie['title']
                    
                    # ★★★ 修改：直接更新状态为 'R' (Run) ★★★
                    # season=None 表示电影
                    if moviepilot.update_subscription_status(int(tmdb_id), None, 'R', config):
                        revived_ids.append(tmdb_id)
                    else:
                        # 如果更新失败（比如MP里订阅丢了），尝试重新订阅兜底
                        logger.warning(f"    - 《{title}》状态切换失败，尝试重新提交订阅...")
                        if moviepilot.subscribe_with_custom_payload({"tmdbid": int(tmdb_id), "type": "电影"}, config):
                            revived_ids.append(tmdb_id)
                
                if revived_ids:
                    request_db.update_movie_status_revived(revived_ids)
                    logger.info(f"  ➜ 成功复活 {len(revived_ids)} 部电影 (MP状态->R)。")

            # 2.2 暂停 (Pause: SUBSCRIBED -> PAUSED)
            # 对应 MP 状态: 'R' -> 'S'
            movies_to_pause = request_db.get_movies_to_pause(search_window_days=movie_search_window, protection_days=movie_protection_days)
            if movies_to_pause:
                paused_ids = []
                for movie in movies_to_pause:
                    tmdb_id = movie['tmdb_id']
                    title = movie['title']
                    
                    # ★★★ 修改开始：尝试暂停，失败则补订后再次暂停 ★★★
                    if moviepilot.update_subscription_status(int(tmdb_id), None, 'S', config):
                        paused_ids.append(tmdb_id)
                    else:
                        logger.warning(f"    - 《{title}》暂停失败 (MP中可能不存在)，尝试重新订阅并同步状态...")
                        
                        # 1. 尝试补订 (默认状态通常为 R)
                        mp_payload = {"name": title, "tmdbid": int(tmdb_id), "type": "电影"}
                        if moviepilot.subscribe_with_custom_payload(mp_payload, config):
                            # 2. 补订成功后，再次尝试将其状态更新为 'S'
                            if moviepilot.update_subscription_status(int(tmdb_id), None, 'S', config):
                                paused_ids.append(tmdb_id)
                                logger.info(f"    - ➜ 《{title}》补订并暂停成功。")
                            else:
                                logger.warning(f"    - ➜ 《{title}》补订成功，但暂停状态同步失败。")
                        else:
                            logger.error(f"    - ➜ 《{title}》补订失败，无法执行暂停操作。")
                
                if paused_ids:
                    request_db.update_movie_status_paused(paused_ids, pause_days=movie_pause_days)
                    logger.info(f"  ➜ 成功暂停 {len(paused_ids)} 部暂无资源的新片 (MP状态->S)。")
        
        # ======================================================================
        # 阶段 3 - 超时订阅复活 (轮回机制)
        # ======================================================================
        if timeout_revive_days > 0:
            logger.info(f"  ➜ [策略] 检查是否有被'订阅超时'清理的项目满足复活条件 (>{timeout_revive_days}天)...")
            
            items_to_revive = media_db.get_timed_out_items_to_revive(timeout_revive_days)
            
            if items_to_revive:
                logger.info(f"  🧟 发现 {len(items_to_revive)} 个超时项目满足复活条件，正在重置为 '待订阅'...")
                
                revived_count = 0
                for item in items_to_revive:
                    # 将状态重置为 WANTED，且 force_unignore=True 以允许从 IGNORED 状态流转
                    # source 设为 auto_revive 以便追踪
                    request_db.set_media_status_wanted(
                        tmdb_ids=[item['tmdb_id']],
                        item_type=item['item_type'],
                        source={"type": "revive_from_timeout", "reason": "auto_revive_from_timeout"}, # 使用 manual_add 类型确保能被 set_media_status_wanted 处理
                        force_unignore=True
                    )
                    revived_count += 1
                    logger.debug(f"    - 《{item['title']}》已复活。")
                
                logger.info(f"  ➜ 成功复活了 {revived_count} 个项目，它们将在本次或下次任务中被重新处理。")
            else:
                logger.debug("  ➜ 没有满足复活条件的项目。")
        
        # ======================================================================
        # 阶段 4 - 执行订阅 
        # ======================================================================
        logger.info("  ➜ 正在检查未上映...")
        promoted_count = media_db.promote_pending_to_wanted()
        if promoted_count > 0:
            logger.info(f"  ➜ 成功将 {promoted_count} 个项目从“未上映”更新为“待订阅”。")
        else:
            logger.trace("  ➜ 没有需要晋升状态的媒体项。")

        wanted_items = media_db.get_all_wanted_media()
        if not wanted_items:
            logger.info("  ➜ 待订阅列表为空，无需处理。")
            task_manager.update_status_from_thread(100, "待订阅列表为空。")
            return

        logger.info(f"  ➜ 发现 {len(wanted_items)} 个待处理的订阅请求。")
        task_manager.update_status_from_thread(10, f"发现 {len(wanted_items)} 个待处理请求...")

        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        subscription_details = []
        rejected_details = []
        notifications_to_send = {}
        failed_notifications_to_send = {}
        quota_exhausted = False

        # 2. 遍历待办列表，逐一处理
        for i, item in enumerate(wanted_items):
            if processor.is_stop_requested(): break
            
            task_manager.update_status_from_thread(
                int(10 + (i / len(wanted_items)) * 85),
                f"({i+1}/{len(wanted_items)}) 正在处理: {item['title']}"
            )

            # 2.1 检查发行日期 (只对电影检查，剧集由 smart_subscribe 处理)
            if item['item_type'] == 'Movie' and not is_movie_subscribable(int(item['tmdb_id']), tmdb_api_key, config):
                logger.info(f"  ➜ 电影《{item['title']}》未到发行日期，本次跳过。")
                rejected_details.append({'item': f"电影《{item['title']}》", 'reason': '未发行'})
                # ★★★ 新增：解析来源并记录失败通知 ★★★
                sources = item.get('subscription_sources_json', [])
                for source in sources:
                    if source.get('type') == 'user_request' and (user_id := source.get('user_id')):
                        if user_id not in failed_notifications_to_send:
                            failed_notifications_to_send[user_id] = []
                        failed_notifications_to_send[user_id].append(f"《{item['title']}》(原因: 不满足发行日期延迟订阅)")
                continue

            # ★★★ 1. 准备基础信息 (提前获取剧集标题，用于日志和搜索) ★★★
            tmdb_id = item['tmdb_id']
            item_type = item['item_type']
            title = item['title'] # 默认为 item 标题
            season_number = item.get('season_number')
            parent_tmdb_id = None

            # 如果是季/集，修正标题为剧集标题
            if item_type in ['Series', 'Season']:
                if item_type == 'Season':
                    parent_tmdb_id = item.get('parent_series_tmdb_id')
                    # 尝试解析 ID
                    if not parent_tmdb_id and '_' in str(tmdb_id):
                        parent_tmdb_id = str(tmdb_id).split('_')[0]
                    if not parent_tmdb_id:
                        parent_tmdb_id = tmdb_id
                else:
                    parent_tmdb_id = tmdb_id

                # 获取剧集名称
                series_name = media_db.get_series_title_by_tmdb_id(parent_tmdb_id)
                if not series_name:
                     # 尝试从 item title 解析 (例如 "Breaking Bad - S1")
                     raw_title = item.get('title', '')
                     parsed_name, _ = parse_series_title_and_season(raw_title, tmdb_api_key)
                     series_name = parsed_name if parsed_name else raw_title
                
                # 更新 title 变量为剧集标题
                if series_name:
                    title = series_name

            # --- MoviePilot 订阅 ---
            #  检查配额
            if settings_db.get_subscription_quota() <= 0:
                quota_exhausted = True
                break

            # 提交 MP 订阅
            success = False
            action_type = "MP" 
            watchlist_config = settings_db.get_setting('watchlist_config') or {}
            tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)
            subscription_priority = strategy_config.get('subscription_priority', 'mp')

            if item_type == 'Movie':
                # ==========================================
                # 电影逻辑：影巢优先 -> MP 兜底
                # ==========================================
                if subscription_priority == 'hdhive':
                    logger.info(f"  ➜ [策略] 电影《{title}》启用影巢优先，正在检索并筛选资源...")
                    hdhive_api_key = settings_db.get_setting('hdhive_api_key')
                    
                    if hdhive_api_key:
                        hd_client = HDHiveClient(hdhive_api_key)
                        resources = hd_client.get_resources(tmdb_id, 'movie')
                        
                        if resources:
                            logger.info(f"  ➜ 影巢共返回 {len(resources)} 个资源，开始执行条件筛选：")
                            # --- ★★★ 智能漏斗：开始过滤资源 ★★★ ---
                            valid_resources = []
                            
                            # 读取过滤配置
                            hd_free_only = strategy_config.get('hdhive_free_only', False)
                            hd_max_points = strategy_config.get('hdhive_max_points', 10)
                            hd_max_size = strategy_config.get('hdhive_max_size_gb', 120)
                            hd_res = strategy_config.get('hdhive_resolution', 'All')
                            hd_zh_sub_only = strategy_config.get('hdhive_zh_sub_only', True)
                            hd_exclude_iso = strategy_config.get('hdhive_exclude_iso', False)
                            
                            for i, r in enumerate(resources, 1):
                                r_title = r.get('title') or '未知标题'
                                r_source = r.get('source') or []
                                r_sub_lang = r.get('subtitle_language') or []
                                r_remark = r.get('remark') or ''
                                
                                # 提前计算积分和体积，用于日志打印
                                is_unlocked = r.get('is_unlocked', False)
                                raw_points = r.get('unlock_points') or 0
                                effective_points = 0 if is_unlocked else raw_points
                                size_gb = _parse_size_to_gb(r.get('share_size'))
                                
                                logger.info(f"  ➜ [{i}/{len(resources)}] 检查: {r_title} (体积: {size_gb:.2f}GB, 需积分: {effective_points})")
                                
                                # 1. 积分过滤
                                if hd_free_only and effective_points > 0:
                                    logger.info(f"  ➜ 排除: 仅限免费资源 (该资源需 {effective_points} 积分)")
                                    continue
                                if effective_points > hd_max_points:
                                    logger.info(f"  ➜ 排除: 超过最大积分限制 (限制 {hd_max_points} 积分)")
                                    continue
                                    
                                # 2. 体积过滤 (核心逻辑：大于设定体积就丢弃)
                                if size_gb > hd_max_size:
                                    logger.info(f"  ➜ 排除: 超过最大体积限制 (限制 {hd_max_size}GB)")
                                    continue
                                
                                # 安全策略：丢弃无法获取体积的资源，防止盲盒下载超大合集
                                # 如果你想放行未知体积的资源，请在下面两行前面加上 # 注释掉
                                if size_gb <= 0.0:
                                    logger.info(f"  ➜ 排除: 无法获取有效体积信息，为防止超大文件自动拦截")
                                    continue
                                    
                                # 3. 分辨率过滤
                                if hd_res != 'All':
                                    res_list = r.get('video_resolution', [])
                                    if hd_res not in res_list:
                                        logger.info(f"  ➜ 排除: 分辨率不匹配 (需要 {hd_res}，实际为 {res_list})")
                                        continue

                                # 4. 排除 ISO 原盘
                                if hd_exclude_iso:
                                    is_iso = False
                                    if any('ISO' in s.upper() for s in r_source):
                                        is_iso = True
                                    if 'ISO' in r_title.upper() and 'REMUX' not in r_title.upper():
                                        is_iso = True
                                        
                                    if is_iso:
                                        logger.info(f"  ➜ 排除: 命中了排除 ISO 原盘规则")
                                        continue

                                # 5. 仅限中文字幕
                                if hd_zh_sub_only:
                                    has_zh_sub = False
                                    if any(lang in ['简中', '繁中', '中文', '国语', '粤语', '中英'] for lang in r_sub_lang):
                                        has_zh_sub = True
                                    elif re.search(r'(中字|简中|繁中|特效字幕|国语|粤语|简繁|中英)', (r_title or "") + (r_remark or ""), re.IGNORECASE):
                                        has_zh_sub = True
                                    
                                    if not has_zh_sub:
                                        logger.info(f"  ➜ 排除: 未检测到中文字幕标识")
                                        continue
                                        
                                # 如果代码能走到这里，说明所有条件都满足了
                                logger.info(f"  ➜ 筛选通过，加入候选列表")
                                r['_effective_points'] = effective_points
                                r['_size_gb'] = size_gb
                                valid_resources.append(r)
                            
                            # --- ★★★ 智能排序：选出最优解 ★★★ ---
                            if valid_resources:
                                logger.info(f"  ➜ 共有 {len(valid_resources)} 个资源通过筛选，正在根据 (积分最少 -> 体积最大) 排序...")
                                # 排序规则：优先选积分最少的(白嫖优先) -> 积分相同时，选体积最大的(画质更好)
                                valid_resources.sort(key=lambda x: (x['_effective_points'], -x['_size_gb']))
                                
                                target_resource = valid_resources[0]
                                slug = target_resource.get('slug')
                                
                                logger.info(f"  ➜ 最终选定最优影巢资源: {target_resource.get('title')} "
                                            f"(体积: {target_resource['_size_gb']:.2f}GB, 需积分: {target_resource['_effective_points']})")
                                
                                if slug:
                                    success = task_download_from_hdhive(hdhive_api_key, slug, tmdb_id, 'movie', title)
                                    if success:
                                        action_type = "影巢" 
                                        logger.info(f"  ➜ 影巢秒传成功！已跳过 MoviePilot 订阅。")
                                    else:
                                        logger.warning(f"  ➜ 影巢转存失败，准备降级到 MoviePilot 兜底...")
                            else:
                                logger.info(f"  ➜ 影巢有资源，但全部被过滤规则拦截，准备降级到 MoviePilot 兜底...")
                        else:
                            logger.info(f"  ➜ 影巢未找到电影《{title}》的资源，准备降级到 MoviePilot 兜底...")
                    else:
                        logger.warning(f"  ➜ 未配置影巢 API Key，自动降级到 MoviePilot...")

                # 如果影巢没开、没找到资源、或者转存失败，统一交由 MP 兜底
                if not success:
                    logger.info(f"  ➜ 正在向 MoviePilot 提交电影《{title}》的订阅...")
                    mp_payload = {"name": title, "tmdbid": int(tmdb_id), "type": "电影"}
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
            elif item_type == 'Series':
                success = _subscribe_full_series_with_logic(int(tmdb_id), title, config, tmdb_api_key)
            elif item_type == 'Season' and parent_tmdb_id and season_number is not None:
                mp_payload = {"name": title, "tmdbid": int(parent_tmdb_id), "type": "电视剧", "season": int(season_number)}
                
                # 判定洗版/追更
                is_pending, fake_eps = should_mark_as_pending(int(parent_tmdb_id), int(season_number), tmdb_api_key)
                is_completed = False # ★★★ 新增标志位
                
                if not is_pending and check_series_completion(int(parent_tmdb_id), tmdb_api_key, season_number=int(season_number), series_name=title):
                    mp_payload["best_version"] = 1
                    is_completed = True # ★★★ 标记为已完结
                
                # ★★★ 拦截 TG 频道追更 ★★★
                if tg_channel_tracking and not is_completed:
                    logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{season_number} 的订阅。")
                    success = True # 模拟成功
                else:
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
                    if success and is_pending:
                        moviepilot.update_subscription_status(int(parent_tmdb_id), int(season_number), 'P', config, total_episodes=fake_eps)

            # 处理订阅结果
            if success:
                logger.info(f"  ➜ 《{item['title']}》订阅成功！")
                
                # 将状态从 WANTED 更新为 SUBSCRIBED
                if item_type != 'Series':
                    request_db.set_media_status_subscribed(
                        tmdb_ids=item['tmdb_id'], 
                        item_type=item_type,
                    )

                # 扣除配额
                settings_db.decrement_subscription_quota()

                # 准备通知 (智能拼接通知标题)
                item_display_name = ""
                if item_type == 'Season':
                    season_num = item.get('season_number')
                    if season_num is not None:
                        item_display_name = f"剧集《{series_name} 第 {season_num} 季》"
                    else:
                        item_display_name = f"剧集《{series_name}》"
                else:
                    item_display_name = f"{item_type}《{item['title']}》"
                
                # 解析订阅来源，找出需要通知的用户
                sources = item.get('subscription_sources_json', [])
                source_display_parts = []
                for source in sources:
                    source_type = source.get('type')
                    if source_type == 'resubscribe':
                        rule_name = source.get('rule_name', '未知规则')
                        source_display_parts.append(f"自动洗版({rule_name})")
                    elif source_type == 'user_request' and (user_id := source.get('user_id')):
                        if user_id not in notifications_to_send:
                            notifications_to_send[user_id] = []
                        
                        # 为用户通知构建完整的标题
                        user_notify_title = item['title']
                        if item_type == 'Season':
                            season_num = item.get('season_number')
                            if season_num is not None:
                                user_notify_title = f"{series_name} 第 {season_num} 季"
                        
                        notifications_to_send[user_id].append(user_notify_title)
                        source_display_parts.append(f"用户请求({user_db.get_username_by_id(user_id) or user_id})")
                    elif source_type == 'actor_subscription':
                        source_display_parts.append(f"演员订阅({source.get('name', '未知')})")
                    elif source_type in ['custom_collection', 'native_collection']:
                        source_display_parts.append(f"合集({source.get('name', '未知')})")
                    elif source_type == 'watchlist':
                        source_display_parts.append("追剧补全")
                
                source_display = ", ".join(set(source_display_parts)) or "未知来源"
                subscription_details.append({'source': source_display, 'item': item_display_name, 'action': action_type})

            else:
                logger.error(f"  ➜ 订阅《{item['title']}》失败，请检查 MoviePilot 连接或日志。")

            # 如果配置了延时，且不是列表中的最后一个项目，则进行休眠
            if request_delay > 0 and i < len(wanted_items) - 1:
                logger.debug(f"  ➜ 根据配置暂停 {request_delay} 秒...")
                time.sleep(request_delay)
        
        # 发送用户通知
        logger.info(f"  ➜ 准备为 {len(notifications_to_send)} 位用户发送合并的成功通知...")
        for user_id, subscribed_items in notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{item}`" for item in subscribed_items])
                    message_text = (f"🎉 *您的 {len(subscribed_items)} 个订阅已成功处理*\n\n您之前想看的下列内容现已加入下载队列：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"  ➜ 为用户 {user_id} 发送自动订阅的合并通知时出错: {e}")

        # 失败的通知
        logger.info(f"  ➜ 准备为 {len(failed_notifications_to_send)} 位用户发送合并的失败通知...")
        for user_id, failed_items in failed_notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{item}`" for item in failed_items])
                    message_text = (f"➜ *您的部分订阅请求未被处理*\n\n下列内容因不满足条件而被跳过：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"为用户 {user_id} 发送自动订阅的合并失败通知时出错: {e}")

        if subscription_details:
            header = f"  ✅ *统一订阅任务完成，成功处理 {len(subscription_details)} 项:*"
            
            item_lines = []
            for detail in subscription_details:
                source = telegram.escape_markdown(detail.get('source', '未知来源'))
                item = telegram.escape_markdown(detail['item'])
                
                action_tag = "影巢转存" if detail.get('action') == '影巢' else "MP订阅"
                
                item_lines.append(f"├─ `[{action_tag}]` `[{source}]` {item}")
                
            summary_message = header + "\n" + "\n".join(item_lines)
        else:
            summary_message = "ℹ️ *统一订阅任务完成，无成功处理的订阅项。*"

        if rejected_details:
            rejected_header = f"\n\n➜ *下列 {len(rejected_details)} 项因不满足订阅条件而被跳过:*"
            
            rejected_lines = []
            for detail in rejected_details:
                reason = telegram.escape_markdown(detail.get('reason', '未知原因'))
                item = telegram.escape_markdown(detail['item'])
                rejected_lines.append(f"├─ `{reason}` {item}")
                
            summary_message += rejected_header + "\n" + "\n".join(rejected_lines)

        if quota_exhausted:
            content = "(每日订阅配额已用尽，部分项目可能未处理)"
            escaped_content = telegram.escape_markdown(content)
            summary_message += f"\n\n*{escaped_content}*"

        # 打印日志和发送通知的逻辑保持不变
        logger.info(summary_message.replace('*', '').replace('`', ''))
        admin_chat_ids = user_db.get_admin_telegram_chat_ids()
        if admin_chat_ids:
            logger.info(f"  ➜ 准备向 {len(admin_chat_ids)} 位管理员发送任务总结...")
            for chat_id in admin_chat_ids:
                # 发送通知，静默模式，避免打扰
                telegram.send_telegram_message(chat_id, summary_message, disable_notification=True)

        task_manager.update_status_from_thread(100, "统一订阅任务处理完成。")
        logger.info(f"--- '{task_name}' 任务执行完毕 ---")

    except Exception as e:
        logger.error(f"  ➜ {task_name} 任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")