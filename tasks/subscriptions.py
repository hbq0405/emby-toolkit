# tasks/subscriptions.py
# 智能订阅模块
import time
from datetime import datetime
import logging
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed 

# 导入需要的底层模块和共享实例
import config_manager
import constants
import handler.tmdb as tmdb
import handler.moviepilot as moviepilot
import handler.nullbr as nullbr_handler
import task_manager
from handler import telegram
from database import settings_db, request_db, user_db, media_db, watchlist_db
from .helpers import is_movie_subscribable, check_series_completion, parse_series_title_and_season, should_mark_as_pending

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
                logger.info(f"  ⏳ 季《{final_series_name}》S{s_num} 无发行日期，视为 '待上映'。")
            
            if is_future_season:
                logger.info(f"  ⏳ 《{final_series_name}》第 {s_num} 季 尚未播出 ({air_date_str})，已加入待上映列表。")
                
                media_info = {
                    'tmdb_id': str(s_id) if s_id else f"{tmdb_id}_S{s_num}",
                    'title': season.get('name', f"第 {s_num} 季"),
                    'season_number': s_num,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'release_date': air_date_str,
                    'poster_path': final_poster, # 使用处理后的海报
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
                logger.info(f"  ⏳ 季《{final_series_name}》S{s_num} 满足自动待定条件，将执行 [订阅 -> 转待定] 流程。")

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
            if not is_pending_logic:
                if check_series_completion(tmdb_id, tmdb_api_key, season_number=s_num, series_name=final_series_name):
                    mp_payload["best_version"] = 1
                    logger.info(f"  ➜ S{s_num} 已完结，启用洗版模式订阅。")
                else:
                    logger.info(f"  ➜ S{s_num} 未完结，使用追更模式订阅。")
            else:
                logger.info(f"  ➜ S{s_num} 处于待定模式，使用追更模式订阅。")

            # ==============================================================
            # 逻辑 E: 提交订阅 & 后置状态修正
            # ==============================================================
            if moviepilot.subscribe_with_custom_payload(mp_payload, config):
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
                        logger.info(f"  ✅ S{s_num} 已成功转为待定状态。")
                    else:
                        logger.warning(f"  ⚠️ S{s_num} 订阅成功，但转待定状态失败。")

                # 订阅成功后，更新本地数据库状态为 SUBSCRIBED
                # (即使 MP 是 Pending，对于本地请求队列来说，它也算是“已处理/已订阅”)
                target_s_id = str(s_id) if s_id else f"{tmdb_id}_S{s_num}"
                media_info = {
                    'tmdb_id': target_s_id,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'season_number': s_num,
                    'title': season.get('name'),
                    'poster_path': final_poster,
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
                # 1. ID 和 季号 修正
                if item_type == 'Season' and season_number is None:
                    season_info = media_db.get_media_details(str(tmdb_id), 'Season')
                    if season_info:
                        season_number = season_info.get('season_number')
                        parent_id = season_info.get('parent_series_tmdb_id')
                        if parent_id: tmdb_id = parent_id 
                
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
                    # 手动订阅不看配置，只看事实：完结了就洗版(best_version=1)，没完结就追更。
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
                        # 连载中 -> 不传 best_version (默认为0)
                        logger.info(f"  ➜ [手动交互] S{season_number} 尚未完结 (连载中)，使用普通追更模式。")
                    
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
                logger.info(f"  ✅ 《{item_title_for_log}》订阅成功！")
                settings_db.decrement_subscription_quota()
                
                # 更新数据库状态 (Series 类型在 _subscribe_full_series_with_logic 里处理了)
                if item_type != 'Series':
                    # 如果是季，需要构建正确的 ID (例如 tmdbid_S1)
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
        
        final_message = f"  ✅ 手动订阅任务完成，成功处理 {processed_count}/{total_items} 个项目。"
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
    enable_nullbr_fallback = strategy_config.get('enable_nullbr_fallback', False)     # 默认不启用 NULLBR 兜底
    nullbr_priority = strategy_config.get('nullbr_priority', 'mp')                    # 默认 MP 优先
    
    # 2. 读取请求延迟配置
    try:
        request_delay = int(config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 0))
    except:
        request_delay = 0
    
    # 兼容旧的全局开关 (如果用户还没配置过策略，可以回退读取 config.ini，或者直接用默认值)
    if not config.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("  ➜ 订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        # 读取配置
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
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
                fallback_success_report = [] # ★★★ 新增：用于记录兜底成功的项目

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
                    
                    is_fallback_success = False

                    # ★★★ NULLBR 兜底逻辑 ★★★
                    if enable_nullbr_fallback and item_type == 'Movie':
                        logger.info(f"  🚑 尝试对老片《{title}》执行 NULLBR 兜底搜索...")
                        if nullbr_handler.auto_download_best_resource(tmdb_id_to_cancel, 'movie', title):
                            logger.info(f"  ✅ 《{title}》NULLBR 兜底推送成功！")
                            is_fallback_success = True
                        else:
                            logger.info(f"  ❌ 《{title}》NULLBR 未找到合适资源。")

                    # --- 取消 MP 订阅 ---
                    success = moviepilot.cancel_subscription(
                        tmdb_id=tmdb_id_to_cancel,
                        item_type=item_type,
                        config=config,
                        season=season_to_cancel
                    )
                    
                    if success:
                        # ★★★ 如果兜底成功 ★★★
                        if is_fallback_success:
                            logger.info(f"  ➜ 《{title}》 已通过NULLBR兜底下载，取消MP订阅。")
                            fallback_success_report.append(f"《{title}》") # 加入成功报告
                            # 将其标记为 IGNORED，理由是“NULLBR兜底成功”，这样就不会再被当作超时订阅处理了
                            request_db.set_media_status_ignored(
                                tmdb_ids=[tmdb_id_to_cancel],
                                item_type=item_type,
                                source={"type": "nullbr_fallback", "reason": "stale_timeout_recovered"},
                                ignore_reason="NULLBR兜底成功"
                            )
                            continue # 跳过后续的 IGNORED 更新

                        # ★★★ 如果兜底失败或未启用 ★★★
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

                # 1. 批量更新数据库状态 (仅针对未兜底成功的项目)
                for item_type, tmdb_ids in cancelled_ids_map.items():
                    if tmdb_ids:
                        request_db.set_media_status_ignored(
                            tmdb_ids=tmdb_ids, 
                            item_type=item_type,
                            source={"type": "auto_ignored", "reason": "stale_subscription"},
                            ignore_reason="订阅超时"
                        )
                
                # 2. 发送取消通知 (原有逻辑)
                if cancelled_for_report:
                    admin_chat_ids = user_db.get_admin_telegram_chat_ids()
                    if admin_chat_ids:
                        items_list_str = "\n".join([f"· `{item}`" for item in cancelled_for_report])
                        message_text = (f"🚫 *自动取消了 {len(cancelled_for_report)} 个超时订阅*\n\n"
                                        f"下列项目因超过 {movie_search_window} 天未入库而被自动取消：\n{items_list_str}")
                        for admin_id in admin_chat_ids:
                            telegram.send_telegram_message(admin_id, message_text, disable_notification=True)

                # 3. ★★★ 发送兜底成功通知 (新增逻辑) ★★★
                if fallback_success_report:
                    admin_chat_ids = user_db.get_admin_telegram_chat_ids()
                    if admin_chat_ids:
                        items_list_str = "\n".join([f"· `{item}`" for item in fallback_success_report])
                        message_text = (f"🚑 *NULLBR 兜底成功通知*\n\n"
                                        f"下列老片因订阅超时被取消，但成功通过 NULLBR 找到资源并推送下载：\n{items_list_str}")
                        for admin_id in admin_chat_ids:
                            telegram.send_telegram_message(admin_id, message_text, disable_notification=True)

            else:
                logger.info("  ➜ 未发现超时订阅。")

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
                    logger.info(f"  ✅ 成功复活 {len(revived_ids)} 部电影 (MP状态->R)。")

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
                                logger.info(f"    - ✅ 《{title}》补订并暂停成功。")
                            else:
                                logger.warning(f"    - ⚠️ 《{title}》补订成功，但暂停状态同步失败。")
                        else:
                            logger.error(f"    - ❌ 《{title}》补订失败，无法执行暂停操作。")
                
                if paused_ids:
                    request_db.update_movie_status_paused(paused_ids, pause_days=movie_pause_days)
                    logger.info(f"  💤 成功暂停 {len(paused_ids)} 部暂无资源的新片 (MP状态->S)。")
        
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
                
                logger.info(f"  ✅ 成功复活了 {revived_count} 个项目，它们将在本次或下次任务中被重新处理。")
            else:
                logger.debug("  ➜ 没有满足复活条件的项目。")
        
        # ======================================================================
        # 阶段 3 - 执行常规订阅 
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

        # 准备变量
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        subscription_details = [] # 给管理员的报告
        rejected_details = []     # 给管理员的报告
        notifications_to_send = {} # 给用户的通知 {user_id: [item_name, ...]}
        failed_notifications_to_send = {} #失败的通知
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

            # 2.2启用NULLBR + 优先级为NULLBR + 是老片
            nullbr_handled = False
            
            if enable_nullbr_fallback and nullbr_priority == 'nullbr':
                
                if item['item_type'] != 'Movie':
                    logger.debug(f"  ➜ 《{item['title']}》是剧集，跳过 NULLBR 优先模式，交由 MP 处理。")
                else:
                    is_old_item = False
                    release_date_value = item.get('release_date')

                    if not release_date_value:
                        logger.warning(f"《{item['title']}》无发行日期，无法判定是否为老片")
                        is_old_item = False
                    else:
                        try:
                            days_since_release = (datetime.now().date() - release_date_value).days
                            logger.debug(f"《{item['title']}》距离发行天数: {days_since_release}")
                            is_old_item = days_since_release > movie_protection_days
                        except Exception as e:
                            logger.error(f"计算发行日期天数失败: {e}, 值: {release_date_value}")
                            is_old_item = False
                    
                    if is_old_item:
                        logger.info(f"  ➜ 检测到电影《{item['title']}》为老片且策略为 NULLBR 优先，尝试直接搜索资源...")
                    
                        # 执行下载
                        if nullbr_handler.auto_download_best_resource(item['tmdb_id'], 'movie', item['title']):
                            logger.info(f"  ✅ 《{item['title']}》NULLBR 直下成功，跳过 MP 订阅。")
                            
                            # 1. 标记为 IGNORED (原因: NULLBR直下)
                            # 使用 IGNORED 而不是 SUBSCRIBED，是为了避免 MP 状态同步逻辑去检查它
                            request_db.set_media_status_ignored(
                                tmdb_ids=[item['tmdb_id']],
                                item_type=item['item_type'],
                                source={"type": "nullbr_priority", "reason": "downloaded_by_nullbr"},
                                ignore_reason="NULLBR直下"
                            )
                            
                            # 3. 记录通知
                            subscription_details.append({'source': 'NULLBR优先', 'item': f"{item['title']} (直下)"})
                            
                            # 4. 标记已处理，跳过后续 MP 逻辑
                            nullbr_handled = True
                        else:
                            logger.info(f"  ❌ NULLBR 未找到合适资源，回退到 MP 订阅流程。")

            if nullbr_handled:
                continue

            # 2.3 检查配额
            if settings_db.get_subscription_quota() <= 0:
                quota_exhausted = True
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            # 2.4 执行订阅
            success = False
            item_type = item['item_type']
            series_name = ""
            mp_payload = {}
            
            # ★★★ 检查是否包含洗版专用的 Payload ★★★
            sources = item.get('subscription_sources_json', [])
            resub_source = next((s for s in sources if s.get('type') == 'resubscribe'), None)
            custom_payload = resub_source.get('payload') if resub_source else None
            
            # 如果存在自定义 Payload，直接使用它 (这是最高优先级)
            if custom_payload:
                logger.info(f"  ➜ 检测到《{item['title']}》包含自定义洗版 Payload，将执行精准洗版订阅。")
                success = moviepilot.subscribe_with_custom_payload(custom_payload, config)
            
            else:
                if item_type == 'Movie':
                    mp_payload = {"name": item['title'], "tmdbid": int(item['tmdb_id']), "type": "电影"}
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

                elif item_type == 'Series':
                    sources = item.get('subscription_sources_json', [])
                    primary_source = sources[0] if sources else None
                    should_force_resub = False
                    for src in sources:
                        stype = src.get('type')
                        if stype == 'resubscribe':
                            should_force_resub = True
                            break

                    success = _subscribe_full_series_with_logic(
                        tmdb_id=int(item['tmdb_id']),
                        series_name=item['title'],
                        config=config,
                        tmdb_api_key=tmdb_api_key,
                        source=primary_source 
                    )
                    if success:
                        request_db.set_media_status_subscribed(
                            tmdb_ids=[str(item['tmdb_id'])], 
                            item_type='Series'
                        )

                elif item_type == 'Season':
                    parent_tmdb_id = item.get('parent_series_tmdb_id')
                    season_num = item.get('season_number')
                    
                    series_name = media_db.get_series_title_by_tmdb_id(parent_tmdb_id)
                    if not series_name:
                         raw_title = item.get('title', '')
                         parsed_name, _ = parse_series_title_and_season(raw_title, tmdb_api_key)
                         series_name = parsed_name if parsed_name else raw_title

                    if parent_tmdb_id and season_num is not None:
                        mp_payload = {
                            "name": series_name,
                            "tmdbid": int(parent_tmdb_id),
                            "type": "电视剧",
                            "season": season_num
                        }
                        
                        # 1. 检查具体的来源类型
                        is_explicit_resub = any(source.get('type') == 'resubscribe' for source in sources)
                        is_gap_scan = any(source.get('type') == 'gap_scan' for source in sources)
                        
                        # --- 预先检查是否满足自动待定条件 ---
                        is_pending_logic = False
                        fake_total_episodes = 0
                        # 只有非洗版、非缺集扫描的普通订阅才检查待定逻辑
                        if not is_explicit_resub and not is_gap_scan:
                            is_pending_logic, fake_total_episodes = should_mark_as_pending(
                                int(parent_tmdb_id), season_num, tmdb_api_key
                            )
                            if is_pending_logic:
                                logger.info(f"  ⏳ 季《{series_name}》S{season_num} 满足自动待定条件，将执行 [订阅 -> 转待定] 流程。")

                        # 2. 应用订阅策略
                        if is_explicit_resub:
                            # 情况 A: 明确的洗版规则触发 -> 强制 best_version=1
                            mp_payload["best_version"] = 1
                            logger.info(f"  ➜ 触发自动洗版规则，为《{series_name}》S{season_num} 启用洗版模式。")
                            
                        elif "best_version" not in mp_payload:
                            # 情况 C: 普通订阅
                            
                            # --- 如果是待定逻辑，则不检查完结，保持 best_version=0 ---
                            if is_pending_logic:
                                logger.info(f"  ➜ S{season_num} 处于待定模式，使用追更模式订阅。")
                            # 否则才检查是否完结
                            elif check_series_completion(int(parent_tmdb_id), tmdb_api_key, season_number=season_num, series_name=series_name):
                                mp_payload["best_version"] = 1
                                logger.info(f"  ➜ S{season_num} 已完结，启用洗版模式。")
                        
                        success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

                        # --- [新增] 订阅成功后的后置处理：修改状态为待定 ---
                        if success and is_pending_logic:
                            logger.info(f"  ➜ [后置操作] 正在将 S{season_num} 的状态修改为 'P' (待定)，并将总集数修正为 {fake_total_episodes}...")
                            mp_update_success = moviepilot.update_subscription_status(
                                tmdb_id=int(parent_tmdb_id),
                                season=season_num,
                                status='P', 
                                config=config,
                                total_episodes=fake_total_episodes
                            )
                            if mp_update_success:
                                logger.info(f"  ✅ S{season_num} 已成功转为待定状态。")
                            else:
                                logger.warning(f"  ⚠️ S{season_num} 订阅成功，但转待定状态失败。")

                    else:
                        success = False

            # 2.4 根据订阅结果更新状态和发送通知
            if success:
                logger.info(f"  ✅ 《{item['title']}》订阅成功！")
                
                # a. 将状态从 WANTED 更新为 SUBSCRIBED
                if item_type != 'Series':
                    request_db.set_media_status_subscribed(
                        tmdb_ids=item['tmdb_id'], 
                        item_type=item_type,
                    )

                # b. 扣除配额
                settings_db.decrement_subscription_quota()

                # d. 准备通知 (智能拼接通知标题)
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
                        
                        # ★★★ 修改：为用户通知构建完整的标题 ★★★
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
                    elif source_type == 'gap_scan':
                        source_display_parts.append("缺集扫描")
                    elif source_type == 'watchlist':
                        source_display_parts.append("追剧补全")
                
                source_display = ", ".join(set(source_display_parts)) or "未知来源"
                subscription_details.append({'source': source_display, 'item': item_display_name})

            else:
                logger.error(f"  ➜ 订阅《{item['title']}》失败，请检查 MoviePilot 连接或日志。")

            # 如果配置了延时，且不是列表中的最后一个项目，则进行休眠
            if request_delay > 0 and i < len(wanted_items) - 1:
                logger.debug(f"  zzz 根据配置暂停 {request_delay} 秒...")
                time.sleep(request_delay)
        
        # 3. 发送用户通知
        logger.info(f"  ➜ 准备为 {len(notifications_to_send)} 位用户发送合并的成功通知...")
        for user_id, subscribed_items in notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{item}`" for item in subscribed_items])
                    message_text = (f"🎉 *您的 {len(subscribed_items)} 个订阅已成功处理*\n\n您之前想看的下列内容现已加入下载队列：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"为用户 {user_id} 发送自动订阅的合并通知时出错: {e}")

        # 4. 失败的通知
        logger.info(f"  ➜ 准备为 {len(failed_notifications_to_send)} 位用户发送合并的失败通知...")
        for user_id, failed_items in failed_notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{item}`" for item in failed_items])
                    message_text = (f"⚠️ *您的部分订阅请求未被处理*\n\n下列内容因不满足条件而被跳过：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"为用户 {user_id} 发送自动订阅的合并失败通知时出错: {e}")

        if subscription_details:
            # ★★★ 核心修改 1/3: 调整标题，使用更通用的措辞 ★★★
            header = f"  ✅ *统一订阅任务完成，成功处理 {len(subscription_details)} 项:*"
            
            item_lines = []
            for detail in subscription_details:
                # ★★★ 核心修改 2/3: 移除 module，直接使用 source ★★★
                # 我们在前面已经把来源格式化得很好了，比如 "用户请求(admin)" 或 "合集(豆瓣电影Top250)"
                source = telegram.escape_markdown(detail.get('source', '未知来源'))
                item = telegram.escape_markdown(detail['item'])
                # 新的格式更简洁: [来源] -> 项目
                item_lines.append(f"├─ `[{source}]` {item}")
                
            summary_message = header + "\n" + "\n".join(item_lines)
        else:
            summary_message = "ℹ️ *统一订阅任务完成，无成功处理的订阅项。*"

        if rejected_details:
            # ★★★ 核心修改 3/3: 调整被拒部分的措辞和格式 ★★★
            rejected_header = f"\n\n⚠️ *下列 {len(rejected_details)} 项因不满足订阅条件而被跳过:*"
            
            rejected_lines = []
            for detail in rejected_details:
                # 这里不再需要 module 和 source，因为被拒的原因更重要
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