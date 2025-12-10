# tasks/subscriptions.py
# 智能订阅模块
import json
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
def _subscribe_full_series_with_logic(tmdb_id: int, series_name: str, config: Dict, tmdb_api_key: str, use_gap_fill_resubscribe: bool = False) -> bool:
    """
    处理整部剧集的订阅：
    1. 查询 TMDb 获取所有季。
    2. 遍历所有季。
    3. 仅对【最后一季】检查是否需要“自动待定”。
    4. 逐季提交订阅并更新本地数据库。
    """
    try:
        # 1. 获取剧集详情
        series_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
        if not series_details:
            logger.error(f"  ➜ 无法获取剧集 ID {tmdb_id} 的详情，跳过订阅。")
            return False

        # 规范化名称
        final_series_name = series_details.get('name', series_name)
        
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
        # 这会将 Series 设为 Watching，并重置所有已存在的季为 NONE
        watchlist_db.add_item_to_watchlist(str(tmdb_id), final_series_name)

        logger.info(f"  ➜ 正在处理《{final_series_name}》的 {len(valid_seasons)} 个季 (S{valid_seasons[0]['season_number']} - S{last_season_num})...")

        # 4. 遍历逐个订阅
        for season in valid_seasons:
            s_num = season['season_number']
            
            mp_payload = {
                "name": final_series_name,
                "tmdbid": tmdb_id,
                "type": "电视剧",
                "season": s_num
            }
            
            is_pending = False
            fake_total = 0

            # ★★★ 核心逻辑：只检查最后一季是否需要待定 ★★★
            if s_num == last_season_num:
                is_pending, fake_total = should_mark_as_pending(tmdb_id, s_num, tmdb_api_key)
                if is_pending:
                    mp_payload["status"] = "P"
                    mp_payload["total_episode"] = fake_total
                    logger.info(f"  🛡️ [自动待定] S{s_num} 是最新季且符合条件，初始状态设为 '待定(P)'。")

            # 洗版/完结检测 (非待定状态下才考虑 BestVersion)
            if not is_pending:
                if use_gap_fill_resubscribe:
                    mp_payload["best_version"] = 1
                elif check_series_completion(tmdb_id, tmdb_api_key, season_number=s_num, series_name=final_series_name):
                    mp_payload["best_version"] = 1

            # 提交订阅
            if moviepilot.subscribe_with_custom_payload(mp_payload, config):
                any_success = True
                
        return any_success

    except Exception as e:
        logger.error(f"处理整剧订阅逻辑时出错: {e}", exc_info=True)
        return False

# ★★★ 手动动订阅任务 ★★★
def task_manual_subscribe_batch(processor, subscribe_requests: List[Dict]):
    """
    - 统一订阅手动任务
    """
    total_items = len(subscribe_requests)
    task_name = f"手动订阅 {total_items} 个项目"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在准备手动订阅任务...")

    if not subscribe_requests:
        task_manager.update_status_from_thread(100, "任务完成：没有需要处理的项目。")
        return

    try:
        config = config_manager.APP_CONFIG
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        
        # 读取配置
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        use_gap_fill_resubscribe = watchlist_cfg.get('gap_fill_resubscribe', False)
        
        processed_count = 0

        for i, req in enumerate(subscribe_requests):
            tmdb_id = req.get('tmdb_id') # 注意：对于季，这里已经是 Series ID
            item_type = req.get('item_type')
            item_title_for_log = req.get('title', f"ID: {tmdb_id}")
            season_number = req.get('season_number')

            if not tmdb_id or not item_type:
                logger.warning(f"跳过一个无效的订阅请求: {req}")
                continue

            task_manager.update_status_from_thread(
                int((i / total_items) * 100),
                f"({i+1}/{total_items}) 正在处理: {item_title_for_log}"
            )

            if settings_db.get_subscription_quota() <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            success = False
            is_pending = False
            fake_total = 0
            
            # ==================================================================
            # 1. 尝试获取数据库中已存在的自定义 Payload (精准洗版)
            # ==================================================================
            custom_payload = None
            try:
                query_id = str(tmdb_id)
                if item_type == 'Season' and season_number is not None:
                    real_season_id = request_db.get_season_tmdb_id(query_id, season_number)
                    if real_season_id:
                        query_id = real_season_id
                    else:
                        query_id = f"{query_id}_S{season_number}"

                sources = request_db.get_subscribers_by_tmdb_id(query_id, item_type)
                
                if sources:
                    if isinstance(sources, str):
                        try: sources = json.loads(sources)
                        except: sources = []
                    
                    resub_source = next((s for s in sources if isinstance(s, dict) and s.get('type') == 'resubscribe' and s.get('payload')), None)
                    if resub_source:
                        custom_payload = resub_source['payload']
                        if 'tmdbid' in custom_payload:
                            custom_payload['tmdbid'] = int(custom_payload['tmdbid'])
                    
                    is_gap_or_resub = any(s.get('type') in ['gap_scan', 'resubscribe'] for s in sources if isinstance(s, dict))

            except Exception as e:
                logger.warning(f"  ⚠ 尝试获取自定义Payload时出错: {e}")

            # ==================================================================
            # 2. 执行订阅
            # ==================================================================

            # 分支 A: 使用自定义 Payload (精准洗版)
            if custom_payload:
                logger.info(f"  ➜ 检测到《{item_title_for_log}》包含自定义洗版参数，将执行精准洗版订阅。")
                success = moviepilot.subscribe_with_custom_payload(custom_payload, config)

            # 分支 B: 剧集/季 处理逻辑
            elif item_type == 'Series' or item_type == 'Season':
                # 查库获取 season_number
                if item_type == 'Season' and season_number is None:
                    season_info = media_db.get_media_details(str(tmdb_id), 'Season')
                    if season_info:
                        season_number = season_info.get('season_number')
                        parent_id = season_info.get('parent_series_tmdb_id')
                        if parent_id:
                            tmdb_id = parent_id # ★ 关键：切换为父剧集 ID
                
                # 情况 1: 分季订阅 (有季号)
                if season_number is not None:
                    series_name = media_db.get_series_title_by_tmdb_id(str(tmdb_id))
                    mp_payload = {
                        "name": series_name,
                        "tmdbid": int(tmdb_id),
                        "type": "电视剧",
                        "season": int(season_number)
                    }
                    
                    # ★★★ 初始待定判断 ★★★
                    is_pending, fake_total = should_mark_as_pending(int(tmdb_id), int(season_number), tmdb_api_key)
                    if is_pending:
                        mp_payload["status"] = "P"
                        mp_payload["total_episode"] = fake_total
                        logger.info(f"  🛡️ [自动待定] 手动订阅《{series_name}》S{season_number} 符合条件，初始状态将设为 '待定(P)'。")
                    
                    # 如果是洗版/缺集来源，或者全局开关开启，强制 best_version=1
                    if use_gap_fill_resubscribe or is_gap_or_resub:
                        logger.info(f"  ➜ 检测到洗版/缺集来源或全局开关，为《{series_name}》第 {season_number} 季启用洗版模式。")
                        mp_payload["best_version"] = 1
                    elif "best_version" not in mp_payload:
                        if check_series_completion(int(tmdb_id), tmdb_api_key, season_number=season_number, series_name=series_name):
                                mp_payload["best_version"] = 1
                    
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

                # 情况 2: 整剧订阅 (没有季号)
                elif item_type == 'Series':
                    # 使用新逻辑函数替代 smart_subscribe_series
                    success = _subscribe_full_series_with_logic(
                        tmdb_id=int(tmdb_id),
                        series_name=item_title_for_log,
                        config=config,
                        tmdb_api_key=tmdb_api_key,
                        use_gap_fill_resubscribe=use_gap_fill_resubscribe
                    )
                
                else:
                    logger.error(f"  ➜ 订阅失败：季《{item_title_for_log}》缺少季号信息。")
                    continue
            
            # 分支 C: 电影 处理逻辑
            elif item_type == 'Movie':
                if not is_movie_subscribable(int(tmdb_id), tmdb_api_key, config): 
                    logger.warning(f"  ➜ 电影《{item_title_for_log}》不满足发行日期条件，跳过订阅。")
                    continue
                mp_payload = {"name": item_title_for_log, "tmdbid": int(tmdb_id), "type": "电影"}
                if is_gap_or_resub:
                    logger.info(f"  ➜ 检测到洗版来源，为电影《{item_title_for_log}》启用洗版模式。")
                    mp_payload["best_version"] = 1
                success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            # --- 统一的后续处理 ---
            if success:
                logger.info(f"  ✅ 《{item_title_for_log}》订阅成功！")
                settings_db.decrement_subscription_quota()
                
                # 更新状态时，尽量使用查询用的 ID (query_id)，确保能更新到正确的 Season 记录
                target_id_for_update = query_id if (item_type == 'Season' and 'query_id' in locals()) else str(tmdb_id)
                
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
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在加载订阅策略...")
    config = config_manager.APP_CONFIG
    
    # 1. 加载策略配置 (优先从数据库读取，如果没有则使用默认值)
    strategy_config = settings_db.get_setting('subscription_strategy_config') or {}
    
    # 默认策略参数
    movie_protection_days = int(strategy_config.get('movie_protection_days', 180))    # 默认半年新片保护
    movie_search_window = int(strategy_config.get('movie_search_window_days', 1))     # 默认搜索1天
    movie_pause_days = int(strategy_config.get('movie_pause_days', 7))                # 默认暂停7天
    
    # 兼容旧的全局开关 (如果用户还没配置过策略，可以回退读取 config.ini，或者直接用默认值)
    if not config.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("  ➜ 订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        # 读取配置
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        use_gap_fill_resubscribe = watchlist_cfg.get('gap_fill_resubscribe', False)
        # ======================================================================
        # 阶段 1 - 清理超时订阅 
        # ======================================================================
        if movie_search_window > 0:
            logger.info(f"  ➜ 正在检查超过 {movie_search_window} 天仍未入库的订阅...")
            task_manager.update_status_from_thread(2, "正在清理超时订阅...")
            
            stale_items = request_db.get_stale_subscribed_media(movie_search_window, movie_protection_days)
            
            if stale_items:
                logger.warning(f"  ➜ 发现 {len(stale_items)} 个超时订阅，将尝试取消它们。")
                cancelled_ids_map = {} # 用于批量更新数据库状态 { 'Movie': [...], 'Series': [...], ... }
                cancelled_for_report = []

                for item in stale_items:
                    tmdb_id_to_cancel = item['tmdb_id']
                    item_type = item['item_type']
                    season_to_cancel = None

                    # 特殊处理季：取消时需要使用父剧集的ID
                    if item_type == 'Season':
                        if item['parent_series_tmdb_id']:
                            tmdb_id_to_cancel = item['parent_series_tmdb_id']
                            season_to_cancel = item['season_number']
                        else:
                            logger.error(f"  ➜ 无法取消季《{item['title']}》，因为它缺少父剧集ID。")
                            continue
                    
                    # 调用 MoviePilot 取消接口
                    success = moviepilot.cancel_subscription(
                        tmdb_id=tmdb_id_to_cancel,
                        item_type=item_type,
                        config=config,
                        season=season_to_cancel
                    )
                    
                    if success:
                        # 如果取消成功，记录下来以便稍后批量更新数据库
                        if item_type not in cancelled_ids_map:
                            cancelled_ids_map[item_type] = []
                        cancelled_ids_map[item_type].append(item['tmdb_id']) # ★ 注意：这里用原始的 tmdb_id
                        cancelled_for_report.append(f"《{item['title']}》")

                # 批量更新数据库状态
                for item_type, tmdb_ids in cancelled_ids_map.items():
                    if tmdb_ids:
                        # 设置忽略状态
                        request_db.set_media_status_ignored(
                            tmdb_ids=tmdb_ids, 
                            item_type=item_type,
                            source={"type": "auto_ignored", "reason": "stale_subscription"},
                            ignore_reason="订阅超时"
                        )
                
                # 如果有成功取消的，给管理员发个通知
                if cancelled_for_report:
                    admin_chat_ids = user_db.get_admin_telegram_chat_ids()
                    if admin_chat_ids:
                        items_list_str = "\n".join([f"· `{item}`" for item in cancelled_for_report])
                        message_text = (f"🚫 *自动取消了 {len(cancelled_for_report)} 个超时订阅*\n\n"
                                        f"下列项目因超过 {movie_search_window} 天未入库而被自动取消：\n{items_list_str}")
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
                    
                    # ★★★ 修改：直接更新状态为 'S' (Stop/Pause) ★★★
                    if moviepilot.update_subscription_status(int(tmdb_id), None, 'S', config):
                        paused_ids.append(tmdb_id)
                    else:
                        logger.warning(f"    - 《{title}》暂停失败 (MP请求错误或订阅不存在)。")
                
                if paused_ids:
                    request_db.update_movie_status_paused(paused_ids, pause_days=movie_pause_days)
                    logger.info(f"  💤 成功暂停 {len(paused_ids)} 部暂无资源的新片 (MP状态->S)。")
        
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

            # 2.1 检查配额
            if settings_db.get_subscription_quota() <= 0:
                quota_exhausted = True
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            # 2.2 检查发行日期 (只对电影检查，剧集由 smart_subscribe 处理)
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

            # 2.3 执行订阅
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
                    success = _subscribe_full_series_with_logic(
                        tmdb_id=int(item['tmdb_id']),
                        series_name=item['title'],
                        config=config,
                        tmdb_api_key=tmdb_api_key,
                        use_gap_fill_resubscribe=use_gap_fill_resubscribe
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
                        
                        # 初始待定判断 
                        is_pending, fake_total = should_mark_as_pending(int(parent_tmdb_id), season_num, tmdb_api_key)
                        if is_pending:
                            mp_payload["status"] = "P"
                            mp_payload["total_episode"] = fake_total
                            logger.info(f"  🛡️ [自动待定] 新订阅《{series_name}》S{season_num} 符合条件，初始状态将设为 '待定(P)'。")

                        # 1. 检查具体的来源类型
                        is_explicit_resub = any(source.get('type') == 'resubscribe' for source in sources)
                        is_gap_scan = any(source.get('type') == 'gap_scan' for source in sources)
                        
                        # 2. 应用订阅策略
                        if is_explicit_resub:
                            # 情况 A: 明确的洗版规则触发 -> 强制 best_version=1
                            mp_payload["best_version"] = 1
                            logger.info(f"  ➜ 触发自动洗版规则，为《{series_name}》S{season_num} 启用洗版模式。")
                            
                        elif is_gap_scan:
                            # 情况 B: 缺集扫描触发 -> 根据配置决定是否 best_version=1
                            if use_gap_fill_resubscribe:
                                mp_payload["best_version"] = 1
                                logger.info(f"  ➜ 触发缺集扫描且配置开启，为《{series_name}》S{season_num} 启用洗版模式。")
                            else:
                                logger.info(f"  ➜ 触发缺集扫描 (配置未开启洗版)，为《{series_name}》S{season_num} 执行普通订阅。")
                                
                        elif "best_version" not in mp_payload:
                            # 情况 C: 普通订阅 -> 检查是否完结，完结则洗版
                            if check_series_completion(int(parent_tmdb_id), tmdb_api_key, season_number=season_num, series_name=series_name):
                                mp_payload["best_version"] = 1
                        
                        success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
                        # 如果待定，更新本地 DB 状态为 PENDING_METADATA
                        if is_pending:
                                watchlist_db.update_watching_status_by_tmdb_id(
                                    str(item['tmdb_id']), # 或者是 parent_tmdb_id，取决于当前上下文是季还是剧
                                    'Pending'
                                )
                                
                                # 我们还需要把父剧集设为 Watching (或 Pending)
                                if item_type == 'Season':
                                    parent_id = item.get('parent_series_tmdb_id')
                                    if parent_id:
                                        watchlist_db.add_item_to_watchlist(str(parent_id), series_name)
                    else:
                        success = False

            # 2.4 根据订阅结果更新状态和发送通知
            if success:
                logger.info(f"  ✅ 《{item['title']}》订阅成功！")
                
                # a. 将状态从 WANTED 更新为 SUBSCRIBED
                request_db.set_media_status_subscribed(
                    tmdb_ids=item['tmdb_id'], # 更新的是季/电影自己的记录
                    item_type=item_type,
                )

                # b. 扣除配额
                settings_db.decrement_subscription_quota()

                # d. 准备通知 (智能拼接通知标题)
                item_display_name = ""
                if item_type == 'Season':
                    season_num = item.get('season_number')
                    default_season_title = f"第{season_num}季" if season_num is not None else ""
                    season_display_title = item.get('season_title', default_season_title)
                    item_display_name = f"剧集《{series_name} - {season_display_title}》"
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
                        notifications_to_send[user_id].append(item['title'])
                        source_display_parts.append(f"用户请求({user_db.get_username_by_id(user_id) or user_id})")
                    elif source_type == 'actor_subscription':
                        source_display_parts.append(f"演员订阅({source.get('name', '未知')})")
                    elif source_type in ['collection', 'native_collection']:
                        source_display_parts.append(f"合集({source.get('name', '未知')})")
                    elif source_type == 'gap_scan':
                        source_display_parts.append("缺集扫描")
                    elif source_type == 'watchlist':
                        source_display_parts.append("追剧补全")
                
                source_display = ", ".join(set(source_display_parts)) or "未知来源"
                subscription_details.append({'source': source_display, 'item': item_display_name})

            else:
                logger.error(f"  ➜ 订阅《{item['title']}》失败，请检查 MoviePilot 连接或日志。")
        
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

