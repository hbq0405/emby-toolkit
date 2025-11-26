# tasks/subscriptions.py
# 智能订阅与媒体洗版任务模块
import re
import os
import json
import time
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed 

# 导入需要的底层模块和共享实例
import config_manager
import constants
import utils
import handler.emby as emby
import handler.tmdb as tmdb
import handler.moviepilot as moviepilot
import task_manager
from handler import telegram
from database import settings_db, request_db, user_db, media_db
from .helpers import is_movie_subscribable, check_series_completion

logger = logging.getLogger(__name__)

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
        use_gap_fill_resubscribe = config.get(constants.CONFIG_OPTION_GAP_FILL_RESUBSCRIBE_ENABLED, False)
        processed_count = 0

        for i, req in enumerate(subscribe_requests):
            tmdb_id = req.get('tmdb_id')
            item_type = req.get('item_type')
            item_title_for_log = req.get('title', f"ID: {tmdb_id}")

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
            
            parent_tmdb_id_for_preprocess = tmdb_id
            parent_title_for_preprocess = item_title_for_log
            preprocess_type = item_type

            # --- 逻辑分支开始 ---
            if item_type == 'Series':
                # 如果是未发行的剧集，直接跳过，保留 user_portal 设置的 PENDING_RELEASE 状态
                series_details = tmdb.get_tv_details(int(tmdb_id), tmdb_api_key)
                if series_details:
                    first_air_date = series_details.get('first_air_date')
                    if first_air_date:
                        try:
                            air_date_obj = datetime.strptime(first_air_date, '%Y-%m-%d').date()
                            if air_date_obj > date.today():
                                logger.warning(f"  ➜ 剧集《{item_title_for_log}》首播日期 ({first_air_date}) 未到，跳过订阅。")
                                continue # 跳过本次循环，success 保持为 False，不会更新数据库为 SUBSCRIBED
                        except (ValueError, TypeError):
                            pass
                # --- 路径 A: 整剧订阅 (逻辑特殊，单独处理) ---
                series_info = {"tmdb_id": int(tmdb_id), "item_name": item_title_for_log}
                success = moviepilot.smart_subscribe_series(series_info, config) is not None
            
            else:
                # --- 路径 B: 电影或季订阅 (可以统一处理) ---
                mp_payload = {}
                
                if item_type == 'Movie':
                    if not is_movie_subscribable(int(tmdb_id), tmdb_api_key, config): 
                        logger.warning(f"  ➜ 电影《{item_title_for_log}》不满足发行日期条件，跳过订阅。")
                        continue
                    mp_payload = {"name": item_title_for_log, "tmdbid": int(tmdb_id), "type": "电影"}

                elif item_type == 'Season':
                    media_info = media_db.get_media_details(tmdb_id, item_type)
                    if not media_info:
                        logger.error(f"  ➜ 订阅失败：无法在数据库中找到季 {item_title_for_log} (ID: {tmdb_id}) 的元数据。")
                        continue

                    parent_tmdb_id = media_info.get('parent_series_tmdb_id')
                    season_num = media_info.get('season_number')
                    parent_title = media_db.get_series_title_by_tmdb_id(parent_tmdb_id) or '未知剧集'

                    if not parent_tmdb_id or season_num is None:
                        logger.error(f"  ➜ 订阅失败：季 {item_title_for_log} 的父剧集信息不完整。")
                        continue
                    
                    mp_payload = {"name": parent_title, "tmdbid": int(parent_tmdb_id), "type": "电视剧", "season": int(season_num)}
                    
                    if use_gap_fill_resubscribe:
                        logger.info(f"  ➜ 检测到洗版开关已开启，为《{parent_title}》第 {season_num} 季启用洗版模式。")
                        mp_payload["best_version"] = 1

                    parent_tmdb_id_for_preprocess = parent_tmdb_id
                    parent_title_for_preprocess = parent_title
                    preprocess_type = 'Series'
                
                # ★★★ 核心修复：在这里统一调用，确保电影和季的订阅都被执行 ★★★
                if mp_payload:
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            # --- 统一的后续处理 ---
            if success:
                logger.info(f"  ✅ 《{item_title_for_log}》订阅成功！")
                settings_db.decrement_subscription_quota()
                
                request_db.set_media_status_subscribed(
                    tmdb_ids=[str(tmdb_id)],
                    item_type=item_type, 
                )
                
                processed_count += 1
            else:
                logger.error(f"  ➜ 订阅《{item_title_for_log}》失败，请检查 MoviePilot 日志。")
        
        final_message = f"手动订阅任务完成，成功处理 {processed_count}/{total_items} 个项目。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务执行完毕 ---")

    except Exception as e:
        logger.error(f"  ➜ {task_name} 任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# ★★★ 自动订阅任务 ★★★
def task_auto_subscribe(processor):
    """
    【V2 - 统一订阅处理器】
    - 唯一的职责：处理 media_metadata 表中所有状态为 'WANTED' 的媒体项。
    - 在这里统一进行配额、发行日期检查，并执行订阅。
    """
    task_name = "统一订阅处理"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    task_manager.update_status_from_thread(0, "正在启动统一订阅处理器...")
    config = config_manager.APP_CONFIG
    if not config.get(constants.CONFIG_OPTION_AUTOSUB_ENABLED):
        logger.info("  ➜ 订阅总开关未开启，任务跳过。")
        task_manager.update_status_from_thread(100, "任务跳过：总开关未开启")
        return

    try:
        use_gap_fill_resubscribe = config.get(constants.CONFIG_OPTION_GAP_FILL_RESUBSCRIBE_ENABLED, False)
        # ======================================================================
        # 阶段 1 - 清理超时订阅 
        # ======================================================================
        cancel_threshold_days = config.get(constants.CONFIG_OPTION_AUTOCANCEL_SUBSCRIBED_DAYS, 0)
        
        if cancel_threshold_days > 0:
            logger.info(f"  ➜ 正在检查超过 {cancel_threshold_days} 天仍未入库的订阅...")
            task_manager.update_status_from_thread(2, "正在清理超时订阅...")
            
            stale_items = request_db.get_stale_subscribed_media(cancel_threshold_days)
            
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
                                        f"下列项目因超过 {cancel_threshold_days} 天未入库而被自动取消：\n{items_list_str}")
                        for admin_id in admin_chat_ids:
                            telegram.send_telegram_message(admin_id, message_text, disable_notification=True)
            else:
                logger.info("  ➜ 未发现超时订阅。")

        # ======================================================================
        # 阶段 2 - 执行常规订阅 
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
            best_version_flag = None

            if item_type == 'Movie':
                mp_payload = {"name": item['title'], "tmdbid": int(item['tmdb_id']), "type": "电影"}
                success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            elif item_type == 'Series':
                series_info = {"tmdb_id": int(item['tmdb_id']), "item_name": item['title']}
                success = moviepilot.smart_subscribe_series(series_info, config) is not None

            #  处理季订阅的专属逻辑 
            elif item_type == 'Season':
                parent_tmdb_id = item.get('parent_series_tmdb_id')
                season_num = item.get('season_number')
                
                # 不再相信传入的 title，而是主动查询父剧集标题
                series_name = media_db.get_series_title_by_tmdb_id(parent_tmdb_id)

                # 如果因为某种原因查不到父剧名，尝试从标题解析，最后才用原始标题
                if not series_name:
                    raw_title = item.get('title', '')
                    # 利用 utils 解析出不带季号的干净名字
                    parsed_name, _ = utils.parse_series_title_and_season(raw_title)
                    series_name = parsed_name if parsed_name else raw_title

                if parent_tmdb_id and season_num is not None:
                    logger.info(f"  ➜ 检测到季订阅请求：为剧集《{series_name}》(ID: {parent_tmdb_id}) 订阅第 {season_num} 季。")
                    mp_payload = {
                        "name": series_name,
                        "tmdbid": int(parent_tmdb_id),
                        "type": "电视剧",
                        "season": season_num
                    }
                    sources = item.get('subscription_sources_json', [])
                    is_from_gap_scan = any(source.get('type') == 'gap_scan' for source in sources)
                    
                    if is_from_gap_scan and use_gap_fill_resubscribe:
                        logger.info(f"  ➜ 检测到缺集扫描来源，且洗版开关已开启，为《{series_name}》第 {season_num} 季启用洗版模式。")
                        mp_payload["best_version"] = 1

                    elif "best_version" not in mp_payload:
                        # 如果已完结，则开启洗版模式
                        if check_series_completion(
                            int(parent_tmdb_id), 
                            tmdb_api_key, 
                            season_number=season_num, 
                            series_name=series_name
                        ):
                            mp_payload["best_version"] = 1
                    
                    success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
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
                    if source_type == 'user_request' and (user_id := source.get('user_id')):
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
            header = f"✅ *统一订阅任务完成，成功处理 {len(subscription_details)} 项:*"
            
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

