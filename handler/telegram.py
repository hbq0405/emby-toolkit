# 文件: handler/telegram.py
import json
import threading
import extensions
import requests
import logging
from datetime import datetime
from config_manager import APP_CONFIG, get_proxies_for_requests
from handler.emby import get_emby_item_details
from database import user_db, request_db, media_db
from database.connection import get_db_connection
import constants

logger = logging.getLogger(__name__)

def _format_episode_ranges(episode_list: list) -> str:
    """
    辅助函数：将 [(season, episode), ...] 转换为易读的范围字符串。
    输入: [(1, 1), (1, 2), (1, 3), (1, 5)]
    输出: "S01E01-E03, S01E05"
    """
    if not episode_list:
        return ""
    
    # 1. 按季分组
    season_map = {}
    for s, e in episode_list:
        season_map.setdefault(s, []).append(e)
    
    final_parts = []
    
    # 2. 按季排序处理
    for season in sorted(season_map.keys()):
        episodes = sorted(list(set(season_map[season]))) # 去重并排序
        if not episodes: continue
        
        # 3. 查找连续区间
        ranges = []
        start = episodes[0]
        prev = episodes[0]
        
        for ep in episodes[1:]:
            if ep == prev + 1:
                prev = ep
            else:
                # 结算上一段
                if start == prev:
                    ranges.append(f"E{start:02d}")
                else:
                    ranges.append(f"E{start:02d}-E{prev:02d}")
                start = ep
                prev = ep
        
        # 结算最后一段
        if start == prev:
            ranges.append(f"E{start:02d}")
        else:
            ranges.append(f"E{start:02d}-E{prev:02d}")
        
        # 4. 组装当前季的字符串
        for r in ranges:
            final_parts.append(f"S{season:02d}{r}")
            
    return ", ".join(final_parts)

def escape_markdown(text: str) -> str:
    """
    Helper function to escape characters for Telegram's MarkdownV2.
    只应该用于转义从外部API获取的、内容不可控的文本部分。
    """
    if not isinstance(text, str):
        return ""
    # 根据 Telegram Bot API 文档，这些字符需要转义: _ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

# --- 通用的 Telegram 文本消息发送函数 ---
def send_telegram_message(chat_id: str, text: str, disable_notification: bool = False, reply_markup: dict = None):
    """通用的 Telegram 文本消息发送函数，支持内联键盘。"""
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id:
        return False
    
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': final_chat_id,
        'text': text, 
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True,
        'disable_notification': disable_notification,
    }
    
    # 支持传入键盘标记
    if reply_markup:
        payload['reply_markup'] = reply_markup

    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=15, proxies=proxies)
        if response.status_code == 200:
            logger.info(f"  ➜ 成功发送 Telegram 文本消息至 Chat ID: {final_chat_id}")
            return True
        else:
            logger.error(f"  ➜ 发送 Telegram 文本消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"  ➜ 发送 Telegram 文本消息时发生网络请求错误: {e}")
        return False

# --- 通用的 Telegram 图文消息发送函数 ---
def send_telegram_photo(chat_id: str, photo_url: str, caption: str, disable_notification: bool = False):
    """通用的 Telegram 图文消息发送函数。"""
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id or not photo_url:
        return False
    
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    payload = {
        'chat_id': final_chat_id,
        'photo': photo_url,
        'caption': caption, 
        'parse_mode': 'MarkdownV2',
        'disable_notification': disable_notification,
    }
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=30, proxies=proxies)
        if response.status_code == 200:
            logger.debug(f"  ➜ 成功发送 Telegram 图文消息至 Chat ID: {final_chat_id}")
            return True
        else:
            logger.error(f"  ➜ 发送 Telegram 图文消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"  ➜ 发送 Telegram 图文消息时发生网络请求错误: {e}")
        return False
    
# --- 全能的通知函数 ---
def send_media_notification(item_details: dict, notification_type: str = 'new', new_episode_ids: list = None):
    """
    【全能媒体通知函数】
    根据传入的媒体详情，自动获取图片、组装消息并发送给频道和订阅者。
    """
    logger.info(f"  ➜ 准备为 '{item_details.get('Name')}' 发送 '{notification_type}' 类型的 Telegram 通知...")
    
    try:
        # --- 1. 准备基础信息 ---
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_id = item_details.get("Id")
        item_name_for_log = item_details.get("Name", f"ID:{item_id}")
        year = item_details.get("ProductionYear", "")
        title = f"{item_name_for_log} ({year})" if year else item_name_for_log
        overview = item_details.get("Overview", "暂无剧情简介。")
        if len(overview) > 200:
            overview = overview[:200] + "..."
            
        item_type = item_details.get("Type")

        escaped_title = escape_markdown(title)
        escaped_overview = escape_markdown(overview)

        # --- 2. 准备剧集信息 (如果适用) ---
        episode_info_text = ""
        if item_type == "Series" and new_episode_ids:
            emby_url = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
            api_key = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
            user_id = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID)

            # 收集原始数据而不是直接格式化字符串，这样我们可以在格式化字符串时使用
            raw_episodes = [] 
            for ep_id in new_episode_ids:
                detail = get_emby_item_details(ep_id, emby_url, api_key, user_id, fields="IndexNumber,ParentIndexNumber")
                if detail:
                    season_num = detail.get("ParentIndexNumber", 0)
                    episode_num = detail.get("IndexNumber", 0)
                    # 收集元组 (季号, 集号)
                    raw_episodes.append((season_num, episode_num))
            
            # 调用辅助函数生成合并后的字符串
            if raw_episodes:
                formatted_episodes = _format_episode_ranges(raw_episodes)
                episode_info_text = f"🎞️ *集数*: `{formatted_episodes}`\n"

        # --- 3. 调用本地数据库获取图片路径 ---
        photo_url = None
        try:
            db_info = media_db.get_notification_media_info_by_emby_id(item_id)
            if db_info:
                # 优先横幅，其次竖图，如果是分集没图，找它爹(剧集)要横幅
                path = db_info.get('backdrop_path') or db_info.get('poster_path')
                if not path and db_info.get('item_type') == 'Episode':
                    path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                if path:
                    photo_url = f"https://image.tmdb.org/t/p/w780{path}"
        except Exception as e:
            logger.error(f"  ➜ [通知] 从本地数据库获取图片信息时出错: {e}", exc_info=True)

        # =================================================================
        # ★★★ 查询该项目是否被标记为【待复核】 ★★★
        # =================================================================
        needs_review = False
        review_reason = ""
        try:
            # 核心处理器中，分集的报错是挂在父剧集 ID 下的，所以这里要做个转换
            check_id = str(item_id)
            if item_type == 'Episode' and item_details.get('SeriesId'):
                check_id = str(item_details.get('SeriesId'))
                
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT reason FROM failed_log WHERE item_id = %s", (check_id,))
                    row = cursor.fetchone()
                    if row:
                        needs_review = True
                        review_reason = row['reason']
        except Exception as e:
            logger.error(f"  ➜ [通知] 查询待复核状态失败: {e}")
        
        # --- 4. 组装最终的通知文本 (Caption) ---
        notification_title_map = {
            'new': '✨ 入库成功',
            'update': '🔄 已更新'
        }
        notification_title = notification_title_map.get(notification_type, '🔔 状态更新')
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        media_icon = "🎬" if item_type == "Movie" else "📺"
        
        # ★★★ 构建待复核警告文本 ★★★
        review_warning = ""
        if needs_review:
            escaped_reason = escape_markdown(review_reason)
            review_warning = (
                f"\n\n⚠️ *系统提示*: 本次处理被标记为【待复核】\n"
                f"🔍 *原因*: {escaped_reason}\n"
                f"💡 _请前往 WebUI 手动介入处理_"
            )

        # ★★★ 修改：将 review_warning 追加到 caption 尾部 ★★★
        caption = (
            f"{media_icon} *{escaped_title}* {notification_title}\n\n"
            f"{episode_info_text}"
            f"⏰ *时间*: `{current_time}`\n"
            f"📝 *剧情*: {escaped_overview}"
            f"{review_warning}"
        )
        
        # --- 5. 查询订阅者 ---
        subscribers = request_db.get_subscribers_by_tmdb_id(tmdb_id, item_type) if tmdb_id else []
        subscriber_chat_ids = {
            user_db.get_user_telegram_chat_id(sub.get('user_id')) 
            for sub in subscribers 
            if sub.get('type') == 'user_request' and sub.get('user_id')
        }
        subscriber_chat_ids = {chat_id for chat_id in subscriber_chat_ids if chat_id}

        # --- 6 & 7. 发送全局和管理员通知 ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        
        # ★ 严格判定：只有勾选了“入库通知”，才允许向频道和管理员发送非订阅类的公播消息
        if 'library_new' in notify_types:
            # A. 发送给频道
            if global_channel_id:
                logger.info(f"  ➜ 正在向全局频道 {global_channel_id} 发送通知...")
                if photo_url:
                    send_telegram_photo(global_channel_id, photo_url, caption)
                else:
                    send_telegram_message(global_channel_id, caption)

            # B. 发送给管理员
            all_admin_chat_ids = set(user_db.get_admin_telegram_chat_ids())
            if all_admin_chat_ids:
                subscriber_id_set = {str(sid) for sid in subscriber_chat_ids}
                for admin_chat_id in all_admin_chat_ids:
                    # 去重：不发给频道，也不发给已经是订阅者的管理员
                    if str(admin_chat_id) == str(global_channel_id) or str(admin_chat_id) in subscriber_id_set:
                        continue
                    
                    logger.info(f"  ➜ 正在向管理员 {admin_chat_id} 发送全局入库通知...")
                    if photo_url:
                        send_telegram_photo(admin_chat_id, photo_url, caption)
                    else:
                        send_telegram_message(admin_chat_id, caption)
        else:
            logger.debug(f"  ➜ [通知] '入库通知' 设置为关闭，跳过频道和管理员的全局广播。")

        # --- 8. 发送个人订阅到货通知 ---
        if subscriber_chat_ids:
            personal_caption_map = {
                'new': f"✅ *您的订阅已入库*\n\n{caption}",
                'update': f"🔄 *您的订阅已更新*\n\n{caption}"
            }
            personal_caption = personal_caption_map.get(notification_type, caption)
            
            for chat_id in subscriber_chat_ids:
                if chat_id == global_channel_id: continue
                logger.info(f"  ➜ 正在向订阅者 {chat_id} 发送个人通知...")
                if photo_url:
                    send_telegram_photo(chat_id, photo_url, personal_caption)
                else:
                    send_telegram_message(chat_id, personal_caption)
            
    except Exception as e:
        logger.error(f"  ➜ 发送媒体通知时发生严重错误: {e}", exc_info=True)

def send_transfer_success_notification(task: dict):
    """发送频道监听转存成功的通知"""
    try:
        title = task.get('title', '未知标题')
        year = task.get('year', '')
        item_type = task.get('item_type', 'movie')
        season_number = task.get('season_number')
        episode_number = task.get('episode_number')
        is_pack = task.get('is_pack', False)
        tmdb_id = task.get('tmdb_id')

        display_title = f"{title} ({year})" if year else title
        escaped_title = escape_markdown(display_title)
        
        type_str = "🎬 电影" if item_type == 'movie' else "📺 剧集"
        
        season_info = ""
        if item_type == 'tv':
            if season_number is not None:
                if episode_number is not None:
                    if is_pack:
                        season_info = f"📦 *季集*: `S{season_number:02d} 包含 {episode_number} 集`\n"
                    else:
                        season_info = f"📦 *季集*: `S{season_number:02d}E{episode_number:02d}`\n"
                else:
                    season_info = f"📦 *季集*: `S{season_number:02d}`\n"

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 尝试获取 TMDB 图片和评分
        photo_url = None
        rating = ""
        overview_text = "" 
        
        if tmdb_id:
            # ★ 转存时的 ID 绝对是纯种 TMDB ID
            base_tmdb_id = str(tmdb_id).strip()
            
            if base_tmdb_id.isdigit():
                try:
                    from database import media_db
                    # 极速本地盲查，不需要管是电影还是剧集
                    db_info = media_db.get_notification_media_info_by_tmdb_id(base_tmdb_id)
                    
                    if db_info:
                        # 优先横幅，如果没有再找竖图
                        path = db_info.get('backdrop_path') or db_info.get('poster_path')
                        # 如果拿到的是单集或季，且没图，向父剧集借图
                        if not path and db_info.get('item_type') in ['Episode', 'Season']:
                            path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                            
                        if path:
                            photo_url = f"https://image.tmdb.org/t/p/w780{path}"
                            
                        vote_average = db_info.get('rating')
                        if vote_average:
                            rating = f"✨ *评分*: `{vote_average:.1f}/10`\n"
                            
                        raw_overview = db_info.get('overview', '')
                        if raw_overview:
                            if len(raw_overview) > 200:
                                raw_overview = raw_overview[:200] + "..."
                            overview_text = f"📝 *剧情*: {escape_markdown(raw_overview)}\n"
                except Exception as e:
                    logger.error(f"  ➜ 获取转存通知图片(本地查库)失败: {e}")

        # 组装卡片文本
        # ★ 区分是 115 转存还是离线下载
        action_title = "📥 *离线任务已提交*" if task.get('is_offline') else "📥 *转存成功*"
        
        caption = (
            f"{action_title}\n"
            f"*{escaped_title}*\n\n"
            f"{season_info}"
            f"🕒 *时间*: `{current_time}`\n"
            f"🎭 *类别*: {type_str}\n"
            f"{rating}"
            f"{overview_text}" 
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            if photo_url:
                send_telegram_photo(target, photo_url, caption)
            else:
                send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送转存成功通知时出错: {e}", exc_info=True)

def send_playback_notification(data: dict):
    """发送图文并茂的播放状态通知 (附带剧集或电影海报，注入灵魂版)"""
    try:
        event_type = data.get("Event")
        user_name = data.get("User", {}).get("Name", "未知用户")
        device_name = data.get("Session", {}).get("DeviceName", "未知设备")
        client_name = data.get("Session", {}).get("Client", "未知客户端")
        
        item = data.get("Item", {})
        original_item_name = item.get("Name", "未知项目")
        original_item_type = item.get("Type", "Unknown")
        item_id = item.get("Id")
        
        # 优先从 Emby Webhook 数据中提取剧情
        raw_overview = item.get("Overview", "")
        
        display_item_name = original_item_name
        if original_item_type == "Episode" and item.get("SeriesName"):
            display_item_name = f"{item.get('SeriesName')} - {original_item_name}"
            
        # --- 本地数据库提取图片和剧情兜底 (极速，无网络请求依赖) ---
        photo_url = None
        if item_id:
            db_info = media_db.get_notification_media_info_by_emby_id(item_id)
            if db_info:
                # 优先横幅，如果没有再用竖图。如果是分集没图，自动用父剧集的横幅图
                path = db_info.get('backdrop_path') or db_info.get('poster_path')
                if not path and db_info.get('item_type') == 'Episode':
                    path = db_info.get('parent_backdrop_path') or db_info.get('parent_poster_path')
                if path:
                    photo_url = f"https://image.tmdb.org/t/p/w780{path}"
                
                # ★ 新增：如果 Emby 没传剧情，从本地数据库兜底获取
                if not raw_overview:
                    raw_overview = db_info.get('overview', '')
        
        # 格式化剧情文本 (限制长度防刷屏)
        overview_text = ""
        if raw_overview:
            if len(raw_overview) > 150:
                raw_overview = raw_overview[:150] + "..."
            overview_text = f"\n📝 *剧情*: {escape_markdown(raw_overview)}"
                    
        action_map = {
            "playback.start": "▶️ 开始播放",
            "playback.pause": "⏸ 暂停播放",
            "playback.stop": "⏹ 停止播放"
        }
        action_str = action_map.get(event_type, "🎬 播放状态改变")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ★ 修改：将剧情变量追加到卡片末尾
        caption = (
            f"{action_str}\n\n"
            f"👤 *用户*: `{escape_markdown(user_name)}`\n"
            f"🎬 *媒体*: *{escape_markdown(display_item_name)}*\n"
            f"📱 *设备*: `{escape_markdown(device_name)} ({escape_markdown(client_name)})`\n"
            f"🕒 *时间*: `{escape_markdown(current_time)}`"
            f"{overview_text}" 
        )
        
        # --- 收集发送目标 (频道 + 所有管理员) ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            if aid:
                targets.add(str(aid))

        if not targets:
            logger.debug("  ➜ [播放通知] 未配置接收人 (频道或管理员均为空)，跳过发送。")
            return

        # --- 遍历发送 (移除所有静音参数，让通知发出清脆的叮咚声！) ---
        for target in targets:
            if photo_url:
                send_telegram_photo(target, photo_url, caption)
            else:
                send_telegram_message(target, caption)
                
    except Exception as e:
        logger.error(f"  ➜ 组装/发送播放图文通知时发生异常: {e}")

def send_unrecognized_notification(file_name: str, reason: str = "未匹配到有效的 TMDb 数据"):
    """
    发送文件识别失败/打入未识别目录的 Telegram 通知
    """
    try:
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        # 检查用户是否在设置中勾选了“识别失败”通知
        if 'recognize_fail' not in notify_types:
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        escaped_file_name = escape_markdown(file_name)
        escaped_reason = escape_markdown(reason)

        caption = (
            f"⚠️ *识别失败通知*\n\n"
            f"📁 *文件名*: `{escaped_file_name}`\n"
            f"❓ *原因*: {escaped_reason}\n"
            f"🕒 *时间*: `{current_time}`\n\n"
            f"💡 _文件已被移入「未识别」目录，请前往 WebUI 手动纠错。_"
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送识别失败通知时出错: {e}", exc_info=True)

def send_intercept_notification(file_names, reason: str):
    """
    发送洗版拦截/质检不合格的 Telegram 通知 (支持多文件聚合)
    """
    try:
        notify_types = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
        # 检查用户是否在设置中勾选了“拦截通知”
        if 'intercept_notify' not in notify_types:
            return

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        escaped_reason = escape_markdown(reason)

        # 兼容单文件字符串传入
        if isinstance(file_names, str):
            file_names = [file_names]
            
        count = len(file_names)
        if count == 1:
            name_str = f"`{escape_markdown(file_names[0])}`"
        else:
            # 最多显示 5 个文件名，防止消息过长刷屏
            display_names = file_names[:5]
            name_str = "\n".join([f"• `{escape_markdown(n)}`" for n in display_names])
            if count > 5:
                name_str += f"\n_{escape_markdown(f'...等共 {count} 个文件')}_"
            name_str = f"共 {count} 个文件:\n{name_str}"

        caption = (
            f"⛔ *洗版拦截通知*\n\n"
            f"📁 *拦截文件*: {name_str}\n"
            f"🚫 *原因*: {escaped_reason}\n"
            f"🕒 *时间*: `{current_time}`\n\n"
            f"💡 _文件未达到优先级标准，已被标记「质检不合格」。_"
        )

        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        admin_ids = set(user_db.get_admin_telegram_chat_ids())

        targets = set()
        if global_channel_id:
            targets.add(str(global_channel_id))
        for aid in admin_ids:
            targets.add(str(aid))

        for target in targets:
            send_telegram_message(target, caption)

    except Exception as e:
        logger.error(f"  ➜ 发送洗版拦截通知时出错: {e}", exc_info=True)

# ======================================================================
# ★★★ Telegram 机器人交互监听 (长轮询) ★★★
# ======================================================================
import re
import time
import threading
from handler.p115_service import P115Service

# 全局变量控制轮询线程
_tg_polling_thread = None
_tg_polling_active = False

# Telegram 资源搜索会话：chat_id -> {stage, results/resources, media, created_at}
_tg_resource_search_sessions = {}
_tg_resource_search_lock = threading.Lock()
_TG_RESOURCE_SEARCH_TTL = 15 * 60
# TMDb 候选仍保持 10 个；资源结果单页 10 个，但最多收集 50 个用于翻页。
_TG_TMDB_SEARCH_LIMIT = 10
_TG_RESOURCE_PAGE_SIZE = 10
_TG_RESOURCE_COLLECT_LIMIT = 50
_TG_RESOURCE_SEARCH_LIMIT = _TG_TMDB_SEARCH_LIMIT  # 兼容旧变量名


def _tg_send_plain(chat_id: str, text: str, disable_notification: bool = False, reply_markup: dict = None):
    """发送普通文本；统一转义 MarkdownV2，避免外部片名/资源名导致 TG 发送失败。"""
    return send_telegram_message(
        chat_id,
        escape_markdown(str(text or "")),
        disable_notification=disable_notification,
        reply_markup=reply_markup,
    )


def _tg_get_tmdb_api_key() -> str:
    """兼容不同版本 constants 命名，读取 TMDb API Key。"""
    constant_names = [
        "CONFIG_OPTION_TMDB_API_KEY",
        "CONFIG_OPTION_TMDB_APIKEY",
        "CONFIG_OPTION_TMDB_KEY",
    ]
    for name in constant_names:
        config_key = getattr(constants, name, None)
        if config_key:
            value = APP_CONFIG.get(config_key)
            if value:
                return str(value).strip()

    fallback_keys = [
        "tmdb_api_key",
        "tmdb_apikey",
        "TMDB_API_KEY",
        "tmdb_key",
    ]
    for key in fallback_keys:
        value = APP_CONFIG.get(key)
        if value:
            return str(value).strip()

    return ""


def _tg_normalize_digits(text: str) -> str:
    return str(text or "").translate(str.maketrans("０１２３４５６７８９", "0123456789"))


def _tg_parse_selection_text(text: str):
    """解析“1”“第1个”“2 s3”“2 第3季”这类回复，返回 (序号, 季号)。
    注意：TG 手动影巢搜索不再按季过滤，季号仅兼容旧输入。
    """
    normalized = _tg_normalize_digits(text).strip()
    match = re.match(
        r"^(?:第\s*)?(\d{1,2})(?:\s*(?:个|项|号))?(?:\s*(?:s|S|第)?\s*(\d{1,2})\s*(?:季)?)?$",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return None, None

    number = int(match.group(1))
    season = int(match.group(2)) if match.group(2) else None
    return number, season


def _tg_is_session_expired(session: dict) -> bool:
    if not session:
        return True
    return (time.time() - float(session.get("created_at") or 0)) > _TG_RESOURCE_SEARCH_TTL


def _tg_get_session(chat_id: str):
    with _tg_resource_search_lock:
        session = _tg_resource_search_sessions.get(str(chat_id))
        if _tg_is_session_expired(session):
            _tg_resource_search_sessions.pop(str(chat_id), None)
            return None
        return session


def _tg_set_session(chat_id: str, session: dict):
    session["created_at"] = time.time()
    with _tg_resource_search_lock:
        _tg_resource_search_sessions[str(chat_id)] = session


def _tg_clear_session(chat_id: str):
    with _tg_resource_search_lock:
        _tg_resource_search_sessions.pop(str(chat_id), None)


def _tg_build_number_keyboard(prefix: str, count: int) -> dict:
    keyboard = []
    row = []
    for idx in range(1, min(count, _TG_TMDB_SEARCH_LIMIT) + 1):
        row.append({"text": f"{idx:02d}", "callback_data": f"{prefix}:{idx}"})
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "取消", "callback_data": "tg_search_cancel"}])
    return {"inline_keyboard": keyboard}


def _tg_clamp_page(page: int, total_count: int) -> int:
    try:
        page = int(page)
    except Exception:
        page = 0
    page_count = max(1, (max(0, int(total_count or 0)) + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    return max(0, min(page, page_count - 1))


def _tg_slice_resource_page(resources: list, page: int) -> list:
    page = _tg_clamp_page(page, len(resources or []))
    start = page * _TG_RESOURCE_PAGE_SIZE
    end = start + _TG_RESOURCE_PAGE_SIZE
    return list(resources or [])[start:end]


def _tg_build_resource_page_keyboard(total_count: int, page: int) -> dict:
    total_count = int(total_count or 0)
    page = _tg_clamp_page(page, total_count)
    page_count = max(1, (total_count + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    start = page * _TG_RESOURCE_PAGE_SIZE
    end = min(start + _TG_RESOURCE_PAGE_SIZE, total_count)

    keyboard = []
    row = []
    for idx in range(start + 1, end + 1):
        row.append({"text": f"{idx:02d}", "callback_data": f"tg_hdhive:{idx}"})
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav_row = []
    if page > 0:
        nav_row.append({"text": "⬅️ 上一页", "callback_data": f"tg_res_page:{page - 1}"})
    nav_row.append({"text": f"{page + 1}/{page_count}", "callback_data": "tg_res_page:noop"})
    if page < page_count - 1:
        nav_row.append({"text": "下一页 ➡️", "callback_data": f"tg_res_page:{page + 1}"})
    keyboard.append(nav_row)
    keyboard.append([{"text": "取消", "callback_data": "tg_search_cancel"}])
    return {"inline_keyboard": keyboard}


def _tg_media_type_label(media_type: str) -> str:
    return "电影" if media_type == "movie" else "剧集"


def _tg_tmdb_title(item: dict) -> str:
    return item.get("title") or item.get("name") or item.get("original_title") or item.get("original_name") or "未知标题"


def _tg_tmdb_year(item: dict) -> str:
    date_text = item.get("release_date") or item.get("first_air_date") or ""
    return str(date_text)[:4] if date_text else "未知年份"


def _tg_tmdb_result_line(index: int, item: dict) -> str:
    media_type = item.get("media_type") or "movie"
    title = _tg_tmdb_title(item)
    year = _tg_tmdb_year(item)
    tmdb_id = item.get("id") or "-"
    rating = item.get("vote_average")
    rating_text = f" / 评分 {float(rating):.1f}" if isinstance(rating, (int, float)) and rating else ""
    return f"{index}. [{_tg_media_type_label(media_type)}] {title} ({year}) / TMDb {tmdb_id}{rating_text}"


def _tg_format_tmdb_results(query: str, results: list) -> str:
    lines = [
        f"🔎 TMDb 搜索 | {query}",
        "━━━━━━━━━━━━━━",
        "↩️ 回复序号选择影片/剧集，或点击下方按钮。",
        "📺 剧集资源将全量返回，不按季过滤；需要哪一季请在资源备注里肉眼挑选。",
        "🚫 输入 取消 可结束本次搜索。",
        "",
    ]
    for idx, item in enumerate(results, 1):
        lines.append(_tg_tmdb_result_line(idx, item))
    return "\n".join(lines)


def _tg_truncate(text: str, limit: int = 90) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _tg_resource_title(resource: dict) -> str:
    # remark 往往是质量说明，适合作为独立备注展示，不优先拿来当标题。
    for key in ("title", "name", "resource_name", "share_name", "filename", "file_name", "slug", "remark", "summary"):
        value = resource.get(key)
        if value:
            return _tg_truncate(value, 80)
    return "未知资源"


def _tg_flatten_resource_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " / ".join(_tg_flatten_resource_value(v) for v in value if v)
    if isinstance(value, dict):
        return " / ".join(_tg_flatten_resource_value(v) for v in value.values() if v)
    return re.sub(r"\s+", " ", str(value)).strip()


def _tg_resource_remark(resource: dict, limit: int = 160) -> str:
    for key in ("remark", "description", "summary", "subtitle", "subtitles"):
        value = _tg_flatten_resource_value(resource.get(key))
        if value:
            return _tg_truncate(value, limit=limit)
    return ""


def _tg_resource_size_gb(resource: dict):
    raw = resource.get("share_size") or resource.get("size") or resource.get("file_size") or resource.get("total_size")
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float)):
            return float(raw) / 1024 / 1024 / 1024 if float(raw) > 10000 else float(raw)
        text = str(raw).strip().upper().replace(",", "")
        match = re.search(r"([\d.]+)\s*(TB|GB|MB|KB|B)?", text)
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2) or "GB"
        if unit == "TB":
            return value * 1024
        if unit == "GB":
            return value
        if unit == "MB":
            return value / 1024
        if unit == "KB":
            return value / 1024 / 1024
        if unit == "B":
            return value / 1024 / 1024 / 1024
    except Exception:
        return None
    return None


def _tg_resource_resolution(resource: dict) -> str:
    values = resource.get("video_resolution") or resource.get("resolution") or ""
    if isinstance(values, list):
        values = "/".join(str(v) for v in values if v)
    text = str(values or "未知").strip()
    return text.upper() if text else "未知"


def _tg_resource_pan_text(resource: dict) -> str:
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        return "📡 频道"
    pan_type = str(resource.get("pan_type") or "115").upper()
    return f"🟡 {pan_type}"


def _tg_resource_points_text(resource: dict) -> str:
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        return "🆓 可转存"
    points = resource.get("unlock_points")
    already_owned = bool(resource.get("already_owned"))
    if already_owned:
        return "✅ 已拥有"
    if points in (None, 0, "0", "", "free", "FREE"):
        return "🆓 免费"
    return f"💎 {points}积分"

def _tg_resource_size_text(resource: dict) -> str:
    size_gb = _tg_resource_size_gb(resource)
    if size_gb is None:
        return "💾 未知大小"
    if size_gb >= 100:
        return f"💾 {size_gb:.0f}GB"
    return f"💾 {size_gb:.1f}GB"


def _tg_resource_quality_text(resource: dict, limit: int = 96) -> str:
    # 尽量提取一行“版本/质量/来源”摘要，和备注分开展示。
    preferred = []
    for key in ("quality", "source", "video_codec", "audio", "format", "category", "edition"):
        value = _tg_flatten_resource_value(resource.get(key))
        if value:
            preferred.append(value)

    if preferred:
        return _tg_truncate(" / ".join(dict.fromkeys(preferred)), limit=limit)

    # 字段不全时，用名称字段兜底，但避免把 slug 当质量说明。
    for key in ("title", "name", "resource_name", "share_name", "filename", "file_name"):
        value = _tg_flatten_resource_value(resource.get(key))
        if value:
            return _tg_truncate(value, limit=limit)
    return ""


def _tg_is_similar_text(a: str, b: str) -> bool:
    a_norm = re.sub(r"\s+", "", str(a or "")).lower()
    b_norm = re.sub(r"\s+", "", str(b or "")).lower()
    if not a_norm or not b_norm:
        return False
    return a_norm == b_norm or a_norm in b_norm or b_norm in a_norm


def _tg_resource_line(index: int, resource: dict) -> str:
    title = _tg_resource_title(resource)
    res_text = _tg_resource_resolution(resource)
    extra = []
    if resource.get("_completion_label"):
        extra.append(str(resource.get("_completion_label")))
    if resource.get("_season_match_label"):
        extra.append(str(resource.get("_season_match_label")))
    if resource.get("_tg_source") == "channel" or resource.get("source") == "channel":
        source_channel = resource.get("source_channel") or "未知频道"
        message_date = resource.get("message_date") or ""
        extra.append(f"来自：{source_channel}{' · ' + message_date if message_date else ''}")
    extra_text = f"  {' / '.join(extra)}" if extra else ""

    lines = [
        f"{index:02d}. {_tg_resource_pan_text(resource)}  {_tg_resource_points_text(resource)}  {_tg_resource_size_text(resource)}  🎞 {res_text}{extra_text}",
    ]

    quality = _tg_resource_quality_text(resource)
    if quality:
        lines.append(f"    📦 {quality}")

    remark = _tg_resource_remark(resource)
    if remark and not _tg_is_similar_text(remark, title) and not _tg_is_similar_text(remark, quality):
        lines.append(f"    📝 {remark}")

    # 保留一个可识别标题，避免某些资源只有 remark 时看不出是哪条。
    if title and not _tg_is_similar_text(title, quality) and not _tg_is_similar_text(title, remark):
        lines.append(f"    🎬 {title}")

    return "\n".join(lines)

def _tg_format_hdhive_resources(
    media: dict,
    resources: list,
    raw_count: int,
    filtered_count: int,
    used_filtered: bool,
    channel_count: int = 0,
    notes: list = None,
    page: int = 0,
    total_count: int = None,
) -> str:
    media_type = media.get("media_type") or "movie"
    title = media.get("title") or "未知标题"
    year = media.get("year") or "未知年份"
    tmdb_id = media.get("tmdb_id") or "-"

    resources = resources or []
    total_count = len(resources) if total_count is None else int(total_count or 0)
    page = _tg_clamp_page(page, total_count)
    page_count = max(1, (total_count + _TG_RESOURCE_PAGE_SIZE - 1) // _TG_RESOURCE_PAGE_SIZE)
    show_start = page * _TG_RESOURCE_PAGE_SIZE + 1 if total_count else 0
    show_end = min(page * _TG_RESOURCE_PAGE_SIZE + len(resources), total_count)

    hdhive_count = raw_count or 0
    channel_count = channel_count or 0
    source_text = f"🪺 影巢 {hdhive_count} 条"
    if channel_count:
        source_text += f" / 📡 频道 {channel_count} 条"

    if media_type == "tv":
        count_text = f"🔎 {source_text}；剧集手动搜索不按季过滤。"
    else:
        count_text = f"🔎 {source_text}。"

    if total_count > _TG_RESOURCE_PAGE_SIZE:
        page_text = f"📄 共 {total_count} 条，当前第 {page + 1}/{page_count} 页，显示 {show_start}-{show_end}。"
    else:
        page_text = f"📄 共 {total_count} 条。"

    lines = [
        f"🔎 资源搜索 | {title} ({year})",
        "━━━━━━━━━━━━━━",
        f"🎭 类型：{_tg_media_type_label(media_type)}    🆔 TMDb：{tmdb_id}",
        count_text,
        page_text,
        "↩️ 回复当前页显示的编号直接转存，或点击下方按钮。",
        "➡️ 输入 下一页 / 上一页 也可以翻页。",
        "🚫 输入 取消 结束本次搜索。",
        "",
    ]

    base_index = page * _TG_RESOURCE_PAGE_SIZE
    for offset, item in enumerate(resources, 1):
        lines.append(_tg_resource_line(base_index + offset, item))
        if offset != len(resources):
            lines.append("")

    if notes:
        lines.append("")
        lines.append("ℹ️ " + "；".join(str(n) for n in notes if n))

    return "\n".join(lines)

def _tg_start_tmdb_search(chat_id: str, query: str):
    query = str(query or "").strip()
    if not query:
        _tg_send_plain(chat_id, "请输入要搜索的片名，例如：阿凡达")
        return

    def run():
        try:
            api_key = _tg_get_tmdb_api_key()
            if not api_key:
                _tg_send_plain(chat_id, "❌ 未配置 TMDb API Key，无法搜索。")
                return

            _tg_send_plain(chat_id, f"⏳ 正在搜索 TMDb：{query}", disable_notification=True)

            from handler.tmdb import search_media, search_multi_media

            data = search_multi_media(query=query, api_key=api_key, page=1)
            results = (data or {}).get("results") or []

            # 兼容旧版本：如果 multi 搜不到，再分别查电影/剧集。
            if not results:
                movie_results = search_media(query=query, api_key=api_key, item_type="movie") or []
                tv_results = search_media(query=query, api_key=api_key, item_type="tv") or []
                for item in movie_results:
                    item["media_type"] = "movie"
                for item in tv_results:
                    item["media_type"] = "tv"
                results = movie_results + tv_results

            normalized_results = []
            seen = set()
            for item in results:
                media_type = item.get("media_type")
                tmdb_id = item.get("id")
                if media_type not in {"movie", "tv"} or not tmdb_id:
                    continue
                key = (media_type, str(tmdb_id))
                if key in seen:
                    continue
                seen.add(key)
                normalized_results.append(item)
                if len(normalized_results) >= _TG_TMDB_SEARCH_LIMIT:
                    break

            if not normalized_results:
                _tg_clear_session(chat_id)
                _tg_send_plain(chat_id, f"❌ TMDb 未搜索到：{query}")
                return

            _tg_set_session(chat_id, {
                "stage": "tmdb_results",
                "query": query,
                "results": normalized_results,
            })

            reply_markup = _tg_build_number_keyboard("tg_tmdb", len(normalized_results))
            _tg_send_plain(chat_id, _tg_format_tmdb_results(query, normalized_results), reply_markup=reply_markup)

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] TMDb 搜索失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ TMDb 搜索异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_TMDb", daemon=True).start()


def _tg_query_hdhive_resources(chat_id: str, selection_number: int, target_season=None):
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "tmdb_results":
        _tg_send_plain(chat_id, "❌ 当前没有可选择的 TMDb 搜索结果，请重新输入片名搜索。")
        return

    results = session.get("results") or []
    if selection_number < 1 or selection_number > len(results):
        _tg_send_plain(chat_id, f"❌ 序号无效，请回复 1-{len(results)}。")
        return

    selected = results[selection_number - 1]
    media_type = selected.get("media_type") or "movie"
    tmdb_id = selected.get("id")
    title = _tg_tmdb_title(selected)
    year = _tg_tmdb_year(selected)
    original_title = selected.get("original_title") or selected.get("original_name") or ""

    # TG 手动搜索不再按季过滤。target_season 仅兼容旧输入，不参与剧集筛选。
    media = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "target_season": None,
    }

    def run():
        notes = []
        hdhive_raw_count = 0
        hdhive_filtered_count = 0
        hdhive_used_filtered = False
        hdhive_resources = []
        channel_resources = []

        try:
            season_tip = "（剧集全量，不按季过滤）" if media_type == "tv" else ""
            _tg_send_plain(chat_id, f"⏳ 正在查询资源：{title} ({year}){season_tip}\n来源：影巢 + 已配置监听频道", disable_notification=True)

            # 1. 查询影巢资源：失败不直接中断，继续查频道。
            try:
                from handler.hdhive_client import HDHiveClient
                from tasks.hdhive import filter_hdhive_resources

                client = HDHiveClient()
                if client.ping():
                    query_season = None if media_type == "tv" else target_season
                    raw_resources = client.get_resources(tmdb_id, media_type, target_season=query_season) or []
                    hdhive_raw_count = len(raw_resources)

                    if media_type == "tv":
                        hdhive_resources = raw_resources[:_TG_RESOURCE_COLLECT_LIMIT]
                    else:
                        filtered_resources = filter_hdhive_resources(
                            raw_resources,
                            target_season=None,
                            media_type=media_type,
                            require_complete=False,
                        )
                        hdhive_filtered_count = len(filtered_resources)
                        hdhive_used_filtered = bool(filtered_resources)
                        hdhive_resources = (filtered_resources or raw_resources)[:_TG_RESOURCE_COLLECT_LIMIT]

                    for item in hdhive_resources:
                        item["_tg_source"] = "hdhive"
                else:
                    notes.append("影巢未授权，已跳过影巢查询")
            except Exception as e:
                logger.error(f"  ➜ [TG资源搜索] 影巢资源查询失败: {e}", exc_info=True)
                notes.append(f"影巢查询失败：{e}")

            # 2. 查询已配置监听频道历史。使用 UserBot 账号搜索频道历史消息；不影响原来的频道自动监听。
            try:
                from handler.tg_userbot import TGUserBotManager

                extra_queries = []
                if original_title and original_title != title:
                    extra_queries.append(original_title)
                # 部分频道标题带年份，单独用“片名 年份”有时更准；但仍保留片名搜索。
                if year and year != "未知年份":
                    extra_queries.append(f"{title} {year}")

                ub = TGUserBotManager.get_instance()
                search_result = ub.search_channel_resources(
                    query=title,
                    media_type=media_type,
                    tmdb_id=tmdb_id,
                    year=year,
                    limit=_TG_RESOURCE_COLLECT_LIMIT,
                    extra_queries=extra_queries,
                    timeout=30,
                ) or {}

                if search_result.get("ok"):
                    channel_resources = search_result.get("results") or []
                    for item in channel_resources:
                        item["_tg_source"] = "channel"
                        # 手动点击频道资源应该直接放行，不再要求它本来就在订阅/追剧列表。
                        item["is_keyword_matched"] = True
                        item["is_subscribe"] = False
                        item.setdefault("title", title)
                        item.setdefault("year", year)
                        item.setdefault("tmdb_id", tmdb_id)
                        item.setdefault("item_type", media_type)
                else:
                    err = search_result.get("error")
                    if err:
                        notes.append(f"频道搜索跳过：{err}")
            except Exception as e:
                logger.error(f"  ➜ [TG资源搜索] 频道资源查询失败: {e}", exc_info=True)
                notes.append(f"频道搜索失败：{e}")

            # 3. 合并展示：影巢优先，但第一页固定给频道结果留几个位置；后续全部靠翻页查看。
            all_resources = []
            if channel_resources:
                first_page_channel_slots = min(len(channel_resources), 4)
                first_page_hdhive_slots = max(0, _TG_RESOURCE_PAGE_SIZE - first_page_channel_slots)
                hdhive_quota = min(len(hdhive_resources), first_page_hdhive_slots)
                all_resources.extend(hdhive_resources[:hdhive_quota])
                all_resources.extend(channel_resources)
                all_resources.extend(hdhive_resources[hdhive_quota:])
            else:
                all_resources = list(hdhive_resources)

            all_resources = all_resources[:_TG_RESOURCE_COLLECT_LIMIT]
            if not all_resources:
                msg = f"❌ 没有找到可处理资源：{title} ({year})"
                if notes:
                    msg += "\n" + "\n".join(f"- {n}" for n in notes)
                _tg_send_plain(chat_id, msg)
                return

            _tg_set_session(chat_id, {
                "stage": "hdhive_resources",  # 实际可包含影巢+频道资源。
                "media": media,
                "all_resources": all_resources,
                "resources": _tg_slice_resource_page(all_resources, 0),
                "page": 0,
                "raw_count": hdhive_raw_count,
                "filtered_count": hdhive_filtered_count,
                "used_filtered": hdhive_used_filtered,
                "channel_count": len(channel_resources),
                "notes": notes,
            })

            _tg_show_resource_page(chat_id, 0)

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 资源查询失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 资源查询异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_All", daemon=True).start()

def _tg_show_resource_page(chat_id: str, page: int):
    """根据当前资源搜索会话发送指定页。每页 10 条，编号使用全局序号。"""
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "hdhive_resources":
        _tg_send_plain(chat_id, "❌ 当前没有可翻页的资源搜索结果，请重新输入片名搜索。")
        return

    all_resources = session.get("all_resources") or session.get("resources") or []
    if not all_resources:
        _tg_send_plain(chat_id, "❌ 当前资源列表为空，请重新输入片名搜索。")
        return

    page = _tg_clamp_page(page, len(all_resources))
    page_resources = _tg_slice_resource_page(all_resources, page)
    session["page"] = page
    session["resources"] = page_resources
    _tg_set_session(chat_id, session)

    _tg_send_plain(
        chat_id,
        _tg_format_hdhive_resources(
            session.get("media") or {},
            page_resources,
            session.get("raw_count") or 0,
            session.get("filtered_count") or 0,
            bool(session.get("used_filtered")),
            channel_count=session.get("channel_count") or 0,
            notes=session.get("notes") or [],
            page=page,
            total_count=len(all_resources),
        ),
        reply_markup=_tg_build_resource_page_keyboard(len(all_resources), page),
    )


def _tg_start_hdhive_transfer(chat_id: str, selection_number: int):
    session = _tg_get_session(chat_id)
    if not session or session.get("stage") != "hdhive_resources":
        _tg_send_plain(chat_id, "❌ 当前没有可选择的资源，请重新输入片名搜索。")
        return

    # 翻页后按钮和正文都使用“全局编号”，所以这里从 all_resources 里取。
    resources = session.get("all_resources") or session.get("resources") or []
    if selection_number < 1 or selection_number > len(resources):
        _tg_send_plain(chat_id, f"❌ 序号无效，请回复 1-{len(resources)}，或点击翻页按钮查看更多。")
        return

    resource = resources[selection_number - 1]
    media = session.get("media") or {}
    source = resource.get("_tg_source") or resource.get("source") or "hdhive"

    title = media.get("title") or resource.get("title") or _tg_resource_title(resource)
    year = media.get("year") or resource.get("year") or ""
    display_title = f"{title} ({year})" if year else title
    media_type = media.get("media_type") or resource.get("item_type") or "movie"
    tmdb_id = media.get("tmdb_id") or resource.get("tmdb_id")

    # 开始转存后清理会话，避免用户重复点按钮造成重复转存。
    _tg_clear_session(chat_id)

    if source == "channel":
        try:
            from handler.tg_userbot import tg_task_queue

            task = {
                "type": "channel_resource_complex",
                "tmdb_id": tmdb_id,
                "title": title,
                "year": year,
                "item_type": media_type,
                "target_link": resource.get("target_link"),
                "magnet_url": resource.get("magnet_url"),
                "receive_code": resource.get("receive_code") or "",
                "season_number": resource.get("season_number"),
                "episode_number": resource.get("episode_number"),
                "is_pack": bool(resource.get("is_pack")),
                "is_completed_pack": bool(resource.get("is_completed_pack")),
                "is_brainless": False,
                # 手动选择频道搜索结果，直接放行转存。
                "is_keyword_matched": True,
                "is_subscribe": False,
            }

            if not task.get("target_link") and not task.get("magnet_url"):
                _tg_send_plain(chat_id, "❌ 当前频道资源缺少 115/影巢/磁力链接，无法转存。")
                return

            tg_task_queue.put(task)
            source_channel = resource.get("source_channel") or "频道"
            _tg_send_plain(chat_id, f"✅ 已提交频道资源转存：{display_title}\n来源：{source_channel}\n请稍后查看转存通知/系统日志。")
            return
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 频道资源提交失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 频道资源提交异常：{e}")
            return

    slug = resource.get("slug") or resource.get("resource_slug") or resource.get("id")
    if not slug:
        _tg_send_plain(chat_id, "❌ 当前影巢资源缺少 slug，无法解锁转存。")
        return

    def run():
        try:
            _tg_send_plain(chat_id, f"⏳ 已选择影巢资源：{_tg_resource_title(resource)}\n正在解锁并转存到 115，请稍后查看通知/日志。", disable_notification=True)

            from tasks.hdhive import task_download_from_hdhive

            ok = task_download_from_hdhive(
                api_key=None,
                slug=slug,
                tmdb_id=tmdb_id,
                media_type=media_type,
                title=display_title,
            )

            if ok:
                _tg_send_plain(chat_id, f"✅ 影巢资源已提交转存：{display_title}")
            else:
                _tg_send_plain(chat_id, f"❌ 影巢资源转存失败：{display_title}\n请查看系统日志确认原因。")

        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 影巢转存失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, f"❌ 影巢转存异常：{e}")

    threading.Thread(target=run, name="TG_Resource_Search_Transfer", daemon=True).start()

def _tg_try_handle_resource_session_input(chat_id: str, text: str) -> bool:
    """处理资源搜索会话中的数字回复/取消。返回 True 表示已消费消息。"""
    stripped = str(text or "").strip()
    if stripped.lower() in {"取消", "cancel", "/cancel", "退出", "停止"}:
        if _tg_get_session(chat_id):
            _tg_clear_session(chat_id)
            _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
            return True
        return False

    session = _tg_get_session(chat_id)
    if session and session.get("stage") == "hdhive_resources":
        lower = stripped.lower()
        if lower in {"下一页", "下页", "next", "n", ">", "➡️"}:
            _tg_show_resource_page(chat_id, int(session.get("page") or 0) + 1)
            return True
        if lower in {"上一页", "上页", "prev", "previous", "p", "<", "⬅️"}:
            _tg_show_resource_page(chat_id, int(session.get("page") or 0) - 1)
            return True

    number, season = _tg_parse_selection_text(stripped)
    if number is None:
        return False

    session = _tg_get_session(chat_id)
    if not session:
        return False

    if session.get("stage") == "tmdb_results":
        _tg_query_hdhive_resources(chat_id, number, target_season=season)
        return True

    if session.get("stage") == "hdhive_resources":
        _tg_start_hdhive_transfer(chat_id, number)
        return True

    return False


def _execute_task_from_tg(chat_id: str, task_key: str):
    """在后台线程中执行选定的任务"""
    from tasks.core import get_task_registry
    registry = get_task_registry(context='all')
    task_info = registry.get(task_key)
    
    if not task_info:
        send_telegram_message(chat_id, escape_markdown("❌ 任务不存在或已失效。"))
        return

    task_function, task_description, processor_type = task_info[:3]
    
    # 获取对应的处理器实例
    target_processor = None
    if processor_type == 'media':
        target_processor = extensions.media_processor_instance
    elif processor_type == 'watchlist':
        target_processor = extensions.watchlist_processor_instance
    elif processor_type == 'actor':
        target_processor = extensions.actor_subscription_processor_instance

    if not target_processor:
        send_telegram_message(chat_id, escape_markdown(f"❌ 无法获取 {processor_type} 处理器实例。"))
        return

    send_telegram_message(chat_id, escape_markdown(f"🚀 任务已启动：*{task_description}*\n请在系统日志或任务中心查看进度。"))
    logger.info(f"  ➜ [TG交互] 管理员 {chat_id} 触发了任务: {task_description}")

    # 包装执行逻辑，处理特殊参数
    def run_wrapper():
        try:
            tasks_requiring_force_flag = ['role-translation', 'enrich-aliases', 'populate-metadata']
            if task_key in tasks_requiring_force_flag:
                task_function(target_processor, force_full_update=False)
            else:
                task_function(target_processor)
            
            send_telegram_message(chat_id, escape_markdown(f"✅ 任务执行完毕：*{task_description}*"))
        except Exception as e:
            logger.error(f"  ➜ TG触发任务 '{task_description}' 失败: {e}", exc_info=True)
            send_telegram_message(chat_id, escape_markdown(f"❌ 任务执行失败：*{task_description}*\n错误信息: {str(e)}"))

    # 启动独立线程执行任务，避免阻塞 TG 轮询
    threading.Thread(target=run_wrapper, name=f"TG_Task_{task_key}", daemon=True).start()

def _handle_callback_query(callback_query: dict):
    """处理内联键盘的按钮点击事件"""
    query_id = callback_query.get('id')
    from_user = callback_query.get('from', {})
    requester_id = str(from_user.get('id', ''))
    message_chat = (callback_query.get('message') or {}).get('chat') or {}
    chat_id = str(message_chat.get('id') or requester_id)
    data = callback_query.get('data', '')

    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    
    # 1. 权限校验：按钮点击按点击者身份校验；消息发送仍回到原聊天。
    admin_ids = [str(aid) for aid in user_db.get_admin_telegram_chat_ids()]
    if requester_id not in admin_ids:
        logger.warning(f"  ➜ [TG交互] 收到未授权用户 ({requester_id}) 的回调请求，已拒绝。")
        return

    # 2. 响应 Callback Query (消除按钮上的加载圈圈)
    if bot_token and query_id:
        answer_url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
        try:
            requests.post(answer_url, json={'callback_query_id': query_id}, proxies=get_proxies_for_requests(), timeout=5)
        except Exception:
            pass

    # 3. 处理资源搜索/转存选择按钮
    if data == 'tg_search_cancel':
        _tg_clear_session(chat_id)
        _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
        return

    if data.startswith('tg_tmdb:'):
        try:
            _tg_query_hdhive_resources(chat_id, int(data.split(':', 1)[1]))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理 TMDb 选择按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 选择失败，请重新输入片名搜索。")
        return

    if data.startswith('tg_res_page:'):
        page_value = data.split(':', 1)[1]
        if page_value == 'noop':
            return
        try:
            _tg_show_resource_page(chat_id, int(page_value))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理资源翻页按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 翻页失败，请重新输入片名搜索。")
        return

    if data.startswith('tg_hdhive:'):
        try:
            _tg_start_hdhive_transfer(chat_id, int(data.split(':', 1)[1]))
        except Exception as e:
            logger.error(f"  ➜ [TG资源搜索] 处理资源选择按钮失败: {e}", exc_info=True)
            _tg_send_plain(chat_id, "❌ 选择失败，请重新输入片名搜索。")
        return

    # 4. 处理任务触发逻辑
    if data.startswith('run_task_'):
        task_key = data.replace('run_task_', '')
        _execute_task_from_tg(chat_id, task_key)
        return

def _handle_incoming_message(message: dict):
    """处理接收到的单条消息 (纯手动遥控器模式)"""
    chat_id = str(message.get('chat', {}).get('id', ''))
    text = message.get('text', '') or message.get('caption', '') # 兼容带图片的 caption
    text = text.strip()
    if not chat_id or not text:
        return

    # 1. 权限校验：只允许管理员发送指令 (或者来自全局频道)
    admin_ids = [str(aid) for aid in user_db.get_admin_telegram_chat_ids()]
    from_user_id = str((message.get('from') or {}).get('id', ''))
    global_channel = str(APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID, ''))
    is_admin = chat_id in admin_ids or from_user_id in admin_ids
    
    if not is_admin and chat_id != global_channel:
        logger.warning(f"  ➜ [TG交互] 收到未授权用户/聊天 ({from_user_id or chat_id}) 的消息，已忽略。")
        return


    # 会话中的“回复序号/取消”优先处理
    if is_admin and _tg_try_handle_resource_session_input(chat_id, text):
        return

    # ★★★ 处理 M 菜单发来的命令 ★★★
    if text.startswith('/'):
        cmd_body = text[1:].strip()
        cmd_token = cmd_body.split()[0].lower() if cmd_body else ''
        cmd = cmd_token.split('@', 1)[0]
        cmd_args = cmd_body[len(cmd_token):].strip() if cmd_token else ''

        if cmd in ['cancel', '取消']:
            _tg_clear_session(chat_id)
            _tg_send_plain(chat_id, "✅ 已取消本次资源搜索。")
            return

        if cmd in ['search', 'find', 'hdhive']:
            if not is_admin:
                _tg_send_plain(chat_id, "❌ 只有管理员可以使用资源搜索。")
                return
            if not cmd_args:
                _tg_send_plain(chat_id, "请输入要搜索的片名，例如：/search 阿凡达")
                return
            _tg_start_tmdb_search(chat_id, cmd_args)
            return

        from tasks.core import get_task_registry
        registry = get_task_registry(context='all')

        if cmd in ['all_tasks', 'tasks', 'menu']:
            keyboard = []
            row = []
            for key, info in registry.items():
                desc = info[1]
                row.append({"text": desc, "callback_data": f"run_task_{key}"})
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row: keyboard.append(row)
            reply_markup = {"inline_keyboard": keyboard}
            send_telegram_message(chat_id, escape_markdown("📋 *所有可用任务列表*\n请点击下方按钮执行对应任务："), reply_markup=reply_markup)
            return

        for key in registry.keys():
            expected_cmd = key.replace('-', '_').lower()
            if cmd == expected_cmd:
                _execute_task_from_tg(chat_id, key)
                return

    # 2. 识别链接类型
    is_magnet = text.lower().startswith('magnet:?')
    is_ed2k = text.lower().startswith('ed2k://')
    is_115_share = re.search(r'115(?:cdn)?\.com/s/', text, re.IGNORECASE) is not None

    if not (is_magnet or is_ed2k or is_115_share):
        # 管理员私聊/群聊中输入普通片名，进入 TMDb -> 影巢 -> 115 转存流程。
        # 全局频道普通文本不触发搜索，避免频道公告被误当作片名。
        if is_admin:
            _tg_start_tmdb_search(chat_id, text)
        return

    # =================================================================
    # ★ 纯手动处理逻辑 (不再包含任何自动订阅和查库代码)
    # =================================================================
    logger.info(f"  ➜ [TG交互] 收到来自 {chat_id} 的手动资源链接，准备处理...")
    send_telegram_message(chat_id, escape_markdown("⏳ *收到链接，正在提交至 115...*"), disable_notification=True)

    client = P115Service.get_client()
    if not client:
        send_telegram_message(chat_id, "❌ *提交失败*：115 客户端未初始化，请检查配置。")
        return
        
    target_cid = APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')

    try:
        # --- 处理 115 分享链接转存 ---
        if is_115_share:
            share_code_match = re.search(r'115(?:cdn)?\.com/s/([a-zA-Z0-9]+)', text, re.IGNORECASE)
            share_code = share_code_match.group(1) if share_code_match else None
            
            receive_code = ""
            pwd_match = re.search(r'(?:访问码|提取码|密码|password)[:：=\s]*([a-zA-Z0-9]{4})', text, re.IGNORECASE)
            if pwd_match: receive_code = pwd_match.group(1)

            if not share_code:
                send_telegram_message(chat_id, escape_markdown("❌ *解析失败*：未找到有效的 115 分享码。"))
                return

            res = client.share_import(share_code, receive_code, target_cid)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, escape_markdown("✅ *分享链接转存成功！*\n系统已自动触发整理任务。"))
                try:
                    import task_manager
                    threading.Timer(5.0, task_manager.trigger_115_organize_task).start()
                except Exception as e:
                    logger.error(f"  ➜ 唤醒整理任务失败: {e}")
            else:
                err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                send_telegram_message(chat_id, escape_markdown(f"❌ *转存失败*：{err}"))
                logger.error(f"  ➜ [TG交互] 转存失败: {err}")

        # --- 处理磁力/ED2K 离线下载 ---
        if is_magnet or is_ed2k:
            link_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)
            target_url = link_match.group(1) if link_match else text

            payload = {"url[0]": target_url, "wp_path_id": target_cid}
            res = client.offline_add_urls(payload)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, escape_markdown("✅ *离线任务提交成功！*\n系统将在后台自动监控并整理入库。"))
                try:
                    import task_manager
                    threading.Timer(10.0, task_manager.trigger_115_organize_task).start()
                except: pass
            else:
                err = res.get('error_msg') or res.get('message') or str(res) or '未知错误'
                send_telegram_message(chat_id, escape_markdown(f"❌ *离线提交失败*：{err}"))

    except Exception as e:
        logger.error(f"  ➜ [TG交互] 处理链接失败: {e}", exc_info=True)
        send_telegram_message(chat_id, f"❌ *系统异常*：处理链接时发生错误。")

def _setup_bot_commands(bot_token: str):
    """
    向 Telegram 注册机器人的命令菜单 (生成输入框左侧的 Menu 按钮)
    将常用任务直接注册为快捷命令。
    """
    from tasks.core import get_task_registry
    registry = get_task_registry(context='all')

    # ==========================================
    # ★★★ 修改：使用常量读取 TG 菜单任务列表 ★★★
    # ==========================================
    # 从 APP_CONFIG 中获取前端保存的配置，如果没有则使用 constants 中的默认值
    allowed_tasks = APP_CONFIG.get(
        constants.CONFIG_OPTION_TELEGRAM_MENU_TASKS, 
        constants.DEFAULT_TELEGRAM_MENU_TASKS
    )
    
    # 如果前端传过来的是空列表（用户清空了菜单），为了防止菜单为空报错，回退到默认值
    if not allowed_tasks:
        allowed_tasks = constants.DEFAULT_TELEGRAM_MENU_TASKS

    commands = []
    for key in allowed_tasks:
        if key in registry:
            desc = registry[key][1]
            # Telegram 命令只允许小写字母、数字和下划线，所以把横杠替换为下划线
            cmd_name = key.replace('-', '_').lower()
            commands.append({"command": cmd_name, "description": f"🚀 {desc}"})

    # 在菜单最下方追加资源搜索和“查看所有任务”的备选命令
    commands.append({"command": "search", "description": "🔎 搜索影巢资源并转存"})
    commands.append({"command": "all_tasks", "description": "📋 查看所有可用任务"})

    api_url = f"https://api.telegram.org/bot{bot_token}/setMyCommands"
    payload = {"commands": commands}
    
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=10, proxies=proxies)
        if response.status_code == 200:
            logger.trace("  ➜ 成功注册 Telegram 机器人快捷菜单。")
        else:
            logger.warning(f"  ➜ 注册 Telegram 菜单命令失败: {response.text}")
    except Exception as e:
        logger.error(f"  ➜ 注册 Telegram 菜单命令时发生网络异常: {e}")

def _telegram_polling_worker():
    """后台轮询线程"""
    global _tg_polling_active
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        logger.info("  ➜ 未配置 Telegram Bot Token，交互功能未启动。")
        return

    # ==========================================
    # ★★★ 新增：启动时自动向 TG 注册菜单按钮 ★★★
    _setup_bot_commands(bot_token)
    # ==========================================

    api_url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    offset = None
    
    logger.trace("  ➜ Telegram 机器人交互监听已启动！")
    
    while _tg_polling_active:
        try:
            # ★★★ 修改：允许接收 message 和 callback_query ★★★
            params = {'timeout': 30, 'allowed_updates': ['message', 'callback_query']}
            if offset:
                params['offset'] = offset
                
            proxies = get_proxies_for_requests()
            response = requests.get(api_url, params=params, timeout=40, proxies=proxies)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    for update in data.get('result', []):
                        offset = update['update_id'] + 1
                        
                        # ★★★ 修改：分发不同类型的更新 ★★★
                        if 'message' in update:
                            _handle_incoming_message(update['message'])
                        elif 'callback_query' in update:
                            _handle_callback_query(update['callback_query'])
                            
            elif response.status_code == 401 or response.status_code == 404:
                logger.error("  ➜ Telegram Bot Token 无效，停止轮询。")
                break
                
        except requests.exceptions.Timeout:
            pass 
        except Exception as e:
            logger.debug(f"  ➜ Telegram 轮询网络异常 (将自动重试): {e}")
            time.sleep(5) 
            
        time.sleep(1)

def send_hdhive_checkin_notification(checkin_res: dict, is_gambler: bool, user_info: dict = None):
    """
    发送影巢签到结果的 Telegram 通知卡片 (精简版)
    """
    if user_info is None:
        user_info = {}
        
    res_data = checkin_res.get("data", {})
    message_text = res_data.get("message") or checkin_res.get("message", "签到请求成功")
    # 判断是否真正签到成功 (success 为 true 且 checked_in 不为 false)
    is_success = checkin_res.get("success", False) and res_data.get("checked_in") is not False

    # 提取奖励积分 (正则匹配 "获得 X 积分")
    import re
    reward_match = re.search(r'获得\s*(-?\d+)\s*积分', message_text)
    reward = reward_match.group(1) if reward_match else "0"

    # 提取用户名 (OpenAPI 返回的是 username)
    username = user_info.get("username") or user_info.get("name") or "未知用户"
    mode_text = "赌狗签到" if is_gambler else "普通签到"

    status_icon = "✅" if is_success else "⚠️"
    status_title = "影巢签到成功" if is_success else "影巢签到提示"
    status_text = "签到成功" if is_success else "今日已签到或失败"

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 构造精简版 MarkdownV2 文本 
    text = (
        f"【{status_icon} *{escape_markdown(status_title)}*】\n"
        f"📢 *执行结果*\n"
        f"{'\\-' * 24}\n"
        f"🕒 *时间*: `{escape_markdown(current_time)}`\n"
        f"👤 *用户*: `{escape_markdown(username)}`\n"
        f"📍 *模式*: {escape_markdown(mode_text)}\n"
        f"✨ *状态*: {escape_markdown(status_text)}\n\n"
        f"📊 *签到详情*\n"
        f"💬 *消息*: {escape_markdown(message_text)}\n"
        f"🎁 *奖励*: {escape_markdown(reward)} 积分"
    )

    # 发送给频道和所有管理员
    global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
    admin_ids = set(user_db.get_admin_telegram_chat_ids())

    if global_channel_id:
        send_telegram_message(global_channel_id, text)

    for admin_id in admin_ids:
        if str(admin_id) != str(global_channel_id):
            send_telegram_message(admin_id, text)

def start_telegram_bot():
    """启动 Telegram 机器人监听"""
    global _tg_polling_thread, _tg_polling_active
    
    # Pro 权限拦截
    if not APP_CONFIG.get('is_pro_active', False):
        return

    if _tg_polling_active:
        return
        
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        return
        
    _tg_polling_active = True
    _tg_polling_thread = threading.Thread(target=_telegram_polling_worker, daemon=True, name="TG_Polling_Thread")
    _tg_polling_thread.start()

def stop_telegram_bot():
    """停止 Telegram 机器人监听"""
    global _tg_polling_active
    _tg_polling_active = False
    logger.info("  ➜ Telegram 机器人交互监听已停止。")
