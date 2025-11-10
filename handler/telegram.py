# 文件: handler/telegram.py
import requests
import logging
from datetime import datetime
from config_manager import APP_CONFIG, get_proxies_for_requests
from handler.tmdb import get_movie_details, get_tv_details
from handler.emby import get_emby_item_details
from database import user_db
import constants

logger = logging.getLogger(__name__)

def escape_markdown_v2(text: str) -> str:
    """
    【中央转义函数】
    这是整个系统中唯一负责转义 Telegram MarkdownV2 保留字符的地方。
    """
    if not isinstance(text, str):
        return ""
    # 根据 Telegram Bot API 官方文档，这些字符在 MarkdownV2 中需要转义
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    
    # 遍历所有需要转义的字符，并在其前面加上反斜杠
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

# --- 黄金标准：通用的 Telegram 文本消息发送函数 ---
def send_telegram_message(chat_id: str, text: str, disable_notification: bool = False):
    """
    【V3 - 安全出口】
    所有文本消息都必须通过此函数发送。它会在发送前自动处理转义。
    """
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id:
        return False
    
    # (处理 @username 的逻辑保持不变)
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # ★★★ 核心修改：在这里，在发送前的最后一刻，对完整的消息文本进行转义 ★★★
    safe_text = escape_markdown_v2(text)
    
    payload = {
        'chat_id': final_chat_id,
        'text': safe_text, # 使用转义后的安全文本
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True,
        'disable_notification': disable_notification,
    }
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=15, proxies=proxies)
        if response.status_code == 200:
            logger.info(f"  ➜ 成功发送 Telegram 文本消息至 Chat ID: {final_chat_id}")
            return True
        else:
            # 即使失败，也打印原始文本和转义后的文本，方便调试
            logger.error(f"  ➜ 发送 Telegram 文本消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            logger.debug(f"    - 原始文本: {text}")
            logger.debug(f"    - 转义后文本: {safe_text}")
            return False
    except requests.RequestException as e:
        logger.error(f"  ➜ 发送 Telegram 文本消息时发生网络请求错误: {e}")
        return False

# --- 黄金标准：通用的 Telegram 图文消息发送函数 ---
def send_telegram_photo(chat_id: str, photo_url: str, caption: str, disable_notification: bool = False):
    """
    【V3 - 安全出口】
    所有图文消息都必须通过此函数发送。它会在发送前自动处理 caption 的转义。
    """
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token or not chat_id or not photo_url:
        return False
    
    # (处理 @username 的逻辑保持不变)
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    
    # ★★★ 核心修改：同样地，在发送前对完整的 caption 进行转义 ★★★
    safe_caption = escape_markdown_v2(caption)
    
    payload = {
        'chat_id': final_chat_id,
        'photo': photo_url,
        'caption': safe_caption, # 使用转义后的安全文本
        'parse_mode': 'MarkdownV2',
        'disable_notification': disable_notification,
    }
    try:
        proxies = get_proxies_for_requests()
        response = requests.post(api_url, json=payload, timeout=30, proxies=proxies)
        if response.status_code == 200:
            logger.info(f"  ➜ 成功发送 Telegram 图文消息至 Chat ID: {final_chat_id}")
            return True
        else:
            logger.error(f"  ➜ 发送 Telegram 图文消息失败, 状态码: {response.status_code}, 响应: {response.text}")
            logger.debug(f"    - 原始 Caption: {caption}")
            logger.debug(f"    - 转义后 Caption: {safe_caption}")
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
        # --- 1. 准备基础信息 (不再需要 _escape_markdown) ---
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name_for_log = item_details.get("Name", f"ID:{item_details.get('Id')}")
        year = item_details.get("ProductionYear", "")
        title = f"{item_name_for_log} ({year})" if year else item_name_for_log
        overview = item_details.get("Overview", "暂无剧情简介。")
        if len(overview) > 200:
            overview = overview[:200] + "..."
        item_type = item_details.get("Type")

        # --- 2. 准备剧集信息 (如果适用) ---
        episode_info_text = ""
        if item_type == "Series" and new_episode_ids:
            emby_url = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
            api_key = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
            user_id = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID)

            episode_details = []
            for ep_id in new_episode_ids:
                detail = get_emby_item_details(ep_id, emby_url, api_key, user_id, fields="IndexNumber,ParentIndexNumber")
                if detail:
                    season_num = detail.get("ParentIndexNumber", 0)
                    episode_num = detail.get("IndexNumber", 0)
                    episode_details.append(f"S{season_num:02d}E{episode_num:02d}")
            if episode_details:
                episode_info_text = f"🎞️ *集数*: `{', '.join(sorted(episode_details))}`\n"

        # --- 3. 调用 tmdb_handler 获取图片路径 ---
        photo_url = None
        if tmdb_id:
            tmdb_api_key = APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            image_details = None
            try:
                if item_type == 'Movie':
                    image_details = get_movie_details(int(tmdb_id), tmdb_api_key, append_to_response=None)
                elif item_type == 'Series':
                    image_details = get_tv_details(int(tmdb_id), tmdb_api_key, append_to_response=None)

                if image_details:
                    if image_details.get('backdrop_path'):
                        photo_url = f"https://image.tmdb.org/t/p/w780{image_details['backdrop_path']}"
                    elif image_details.get('poster_path'):
                        photo_url = f"https://image.tmdb.org/t/p/w500{image_details['poster_path']}"
            except Exception as e:
                 logger.error(f"  ➜ [通知] 调用 tmdb_handler 获取图片信息时出错: {e}", exc_info=True)
        
        # --- 4. 组装最终的通知文本 (使用原生 Markdown，不再调用任何转义函数) ---
        notification_title_map = {'new': '✨ 入库成功', 'update': '🔄 已更新'}
        notification_title = notification_title_map.get(notification_type, '🔔 状态更新')
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        media_icon = "🎬" if item_type == "Movie" else "📺"
        
        caption = (
            f"{media_icon} *{title}* {notification_title}\n\n"
            f"{episode_info_text}"
            f"⏰ *时间*: `{current_time}`\n"
            f"📝 *剧情*: {overview}"
        )
        
        # --- 5. 查询订阅者 ---
        subscribers = user_db.get_subscribers_by_tmdb_id(tmdb_id) if tmdb_id else []
        subscriber_chat_ids = {user_db.get_user_telegram_chat_id(sub['emby_user_id']) for sub in subscribers}
        subscriber_chat_ids = {chat_id for chat_id in subscriber_chat_ids if chat_id}

        # --- 6 发送全局通知 ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        if global_channel_id:
            logger.info(f"  ➜ 正在向全局频道 {global_channel_id} 发送通知...")
            if photo_url:
                send_telegram_photo(global_channel_id, photo_url, caption)
            else:
                send_telegram_message(global_channel_id, caption)

        # --- 7 发送个人通知 ---
        if subscriber_chat_ids:
            personal_caption_map = {'new': f"✅ *您的订阅已入库*\n\n{caption}", 'update': f"🔄 *您的订阅已更新*\n\n{caption}"}
            personal_caption = personal_caption_map.get(notification_type, caption)
            
            for chat_id in subscriber_chat_ids:
                if chat_id == global_channel_id: continue
                logger.info(f"  ➜ 正在向订阅者 {chat_id} 发送个人通知...")
                if photo_url:
                    send_telegram_photo(chat_id, photo_url, personal_caption)
                else:
                    send_telegram_message(chat_id, personal_caption)
            
    except Exception as e:
        logger.error(f"发送媒体通知时发生严重错误: {e}", exc_info=True)