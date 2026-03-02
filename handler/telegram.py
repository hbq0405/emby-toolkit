# 文件: handler/telegram.py
import requests
import logging
from datetime import datetime
from config_manager import APP_CONFIG, get_proxies_for_requests
from handler.tmdb import get_movie_details, get_tv_details
from handler.emby import get_emby_item_details
from database import user_db, request_db
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
def send_telegram_message(chat_id: str, text: str, disable_notification: bool = False):
    """通用的 Telegram 文本消息发送函数。"""
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
            logger.info(f"  ➜ 成功发送 Telegram 图文消息至 Chat ID: {final_chat_id}")
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

            # ★★★ 修改开始：收集原始数据而不是直接格式化字符串 ★★★
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
        
        # --- 4. 组装最终的通知文本 (Caption) ---
        notification_title_map = {
            'new': '✨ 入库成功',
            'update': '🔄 已更新'
        }
        notification_title = notification_title_map.get(notification_type, '🔔 状态更新')
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        media_icon = "🎬" if item_type == "Movie" else "📺"
        
        # 使用转义后的变量来构建消息，同时保留我们自己的格式化符号
        caption = (
            f"{media_icon} *{escaped_title}* {notification_title}\n\n"
            f"{episode_info_text}"
            f"⏰ *时间*: `{current_time}`\n"
            f"📝 *剧情*: {escaped_overview}"
        )
        
        # --- 5. 查询订阅者 ---
        subscribers = request_db.get_subscribers_by_tmdb_id(tmdb_id, item_type) if tmdb_id else []
        subscriber_chat_ids = {
            user_db.get_user_telegram_chat_id(sub.get('user_id')) 
            for sub in subscribers 
            if sub.get('type') == 'user_request' and sub.get('user_id')
        }
        subscriber_chat_ids = {chat_id for chat_id in subscriber_chat_ids if chat_id}

        # --- 6. 发送全局通知 ---
        global_channel_id = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        if global_channel_id:
            logger.info(f"  ➜ 正在向全局频道 {global_channel_id} 发送通知...")
            if photo_url:
                send_telegram_photo(global_channel_id, photo_url, caption)
            else:
                send_telegram_message(global_channel_id, caption)

        # --- 7. 发送管理员通知 ---
        # 逻辑：如果管理员没有配置频道，或者管理员想接收所有入库通知，但又不想和个人订阅通知重复
        all_admin_chat_ids = set(user_db.get_admin_telegram_chat_ids())

        if all_admin_chat_ids:
            # 预处理订阅者 ID 集合
            subscriber_id_set = {str(sid) for sid in subscriber_chat_ids}
            
            for admin_chat_id in all_admin_chat_ids:
                # 排除掉频道 ID
                if str(admin_chat_id) == str(global_channel_id):
                    continue

                # ★★★ 核心去重：如果管理员也是订阅者，跳过 ★★★
                if str(admin_chat_id) in subscriber_id_set:
                    logger.info(f"  ➜ 管理员 {admin_chat_id} 也是订阅者，跳过通用通知，等待发送个人通知。")
                    continue
                
                logger.info(f"  ➜ 正在向管理员 {admin_chat_id} 发送全局入库通知...")
                if photo_url:
                    send_telegram_photo(admin_chat_id, photo_url, caption)
                else:
                    send_telegram_message(admin_chat_id, caption)

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
        logger.error(f"发送媒体通知时发生严重错误: {e}", exc_info=True)

# ======================================================================
# ★★★ 新增：Telegram 机器人交互监听 (长轮询) ★★★
# ======================================================================
import re
import time
import threading
from handler.p115_service import P115Service

# 全局变量控制轮询线程
_tg_polling_thread = None
_tg_polling_active = False

def _handle_incoming_message(message: dict):
    """处理接收到的单条消息"""
    chat_id = str(message.get('chat', {}).get('id', ''))
    text = message.get('text', '').strip()
    if not chat_id or not text:
        return

    # 1. 权限校验：只允许管理员发送指令
    admin_ids = [str(aid) for aid in user_db.get_admin_telegram_chat_ids()]
    global_channel = str(APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID, ''))
    
    if chat_id not in admin_ids and chat_id != global_channel:
        logger.warning(f"  ⚠️ [TG交互] 收到未授权用户 ({chat_id}) 的消息，已忽略。")
        return

    # 2. 识别链接类型
    is_magnet = text.lower().startswith('magnet:?')
    is_ed2k = text.lower().startswith('ed2k://')
    is_115_share = '115.com/s/' in text

    if not (is_magnet or is_ed2k or is_115_share):
        # 不是支持的链接，忽略 (或者你可以加个 /help 指令回复)
        return

    logger.info(f"  📥 [TG交互] 收到来自 {chat_id} 的资源链接，准备处理...")
    send_telegram_message(chat_id, "⏳ *收到链接，正在提交至 115...*", disable_notification=True)

    # 3. 获取 115 客户端和目标目录
    client = P115Service.get_client()
    if not client:
        send_telegram_message(chat_id, "❌ *提交失败*：115 客户端未初始化，请检查配置。")
        return
        
    target_cid = APP_CONFIG.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, '0')

    try:
        # --- 处理磁力/ED2K 离线下载 ---
        if is_magnet or is_ed2k:
            # 提取纯链接，防止用户发了一段话里面夹着链接
            link_match = re.search(r'(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)', text, re.IGNORECASE)
            target_url = link_match.group(1) if link_match else text

            payload = {
                "url[0]": target_url,
                "wp_path_id": target_cid
            }
            res = client.offline_add_urls(payload)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, "✅ *离线任务提交成功！*\n系统将在后台自动监控并整理入库。")
            else:
                err = res.get('error_msg', '未知错误') if res else '无响应'
                send_telegram_message(chat_id, f"❌ *离线提交失败*：{err}")

        # --- 处理 115 分享链接转存 ---
        elif is_115_share:
            # 提取分享码
            share_code_match = re.search(r'115\.com/s/([a-zA-Z0-9]+)', text)
            share_code = share_code_match.group(1) if share_code_match else None
            
            # 提取接收码 (密码)
            receive_code = ""
            pwd_match = re.search(r'(?:访问码|提取码|密码|password)[:：=\s]*([a-zA-Z0-9]{4})', text, re.IGNORECASE)
            if pwd_match:
                receive_code = pwd_match.group(1)

            if not share_code:
                send_telegram_message(chat_id, "❌ *解析失败*：未找到有效的 115 分享码。")
                return

            res = client.share_import(share_code, receive_code, target_cid)
            
            if res and res.get('state'):
                send_telegram_message(chat_id, "✅ *分享链接转存成功！*\n文件已保存至待整理目录，等待系统处理。")
            else:
                err = res.get('error_msg', '未知错误') if res else '无响应'
                send_telegram_message(chat_id, f"❌ *转存失败*：{err}")

    except Exception as e:
        logger.error(f"  ❌ [TG交互] 处理链接失败: {e}", exc_info=True)
        send_telegram_message(chat_id, f"❌ *系统异常*：处理链接时发生错误。")

def _telegram_polling_worker():
    """后台轮询线程"""
    global _tg_polling_active
    bot_token = APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        logger.info("  ➜ 未配置 Telegram Bot Token，交互功能未启动。")
        return

    api_url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    offset = None
    
    logger.info("  🚀 Telegram 机器人交互监听已启动！")
    
    while _tg_polling_active:
        try:
            params = {'timeout': 30, 'allowed_updates': ['message']}
            if offset:
                params['offset'] = offset
                
            proxies = get_proxies_for_requests()
            # 使用长轮询，timeout 设为 30 秒
            response = requests.get(api_url, params=params, timeout=40, proxies=proxies)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    for update in data.get('result', []):
                        # 更新 offset，确保不重复处理
                        offset = update['update_id'] + 1
                        
                        if 'message' in update:
                            _handle_incoming_message(update['message'])
            elif response.status_code == 401 or response.status_code == 404:
                logger.error("  ❌ Telegram Bot Token 无效，停止轮询。")
                break
                
        except requests.exceptions.Timeout:
            pass # 正常的长轮询超时，继续下一次循环
        except Exception as e:
            logger.debug(f"  ⚠️ Telegram 轮询网络异常 (将自动重试): {e}")
            time.sleep(5) # 出错时休眠 5 秒防死循环
            
        time.sleep(1) # 基础循环间隔

def start_telegram_bot():
    """启动 Telegram 机器人监听"""
    global _tg_polling_thread, _tg_polling_active
    
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