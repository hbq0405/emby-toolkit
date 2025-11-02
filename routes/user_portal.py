# routes/user_portal.py
import logging
import requests
from flask import Blueprint, jsonify, session, request

from extensions import emby_login_required # 保护我们的新接口
from database import user_db, settings_db
import moviepilot_handler # ★ 1. 导入我们的 MP 处理器
import config_manager     # ★ 2. 导入配置管理器，因为 MP 处理器需要它
import constants
from telegram_handler import send_telegram_message

# 1. 创建一个新的蓝图
user_portal_bp = Blueprint('user_portal_bp', __name__, url_prefix='/api/portal')
logger = logging.getLogger(__name__)

@user_portal_bp.route('/subscribe', methods=['POST'])
@emby_login_required
def request_subscription():
    """【V3 - 适配多季订阅】处理用户的订阅或“想看”请求。"""
    data = request.json
    emby_user_id = session['emby_user_id']
    
    is_vip = user_db.get_user_subscription_permission(emby_user_id)

    if not is_vip:
        # 普通用户的订阅申请
        user_db.create_subscription_request(
            emby_user_id=emby_user_id,
            tmdb_id=str(data.get('tmdb_id')),
            item_type=data.get('item_type'),
            item_name=data.get('item_name'),
            status='pending',
            processed_by=None
        )
        message = "“想看”请求已提交，请等待管理员审核。"
    else:
        # --- VIP 用户的自动订阅逻辑 ---
        logger.info(f"VIP 用户 {emby_user_id} 的订阅请求已自动批准，准备通过 MoviePilot 订阅...")
        
        # a. 检查总配额
        if settings_db.get_subscription_quota() <= 0:
            logger.warning(f"VIP 用户 {emby_user_id} 尝试自动订阅，但配额已用尽。")
            return jsonify({"status": "error", "message": "今日订阅配额已用尽，请明天再试。"}), 429

        item_type = data.get('item_type')
        config = config_manager.APP_CONFIG
        
        subscription_successful = False
        
        if item_type == 'Movie':
            mp_payload = { "name": data.get('item_name'), "tmdbid": int(data.get('tmdb_id')), "type": "电影" }
            if moviepilot_handler.subscribe_with_custom_payload(mp_payload, config):
                # 电影订阅成功，扣配额，创建一条记录
                settings_db.decrement_subscription_quota()
                user_db.create_subscription_request(
                    emby_user_id=emby_user_id, tmdb_id=str(data.get('tmdb_id')),
                    item_type=item_type, item_name=data.get('item_name'),
                    status='approved', processed_by='auto'
                )
                subscription_successful = True
        
        elif item_type == 'Series':
            series_info = { "tmdb_id": int(data.get('tmdb_id')), "item_name": data.get('item_name') }
            subscription_results = moviepilot_handler.smart_subscribe_series(series_info, config)

            # 如果返回了列表 (即使是空列表)，说明 MP 调用没问题
            if subscription_results is not None:
                # ★★★ 核心修改：遍历返回的列表 ★★★
                if not subscription_results:
                    logger.warning(f"智能订阅 '{data.get('item_name')}' 未返回任何有效的季订阅信息，但仍视为成功。")
                    # 即使没有订阅任何季，也创建一个总的请求记录
                    user_db.create_subscription_request(
                        emby_user_id=emby_user_id, tmdb_id=str(data.get('tmdb_id')),
                        item_type=item_type, item_name=data.get('item_name'),
                        status='approved', processed_by='auto'
                    )
                else:
                    # 为返回的每一季都创建一条记录
                    for season_info in subscription_results:
                        # 每次循环都检查配额，防止超额
                        if settings_db.get_subscription_quota() <= 0:
                            logger.warning("在订阅多季剧集时配额耗尽，部分季可能未被记录。")
                            break 
                        
                        settings_db.decrement_subscription_quota()
                        user_db.create_subscription_request(
                            emby_user_id=emby_user_id,
                            tmdb_id=str(season_info.get('parent_tmdb_id')), # 使用返回的父ID
                            item_type=item_type,
                            item_name=f"{season_info.get('parsed_series_name')} - 第 {season_info.get('parsed_season_number')} 季",
                            status='approved',
                            processed_by='auto',
                            # 将解析出的信息也存入数据库
                            parent_tmdb_id=str(season_info.get('parent_tmdb_id')),
                            parsed_series_name=season_info.get('parsed_series_name'),
                            parsed_season_number=season_info.get('parsed_season_number')
                        )
                subscription_successful = True

        if not subscription_successful:
            return jsonify({"status": "error", "message": "提交给 MoviePilot 失败，请联系管理员。"}), 500
        
        message = "订阅成功，已自动提交给 MoviePilot！"

    # --- 统一的通知逻辑 ---
    try:
        user_chat_id = user_db.get_user_telegram_chat_id(emby_user_id)
        if user_chat_id:
            item_name = data.get('item_name')
            if is_vip:
                message_text = f"✅ *您的订阅已自动处理*\n\n您订阅的 *{item_name}* 已成功提交订阅。"
                send_telegram_message(user_chat_id, message_text)
            else:
                message_text = f"🔔 *您的订阅请求已提交*\n\n您想看的 *{item_name}* 已进入待审队列，管理员处理后会通知您。"
                send_telegram_message(user_chat_id, message_text)
    except Exception as e:
        logger.error(f"发送订阅请求提交通知时出错: {e}")
        
    return jsonify({"status": "ok", "message": message})
    
# ★★★ 获取当前用户账户信息的接口 ★★★
@user_portal_bp.route('/account-info', methods=['GET'])
@emby_login_required # 必须登录才能访问
def get_account_info():
    """获取当前登录用户的详细账户信息，并附带全局配置信息。"""
    emby_user_id = session['emby_user_id']
    try:
        # 1. 照常获取用户的个人账户详情
        account_info = user_db.get_user_account_details(emby_user_id)
        
        # 2. ★★★ 核心修改：即使个人详情为空，也创建一个空字典 ★★★
        #    这样可以确保即使用户是新来的，也能看到全局频道信息。
        if not account_info:
            account_info = {}

        # 3. ★★★ 从全局配置中读取频道ID，并添加到返回的字典中 ★★★
        channel_id = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID)
        account_info['telegram_channel_id'] = channel_id
            
        return jsonify(account_info)
    except Exception as e:
        logger.error(f"为用户 {emby_user_id} 获取账户信息时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取账户信息失败"}), 500
    
@user_portal_bp.route('/subscription-history', methods=['GET'])
@emby_login_required
def get_subscription_history():
    """获取当前用户的订阅历史记录。"""
    emby_user_id = session['emby_user_id']
    try:
        history = user_db.get_user_subscription_history(emby_user_id)
        return jsonify(history)
    except Exception as e:
        logger.error(f"为用户 {emby_user_id} 获取订阅历史时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取订阅历史失败"}), 500
    
@user_portal_bp.route('/telegram-chat-id', methods=['POST'])
@emby_login_required
def save_telegram_chat_id():
    """保存当前用户的 Telegram Chat ID。"""
    data = request.json
    chat_id = data.get('chat_id', '').strip() # 获取并去除前后空格
    emby_user_id = session['emby_user_id']

    success = user_db.update_user_telegram_chat_id(emby_user_id, chat_id)
    if success:
        return jsonify({"status": "ok", "message": "Telegram Chat ID 保存成功！"})
    else:
        return jsonify({"status": "error", "message": "保存失败，请联系管理员"}), 500
    
@user_portal_bp.route('/telegram-bot-info', methods=['GET'])
@emby_login_required
def get_telegram_bot_info():
    """安全地获取 Telegram 机器人的用户名，并返回详细的错误信息。"""
    bot_token = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN)
    if not bot_token:
        return jsonify({"bot_username": None, "error": "Bot Token未配置"})

    try:
        api_url = f"https://api.telegram.org/bot{bot_token}/getMe"
        from config_manager import get_proxies_for_requests
        proxies = get_proxies_for_requests()
        
        # ★★★ 核心修改 1: 增加超时时间到20秒，给网络多一点机会 ★★★
        response = requests.get(api_url, timeout=20, proxies=proxies)
        
        if response.status_code == 200:
            bot_info = response.json()
            if bot_info.get("ok"):
                return jsonify({"bot_username": bot_info.get("result", {}).get("username")})
            else:
                # Token正确但API返回错误 (例如被吊销)
                error_desc = bot_info.get('description', '未知API错误')
                return jsonify({"bot_username": None, "error": f"Telegram API 错误: {error_desc}"})
        else:
            # HTTP请求失败
            return jsonify({"bot_username": None, "error": f"HTTP错误, 状态码: {response.status_code}"})

    except requests.RequestException as e:
        # ★★★ 核心修改 2: 捕获异常后，将错误信息返回给前端 ★★★
        logger.error(f"调用 Telegram getMe API 失败: {e}")
        # 将具体的网络错误（如超时）作为 error 字段返回
        return jsonify({"bot_username": None, "error": f"网络请求失败: {str(e)}"})