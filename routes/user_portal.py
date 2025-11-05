# routes/user_portal.py
import logging
import requests
from flask import Blueprint, jsonify, session, request

from extensions import emby_login_required # 保护我们的新接口
from database import user_db, settings_db
import handler.moviepilot as moviepilot # ★ 1. 导入我们的 MP 处理器
import config_manager     # ★ 2. 导入配置管理器，因为 MP 处理器需要它
import constants
from handler.telegram import send_telegram_message
from routes.discover import check_and_replenish_pool

# 1. 创建一个新的蓝图
user_portal_bp = Blueprint('user_portal_bp', __name__, url_prefix='/api/portal')
logger = logging.getLogger(__name__)

@user_portal_bp.route('/subscribe', methods=['POST'])
@emby_login_required
def request_subscription():
    """
    【V5.8 - 最终正确版】处理用户订阅请求。
    - VIP/管理员的请求拥有最高优先级，无视任何现有的 pending 状态。
    - 普通用户的请求在项目已有状态时会被拦截。
    """
    data = request.json
    emby_user_id = session['emby_user_id']
    emby_username = session.get('emby_username', emby_user_id)
    
    is_emby_admin = session.get('emby_is_admin', False)
    is_vip = user_db.get_user_subscription_permission(emby_user_id)
    
    tmdb_id = str(data.get('tmdb_id'))
    item_type = data.get('item_type')
    item_name = data.get('item_name')

    message = ""
    new_status_for_frontend = None

    # ★★★ 核心逻辑：VIP/管理员先进专属通道 ★★★
    if is_vip or is_emby_admin:
        # --- VIP 或管理员的自动订阅逻辑 (拥有最高优先级) ---
        # 这个通道里，不检查任何 existing_status，直接往下走！
        log_user_type = "管理员" if is_emby_admin else "VIP 用户"
        logger.info(f"  ➜ 【VIP通道】{log_user_type} '{emby_username}' 的订阅请求已自动批准...")
        
        if settings_db.get_subscription_quota() <= 0:
            logger.warning(f"{log_user_type} {emby_user_id} 尝试自动订阅，但配额已用尽。")
            return jsonify({"status": "error", "message": "今日订阅配额已用尽，请明天再试。"}), 429

        config = config_manager.APP_CONFIG
        subscription_successful = False
        seasons_subscribed_count = 0 # 初始化季数统计
        
        if item_type == 'Movie':
            mp_payload = { "name": item_name, "tmdbid": int(tmdb_id), "type": "电影" }
            if moviepilot.subscribe_with_custom_payload(mp_payload, config):
                settings_db.decrement_subscription_quota()
                user_db.create_subscription_request(
                    emby_user_id=emby_user_id, tmdb_id=tmdb_id,
                    item_type=item_type, item_name=item_name,
                    status='approved', processed_by='auto'
                )
                subscription_successful = True
        
        elif item_type == 'Series':
            series_info = { "tmdb_id": int(tmdb_id), "item_name": item_name }
            subscription_results = moviepilot.smart_subscribe_series(series_info, config)

            if subscription_results is not None:
                seasons_subscribed_count = len(subscription_results)
                # ... (内部处理多季订阅的逻辑保持不变) ...
                if not subscription_results:
                    user_db.create_subscription_request(
                        emby_user_id=emby_user_id, tmdb_id=tmdb_id, item_type=item_type,
                        item_name=item_name, status='approved', processed_by='auto'
                    )
                else:
                    for season_info in subscription_results:
                        if settings_db.get_subscription_quota() <= 0: break 
                        settings_db.decrement_subscription_quota()
                        user_db.create_subscription_request(
                            emby_user_id=emby_user_id, tmdb_id=str(season_info.get('parent_tmdb_id')),
                            item_type=item_type, item_name=f"{season_info.get('parsed_series_name')} - 第 {season_info.get('parsed_season_number')} 季",
                            status='approved', processed_by='auto', parent_tmdb_id=str(season_info.get('parent_tmdb_id')),
                            parsed_series_name=season_info.get('parsed_series_name'), parsed_season_number=season_info.get('parsed_season_number')
                        )
                subscription_successful = True 
        if subscription_successful:
            message = "订阅成功，已自动提交给 MoviePilot！"
            new_status_for_frontend = 'approved'
        else:
            # 处理订阅失败的情况
            pass
    else:
        # --- 普通用户通道 ---
        # ★ 只有普通用户才需要检查全局状态
        existing_status = user_db.get_global_subscription_status_by_tmdb_id(tmdb_id)
        if existing_status:
            message = "该项目正在等待审核。" if existing_status == 'pending' else "该项目已在订阅队列中。"
            return jsonify({"status": existing_status, "message": message}), 200
        
        # 如果没有全局状态，则为普通用户创建待审请求
        user_db.create_subscription_request(
            emby_user_id=emby_user_id, tmdb_id=tmdb_id, item_type=item_type,
            item_name=item_name, status='pending'
        )
        message = "“想看”请求已提交，请等待管理员审核。"
        new_status_for_frontend = 'pending'

        # 给管理员发送需要审核的通知
        try:
            # 1. 调用我们100%正确的函数
            admin_chat_ids = user_db.get_admin_telegram_chat_ids()
            
            if admin_chat_ids:
                notification_text = (
                    f"🔔 *新的订阅审核请求*\n\n"
                    f"用户 *{emby_username}* 提交了想看请求：\n"
                    f"*{item_name}*\n\n"
                    f"请前往管理后台审核。"
                )
                # 2. 循环列表，给每个查询到的管理员都发送通知
                for admin_id in admin_chat_ids:
                    logger.debug(f"  ➜ 正在向管理员 (TGID: {admin_id}) 发送新的审核请求通知...")
                    send_telegram_message(admin_id, notification_text)
            else:
                logger.warning("  ➜ 未查询到任何已配置Telegram的管理员，无法发送审核通知。")
        except Exception as e:
            logger.error(f"  ➜ 发送管理员审核通知时出错: {e}", exc_info=True)

    # 只要成功创建了 'approved' 或 'pending' 状态的请求，并且是电影，就执行此逻辑块
    if new_status_for_frontend in ['approved', 'pending'] and item_type == 'Movie':
        logger.info(f"  ➜ 订阅请求已创建 (状态: {new_status_for_frontend})，开始更新推荐池...")
        # 第一步：从池中移除当前这个电影，避免重复推荐
        settings_db.remove_item_from_recommendation_pool(tmdb_id)
        # 第二步：检查库存并决定是否需要补充
        check_and_replenish_pool()

    # --- 统一的通知逻辑 ---
    try:
        user_chat_id = user_db.get_user_telegram_chat_id(emby_user_id)
        if user_chat_id:
            # ★★★ 核心修改 3/3：通知逻辑也使用相同的条件 ★★★
            if is_vip or is_emby_admin:
                if item_type == 'Series' and seasons_subscribed_count > 1:
                    message_text = f"✅ *您的订阅已自动处理*\n\n您订阅的 *{item_name}* 已成功提交订阅，共计 *{seasons_subscribed_count}* 季。"
                else:
                    message_text = f"✅ *您的订阅已自动处理*\n\n您订阅的 *{item_name}* 已成功提交订阅。"
                send_telegram_message(user_chat_id, message_text)
            else:
                message_text = f"🔔 *您的订阅请求已提交*\n\n您想看的 *{item_name}* 已进入待审队列，管理员处理后会通知您。"
                send_telegram_message(user_chat_id, message_text)
    except Exception as e:
        logger.error(f"发送订阅请求提交通知时出错: {e}")
        
    return jsonify({"status": new_status_for_frontend, "message": message})
    
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
    """获取当前用户的订阅历史记录，支持分页。"""
    emby_user_id = session['emby_user_id']
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 10, type=int)
    
    try:
        history, total_records = user_db.get_user_subscription_history(emby_user_id, page, page_size)
        return jsonify({
            "items": history,
            "total_records": total_records,
            "page": page,
            "page_size": page_size
        })
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
