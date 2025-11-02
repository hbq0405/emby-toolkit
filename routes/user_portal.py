# routes/user_portal.py
import logging
from flask import Blueprint, jsonify, session, request

from extensions import emby_login_required # 保护我们的新接口
from database import user_db, settings_db
import moviepilot_handler # ★ 1. 导入我们的 MP 处理器
import config_manager     # ★ 2. 导入配置管理器，因为 MP 处理器需要它

# 1. 创建一个新的蓝图
user_portal_bp = Blueprint('user_portal_bp', __name__, url_prefix='/api/portal')
logger = logging.getLogger(__name__)

@user_portal_bp.route('/subscribe', methods=['POST'])
@emby_login_required
def request_subscription():
    """【V2 - 集成 MoviePilot】处理用户的订阅或“想看”请求。"""
    data = request.json
    emby_user_id = session['emby_user_id']
    
    # 判断用户是否为 VIP
    is_vip = user_db.get_user_subscription_permission(emby_user_id)

    status = 'approved' if is_vip else 'pending'
    processed_by = 'auto' if is_vip else None

    try:
        # 将申请写入数据库
        request_id = user_db.create_subscription_request(
            emby_user_id=emby_user_id,
            tmdb_id=str(data.get('tmdb_id')),
            item_type=data.get('item_type'),
            item_name=data.get('item_name'),
            status=status,
            processed_by=processed_by
        )

        # ★★★ 4. 核心改造：如果是 VIP，立即调用 MoviePilot ★★★
        if is_vip:
            logger.info(f"VIP 用户 {emby_user_id} 的订阅请求 (ID: {request_id}) 已自动批准，准备通过 MoviePilot 订阅...")
            
            # a. 首先检查配额
            current_quota = settings_db.get_subscription_quota()
            if current_quota <= 0:
                logger.warning(f"VIP 用户 {emby_user_id} 尝试自动订阅，但配额已用尽。")
                return jsonify({"status": "error", "message": "今日订阅配额已用尽，请明天再试。"}), 429 # 429 Too Many Requests

            # b. 构造 MoviePilot payload
            mp_payload = {
                "name": data.get('item_name'),
                "tmdbid": int(data.get('tmdb_id')),
                "type": "电影" if data.get('item_type') == 'Movie' else "电视剧"
            }
            
            # c. 调用 MoviePilot 订阅
            item_type = data.get('item_type')
            config = config_manager.APP_CONFIG

            parsed_info = None
            if item_type == 'Movie':
                mp_payload = { "name": data.get('item_name'), "tmdbid": int(data.get('tmdb_id')), "type": "电影" }
                if moviepilot_handler.subscribe_with_custom_payload(mp_payload, config):
                    parsed_info = {} # 电影没有解析信息，给个空字典表示成功
            elif item_type == 'Series':
                series_info = { "tmdb_id": int(data.get('tmdb_id')), "item_name": data.get('item_name') }
                parsed_info = moviepilot_handler.smart_subscribe_series(series_info, config)

            if parsed_info is not None: # ★★★ 判断是否成功
                settings_db.decrement_subscription_quota()
                
                # ★★★ 把所有信息都传给数据库函数 ★★★
                user_db.create_subscription_request(
                    emby_user_id=emby_user_id, tmdb_id=str(data.get('tmdb_id')),
                    item_type=item_type, item_name=data.get('item_name'),
                    status='approved', processed_by='auto',
                    **parsed_info # ★★★ 用 ** 解包字典，优雅地传入所有解析字段
                )
                message = "订阅成功，已自动提交给 MoviePilot！"
            else:
                return jsonify({"status": "error", "message": "提交给 MoviePilot 失败，请联系管理员。"}), 500
        else:
            message = "“想看”请求已提交，请等待管理员审核。"
            
        return jsonify({"status": "ok", "message": message})

    except Exception as e:
        logger.error(f"用户 {emby_user_id} 提交订阅请求时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "提交请求失败"}), 500
    
# ★★★ 获取当前用户账户信息的接口 ★★★
@user_portal_bp.route('/account-info', methods=['GET'])
@emby_login_required # 必须登录才能访问
def get_account_info():
    """获取当前登录用户的详细账户信息，如模板、有效期等。"""
    emby_user_id = session['emby_user_id']
    
    try:
        # 我们将在下一步的 user_db.py 中创建这个函数
        account_info = user_db.get_user_account_details(emby_user_id)
        if not account_info:
            return jsonify({"status": "error", "message": "找不到用户账户信息"}), 404
            
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