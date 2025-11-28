# routes/unified_auth.py

import logging
import os
from flask import Blueprint, request, jsonify, session
from werkzeug.security import generate_password_hash, check_password_hash 
import config_manager
import constants
import handler.emby as emby
from database import user_db
from extensions import login_required

unified_auth_bp = Blueprint('unified_auth_bp', __name__, url_prefix='/api/auth')
logger = logging.getLogger(__name__)

DEFAULT_INITIAL_PASSWORD = "password"

def init_auth():
    """初始化认证系统，仅在数据库没有用户时创建默认用户。"""
    auth_enabled = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_ENABLED, False)
    if not auth_enabled:
        logger.info("用户认证功能未启用。")
        return

    try:
        # ### 核心修改：调用新的数据库函数 ###
        if user_db.get_user_count() > 0:
            logger.info("  ➜ 数据库中已存在用户，跳过初始用户创建。")
            return

        logger.info("  ➜ 数据库中未发现任何用户，开始创建初始管理员账户。")
        
        env_username = os.environ.get("AUTH_USERNAME")
        username = env_username.strip() if env_username else config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_USERNAME, constants.DEFAULT_USERNAME).strip()
        password_hash = generate_password_hash(DEFAULT_INITIAL_PASSWORD)
        
        # ### 核心修改：调用新的数据库函数 ###
        user_db.create_initial_admin_user(username, password_hash)
        
        logger.critical("=" * 60)
        logger.critical(f"首次运行，已为用户 '{username}' 自动生成初始密码。")
        logger.critical(f"用户名: {username}")
        logger.critical(f"初始密码: {DEFAULT_INITIAL_PASSWORD}")
        logger.critical("请使用此密码登录，并修改密码。")
        logger.critical("=" * 60)

    except Exception as e:
        logger.error(f"初始化认证系统时发生错误: {e}", exc_info=True)

@unified_auth_bp.route('/login', methods=['POST'])
def unified_login():
    """【统一登录接口】"""
    data = request.json
    username = data.get('username')
    password = data.get('password')
    login_type = data.get('loginType')

    if not all([username, password, login_type]):
        return jsonify({"status": "error", "message": "缺少用户名、密码或登录类型"}), 400

    session.clear()

    if login_type == 'local':
        try:
            # ### 核心修改：调用新的数据库函数 ###
            local_user = user_db.get_local_user_by_username(username)
            if local_user and check_password_hash(local_user['password_hash'], password):
                session['user_id'] = local_user['id']
                session['username'] = local_user['username']
                session.permanent = True
                logger.info(f"  ➜ 用户 '{username}' 作为本地管理员登录成功。")
                
                return jsonify({
                    "status": "ok",
                    "user": { 
                        "name": local_user['username'], "user_type": "local_admin", 
                        "is_admin": True, "allow_unrestricted_subscriptions": True 
                    }
                }), 200
        except Exception as e:
            logger.error(f"  ➜ 本地认证时数据库出错: {e}", exc_info=True)
            return jsonify({"status": "error", "message": "服务器内部错误"}), 500

    elif login_type == 'emby':
        auth_result = emby.authenticate_emby_user(username, password)
        if auth_result:
            user_info = auth_result.get('User', {})
            session['emby_user_id'] = user_info.get('Id')
            session['emby_username'] = user_info.get('Name')
            session['emby_is_admin'] = user_info.get('Policy', {}).get('IsAdministrator', False)
            session.permanent = True
            logger.info(f"  ➜ 用户 '{username}' 作为 Emby 用户登录成功。")

            # --- 轻量级同步用户数据 ---
            try:
                # 仅同步基础信息，确保数据库里有这个人
                user_db.upsert_emby_users_batch([user_info])
            except Exception as e:
                logger.warning(f"  ➜ 登录时同步用户基础信息失败: {e}")

            can_subscribe_without_review = user_db.get_user_subscription_permission(session['emby_user_id'])
            
            return jsonify({
                "status": "ok",
                "user": {
                    "id": session['emby_user_id'], "name": session['emby_username'],
                    "user_type": "emby_user", "is_admin": session['emby_is_admin'],
                    "allow_unrestricted_subscriptions": can_subscribe_without_review
                }
            }), 200

    logger.warning(f"  ➜ 用户 '{username}' 使用 '{login_type}' 方式登录失败。")
    return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

@unified_auth_bp.route('/status', methods=['GET'])
def unified_status():
    """【统一状态接口】"""
    is_local_admin_logged_in = 'user_id' in session
    is_emby_user_logged_in = 'emby_user_id' in session
    
    response = {"logged_in": is_local_admin_logged_in or is_emby_user_logged_in, "user": {}}

    if is_local_admin_logged_in:
        response["user"] = {
            "name": session.get('username'), "user_type": "local_admin",
            "is_admin": True, "allow_unrestricted_subscriptions": True
        }
    elif is_emby_user_logged_in:
        can_subscribe_without_review = user_db.get_user_subscription_permission(session['emby_user_id'])
        response["user"] = {
            "id": session.get('emby_user_id'), "name": session.get('emby_username'),
            "user_type": "emby_user", "is_admin": session.get('emby_is_admin'),
            "allow_unrestricted_subscriptions": can_subscribe_without_review
        }
    return jsonify(response)

@unified_auth_bp.route('/logout', methods=['POST'])
def unified_logout():
    """【统一登出接口】"""
    session.clear()
    return jsonify({"status": "ok", "message": "登出成功"})

@unified_auth_bp.route('/change_password', methods=['POST'])
@login_required
def change_password():
    """修改本地管理员的密码。"""
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password or len(new_password) < 6:
        return jsonify({"error": "缺少参数或新密码长度不足6位"}), 400

    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "未授权或会话已过期"}), 401

    try:
        # ### 核心修改：调用新的数据库函数 ###
        user = user_db.get_local_user_by_id(user_id)

        if not user or not check_password_hash(user['password_hash'], current_password):
            return jsonify({"error": "当前密码不正确"}), 403

        new_password_hash = generate_password_hash(new_password)
        # ### 核心修改：调用新的数据库函数 ###
        user_db.update_local_user_password(user_id, new_password_hash)

    except Exception as e:
        logger.error(f"修改密码时发生数据库错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

    logger.info(f"用户 '{user['username']}' 成功修改密码。")
    return jsonify({"message": "密码修改成功"})