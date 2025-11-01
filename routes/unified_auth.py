# routes/unified_login.py
import logging
import os
from flask import Blueprint, request, jsonify, session
from werkzeug.security import generate_password_hash, check_password_hash
import config_manager
import constants
import emby_handler
from database import connection, user_db

# ★ 1. 创建一个新的蓝图，它将处理所有登录和状态检查
unified_auth_bp = Blueprint('unified_auth_bp', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

DEFAULT_INITIAL_PASSWORD = "password"
def init_auth():
    """初始化认证系统，仅在数据库没有用户时创建默认用户。"""
    auth_enabled = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_ENABLED, False)
    if not auth_enabled:
        logger.info("用户认证功能未启用。")
        return

    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users LIMIT 1")
            user_exists = cursor.fetchone()

            if user_exists:
                logger.info("  ➜ 数据库中已存在用户，跳过初始用户创建。")
                return

            logger.info("数据库中未发现任何用户，开始创建初始管理员账户。")
            
            env_username = os.environ.get("AUTH_USERNAME")
            if env_username:
                username = env_username.strip()
            else:
                username = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_USERNAME, constants.DEFAULT_USERNAME).strip()

            password_hash = generate_password_hash(DEFAULT_INITIAL_PASSWORD)
            
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                (username, password_hash)
            )
            conn.commit()
            
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
    """
    【统一登录接口 - V2 修正版】
    返回的 user 对象结构统一，包含所有前端需要的信息。
    """
    data = request.json
    username = data.get('username')
    password = data.get('password')
    login_type = data.get('loginType')

    if not all([username, password, login_type]):
        return jsonify({"status": "error", "message": "缺少用户名、密码或登录类型"}), 400

    session.clear()

    if login_type == 'local':
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
                local_user = cursor.fetchone()
            
            if local_user and check_password_hash(local_user['password_hash'], password):
                session['user_id'] = local_user['id']
                session['username'] = local_user['username']
                session.permanent = True
                logger.info(f"  ➜ [统一登录] 用户 '{username}' 作为本地管理员登录成功。")
                
                # ★★★ 核心修正 1/2：返回一个信息完整的 user 对象 ★★★
                return jsonify({
                    "status": "ok",
                    "user": { 
                        "name": local_user['username'],
                        "user_type": "local_admin", 
                        "is_admin": True,
                        "allow_unrestricted_subscriptions": True 
                    }
                }), 200
        except Exception as e:
            logger.error(f"  ➜ [统一登录] 本地认证时数据库出错: {e}", exc_info=True)
            return jsonify({"status": "error", "message": "服务器内部错误"}), 500

    elif login_type == 'emby':
        auth_result = emby_handler.authenticate_emby_user(username, password)
        if auth_result:
            user_info = auth_result.get('User', {})
            session['emby_user_id'] = user_info.get('Id')
            session['emby_username'] = user_info.get('Name')
            session['emby_is_admin'] = user_info.get('Policy', {}).get('IsAdministrator', False)
            session.permanent = True
            logger.info(f"  ➜ [统一登录] 用户 '{username}' 作为 Emby 用户登录成功。")

            can_subscribe_without_review = user_db.get_user_subscription_permission(session['emby_user_id'])
            
            # ★★★ Emby 登录也返回统一结构的 user 对象 ★★★
            return jsonify({
                "status": "ok",
                "user": {
                    "id": session['emby_user_id'],
                    "name": session['emby_username'],
                    "user_type": "emby_user", 
                    "is_admin": session['emby_is_admin'],
                    "allow_unrestricted_subscriptions": can_subscribe_without_review
                }
            }), 200

    logger.warning(f"  ➜ [统一登录] 用户 '{username}' 使用 '{login_type}' 方式登录失败。")
    return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

@unified_auth_bp.route('/status', methods=['GET'])
def unified_status():
    """【统一状态接口 - V2 修正版】返回与登录接口完全一致的 user 对象结构"""
    is_local_admin_logged_in = 'user_id' in session
    is_emby_user_logged_in = 'emby_user_id' in session
    
    response = {
        "logged_in": is_local_admin_logged_in or is_emby_user_logged_in,
        "user": {} 
    }

    # ★★★ 核心修正 2/2：无论哪种用户，都构建一个信息完整的 user 对象 ★★★
    if is_local_admin_logged_in:
        response["user"] = {
            "name": session.get('username'),
            "user_type": "local_admin",
            "is_admin": True,
            "allow_unrestricted_subscriptions": True
        }
    elif is_emby_user_logged_in:
        can_subscribe_without_review = user_db.get_user_subscription_permission(session['emby_user_id'])
        response["user"] = {
            "id": session.get('emby_user_id'),
            "name": session.get('emby_username'),
            "user_type": "emby_user",
            "is_admin": session.get('emby_is_admin'),
            "allow_unrestricted_subscriptions": can_subscribe_without_review
        }

    return jsonify(response)

@unified_auth_bp.route('/logout', methods=['POST'])
def unified_logout():
    """【统一登出接口】"""
    session.clear()
    return jsonify({"status": "ok", "message": "登出成功"})