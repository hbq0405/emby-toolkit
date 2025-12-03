# routes/unified_auth.py

import logging
import secrets
import time
from flask import Blueprint, request, jsonify, session
import config_manager
import constants
import handler.emby as emby
from database import user_db

unified_auth_bp = Blueprint('unified_auth_bp', __name__, url_prefix='/api/auth')
logger = logging.getLogger(__name__)

# --- 内存存储：灾难恢复令牌 ---
# 结构: { 'token_string': expiry_timestamp }
RECOVERY_TOKENS = {}

def clean_expired_tokens():
    """清理过期的令牌"""
    now = time.time()
    expired = [t for t, exp in RECOVERY_TOKENS.items() if now > exp]
    for t in expired:
        del RECOVERY_TOKENS[t]

@unified_auth_bp.route('/check_status', methods=['GET'])
def check_system_status():
    """
    【前端入口检查】
    前端 App.vue 加载时首先调用此接口，决定跳转到哪个页面。
    """
    # 1. 检查系统是否已配置
    if not config_manager.is_system_configured():
        return jsonify({
            "status": "setup_required", 
            "message": "系统未配置"
        }), 200
    
    # 2. 检查是否已登录
    if 'emby_user_id' in session:
        return jsonify({
            "status": "logged_in",
            "user": {
                "id": session['emby_user_id'],
                "name": session.get('emby_username'),
                "is_admin": session.get('emby_is_admin', False)
            }
        }), 200

    # 3. 既已配置又未登录 -> 需要登录
    return jsonify({"status": "login_required"}), 200

@unified_auth_bp.route('/login', methods=['POST'])
def emby_only_login():
    """
    【纯 Emby 登录接口】
    """
    data = request.json
    username = data.get('username')
    password = data.get('password')

    # 双重检查：如果系统没配置，不允许登录，强制去设置
    if not config_manager.is_system_configured():
        return jsonify({
            "status": "error", 
            "code": "SETUP_REQUIRED", 
            "message": "系统尚未配置 Emby 连接"
        }), 428 # 428 Precondition Required

    # 调用 Emby 验证
    auth_result = emby.authenticate_emby_user(username, password)
    
    if not auth_result:
        return jsonify({
            "status": "error", 
            "message": "登录失败：用户名/密码错误，或无法连接 Emby 服务器"
        }), 401

    user_info = auth_result.get('User', {})
    user_id = user_info.get('Id')
    
    # 同步用户基础信息到本地数据库
    try:
        user_db.upsert_emby_users_batch([user_info])
    except Exception as e:
        logger.warning(f"登录时同步用户信息失败: {e}")

    # 设置 Session
    session.clear() 
    session['emby_user_id'] = user_id
    session['emby_username'] = user_info.get('Name')
    session['emby_is_admin'] = user_info.get('Policy', {}).get('IsAdministrator', False)
    session.permanent = True

    logger.info(f"Emby 用户 '{session['emby_username']}' 登录成功。")
    
    # 获取用户权限信息
    can_subscribe = user_db.get_user_subscription_permission(user_id)
    
    return jsonify({
        "status": "ok",
        "user": {
            "id": user_id,
            "name": session['emby_username'],
            "is_admin": session['emby_is_admin'],
            "allow_unrestricted_subscriptions": can_subscribe,
            "user_type": "emby_user" # 前端兼容字段
        }
    }), 200

# ==========================================
#  设置与灾难恢复逻辑
# ==========================================

@unified_auth_bp.route('/request_recovery', methods=['POST'])
def request_recovery_token():
    """
    【步骤1】用户请求重置连接。
    生成一个 Token 打印到日志，不返回给前端。
    """
    clean_expired_tokens()
    
    # 生成 6 位随机字符作为令牌
    token = secrets.token_hex(3).upper() 
    # 有效期 5 分钟
    RECOVERY_TOKENS[token] = time.time() + 300 
    
    logger.critical("=" * 60)
    logger.critical(f"【安全警告】收到重置连接配置的请求。")
    logger.critical(f"若这是您本人的操作，请在页面输入以下安全令牌以进入设置模式:")
    logger.critical(f"安全令牌:  {token}")
    logger.critical(f"令牌有效期: 5 分钟")
    logger.critical("=" * 60)
    
    return jsonify({
        "status": "ok", 
        "message": "安全令牌已发送至服务器控制台日志(Docker Logs)，请查阅并输入。"
    }), 200

@unified_auth_bp.route('/verify_recovery', methods=['POST'])
def verify_recovery_token():
    """
    【步骤2】验证令牌。
    如果通过，给予临时 Session 权限进入设置页面。
    """
    data = request.json
    token = data.get('token', '').strip().upper()
    
    clean_expired_tokens()
    
    if token in RECOVERY_TOKENS:
        del RECOVERY_TOKENS[token] # 一次性使用
        session['is_setup_mode'] = True # 标记：允许访问 setup 接口
        return jsonify({"status": "ok", "message": "验证成功"}), 200
    
    return jsonify({"status": "error", "message": "令牌无效或已过期"}), 403

@unified_auth_bp.route('/setup', methods=['POST'])
def save_emby_config():
    """
    【步骤3】保存 Emby 配置。
    仅在 (系统未配置) 或 (拥有 setup_mode 权限) 时允许调用。
    """
    # 权限检查
    is_configured = config_manager.is_system_configured()
    has_setup_permission = session.get('is_setup_mode')
    
    if is_configured and not has_setup_permission:
        return jsonify({"status": "error", "message": "系统已配置，且无重置权限"}), 403

    data = request.json
    url = data.get('url')
    api_key = data.get('api_key')

    if not url or not api_key:
        return jsonify({"status": "error", "message": "URL 和 API Key 不能为空"}), 400

    # 1. 验证连接有效性
    logger.info(f"正在测试新的 Emby 配置: {url}")
    test_result = emby.test_connection(url, api_key) 
    
    if not test_result['success']:
        return jsonify({
            "status": "error", 
            "message": f"连接测试失败: {test_result.get('error')}"
        }), 400

    # 2. 保存配置
    new_config = {
        constants.CONFIG_OPTION_EMBY_SERVER_URL: url,
        constants.CONFIG_OPTION_EMBY_API_KEY: api_key
    }
    try:
        config_manager.save_config(new_config)
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        return jsonify({"status": "error", "message": "保存配置失败，请检查日志"}), 500
    
    # 3. 清除设置模式标记
    session.pop('is_setup_mode', None)
    
    logger.info("Emby 配置已更新，系统进入正常模式。")
    return jsonify({"status": "ok", "message": "配置保存成功，请登录"}), 200

@unified_auth_bp.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({"status": "ok"})