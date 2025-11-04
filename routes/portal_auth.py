# routes/portal_auth.py
import logging
from flask import Blueprint, request, jsonify, session

# 导入我们正确的认证函数和装饰器
import handler.emby as emby
from extensions import emby_login_required

# 创建蓝图
portal_auth_bp = Blueprint('portal_auth_bp', __name__, url_prefix='/api/portal')
logger = logging.getLogger(__name__)

@portal_auth_bp.route('/login', methods=['POST'])
def portal_login():
    """处理 Emby 用户的登录请求，使用 Session 机制"""
    data = request.json
    username = data.get('username')
    password = data.get('password')

    if not username or password is None:
        return jsonify({"status": "error", "message": "用户名和密码不能为空"}), 400

    # 1. 直接调用 emby_handler 中的函数，它会自己处理所有细节
    auth_result = emby.authenticate_emby_user(username, password)

    if not auth_result:
        return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

    # 2. 从返回结果中提取关键信息
    user_info = auth_result.get('User', {})
    emby_user_id = user_info.get('Id')
    emby_is_admin = user_info.get('Policy', {}).get('IsAdministrator', False)

    if not emby_user_id:
        return jsonify({"status": "error", "message": "认证成功但无法获取用户ID"}), 500

    # 3. 使用独立的 session key 来存储 Emby 用户信息
    session['emby_user_id'] = emby_user_id
    session['emby_username'] = user_info.get('Name')
    session['emby_is_admin'] = emby_is_admin
    session.permanent = True

    logger.info(f"Emby 用户 '{username}' (ID: {emby_user_id}) 登录门户成功。")

    # 4. 返回成功响应
    return jsonify({
        "status": "ok",
        "user": {
            "id": emby_user_id,
            "name": session['emby_username'],
            "is_admin": emby_is_admin
        }
    }), 200

@portal_auth_bp.route('/logout', methods=['POST'])
@emby_login_required
def portal_logout():
    """处理 Emby 用户的登出请求"""
    username = session.pop('emby_username', '未知Emby用户')
    session.pop('emby_user_id', None)
    session.pop('emby_is_admin', None)
    logger.info(f"Emby 用户 '{username}' 已成功登出。")
    return jsonify({"status": "ok"}), 200

@portal_auth_bp.route('/status', methods=['GET'])
def portal_check_status():
    """检查 Emby 用户的登录状态"""
    if 'emby_user_id' in session:
        return jsonify({
            "logged_in": True,
            "user": {
                "id": session['emby_user_id'],
                "name": session.get('emby_username'),
                "is_admin": session.get('emby_is_admin', False)
            }
        }), 200
    else:
        return jsonify({"logged_in": False}), 200