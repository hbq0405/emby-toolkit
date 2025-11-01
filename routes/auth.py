# routes/auth.py
# 这个文件现在专门负责本地管理员的账户管理

from flask import Blueprint, request, jsonify, session
from werkzeug.security import generate_password_hash, check_password_hash
import logging

from database import connection
from extensions import login_required # ★ 注意：这里仍然使用 login_required

# 1. 蓝图保持不变，它提供了 /api/auth 的前缀
auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')
logger = logging.getLogger(__name__)

# 2. login, logout, status, init_auth 函数都已移除

# 3. 只保留 change_password 函数
@auth_bp.route('/change_password', methods=['POST'])
@login_required # ★ 这个接口必须由本地管理员自己调用，所以用 login_required 是正确的
def change_password():
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password or len(new_password) < 6:
        return jsonify({"error": "缺少参数或新密码长度不足6位"}), 400

    user_id = session.get('user_id')
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = cursor.fetchone()

            if not user or not check_password_hash(user['password_hash'], current_password):
                return jsonify({"error": "当前密码不正确"}), 403

            new_password_hash = generate_password_hash(new_password)
            cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (new_password_hash, user_id))
            conn.commit()
    except Exception as e:
        logger.error(f"修改密码时发生数据库错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

    logger.info(f"用户 '{user['username']}' 成功修改密码。")
    return jsonify({"message": "密码修改成功"})