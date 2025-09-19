import uuid
import json
import logging
import psycopg2
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify
import db_handler
import emby_handler
import config_manager
import constants
from extensions import login_required

# 创建一个新的蓝图
user_management_bp = Blueprint('user_management_bp', __name__)

logger = logging.getLogger(__name__)

# --- 模块 1: 用户模板管理 (Templates) ---
@user_management_bp.route('/api/admin/user_templates', methods=['GET'])
@login_required
def get_all_templates():
    """获取所有用户模板"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, description, default_expiration_days FROM user_templates ORDER BY name")
            templates = [dict(row) for row in cursor.fetchall()]
        return jsonify(templates), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates', methods=['POST'])
@login_required
def create_template():
    """创建一个新的用户模板"""
    data = request.json
    name = data.get('name')
    description = data.get('description')
    default_expiration_days = data.get('default_expiration_days', 30)
    source_emby_user_id = data.get('source_emby_user_id') # 前端需要提供一个作为模板的 Emby 用户ID

    if not name or not source_emby_user_id:
        return jsonify({"status": "error", "message": "模板名称和源用户ID不能为空"}), 400

    try:
        # 从源用户获取 Policy
        config = config_manager.APP_CONFIG
        user_details = emby_handler.get_user_details(
            source_emby_user_id, config.get("emby_server_url"), config.get("emby_api_key")
        )
        if not user_details or 'Policy' not in user_details:
            return jsonify({"status": "error", "message": "无法获取源用户的权限策略"}), 404
        
        policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)

        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO user_templates (name, description, emby_policy_json, default_expiration_days)
                VALUES (%s, %s, %s, %s) RETURNING id
                """,
                (name, description, policy_json, default_expiration_days)
            )
            new_id = cursor.fetchone()['id']
            conn.commit()
        
        return jsonify({"status": "ok", "message": "模板创建成功", "id": new_id}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# (未来可以添加更新和删除模板的 API)

# --- 模块 2: 邀请链接管理 (Invitations) ---

@user_management_bp.route('/api/admin/invitations', methods=['POST'])
@login_required
def create_invitation():
    """生成一个新的邀请链接"""
    data = request.json
    template_id = data.get('template_id')
    expiration_days_from_req = data.get('expiration_days') # 先用一个临时变量接收
    link_expires_in_days = data.get('link_expires_in_days', 7)

    if not template_id:
        return jsonify({"status": "error", "message": "必须选择一个模板"}), 400

    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复：在这里进行健壮的逻辑判断 ★★★
            final_expiration_days = None
            # 1. 检查前端是否传来了一个有效的值 (不是 None 也不是空字符串)
            if expiration_days_from_req is not None and str(expiration_days_from_req).strip() != '':
                try:
                    # 2. 如果有值，就用它
                    final_expiration_days = int(expiration_days_from_req)
                except (ValueError, TypeError):
                    # 3. 如果值无效（比如传了个乱七八糟的字符串），则忽略它，后面会用默认值
                    pass
            
            # 4. 如果经过上面的处理，我们还是没有拿到有效期，就从模板里查默认值
            if final_expiration_days is None:
                cursor.execute("SELECT default_expiration_days FROM user_templates WHERE id = %s", (template_id,))
                template = cursor.fetchone()
                if not template:
                    return jsonify({"status": "error", "message": "模板不存在"}), 404
                final_expiration_days = template['default_expiration_days']

            token = str(uuid.uuid4())
            expires_at = datetime.now(timezone.utc) + timedelta(days=link_expires_in_days)

            cursor.execute(
                """
                INSERT INTO invitations (token, template_id, expiration_days, expires_at, status)
                VALUES (%s, %s, %s, %s, 'active')
                """,
                # ★★★ 使用我们最终计算好的有效期 ★★★
                (token, template_id, final_expiration_days, expires_at)
            )
            conn.commit()
            
            app_base_url = config_manager.APP_CONFIG.get("app_base_url", request.host_url.rstrip('/'))
            invite_link = f"{app_base_url}/register/invite/{token}"

        return jsonify({"status": "ok", "invite_link": invite_link}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 模块 3: 用户注册 (Public Facing) ---

@user_management_bp.route('/api/register/invite/validate/<string:token>', methods=['GET'])
def validate_invite_token(token):
    """公开API：验证邀请码是否有效，供注册页面加载时调用"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status, expires_at FROM invitations WHERE token = %s", (token,)
            )
            invitation = cursor.fetchone()
            if not invitation:
                return jsonify({"valid": False, "reason": "邀请链接不存在"}), 404
            
            if invitation['status'] != 'active':
                return jsonify({"valid": False, "reason": f"邀请链接已{invitation['status']}"}), 410 # 410 Gone
            
            if invitation['expires_at'] and invitation['expires_at'] < datetime.now(timezone.utc):
                return jsonify({"valid": False, "reason": "邀请链接已过期"}), 410

        return jsonify({"valid": True}), 200
    except Exception as e:
        return jsonify({"valid": False, "reason": "服务器内部错误"}), 500

@user_management_bp.route('/api/register/invite', methods=['POST'])
def register_with_invite():
    """公开API：处理带邀请码的注册请求"""
    data = request.json
    username = data.get('username')
    password = data.get('password')
    token = data.get('token')

    if not all([username, password, token]):
        return jsonify({"status": "error", "message": "用户名、密码和邀请码不能为空"}), 400

    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("BEGIN;")
            cursor.execute(
                "SELECT * FROM invitations WHERE token = %s AND status = 'active' FOR UPDATE", (token,)
            )
            invitation = cursor.fetchone()
            if not invitation:
                conn.rollback()
                return jsonify({"status": "error", "message": "邀请链接无效或已被使用"}), 400

            cursor.execute("SELECT * FROM user_templates WHERE id = %s", (invitation['template_id'],))
            template = cursor.fetchone()
            if not template:
                conn.rollback()
                return jsonify({"status": "error", "message": "内部错误：找不到关联的模板"}), 500

            config = config_manager.APP_CONFIG
            if emby_handler.check_if_user_exists(username, config.get("emby_server_url"), config.get("emby_api_key")):
                conn.rollback()
                return jsonify({"status": "error", "message": "该用户名已被占用"}), 409

            # ★★★ 核心修改点 ★★★

            # 1. 调用【纯净版】的创建函数，它不再需要 policy 参数
            new_user_id = emby_handler.create_user_with_policy(
                username, password,
                config.get("emby_server_url"), config.get("emby_api_key")
            )
            if not new_user_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "在 Emby 中创建用户失败，请联系管理员"}), 500

            # 2. 用户创建成功后，立刻调用强制设置函数，应用模板中的完整 Policy
            # 这一步和之前一样，现在是它唯一负责设置权限的地方
            template_policy = template['emby_policy_json']
            policy_applied = emby_handler.force_set_user_policy(
                new_user_id, template_policy,
                config.get("emby_server_url"), config.get("emby_api_key")
            )
            if not policy_applied:
                conn.rollback()
                logger.error(f"用户 {username} (ID: {new_user_id}) 创建成功，但应用模板权限失败！已回滚。")
                return jsonify({"status": "error", "message": "应用模板权限失败，请联系管理员"}), 500

            # 3. 后续的数据库操作保持不变
            cursor.execute(
                "INSERT INTO emby_users (id, name, is_administrator) VALUES (%s, %s, %s)",
                (new_user_id, username, False)
            )
            expiration_date = datetime.now(timezone.utc) + timedelta(days=invitation['expiration_days'])
            cursor.execute(
                """
                INSERT INTO emby_users_extended (emby_user_id, status, expiration_date, created_by)
                VALUES (%s, 'active', %s, 'self-registered')
                """,
                (new_user_id, expiration_date)
            )
            cursor.execute(
                "UPDATE invitations SET status = 'used', used_by_user_id = %s WHERE id = %s",
                (new_user_id, invitation['id'])
            )
            
            conn.commit()

        # ★★★ 核心修改点：在成功响应中增加跳转地址 ★★★
        config = config_manager.APP_CONFIG
        # 1. 从配置中读取自定义的跳转地址
        custom_redirect_url = config.get(constants.CONFIG_OPTION_REGISTRATION_REDIRECT_URL)
        
        # 2. 如果自定义地址不为空，就用它；否则，使用 Emby 服务器地址作为默认值
        if custom_redirect_url and custom_redirect_url.strip():
            final_redirect_url = custom_redirect_url
        else:
            final_redirect_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)

        return jsonify({
            "status": "ok", 
            "message": "注册成功！",
            "redirect_url": final_redirect_url # 3. 将最终地址返回给前端
        }), 201

        return jsonify({"status": "ok", "message": "注册成功！"}), 201
    except Exception as e:
        if 'conn' in locals() and conn and conn.status != psycopg2.extensions.STATUS_READY:
            try:
                conn.rollback()
                logger.warning("注册失败，数据库事务已回滚。")
            except Exception as rollback_e:
                logger.error(f"尝试回滚事务时发生额外错误: {rollback_e}")
        
        logger.error(f"用户注册时发生严重错误: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "注册过程中发生服务器内部错误，请联系管理员。"}), 500
@user_management_bp.route('/api/admin/invitations', methods=['GET'])
@login_required
def get_all_invitations():
    """获取所有已生成的邀请码及其状态"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 JOIN 查询，一次性把模板名称也查出来
            cursor.execute("""
                SELECT i.*, t.name as template_name 
                FROM invitations i
                JOIN user_templates t ON i.template_id = t.id
                ORDER BY i.created_at DESC
            """)
            invitations = [dict(row) for row in cursor.fetchall()]
        return jsonify(invitations)
    except Exception as e:
        logger.error(f"获取邀请码列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取邀请码列表失败"}), 500

@user_management_bp.route('/api/admin/invitations/<int:invitation_id>', methods=['DELETE'])
@login_required
def delete_invitation(invitation_id):
    """删除一个邀请码"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM invitations WHERE id = %s", (invitation_id,))
            conn.commit()
            if cursor.rowcount > 0:
                return jsonify({"status": "ok", "message": "邀请码已删除"}), 200
            else:
                return jsonify({"status": "error", "message": "未找到该邀请码"}), 404
    except Exception as e:
        logger.error(f"删除邀请码 {invitation_id} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "删除邀请码失败"}), 500
@user_management_bp.route('/api/admin/users', methods=['GET'])
@login_required
def get_all_managed_users():
    """获取所有 Emby 用户，并用我们数据库中的扩展信息丰富他们"""
    try:
        config = config_manager.APP_CONFIG
        # 1. 从 Emby 获取所有用户的基础信息
        all_emby_users = emby_handler.get_all_emby_users_from_server(
            config.get("emby_server_url"), config.get("emby_api_key")
        )
        if all_emby_users is None:
            return jsonify({"error": "无法从 Emby 获取用户列表"}), 500

        # 2. 从我们的数据库获取所有扩展信息
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM emby_users_extended")
            extended_info_rows = cursor.fetchall()
            extended_info_map = {row['emby_user_id']: dict(row) for row in extended_info_rows}

        # 3. 合并数据
        enriched_users = []
        for user in all_emby_users:
            user_id = user.get('Id')
            extended_data = extended_info_map.get(user_id, {})
            
            # 将 Policy 中的 IsDisabled 状态作为最终的用户状态
            user['IsDisabled'] = user.get('Policy', {}).get('IsDisabled', False)
            
            # 合并我们的扩展字段
            user['expiration_date'] = extended_data.get('expiration_date')
            user['status_in_db'] = extended_data.get('status') # 我们数据库里的状态
            
            enriched_users.append(user)
            
        return jsonify(enriched_users)
    except Exception as e:
        logger.error(f"获取托管用户列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取用户列表失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/status', methods=['POST'])
@login_required
def set_user_status(user_id):
    """手动禁用或启用一个用户"""
    data = request.json
    disable = data.get('disable', False) # True 为禁用, False 为启用
    
    config = config_manager.APP_CONFIG
    success = emby_handler.set_user_disabled_status(
        user_id, disable, config.get("emby_server_url"), config.get("emby_api_key")
    )
    
    if success:
        # 同时更新我们自己数据库的状态
        new_status = 'disabled' if disable else 'active'
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE emby_users_extended SET status = %s WHERE emby_user_id = %s",
                (new_status, user_id)
            )
            conn.commit()
        return jsonify({"status": "ok", "message": "用户状态已更新"}), 200
    else:
        return jsonify({"status": "error", "message": "更新用户状态失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/expiration', methods=['POST'])
@login_required
def set_user_expiration(user_id):
    """设置或清除用户的有效期"""
    data = request.json
    # 前端可以传来一个 ISO 格式的日期字符串，或者 null 来清除有效期
    expiration_date = data.get('expiration_date') 
    
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE emby_users_extended SET expiration_date = %s WHERE emby_user_id = %s",
                (expiration_date, user_id)
            )
            conn.commit()
        return jsonify({"status": "ok", "message": "用户有效期已更新"}), 200
    except Exception as e:
        logger.error(f"更新用户 {user_id} 有效期时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "更新有效期失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>', methods=['DELETE'])
@login_required
def delete_user(user_id):
    """从 Emby 和本地数据库中彻底删除一个用户"""
    config = config_manager.APP_CONFIG
    
    # ★★★ 核心修改：调用我们刚刚创建的、正确的删除函数 ★★★
    success = emby_handler.delete_emby_user(
        user_id, 
        config.get("emby_server_url"), 
        config.get("emby_api_key") # api_key 仍然需要，用于登录获取令牌
    )
    
    if success:
        # Emby 删除成功后，数据库记录会因为外键的 ON DELETE CASCADE 自动被清理
        return jsonify({"status": "ok", "message": "用户已彻底删除"}), 200
    else:
        return jsonify({"status": "error", "message": "在 Emby 中删除用户失败"}), 500
@user_management_bp.route('/api/admin/user_templates/<int:template_id>', methods=['DELETE'])
@login_required
def delete_template(template_id):
    """删除一个用户模板"""
    try:
        with db_handler.get_db_connection() as conn:
            cursor = conn.cursor()
            # 我们设置了外键 ON DELETE CASCADE，所以删除模板时，
            # 关联的邀请链接会自动被删除。
            cursor.execute("DELETE FROM user_templates WHERE id = %s", (template_id,))
            conn.commit()
            
            if cursor.rowcount > 0:
                return jsonify({"status": "ok", "message": "模板已删除"}), 200
            else:
                return jsonify({"status": "error", "message": "未找到该模板"}), 404
    except psycopg2.Error as e:
        # 捕获可能的外键约束错误
        if e.pgcode == '23503': # foreign_key_violation
             return jsonify({"status": "error", "message": "无法删除：仍有邀请链接在使用此模板。"}), 409
        logger.error(f"删除模板 {template_id} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "删除模板失败"}), 500
    except Exception as e:
        logger.error(f"删除模板 {template_id} 时发生未知错误: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "删除模板时发生未知错误"}), 500
