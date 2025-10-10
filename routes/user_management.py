import uuid
import json
import time
import logging
import psycopg2
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify

import emby_handler
import config_manager
import constants
from extensions import login_required
from database import connection
# 创建一个新的蓝图
user_management_bp = Blueprint('user_management_bp', __name__)

logger = logging.getLogger(__name__)

# --- 模块 1: 用户模板管理 (Templates) ---
@user_management_bp.route('/api/admin/user_templates', methods=['GET'])
@login_required
def get_all_templates():
    """【V2 - 返回源用户ID】获取所有用户模板"""
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 
                    id, 
                    name, 
                    description, 
                    default_expiration_days, 
                    source_emby_user_id,
                    COALESCE(max_concurrent_streams, 1) as max_concurrent_streams
                FROM user_templates 
                ORDER BY name
                """
            )
            templates = [dict(row) for row in cursor.fetchall()]
        return jsonify(templates), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates', methods=['POST'])
@login_required
def create_template():
    """【V3 - 支持首选项】创建一个新的用户模板"""
    data = request.json
    name = data.get('name')
    description = data.get('description')
    default_expiration_days = data.get('default_expiration_days', 30)
    source_emby_user_id = data.get('source_emby_user_id')
    include_configuration = data.get('include_configuration', False)
    max_concurrent_streams = data.get('max_concurrent_streams', 1)

    if not name or not source_emby_user_id:
        return jsonify({"status": "error", "message": "模板名称和源用户ID不能为空"}), 400

    try:
        config = config_manager.APP_CONFIG
        user_details = emby_handler.get_user_details(
            source_emby_user_id, config.get("emby_server_url"), config.get("emby_api_key")
        )
        if not user_details or 'Policy' not in user_details:
            return jsonify({"status": "error", "message": "无法获取源用户的权限策略"}), 404
        
        policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)
        # ★★★ 新增：根据标志决定是否保存首选项 ★★★
        configuration_json = None
        if include_configuration and 'Configuration' in user_details:
            configuration_json = json.dumps(user_details['Configuration'], ensure_ascii=False)

        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # ★★★ 核心修改：在 INSERT 语句中增加 emby_configuration_json ★★★
            cursor.execute(
                """
                INSERT INTO user_templates (name, description, emby_policy_json, default_expiration_days, source_emby_user_id, emby_configuration_json, max_concurrent_streams)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (name, description, policy_json, default_expiration_days, source_emby_user_id, configuration_json, max_concurrent_streams)
            )
            new_id = cursor.fetchone()['id']
            conn.commit()
        
        return jsonify({"status": "ok", "message": "模板创建成功", "id": new_id}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates/<int:template_id>/sync', methods=['POST'])
@login_required
def sync_template(template_id):
    """【V3 - 修正版】从源用户同步并更新模板，并将其应用到所有关联用户。"""
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT source_emby_user_id, name, emby_configuration_json IS NOT NULL as has_config FROM user_templates WHERE id = %s", (template_id,))
            template = cursor.fetchone()
            if not template: return jsonify({"status": "error", "message": "模板不存在"}), 404
            
            source_user_id = template.get('source_emby_user_id')
            template_name = template.get('name')
            template_has_config = template.get('has_config')
            if not source_user_id: return jsonify({"status": "error", "message": f"无法同步：模板 '{template_name}' 没有记录源用户信息。"}), 400

            logger.info(f"正在为模板 '{template_name}' 从源用户 {source_user_id} 同步最新权限和首选项...")

            config = config_manager.APP_CONFIG
            user_details = emby_handler.get_user_details(
                source_user_id, config.get("emby_server_url"), config.get("emby_api_key")
            )
            if not user_details or 'Policy' not in user_details: return jsonify({"status": "error", "message": "无法获取源用户的最新权限策略。"}), 404
            
            new_policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)
            new_policy_dict = user_details['Policy']
            
            new_config_json = None
            new_config_dict = None
            if template_has_config and 'Configuration' in user_details:
                new_config_json = json.dumps(user_details['Configuration'], ensure_ascii=False)
                new_config_dict = user_details['Configuration']

            cursor.execute(
                "UPDATE user_templates SET emby_policy_json = %s, emby_configuration_json = %s WHERE id = %s",
                (new_policy_json, new_config_json, template_id)
            )
            logger.info(f"模板 '{template_name}' 的数据库记录已更新。")

            cursor.execute(
                "SELECT u.id, u.name FROM emby_users_extended uex JOIN emby_users u ON uex.emby_user_id = u.id WHERE uex.template_id = %s",
                (template_id,)
            )
            users_to_update = cursor.fetchall()
            
            successful_pushes = 0
            if users_to_update:
                logger.warning(f"检测到 {len(users_to_update)} 个用户正在使用此模板，将开始逐一推送新配置...")
                for user in users_to_update:
                    user_id = user['id']
                    user_name = user['name']
                    logger.info(f"  ➜ 正在将新配置应用到用户 '{user_name}'...")
                    
                    # ★★★ 核心修正：将 (...) 替换为完整的函数参数 ★★★
                    policy_applied = emby_handler.force_set_user_policy(
                        user_id, 
                        new_policy_dict,
                        config.get("emby_server_url"), 
                        config.get("emby_api_key")
                    )
                    
                    config_applied = True
                    if new_config_dict:
                        config_applied = emby_handler.force_set_user_configuration(
                            user_id, new_config_dict,
                            config.get("emby_server_url"), config.get("emby_api_key")
                        )

                    if policy_applied and config_applied:
                        successful_pushes += 1
                    else:
                        logger.error(f"  ➜ 为用户 '{user_name}' (ID: {user_id}) 推送新配置失败！")
                    
                    time.sleep(0.2)
                
                logger.info(f"配置推送完成！共成功更新了 {successful_pushes}/{len(users_to_update)} 个用户。")
            else:
                logger.info("没有用户正在使用此模板，无需推送配置。")

            conn.commit()
            
            return jsonify({"status": "ok", "message": f"模板已更新，并已成功应用到 {successful_pushes} 个用户！"}), 200

    except Exception as e:
        logger.error(f"同步模板 {template_id} 时发生严重错误: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

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
        with connection.get_db_connection() as conn:
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
        with connection.get_db_connection() as conn:
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
        with connection.get_db_connection() as conn:
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
            
            # ★★★ 3. 应用首选项配置 ★★★
            template_config = template.get('emby_configuration_json')
            if template_config:
                logger.info(f"正在为新用户 {username} 应用模板中的个性化首选项...")
                emby_handler.force_set_user_configuration(
                    new_user_id, template_config,
                    config.get("emby_server_url"), config.get("emby_api_key")
                )

            # 4. 后续的数据库操作保持不变
            cursor.execute(
                "INSERT INTO emby_users (id, name, is_administrator) VALUES (%s, %s, %s)",
                (new_user_id, username, False)
            )
            expiration_date = None # 默认设置为 None (即 NULL)
            if invitation['expiration_days'] > 0:
                # 只有当有效期天数大于0时，才计算具体的到期日期
                expiration_date = datetime.now(timezone.utc) + timedelta(days=invitation['expiration_days'])
            
            cursor.execute(
                """
                INSERT INTO emby_users_extended (emby_user_id, status, expiration_date, created_by, template_id)
                VALUES (%s, 'active', %s, 'self-registered', %s)
                """,
                (new_user_id, expiration_date, invitation['template_id'])
            )
            cursor.execute(
                "UPDATE invitations SET status = 'used', used_by_user_id = %s WHERE id = %s",
                (new_user_id, invitation['id'])
            )
            
            conn.commit()

        # ★★★ 核心修改点：构建一个包含详细信息的数据包返回给前端 ★★★
        config = config_manager.APP_CONFIG
        
        # 1. 准备跳转地址
        custom_redirect_url = config.get(constants.CONFIG_OPTION_REGISTRATION_REDIRECT_URL)
        final_redirect_url = custom_redirect_url.strip() or config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)

        # 2. 准备有效期显示信息
        expiration_info = "永久有效"
        if 'expiration_date' in locals() and expiration_date:
            # 格式化日期为 YYYY-MM-DD
            expiration_info = f"至 {expiration_date.strftime('%Y-%m-%d')}"

        # ★★★ 新增逻辑：获取模板描述 ★★★
        template_description = template.get('description') or template.get('name') # 如果描述为空，用模板名作为备用

        # 3. 将所有信息打包返回
        return jsonify({
            "status": "ok", 
            "message": "注册成功！",
            "data": {
                "username": username,
                "expiration_info": expiration_info,
                "redirect_url": final_redirect_url,
                "template_description": template_description # <-- 新增返回字段
            }
        }), 201
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
        with connection.get_db_connection() as conn:
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
        with connection.get_db_connection() as conn:
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
    
def _get_all_source_user_ids(cursor) -> set:
    """从数据库中获取所有被用作模板源用户的ID集合，用于过滤。"""
    try:
        cursor.execute("SELECT DISTINCT source_emby_user_id FROM user_templates WHERE source_emby_user_id IS NOT NULL")
        # 返回一个集合(set)，用于实现 O(1) 的高效查找
        return {row['source_emby_user_id'] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"获取源用户ID列表时出错: {e}", exc_info=True)
        return set() # 出错时返回空集合，确保不会意外过滤掉所有用户

@user_management_bp.route('/api/admin/users', methods=['GET'])
@login_required
def get_all_managed_users():
    """
    【V2 - 隐藏源用户】获取所有 Emby 用户，并自动过滤掉被用作模板源的用户。
    """
    try:
        config = config_manager.APP_CONFIG
        all_emby_users = emby_handler.get_all_emby_users_from_server(
            config.get("emby_server_url"), config.get("emby_api_key")
        )
        if all_emby_users is None:
            return jsonify({"error": "无法从 Emby 获取用户列表"}), 500

        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复 1/2：在开始时就获取所有源用户的ID ★★★
            source_user_ids = _get_all_source_user_ids(cursor)
            if source_user_ids:
                logger.debug(f"将从用户列表中隐藏 {len(source_user_ids)} 个源用户。")

            cursor.execute("""
                SELECT eue.*, ut.name as template_name 
                FROM emby_users_extended eue
                LEFT JOIN user_templates ut ON eue.template_id = ut.id
            """)
            extended_info_rows = cursor.fetchall()
            extended_info_map = {row['emby_user_id']: dict(row) for row in extended_info_rows}

        enriched_users = []
        for user in all_emby_users:
            user_id = user.get('Id')
            
            # ★★★ 核心修复 2/2：在这里执行过滤 ★★★
            if user_id in source_user_ids:
                continue # 如果是源用户，则直接跳过，不添加到最终列表

            extended_data = extended_info_map.get(user_id, {})
            
            user['IsDisabled'] = user.get('Policy', {}).get('IsDisabled', False)
            
            user['expiration_date'] = extended_data.get('expiration_date')
            user['status_in_db'] = extended_data.get('status')
            user['template_id'] = extended_data.get('template_id')
            user['template_name'] = extended_data.get('template_name')
            
            enriched_users.append(user)
            
        return jsonify(enriched_users)
    except Exception as e:
        logger.error(f"获取托管用户列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取用户列表失败"}), 500


# 3. ★★★ 一个用于切换模板的 API 函数 ★★★
# (可以放在 get_all_managed_users 函数的下面)
@user_management_bp.route('/api/admin/users/<string:user_id>/template', methods=['POST'])
@login_required
def change_user_template(user_id):
    """
    【V6 - 支持首选项】为一个现有用户切换模板并应用新权限和首选项。
    """
    data = request.json
    new_template_id = data.get('template_id')

    if not new_template_id:
        return jsonify({"status": "error", "message": "必须提供新的模板ID"}), 400

    try:
        # ★★★ 核心修正：在这里定义 config 变量 ★★★
        config = config_manager.APP_CONFIG
        
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # --- 获取用户名和模板名用于日志 ---
            user_name_for_log = user_id
            new_template_name = f"ID:{new_template_id}"
            try:
                cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
                user_record = cursor.fetchone()
                if user_record: user_name_for_log = user_record['name']
                
                cursor.execute("SELECT name FROM user_templates WHERE id = %s", (new_template_id,))
                template_record = cursor.fetchone()
                if template_record: new_template_name = template_record['name']
            except Exception:
                pass
            logger.info(f"准备为用户 '{user_name_for_log}' 切换模板至 '{new_template_name}'...")

            # 1. 从新模板中获取权限策略和首选项
            cursor.execute("SELECT emby_policy_json, emby_configuration_json FROM user_templates WHERE id = %s", (new_template_id,))
            template = cursor.fetchone()
            if not template:
                return jsonify({"status": "error", "message": "模板不存在"}), 404
            
            # 2. 应用权限
            template_policy = template['emby_policy_json']
            policy_applied = emby_handler.force_set_user_policy(
                user_id, template_policy,
                config.get("emby_server_url"), config.get("emby_api_key")
            )
            
            if not policy_applied:
                return jsonify({"status": "error", "message": "在 Emby 中应用新模板权限失败"}), 500
            
            # 3. 应用首选项
            template_config = template.get('emby_configuration_json')
            if template_config:
                emby_handler.force_set_user_configuration(
                    user_id, template_config,
                    config.get("emby_server_url"), config.get("emby_api_key")
                )

            # 4. 更新数据库中的模板关联
            upsert_sql = """
                INSERT INTO emby_users_extended (emby_user_id, template_id, status, created_by)
                VALUES (%s, %s, 'active', 'admin-assigned')
                ON CONFLICT (emby_user_id) DO UPDATE SET
                    template_id = EXCLUDED.template_id;
            """
            cursor.execute(upsert_sql, (user_id, new_template_id))
            
            conn.commit()
            logger.info(f"用户 '{user_name_for_log}' 的模板已成功切换，有效期保持不变。")

        return jsonify({"status": "ok", "message": "用户模板已成功切换并应用新配置"}), 200
    except Exception as e:
        logger.error(f"切换用户 {user_id} 的模板时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "切换模板失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/status', methods=['POST'])
@login_required
def set_user_status(user_id):
    """【V2 - 增加日志用户名】手动禁用或启用一个用户"""
    data = request.json
    disable = data.get('disable', False)
    action_text = "禁用" if disable else "启用"
    
    # 在操作前获取用户名
    user_name_for_log = user_id
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
            user_record = cursor.fetchone()
            if user_record: user_name_for_log = user_record['name']
    except Exception:
        pass

    logger.info(f"准备为用户 '{user_name_for_log}' 执行 '{action_text}' 操作...")
    
    config = config_manager.APP_CONFIG
    success = emby_handler.set_user_disabled_status(
        user_id, disable, config.get("emby_server_url"), config.get("emby_api_key")
    )
    
    if success:
        new_status = 'disabled' if disable else 'active'
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE emby_users_extended SET status = %s WHERE emby_user_id = %s",
                (new_status, user_id)
            )
            conn.commit()
        logger.info(f"用户 '{user_name_for_log}' 状态更新成功。")
        return jsonify({"status": "ok", "message": "用户状态已更新"}), 200
    else:
        logger.error(f"为用户 '{user_name_for_log}' 更新状态失败。")
        return jsonify({"status": "error", "message": "更新用户状态失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/expiration', methods=['POST'])
@login_required
def set_user_expiration(user_id):
    """【V2 - 增加日志用户名】设置或清除用户的有效期"""
    data = request.json
    expiration_date = data.get('expiration_date') 
    
    # 在操作前获取用户名
    user_name_for_log = user_id
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
            user_record = cursor.fetchone()
            if user_record: user_name_for_log = user_record['name']
    except Exception:
        pass

    log_message = f"准备为用户 '{user_name_for_log}' 更新有效期至: {expiration_date or '永久'}"
    logger.info(log_message)
    
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 检查用户是否在 emby_users_extended 中，如果不存在则先创建
            cursor.execute("SELECT 1 FROM emby_users_extended WHERE emby_user_id = %s", (user_id,))
            if not cursor.fetchone():
                logger.info(f"用户 '{user_name_for_log}' 是原生用户，正在为其创建扩展记录...")
                cursor.execute(
                    "INSERT INTO emby_users_extended (emby_user_id, status, created_by) VALUES (%s, 'active', 'admin-assigned')",
                    (user_id,)
                )

            cursor.execute(
                "UPDATE emby_users_extended SET expiration_date = %s WHERE emby_user_id = %s",
                (expiration_date, user_id)
            )
            conn.commit()
        logger.info(f"用户 '{user_name_for_log}' 的有效期更新成功。")
        return jsonify({"status": "ok", "message": "用户有效期已更新"}), 200
    except Exception as e:
        logger.error(f"更新用户 '{user_name_for_log}' 有效期时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "更新有效期失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>', methods=['DELETE'])
@login_required
def delete_user(user_id):
    """【V2 - 增加日志用户名】从 Emby 和本地数据库中彻底删除一个用户"""
    config = config_manager.APP_CONFIG
    
    # 在删除前，先从本地数据库获取用户名用于日志记录
    user_name_for_log = user_id
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM emby_users WHERE id = %s", (user_id,))
            user_record = cursor.fetchone()
            if user_record and user_record.get('name'):
                user_name_for_log = user_record['name']
    except Exception:
        pass # 获取失败则继续使用ID

    emby_delete_success = emby_handler.delete_emby_user(
        user_id, 
        config.get("emby_server_url"), 
        config.get("emby_api_key")
    )
    
    if emby_delete_success:
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM emby_users WHERE id = %s", (user_id,))
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"成功从本地数据库中删除了用户 '{user_name_for_log}' (ID: {user_id}) 的记录。")
                else:
                    logger.warning(f"用户 '{user_name_for_log}' 已从 Emby 删除，但在本地数据库中未找到其主记录。")

            return jsonify({"status": "ok", "message": "用户已彻底删除"}), 200
            
        except Exception as e:
            logger.error(f"用户 '{user_name_for_log}' 已从 Emby 删除，但在清理本地数据库时出错: {e}", exc_info=True)
            return jsonify({"status": "error", "message": "用户已从 Emby 删除，但清理本地数据时发生错误，请联系管理员。"}), 500
    else:
        return jsonify({"status": "error", "message": f"在 Emby 中删除用户 '{user_name_for_log}' 失败"}), 500
@user_management_bp.route('/api/admin/user_templates/<int:template_id>', methods=['DELETE'])
@login_required
def delete_template(template_id):
    """删除一个用户模板"""
    try:
        with connection.get_db_connection() as conn:
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
