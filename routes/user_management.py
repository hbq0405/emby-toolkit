# routes/user_management.py (V2 - 终极净化版)

import uuid
import json
import time
import logging
import psycopg2
import concurrent.futures
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify

import handler.emby as emby
import handler.tmdb as tmdb
import config_manager
import constants
from handler.telegram import send_telegram_message
from extensions import admin_required
from database import user_db, media_db, connection

# 创建一个新的蓝图
user_management_bp = Blueprint('user_management_bp', __name__)
logger = logging.getLogger(__name__)

# --- 模块 1: 用户模板管理 (Templates) ---
@user_management_bp.route('/api/admin/user_templates', methods=['GET'])
@admin_required
def get_all_templates():
    try:
        templates = user_db.get_all_user_templates()
        return jsonify(templates), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates', methods=['POST'])
@admin_required
def create_template():
    data = request.json
    name, description = data.get('name'), data.get('description')
    default_expiration_days = data.get('default_expiration_days', 30)
    source_emby_user_id = data.get('source_emby_user_id')
    include_configuration = data.get('include_configuration', False)
    allow_unrestricted_subscriptions = data.get('allow_unrestricted_subscriptions', False)

    if not name or not source_emby_user_id:
        return jsonify({"status": "error", "message": "模板名称和源用户ID不能为空"}), 400

    try:
        config = config_manager.APP_CONFIG
        user_details = emby.get_user_details(source_emby_user_id, config.get("emby_server_url"), config.get("emby_api_key"))
        if not user_details or 'Policy' not in user_details:
            return jsonify({"status": "error", "message": "无法获取源用户的权限策略"}), 404
        
        policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)
        configuration_json = json.dumps(user_details['Configuration'], ensure_ascii=False) if include_configuration and 'Configuration' in user_details else None

        new_id = user_db.create_user_template(name, description, policy_json, default_expiration_days, source_emby_user_id, configuration_json, allow_unrestricted_subscriptions)
        return jsonify({"status": "ok", "message": "模板创建成功", "id": new_id}), 201

    except psycopg2.IntegrityError:
        return jsonify({"status": "error", "message": "模板名称已被占用"}), 409
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates/<int:template_id>/sync', methods=['POST'])
@admin_required
def sync_template(template_id):
    try:
        template = user_db.get_template_for_sync(template_id)
        if not template: return jsonify({"status": "error", "message": "模板不存在"}), 404
        
        source_user_id, template_name, template_has_config = template['source_emby_user_id'], template['name'], template['has_config']
        if not source_user_id: return jsonify({"status": "error", "message": f"无法同步：模板 '{template_name}' 没有记录源用户信息。"}), 400

        logger.info(f"正在为模板 '{template_name}' 从源用户 {source_user_id} 同步最新权限和首选项...")
        config = config_manager.APP_CONFIG
        user_details = emby.get_user_details(source_user_id, config.get("emby_server_url"), config.get("emby_api_key"))
        if not user_details or 'Policy' not in user_details: return jsonify({"status": "error", "message": "无法获取源用户的最新权限策略。"}), 404
        
        new_policy_json = json.dumps(user_details['Policy'], ensure_ascii=False)
        new_config_json = json.dumps(user_details['Configuration'], ensure_ascii=False) if template_has_config and 'Configuration' in user_details else None
        
        user_db.update_template_from_sync(template_id, new_policy_json, new_config_json)
        logger.info(f"模板 '{template_name}' 的数据库记录已更新。")

        users_to_update = user_db.get_users_associated_with_template(template_id)
        successful_pushes = 0
        if users_to_update:
            logger.warning(f"检测到 {len(users_to_update)} 个用户正在使用此模板，将开始逐一推送新配置...")
            for user in users_to_update:
                policy_applied = emby.force_set_user_policy(user['id'], user_details['Policy'], config.get("emby_server_url"), config.get("emby_api_key"))
                config_applied = True
                if new_config_json:
                    config_applied = emby.force_set_user_configuration(user['id'], user_details['Configuration'], config.get("emby_server_url"), config.get("emby_api_key"))
                if policy_applied and config_applied: successful_pushes += 1
                else: logger.error(f"  ➜ 为用户 '{user['name']}' (ID: {user['id']}) 推送新配置失败！")
                time.sleep(0.2)
            logger.info(f"配置推送完成！共成功更新了 {successful_pushes}/{len(users_to_update)} 个用户。")
        
        return jsonify({"status": "ok", "message": f"模板已更新，并已成功应用到 {successful_pushes} 个用户！"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/user_templates/<int:template_id>', methods=['DELETE'])
@admin_required
def delete_template(template_id):
    try:
        deleted_count = user_db.delete_user_template(template_id)
        if deleted_count > 0:
            return jsonify({"status": "ok", "message": "模板已删除"}), 200
        else:
            return jsonify({"status": "error", "message": "未找到该模板"}), 404
    except psycopg2.Error as e:
        if e.pgcode == '23503':
             return jsonify({"status": "error", "message": "无法删除：仍有邀请链接在使用此模板。"}), 409
        return jsonify({"status": "error", "message": "删除模板失败"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": "删除模板时发生未知错误"}), 500
    
@user_management_bp.route('/api/admin/user_templates/<int:template_id>', methods=['PUT'])
@admin_required
def update_template(template_id):
    data = request.json
    name, description = data.get('name'), data.get('description')
    default_expiration_days = data.get('default_expiration_days')
    allow_unrestricted_subscriptions = data.get('allow_unrestricted_subscriptions', False)

    if not name:
        return jsonify({"status": "error", "message": "模板名称不能为空"}), 400

    try:
        updated_count = user_db.update_user_template_details(template_id, name, description, default_expiration_days, allow_unrestricted_subscriptions)
        if updated_count == 0:
            return jsonify({"status": "error", "message": "模板不存在"}), 404
        return jsonify({"status": "ok", "message": "模板更新成功"}), 200
    except psycopg2.IntegrityError:
        return jsonify({"status": "error", "message": "模板名称已被占用"}), 409
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 模块 2: 邀请链接管理 (Invitations) ---

@user_management_bp.route('/api/admin/invitations', methods=['POST'])
@admin_required
def create_invitation():
    data = request.json
    template_id = data.get('template_id')
    expiration_days = data.get('expiration_days')
    link_expires_in_days = data.get('link_expires_in_days', 7)

    if not template_id:
        return jsonify({"status": "error", "message": "必须选择一个模板"}), 400

    try:
        token = user_db.create_invitation_link(template_id, expiration_days, link_expires_in_days)
        app_base_url = config_manager.APP_CONFIG.get("app_base_url", request.host_url.rstrip('/'))
        invite_link = f"{app_base_url}/register/invite/{token}"
        return jsonify({"status": "ok", "invite_link": invite_link}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/invitations', methods=['GET'])
@admin_required
def get_all_invitations():
    try:
        invitations = user_db.get_all_invitation_links()
        return jsonify(invitations)
    except Exception as e:
        return jsonify({"status": "error", "message": "获取邀请码列表失败"}), 500

@user_management_bp.route('/api/admin/invitations/<int:invitation_id>', methods=['DELETE'])
@admin_required
def delete_invitation(invitation_id):
    try:
        deleted_count = user_db.delete_invitation_link(invitation_id)
        if deleted_count > 0:
            return jsonify({"status": "ok", "message": "邀请码已删除"}), 200
        else:
            return jsonify({"status": "error", "message": "未找到该邀请码"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": "删除邀请码失败"}), 500

# --- 模块 3: 用户管理 (User Management) ---

@user_management_bp.route('/api/admin/users', methods=['GET'])
@admin_required
def get_all_managed_users():
    try:
        config = config_manager.APP_CONFIG
        all_emby_users = emby.get_all_emby_users_from_server(config.get("emby_server_url"), config.get("emby_api_key"))
        if all_emby_users is None:
            return jsonify({"error": "无法从 Emby 获取用户列表"}), 500

        source_user_ids = user_db.get_template_source_user_ids()
        extended_info_map = user_db.get_all_extended_user_info()

        enriched_users = []
        for user in all_emby_users:
            user_id = user.get('Id')
            if user_id in source_user_ids:
                continue

            extended_data = extended_info_map.get(user_id, {})
            user['IsDisabled'] = user.get('Policy', {}).get('IsDisabled', False)
            user['expiration_date'] = extended_data.get('expiration_date')
            user['status_in_db'] = extended_data.get('status')
            user['template_id'] = extended_data.get('template_id')
            user['template_name'] = extended_data.get('template_name')
            enriched_users.append(user)
            
        return jsonify(enriched_users)
    except Exception as e:
        return jsonify({"status": "error", "message": "获取用户列表失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/template', methods=['POST'])
@admin_required
def change_user_template(user_id):
    data = request.json
    new_template_id = data.get('template_id')
    if not new_template_id:
        return jsonify({"status": "error", "message": "必须提供新的模板ID"}), 400

    try:
        config = config_manager.APP_CONFIG
        user_name, new_template_name, template = user_db.change_user_template_and_get_names(user_id, new_template_id)
        
        logger.info(f"  ➜ 准备为用户 '{user_name}' 切换模板至 '{new_template_name}'...")
        
        policy_applied = emby.force_set_user_policy(user_id, template['emby_policy_json'], config.get("emby_server_url"), config.get("emby_api_key"))
        if not policy_applied:
            return jsonify({"status": "error", "message": "在 Emby 中应用新模板权限失败"}), 500
        
        if template.get('emby_configuration_json'):
            emby.force_set_user_configuration(user_id, template['emby_configuration_json'], config.get("emby_server_url"), config.get("emby_api_key"))

        logger.info(f"  ➜ 用户 '{user_name}' 的模板已成功切换，有效期保持不变。")
        return jsonify({"status": "ok", "message": "用户模板已成功切换并应用新配置"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": "切换模板失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/status', methods=['POST'])
@admin_required
def set_user_status(user_id):
    data = request.json
    disable = data.get('disable', False)
    
    try:
        user_name = user_db.get_username_by_id(user_id) or user_id
        logger.info(f"  ➜ 准备为用户 '{user_name}' 执行 '{'禁用' if disable else '启用'}' 操作...")
        
        config = config_manager.APP_CONFIG
        success = emby.set_user_disabled_status(user_id, disable, config.get("emby_server_url"), config.get("emby_api_key"))
        
        if success:
            user_db.set_user_status_in_db(user_id, 'disabled' if disable else 'active')
            logger.info(f"  ➜ 用户 '{user_name}' 状态更新成功。")
            return jsonify({"status": "ok", "message": "用户状态已更新"}), 200
        else:
            logger.error(f"  ➜ 为用户 '{user_name}' 更新状态失败。")
            return jsonify({"status": "error", "message": "更新用户状态失败"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>/expiration', methods=['POST'])
@admin_required
def set_user_expiration(user_id):
    data = request.json
    expiration_date = data.get('expiration_date') 
    
    try:
        user_name = user_db.get_username_by_id(user_id) or user_id
        logger.info(f"准备为用户 '{user_name}' 更新有效期至: {expiration_date or '永久'}")
        
        user_db.set_user_expiration_in_db(user_id, expiration_date)
        
        logger.info(f"  ➜ 用户 '{user_name}' 的有效期更新成功。")
        return jsonify({"status": "ok", "message": "用户有效期已更新"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": "更新有效期失败"}), 500

@user_management_bp.route('/api/admin/users/<string:user_id>', methods=['DELETE'])
@admin_required
def delete_user(user_id):
    try:
        user_name = user_db.get_username_by_id(user_id) or user_id
        emby_delete_success = emby.delete_emby_user(user_id)
        
        if emby_delete_success:
            deleted_count = user_db.delete_user_from_db(user_id)
            if deleted_count > 0:
                logger.info(f"  ✅ 成功从本地数据库中删除了用户 '{user_name}' (ID: {user_id}) 的记录。")
            else:
                logger.warning(f"  ➜ 用户 '{user_name}' 已从 Emby 删除，但在本地数据库中未找到其主记录。")
            return jsonify({"status": "ok", "message": "用户已彻底删除"}), 200
        else:
            return jsonify({"status": "error", "message": f"在 Emby 中删除用户 '{user_name}' 失败"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 模块 4: 订阅审核管理 ---

@user_management_bp.route('/api/admin/subscriptions/pending', methods=['GET'])
@admin_required
def get_pending_subscriptions():
    """获取所有待审核的订阅请求。"""
    try:
        requests = media_db.get_pending_requests_for_admin()
        return jsonify(requests)
    except Exception as e:
        return jsonify({"status": "error", "message": "获取列表失败"}), 500

@user_management_bp.route('/api/admin/subscriptions/batch-approve', methods=['POST'])
@admin_required
def batch_approve_subscriptions():
    """【V8 - 增加前置状态校验】"""
    data = request.json
    requests_to_approve = data.get('requests', [])
    if not requests_to_approve:
        return jsonify({"status": "error", "message": "未提供任何订阅请求"}), 400

    try:
        # ★★★ 核心修改 1/3: 在处理前，先批量获取所有请求的当前状态 ★★★
        all_tmdb_ids = [req['tmdb_id'] for req in requests_to_approve]
        media_details_map = media_db.get_media_details_by_tmdb_ids(all_tmdb_ids)

        # ★★★ 核心修改 2/3: 筛选出真正处于 'REQUESTED' 状态的请求 ★★★
        valid_requests = []
        for req in requests_to_approve:
            details = media_details_map.get(req['tmdb_id'])
            if details and details.get('subscription_status') == 'REQUESTED':
                valid_requests.append(req)
        
        if not valid_requests:
            return jsonify({"status": "ok", "message": "选中的请求已被处理或不存在。"}), 200

        # 后续的逻辑只针对 valid_requests 操作
        approved_count = 0
        notifications_to_send = {}

        # ★★★ 核心修改 3/3: 逐条处理【有效】的请求 ★★★
        for req in valid_requests:
            try:
                # (这里的 media_info 理论上可以从上面的 media_details_map 复用，但为了逻辑清晰，重新构建也无妨)
                media_info = {
                    'tmdb_id': req['tmdb_id'], 'item_type': req['item_type'], 'title': req['title'],
                    # ... (可以补充更多元数据)
                }
                
                media_db.set_media_status_wanted(
                    tmdb_ids=[req['tmdb_id']],
                    item_type=req['item_type'],
                    source={"type": "admin_approval", "admin": "admin"}
                )
                approved_count += 1
                
                # 准备通知
                details = media_details_map.get(req['tmdb_id'])
                if details and details.get('subscription_sources_json'):
                    first_source = details['subscription_sources_json'][0]
                    if first_source.get('type') == 'user_request' and (subscriber_id := first_source.get('user_id')):
                        if subscriber_id not in notifications_to_send:
                            notifications_to_send[subscriber_id] = []
                        notifications_to_send[subscriber_id].append(req)

            except Exception as e_ind:
                logger.error(f"批准请求 {req['tmdb_id']} 时失败: {e_ind}")
                continue
        
        # 按用户ID对要通知的请求进行分组
        notifications_to_send = {}
        for req_details in requests_to_approve:
            subscriber_id = req_details.get('emby_user_id')
            if subscriber_id not in notifications_to_send:
                notifications_to_send[subscriber_id] = []
            notifications_to_send[subscriber_id].append(req_details)

        # ★★★ 核心修改 3/3：循环分组，构建并发送合并通知 ★★★
        for subscriber_id, user_requests in notifications_to_send.items():
            try:
                subscriber_chat_id = user_db.get_user_telegram_chat_id(subscriber_id)
                if subscriber_chat_id:
                    approved_items_list = [f"· `{req.get('item_name')}`" for req in user_requests]
                    approved_items_str = "\n".join(approved_items_list)
                    personal_message = (
                        f"✅ *您的 {len(user_requests)} 个订阅请求已批准*\n\n"
                        f"下列内容已进入自动订阅队列，系统将很快为您处理：\n{approved_items_str}"
                    )
                    send_telegram_message(subscriber_chat_id, personal_message)
            except Exception as e:
                logger.error(f"为用户 {subscriber_id} 发送批量批准的合并通知时发生错误: {e}")

        final_message = f"成功批准了 {approved_count} 条订阅，它们已加入待订阅队列。"
        return jsonify({"status": "ok", "message": final_message}), 200
        
    except Exception as e:
        logger.error(f"批量批准订阅请求时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "处理批量批准时发生内部错误"}), 500

@user_management_bp.route('/api/admin/subscriptions/batch-reject', methods=['POST'])
@admin_required
def batch_reject_subscriptions():
    """【V3 - 统一订阅架构版】批量拒绝订阅请求，并更新状态为 IGNORED。"""
    data = request.json
    # ★★★ 1. 接收的不再是 id 列表，而是包含完整信息的对象列表 ★★★
    requests_to_reject = data.get('requests', [])
    reason = data.get('reason')

    if not requests_to_reject:
        return jsonify({"status": "error", "message": "未提供任何订阅请求"}), 400

    try:
        # ★★★ 2. 按 item_type 对请求进行分组，为批量更新做准备 ★★★
        grouped_requests = {}
        for req in requests_to_reject:
            item_type = req.get('item_type')
            if item_type not in grouped_requests:
                grouped_requests[item_type] = []
            grouped_requests[item_type].append(req)

        updated_count = 0
        
        # ★★★ 3. 遍历分组，为每种 item_type 执行一次批量更新 ★★★
        for item_type, req_list in grouped_requests.items():
            tmdb_ids = [req['tmdb_id'] for req in req_list]
            
            media_db.set_media_status_ignored(
                tmdb_ids=tmdb_ids,
                item_type=item_type,
                source={"type": "admin_rejection"},
                ignore_reason=reason
            )
            updated_count += len(tmdb_ids)

        # ★★★ 4. 发送合并通知 (逻辑与 batch-approve 类似) ★★★
        notifications_to_send = {}
        # 为了获取用户名，我们需要从 `subscription_sources_json` 中解析
        # 我们可以通过一次批量查询获取所有相关媒体的详情
        all_tmdb_ids = [req['tmdb_id'] for req in requests_to_reject]
        media_details_map = media_db.get_media_details_by_tmdb_ids(all_tmdb_ids)

        for req in requests_to_reject:
            media_details = media_details_map.get(req['tmdb_id'])
            if not media_details or not media_details.get('subscription_sources_json'):
                continue
            
            # 假设第一个 source 就是原始请求者
            first_source = media_details['subscription_sources_json'][0]
            if first_source.get('type') == 'user_request' and (subscriber_id := first_source.get('user_id')):
                if subscriber_id not in notifications_to_send:
                    notifications_to_send[subscriber_id] = []
                notifications_to_send[subscriber_id].append(req)

        for subscriber_id, user_requests in notifications_to_send.items():
            try:
                subscriber_chat_id = user_db.get_user_telegram_chat_id(subscriber_id)
                if subscriber_chat_id:
                    rejected_items_list = [f"`{req.get('item_name') or req.get('title')}`" for req in user_requests]
                    rejected_items_str = "\n".join(rejected_items_list)
                    reason_text = f"\n\n*拒绝理由*: {reason}" if reason else ""
                    message_text = (
                        f"❌ *您的 {len(user_requests)} 个订阅请求已被拒绝*\n\n"
                        f"您想看的以下内容未能通过审核：\n"
                        f"{rejected_items_str}"
                        f"{reason_text}"
                    )
                    send_telegram_message(subscriber_chat_id, message_text)
            except Exception as e:
                logger.error(f"为用户 {subscriber_id} 发送批量拒绝的合并通知时发生错误: {e}")

        return jsonify({"status": "ok", "message": f"成功拒绝了 {updated_count} 条订阅请求。"}), 200
    except Exception as e:
        logger.error(f"批量拒绝订阅请求时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "处理批量拒绝时发生内部错误"}), 500
    
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
            if emby.check_if_user_exists(username, config.get("emby_server_url"), config.get("emby_api_key")):
                conn.rollback()
                return jsonify({"status": "error", "message": "该用户名已被占用"}), 409

            # ★★★ 核心修改点 ★★★

            # 1. 调用【纯净版】的创建函数，它不再需要 policy 参数
            new_user_id = emby.create_user_with_policy(
                username, password,
                config.get("emby_server_url"), config.get("emby_api_key")
            )
            if not new_user_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "在 Emby 中创建用户失败，请联系管理员"}), 500

            # 2. 用户创建成功后，立刻调用强制设置函数，应用模板中的完整 Policy
            # 这一步和之前一样，现在是它唯一负责设置权限的地方
            template_policy = template['emby_policy_json']
            policy_applied = emby.force_set_user_policy(
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
                emby.force_set_user_configuration(
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
