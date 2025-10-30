# routes/resubscribe.py (多规则改造最终版)

from flask import Blueprint, request, jsonify
import logging

import tasks
import task_manager
import moviepilot_handler
import config_manager
import extensions
import emby_handler
from extensions import login_required, task_lock_required
from database import resubscribe_db, settings_db
resubscribe_bp = Blueprint('resubscribe', __name__, url_prefix='/api/resubscribe')
logger = logging.getLogger(__name__)

# ======================================================================
# ★★★ 规则管理 (Rules Management) - RESTful API ★★★
# ======================================================================

@resubscribe_bp.route('/rules', methods=['GET'])
@login_required
def get_rules():
    """获取所有洗版规则列表。"""
    try:
        rules = resubscribe_db.get_all_resubscribe_rules()
        return jsonify(rules)
    except Exception as e:
        logger.error(f"API: 获取洗版规则列表失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules', methods=['POST'])
@login_required
def create_rule():
    """创建一条新的洗版规则。"""
    try:
        rule_data = request.json
        if not rule_data or not rule_data.get('name'):
            return jsonify({"error": "规则名称不能为空"}), 400
        
        new_id = resubscribe_db.create_resubscribe_rule(rule_data)
        return jsonify({"message": "洗版规则已成功创建！", "id": new_id}), 201
    except Exception as e:
        # 捕获由 resubscribe_db 抛出的唯一性冲突
        if "UNIQUE constraint failed" in str(e) or "violates unique constraint" in str(e):
             return jsonify({"error": f"创建失败：规则名称 '{rule_data.get('name')}' 已存在。"}), 409
        logger.error(f"API: 创建洗版规则失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/<int:rule_id>', methods=['PUT'])
@login_required
def update_rule(rule_id):
    """更新指定ID的洗版规则。"""
    try:
        rule_data = request.json
        if not rule_data:
            return jsonify({"error": "请求体不能为空"}), 400
        
        success = resubscribe_db.update_resubscribe_rule(rule_id, rule_data)
        if success:
            return jsonify({"message": "洗版规则已成功更新！"})
        else:
            return jsonify({"error": f"未找到ID为 {rule_id} 的规则"}), 404
    except Exception as e:
        logger.error(f"API: 更新洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/<int:rule_id>', methods=['DELETE'])
@login_required
def delete_rule(rule_id):
    """删除指定ID的洗版规则。"""
    try:
        logger.info(f"API: 准备删除规则 {rule_id}，将首先清理其关联的缓存...")
        resubscribe_db.delete_resubscribe_cache_by_rule_id(rule_id)
        success = resubscribe_db.delete_resubscribe_rule(rule_id)
        if success:
            return jsonify({"message": "洗版规则已成功删除！"})
        else:
            return jsonify({"error": f"未找到ID为 {rule_id} 的规则"}), 404
    except Exception as e:
        logger.error(f"API: 删除洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/order', methods=['POST'])
@login_required
def update_rules_order():
    """更新所有规则的排序。"""
    try:
        ordered_ids = request.json
        if not isinstance(ordered_ids, list):
            return jsonify({"error": "请求体必须是一个ID数组"}), 400
        
        resubscribe_db.update_resubscribe_rules_order(ordered_ids)
        return jsonify({"message": "规则顺序已更新！"})
    except Exception as e:
        logger.error(f"API: 更新规则顺序失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# ======================================================================
# ★★★ 海报墙与任务触发 (Library & Tasks) - 保持不变 ★★★
# ======================================================================

@resubscribe_bp.route('/library_status', methods=['GET'])
@login_required
def get_library_status():
    """获取海报墙数据。"""
    try:
        items = resubscribe_db.get_all_resubscribe_cache()
        return jsonify(items)
    except Exception as e:
        logger.error(f"API: 获取洗版状态缓存失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/refresh_status', methods=['POST'])
@login_required
@task_lock_required
def trigger_refresh_status():
    """触发缓存刷新任务。"""
    try:
        task_manager.submit_task(
            tasks.task_update_resubscribe_cache,
            task_name="刷新媒体洗版状态",
            processor_type='media'
        )
        return jsonify({"message": "刷新媒体洗版状态任务已提交！"}), 202
    except Exception as e:
        return jsonify({"error": f"提交任务失败: {e}"}), 500

@resubscribe_bp.route('/resubscribe_all', methods=['POST'])
@login_required
@task_lock_required
def trigger_resubscribe_all():
    """触发一键洗版全部的任务。"""
    try:
        task_manager.submit_task(
            tasks.task_resubscribe_library,
            task_name="全库媒体洗版",
            processor_type='media'
        )
        return jsonify({"message": "一键洗版任务已提交！"}), 202
    except Exception as e:
        return jsonify({"error": f"提交任务失败: {e}"}), 500

@resubscribe_bp.route('/resubscribe_item', methods=['POST'])
@login_required
def resubscribe_single_item():
    """
    【V4 - 最终修正版】修复了只传递三个键值对的致命BUG。
    现在会从数据库获取完整的媒体项信息进行处理。
    """
    data = request.json
    item_id = data.get('item_id')
    
    if not item_id:
        return jsonify({"error": "请求中缺少必要的 item_id 参数"}), 400

    try:
        current_quota = settings_db.get_subscription_quota()
        if current_quota <= 0:
            return jsonify({"error": "今日订阅配额已用尽，请明天再试。"}), 429

        processor = extensions.media_processor_instance
        if not processor:
            return jsonify({"error": "核心处理器未初始化"}), 503
            
        # ======================================================================
        # ★★★★★★★★★★★★★★★★★ 核心修复 ★★★★★★★★★★★★★★★★★
        # ======================================================================
        # 1. 不再构建残缺字典！直接从数据库获取最完整的原始数据！
        item_details_for_payload = resubscribe_db.get_resubscribe_cache_item(item_id)

        if not item_details_for_payload:
            return jsonify({"error": f"数据库中未找到 Item ID 为 {item_id} 的缓存记录。"}), 404
        
        item_name = item_details_for_payload.get('item_name', '未知项目') # 从完整数据中获取名字
        # ======================================================================

        # 2. 获取与此项匹配的规则
        rule_to_check = None
        if item_details_for_payload.get('matched_rule_id'):
            rule_to_check = resubscribe_db.get_resubscribe_rule_by_id(item_details_for_payload['matched_rule_id'])

        # 3. 让“智能荷官”配牌 (现在它拿到的是包含 filename 的完整数据了！)
        payload = tasks.build_resubscribe_payload(item_details_for_payload, rule_to_check)

        if not payload:
            return jsonify({"error": "构建订阅请求失败，请检查日志。"}), 500
        
        # 4. 发送订阅
        success = moviepilot_handler.subscribe_with_custom_payload(payload, processor.config)
        
        if success:
            settings_db.decrement_subscription_quota()
            
            message = f"《{item_name}》的洗版请求已成功提交！"
            
            # 重新获取一次规则，因为上面的 rule_to_check 变量可能没被赋值
            final_rule = None
            if item_details_for_payload.get('matched_rule_id'):
                final_rule = resubscribe_db.get_resubscribe_rule_by_id(item_details_for_payload['matched_rule_id'])

            if final_rule and final_rule.get('delete_after_resubscribe'):
                logger.warning(f"  ➜ 规则 '{final_rule['name']}' 要求删除源文件，正在为项目 {item_name} 执行删除...")
                delete_success = emby_handler.delete_item(
                    item_id=item_id, emby_server_url=processor.emby_url,
                    emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                )
                if delete_success:
                    resubscribe_db.delete_resubscribe_cache_item(item_id)
                    message += " Emby中的源文件已根据规则删除，并已从洗版列表移除。"
                else:
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                    message += " 但根据规则删除Emby源文件时失败。"
            else:
                resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')

            return jsonify({"message": message})
        else:
            return jsonify({"error": "提交洗版请求失败..."}), 500
            
    except Exception as e:
        logger.error(f"API: 处理单独洗版请求时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": f"处理请求时发生服务器内部错误: {e}"}), 500
    
# ★★★ 新增：为洗版规则提供媒体库选项的 API ★★★
@resubscribe_bp.route('/libraries', methods=['GET'])
@login_required
def get_emby_libraries_for_rules():
    """
    获取所有 Emby 媒体库，并返回一个精简的列表 (label, value)，
    专门用于洗版规则设置页面的下拉选择框。
    """
    try:
        if not extensions.media_processor_instance or \
           not extensions.media_processor_instance.emby_url or \
           not extensions.media_processor_instance.emby_api_key:
            return jsonify({"error": "Emby配置不完整或服务未就绪"}), 503

        full_libraries_list = emby_handler.get_emby_libraries(
            extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key,
            extensions.media_processor_instance.emby_user_id
        )

        if full_libraries_list is None:
            return jsonify({"error": "无法获取Emby媒体库列表"}), 500
        
        simplified_libraries = [
            {'label': item.get('Name'), 'value': item.get('Id')}
            for item in full_libraries_list
            if item.get('Name') and item.get('Id') and item.get('CollectionType') in ['movies', 'tvshows', 'mixed']
        ]
        
        return jsonify(simplified_libraries)

    except Exception as e:
        logger.error(f"API: 获取洗版用媒体库列表时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# ★★★ 处理批量操作的 API ★★★
@resubscribe_bp.route('/batch_action', methods=['POST'])
@login_required
def batch_action():
    """【V4 - 支持一键操作】处理对洗版缓存项的批量操作。"""
    data = request.json
    item_ids = data.get('item_ids')
    action = data.get('action')
    is_one_click = data.get('is_one_click', False) # ★ 新增：接收“一键”标志

    # 如果是一键操作，我们就不需要 item_ids，而是自己去数据库里找
    if is_one_click:
        current_filter = data.get('filter', 'all') # 接收当前视图
        if current_filter == 'needed':
            sql = "SELECT item_id FROM resubscribe_cache WHERE status = 'needed'"
        elif current_filter == 'ignored':
            sql = "SELECT item_id FROM resubscribe_cache WHERE status = 'ignored'"
        else: # 'all'
            # “一键全部订阅”没有意义，所以我们只处理“需洗版”的
            sql = "SELECT item_id FROM resubscribe_cache WHERE status = 'needed'"

        with resubscribe_db.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            item_ids = [row['item_id'] for row in cursor.fetchall()]
    
    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"message": "当前视图下没有可操作的项目。"}), 200
    if action not in ['subscribe', 'ignore', 'ok', 'delete']: # ★ 新增：支持 'delete'
        return jsonify({"error": "无效的操作类型"}), 400

    try:
        if action == 'subscribe':
            task_manager.submit_task(
                tasks.task_resubscribe_batch,
                task_name="批量媒体洗版",
                processor_type='media',
                item_ids=item_ids
            )
            resubscribe_db.batch_update_resubscribe_cache_status(item_ids, 'subscribed')
            return jsonify({"message": "批量订阅任务已提交到后台！"}), 202

        elif action == 'ignore':
            updated_count = resubscribe_db.batch_update_resubscribe_cache_status(item_ids, 'ignored')
            return jsonify({"message": f"成功忽略了 {updated_count} 个媒体项。"})

        elif action == 'ok':
            updated_count = resubscribe_db.batch_update_resubscribe_cache_status(item_ids, 'ok')
            return jsonify({"message": f"成功取消忽略了 {updated_count} 个媒体项。"})
        
        # ★★★ 新增：处理删除动作 ★★★
        elif action == 'delete':
            task_manager.submit_task(
                tasks.task_delete_batch, # ★ 需要一个新的后台任务
                task_name="批量删除媒体",
                processor_type='media',
                item_ids=item_ids
            )
            return jsonify({"message": "批量删除任务已提交到后台！"}), 202

    except Exception as e:
        logger.error(f"API: 处理批量操作 '{action}' 时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500