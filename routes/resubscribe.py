# routes/resubscribe.py (V3 - 完整注释最终版)

from flask import Blueprint, request, jsonify
import logging
from typing import List, Tuple, Optional

import tasks
import task_manager
import handler.moviepilot as moviepilot
import extensions
import handler.emby as emby
from extensions import admin_required, task_lock_required
from database import resubscribe_db, settings_db

resubscribe_bp = Blueprint('resubscribe', __name__, url_prefix='/api/resubscribe')
logger = logging.getLogger(__name__)

# ======================================================================
# ★★★ 规则管理 (Rules Management) - RESTful API ★★★
# ======================================================================

@resubscribe_bp.route('/rules', methods=['GET'])
@admin_required
def get_rules():
    """获取所有洗版规则列表。"""
    try:
        rules = resubscribe_db.get_all_resubscribe_rules()
        return jsonify(rules)
    except Exception as e:
        logger.error(f"API: 获取洗版规则列表失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules', methods=['POST'])
@admin_required
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
        if "violates unique constraint" in str(e):
             return jsonify({"error": f"创建失败：规则名称 '{rule_data.get('name')}' 已存在。"}), 409
        logger.error(f"API: 创建洗版规则失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/<int:rule_id>', methods=['PUT'])
@admin_required
def update_rule(rule_id):
    """更新指定ID的洗版规则。"""
    try:
        rule_data = request.json
        if not rule_data:
            return jsonify({"error": "请求体不能为空"}), 400
        
        if resubscribe_db.update_resubscribe_rule(rule_id, rule_data):
            return jsonify({"message": "洗版规则已成功更新！"})
        else:
            return jsonify({"error": f"未找到ID为 {rule_id} 的规则"}), 404
    except Exception as e:
        logger.error(f"API: 更新洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/<int:rule_id>', methods=['DELETE'])
@admin_required
def delete_rule(rule_id):
    """删除指定ID的洗版规则。"""
    try:
        # 删除规则时，联动删除其关联的洗版索引
        resubscribe_db.delete_resubscribe_index_by_rule_id(rule_id)
        
        if resubscribe_db.delete_resubscribe_rule(rule_id):
            return jsonify({"message": "洗版规则已成功删除！"})
        else:
            return jsonify({"error": f"未找到ID为 {rule_id} 的规则"}), 404
    except Exception as e:
        logger.error(f"API: 删除洗版规则 {rule_id} 失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/rules/order', methods=['POST'])
@admin_required
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
# ★★★ 海报墙与任务触发 (Library & Tasks) ★★★
# ======================================================================

@resubscribe_bp.route('/library_status', methods=['GET'])
@admin_required
def get_library_status():
    """获取海报墙数据。"""
    try:
        items = resubscribe_db.get_resubscribe_library_status()
        return jsonify(items)
    except Exception as e:
        logger.error(f"API: 获取洗版状态缓存失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

@resubscribe_bp.route('/refresh_status', methods=['POST'])
@admin_required
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
@admin_required
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
@admin_required
def resubscribe_single_item():
    """为单个媒体项触发洗版订阅。"""
    data = request.json
    item_id = data.get('item_id')
    if not item_id:
        return jsonify({"error": "请求中缺少必要的 item_id 参数"}), 400

    try:
        # 检查订阅配额
        if settings_db.get_subscription_quota() <= 0:
            return jsonify({"error": "今日订阅配额已用尽，请明天再试。"}), 429

        # 检查核心处理器
        processor = extensions.media_processor_instance
        if not processor:
            return jsonify({"error": "核心处理器未初始化"}), 503
            
        # 从数据库获取完整的项目详情，确保数据准确
        item_details = resubscribe_db.get_resubscribe_cache_item(item_id)
        if not item_details:
            return jsonify({"error": f"数据库中未找到 Item ID 为 {item_id} 的缓存记录。"}), 404
        
        item_name = item_details.get('item_name', '未知项目')
        
        # 获取匹配的规则
        rule = resubscribe_db.get_resubscribe_rule_by_id(item_details['matched_rule_id']) if item_details.get('matched_rule_id') else None
        
        # 构建订阅请求
        payload = tasks.build_resubscribe_payload(item_details, rule)
        if not payload:
            return jsonify({"error": "构建订阅请求失败，请检查日志。"}), 500
        
        # 发送订阅
        if moviepilot.subscribe_with_custom_payload(payload, processor.config):
            settings_db.decrement_subscription_quota()
            message = f"《{item_name}》的洗版请求已成功提交！"
            
            # 检查是否需要删除源文件
            if rule and rule.get('delete_after_resubscribe'):
                id_to_delete = item_details.get('emby_item_id')
                if not id_to_delete:
                    logger.error(f"无法删除 '{item_name}'：emby_item_id 为空。")
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                    message += " 但无法删除源文件，因Emby ID为空。"
                elif emby.delete_item(id_to_delete, processor.emby_url, processor.emby_api_key, processor.emby_user_id):
                    resubscribe_db.delete_resubscribe_cache_item(item_id)
                    message += " 源文件已根据规则删除。"
                else:
                    resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')
                    message += " 但删除Emby源文件时失败。"
            else:
                # 仅更新状态为“已订阅”
                resubscribe_db.update_resubscribe_item_status(item_id, 'subscribed')

            return jsonify({"message": message})
        else:
            return jsonify({"error": "提交洗版请求失败..."}), 500
            
    except Exception as e:
        logger.error(f"API: 处理单独洗版请求时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": f"处理请求时发生服务器内部错误: {e}"}), 500
    
@resubscribe_bp.route('/libraries', methods=['GET'])
@admin_required
def get_emby_libraries_for_rules():
    """
    获取所有 Emby 媒体库，并返回一个精简的列表 (label, value)，
    专门用于洗版规则设置页面的下拉选择框。
    """
    try:
        processor = extensions.media_processor_instance
        if not processor or not processor.emby_url or not processor.emby_api_key:
            return jsonify({"error": "Emby配置不完整或服务未就绪"}), 503
        
        full_list = emby.get_emby_libraries(processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if full_list is None:
            return jsonify({"error": "无法获取Emby媒体库列表"}), 500
        
        simplified = [
            {'label': item.get('Name'), 'value': item.get('Id')}
            for item in full_list
            if item.get('Name') and item.get('Id') and item.get('CollectionType') in ['movies', 'tvshows']
        ]
        return jsonify(simplified)

    except Exception as e:
        logger.error(f"API: 获取洗版用媒体库列表时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
@resubscribe_bp.route('/batch_action', methods=['POST'])
@admin_required
def batch_action():
    """处理对洗版索引项的批量操作。"""
    data = request.json
    item_ids = data.get('item_ids')
    action = data.get('action')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"message": "当前视图下没有可操作的项目。"}), 200
    if action not in ['subscribe', 'ignore', 'ok', 'delete']:
        return jsonify({"error": "无效的操作类型"}), 400

    try:
        # 辅助函数：将前端的 item_id 字符串解析为数据库需要的复合主键元组
        def parse_item_id_for_batch(item_id: str) -> Optional[Tuple[str, str, int]]:
            try:
                parts = item_id.split('-')
                tmdb_id = parts[0]
                item_type = parts[1]
                season_number = -1
                if item_type == 'Season' and len(parts) > 2:
                    season_number = int(parts[2].replace('S',''))
                return (tmdb_id, item_type, season_number)
            except (IndexError, ValueError):
                return None

        # 将 item_id 列表转换为数据库函数需要的格式
        item_keys_for_db = [key for item_id in item_ids if (key := parse_item_id_for_batch(item_id)) is not None]
        if not item_keys_for_db:
             return jsonify({"error": "无法从请求中解析任何有效的项目ID"}), 400

        if action == 'subscribe':
            # 提交后台任务时，仍然使用原始的 item_id 列表
            task_manager.submit_task(
                tasks.task_resubscribe_batch,
                task_name="批量媒体洗版",
                processor_type='media',
                item_ids=item_ids
            )
            # 乐观更新UI，将状态设置为“已订阅”
            resubscribe_db.batch_update_resubscribe_index_status(item_keys_for_db, 'subscribed')
            return jsonify({"message": "批量订阅任务已提交到后台！"}), 202

        elif action == 'ignore':
            updated_count = resubscribe_db.batch_update_resubscribe_index_status(item_keys_for_db, 'ignored')
            return jsonify({"message": f"成功忽略了 {updated_count} 个媒体项。"})

        elif action == 'ok':
            updated_count = resubscribe_db.batch_update_resubscribe_index_status(item_keys_for_db, 'ok')
            return jsonify({"message": f"成功取消忽略了 {updated_count} 个媒体项。"})
        
        elif action == 'delete':
            # 提交后台任务时，也使用原始的 item_id 列表
            task_manager.submit_task(
                tasks.task_delete_batch,
                task_name="批量删除媒体",
                processor_type='media',
                item_ids=item_ids
            )
            return jsonify({"message": "批量删除任务已提交到后台！"}), 202

    except Exception as e:
        logger.error(f"API: 处理批量操作 '{action}' 时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500