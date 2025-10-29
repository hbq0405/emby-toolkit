# routes/media_cleanup.py

from flask import Blueprint, jsonify, request
from extensions import task_lock_required, processor_ready_required

import task_manager
import config_manager
from tasks import task_execute_cleanup
from database import (
    maintenance_db,
    settings_db
)
import logging

logger = logging.getLogger(__name__)

media_cleanup_bp = Blueprint('media_cleanup_bp', __name__)

@media_cleanup_bp.route('/api/cleanup/tasks', methods=['GET'])
def get_cleanup_tasks():
    """获取所有待处理的媒体去重任务。"""
    try:
        tasks = maintenance_db.get_all_cleanup_tasks()
        return jsonify(tasks)
    except Exception as e:
        return jsonify({"error": f"获取清理任务失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/execute', methods=['POST'])
@task_lock_required
@processor_ready_required
def execute_cleanup_tasks():
    """执行指定的清理任务。"""
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400

    task_manager.submit_task(
        task_execute_cleanup,
        f"执行 {len(task_ids)} 项媒体去重", # task_name 是第二个位置参数
        'media',                         # processor_type 是第三个位置参数
        task_ids                         # task_ids 现在是第四个位置参数 (*args)
    )
    return jsonify({"message": "清理任务已提交到后台执行。"}), 202

@media_cleanup_bp.route('/api/cleanup/ignore', methods=['POST'])
def ignore_cleanup_tasks():
    """将指定的清理任务标记为已忽略。"""
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400
    
    try:
        updated_count = maintenance_db.batch_update_cleanup_task_status(task_ids, 'ignored')
        return jsonify({"message": f"成功忽略 {updated_count} 个任务。"}), 200
    except Exception as e:
        return jsonify({"error": f"忽略任务时失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/delete', methods=['POST'])
def delete_cleanup_tasks():
    """从列表中删除指定的清理任务（不执行清理）。"""
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400

    try:
        deleted_count = maintenance_db.batch_delete_cleanup_tasks(task_ids)
        return jsonify({"message": f"成功删除 {deleted_count} 个任务。"}), 200
    except Exception as e:
        return jsonify({"error": f"删除任务时失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/clear_all', methods=['POST'])
@task_lock_required
@processor_ready_required
def clear_all_cleanup_tasks():
    """一键执行所有媒体去重任务。"""
    try:
        # 1. 获取所有待处理的清理任务ID
        all_pending_tasks = maintenance_db.get_all_cleanup_tasks()
        task_ids = [task['id'] for task in all_pending_tasks]

        if not task_ids:
            return jsonify({"message": "没有发现待处理的清理任务，无需执行。"}), 200

        # 2. 提交任务到后台执行
        task_manager.submit_task(
            task_execute_cleanup,
            f"一键执行所有 {len(task_ids)} 项媒体去重",
            'media',
            task_ids
        )
        return jsonify({"message": f"一键执行所有 {len(task_ids)} 项清理任务已提交到后台。"}), 202
    except Exception as e:
        logger.error(f"一键执行所有清理任务时失败: {e}", exc_info=True)
        return jsonify({"error": f"一键执行所有清理任务失败: {e}"}), 500
    
@media_cleanup_bp.route('/api/cleanup/rules', methods=['GET'])
def get_cleanup_rules():
    """【V5 - 智能升级最终版】获取去重规则，并能将新选项智能合并到旧的已存规则中。"""
    try:
        # 1. 定义一套完整的、包含所有最新选项的默认规则
        default_rules_map = {
            "quality": {"id": "quality", "enabled": True, "priority": ["Remux", "BluRay", "WEB-DL", "HDTV"]},
            "resolution": {"id": "resolution", "enabled": True, "priority": ["2160p", "1080p", "720p"]},
            "effect": {
                "id": "effect", 
                "enabled": True, 
                "priority": ["dovi_p8", "dovi_p7", "dovi_p5", "dovi_other", "hdr10+", "hdr", "sdr"]
            },
            "filesize": {"id": "filesize", "enabled": True, "priority": "desc"}
        }
        
        # 2. 从数据库加载用户已保存的规则列表
        saved_rules_list = settings_db.get_setting('media_cleanup_rules')
        
        if not saved_rules_list:
            # 如果数据库为空，直接返回最新的默认规则
            return jsonify(list(default_rules_map.values()))

        # --- 如果数据库中有规则，则执行智能合并 ---
        final_rules = []
        saved_rules_map = {rule['id']: rule for rule in saved_rules_list}
        
        # 3. 以用户保存的顺序为准进行遍历
        for saved_rule in saved_rules_list:
            rule_id = saved_rule['id']
            # 将数据库中的规则与默认规则合并，确保 enabled 等基础字段存在
            merged_rule = {**default_rules_map.get(rule_id, {}), **saved_rule}

            # ★★★ 核心修改：对 "effect" 规则进行特殊处理，智能升级选项列表 ★★★
            if rule_id == 'effect' and 'priority' in merged_rule and isinstance(merged_rule['priority'], list):
                # 获取用户保存的旧优先级 (例如: ['dovi', 'hdr'])
                saved_priority = merged_rule['priority']
                # 获取代码中定义的最新的完整优先级
                default_priority = default_rules_map['effect']['priority']

                # 创建一个最终的优先级列表，先放入用户已经保存并排序好的项
                final_priority = []
                # 创建一个集合用于快速判断是否存在，并统一标准化 'dovi_other'
                saved_priority_set = set()

                for p_item in saved_priority:
                    p_lower = str(p_item).lower().replace(' ', '_')
                    # 统一标准化 'dovi_other' 的各种变体，包括 'dovi_(other)'
                    if p_lower in ['dovi', 'dovi_other', 'dovi(other)', 'dovi_(other)']:
                        p_lower = 'dovi_other'
                    
                    if p_lower not in saved_priority_set:
                        final_priority.append(p_lower)
                        saved_priority_set.add(p_lower)

                # 获取代码中定义的最新的完整优先级
                default_priority = default_rules_map['effect']['priority']

                # 遍历最新的默认列表，将用户没有的“新”选项追加到末尾
                for new_item in default_priority:
                    if new_item not in saved_priority_set: # new_item 已经是标准化的
                        final_priority.append(new_item)
                        saved_priority_set.add(new_item) # 添加到集合以防止重复

                # 用升级后的完整列表替换掉旧的列表
                merged_rule['priority'] = final_priority
            
            final_rules.append(merged_rule)

        # 4. 检查是否有在代码中新增的、但用户从未保存过的规则（例如未来新增一个 "codec" 规则）
        saved_ids = set(saved_rules_map.keys())
        for key, default_rule in default_rules_map.items():
            if key not in saved_ids:
                final_rules.append(default_rule)

        return jsonify(final_rules)
        
    except Exception as e:
        logger.error(f"获取媒体去重规则时出错: {e}", exc_info=True)
        return jsonify({"error": f"获取清理规则失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/rules', methods=['POST'])
def save_cleanup_rules():
    """保存新的媒体去重规则。"""
    new_rules = request.get_json()
    if not isinstance(new_rules, list):
        return jsonify({"error": "无效的规则格式，必须是一个列表。"}), 400
    
    try:
        # 直接使用 db_handler 保存设置
        settings_db.save_setting('media_cleanup_rules', new_rules)
        # 通知配置管理器重新加载内存中的设置，确保后续任务使用新规则
        return jsonify({"message": "清理规则已成功保存！"}), 200
    except Exception as e:
        return jsonify({"error": f"保存清理规则时失败: {e}"}), 500
