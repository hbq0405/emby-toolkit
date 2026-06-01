# routes/tasks.py

import logging
from flask import Blueprint, request, jsonify

# 导入您项目中用于管理和执行任务的核心模块
import task_manager 
from extensions import admin_required, processor_ready_required, task_lock_required
# ★★★ 导入任务注册表，这是“翻译”的关键 ★★★
from tasks.core import get_task_registry, get_available_task_definitions

logger = logging.getLogger(__name__)

# 创建一个新的蓝图
tasks_bp = Blueprint('tasks', __name__, url_prefix='/api/tasks')

# --- 获取所有可供选择的任务 ---
@tasks_bp.route('/available', methods=['GET'])
@admin_required
def get_available_tasks():
    """
    返回可供前端选择/展示的任务列表。
    支持 context=chain/all，并从 tasks.core 透传任务说明 help，避免前端硬编码说明 map。
    """
    try:
        context = (request.args.get('context') or 'chain').strip().lower()
        if context not in ('chain', 'all', 'manual'):
            context = 'chain'

        available_tasks = get_available_task_definitions(context=context)
        return jsonify(available_tasks), 200
    except Exception as e:
        logger.error(f"获取可用任务列表时出错: {e}", exc_info=True)
        return jsonify({"error": "无法获取可用任务列表"}), 500

@tasks_bp.route('/run', methods=['POST'])
@admin_required
@task_lock_required
@processor_ready_required
def run_task():
    """
    【V2 - 精确调度版】
    一个通用的、用于从前端触发后台任务的API端点。
    它会从任务注册表中查找任务所需处理器的类型，并精确地提交给任务管理器。
    """
    data = request.get_json()
    if not data or 'task_name' not in data:
        return jsonify({"error": "请求体中缺少 'task_name' 参数"}), 400

    task_key = data.pop('task_name')
    logger.trace(f"收到来自前端的通用任务执行请求: {task_key}, 参数: {data}")

    try:
        task_registry = get_task_registry()
        task_info = task_registry.get(task_key)
        if not task_info:
            return jsonify({"error": f"未知的任务名称: {task_key}"}), 404

        task_function_obj, task_description, processor_type = task_info
        
        success = task_manager.submit_task(
            task_function=task_function_obj, 
            task_name=task_description,
            processor_type=processor_type, 
            **data
        )
        
        if success:
            return jsonify({"message": f"任务 '{task_description}' 已成功提交。"}), 202
        else:
            return jsonify({"error": "任务提交失败，未知错误。"}), 500

    except Exception as e:
        logger.error(f"提交任务 '{task_key}' 时出错: {e}", exc_info=True)
        return jsonify({"error": f"服务器内部错误: {e}"}), 500