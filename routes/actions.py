# routes/actions.py

from flask import Blueprint, request, jsonify
import logging

# 导入底层和共享模块
import task_manager
import extensions
from extensions import admin_required, processor_ready_required, task_lock_required

# 1. 创建蓝图
actions_bp = Blueprint('actions', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

# 2. 定义路由

# ★★★ 重新处理单个项目 ★★★
@actions_bp.route('/actions/reprocess_item/<item_id>', methods=['POST'])
@admin_required
def api_reprocess_item(item_id):
    from tasks import task_reprocess_single_item # 延迟导入
    import handler.emby as emby

    item_details = emby.get_emby_item_details(
        item_id,
        extensions.media_processor_instance.emby_url,
        extensions.media_processor_instance.emby_api_key,
        extensions.media_processor_instance.emby_user_id
    )
    item_name_for_ui = item_details.get("Name", f"ItemID: {item_id}") if item_details else f"ItemID: {item_id}"

    success = task_manager.submit_task(
        task_reprocess_single_item,
        f"任务已提交: {item_name_for_ui}",
        processor_type='media',
        item_id=item_id,
        item_name_for_ui=item_name_for_ui
        
    )
    if success:
        return jsonify({"message": f"重新处理项目 '{item_name_for_ui}' 的任务已提交。"}), 202
    else:
        return jsonify({"error": "提交任务失败，已有任务在运行。"}), 409

# ★★★ 重新处理所有待复核项 ★★★
@actions_bp.route('/actions/reprocess_all_review_items', methods=['POST'])
@admin_required
@task_lock_required
@processor_ready_required
def api_reprocess_all_review_items():
    from tasks import task_reprocess_all_review_items # 延迟导入
    success = task_manager.submit_task(task_reprocess_all_review_items, "重新处理所有待复核项", processor_type='media')
    if success:
        return jsonify({"message": "重新处理所有待复核项的任务已提交。"}), 202
    else:
        return jsonify({"error": "提交任务失败，已有任务在运行。"}), 409

# +++ 一键添加所有剧集到追剧列表的 API +++
@actions_bp.route('/actions/add_all_series_to_watchlist', methods=['POST'])
@admin_required
@processor_ready_required
def api_add_all_series_to_watchlist():
    from tasks import task_add_all_series_to_watchlist # 延迟导入
    
    success = task_manager.submit_task(
        task_add_all_series_to_watchlist, 
        "一键扫描全库剧集",
        processor_type='media'
    )
    
    if success:
        return jsonify({"message": "一键扫描全库剧集的任务已提交。"}), 202
    else:
        return jsonify({"error": "提交任务失败，已有任务在运行。"}), 409