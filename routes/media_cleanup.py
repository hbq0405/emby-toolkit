# routes/media_cleanup.py

from flask import Blueprint, jsonify, request
from extensions import task_lock_required, processor_ready_required

import task_manager
from tasks import task_execute_cleanup, task_scan_for_cleanup_issues
from database import (
    maintenance_db,
    settings_db
)
from database.connection import get_db_connection
import logging

logger = logging.getLogger(__name__)

media_cleanup_bp = Blueprint('media_cleanup_bp', __name__)

@media_cleanup_bp.route('/api/cleanup/tasks', methods=['GET'])
def get_cleanup_tasks():
    """
    【V2 - 瘦身关联版】
    获取所有待处理的清理任务，并关联 media_metadata 表获取最新的元数据。
    """
    try:
        sql_query = """
            SELECT 
                t.id,
                t.tmdb_id,
                t.item_type,
                t.versions_info_json,
                t.best_version_id,
                m.title AS item_name,
                m.season_number,
                m.episode_number,
                m.parent_series_tmdb_id,
                parent.title AS parent_series_name
            FROM cleanup_index AS t
            JOIN media_metadata AS m ON t.tmdb_id = m.tmdb_id AND t.item_type = m.item_type
            LEFT JOIN media_metadata AS parent ON m.parent_series_tmdb_id = parent.tmdb_id AND parent.item_type = 'Series'
            WHERE t.status = 'pending'
            ORDER BY parent.title, m.season_number, m.episode_number, m.title;
        """
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                tasks = [dict(row) for row in cursor.fetchall()]
        
        return jsonify(tasks)
    except Exception as e:
        logger.error(f"获取关联后的清理任务失败: {e}", exc_info=True)
        return jsonify({"error": f"获取清理任务失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/execute', methods=['POST'])
@task_lock_required
@processor_ready_required
def execute_cleanup_tasks():
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400

    task_manager.submit_task(
        task_execute_cleanup, 
        f"执行 {len(task_ids)} 项媒体去重",
        task_ids=task_ids
    )
    return jsonify({"message": "清理任务已提交到后台执行。"}), 202

@media_cleanup_bp.route('/api/cleanup/ignore', methods=['POST'])
def ignore_cleanup_tasks():
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400
    try:
        # ★★★ 调用新函数 ★★★
        updated_count = maintenance_db.batch_update_cleanup_index_status(task_ids, 'ignored')
        return jsonify({"message": f"成功忽略 {updated_count} 个任务。"}), 200
    except Exception as e:
        return jsonify({"error": f"忽略任务时失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/delete', methods=['POST'])
def delete_cleanup_tasks():
    data = request.get_json()
    task_ids = data.get('task_ids')
    if not task_ids or not isinstance(task_ids, list):
        return jsonify({"error": "缺少或无效的 task_ids 参数"}), 400
    try:
        # ★★★ 调用新函数 ★★★
        deleted_count = maintenance_db.batch_delete_cleanup_index(task_ids)
        return jsonify({"message": f"成功删除 {deleted_count} 个任务。"}), 200
    except Exception as e:
        return jsonify({"error": f"删除任务时失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/clear_all', methods=['POST'])
@task_lock_required
@processor_ready_required
def clear_all_cleanup_tasks():
    try:
        all_pending_tasks = maintenance_db.get_all_cleanup_index()
        task_ids = [task['id'] for task in all_pending_tasks]
        if not task_ids:
            return jsonify({"message": "没有发现待处理的清理任务。"}), 200

        task_manager.submit_task(
            task_execute_cleanup,
            f"一键执行所有 {len(task_ids)} 项媒体去重",
            task_ids=task_ids
        )
        return jsonify({"message": f"一键清理任务已提交到后台。"}), 202
    except Exception as e:
        logger.error(f"一键执行所有清理任务时失败: {e}", exc_info=True)
        return jsonify({"error": f"一键清理失败: {e}"}), 500
    
@media_cleanup_bp.route('/api/cleanup/settings', methods=['GET'])
def get_cleanup_settings():
    """获取所有媒体去重设置（包括规则和指定的媒体库）。"""
    try:
        # --- Part 1: 获取规则 (逻辑与之前完全相同) ---
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
        saved_rules_list = settings_db.get_setting('media_cleanup_rules')
        
        if not saved_rules_list:
            final_rules = list(default_rules_map.values())
        else:
            final_rules = []
            saved_rules_map = {rule['id']: rule for rule in saved_rules_list}
            for saved_rule in saved_rules_list:
                rule_id = saved_rule['id']
                merged_rule = {**default_rules_map.get(rule_id, {}), **saved_rule}
                if rule_id == 'effect' and 'priority' in merged_rule and isinstance(merged_rule['priority'], list):
                    saved_priority = merged_rule['priority']
                    default_priority = default_rules_map['effect']['priority']
                    final_priority = []
                    saved_priority_set = set()
                    for p_item in saved_priority:
                        p_lower = str(p_item).lower().replace(' ', '_')
                        if p_lower in ['dovi', 'dovi_other', 'dovi(other)', 'dovi_(other)']:
                            p_lower = 'dovi_other'
                        if p_lower not in saved_priority_set:
                            final_priority.append(p_lower)
                            saved_priority_set.add(p_lower)
                    for new_item in default_priority:
                        if new_item not in saved_priority_set:
                            final_priority.append(new_item)
                            saved_priority_set.add(new_item)
                    merged_rule['priority'] = final_priority
                final_rules.append(merged_rule)
            saved_ids = set(saved_rules_map.keys())
            for key, default_rule in default_rules_map.items():
                if key not in saved_ids:
                    final_rules.append(default_rule)

        # --- Part 2: 新增 - 获取已保存的媒体库ID列表 ---
        saved_library_ids = settings_db.get_setting('media_cleanup_library_ids') or []

        # --- Part 3: 将规则和媒体库ID合并到一个对象中返回给前端 ---
        return jsonify({
            "rules": final_rules,
            "library_ids": saved_library_ids
        })
        
    except Exception as e:
        logger.error(f"获取媒体去重设置时出错: {e}", exc_info=True)
        return jsonify({"error": f"获取清理设置失败: {e}"}), 500

@media_cleanup_bp.route('/api/cleanup/settings', methods=['POST'])
def save_cleanup_settings():
    """保存新的媒体去重设置（包括规则和指定的媒体库）。"""
    data = request.get_json()
    if not isinstance(data, dict):
        return jsonify({"error": "无效的数据格式，必须是一个对象。"}), 400

    new_rules = data.get('rules')
    library_ids = data.get('library_ids')

    if not isinstance(new_rules, list):
        return jsonify({"error": "无效的规则格式，'rules' 必须是一个列表。"}), 400
    # 对 library_ids 也进行校验
    if not isinstance(library_ids, list):
        return jsonify({"error": "无效的媒体库格式，'library_ids' 必须是一个列表。"}), 400
    
    try:
        # 分别保存规则和媒体库ID到数据库
        settings_db.save_setting('media_cleanup_rules', new_rules)
        settings_db.save_setting('media_cleanup_library_ids', library_ids)
        
        return jsonify({"message": "清理设置已成功保存！"}), 200
    except Exception as e:
        return jsonify({"error": f"保存清理设置时失败: {e}"}), 500

# ★★★ 核心修改 2/3: 新增一个触发扫描的路由 ★★★
# 这个路由会调用更新后的 task_scan_for_cleanup_issues 任务
@media_cleanup_bp.route('/api/cleanup/scan', methods=['POST'])
@task_lock_required
@processor_ready_required
def trigger_cleanup_scan():
    """触发一次媒体库重复项扫描。"""
    try:
        # 新的调用方式不再需要 'media' 这个 processor_type 参数
        task_manager.submit_task(
            task_scan_for_cleanup_issues,
            "扫描媒体库重复项 (数据库模式)"
        )
        return jsonify({"message": "扫描任务已提交到后台执行。"}), 202
    except Exception as e:
        logger.error(f"提交扫描任务时失败: {e}", exc_info=True)
        return jsonify({"error": f"提交扫描任务失败: {e}"}), 500
