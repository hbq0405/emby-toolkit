# routes/local_organize.py
import logging
import os
import json
from flask import Blueprint, jsonify, request
from extensions import admin_required
import constants
import config_manager

local_organize_bp = Blueprint('local_organize_bp', __name__, url_prefix='/api/local_organize')
logger = logging.getLogger(__name__)

def get_config():
    return config_manager.APP_CONFIG

@local_organize_bp.route('/status', methods=['GET'])
@admin_required
def get_status():
    """获取配置和状态"""
    try:
        from tasks.local_organize import get_monitor_status
        
        config = get_config()
        monitor_status = get_monitor_status()
        
        return jsonify({
            "success": True,
            "data": {
                "enabled": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_ENABLED, False),
                "source_movie": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MOVIE, ''),
                "source_tv": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_TV, ''),
                "source_mixed": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MIXED, ''),
                "target_base": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_TARGET_BASE, ''),
                "mode": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MODE, 'hardlink'),
                "auto_scrape": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_AUTO_SCRAPE, True),
                "max_workers": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MAX_WORKERS, 5),
                "monitor_running": monitor_status.get('running', False),
            }
        })
    except Exception as e:
        logger.error(f"获取状态失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/config', methods=['GET'])
@admin_required
def get_config_api():
    """获取配置"""
    try:
        config = get_config()
        return jsonify({
            "success": True,
            "data": {
                "enabled": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_ENABLED, False),
                "source_movie": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MOVIE, ''),
                "source_tv": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_TV, ''),
                "source_mixed": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MIXED, ''),
                "target_base": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_TARGET_BASE, ''),
                "mode": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MODE, 'hardlink'),
                "auto_scrape": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_AUTO_SCRAPE, True),
                "max_workers": config.get(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MAX_WORKERS, 5),
            }
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/config', methods=['POST'])
@admin_required
def save_config():
    """保存配置"""
    try:
        data = request.json or {}
        
        from database import settings_db
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_ENABLED, data.get('enabled', False))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MOVIE, data.get('source_movie', ''))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_TV, data.get('source_tv', ''))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_SOURCE_MIXED, data.get('source_mixed', ''))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_TARGET_BASE, data.get('target_base', ''))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MODE, data.get('mode', 'hardlink'))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_AUTO_SCRAPE, data.get('auto_scrape', True))
        settings_db.save_setting(constants.CONFIG_OPTION_LOCAL_ORGANIZE_MAX_WORKERS, data.get('max_workers', 5))
        
        config_manager.load_config()
        
        return jsonify({"success": True, "message": "配置已保存"})
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/start', methods=['POST'])
@admin_required
def start_organize():
    """手动触发全量整理"""
    try:
        from tasks.local_organize import task_local_organize
        import task_manager
        
        task_manager.submit_task(
            task_local_organize,
            task_name="本地文件整理",
            processor_type='media'
        )
        
        return jsonify({
            "success": True,
            "message": "任务已提交"
        })
    except Exception as e:
        logger.error(f"触发整理失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/monitor/start', methods=['POST'])
@admin_required
def start_monitor():
    """启动监控"""
    try:
        from tasks.local_organize import start_monitor
        result = start_monitor()
        return jsonify(result)
    except Exception as e:
        logger.error(f"启动监控失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/monitor/stop', methods=['POST'])
@admin_required
def stop_monitor():
    """停止监控"""
    try:
        from tasks.local_organize import stop_monitor
        result = stop_monitor()
        return jsonify(result)
    except Exception as e:
        logger.error(f"停止监控失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/records', methods=['GET'])
@admin_required
def get_records():
    """获取整理记录"""
    try:
        from database.connection import get_db_connection
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 15))
        search = request.args.get('search', '')
        status = request.args.get('status', 'all')
        
        offset = (page - 1) * per_page
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                where_clauses = ["(fail_reason = 'local' OR fail_reason IS NULL OR fail_reason = '')"]
                params = []
                
                if search:
                    where_clauses.append("(original_name ILIKE %s OR renamed_name ILIKE %s)")
                    params.extend([f"%{search}%", f"%{search}%"])
                
                if status != 'all':
                    where_clauses.append("status = %s")
                    params.append(status)
                
                where_sql = "WHERE " + " AND ".join(where_clauses)
                
                cursor.execute(f"SELECT COUNT(*) FROM p115_organize_records {where_sql}", tuple(params))
                total = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT * FROM p115_organize_records 
                    {where_sql} 
                    ORDER BY processed_at DESC 
                    LIMIT %s OFFSET %s
                """, tuple(params + [per_page, offset]))
                items = cursor.fetchall()
                
                cursor.execute("SELECT COUNT(*) FROM p115_organize_records WHERE status = 'success'")
                success = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM p115_organize_records WHERE status = 'unrecognized'")
                unrecognized = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM p115_organize_records WHERE processed_at >= NOW() - INTERVAL '7 days'")
                this_week = cursor.fetchone()[0]
        
        return jsonify({
            "success": True,
            "items": items,
            "total": total,
            "stats": {
                "total": total,
                "success": success,
                "unrecognized": unrecognized,
                "thisWeek": this_week,
            }
        })
    except Exception as e:
        logger.error(f"获取记录失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/records/correct', methods=['POST'])
@admin_required
def correct_record():
    """手动重新整理/纠错"""
    try:
        data = request.json
        record_id = data.get('id')
        tmdb_id = data.get('tmdb_id')
        target_cid = data.get('target_cid')
        
        if not record_id or not tmdb_id:
            return jsonify({"success": False, "message": "缺少必要参数"}), 400
        
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE p115_organize_records 
                    SET tmdb_id = %s, target_cid = %s, status = 'success'
                    WHERE id = %s
                """, (tmdb_id, target_cid, record_id))
                conn.commit()
        
        return jsonify({"success": True, "message": "纠错已保存"})
    except Exception as e:
        logger.error(f"纠错失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@local_organize_bp.route('/records/<int:record_id>', methods=['DELETE'])
@admin_required
def delete_record(record_id):
    """删除记录"""
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM p115_organize_records WHERE id = %s", (record_id,))
                conn.commit()
        
        return jsonify({"success": True, "message": "记录已删除"})
    except Exception as e:
        logger.error(f"删除记录失败: {e}")
        return jsonify({"success": False, "message": str(e)}), 500