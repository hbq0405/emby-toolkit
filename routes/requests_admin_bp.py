# routes/requests_admin_bp.py (新文件)

import logging
from flask import Blueprint, request, jsonify

from database import connection
import handler.moviepilot as moviepilot
import config_manager
from extensions import admin_required # 假设你有这个登录装饰器

# --- 创建一个新的蓝图 ---
requests_admin_bp = Blueprint('requests_admin_bp', __name__)
logger = logging.getLogger(__name__)

# ======================================================================
# API 1: 获取待审批的请求列表
# ======================================================================
@requests_admin_bp.route('/api/admin/requests', methods=['GET'])
@admin_required
def get_pending_requests():
    """获取所有待审批的媒体请求，并关联请求者的用户名。"""
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 使用 LEFT JOIN 来获取请求者的用户名
            cursor.execute("""
                SELECT r.*, u.name as requested_by_username
                FROM media_requests r
                LEFT JOIN emby_users u ON r.requested_by_user_id = u.id
                WHERE r.status = 'pending'
                ORDER BY r.requested_at ASC
            """)
            requests = [dict(row) for row in cursor.fetchall()]
        return jsonify(requests)
    except Exception as e:
        logger.error(f"获取待审批请求列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取列表失败"}), 500

# ======================================================================
# API 2: 批准一个请求
# ======================================================================
@requests_admin_bp.route('/api/admin/requests/<int:request_id>/approve', methods=['POST'])
@admin_required
def approve_request(request_id):
    """批准一个媒体请求，并触发下载。"""
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            # 1. 先查出这条请求的详细信息
            cursor.execute("SELECT * FROM media_requests WHERE id = %s AND status = 'pending'", (request_id,))
            req = cursor.fetchone()
            if not req:
                return jsonify({"status": "error", "message": "请求不存在或已被处理"}), 404

            # 2. 调用 MoviePilot 订阅
            config = config_manager.APP_CONFIG
            media_info = {'tmdb_id': req['tmdb_id'], 'title': req['title']}
            success = False
            if req['media_type'] == 'movie':
                success = moviepilot.subscribe_movie_to_moviepilot(media_info, config)
            elif req['media_type'] == 'tv':
                success = moviepilot.subscribe_series_to_moviepilot(media_info, None, config)

            if not success:
                return jsonify({"status": "error", "message": "推送到MoviePilot失败，请检查其配置和连接"}), 500

            # 3. 如果推送成功，更新数据库状态
            cursor.execute("UPDATE media_requests SET status = 'approved' WHERE id = %s", (request_id,))
            conn.commit()
            
        return jsonify({"status": "ok", "message": "请求已批准，并已推送到下载器"})
    except Exception as e:
        logger.error(f"批准请求 {request_id} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "处理批准时发生内部错误"}), 500

# ======================================================================
# API 3: 拒绝一个请求
# ======================================================================
@requests_admin_bp.route('/api/admin/requests/<int:request_id>/deny', methods=['POST'])
@admin_required
def deny_request(request_id):
    """拒绝一个媒体请求。"""
    data = request.json
    reason = data.get('reason', '') # 可以选择性地提供拒绝理由
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE media_requests SET status = 'denied', admin_notes = %s WHERE id = %s AND status = 'pending'",
                (reason, request_id)
            )
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({"status": "error", "message": "请求不存在或已被处理"}), 404
        return jsonify({"status": "ok", "message": "请求已拒绝"})
    except Exception as e:
        logger.error(f"拒绝请求 {request_id} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "处理拒绝时发生内部错误"}), 500