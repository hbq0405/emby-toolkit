# routes/requests_admin_bp.py 

import logging
from flask import Blueprint, request, jsonify

from database import requests_db 
import handler.moviepilot as moviepilot
import config_manager
from extensions import admin_required

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
        # ### 核心修改：直接调用数据库函数 ###
        pending_requests = requests_db.get_pending_requests_with_username()
        return jsonify(pending_requests)
    except Exception as e:
        # 日志已在数据库层记录，这里只返回错误
        return jsonify({"status": "error", "message": "获取列表失败"}), 500

# ======================================================================
# API 2: 批准一个请求
# ======================================================================
@requests_admin_bp.route('/api/admin/requests/<int:request_id>/approve', methods=['POST'])
@admin_required
def approve_request(request_id):
    """批准一个媒体请求，并触发下载。"""
    try:
        # ### 核心修改：第一步，短事务，只读 ###
        req = requests_db.get_request_by_id(request_id)
        if not req or req['status'] != 'pending':
            return jsonify({"status": "error", "message": "请求不存在或已被处理"}), 404

        # ### 核心修改：第二步，执行缓慢的网络调用，此时无数据库连接 ###
        config = config_manager.APP_CONFIG
        media_info = {'tmdb_id': req['tmdb_id'], 'title': req['title']}
        success = False
        if req['media_type'] == 'movie':
            success = moviepilot.subscribe_movie_to_moviepilot(media_info, config)
        elif req['media_type'] == 'tv':
            success = moviepilot.subscribe_series_to_moviepilot(media_info, None, config)

        if not success:
            return jsonify({"status": "error", "message": "推送到MoviePilot失败，请检查其配置和连接"}), 500

        # ### 核心修改：第三步，短事务，只写 ###
        requests_db.update_request_status(request_id, 'approved')
            
        return jsonify({"status": "ok", "message": "请求已批准，并已推送到下载器"})
    except Exception as e:
        return jsonify({"status": "error", "message": "处理批准时发生内部错误"}), 500

# ======================================================================
# API 3: 拒绝一个请求
# ======================================================================
@requests_admin_bp.route('/api/admin/requests/<int:request_id>/deny', methods=['POST'])
@admin_required
def deny_request(request_id):
    """拒绝一个媒体请求。"""
    data = request.json
    reason = data.get('reason', '')
    try:
        # ### 核心修改：直接调用数据库函数 ###
        updated_rows = requests_db.update_request_status(request_id, 'denied', admin_notes=reason)
        
        if updated_rows == 0:
            return jsonify({"status": "error", "message": "请求不存在或已被处理"}), 404
            
        return jsonify({"status": "ok", "message": "请求已拒绝"})
    except Exception as e:
        return jsonify({"status": "error", "message": "处理拒绝时发生内部错误"}), 500