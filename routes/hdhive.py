# routes/hdhive.py
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive
import threading

hdhive_bp = Blueprint('hdhive_bp', __name__, url_prefix='/api/hdhive')

@hdhive_bp.route('/config', methods=['GET', 'POST'])
@admin_required
def handle_config():
    """获取或保存影巢配置，并返回用户信息"""
    if request.method == 'GET':
        api_key = settings_db.get_setting('hdhive_api_key') or ''
        user_info = None
        quota_info = None
        if api_key:
            client = HDHiveClient(api_key)
            user_info = client.get_user_info()
            quota_info = client.get_weekly_quota()
            
        return jsonify({
            "success": True, 
            "api_key": api_key,
            "user_info": user_info,
            "quota_info": quota_info
        })
        
    if request.method == 'POST':
        api_key = request.json.get('api_key', '').strip()
        settings_db.save_setting('hdhive_api_key', api_key)
        
        client = HDHiveClient(api_key)
        user_info = client.get_user_info()
        if user_info:
            quota_info = client.get_weekly_quota()
            return jsonify({
                "success": True, 
                "message": "API Key 保存成功！",
                "user_info": user_info,
                "quota_info": quota_info
            })
        else:
            return jsonify({"success": False, "message": "API Key 无效或网络异常！"})

@hdhive_bp.route('/resources', methods=['GET'])
@admin_required
def get_resources():
    """查询影巢资源 (支持按季过滤)"""
    tmdb_id = request.args.get('tmdb_id')
    media_type = request.args.get('media_type')
    season = request.args.get('season') # 可选
    
    api_key = settings_db.get_setting('hdhive_api_key')
    if not api_key:
        return jsonify({"success": False, "message": "请先配置影巢 API Key"}), 400
        
    client = HDHiveClient(api_key)
    # 调用我们上一轮优化的带 season 过滤的方法
    resources = client.get_resources(tmdb_id, media_type, target_season=season)
    
    return jsonify({"success": True, "data": resources})

@hdhive_bp.route('/download', methods=['POST'])
@admin_required
def trigger_download():
    """触发转存与整理任务"""
    data = request.json
    slug = data.get('slug')
    tmdb_id = data.get('tmdb_id')
    media_type = data.get('media_type')
    title = data.get('title', '未知影视')
    
    api_key = settings_db.get_setting('hdhive_api_key')
    
    # 扔到后台执行
    threading.Thread(
        target=task_download_from_hdhive, 
        args=(api_key, slug, tmdb_id, media_type, title)
    ).start()
    
    return jsonify({"success": True, "message": f"已向 115 发送转存指令，后台正在处理！"})