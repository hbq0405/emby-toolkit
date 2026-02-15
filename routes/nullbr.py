# routes/nullbr.py
import logging
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
import handler.nullbr as nullbr_handler

nullbr_bp = Blueprint('nullbr_bp', __name__, url_prefix='/api/nullbr')
logger = logging.getLogger(__name__)

@nullbr_bp.route('/config', methods=['GET', 'POST'])
@admin_required
def handle_config():
    if request.method == 'GET':
        config = settings_db.get_setting('nullbr_config') or {}
        if 'filters' not in config:
            config['filters'] = {
                "resolutions": [], "qualities": [],
                "movie_min_size": 0, "movie_max_size": 0,
                "tv_min_size": 0, "tv_max_size": 0,
                "require_zh": False, "containers": []
            }
        if 'enabled_sources' not in config:
            config['enabled_sources'] = ['115', 'magnet', 'ed2k']
        
        # 清理旧字段
        config.pop('daily_limit', None)
        config.pop('request_interval', None)
        
        if 'p115_cookies' not in config: config['p115_cookies'] = ''
        if 'p115_save_path_cid' not in config: config['p115_save_path_cid'] = 0
            
        return jsonify(config)
    
    if request.method == 'POST':
        data = request.json
        new_config = {
            "api_key": data.get('api_key', '').strip(),
            "cms_url": data.get('cms_url', '').strip(),     
            "cms_token": data.get('cms_token', '').strip(),
            "p115_cookies": data.get('p115_cookies', '').strip(),
            "p115_save_path_cid": data.get('p115_save_path_cid', 0),
            "filters": data.get('filters', {}),
            # 移除了 daily_limit 和 request_interval 的保存
            "enabled_sources": data.get('enabled_sources', ['115', 'magnet', 'ed2k']),
            "updated_at": "now"
        }
        settings_db.save_setting('nullbr_config', new_config)
        return jsonify({"status": "success", "message": "配置已保存"})

@nullbr_bp.route('/user/info', methods=['GET'])
@admin_required
def get_user_info():
    """获取 NULLBR 用户信息"""
    try:
        info = nullbr_handler.get_user_info()
        return jsonify({"status": "success", "data": info})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@nullbr_bp.route('/user/redeem', methods=['POST'])
@admin_required
def redeem_code():
    """兑换激活码"""
    data = request.json
    code = data.get('code')
    if not code:
        return jsonify({"status": "error", "message": "请输入兑换码"}), 400
    
    try:
        result = nullbr_handler.redeem_code(code)
        if result.get('success'):
            return jsonify({"status": "success", "data": result})
        else:
            return jsonify({"status": "error", "message": result.get('message')}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@nullbr_bp.route('/search', methods=['POST'])
@admin_required
def search_resources():
    data = request.json
    keyword = data.get('keyword')
    page = data.get('page', 1)
    if not keyword: return jsonify({"status": "error", "message": "关键词为空"}), 400
    try:
        result = nullbr_handler.search_media(keyword, page)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@nullbr_bp.route('/resources', methods=['POST'])
@admin_required
def get_resources():
    data = request.json
    try:
        resource_list = nullbr_handler.fetch_resource_list(
            data.get('tmdb_id') or data.get('id'), 
            data.get('media_type', 'movie'), 
            specific_source=data.get('source_type'), 
            season_number=data.get('season_number')
        )
        return jsonify({"status": "success", "data": resource_list, "total": len(resource_list)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@nullbr_bp.route('/push', methods=['POST'])
@admin_required
def push_resource():
    data = request.json
    try:
        nullbr_handler.handle_push_request(data.get('link'), data.get('title', '未知资源'))
        return jsonify({"status": "success", "message": "已添加至 115 离线任务"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@nullbr_bp.route('/presets', methods=['GET', 'POST', 'DELETE'])
@admin_required
def handle_presets():
    if request.method == 'GET':
        return jsonify(nullbr_handler.get_preset_lists())
    if request.method == 'POST':
        data = request.json
        presets = [{"id": str(i.get('id')).strip(), "name": str(i.get('name')).strip()} for i in data.get('presets', []) if i.get('id') and i.get('name')]
        settings_db.save_setting('nullbr_presets', presets)
        return jsonify({"status": "success", "message": "片单已保存"})
    if request.method == 'DELETE':
        settings_db.delete_setting('nullbr_presets')
        return jsonify({"status": "success", "message": "已恢复默认", "data": nullbr_handler.get_preset_lists()})

@nullbr_bp.route('/list', methods=['POST'])
@admin_required
def get_list_content():
    data = request.json
    try:
        result = nullbr_handler.fetch_list_items(data.get('list_id'), data.get('page', 1))
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@nullbr_bp.route('/115/status', methods=['GET'])
@admin_required
def get_115_status():
    try:
        info = nullbr_handler.get_115_account_info()
        return jsonify({"status": "success", "data": info})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500