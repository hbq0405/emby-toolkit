# routes/nullbr.py
import logging
from datetime import datetime
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
                "resolutions": [],
                "qualities": [],
                "min_size": 0, "max_size": 0,
                "movie_min_size": 0, "movie_max_size": 0,
                "tv_min_size": 0, "tv_max_size": 0,
                "require_zh": False,
                "containers": []
            }
        # ★ 默认开启所有源
        if 'enabled_sources' not in config:
            config['enabled_sources'] = ['115', 'magnet', 'ed2k']
            
        # ... (统计逻辑) ...
        stats = settings_db.get_setting('nullbr_usage_stats') or {}
        today_str = datetime.now().strftime('%Y-%m-%d')
        current_usage = stats.get('count', 0) if stats.get('date') == today_str else 0
        config['current_usage'] = current_usage
            
        return jsonify(config)
    
    if request.method == 'POST':
        data = request.json
        new_config = {
            "api_key": data.get('api_key', '').strip(),
            "cms_url": data.get('cms_url', '').strip(),     
            "cms_token": data.get('cms_token', '').strip(),
            "filters": data.get('filters', {}),
            "daily_limit": int(data.get('daily_limit', 100)),
            "request_interval": float(data.get('request_interval', 5)),
            "enabled_sources": data.get('enabled_sources', ['115', 'magnet', 'ed2k']),
            "updated_at": "now"
        }
        settings_db.save_setting('nullbr_config', new_config)
        return jsonify({"status": "success", "message": "配置已保存"})

@nullbr_bp.route('/search', methods=['POST'])
@admin_required
def search_resources():
    """搜索接口"""
    data = request.json
    keyword = data.get('keyword')
    page = data.get('page', 1)
    
    if not keyword:
        return jsonify({"status": "error", "message": "搜索关键词不能为空"}), 400

    try:
        result = nullbr_handler.search_media(keyword, page)
        return jsonify(result)
    except ValueError as ve:
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        logger.error(f"搜索出错: {e}")
        return jsonify({"status": "error", "message": "搜索服务暂时不可用"}), 500
    
@nullbr_bp.route('/resources', methods=['POST'])
@admin_required
def get_resources():
    """
    新接口：获取资源列表供前端选择
    """
    data = request.json
    tmdb_id = data.get('tmdb_id') or data.get('id')
    media_type = data.get('media_type', 'movie')
    
    if not tmdb_id:
        return jsonify({"status": "error", "message": "缺少 TMDB ID"}), 400

    try:
        resource_list = nullbr_handler.fetch_resource_list(tmdb_id, media_type)
        return jsonify({
            "status": "success", 
            "data": resource_list,
            "total": len(resource_list)
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@nullbr_bp.route('/push', methods=['POST'])
@admin_required
def push_resource():
    """
    改造后的推送接口：直接接收前端选好的 link
    """
    data = request.json
    link = data.get('link')
    title = data.get('title', '未知资源')
    
    if not link:
        return jsonify({"status": "error", "message": "链接为空"}), 400

    try:
        # ★★★ 核心修复：清洗链接的“脏尾巴” ★★★
        # NULLBR 返回的链接经常以 &# 或 & 结尾，导致 115 转存失败
        # 我们做一个循环清洗，直到尾巴干干净净
        if link:
            link = link.strip() # 先去空格
            
            # 如果结尾是 &# 或者 &，就一直切，直到没有为止
            while link.endswith('&#') or link.endswith('&') or link.endswith('#'):
                if link.endswith('&#'):
                    link = link[:-2]
                elif link.endswith('&') or link.endswith('#'):
                    link = link[:-1]
        
        # 打印一下清洗后的链接，方便调试
        logger.info(f"清洗后的链接: {link}")

        # 推送清洗后的链接
        nullbr_handler.push_to_cms(link, title)
        
        return jsonify({"status": "success", "message": "已推送到 CMS"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@nullbr_bp.route('/presets', methods=['GET'])
@admin_required
def get_presets():
    """获取预设片单列表"""
    return jsonify(nullbr_handler.get_preset_lists())

@nullbr_bp.route('/list', methods=['POST'])
@admin_required
def get_list_content():
    """获取具体片单内容"""
    data = request.json
    list_id = data.get('list_id')
    page = data.get('page', 1)
    
    if not list_id:
        return jsonify({"status": "error", "message": "缺少 List ID"}), 400

    try:
        result = nullbr_handler.fetch_list_items(list_id, page)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
@nullbr_bp.route('/presets', methods=['GET', 'POST', 'DELETE'])
@admin_required
def handle_presets():
    """获取或保存片单配置"""
    if request.method == 'GET':
        return jsonify(nullbr_handler.get_preset_lists())
    
    if request.method == 'POST':
        data = request.json
        presets = data.get('presets')
        
        if not isinstance(presets, list):
            return jsonify({"status": "error", "message": "格式错误，必须是列表"}), 400
            
        # 简单的校验
        valid_presets = []
        for item in presets:
            if item.get('id') and item.get('name'):
                valid_presets.append({
                    "id": str(item.get('id')).strip(),
                    "name": str(item.get('name')).strip()
                })
        
        # 保存到数据库
        settings_db.save_setting('nullbr_presets', valid_presets)
        return jsonify({"status": "success", "message": "片单配置已保存"})
    
    if request.method == 'DELETE':
        # 删除数据库里的配置
        settings_db.delete_setting('nullbr_presets')
        # 返回默认值给前端，方便前端立即更新 UI
        return jsonify({
            "status": "success", 
            "message": "已恢复默认片单",
            "data": nullbr_handler.get_preset_lists() # 此时 get_preset_lists 会自动返回默认值
        })