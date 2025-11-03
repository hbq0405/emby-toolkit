# routes/discover.py
import logging
from flask import Blueprint, jsonify, request, g, session

from extensions import any_login_required
import tmdb_handler
from utils import KEYWORD_ID_MAP, contains_chinese
from database import media_db

discover_bp = Blueprint('discover_bp', __name__, url_prefix='/api/discover')
logger = logging.getLogger(__name__)

def _filter_and_enrich_results(tmdb_data: dict, current_user_id: str, db_item_type: str) -> dict:
    """
    【V3 - 全局订阅状态版】
    辅助函数：过滤TMDb结果，并附加数据库中的全局信息。
    """
    if not tmdb_data or not tmdb_data.get("results"):
        return {"results": [], "total_pages": 0}

    # 步骤 1: 过滤掉没有海报的结果
    original_results = tmdb_data.get("results", [])
    results_with_poster = [item for item in original_results if item.get("poster_path")]

    # 步骤 2: 过滤掉没有中文元数据的结果
    final_filtered_results = [
        item for item in results_with_poster 
        if contains_chinese(item.get('title') or item.get('name'))
    ]

    if not final_filtered_results:
        return {"results": [], "total_pages": 0}

    # 步骤 3: 附加数据库信息
    tmdb_ids = [str(item.get("id")) for item in final_filtered_results]
    
    library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type=db_item_type)
    
    # ★★★ 核心修改：调用新的全局状态查询函数，不再传入 current_user_id ★★★
    subscription_statuses = media_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids)

    for item in final_filtered_results:
        tmdb_id_str = str(item.get("id"))
        item["in_library"] = tmdb_id_str in library_items_map
        item["emby_item_id"] = library_items_map.get(tmdb_id_str)
        # 从全局状态字典中获取状态
        item["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)
    
    # 步骤 4: 将原始数据中的 results 替换为我们处理后的版本并返回
    tmdb_data["results"] = final_filtered_results
    return tmdb_data

@discover_bp.route('/movie', methods=['POST'])
@any_login_required
def discover_movies():
    """
    根据前端传来的筛选条件，从 TMDb 发现电影。
    """
    params = request.json
    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)
    params.setdefault('with_origin_country', '')

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # ★★★ 核心修改 2: 调用辅助函数简化逻辑 ★★★
        tmdb_data = tmdb_handler.discover_movie_tmdb(api_key, params)
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, 'Movie')
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 发现电影时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取数据失败"}), 500

@discover_bp.route('/tv', methods=['POST'])
@any_login_required
def discover_tv_shows():
    """
    根据前端传来的筛选条件，从 TMDb 发现电视剧。
    """
    params = request.json
    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)
    params.setdefault('with_origin_country', '')

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # ★★★ 核心修改 3: 再次调用辅助函数 ★★★
        tmdb_data = tmdb_handler.discover_tv_tmdb(api_key, params)
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, 'Series')
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 发现电视剧时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取数据失败"}), 500


# genres 接口不需要改动
@discover_bp.route('/genres/<string:media_type>', methods=['GET'])
@any_login_required
def get_genres(media_type):
    """获取电影或电视剧的类型列表。"""
    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)
    try:
        if media_type == 'movie':
            genres = tmdb_handler.get_movie_genres_tmdb(api_key)
        elif media_type == 'tv':
            genres = tmdb_handler.get_tv_genres_tmdb(api_key)
        else:
            return jsonify({"status": "error", "message": "无效的媒体类型"}), 400
        return jsonify(genres)
    except Exception as e:
        logger.error(f"获取 TMDb 类型列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取类型列表失败"}), 500
    
# ★★★ 新增搜索接口 ★★★
@discover_bp.route('/search', methods=['POST'])
@any_login_required
def search_media_handler():
    """
    根据前端传来的搜索词，从 TMDb 搜索影视。
    """
    data = request.json
    query = data.get('query')
    media_type = data.get('media_type', 'movie')
    page = data.get('page', 1)

    if not query:
        return jsonify({"status": "error", "message": "搜索词不能为空"}), 400

    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']
        
        # ★★★ 核心修改 4: 第三次调用辅助函数 ★★★
        tmdb_data = tmdb_handler.search_media_for_discover(query=query, api_key=api_key, item_type=media_type, page=page)
        db_item_type = 'Movie' if media_type == 'movie' else 'Series'
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, db_item_type)
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 搜索 {media_type} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 搜索数据失败"}), 500
    
@discover_bp.route('/config/keywords', methods=['GET'])
@any_login_required
def api_get_discover_keywords():
    """为影视探索页面提供专用的、带TMDb ID的关键词列表。"""
    try:
        # 从 KEYWORD_ID_MAP 构建前端需要的格式
        keyword_options = [
            {"label": chinese_name, "value": tmdb_id}
            for chinese_name, tmdb_id in KEYWORD_ID_MAP.items()
        ]
        # 按中文标签排序
        sorted_options = sorted(keyword_options, key=lambda x: x['label'])
        return jsonify(sorted_options)
    except Exception as e:
        logger.error(f"获取 Discover 关键词列表时出错: {e}", exc_info=True)
        return jsonify([]), 500