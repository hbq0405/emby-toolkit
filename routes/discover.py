# routes/discover.py
import logging
from flask import Blueprint, jsonify, request, g, session

from extensions import any_login_required
import tmdb_handler
from database import media_db

discover_bp = Blueprint('discover_bp', __name__, url_prefix='/api/discover')
logger = logging.getLogger(__name__)

@discover_bp.route('/movie', methods=['POST'])
@any_login_required
def discover_movies():
    """
    根据前端传来的筛选条件，从 TMDb 发现电影，并附加上“是否在库”和“订阅状态”的信息。
    """
    params = request.json
    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)
    params.setdefault('with_origin_country', '')

    try:
        # ★ 核心修改 1: 直接检查并获取 'emby_user_id' ★
        if 'emby_user_id' not in session:
            logger.error("API 认证通过，但 session 中未找到 'emby_user_id'。这通常发生在本地管理员访问此接口时。")
            # 对于 Emby 探索功能，我们只允许 Emby 用户访问
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403

        current_user_id = session['emby_user_id']

        tmdb_data = tmdb_handler.discover_movie_tmdb(api_key, params)
        if not tmdb_data or not tmdb_data.get("results"):
            return jsonify({"results": [], "total_pages": 0})

        tmdb_ids = [str(movie.get("id")) for movie in tmdb_data["results"]]
        
        library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type='Movie')
        subscription_statuses = media_db.get_subscription_statuses(tmdb_ids, current_user_id)

        # ★ 2. 遍历结果，同时附加三个状态字段
        for movie in tmdb_data["results"]:
            tmdb_id_str = str(movie.get("id"))
            
            # in_library 现在通过检查字典的键来判断
            movie["in_library"] = tmdb_id_str in library_items_map
            # 新增 emby_item_id 字段
            movie["emby_item_id"] = library_items_map.get(tmdb_id_str) # 如果不在库，get返回None
            
            movie["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)

        return jsonify(tmdb_data)

    except Exception as e:
        logger.error(f"TMDb 发现电影时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取数据失败"}), 500

@discover_bp.route('/tv', methods=['POST'])
@any_login_required
def discover_tv_shows():
    """
    根据前端传来的筛选条件，从 TMDb 发现电视剧，并附加上“是否在库”和“订阅状态”的信息。
    """
    params = request.json
    api_key = tmdb_handler.config_manager.APP_CONFIG.get(tmdb_handler.constants.CONFIG_OPTION_TMDB_API_KEY)
    params.setdefault('with_origin_country', '')

    try:
        # ★ 核心修改 2: 在电视剧接口也使用同样精准的逻辑 ★
        if 'emby_user_id' not in session:
            logger.error("API 认证通过，但 session 中未找到 'emby_user_id'。")
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
            
        current_user_id = session['emby_user_id']

        tmdb_data = tmdb_handler.discover_tv_tmdb(api_key, params)
        if not tmdb_data or not tmdb_data.get("results"):
            return jsonify({"results": [], "total_pages": 0})

        tmdb_ids = [str(tv.get("id")) for tv in tmdb_data["results"]]
        
        library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type='Series')
        subscription_statuses = media_db.get_subscription_statuses(tmdb_ids, current_user_id)

        for tv_show in tmdb_data["results"]:
            tmdb_id_str = str(tv_show.get("id"))
            tv_show["in_library"] = tmdb_id_str in library_items_map
            tv_show["emby_item_id"] = library_items_map.get(tmdb_id_str)
            tv_show["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)

        return jsonify(tmdb_data)

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