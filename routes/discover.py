# routes/discover.py
import logging
from flask import Blueprint, jsonify, request, g, session

from extensions import any_login_required
import handler.tmdb as tmdb
from utils import KEYWORD_ID_MAP, contains_chinese
from database import user_db, media_db, settings_db
from tasks.discover import task_update_daily_recommendation
import task_manager

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
    subscription_statuses = user_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids)

    media_type_for_frontend = 'movie' if db_item_type == 'Movie' else 'tv'

    for item in final_filtered_results:
        tmdb_id_str = str(item.get("id"))
        item["in_library"] = tmdb_id_str in library_items_map
        item["emby_item_id"] = library_items_map.get(tmdb_id_str)
        item["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)
        # ★★★ 把标签贴上！ ★★★
        item["media_type"] = media_type_for_frontend
    
    tmdb_data["results"] = final_filtered_results
    return tmdb_data

@discover_bp.route('/movie', methods=['POST'])
@any_login_required
def discover_movies():
    """
    【V2 - 支持高级筛选】
    根据前端传来的筛选条件，从 TMDb 发现电影。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # ★★★ 核心修改 1: 构建一个干净的参数字典 ★★★
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            'page': data.get('page', 1),
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''),
            'with_origin_country': data.get('with_origin_country', ''),
            'with_keywords': data.get('with_keywords', ''),
            
            # ★★★ 新增的参数 ★★★
            'without_genres': data.get('without_genres', ''),
            'primary_release_date.gte': data.get('primary_release_date.gte', ''),
            'primary_release_date.lte': data.get('primary_release_date.lte', ''),
        }
        
        # 清理掉值为 None 或空字符串的键，避免发送空参数
        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        tmdb_data = tmdb.discover_movie_tmdb(api_key, tmdb_params)
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, 'Movie')
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 发现电影时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取数据失败"}), 500

@discover_bp.route('/tv', methods=['POST'])
@any_login_required
def discover_tv_shows():
    """
    【V2 - 支持高级筛选】
    根据前端传来的筛选条件，从 TMDb 发现电视剧。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # ★★★ 核心修改 2: 为电视剧构建参数字典 ★★★
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            'page': data.get('page', 1),
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''),
            'with_origin_country': data.get('with_origin_country', ''),
            'with_keywords': data.get('with_keywords', ''),
            
            # ★★★ 新增的参数 (注意日期参数名不同) ★★★
            'without_genres': data.get('without_genres', ''),
            'first_air_date.gte': data.get('first_air_date.gte', ''),
            'first_air_date.lte': data.get('first_air_date.lte', ''),
        }
        
        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        tmdb_data = tmdb.discover_tv_tmdb(api_key, tmdb_params)
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
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)
    try:
        if media_type == 'movie':
            genres = tmdb.get_movie_genres_tmdb(api_key)
        elif media_type == 'tv':
            genres = tmdb.get_tv_genres_tmdb(api_key)
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

    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']
        
        # ★★★ 核心修改 4: 第三次调用辅助函数 ★★★
        tmdb_data = tmdb.search_media_for_discover(query=query, api_key=api_key, item_type=media_type, page=page)
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
    
@discover_bp.route('/daily_recommendation', methods=['GET'])
@any_login_required
def get_recommendation_pool():
    """
    读取并返回由后台任务生成的“推荐池”列表。
    """
    try:
        # ★ 使用新的 key 来获取数据
        pool_data = settings_db.get_setting('recommendation_pool')

        if pool_data is None:
            return jsonify({"error": "推荐池尚未生成，请稍后再试。"}), 404
        
        # 直接返回整个列表
        return jsonify(pool_data)
    except Exception as e:
        logger.error(f"读取推荐池数据时出错: {e}", exc_info=True)
        return jsonify({"error": "获取推荐池失败"}), 500
    
@discover_bp.route('/trigger_recommendation_update', methods=['POST'])
@any_login_required
def trigger_recommendation_update():
    """
    手动触发一次“每日推荐”更新任务。
    这是一个异步操作，接口会立即返回。
    """
    try:
        logger.info("  ➜ 收到前端请求，手动触发【每日推荐】更新任务...")
        # 使用 task_manager 提交任务到后台执行
        task_manager.submit_task(
            task_function=task_update_daily_recommendation,
            task_name="手动更新每日推荐",
            processor_type='media' # 这个任务需要 'media' 类型的处理器
        )
        return jsonify({"status": "ok", "message": "更新任务已在后台启动。"}), 202
    except Exception as e:
        logger.error(f"手动触发每日推荐任务时失败: {e}", exc_info=True)
        return jsonify({"error": "启动任务失败"}), 500