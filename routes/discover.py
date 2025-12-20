# routes/discover.py
import logging
from flask import Blueprint, jsonify, request, g, session

from extensions import any_login_required
import handler.tmdb as tmdb
from utils import DEFAULT_KEYWORD_MAPPING, contains_chinese, get_tmdb_language_options
from database import media_db, settings_db, request_db
from tasks.discover import task_update_daily_theme, task_replenish_recommendation_pool
import task_manager

discover_bp = Blueprint('discover_bp', __name__, url_prefix='/api/discover')
logger = logging.getLogger(__name__)

def _expand_keyword_labels_to_ids(labels: list) -> str:
    """
    【AND 逻辑版】将中文标签展开为 TMDb 关键词 ID
    不同标签之间使用 ',' (AND)，标签内部 ID 使用 '|' (OR)
    """
    mapping = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
    
    label_groups = []
    for label in labels:
        if label in mapping:
            ids = mapping[label].get('ids', [])
            if ids:
                # 同一个标签内的 ID（如“恐怖”口袋里的多个 ID）依然用 OR 连接
                label_groups.append("|".join([str(_id) for _id in ids]))
        elif str(label).isdigit():
            label_groups.append(str(label))
    
    # ✨ 核心修改：不同标签组之间用逗号连接，实现 AND 逻辑
    return ",".join(label_groups)

def _filter_and_enrich_results(tmdb_data: dict, current_user_id: str, db_item_type: str) -> dict:
    """
    【V3 - 全局订阅状态版】
    辅助函数：过滤TMDb结果，并附加数据库中的全局信息。
    """
    if not tmdb_data or not tmdb_data.get("results"):
        return {"results": [], "total_pages": 0}

    # 步骤 1: 过滤掉没有海报的结果
    original_results = tmdb_data.get("results", [])
    final_filtered_results = [item for item in original_results if item.get("poster_path")]

    if not final_filtered_results:
        return {"results": [], "total_pages": 0}

    # 步骤 3: 附加数据库信息
    tmdb_ids = [str(item.get("id")) for item in final_filtered_results]
    
    # 获取在库状态映射表 (现在 Key 是 "id_type")
    library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type=db_item_type)
    
    # 获取订阅状态 (假设 request_db 内部处理了类型或仅基于ID，如果 request_db 也有同样问题建议一并修改，这里仅展示 discover 的适配)
    subscription_statuses = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, item_type=db_item_type)

    media_type_for_frontend = 'movie' if db_item_type == 'Movie' else 'tv'

    for item in final_filtered_results:
        tmdb_id_str = str(item.get("id"))
        
        # ★★★ 修改点：构建复合键进行查找 ★★★
        lookup_key = f"{tmdb_id_str}_{db_item_type}"
        
        item["in_library"] = lookup_key in library_items_map
        item["emby_item_id"] = library_items_map.get(lookup_key)
        item["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)
        item["media_type"] = media_type_for_frontend
    
    tmdb_data["results"] = final_filtered_results
    return tmdb_data

@discover_bp.route('/movie', methods=['POST'])
@any_login_required
def discover_movies():
    """
    【V3 - 纯净关键词版 + 异常保护】
    根据前端传来的筛选条件，从 TMDb 发现电影。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        # 1. 权限与用户校验
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # 2. 关键词标签 -> 纯关键词 IDs (调用辅助函数)
        labels = data.get('with_keywords', [])
        if isinstance(labels, str): labels = labels.split(',')
        k_ids_str = _expand_keyword_labels_to_ids(labels)

        # 3. 构建干净的参数字典
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            'page': data.get('page', 1),
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''), # 页面顶部原生勾选
            'with_keywords': k_ids_str,                # 映射表生成的 ID
            'without_genres': data.get('without_genres', ''),
            'primary_release_date.gte': data.get('primary_release_date.gte', ''),
            'primary_release_date.lte': data.get('primary_release_date.lte', ''),
            'with_original_language': data.get('with_original_language', ''),
            'with_origin_country': data.get('with_origin_country', ''),
        }
        
        # 4. 清理空参数
        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        # 5. 调用 TMDb 接口
        tmdb_data = tmdb.discover_movie_tmdb(api_key, tmdb_params)
        
        # 6. 附加在库状态和订阅状态
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, 'Movie')
        
        return jsonify(processed_data)

    except Exception as e:
        # 记录详细的错误堆栈到日志，方便排查
        logger.error(f"TMDb 发现电影时发生严重错误: {e}", exc_info=True)
        # 给前端返回一个友好的错误提示
        return jsonify({"status": "error", "message": "从 TMDb 获取电影数据失败，请检查网络或配置。"}), 500

@discover_bp.route('/tv', methods=['POST'])
@any_login_required
def discover_tv_shows():
    """
    【V3 - 纯净关键词版】
    根据前端传来的筛选条件，从 TMDb 发现电视剧。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # ✨ 1. 关键词标签 -> 纯关键词 IDs (调用刚才那个纯净版辅助函数)
        labels = data.get('with_keywords', [])
        if isinstance(labels, str): labels = labels.split(',')
        k_ids_str = _expand_keyword_labels_to_ids(labels)

        # ✨ 2. 为电视剧构建参数字典
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            'page': data.get('page', 1),
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''), # 页面上方勾选的“风格”
            'with_keywords': k_ids_str,                # 映射表生成的“关键词”
            'without_genres': data.get('without_genres', ''),
            'first_air_date.gte': data.get('first_air_date.gte', ''), # 👈 注意这里是 first_air_date
            'first_air_date.lte': data.get('first_air_date.lte', ''),
            'with_original_language': data.get('with_original_language', ''),
            'with_origin_country': data.get('with_origin_country', ''),
        }
        
        # 清理掉值为 None 或空字符串的键
        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        # 调用 TMDb 电视剧发现接口
        tmdb_data = tmdb.discover_tv_tmdb(api_key, tmdb_params)
        
        # 附加在库状态和订阅状态 (类型设为 'Series')
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
    
# ★★★ 搜索接口 ★★★
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
    
@discover_bp.route('/config/languages', methods=['GET'])
@any_login_required
def api_get_discover_languages():
    """为影视探索页面提供专用的、友好的常用语言列表。"""
    try:
        # 直接调用 utils 中的新函数，它已经返回了前端所需的格式
        language_options = get_tmdb_language_options()
        return jsonify(language_options)
    except Exception as e:
        logger.error(f"获取 Discover 语言列表时出错: {e}", exc_info=True)
        return jsonify([]), 500
    
@discover_bp.route('/config/keywords', methods=['GET'])
@any_login_required
def api_get_discover_keywords():
    try:
        mapping = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        options = [{"label": k, "value": k} for k in mapping.keys()]
        return jsonify(sorted(options, key=lambda x: x['label']))
    except Exception as e:
        return jsonify([]), 500
    
@discover_bp.route('/daily_recommendation', methods=['GET'])
@any_login_required
def get_recommendation_pool():
    """
    【V4 - 动态主题适配版】
    读取推荐池，并根据索引从动态映射表中获取主题名称。
    """
    try:
        pool_data = settings_db.get_setting('recommendation_pool')
        theme_index = settings_db.get_setting('recommendation_theme_index')

        # 1. 基础检查
        if not pool_data:
            logger.debug("  ➜ 推荐池不存在或为空，返回 404 以触发前端生成任务。")
            return jsonify({"error": "推荐池尚未生成或为空。"}), 404

        # ✨ 2. 核心修改：从动态映射表中获取主题名称 ✨
        # 这里的逻辑必须与 tasks/discover.py 保持高度一致
        mapping = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        
        # 过滤出有 ID 的项作为轮换池
        theme_list = [(label, info) for label, info in mapping.items() if info.get('ids')]

        theme_name = "今日精选" # 默认兜底名称
        
        if theme_index is not None:
            # 检查索引是否有效（防止用户删除了关键词导致索引越界）
            if 0 <= theme_index < len(theme_list):
                theme_name = theme_list[theme_index][0] # 拿到中文标签，如“恐怖”
            else:
                # 如果索引失效，通常是因为映射表变动了，这里返回兜底名
                # 下次后台任务运行时会自动校正索引
                theme_name = "主题更新中"

        response_data = {
            "theme_name": theme_name,
            "pool": pool_data
        }
        
        return jsonify(response_data)
        
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
        logger.debug("  ➜ 收到前端请求，自动触发【每日推荐】更新任务...")
        # 使用 task_manager 提交任务到后台执行
        task_manager.submit_task(
            task_function=task_update_daily_theme,
            task_name="自动更新每日推荐",
            processor_type='media' # 这个任务需要 'media' 类型的处理器
        )
        return jsonify({"status": "ok", "message": "更新任务已在后台启动。"}), 202
    except Exception as e:
        logger.error(f"自动触发每日推荐任务时失败: {e}", exc_info=True)
        return jsonify({"error": "启动任务失败"}), 500
    
def check_and_replenish_pool():
    """
    【V2 - 修正版】
    检查推荐池库存，如果低于阈值则触发后台补充任务。
    这个函数应该在订阅成功后被调用。
    """
    try:
        # ★ 核心修正：分两步安全地获取推荐池数据
        # 1. 先用正确的单个参数获取设置
        pool_data = settings_db.get_setting('recommendation_pool')
        # 2. 如果返回的是 None (比如第一次运行还没有这个设置)，则视为空列表
        pool = pool_data or []
        
        # 定义库存阈值
        REPLENISH_THRESHOLD = 5 

        if len(pool) < REPLENISH_THRESHOLD:
            logger.debug(f"  ➜ 推荐池库存 ({len(pool)}) 低于阈值 ({REPLENISH_THRESHOLD})，触发后台补充任务。")
            task_manager.submit_task(
                task_function=task_replenish_recommendation_pool,
                task_name="补充每日推荐池",
                processor_type='media'
            )
        else:
            logger.debug(f"  ➜ 推荐池库存充足 ({len(pool)})，无需补充。")
            
    except Exception as e:
        logger.error(f"检查并补充推荐池时出错: {e}", exc_info=True)