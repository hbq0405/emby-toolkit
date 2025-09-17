# routes/actor_subscriptions.py

from flask import Blueprint, request, jsonify
import logging
import psycopg2 

# 导入需要的模块
import db_handler 
import config_manager
import tmdb_handler
import task_manager
import moviepilot_handler
from extensions import login_required, processor_ready_required, task_lock_required

# 1. 创建演员订阅蓝图
actor_subscriptions_bp = Blueprint('actor_subscriptions', __name__, url_prefix='/api/actor-subscriptions')

logger = logging.getLogger(__name__)

# 2. 使用蓝图定义路由
@actor_subscriptions_bp.route('/search', methods=['GET'])
@login_required
@processor_ready_required
def api_search_actors():
    # ... (此函数不直接与本地数据库交互，无需修改) ...
    query = request.args.get('name', '').strip()
    if not query:
        return jsonify({"error": "必须提供搜索关键词 'name'"}), 400

    tmdb_api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
    if not tmdb_api_key:
        return jsonify({"error": "服务器未配置TMDb API Key"}), 503

    try:
        search_results = tmdb_handler.search_person_tmdb(query, tmdb_api_key)
        if search_results is None:
            return jsonify({"error": "从TMDb搜索演员时发生错误"}), 500
        
        formatted_results = []
        for person in search_results:
            if person.get('profile_path') and person.get('known_for'):
                 formatted_results.append({
                     "id": person.get("id"), "name": person.get("name"),
                     "profile_path": person.get("profile_path"),
                     "known_for_department": person.get("known_for_department"),
                     "known_for": ", ".join([item.get('title', item.get('name', '')) for item in person.get('known_for', [])])
                 })
        return jsonify(formatted_results)
    except Exception as e:
        logger.error(f"API /api/actor-subscriptions/search 发生错误: {e}", exc_info=True)
        return jsonify({"error": "搜索演员时发生未知的服务器错误"}), 500

# ✨ 定义默认订阅配置的路由
@actor_subscriptions_bp.route('/default-config', methods=['GET', 'POST'])
@login_required
def handle_default_actor_config():
    """
    处理获取和保存演员订阅的默认配置。
    此函数直接与数据库交互，使用带 _json 后缀的标准键名。
    """
    if request.method == 'GET':
        try:
            # 直接从数据库获取标准格式的配置
            default_config = db_handler.get_setting('actor_subscriptions_default_config') or {}
            
            # ★★★ 最终方案：直接返回标准格式，确保所有必需的键存在 ★★★
            final_config = {
                "start_year": default_config.get("start_year"),
                "media_types": default_config.get("media_types", []),
                "genres_include_json": default_config.get("genres_include_json", []),
                "genres_exclude_json": default_config.get("genres_exclude_json", []),
                "min_rating": default_config.get("min_rating", 0.0)
            }
            return jsonify(final_config)
        except Exception as e:
            logger.error(f"获取默认演员订阅配置失败: {e}", exc_info=True)
            return jsonify({"error": "获取默认配置时发生服务器内部错误"}), 500

    if request.method == 'POST':
        try:
            # ★★★ 最终方案：假设前端发送的就是带 _json 后缀的标准格式，直接保存 ★★★
            new_config = request.json
            db_handler.save_setting('actor_subscriptions_default_config', new_config)
            return jsonify({"message": "默认配置已成功保存！"})
        except Exception as e:
            logger.error(f"保存默认演员订阅配置失败: {e}", exc_info=True)
            return jsonify({"error": "保存默认配置时发生服务器内部错误"}), 500

@actor_subscriptions_bp.route('', methods=['GET', 'POST'])
@login_required
def handle_actor_subscriptions():
    if request.method == 'GET':
        try:
            subscriptions = db_handler.get_all_actor_subscriptions()
            return jsonify(subscriptions)
        except Exception as e:
            logger.error(f"获取演员订阅列表失败: {e}", exc_info=True)
            return jsonify({"error": "获取订阅列表时发生服务器内部错误"}), 500

    if request.method == 'POST':
        data = request.json
        tmdb_person_id = data.get('tmdb_person_id')
        actor_name = data.get('actor_name')

        if not tmdb_person_id or not actor_name:
            return jsonify({"error": "请求无效: 缺少 tmdb_person_id 或 actor_name"}), 400
        
        # ✨ [核心修改] 应用默认订阅配置
        subscription_config = data.get('config')
        
        # 如果前端没有提供配置 (None 或空字典)，则从系统中加载默认配置
        if not subscription_config:
            logger.info(f"为新演员 '{actor_name}' 应用默认订阅配置。")
            # ★★★ 从数据库获取默认配置 ★★★
            subscription_config = db_handler.get_setting('actor_subscriptions_default_config') or {}
        else:
            logger.info(f"为新演员 '{actor_name}' 使用了自定义的订阅配置。")

        try:
            new_sub_id = db_handler.add_actor_subscription(
                tmdb_person_id=tmdb_person_id,
                actor_name=actor_name,
                profile_path=data.get('profile_path'),
                config=subscription_config # ★ 使用最终确定的配置
            )
            return jsonify({"message": f"演员 {actor_name} 已成功订阅！", "id": new_sub_id}), 201
        
        except psycopg2.IntegrityError:
            return jsonify({"error": "该演员已经被订阅过了"}), 409
        except Exception as e:
            logger.error(f"添加演员订阅失败: {e}", exc_info=True)
            return jsonify({"error": "添加订阅时发生服务器内部错误"}), 500

@actor_subscriptions_bp.route('/<int:sub_id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def handle_single_actor_subscription(sub_id):
    if request.method == 'GET':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            response_data = db_handler.get_single_subscription_details(sub_id)
            return jsonify(response_data) if response_data else ({"error": "未找到指定的订阅"}, 404)
        except Exception as e:
            logger.error(f"获取订阅详情 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "获取订阅详情时发生服务器内部错误"}), 500
    
    if request.method == 'PUT':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            success = db_handler.update_actor_subscription(sub_id, request.json)
            return jsonify({"message": "订阅已成功更新！"}) if success else ({"error": "未找到指定的订阅"}, 404)
        except Exception as e:
            logger.error(f"更新订阅 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "更新订阅时发生服务器内部错误"}), 500

    if request.method == 'DELETE':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            db_handler.delete_actor_subscription(sub_id)
            return jsonify({"message": "订阅已成功删除。"})
        except Exception as e:
            logger.error(f"删除订阅 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "删除订阅时发生服务器内部错误"}), 500

@actor_subscriptions_bp.route('/<int:sub_id>/refresh', methods=['POST'])
@login_required
def refresh_single_actor_subscription(sub_id):
    # ★★★ 核心修复：现在我们确实需要导入函数对象了 ★★★
    from tasks import task_scan_actor_media 

    actor_name = f"订阅ID {sub_id}"

    # ★★★ 核心修复：按照正确的参数顺序调用 submit_task ★★★
    # 1. task_function: 任务函数本身 (task_scan_actor_media)
    # 2. task_name:     任务的显示名称 (一个字符串)
    # 3. processor_type: 指定使用 'actor' 处理器 (一个字符串)
    # 4. *args:         所有要传递给 task_scan_actor_media 的额外参数 (sub_id)
    task_manager.submit_task(
        task_scan_actor_media, 
        f"手动刷新演员: {actor_name}", 
        'actor', 
        sub_id
    )
    
    return jsonify({"message": f"刷新演员 {actor_name} 作品的任务已提交！"}), 202

# ★★★ 手动订阅单个缺失作品的API端点 ★★★
@actor_subscriptions_bp.route('/media/<int:media_id>/subscribe', methods=['POST'])
@login_required
def subscribe_single_tracked_media(media_id):
    """
    手动触发对单个已追踪但状态为'MISSING'的媒体项的订阅。
    """
    try:
        # 1. 检查配额
        current_quota = db_handler.get_subscription_quota()
        if current_quota <= 0:
            return jsonify({"error": "今日订阅配额已用尽"}), 429 # 429 Too Many Requests

        # 2. 获取媒体信息
        media_info = db_handler.get_tracked_media_by_id(media_id)
        if not media_info:
            return jsonify({"error": "未找到指定的媒体项"}), 404
        if media_info.get('status') != 'MISSING':
            return jsonify({"error": f"此媒体项状态为 {media_info.get('status')}，无需订阅"}), 400

        # 3. 执行订阅
        success = False
        config = config_manager.APP_CONFIG
        if media_info['media_type'] == 'Movie':
            movie_payload = {'title': media_info['title'], 'tmdb_id': media_info['tmdb_media_id']}
            success = moviepilot_handler.subscribe_movie_to_moviepilot(movie_payload, config)
        elif media_info['media_type'] == 'Series':
            series_payload = {'item_name': media_info['title'], 'tmdb_id': media_info['tmdb_media_id']}
            success = moviepilot_handler.subscribe_series_to_moviepilot(series_payload, season_number=None, config=config)

        # 4. 根据结果更新数据库和配额
        if success:
            db_handler.decrement_subscription_quota()
            db_handler.update_tracked_media_status(media_id, 'SUBSCRIBED')
            return jsonify({"message": f"《{media_info['title']}》已成功提交订阅！"})
        else:
            return jsonify({"error": "提交到 MoviePilot 失败，请检查其日志"}), 500

    except Exception as e:
        logger.error(f"手动订阅媒体项 {media_id} 失败: {e}", exc_info=True)
        return jsonify({"error": "订阅时发生未知的服务器错误"}), 500