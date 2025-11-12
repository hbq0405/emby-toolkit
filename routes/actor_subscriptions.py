# routes/actor_subscriptions.py

from flask import Blueprint, request, jsonify
import logging
import psycopg2 

# 导入需要的模块
 
import config_manager
import handler.tmdb as tmdb
import task_manager
import handler.moviepilot as moviepilot
from database import settings_db, actor_db, media_db
from extensions import admin_required, processor_ready_required
from tasks.subscriptions import _check_and_get_series_best_version_flag
from tasks.helpers import is_movie_subscribable

# 1. 创建演员订阅蓝图
actor_subscriptions_bp = Blueprint('actor_subscriptions', __name__, url_prefix='/api/actor-subscriptions')

logger = logging.getLogger(__name__)

# 2. 使用蓝图定义路由
@actor_subscriptions_bp.route('/search', methods=['GET'])
@admin_required
@processor_ready_required
def api_search_actors():
    query = request.args.get('name', '').strip()
    if not query:
        return jsonify({"error": "必须提供搜索关键词 'name'"}), 400

    tmdb_api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
    if not tmdb_api_key:
        return jsonify({"error": "服务器未配置TMDb API Key"}), 503

    try:
        search_results = tmdb.search_person_tmdb(query, tmdb_api_key)
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
@admin_required
def handle_default_actor_config():
    """
    处理获取和保存演员订阅的默认配置。
    此函数直接与数据库交互，使用带 _json 后缀的标准键名。
    """
    if request.method == 'GET':
        try:
            # 直接从数据库获取标准格式的配置
            default_config = settings_db.get_setting('actor_subscriptions_default_config') or {}
            
            # ★★★ 最终方案：直接返回标准格式，确保所有必需的键存在 ★★★
            final_config = {
                "start_year": default_config.get("start_year"),
                "media_types": default_config.get("media_types", []),
                "genres_include_json": default_config.get("genres_include_json", []),
                "genres_exclude_json": default_config.get("genres_exclude_json", []),
                "min_rating": default_config.get("min_rating", 0.0),
                "main_role_only": default_config.get("main_role_only", False),
                "min_vote_count": default_config.get("min_vote_count", 10)
            }
            return jsonify(final_config)
        except Exception as e:
            logger.error(f"获取默认演员订阅配置失败: {e}", exc_info=True)
            return jsonify({"error": "获取默认配置时发生服务器内部错误"}), 500

    if request.method == 'POST':
        try:
            # ★★★ 最终方案：假设前端发送的就是带 _json 后缀的标准格式，直接保存 ★★★
            new_config = request.json
            settings_db.save_setting('actor_subscriptions_default_config', new_config)
            return jsonify({"message": "默认配置已成功保存！"})
        except Exception as e:
            logger.error(f"保存默认演员订阅配置失败: {e}", exc_info=True)
            return jsonify({"error": "保存默认配置时发生服务器内部错误"}), 500

@actor_subscriptions_bp.route('', methods=['GET', 'POST'])
@admin_required
def handle_actor_subscriptions():
    if request.method == 'GET':
        try:
            subscriptions = actor_db.get_all_actor_subscriptions()
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
            subscription_config = settings_db.get_setting('actor_subscriptions_default_config') or {}
        else:
            logger.info(f"为新演员 '{actor_name}' 使用了自定义的订阅配置。")

        try:
            new_sub_id = actor_db.add_actor_subscription(
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
@admin_required
def handle_single_actor_subscription(sub_id):
    if request.method == 'GET':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            response_data = actor_db.get_single_subscription_details(sub_id)
            return jsonify(response_data) if response_data else ({"error": "未找到指定的订阅"}, 404)
        except Exception as e:
            logger.error(f"获取订阅详情 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "获取订阅详情时发生服务器内部错误"}), 500
    
    if request.method == 'PUT':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            success = actor_db.update_actor_subscription(sub_id, request.json)
            return jsonify({"message": "订阅已成功更新！"}) if success else ({"error": "未找到指定的订阅"}, 404)
        except Exception as e:
            logger.error(f"更新订阅 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "更新订阅时发生服务器内部错误"}), 500

    if request.method == 'DELETE':
        try:
            # ★★★ 核心修改：调用新的 db_handler 函数，不再需要 db_path 参数
            actor_db.delete_actor_subscription(sub_id)
            return jsonify({"message": "订阅已成功删除。"})
        except Exception as e:
            logger.error(f"删除订阅 {sub_id} 失败: {e}", exc_info=True)
            return jsonify({"error": "删除订阅时发生服务器内部错误"}), 500

@actor_subscriptions_bp.route('/<int:sub_id>/refresh', methods=['POST'])
@admin_required
def refresh_single_actor_subscription(sub_id):
    from tasks import task_scan_actor_media 

    # ★★★ 核心修改：先从数据库获取订阅详情以拿到演员名 ★★★
    try:
        subscription_details = actor_db.get_single_subscription_details(sub_id)
        if not subscription_details:
            return jsonify({"error": f"未找到 ID 为 {sub_id} 的订阅"}), 404
        
        # 如果找到了，就用真实的演员名；如果没找到名字，再用 ID 作为备用
        actor_name = subscription_details.get('actor_name', f"订阅ID {sub_id}")
    except Exception as e:
        logger.error(f"刷新订阅 {sub_id} 前获取演员名失败: {e}", exc_info=True)
        # 即使数据库查询失败，也用 ID 作为备用名称提交任务，保证功能可用性
        actor_name = f"订阅ID {sub_id}"

    # 使用获取到的 actor_name 提交任务
    task_manager.submit_task(
        task_scan_actor_media, 
        f"手动刷新演员: {actor_name}", # <--- 这里现在会显示演员的真实姓名
        'actor', 
        sub_id
    )
    
    return jsonify({"message": f"刷新演员 {actor_name} 作品的任务已提交！"}), 202

# ★★★ 智能恢复（重新评估）单个作品状态的 API 端点 ★★★
@actor_subscriptions_bp.route('/media/<int:media_id>/re-evaluate', methods=['POST'])
@admin_required
def api_re_evaluate_tracked_media(): # ★ 2. 移除函数参数
    """将一个“已忽略”或“缺失”的媒体项恢复到 'WANTED' 状态，以便重新评估。"""
    
    # ★ 3. 从 request.json 中获取参数
    data = request.json
    tmdb_id = data.get('tmdb_id')
    item_type = data.get('item_type')

    if not tmdb_id or not item_type:
        return jsonify({"error": "请求体中必须包含 'tmdb_id' 和 'item_type'"}), 400

    try:
        media_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
        media_info = media_map.get(tmdb_id)
        if not media_info:
            return jsonify({"error": "未找到指定的媒体项"}), 404

        # 核心操作：将状态改为 WANTED
        media_db.update_subscription_status(
            tmdb_ids=tmdb_id,
            item_type=item_type,
            new_status='WANTED',
            source={"type": "manual_re_evaluate"},
            force_unignore=True # ★ 4. 增加 force_unignore 参数，确保能从 IGNORED 状态恢复
        )
        
        message = f"《{media_info['title']}》已恢复评估！下次演员扫描时将自动更新其最新状态。"
        return jsonify({"message": message, "new_status": "WANTED"})
    except Exception as e:
        logger.error(f"恢复媒体项 {tmdb_id} 状态失败: {e}", exc_info=True)
        return jsonify({"error": "恢复状态时发生未知的服务器错误"}), 500