# routes/collections.py

from flask import Blueprint, request, jsonify
import logging
import json

# 导入需要的模块
import db_handler
import config_manager
import moviepilot_handler
from extensions import login_required, task_lock_required, processor_ready_required

# 1. 创建电影合集蓝图
collections_bp = Blueprint('collections', __name__, url_prefix='/api/collections')

logger = logging.getLogger(__name__)

# 2. 使用蓝图定义路由
@collections_bp.route('/status', methods=['GET'])
@login_required
@processor_ready_required
def api_get_collections_status():
    try:
        final_results = db_handler.get_all_collections()
        return jsonify(final_results)
    except Exception as e:
        logger.error(f"读取合集状态时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "读取合集时发生服务器内部错误"}), 500

# ★★★ 将电影提交到 MoviePilot 订阅  ★★★
@collections_bp.route('/subscribe', methods=['POST'])
@login_required
def api_subscribe_moviepilot():
    data = request.json
    tmdb_id = data.get('tmdb_id')
    title = data.get('title')

    if not tmdb_id or not title:
        return jsonify({"error": "请求中缺少 tmdb_id 或 title"}), 400

    # 1. 准备传递给业务逻辑函数的数据
    movie_info = {
        'tmdb_id': tmdb_id,
        'title': title
    }

    # 2. 直接调用业务逻辑函数，并将全局配置传递进去
    #    所有复杂的登录、请求、错误处理都已封装在函数内部
    success = moviepilot_handler.subscribe_movie_to_moviepilot(movie_info, config_manager.APP_CONFIG)

    # 3. 根据返回的布尔值，生成对应的API响应
    if success:
        return jsonify({"message": f"《{title}》已成功提交到 MoviePilot 订阅！"}), 200
    else:
        # 具体的错误原因已经被 moviepilot_handler 记录到日志中
        # API层面只需返回一个通用的失败信息
        return jsonify({"error": "订阅失败，请检查后端日志获取详细信息。"}), 500

@collections_bp.route('/subscribe_all_missing', methods=['POST'])
@login_required
def api_subscribe_all_missing():
    """
    【PG 兼容最终修复版】一键订阅所有缺失的电影。
    - 移除了所有不必要的 json.loads() 调用。
    - 直接处理由 psycopg2 解析好的 Python 列表对象。
    """
    logger.info("API (Blueprint): 收到一键订阅所有缺失电影的请求。")
    total_subscribed_count = 0
    total_failed_count = 0
    
    try:
        # 步骤 1: 从数据库获取所有包含缺失电影的合集。
        # db_handler.get_collections_with_missing_movies() 现在返回的每一行中，
        # 'missing_movies_json' 字段已经是 Python 列表。
        collections_to_process = db_handler.get_collections_with_missing_movies()

        if not collections_to_process:
            return jsonify({"message": "没有发现任何缺失的电影需要订阅。", "count": 0}), 200

        # 步骤 2: 遍历合集列表
        for collection in collections_to_process:
            collection_id = collection['emby_collection_id']
            
            # ▼▼▼ 核心修正：直接使用已经是列表的字段，不再需要 try-except 和 json.loads ▼▼▼
            movies = collection.get('missing_movies_json')
            
            # 增加一个健壮性检查，确保它确实是一个列表
            if not isinstance(movies, list):
                logger.warning(f"合集 {collection_id} 的 missing_movies_json 字段格式不正确，已跳过。")
                continue
            # ▲▲▲ 修正结束 ▲▲▲

            needs_db_update = False
            for movie in movies:
                # 步骤 3: 检查状态并订阅
                if movie.get('status') == 'missing':
                    success = moviepilot_handler.subscribe_movie_to_moviepilot(movie, config_manager.APP_CONFIG)
                    if success:
                        movie['status'] = 'subscribed'
                        total_subscribed_count += 1
                        needs_db_update = True
                    else:
                        total_failed_count += 1
            
            # 步骤 4: 如果状态有变，则更新数据库
            if needs_db_update:
                db_handler.update_collection_movies(collection_id, movies)
        
        message = f"操作完成！成功提交 {total_subscribed_count} 部电影订阅。"
        if total_failed_count > 0:
            message += f" 有 {total_failed_count} 部电影订阅失败，请检查日志。"
        
        return jsonify({"message": message, "count": total_subscribed_count}), 200

    except Exception as e:
        logger.error(f"执行一键订阅时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理一键订阅时发生内部错误"}), 500

@collections_bp.route('/update_movie_status', methods=['POST'])
@login_required
def api_update_movie_status():
    data = request.json
    collection_id = data.get('collection_id')
    movie_tmdb_id = data.get('movie_tmdb_id')
    new_status = data.get('new_status')

    if not all([collection_id, movie_tmdb_id, new_status]):
        return jsonify({"error": "缺少 collection_id, movie_tmdb_id 或 new_status"}), 400
    
    if new_status not in ['subscribed', 'missing', 'ignored']:
        return jsonify({"error": "无效的状态"}), 400

    try:
        success = db_handler.update_single_movie_status_in_collection(
            collection_id=collection_id,
            movie_tmdb_id=movie_tmdb_id,
            new_status=new_status
        )
        if success:
            return jsonify({"message": "电影状态已成功更新！"}), 200
        else:
            return jsonify({"error": "未在该合集的电影列表中找到指定的电影或合集"}), 404
    except Exception as e:
        logger.error(f"更新电影状态时发生数据库错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理请求时发生内部错误"}), 500
    
# ★★★ 批量将缺失电影的状态标记为“已订阅”（不触发真订阅） ★★★
@collections_bp.route('/batch_mark_as_subscribed', methods=['POST'])
@login_required
def api_batch_mark_as_subscribed():
    data = request.json
    collection_ids = data.get('collection_ids')

    if not collection_ids or not isinstance(collection_ids, list):
        return jsonify({"error": "请求中缺少 collection_ids 或格式不正确"}), 400

    logger.info(f"API: 收到请求，将 {len(collection_ids)} 个合集中的缺失电影标记为已订阅。")
    
    try:
        # 调用一个新的、只操作数据库的函数
        updated_count = db_handler.batch_mark_movies_as_subscribed_in_collections(
            collection_ids=collection_ids
        )
        
        if updated_count > 0:
            message = f"操作成功！共将 {updated_count} 部缺失电影的状态标记为“已订阅”。"
        else:
            message = "操作完成，但在所选合集中没有找到需要更新状态的缺失电影。"
            
        return jsonify({"message": message, "count": updated_count}), 200

    except Exception as e:
        logger.error(f"执行批量标记为已订阅时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理请求时发生内部错误"}), 500