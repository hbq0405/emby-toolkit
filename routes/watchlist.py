# routes/watchlist.py

from flask import Blueprint, request, jsonify
import logging
from datetime import datetime, date
# 导入需要的模块

import config_manager
import handler.moviepilot as moviepilot
import task_manager
import extensions
import constants
from extensions import admin_required, task_lock_required
from tasks import task_batch_subscribe_gaps
from database import watchlist_db
from database import settings_db
# 1. 创建追剧列表蓝图
watchlist_bp = Blueprint('watchlist', __name__, url_prefix='/api/watchlist')

logger = logging.getLogger(__name__)

# 2. 使用蓝图定义路由
@watchlist_bp.route('', methods=['GET']) # 注意：这里的路径是空的，因为前缀已经定义
@admin_required
def api_get_watchlist():
    logger.debug("API (Blueprint): 收到获取追剧列表的请求。")
    try:
        items = watchlist_db.get_all_watchlist_items()

        for item in items:
            # 1. 直接重命名字段，将 psycopg2 解析好的对象传递给前端
            #    前端将收到一个名为 'next_episode_to_air' 的对象 (或 null)
            item['next_episode_to_air'] = item.get('next_episode_to_air_json')
            if 'next_episode_to_air_json' in item:
                del item['next_episode_to_air_json']

            # 2. 对缺失信息做同样处理
            #    前端将收到一个名为 'missing_info' 的对象 (或 null)
            item['missing_info'] = item.get('missing_info_json')
            if 'missing_info_json' in item:
                del item['missing_info_json']

            # 3. 格式化日期 (保留原有逻辑)
            for key, value in item.items():
                if isinstance(value, (datetime, date)):
                    item[key] = value.isoformat()
        
        return jsonify(items)
    except Exception as e:
        logger.error(f"获取追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取追剧列表时发生服务器内部错误"}), 500

@watchlist_bp.route('/add', methods=['POST'])
@admin_required
def api_add_to_watchlist():
    data = request.json
    item_id = data.get('item_id')
    tmdb_id = data.get('tmdb_id')
    item_name = data.get('item_name')
    item_type = data.get('item_type')

    if not all([item_id, tmdb_id, item_name, item_type]):
        return jsonify({"error": "缺少必要的项目信息"}), 400
    
    if item_type != 'Series':
        return jsonify({"error": "只能将'剧集'类型添加到追剧列表"}), 400

    logger.info(f"API (Blueprint): 收到手动添加 '{item_name}' 到追剧列表的请求。")
    
    try:
        watchlist_db.add_item_to_watchlist(
            item_id=item_id,
            tmdb_id=tmdb_id,
            item_name=item_name,
            item_type=item_type
        )
        return jsonify({"message": f"《{item_name}》已成功添加到追剧列表！"}), 200
    except Exception as e:
        logger.error(f"手动添加项目到追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在添加时发生内部错误"}), 500

@watchlist_bp.route('/update_status', methods=['POST'])
@admin_required
def api_update_watchlist_status():
    data = request.json
    item_id = data.get('item_id')
    new_status = data.get('new_status')

    if not item_id or new_status not in ['Watching', 'Ended', 'Paused']:
        return jsonify({"error": "请求参数无效"}), 400

    logger.info(f"  ➜ API (Blueprint): 收到请求，将项目 {item_id} 的追剧状态更新为 '{new_status}'。")
    try:
        success = watchlist_db.update_watchlist_item_status(
            item_id=item_id,
            new_status=new_status
        )
        if success:
            return jsonify({"message": "状态更新成功"}), 200
        else:
            return jsonify({"error": "未在追剧列表中找到该项目"}), 404
    except Exception as e:
        logger.error(f"更新追剧状态时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在更新状态时发生内部错误"}), 500

@watchlist_bp.route('/remove/<item_id>', methods=['POST'])
@admin_required
def api_remove_from_watchlist(item_id):
    logger.info(f"  ➜ API (Blueprint): 收到请求，将项目 {item_id} 从追剧列表移除。")
    try:
        success = watchlist_db.remove_item_from_watchlist(
            item_id=item_id
        )
        if success:
            return jsonify({"message": "已从追剧列表移除"}), 200
        else:
            return jsonify({"error": "未在追剧列表中找到该项目"}), 404
    except Exception as e:
        logger.error(f"从追剧列表移除项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "移除项目时发生未知的服务器内部错误"}), 500

@watchlist_bp.route('/refresh/<item_id>', methods=['POST'])
@admin_required
def api_trigger_single_watchlist_refresh(item_id):
    from tasks import task_refresh_single_watchlist_item # 延迟导入任务函数
    logger.trace(f"API (Blueprint): 收到对单个追剧项目 {item_id} 的刷新请求。")
    if not extensions.watchlist_processor_instance:
        return jsonify({"error": "追剧处理模块未就绪"}), 503

    item_name = watchlist_db.get_watchlist_item_name(item_id) or "未知剧集"

    task_manager.submit_task(
        task_refresh_single_watchlist_item,
        f"手动刷新: {item_name}",
        processor_type='watchlist',
        item_id=item_id
        
    )
    
    return jsonify({"message": f"《{item_name}》的刷新任务已在后台启动！"}), 202

# --- 批量强制完结选中的追剧项目 ---
@watchlist_bp.route('/batch_force_end', methods=['POST'])
@admin_required
def api_batch_force_end_watchlist_items():
    """
    【V2】接收前端请求，批量强制完结选中的追剧项目。
    这可以解决因TMDB数据不准确导致已完结剧集被错误复活的问题，但保留了对新一季的检查。
    """
    data = request.json
    item_ids = data.get('item_ids')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400

    logger.info(f"API (Blueprint): 收到对 {len(item_ids)} 个项目的批量强制完结请求。")
    
    try:
        # 调用更新后的 watchlist_db 函数
        updated_count = watchlist_db.batch_force_end_watchlist_items(
            item_ids=item_ids
        )
        
        return jsonify({
            # 【修改】更新返回信息，使其更准确
            "message": f"操作成功！已将 {updated_count} 个项目标记为强制完结。它们不会因集数更新而复活，但若有新一季发布仍会自动恢复追剧。",
            "updated_count": updated_count
        }), 200
    except Exception as e:
        logger.error(f"批量强制完结项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量强制完结项目时发生未知的服务器内部错误"}), 500
    
# ★★★ 批量更新追剧状态的 API (用于“重新追剧”) ★★★
@watchlist_bp.route('/batch_update_status', methods=['POST'])
@admin_required
def api_batch_update_watchlist_status():
    """
    接收前端请求，批量更新选中项目的追剧状态。
    主要用于“已完结”列表中的“重新追剧”功能。
    """
    data = request.json
    item_ids = data.get('item_ids')
    new_status = data.get('new_status')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400
    
    # 增加对 new_status 的校验，确保只接受合法的状态
    if new_status not in ['Watching', 'Paused', 'Completed']:
        return jsonify({"error": f"无效的状态值: {new_status}"}), 400

    logger.info(f"API: 收到对 {len(item_ids)} 个项目的批量状态更新请求，新状态为 '{new_status}'。")
    
    try:
        # 调用 watchlist_db 中我们将要创建的新函数
        updated_count = watchlist_db.batch_update_watchlist_status(
            item_ids=item_ids,
            new_status=new_status
        )
        
        return jsonify({
            "message": f"操作成功！已将 {updated_count} 个项目的状态更新为 '{new_status}'。",
            "updated_count": updated_count
        }), 200
    except Exception as e:
        logger.error(f"批量更新项目状态时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量更新项目状态时发生未知的服务器内部错误"}), 500
    
@watchlist_bp.route('/subscribe/moviepilot/series', methods=['POST'])
@admin_required
def api_subscribe_series_to_moviepilot():
    """
    接收前端请求，将指定的一季剧集订阅到 MoviePilot。
    """
    data = request.json
    tmdb_id = data.get('tmdb_id')
    title = data.get('title')
    season_number = data.get('season_number')
    # 校验输入参数
    if not all([tmdb_id, title, season_number is not None]):
        return jsonify({"error": "请求参数无效，必须提供 tmdb_id, title 和 season_number。"}), 400
    logger.info(f"API: 收到对《{title}》第 {season_number} 季 (TMDb ID: {tmdb_id}) 的 MoviePilot 订阅请求。")

    # --- 配额检查 ---
    current_quota = settings_db.get_subscription_quota()
    if current_quota <= 0:
        logger.warning(f"API: 用户尝试订阅《{title}》第 {season_number} 季，但每日配额已用尽。")
        return jsonify({"error": "今日订阅配额已用尽，请明天再试。"}), 429

    try:
        # 准备需要传递给 handler 的信息
        series_info = {
            "tmdb_id": tmdb_id,
            "item_name": title  # 使用 item_name 以匹配 handler 的兼容性
        }
        
        # 调用 handler 函数执行实际的订阅操作
        success = moviepilot.subscribe_series_to_moviepilot(
            series_info=series_info,
            season_number=season_number,
            config=config_manager.APP_CONFIG
        )
        if success:
            # 订阅成功后扣减配额
            settings_db.decrement_subscription_quota()

            return jsonify({"message": f"《{title}》第 {season_number} 季的订阅任务已成功提交到 MoviePilot！"}), 200
        else:
            return jsonify({"error": "提交订阅到 MoviePilot 失败，请检查 MoviePilot 的日志。"}), 500
    except Exception as e:
        logger.error(f"订阅剧集到 MoviePilot 时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "订阅时发生未知的服务器内部错误。"}), 500
    
# ★★★ 订阅单个“缺集的季” ★★★
@watchlist_bp.route('/subscribe/gap_season', methods=['POST'])
@admin_required
def api_subscribe_gap_season():
    data = request.json
    item_id = data.get('item_id')
    season_number = data.get('season_number')

    if not all([item_id, season_number is not None]):
        return jsonify({"error": "请求参数无效，必须提供 item_id 和 season_number。"}), 400

    # 配额检查
    current_quota = settings_db.get_subscription_quota()
    if current_quota <= 0:
        return jsonify({"error": "今日订阅配额已用尽，请明天再试。"}), 429

    try:
        series_info = watchlist_db.get_watchlist_item_details(item_id) # 假设有这个函数，或直接查询
        if not series_info:
            return jsonify({"error": "未找到指定的剧集"}), 404

        # 核心：根据用户设置决定订阅模式
        config = config_manager.APP_CONFIG
        use_best_version = config.get(constants.CONFIG_OPTION_RESUBSCRIBE_USE_BEST_VERSION, False)
        best_version_param = 1 if use_best_version else None
        
        success = moviepilot.subscribe_series_to_moviepilot(
            series_info=series_info,
            season_number=season_number,
            config=config,
            best_version=best_version_param
        )
        
        if success:
            settings_db.decrement_subscription_quota()
            # 订阅成功后，从数据库标记中移除，防止重复
            watchlist_db.remove_seasons_from_gaps_list(item_id, [season_number])
            return jsonify({"message": "订阅任务已成功提交！"}), 200
        else:
            return jsonify({"error": "提交订阅到 MoviePilot 失败。"}), 500
    except Exception as e:
        logger.error(f"订阅缺集的季时发生错误: {e}", exc_info=True)
        return jsonify({"error": "订阅时发生服务器内部错误。"}), 500

# ★★★ 批量订阅“缺集的季” ★★★
@watchlist_bp.route('/batch_subscribe_gaps', methods=['POST'])
@admin_required
@task_lock_required
def api_batch_subscribe_gaps():
    data = request.json
    item_ids = data.get('item_ids')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个 item_ids 列表。"}), 400

    task_manager.submit_task(
        task_batch_subscribe_gaps,
        f"批量订阅 {len(item_ids)} 个项目的缺集季",
        item_ids=item_ids
    )
    
    return jsonify({"message": f"已在后台启动任务，为 {len(item_ids)} 个项目订阅缺集的季。"}), 202

@watchlist_bp.route('/batch_remove', methods=['POST'])
@admin_required
def api_batch_remove_from_watchlist():
    """
    接收前端请求，批量从追剧列表中移除项目。
    """
    data = request.json
    item_ids = data.get('item_ids')

    if not isinstance(item_ids, list) or not item_ids:
        return jsonify({"error": "请求参数无效：必须提供一个包含项目ID的列表 (item_ids)。"}), 400

    logger.info(f"API: 收到对 {len(item_ids)} 个项目的批量移除请求。")
    
    try:
        removed_count = watchlist_db.batch_remove_from_watchlist(item_ids)
        
        return jsonify({
            "message": f"操作成功！已从追剧列表移除了 {removed_count} 个项目。",
            "removed_count": removed_count
        }), 200
    except Exception as e:
        logger.error(f"批量移除项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "批量移除项目时发生未知的服务器内部错误"}), 500