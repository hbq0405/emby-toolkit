# routes/tmdb_collections.py

from flask import Blueprint, request, jsonify
import logging
from gevent import spawn_later
# 导入需要的模块
from database import custom_collection_db, tmdb_collection_db, settings_db
from extensions import admin_required, DELETING_COLLECTIONS
from handler import tmdb_collections as collections_handler 
import config_manager
import constants
from handler import emby

# 1. 创建电影合集蓝图
collections_bp = Blueprint('collections', __name__, url_prefix='/api/collections')

logger = logging.getLogger(__name__)

# ======================================================================
# 读取操作 (Read Operations) - 负责动态组装数据
# ======================================================================

@collections_bp.route('/status', methods=['GET'])
@admin_required
def api_get_collections_status():
    """
    【V3 - 新架构核心】获取所有原生合集的完整状态。
    此端点现在会调用业务逻辑层来动态组装数据，而不是直接返回数据库内容。
    """
    try:
        # ★★★ 核心修正: 调用专门为前端组装数据的 handler 函数 ★★★
        # 这个函数只读取数据并进行处理，速度快，且返回前端需要的数据结构。
        final_results = collections_handler.assemble_all_collection_details()
        return jsonify(final_results)
    except Exception as e:
        logger.error(f"组装原生合集状态时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "读取合集时发生服务器内部错误"}), 500

# ======================================================================
# 写入操作 (Write Operations) - 负责触发媒体状态变更
# ======================================================================

@collections_bp.route('/subscribe_missing', methods=['POST'])
@admin_required
def api_subscribe_missing_movies():
    """
    【V3 - 新架构核心】一键订阅所有原生合集中的缺失电影。
    """
    logger.info("API: 收到一键订阅所有原生合集缺失电影的请求。")
    try:
        # ★★★ 核心修改: 调用 handler 函数来执行订阅，并返回结果 ★★★
        result = collections_handler.subscribe_all_missing_in_native_collections()
        
        message = f"操作完成！成功将 {result['subscribed_count']} 部电影加入订阅队列。"
        if result['skipped_count'] > 0:
            message += f" 因未发行或已订阅等原因跳过了 {result['skipped_count']} 部。"
        if result['quota_exceeded']:
            message += " 每日订阅配额已用尽，部分订阅可能未完成。"

        return jsonify({"message": message, "count": result['subscribed_count']}), 200

    except Exception as e:
        logger.error(f"执行一键订阅时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在处理一键订阅时发生内部错误"}), 500
    
# ======================================================================
# ★★★ 删除合集路由 ★★★
# ======================================================================
@collections_bp.route('/<emby_collection_id>', methods=['DELETE'])
@admin_required
def api_delete_collection(emby_collection_id):
    """
    删除指定的 Emby 合集。
    逻辑：先清空合集内的所有媒体项 -> 再删除合集条目本身。
    """
    logger.info(f"API: 收到删除 Emby 合集请求 (ID: {emby_collection_id})")
    
    try:
        # 1. 获取配置
        app_config = config_manager.APP_CONFIG
        base_url = app_config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        api_key = app_config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        user_id = app_config.get(constants.CONFIG_OPTION_EMBY_USER_ID)

        if not all([base_url, api_key, user_id]):
            return jsonify({"error": "Emby 配置不完整，无法执行删除操作"}), 500
        DELETING_COLLECTIONS.add(emby_collection_id)
        def _clear_flag():
            DELETING_COLLECTIONS.discard(emby_collection_id)
        spawn_later(10, _clear_flag)
        # 2. 第一步：清空合集 (移除所有成员)
        # 这一步是为了防止 Emby 只是删除了合集壳子但没解绑关系，或者删除失败
        logger.info(f"  ➜ [删除合集] 步骤1: 正在清空合集 {emby_collection_id} 的成员...")
        empty_success = emby.empty_collection_in_emby(emby_collection_id, base_url, api_key, user_id)
        
        if not empty_success:
            logger.warning(f"  ➜ [删除合集] 清空合集成员失败，但将尝试强制删除合集条目。")

        # 3. 第二步：删除合集条目本身
        logger.info(f"  ➜ [删除合集] 步骤2: 正在删除合集条目 {emby_collection_id}...")
        delete_success = emby.delete_item(emby_collection_id, base_url, api_key, user_id)

        if delete_success:
            # 4. 清理本地数据库缓存 
            tmdb_collection_db.delete_native_collection_by_emby_id(emby_collection_id)
            return jsonify({"message": "合集已成功从 Emby 删除"}), 200
        else:
            return jsonify({"error": "删除合集失败，请检查 Emby 日志"}), 500

    except Exception as e:
        logger.error(f"删除合集时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": f"服务器内部错误: {str(e)}"}), 500
    
# ======================================================================
# ★★★ 新增：合集模块专属设置接口 ★★★
# ======================================================================
@collections_bp.route('/settings', methods=['GET'])
@admin_required
def api_get_collection_settings():
    """获取合集模块的设置 (结构化配置)"""
    try:
        # 1. 读取大配置对象，如果不存在则给默认值
        config = settings_db.get_setting('native_collections_config') or {}
        
        # 2. 确保返回给前端的数据包含默认字段 (防止前端报错)
        default_config = {
            "auto_complete_enabled": False,
            # 未来可以在这里加更多默认值，例如:
            # "exclude_genres": [],
            # "min_rating": 0
        }
        
        # 合并默认值 (数据库里的值覆盖默认值)
        final_config = {**default_config, **config}
        
        return jsonify(final_config)
    except Exception as e:
        logger.error(f"获取合集设置失败: {e}")
        return jsonify({"error": "获取设置失败"}), 500

@collections_bp.route('/settings', methods=['POST'])
@admin_required
def api_save_collection_settings():
    """保存合集模块的设置 (支持增量更新)"""
    try:
        new_data = request.json
        if not isinstance(new_data, dict):
            return jsonify({"error": "无效的数据格式"}), 400

        # 1. 先读取旧配置
        current_config = settings_db.get_setting('native_collections_config') or {}
        
        # 2. 更新字段 (增量更新，保留旧配置中未修改的字段)
        current_config.update(new_data)
        
        # 3. 保存回数据库
        settings_db.save_setting('native_collections_config', current_config)
        
        logger.info(f"API: 原生合集配置已更新: {current_config}")
            
        return jsonify({"message": "设置已保存"}), 200
    except Exception as e:
        logger.error(f"保存合集设置失败: {e}")
        return jsonify({"error": "保存设置失败"}), 500