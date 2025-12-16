# routes/collections.py

from flask import Blueprint, request, jsonify
import logging

# 导入需要的模块
from database import collection_db, media_db 
from extensions import admin_required, processor_ready_required
from handler import collections as collections_handler 
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
            # 4. 可选：清理本地数据库缓存 (如果有的话)
            # collection_db.delete_by_emby_id(emby_collection_id) 
            # 这里我们不做硬性数据库操作，让前端刷新或下次同步自动处理即可
            return jsonify({"message": "合集已成功从 Emby 删除"}), 200
        else:
            return jsonify({"error": "删除合集失败，请检查 Emby 日志"}), 500

    except Exception as e:
        logger.error(f"删除合集时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": f"服务器内部错误: {str(e)}"}), 500