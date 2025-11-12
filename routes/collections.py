# routes/collections.py

from flask import Blueprint, request, jsonify
import logging

# 导入需要的模块
from database import collection_db, media_db # ★★★ 引入新的 media_db 模块
from extensions import admin_required, processor_ready_required
from handler import collections as collections_handler # ★★★ 引入新的业务逻辑处理器
from tasks import task_manager # ★★★ 引入任务管理器来触发订阅

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