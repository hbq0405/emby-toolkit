# routes/user_data.py

import logging
from flask import Blueprint, request, jsonify
import db_handler
import task_manager
import emby_handler
import extensions
from extensions import login_required, processor_ready_required

logger = logging.getLogger(__name__)

# 1. 创建用户数据蓝图
user_data_bp = Blueprint('user_data', __name__, url_prefix='/api/user_data')

# 2. 定义 Webhook 接收器 API
@user_data_bp.route('/webhook', methods=['POST'])
def emby_webhook_receiver():
    """
    【V2 - 本地化版】
    接收 Emby Webhook，并将播放状态和收藏记录实时更新到本地数据库。
    """
    try:
        data = request.json
        if not data:
            return jsonify({"status": "empty_payload"}), 400

        event_type = data.get("Event")
        logger.info(f"收到Emby Webhook: {event_type}")

        # 我们只关心用户数据的保存事件
        if event_type != "userdata.save":
            return jsonify({"status": "event_ignored"}), 200

        user = data.get("User", {})
        item = data.get("Item", {})
        user_data = data.get("UserData", {})

        user_id = user.get("Id")
        item_id = item.get("Id")

        if not all([user_id, item_id]):
            logger.warning("Webhook 'userdata.save' 缺少 UserId 或 ItemId，已忽略。")
            return jsonify({"status": "missing_ids"}), 400

        # 准备要写入数据库的数据
        update_payload = {
            "is_favorite": user_data.get("IsFavorite", False),
            "is_played": user_data.get("Played", False),
            "playback_position_ticks": user_data.get("PlaybackPositionTicks", 0)
        }
        
        # 调用 db_handler 执行数据库更新
        db_handler.upsert_user_media_data(user_id, item_id, update_payload)
        
        logger.debug(f"成功将用户 '{user.get('Name')}' 对项目 '{item.get('Name')}' 的数据更新到本地库。")
        return jsonify({"status": "success"}), 200

    except Exception as e:
        logger.error(f"处理 Emby Webhook 时发生严重错误: {e}", exc_info=True)
        return jsonify({"status": "internal_server_error"}), 500

# 3. 定义手动触发全量同步的 API
# @user_data_bp.route('/sync', methods=['POST'])
# @login_required
# @processor_ready_required
# def trigger_user_data_sync():
#     """
#     手动触发一个后台任务，对所有用户的播放和收藏状态进行一次全量同步。
#     """
#     try:
#         task_manager.submit_task(
#             task_sync_all_user_data,
#             task_name="全量同步用户数据",
#             processor_type='media' # 复用 media 处理器以获取 Emby 配置
#         )
#         return jsonify({"message": "全量同步用户数据任务已成功提交到后台队列。"}), 202
#     except Exception as e:
#         logger.error(f"提交全量用户数据同步任务时失败: {e}", exc_info=True)
#         return jsonify({"error": "提交任务失败，请检查后端日志。"}), 500