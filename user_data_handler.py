# user_data_handler.py (V3 - 智能上下文最终版)

import logging
import db_handler
import emby_handler  # ★★★ 1. 导入 emby_handler ★★★
import config_manager # ★★★ 2. 导入 config_manager 以获取配置 ★★★

logger = logging.getLogger(__name__)

def process_user_data_event(webhook_data: dict):
    """
    【专家模块 V3】
    - 新增智能上下文获取：当事件来自一个分集(Episode)时，
      会主动调用Emby API查询其所属的剧集ID(SeriesId)，
      确保写入数据库的数据是完整的。
    - 彻底解决手动标记剧集状态后，筛选不生效的问题。
    """
    try:
        user = webhook_data.get("User", {})
        item = webhook_data.get("Item", {})
        user_data = webhook_data.get("UserData", {})
        if not user_data and item:
            user_data = item.get("UserData", {})

        user_id = user.get("Id")
        item_id = item.get("Id")
        item_type = item.get("Type") # 获取项目类型

        if not all([user_id, item_id]):
            logger.warning("Webhook 用户数据事件缺少 UserId 或 ItemId，已忽略。")
            return

        # ★★★ 3. 智能上下文获取逻辑 ★★★
        series_id_for_db = None
        if item_type == "Episode":
            logger.debug(f"检测到分集事件 for '{item.get('Name')}'，正在查询其所属剧集ID...")
            # 调用我们已有的、可靠的函数来向上查找
            series_id_for_db = emby_handler.get_series_id_from_child_id(
                item_id=item_id,
                base_url=config_manager.APP_CONFIG.get("emby_server_url"),
                api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                user_id=user_id # 使用当前事件的用户ID进行查询
            )
            if series_id_for_db:
                logger.debug(f"  -> 成功找到所属剧集ID: {series_id_for_db}")
            else:
                logger.warning(f"  -> 未能找到分集 {item_id} 所属的剧集ID。")
        # --- ★★★ 核心修复：处理剧集(Series)本身的事件 ★★★ ---
        elif item_type == "Series":
            logger.debug(f"检测到剧集(Series)本身的事件 for '{item.get('Name')}'，将 series_emby_id 设置为其自身的 ID ({item_id})。")
            series_id_for_db = item_id

        # 准备要写入数据库的数据
        update_payload = {
            "series_emby_id": series_id_for_db, # ★★★ 4. 将找到的剧集ID加入Payload ★★★
            "is_favorite": user_data.get("IsFavorite", False),
            "is_played": user_data.get("Played", False),
            "playback_position_ticks": user_data.get("PlaybackPositionTicks", 0)
        }
        
        # 调用 db_handler 执行数据库更新
        db_handler.upsert_user_media_data(user_id, item_id, update_payload)
        
        event_type = webhook_data.get("Event", "未知")
        logger.debug(f"事件 '{event_type}' 已成功处理，用户 '{user.get('Name')}' 对项目 '{item.get('Name')}' 的数据已更新到本地库。")

    except Exception as e:
        logger.error(f"处理用户数据事件时发生严重错误: {e}", exc_info=True)