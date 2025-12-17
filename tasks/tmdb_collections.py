# tasks/tmdb_collections.py
# TMDb 原生合集任务模块

import logging
import task_manager
from handler import tmdb_collections

logger = logging.getLogger(__name__)

def task_refresh_collections(processor):
    """
    后台任务：启动 TMDb 合集扫描。
    职责：只负责调用 handler 层的总指挥函数。
    """
    task_name = "刷新 TMDb 合集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (独立任务模块) ---")
    try:
        def progress_callback(percent, message):
            task_manager.update_status_from_thread(percent, message)
        
        # 调用 handler 层逻辑
        tmdb_collections.sync_and_subscribe_native_collections(progress_callback=progress_callback)
        
        task_manager.update_status_from_thread(100, "TMDb 合集扫描与订阅任务完成。")
        logger.info(f"--- '{task_name}' 任务成功完成 ---")
    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")