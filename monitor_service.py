# monitor_service.py

import os
import time
import logging
import threading
from typing import List, Optional, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 导入项目内部模块
import constants
import config_manager
# 为了类型提示，导入 MediaProcessor 类 (运行时不直接实例化)
from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器
    负责过滤文件类型、防抖动，并触发处理流程
    """
    def __init__(self, processor: 'MediaProcessor', extensions: List[str]):
        self.processor = processor
        # 将扩展名统一转为小写，方便比较
        self.extensions = [ext.lower() for ext in extensions]

    def _is_valid_media_file(self, file_path: str) -> bool:
        """检查文件是否为有效的媒体文件"""
        # 1. 忽略目录
        if os.path.isdir(file_path):
            return False
        
        # 2. 检查扩展名
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.extensions:
            return False
        
        # 3. 忽略隐藏文件和临时下载文件
        filename = os.path.basename(file_path)
        if filename.startswith('.'):
            return False
        if filename.endswith(('.part', '.crdownload', '.tmp', '.aria2')):
            return False
            
        return True

    def on_created(self, event):
        """当文件被创建时触发"""
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._process_event(event.src_path, "新建")

    def on_moved(self, event):
        """当文件被移动/重命名时触发"""
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._process_event(event.dest_path, "移动/重命名")

    def _process_event(self, file_path: str, event_type: str):
        """
        处理文件事件的入口
        启动一个新线程来处理，避免阻塞监控主线程
        """
        logger.info(f"  🔍 [实时监控] 检测到{event_type}文件: {file_path}")
        threading.Thread(target=self._handle_file_async, args=(file_path,)).start()

    def _handle_file_async(self, file_path: str):
        """
        异步处理文件：包含防抖动逻辑（等待文件写入完成）
        """
        logger.debug(f"  ⏳ [实时监控] 等待文件写入完成: {os.path.basename(file_path)}")
        
        # --- 防抖动逻辑 ---
        # 策略：每秒检查一次文件大小。
        # 如果文件大小大于0，且连续 5 秒没有变化，则认为文件写入完成（复制/下载结束）。
        stable_count = 0
        last_size = -1
        max_wait_seconds = 300 # 最多等待 5 分钟
        
        for _ in range(max_wait_seconds):
            try:
                if not os.path.exists(file_path):
                    logger.debug(f"  ➜ [实时监控] 文件在处理前已消失: {file_path}")
                    return
                
                current_size = os.path.getsize(file_path)
                
                # 如果文件大小稳定（且不为0）
                if current_size > 0 and current_size == last_size:
                    stable_count += 1
                else:
                    stable_count = 0 # 大小变了，重置计数器
                
                last_size = current_size
                
                # 连续 5 秒稳定，认为就绪
                if stable_count >= 5:
                    break
                
                time.sleep(1)
            except Exception as e:
                logger.warning(f"  ➜ [实时监控] 检查文件大小时出错: {e}")
                time.sleep(1)
        
        # --- 调用核心处理器 ---
        logger.info(f"  🚀 [实时监控] 文件就绪，开始主动处理: {os.path.basename(file_path)}")
        
        # 调用 core_processor.py 中新增的方法
        self.processor.process_file_actively(file_path)


class MonitorService:
    """
    监控服务管理器
    负责启动和停止 Watchdog Observer
    """
    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        self.observer: Optional[Any] = None
        
        # 从配置加载参数
        self.enabled = self.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        self.paths = self.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        self.extensions = self.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)

    def start(self):
        """启动监控服务"""
        if not self.enabled:
            logger.info("  ➜ 实时监控功能未启用。")
            return

        if not self.paths:
            logger.warning("  ➜ 实时监控已启用，但未配置监控目录列表。")
            return

        # 实例化 Watchdog 观察者
        self.observer = Observer()
        event_handler = MediaFileHandler(self.processor, self.extensions)

        started_paths = []
        for path in self.paths:
            # 确保路径存在且是目录
            if os.path.exists(path) and os.path.isdir(path):
                try:
                    # recursive=True 表示递归监控子目录
                    self.observer.schedule(event_handler, path, recursive=True)
                    started_paths.append(path)
                except Exception as e:
                    logger.error(f"  ➜ 无法监控目录 '{path}': {e}")
            else:
                logger.warning(f"  ➜ 监控目录不存在或无效，已跳过: {path}")

        if started_paths:
            self.observer.start()
            logger.info(f"  👀 实时监控服务已启动，正在监听 {len(started_paths)} 个目录: {started_paths}")
        else:
            logger.warning("  ➜ 没有有效的监控目录，实时监控服务未启动。")

    def stop(self):
        """停止监控服务"""
        if self.observer:
            logger.info("  ➜ 正在停止实时监控服务...")
            self.observer.stop()
            self.observer.join()
            logger.info("  ➜ 实时监控服务已停止。")