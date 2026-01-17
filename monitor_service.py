# monitor_service.py

import os
import time
import logging
import threading
from typing import Optional, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from gevent import spawn_later

import constants
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- ★★★ 全局账本 (核心机制) ★★★ ---
# 记录所有“已检测到但尚未完成处理”的文件路径
PENDING_FILES = set()
PENDING_LOCK = threading.Lock()

class MediaFileHandler(FileSystemEventHandler):
    def __init__(self, extensions, processor):
        self.extensions = [ext.lower() for ext in extensions]
        self.processor = processor

    def _is_valid_media_file(self, file_path: str) -> bool:
        if os.path.isdir(file_path): return False
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.extensions: return False
        filename = os.path.basename(file_path)
        if filename.startswith('.'): return False
        return True

    def on_created(self, event):
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._start_task(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._start_task(event.dest_path)

    def on_deleted(self, event):
        # 删除事件不需要复杂的添油逻辑，直接处理即可
        if not event.is_directory:
            _, ext = os.path.splitext(event.src_path)
            if ext.lower() in self.extensions:
                logger.info(f"  🗑️ [实时监控] 检测到删除: {os.path.basename(event.src_path)}")
                threading.Thread(target=self.processor.process_file_deletion, args=(event.src_path,)).start()

    def _start_task(self, file_path):
        """
        文件入库入口：
        1. 立即在账本上挂号。
        2. 启动独立线程处理该文件。
        """
        with PENDING_LOCK:
            if file_path in PENDING_FILES:
                return # 防止重复触发
            PENDING_FILES.add(file_path)
            logger.info(f"  🔍 [实时监控] 发现新文件 (挂号中): {os.path.basename(file_path)}")
        
        # 启动独立线程处理，互不阻塞
        threading.Thread(target=_worker_logic, args=(self.processor, file_path)).start()

def _worker_logic(processor, file_path):
    """
    独立工作线程逻辑：
    1. 等待文件拷贝完成 (稳定性检查)。
    2. 生成缓存 (不刷新)。
    3. 销号。
    4. 检查是否还有同目录的“战友”。
    5. 决定是否刷新。
    """
    # --- 1. 稳定性检查 ---
    stable_count = 0
    last_size = -1
    for _ in range(60): # 最多等60秒
        try:
            if not os.path.exists(file_path):
                # 文件中途消失，直接销号退出
                with PENDING_LOCK:
                    if file_path in PENDING_FILES: PENDING_FILES.remove(file_path)
                return
            
            size = os.path.getsize(file_path)
            if size > 0 and size == last_size:
                stable_count += 1
            else:
                stable_count = 0
            last_size = size
            
            if stable_count >= 3: break # 连续3秒大小不变，认为拷贝完成
            time.sleep(1)
        except: pass

    # --- 2. 生成缓存 (Skip Refresh = True) ---
    # 我们只让处理器生成数据，不要它去刷新，刷新权在我们手里
    refresh_path = processor.process_file_actively(file_path, skip_refresh=True)

    # --- 3. 销号与决策 (核心) ---
    should_refresh = False
    
    with PENDING_LOCK:
        # A. 销号：我处理完了
        if file_path in PENDING_FILES:
            PENDING_FILES.remove(file_path)
        
        # B. 决策：还有没有同目录的兄弟在账本里？
        if refresh_path:
            # 检查 PENDING_FILES 里是否还有任何文件属于 refresh_path 这个目录
            # 注意：refresh_path 可能是父目录 (电影) 或 爷目录 (剧集)
            # 我们需要判断 pending_file 是否 startswith refresh_path
            
            has_siblings = False
            for pending_file in PENDING_FILES:
                # 规范化路径比较
                if os.path.commonpath([pending_file, refresh_path]) == os.path.normpath(refresh_path):
                    has_siblings = True
                    break
            
            if not has_siblings:
                # 账本里没有同目录的文件了，我是最后一个！
                should_refresh = True
            else:
                logger.info(f"  ⛽ 检测到目录 '{os.path.basename(refresh_path)}' 仍有文件在处理中，推迟刷新...")

    # --- 4. 执行刷新 ---
    if should_refresh and refresh_path:
        # 导入 emby 模块进行刷新 (或者在 processor 里加一个专门的刷新方法，这里直接调 emby 模块也行)
        import handler.emby as emby
        logger.info(f"  🚀 [批量完成] 所有任务结束，统一刷新目录: {refresh_path}")
        emby.refresh_library_by_path(refresh_path, processor.emby_url, processor.emby_api_key)

class MonitorService:
    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        self.observer = None
        self.enabled = self.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        self.paths = self.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        self.extensions = self.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)

    def start(self):
        if not self.enabled or not self.paths: return
        self.observer = Observer()
        handler = MediaFileHandler(self.extensions, self.processor)
        for path in self.paths:
            if os.path.isdir(path):
                self.observer.schedule(handler, path, recursive=True)
        self.observer.start()
        logger.info(f"  👀 实时监控已启动，监听 {len(self.paths)} 个目录。")

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()