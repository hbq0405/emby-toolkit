# monitor_service.py

import os
import re
import time
import logging
import threading
from typing import List, Optional, Any, Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from gevent import spawn_later

import constants
import config_manager
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- 全局队列和锁 ---
FILE_EVENT_QUEUE = set() 
QUEUE_LOCK = threading.Lock()
DEBOUNCE_TIMER = None
DELETE_EVENT_QUEUE = set()
DELETE_QUEUE_LOCK = threading.Lock()
DELETE_DEBOUNCE_TIMER = None

DEBOUNCE_DELAY = 5 # 防抖延迟秒数

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器
    """
    def __init__(self, extensions: List[str]):
        self.extensions = [ext.lower() for ext in extensions]

    def _is_valid_media_file(self, file_path: str) -> bool:
        if os.path.exists(file_path) and os.path.isdir(file_path): return False
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.extensions: return False
        filename = os.path.basename(file_path)
        if filename.startswith('.'): return False
        if filename.endswith(('.part', '.crdownload', '.tmp', '.aria2')): return False
        return True

    def on_created(self, event):
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._enqueue_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._enqueue_file(event.dest_path)

    # ★★★ 修改：删除事件走专用入队逻辑 ★★★
    def on_deleted(self, event):
        if event.is_directory:
            return
        
        _, ext = os.path.splitext(event.src_path)
        if ext.lower() not in self.extensions:
            return

        self._enqueue_delete(event.src_path)

    def _enqueue_file(self, file_path: str):
        """新增/移动文件入队"""
        global DEBOUNCE_TIMER
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.add(file_path)
            logger.debug(f"  🔍 [实时监控] 文件加入队列: {os.path.basename(file_path)}")
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

    def _enqueue_delete(self, file_path: str):
        global DELETE_DEBOUNCE_TIMER
        with DELETE_QUEUE_LOCK:
            DELETE_EVENT_QUEUE.add(file_path)
            logger.debug(f"  🗑️ [实时监控] 删除事件入队: {os.path.basename(file_path)}")
            if DELETE_DEBOUNCE_TIMER: DELETE_DEBOUNCE_TIMER.kill()
            DELETE_DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_delete_batch_queue)

def process_batch_queue():
    """处理新增/修改队列"""
    global DEBOUNCE_TIMER
    with QUEUE_LOCK:
        files_to_process = list(FILE_EVENT_QUEUE)
        FILE_EVENT_QUEUE.clear()
        DEBOUNCE_TIMER = None
    
    if not files_to_process: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    grouped_files = {}
    for file_path in files_to_process:
        parent_dir = os.path.dirname(file_path)
        folder_name = os.path.basename(parent_dir)
        display_name = folder_name
        if re.match(r'^(Season|S)\s*\d+|Specials', folder_name, re.IGNORECASE):
            grandparent_dir = os.path.dirname(parent_dir)
            series_name = os.path.basename(grandparent_dir)
            display_name = f"{series_name} ({folder_name})"
        
        logger.info(f"  🚀 [实时监控] 聚合处理新增: {display_name} (包含 {len(files)} 个文件)")
        
        threading.Thread(target=_handle_single_file_task, args=(processor, representative_file)).start()

    for parent_dir, files in grouped_files.items():
        representative_file = files[0]
        logger.info(f"  🚀 [实时监控] 聚合处理新增: {os.path.basename(parent_dir)} (包含 {len(files)} 个文件)")
        threading.Thread(target=_handle_single_file_task, args=(processor, representative_file)).start()

def process_delete_batch_queue():
    global DELETE_DEBOUNCE_TIMER
    with DELETE_QUEUE_LOCK:
        files = list(DELETE_EVENT_QUEUE)
        DELETE_EVENT_QUEUE.clear()
        DELETE_DEBOUNCE_TIMER = None
    
    if not files: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    # 按父目录分组去重
    # 逻辑：同一个目录删了10个文件，只需要通知Emby刷新一次这个目录即可
    parent_dirs = {}
    for f in files:
        p_dir = os.path.dirname(f)
        if p_dir not in parent_dirs:
            parent_dirs[p_dir] = f # 记录一个代表文件即可

    logger.info(f"  🗑️ [实时监控] 防抖结束，聚合处理删除事件: 涉及 {len(parent_dirs)} 个目录")

    for p_dir, rep_file in parent_dirs.items():
        # 调用 processor.process_file_deletion
        # 虽然传入的是一个文件路径，但 processor 内部会提取 dirname 并刷新整个目录
        # 这样就实现了“删多文件，只刷一次”的效果
        threading.Thread(target=processor.process_file_deletion, args=(rep_file,)).start()

def _handle_single_file_task(processor, file_path):
    # ... (保持不变) ...
    stable_count = 0
    last_size = -1
    for _ in range(60): 
        try:
            if not os.path.exists(file_path): return
            size = os.path.getsize(file_path)
            if size > 0 and size == last_size:
                stable_count += 1
            else:
                stable_count = 0
            last_size = size
            if stable_count >= 3: break
            time.sleep(1)
        except: pass
        
    processor.process_file_actively(file_path)

class MonitorService:
    # ... (保持不变) ...
    processor_instance = None

    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        MonitorService.processor_instance = processor 
        
        self.observer: Optional[Any] = None
        self.enabled = self.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        self.paths = self.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        self.extensions = self.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)

    def start(self):
        if not self.enabled:
            logger.info("  ➜ 实时监控功能未启用。")
            return

        if not self.paths:
            logger.warning("  ➜ 实时监控已启用，但未配置监控目录列表。")
            return

        self.observer = Observer()
        event_handler = MediaFileHandler(self.extensions)

        started_paths = []
        for path in self.paths:
            if os.path.exists(path) and os.path.isdir(path):
                try:
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
        if self.observer:
            logger.info("  ➜ 正在停止实时监控服务...")
            self.observer.stop()
            self.observer.join()
            logger.info("  ➜ 实时监控服务已停止。")