# monitor_service.py

import os
import time
import logging
import threading
from typing import List, Optional, Any, Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from gevent import spawn_later

import constants
import config_manager
import handler.emby as emby

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- 全局队列和锁 ---
FILE_EVENT_QUEUE = set() 
QUEUE_LOCK = threading.Lock()
DEBOUNCE_TIMER = None
DEBOUNCE_DELAY = 3 # 防抖延迟秒数

# --- 全局队列抑制标志 ---
IS_PROCESSING_PAUSED = False

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器 (纯净版：仅监控媒体文件的新增和移动)
    """
    def __init__(self, extensions: List[str], exclude_dirs: List[str] = None):
        self.extensions = []
        for ext in extensions:
            if not ext: continue
            clean_ext = ext.strip().lower().replace('*', '')
            if clean_ext:
                if not clean_ext.startswith('.'):
                    clean_ext = '.' + clean_ext
                self.extensions.append(clean_ext)
        
        logger.trace(f"  [实时监控] 已加载监控后缀: {self.extensions}")

    def _is_valid_media_file(self, file_path: str) -> bool:
        if os.path.exists(file_path) and os.path.isdir(file_path): 
            return False
        
        _, ext = os.path.splitext(file_path)
        if ext.lower() not in self.extensions: 
            return False
        
        filename = os.path.basename(file_path)
        if filename.startswith('.'): return False
        if filename.endswith(('.part', '.!qB', '.crdownload', '.tmp', '.aria2')): return False

        return True

    def on_created(self, event):
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._enqueue_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._enqueue_file(event.dest_path)

    def _enqueue_file(self, file_path: str):
        """新增/移动文件入队"""
        enqueue_file_actively(file_path)

def _is_path_excluded(file_path: str, exclude_paths: List[str]) -> bool:
    if not exclude_paths:
        return False
    norm_file = os.path.normpath(file_path).lower()
    for exc in exclude_paths:
        norm_exc = os.path.normpath(exc).lower()
        if norm_file == norm_exc or norm_file.startswith(norm_exc + os.sep):
            return True
    return False

def enqueue_file_actively(file_path: str):
    """主动将文件推入监控队列"""
    global DEBOUNCE_TIMER
    with QUEUE_LOCK:
        if file_path not in FILE_EVENT_QUEUE:
            logger.info(f"  ➜ [主动推送] 文件加入监控队列: {os.path.basename(file_path)}")
        
        FILE_EVENT_QUEUE.add(file_path)
        
        if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
        DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

def process_batch_queue():
    """处理新增/修改队列"""
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.clear()
        return
        
    global DEBOUNCE_TIMER, IS_PROCESSING_PAUSED
    
    if IS_PROCESSING_PAUSED:
        with QUEUE_LOCK:
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(5.0, process_batch_queue)
        return

    with QUEUE_LOCK:
        files_to_process = list(FILE_EVENT_QUEUE)
        FILE_EVENT_QUEUE.clear()
        DEBOUNCE_TIMER = None
    
    if not files_to_process: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    exclude_paths = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, [])

    files_to_scrape = []
    files_to_refresh_only = []

    for file_path in files_to_process:
        if _is_path_excluded(file_path, exclude_paths):
            files_to_refresh_only.append(file_path)
        else:
            files_to_scrape.append(file_path)

    if files_to_scrape:
        grouped_files = {}
        for file_path in files_to_scrape:
            parent_dir = os.path.dirname(file_path)
            if parent_dir not in grouped_files: 
                grouped_files[parent_dir] = []
            grouped_files[parent_dir].append(file_path)

        representative_files = []
        logger.info(f"  ➜ [实时监控] 准备刮削 {len(files_to_scrape)} 个文件，聚合为 {len(grouped_files)} 个任务组。")

        for parent_dir, files in grouped_files.items():
            rep_file = files[0]
            representative_files.append(rep_file)
            folder_name = os.path.basename(parent_dir)
            if len(files) > 1:
                logger.info(f"    ├─ [刮削] 目录 '{folder_name}' 含 {len(files)} 个文件，选取代表: {os.path.basename(rep_file)}")
            else:
                logger.info(f"    ├─ [刮削] 目录 '{folder_name}' 单文件: {os.path.basename(rep_file)}")

        threading.Thread(target=_handle_batch_file_task, args=(processor, representative_files)).start()

    if files_to_refresh_only:
        logger.info(f"  ➜ [实时监控] 发现 {len(files_to_refresh_only)} 个文件命中排除路径，将跳过刮削直接刷新 Emby。")
        threading.Thread(target=_handle_batch_refresh_only_task, args=(files_to_refresh_only,)).start()

def _handle_batch_file_task(processor, file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    processor.process_file_actively_batch(valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    
    config = config_manager.APP_CONFIG
    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    delay_seconds = config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY, 0)

    if not base_url or not api_key:
        return

    if delay_seconds > 0:
        logger.info(f"  ➜ [实时监控-排除路径] 等待 {delay_seconds} 秒后通知 Emby 刷新...")
        time.sleep(delay_seconds)
        if not config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
            return

    logger.info(f"  ➜ [实时监控-排除路径] 正在向 Emby 发送 {len(valid_files)} 个文件的极速入库通知。")
    emby.notify_emby_file_changes(valid_files, base_url, api_key)

def _wait_for_files_stability(file_paths: List[str]) -> List[str]:
    """
    文件稳定性检测 (仅针对媒体文件)
    """
    valid_files = []
    pending_files = {fp: {'last_size': -1, 'stable_count': 0} for fp in file_paths if os.path.exists(fp)}
            
    for _ in range(60):
        if not pending_files: break
            
        for fp in list(pending_files.keys()):
            if not os.path.exists(fp):
                del pending_files[fp]
                continue
                
            try:
                size = os.path.getsize(fp)
                if fp.lower().endswith('.strm') and size > 0:
                    valid_files.append(fp)
                    del pending_files[fp]
                    continue
                
                if size > 0 and size == pending_files[fp]['last_size']:
                    pending_files[fp]['stable_count'] += 1
                else:
                    pending_files[fp]['stable_count'] = 0
                    
                pending_files[fp]['last_size'] = size
                if pending_files[fp]['stable_count'] >= 3:
                    valid_files.append(fp)
                    del pending_files[fp]
            except Exception:
                pass
                
        if pending_files:
            time.sleep(1)
            
    return valid_files

class MonitorService:
    processor_instance = None

    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        MonitorService.processor_instance = processor 
        
        self.observer: Optional[Any] = None
        self.enabled = self.config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False)
        self.paths = self.config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
        self.extensions = self.config.get(constants.CONFIG_OPTION_MONITOR_EXTENSIONS, constants.DEFAULT_MONITOR_EXTENSIONS)
        self.exclude_dirs = self.config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, constants.DEFAULT_MONITOR_EXCLUDE_DIRS)

    def start(self):
        if not self.enabled:
            return

        if not self.paths:
            return

        def _async_start():
            self.observer = Observer()
            event_handler = MediaFileHandler(self.extensions, self.exclude_dirs)

            started_paths = []
            for path in self.paths:
                if os.path.exists(path) and os.path.isdir(path):
                    try:
                        self.observer.schedule(event_handler, path, recursive=True)
                        started_paths.append(path)
                    except Exception as e:
                        logger.error(f"  ➜ 无法监控目录 '{path}': {e}")

            if started_paths:
                self.observer.start()
                logger.info(f"  ➜ [实时监控] 服务已启动，正在监听 {len(started_paths)} 个目录。")

        threading.Thread(target=_async_start, name="MonitorServiceStarter", daemon=True).start()

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()

def pause_queue_processing():
    global IS_PROCESSING_PAUSED
    IS_PROCESSING_PAUSED = True

def resume_queue_processing():
    global IS_PROCESSING_PAUSED, DEBOUNCE_TIMER
    IS_PROCESSING_PAUSED = False
    with QUEUE_LOCK:
        if FILE_EVENT_QUEUE:
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(1, process_batch_queue)