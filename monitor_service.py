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
from tasks.helpers import convert_strm_content_to_etk

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- 全局队列和锁 ---
FILE_EVENT_QUEUE = set() 
QUEUE_LOCK = threading.Lock()
DEBOUNCE_TIMER = None
DEBOUNCE_DELAY = 3 # 防抖延迟秒数

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器 (纯净版：仅监控新增和修改，忽略删除)
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
        global DEBOUNCE_TIMER
        with QUEUE_LOCK:
            if file_path not in FILE_EVENT_QUEUE:
                logger.info(f"  🔍 [实时监控] 文件加入队列: {file_path}")
            
            FILE_EVENT_QUEUE.add(file_path)
            
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

def _is_path_excluded(file_path: str, exclude_paths: List[str]) -> bool:
    if not exclude_paths:
        return False
    norm_file = os.path.normpath(file_path).lower()
    for exc in exclude_paths:
        norm_exc = os.path.normpath(exc).lower()
        if norm_file == norm_exc or norm_file.startswith(norm_exc + os.sep):
            return True
    return False

def process_batch_queue():
    """
    处理新增/修改队列
    """
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.clear()
        return
    global DEBOUNCE_TIMER
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
        logger.info(f"  🚀 [实时监控] 准备刮削 {len(files_to_scrape)} 个文件，聚合为 {len(grouped_files)} 个任务组。")

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
        logger.info(f"  🚀 [实时监控] 发现 {len(files_to_refresh_only)} 个文件命中排除路径，将跳过刮削直接刷新 Emby。")
        threading.Thread(target=_handle_batch_refresh_only_task, args=(files_to_refresh_only,)).start()

def _process_strm_conversions(file_paths: List[str]) -> List[str]:
    """
    拦截并处理新增/修改的 strm 文件，将其转换为 ETK 标准格式
    """
    config = config_manager.APP_CONFIG
    # 检查转换开关
    convert_enabled = config.get(constants.CONFIG_OPTION_MONITOR_CONVERT_STRM_TO_ETK, False)
    
    if not convert_enabled:
        return file_paths

    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    if not etk_url:
        logger.warning("  ⚠️ [实时监控] 开启了 STRM 转换，但未配置 STRM 链接地址，跳过转换。")
        return file_paths

    for file_path in file_paths:
        if not file_path.lower().endswith('.strm'):
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            needs_update, new_content = convert_strm_content_to_etk(content, etk_url)
            
            if needs_update and new_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                logger.info(f"  🔗 [实时监控] 已将第三方 STRM 链接转换为 ETK 格式: {os.path.basename(file_path)}")
            elif new_content is None:
                logger.warning(f"  ⚠️ [实时监控] 无法识别的 STRM 链接格式，跳过转换: {os.path.basename(file_path)}")
                
        except Exception as e:
            logger.error(f"  ❌ [实时监控] 处理 STRM 文件转换失败 {file_path}: {e}")
            
    return file_paths

def _handle_batch_file_task(processor, file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    valid_files = _process_strm_conversions(valid_files)
    processor.process_file_actively_batch(valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    valid_files = _process_strm_conversions(valid_files)
    parent_dirs = set()
    for f in valid_files:
        parent_dirs.add(os.path.dirname(f))
    
    _refresh_parent_dirs(parent_dirs, "新增/修改")

def _refresh_parent_dirs(parent_dirs: Set[str], action_type: str):
    config = config_manager.APP_CONFIG
    if not config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        return

    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    delay_seconds = config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY, 0)

    if not base_url or not api_key:
        logger.error(f"  ❌ [实时监控-{action_type}] 无法执行刷新：Emby 配置缺失。")
        return

    if delay_seconds > 0:
        logger.info(f"  ⏳ [实时监控-{action_type}] 命中排除路径，等待 {delay_seconds} 秒后通知 Emby 刷新...")
        time.sleep(delay_seconds)
        if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
            return

    logger.info(f"  🔄 [实时监控-{action_type}] 正在通知 Emby 刷新 {len(parent_dirs)} 个排除目录...")
    for folder_path in parent_dirs:
        try:
            emby.refresh_library_by_path(folder_path, base_url, api_key)
            logger.info(f"    └─ 已通知刷新: {folder_path}")
        except Exception as e:
            logger.error(f"    ❌ 刷新目录失败 {folder_path}: {e}")

def _wait_for_files_stability(file_paths: List[str]) -> List[str]:
    valid_files = []
    for file_path in file_paths:
        if not os.path.exists(file_path):
            continue
            
        stable_count = 0
        last_size = -1
        is_stable = False
        
        for _ in range(60): 
            try:
                if not os.path.exists(file_path): 
                    break 
                
                size = os.path.getsize(file_path)
                if size > 0 and size == last_size:
                    stable_count += 1
                else:
                    stable_count = 0
                
                last_size = size
                
                if stable_count >= 3: 
                    is_stable = True
                    break
                
                time.sleep(1)
            except: 
                pass
        
        if is_stable:
            valid_files.append(file_path)
        else:
            logger.warning(f"  ⚠️ [实时监控] 文件不稳定或超时，跳过处理: {os.path.basename(file_path)}")
    
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
            logger.info("  ➜ 实时监控功能未启用。")
            return

        if not self.paths:
            logger.warning("  ➜ 实时监控已启用，但未配置监控目录列表。")
            return

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