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
import handler.emby as emby

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

DEBOUNCE_DELAY = 3 # 防抖延迟秒数

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器
    """
    def __init__(self, extensions: List[str], exclude_dirs: List[str] = None):
        # ★★★ 防呆处理：标准化扩展名 ★★★
        # 1. 去除空格、转小写
        # 2. 去除可能误输入的通配符 * (例如 *.mp4 -> .mp4)
        # 3. 补齐开头的 . (例如 mp4 -> .mp4)
        self.extensions = []
        for ext in extensions:
            if not ext: continue
            
            clean_ext = ext.strip().lower().replace('*', '')
            
            if clean_ext:
                if not clean_ext.startswith('.'):
                    clean_ext = '.' + clean_ext
                self.extensions.append(clean_ext)
        
        # 记录一下最终生效的监控后缀，方便调试
        logger.trace(f"  [实时监控] 已加载监控后缀: {self.extensions}")

        # 注意：exclude_dirs 参数在这里不再用于过滤，过滤逻辑已移至 process_batch_queue
        # 这里保留参数是为了兼容调用签名

    def _is_valid_media_file(self, file_path: str) -> bool:
        # 1. 忽略文件夹
        if os.path.exists(file_path) and os.path.isdir(file_path): 
            return False
        
        # 2. 检查扩展名
        _, ext = os.path.splitext(file_path)
        # os.path.splitext 提取的后缀是带点的 (如 .mp4)，所以我们的 self.extensions 也必须带点
        if ext.lower() not in self.extensions: 
            # 调试日志：如果扩展名不匹配，记录一下（仅在调试模式下）
            # logger.trace(f"  [监控忽略] 扩展名不匹配: {os.path.basename(file_path)}")
            return False
        
        filename = os.path.basename(file_path)
        if filename.startswith('.'): return False
        if filename.endswith(('.part', '.!qB', '.crdownload', '.tmp', '.aria2')): return False

        # ★★★ 关键：此处不再进行任何排除目录的检查 ★★★
        # 只要是媒体文件，全部放行进入队列，由后续逻辑决定是“刮削”还是“仅刷新”
        return True

    def on_created(self, event):
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._enqueue_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._enqueue_file(event.dest_path)

    def on_deleted(self, event):
        if event.is_directory:
            return
        
        _, ext = os.path.splitext(event.src_path)
        # 即使是删除事件，也要检查后缀是否在监控列表中，避免误报非媒体文件的删除
        if ext.lower() not in self.extensions:
            return

        self._enqueue_delete(event.src_path)

    def _enqueue_file(self, file_path: str):
        """新增/移动文件入队"""
        global DEBOUNCE_TIMER
        with QUEUE_LOCK:
            if file_path not in FILE_EVENT_QUEUE:
                logger.info(f"  🔍 [实时监控] 文件加入队列: {os.path.basename(file_path)}")
            
            FILE_EVENT_QUEUE.add(file_path)
            
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

    def _enqueue_delete(self, file_path: str):
        """删除文件入队"""
        global DELETE_DEBOUNCE_TIMER
        with DELETE_QUEUE_LOCK:
            if file_path not in DELETE_EVENT_QUEUE:
                logger.info(f"  🗑️ [实时监控] 删除事件入队: {os.path.basename(file_path)}")
            
            DELETE_EVENT_QUEUE.add(file_path)
            
            if DELETE_DEBOUNCE_TIMER: DELETE_DEBOUNCE_TIMER.kill()
            DELETE_DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_delete_batch_queue)

def _is_path_excluded(file_path: str, exclude_paths: List[str]) -> bool:
    """
    检查文件路径是否命中排除规则（严谨的路径匹配）
    """
    if not exclude_paths:
        return False
        
    norm_file = os.path.normpath(file_path).lower()
    
    for exc in exclude_paths:
        norm_exc = os.path.normpath(exc).lower()
        
        # ★★★ 修复：确保是目录层级的匹配，防止 /foo 匹配到 /foobar ★★★
        # 1. 完全相等
        if norm_file == norm_exc:
            return True
        # 2. 是子目录 (以 排除路径 + 分隔符 开头)
        if norm_file.startswith(norm_exc + os.sep):
            return True
            
    return False

def process_batch_queue():
    """
    处理新增/修改队列 (分组优化 + 排除路径分流版)
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

    # ★★★ 分流逻辑 ★★★
    files_to_scrape = []
    files_to_refresh_only = []

    for file_path in files_to_process:
        if _is_path_excluded(file_path, exclude_paths):
            files_to_refresh_only.append(file_path)
        else:
            files_to_scrape.append(file_path)

    # 1. 正常刮削流程
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

    # 2. 仅刷新流程
    if files_to_refresh_only:
        logger.info(f"  🚀 [实时监控] 发现 {len(files_to_refresh_only)} 个文件命中排除路径，将跳过刮削直接刷新 Emby。")
        threading.Thread(target=_handle_batch_refresh_only_task, args=(files_to_refresh_only,)).start()

def process_delete_batch_queue():
    """
    处理删除队列 (批量版 + 排除路径分流版)
    """
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with DELETE_QUEUE_LOCK:
            DELETE_EVENT_QUEUE.clear()
        return
    
    global DELETE_DEBOUNCE_TIMER
    with DELETE_QUEUE_LOCK:
        files = list(DELETE_EVENT_QUEUE)
        DELETE_EVENT_QUEUE.clear()
        DELETE_DEBOUNCE_TIMER = None
    
    if not files: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    exclude_paths = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS, [])
    
    files_to_delete_logic = []
    files_to_refresh_only = []

    for file_path in files:
        if _is_path_excluded(file_path, exclude_paths):
            files_to_refresh_only.append(file_path)
        else:
            files_to_delete_logic.append(file_path)

    # 1. 正常逻辑：走处理器删除流程 (清理DB等)
    if files_to_delete_logic:
        logger.info(f"  🗑️ [实时监控] 聚合处理删除事件: {len(files_to_delete_logic)} 个常规文件")
        threading.Thread(target=processor.process_file_deletion_batch, args=(files_to_delete_logic,)).start()

    # 2. 排除路径逻辑：仅刷新 Emby (移除条目)
    if files_to_refresh_only:
        logger.info(f"  🗑️ [实时监控] 聚合处理删除事件: {len(files_to_refresh_only)} 个排除路径文件 (仅刷新)")
        threading.Thread(target=_handle_batch_delete_refresh_only, args=(files_to_refresh_only,)).start()

def _handle_batch_file_task(processor, file_paths: List[str]):
    """批量处理新增文件任务 (刮削模式)"""
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    processor.process_file_actively_batch(valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    """批量处理仅刷新任务 (新增/修改)"""
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return

    parent_dirs = set()
    for f in valid_files:
        parent_dirs.add(os.path.dirname(f))
    
    _refresh_parent_dirs(parent_dirs, "新增/修改")

def _handle_batch_delete_refresh_only(file_paths: List[str]):
    """
    批量处理仅刷新任务 (删除)
    注意：删除不需要等待文件稳定，因为文件已经没了。
    """
    parent_dirs = set()
    for f in file_paths:
        parent_dirs.add(os.path.dirname(f))
    
    _refresh_parent_dirs(parent_dirs, "删除")

def _refresh_parent_dirs(parent_dirs: Set[str], action_type: str):
    """
    辅助函数：执行目录刷新
    ★★★ 新增：支持延迟刷新逻辑 ★★★
    """
    config = config_manager.APP_CONFIG
    
    # 再次检查开关，防止在延迟等待期间用户关闭了监控
    if not config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        return

    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    
    # 获取延迟时间配置
    delay_seconds = config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY, 0)

    if not base_url or not api_key:
        logger.error(f"  ❌ [实时监控-{action_type}] 无法执行刷新：Emby 配置缺失。")
        return

    # ★★★ 延迟逻辑 ★★★
    if delay_seconds > 0:
        logger.info(f"  ⏳ [实时监控-{action_type}] 命中排除路径，等待 {delay_seconds} 秒后通知 Emby 刷新 (等待其他工具处理)...")
        time.sleep(delay_seconds)
        
        # 等待结束后再次检查开关，如果用户中途关闭了监控，则取消刷新
        if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
            logger.info(f"  🛑 [实时监控] 监控已关闭，取消挂起的刷新任务。")
            return

    logger.info(f"  🔄 [实时监控-{action_type}] 正在通知 Emby 刷新 {len(parent_dirs)} 个排除目录...")
    for folder_path in parent_dirs:
        try:
            emby.refresh_library_by_path(folder_path, base_url, api_key)
            logger.info(f"    └─ 已通知刷新: {folder_path}")
        except Exception as e:
            logger.error(f"    ❌ 刷新目录失败 {folder_path}: {e}")

def _wait_for_files_stability(file_paths: List[str]) -> List[str]:
    """
    辅助函数：等待文件列表中的文件大小不再变化（拷贝完成）
    """
    valid_files = []
    for file_path in file_paths:
        if not os.path.exists(file_path):
            continue
            
        stable_count = 0
        last_size = -1
        is_stable = False
        
        # 最多等待 60秒
        for _ in range(60): 
            try:
                if not os.path.exists(file_path): 
                    break # 文件中途消失
                
                size = os.path.getsize(file_path)
                if size > 0 and size == last_size:
                    stable_count += 1
                else:
                    stable_count = 0
                
                last_size = size
                
                # 连续 3秒 大小不变，认为拷贝完成
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