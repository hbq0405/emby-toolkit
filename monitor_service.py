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

class EmbyRefreshManager:
    """
    全局刷新蓄水池：
    驻留在 MonitorService 中，负责接收 CoreProcessor 处理完的路径，
    进行二次防抖，直到静默期结束才通知 Emby 刷新。
    """
    def __init__(self, processor, cooldown=5.0):
        self.processor = processor # 需要用到 processor 里的 emby_url 等配置
        self.cooldown = cooldown
        self.pending_paths = set()
        self.timer = None
        self.lock = threading.Lock()

    def add_paths(self, paths: Set[str]):
        """接收一批待刷新路径"""
        if not paths: return
        
        with self.lock:
            count_before = len(self.pending_paths)
            self.pending_paths.update(paths)
            count_after = len(self.pending_paths)
            
            if count_after > count_before:
                logger.info(f"  🌊 [刷新蓄水池] 新增 {count_after - count_before} 个路径，当前积压: {count_after}。倒计时重置为 {self.cooldown}s...")
            else:
                # 路径已存在，但也重置倒计时，因为说明还在写入
                logger.debug(f"  🌊 [刷新蓄水池] 路径已在队列中，重置倒计时...")

            if self.timer:
                self.timer.cancel()
            self.timer = threading.Timer(self.cooldown, self._flush_and_execute)
            self.timer.start()

    def _flush_and_execute(self):
        """执行刷新"""
        paths_to_process = []
        with self.lock:
            paths_to_process = list(self.pending_paths)
            self.pending_paths.clear()
            self.timer = None
        
        if not paths_to_process: return

        logger.info(f"  🚀 [全局刷新] 静默期结束，开始统一刷新 {len(paths_to_process)} 个累积路径...")
        
        # 使用 processor 中的配置
        url = self.processor.emby_url
        key = self.processor.emby_api_key

        unique_anchor_map = {}
        fallback_paths = []

        # 解析 ID (利用 processor 中引用的 emby 模块)
        for folder_path in paths_to_process:
            anchor_id, anchor_name = emby.find_nearest_library_anchor(folder_path, url, key)
            if anchor_id:
                unique_anchor_map[anchor_id] = anchor_name
            else:
                fallback_paths.append(folder_path)

        # 刷新 ID
        if unique_anchor_map:
            logger.info(f"    ➜ 聚合为 {len(unique_anchor_map)} 个 Emby 锚点进行刷新: {list(unique_anchor_map.values())}")
            for anchor_id, anchor_name in unique_anchor_map.items():
                try:
                    emby.refresh_item_by_id(anchor_id, url, key)
                    time.sleep(0.2)
                except Exception as e:
                    logger.error(f"刷新 Emby ID {anchor_id} 失败: {e}")

        # 刷新 路径
        if fallback_paths:
            logger.info(f"    ➜ 对 {len(fallback_paths)} 个无法解析ID的路径执行普通刷新...")
            for path in fallback_paths:
                try:
                    emby.refresh_library_by_path(path, url, key)
                except Exception as e:
                    logger.error(f"刷新路径 {path} 失败: {e}")
        
        logger.info(f"  ✅ [全局刷新] 完成。")

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

def process_batch_queue():
    """
    处理新增/修改队列 (分组优化版)
    """
    global DEBOUNCE_TIMER
    with QUEUE_LOCK:
        files_to_process = list(FILE_EVENT_QUEUE)
        FILE_EVENT_QUEUE.clear()
        DEBOUNCE_TIMER = None
    
    if not files_to_process: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    # 1. 按父目录分组
    grouped_files = {}
    for file_path in files_to_process:
        parent_dir = os.path.dirname(file_path)
        if parent_dir not in grouped_files: 
            grouped_files[parent_dir] = []
        grouped_files[parent_dir].append(file_path)

    # 2. 提取代表文件 (每个目录只取一个)
    representative_files = []
    
    logger.info(f"  🚀 [实时监控] 防抖结束，共检测到 {len(files_to_process)} 个文件，聚合为 {len(grouped_files)} 个任务组。")

    for parent_dir, files in grouped_files.items():
        # 取第一个文件作为代表
        rep_file = files[0]
        representative_files.append(rep_file)
        
        # 打印日志方便调试
        folder_name = os.path.basename(parent_dir)
        if len(files) > 1:
            logger.info(f"    ├─ 目录 '{folder_name}' 含 {len(files)} 个文件，选取 '{os.path.basename(rep_file)}' 为代表。")
        else:
            logger.info(f"    ├─ 目录 '{folder_name}' 单文件: '{os.path.basename(rep_file)}'")

    # 3. 将代表文件列表传给批量处理线程
    threading.Thread(target=_handle_batch_file_task, args=(processor, representative_files)).start()

def process_delete_batch_queue():
    """
    处理删除队列 (批量版)
    """
    global DELETE_DEBOUNCE_TIMER
    with DELETE_QUEUE_LOCK:
        files = list(DELETE_EVENT_QUEUE)
        DELETE_EVENT_QUEUE.clear()
        DELETE_DEBOUNCE_TIMER = None
    
    if not files: return
    
    processor = MonitorService.processor_instance
    if not processor: return

    logger.info(f"  🗑️ [实时监控] 防抖结束，聚合处理删除事件: 共 {len(files)} 个文件")

    # 调用处理器的批量删除接口
    threading.Thread(target=processor.process_file_deletion_batch, args=(files,)).start()

def _handle_batch_file_task(processor, file_paths: List[str]):
    """
    批量处理新增文件任务：
    1. 逐个检查代表文件的稳定性（等待拷贝完成）。
    2. 将所有有效的代表文件传给核心处理器的批量入口。
    """
    valid_files = []
    
    # 1. 检查文件稳定性 (Wait for copy to finish)
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

    if not valid_files:
        return

    # 1. 调用 Processor 处理元数据，并获取返回值
    refresh_paths = processor.process_file_actively_batch(valid_files)

    if refresh_paths and hasattr(processor, 'refresh_manager_ref'):
         processor.refresh_manager_ref.add_paths(refresh_paths)

class MonitorService:
    processor_instance = None

    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        MonitorService.processor_instance = processor 
        self.refresh_manager = EmbyRefreshManager(processor, cooldown=5.0)
        self.processor.refresh_manager_ref = self.refresh_manager
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