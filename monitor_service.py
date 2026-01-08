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
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor

logger = logging.getLogger(__name__)

# --- 全局队列和锁 ---
FILE_EVENT_QUEUE = set() # 使用 set 自动去重
QUEUE_LOCK = threading.Lock()
DEBOUNCE_TIMER = None
DEBOUNCE_DELAY = 5 # 防抖延迟秒数

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器
    负责过滤文件类型，并将有效文件加入全局队列
    """
    def __init__(self, extensions: List[str]):
        self.extensions = [ext.lower() for ext in extensions]

    def _is_valid_media_file(self, file_path: str) -> bool:
        # 注意：对于删除事件，文件已不存在，不能用 isdir 判断，只能靠扩展名
        # 所以这个辅助函数主要用于 created/moved
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

    # ★★★ 新增：处理文件删除事件 ★★★
    def on_deleted(self, event):
        if event.is_directory:
            return
        
        # 简单的扩展名检查 (因为文件已删，无法做更多检查)
        _, ext = os.path.splitext(event.src_path)
        if ext.lower() not in self.extensions:
            return

        # 直接调用 processor 处理删除，不走防抖队列
        # 因为删除操作通常是瞬间完成的，且不需要像新增那样等待文件写入
        processor = MonitorService.processor_instance
        if processor:
            # 异步执行，避免阻塞监控线程
            threading.Thread(target=processor.process_file_deletion, args=(event.src_path,)).start()
        else:
            logger.warning("  ⚠️ [实时监控] 检测到文件删除，但处理器未就绪。")

    def _enqueue_file(self, file_path: str):
        """将文件加入队列并重置计时器"""
        global DEBOUNCE_TIMER
        
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.add(file_path)
            logger.debug(f"  🔍 [实时监控] 文件加入队列: {os.path.basename(file_path)} (当前积压: {len(FILE_EVENT_QUEUE)})")
            
            # 重置计时器
            if DEBOUNCE_TIMER:
                DEBOUNCE_TIMER.kill()
            
            DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

# --- 批量处理函数 ---
def process_batch_queue():
    """
    计时器到期后执行的批量处理逻辑
    """
    global DEBOUNCE_TIMER
    
    # 1. 取出队列中的所有文件
    with QUEUE_LOCK:
        files_to_process = list(FILE_EVENT_QUEUE)
        FILE_EVENT_QUEUE.clear()
        DEBOUNCE_TIMER = None
    
    if not files_to_process:
        return

    logger.info(f"  📦 [实时监控] 防抖结束，开始批量处理 {len(files_to_process)} 个文件...")
    
    # 2. 获取处理器实例
    processor = MonitorService.processor_instance
    if not processor:
        logger.error("  ❌ [实时监控] 处理器未初始化，无法处理文件。")
        return

    # 3. 智能分组 (按父目录分组)
    grouped_files = {}
    for file_path in files_to_process:
        parent_dir = os.path.dirname(file_path)
        if parent_dir not in grouped_files:
            grouped_files[parent_dir] = []
        grouped_files[parent_dir].append(file_path)

    # 4. 逐组处理
    for parent_dir, files in grouped_files.items():
        representative_file = files[0]
        logger.info(f"  🚀 [实时监控] 聚合处理: {os.path.basename(parent_dir)} (包含 {len(files)} 个新文件)")
        
        # 启动异步任务处理，避免阻塞
        threading.Thread(target=_handle_single_file_task, args=(processor, representative_file)).start()

def _handle_single_file_task(processor, file_path):
    """
    处理单个文件的包装函数，包含文件就绪检查
    """
    # 等待文件写入完成 (简单的检查)
    stable_count = 0
    last_size = -1
    for _ in range(60): # 最多等 60 秒
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
    """
    监控服务管理器
    """
    # 静态变量，用于给 process_batch_queue 访问
    processor_instance = None

    def __init__(self, config: dict, processor: 'MediaProcessor'):
        self.config = config
        self.processor = processor
        MonitorService.processor_instance = processor # 注入实例
        
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
        # 注意：这里不再传 processor 给 handler，handler 只负责入队
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