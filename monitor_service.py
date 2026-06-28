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
MEDIAINFO_DIR_SCAN_LOCK = threading.Lock()
MEDIAINFO_DIR_SCAN_TIMERS = {}
MEDIAINFO_DIR_SCAN_WINDOW_SECONDS = 600
MEDIAINFO_DIR_SCAN_LIMIT = 30
MEDIAINFO_UPLOAD_LOCK = threading.Lock()
MEDIAINFO_UPLOAD_TIMERS = {}
MEDIAINFO_UPLOAD_RECENT = {}
MEDIAINFO_UPLOAD_INFLIGHT = set()
MEDIAINFO_UPLOAD_DEDUPE_SECONDS = 300
MEDIAINFO_UPLOAD_LOG_DEDUPE_SECONDS = 120
DEBOUNCE_DELAY = 3 # 防抖延迟秒数

# --- 全局队列抑制标志 ---
IS_PROCESSING_PAUSED = False

class MediaFileHandler(FileSystemEventHandler):
    """
    文件系统事件处理器 (纯净版：仅监控媒体文件的新增和移动)
    """
    def __init__(self, extensions: List[str], exclude_dirs: List[str] = None):
        self.exclude_dirs = exclude_dirs or []
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
        if str(file_path or '').lower().endswith('-mediainfo.json'):
            return True
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
        if event.is_directory:
            self._handle_mediainfo_dir_update(event.src_path)
            return
        if not event.is_directory and str(event.src_path or '').lower().endswith('-mediainfo.json'):
            self._handle_mediainfo_update(event.src_path)
            return
        if not event.is_directory and self._is_valid_media_file(event.src_path):
            self._enqueue_file(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            self._handle_mediainfo_dir_update(event.src_path)
            return
        if not event.is_directory and str(event.src_path or '').lower().endswith('-mediainfo.json'):
            self._handle_mediainfo_update(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            self._handle_mediainfo_dir_update(event.dest_path)
            return
        if not event.is_directory and str(event.dest_path or '').lower().endswith('-mediainfo.json'):
            self._handle_mediainfo_update(event.dest_path)
            return
        if not event.is_directory and self._is_valid_media_file(event.dest_path):
            self._enqueue_file(event.dest_path)

    def _handle_mediainfo_update(self, file_path: str):
        if not file_path or _is_path_excluded(file_path, self.exclude_dirs):
            return
        try:
            from handler.shared_intro_service import shared_intro_enabled
            if not shared_intro_enabled():
                return
        except Exception:
            return
        norm_path = os.path.normpath(file_path)

        with MEDIAINFO_UPLOAD_LOCK:
            old_timer = MEDIAINFO_UPLOAD_TIMERS.get(norm_path)
            if old_timer:
                try:
                    old_timer.cancel()
                except Exception:
                    pass
            timer_ref = {}
            def _timer_runner():
                with MEDIAINFO_UPLOAD_LOCK:
                    if MEDIAINFO_UPLOAD_TIMERS.get(norm_path) is not timer_ref.get("timer"):
                        return
                    MEDIAINFO_UPLOAD_TIMERS.pop(norm_path, None)
                _run_mediainfo_intro_upload(norm_path)
            timer = threading.Timer(DEBOUNCE_DELAY, _timer_runner)
            timer_ref["timer"] = timer
            timer.daemon = True
            MEDIAINFO_UPLOAD_TIMERS[norm_path] = timer
            timer.start()

    def _handle_mediainfo_dir_update(self, dir_path: str):
        if not dir_path or _is_path_excluded(dir_path, self.exclude_dirs):
            return
        try:
            from handler.shared_intro_service import shared_intro_enabled
            if not shared_intro_enabled():
                return
        except Exception:
            return
        norm_dir = os.path.normpath(dir_path)

        def _runner():
            try:
                now = time.time()
                candidates = []
                if not os.path.isdir(norm_dir):
                    return
                with os.scandir(norm_dir) as it:
                    for entry in it:
                        if not entry.is_file():
                            continue
                        if not entry.name.lower().endswith('-mediainfo.json'):
                            continue
                        try:
                            mtime = entry.stat().st_mtime
                        except Exception:
                            continue
                        if now - mtime <= MEDIAINFO_DIR_SCAN_WINDOW_SECONDS:
                            candidates.append((mtime, entry.path))
                if not candidates:
                    return
                candidates.sort(reverse=True)
                logger.debug(f"  ➜ [共享片头] 目录变化，发现 {len(candidates)} 个最近更新的媒体信息文件。")
                for _mtime, path in candidates[:MEDIAINFO_DIR_SCAN_LIMIT]:
                    self._handle_mediainfo_update(path)
            finally:
                with MEDIAINFO_DIR_SCAN_LOCK:
                    MEDIAINFO_DIR_SCAN_TIMERS.pop(norm_dir, None)

        with MEDIAINFO_DIR_SCAN_LOCK:
            old_timer = MEDIAINFO_DIR_SCAN_TIMERS.get(norm_dir)
            if old_timer:
                try:
                    old_timer.cancel()
                except Exception:
                    pass
            timer = threading.Timer(DEBOUNCE_DELAY, _runner)
            timer.daemon = True
            MEDIAINFO_DIR_SCAN_TIMERS[norm_dir] = timer
            timer.start()

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

def _intro_chapters_key(chapters: Any) -> tuple:
    key = []
    for item in chapters or []:
        if not isinstance(item, dict):
            continue
        try:
            ticks = int(item.get("StartPositionTicks") or 0)
        except Exception:
            ticks = 0
        key.append((str(item.get("MarkerType") or ""), ticks))
    return tuple(sorted(key))

def _shared_intro_skip_reason(reason: str) -> str:
    return {
        "no_intro_chapters": "未检测到片头章节",
        "sha1_not_found": "未找到本地 SHA1 缓存",
        "shared_center_disabled": "共享中心未启用",
        "shared_intro_disabled": "共享片头未启用",
    }.get(str(reason or ""), str(reason or "无需上传"))

def _should_log_mediainfo_intro_state(file_path: str, state_key: tuple, seconds: int = MEDIAINFO_UPLOAD_LOG_DEDUPE_SECONDS) -> bool:
    now = time.time()
    with MEDIAINFO_UPLOAD_LOCK:
        last_key, last_time = MEDIAINFO_UPLOAD_RECENT.get(file_path, (None, 0))
        if last_key == state_key and now - last_time <= seconds:
            return False
        MEDIAINFO_UPLOAD_RECENT[file_path] = (state_key, now)
        return True

def _run_mediainfo_intro_upload(file_path: str):
    basename = os.path.basename(file_path)
    dedupe_key = None
    try:
        from handler.shared_intro_service import (
            _load_json_file,
            extract_intro_chapters,
            sha1_for_mediainfo_path,
            upload_intro_for_mediainfo_path,
        )
        data = _load_json_file(file_path)
        chapters = extract_intro_chapters(data)
        if not chapters:
            if _should_log_mediainfo_intro_state(file_path, ("skip", "no_intro_chapters")):
                logger.debug(f"  ➜ [共享片头] 跳过：{basename}（未检测到片头章节）")
            return
        sha1 = sha1_for_mediainfo_path(file_path)
        if not sha1:
            if _should_log_mediainfo_intro_state(file_path, ("skip", "sha1_not_found")):
                logger.debug(f"  ➜ [共享片头] 跳过：{basename}（未找到本地 SHA1 缓存）")
            return

        chapter_key = _intro_chapters_key(chapters)
        dedupe_key = (sha1, chapter_key)
        now = time.time()
        with MEDIAINFO_UPLOAD_LOCK:
            last_key, last_time = MEDIAINFO_UPLOAD_RECENT.get(file_path, (None, 0))
            if last_key == dedupe_key and now - last_time <= MEDIAINFO_UPLOAD_DEDUPE_SECONDS:
                return
            if dedupe_key in MEDIAINFO_UPLOAD_INFLIGHT:
                return
            MEDIAINFO_UPLOAD_INFLIGHT.add(dedupe_key)

        res = upload_intro_for_mediainfo_path(file_path, reason='monitor_update')

        if res.get("ok"):
            with MEDIAINFO_UPLOAD_LOCK:
                MEDIAINFO_UPLOAD_RECENT[file_path] = (dedupe_key, now)
            logger.info(f"  ➜ [共享片头] 已上传：{basename}（SHA1 {sha1[:12]}，{len(chapters)} 个章节）")
            return

        if res.get("skipped"):
            logger.debug(f"  ➜ [共享片头] 跳过：{basename}（{_shared_intro_skip_reason(res.get('reason'))}）")
            return

        message = res.get("message") or res.get("reason") or "未知错误"
        center = res.get("center")
        if isinstance(center, dict):
            message = center.get("detail") or center.get("message") or message
        logger.warning(f"  ➜ [共享片头] 上传失败：{basename}（{message}）")
    except Exception as e:
        logger.warning(f"  ➜ [共享片头] 上传失败：{basename}（{e}）")
    finally:
        with MEDIAINFO_UPLOAD_LOCK:
            if dedupe_key:
                MEDIAINFO_UPLOAD_INFLIGHT.discard(dedupe_key)

def _is_etk_standard_strm(file_path: str) -> bool:
    if not str(file_path or '').lower().endswith('.strm'):
        return True
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) <= 0:
            return False
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(2048).strip()
        return '/api/p115/play/' in content or '/api/p115/virtual-play/' in content
    except Exception as e:
        logger.warning(f"  ➜ [实时监控] 读取 STRM 失败，已跳过：{os.path.basename(file_path)}，原因：{e}")
        return False

def _filter_etk_standard_files(file_paths: List[str]) -> List[str]:
    valid = []
    for fp in file_paths or []:
        if _is_etk_standard_strm(fp):
            valid.append(fp)
        else:
            logger.warning(f"  ➜ [实时监控] 非 ETK 标准 STRM，已跳过：{os.path.basename(fp)}")
    return valid

def enqueue_file_actively(file_path: str):
    """主动将文件推入监控队列"""
    global DEBOUNCE_TIMER
    if not _is_etk_standard_strm(file_path):
        logger.warning(f"  ➜ [实时监控] 非 ETK 标准 STRM，已跳过：{os.path.basename(file_path)}")
        return

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
    valid_files = _filter_etk_standard_files(valid_files)
    if not valid_files: return
    processor.process_file_actively_batch(valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    valid_files = _wait_for_files_stability(file_paths)
    valid_files = _filter_etk_standard_files(valid_files)
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
