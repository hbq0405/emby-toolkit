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

# --- 媒体信息更新专用队列 ---
MEDIAINFO_EVENT_QUEUE = set()
MEDIAINFO_QUEUE_LOCK = threading.Lock()
MEDIAINFO_DEBOUNCE_TIMER = None
# --- 全局队列抑制标志 ---
IS_PROCESSING_PAUSED = False
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

    def on_modified(self, event):
        """专门监听 -mediainfo.json 的修改事件"""
        if not event.is_directory and event.src_path.endswith('-mediainfo.json'):
            self._enqueue_mediainfo(event.src_path)

    def _enqueue_file(self, file_path: str):
        """新增/移动文件入队 (被动监听)"""
        enqueue_file_actively(file_path)

    def _enqueue_mediainfo(self, file_path: str):
        """媒体信息入队逻辑 (独立防抖)"""
        global MEDIAINFO_DEBOUNCE_TIMER
        with MEDIAINFO_QUEUE_LOCK:
            if file_path not in MEDIAINFO_EVENT_QUEUE:
                logger.debug(f"  ➜ [实时监控] 媒体信息更新加入队列: {file_path}")
            
            MEDIAINFO_EVENT_QUEUE.add(file_path)
            
            if MEDIAINFO_DEBOUNCE_TIMER: MEDIAINFO_DEBOUNCE_TIMER.kill()
            MEDIAINFO_DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_mediainfo_queue)

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
    """主动将文件推入监控队列 (供内部模块调用，防侧漏)"""
    global DEBOUNCE_TIMER
    with QUEUE_LOCK:
        if file_path not in FILE_EVENT_QUEUE:
            logger.info(f"  ➜ [主动推送] 文件加入监控队列: {os.path.basename(file_path)}")
        
        FILE_EVENT_QUEUE.add(file_path)
        
        if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
        DEBOUNCE_TIMER = spawn_later(DEBOUNCE_DELAY, process_batch_queue)

def process_batch_queue():
    """
    处理新增/修改队列
    """
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with QUEUE_LOCK:
            FILE_EVENT_QUEUE.clear()
        return
        
    global DEBOUNCE_TIMER, IS_PROCESSING_PAUSED
    
    # 如果处于抑制状态，重新定个 5 秒的闹钟，继续憋着
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

def process_mediainfo_queue():
    """处理媒体信息更新队列"""
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
        with MEDIAINFO_QUEUE_LOCK:
            MEDIAINFO_EVENT_QUEUE.clear()
        return
    
    global MEDIAINFO_DEBOUNCE_TIMER
    with MEDIAINFO_QUEUE_LOCK:
        files_to_process = list(MEDIAINFO_EVENT_QUEUE)
        MEDIAINFO_EVENT_QUEUE.clear()
        MEDIAINFO_DEBOUNCE_TIMER = None
    
    if not files_to_process: return
    
    threading.Thread(target=_handle_mediainfo_update_task, args=(files_to_process,)).start()

def _handle_mediainfo_update_task(file_paths: List[str]):
    """处理 -mediainfo.json 的更新，提取 SHA1 并覆盖备份到数据库"""
    # 复用现有的稳定性检测，确保神医插件已经把文件写完了
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return

    import json
    from database.connection import get_db_connection

    for file_path in valid_files:
        try:
            # 1. 读取文件内容，先进行字符串级别的“片头”检测 (最高效)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 如果没有“片头”字样，直接忽略，不消耗性能
            if "IntroStart" not in content:
                logger.trace(f"  ➜ [实时监控] 文件未包含'片头'信息，忽略更新: {os.path.basename(file_path)}")
                continue
                
            # 确认有片头后，再解析 JSON
            raw_info = json.loads(content)
            if not raw_info or not isinstance(raw_info, list):
                continue

            # 2. 寻找对应的 SHA1
            base_path = file_path.replace('-mediainfo.json', '')
            base_name = os.path.basename(base_path)
            
            sha1 = None
            pickcode = None

            # 尝试从同名 STRM 提取 PC 码
            strm_path = base_path + '.strm'
            if os.path.exists(strm_path):
                with open(strm_path, 'r', encoding='utf-8') as f:
                    strm_content = f.read().strip()
                    
                    # 寻找特征锚点
                    marker = '/p115/play/'
                    if marker in strm_content:
                        # 提取锚点之后的内容，并取第一个斜杠前、问号前的部分
                        pickcode = strm_content.split(marker)[-1].split('/')[0].split('?')[0].strip()
                    else:
                        # 兜底旧逻辑
                        pickcode = strm_content.rstrip('/').split('/')[-1].split('?')[0].strip()

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # --- 复用 media_db.py 中的 3 步高精度查找逻辑 ---
                    
                    # 步1: 优先通过 PC 码查 p115_filesystem_cache (最快最准)
                    if pickcode:
                        cursor.execute("SELECT sha1 FROM p115_filesystem_cache WHERE pick_code = %s AND sha1 IS NOT NULL LIMIT 1", (pickcode,))
                        row = cursor.fetchone()
                        if row: sha1 = row['sha1']

                    # 步2: 如果没有，通过 media_metadata 兜底查 PC 码
                    if not sha1 and pickcode:
                        sql = """
                            SELECT m.file_sha1_json, arr.idx
                            FROM media_metadata m,
                                 jsonb_array_elements_text(m.file_pickcode_json) WITH ORDINALITY AS arr(pc, idx)
                            WHERE arr.pc = %s
                            LIMIT 1
                        """
                        cursor.execute(sql, (pickcode,))
                        row = cursor.fetchone()
                        if row:
                            sha1_arr = row['file_sha1_json']
                            idx = row['idx'] - 1 # ORDINALITY 是从 1 开始的
                            if isinstance(sha1_arr, list) and idx < len(sha1_arr):
                                sha1 = sha1_arr[idx]

                    # 步3: 兜底：通过文件名前缀查 SHA1 (适配挂载模式)
                    if not sha1:
                        cursor.execute("SELECT sha1 FROM p115_filesystem_cache WHERE name LIKE %s AND sha1 IS NOT NULL LIMIT 1", (f"{base_name}.%",))
                        row = cursor.fetchone()
                        if row: sha1 = row['sha1']

                    # 3. 覆盖写入指纹库
                    if sha1:
                        cursor.execute("""
                            INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, created_at)
                            VALUES (%s, %s::jsonb, NOW())
                            ON CONFLICT (sha1) DO UPDATE SET 
                                mediainfo_json = EXCLUDED.mediainfo_json,
                                created_at = NOW()
                        """, (sha1, json.dumps(raw_info, ensure_ascii=False)))
                        conn.commit()
                        logger.trace(f"  ➜ [实时监控] 检测到片头更新，已成功备份至数据库: {os.path.basename(file_path)}")
                    else:
                        logger.trace(f"  ➜ [实时监控] 无法匹配到 SHA1，跳过备份: {os.path.basename(file_path)}")

        except Exception as e:
            logger.error(f"  ➜ [实时监控] 处理媒体信息更新失败 {file_path}: {e}")

def _handle_batch_file_task(processor, file_paths: List[str]):
    """
    处理实时监控触发的文件刮削任务。
    现在 `processor.process_file_actively_batch` 内部已经会调用 `emby.notify_emby_file_changes`。
    """
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    processor.process_file_actively_batch(valid_files)

def _handle_batch_refresh_only_task(file_paths: List[str]):
    """
    处理命中排除路径的文件，直接通知 Emby 进行轻量级刷新。
    """
    valid_files = _wait_for_files_stability(file_paths)
    if not valid_files: return
    
    config = config_manager.APP_CONFIG
    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    delay_seconds = config.get(constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY, 0)

    if not base_url or not api_key:
        logger.error(f"  ➜ [实时监控-排除路径] 无法执行刷新：Emby 配置缺失。")
        return

    if delay_seconds > 0:
        logger.info(f"  ➜ [实时监控-排除路径] 命中排除路径，等待 {delay_seconds} 秒后通知 Emby 刷新...")
        time.sleep(delay_seconds)
        # 再次检查监控是否被禁用，防止长时间等待后状态改变
        if not config.get(constants.CONFIG_OPTION_MONITOR_ENABLED, False):
            logger.info("  ➜ [实时监控-排除路径] 监控已禁用，跳过刷新。")
            return

    logger.info(f"  ➜ [实时监控-排除路径] 正在向 Emby 发送 {len(valid_files)} 个文件的极速入库通知 (命中排除路径)。")
    # ★★★ 核心修改：直接调用极速通知接口，传入具体文件路径 ★★★
    emby.notify_emby_file_changes(valid_files, base_url, api_key)
    logger.info(f"  ➜ [实时监控-排除路径] 批量极速通知完成！Emby 将仅针对这些文件进行秒级入库。")

def _wait_for_files_stability(file_paths: List[str]) -> List[str]:
    """
    【极速并发版】文件稳定性检测
    1. 并发检测：所有文件同时倒计时，40个文件也只需要3秒。
    2. STRM 绿色通道：.strm 文本文件写入极快，只要 size > 0 瞬间放行，0 延迟！
    """
    valid_files = []
    
    # 初始化待检测字典 { filepath: {'last_size': -1, 'stable_count': 0} }
    pending_files = {}
    for fp in file_paths:
        if os.path.exists(fp):
            pending_files[fp] = {'last_size': -1, 'stable_count': 0}
            
    # 全局最多等待 60 秒
    for _ in range(60):
        if not pending_files:
            break # 所有文件都已稳定，提前结束
            
        # 遍历当前还在等待的文件 (使用 list 包装以便在循环中删除字典元素)
        for fp in list(pending_files.keys()):
            if not os.path.exists(fp):
                del pending_files[fp]
                continue
                
            try:
                size = os.path.getsize(fp)
                
                # ★★★ 优化 1：STRM 绿色通道 ★★★
                # STRM 文件极小，只要有内容(size>0)说明已经写完，瞬间放行！
                if fp.lower().endswith('.strm') and size > 0:
                    valid_files.append(fp)
                    del pending_files[fp]
                    continue
                
                # ★★★ 优化 2：常规文件的并发检测 ★★★
                if size > 0 and size == pending_files[fp]['last_size']:
                    pending_files[fp]['stable_count'] += 1
                else:
                    pending_files[fp]['stable_count'] = 0
                    
                pending_files[fp]['last_size'] = size
                
                # 连续 3 秒大小不变，视为稳定
                if pending_files[fp]['stable_count'] >= 3:
                    valid_files.append(fp)
                    del pending_files[fp]
                    
            except Exception:
                pass # 忽略读取过程中的权限/占用错误
                
        # 如果还有未稳定的文件，全局休眠 1 秒后继续下一轮检查
        if pending_files:
            time.sleep(1)
            
    # 记录那些等了 60 秒还没写完的死文件
    for fp in pending_files:
        logger.warning(f"  ➜ [实时监控] 文件不稳定或超时，跳过处理: {os.path.basename(fp)}")
        
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

        # ★★★ 核心修改：将耗时的目录遍历和监听注册放到后台线程执行，防止阻塞主程序启动 ★★★
        def _async_start():
            self.observer = Observer()
            event_handler = MediaFileHandler(self.extensions, self.exclude_dirs)

            started_paths = []
            for path in self.paths:
                if os.path.exists(path) and os.path.isdir(path):
                    try:
                        logger.debug(f"  ➜ [实时监控] 正在为目录建立监听树 (若目录较大或为网盘挂载，可能需要一些时间): {path}")
                        # 这里的 recursive=True 会遍历所有子目录，是耗时的元凶
                        self.observer.schedule(event_handler, path, recursive=True)
                        started_paths.append(path)
                    except Exception as e:
                        logger.error(f"  ➜ 无法监控目录 '{path}': {e}")
                else:
                    logger.warning(f"  ➜ 监控目录不存在或无效，已跳过: {path}")

            if started_paths:
                self.observer.start()
                logger.info(f"  ➜ 实时监控服务已启动，正在监听 {len(started_paths)} 个目录: {started_paths}")
            else:
                logger.warning("  ➜ 没有有效的监控目录，实时监控服务未启动。")

        # 启动后台守护线程执行扫描
        threading.Thread(target=_async_start, name="MonitorServiceStarter", daemon=True).start()

    def stop(self):
        if self.observer:
            logger.info("  ➜ 正在停止实时监控服务...")
            self.observer.stop()
            self.observer.join()
            logger.info("  ➜ 实时监控服务已停止。")

def pause_queue_processing():
    """暂停监控队列处理 (进入蓄水池模式)"""
    global IS_PROCESSING_PAUSED
    IS_PROCESSING_PAUSED = True
    logger.info("  ➜ [实时监控] 已开启队列抑制，暂停处理新文件 (等待网盘处理完成)...")

def resume_queue_processing():
    """恢复监控队列处理 (开闸放水)"""
    global IS_PROCESSING_PAUSED, DEBOUNCE_TIMER
    IS_PROCESSING_PAUSED = False
    logger.info("  ➜ [实时监控] 队列抑制已解除，恢复处理。")
    
    # 开闸后，如果池子里有水，立刻触发处理
    with QUEUE_LOCK:
        if FILE_EVENT_QUEUE:
            if DEBOUNCE_TIMER: DEBOUNCE_TIMER.kill()
            DEBOUNCE_TIMER = spawn_later(1, process_batch_queue)