# task_manager.py (V2 - 精确调度版)
import threading
import logging
from queue import Queue
from typing import Optional, Callable, Union, Literal

# 导入类型提示，注意使用字符串避免循环导入
from core_processor import MediaProcessor
from watchlist_processor import WatchlistProcessor
from actor_subscription_processor import ActorSubscriptionProcessor
import extensions

logger = logging.getLogger(__name__)

# 定义处理器类型的字面量，提供类型提示和静态检查
ProcessorType = Literal['media', 'watchlist', 'actor']

# --- 任务状态和控制 ---
background_task_status = {
    "is_running": False,
    "current_action": "无",
    "progress": 0,
    "message": "等待任务",
    "last_action": None
}
task_lock = threading.Lock()

# --- 任务队列和工人线程 ---
task_queue = Queue()
task_worker_thread: Optional[threading.Thread] = None
task_worker_lock = threading.Lock()

def update_status_from_thread(progress: int, message: str):
    """由处理器或任务函数调用，用于更新任务状态。"""
    if progress >= 0:
        background_task_status["progress"] = progress
    background_task_status["message"] = message

def get_task_status() -> dict:
    """获取当前后台任务的状态。"""
    return background_task_status.copy()

def is_task_running() -> bool:
    """检查是否有后台任务正在运行。"""
    return task_lock.locked()

def _execute_task_with_lock(task_function: Callable, task_name: str, processor: Union[MediaProcessor, WatchlistProcessor, ActorSubscriptionProcessor], *args, **kwargs):
    """【工人专用】通用后台任务执行器。"""
    global background_task_status
    
    with task_lock:
        if not processor:
            logger.error(f"任务 '{task_name}' 无法启动：对应的处理器未初始化。")
            return

        processor.clear_stop_signal()

        background_task_status.update({
            "is_running": True, "current_action": task_name, "last_action": task_name,
            "progress": 0, "message": f"{task_name} 初始化..."
        })
        logger.info(f"  -> 后台任务 '{task_name}' 开始执行")

        task_completed_normally = False
        try:
            if processor.is_stop_requested():
                raise InterruptedError("任务被取消")

            task_function(processor, *args, **kwargs)
            
            if not processor.is_stop_requested():
                task_completed_normally = True
        finally:
            final_message = "未知结束状态"
            current_progress = background_task_status["progress"]

            if processor.is_stop_requested():
                final_message = "任务已成功中断。"
            elif task_completed_normally:
                final_message = "处理完成。"
                current_progress = 100
            
            update_status_from_thread(current_progress, final_message)
            logger.info(f"  -> ✅ 后台任务 '{task_name}' 结束，最终状态: {final_message}")

            background_task_status.update({
                "is_running": False, "current_action": "无", "progress": 0, "message": "等待任务"
            })
            processor.clear_stop_signal()
            logger.trace(f"后台任务 '{task_name}' 状态已重置。")

def task_worker_function():
    """
    【V2 - 精确调度版】
    通用工人线程，根据提交任务时指定的 processor_type 来精确选择处理器。
    """
    logger.info("通用任务线程已启动，等待任务...")
    while True:
        try:
            task_info = task_queue.get()
            if task_info is None:
                logger.info("工人线程收到停止信号，即将退出。")
                break

            task_function, task_name, processor_type, args, kwargs = task_info
            
            # ★★★ 核心修复：使用精确的、基于类型的调度逻辑 ★★★
            processor_map = {
                'media': extensions.media_processor_instance,
                'watchlist': extensions.watchlist_processor_instance,
                'actor': extensions.actor_subscription_processor_instance
            }
            
            processor_to_use = processor_map.get(processor_type)
            logger.trace(f"任务 '{task_name}' 请求使用 '{processor_type}' 处理器。")

            if not processor_to_use:
                logger.error(f"任务 '{task_name}' 无法执行：类型为 '{processor_type}' 的处理器未初始化或不存在。")
                task_queue.task_done()
                continue

            _execute_task_with_lock(task_function, task_name, processor_to_use, *args, **kwargs)
            task_queue.task_done()
        except Exception as e:
            logger.error(f"通用工人线程发生未知错误: {e}", exc_info=True)

def start_task_worker_if_not_running():
    """安全地启动通用工人线程。"""
    global task_worker_thread
    with task_worker_lock:
        if task_worker_thread is None or not task_worker_thread.is_alive():
            logger.trace("通用任务线程未运行，正在启动...")
            task_worker_thread = threading.Thread(target=task_worker_function, daemon=True)
            task_worker_thread.start()
        else:
            logger.trace("通用任务线程已在运行。")

def submit_task(task_function: Callable, task_name: str, processor_type: ProcessorType = 'media', *args, **kwargs) -> bool:
    """
    【V2 - 公共接口】将一个任务提交到通用队列中。
    新增 processor_type 参数，用于精确指定任务所需的处理器。
    """
    from logger_setup import frontend_log_queue # 延迟导入以避免循环

    with task_lock:
        if background_task_status["is_running"]:
            logger.warning(f"任务 '{task_name}' 提交失败：已有任务正在运行。")
            return False

        frontend_log_queue.clear()
        logger.trace(f"  -> 任务 '{task_name}' 已提交到队列，并已清空前端日志。")
        
        # ★★★ 核心修复：将 processor_type 加入任务信息元组 ★★★
        task_info = (task_function, task_name, processor_type, args, kwargs)
        task_queue.put(task_info)
        start_task_worker_if_not_running()
        return True

def stop_task_worker():
    """【公共接口】停止工人线程，用于应用退出。"""
    global task_worker_thread
    if task_worker_thread and task_worker_thread.is_alive():
        logger.info("正在发送停止信号给任务工人线程...")
        task_queue.put(None)
        task_worker_thread.join(timeout=5)
        if task_worker_thread.is_alive():
            logger.warning("任务工人线程在5秒内未能正常退出。")
        else:
            logger.info("任务工人线程已成功停止。")

def clear_task_queue():
    """【公共接口】清空任务队列，用于应用退出。"""
    if not task_queue.empty():
        logger.info(f"队列中还有 {task_queue.qsize()} 个任务，正在清空...")
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except Queue.Empty:
                break
        logger.info("任务队列已清空。")