# task_manager.py (V3 - 队列缓冲 & 细粒度锁版)
import threading
import logging
from queue import Queue
from typing import Optional, Callable, Union, Literal

# 导入类型提示
from core_processor import MediaProcessor
from watchlist_processor import WatchlistProcessor
from actor_subscription_processor import ActorSubscriptionProcessor
import extensions

logger = logging.getLogger(__name__)

# 定义处理器类型的字面量
ProcessorType = Literal['media', 'watchlist', 'actor']

# --- 任务状态和控制 ---
background_task_status = {
    "is_running": False,
    "current_action": "无",
    "progress": 0,
    "message": "等待任务",
    "last_action": None
}
# 这个锁现在只用来保护 status 字典的读写，不再锁任务执行过程
task_lock = threading.Lock()

# --- 任务队列和工人线程 ---
task_queue = Queue()
task_worker_thread: Optional[threading.Thread] = None
task_worker_lock = threading.Lock()

def update_status_from_thread(progress: int, message: str):
    """由处理器或任务函数调用，用于更新任务状态。"""
    # 这里加锁是为了防止多线程同时写入状态导致混乱
    with task_lock:
        if progress >= 0:
            background_task_status["progress"] = progress
        background_task_status["message"] = message

def get_task_status() -> dict:
    """获取当前后台任务的状态。"""
    with task_lock:
        return background_task_status.copy()

def is_task_running() -> bool:
    """检查是否有后台任务正在运行。"""
    with task_lock:
        return background_task_status["is_running"]

def _execute_task_wrapper(task_function: Callable, task_name: str, processor: Union[MediaProcessor, WatchlistProcessor, ActorSubscriptionProcessor], *args, **kwargs):
    """
    【V3 新增】任务执行包装器。
    替代了原来的 _execute_task_with_lock。
    核心改进：只在更新状态时加锁，任务执行期间不持有锁，允许新任务入队。
    """
    global background_task_status
    
    # --- 阶段1：加锁，更新状态为“开始” ---
    with task_lock:
        if not processor:
            logger.error(f"任务 '{task_name}' 无法启动：对应的处理器未初始化。")
            return

        processor.clear_stop_signal()
        background_task_status.update({
            "is_running": True, 
            "current_action": task_name, 
            "last_action": task_name,
            "progress": 0, 
            "message": f"{task_name} 初始化..."
        })
    
    # 锁已释放，现在开始执行耗时任务
    logger.info(f"  ➜ 后台任务 '{task_name}' 开始执行")

    task_completed_normally = False
    final_message = "未知结束状态"
    
    try:
        # --- 阶段2：真正执行任务 (无锁状态) ---
        if processor.is_stop_requested():
            raise InterruptedError("任务被取消")

        task_function(processor, *args, **kwargs)
        
        if not processor.is_stop_requested():
            task_completed_normally = True

    except Exception as e:
        logger.error(f"任务 '{task_name}' 执行出错: {e}", exc_info=True)
        final_message = f"出错: {str(e)}"
    
    # --- 阶段3：加锁，更新状态为“结束” ---
    with task_lock:
        current_progress = background_task_status["progress"]

        if processor.is_stop_requested():
            final_message = "任务已成功中断。"
        elif task_completed_normally:
            final_message = "处理完成。"
            current_progress = 100
        
        # 更新最终状态
        if current_progress >= 0:
            background_task_status["progress"] = current_progress
        background_task_status["message"] = final_message
        
        # 标记为不再运行
        background_task_status["is_running"] = False
        background_task_status["current_action"] = "无 (空闲)"
        
        processor.clear_stop_signal()
    
    logger.info(f"  ✅ 后台任务 '{task_name}' 结束，最终状态: {final_message}")

def task_worker_function():
    """
    【V3 优化版】通用工人线程。
    从队列中获取任务并调用 _execute_task_wrapper 执行。
    """
    logger.trace("  ➜ 通用任务线程已启动，等待任务...")
    while True:
        try:
            # 阻塞等待新任务，直到队列中有东西
            task_info = task_queue.get()
            
            if task_info is None:
                logger.info("工人线程收到停止信号，即将退出。")
                break

            task_function, task_name, processor_type, args, kwargs = task_info
            
            # 精确选择处理器
            processor_map = {
                'media': extensions.media_processor_instance,
                'watchlist': extensions.watchlist_processor_instance,
                'actor': extensions.actor_subscription_processor_instance
            }
            
            processor_to_use = processor_map.get(processor_type)
            logger.trace(f"任务 '{task_name}' 请求使用 '{processor_type}' 处理器。")

            if processor_to_use:
                # 调用新的无锁包装器
                _execute_task_wrapper(task_function, task_name, processor_to_use, *args, **kwargs)
            else:
                logger.error(f"任务 '{task_name}' 无法执行：类型为 '{processor_type}' 的处理器未初始化。")

            # 标记该任务完成，让队列计数器减一
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
            # logger.trace("通用任务线程已在运行。") 
            pass

def submit_task(task_function: Callable, task_name: str, processor_type: ProcessorType = 'media', *args, **kwargs) -> bool:
    """
    【V3 优化版】非阻塞提交接口。
    无论当前是否有任务在运行，都将新任务加入队列等待执行。
    """
    from logger_setup import frontend_log_queue # 延迟导入

    # 注意：这里不再检查 is_running，也不再持有 task_lock
    # 实现了“提交即成功”，彻底解决死锁问题

    # 1. 如果队列是空的，说明可能是新的一轮操作，清空一下前端日志看起来更清爽
    #    (可选逻辑，如果觉得日志太乱可以保留)
    if task_queue.empty() and not is_task_running():
        frontend_log_queue.clear()

    # 2. 构造任务包
    task_info = (task_function, task_name, processor_type, args, kwargs)
    
    # 3. 入队 (Queue 是线程安全的)
    task_queue.put(task_info)
    logger.info(f"  ➜ 任务 '{task_name}' 已加入等待队列 (当前队列积压: {task_queue.qsize()})")
    
    # 4. 确保工人线程活着
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