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
    
    # 1. 仅在更新状态时加锁 (瞬间完成)
    with task_lock:
        if not processor:
            logger.error(f"任务 '{task_name}' 无法启动：对应的处理器未初始化。")
            return

        processor.clear_stop_signal()
        background_task_status.update({
            "is_running": True, "current_action": task_name, "last_action": task_name,
            "progress": 0, "message": f"{task_name} 初始化..."
        })
        
    logger.info(f"  ➜ 后台任务 '{task_name}' 开始执行")

    task_completed_normally = False
    try:
        if processor.is_stop_requested():
            raise InterruptedError("任务被取消")

        # 2. ★★★ 核心修复：在无锁状态下执行耗时任务！★★★
        # 这样就不会阻塞其他任务（如 Webhook）进入队列了
        task_function(processor, *args, **kwargs)
        
        if not processor.is_stop_requested():
            task_completed_normally = True
    finally:
        # 3. 任务结束后，再次加锁更新状态
        with task_lock:
            final_message = "未知结束状态"
            current_progress = background_task_status["progress"]

            if processor.is_stop_requested():
                final_message = "任务已成功中断。"
            elif task_completed_normally:
                final_message = "处理完成。"
                current_progress = 100
            
            background_task_status.update({
                "is_running": False, "current_action": "无", "progress": current_progress, "message": final_message
            })
            processor.clear_stop_signal()
            logger.trace(f"后台任务 '{task_name}' 状态已重置。")

def task_worker_function():
    """
    【V2 - 精确调度版】
    通用工人线程，根据提交任务时指定的 processor_type 来精确选择处理器。
    """
    logger.trace("  ➜ 通用任务线程已启动，等待任务...")
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
    【V3 - 防抱死公共接口】将一个任务提交到通用队列中。
    """
    from logger_setup import frontend_log_queue 

    # 尝试获取锁，最多等 2 秒
    if not task_lock.acquire(timeout=2.0):
        logger.error(f"任务 '{task_name}' 提交失败：系统底盘锁死！")
        return False

    try:
        # ★★★ 核心修复：允许 Webhook 和 TG 任务排队，拒绝重复的手动任务 ★★★
        is_webhook_or_tg = "webhook" in task_name.lower() or "tg" in task_name.lower()
        
        if background_task_status["is_running"] and not is_webhook_or_tg:
            logger.warning(f"任务 '{task_name}' 提交失败：已有任务正在运行。")
            return False

        # 只有手动触发的任务才清空前端日志，Webhook 默默排队不打扰用户
        if not is_webhook_or_tg:
            frontend_log_queue.clear()
            
        logger.trace(f"  ➜ 任务 '{task_name}' 已提交到队列。")
        
        task_info = (task_function, task_name, processor_type, args, kwargs)
        task_queue.put(task_info)
        start_task_worker_if_not_running()
        return True
    finally:
        task_lock.release()

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

def emergency_stop():
    """
    【V3 - ABS 防抱死紧急刹车系统】
    当常规停止信号失效，任务卡死导致系统瘫痪时，调用此函数强行重置。
    """
    global background_task_status, task_worker_thread

    logger.warning("🚨 触发终极刹车系统！正在强行介入...")

    # 1. 踩下所有处理器的常规刹车踏板
    for proc in [extensions.media_processor_instance,
                 extensions.watchlist_processor_instance,
                 extensions.actor_subscription_processor_instance]:
        if proc:
            proc.signal_stop()

    # 2. 拔掉油门：清空排队的任务
    clear_task_queue()

    # 3. 强行篡改 UI 状态面板
    background_task_status.update({
        "is_running": False,
        "current_action": "无",
        "progress": 0,
        "message": "已强制重置，等待新任务"
    })

    # 4. 核心科技：强行撬开死锁！
    # 如果旧工人卡死了，这把锁会被永远霸占。我们直接把它撬开。
    if task_lock.locked():
        try:
            task_lock.release()
            logger.warning("  ⚠️ 已强行释放被卡死的任务锁！")
        except RuntimeError:
            pass # 锁可能刚好被释放了，忽略报错

    # 5. 弹射座椅：抛弃旧的僵尸线程
    # 将 worker_thread 设为 None，下次提交任务时，系统会自动孵化一个全新的健康工人
    with task_worker_lock:
        if task_worker_thread and task_worker_thread.is_alive():
            logger.warning("  🧟 发现无响应的僵尸线程，已将其抛弃。")
        task_worker_thread = None

    logger.info("  ✅ 紧急制动执行完毕，系统已恢复就绪状态。")

def trigger_115_organize_task():
    """
    【公共接口】触发 115 网盘整理任务。
    通过 Telegram 分享链接后调用此函数来唤醒后台整理任务。
    """
    try:
        # 延迟导入避免循环依赖
        from tasks.core import task_scan_and_organize_115
        
        # 使用 submit_task 提交任务，processor_type 为 'media'
        result = submit_task(
            task_scan_and_organize_115, 
            "115网盘整理(TG触发)", 
            processor_type='media'
        )
        
        if result:
            logger.info("  ✅ [TG交互] 115 整理任务已成功提交到后台队列。")
        else:
            logger.warning("  ⚠️ [TG交互] 115 整理任务提交失败，可能有其他任务正在运行。")
        
        return result
    except Exception as e:
        logger.error(f"  ❌ [TG交互] 触发 115 整理任务时发生错误: {e}", exc_info=True)
        return False
