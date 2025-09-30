# logger_setup.py
import logging
import sys
from collections import deque
import constants
import os
from concurrent_log_handler import ConcurrentRotatingFileHandler

# --- 定义常量 ---
LOG_FILE_NAME = "app.log"

# ★★★ 新增部分 1: 定义并注册 TRACE 日志级别 ★★★
# 1. 定义一个比 DEBUG (10) 更低的级别数值
TRACE_LEVEL = 5
# 2. 注册级别名称和方法
logging.addLevelName(TRACE_LEVEL, "TRACE")
# 3. 给 Logger 类添加一个便捷方法 trace()，方便使用 logger.trace(...)
#    我们还加入了 isEnabledFor 检查，这是一个好习惯，可以避免在日志级别不够时浪费资源去格式化消息字符串
def trace(self, message, *args, **kws):
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kws)
logging.Logger.trace = trace
# ★★★ 新增部分结束 ★★★

# --- 前端队列和 Handler ---
frontend_log_queue = deque(maxlen=100)

class FrontendQueueHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def emit(self, record):
        try:
            log_entry = self.format(record)
            # ★★★ 新增部分 2: 确保 TRACE 级别的日志不会进入前端 ★★★
            # 前端只应显示 INFO 及以上级别，所以这里加一个判断
            if record.levelno >= logging.INFO:
                frontend_log_queue.append(log_entry)
        except Exception:
            self.handleError(record)

# ★★★ 新增部分 1: 定义 httpx 日志降级过滤器 ★★★
class DowngradeHttpx200Filter(logging.Filter):
    """
    一个将 httpx 库中成功的 "200 OK" 请求日志从 INFO 降级到 DEBUG 的过滤器。
    """
    def filter(self, record):
        # 检查是否是 httpx 的 INFO 级别日志，并且内容是我们想修改的
        if (record.name == 'httpx' and 
            record.levelno == logging.INFO and 
            'HTTP Request:' in record.getMessage() and 
            '"HTTP/1.1 200 OK"' in record.getMessage()):
            
            # 动态修改这条日志的级别
            record.levelname = 'DEBUG'
            record.levelno = logging.DEBUG
            
        # 必须返回 True，否则所有日志都会被这个 filter 拦截掉
        return True

# --- 获取根记录器 ---
logger = logging.getLogger() 
# ★★★ 修改部分 1: 根级别必须是最低的 TRACE，才能捕获到所有日志 ★★★
logger.setLevel(TRACE_LEVEL) 

# --- 检查并清空处理器 ---
if logger.hasHandlers():
    logger.handlers.clear()

# --- 初始化基础 Handler ---

# 1. 控制台 Handler
stream_handler = logging.StreamHandler(sys.stdout)
# ★★★ 修改部分 2: 在 DEBUG_MODE 下，让控制台显示 TRACE 级别的日志 ★★★
stream_handler.setLevel(TRACE_LEVEL if constants.DEBUG_MODE else logging.INFO)
console_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
stream_handler.setFormatter(console_formatter)
logger.addHandler(stream_handler)

# 2. 前端队列 Handler
try:
    frontend_handler = FrontendQueueHandler()
    frontend_handler.setLevel(logging.INFO)
    frontend_formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
    frontend_handler.setFormatter(frontend_formatter)
    logger.addHandler(frontend_handler)
except Exception as e:
    logging.error(f"Failed to add FrontendQueueHandler: {e}", exc_info=True)

# ★★★ 新增部分 2: 获取 httpx logger 并应用过滤器 ★★★
# 获取名为 'httpx' 的 logger
httpx_logger = logging.getLogger('httpx')
# 为它单独添加我们自定义的过滤器
httpx_logger.addFilter(DowngradeHttpx200Filter())
# 注意：不需要设置 httpx_logger 的 level 或 handler，
# 它会把日志传递给根 logger (propagate=True 默认行为)，由根 logger 的 handler 处理。

# --- 专门用于添加文件日志的函数 ---
def add_file_handler(log_directory: str, 
                     log_size_mb: int = constants.DEFAULT_LOG_ROTATION_SIZE_MB, 
                     log_backups: int = constants.DEFAULT_LOG_ROTATION_BACKUPS):
    """
    向根 logger 添加一个线程安全的轮转文件处理器。
    配置值通过参数传入，而不是在此文件中导入配置管理器。
    """
    try:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory, exist_ok=True)
            logging.info(f"日志目录已创建: {log_directory}")

        log_file_path = os.path.join(log_directory, LOG_FILE_NAME)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # ▼▼▼ [ 核心修改 ] 直接使用传入的参数来初始化 Handler ▼▼▼
        file_handler = ConcurrentRotatingFileHandler(
            log_file_path,
            "a",
            log_size_mb * 1024 * 1024, # 使用传入的参数
            log_backups,               # 使用传入的参数
            encoding='utf-8',
            use_gzip=False
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        if not any(isinstance(h, ConcurrentRotatingFileHandler) for h in logger.handlers):
            logger.addHandler(file_handler)
            # 在日志中明确打印出当前生效的配置
            logging.info(f"  -> 文件日志功能已配置。轮转策略: {log_size_mb}MB * {log_backups}个备份。日志路径: {log_file_path}")
        else:
            logging.warning("文件日志处理器已存在，本次不再重复添加。")

    except Exception as e:
        logging.error(f"配置日志文件处理器时发生错误: {e}", exc_info=True)

# 启动时打印一条消息，表示基础 logger 已就绪
logging.info("  -> 基础 Logger (控制台/前端/过滤器) 已初始化。")