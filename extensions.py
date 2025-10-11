# extensions.py

from flask import session, jsonify
from functools import wraps
from typing import Optional
import threading
import time

# ======================================================================
# 共享装饰器
# ======================================================================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import config_manager # 在函数内部导入，避免循环
        if not config_manager.APP_CONFIG.get("auth_enabled", False) or 'user_id' in session:
            return f(*args, **kwargs)
        return jsonify({"error": "未授权，请先登录"}), 401
    return decorated_function

def task_lock_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import task_manager # 在函数内部导入
        if task_manager.is_task_running():
            return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409
        return f(*args, **kwargs)
    return decorated_function

def processor_ready_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 直接访问本模块下面定义的全局变量
        if not media_processor_instance:
            return jsonify({"error": "核心处理器未就绪。"}), 503
        return f(*args, **kwargs)
    return decorated_function


# ======================================================================
# 共享的全局实例
# ======================================================================
# 这些变量由 web_app.py 在启动时进行初始化和赋值

media_processor_instance: Optional['MediaProcessor'] = None
watchlist_processor_instance: Optional['WatchlistProcessor'] = None
actor_subscription_processor_instance: Optional['ActorSubscriptionProcessor'] = None
EMBY_SERVER_ID: Optional[str] = None
TASK_REGISTRY = {}
# 为了让类型检查器正常工作
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core_processor import MediaProcessor
    from watchlist_processor import WatchlistProcessor
    from actor_subscription_processor import ActorSubscriptionProcessor
    
# ======================================================================
# --- Webhook 递归抑制机制 ---
# 这个字典用来存放系统刚刚通过API更新过的用户ID和时间戳
# 结构: {'user_id': timestamp}
# ======================================================================
SYSTEM_UPDATE_MARKERS = {}
SYSTEM_UPDATE_LOCK = threading.Lock()
# 抑制窗口期（秒），在这个时间内收到的相同用户的 policyupdated Webhook 将被忽略
RECURSION_SUPPRESSION_WINDOW = 10