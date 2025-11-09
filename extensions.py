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
    """
    【本地管理员专用】
    保护那些只能由工具本地管理员访问的路由。
    如果认证功能关闭，则此装饰器无效。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import config_manager # 在函数内部导入，避免循环
        # 如果认证功能未启用，或者 session 中存在本地 user_id，则放行
        if not config_manager.APP_CONFIG.get("auth_enabled", False) or 'user_id' in session:
            return f(*args, **kwargs)
        # ★★★ 优化：返回更明确的错误信息 ★★★
        return jsonify({"status": "error", "message": "需要后台管理员权限"}), 401
    return decorated_function

# ★★★ 新增：智能的、统一的管理员权限装饰器 ★★★
def admin_required(f):
    """
    【V2 - 终极安全版】
    保护所有后台管理 API 的核心装饰器。
    - 它自己负责进行数据库查询，不再依赖任何可能存在问题的上游中间件。
    - 严格遵循“用完即走”的短事务原则，检查完权限立刻释放数据库连接。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import config_manager
        # 导入 user_db，因为我们需要在这里直接调用它
        from database import user_db 
        
        # 1. 如果工具本身的认证功能被禁用了，则直接放行
        if not config_manager.APP_CONFIG.get("auth_enabled", False):
            return f(*args, **kwargs)

        # 2. 检查是否为本地管理员 (这个不涉及数据库，很快)
        if 'user_id' in session:
            return f(*args, **kwargs)

        # 3. 检查是否为 Emby 管理员 (这是核心修改)
        emby_user_id = session.get('emby_user_id')
        if emby_user_id:
            # ★★★ 核心逻辑 ★★★
            # 调用一个独立的、遵循“短事务”原则的函数来检查权限。
            # 这个函数会自己连接数据库、查询、然后立刻断开。
            if user_db.is_user_admin(emby_user_id):
                # 权限检查通过，并且数据库连接已经释放。
                # 现在才调用主业务函数，此时没有任何活动的事务。
                return f(*args, **kwargs)
        
        # 如果以上所有检查都不通过，则拒绝访问
        return jsonify({"status": "error", "message": "需要管理员权限才能执行此操作"}), 403
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

def emby_login_required(f):
    """
    【Emby 用户专用】
    保护那些需要普通 Emby 用户登录后才能访问的路由（如用户中心）。
    它检查的是 'emby_user_id' 这个 session key。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "需要 Emby 用户登录才能访问此资源"}), 401
        # 如果已登录，则正常执行
        return f(*args, **kwargs)
    return decorated_function

def any_login_required(f):
    """
    【通用登录认证】
    保护那些所有类型的登录用户（本地管理员 或 Emby用户）都能访问的路由。
    这是最宽松的登录检查。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 检查是否存在本地管理员的 session key
        is_local_admin_logged_in = 'user_id' in session
        # 检查是否存在 Emby 用户的 session key
        is_emby_user_logged_in = 'emby_user_id' in session

        # 只要其中任意一个存在，就说明有用户登录了，放行！
        if is_local_admin_logged_in or is_emby_user_logged_in:
            return f(*args, **kwargs)
        else:
            # 如果两种 session key 都不存在，则返回未授权
            return jsonify({"status": "error", "message": "需要登录才能访问此资源"}), 401
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