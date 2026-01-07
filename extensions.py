# extensions.py

from flask import session, jsonify
from functools import wraps
from typing import Optional
import threading
import config_manager 

# ======================================================================
# 共享装饰器
# ======================================================================

def admin_required(f):
    """
    【V3 - 去本地化版】
    保护所有后台管理 API 的核心装饰器。
    - 移除了本地管理员 (user_id) 的检查。
    - 仅允许 Emby 管理员访问。
    - 严格遵循“用完即走”的短事务原则，检查完权限立刻释放数据库连接。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 在函数内部导入，避免循环引用
        from database import user_db 
        
        # 1. 如果工具本身的认证功能被禁用了，则直接放行 (开发调试用)
        if not config_manager.APP_CONFIG.get("auth_enabled", False):
            return f(*args, **kwargs)

        # 2. 获取 Emby 用户 ID
        emby_user_id = session.get('emby_user_id')
        
        if emby_user_id:
            # 3. ★★★ 核心逻辑 ★★★
            # 调用数据库检查权限。
            # 注意：我们不信任 session['emby_is_admin']，因为那个状态可能过时。
            # 查库是最安全的，而且 user_db.is_user_admin 是极速查询。
            if user_db.is_user_admin(emby_user_id):
                return f(*args, **kwargs)
        
        # 4. 如果不是 Emby 管理员，拒绝访问
        return jsonify({"status": "error", "message": "需要 Emby 管理员权限才能执行此操作"}), 403
    return decorated_function

def login_required(f):
    """
    【已废弃 - 兼容模式】
    原用于本地管理员认证。现在系统已去本地化。
    为了防止遗漏的引用导致报错，将其逻辑重定向为 admin_required。
    """
    return admin_required(f)

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
    保护那些只要登录就能访问的路由。
    由于去掉了本地用户，现在逻辑等同于 emby_login_required。
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 检查是否存在 Emby 用户的 session key
        if 'emby_user_id' in session:
            return f(*args, **kwargs)
        else:
            return jsonify({"status": "error", "message": "需要登录才能访问此资源"}), 401
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
DELETING_COLLECTIONS = set()
UPDATING_IMAGES = set()
UPDATING_METADATA = set()