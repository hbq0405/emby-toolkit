# routes/subscription.py
import logging
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive, filter_hdhive_resources
from handler.tg_userbot import TGUserBotManager
import threading

subscription_bp = Blueprint('subscription_bp', __name__, url_prefix='/api/subscription')
logger = logging.getLogger(__name__)

# ==========================================
# 状态查询 (供前端判断是否显示按钮)
# ==========================================
@subscription_bp.route('/status', methods=['GET'])
def get_subscription_status():
    mp_config = settings_db.get_setting('mp_config') or {}
    mp_url = mp_config.get('moviepilot_url')
    hdhive_config = settings_db.get_setting("hdhive_config") or {}
    hdhive_key = hdhive_config.get("api_key")
    return jsonify({
        "success": True,
        "mp_configured": bool(mp_url),
        "hdhive_configured": bool(hdhive_key)
    })

# ==========================================
# MoviePilot 配置接口
# ==========================================
@subscription_bp.route('/mp/config', methods=['GET'])
@admin_required
def get_mp_config():
    """获取 MoviePilot 配置"""
    cfg = settings_db.get_setting('mp_config') or {}
    # 提供默认值
    default_cfg = {
        'moviepilot_url': '', 'moviepilot_username': '', 'moviepilot_password': '',
        'moviepilot_recognition': False,
        'link_delete_transfer_history': False,
        'link_delete_download_files': False,
        'resubscribe_daily_cap': 10, 'resubscribe_delay_seconds': 2.0
    }
    default_cfg.update(cfg)
    return jsonify({"success": True, "data": default_cfg})

@subscription_bp.route('/mp/config', methods=['POST'])
@admin_required
def save_mp_config():
    """保存 MoviePilot 配置"""
    new_cfg = request.json
    settings_db.save_setting('mp_config', new_cfg)
    
    return jsonify({"success": True, "message": "MoviePilot 配置已保存生效"})

# ==========================================
# 影巢 (HDHive) 接口
# ==========================================
HDHIVE_DEFAULT_CONFIG = {
    # 兼容旧版个人 API Key。第三方应用模式下可以留空。
    "api_key": "",

    # 推荐：VPS 授权中转。
    # relay_base_url 示例：https://hdhive.847977.xyz
    # relay_secret 对应 VPS .env 里的 ETK_SHARED_SECRET。
    "relay_base_url": "",
    "relay_secret": "",

    "unlock_limit": {
        "count": 3,
        "window": 60
    },
    "filter": {
        "free_only": False,
        "max_points": 10,
        "max_size_gb": 120,
        "resolution": "All",
        "zh_sub_only": True,
        "exclude_iso": False
    }
}


def _get_hdhive_config():
    cfg = settings_db.get_setting("hdhive_config") or {}
    if not isinstance(cfg, dict):
        cfg = {}

    unlock_cfg = cfg.get("unlock_limit") or {}
    filter_cfg = cfg.get("filter") or {}

    return {
        "api_key": cfg.get("api_key") or "",
        "relay_base_url": (
            cfg.get("relay_base_url")
            or cfg.get("auth_relay_url")
            or cfg.get("proxy_base_url")
            or ""
        ),
        "relay_secret": (
            cfg.get("relay_secret")
            or cfg.get("auth_relay_secret")
            or cfg.get("proxy_secret")
            or ""
        ),
        "unlock_limit": {
            "count": int(unlock_cfg.get("count", 3)),
            "window": int(unlock_cfg.get("window", 60))
        },
        "filter": {
            "free_only": bool(filter_cfg.get("free_only", False)),
            "max_points": int(filter_cfg.get("max_points", 10)),
            "max_size_gb": float(filter_cfg.get("max_size_gb", 120)),
            "resolution": filter_cfg.get("resolution") or "All",
            "zh_sub_only": bool(filter_cfg.get("zh_sub_only", True)),
            "exclude_iso": bool(filter_cfg.get("exclude_iso", False))
        }
    }


def _is_hdhive_configured(cfg):
    api_key = (cfg.get("api_key") or "").strip()
    relay_base_url = (cfg.get("relay_base_url") or "").strip()
    relay_secret = (cfg.get("relay_secret") or "").strip()
    return bool(api_key or (relay_base_url and relay_secret))


@subscription_bp.route('/hdhive/config', methods=['GET', 'POST'])
@admin_required
def handle_hdhive_config():
    if request.method == 'GET':
        cfg = _get_hdhive_config()
        api_key = cfg.get("api_key") or ""
        relay_base_url = cfg.get("relay_base_url") or ""
        relay_secret = cfg.get("relay_secret") or ""
        unlock_cfg = cfg.get("unlock_limit") or {}
        filter_cfg = cfg.get("filter") or {}

        user_info = None
        quota_info = None
        relay_status = None
        authorize_url = ""

        if _is_hdhive_configured(cfg):
            client = HDHiveClient(api_key)
            authorize_url = client.authorize_url()
            relay_status = client.get_relay_status()
            user_info = client.get_user_info()
            quota_info = client.get_quota()

        return jsonify({
            "success": True,
            "api_key": api_key,

            "relay_base_url": relay_base_url,
            # 不把密钥明文回显给前端，前端只显示是否已配置。
            "relay_secret_configured": bool(relay_secret),
            "relay_secret": "",
            "authorize_url": authorize_url,
            "relay_status": relay_status,

            "unlock_limit_count": unlock_cfg.get("count", 3),
            "unlock_limit_window": unlock_cfg.get("window", 60),

            "hdhive_free_only": filter_cfg.get("free_only", False),
            "hdhive_max_points": filter_cfg.get("max_points", 10),
            "hdhive_max_size_gb": filter_cfg.get("max_size_gb", 120),
            "hdhive_resolution": filter_cfg.get("resolution", "All"),
            "hdhive_zh_sub_only": filter_cfg.get("zh_sub_only", True),
            "hdhive_exclude_iso": filter_cfg.get("exclude_iso", False),

            "user_info": user_info,
            "quota_info": quota_info
        })

    elif request.method == 'POST':
        data = request.json or {}
        old_cfg = _get_hdhive_config()

        # relay_secret 为空时保留旧值，避免每次打开配置页都需要重填。
        incoming_secret = (data.get("relay_secret") or "").strip()
        relay_secret = incoming_secret or (old_cfg.get("relay_secret") or "")

        cfg = {
            "api_key": (data.get("api_key") or "").strip(),
            "relay_base_url": (data.get("relay_base_url") or "").strip().rstrip("/"),
            "relay_secret": relay_secret,
            "unlock_limit": {
                "count": int(data.get("unlock_limit_count") or 3),
                "window": int(data.get("unlock_limit_window") or 60)
            },
            "filter": {
                "free_only": bool(data.get("hdhive_free_only", False)),
                "max_points": int(data.get("hdhive_max_points") or 10),
                "max_size_gb": float(data.get("hdhive_max_size_gb") or 120),
                "resolution": data.get("hdhive_resolution") or "All",
                "zh_sub_only": bool(data.get("hdhive_zh_sub_only", True)),
                "exclude_iso": bool(data.get("hdhive_exclude_iso", False))
            }
        }

        settings_db.save_setting("hdhive_config", cfg)

        user_info = None
        quota_info = None
        relay_status = None
        authorize_url = ""

        if _is_hdhive_configured(cfg):
            client = HDHiveClient(cfg.get("api_key"))
            authorize_url = client.authorize_url()
            relay_status = client.get_relay_status()

            # ping 在 relay 模式下要求已完成授权；未授权时不视作配置保存失败。
            ping_ok = client.ping()
            if client.use_relay and not ping_ok:
                return jsonify({
                    "success": True,
                    "message": "影巢中转配置已保存，但尚未完成用户授权。请点击“前往影巢授权”。",
                    "authorize_url": authorize_url,
                    "relay_status": relay_status,
                    "user_info": None,
                    "quota_info": None
                })

            if not client.use_relay and not ping_ok:
                return jsonify({
                    "success": False,
                    "message": "API Key 无效或网络异常，配置已保存，请检查后重试。"
                })

            user_info = client.get_user_info()
            quota_info = client.get_quota()

        return jsonify({
            "success": True,
            "message": "影巢配置保存成功！",
            "authorize_url": authorize_url,
            "relay_status": relay_status,
            "user_info": user_info,
            "quota_info": quota_info
        })


@subscription_bp.route('/hdhive/resources', methods=['GET'])
@admin_required
def get_hdhive_resources():
    tmdb_id = request.args.get('tmdb_id')
    media_type = request.args.get('media_type')
    season = request.args.get('season')

    hdhive_config = _get_hdhive_config()
    if not _is_hdhive_configured(hdhive_config):
        return jsonify({"success": False, "message": "请先配置影巢授权中转或 API Key"}), 400

    client = HDHiveClient(hdhive_config.get("api_key"))
    resources = client.get_resources(tmdb_id, media_type)
    filtered_resources = filter_hdhive_resources(
        resources,
        target_season=season,
        media_type=media_type
    )

    return jsonify({
        "success": True,
        "data": filtered_resources,
        "total": len(resources),
        "filtered": len(filtered_resources)
    })


@subscription_bp.route('/hdhive/download', methods=['POST'])
@admin_required
def trigger_hdhive_download():
    data = request.json or {}
    slug = data.get('slug')
    tmdb_id = data.get('tmdb_id')
    media_type = data.get('media_type')
    title = data.get('title', '未知影视')

    hdhive_config = _get_hdhive_config()
    if not _is_hdhive_configured(hdhive_config):
        return jsonify({"success": False, "message": "请先配置影巢授权中转或 API Key"}), 400

    api_key = hdhive_config.get("api_key")
    threading.Thread(
        target=task_download_from_hdhive,
        args=(api_key, slug, tmdb_id, media_type, title)
    ).start()
    return jsonify({"success": True, "message": f"已向 115 发送转存指令，后台正在处理！"})


@subscription_bp.route('/hdhive/checkin', methods=['POST'])
@admin_required
def trigger_hdhive_checkin():
    data = request.json or {}
    is_gambler = data.get('is_gambler', False)

    hdhive_config = _get_hdhive_config()
    if not _is_hdhive_configured(hdhive_config):
        return jsonify({"success": False, "message": "请先配置影巢授权中转或 API Key"}), 400

    client = HDHiveClient(hdhive_config.get("api_key"))
    res = client.checkin(is_gambler)

    if res.get("success"):
        res_data = res.get("data", {})
        real_message = res_data.get("message") or res.get("message", "签到请求成功")
        if res_data.get("checked_in") is False:
            return jsonify({"success": False, "message": real_message})
        else:
            return jsonify({"success": True, "message": real_message})
    else:
        return jsonify({"success": False, "message": res.get("message", "签到失败")})

# ==========================================
# TG频道 (Telegram) 接口
# ==========================================
@subscription_bp.route('/tg_userbot/config', methods=['GET'])
@admin_required
def get_tg_config():
    """获取独立的频道监听配置"""
    cfg = settings_db.get_setting('tg_userbot_config') or {}
    # 提供默认值
    default_cfg = {
        'enabled': False, 'api_id': '', 'api_hash': '', 
        'phone': '', 'password': '', 'channels': [], 'monitor_types': ['movie', 'tv'],
        'transfer_modes': ['subscribe'], 'transfer_keywords': [], 'block_keywords': []
    }
    default_cfg.update(cfg)
    return jsonify({"success": True, "data": default_cfg})

@subscription_bp.route('/tg_userbot/config', methods=['POST'])
@admin_required
def save_tg_config():
    """保存独立的频道监听配置，并根据状态控制后台进程"""
    new_cfg = request.json
    settings_db.save_setting('tg_userbot_config', new_cfg)
    
    manager = TGUserBotManager.get_instance()
    if new_cfg.get('enabled'):
        manager.start() # 如果启用了，立刻启动/重启
    else:
        manager.stop()  # 如果关闭了，立刻停止
        
    return jsonify({"success": True, "message": "频道订阅监听配置已保存生效"})

@subscription_bp.route('/tg_userbot/status', methods=['GET'])
@admin_required
def tg_userbot_status():
    manager = TGUserBotManager.get_instance()
    return jsonify({"success": True, "data": manager.get_status()})

@subscription_bp.route('/tg_userbot/send_code', methods=['POST'])
@admin_required
def tg_userbot_send_code():
    try:
        TGUserBotManager.get_instance().send_login_code()
        return jsonify({"success": True, "message": "验证码已发送到您的 Telegram 客户端"})
    except Exception as e:
        logger.error(f"发送验证码失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"发送失败: {str(e)}"})

@subscription_bp.route('/tg_userbot/login', methods=['POST'])
@admin_required
def tg_userbot_login():
    code = request.json.get('code')
    if not code: return jsonify({"success": False, "message": "请输入验证码"})
    try:
        res = TGUserBotManager.get_instance().submit_login_code(code)
        if res.get('success'):
            return jsonify({"success": True, "message": "登录成功！监听服务已启动。"})
        else:
            return jsonify({"success": False, "message": res.get('msg', '登录失败')})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@subscription_bp.route('/tg_userbot/logout', methods=['POST'])
@admin_required
def tg_userbot_logout():
    TGUserBotManager.get_instance().logout()
    return jsonify({"success": True, "message": "已注销并清除凭证"})