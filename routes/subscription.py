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
    try:
        hdhive_configured = HDHiveClient().ping()
    except Exception:
        hdhive_configured = False
    return jsonify({
        "success": True,
        "mp_configured": bool(mp_url),
        "hdhive_configured": bool(hdhive_configured)
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
    # 面向用户：不再暴露 API Key / 中转地址 / 中转密钥。
    # ETK 后端会自动向项目方统一 Relay 注册实例，并保存 relay_instance_id / relay_instance_secret。
    "relay_base_url": "",
    "relay_instance_id": "",
    "relay_instance_secret": "",
    # 自动签到方式：normal=普通签到，gambler=赌狗签到
    "checkin_mode": "normal",
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

def _has_hdhive_scope(relay_status, scope_name):
    if not relay_status:
        return False

    scopes = relay_status.get("scopes")

    if isinstance(scopes, list):
        return scope_name in scopes

    scope_text = relay_status.get("scope") or ""
    if isinstance(scope_text, str):
        return scope_name in scope_text.split()

    return False

def _get_hdhive_config():
    cfg = settings_db.get_setting("hdhive_config") or {}
    if not isinstance(cfg, dict):
        cfg = {}

    unlock_cfg = cfg.get("unlock_limit") or {}
    filter_cfg = cfg.get("filter") or {}

    return {
        "relay_base_url": cfg.get("relay_base_url") or "",
        "relay_instance_id": cfg.get("relay_instance_id") or "",
        "relay_instance_secret": cfg.get("relay_instance_secret") or "",
        "checkin_mode": cfg.get("checkin_mode") if cfg.get("checkin_mode") in ["normal", "gambler"] else "normal",
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


def _build_hdhive_config_from_request(data):
    old_cfg = _get_hdhive_config()
    return {
        # 这些字段由后端自动注册维护，不由前端填写。
        "relay_base_url": old_cfg.get("relay_base_url") or "",
        "relay_instance_id": old_cfg.get("relay_instance_id") or "",
        "relay_instance_secret": old_cfg.get("relay_instance_secret") or "",
        "checkin_mode": data.get("hdhive_checkin_mode") if data.get("hdhive_checkin_mode") in ["normal", "gambler"] else "normal",
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


@subscription_bp.route('/hdhive/config', methods=['GET', 'POST'])
@admin_required
def handle_hdhive_config():
    if request.method == 'GET':
        cfg = _get_hdhive_config()
        unlock_cfg = cfg.get("unlock_limit") or {}
        filter_cfg = cfg.get("filter") or {}

        client = HDHiveClient()
        authorize_url = client.authorize_url()
        relay_status = client.get_relay_status()

        user_info = None
        usage_today = None
        vip_info = None 

        if relay_status and relay_status.get("has_access_token"):
            user_info = client.get_user_info()

            if _has_hdhive_scope(relay_status, "meta"):
                usage_today = client.get_usage_today() 

            if _has_hdhive_scope(relay_status, "vip"):
                user_level = user_info.get("level") if user_info else "normal"
                # 包含影巢可能返回的各种 VIP 标识
                if user_level in ["vip", "forever_vip", "lifetime_vip", "premium"]:
                    vip_info = client.get_vip_entitlements()

        return jsonify({
            "success": True,
            "authorize_url": authorize_url,
            "relay_status": relay_status,
            "authorized": bool(relay_status and relay_status.get("has_access_token")),

            "hdhive_checkin_mode": cfg.get("checkin_mode", "normal"),
            "unlock_limit_count": unlock_cfg.get("count", 3),
            "unlock_limit_window": unlock_cfg.get("window", 60),

            "hdhive_free_only": filter_cfg.get("free_only", False),
            "hdhive_max_points": filter_cfg.get("max_points", 10),
            "hdhive_max_size_gb": filter_cfg.get("max_size_gb", 120),
            "hdhive_resolution": filter_cfg.get("resolution", "All"),
            "hdhive_zh_sub_only": filter_cfg.get("zh_sub_only", True),
            "hdhive_exclude_iso": filter_cfg.get("exclude_iso", False),

            "user_info": user_info,
            "usage_today": usage_today,
            "vip_info": vip_info
        })

    data = request.json or {}
    cfg = _build_hdhive_config_from_request(data)
    settings_db.save_setting("hdhive_config", cfg)

    client = HDHiveClient()
    authorize_url = client.authorize_url()
    relay_status = client.get_relay_status()

    user_info = None
    usage_today = None
    vip_info = None 
    
    if relay_status and relay_status.get("has_access_token"):
        user_info = client.get_user_info()

        if _has_hdhive_scope(relay_status, "meta"):
            usage_today = client.get_usage_today() 

        if _has_hdhive_scope(relay_status, "vip"):
            user_level = user_info.get("level") if user_info else "normal"
            # 包含影巢可能返回的各种 VIP 标识
            if user_level in ["vip", "forever_vip", "lifetime_vip", "premium"]:
                vip_info = client.get_vip_entitlements()

    return jsonify({
        "success": True,
        "message": "影巢配置保存成功！" if user_info else "筛选配置已保存。若未授权，请点击“前往影巢授权”。",
        "authorize_url": authorize_url,
        "relay_status": relay_status,
        "authorized": bool(relay_status and relay_status.get("has_access_token")),
        "hdhive_checkin_mode": cfg.get("checkin_mode", "normal"),
        "user_info": user_info,
        "usage_today": usage_today,
        "vip_info": vip_info
    })


@subscription_bp.route('/hdhive/authorize_url', methods=['GET'])
@admin_required
def get_hdhive_authorize_url():
    client = HDHiveClient()
    url = client.authorize_url()
    if not url:
        return jsonify({"success": False, "message": "生成影巢授权链接失败，请查看后端日志。"}), 500
    return jsonify({"success": True, "authorize_url": url})


@subscription_bp.route('/hdhive/clear_authorization', methods=['POST'])
@admin_required
def clear_hdhive_authorization():
    """清除当前 ETK 实例在影巢中转服务上的用户授权，保留本地筛选/签到配置。"""
    client = HDHiveClient()
    res = client.clear_authorization()

    if not res.get("success"):
        return jsonify({
            "success": False,
            "message": res.get("message") or "清除影巢授权失败"
        }), 500

    authorize_url = client.authorize_url()
    relay_status = client.get_relay_status()

    return jsonify({
        "success": True,
        "message": "影巢授权已清除，需要使用时请重新授权。",
        "authorize_url": authorize_url,
        "relay_status": relay_status,
        "authorized": bool(relay_status and relay_status.get("has_access_token"))
    })



def _normalize_hdhive_media_type(media_type, item_type=None):
    """影巢 OpenAPI 只接受 movie / tv。
    前端/内部对象可能传 Movie、Series、Season、Episode、tvshow 等，统一归一化。
    """
    raw = str(media_type or item_type or "").strip().lower()
    if raw in {"movie", "movies", "film", "films"}:
        return "movie"
    if raw in {"tv", "series", "season", "episode", "show", "shows", "tvshow", "tvshows", "电视剧", "剧集", "季", "集"}:
        return "tv"
    return "movie" if not raw else "tv"

@subscription_bp.route('/hdhive/resources', methods=['GET'])
@admin_required
def get_hdhive_resources():
    tmdb_id = request.args.get('tmdb_id')
    raw_media_type = request.args.get('media_type')
    season = request.args.get('season')
    media_type = _normalize_hdhive_media_type(raw_media_type)

    if not tmdb_id:
        return jsonify({"success": False, "message": "缺少 TMDB ID"}), 400

    client = HDHiveClient()
    if not client.ping():
        return jsonify({"success": False, "message": "请先完成影巢授权"}), 401

    logger.debug(
        "  ➜ 影巢资源查询: raw_media_type=%s, normalized_media_type=%s, tmdb_id=%s, season=%s",
        raw_media_type, media_type, tmdb_id, season
    )

    resources = client.get_resources(tmdb_id, media_type, target_season=season)
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
    raw_media_type = data.get('media_type')
    media_type = _normalize_hdhive_media_type(raw_media_type)
    title = data.get('title', '未知影视')

    client = HDHiveClient()
    if not client.ping():
        return jsonify({"success": False, "message": "请先完成影巢授权"}), 401

    # task_download_from_hdhive 的第一个参数保留兼容旧函数签名，新版 HDHiveClient 会忽略该参数。
    threading.Thread(
        target=task_download_from_hdhive,
        args=(None, slug, tmdb_id, media_type, title)
    ).start()
    return jsonify({"success": True, "message": "已向 115 发送转存指令，后台正在处理！"})


@subscription_bp.route('/hdhive/checkin', methods=['POST'])
@admin_required
def trigger_hdhive_checkin():
    data = request.json or {}

    # 手动签到接口：如果前端显式传了 is_gambler，就按前端按钮执行；
    # 如果没传，则按配置里的自动签到方式执行。
    if 'is_gambler' in data:
        is_gambler = bool(data.get('is_gambler'))
    else:
        cfg = _get_hdhive_config()
        is_gambler = cfg.get("checkin_mode", "normal") == "gambler"

    client = HDHiveClient()
    if not client.ping():
        return jsonify({"success": False, "message": "请先完成影巢授权"}), 401

    res = client.checkin(is_gambler)

    if res.get("success"):
        res_data = res.get("data", {})
        real_message = res_data.get("message") or res.get("message", "签到请求成功")
        if res_data.get("checked_in") is False:
            return jsonify({"success": False, "message": real_message})
        return jsonify({"success": True, "message": real_message})

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