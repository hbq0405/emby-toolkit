# routes/subscription.py
import logging
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive, filter_hdhive_resources
from handler.tg_userbot import TGUserBotManager, tg_task_queue
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

    tg_cfg = settings_db.get_setting('tg_userbot_config') or {}
    tg_userbot_configured = bool(
        tg_cfg.get('enabled') and tg_cfg.get('api_id') and tg_cfg.get('api_hash') and tg_cfg.get('channels')
    )

    return jsonify({
        "success": True,
        "mp_configured": bool(mp_url),
        "hdhive_configured": bool(hdhive_configured),
        "tg_userbot_configured": tg_userbot_configured,
        "cloud_search_configured": bool(hdhive_configured or tg_userbot_configured)
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


def _safe_int(value, default=0, min_value=None, max_value=None):
    try:
        number = int(value)
    except Exception:
        number = default
    if min_value is not None:
        number = max(number, min_value)
    if max_value is not None:
        number = min(number, max_value)
    return number


def _cloud_resource_key(resource):
    if not isinstance(resource, dict):
        return ""
    for key in ("slug", "target_link", "magnet_url", "message_link"):
        value = resource.get(key)
        if value:
            return f"{key}:{value}"
    if resource.get("source_username") and resource.get("message_id"):
        return f"channel:{resource.get('source_username')}:{resource.get('message_id')}"
    return ""


def _normalize_hdhive_resource(resource):
    item = dict(resource or {})
    item["source_type"] = "hdhive"
    item["source_name"] = "影巢"
    item["_cloud_source"] = "hdhive"
    if item.get("slug"):
        item["unique_id"] = f"hdhive:{item.get('slug')}"
    return item


def _normalize_channel_resource(resource):
    item = dict(resource or {})
    item["source_type"] = "channel"
    item["source_name"] = item.get("source_channel") or "TG频道"
    item["_cloud_source"] = "channel"
    item["already_owned"] = False
    item["unlock_points"] = 0
    if item.get("source_username") and item.get("message_id"):
        item["unique_id"] = f"channel:{item.get('source_username')}:{item.get('message_id')}"
    elif item.get("target_link"):
        item["unique_id"] = f"channel:{item.get('target_link')}"
    elif item.get("magnet_url"):
        item["unique_id"] = f"channel:{item.get('magnet_url')[:80]}"
    return item


def _build_cloud_extra_queries(title, year=None, season=None):
    title = str(title or "").strip()
    year = str(year or "").strip()
    season = str(season or "").strip()
    queries = []
    if title and year:
        queries.append(f"{title} {year}")
    if title and season:
        try:
            s = int(season)
            queries.extend([f"{title} S{s:02d}", f"{title} 第{s}季"])
        except Exception:
            pass
    return queries


@subscription_bp.route('/cloud/resources', methods=['GET'])
@admin_required
def get_cloud_resources():
    """云资源搜索：合并影巢 OpenAPI 与已配置 TG 频道历史消息搜索。"""
    tmdb_id = request.args.get('tmdb_id')
    raw_media_type = request.args.get('media_type')
    season = request.args.get('season')
    title = (request.args.get('title') or request.args.get('query') or '').strip()
    year = (request.args.get('year') or '').strip()
    media_type = _normalize_hdhive_media_type(raw_media_type)

    collect_limit = _safe_int(request.args.get('limit'), default=50, min_value=1, max_value=100)
    hdhive_limit = _safe_int(request.args.get('hdhive_limit'), default=collect_limit, min_value=1, max_value=100)
    channel_limit = _safe_int(request.args.get('channel_limit'), default=collect_limit, min_value=1, max_value=100)

    if not tmdb_id and not title:
        return jsonify({"success": False, "message": "缺少 TMDB ID 或搜索标题"}), 400

    warnings = []
    hdhive_total = 0
    hdhive_filtered = 0
    channel_total = 0
    resources = []
    seen = set()

    # 1. 影巢资源。云搜索属于手动搜索场景，剧集默认不按季过滤，避免用户误以为搜不到资源。
    if tmdb_id:
        try:
            client = HDHiveClient()
            if client.ping():
                hdhive_target_season = None if media_type == 'tv' else season
                raw_resources = client.get_resources(tmdb_id, media_type, target_season=hdhive_target_season)
                hdhive_total = len(raw_resources or [])

                if media_type == 'tv':
                    shown_hdhive = list(raw_resources or [])
                else:
                    shown_hdhive = filter_hdhive_resources(
                        raw_resources,
                        target_season=season,
                        media_type=media_type
                    )
                hdhive_filtered = len(shown_hdhive or [])

                for item in (shown_hdhive or [])[:hdhive_limit]:
                    normalized = _normalize_hdhive_resource(item)
                    key = _cloud_resource_key(normalized)
                    if key and key in seen:
                        continue
                    if key:
                        seen.add(key)
                    resources.append(normalized)
            else:
                warnings.append('影巢未授权或连接不可用，已跳过影巢搜索。')
        except Exception as e:
            logger.error(f"  ➜ 云资源搜索：影巢查询失败: {e}", exc_info=True)
            warnings.append(f"影巢搜索失败：{e}")

    # 2. TG 频道历史资源。只搜索已配置监听列表中的频道。
    try:
        manager = TGUserBotManager.get_instance()
        if not hasattr(manager, 'search_channel_resources'):
            warnings.append('当前 handler/tg_userbot.py 尚未包含频道历史搜索能力，请替换新版 tg_userbot.py。')
        elif title:
            tg_res = manager.search_channel_resources(
                query=title,
                media_type=media_type,
                tmdb_id=tmdb_id,
                year=year,
                limit=channel_limit,
                extra_queries=_build_cloud_extra_queries(title, year=year, season=season),
                timeout=35,
                include_tmdb_query=False,
                strict_title_match=True,
            )

            if not tg_res.get('ok') and tg_res.get('error'):
                warnings.append(tg_res.get('error'))

            for err in tg_res.get('errors') or []:
                warnings.append(f"频道搜索提示：{err}")

            channel_items = tg_res.get('results') or []
            channel_total = len(channel_items)
            for item in channel_items:
                normalized = _normalize_channel_resource(item)
                key = _cloud_resource_key(normalized)
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                resources.append(normalized)
        else:
            warnings.append('未提供标题，已跳过频道历史搜索。')
    except Exception as e:
        logger.error(f"  ➜ 云资源搜索：频道查询失败: {e}", exc_info=True)
        warnings.append(f"频道搜索失败：{e}")

    return jsonify({
        "success": True,
        "data": resources[:collect_limit],
        "total": len(resources),
        "stats": {
            "hdhive_total": hdhive_total,
            "hdhive_filtered": hdhive_filtered,
            "channel_total": channel_total,
            "shown": min(len(resources), collect_limit),
            "limit": collect_limit,
            "warnings": warnings
        }
    })


@subscription_bp.route('/cloud/download', methods=['POST'])
@admin_required
def trigger_cloud_download():
    """云资源下载/转存：影巢资源走影巢解锁；频道资源复用频道监听队列。"""
    data = request.json or {}
    resource = data.get('resource') or {}
    source_type = (data.get('source_type') or resource.get('source_type') or resource.get('_cloud_source') or '').strip().lower()

    slug = data.get('slug') or resource.get('slug')
    tmdb_id = data.get('tmdb_id') or resource.get('tmdb_id')
    raw_media_type = data.get('media_type') or resource.get('media_type') or resource.get('item_type')
    media_type = _normalize_hdhive_media_type(raw_media_type)
    title = data.get('title') or resource.get('title') or resource.get('name') or '未知影视'

    if source_type in {'hdhive', 'hive', '影巢'} or slug:
        if not slug:
            return jsonify({"success": False, "message": "缺少影巢资源 slug"}), 400

        client = HDHiveClient()
        if not client.ping():
            return jsonify({"success": False, "message": "请先完成影巢授权"}), 401

        threading.Thread(
            target=task_download_from_hdhive,
            args=(None, slug, tmdb_id, media_type, title)
        ).start()
        return jsonify({"success": True, "message": "已向 115 发送影巢资源转存指令，后台正在处理！"})

    if source_type in {'channel', 'tg', 'telegram', '频道'} or resource.get('target_link') or resource.get('magnet_url'):
        target_link = resource.get('target_link')
        magnet_url = resource.get('magnet_url')
        if not target_link and not magnet_url:
            return jsonify({"success": False, "message": "频道资源缺少可转存链接"}), 400

        tg_task_queue.put({
            "type": "channel_resource_complex",
            "tmdb_id": tmdb_id,
            "title": title,
            "year": data.get('year') or resource.get('year'),
            "item_type": media_type,
            "target_link": target_link,
            "magnet_url": magnet_url,
            "receive_code": resource.get('receive_code') or '',
            "season_number": resource.get('season_number'),
            "episode_number": resource.get('episode_number'),
            "is_pack": bool(resource.get('is_pack')),
            "is_completed_pack": bool(resource.get('is_completed_pack')),
            # 手动云搜索选择的资源应直接转存，不再要求已订阅/追剧。
            "is_brainless": True,
            "is_keyword_matched": True,
            "is_subscribe": False
        })
        return jsonify({"success": True, "message": "已推送频道资源转存任务，后台正在处理！"})

    return jsonify({"success": False, "message": "未知资源来源，无法执行转存"}), 400


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