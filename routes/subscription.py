# routes/subscription.py
import logging
import re
from flask import Blueprint, jsonify, request
from extensions import admin_required
from database import settings_db
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive, filter_hdhive_resources
from handler.tg_userbot import TGUserBotManager, tg_task_queue
from handler.tg_media_candidate import build_channel_task_payload
from handler.shared_center_client import SharedCenterClient, shared_center_enabled
from handler.shared_subscription_service import consume_center_source_payload
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
    try:
        shared_pool_configured = bool(shared_center_enabled())
    except Exception:
        shared_pool_configured = False

    return jsonify({
        "success": True,
        "mp_configured": bool(mp_url),
        "hdhive_configured": bool(hdhive_configured),
        "tg_userbot_configured": tg_userbot_configured,
        "shared_pool_configured": shared_pool_configured,
        "cloud_search_configured": bool(hdhive_configured or tg_userbot_configured or shared_pool_configured)
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



def _format_bytes_for_cloud(size):
    try:
        n = int(float(size or 0))
    except Exception:
        n = 0
    if n <= 0:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(n)
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    if idx <= 1:
        return f"{int(value)} {units[idx]}"
    return f"{value:.2f} {units[idx]}"


def _first_cloud_text(*values):
    for value in values:
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            text = ", ".join(str(x) for x in value if str(x or "").strip())
        else:
            text = str(value).strip()
        if text:
            return text
    return ""



def _cloud_json_obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            import json as _json
            parsed = _json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _cloud_bool_state(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "是", "启用", "开启"}:
        return True
    if text in {"0", "false", "no", "n", "off", "否", "停用", "关闭"}:
        return False
    return None


def _shared_pool_tag_containers(item: dict) -> list[dict]:
    item = item if isinstance(item, dict) else {}
    out = [item]
    for key in (
        "version_summary", "summary_json", "media_signature_json", "raw_summary_json", "rapid_meta_json",
        "clean_version_meta_json", "short_drama_meta_json", "animation_meta_json", "completed_certified_meta_json",
    ):
        value = _cloud_json_obj(item.get(key))
        if value:
            out.append(value)
    return out


def _shared_pool_flag_enabled(item: dict, flag_key: str, meta_key: str = "") -> bool:
    """读取共享池中心源标签。顶层/摘要/专用 meta 任一显式 true 即认为命中。"""
    for part in _shared_pool_tag_containers(item):
        state = _cloud_bool_state(part.get(flag_key)) if flag_key in part else None
        if state is True:
            return True
        meta = _cloud_json_obj(part.get(meta_key)) if meta_key else {}
        state = _cloud_bool_state(meta.get(flag_key)) if flag_key in meta else None
        if state is True:
            return True
    return False


def _shared_pool_plain_tag_labels(item: dict) -> list[str]:
    labels: list[str] = []
    for part in _shared_pool_tag_containers(item):
        raw = part.get("tag_labels")
        if isinstance(raw, str):
            raw = [x.strip() for x in re.split(r"[,，/|]", raw) if x.strip()]
        if isinstance(raw, list):
            for label in raw:
                text = str(label or "").strip()
                if text and text not in labels:
                    labels.append(text)
    return labels


def _cloud_track_text_values(value) -> list[str]:
    """把音轨/字幕字段统一展开成可匹配文本。

    共享池的 MediaInfo 摘要可能是字符串数组，也可能是
    [{display, language, title, codec}] 这类对象；这里只抽取短文本，
    不把整个资源 JSON 都扫进去，避免标题/备注误触发。
    """
    out: list[str] = []

    def add_text(text):
        text = str(text or "").strip()
        if text and text not in out:
            out.append(text)

    def walk(v, depth: int = 0):
        if v in (None, "", [], {}):
            return
        if isinstance(v, (str, int, float, bool)):
            add_text(v)
            return
        if isinstance(v, list):
            for x in v[:80]:
                walk(x, depth + 1)
            return
        if isinstance(v, dict):
            # 只取音轨/字幕对象常见的描述字段，避免把 unrelated meta 全部拼进去。
            for key in (
                "display", "DisplayTitle", "title", "Title", "name", "Name",
                "language", "Language", "lang", "DisplayLanguage",
                "codec", "Codec", "channel_layout", "channels"
            ):
                if key in v:
                    walk(v.get(key), depth + 1)
            return

    walk(value)
    return out


def _shared_pool_track_texts(item: dict, kind: str) -> list[str]:
    item = item if isinstance(item, dict) else {}
    if kind == "audio":
        keys = (
            "audio_list", "audios", "audio_tracks", "audio", "audio_track",
            "default_audio", "default_audio_track", "audio_languages", "languages",
        )
    else:
        keys = (
            "subtitle_list", "subtitles", "subtitle_tracks", "subtitle", "subtitles_text",
            "default_subtitle", "default_subtitle_track", "subtitle_languages",
        )

    texts: list[str] = []
    for part in _shared_pool_tag_containers(item):
        for key in keys:
            for text in _cloud_track_text_values(part.get(key)):
                if text and text not in texts:
                    texts.append(text)
    return texts


def _contains_mandarin_audio_text(text: str) -> bool:
    """格式化后的音轨标题命中“国语/普通话”即认为有国语音轨。"""
    text = str(text or "").strip()
    if not text:
        return False
    return "国语" in text or "普通话" in text or "普通話" in text


def _contains_chinese_subtitle_text(text: str) -> bool:
    """格式化后的字幕标题只按中文关键词打“中字”标签，不再扫英文语言码。"""
    text = str(text or "").strip()
    if not text:
        return False
    return any(token in text for token in ("中文", "简中", "繁中", "简体", "繁体", "中英"))


def _contains_special_effect_subtitle_text(text: str) -> bool:
    text = str(text or "").strip()
    return bool(text and "特效" in text)


def _contains_bilingual_subtitle_text(text: str) -> bool:
    text = str(text or "").strip()
    return bool(text and ("双语" in text or "雙語" in text))


def _shared_pool_has_mandarin_audio(item: dict) -> bool:
    return any(_contains_mandarin_audio_text(text) for text in _shared_pool_track_texts(item, "audio"))


def _shared_pool_has_chinese_subtitle(item: dict) -> bool:
    return any(_contains_chinese_subtitle_text(text) for text in _shared_pool_track_texts(item, "subtitle"))


def _shared_pool_has_special_effect_subtitle(item: dict) -> bool:
    return any(_contains_special_effect_subtitle_text(text) for text in _shared_pool_track_texts(item, "subtitle"))


def _shared_pool_has_bilingual_subtitle(item: dict) -> bool:
    return any(_contains_bilingual_subtitle_text(text) for text in _shared_pool_track_texts(item, "subtitle"))


def _shared_pool_cloud_tag_labels(item: dict) -> list[dict]:
    """云资源搜索共享池卡片额外标签。中心资源库已有的纯净版/短剧/动漫等标签不能丢。"""
    tags: list[dict] = []
    seen: set[str] = set()

    def add(label: str, tag_type: str = "default", bordered: bool = False):
        label = str(label or "").strip()
        if not label or label in seen:
            return
        seen.add(label)
        tags.append({"label": label, "type": tag_type, "bordered": bool(bordered)})

    if _shared_pool_flag_enabled(item, "is_clean_version", "clean_version_meta_json"):
        add("纯净版", "warning", False)
    if _shared_pool_flag_enabled(item, "is_short_drama", "short_drama_meta_json"):
        add("短剧", "info", False)
    if _shared_pool_flag_enabled(item, "is_animation", "animation_meta_json"):
        add("动漫", "success", False)
    if _shared_pool_has_mandarin_audio(item):
        add("国语", "success", False)
    if _shared_pool_has_chinese_subtitle(item):
        add("中字", "info", False)
    if _shared_pool_has_special_effect_subtitle(item):
        add("特效", "warning", False)
    if _shared_pool_has_bilingual_subtitle(item):
        add("双语", "info", False)

    # 保留中心端扩展标签；已完结/连载中由现有 _completion_label 单独展示，避免重复。
    skip = {"已完结", "完结", "已认证完结", "完结认证", "连载中", "可用"}
    for label in _shared_pool_plain_tag_labels(item):
        if label in skip:
            continue
        if label in {"纯净版", "短剧", "动漫"}:
            # 上面已按更清晰颜色加过。
            add(label, {"纯净版": "warning", "短剧": "info", "动漫": "success"}.get(label, "default"), False)
        else:
            add(label, "default", True)
    return tags


def _shared_pool_summary_for_version(item: dict) -> dict:
    """取一个共享池行/版本行的摘要，用来判断是否真的是不同内容版本。"""
    item = item if isinstance(item, dict) else {}
    for key in ("version_summary", "summary_json", "media_signature_json", "raw_summary_json"):
        value = item.get(key)
        if isinstance(value, dict) and value:
            return value
        value = _cloud_json_obj(value)
        if value:
            return value
    return {}


def _shared_pool_source_count(item: dict) -> int:
    """中心可能把同一内容的多个 holder/source 折叠到不同字段里，这里统一读取数量。"""
    item = item if isinstance(item, dict) else {}
    for key in (
        "source_count", "sources_count", "shared_source_count", "share_source_count",
        "holder_count", "holders_count", "mirror_count", "backup_count", "resource_count",
    ):
        count = _safe_int(item.get(key), default=0)
        if count > 0:
            return count
    for key in ("source_ids", "source_list", "sources", "holders", "mirrors", "backups"):
        value = item.get(key)
        if isinstance(value, list) and value:
            return len(value)
    return 1


def _shared_pool_version_identity(parent: dict, version: dict) -> str:
    """生成“内容版本”指纹。

    注意：共享中心里的 versions 可能既包含真正的内容版本，也包含同一内容的多个共享源。
    云搜索不能把多个共享源显示成 1/4、2/4 这种版本，所以这里先按强指纹
    （manifest_hash / sha1 / 版本 key）合并；没有强指纹时再按媒体摘要兜底合并。
    """
    parent = parent if isinstance(parent, dict) else {}
    version = version if isinstance(version, dict) else {}
    merged = dict(parent)
    merged.update(version)

    for key in ("manifest_hash", "sha1", "file_sha1", "content_hash", "hash"):
        value = str(merged.get(key) or "").strip().upper()
        if value:
            return f"{key}:{value}"

    # 季包如果已经带了 children/pack_items，按整季每集 SHA1 集合判断真实版本。
    for child_key in ("pack_items", "children", "files"):
        children = merged.get(child_key)
        if isinstance(children, list) and children:
            sha_parts = []
            for child in children:
                if not isinstance(child, dict):
                    continue
                ep = _safe_int(child.get("episode_number"), default=0)
                sha = str(child.get("sha1") or child.get("file_sha1") or "").strip().upper()
                if sha:
                    sha_parts.append(f"{ep}:{sha}" if ep > 0 else sha)
            if sha_parts:
                return f"{child_key}:" + "|".join(sorted(sha_parts))

    summary = _shared_pool_summary_for_version(merged)
    size = _safe_int(merged.get("size") or merged.get("total_size") or summary.get("size"), default=0)
    parts = [
        str(merged.get("source_kind") or parent.get("source_kind") or "").strip().lower(),
        str(merged.get("item_type") or parent.get("item_type") or "").strip().lower(),
        str(merged.get("tmdb_id") or parent.get("tmdb_id") or "").strip(),
        str(merged.get("season_number") or parent.get("season_number") or "").strip(),
        str(summary.get("resolution") or summary.get("resolution_display") or "").strip().lower(),
        str(summary.get("effect") or summary.get("effect_key") or "").strip().lower(),
        str(summary.get("codec") or summary.get("video_codec") or summary.get("codec_display") or "").strip().lower(),
        str(summary.get("bit_depth") or "").strip().lower(),
        str(summary.get("fps") or summary.get("frame_rate") or "").strip().lower(),
        str(summary.get("video_display") or "").strip().lower(),
        str(size) if size > 0 else "",
    ]
    compact = "|".join(parts)
    return f"summary:{compact}" if compact.strip("|") else "summary:unknown"


def _shared_pool_choose_representative(items: list[dict]) -> dict:
    """同一内容版本有多个共享源时，列表只展示一个代表源。"""
    candidates = [dict(x) for x in (items or []) if isinstance(x, dict)]
    if not candidates:
        return {}

    def score(item: dict):
        status = str(item.get("status") or item.get("center_status") or "").strip().lower()
        status_score = 2 if status in {"alive", "available", "active", "reported"} else (1 if not status else 0)
        source_score = 1 if (item.get("source_id") or item.get("source_ref_id")) else 0
        size_score = _safe_int(item.get("size") or item.get("total_size"), default=0)
        return (status_score, source_score, size_score)

    return max(candidates, key=score)


def _shared_pool_version_rows(resource):
    """把中心资源库聚合行转换成云搜索卡片。

    这里的关键点是：versions 里可能混有“真实内容版本”和“同一内容的多个共享源”。
    只有内容指纹不同才拆成多个版本；同一内容的多个共享源只合并为一张卡片，
    用“共享源 N”表示备份数量，避免出现“版本 1/4、2/4”这种误导。
    """
    parent = dict(resource or {})
    raw_versions = parent.get("versions")
    versions = [dict(x) for x in raw_versions if isinstance(x, dict)] if isinstance(raw_versions, list) else []
    if not versions:
        one = dict(parent)
        one.pop("versions", None)
        one["_shared_pool_source_count"] = _shared_pool_source_count(one)
        one["_shared_pool_is_version"] = False
        return [one]

    grouped = []
    group_index = {}
    for version in versions:
        key = _shared_pool_version_identity(parent, version)
        if key not in group_index:
            group_index[key] = len(grouped)
            grouped.append({"key": key, "items": []})
        group_index[key]
        grouped[group_index[key]]["items"].append(version)

    rows = []
    total_versions = len(grouped)
    parent_source_id = parent.get("source_id") or parent.get("source_ref_id") or parent.get("hub_id")
    for idx, group in enumerate(grouped, start=1):
        group_items = group.get("items") or []
        representative = _shared_pool_choose_representative(group_items)
        source_count = sum(max(1, _shared_pool_source_count(item)) for item in group_items) or len(group_items) or 1

        merged = dict(parent)
        # 版本行里的 source_id / sha1 / summary / size 才代表真正要秒传的版本。
        merged.update(representative)
        merged.pop("versions", None)
        # children/pack_items 仍保持懒加载，不从列表卡片里携带大对象。
        if not representative.get("children"):
            merged.pop("children", None)
        if not representative.get("pack_items"):
            merged.pop("pack_items", None)
        merged["_shared_pool_parent_source_id"] = parent_source_id
        merged["_shared_pool_version_index"] = idx if total_versions > 1 else 0
        merged["_shared_pool_version_count"] = total_versions if total_versions > 1 else 0
        merged["_shared_pool_is_version"] = total_versions > 1
        merged["_shared_pool_source_count"] = source_count
        merged["_shared_pool_alternative_sources"] = group_items[:20]
        merged["_shared_pool_content_key"] = group.get("key") or ""
        # 聚合主行的季进度/懒加载标记要保留，版本行缺这些字段时前端仍能显示。
        for key in (
            "progress_current", "progress_total", "progress_text", "season_number", "tmdb_id",
            "has_children", "children_loaded", "lazy_children_kind", "children_count", "child_count",
            "pack_item_count", "is_completed_certified", "is_completed", "is_ongoing_hub",
            "is_clean_version", "clean_version_meta_json", "is_short_drama", "short_drama_meta_json",
            "is_animation", "animation_meta_json", "tag_labels",
            "share_channel", "logical_season_share_channel", "completed_share_channel",
            "has_share_channel", "share_channel_status", "has_valid_share_channel",
            "share_transfer_available", "preferred_transfer_mode", "transfer_mode",
            "_completion_label", "release_year",
        ):
            if merged.get(key) in (None, "", [], {}) and parent.get(key) not in (None, "", [], {}):
                merged[key] = parent.get(key)
        rows.append(merged)
    return rows


def _cloud_year_text(*values):
    for value in values:
        match = re.search(r"(19|20)\d{2}", str(value or ""))
        if match:
            return match.group(0)
    return ""


def _format_shared_pool_cloud_title(item: dict) -> str:
    """共享池云搜索标题：影视名（年份）第 N 季。

    中心资源库内部为了排序/聚合会保留 S01 这类机器友好标题；云搜索面向用户，
    剧集包统一显示成“家业（2026）第 1 季”，电影显示成“阿凡达（2009）”。
    """
    item = item if isinstance(item, dict) else {}
    raw_title = str(item.get("title") or item.get("name") or item.get("file_name") or "共享池资源").strip()
    year = _cloud_year_text(item.get("release_year"), item.get("year"), item.get("release_date"), item.get("first_air_date"))
    season = _safe_int(item.get("season_number"), default=0)
    display_kind = str(item.get("display_type") or item.get("item_type") or item.get("source_kind") or "").strip().lower()
    source_kind = str(item.get("source_kind") or "").strip().lower()
    is_pack = bool(
        season > 0
        and (
            display_kind in {"pack", "season", "series"}
            or source_kind in {"season_hub", "completed_season"}
            or item.get("progress_text")
        )
    )

    # 去掉历史 S01 / Season 1 / 第 1 季 后缀，再重新按中文口径拼接。
    base = raw_title
    if is_pack:
        base = re.sub(r"\s*(?:第\s*\d+\s*季|S\d{1,3}|Season\s*\d{1,3})\s*$", "", base, flags=re.I).strip() or raw_title

    has_year = bool(year and re.search(rf"[（(]\s*{re.escape(year)}\s*[）)]", base))
    if year and not has_year:
        base = f"{base}（{year}）"

    if is_pack:
        season_label = f"第 {season} 季"
        if not re.search(r"第\s*\d+\s*季", base):
            base = f"{base}{season_label}"
    return base



def _shared_pool_season_sort_number(item: dict) -> int:
    """云资源搜索共享池排序：剧集按第 1/2/3 季自然顺序展示。"""
    item = item if isinstance(item, dict) else {}
    season = _safe_int(item.get("season_number"), default=0)
    if season > 0:
        return season
    text = " ".join(str(item.get(k) or "") for k in ("title", "name", "file_name", "remark"))
    for pattern in (r"第\s*(\d{1,3})\s*季", r"\bS(\d{1,3})\b", r"Season\s*(\d{1,3})"):
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return _safe_int(m.group(1), default=0)
    return 0


def _shared_pool_cloud_sort_key(index_and_item):
    """共享池云搜索卡片排序。

    中心资源库默认按最新共享排序没问题，但云搜索面向人工挑选；
    搜同一部剧时应该按季号 1、2、3 展示，每季内再按版本序号展示。
    """
    index, item = index_and_item
    item = item if isinstance(item, dict) else {}
    media_type_rank = 0 if str(item.get("item_type") or item.get("display_type") or "").lower() in {"season", "pack", "series"} else 1
    season = _shared_pool_season_sort_number(item)
    version = _safe_int(item.get("_shared_pool_version_index"), default=0)
    if version <= 0:
        m = re.search(r"版本\s*(\d{1,3})\s*/", str(item.get("_shared_pool_version_label") or ""))
        version = _safe_int(m.group(1), default=0) if m else 0
    title = str(item.get("title") or item.get("name") or item.get("file_name") or "")
    # 没有季号的电影/散资源保持原始顺序；有季号的剧集按季号升序。
    season_rank = season if season > 0 else 9999
    version_rank = version if version > 0 else 9999
    return (media_type_rank, season_rank, version_rank, title, index)

def _normalize_shared_pool_resource(resource):
    # 把共享中心资源库展示行转换成云资源搜索卡片。
    item = dict(resource or {})
    summary = item.get("version_summary") if isinstance(item.get("version_summary"), dict) else {}
    if not summary:
        summary = item.get("summary_json") if isinstance(item.get("summary_json"), dict) else {}
    source_kind = str(item.get("source_kind") or "").strip()
    source_id = str(item.get("source_id") or item.get("source_ref_id") or "").strip()
    sha1 = str(item.get("sha1") or "").strip()
    manifest_hash = str(item.get("manifest_hash") or "").strip()
    if source_kind and source_id:
        unique = f"shared_pool:{source_kind}:{source_id}:{sha1 or manifest_hash}"
    else:
        unique = f"shared_pool:{item.get('tmdb_id')}:{item.get('season_number') or ''}:{sha1 or manifest_hash or item.get('title') or item.get('file_name') or ''}"
    title = _format_shared_pool_cloud_title(item)

    version_index = _safe_int(item.get("_shared_pool_version_index"), default=0)
    version_count = _safe_int(item.get("_shared_pool_version_count"), default=0)
    version_label = f"版本 {version_index}/{version_count}" if version_index and version_count > 1 else ""
    source_count = _safe_int(item.get("_shared_pool_source_count") or item.get("source_count") or item.get("shared_source_count"), default=0)
    source_label = f"共享源 {source_count}" if source_count > 1 else ""
    # 云搜索卡片已经用标签展示资源数、画质、字幕等信息；备注只保留真正的状态提示。
    # 不再把“共享秒传 / 版本 x/y / 共享池 · 可秒传”塞进标签或描述，避免和按钮/标签重复。
    remark_parts = [x for x in (item.get("status_message"),) if x]

    has_mandarin_audio = _shared_pool_has_mandarin_audio(item)
    has_chinese_subtitle = _shared_pool_has_chinese_subtitle(item)
    has_special_effect_subtitle = _shared_pool_has_special_effect_subtitle(item)
    has_bilingual_subtitle = _shared_pool_has_bilingual_subtitle(item)

    item.update({
        "source_type": "shared_pool",
        "source_name": "共享池",
        "_cloud_source": "shared_pool",
        "unique_id": unique,
        "title": title,
        "name": title,
        "pan_type": "rapid115",
        "already_owned": bool(item.get("is_mine")),
        "unlock_points": 0,
        "share_size": _first_cloud_text(item.get("share_size"), _format_bytes_for_cloud(item.get("size") or item.get("total_size"))),
        "video_resolution": _first_cloud_text(summary.get("resolution"), summary.get("resolution_display")),
        "quality": _first_cloud_text(summary.get("video_display"), summary.get("codec"), summary.get("video_codec")),
        "source": _first_cloud_text(summary.get("effect"), summary.get("effect_key")),
        "source_detail": "",
        "remark": " · ".join(str(x) for x in remark_parts if str(x).strip()),
        "_season_match_label": item.get("progress_text") or "",
        "_shared_pool_version_label": version_label,
        "_shared_pool_source_count": source_count,
        "_shared_pool_source_label": source_label,
        "has_mandarin_audio": has_mandarin_audio,
        "has_chinese_subtitle": has_chinese_subtitle,
        "has_special_effect_subtitle": has_special_effect_subtitle,
        "has_bilingual_subtitle": has_bilingual_subtitle,
        "_shared_pool_tag_labels": _shared_pool_cloud_tag_labels(item),
        "_shared_pool_tags": _shared_pool_cloud_tag_labels(item),
        "_completion_label": "已完结" if item.get("is_completed_certified") or item.get("is_completed") else ("连载中" if item.get("is_ongoing_hub") else ""),
    })
    return item


def _shared_pool_resource_key(resource):
    if not isinstance(resource, dict):
        return ""
    for key in ("unique_id", "source_id", "source_ref_id", "hub_id"):
        value = resource.get(key)
        if value:
            return f"shared_pool:{value}"
    return ""


def _strip_season_suffix(text):
    value = str(text or "").strip()
    if not value:
        return ""

    return re.sub(
        r"\s*(?:第\s*\d+\s*季|S\d{1,3}|Season\s*\d{1,3})\s*$",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip()


def _build_cloud_extra_queries(title, year=None):
    title = str(title or "").strip()
    year = str(year or "").strip()
    queries = []
    if title and year:
        queries.append(f"{title} {year}")
    return queries


@subscription_bp.route('/cloud/resources', methods=['GET'])
@admin_required
def get_cloud_resources():
    """云资源搜索：合并影巢 OpenAPI 与已配置 TG 频道历史消息搜索。"""
    tmdb_id = request.args.get('tmdb_id')
    raw_media_type = request.args.get('media_type')
    season = request.args.get('season')
    title = _strip_season_suffix(request.args.get('title') or request.args.get('query') or '')
    year = (request.args.get('year') or '').strip()
    media_type = _normalize_hdhive_media_type(raw_media_type)

    collect_limit = _safe_int(request.args.get('limit'), default=50, min_value=1, max_value=100)
    hdhive_limit = _safe_int(request.args.get('hdhive_limit'), default=collect_limit, min_value=1, max_value=100)
    channel_limit = _safe_int(request.args.get('channel_limit'), default=collect_limit, min_value=1, max_value=100)
    shared_limit = _safe_int(request.args.get('shared_limit'), default=collect_limit, min_value=1, max_value=100)

    if not tmdb_id and not title:
        return jsonify({"success": False, "message": "缺少 TMDB ID 或搜索标题"}), 400

    warnings = []
    hdhive_total = 0
    hdhive_filtered = 0
    channel_total = 0
    shared_pool_total = 0
    resources = []
    seen = set()

    # 0. 共享池资源。共享池已经是本机可直接秒传的中心资源，优先展示。
    try:
        if shared_center_enabled():
            client = SharedCenterClient()
            shared_item_type = 'Movie' if media_type == 'movie' else 'Pack'
            shared_status = 'alive,available' if media_type == 'movie' else 'alive,available,updating,inconsistent,incomplete'
            # 剧集云搜索要先按季号重排，不能只取“最新共享”的前 N 条后再排序，
            # 否则老一点的第 1 季可能被中心端分页截掉。这里多取一段，再本地按 1/2/3 季展示。
            shared_fetch_limit = shared_limit
            if media_type == 'tv':
                shared_fetch_limit = max(shared_limit, min(500, shared_limit * 5))
            shared_resp = client.list_cloud_search_sources(
                q='' if tmdb_id else title,
                status=shared_status,
                item_type=shared_item_type,
                tmdb_id=tmdb_id or '',
                order_by='latest',
                limit=shared_fetch_limit,
                offset=0,
            )
            shared_items = [x for x in (shared_resp.get('items') or []) if isinstance(x, dict)]
            shared_version_items = []
            for item in shared_items:
                shared_version_items.extend(_shared_pool_version_rows(item))
            shared_version_items = [
                item for _, item in sorted(
                    enumerate(shared_version_items),
                    key=_shared_pool_cloud_sort_key,
                )
            ]
            shared_pool_total = len(shared_version_items)
            for item in shared_version_items[:shared_limit]:
                # 中心旧数据可能没有 release_year，云搜索入口已带 year 时作为展示兜底。
                if isinstance(item, dict) and not item.get('release_year') and year:
                    item = dict(item)
                    item['release_year'] = year
                normalized = _normalize_shared_pool_resource(item)
                key = _shared_pool_resource_key(normalized) or _cloud_resource_key(normalized)
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                resources.append(normalized)
        else:
            warnings.append('共享池未启用或未配置中心地址，已跳过共享池搜索。')
    except Exception as e:
        logger.error(f"  ➜ 云资源搜索：共享池查询失败: {e}", exc_info=True)
        warnings.append(f"共享池搜索失败：{e}")

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
                extra_queries=_build_cloud_extra_queries(title, year=year),
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
            "shared_pool_total": shared_pool_total,
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

    if source_type in {'shared_pool', 'shared', 'shared_center', 'center', '共享池'} or resource.get('source_kind') in {'movie', 'episode', 'completed_season', 'season_hub'}:
        if not shared_center_enabled():
            return jsonify({"success": False, "message": "共享池未启用或未配置中心地址"}), 401

        shared_source = dict(resource or {})
        source_kind = str(shared_source.get('source_kind') or '').strip()
        source_id = str(shared_source.get('source_id') or shared_source.get('source_ref_id') or '').strip()
        if not source_kind or not source_id:
            return jsonify({"success": False, "message": "共享池资源缺少 source_kind/source_id，无法秒传"}), 400

        # 列表页为了秒开只返回季包壳；公共连载季真正秒传前必须展开一次，拿到该季 children。
        if source_kind == 'season_hub' and not (shared_source.get('children') or shared_source.get('pack_items')):
            try:
                child_resp = SharedCenterClient().list_display_children(
                    source_kind='season_hub',
                    source_id=source_id,
                    hub_id=shared_source.get('hub_id') or source_id,
                    limit=5000,
                )
                children = child_resp.get('children') or child_resp.get('items') or []
                pack_items = child_resp.get('pack_items') or children
                shared_source['children'] = children
                shared_source['pack_items'] = pack_items
            except Exception as e:
                return jsonify({"success": False, "message": f"加载共享池季包明细失败：{e}"}), 500

        mode = str(data.get('mode') or resource.get('preferred_transfer_mode') or resource.get('transfer_mode') or 'rapid').strip().lower()
        result = consume_center_source_payload(shared_source, mode=mode)
        ok = bool(result.get('ok') or result.get('success'))
        status_code = 200 if ok else 400
        action = '转存' if (result.get('transfer_mode') == 'share' or mode == 'share') else '秒传'
        msg = result.get('message') or (f"共享池{action}完成：{result.get('success_count', 0)}/{result.get('total', 0)}" if ok else f"共享池{action}失败")
        return jsonify({"success": ok, "message": msg, "data": result}), status_code

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

        tg_task_queue.put(
            build_channel_task_payload(
                resource,
                is_brainless=True,
                is_keyword_matched=True,
                is_subscribe=False,
                title_override=title,
                tmdb_id_override=tmdb_id,
                media_type_override=media_type,
                year_override=data.get('year') or resource.get('year'),
            )
        )
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
