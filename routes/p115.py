# routes/p115.py
import logging
import threading
from queue import Queue
from datetime import datetime, timedelta
import json
import base64
import os
import re
import time
import random
import secrets
from urllib.parse import unquote, urlparse
import requests
from flask import Blueprint, jsonify, request, redirect, Response, stream_with_context, current_app, session
from extensions import admin_required, emby_login_required
from database import settings_db
from handler import moviepilot, emby
from handler.p115_service import P115Service, P115CacheManager, get_config
from handler.etk_plugin_update import is_etk_plugin_installed
from handler.p115_rename import P115RenameRenderer
from handler import p115_play_pool
import constants
import config_manager
from functools import lru_cache, wraps

# 115扫码登录相关变量 (OAuth 2.0 + PKCE 模式)
_qrcode_data = {
    "qrcode": None,        # 二维码内容
    "uid": None,           # 设备码
    "time": None,         # 时间戳
    "sign": None,         # 签名
    "code_verifier": None,# PKCE verifier
    "access_token": None,  # 最终获取的 access_token
    "refresh_token": None  # 刷新token
}
p115_bp = Blueprint('115_bp', __name__, url_prefix='/api/p115')
logger = logging.getLogger(__name__)

_DEEP_DELETE_TTL_SECONDS = 300
_deep_delete_lock = threading.Lock()
_deep_delete_snapshots = {}


def _build_deep_delete_file_snapshots(pickcodes):
    snapshots = []
    for pc in pickcodes or []:
        pc_text = str(pc or '').strip()
        if not pc_text:
            continue
        try:
            row = P115CacheManager.get_file_cache_by_pickcode(pc_text) or {}
        except Exception as e:
            logger.debug(f"  ➜ [ETK 深度删除] 读取 115 缓存快照失败: PC={pc_text[:8]}..., err={e}")
            row = {}
        fid = str(row.get('id') or '').strip()
        if not fid:
            continue
        snapshots.append({
            'pick_code': str(row.get('pick_code') or pc_text).strip(),
            'fid': fid,
            'parent_id': str(row.get('parent_id') or '').strip(),
        })
    return snapshots


def _extract_p115_pickcodes_from_emby_item(item):
    if not isinstance(item, dict):
        return []
    values = [item.get('Path')]
    values.extend(
        source.get('Path')
        for source in (item.get('MediaSources') or [])
        if isinstance(source, dict)
    )
    pickcodes = []
    seen = set()
    for value in values:
        text = unquote(str(value or ''))
        play_match = re.search(r'/api/p115/play/([^/?#]+)', text, flags=re.IGNORECASE)
        if play_match:
            pc_text = play_match.group(1).strip()
            pc_key = pc_text.lower()
            if pc_text and pc_key not in seen:
                seen.add(pc_key)
                pickcodes.append(pc_text)
            continue

        virtual_match = re.search(
            r'/api/p115/virtual-play/[^/?#]+/([A-Fa-f0-9]{40})(?:[/?#]|$)',
            text,
            flags=re.IGNORECASE,
        )
        if virtual_match:
            row = P115CacheManager.get_file_cache_by_sha1(virtual_match.group(1).upper()) or {}
            pc_text = str(row.get('pick_code') or '').strip()
            pc_key = pc_text.lower()
            if pc_text and pc_key not in seen:
                seen.add(pc_key)
                pickcodes.append(pc_text)
    return pickcodes


def _extract_p115_virtual_id_from_emby_item(item, sha1=''):
    if not isinstance(item, dict):
        return 0
    expected_sha1 = str(sha1 or '').strip().upper()
    values = [item.get('Path')]
    values.extend(
        source.get('Path')
        for source in (item.get('MediaSources') or [])
        if isinstance(source, dict)
    )
    for value in values:
        text = unquote(str(value or ''))
        virtual_match = re.search(
            r'/api/p115/virtual-play/(\d+)/([A-Fa-f0-9]{40})(?:[/?#]|$)',
            text,
            flags=re.IGNORECASE,
        )
        if not virtual_match:
            continue
        if expected_sha1 and virtual_match.group(2).upper() != expected_sha1:
            continue
        try:
            return int(virtual_match.group(1))
        except Exception:
            return 0
    return 0


def _collect_deep_delete_fallback_pickcodes(root_item_id, actual_type, anchor):
    pickcodes = []
    seen = set()

    def add_from_item(item):
        for pc in _extract_p115_pickcodes_from_emby_item(item):
            pc_key = pc.lower()
            if pc_key not in seen:
                seen.add(pc_key)
                pickcodes.append(pc)

    add_from_item(anchor)
    if actual_type not in {'Series', 'Season'}:
        return pickcodes

    children = emby.get_emby_library_items(
        base_url=config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
        api_key=config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        media_type_filter='Episode,Movie,Video',
        user_id=config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID),
        library_ids=[root_item_id],
        fields='Path,MediaSources,Type,Name',
    ) or []
    for child in children:
        add_from_item(child)
    return pickcodes


def _normalize_p115_category_path(path):
    text = str(path or '').strip().replace('\\', '/')
    text = re.sub(r'/+', '/', text).strip('/')
    return text


def _strip_p115_media_root_prefix(path, media_root_name):
    clean_path = _normalize_p115_category_path(path)
    clean_root = _normalize_p115_category_path(media_root_name)
    if clean_root:
        root_cf = clean_root.casefold()
        path_cf = clean_path.casefold()
        if path_cf == root_cf:
            return ''
        if path_cf.startswith(root_cf + '/'):
            return clean_path[len(clean_root):].strip('/')
    return clean_path


# --- 115扫码登录相关API (OAuth 2.0 + PKCE 模式) ---

def _generate_pkce_pair():
    """生成 PKCE 的 verifier 和 challenge"""
    import base64
    import os
    import hashlib
    
    # 1. 生成 43~128 位的随机字符串 (code_verifier)
    verifier = base64.urlsafe_b64encode(os.urandom(40)).decode('utf-8').rstrip('=')
    
    # 2. 计算 SHA256 并进行 Base64Url 编码 (code_challenge)
    digest = hashlib.sha256(verifier.encode('ascii')).digest()
    challenge = base64.urlsafe_b64encode(digest).decode('utf-8').rstrip('=')
    
    return verifier, challenge

def _generate_qrcode():
    """生成115扫码登录二维码 (OAuth 2.0 + PKCE 新版API)"""
    try:
        # 读取自定义 AppID
        config = get_config()
        client_id = config.get(constants.CONFIG_OPTION_115_APP_ID)
        if not client_id or not client_id.strip():
            return {"error": "未配置自定义 AppID，请先在设置中保存"}
        else:
            client_id = client_id.strip()

        # 1. 生成 PKCE 密钥对
        verifier, challenge = _generate_pkce_pair()
        
        # 2. 调用获取二维码接口
        url = "https://passportapi.115.com/open/authDeviceCode"
        payload = {
            "client_id": client_id,  # ★ 使用动态 AppID
            "code_challenge": challenge,
            "code_challenge_method": "sha256"
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        resp = requests.post(url, data=payload, headers=headers, timeout=10)
        result = resp.json()
        
        if result.get('state'):
            qr_data = result.get('data', {})
            _qrcode_data['qrcode'] = qr_data.get('qrcode')
            _qrcode_data['uid'] = qr_data.get('uid')
            _qrcode_data['time'] = qr_data.get('time')
            _qrcode_data['sign'] = qr_data.get('sign')
            _qrcode_data['code_verifier'] = verifier
            _qrcode_data['access_token'] = None
            _qrcode_data['refresh_token'] = None
            return qr_data
        else:
            logger.error(f"获取二维码失败: {result.get('message')}")
            return None
    except Exception as e:
        logger.error(f"生成二维码失败: {e}")
        return None

def _check_qrcode_status():
    """检查二维码扫码状态 (OAuth 2.0 + PKCE 新版API)"""
    if not _qrcode_data.get('uid') or not _qrcode_data.get('time'):
        return {"status": "waiting", "message": "请先获取二维码"}
    
    try:
        # 1. 先轮询二维码状态
        url = "https://qrcodeapi.115.com/get/status/"
        params = {
            "uid": _qrcode_data.get('uid'),
            "time": _qrcode_data.get('time'),
            "sign": _qrcode_data.get('sign')
        }
        
        resp = requests.get(url, params=params, timeout=30)
        result = resp.json()
        
        state = result.get('state')
        
        # state=0 表示二维码无效/过期
        if state == 0:
            return {"status": "expired", "message": "二维码已过期，请重新获取"}
        
        # state=1 需要看 status 字段
        if state == 1:
            data = result.get('data', {})
            status = data.get('status')
            
            if status == 1:
                # 已扫码，等待确认
                return {"status": "waiting", "message": "已扫码，等待手机端确认..."}
            elif status == 2:
                # 已确认，现在需要换取 token
                # 2. 用 device code 换取 access_token
                token_url = "https://passportapi.115.com/open/deviceCodeToToken"
                token_payload = {
                    "uid": _qrcode_data.get('uid'),
                    "code_verifier": _qrcode_data.get('code_verifier')
                }
                token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
                
                token_resp = requests.post(token_url, data=token_payload, headers=token_headers, timeout=10)
                token_result = token_resp.json()
                
                if token_result.get('state'):
                    token_data = token_result.get('data', {})
                    access_token = token_data.get('access_token')
                    refresh_token = token_data.get('refresh_token')
                    
                    if access_token:
                        _qrcode_data['access_token'] = access_token
                        _qrcode_data['refresh_token'] = refresh_token
                        
                        # 3. 用 access_token 获取用户信息来验证
                        user_info_url = "https://proapi.115.com/open/user/info"
                        user_headers = {"Authorization": f"Bearer {access_token}"}
                        user_resp = requests.get(user_info_url, headers=user_headers, timeout=10)
                        user_result = user_resp.json()
                        
                        # 构造 cookies 格式 (UID=...; CID=...; SEID=...)
                        cookies = f"UID={_qrcode_data.get('uid')}; CID={_qrcode_data.get('uid')}; SEID={access_token}"
                        
                        return {
                            "status": "success", 
                            "message": "登录成功",
                            "user_info": user_result.get('data', {}),
                            "refresh_token": refresh_token
                        }
                else:
                    return {"status": "error", "message": "获取Token失败: " + token_result.get('message', '未知错误')}
            else:
                return {"status": "waiting", "message": data.get('msg', '等待扫码...')}
        
        return {"status": "waiting", "message": "等待扫码..."}
            
    except requests.exceptions.Timeout:
        return {"status": "waiting", "message": "轮询超时，继续等待..."}
    except Exception as e:
        logger.error(f"检查二维码状态失败: {e}")
        return {"status": "error", "message": str(e)}
    
@p115_bp.route('/qrcode', methods=['POST'])
@admin_required
def get_qrcode():
    """获取115登录二维码"""
    data = _generate_qrcode()
    if data:
        # 拦截未配置 AppID 的情况
        if "error" in data:
            return jsonify({"success": False, "message": data["error"]}), 400
            
        return jsonify({
            "success": True, 
            "data": {
                "qrcode": data.get('qrcode'),
                "uid": data.get('uid')
            }
        })
    return jsonify({"success": False, "message": "获取二维码失败"}), 500

@p115_bp.route('/qrcode/status', methods=['GET'])
@admin_required
def check_qrcode_status():
    """检查扫码登录状态"""
    status = _check_qrcode_status()
    
    if status.get('status') == 'success':
        access_token = _qrcode_data.get('access_token')
        refresh_token = _qrcode_data.get('refresh_token')
        
        if access_token and refresh_token:
            try:
                # ★ 直接调用小金库存钱函数
                from handler.p115_service import save_115_tokens
                save_115_tokens(access_token, refresh_token)
                logger.info(f"  ➜ [115] 扫码成功！Token 已保存。")
                    
            except Exception as e:
                logger.error(f"  ➜ 保存 Token 失败: {e}")
        
        return jsonify({
            "success": True,
            "status": "success",
            "message": "授权成功！",
        })
        
    elif status.get('status') == 'expired':
        return jsonify({"success": False, "status": "expired", "message": "二维码已过期，请重新获取"})
    elif status.get('status') == 'waiting':
        return jsonify({"success": True, "status": "waiting", "message": "等待扫码..."})
    else:
        return jsonify({"success": False, "status": "error", "message": status.get('message', '检查状态失败')}), 500

# --- 经典扫码获取 Cookie 流程 (支持多端) ---
_cookie_qrcode_data = {
    "uid": None,
    "time": None,
    "sign": None
}

_play_pool_cookie_qrcode_data = {
    "uid": None,
    "time": None,
    "sign": None,
    "app_type": "alipaymini",
    "mode": "cookie",
    "code_verifier": None,
}
_play_pool_cookie_qrcode_sessions = {}
_play_pool_cookie_qrcode_lock = threading.Lock()
PLAY_POOL_OPENAPI_CLIENT_ID = "100196261"

@p115_bp.route('/cookie_qrcode', methods=['GET'])
@admin_required
def get_cookie_qrcode():
    """获取用于生成 Cookie 的二维码 (支持指定 APP 类型)"""
    app_type = request.args.get('app', 'alipaymini') # 默认支付宝小程序
    try:
        from handler.p115_service import get_115_ua
        headers = {"User-Agent": get_115_ua(app_type)} # ★ 注入 UA
        
        # ★ 修复：URL 路径动态化
        url = f"https://qrcodeapi.115.com/api/1.0/{app_type}/1.0/token/?app={app_type}"
        resp = requests.get(url, headers=headers, timeout=10).json()
        
        if resp.get('state') == 1:
            data = resp.get('data', {})
            _cookie_qrcode_data['uid'] = data.get('uid')
            _cookie_qrcode_data['time'] = data.get('time')
            _cookie_qrcode_data['sign'] = data.get('sign')
            
            return jsonify({
                "success": True, 
                "data": {"qrcode": data.get('qrcode')}
            })
        return jsonify({"success": False, "message": resp.get('message', '获取失败')}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/cookie_qrcode/status', methods=['GET'])
@admin_required
def check_cookie_qrcode_status():
    """轮询 Cookie 二维码状态并执行登录获取 Cookie"""
    app_type = request.args.get('app', 'alipaymini')
    uid = _cookie_qrcode_data.get('uid')
    time_val = _cookie_qrcode_data.get('time')
    sign = _cookie_qrcode_data.get('sign')
    
    if not uid:
        return jsonify({"success": False, "status": "expired", "message": "请先获取二维码"})
        
    try:
        from handler.p115_service import get_115_ua
        headers = {"User-Agent": get_115_ua(app_type)} # ★ 注入 UA
        
        # 1. 轮询状态
        url = f"https://qrcodeapi.115.com/get/status/?uid={uid}&time={time_val}&sign={sign}"
        resp = requests.get(url, headers=headers, timeout=10).json()
        
        state = resp.get('state')
        if state == 0:
            return jsonify({"success": False, "status": "expired", "message": "二维码已过期"})
            
        if state == 1:
            status = resp.get('data', {}).get('status')
            if status == 1:
                return jsonify({"success": True, "status": "waiting", "message": "已扫码，请在手机端确认"})
            elif status == 2:
                # 2. 手机端已确认，调用登录接口换取 Cookie
                # ★ 修复：URL 路径动态化
                login_url = f"https://passportapi.115.com/app/1.0/{app_type}/1.0/login/qrcode"
                payload = {"account": uid, "app": app_type}
                
                # ★ 注入 headers
                login_resp = requests.post(login_url, data=payload, headers=headers, timeout=10)
                login_data = login_resp.json()
                
                if login_data.get('state') == 1:
                    cookies_dict = login_resp.cookies.get_dict()
                    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
                    
                    # ★ 保存到独立数据库，连同 app_type 一起保存！
                    from handler.p115_service import save_115_tokens
                    save_115_tokens(None, None, cookie_str, app_type)
                    
                    P115Service.reset_cookie_client()
                    return jsonify({"success": True, "status": "success", "message": "Cookie 获取成功！"})
                else:
                    return jsonify({"success": False, "status": "error", "message": login_data.get('message', '登录失败')})
                    
        return jsonify({"success": True, "status": "waiting", "message": "等待扫码..."})
    except Exception as e:
        return jsonify({"success": False, "status": "error", "message": str(e)}), 500

@p115_bp.route('/cookie', methods=['POST'])
@admin_required
def save_manual_cookie():
    """手动保存 Cookie 到独立数据库"""
    cookie_str = request.json.get('cookie', '').strip()
    # ★ 允许前端传 app_type，如果不传默认 web
    app_type = request.json.get('app_type', 'web').strip() 
    try:
        from handler.p115_service import save_115_tokens
        save_115_tokens(None, None, cookie_str, app_type)
        P115Service.reset_cookie_client()
        return jsonify({"success": True, "message": "Cookie 已保存"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@p115_bp.route('/play_pool/cookie_qrcode', methods=['GET'])
@emby_login_required
def get_play_pool_cookie_qrcode():
    mode = str(request.args.get('mode') or 'cookie').strip().lower()
    app_type = request.args.get('app', 'alipaymini')
    try:
        verifier = None
        if mode == "openapi":
            verifier, challenge = _generate_pkce_pair()
            resp = requests.post(
                "https://passportapi.115.com/open/authDeviceCode",
                data={
                    "client_id": PLAY_POOL_OPENAPI_CLIENT_ID,
                    "code_challenge": challenge,
                    "code_challenge_method": "sha256",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            ).json()
        else:
            from handler.p115_service import get_115_ua
            headers = {"User-Agent": get_115_ua(app_type)}
            url = f"https://qrcodeapi.115.com/api/1.0/{app_type}/1.0/token/?app={app_type}"
            resp = requests.get(url, headers=headers, timeout=10).json()
        if resp.get('state') == 1:
            data = resp.get('data', {})
            uid = data.get('uid')
            session = {
                "uid": uid,
                "time": data.get('time'),
                "sign": data.get('sign'),
                "app_type": app_type,
                "mode": mode,
                "code_verifier": verifier,
                "created_at": time.time(),
            }
            with _play_pool_cookie_qrcode_lock:
                _play_pool_cookie_qrcode_data.update(session)
                if uid:
                    _play_pool_cookie_qrcode_sessions[str(uid)] = session
                    cutoff = time.time() - 600
                    for key, value in list(_play_pool_cookie_qrcode_sessions.items()):
                        if float(value.get("created_at") or 0) < cutoff:
                            _play_pool_cookie_qrcode_sessions.pop(key, None)
            return jsonify({"success": True, "data": {"qrcode": data.get('qrcode'), "uid": uid}})
        return jsonify({"success": False, "message": resp.get('message', '获取失败')}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@p115_bp.route('/play_pool/cookie_qrcode/status', methods=['GET'])
@emby_login_required
def check_play_pool_cookie_qrcode_status():
    request_uid = str(request.args.get('uid') or '').strip()
    with _play_pool_cookie_qrcode_lock:
        session = _play_pool_cookie_qrcode_sessions.get(request_uid) if request_uid else None
        if not session:
            session = dict(_play_pool_cookie_qrcode_data)
    mode = str(request.args.get('mode') or session.get('mode') or 'cookie').strip().lower()
    app_type = request.args.get('app', session.get('app_type', 'alipaymini'))
    uid = session.get('uid')
    time_val = session.get('time')
    sign = session.get('sign')
    if not uid:
        return jsonify({"success": False, "status": "expired", "message": "请先获取二维码"})
    try:
        if mode == "openapi":
            resp = requests.get(
                "https://qrcodeapi.115.com/get/status/",
                params={"uid": uid, "time": time_val, "sign": sign},
                timeout=10,
            ).json()
            state = resp.get('state')
            if state == 0:
                return jsonify({"success": False, "status": "expired", "message": "二维码已过期"})
            if state == 1:
                status = resp.get('data', {}).get('status')
                if status == 1:
                    return jsonify({"success": True, "status": "waiting", "message": "等待扫码"})
                if status == 2:
                    token_resp = requests.post(
                        "https://passportapi.115.com/open/deviceCodeToToken",
                        data={"uid": uid, "code_verifier": session.get("code_verifier")},
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=10,
                    )
                    token_data = token_resp.json()
                    if token_data.get('state'):
                        token = token_data.get('data') or {}
                        access_token = str(token.get('access_token') or '').strip()
                        refresh_token = str(token.get('refresh_token') or '').strip()
                        if access_token:
                            return jsonify({
                                "success": True,
                                "status": "success",
                                "message": "OpenAPI 授权成功",
                                "data": {
                                    "access_token": access_token,
                                    "refresh_token": refresh_token,
                                }
                            })
                    return jsonify({"success": False, "status": "error", "message": token_data.get('message', '获取 Token 失败')})
            return jsonify({"success": True, "status": "waiting", "message": "等待扫码"})

        from handler.p115_service import get_115_ua
        headers = {"User-Agent": get_115_ua(app_type)}
        url = f"https://qrcodeapi.115.com/get/status/?uid={uid}&time={time_val}&sign={sign}"
        resp = requests.get(url, headers=headers, timeout=10).json()
        state = resp.get('state')
        if state == 0:
            return jsonify({"success": False, "status": "expired", "message": "二维码已过期"})
        if state == 1:
            status = resp.get('data', {}).get('status')
            if status == 1:
                return jsonify({"success": True, "status": "waiting", "message": "等待扫码"})
            if status == 2:
                login_url = f"https://passportapi.115.com/app/1.0/{app_type}/1.0/login/qrcode"
                login_resp = requests.post(login_url, data={"account": uid, "app": app_type}, headers=headers, timeout=10)
                login_data = login_resp.json()
                if login_data.get('state') == 1:
                    cookies_dict = login_resp.cookies.get_dict()
                    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
                    return jsonify({
                        "success": True,
                        "status": "success",
                        "message": "Cookie 获取成功",
                        "data": {
                            "cookie": cookie_str,
                            "app_type": app_type,
                        }
                    })
                return jsonify({"success": False, "status": "error", "message": login_data.get('message', '登录失败')})
        return jsonify({"success": True, "status": "waiting", "message": "等待扫码"})
    except Exception as e:
        return jsonify({"success": False, "status": "error", "message": str(e)}), 500

# --- 授权码模式登录 API ---
@p115_bp.route('/auto_save_auth', methods=['GET'])
@admin_required
def auto_save_auth():
    """接收 CF Worker 重定向回来的 Token 数据并自动保存"""
    data = request.args.get('data', '').strip()
    if not data:
        return "授权失败：缺少 Token 数据", 400
        
    try:
        # 1. Base64 解码
        decoded_bytes = base64.b64decode(data)
        token_data = json.loads(decoded_bytes.decode('utf-8'))
        
        access_token = token_data.get('access_token')
        refresh_token = token_data.get('refresh_token')

        if access_token and refresh_token:
            # 2. 保存到数据库
            from handler.p115_service import save_115_tokens, P115Service
            save_115_tokens(access_token, refresh_token)
            
            # 强制清空旧的 OpenAPI 客户端缓存，让它立即使用新 Token
            with P115Service._lock:
                P115Service._openapi_client = None
                
            logger.info(f"  ➜ [115] 网页自动授权成功！Token 已无感保存。")
            
            # 3. 返回一个自动关闭的精美提示页
            html = """
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>授权成功</title>
                <style>
                    body { font-family: system-ui, sans-serif; background: #f0f2f5; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
                    .card { background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; }
                    h2 { color: #18a058; margin-top: 0; }
                    p { color: #666; font-size: 14px; }
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>➜ 授权成功！</h2>
                    <p>Token 已自动保存到 ETK 系统。</p>
                    <p style="color: #999; font-size: 12px;">本窗口将在 3 秒后自动关闭...</p>
                </div>
                <script>
                    setTimeout(function() { window.close(); }, 3000);
                </script>
            </body>
            </html>
            """
            return html
        else:
            return "无效的授权码格式：缺少 token", 400
            
    except Exception as e:
        logger.error(f"自动保存授权码失败: {e}")
        return f"解析授权码失败: {str(e)}", 400

# --- 简单的令牌桶/计数器限流器 ---
class RateLimiter:
    def __init__(self, max_requests=3, period=2):
        self.max_requests = max_requests  # 周期内最大请求数
        self.period = period              # 周期（秒）
        self.tokens = max_requests
        self.last_sync = datetime.now()
        self.lock = threading.Lock()

    def consume(self):
        with self.lock:
            now = datetime.now()
            # 补充令牌
            elapsed = (now - self.last_sync).total_seconds()
            self.tokens = min(self.max_requests, self.tokens + elapsed * (self.max_requests / self.period))
            self.last_sync = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False



def _p115_jsonish_to_obj(value, default=None):
    """宽松解析 JSON/JSONB 字段。"""
    if default is None:
        default = []
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default
    return default


def _p115_human_bytes(value):
    try:
        size = float(value or 0)
    except Exception:
        size = 0.0
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"


def _p115_asset_file_name(asset_details, index=0):
    assets = _p115_jsonish_to_obj(asset_details, [])
    if isinstance(assets, dict):
        assets = [assets]
    if not isinstance(assets, list):
        return ""

    candidates = []
    if 0 <= index < len(assets):
        candidates.append(assets[index])
    candidates.extend(assets)

    for asset in candidates:
        if not isinstance(asset, dict):
            continue
        name = (
            asset.get('file_name') or asset.get('FileName') or
            asset.get('name') or asset.get('Name')
        )
        if name:
            return str(name)
        asset_path = asset.get('path') or asset.get('Path') or asset.get('file_path') or asset.get('FilePath')
        if asset_path:
            return os.path.basename(str(asset_path).replace('\\', '/'))
    return ""


def _p115_pick_speedtest_sample_from_library():
    """从本地媒体库随机挑一个带 115 pick_code 的库内视频。"""
    from database.connection import get_db_connection

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # 优先使用 media_metadata：这才是“媒体库资源”。
            cursor.execute("""
                SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                       season_number, episode_number, file_pickcode_json,
                       file_sha1_json, asset_details_json, date_added
                FROM media_metadata
                WHERE in_library = TRUE
                  AND item_type IN ('Movie', 'Episode')
                  AND file_pickcode_json IS NOT NULL
                  AND jsonb_typeof(file_pickcode_json) = 'array'
                  AND jsonb_array_length(file_pickcode_json) > 0
                ORDER BY RANDOM()
                LIMIT 20
            """)
            rows = cursor.fetchall() or []

            for row in rows:
                pickcodes = _p115_jsonish_to_obj(row.get('file_pickcode_json'), [])
                sha1s = _p115_jsonish_to_obj(row.get('file_sha1_json'), [])
                if not isinstance(pickcodes, list) or not pickcodes:
                    continue

                valid_indexes = [i for i, pc in enumerate(pickcodes) if str(pc or '').strip()]
                if not valid_indexes:
                    continue
                index = random.choice(valid_indexes)
                pick_code = str(pickcodes[index] or '').strip()
                sha1 = str(sha1s[index] or '').strip().upper() if index < len(sha1s) else ''
                file_name = _p115_asset_file_name(row.get('asset_details_json'), index)
                cache_row = None

                if pick_code or sha1:
                    clauses = []
                    params = []
                    if pick_code:
                        clauses.append('pick_code = %s')
                        params.append(pick_code)
                    if sha1:
                        clauses.append('UPPER(sha1) = %s')
                        params.append(sha1)
                    if clauses:
                        cursor.execute(f"""
                            SELECT id, name, pick_code, sha1, size, local_path
                            FROM p115_filesystem_cache
                            WHERE {' OR '.join(clauses)}
                            ORDER BY updated_at DESC NULLS LAST
                            LIMIT 1
                        """, tuple(params))
                        cache_row = cursor.fetchone()

                cache_row = dict(cache_row or {})
                file_name = file_name or cache_row.get('name') or row.get('title') or pick_code
                return {
                    'pick_code': pick_code or str(cache_row.get('pick_code') or '').strip(),
                    'sha1': sha1 or str(cache_row.get('sha1') or '').strip().upper(),
                    'file_name': file_name,
                    'size': int(cache_row.get('size') or 0),
                    'local_path': cache_row.get('local_path') or '',
                    'tmdb_id': row.get('tmdb_id'),
                    'item_type': row.get('item_type'),
                    'title': row.get('title') or '',
                    'parent_series_tmdb_id': row.get('parent_series_tmdb_id') or '',
                    'season_number': row.get('season_number'),
                    'episode_number': row.get('episode_number'),
                }

            # 兜底：如果 media_metadata 还没写 pickcode，就从 115 文件缓存里随机找一个视频文件。
            cursor.execute("""
                SELECT id, name, pick_code, sha1, size, local_path
                FROM p115_filesystem_cache
                WHERE pick_code IS NOT NULL AND pick_code <> ''
                  AND lower(split_part(name, '.', array_length(string_to_array(name, '.'), 1)))
                      IN ('mp4','mkv','avi','ts','iso','rmvb','wmv','mov','m2ts','flv','mpg')
                ORDER BY RANDOM()
                LIMIT 1
            """)
            cache_row = cursor.fetchone()
            if cache_row:
                cache_row = dict(cache_row)
                return {
                    'pick_code': str(cache_row.get('pick_code') or '').strip(),
                    'sha1': str(cache_row.get('sha1') or '').strip().upper(),
                    'file_name': cache_row.get('name') or cache_row.get('pick_code') or '',
                    'size': int(cache_row.get('size') or 0),
                    'local_path': cache_row.get('local_path') or '',
                    'tmdb_id': '',
                    'item_type': '',
                    'title': cache_row.get('name') or '',
                    'season_number': None,
                    'episode_number': None,
                }
    return None


def _p115_resolve_download_url_for_speedtest(client, pick_code, user_agent):
    """按当前 115 API 优先级提取直链，失败自动切到另一套接口。"""
    last_error = ''

    for _ in range(2):
        try:
            real_url, backend = client.resolve_download_url(pick_code, user_agent=user_agent, return_backend=True)
            if real_url:
                return str(real_url), backend, last_error
        except Exception as e:
            last_error = str(e)
            logger.warning(f"  ➜ [115测速] 提取直链异常: {e}")
        time.sleep(0.3)

    return '', '', last_error or '无法提取下载直链'


def _p115_download_speedtest(real_url, *, user_agent, max_bytes=32 * 1024 * 1024, max_seconds=12):
    """对真实 115 直链做小流量下载测速。只读前 max_bytes，避免把整片拖下来。"""
    max_bytes = max(1 * 1024 * 1024, min(int(max_bytes or 0), 128 * 1024 * 1024))
    max_seconds = max(3, min(int(max_seconds or 0), 30))
    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
        'Connection': 'close',
        'Range': f'bytes=0-{max_bytes - 1}',
    }

    downloaded = 0
    first_byte_ms = None
    start = time.monotonic()
    status_code = None
    content_length = None

    with requests.get(real_url, headers=headers, stream=True, timeout=(8, max_seconds + 5), allow_redirects=True) as resp:
        status_code = resp.status_code
        content_length = resp.headers.get('Content-Length')
        if status_code >= 400:
            raise RuntimeError(f"下载测速请求失败，HTTP {status_code}")

        for chunk in resp.iter_content(chunk_size=256 * 1024):
            if not chunk:
                continue
            now = time.monotonic()
            if first_byte_ms is None:
                first_byte_ms = int((now - start) * 1000)
            downloaded += len(chunk)
            if downloaded >= max_bytes or (now - start) >= max_seconds:
                break

    elapsed = max(time.monotonic() - start, 0.001)
    mb_per_second = downloaded / 1024 / 1024 / elapsed
    return {
        'downloaded_bytes': downloaded,
        'downloaded_human': _p115_human_bytes(downloaded),
        'elapsed_seconds': round(elapsed, 2),
        'first_byte_ms': first_byte_ms,
        'mb_per_second': round(mb_per_second, 2),
        'mbps': round(mb_per_second * 8, 2),
        'speed_text': f"{mb_per_second:.2f} MB/s",
        'status_code': status_code,
        'content_length': content_length,
        'range_bytes': max_bytes,
    }

@p115_bp.route('/status', methods=['GET'])
@admin_required
def get_115_status():
    """检查 115 凭证状态 (分别检查 Token 和 Cookie)"""
    try:
        from handler.p115_service import P115Service, get_115_tokens, get_115_ua, get_115_app_label

        token, _, cookie, app_type = get_115_tokens()
        app_type = str(app_type or "web").strip().lower()
        cookie_app_label = get_115_app_label(app_type)
        token = (token or "").strip() 
        cookie = (cookie or "").strip()
        
        result = {
            "has_token": bool(token),
            "has_cookie": bool(cookie),
            "cookie_valid": None,
            "cookie_app_type": app_type if cookie else None,
            "cookie_app_label": cookie_app_label if cookie else None,
            "valid": False,
            "msg": "",
            "user_info": None
        }
        
        # 1. 优先检查 Token (OpenAPI 官方接口，极安全)
        if token:
            openapi_client = P115Service.get_openapi_client()
            if openapi_client:
                try:
                    user_resp = openapi_client.get_user_info()
                    if user_resp and user_resp.get('state'):
                        result["valid"] = True
                        result["msg"] = "Token 有效 (OpenAPI)"
                        result["user_info"] = user_resp.get('data', {})
                        
                        # 如果也有 Cookie，顺便轻量级探测一下 (★ 绝对不初始化 P115Client)
                        if cookie:
                            try:
                                headers = {
                                    "User-Agent": get_115_ua(app_type),
                                    "Cookie": cookie
                                }
                                res = requests.get(
                                    "https://webapi.115.com/files?cid=0&limit=1",
                                    headers=headers,
                                    timeout=5
                                ).json()

                                if res.get("state"):
                                    result["cookie_valid"] = True
                                    result["msg"] = f"Token + Cookie 均有效（{cookie_app_label}）"
                                else:
                                    result["cookie_valid"] = False
                                    result["msg"] = f"Token 有效，但 Cookie 已失效（{cookie_app_label}）"
                            except:
                                result["msg"] = "Token 有效，Cookie 状态未知"
                                
                        return jsonify({"status": "success", "data": result})
                    else:
                        result["msg"] = f"Token 无效: {user_resp.get('message', '未知错误')}"
                except Exception as e:
                    result["msg"] = f"Token 检查异常: {str(e)}"
            else:
                result["msg"] = "Token 初始化失败"
        
        # 2. 如果没有 Token，或者 Token 失效，轻量级检查 Cookie (★ 绝对不初始化 P115Client)
        if cookie and not result.get("user_info"):
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Cookie": cookie
                }
                resp = requests.get("https://webapi.115.com/files?cid=0&limit=1", headers=headers, timeout=10).json()
                if resp.get('state'):
                    result["valid"] = True
                    result["msg"] = "仅配置 Cookie (播放专用)"
                    # Cookie 模式下随便给个标识，防止前端报错
                    result["user_info"] = {"user_name": "Cookie用户(正常)"}
                    return jsonify({"status": "success", "data": result})
                else:
                    result["msg"] = "Cookie 已失效或被风控拦截"
            except Exception as e:
                result["msg"] = f"Cookie 检查失败: {str(e)}"
        
        if not token and not cookie:
            result["msg"] = "未配置任何凭证"
            
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@p115_bp.route('/speedtest', methods=['GET'])
@admin_required
def speedtest_115_download():
    """随机从媒体库选一个 115 资源，提取真实直链后进行小流量下载测速。"""
    try:
        from handler.p115_service import get_115_tokens, get_115_app_label

        token, _, cookie, app_type = get_115_tokens()
        app_type = str(app_type or 'web').strip().lower()
        sample = _p115_pick_speedtest_sample_from_library()
        if not sample or not sample.get('pick_code'):
            return jsonify({
                'status': 'error',
                'message': '媒体库中没有可用于测速的 115 pick_code 资源，请先完成媒体库同步或整理。'
            }), 400

        client = P115Service.get_client()
        if not client:
            return jsonify({'status': 'error', 'message': '115 客户端未初始化，请检查 Token/Cookie 配置'}), 500

        user_agent = (
            request.headers.get('User-Agent')
            or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        )
        # 用浏览器 UA 申请直链并下载，避免 UA 不一致导致 115 防盗链 403。
        browser_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        real_url, backend, last_error = _p115_resolve_download_url_for_speedtest(client, sample.get('pick_code'), browser_ua)
        if not real_url:
            return jsonify({'status': 'error', 'message': f'提取 115 下载直链失败：{last_error}'}), 500

        max_mb = request.args.get('max_mb') or 32
        max_seconds = request.args.get('seconds') or 12
        try:
            max_bytes = int(float(max_mb) * 1024 * 1024)
        except Exception:
            max_bytes = 32 * 1024 * 1024
        try:
            max_seconds = int(float(max_seconds))
        except Exception:
            max_seconds = 12

        speed = _p115_download_speedtest(real_url, user_agent=browser_ua, max_bytes=max_bytes, max_seconds=max_seconds)
        host = ''
        try:
            host = urlparse(real_url).netloc
        except Exception:
            host = ''

        public_sample = dict(sample)
        if public_sample.get('pick_code'):
            pc = str(public_sample.get('pick_code'))
            public_sample['pick_code_masked'] = pc[:4] + '****' + pc[-4:] if len(pc) > 8 else '****'
        public_sample.pop('pick_code', None)

        speed.update({
            'ok': True,
            'backend': backend or '直链',
            'sample': public_sample,
            'host': host,
            'tested_at': datetime.now().isoformat(timespec='seconds'),
        })

        result = {
            'has_token': bool((token or '').strip()),
            'has_cookie': bool((cookie or '').strip()),
            'cookie_app_type': app_type if cookie else None,
            'cookie_app_label': get_115_app_label(app_type) if cookie else None,
            'cookie_valid': None,
            'valid': True,
            'msg': f"测速完成：{speed.get('speed_text')}",
            'user_info': None,
            'speed_test': speed,
        }
        return jsonify({'status': 'success', 'data': result})
    except Exception as e:
        logger.error(f"  ➜ [115测速] 执行失败: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

@p115_bp.route('/dirs', methods=['GET'])
@admin_required
def list_115_directories():
    """获取 115 目录列表 (支持搜索)"""
    client = P115Service.get_client()
    if not client:
        return jsonify({"status": "error", "message": "无法初始化 115 客户端，请检查凭证"}), 500

    try:
        cid = int(request.args.get('cid', 0))
    except:
        cid = 0
        
    search_val = request.args.get('search', '').strip()
    
    try:
        request_payload = {'cid': cid, 'limit': 1000}
        
        # 智能切换 115 底层接口
        if search_val:
            request_payload['search_value'] = search_val
            resp = client.fs_search(request_payload) # 调用专门的搜索接口
        else:
            resp = client.fs_files(request_payload)  # 调用普通的列表接口
        
        if not resp.get('state'):
            return jsonify({"success": False, "message": resp.get('error_msg', resp.get('message', '获取失败'))}), 500
            
        data = resp.get('data', [])
        
        dirs = []
        for item in data:
            # 兼容 OpenAPI / Cookie webapi / appapi 字段差异。
            # OpenAPI 常见：fid/fn/fc/pid；Cookie /files 目录项常见：cid/n/pid，未必有 fid。
            item_type = item.get('fc')
            if item_type is None:
                item_type = item.get('file_category')
            if item_type is None:
                icon = str(item.get('ico') or item.get('icon') or '').lower()
                if icon in ('folder', 'dir', 'directory') or str(item.get('is_dir')).lower() in ('1', 'true'):
                    item_type = '0'
            item_type = str(item_type)
            
            # '0' 代表文件夹
            if item_type == '0':
                dir_id = item.get('fid') or item.get('file_id') or item.get('id') or item.get('cid')
                if dir_id is None:
                    continue
                dirs.append({
                    "id": str(dir_id),
                    "name": item.get('fn') or item.get('file_name') or item.get('n') or item.get('name'),
                    "parent_id": item.get('pid') or item.get('parent_id') or str(cid)
                })
        
        current_name = '根目录'
        if cid != 0 and resp.get('path'):
            last_path = resp.get('path')[-1]
            current_name = (
                last_path.get('file_name') or last_path.get('fn') or
                last_path.get('n') or last_path.get('name') or '未知目录'
            )
                
        return jsonify({
            "success": True, 
            "data": dirs,
            "current": {
                "id": str(cid),
                "name": current_name
            }
        })
        
    except Exception as e:
        logger.error(f"  ➜ [115目录] 获取目录异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/mkdir', methods=['POST'])
@admin_required
def create_115_directory():
    """创建 115 目录"""
    data = request.json
    pid = data.get('pid') or data.get('cid')
    name = data.get('name')
    
    if not name:
        return jsonify({"status": "error", "message": "目录名称不能为空"}), 400
        
    client = P115Service.get_client()
    if not client:
        return jsonify({"status": "error", "message": "无法初始化 115 客户端"}), 500
        
    try:
        resp = client.fs_mkdir(name, pid)
        if resp.get('state'):
            return jsonify({"status": "success", "data": resp})
        else:
            return jsonify({"status": "error", "message": resp.get('error_msg', '创建失败')}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@p115_bp.route('/play_pool', methods=['GET'])
@admin_required
def get_play_pool_config():
    return jsonify({'success': True, 'data': p115_play_pool.get_public_config()})


@p115_bp.route('/play_pool', methods=['POST'])
@admin_required
def save_play_pool_config():
    data = request.json or {}
    result = p115_play_pool.save_pool_settings(data)
    return jsonify({'success': True, 'data': result})


@p115_bp.route('/play_pool/accounts', methods=['POST'])
@admin_required
def add_play_pool_account():
    data = request.json or {}
    if not str(data.get('cookie') or '').strip() and not str(data.get('access_token') or '').strip():
        return jsonify({'success': False, 'message': 'Cookie/OpenAPI 不能为空'}), 400
    item = p115_play_pool.upsert_account(data)
    return jsonify({'success': True, 'data': item})


@p115_bp.route('/play_pool/user-account', methods=['POST'])
@emby_login_required
def save_user_play_pool_account():
    data = request.json or {}
    emby_user_id = session.get('emby_user_id')
    if not emby_user_id:
        return jsonify({'success': False, 'message': '未登录'}), 401
    cookie = str(data.get('cookie') or '').strip()
    access_token = str(data.get('access_token') or '').strip()
    refresh_token = str(data.get('refresh_token') or '').strip()
    existing = p115_play_pool.get_public_account_by_owner('user', emby_user_id) or {}
    if not cookie and not access_token and not existing.get('id'):
        return jsonify({'success': False, 'message': 'Cookie/OpenAPI 不能为空'}), 400
    shared = bool(data.get('shared', False))
    payload = {
        'alias': str(data.get('alias') or session.get('emby_username') or '用户小号').strip() or '用户小号',
        'app_type': str(data.get('app_type') or 'alipaymini').strip() or 'alipaymini',
        'owner_type': 'user',
        'owner_user_id': emby_user_id,
        'shared': shared,
        '_skip_auto_speedtest': True,
    }
    if cookie:
        payload['cookie'] = cookie
        payload['enabled'] = True
    if access_token:
        payload['access_token'] = access_token
        payload['refresh_token'] = refresh_token
        payload['enabled'] = True
    item = p115_play_pool.upsert_account(payload, account_id=existing.get('id') if existing else None)
    reward_result = {}
    if cookie or access_token:
        try:
            result = p115_play_pool.speedtest_account(item['id'])
            item['last_speed_bps'] = result.get('bps', 0)
            item = p115_play_pool._find_account_by_id(item['id']) or item
            reward_result = p115_play_pool._reward_user_cookie(
                item,
                notify_user=False,
                speed_text=result.get('speed_text') or '',
                error_text='' if item.get('enabled') else item.get('last_error') or '测速未达标',
            )
        except Exception as e:
            p115_play_pool.upsert_account({'enabled': False, 'last_error': str(e)}, account_id=item['id'])
            item = p115_play_pool._find_account_by_id(item['id']) or item
            reward_result = p115_play_pool._reward_user_cookie(item, notify_user=False, error_text=str(e))
    item = p115_play_pool._find_account_by_id(item['id']) or item
    public_item = p115_play_pool.get_public_account(item.get('id')) or {}
    if reward_result.get('reward_days'):
        public_item['reward_days'] = reward_result.get('reward_days')
    public_item['reward_summary'] = p115_play_pool.public_user_reward(emby_user_id)
    return jsonify({'success': True, 'data': public_item})


@p115_bp.route('/play_pool/user-account', methods=['GET'])
@emby_login_required
def get_user_play_pool_account():
    emby_user_id = session.get('emby_user_id')
    item = p115_play_pool.get_public_account_by_owner('user', emby_user_id) or {}
    item['reward_summary'] = p115_play_pool.public_user_reward(emby_user_id)
    return jsonify({'success': True, 'data': item})


@p115_bp.route('/play_pool/user-account', methods=['DELETE'])
@emby_login_required
def delete_user_play_pool_account():
    emby_user_id = session.get('emby_user_id')
    item = p115_play_pool.get_public_account_by_owner('user', emby_user_id) or {}
    if not item.get('id'):
        return jsonify({'success': True, 'deleted': False})
    ok = p115_play_pool.delete_account(item['id'])
    return jsonify({'success': True, 'deleted': ok})


@p115_bp.route('/play_pool/user-account/rewards', methods=['GET'])
@emby_login_required
def get_user_play_pool_rewards():
    emby_user_id = session.get('emby_user_id')
    return jsonify({'success': True, 'data': p115_play_pool.public_user_reward(emby_user_id)})


@p115_bp.route('/play_pool/accounts/<account_id>', methods=['PUT'])
@admin_required
def update_play_pool_account(account_id):
    data = request.json or {}
    if not str(data.get('cookie') or '').strip() and not str(data.get('access_token') or '').strip():
        data['_skip_auto_speedtest'] = True
    item = p115_play_pool.upsert_account(data, account_id=account_id)
    return jsonify({'success': True, 'data': item})


@p115_bp.route('/play_pool/accounts/<account_id>', methods=['DELETE'])
@admin_required
def delete_play_pool_account(account_id):
    ok = p115_play_pool.delete_account(account_id)
    return jsonify({'success': ok})


@p115_bp.route('/play_pool/accounts/<account_id>/speedtest', methods=['POST'])
@admin_required
def speedtest_play_pool_account(account_id):
    try:
        browser_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        result = p115_play_pool.speedtest_account(account_id, user_agent=browser_ua)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logger.error(f"  ➜ [小号播放] 小号测速失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': str(e)}), 500


@p115_bp.route('/play_pool/cleanup', methods=['POST'])
@admin_required
def cleanup_play_pool_sessions():
    removed = p115_play_pool.cleanup_expired_sessions()
    return jsonify({'success': True, 'removed': removed})


@p115_bp.route('/temp_dir_config', methods=['GET'])
@admin_required
def get_temp_dir_config():
    from handler.p115_temp_dir import get_temp_dir_config
    return jsonify({'success': True, 'data': get_temp_dir_config()})


@p115_bp.route('/temp_dir_config', methods=['POST'])
@admin_required
def save_temp_dir_config():
    from handler.p115_temp_dir import save_temp_dir_config
    data = request.json or {}
    client = P115Service.get_client()
    if not client:
        return jsonify({'success': False, 'message': '115 客户端未初始化，请先配置 115 Cookie/OpenAPI'}), 500
    try:
        if 'cleanup_cron' in data:
            config = save_temp_dir_config(client, cleanup_cron=data.get('cleanup_cron'))
        else:
            config = save_temp_dir_config(client)
        account_results = p115_play_pool.ensure_all_account_temp_dirs()
        try:
            from scheduler_manager import scheduler_manager
            scheduler_manager.update_p115_temp_dir_cleanup_job()
        except Exception as e:
            logger.debug("  ➜ [115临时目录] 刷新定时清理任务失败: %s", e)
        return jsonify({'success': True, 'data': {**config, 'accounts': account_results}})
    except Exception as e:
        logger.error("  ➜ [115临时目录] 保存临时目录配置失败: %s", e, exc_info=True)
        return jsonify({'success': False, 'message': str(e)}), 500


def _p115_folder_id(item):
    return item.get('fid') or item.get('file_id') or item.get('id') or item.get('cid')


def _p115_folder_name(item):
    return item.get('fn') or item.get('file_name') or item.get('n') or item.get('name')


def _p115_list_child_folders(client, parent_cid):
    resp = client.fs_files({'cid': str(parent_cid), 'limit': 1000})
    if not resp.get('state'):
        raise RuntimeError(resp.get('error_msg') or resp.get('message') or '读取 115 目录失败')
    folders = []
    for item in resp.get('data') or []:
        item_type = item.get('fc')
        if item_type is None:
            item_type = item.get('file_category')
        if item_type is None:
            icon = str(item.get('ico') or item.get('icon') or '').lower()
            if icon in ('folder', 'dir', 'directory') or str(item.get('is_dir')).lower() in ('1', 'true'):
                item_type = '0'
        if str(item_type) != '0':
            continue
        cid = _p115_folder_id(item)
        name = _p115_folder_name(item)
        if cid is not None and name:
            folders.append({'cid': str(cid), 'name': str(name)})
    return folders


def _p115_ensure_folder(client, parent_cid, name):
    name = str(name or '').strip()
    for folder in _p115_list_child_folders(client, parent_cid):
        if folder['name'] == name:
            return {**folder, 'created': False}

    resp = client.fs_mkdir(name, str(parent_cid))
    if not resp.get('state'):
        raise RuntimeError(resp.get('error_msg') or resp.get('message') or f'创建目录失败：{name}')

    cid = resp.get('cid')
    if not cid and isinstance(resp.get('data'), dict):
        cid = _p115_folder_id(resp['data'])
    if cid:
        return {'cid': str(cid), 'name': name, 'created': True}

    for folder in _p115_list_child_folders(client, parent_cid):
        if folder['name'] == name:
            return {**folder, 'created': True}
    raise RuntimeError(f'目录已创建但未能确认 CID：{name}')


def _p115_deploy_sorting_rules(category_dirs):
    base_rules = [
        ('国漫', 'tv', {'genres': [16], 'languages': ['国语', '粤语']}),
        ('日番', 'tv', {'genres': [16], 'languages': ['日语']}),
        ('美漫', 'tv', {'genres': [16]}),
        ('国产片', 'movie', {'languages': ['国语', '粤语']}),
        ('日韩片', 'movie', {'languages': ['日语', '韩语']}),
        ('欧美片', 'movie', {}),
        ('国产剧', 'tv', {'languages': ['国语', '粤语']}),
        ('日韩剧', 'tv', {'languages': ['日语', '韩语']}),
        ('欧美剧', 'tv', {}),
    ]
    rules = []
    for index, (name, media_type, extra) in enumerate(base_rules, start=1):
        folder = category_dirs[name]
        rules.append({
            'id': f'p115_quick_{index}',
            'name': name,
            'cid': folder['cid'],
            'dir_name': name,
            'category_path': folder['path'],
            'enabled': True,
            'match_mode': 'and',
            'media_type': media_type,
            'genres': extra.get('genres', []),
            'countries': [],
            'languages': extra.get('languages', []),
            'studios': [],
            'keywords': [],
            'ratings': [],
            'file_extensions': [],
            'actors': [],
            'watching_status': 'all',
            'year_min': None,
            'year_max': None,
            'runtime_min': None,
            'runtime_max': None,
            'min_rating': 0,
        })
    settings_db.save_setting('p115_sorting_rules', rules)
    return rules


def _replace_washing_priority_groups(groups):
    from database.connection import get_db_connection

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM washing_priority_groups ORDER BY sort_order ASC")
            old_groups = cursor.fetchall() or []
            affected_scope = settings_db.build_washing_priority_recalculate_scope(old_groups, groups)

            cursor.execute(
                "SELECT value_json FROM app_settings WHERE setting_key = %s FOR UPDATE",
                (settings_db.WASHING_PRIORITY_RECALCULATE_SETTING_KEY,),
            )
            state_row = cursor.fetchone()
            current_state = state_row.get('value_json') if state_row else {}
            pending_state = settings_db.merge_washing_priority_recalculate_state(
                current_state,
                affected_scope,
            )

            cursor.execute("TRUNCATE TABLE washing_priority_groups")
            for index, group in enumerate(groups):
                cursor.execute("""
                    INSERT INTO washing_priority_groups (name, media_type, target_cids, priorities, sort_order)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s)
                """, (
                    group['name'],
                    group['media_type'],
                    json.dumps(group.get('target_cids', [])),
                    json.dumps(group.get('priorities', [])),
                    index,
                ))

            if affected_scope:
                settings_db._save_setting_with_cursor(
                    cursor,
                    settings_db.WASHING_PRIORITY_RECALCULATE_SETTING_KEY,
                    pending_state,
                )
        conn.commit()

    return affected_scope, pending_state.get('media_types', {})


def _p115_deploy_washing_groups(category_dirs):
    def base_priorities():
        return [
            {'resolution': ['4k'], 'codec': [], 'effect': [], 'audio': [], 'subtitle': [], 'subtitle_effect': False, 'clean_version': False, 'min_size_gb': None, 'max_size_gb': None, 'is_exclude': False},
            {'resolution': ['1080p'], 'codec': [], 'effect': [], 'audio': [], 'subtitle': [], 'subtitle_effect': False, 'clean_version': False, 'min_size_gb': None, 'max_size_gb': None, 'is_exclude': False},
        ]

    groups = []
    for index, (name, media_type) in enumerate((
        ('国漫', 'Series'), ('日番', 'Series'), ('美漫', 'Series'),
        ('国产片', 'Movie'), ('日韩片', 'Movie'), ('欧美片', 'Movie'),
        ('国产剧', 'Series'), ('日韩剧', 'Series'), ('欧美剧', 'Series'),
    ), start=1):
        root = category_dirs[name]
        groups.append({
            'id': index,
            'name': f'{name}基础洗版',
            'media_type': media_type,
            'target_cids': [root['cid']],
            'priorities': base_priorities(),
        })

    _replace_washing_priority_groups(groups)
    return groups


def _p115_local_path(*parts):
    clean_parts = [str(p).strip('/\\') for p in parts[1:] if str(p or '').strip('/\\')]
    return os.path.normpath(os.path.join(str(parts[0]), *clean_parts))


def _p115_create_local_mirror(local_root, category_dirs):
    if not local_root:
        raise RuntimeError('请先在 Emby 配置页填写本地 STRM 根目录')

    local_dirs = []
    for name in ('动漫', '电影', '剧集'):
        rel_path = category_dirs[name]['path']
        abs_path = _p115_local_path(local_root, rel_path)
        os.makedirs(abs_path, exist_ok=True)
        local_dirs.append({'name': name, 'path': abs_path, 'category_path': rel_path})

    for name in ('国漫', '日番', '美漫', '国产片', '日韩片', '欧美片', '国产剧', '日韩剧', '欧美剧'):
        rel_path = category_dirs[name]['path']
        abs_path = _p115_local_path(local_root, rel_path)
        os.makedirs(abs_path, exist_ok=True)
        local_dirs.append({'name': name, 'path': abs_path, 'category_path': rel_path})
        category_dirs[name]['local_path'] = abs_path

    return local_dirs


def _p115_deploy_emby_libraries(local_root, category_dirs):
    config = get_config()
    base_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    if not base_url or not api_key:
        raise RuntimeError('请先填写 Emby URL 和 API Key 并保存')

    library_defs = [
        ('国漫', 'tvshows'), ('日番', 'tvshows'), ('美漫', 'tvshows'),
        ('国产片', 'movies'), ('日韩片', 'movies'), ('欧美片', 'movies'),
        ('国产剧', 'tvshows'), ('日韩剧', 'tvshows'), ('欧美剧', 'tvshows'),
    ]
    libraries = []
    for name, collection_type in library_defs:
        local_path = category_dirs[name].get('local_path') or _p115_local_path(local_root, category_dirs[name]['path'])
        libraries.append(emby.create_library(base_url, api_key, name, collection_type, local_path))
    return libraries


def _quick_deploy_payload(progress=None):
    def emit(percent, text):
        if progress:
            progress(percent, text)

    client = P115Service.get_client()
    config = get_config()
    missing = []
    if not client:
        missing.append("115 授权")
    if not config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL):
        missing.append("Emby URL")
    service_authorized = config_manager.is_emby_service_authorized()
    if service_authorized:
        service_authorized = emby.test_connection(
            config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
            config.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        ).get('success', False)
    if not service_authorized:
        missing.append("Emby 服务授权")
    elif not is_etk_plugin_installed(
        config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
        config.get(constants.CONFIG_OPTION_EMBY_API_KEY),
    ):
        missing.append("ETK MediaInfo Bridge 插件")
    if not config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT):
        missing.append("本地 STRM 根目录")
    etk_server_url = str(config.get(constants.CONFIG_OPTION_ETK_SERVER_URL) or '').strip()
    if not etk_server_url:
        missing.append("STRM 链接地址")
    elif not etk_server_url.startswith(('http://', 'https://')):
        raise RuntimeError("STRM 链接地址必须以 http:// 或 https:// 开头")
    if missing:
        raise RuntimeError("一键部署前请先配置：" + "、".join(missing))

    emit(5, '正在创建 115 一级目录')
    media_root = _p115_ensure_folder(client, '0', 'ETK媒体库')
    save_root = _p115_ensure_folder(client, '0', 'ETK待整理')
    unrecognized_root = _p115_ensure_folder(client, '0', 'ETK未识别')

    category_dirs = {
        '动漫': _p115_ensure_folder(client, media_root['cid'], '动漫'),
        '电影': _p115_ensure_folder(client, media_root['cid'], '电影'),
        '剧集': _p115_ensure_folder(client, media_root['cid'], '剧集'),
    }
    child_map = {
        '动漫': ['国漫', '日番', '美漫'],
        '电影': ['国产片', '日韩片', '欧美片'],
        '剧集': ['国产剧', '日韩剧', '欧美剧'],
    }
    emit(15, '正在创建 115 二级分类目录')
    for parent_name, child_names in child_map.items():
        parent = category_dirs[parent_name]
        parent['children'] = []
        parent['path'] = parent_name
        for child_name in child_names:
            child = _p115_ensure_folder(client, parent['cid'], child_name)
            child['path'] = f"{parent_name}/{child_name}"
            parent['children'].append(child)
            category_dirs[child_name] = child

    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    emit(30, '正在创建本地 STRM 镜像目录')
    local_dirs = _p115_create_local_mirror(local_root, category_dirs)

    emit(45, '正在写入 115 基础配置')
    monitor_paths = config.get(constants.CONFIG_OPTION_MONITOR_PATHS, [])
    if not isinstance(monitor_paths, list):
        monitor_paths = []
    norm_local_root = os.path.normpath(str(local_root))
    existing_monitor_paths = [str(p) for p in monitor_paths if str(p or '').strip()]
    if norm_local_root not in {os.path.normpath(p) for p in existing_monitor_paths}:
        existing_monitor_paths.append(local_root)

    dynamic_config = {
        constants.CONFIG_OPTION_115_SAVE_PATH_CID: save_root['cid'],
        constants.CONFIG_OPTION_115_SAVE_PATH_NAME: save_root['name'],
        constants.CONFIG_OPTION_115_UNRECOGNIZED_CID: unrecognized_root['cid'],
        constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME: unrecognized_root['name'],
        constants.CONFIG_OPTION_115_MEDIA_ROOT_CID: media_root['cid'],
        constants.CONFIG_OPTION_115_MEDIA_ROOT_NAME: media_root['name'],
        constants.CONFIG_OPTION_115_MP_CLASSIFY: False,
        constants.CONFIG_OPTION_115_API_PRIORITY: config.get(constants.CONFIG_OPTION_115_API_PRIORITY, 'cookie'),
        constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE: config.get(constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE, 10),
        constants.CONFIG_OPTION_115_EXTENSIONS: config.get(constants.CONFIG_OPTION_115_EXTENSIONS, ['mkv', 'mp4', 'iso', 'ts', 'm2ts']),
        constants.CONFIG_OPTION_MONITOR_ENABLED: True,
        constants.CONFIG_OPTION_MONITOR_PATHS: existing_monitor_paths,
    }
    config_manager.save_config(dynamic_config)

    emit(55, '正在写入重命名配置')
    rename_config = {
        'keep_original_name': False,
        'main_dir_template': '{{title}}{% if year %} ({{year}}){% endif %}{% if tmdbid %} {tmdb={{tmdbid}}}{% endif %}',
        'season_dir_template': 'Season {{season_no}}',
        'movie_file_template': '{{title}}{{fileExt}}',
        'tv_file_template': '{{title}}{% if season_episode %} {{season_episode}}{% endif %}{{fileExt}}',
        'file_template': '{{title}}{% if season_episode %} {{season_episode}}{% endif %}{{fileExt}}',
        'main_dir_format': ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'],
        'season_dir_format': ['season_name_en'],
        'file_format': ['title_zh', 'sep_space', 's_e'],
        'file_tmdb_fmt': 'none',
        'video_codec_style': 'hevc',
        'hide_audio_channels': False,
        'strm_url_fmt': 'standard',
    }
    settings_db.save_setting('p115_rename_config', rename_config)
    settings_db.save_washing_priority_config({'conflict_mode': 'replace'})

    emit(65, '正在写入二级分类规则')
    sorting_rules = _p115_deploy_sorting_rules(category_dirs)
    emit(72, '正在写入二级分类洗版规则')
    washing_groups = _p115_deploy_washing_groups(category_dirs)

    emit(82, '正在创建 Emby 媒体库')
    emby_libraries = _p115_deploy_emby_libraries(local_root, category_dirs)
    library_ids = [item['id'] for item in emby_libraries if item.get('id')]
    if library_ids:
        dynamic_config[constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS] = library_ids
        config_manager.save_config({constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS: library_ids})

    tree = {
        'media_root': {**media_root, 'children': [category_dirs['动漫'], category_dirs['电影'], category_dirs['剧集']]},
        'save_root': save_root,
        'unrecognized_root': unrecognized_root,
    }
    emit(100, '一键部署完成')
    return {
        'config': dynamic_config,
        'tree': tree,
        'local_dirs': local_dirs,
        'emby_libraries': emby_libraries,
        'sorting_rules_count': len(sorting_rules),
        'washing_groups_count': len(washing_groups),
        'rename_config': rename_config,
    }


@p115_bp.route('/quick_deploy', methods=['POST'])
@admin_required
def quick_deploy_115():
    """一键部署 115 基础目录、分类、重命名、洗版规则、本地目录和 Emby 媒体库。"""
    if request.args.get('stream') == '1':
        def generate():
            q = Queue()

            def progress(percent, text):
                q.put({'type': 'progress', 'percent': percent, 'message': text})

            def worker():
                try:
                    data = _quick_deploy_payload(progress)
                    q.put({'type': 'done', 'success': True, 'message': '115 网盘基础配置已部署完成', 'data': data})
                except Exception as e:
                    logger.error(f"  ➜ [115一键部署] 执行失败: {e}", exc_info=True)
                    q.put({'type': 'done', 'success': False, 'message': str(e)})

            threading.Thread(target=worker, daemon=True).start()
            while True:
                event = q.get()
                yield json.dumps(event, ensure_ascii=False) + '\n'
                if event.get('type') == 'done':
                    break

        response = Response(stream_with_context(generate()), mimetype='application/x-ndjson')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['X-Accel-Buffering'] = 'no'
        return response

    try:
        data = _quick_deploy_payload()
        return jsonify({
            'success': True,
            'message': '115 网盘基础配置已部署完成',
            'data': data
        })
    except Exception as e:
        logger.error(f"  ➜ [115一键部署] 执行失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/washing_priority_groups', methods=['GET', 'POST'])
@admin_required
def handle_washing_priority_groups():
    """处理 115 洗版优先级规则的增删改查"""
    from database.connection import get_db_connection
    if request.method == 'GET':
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM washing_priority_groups ORDER BY sort_order ASC")
                    return jsonify({"success": True, "data": cursor.fetchall()})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 500
            
    if request.method == 'POST':
        groups = request.json
        if not isinstance(groups, list):
            return jsonify({"success": False, "message": "规则组数据格式错误"}), 400
        try:
            affected_scope, pending_scope = _replace_washing_priority_groups(groups)
            return jsonify({
                "success": True,
                "message": "洗版优先级规则已保存",
                "data": {
                    "affected_scope": affected_scope,
                    "pending_scope": pending_scope,
                },
            })
        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 500


@p115_bp.route('/release_groups', methods=['GET'])
@admin_required
def handle_release_groups():
    """返回洗版优先级可选的发布组标准名。"""
    try:
        from tasks.helpers import get_release_group_names
        options = [
            {"label": str(group), "value": str(group)}
            for group in get_release_group_names()
            if str(group or "").strip()
        ]
        options.sort(key=lambda item: item["label"].lower())
        return jsonify({"success": True, "data": options})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@p115_bp.route('/washing_priority_config', methods=['GET', 'POST'])
@admin_required
def handle_washing_priority_config():
    """处理洗版相关全局配置，兼容从旧重命名配置迁移覆盖模式。"""
    if request.method == 'GET':
        try:
            return jsonify({"success": True, "data": settings_db.get_washing_priority_config()})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 500

    try:
        payload = request.json if isinstance(request.json, dict) else {}
        config = settings_db.save_washing_priority_config(payload)
        return jsonify({"success": True, "message": "洗版配置已保存", "data": config})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ======================================================================
# ★★★ 洗版优先级一键重算 API ★★★
# ======================================================================
@p115_bp.route('/washing_priority_recalculate', methods=['POST'])
@admin_required
def trigger_washing_priority_recalculate():
    """触发任务层：只重算受规则变更影响的媒体项。"""
    payload = request.json or {}
    item_type = payload.get('item_type') or 'all'
    limit = payload.get('limit')
    background = payload.get('background', True)
    force_full = payload.get('full') is True

    try:
        from tasks.p115 import (
            task_recalculate_library_washing_priorities,
            submit_washing_priority_recalculate_task,
        )

        affected_scope = None
        scope_revision = None
        if not force_full:
            pending_state = settings_db.get_washing_priority_recalculate_state()
            affected_scope = pending_state.get('media_types') or {}
            scope_revision = pending_state.get('revision')
            if not affected_scope:
                return jsonify({
                    "success": True,
                    "message": "当前没有受规则变更影响的媒体项",
                    "data": {"scanned_items": 0, "updated_items": 0},
                })

        if background is False or str(background).strip().lower() in ('0', 'false', 'no'):
            result = task_recalculate_library_washing_priorities(
                item_type=item_type,
                limit=limit,
                affected_scope=affected_scope,
                scope_revision=scope_revision,
            )
            return jsonify({"success": True, "message": "洗版优先级重算完成", "data": result})

        submit_washing_priority_recalculate_task(
            item_type=item_type,
            limit=limit,
            affected_scope=affected_scope,
            scope_revision=scope_revision,
        )
        return jsonify({"success": True, "message": "受影响媒体项的优先级重算任务已在后台启动"})
    except Exception as e:
        logger.error(f"  ➜ [洗版优先级重算] 启动失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/sorting_rules', methods=['GET', 'POST'])
@admin_required
def handle_sorting_rules():
    """管理 115 分类规则"""
    if request.method == 'GET':
        raw_rules = settings_db.get_setting('p115_sorting_rules')
        rules = []
        if raw_rules:
            if isinstance(raw_rules, list):
                rules = raw_rules
            elif isinstance(raw_rules, str):
                try:
                    parsed = json.loads(raw_rules)
                    if isinstance(parsed, list):
                        rules = parsed
                except Exception as e:
                    logger.error(f"解析分类规则 JSON 失败: {e}")
        
        # 确保每个规则都有 id
        for r in rules:
            if 'id' not in r:
                r['id'] = str(int(time.time() * 1000))
                
        return jsonify(rules)
    
    if request.method == 'POST':
        rules = request.json
        if not isinstance(rules, list):
            rules = []
            
        # ★ 优化：获取旧规则，用于对比，避免重复请求 115 API
        raw_old_rules = settings_db.get_setting('p115_sorting_rules')
        old_rules_dict = {}
        if raw_old_rules:
            if isinstance(raw_old_rules, list):
                old_rules_dict = {str(r.get('id')): r for r in raw_old_rules if r.get('id')}
            elif isinstance(raw_old_rules, str):
                try:
                    parsed = json.loads(raw_old_rules)
                    if isinstance(parsed, list):
                        old_rules_dict = {str(r.get('id')): r for r in parsed if r.get('id')}
                except Exception:
                    pass
        
        # ★★★ 修复：精准计算基于 p115_media_root_cid 的相对层级路径 ★★★
        client = P115Service.get_client()
        if client:
            config = get_config()
            # 获取用户配置的媒体库根目录 CID
            media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
            media_root_name = config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_NAME)
            
            for rule in rules:
                rule_id = str(rule.get('id', ''))
                cid = rule.get('cid')
                
                # ★ 核心优化：检查是否需要重新计算路径
                need_recalc = True
                if rule_id and rule_id in old_rules_dict:
                    old_rule = old_rules_dict[rule_id]
                    # 如果 cid 没变，且旧的 category_path 存在，则直接复用，跳过网络请求
                    if str(old_rule.get('cid')) == str(cid) and old_rule.get('category_path'):
                        rule['category_path'] = _strip_p115_media_root_prefix(
                            old_rule.get('category_path'),
                            media_root_name,
                        )
                        need_recalc = False
                
                if need_recalc and cid and str(cid) != '0':
                    try:
                        payload = {'cid': cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0}
                        # 顺手修复了原代码中 hasattr 判断可能导致 dir_info 未定义的潜在 Bug
                        dir_info = client.fs_files(payload)
                            
                        path_nodes = dir_info.get('path', [])
                        
                        start_idx = 0
                        found_root = False
                        
                        # 在链路中寻找“媒体库根目录”
                        if media_root_cid == '0':
                            # ★ 修复 0 层级 Bug：115 的根目录永远在 index 0，所以从 1 开始切片是绝对正确的。
                            # 但如果分类目录本身就是根目录，这里需要特殊处理
                            if str(cid) == '0':
                                start_idx = 0
                            else:
                                start_idx = 1 
                            found_root = True
                        else:
                            for i, node in enumerate(path_nodes):
                                if str(node.get('cid')) == media_root_cid:
                                    start_idx = i + 1 # 从根目录的下一级开始取
                                    found_root = True
                                    break
                        
                        if found_root and start_idx < len(path_nodes):
                            # ★ 修复：兼容所有可能的键名，并防止 str(None) 变成 "None"
                            rel_segments = []
                            for n in path_nodes[start_idx:]:
                                node_name = n.get('file_name') or n.get('fn') or n.get('name') or n.get('n')
                                if node_name:
                                    rel_segments.append(str(node_name).strip())
                            
                            rule['category_path'] = "/".join(rel_segments) if rel_segments else rule.get('dir_name', '未识别')
                        else:
                            # 兜底：如果层级异常或没找到根目录，用规则里配的名称
                            rule['category_path'] = rule.get('dir_name', '未识别')
                            
                        logger.info(f"  📂 已为规则 '{rule.get('name')}' 自动计算并保存路径: {rule.get('category_path')}")
                        
                    except Exception as e:
                        logger.warning(f"  ➜ 获取规则 '{rule.get('name')}' 路径失败: {e}")
                        if not rule.get('category_path'):
                            rule['category_path'] = rule.get('dir_name', '')
                elif not need_recalc:
                    # 不需要重新计算，静默跳过
                    pass
                else:
                    # 兜底：没有 cid 或者 cid 为 0
                    if not rule.get('category_path'):
                        rule['category_path'] = rule.get('dir_name', '')
                if rule.get('category_path'):
                    rule['category_path'] = _strip_p115_media_root_prefix(
                        rule.get('category_path'),
                        media_root_name,
                    )
        
        settings_db.save_setting('p115_sorting_rules', rules)
        return jsonify({"status": "success", "message": "115 分类规则已保存"})
    
def _cached_mediainfo_response(sha1):
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-F0-9]{40}', sha1):
        return jsonify({"error": "invalid sha1"}), 400
    text = P115CacheManager.get_mediainfo_cache_text(sha1)
    if not text:
        return jsonify({"error": "media info cache not found"}), 404
    try:
        payload = emby.normalize_etk_mediainfo_payload(text)
    except Exception as e:
        logger.warning("  ➜ [媒体信息桥接] 缓存格式无效: %s -> %s", sha1[:12], e)
        return jsonify({"error": "invalid media info cache"}), 422
    response = jsonify(payload)
    response.headers['Cache-Control'] = 'no-store'
    return response


@p115_bp.route('/mediainfo/<pick_code>', methods=['GET'])
def get_cached_mediainfo_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({"error": "media info cache not found"}), 404
    return _cached_mediainfo_response(row.get('sha1'))


@p115_bp.route('/mediainfo/sha1/<sha1>', methods=['GET'])
def get_cached_mediainfo_by_sha1(sha1):
    return _cached_mediainfo_response(sha1)


def _cached_metadata_response(sha1):
    from database.metadata_provider_db import load_emby_metadata
    from handler.p115_service import _extract_raw_ffprobe_identity

    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-F0-9]{40}', sha1):
        return jsonify({"error": "invalid sha1"}), 400
    identity = _extract_raw_ffprobe_identity(P115CacheManager.get_raw_ffprobe_cache(sha1)) or {}
    tmdb_id = str(identity.get('tmdb_id') or '').strip()
    media_type = str(identity.get('media_type') or '').strip().lower()
    if not tmdb_id or media_type not in {'movie', 'tv'}:
        return jsonify({"error": "metadata identity not found"}), 404

    requested_type = str(request.args.get('item_type') or ('Movie' if media_type == 'movie' else 'Series')).title()
    if requested_type not in {'Movie', 'Series', 'Season', 'Episode'}:
        return jsonify({"error": "invalid item_type"}), 400

    def _number(name, fallback=None):
        value = request.args.get(name)
        if value in (None, ''):
            value = fallback
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    season_number = (
        _number('season_number', identity.get('season_number'))
        if requested_type in {'Season', 'Episode'} else None
    )
    episode_number = (
        _number('episode_number', identity.get('episode_number'))
        if requested_type == 'Episode' else None
    )
    payload = load_emby_metadata(
        tmdb_id,
        media_type,
        requested_type,
        season_number=season_number,
        episode_number=episode_number,
    )
    if not payload:
        return jsonify({"error": "metadata cache not found"}), 404
    from handler.media_image_cache import archive_metadata_images
    archive_metadata_images(payload, request.host_url, archive_sources=False)
    response = jsonify(payload)
    response.headers['Cache-Control'] = 'no-store'
    return response


@p115_bp.route('/mediainfo/<pick_code>/metadata', methods=['GET'])
def get_cached_metadata_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({"error": "metadata cache not found"}), 404
    return _cached_metadata_response(row.get('sha1'))


@p115_bp.route('/mediainfo/sha1/<sha1>/metadata', methods=['GET'])
def get_cached_metadata_by_sha1(sha1):
    return _cached_metadata_response(sha1)


def _trigger_metadata_backfill_response():
    import task_manager
    from tasks.media import task_backfill_media_metadata

    submitted = task_manager.submit_task(
        task_function=task_backfill_media_metadata,
        task_name='补齐媒体元数据',
        processor_type='media',
        silent=True,
    )
    if submitted:
        return jsonify({'ok': True, 'submitted': True}), 202
    status = task_manager.get_task_status()
    if status.get('is_running') and status.get('current_action') == '补齐媒体元数据':
        return jsonify({'ok': True, 'submitted': False, 'reason': 'task_already_running'}), 200
    return jsonify({'ok': False, 'submitted': False, 'reason': 'etk_busy'}), 409


@p115_bp.route('/mediainfo/<pick_code>/metadata/backfill', methods=['POST'])
def trigger_metadata_backfill_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({'error': 'metadata identity not found'}), 404
    return _trigger_metadata_backfill_response()


@p115_bp.route('/mediainfo/sha1/<sha1>/metadata/backfill', methods=['POST'])
def trigger_metadata_backfill_by_sha1(sha1):
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-F0-9]{40}', sha1) or not P115CacheManager.get_mediainfo_cache_text(sha1):
        return jsonify({'error': 'metadata identity not found'}), 404
    return _trigger_metadata_backfill_response()


def _sync_intro_snapshot_response(sha1, expected_pick_code=''):
    from handler.shared_intro_service import sync_intro_from_emby_item

    result = sync_intro_from_emby_item(
        sha1,
        request.args.get('emby_item_id'),
        expected_pick_code=expected_pick_code,
    )
    return jsonify(result)


def _accept_emby_item_response(sha1, expected_pick_code=''):
    from handler.shared_intro_service import get_verified_emby_item_for_cache
    from monitor_service import accept_plugin_emby_binding

    item = get_verified_emby_item_for_cache(
        sha1,
        request.args.get('emby_item_id'),
        expected_pick_code=expected_pick_code,
    )
    if not item:
        return jsonify({'ok': False, 'error': 'item identity mismatch'}), 409
    return jsonify(accept_plugin_emby_binding(
        item,
        sha1=sha1,
        expected_pick_code=expected_pick_code,
    ))


@p115_bp.route('/mediainfo/<pick_code>/intro-sync', methods=['POST'])
def sync_intro_snapshot_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({"error": "media info cache not found"}), 404
    return _sync_intro_snapshot_response(row.get('sha1'), expected_pick_code=pick_code)


@p115_bp.route('/mediainfo/sha1/<sha1>/intro-sync', methods=['POST'])
def sync_intro_snapshot_by_sha1(sha1):
    return _sync_intro_snapshot_response(sha1)


@p115_bp.route('/mediainfo/<pick_code>/item-ready', methods=['POST'])
def accept_emby_item_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({'error': 'media info cache not found'}), 404
    return _accept_emby_item_response(row.get('sha1'), expected_pick_code=pick_code)


@p115_bp.route('/mediainfo/sha1/<sha1>/item-ready', methods=['POST'])
def accept_emby_item_by_sha1(sha1):
    return _accept_emby_item_response(sha1)


def _prepare_deep_delete_response(sha1, expected_pick_code=''):
    from database import media_db
    from handler.shared_intro_service import get_verified_emby_item_for_cache

    payload = request.get_json(silent=True) or {}
    root_item_id = str(payload.get('root_item_id') or '').strip()
    anchor_item_id = str(payload.get('anchor_item_id') or '').strip()
    requested_type = str(payload.get('item_type') or '').strip().title()
    if not re.fullmatch(r'\d{1,20}', root_item_id) or not re.fullmatch(r'\d{1,20}', anchor_item_id):
        return jsonify({'ok': False, 'error': 'invalid item id'}), 400
    if requested_type not in {'Movie', 'Series', 'Season', 'Episode', 'Video'}:
        return jsonify({'ok': False, 'error': 'unsupported item type'}), 400

    anchor = get_verified_emby_item_for_cache(
        sha1,
        anchor_item_id,
        expected_pick_code=expected_pick_code,
    )
    if not anchor:
        return jsonify({'ok': False, 'error': 'anchor identity mismatch'}), 409

    root = emby.get_emby_item_details(
        root_item_id,
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID),
        fields='Type,Name',
        silent_404=True,
    )
    if not root:
        return jsonify({'ok': False, 'error': 'root item not found'}), 404
    actual_type = str(root.get('Type') or requested_type).title()
    if actual_type != requested_type:
        return jsonify({'ok': False, 'error': 'root item type mismatch'}), 409

    virtual_id = _extract_p115_virtual_id_from_emby_item(anchor, sha1)
    if virtual_id:
        now = time.time()
        token = secrets.token_urlsafe(24)
        snapshot = {
            'created_at': now,
            'item_id': root_item_id,
            'item_name': str(root.get('Name') or payload.get('item_name') or root_item_id),
            'item_type': actual_type,
            'series_id': str(root.get('SeriesId') or payload.get('series_id') or '').strip(),
            'pickcodes': [],
            'p115_file_snapshots': [],
            'local_only': True,
            'virtual_id': virtual_id,
        }
        with _deep_delete_lock:
            expired = [
                key for key, value in _deep_delete_snapshots.items()
                if now - float(value.get('created_at') or 0) > _DEEP_DELETE_TTL_SECONDS
            ]
            for key in expired:
                _deep_delete_snapshots.pop(key, None)
            _deep_delete_snapshots[token] = snapshot

        logger.info(
            '  ➜ [ETK 深度删除] 已准备虚拟入库本地善后：%s（类型 %s，EmbyID=%s，VirtualID=%s）。',
            snapshot['item_name'], actual_type, root_item_id, virtual_id,
        )
        return jsonify({
            'ok': True,
            'token': token,
            'pickcode_count': 0,
            'local_only': True,
            'virtual_id': virtual_id,
        })

    pickcodes = media_db.get_pickcodes_for_deleted_emby_item(root_item_id, actual_type)
    anchor_pick_code = str(expected_pick_code or '').strip()
    if not anchor_pick_code:
        cache_row = P115CacheManager.get_file_cache_by_sha1(sha1) or {}
        anchor_pick_code = str(cache_row.get('pick_code') or '').strip()
    if not pickcodes:
        pickcodes = _collect_deep_delete_fallback_pickcodes(root_item_id, actual_type, anchor)
        if not anchor_pick_code and pickcodes:
            anchor_pick_code = pickcodes[0]

    matched_anchor_pick_code = next(
        (pc for pc in pickcodes if pc.lower() == anchor_pick_code.lower()),
        '',
    ) if anchor_pick_code else ''
    if not pickcodes or not matched_anchor_pick_code:
        return jsonify({'ok': False, 'error': 'delete scope identity mismatch'}), 409
    if actual_type in {'Movie', 'Episode', 'Video'}:
        pickcodes = [matched_anchor_pick_code]
        delete_scope = '精确版本'
    else:
        delete_scope = '层级展开'

    now = time.time()
    token = secrets.token_urlsafe(24)
    snapshot = {
        'created_at': now,
        'item_id': root_item_id,
        'item_name': str(root.get('Name') or payload.get('item_name') or root_item_id),
        'item_type': actual_type,
        'series_id': str(root.get('SeriesId') or payload.get('series_id') or '').strip(),
        'pickcodes': pickcodes,
        'p115_file_snapshots': _build_deep_delete_file_snapshots(pickcodes),
    }
    with _deep_delete_lock:
        expired = [
            key for key, value in _deep_delete_snapshots.items()
            if now - float(value.get('created_at') or 0) > _DEEP_DELETE_TTL_SECONDS
        ]
        for key in expired:
            _deep_delete_snapshots.pop(key, None)
        _deep_delete_snapshots[token] = snapshot

    logger.info(
        '  ➜ [ETK 深度删除] 已准备删除：%s（类型=%s，范围=%s，EmbyID=%s，PickCode=%s 个）。',
        snapshot['item_name'], actual_type, delete_scope, root_item_id, len(pickcodes),
    )
    return jsonify({'ok': True, 'token': token, 'pickcode_count': len(pickcodes)})


def _commit_deep_delete_response():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get('token') or '').strip()
    if not token:
        return jsonify({'ok': False, 'error': 'missing token'}), 400
    with _deep_delete_lock:
        snapshot = _deep_delete_snapshots.pop(token, None)
    if not snapshot:
        return jsonify({'ok': False, 'error': 'delete snapshot not found'}), 404
    if time.time() - float(snapshot.get('created_at') or 0) > _DEEP_DELETE_TTL_SECONDS:
        return jsonify({'ok': False, 'error': 'delete snapshot expired'}), 410

    def _process_deep_delete():
        try:
            from database import maintenance_db
            local_only = bool(snapshot.get('local_only'))
            if not local_only:
                from handler.p115_service import delete_115_files_by_webhook

                delete_115_files_by_webhook(
                    snapshot['pickcodes'],
                    file_snapshots=snapshot.get('p115_file_snapshots') or [],
                )
            else:
                logger.info(
                    '  ➜ [ETK 深度删除] 虚拟入库删除仅执行本地善后，跳过 115 网盘联动：EmbyID=%s，VirtualID=%s。',
                    snapshot['item_id'],
                    snapshot.get('virtual_id') or '-',
                )
            maintenance_db.cleanup_deleted_media_item(
                item_id=snapshot['item_id'],
                item_name=snapshot['item_name'],
                item_type=snapshot['item_type'],
                series_id_from_webhook=snapshot.get('series_id') or None,
                local_only=local_only,
            )
        except Exception:
            logger.exception(
                '  ➜ [ETK 深度删除] 处理 Emby 媒体项失败：EmbyID=%s。',
                snapshot['item_id'],
            )

    threading.Thread(
        target=_process_deep_delete,
        name=f"etk-deep-delete-{snapshot['item_id']}",
        daemon=True,
    ).start()
    logger.info(
        '  ➜ [ETK 深度删除] 已确认删除并提交后续处理：%s（类型=%s，EmbyID=%s）。',
        snapshot['item_name'], snapshot['item_type'], snapshot['item_id'],
    )
    return jsonify({
        'ok': True,
        'accepted': True,
        'pickcode_count': len(snapshot['pickcodes']),
        'local_only': bool(snapshot.get('local_only')),
    }), 202


@p115_bp.route('/mediainfo/<pick_code>/deep-delete/prepare', methods=['POST'])
def prepare_deep_delete_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({'error': 'media identity not found'}), 404
    return _prepare_deep_delete_response(row.get('sha1'), expected_pick_code=pick_code)


@p115_bp.route('/mediainfo/sha1/<sha1>/deep-delete/prepare', methods=['POST'])
def prepare_deep_delete_by_sha1(sha1):
    return _prepare_deep_delete_response(sha1)


@p115_bp.route('/mediainfo/<pick_code>/deep-delete/commit', methods=['POST'])
def commit_deep_delete_by_pick_code(pick_code):
    row = P115CacheManager.get_file_cache_by_pickcode(pick_code) or {}
    if not row.get('sha1'):
        return jsonify({'error': 'media identity not found'}), 404
    return _commit_deep_delete_response()


@p115_bp.route('/mediainfo/sha1/<sha1>/deep-delete/commit', methods=['POST'])
def commit_deep_delete_by_sha1(sha1):
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-F0-9]{40}', sha1):
        return jsonify({'error': 'media identity not found'}), 404
    return _commit_deep_delete_response()


@p115_bp.route('/play/<pick_code>', methods=['GET', 'HEAD']) 
@p115_bp.route('/play/<pick_code>/<path:filename>', methods=['GET', 'HEAD'])
def play_115_video(pick_code, filename=None):
    """
    302 直链解析服务
    """
    client_ua = request.headers.get('User-Agent', '')
    client_ua_lower = client_ua.lower()
    
    if request.method == 'HEAD':
        return '', 200

    try:
        # 1. 识别是否为 Emby 服务端 (Probe 或 ffmpeg Remux)
        is_emby_server = False
        # Emby server-side fetches can omit User-Agent. Keep OpenAPI downurl and
        # the follow-up Range GET on the same browser UA to avoid 115 403.
        if not client_ua.strip() or any(kw in client_ua_lower for kw in ['emby', 'jellyfin', 'lavf', 'kodi']):
            is_emby_server = True

        # 2. 决定申请直链使用的 UA
        # 如果是 Emby 服务端，用标准 Chrome UA 伪装骗过 115
        # 如果是真实客户端，必须用客户端自己的 UA，否则 302 后 115 会报 403 防盗链！
        fake_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        request_ua = fake_ua if is_emby_server else client_ua
        
        client = P115Service.get_client()
        if not client:
            return "115 Client not initialized", 500

        real_url = None
        for _ in range(2):
            try:
                real_url = client.resolve_download_url(
                    pick_code,
                    user_agent=request_ua,
                )
                if real_url:
                    break
            except Exception as e:
                logger.warning(f"  ➜ [直链解析] 获取直链异常: {e}")
            
            time.sleep(0.5)
        
        if not real_url:
            return "Failed to get download URL or Rate Limited", 404

        # =================================================================
        # ★★★ 核心分流逻辑 ★★★
        # =================================================================
        if is_emby_server:
            # logger.info(f"  ➜ 检测到 Emby 服务端介入 ({client_ua})，启动中转代理！")
            
            headers_to_115 = {
                "User-Agent": request_ua,
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            if 'Range' in request.headers:
                headers_to_115['Range'] = request.headers['Range']

            resp = requests.get(real_url, headers=headers_to_115, stream=True, timeout=10)
            
            excluded_headers = ['content-encoding', 'transfer-encoding', 'connection', 'host']
            response_headers = [(name, value) for name, value in resp.headers.items() if name.lower() not in excluded_headers]
            
            return Response(stream_with_context(resp.iter_content(chunk_size=8192)), status=resp.status_code, headers=response_headers)

        else:
            # 正常第三方客户端，下发 302，让它自己去连 115！
            # logger.info(f"  ➜ 客户端 ({client_ua})，下发 302 直链！")
            response = redirect(real_url, code=302)
            response.headers['Access-Control-Allow-Origin'] = '*'
            return response
    except Exception as e:
        logger.error(f"  ➜ 直链解析发生异常: {e}")
        return str(e), 500


@p115_bp.route('/replace_strm', methods=['POST'])
@admin_required
def replace_strm_files():
    """遍历本地所有 .strm 文件，执行普通或正则替换"""
    data = request.json
    mode = data.get('mode', 'plain')
    search_str = data.get('search', '')
    replace_str = data.get('replace', '')
    
    if not search_str:
        return jsonify({"success": False, "message": "查找内容不能为空！"}), 400

    config = get_config()
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    
    if not local_root or not os.path.exists(local_root):
        return jsonify({"success": False, "message": "未配置本地 STRM 根目录，或该目录在容器中不存在！"}), 400
        
    fixed_count = 0
    skipped_count = 0
    
    try:
        # 预编译正则以提高性能
        regex_pattern = None
        if mode == 'regex':
            try:
                regex_pattern = re.compile(search_str)
            except Exception as e:
                return jsonify({"success": False, "message": f"正则表达式语法错误: {e}"}), 400

        # 递归遍历整个本地 STRM 目录
        for root_dir, _, files in os.walk(local_root):
            for file in files:
                if file.endswith('.strm'):
                    file_path = os.path.join(root_dir, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read().strip()
                        
                        new_content = content
                        if mode == 'plain':
                            if search_str in content:
                                new_content = content.replace(search_str, replace_str)
                        elif mode == 'regex':
                            new_content = regex_pattern.sub(replace_str, content)
                        
                        if new_content != content:
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(new_content)
                            fixed_count += 1
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        logger.error(f"  ➜ 处理文件 {file_path} 失败: {e}")
        
        msg = f"替换完毕！成功修改了 {fixed_count} 个文件"
        if skipped_count > 0:
            msg += f" (已跳过 {skipped_count} 个未匹配的文件)"
        logger.info(f"  ➜ [批量替换] {msg}")
        return jsonify({"success": True, "message": msg})
        
    except Exception as e:
        logger.error(f"  ➜ 批量替换异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/rename_config', methods=['GET', 'POST'])
@admin_required
def handle_rename_config():
    """管理 115 自定义重命名独立配置"""
    if request.method == 'GET':
        config = settings_db.get_setting('p115_rename_config') or {}
        # 提供默认值，确保前端始终有完整的数据结构
        defaults = {
            "keep_original_name": False,   
            "main_title_lang": "zh",       
            "main_year_en": True,          
            "main_tmdb_fmt": "{tmdb=ID}",  
            "season_fmt": "Season {02}",   
            "main_dir_template": "{{title}}{% if year %} ({{year}}){% endif %} {tmdb={{tmdbid}}}",
            "season_dir_template": "Season {{season_no}}",
            "file_template": "{{title}}{% if year %} ({{year}}){% endif %}{% if season_episode %} · {{season_episode}}{% endif %}{% if resolution %} · {{resolution}}{% endif %}{% if videoCodec %} · {{videoCodec | upper}}{% endif %}{% if audioCodec %} · {{audioCodec}}{% endif %}{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}",
            "movie_file_template": "{{title}}{% if year %} ({{year}}){% endif %}{% if resolution %} · {{resolution}}{% endif %}{% if videoCodec %} · {{videoCodec | upper}}{% endif %}{% if audioCodec %} · {{audioCodec}}{% endif %}{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}",
            "tv_file_template": "{{title}}{% if year %} ({{year}}){% endif %}{% if season_episode %} · {{season_episode}}{% endif %}{% if resolution %} · {{resolution}}{% endif %}{% if videoCodec %} · {{videoCodec | upper}}{% endif %}{% if audioCodec %} · {{audioCodec}}{% endif %}{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}",
            "file_format": ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio', 'sep_middot_space', 'group'],
            "file_tmdb_fmt": "none",       
            "video_codec_style": "hevc",
            "hide_audio_channels": False,
            "strm_url_fmt": "standard",
            "customization": P115RenameRenderer.DEFAULT_CUSTOMIZATION
        }
        defaults.update(config)
        defaults.pop("conflict_mode", None)
        if not config.get("movie_file_template"):
            defaults["movie_file_template"] = config.get("file_template") or defaults["movie_file_template"]
        if not config.get("tv_file_template"):
            defaults["tv_file_template"] = config.get("file_template") or defaults["tv_file_template"]
        if not defaults.get("file_template"):
            defaults["file_template"] = defaults.get("tv_file_template")
        return jsonify({"success": True, "data": defaults})
    
    if request.method == 'POST':
        new_config = request.json if isinstance(request.json, dict) else {}
        legacy_conflict_mode = new_config.pop("conflict_mode", None)
        if legacy_conflict_mode:
            settings_db.save_washing_priority_config({"conflict_mode": legacy_conflict_mode})
        settings_db.save_setting('p115_rename_config', new_config)
        return jsonify({"success": True, "message": "重命名规则已保存"})


def _rename_preview_payload(is_tv=False):
    if is_tv:
        return {
            "details": {
                "title": "绝命毒师",
                "title_en": "Breaking Bad",
                "original_title": "Breaking Bad",
                "date": "2008-01-20",
            },
            "tmdb_id": "1396",
            "original_title": "Breaking Bad",
            "safe_title": "绝命毒师",
            "original_name": "Breaking.Bad.S01E01.2160p.NF.WEB-DL.HDR.60fps.HEVC.Atmos-HHWEB.mp4",
            "season_num": 1,
            "episode_num": 1,
            "file_ext": ".mp4",
            "video_info": {
                "resolution": "2160p",
                "source": "WEB-DL",
                "stream": "friDay",
                "customization": "friDay",
                "effect": "HDR",
                "codec": "HEVC",
                "videoCodec": "HEVC",
                "videoBit": "10bit",
                "audio_count": "2Audios",
                "audio": "Atmos",
                "audioCodec": "Atmos",
                "fps": "60",
                "group": "HHWEB",
            },
        }

    return {
        "details": {
            "title": "寄生虫",
            "title_en": "Parasite",
            "original_title": "기생충",
            "date": "2019-05-30",
        },
        "tmdb_id": "496243",
        "original_title": "Parasite",
        "safe_title": "寄生虫",
        "original_name": "Parasite.2019.60fps.1080p.BluRay.HDR.AVC.DDP5.1-CMCT.mkv",
        "season_num": None,
        "episode_num": None,
        "file_ext": ".mkv",
        "video_info": {
            "resolution": "1080p",
            "source": "BluRay",
            "stream": "60fps",
            "customization": "60fps",
            "effect": "HDR",
            "codec": "AVC",
            "videoCodec": "AVC",
            "videoBit": "8bit",
            "audio_count": "2Audios",
            "audio": "DDP 5.1",
            "audioCodec": "DDP 5.1",
            "fps": "23.976",
            "group": "CMCT",
        },
    }


@p115_bp.route('/rename_config/preview', methods=['POST'])
@admin_required
def preview_rename_config():
    data = request.json if isinstance(request.json, dict) else {}
    config = data.get("config") if isinstance(data.get("config"), dict) else {}
    templates = data.get("templates") if isinstance(data.get("templates"), dict) else {}

    try:
        movie_ctx = _rename_preview_payload(False)
        tv_ctx = _rename_preview_payload(True)
        customization_rules = config.get("customization", P115RenameRenderer.DEFAULT_CUSTOMIZATION)
        for ctx in (movie_ctx, tv_ctx):
            customization = P115RenameRenderer.extract_customization(
                ctx["original_name"], customization_rules
            )
            ctx["video_info"]["customization"] = customization
            ctx["video_info"]["stream"] = customization
        movie_renderer = P115RenameRenderer(
            movie_ctx["details"],
            movie_ctx["tmdb_id"],
            movie_ctx["original_title"],
            config,
        )
        tv_renderer = P115RenameRenderer(
            tv_ctx["details"],
            tv_ctx["tmdb_id"],
            tv_ctx["original_title"],
            config,
        )

        movie_dir = movie_renderer.render_template(
            templates.get("main_dir_template") or "",
            original_title=movie_ctx["original_title"],
            original_name=movie_ctx["original_name"],
            video_info=movie_ctx["video_info"],
            safe_title=movie_ctx["safe_title"],
            file_ext=movie_ctx["file_ext"],
        )
        tv_dir = tv_renderer.render_template(
            templates.get("main_dir_template") or "",
            is_tv=True,
            season_num=tv_ctx["season_num"],
            episode_num=tv_ctx["episode_num"],
            original_title=tv_ctx["original_title"],
            original_name=tv_ctx["original_name"],
            video_info=tv_ctx["video_info"],
            safe_title=tv_ctx["safe_title"],
            file_ext=tv_ctx["file_ext"],
        )
        tv_season = tv_renderer.render_template(
            templates.get("season_dir_template") or "",
            is_tv=True,
            season_num=tv_ctx["season_num"],
            episode_num=tv_ctx["episode_num"],
            original_title=tv_ctx["original_title"],
            original_name=tv_ctx["original_name"],
            video_info=tv_ctx["video_info"],
            safe_title=tv_ctx["safe_title"],
            file_ext=tv_ctx["file_ext"],
        )
        movie_file = movie_ctx["original_name"] if config.get("keep_original_name") else movie_renderer.render_template(
            templates.get("movie_file_template") or "",
            original_title=movie_ctx["original_title"],
            original_name=movie_ctx["original_name"],
            video_info=movie_ctx["video_info"],
            safe_title=movie_ctx["safe_title"],
            file_ext=movie_ctx["file_ext"],
        )
        tv_file = tv_ctx["original_name"] if config.get("keep_original_name") else tv_renderer.render_template(
            templates.get("tv_file_template") or templates.get("file_template") or "",
            is_tv=True,
            season_num=tv_ctx["season_num"],
            episode_num=tv_ctx["episode_num"],
            original_title=tv_ctx["original_title"],
            original_name=tv_ctx["original_name"],
            video_info=tv_ctx["video_info"],
            safe_title=tv_ctx["safe_title"],
            file_ext=tv_ctx["file_ext"],
        )

        return jsonify({
            "success": True,
            "data": {
                "movie_dir": movie_dir or "未配置主目录规则",
                "tv_dir": tv_dir or "未配置主目录规则",
                "tv_season": tv_season or "未配置季目录规则",
                "movie_file": movie_file or movie_ctx["original_name"],
                "tv_file": tv_file or tv_ctx["original_name"],
            }
        })
    except Exception as e:
        logger.warning(f"  ➜ [重命名预览] Jinja2 渲染失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 400
    
def _split_mp_template_from_right(template, separator_count):
    text = str(template or "").strip()
    parts = []
    end = len(text)
    for _ in range(separator_count):
        index = text.rfind("/", 0, end)
        if index < 0:
            return None
        parts.insert(0, text[index + 1:end].strip())
        end = index
    parts.insert(0, text[:end].strip())
    if any(not part for part in parts):
        return None
    return parts


def _join_mp_template(*parts):
    clean_parts = [str(part or "").strip().strip("/") for part in parts]
    if any(not part for part in clean_parts):
        return ""
    return "/".join(clean_parts)


def _ensure_mp_file_ext_template(template):
    text = str(template or "").strip()
    if re.search(r"\b(fileExt|file_ext)\b", text):
        return text
    return f"{text}{{{{fileExt}}}}"


@p115_bp.route('/rename_config/mp/import', methods=['GET'])
@admin_required
def import_rename_config_from_mp():
    ok, templates, error = moviepilot.get_rename_templates(get_config())
    if not ok:
        return jsonify({"success": False, "message": error or "读取 MoviePilot 模板失败"}), 400

    movie_parts = _split_mp_template_from_right(templates.get("movie"), 1)
    tv_parts = _split_mp_template_from_right(templates.get("tv"), 2)
    if not movie_parts:
        return jsonify({"success": False, "message": "MoviePilot 电影模板格式不正确，至少需要 主目录/文件名 两段"}), 400
    if not tv_parts:
        return jsonify({"success": False, "message": "MoviePilot 剧集模板格式不正确，至少需要 主目录/季目录/文件名 三段"}), 400

    main_dir_template = moviepilot.convert_mp_rename_template_to_etk(movie_parts[0])
    season_dir_template = moviepilot.convert_mp_rename_template_to_etk(tv_parts[1])
    movie_file_template = moviepilot.convert_mp_rename_template_to_etk(movie_parts[1])
    tv_file_template = moviepilot.convert_mp_rename_template_to_etk(tv_parts[2])

    return jsonify({
        "success": True,
        "message": "已读取 MoviePilot 重命名模板",
        "data": {
            "main_dir_template": main_dir_template,
            "season_dir_template": season_dir_template,
            "file_template": tv_file_template,
            "movie_file_template": movie_file_template,
            "tv_file_template": tv_file_template,
            "mp_movie_template": templates.get("movie"),
            "mp_tv_template": templates.get("tv"),
        }
    })


@p115_bp.route('/rename_config/mp/export', methods=['POST'])
@admin_required
def export_rename_config_to_mp():
    data = request.json if isinstance(request.json, dict) else {}
    movie_file_template = data.get("movie_file_template") or data.get("file_template")
    tv_file_template = data.get("tv_file_template") or data.get("file_template")
    main_dir_template, main_unsupported = moviepilot.convert_etk_rename_template_to_mp(data.get("main_dir_template"))
    season_dir_template, season_unsupported = moviepilot.convert_etk_rename_template_to_mp(data.get("season_dir_template"))
    movie_file_template, movie_unsupported = moviepilot.convert_etk_rename_template_to_mp(movie_file_template)
    tv_file_template, tv_unsupported = moviepilot.convert_etk_rename_template_to_mp(tv_file_template)
    unsupported = []
    for name in main_unsupported + season_unsupported + movie_unsupported + tv_unsupported:
        if name not in unsupported:
            unsupported.append(name)
    if unsupported:
        unsupported_text = moviepilot.format_mp_unsupported_rename_vars(unsupported)
        return jsonify({
            "success": False,
            "message": f"MoviePilot 不支持这些 ETK 变量：{unsupported_text}。请先从模板里删除或改用 MP 支持的变量。"
        }), 400

    movie_template = _join_mp_template(main_dir_template, _ensure_mp_file_ext_template(movie_file_template))
    tv_template = _join_mp_template(main_dir_template, season_dir_template, _ensure_mp_file_ext_template(tv_file_template))
    if not movie_template or not tv_template:
        return jsonify({"success": False, "message": "主目录、季目录、电影文件名和剧集文件名模板不能为空"}), 400

    ok, error = moviepilot.set_rename_templates(movie_template, tv_template, get_config())
    if not ok:
        return jsonify({"success": False, "message": error or "写入 MoviePilot 模板失败"}), 400

    return jsonify({
        "success": True,
        "message": "已把当前模板写入 MoviePilot",
        "data": {
            "mp_movie_template": movie_template,
            "mp_tv_template": tv_template,
        }
    })


def _normalize_episode_regex_rules(raw_rules):
    if not isinstance(raw_rules, list):
        return [], "规则数据格式错误，必须是数组"

    clean_rules = []

    for i, item in enumerate(raw_rules):
        if not isinstance(item, dict):
            continue

        name = str(item.get('name') or f'规则{i + 1}').strip()
        pattern = str(item.get('pattern') or '').strip()
        mode = str(item.get('mode') or 'episode_only').strip()
        enabled = bool(item.get('enabled', True))

        if not pattern:
            continue

        if mode not in ('season_episode', 'episode_only'):
            return [], f"第 {i + 1} 条规则模式非法: {mode}"

        try:
            re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            return [], f"第 {i + 1} 条规则正则语法错误: {e}"

        try:
            season_group = int(item.get('season_group') or 1)
            episode_group = int(item.get('episode_group') or 1)
            raw_default_season = item.get('default_season')
            default_season = 1 if raw_default_season in (None, '') else int(raw_default_season)
        except Exception:
            return [], f"第 {i + 1} 条规则分组序号或默认季号必须是整数"

        if season_group < 1 or episode_group < 1:
            return [], f"第 {i + 1} 条规则捕获组序号必须 >= 1"

        if default_season < 0:
            return [], f"第 {i + 1} 条规则默认季号不能小于 0"

        clean_rules.append({
            "id": str(item.get('id') or f'episode_regex_{i + 1}'),
            "enabled": enabled,
            "name": name,
            "pattern": pattern,
            "mode": mode,  # season_episode | episode_only
            "season_group": season_group,
            "episode_group": episode_group,
            "default_season": default_season,
        })

    return clean_rules, None

@p115_bp.route('/episode_regex_rules', methods=['GET', 'POST'])
@admin_required
def handle_episode_regex_rules():
    """管理自定义季集号识别正则"""
    setting_key = 'p115_episode_regex_rules'

    if request.method == 'GET':
        rules = settings_db.get_setting(setting_key) or []
        if not isinstance(rules, list):
            rules = []
        return jsonify({"success": True, "data": rules})

    payload = request.json or {}
    raw_rules = payload if isinstance(payload, list) else payload.get('rules', [])

    clean_rules, error = _normalize_episode_regex_rules(raw_rules)
    if error:
        return jsonify({"success": False, "message": error}), 400

    settings_db.save_setting(setting_key, clean_rules)
    return jsonify({
        "success": True,
        "message": "自定义季集号识别规则已保存",
        "data": clean_rules
    })
    
# ======================================================================
# ★★★ 115 整理记录面板 API ★★★
# ======================================================================
from database.connection import get_db_connection

@p115_bp.route('/records', methods=['GET'])
@admin_required
def get_organize_records():
    """获取整理记录列表及统计数据"""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 15))
    search = request.args.get('search', '')
    status = request.args.get('status', 'all')
    cid = request.args.get('cid', '')
    fail_reason = str(request.args.get('fail_reason') or '').strip()
    
    offset = (page - 1) * per_page
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 基础查询条件构建
                where_clauses = []
                params = []
                
                if search:
                    where_clauses.append("(original_name ILIKE %s OR renamed_name ILIKE %s)")
                    params.extend([f"%{search}%", f"%{search}%"])
                
                if status != 'all':
                    where_clauses.append("status = %s")
                    params.append(status)
                    
                if cid:
                    where_clauses.append("target_cid = %s")
                    params.append(str(cid))

                if fail_reason:
                    where_clauses.append("fail_reason = %s")
                    params.append(fail_reason)
                    
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                # 3. 获取总条数
                cursor.execute(f"SELECT COUNT(*) as count FROM p115_organize_records {where_sql}", tuple(params))
                total = cursor.fetchone()['count']
                
                # 4. 获取分页数据
                cursor.execute(f"""
                    SELECT * FROM p115_organize_records 
                    {where_sql} 
                    ORDER BY processed_at DESC 
                    LIMIT %s OFFSET %s
                """, tuple(params + [per_page, offset]))
                items = cursor.fetchall()
                
                # 5. 获取顶部 Dashboard 统计面板数据
                cursor.execute("SELECT COUNT(*) as total FROM p115_organize_records")
                stat_total = cursor.fetchone()['total']
                
                cursor.execute("SELECT COUNT(*) as success FROM p115_organize_records WHERE status = 'success'")
                stat_success = cursor.fetchone()['success']
                
                cursor.execute("SELECT COUNT(*) as unrecognized FROM p115_organize_records WHERE status = 'unrecognized'")
                stat_unrecognized = cursor.fetchone()['unrecognized']
                
                cursor.execute("SELECT COUNT(*) as unqualified FROM p115_organize_records WHERE status = 'unqualified'")
                stat_unqualified = cursor.fetchone()['unqualified']
                
                cursor.execute("SELECT COUNT(*) as this_week FROM p115_organize_records WHERE processed_at >= NOW() - INTERVAL '7 days'")
                stat_week = cursor.fetchone()['this_week']

                cursor.execute("""
                    SELECT fail_reason, COUNT(*) AS count
                    FROM p115_organize_records
                    WHERE NULLIF(BTRIM(fail_reason), '') IS NOT NULL
                    GROUP BY fail_reason
                    ORDER BY count DESC, fail_reason
                """)
                fail_reasons = cursor.fetchall()

                return jsonify({
                    "success": True,
                    "items": items,
                    "total": total,
                    "fail_reasons": fail_reasons,
                    "stats": {
                        "total": stat_total,
                        "success": stat_success,
                        "unrecognized": stat_unrecognized,
                        "unqualified": stat_unqualified,
                        "thisWeek": stat_week
                    }
                })
    except Exception as e:
        logger.error(f"获取整理记录失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/records/<int:record_id>', methods=['DELETE'])
@admin_required
def delete_organize_record(record_id):
    """删除单条整理记录，并同步清理对应的文件和文件夹缓存 (不影响网盘物理文件)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 先查出该记录对应的 file_id 和 target_cid
                cursor.execute("SELECT file_id, target_cid FROM p115_organize_records WHERE id = %s", (record_id,))
                record = cursor.fetchone()
                
                if record:
                    file_id = record['file_id']
                    target_cid = record['target_cid']
                    
                    # 2. 删除整理记录
                    cursor.execute("DELETE FROM p115_organize_records WHERE id = %s", (record_id,))
                    
                    # 3. 同步删除文件本身的缓存
                    if file_id:
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s", (file_id,))
                        
                    # 4. 同步删除目标文件夹的缓存 (连同其下的子项缓存一并清理，促使下次强制从 115 拉取最新状态)
                    if target_cid:
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s OR parent_id = %s", (target_cid, target_cid))
                        
                conn.commit()
        return jsonify({"success": True, "message": "记录及相关目录缓存已清理"})
    except Exception as e:
        logger.error(f"  ➜ 删除整理记录及缓存失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/recycle_bin/empty', methods=['POST'])
@admin_required
def empty_recycle_bin():
    """一键清空 115 回收站"""
    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "115客户端未初始化"}), 500
    try:
        res = client.rb_del() # 不传 tid 即为清空全部
        if res.get('state'):
            return jsonify({"success": True, "message": "回收站已彻底清空！"})
        return jsonify({"success": False, "message": res.get('error_msg', '清空失败')}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/unrecognized/empty', methods=['POST'])
@admin_required
def empty_unrecognized_files():
    """一键清空未识别目录物理文件及本地记录"""
    config = get_config()
    un_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
    
    if not un_cid or str(un_cid) == '0':
        return jsonify({"success": False, "message": "未配置未识别目录，无法执行清空。"})

    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "115客户端未初始化"}), 500

    try:
        # 1. 循环获取未识别目录下的所有文件/文件夹
        offset = 0
        limit = 1000
        fids_to_delete = []
        
        while True:
            res = client.fs_files({'cid': un_cid, 'limit': limit, 'offset': offset, 'record_open_time': 0})
            if not res.get('state'): break
            items = res.get('data', [])
            if not items: break
            
            fids_to_delete.extend([item.get('fid') or item.get('file_id') for item in items])
            if len(items) < limit: break
            offset += limit
            
        # 2. 分批删除网盘物理文件 (防止 URL 过长报错)
        deleted_count = 0
        if fids_to_delete:
            chunk_size = 500
            for i in range(0, len(fids_to_delete), chunk_size):
                chunk = fids_to_delete[i:i+chunk_size]
                del_res = client.fs_delete(chunk)
                if del_res.get('state'):
                    deleted_count += len(chunk)
                else:
                    logger.error(f"  ➜ 清空未识别物理文件失败: {del_res}")
        
        # 3. 清理本地数据库中的未识别记录
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM p115_organize_records WHERE status = 'unrecognized'")
                db_deleted = cursor.rowcount
                conn.commit()
                
        return jsonify({
            "success": True, 
            "message": f"清空完毕！删除了 {deleted_count} 个网盘文件，清理了 {db_deleted} 条本地记录。"
        })
    except Exception as e:
        logger.error(f"  ➜ 清空未识别目录异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/system/directories', methods=['GET'])
@admin_required
def get_local_directories():
    """获取本地服务器/容器内的物理目录列表"""
    path = request.args.get('path', '').strip()
    
    try:
        # 1. 如果没有传路径，返回根目录
        if not path:
            if os.name == 'nt': # Windows 系统，返回盘符
                import string
                from ctypes import windll
                drives = []
                bitmask = windll.kernel32.GetLogicalDrives()
                for letter in string.ascii_uppercase:
                    if bitmask & 1:
                        drives.append({'name': f"{letter}:\\", 'path': f"{letter}:\\", 'is_parent': False})
                    bitmask >>= 1
                return jsonify({'code': 200, 'data': drives, 'current_path': ''})
            else: # Linux/Docker 系统，返回根目录 /
                path = '/'

        # 2. 检查路径是否存在且为目录
        if not os.path.exists(path) or not os.path.isdir(path):
            return jsonify({'code': 404, 'message': '目录不存在或不是文件夹'}), 404

        directories = []
        
        # 3. 添加 "返回上一级" 选项 (如果不在根目录)
        parent_path = os.path.dirname(path)
        if parent_path and parent_path != path:
            directories.append({'name': '.. [返回上一级]', 'path': parent_path, 'is_parent': True})

        # 4. 遍历当前目录下的所有文件夹
        for item in sorted(os.listdir(path)):
            item_path = os.path.join(path, item)
            # 忽略隐藏文件夹和没有权限的文件夹，只显示目录
            if os.path.isdir(item_path) and not item.startswith('.'):
                directories.append({'name': item, 'path': item_path, 'is_parent': False})

        return jsonify({'code': 200, 'data': directories, 'current_path': path})

    except PermissionError:
        return jsonify({'code': 403, 'message': '没有权限访问该目录，请检查 Docker 映射或系统权限'}), 403
    except Exception as e:
        return jsonify({'code': 500, 'message': str(e)}), 500


@p115_bp.route('/upload_monitor/config', methods=['GET', 'POST'])
@admin_required
def handle_upload_monitor_config():
    """独立管理本地目录到 115 的实时分片上传配置。"""
    from handler.p115_upload_monitor import (
        get_upload_monitor_config,
        get_upload_monitor_status,
        save_upload_monitor_config,
    )

    if request.method == 'GET':
        return jsonify({
            'success': True,
            'data': get_upload_monitor_config(),
            'status': get_upload_monitor_status(),
        })

    payload = request.get_json(silent=True) or {}
    raw_mappings = payload.get('mappings') if isinstance(payload.get('mappings'), list) else []
    if len(raw_mappings) > 50:
        return jsonify({'success': False, 'message': '上传监控目录最多配置 50 组'}), 400

    seen_paths = set()
    for index, item in enumerate(raw_mappings, start=1):
        if not isinstance(item, dict):
            return jsonify({'success': False, 'message': f'第 {index} 组目录映射格式无效'}), 400
        local_dir = os.path.normpath(str(item.get('local_dir') or '').strip())
        target_cid = str(item.get('target_cid') or '').strip()
        if not local_dir or not os.path.isdir(local_dir):
            return jsonify({'success': False, 'message': f'第 {index} 组本地目录不存在或不可访问'}), 400
        if not target_cid or target_cid == '0':
            return jsonify({'success': False, 'message': f'第 {index} 组未选择有效的 115 目录'}), 400
        normalized_path = os.path.normcase(os.path.abspath(local_dir))
        if normalized_path in seen_paths:
            return jsonify({'success': False, 'message': f'本地目录重复配置: {local_dir}'}), 400
        seen_paths.add(normalized_path)

    if payload.get('enabled') and not raw_mappings:
        return jsonify({'success': False, 'message': '启用上传监控前至少配置一组目录映射'}), 400

    try:
        config = save_upload_monitor_config(payload)
        from monitor_service import MonitorService
        restarted = MonitorService.restart_active()
        return jsonify({
            'success': True,
            'message': '上传监控配置已保存并生效',
            'data': config,
            'status': get_upload_monitor_status(),
            'monitor_restarted': restarted,
        })
    except Exception as e:
        logger.error(f"  ➜ 保存 115 上传监控配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': str(e)}), 500

@p115_bp.route('/default_stream_config', methods=['GET', 'POST'])
@admin_required
def handle_default_stream_config():
    """管理默认音轨与字幕配置"""

    audio_lang_allowed = {'chi', 'yue', 'original', 'eng', 'jpn', 'kor'}
    subtitle_lang_allowed = {'chs', 'cht', 'original', 'eng', 'jpn', 'kor'}

    defaults = {
        "audio_lang": "",
        "subtitle_lang": "",
        "audio_lang_priority": [],
        "subtitle_lang_priority": [],
        "audio_priority_order": ["param", "feature"],
        "audio_features": ["公映", "上译", "京译", "央视", "长译", "八一", "国配", "台配", "国语", "粤语"],
        "audio_param_priority": ["atmos", "dts_x", "truehd", "dts_hd_ma", "dts_hd_hra", "ddp", "dts", "flac", "ac3", "aac", "7_1", "5_1", "2_0"],
        "sub_priority": ["effect", "chs", "cht", "chs_eng", "cht_eng", "chs_jpn", "cht_jpn", "chs_kor", "cht_kor"],
        # ★ 新增：实时字幕流嗅探开关
        "realtime_sub_detect": False 
    }

    def _clean_priority_list(value, allowed_values, legacy_value=""):
        if isinstance(value, str):
            value = [value] if value.strip() else []
        elif not isinstance(value, list):
            value = []

        result = []
        for item in value:
            item = str(item or "").strip().lower()
            if item and item in allowed_values and item not in result:
                result.append(item)

        legacy_value = str(legacy_value or "").strip().lower()
        if not result and legacy_value and legacy_value in allowed_values:
            result.append(legacy_value)

        return result

    def _normalize_stream_config(raw_config):
        config = defaults.copy()
        if isinstance(raw_config, dict):
            config.update(raw_config)

        config['audio_lang_priority'] = _clean_priority_list(
            config.get('audio_lang_priority'),
            audio_lang_allowed,
            config.get('audio_lang')
        )
        config['subtitle_lang_priority'] = _clean_priority_list(
            config.get('subtitle_lang_priority'),
            subtitle_lang_allowed,
            config.get('subtitle_lang')
        )

        config['audio_lang'] = config['audio_lang_priority'][0] if config['audio_lang_priority'] else ''
        config['subtitle_lang'] = config['subtitle_lang_priority'][0] if config['subtitle_lang_priority'] else ''

        raw_group_order = config.get('audio_priority_order')
        if not isinstance(raw_group_order, list):
            raw_group_order = defaults['audio_priority_order']
        group_order = []
        for item in raw_group_order:
            item = str(item or '').strip().lower()
            if item in ['param', 'feature'] and item not in group_order:
                group_order.append(item)
        for item in defaults['audio_priority_order']:
            if item not in group_order:
                group_order.append(item)
        config['audio_priority_order'] = group_order

        for key in ['audio_features', 'audio_param_priority', 'sub_priority']:
            if not isinstance(config.get(key), list):
                config[key] = defaults[key]
            else:
                seen = set()
                cleaned = []
                for item in config[key]:
                    item = str(item or '').strip()
                    if item and item not in seen:
                        seen.add(item)
                        cleaned.append(item)
                config[key] = cleaned

        # ★ 新增：确保布尔值正确转换
        config['realtime_sub_detect'] = bool(config.get('realtime_sub_detect', False))

        return config

    if request.method == 'GET':
        saved_config = settings_db.get_setting('p115_default_stream_config') or {}
        if not isinstance(saved_config, dict):
            saved_config = {}

        config = _normalize_stream_config(saved_config)
        return jsonify({"success": True, "data": config})

    if request.method == 'POST':
        new_config = request.json or {}
        if not isinstance(new_config, dict):
            new_config = {}

        config = _normalize_stream_config(new_config)
        settings_db.save_setting('p115_default_stream_config', config)
        return jsonify({"success": True, "message": "默认音轨与字幕配置已保存"})
