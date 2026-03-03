# routes/p115.py
import logging
from flask import redirect
import threading
from datetime import datetime, timedelta
import json
import os
import re
import time
import requests
from flask import Blueprint, jsonify, request, redirect
from extensions import admin_required
from database import settings_db
from handler.p115_service import P115Service, get_config
import constants
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
        # 1. 生成 PKCE 密钥对
        verifier, challenge = _generate_pkce_pair()
        
        # 2. 调用获取二维码接口
        url = "https://passportapi.115.com/open/authDeviceCode"
        payload = {
            "client_id": "100196261",  # 115开发者后台的AppID
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
    
# --- ★★★ 新增：经典扫码获取 Cookie 流程 (支持多端) ★★★ ---
_cookie_qrcode_data = {
    "uid": None,
    "time": None,
    "sign": None
}

@p115_bp.route('/cookie_qrcode', methods=['GET'])
@admin_required
def get_cookie_qrcode():
    """获取用于生成 Cookie 的二维码 (支持指定 APP 类型)"""
    app_type = request.args.get('app', 'alipaymini') # 默认支付宝小程序
    try:
        url = f"https://qrcodeapi.115.com/api/1.0/web/1.0/token/?app={app_type}"
        resp = requests.get(url, timeout=10).json()
        
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
        # 1. 轮询状态
        url = f"https://qrcodeapi.115.com/get/status/?uid={uid}&time={time_val}&sign={sign}"
        resp = requests.get(url, timeout=10).json()
        
        state = resp.get('state')
        if state == 0:
            return jsonify({"success": False, "status": "expired", "message": "二维码已过期"})
            
        if state == 1:
            status = resp.get('data', {}).get('status')
            if status == 1:
                return jsonify({"success": True, "status": "waiting", "message": "已扫码，请在手机端确认"})
            elif status == 2:
                # 2. 手机端已确认，调用登录接口换取 Cookie
                login_url = "https://passportapi.115.com/app/1.0/web/1.0/login/qrcode"
                payload = {"account": uid, "app": app_type}
                
                # ★ 关键：必须捕获响应头里的 Set-Cookie
                login_resp = requests.post(login_url, data=payload, timeout=10)
                login_data = login_resp.json()
                
                if login_data.get('state') == 1:
                    # 提取 Cookie
                    cookies_dict = login_resp.cookies.get_dict()
                    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
                    
                    # ★ 保存到独立数据库
                    from handler.p115_service import save_115_tokens
                    save_115_tokens(None, None, cookie_str)
                    
                    # 重置客户端缓存
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
    try:
        from handler.p115_service import save_115_tokens
        save_115_tokens(None, None, cookie_str)
        P115Service.reset_cookie_client()
        return jsonify({"success": True, "message": "Cookie 已保存"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/qrcode', methods=['POST'])
@admin_required
def get_qrcode():
    """获取115登录二维码"""
    data = _generate_qrcode()
    if data:
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
                logger.info(f"  ✅ [115] 扫码成功！Token 已保存。")
                    
            except Exception as e:
                logger.error(f"  ❌ 保存 Token 失败: {e}")
        
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

@p115_bp.route('/status', methods=['GET'])
@admin_required
def get_115_status():
    """检查 115 凭证状态 (分别检查 Token 和 Cookie)"""
    try:
        from handler.p115_service import P115Service, get_115_tokens
        token, _, cookie = get_115_tokens() # ★ 从数据库读
        token = (token or "").strip() 
        cookie = (cookie or "").strip()
        
        result = {
            "has_token": bool(token),
            "has_cookie": bool(cookie),
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
                                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                                    "Cookie": cookie
                                }
                                # 用极轻量的官方目录接口探测 Cookie 存活状态
                                resp = requests.get("https://webapi.115.com/files?cid=0&limit=1", headers=headers, timeout=5).json()
                                if resp.get('state'):
                                    result["msg"] = "Token + Cookie 均有效"
                                else:
                                    result["msg"] = "Token 有效，但 Cookie 已失效！请重新扫码"
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

@p115_bp.route('/dirs', methods=['GET'])
@admin_required
def list_115_directories():
    """获取 115 目录列表"""
    client = P115Service.get_client()
    if not client:
        return jsonify({"status": "error", "message": "无法初始化 115 客户端，请检查凭证"}), 500

    try:
        cid = int(request.args.get('cid', 0))
    except:
        cid = 0
    
    try:
        request_payload = {'cid': cid, 'limit': 1000}
        
        resp = client.fs_files(request_payload)
        
        if not resp.get('state'):
            return jsonify({"success": False, "message": resp.get('error_msg', '获取失败')}), 500
            
        data = resp.get('data', [])
        
        dirs = []
        
        for item in data:
            # 官方文档：fc='0' 代表文件夹
            if str(item.get('fc')) == '0':
                dirs.append({
                    "id": str(item.get('fid')),
                    "name": item.get('fn'),
                    "parent_id": item.get('pid')
                })
        
        current_name = '根目录'
        if cid != 0 and resp.get('path'):
            # path 数组中官方返回的是 file_name
            current_name = resp.get('path')[-1].get('file_name') or resp.get('path')[-1].get('fn', '未知目录')
                
        return jsonify({
            "success": True, 
            "data": dirs,
            "current": {
                "id": str(cid),
                "name": current_name
            }
        })
        
    except Exception as e:
        logger.error(f"  ❌ [115目录] 获取目录异常: {e}", exc_info=True)
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

@p115_bp.route('/sorting_rules', methods=['GET', 'POST'])
@admin_required
def handle_sorting_rules():
    """管理 115 分类规则"""
    if request.method == 'GET':
        raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
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
        
        # ★★★ 修复：精准计算基于 p115_media_root_cid 的相对层级路径 ★★★
        client = P115Service.get_client()
        if client:
            config = get_config()
            # 获取用户配置的媒体库根目录 CID
            media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
            
            for rule in rules:
                cid = rule.get('cid')
                if cid and str(cid) != '0':
                    try:
                        payload = {'cid': cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0}
                        if hasattr(client, 'fs_files_app'):
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
                        logger.warning(f"  ⚠️ 获取规则 '{rule.get('name')}' 路径失败: {e}")
                        if not rule.get('category_path'):
                            rule['category_path'] = rule.get('dir_name', '')
        
        settings_db.save_setting(constants.DB_KEY_115_SORTING_RULES, rules)
        return jsonify({"status": "success", "message": "115 分类规则已保存"})
    

# ★ 收紧限流器，专门对付 Emby 的并发探测 (1秒1次即可，保护 115 账号)
api_limiter = RateLimiter(max_requests=1, period=1)
fetch_lock = threading.Lock()
_url_cache = {}

def _get_cached_115_url(pick_code, user_agent, client_ip=None):
    """
    带缓存的 115 直链获取器 (智能区分真实播放与后台刮削)
    """
    # ★ 恢复 UA 隔离：确保刮削器和播放器获取各自专属的直链，防止 403！
    cache_key = (pick_code, user_agent) 
    now = time.time()
    
    # 1. 先检查缓存及是否过期 (无锁极速读取)
    if cache_key in _url_cache:
        cached_data = _url_cache[cache_key]
        if now < cached_data["expire_at"]:
            return cached_data["url"]
        else:
            del _url_cache[cache_key]
    
    # =================================================================
    # ★ 智能识别 Emby 后台刮削 (Lavf/ffmpeg)
    # =================================================================
    is_scanner = user_agent and 'Lavf' in user_agent
    
    # 如果是后台刮削，且触发了流控，直接瞬间返回 None，绝不阻塞 Flask 线程！
    if is_scanner:
        if not api_limiter.consume():
            return None # 静默拦截，防止 2 万集并发把日志撑爆
    
    client = P115Service.get_client()
    if not client: 
        _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
        return None
    
    # 使用锁：即使并发进来，也只有一个能去查 115 API
    with fetch_lock:
        now = time.time()
        if cache_key in _url_cache and now < _url_cache[cache_key]["expire_at"]:
            return _url_cache[cache_key]["url"]
            
        try:
            time.sleep(0.1) 
            
            url_obj = client.download_url(pick_code, user_agent=user_agent)
            direct_url = str(url_obj) if url_obj else None
            
            if direct_url:
                display_name = pick_code[:8] + "..."
                try:
                    from urllib.parse import urlparse, parse_qs, unquote
                    parsed = urlparse(direct_url)
                    qs = parse_qs(parsed.query)
                    if 'file' in qs: display_name = unquote(qs['file'][0])
                    elif 'filename' in qs: display_name = unquote(qs['filename'][0])
                    else:
                        path_name = unquote(os.path.basename(parsed.path))
                        if path_name: display_name = path_name
                except: pass

                # 定制化日志输出
                if is_scanner:
                    logger.info(f"  🎬 [115直链] 提取媒体信息 -> {display_name}")
                else:
                    logger.info(f"  ▶️ [115直链] 用户正在播放 -> {display_name}")
                
                _url_cache[cache_key] = {"url": direct_url, "name": display_name, "expire_at": now + 7200}
                return direct_url
            else:
                _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
                return None
        except Exception as e:
            logger.error(f"  ❌ 获取 115 直链 API 报错: {e}")
            _url_cache[cache_key] = {"url": None, "name": pick_code, "expire_at": now + 10}
            return None

# 保留原来的 lru_cache 装饰器作为备用（用于 play_115_video 直接调用）
@lru_cache(maxsize=2048)
def _get_cached_115_url_legacy(pick_code, user_agent, client_ip=None):
    """
    带缓存的 115 直链获取器（旧版本，保留兼容性）
    """
    return _get_cached_115_url(pick_code, user_agent, client_ip)

@p115_bp.route('/play/<pick_code>', methods=['GET', 'HEAD']) # 允许 HEAD 请求，加速客户端嗅探
@p115_bp.route('/play/<pick_code>/<path:filename>', methods=['GET', 'HEAD'])
def play_115_video(pick_code, filename=None):
    """
    终极极速 302 直链解析服务 (带内存缓存版)
    """
    if request.method == 'HEAD':
        # HEAD 请求通常是播放器嗅探，直接返回 200 或简单处理，不触发解析
        return '', 200

    try:
        player_ua = request.headers.get('User-Agent', 'Mozilla/5.0')
        
        # 尝试从缓存获取
        real_url = _get_cached_115_url(pick_code, player_ua)
        
        if not real_url:
            # 如果解析太快被拦截了，给播放器返回 429 告知稍后再试
            return "Too Many Requests - 115 API Protection", 429
            
        return redirect(real_url, code=302)
        
    except Exception as e:
        logger.error(f"  ❌ 直链解析发生异常: {e}")
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
                        logger.error(f"  ❌ 处理文件 {file_path} 失败: {e}")
        
        msg = f"替换完毕！成功修改了 {fixed_count} 个文件"
        if skipped_count > 0:
            msg += f" (已跳过 {skipped_count} 个未匹配的文件)"
        logger.info(f"  🧹 [批量替换] {msg}")
        return jsonify({"success": True, "message": msg})
        
    except Exception as e:
        logger.error(f"  ❌ 批量替换异常: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@p115_bp.route('/rename_config', methods=['GET', 'POST'])
@admin_required
def handle_rename_config():
    """管理 115 自定义重命名独立配置"""
    if request.method == 'GET':
        config = settings_db.get_setting(constants.DB_KEY_115_RENAME_CONFIG) or {}
        # 提供默认值，确保前端始终有完整的数据结构
        defaults = {
            "main_title_lang": "zh",       # zh, original
            "main_year_en": True,          # bool
            "main_tmdb_fmt": "{tmdb=ID}",  # {tmdb=ID}, [tmdbid=ID], tmdb-ID, none
            "season_fmt": "Season {02}",   # Season {02}, Season {1}, S{02}, S{1}, 第{1}季
            "file_title_lang": "zh",       # zh, original
            "file_year_en": False,         # bool
            "file_tmdb_fmt": "none",       # {tmdb=ID}, [tmdbid=ID], tmdb-ID, none
            "file_params_en": True,        # bool
            "file_sep": " - "              # " - ", ".", " ", "_"
        }
        defaults.update(config)
        return jsonify({"success": True, "data": defaults})
    
    if request.method == 'POST':
        new_config = request.json
        settings_db.save_setting(constants.DB_KEY_115_RENAME_CONFIG, new_config)
        return jsonify({"success": True, "message": "重命名规则已保存"})