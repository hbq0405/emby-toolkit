# routes/p115.py
import logging
import threading
from datetime import datetime, timedelta
import json
import base64
import os
import re
import time
import requests
from flask import Blueprint, jsonify, request, redirect, Response, stream_with_context, current_app
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
            # ★★★ 核心修复：兼容 fs_files 和 fs_search 的字段名差异 ★★★
            # fs_files 用 fc, fid, fn, pid
            # fs_search 用 file_category, file_id, file_name, parent_id
            
            item_type = str(item.get('fc') if item.get('fc') is not None else item.get('file_category'))
            
            # '0' 代表文件夹
            if item_type == '0':
                dirs.append({
                    "id": str(item.get('fid') or item.get('file_id')),
                    "name": item.get('fn') or item.get('file_name'),
                    "parent_id": item.get('pid') or item.get('parent_id') or str(cid)
                })
        
        current_name = '根目录'
        if cid != 0 and resp.get('path'):
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
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE washing_priority_groups")
                    for i, g in enumerate(groups):
                        cursor.execute("""
                            INSERT INTO washing_priority_groups (name, media_type, target_cids, priorities, sort_order)
                            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s)
                        """, (g['name'], g['media_type'], json.dumps(g.get('target_cids', [])), json.dumps(g.get('priorities', [])), i))
                    conn.commit()
            return jsonify({"success": True, "message": "洗版优先级规则已保存"})
        except Exception as e:
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
            
            for rule in rules:
                rule_id = str(rule.get('id', ''))
                cid = rule.get('cid')
                
                # ★ 核心优化：检查是否需要重新计算路径
                need_recalc = True
                if rule_id and rule_id in old_rules_dict:
                    old_rule = old_rules_dict[rule_id]
                    # 如果 cid 没变，且旧的 category_path 存在，则直接复用，跳过网络请求
                    if str(old_rule.get('cid')) == str(cid) and old_rule.get('category_path'):
                        rule['category_path'] = old_rule.get('category_path')
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
        
        settings_db.save_setting('p115_sorting_rules', rules)
        return jsonify({"status": "success", "message": "115 分类规则已保存"})
    
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
        if any(kw in client_ua_lower for kw in ['emby', 'jellyfin', 'lavf', 'kodi']):
            is_emby_server = True

        # 2. 决定申请直链使用的 UA
        # 如果是 Emby 服务端，用标准 Chrome UA 伪装骗过 115
        # 如果是真实客户端，必须用客户端自己的 UA，否则 302 后 115 会报 403 防盗链！
        fake_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        request_ua = fake_ua if is_emby_server else client_ua
        
        client = P115Service.get_client()
        if not client:
            return "115 Client not initialized", 500
            
        max_retries = 4
        real_url = None
        config = get_config()
        api_priority = config.get(constants.CONFIG_OPTION_115_PLAYBACK_API_PRIORITY, 'openapi')
        use_openapi = (api_priority != 'cookie')
        
        for i in range(max_retries):
            try:
                if use_openapi:
                    real_url = client.openapi_downurl(pick_code, user_agent=request_ua)
                else:
                    real_url = client.download_url(pick_code, user_agent=request_ua)
                    
                if real_url:
                    break
            except Exception as e:
                logger.warning(f"  ➜ [直链解析] {'OpenAPI' if use_openapi else 'Cookie'} 接口异常: {e}")
            
            use_openapi = not use_openapi
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
            "file_format": ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio', 'sep_middot_space', 'group'],
            "file_tmdb_fmt": "none",       
            "strm_url_fmt": "standard"
        }
        defaults.update(config)
        return jsonify({"success": True, "data": defaults})
    
    if request.method == 'POST':
        new_config = request.json
        settings_db.save_setting('p115_rename_config', new_config)
        return jsonify({"success": True, "message": "重命名规则已保存"})
    
@p115_bp.route('/custom_strm_regex', methods=['GET', 'POST'])
@admin_required
def handle_custom_strm_regex():
    """管理自定义 STRM 提取正则"""
    if request.method == 'GET':
        rules = settings_db.get_setting("custom_strm_regex") or []
        return jsonify({"success": True, "data": rules})
    
    if request.method == 'POST':
        data = request.json
        rules = data.get('rules', [])
        # 简单清洗一下空字符串
        clean_rules = [r.strip() for r in rules if r and r.strip()]
        settings_db.save_setting("custom_strm_regex", clean_rules)
        return jsonify({"success": True, "message": "自定义正则已保存"})
    
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
            default_season = int(item.get('default_season') or 1)
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
                
                # 2. 处理命中缓存的筛选
                if status == 'center_cached':
                    where_clauses.append("is_center_cached = TRUE")
                elif status != 'all':
                    where_clauses.append("status = %s")
                    params.append(status)
                    
                if cid:
                    where_clauses.append("target_cid = %s")
                    params.append(str(cid))
                    
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

                # 6. 统计命中中心缓存的数量
                cursor.execute("SELECT COUNT(*) as center_cached FROM p115_organize_records WHERE is_center_cached = TRUE")
                stat_center_cached = cursor.fetchone()['center_cached']

                return jsonify({
                    "success": True,
                    "items": items,
                    "total": total,
                    "stats": {
                        "total": stat_total,
                        "success": stat_success,
                        "unrecognized": stat_unrecognized,
                        "unqualified": stat_unqualified,
                        "thisWeek": stat_week,
                        "center_cached": stat_center_cached
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

@p115_bp.route('/records/correct', methods=['POST'])
@admin_required
def correct_organize_record():
    """手动纠错与重新排盘核心 API"""
    data = request.json
    record_id = data.get('id')
    tmdb_id = data.get('tmdb_id')
    media_type = data.get('media_type')
    target_cid = data.get('target_cid')
    season_num = data.get('season_num')  
    
    if not all([record_id, tmdb_id, media_type, target_cid]):
        return jsonify({"success": False, "message": "缺少必要参数！"}), 400
        
    try:
        from handler.p115_service import manual_correct_organize_record
        manual_correct_organize_record(record_id, tmdb_id, media_type, target_cid, season_num)
        return jsonify({"success": True, "message": "重组完成！网盘与 STRM 已迁移。"})
    except Exception as e:
        logger.error(f"  ➜ 手动重组失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500
    
# ======================================================================
# ★★★ 独立音乐库 API ★★★
# ======================================================================

@p115_bp.route('/music/config', methods=['GET', 'POST'])
@admin_required
def handle_music_config():
    """获取/保存音乐库配置"""
    if request.method == 'GET':
        # ★ 修复：直接从数据库读取，避免 get_config() 缓存不同步
        return jsonify({
            "success": True,
            "data": {
                "p115_music_root_cid": settings_db.get_setting('p115_music_root_cid') or '0',
                "p115_music_root_name": settings_db.get_setting('p115_music_root_name') or ''
            }
        })
    
    if request.method == 'POST':
        data = request.json
        settings_db.save_setting('p115_music_root_cid', data.get('p115_music_root_cid'))
        settings_db.save_setting('p115_music_root_name', data.get('p115_music_root_name'))
        return jsonify({"success": True, "message": "音乐库配置已保存"})

@p115_bp.route('/music/sync', methods=['POST'])
@admin_required
def trigger_music_sync():
    """触发音乐库全量同步"""
    from tasks.p115 import task_sync_music_library
    import task_manager # ★ 引入全局任务管理器
    
    # ★ 核心修复：使用 submit_task 提交任务，这样前端顶部才会弹出进度条！
    task_manager.submit_task(
        task_sync_music_library,
        task_name="全量同步音乐库 STRM",
        processor_type='media'
    )
    
    return jsonify({"success": True, "message": "音乐库同步任务已在后台启动"})

@p115_bp.route('/music/upload', methods=['POST'])
@admin_required
def upload_music_file():
    """上传音乐文件并生成 STRM (附属文件直接存本地，不传网盘)"""
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "没有文件"}), 400
        
    file = request.files['file']
    target_cid = request.form.get('target_cid')
    relative_path = request.form.get('relative_path', '') 
    
    if not target_cid or target_cid == '0':
        return jsonify({"success": False, "message": "未选择上传目标目录"}), 400

    from handler.p115_service import P115Service, P115CacheManager, get_config
    from database import settings_db
    import constants
    import os
    import time

    try:
        # ==========================================
        # 步骤 1：提前判断文件类型与计算本地基础路径
        # ==========================================
        audio_exts = {'mp3', 'flac', 'wav', 'ape', 'm4a', 'aac', 'ogg', 'wma', 'alac'}
        ext = file.filename.split('.')[-1].lower() if '.' in file.filename else ''
        is_audio = ext in audio_exts

        music_root_cid = settings_db.get_setting('p115_music_root_cid')
        music_root_name = settings_db.get_setting('p115_music_root_name') or "音乐库"
        music_root_name = music_root_name.strip('/')
        target_rel_path = ""
        
        # 如果是音频文件，需要用到 client 来查路径；如果是附属文件，尽量不调 API
        client = P115Service.get_client()
        
        if str(target_cid) != str(music_root_cid) and client:
            # ★ 核心修复：并发上传时，115 接口返回 path 可能有延迟或截断，导致单首歌跑飞
            # 优先 1：从本地 DB 缓存中推导相对路径，零延迟且百分百精准
            cached_local_path = P115CacheManager.get_local_path(target_cid)
            if cached_local_path and cached_local_path.replace('\\', '/').startswith(music_root_name):
                rel = cached_local_path.replace('\\', '/')[len(music_root_name):].strip('/')
                if rel: target_rel_path = rel
            
            # 优先 2：如果缓存没命中，再去 115 查，并增加重试机制对抗 115 目录树同步延迟
            if not target_rel_path:
                for retry in range(3):
                    dir_info = client.fs_files({'cid': target_cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    path_nodes = dir_info.get('path', [])
                    
                    start_idx = -1
                    for i, node in enumerate(path_nodes):
                        if str(node.get('cid') or node.get('file_id')) == str(music_root_cid):
                            start_idx = i + 1
                            break
                            
                    if start_idx != -1:
                        sub_folders = [str(p.get('name') or p.get('file_name')).strip() for p in path_nodes[start_idx:]]
                        if sub_folders:
                            target_rel_path = os.path.join(*sub_folders)
                        break
                    else:
                        time.sleep(1) # 115 路径树存在延迟，暂停 1 秒后重查
                        
                # 终极兜底
                if start_idx == -1:
                    target_rel_path = "未分类上传"

        base_local_path = os.path.join(music_root_name, target_rel_path).replace('\\', '/')

        # 提前计算最终的本地绝对路径目录
        config = get_config()
        local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
        local_dir = os.path.join(local_root, base_local_path) if local_root else ""
        
        if relative_path and '/' in relative_path and local_dir:
            clean_path = relative_path.strip('/')
            local_dir = os.path.join(local_dir, os.path.dirname(clean_path))

        # ==========================================
        # ★ 核心优化：如果是附属文件，直接存本地，不走 115
        # ==========================================
        if not is_audio:
            if not local_root:
                return jsonify({"success": False, "message": "未配置本地 STRM 根目录，无法保存附属文件"}), 400
                
            os.makedirs(local_dir, exist_ok=True)
            local_file_path = os.path.join(local_dir, file.filename)
            file.save(local_file_path) # Flask 原生方法直接保存文件
            
            logger.info(f"  ➜ [本地直存] 附属文件已直接保存到本地 STRM 目录: {local_file_path}")
            return jsonify({"success": True, "message": f"{file.filename} 已直接保存到本地"})

        # ==========================================
        # 以下为音频文件的原有逻辑 (走 115 上传)
        # ==========================================
        if not client:
            return jsonify({"success": False, "message": "115 客户端未初始化"}), 500

        # 步骤 2：动态创建拖拽的文件夹并写入缓存
        final_cid = target_cid
        if relative_path and '/' in relative_path:
            clean_path = relative_path.strip('/')
            dir_parts = [p for p in clean_path.split('/')[:-1] if p]
            
            current_pid = target_cid
            current_local_path = base_local_path
            
            for part in dir_parts:
                current_local_path = os.path.join(current_local_path, part).replace('\\', '/')
                
                cached_cid = P115CacheManager.get_cid(current_pid, part)
                if cached_cid:
                    current_pid = cached_cid
                    P115CacheManager.update_local_path(cached_cid, current_local_path)
                    continue
                
                mk_res = client.fs_mkdir(part, current_pid)
                if mk_res.get('state'):
                    new_cid = mk_res.get('cid')
                    P115CacheManager.save_cid(new_cid, current_pid, part)
                    P115CacheManager.update_local_path(new_cid, current_local_path)
                    current_pid = new_cid
                else:
                    found = False
                    for attempt in range(3):
                        search_res = client.fs_files({'cid': current_pid, 'search_value': part, 'limit': 100})
                        for item in search_res.get('data', []):
                            if item.get('fn') == part and str(item.get('fc')) == '0':
                                new_cid = item.get('fid')
                                P115CacheManager.save_cid(new_cid, current_pid, part)
                                P115CacheManager.update_local_path(new_cid, current_local_path)
                                current_pid = new_cid
                                found = True
                                break
                        if found: break
                        time.sleep(1.5)
                        
                    if not found: 
                        raise Exception(f"无法创建或找到目录: {part} (115后端同步延迟)")
            final_cid = current_pid

        # 步骤 3：执行上传
        file_data = file.read()
        file_size = len(file_data)
        file.seek(0) 
        
        upload_res = client.upload_file_stream(file, file.filename, final_cid)
        pick_code = upload_res.get('pick_code')
        file_id = upload_res.get('file_id')
        file_sha1 = upload_res.get('sha1')

        # 步骤 4：生成 STRM 并写入文件缓存
        etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
        
        if local_root and etk_url and pick_code:
            strm_name = os.path.splitext(file.filename)[0] + ".strm"
            os.makedirs(local_dir, exist_ok=True)
            strm_path = os.path.join(local_dir, strm_name)
            
            if not etk_url.startswith('http'):
                rel_p = os.path.relpath(strm_path, local_root)
                content = os.path.join(etk_url, rel_p).replace('\\', '/')
                content = content[:-5] + f".{ext}"
            else:
                content = f"{etk_url}/api/p115/play/{pick_code}/{file.filename}"
                
            with open(strm_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            if file_id:
                rel_dir = os.path.relpath(local_dir, local_root)
                file_local_path = os.path.join(rel_dir, file.filename).replace('\\', '/')
                P115CacheManager.save_file_cache(
                    fid=file_id, parent_id=final_cid, name=file.filename,
                    sha1=file_sha1, pick_code=pick_code,
                    local_path=file_local_path, size=file_size
                )

        return jsonify({"success": True, "message": f"{file.filename} 上传成功"})
    except Exception as e:
        logger.error(f"音乐上传失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500
    
# ======================================================================
# ★★★ 全局清理与回收站 API ★★★
# ======================================================================

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

@p115_bp.route('/default_stream_config', methods=['GET', 'POST'])
@admin_required
def handle_default_stream_config():
    """管理默认音轨与字幕配置"""
    if request.method == 'GET':
        config = settings_db.get_setting('p115_default_stream_config') or {}
        defaults = {
            "audio_lang": "",
            "subtitle_lang": "",
            "audio_features": ["公映", "上译", "京译", "长译", "八一", "台配", "粤语", "评论", "导评"],
            "sub_priority": ["effect", "chs_eng", "cht_eng", "chs", "cht"]
        }
        defaults.update(config)
        return jsonify({"success": True, "data": defaults})
    
    if request.method == 'POST':
        new_config = request.json
        settings_db.save_setting('p115_default_stream_config', new_config)
        return jsonify({"success": True, "message": "默认音轨与字幕配置已保存"})