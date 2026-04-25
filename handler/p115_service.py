# handler/p115_service.py
import logging
import requests
import os
import hashlib
import base64
import hmac    
from email.utils import formatdate 
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from gevent import spawn_later
import time
import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
import handler.tmdb as tmdb
from tasks import helpers
import utils
try:
    from p115client import P115Client
except ImportError:
    P115Client = None

logger = logging.getLogger(__name__)

# 内存级缓存，防止同剧集/同系列疯狂重复请求 TMDb
_TMDB_METADATA_CACHE = {}
_TMDB_SEARCH_CACHE = {}
_AI_PARSE_CACHE = {}
_MP_PARSE_CACHE = {}

# 全局直链缓存池，供反向代理和Web路由共享 
_DIRECT_URL_CACHE = {}

# 全局目录缓存池
_GLOBAL_DIR_CACHE = {}
_GLOBAL_DIR_LOCK = threading.Lock()

def get_115_tokens():
    """唯一真理：只从独立数据库获取 Token 和 Cookie"""
    auth_data = settings_db.get_setting('p115_auth_tokens')
    if auth_data:
        cookie = auth_data.get('cookie')
                
        return auth_data.get('access_token'), auth_data.get('refresh_token'), cookie
    return None, None, None

def save_115_tokens(access_token, refresh_token, cookie=None):
    """唯一真理：只写入独立数据库"""
    existing = settings_db.get_setting('p115_auth_tokens') or {}
    settings_db.save_setting('p115_auth_tokens', {
        'access_token': access_token if access_token is not None else existing.get('access_token'),
        'refresh_token': refresh_token if refresh_token is not None else existing.get('refresh_token'),
        'cookie': cookie if cookie is not None else existing.get('cookie')
    })

_refresh_lock = threading.Lock()

def refresh_115_token(failed_token=None):
    """使用 refresh_token 换取新的 access_token (纯数据库读写)"""
    with _refresh_lock:
        try:
            current_access, current_refresh, _ = get_115_tokens()
            if not current_refresh:
                return False
                
            # ★ 并发防御：如果数据库里的 token 已经和刚才报错的 token 不一样了，说明别的线程刚续期完，直接放行！
            if failed_token and current_access and current_access != failed_token:
                logger.info("  ➜ [115] 检测到 Token 已被其他线程续期，直接放行。")
                if P115Service._openapi_client:
                    P115Service._openapi_client.access_token = current_access
                    P115Service._openapi_client.headers["Authorization"] = f"Bearer {current_access}"
                return True

            url = "https://passportapi.115.com/open/refreshToken"
            payload = {"refresh_token": current_refresh}
            resp = requests.post(url, data=payload, timeout=10).json()
            
            if resp.get('state'):
                new_access_token = resp['data']['access_token']
                new_refresh_token = resp['data']['refresh_token']
                expires_in = resp['data'].get('expires_in', 0)
                hours = round(expires_in / 3600, 1)
                
                # 写入数据库
                save_115_tokens(new_access_token, new_refresh_token)
                
                if P115Service._openapi_client:
                    P115Service._openapi_client.access_token = new_access_token
                    P115Service._openapi_client.headers["Authorization"] = f"Bearer {new_access_token}"
                
                logger.info(f"  ➜ [115] Token 自动续期成功！有效时长 {hours} 小时。")
                return True
            else:
                logger.error(f"  ➜ Token 续期失败: {resp.get('message')}，可能需要重新扫码")
                return False
        except Exception as e:
            logger.error(f"  ➜ Token 续期请求异常: {e}")
            return False

# ======================================================================
# ★★★ 115 OpenAPI 客户端 (仅管理操作：扫描/创建目录/移动文件) ★★★
# ======================================================================
class P115OpenAPIClient:
    """使用 Access Token 进行管理操作"""
    def __init__(self, access_token):
        if not access_token:
            raise ValueError("Access Token 不能为空")
        self.access_token = access_token.strip()
        self.base_url = "https://proapi.115.com"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "Emby-toolkit/1.0 (OpenAPI)"
        }

    def _do_request(self, method, url, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                current_token = self.access_token # 记录当前请求使用的 token
                
                # 支持自定义 headers 覆盖 (用于透传播放器 UA)
                req_headers = self.headers.copy()
                if 'headers' in kwargs:
                    req_headers.update(kwargs.pop('headers'))
                    
                resp = requests.request(method, url, headers=req_headers, timeout=30, **kwargs).json()
                
                if not resp.get("state") and resp.get("code") in [40140123, 40140124, 40140125, 40140126]:
                    logger.warning("  ➜ [115] 检测到 Token 已过期，正在触发自动续期...")
                    
                    # ★ 传入 current_token 进行比对
                    if refresh_115_token(current_token):
                        logger.info("  ➜ [115] 续期完成，重新发送刚才失败的请求...")
                        return requests.request(method, url, headers=self.headers, timeout=30, **kwargs).json()
                    else:
                        logger.error("  ➜ [115] 续期彻底失败，Token 已死亡，请前往 WebUI 重新扫码！")
                
                return resp
            except Exception as e:
                err_str = str(e)
                # ★ 核心修复：遇到 DNS解析失败、连接重置、超时等纯网络错误时，自动休眠重试
                if "NameResolutionError" in err_str or "Connection" in err_str or "Timeout" in err_str:
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                return {"state": False, "error_msg": err_str}

    def get_user_info(self):
        url = f"{self.base_url}/open/user/info"
        return self._do_request("GET", url)

    def fs_files(self, payload):
        url = f"{self.base_url}/open/ufile/files"
        params = {"show_dir": 1, "limit": 1000, "offset": 0}
        if isinstance(payload, dict): params.update(payload)
        return self._do_request("GET", url, params=params)

    def fs_files_app(self, payload): 
        return self.fs_files(payload)
    
    def fs_search(self, payload):
        url = f"{self.base_url}/open/ufile/search"
        params = {"limit": 100, "offset": 0}
        if isinstance(payload, dict): params.update(payload)
        return self._do_request("GET", url, params=params)
    
    def fs_downurl(self, pick_code, user_agent=None):
        """OpenAPI 获取下载直链"""
        url = f"{self.base_url}/open/ufile/downurl"
        headers = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        return self._do_request("POST", url, data={"pick_code": str(pick_code)}, headers=headers)

    def fs_get_info(self, file_id):
        url = f"{self.base_url}/open/folder/get_info"
        return self._do_request("GET", url, params={"file_id": str(file_id)})

    def fs_mkdir(self, name, pid):
        url = f"{self.base_url}/open/folder/add"
        resp = self._do_request("POST", url, data={"pid": str(pid), "file_name": str(name)})
        if resp.get("state") and "data" in resp: 
            resp["cid"] = resp["data"].get("file_id")
        return resp

    def fs_move(self, fids, to_cid):
        url = f"{self.base_url}/open/ufile/move"
        # ★ 支持传入列表，自动用逗号拼接
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_ids": fids_str, "to_cid": str(to_cid)})

    def fs_rename(self, fid_name_tuple):
        url = f"{self.base_url}/open/ufile/update"
        return self._do_request("POST", url, data={"file_id": str(fid_name_tuple[0]), "file_name": str(fid_name_tuple[1])})

    def fs_delete(self, fids):
        url = f"{self.base_url}/open/ufile/delete"
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_ids": fids_str})

    def rb_del(self, tids=None):
        url = f"{self.base_url}/open/rb/del"
        data = {}
        if tids:
            data['tid'] = ",".join([str(t) for t in tids]) if isinstance(tids, list) else str(tids)
        return self._do_request("POST", url, data=data)
    
    def fs_upload_init(self, file_name, file_size, target_cid, sha1, preid, sign_key=None, sign_val=None):
        """文件上传初始化调度接口"""
        url = f"{self.base_url}/open/upload/init"
        data = {
            "file_name": file_name,
            "file_size": file_size,
            "target": f"U_1_{target_cid}",
            "fileid": sha1,
            "preid": preid
        }
        if sign_key and sign_val:
            data["sign_key"] = sign_key
            data["sign_val"] = sign_val
        return self._do_request("POST", url, data=data)

    def fs_upload_get_token(self):
        """获取上传凭证"""
        url = f"{self.base_url}/open/upload/get_token"
        return self._do_request("GET", url)

    def upload_file_stream(self, file_stream, file_name, target_cid):
        """
        完整的文件上传流程 (支持秒传、二次认证、OSS直传带签名与网络容错)
        """
        import urllib.parse 
        import json # ★ 确保引入 json
        
        file_data = file_stream.read()
        file_size = len(file_data)
        
        sha1_obj = hashlib.sha1()
        sha1_obj.update(file_data)
        file_sha1 = sha1_obj.hexdigest().upper()
        
        pre_sha1_obj = hashlib.sha1()
        pre_sha1_obj.update(file_data[:131072]) 
        preid = pre_sha1_obj.hexdigest().upper()
        
        init_res = self.fs_upload_init(file_name, file_size, target_cid, file_sha1, preid)
        
        if init_res.get('state') and init_res.get('data', {}).get('status') == 7:
            sign_key = init_res['data']['sign_key']
            sign_check = init_res['data']['sign_check']
            start, end = map(int, sign_check.split('-'))
            chunk = file_data[start:end+1]
            
            chunk_sha1 = hashlib.sha1()
            chunk_sha1.update(chunk)
            sign_val = chunk_sha1.hexdigest().upper()
            
            time.sleep(0.5) 
            init_res = self.fs_upload_init(file_name, file_size, target_cid, file_sha1, preid, sign_key, sign_val)
            
        if not init_res.get('state'):
            raise Exception(f"上传初始化失败: {init_res.get('message')}")
            
        status = init_res['data'].get('status')
        
        if status == 2:
            return init_res['data']
            
        if status == 1:
            time.sleep(0.5) 
            token_res = self.fs_upload_get_token()
            if not token_res.get('state'):
                raise Exception("获取上传凭证失败")
                
            t_data = token_res['data']
            
            raw_endpoint = t_data['endpoint'].replace('http://', '').replace('https://', '')
            clean_endpoint = raw_endpoint.replace('-internal', '')
            
            bucket = init_res['data']['bucket']
            object_key = init_res['data']['object'].lstrip('/')
            callback_data = init_res['data'].get('callback', {})
            
            encoded_object_key = urllib.parse.quote(object_key, safe='/')
            
            if 'aliyuncs.com' in clean_endpoint:
                upload_url = f"https://{bucket}.{clean_endpoint}/{encoded_object_key}"
            else:
                upload_url = f"https://{clean_endpoint}/{encoded_object_key}"
            
            date_gmt = formatdate(None, usegmt=True)
            content_type = "application/octet-stream"
            
            headers = {
                "Date": date_gmt,
                "Content-Type": content_type,
                "x-oss-security-token": t_data['SecurityToken'],
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            
            # ==========================================
            # ★ 核心修复：将 callback 转换为 Base64 编码
            # ==========================================
            def _encode_cb(val):
                if isinstance(val, dict):
                    val = json.dumps(val, separators=(',', ':'))
                return base64.b64encode(val.encode('utf-8') if isinstance(val, str) else val).decode('utf-8')

            if 'callback' in callback_data:
                headers["x-oss-callback"] = _encode_cb(callback_data['callback'])
            if 'callback_var' in callback_data:
                headers["x-oss-callback-var"] = _encode_cb(callback_data['callback_var'])
            
            # 计算签名
            oss_headers = {k.lower(): v for k, v in headers.items() if k.lower().startswith('x-oss-')}
            canonicalized_oss_headers = ""
            for k in sorted(oss_headers.keys()):
                canonicalized_oss_headers += f"{k}:{oss_headers[k]}\n"
                
            canonicalized_resource = f"/{bucket}/{object_key}"
            string_to_sign = f"PUT\n\n{content_type}\n{date_gmt}\n{canonicalized_oss_headers}{canonicalized_resource}"
            
            h = hmac.new(t_data['AccessKeySecret'].encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha1)
            signature = base64.b64encode(h.digest()).decode('utf-8')
            
            headers["Authorization"] = f"OSS {t_data['AccessKeyId']}:{signature}"
            
            try:
                oss_res = requests.put(upload_url, data=file_data, headers=headers, timeout=300)
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"  ➜ HTTPS 握手失败，尝试降级为 HTTP 上传... ({e})")
                upload_url_http = upload_url.replace('https://', 'http://')
                oss_res = requests.put(upload_url_http, data=file_data, headers=headers, timeout=300)
            
            try:
                oss_res_data = oss_res.json()
            except Exception:
                raise Exception(f"OSS上传失败，返回非JSON数据: {oss_res.text}")
                
            if oss_res_data.get('state') or oss_res_data.get('code') == 200:
                # 115 的 callback 返回结构可能略有不同，只要有 state=True 或 code=200 就算成功
                return oss_res_data.get('data', oss_res_data)
            else:
                raise Exception(f"OSS上传失败: {oss_res_data}")
                
        raise Exception(f"未知的上传状态: {status}")


# ======================================================================
# ★★★ 115 Cookie 客户端 (仅播放：获取直链) ★★★
# ======================================================================
class P115CookieClient:
    """使用 Cookie 进行播放操作"""
    def __init__(self, cookie_str):
        if not cookie_str:
            raise ValueError("Cookie 不能为空")
        self.cookie_str = cookie_str.strip()
        self.webapi = None
        if P115Client:
            try:
                self.webapi = P115Client(self.cookie_str)
            except Exception as e:
                logger.warning(f"  ➜ Cookie 客户端初始化失败: {e}")
                raise

    def download_url(self, pick_code, user_agent=None):
        """获取直链 (仅 Cookie 可用)"""
        if self.webapi:
            url_obj = self.webapi.download_url(pick_code, user_agent=user_agent)
            if url_obj: return str(url_obj)
        return None

    def get_user_info(self):
        """获取用户信息 (仅用于验证)"""
        if self.webapi:
            try:
                # Cookie 模式获取用户信息的方式有限
                return {"state": True, "data": {"user_name": "Cookie用户"}}
            except:
                pass
        return None
    
    def request(self, url, method='GET', **kwargs):
        if self.webapi and hasattr(self.webapi, 'request'):
            return self.webapi.request(url, method=method, **kwargs)
        
        # 兜底：使用 requests 手动发请求
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Cookie": self.cookie_str
        }
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']
        
        return requests.request(method, url, headers=headers, **kwargs)

    def offline_add_urls(self, payload):
        if self.webapi and hasattr(self.webapi, 'offline_add_urls'):
            return self.webapi.offline_add_urls(payload)
        
        # 兜底：手动调用离线接口
        url = "https://115.com/web/lixian/?ct=lixian&ac=add_task_urls"
        r = self.request(url, method='POST', data=payload)
        return r.json() if hasattr(r, 'json') else r

    def share_import(self, share_code, receive_code, cid):
        # 放弃调用第三方库的 share_receive，直接使用最稳妥的官方原生 API
        # 官方接口完美支持直接传入 cid 保存到指定目录
        url = "https://webapi.115.com/share/receive"
        payload = {'share_code': share_code, 'receive_code': receive_code, 'cid': cid}
        r = self.request(url, method='POST', data=payload)
        return r.json() if hasattr(r, 'json') else r
    
    def life_batch_delete(self, delete_data_list):
        url = "https://life.115.com/api/1.0/web/1.0/life/life_batch_delete"
        # 115 要求 delete_data 是一个 JSON 字符串
        payload = {"delete_data": json.dumps(delete_data_list)}
        r = self.request(url, method='POST', data=payload)
        return r.json() if hasattr(r, 'json') else r
    
    def life_behavior_detail(self, payload=None):
        # ★ 彻底抛弃第三方库，直接用原生 requests，加上严格的 timeout 防止卡死！
        url = "https://webapi.115.com/behavior/detail"
        params = {"limit": 100, "offset": 0}
        if isinstance(payload, dict): 
            params.update(payload)
        
        # 强制加上 timeout=15，如果 15 秒没响应直接报错，绝不卡死线程
        r = self.request(url, method='GET', params=params, timeout=15)
        return r.json() if hasattr(r, 'json') else r


# ======================================================================
# ★★★ 115 服务管理器 (分离管理/播放客户端 + 延迟初始化) ★★★
# ======================================================================
class P115Service:
    """统一管理 OpenAPI 和 Cookie 客户端"""
    _instance = None
    _lock = threading.Lock()
    _rate_limit_lock = threading.Lock() # 专用于 API 流控的锁
    _downurl_lock = threading.Lock() # 直链专用锁
    # 移动接口的绝对互斥锁
    _move_lock = threading.Lock()
    _last_move_time = 0
    
    # 客户端缓存
    _openapi_client = None
    _cookie_client = None
    _token_cache = None
    _cookie_cache = None
    
    _last_request_time = 0
    _last_downurl_time = 0 # 直链专用时间戳

    @classmethod
    def get_openapi_client(cls):
        """获取管理客户端 (OpenAPI) - 启动时初始化"""
        token, _, _ = get_115_tokens()
        if not token:
            return None

        with cls._lock:
            # 如果 client 不存在，或者 token 变了，重新初始化
            if cls._openapi_client is None or getattr(cls._openapi_client, 'access_token', None) != token:
                try:
                    cls._openapi_client = P115OpenAPIClient(token)
                    logger.info("  ➜ [115] OpenAPI 客户端已初始化")
                except Exception as e:
                    logger.error(f"  ➜ 115 OpenAPI 客户端初始化失败: {e}")
                    cls._openapi_client = None
            
            return cls._openapi_client

    @classmethod
    def init_cookie_client(cls):
        """初始化 Cookie 客户端 (延迟到播放请求时)"""
        _, _, cookie = get_115_tokens() # ★ 从数据库读
        cookie = (cookie or "").strip()
        
        if not cookie:
            return None

        with cls._lock:
            # 双重检查：检查配置是否变化
            if cls._cookie_client is None or cookie != cls._cookie_cache:
                try:
                    cls._cookie_client = P115CookieClient(cookie)
                    cls._cookie_cache = cookie
                    logger.info("  ➜ [115] Cookie 客户端已初始化")
                except Exception as e:
                    logger.error(f"  ➜ 115 Cookie 客户端初始化失败: {e}")
                    cls._cookie_client = None
            
            return cls._cookie_client

    @classmethod
    def get_cookie_client(cls):
        """获取播放客户端 (Cookie) - 延迟初始化，失败时重试"""
        # 如果已经初始化过，直接返回
        if cls._cookie_client is not None:
            return cls._cookie_client
        
        # 未初始化，尝试初始化（可能容器重启后首次调用）
        return cls.init_cookie_client()
    
    @classmethod
    def reset_cookie_client(cls):
        """重置 Cookie 客户端 (当检测到失效时调用)"""
        with cls._lock:
            cls._cookie_client = None
            cls._cookie_cache = None
            logger.info("  ➜ [115] Cookie 客户端已重置，下次请求将重新初始化")

    @classmethod
    def get_client(cls):
        """
        获取严格分离客户端：
        管理操作 -> 强制走 OpenAPI
        播放操作 -> 强制走 Cookie
        """
        openapi = cls.get_openapi_client()
        cookie = cls.get_cookie_client()
        
        if not openapi and not cookie:
            return None

        class StrictSplitClient:
            def __init__(self, openapi_client, cookie_client):
                self._openapi = openapi_client
                self._cookie = cookie_client

            @property
            def raw_client(self):
                """暴露底层原生 P115Client 供极速遍历使用"""
                if self._cookie and hasattr(self._cookie, 'webapi'):
                    return self._cookie.webapi
                return None

            def _check_openapi(self):
                if not self._openapi:
                    raise Exception("未配置 115 Token (OpenAPI)，无法执行管理操作")

            def _rate_limit(self):
                """底层统一 API 流控拦截器 """
                try:
                    interval = float(get_config().get(constants.CONFIG_OPTION_115_INTERVAL, 1.5))
                    if interval < 1.5:
                        interval = 1.5
                except (ValueError, TypeError):
                    interval = 1.5
                
                with P115Service._rate_limit_lock:
                    current_time = time.time()
                    elapsed = current_time - P115Service._last_request_time
                    if elapsed < interval:
                        import random
                        # ★ 核心修复：加入 0.1~0.5 秒的随机抖动，打破固定频率的机器人特征
                        jitter = random.uniform(0.1, 0.5)
                        time.sleep((interval - elapsed) + jitter)
                    P115Service._last_request_time = time.time()

            def get_user_info(self):
                self._rate_limit()
                if self._openapi: return self._openapi.get_user_info()
                if self._cookie: return self._cookie.get_user_info()
                return None

            def fs_files(self, payload):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_files(payload)

            def fs_files_app(self, payload):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_files_app(payload)
            
            def fs_search(self, payload):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_search(payload)
            
            def fs_get_info(self, file_id):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_get_info(file_id)

            def fs_mkdir(self, name, pid):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_mkdir(name, pid)

            def fs_move(self, fids, to_cid):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_move(fids, to_cid)

            def fs_rename(self, fid_name_tuple):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_rename(fid_name_tuple)

            def fs_delete(self, fids):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_delete(fids)
            
            def rb_del(self, tids=None):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.rb_del(tids)
            
            def life_behavior_detail(self, payload=None):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法获取生活事件")
                return self._cookie.life_behavior_detail(payload)

            def life_batch_delete(self, delete_data_list):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法删除生活事件")
                return self._cookie.life_batch_delete(delete_data_list)
            
            def upload_file_stream(self, file_stream, file_name, target_cid):
                self._check_openapi()
                self._rate_limit() 
                return self._openapi.upload_file_stream(file_stream, file_name, target_cid)

            def download_url(self, pick_code, user_agent=None):
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法获取播放直链")
                
                cache_key = (pick_code, user_agent)
                now = time.time()
                
                # 1. 查缓存
                if cache_key in _DIRECT_URL_CACHE:
                    cached_data = _DIRECT_URL_CACHE[cache_key]
                    if now < cached_data['expire_at']:
                        return cached_data['url']

                with P115Service._downurl_lock:
                    if cache_key in _DIRECT_URL_CACHE and now < _DIRECT_URL_CACHE[cache_key]['expire_at']:
                        return _DIRECT_URL_CACHE[cache_key]['url']

                    current_time = time.time()
                    elapsed = current_time - P115Service._last_downurl_time
                    if elapsed < 1.5:
                        time.sleep(1.5 - elapsed)
                    
                    try:
                        # ★ 核心修复：使用独立线程池强制设置 15 秒超时，防止第三方库底层 Socket 死锁！
                        from concurrent.futures import ThreadPoolExecutor, TimeoutError
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(self._cookie.download_url, pick_code, user_agent)
                            try:
                                res = future.result(timeout=15)
                            except TimeoutError:
                                logger.error(f"  🛑 [超时拦截] 获取直链网络卡死超过 15 秒，已强制切断！")
                                P115Service._last_downurl_time = time.time()
                                return None

                        P115Service._last_downurl_time = time.time()
                        
                        if res:
                            direct_url = str(res)
                            display_name = pick_code[:8] + "..."
                            
                            # ★ 从 115 返回的直链 URL 中反向解析出真实文件名
                            try:
                                from urllib.parse import urlparse, parse_qs, unquote
                                import os
                                parsed = urlparse(direct_url)
                                qs = parse_qs(parsed.query)
                                if 'file' in qs: display_name = unquote(qs['file'][0])
                                elif 'filename' in qs: display_name = unquote(qs['filename'][0])
                                else:
                                    path_name = unquote(os.path.basename(parsed.path))
                                    if path_name: display_name = path_name
                            except: pass

                            logger.info(f"  ➜ [Cookie] 成功获取直链 -> {display_name}")

                            # ★ 将文件名一起存入缓存
                            _DIRECT_URL_CACHE[cache_key] = {
                                'url': direct_url,
                                'name': display_name,
                                'expire_at': time.time() + 300
                            }
                            return direct_url
                        return None
                    except Exception as e:
                        err_str = str(e)
                        if '405' in err_str or 'Method Not Allowed' in err_str:
                            logger.error("  🛑 [熔断] 获取直链触发 115 WAF 风控 (405)，强制休眠 10 秒...")
                            P115Service._last_downurl_time = time.time() + 10
                        else:
                            P115Service._last_downurl_time = time.time()
                        raise e
                    
            def openapi_downurl(self, pick_code, user_agent=None):
                """使用 OpenAPI 获取直链 (带缓存和 UA 透传)"""
                self._check_openapi()
                cache_key = (f"openapi_{pick_code}", user_agent)
                now = time.time()
                
                if cache_key in _DIRECT_URL_CACHE:
                    cached_data = _DIRECT_URL_CACHE[cache_key]
                    if now < cached_data['expire_at']:
                        return cached_data['url']

                with P115Service._downurl_lock:
                    if cache_key in _DIRECT_URL_CACHE and now < _DIRECT_URL_CACHE[cache_key]['expire_at']:
                        return _DIRECT_URL_CACHE[cache_key]['url']

                    self._rate_limit()
                    try:
                        res = self._openapi.fs_downurl(pick_code, user_agent)
                        if res and res.get('state') and res.get('data'):
                            data_dict = res['data']
                            file_info = next(iter(data_dict.values()), None)
                            if file_info and 'url' in file_info and 'url' in file_info['url']:
                                direct_url = file_info['url']['url']
                                display_name = file_info.get('file_name', pick_code)
                                logger.info(f"  ➜ [OpenAPI] 成功获取直链 -> {display_name}")
                                _DIRECT_URL_CACHE[cache_key] = {
                                    'url': direct_url,
                                    'name': display_name,
                                    'expire_at': time.time() + 300 
                                }
                                return direct_url
                        return None
                    except Exception as e:
                        logger.warning(f"  ➜ [115 OpenAPI] 获取直链异常: {e}")
                        return None

            def request(self, *args, **kwargs):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行网络请求")
                return self._cookie.request(*args, **kwargs)

            def offline_add_urls(self, payload):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行离线下载")
                return self._cookie.offline_add_urls(payload)

            def share_import(self, share_code, receive_code, cid):
                self._rate_limit()
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法执行转存")
                return self._cookie.share_import(share_code, receive_code, cid)

        return StrictSplitClient(openapi, cookie)
    
    @classmethod
    def get_cookies(cls):
        """获取 Cookie (用于直链下载等)"""
        _, _, cookie = get_115_tokens()
        return cookie
    
    @classmethod
    def get_token(cls):
        """获取 Token (用于 API 调用)"""
        token, _, _ = get_115_tokens()
        return token


# ======================================================================
# ★★★ 115 目录树 DB 缓存管理器 ★★★
# ======================================================================
class P115CacheManager:
    @staticmethod
    def get_local_path(cid):
        """从本地数据库获取已缓存的完整相对路径"""
        if not cid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT local_path FROM p115_filesystem_cache WHERE id = %s", (str(cid),))
                    row = cursor.fetchone()
                    return row['local_path'] if row else None
        except Exception:
            return None
        
    @staticmethod
    def get_fid_by_pickcode(pick_code):
        """通过 PC 码获取文件 FID"""
        if not pick_code: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE pick_code = %s LIMIT 1", (pick_code,))
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception:
            return None

    @staticmethod
    def update_local_path(cid, local_path):
        """更新数据库中的 local_path"""
        if not cid or not local_path: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE p115_filesystem_cache 
                        SET local_path = %s, updated_at = NOW() 
                        WHERE id = %s
                    """, (str(local_path), str(cid)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 更新 local_path 失败: {e}")

    @staticmethod
    def get_node_info(cid):
        """获取节点的 parent_id 和 name (查户口)"""
        if not cid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT parent_id, name FROM p115_filesystem_cache WHERE id = %s", (str(cid),))
                    return cursor.fetchone()
        except Exception:
            return None

    @staticmethod
    def get_cid(parent_cid, name):
        """从本地数据库获取 CID (毫秒级)"""
        if not parent_cid or not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id FROM p115_filesystem_cache WHERE parent_id = %s AND name = %s", 
                        (str(parent_cid), str(name))
                    )
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            logger.error(f"  ➜ 读取 115 DB 缓存失败: {e}")
            return None

    @staticmethod
    def save_cid(cid, parent_cid, name, sha1=None):
        """将 CID 和 SHA1 存入本地数据库缓存"""
        if not cid or not parent_cid or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (id, parent_id, name, sha1)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET id = EXCLUDED.id, sha1 = EXCLUDED.sha1, updated_at = NOW()
                    """, (str(cid), str(parent_cid), str(name), sha1))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 DB 缓存失败: {e}")

    @staticmethod
    def get_file_sha1(fid):
        """从本地数据库获取已缓存的文件 SHA1"""
        if not fid: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT sha1 FROM p115_filesystem_cache WHERE id = %s", (str(fid),))
                    row = cursor.fetchone()
                    return row['sha1'] if row else None
        except Exception:
            return None

    @staticmethod
    def get_cid_by_name(name):
        """仅通过名称查找 CID (适用于带有 {tmdb=xxx} 的唯一主目录)"""
        if not name: return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE name = %s LIMIT 1", (str(name),))
                    row = cursor.fetchone()
                    return row['id'] if row else None
        except Exception as e:
            return None
        
    @staticmethod
    def get_files_by_pickcodes(pickcodes):
        """通过 PC 码批量查出文件 ID 和 父目录 ID"""
        if not pickcodes: return []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法进行数组匹配
                    cursor.execute("SELECT id, parent_id, pick_code FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (list(pickcodes),))
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"  ➜ 查询文件缓存失败: {e}")
            return []

    @staticmethod
    def delete_cid(cid):
        """从缓存中物理删除该目录及其子目录的记录"""
        if not cid: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 删除自身以及以它为父目录的子项
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s OR parent_id = %s", (str(cid), str(cid)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 DB 缓存失败: {e}")

    @staticmethod
    def save_file_cache(fid, parent_id, name, sha1=None, pick_code=None, local_path=None, size=0):
        """专门将文件(fc=1)的 SHA1、PC码、本地相对路径和大小存入本地数据库缓存"""
        if not fid or not parent_id or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = %s", (str(fid),))
                    
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (id, parent_id, name, sha1, pick_code, local_path, size)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET 
                            sha1 = CASE 
                                WHEN p115_filesystem_cache.id != EXCLUDED.id THEN EXCLUDED.sha1 
                                ELSE COALESCE(EXCLUDED.sha1, p115_filesystem_cache.sha1) 
                            END,
                            pick_code = CASE 
                                WHEN p115_filesystem_cache.id != EXCLUDED.id THEN EXCLUDED.pick_code 
                                ELSE COALESCE(EXCLUDED.pick_code, p115_filesystem_cache.pick_code) 
                            END,
                            local_path = COALESCE(EXCLUDED.local_path, p115_filesystem_cache.local_path),
                            size = CASE 
                                WHEN EXCLUDED.size > 0 THEN EXCLUDED.size 
                                ELSE p115_filesystem_cache.size 
                            END,
                            id = EXCLUDED.id,
                            updated_at = NOW()
                    """, (str(fid), str(parent_id), str(name), sha1, pick_code, local_path, size))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 文件缓存失败: {e}")

    @staticmethod
    def delete_files(fids):
        """批量从缓存中物理删除文件记录"""
        if not fids: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法批量删除
                    cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(fids),))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 文件缓存失败: {e}")

    @staticmethod
    def save_mediainfo_cache(sha1, mediainfo_json):
        """写入本地 p115_mediainfo_cache，结构保持 Emby MediaSourceInfo 标准格式"""
        if not sha1 or not mediainfo_json:
            return False

        try:
            from psycopg2.extras import Json

            sha1 = str(sha1).upper()

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_mediainfo_cache (sha1, mediainfo_json, created_at, hit_count)
                        VALUES (%s, %s, NOW(), 0)
                        ON CONFLICT (sha1)
                        DO UPDATE SET
                            mediainfo_json = EXCLUDED.mediainfo_json,
                            created_at = NOW()
                    """, (
                        sha1,
                        Json(mediainfo_json, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))
                    ))
                    conn.commit()

            logger.info(f"  ➜ [媒体信息缓存] 已写入本地 p115_mediainfo_cache -> {sha1[:12]}...")
            return True

        except Exception as e:
            logger.error(f"  ➜ 写入 p115_mediainfo_cache 失败: {e}", exc_info=True)
            return False

    @staticmethod
    def get_mediainfo_cache_text(sha1):
        """从本地 p115_mediainfo_cache 读取 JSON 原文，用于直接生成 -mediainfo.json 文件"""
        if not sha1:
            return None

        try:
            sha1 = str(sha1).upper()
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT mediainfo_json::text AS mediainfo_json_text FROM p115_mediainfo_cache WHERE sha1 = %s",
                        (sha1,)
                    )
                    row = cursor.fetchone()
                    return row['mediainfo_json_text'] if row and row.get('mediainfo_json_text') else None
        except Exception as e:
            logger.error(f"  ➜ 读取 p115_mediainfo_cache 失败: {e}")
            return None

# ======================================================================
# ★★★ 115 整理记录 DB 管理器 ★★★
# ======================================================================
class P115RecordManager:
    @staticmethod
    def add_or_update_record(file_id, original_name, status, tmdb_id=None, media_type=None, target_cid=None, category_name=None, renamed_name=None, is_center_cached=False, pick_code=None, season_number=None, fail_reason=None):
        """添加或更新整理记录（基于 file_id 和 pick_code 唯一约束，智能继承原名）"""
        if not file_id or not original_name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if pick_code:
                        cursor.execute("SELECT file_id, original_name FROM p115_organize_records WHERE pick_code = %s", (pick_code,))
                        row = cursor.fetchone()
                        if row:
                            old_file_id = row['file_id']
                            original_name = row['original_name'] 
                            if str(old_file_id) != str(file_id):
                                cursor.execute("DELETE FROM p115_organize_records WHERE file_id = %s", (old_file_id,))

                    cursor.execute("""
                        INSERT INTO p115_organize_records 
                        (file_id, pick_code, original_name, status, tmdb_id, media_type, target_cid, category_name, renamed_name, processed_at, is_center_cached, season_number, fail_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
                        ON CONFLICT (file_id) 
                        DO UPDATE SET 
                            pick_code = EXCLUDED.pick_code,
                            status = EXCLUDED.status,
                            tmdb_id = EXCLUDED.tmdb_id,
                            media_type = EXCLUDED.media_type,
                            target_cid = EXCLUDED.target_cid,
                            category_name = EXCLUDED.category_name,
                            renamed_name = EXCLUDED.renamed_name,
                            processed_at = NOW(),
                            is_center_cached = p115_organize_records.is_center_cached OR EXCLUDED.is_center_cached,
                            season_number = EXCLUDED.season_number,
                            fail_reason = EXCLUDED.fail_reason
                    """, (str(file_id), pick_code, str(original_name), str(status), str(tmdb_id) if tmdb_id else None, 
                          str(media_type) if media_type else None, str(target_cid) if target_cid else None, 
                          str(category_name) if category_name else None, str(renamed_name) if renamed_name else None, bool(is_center_cached), season_number, fail_reason))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 写入 115 整理记录失败: {e}")
    @staticmethod
    def delete_records(file_ids):
        """批量删除整理记录 (用于洗版替换时清理旧记录)"""
        if not file_ids: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 使用 ANY 语法批量删除
                    cursor.execute("DELETE FROM p115_organize_records WHERE file_id = ANY(%s)", (list(file_ids),))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ➜ 清理 115 整理记录失败: {e}")

# ======================================================================
# ★★★ 115 全局批量删除缓冲队列 (极简暴力清理版) ★★★
# ======================================================================
class P115DeleteBuffer:
    _lock = threading.Lock()
    _fids_to_delete = set()
    _cids_to_check = set()
    _check_save_path = False # ★ 新增：是否检查待整理根目录
    _timer = None
    _last_add_time = 0

    @classmethod
    def add(cls, fids=None, base_cids=None, check_save_path=False):
        with cls._lock:
            if fids:
                cls._fids_to_delete.update(fids)
            if base_cids:
                if isinstance(base_cids, (list, set)):
                    cls._cids_to_check.update(base_cids)
                else:
                    cls._cids_to_check.add(base_cids)
            if check_save_path:
                cls._check_save_path = True

            # ★ 核心防抖：每次有新文件整理完，刷新倒计时
            cls._last_add_time = time.time()
            if cls._timer is None:
                cls._timer = spawn_later(5.0, cls._check_and_flush)

    @classmethod
    def _check_and_flush(cls):
        with cls._lock:
            now = time.time()
            # ★ 智能防抖：如果距离最后一次整理还不到 10 秒，说明大部队还在干活，继续等！
            if now - cls._last_add_time < 10.0:
                cls._timer = spawn_later(10.0 - (now - cls._last_add_time), cls._check_and_flush)
                return
            
            fids = list(cls._fids_to_delete)
            cids = list(cls._cids_to_check)
            check_save = cls._check_save_path
            
            cls._fids_to_delete.clear()
            cls._cids_to_check.clear()
            cls._check_save_path = False
            cls._timer = None

        client = P115Service.get_client()
        if not client: return

        # =================================================================
        # ★ 核心修改：直接拉取“待整理”目录下的所有一级子目录加入死刑检查名单
        # =================================================================
        config = get_config()
        if check_save:
            save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            unidentified_name = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME, "未识别")
            if save_path and str(save_path) != '0':
                try:
                    res = client.fs_files({'cid': save_path, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    for item in res.get('data', []):
                        if str(item.get('fc') or item.get('type')) == '0':
                            sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                            sub_cid = item.get('fid') or item.get('file_id')
                            # 排除“未识别”目录，其他的全部拉进去检查
                            if sub_name != unidentified_name and sub_cid:
                                cids.append(sub_cid)
                except Exception as e:
                    logger.error(f"  ➜ 获取待整理目录子项失败: {e}")

        # 去重
        cids = list(set(cids))

        if not fids and not cids:
            return

        def _safe_batch_delete(ids, is_dir=False):
            if not ids: return []
            item_type = "目录" if is_dir else "文件"
            max_retries = 3
            
            for attempt in range(max_retries):
                resp = client.fs_delete(ids)
                if resp.get('state'):
                    return ids
                
                if resp.get('code') in [770004, 990001]:
                    logger.error(f"  🛑 [触发流控] 115 API 提示达到访问上限 ({resp.get('code')})，立即终止本次删除任务！")
                    return [] 

                logger.error(f"  ➜ [批量销毁] 115 删除{item_type}失败 (第 {attempt + 1}/{max_retries} 次): {resp}")
                if attempt < max_retries - 1:
                    time.sleep(3)
            
            logger.warning(f"  ➜ [批量销毁] 批量删除彻底失败，放弃本次清理。")
            return []

        # 1. 删除明确指定的文件
        if fids:
            logger.info(f"  ➜ [批量销毁] 缓冲期结束，正在删除 {len(fids)} 个文件...")
            success_fids = _safe_batch_delete(fids, is_dir=False)
            if success_fids:
                P115CacheManager.delete_files(success_fids)

        # 2. 获取免死金牌名单
        protected_cids = {'0'}
        media_root = config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID)
        if media_root: protected_cids.add(str(media_root))
        save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
        if save_path: protected_cids.add(str(save_path))
        
        raw_rules = settings_db.get_setting('p115_sorting_rules')
        if raw_rules:
            rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
            for rule in rules:
                if rule.get('cid'): protected_cids.add(str(rule['cid']))

        # 3. 检查空目录
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        media_exts = allowed_exts | {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg', 'mp3', 'flac', 'wav', 'ape', 'm4a', 'aac', 'ogg'}

        empty_cids_to_delete = []

        for cid in cids:
            if str(cid) in protected_cids: continue
            
            media_count = 0
            def count_media(current_cid):
                nonlocal media_count
                for attempt in range(3):
                    try:
                        res = client.fs_files({'cid': current_cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                        for item in res.get('data', []):
                            if str(item.get('fc')) == '1':
                                ext = str(item.get('fn', '')).split('.')[-1].lower()
                                if ext in media_exts:
                                    item_size = _parse_115_size(item.get('fs') or item.get('size'))
                                    if item_size == 0 or item_size > 10 * 1024 * 1024:
                                        media_count += 1
                            elif str(item.get('fc')) == '0':
                                count_media(item.get('fid'))
                        return 
                    except Exception as e:
                        if attempt == 2:
                            media_count += 999 
                        time.sleep(1)

            count_media(cid)
            # ★ 只要没有媒体文件（哪怕里面有一堆 nfo 和 jpg），统统判定为空目录！
            if media_count == 0:
                empty_cids_to_delete.append(cid)
                logger.debug(f"  ➜ 判定为空目录，加入待清理队列: CID {cid}")

        # 4. 批量删除空目录
        if empty_cids_to_delete:
            logger.debug(f"  ➜ [批量清理] 正在向 115 发送批量删除空目录指令 ({len(empty_cids_to_delete)} 个)...")
            success_cids = _safe_batch_delete(empty_cids_to_delete, is_dir=True)
            if success_cids:
                for cid in success_cids:
                    P115CacheManager.delete_cid(cid)
                logger.info(f"  ➜ [批量清理] 成功删除了 {len(success_cids)} 个空目录。")

    @classmethod
    def flush(cls):
        """兼容老接口调用"""
        cls._check_and_flush()

def get_config():
    return config_manager.APP_CONFIG

class SmartOrganizer:
    def __init__(self, client, tmdb_id, media_type, original_title, ai_translator=None, use_ai=False):
        self.client = client
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_title = original_title
        self.ai_translator = ai_translator # 新增
        self.use_ai = use_ai
        self.api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        self.forced_season = None
        self.studio_map = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
        self.keyword_map = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
        self.rating_map = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
        self.rating_priority = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY
        self.country_map = settings_db.get_setting('country_mapping') or utils.DEFAULT_COUNTRY_MAPPING
        self.language_map = settings_db.get_setting('language_mapping') or utils.DEFAULT_LANGUAGE_MAPPING

        self.raw_metadata = self._fetch_raw_metadata()
        self.details = self.raw_metadata
        self.rename_config = settings_db.get_setting('p115_rename_config') or {
            "main_title_lang": "zh", "main_year_en": True, "main_tmdb_fmt": "{tmdb=ID}",
            "season_fmt": "Season {02}", "file_title_lang": "zh", "file_year_en": False,
            "file_tmdb_fmt": "none", "file_params_en": True, "file_sep": " - ",
            "strm_url_fmt": "standard"
        }
        raw_rules = settings_db.get_setting('p115_sorting_rules')
        self.rules = []
        
        if raw_rules:
            if isinstance(raw_rules, list):
                self.rules = raw_rules
            elif isinstance(raw_rules, str):
                try:
                    self.rules = json.loads(raw_rules)
                except Exception as e:
                    logger.error(f"  ➜ 解析 115 分类规则失败: {e}")
                    self.rules = []

    def _fetch_raw_metadata(self):
        """
        获取 TMDb 原始元数据 (ID/Code)，不进行任何中文转换。
        """
        if not self.api_key: return {}
        
        # 读取内存缓存
        cache_key = f"{self.media_type}_{self.tmdb_id}"
        if cache_key in _TMDB_METADATA_CACHE:
            return _TMDB_METADATA_CACHE[cache_key]

        data = {
            'genre_ids': [],
            'country_codes': [],
            'lang_code': None,
            'company_ids': [],
            'network_ids': [],
            'keyword_ids': [],
            'rating_label': '未知' # 分级是特例，必须计算出标签才能匹配
        }

        try:
            raw_details = {}
            if self.media_type == 'tv':
                raw_details = tmdb.get_tv_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,content_ratings,networks,credits,alternative_titles"
                )
            else:
                raw_details = tmdb.get_movie_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,release_dates,credits,alternative_titles"
                )

            if not raw_details: return {}

            # 1. 基础 ID/Code 提取
            data['genre_ids'] = [g.get('id') for g in raw_details.get('genres', [])]
            data['country_codes'] = [c.get('iso_3166_1') for c in raw_details.get('production_countries', [])]
            if not data['country_codes'] and raw_details.get('origin_country'):
                data['country_codes'] = raw_details.get('origin_country')

            data['lang_code'] = raw_details.get('original_language')

            data['company_ids'] = [c.get('id') for c in raw_details.get('production_companies', [])]
            data['network_ids'] = [n.get('id') for n in raw_details.get('networks', [])] if self.media_type == 'tv' else []

            # 2. 关键词 ID 提取
            kw_container = raw_details.get('keywords', {})
            raw_kw_list = kw_container.get('keywords', []) if self.media_type == 'movie' else kw_container.get('results', [])
            data['keyword_ids'] = [k.get('id') for k in raw_kw_list]

            # 3. 分级计算 
            data['rating_label'] = utils.get_rating_label(
                raw_details,
                self.media_type,
                self.rating_map,
                self.rating_priority
            )

            # 4. 演员提取
            # 只取前 3 名主演，避免客串演员乱入导致规则匹配不准确
            data['actor_ids'] = [cast.get('id') for cast in raw_details.get('credits', {}).get('cast', [])[:3]]

            # =====================================================================
            # ★★★ 5. 标题提取 (本地缓存优先 -> 隐身符清洗 -> 广告拦截 -> 别名兜底) ★★★
            # =====================================================================
            cached_title = None
            cached_original_title = None
            
            # 5.1 优先查询本地数据库缓存 (免疫 TMDb 后期篡改，保持网盘与 Emby 绝对一致)
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        db_item_type = 'Movie' if self.media_type == 'movie' else 'Series'
                        cursor.execute(
                            "SELECT title, original_title FROM media_metadata WHERE tmdb_id = %s AND item_type = %s",
                            (str(self.tmdb_id), db_item_type)
                        )
                        row = cursor.fetchone()
                        if row and row['title']:
                            cached_title = row['title']
                            cached_original_title = row['original_title']
            except Exception as e:
                logger.warning(f"  ➜ [115整理] 查询本地标题缓存失败: {e}")

            if cached_title:
                logger.info(f"  ➜ [115整理] 命中本地数据库片名: '{cached_title}'，无视 TMDb 最新变动。")
                current_title = cached_title
                original_title = cached_original_title or cached_title
            else:
                # 5.2 本地无缓存 (首次入库)，走 TMDb 提取与清洗流程
                raw_title = raw_details.get('title') or raw_details.get('name')
                current_title = utils.clean_invisible_chars(raw_title)
                
                if utils.is_spam_title(current_title):
                    logger.warning(f"  ➜ [115整理] 拦截到恶意广告片名: '{current_title}'，准备寻找干净的别名...")
                    current_title = ""

                if not current_title or not utils.contains_chinese(current_title):
                    chinese_alias = None
                    alt_titles_data = raw_details.get("alternative_titles", {})
                    alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                    
                    priority_map = {"CN": 1, "SG": 2, "TW": 3, "HK": 4}
                    best_priority = 99
                    
                    for alt in alt_list:
                        alt_title = utils.clean_invisible_chars(alt.get("title", ""))
                        if utils.contains_chinese(alt_title) and not utils.is_spam_title(alt_title):
                            iso_country = alt.get("iso_3166_1", "").upper()
                            current_priority = priority_map.get(iso_country, 5) 
                            
                            if current_priority < best_priority:
                                chinese_alias = alt_title
                                best_priority = current_priority
                                
                            if best_priority == 1:
                                break 
                    
                    if chinese_alias:
                        logger.info(f"  ➜ [115整理] 发现干净的 TMDb 官方中文别名: '{chinese_alias}'")
                        current_title = chinese_alias
                    else:
                        raw_original = raw_details.get("original_title") or raw_details.get("original_name")
                        original_title = utils.clean_invisible_chars(raw_original)
                        logger.info(f"  ➜ [115整理] 未找到干净的中文别名，回退到原名: '{original_title}'")
                        current_title = original_title
                else:
                    # 如果主标题正常，提取原名
                    raw_original = raw_details.get("original_title") or raw_details.get("original_name")
                    original_title = utils.clean_invisible_chars(raw_original)

            data['title'] = current_title
            data['original_title'] = original_title

            # ★★★ 尝试提取纯英文名 (title_en) ★★★
            english_title = None
            # 如果原名本身就是英文，直接用原名
            if original_title and not utils.contains_chinese(original_title) and re.match(r'^[a-zA-Z0-9\s\-_:\.,!\?\'"&]+$', original_title):
                english_title = original_title
            else:
                # 否则去别名里找美国的别名
                alt_titles_data = raw_details.get("alternative_titles", {})
                alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                for alt in alt_list:
                    if alt.get("iso_3166_1", "").upper() == "US":
                        english_title = utils.clean_invisible_chars(alt.get("title", ""))
                        break
            
            # 存入 data 供后续调用
            data['title_en'] = english_title or original_title # 兜底用原名
            
            # 提取年份
            date_str = raw_details.get('release_date') or raw_details.get('first_air_date')
            data['date'] = date_str
            data['year'] = 0
            if date_str and len(str(date_str)) >= 4:
                try:
                    data['year'] = int(str(date_str)[:4])
                except: 
                    pass
            
            # 补充评分供规则匹配
            data['vote_average'] = raw_details.get('vote_average', 0)
            
            # ★ 补充时长供规则匹配
            if self.media_type == 'movie':
                data['runtime'] = raw_details.get('runtime', 0)
            else:
                data['episode_run_time'] = raw_details.get('episode_run_time', [])

            # ★ 补充季集数据，供动漫绝对集数推算使用
            data['seasons'] = raw_details.get('seasons', [])
            data['last_episode_to_air'] = raw_details.get('last_episode_to_air', {})

            _TMDB_METADATA_CACHE[cache_key] = data # 写入缓存

            return data

        except Exception as e:
            logger.warning(f"  ➜ [整理] 获取原始元数据失败: {e}", exc_info=True)
            return {}

    def _match_rule(self, rule):
        """
        规则匹配逻辑 (支持 AND / OR 复合匹配)
        """
        if not self.raw_metadata: return False

        # ==========================================
        # 1. 绝对前置过滤条件 (必须满足，无视 AND/OR)
        # ==========================================
        # 媒体类型 (电影/剧集) 是硬性分类，必须优先满足
        if rule.get('media_type') and rule['media_type'] != 'all':
            if rule['media_type'] != self.media_type: return False

        # ★★★ 核心重构：追剧状态的主动判定与分季隔离 ★★★
        if rule.get('watching_status') == 'watching' and self.media_type == 'tv':
            try:
                from database.watchlist_db import get_watching_tmdb_ids, get_season_watching_status
                
                season_num = getattr(self, 'forced_season', None)
                
                if season_num is not None:
                    # 1. 优先查本地数据库 (速度最快)
                    season_status = get_season_watching_status(self.tmdb_id, season_num)
                    
                    if season_status in ['Watching', 'Paused', 'Pending']:
                        # 明确在追，直接放行，无需查 TMDb
                        pass 
                    elif season_status == 'Completed':
                        # 明确完结，直接拦截
                        logger.debug(f"  🛑 [规则拦截] '第 {season_num} 季' 真实状态为 'Completed'，跳过连载规则。")
                        return False
                    else:
                        # 状态是 'NONE'、空值、或者其他未知状态，主动向 TMDb 查连载状态！
                        from tasks.helpers import evaluate_season_airing_status
                        logger.info(f"  ➜ 数据库状态为 '{season_status or '空'}'，正在向 TMDb 实时查询 '第 {season_num} 季' 的连载状态...")
                        is_airing = evaluate_season_airing_status(self.tmdb_id, season_num, self.api_key)
                        
                        if is_airing:
                            logger.info(f"  ➜ [连载判定] 确认 '第 {season_num} 季' 正在连载，命中连载规则！")
                            # 既然是连载，就让它继续往下走，命中规则
                        else:
                            logger.debug(f"  🛑 [连载判定] 确认 '第 {season_num} 季' 已完结，跳过连载规则。")
                            return False
                else:
                    # 没提取到季号，退化为查整部剧的状态
                    watching_ids = get_watching_tmdb_ids()
                    if str(self.tmdb_id) not in watching_ids:
                        return False
            except Exception as e:
                logger.warning(f"获取追剧状态失败: {e}")
                return False

        # ==========================================
        # 2. 动态条件匹配 (根据 match_mode 决定 AND 或 OR)
        # ==========================================
        match_mode = rule.get('match_mode', 'and')
        conditions_configured = 0  # 记录配置了多少个条件
        conditions_met = 0         # 记录满足了多少个条件

        def _evaluate(is_match):
            nonlocal conditions_configured, conditions_met
            conditions_configured += 1
            if is_match:
                conditions_met += 1

        # 2.1 类型 (Genres)
        if rule.get('genres'):
            rule_ids = [int(x) for x in rule['genres']]
            tmdb_genre_ids = self.raw_metadata.get('genre_ids', [])
            _evaluate(any(gid in rule_ids for gid in tmdb_genre_ids))

        # 2.2 国家 (Countries)
        if rule.get('countries'):
            target_codes = set()
            for item in rule['countries']:
                # 尝试在映射表中找中文标签
                mapping = next((m for m in self.country_map if m['label'] == item), None)
                if mapping:
                    target_codes.add(mapping['value'])
                    if 'aliases' in mapping:
                        target_codes.update(mapping['aliases'])
                else:
                    # 兼容旧规则（直接存了代码的情况）
                    target_codes.add(item)
            
            current_countries = self.raw_metadata.get('country_codes', [])
            _evaluate(any(c in target_codes for c in current_countries))

        # 2.3 语言 (Languages)
        if rule.get('languages'):
            target_codes = set()
            for item in rule['languages']:
                # 尝试在映射表中找中文标签
                mapping = next((m for m in self.language_map if m['label'] == item), None)
                if mapping:
                    target_codes.add(mapping['value'])
                    if 'aliases' in mapping:
                        target_codes.update(mapping['aliases'])
                else:
                    # 兼容旧规则（直接存了代码的情况）
                    target_codes.add(item)
                    
            _evaluate(self.raw_metadata.get('lang_code') in target_codes)

        # 2.4 工作室 (Studios)
        if rule.get('studios'):
            target_ids = set()
            for label in rule['studios']:
                config_item = next((item for item in self.studio_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('company_ids', []))
                    target_ids.update(config_item.get('network_ids', []))

            has_company = any(cid in target_ids for cid in self.raw_metadata.get('company_ids', []))
            has_network = any(nid in target_ids for nid in self.raw_metadata.get('network_ids', []))
            _evaluate(has_company or has_network)

        # 2.5 关键词 (Keywords)
        if rule.get('keywords'):
            target_ids = set()
            for label in rule['keywords']:
                config_item = next((item for item in self.keyword_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('ids', []))

            tmdb_kw_ids = [int(k) for k in self.raw_metadata.get('keyword_ids', [])]
            target_ids_int = [int(k) for k in target_ids]
            _evaluate(any(kid in target_ids_int for kid in tmdb_kw_ids))

        # 2.6 分级 (Rating)
        if rule.get('ratings'):
            _evaluate(self.raw_metadata.get('rating_label') in rule['ratings'])

        # 2.7 年份 (Year)
        year_min = rule.get('year_min')
        year_max = rule.get('year_max')
        if year_min or year_max:
            current_year = self.raw_metadata.get('year', 0)
            if current_year == 0:
                _evaluate(False)
            else:
                is_y_match = True
                if year_min and current_year < int(year_min): is_y_match = False
                if year_max and current_year > int(year_max): is_y_match = False
                _evaluate(is_y_match)

        # 2.8 时长 (Runtime)
        run_min = rule.get('runtime_min')
        run_max = rule.get('runtime_max')
        if run_min or run_max:
            current_runtime = 0
            if self.media_type == 'movie':
                current_runtime = self.details.get('runtime') or 0
            else:
                runtimes = self.details.get('episode_run_time', [])
                if runtimes and len(runtimes) > 0:
                    current_runtime = runtimes[0]

            if current_runtime == 0:
                _evaluate(False)
            else:
                is_r_match = True
                if run_min and current_runtime < int(run_min): is_r_match = False
                if run_max and current_runtime > int(run_max): is_r_match = False
                _evaluate(is_r_match)

        # 2.9 评分 (Min Rating)
        if rule.get('min_rating') and float(rule['min_rating']) > 0:
            vote_avg = self.details.get('vote_average', 0)
            _evaluate(vote_avg >= float(rule['min_rating']))

        # 2.10 演员 (Actors)
        if rule.get('actors'):
            rule_actor_ids = [int(a['id']) for a in rule['actors'] if 'id' in a]
            _evaluate(any(aid in self.raw_metadata.get('actor_ids', []) for aid in rule_actor_ids))

        # ==========================================
        # 3. 最终结果判定
        # ==========================================
        if conditions_configured == 0:
            return True # 没有配置任何条件，默认命中（兜底规则）

        if match_mode == 'or':
            # OR 模式：只要满足了任意一个条件，就算命中
            return conditions_met > 0
        else: 
            # AND 模式：必须满足所有配置的条件
            return conditions_met == conditions_configured

    def get_target_cid(self, ignore_memory=False, season_num=None):
        """获取目标 CID：优先查历史整理记录（记忆手动纠错），其次遍历规则"""
        self.is_from_memory = False # 初始化记忆标记
        # 辅助函数：校验历史 CID 是否仍在当前启用的规则中
        def _is_cid_valid_in_rules(check_cid):
            if not check_cid: return False
            for r in self.rules:
                if str(r.get('cid')) == str(check_cid) and r.get('enabled', True):
                    return True
            return False

        # ★★★ 1. 查历史记录 (记忆功能 - 升级为分季隔离 + 规则校验版) ★★★
        if not ignore_memory:
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        if self.media_type == 'tv' and season_num is not None:
                            # 查找该剧最近的 50 条记录，寻找属于该季的专属记忆
                            cursor.execute("""
                                SELECT target_cid, category_name, renamed_name, original_name 
                                FROM p115_organize_records 
                                WHERE tmdb_id = %s AND status = 'success' 
                                ORDER BY processed_at DESC LIMIT 50
                            """, (str(self.tmdb_id),))
                            rows = cursor.fetchall()
                            import re
                            for row in rows:
                                name_to_check = row['renamed_name'] or row['original_name'] or ""
                                m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', name_to_check)
                                m2 = re.search(r'Season\s*(\d{1,4})\b', name_to_check, re.IGNORECASE)
                                m3 = re.search(r'第(\d{1,4})季', name_to_check)
                                s_val = None
                                if m1: s_val = int(m1.group(1))
                                elif m2: s_val = int(m2.group(1))
                                elif m3: s_val = int(m3.group(1))
                                
                                if s_val == season_num:
                                    history_cid = str(row['target_cid'])
                                    # ★ 核心修复：校验记忆是否失效
                                    if _is_cid_valid_in_rules(history_cid):
                                        logger.info(f"  ➜ [分季记忆体] 发现该剧 '第 {season_num} 季' 曾被整理过，沿用专属分类: {row['category_name']} (CID: {history_cid})")
                                        self.is_from_memory = True # 打上记忆命中标记
                                        return history_cid
                                    else:
                                        logger.warning(f"  ➜ [分季记忆体] 历史分类 (CID: {history_cid}) 已不在当前规则中，记忆失效，交由规则引擎重新分配。")
                                        break # 记忆失效，跳出循环走规则
                            
                            logger.debug(f"  ➜ [分季记忆体] 未找到 '第 {season_num} 季' 的有效专属记忆，将使用规则引擎进行分配。")
                        else:
                            # 电影或未提供季号的兜底逻辑
                            cursor.execute("""
                                SELECT target_cid, category_name 
                                FROM p115_organize_records 
                                WHERE tmdb_id = %s AND status = 'success' 
                                ORDER BY processed_at DESC LIMIT 1
                            """, (str(self.tmdb_id),))
                            row = cursor.fetchone()
                            if row and row['target_cid']:
                                history_cid = str(row['target_cid'])
                                # ★ 核心修复：校验记忆是否失效
                                if _is_cid_valid_in_rules(history_cid):
                                    logger.info(f"  ➜ [记忆体] 发现该媒体曾被整理过，沿用历史分类: {row['category_name']} (CID: {history_cid})")
                                    self.is_from_memory = True # 打上记忆命中标记
                                    return history_cid
                                else:
                                    logger.warning(f"  ➜ [记忆体] 历史分类 (CID: {history_cid}) 已不在当前规则中，记忆失效，交由规则引擎重新分配。")
            except Exception as e:
                logger.warning(f"  ➜ 查询历史整理记录失败: {e}")

        # 2. 遍历规则
        for rule in self.rules:
            if not rule.get('enabled', True): continue
            if self._match_rule(rule):
                logger.info(f"  ➜ [115] 命中规则: {rule.get('name')} -> 目录: {rule.get('dir_name')}")
                return rule.get('cid')
        return None

    def _extract_video_info(self, filename):
        """
        从文件名提取视频信息，返回字典供乐高模块调用
        """
        info_dict = {
            'source': '', 'effect': '', 'resolution': '', 
            'codec': '', 'audio': '', 'group': '', 'stream': '', 'fps': '' # ★ 新增 fps 字段
        }
        name_upper = filename.upper()

        # 1. 来源 (Source)
        if re.search(r'REMUX', name_upper): info_dict['source'] = 'Remux'
        elif re.search(r'BLU-?RAY|BD', name_upper): info_dict['source'] = 'BluRay'
        elif re.search(r'WEB-?DL', name_upper): info_dict['source'] = 'WEB-DL'
        elif re.search(r'WEB-?RIP', name_upper): info_dict['source'] = 'WEBRip'
        elif re.search(r'HDTV', name_upper): info_dict['source'] = 'HDTV'
        elif re.search(r'DVD', name_upper): info_dict['source'] = 'DVD'
        if 'UHD' in name_upper:
            info_dict['source'] = 'UHD BluRay' if info_dict['source'] == 'BluRay' else 'UHD'

        # 2. 特效 (Effect)
        is_dv = re.search(r'(?:^|[\.\s\-\_])(DV|DOVI|DOLBY\s?VISION)(?:$|[\.\s\-\_])', name_upper)
        # 优化正则顺序，优先匹配 HDR10+ 和 HDR10
        is_hdr = re.search(r'(?:^|[\.\s\-\_])(HDR10\+|HDR10|HDR)(?:$|[\.\s\-\_])', name_upper)
        
        hdr_str = is_hdr.group(1) if is_hdr else ""
        if is_dv and is_hdr: info_dict['effect'] = f"{hdr_str} DV"
        elif is_dv: info_dict['effect'] = "DV"
        elif is_hdr: info_dict['effect'] = hdr_str

        # 3. 分辨率 (Resolution)
        res_match = re.search(r'(2160|1080|720|480)[pP]', filename)
        if res_match: info_dict['resolution'] = res_match.group(0).lower()
        elif '4K' in name_upper: info_dict['resolution'] = '2160p'

        # 4. 编码 (Codec) - ★ 统一使用商业名
        codec = ""
        if re.search(r'[HX][\.\s]?265|HEVC', name_upper): codec = 'HEVC'
        elif re.search(r'[HX][\.\s]?264|AVC', name_upper): codec = 'AVC'
        elif re.search(r'AV1', name_upper): codec = 'AV1'
        
        bit_match = re.search(r'(\d{1,2})BIT', name_upper)
        bit_depth = f"{bit_match.group(1)}bit" if bit_match else ""
        
        if codec and bit_depth: info_dict['codec'] = f"{codec} {bit_depth}"
        elif codec: info_dict['codec'] = codec
        elif bit_depth: info_dict['codec'] = bit_depth

        # 5. 音频 (Audio) 与 音轨数 (Audio Count) 分离
        audio_info = []
        audio_count_str = ""
        
        # 提取音轨数
        num_audio_match = re.search(r'\b(\d+)\s?Audios?\b', name_upper, re.IGNORECASE)
        if num_audio_match: 
            audio_count_str = f"{num_audio_match.group(1)}Audios"
        elif re.search(r'\b(Multi|双语|多音轨|Dual-Audio)\b', name_upper, re.IGNORECASE): 
            audio_count_str = 'Multi'
            
        if audio_count_str:
            info_dict['audio_count'] = audio_count_str

        # 提取音频格式
        if re.search(r'ATMOS', name_upper): audio_info.append('Atmos')
        elif re.search(r'TRUEHD', name_upper): audio_info.append('TrueHD')
        elif re.search(r'DTS-?HD(\s?MA)?', name_upper): audio_info.append('DTS-HD')
        elif re.search(r'DTS', name_upper): audio_info.append('DTS')
        elif re.search(r'DDP|EAC3|DOLBY\s?DIGITAL\+', name_upper): audio_info.append('DDP')
        elif re.search(r'AC3|DD', name_upper): audio_info.append('AC3')
        elif re.search(r'AAC', name_upper): audio_info.append('AAC')
        elif re.search(r'FLAC', name_upper): audio_info.append('FLAC')
        
        # 声道
        chan_match = re.search(r'(?<!\d)(7\.1|5\.1|2\.0)(?!\d)', filename)
        if chan_match: audio_info.append(chan_match.group(1))
        
        if audio_info: 
            info_dict['audio'] = " ".join(audio_info)

        # 帧率 (FPS) 提取
        fps_match = re.search(r'(?<!\d)(\d{2,3}FPS)\b', name_upper)
        if fps_match:
            info_dict['fps'] = fps_match.group(1).lower() # 统一转为小写 60fps

        # 流媒体平台识别 (扩充国内平台与HQ标识)
        stream_match = re.search(r'\b(NF|AMZN|DSNP|HMAX|HULU|NETFLIX|DISNEY\+|APPLETV\+|B-GLOBAL|ITUNES|IQ|YK|TC|VIU|HQ)\b', name_upper)
        if stream_match:
            info_dict['stream'] = stream_match.group(1)

        # 6. 发布组 (Group)
        try:
            from tasks import helpers
            for group_name, patterns in helpers.RELEASE_GROUPS.items():
                for pattern in patterns:
                    match = re.search(pattern, filename, re.IGNORECASE)
                    if match:
                        info_dict['group'] = match.group(0) 
                        break
                if info_dict['group']: break
            if not info_dict['group']:
                match_suffix = re.search(r'-([a-zA-Z0-9]+)$', os.path.splitext(filename)[0])
                if match_suffix and len(match_suffix.group(1)) > 2 and match_suffix.group(1).upper() not in ['1080P', '2160P', '4K', 'HDR', 'H265', 'H264']:
                    info_dict['group'] = match_suffix.group(1)
        except: pass

        return info_dict

    def _parse_ffprobe_rate(self, value):
        """解析 ffprobe 的帧率字段，例如 24000/1001 -> 23.976"""
        if not value or value == "0/0":
            return None
        try:
            if "/" in str(value):
                a, b = str(value).split("/", 1)
                a = float(a)
                b = float(b)
                if b == 0:
                    return None
                return a / b
            return float(value)
        except Exception:
            return None

    def _probe_mediainfo_with_ffprobe(self, file_node, sha1=None, silent_log=False):
        """
        最终兜底：通过 115 直链调用容器内 ffprobe。
        返回 Emby MediaSourceInfo 标准兼容结构，可直接写入 p115_mediainfo_cache。
        """
        if not file_node:
            return None

        if isinstance(file_node, dict):
            pick_code = (
                file_node.get('pc')
                or file_node.get('pick_code')
                or file_node.get('pickcode')
            )
            original_name = (
                file_node.get('fn')
                or file_node.get('n')
                or file_node.get('file_name')
                or file_node.get('original_name')
                or sha1
                or "unknown"
            )
        else:
            pick_code = (
                getattr(file_node, 'pc', None)
                or getattr(file_node, 'pick_code', None)
                or getattr(file_node, 'pickcode', None)
            )
            original_name = (
                getattr(file_node, 'fn', None)
                or getattr(file_node, 'n', None)
                or getattr(file_node, 'file_name', None)
                or getattr(file_node, 'original_name', None)
                or sha1
                or "unknown"
            )

        if not pick_code:
            if not silent_log:
                logger.debug(f"  ➜ [ffprobe] 缺少 pick_code，跳过: {original_name}")
            return None

        try:
            import shutil
            import subprocess

            if not shutil.which("ffprobe"):
                if not silent_log:
                    logger.warning("  ➜ [ffprobe] 容器内未找到 ffprobe，请在镜像中安装 ffmpeg。")
                return None

            direct_url = None

            # 1. Cookie 直链优先
            try:
                direct_url = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
            except Exception as e:
                if not silent_log:
                    logger.debug(f"  ➜ [ffprobe] Cookie 直链获取失败: {e}")

            # 2. OpenAPI 直链兜底
            if not direct_url:
                try:
                    direct_url = self.client.openapi_downurl(pick_code, user_agent="Mozilla/5.0")
                except Exception as e:
                    if not silent_log:
                        logger.debug(f"  ➜ [ffprobe] OpenAPI 直链获取失败: {e}")

            if not direct_url:
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 无法获取直链，跳过: {original_name}")
                return None

            if not silent_log:
                logger.info(f"  ➜ [ffprobe] 尝试用 ffprobe 解析媒体信息")

            cmd = [
                "ffprobe",
                "-hide_banner",
                "-v", "error",
                "-user_agent", "Mozilla/5.0",
                "-rw_timeout", "15000000",
                "-analyzeduration", "20000000",
                "-probesize", "20000000",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                "-show_chapters",
                str(direct_url)
            ]

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=45
            )

            if proc.returncode != 0:
                err = (proc.stderr or "").strip()
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 解析失败: {original_name} -> {err[:300]}")
                return None

            probe_data = json.loads(proc.stdout or "{}")
            emby_json = self._build_emby_mediainfo_from_ffprobe(
                probe_data,
                file_node,
                sha1=sha1
            )

            if not emby_json:
                if not silent_log:
                    logger.warning(f"  ➜ [ffprobe] 未解析出有效 MediaStreams: {original_name}")
                return None

            if not silent_log:
                logger.info(f"  ➜ [ffprobe] 成功生成媒体信息 -> {original_name}")

            return emby_json

        except subprocess.TimeoutExpired:
            if not silent_log:
                logger.warning(f"  ➜ [ffprobe] 解析超时，跳过: {original_name}")
            return None
        except Exception as e:
            if not silent_log:
                logger.warning(f"  ➜ [ffprobe] 解析异常: {original_name} -> {e}", exc_info=True)
            return None
    
    def _ffprobe_rate_to_float(self, value):
        """解析 ffprobe 帧率：24000/1001 -> 23.976"""
        if not value or value == "0/0":
            return None
        try:
            value = str(value)
            if "/" in value:
                a, b = value.split("/", 1)
                a = float(a)
                b = float(b)
                if b == 0:
                    return None
                return a / b
            return float(value)
        except Exception:
            return None

    def _safe_int(self, value, default=0):
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except Exception:
            return default

    def _get_friendly_display_info(self, raw_lang, raw_title, stream_type):
        """
        返回: (底层ISO代码, UI主标题, UI副标题)
        Title 才是真理！强制纠错底层语言标签，并智能转换“语/文”。
        """
        raw_lang = str(raw_lang or "").strip()
        raw_title = str(raw_title or "").strip()

        title_lower = raw_title.lower()
        lang_lower = raw_lang.lower()
        title_clean = re.sub(r'[\.\-_+/|]+', ' ', title_lower)

        # ==========================================
        # ★ 贯彻“以 Title 为准”：如果 Title 中包含明确的语言关键字，则屏蔽底层 Lang 的干扰
        # ==========================================
        title_has_lang = False
        if any(x in title_clean for x in ["chs", "sc", "gb", "zh cn", "zh hans", "简中", "简体", "简英", "cht", "tc", "big5", "zh tw", "zh hk", "zh hant", "繁中", "繁体", "繁英", "eng", "english", "英文", "英语", "英字", "台配", "台灣", "台湾"]):
            title_has_lang = True
        else:
            for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                if any(k.lower() in title_clean for k in keywords):
                    title_has_lang = True
                    break

        if title_has_lang:
            lang_lower = "" # Title 已经有语言信息，完全忽略 raw_lang，防止冲突
            norm_lang = helpers.normalize_lang_code(raw_title) # 尝试从 title 提取底层代码
        else:
            norm_lang = helpers.normalize_lang_code(raw_lang)

        combined_text = f"{title_lower} {lang_lower}".strip()
        clean_text = re.sub(r'[\.\-_+/|]+', ' ', combined_text)

        display_lang = ""

        # ==========================================
        # ★ 核心重构：回归正道，字幕统统用 chi，靠 Title 区分简繁
        # ==========================================
        if stream_type == "Subtitle":
            has_chs = any(x in clean_text for x in ["chs", "sc", "gb", "zh cn", "zh hans", "简中", "简体", "简英"])
            has_cht = any(x in clean_text for x in ["cht", "tc", "big5", "zh tw", "zh hk", "zh hant", "繁中", "繁体", "繁英"])
            has_eng = any(x in clean_text for x in ["eng", "english", "英文", "英语", "英字"])

            if has_chs and has_eng and not has_cht:
                norm_lang = "chi"
                display_lang = "简英双语"
            elif has_cht and has_eng and not has_chs:
                norm_lang = "chi"
                display_lang = "繁英双语"
            elif has_cht and not has_chs:
                norm_lang = "chi" # 繁体字幕底层依然是中文
                display_lang = "繁体"
            elif has_chs and not has_cht:
                norm_lang = "chi"
                display_lang = "简体"
            elif has_cht and has_chs:
                norm_lang = "chi"
                display_lang = "简体"
            else:
                # 兜底：走 AUDIO_SUBTITLE_KEYWORD_MAP
                is_yue = any(k.lower() in combined_text for k in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_yue", []))
                is_chi = any(k.lower() in combined_text for k in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("sub_chi", []))
                
                if "台配" in combined_text or "台灣" in combined_text or "台湾" in combined_text:
                    norm_lang = "chi"
                    display_lang = "繁体"
                elif is_yue:
                    norm_lang = "chi" # 即使是粤语压制组，字幕也是繁体中文
                    display_lang = "繁体"
                elif is_chi:
                    norm_lang = "chi"
                    display_lang = "简体"
                else:
                    # ★ 补充：如果 Title 是纯外语（如 English, Japanese），也应该能推导出 norm_lang
                    for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                        if any(k.lower() in combined_text for k in keywords):
                            norm_lang = key.replace('sub_', '')
                            break

        else:
            # ==========================================
            # 音轨流：保留 yue，专门用于标识粤语发音
            # ==========================================
            is_yue = any(k.lower() in combined_text for k in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("yue", []))
            is_chi = any(k.lower() in combined_text for k in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.get("chi", []))

            if is_yue and not is_chi:
                norm_lang = "yue"
            elif is_chi and not is_yue:
                norm_lang = "chi"
            else:
                for key, keywords in helpers.AUDIO_SUBTITLE_KEYWORD_MAP.items():
                    if key.startswith("sub_"):
                        continue
                    if any(k.lower() in combined_text for k in keywords):
                        norm_lang = key.replace('sub_', '')
                        break

        # 格式化兜底的 display_lang
        if not display_lang:
            base_label = helpers.get_lang_display_label(norm_lang) if norm_lang else ""
            if not base_label or base_label == "未知":
                display_lang = base_label
            else:
                if stream_type == "Subtitle":
                    if base_label in ["国语", "普通话", "中文"]:
                        display_lang = "简体"
                    elif base_label in ["粤语", "广东话"]:
                        display_lang = "繁体"
                    elif base_label.endswith("语") and base_label != "无语言":
                        display_lang = base_label[:-1] + "文"
                    else:
                        display_lang = base_label
                else:
                    display_lang = base_label

        friendly_title = raw_title

        # ★ Audio Title 净化：语言头中文化 + 清理 DD5.1 / DD2.0 / 640Kbps 这类纯技术标题
        if stream_type == "Audio":
            # 1. 先把英文语言名替换成中文
            audio_replace_map = {
                "Mandarin Chinese": "国语",
                "Mandarin": "国语",
                "Chinese": "国语",
                "Cantonese": "粤语",
                "English": "英语",
                "Japanese": "日语",
                "Korean": "韩语",
                "Director's Commentary": "导评",
                "Audio Commentary": "导评",
                "Commentary": "导评",
            }

            for old, new in audio_replace_map.items():
                friendly_title = re.sub(rf'\b{re.escape(old)}\b', new, friendly_title, flags=re.IGNORECASE)

            # 2. 判断 Title 是否只是纯技术参数
            title_compact = re.sub(r'[\s\.\-_]+', '', friendly_title.lower())

            is_pure_audio_tech_title = bool(re.fullmatch(
                r'(dd|ddp|ac3|eac3|dts|dtshdma|truehd|aac|flac)?'
                r'(\d{1,2}(\.\d)?)?'
                r'(\d+)?'
                r'(kbps|mbps|k)?',
                title_compact
            ))

            # 更宽松处理：DD5.1 640Kbps / DD2.0 192Kbps / AC3 stereo
            if re.fullmatch(
                r'(?i)\s*(dd|ddp|ac3|eac3|dts|dts[-\s]?hd(\s?ma)?|truehd|aac|flac)?'
                r'[\s\.\-]*(\d{1,2}(\.\d)?)?'
                r'[\s\.\-]*(stereo|mono|kbps|mbps|k|bps|\d+\s?kbps)?\s*',
                raw_title or ""
            ):
                is_pure_audio_tech_title = True

            # 3. 纯技术标题直接覆盖成语言名
            if is_pure_audio_tech_title and display_lang and display_lang != "未知":
                friendly_title = display_lang
            else:
                # 4. 非纯技术标题，去掉重复编码/声道，只保留有意义的特色词
                friendly_title = re.sub(
                    r'\b(AC3|EAC3|DTS|DTS[- ]?HD|DTS[- ]?HD MA|TRUEHD|ATMOS|AAC|FLAC|DDP|DD|5\.1|7\.1|2\.0|stereo|mono|\d+\s?kbps|\d+\s?mbps)\b',
                    '',
                    friendly_title,
                    flags=re.IGNORECASE
                )
                friendly_title = re.sub(r'\s+', ' ', friendly_title).strip(" -_/()（）")

                if not friendly_title and display_lang and display_lang != "未知":
                    friendly_title = display_lang
        
        # ==========================================
        # ★ 智能标题处理：保留有用信息，替换不规范词，追加缺失属性
        # ==========================================
        # 抹杀冗余标题，强制使用我们精简的 display_lang
        if stream_type == "Subtitle" and display_lang == "简英双语":
            friendly_title = "简英双语（简体）"
        elif stream_type == "Subtitle" and display_lang == "繁英双语":
            friendly_title = "繁英双语（繁体）"
        redundant_exact_matches = {
            "yue", "cn", "cht", "tc", "chi", "zho", "zh", "chs", "sc", 
            "粵語", "國語", "粤语", "国语", "简中", "繁中", "简体", "繁体", 
            "中文", "英语", "英文", "english", "korean", "韩语", "韩文",
            "中文(简体)", "中文（简体）", "简体中文", 
            "中文(繁体)", "中文（繁體）", "繁体中文", "繁體中文",
            "simplified", "traditional", "simplified(简体)", "traditional(繁体)"
        }
        
        if not friendly_title or friendly_title.lower().replace(" ", "") in redundant_exact_matches or friendly_title.lower() == raw_lang:
            friendly_title = display_lang if display_lang and display_lang != "未知" else raw_title
        elif not (stream_type == "Subtitle" and display_lang in ["简英双语", "繁英双语"]):
            # 2. 有实质内容的标题，进行智能替换和追加
            if stream_type == "Subtitle":
                # 替换不规范的简繁称呼，确保能触发 Emby 的 Simplified/Traditional 机制
                replace_map = {
                    "简中": "简体", "简体中文": "简体", "中文(简体)": "简体", "中文（简体）": "简体",
                    "繁中": "繁体", "繁体中文": "繁体", "中文(繁体)": "繁体", "中文（繁體）": "繁体",
                    "Director's Commentary": "导评",
                    "Audio Commentary": "导评",
                    "Commentary": "导评",
                    "commentary": "导评"
                }
                for old, new in replace_map.items():
                    friendly_title = friendly_title.replace(old, new)

                # 修复“双语双语”的叠加 Bug
                friendly_title = friendly_title.replace("简英双语", "简英").replace("简英", "简英双语")
                friendly_title = friendly_title.replace("繁英双语", "繁英").replace("繁英", "繁英双语")

                # 如果推导出了明确的简繁属性，且标题里没有，则在末尾追加
                if display_lang in ["简体", "繁体", "简英双语", "繁英双语"]:
                    core_keyword = display_lang.replace("双语", "") # 变成 简体, 繁体, 简英, 繁英
                    if core_keyword not in friendly_title:
                        friendly_title = f"{friendly_title} ({display_lang})"

        if not display_lang:
            display_lang = "未知"
        if not norm_lang:
            norm_lang = raw_lang

        return norm_lang, display_lang, friendly_title

    def _channel_layout_label(self, channels, channel_layout=None):
        channel_layout = (channel_layout or "").lower()

        if channels == 8:
            return "7.1"
        if channels == 7:
            return "6.1"
        if channels == 6:
            return "5.1"
        if channels == 2:
            return "stereo"
        if channels == 1:
            return "mono"

        if channel_layout:
            return channel_layout.replace("(side)", "")

        return str(channels) if channels else ""

    def _audio_codec_profile_label(self, codec, profile="", title=""):
        codec = (codec or "").lower()
        profile_mix = f"{profile or ''} {title or ''}".lower()

        if codec == "truehd":
            return "TRUEHD Atmos" if "atmos" in profile_mix else "TRUEHD"

        if codec == "eac3":
            return "DDP Atmos" if "atmos" in profile_mix else "DDP"

        if codec == "dts":
            if "ma" in profile_mix or "master" in profile_mix or "xll" in profile_mix:
                return "DTS-HD MA"
            if "hra" in profile_mix or "high resolution" in profile_mix:
                return "DTS-HD HRA"
            return "DTS"

        if codec == "ac3":
            return "AC3"
        if codec == "aac":
            return "AAC"
        if codec == "flac":
            return "FLAC"
        if codec == "opus":
            return "OPUS"
        if codec == "mp3":
            return "MP3"

        return codec.upper() if codec else ""

    def _subtitle_codec_label(self, codec):
        codec = (codec or "").lower()

        mapping = {
            "hdmv_pgs_subtitle": "PGSSUB",
            "pgssub": "PGSSUB",
            "subrip": "SUBRIP",
            "srt": "SUBRIP",
            "ass": "ASS",
            "ssa": "SSA",
            "webvtt": "VTT",
            "mov_text": "MOV_TEXT",
            "dvd_subtitle": "DVDSUB",
        }

        return mapping.get(codec, codec.upper() if codec else "")

    def _build_emby_mediainfo_from_ffprobe(self, probe_data, file_node, sha1=None):
        """
        将 ffprobe 原始 JSON 转成 Emby MediaSourceInfo 兼容格式。
        注意：这是给 p115_mediainfo_cache 用的，不是给 ffprobe 自己用的。
        """
        if not probe_data:
            return None

        original_name = (
            file_node.get("fn")
            or file_node.get("n")
            or file_node.get("file_name")
            or file_node.get("original_name")
            or sha1
            or "unknown"
        )

        ext = ""
        if "." in original_name:
            ext = original_name.rsplit(".", 1)[-1].lower()

        fmt = probe_data.get("format") or {}
        streams = probe_data.get("streams") or []
        chapters_raw = probe_data.get("chapters") or []

        media_streams = []

        size = self._safe_int(
            file_node.get("fs")
            or file_node.get("size")
            or fmt.get("size")
            or 0
        )

        duration = 0.0
        try:
            duration = float(fmt.get("duration") or 0)
        except Exception:
            duration = 0.0

        run_time_ticks = int(duration * 10000000) if duration > 0 else 0

        bitrate = self._safe_int(fmt.get("bit_rate") or 0)
        if not bitrate and size and duration > 0:
            bitrate = int(size * 8 / duration)

        container = ext
        if not container:
            format_name = (fmt.get("format_name") or "").lower()
            if "matroska" in format_name:
                container = "mkv"
            elif "mov" in format_name or "mp4" in format_name:
                container = "mp4"
            else:
                container = format_name.split(",")[0] if format_name else ""

        # 章节
        chapters = []
        for idx, ch in enumerate(chapters_raw):
            tags = ch.get("tags") or {}
            try:
                start_time = float(ch.get("start_time") or 0)
            except Exception:
                start_time = 0

            chapters.append({
                "Name": tags.get("title") or f"章节 {idx + 1}",
                "MarkerType": "Chapter",
                "ChapterIndex": idx,
                "StartPositionTicks": int(start_time * 10000000)
            })

        for s in streams:
            codec_type = (s.get("codec_type") or "").lower()
            codec = (s.get("codec_name") or "").lower()
            tags = s.get("tags") or {}
            disposition = s.get("disposition") or {}

            index = self._safe_int(s.get("index"), len(media_streams))
            is_default = bool(disposition.get("default"))
            is_forced = bool(disposition.get("forced"))

            if codec_type == "video":
                title = tags.get("title") or ""
                width = self._safe_int(s.get("width"))
                height = self._safe_int(s.get("height"))

                bit_depth = 0
                for k in ("bits_per_raw_sample", "bits_per_sample"):
                    bit_depth = self._safe_int(s.get(k))
                    if bit_depth:
                        break

                pix_fmt = (s.get("pix_fmt") or "").lower()
                if not bit_depth:
                    if "12" in pix_fmt:
                        bit_depth = 12
                    elif "10" in pix_fmt:
                        bit_depth = 10
                    elif pix_fmt:
                        bit_depth = 8

                fps = (
                    self._ffprobe_rate_to_float(s.get("avg_frame_rate"))
                    or self._ffprobe_rate_to_float(s.get("r_frame_rate"))
                )

                color_space = s.get("colorspace") or s.get("color_space") or ""
                color_transfer = s.get("color_transfer") or ""
                color_primaries = s.get("color_primaries") or ""
                profile = s.get("profile") or ""

                side_data_list = s.get("side_data_list") or []
                side_text = json.dumps(side_data_list, ensure_ascii=False)

                video_range = ""
                extended_video_type = ""
                extended_video_sub_type = "None"
                extended_video_desc = "None"

                # Dolby Vision
                dv_profile = None
                dv_compat = None

                for sd in side_data_list:
                    if not isinstance(sd, dict):
                        continue

                    side_type = str(sd.get("side_data_type") or "")
                    if "DOVI" in side_type.upper() or "DOLBY VISION" in side_type.upper():
                        dv_profile = sd.get("dv_profile") or sd.get("profile")
                        dv_compat = (
                            sd.get("dv_bl_signal_compatibility_id")
                            or sd.get("bl_signal_compatibility_id")
                            or sd.get("compatibility_id")
                        )
                        break

                if not dv_profile and ("DOVI" in side_text.upper() or "DOLBY VISION" in side_text.upper()):
                    m = re.search(r"dv_profile['\"]?\s*[:=]\s*['\"]?(\d+)", side_text, re.IGNORECASE)
                    if m:
                        dv_profile = m.group(1)

                if dv_profile:
                    dv_profile_str = str(dv_profile)
                    extended_video_type = "DolbyVision"

                    if dv_profile_str == "8":
                        if str(dv_compat or "") == "1":
                            extended_video_sub_type = "DoviProfile81"
                            extended_video_desc = "Profile 8.1 (HDR10 compatible)"
                        else:
                            extended_video_sub_type = "DoviProfile8"
                            extended_video_desc = "Profile 8"
                    elif dv_profile_str == "7":
                        extended_video_sub_type = "DoviProfile7"
                        extended_video_desc = "Profile 7 (HDR10 compatible)" if color_transfer == "smpte2084" else "Profile 7"
                    elif dv_profile_str == "5":
                        extended_video_sub_type = "DoviProfile5"
                        extended_video_desc = "Profile 5"
                    else:
                        extended_video_sub_type = f"DoviProfile{dv_profile_str}"
                        extended_video_desc = f"Profile {dv_profile_str}"

                    # 让现有解析器能得到 HDR10 DoVi P7 / HDR10 DoVi P8
                    if color_transfer == "smpte2084":
                        video_range = "DolbyVision HDR10"
                    else:
                        video_range = "DolbyVision"

                # HDR10+
                elif "HDR10+" in side_text or "SMPTE2094-40" in side_text:
                    video_range = "HDR10+"

                # HDR10 / HDR
                elif color_transfer == "smpte2084":
                    video_range = "HDR10"
                elif color_primaries == "bt2020":
                    video_range = "HDR"

                video_codec_display = {
                    "hevc": "HEVC",
                    "h264": "H264",
                    "av1": "AV1",
                    "mpeg2video": "MPEG2VIDEO",
                    "vc1": "VC1",
                }.get(codec, codec.upper())

                if width >= 3800:
                    res_display = "4K"
                elif width >= 1900:
                    res_display = "1080p"
                elif width >= 1200:
                    res_display = "720p"
                else:
                    res_display = f"{height}p" if height else ""

                if extended_video_type == "DolbyVision":
                    effect_display = "Dolby Vision"
                elif video_range == "HDR10+":
                    effect_display = "HDR10+"
                elif video_range == "HDR10":
                    effect_display = "HDR10"
                elif video_range == "HDR":
                    effect_display = "HDR"
                else:
                    effect_display = ""

                display_title = " ".join([x for x in [res_display, effect_display, video_codec_display] if x])

                media_streams.append({
                    "Type": "Video",
                    "Codec": codec,
                    "Index": index,
                    "Level": self._safe_int(s.get("level")),
                    "Title": title,
                    "Width": width,
                    "Height": height,
                    "BitRate": self._safe_int(s.get("bit_rate") or bitrate),
                    "Profile": profile,
                    "BitDepth": bit_depth,
                    "IsForced": is_forced,
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "RefFrames": self._safe_int(s.get("refs"), 1),
                    "ColorSpace": color_space,
                    "IsExternal": False,
                    "VideoRange": video_range,
                    "AspectRatio": f"{width}:{height}" if width and height else "",
                    "PixelFormat": pix_fmt,
                    "DisplayTitle": display_title,
                    "IsAnamorphic": False,
                    "IsInterlaced": False,
                    "ColorTransfer": color_transfer,
                    "RealFrameRate": fps,
                    "AttachmentSize": 0,
                    "ColorPrimaries": color_primaries,
                    "AverageFrameRate": fps,
                    "ExtendedVideoType": extended_video_type or "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": extended_video_sub_type,
                    "IsTextSubtitleStream": False,
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": extended_video_desc
                })

            elif codec_type == "audio":
                raw_lang = tags.get("language")
                raw_title = tags.get("title")
                
                # ★ 调用新的智能解析方法
                lang, display_lang, title = self._get_friendly_display_info(raw_lang, raw_title, "Audio")

                channels = self._safe_int(s.get("channels"))
                channel_layout = self._channel_layout_label(channels, s.get("channel_layout"))
                sample_rate = self._safe_int(s.get("sample_rate"))

                profile = s.get("profile") or ""
                codec_display = self._audio_codec_profile_label(codec, profile, title)

                display_title_parts = []
                if display_lang and display_lang != "未知":
                    display_title_parts.append(display_lang)
                if codec_display:
                    display_title_parts.append(codec_display)
                if channel_layout:
                    display_title_parts.append(channel_layout)
                if is_default:
                    display_title_parts.append("(默认)")

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Audio",
                    "Codec": codec,
                    "Index": index,
                    "Title": title, # ★ 净化后的副标题
                    "BitRate": self._safe_int(s.get("bit_rate")),
                    "BitDepth": self._safe_int(s.get("bits_per_raw_sample") or s.get("bits_per_sample")),
                    "Channels": channels,
                    "IsForced": is_forced,
                    "Language": lang, # ★ 伪装后的底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "SampleRate": sample_rate,
                    "DisplayTitle": display_title, # ★ 完美的 UI 标题 (如: 国语 AAC stereo (默认))
                    "IsInterlaced": False,
                    "ChannelLayout": channel_layout,
                    "AttachmentSize": 0,
                    "DisplayLanguage": display_lang, # ★ 完美的 UI 语言 (如: 国语)
                    "ExtendedVideoType": "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": "None",
                    "IsTextSubtitleStream": False,
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": "None",
                    "Profile": profile or codec_display
                })

            elif codec_type == "subtitle":
                raw_lang = tags.get("language")
                raw_title = tags.get("title")
                
                # ★ 调用新的智能解析方法
                lang, display_lang, title = self._get_friendly_display_info(raw_lang, raw_title, "Subtitle")

                sub_codec = self._subtitle_codec_label(codec)
                is_text_sub = codec in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"}

                display_title_parts = []
                if display_lang and display_lang != "未知":
                    display_title_parts.append(display_lang)
                if is_default:
                    display_title_parts.append(f"(默认 {sub_codec})")
                else:
                    display_title_parts.append(f"({sub_codec})")

                display_title = " ".join(display_title_parts)

                media_streams.append({
                    "Type": "Subtitle",
                    "Codec": sub_codec,
                    "Index": index,
                    "Title": title, # ★ 净化后的副标题
                    "IsForced": is_forced,
                    "Language": lang, # ★ 伪装后的底层 ISO 代码
                    "Protocol": "File",
                    "TimeBase": s.get("time_base") or "1/1000",
                    "IsDefault": is_default,
                    "IsExternal": False,
                    "DisplayTitle": display_title, # ★ 完美的 UI 标题 (如: 简中 (默认 SRT))
                    "IsInterlaced": False,
                    "AttachmentSize": 0,
                    "DisplayLanguage": display_lang, # ★ 完美的 UI 语言 (如: 简中)
                    "ExtendedVideoType": "None",
                    "IsHearingImpaired": False,
                    "ExtendedVideoSubType": "None",
                    "IsTextSubtitleStream": is_text_sub,
                    "SubtitleLocationType": "InternalStream",
                    "SupportsExternalStream": False,
                    "ExtendedVideoSubTypeDescription": "None"
                })

        # =================================================================
        # ★★★ 终极智能默认轨道选择算法 (独立配置 + 拖拽优先级 + 智能跟随) ★★★
        # =================================================================
        def _set_smart_default_streams(streams):
            # 1. 从独立数据库读取用户配置
            stream_config = settings_db.get_setting('p115_default_stream_config') or {
                "audio_lang": "",
                "subtitle_lang": "",
                "audio_features": ["国配", "上译", "京译", "长译", "八一", "台配", "粤语", "评论", "导评"],
                "sub_priority": ["effect", "chs_eng", "cht_eng", "chs", "cht"]
            }
            audio_pref_code = stream_config.get("audio_lang", "")
            subtitle_pref = stream_config.get("subtitle_lang", "")
            audio_features_config = stream_config.get("audio_features", [])
            sub_priority = stream_config.get("sub_priority", [])

            audio_streams = [s for s in streams if s.get("Type") == "Audio"]
            sub_streams = [s for s in streams if s.get("Type") == "Subtitle"]

            default_audio = None
            
            # -----------------------------------------
            # 1. 决出默认音轨 (真太子)
            # -----------------------------------------
            if audio_streams:
                candidates = audio_streams

                if audio_pref_code:
                    lang_matched = [s for s in candidates if s.get("Language") == audio_pref_code]
                    if lang_matched:
                        candidates = lang_matched

                # 在候选者中，优先选原本就是默认的；如果候选里没有默认，就选第一条
                default_audio = next((s for s in candidates if s.get("IsDefault")), candidates[0])

                for s in audio_streams:
                    is_target = (s == default_audio)
                    s["IsDefault"] = is_target

                    import re
                    dt = re.sub(r'\(默认\s*', '(', s.get("DisplayTitle", ""))
                    dt = dt.replace('(默认)', '').replace('默认', '').replace('()', '').strip()
                    if is_target:
                        dt += " (默认)"
                    s["DisplayTitle"] = dt.replace("  ", " ")

            # -----------------------------------------
            # 2. 决出默认字幕 (智能跟随最高优 + 用户自定义优先级打分)
            # -----------------------------------------
            if sub_streams:
                default_sub = None
                audio_title = (default_audio.get("Title", "") + " " + default_audio.get("DisplayTitle", "")) if default_audio else ""
                audio_title_lower = audio_title.lower()
                
                # 提取真太子音轨命中的特征词
                active_audio_features = []
                for kw in audio_features_config:
                    if kw.lower() in audio_title_lower:
                        active_audio_features.append(kw.lower())

                # ★ 核心打分函数
                def get_sub_score(sub):
                    score = 0
                    sub_title = (sub.get("Title", "") + " " + sub.get("DisplayTitle", "")).lower()
                    codec = sub.get("Codec", "").upper()

                    # 统一特征判断
                    is_effect = (
                        "特效" in sub_title
                        or "effect" in sub_title
                        or "effects" in sub_title
                    )

                    is_chs_eng = (
                        "简英" in sub_title
                        or "中英" in sub_title
                        or "chs/eng" in sub_title
                        or "chs&eng" in sub_title
                        or "chs.eng" in sub_title
                    )

                    is_cht_eng = (
                        "繁英" in sub_title
                        or "cht/eng" in sub_title
                        or "cht&eng" in sub_title
                        or "cht.eng" in sub_title
                    )

                    is_chs = (
                        "简体" in sub_title
                        or "简中" in sub_title
                        or "chs" in sub_title
                    ) and not is_chs_eng

                    is_cht = (
                        "繁体" in sub_title
                        or "繁中" in sub_title
                        or "cht" in sub_title
                    ) and not is_cht_eng

                    # 优先级 1: 智能跟随音轨特征
                    if active_audio_features and any(f in sub_title for f in active_audio_features):
                        score += 100000

                    # 优先级 2: 用户拖拽顺序
                    priority_score = 0
                    for idx, p_type in enumerate(reversed(sub_priority)):
                        weight = (idx + 1) * 1000

                        if p_type == "effect" and is_effect:
                            priority_score = max(priority_score, weight)
                        elif p_type == "chs_eng" and is_chs_eng:
                            priority_score = max(priority_score, weight)
                        elif p_type == "cht_eng" and is_cht_eng:
                            priority_score = max(priority_score, weight)
                        elif p_type == "chs" and is_chs:
                            priority_score = max(priority_score, weight)
                        elif p_type == "cht" and is_cht:
                            priority_score = max(priority_score, weight)

                    score += priority_score

                    # 字幕简繁偏好只能做小加分，不能推翻拖拽排序
                    if subtitle_pref:
                        if subtitle_pref == "chs" and (is_chs or is_chs_eng):
                            score += 50
                        elif subtitle_pref == "cht" and (is_cht or is_cht_eng):
                            score += 50

                    # 原本默认只做极小兜底，不能压过用户排序
                    if sub.get("IsDefault"):
                        score += 1

                    return score

                # 按分数降序排列，取最高分作为默认字幕
                sorted_subs = sorted(sub_streams, key=get_sub_score, reverse=True)
                default_sub = sorted_subs[0]

                for s in sub_streams:
                    is_target = (s == default_sub)
                    s["IsDefault"] = is_target
                    import re
                    dt = re.sub(r'\(默认\s*', '(', s.get("DisplayTitle", ""))
                    dt = dt.replace('(默认)', '').replace('默认', '').replace('()', '').strip()
                    if is_target: dt += " (默认)"
                    s["DisplayTitle"] = dt.replace("  ", " ")

        # 执行智能篡改 (不再需要传 pref_language_code)
        _set_smart_default_streams(media_streams)

        if not media_streams:
            return None

        media_source_info = {
            "Size": size,
            "Type": "Default",
            "Bitrate": bitrate,
            "Formats": [],
            "Chapters": [],
            "IsRemote": True,
            "Protocol": "File",
            "Container": container,
            "MediaStreams": media_streams,
            "RunTimeTicks": run_time_ticks,
            "RequiresClosing": False,
            "RequiresLooping": False,
            "RequiresOpening": False,
            "SupportsProbing": True,
            "IsInfiniteStream": False,
            "HasMixedProtocols": False,
            "SupportsDirectPlay": True,
            "RequiredHttpHeaders": {},
            "SupportsTranscoding": True,
            "SupportsDirectStream": True,
            "ReadAtNativeFramerate": False,
            "AddApiKeyToDirectStreamUrl": False
        }

        return [{
            "Chapters": chapters,
            "MediaSourceInfo": media_source_info
        }]

    def _fetch_and_parse_mediainfo(self, sha1, guessed_info=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None, file_node=None, silent_log=False):
        """
        通过 SHA1 获取真实的媒体信息，并转换为乐高重命名参数
        """
        if not sha1: return {}, False
        
        raw_json = None
        is_center = False
        data_source = "本地缓存"

        # 1. ★ 核心优化：直接从内存字典读取本地缓存，彻底消除数据库 I/O 瓶颈！
        if local_pre_fetched_mediainfo and sha1 in local_pre_fetched_mediainfo:
            raw_json = local_pre_fetched_mediainfo[sha1]

        # 2. 本地没有，优先查批量预获取的字典 (瞬间读取，无网络延迟)
        if not raw_json and pre_fetched_mediainfo and sha1 in pre_fetched_mediainfo:
            raw_json = pre_fetched_mediainfo[sha1]
            is_center = True
            data_source = "中心服务器(批量)"

        # 3. 尝试查 P115Center 中心服务器 (单次查询)
        if not raw_json and pre_fetched_mediainfo is None:
            try:
                import extensions
                processor = extensions.media_processor_instance
                if processor and getattr(processor, 'p115_center', None):
                    resp = processor.p115_center.download_emby_mediainfo_data([sha1])
                    if resp and sha1 in resp:
                        raw_json = resp[sha1]
                        is_center = True
                        data_source = "中心服务器(单次)"
                        P115CacheManager.save_mediainfo_cache(sha1, raw_json)
            except Exception:
                pass

        # 4. 本地和中心服务器都没有，最终用 ffprobe 解析 115 直链，并写入本地缓存
        if not raw_json and file_node:
            raw_json = self._probe_mediainfo_with_ffprobe(
                file_node,
                sha1=sha1,
                silent_log=silent_log
            )

            if raw_json:
                is_center = False
                data_source = "ffprobe解析"

                # 写入 p115_mediainfo_cache，后续同 SHA1 直接走本地缓存
                P115CacheManager.save_mediainfo_cache(sha1, raw_json)

                # 同步塞回本轮预取字典，避免同一批里重复 probe
                if local_pre_fetched_mediainfo is not None:
                    local_pre_fetched_mediainfo[str(sha1).upper()] = raw_json
                    local_pre_fetched_mediainfo[str(sha1)] = raw_json

        if not raw_json:
            return {}, False

        # 5. 开始解析 Emby 的真实数据
        info = {}
        try:
            if isinstance(raw_json, list) and len(raw_json) > 0:
                source_info = raw_json[0].get("MediaSourceInfo", raw_json[0])
            else:
                source_info = raw_json

            streams = source_info.get("MediaStreams", [])
            
            # ★ 核心修复：物理数据是唯一真理。只要存在流信息，强制接管这 6 个核心参数
            if streams:
                # 默认全部置空。如果下方没有解析出对应属性，就会用空值彻底洗掉文件名瞎猜的错误标签
                info = {
                    'resolution': '', 'codec': '', 'effect': '', 
                    'fps': '', 'audio': '', 'audio_count': ''
                }

                video_stream = next((s for s in streams if s.get("Type") == "Video"), None)
                audio_streams = [s for s in streams if s.get("Type") == "Audio"]

                if video_stream:
                    w = video_stream.get("Width", 0)
                    if w >= 3800: info['resolution'] = '2160p'
                    elif w >= 1900: info['resolution'] = '1080p'
                    elif w >= 1200: info['resolution'] = '720p'
                    elif w > 0: info['resolution'] = '480p' # 补充低分辨率兜底，防止伪 4K 逃脱

                    codec_raw = video_stream.get("Codec", "").lower()
                    codec_map = {'hevc': 'HEVC', 'h265': 'HEVC', 'h264': 'AVC', 'avc': 'AVC', 'av1': 'AV1'}
                    c_str = codec_map.get(codec_raw, codec_raw.upper())
                    
                    bit_depth = video_stream.get("BitDepth")
                    if bit_depth and bit_depth > 8:
                        info['codec'] = f"{c_str} {bit_depth}bit"
                    else:
                        info['codec'] = c_str

                    v_range = video_stream.get("VideoRange", "")
                    ext_type = video_stream.get("ExtendedVideoType", "")
                    ext_sub_type = video_stream.get("ExtendedVideoSubType", "")
                    ext_desc = video_stream.get("ExtendedVideoSubTypeDescription", "")

                    is_dv = "DolbyVision" in v_range or "DolbyVision" in ext_type
                    
                    hdr_str = ""
                    if "HDR10+" in v_range or "HDR10+" in ext_desc: hdr_str = "HDR10+"
                    elif "HDR10" in v_range or "HDR10" in ext_desc: hdr_str = "HDR10"
                    elif "HDR" in v_range or video_stream.get("ColorTransfer") == "smpte2084": hdr_str = "HDR"

                    dv_str = "DV"
                    if is_dv:
                        if "Profile8" in ext_sub_type or "Profile 8" in ext_desc: dv_str = "DoVi P8"
                        elif "Profile7" in ext_sub_type or "Profile 7" in ext_desc: dv_str = "DoVi P7"
                        elif "Profile5" in ext_sub_type or "Profile 5" in ext_desc: dv_str = "DoVi P5"
                        else: dv_str = "DoVi"

                    if is_dv and hdr_str: info['effect'] = f"{hdr_str} {dv_str}"
                    elif is_dv: info['effect'] = dv_str
                    elif hdr_str: info['effect'] = hdr_str

                    fps = video_stream.get("RealFrameRate") or video_stream.get("AverageFrameRate")
                    if fps: info['fps'] = f"{round(fps)}fps"

                # ★ 提取真实的音轨和字幕语言数组，供洗版裁判使用
                info['audio_langs'] = []
                info['sub_langs'] = []
                
                def _extract_lang_from_stream(stream_dict):
                    # 1. 优先取 Language 字段
                    lang = stream_dict.get("Language")
                    if lang: return lang
                    
                    # 2. 其次从 Title 或 DisplayTitle 中模糊匹配
                    text_to_search = f"{stream_dict.get('Title', '')} {stream_dict.get('DisplayTitle', '')}".lower()
                    if 'chi' in text_to_search or 'zh' in text_to_search or '中' in text_to_search or '国语' in text_to_search or '粤语' in text_to_search:
                        return 'chi'
                    if 'eng' in text_to_search or 'en' in text_to_search or '英' in text_to_search:
                        return 'eng'
                    if 'jpn' in text_to_search or 'ja' in text_to_search or '日' in text_to_search:
                        return 'jpn'
                    if 'kor' in text_to_search or 'ko' in text_to_search or '韩' in text_to_search:
                        return 'kor'
                    return None

                for s in streams:
                    if s.get("Type") == "Audio":
                        l = _extract_lang_from_stream(s)
                        if l: info['audio_langs'].append(l)
                    elif s.get("Type") == "Subtitle":
                        l = _extract_lang_from_stream(s)
                        if l: info['sub_langs'].append(l)

                if audio_streams:
                    audio_tags = []
                    
                    # ★ 如果这里只有 1 条音轨，info['audio_count'] 会保持上面的空字符串，从而洗掉文件名的 "2Audios"
                    num_audios = len(audio_streams)
                    if num_audios > 1: 
                        info['audio_count'] = f"{num_audios}Audios"

                    primary_audio = next((s for s in audio_streams if s.get("IsDefault")), audio_streams[0])
                    acodec = primary_audio.get("Codec", "").lower()
                    profile = primary_audio.get("Profile", "").lower()

                    if acodec == 'truehd' and 'atmos' in profile: audio_tags.append("TrueHD Atmos")
                    elif acodec == 'truehd': audio_tags.append("TrueHD")
                    elif acodec == 'dts' and 'ma' in profile: audio_tags.append("DTS-HD MA")
                    elif acodec == 'dts': audio_tags.append("DTS")
                    elif acodec == 'eac3': audio_tags.append("DDP")
                    elif acodec == 'ac3': audio_tags.append("AC3")
                    elif acodec == 'aac': audio_tags.append("AAC")
                    elif acodec == 'flac': audio_tags.append("FLAC")

                    channels = primary_audio.get("Channels")
                    if channels == 8: audio_tags.append("7.1")
                    elif channels == 6: audio_tags.append("5.1")
                    elif channels == 2: audio_tags.append("2.0")

                    if audio_tags:
                        info['audio'] = " ".join(audio_tags)

        except Exception as e:
            logger.warning(f"  ➜ 解析真实媒体信息失败: {e}")

        return info, is_center

    def _build_name_from_format(self, format_array, is_tv=False, season_num=None, episode_num=None, original_title=None, video_info=None, safe_title=None):
        """解析乐高轨道生成名称 (支持目录和文件，自动过滤特殊字符)"""
        if not format_array: return ""
        
        evaluated = []
        for raw_id in format_array:
            block = raw_id.rsplit('_', 1)[0] if re.search(r'_\d+$', raw_id) else raw_id
            val = None
            is_sep = False
            
            # 优先使用传入的 safe_title，防止文件名包含 \/:*?"<>| 导致报错
            if block == 'title_zh': val = safe_title if safe_title else (self.details.get('title') or self.original_title)
            elif block == 'title_en': val = self.details.get('title_en') or original_title or self.details.get('original_title') or self.original_title
            elif block == 'title_orig': val = original_title or self.details.get('original_title') or self.original_title
            elif block == 'year': val = f"({self.details.get('date', '')[:4]})" if self.details.get('date') else None
            elif block == 'year_pure': val = self.details.get('date', '')[:4] if self.details.get('date') else None
            elif block == 'tmdb_bracket': val = f"{{tmdb={self.tmdb_id}}}"
            elif block == 'tmdb_square': val = f"[tmdbid={self.tmdb_id}]"
            elif block == 'tmdb_dash': val = f"tmdb-{self.tmdb_id}"
            elif block == 's_e' and is_tv: 
                s_val = season_num if season_num is not None else 1
                e_val = episode_num if episode_num is not None else 1
                val = f"S{s_val:02d}E{e_val:02d}" 
            elif block == 'season_name_en' and is_tv: val = f"Season {season_num:02d}" if season_num else None
            elif block == 'season_name_en_no0' and is_tv: val = f"Season {season_num}" if season_num else None
            elif block == 'season_name_zh' and is_tv: val = f"第{season_num}季" if season_num else None
            elif block == 'season_name_s' and is_tv: val = f"S{season_num:02d}" if season_num else None
            elif block == 'season_name_s_no0' and is_tv: val = f"S{season_num}" if season_num else None
            elif video_info and block in video_info: val = video_info.get(block)
            elif block.startswith('sep_'):
                is_sep = True
                if block == 'sep_slash': val = '/'
                elif block.startswith('sep_dash_space'): val = ' - '
                elif block.startswith('sep_middot_space'): val = ' · '
                elif block.startswith('sep_middot'): val = '·'
                elif block.startswith('sep_dot'): val = '.'
                elif block.startswith('sep_dash'): val = '-'
                elif block.startswith('sep_underline'): val = '_'
                elif block.startswith('sep_space'): val = ' '

            if val: evaluated.append({'val': str(val).strip() if not is_sep else val, 'is_sep': is_sep})

        # 智能消除多余分隔符
        final_parts = []
        for i, item in enumerate(evaluated):
            if item['is_sep']:
                has_content_before = any(not x['is_sep'] for x in evaluated[:i])
                has_content_after = any(not x['is_sep'] for x in evaluated[i+1:])
                is_last_sep_in_group = True
                if i + 1 < len(evaluated) and evaluated[i+1]['is_sep']:
                    is_last_sep_in_group = False
                if has_content_before and has_content_after and is_last_sep_in_group:
                    final_parts.append(item['val'])
            else:
                final_parts.append(item['val'])

        return "".join(final_parts)

    def _get_episode_regex_rules(self):
        """懒加载自定义季集号识别规则，避免每个文件都查数据库"""
        cache_attr = '_episode_regex_rules_cache'

        if not hasattr(self, cache_attr):
            try:
                rules = settings_db.get_setting('p115_episode_regex_rules') or []
                if not isinstance(rules, list):
                    rules = []
            except Exception as e:
                logger.warning(f"  ➜ [自定义季集号识别] 读取规则失败，已忽略: {e}")
                rules = []

            setattr(self, cache_attr, rules)

        return getattr(self, cache_attr, [])
    
    def _safe_group_to_int(self, match, group_index):
        """安全获取组索引，防止组索引不存在"""
        try:
            if not group_index:
                return None
            value = match.group(int(group_index))
            if value is None:
                return None
            value = str(value).strip()
            if not value:
                return None
            return int(value)
        except Exception:
            return None
        
    def _parse_season_episode_by_custom_regex(self, original_name, rel_path=''):
        """
        返回:
            (season_num, episode_num, matched_rule_name) 或 (None, None, None)
        """
        rules = self._get_episode_regex_rules()
        if not rules or not original_name:
            return None, None, None

        for idx, rule in enumerate(rules):
            if not isinstance(rule, dict):
                continue
            if not rule.get('enabled', True):
                continue

            rule_name = str(rule.get('name') or f'规则{idx + 1}').strip()
            pattern = str(rule.get('pattern') or '').strip()
            mode = str(rule.get('mode') or 'episode_only').strip()

            if not pattern:
                continue

            try:
                match = re.search(pattern, original_name, re.IGNORECASE)
                if not match and rel_path:
                    # 可选增强：允许规则匹配相对路径，适合目录名里带季号、文件名只写 01 的情况
                    match = re.search(pattern, rel_path, re.IGNORECASE)
            except re.error as e:
                logger.warning(f"  ➜ [自定义季集号识别] 规则 '{rule_name}' 正则非法，已跳过: {e}")
                continue

            if not match:
                continue

            if mode == 'season_episode':
                season_group = int(rule.get('season_group') or 1)
                episode_group = int(rule.get('episode_group') or 2)

                season_num = self._safe_group_to_int(match, season_group)
                episode_num = self._safe_group_to_int(match, episode_group)

                if season_num is not None and episode_num is not None:
                    return season_num, episode_num, rule_name

            else:
                # episode_only
                episode_group = int(rule.get('episode_group') or 1)
                default_season = int(rule.get('default_season') or 1)

                episode_num = self._safe_group_to_int(match, episode_group)
                if episode_num is not None:
                    return default_season, episode_num, rule_name

        return None, None, None

    def _rename_file_node(self, file_node, new_base_name, year=None, is_tv=False, original_title=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None, silent_log=False):
        original_name = file_node.get('fn') or file_node.get('n') or file_node.get('file_name', '')
        
        # ★ 修复 1：无后缀文件的提前返回，补齐为 8 个返回值
        if '.' not in original_name: 
            return original_name, None, None, None, False, {}, False, None

        parts = original_name.rsplit('.', 1)
        name_body = parts[0]
        ext = parts[1].lower()

        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']
        lang_suffix = ""
        if is_sub:
            # ★ 核心修复：支持提取无限连击的复合语言标签 (如 .chs&eng, .zh-cn.default, _eng.forced)
            lang_keywords = r'(?:chs|cht|zh\-cn|zh\-tw|zh|cn|tw|hk|tc|sc|eng|en|jpn|jp|kor|kr|fre|spa|ara|ger|cze|dan|fin|fil|glg|heb|hin|hun|ind|ita|kan|mal|may|nob|dut|pol|por|rum|rus|swe|tam|tel|tha|tur|ukr|vie|default|forced|sdh|cc)'
            
            # 匹配结尾由分隔符(.-_&)和语言代码组成的字符串，最多允许4个组合连击
            match = re.search(rf'((?:[\.\-\_\&]+{lang_keywords}){{1,4}})$', name_body, re.IGNORECASE)
            
            if match:
                lang_suffix = match.group(1)
                # 统一将第一个分隔符替换为点，符合 Emby 规范 (例如 _chs&eng 变成 .chs&eng)
                lang_suffix = '.' + re.sub(r'^[\.\-\_\&]+', '', lang_suffix)

            # ★★★ 强制基础名注入 (专为 MP 字幕挂起等待机制设计) ★★★
            forced_base_name = file_node.get('_forced_base_name')
            if forced_base_name:
                new_name = f"{forced_base_name}{lang_suffix}.{ext}"
                season_num = file_node.get('_forced_season')
                episode_num = file_node.get('_forced_episode')
                s_name = None
                if is_tv and season_num is not None:
                    cfg = self.rename_config
                    season_format = cfg.get('season_dir_format', ['season_name_en'])
                    s_name = self._build_name_from_format(
                        season_format, 
                        is_tv=True, 
                        season_num=season_num, 
                        original_title=original_title, 
                        safe_title=new_base_name
                    )
                    if not s_name: s_name = f"Season {season_num:02d}"
                
                # ★ 修复 2：字幕文件的提前返回，补齐为 8 个返回值
                return new_name, season_num, episode_num, s_name, False, {}, False, None

        cfg = self.rename_config
        
        # 提取视频信息字典 (基于文件名的猜测)
        search_name = original_name
        if is_sub and lang_suffix and name_body.endswith(lang_suffix):
            search_name = f"{name_body[:-len(lang_suffix)]}.mkv"
        video_info = self._extract_video_info(search_name)

        # 基于 SHA1 获取真实参数
        is_center_cached = False
        real_info = None
        
        if not is_sub:
            sha1 = file_node.get('sha1') or file_node.get('sha')
            if sha1:
                real_info, is_center_cached = self._fetch_and_parse_mediainfo(
                    sha1,
                    video_info,
                    pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo,
                    file_node=file_node,
                    silent_log=silent_log
                )
                if real_info:
                    for k, v in real_info.items():
                        video_info[k] = v
                    
        # 解析季集号
        # ★ 优先使用 Webhook 强塞进来的精准数据
        season_num = file_node.get('_forced_season')
        episode_num = file_node.get('_forced_episode')

        if is_tv and (season_num is None or episode_num is None):
            rel_path = file_node.get('rel_path', '')

            # 0. ★ 先跑用户自定义规则，命中即优先使用
            custom_season, custom_episode, custom_rule_name = self._parse_season_episode_by_custom_regex(
                original_name=original_name,
                rel_path=rel_path
            )

            if custom_season is not None and season_num is None:
                season_num = custom_season
            if custom_episode is not None and episode_num is None:
                episode_num = custom_episode

            if custom_rule_name and not silent_log:
                logger.info(
                    f"  ➜ [自定义季集号识别] 命中规则 '{custom_rule_name}' -> "
                    f"S{int(season_num or 1):02d}E{int(episode_num or 0):02d} | {original_name}"
                )

            # 1. 自定义没补全，再走原有硬编码识别
            if season_num is None or episode_num is None:
                pattern = (
                    r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*?(?:e|E|p|P)(\d{1,4})\b'
                    r'|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b'
                    r'|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b'
                    r'|第(\d{1,4})[集话話回](?=$|[^\u4e00-\u9fff]|完|完结|完結)'
                    r'|(?:^|[ \.\-\_\[\(])(\d{1,4})[集话話回](?=$|[^\u4e00-\u9fff]|完|完结|完結)'
                )

                match = re.search(pattern, original_name, re.IGNORECASE)
                if match:
                    s = match.group(1)
                    e = match.group(2)
                    ep_only = match.group(3)
                    e_only = match.group(4)
                    zh_ep = match.group(5)

                    if season_num is None:
                        season_num = int(s) if s else None

                    if episode_num is None:
                        episode_num = int(e) if e else (
                            int(ep_only) if ep_only else (
                                int(e_only) if e_only else int(zh_ep)
                            )
                        )

            # 2. 从相对路径提取季号
            if season_num is None and rel_path:
                m_rel = re.search(r'(?:Season\s*|S|第)(\d{1,4})(?:季)?(?:/|$)', rel_path, re.IGNORECASE)
                if m_rel:
                    season_num = int(m_rel.group(1))

            # 3. ★ 纯数字 / 动漫数字兜底提取集号
            if episode_num is None:
                name_without_ext = original_name.rsplit('.', 1)[0]
                if name_without_ext.isdigit():
                    episode_num = int(name_without_ext)
                else:
                    clean_name = re.sub(
                        r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0',
                        '',
                        name_without_ext
                    )

                    anime_match = re.search(r'(?:\s-\s+)(\d{1,4})(?:\s|$)|\[(\d{1,4})\]|【(\d{1,4})】', clean_name)
                    if anime_match:
                        ep_str = anime_match.group(1) or anime_match.group(2) or anime_match.group(3)
                        episode_num = int(ep_str)
                    else:
                        end_match = re.search(r'(?:^|[ \.\-\_\[\(])(\d{1,4})(?:[\]\)]|\s*)$', clean_name)
                        if end_match:
                            episode_num = int(end_match.group(1))
                        else:
                            mid_match = re.search(r'(?:^|[ \-\_\[\(])(\d{1,4})(?:[ \.\-\_\]\)]|$)', clean_name)
                            if mid_match:
                                episode_num = int(mid_match.group(1))

            # 4. 终极兜底
            if season_num is None:
                season_num = 1

        # ★★★ 动漫绝对集数转季号逻辑 (解决海贼王 S01E1158 的问题) ★★★
        if is_tv and episode_num is not None and episode_num > 30:
            seasons_data = self.details.get('seasons', [])
            last_ep_data = self.details.get('last_episode_to_air', {})
            
            # ★ 核心修复：容量校验。检查当前解析出的 season_num 是否真的能容纳这个 episode_num
            # 如果不能容纳 (比如 S01 只有 61 集，但文件是 E1158)，说明这是绝对集数，强制反推！
            needs_recalc = False
            if seasons_data:
                current_season_data = next((s for s in seasons_data if s.get('season_number') == season_num), None)
                if not current_season_data or current_season_data.get('episode_count', 0) < episode_num:
                    needs_recalc = True
            elif season_num == 1:
                needs_recalc = True

            if needs_recalc:
                # 捷径：如果是最新集，直接取最新季
                if last_ep_data and last_ep_data.get('episode_number') == episode_num:
                    season_num = last_ep_data.get('season_number', 1)
                    if not silent_log:
                        logger.info(f"  ➜ [动漫分季修正] 命中最新集，自动修正为第 {season_num} 季")
                elif seasons_data:
                    # 累加算法：排除第 0 季(SP)，按顺序累加集数，推算所属季
                    valid_seasons = sorted([s for s in seasons_data if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])
                    cumulative = 0
                    for s in valid_seasons:
                        cumulative += s.get('episode_count', 0)
                        if episode_num <= cumulative:
                            season_num = s['season_number']
                            if not silent_log:
                                logger.info(f"  ➜ [动漫分季修正] 绝对集数 {episode_num} 超出原季容量，已自动推算并修正为第 {season_num} 季！")
                            break

        if hasattr(self, 'forced_season') and self.forced_season is not None:
            # ★ 核心修复：防止批量整理时，第一个文件的季号污染后续所有不同季号的文件
            if getattr(self, 'is_manual_correct', False):
                season_num = int(self.forced_season)
            else:
                # 仅当文件名和相对路径中都没有明确的季号特征时，才使用外层推导的 forced_season 作为兜底
                has_explicit_season = False
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)|Season\s*\d{1,4}|第\d{1,4}季', original_name, re.IGNORECASE):
                    has_explicit_season = True
                elif file_node.get('rel_path') and re.search(r'(?:Season\s*|S|第)\d{1,4}(?:季)?(?:/|$)', file_node.get('rel_path'), re.IGNORECASE):
                    has_explicit_season = True
                    
                if not has_explicit_season:
                    season_num = int(self.forced_season)

        # ★★★ 核心升级：直接调用统一乐高引擎生成文件名 ★★★
        default_format = ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio', 'sep_middot_space', 'group']
        file_format = cfg.get('file_format', default_format)

        core_name = self._build_name_from_format(
            file_format, 
            is_tv=is_tv, 
            season_num=season_num, 
            episode_num=episode_num, 
            original_title=original_title, 
            video_info=video_info,
            safe_title=new_base_name # 传入过滤过特殊字符的标题
        )

        # 兜底：如果轨道配空了，用原名
        if not core_name: core_name = name_body

        # ★★★ 提取 Part/CD 上下集信息，符合 Emby 规范 ★★★
        part_num = None
        part_suffix = ""
        part_match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', original_name)
        if part_match:
            part_num = int(part_match.group(2))
            part_suffix = f" - pt{part_num}"

        new_name = f"{core_name}{part_suffix}{lang_suffix}.{ext}"
        
        # ★★★ 核心修复：在这里利用齐全的 video_info 生成季目录名称 ★★★
        s_name = None
        if is_tv and season_num is not None:
            season_format = cfg.get('season_dir_format', ['season_name_en'])
            s_name = self._build_name_from_format(
                season_format, 
                is_tv=True, 
                season_num=season_num, 
                original_title=original_title, 
                video_info=video_info, # ★ 关键：把视频信息传进去！
                safe_title=new_base_name
            )
            if not s_name: s_name = f"Season {season_num:02d}"

        return new_name, season_num, episode_num, s_name, is_center_cached, video_info, bool(real_info), part_num

    def _scan_files_recursively(self, cid, depth=0, max_depth=3, current_rel_path=""):
        all_files = []
        if depth > max_depth: return []
        try:
            res = self.client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            if res.get('data'):
                for item in res['data']:
                    # 兼容 OpenAPI 键名
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if str(fc_val) == '1':
                        item['rel_path'] = current_rel_path
                        all_files.append(item)
                    elif str(fc_val) == '0':
                        sub_id = item.get('fid') or item.get('file_id')
                        sub_name = item.get('fn') or item.get('n') or item.get('file_name', '')
                        new_rel = f"{current_rel_path}/{sub_name}" if current_rel_path else sub_name
                        sub_files = self._scan_files_recursively(sub_id, depth + 1, max_depth, new_rel)
                        all_files.extend(sub_files)
        except Exception as e:
            logger.warning(f"  ➜ 扫描目录出错 (CID: {cid}): {e}")
        return all_files

    def _is_junk_file(self, filename):
        """
        检查是否为垃圾文件/样本/花絮 (基于 MP 规则)
        """
        # 垃圾文件正则列表 (合并了通用规则和你提供的 MP 规则)
        junk_patterns = [
            # 基础关键词
            r'(?i)\b(sample|trailer|featurette|bonus)\b',

            # MP 规则集
            r'(?i)Special Ending Movie',
            r'(?i)\[((TV|BD|\bBlu-ray\b)?\s*CM\s*\d{2,3})\]',
            r'(?i)\[Teaser.*?\]',
            r'(?i)\[PV.*?\]',
            r'(?i)\[NC[OPED]+.*?\]',
            r'(?i)\[S\d+\s+Recap(\s+\d+)?\]',
            r'(?i)Menu',
            r'(?i)Preview',
            r'(?i)\b(CDs|SPs|Scans|Bonus|映像特典|映像|specials|特典CD|Menu|Logo|Preview|/mv)\b',
            r'(?i)\b(NC)?(Disc|片头|OP|SP|ED|Advice|Trailer|BDMenu|片尾|PV|CM|Preview|MENU|Info|EDPV|SongSpot|BDSpot)(\d{0,2}|_ALL)\b',
            r'(?i)WiKi\.sample'
        ]

        for pattern in junk_patterns:
            if re.search(pattern, filename):
                return True
        return False
    
    def _execute_collection_breakdown(self, root_item, collection_movies, skip_gc=False):
        """内部方法：拆解并独立整理合集包内的文件 (已升级批量模式)"""
        source_root_id = root_item.get('fid') or root_item.get('file_id')
        root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
        unidentified_cid = None 
        
        # 获取或创建未识别目录 CID
        config = get_config()
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        
        if not unidentified_cid or str(unidentified_cid) == '0':
            save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            unidentified_folder_name = "未识别"
            if save_cid and str(save_cid) != '0':
                try:
                    search_res = self.client.fs_files({'cid': save_cid, 'search_value': unidentified_folder_name, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            if item.get('fn') == unidentified_folder_name and str(item.get('fc')) == '0':
                                unidentified_cid = item.get('fid')
                                break
                except: pass
                
                if not unidentified_cid:
                    try:
                        mk_res = self.client.fs_mkdir(unidentified_folder_name, save_cid)
                        if mk_res.get('state'): unidentified_cid = mk_res.get('cid')
                    except: pass

        processed_count = 0
        try:
            sub_res = self.client.fs_files({'cid': source_root_id, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            sub_items = sub_res.get('data', [])
            
            # ★ 新增：分组字典
            grouped_sub_items = {}
            unidentified_sub_fids = []
            unidentified_video_names = []
            
            for sub_item in sub_items:
                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name')
                sub_id = sub_item.get('fid') or sub_item.get('file_id')
                
                # 1. 优先看子项自己有没有带 ID
                tmdb_id, sub_type, sub_title = _identify_media_enhanced(
                    sub_name, 
                    ai_translator=self.ai_translator, 
                    use_ai=self.use_ai,
                    is_folder=(str(sub_item.get('fc')) == '0') 
                )
                
                # 2. 模糊匹配 (仅当有官方合集列表时)
                if not tmdb_id and collection_movies:
                    matched_movie = None
                    clean_sub_name = re.sub(r'[^\w\u4e00-\u9fa5]', '', sub_name).lower()
                    
                    for movie in collection_movies:
                        m_title = movie.get('title', '')
                        m_orig = movie.get('original_title', '')
                        m_year = movie.get('release_date', '')[:4] if movie.get('release_date') else ''
                        
                        clean_m_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', m_title).lower()
                        clean_m_orig = re.sub(r'[^\w\u4e00-\u9fa5]', '', m_orig).lower()
                        
                        if (clean_m_title and clean_m_title in clean_sub_name) or \
                           (clean_m_orig and clean_m_orig in clean_sub_name):
                            if m_year and m_year in sub_name:
                                matched_movie = movie
                                break
                            elif not matched_movie:
                                matched_movie = movie
                    
                    if matched_movie:
                        tmdb_id = str(matched_movie['id'])
                        sub_type = 'movie'
                        sub_title = matched_movie.get('title')
                        logger.info(f"    ├─ 官方合集匹配成功: {sub_name} -> {sub_title} (ID:{tmdb_id})")

                # 3. 终极兜底：无官方合集时的文件名暴力解析搜索
                if not tmdb_id and not collection_movies:
                    clean_name = re.sub(r'^\[.*?\]|^.*?\.com-|^.*?\.[a-z]{2,3}-', '', sub_name, flags=re.IGNORECASE)
                    match_year = re.search(r'^(.*?)(?:\.|_|-|\s|\()+(19\d{2}|20\d{2})\b', clean_name)
                    if match_year:
                        guess_title = match_year.group(1).replace('.', ' ').strip()
                        guess_year = match_year.group(2)
                        logger.info(f"    ├─ 尝试搜索: '{guess_title}' ({guess_year})")
                        try:
                            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                            results = tmdb.search_media(query=guess_title, api_key=api_key, item_type='movie', year=guess_year)
                            if results and len(results) > 0:
                                tmdb_id = str(results[0]['id'])
                                sub_type = 'movie'
                                sub_title = results[0].get('title') or results[0].get('name')
                                logger.info(f"    ├─ 搜索成功: {sub_title} (ID:{tmdb_id})")
                        except Exception as e:
                            logger.debug(f"    ├─ 搜索出错: {e}")
                
                # ★ 核心修改：不再立即执行，而是加入分组字典
                if tmdb_id:
                    key = (tmdb_id, sub_type, sub_title)
                    if key not in grouped_sub_items:
                        grouped_sub_items[key] = []
                    grouped_sub_items[key].append(sub_item)
                else:
                    unidentified_sub_fids.append(sub_id)
                    # ★ 检查是否为真正的视频文件
                    sub_ext = sub_name.split('.')[-1].lower() if '.' in sub_name else ''
                    if sub_ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']:
                        unidentified_video_names.append(sub_name)
            
            # ★ 核心修改：遍历分组，批量执行
            for (tmdb_id, sub_type, sub_title), items in grouped_sub_items.items():
                logger.info(f"    ├─ 准备批量整理合集子项: {sub_title} -> ID:{tmdb_id} (共 {len(items)} 个文件)")
                try:
                    organizer = SmartOrganizer(self.client, tmdb_id, sub_type, sub_title, self.ai_translator, self.use_ai)
                    target_cid_for_sub = organizer.get_target_cid()
                    if organizer.execute(items, target_cid_for_sub):
                        processed_count += len(items)
                except Exception as e:
                    logger.error(f"    ➜ 批量处理子项失败: {e}")
            
            # ★ 核心修改：批量移入未识别
            if unidentified_sub_fids and unidentified_cid:
                logger.warning(f"    ➜ 无法识别合集子项 {len(unidentified_sub_fids)} 个，批量移入未识别。")
                try: 
                    self.client.fs_move(unidentified_sub_fids, unidentified_cid)
                    # ★★★ 核心修复：只有当存在真正的视频文件时，才发送通知 ★★★
                    if unidentified_video_names:
                        from handler.telegram import send_unrecognized_notification
                        send_unrecognized_notification(f"合集包 [{root_name}] 内的 {len(unidentified_video_names)} 个视频文件", reason="合集拆解时无法匹配到 TMDb 数据")
                except Exception as e: 
                    logger.error(f"    ➜ 移入未识别失败: {e}")
            
            if not skip_gc:
                from handler.p115_service import P115DeleteBuffer
                P115DeleteBuffer.add(check_save_path=True)
                logger.info(f"  ➜ [清理空目录] 已将拆解完毕的合集包交由垃圾回收器检查: {root_name}")
            
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"  ➜ 拆解合集包失败: {e}")
            return False

    def execute(self, root_item_or_items, target_cid, progress_callback=None, skip_gc=False):
        # 判断传入的是单个文件还是批量文件列表
        is_batch = isinstance(root_item_or_items, list)
        
        if is_batch:
            if not root_item_or_items: return True 
            root_item = root_item_or_items[0]      
            root_name = "批量文件"
            parse_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '') 
            source_root_id = root_item.get('pid') or root_item.get('parent_id')
            is_source_file = True
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else source_root_id
        else:
            root_item = root_item_or_items
            root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
            parse_name = root_name 
            source_root_id = root_item.get('fid') or root_item.get('file_id')
            fc_val = root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type')
            is_source_file = str(fc_val) == '1'
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))

        # =================================================================
        # 1. 拦截合集包 (Collection Breakdown) - 仅限单项传入时触发
        # =================================================================
        if not is_batch and not is_source_file and re.search(r'(合集|部曲|系列|Collection|Pack|Trilogy|Quadrilogy|\d+-\d+)', root_name, re.IGNORECASE):
            logger.info(f"  ➜ 检测到疑似合集包: {root_name}，正在验证...")
            collection_movies = []
            try:
                res_c = tmdb.get_collection_details(int(self.tmdb_id), self.api_key)
                if res_c and 'parts' in res_c: collection_movies = res_c['parts']
            except: pass
            
            if not collection_movies and self.media_type == 'movie':
                try:
                    c_id = None
                    if hasattr(self, 'raw_metadata') and self.raw_metadata and self.raw_metadata.get('belongs_to_collection'):
                        c_id = self.raw_metadata['belongs_to_collection']['id']
                    else:
                        res_m = tmdb.get_movie_details(int(self.tmdb_id), self.api_key)
                        if res_m and res_m.get('belongs_to_collection'):
                            c_id = res_m['belongs_to_collection']['id']
                    if c_id:
                        res_c = tmdb.get_collection_details(int(c_id), self.api_key)
                        if res_c and 'parts' in res_c: collection_movies = res_c['parts']
                except: pass

            if collection_movies:
                logger.info(f"  ➜ 确认为官方合集包，包含 {len(collection_movies)} 部电影，启动精确拆解模式...")
            else:
                logger.info(f"  ➜ 未找到官方合集信息 (可能是民间自制包)，启动基于文件名的暴力拆解模式...")
            return self._execute_collection_breakdown(root_item, collection_movies, skip_gc=skip_gc)

        # =================================================================
        # 2. 提前获取候选文件列表 (支持批量合并)
        # =================================================================
        candidates = []
        if is_batch:
            for item in root_item_or_items:
                fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                if str(fc_val) == '1':
                    candidates.append(item)
                else:
                    candidates.extend(self._scan_files_recursively(item.get('fid') or item.get('file_id'), max_depth=3))
        else:
            if is_source_file:
                candidates.append(root_item)
            else:
                candidates = self._scan_files_recursively(source_root_id, max_depth=3)

        if not candidates: return True

        # =================================================================
        # ★★★ 3. 核心重构：提前提取物理视频流信息 (替代原有的冗余嗅探逻辑) ★★★
        # =================================================================
        # 无论是否配置了时长规则，我们都提前抓取第一个视频的真实媒体信息。
        # 这样不仅代码更简洁，而且能确保后续所有的分类规则判定都基于最准确的数据。
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        first_video = next((c for c in candidates if (c.get('fn') or c.get('n') or c.get('file_name') or '').split('.')[-1].lower() in known_video_exts), None)

        # 准备预取字典，供后续重命名主循环复用，避免重复查库/ffprobe
        pre_fetched_mediainfo = {}
        local_pre_fetched_mediainfo = {}

        if first_video and not getattr(self, 'is_manual_correct', False) and not getattr(self, 'is_from_memory', False):
            v_sha1 = first_video.get('sha1') or first_video.get('sha')
            v_fid = first_video.get('fid') or first_video.get('file_id')
            
            if not v_sha1 and v_fid:
                try:
                    info_res = self.client.fs_get_info(v_fid)
                    if info_res.get('state') and info_res.get('data'):
                        v_sha1 = info_res['data'].get('sha1')
                        first_video['sha1'] = v_sha1
                except: pass

            if v_sha1 or v_fid:
                # ★★★ 核心修复：提前解析前，先主动查一次本地缓存，防止 ffprobe 击穿！
                if v_sha1:
                    cached_text = P115CacheManager.get_mediainfo_cache_text(v_sha1)
                    if cached_text:
                        try:
                            local_pre_fetched_mediainfo[v_sha1] = json.loads(cached_text)
                            logger.debug(f"  ➜ [提前解析] 命中本地媒体信息缓存: {v_sha1[:8]}")
                        except: pass

                # 提前解析媒体信息 (内部会自动走本地缓存 -> 中心 -> ffprobe)
                self._fetch_and_parse_mediainfo(
                    v_sha1,
                    guessed_info={},
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                    file_node=first_video,
                    silent_log=True
                )
                
                # 尝试补齐 TMDb 缺失的时长
                if v_sha1:
                    cached_text = P115CacheManager.get_mediainfo_cache_text(v_sha1)
                    if cached_text:
                        try:
                            mi_json = json.loads(cached_text)
                            ticks = 0
                            if isinstance(mi_json, list) and len(mi_json) > 0:
                                ticks = mi_json[0].get("MediaSourceInfo", {}).get("RunTimeTicks", 0)
                            elif isinstance(mi_json, dict):
                                ticks = mi_json.get("MediaSourceInfo", {}).get("RunTimeTicks", 0)

                            if ticks > 0:
                                physical_runtime = int(ticks / 10000000 / 60)
                                if self.media_type == 'movie':
                                    if not self.details.get('runtime'):
                                        self.details['runtime'] = physical_runtime
                                        logger.info(f"  ➜ [提前解析] 成功补齐电影物理时长: {physical_runtime} 分钟")
                                else:
                                    runtimes = self.details.get('episode_run_time', [])
                                    if not runtimes or runtimes[0] == 0:
                                        self.details['episode_run_time'] = [physical_runtime]
                                        logger.info(f"  ➜ [提前解析] 成功补齐剧集物理时长: {physical_runtime} 分钟")
                        except Exception:
                            pass

        # =================================================================
        # ★★★ 4. 智能类型纠错嗅探 (Movie -> TV) ★★★
        # =================================================================
        if self.media_type == 'movie' and not getattr(self, 'is_manual_correct', False):
            is_actually_tv = False
            for c in candidates:
                c_name = c.get('fn') or c.get('n') or c.get('file_name', '')
                rel_path = c.get('rel_path', '')
                
                if re.search(r'(?:Season\s?\d+|S\d+|第[一二三四五六七八九十\d]+季)', rel_path, re.IGNORECASE):
                    is_actually_tv = True
                    break
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第\d{1,4}[集话話回]', c_name, re.IGNORECASE):
                    is_actually_tv = True
                    break
                
                clean_c_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', c_name)
                if re.search(r'(?:\s-\s+)(\d{2,4})(?:\s|$)|\[(\d{2,4})\]|【(\d{2,4})】', clean_c_name): 
                    is_actually_tv = True
                    break
            
            if is_actually_tv:
                logger.warning(f"  🕵️‍♂️ [智能纠错] 发现文件包含明显的剧集特征(如季目录/EP01)，但当前被错误识别为电影。正在尝试自动纠错...")
                try:
                    self.media_type = 'tv'
                    cache_key = f"tv_{self.tmdb_id}"
                    if cache_key in _TMDB_METADATA_CACHE:
                        del _TMDB_METADATA_CACHE[cache_key]
                        
                    self.raw_metadata = self._fetch_raw_metadata()
                    
                    if self.raw_metadata and self.raw_metadata.get('title'):
                        self.details = self.raw_metadata
                        logger.info(f"  ➜ [智能纠错] 成功保留原 ID ({self.tmdb_id}) 并切换为剧集: {self.details.get('title')}")
                    else:
                        logger.warning(f"  ➜ [智能纠错] 原 ID ({self.tmdb_id}) 作为剧集查询失败，尝试用名称重新搜索...")
                        search_title = self.original_title
                        clean_title = re.sub(r'\(\d{4}\)', '', search_title).strip()
                        results = tmdb.search_media(query=clean_title, api_key=self.api_key, item_type='tv')
                        
                        if results and len(results) > 0:
                            new_tmdb_id = str(results[0]['id'])
                            logger.info(f"  ➜ [智能纠错] 成功重新搜索并纠正为剧集: {results[0].get('name')} (ID:{new_tmdb_id})")
                            self.tmdb_id = new_tmdb_id
                            self.raw_metadata = self._fetch_raw_metadata()
                            self.details = self.raw_metadata
                        else:
                            logger.warning(f"  ➜ [智能纠错] 未能在 TMDb 找到对应的剧集，将强制按剧集格式重命名以防冲突。")
                except Exception as e:
                    logger.error(f"  ➜ [智能纠错] 纠错失败: {e}")

        # =================================================================
        # ★★★ 5. 提取季号并统一计算最终 Target CID ★★★
        # =================================================================
        if self.media_type == 'tv' and getattr(self, 'forced_season', None) is None:
            m1 = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})(?:[ \.\-]*(?:e|E|p|P)\d{1,4}\b)?', parse_name, re.IGNORECASE)
            m2 = re.search(r'Season\s*(\d{1,4})\b', parse_name, re.IGNORECASE)
            m3 = re.search(r'第(\d{1,4})季', parse_name)
            extracted_season = None
            
            if m1: extracted_season = int(m1.group(1))
            elif m2: extracted_season = int(m2.group(1))
            elif m3: extracted_season = int(m3.group(1))
            else:
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b|第(\d{1,4})[集话話回]', parse_name, re.IGNORECASE):
                    extracted_season = 1
            
            if extracted_season is not None:
                self.forced_season = extracted_season

        # ★ 统一在这里获取最终的 target_cid！(因为 details 已经补齐了时长，media_type 也可能被纠错了，season 也提取了)
        if not getattr(self, 'is_manual_correct', False):
            new_target_cid = self.get_target_cid(season_num=getattr(self, 'forced_season', None))
            if new_target_cid and str(new_target_cid) != str(target_cid):
                logger.info(f"  ➜ [智能分类] 目标目录已根据最新元数据(时长/类型/连载状态)修正！")
                target_cid = new_target_cid
                dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else source_root_id

        # =================================================================
        # 6. 计算最终的目录名称和路径 (支持 / 多级目录)
        # =================================================================
        title = self.details.get('title') or self.original_title
        original_title = self.details.get('original_title') or title
        date_str = self.details.get('date') or ''
        year = date_str[:4] if date_str else ''

        cfg = self.rename_config
        keep_original = cfg.get('keep_original_name', False)
        
        # ★ 必须保留 safe_title 的计算，供后续文件重命名使用
        base_title = original_title if cfg.get('main_title_lang', 'zh') == 'original' else title
        safe_title = re.sub(r'[\\/:*?"<>|]', '', base_title).strip()

        if keep_original:
            std_root_name = root_name
            safe_title = root_name # 如果保留原名，safe_title 也退化为原名
        else:
            # ★ 使用新的乐高引擎生成主目录名 (可能包含 /)
            main_format = cfg.get('main_dir_format', ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'])
            std_root_name = self._build_name_from_format(main_format, is_tv=(self.media_type=='tv'), original_title=original_title)
            # 兜底防空
            if not std_root_name: std_root_name = safe_title

        config = get_config()
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        min_size_mb = int(config.get(constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE, 10))
        MIN_VIDEO_SIZE = min_size_mb * 1024 * 1024

        # 获取“未识别”目录的 CID
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        if not unidentified_cid or str(unidentified_cid) == '0':
            save_cid = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            if save_cid and str(save_cid) != '0':
                try:
                    search_res = self.client.fs_files({'cid': save_cid, 'search_value': '未识别', 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            if item.get('fn') == '未识别' and str(item.get('fc')) == '0':
                                unidentified_cid = item.get('fid')
                                break
                except: pass

        logger.info(f"  ➜ [115] 开始整理: {root_name} -> {std_root_name}")

        final_home_cid = None
        current_parent_cid = dest_parent_cid
        
        # ★★★ 核心升级：支持 / 分层创建多级目录 ★★★
        dir_parts = [p.strip() for p in std_root_name.split('/') if p.strip()]
        
        # 提前计算基础相对路径，用于逐级修复 local_path
        category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
        base_rel_path = category_rule.get('category_path') or category_rule.get('dir_name', '未识别') if category_rule else "未识别"
        
        for attempt in range(2):
            success_chain = True
            temp_parent_cid = current_parent_cid
            
            # 逐级检查/创建目录
            for part_name in dir_parts:
                cache_key = f"{temp_parent_cid}_{part_name}"
                
                # 1. 优先查全局内存缓存 (抵抗并发)
                with _GLOBAL_DIR_LOCK:
                    part_cid = _GLOBAL_DIR_CACHE.get(cache_key)
                
                # 2. 查数据库缓存
                if not part_cid:
                    part_cid = P115CacheManager.get_cid(temp_parent_cid, part_name)
                    if part_cid:
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = part_cid

                # 缓存自愈检查
                if part_cid and str(part_cid) == str(source_root_id) and str(temp_parent_cid) != str(root_item.get('pid') or root_item.get('parent_id')):
                    P115CacheManager.delete_cid(part_cid)
                    with _GLOBAL_DIR_LOCK:
                        _GLOBAL_DIR_CACHE.pop(cache_key, None)
                    part_cid = None

                if not part_cid:
                    mk_res = self.client.fs_mkdir(part_name, temp_parent_cid)
                    if mk_res.get('state'):
                        part_cid = mk_res.get('cid')
                        P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                        with _GLOBAL_DIR_LOCK:
                            _GLOBAL_DIR_CACHE[cache_key] = part_cid
                    else:
                        err_text = json.dumps(mk_res, ensure_ascii=False)
                        should_search_after_mkdir_fail = any(
                            kw in err_text.lower()
                            for kw in ['exist', 'exists', 'already', '重复', '已存在', 'same_name', '文件名重复']
                        )

                        if should_search_after_mkdir_fail:
                            try:
                                # ★ 核心修复：使用 fs_files + search_value 精准定位！
                                # 既突破了 1000 条限制，又不会触发全局 search 的 WAF 风控
                                search_res = self.client.fs_files({
                                    'cid': temp_parent_cid,
                                    'search_value': part_name,
                                    'limit': 100,
                                    'show_dir': 1,
                                    'record_open_time': 0
                                })
                                for item in search_res.get('data', []):
                                    item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                    item_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                                    item_cid = item.get('fid') or item.get('file_id')
                                    
                                    if item_fc == '0' and item_name == part_name and item_cid:
                                        part_cid = item_cid
                                        P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                                        with _GLOBAL_DIR_LOCK:
                                            _GLOBAL_DIR_CACHE[cache_key] = part_cid
                                        break
                            except Exception as e:
                                logger.debug(f"  ➜ 目录精准定位失败: {e}")
                
                if part_cid:
                    temp_parent_cid = part_cid
                    # ★ 核心修复：逐级累加路径并更新 DB，彻底解决年份目录 local_path 为 NULL 的问题！
                    base_rel_path = f"{base_rel_path}/{part_name}"
                    P115CacheManager.update_local_path(part_cid, base_rel_path)
                else:
                    success_chain = False
                    break
            
            if success_chain:
                final_home_cid = temp_parent_cid
                break # 成功获取最终层级，跳出重试循环
                
            # 失败回退逻辑
            if attempt == 0:
                fallback_cid = self.get_target_cid(ignore_memory=True)
                if fallback_cid and str(fallback_cid) != str(current_parent_cid):
                    P115CacheManager.delete_cid(current_parent_cid)
                    current_parent_cid = fallback_cid
                    target_cid = fallback_cid 
                else:
                    break

        if not final_home_cid:
            logger.error(f"  ➜ 无法获取或创建目标目录链 (已尝试所有手段)")
            return False
        
        if not candidates: return True

        moved_count = 0
        move_groups = {}
        unrecognized_fids = [] # ★ 终极垃圾桶：收集所有不符合要求的文件
        unqualified_items = [] # ★ 质检不合格垃圾桶
        
        # ★ 新增：用于记录本批次已经生成的目标文件名，防止同名冲突
        seen_new_filenames = set()

        # 批量预查询中心服务器与本地数据库
        pre_fetched_mediainfo = {}
        local_pre_fetched_mediainfo = {} # ★ 新增：本地预获取字典
        
        # ★ 核心修复：移除开关限制，强制批量预取真实媒体信息
        video_sha1s = []
        for file_item in candidates:
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            if ext in known_video_exts:
                sha1 = file_item.get('sha1') or file_item.get('sha')
                
                # =========================================================
                # ★ 核心修复：在收集阶段，如果发现缺失 SHA1，提前主动请求补齐！
                # =========================================================
                if not sha1:
                    fid = file_item.get('fid') or file_item.get('file_id')
                    if fid:
                        try:
                            info_res = self.client.fs_get_info(fid)
                            if info_res.get('state') and info_res.get('data'):
                                sha1 = info_res['data'].get('sha1')
                                if sha1:
                                    file_item['sha1'] = sha1 # 存回字典，供后续主循环直接使用
                        except Exception:
                            pass
                            
                if sha1: 
                    video_sha1s.append(sha1)
        
        if video_sha1s:
            # 先查本地缓存，剔除已有的，只查缺失的
            local_cached_sha1s = set()
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # ★ 核心优化：直接把 json 也查出来放进内存！
                        cursor.execute("SELECT sha1, mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = ANY(%s)", (list(video_sha1s),))
                        rows = cursor.fetchall()
                        if rows:
                            hit_sha1s = []
                            for row in rows:
                                local_cached_sha1s.add(row['sha1'])
                                hit_sha1s.append(row['sha1'])
                                if row['mediainfo_json']:
                                    val = row['mediainfo_json']
                                    local_pre_fetched_mediainfo[row['sha1']] = val if isinstance(val, (list, dict)) else json.loads(val)
                            
                            # 批量更新命中计数
                            if hit_sha1s:
                                cursor.execute("UPDATE p115_mediainfo_cache SET hit_count = hit_count + 1 WHERE sha1 = ANY(%s)", (hit_sha1s,))
                                conn.commit()
            except Exception: pass
            
            missing_sha1s = list(set(video_sha1s) - local_cached_sha1s)
            if missing_sha1s:
                req_count = len(missing_sha1s)
                logger.info(f"  ➜ [批量查询] 准备向中心服务器查询 {req_count} 个文件的媒体信息...")
                try:
                    import extensions
                    processor = extensions.media_processor_instance
                    if processor and getattr(processor, 'p115_center', None):
                        resp = processor.p115_center.download_emby_mediainfo_data(missing_sha1s)
                        
                        if isinstance(resp, dict):
                            # ★ 核心优化：过滤掉可能返回的空值/None，只统计真正有数据的命中项
                            valid_hits = {k: v for k, v in resp.items() if v}
                            hit_count = len(valid_hits)
                            
                            if hit_count > 0:
                                pre_fetched_mediainfo = valid_hits
                                # ★ 核心修复：批量查询中心服务器后，立即写入本地缓存！
                                for k_sha1, v_json in valid_hits.items():
                                    P115CacheManager.save_mediainfo_cache(k_sha1, v_json)
                                    local_pre_fetched_mediainfo[k_sha1] = v_json
                                
                                if hit_count == req_count:
                                    logger.info(f"  ➜ [批量查询] 完美命中！成功获取全部 {hit_count} 个文件的媒体信息。")
                                else:
                                    logger.info(f"  ➜ [批量查询] 部分命中：成功获取 {hit_count}/{req_count} 个文件的媒体信息。")
                            else:
                                logger.info(f"  ➜ [批量查询] 中心服务器暂无这 {req_count} 个文件的媒体信息。")
                        else:
                            logger.warning(f"  ➜ [批量查询] 中心服务器返回数据格式异常。")
                            
                except Exception as e:
                    logger.warning(f"  ➜ [批量查询] 中心服务器查询失败: {e}")

        # 确保 allowed_exts 有兜底，防止用户清空列表导致报错
        if not allowed_exts:
            allowed_exts = known_video_exts | {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

        # =================================================================
        # ★★★ 同批次字幕完美对齐视频命名 (解决 MP 单文件上传分离问题) ★★★
        # =================================================================
        batch_video_names = {} # key: (season, episode, part) -> base_name
        if not keep_original and is_batch:
            # 1. 预扫描视频，生成标准命名
            for file_item in candidates:
                fn = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                ext = fn.split('.')[-1].lower() if '.' in fn else ''
                if ext in known_video_exts:
                    # 临时调用重命名获取名字
                    v_name, v_s, v_e, _, _, _, _, v_part = self._rename_file_node(
                        file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), 
                        original_title=original_title, pre_fetched_mediainfo=pre_fetched_mediainfo, 
                        local_pre_fetched_mediainfo=local_pre_fetched_mediainfo,
                        silent_log=True  # ★ 开启静默，防止预扫描时重复打印日志
                    )
                    key = (v_s, v_e, v_part) if self.media_type == 'tv' else ('movie', v_part)
                    # 电影只保留第一个视频作为基准 (通常电影只有一个正片)
                    if key not in batch_video_names:
                        batch_video_names[key] = v_name.rsplit('.', 1)[0]
            
            # 2. 将视频基础名注入到同批次的字幕中
            if batch_video_names:
                for file_item in candidates:
                    fn = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                    ext = fn.split('.')[-1].lower() if '.' in fn else ''
                    if ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']:
                        s_num = file_item.get('_forced_season')
                        e_num = file_item.get('_forced_episode')
                        
                        # 提取字幕的 Part 信息
                        sub_part_num = None
                        sub_part_match = re.search(r'(?i)[ \.\-\_\[\(]*(part|pt|cd)[ \.\-\_]*(\d{1,2})\b', fn)
                        if sub_part_match:
                            sub_part_num = int(sub_part_match.group(2))
                        
                        # ★ 电影无脑匹配逻辑
                        if self.media_type == 'movie':
                            m_key = ('movie', sub_part_num)
                            if m_key in batch_video_names:
                                file_item['_forced_base_name'] = batch_video_names[m_key]
                            elif ('movie', None) in batch_video_names:
                                file_item['_forced_base_name'] = batch_video_names[('movie', None)]
                            continue

                        # ★ 剧集匹配逻辑：使用强大的正则和纯数字兜底提取集号
                        if self.media_type == 'tv' and (s_num is None or e_num is None):
                            # 1. 标准特征匹配
                            match = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*?(\d{1,4})\b|(?:^|[ \.\-\_\[\(])e(\d{1,4})\b|第(\d{1,4})[集话話回]', fn, re.IGNORECASE)
                            if match:
                                s = match.group(1)
                                e = match.group(2)
                                ep_only = match.group(3)
                                e_only = match.group(4)
                                zh_ep = match.group(5)
                                if s_num is None: s_num = int(s) if s else None
                                if e_num is None: e_num = int(e) if e else (int(ep_only) if ep_only else (int(e_only) if e_only else int(zh_ep)))
                            
                            # 2. 纯数字兜底 (针对 01.srt, 02.ass 这种)
                            if e_num is None:
                                name_without_ext = fn.rsplit('.', 1)[0]
                                if name_without_ext.isdigit():
                                    e_num = int(name_without_ext)
                                else:
                                    clean_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', name_without_ext)
                                    anime_match = re.search(r'(?:\s-\s+)(\d{1,4})(?:\s|$)|\[(\d{1,4})\]|【(\d{1,4})】', clean_name)
                                    if anime_match:
                                        ep_str = anime_match.group(1) or anime_match.group(2) or anime_match.group(3)
                                        e_num = int(ep_str)
                                    else:
                                        end_match = re.search(r'(?:^|[ \.\-\_\[\(])(\d{1,4})(?:[\]\)]|\s*)$', clean_name)
                                        if end_match:
                                            e_num = int(end_match.group(1))
                                        else:
                                            mid_match = re.search(r'(?:^|[ \-\_\[\(])(\d{1,4})(?:[ \.\-\_\]\)]|$)', clean_name)
                                            if mid_match:
                                                e_num = int(mid_match.group(1))
                            
                            # 3. 季号兜底
                            if s_num is None:
                                s_num = getattr(self, 'forced_season', 1)
                        
                        key = (s_num, e_num, sub_part_num)
                        fallback_key = (s_num, e_num, None)
                        
                        if key in batch_video_names:
                            file_item['_forced_base_name'] = batch_video_names[key]
                            file_item['_forced_season'] = s_num
                            file_item['_forced_episode'] = e_num
                            logger.debug(f"  ➜ [字幕对齐] 剧集精准绑定: 字幕 '{fn}' -> 视频 '{batch_video_names[key]}'")
                        elif fallback_key in batch_video_names:
                            file_item['_forced_base_name'] = batch_video_names[fallback_key]
                            file_item['_forced_season'] = s_num
                            file_item['_forced_episode'] = e_num
                            logger.debug(f"  ➜ [字幕对齐] 剧集降级绑定: 字幕 '{fn}' -> 视频 '{batch_video_names[fallback_key]}'")
                        else:
                            logger.warning(f"  ➜ [字幕对齐] 警告：字幕 '{fn}' 提取到 S{s_num}E{e_num}，但未找到对应的视频文件！")

        # =================================================================
        # ★★★ 核心性能修复：内存级目录缓存 ★★★
        # 解决超大季/超多集整理时，频繁查询本地DB和请求115 API导致的严重卡死问题
        # =================================================================
        memory_dir_cache = {}
        
        # 提前拉取目标主目录下的现有文件夹，填充到内存缓存中 (★ 优化：直接查本地数据库，零 API 消耗)
        if final_home_cid:
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 直接拉取该目录下的所有缓存项，利用 parent_id 索引极速返回
                        cursor.execute("SELECT id, name FROM p115_filesystem_cache WHERE parent_id = %s", (str(final_home_cid),))
                        for row in cursor.fetchall():
                            d_name = row['name']
                            d_id = str(row['id'])
                            if d_name and d_id:
                                memory_dir_cache[f"{final_home_cid}_{d_name}"] = d_id
            except Exception as e:
                pass

        for file_item in candidates:
            # 兼容 OpenAPI 键名
            fid = file_item.get('fid') or file_item.get('file_id')
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            
            # 1. 扩展名绝对白名单校验 (最高优先级)
            if ext not in allowed_exts:
                logger.debug(f"  ➜ 扩展名 .{ext} 不在允许列表中，打入未识别: {file_name}")
                if fid: unrecognized_fids.append(fid)
                if progress_callback: progress_callback()
                continue

            # 2. 垃圾/花絮/样本校验 (仅针对视频)
            if ext in known_video_exts:
                if self._is_junk_file(file_name) or (0 < file_size < MIN_VIDEO_SIZE):
                    logger.debug(f"  ➜ 判定为花絮或体积过小，打入未识别: {file_name}")
                    if fid: unrecognized_fids.append(fid)
                    if progress_callback: progress_callback()
                    continue

            # 在重命名和查缓存前，如果缺失 SHA1，主动请求详情补齐 
            file_sha1 = file_item.get('sha1') or file_item.get('sha')
            if not file_sha1 and fid and ext in known_video_exts:
                try:
                    info_res = self.client.fs_get_info(fid)
                    if info_res.get('state') and info_res.get('data'):
                        fetched_sha1 = info_res['data'].get('sha1')
                        if fetched_sha1:
                            file_item['sha1'] = fetched_sha1 
                except Exception:
                    pass

            if keep_original:
                new_filename = file_name
                season_num = None
                s_name = None
                is_center_cached = False
                real_target_cid = final_home_cid
                has_real_info = False
                
                # 即使保留原名，也要提取真实参数供洗版使用
                video_info = self._extract_video_info(file_name)
                if ext in known_video_exts:
                    if file_sha1:
                        real_info, is_center_cached = self._fetch_and_parse_mediainfo(file_sha1, video_info, pre_fetched_mediainfo, local_pre_fetched_mediainfo, file_node=file_item, silent_log=True)
                        if real_info:
                            video_info.update(real_info)
                            has_real_info = True
                
                # 1:1 复刻原始目录架构
                rel_path = file_item.get('rel_path', '')
                if rel_path:
                    current_parent = final_home_cid
                    for part in rel_path.split('/'):
                        if not part: continue
                        
                        # ★ 优先查内存缓存
                        cache_key = f"{current_parent}_{part}"
                        part_cid = memory_dir_cache.get(cache_key)
                        
                        # ★ 失败记忆体拦截
                        if part_cid == 'FAILED':
                            break
                            
                        if not part_cid:
                            part_cid = P115CacheManager.get_cid(current_parent, part)
                            
                        if not part_cid:
                            mk_res = self.client.fs_mkdir(part, current_parent)
                            if mk_res.get('state'):
                                part_cid = mk_res.get('cid')
                            else:
                                try:
                                    s_search = self.client.fs_files({'cid': current_parent, 'search_value': part, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                                    for s_item in s_search.get('data', []):
                                        if s_item.get('fn') == part and str(s_item.get('fc', s_item.get('type'))) == '0':
                                            part_cid = s_item.get('fid') or s_item.get('file_id')
                                            break
                                except: pass
                        if part_cid:
                            P115CacheManager.save_cid(part_cid, current_parent, part)
                            memory_dir_cache[cache_key] = part_cid # ★ 写入内存缓存
                            current_parent = part_cid
                        else:
                            memory_dir_cache[cache_key] = 'FAILED' # ★ 写入失败记忆体
                            break
                    real_target_cid = current_parent
            else:
                new_filename, season_num, episode_num, s_name, is_center_cached, video_info, has_real_info, part_num = self._rename_file_node(
                    file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), original_title=original_title,
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo 
                )

                real_target_cid = final_home_cid
                
                # ★ 直接使用返回的 s_name 创建/查找季目录
                if self.media_type == 'tv' and season_num is not None and s_name:
                    cache_key = f"{final_home_cid}_{s_name}"
                    
                    with _GLOBAL_DIR_LOCK:
                        s_cid = _GLOBAL_DIR_CACHE.get(cache_key)
                    
                    if s_cid == 'FAILED':
                        real_target_cid = final_home_cid
                    else:
                        if not s_cid:
                            s_cid = P115CacheManager.get_cid(final_home_cid, s_name)
                        
                        if s_cid:
                            real_target_cid = s_cid
                            with _GLOBAL_DIR_LOCK:
                                _GLOBAL_DIR_CACHE[cache_key] = s_cid
                        else:
                            s_mk = self.client.fs_mkdir(s_name, final_home_cid)
                            s_cid = s_mk.get('cid') if s_mk.get('state') else None
                            
                            if not s_cid: 
                                try:
                                    # ★ 核心修复：使用 fs_files + search_value 精准定位
                                    s_search = self.client.fs_files({
                                        'cid': final_home_cid, 
                                        'search_value': s_name,
                                        'limit': 100, 
                                        'show_dir': 1,
                                        'record_open_time': 0
                                    })
                                    for item in s_search.get('data', []):
                                        item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                        item_fc = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                                        item_cid = item.get('fid') or item.get('file_id')
                                        
                                        if item_fc == '0' and item_name == s_name and item_cid:
                                            s_cid = item_cid
                                            break
                                except Exception: pass
                            
                            if s_cid:
                                P115CacheManager.save_cid(s_cid, final_home_cid, s_name)
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = s_cid
                                real_target_cid = s_cid
                                
                                # ★ 同步更新季目录的 local_path
                                season_rel_path = f"{base_rel_path}/{s_name}"
                                P115CacheManager.update_local_path(s_cid, season_rel_path)
                            else:
                                with _GLOBAL_DIR_LOCK:
                                    _GLOBAL_DIR_CACHE[cache_key] = 'FAILED'
                                real_target_cid = final_home_cid

            # =================================================================
            # ★★★ 核心修复：严格去重逻辑 (防多版本/洗版残留冲突) ★★★
            # =================================================================
            if new_filename in seen_new_filenames:
                logger.warning(f"  ➜ [去重丢弃] 发现重复版本: '{file_name}' -> 目标名 '{new_filename}' 已被占用，当作垃圾打入未识别！")
                if fid: unrecognized_fids.append(fid)
                continue # 直接跳过，绝不重命名，绝不移动，绝不生成 STRM！
            
            # 记录已占用的文件名
            seen_new_filenames.add(new_filename)

            # 暂存入分组字典
            file_item['_new_filename'] = new_filename
            file_item['_season_num'] = season_num
            file_item['_episode_num'] = episode_num
            file_item['_s_name'] = s_name
            file_item['_is_center_cached'] = is_center_cached
            file_item['_video_info'] = video_info
            
            if real_target_cid not in move_groups:
                move_groups[real_target_cid] = []
            move_groups[real_target_cid].append(file_item)

        # =================================================================
        # ★★★ 执行批量移动与后续 STRM 生成 ★★★
        # =================================================================
        conflict_mode = cfg.get('conflict_mode', 'replace') # 获取覆盖模式，默认洗版替换
        
        # ★★★ 洗版特权检测 (细化到单集) ★★★
        active_washing_eps = set()
        movie_active_washing = False
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if self.media_type == 'tv':
                        # 查出该剧所有带有特权的分集
                        cursor.execute("SELECT season_number, episode_number FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND active_washing = TRUE", (str(self.tmdb_id),))
                        for row in cursor.fetchall():
                            active_washing_eps.add((row['season_number'], row['episode_number']))
                    else:
                        cursor.execute("SELECT active_washing FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Movie'", (str(self.tmdb_id),))
                        row = cursor.fetchone()
                        if row and row.get('active_washing'):
                            movie_active_washing = True
        except Exception as e:
            pass

        if active_washing_eps or movie_active_washing:
            logger.info(f"  ➜ [洗版特权] 检测到当前媒体存在洗版特权标记，命中特权的文件将强制替换旧版！")
        
        for batch_target_cid, items in move_groups.items():
            # -----------------------------------------------------------
            # ★ 1. 移动前：拉取目标目录现有文件，进行冲突检测 (保持不变)
            # -----------------------------------------------------------
            existing_names = {}      
            existing_tv_eps = {}     
            existing_movie_vids = [] 
            
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT id, name FROM p115_filesystem_cache WHERE parent_id = %s", (str(batch_target_cid),))
                        for row in cursor.fetchall():
                            e_name = row['name']
                            e_fid = str(row['id'])
                            e_ext = e_name.split('.')[-1].lower() if '.' in e_name else ''
                            
                            if e_ext in known_video_exts:
                                existing_names[e_name] = e_fid
                                if self.media_type == 'tv':
                                    match = re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*(?:e|E|p|P)(\d{1,4})\b', e_name, re.IGNORECASE)
                                    if match:
                                        s, e = int(match.group(1)), int(match.group(2))
                                        if (s, e) not in existing_tv_eps: existing_tv_eps[(s, e)] = []
                                        existing_tv_eps[(s, e)].append(e_fid)
                                else:
                                    existing_movie_vids.append(e_fid)
            except Exception as e:
                logger.warning(f"  ➜ [冲突检测] 查询本地缓存失败: {e}")

            # =================================================================
            # ★★★ 核心修复 1：视频优先排序 ★★★
            # =================================================================
            items.sort(key=lambda x: 0 if (x['_new_filename'].split('.')[-1].lower() in known_video_exts) else 1)
            known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

            # =================================================================
            # ★★★ 核心修复 4：外挂字幕豁免机制 ★★★
            # 扫描本批次，记录哪些视频带有外挂字幕 (支持电影和剧集)
            # =================================================================
            episodes_with_ext_subs = set()
            for item in items:
                temp_name = item['_new_filename']
                temp_ext = temp_name.split('.')[-1].lower() if '.' in temp_name else ''
                if temp_ext in known_sub_exts:
                    episodes_with_ext_subs.add((item.get('_season_num'), item.get('_episode_num')))

            valid_items = []
            fids_to_delete = set()
            rejected_episodes = set()

            from handler.resubscribe_service import WashingService
            original_lang = (self.raw_metadata or {}).get('lang_code')
            
            for item in items:
                new_name = item['_new_filename']
                s_num = item.get('_season_num')
                e_num = item.get('_episode_num')
                ext = new_name.split('.')[-1].lower() if '.' in new_name else ''
                is_vid = ext in known_video_exts
                file_size = _parse_115_size(item.get('fs') or item.get('size'))
                
                # 检查带头大哥是否已经挂了
                if (s_num, e_num) in rejected_episodes:
                    logger.info(f"  ➜ [关联跳过] 视频已被拦截/跳过，同步忽略字幕: {new_name}")
                    unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                    continue

                # 判断当前文件是否享有洗版特权
                is_ep_active_washing = False
                if self.media_type == 'tv' and s_num is not None and e_num is not None:
                    is_ep_active_washing = (s_num, e_num) in active_washing_eps
                elif self.media_type == 'movie':
                    is_ep_active_washing = movie_active_washing
                
                effective_conflict_mode = 'replace' if is_ep_active_washing else conflict_mode

                # ★ 判断是否享有外挂字幕豁免权
                has_ext_sub = (s_num, e_num) in episodes_with_ext_subs

                # 调用阶梯洗版优先级服务
                if is_vid and effective_conflict_mode == 'replace':
                    # ★ 核心修复：手动重组拥有最高特权，无视洗版规则直接放行！
                    if getattr(self, 'is_manual_correct', False):
                        action = 'REPLACE'
                        reason = '手动重组，无视洗版规则强制放行'
                    else:
                        logger.debug(f"  ➜ [覆盖模式:洗版] 正在调用洗版规则评估文件: {new_name}")
                        
                        video_info = item.get('_video_info') or self._extract_video_info(new_name)
                        file_sha1 = item.get('sha1') or item.get('sha')
                        
                        action, reason = WashingService.decide_washing_action(
                            sha1=file_sha1,
                            file_name=new_name,
                            file_size=file_size,
                            target_cid=target_cid,
                            media_type=self.media_type,
                            tmdb_id=self.tmdb_id,
                            season_num=s_num,
                            episode_num=e_num,
                            original_lang=original_lang,
                            is_active_washing=is_ep_active_washing,
                            has_external_subtitle=has_ext_sub # ★★★ 传入外挂字幕豁免标志 ★★★
                        )
                    
                    if action == 'REJECT':
                        logger.warning(f"  ➜ [洗版拦截] {new_name} -> {reason}")
                        unqualified_items.append({
                            'fid': item.get('fid') or item.get('file_id'), 'name': item.get('fn') or item.get('file_name'), 
                            'reason': reason, 'pc': item.get('pc') or item.get('pick_code'), 'season_num': s_num
                        })
                        # ★ 记入黑名单，株连九族
                        rejected_episodes.add((s_num, e_num))
                        continue
                    elif action == 'SKIP':
                        logger.info(f"  ➜ [洗版跳过] {new_name} -> {reason}")
                        unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                        # ★ 记入黑名单，株连九族
                        rejected_episodes.add((s_num, e_num))
                        continue
                    elif action == 'REPLACE':
                        logger.info(f"  ➜ [洗版替换] {new_name} -> {reason}")
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            fids_to_delete.update(existing_tv_eps.get((s_num, e_num), []))
                        else:
                            fids_to_delete.update(existing_movie_vids)
                        valid_items.append(item)
                    elif action == 'ACCEPT':
                        logger.info(f"  ➜ [洗版入库] {new_name} -> {reason}")
                        if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                        valid_items.append(item)
                else:
                    # 非视频文件，或非替换模式
                    is_conflict = False
                    conflict_old_fids = []
                    if is_vid:
                        if self.media_type == 'tv' and s_num is not None and e_num is not None:
                            if (s_num, e_num) in existing_tv_eps:
                                is_conflict = True
                                conflict_old_fids = existing_tv_eps[(s_num, e_num)]
                        elif self.media_type == 'movie':
                            if existing_movie_vids:
                                is_conflict = True
                                conflict_old_fids = existing_movie_vids
                    
                    if is_conflict:
                        if conflict_mode == 'skip':
                            logger.info(f"  ➜ [覆盖模式:跳过] 目标目录已存在同集/同电影，放弃处理: {new_name}")
                            unrecognized_fids.append(item.get('fid') or item.get('file_id'))
                            continue 
                        elif conflict_mode == 'keep_both':
                            logger.info(f"  ➜ [覆盖模式:共存] 目标目录已存在同集/同电影，保留两者: {new_name}")
                            if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                            valid_items.append(item)
                    else:
                        if new_name in existing_names: fids_to_delete.add(existing_names[new_name])
                        valid_items.append(item)
            
            if not valid_items:
                continue # 这批全被 skip/reject 了
                
            # -----------------------------------------------------------
            # ★ 2. 执行删除旧文件 (洗版/同名覆盖 + 完美擦屁股)
            # -----------------------------------------------------------
            if fids_to_delete:
                logger.warning(f"  ➜ [版本控制] 正在删除 {len(fids_to_delete)} 个被替换的旧版本文件...")
                
                # === 本地擦屁股逻辑 ===
                local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
                old_strm_paths_for_emby = []
                old_dirs_to_check = set() # ★ 新增：记录需要检查是否为空的旧目录
                
                if local_root and os.path.exists(local_root):
                    try:
                        from database.connection import get_db_connection
                        with get_db_connection() as conn:
                            with conn.cursor() as cursor:
                                # 从缓存查出旧文件的本地路径
                                cursor.execute("SELECT local_path FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(fids_to_delete),))
                                for row in cursor.fetchall():
                                    old_file_rel_path = row['local_path']
                                    if not old_file_rel_path: continue
                                    
                                    old_file_rel_path = str(old_file_rel_path).lstrip('\\/')
                                    old_strm_rel_path = os.path.splitext(old_file_rel_path)[0] + ".strm"
                                    old_strm_full_path = os.path.join(local_root, old_strm_rel_path)
                                    
                                    old_strm_paths_for_emby.append(old_strm_full_path)
                                    
                                    # 1. 删除 STRM
                                    if os.path.exists(old_strm_full_path):
                                        os.remove(old_strm_full_path)
                                        logger.debug(f"  ➜ 删除本地旧 STRM: {old_strm_full_path}")
                                        
                                    # 2. 删除 mediainfo.json
                                    old_mi_full_path = os.path.splitext(old_file_rel_path)[0] + "-mediainfo.json"
                                    if os.path.exists(old_mi_full_path):
                                        os.remove(old_mi_full_path)
                                        
                                    # 3. 删除关联字幕和专属 NFO
                                    old_dir_full_path = os.path.dirname(old_strm_full_path)
                                    old_base_name = os.path.splitext(os.path.basename(old_file_rel_path))[0]
                                    if os.path.exists(old_dir_full_path):
                                        old_dirs_to_check.add(old_dir_full_path) # ★ 记录旧目录
                                        for f in os.listdir(old_dir_full_path):
                                            # ★ 核心修复：把 nfo 和图片也加进去，连同字幕一起删，彻底擦干净屁股
                                            if f.startswith(old_base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup', 'nfo', 'jpg', 'png', 'jpeg', 'bif']:
                                                sub_to_del = os.path.join(old_dir_full_path, f)
                                                try:
                                                    os.remove(sub_to_del)
                                                    logger.debug(f"  ➜ 删除本地旧附属文件: {sub_to_del}")
                                                except: pass
                                                
                        # ★ 4. 向上递归清理本地空目录 (连锅端海报和tvshow.nfo)
                        if old_dirs_to_check:
                            import shutil
                            protected_dirs = {os.path.abspath(local_root)}
                            for rule in self.rules:
                                cat_path = rule.get('category_path') or rule.get('dir_name')
                                if cat_path:
                                    protected_dirs.add(os.path.abspath(os.path.join(local_root, cat_path.lstrip('\\/'))))
                            protected_dirs.add(os.path.abspath(os.path.join(local_root, "未识别")))

                            for old_dir in list(old_dirs_to_check):
                                curr_dir = old_dir
                                while curr_dir and curr_dir not in protected_dirs:
                                    if os.path.exists(curr_dir):
                                        has_media = False
                                        for root_dir, _, files in os.walk(curr_dir):
                                            for f in files:
                                                ext = f.split('.')[-1].lower()
                                                if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov'}:
                                                    has_media = True
                                                    break
                                            if has_media: break

                                        if not has_media:
                                            shutil.rmtree(curr_dir)
                                            logger.info(f"  ➜ 本地旧目录已无媒体文件，连目录删除: {curr_dir}")
                                            curr_dir = os.path.dirname(curr_dir)
                                        else:
                                            break
                                    else:
                                        break
                    except Exception as e:
                        logger.warning(f"  ➜ 清理本地旧文件失败: {e}")
                
                # === 执行网盘删除和缓存清理 ===
                self.client.fs_delete(list(fids_to_delete))
                P115CacheManager.delete_files(list(fids_to_delete))
                P115RecordManager.delete_records(list(fids_to_delete))
                
            # -----------------------------------------------------------
            # ★ 3. 执行移动新文件
            # -----------------------------------------------------------
            move_fids = [item.get('fid') or item.get('file_id') for item in valid_items]
            move_res = self.client.fs_move(move_fids, batch_target_cid)
            
            if move_res.get('state'):
                display_target = std_root_name
                if valid_items and valid_items[0].get('_s_name'):
                    display_target = f"{std_root_name} - {valid_items[0]['_s_name']}"
                logger.info(f"  ➜ [批量移动] 成功将 {len(move_fids)} 个文件移动至 -> {display_target}")
                
                # -----------------------------------------------------------
                # ★ 4. 执行重命名
                # -----------------------------------------------------------
                for item in valid_items:
                    fid = str(item.get('fid') or item.get('file_id'))
                    old_name = item.get('fn') or item.get('n') or item.get('file_name')
                    new_name = item['_new_filename']
                    if old_name != new_name:
                        ren_res = self.client.fs_rename((fid, new_name))
                        if ren_res.get('state'):
                            logger.info(f"  ➜ [重命名] {old_name} -> {new_name}")
                        else:
                            logger.warning(f"  ➜ [重命名失败] {old_name} -> {new_name}, 原因: {ren_res.get('error_msg', ren_res)}")
                            # ★ 核心修复：如果 115 API 重命名失败，强制退回原名，确保后续生成的 STRM 挂载路径 100% 准确！
                            item['_new_filename'] = old_name
                
                # -----------------------------------------------------------
                # ★ 5. 生成 STRM 和记录日志
                # -----------------------------------------------------------
                processed_episodes_for_flag = set() # ★ 新增：记录成功处理的集数
                
                for file_item in valid_items:
                    fid = file_item.get('fid') or file_item.get('file_id')
                    file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                    new_filename = file_item['_new_filename']
                    season_num = file_item['_season_num']
                    s_name = file_item['_s_name']
                    is_center_cached = file_item['_is_center_cached']
                    
                    moved_count += 1
                    
                    # ★ 记录成功处理的集数，用于后续精准核销特权
                    if self.media_type == 'tv' and season_num is not None and file_item.get('_episode_num') is not None:
                        processed_episodes_for_flag.add((season_num, file_item.get('_episode_num')))
                    pick_code = file_item.get('pc') or file_item.get('pick_code') 
                    file_sha1 = file_item.get('sha1') or file_item.get('sha')
                    ext = new_filename.split('.')[-1].lower() if '.' in new_filename else ''
                    
                    # 整理日志
                    if ext in known_video_exts:
                        try:
                            category_name = "未识别"
                            for rule in self.rules:
                                if str(rule.get('cid')) == str(target_cid):
                                    category_name = rule.get('dir_name', '未识别')
                                    break
                            P115RecordManager.add_or_update_record(
                                file_id=fid,
                                original_name=file_name,
                                status='success',
                                tmdb_id=self.tmdb_id,
                                media_type=self.media_type,
                                target_cid=target_cid,
                                category_name=category_name,
                                renamed_name=new_filename,
                                is_center_cached=is_center_cached if not keep_original else False,
                                pick_code=pick_code,
                                season_number=season_num 
                            )
                        except Exception as e:
                            logger.error(f"  ➜ 记录文件整理日志失败: {e}")

                    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
                    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "http://127.0.0.1:5257").rstrip('/')
                    
                    if pick_code and local_root and os.path.exists(local_root):
                        try:
                            category_name = None
                            for rule in self.rules:
                                if rule.get('cid') == str(target_cid):
                                    category_name = rule.get('dir_name', '未识别')
                                    break
                            if not category_name: category_name = "未识别"

                            category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
                            relative_category_path = "未识别"
                            
                            if category_rule:
                                if 'category_path' in category_rule and category_rule['category_path']:
                                    relative_category_path = category_rule['category_path']
                                else:
                                    media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
                                    try:
                                        dir_info = self.client.fs_files({'cid': target_cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                                        path_nodes = dir_info.get('path', [])
                                        start_idx = 0
                                        found_root = False
                                        
                                        if media_root_cid == '0':
                                            if str(target_cid) == '0': start_idx = 0
                                            else: start_idx = 1 
                                            found_root = True
                                        else:
                                            for i, node in enumerate(path_nodes):
                                                if str(node.get('cid') or node.get('file_id')) == media_root_cid:
                                                    start_idx = i + 1
                                                    found_root = True
                                                    break
                                        
                                        if found_root and start_idx < len(path_nodes):
                                            rel_segments = [str(n.get('file_name') or n.get('fn') or n.get('name') or n.get('n')).strip() for n in path_nodes[start_idx:] if (n.get('file_name') or n.get('fn') or n.get('name') or n.get('n'))]
                                            relative_category_path = "/".join(rel_segments) if rel_segments else category_rule.get('dir_name', '未识别')
                                        else:
                                            relative_category_path = category_rule.get('dir_name', '未识别')
                                            
                                        category_rule['category_path'] = relative_category_path
                                        settings_db.save_setting('p115_sorting_rules', self.rules)
                                        
                                    except Exception as e:
                                        relative_category_path = category_rule.get('dir_name', '未识别')

                            if keep_original:
                                rel_path = file_item.get('rel_path', '')
                                if rel_path:
                                    local_dir = os.path.join(local_root, relative_category_path, std_root_name, rel_path.replace('/', os.sep))
                                else:
                                    local_dir = os.path.join(local_root, relative_category_path, std_root_name)
                            elif self.media_type == 'tv' and season_num is not None:
                                local_dir = os.path.join(local_root, relative_category_path, std_root_name, s_name)
                            else:
                                local_dir = os.path.join(local_root, relative_category_path, std_root_name)
                            
                            os.makedirs(local_dir, exist_ok=True) 

                            try:
                                main_folder_path = os.path.join(relative_category_path, std_root_name)
                                P115CacheManager.update_local_path(final_home_cid, main_folder_path)
                                if keep_original:
                                    rel_path = file_item.get('rel_path', '')
                                    if rel_path:
                                        P115CacheManager.update_local_path(batch_target_cid, os.path.join(main_folder_path, rel_path.replace('/', os.sep)))
                                elif self.media_type == 'tv' and season_num is not None:
                                    P115CacheManager.update_local_path(batch_target_cid, os.path.join(main_folder_path, s_name))
                            except Exception: pass 

                            is_video = ext in known_video_exts
                            is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']

                            if is_video:
                                strm_filename = os.path.splitext(new_filename)[0] + ".strm"
                                strm_filepath = os.path.join(local_dir, strm_filename)
                                if not etk_url.startswith('http'):
                                    mount_prefix = etk_url
                                    if keep_original:
                                        rel_path = file_item.get('rel_path', '')
                                        if rel_path: mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, rel_path.replace('/', os.sep), new_filename)
                                        else: mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, new_filename)
                                    elif self.media_type == 'tv' and season_num is not None:
                                        mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, s_name, new_filename)
                                    else:
                                        mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, new_filename)
                                    strm_content = mount_path.replace('\\', '/')
                                else:
                                    strm_content = f"{etk_url}/api/p115/play/{pick_code}"
                                    if cfg.get('strm_url_fmt') == 'with_name':
                                        strm_content = f"{strm_content}/{new_filename}"
                                
                                with open(strm_filepath, 'w', encoding='utf-8') as f:
                                    f.write(strm_content)
                                logger.info(f"  ➜ STRM 已生成 -> {strm_filename}")
                                
                                try:
                                    from monitor_service import enqueue_file_actively
                                    enqueue_file_actively(strm_filepath)
                                except Exception: pass

                                if not file_sha1 and fid:
                                    try:
                                        info_res = self.client.fs_get_info(fid)
                                        if info_res.get('state') and info_res.get('data'):
                                            file_sha1 = info_res['data'].get('sha1')
                                    except Exception: pass

                                if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                                    try:
                                        mediainfo_filename = os.path.splitext(new_filename)[0] + "-mediainfo.json"
                                        mediainfo_filepath = os.path.join(local_dir, mediainfo_filename)
                                        mediainfo_text = P115CacheManager.get_mediainfo_cache_text(file_sha1) if file_sha1 else None
                                        if mediainfo_text:
                                            with open(mediainfo_filepath, 'w', encoding='utf-8') as f:
                                                f.write(mediainfo_text)
                                            logger.info(f"  ➜ 媒体信息文件已生成 -> {mediainfo_filename}")
                                        else:
                                            logger.debug(f"  ➜ 跳过媒体信息文件生成，未命中本地缓存: {new_filename}")
                                    except Exception as e:
                                        logger.error(f"  ➜ 生成媒体信息文件失败: {e}")

                                if keep_original:
                                    rel_path = file_item.get('rel_path', '')
                                    if rel_path: file_local_path = os.path.join(relative_category_path, std_root_name, rel_path.replace('/', os.sep), new_filename)
                                    else: file_local_path = os.path.join(relative_category_path, std_root_name, new_filename)
                                elif self.media_type == 'tv' and season_num is not None:
                                    file_local_path = os.path.join(relative_category_path, std_root_name, s_name, new_filename)
                                else:
                                    file_local_path = os.path.join(relative_category_path, std_root_name, new_filename)
                                
                                file_local_path = file_local_path.replace('\\', '/')
                                file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))

                                if pick_code and fid:
                                    P115CacheManager.save_file_cache(
                                        fid, batch_target_cid, new_filename, 
                                        sha1=file_sha1, pick_code=pick_code, 
                                        local_path=file_local_path, size=file_size 
                                    )
                                    
                            elif is_sub:
                                if config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True):
                                    sub_filepath = os.path.join(local_dir, new_filename)
                                    if not os.path.exists(sub_filepath):
                                        try:
                                            url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                                            if url_obj:
                                                import requests
                                                headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                                resp = requests.get(str(url_obj), stream=True, timeout=30, headers=headers)
                                                resp.raise_for_status()
                                                with open(sub_filepath, 'wb') as f:
                                                    for chunk in resp.iter_content(chunk_size=8192): f.write(chunk)
                                                logger.info(f"  ➜ [字幕下载] {new_filename} 下载完成！")
                                        except Exception as e:
                                            logger.error(f"  ➜ 下载字幕失败: {e}")
                            
                        except Exception as e:
                            logger.error(f"  ➜ 生成 STRM 文件失败: {e}", exc_info=True)
                    if progress_callback:
                        progress_callback()
            else:
                raw_err_msg = str(move_res.get('error_msg', move_res))
                if (
                    'Expecting value: line 1 column 1 (char 0)' in raw_err_msg
                    or 'JSONDecodeError' in raw_err_msg
                    or '<html' in raw_err_msg.lower()
                    or '<!doctype html' in raw_err_msg.lower()
                ):
                    err_msg = '该片无法整理，请手动重命名移动后增量生成STRM。'
                else:
                    err_msg = raw_err_msg

                logger.error(f"  ➜ [批量移动失败] 目标CID:{batch_target_cid}, 包含 {len(move_fids)} 个文件, 原因: {err_msg}")
                
                if '不存在' in raw_err_msg or move_res.get('code') in [20004, 70004]:
                    logger.warning(f"  ➜ 检测到目标目录在网盘中已不存在，正在清理失效缓存: CID {batch_target_cid}")
                    P115CacheManager.delete_cid(batch_target_cid)
                if progress_callback:
                    for _ in valid_items:
                        progress_callback()

        # =================================================================
        # ★★★ 终极清理：将所有不合规文件移入未识别目录 ★★★
        # =================================================================
        if unrecognized_fids and unidentified_cid:
            logger.info(f"  ➜ 发现 {len(unrecognized_fids)} 个不合规文件(扩展名不符/花絮/样本/广告)，正在移入未识别目录...")
            # 同样传入列表，防止 115 API 报错
            self.client.fs_move(unrecognized_fids, unidentified_cid)
            
        if unqualified_items and unidentified_cid:
            logger.info(f"  ➜ 发现 {len(unqualified_items)} 个质检不合格文件，正在移入未识别目录...")
            unq_fids = [item['fid'] for item in unqualified_items if item['fid']]
            self.client.fs_move(unq_fids, unidentified_cid)
            
            for item in unqualified_items:
                P115RecordManager.add_or_update_record(
                    file_id=item['fid'],
                    original_name=item['name'],
                    status='unqualified',
                    tmdb_id=self.tmdb_id,
                    media_type=self.media_type,
                    target_cid=target_cid,
                    category_name="质检不合格",
                    renamed_name=None,
                    pick_code=item['pc'],
                    season_number=item['season_num'],
                    fail_reason=item['reason']
                )
                
            # ★★★ 触发 TG 拦截通知 (聚合版) ★★★
            try:
                from handler.telegram import send_intercept_notification
                grouped_unqualified = {}
                for item in unqualified_items:
                    reason = item['reason']
                    if reason not in grouped_unqualified:
                        grouped_unqualified[reason] = []
                    grouped_unqualified[reason].append(item['name'])
                    
                for reason, names in grouped_unqualified.items():
                    send_intercept_notification(names, reason)
            except Exception as e:
                logger.error(f"  ➜ 触发拦截通知失败: {e}")

        # =================================================================
        # ★ 极简垃圾回收：直接通知缓冲队列检查“待整理”目录
        # =================================================================
        if not skip_gc:
            if not (not is_batch and root_item.get('_skip_gc')):
                logger.info(f"  ➜ [清理空目录] 整理完毕，已通知全局垃圾回收器检查待整理目录...")
                from handler.p115_service import P115DeleteBuffer
                P115DeleteBuffer.add(check_save_path=True)
            else:
                logger.info("  ➜ [MP上传] 单文件跳过垃圾回收检查。")
        else:
            logger.debug("  ➜ [清理空目录] 批量任务模式，跳过单次垃圾回收检查，等待统一清理。")

        # --- 整理记录 ---
        if moved_count > 0:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        if self.media_type == 'tv' and processed_episodes_for_flag:
                            # ★ 核心修复：取交集，只核销真正拥有特权且本次处理成功的集数
                            actually_washed_eps = processed_episodes_for_flag.intersection(active_washing_eps)
                            if actually_washed_eps:
                                from psycopg2.extras import execute_batch
                                update_data = [(str(self.tmdb_id), s, e) for s, e in actually_washed_eps]
                                execute_batch(cursor, """
                                    UPDATE media_metadata 
                                    SET active_washing = FALSE 
                                    WHERE parent_series_tmdb_id = %s 
                                      AND item_type = 'Episode' 
                                      AND season_number = %s 
                                      AND episode_number = %s
                                """, update_data)
                                logger.info(f"  ➜ [洗版特权] 已精准核销 {len(actually_washed_eps)} 个分集的洗版特权状态。")
                        elif self.media_type == 'movie' and movie_active_washing:
                            cursor.execute("UPDATE media_metadata SET active_washing = FALSE WHERE tmdb_id = %s AND item_type = 'Movie'", (str(self.tmdb_id),))
                            logger.info(f"  ➜ [洗版特权] 已核销电影的洗版特权状态。")
                        conn.commit()
            except Exception as e:
                logger.error(f"  ➜ 解除洗版状态失败: {e}")

        return True
    
    def execute_mp_passthrough(self, file_nodes):
        """
        MP直出模式 (终极优化版)：
        完全信任 115 现有的目录结构和文件名 (直接从 Webhook 传来的 115_path 提取)。
        跳过整理、归类、移动、重命名。
        直接在本地 1:1 映射生成 STRM 和 -mediainfo.json。
        """
        config = get_config()
        local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
        etk_url = (config.get(constants.CONFIG_OPTION_ETK_SERVER_URL) or "").rstrip("/")
        media_root_name = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_NAME) or "").strip("/")

        if not local_root or not etk_url:
            logger.warning("  ➜ [MP直出] 未配置本地 STRM 根目录或 ETK 地址，跳过。")
            return False

        os.makedirs(local_root, exist_ok=True)

        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

        for file_item in file_nodes:
            original_name = file_item.get("fn") or file_item.get("file_name") or ""
            if "." not in original_name:
                continue

            # ★ 修复：使用 [-1] 提取后缀，防止文件名中包含多个点导致报错
            ext = original_name.rsplit(".", 1)[-1].lower()
            fid = file_item.get("fid") or file_item.get("file_id")
            parent_id = file_item.get("pid") or file_item.get("parent_id")
            pick_code = file_item.get("pc") or file_item.get("pick_code")
            sha1 = file_item.get("sha1") or file_item.get("sha")
            full_115_path = file_item.get("115_path")

            is_video = ext in known_video_exts
            is_sub = ext in sub_exts
            
            if not is_video and not is_sub:
                continue

            # ==========================================================
            # ★ 核心优化：直接从 Webhook 传来的 115_path 提取相对路径，0 API 消耗！
            # 彻底砍掉画蛇添足的 Season 拼接逻辑，完全 1:1 映射 115 物理路径
            # ==========================================================
            parent_rel_path = ""
            if full_115_path:
                path_parts = [p for p in full_115_path.split('/') if p]
                
                start_idx = 0
                if media_root_name and media_root_name in path_parts:
                    # 如果配置了根目录名称，从根目录的下一级开始截取
                    start_idx = path_parts.index(media_root_name) + 1
                elif len(path_parts) > 1:
                    # 兜底：如果没有配置，默认剥离第一层目录 (如 /影视待整理/)
                    start_idx = 1
                    
                if len(path_parts) > start_idx:
                    # 剥离根目录，并去掉最后的文件名，剩下的就是纯净的相对目录！
                    # 例如：['影视待整理', '虾路相逢', 'Season 01', 'S01E01.mkv'] -> '虾路相逢/Season 01'
                    parent_rel_path = "/".join(path_parts[start_idx:-1])
            else:
                logger.warning(f"  ➜ [MP直出] 缺少 115_path 参数，无法映射目录结构: {original_name}")
                continue

            # 确定本地落盘目录
            local_dir = os.path.join(local_root, parent_rel_path) if parent_rel_path else local_root
            os.makedirs(local_dir, exist_ok=True)

            # 补齐 SHA1 (仅视频需要，用于缓存 mediainfo)
            if is_video and not sha1 and fid:
                try:
                    info_res = self.client.fs_get_info(fid)
                    if info_res.get('state') and info_res.get('data'):
                        fetched_sha1 = info_res['data'].get('sha1')
                        if fetched_sha1:
                            sha1 = str(fetched_sha1).upper()
                            file_item['sha1'] = sha1
                except Exception:
                    pass

            # 1. 处理视频 (STRM + Mediainfo)
            if is_video and pick_code:
                # 生成 STRM
                strm_filename = os.path.splitext(original_name)[0] + ".strm"
                strm_filepath = os.path.join(local_dir, strm_filename)

                if not etk_url.startswith("http"):
                    # 挂载模式
                    mount_path = os.path.join(etk_url, parent_rel_path, original_name).replace("\\", "/")
                    strm_content = mount_path
                else:
                    # API 模式
                    strm_content = f"{etk_url}/api/p115/play/{pick_code}/{original_name}"

                with open(strm_filepath, "w", encoding="utf-8") as f:
                    f.write(strm_content)
                logger.info(f"  ➜ [MP直出] STRM 已生成 -> {strm_filename}")

                # 生成 Mediainfo
                if config.get(constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO, False):
                    try:
                        mediainfo_text = None
                        if sha1:
                            mediainfo_text = P115CacheManager.get_mediainfo_cache_text(sha1)

                        if not mediainfo_text:
                            mediainfo_obj = self._probe_mediainfo_with_ffprobe(file_item, sha1=sha1, silent_log=False)
                            if mediainfo_obj:
                                probe_sha1 = sha1 or file_item.get('sha1') or file_item.get('sha')
                                if probe_sha1:
                                    probe_sha1 = str(probe_sha1).upper()
                                    P115CacheManager.save_mediainfo_cache(probe_sha1, mediainfo_obj)
                                    sha1 = probe_sha1
                                    file_item['sha1'] = probe_sha1
                                mediainfo_text = json.dumps(mediainfo_obj, ensure_ascii=False, indent=2)

                        if mediainfo_text:
                            mediainfo_filename = os.path.splitext(original_name)[0] + "-mediainfo.json"
                            mediainfo_filepath = os.path.join(local_dir, mediainfo_filename)
                            with open(mediainfo_filepath, "w", encoding="utf-8") as f:
                                f.write(mediainfo_text)
                            logger.info(f"  ➜ [MP直出] 媒体信息已生成 -> {mediainfo_filename}")
                    except Exception as e:
                        logger.error(f"  ➜ [MP直出] 生成媒体信息失败: {e}")

            # 2. 处理字幕 (直接下载)
            elif is_sub and config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True) and pick_code:
                try:
                    sub_filepath = os.path.join(local_dir, original_name)
                    if not os.path.exists(sub_filepath):
                        url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                        if url_obj:
                            import requests
                            headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                            resp = requests.get(str(url_obj), stream=True, timeout=30, headers=headers)
                            resp.raise_for_status()
                            with open(sub_filepath, "wb") as f:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            logger.info(f"  ➜ [MP直出] 字幕下载完成 -> {original_name}")
                except Exception as e:
                    logger.error(f"  ➜ [MP直出] 下载字幕失败: {e}")

            # 3. 写入数据库缓存 (保持 Emby 扫库和后续删除的闭环)
            try:
                file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            except Exception:
                file_size = 0

            file_local_path = os.path.join(parent_rel_path, original_name).replace("\\", "/") if parent_rel_path else original_name

            if fid and pick_code:
                P115CacheManager.save_file_cache(
                    fid=fid,
                    parent_id=parent_id,
                    name=original_name,
                    sha1=sha1,
                    pick_code=pick_code,
                    local_path=file_local_path,
                    size=file_size
                )

                P115RecordManager.add_or_update_record(
                    file_id=fid,
                    pick_code=pick_code,
                    original_name=original_name,
                    status="success",
                    tmdb_id=self.tmdb_id,
                    media_type=self.media_type,
                    target_cid=parent_id,
                    category_name="MP直出",
                    renamed_name=original_name,
                    season_number=file_item.get('_forced_season')
                )

        return True

def _parse_115_size(size_val):
    """
    统一解析 115 返回的文件大小为字节(Int)
    支持: 12345(int), "12345"(str), "1.2GB", "500KB"
    """
    try:
        if size_val is None: return 0

        # 1. 如果已经是数值 (115 API 's' 字段通常是 int)
        if isinstance(size_val, (int, float)):
            return int(size_val)

        # 2. 如果是字符串
        if isinstance(size_val, str):
            s = size_val.strip()
            if not s: return 0
            # 纯数字字符串
            if s.isdigit():
                return int(s)

            s_upper = s.upper().replace(',', '')
            mult = 1
            if 'TB' in s_upper: mult = 1024**4
            elif 'GB' in s_upper: mult = 1024**3
            elif 'MB' in s_upper: mult = 1024**2
            elif 'KB' in s_upper: mult = 1024

            match = re.search(r'([\d\.]+)', s_upper)
            if match:
                return int(float(match.group(1)) * mult)
    except Exception:
        pass
    return 0

def _identify_media_enhanced(filename, main_dir_name=None, has_season_subdirs=False, forced_media_type=None, ai_translator=None, use_ai=False, is_folder=False):
    """
    【绝对正确版】识别逻辑：
    1. 先定类型：综合主目录、子目录特征、文件名，判断是 Movie 还是 TV。
    2. 再提 ID：优先从主目录/文件名提取 {tmdb=xxx}。
    3. 目录拦截：如果是目录且没有显式 ID，直接返回 None，强制深入扫描子目录！
    4. 定向查询：用确定的类型 + 提取的 Title (Year) 向 TMDb 发起查询。
    """
    tmdb_id = None
    media_type = 'movie' # 默认兜底
    title = filename
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    
    is_same_name = (main_dir_name == filename)

    # =================================================================
    # ★ 第一步：铁腕判定媒体类型 (Movie or TV)
    # =================================================================
    if forced_media_type:
        media_type = forced_media_type
    else:
        # 将主目录名和文件名拼在一起，寻找剧集特征
        combined_text = f"{main_dir_name or ''} {filename}"
        if has_season_subdirs or re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|episode)[ \.\-]*\d{1,4}\b|(?:^|[ \.\-\_\[\(])e\d{1,4}\b|第[一二三四五六七八九十\d]+季|Season', combined_text, re.IGNORECASE):
            media_type = 'tv'

    # 辅助函数：用已锁定的类型去 TMDb 查官方标题
    def _fetch_title_by_id(ext_id, m_type):
        if not api_key: return None
        try:
            if m_type == 'tv':
                res = tmdb.get_tv_details(ext_id, api_key)
                if res: return res.get('name') or res.get('original_name')
            else:
                res = tmdb.get_movie_details(ext_id, api_key)
                if res: return res.get('title') or res.get('original_title')
        except Exception:
            pass
        return None

    # =================================================================
    # ★ 第二步：按优先级提取信息并定向查询
    # =================================================================
    
    # 优先级 1: 显式 TMDb ID (最高优先级，绝不误判)
    tmdb_regex = r'(?:tmdb|tmdbid)[=\-_]*(\d+)'
    
    # 1.1 优先从 filename 提取
    match_id_file = re.search(tmdb_regex, filename, re.IGNORECASE)
    if match_id_file:
        tmdb_id = match_id_file.group(1)
        clean_name = re.sub(r'\[.*?\]|\{.*?\}|\(.*?\)', '', filename).strip()
        official_title = _fetch_title_by_id(tmdb_id, media_type)
        return tmdb_id, media_type, official_title or clean_name or filename

    # 1.2 其次从 main_dir_name 提取
    if main_dir_name:
        match_id_dir = re.search(tmdb_regex, main_dir_name, re.IGNORECASE)
        if match_id_dir:
            tmdb_id = match_id_dir.group(1)
            clean_name = re.sub(r'\[.*?\]|\{.*?\}|\(.*?\)', '', main_dir_name).strip()
            official_title = _fetch_title_by_id(tmdb_id, media_type)
            return tmdb_id, media_type, official_title or clean_name or main_dir_name

    # ★★★ 核心拦截：如果是目录，且没有显式 ID，直接返回 None，强制深入扫描子目录！★★★
    if is_folder:
        return None, None, None

    # 优先级 2: 提取 Title (Year) 进行搜索 (仅限文件)
    def _search_by_title_year(text):
        # 剔除 S01E01 等干扰字符 (连同前面的点和下划线一起剔除，防止留下 "The.Crown.")
        clean_text = re.sub(r'(?i)[\.\s\-_]*s\d{1,4}(?:e\d{1,4})?\b.*$', '', text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*ep?\d{1,4}\b.*$', '', clean_text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*season\s*\d{1,4}\b.*$', '', clean_text).strip()
        clean_text = re.sub(r'(?i)[\.\s\-_]*第[一二三四五六七八九十\d]+季.*$', '', clean_text).strip()

        # 尝试提取年份 (不再强制要求必须有年份)
        name_part = clean_text
        year_part = None
        match_std = re.search(r'[\(\[\.\s_-](\d{4})(?:[\)\]\.\s_-]|$)', clean_text)
        if match_std:
            year_part = match_std.group(1)
            # 把年份从名字里剔除
            name_part = clean_text[:match_std.start()].strip()

        # 清理名字里的点和下划线
        name_part = name_part.replace('.', ' ').replace('_', ' ').strip()

        if not name_part: return None

        try:
            if api_key:
                search_key = f"{name_part}_{year_part}_{media_type}"
                if search_key in _TMDB_SEARCH_CACHE:
                    results = _TMDB_SEARCH_CACHE[search_key]
                else:
                    # 严格按照锁定的 media_type 搜索
                    results = tmdb.search_media(query=name_part, api_key=api_key, item_type=media_type, year=year_part)
                    _TMDB_SEARCH_CACHE[search_key] = results

                if results and len(results) > 0:
                    best = results[0]
                    # ★★★ 核心修复：精准匹配，防止 TMDb 瞎给结果 ★★★
                    name_lower = name_part.lower()
                    name_parts = [p for p in name_lower.split() if p]
                    
                    for res in results:
                        res_title = (res.get('title') or res.get('name') or '').lower()
                        res_orig = (res.get('original_title') or res.get('original_name') or '').lower()
                        
                        if name_lower == res_title or name_lower == res_orig:
                            best = res
                            break
                            
                        part_match = False
                        for part in name_parts:
                            if part == res_title or part == res_orig:
                                best = res
                                part_match = True
                                break
                        if part_match:
                            break
                            
                    return str(best['id']), media_type, (best.get('title') or best.get('name'))
        except Exception:
            pass
        return None

    # 2.1 优先从 filename 搜索
    res = _search_by_title_year(filename)
    if res: return res

    # 2.2 其次从 main_dir_name 搜索
    if main_dir_name and not is_same_name:
        res = _search_by_title_year(main_dir_name)
        if res: return res

    # =================================================================
    # ★ 第三步：MoviePilot 辅助识别 (免费、快速、高准确率)
    # =================================================================
    mp_config = settings_db.get_setting('mp_config') or {}
    use_mp_recognition = mp_config.get('moviepilot_recognition', False)
    if use_mp_recognition:
        import handler.moviepilot as mp
        target_mp_name = main_dir_name if main_dir_name else filename
        
        def _do_mp_search(target_name):
            if target_name in _MP_PARSE_CACHE:
                return _MP_PARSE_CACHE[target_name]
                
            logger.debug(f"  ➜ 本地正则失败，尝试调用 MoviePilot 辅助识别: {target_name}")
            mp_res = mp.recognize_media(target_name, config_manager.APP_CONFIG)
            
            if mp_res:
                logger.info(f"  ➜ [MP辅助识别] 成功命中: {mp_res[2]} (ID:{mp_res[0]})")
                _MP_PARSE_CACHE[target_name] = mp_res
                return mp_res
            
            _MP_PARSE_CACHE[target_name] = None
            return None

        # 优先尝试主目录
        res = _do_mp_search(target_mp_name)
        if res: return res
        
        # 如果主目录失败，且当前是文件，尝试解析文件名
        if main_dir_name and not is_same_name:
            res_file = _do_mp_search(filename)
            if res_file: return res_file

    # =================================================================
    # ★ 第四步：AI 辅助识别 (终极兜底 + 记忆体优化)
    # =================================================================
    if use_ai and ai_translator:
        target_ai_name = main_dir_name if main_dir_name else filename
        
        def _do_ai_search(target_name):
            # 1. 查 AI 记忆体
            if target_name in _AI_PARSE_CACHE:
                ai_result = _AI_PARSE_CACHE[target_name]
            else:
                logger.info(f"  🤖 常规识别失败，消耗 Token 请求 AI 解析: {target_name}")
                try:
                    ai_result = ai_translator.parse_media_filename(target_name)
                    _AI_PARSE_CACHE[target_name] = ai_result # 写入记忆体
                except Exception as e:
                    logger.error(f"  ➜ AI 解析出错: {e}")
                    return None

            # 2. 查 TMDb 记忆体
            if ai_result and ai_result.get('title'):
                ai_title = ai_result.get('title')
                ai_year = ai_result.get('year')
                ai_type = forced_media_type or ai_result.get('type') or media_type
                
                if api_key:
                    search_key = f"AI_{ai_title}_{ai_year}_{ai_type}"
                    if search_key in _TMDB_SEARCH_CACHE:
                        results = _TMDB_SEARCH_CACHE[search_key]
                    else:
                        results = tmdb.search_media(query=ai_title, api_key=api_key, item_type=ai_type, year=ai_year)
                        _TMDB_SEARCH_CACHE[search_key] = results

                    if results and len(results) > 0:
                        best = results[0]
                        # ★★★ 核心修复：精准匹配 ★★★
                        ai_title_lower = ai_title.lower()
                        ai_title_parts = [p for p in ai_title_lower.split() if p]
                        
                        for res in results:
                            res_title = (res.get('title') or res.get('name') or '').lower()
                            res_orig = (res.get('original_title') or res.get('original_name') or '').lower()
                            
                            if ai_title_lower == res_title or ai_title_lower == res_orig:
                                best = res
                                break
                                
                            part_match = False
                            for part in ai_title_parts:
                                if part == res_title or part == res_orig:
                                    best = res
                                    part_match = True
                                    break
                            if part_match:
                                break
                                
                        return str(best['id']), ai_type, (best.get('title') or best.get('name'))
                    else:
                        logger.debug(f"  🤖 AI 提取了标题 '{ai_title}'，但在 TMDb 未搜索到结果。")
            return None

        # 优先尝试主目录
        res = _do_ai_search(target_ai_name)
        if res: return res
        
        # 如果主目录彻底没救了，且当前是文件，才尝试解析文件名
        if main_dir_name and not is_same_name:
            res_file = _do_ai_search(filename)
            if res_file: return res_file

    return None, None, None

# ======================================================================
# ★★★ Webhook 深度删除缓冲队列 (实现并发删除请求的批量合并) ★★★
# ======================================================================
class WebhookDeleteBuffer:
    _lock = threading.Lock()
    _pickcodes = set()
    _timer = None

    @classmethod
    def add(cls, pickcodes):
        if not pickcodes: return
        with cls._lock:
            cls._pickcodes.update(pickcodes)
            
            # 如果有新任务进来，重置定时器
            if cls._timer is not None:
                cls._timer.kill()
            
            from gevent import spawn_later
            # 延迟 3 秒，足以收集一键去重/批量删除瞬间发来的所有 Webhook
            cls._timer = spawn_later(3.0, cls._execute_all)

    @classmethod
    def _execute_all(cls):
        with cls._lock:
            pickcodes = list(cls._pickcodes)
            cls._pickcodes.clear()
            cls._timer = None

        if not pickcodes: return
        
        from gevent import spawn
        spawn(cls._process_batch, pickcodes)

    @classmethod
    def _process_batch(cls, pickcodes):
        client = P115Service.get_client()
        if not client: return

        try:
            # 1. 获取免死金牌名单 (绝对不能删的根目录)
            config = get_config()
            protected_cids = {'0'}
            media_root = config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID)
            if media_root: protected_cids.add(str(media_root))
            save_path = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
            if save_path: protected_cids.add(str(save_path))

            raw_rules = settings_db.get_setting('p115_sorting_rules')
            if raw_rules:
                rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
                for rule in rules:
                    if rule.get('cid'): protected_cids.add(str(rule['cid']))

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # =================================================================
                    # 第一步：通过 PC 码从本地缓存锁定初始文件 (FID) 和 父目录 (PID)
                    # =================================================================
                    cursor.execute("SELECT id, parent_id FROM p115_filesystem_cache WHERE pick_code = ANY(%s)", (list(pickcodes),))
                    initial_files = cursor.fetchall()

                    if not initial_files:
                        logger.warning(f"  ➜ [深度删除] 本地缓存未找到对应 PC 码的文件，无法执行本地推导，任务终止。")
                        return

                    deleted_nodes = set()       # 记录所有被判死刑的节点 (文件 + 变空的目录)
                    nodes_to_check = set()      # 待检查是否变空的父目录
                    node_parent_map = {}        # 缓存节点关系 (id -> parent_id)，用于最后提炼顶级节点

                    for row in initial_files:
                        fid = str(row['id'])
                        pid = str(row['parent_id'])
                        deleted_nodes.add(fid)
                        node_parent_map[fid] = pid
                        if pid and pid not in protected_cids:
                            nodes_to_check.add(pid)

                    # =================================================================
                    # 第二步：自下而上溯源，本地计算空目录 (季目录 -> 剧目录)
                    # =================================================================
                    while nodes_to_check:
                        current_pid = nodes_to_check.pop()
                        if current_pid in protected_cids:
                            continue

                        # 查当前目录下的所有子节点
                        cursor.execute("SELECT id FROM p115_filesystem_cache WHERE parent_id = %s", (current_pid,))
                        children = {str(r['id']) for r in cursor.fetchall()}

                        # ★ 核心逻辑：如果该目录下的所有子节点都在死刑名单里，说明该目录将被掏空！
                        if children and children.issubset(deleted_nodes):
                            deleted_nodes.add(current_pid) # 目录本身加入死刑名单
                            
                            # 查当前目录的父目录，继续向上溯源 (比如季目录空了，继续查剧目录)
                            cursor.execute("SELECT parent_id FROM p115_filesystem_cache WHERE id = %s", (current_pid,))
                            parent_row = cursor.fetchone()
                            if parent_row and parent_row['parent_id']:
                                grand_pid = str(parent_row['parent_id'])
                                node_parent_map[current_pid] = grand_pid
                                if grand_pid not in protected_cids:
                                    nodes_to_check.add(grand_pid)

                    # =================================================================
                    # 第三步：提炼最终需要发送给 115 API 的顶级节点
                    # =================================================================
                    final_api_ids = []
                    for node in deleted_nodes:
                        parent_id = node_parent_map.get(node)
                        # 如果缓存 map 里没有，去库里查一下兜底
                        if not parent_id:
                            cursor.execute("SELECT parent_id FROM p115_filesystem_cache WHERE id = %s", (node,))
                            p_row = cursor.fetchone()
                            parent_id = str(p_row['parent_id']) if p_row else None

                        # ★ 核心优化：如果一个节点的父节点也在死刑名单里，说明它会被连锅端，不需要单独发 API！
                        if parent_id not in deleted_nodes:
                            final_api_ids.append(node)

                    # =================================================================
                    # 第四步：执行唯一一次 115 API 删除调用
                    # =================================================================
                    if final_api_ids:
                        logger.info(f"  ➜ [深度删除] 本地推导完毕！向 115 发送批量删除指令 (共 {len(final_api_ids)} 个顶级节点)...")
                        resp = client.fs_delete(final_api_ids)
                        
                        if resp.get('state'):
                            logger.info(f"  ➜ [深度删除] 115 网盘文件/空目录物理销毁成功！")
                        else:
                            logger.error(f"  ➜ [深度删除] 115 API 删除失败: {resp}")
                            return # API 失败则不清理本地库，保持一致性

                    # =================================================================
                    # 第五步：清理本地数据库记录 (缓存表 + 整理记录表)
                    # =================================================================
                    if deleted_nodes:
                        # 1. 清理目录树缓存
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(deleted_nodes),))
                        deleted_cache_count = cursor.rowcount

                        # 2. 清理整理记录
                        cursor.execute("DELETE FROM p115_organize_records WHERE pick_code = ANY(%s)", (list(pickcodes),))
                        deleted_record_count = cursor.rowcount

                        conn.commit()
                        logger.info(f"  ➜ [深度删除] 本地数据清理完毕: 缓存表移除 {deleted_cache_count} 条, 记录表移除 {deleted_record_count} 条。")

        except Exception as e:
            logger.error(f"  ➜ [深度删除] 执行异常: {e}", exc_info=True)

def delete_115_files_by_webhook(item_path, pickcodes):
    """
    【V6 终极缓冲版】接收神医 Webhook 传来的提取码，加入缓冲队列。
    """
    if not pickcodes: return
    WebhookDeleteBuffer.add(pickcodes)

# ======================================================================
# ★★★ 手动纠错缓冲队列 (实现批量重组与一次性刷新) ★★★
# ======================================================================
class ManualCorrectTaskQueue:
    _lock = threading.Lock()
    _tasks = {}  # 结构: {(tmdb_id, media_type, target_cid, season_num): [record_id1, record_id2, ...]}
    _timer = None

    @classmethod
    def add(cls, record_id, tmdb_id, media_type, target_cid, season_num):
        with cls._lock:
            key = (tmdb_id, media_type, target_cid, season_num)
            if key not in cls._tasks:
                cls._tasks[key] = []
            cls._tasks[key].append(record_id)

            if cls._timer is not None:
                cls._timer.kill()
            from gevent import spawn_later
            # 延迟 2 秒，收集前端并发发来的所有同批次请求
            cls._timer = spawn_later(2.0, cls._execute_all)

    @classmethod
    def _execute_all(cls):
        with cls._lock:
            tasks = cls._tasks.copy()
            cls._tasks.clear()
            cls._timer = None

        from gevent import spawn
        for key, record_ids in tasks.items():
            spawn(cls._process_batch, key, record_ids)

    @classmethod
    def _process_batch(cls, key, record_ids):
        tmdb_id, media_type, target_cid, season_num = key
        try:
            _batch_manual_correct(record_ids, tmdb_id, media_type, target_cid, season_num)
        except Exception as e:
            logger.error(f"  ➜ 批量重组失败: {e}", exc_info=True)


def manual_correct_organize_record(record_id, tmdb_id, media_type, target_cid, season_num=None):
    """手动纠错入口：将任务加入缓冲队列，实现批量重组"""
    ManualCorrectTaskQueue.add(record_id, tmdb_id, media_type, target_cid, season_num)
    return True


def _batch_manual_correct(record_ids, tmdb_id, media_type, target_cid, season_num=None):
    """真正的批量执行逻辑"""
    client = P115Service.get_client()
    if not client: return

    # 1. 批量获取数据库记录
    records = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, file_id, original_name FROM p115_organize_records WHERE id = ANY(%s)", (list(record_ids),))
                records = cursor.fetchall()
    except Exception as e:
        logger.error(f"数据库查询失败: {e}")
        return

    if not records: return

    # 2. 批量获取旧缓存
    old_caches = {}
    file_ids = [str(r['file_id']) for r in records]
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, parent_id, pick_code, sha1, local_path FROM p115_filesystem_cache WHERE id = ANY(%s)", (list(file_ids),))
                for row in cursor.fetchall():
                    old_caches[str(row['id'])] = row
    except: pass

    root_items = []
    old_pids = set()
    refresh_target_dirs = set()
    config = get_config()
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)

    for r in records:
        file_id = str(r['file_id'])
        original_name = r['original_name']
        old_cache = old_caches.get(file_id)

        old_pid = None
        pick_code = None
        sha1 = None
        info_data = {}

        # ★ 核心提速：优先使用本地缓存，彻底干掉 1.5 秒/次的 API 延迟！
        if old_cache and old_cache.get('parent_id') and old_cache.get('pick_code'):
            old_pid = old_cache['parent_id']
            pick_code = old_cache['pick_code']
            sha1 = old_cache.get('sha1')
            info_data = {
                'file_id': file_id, 
                'file_name': original_name, 
                'file_category': '1', 
                'parent_id': old_pid, 
                'pick_code': pick_code, 
                'sha1': sha1
            }
        else:
            # 只有当缓存丢失时，才迫不得已去请求 115 API
            info_res = client.fs_get_info(file_id)
            if not info_res or not info_res.get('state') or not info_res.get('data'):
                logger.warning(f"无法在 115 中定位到该文件(ID:{file_id})，可能已被删除。")
                continue
            info_data = info_res['data']
            old_pid = info_data.get('parent_id') or info_data.get('cid')
            pick_code = info_data.get('pick_code')
            sha1 = info_data.get('sha1')

        if old_pid: old_pids.add(str(old_pid))

        root_items.append({
            'fid': file_id,
            'file_id': file_id,
            'fn': original_name,
            'fc': str(info_data.get('file_category', '1')),
            'pid': old_pid,
            'pc': pick_code,
            'pick_code': pick_code,
            'sha1': sha1,
            '_record_id': r['id'],
            '_old_cache': old_cache,
            '_info_data': info_data
        })

        # 收集需要刷新的本地旧目录
        if local_root and old_cache and old_cache.get('local_path'):
            old_file_rel_path = str(old_cache['local_path']).lstrip('\\/')
            old_dir = os.path.abspath(os.path.dirname(os.path.join(local_root, old_file_rel_path)))
            refresh_target_dirs.add(old_dir)

    if not root_items: return

    title = root_items[0]['fn']
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    try:
        import handler.tmdb as tmdb
        if media_type == 'tv': details = tmdb.get_tv_details(tmdb_id, api_key)
        else: details = tmdb.get_movie_details(tmdb_id, api_key)
        if details: title = details.get('title') or details.get('name') or title
    except: pass

    logger.info(f"  ➜ [批量重组] 开始对 {len(root_items)} 个文件执行定向整理 -> ID:{tmdb_id}")

    organizer = SmartOrganizer(client, tmdb_id, media_type, title, None, False)
    organizer.is_manual_correct = True
    if season_num is not None and str(season_num).strip():
        organizer.forced_season = int(season_num)
        logger.info(f"  ➜ [批量重组] 已强制指定季号: Season {organizer.forced_season}")

    # ★ 核心：将列表直接传给 execute，底层会自动打包成一次 115 API 移动请求！
    success = organizer.execute(root_items, target_cid)
    if not success:
        logger.error("执行批量重组失败。")
        return

    # ★ 查找并重组关联字幕 (批量)
    sub_items = []
    for old_pid in old_pids:
        if str(old_pid) == '0': continue
        try:
            sub_res = client.fs_files({'cid': old_pid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            for item in sub_res.get('data', []):
                if str(item.get('fc', '0')) == '1':
                    sub_name = item.get('fn') or item.get('n') or item.get('file_name', '')
                    ext = sub_name.split('.')[-1].lower() if '.' in sub_name else ''
                    if ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']:
                        # 检查是否匹配任何一个视频的基础名
                        for r_item in root_items:
                            v_name = r_item['_info_data'].get('file_name') or r_item['fn']
                            v_base = v_name.rsplit('.', 1)[0] if '.' in v_name else v_name
                            if sub_name.startswith(v_base):
                                sub_items.append(item)
                                break
        except Exception as e:
            logger.warning(f"  ➜ 查找关联字幕失败: {e}")

    if sub_items:
        logger.info(f"  🔤 [批量重组] 发现 {len(sub_items)} 个关联字幕，跟随重组...")
        organizer.execute(sub_items, target_cid)

    # ★ 本地擦屁股：精准删除旧的本地 STRM 和空目录
    if local_root:
        import shutil
        protected_dirs = {os.path.abspath(local_root)}
        for rule in organizer.rules:
            cat_path = rule.get('category_path') or rule.get('dir_name')
            if cat_path:
                protected_dirs.add(os.path.abspath(os.path.join(local_root, cat_path.lstrip('\\/'))))
        protected_dirs.add(os.path.abspath(os.path.join(local_root, "未识别")))

        old_strm_paths_for_emby = [] # ★ 新增：收集旧路径用于极速扫描

        for r_item in root_items:
            old_cache = r_item['_old_cache']
            if not old_cache or not old_cache.get('local_path'): continue

            old_file_rel_path = str(old_cache['local_path']).lstrip('\\/')
            old_strm_rel_path = os.path.splitext(old_file_rel_path)[0] + ".strm"
            old_strm_full_path = os.path.join(local_root, old_strm_rel_path)

            old_strm_paths_for_emby.append(old_strm_full_path) # ★ 收集路径

            if os.path.exists(old_strm_full_path):
                os.remove(old_strm_full_path)
                logger.debug(f"  ➜ 删除本地旧 STRM: {old_strm_full_path}")

            old_mi_full_path = os.path.splitext(old_file_rel_path)[0] + "-mediainfo.json"
            if os.path.exists(old_mi_full_path):
                os.remove(old_mi_full_path)

            old_dir_full_path = os.path.dirname(old_strm_full_path)
            old_base_name = os.path.splitext(os.path.basename(old_file_rel_path))[0]
            if os.path.exists(old_dir_full_path):
                for f in os.listdir(old_dir_full_path):
                    if f.startswith(old_base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup', 'nfo', 'jpg', 'png', 'jpeg', 'bif']:
                        sub_to_del = os.path.join(old_dir_full_path, f)
                        try:
                            os.remove(sub_to_del)
                        except: pass

        # 向上递归清理本地空目录
        for old_dir in list(refresh_target_dirs):
            curr_dir = old_dir
            while curr_dir and curr_dir not in protected_dirs:
                if os.path.exists(curr_dir):
                    has_media = False
                    for root, _, files in os.walk(curr_dir):
                        for f in files:
                            ext = f.split('.')[-1].lower()
                            if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov'}:
                                has_media = True
                                break
                        if has_media: break

                    if not has_media:
                        shutil.rmtree(curr_dir)
                        logger.info(f"  ➜ 本地旧目录已无媒体文件，执行删除: {curr_dir}")
                        curr_dir = os.path.dirname(curr_dir)
                    else:
                        break
                else:
                    break

        # =================================================================
        # ★ 核心优化：调用极速扫描接口，秒级清理 Emby 中的失效旧条目
        # =================================================================
        emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        if emby_url and emby_api_key and old_strm_paths_for_emby:
            from handler import emby
            logger.info(f"  ➜ 正在通知 Emby 极速扫描旧路径以清理失效媒体项...")
            try:
                # 传入 update_type="Deleted"，复用我们刚写的极速向上寻根扫描逻辑
                emby.notify_emby_file_changes(old_strm_paths_for_emby, emby_url, emby_api_key, update_type="Deleted")
            except Exception as e:
                logger.warning(f"  ➜ 通知 Emby 极速扫描旧路径失败: {e}")

    # ★ 网盘擦屁股：直接移交全局垃圾回收器
    old_cids_to_check = set()
    for r_item in root_items:
        info_data = r_item['_info_data']
        if info_data.get('paths'):
            for p in info_data['paths']:
                cid_val = str(p.get('file_id') or p.get('cid', ''))
                if cid_val and cid_val != '0':
                    old_cids_to_check.add(cid_val)
        elif r_item['pid'] and str(r_item['pid']) != '0':
            old_cids_to_check.add(str(r_item['pid']))

    if old_cids_to_check:
        from handler.p115_service import P115DeleteBuffer
        logger.info(f"  ➜ 已将网盘旧目录链条 ({len(old_cids_to_check)}个层级) 加入全局清理队列，稍后执行清理...")
        P115DeleteBuffer.add(fids=[], base_cids=list(old_cids_to_check))

    # ★ 更新记录状态
    try:
        category_name = "未识别"
        for rule in organizer.rules:
            if str(rule.get('cid')) == str(target_cid):
                category_name = rule.get('dir_name', '未识别')
                break
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE p115_organize_records 
                    SET status = 'success', tmdb_id = %s, media_type = %s, target_cid = %s, category_name = %s
                    WHERE id = ANY(%s)
                """, (tmdb_id, media_type, target_cid, category_name, list(record_ids)))
                conn.commit()
    except Exception as e: pass

    logger.info(f"  ➜ [批量重组] {len(root_items)} 个文件处理完成！")
