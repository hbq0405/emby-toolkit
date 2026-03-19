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
                logger.info("  ⚡ [115] 检测到 Token 已被其他线程续期，直接放行。")
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
                
                logger.info(f"  🔄 [115] Token 自动续期成功！有效时长 {hours} 小时。")
                return True
            else:
                logger.error(f"  ❌ Token 续期失败: {resp.get('message')}，可能需要重新扫码")
                return False
        except Exception as e:
            logger.error(f"  ❌ Token 续期请求异常: {e}")
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
        try:
            current_token = self.access_token # 记录当前请求使用的 token
            resp = requests.request(method, url, headers=self.headers, timeout=30, **kwargs).json()
            
            if not resp.get("state") and resp.get("code") in [40140123, 40140124, 40140125, 40140126]:
                logger.warning("  ⚠️ [115] 检测到 Token 已过期，正在触发自动续期...")
                
                # ★ 传入 current_token 进行比对
                if refresh_115_token(current_token):
                    logger.info("  🚀 [115] 续期完成，重新发送刚才失败的请求...")
                    return requests.request(method, url, headers=self.headers, timeout=30, **kwargs).json()
                else:
                    logger.error("  💀 [115] 续期彻底失败，Token 已死亡，请前往 WebUI 重新扫码！")
            
            return resp
        except Exception as e:
            return {"state": False, "error_msg": str(e)}

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
                logger.warning(f"  ⚠️ HTTPS 握手失败，尝试降级为 HTTP 上传... ({e})")
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
                logger.warning(f"  ⚠️ Cookie 客户端初始化失败: {e}")
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


# ======================================================================
# ★★★ 115 服务管理器 (分离管理/播放客户端 + 延迟初始化) ★★★
# ======================================================================
class P115Service:
    """统一管理 OpenAPI 和 Cookie 客户端"""
    _instance = None
    _lock = threading.Lock()
    _rate_limit_lock = threading.Lock() # 专用于 API 流控的锁
    _downurl_lock = threading.Lock() # 直链专用锁
    
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
                    logger.info("  🚀 [115] OpenAPI 客户端已初始化/更新 (整理用)")
                except Exception as e:
                    logger.error(f"  ❌ 115 OpenAPI 客户端初始化失败: {e}")
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
                    logger.info("  🚀 [115] Cookie 客户端已初始化 (播放用)")
                except Exception as e:
                    logger.error(f"  ❌ 115 Cookie 客户端初始化失败: {e}")
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
            logger.info("  🔄 [115] Cookie 客户端已重置，下次请求将重新初始化")

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
                    # 默认 0.5 秒
                    interval = float(get_config().get(constants.CONFIG_OPTION_115_INTERVAL, 0.5))
                except (ValueError, TypeError):
                    interval = 0.5
                
                # 将 sleep 放回锁内。
                # 现在改为公平阻塞：谁拿到锁，谁就等够间隔再释放。前端请求最多只需等前面一个请求的 0.5 秒。
                with P115Service._rate_limit_lock:
                    current_time = time.time()
                    elapsed = current_time - P115Service._last_request_time
                    if elapsed < interval:
                        time.sleep(interval - elapsed)
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
            
            def upload_file_stream(self, file_stream, file_name, target_cid):
                self._check_openapi()
                self._rate_limit() 
                return self._openapi.upload_file_stream(file_stream, file_name, target_cid)

            def download_url(self, pick_code, user_agent=None):
                if not self._cookie:
                    raise Exception("未配置 115 Cookie，无法获取播放直链")
                
                with P115Service._downurl_lock:
                    # ★ 专门针对 downurl 的严格流控 (最少 1.5 秒)
                    current_time = time.time()
                    elapsed = current_time - P115Service._last_downurl_time
                    if elapsed < 1.5:
                        time.sleep(1.5 - elapsed)
                    
                    try:
                        res = self._cookie.download_url(pick_code, user_agent)
                        P115Service._last_downurl_time = time.time()
                        return res
                    except Exception as e:
                        err_str = str(e)
                        # ★ 如果触发 405 风控，强制熔断 10 秒
                        if '405' in err_str or 'Method Not Allowed' in err_str:
                            logger.error("  🛑 [熔断] 获取直链触发 115 WAF 风控 (405)，强制休眠 10 秒...")
                            P115Service._last_downurl_time = time.time() + 10
                        else:
                            P115Service._last_downurl_time = time.time()
                        raise e

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
            logger.error(f"  ❌ 更新 local_path 失败: {e}")

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
            logger.error(f"  ❌ 读取 115 DB 缓存失败: {e}")
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
            logger.error(f"  ❌ 写入 115 DB 缓存失败: {e}")

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
            logger.error(f"  ❌ 查询文件缓存失败: {e}")
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
            logger.error(f"  ❌ 清理 115 DB 缓存失败: {e}")

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
            logger.error(f"  ❌ 写入 115 文件缓存失败: {e}")

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
            logger.error(f"  ❌ 清理 115 文件缓存失败: {e}")

# ======================================================================
# ★★★ 115 整理记录 DB 管理器 ★★★
# ======================================================================
class P115RecordManager:
    @staticmethod
    def add_or_update_record(file_id, original_name, status, tmdb_id=None, media_type=None, target_cid=None, category_name=None, renamed_name=None, is_center_cached=False, pick_code=None):
        """添加或更新整理记录（基于 file_id 和 pick_code 唯一约束，智能继承原名）"""
        if not file_id or not original_name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # ★ 核心逻辑 1：如果提供了 PC 码，先查前世今生
                    if pick_code:
                        cursor.execute("SELECT file_id, original_name FROM p115_organize_records WHERE pick_code = %s", (pick_code,))
                        row = cursor.fetchone()
                        if row:
                            old_file_id = row['file_id']
                            # 强制继承最开始的原始文件名！
                            original_name = row['original_name'] 
                            
                            # 如果 file_id 变了 (网盘内移动/复制导致)，删掉旧记录，给新记录腾出 PC 码的唯一约束位置
                            if str(old_file_id) != str(file_id):
                                cursor.execute("DELETE FROM p115_organize_records WHERE file_id = %s", (old_file_id,))

                    # ★ 核心逻辑 2：执行插入或更新
                    cursor.execute("""
                        INSERT INTO p115_organize_records 
                        (file_id, pick_code, original_name, status, tmdb_id, media_type, target_cid, category_name, renamed_name, processed_at, is_center_cached)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
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
                            is_center_cached = p115_organize_records.is_center_cached OR EXCLUDED.is_center_cached
                    """, (str(file_id), pick_code, str(original_name), str(status), str(tmdb_id) if tmdb_id else None, 
                          str(media_type) if media_type else None, str(target_cid) if target_cid else None, 
                          str(category_name) if category_name else None, str(renamed_name) if renamed_name else None, bool(is_center_cached)))
                    conn.commit()
        except Exception as e:
            logger.error(f"  ❌ 写入 115 整理记录失败: {e}")

# ======================================================================
# ★★★ 115 全局批量删除缓冲队列 (防流控 + 绝对防御版) ★★★
# ======================================================================
class P115DeleteBuffer:
    _lock = threading.Lock()
    _fids_to_delete = set()
    _cids_to_check = set()
    _timer = None
    _last_add_time = 0  # ★ 新增：记录最后一次添加任务的时间

    @classmethod
    def add(cls, fids, base_cids=None):
        with cls._lock:
            if fids:
                cls._fids_to_delete.update(fids)
            if base_cids:
                if isinstance(base_cids, (list, set)):
                    cls._cids_to_check.update(base_cids)
                else:
                    cls._cids_to_check.add(base_cids)

            # ★ 核心防抖：每次有新文件整理完，刷新倒计时
            cls._last_add_time = time.time()
            if cls._timer is None:
                cls._timer = spawn_later(5.0, cls._check_and_flush)

    @classmethod
    def _check_and_flush(cls):
        with cls._lock:
            now = time.time()
            # ★ 智能防抖：如果距离最后一次整理还不到 10 秒，说明大部队还在干活，继续等！
            # 这能完美解决 115 后端数据同步延迟导致的“假装非空”问题
            if now - cls._last_add_time < 10.0:
                cls._timer = spawn_later(10.0 - (now - cls._last_add_time), cls._check_and_flush)
                return
            
            fids = list(cls._fids_to_delete)
            cids = list(cls._cids_to_check)
            cls._fids_to_delete.clear()
            cls._cids_to_check.clear()
            cls._timer = None

        if not fids and not cids:
            return

        client = P115Service.get_client()
        if not client: return

        def _safe_batch_delete(ids, is_dir=False):
            if not ids: return []
            item_type = "目录" if is_dir else "文件"
            max_retries = 3
            
            for attempt in range(max_retries):
                resp = client.fs_delete(ids)
                if resp.get('state'):
                    return ids
                
                # ★ 流控熔断机制
                if resp.get('code') in [770004, 990001]:
                    logger.error(f"  🛑 [触发流控] 115 API 提示达到访问上限 ({resp.get('code')})，立即终止本次删除任务，保护账号！")
                    return [] 

                logger.error(f"  ❌ [批量销毁] 115 删除{item_type}失败 (第 {attempt + 1}/{max_retries} 次): {resp}")
                if attempt < max_retries - 1:
                    time.sleep(3)
            
            # ★ 核心修改：重试3次全失败后，直接放弃，不再降级为逐个删除！
            logger.warning(f"  ⚠️ [批量销毁] 批量删除彻底失败，放弃本次清理，等待下次任务回收或手动删除。")
            return []

        # 1. 删除文件
        if fids:
            logger.info(f"  💥 [批量销毁] 缓冲期结束，正在删除 {len(fids)} 个文件...")
            success_fids = _safe_batch_delete(fids, is_dir=False)
            if success_fids:
                P115CacheManager.delete_files(success_fids)

        # 2. 获取免死金牌名单
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
                # ★ 增加重试机制，防止网络波动导致误判为非空
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
                        return # 成功则退出重试
                    except Exception as e:
                        if attempt == 2:
                            media_count += 999 # 彻底失败才假装有文件
                        time.sleep(1)

            count_media(cid)
            if media_count == 0:
                empty_cids_to_delete.append(cid)
                logger.info(f"  🗑️ 判定为空目录，加入待清理队列: CID {cid}")

        # 4. 批量删除空目录
        if empty_cids_to_delete:
            logger.info(f"  💥 [批量清理] 正在向 115 发送批量删除空目录指令 ({len(empty_cids_to_delete)} 个)...")
            success_cids = _safe_batch_delete(empty_cids_to_delete, is_dir=True)
            if success_cids:
                for cid in success_cids:
                    P115CacheManager.delete_cid(cid)
                logger.info(f"  🧹 [批量清理] 成功删除 {len(success_cids)} 个空目录。")

    @classmethod
    def flush(cls):
        """兼容老接口调用"""
        cls._check_and_flush()

def get_config():
    return config_manager.APP_CONFIG

class SmartOrganizer:
    def __init__(self, client, tmdb_id, media_type, original_title, ai_translator=None, use_ai=False, season_num=None):
        self.client = client
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_title = original_title
        self.ai_translator = ai_translator
        self.use_ai = use_ai
        self.season_num = season_num 
        self.api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)

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
                    logger.error(f"  ❌ 解析 115 分类规则失败: {e}")
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

            # 补充标题日期供重命名
            current_title = raw_details.get('title') or raw_details.get('name')
            
            # ★★★ 新增：如果标题不是中文，尝试从别名中寻找中文名 ★★★
            if current_title and not utils.contains_chinese(current_title):
                chinese_alias = None
                alt_titles_data = raw_details.get("alternative_titles", {})
                alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                
                for alt in alt_list:
                    alt_title = alt.get("title", "")
                    if utils.contains_chinese(alt_title):
                        chinese_alias = alt_title
                        iso_country = alt.get("iso_3166_1", "").upper()
                        if iso_country in ["CN", "TW", "HK", "SG"]:
                            break # 找到最正宗的，直接跳出
                
                if chinese_alias:
                    logger.info(f"  ➜ [115整理] 发现 TMDb 官方中文别名: '{current_title}' -> '{chinese_alias}'")
                    current_title = chinese_alias

            data['title'] = current_title
            data['original_title'] = raw_details.get('original_title') or raw_details.get('original_name') or data['title']
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

            _TMDB_METADATA_CACHE[cache_key] = data # 写入缓存

            return data

        except Exception as e:
            logger.warning(f"  ⚠️ [整理] 获取原始元数据失败: {e}", exc_info=True)
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

        # ★★★ 核心优化：长寿剧大迁徙终结者 ★★★
        # 追剧状态也是硬性分类
        if rule.get('watching_status') == 'watching' and self.media_type == 'tv':
            try:
                from database.connection import get_db_connection
                
                # 1. 先查这部剧是不是在追剧列表中
                is_series_watching = False
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT watching_status FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Series'", (str(self.tmdb_id),))
                        row = cursor.fetchone()
                        if row and row['watching_status'] in ['Watching', 'Paused', 'Pending']:
                            is_series_watching = True
                
                if not is_series_watching:
                    return False # 剧集本身都没在追，直接不匹配
                
                # 2. 如果剧集在追，进一步判断当前处理的【季】是不是活跃季！
                # 如果我们知道当前正在处理哪一季 (self.season_num)
                if self.season_num is not None:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            # 查一下这个季在数据库里是不是活跃的
                            cursor.execute("""
                                SELECT watching_status 
                                FROM media_metadata 
                                WHERE parent_series_tmdb_id = %s AND season_number = %s AND item_type = 'Season'
                            """, (str(self.tmdb_id), self.season_num))
                            s_row = cursor.fetchone()
                            
                            # 如果这个季的状态是 NONE (非活跃)，说明它是老季！
                            # 老季绝对不能进入连载目录，直接返回 False！
                            if not s_row or s_row['watching_status'] == 'NONE':
                                logger.debug(f"  🛡️ [防迁徙保护] S{self.season_num} 是已完结的老季，拒绝进入连载目录！")
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

    def get_target_cid(self, ignore_memory=False):
        """获取目标 CID：优先查历史整理记录（记忆手动纠错），其次遍历规则"""
        # ★★★ 1. 查历史记录 (记忆功能) ★★★
        if not ignore_memory:
            try:
                from database.connection import get_db_connection
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 查找该 tmdb_id 最近一次成功的整理记录
                        cursor.execute("""
                            SELECT target_cid, category_name 
                            FROM p115_organize_records 
                            WHERE tmdb_id = %s AND status = 'success' 
                            ORDER BY processed_at DESC LIMIT 1
                        """, (str(self.tmdb_id),))
                        row = cursor.fetchone()
                        if row and row['target_cid']:
                            logger.info(f"  🧠 [记忆体] 发现该媒体曾被整理过，沿用历史分类: {row['category_name']} (CID: {row['target_cid']})")
                            return row['target_cid']
            except Exception as e:
                logger.warning(f"  ⚠️ 查询历史整理记录失败: {e}")

        # 2. 遍历规则 (原有逻辑)
        for rule in self.rules:
            if not rule.get('enabled', True): continue
            if self._match_rule(rule):
                logger.info(f"  🎯 [115] 命中规则: {rule.get('name')} -> 目录: {rule.get('dir_name')}")
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
        if re.search(r'[HX]265|HEVC', name_upper): codec = 'HEVC'
        elif re.search(r'[HX]264|AVC', name_upper): codec = 'AVC'
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

    def _fetch_and_parse_mediainfo(self, sha1, guessed_info=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None):
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

        # 3. 兜底：尝试查 P115Center 中心服务器 (单次查询)
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
            except Exception:
                pass

        if not raw_json: return {}, False

        # 3. 开始解析 Emby 的真实数据
        info = {}
        try:
            if isinstance(raw_json, list) and len(raw_json) > 0:
                source_info = raw_json[0].get("MediaSourceInfo", raw_json[0])
            else:
                source_info = raw_json

            streams = source_info.get("MediaStreams", [])
            video_stream = next((s for s in streams if s.get("Type") == "Video"), None)
            audio_streams = [s for s in streams if s.get("Type") == "Audio"]

            if video_stream:
                w = video_stream.get("Width", 0)
                if w >= 3800: info['resolution'] = '2160p'
                elif w >= 1900: info['resolution'] = '1080p'
                elif w >= 1200: info['resolution'] = '720p'

                codec_raw = video_stream.get("Codec", "").lower()
                # ★ 核心修改：统一映射为商业名 HEVC 和 AVC
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

            if audio_streams:
                audio_tags = []
                
                # ★ 核心修改：音轨数独立赋值给 audio_count
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
            logger.warning(f"  ⚠️ 解析真实媒体信息失败: {e}")

        # ★★★ 神医赋能日志转移到这里，并区分数据源 ★★★
        if guessed_info is not None and info:
            corrected_items = []
            for k, v in info.items():
                if v and guessed_info.get(k) != v:
                    corrected_items.append(f"{k}: '{guessed_info.get(k, '空')}' -> '{v}'")
            
            if corrected_items:
                logger.info(f"  ✨ [智能重命名] 成功利用 {data_source} 补全/纠错文件参数: {', '.join(corrected_items)}")

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
            elif block == 'title_en': val = original_title or self.details.get('original_title') or self.original_title
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

    def _rename_file_node(self, file_node, new_base_name, year=None, is_tv=False, original_title=None, pre_fetched_mediainfo=None, local_pre_fetched_mediainfo=None):
        original_name = file_node.get('fn') or file_node.get('n') or file_node.get('file_name', '')
        if '.' not in original_name: return original_name, None, False

        parts = original_name.rsplit('.', 1)
        name_body = parts[0]
        ext = parts[1].lower()

        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']
        lang_suffix = ""
        if is_sub:
            lang_keywords = ['zh', 'cn', 'tw', 'hk', 'en', 'jp', 'kr', 'chs', 'cht', 'eng', 'jpn', 'kor', 'fre', 'spa', 'default', 'forced', 'tc', 'sc']
            sub_parts = name_body.split('.')
            if len(sub_parts) > 1 and (sub_parts[-1].lower() in lang_keywords or '-' in sub_parts[-1].lower()):
                lang_suffix = f".{sub_parts[-1]}"
            if not lang_suffix:
                match = re.search(r'(?:\.|-|_|\s)(chs|cht|zh-cn|zh-tw|eng|jpn|kor|tc|sc)(?:\.|-|_|$)', name_body, re.IGNORECASE)
                if match: lang_suffix = f".{match.group(1)}"

        cfg = self.rename_config
        
        # 提取视频信息字典 (基于文件名的猜测)
        search_name = original_name
        if is_sub and lang_suffix and name_body.endswith(lang_suffix):
            search_name = f"{name_body[:-len(lang_suffix)]}.mkv"
        video_info = self._extract_video_info(search_name)

        # ★★★ 神医降维打击：基于 SHA1 获取真实参数并覆盖猜测 ★★★
        enable_smart_rename = cfg.get('enable_smart_rename', False)
        is_center_cached = False
        
        if not is_sub and enable_smart_rename:
            sha1 = file_node.get('sha1') or file_node.get('sha')
            if sha1:
                # ★ 将预获取的两个字典都传进去
                real_info, is_center_cached = self._fetch_and_parse_mediainfo(sha1, video_info, pre_fetched_mediainfo, local_pre_fetched_mediainfo)
                if real_info:
                    for k, v in real_info.items():
                        if v: video_info[k] = v
                    
        # 解析季集号
        # ★ 优先使用 Webhook 强塞进来的精准数据
        season_num = file_node.get('_forced_season')
        episode_num = file_node.get('_forced_episode')
        
        if is_tv and (season_num is None or episode_num is None):
            # 1. 标准特征匹配 (S01E01, EP01, 第1集)
            pattern = r'(?:^|[ \.\-\_\[\(])(?:s|S)(\d{1,4})[ \.\-]*?(?:e|E|p|P)(\d{1,4})\b|(?:^|[ \.\-\_\[\(])(?:ep|e|episode)[ \.\-]*?(\d{1,4})\b|第(\d{1,4})[集话]'
            match = re.search(pattern, original_name, re.IGNORECASE)
            if match:
                s = match.group(1)
                e = match.group(2)
                ep_only = match.group(3)
                zh_ep = match.group(4)
                if season_num is None:
                    season_num = int(s) if s else 1
                if episode_num is None:
                    episode_num = int(e) if e else (int(ep_only) if ep_only else int(zh_ep))
            else:
                # 2. ★ 纯数字兜底 (绝对安全：因为外层有 if is_tv 保护，绝不会把电影当成剧集)
                name_without_ext = original_name.rsplit('.', 1)[0]
                
                # 策略A：文件名就是纯数字 (如 "01.mp4")
                if name_without_ext.isdigit():
                    if episode_num is None: episode_num = int(name_without_ext)
                else:
                    # 策略B：剔除年份、分辨率等干扰项后，寻找独立的数字
                    clean_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', name_without_ext)
                    
                    # 优先找末尾的数字 (如 "白夜追凶 - 02")
                    end_match = re.search(r'(?:^|[ \.\-\_\[\(])(\d{1,4})(?:[\]\)]|\s*)$', clean_name)
                    if end_match:
                        if episode_num is None: episode_num = int(end_match.group(1))
                    else:
                        # 找中间被明显分隔的数字 (如 "白夜追凶 02 1080p")
                        mid_match = re.search(r'(?:^|[ \-\_\[\(])(\d{1,4})(?:[ \.\-\_\]\)]|$)', clean_name)
                        if mid_match:
                            if episode_num is None: episode_num = int(mid_match.group(1))
                
                if season_num is None:
                    season_num = 1

        if hasattr(self, 'forced_season') and self.forced_season is not None:
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

        new_name = f"{core_name}{lang_suffix}.{ext}"
        
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

        # ★ 返回值增加 s_name
        return new_name, season_num, s_name, is_center_cached

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
            logger.warning(f"  ⚠️ 扫描目录出错 (CID: {cid}): {e}")
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
    
    def _execute_collection_breakdown(self, root_item, collection_movies):
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
            
            for sub_item in sub_items:
                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name')
                sub_id = sub_item.get('fid') or sub_item.get('file_id')
                
                # 1. 优先看子项自己有没有带 ID
                tmdb_id, sub_type, sub_title = _identify_media_enhanced(
                    sub_name, 
                    ai_translator=self.ai_translator, 
                    use_ai=self.use_ai
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
            
            # ★ 核心修改：遍历分组，批量执行
            for (tmdb_id, sub_type, sub_title), items in grouped_sub_items.items():
                logger.info(f"    ├─ 准备批量整理合集子项: {sub_title} -> ID:{tmdb_id} (共 {len(items)} 个文件)")
                try:
                    organizer = SmartOrganizer(self.client, tmdb_id, sub_type, sub_title, self.ai_translator, self.use_ai)
                    target_cid_for_sub = organizer.get_target_cid()
                    if organizer.execute(items, target_cid_for_sub):
                        processed_count += len(items)
                except Exception as e:
                    logger.error(f"    ❌ 批量处理子项失败: {e}")
            
            # ★ 核心修改：批量移入未识别
            if unidentified_sub_fids and unidentified_cid:
                logger.warning(f"    ⚠️ 无法识别合集子项 {len(unidentified_sub_fids)} 个，批量移入未识别。")
                try: 
                    self.client.fs_move(unidentified_sub_fids, unidentified_cid)
                except Exception as e: 
                    logger.error(f"    ❌ 移入未识别失败: {e}")
            
            # 绝对安全防御：禁止直接删除，交由垃圾回收器检查是否为空
            from handler.p115_service import P115DeleteBuffer
            P115DeleteBuffer.add(fids=[], base_cids=[source_root_id])
            logger.info(f"  ⏳ [清理空目录] 已将拆解完毕的合集包交由垃圾回收器检查: {root_name}")
            
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"  ❌ 拆解合集包失败: {e}")
            return False

    def execute(self, root_item_or_items, target_cid, progress_callback=None):
        # ★ 新增：判断传入的是单个文件还是批量文件列表
        is_batch = isinstance(root_item_or_items, list)
        
        if is_batch:
            if not root_item_or_items: return True # 防御性检查：空列表直接返回
            root_item = root_item_or_items[0]      # ★ 修复报错：取第一个元素作为代表项，供后续提取父目录ID使用
            root_name = "批量文件"
            source_root_id = root_item.get('pid') or root_item.get('parent_id')
            is_source_file = True
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else source_root_id
        else:
            root_item = root_item_or_items
            # 兼容 OpenAPI 键名
            root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
            source_root_id = root_item.get('fid') or root_item.get('file_id')
            fc_val = root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type')
            is_source_file = str(fc_val) == '1'
            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))

        # =================================================================
        # 1. 拦截合集包 (Collection Breakdown) - 仅限单项传入时触发
        # =================================================================
        if not is_batch and not is_source_file and re.search(r'(合集|部曲|系列|Collection|Pack|Trilogy|Quadrilogy|\d+-\d+)', root_name, re.IGNORECASE):
            logger.info(f"  📦 检测到疑似合集包: {root_name}，正在验证...")
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
                logger.info(f"  📦 确认为官方合集包，包含 {len(collection_movies)} 部电影，启动精确拆解模式...")
            else:
                logger.info(f"  📦 未找到官方合集信息 (可能是民间自制包)，启动基于文件名的暴力拆解模式...")
            return self._execute_collection_breakdown(root_item, collection_movies)

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
        # ★★★ 3. 智能类型纠错嗅探 (Movie -> TV) ★★★
        # =================================================================
        if self.media_type == 'movie' and not getattr(self, 'is_manual_correct', False):
            is_actually_tv = False
            for c in candidates:
                c_name = c.get('fn') or c.get('n') or c.get('file_name', '')
                rel_path = c.get('rel_path', '')
                
                # 1. 相对路径特征 (Season 1)
                if re.search(r'(?:Season\s?\d+|S\d+|第[一二三四五六七八九十\d]+季)', rel_path, re.IGNORECASE):
                    is_actually_tv = True
                    break
                
                # 2. 标准特征 (EP01, S01E01)
                if re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|e|episode)[ \.\-]*\d{1,4}\b|第\d{1,4}[集话]', c_name, re.IGNORECASE):
                    is_actually_tv = True
                    break
                
                # 3. 动漫特征 (剔除干扰后，寻找纯数字序号)
                clean_c_name = re.sub(r'(19|20)\d{2}|1080[pP]?|2160[pP]?|720[pP]?|480[pP]?|4[kK]|264|265|10bit|8bit|5\.1|7\.1|2\.0', '', c_name)
                if re.search(r'(?:-\s*|\[|【)(\d{2,4})(?:\s+|\]|】)', clean_c_name): 
                    is_actually_tv = True
                    break
            
            if is_actually_tv:
                logger.warning(f"  🕵️‍♂️ [智能纠错] 发现文件包含明显的剧集特征(如季目录/EP01)，但当前被错误识别为电影。正在尝试自动纠错...")
                try:
                    # ★ 核心修复：坚决保留原 TMDb ID，只切换类型重新拉取数据！
                    self.media_type = 'tv'
                    
                    # 强制清除旧的电影缓存数据，重新拉取剧集数据
                    cache_key = f"tv_{self.tmdb_id}"
                    if cache_key in _TMDB_METADATA_CACHE:
                        del _TMDB_METADATA_CACHE[cache_key]
                        
                    self.raw_metadata = self._fetch_raw_metadata()
                    
                    # 如果拉取成功（说明这个 ID 确实有对应的剧集数据）
                    if self.raw_metadata and self.raw_metadata.get('title'):
                        self.details = self.raw_metadata
                        logger.info(f"  ✅ [智能纠错] 成功保留原 ID ({self.tmdb_id}) 并切换为剧集: {self.details.get('title')}")
                        
                        target_cid = self.get_target_cid()
                        dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))
                    else:
                        # 只有在原 ID 作为剧集彻底查不到数据时，才迫不得已用名字重新搜索
                        logger.warning(f"  ⚠️ [智能纠错] 原 ID ({self.tmdb_id}) 作为剧集查询失败，尝试用名称重新搜索...")
                        search_title = self.original_title
                        clean_title = re.sub(r'\(\d{4}\)', '', search_title).strip()
                        results = tmdb.search_media(query=clean_title, api_key=self.api_key, item_type='tv')
                        
                        if results and len(results) > 0:
                            new_tmdb_id = str(results[0]['id'])
                            logger.info(f"  ✅ [智能纠错] 成功重新搜索并纠正为剧集: {results[0].get('name')} (ID:{new_tmdb_id})")
                            self.tmdb_id = new_tmdb_id
                            self.raw_metadata = self._fetch_raw_metadata()
                            self.details = self.raw_metadata
                            
                            target_cid = self.get_target_cid()
                            dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))
                        else:
                            logger.warning(f"  ⚠️ [智能纠错] 未能在 TMDb 找到对应的剧集，将强制按剧集格式重命名以防冲突。")
                except Exception as e:
                    logger.error(f"  ❌ [智能纠错] 纠错失败: {e}")

        # =================================================================
        # 4. 计算最终的目录名称和路径 (支持 / 多级目录)
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
        MIN_VIDEO_SIZE = 10 * 1024 * 1024

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

        logger.info(f"  🚀 [115] 开始整理: {root_name} -> {std_root_name}")

        final_home_cid = None
        current_parent_cid = dest_parent_cid
        
        # ★★★ 核心升级：支持 / 分层创建多级目录 ★★★
        dir_parts = [p.strip() for p in std_root_name.split('/') if p.strip()]
        
        for attempt in range(2):
            success_chain = True
            temp_parent_cid = current_parent_cid
            
            # 逐级检查/创建目录
            for part_name in dir_parts:
                part_cid = P115CacheManager.get_cid(temp_parent_cid, part_name)
                
                # 缓存自愈检查
                if part_cid and str(part_cid) == str(source_root_id) and str(temp_parent_cid) != str(root_item.get('pid') or root_item.get('parent_id')):
                    P115CacheManager.delete_cid(part_cid)
                    part_cid = None

                if not part_cid:
                    mk_res = self.client.fs_mkdir(part_name, temp_parent_cid)
                    if mk_res.get('state'):
                        part_cid = mk_res.get('cid')
                        P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                    else:
                        # 模糊查找兜底
                        try:
                            search_res = self.client.fs_files({'cid': temp_parent_cid, 'search_value': part_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                            for item in search_res.get('data', []):
                                if item.get('fn') == part_name and str(item.get('fc', item.get('type'))) == '0':
                                    part_cid = item.get('fid') or item.get('file_id')
                                    P115CacheManager.save_cid(part_cid, temp_parent_cid, part_name)
                                    break
                        except: pass
                
                if part_cid:
                    temp_parent_cid = part_cid
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
            logger.error(f"  ❌ 无法获取或创建目标目录链 (已尝试所有手段)")
            return False
        
        if not candidates: return True

        moved_count = 0
        move_groups = {}
        unrecognized_fids = [] # ★ 终极垃圾桶：收集所有不符合要求的文件
        
        # ★ 新增：用于记录本批次已经生成的目标文件名，防止同名冲突
        seen_new_filenames = set()

        # 批量预查询中心服务器与本地数据库
        pre_fetched_mediainfo = {}
        local_pre_fetched_mediainfo = {} # ★ 新增：本地预获取字典
        
        if cfg.get('enable_smart_rename', False) and not keep_original:
            video_sha1s = []
            for file_item in candidates:
                file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
                if ext in known_video_exts:
                    sha1 = file_item.get('sha1') or file_item.get('sha')
                    if sha1: video_sha1s.append(sha1)
            
            if video_sha1s:
                # 先查本地缓存，剔除已有的，只查缺失的
                local_cached_sha1s = set()
                try:
                    from database.connection import get_db_connection
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            # ★ 核心优化：直接把 json 也查出来放进内存！
                            cursor.execute("SELECT sha1, mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = ANY(%s)", (list(video_sha1s),))
                            for row in cursor.fetchall():
                                local_cached_sha1s.add(row['sha1'])
                                if row['mediainfo_json']:
                                    local_pre_fetched_mediainfo[row['sha1']] = row['mediainfo_json'] if isinstance(row['mediainfo_json'], list) else json.loads(row['mediainfo_json'])
                except Exception: pass
                
                missing_sha1s = list(set(video_sha1s) - local_cached_sha1s)
                if missing_sha1s:
                    logger.info(f"  🌐 [批量查询] 准备向中心服务器查询 {len(missing_sha1s)} 个文件的媒体信息...")
                    try:
                        import extensions
                        processor = extensions.media_processor_instance
                        if processor and getattr(processor, 'p115_center', None):
                            resp = processor.p115_center.download_emby_mediainfo_data(missing_sha1s)
                            if resp:
                                pre_fetched_mediainfo = resp
                                logger.info(f"  ✅ [批量查询] 成功获取 {len(resp)} 个文件的媒体信息。")
                    except Exception as e:
                        logger.warning(f"  ⚠️ [批量查询] 中心服务器查询失败: {e}")

        # 确保 allowed_exts 有兜底，防止用户清空列表导致报错
        if not allowed_exts:
            allowed_exts = known_video_exts | {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}

        # =================================================================
        # ★★★ 内存级目录缓存 ★★★
        # =================================================================
        memory_dir_cache = {}
        
        for file_item in candidates:
            # 兼容 OpenAPI 键名
            fid = file_item.get('fid') or file_item.get('file_id')
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            
            # 1. 扩展名绝对白名单校验 (最高优先级)
            if ext not in allowed_exts:
                logger.debug(f"  🚫 扩展名 .{ext} 不在允许列表中，打入未识别: {file_name}")
                if fid: unrecognized_fids.append(fid)
                if progress_callback: progress_callback()
                continue

            # 2. 垃圾/花絮/样本校验 (仅针对视频)
            if ext in known_video_exts:
                if self._is_junk_file(file_name) or (0 < file_size < MIN_VIDEO_SIZE):
                    logger.debug(f"  🗑️ 判定为花絮或体积过小，打入未识别: {file_name}")
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
                new_filename, season_num, s_name, is_center_cached = self._rename_file_node(
                    file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), original_title=original_title,
                    pre_fetched_mediainfo=pre_fetched_mediainfo,
                    local_pre_fetched_mediainfo=local_pre_fetched_mediainfo 
                )

                # ★ 核心修改：为当前文件单独计算目标主目录 (传入 season_num)
                # 这样，如果是老季，它会返回常规目录；如果是新季，它会返回连载目录！
                temp_organizer = SmartOrganizer(self.client, self.tmdb_id, self.media_type, self.original_title, season_num=season_num)
                temp_organizer.rules = self.rules # 继承规则
                file_target_cid = temp_organizer.get_target_cid(ignore_memory=True) # 忽略记忆体，强制重新计算
                
                # 如果没匹配到新规则，就用原来的 dest_parent_cid 兜底
                if not file_target_cid:
                    file_target_cid = dest_parent_cid
                
                real_target_cid = file_target_cid
                
                # ★ 直接使用返回的 s_name 创建/查找季目录
                if self.media_type == 'tv' and season_num is not None and s_name:
                    cache_key = f"{file_target_cid}_{s_name}"
                    s_cid = memory_dir_cache.get(cache_key) # ★ 优先查内存缓存
                    
                    # ★ 如果缓存里存的是 'FAILED'，说明之前尝试过且失败了，直接跳过，防止 API 风暴
                    if s_cid == 'FAILED':
                        real_target_cid = file_target_cid
                    else:
                        if not s_cid:
                            s_cid = P115CacheManager.get_cid(file_target_cid, s_name)
                        
                        if s_cid:
                            real_target_cid = s_cid
                            memory_dir_cache[cache_key] = s_cid # 顺手存入内存
                        else:
                            s_mk = self.client.fs_mkdir(s_name, file_target_cid)
                            s_cid = s_mk.get('cid') if s_mk.get('state') else None
                            
                            if not s_cid: 
                                try:
                                    s_search = self.client.fs_files({'cid': file_target_cid, 'search_value': s_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                                    for item in s_search.get('data', []):
                                        item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                        item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                                        item_pid = str(item.get('pid') or item.get('parent_id') or item.get('cid'))
                                        
                                        if item_name == s_name and str(item_fc) == '0' and item_pid == str(file_target_cid):
                                            s_cid = item.get('fid') or item.get('file_id')
                                            break
                                except: pass
                            
                            if s_cid:
                                P115CacheManager.save_cid(s_cid, file_target_cid, s_name)
                                memory_dir_cache[cache_key] = s_cid # ★ 写入内存缓存
                                real_target_cid = s_cid
                            else:
                                # ★ 核心防御：如果创建和搜索都失败了，标记为 FAILED，同批次不再重试！
                                memory_dir_cache[cache_key] = 'FAILED'
                                real_target_cid = file_target_cid

            # =================================================================
            # ★★★ 核心修复：严格去重逻辑 (防多版本/洗版残留冲突) ★★★
            # =================================================================
            if new_filename in seen_new_filenames:
                logger.warning(f"  ⚠️ [去重丢弃] 发现重复版本: '{file_name}' -> 目标名 '{new_filename}' 已被占用，当作垃圾打入未识别！")
                if fid: unrecognized_fids.append(fid)
                continue # 直接跳过，绝不重命名，绝不移动，绝不生成 STRM！
            
            # 记录已占用的文件名
            seen_new_filenames.add(new_filename)

            # 暂存入分组字典
            file_item['_new_filename'] = new_filename
            file_item['_season_num'] = season_num
            file_item['_s_name'] = s_name
            file_item['_is_center_cached'] = is_center_cached
            
            if real_target_cid not in move_groups:
                move_groups[real_target_cid] = []
            move_groups[real_target_cid].append(file_item)

        # =================================================================
        # ★★★ 执行批量移动与后续 STRM 生成 ★★★
        # =================================================================
        for batch_target_cid, items in move_groups.items():
            fids = [item.get('fid') or item.get('file_id') for item in items]
            
            # 1. 批量发送移动指令 (一次 API 请求搞定整个目录的文件)
            move_res = self.client.fs_move(fids, batch_target_cid)
            
            if move_res.get('state'):
                # 提取展示用的目录名
                display_target = std_root_name
                if items and items[0].get('_s_name'):
                    display_target = f"{std_root_name} - {items[0]['_s_name']}"
                logger.info(f"  📁 [批量移动] 成功将 {len(fids)} 个文件移动至 -> {display_target}")
                
                # =================================================================
                # ★ 批量同名覆盖与重命名逻辑 (完美解决 (1) 冲突，且最小化 API 请求)
                # =================================================================
                try:
                    # 获取目标目录当前的所有文件 (仅 1 次 API 请求)
                    existing_res = self.client.fs_files({'cid': batch_target_cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    existing_files = existing_res.get('data', [])
                    
                    # 建立 FID -> 当前真实名称 的映射 (找回移动后可能被 115 加上 (1) 的文件)
                    fid_to_current_name = {
                        str(e.get('fid') or e.get('file_id')): (e.get('fn') or e.get('n') or e.get('file_name'))
                        for e in existing_files if str(e.get('fc') or e.get('type')) == '1'
                    }
                    
                    # 建立 名称 -> FID 的映射 (用于寻找占用完美名字的旧文件)
                    existing_name_to_fid = {
                        (e.get('fn') or e.get('n') or e.get('file_name')): str(e.get('fid') or e.get('file_id'))
                        for e in existing_files if str(e.get('fc') or e.get('type')) == '1'
                    }
                    
                    # 收集需要被删除的旧文件 FID (用于批量删除)
                    conflict_fids_to_delete = []
                    # 收集需要被重命名的新文件 (用于逐个重命名)
                    items_to_rename = []
                    
                    for file_item in items:
                        fid = str(file_item.get('fid') or file_item.get('file_id'))
                        new_filename = file_item['_new_filename']
                        current_name_in_115 = fid_to_current_name.get(fid, file_item.get('fn'))
                        
                        if current_name_in_115 != new_filename:
                            # 如果完美名字被别人占用了，且那个人不是我自己
                            if new_filename in existing_name_to_fid:
                                conflict_fid = existing_name_to_fid[new_filename]
                                if conflict_fid != fid:
                                    conflict_fids_to_delete.append(conflict_fid)
                                    # 从字典移除，防止多个文件重名时重复添加同一个 FID
                                    del existing_name_to_fid[new_filename] 
                            
                            items_to_rename.append((fid, current_name_in_115, new_filename))
                    
                    # 执行批量删除 (仅 1 次 API 请求，瞬间秒杀所有旧版文件)
                    if conflict_fids_to_delete:
                        logger.warning(f"  ⚠️ [同名覆盖] 目标目录发现 {len(conflict_fids_to_delete)} 个同名旧文件，正在批量删除以腾出空间...")
                        self.client.fs_delete(conflict_fids_to_delete)
                        P115CacheManager.delete_files(conflict_fids_to_delete)
                    
                    # 执行重命名 (N 次 API 请求，115 不支持批量重命名，这和原来保持一致)
                    for fid, current_name, new_name in items_to_rename:
                        ren_res = self.client.fs_rename((fid, new_name))
                        if ren_res.get('state'):
                            logger.info(f"  ✏️ [重命名] {current_name} -> {new_name}")
                        else:
                            logger.warning(f"  ⚠️ [重命名失败] {current_name} -> {new_name}, 原因: {ren_res.get('error_msg', ren_res)}")
                            
                except Exception as e:
                    logger.error(f"  ❌ [同名覆盖] 处理重命名逻辑失败: {e}")
                
                # 2. 移动成功后，遍历该批次文件，生成 STRM 和记录日志
                for file_item in items:
                    fid = file_item.get('fid') or file_item.get('file_id')
                    file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
                    new_filename = file_item['_new_filename']
                    season_num = file_item['_season_num']
                    s_name = file_item['_s_name']
                    is_center_cached = file_item['_is_center_cached']
                    
                    moved_count += 1
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
                            from handler.p115_service import P115RecordManager
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
                                pick_code=pick_code 
                            )
                        except Exception as e:
                            logger.error(f"  ❌ 记录文件整理日志失败: {e}")

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
                                logger.info(f"  📝 STRM 已生成 -> {strm_filename}")
                                
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
                                                logger.info(f"  ✅ [字幕下载] {new_filename} 下载完成！")
                                        except Exception as e:
                                            logger.error(f"  ❌ 下载字幕失败: {e}")
                            
                        except Exception as e:
                            logger.error(f"  ❌ 生成 STRM 文件失败: {e}", exc_info=True)
                    if progress_callback:
                        progress_callback()
            else:
                err_msg = str(move_res.get('error_msg', move_res))
                logger.error(f"  ❌ [批量移动失败] 目标CID:{batch_target_cid}, 包含 {len(fids)} 个文件, 原因: {err_msg}")
                
                if '不存在' in err_msg or move_res.get('code') in [20004, 70004]:
                    logger.warning(f"  🧹 检测到目标目录在网盘中已不存在，正在清理失效缓存: CID {batch_target_cid}")
                    P115CacheManager.delete_cid(batch_target_cid)
                if progress_callback:
                    for _ in items:
                        progress_callback()

        # =================================================================
        # ★★★ 终极清理：将所有不合规文件移入未识别目录 ★★★
        # =================================================================
        if unrecognized_fids and unidentified_cid:
            logger.info(f"  🗑️ 发现 {len(unrecognized_fids)} 个不合规文件(扩展名不符/花絮/样本)，正在移入未识别目录...")
            # 同样传入列表，防止 115 API 报错
            self.client.fs_move(unrecognized_fids, unidentified_cid)

        # =================================================================
        # ★ 精准收集所有涉及的源目录和父目录，交由垃圾回收器
        # =================================================================
        cids_to_check = set()
        if is_batch:
            for item in root_item_or_items:
                fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                if str(fc_val) == '0': 
                    cids_to_check.add(item.get('fid') or item.get('file_id'))
                    cids_to_check.add(item.get('pid') or item.get('parent_id') or item.get('cid'))
                else: 
                    cids_to_check.add(item.get('pid') or item.get('parent_id') or item.get('cid'))
        else:
            # ★ 核心拦截：如果带有免死金牌，直接跳过收集，彻底切断与垃圾回收器的联系！
            if not root_item.get('_skip_gc'):
                if is_source_file:
                    cids_to_check.add(root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))
                else:
                    cids_to_check.add(source_root_id)
                    cids_to_check.add(root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))
            else:
                logger.info("  🛡️ [MP上传] 单文件跳过源目录垃圾回收检查。")

        if final_home_cid and str(final_home_cid) != '0':
            cids_to_check.add(final_home_cid)
        
        # 过滤掉空的和 '0' (根目录)
        valid_cids_to_check = [str(cid) for cid in cids_to_check if cid and str(cid) != '0']

        if valid_cids_to_check:
            logger.info(f"  ⏳ [清理空目录] 已将 {len(valid_cids_to_check)} 个源目录交由全局垃圾回收器检查清理...")
            from handler.p115_service import P115DeleteBuffer
            P115DeleteBuffer.add(fids=[], base_cids=valid_cids_to_check)

        # --- 整理记录 ---
        if moved_count > 0 or keep_original:
            category_name = "未识别"
            for rule in self.rules:
                if str(rule.get('cid')) == str(target_cid):
                    category_name = rule.get('dir_name', '未识别')
                    break
            
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

def _identify_media_enhanced(filename, main_dir_name=None, has_season_subdirs=False, forced_media_type=None, ai_translator=None, use_ai=False):
    """
    【绝对正确版】识别逻辑：
    1. 先定类型：综合主目录、子目录特征、文件名，判断是 Movie 还是 TV。
    2. 再提 ID：优先从主目录提取 {tmdb=xxx}，其次提取 Title (Year)，最后看文件名。
    3. 定向查询：用确定的类型 + ID/名称 向 TMDb 发起查询。
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
        if has_season_subdirs or re.search(r'(?:^|[ \.\-\_\[\(])(?:s|S)\d{1,4}[ \.\-]*(?:e|E|p|P)\d{1,4}\b|(?:^|[ \.\-\_\[\(])(?:ep|e|episode)[ \.\-]*\d{1,4}\b|第[一二三四五六七八九十\d]+季|Season', combined_text, re.IGNORECASE):
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
    
    # 优先级 1: 从主目录名提取 TMDb ID
    if main_dir_name:
        match_tag_main = re.search(r'\{?tmdb(?:id)?[=\-](\d+)\}?', main_dir_name, re.IGNORECASE)
        if match_tag_main:
            tmdb_id = match_tag_main.group(1)
            clean_name = re.sub(r'\{?tmdb(?:id)?[=\-]\d+\}?', '', main_dir_name, flags=re.IGNORECASE).strip()
            match_title = re.match(r'^(.+?)\s*[\(\[]\d{4}[\)\]]', clean_name)
            fallback_title = match_title.group(1).strip() if match_title else clean_name
            
            # 用锁定的类型去查标题
            official_title = _fetch_title_by_id(tmdb_id, media_type)
            return tmdb_id, media_type, official_title or fallback_title

    # 优先级 2: 从主目录名提取 Title (Year) 进行搜索
    if main_dir_name:
        match_std_main = re.match(r'^(.+?)(?:\s+[\(\[]|\.|\s+)(\d{4})(?:[\)\]]|\.|\s+|$)', main_dir_name)
        if match_std_main:
            name_part = match_std_main.group(1).replace('.', ' ').strip()
            year_part = match_std_main.group(2)
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
                        return str(best['id']), media_type, (best.get('title') or best.get('name'))
            except Exception:
                pass

    # 优先级 3: 兜底 - 从当前文件名提取 TMDb ID 或 Title (Year)
    if not is_same_name:
        match_tag_file = re.search(r'\{?tmdb(?:id)?[=\-](\d+)\}?', filename, re.IGNORECASE)
        if match_tag_file:
            tmdb_id = match_tag_file.group(1)
            clean_name = re.sub(r'\{?tmdb(?:id)?[=\-]\d+\}?', '', filename, flags=re.IGNORECASE).strip()
            match_title = re.match(r'^(.+?)\s*[\(\[]\d{4}[\)\]]', clean_name)
            fallback_title = match_title.group(1).strip() if match_title else clean_name
            
            official_title = _fetch_title_by_id(tmdb_id, media_type)
            return tmdb_id, media_type, official_title or fallback_title

        match_std_file = re.match(r'^(.+?)(?:\s+[\(\[]|\.|\s+)(\d{4})(?:[\)\]]|\.|\s+|$)', filename)
        if match_std_file:
            name_part = match_std_file.group(1).replace('.', ' ').strip()
            year_part = match_std_file.group(2)
            try:
                if api_key:
                    search_key = f"{name_part}_{year_part}_{media_type}"
                    if search_key in _TMDB_SEARCH_CACHE:
                        results = _TMDB_SEARCH_CACHE[search_key]
                    else:
                        results = tmdb.search_media(query=name_part, api_key=api_key, item_type=media_type, year=year_part)
                        _TMDB_SEARCH_CACHE[search_key] = results

                    if results and len(results) > 0:
                        best = results[0]
                        return str(best['id']), media_type, (best.get('title') or best.get('name'))
            except Exception:
                pass

    # =================================================================
    # ★ 第三步：AI 辅助识别 (终极兜底 + 记忆体优化)
    # =================================================================
    if use_ai and ai_translator:
        target_ai_name = main_dir_name if main_dir_name else filename
        
        def _do_ai_search(target_name):
            # 1. 查 AI 记忆体
            if target_name in _AI_PARSE_CACHE:
                ai_result = _AI_PARSE_CACHE[target_name]
                # logger.debug(f"  🤖 [AI缓存命中] 无需消耗 Token: {target_name}")
            else:
                logger.info(f"  🤖 常规识别失败，消耗 Token 请求 AI 解析: {target_name}")
                try:
                    ai_result = ai_translator.parse_media_filename(target_name)
                    _AI_PARSE_CACHE[target_name] = ai_result # 写入记忆体
                except Exception as e:
                    logger.error(f"  ❌ AI 解析出错: {e}")
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
                        return str(best['id']), ai_type, (best.get('title') or best.get('name'))
                    else:
                        logger.debug(f"  🤖 AI 提取了标题 '{ai_title}'，但在 TMDb 未搜索到结果。")
            return None

        # 优先尝试主目录 (如果有 50 个文件，这里只会调 1 次 AI)
        res = _do_ai_search(target_ai_name)
        if res: return res
        
        # 如果主目录彻底没救了，且当前是文件，才尝试解析文件名
        if main_dir_name and not is_same_name:
            res_file = _do_ai_search(filename)
            if res_file: return res_file

    return None, None, None


def task_scan_and_organize_115(processor=None):
    """
    [任务链] 主动扫描 115 待整理目录 (多线程并发极速版)
    """
    logger.info("=== 开始执行 115 待整理目录扫描 (多线程并发模式) ===")

    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)

    update_progress(10, "正在初始化 115 客户端与目录扫描...")

    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    # 通知监控服务进入蓄水池模式
    try:
        from monitor_service import pause_queue_processing, resume_queue_processing
        pause_queue_processing()
    except Exception as e:
        logger.warning(f"  ⚠️ 无法暂停监控队列: {e}")
        resume_queue_processing = lambda: None # 兜底防报错

    config = get_config()
    cid_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
    save_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_NAME, '待整理')
    enable_organize = config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)
    use_ai = config.get(constants.CONFIG_OPTION_AI_RECOGNITION, False)
    ai_translator = processor.ai_translator if processor and hasattr(processor, 'ai_translator') else None

    if not cid_val or str(cid_val) == '0':
        logger.error("  ⚠️ 未配置待整理目录，跳过。")
        return
    if not enable_organize:
        logger.warning("  ⚠️ 未开启智能整理开关，仅扫描不处理。")
        return
        
    current_time = time.time()
    try:
        save_cid = int(cid_val)
        save_name = str(save_val)

        # 1. 准备 '未识别' 目录
        unidentified_cid = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID)
        unidentified_folder_name = config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_NAME, "未识别")
        
        if not unidentified_cid or str(unidentified_cid) == '0':
            unidentified_folder_name = "未识别"
            try:
                search_res = client.fs_files({
                    'cid': save_cid, 'search_value': unidentified_folder_name, 'limit': 1,
                    'record_open_time': 0, 'count_folders': 0
                })
                if search_res.get('data'):
                    for item in search_res['data']:
                        if item.get('fn') == unidentified_folder_name and str(item.get('fc')) == '0':
                            unidentified_cid = item.get('fid')
                            break
            except: pass

            if not unidentified_cid:
                try:
                    mk_res = client.fs_mkdir(unidentified_folder_name, save_cid)
                    if mk_res.get('state'): unidentified_cid = mk_res.get('cid')
                except: pass

        logger.info(f"  🔍 正在扫描主目录: {save_name} ...")
        
        # =================================================================
        # ★★★ 核心重构：多线程并发任务池 (两阶段批量模式) ★★★
        # =================================================================
        processed_count = 0
        moved_to_unidentified = 0
        counter_lock = threading.Lock() # 计数器锁

        # ★ 读取前端配置的并发数，默认 3
        max_workers = int(config.get(constants.CONFIG_OPTION_115_MAX_WORKERS, 3))
        executor = ThreadPoolExecutor(max_workers=max_workers) 
        
        active_tasks = 0
        task_cond = threading.Condition()

        # ★ 用于收集第一阶段识别结果的容器
        grouped_items = {} # 结构: {(tmdb_id, media_type, title): [item1, item2, ...]}
        unidentified_items = []
        group_lock = threading.Lock()

        def submit_task(func, *args):
            """安全提交任务到线程池"""
            nonlocal active_tasks
            with task_cond:
                active_tasks += 1
            executor.submit(task_wrapper, func, *args)

        def task_wrapper(func, *args):
            """任务执行包装器，确保任务计数正确递减"""
            nonlocal active_tasks
            try:
                func(*args)
            except Exception as e:
                logger.error(f"  ❌ 线程执行异常: {e}", exc_info=True)
            finally:
                with task_cond:
                    active_tasks -= 1
                    if active_tasks == 0:
                        task_cond.notify_all() # 所有任务完成，唤醒主线程

        def process_single_item(item, name, is_folder, depth, forced_type, main_dir_name=None, has_season_subdirs=False):
            """单个媒体项的识别逻辑 (仅识别并分组，不执行整理)"""
            item_id = item.get('fid') or item.get('file_id')

            if is_folder and not forced_type:
                try:
                    res = client.fs_files({'cid': item_id, 'limit': 100, 'record_open_time': 0, 'count_folders': 0})
                    for sub_item in res.get('data', []):
                        sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name', '')
                        if re.search(r'(?:Season\s?\d+|S\d{1,4}[ \.\-]*(?:E|P)\d{1,4}|EP?\d{1,4}|第[一二三四五六七八九十\d]+季)', sub_name, re.IGNORECASE):
                            forced_type = 'tv'
                            break
                except Exception:
                    pass

            tmdb_id, media_type, title = _identify_media_enhanced(
                name, 
                main_dir_name=main_dir_name,
                has_season_subdirs=has_season_subdirs,
                forced_media_type=forced_type, 
                ai_translator=ai_translator, 
                use_ai=use_ai
            )
            
            if tmdb_id:
                # ★ 核心修改：识别成功后，加入分组字典，等待第二阶段统一处理
                with group_lock:
                    key = (tmdb_id, media_type, title)
                    if key not in grouped_items:
                        grouped_items[key] = []
                    grouped_items[key].append(item)
            else:
                if is_folder:
                    logger.info(f"  📂 目录 '{name}' 无法直接识别，深入扫描子目录 (层级 {depth+1})...")
                    submit_task(scan_directory, item_id, name, depth + 1, main_dir_name)
                    from handler.p115_service import P115DeleteBuffer
                    P115DeleteBuffer.add(fids=[], base_cids=[item_id])
                else:
                    # ★ 核心修改：未识别文件也加入列表，等待批量移动
                    with group_lock:
                        unidentified_items.append(item)

        def scan_directory(current_cid, current_name, depth=0, root_dir_name=None):
            """目录透视与任务分发逻辑"""
            if depth > 5: return
                
            offset = 0
            limit = 1000 
            
            while True: 
                res = {}
                for retry in range(3):
                    try:
                        res = client.fs_files({
                            'cid': current_cid, 'limit': limit, 'offset': offset, 'o': 'user_utime', 'asc': 0,
                            'record_open_time': 0, 'count_folders': 0
                        })
                        break 
                    except Exception as e:
                        if '405' in str(e) or 'Method Not Allowed' in str(e): time.sleep(3)
                        else: raise

                data = res.get('data', [])
                if not data: break 

                has_season_subdirs = False
                for item in data:
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if str(fc_val) == '0':
                        sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                        if sub_name and re.search(r'^(Season\s?\d+|S\d+|第[一二三四五六七八九十\d]+季)$', sub_name, re.IGNORECASE):
                            has_season_subdirs = True
                            break

                for item in data:
                    name = item.get('fn') or item.get('n') or item.get('file_name')
                    if not name: continue
                    item_id = item.get('fid') or item.get('file_id')
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    is_folder = str(fc_val) == '0'

                    if str(item_id) == str(unidentified_cid) or (not config.get(constants.CONFIG_OPTION_115_UNRECOGNIZED_CID) and name == '未识别'):
                        continue

                    if is_folder and name.upper() in ['BDMV', 'CERTIFICATE', 'ANY!', 'VIDEO_TS', 'AUDIO_TS', 'PLAYLIST', 'CLIPINF', 'STREAM', 'BACKUP']:
                        continue
                        
                    if not is_folder:
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        if ext in ['clpi', 'mpls', 'bdmv', 'jar', 'bup', 'ifo']:
                            continue

                    forced_type = None
                    if is_folder and re.search(r'^(Season\s?\d+|S\d+|Ep?\d+|第[一二三四五六七八九十\d]+季)$', name, re.IGNORECASE):
                        forced_type = 'tv'

                    pass_root_name = name if depth == 0 else root_dir_name
                    submit_task(process_single_item, item, name, is_folder, depth, forced_type, pass_root_name, has_season_subdirs)

                if len(data) < limit: break
                offset += limit

        # =================================================================
        # 阶段一：启动初始扫描任务，等待所有文件识别完毕
        # =================================================================
        submit_task(scan_directory, save_cid, save_name, 0, None)

        with task_cond:
            while active_tasks > 0:
                task_cond.wait()

        # =================================================================
        # 阶段二：并发执行批量整理 (将同一部剧的散落文件打包成一次请求)
        # =================================================================
        if grouped_items:
            logger.info(f"  📦 扫描与识别完成，共分拣出 {len(grouped_items)} 个媒体组，开始并发批量整理...")
            active_tasks = 0 # 重置计数器
            
            # ★ 新增：计算总文件数和进度锁
            total_items_to_process = sum(len(items) for items in grouped_items.values())
            global_processed_count = 0
            progress_lock = threading.Lock()

            def execute_group(tmdb_id, media_type, title, items):
                nonlocal processed_count, global_processed_count
                try:
                    organizer = SmartOrganizer(client, tmdb_id, media_type, title, ai_translator, use_ai)
                    target_cid = organizer.get_target_cid()
                    
                    # ★ 新增：定义进度回调函数 (对讲机)
                    def item_progress_callback():
                        nonlocal global_processed_count
                        with progress_lock:
                            global_processed_count += 1
                            # 进度从 20% 到 95% 平滑过渡
                            prog = 20 + int((global_processed_count / total_items_to_process) * 75)
                            update_progress(prog, f"正在极速整理... ({global_processed_count}/{total_items_to_process})")

                    # ★ 核心：将对讲机传给底层执行器
                    if organizer.execute(items, target_cid, progress_callback=item_progress_callback):
                        with counter_lock:
                            processed_count += len(items)
                except Exception as e:
                    logger.error(f"  ❌ 批量整理出错 (ID:{tmdb_id}): {e}")

            for key, items in grouped_items.items():
                submit_task(execute_group, key[0], key[1], key[2], items)

            with task_cond:
                while active_tasks > 0:
                    task_cond.wait()

        # =================================================================
        # 阶段三：批量移入未识别目录
        # =================================================================
        if unidentified_items and unidentified_cid:
            logger.info(f"  🗑️ 正在批量移入未识别目录 ({len(unidentified_items)} 个文件)...")
            u_fids = [i.get('fid') or i.get('file_id') for i in unidentified_items]
            
            # 115 API fs_move 建议单次不超过 1000 个，这里按 500 切片
            chunk_size = 500
            for i in range(0, len(u_fids), chunk_size):
                chunk_fids = u_fids[i:i+chunk_size]
                chunk_items = unidentified_items[i:i+chunk_size]
                try:
                    client.fs_move(chunk_fids, unidentified_cid)
                    moved_to_unidentified += len(chunk_fids)
                    
                    # 记录日志
                    for item in chunk_items:
                        name = item.get('fn') or item.get('n') or item.get('file_name', '')
                        item_id = item.get('fid') or item.get('file_id')
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        if ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']:
                            pc = item.get('pc') or item.get('pick_code') 
                            P115RecordManager.add_or_update_record(
                                item_id, name, 'unrecognized', 
                                target_cid=unidentified_cid, category_name="未识别", 
                                pick_code=pc 
                            )
                except Exception as e:
                    logger.error(f"  ❌ 批量移入未识别目录失败: {e}")

        executor.shutdown()
        final_msg = f"扫描结束！成功归类 {processed_count} 个，移入未识别 {moved_to_unidentified} 个。"
        logger.info(f"=== {final_msg} ===")
        update_progress(100, final_msg)

    except Exception as e:
        logger.error(f"  ⚠️ 115 扫描任务异常: {e}", exc_info=True)
        update_progress(100, f"扫描异常结束: {e}")
    finally:
        try:
            resume_queue_processing()
        except:
            pass

def task_sync_115_directory_tree(processor=None):
    """
    主动同步 115 分类目录下的所有子目录到本地 DB 缓存。
    这能彻底解决 115 API search_value 失效导致的老目录无法识别问题。
    ★ 终极版：支持自动清理本地已失效的旧目录缓存。
    """
    logger.info("=== 开始全量同步 115 目录树到本地数据库 ===")
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    client = P115Service.get_client()
    if not client: 
        update_progress(100, "115 客户端未初始化，任务结束。")
        return

    raw_rules = settings_db.get_setting('p115_sorting_rules')
    if not raw_rules: 
        update_progress(100, "未配置分类规则，无需同步。")
        return
    
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
    
    target_dirs = {}
    for rule in rules:
        if rule.get('enabled', True) and rule.get('cid'):
            cid_str = str(rule['cid'])
            if cid_str and cid_str != '0':
                display_name = rule.get('category_path') or rule.get('dir_name') or rule.get('name') or f"CID:{cid_str}"
                target_dirs[cid_str] = display_name

    if not target_dirs:
        update_progress(100, "未找到有效的分类目标目录 CID，任务结束。")
        return

    total_cached = 0
    total_cleaned = 0
    total_cids = len(target_dirs)
    
    for idx, (cid, dir_name) in enumerate(target_dirs.items()):
        base_prog = int((idx / total_cids) * 100)
        update_progress(base_prog, f"  🔍 正在扫描第 {idx+1}/{total_cids} 个分类目录: [{dir_name}] ...")
        
        offset = 0
        limit = 1000
        page_count = 0
        
        # ★ 核心新增：记录本次从网盘真实扫到的所有子目录 ID
        current_valid_sub_cids = set()
        
        while True:
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                update_progress(100, "任务已被用户手动终止。")
                return

            try:
                res = client.fs_files({'cid': cid, 'limit': limit, 'offset': offset, 'record_open_time': 0, 'count_folders': 0})
                data = res.get('data', [])
                
                if not data: 
                    break
                
                page_count += 1
                dir_count_in_page = 0
                
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        for item in data:
                            fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                            if str(fc_val) == '0':
                                sub_cid = item.get('fid') or item.get('file_id')
                                sub_name = item.get('fn') or item.get('n') or item.get('file_name')
                                if sub_cid and sub_name:
                                    # 记录有效的子目录 ID
                                    current_valid_sub_cids.add(str(sub_cid))
                                    
                                    current_local_path = os.path.join(dir_name, str(sub_name))
                                    
                                    cursor.execute("""
                                        INSERT INTO p115_filesystem_cache (id, parent_id, name, local_path)
                                        VALUES (%s, %s, %s, %s)
                                        ON CONFLICT (parent_id, name)
                                        DO UPDATE SET 
                                            id = EXCLUDED.id, 
                                            local_path = EXCLUDED.local_path,
                                            updated_at = NOW()
                                    """, (str(sub_cid), str(cid), str(sub_name), current_local_path))
                                    total_cached += 1
                                    dir_count_in_page += 1
                        conn.commit()
                
                update_progress(base_prog, f"  ➜ [{dir_name}] | 翻阅第 {page_count} 页 | 新增/更新 {dir_count_in_page} 个目录...")
                
                if len(data) < limit:
                    break
                    
                offset += limit
                
            except Exception as e:
                logger.error(f"  ❌ 同步目录树异常 [{dir_name}]: {e}")
                break 

        # =================================================================
        # ★★★ 核心新增：清理本地数据库中多余的失效目录 ★★★
        # =================================================================
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. 先查出本地数据库里，属于当前父目录(cid)的所有子目录 ID
                    cursor.execute("SELECT id FROM p115_filesystem_cache WHERE parent_id = %s", (str(cid),))
                    db_sub_cids = {row['id'] for row in cursor.fetchall()}
                    
                    # 2. 找出“在本地数据库里，但不在网盘真实列表里”的失效 ID
                    invalid_cids = db_sub_cids - current_valid_sub_cids
                    
                    # 3. 执行删除
                    if invalid_cids:
                        # 转换成元组供 SQL IN 语句使用
                        invalid_cids_tuple = tuple(invalid_cids)
                        cursor.execute("DELETE FROM p115_filesystem_cache WHERE id IN %s", (invalid_cids_tuple,))
                        conn.commit()
                        
                        cleaned_count = len(invalid_cids)
                        total_cleaned += cleaned_count
                        logger.info(f"  🧹 [{dir_name}] 清理了 {cleaned_count} 个已失效的本地目录缓存。")
        except Exception as e:
            logger.error(f"  ❌ 清理失效目录异常 [{dir_name}]: {e}")

    update_progress(100, f"=== 同步结束！共更新 {total_cached} 个目录，清理 {total_cleaned} 个失效缓存 ===")

def task_full_sync_strm_and_subs(processor=None):
    """
    【V4 终极上帝视角版】全量生成 STRM 与 同步字幕
    利用 115 分类目录级全局拉取 (type=4/1) + 动态 API 溯源 + 本地 DB 目录树缓存，实现秒级增量同步！
    """
    config = get_config()
    download_subs = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True)
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    
    start_msg = "=== 🚀 开始极速全量同步 STRM 与 字幕 ===" if download_subs else "=== 🚀 开始极速全量同步 STRM (跳过字幕) ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    logger.info(start_msg)
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    # ★ 通知监控服务进入蓄水池模式，防止全量同步触发海量刮削
    try:
        from monitor_service import pause_queue_processing, resume_queue_processing
        pause_queue_processing()
    except Exception as e:
        logger.warning(f"  ⚠️ 无法暂停监控队列: {e}")
        resume_queue_processing = lambda: None # 兜底防报错

    try:
        local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
        etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
        
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        known_sub_exts = {'srt', 'ass', 'ssa', 'sub', 'vtt', 'sup'}
        
        allowed_exts = set(e.lower() for e in config.get(constants.CONFIG_OPTION_115_EXTENSIONS, []))
        if not allowed_exts:
            allowed_exts = known_video_exts | known_sub_exts
        
        if not local_root or not etk_url:
            update_progress(100, "错误：未配置本地 STRM 根目录或 ETK 访问地址！")
            return

        client = P115Service.get_client()
        if not client: return

        raw_rules = settings_db.get_setting('p115_sorting_rules')
        if not raw_rules: 
            update_progress(100, "错误：未配置分类规则！")
            return
        rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules

        # 获取重命名配置，用于判断 STRM 直链是否需要带文件名
        rename_config = settings_db.get_setting('p115_rename_config') or {}

        # =================================================================
        # 阶段 1: 加载规则与本地目录树缓存到内存 (耗时: 毫秒级)
        # =================================================================
        update_progress(5, "  🧠 正在加载本地目录树缓存到内存...")
        
        cid_to_rel_path = {}  
        target_cids = set()   
        
        for r in rules:
            if r.get('enabled', True) and r.get('cid') and str(r['cid']) != '0':
                cid = str(r['cid'])
                target_cids.add(cid)
                cid_to_rel_path[cid] = r.get('category_path') or r.get('dir_name', '未识别')

        # 加载 DB 中的目录树 (新增提取 local_path)
        dir_cache = {} 
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id, parent_id, name, local_path FROM p115_filesystem_cache")
                    for row in cursor.fetchall():
                        dir_cache[str(row['id'])] = {
                            'pid': str(row['parent_id']), 
                            'name': str(row['name']),
                            'local_path': row['local_path']
                        }
        except Exception as e:
            update_progress(100, f"读取本地目录缓存失败: {e}")
            return

        # 动态 API 路径缓存池 (防止重复请求 115 接口)
        dynamic_path_cache = {}

        # 内存路径推导函数 (★ 终极修复版：DB缓存 + API动态溯源)
        def resolve_local_dir(pid, target_cid):
            pid = str(pid)
            # 1. 如果文件直接在分类根目录下
            if pid in cid_to_rel_path:
                return cid_to_rel_path[pid]
                
            # 2. 如果刚才已经通过 API 查过这个目录了，直接秒回
            if pid in dynamic_path_cache:
                return dynamic_path_cache[pid]

            # 3. 尝试使用数据库中已有的 local_path
            if pid in dir_cache and dir_cache[pid].get('local_path'):
                return dir_cache[pid]['local_path']
                
            # 4. 尝试在数据库缓存中向上追溯
            parts = []
            curr = pid
            while curr and curr in dir_cache:
                parts.append(dir_cache[curr]['name'])
                curr = dir_cache[curr]['pid']
                
                if curr in cid_to_rel_path:
                    parts.append(cid_to_rel_path[curr])
                    parts.reverse()
                    resolved_path = os.path.join(*parts)
                    dynamic_path_cache[pid] = resolved_path # 存入内存池
                    return resolved_path

            # 5. ★ 终极兜底：缓存穿透时，主动向 115 请求该目录的真实路径
            try:
                dir_info = client.fs_files({'cid': pid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                path_nodes = dir_info.get('path', [])
                if path_nodes:
                    start_idx = -1
                    for i, p_node in enumerate(path_nodes):
                        if str(p_node.get('cid') or p_node.get('file_id')) == target_cid:
                            start_idx = i + 1
                            break
                    if start_idx != -1:
                        sub_folders = [str(p.get('name') or p.get('file_name')).strip() for p in path_nodes[start_idx:]]
                        base_cat_path = cid_to_rel_path.get(target_cid, '未识别')
                        resolved_path = os.path.join(base_cat_path, *sub_folders) if sub_folders else base_cat_path
                        dynamic_path_cache[pid] = resolved_path # 存入内存池，同目录文件不再请求
                        logger.debug(f"  🔍 [API溯源] 成功动态推导路径: {resolved_path}")
                        return resolved_path
            except Exception as e:
                logger.debug(f"  ⚠️ 动态查询目录路径失败 (pid: {pid}): {e}")

            return None

        # =================================================================
        # 阶段 2: 分类目录级全局拉取 (耗时: 秒级/分钟级)
        # =================================================================
        sync_has_errors = False
        valid_local_files = set()
        files_generated = 0
        subs_downloaded = 0
        
        fetch_types = [4] # 4=视频
        if download_subs: fetch_types.append(1) # 1=文档(含字幕)

        total_targets = len(target_cids)
        
        for idx, target_cid in enumerate(target_cids):
            category_name = cid_to_rel_path.get(target_cid, "未知分类")
            base_prog = 10 + int((idx / total_targets) * 80)
            update_progress(base_prog, f"  🌐 正在全局拉取分类 [{category_name}] 下的所有文件...")
            
            for f_type in fetch_types:
                type_name = "视频" if f_type == 4 else "文档/字幕"
                offset = 0
                limit = 1000
                page = 1
                
                while True:
                    if processor and getattr(processor, 'is_stop_requested', lambda: False)(): return
                    
                    try:
                        # ★ 核心：指定 cid 并传入 type，强制 115 在该分类下进行全局递归检索！
                        res = client.fs_files({'cid': target_cid, 'type': f_type, 'limit': limit, 'offset': offset, 'record_open_time': 0})
                        if not res.get('state') and res.get('code'):
                            logger.error(f"  ❌ API 返回异常状态 (可能触发流控): {res}")
                            sync_has_errors = True
                            break
                        data = res.get('data', [])
                        if not data: break
                        
                        logger.info(f"  ➜ [{category_name}] - [{type_name}] 获取第 {page} 页 ({len(data)} 个文件)...")
                        
                        for item in data:
                            # 兼容 OpenAPI 键名
                            name = item.get('fn') or item.get('n') or item.get('file_name', '')
                            ext = name.split('.')[-1].lower() if '.' in name else ''
                            if ext not in allowed_exts: continue
                            
                            pc = item.get('pc') or item.get('pick_code')
                            # 115 返回的文件数据中，pid/cid 代表它所在的父目录 ID
                            pid = item.get('pid') or item.get('cid') or item.get('parent_id')
                            if not pc or not pid: continue
                            
                            # ★ 瞬间推导本地路径 (使用终极修复版函数)
                            rel_dir = resolve_local_dir(pid, target_cid)
                                
                            if not rel_dir: 
                                logger.warning(f"  ⚠️ 彻底无法推导路径，跳过文件: {name} (pid: {pid})")
                                continue 
                                
                            current_local_path = os.path.join(local_root, rel_dir)
                            os.makedirs(current_local_path, exist_ok=True)
                            
                            # 处理视频 STRM
                            if ext in known_video_exts:
                                strm_name = os.path.splitext(name)[0] + ".strm"
                                strm_path = os.path.join(current_local_path, strm_name)
                                
                                # ==================================================
                                # ★ 动态计算 STRM 内容 (支持挂载模式与直链模式)
                                # ==================================================
                                if not etk_url.startswith('http'):
                                    # 挂载模式
                                    mount_prefix = etk_url
                                    mount_path = os.path.join(mount_prefix, rel_dir, name)
                                    content = mount_path.replace('\\', '/')
                                else:
                                    # 默认的 ETK 302 直链模式
                                    content = f"{etk_url}/api/p115/play/{pc}"
                                    if rename_config.get('strm_url_fmt') == 'with_name':
                                        content = f"{content}/{name}"
                                
                                need_write = True
                                if os.path.exists(strm_path):
                                    try:
                                        with open(strm_path, 'r', encoding='utf-8') as f:
                                            old_content = f.read().strip()
                                            if old_content == content: 
                                                need_write = False
                                            else:
                                                logger.debug(f"  🔄 [更新] 内容不一致触发覆盖 -> 旧: [{old_content}] | 新: [{content}]")
                                    except Exception as e: pass
                                            
                                if need_write:
                                    with open(strm_path, 'w', encoding='utf-8') as f: f.write(content)
                                    if not os.path.exists(strm_path):
                                        logger.debug(f"  📝 [新增] 生成 STRM: {strm_name}")
                                    files_generated += 1
                                    
                                valid_local_files.add(os.path.abspath(strm_path))
                                
                                # ==================================================
                                # ★ 写入本地数据库缓存 (p115_filesystem_cache)
                                # ==================================================
                                fid = item.get('fid') or item.get('file_id')
                                sha1 = item.get('sha1') or item.get('sha')
                                file_size = _parse_115_size(item.get('fs') or item.get('size'))
                                if pc and fid:
                                    file_local_path = os.path.join(rel_dir, name).replace('\\', '/')
                                    P115CacheManager.save_file_cache(
                                        fid=fid, parent_id=pid, name=name, 
                                        sha1=sha1, pick_code=pc, 
                                        local_path=file_local_path, size=file_size 
                                    )
                                    
                            # 处理字幕下载
                            elif ext in known_sub_exts and download_subs:
                                sub_path = os.path.join(current_local_path, name)
                                if not os.path.exists(sub_path):
                                    try:
                                        import requests
                                        url_obj = client.download_url(pc, user_agent="Mozilla/5.0")
                                        if url_obj:
                                            headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                            resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                                            resp.raise_for_status()
                                            with open(sub_path, 'wb') as f:
                                                for chunk in resp.iter_content(8192): f.write(chunk)
                                            logger.info(f"  ⬇️ [增量] 下载字幕: {name}")
                                            subs_downloaded += 1
                                    except Exception as e:
                                        logger.error(f"  ❌ 下载字幕失败 [{name}]: {e}")
                                        
                                valid_local_files.add(os.path.abspath(sub_path))

                        if len(data) < limit: break
                        offset += limit
                        page += 1
                        
                    except Exception as e:
                        logger.error(f"  ❌ 全局拉取异常 (cid={target_cid}, type={f_type}): {e}")
                        sync_has_errors = True
                        break

        logger.info(f"  ✅ 增量同步完成！新增/更新 STRM: {files_generated} 个, 下载字幕: {subs_downloaded} 个。")

        # =================================================================
        # 阶段 3: 本地失效文件清理 (耗时: 秒级)
        # =================================================================
        if enable_cleanup:
            if sync_has_errors:
                logger.warning("  🛑 致命警告：本次同步过程中发生 API 异常或触发 115 流控！为防止灾难性误删，已强制跳过本地清理阶段！")
            elif not valid_local_files and files_generated == 0:
                logger.warning("  ⚠️ 警告：本次同步未获取到任何有效文件，为防止误删，已跳过本地清理阶段！")
            else:
                update_progress(90, "  🧹 正在比对并清理本地失效文件与空壳目录...")
                cleaned_files = 0
                cleaned_dirs = 0
                import shutil  # 引入 shutil 用于连锅端
                
                for cid, rel_path in cid_to_rel_path.items():
                    target_local_dir = os.path.join(local_root, rel_path)
                    if not os.path.exists(target_local_dir): continue
                    
                    # 1. 先清理失效的 STRM 和 字幕文件
                    for root_dir, dirs, files in os.walk(target_local_dir):
                        for file in files:
                            ext = file.split('.')[-1].lower()
                            if ext in known_sub_exts or ext == 'strm':
                                file_path = os.path.abspath(os.path.join(root_dir, file))
                                if file_path not in valid_local_files:
                                    try:
                                        os.remove(file_path)
                                        cleaned_files += 1
                                        logger.debug(f"  🗑️ [清理] 删除失效文件: {file}")
                                    except Exception as e:
                                        logger.warning(f"  ⚠️ 删除文件失败 {file}: {e}")
                    
                    # 2. ★ 终极暴力清理：自下而上扫描，只要没有 STRM，无视任何残留文件直接连锅端！
                    for root_dir, dirs, files in os.walk(target_local_dir, topdown=False):
                        for d in dirs:
                            dir_path = os.path.join(root_dir, d)
                            if not os.path.exists(dir_path):
                                continue
                                
                            # 检查该目录及其所有子目录中，是否还存在任何 .strm 文件
                            has_strm = False
                            for r, _, fs in os.walk(dir_path):
                                if any(f.lower().endswith('.strm') for f in fs):
                                    has_strm = True
                                    break
                                    
                            # 如果没有 STRM，判定为空壳目录，直接物理超度（连带里面的 nfo/jpg 一起扬了）
                            if not has_strm:
                                try:
                                    shutil.rmtree(dir_path)
                                    cleaned_dirs += 1
                                    logger.debug(f"  🗑️ [清理] 删除无 STRM 的空壳目录: {dir_path}")
                                except Exception as e:
                                    logger.warning(f"  ⚠️ 删除目录失败 {dir_path}: {e}")
                            
                logger.info(f"  🧹 清理完成: 删除了 {cleaned_files} 个失效文件, {cleaned_dirs} 个无STRM的空壳目录。")

        update_progress(100, "=== 全量生成STRM任务结束 ===")

    except Exception as e:
        logger.error(f"  ❌ 全量同步任务异常: {e}", exc_info=True)
        update_progress(100, f"任务异常结束: {e}")
    finally:
        # ★ 任务结束（无论成功失败），务必解除监控队列抑制，恢复处理
        try:
            resume_queue_processing()
        except:
            pass

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
                        logger.warning(f"  ⚠️ [深度删除] 本地缓存未找到对应 PC 码的文件，无法执行本地推导，任务终止。")
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
                        logger.info(f"  💥 [深度删除] 本地推导完毕！向 115 发送批量删除指令 (共 {len(final_api_ids)} 个顶级节点)...")
                        resp = client.fs_delete(final_api_ids)
                        
                        if resp.get('state'):
                            logger.info(f"  ✅ [深度删除] 115 网盘文件/空目录物理销毁成功！")
                        else:
                            logger.error(f"  ❌ [深度删除] 115 API 删除失败: {resp}")
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
                        logger.info(f"  🧹 [深度删除] 本地数据清理完毕: 缓存表移除 {deleted_cache_count} 条, 记录表移除 {deleted_record_count} 条。")

        except Exception as e:
            logger.error(f"  ❌ [深度删除] 执行异常: {e}", exc_info=True)

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
            logger.error(f"  ❌ 批量重组失败: {e}", exc_info=True)


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

        info_res = client.fs_get_info(file_id)
        if not info_res or not info_res.get('state') or not info_res.get('data'):
            logger.warning(f"无法在 115 中定位到该文件(ID:{file_id})，可能已被删除。")
            continue

        info_data = info_res['data']
        old_pid = info_data.get('parent_id') or info_data.get('cid')
        if old_pid: old_pids.add(str(old_pid))

        pick_code = info_data.get('pick_code')
        if not pick_code and old_cache:
            pick_code = old_cache['pick_code']

        root_items.append({
            'fid': info_data.get('file_id') or file_id,
            'file_id': info_data.get('file_id') or file_id,
            'fn': original_name,
            'fc': str(info_data.get('file_category', '1')),
            'pid': old_pid,
            'pc': pick_code,
            'pick_code': pick_code,
            'sha1': info_data.get('sha1') or (old_cache['sha1'] if old_cache else None),
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

    logger.info(f"  🛠️ [批量重组] 开始对 {len(root_items)} 个文件执行定向整理 -> ID:{tmdb_id}")

    organizer = SmartOrganizer(client, tmdb_id, media_type, title, None, False, season_num=season_num)
    organizer.is_manual_correct = True
    if season_num is not None and str(season_num).strip():
        organizer.forced_season = int(season_num)
        logger.info(f"  📌 [批量重组] 已强制指定季号: Season {organizer.forced_season}")

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
            logger.warning(f"  ⚠️ 查找关联字幕失败: {e}")

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

        for r_item in root_items:
            old_cache = r_item['_old_cache']
            if not old_cache or not old_cache.get('local_path'): continue

            old_file_rel_path = str(old_cache['local_path']).lstrip('\\/')
            old_strm_rel_path = os.path.splitext(old_file_rel_path)[0] + ".strm"
            old_strm_full_path = os.path.join(local_root, old_strm_rel_path)

            if os.path.exists(old_strm_full_path):
                os.remove(old_strm_full_path)
                logger.debug(f"  🧹 删除本地旧 STRM: {old_strm_full_path}")

            old_mi_full_path = os.path.splitext(old_file_rel_path)[0] + "-mediainfo.json"
            if os.path.exists(old_mi_full_path):
                os.remove(old_mi_full_path)

            old_dir_full_path = os.path.dirname(old_strm_full_path)
            old_base_name = os.path.splitext(os.path.basename(old_file_rel_path))[0]
            if os.path.exists(old_dir_full_path):
                for f in os.listdir(old_dir_full_path):
                    if f.startswith(old_base_name) and f.split('.')[-1].lower() in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']:
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
                            if ext in {'strm', 'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'nfo'}:
                                has_media = True
                                break
                        if has_media: break

                    if not has_media:
                        shutil.rmtree(curr_dir)
                        logger.info(f"  🧹 本地旧目录已无媒体文件，执行删除: {curr_dir}")
                        curr_dir = os.path.dirname(curr_dir)
                    else:
                        break
                else:
                    break

        # ★ 批量通知 Emby 刷新旧路径 (去重后一次性通知)
        emby_url = config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        emby_api_key = config.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        if emby_url and emby_api_key:
            from handler import emby
            top_dirs = set()
            for d in refresh_target_dirs:
                top_dirs.add(d)
            
            for d in top_dirs:
                target_to_refresh = d if os.path.exists(d) else os.path.dirname(d)
                logger.info(f"  🔄 正在通知 Emby 刷新旧路径以清理失效媒体项: {target_to_refresh}")
                try:
                    emby.refresh_library_by_path(target_to_refresh, emby_url, emby_api_key)
                except Exception as e:
                    logger.warning(f"  ⚠️ 通知 Emby 刷新旧路径失败: {e}")

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
        logger.info(f"  ⏳ 已将网盘旧目录链条 ({len(old_cids_to_check)}个层级) 加入全局清理队列，稍后执行清理...")
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

    logger.info(f"  ✅ [批量重组] {len(root_items)} 个文件处理完成！")

def task_sync_music_library(processor=None):
    """
    独立音乐库全量同步任务：增量生成 STRM + 下载附属文件(封面/歌词) + 自动清理
    """
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager:
            task_manager.update_status_from_thread(prog, msg)

    config = get_config()
    from database import settings_db
    import constants
    import os
    import shutil
    
    music_cid = settings_db.get_setting('p115_music_root_cid')
    music_root_name = settings_db.get_setting('p115_music_root_name') or "音乐库"
    music_root_name = music_root_name.strip('/')
    
    local_root = config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT)
    etk_url = config.get(constants.CONFIG_OPTION_ETK_SERVER_URL, "").rstrip('/')
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    # ★ 复用下载字幕的开关来控制是否下载音乐附属文件
    download_aux = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True) 
    
    if not music_cid or str(music_cid) == '0':
        msg = "未配置音乐库根目录，跳过同步。"
        logger.warning(msg)
        update_progress(100, msg)
        return
        
    if not local_root or not etk_url:
        msg = "未配置本地 STRM 根目录或 ETK 访问地址！"
        logger.error(msg)
        update_progress(100, msg)
        return

    start_msg = f"=== 🎵 开始同步音乐库 [{music_root_name}] ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    logger.info(start_msg)
    update_progress(5, f"正在连接 115 获取 [{music_root_name}] 目录信息...")

    client = P115Service.get_client()
    if not client: 
        update_progress(100, "115 客户端未初始化，同步失败。")
        return

    audio_exts = {'mp3', 'flac', 'wav', 'ape', 'm4a', 'aac', 'ogg', 'wma', 'alac'}
    # ★ 定义需要下载的附属文件扩展名
    aux_exts = {'lrc', 'jpg', 'jpeg', 'png', 'nfo', 'txt', 'cue'}
    
    music_local_base = os.path.join(local_root, music_root_name)
    os.makedirs(music_local_base, exist_ok=True)

    files_generated = 0
    files_skipped = 0
    aux_downloaded = 0
    dirs_scanned = 0
    valid_local_files = set() 
    sync_has_errors = False

    def _recursive_sync(current_cid, current_local_path):
        nonlocal files_generated, files_skipped, aux_downloaded, dirs_scanned, sync_has_errors
        
        dirs_scanned += 1
        display_path = os.path.basename(current_local_path) or music_root_name
        update_progress(50, f"正在扫描: {display_path} (已扫 {dirs_scanned} 个目录)")
        
        offset = 0
        limit = 1000
        
        while True:
            if processor and getattr(processor, 'is_stop_requested', lambda: False)():
                logger.info("音乐库同步任务被手动终止。")
                update_progress(100, "任务已手动终止。")
                return

            try:
                res = client.fs_files({'cid': current_cid, 'limit': limit, 'offset': offset, 'record_open_time': 0})
                if not res.get('state') and res.get('code'):
                    logger.error(f"  ❌ API 返回异常状态 (可能触发流控): {res}")
                    sync_has_errors = True
                    break
                data = res.get('data', [])
                if not data: break
                
                for item in data:
                    name = item.get('fn') or item.get('n') or item.get('file_name', '')
                    fc_val = str(item.get('fc') if item.get('fc') is not None else item.get('type'))
                    item_id = item.get('fid') or item.get('file_id')
                    
                    if fc_val == '0': # 文件夹
                        sub_local_path = os.path.join(current_local_path, name)
                        os.makedirs(sub_local_path, exist_ok=True)
                        
                        P115CacheManager.save_cid(item_id, current_cid, name)
                        rel_dir = os.path.relpath(sub_local_path, local_root).replace('\\', '/')
                        P115CacheManager.update_local_path(item_id, rel_dir)
                        
                        _recursive_sync(item_id, sub_local_path)
                        
                    elif fc_val == '1': # 文件
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        pc = item.get('pc') or item.get('pick_code')
                        if not pc: continue
                        
                        # ==========================================
                        # 1. 处理音频文件 -> 生成 STRM
                        # ==========================================
                        if ext in audio_exts:
                            strm_name = os.path.splitext(name)[0] + ".strm"
                            strm_path = os.path.join(current_local_path, strm_name)
                            
                            if not etk_url.startswith('http'):
                                rel_p = os.path.relpath(strm_path, local_root)
                                content = os.path.join(etk_url, rel_p).replace('\\', '/')
                                content = content[:-5] + f".{ext}" 
                            else:
                                content = f"{etk_url}/api/p115/play/{pc}/{name}"
                                
                            need_write = True
                            if os.path.exists(strm_path):
                                try:
                                    with open(strm_path, 'r', encoding='utf-8') as f:
                                        old_content = f.read().strip()
                                        if old_content == content: 
                                            need_write = False
                                except Exception: pass
                                            
                            if need_write:
                                with open(strm_path, 'w', encoding='utf-8') as f:
                                    f.write(content)
                                files_generated += 1
                            else:
                                files_skipped += 1
                                
                            valid_local_files.add(os.path.abspath(strm_path))
                            
                            if (files_generated + files_skipped) % 200 == 0:
                                logger.info(f"  🎵 进度: 新增/更新 {files_generated} 首, 跳过 {files_skipped} 首...")
                            
                            sha1 = item.get('sha1') or item.get('sha')
                            file_size = _parse_115_size(item.get('fs') or item.get('size'))
                            rel_dir = os.path.relpath(current_local_path, local_root)
                            file_local_path = os.path.join(rel_dir, name).replace('\\', '/')
                            
                            P115CacheManager.save_file_cache(
                                fid=item_id, parent_id=current_cid, name=name,
                                sha1=sha1, pick_code=pc,
                                local_path=file_local_path, size=file_size
                            )
                            
                        # ==========================================
                        # ★ 2. 处理附属文件 -> 直接下载到本地
                        # ==========================================
                        elif ext in aux_exts and download_aux:
                            aux_path = os.path.join(current_local_path, name)
                            if not os.path.exists(aux_path):
                                try:
                                    import requests
                                    url_obj = client.download_url(pc, user_agent="Mozilla/5.0")
                                    if url_obj:
                                        headers = {"User-Agent": "Mozilla/5.0", "Cookie": P115Service.get_cookies()}
                                        resp = requests.get(str(url_obj), stream=True, timeout=15, headers=headers)
                                        resp.raise_for_status()
                                        with open(aux_path, 'wb') as f:
                                            for chunk in resp.iter_content(8192): f.write(chunk)
                                        logger.info(f"  ⬇️ [增量] 下载音乐附属文件: {name}")
                                        aux_downloaded += 1
                                except Exception as e:
                                    logger.error(f"  ❌ 下载音乐附属文件失败 [{name}]: {e}")
                            
                            # 无论是否刚刚下载，只要网盘里有，就加入有效名单，防止被清理
                            valid_local_files.add(os.path.abspath(aux_path))
                            
                if len(data) < limit: break
                offset += limit
            except Exception as e:
                logger.error(f"同步音乐目录异常 (CID:{current_cid}): {e}")
                sync_has_errors = True
                break

    _recursive_sync(music_cid, music_local_base)
    
    # =================================================================
    # ★ 本地失效文件清理阶段 (包含附属文件)
    # =================================================================
    cleaned_files = 0
    cleaned_dirs = 0
    
    if enable_cleanup:
        if sync_has_errors:
            logger.warning("  🛑 致命警告：音乐库同步过程中发生 API 异常或触发流控！为防止灾难性误删，已强制跳过本地清理阶段！")
        elif not valid_local_files and files_generated == 0 and files_skipped == 0:
            logger.warning("  ⚠️ 警告：本次同步未获取到任何有效文件，为防止误删，已跳过本地清理阶段！")
        else:
            update_progress(90, "  🧹 正在比对并清理本地失效文件与空壳目录...")
            
            if os.path.exists(music_local_base):
                # 1. 清理失效的 STRM 和 附属文件
                for root_dir, dirs, files in os.walk(music_local_base):
                    for file in files:
                        ext = file.split('.')[-1].lower()
                        # ★ 检查范围扩大：包含 strm 和所有附属扩展名
                        if ext == 'strm' or ext in aux_exts:
                            file_path = os.path.abspath(os.path.join(root_dir, file))
                            if file_path not in valid_local_files:
                                try:
                                    os.remove(file_path)
                                    cleaned_files += 1
                                    logger.debug(f"  🗑️ [清理] 删除失效文件: {file}")
                                except Exception: pass
                
                # 2. 自下而上扫描，清理空壳目录 (逻辑不变：只要没有 STRM 就连锅端)
                for root_dir, dirs, files in os.walk(music_local_base, topdown=False):
                    for d in dirs:
                        dir_path = os.path.join(root_dir, d)
                        if not os.path.exists(dir_path): continue
                            
                        has_strm = False
                        for r, _, fs in os.walk(dir_path):
                            if any(f.lower().endswith('.strm') for f in fs):
                                has_strm = True
                                break
                                
                        if not has_strm:
                            try:
                                shutil.rmtree(dir_path)
                                cleaned_dirs += 1
                                logger.debug(f"  🗑️ [清理] 删除无 STRM 的空壳目录: {dir_path}")
                            except Exception: pass

    end_msg = f"=== 🎵 音乐库同步完成！新增/更新: {files_generated} 首, 下载附属: {aux_downloaded} 个 ==="
    if enable_cleanup:
        end_msg += f" | 清理失效文件: {cleaned_files} 个, 空目录: {cleaned_dirs} 个"
        
    logger.info(end_msg)
    update_progress(100, f"同步完成！生成 {files_generated} 首，下载 {aux_downloaded} 个附属文件。")
