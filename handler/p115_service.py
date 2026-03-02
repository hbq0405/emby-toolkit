# handler/p115_service.py
import logging
import requests
import random
import os
import json
import re
import threading
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

    def fs_move(self, fid, to_cid):
        url = f"{self.base_url}/open/ufile/move"
        return self._do_request("POST", url, data={"file_ids": str(fid), "to_cid": str(to_cid)})

    def fs_rename(self, fid_name_tuple):
        url = f"{self.base_url}/open/ufile/update"
        return self._do_request("POST", url, data={"file_id": str(fid_name_tuple[0]), "file_name": str(fid_name_tuple[1])})

    def fs_delete(self, fids):
        url = f"{self.base_url}/open/ufile/delete"
        fids_str = ",".join([str(f) for f in fids]) if isinstance(fids, list) else str(fids)
        return self._do_request("POST", url, data={"file_ids": fids_str})


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

            def _check_openapi(self):
                if not self._openapi:
                    raise Exception("未配置 115 Token (OpenAPI)，无法执行管理操作")

            def _rate_limit(self):
                """★ 核心升级：底层统一 API 流控拦截器 ★"""
                with P115Service._lock:
                    try:
                        # 默认 0.5 秒请求一次 (即 2 QPS)，对 OpenAPI 来说非常安全且高效
                        interval = float(get_config().get(constants.CONFIG_OPTION_115_INTERVAL, 0.5))
                    except (ValueError, TypeError):
                        interval = 0.5
                    
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

            def fs_move(self, fid, to_cid):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_move(fid, to_cid)

            def fs_rename(self, fid_name_tuple):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_rename(fid_name_tuple)

            def fs_delete(self, fids):
                self._check_openapi()
                self._rate_limit()
                return self._openapi.fs_delete(fids)

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
# ★★★ 新增：115 目录树 DB 缓存管理器 ★★★
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
    def save_file_cache(fid, parent_id, name, sha1=None, pick_code=None):
        """专门将文件(fc=1)的 SHA1 和 PC码 存入本地数据库缓存"""
        if not fid or not parent_id or not name: return
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO p115_filesystem_cache (id, parent_id, name, sha1, pick_code)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (parent_id, name)
                        DO UPDATE SET 
                            id = EXCLUDED.id, 
                            sha1 = COALESCE(EXCLUDED.sha1, p115_filesystem_cache.sha1), 
                            pick_code = COALESCE(EXCLUDED.pick_code, p115_filesystem_cache.pick_code), 
                            updated_at = NOW()
                    """, (str(fid), str(parent_id), str(name), sha1, pick_code))
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

def get_config():
    return config_manager.APP_CONFIG

class SmartOrganizer:
    def __init__(self, client, tmdb_id, media_type, original_title):
        self.client = client
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_title = original_title
        self.api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)

        self.studio_map = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
        self.keyword_map = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
        self.rating_map = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
        self.rating_priority = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY

        self.raw_metadata = self._fetch_raw_metadata()
        self.details = self.raw_metadata
        self.rename_config = settings_db.get_setting(constants.DB_KEY_115_RENAME_CONFIG) or {
            "main_title_lang": "zh", "main_year_en": True, "main_tmdb_fmt": "{tmdb=ID}",
            "season_fmt": "Season {02}", "file_title_lang": "zh", "file_year_en": False,
            "file_tmdb_fmt": "none", "file_params_en": True, "file_sep": " - "
        }
        raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
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
                    append_to_response="keywords,content_ratings,networks"
                )
            else:
                raw_details = tmdb.get_movie_details(
                    self.tmdb_id, self.api_key,
                    append_to_response="keywords,release_dates"
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

            # 补充标题日期供重命名
            data['title'] = raw_details.get('title') or raw_details.get('name')
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

            return data

        except Exception as e:
            logger.warning(f"  ⚠️ [整理] 获取原始元数据失败: {e}", exc_info=True)
            return {}

    def _match_rule(self, rule):
        """
        规则匹配逻辑：
        - 标准字段：直接比对 ID/Code
        - 集合字段（工作室/关键词）：通过 Label 反查 Config 中的 ID 列表，再比对 TMDb ID
        """
        if not self.raw_metadata: return False

        # 1. 媒体类型
        if rule.get('media_type') and rule['media_type'] != 'all':
            if rule['media_type'] != self.media_type: return False

        # 2. 类型 (Genres) - ID 匹配
        if rule.get('genres'):
            # rule['genres'] 存的是 ID 列表 (如 [16, 35])
            # self.raw_metadata['genre_ids'] 是 TMDb ID 列表
            # 只要有一个交集就算命中
            rule_ids = [int(x) for x in rule['genres']]
            if not any(gid in self.raw_metadata['genre_ids'] for gid in rule_ids): return False

        # 3. 国家 (Countries) - Code 匹配
        if rule.get('countries'):
            # rule['countries'] 存的是 Code (如 ['US', 'CN'])
            # 只匹配第一个主要国家，避免合拍片误判 
            current_countries = self.raw_metadata.get('country_codes', [])
            # 获取列表中的第一个国家作为主要国家
            primary_country = current_countries[0] if current_countries else None
            
            # 如果没有国家信息，或者主要国家不在规则允许的列表中，则不匹配
            if not primary_country or primary_country not in rule['countries']:
                return False

        # 4. 语言 (Languages) - Code 匹配
        if rule.get('languages'):
            if self.raw_metadata['lang_code'] not in rule['languages']: return False

        # 5. 工作室 (Studios) - Label -> ID 匹配
        if rule.get('studios'):
            # rule['studios'] 存的是 Label (如 ['漫威', 'Netflix'])
            # 我们需要遍历这些 Label，去 self.studio_map 里找对应的 ID
            target_ids = set()
            for label in rule['studios']:
                # 找到配置项
                config_item = next((item for item in self.studio_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('company_ids', []))
                    target_ids.update(config_item.get('network_ids', []))

            # 检查 TMDb 的 company/network ID 是否在 target_ids 中
            has_company = any(cid in target_ids for cid in self.raw_metadata['company_ids'])
            has_network = any(nid in target_ids for nid in self.raw_metadata['network_ids'])

            if not (has_company or has_network): return False

        # 6. 关键词 (Keywords) - Label -> ID 匹配
        if rule.get('keywords'):
            target_ids = set()
            for label in rule['keywords']:
                config_item = next((item for item in self.keyword_map if item['label'] == label), None)
                if config_item:
                    target_ids.update(config_item.get('ids', []))

            # 兼容字符串/数字 ID
            tmdb_kw_ids = [int(k) for k in self.raw_metadata['keyword_ids']]
            target_ids_int = [int(k) for k in target_ids]

            if not any(kid in target_ids_int for kid in tmdb_kw_ids): return False

        # 7. 分级 (Rating) - Label 匹配
        if rule.get('ratings'):
            if self.raw_metadata['rating_label'] not in rule['ratings']: return False

        # 8. 年份 (Year) 
        year_min = rule.get('year_min')
        year_max = rule.get('year_max')
        
        if year_min or year_max:
            current_year = self.raw_metadata.get('year', 0)
            
            # 如果获取不到年份，且设置了年份限制，则视为不匹配
            if current_year == 0: return False
            
            if year_min and current_year < int(year_min): return False
            if year_max and current_year > int(year_max): return False

        # 9. 时长 (Runtime) 
        # 逻辑：电影取 runtime，剧集取 episode_run_time (列表取平均或第一个)
        run_min = rule.get('runtime_min')
        run_max = rule.get('runtime_max')

        if run_min or run_max:
            current_runtime = 0
            if self.media_type == 'movie':
                current_runtime = self.details.get('runtime') or 0
            else:
                # 剧集时长通常是一个列表 [45, 60]，取第一个作为参考
                runtimes = self.details.get('episode_run_time', [])
                if runtimes and len(runtimes) > 0:
                    current_runtime = runtimes[0]

            # 如果获取不到时长，且设置了限制，视为不匹配
            if current_runtime == 0: return False

            if run_min and current_runtime < int(run_min): return False
            if run_max and current_runtime > int(run_max): return False

        # 10. 评分 (Min Rating) - 数值比较
        if rule.get('min_rating') and float(rule['min_rating']) > 0:
            vote_avg = self.details.get('vote_average', 0)
            if vote_avg < float(rule['min_rating']):
                return False

        return True

    def get_target_cid(self):
        """遍历规则，返回命中的 CID。未命中返回 None"""
        for rule in self.rules:
            if not rule.get('enabled', True): continue
            if self._match_rule(rule):
                logger.info(f"  🎯 [115] 命中规则: {rule.get('name')} -> 目录: {rule.get('dir_name')}")
                return rule.get('cid')
        return None

    def _extract_video_info(self, filename):
        """
        从文件名提取视频信息 (来源 · 分辨率 · 编码 · 音频 · 制作组)
        参考格式: BluRay · 1080p · X264 · DDP 7.1 · CMCT
        """
        info_tags = []
        name_upper = filename.upper()

        # 1. 来源/质量 (Source)
        source = ""
        if re.search(r'REMUX', name_upper): source = 'Remux'
        elif re.search(r'BLU-?RAY|BD', name_upper): source = 'BluRay'
        elif re.search(r'WEB-?DL', name_upper): source = 'WEB-DL'
        elif re.search(r'WEB-?RIP', name_upper): source = 'WEBRip'
        elif re.search(r'HDTV', name_upper): source = 'HDTV'
        elif re.search(r'DVD', name_upper): source = 'DVD'

        # ★★★ 修复：UHD 识别 ★★★
        if 'UHD' in name_upper:
            if source == 'BluRay': source = 'UHD BluRay'
            elif not source: source = 'UHD'

        # 2. 特效 (Effect: HDR/DV)
        effect = ""
        is_dv = re.search(r'(?:^|[\.\s\-\_])(DV|DOVI|DOLBY\s?VISION)(?:$|[\.\s\-\_])', name_upper)
        is_hdr = re.search(r'(?:^|[\.\s\-\_])(HDR|HDR10\+?)(?:$|[\.\s\-\_])', name_upper)

        if is_dv and is_hdr: effect = "HDR DV"
        elif is_dv: effect = "DV"
        elif is_hdr: effect = "HDR"

        if source:
            info_tags.append(f"{source} {effect}".strip())
        elif effect:
            info_tags.append(effect)

        # 3. 分辨率 (Resolution)
        res_match = re.search(r'(2160|1080|720|480)[pP]', filename)
        if res_match:
            info_tags.append(res_match.group(0).lower())
        elif '4K' in name_upper:
            info_tags.append('2160p')

        # 4. 编码 (Codec)
        codec = ""
        if re.search(r'[HX]265|HEVC', name_upper): info_tags.append('H265')
        elif re.search(r'[HX]264|AVC', name_upper): info_tags.append('H264')
        elif re.search(r'AV1', name_upper): info_tags.append('AV1')
        elif re.search(r'MPEG-?2', name_upper): info_tags.append('MPEG2')
        # 比特率提取 (Bit Depth) 
        bit_depth = ""
        bit_match = re.search(r'(\d{1,2})BIT', name_upper)
        if bit_match:
            bit_depth = f"{bit_match.group(1)}bit" # 统一格式为小写 bit

        # 将编码和比特率组合，比如 "H265 10bit" 或单独 "H265"
        if codec:
            full_codec = f"{codec} {bit_depth}".strip()
            info_tags.append(full_codec)
        elif bit_depth:
            info_tags.append(bit_depth)

        # 5. 音频 (Audio) - ★★★ 修复重点 ★★★
        audio_info = []
        
        # (1) 优先匹配带数字的音轨 (2Audio, 3Audios) 并统一格式为 "xAudios"
        # 正则说明: 匹配边界 + 数字 + 空格(可选) + Audio + s(可选) + 边界
        num_audio_match = re.search(r'\b(\d+)\s?Audios?\b', name_upper, re.IGNORECASE)
        if num_audio_match:
            # 统一格式化为: 数字 + Audios (例如: 2Audios)
            audio_info.append(f"{num_audio_match.group(1)}Audios")
        else:
            # (2) 如果没有数字音轨，再匹配 Multi/Dual 等通用标签
            if re.search(r'\b(Multi|双语|多音轨|Dual-Audio)\b', name_upper, re.IGNORECASE):
                audio_info.append('Multi')

        # (3) 其他具体音频编码
        if re.search(r'ATMOS', name_upper): audio_info.append('Atmos')
        elif re.search(r'TRUEHD', name_upper): audio_info.append('TrueHD')
        elif re.search(r'DTS-?HD(\s?MA)?', name_upper): audio_info.append('DTS-HD')
        elif re.search(r'DTS', name_upper): audio_info.append('DTS')
        elif re.search(r'DDP|EAC3|DOLBY\s?DIGITAL\+', name_upper): audio_info.append('DDP')
        elif re.search(r'AC3|DD', name_upper): audio_info.append('AC3')
        elif re.search(r'AAC', name_upper): audio_info.append('AAC')
        elif re.search(r'FLAC', name_upper): audio_info.append('FLAC')
        elif re.search(r'OPUS', name_upper): audio_info.append('Opus')
        
        chan_match = re.search(r'\b(7\.1|5\.1|2\.0)\b', filename)
        if chan_match:
            audio_info.append(chan_match.group(1))
            
        if audio_info:
            info_tags.append(" ".join(audio_info))

        # 流媒体平台识别
        # 匹配 NF, AMZN, DSNP, HMAX, HULU, NETFLIX, DISNEY+, APPLETV+
        stream_match = re.search(r'\b(NF|AMZN|DSNP|HMAX|HULU|NETFLIX|DISNEY\+|APPLETV\+|B-GLOBAL)\b', name_upper)
        if stream_match:
            info_tags.append(stream_match.group(1))

        # 6. 发布组 (Release Group)
        group_found = False
        try:
            from tasks import helpers
            for group_name, patterns in helpers.RELEASE_GROUPS.items():
                for pattern in patterns:
                    try:
                        match = re.search(pattern, filename, re.IGNORECASE)
                        if match:
                            info_tags.append(match.group(0))
                            group_found = True
                            break
                    except: pass
                if group_found: break

            if not group_found:
                name_no_ext = os.path.splitext(filename)[0]
                match_suffix = re.search(r'-([a-zA-Z0-9]+)$', name_no_ext)
                if match_suffix:
                    possible_group = match_suffix.group(1)
                    if len(possible_group) > 2 and possible_group.upper() not in ['1080P', '2160P', '4K', 'HDR', 'H265', 'H264']:
                        info_tags.append(possible_group)
        except ImportError:
            pass

        return " · ".join(info_tags) if info_tags else ""

    def _rename_file_node(self, file_node, new_base_name, year=None, is_tv=False, original_title=None):
        original_name = file_node.get('fn') or file_node.get('n') or file_node.get('file_name', '')
        if '.' not in original_name: return original_name, None

        parts = original_name.rsplit('.', 1)
        name_body = parts[0]
        ext = parts[1].lower()

        # ... (保留原有的 is_sub 和 lang_suffix 逻辑) ...
        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']
        lang_suffix = ""
        if is_sub:
            lang_keywords = [
                'zh', 'cn', 'tw', 'hk', 'en', 'jp', 'kr',
                'chs', 'cht', 'eng', 'jpn', 'kor', 'fre', 'spa',
                'default', 'forced', 'tc', 'sc'
            ]
            sub_parts = name_body.split('.')
            if len(sub_parts) > 1:
                last_part = sub_parts[-1].lower()
                if last_part in lang_keywords or '-' in last_part:
                    lang_suffix = f".{sub_parts[-1]}"

            if not lang_suffix:
                match = re.search(r'(?:\.|-|_|\s)(chs|cht|zh-cn|zh-tw|eng|jpn|kor|tc|sc)(?:\.|-|_|$)', name_body, re.IGNORECASE)
                if match:
                    lang_suffix = f".{match.group(1)}"

        # ★ 应用文件重命名配置
        cfg = self.rename_config
        base_title = original_title if cfg.get('file_title_lang') == 'original' and original_title else new_base_name
        
        # ★ 修复：将年份直接拼接到片名后面，用空格隔开
        if cfg.get('file_year_en') and year:
            base_title = f"{base_title} ({year})"
            
        file_sep = cfg.get('file_sep', ' - ')

        tag_suffix = ""
        if cfg.get('file_params_en', True):
            try:
                search_name = original_name
                if is_sub:
                    if lang_suffix and name_body.endswith(lang_suffix):
                        clean_body = name_body[:-len(lang_suffix)]
                        search_name = f"{clean_body}.mkv"
                    else:
                        search_name = f"{name_body}.mkv"

                video_info = self._extract_video_info(search_name)
                if video_info:
                    if file_sep.strip() == '.':
                        tag_suffix = f".{video_info.replace(' · ', '.')}"
                    else:
                        tag_suffix = f" · {video_info}"
            except Exception as e:
                pass

        # 构建文件名主体
        name_parts = [base_title] # 此时 base_title 已经包含了年份
            
        tmdb_fmt = cfg.get('file_tmdb_fmt', 'none')
        if tmdb_fmt != 'none':
            name_parts.append(tmdb_fmt.replace('ID', str(self.tmdb_id)))

        if is_tv:
            pattern = r'(?:s|S)(\d{1,2})(?:e|E)(\d{1,2})|Ep?(\d{1,2})|第(\d{1,3})[集话]'
            match = re.search(pattern, original_name)
            if match:
                s, e, ep_only, zh_ep = match.groups()
                season_num = int(s) if s else 1
                episode_num = int(e) if e else (int(ep_only) if ep_only else int(zh_ep))

                s_str = f"S{season_num:02d}"
                e_str = f"E{episode_num:02d}"
                
                name_parts.append(f"{s_str}{e_str}")
                core_name = file_sep.join(name_parts)
                new_name = f"{core_name}{tag_suffix}{lang_suffix}.{ext}"
                return new_name, season_num
            else:
                return original_name, None
        else:
            core_name = file_sep.join(name_parts)
            new_name = f"{core_name}{tag_suffix}{lang_suffix}.{ext}"
            return new_name, None

    def _scan_files_recursively(self, cid, depth=0, max_depth=3):
        all_files = []
        if depth > max_depth: return []
        try:
            res = self.client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
            if res.get('data'):
                for item in res['data']:
                    # 兼容 OpenAPI 键名
                    fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if str(fc_val) == '1':
                        all_files.append(item)
                    elif str(fc_val) == '0':
                        sub_id = item.get('fid') or item.get('file_id')
                        sub_files = self._scan_files_recursively(sub_id, depth + 1, max_depth)
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
        """内部方法：拆解并独立整理合集包内的文件"""
        source_root_id = root_item.get('fid') or root_item.get('file_id')
        root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
        unidentified_cid = None 
        
        # 获取未识别目录 CID
        config = get_config()
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

        processed_count = 0
        try:
            sub_res = self.client.fs_files({'cid': source_root_id, 'limit': 100, 'record_open_time': 0, 'count_folders': 0})
            sub_items = sub_res.get('data', [])
            
            for sub_item in sub_items:
                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name')
                sub_id = sub_item.get('fid') or sub_item.get('file_id')
                
                # 1. 优先看子项自己有没有带 ID
                sub_tmdb_id, sub_type, sub_title = _identify_media_enhanced(sub_name)
                
                # 2. 模糊匹配 (仅当有官方合集列表时)
                if not sub_tmdb_id and collection_movies:
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
                        sub_tmdb_id = str(matched_movie['id'])
                        sub_type = 'movie'
                        sub_title = matched_movie.get('title')
                        logger.info(f"    ├─ 官方合集匹配成功: {sub_name} -> {sub_title} (ID:{sub_tmdb_id})")

                # ★★★ 3. 终极兜底：无官方合集时的文件名暴力解析搜索 ★★★
                if not sub_tmdb_id and not collection_movies:
                    # 去除常见的前缀广告，如 魅力社989pa.com- 或 [xxx]
                    clean_name = re.sub(r'^\[.*?\]|^.*?\.com-|^.*?\.[a-z]{2,3}-', '', sub_name, flags=re.IGNORECASE)
                    # 提取年份前面的部分作为标题
                    match_year = re.search(r'^(.*?)(?:\.|_|-|\s|\()+(19\d{2}|20\d{2})\b', clean_name)
                    if match_year:
                        guess_title = match_year.group(1).replace('.', ' ').strip()
                        guess_year = match_year.group(2)
                        logger.info(f"    ├─ 尝试暴力搜索: '{guess_title}' ({guess_year})")
                        try:
                            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
                            results = tmdb.search_media(query=guess_title, api_key=api_key, item_type='movie', year=guess_year)
                            if results and len(results) > 0:
                                sub_tmdb_id = str(results[0]['id'])
                                sub_type = 'movie'
                                sub_title = results[0].get('title') or results[0].get('name')
                                logger.info(f"    ├─ 暴力搜索成功: {sub_title} (ID:{sub_tmdb_id})")
                        except Exception as e:
                            logger.debug(f"    ├─ 暴力搜索出错: {e}")
                
                # 4. 执行单体整理 (递归调用新的 Organizer)
                if sub_tmdb_id:
                    logger.info(f"    ├─ 准备整理子项: {sub_name} -> ID:{sub_tmdb_id}")
                    try:
                        organizer = SmartOrganizer(self.client, sub_tmdb_id, sub_type, sub_title)
                        target_cid = organizer.get_target_cid()
                        if organizer.execute(sub_item, target_cid, delete_source=False):
                            processed_count += 1
                    except Exception as e:
                        logger.error(f"    ❌ 处理子项失败: {e}")
                else:
                    logger.warning(f"    ⚠️ 无法识别合集子项: {sub_name}，移入未识别。")
                    if unidentified_cid:
                        try: 
                            self.client.fs_move(sub_id, unidentified_cid)
                        except: pass
            
            # 拆解完毕，尝试删除空的合集文件夹
            try: 
                self.client.fs_delete([source_root_id])
                logger.info(f"  🧹 已清理拆解完毕的合集包空目录: {root_name}")
            except: pass
            
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"  ❌ 拆解合集包失败: {e}")
            return False

    def execute(self, root_item, target_cid, delete_source=True):
        title = self.details.get('title') or self.original_title
        original_title = self.details.get('original_title') or title
        date_str = self.details.get('date') or ''
        year = date_str[:4] if date_str else ''

        # ★ 应用主目录重命名配置
        cfg = self.rename_config
        base_title = original_title if cfg.get('main_title_lang') == 'original' else title
        safe_title = re.sub(r'[\\/:*?"<>|]', '', base_title).strip()
        
        std_root_name = safe_title
        if cfg.get('main_year_en', True) and year:
            std_root_name += f" ({year})"
            
        main_tmdb_fmt = cfg.get('main_tmdb_fmt', '{tmdb=ID}')
        if main_tmdb_fmt != 'none':
            std_root_name += f" {main_tmdb_fmt.replace('ID', str(self.tmdb_id))}"

        # 兼容 OpenAPI 键名
        root_name = root_item.get('fn') or root_item.get('n') or root_item.get('file_name', '未知')
        source_root_id = root_item.get('fid') or root_item.get('file_id')
        fc_val = root_item.get('fc') if root_item.get('fc') is not None else root_item.get('type')
        is_source_file = str(fc_val) == '1'
        dest_parent_cid = target_cid if (target_cid and str(target_cid) != '0') else (root_item.get('pid') or root_item.get('parent_id') or root_item.get('cid'))

        # =================================================================
        # ★★★ 新增：在底层拦截 NULLBR 传来的合集包 ★★★
        # =================================================================
        if not is_source_file and re.search(r'(合集|部曲|系列|Collection|Pack|Trilogy|Quadrilogy|\d+-\d+)', root_name, re.IGNORECASE):
            logger.info(f"  📦 [底层拦截] 检测到疑似合集包: {root_name}，正在验证...")
            collection_movies = []
            
            # 1. 检查当前传入的 tmdb_id 是否本身就是合集 ID
            try:
                res_c = tmdb.get_collection_details(int(self.tmdb_id), self.api_key)
                if res_c and 'parts' in res_c:
                    collection_movies = res_c['parts']
            except: pass
            
            # 2. 检查当前传入的电影是否属于某个合集
            if not collection_movies and self.media_type == 'movie':
                try:
                    c_id = None
                    # 优先从已获取的元数据中取
                    if hasattr(self, 'raw_metadata') and self.raw_metadata and self.raw_metadata.get('belongs_to_collection'):
                        c_id = self.raw_metadata['belongs_to_collection']['id']
                    else:
                        res_m = tmdb.get_movie_details(int(self.tmdb_id), self.api_key)
                        if res_m and res_m.get('belongs_to_collection'):
                            c_id = res_m['belongs_to_collection']['id']
                            
                    if c_id:
                        res_c = tmdb.get_collection_details(int(c_id), self.api_key)
                        if res_c and 'parts' in res_c:
                            collection_movies = res_c['parts']
                except Exception as e:
                    logger.debug(f"    ├─ 验证合集失败: {e}")

            if collection_movies:
                logger.info(f"  📦 确认为官方合集包，包含 {len(collection_movies)} 部电影，启动精确拆解模式...")
            else:
                logger.info(f"  📦 未找到官方合集信息 (可能是民间自制包)，启动基于文件名的暴力拆解模式...")
                
            return self._execute_collection_breakdown(root_item, collection_movies)

        config = get_config()
        configured_exts = config.get(constants.CONFIG_OPTION_115_EXTENSIONS, [])
        allowed_exts = set(e.lower() for e in configured_exts)
        known_video_exts = {'mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg'}
        MIN_VIDEO_SIZE = 10 * 1024 * 1024

        logger.info(f"  🚀 [115] 开始整理: {root_name} -> {std_root_name}")

        final_home_cid = P115CacheManager.get_cid(dest_parent_cid, std_root_name)

        if final_home_cid:
            logger.info(f"  ⚡ [缓存命中] 主目录: {std_root_name}")
        else:
            mk_res = self.client.fs_mkdir(std_root_name, dest_parent_cid)
            if mk_res.get('state'):
                final_home_cid = mk_res.get('cid')
                P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                logger.info(f"  🆕 创建新主目录并缓存: {std_root_name}")
            else:
                try:
                    search_res = self.client.fs_files({'cid': dest_parent_cid, 'search_value': std_root_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                    if search_res.get('data'):
                        for item in search_res['data']:
                            item_name = item.get('fn') or item.get('n') or item.get('file_name')
                            item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                            if item_name == std_root_name and str(item_fc) == '0':
                                final_home_cid = item.get('fid') or item.get('file_id')
                                P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                                logger.info(f"  📂 成功查找到已存在主目录并永久缓存: {std_root_name}")
                                break
                except Exception as e:
                    logger.warning(f"  ⚠️ 115模糊查找异常: {e}")

                if not final_home_cid:
                    logger.warning(f"  ⚠️ 115搜索失效，启动全量遍历查找老目录: '{std_root_name}' ...")
                    offset = 0
                    limit = 1000
                    while True:
                        try:
                            res = self.client.fs_files({'cid': dest_parent_cid, 'limit': limit, 'offset': offset, 'type': 0, 'record_open_time': 0, 'count_folders': 0})
                            data = res.get('data', [])
                            if not data: break 
                            
                            for item in data:
                                item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                                if item_name == std_root_name and str(item_fc) == '0':
                                    final_home_cid = item.get('fid') or item.get('file_id')
                                    P115CacheManager.save_cid(final_home_cid, dest_parent_cid, std_root_name)
                                    logger.info(f"  📂 成功查找到已存在主目录并永久缓存: {std_root_name}")
                                    break
                                    
                            if final_home_cid: break 
                            offset += limit 
                        except Exception as e:
                            logger.error(f"遍历查找失败: {e}")
                            break

        if not final_home_cid:
            logger.error(f"  ❌ 无法获取或创建目标目录 (已尝试所有手段)")
            return False

        candidates = []
        if is_source_file:
            candidates.append(root_item)
        else:
            candidates = self._scan_files_recursively(source_root_id, max_depth=3)

        if not candidates: return True

        moved_count = 0
        for file_item in candidates:
            # 兼容 OpenAPI 键名
            fid = file_item.get('fid') or file_item.get('file_id')
            file_name = file_item.get('fn') or file_item.get('n') or file_item.get('file_name', '')
            ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
            if self._is_junk_file(file_name): continue
            if ext not in allowed_exts: continue
            
            file_size = _parse_115_size(file_item.get('fs') or file_item.get('size'))
            if ext in known_video_exts and 0 < file_size < MIN_VIDEO_SIZE: continue

            new_filename, season_num = self._rename_file_node(
                file_item, safe_title, year=year, is_tv=(self.media_type=='tv')
            )

            new_filename, season_num = self._rename_file_node(
                file_item, safe_title, year=year, is_tv=(self.media_type=='tv'), original_title=original_title
            )

            real_target_cid = final_home_cid
            if self.media_type == 'tv' and season_num is not None:
                # ★ 应用季目录重命名配置
                season_fmt = cfg.get('season_fmt', 'Season {02}')
                if '{02}' in season_fmt:
                    s_name = season_fmt.replace('{02}', f"{season_num:02d}")
                else:
                    s_name = season_fmt.replace('{1}', f"{season_num}")
                    
                s_cid = P115CacheManager.get_cid(final_home_cid, s_name)
                
                if s_cid:
                    logger.info(f"  ⚡ [缓存命中] 季目录: {std_root_name} - {s_name}")
                    real_target_cid = s_cid
                else:
                    s_mk = self.client.fs_mkdir(s_name, final_home_cid)
                    s_cid = s_mk.get('cid') if s_mk.get('state') else None
                    
                    if not s_cid: 
                        try:
                            s_search = self.client.fs_files({'cid': final_home_cid, 'search_value': s_name, 'limit': 1150, 'record_open_time': 0, 'count_folders': 0})
                            for item in s_search.get('data', []):
                                item_name = item.get('fn') or item.get('n') or item.get('file_name')
                                item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                                if item_name == s_name and str(item_fc) == '0':
                                    s_cid = item.get('fid') or item.get('file_id')
                                    break
                        except: pass
                    
                    if s_cid:
                        P115CacheManager.save_cid(s_cid, final_home_cid, s_name)
                        logger.info(f"  🆕 创建季目录并缓存: {std_root_name} - {s_name}")
                        real_target_cid = s_cid

            if new_filename != file_name:
                ren_res = self.client.fs_rename((fid, new_filename))
                if ren_res.get('state'):
                    logger.info(f"  ✏️ [重命名] {file_name} -> {new_filename}")
                else:
                    logger.warning(f"  ⚠️ [重命名失败] {file_name} -> {new_filename}, 原因: {ren_res.get('error_msg', ren_res)}")

            move_res = self.client.fs_move(fid, real_target_cid)
            if move_res.get('state'):
                if self.media_type == 'tv' and season_num is not None:
                    logger.info(f"  📁 [移动] {file_name} -> {std_root_name} - {s_name}")
                else:
                    logger.info(f"  📁 [移动] {file_name} -> {std_root_name}")
                moved_count += 1

                # 兼容 OpenAPI 键名
                pick_code = file_item.get('pc') or file_item.get('pick_code')
                file_sha1 = file_item.get('sha1') or file_item.get('sha')
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

                        # ==================================================
                        # ★ 动态计算并缓存分类路径 (category_path)
                        # ==================================================
                        category_rule = next((r for r in self.rules if str(r.get('cid')) == str(target_cid)), None)
                        relative_category_path = "未识别"
                        
                        if category_rule:
                            if 'category_path' in category_rule and category_rule['category_path']:
                                relative_category_path = category_rule['category_path']
                                logger.debug(f"  ⚡ [规则缓存] 命中分类路径: '{relative_category_path}'")
                            else:
                                # 缓存未命中，动态计算 (完全对齐 routes/p115.py 的逻辑)
                                logger.info(f"  🔍 [规则缓存] 未命中路径缓存，正在向 115 请求计算层级...")
                                media_root_cid = str(config.get(constants.CONFIG_OPTION_115_MEDIA_ROOT_CID, '0'))
                                try:
                                    dir_info = self.client.fs_files({'cid': target_cid, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
                                    path_nodes = dir_info.get('path', [])
                                    start_idx = 0
                                    found_root = False
                                    
                                    if media_root_cid == '0':
                                        # ★ 修复 0 层级 Bug：115 的根目录永远在 index 0，所以从 1 开始切片是绝对正确的。
                                        # 但如果分类目录本身就是根目录，这里需要特殊处理
                                        if str(target_cid) == '0':
                                            start_idx = 0
                                        else:
                                            start_idx = 1 
                                        found_root = True
                                    else:
                                        for i, node in enumerate(path_nodes):
                                            node_cid = str(node.get('cid') or node.get('file_id'))
                                            if node_cid == media_root_cid:
                                                start_idx = i + 1
                                                found_root = True
                                                break
                                    
                                    if found_root and start_idx < len(path_nodes):
                                        rel_segments = []
                                        for n in path_nodes[start_idx:]:
                                            node_name = n.get('file_name') or n.get('fn') or n.get('name') or n.get('n')
                                            if node_name:
                                                rel_segments.append(str(node_name).strip())
                                        relative_category_path = "/".join(rel_segments) if rel_segments else category_rule.get('dir_name', '未识别')
                                    else:
                                        relative_category_path = category_rule.get('dir_name', '未识别')
                                        
                                    # 更新内存规则并持久化到数据库
                                    category_rule['category_path'] = relative_category_path
                                    settings_db.save_setting(constants.DB_KEY_115_SORTING_RULES, self.rules)
                                    logger.info(f"  💾 [规则缓存] 已动态计算并永久保存路径: '{relative_category_path}'")
                                    
                                except Exception as e:
                                    logger.warning(f"  ⚠️ 动态计算分类路径失败: {e}")
                                    relative_category_path = category_rule.get('dir_name', '未识别')

                        if self.media_type == 'tv' and season_num is not None:
                            local_dir = os.path.join(local_root, relative_category_path, std_root_name, s_name)
                        else:
                            local_dir = os.path.join(local_root, relative_category_path, std_root_name)
                        
                        os.makedirs(local_dir, exist_ok=True) 

                        # 实时将计算好的路径写入数据库缓存，以便后续快速访问
                        try:
                            # 1. 实时更新主目录的 local_path
                            main_folder_path = os.path.join(relative_category_path, std_root_name)
                            P115CacheManager.update_local_path(final_home_cid, main_folder_path)
                            
                            # 2. 如果是剧集，实时更新季目录的 local_path
                            if self.media_type == 'tv' and season_num is not None:
                                season_folder_path = os.path.join(main_folder_path, s_name)
                                # 此时 real_target_cid 就是季目录的 CID
                                P115CacheManager.update_local_path(real_target_cid, season_folder_path)
                        except Exception as e:
                            logger.warning(f"  ⚠️ 实时更新目录路径缓存失败: {e}") 

                        ext = new_filename.split('.')[-1].lower() if '.' in new_filename else ''
                        is_video = ext in known_video_exts
                        is_sub = ext in ['srt', 'ass', 'ssa', 'sub', 'vtt', 'sup']

                        if is_video:
                            strm_filename = os.path.splitext(new_filename)[0] + ".strm"
                            strm_filepath = os.path.join(local_dir, strm_filename)
                            # ★★★ 判断是否是否挂载 ★★★
                            if not etk_url.startswith('http'):
                                mount_prefix = etk_url
                                if self.media_type == 'tv' and season_num is not None:
                                    mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, s_name, new_filename)
                                else:
                                    mount_path = os.path.join(mount_prefix, relative_category_path, std_root_name, new_filename)
                                strm_content = mount_path.replace('\\', '/')
                                logger.debug(f"  💿 [挂载模式] 生成 STRM: {strm_content}")
                            else:
                                # 默认的 ETK 302 直链模式
                                strm_content = f"{etk_url}/api/p115/play/{pick_code}"
                            
                            with open(strm_filepath, 'w', encoding='utf-8') as f:
                                f.write(strm_content)
                            logger.info(f"  📝 STRM 已生成 -> {strm_filename}")

                            if not file_sha1 and fid:
                                try:
                                    info_res = self.client.fs_get_info(fid)
                                    if info_res.get('state') and info_res.get('data'):
                                        file_sha1 = info_res['data'].get('sha1')
                                        if file_sha1:
                                            logger.debug(f"  ➜ [API补充] 成功通过详情接口获取到 SHA1: {file_sha1}")
                                except Exception as e_info:
                                    logger.warning(f"  ⚠️ 调用详情接口获取 SHA1 失败: {e_info}")

                            # 存入缓存表
                            if pick_code and fid:
                                P115CacheManager.save_file_cache(fid, real_target_cid, new_filename, sha1=file_sha1, pick_code=pick_code)
                                
                            # 实时跨号秒传
                            if file_sha1:
                                try:
                                    with get_db_connection() as conn:
                                        with conn.cursor() as cursor:
                                            # ★ 极速指纹库查询
                                            cursor.execute("""
                                                SELECT mediainfo_json FROM p115_mediainfo_cache 
                                                WHERE sha1 = %s LIMIT 1
                                            """, (file_sha1,))
                                            row = cursor.fetchone()
                                            if row and row['mediainfo_json']:
                                                raw_info = row['mediainfo_json']
                                                if isinstance(raw_info, list) and len(raw_info) > 0:
                                                    mediainfo_path = os.path.join(local_dir, os.path.splitext(new_filename)[0] + "-mediainfo.json")
                                                    
                                                    # ★★★ 新增：判断文件是否存在，不存在才写入，防止更新时间戳触发监控死循环 ★★★
                                                    if not os.path.exists(mediainfo_path):
                                                        with open(mediainfo_path, 'w', encoding='utf-8') as f_json:
                                                            json.dump(raw_info, f_json, ensure_ascii=False)
                                                        
                                                        # 更新命中次数
                                                        cursor.execute("UPDATE p115_mediainfo_cache SET hit_count = hit_count + 1 WHERE sha1 = %s", (file_sha1,))
                                                        conn.commit()
                                                        
                                                        logger.info(f"  ⚡ [媒体信息缓存] 匹配到相同 SHA1，极速生成媒体信息: {os.path.basename(mediainfo_path)}")
                                                    else:
                                                        logger.debug(f"  ⚡ [媒体信息缓存] 本地已存在媒体信息文件，跳过生成: {os.path.basename(mediainfo_path)}")
                                except Exception as e_sha1:
                                    logger.warning(f"  ⚠️ 尝试秒传媒体信息失败: {e_sha1}")
                            
                        elif is_sub:
                            if config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True):
                                sub_filepath = os.path.join(local_dir, new_filename)
                                if not os.path.exists(sub_filepath):
                                    try:
                                        logger.info(f"  ⬇️ [字幕下载] 正在向 115 拉取外挂字幕: {new_filename} ...")
                                        url_obj = self.client.download_url(pick_code, user_agent="Mozilla/5.0")
                                        dl_url = str(url_obj)
                                        if dl_url:
                                            import requests
                                            headers = {
                                                "User-Agent": "Mozilla/5.0",
                                                "Cookie": P115Service.get_cookies()
                                            }
                                            resp = requests.get(dl_url, stream=True, timeout=30, headers=headers)
                                            resp.raise_for_status()
                                            with open(sub_filepath, 'wb') as f:
                                                for chunk in resp.iter_content(chunk_size=8192):
                                                    f.write(chunk)
                                            logger.info(f"  ✅ [字幕下载] 下载完成！")
                                    except Exception as e:
                                        logger.error(f"  ❌ 下载字幕失败: {e}")
                        
                    except Exception as e:
                        logger.error(f"  ❌ 生成 STRM 文件失败: {e}", exc_info=True)
            else:
                err_msg = str(move_res.get('error_msg', move_res))
                logger.error(f"  ❌ [移动失败] {file_name} -> 目标CID:{real_target_cid}, 原因: {err_msg}")
                
                # ★ 智能自愈：如果是目标目录不存在，说明缓存失效了，立刻清理本地缓存！
                if '不存在' in err_msg or move_res.get('code') in [20004, 70004]:
                    logger.warning(f"  🧹 检测到目标目录在网盘中已不存在，正在清理失效缓存: CID {real_target_cid}")
                    P115CacheManager.delete_cid(real_target_cid)

        if delete_source and not is_source_file and moved_count > 0:
            self.client.fs_delete([source_root_id])
            logger.info(f"  🧹 已清理空目录")

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

def _identify_media_enhanced(filename, forced_media_type=None):
    """
    增强识别逻辑：
    1. 支持多种 TMDb ID 标签格式: {tmdb=xxx}
    2. 支持标准命名格式: Title (Year)
    3. 接收外部强制指定的类型 (forced_media_type)，不再轮询猜测
    
    返回: (tmdb_id, media_type, title) 或 (None, None, None)
    """
    tmdb_id = None
    media_type = 'movie' # 默认
    title = filename
    
    # 1. 优先提取 TMDb ID 标签 (最稳)
    match_tag = re.search(r'\{?tmdb(?:id)?[=\-](\d+)\}?', filename, re.IGNORECASE)
    
    if match_tag:
        tmdb_id = match_tag.group(1)
        
        # 如果外部指定了类型，直接用；否则看文件名特征
        if forced_media_type:
            media_type = forced_media_type
        elif re.search(r'(?:S\d{1,2}|E\d{1,2}|第\d+季|Season)', filename, re.IGNORECASE):
            media_type = 'tv'
        
        # 提取标题
        clean_name = re.sub(r'\{?tmdb(?:id)?[=\-]\d+\}?', '', filename, flags=re.IGNORECASE).strip()
        match_title = re.match(r'^(.+?)\s*[\(\[]\d{4}[\)\]]', clean_name)
        if match_title:
            title = match_title.group(1).strip()
        else:
            title = clean_name
            
        return tmdb_id, media_type, title

    # 2. 其次提取标准格式 Title (Year)
    match_std = re.match(r'^(.+?)\s+[\(\[](\d{4})[\)\]]', filename)
    if match_std:
        name_part = match_std.group(1).strip()
        year_part = match_std.group(2)
        
        # === 关键修正：类型判断逻辑 ===
        if forced_media_type:
            # 如果外部透视过目录，确定是 TV，直接信赖
            media_type = forced_media_type
        else:
            # 否则才根据文件名特征判断
            if re.search(r'(?:S\d{1,2}|E\d{1,2}|第\d+季|Season)', filename, re.IGNORECASE):
                media_type = 'tv'
            else:
                media_type = 'movie'
            
        # 尝试通过 TMDb API 确认 ID
        try:
            api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            if api_key:
                # 精准搜索，不轮询，不瞎猜
                results = tmdb.search_media(
                    query=name_part, 
                    api_key=api_key, 
                    item_type=media_type, 
                    year=year_part
                )
                
                if results and len(results) > 0:
                    best = results[0]
                    return best['id'], media_type, (best.get('title') or best.get('name'))
                else:
                    logger.warning(f"  ⚠️ TMDb 未找到资源: {name_part} ({year_part}) 类型: {media_type}")

        except Exception as e:
            pass

    return None, None, None


def task_scan_and_organize_115(processor=None):
    """
    [任务链] 主动扫描 115 待整理目录
    - 识别成功 -> 归类到目标目录
    - 识别失败 -> 移动到 '未识别' 目录
    ★ 修复：增加子文件探测逻辑，防止剧集文件夹因命名不规范被误判为电影
    """
    logger.info("=== 开始执行 115 待整理目录扫描 ===")

    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    config = get_config()
    cid_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID)
    save_val = config.get(constants.CONFIG_OPTION_115_SAVE_PATH_NAME, '待整理')
    enable_organize = config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)

    if not cid_val or str(cid_val) == '0':
        logger.error("  ⚠️ 未配置待整理目录 (CID)，跳过。")
        return
    if not enable_organize:
        logger.warning("  ⚠️ 未开启智能整理开关，仅扫描不处理。")
        return
    current_time = time.time()
    try:
        save_cid = int(cid_val)
        save_name = str(save_val)

        # 1. 准备 '未识别' 目录
        unidentified_folder_name = "未识别"
        unidentified_cid = None
        try:
            # ★ 优化：纯读模式，不统计文件夹
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

        logger.info(f"  🔍 正在扫描目录: {save_name} ...")
        
        # =================================================================
        # ★★★ 主目录扫描：纯读模式 + 修正排序字段 + 退避重试 ★★★
        # =================================================================
        res = {}
        for retry in range(3):
            try:
                res = client.fs_files({
                    'cid': save_cid, 'limit': 50, 'o': 'user_utime', 'asc': 0,
                    'record_open_time': 0, 'count_folders': 0
                })
                break 
            except Exception as e:
                if '405' in str(e) or 'Method Not Allowed' in str(e):
                    logger.warning(f"  ⚠️ 扫描主目录触发 115 风控拦截 (405)，休眠 5 秒后重试 ({retry+1}/3)...")
                else:
                    raise

        if not res.get('data'):
            logger.info(f"  📂 [{save_name}] 目录为空或获取失败。")
            return

        processed_count = 0
        moved_to_unidentified = 0

        for item in res['data']:
            # 兼容 OpenAPI 键名
            name = item.get('fn') or item.get('n') or item.get('file_name')
            if not name: continue
            item_id = item.get('fid') or item.get('file_id')
            fc_val = item.get('fc') if item.get('fc') is not None else item.get('type')
            is_folder = str(fc_val) == '0'

            if str(item_id) == str(unidentified_cid) or name == unidentified_folder_name:
                continue

            forced_type = None
            peek_failed = False

            if is_folder:
                # =================================================================
                # ★★★ 子目录透视：扫描前20个项目(包含文件和文件夹)来判断是否为剧集 ★★★
                # =================================================================
                for retry in range(2):
                    try:
                        sub_res = client.fs_files({
                            'cid': item_id, 'limit': 20, # ★ 修复1: 使用 item_id 作为目标目录
                            'record_open_time': 0, 'count_folders': 0
                            # ★ 修复2: 移除 'nf': 1，允许读取视频文件，兼容没有季文件夹的扁平剧集
                        })
                        if sub_res.get('data'):
                            for sub_item in sub_res['data']:
                                # ★ 修复3: 兼容 OpenAPI 键名
                                sub_name = sub_item.get('fn') or sub_item.get('n') or sub_item.get('file_name', '')
                                if re.search(r'(Season\s?\d+|S\d+|Ep?\d+|第\d+季)', sub_name, re.IGNORECASE):
                                    forced_type = 'tv'
                                    break
                        peek_failed = False
                        break
                    except Exception as e:
                        if '405' in str(e) or 'Method Not Allowed' in str(e):
                            logger.warning(f"  ⚠️ 透视目录 '{name}' 触发风控，休眠 3 秒后重试 ({retry+1}/2)...")
                            peek_failed = True
                        else:
                            peek_failed = True
                            break

            if peek_failed:
                logger.warning(f"  ⏭️ 透视 '{name}' 连续失败，为防误判跳过本次识别。")
                continue

            tmdb_id, media_type, title = _identify_media_enhanced(name, forced_media_type=forced_type)
            
            if tmdb_id:
                logger.info(f"  ➜ 识别成功: {name} -> ID:{tmdb_id} ({media_type})")
                try:
                    organizer = SmartOrganizer(client, tmdb_id, media_type, title)
                    target_cid = organizer.get_target_cid()
                    
                    if organizer.execute(item, target_cid, delete_source=False):
                        processed_count += 1
                        
                        if is_folder:
                            update_time_str = item.get('upt') or '0'
                            try:
                                update_time = int(update_time_str)
                            except:
                                update_time = current_time
                                
                            if (current_time - update_time) > 86400:
                                logger.info(f"  🧹 [兜底清理] 清理已过期(>24h)的残留目录: {name}")
                                client.fs_delete([item_id])

                except Exception as e:
                    logger.error(f"  ❌ 整理出错: {e}")
            else:
                if unidentified_cid:
                    try:
                        client.fs_move(item_id, unidentified_cid)
                        moved_to_unidentified += 1
                    except: pass

        logger.info(f"=== 扫描结束，成功归类 {processed_count} 个，移入未识别 {moved_to_unidentified} 个 ===")

    except Exception as e:
        logger.error(f"  ⚠️ 115 扫描任务异常: {e}", exc_info=True)

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

    raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
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
    增量生成 STRM 与 同步字幕
    利用 115 分类目录级全局拉取 (type=4/1) + 本地 DB 目录树缓存，实现秒级增量同步！
    """
    config = get_config()
    download_subs = config.get(constants.CONFIG_OPTION_115_DOWNLOAD_SUBS, True)
    enable_cleanup = config.get(constants.CONFIG_OPTION_115_LOCAL_CLEANUP, False)
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

    # ★ 修复：让前端第一时间收到启动消息
    start_msg = "=== 🚀 开始增量同步 STRM 与 字幕 ===" if download_subs else "=== 🚀 开始增量同步 STRM (跳过字幕) ==="
    if enable_cleanup: start_msg += " [已开启本地清理]"
    update_progress(0, start_msg)
    
    try:
        import task_manager
    except ImportError:
        task_manager = None

    def update_progress(prog, msg):
        if task_manager: task_manager.update_status_from_thread(prog, msg)
        logger.info(msg)

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

    raw_rules = settings_db.get_setting(constants.DB_KEY_115_SORTING_RULES)
    if not raw_rules: 
        update_progress(100, "错误：未配置分类规则！")
        return
    rules = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules

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

    # =================================================================
    # ★ 核心升级：动态智能路径推导器 (带内存与 DB 双重缓存)
    # =================================================================
    pid_path_cache = {} # 内存缓存，防止同一个文件夹重复请求 115

    def get_local_path_for_pid(pid, target_cid, base_category_path):
        pid = str(pid)
        target_cid = str(target_cid)
        
        # 1. 如果文件直接在分类主目录下，直接返回分类路径
        if pid == target_cid:
            return base_category_path
            
        # 2. 查内存缓存 (极速)
        if pid in pid_path_cache:
            return pid_path_cache[pid]
            
        # 3. 查本地数据库缓存 (直接命中)
        db_path = P115CacheManager.get_local_path(pid)
        if db_path:
            pid_path_cache[pid] = db_path
            return db_path
            
        # =================================================================
        # ★ 3.5 找他爹要路径 (神级优化：如果自己没路径，但数据库里有爹的记录)
        # =================================================================
        node_info = P115CacheManager.get_node_info(pid)
        if node_info:
            parent_id = node_info['parent_id']
            node_name = node_info['name']
            
            # 递归找爹的路径 (利用内存和DB缓存，瞬间返回)
            parent_path = get_local_path_for_pid(parent_id, target_cid, base_category_path)
            if parent_path:
                # 爹有路径，直接拼上自己的名字！
                final_path = os.path.join(parent_path, node_name)
                
                # 存入内存，并顺手更新自己的数据库记录，下次连爹都不用找了！
                pid_path_cache[pid] = final_path
                P115CacheManager.update_local_path(pid, final_path)
                
                logger.debug(f"  👨‍👦 成功通过父目录推导路径: {final_path}")
                return final_path

        # 4. 终极兜底：向 115 问路！(100% 准确，且每个文件夹只会问一次)
        try:
            dir_info = client.fs_files({'cid': pid, 'limit': 1, 'record_open_time': 0})
            path_nodes = dir_info.get('path', [])
            
            start_idx = -1
            # 在路径链路中寻找 target_cid (分类目录)
            for i, node in enumerate(path_nodes):
                if str(node.get('cid') or node.get('file_id')) == target_cid:
                    start_idx = i + 1
                    break
            
            if start_idx != -1:
                sub_folders = []
                for n in path_nodes[start_idx:]:
                    node_name = n.get('file_name') or n.get('fn') or n.get('name') or n.get('n')
                    if node_name: 
                        sub_folders.append(str(node_name).strip())
                
                # 拼接出最终的本地相对路径
                final_path = os.path.join(base_category_path, *sub_folders) if sub_folders else base_category_path
                
                # 存入内存和数据库，下次秒出！
                pid_path_cache[pid] = final_path
                
                # 顺手把这个目录的结构存入数据库，防止外键报错
                P115CacheManager.save_cid(pid, path_nodes[-2].get('cid') if len(path_nodes)>1 else '0', path_nodes[-1].get('file_name'))
                P115CacheManager.update_local_path(pid, final_path)
                
                logger.info(f"  🔍 [动态推导] 缓存新路径: {final_path}")
                return final_path
            else:
                logger.warning(f"  ⚠️ 路径异常: 文件夹 {pid} 不在分类 {target_cid} 之下！")
        except Exception as e:
            logger.warning(f"  ⚠️ 向 115 动态查询路径失败 (pid={pid}): {e}")
            
        return None

    # =================================================================
    # 阶段 2: 分类目录级全局拉取 (耗时: 秒级/分钟级)
    # =================================================================
    valid_local_files = set()
    files_generated = 0
    subs_downloaded = 0
    
    total_targets = len(target_cids)
    api_fatal_error = False 
    
    for idx, target_cid in enumerate(target_cids):
        category_name = cid_to_rel_path.get(target_cid, "未知分类")
        base_prog = 10 + int((idx / total_targets) * 80)
        update_progress(base_prog, f"  🌐 正在全局拉取分类 [{category_name}] 下的所有文件...")
        
        # ★ 核心修改：将拉取任务拆分为“按类型拉取视频”和“按关键词搜索字幕”
        pull_tasks = [{"name": "视频", "is_search": False, "params": {'type': 4}}]
        
        if download_subs:
            # 使用官方搜索接口精准打击！
            for ext in ['srt', 'ass', 'ssa', 'sub', 'vtt']:
                pull_tasks.append({"name": f"字幕(.{ext})", "is_search": True, "params": {'search_value': f'.{ext}'}})
        
        for task in pull_tasks:
            task_name = task["name"]
            is_search = task["is_search"]
            base_params = task["params"]
            
            offset = 0
            limit = 1000
            page = 1
            
            while True:
                if processor and getattr(processor, 'is_stop_requested', lambda: False)(): return
                
                try:
                    req_payload = {'cid': target_cid, 'limit': limit, 'offset': offset}
                    req_payload.update(base_params)
                    
                    # ★ 区分调用：搜索走官方 fs_search，拉取走 fs_files
                    if is_search and hasattr(client, 'fs_search'):
                        res = client.fs_search(req_payload)
                    else:
                        req_payload['record_open_time'] = 0
                        res = client.fs_files(req_payload)
                    
                    # 绝对熔断保护
                    if not res.get('state'):
                        logger.error(f"  🛑 [致命错误] 115 API 返回失败: {res.get('error_msg', res)}，触发熔断保护！")
                        api_fatal_error = True
                        break

                    data = res.get('data', [])
                    if not data: break
                    
                    update_progress(base_prog, f"  ➜ [{category_name}] - [{task_name}] 获取第 {page} 页 ({len(data)} 个文件)...")
                    
                    for item in data:
                        # 兼容 OpenAPI 键名
                        name = item.get('fn') or item.get('n') or item.get('file_name', '')
                        ext = name.split('.')[-1].lower() if '.' in name else ''
                        if ext not in allowed_exts: continue
                        
                        pc = item.get('pc') or item.get('pick_code')
                        pid = item.get('pid') or item.get('cid') or item.get('parent_id')
                        fid = item.get('fid') or item.get('file_id') 
                        file_sha1 = item.get('sha1') or item.get('sha')
                        
                        if not pc or not pid or not fid: continue

                        # 如果列表没给 SHA1，先查本地缓存，没有再调用详情接口硬抠！ 
                        if not file_sha1:
                            # 1. 优先查本地数据库缓存 (极速)
                            cached_sha1 = P115CacheManager.get_file_sha1(fid)
                            if cached_sha1:
                                file_sha1 = cached_sha1
                            else:
                                # 2. 缓存没有，再向 115 发起网络请求
                                try:
                                    logger.info(f"  ➜ 正在通过 API 提取 SHA1: {name}")
                                    info_res = client.fs_get_info(fid)
                                    if info_res.get('state') and info_res.get('data'):
                                        file_sha1 = info_res['data'].get('sha1')
                                except Exception:
                                    pass

                        # 存入缓存表
                        P115CacheManager.save_file_cache(fid, pid, name, sha1=file_sha1, pick_code=pc)
                        
                        # ★ 智能推导本地路径 (传入 pid, 当前分类 cid, 当前分类的基准路径)
                        rel_dir = get_local_path_for_pid(pid, target_cid, category_name)
                        if not rel_dir: 
                            logger.debug(f"  ⚠️ 无法推导路径，跳过文件: {name} (pid: {pid})")
                            continue 
                            
                        current_local_path = os.path.join(local_root, rel_dir)
                        os.makedirs(current_local_path, exist_ok=True)
                        
                        # 处理视频 STRM
                        if ext in known_video_exts:
                            strm_name = os.path.splitext(name)[0] + ".strm"
                            strm_path = os.path.join(current_local_path, strm_name)
                            
                            # ★★★ 判断是否命中挂载扩展名 ★★★
                            if not etk_url.startswith('http'):
                                mount_prefix = etk_url
                                # 在这个函数里，rel_dir 已经是计算好的完整相对路径了，直接拼上文件名 name 即可
                                mount_path = os.path.join(mount_prefix, rel_dir, name)
                                content = mount_path.replace('\\', '/')
                                logger.debug(f"  💿 [挂载模式] 生成 STRM: {content}")
                            else:
                                # 默认的 ETK 302 直链模式
                                content = f"{etk_url}/api/p115/play/{pc}"
                            
                            # ★ 优化：在写入前先判断文件存不存在
                            is_new_file = not os.path.exists(strm_path)
                            need_write = True
                            
                            if not is_new_file:
                                try:
                                    with open(strm_path, 'r', encoding='utf-8') as f:
                                        old_content = f.read().strip()
                                        if old_content == content: 
                                            need_write = False
                                except Exception as e: pass
                                        
                            if need_write:
                                with open(strm_path, 'w', encoding='utf-8') as f: 
                                    f.write(content)
                                
                                # ★ 优化：准确打印日志
                                if is_new_file:
                                    logger.debug(f"  📝 [新增] 生成 STRM: {strm_name}")
                                else:
                                    logger.debug(f"  🔄 [更新] 覆盖 STRM: {strm_name}")
                                    
                                files_generated += 1
                                
                            valid_local_files.add(os.path.abspath(strm_path))

                            # ★★★ 秒传生成媒体信息 JSON ★★★
                            file_sha1 = item.get('sha1')
                            if file_sha1:
                                try:
                                    with get_db_connection() as conn:
                                        with conn.cursor() as cursor:
                                            cursor.execute("""
                                                SELECT mediainfo_json FROM p115_mediainfo_cache 
                                                WHERE sha1 = %s LIMIT 1
                                            """, (file_sha1,))
                                            row = cursor.fetchone()
                                            if row and row['mediainfo_json']:
                                                raw_info = row['mediainfo_json']
                                                if isinstance(raw_info, list) and len(raw_info) > 0:
                                                    mediainfo_path = os.path.join(current_local_path, os.path.splitext(name)[0] + "-mediainfo.json")
                                                    if not os.path.exists(mediainfo_path):
                                                        with open(mediainfo_path, 'w', encoding='utf-8') as f_json:
                                                            json.dump(raw_info, f_json, ensure_ascii=False)
                                                        
                                                        cursor.execute("UPDATE p115_mediainfo_cache SET hit_count = hit_count + 1 WHERE sha1 = %s", (file_sha1,))
                                                        conn.commit()
                                                        logger.debug(f"  ⚡ [指纹库秒传] 匹配到相同 SHA1，自动生成媒体信息: {os.path.basename(mediainfo_path)}")
                                except Exception: pass
                                
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
                    logger.error(f"  ❌ 全局拉取异常 (cid={target_cid}, type={task_name}): {e}")
                    api_fatal_error = True # ★ 触发熔断
                    break
            
            # 如果内层循环触发了熔断，外层循环也直接跳出
            if api_fatal_error: break
        if api_fatal_error: break

    logger.info(f"  ✅ 增量同步完成！新增/更新 STRM: {files_generated} 个, 下载字幕: {subs_downloaded} 个。")

    # =================================================================
    # 阶段 3: 本地失效文件清理 (耗时: 秒级)
    # =================================================================
    if enable_cleanup:
        if api_fatal_error:
            update_progress(90, "  🛑 [熔断保护] 由于拉取过程中发生 API 错误，为防止误删，已强制跳过本地清理阶段！")
            logger.warning("  🛑 [熔断保护] 拒绝执行本地清理！")
        else:
            update_progress(90, "  🧹 正在比对并清理本地失效文件...")
        cleaned_files = 0
        cleaned_dirs = 0
        
        for cid, rel_path in cid_to_rel_path.items():
            target_local_dir = os.path.join(local_root, rel_path)
            if not os.path.exists(target_local_dir): continue
            
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
            
            for root_dir, dirs, files in os.walk(target_local_dir, topdown=False):
                for d in dirs:
                    dir_path = os.path.join(root_dir, d)
                    try:
                        if not os.listdir(dir_path): 
                            os.rmdir(dir_path)
                            cleaned_dirs += 1
                    except: pass
                    
        logger.info(f"  🧹 清理完成: 删除了 {cleaned_files} 个失效文件, {cleaned_dirs} 个空目录。")

    update_progress(100, "=== 极速全量同步任务圆满结束 ===")

def delete_115_files_by_webhook(item_path, pickcodes):
    """
    接收神医 Webhook 传来的路径和提取码，精准销毁 115 网盘文件。
    ★ 终极优化版：优先查本地缓存瞬间锁定，未命中再兜底扫描。
    """
    if not pickcodes or not item_path: return

    client = P115Service.get_client()
    if not client: return

    try:
        # 1. 提取主目录名称
        match = re.search(r'([^/\\]+\{tmdb=\d+\})', item_path)
        if not match:
            logger.warning(f"  ⚠️ [联动删除] 无法从路径提取 TMDb 目录名: {item_path}")
            return
        tmdb_folder_name = match.group(1)

        # 2. 查找主目录 CID
        base_cid = P115CacheManager.get_cid_by_name(tmdb_folder_name)
        if not base_cid:
            try:
                res = client.fs_files({'search_value': tmdb_folder_name, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                for item in res.get('data', []):
                    if item.get('fn') == tmdb_folder_name and str(item.get('fc')) == '0':
                        base_cid = item.get('fid')
                        break
            except Exception: pass

        if not base_cid:
            logger.warning(f"  ⚠️ [联动删除] 未在 115 找到对应主目录，可能已被删除: {tmdb_folder_name}")
            return

        # =================================================================
        # ★ 3. 核心优化：优先查本地数据库缓存，瞬间锁定文件 ID
        # =================================================================
        fids_to_delete = []
        cached_files = P115CacheManager.get_files_by_pickcodes(pickcodes)
        
        for f in cached_files:
            fids_to_delete.append(f['id'])
            
        # 找出哪些 PC 码没有在缓存中命中
        cached_pcs = [f['pick_code'] for f in cached_files]
        unmatched_pickcodes = set(pickcodes) - set(cached_pcs)

        if not unmatched_pickcodes:
            logger.info("  ⚡ [联动删除] 缓存全命中，已定位所有待删除文件！")
        else:
            logger.info(f"  🔍 [联动删除] 有 {len(unmatched_pickcodes)} 个文件未命中缓存，启动网盘扫描兜底...")
            # 兜底扫描：只匹配那些没找到的 PC 码
            def scan_and_match(cid):
                try:
                    res = client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    for item in res.get('data', []):
                        if str(item.get('fc')) == '1':
                            if item.get('pc') in unmatched_pickcodes:
                                fids_to_delete.append(item.get('fid'))
                        elif str(item.get('fc')) == '0':
                            scan_and_match(item.get('fid'))
                except Exception as e:
                    logger.warning(f"  ⚠️ [联动删除] 扫描目录 {cid} 报错: {e}")

            scan_and_match(base_cid)

        # 4. 执行物理销毁
        if fids_to_delete:
            resp = client.fs_delete(fids_to_delete)
            if resp.get('state'):
                logger.info(f"  💥 [联动删除] 成功在 115 网盘删除了 {len(fids_to_delete)} 个文件！")
                # 同步清理这些文件在本地数据库的缓存记录
                P115CacheManager.delete_files(fids_to_delete)
                logger.info(f"  🧹 [联动删除] 已清理被删文件的本地缓存记录。")
            else:
                logger.error(f"  ❌ [联动删除] 115 删除接口调用失败: {resp}")

            # 5. 鞭尸检查：如果主目录里已经没有视频文件了，连目录一起扬了
            video_count = 0
            def count_videos(cid):
                nonlocal video_count
                try:
                    res = client.fs_files({'cid': cid, 'limit': 1000, 'record_open_time': 0, 'count_folders': 0})
                    for item in res.get('data', []):
                        if str(item.get('fc')) == '1':
                            ext = str(item.get('fn', '')).split('.')[-1].lower()
                            if ext in ['mp4', 'mkv', 'avi', 'ts', 'iso']:
                                video_count += 1
                        elif str(item.get('fc')) == '0':
                            count_videos(item.get('fid'))
                except Exception:
                    video_count += 999 

            count_videos(base_cid)
            if video_count == 0:
                client.fs_delete(base_cid)
                P115CacheManager.delete_cid(base_cid)
                logger.info(f"  🧹 [联动删除] 主目录已空，已删除网盘目录及本地目录缓存: {tmdb_folder_name}")
            else:
                logger.debug(f"  🛡️ [联动删除] 目录内仍有视频或检查受阻，保留主目录。")
        else:
            logger.warning(f"  ⚠️ [联动删除] 未在网盘找到匹配的提取码文件。")

    except Exception as e:
        logger.error(f"  ❌ [联动删除] 执行异常: {e}", exc_info=True)
