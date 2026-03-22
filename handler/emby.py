# handler/emby.py

import requests
import concurrent.futures
import os
import gc
import json
import base64
import shutil
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import BoundedSemaphore
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import config_manager
import constants
from typing import Optional, List, Dict, Any, Generator, Tuple, Set, Callable
import logging
logger = logging.getLogger(__name__)

class EmbyAPIClient:
    """
    Emby API 客户端封装
    功能：
    1. 自动重试：遇到 500, 502, 503, 504 错误时自动重试。
    2. 并发控制：限制最大并发请求数，防止冲垮服务器。
    3. 会话复用：使用 Session 保持长连接。
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(EmbyAPIClient, cls).__new__(cls)
                    cls._instance._init_session()
        return cls._instance

    def _init_session(self):
        self.session = requests.Session()
        
        # --- 配置重试策略 ---
        # total=5: 最多重试5次
        # backoff_factor=1: 重试间隔 (1s, 2s, 4s, 8s...)
        # status_forcelist: 遇到这些状态码时重试
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # --- 并发限制 ---
        # 限制同时只有 10 个请求能打到 Emby，多余的会在本地排队等待
        self.semaphore = BoundedSemaphore(10)

    def request(self, method, url, **kwargs):
        """
        统一请求入口，带并发锁
        """
        # 自动注入超时，如果未指定
        if 'timeout' not in kwargs:
            kwargs['timeout'] = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

        with self.semaphore:
            try:
                response = self.session.request(method, url, **kwargs)
                return response
            except requests.exceptions.RetryError:
                logger.error(f"Emby API 请求重试多次后失败: {url}")
                raise
            except Exception as e:
                logger.error(f"Emby API 请求异常: {e} | URL: {url}")
                raise

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)

# 初始化全局客户端实例
emby_client = EmbyAPIClient()

def get_running_tasks(base_url: str, api_key: str) -> List[Dict[str, Any]]:
    """
    获取当前正在运行的 Emby 后台任务
    """
    api_url = f"{base_url.rstrip('/')}/ScheduledTasks"
    params = {"api_key": api_key}
    
    try:
        # 使用新的客户端发送请求
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        tasks = response.json()
        
        # 筛选出状态为 Running 的任务
        running = [
            {
                "Name": t.get("Name"),
                "Progress": t.get("CurrentProgressPercentage", 0),
                "Id": t.get("Id")
            }
            for t in tasks if t.get("State") == "Running"
        ]
        return running
    except Exception as e:
        logger.error(f"获取后台任务状态失败: {e}")
        return []

def get_active_transcoding_sessions(base_url: str, api_key: str) -> List[str]:
    """
    获取当前正在【转码】的会话列表。
    转码非常消耗 CPU，应视为服务器忙碌。
    """
    api_url = f"{base_url.rstrip('/')}/Sessions"
    params = {"api_key": api_key}
    
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        sessions = response.json()
        
        transcoding_sessions = []
        for s in sessions:
            # 检查 TranscodingInfo 字段是否存在且不为空
            if s.get("TranscodingInfo") and s.get("PlayState", {}).get("IsPaused") is False:
                user = s.get("UserName", "未知用户")
                item = s.get("NowPlayingItem", {}).get("Name", "未知视频")
                transcoding_sessions.append(f"{user} 正在转码观看 [{item}]")
                
        return transcoding_sessions
    except Exception as e:
        logger.error(f"获取会话状态失败: {e}")
        return []

def wait_for_server_idle(base_url: str, api_key: str, max_wait_seconds: int = 300):
    """
    【队列机制核心 - 增强版】
    阻塞等待，直到没有【非忽略】的后台任务在运行。
    """
    # 1. 任务名称翻译 (用于日志显示)
    TASK_TRANSLATIONS = {
        "Scan media library": "扫描媒体库",
        "Refresh people": "刷新人物信息",
        "Refresh metadata": "刷新元数据",
        "Generate video preview thumbnails": "生成视频缩略图",
        "Chapter image extraction": "提取章节图片",
        "Convert media": "转换媒体",
        "Extract MediaInfo": "神医-提取媒体信息",
        "Extract Intro Fingerprint": "神医-提取片头指纹",
        "Extract Video Thumbnail": "神医-提取视频缩略图",
        "Build Douban Cache": "神医-构建豆瓣缓存"
    }

    # 2. ★★★ 忽略列表 (白名单) ★★★
    # 只要任务名称包含以下任意关键词(不区分大小写)，脚本就会无视它，直接继续执行
    IGNORED_TASKS = [
        "Rotate log file",               # 日志轮转 (通常极快)
        "Check for application updates", # 检查更新 (不占资源)
        "Refresh Guide",                 # 刷新直播指南 (IPTV相关，通常只占网络)
        "Clean up collections",          # 清理合集 (通常很快)
        "Build Douban Cache",            # 神医-构建豆瓣缓存 (不影响可以忽略)
        # "Scan media library",          # <--- 如果你想一边扫库一边硬跑，可以把这个注释解开
    ]
    
    start_time = time.time()
    
    while True:
        # --- 检查 1: 后台计划任务 ---
        running_tasks = get_running_tasks(base_url, api_key)
        
        # --- 检查 2: 活跃转码会话 ---
        # transcoding_sessions = get_active_transcoding_sessions(base_url, api_key)
        
        busy_reasons = []

        # A. 判定任务忙碌
        for task in running_tasks:
            raw_name = task['Name']
            
            # --- ★★★ 检查是否在忽略列表中 ★★★ ---
            is_ignored = False
            for ignore_kw in IGNORED_TASKS:
                if ignore_kw.lower() in raw_name.lower():
                    is_ignored = True
                    break
            
            if is_ignored:
                # 如果是忽略的任务，仅在调试日志里记录一下，不加入 busy_reasons
                # logger.debug(f"  ➜ 忽略任务: {raw_name} 执行刷新请求。")
                continue
            # ---------------------------------------

            display_name = TASK_TRANSLATIONS.get(raw_name, raw_name)
            progress = task.get('Progress', 0)
            busy_reasons.append(f"任务: {display_name}({progress:.1f}%)")

        # B. 判定转码忙碌
        # if transcoding_sessions:
        #     busy_reasons.extend(transcoding_sessions)

        # --- 决策 ---
        if not busy_reasons:
            return # 服务器空闲 (或者只有被忽略的任务)，放行
            
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            logger.warning(f"  ⚠️ 等待 Emby 空闲超时 ({max_wait_seconds}s)，强制继续执行。")
            return
            
        # 取第一个忙碌原因显示在日志里
        reason_str = busy_reasons[0]
        if len(busy_reasons) > 1:
            reason_str += f" 等{len(busy_reasons)}项"
            
        logger.info(f"  ⏳ Emby 负载高 [{reason_str}]，暂停等待中... (已等待 {int(elapsed)}s)")
        time.sleep(10)

# 获取管理员令牌
_admin_token_cache = {}

class SimpleLogger:
    def info(self, msg): print(f"[EMBY_INFO] {msg}")
    def error(self, msg): print(f"[EMBY_ERROR] {msg}")
    def warning(self, msg): print(f"[EMBY_WARN] {msg}")
    def debug(self, msg): print(f"[EMBY_DEBUG] {msg}")
    def success(self, msg): print(f"[EMBY_SUCCESS] {msg}")
_emby_id_cache = {}
_emby_season_cache = {}
_emby_episode_cache = {}
# ★★★ 模拟用户登录以获取临时 AccessToken 的辅助函数 ★★★
def _login_and_get_token() -> tuple[Optional[str], Optional[str]]:
    """
    【私有】执行实际的 Emby 登录操作来获取新的 Token。
    这个函数不应被外部直接调用。
    """
    global _admin_token_cache
    
    cfg = config_manager.APP_CONFIG
    emby_url = cfg.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    admin_user = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_USER)
    admin_pass = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_PASS)

    if not all([emby_url, admin_user, admin_pass]):
        logger.error("  ➜ [自动登录] 失败：未在设置中完整配置 Emby 服务器地址和管理员账密。")
        return None, None

    auth_url = f"{emby_url.rstrip('/')}/Users/AuthenticateByName"
    headers = {
        'Content-Type': 'application/json',
        'X-Emby-Authorization': 'Emby Client="Emby Toolkit", Device="Toolkit", DeviceId="d4f3e4b4-9f5b-4b8f-8b8a-5c5c5c5c5c5c", Version="1.0.0"'
    }
    payload = {"Username": admin_user, "Pw": admin_pass}
    
    try:
        response = emby_client.post(auth_url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        access_token = data.get("AccessToken")
        user_id = data.get("User", {}).get("Id")
        
        if access_token and user_id:
            logger.debug("  ➜ [自动登录] 成功，已获取并缓存了新的管理员 AccessToken。")
            # 成功获取后，存入缓存
            _admin_token_cache['access_token'] = access_token
            _admin_token_cache['user_id'] = user_id
            return access_token, user_id
        else:
            logger.error("  ➜ [自动登录] 登录 Emby 成功，但响应中未找到 AccessToken 或 UserId。")
            return None, None
    except Exception as e:
        logger.error(f"  ➜ [自动登录] 模拟登录 Emby 失败: {e}")
        return None, None

def get_admin_access_token() -> tuple[Optional[str], Optional[str]]:
    """
    【V2 - 缓存版】获取管理员的 AccessToken 和 UserId。
    优先从内存缓存中读取，如果缓存为空，则自动执行登录并填充缓存。
    """
    # 1. 先检查缓存
    if 'access_token' in _admin_token_cache and 'user_id' in _admin_token_cache:
        logger.trace("  ➜ [自动登录] 从缓存中成功获取 AccessToken。")
        return _admin_token_cache['access_token'], _admin_token_cache['user_id']
    
    # 2. 缓存未命中，执行登录
    logger.debug("  ➜ [自动登录] 缓存未命中，正在执行首次登录以获取 AccessToken...")
    return _login_and_get_token()

# ✨✨✨ 快速获取指定类型的项目总数，不获取项目本身 ✨✨✨
def get_item_count(base_url: str, api_key: str, user_id: Optional[str], item_type: str, parent_id: Optional[str] = None) -> Optional[int]:
    """
    【增强版】快速获取指定类型的项目总数。
    新增 parent_id 参数，用于统计特定媒体库或合集内的项目数量。
    """
    if not all([base_url, api_key, user_id, item_type]):
        logger.error(f"get_item_count: 缺少必要的参数 (需要 user_id)。")
        return None
    
    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "IncludeItemTypes": item_type,
        "Recursive": "true",
        "Limit": 0 # ★★★ 核心：Limit=0 只返回元数据（包括总数），不返回任何项目，速度极快
    }
    
    if parent_id:
        params["ParentId"] = parent_id
        logger.debug(f"正在获取父级 {parent_id} 下 {item_type} 的总数...")
    else:
        logger.debug(f"正在获取所有 {item_type} 的总数...")
            
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        total_count = data.get("TotalRecordCount")
        if total_count is not None:
            logger.debug(f"成功获取到总数: {total_count}")
            return int(total_count)
        else:
            logger.warning(f"Emby API 响应中未找到 'TotalRecordCount' 字段。")
            return None
            
    except Exception as e:
        logger.error(f"通过 API 获取 {item_type} 总数时失败: {e}")
        return None

# ✨✨✨ 获取Emby项目详情 ✨✨✨
def get_emby_item_details(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str, fields: Optional[str] = None, silent_404: bool = False) -> Optional[Dict[str, Any]]:
    if not all([item_id, emby_server_url, emby_api_key, user_id]):
        logger.error("获取Emby项目详情参数不足：缺少ItemID、服务器URL、API Key或UserID。")
        return None

    url = f"{emby_server_url.rstrip('/')}/Users/{user_id}/Items/{item_id}"

    if fields:
        fields_to_request = fields
    else:
        fields_to_request = "Type,ProviderIds,People,Path,OriginalTitle,DateCreated,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines,MediaStreams,TagItems,Tags"

    params = {
        "api_key": emby_api_key,
        "Fields": fields_to_request
    }
    
    params["PersonFields"] = "ImageTags,ProviderIds"
    
    try:
        response = emby_client.get(url, params=params)

        if response.status_code != 200:
            logger.trace(f"响应头部: {response.headers}")
            logger.trace(f"响应内容 (前500字符): {response.text[:500]}")

        response.raise_for_status()
        item_data = response.json()
        logger.trace(
            f"成功获取Emby项目 '{item_data.get('Name', item_id)}' (ID: {item_id}) 的详情。")

        if not item_data.get('Name') or not item_data.get('Type'):
            logger.warning(f"Emby项目 {item_id} 返回的数据缺少Name或Type字段。")

        return item_data

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            if silent_404:
                logger.debug(f"Emby API未找到项目ID: {item_id} (预期内的 404，已忽略)。")
            else:
                logger.warning(f"Emby API未找到项目ID: {item_id} (UserID: {user_id})。URL: {e.request.url}")
        elif e.response.status_code == 401 or e.response.status_code == 403:
            logger.error(
                f"获取Emby项目详情时发生认证/授权错误 (ItemID: {item_id}, UserID: {user_id}): {e.response.status_code} - {e.response.text[:200]}. URL: {e.request.url}. 请检查API Key和UserID权限。")
        else:
            logger.error(
                f"获取Emby项目详情时发生HTTP错误 (ItemID: {item_id}, UserID: {user_id}): {e.response.status_code} - {e.response.text[:200]}. URL: {e.request.url}")
        return None
    except requests.exceptions.RequestException as e:
        url_requested = e.request.url if e.request else url
        logger.error(
            f"获取Emby项目详情时发生请求错误 (ItemID: {item_id}, UserID: {user_id}): {e}. URL: {url_requested}")
        return None
    except Exception as e:
        import traceback
        logger.error(
            f"获取Emby项目详情时发生未知错误 (ItemID: {item_id}, UserID: {user_id}): {e}\n{traceback.format_exc()}")
        return None
    
# ✨✨✨ 更新一个 Person 条目本身的信息 ✨✨✨
def update_person_details(person_id: str, new_data: Dict[str, Any], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([person_id, new_data, emby_server_url, emby_api_key, user_id]):
        logger.error("update_person_details: 参数不足 (需要 user_id)。")
        return False

    api_url = f"{emby_server_url.rstrip('/')}/Users/{user_id}/Items/{person_id}"
    params = {"api_key": emby_api_key}
    wait_for_server_idle(emby_server_url, emby_api_key)
    try:
        logger.trace(f"准备获取 Person 详情 (ID: {person_id}, UserID: {user_id}) at {api_url}")
        response_get = emby_client.get(api_url, params=params)
        response_get.raise_for_status()
        person_to_update = response_get.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"更新Person前获取其详情失败 (ID: {person_id}, UserID: {user_id}): {e}")
        return False

    for key, value in new_data.items():
        person_to_update[key] = value
    
    update_url = f"{emby_server_url.rstrip('/')}/Items/{person_id}"
    headers = {'Content-Type': 'application/json'}

    logger.trace(f"  ➜ 准备更新 Person (ID: {person_id}) 的信息，新数据: {new_data}")
    try:
        response_post = emby_client.post(update_url, json=person_to_update, headers=headers, params=params)
        response_post.raise_for_status()
        logger.trace(f"  ➜ 成功更新 Person (ID: {person_id}) 的信息。")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"  ➜ 更新 Person (ID: {person_id}) 时发生错误: {e}")
        return False

# ✨✨✨ 获取 Emby 用户可见媒体库列表 ✨✨✨
def get_emby_libraries(emby_server_url, emby_api_key, user_id):
    if not all([emby_server_url, emby_api_key, user_id]):
        logger.error("get_emby_libraries: 缺少必要的Emby配置信息。")
        return None

    target_url = f"{emby_server_url.rstrip('/')}/emby/Users/{user_id}/Views"
    params = {'api_key': emby_api_key}
    
    try:
        logger.trace(f"  ➜ 正在从 {target_url} 获取媒体库和合集...")
        response = emby_client.get(target_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        items = data.get('Items', [])
        logger.trace(f"  ➜ 成功获取到 {len(items)} 个媒体库/合集。")
        return items

    except requests.exceptions.RequestException as e:
        logger.error(f"连接Emby服务器获取媒体库/合集时失败: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"处理Emby媒体库/合集数据时发生未知错误: {e}", exc_info=True)
        return None

# --- 遍历指定的媒体库，通过分页获取所有独立的、未被聚合的媒体项 ---
def get_all_library_versions(
    base_url: str,
    api_key: str,
    user_id: str,
    media_type_filter: str,
    fields: str,
    library_ids: Optional[List[str]] = None,
    parent_id: Optional[str] = None,
    update_status_callback: Optional[Callable[[int, str], None]] = None
) -> List[Dict[str, Any]]:
    """
    - 获取服务器级的、未经聚合的原始媒体项列表。
    - 支持扫描指定媒体库列表 (library_ids) 或指定父对象 (parent_id)。
    """
    all_items = []
    
    target_ids = []
    if parent_id:
        target_ids = [parent_id]
    elif library_ids:
        target_ids = library_ids
    
    if not target_ids:
        return []

    total_items_to_fetch = 0
    logger.info("  ➜ 开始获取所有在库媒体项...")
    if update_status_callback:
        update_status_callback(1, "正在计算媒体库总项目数...")

    for target_id in target_ids: 
        try:
            count_url = f"{base_url.rstrip('/')}/Items"
            count_params = {
                "api_key": api_key, "ParentId": target_id, "IncludeItemTypes": media_type_filter,
                "Recursive": "true", "Limit": 0 
            }
            response = emby_client.get(count_url, params=count_params)
            response.raise_for_status()
            count = response.json().get("TotalRecordCount", 0)
            total_items_to_fetch += count
        except requests.RequestException as e:
            logger.warning(f"  ➜ 预计算目标 {target_id} 的项目总数时失败: {e}，总数可能不准。")
            continue
    
    total_processed_items = 0
    
    for target_id in target_ids: 
        start_index = 0
        limit = 500
        
        while True:
            api_url = f"{base_url.rstrip('/')}/Items"
            params = {
                "api_key": api_key, "ParentId": target_id, "IncludeItemTypes": media_type_filter,
                "Recursive": "true", "Fields": fields, "StartIndex": start_index, "Limit": limit
            }
            try:
                response = emby_client.get(api_url, params=params)
                response.raise_for_status()
                items_in_batch = response.json().get("Items", [])
                if not items_in_batch: break

                for item in items_in_batch: item['_SourceLibraryId'] = target_id
                all_items.extend(items_in_batch)
                start_index += len(items_in_batch)
                
                total_processed_items += len(items_in_batch)
                if update_status_callback and total_items_to_fetch > 0:
                    # 进度计算：网络请求阶段占总进度的 80%
                    progress = int((total_processed_items / total_items_to_fetch) * 80)
                    # 确保进度不会超过80%
                    progress = min(progress, 80) 
                    update_status_callback(progress, f"正在索引 {total_processed_items}/{total_items_to_fetch} 个媒体项...")

                if len(items_in_batch) < limit: break
            except requests.RequestException as e:
                logger.error(f"  ➜ 从媒体库 {target_id} 获取数据时出错: {e}")
                break
    
    logger.info(f"  ➜ 获取完成，共找到 {len(all_items)} 个媒体项。")
    
    if update_status_callback:
        update_status_callback(80, "媒体项索引完成，即将进行本地数据比对...")
        
    return all_items

# --- 分页生成器 ---
def fetch_all_emby_items_generator(base_url: str, api_key: str, library_ids: list, fields: str):
    """
    生成器：分页从 Emby 获取所有项目。
    优化：逐个库遍历，并自动注入 _SourceLibraryId，解决资产数据缺失来源库ID的问题。
    """
    limit = 1000 
    headers = {
        'X-Emby-Token': api_key,
        'Content-Type': 'application/json'
    }
    url = f"{base_url.rstrip('/')}/Items"
    # 确保 library_ids 是列表
    target_libs = library_ids if library_ids else [None]

    for lib_id in target_libs:
        start_index = 0
        while True:
            params = {
                'Recursive': 'true',
                'Fields': fields,
                'StartIndex': start_index,
                'Limit': limit,
                'IncludeItemTypes': "Movie,Series,Season,Episode,Folder,CollectionFolder,UserView",
            }
            if lib_id:
                params['ParentId'] = lib_id

            try:
                # 增加超时时间
                response = emby_client.get(url, params=params, headers=headers)
                
                # 简单的 500 错误重试逻辑
                if response.status_code == 500:
                    time.sleep(2)
                    params['Limit'] = 500
                    response = emby_client.get(url, params=params, headers=headers)

                response.raise_for_status()
                data = response.json()
                items = data.get('Items', [])
                
                if not items:
                    break
                    
                for item in items:
                    # ★★★ 核心修复：在这里直接注入来源库 ID ★★★
                    # 这样后续处理 asset_details 时就能直接读到了，无需反查
                    if lib_id:
                        item['_SourceLibraryId'] = lib_id
                    
                    yield item
                
                if len(items) < params['Limit']:
                    break
                    
                start_index += params['Limit']
                
                # 主动 GC，防止大循环内存累积
                if start_index % 5000 == 0:
                    gc.collect()
                
                time.sleep(0.1) # 稍微歇一下
                    
            except Exception as e:
                logger.error(f"分页获取 Emby 项目失败 (Lib: {lib_id}, Index: {start_index}): {e}")
                break

# ✨✨✨ 获取项目，并为每个项目添加来源库ID ✨✨✨
def get_emby_library_items(
    base_url: str,
    api_key: str,
    media_type_filter: Optional[str] = None,
    user_id: Optional[str] = None,
    library_ids: Optional[List[str]] = None,
    search_term: Optional[str] = None,
    library_name_map: Optional[Dict[str, str]] = None,
    fields: Optional[str] = None,
    # ★★★ 核心修复：增加新参数并提供默认值，以兼容旧调用 ★★★
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = "Descending",
    limit: Optional[int] = None,
    force_user_endpoint: bool = False
) -> Optional[List[Dict[str, Any]]]:
    if not base_url or not api_key:
        logger.error("get_emby_library_items: base_url 或 api_key 未提供。")
        return None

    if search_term and search_term.strip():
        # ... (搜索逻辑保持不变) ...
        logger.info(f"进入搜索模式，关键词: '{search_term}'")
        api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
        params = {
            "api_key": api_key,
            "SearchTerm": search_term.strip(),
            "IncludeItemTypes": media_type_filter or "Movie,Series",
            "Recursive": "true",
            "Fields": "Id,Name,Type,ProductionYear,ProviderIds,Path",
            "Limit": 100
        }
        try:
            response = emby_client.get(api_url, params=params)
            response.raise_for_status()
            items = response.json().get("Items", [])
            logger.info(f"搜索到 {len(items)} 个匹配项。")
            return items
        except requests.exceptions.RequestException as e:
            logger.error(f"搜索 Emby 时发生网络错误: {e}")
            return None

    if not library_ids:
        return []

    all_items_from_selected_libraries: List[Dict[str, Any]] = []
    for lib_id in library_ids:
        if not lib_id or not lib_id.strip():
            continue
        
        library_name = library_name_map.get(lib_id, lib_id) if library_name_map else lib_id
        
        try:
            fields_to_request = fields if fields else "ProviderIds,Name,Type,MediaStreams,ChildCount,Path,OriginalTitle"

            params = {
                "api_key": api_key, "Recursive": "true", "ParentId": lib_id,
                "Fields": fields_to_request,
            }
            if media_type_filter:
                params["IncludeItemTypes"] = media_type_filter
            
            # ★★★ 核心修复：应用服务器端优化参数 ★★★
            if sort_by:
                params["SortBy"] = sort_by
            if sort_order and sort_by: # 只有在指定排序时才需要排序顺序
                params["SortOrder"] = sort_order
            if limit is not None:
                params["Limit"] = limit

            if force_user_endpoint and user_id:
                api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
            else:
                api_url = f"{base_url.rstrip('/')}/Items"
                if user_id:
                    params["UserId"] = user_id

            logger.trace(f"Requesting items from library '{library_name}' (ID: {lib_id}) using URL: {api_url}.")
            
            response = emby_client.get(api_url, params=params)
            response.raise_for_status()
            items_in_lib = response.json().get("Items", [])
            
            if items_in_lib:
                for item in items_in_lib:
                    item['_SourceLibraryId'] = lib_id
                all_items_from_selected_libraries.extend(items_in_lib)
        
        except Exception as e:
            logger.error(f"请求库 '{library_name}' 中的项目失败: {e}", exc_info=True)
            continue

    type_to_chinese = {"Movie": "电影", "Series": "电视剧", "Video": "视频", "MusicAlbum": "音乐专辑"}
    media_type_in_chinese = ""

    if media_type_filter:
        types = media_type_filter.split(',')
        translated_types = [type_to_chinese.get(t, t) for t in types]
        media_type_in_chinese = "、".join(translated_types)
    else:
        media_type_in_chinese = '所有'

    logger.debug(f"  ➜ 总共从 {len(library_ids)} 个选定库中获取到 {len(all_items_from_selected_libraries)} 个 {media_type_in_chinese} 项目。")
    
    return all_items_from_selected_libraries

# ✨✨✨ 刷新Emby元数据 ✨✨✨
def refresh_emby_item_metadata(item_emby_id: str,
                               emby_server_url: str,
                               emby_api_key: str,
                               user_id_for_ops: str,
                               replace_all_metadata_param: bool = False,
                               replace_all_images_param: bool = False,
                               item_name_for_log: Optional[str] = None
                               ) -> bool:
    if not all([item_emby_id, emby_server_url, emby_api_key, user_id_for_ops]):
        logger.error("刷新Emby元数据参数不足：缺少ItemID、服务器URL、API Key或UserID。")
        return False
    wait_for_server_idle(emby_server_url, emby_api_key)
    log_identifier = f"'{item_name_for_log}'" if item_name_for_log else f"ItemID: {item_emby_id}"
    
    try:
        logger.trace(f"  ➜ 正在为 {log_identifier} 获取当前详情...")
        item_data = get_emby_item_details(item_emby_id, emby_server_url, emby_api_key, user_id_for_ops)
        if not item_data:
            logger.error(f"  🚫 无法获取 {log_identifier} 的详情，所有操作中止。")
            return False

        item_needs_update = False
        
        if replace_all_metadata_param:
            logger.trace(f"  ➜ 检测到 ReplaceAllMetadata=True，执行解锁...")
            if item_data.get("LockData") is True:
                item_data["LockData"] = False
                item_needs_update = True
            if item_data.get("LockedFields"):
                item_data["LockedFields"] = []
                item_needs_update = True
        
        if item_needs_update:
            logger.trace(f"  ➜ 正在为 {log_identifier} 提交锁状态更新...")
            update_url = f"{emby_server_url.rstrip('/')}/Items/{item_emby_id}"
            update_params = {"api_key": emby_api_key}
            headers = {'Content-Type': 'application/json'}
            update_response = emby_client.post(update_url, json=item_data, headers=headers, params=update_params)
            update_response.raise_for_status()
            logger.trace(f"  ➜ 成功更新 {log_identifier} 的锁状态。")
        else:
            logger.trace(f"  ➜ 项目 {log_identifier} 的锁状态无需更新。")

    except Exception as e:
        logger.warning(f"  ➜ 在刷新前更新锁状态时失败: {e}。刷新将继续，但可能受影响。")

    logger.debug(f"  ➜ 正在为 {log_identifier} 发送最终的刷新请求...")
    refresh_url = f"{emby_server_url.rstrip('/')}/Items/{item_emby_id}/Refresh"
    params = {
        "api_key": emby_api_key,
        "Recursive": str(item_data.get("Type") == "Series").lower(),
        "MetadataRefreshMode": "Default",
        "ImageRefreshMode": "Default",
        "ReplaceAllMetadata": str(replace_all_metadata_param).lower(),
        "ReplaceAllImages": str(replace_all_images_param).lower()
    }
    
    try:
        response = emby_client.post(refresh_url, params=params)
        if response.status_code == 204:
            logger.info(f"  ➜ 已成功为 {log_identifier} 刷新元数据。")
            return True
        else:
            logger.error(f"  - 刷新请求失败: HTTP状态码 {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"  - 刷新请求时发生网络错误: {e}")
        return False

def _force_refresh_directory_tree(target_dir: str, base_url: str, api_key: str):
    """
    【内部辅助】向上逐级查找 Emby 中已存在的父目录，并对其触发精准的局部刷新。
    """
    current_path = target_dir
    
    # 最多向上找 4 级 (文件 -> 电影目录 -> 分类目录 -> 媒体库根目录)，防止扫到太顶层
    for _ in range(4):
        if not current_path or current_path == '/' or current_path == '\\':
            break
            
        # 查询 Emby 中是否存在这个路径
        api_url = f"{base_url.rstrip('/')}/Items"
        params = {
            "api_key": api_key,
            "Recursive": "true",
            "Path": current_path,
            "Fields": "Id,Path,Name"
        }
        
        try:
            resp = emby_client.get(api_url, params=params)
            if resp.status_code == 200:
                items = resp.json().get("Items", [])
                if items:
                    # 找到了 Emby 认识的父目录！
                    target_id = items[0].get("Id")
                    target_name = items[0].get("Name", current_path)
                    
                    logger.info(f"  🎯 [定点扫描] 找到已存在的父目录: '{target_name}'，准备扫描...")
                    
                    # 对这个特定的父目录触发刷新
                    refresh_url = f"{base_url.rstrip('/')}/Items/{target_id}/Refresh"
                    refresh_params = {
                        "api_key": api_key,
                        "Recursive": "true",
                        "MetadataRefreshMode": "Default",
                        "ImageRefreshMode": "Default"
                    }
                    emby_client.post(refresh_url, params=refresh_params)
                    logger.info(f"  🚀 [局部刷新] 已对 '{target_name}' 触发秒级扫描，Emby 正在干活！")
                    return True
        except Exception as e:
            pass # 忽略查询错误，继续向上找
            
        # 向上退一级 (例如从 /strm/电影/超级英雄/奇异博士 退到 /strm/电影/超级英雄)
        current_path = os.path.dirname(current_path)
        
    logger.warning(f"  ⚠️ [局部刷新] 未能在 Emby 中找到 {target_dir} 的有效父目录，将依赖 90 秒自动扫描。")
    return False

# --- 极速轻量级文件变更通知 ---
def notify_emby_file_changes(file_paths: List[str], base_url: str, api_key: str, update_type: str = "Created") -> bool:
    """
    【极速轻量级刷新】
    利用 Emby 的 /Library/Media/Updated 接口，直接通知底层文件系统变更。
    支持 Created, Modified, Deleted。
    """
    if not file_paths: 
        return True
        
    api_url = f"{base_url.rstrip('/')}/Library/Media/Updated"
    
    # 构造 Payload，传入指定的 UpdateType
    updates = [{"Path": path, "UpdateType": update_type} for path in file_paths]
    payload = {"Updates": updates}
    
    action_map = {
        "Created": "新增",
        "Modified": "修改",
        "Deleted": "删除"
    }
    action_zh = action_map.get(update_type, update_type)
    
    try:
        # 1. 提交变更路径到 Emby 的等待队列
        emby_client.post(api_url, params={"api_key": api_key}, json=payload)
        logger.info(f"  ⚡ [极速通知] 已通知 Emby 有 {len(file_paths)} 个文件{action_zh}。")
        
        # 2. ★★★ 局部精准刷新 (打断 90 秒摸鱼) ★★★
        # 提取所有文件所在的目录，去重 (防止批量入库时重复刷新同一个父目录)
        dirs_to_refresh = set(os.path.dirname(p) for p in file_paths if p)
        
        for d in dirs_to_refresh:
            _force_refresh_directory_tree(d, base_url, api_key)
            
        return True
    except Exception as e:
        logger.error(f"  ❌ [极速通知] 发送文件{action_zh}通知失败: {e}")
        return False

# ✨✨✨ 分批次地从 Emby 获取所有 Person 条目 ✨✨✨
def get_all_persons_from_emby(
    base_url: str, 
    api_key: str, 
    user_id: Optional[str], 
    stop_event: Optional[threading.Event] = None,
    batch_size: int = 500,
    update_status_callback: Optional[Callable] = None,
    force_full_scan: bool = False
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    【V6.0 - 4.9+ 终极兼容版】
    - 修正了全量扫描模式，使其在 Emby 4.9+ 上能正常工作。
    - 同样切换到 /Items 端点并移除了 UserId 参数。
    """
    if not user_id:
        logger.error("  🚫 获取所有演员需要提供 User ID，但未提供。任务中止。")
        return

    library_ids = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS)
    
    # ======================================================================
    # 模式一：尝试按媒体库进行精准扫描 (如果配置了媒体库且未强制全量)
    # ======================================================================
    if library_ids and not force_full_scan:
        logger.info(f"  ➜ 检测到配置了 {len(library_ids)} 个媒体库，将优先尝试精准扫描...")
        
        media_items = get_emby_library_items(
            base_url=base_url, api_key=api_key, user_id=user_id,
            library_ids=library_ids, media_type_filter="Movie,Series", fields="People"
        )

        unique_person_ids = set()
        if media_items:
            for item in media_items:
                if stop_event and stop_event.is_set(): return
                for person in item.get("People", []):
                    if person_id := person.get("Id"):
                        unique_person_ids.add(person_id)

        # ★★★ 核心智能检测逻辑 ★★★
        # 如果成功通过精准模式获取到了演员ID，则继续执行并返回
        if unique_person_ids:
            logger.info(f"  ➜ 精准扫描成功，发现 {len(unique_person_ids)} 位独立演员需要同步。")
            person_ids_to_fetch = list(unique_person_ids)
            
            precise_batch_size = 500
            total_precise = len(person_ids_to_fetch)
            processed_precise = 0
            for i in range(0, total_precise, precise_batch_size):
                if stop_event and stop_event.is_set(): return
                batch_ids = person_ids_to_fetch[i:i + precise_batch_size]
                person_details_batch = get_emby_items_by_id(
                    base_url=base_url, api_key=api_key, user_id=user_id,
                    item_ids=batch_ids, fields="ProviderIds,Name"
                )
                if person_details_batch:
                    yield person_details_batch
                    processed_precise += len(person_details_batch)
                    if update_status_callback:
                        progress = int((processed_precise / total_precise) * 95)
                        update_status_callback(progress, f"已扫描 {processed_precise}/{total_precise} 名演员...")
            return # ★★★ 精准模式成功，任务结束 ★★★

        # ★★★ 自动降级触发点 ★★★
        # 如果代码执行到这里，说明精准模式没找到任何演员，需要降级
        if media_items is not None: # 仅在API调用成功但结果为空时显示警告
             logger.warning("  ➜ 精准扫描未返回任何演员（可能您是 beta 版本），将自动降级为全量扫描模式...")
    
    # ======================================================================
    # 模式二：执行全量扫描 (在未配置媒体库、强制全量或精准扫描失败时)
    # ======================================================================
    if force_full_scan:
        logger.info("  ➜ [强制全量扫描模式] 已激活，将扫描服务器上的所有演员...")
    else:
        logger.info("  ➜ 开始从整个 Emby 服务器分批获取所有演员数据...")
    
    total_count = 0
    try:
        # ★★★ 核心修正: 切换到 /Items 端点且不使用 UserId 获取总数 ★★★
        count_url = f"{base_url.rstrip('/')}/Items"
        count_params = {"api_key": api_key, "IncludeItemTypes": "Person", "Recursive": "true", "Limit": 0}
        response = emby_client.get(count_url, params=count_params)
        response.raise_for_status()
        total_count = response.json().get("TotalRecordCount", 0)
        logger.info(f"  ➜ Emby 演员 总数: {total_count}")
    except Exception as e:
        logger.error(f"  ➜ 获取 Emby 演员 总数失败: {e}")
    
    # ★★★ 核心修正: 切换到 /Items 端点 ★★★
    api_url = f"{base_url.rstrip('/')}/Items"
    headers = {"X-Emby-Token": api_key, "Accept": "application/json"}
    params = {
        "Recursive": "true",
        "IncludeItemTypes": "Person",
        "Fields": "ProviderIds,Name",
        # ★★★ 核心修正: 不再传递 UserId。演员是全局对象。 ★★★
    }
    start_index = 0

    while True:
        if stop_event and stop_event.is_set():
            logger.info("  🚫 Emby Person 获取任务被中止。")
            return

        request_params = params.copy()
        request_params["StartIndex"] = start_index
        request_params["Limit"] = batch_size
        
        try:
            response = emby_client.get(api_url, headers=headers, params=request_params)
            response.raise_for_status()
            items = response.json().get("Items", [])
            
            if not items:
                break

            yield items
            start_index += len(items)

            if update_status_callback:
                progress = int((start_index / total_count) * 95) if total_count > 0 else 5
                update_status_callback(progress, f"已扫描 {start_index}/{total_count if total_count > 0 else '未知'} 名演员...")

        except requests.exceptions.RequestException as e:
            logger.error(f"请求 Emby API 失败 (批次 StartIndex={start_index}): {e}", exc_info=True)
            return

# ✨✨✨ 获取剧集下所有子项目 ✨✨✨
def get_series_children(
    series_id: str,
    base_url: str,
    api_key: str,
    user_id: str,
    series_name_for_log: Optional[str] = None,
    include_item_types: str = "Season,Episode",
    fields: str = "Id,Name,ParentIndexNumber,IndexNumber,Overview"
) -> Optional[List[Dict[str, Any]]]:
    log_identifier = f"'{series_name_for_log}' (ID: {series_id})" if series_name_for_log else f"ID {series_id}"

    if not all([series_id, base_url, api_key, user_id]):
        logger.error("get_series_children: 参数不足。")
        return None

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "ParentId": series_id,
        "IncludeItemTypes": include_item_types,
        "Recursive": "true",
        "Fields": fields,
        "Limit": 10000
    }
    
    logger.debug(f"  ➜ 准备获取剧集 {log_identifier} 的子项目 (类型: {include_item_types})...")
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()
        children = data.get("Items", [])
        logger.debug(f"  ➜ 成功为剧集 {log_identifier} 获取到 {len(children)} 个子项目。")
        return children
    except requests.exceptions.RequestException as e:
        logger.error(f"获取剧集 {log_identifier} 的子项目列表时发生错误: {e}", exc_info=True)
        return None

# ✨✨✨ 获取剧集下所有季 ✨✨✨
def get_series_seasons(
    series_id: str,
    base_url: str,
    api_key: str,
    user_id: str,
    series_name_for_log: Optional[str] = None
) -> Optional[List[Dict[str, Any]]]:
    """
    【新增】专门用于获取一个剧集下所有“季”（Season）的列表。
    这是通过调用 get_series_children 实现的，以确保代码复用。
    """
    # 直接调用通用的 get_series_children 函数，并指定只获取 Season 类型
    return get_series_children(
        series_id=series_id,
        base_url=base_url,
        api_key=api_key,
        user_id=user_id,
        series_name_for_log=series_name_for_log,
        include_item_types="Season",  # ★★★ 核心：只请求季
        fields="Id,Name,IndexNumber"  # ★★★ 核心：请求季ID和季号，这是洗版逻辑需要的
    )

# ✨✨✨ 根据子项目ID（如分集或季）获取其所属的剧集（Series）的ID ✨✨✨    
def get_series_id_from_child_id(
    item_id: str,
    base_url: str,
    api_key: str,
    user_id: Optional[str],
    item_name: Optional[str] = None
) -> Optional[str]:
    name_for_log = item_name or item_id
    if not all([item_id, base_url, api_key, user_id]):
        logger.error(f"get_series_id_from_child_id({name_for_log}): 缺少必要的参数。")
        return None
    
    item_details = get_emby_item_details(
        item_id=item_id,
        emby_server_url=base_url,
        emby_api_key=api_key,
        user_id=user_id,
        fields="Type,SeriesId"
    )
    
    if not item_details:
        logger.warning(f"无法获取项目 '{name_for_log}' ({item_id}) 的详情，无法向上查找剧集ID。")
        return None
    
    item_type = item_details.get("Type")
    
    if item_type == "Series":
        logger.info(f"  ➜ 媒体项 '{name_for_log}' 本身就是剧集，直接返回其ID。")
        return item_id
    
    series_id = item_details.get("SeriesId")
    if series_id:
        series_details = get_emby_item_details(
            item_id=series_id,
            emby_server_url=base_url,
            emby_api_key=api_key,
            user_id=user_id,
            fields="Name"
        )
        series_name = series_details.get("Name") if series_details else None
        series_name_for_log = f"'{series_name}'" if series_name else "未知片名"
        logger.trace(f"  ➜ 媒体项 '{name_for_log}' 所属剧集为：{series_name_for_log}。")
        return str(series_id)
    
    logger.warning(f"  ➜ 媒体项 '{name_for_log}' (类型: {item_type}) 的详情中未找到 'SeriesId' 字段，无法确定所属剧集。")
    return None

# ✨✨✨ 从 Emby 下载指定类型的图片并保存到本地 ✨✨✨
def download_emby_image(
    item_id: str,
    image_type: str,
    save_path: str,
    emby_server_url: str,
    emby_api_key: str,
    image_tag: Optional[str] = None,
    max_width: Optional[int] = None,
    max_height: Optional[int] = None
) -> bool:
    if not all([item_id, image_type, save_path, emby_server_url, emby_api_key]):
        logger.error("download_emby_image: 参数不足。")
        return False

    image_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}/Images/{image_type}"
    params = {"api_key": emby_api_key}
    if max_width: params["maxWidth"] = max_width
    if max_height: params["maxHeight"] = max_height

    if image_tag:
        params["tag"] = image_tag

    logger.trace(f"准备下载图片: 类型='{image_type}', 从 URL: {image_url}")
    
    try:
        with emby_client.get(image_url, params=params, stream=True) as r:
            r.raise_for_status()
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        logger.trace(f"成功下载图片并保存到: {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
            logger.trace(f"图片类型 '{image_type}' 在 Emby 项目 '{item_id}' 中不存在。")
        else:
            logger.error(f"下载图片时发生网络错误: {e}")
        return False
    except Exception as e:
        logger.error(f"保存图片到 '{save_path}' 时发生未知错误: {e}")
        return False

# --- 获取所有合集 ---
def get_all_collections_from_emby_generic(base_url: str, api_key: str, user_id: str) -> Optional[List[Dict[str, Any]]]:
    if not all([base_url, api_key, user_id]):
        logger.error("get_all_collections_from_emby_generic: 缺少必要的参数。")
        return None

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "IncludeItemTypes": "BoxSet",
        "Recursive": "true",
        "Fields": "ProviderIds,Name,ImageTags"
    }
    
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        all_collections = response.json().get("Items", [])
        logger.debug(f"  ➜ 成功从 Emby 获取到 {len(all_collections)} 个合集。")
        return all_collections
    except Exception as e:
        logger.error(f"通用函数在获取所有Emby合集时发生错误: {e}", exc_info=True)
        return None

# --- 获取所有原生合集---
def get_all_native_collections_from_emby(base_url: str, api_key: str, user_id: str) -> List[Dict[str, Any]]:
    """
    【V9 - 回归本质终极版】
    - 融合了“库优先”策略以准确获取 ParentId。
    - 回归了通过检查 ProviderIds.Tmdb 字段是否存在来区分原生合集与自建合集的
      正确、简单且高效的原始逻辑。
    """
    logger.trace("  -> 正在采用“库优先+ProviderID过滤”策略获取真正的原生合集...")
    
    try:
        # 步骤 1: 获取服务器上所有的媒体库 (过滤掉顶层合集文件夹)
        libraries_url = f"{base_url}/Library/VirtualFolders"
        lib_params = {"api_key": api_key}
        lib_response = emby_client.get(libraries_url, params=lib_params)
        lib_response.raise_for_status()
        all_libraries_raw = lib_response.json()
        
        if not all_libraries_raw:
            logger.warning("  ➜ 未能从服务器获取到任何媒体库。")
            return []

        all_libraries = [lib for lib in all_libraries_raw if lib.get('CollectionType') != 'boxsets']
        logger.info(f"  ➜ 发现 {len(all_libraries)} 个有效媒体库，将并发查询其中的原生合集...")
        
        all_enriched_collections = []
        
        # 辅助函数，用于在线程中处理单个媒体库
        def process_library(library: Dict[str, Any]) -> List[Dict[str, Any]]:
            library_id = library.get('Id')
            library_name = library.get('Name')
            
            collections_url = f"{base_url}/Users/{user_id}/Items"
            params = { "ParentId": library_id, "IncludeItemTypes": "BoxSet", "Recursive": "true", "fields": "ProviderIds,Name,Id,ImageTags", "api_key": api_key }
            
            try:
                response = emby_client.get(collections_url, params=params)
                response.raise_for_status()
                collections_in_library = response.json().get("Items", [])
                
                if not collections_in_library: return []

                processed = []
                # ★★★ 核心逻辑回归：在这里使用你最初的正确判断方法 ★★★
                for collection in collections_in_library:
                    provider_ids = collection.get("ProviderIds", {})
                    tmdb_collection_id = provider_ids.get("Tmdb")
                    
                    # 只有当 Tmdb ID 存在时，才认为它是一个原生合集
                    if tmdb_collection_id:
                        processed.append({
                            'emby_collection_id': collection.get('Id'),
                            'name': collection.get('Name'),
                            'tmdb_collection_id': tmdb_collection_id,
                            'ImageTags': collection.get('ImageTags'),
                            'ParentId': library_id
                        })
                
                if processed:
                    logger.debug(f"  ➜ 在媒体库 '{library_name}' 中找到 {len(processed)} 个原生合集。")
                
                return processed
            except requests.RequestException as e_coll:
                logger.error(f"  ➜ 查询媒体库 '{library_name}' (ID: {library_id}) 中的合集时失败: {e_coll}")
                return []

        # 步骤 2: 使用线程池并发处理所有媒体库
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_library = {executor.submit(process_library, lib): lib for lib in all_libraries}
            for future in as_completed(future_to_library):
                result = future.result()
                if result:
                    all_enriched_collections.extend(result)

        logger.info(f"  ➜ 成功从所有媒体库中处理了 {len(all_enriched_collections)} 个原生合集。")
        return all_enriched_collections

    except requests.RequestException as e:
        logger.error(f"  ➜ 获取原生合集列表时发生严重网络错误: {e}", exc_info=True)
        return []

# ★★★ 查询包含指定媒体项的合集 ★★★
def get_collections_containing_item(item_id: str, base_url: str, api_key: str, user_id: str) -> List[Dict[str, Any]]:
    """
    查询包含指定 Item ID 的所有合集 (BoxSet)。
    用于反查某部电影所属的 Emby 合集。
    """
    if not all([item_id, base_url, api_key, user_id]):
        return []

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "IncludeItemTypes": "BoxSet", # 只找合集
        "Recursive": "true",
        "ListItemIds": item_id,       # ★★★ 核心参数：包含此ID的容器 ★★★
        "Fields": "ProviderIds,Name"
    }

    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        return response.json().get("Items", [])
    except Exception as e:
        logger.error(f"反查项目 {item_id} 所属合集失败: {e}")
        return []

# ✨✨✨ 获取 Emby 服务器信息 (如 Server ID) ✨✨✨
def get_emby_server_info(base_url: str, api_key: str, **kwargs) -> Optional[Dict[str, Any]]:
    if not base_url or not api_key:
        return None
    
    api_url = f"{base_url.rstrip('/')}/System/Info"
    params = {"api_key": api_key}
    
    logger.debug("正在获取 Emby 服务器信息...")
    try:
        # 修改点：将 kwargs 传递给 emby_client.get
        # 这样就可以支持 timeout=5 这种参数了
        response = emby_client.get(api_url, params=params, **kwargs)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        # 修改日志级别为 warning，因为在离线启动时这是预期内的错误
        logger.warning(f"获取 Emby 服务器信息失败 (可能是服务器离线): {e}")
        return None

# --- 根据名称查找一个特定的电影合集 ---
def get_collection_by_name(name: str, base_url: str, api_key: str, user_id: str) -> Optional[Dict[str, Any]]:
    all_collections = get_all_collections_from_emby_generic(base_url, api_key, user_id)
    if all_collections is None:
        return None
    
    for collection in all_collections:
        if collection.get('Name', '').lower() == name.lower():
            logger.debug(f"  ➜ 根据名称 '{name}' 找到了已存在的合集 (ID: {collection.get('Id')})。")
            return collection
    
    logger.trace(f"未找到名为 '{name}' 的合集。")
    return None

# --- 获取合集成员列表 ---
def get_collection_members(collection_id: str, base_url: str, api_key: str, user_id: str) -> Optional[List[str]]:
    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {'api_key': api_key, 'ParentId': collection_id, 'Fields': 'Id'}
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        items = response.json().get("Items", [])
        return [item['Id'] for item in items]
    except Exception as e:
        logger.error(f"获取合集 {collection_id} 成员时失败: {e}")
        return None

# --- 向合集添加成员 ---
def add_items_to_collection(collection_id: str, item_ids: List[str], base_url: str, api_key: str) -> bool:
    if not item_ids: return True
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    params = {'api_key': api_key, 'Ids': ",".join(item_ids)}
    wait_for_server_idle(base_url, api_key)
    try:
        response = emby_client.post(api_url, params=params)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

# --- 从合集移除成员 ---
def remove_items_from_collection(collection_id: str, item_ids: List[str], base_url: str, api_key: str) -> bool:
    if not item_ids: return True
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    params = {'api_key': api_key, 'Ids': ",".join(item_ids)}
    wait_for_server_idle(base_url, api_key)
    try:
        response = emby_client.delete(api_url, params=params)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

# --- 清空合集内容 ---
def empty_collection_in_emby(collection_id: str, base_url: str, api_key: str, user_id: str) -> bool:
    logger.trace(f"  ➜ 开始清空 Emby 合集 {collection_id} 的所有成员...")
    
    member_ids = get_collection_members(collection_id, base_url, api_key, user_id)
    
    if member_ids is None:
        logger.error("  🚫 无法获取合集成员，清空操作中止。")
        return False
        
    if not member_ids:
        logger.info("  - 合集本身已为空，无需清空。")
        return True

    logger.trace(f"  ➜ 正在从合集 {collection_id} 中移除 {len(member_ids)} 个成员...")
    success = remove_items_from_collection(collection_id, member_ids, base_url, api_key)
    
    if success:
        logger.info(f"  ✅ 成功从Emby删除合集 {collection_id} 。")
    else:
        logger.error(f"  ❌ 发送清空合集 {collection_id} 的请求失败。")
        
    return success

# --- 彻底删除合集 ---
def delete_collection_by_name(collection_name: str, base_url: str, api_key: str, user_id: str) -> bool:
    """
    根据合集名称查找并彻底删除合集。
    策略：先调用 empty_collection_in_emby 清空内容 (触发Emby自动清理)，
    如果合集依然存在 (例如原本就是空的)，则强制调用删除接口。
    """
    wait_for_server_idle(base_url, api_key)
    try:
        # 1. 查找合集
        collection = get_collection_by_name(collection_name, base_url, api_key, user_id)
        if not collection:
            return True # 合集本来就不存在，视为删除成功
            
        collection_id = collection.get('Id')
        if not collection_id:
            return False

        logger.info(f"  ➜ 正在清理合集: {collection_name} (ID: {collection_id})...")

        # 2. 核心步骤：清空合集内容
        # Emby 的机制通常是：当合集内最后一个物品被移除时，合集会自动消失
        empty_collection_in_emby(collection_id, base_url, api_key, user_id)
        
        # 3. 补刀检查：如果清空后合集还在 (比如它本来就是空的，或者Emby没自动删)，则强制删除
        # 稍微等待一下 Emby 处理
        time.sleep(0.5)
        
        # 再次检查是否存在
        check_again = get_emby_item_details(collection_id, base_url, api_key, user_id, silent_404=True)
        if check_again:
            logger.info(f"  ➜ 合集 {collection_name} 清空后依然存在 (可能是空壳)，执行强制删除...")
            return delete_item(collection_id, base_url, api_key, user_id)
        else:
            logger.info(f"  ✅ 合集 {collection_name} 已通过清空内容自动移除。")
            return True
        
    except Exception as e:
        logger.error(f"删除合集 '{collection_name}' 失败: {e}")
        return False

# --- 创建或更新合集 ---
def create_or_update_collection_with_emby_ids(
    collection_name: str, 
    emby_ids_in_library: List[str],
    base_url: str, 
    api_key: str, 
    user_id: str,
    prefetched_collection_map: Optional[dict] = None,
    allow_empty: bool = False
) -> Optional[str]:
    logger.info(f"  ➜ 开始在Emby中处理名为 '{collection_name}' 的合集...")
    wait_for_server_idle(base_url, api_key)
    try:
        # ==============================================================================
        # ★★★ 核心修复：将“特洛伊木马”逻辑提权到最顶层 ★★★
        # 无论是创建还是更新，只要目标列表为空且允许为空，就先抓壮丁
        # ==============================================================================
        final_emby_ids = list(emby_ids_in_library)
        if not final_emby_ids and allow_empty:
            # 想要生成 9 宫格封面，至少需要 9 个占位符
            PLACEHOLDER_COUNT = 9 
            logger.info(f"  ➜ 合集 '{collection_name}' 内容为空，正在抓取 {PLACEHOLDER_COUNT} 个随机媒体项作为占位...")
            
            try:
                target_lib_ids = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS) or []
                search_scopes = target_lib_ids if target_lib_ids else [None]
                
                found_items_batch = [] # 改用列表存储
                
                # 1. 优先尝试：带分级过滤 (PG-13)
                for parent_id in search_scopes:
                    params = {
                        'api_key': api_key, 
                        'Limit': PLACEHOLDER_COUNT, # ★ 请求 9 个
                        'Recursive': 'true', 
                        'IncludeItemTypes': 'Movie,Series',
                        'SortBy': 'Random',     
                        'ImageTypes': 'Primary',
                        'MaxOfficialRating': 'PG-13'
                    }
                    if parent_id: params['ParentId'] = parent_id
                    
                    try:
                        temp_resp = emby_client.get(f"{base_url.rstrip('/')}/Items", params=params)
                        if temp_resp.status_code == 200:
                            items = temp_resp.json().get('Items', [])
                            if items:
                                found_items_batch = items # ★ 保留所有结果
                                scope_name = f"媒体库 {parent_id}" if parent_id else "全局"
                                logger.info(f"  ➜ 在 {scope_name} 中成功抓取到 {len(items)} 个随机素材 (已过滤R级+)。")
                                break
                    except Exception: continue

                # 2. 兜底尝试
                if not found_items_batch and target_lib_ids:
                     logger.warning("  ➜ 严格分级模式下未找到素材，尝试在受控库中放宽分级限制重试...")
                     for parent_id in target_lib_ids:
                        params = {
                            'api_key': api_key, 
                            'Limit': PLACEHOLDER_COUNT, # ★ 请求 9 个
                            'Recursive': 'true', 
                            'IncludeItemTypes': 'Movie,Series', 'SortBy': 'Random', 'ImageTypes': 'Primary',
                            'ParentId': parent_id
                        }
                        try:
                            temp_resp = emby_client.get(f"{base_url.rstrip('/')}/Items", params=params)
                            items = temp_resp.json().get('Items', [])
                            if items:
                                found_items_batch = items # ★ 保留所有结果
                                logger.info(f"  ➜ 重试成功：在媒体库 {parent_id} 中抓取到 {len(items)} 个素材 (无分级限制)。")
                                break
                        except Exception: continue
                
                # ★★★ 将抓取到的所有 ID 加入列表 ★★★
                if found_items_batch:
                    found_ids = [i['Id'] for i in found_items_batch]
                    final_emby_ids.extend(found_ids) # 使用 extend 批量添加
                else:
                    if not allow_empty:
                        logger.warning(f"无法获取占位素材，且不允许创建空合集，跳过处理 '{collection_name}'。")
                        return None
                    else:
                        logger.warning(f"无法获取占位素材，合集 '{collection_name}' 将保持真正的空状态。")

            except Exception as e:
                logger.error(f"  ➜ 获取随机素材失败: {e}")

        # ==============================================================================
        
        # 1. 先尝试查找合集
        collection = prefetched_collection_map.get(collection_name.lower()) if prefetched_collection_map is not None else get_collection_by_name(collection_name, base_url, api_key, user_id)
        
        emby_collection_id = None

        if collection:
            # ==============================================================================
            # 分支 A: 合集已存在 -> 更新 (使用 final_emby_ids)
            # ==============================================================================
            emby_collection_id = collection['Id']
            logger.info(f"  ➜ 发现已存在的合集 '{collection_name}' (ID: {emby_collection_id})，开始同步...")
            
            current_emby_ids = get_collection_members(emby_collection_id, base_url, api_key, user_id)
            if current_emby_ids is None:
                raise Exception("无法获取当前合集成员，同步中止。")

            set_current = set(current_emby_ids)
            set_desired = set(final_emby_ids) # ★ 使用处理后的列表
            
            ids_to_remove = list(set_current - set_desired)
            ids_to_add = list(set_desired - set_current)

            if ids_to_remove:
                logger.info(f"  ➜ 发现 {len(ids_to_remove)} 个旧素材需要移除...")
                remove_items_from_collection(emby_collection_id, ids_to_remove, base_url, api_key)
            
            if ids_to_add:
                logger.info(f"  ➜ 发现 {len(ids_to_add)} 个新素材需要添加...")
                add_items_to_collection(emby_collection_id, ids_to_add, base_url, api_key)

            if not ids_to_remove and not ids_to_add:
                logger.info("  ➜ 合集素材已是最新，无需改动。")

            return emby_collection_id
            
        else:
            # ==============================================================================
            # 分支 B: 合集不存在 -> 创建 (使用 final_emby_ids)
            # ==============================================================================
            logger.info(f"  ➜ 未找到合集 '{collection_name}'，将开始创建...")
            
            # 如果经过抓取后还是空的，且不允许为空，则放弃
            if not final_emby_ids:
                if not allow_empty:
                    logger.warning(f"合集 '{collection_name}' 在媒体库中没有任何匹配项，跳过创建。")
                    return None
                # 如果 allow_empty=True 但没抓到壮丁，尝试创建空合集（Emby可能会报错，但值得一试）

            api_url = f"{base_url.rstrip('/')}/Collections"
            params = {'api_key': api_key}
            payload = {'Name': collection_name, 'Ids': ",".join(final_emby_ids)} # ★ 使用处理后的列表
            
            response = emby_client.post(api_url, params=params, json=payload)
            response.raise_for_status()
            new_collection_info = response.json()
            emby_collection_id = new_collection_info.get('Id')
            
            return emby_collection_id

    except Exception as e:
        logger.error(f"处理Emby合集 '{collection_name}' 时发生未知错误: {e}", exc_info=True)
        return None

# --- 根据ID列表批量获取Emby项目 ---    
def get_emby_items_by_id(
    base_url: str,
    api_key: str,
    user_id: str, # 参数保留以兼容旧的调用，但内部不再使用
    item_ids: List[str],
    fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    【V4 - 4.9+ 终极兼容版】
    根据ID列表批量获取Emby项目。
    - 核心变更: 适配 Emby 4.9+ API, 切换到 /Items 端点。
    - 关键修正: 在查询 Person 等全局项目时，不能传递 UserId，否则新版API会返回空结果。
      此函数现在不再将 UserId 传递给 API，以确保能获取到演员详情。
    """
    if not all([base_url, api_key]) or not item_ids: # UserId 不再是必须检查的参数
        return []

    all_items = []
    # 定义一个安全的分批大小，比如每次请求100个ID
    BATCH_SIZE = 100

    # 将长列表切分成多个小批次
    id_chunks = [item_ids[i:i + BATCH_SIZE] for i in range(0, len(item_ids), BATCH_SIZE)]
    
    if len(id_chunks) > 1:
        logger.trace(f"  ➜ ID列表总数({len(item_ids)})过长，已切分为 {len(id_chunks)} 个批次进行请求。")

    # ★★★ 核心修改: 切换到 /Items 端点以兼容 Emby 4.9+ ★★★
    api_url = f"{base_url.rstrip('/')}/Items"
    
    # 循环处理每个批次
    for i, batch_ids in enumerate(id_chunks):
        params = {
            "api_key": api_key,
            "Ids": ",".join(batch_ids), # 只使用当前批次的ID
            "Fields": fields or "ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName"
            # ★★★ 核心修正: 不再传递 UserId。演员等Person对象是全局的，使用UserId会导致查询失败。★★★
        }

        try:
            
            if len(id_chunks) > 1:
                logger.trace(f"  ➜ 正在请求批次 {i+1}/{len(id_chunks)} (包含 {len(batch_ids)} 个ID)...")
            response = emby_client.get(api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            batch_items = data.get("Items", [])
            all_items.extend(batch_items) # 将获取到的结果合并到总列表中
            
        except requests.exceptions.RequestException as e:
            # 记录当前批次的错误，但继续处理下一批
            logger.error(f"根据ID列表批量获取Emby项目时，处理批次 {i+1} 失败: {e}")
            continue

    logger.trace(f"  ➜ 所有批次请求完成，共获取到 {len(all_items)} 个媒体项。")
    return all_items

# --- 向合集追加单个成员 ---    
def append_item_to_collection(collection_id: str, item_emby_id: str, base_url: str, api_key: str, user_id: str) -> bool:
    logger.trace(f"准备将项目 {item_emby_id} 追加到合集 {collection_id}...")
    
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    
    params = {
        'api_key': api_key,
        'Ids': item_emby_id
    }
    
    try:
        response = emby_client.post(api_url, params=params)
        response.raise_for_status()
        
        logger.trace(f"成功发送追加请求：将项目 {item_emby_id} 添加到合集 {collection_id}。")
        return True
        
    except requests.RequestException as e:
        if e.response is not None:
            logger.error(f"向合集 {collection_id} 追加项目 {item_emby_id} 时失败: HTTP {e.response.status_code} - {e.response.text[:200]}")
        else:
            logger.error(f"向合集 {collection_id} 追加项目 {item_emby_id} 时发生网络错误: {e}")
        return False
    except Exception as e:
        logger.error(f"向合集 {collection_id} 追加项目时发生未知错误: {e}", exc_info=True)
        return False

# --- 获取所有媒体库及其源文件夹路径 ---    
def get_all_libraries_with_paths(base_url: str, api_key: str) -> List[Dict[str, Any]]:
    logger.debug("  ➜ 正在实时获取所有媒体库及其源文件夹路径...")
    try:
        folders_url = f"{base_url.rstrip('/')}/Library/VirtualFolders"
        params = {"api_key": api_key}
        response = emby_client.get(folders_url, params=params)
        response.raise_for_status()
        virtual_folders_data = response.json()

        libraries_with_paths = []
        for folder in virtual_folders_data:
            lib_id = folder.get("ItemId")
            lib_name = folder.get("Name")
            lib_guid = folder.get("Guid")
            locations = folder.get("Locations", [])

            # 只要有 ID、名字，并且配置了物理路径，它就是一个有效的实体媒体库！
            if lib_id and lib_name and locations:
                libraries_with_paths.append({
                    "info": {
                        "Name": lib_name,
                        "Id": lib_id,
                        "Guid": lib_guid,
                        # 如果为空，给个默认标识 'mixed'，防止后续逻辑报错
                        "CollectionType": folder.get("CollectionType") or "mixed" 
                    },
                    "paths": locations
                })
        
        logger.debug(f"  ➜ 实时获取到 {len(libraries_with_paths)} 个媒体库的路径信息。")
        return libraries_with_paths

    except Exception as e:
        logger.error(f"实时获取媒体库路径时发生错误: {e}", exc_info=True)
        return []

# --- 定位媒体库 ---
def get_library_root_for_item(item_id: str, base_url: str, api_key: str, user_id: str) -> Optional[Dict[str, Any]]:
    logger.debug(f"  ➜ 正在为项目ID {item_id} 定位媒体库...")
    try:
        all_libraries_data = get_all_libraries_with_paths(base_url, api_key)
        if not all_libraries_data:
            logger.error("无法获取任何媒体库的路径信息，定位失败。")
            return None

        item_details = get_emby_item_details(item_id, base_url, api_key, user_id, fields="Path")
        if not item_details or not item_details.get("Path"):
            logger.error(f"无法获取项目 {item_id} 的文件路径，定位失败。")
            return None
        item_path = item_details["Path"]

        best_match_library = None
        longest_match_length = 0
        for lib_data in all_libraries_data:
            for library_source_path in lib_data["paths"]:
                source_path_with_slash = os.path.join(library_source_path, "")
                if item_path.startswith(source_path_with_slash):
                    if len(source_path_with_slash) > longest_match_length:
                        longest_match_length = len(source_path_with_slash)
                        best_match_library = lib_data["info"]
        
        if best_match_library:
            logger.trace(f"  ➜ 匹配到媒体库 '{best_match_library.get('Name')}'。")
            return best_match_library
        else:
            logger.error(f"项目路径 '{item_path}' 未能匹配任何媒体库的源文件夹。")
            return None

    except Exception as e:
        logger.error(f"  ➜ 定位媒体库时发生未知严重错误: {e}", exc_info=True)
        return None

# --- 更新媒体项详情 ---    
def update_emby_item_details(item_id: str, new_data: Dict[str, Any], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([item_id, new_data, emby_server_url, emby_api_key, user_id]):
        return False
    wait_for_server_idle(emby_server_url, emby_api_key)
    try:
        # 1. 获取当前完整详情
        current_item_details = get_emby_item_details(item_id, emby_server_url, emby_api_key, user_id)
        if not current_item_details:
            return False
        
        # 2. 合并新数据
        item_to_update = current_item_details.copy()
        item_to_update.update(new_data)
        
        # ★★★ 核心修复：踢除所有干扰字段 ★★★
        # 这些字段是 Emby 生成的，带回去会导致 Tags、People 等字段更新失效或被覆盖
        black_list = [
            'TagItems',      # 标签对象列表 (Tags 的死对头)
            # 'People',        # 演员列表 (除非你是在更新演员，否则不要带回去)
            'MediaStreams',  # 媒体流信息
            'MediaSources',  # 媒体源信息
            'Chapters',      # 章节信息
            'RecursiveItemCount',
            'ChildCount',
            'ImageTags',
            'SeriesTimerId',
            'RunTimeTicks'
        ]
        
        for key in black_list:
            # 只有当 new_data 里没有显式要更新这些字段时，才删除它们
            if key not in new_data:
                item_to_update.pop(key, None)

        # 3. 执行 POST
        update_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}"
        params = {"api_key": emby_api_key}
        headers = {'Content-Type': 'application/json'}

        response_post = emby_client.post(update_url, json=item_to_update, headers=headers, params=params)
        response_post.raise_for_status()
        
        return True

    except Exception as e:
        logger.error(f"更新项目详情失败 (ID: {item_id}): {e}")
        return False

# --- 删除媒体项神医接口 (带自动回退) ---    
def delete_item_sy(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    """
    删除媒体项神医接口
    逻辑：优先尝试神医专用接口 /DeleteVersion，如果失败（如未安装插件或报错），
    则自动降级调用官方接口 /Delete 进行重试。
    """
    wait_for_server_idle(emby_server_url, emby_api_key)
    logger.warning(f"  ➜ 检测到删除请求，优先尝试使用 [神医Pro接口] 执行...")

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = get_admin_access_token()
    
    if not access_token:
        logger.error("  🚫 无法获取临时 AccessToken，删除操作中止。请检查管理员账号密码是否正确。")
        return False

    # 2. 使用临时令牌执行删除
    # 使用神医Pro专用的 POST /Items/{Id}/DeleteVersion 接口
    api_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}/DeleteVersion"
    
    headers = {
        'X-Emby-Token': access_token  # ★ 使用临时的 AccessToken
    }
    
    params = {
        'UserId': logged_in_user_id # ★ 使用登录后返回的 UserId
    }
    
    try:
        response = emby_client.post(api_url, headers=headers, params=params)
        response.raise_for_status()
        logger.info(f"  ✅ [神医接口] 成功删除 Emby 媒体项 ID: {item_id}。")
        return True
    except Exception as e:
        # 区分一下错误类型，方便排查，但处理逻辑是一样的：都去试官方接口
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
            logger.warning(f"  ⚠️ [神医接口] 调用失败 (404): 服务端未安装神医Pro插件或接口不匹配。")
        else:
            logger.warning(f"  ⚠️ [神医接口] 调用异常: {e}")
            
        logger.info(f"  ➜ 正在自动切换至 [官方接口] 重试删除 ID: {item_id} ...")
        
        # ★★★ 核心修改：失败后直接调用官方接口函数 ★★★
        return delete_item(item_id, emby_server_url, emby_api_key, user_id)

# --- 删除媒体项官方接口 ---
def delete_item(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    """
    删除媒体项官方接口
    """
    logger.warning(f"  ➜ 检测到删除请求，将尝试使用 [自动登录] 执行...")

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = get_admin_access_token()
    
    if not access_token:
        logger.error("  🚫 无法获取临时 AccessToken，删除操作中止。请检查管理员账号密码是否正确。")
        return False

    # 2. 使用临时令牌执行删除
    # 使用最被社区推荐的 POST /Items/{Id}/Delete 接口
    api_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}/Delete"
    
    headers = {
        'X-Emby-Token': access_token  # ★ 使用临时的 AccessToken
    }
    
    params = {
        'UserId': logged_in_user_id # ★ 使用登录后返回的 UserId
    }
    
    try:
        response = emby_client.post(api_url, headers=headers, params=params)
        response.raise_for_status()
        logger.info(f"  ✅ 成功删除 Emby 媒体项 ID: {item_id}。")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ✅ 使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"  ✅ 使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生未知错误: {e}")
        return False    

# --- 清理幽灵演员 ---
def delete_person_custom_api(base_url: str, api_key: str, person_id: str) -> bool:
    """
    【V-Final Frontier 终极版 - 同样使用账密获取令牌】
    通过模拟管理员登录获取临时 AccessToken 来删除演员。
    这个接口只在神医Pro版插件中存在。
    """
    wait_for_server_idle(base_url, api_key)
    logger.trace(f"检测到删除演员请求，将尝试使用 [自动登录模式] 执行...")

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = get_admin_access_token()
    
    if not access_token:
        logger.error("  🚫 无法获取临时 AccessToken，删除演员操作中止。请检查管理员账号密码是否正确。")
        return False

    # 2. 使用临时令牌执行删除
    # 调用非标准的 /Items/{Id}/DeletePerson POST 接口
    api_url = f"{base_url.rstrip('/')}/Items/{person_id}/DeletePerson"
    
    headers = {
        'X-Emby-Token': access_token  # ★ 使用临时的 AccessToken
    }
    
    # 注意：神医的这个接口可能不需要 UserId，但为了统一和以防万一，可以加上
    # 如果确认不需要，可以移除 params
    params = {
        'UserId': logged_in_user_id # ★ 使用登录后返回的 UserId
    }
    
    try:
        # 这个接口是 POST 请求
        response = emby_client.post(api_url, headers=headers, params=params)
        response.raise_for_status()
        logger.info(f"  ✅ 成功删除演员 ID: {person_id}。")
        return True
    except requests.exceptions.HTTPError as e:
        # 404 Not Found 意味着这个专用接口在您的服务器上不存在
        if e.response.status_code == 404:
            logger.error(f"删除演员 {person_id} 失败：需神医Pro版本才支持此功能。")
        else:
            logger.error(f"使用临时令牌删除演员 {person_id} 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"使用临时令牌删除演员 {person_id} 时发生未知错误: {e}")
        return False

# --- 获取所有 Emby 用户列表 ---
def get_all_emby_users_from_server(base_url: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    【V1】从 Emby 服务器获取所有用户的列表。
    """
    if not base_url or not api_key:
        return None
    
    api_url = f"{base_url.rstrip('/')}/Users"
    params = {"api_key": api_key}
    
    logger.debug("正在从 Emby 服务器获取所有用户列表...")
    try:
        response = emby_client.get(api_url, params=params)
        response.raise_for_status()
        users = response.json()
        logger.info(f"  ➜ 成功从 Emby 获取到 {len(users)} 个用户。")
        return users
    except Exception as e:
        logger.error(f"从 Emby 获取用户列表失败: {e}", exc_info=True)
        return None

# --- 获取指定用户的所有媒体的用户数据 ---
def get_all_user_view_data(user_id: str, base_url: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    【V5 - 魔法日志版】
    - 增加 CRITICAL 级别的日志，用于打印从 Emby 获取到的最原始的 Item JSON 数据。
    """
    if not all([user_id, base_url, api_key]):
        return None

    all_items_with_data = []
    item_types = "Movie,Series,Episode"
    fields = "UserData,Type,SeriesId,ProviderIds,Name,LastPlayedDate,PlayCount" 
    
    api_url = f"{base_url.rstrip('/')}/Items"
    
    params = {
        "api_key": api_key,
        "Recursive": "true",
        "IncludeItemTypes": item_types,
        "Fields": fields,
        "UserId": user_id
    }
    
    start_index = 0
    batch_size = 2000

    # ★★★ 2. 设置一个计数器，我们不需要打印所有日志，有几个样本就够了 ★★★
    log_counter = 0
    LOG_LIMIT = 5 # 只打印前 5 个有用户数据的条目

    logger.debug(f"开始为用户 {user_id} 分批获取所有媒体的用户数据")
    while True:
        try:
            request_params = params.copy()
            request_params["StartIndex"] = start_index
            request_params["Limit"] = batch_size

            response = emby_client.get(api_url, params=request_params)
            response.raise_for_status()
            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                break

            for item in items:
                user_data = item.get("UserData", {})
                # 我们只关心那些确实有播放记录或收藏的条目
                if user_data.get('Played') or user_data.get('IsFavorite') or user_data.get('PlaybackPositionTicks', 0) > 0:
                    
                    all_items_with_data.append(item)
            
            start_index += len(items)
            if len(items) < batch_size:
                break

        except Exception as e:
            logger.error(f"为用户 {user_id} 获取媒体数据时，处理批次 StartIndex={start_index} 失败: {e}", exc_info=True)
            break
            
    logger.debug(f"为用户 {user_id} 的全量同步完成，共找到 {len(all_items_with_data)} 个有状态的媒体项。")
    return all_items_with_data

# --- 在 Emby 中创建一个新用户 ---
def create_user_with_policy(
    username: str, 
    password: str, 
    # policy: Dict[str, Any],  <-- ★★★ 1. 删除 policy 参数 ★★★
    base_url: str, 
    api_key: str
) -> Optional[str]:
    """
    在 Emby 中创建一个新用户，只负责创建和设置密码，不处理权限策略。
    权限策略由调用方在之后通过 force_set_user_policy 单独设置。
    """
    logger.info(f"准备在 Emby 中创建新用户 '{username}'...")
    
    create_url = f"{base_url}/Users/New"
    headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
    
    # ★★★ 2. 创建用户的请求体中，只包含 Name ★★★
    create_payload = {
        "Name": username
    }
    
    try:
        # ★★★ 3. 请求体不再包含 Policy ★★★
        response = emby_client.post(create_url, headers=headers, json=create_payload)
        
        if response.status_code == 200:
            new_user_data = response.json()
            new_user_id = new_user_data.get("Id")
            if not new_user_id:
                logger.error("Emby 用户创建成功，但响应中未返回用户 ID。")
                return None
            
            logger.info(f"  ➜ 用户 '{username}' 创建成功，新用户 ID: {new_user_id}。正在设置密码...")

            password_url = f"{base_url}/Users/{new_user_id}/Password"
            password_payload = {
                "Id": new_user_id,
                "CurrentPw": "",  
                "NewPw": password
            }
            
            pw_response = emby_client.post(password_url, headers=headers, json=password_payload)
            
            if pw_response.status_code == 204:
                logger.info(f"  ✅ 成功为用户 '{username}' 设置密码。")
                return new_user_id
            else:
                logger.error(f"为用户 '{username}' 设置密码失败。状态码: {pw_response.status_code}, 响应: {pw_response.text}")
                return None
        else:
            logger.error(f"创建 Emby 用户 '{username}' 失败。状态码: {response.status_code}, 响应: {response.text}")
            return None

    except Exception as e:
        logger.error(f"创建 Emby 用户 '{username}' 时发生网络或未知错误: {e}", exc_info=True)
        return None

# --- 禁用或启用 Emby 用户 ---
def set_user_disabled_status(
    user_id: str, 
    disable: bool, 
    base_url: str, 
    api_key: str
) -> bool:
    """
    禁用或启用一个 Emby 用户。
    """
    action_text = "禁用" if disable else "启用"
    
    # 尝试获取用户名用于日志
    user_name_for_log = user_id
    try:
        user_details = get_user_details(user_id, base_url, api_key)
        if user_details and user_details.get('Name'):
            user_name_for_log = user_details['Name']
    except Exception:
        pass

    logger.info(f"正在为用户 '{user_name_for_log}' (ID: {user_id}) 执行【{action_text}】操作...")
    
    try:
        if not user_details or 'Policy' not in user_details:
            logger.error(f"无法获取用户 '{user_name_for_log}' 的当前策略，{action_text}失败。")
            return False
        
        current_policy = user_details['Policy']
        current_policy['IsDisabled'] = disable
        
        policy_update_url = f"{base_url}/Users/{user_id}/Policy"
        headers = {
            "X-Emby-Token": api_key,
            "Content-Type": "application/json"
        }

        response = emby_client.post(policy_update_url, headers=headers, json=current_policy)

        if response.status_code == 204:
            logger.info(f"✅ 成功{action_text}用户 '{user_name_for_log}'。")
            return True
        else:
            logger.error(f"{action_text}用户 '{user_name_for_log}' 失败。状态码: {response.status_code}, 响应: {response.text}")
            return False

    except Exception as e:
        logger.error(f"{action_text}用户 '{user_name_for_log}' 时发生严重错误: {e}", exc_info=True)
        return False

    except Exception as e:
        logger.error(f"{action_text}用户 {user_id} 时发生严重错误: {e}", exc_info=True)
        return False

# --- 获取用户完整详情 (含 Policy 和 Configuration) ---
def get_user_details(user_id: str, base_url: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    获取用户的完整详情，同时包含 Policy 和 Configuration。
    能够兼容不支持独立 /Configuration 接口的旧版 Emby。
    """
    details = {}
    headers = {"X-Emby-Token": api_key, "Accept": "application/json"}
    
    # 1. 总是先调用基础的用户信息接口
    user_info_url = f"{base_url}/Users/{user_id}"
    try:
        response = emby_client.get(user_info_url, headers=headers)
        response.raise_for_status()
        user_data = response.json()
        details.update(user_data)
    except requests.RequestException as e:
        logger.error(f"获取用户 {user_id} 的基础信息和 Policy 失败: {e}")
        return None

    # ★★★ 核心修正：智能判断是否需要再次请求 ★★★
    # 2. 如果基础信息中已经包含了 Configuration (旧版 Emby 的行为)，我们就不再需要额外请求。
    if 'Configuration' in details:
        logger.trace(f"  ➜ 已从主用户接口获取到 Configuration (旧版 Emby 模式)。")
        return details

    # 3. 如果基础信息中没有，再尝试请求专用的 Configuration 接口 (新版 Emby 的行为)。
    logger.trace(f"  ➜ 主用户接口未返回 Configuration，尝试请求专用接口 (新版 Emby 模式)...")
    config_url = f"{base_url}/Users/{user_id}/Configuration"
    try:
        response = emby_client.get(config_url, headers=headers)
        response.raise_for_status()
        details['Configuration'] = response.json()
    except requests.RequestException as e:
        # 如果专用接口不存在，这不是一个错误，只是版本差异。
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
            logger.warning(f"  ➜ 专用 /Configuration 接口不存在，您的 Emby 版本可能较旧。将跳过首选项同步。")
        else:
            # 其他网络错误则需要记录
            logger.error(f"请求专用 /Configuration 接口时发生未知错误: {e}")
    
    return details

# --- 强制设置用户首选项 (含智能回退) ---
def force_set_user_configuration(user_id: str, configuration_dict: Dict[str, Any], base_url: str, api_key: str) -> bool:
    """
    【V3 - 智能兼容最终版】为一个用户强制设置首选项。
    优先尝试新版专用接口，如果失败则回退到兼容旧版的完整更新模式。
    """
    # 策略1：优先尝试新版的、高效的专用接口
    url = f"{base_url}/Users/{user_id}/Configuration"
    headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
    try:
        response = emby_client.post(url, headers=headers, json=configuration_dict)
        response.raise_for_status()
        logger.info(f"  ➜ 成功为用户 {user_id} 应用了个性化配置 (新版接口)。")
        return True
    except requests.RequestException as e:
        # 如果是因为接口不存在 (404)，则启动备用策略
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
            logger.warning(f"  ➜ 专用 /Configuration 接口不存在，将回退到兼容模式更新用户 {user_id} 的首选项...")
            
            # 策略2：回退到旧版的、兼容的完整更新模式
            # a. 先获取当前用户的完整对象
            full_user_object = get_user_details(user_id, base_url, api_key)
            if not full_user_object:
                logger.error(f"  ➜ 回退模式失败：无法获取用户 {user_id} 的当前完整信息。")
                return False
            
            # b. 将新的首选项合并到这个完整对象中
            full_user_object['Configuration'] = configuration_dict
            
            # c. 提交这个完整的对象进行更新
            update_url = f"{base_url}/Users/{user_id}"
            update_response = emby_client.post(update_url, headers=headers, json=full_user_object)
            
            try:
                update_response.raise_for_status()
                logger.info(f"  ➜ 成功为用户 {user_id} 应用了个性化配置 (兼容模式)。")
                return True
            except requests.RequestException as update_e:
                logger.error(f"  ➜ 在兼容模式下更新用户 {user_id} 时失败: {update_e}")
                return False
        else:
            # 如果是其他错误，则正常报错
            logger.error(f"  ➜ 为用户 {user_id} 应用个性化配置时失败: {e}")
            return False

# --- 检查用户名是否存在 ---
def check_if_user_exists(username: str, base_url: str, api_key: str) -> bool:
    """
    检查指定的用户名是否已在 Emby 中存在。
    
    :param username: 要检查的用户名 (不区分大小写)。
    :return: 如果存在则返回 True，否则返回 False。
    """
    all_users = get_all_emby_users_from_server(base_url, api_key)
    if all_users is None:
        # 如果无法获取用户列表，为安全起见，我们假设用户可能存在，并抛出异常让上层处理
        raise RuntimeError("无法从 Emby 获取用户列表来检查用户名是否存在。")
    
    # 进行不区分大小写的比较
    username_lower = username.lower()
    for user in all_users:
        if user.get('Name', '').lower() == username_lower:
            return True
            
    return False

# --- 强制设置用户权限策略 ---
def force_set_user_policy(user_id: str, policy: Dict[str, Any], base_url: str, api_key: str) -> bool:
    """
    【V2 - 增加日志用户名】为一个已存在的用户强制设置一个全新的、完整的 Policy 对象。
    """
    # 尝试获取用户名用于日志记录，即使失败也不影响核心功能
    user_name_for_log = user_id
    try:
        user_details = get_user_details(user_id, base_url, api_key)
        if user_details and user_details.get('Name'):
            user_name_for_log = user_details['Name']
    except Exception:
        pass # 获取失败则继续使用ID

    logger.trace(f"  ➜ 正在为用户 '{user_name_for_log}' (ID: {user_id}) 强制应用新的权限策略...")
    
    policy_update_url = f"{base_url}/Users/{user_id}/Policy"
    headers = {
        "X-Emby-Token": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        response = emby_client.post(policy_update_url, headers=headers, json=policy)
        
        if response.status_code == 204: # 204 No Content 表示成功
            logger.info(f"  ✅ 成功为用户 '{user_name_for_log}' 应用了新的权限策略。")
            return True
        else:
            logger.error(f"  ➜ 为用户 '{user_name_for_log}' 应用新策略失败。状态码: {response.status_code}, 响应: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"  ➜ 为用户 '{user_name_for_log}' 应用新策略时发生严重错误: {e}", exc_info=True)
        return False

# --- 删除 Emby 用户 ---
def delete_emby_user(user_id: str) -> bool:
    """
    【V3 - 配置统一版】专门用于删除一个 Emby 用户的函数。
    不再接收 base_url 和 api_key 参数，而是直接从全局配置读取。
    """
    # 1. 在函数开头，从全局配置获取所需信息
    config = config_manager.APP_CONFIG
    base_url = config.get("emby_server_url")
    api_key = config.get("emby_api_key")

    # 在删除操作前先获取用户名，因为删除后就获取不到了
    user_name_for_log = user_id
    try:
        # 使用我们刚刚从配置中获取的 base_url 和 api_key
        user_details = get_user_details(user_id, base_url, api_key)
        if user_details and user_details.get('Name'):
            user_name_for_log = user_details['Name']
    except Exception:
        pass

    logger.warning(f"  ➜ 检测到删除用户 '{user_name_for_log}' 的请求，将使用 [自动登录模式] 执行...")
    
    # 2. 直接调用新的、无参数的令牌获取函数
    access_token, _ = get_admin_access_token()
    
    if not access_token:
        logger.error("  🚫 无法获取管理员 AccessToken，删除用户操作中止。")
        return False

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}"
    
    headers = { 'X-Emby-Token': access_token }
    
    try:
        response = emby_client.delete(api_url, headers=headers)
        response.raise_for_status()
        logger.info(f"  ✅ 成功删除 Emby 用户 '{user_name_for_log}' (ID: {user_id})。")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ➜ 删除 Emby 用户 '{user_name_for_log}' 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"  ➜ 删除 Emby 用户 '{user_name_for_log}' 时发生未知错误: {e}")
        return False

# --- 认证 Emby 用户 ---
def authenticate_emby_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    【V4 - 终极伪装与日志版】
    - 伪装成一个标准的 Emby Web 客户端，提供更完整的 Header 和 Payload。
    - 增加最关键的失败日志，直接打印 Emby Server 返回的原始错误文本。
    """
    # 1. 它自己会从全局配置读取 URL，API 端点无需关心
    cfg = config_manager.APP_CONFIG
    emby_url = cfg.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)

    if not all([emby_url, username]):
        logger.error("  ➜ [用户认证] 失败：缺少服务器地址或用户名。")
        return None

    auth_url = f"{emby_url.rstrip('/')}/Users/AuthenticateByName"
    
    device_id = "my-emby-toolkit-auth-v4"
    auth_header = (
        f'Emby Client="Emby Web", '
        f'Device="Chrome", '
        f'DeviceId="{device_id}", '
        f'Version="4.8.0.80"'
    )
    headers = {
        'Content-Type': 'application/json',
        'X-Emby-Authorization': auth_header
    }
    
    payload = {
        "Username": username,
        "LoginType": "Manual"
    }
    if password:
        payload['Pw'] = password
    else:
        payload['Pw'] = ""

    logger.debug(f"  ➜ 准备向 {auth_url} 发送认证请求，Payload: {{'Username': '{username}', 'Pw': '***'}}")
    
    try:
        response = emby_client.post(auth_url, headers=headers, json=payload)
        
        logger.debug(f"  ➜ Emby 服务器响应状态码: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            if data.get("AccessToken") and data.get("User"):
                logger.debug(f"  ➜ 用户 '{username}' 认证成功！")
                # ★★★ 注意：这里返回的是包含 User 和 AccessToken 的完整 data ★★★
                return data
            else:
                logger.error(f"  ➜ 登录成功但响应格式不正确: {data}")
                return None
        else:
            error_message = response.text
            logger.error(f"  ➜ 登录失败，Emby 返回的原始错误信息: {error_message}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"  ➜ 认证用户 '{username}' 时发生网络请求错误: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"  ➜ 认证用户 '{username}' 时发生未知错误: {e}", exc_info=True)
        return None

# --- 测试连接 Emby 服务器 ---
def test_connection(url: str, api_key: str) -> dict:
    """
    测试给定的 URL 和 Key 是否能连通 Emby。
    用于设置页面验证配置有效性。
    """
    if not url or not api_key:
        return {'success': False, 'error': 'URL 或 API Key 为空'}

    # 去掉末尾斜杠，确保格式统一
    url = url.rstrip('/')
    
    # 使用 System/Info 端点，这是一个轻量级且通常开放的端点
    endpoint = f"{url}/emby/System/Info"
    params = {'api_key': api_key}
    
    try:
        # 设置较短的超时时间，避免前端长时间等待
        resp = emby_client.get(endpoint, params=params)
        
        if resp.status_code == 200:
            return {'success': True}
        elif resp.status_code == 401:
            return {'success': False, 'error': 'API Key 无效或无权限'}
        elif resp.status_code == 404:
            return {'success': False, 'error': '找不到 Emby 服务器 (404)，请检查 URL'}
        else:
            return {'success': False, 'error': f'连接失败 (HTTP {resp.status_code})'}
            
    except requests.exceptions.ConnectionError:
        return {'success': False, 'error': '无法连接到服务器，请检查 URL 或网络'}
    except requests.exceptions.Timeout:
        return {'success': False, 'error': '连接超时'}
    except Exception as e:
        return {'success': False, 'error': str(e)}   

# --- 上传用户头像 ---
def upload_user_image(base_url, api_key, user_id, image_data, content_type):
    """
    上传用户头像到 Emby 服务器。
    策略：使用 /Users 接口 + Base64 编码。
    """
    # 1. 构造 URL：改回 /Users 接口
    base_url = base_url.rstrip('/')
    url = f"{base_url}/Users/{user_id}/Images/Primary"
    
    # 2. Base64 编码
    try:
        b64_data = base64.b64encode(image_data)
    except Exception as e:
        logger.error(f"图片 Base64 编码失败: {e}")
        return False

    headers = {
        'X-Emby-Token': api_key,
        'Content-Type': content_type # 保持 image/jpeg 或 image/png，Emby靠这个识别文件后缀
    }
    
    # 3. (可选) 先尝试删除旧头像，防止覆盖失败
    try:
        emby_client.delete(url, headers=headers, timeout=10)
    except Exception:
        pass # 删除失败也不影响，可能是本来就没有头像

    # 4. 发送上传请求
    try:
        # 增加超时时间
        response = emby_client.post(url, headers=headers, data=b64_data, timeout=60)
        response.raise_for_status()
        return True
    except Exception as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" | Response: {e.response.text}"
        logger.error(f"向 Emby 上传用户 {user_id} 头像失败: {error_msg}")
        return False

# --- 获取单个用户信息 ---
def get_user_info_from_server(base_url, api_key, user_id):
    """
    从 Emby 服务器获取单个用户的最新信息（主要为了获取新的 ImageTag）。
    """
    url = f"{base_url}/Users/{user_id}"
    headers = {'X-Emby-Token': api_key}
    try:
        response = emby_client.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"从 Emby 获取用户 {user_id} 信息失败: {e}")
    return None

# --- 获取所有文件夹映射 ---
def get_all_folder_mappings(base_url: str, api_key: str) -> dict:
    if not base_url or not api_key:
        return {}

    folder_map = {}

    # --- 阶段 1: 顶层媒体库 (VirtualFolders) ---
    try:
        lib_url = f"{base_url.rstrip('/')}/Library/VirtualFolders"
        response = emby_client.get(lib_url, params={"api_key": api_key})
        libs = response.json()
        for lib in libs:
            guid = lib.get('Guid') or lib.get('ItemId')
            num_id = lib.get('ItemId')
            if lib.get('Locations'):
                for loc in lib.get('Locations'):
                    norm_loc = os.path.normpath(loc)
                    folder_map[norm_loc] = {'id': str(num_id), 'guid': str(guid), 'type': 'Library'}
    except Exception: pass

    # --- 阶段 2: 【新增】权限专用文件夹 (SelectableMediaFolders) ---
    # 这是抓取 294461 这种权限 ID 的核心逻辑
    try:
        sel_url = f"{base_url.rstrip('/')}/Library/SelectableMediaFolders"
        response = emby_client.get(sel_url, params={"api_key": api_key})
        selectable_folders = response.json()
        for folder in selectable_folders:
            path = folder.get('Path')
            if path:
                norm_path = os.path.normpath(path)
                # 如果该路径已存在，我们更新它，或者添加一个备用 ID 字段
                if norm_path in folder_map:
                    folder_map[norm_path]['selectable_id'] = str(folder.get('Id'))
                else:
                    folder_map[norm_path] = {
                        'id': str(folder.get('Id')), 
                        'guid': str(folder.get('Guid') or ""),
                        'type': 'SelectableFolder'
                    }
        logger.debug(f"  ➜ [权限调试] 已加载 {len(selectable_folders)} 个权限专用文件夹映射。")
    except Exception as e:
        logger.error(f"获取 SelectableMediaFolders 失败: {e}")

    # --- 阶段 3: 普通子文件夹 (Items) ---
    try:
        items_url = f"{base_url.rstrip('/')}/Items"
        items_params = {"api_key": api_key, "Recursive": "true", "IsFolder": "true", "Fields": "Path,Id,Guid", "Limit": 10000}
        response = emby_client.get(items_url, params=items_params)
        items = response.json().get("Items", [])
        for item in items:
            path = item.get('Path')
            if path:
                norm_path = os.path.normpath(path)
                if norm_path not in folder_map:
                    folder_map[norm_path] = {'id': str(item.get('Id')), 'guid': str(item.get('Guid')), 'type': 'Folder'}
    except Exception: pass
        
    return folder_map

# --- 为 Emby 项目添加标签 ---
def add_tags_to_item(item_id: str, tags_to_add: List[str], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not tags_to_add:
        return True
    wait_for_server_idle(emby_server_url, emby_api_key)    
    try:
        # 1. 显式请求 Tags 和 TagItems
        item_details = get_emby_item_details(item_id, emby_server_url, emby_api_key, user_id, fields="Tags,TagItems,LockedFields")
        if not item_details:
            return False
            
        # 2. 【核心增强】双路提取旧标签
        existing_tags = set()
        
        # 从 Tags 字符串列表提取
        if item_details.get("Tags"):
            existing_tags.update(item_details["Tags"])
            
        # 从 TagItems 对象列表提取 (防止 Tags 字段为空但 TagItems 有值的情况)
        if item_details.get("TagItems"):
            for ti in item_details["TagItems"]:
                if isinstance(ti, dict) and ti.get("Name"):
                    existing_tags.add(ti["Name"])

        # 3. 合并新标签
        new_tags_set = existing_tags.copy()
        added_any = False
        for t in tags_to_add:
            if t not in new_tags_set:
                new_tags_set.add(t)
                added_any = True
        
        if not added_any:
            logger.trace(f"项目 {item_id} 标签已存在，无需更新。")
            return True

        # 4. 准备更新负载
        update_payload = {"Tags": list(new_tags_set)}
        
        # 处理锁定逻辑
        locked_fields = item_details.get("LockedFields", [])
        if "Tags" in locked_fields:
            locked_fields.remove("Tags")
            update_payload["LockedFields"] = locked_fields

        # 5. 调用更新函数
        return update_emby_item_details(item_id, update_payload, emby_server_url, emby_api_key, user_id)

    except Exception as e:
        logger.error(f"追加标签失败 (ID: {item_id}): {e}")
        return False

# --- 从 Emby 项目移除标签 ---    
def remove_tags_from_item(item_id: str, tags_to_remove: List[str], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    """
    从 Emby 项目中精准移除指定的标签。
    """
    if not tags_to_remove:
        return True
    wait_for_server_idle(emby_server_url, emby_api_key)    
    try:
        # 1. 获取当前标签
        item_details = get_emby_item_details(item_id, emby_server_url, emby_api_key, user_id, fields="Tags,TagItems")
        if not item_details:
            return False
            
        # 2. 提取现有标签名
        existing_tags = set()
        if item_details.get("Tags"):
            existing_tags.update(item_details["Tags"])
        if item_details.get("TagItems"):
            for ti in item_details["TagItems"]:
                if isinstance(ti, dict) and ti.get("Name"):
                    existing_tags.add(ti["Name"])

        # 3. 移除匹配的标签
        new_tags = [t for t in existing_tags if t not in tags_to_remove]
        
        if len(new_tags) == len(existing_tags):
            return True # 没有匹配到要删除的标签，直接返回

        # 4. 提交更新 (update_emby_item_details 已经处理了 TagItems 冲突)
        return update_emby_item_details(item_id, {"Tags": new_tags}, emby_server_url, emby_api_key, user_id)

    except Exception as e:
        logger.error(f"移除标签失败 (ID: {item_id}): {e}")
        return False

# --- 触发 神医 重新提取媒体信息 ---
def trigger_media_info_refresh(item_id: str, base_url: str, api_key: str, user_id: str) -> bool:
    """
    通过伪造 PlaybackInfo 请求，触发 Emby (及神医插件) 重新提取媒体信息。
    接口: POST /Items/{Id}/PlaybackInfo?AutoOpenLiveStream=true&IsPlayback=true
    """
    if not item_id: return False
    
    url = f"{base_url}/Items/{item_id}/PlaybackInfo"
    params = {
        "AutoOpenLiveStream": "true",
        "IsPlayback": "true",
        "api_key": api_key,
        "UserId": user_id
    }
    wait_for_server_idle(base_url, api_key)
    try:
        # 这是一个伪造的播放请求，不需要 body，或者传个空的
        response = emby_client.post(url, params=params, json={})
        
        if response.status_code == 200:
            logger.info(f"  💉 已对 ID:{item_id} 触发媒体信息提取请求。")
            return True
        else:
            logger.warning(f"  ⚠️ 触发失败 ID:{item_id}, HTTP {response.status_code}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"  🚫 请求异常 ID:{item_id}: {e}")
        return False
    
# --- Playback Reporting 插件集成 ---
def get_playback_reporting_data(base_url: str, api_key: str, user_id: str, days: int = 30) -> dict:
    """
    获取【个人】详细播放流水
    【V5 - 修复版】
    适配实际浏览器响应：snake_case 字段、字符串类型的秒数时长、日期时间合并。
    """
    # 1. 构造 URL
    if "/emby" not in base_url:
        api_url = f"{base_url.rstrip('/')}/emby/user_usage_stats/UserPlaylist"
    else:
        api_url = f"{base_url.rstrip('/')}/user_usage_stats/UserPlaylist"
    
    # 2. 构造参数
    params = {
        "api_key": api_key,
        "user_id": user_id,
        "days": days,
        "aggregate_data": "true",
        "include_stats": "true"
    }
    
    try:
        logger.debug(f"正在请求 UserPlaylist 接口: {api_url} | User: {user_id}")
        response = emby_client.get(api_url, params=params, timeout=20)
        
        if response.status_code == 404:
            return {"error": "plugin_not_installed"}
        response.raise_for_status()
        
        # 3. 解析数据
        raw_data = response.json()
        cleaned_data = []
        
        if raw_data and isinstance(raw_data, list):
            for item in raw_data:
                normalized_item = {}
                
                # --- 1. 标题 (修复：优先匹配 item_name) ---
                # 实际返回: "item_name": "欢乐颂..."
                normalized_item['Name'] = item.get('item_name') or item.get('Name') or item.get('ItemName') or "未知影片"
                
                # --- 2. 日期 (修复：合并 date 和 time) ---
                # 实际返回: "date": "2026-02-03", "time": "23:22:59"
                date_str = item.get('date') or item.get('Date')
                time_str = item.get('time') or ""
                
                if date_str and time_str:
                    # 如果都有，拼接成完整时间字符串，方便前端排序或显示
                    normalized_item['Date'] = f"{date_str} {time_str}"
                else:
                    normalized_item['Date'] = date_str or item.get('DateCreated')
                
                # --- 3. 时长 (修复：处理字符串类型的纯数字) ---
                # 实际返回: "duration": "2513" (字符串秒数)
                raw_duration = item.get('duration') or item.get('PlayDuration') or item.get('total_time') or 0
                final_duration_sec = 0
                
                try:
                    # 尝试直接转 float/int (处理 "2513" 或 2513)
                    val = float(raw_duration)
                    
                    # 策略判定：
                    # 如果数值巨大(>100000)，可能是 Ticks (1秒=1000万Ticks)，但这里不太像
                    # 根据你的日志 "2513" 对应 41分钟，说明这就是【秒】
                    # 如果数值很小 (<300)，也可能是【分钟】？
                    # 但根据 API 响应 "2513" ≈ 41分钟，可以直接认定为秒。
                    final_duration_sec = int(val)
                    
                except (ValueError, TypeError):
                    # 如果转换失败，尝试处理 "HH:MM:SS" 格式
                    if isinstance(raw_duration, str) and ":" in raw_duration:
                        try:
                            parts = raw_duration.split(':')
                            if len(parts) == 3:
                                h, m, s = map(int, parts)
                                final_duration_sec = h * 3600 + m * 60 + s
                            elif len(parts) == 2:
                                m, s = map(int, parts)
                                final_duration_sec = m * 60 + s
                        except:
                            final_duration_sec = 0
                            
                normalized_item['PlayDuration'] = final_duration_sec
                
                # --- 4. 类型 (修复：优先匹配 item_type) ---
                # 实际返回: "item_type": "Episode"
                normalized_item['ItemType'] = item.get('item_type') or item.get('Type') or 'Video'
                
                # --- 5. 补充字段 (可选，方便调试) ---
                normalized_item['ItemId'] = item.get('item_id')
                
                cleaned_data.append(normalized_item)
        
        if cleaned_data:
            import json
            # 只打印第一条，防止日志刷屏
            logger.debug(f"  🔍 [UserPlaylist] 数据获取成功，Count: {len(cleaned_data)} | Sample: {json.dumps(cleaned_data[0], ensure_ascii=False)}")
        else:
            logger.warning(f"  🔍 [UserPlaylist] 请求成功但返回空列表 (User: {user_id})")

        return {"data": cleaned_data}

    except Exception as e:
        logger.error(f"获取个人播放数据失败: {e}")
        return {"error": str(e)}

# ✨✨✨ 神医插件: 同步/提取媒体信息 ✨✨✨
def sync_item_media_info(item_id: str, media_data: Optional[Dict[str, Any]], base_url: str, api_key: str) -> Optional[Dict[str, Any]]:
    # 只需要校验 item_id, base_url, api_key
    if not all([item_id, base_url, api_key]):
        return None

    # 严格按照作者的写法，把参数直接拼在 URL 上，只传 Id
    api_url = f"{base_url.rstrip('/')}/emby/Items/SyncMediaInfo?Id={item_id}&api_key={api_key}"
        
    headers = {"Content-Type": "application/json"}
    custom_timeout = 600.0 # 真实提取耗时较长
    
    try:
        response = emby_client.post(api_url, json=media_data, headers=headers, timeout=custom_timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            # 备用接口同样只传 Id
            api_url_fallback = f"{base_url.rstrip('/')}/Items/SyncMediaInfo?Id={item_id}&api_key={api_key}"
            try:
                response_fallback = emby_client.post(api_url_fallback, json=media_data, headers=headers, timeout=custom_timeout)
                response_fallback.raise_for_status()
                return response_fallback.json()
            except Exception as ex:
                logger.error(f"  ➜ 神医备用接口调用失败: {ex}")
                return None
        else:
            logger.error(f"  ➜ 神医接口报错: HTTP {e.response.status_code} - {e.response.text}")
            return None
    except Exception as e:
        logger.error(f"  ➜ 调用神医接口时发生网络异常: {e}")
        return None

# ✨✨✨ 神医插件: 清除媒体信息 (强制重新提取) ✨✨✨
def clear_item_media_info(item_id: str, base_url: str, api_key: str) -> bool:
    """
    调用神医插件接口，彻底清除指定项目的媒体信息缓存 (清得毛都不剩)。
    接口: POST /Items/{Id}/ClearMediaInfo
    """
    if not all([item_id, base_url, api_key]):
        return False

    api_url = f"{base_url.rstrip('/')}/Items/{item_id}/ClearMediaInfo"
    params = {"api_key": api_key}

    try:
        # 这个接口是 POST 请求，不需要 body
        response = emby_client.post(api_url, params=params)
        response.raise_for_status()
        logger.info(f"  🧹 [神医] 成功清除项目 (ID:{item_id}) 的错误媒体信息缓存。")
        return True
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            logger.warning(f"  ⚠️ [神医] 清除媒体信息失败 (404): 插件版本可能过低，不支持此接口。")
        else:
            logger.error(f"  ❌ [神医] 清除媒体信息报错: HTTP {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"  ❌ [神医] 调用清除媒体信息接口时发生网络异常: {e}")
        return False