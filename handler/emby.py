# handler/emby.py

import requests
import concurrent.futures
import os
import gc
import base64
import shutil
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import config_manager
import constants
from typing import Optional, List, Dict, Any, Generator, Tuple, Set, Callable
import logging
logger = logging.getLogger(__name__)

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
        response = requests.post(auth_url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        access_token = data.get("AccessToken")
        user_id = data.get("User", {}).get("Id")
        
        if access_token and user_id:
            logger.info("  ➜ [自动登录] 成功，已获取并缓存了新的管理员 AccessToken。")
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
    logger.info("  ➜ [自动登录] 缓存未命中，正在执行首次登录以获取 AccessToken...")
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
        # ★★★ 核心修改 3/3: 在所有 requests 调用中动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
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
def get_emby_item_details(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str, fields: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not all([item_id, emby_server_url, emby_api_key, user_id]):
        logger.error("获取Emby项目详情参数不足：缺少ItemID、服务器URL、API Key或UserID。")
        return None

    url = f"{emby_server_url.rstrip('/')}/Users/{user_id}/Items/{item_id}"

    if fields:
        fields_to_request = fields
    else:
        fields_to_request = "Type,ProviderIds,People,Path,OriginalTitle,DateCreated,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines,MediaStreams"

    params = {
        "api_key": emby_api_key,
        "Fields": fields_to_request
    }
    
    params["PersonFields"] = "ImageTags,ProviderIds"
    
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(url, params=params, timeout=api_timeout)

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
            logger.warning(
                f"Emby API未找到项目ID: {item_id} (UserID: {user_id})。URL: {e.request.url}")
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
    
# --- 通过 Provider ID (如 Tmdb, Imdb) 在 Emby 媒体库中查找一个媒体项 ---
def find_emby_item_by_provider_id(provider_name: str, provider_id: str, base_url: str, api_key: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    通过 Provider ID (如 Tmdb, Imdb) 在 Emby 媒体库中查找一个媒体项。
    
    :param provider_name: Provider 的名称, e.g., 'Tmdb', 'Imdb'.
    :param provider_id: Provider 的 ID 值.
    :param base_url: Emby 服务器地址.
    :param api_key: Emby API Key.
    :param user_id: Emby 用户 ID.
    :return: 如果找到，返回媒体项的详情字典；否则返回 None.
    """
    if not all([provider_name, provider_id, base_url, api_key, user_id]):
        logger.error("find_emby_item_by_provider_id: 缺少必要的参数。")
        return None

    headers = {
        'X-Emby-Token': api_key,
        'Content-Type': 'application/json'
    }
    # 构造查询参数，格式为 ProviderName:ProviderId
    provider_ids_query = f"{provider_name}:{provider_id}"
    
    # API 端点 /Users/{UserId}/Items 可以让我们在特定用户的视图下查找
    url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    
    params = {
        'Recursive': 'true',
        'IncludeItemTypes': 'Movie,Series', # 只关心电影和剧集
        'ProviderIds': provider_ids_query,
        'Fields': 'Id,Name,ProviderIds' # 请求最少的字段以提高效率
    }

    try:
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(url, headers=headers, params=params, timeout=api_timeout)
        response.raise_for_status()
        
        data = response.json()
        items = data.get("Items", [])
        
        if items:
            # 通常只会有一个结果
            logger.debug(f"通过 {provider_name}:{provider_id} 在 Emby 中找到了项目: {items[0].get('Name')}")
            return items[0]
        else:
            logger.debug(f"通过 {provider_name}:{provider_id} 在 Emby 中未找到任何项目。")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"通过 Provider ID ({provider_ids_query}) 查询 Emby 时发生网络错误: {e}")
        return None
    except Exception as e:
        logger.error(f"解析 Emby Provider ID 查询结果时发生未知错误: {e}")
        return None

# ✨✨✨ 精确清除 Person 的某个 Provider ID ✨✨✨
def clear_emby_person_provider_id(person_id: str, provider_key_to_clear: str, emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([person_id, provider_key_to_clear, emby_server_url, emby_api_key, user_id]):
        logger.error("clear_emby_person_provider_id: 参数不足。")
        return False

    try:
        person_details = get_emby_item_details(person_id, emby_server_url, emby_api_key, user_id, fields="ProviderIds,Name")
        if not person_details:
            logger.warning(f"无法获取 Person {person_id} 的详情，跳过清除 Provider ID 操作。")
            return False

        person_name = person_details.get("Name", f"ID:{person_id}")
        current_provider_ids = person_details.get("ProviderIds", {})

        if provider_key_to_clear not in current_provider_ids:
            logger.trace(f"Person '{person_name}' ({person_id}) 已不包含 '{provider_key_to_clear}' ID，无需操作。")
            return True

        logger.debug(f"  ➜ 正在从 Person '{person_name}' ({person_id}) 的 ProviderIds 中移除 '{provider_key_to_clear}'...")
        
        updated_provider_ids = current_provider_ids.copy()
        del updated_provider_ids[provider_key_to_clear]
        
        update_payload = {"ProviderIds": updated_provider_ids}

        return update_person_details(person_id, update_payload, emby_server_url, emby_api_key, user_id)

    except Exception as e:
        logger.error(f"清除 Person {person_id} 的 Provider ID '{provider_key_to_clear}' 时发生未知错误: {e}", exc_info=True)
        return False
# ✨✨✨ 更新一个 Person 条目本身的信息 ✨✨✨
def update_person_details(person_id: str, new_data: Dict[str, Any], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([person_id, new_data, emby_server_url, emby_api_key, user_id]):
        logger.error("update_person_details: 参数不足 (需要 user_id)。")
        return False

    api_url = f"{emby_server_url.rstrip('/')}/Users/{user_id}/Items/{person_id}"
    params = {"api_key": emby_api_key}
    
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        logger.trace(f"准备获取 Person 详情 (ID: {person_id}, UserID: {user_id}) at {api_url}")
        response_get = requests.get(api_url, params=params, timeout=api_timeout)
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
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response_post = requests.post(update_url, json=person_to_update, headers=headers, params=params, timeout=api_timeout)
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
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        logger.trace(f"  ➜ 正在从 {target_url} 获取媒体库和合集...")
        response = requests.get(target_url, params=params, timeout=api_timeout)
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
    session = requests.Session()
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
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
            response = session.get(count_url, params=count_params, timeout=api_timeout)
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
                response = session.get(api_url, params=params, timeout=api_timeout)
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
                'IncludeItemTypes': "Movie,Series,Season,Episode",
            }
            if lib_id:
                params['ParentId'] = lib_id

            try:
                # 增加超时时间
                response = requests.get(url, params=params, headers=headers, timeout=45)
                
                # 简单的 500 错误重试逻辑
                if response.status_code == 500:
                    time.sleep(2)
                    params['Limit'] = 500
                    response = requests.get(url, params=params, headers=headers, timeout=60)

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

    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

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
            response = requests.get(api_url, params=params, timeout=api_timeout)
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
            
            response = requests.get(api_url, params=params, timeout=api_timeout)
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
# --- 媒体去重专用 ---
def get_library_items_for_cleanup(
    base_url: str,
    api_key: str,
    user_id: Optional[str],
    library_ids: List[str],
    media_type_filter: str,
    fields: str
) -> Optional[List[Dict[str, Any]]]:
    """
    【媒体清理专用】根据媒体库ID列表，高效获取所有项目。
    - 循环请求每个媒体库以确保稳定性。
    - 自动为每个项目注入来源库ID `_SourceLibraryId`。
    """
    if not base_url or not api_key:
        logger.error("get_emby_library_items_new: base_url 或 api_key 未提供。")
        return None

    if not library_ids:
        return []

    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    all_items = []
    
    # 循环遍历每个媒体库ID，而不是用逗号拼接，以提高稳定性
    for lib_id in library_ids:
        if not lib_id or not lib_id.strip():
            continue
        
        try:
            # 为本次请求构建参数
            params = {
                "api_key": api_key,
                "Recursive": "true",
                "ParentId": lib_id,
                "Fields": fields,
                "IncludeItemTypes": media_type_filter,
            }
            
            # 默认使用 /Items 端点，如果提供了 user_id 则作为参数传入
            api_url = f"{base_url.rstrip('/')}/Items"
            if user_id:
                params["UserId"] = user_id

            logger.trace(f"正在从媒体库 ID: {lib_id} 获取项目...")
            
            response = requests.get(api_url, params=params, timeout=api_timeout)
            response.raise_for_status()
            items_in_lib = response.json().get("Items", [])
            
            # 为每个项目注入来源库ID，以便上层逻辑使用
            for item in items_in_lib:
                item['_SourceLibraryId'] = lib_id
            all_items.extend(items_in_lib)
        
        except Exception as e:
            logger.error(f"请求库 ID: {lib_id} 中的项目失败: {e}", exc_info=True)
            continue # 一个库失败了，继续处理下一个

    logger.debug(f"  ➜ 总共从 {len(library_ids)} 个选定库中获取到 {len(all_items)} 个项目。")
    return all_items
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
    
    log_identifier = f"'{item_name_for_log}'" if item_name_for_log else f"ItemID: {item_emby_id}"
    
    # ★★★ 核心修改: 在函数开头一次性获取超时时间 ★★★
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

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
            update_response = requests.post(update_url, json=item_data, headers=headers, params=update_params, timeout=api_timeout)
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
        response = requests.post(refresh_url, params=params, timeout=api_timeout)
        if response.status_code == 204:
            logger.info(f"  ➜ 已成功为 {log_identifier} 刷新元数据。")
            return True
        else:
            logger.error(f"  - 刷新请求失败: HTTP状态码 {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"  - 刷新请求时发生网络错误: {e}")
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
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(count_url, params=count_params, timeout=api_timeout)
        response.raise_for_status()
        total_count = response.json().get("TotalRecordCount", 0)
        logger.info(f"Emby Person 总数: {total_count}")
    except Exception as e:
        logger.error(f"获取 Emby Person 总数失败: {e}")
    
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
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

    while True:
        if stop_event and stop_event.is_set():
            logger.info("  🚫 Emby Person 获取任务被中止。")
            return

        request_params = params.copy()
        request_params["StartIndex"] = start_index
        request_params["Limit"] = batch_size
        
        try:
            response = requests.get(api_url, headers=headers, params=request_params, timeout=api_timeout)
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
# ✨✨✨ 获取剧集下所有剧集的函数 ✨✨✨
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
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        children = data.get("Items", [])
        logger.debug(f"  ➜ 成功为剧集 {log_identifier} 获取到 {len(children)} 个子项目。")
        return children
    except requests.exceptions.RequestException as e:
        logger.error(f"获取剧集 {log_identifier} 的子项目列表时发生错误: {e}", exc_info=True)
        return None
# ✨✨✨ 获取剧集下所有季的函数 ✨✨✨
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
# ✨✨✨ 获取季下所有分集的函数 ✨✨✨
def get_season_children(
    season_id: str,
    base_url: str,
    api_key: str,
    user_id: str,
    fields: str = "Id,Name",
    limit: Optional[int] = None
) -> Optional[List[Dict[str, Any]]]:
    """
    【新增】获取一个季（Season）下的所有子项目，通常是分集（Episode）。
    """
    if not all([season_id, base_url, api_key, user_id]):
        logger.error(f"get_season_children for ID {season_id}: 参数不足。")
        return None

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "ParentId": season_id,
        "IncludeItemTypes": "Episode",
        "Recursive": "true",
        "Fields": fields,
    }
    if limit is not None:
        params["Limit"] = limit
    
    logger.debug(f"  ➜ 准备获取季 {season_id} 的子项目...")
    try:
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        children = data.get("Items", [])
        logger.debug(f"  ➜ 成功为季 {season_id} 获取到 {len(children)} 个子项目。")
        return children
    except requests.exceptions.RequestException as e:
        logger.error(f"获取季 {season_id} 的子项目列表时发生错误: {e}", exc_info=True)
        return None
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
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        with requests.get(image_url, params=params, stream=True, timeout=api_timeout) as r:
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
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        all_collections = response.json().get("Items", [])
        logger.debug(f"  ➜ 成功从 Emby 获取到 {len(all_collections)} 个合集。")
        return all_collections
    except Exception as e:
        logger.error(f"通用函数在获取所有Emby合集时发生错误: {e}", exc_info=True)
        return None
# ✨✨✨ 获取所有合集（过滤自建） ✨✨✨
def get_all_collections_with_items(base_url: str, api_key: str, user_id: str) -> Optional[List[Dict[str, Any]]]:
    if not all([base_url, api_key, user_id]):
        logger.error("get_all_collections_with_items: 缺少必要的参数。")
        return None

    logger.info("  ➜ 正在从 Emby 获取所有合集...")
    
    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        "api_key": api_key,
        "IncludeItemTypes": "BoxSet",
        "Recursive": "true",
        "Fields": "ProviderIds,Name,ImageTags"
    }
    
    # ★★★ 核心修改: 在函数开头一次性获取超时时间 ★★★
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

    try:
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        all_collections_from_emby = response.json().get("Items", [])
        
        regular_collections = []
        for coll in all_collections_from_emby:
            if coll.get("ProviderIds", {}).get("Tmdb"):
                regular_collections.append(coll)
            else:
                logger.debug(f"  ➜ 已跳过自建合集: '{coll.get('Name')}' (ID: {coll.get('Id')})。")

        logger.info(f"  ➜ 成功从 Emby 获取到 {len(regular_collections)} 个合集，准备获取其内容...")

        detailed_collections = []
        
        def _fetch_collection_children(collection):
            collection_id = collection.get("Id")
            if not collection_id: return None
            
            logger.debug(f"  ➜ 正在获取合集 '{collection.get('Name')}' (ID: {collection_id}) 的内容...")
            children_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
            children_params = {
                "api_key": api_key, "ParentId": collection_id,
                "IncludeItemTypes": "Movie",
                "Fields": "ProviderIds"
            }
            try:
                children_response = requests.get(children_url, params=children_params, timeout=api_timeout)
                children_response.raise_for_status()
                media_in_collection = children_response.json().get("Items", [])
                
                existing_media_tmdb_ids = [
                    media.get("ProviderIds", {}).get("Tmdb")
                    for media in media_in_collection if media.get("ProviderIds", {}).get("Tmdb")
                ]
                collection['ExistingMovieTmdbIds'] = existing_media_tmdb_ids
                return collection
            except requests.exceptions.RequestException as e:
                logger.error(f"  ➜ 获取合集 '{collection.get('Name')}' 内容时失败: {e}")
                collection['ExistingMovieTmdbIds'] = []
                return collection

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_collection = {}
            for coll in regular_collections:
                future = executor.submit(_fetch_collection_children, coll)
                future_to_collection[future] = coll
                time.sleep(0.1)

            for future in concurrent.futures.as_completed(future_to_collection):
                result = future.result()
                if result:
                    detailed_collections.append(result)

        logger.info(f"  ➜ 所有合集内容获取完成，共成功处理 {len(detailed_collections)} 个合集。")
        return detailed_collections

    except Exception as e:
        logger.error(f"处理 Emby 电影合集时发生未知错误: {e}", exc_info=True)
        return None
# --- 获取所有原生合集（新版）---
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
        lib_response = requests.get(libraries_url, params=lib_params, timeout=30)
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
                response = requests.get(collections_url, params=params, timeout=60)
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
# ✨✨✨ 获取 Emby 服务器信息 (如 Server ID) ✨✨✨
def get_emby_server_info(base_url: str, api_key: str) -> Optional[Dict[str, Any]]:
    if not base_url or not api_key:
        return None
    
    api_url = f"{base_url.rstrip('/')}/System/Info"
    params = {"api_key": api_key}
    
    logger.debug("正在获取 Emby 服务器信息...")
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        logger.error(f"获取 Emby 服务器信息失败: {e}")
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

def get_collection_members(collection_id: str, base_url: str, api_key: str, user_id: str) -> Optional[List[str]]:
    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {'api_key': api_key, 'ParentId': collection_id, 'Fields': 'Id'}
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        items = response.json().get("Items", [])
        return [item['Id'] for item in items]
    except Exception as e:
        logger.error(f"获取合集 {collection_id} 成员时失败: {e}")
        return None

def add_items_to_collection(collection_id: str, item_ids: List[str], base_url: str, api_key: str) -> bool:
    if not item_ids: return True
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    params = {'api_key': api_key, 'Ids': ",".join(item_ids)}
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.post(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

def remove_items_from_collection(collection_id: str, item_ids: List[str], base_url: str, api_key: str) -> bool:
    if not item_ids: return True
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    params = {'api_key': api_key, 'Ids': ",".join(item_ids)}
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.delete(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

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
        logger.error(f"❌ 发送清空合集 {collection_id} 的请求失败。")
        
    return success

def create_or_update_collection_with_emby_ids(
    collection_name: str, 
    emby_ids_in_library: List[str],
    base_url: str, 
    api_key: str, 
    user_id: str,
    prefetched_collection_map: Optional[dict] = None
) -> Optional[str]:
    logger.info(f"  ➜ 开始在Emby中处理名为 '{collection_name}' 的合集...")
    
    try:
        desired_emby_ids = emby_ids_in_library
        
        collection = prefetched_collection_map.get(collection_name.lower()) if prefetched_collection_map is not None else get_collection_by_name(collection_name, base_url, api_key, user_id)
        
        emby_collection_id = None

        if collection:
            emby_collection_id = collection['Id']
            logger.info(f"  ➜ 发现已存在的合集 '{collection_name}' (ID: {emby_collection_id})，开始同步...")
            
            current_emby_ids = get_collection_members(emby_collection_id, base_url, api_key, user_id)
            if current_emby_ids is None:
                raise Exception("无法获取当前合集成员，同步中止。")

            set_current = set(current_emby_ids)
            set_desired = set(desired_emby_ids)
            
            ids_to_remove = list(set_current - set_desired)
            ids_to_add = list(set_desired - set_current)

            if ids_to_remove:
                logger.info(f"  ➜ 发现 {len(ids_to_remove)} 个项目需要移除...")
                remove_items_from_collection(emby_collection_id, ids_to_remove, base_url, api_key)
            
            if ids_to_add:
                logger.info(f"  ➜ 发现 {len(ids_to_add)} 个新项目需要添加...")
                add_items_to_collection(emby_collection_id, ids_to_add, base_url, api_key)

            if not ids_to_remove and not ids_to_add:
                logger.info("  ➜ 合集内容已是最新，无需改动。")

            return emby_collection_id
        else:
            logger.info(f"  ➜ 未找到合集 '{collection_name}'，将开始创建...")
            if not desired_emby_ids:
                logger.warning(f"合集 '{collection_name}' 在媒体库中没有任何匹配项，跳过创建。")
                return None

            api_url = f"{base_url.rstrip('/')}/Collections"
            params = {'api_key': api_key}
            payload = {'Name': collection_name, 'Ids': ",".join(desired_emby_ids)}
            
            # ★★★ 核心修改: 动态获取超时时间 ★★★
            api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
            response = requests.post(api_url, params=params, data=payload, timeout=api_timeout)
            response.raise_for_status()
            new_collection_info = response.json()
            emby_collection_id = new_collection_info.get('Id')
            
            return emby_collection_id

    except Exception as e:
        logger.error(f"处理Emby合集 '{collection_name}' 时发生未知错误: {e}", exc_info=True)
        return None
    
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
            api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
            
            if len(id_chunks) > 1:
                logger.trace(f"  ➜ 正在请求批次 {i+1}/{len(id_chunks)} (包含 {len(batch_ids)} 个ID)...")
            response = requests.get(api_url, params=params, timeout=api_timeout)
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
    
def append_item_to_collection(collection_id: str, item_emby_id: str, base_url: str, api_key: str, user_id: str) -> bool:
    logger.trace(f"准备将项目 {item_emby_id} 追加到合集 {collection_id}...")
    
    api_url = f"{base_url.rstrip('/')}/Collections/{collection_id}/Items"
    
    params = {
        'api_key': api_key,
        'Ids': item_emby_id
    }
    
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.post(api_url, params=params, timeout=api_timeout)
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
    
def get_all_libraries_with_paths(base_url: str, api_key: str) -> List[Dict[str, Any]]:
    logger.debug("  ➜ 正在实时获取所有媒体库及其源文件夹路径...")
    try:
        folders_url = f"{base_url.rstrip('/')}/Library/VirtualFolders"
        params = {"api_key": api_key}
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(folders_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        virtual_folders_data = response.json()

        libraries_with_paths = []
        for folder in virtual_folders_data:
            if not folder.get("CollectionType"):
                continue

            lib_id = folder.get("ItemId")
            lib_name = folder.get("Name")
            locations = folder.get("Locations", [])

            if lib_id and lib_name and locations:
                libraries_with_paths.append({
                    "info": {
                        "Name": lib_name,
                        "Id": lib_id,
                        "CollectionType": folder.get("CollectionType")
                    },
                    "paths": locations
                })
        
        logger.debug(f"  ➜ 实时获取到 {len(libraries_with_paths)} 个媒体库的路径信息。")
        return libraries_with_paths

    except Exception as e:
        logger.error(f"实时获取媒体库路径时发生错误: {e}", exc_info=True)
        return []

def get_library_root_for_item(item_id: str, base_url: str, api_key: str, user_id: str) -> Optional[Dict[str, Any]]:
    logger.debug("  ➜ 正在为项目ID {item_id} 定位媒体库...")
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
            logger.info(f"  ➜ 匹配到媒体库 '{best_match_library.get('Name')}'。")
            return best_match_library
        else:
            logger.error(f"项目路径 '{item_path}' 未能匹配任何媒体库的源文件夹。")
            return None

    except Exception as e:
        logger.error(f"  ➜ 定位媒体库时发生未知严重错误: {e}", exc_info=True)
        return None
    
def update_emby_item_details(item_id: str, new_data: Dict[str, Any], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([item_id, new_data, emby_server_url, emby_api_key, user_id]):
        logger.error("update_emby_item_details: 参数不足。")
        return False

    try:
        current_item_details = get_emby_item_details(item_id, emby_server_url, emby_api_key, user_id)
        if not current_item_details:
            logger.error(f"  🚫 更新前无法获取项目 {item_id} 的详情，操作中止。")
            return False
        
        item_name_for_log = current_item_details.get("Name", f"ID:{item_id}")

        logger.debug(f"准备将以下新数据合并到 '{item_name_for_log}': {new_data}")
        item_to_update = current_item_details.copy()
        item_to_update.update(new_data)
        
        update_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}"
        params = {"api_key": emby_api_key}
        headers = {'Content-Type': 'application/json'}

        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response_post = requests.post(update_url, json=item_to_update, headers=headers, params=params, timeout=api_timeout)
        response_post.raise_for_status()
        
        logger.info(f"✅ 成功更新项目 '{item_name_for_log}' 的详情。")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"更新项目详情时发生网络错误 (ID: {item_id}): {e}")
        return False
    except Exception as e:
        logger.error(f"更新项目详情时发生未知错误 (ID: {item_id}): {e}", exc_info=True)
        return False
# --- 删除媒体项神医接口 ---    
def delete_item_sy(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    """
    删除媒体项神医接口
    """
    logger.warning(f"  ➜ 检测到删除请求，将尝试使用 [自动登录] 执行...")

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = get_admin_access_token()
    
    if not access_token:
        logger.error("  🚫 无法获取临时 AccessToken，删除操作中止。请检查管理员账号密码是否正确。")
        return False

    # 2. 使用临时令牌执行删除
    # 使用最被社区推荐的 POST /Items/{Id}/Delete 接口
    api_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}/DeleteVersion"
    
    headers = {
        'X-Emby-Token': access_token  # ★ 使用临时的 AccessToken
    }
    
    params = {
        'UserId': logged_in_user_id # ★ 使用登录后返回的 UserId
    }
    
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        response = requests.post(api_url, headers=headers, params=params, timeout=api_timeout)
        response.raise_for_status()
        logger.info(f"  ✅ 成功删除 Emby 媒体项 ID: {item_id}。")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ✅ 使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"  ✅ 使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生未知错误: {e}")
        return False
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
    
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        response = requests.post(api_url, headers=headers, params=params, timeout=api_timeout)
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
    
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        # 这个接口是 POST 请求
        response = requests.post(api_url, headers=headers, params=params, timeout=api_timeout)
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
# ======================================================================
# ★★★ 新增模块：用户数据中心相关函数 ★★★
# ======================================================================

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
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        users = response.json()
        logger.info(f"  ➜ 成功从 Emby 获取到 {len(users)} 个用户。")
        return users
    except Exception as e:
        logger.error(f"从 Emby 获取用户列表失败: {e}", exc_info=True)
        return None

def get_all_user_view_data(user_id: str, base_url: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    【V5 - 魔法日志版】
    - 增加 CRITICAL 级别的日志，用于打印从 Emby 获取到的最原始的 Item JSON 数据。
    """
    if not all([user_id, base_url, api_key]):
        return None

    all_items_with_data = []
    item_types = "Movie,Series,Episode"
    # ★★★ 1. 为了拿到所有可能的字段，我们请求更多信息 ★★★
    fields = "UserData,Type,SeriesId,ProviderIds,Name,LastPlayedDate" 
    
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
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 120)

    # ★★★ 2. 设置一个计数器，我们不需要打印所有日志，有几个样本就够了 ★★★
    log_counter = 0
    LOG_LIMIT = 5 # 只打印前 5 个有用户数据的条目

    logger.debug(f"开始为用户 {user_id} 分批获取所有媒体的用户数据")
    while True:
        try:
            request_params = params.copy()
            request_params["StartIndex"] = start_index
            request_params["Limit"] = batch_size
            
            response = requests.get(api_url, params=request_params, timeout=api_timeout)
            response.raise_for_status()
            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                break

            for item in items:
                user_data = item.get("UserData", {})
                # 我们只关心那些确实有播放记录或收藏的条目
                if user_data.get('Played') or user_data.get('IsFavorite') or user_data.get('PlaybackPositionTicks', 0) > 0:
                    
                    # ★★★ 3. 魔法日志：在这里把原始数据打印出来！★★★
                    # if log_counter < LOG_LIMIT:
                    #     # 使用 CRITICAL 级别让它在日志里最显眼，并用 json.dumps 保证完整输出
                    #     logger.critical(f"  ➜ [魔法日志] 捕获到原始 Emby Item 数据: {json.dumps(item, indent=2, ensure_ascii=False)}")
                    #     log_counter += 1

                    all_items_with_data.append(item)
            
            start_index += len(items)
            if len(items) < batch_size:
                break

        except Exception as e:
            logger.error(f"为用户 {user_id} 获取媒体数据时，处理批次 StartIndex={start_index} 失败: {e}", exc_info=True)
            break
            
    logger.debug(f"为用户 {user_id} 的全量同步完成，共找到 {len(all_items_with_data)} 个有状态的媒体项。")
    return all_items_with_data

def get_all_accessible_item_ids_for_user_optimized(base_url: str, api_key: str, user_id: str) -> Optional[Set[str]]:
    """
    【V5.8 优化版 - 基于已有逻辑】
    高效获取指定用户在Emby中拥有原生访问权限的所有媒体项的ID集合。
    此函数基于 get_all_user_view_data 的核心逻辑，但为权限检查进行了优化：
    - 只请求 'Id' 字段，最小化网络传输。
    - 不进行任何 UserData 过滤，返回所有可见项。
    - 使用 set 数据结构以便于进行高效的交集运算。
    """
    if not all([user_id, base_url, api_key]):
        logger.error("get_all_accessible_item_ids_for_user_optimized: 缺少必要参数。")
        return None

    accessible_ids = set()
    
    # 使用和 get_all_user_view_data 相同的强大API端点
    api_url = f"{base_url.rstrip('/')}/Items"
    
    params = {
        "api_key": api_key,
        "Recursive": "true",
        "IncludeItemTypes": "Movie,Series,Video", # 您可以根据需要调整
        "Fields": "Id",  # ★★★ 优化点：只请求ID，速度最快
        "UserId": user_id 
    }
    
    start_index = 0
    batch_size = 5000 # 可以适当调大批次大小，因为数据量很小
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 120)

    logger.debug(f"开始为用户 {user_id} 高效获取所有可访问媒体的ID...")
    while True:
        try:
            request_params = params.copy()
            request_params["StartIndex"] = start_index
            request_params["Limit"] = batch_size
            
            response = requests.get(api_url, params=request_params, timeout=api_timeout)
            response.raise_for_status()
            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                break

            # 将获取到的ID添加到集合中
            for item in items:
                if item_id := item.get("Id"):
                    accessible_ids.add(item_id)
            
            start_index += len(items)
            if len(items) < batch_size:
                break

        except Exception as e:
            logger.error(f"为用户 {user_id} 高效获取媒体ID时，处理批次 StartIndex={start_index} 失败: {e}", exc_info=True)
            # 如果在任何批次失败，返回None表示整个操作失败
            return None
            
    logger.trace(f"  ➜ 成功为用户 {user_id} 获取到 {len(accessible_ids)} 个原生可访问的媒体项ID。")
    return accessible_ids

def get_user_ids_with_access_to_item(item_id: str, base_url: str, api_key: str) -> List[str]:
    """
    获取对特定媒体项拥有原生访问权限的所有用户ID列表。
    通过并发查询每个用户的视图来实现，效率较高。
    """
    if not all([item_id, base_url, api_key]):
        logger.error("get_user_ids_with_access_to_item: 缺少必要参数。")
        return []

    all_users = get_all_emby_users_from_server(base_url, api_key)
    if not all_users:
        logger.error("无法获取用户列表，无法确定项目访问权限。")
        return []

    user_ids_with_access = []
    # 使用线程锁来确保并发写入列表时的线程安全
    lock = threading.Lock()

    def check_access_for_user(user: Dict[str, Any]):
        """在单个线程中为单个用户检查权限"""
        user_id = user.get("Id")
        if not user_id:
            return

        # 我们查询用户的 /Items 接口，如果能查到这个 item_id，就说明有权限
        api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
        params = {
            "api_key": api_key,
            "Ids": item_id,
            "Limit": 1,
            "Fields": "Id"  # 只请求最少的数据以提高效率
        }
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 30)

        try:
            response = requests.get(api_url, params=params, timeout=api_timeout)
            # 只要成功返回200，就说明在用户视图内
            if response.status_code == 200:
                data = response.json()
                # 再次确认 Items 列表不为空
                if data.get("Items"):
                    with lock:
                        user_ids_with_access.append(user_id)
                    logger.trace(f"  ➜ 权限检查：用户 '{user.get('Name')}' 可以访问项目 {item_id}。")
        except Exception as e:
            logger.warning(f"  ➜ 为用户 '{user.get('Name')}' 检查项目 {item_id} 访问权限时出错: {e}")

    logger.debug(f"  ➜ 开始为 {len(all_users)} 个用户并发检查新项目 {item_id} 的访问权限...")
    # 使用 concurrent.futures.ThreadPoolExecutor 来并发执行检查
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # 提交所有用户的检查任务
        futures = [executor.submit(check_access_for_user, user) for user in all_users]
        # 等待所有任务完成
        for future in concurrent.futures.as_completed(futures):
            pass  # 我们不需要处理返回值，因为函数内部直接操作列表
    
    logger.debug(f"  ➜ 权限检查完成，共有 {len(user_ids_with_access)} 个用户可以访问新项目。")
    return user_ids_with_access

# --- 用户管理模块 ---
def create_user_with_policy(
    username: str, 
    password: str, 
    # policy: Dict[str, Any],  <-- ★★★ 1. 删除 policy 参数 ★★★
    base_url: str, 
    api_key: str
) -> Optional[str]:
    """
    【V2 - 纯净创建版】
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
        response = requests.post(create_url, headers=headers, json=create_payload, timeout=15)
        
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
            
            pw_response = requests.post(password_url, headers=headers, json=password_payload, timeout=15)
            
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
def set_user_disabled_status(
    user_id: str, 
    disable: bool, 
    base_url: str, 
    api_key: str
) -> bool:
    """
    【V2 - 增加日志用户名】禁用或启用一个 Emby 用户。
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
        
        response = requests.post(policy_update_url, headers=headers, json=current_policy, timeout=15)
        
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
def get_user_details(user_id: str, base_url: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    【V3 - 智能兼容最终版】获取用户的完整详情，同时包含 Policy 和 Configuration。
    能够兼容不支持独立 /Configuration 接口的旧版 Emby。
    """
    details = {}
    headers = {"X-Emby-Token": api_key, "Accept": "application/json"}
    
    # 1. 总是先调用基础的用户信息接口
    user_info_url = f"{base_url}/Users/{user_id}"
    try:
        response = requests.get(user_info_url, headers=headers, timeout=10)
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
        response = requests.get(config_url, headers=headers, timeout=10)
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

def force_set_user_configuration(user_id: str, configuration_dict: Dict[str, Any], base_url: str, api_key: str) -> bool:
    """
    【V3 - 智能兼容最终版】为一个用户强制设置首选项。
    优先尝试新版专用接口，如果失败则回退到兼容旧版的完整更新模式。
    """
    # 策略1：优先尝试新版的、高效的专用接口
    url = f"{base_url}/Users/{user_id}/Configuration"
    headers = {"X-Emby-Token": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json=configuration_dict, timeout=15)
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
            update_response = requests.post(update_url, headers=headers, json=full_user_object, timeout=15)
            
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
        response = requests.post(policy_update_url, headers=headers, json=policy, timeout=15)
        
        if response.status_code == 204: # 204 No Content 表示成功
            logger.info(f"  ✅ 成功为用户 '{user_name_for_log}' 应用了新的权限策略。")
            return True
        else:
            logger.error(f"  ➜ 为用户 '{user_name_for_log}' 应用新策略失败。状态码: {response.status_code}, 响应: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"  ➜ 为用户 '{user_name_for_log}' 应用新策略时发生严重错误: {e}", exc_info=True)
        return False
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
    api_timeout = config.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        response = requests.delete(api_url, headers=headers, timeout=api_timeout)
        response.raise_for_status()
        logger.info(f"  ✅ 成功删除 Emby 用户 '{user_name_for_log}' (ID: {user_id})。")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ➜ 删除 Emby 用户 '{user_name_for_log}' 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"  ➜ 删除 Emby 用户 '{user_name_for_log}' 时发生未知错误: {e}")
        return False
# ★★★ 通用 Emby 用户认证函数 ★★★
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
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.post(auth_url, headers=headers, json=payload, timeout=api_timeout)
        
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
        requests.delete(url, headers=headers, timeout=10)
    except Exception:
        pass # 删除失败也不影响，可能是本来就没有头像

    # 4. 发送上传请求
    try:
        # 增加超时时间
        response = requests.post(url, headers=headers, data=b64_data, timeout=60)
        response.raise_for_status()
        return True
    except Exception as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f" | Response: {e.response.text}"
        logger.error(f"向 Emby 上传用户 {user_id} 头像失败: {error_msg}")
        return False

def get_user_info_from_server(base_url, api_key, user_id):
    """
    从 Emby 服务器获取单个用户的最新信息（主要为了获取新的 ImageTag）。
    """
    url = f"{base_url}/Users/{user_id}"
    headers = {'X-Emby-Token': api_key}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"从 Emby 获取用户 {user_id} 信息失败: {e}")
    return None