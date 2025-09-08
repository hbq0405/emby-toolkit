# emby_handler.py

import requests
import concurrent.futures
import os
import shutil
import time
import utils
import threading
# ★★★ 核心修改 1/3: 导入我们需要的配置管理器和常量 ★★★
import config_manager
import constants
from typing import Optional, List, Dict, Any, Generator, Tuple, Set
import logging
logger = logging.getLogger(__name__)
# (SimpleLogger 和 logger 的导入保持不变)

# ★★★ 核心修改 2/3: 删除之前硬编码的超时常量 (如果存在) ★★★
# EMBY_API_TIMEOUT = 60  <-- 删除这一行

class SimpleLogger:
    def info(self, msg): print(f"[EMBY_INFO] {msg}")
    def error(self, msg): print(f"[EMBY_ERROR] {msg}")
    def warning(self, msg): print(f"[EMBY_WARN] {msg}")
    def debug(self, msg): print(f"[EMBY_DEBUG] {msg}")
    def success(self, msg): print(f"[EMBY_SUCCESS] {msg}")
_emby_id_cache = {}
_emby_season_cache = {}
_emby_episode_cache = {}
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
        fields_to_request = "ProviderIds,People,Path,OriginalTitle,DateCreated,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines"

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

        logger.debug(f"  -> 正在从 Person '{person_name}' ({person_id}) 的 ProviderIds 中移除 '{provider_key_to_clear}'...")
        
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

    logger.trace(f"  -> 准备更新 Person (ID: {person_id}) 的信息，新数据: {new_data}")
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response_post = requests.post(update_url, json=person_to_update, headers=headers, params=params, timeout=api_timeout)
        response_post.raise_for_status()
        logger.trace(f"  -> 成功更新 Person (ID: {person_id}) 的信息。")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"  -> 更新 Person (ID: {person_id}) 时发生错误: {e}")
        return False
# ✨✨✨ 更新 Emby 媒体项目的演员列表 ✨✨✨
def update_emby_item_cast(item_id: str, new_cast_list_for_handler: List[Dict[str, Any]],
                          emby_server_url: str, emby_api_key: str, user_id: str,
                          new_rating: Optional[float] = None
                          ) -> bool:
    if not all([item_id, emby_server_url, emby_api_key, user_id]):
        logger.error(
            "update_emby_item_cast: 参数不足：缺少ItemID、服务器URL、API Key或UserID。")
        return False
    if new_cast_list_for_handler is None:
        new_cast_list_for_handler = []

    current_item_url = f"{emby_server_url.rstrip('/')}/Users/{user_id}/Items/{item_id}"
    params_get = {"api_key": emby_api_key}
    item_to_update: Optional[Dict[str, Any]] = None
    item_name_for_log = f"ID:{item_id}"
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response_get = requests.get(
            current_item_url, params=params_get, timeout=api_timeout)
        response_get.raise_for_status()
        item_to_update = response_get.json()
        item_name_for_log = item_to_update.get("Name", f"ID:{item_id}")
    except requests.exceptions.RequestException as e:
        logger.error(
            f"update_emby_item_cast: 获取Emby项目 {item_name_for_log} (UserID: {user_id}) 失败: {e}", exc_info=True)
        return False
    
    if not item_to_update:
        return False

    if new_rating is not None:
        try:
            rating_float = float(new_rating)
            if 0 <= rating_float <= 10:
                item_to_update["CommunityRating"] = rating_float
                logger.info(f"  -> 将 '{item_name_for_log}' 的评分更新为豆瓣评分: {rating_float}")
        except (ValueError, TypeError):
            pass

    formatted_people_for_emby: List[Dict[str, Any]] = []
    for actor_entry in new_cast_list_for_handler:
        actor_name = actor_entry.get("name")
        if not actor_name or not str(actor_name).strip():
            continue

        person_obj: Dict[str, Any] = {
            "Name": str(actor_name).strip(),
            "Role": str(actor_entry.get("character", "")).strip(),
            "Type": "Actor"
        }

        emby_person_id = actor_entry.get("emby_person_id")

        if emby_person_id and str(emby_person_id).strip():
            person_obj["Id"] = str(emby_person_id).strip()
            logger.trace(f"  -> 链接现有演员 '{person_obj['Name']}' (ID: {person_obj['Id']})")
        else:
            logger.trace(f"  -> 添加新演员 '{person_obj['Name']}'")
            provider_ids = actor_entry.get("provider_ids")
            if isinstance(provider_ids, dict) and provider_ids:
                sanitized_ids = {k: str(v) for k, v in provider_ids.items() if v is not None and str(v).strip()}
                if sanitized_ids:
                    person_obj["ProviderIds"] = sanitized_ids
                    logger.trace(f"    -> 为新演员 '{person_obj['Name']}' 设置初始 ProviderIds: {sanitized_ids}")

        formatted_people_for_emby.append(person_obj)

    item_to_update["People"] = formatted_people_for_emby

    if "LockedFields" in item_to_update and "Cast" in item_to_update.get("LockedFields", []):
        current_locked_fields = set(item_to_update["LockedFields"])
        current_locked_fields.remove("Cast")
        item_to_update["LockedFields"] = list(current_locked_fields)

    update_url = f"{emby_server_url.rstrip('/')}/Items/{item_id}"
    headers = {'Content-Type': 'application/json'}
    params_post = {"api_key": emby_api_key}

    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response_post = requests.post(
            update_url, json=item_to_update, headers=headers, params=params_post, timeout=api_timeout)
        response_post.raise_for_status()
        logger.trace(f"成功更新Emby项目 {item_name_for_log} 的演员信息。")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"更新Emby项目 {item_name_for_log} 演员信息时发生错误: {e}", exc_info=True)
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
        logger.trace(f"  -> 正在从 {target_url} 获取媒体库和合集...")
        response = requests.get(target_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        
        items = data.get('Items', [])
        logger.trace(f"  -> 成功获取到 {len(items)} 个媒体库/合集。")
        return items

    except requests.exceptions.RequestException as e:
        logger.error(f"连接Emby服务器获取媒体库/合集时失败: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"处理Emby媒体库/合集数据时发生未知错误: {e}", exc_info=True)
        return None
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
    force_user_endpoint: bool = False
) -> Optional[List[Dict[str, Any]]]:
    if not base_url or not api_key:
        logger.error("get_emby_library_items: base_url 或 api_key 未提供。")
        return None

    # ★★★ 核心修改: 在函数开头一次性获取超时时间 ★★★
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

    if search_term and search_term.strip():
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
            fields_to_request = fields if fields else "Id,Name,Type,ProductionYear,ProviderIds,Path,OriginalTitle,DateCreated,PremiereDate,ChildCount,RecursiveItemCount,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines,People,ProductionLocations"

            params = {
                "api_key": api_key, "Recursive": "true", "ParentId": lib_id,
                "Fields": fields_to_request,
            }
            if media_type_filter:
                params["IncludeItemTypes"] = media_type_filter
            else:
                params["IncludeItemTypes"] = "Movie,Series,Video"

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

    logger.debug(f"  -> 总共从 {len(library_ids)} 个选定库中获取到 {len(all_items_from_selected_libraries)} 个 {media_type_in_chinese} 项目。")
    
    return all_items_from_selected_libraries
# ✨✨✨ 刷新Emby元数据 ✨✨✨
def refresh_emby_item_metadata(item_emby_id: str,
                               emby_server_url: str,
                               emby_api_key: str,
                               user_id_for_ops: str,
                               lock_fields: Optional[List[str]] = None,
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
        logger.debug(f"  -> 正在为 {log_identifier} 获取当前详情...")
        item_data = get_emby_item_details(item_emby_id, emby_server_url, emby_api_key, user_id_for_ops)
        if not item_data:
            logger.error(f"  -> 无法获取 {log_identifier} 的详情，所有操作中止。")
            return False

        item_needs_update = False
        
        if replace_all_metadata_param:
            logger.debug(f"  -> 检测到 ReplaceAllMetadata=True，执行解锁...")
            if item_data.get("LockData") is True:
                item_data["LockData"] = False
                item_needs_update = True
            if item_data.get("LockedFields"):
                item_data["LockedFields"] = []
                item_needs_update = True
        
        if lock_fields:
            logger.debug(f"  -> 检测到需要锁定字段: {lock_fields}...")
            current_locked_fields = set(item_data.get("LockedFields", []))
            original_lock_count = len(current_locked_fields)
            
            for field in lock_fields:
                current_locked_fields.add(field)
            
            if len(current_locked_fields) > original_lock_count:
                item_data["LockedFields"] = list(current_locked_fields)
                item_needs_update = True

        if item_needs_update:
            logger.debug(f"  -> 正在为 {log_identifier} 提交锁状态更新...")
            update_url = f"{emby_server_url.rstrip('/')}/Items/{item_emby_id}"
            update_params = {"api_key": emby_api_key}
            headers = {'Content-Type': 'application/json'}
            update_response = requests.post(update_url, json=item_data, headers=headers, params=update_params, timeout=api_timeout)
            update_response.raise_for_status()
            logger.debug(f"  -> 成功更新 {log_identifier} 的锁状态。")
        else:
            logger.debug(f"  -> 项目 {log_identifier} 的锁状态无需更新。")

    except Exception as e:
        logger.warning(f"  -> 在刷新前更新锁状态时失败: {e}。刷新将继续，但可能受影响。")

    logger.debug(f"  -> 正在为 {log_identifier} 发送最终的刷新请求...")
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
            logger.info(f"  -> 刷新请求已成功发送给 {log_identifier}。")
            return True
        else:
            logger.error(f"  - 刷新请求失败: HTTP状态码 {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"  - 刷新请求时发生网络错误: {e}")
        return False
# ✨✨✨ 分批次地从 Emby 获取所有 Person 条目 ✨✨✨
def get_all_persons_from_emby(base_url: str, api_key: str, user_id: Optional[str], stop_event: Optional[threading.Event] = None) -> Generator[List[Dict[str, Any]], None, None]:
    if not user_id:
        logger.error("获取所有演员需要提供 User ID，但未提供。任务中止。")
        return

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    
    headers = {
        "X-Emby-Token": api_key,
        "Accept": "application/json",
    }
    
    params = {
        "Recursive": "true",
        "IncludeItemTypes": "Person",
        "Fields": "ProviderIds,Name",
    }

    start_index = 0
    batch_size = 5000

    logger.info(f"  -> 开始从 Emby 分批次获取所有演员数据 (每批: {batch_size})...")
    
    # ★★★ 核心修改: 在循环外一次性获取超时时间 ★★★
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)

    while True:
        if stop_event and stop_event.is_set():
            logger.info("Emby Person 获取任务被中止。")
            return

        request_params = params.copy()
        request_params["StartIndex"] = start_index
        request_params["Limit"] = batch_size
        
        logger.debug(f"  -> 获取 Person 批次: StartIndex={start_index}, Limit={batch_size}")
        
        try:
            response = requests.get(api_url, headers=headers, params=request_params, timeout=api_timeout)
            response.raise_for_status()
            data = response.json()
            items = data.get("Items", [])
            
            if not items:
                logger.trace("API 返回空列表，已获取所有 Person 数据。")
                break

            yield items
            
            start_index += len(items)
            
            time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            logger.error(f"请求 Emby API 失败 (批次 StartIndex={start_index}): {e}", exc_info=True)
            return
        except Exception as e:
            logger.error(f"处理 Emby 响应时发生未知错误 (批次 StartIndex={start_index}): {e}", exc_info=True)
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
    }
    
    logger.debug(f"  -> 准备获取剧集 {log_identifier} 的子项目 (类型: {include_item_types})...")
    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        children = data.get("Items", [])
        logger.debug(f"  -> 成功为剧集 {log_identifier} 获取到 {len(children)} 个子项目。")
        return children
    except requests.exceptions.RequestException as e:
        logger.error(f"获取剧集 {log_identifier} 的子项目列表时发生错误: {e}", exc_info=True)
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
        logger.info(f"  -> 媒体项 '{name_for_log}' 本身就是剧集，直接返回其ID。")
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
        logger.info(f"  -> 媒体项 '{name_for_log}' 所属剧集为：{series_name_for_log}。")
        return str(series_id)
    
    logger.warning(f"  -> 媒体项 '{name_for_log}' (类型: {item_type}) 的详情中未找到 'SeriesId' 字段，无法确定所属剧集。")
    return None
# ✨✨✨ 从 Emby 下载指定类型的图片并保存到本地 ✨✨✨
def download_emby_image(
    item_id: str,
    image_type: str,
    save_path: str,
    emby_server_url: str,
    emby_api_key: str,
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
            logger.debug(f"图片类型 '{image_type}' 在 Emby 项目 '{item_id}' 中不存在。")
        else:
            logger.error(f"下载图片时发生网络错误: {e}")
        return False
    except Exception as e:
        logger.error(f"保存图片到 '{save_path}' 时发生未知错误: {e}")
        return False
# --- 定时翻译演员 ---
def prepare_actor_translation_data(
    emby_url: str,
    emby_api_key: str,
    user_id: str,
    ai_translator,
    stop_event: threading.Event = None
) -> Tuple[Dict[str, str], Dict[str, List[Dict[str, Any]]]]:
    logger.info("  -> 正在从Emby获取所有演员列表...")
    all_persons = []
    try:
        person_generator = get_all_persons_from_emby(
            base_url=emby_url,
            api_key=emby_api_key,
            user_id=user_id,
            stop_event=stop_event
        )
        
        for person_batch in person_generator:
            if stop_event and stop_event.is_set():
                logger.info("  -> 在获取演员阶段任务被中止。")
                return {}, {}

            all_persons.extend(person_batch)

    except Exception as e:
        logger.error(f"【演员数据准备】从Emby获取演员列表时发生错误: {e}", exc_info=True)
        return {}, {}

    logger.info(f"  -> 已获取 {len(all_persons)} 位演员，正在筛选需要翻译的名字...")
    names_to_translate: Set[str] = set()
    name_to_persons_map: Dict[str, List[Dict[str, Any]]] = {}
    
    for person in all_persons:
        name = person.get("Name")
        person_id = person.get("Id")
        if name and person_id and not utils.contains_chinese(name):
            names_to_translate.add(name)
            if name not in name_to_persons_map:
                name_to_persons_map[name] = []
            name_to_persons_map[name].append(person)

    if not names_to_translate:
        logger.info("  -> 任务完成，没有发现需要翻译的演员名。")
        return {}, {}

    logger.info(f"  -> 筛选出 {len(names_to_translate)} 个外文名需要翻译。")

    logger.info(f"  -> 正在调用AI批量翻译 {len(names_to_translate)} 个名字...")
    translation_map: Dict[str, str] = {}
    try:
        translation_map = ai_translator.batch_translate(
            texts=list(names_to_translate),
            mode="fast"
        )
        if not translation_map:
            logger.warning("【演员数据准备】翻译引擎未能返回任何有效结果。")
            return {}, name_to_persons_map

    except Exception as e:
        logger.error(f"【演员数据准备】批量翻译时发生错误: {e}", exc_info=True)
        return {}, name_to_persons_map

    logger.info("  -> 所有演员名翻译完毕，正在写回Emby数据库...")
    
    return translation_map, name_to_persons_map
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
        logger.debug(f"  -> 成功从 Emby 获取到 {len(all_collections)} 个合集。")
        return all_collections
    except Exception as e:
        logger.error(f"通用函数在获取所有Emby合集时发生错误: {e}", exc_info=True)
        return None
# ✨✨✨ 获取所有合集（过滤自建） ✨✨✨
def get_all_collections_with_items(base_url: str, api_key: str, user_id: str) -> Optional[List[Dict[str, Any]]]:
    if not all([base_url, api_key, user_id]):
        logger.error("get_all_collections_with_items: 缺少必要的参数。")
        return None

    logger.info("  -> 正在从 Emby 获取所有合集...")
    
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
                logger.debug(f"  -> 已跳过自建合集: '{coll.get('Name')}' (ID: {coll.get('Id')})。")

        logger.info(f"  -> 成功从 Emby 获取到 {len(regular_collections)} 个合集，准备获取其内容...")

        detailed_collections = []
        
        def _fetch_collection_children(collection):
            collection_id = collection.get("Id")
            if not collection_id: return None
            
            logger.debug(f"  -> 正在获取合集 '{collection.get('Name')}' (ID: {collection_id}) 的内容...")
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
                logger.error(f"  -> 获取合集 '{collection.get('Name')}' 内容时失败: {e}")
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

        logger.info(f"  -> 所有合集内容获取完成，共成功处理 {len(detailed_collections)} 个合集。")
        return detailed_collections

    except Exception as e:
        logger.error(f"处理 Emby 电影合集时发生未知错误: {e}", exc_info=True)
        return None

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
            logger.debug(f"  -> 根据名称 '{name}' 找到了已存在的合集 (ID: {collection.get('Id')})。")
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
    logger.trace(f"  -> 开始清空 Emby 合集 {collection_id} 的所有成员...")
    
    member_ids = get_collection_members(collection_id, base_url, api_key, user_id)
    
    if member_ids is None:
        logger.error("  - 无法获取合集成员，清空操作中止。")
        return False
        
    if not member_ids:
        logger.info("  - 合集本身已为空，无需清空。")
        return True

    logger.trace(f"  - 正在从合集 {collection_id} 中移除 {len(member_ids)} 个成员...")
    success = remove_items_from_collection(collection_id, member_ids, base_url, api_key)
    
    if success:
        logger.info(f"  -> ✅ 成功从Emby删除合集 {collection_id} 。")
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
    logger.info(f"  -> 开始在Emby中处理名为 '{collection_name}' 的合集 (基于 Emby ID)...")
    
    try:
        desired_emby_ids = emby_ids_in_library
        
        collection = prefetched_collection_map.get(collection_name.lower()) if prefetched_collection_map is not None else get_collection_by_name(collection_name, base_url, api_key, user_id)
        
        emby_collection_id = None

        if collection:
            emby_collection_id = collection['Id']
            logger.info(f"  -> 发现已存在的合集 '{collection_name}' (ID: {emby_collection_id})，开始同步...")
            
            current_emby_ids = get_collection_members(emby_collection_id, base_url, api_key, user_id)
            if current_emby_ids is None:
                raise Exception("无法获取当前合集成员，同步中止。")

            set_current = set(current_emby_ids)
            set_desired = set(desired_emby_ids)
            
            ids_to_remove = list(set_current - set_desired)
            ids_to_add = list(set_desired - set_current)

            if ids_to_remove:
                logger.info(f"  -> 发现 {len(ids_to_remove)} 个项目需要移除...")
                remove_items_from_collection(emby_collection_id, ids_to_remove, base_url, api_key)
            
            if ids_to_add:
                logger.info(f"  -> 发现 {len(ids_to_add)} 个新项目需要添加...")
                add_items_to_collection(emby_collection_id, ids_to_add, base_url, api_key)

            if not ids_to_remove and not ids_to_add:
                logger.info("  -> 合集内容已是最新，无需改动。")

            return emby_collection_id
        else:
            logger.info(f"  -> 未找到合集 '{collection_name}'，将开始创建...")
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
    user_id: str,
    item_ids: List[str],
    fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    if not all([base_url, api_key, user_id]) or not item_ids:
        return []

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    
    params = {
        "api_key": api_key,
        "Ids": ",".join(item_ids),
        "Fields": fields or "ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName"
    }

    try:
        # ★★★ 核心修改: 动态获取超时时间 ★★★
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        return data.get("Items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"根据ID列表批量获取Emby项目时失败: {e}")
        return []
    
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
    logger.debug("正在实时获取所有媒体库及其源文件夹路径...")
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
        
        logger.debug(f"实时获取到 {len(libraries_with_paths)} 个媒体库的路径信息。")
        return libraries_with_paths

    except Exception as e:
        logger.error(f"实时获取媒体库路径时发生错误: {e}", exc_info=True)
        return []

def get_library_root_for_item(item_id: str, base_url: str, api_key: str, user_id: str) -> Optional[Dict[str, Any]]:
    logger.debug("正在为项目ID {item_id} 定位媒体库...")
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
            logger.info(f"  -> 匹配到媒体库 '{best_match_library.get('Name')}'。")
            return best_match_library
        else:
            logger.error(f"项目路径 '{item_path}' 未能匹配任何媒体库的源文件夹。")
            return None

    except Exception as e:
        logger.error(f"定位媒体库时发生未知严重错误: {e}", exc_info=True)
        return None
    
def update_emby_item_details(item_id: str, new_data: Dict[str, Any], emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    if not all([item_id, new_data, emby_server_url, emby_api_key, user_id]):
        logger.error("update_emby_item_details: 参数不足。")
        return False

    try:
        current_item_details = get_emby_item_details(item_id, emby_server_url, emby_api_key, user_id)
        if not current_item_details:
            logger.error(f"更新前无法获取项目 {item_id} 的详情，操作中止。")
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
def get_persons_by_ids_in_batches(
    base_url: str,
    api_key: str,
    user_id: str,
    person_ids: List[str],
    batch_size: int = 200,
    stop_event: Optional[threading.Event] = None  # ★★★ 核心修复：在这里添加 stop_event 参数 ★★★
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    【新增】根据一个庞大的演员ID列表，分批次地获取他们的详细信息。
    这是为了避免URL过长或一次请求过多数据。
    """
    total_ids = len(person_ids)
    logger.info(f"  -> 分批次获取 {total_ids} 位演员的详细数据 (每批: {batch_size})...")
    
    for i in range(0, total_ids, batch_size):
        # ★★★ 核心修复：在循环开始时检查停止信号 ★★★
        if stop_event and stop_event.is_set():
            logger.warning("分批获取演员详情任务被中止。")
            return

        batch_ids = person_ids[i:i + batch_size]
        logger.debug(f"    -> 正在获取批次 {i//batch_size + 1} (数量: {len(batch_ids)})...")
        
        # 复用已有的 get_emby_items_by_id 函数
        persons_batch = get_emby_items_by_id(
            base_url=base_url,
            api_key=api_key,
            user_id=user_id,
            item_ids=batch_ids,
            fields="ProviderIds,Name"  # 我们只需要这两个核心字段
        )
        
        if persons_batch:
            yield persons_batch
        
        time.sleep(0.2) # 友好地请求，避免给Emby太大压力