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
# ★★★ 模拟用户登录以获取临时 AccessToken 的辅助函数 ★★★
def _get_emby_access_token(emby_url, username, password) -> tuple[Optional[str], Optional[str]]:
    """通过用户名和密码登录，获取临时的 AccessToken 和 UserId。"""
    auth_url = f"{emby_url.rstrip('/')}/Users/AuthenticateByName"
    
    # Emby 登录需要特定的请求头来表明自己是哪个应用
    headers = {
        'Content-Type': 'application/json',
        'X-Emby-Authorization': 'Emby Client="Emby Toolkit", Device="Toolkit", DeviceId="d4f3e4b4-9f5b-4b8f-8b8a-5c5c5c5c5c5c", Version="1.0.0"'
    }
    
    payload = {
        "Username": username,
        "Pw": password
    }
    
    try:
        response = requests.post(auth_url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        access_token = data.get("AccessToken")
        user_id = data.get("User", {}).get("Id")
        if access_token and user_id:
            logger.debug("  -> [自动登录] 成功，已获取到临时的 AccessToken。")
            return access_token, user_id
        else:
            logger.error("  -> [自动登录] 登录 Emby 成功，但响应中未找到 AccessToken 或 UserId。")
            return None, None
    except Exception as e:
        logger.error(f"  -> [自动登录] 模拟登录 Emby 失败: {e}")
        return None, None
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
        fields_to_request = "ProviderIds,People,Path,OriginalTitle,DateCreated,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines,MediaStreams"

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
    
# ✨✨✨ 获取剧集详情，并聚合所有分集的演员 ✨✨✨
def get_emby_series_details_with_full_cast(
    series_id: str,
    emby_server_url: str,
    emby_api_key: str,
    user_id: str
) -> Optional[Dict[str, Any]]:
    """
    获取剧集的详细信息，并主动遍历其所有分集，
    将所有分集中的演员聚合到主剧集的 'People' 列表中，
    返回一个包含“完整演员表”的剧集详情对象。
    """
    logger.info(f"  -> [演员聚合模式] 开始为剧集 ID {series_id} 获取完整演员表...")

    # 1. 首先，获取剧集本身的基础详情和主演列表
    main_series_details = get_emby_item_details(series_id, emby_server_url, emby_api_key, user_id)
    if not main_series_details or main_series_details.get("Type") != "Series":
        logger.error(f"获取剧集 {series_id} 基础信息失败或该ID不是剧集类型。")
        return main_series_details # 返回原始数据或None

    # 2. 使用一个Map来存储所有唯一的演员，以Emby Person ID为键，避免重复
    aggregated_cast_map = {}
    
    # 2.1 首先将主剧集的演职员加入Map
    main_people = main_series_details.get("People", [])
    for person in main_people:
        person_id = person.get("Id")
        if person_id:
            aggregated_cast_map[person_id] = person
    
    logger.info(f"  -> 从主剧集加载了 {len(aggregated_cast_map)} 位主要演职员。")

    # 3. 获取该剧集下的所有分集
    all_episodes = get_series_children(
        series_id=series_id,
        base_url=emby_server_url,
        api_key=emby_api_key,
        user_id=user_id,
        series_name_for_log=main_series_details.get("Name"),
        include_item_types="Episode", # ★ 只关心分集
        fields="Id,Name" # ★ 只需要ID和名字用于日志
    )

    if not all_episodes:
        logger.info("  -> 未找到任何分集，直接使用主剧集演员表。")
        return main_series_details

    logger.info(f"  -> 发现 {len(all_episodes)} 个分集，开始遍历获取客串演员...")

    # 4. 遍历所有分集，获取它们的详情，并将演员补充到Map中
    # (注意：这里会产生较多API请求，但这是获取完整信息的唯一方式)
    for i, episode in enumerate(all_episodes):
        episode_id = episode.get("Id")
        if not episode_id:
            continue
        
        logger.debug(f"    ({i+1}/{len(all_episodes)}) 正在获取分集 '{episode.get('Name')}' (ID: {episode_id}) 的演员...")
        
        # 为每个分集获取完整的详情，特别是 'People' 字段
        episode_details = get_emby_item_details(episode_id, emby_server_url, emby_api_key, user_id)
        
        if episode_details and episode_details.get("People"):
            for person in episode_details["People"]:
                person_id = person.get("Id")
                # 如果这个演员不在我们的Map里，就加进去
                if person_id and person_id not in aggregated_cast_map:
                    aggregated_cast_map[person_id] = person
                    logger.debug(f"      -> 发现新演员: '{person.get('Name')}'")
    
    # 5. 将聚合好的完整演员列表替换掉原始剧集详情中的不完整列表
    full_cast_list = list(aggregated_cast_map.values())
    main_series_details["People"] = full_cast_list
    
    logger.info(f"  -> [演员聚合模式] 完成！共为剧集 '{main_series_details.get('Name')}' 聚合了 {len(full_cast_list)} 位独立演职员。")

    return main_series_details

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

        # 1. 准备一个基础的 person 对象
        person_obj: Dict[str, Any] = {
            "Name": str(actor_name).strip(),
            "Role": str(actor_entry.get("character", "")).strip(),
            "Type": "Actor"
        }

        emby_person_id = actor_entry.get("emby_person_id")

        # 2. 根据是“现有演员”还是“新演员”来决定如何构建
        if emby_person_id and str(emby_person_id).strip():
            # 对于【现有演员】，我们只需要提供 Id
            person_obj["Id"] = str(emby_person_id).strip()
            logger.trace(f"  -> 链接现有演员 '{person_obj['Name']}' (ID: {person_obj['Id']})")
        else:
            # 对于【新演员】，我们不提供 Id，而是提供 ProviderIds
            logger.trace(f"  -> 添加新演员 '{person_obj['Name']}'")
            provider_ids = actor_entry.get("provider_ids")
            if isinstance(provider_ids, dict) and provider_ids:
                # 清理掉值为 None 或空字符串的键
                sanitized_ids = {k: str(v) for k, v in provider_ids.items() if v is not None and str(v).strip()}
                if sanitized_ids:
                    person_obj["ProviderIds"] = sanitized_ids
                    logger.trace(f"    -> 为新演员 '{person_obj['Name']}' 设置初始 ProviderIds: {sanitized_ids}")

        formatted_people_for_emby.append(person_obj)

    # 1. 从原始的 item_to_update 中筛选出所有非演员
    other_people = [person for person in item_to_update.get("People", []) if person.get("Type") != "Actor"]
    
    # 2. 将处理好的演员列表与非演员列表合并
    item_to_update["People"] = other_people + formatted_people_for_emby
    
    logger.debug(f"  -> 最终写回Emby的演职员列表包含 {len(other_people)} 位非演员和 {len(formatted_people_for_emby)} 位演员。")

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
def get_all_persons_from_emby(
    base_url: str, 
    api_key: str, 
    user_id: Optional[str], 
    stop_event: Optional[threading.Event] = None,
    # ★★★ 核心修改：新增 batch_size 参数，并设置高效的默认值 ★★★
    batch_size: int = 5000
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    【V3 - 参数化批次版】
    分批次获取 Emby 中的 Person (演员) 项目。
    - 新增 batch_size 参数，允许调用方根据任务需求自定义批次大小。
    - 默认批次大小为 5000，以保证常规同步任务的效率。
    """
    if not user_id:
        logger.error("获取所有演员需要提供 User ID，但未提供。任务中止。")
        return

    library_ids = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS)

    if not library_ids:
        logger.info("  -> 未在配置中指定媒体库，将从整个 Emby 服务器分批获取所有演员数据...")
        api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
        headers = {"X-Emby-Token": api_key, "Accept": "application/json"}
        params = {
            "Recursive": "true",
            "IncludeItemTypes": "Person",
            "Fields": "ProviderIds,Name",
        }
        start_index = 0
        # ★★★ 核心修改：使用传入的 batch_size 参数 ★★★
        # batch_size = 5000  <-- 删除或注释掉这一行硬编码
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
        return

    # --- 模式二：已配置特定媒体库，执行精确扫描 (此部分逻辑不变) ---
    logger.info(f"  -> 检测到配置了 {len(library_ids)} 个媒体库，将只获取这些库中的演员数据...")
    media_items = get_emby_library_items(
        base_url=base_url, api_key=api_key, user_id=user_id,
        library_ids=library_ids, media_type_filter="Movie,Series", fields="People"
    )
    if media_items is None: return
    if not media_items:
        yield []
        return

    unique_person_ids = set()
    for item in media_items:
        if stop_event and stop_event.is_set(): return
        for person in item.get("People", []):
            if person_id := person.get("Id"):
                unique_person_ids.add(person_id)

    person_ids_to_fetch = list(unique_person_ids)
    if not person_ids_to_fetch:
        yield []
        return

    # 对于精确扫描，批次大小固定为500是合理的
    precise_batch_size = 500
    for i in range(0, len(person_ids_to_fetch), precise_batch_size):
        if stop_event and stop_event.is_set(): return
        batch_ids = person_ids_to_fetch[i:i + precise_batch_size]
        person_details_batch = get_emby_items_by_id(
            base_url=base_url, api_key=api_key, user_id=user_id,
            item_ids=batch_ids, fields="ProviderIds,Name"
        )
        if person_details_batch:
            yield person_details_batch
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
            logger.debug(f"图片类型 '{image_type}' 在 Emby 项目 '{item_id}' 中不存在。")
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
    """
    【V2 - 批量安全版】
    根据ID列表批量获取Emby项目，并自动分批处理超长ID列表以避免414错误。
    """
    if not all([base_url, api_key, user_id]) or not item_ids:
        return []

    all_items = []
    # 定义一个安全的分批大小，比如每次请求150个ID
    BATCH_SIZE = 150

    # 将长列表切分成多个小批次
    id_chunks = [item_ids[i:i + BATCH_SIZE] for i in range(0, len(item_ids), BATCH_SIZE)]
    
    logger.debug(f"ID列表总数({len(item_ids)})过长，已切分为 {len(id_chunks)} 个批次进行请求。")

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    
    # ★★★ 核心修改: 循环处理每个批次 ★★★
    for i, batch_ids in enumerate(id_chunks):
        params = {
            "api_key": api_key,
            "Ids": ",".join(batch_ids), # 只使用当前批次的ID
            "Fields": fields or "ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName"
        }

        try:
            api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
            
            logger.trace(f"  -> 正在请求批次 {i+1}/{len(id_chunks)} (包含 {len(batch_ids)} 个ID)...")
            response = requests.get(api_url, params=params, timeout=api_timeout)
            response.raise_for_status()
            
            data = response.json()
            batch_items = data.get("Items", [])
            all_items.extend(batch_items) # 将获取到的结果合并到总列表中
            
        except requests.exceptions.RequestException as e:
            # 记录当前批次的错误，但继续处理下一批
            logger.error(f"根据ID列表批量获取Emby项目时，处理批次 {i+1} 失败: {e}")
            continue

    logger.debug(f"所有批次请求完成，共获取到 {len(all_items)} 个媒体项。")
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
    
def delete_item(item_id: str, emby_server_url: str, emby_api_key: str, user_id: str) -> bool:
    """
    【V-Final Frontier 终极版】
    通过模拟管理员登录获取临时 AccessToken 来执行删除，绕过永久 API Key 的权限问题。
    """
    logger.warning(f"检测到删除请求，将尝试使用 [自动登录模式] 执行...")
    
    # 从全局配置中获取管理员登录凭证
    cfg = config_manager.APP_CONFIG
    admin_user = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_USER)
    admin_pass = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_PASS)

    if not all([admin_user, admin_pass]):
        logger.error("删除操作失败：未在设置中配置 [Emby 管理员用户名] 和 [Emby 管理员密码]。")
        return False

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = _get_emby_access_token(emby_server_url, admin_user, admin_pass)
    
    if not access_token:
        logger.error("无法获取临时 AccessToken，删除操作中止。请检查管理员账号密码是否正确。")
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
    
    api_timeout = cfg.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        response = requests.post(api_url, headers=headers, params=params, timeout=api_timeout)
        response.raise_for_status()
        logger.info(f"  -> ✅ 成功使用临时令牌删除 Emby 媒体项 ID: {item_id}。")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生HTTP错误: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"使用临时令牌删除 Emby 媒体项 ID: {item_id} 时发生未知错误: {e}")
        return False
    
# --- 清理幽灵演员 ---
def delete_person_custom_api(base_url: str, api_key: str, person_id: str) -> bool:
    """
    【V-Final Frontier 终极版 - 同样使用账密获取令牌】
    通过模拟管理员登录获取临时 AccessToken 来删除演员。
    这个接口只在神医Pro版插件中存在。
    """
    logger.warning(f"检测到删除演员请求，将尝试使用 [自动登录模式] 执行...")

    # 从全局配置中获取管理员登录凭证
    cfg = config_manager.APP_CONFIG
    admin_user = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_USER)
    admin_pass = cfg.get(constants.CONFIG_OPTION_EMBY_ADMIN_PASS)

    if not all([admin_user, admin_pass]):
        logger.error("删除演员操作失败：未在设置中配置 [Emby 管理员用户名] 和 [Emby 管理员密码]。")
        return False

    # 1. 登录获取临时令牌
    access_token, logged_in_user_id = _get_emby_access_token(base_url, admin_user, admin_pass)
    
    if not access_token:
        logger.error("无法获取临时 AccessToken，删除演员操作中止。请检查管理员账号密码是否正确。")
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
    
    api_timeout = cfg.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 60)
    
    try:
        # 这个接口是 POST 请求
        response = requests.post(api_url, headers=headers, params=params, timeout=api_timeout)
        response.raise_for_status()
        logger.info(f"  -> ✅ 成功使用临时令牌删除演员 ID: {person_id}。")
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
def check_user_has_visible_items_in_id_list(
    user_id: str,
    item_ids: List[str],
    base_url: str,
    api_key: str
) -> bool:
    """
    【V1 - 快速权限探测】
    以特定用户的身份，检查一个ID列表中是否至少有一个项目是该用户可见的。
    使用 Limit=1 进行超快速查询。
    """
    if not all([user_id, item_ids, base_url, api_key]):
        return False

    # 为了防止URL过长，我们只取前200个ID进行探测，这对于判断“是否为空”已经足够准确
    ids_to_check = item_ids[:200]

    api_url = f"{base_url.rstrip('/')}/Users/{user_id}/Items"
    params = {
        'api_key': api_key,
        'Ids': ",".join(ids_to_check),
        'Limit': 1, # ★★★ 核心：我们只需要知道有没有，不需要知道有多少
        'Fields': 'Id' # ★★★ 核心：我们只需要ID，其他信息都不要，追求最快速度
    }

    try:
        api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 15)
        response = requests.get(api_url, params=params, timeout=api_timeout)
        response.raise_for_status()
        data = response.json()
        
        # 如果返回的 Items 列表不为空，就证明用户至少能看到一个
        if data.get("Items"):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"快速权限探测失败 (用户: {user_id}): {e}")
        # 在不确定的情况下，为安全起见，我们默认用户看不到
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
        logger.info(f"  -> 成功从 Emby 获取到 {len(users)} 个用户。")
        return users
    except Exception as e:
        logger.error(f"从 Emby 获取用户列表失败: {e}", exc_info=True)
        return None

def get_all_user_view_data(user_id: str, base_url: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    【V5 - 最高权限版】
    - 切换到 /Items 端点，并使用 UserId 参数进行查询。
    - 这允许我们以管理员的身份，直接查询用户的核心数据，绕过因用户未登录而导致“视图”无法生成的权限问题。
    - 彻底解决了“沉睡用户”无法同步数据的终极BUG。
    """
    if not all([user_id, base_url, api_key]):
        return None

    all_items_with_data = []
    item_types = "Movie,Series,Episode"
    fields = "UserData,Type,SeriesId,LastPlayedDate"
    
    # ★★★ 核心改造：更换API端点，从“用户视图”切换到“系统级项目” ★★★
    api_url = f"{base_url.rstrip('/')}/Items"
    
    params = {
        "api_key": api_key,
        "Recursive": "true",
        "IncludeItemTypes": item_types,
        "Fields": fields,
        "UserId": user_id  # ★★★ 核心改造：将用户ID作为一个筛选参数传入 ★★★
    }
    
    start_index = 0
    batch_size = 2000
    api_timeout = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_TIMEOUT, 120)

    logger.debug(f"开始为用户 {user_id} 分批获取所有媒体的用户数据 (管理员模式)...")
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

            # 过滤出那些真正有用户数据的项目
            for item in items:
                user_data = item.get("UserData", {})
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