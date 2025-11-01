# moviepilot_handler.py

import requests
import re
import logging
from typing import Dict, Any, Optional, Tuple

import tmdb_handler
import constants
import utils 

logger = logging.getLogger(__name__)

def subscribe_movie_to_moviepilot(movie_info: dict, config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """【洗版增强版】一个独立的、可复用的函数，用于订阅单部电影到MoviePilot。"""
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        mp_username = config.get(constants.CONFIG_OPTION_MOVIEPILOT_USERNAME, '')
        mp_password = config.get(constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD, '')
        if not all([moviepilot_url, mp_username, mp_password]):
            logger.warning("MoviePilot订阅跳过：配置不完整。")
            return False

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        access_token = login_response.json().get("access_token")
        if not access_token:
            logger.error("MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        subscribe_url = f"{moviepilot_url}/api/v1/subscribe/"
        subscribe_headers = {"Authorization": f"Bearer {access_token}"}
        subscribe_payload = {
            "name": movie_info['title'],
            "tmdbid": int(movie_info['tmdb_id']),
            "type": "电影"
        }
        
        if best_version is not None:
            subscribe_payload["best_version"] = best_version
            logger.info(f"  ➜ 本次订阅为洗版订阅")
        
        logger.info(f"  ➜ 正在向 MoviePilot 提交订阅: '{movie_info['title']}'")
        sub_response = requests.post(subscribe_url, headers=subscribe_headers, json=subscribe_payload, timeout=15)
        
        if sub_response.status_code in [200, 201, 204]:
            logger.info(f"  ✅ MoviePilot 已接受订阅任务。")
            return True
        else:
            logger.error(f"  ➜ 失败！MoviePilot 返回错误: {sub_response.status_code} - {sub_response.text}")
            return False
    except Exception as e:
        logger.error(f"订阅电影到MoviePilot过程中发生网络或认证错误: {e}")
        return False

def subscribe_series_to_moviepilot(series_info: dict, season_number: Optional[int], config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """【V4 - 洗版订阅增强版】一个独立的、可复用的函数，用于订阅单季或整部剧集到MoviePilot。"""
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        mp_username = config.get(constants.CONFIG_OPTION_MOVIEPILOT_USERNAME, '')
        mp_password = config.get(constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD, '')
        if not all([moviepilot_url, mp_username, mp_password]):
            logger.warning("MoviePilot订阅跳过：配置不完整。")
            return False

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        access_token = login_response.json().get("access_token")
        if not access_token:
            logger.error("MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        series_title = series_info.get('title') or series_info.get('item_name')
        if not series_title:
            logger.error(f"MoviePilot订阅失败：传入的 series_info 字典中缺少 'title' 或 'item_name' 键。字典内容: {series_info}")
            return False

        subscribe_url = f"{moviepilot_url}/api/v1/subscribe/"
        subscribe_headers = {"Authorization": f"Bearer {access_token}"}
        subscribe_payload = {
            "name": series_title,
            "tmdbid": int(series_info['tmdb_id']),
            "type": "电视剧"
        }
        if season_number is not None:
            subscribe_payload["season"] = season_number
        
        if best_version is not None:
            subscribe_payload["best_version"] = best_version
            logger.info(f"  ➜ 本次订阅为洗版订阅")

        log_message = f"  ➜ 正在向 MoviePilot 提交订阅: '{series_title}'"
        if season_number is not None:
            log_message += f" 第 {season_number} 季"
        logger.info(log_message)
        
        sub_response = requests.post(subscribe_url, headers=subscribe_headers, json=subscribe_payload, timeout=15)
        
        if sub_response.status_code in [200, 201, 204]:
            logger.info(f"  ✅ MoviePilot 已接受订阅任务。")
            return True
        else:
            logger.error(f"  ➜ 失败！MoviePilot 返回错误: {sub_response.status_code} - {sub_response.text}")
            return False
            
    except KeyError as e:
        logger.error(f"订阅剧集到MoviePilot时发生KeyError: 键 {e} 不存在。传入的字典: {series_info}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"订阅剧集到MoviePilot过程中发生未知错误: {e}", exc_info=True)
        return False

# ★★★ 新增：通用的、基于Payload的订阅函数 ★★★
def subscribe_with_custom_payload(payload: dict, config: Dict[str, Any]) -> bool:
    """一个通用的订阅函数，直接接收一个完整的订阅 payload。"""
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        mp_username = config.get(constants.CONFIG_OPTION_MOVIEPILOT_USERNAME, '')
        mp_password = config.get(constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD, '')
        if not all([moviepilot_url, mp_username, mp_password]):
            logger.warning("MoviePilot订阅跳过：配置不完整。")
            return False

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        access_token = login_response.json().get("access_token")
        if not access_token:
            logger.error("MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        subscribe_url = f"{moviepilot_url}/api/v1/subscribe/"
        subscribe_headers = {"Authorization": f"Bearer {access_token}"}
        
        # 直接使用传入的 payload
        sub_response = requests.post(subscribe_url, headers=subscribe_headers, json=payload, timeout=15)
        
        if sub_response.status_code in [200, 201, 204]:
            logger.info(f"  ✅ MoviePilot 已接受订阅任务。")
            return True
        else:
            logger.error(f"  ➜ 失败！MoviePilot 返回错误: {sub_response.status_code} - {sub_response.text}")
            return False
    except Exception as e:
        logger.error(f"使用自定义Payload订阅到MoviePilot时发生错误: {e}", exc_info=True)
        return False
    
# ★★★ 一个新的、更强大的智能订阅函数 ★★★
def smart_subscribe_series(series_info: dict, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    【V2 - 返回详细工单】
    解析剧集信息，然后调用 MoviePilot 订阅。
    成功后，返回一个包含所有解析信息的字典，供上层写入数据库。
    失败则返回 None。
    """
    tmdb_id = series_info.get('tmdb_id')
    title = series_info.get('item_name')
    if not tmdb_id or not title:
        return None

    base_name, season_num = utils.parse_series_title_and_season(title) # 调用中央工具

    mp_payload = { "type": "电视剧" }
    parsed_info = {} # 用于返回的“工单”

    if base_name and season_num:
        # 分季剧集
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        search_results = tmdb_handler.search_tv_shows(base_name, tmdb_api_key)
        
        if search_results:
            parent_series = search_results[0]
            parent_tmdb_id = parent_series.get('id')
            parent_name = parent_series.get('name')
            
            mp_payload["name"] = parent_name
            mp_payload["tmdbid"] = parent_tmdb_id
            mp_payload["season"] = season_num
            
            parsed_info = {
                "parent_tmdb_id": str(parent_tmdb_id),
                "parsed_series_name": parent_name,
                "parsed_season_number": season_num
            }
        else:
            # 降级处理
            mp_payload["name"] = title
            mp_payload["tmdbid"] = tmdb_id
    else:
        # 整季剧集
        mp_payload["name"] = title
        mp_payload["tmdbid"] = tmdb_id

    # 调用 MP 订阅
    success = subscribe_with_custom_payload(mp_payload, config)

    # 如果订阅成功，返回“工单”；如果失败，返回 None
    return parsed_info if success else None