# handler/moviepilot.py

import requests
import re
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

import handler.tmdb as tmdb
import constants
import utils 

logger = logging.getLogger(__name__)

def subscribe_movie_to_moviepilot(movie_info: dict, config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """一个独立的、可复用的函数，用于订阅单部电影到MoviePilot。"""
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
            logger.error("  ➜ MoviePilot订阅失败：认证失败，未能获取到 Token。")
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
        logger.error(f"  ➜ 订阅电影到MoviePilot过程中发生网络或认证错误: {e}")
        return False

def subscribe_series_to_moviepilot(series_info: dict, season_number: Optional[int], config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """一个独立的、可复用的函数，用于订阅单季或整部剧集到MoviePilot。"""
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
            logger.error("  ➜ MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        series_title = series_info.get('title') or series_info.get('item_name')
        if not series_title:
            logger.error(f"  ➜ MoviePilot订阅失败：传入的 series_info 字典中缺少 'title' 或 'item_name' 键。字典内容: {series_info}")
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
        logger.error(f"  ➜ 订阅剧集到MoviePilot时发生KeyError: 键 {e} 不存在。传入的字典: {series_info}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"  ➜ 订阅剧集到MoviePilot过程中发生未知错误: {e}", exc_info=True)
        return False

def subscribe_with_custom_payload(payload: dict, config: Dict[str, Any]) -> bool:
    """一个通用的订阅函数，直接接收一个完整的订阅 payload。"""
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        mp_username = config.get(constants.CONFIG_OPTION_MOVIEPILOT_USERNAME, '')
        mp_password = config.get(constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD, '')
        if not all([moviepilot_url, mp_username, mp_password]):
            logger.warning("  ➜ MoviePilot订阅跳过：配置不完整。")
            return False

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        access_token = login_response.json().get("access_token")
        if not access_token:
            logger.error("  ➜ MoviePilot订阅失败：认证失败，未能获取到 Token。")
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
        logger.error(f"  ➜ 使用自定义Payload订阅到MoviePilot时发生错误: {e}", exc_info=True)
        return False
    
def smart_subscribe_series(series_info: dict, config: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    【智能多季订阅 & 洗版增强】
    解析剧集信息，然后调用 MoviePilot 订阅。
    - 如果标题不含季号，且TMDb显示为多季剧集，则自动订阅所有季。
    - 订阅时会检查该季是否已完结（最后一集已播出），完结则自动添加 best_version=1。
    - 成功后，返回一个包含所有成功订阅工单信息的列表，供上层写入数据库。
    - 失败则返回 None。
    """
    tmdb_id = series_info.get('tmdb_id')
    title = series_info.get('item_name')
    tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    if not all([tmdb_id, title, tmdb_api_key]):
        logger.error("  ➜ 智能订阅失败：缺少 tmdb_id, item_name 或 tmdb_api_key。")
        return None

    base_name, season_num = utils.parse_series_title_and_season(title)
    successful_subscriptions = []

    def _is_season_fully_aired(tv_id: int, s_num: int, api_key: str) -> bool:
        """辅助函数：检查一季是否已完全播出。"""
        season_details = tmdb.get_season_details_tmdb(tv_id, s_num, api_key)
        if not season_details or not season_details.get('episodes'):
            logger.warning(f"  ➜ 无法获取 TMDB ID {tv_id} 第 {s_num} 季的剧集列表，无法判断是否完结。")
            return False
        
        last_episode = season_details['episodes'][-1]
        air_date_str = last_episode.get('air_date')
        if not air_date_str:
            logger.warning(f"  ➜ TMDB ID {tv_id} 第 {s_num} 季最后一集缺少播出日期。")
            return False

        try:
            air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
            is_aired = air_date < datetime.now().date()
            logger.info(f"  ➜ 检查 '{title}' S{s_num} 是否完结: 最后一集播出日期 {air_date_str}，是否已播出: {is_aired}")
            return is_aired
        except ValueError:
            logger.error(f"  ➜ 解析播出日期 '{air_date_str}' 时出错。")
            return False

    # --- 情况一：标题中未解析出季号 ---
    if season_num is None:
        logger.info(f"'{title}'  ➜ 未指定季号，正在查询TMDb以决定订阅策略...")
        series_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
        if not series_details:
            logger.error(f"  ➜ 无法从TMDb获取剧集 {title} (ID: {tmdb_id}) 的详情。")
            return None
        
        series_name = series_details.get('name', title)
        # 过滤掉不计入订阅的 "Specials" (第0季)
        seasons_to_subscribe = [s for s in series_details.get('seasons', []) if s.get('season_number', 0) > 0]

        # 如果是多季剧集，则遍历订阅所有季
        if len(seasons_to_subscribe) > 1:
            logger.info(f"'{series_name}'  ➜ 是多季剧集，将为所有 {len(seasons_to_subscribe)} 个季分别提交订阅。")
            for season in seasons_to_subscribe:
                current_season_num = season['season_number']
                best_version = 1 if _is_season_fully_aired(tmdb_id, current_season_num, tmdb_api_key) else None
                
                mp_payload = {
                    "name": series_name,
                    "tmdbid": tmdb_id,
                    "type": "电视剧",
                    "season": current_season_num
                }
                if best_version:
                    mp_payload["best_version"] = best_version

                if subscribe_with_custom_payload(mp_payload, config):
                    successful_subscriptions.append({
                        "parent_tmdb_id": str(tmdb_id),
                        "parsed_series_name": series_name,
                        "parsed_season_number": current_season_num
                    })
        # 如果是单季剧集（或信息不足），则按整部剧订阅
        else:
            logger.info(f"'{series_name}'  ➜ 将作为单季/整部剧集进行订阅。")
            best_version = None
            if seasons_to_subscribe:
                s_num_to_check = seasons_to_subscribe[0]['season_number']
                best_version = 1 if _is_season_fully_aired(tmdb_id, s_num_to_check, tmdb_api_key) else None

            mp_payload = {"name": series_name, "tmdbid": tmdb_id, "type": "电视剧"}
            if best_version:
                mp_payload["best_version"] = best_version
            
            if subscribe_with_custom_payload(mp_payload, config):
                 successful_subscriptions.append({
                    "parent_tmdb_id": str(tmdb_id),
                    "parsed_series_name": series_name,
                    "parsed_season_number": seasons_to_subscribe[0]['season_number'] if seasons_to_subscribe else 1
                })

    # --- 情况二：标题中已解析出季号 ---
    else:
        logger.info(f"'{title}'  ➜ 已解析出季号: {season_num}，执行单季订阅。")
        best_version = 1 if _is_season_fully_aired(tmdb_id, season_num, tmdb_api_key) else None
        
        # 尝试获取更规范的剧集名
        parent_name = base_name
        parent_tmdb_id = tmdb_id
        search_results = tmdb.search_tv_shows(base_name, tmdb_api_key)
        if search_results:
            parent_series = search_results[0]
            parent_tmdb_id = parent_series.get('id', tmdb_id)
            parent_name = parent_series.get('name', base_name)
            logger.info(f"  ➜ 通过TMDb规范化剧集名为: '{parent_name}' (ID: {parent_tmdb_id})")

        mp_payload = {
            "name": parent_name,
            "tmdbid": parent_tmdb_id,
            "type": "电视剧",
            "season": season_num
        }
        if best_version:
            mp_payload["best_version"] = best_version
        
        if subscribe_with_custom_payload(mp_payload, config):
            successful_subscriptions.append({
                "parent_tmdb_id": str(parent_tmdb_id),
                "parsed_series_name": parent_name,
                "parsed_season_number": season_num
            })

    return successful_subscriptions if successful_subscriptions else None