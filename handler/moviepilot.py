# handler/moviepilot.py

import requests
import json
import logging
from typing import Dict, Any, Optional

import handler.tmdb as tmdb
import constants

logger = logging.getLogger(__name__)

# ======================================================================
# 核心基础函数 (Token管理与API请求)
# ======================================================================

def _get_access_token(config: Dict[str, Any]) -> Optional[str]:
    """
    【内部辅助】获取 MoviePilot 的 Access Token。
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        mp_username = config.get(constants.CONFIG_OPTION_MOVIEPILOT_USERNAME, '')
        mp_password = config.get(constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD, '')
        
        if not all([moviepilot_url, mp_username, mp_password]):
            # 仅在第一次调用或配置缺失时记录警告，避免刷屏
            return None

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        
        # 设置超时
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        
        return login_response.json().get("access_token")
    except Exception as e:
        logger.error(f"  ➜ 获取 MoviePilot Token 失败: {e}")
        return None

def subscribe_with_custom_payload(payload: dict, config: Dict[str, Any]) -> bool:
    """
    【核心订阅函数】直接接收一个完整的订阅 payload 并提交。
    所有其他订阅函数最终都应调用此函数。
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token:
            logger.error("  ➜ MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        subscribe_url = f"{moviepilot_url}/api/v1/subscribe/"
        subscribe_headers = {"Authorization": f"Bearer {access_token}"}

        logger.trace(f"  ➜ 最终发送给 MoviePilot 的 Payload: {json.dumps(payload, ensure_ascii=False)}")
        
        sub_response = requests.post(subscribe_url, headers=subscribe_headers, json=payload, timeout=15)
        
        if sub_response.status_code in [200, 201, 204]:
            logger.info(f"  ✅ MoviePilot 已接受订阅任务。")
            return True
        else:
            # 尝试解析错误信息
            try:
                err_msg = sub_response.json().get('detail') or sub_response.text
            except:
                err_msg = sub_response.text
            logger.error(f"  ➜ 失败！MoviePilot 返回错误: {sub_response.status_code} - {err_msg}")
            return False
    except Exception as e:
        logger.error(f"  ➜ 使用自定义Payload订阅到MoviePilot时发生错误: {e}", exc_info=True)
        return False

def cancel_subscription(tmdb_id: str, item_type: str, config: Dict[str, Any], season: Optional[int] = None) -> bool:
    """
    【取消订阅】根据 TMDB ID 和类型取消订阅。
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token:
            logger.error("  ➜ MoviePilot 取消订阅失败：认证失败。")
            return False

        # 内部函数：执行单次取消请求
        def _do_cancel_request(target_season: Optional[int]) -> bool:
            media_id_for_api = f"tmdb:{tmdb_id}"
            cancel_url = f"{moviepilot_url}/api/v1/subscribe/media/{media_id_for_api}"
            
            params = {}
            if target_season is not None:
                params['season'] = target_season
            
            headers = {"Authorization": f"Bearer {access_token}"}
            
            season_log = f" Season {target_season}" if target_season is not None else ""
            logger.info(f"  ➜ 正在向 MoviePilot 发送取消订阅请求: {media_id_for_api}{season_log}")

            try:
                response = requests.delete(cancel_url, headers=headers, params=params, timeout=15)
                if response.status_code in [200, 204]:
                    logger.info(f"  ✅ MoviePilot 已成功取消订阅: {media_id_for_api}{season_log}")
                    return True
                elif response.status_code == 404:
                    logger.info(f"  ✅ MoviePilot 中未找到订阅 {media_id_for_api}{season_log}，无需取消。")
                    return True
                else:
                    logger.error(f"  ➜ MoviePilot 取消订阅失败！API 返回: {response.status_code} - {response.text}")
                    return False
            except Exception as req_e:
                logger.error(f"  ➜ 请求 MoviePilot API 发生异常: {req_e}")
                return False

        # --- 逻辑分支 ---

        # 情况 1: 电影，或者指定了具体季号的剧集 -> 直接取消
        if item_type == 'Movie' or season is not None:
            return _do_cancel_request(season)

        # 情况 2: 剧集 (Series) 且未指定季号 -> 查询 TMDb 遍历取消所有季
        if item_type == 'Series':
            tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            if not tmdb_api_key:
                logger.error("  ➜ 取消剧集订阅失败：未配置 TMDb API Key，无法获取分季信息。")
                return False

            logger.info(f"  ➜ 正在查询 TMDb 获取剧集 (ID: {tmdb_id}) 的所有季信息，以便逐个取消...")
            series_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
            
            if not series_details:
                logger.error(f"  ➜ 无法从 TMDb 获取剧集详情，取消订阅中止。")
                return False

            seasons = series_details.get('seasons', [])
            if not seasons:
                logger.warning(f"  ➜ 该剧集在 TMDb 上没有季信息，尝试直接取消整剧。")
                return _do_cancel_request(None)

            all_success = True
            # 遍历所有季
            for s in seasons:
                s_num = s.get('season_number')
                # 只处理 season_number > 0 的季，跳过第0季 ★★★
                if s_num is not None and s_num > 0:
                    if not _do_cancel_request(s_num):
                        all_success = False
            
            return all_success

        # 默认 fallback
        return _do_cancel_request(None)

    except Exception as e:
        logger.error(f"  ➜ 调用 MoviePilot 取消订阅 API 时发生未知错误: {e}", exc_info=True)
        return False

def check_subscription_exists(tmdb_id: str, item_type: str, config: Dict[str, Any], season: Optional[int] = None) -> bool:
    """
    【查询订阅】检查订阅是否存在。
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token:
            return False

        media_id_param = f"tmdb:{tmdb_id}"
        api_url = f"{moviepilot_url}/api/v1/subscribe/media/{media_id_param}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        params = {}
        if item_type in ['Series', 'Season'] and season is not None:
            params['season'] = season

        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            if data and data.get('id'):
                return True
        return False
    except Exception as e:
        logger.warning(f"  ➜ 检查 MoviePilot 订阅状态时发生错误: {e}")
        return False

# ======================================================================
# 业务封装函数 (保持原有逻辑，底层复用 subscribe_with_custom_payload)
# ======================================================================

def subscribe_movie_to_moviepilot(movie_info: dict, config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """订阅单部电影"""
    payload = {
        "name": movie_info['title'],
        "tmdbid": int(movie_info['tmdb_id']),
        "type": "电影"
    }
    if best_version is not None:
        payload["best_version"] = best_version
        logger.info(f"  ➜ 本次订阅为洗版订阅 (best_version={best_version})")
        
    logger.info(f"  ➜ 正在向 MoviePilot 提交电影订阅: '{movie_info['title']}'")
    return subscribe_with_custom_payload(payload, config)

def subscribe_series_to_moviepilot(series_info: dict, season_number: Optional[int], config: Dict[str, Any], best_version: Optional[int] = None) -> bool:
    """订阅单季或整部剧集"""
    title = series_info.get('title') or series_info.get('item_name')
    if not title:
        logger.error(f"  ➜ 订阅失败：缺少标题。信息: {series_info}")
        return False

    payload = {
        "name": title,
        "tmdbid": int(series_info['tmdb_id']),
        "type": "电视剧"
    }
    if season_number is not None:
        payload["season"] = season_number
    
    if best_version is not None:
        payload["best_version"] = best_version
        logger.info(f"  ➜ 本次订阅为洗版订阅 (best_version={best_version})")

    log_msg = f"  ➜ 正在向 MoviePilot 提交剧集订阅: '{title}'"
    if season_number is not None:
        log_msg += f" 第 {season_number} 季"
    logger.info(log_msg)
    
    return subscribe_with_custom_payload(payload, config)

def update_subscription_status(tmdb_id: int, season: Optional[int], status: str, config: Dict[str, Any], total_episodes: Optional[int] = None) -> bool:
    """
    调用 MoviePilot 接口更新订阅状态。
    兼容电影 (season=None) 和 剧集 (season=int)。
    status: 'R' (运行/订阅), 'S' (暂停/停止), 'P' (待定)
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token:
            return False
        
        headers = {"Authorization": f"Bearer {access_token}"}

        # 1. 查询订阅 ID (subid)
        media_id_param = f"tmdb:{tmdb_id}"
        get_url = f"{moviepilot_url}/api/v1/subscribe/media/{media_id_param}"
        get_params = {}
        
        # ★★★ 修改点：只有当 season 有值时才传参，电影不传 season ★★★
        if season is not None:
            get_params['season'] = season
        
        get_res = requests.get(get_url, headers=headers, params=get_params, timeout=10)
        
        sub_id = None
        if get_res.status_code == 200:
            data = get_res.json()
            if data and isinstance(data, dict):
                sub_id = data.get('id')
        
        if not sub_id:
            # 如果没找到订阅ID，说明可能还没订阅，或者已经被删除了
            return False

        # 2. 更新状态
        status_url = f"{moviepilot_url}/api/v1/subscribe/status/{sub_id}"
        status_params = {"state": status}
        requests.put(status_url, headers=headers, params=status_params, timeout=10)
        
        # 3. 如果提供了 total_episodes，更新订阅详情 ★★★
        if total_episodes is not None:
            # A. 获取完整的订阅详情
            detail_url = f"{moviepilot_url}/api/v1/subscribe/{sub_id}"
            detail_res = requests.get(detail_url, headers=headers, timeout=10)
            
            if detail_res.status_code == 200:
                sub_data = detail_res.json()
                
                old_total = sub_data.get('total_episode', 0)
                old_lack = sub_data.get('lack_episode', 0)
                
                # 只有当当前集数不等于目标集数时才更新
                if old_total != total_episodes:
                    # B. 修改总集数
                    sub_data['total_episode'] = total_episodes
                    
                    if old_total > total_episodes:
                        diff = old_total - total_episodes
                        # 确保不小于 0
                        new_lack = max(0, old_lack - diff)
                        sub_data['lack_episode'] = new_lack
                        
                        logger.info(f"  ➜ [MP修正] 自动修正缺失集数: {old_lack} -> {new_lack} (因总集数 {old_total}->{total_episodes})")

                    # C. 提交更新 (PUT /api/v1/subscribe/)
                    update_url = f"{moviepilot_url}/api/v1/subscribe/"
                    update_res = requests.put(update_url, headers=headers, json=sub_data, timeout=10)
                    
                    if update_res.status_code in [200, 204]:
                        logger.info(f"  ➜ [MP同步] 已将 MP 订阅 (ID:{sub_id}) 的总集数更新为 {total_episodes}")
                    else:
                        logger.warning(f"  ➜ 更新 MP 总集数失败: {update_res.status_code} - {update_res.text}")

        return True

    except Exception as e:
        logger.error(f"  ➜ 调用 MoviePilot 更新接口出错: {e}")
        return False
    
def delete_transfer_history(tmdb_id: str, season: int, title: str, config: Dict[str, Any]) -> list:
    """
    【清理整理记录】
    修改返回值：返回一个包含被删除记录 download_hash 的列表。
    如果失败或无记录，返回空列表 []。
    """
    collected_hashes = [] # 用于收集 Hash
    
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token:
            return []

        headers = {"Authorization": f"Bearer {access_token}"}
        search_url = f"{moviepilot_url}/api/v1/history/transfer"
        
        # 1. 循环获取所有相关记录
        all_records = []
        page = 1
        page_size = 500
        
        logger.info(f"  🔍 [MP清理] 正在全量搜索《{title}》的整理记录...")
        
        while True:
            params = {"title": title, "page": page, "count": page_size}
            try:
                res = requests.get(search_url, headers=headers, params=params, timeout=30)
                if res.status_code != 200: break
                data = res.json()
                if not data: break
                
                records_list = []
                if isinstance(data, dict):
                    inner_data = data.get('data')
                    if isinstance(inner_data, list): records_list = inner_data
                    elif isinstance(inner_data, dict) and 'list' in inner_data: records_list = inner_data['list']
                elif isinstance(data, list): records_list = data
                
                if not records_list: break
                all_records.extend(records_list)
                if len(records_list) < page_size: break
                page += 1
            except: break

        if not all_records:
            logger.info(f"  ✅ [MP清理] 未找到《{title}》的任何整理记录。")
            return []

        # 2. 内存筛选
        ids_to_delete = []
        target_tmdb = int(tmdb_id)
        target_season = int(season)
        
        for record in all_records:
            if not isinstance(record, dict): continue
            rec_tmdb = record.get('tmdbid')
            if rec_tmdb != target_tmdb: continue
            
            rec_seasons = str(record.get('seasons', '')).strip().upper()
            import re
            match = re.search(r'(\d+)', rec_seasons)
            if match:
                try:
                    if int(match.group(1)) == target_season:
                        ids_to_delete.append(record)
                except: continue

        if not ids_to_delete:
            logger.info(f"  ✅ [MP清理] 搜索到 {len(all_records)} 条记录，但没有 S{season} 的记录。")
            return []

        logger.info(f"  🗑️ [MP清理] 筛选出 {len(ids_to_delete)} 条 S{season} 的整理记录，开始执行删除...")

        # 3. 逐条删除并收集 Hash
        delete_url = f"{moviepilot_url}/api/v1/history/transfer"
        del_params = {"deletesrc": "false", "deletedest": "false"}
        
        deleted_count = 0
        for rec in ids_to_delete:
            try:
                # ★★★ 顺手牵羊：收集 Hash ★★★
                rec_hash = rec.get('download_hash')
                if rec_hash:
                    collected_hashes.append(rec_hash)

                del_res = requests.delete(delete_url, headers=headers, params=del_params, json=rec, timeout=15)
                if del_res.status_code == 200:
                    deleted_count += 1
            except: pass

        # 去重 Hash
        collected_hashes = list(set(collected_hashes))
        logger.info(f"  ✅ [MP清理] 清理完成，共删除 {deleted_count} 条记录，提取到 {len(collected_hashes)} 个关联种子Hash。")
        
        return collected_hashes

    except Exception as e:
        logger.error(f"  ❌ [MP清理] 执行出错: {e}")
        return []

def delete_download_tasks(keyword: str, config: Dict[str, Any], hashes: list = None) -> bool:
    """
    【清理下载任务】
    优先使用 hashes 列表进行精确删除。
    如果 hashes 为空，则回退到使用 keyword 搜索删除（兜底）。
    """
    try:
        moviepilot_url = config.get(constants.CONFIG_OPTION_MOVIEPILOT_URL, '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token: return False

        headers = {"Authorization": f"Bearer {access_token}"}
        deleted_count = 0

        # --- 策略 A: 精确打击 (使用 Hash) ---
        if hashes:
            logger.info(f"  🎯 [下载器清理] 正在根据 Hash 精确删除 {len(hashes)} 个任务...")
            for task_hash in hashes:
                if not task_hash: continue
                del_url = f"{moviepilot_url}/api/v1/download/{task_hash}"
                try:
                    del_res = requests.delete(del_url, headers=headers, timeout=10)
                    if del_res.status_code == 200:
                        logger.info(f"  🗑️ [下载器清理] 已精确删除任务 Hash: {task_hash[:8]}...")
                        deleted_count += 1
                except: pass
            
            if deleted_count > 0:
                logger.info(f"  ✅ [下载器清理] Hash 精确清理完成，共删除 {deleted_count} 个任务。")
                import time
                time.sleep(2)
                return True
            else:
                logger.warning(f"  ⚠️ [下载器清理] Hash 清理未成功 (可能任务早已不存在)，尝试关键词搜索兜底...")

        # --- 策略 B: 地毯式轰炸 (使用关键词搜索) ---
        # 只有当没传 Hash，或者 Hash 删除没效果时，才走这一步
        list_url = f"{moviepilot_url}/api/v1/download/"
        params = {"name": keyword}
        
        res = requests.get(list_url, headers=headers, params=params, timeout=15)
        if res.status_code != 200: return False
            
        tasks = res.json()
        if not tasks:
            logger.info(f"  ✅ [下载器清理] 未找到关键词 '{keyword}' 的活跃任务。")
            return True
            
        for task in tasks:
            task_hash = task.get('hash')
            task_title = task.get('title', '未知任务')
            if not task_hash: continue
            
            del_url = f"{moviepilot_url}/api/v1/download/{task_hash}"
            try:
                del_res = requests.delete(del_url, headers=headers, timeout=10)
                if del_res.status_code == 200:
                    logger.info(f"  🗑️ [下载器清理] 已删除旧任务: {task_title}")
                    deleted_count += 1
            except: pass

        if deleted_count > 0:
            logger.info(f"  ✅ [下载器清理] 关键词清理完成，共删除 {deleted_count} 个旧任务。")
            import time
            time.sleep(2)
            
        return True

    except Exception as e:
        logger.error(f"  ❌ [下载器清理] 执行出错: {e}")
        return False