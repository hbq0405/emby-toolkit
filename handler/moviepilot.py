# handler/moviepilot.py

import requests
import json
import logging
from typing import Dict, Any, Optional

import handler.tmdb as tmdb
import constants
from database import settings_db

logger = logging.getLogger(__name__)

# ======================================================================
# 核心基础函数 (Token管理与API请求)
# ======================================================================

def _get_access_token(config: Dict[str, Any] = None) -> Optional[str]:
    """
    【内部辅助】获取 MoviePilot 的 Access Token。
    """
    try:
        # ★★★ 统一从 mp_config 读取
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        mp_username = mp_config.get('moviepilot_username', '')
        mp_password = mp_config.get('moviepilot_password', '')
        
        if not all([moviepilot_url, mp_username, mp_password]):
            return None

        login_url = f"{moviepilot_url}/api/v1/login/access-token"
        login_data = {"username": mp_username, "password": mp_password}
        
        login_response = requests.post(login_url, data=login_data, timeout=10)
        login_response.raise_for_status()
        
        return login_response.json().get("access_token")
    except Exception as e:
        logger.error(f"  ➜ 获取 MoviePilot Token 失败: {e}")
        return None

def subscribe_with_custom_payload(payload: dict, config: Dict[str, Any] = None) -> bool:
    """
    【核心订阅函数】直接接收一个完整的订阅 payload 并提交。
    所有其他订阅函数最终都应调用此函数。
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token:
            logger.error("  ➜ MoviePilot订阅失败：认证失败，未能获取到 Token。")
            return False

        subscribe_url = f"{moviepilot_url}/api/v1/subscribe/"
        subscribe_headers = {"Authorization": f"Bearer {access_token}"}

        logger.trace(f"  ➜ 最终发送给 MoviePilot 的 Payload: {json.dumps(payload, ensure_ascii=False)}")
        
        sub_response = requests.post(subscribe_url, headers=subscribe_headers, json=payload, timeout=60)
        
        if sub_response.status_code in [200, 201, 204]:
            logger.info(f"  ➜ MoviePilot 已接受订阅任务。")
            return True
        else:
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
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token:
            logger.error("  ➜ MoviePilot 取消订阅失败：认证失败。")
            return False

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
                response = requests.delete(cancel_url, headers=headers, params=params, timeout=30)
                if response.status_code in [200, 204]:
                    logger.info(f"  ➜ MoviePilot 已成功取消订阅: {media_id_for_api}{season_log}")
                    return True
                elif response.status_code == 404:
                    logger.info(f"  ➜ MoviePilot 中未找到订阅 {media_id_for_api}{season_log}，无需取消。")
                    return True
                else:
                    logger.error(f"  ➜ MoviePilot 取消订阅失败！API 返回: {response.status_code} - {response.text}")
                    return False
            except Exception as req_e:
                logger.error(f"  ➜ 请求 MoviePilot API 发生异常: {req_e}")
                return False

        if item_type == 'Movie' or season is not None:
            return _do_cancel_request(season)

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
            for s in seasons:
                s_num = s.get('season_number')
                if s_num is not None and s_num > 0:
                    if not _do_cancel_request(s_num):
                        all_success = False
            
            return all_success

        return _do_cancel_request(None)

    except Exception as e:
        logger.error(f"  ➜ 调用 MoviePilot 取消订阅 API 时发生未知错误: {e}", exc_info=True)
        return False

def check_subscription_exists(tmdb_id: str, item_type: str, config: Dict[str, Any] = None, season: Optional[int] = None) -> bool:
    """
    【查询订阅】检查订阅是否存在。
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
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
# 业务封装函数
# ======================================================================

def subscribe_movie_to_moviepilot(movie_info: dict, config: Dict[str, Any] = None, best_version: Optional[int] = None) -> bool:
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

def subscribe_series_to_moviepilot(series_info: dict, season_number: Optional[int], config: Dict[str, Any] = None, best_version: Optional[int] = None) -> bool:
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

def update_subscription_status(tmdb_id: int, season: Optional[int], status: str, config: Dict[str, Any] = None, total_episodes: Optional[int] = None) -> bool:
    """
    调用 MoviePilot 接口更新订阅状态。
    兼容电影 (season=None) 和 剧集 (season=int)。
    status: 'R' (运行/订阅), 'S' (暂停/停止), 'P' (待定)
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token:
            return False
        
        headers = {"Authorization": f"Bearer {access_token}"}

        media_id_param = f"tmdb:{tmdb_id}"
        get_url = f"{moviepilot_url}/api/v1/subscribe/media/{media_id_param}"
        get_params = {}
        
        if season is not None:
            get_params['season'] = season
        
        get_res = requests.get(get_url, headers=headers, params=get_params, timeout=10)
        
        sub_id = None
        if get_res.status_code == 200:
            data = get_res.json()
            if data and isinstance(data, dict):
                sub_id = data.get('id')
        
        if not sub_id:
            return False

        status_url = f"{moviepilot_url}/api/v1/subscribe/status/{sub_id}"
        status_params = {"state": status}
        requests.put(status_url, headers=headers, params=status_params, timeout=10)
        
        if total_episodes is not None:
            detail_url = f"{moviepilot_url}/api/v1/subscribe/{sub_id}"
            detail_res = requests.get(detail_url, headers=headers, timeout=10)
            
            if detail_res.status_code == 200:
                sub_data = detail_res.json()
                
                old_total = sub_data.get('total_episode', 0)
                old_lack = sub_data.get('lack_episode', 0)
                
                if old_total != total_episodes:
                    sub_data['total_episode'] = total_episodes
                    
                    if old_total > total_episodes:
                        diff = old_total - total_episodes
                        new_lack = max(0, old_lack - diff)
                        sub_data['lack_episode'] = new_lack
                        logger.info(f"  ➜ [MP修正] 自动修正缺失集数: {old_lack} -> {new_lack} (因总集数 {old_total}->{total_episodes})")

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
    
# ======================================================================
# ★★★ 智能清理引擎 (支持精准单集、辅种清理、记录与文件分离) ★★★
# ======================================================================

def _parse_episodes_string(ep_str: str) -> set:
    """解析 MP 整理记录中的 episodes 字段 (如 'E01', '01-05', '1,2,3') 为集合"""
    if not ep_str: return set()
    ep_str = str(ep_str).upper().replace('E', '').replace('P', '').strip()
    result = set()
    for part in ep_str.split(','):
        part = part.strip()
        if not part: continue
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                result.update(range(start, end + 1))
            except: pass
        else:
            try: result.add(int(part))
            except: pass
    return result

def _normalize_hash(value: Any) -> str:
    """统一下载任务 Hash，避免大小写导致辅种匹配失败。"""
    return str(value or "").strip().lower()

def _unique_keep_order(values: list) -> list:
    """去重但保持原始顺序，方便日志和 API 调用。"""
    result = []
    seen = set()
    for value in values or []:
        norm = _normalize_hash(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        result.append(str(value).strip())
    return result

def _extract_task_hash(task: dict) -> str:
    """兼容不同 MoviePilot / 下载器返回字段。"""
    if not isinstance(task, dict):
        return ""
    return str(
        task.get("hash")
        or task.get("id")
        or task.get("info_hash")
        or task.get("infoHash")
        or task.get("download_hash")
        or task.get("torrent_hash")
        or ""
    ).strip()

def _extract_task_name(task: dict) -> str:
    """取下载任务名称，用于按官方插件逻辑识别辅种。"""
    if not isinstance(task, dict):
        return ""
    return str(
        task.get("name")
        or task.get("title")
        or task.get("download_name")
        or task.get("torrent_name")
        or task.get("torrentName")
        or ""
    ).strip()

def _extract_task_size(task: dict) -> Optional[int]:
    """取下载任务体积；只有能解析成正整数时才用于辅种匹配，避免误伤。"""
    if not isinstance(task, dict):
        return None

    value = (
        task.get("size")
        or task.get("total_size")
        or task.get("totalSize")
        or task.get("total_size_bytes")
        or task.get("totalSizeBytes")
        or task.get("length")
    )

    try:
        size = int(float(value))
        return size if size > 0 else None
    except Exception:
        return None

def _expand_hashes_with_same_data(target_hashes: list, all_tasks: list, action_name: str = "删除") -> list:
    """
    参考官方自动删种插件的“处理辅种”逻辑：
    先找到目标 Hash 对应的下载任务，再把下载队列中“名称相同 + 体积相同”的其他 Hash 一并纳入。
    这样才能删掉同数据辅种，而不是只处理 MP 整理记录里的主 Hash。
    """
    target_hashes = _unique_keep_order(target_hashes)
    if not target_hashes:
        return []

    # 没拿到下载队列时，只能回退为原 hash 盲处理。
    if not all_tasks:
        return target_hashes

    target_norms = {_normalize_hash(h) for h in target_hashes if h}
    task_items = []

    for task in all_tasks:
        if not isinstance(task, dict):
            continue

        task_hash = _extract_task_hash(task)
        norm_hash = _normalize_hash(task_hash)
        if not norm_hash:
            continue

        task_items.append({
            "hash": task_hash,
            "norm_hash": norm_hash,
            "name": _extract_task_name(task),
            "size": _extract_task_size(task),
        })

    # 主种签名：只有命中待处理 Hash 的任务，才拿它的 name + size 做辅种扩展。
    target_signatures = {
        (item["name"], item["size"])
        for item in task_items
        if item["norm_hash"] in target_norms and item["name"] and item["size"]
    }

    expanded = []
    expanded_norms = set()

    def _add(hash_value: str):
        norm = _normalize_hash(hash_value)
        if norm and norm not in expanded_norms:
            expanded_norms.add(norm)
            expanded.append(hash_value)

    # 主 hash 永远保留，避免下载队列里暂时查不到时漏删。
    for h in target_hashes:
        _add(h)

    if not target_signatures:
        logger.debug("  ➜ [MP智能清理] 未能在下载队列中定位主种名称/大小，跳过辅种扩展。")
        return expanded

    for item in task_items:
        if item["norm_hash"] in expanded_norms:
            continue
        if item["name"] and item["size"] and (item["name"], item["size"]) in target_signatures:
            _add(item["hash"])
            logger.info(
                f"    ├─ 捕获同数据辅种，纳入{action_name}: "
                f"{item['name']} (Hash: {item['hash'][:8]}...)"
            )

    return expanded

def _extract_download_task_list(data: Any) -> list:
    """兼容 /api/v1/download/ 不同版本可能返回 list 或分页 dict。"""
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("data", "list", "items", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = _extract_download_task_list(value)
                if nested:
                    return nested

    return []

def analyze_mp_records_for_deletion(tmdb_id: str, item_type: str, season: Optional[int], episode: Optional[int], title: str, config: Dict[str, Any]) -> tuple:
    """
    智能分析 MP 整理记录，计算出哪些记录该删，哪些种子该删，哪些种子该暂停。
    返回: (records_to_delete, hashes_to_delete, hashes_to_pause)
    """
    records_to_delete = []
    hashes_to_delete = set()
    hashes_to_pause = set()
    
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token: return [], [], []

        headers = {"Authorization": f"Bearer {access_token}"}
        search_url = f"{moviepilot_url}/api/v1/history/transfer"
        
        all_records = []
        page = 1
        
        # 1. 拉取该标题下的所有整理记录
        while True:
            try:
                res = requests.get(search_url, headers=headers, params={"title": title, "page": page, "count": 500}, timeout=30)
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
                if len(records_list) < 500: break
                page += 1
            except: break

        if not all_records: return [], [], []

        target_tmdb = int(tmdb_id)
        hash_usage = {} # 记录每个 hash 被哪些记录使用

        # 2. 遍历记录，进行精准匹配
        for rec in all_records:
            if not isinstance(rec, dict): continue
            if rec.get('tmdbid') != target_tmdb: continue

            rec_hash = rec.get('download_hash')
            if rec_hash:
                if rec_hash not in hash_usage: hash_usage[rec_hash] = []
                hash_usage[rec_hash].append(rec)

            is_target = False
            
            if item_type == 'Movie':
                is_target = True
            else:
                # 剧集匹配
                rec_season_str = str(rec.get('seasons', '')).strip().upper()
                import re
                match = re.search(r'(\d+)', rec_season_str)
                if not match: continue
                rec_season = int(match.group(1))
                
                if season is not None and rec_season == int(season):
                    if episode is None:
                        # 删整季
                        is_target = True
                    else:
                        # 删单集
                        rec_eps = _parse_episodes_string(str(rec.get('episodes', '')))
                        if not rec_eps:
                            # 季包记录，不能删记录，但 hash 会被标记为 pause
                            is_target = False
                        elif int(episode) in rec_eps:
                            # 如果记录只包含这一集，可以删记录
                            if len(rec_eps) == 1: is_target = True
                            else: is_target = False # 包含多集，不能删记录

            if is_target:
                records_to_delete.append(rec)

        # 3. 依赖分析：决定 Hash 的生死
        for h, recs in hash_usage.items():
            can_delete = True
            used_by_target = False
            
            for r in recs:
                # 如果这个 hash 关联的某条记录不在“待删除列表”中，说明还有其他集在用它！
                if r not in records_to_delete:
                    can_delete = False
                
                # 检查这个 hash 是否真的被我们要删的目标用到了
                if item_type == 'Movie' or episode is None:
                    used_by_target = True
                else:
                    eps = _parse_episodes_string(str(r.get('episodes', '')))
                    if not eps or int(episode) in eps:
                        used_by_target = True

            if used_by_target:
                if can_delete:
                    hashes_to_delete.add(h)
                else:
                    hashes_to_pause.add(h)

        return records_to_delete, list(hashes_to_delete), list(hashes_to_pause)
    except Exception as e:
        logger.error(f"  ➜ [MP智能分析] 失败: {e}")
        return [], [], []

def smart_cleanup_mp_media(tmdb_id: str, item_type: str, season: Optional[int], episode: Optional[int], title: str, config: Dict[str, Any], delete_history: bool = True, delete_files: bool = True) -> bool:
    """
    【全新入口】智能清理 MP 媒体 (支持独立控制记录和文件，支持辅种清理)
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token: return False

        headers = {"Authorization": f"Bearer {access_token}"}
        
        log_target = f"《{title}》"
        if item_type != 'Movie':
            log_target += f" S{season}" + (f"E{episode}" if episode else " (整季)")
            
        logger.info(f"  ➜ [MP智能清理] 开始分析 {log_target} 的依赖关系...")
        
        records_to_delete, hashes_to_delete, hashes_to_pause = analyze_mp_records_for_deletion(tmdb_id, item_type, season, episode, title, config)
        
        # 1. 删除整理记录
        if delete_history and records_to_delete:
            logger.info(f"  ➜ [MP智能清理] 准备删除 {len(records_to_delete)} 条整理记录...")
            del_url = f"{moviepilot_url}/api/v1/history/transfer"
            del_params = {"deletesrc": "false", "deletedest": "false"}
            for rec in records_to_delete:
                try: requests.delete(del_url, headers=headers, params=del_params, json=rec, timeout=10)
                except: pass
            logger.info(f"  ➜ [MP智能清理] 整理记录删除完成。")

        # 2. 处理种子及源文件 (包含辅种一网打尽)
        if delete_files and (hashes_to_delete or hashes_to_pause):
            logger.info(f"  ➜ [MP智能清理] 准备处理种子: {len(hashes_to_delete)} 个待删除, {len(hashes_to_pause)} 个待暂停 (因包含其他存活集)...")
            
            # 获取所有下载任务，用“名称 + 大小”扩展同数据辅种。
            # 注意：辅种 Hash 与主种不同，只按 hash 永远抓不到。
            all_tasks = get_downloading_tasks(config)

            tasks_to_delete = _expand_hashes_with_same_data(hashes_to_delete, all_tasks, action_name="删除")
            tasks_to_pause = _expand_hashes_with_same_data(hashes_to_pause, all_tasks, action_name="暂停")

            # 删除优先于暂停，避免同一个 hash 被重复处理。
            delete_norms = {_normalize_hash(h) for h in tasks_to_delete}
            tasks_to_pause = [h for h in tasks_to_pause if _normalize_hash(h) not in delete_norms]

            # 执行删除
            for h in tasks_to_delete:
                try:
                    requests.delete(f"{moviepilot_url}/api/v1/download/{h}", headers=headers, timeout=10)
                    logger.info(f"    ├─ 已彻底删除种子及源文件 (Hash: {h[:8]}...)")
                except: pass
            
            # 执行暂停
            for h in tasks_to_pause:
                try:
                    requests.get(f"{moviepilot_url}/api/v1/download/stop/{h}", headers=headers, timeout=10)
                    logger.info(f"    ├─ 已暂停种子，保留源文件 (Hash: {h[:8]}...)")
                except: pass
                
            logger.info(f"  ➜ [MP智能清理] 种子及源文件处理完成。")

        return True
    except Exception as e:
        logger.error(f"  ➜ [MP智能清理] 发生异常: {e}", exc_info=True)
        return False

# ======================================================================
# ★★★ 兼容旧版接口 (防止其他模块报错) ★★★
# ======================================================================
def delete_transfer_history(tmdb_id: str, season: int, title: str, config: Dict[str, Any] = None) -> list:
    """兼容旧版：只删记录，返回 hashes 供旧版 delete_download_tasks 使用"""
    records_to_delete, hashes_to_delete, hashes_to_pause = analyze_mp_records_for_deletion(tmdb_id, 'Series', season, None, title, config)
    
    if records_to_delete:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        access_token = _get_access_token(config)
        if access_token:
            headers = {"Authorization": f"Bearer {access_token}"}
            del_url = f"{moviepilot_url}/api/v1/history/transfer"
            for rec in records_to_delete:
                try: requests.delete(del_url, headers=headers, params={"deletesrc": "false", "deletedest": "false"}, json=rec, timeout=10)
                except: pass
                
    return list(set(hashes_to_delete + hashes_to_pause))

def delete_download_tasks(keyword: str, config: Dict[str, Any] = None, hashes: list = None) -> bool:
    """兼容旧版：接收 hashes 盲删"""
    if not hashes: return False
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        access_token = _get_access_token(config)
        if not access_token: return False

        headers = {"Authorization": f"Bearer {access_token}"}
        all_tasks = get_downloading_tasks(config)
        hashes = _expand_hashes_with_same_data(hashes, all_tasks, action_name="删除")
        for h in hashes:
            try: requests.delete(f"{moviepilot_url}/api/v1/download/{h}", headers=headers, timeout=10)
            except: pass
        return True
    except: return False
    
def get_downloading_tasks(config: Dict[str, Any] = None) -> list:
    """获取当前正在下载的任务列表"""
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token: return []

        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.get(f"{moviepilot_url}/api/v1/download/", headers=headers, timeout=15)
        if res.status_code == 200:
            return _extract_download_task_list(res.json())
        return []
    except Exception as e:
        logger.error(f"  ➜ 获取 MP 下载队列失败: {e}")
        return []

def get_subscription_by_tmdbid(tmdb_id: int, season: Optional[int], config: Dict[str, Any] = None) -> dict:
    """根据 TMDb ID 获取单条订阅详情 (通过遍历所有订阅实现，更可靠)"""
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token: return {}

        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.get(f"{moviepilot_url}/api/v1/subscribe/", headers=headers, timeout=15)
        
        if res.status_code == 200:
            subs = res.json()
            for sub in subs:
                if str(sub.get('tmdbid')) == str(tmdb_id):
                    if season is not None:
                        if str(sub.get('season')) == str(season):
                            return sub
                    else:
                        return sub
        return {}
    except Exception as e:
        logger.error(f"  ➜ 获取 MP 订阅详情失败: {e}")
        return {}

def update_subscription(payload: dict, config: Dict[str, Any] = None) -> bool:
    """
    【更新订阅】根据提供的 payload 更新订阅信息。
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token: return False

        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.put(f"{moviepilot_url}/api/v1/subscribe/", headers=headers, json=payload, timeout=15)
        return res.status_code in [200, 204]
    except Exception as e:
        logger.error(f"  ➜ 更新 MP 订阅失败: {e}")
        return False

def search_subscription(sub_id: int, config: Dict[str, Any] = None) -> bool:
    """触发指定订阅的立即搜索"""
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token: return False

        headers = {"Authorization": f"Bearer {access_token}"}
        res = requests.get(f"{moviepilot_url}/api/v1/subscribe/search/{sub_id}", headers=headers, timeout=30)
        return res.status_code == 200
    except Exception as e:
        logger.error(f"  ➜ 触发 MP 订阅搜索失败: {e}")
        return False
    
def recognize_media(title: str, config: Dict[str, Any] = None) -> Optional[tuple]:
    """
    【辅助识别】调用 MoviePilot 的识别接口解析文件名。
    返回: (tmdb_id, media_type, title) 或 None
    """
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        moviepilot_url = mp_config.get('moviepilot_url', '').rstrip('/')
        
        access_token = _get_access_token(config)
        if not access_token or not moviepilot_url:
            return None

        url = f"{moviepilot_url}/api/v1/media/recognize"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"title": title}

        res = requests.get(url, headers=headers, params=params, timeout=10)
        if res.status_code == 200:
            data = res.json()
            media_info = data.get("media_info")
            
            if media_info and media_info.get("tmdb_id"):
                tmdb_id = str(media_info.get("tmdb_id"))
                m_type = "tv" if media_info.get("type") == "电视剧" else "movie"
                m_title = media_info.get("title") or media_info.get("name")
                return tmdb_id, m_type, m_title
                
        return None
    except Exception as e:
        logger.warning(f"  ➜ 调用 MoviePilot 识别接口失败: {e}")
        return None