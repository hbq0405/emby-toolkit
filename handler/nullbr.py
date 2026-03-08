# handler/nullbr.py
import logging
import requests
import threading
import re
import time  
from datetime import datetime
from database import settings_db, media_db, request_db
import config_manager

import constants
import utils
import handler.tmdb as tmdb
from handler.p115_service import P115Service, SmartOrganizer, logger

logger = logging.getLogger(__name__)

# 硬编码配置：Nullbr 
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.com"

# 内存缓存，用于存储用户等级以控制请求频率，避免每次都查库
_user_level_cache = {
    "sub_name": "free",
    "daily_used": 0,
    "daily_quota": 0,
    "updated_at": 0
}

def get_config():
    return settings_db.get_setting('nullbr_config') or {}

def _get_headers():
    config = get_config()
    api_key = config.get('api_key')
    headers = {
        "Content-Type": "application/json",
        "X-APP-ID": NULLBR_APP_ID,
        "User-Agent": f"EmbyToolkit/{constants.APP_VERSION}"
    }
    if api_key:
        headers["X-API-KEY"] = api_key
    return headers

def _parse_size_to_gb(size_str):
    """将大小字符串转换为 GB (float)"""
    if not size_str: return 0.0
    size_str = size_str.upper().replace(',', '')
    match = re.search(r'([\d\.]+)\s*(TB|GB|MB|KB)', size_str)
    if not match: return 0.0
    num = float(match.group(1))
    unit = match.group(2)
    if unit == 'TB': return num * 1024
    elif unit == 'GB': return num
    elif unit == 'MB': return num / 1024
    elif unit == 'KB': return num / 1024 / 1024
    return 0.0

def _is_resource_valid(item, filters, media_type='movie', episode_count=0):
    """根据配置过滤资源"""
    if not filters:
        return True

    # 1. 分辨率过滤
    allowed_resolutions = filters.get('resolutions', [])
    if allowed_resolutions:
        res = item.get('resolution')
        if not res or res not in allowed_resolutions:
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为分辨率 {res} 不在允许列表中")
            return False

    # 2. 质量过滤
    allowed_qualities = filters.get('qualities', [])
    if allowed_qualities:
        item_quality = item.get('quality')
        if not item_quality: return False
        q_list = [item_quality] if isinstance(item_quality, str) else item_quality
        if not any(q in q_list for q in allowed_qualities): 
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为质量 {item_quality} 不在允许列表中")
            return False

    # 3. 大小过滤 (GB) 
    min_size = 0.0
    max_size = 0.0

    if media_type == 'tv':
        # 优先取 tv_min_size，取不到(None)则尝试取 min_size，最后默认为 0
        v_min = filters.get('tv_min_size')
        if v_min is None: v_min = filters.get('min_size')
        min_size = float(v_min or 0)

        v_max = filters.get('tv_max_size')
        if v_max is None: v_max = filters.get('max_size')
        max_size = float(v_max or 0)
    else:
        v_min = filters.get('movie_min_size')
        if v_min is None: v_min = filters.get('min_size')
        min_size = float(v_min or 0)

        v_max = filters.get('movie_max_size')
        if v_max is None: v_max = filters.get('max_size')
        max_size = float(v_max or 0)
    
    if min_size > 0 or max_size > 0:
        size_gb = _parse_size_to_gb(item.get('size'))
        
        # 计算检查用的数值
        check_size = size_gb
        
        # 只有当是剧集、且成功获取到了集数、且集数大于0时，才计算平均大小
        if media_type == 'tv' and episode_count > 0:
            check_size = size_gb / episode_count
            # 调试日志 (可选开启)
            # logger.debug(f"  [大小检查] 总大小: {size_gb}G, 集数: {episode_count}, 平均: {check_size:.2f}G (限制: {min_size}-{max_size})")

        if min_size > 0 and check_size < min_size:
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为大小 {check_size:.2f}G 小于最小限制 {min_size}G")
            return False
        if max_size > 0 and check_size > max_size:
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为大小 {check_size:.2f}G 大于最大限制 {max_size}G")
            return False

    # 4. 中字过滤
    if filters.get('require_zh'):
        if item.get('is_zh_sub'): return True
        title = item.get('title', '').upper()
        zh_keywords = ['中字', '中英', '字幕', 'CHS', 'CHT', 'CN', 'DIY', '国语', '国粤']
        if not any(k in title for k in zh_keywords): 
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为未检测到中文字幕")
            return False
            

    # 5. 容器过滤
    allowed_containers = filters.get('containers', [])
    if allowed_containers:
        if media_type == 'tv': return True
        title = item.get('title', '').lower()
        link = item.get('link', '').lower()
        ext = None

        if link.startswith('ed2k://'):
            # Ed2k 格式: ed2k://|file|文件名|大小|哈希|/
            # 使用 | 分割，文件名通常在第 3 部分 (索引 2)
            try:
                parts = link.split('|')
                if len(parts) >= 3:
                    file_name_in_link = parts[2].lower()
                    if file_name_in_link.endswith('.mkv'): ext = 'mkv'
                    elif file_name_in_link.endswith('.mp4'): ext = 'mp4'
                    elif file_name_in_link.endswith('.iso'): ext = 'iso'
                    elif file_name_in_link.endswith('.ts'): ext = 'ts'
                    elif file_name_in_link.endswith('.avi'): ext = 'avi'
            except:
                pass # 解析失败则忽略，回退到下方逻辑

        # 如果上面没提取到 (比如是磁力链或 115 码)，则走原有逻辑
        if not ext:
            if 'mkv' in title or link.endswith('.mkv'): ext = 'mkv'
            elif 'mp4' in title or link.endswith('.mp4'): ext = 'mp4'
            elif 'iso' in title or link.endswith('.iso'): ext = 'iso'
            elif 'ts' in title or link.endswith('.ts'): ext = 'ts'
            elif 'avi' in title or link.endswith('.avi'): ext = 'avi'
            
        if not ext or ext not in allowed_containers: 
            logger.debug(f"  ➜ 资源《{item.get('title')}》被过滤掉了，因为容器 {ext} 不在允许列表中")
            return False

    return True

# ==============================================================================
# ★★★ 新增：用户 API 交互与自动流控 ★★★
# ==============================================================================

def get_user_info():
    """获取用户信息"""
    url = f"{NULLBR_API_BASE}/user/info"
    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, headers=_get_headers(), timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            user_data = data.get('data', {})
            _user_level_cache.update({
                'sub_name': user_data.get('sub_name', 'free').lower(),
                'daily_used': user_data.get('daily_used', 0),
                'daily_quota': user_data.get('daily_quota', 0),
                'updated_at': time.time()
            })
            return user_data
        else:
            raise Exception(data.get('message', '获取用户信息失败'))
    except Exception as e:
        logger.error(f"  ⚠️ 获取 NULLBR 用户信息异常: {e}")
        raise e

def redeem_code(code):
    """
    使用兑换码
    """
    url = f"{NULLBR_API_BASE}/user/redeem"
    payload = {"code": code}
    try:
        proxies = config_manager.get_proxies_for_requests()
        
        response = requests.post(url, json=payload, headers=_get_headers(), timeout=15, proxies=proxies)
        data = response.json()
        
        if response.status_code == 200 and data.get('success'):
            get_user_info()
            return data
        else:
            msg = data.get('message') or "兑换失败"
            return {"success": False, "message": msg}
    except Exception as e:
        logger.error(f"  ➜ 兑换请求异常: {e}")
        return {"success": False, "message": str(e)}

def _wait_for_rate_limit():
    """
    根据用户等级自动执行流控睡眠
    Free: 25 req/min -> ~2.4s interval
    Silver: 60 req/min -> ~1.0s interval
    Golden: 100 req/min -> ~0.6s interval
    """
    # 如果缓存过期(超过1小时)，尝试更新一下，但不阻塞主流程
    if time.time() - _user_level_cache['updated_at'] > 3600:
        try:
            get_user_info()
        except:
            pass 

    level = _user_level_cache.get('sub_name', 'free')
    
    if 'golden' in level:
        time.sleep(0.6)
    elif 'silver' in level:
        time.sleep(1.0)
    else:
        # Free or unknown
        time.sleep(2.5)

def _enrich_items_with_status(items):
    """批量查询本地库状态 (保持不变)"""
    if not items: return items
    tmdb_ids = [str(i.get('tmdbid') or i.get('id')) for i in items if (i.get('tmdbid') or i.get('id'))]
    if not tmdb_ids: return items

    library_map_movie = media_db.check_tmdb_ids_in_library(tmdb_ids, 'Movie')
    library_map_series = media_db.check_tmdb_ids_in_library(tmdb_ids, 'Series')
    sub_status_movie = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, 'Movie')
    sub_status_series = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, 'Series')

    for item in items:
        tid = str(item.get('tmdbid') or item.get('id') or '')
        mtype = item.get('media_type', 'movie')
        if not tid: continue
        
        in_lib = False
        sub_stat = None
        if mtype == 'tv':
            if f"{tid}_Series" in library_map_series: in_lib = True
            sub_stat = sub_status_series.get(tid)
        else:
            if f"{tid}_Movie" in library_map_movie: in_lib = True
            sub_stat = sub_status_movie.get(tid)
        
        item['in_library'] = in_lib
        item['subscription_status'] = sub_stat
    return items

def get_preset_lists():
    custom_presets = settings_db.get_setting('nullbr_presets')
    if custom_presets and isinstance(custom_presets, list) and len(custom_presets) > 0:
        return custom_presets
    return utils.DEFAULT_NULLBR_PRESETS

def fetch_list_items(list_id, page=1):
    _wait_for_rate_limit()
    url = f"{NULLBR_API_BASE}/list/{list_id}"
    params = {"page": page}
    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        items = data.get('items', [])
        enriched_items = _enrich_items_with_status(items)
        return {"code": 200, "data": {"list": enriched_items, "total": data.get('total_results', 0)}}
    except Exception as e:
        logger.error(f"获取片单失败: {e}")
        raise e

def search_media(keyword, page=1):
    _wait_for_rate_limit() # 自动流控
    url = f"{NULLBR_API_BASE}/search"
    params = { "query": keyword, "page": page }
    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        items = data.get('items', [])
        enriched_items = _enrich_items_with_status(items)
        return { "code": 200, "data": { "list": enriched_items, "total": data.get('total_results', 0) } }
    except Exception as e:
        logger.error(f"  ➜ NULLBR 搜索失败: {e}")
        raise e

def _fetch_single_source(tmdb_id, media_type, source_type, season_number=None, episode_number=None):
    _wait_for_rate_limit() # 自动流控
    
    url = ""
    if media_type == 'movie':
        url = f"{NULLBR_API_BASE}/movie/{tmdb_id}/{source_type}"
    elif media_type == 'tv':
        # ★★★ 核心修改：支持单集 URL 拼接 ★★★
        if season_number is not None:
            if episode_number is not None:
                # 接口: /tv/{id}/season/{s}/episode/{e}/{source}
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}/{source_type}"
            else:
                # 接口: /tv/{id}/season/{s}/{source}
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/{season_number}/{source_type}"
        else:
            # 整剧搜索 (通常只有 115 支持，或者 magnet 搜第一季)
            if source_type == '115':
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/115"
            elif source_type == 'magnet':
                # 如果没传季号，默认搜第1季磁力，或者你可以选择不搜
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/1/magnet"
            else:
                return []
    else:
        return []

    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, headers=_get_headers(), timeout=10, proxies=proxies)
        
        if response.status_code == 404: return []
        
        if response.status_code == 402:
            logger.warning("  ⚠️ NULLBR 接口返回 402: 配额已耗尽")
            if _user_level_cache['daily_quota'] > 0:
                _user_level_cache['daily_used'] = _user_level_cache['daily_quota']
            return []
            
        response.raise_for_status()
        
        _user_level_cache['daily_used'] = _user_level_cache.get('daily_used', 0) + 1
        
        data = response.json()
        raw_list = data.get(source_type, [])
        
        cleaned_list = []
        for item in raw_list:
            link = item.get('share_link') or item.get('magnet') or item.get('ed2k')
            title = item.get('title') or item.get('name')
            
            if link and title:
                if media_type == 'tv' and source_type == 'magnet' and not season_number:
                    title = f"[S1] {title}"
                
                is_zh = item.get('zh_sub') == 1
                if not is_zh:
                    t_upper = title.upper()
                    zh_keywords = ['中字', '中英', '字幕', 'CHS', 'CHT', 'CN', 'DIY', '国语', '国粤']
                    if any(k in t_upper for k in zh_keywords): is_zh = True
                
                # 季号清洗逻辑
                if media_type == 'tv' and season_number:
                    try:
                        target_season = int(season_number)
                        match = re.search(r'(?:^|\.|\[|\s|-)S(\d{1,2})(?:\.|\]|\s|E|-|$)', title.upper())
                        if match and int(match.group(1)) != target_season: continue
                        match_zh = re.search(r'第(\d{1,2})季', title)
                        if match_zh and int(match_zh.group(1)) != target_season: continue
                    except: pass

                cleaned_list.append({
                    "title": title,
                    "size": item.get('size', '未知'),
                    "resolution": item.get('resolution'),
                    "quality": item.get('quality'),
                    "link": link,
                    "source_type": source_type.upper(),
                    "is_zh_sub": is_zh
                })
        return cleaned_list
    except Exception as e:
        logger.warning(f"  ➜ 获取 {source_type} 资源失败: {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie', specific_source=None, season_number=None, episode_number=None):
    config = get_config()
    
    # 1. 确定要搜索的源
    if specific_source:
        sources_to_fetch = [specific_source]
    else:
        # 必须拷贝一份，防止修改原配置
        sources_to_fetch = list(config.get('enabled_sources', ['115', 'magnet', 'ed2k']))
    
    # 2. 获取过滤配置 (提前获取)
    filters = config.get('filters', {})
    
    # 如果开启了容器过滤，强制跳过磁力链 搜索以节省配额
    allowed_containers = filters.get('containers', [])
    if allowed_containers and 'magnet' in sources_to_fetch:
        logger.debug(f"  ➜ [NULLBR] 检测到开启了容器过滤 ({allowed_containers})，已跳过磁力链搜索以节省配额。")
        sources_to_fetch.remove('magnet')
    
    # 配额检查
    if _user_level_cache.get('daily_quota', 0) > 0 and _user_level_cache.get('daily_used', 0) >= _user_level_cache.get('daily_quota', 0):
        logger.warning(f"  ⚠️ 今日配额已用完，无法请求API搜索资源。")
        raise Exception("今日 API 配额已用完，请明日再试或升级套餐。")

    # ==============================================================================
    # ★★★ 提前计算集数 (用于大小过滤) ★★★
    # ==============================================================================
    episode_count = 0
    should_fetch_ep_count = False
    
    # 只有是剧集且有季号时才考虑
    if media_type == 'tv' and season_number is not None:
        # 检查是否配置了大小限制
        t_min = filters.get('tv_min_size')
        if t_min is None: t_min = filters.get('min_size')
        
        t_max = filters.get('tv_max_size')
        if t_max is None: t_max = filters.get('max_size')
        
        try:
            if (t_min and float(t_min) > 0) or (t_max and float(t_max) > 0):
                should_fetch_ep_count = True
        except:
            pass 

    if should_fetch_ep_count:
        try:
            tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            if tmdb_api_key:
                season_info = tmdb.get_tv_season_details(tmdb_id, season_number, tmdb_api_key)
                if season_info and 'episodes' in season_info:
                    episode_count = len(season_info['episodes'])
                    logger.info(f"  ➜ [NULLBR] 获取到 （第 {season_number} 季） 总集数: {episode_count}，将按单集平均大小过滤。")
        except Exception as e:
            logger.warning(f"  ⚠️ 获取 TMDb 季集数失败: {e}")

    # ==============================================================================
    # ★★★ 循环获取并分别过滤 ★★★
    # ==============================================================================
    final_filtered_list = []
    
    # 定义源名称映射
    source_name_map = {
        '115': '115分享',
        'magnet': '磁力链',
        'ed2k': '电驴(Ed2k)'
    }

    for source in sources_to_fetch:
        try:
            # 针对 ed2k 的特殊判断 (TV 不搜 ed2k)
            if media_type == 'tv' and source == 'ed2k':
                if episode_number is None:
                    continue
                
            # 1. 获取原始资源
            raw_res = _fetch_single_source(tmdb_id, media_type, source, season_number, episode_number)
            
            if not raw_res:
                continue

            # 2. 立即执行过滤
            current_filtered = [
                res for res in raw_res 
                if _is_resource_valid(res, filters, media_type, episode_count=episode_count)
            ]
            
            # 3. 打印带源名称的日志
            cn_name = source_name_map.get(source, source.upper())
            logger.info(f"  ➜ {cn_name} 资源过滤: 原始 {len(raw_res)} -> 过滤后 {len(current_filtered)}")
            
            # 4. 加入最终列表
            if current_filtered:
                final_filtered_list.extend(current_filtered)

        except Exception as e:
            logger.warning(f"  ➜ 获取 {source} 资源异常: {e}")

    return final_filtered_list

# ==============================================================================
# ★★★ 115 推送逻辑  ★★★
# ==============================================================================

def _clean_link(link):
    """
    清洗链接：去除首尾空格，并安全去除末尾的 HTML 脏字符 (&#)
    """
    if not link:
        return ""
    link = link.strip()
    while link.endswith('&#') or link.endswith('&') or link.endswith('#'):
        if link.endswith('&#'):
            link = link[:-2]
        elif link.endswith('&') or link.endswith('#'):
            link = link[:-1]
    return link

def _standardize_115_file(client, file_item, save_cid, raw_title, tmdb_id, media_type='movie'):
    """
    对 115 新入库的文件/文件夹进行基础的标准化重命名 (套壳加 TMDb ID)
    """
    try:
        # ==================================================
        # 1. 获取官方元数据 (TMDb)
        # ==================================================
        final_title = raw_title
        final_year = None

        try:
            tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
            if tmdb_api_key and tmdb_id:
                details = None
                if media_type == 'tv':
                    details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
                    if details:
                        final_title = details.get('name')
                        first_air_date = details.get('first_air_date')
                        if first_air_date: final_year = first_air_date[:4]
                else:
                    details = tmdb.get_movie_details(tmdb_id, tmdb_api_key)
                    if details:
                        final_title = details.get('title')
                        release_date = details.get('release_date')
                        if release_date: final_year = release_date[:4]
        except Exception as e:
            logger.warning(f"  ⚠️ [基础整理] TMDb 获取失败: {e}")

        if not final_year:
            match = re.search(r'[(（](\d{4})[)）]', raw_title)
            if match: final_year = match.group(1)

        safe_title = re.sub(r'[\\/:*?"<>|]', '', final_title).strip()
        std_name = f"{safe_title} ({final_year}) {{tmdb={tmdb_id}}}" if final_year else f"{safe_title} {{tmdb={tmdb_id}}}"

        # ==================================================
        # 2. 核心修复：精准提取自身 ID，绝不混用父目录 CID
        # ==================================================
        fc_val = file_item.get('fc') if file_item.get('fc') is not None else file_item.get('type')
        # ★ 修复：必须使用 fid 或 file_id 作为操作对象的唯一标识
        item_id = file_item.get('fid') or file_item.get('file_id')
        current_name = file_item.get('n') or file_item.get('fn') or file_item.get('file_name')
        
        if not item_id:
            logger.error(f"  ❌ [基础整理] 无法获取项目自身 ID，跳过操作: {current_name}")
            return

        is_directory = str(fc_val) == '0'

        if current_name == std_name:
            logger.info(f"  ✅ [基础整理] 名称已符合标准，跳过操作。")
            return

        if is_directory:
            # === 情况 A: 文件夹直接重命名 ===
            logger.info(f"  🛠️ [基础整理] 识别为文件夹，执行重命名: {current_name} -> {std_name}")

            # ★ 修复：传入正确的自身 ID
            rename_res = client.fs_rename((item_id, std_name))

            if isinstance(rename_res, dict) and rename_res.get('state'):
                logger.info(f"  ✅ [基础整理] 文件夹重命名成功")
            else:
                logger.warning(f"  ⚠️ [基础整理] 重命名失败: {rename_res}")

        else:
            # === 情况 B: 单文件创建外层标准文件夹并移入 ===
            logger.info(f"  🛠️ [基础整理] 识别为单文件，正在归档至标准目录: {std_name}")

            target_dir_cid = None
            search_res = client.fs_files({'cid': save_cid, 'search_value': std_name, 'record_open_time': 0, 'count_folders': 0})
            if isinstance(search_res, dict) and search_res.get('data'):
                for item in search_res['data']:
                    item_name = item.get('n') or item.get('fn') or item.get('file_name')
                    item_fc = item.get('fc') if item.get('fc') is not None else item.get('type')
                    if item_name == std_name and str(item_fc) == '0':
                        # ★ 修复：找到目标文件夹后，提取它的自身 ID 作为移动的目标 CID
                        target_dir_cid = item.get('fid') or item.get('file_id')
                        break

            if not target_dir_cid:
                mkdir_res = client.fs_mkdir(std_name, save_cid)
                if isinstance(mkdir_res, dict) and mkdir_res.get('state'):
                    target_dir_cid = mkdir_res.get('cid')
                else:
                    logger.error(f"  ❌ [基础整理] 创建标准文件夹失败")
                    return

            # ★ 修复：传入正确的单文件 ID 和目标文件夹 ID
            move_res = client.fs_move(item_id, target_dir_cid)
            if isinstance(move_res, dict) and move_res.get('state'):
                logger.info(f"  ✅ [基础整理] 单文件已成功归档至标准目录")
            else:
                logger.warning(f"  ⚠️ [基础整理] 移动文件失败: {move_res}")

    except Exception as e:
        logger.error(f"  ⚠️ 基础整理流程异常: {e}", exc_info=True)

def push_to_115(resource_link, title, tmdb_id=None, media_type=None, season_number=None):
    """
    智能推送：支持 115/115cdn/anxia 转存 和 磁力离线
    并执行 智能整理 (Smart Organize)
    """
    client = P115Service.get_client()
    if not client: raise Exception("无法初始化 115 客户端")

    # ★ 彻底抛弃 NULLBR 自身配置，统一使用全局配置读取 115 相关设置
    global_config = config_manager.APP_CONFIG
    
    from handler.p115_service import get_115_tokens
    _, _, cookie = get_115_tokens()
    cookie = (cookie or "").strip()
    
    try:
        # ★ 从全局配置读取待整理目录 CID
        cid_val = global_config.get(constants.CONFIG_OPTION_115_SAVE_PATH_CID, 0)
        save_path_cid = int(cid_val) if cid_val else 0
    except:
        save_path_cid = 0

    if not cookie:
        raise ValueError("未配置 115 Cookies")

    clean_url = _clean_link(resource_link)
    logger.info(f"  ➜ [NULLBR] 待处理链接: {clean_url}")
    
    existing_ids = set()
    try:
        files_res = client.fs_files({'cid': save_path_cid, 'limit': 50, 'o': 'user_ptime', 'asc': 0, 'record_open_time': 0, 'count_folders': 0})
        if files_res.get('data'):
            for item in files_res['data']:
                item_id = item.get('fid') or item.get('cid') or item.get('file_id')
                if item_id: existing_ids.add(str(item_id))
    except Exception as e:
        logger.warning(f"  ⚠️ 获取目录快照失败: {e}")

    target_domains = ['115.com', '115cdn.com', 'anxia.com']
    is_115_share = any(d in clean_url for d in target_domains) and ('magnet' not in clean_url)
    task_success = False
    
    try:
        if is_115_share:
            logger.info(f"  ➜ [NULLBR] 识别为 115 转存任务 -> CID: {save_path_cid}")
            share_code = None
            match = re.search(r'/s/([a-z0-9]+)', clean_url)
            if match: share_code = match.group(1)
            if not share_code: raise Exception("无法提取分享码")
            receive_code = ''
            pwd_match = re.search(r'password=([a-z0-9]+)', clean_url)
            if pwd_match: receive_code = pwd_match.group(1)
            
            resp = {} 
            if hasattr(client, 'fs_share_import_to_dir'):
                    resp = client.fs_share_import_to_dir(share_code, receive_code, save_path_cid)
            elif hasattr(client, 'fs_share_import'):
                resp = client.fs_share_import(share_code, receive_code, save_path_cid)
            elif hasattr(client, 'share_import'):
                resp = client.share_import(share_code, receive_code, save_path_cid)
            else:
                api_url = "https://webapi.115.com/share/receive"
                payload = {'share_code': share_code, 'receive_code': receive_code, 'cid': save_path_cid}
                r = client.request(api_url, method='POST', data=payload)
                resp = r.json() if hasattr(r, 'json') else r

            if resp and resp.get('state'):
                logger.info(f"  ✅ 115 转存请求成功")
                task_success = True
            else:
                err = resp.get('error_msg') or resp.get('msg') or str(resp)
                raise Exception(f"转存失败: {err}")
        else:
            logger.info(f"  ➜ [NULLBR] 识别为磁力/离线任务 -> CID: {save_path_cid}")
            payload = {'url[0]': clean_url, 'wp_path_id': save_path_cid}
            resp = client.offline_add_urls(payload)
            if resp.get('state'):
                task_success = True
                logger.info(f"  ➜ [NULLBR] 任务已提交，等待文件生成...")
            else:
                err = resp.get('error_msg') or resp.get('msg') or '未知错误'
                if '已存在' in str(err):
                    task_success = True
                    logger.info(f"  ✅ 任务已存在")
                else:
                    raise Exception(f"离线失败: {err}")
    except Exception as e:
        raise e

    if task_success:
        max_retries = 8
        found_item = None
        
        for i in range(max_retries):
            time.sleep(3)
            try:
                check_res = client.fs_files({'cid': save_path_cid, 'limit': 50, 'o': 'user_ptime', 'asc': 0, 'record_open_time': 0, 'count_folders': 0})
                if check_res.get('data'):
                    for item in check_res['data']:
                        current_id = item.get('fid') or item.get('cid') or item.get('file_id')
                        if current_id and (str(current_id) not in existing_ids):
                            found_item = item
                            break
                if found_item:
                    break
            except Exception as e:
                logger.debug(f"轮询出错: {e}")
        
        if found_item:
            item_name = found_item.get('n') or found_item.get('fn') or found_item.get('file_name', '未知')
            logger.info(f"  👀 捕获到新入库项目: {item_name}")
            
            if tmdb_id:
                try:
                    enable_organize = global_config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False)
                    
                    if enable_organize:
                        logger.info("  🧠 [整理] 智能整理已开启，开始分析...")
                        organizer = SmartOrganizer(client, tmdb_id, media_type, title)
                        
                        # ==================================================
                        # ★★★ 核心修改：注入强制季号，锁死识别结果 ★★★
                        # ==================================================
                        if season_number is not None:
                            try:
                                organizer.forced_season = int(season_number)
                                logger.info(f"  📌 [整理] 已接收到前端传递的强制季号: Season {organizer.forced_season}")
                            except:
                                pass

                        target_cid = organizer.get_target_cid()
                        organizer.execute(found_item, target_cid)
                    else:
                        logger.info("  ⏭️ [整理] 智能整理开关未开启，仅重命名，跳过整理操作。")
                        _standardize_115_file(client, found_item, save_path_cid, title, tmdb_id, media_type)
                        
                except Exception as e:
                    logger.error(f"  ❌ [整理] 智能整理执行失败: {e}", exc_info=True)
            else:
                logger.debug("  ⚠️ 未提供 TMDb ID，跳过整理")
            
            return True
        else:
            if is_115_share:
                logger.warning("  ⚠️ 转存显示成功但未捕获到新文件ID (可能文件已存在)")
                return True
            else:
                logger.warning("  ❌ 离线任务超时，未在目录发现新文件 (死链或下载过慢)")
                return True

    return False

def handle_push_request(link, title, tmdb_id=None, media_type=None, season_number=None):
    """
    统一推送入口
    """
    # 推送到 115 (传递 ID 以便重命名)
    push_to_115(link, title, tmdb_id, media_type, season_number)
    
    return True

def auto_download_best_resource(tmdb_id, media_type, title, season_number=None, episode_number=None):
    """
    [自动任务专用] 搜索并下载最佳资源
    :param season_number: 季号 (仅 media_type='tv' 时有效)
    """
    try:
        config = get_config()
        if not config.get('api_key'):
            logger.warning("NULLBR 未配置 API Key，无法执行自动兜底。")
            return False

        priority_sources = ['115', 'magnet', 'ed2k']
        user_enabled = config.get('enabled_sources', priority_sources)
        
        # 构造日志标题
        log_title = title
        if media_type == 'tv' and season_number:
            log_title = f"《{title}》第 {season_number} 季"

        logger.info(f"  ➜ [NULLBR] 开始搜索资源: {log_title} (ID: {tmdb_id})")

        for source in priority_sources:
            if source not in user_enabled: continue
            if media_type == 'tv' and source == 'ed2k': continue

            resources = fetch_resource_list(tmdb_id, media_type, specific_source=source, season_number=season_number, episode_number=episode_number)
            
            if not resources:
                continue

            logger.info(f"  ➜ [{source.upper()}] 找到 {len(resources)} 个资源，开始尝试推送...")

            for index, res in enumerate(resources):
                try:
                    logger.info(f"  👉 尝试第 {index + 1} 个资源: {res['title']}")
                    
                    # 调用统一推送入口 (115 -> CMS Notify)
                    handle_push_request(res['link'], title, tmdb_id, media_type, season_number)
                    
                    logger.info(f"  ✅ 资源推送成功，停止后续尝试。")
                    return True
                    
                except Exception as e:
                    logger.warning(f"  ❌ 第 {index + 1} 个资源推送失败: {e}")
                    logger.info("  🔄 正在尝试下一个资源...")
                    continue
            
            logger.info(f"  ⚠️ [{source.upper()}] 所有资源均尝试失败，切换下一源...")

        logger.info(f"  ❌ 所有源的所有资源均尝试失败: {log_title}")
        return False

    except Exception as e:
        logger.error(f"  ➜ NULLBR 搜索失败: {e}")
        return False
