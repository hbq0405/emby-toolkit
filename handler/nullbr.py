# handler/nullbr.py
import logging
import requests
import re
import time  
import threading 
from datetime import datetime
from database import settings_db, media_db, request_db
import config_manager

import constants
import utils
try:
    from p115client import P115Client
except ImportError:
    P115Client = None

logger = logging.getLogger(__name__)

# ★★★ 硬编码配置：Nullbr ★★★
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.com"

# 简单的内存缓存，用于存储用户等级以控制请求频率，避免每次都查库
_user_level_cache = {
    "sub_name": "free",
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

def _is_resource_valid(item, filters, media_type='movie'):
    """根据配置过滤资源 (保持原有逻辑)"""
    if not filters: return True
    
    # 1. 分辨率
    if filters.get('resolutions'):
        res = item.get('resolution')
        if not res or res not in filters['resolutions']: return False

    # 2. 质量
    if filters.get('qualities'):
        item_quality = item.get('quality')
        if not item_quality: return False
        q_list = [item_quality] if isinstance(item_quality, str) else item_quality
        if not any(q in q_list for q in filters['qualities']): return False

    # 3. 大小
    min_s = float(filters.get('tv_min_size' if media_type == 'tv' else 'movie_min_size') or 0)
    max_s = float(filters.get('tv_max_size' if media_type == 'tv' else 'movie_max_size') or 0)
    if min_s > 0 or max_s > 0:
        size_gb = _parse_size_to_gb(item.get('size'))
        if min_s > 0 and size_gb < min_s: return False
        if max_s > 0 and size_gb > max_s: return False

    # 4. 中字
    if filters.get('require_zh'):
        if item.get('is_zh_sub'): return True
        title = item.get('title', '').upper()
        zh_keywords = ['中字', '中英', '字幕', 'CHS', 'CHT', 'CN', 'DIY', '国语', '国粤']
        if not any(k in title for k in zh_keywords): return False

    # 5. 容器 (仅电影)
    if media_type != 'tv' and filters.get('containers'):
        title = item.get('title', '').lower()
        link = item.get('link', '').lower()
        ext = None
        if 'mkv' in title or link.endswith('.mkv'): ext = 'mkv'
        elif 'mp4' in title or link.endswith('.mp4'): ext = 'mp4'
        elif 'iso' in title or link.endswith('.iso'): ext = 'iso'
        if not ext or ext not in filters['containers']: return False

    return True

# ==============================================================================
# ★★★ 新增：用户 API 交互与自动流控 ★★★
# ==============================================================================

def get_user_info():
    """
    获取用户信息 (配额、等级、过期时间)
    """
    url = f"{NULLBR_API_BASE}/user/info"
    try:
        proxies = config_manager.get_proxies_for_requests()
        
        logger.info(f"正在获取 NULLBR 用户信息: {url}")
        response = requests.get(url, headers=_get_headers(), timeout=15, proxies=proxies)
        
        # 打印一下原始响应，方便调试
        if response.status_code != 200:
            logger.error(f"NULLBR 用户信息请求失败 [{response.status_code}]: {response.text}")
            
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            user_data = data.get('data', {})
            _user_level_cache['sub_name'] = user_data.get('sub_name', 'free').lower()
            _user_level_cache['updated_at'] = time.time()
            return user_data
        else:
            raise Exception(data.get('message', '获取用户信息失败'))
    except Exception as e:
        logger.error(f"获取 NULLBR 用户信息异常: {e}")
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
        logger.error(f"兑换请求异常: {e}")
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
        logger.error(f"NULLBR 搜索失败: {e}")
        raise e

def _fetch_single_source(tmdb_id, media_type, source_type, season_number=None):
    _wait_for_rate_limit() # 自动流控
    
    url = ""
    if media_type == 'movie':
        url = f"{NULLBR_API_BASE}/movie/{tmdb_id}/{source_type}"
    elif media_type == 'tv':
        if season_number:
            url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/{season_number}/{source_type}"
        else:
            if source_type == '115':
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/115"
            elif source_type == 'magnet':
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/1/magnet"
            else:
                return []
    else:
        return []

    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, headers=_get_headers(), timeout=10, proxies=proxies)
        
        if response.status_code == 404: return []
        # 处理配额耗尽的情况
        if response.status_code == 402:
            logger.warning("NULLBR 配额已耗尽")
            return []
            
        response.raise_for_status()
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
                
                # 季号清洗逻辑 (保持不变)
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
        logger.warning(f"获取 {source_type} 资源失败: {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie', specific_source=None, season_number=None):
    config = get_config()
    if specific_source:
        sources_to_fetch = [specific_source]
    else:
        sources_to_fetch = config.get('enabled_sources', ['115', 'magnet', 'ed2k'])
    
    all_resources = []
    
    if '115' in sources_to_fetch:
        try: all_resources.extend(_fetch_single_source(tmdb_id, media_type, '115', season_number))
        except: pass
    if 'magnet' in sources_to_fetch:
        try: all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'magnet', season_number))
        except: pass
    if media_type == 'movie' and 'ed2k' in sources_to_fetch:
        try: all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'ed2k'))
        except: pass
    
    filters = config.get('filters', {})
    if not any(filters.values()): return all_resources
        
    filtered_list = [res for res in all_resources if _is_resource_valid(res, filters, media_type)]
    return filtered_list

# ==============================================================================
# ★★★ 115 推送逻辑 (保持不变) ★★★
# ==============================================================================

def _clean_link(link):
    if not link: return ""
    link = link.strip()
    while link.endswith('&#') or link.endswith('&') or link.endswith('#'):
        if link.endswith('&#'): link = link[:-2]
        elif link.endswith('&') or link.endswith('#'): link = link[:-1]
    return link

def notify_cms_scan():
    config = get_config()
    cms_url = config.get('cms_url')
    cms_token = config.get('cms_token')
    if not cms_url or not cms_token: return

    api_url = f"{cms_url.rstrip('/')}/api/sync/lift_by_token"
    try:
        requests.get(api_url, params={"type": "auto_organize", "token": cms_token}, timeout=5)
    except Exception as e:
        logger.warning(f"CMS 通知发送失败: {e}")

def push_to_115(resource_link, title):
    if P115Client is None: raise ImportError("未安装 p115 库")
    config = get_config()
    cookies = config.get('p115_cookies')
    save_path_cid = int(config.get('p115_save_path_cid', 0) or 0)

    if not cookies: raise ValueError("未配置 115 Cookies")
    clean_url = _clean_link(resource_link)
    client = P115Client(cookies)
    
    try:
        target_domains = ['115.com', '115cdn.com', 'anxia.com']
        is_115_share = any(d in clean_url for d in target_domains) and ('magnet' not in clean_url)
        
        if is_115_share:
            # 115 转存
            share_code = None
            match = re.search(r'/s/([a-z0-9]+)', clean_url)
            if match: share_code = match.group(1)
            if not share_code: raise Exception("无法提取分享码")
            receive_code = ''
            pwd_match = re.search(r'password=([a-z0-9]+)', clean_url)
            if pwd_match: receive_code = pwd_match.group(1)
            
            resp = {} 
            try:
                if hasattr(client, 'fs_share_import_to_dir'):
                     resp = client.fs_share_import_to_dir(share_code, receive_code, save_path_cid)
                elif hasattr(client, 'fs_share_import'):
                    resp = client.fs_share_import(share_code, receive_code, save_path_cid)
                else:
                    api_url = "https://webapi.115.com/share/receive"
                    payload = {'share_code': share_code, 'receive_code': receive_code, 'cid': save_path_cid}
                    r = client.request(api_url, method='POST', data=payload)
                    resp = r.json() if hasattr(r, 'json') else r
            except Exception as e:
                raise Exception(f"调用转存接口失败: {e}")

            if resp and resp.get('state'): return True
            else: raise Exception(f"转存失败: {resp}")

        else:
            # 离线下载 (指纹对比)
            existing_pick_codes = set()
            try:
                files_res = client.fs_files({'cid': save_path_cid, 'limit': 50, 'o': 'user_ptime', 'asc': 0})
                if files_res.get('data'):
                    for item in files_res['data']:
                        if item.get('pc'): existing_pick_codes.add(item.get('pc'))
            except: pass
            
            payload = {'url[0]': clean_url, 'wp_path_id': save_path_cid}
            resp = client.offline_add_urls(payload)
            
            if resp.get('state'):
                result_list = resp.get('result', [])
                info_hash = result_list[0].get('info_hash') if result_list else None
                
                success_found = False
                for i in range(3): # 检查3次
                    time.sleep(3) 
                    try:
                        check_res = client.fs_files({'cid': save_path_cid, 'limit': 50, 'o': 'user_ptime', 'asc': 0})
                        if check_res.get('data'):
                            for item in check_res['data']:
                                if item.get('pc') and (item.get('pc') not in existing_pick_codes):
                                    success_found = True
                                    break
                        if success_found: break
                    except: pass
                    
                    # 检查任务失败
                    try:
                        list_resp = client.offline_list(page=1)
                        for task in list_resp.get('tasks', [])[:10]:
                            if info_hash and task.get('info_hash') == info_hash and task.get('status') == -1:
                                try: client.offline_delete([info_hash])
                                except: pass
                                raise Exception("115任务下载失败")
                    except Exception as te:
                        if "下载失败" in str(te): raise te

                if success_found: return True
                else:
                    try: 
                        if info_hash: client.offline_delete([info_hash])
                    except: pass
                    raise Exception("资源无效或下载过慢")
            else:
                err = resp.get('error_msg') or resp.get('msg')
                if '已存在' in str(err): return True
                raise Exception(f"离线失败: {err}")

    except Exception as e:
        logger.error(f"115 推送异常: {e}")
        if "Login" in str(e) or "cookie" in str(e).lower(): raise Exception("115 Cookie 无效")
        raise e

def get_115_account_info():
    if P115Client is None: raise Exception("未安装 p115client")
    config = get_config()
    cookies = config.get('p115_cookies')
    if not cookies: raise Exception("未配置 Cookies")
    try:
        client = P115Client(cookies)
        resp = client.fs_files({'limit': 1})
        if not resp.get('state'): raise Exception("Cookie 已失效")
        return {"valid": True, "msg": "Cookie 状态正常"}
    except Exception:
        raise Exception("Cookie 无效或网络不通")

def handle_push_request(link, title):
    push_to_115(link, title)
    notify_cms_scan()
    return True

def auto_download_best_resource(tmdb_id, media_type, title, season_number=None):
    try:
        config = get_config()
        if not config.get('api_key'): return False
        
        # 自动任务前先更新一下用户信息，确保有配额
        try: get_user_info()
        except: pass

        priority_sources = ['115', 'magnet', 'ed2k']
        user_enabled = config.get('enabled_sources', priority_sources)
        
        for source in priority_sources:
            if source not in user_enabled: continue
            if media_type == 'tv' and source == 'ed2k': continue

            resources = fetch_resource_list(tmdb_id, media_type, specific_source=source, season_number=season_number)
            if not resources: continue

            for res in resources:
                try:
                    handle_push_request(res['link'], title)
                    return True
                except: continue
        return False
    except Exception as e:
        logger.error(f"NULLBR 自动兜底失败: {e}")
        return False