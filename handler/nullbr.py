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
    # 只导入主类，不导入工具类，防止报错
    from p115client import P115Client
except ImportError:
    P115Client = None

logger = logging.getLogger(__name__)

# ★★★ 硬编码配置：Nullbr ★★★
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.com"

# 线程锁，防止并发请求导致计数器错乱
_rate_limit_lock = threading.Lock()

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
    """将大小字符串 (如 '83.03 GB', '500 MB') 转换为 GB (float)"""
    if not size_str:
        return 0.0
    
    size_str = size_str.upper().replace(',', '')
    match = re.search(r'([\d\.]+)\s*(TB|GB|MB|KB)', size_str)
    if not match:
        return 0.0
    
    num = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'TB':
        return num * 1024
    elif unit == 'GB':
        return num
    elif unit == 'MB':
        return num / 1024
    elif unit == 'KB':
        return num / 1024 / 1024
    return 0.0

def _is_resource_valid(item, filters, media_type='movie'):
    """根据配置过滤资源"""
    if not filters:
        return True

    # 1. 分辨率过滤 (如果配置了列表，则必须在列表中)
    allowed_resolutions = filters.get('resolutions', [])
    if allowed_resolutions:
        res = item.get('resolution')
        # 如果资源没标分辨率，或者分辨率不在允许列表中，则过滤
        if not res or res not in allowed_resolutions:
            return False

    # 2. 质量过滤 (只要包含其中一个关键词即可)
    allowed_qualities = filters.get('qualities', [])
    if allowed_qualities:
        item_quality = item.get('quality')
        # item_quality 可能是字符串也可能是列表
        if not item_quality:
            return False
        
        if isinstance(item_quality, str):
            q_list = [item_quality]
        else:
            q_list = item_quality
            
        # 检查是否有交集
        has_match = any(q in q_list for q in allowed_qualities)
        if not has_match:
            return False

    # 3. 大小过滤 (GB)
    if media_type == 'tv':
        # 如果配置了 tv_min_size，优先使用，否则回退到旧的 min_size (兼容旧配置)
        min_size = float(filters.get('tv_min_size') or filters.get('min_size') or 0)
        max_size = float(filters.get('tv_max_size') or filters.get('max_size') or 0)
    else:
        # 默认为电影
        min_size = float(filters.get('movie_min_size') or filters.get('min_size') or 0)
        max_size = float(filters.get('movie_max_size') or filters.get('max_size') or 0)
    
    if min_size > 0 or max_size > 0:
        size_gb = _parse_size_to_gb(item.get('size'))
        if min_size > 0 and size_gb < min_size:
            return False
        if max_size > 0 and size_gb > max_size:
            return False

    # 4. 中字过滤
    if filters.get('require_zh'):
        # 1. 优先看 API 返回的硬指标 (zh_sub: 1)
        if item.get('is_zh_sub'):
            return True
            
        # 2. API 没标记，尝试从标题猜测
        title = item.get('title', '').upper()
        
        # 常见的中字/国语标识
        zh_keywords = [
            '中字', '中英', '字幕', 
            'CHS', 'CHT', 'CN', 
            'DIY', '国语', '国粤'
        ]
        
        # 只要包含任意一个关键词即可
        is_zh_guess = any(k in title for k in zh_keywords)
        
        if not is_zh_guess:
            return False

    # 5. 封装容器过滤 (后缀名)
    allowed_containers = filters.get('containers', [])
    if allowed_containers:
        # ★★★ 核心修复：如果是剧集 (TV)，通常是目录或合集，无法从标题判断容器，直接放行 ★★★
        # 否则会导致文件夹形式的资源被误杀
        if media_type == 'tv':
            return True

        title = item.get('title', '').lower()
        # 检查标题结尾或链接结尾
        link = item.get('link', '').lower()
        
        # 提取扩展名逻辑简单版
        ext = None
        if 'mkv' in title or link.endswith('.mkv'): ext = 'mkv'
        elif 'mp4' in title or link.endswith('.mp4'): ext = 'mp4'
        elif 'iso' in title or link.endswith('.iso'): ext = 'iso'
        elif 'ts' in title or link.endswith('.ts'): ext = 'ts'
        
        if not ext or ext not in allowed_containers:
            return False

    return True

def _check_and_update_rate_limit():
    """
    检查 API 调用限制：
    1. 每日限额检查
    2. 请求间隔强制睡眠
    """
    with _rate_limit_lock:
        config = get_config()
        # 获取配置，默认限制 100 次，间隔 5 秒
        daily_limit = int(config.get('daily_limit', 100))
        interval = float(config.get('request_interval', 5.0))
        
        # 获取统计数据
        stats = settings_db.get_setting('nullbr_usage_stats') or {}
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 检查日期，如果是新的一天则重置
        if stats.get('date') != today_str:
            stats = {
                'date': today_str,
                'count': 0,
                'last_request_ts': 0
            }
        
        # 2. 检查每日限额
        current_count = stats.get('count', 0)
        if current_count >= daily_limit:
            logger.warning(f"NULLBR API 今日调用次数已达上限 ({current_count}/{daily_limit})")
            raise Exception(f"今日 API 调用次数已达上限 ({daily_limit}次)，请明日再试或增加配额。")
            
        # 3. 检查请求间隔 (强制睡眠)
        last_ts = stats.get('last_request_ts', 0)
        now_ts = time.time()
        elapsed = now_ts - last_ts
        
        if elapsed < interval:
            sleep_time = interval - elapsed
            logger.info(f"  ⏳ 触发流控，强制等待 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)
            
        # 4. 更新统计
        stats['count'] = current_count + 1
        stats['last_request_ts'] = time.time()
        settings_db.save_setting('nullbr_usage_stats', stats)
        
        logger.debug(f"NULLBR API 调用统计: {stats['count']}/{daily_limit}")

def _enrich_items_with_status(items):
    """
    批量查询本地数据库，为 NULLBR 的结果注入 in_library 和 subscription_status 状态
    """
    if not items:
        return items

    # 1. 提取 ID 列表
    # NULLBR 返回的 ID 可能是 'id' 或 'tmdbid'
    tmdb_ids = []
    for item in items:
        tid = item.get('tmdbid') or item.get('id')
        if tid:
            tmdb_ids.append(str(tid))
    
    if not tmdb_ids:
        return items

    # 2. 批量查询数据库
    # 假设大部分是电影，混合查询比较麻烦，这里简单处理：
    # 分别查 Movie 和 Series，或者根据 item 自身的 media_type 判断
    # 为了效率，我们一次性查出来，在内存里匹配
    
    # 获取所有相关 ID 的库内状态 (Movie 和 Series 都查)
    library_map_movie = media_db.check_tmdb_ids_in_library(tmdb_ids, 'Movie')
    library_map_series = media_db.check_tmdb_ids_in_library(tmdb_ids, 'Series')
    
    # 获取订阅状态
    sub_status_movie = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, 'Movie')
    sub_status_series = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, 'Series')

    # 3. 注入状态
    for item in items:
        tid = str(item.get('tmdbid') or item.get('id') or '')
        mtype = item.get('media_type', 'movie') # 默认为 movie
        
        if not tid:
            continue

        in_lib = False
        sub_stat = None

        if mtype == 'tv':
            if f"{tid}_Series" in library_map_series:
                in_lib = True
            sub_stat = sub_status_series.get(tid)
        else:
            if f"{tid}_Movie" in library_map_movie:
                in_lib = True
            sub_stat = sub_status_movie.get(tid)
            
        item['in_library'] = in_lib
        item['subscription_status'] = sub_stat

    return items

def get_preset_lists():
    """获取片单列表"""
    custom_presets = settings_db.get_setting('nullbr_presets')
    if custom_presets and isinstance(custom_presets, list) and len(custom_presets) > 0:
        return custom_presets
    return utils.DEFAULT_NULLBR_PRESETS

def fetch_list_items(list_id, page=1):
    """获取指定片单的详细内容"""
    url = f"{NULLBR_API_BASE}/list/{list_id}"
    params = {"page": page}
    try:
        logger.info(f"  ➜ 正在获取片单列表: {list_id} (Page {page})")
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15)
        response.raise_for_status()
        data = response.json()
        items = data.get('items', [])
        enriched_items = _enrich_items_with_status(items)
        return {"code": 200, "data": {"list": enriched_items, "total": data.get('total_results', 0)}}
    except Exception as e:
        logger.error(f"  ➜ 获取片单失败: {e}")
        raise e

def search_media(keyword, page=1):
    """搜索资源 """
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

def _fetch_single_source(tmdb_id, media_type, source_type, season_number=None):
    # ★ 插入流控检查
    # 注意：获取一个电影的资源可能会调用 2-3 次这个函数，意味着会触发 2-3 次间隔等待
    # 这是为了安全起见必须的
    try:
        _check_and_update_rate_limit()
    except Exception as e:
        # 如果是获取资源详情时超限，记录日志并返回空列表，不中断整个流程（尽量返回已获取的）
        logger.warning(f"  ⚠️ {e}")
        return []

    if media_type == 'movie':
        url = f"{NULLBR_API_BASE}/movie/{tmdb_id}/{source_type}"
    elif media_type == 'tv':
        # ★★★ 剧集 URL 构造逻辑优化 ★★★
        if season_number:
            # 如果有季号，直接请求单季接口
            # 例如: /tv/12345/season/2/115 或 /tv/12345/season/2/magnet
            url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/{season_number}/{source_type}"
        else:
            # 如果没有季号 (比如搜整部剧)，保持原有逻辑
            if source_type == '115':
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/115"
            elif source_type == 'magnet':
                # 旧逻辑默认只搜第一季，或者你可以改成搜整剧(如果API支持)
                # 这里为了稳妥，如果没有季号，还是默认 S1，或者你可以根据需求调整
                url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/1/magnet"
            else:
                return []
    else:
        return []

    try:
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, headers=_get_headers(), timeout=10, proxies=proxies)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        data = response.json()
        raw_list = data.get(source_type, [])
        
        cleaned_list = []
        for item in raw_list:
            link = item.get('share_link') or item.get('magnet') or item.get('ed2k')
            title = item.get('title') or item.get('name')
            if link and title:
                if media_type == 'tv' and source_type == 'magnet':
                    title = f"[S1] {title}"
                
                is_zh = item.get('zh_sub') == 1
                if not is_zh:
                    t_upper = title.upper()
                    zh_keywords = ['中字', '中英', '字幕', 'CHS', 'CHT', 'CN', 'DIY', '国语', '国粤']
                    if any(k in t_upper for k in zh_keywords):
                        is_zh = True
                
                resource_obj = {
                    "title": title,
                    "size": item.get('size', '未知'),
                    "resolution": item.get('resolution'),
                    "quality": item.get('quality'),
                    "link": link,
                    "source_type": source_type.upper(),
                    "is_zh_sub": is_zh
                }
                cleaned_list.append(resource_obj)
        return cleaned_list
    except Exception as e:
        logger.warning(f"  ➜ 获取 {source_type} 资源失败: {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie', specific_source=None, season_number=None):
    config = get_config()
    
    # ★ 修改点：确定要获取的源
    if specific_source:
        # 如果指定了源 (如 '115')，只请求这一个
        enabled_sources = [specific_source]
    else:
        # 否则获取所有启用的源 (兼容旧逻辑)
        enabled_sources = config.get('enabled_sources', ['115', 'magnet', 'ed2k'])
    
    all_resources = []
    
    # 1. 获取 115 资源 (消耗 1 次配额)
    if '115' in enabled_sources:
        try:
            res_115 = _fetch_single_source(tmdb_id, media_type, '115', season_number)
            all_resources.extend(res_115)
        except Exception:
            pass # 单个源失败不影响其他

    # 2. 获取 Magnet 资源 (消耗 1 次配额)
    if 'magnet' in enabled_sources:
        try:
            res_mag = _fetch_single_source(tmdb_id, media_type, 'magnet', season_number)
            all_resources.extend(res_mag)
        except Exception:
            pass

    # 3. 获取 Ed2k 资源 (仅电影, 消耗 1 次配额)
    if media_type == 'movie' and 'ed2k' in enabled_sources:
        try:
            res_ed2k = _fetch_single_source(tmdb_id, media_type, 'ed2k')
            all_resources.extend(res_ed2k)
        except Exception:
            pass
    
    # 4. 获取过滤配置
    config = get_config()
    filters = config.get('filters', {})
    
    # 5. 执行过滤
    # 如果 filters 全为空值，则不过滤
    has_filter = any(filters.values())
    if not has_filter:
        return all_resources
        
    filtered_list = [res for res in all_resources if _is_resource_valid(res, filters, media_type)]
    
    logger.info(f"  ➜ 资源过滤: 原始 {len(all_resources)} -> 过滤后 {len(filtered_list)}")
    return filtered_list

# ==============================================================================
# ★★★ CMS 推送逻辑 (Token 版) ★★★
# ==============================================================================

def _clean_link(link):
    """
    清洗链接：去除首尾空格，并安全去除末尾的 HTML 脏字符 (&#)
    """
    if not link:
        return ""
    link = link.strip()
    
    # 循环去除结尾的特殊字符，直到干净为止
    # 这样可以把 password=1234&# 变成 password=1234
    while link.endswith('&#') or link.endswith('&') or link.endswith('#'):
        if link.endswith('&#'):
            link = link[:-2]
        elif link.endswith('&') or link.endswith('#'):
            link = link[:-1]
            
    return link

def push_to_cms(resource_link, title):
    """
    推送到 CMS (使用 Token 接口)
    """
    config = get_config()
    cms_url = config.get('cms_url')
    cms_token = config.get('cms_token')

    if not cms_url or not cms_token:
        raise ValueError("未配置 CMS 地址或 Token")

    # ★★★ 核心修复：在此处统一清洗链接 ★★★
    clean_url = _clean_link(resource_link)
    
    cms_url = cms_url.rstrip('/')
    api_url = f"{cms_url}/api/cloud/add_share_down_by_token"
    
    payload = {
        "url": clean_url,
        "token": cms_token
    }

    try:
        logger.info(f"  ➜ 正在推送任务到 CMS: {title}")
        # CMS 通常在内网，不走代理
        response = requests.post(api_url, json=payload, timeout=10)
        response.raise_for_status()
        
        res_json = response.json()
        if res_json.get('code') == 200:
            logger.info(f"  ✅ CMS 推送成功: {res_json.get('msg', 'OK')}")
            return True
        else:
            raise Exception(f"CMS 返回错误: {res_json}")

    except Exception as e:
        logger.error(f"  ➜ CMS 推送异常: {e}")
        raise e

def _format_size(size):
    """辅助函数：格式化字节大小"""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size/1024:.2f} KB"
    elif size < 1024**3:
        return f"{size/1024**2:.2f} MB"
    elif size < 1024**4:
        return f"{size/1024**3:.2f} GB"
    else:
        return f"{size/1024**4:.2f} TB"

def push_to_115(resource_link, title):
    """
    智能推送：支持 115/115cdn/anxia 转存 和 磁力离线 (修复变量作用域报错)
    """
    if P115Client is None:
        raise ImportError("未安装 p115 库")

    config = get_config()
    cookies = config.get('p115_cookies')
    
    try:
        cid_val = config.get('p115_save_path_cid', 0)
        save_path_cid = int(cid_val) if cid_val else 0
    except:
        save_path_cid = 0

    if not cookies:
        raise ValueError("未配置 115 Cookies")

    clean_url = _clean_link(resource_link)
    logger.info(f"  ➜ [DEBUG] 待处理链接: {clean_url}")
    
    client = P115Client(cookies)
    
    try:
        # 支持 115.com, 115cdn.com, anxia.com
        target_domains = ['115.com', '115cdn.com', 'anxia.com']
        is_115_share = any(d in clean_url for d in target_domains) and ('magnet' not in clean_url)
        
        if is_115_share:
            logger.info(f"  ➜ [模式] 识别为 115 转存任务 -> CID: {save_path_cid}")
            
            # 1. 提取 share_code
            share_code = None
            match = re.search(r'/s/([a-z0-9]+)', clean_url)
            if match:
                share_code = match.group(1)
            
            if not share_code:
                raise Exception("无法从链接中提取分享码 (share_code)")
            
            # 2. 提取 receive_code (密码)
            receive_code = ''
            pwd_match = re.search(r'password=([a-z0-9]+)', clean_url)
            if pwd_match:
                receive_code = pwd_match.group(1)
            
            logger.info(f"  ➜ [参数] ShareCode: {share_code}, Pwd: {receive_code}")
            
            # 3. 调用转存
            # ★★★ 修复点：初始化 resp，防止报错 ★★★
            resp = {} 
            
            try:
                if hasattr(client, 'fs_share_import_to_dir'):
                     resp = client.fs_share_import_to_dir(share_code, receive_code, save_path_cid)
                elif hasattr(client, 'fs_share_import'):
                    resp = client.fs_share_import(share_code, receive_code, save_path_cid)
                elif hasattr(client, 'share_import'):
                    resp = client.share_import(share_code, receive_code, save_path_cid)
                else:
                    # 底层构造请求
                    api_url = "https://webapi.115.com/share/receive"
                    payload = {
                        'share_code': share_code,
                        'receive_code': receive_code,
                        'cid': save_path_cid
                    }
                    # 直接获取响应对象
                    r = client.request(api_url, method='POST', data=payload)
                    # 兼容处理：如果是 Response 对象则转 json，如果是 dict 则直接用
                    if hasattr(r, 'json'):
                        resp = r.json()
                    else:
                        resp = r
                        
            except Exception as e:
                raise Exception(f"调用转存接口失败: {e}")

            # ★★★ 修复点：将判断逻辑放在 try 块内部，确保 resp 已定义 ★★★
            if resp and resp.get('state'):
                logger.info(f"  ✅ 115 转存成功: {title}")
                return True
            else:
                err = resp.get('error_msg') if resp else '无响应'
                err = err or resp.get('msg') or str(resp)
                raise Exception(f"转存失败: {err}")

        else:
            # 磁力/Ed2k 离线下载
            logger.info(f"  ➜ [模式] 识别为磁力/离线任务 -> CID: {save_path_cid}")
            
            # 构造 payload 字典
            payload = {
                'url[0]': clean_url,
                'wp_path_id': save_path_cid
            }
            
            resp = client.offline_add_urls(payload)
            
            if resp.get('state'):
                logger.info(f"  ✅ 115 离线添加成功: {title}")
                return True
            else:
                err = resp.get('error_msg') or resp.get('msg') or '未知错误'
                if '已存在' in str(err):
                    logger.info(f"  ✅ 任务已存在: {title}")
                    return True
                raise Exception(f"离线失败: {err}")

    except Exception as e:
        logger.error(f"  ➜ 115 推送异常: {e}")
        if "Login" in str(e) or "cookie" in str(e).lower():
            raise Exception("115 Cookie 无效")
        raise e

def get_115_account_info():
    """
    极简状态检查：只验证 Cookie 是否有效，不获取任何详情
    """
    if P115Client is None:
        raise Exception("未安装 p115client")
        
    config = get_config()
    cookies = config.get('p115_cookies')
    
    if not cookies:
        raise Exception("未配置 Cookies")
        
    try:
        client = P115Client(cookies)
        
        # 尝试列出 1 个文件，这是验证 Cookie 最快最准的方法
        resp = client.fs_files({'limit': 1})
        
        if not resp.get('state'):
            raise Exception("Cookie 已失效")
            
        # 只要没报错，就是有效
        return {
            "valid": True,
            "msg": "Cookie 状态正常，可正常推送"
        }

    except Exception as e:
        # logger.error(f"115 状态检查失败: {e}") # 嫌烦可以注释掉日志
        raise Exception("Cookie 无效或网络不通")

def handle_push_request(link, title):
    """
    统一推送入口，根据配置决定去向
    """
    config = get_config()
    mode = config.get('push_mode', 'cms') # 默认 cms, 可选 '115'
    
    if mode == '115':
        return push_to_115(link, title)
    else:
        return push_to_cms(link, title)

def auto_download_best_resource(tmdb_id, media_type, title):
    """
    [自动任务专用] 搜索并下载最佳资源
    1. 获取资源列表 (已应用过滤器)
    2. 取第一个资源
    3. 推送到 CMS 或 115
    """
    try:
        config = get_config()
        if not config.get('api_key'):
            logger.warning("NULLBR 未配置 API Key，无法执行自动兜底。")
            return False

        # ★ 修改点：按优先级循环，命中即停
        priority_sources = ['115', 'magnet', 'ed2k']
        user_enabled = config.get('enabled_sources', priority_sources)
        
        logger.info(f"  ➜ [自动任务] 开始搜索资源: {title} (ID: {tmdb_id})")

        for source in priority_sources:
            # 如果用户没启用该源，跳过
            if source not in user_enabled: continue
            # 剧集跳过 ed2k
            if media_type == 'tv' and source == 'ed2k': continue

            # 只请求当前这一个源
            resources = fetch_resource_list(tmdb_id, media_type, specific_source=source)
            
            if resources:
                best_resource = resources[0]
                logger.info(f"  ✅ 命中资源 [{source.upper()}]: {best_resource['title']}")
                # 找到后立即推送并返回，不再请求后面的源
                handle_push_request(best_resource['link'], title)
                return True
            
        logger.info(f"  ❌ 所有源均未找到符合过滤条件的资源: {title}")
        return False

    except Exception as e:
        logger.error(f"  ➜ NULLBR 自动兜底失败: {e}")
        return False