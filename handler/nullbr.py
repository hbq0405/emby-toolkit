# handler/nullbr.py
import logging
import requests
import re
from database import settings_db
import config_manager

import constants
import utils
logger = logging.getLogger(__name__)

# ★★★ 硬编码配置：Nullbr ★★★
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.online"

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

def _is_resource_valid(item, filters):
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
    min_size = float(filters.get('min_size') or 0)
    max_size = float(filters.get('max_size') or 0)
    
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
        logger.info(f"正在获取片单列表: {list_id} (Page {page})")
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15)
        response.raise_for_status()
        data = response.json()
        return {"code": 200, "data": {"list": data.get('items', []), "total": data.get('total_results', 0)}}
    except Exception as e:
        logger.error(f"获取片单失败: {e}")
        raise e

def search_media(keyword, page=1):
    url = f"{NULLBR_API_BASE}/search"
    params = { "query": keyword, "page": page }
    try:
        # 搜索走代理（如果是外网）
        proxies = config_manager.get_proxies_for_requests()
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        return { "code": 200, "data": { "list": data.get('items', []), "total": data.get('total_results', 0) } }
    except Exception as e:
        logger.error(f"NULLBR 搜索失败: {e}")
        raise e

def _fetch_single_source(tmdb_id, media_type, source_type):
    if media_type == 'movie':
        url = f"{NULLBR_API_BASE}/movie/{tmdb_id}/{source_type}"
    elif media_type == 'tv':
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
                
                # 构造对象
                resource_obj = {
                    "title": title,
                    "size": item.get('size', '未知'),
                    "resolution": item.get('resolution'),
                    "quality": item.get('quality'),
                    "link": link,
                    "source_type": source_type.upper(),
                    "is_zh_sub": item.get('zh_sub') == 1
                }
                cleaned_list.append(resource_obj)
        return cleaned_list
    except Exception as e:
        logger.warning(f"获取 {source_type} 资源失败: {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie'):
    # 1. 获取所有资源
    all_resources = []
    all_resources.extend(_fetch_single_source(tmdb_id, media_type, '115'))
    all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'magnet'))
    if media_type == 'movie':
        all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'ed2k'))
    
    # 2. 获取过滤配置
    config = get_config()
    filters = config.get('filters', {})
    
    # 3. 执行过滤
    # 如果 filters 全为空值，则不过滤
    has_filter = any(filters.values())
    if not has_filter:
        return all_resources
        
    filtered_list = [res for res in all_resources if _is_resource_valid(res, filters)]
    
    logger.info(f"资源过滤: 原始 {len(all_resources)} -> 过滤后 {len(filtered_list)}")
    return filtered_list

# ==============================================================================
# ★★★ CMS 推送逻辑 (Token 版) ★★★
# ==============================================================================

def push_to_cms(resource_link, title):
    """
    推送到 CMS  (使用 Token 接口)
    """
    config = get_config()
    cms_url = config.get('cms_url')
    cms_token = config.get('cms_token')

    if not cms_url or not cms_token:
        raise ValueError("未配置 CMS 地址或 Token，请在配置页填写")

    # 去除 URL 末尾可能的斜杠
    cms_url = cms_url.rstrip('/')
    
    # 构造接口地址
    api_url = f"{cms_url}/api/cloud/add_share_down_by_token"
    
    # 构造 Payload
    payload = {
        "url": resource_link,
        "token": cms_token
    }

    try:
        logger.info(f"正在推送任务到 CMS: {api_url}")
        
        # ★ 注意：CMS 通常在局域网，一般不需要走代理。
        # 如果你的 CMS 在外网且必须走代理，请取消下面 proxies 的注释
        # proxies = config_manager.get_proxies_for_requests()
        
        response = requests.post(api_url, json=payload, timeout=10) # , proxies=proxies)
        response.raise_for_status()
        
        res_json = response.json()
        
        # 根据截图，成功通常返回 code 200
        if res_json.get('code') == 200:
            logger.info(f"CMS 推送成功: {res_json.get('msg', 'OK')}")
            return True
        else:
            raise Exception(f"CMS 返回错误: {res_json}")

    except Exception as e:
        logger.error(f"CMS 推送异常: {e}")
        raise e