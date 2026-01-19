# handler/nullbr.py
import logging
import requests
import json
from database import settings_db
import config_manager

import constants
import utils
logger = logging.getLogger(__name__)

# ★★★ 硬编码配置：Nullbr ★★★
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.online"

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
                cleaned_list.append({
                    "title": title,
                    "size": item.get('size', '未知'),
                    "resolution": item.get('resolution'),
                    "quality": item.get('quality'),
                    "link": link,
                    "source_type": source_type.upper()
                })
        return cleaned_list
    except Exception as e:
        logger.warning(f"获取 {source_type} 资源失败: {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie'):
    all_resources = []
    all_resources.extend(_fetch_single_source(tmdb_id, media_type, '115'))
    all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'magnet'))
    if media_type == 'movie':
        all_resources.extend(_fetch_single_source(tmdb_id, media_type, 'ed2k'))
    return all_resources

# ==============================================================================
# ★★★ CMS 推送逻辑 (Token 版) ★★★
# ==============================================================================

def push_to_cms(resource_link, title):
    """
    推送到 CMS 下载器 (Token 版 - 增强兼容性)
    """
    config = get_config()
    cms_url = config.get('cms_url')
    cms_token = config.get('cms_token')

    if not cms_url or not cms_token:
        raise ValueError("未配置 CMS 地址或 Token")

    # 去除 URL 末尾斜杠
    base_url = cms_url.rstrip('/')
    
    # ★★★ 修改点：同时在 URL 参数和 Body 中携带 Token ★★★
    # 这样无论接口是取 query param 还是 json body 都能拿到
    api_url = f"{base_url}/api/cloud/add_share_down_by_token"
    
    # URL 参数
    params = {
        "token": cms_token
    }
    
    # Body 参数
    payload = {
        "url": resource_link,
        "token": cms_token
    }

    try:
        # 打印调试日志，方便看有没有空格
        logger.info(f"正在推送 CMS: {api_url}")
        logger.info(f"Token长度: {len(cms_token)} | Token内容: [{cms_token}]") 

        # 发送请求
        response = requests.post(
            api_url, 
            params=params, # URL 参数
            json=payload,  # Body 参数
            timeout=10
        )
        
        # 打印原始响应，方便调试
        logger.info(f"CMS 响应状态码: {response.status_code}")
        # logger.info(f"CMS 响应内容: {response.text}")

        response.raise_for_status()
        res_json = response.json()
        
        if res_json.get('code') == 200:
            logger.info(f"CMS 推送成功: {res_json.get('msg', 'OK')}")
            return True
        else:
            # 抛出详细错误
            raise Exception(f"CMS 拒绝: {res_json.get('message')} (Code: {res_json.get('code')})")

    except Exception as e:
        logger.error(f"CMS 推送异常: {e}")
        raise e