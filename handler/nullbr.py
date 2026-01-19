# handler/nullbr.py
import logging
import requests
import json
from database import settings_db

import constants
import utils
logger = logging.getLogger(__name__)

# ★★★ 硬编码配置 ★★★
NULLBR_APP_ID = "7DqRtfNX3"
NULLBR_API_BASE = "https://api.nullbr.online" # 假设的基础URL，根据文档调整

def get_preset_lists():
    """
    获取片单列表：优先从数据库读取，没有则使用默认值
    """
    # 尝试从数据库读取 'nullbr_presets'
    custom_presets = settings_db.get_setting('nullbr_presets')
    
    if custom_presets and isinstance(custom_presets, list) and len(custom_presets) > 0:
        return custom_presets
    
    # 如果数据库没配置，返回默认值
    return utils.DEFAULT_NULLBR_PRESETS

def fetch_list_items(list_id, page=1):
    """
    【新增】获取指定片单的详细内容
    API: GET /list/{listid}?page=x
    """
    url = f"{NULLBR_API_BASE}/list/{list_id}"
    params = {"page": page}
    
    try:
        logger.info(f"正在获取片单列表: {list_id} (Page {page})")
        # 片单接口通常只需要 AppID，不需要 User Key，但带上也没事
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        # 解析返回结构 (假设结构类似搜索结果)
        # 通常包含 items 数组
        items = data.get('items', [])
        total = data.get('total_results', 0) # 或者 total_items
        
        return {
            "code": 200,
            "data": {
                "list": items,
                "total": total
            }
        }
    except Exception as e:
        logger.error(f"获取片单失败: {e}")
        raise e

def get_api_key():
    """从数据库获取用户配置的 API Key"""
    config = settings_db.get_setting('nullbr_config') or {}
    return config.get('api_key', '')

def get_config():
    return settings_db.get_setting('nullbr_config') or {}

def _get_headers():
    """
    【修正】构造请求头
    文档要求: X-APP-ID 和 X-API-KEY
    """
    config = get_config()
    api_key = config.get('api_key')
    
    headers = {
        "Content-Type": "application/json",
        "X-APP-ID": NULLBR_APP_ID,
        "User-Agent": f"EmbyToolkit/{constants.APP_VERSION} (Private NAS Tool)"
    }
    
    # ★★★ 修正：使用 X-API-KEY ★★★
    if api_key:
        headers["X-API-KEY"] = api_key
        
    return headers

def search_media(keyword, page=1):
    """
    搜索接口 (保持不变，之前已经修好了)
    """
    url = f"{NULLBR_API_BASE}/search"
    params = { "query": keyword, "page": page }
    
    try:
        logger.info(f"正在请求 NULLBR 搜索: {keyword}")
        response = requests.get(url, params=params, headers=_get_headers(), timeout=15)
        response.raise_for_status()
        data = response.json()
        
        items = data.get('items', [])
        total = data.get('total_results', 0)
        
        return { "code": 200, "data": { "list": items, "total": total } }
    except Exception as e:
        logger.error(f"NULLBR 搜索失败: {e}")
        if 'response' in locals(): logger.error(f"错误响应: {response.text}")
        raise e

def _fetch_single_source(tmdb_id, media_type, source_type):
    """
    【智能适配】获取单一类型的资源
    source_type: '115', 'magnet', 'ed2k'
    """
    # 1. 构造 URL
    if media_type == 'movie':
        # 电影很简单: /movie/{id}/{source}
        url = f"{NULLBR_API_BASE}/movie/{tmdb_id}/{source_type}"
    
    elif media_type == 'tv':
        # 剧集比较复杂，分情况处理
        if source_type == '115':
            # 115 在剧集层级就有: /tv/{id}/115 (完美!)
            url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/115"
        
        elif source_type == 'magnet':
            # 磁力在季层级: /tv/{id}/season/{s}/magnet
            # ★ 策略：默认只抓取 "第1季" 的磁力，作为备选
            # 如果你想抓所有季，这里得写循环，太慢了，先抓 S1 够用了
            url = f"{NULLBR_API_BASE}/tv/{tmdb_id}/season/1/magnet"
            
        else:
            # Ed2k 在剧集层级没有，直接跳过，不浪费请求
            return []
    else:
        return []

    try:
        # logger.debug(f"正在获取 {media_type} / {source_type} 资源: {url}")
        response = requests.get(url, headers=_get_headers(), timeout=10)
        
        # 404 说明没资源，正常返回空
        if response.status_code == 404:
            return []
            
        response.raise_for_status()
        data = response.json()
        
        # 2. 提取数据
        # 115 返回 key 是 "115", magnet 是 "magnet"
        raw_list = data.get(source_type, [])
        
        # 3. 数据清洗
        cleaned_list = []
        for item in raw_list:
            # 统一链接字段
            link = item.get('share_link') or item.get('magnet') or item.get('ed2k')
            # 统一标题字段
            title = item.get('title') or item.get('name')
            
            if link and title:
                # 剧集特殊处理：给磁力链标题加个 (S1) 标记，免得误会
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
        # 剧集经常出现某一季没资源的情况，记录个 warning 就行，别报错
        logger.warning(f"获取 {source_type} 资源失败 ({url}): {e}")
        return []

def fetch_resource_list(tmdb_id, media_type='movie'):
    """
    【三合一】聚合获取
    """
    all_resources = []
    
    # 1. 获取 115 (电影和剧集都支持)
    res_115 = _fetch_single_source(tmdb_id, media_type, '115')
    all_resources.extend(res_115)
    
    # 2. 获取 Magnet (电影全拿，剧集只拿 S1)
    res_magnet = _fetch_single_source(tmdb_id, media_type, 'magnet')
    all_resources.extend(res_magnet)
    
    # 3. 获取 ED2K (仅电影)
    if media_type == 'movie':
        res_ed2k = _fetch_single_source(tmdb_id, media_type, 'ed2k')
        all_resources.extend(res_ed2k)
    
    logger.info(f"资源聚合完成 ID:{tmdb_id} Type:{media_type} -> 找到 {len(all_resources)} 个")
    
    return all_resources

def get_tg_config():
    """获取 TG 配置"""
    config = settings_db.get_setting('nullbr_config') or {}
    return config.get('tg_bot_token'), config.get('tg_chat_id')

def push_to_telegram(resource_link, title):
    """
    将资源链接推送到指定的 TG 机器人/频道
    """
    token, chat_id = get_tg_config()
    
    if not token or not chat_id:
        raise ValueError("未配置 Telegram Bot Token 或 Chat ID")

    # 构造消息内容
    # 既然是给网盘工具自动识别，最好把链接单独放一行，或者只发链接
    # 这里我们发一个带标题的格式，通常工具都能正则提取
    message_text = f"📥 **资源入库请求**\n\n🎬 名称：{title}\n🔗 链接：\n`{resource_link}`"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message_text,
        "parse_mode": "Markdown"
    }

    try:
        # 设置超时，防止 TG 网络不通卡住
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"TG 推送失败: {e}")
        # 尝试解析更详细的错误信息
        error_msg = str(e)
        if response and response.text:
            error_msg += f" | TG Response: {response.text}"
        raise Exception(f"推送失败: {error_msg}")

def validate_key(api_key):
    """测试 API Key 是否有效 (通常调用用户信息接口)"""
    # 对应截图: 08. 获取人物信息 person 或者简单的 ping
    # 这里先简单返回 True，后续对接真实 API
    if not api_key: return False
    return True