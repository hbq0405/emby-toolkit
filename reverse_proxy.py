# reverse_proxy.py (最终完美版 V4 - 诊断增强版 - 无水印版)

import logging
import requests
import re
import json
from flask import Flask, request, Response
from urllib.parse import urlparse, urlunparse
import time
import uuid 
from datetime import datetime, timezone
from gevent import spawn
from geventwebsocket.websocket import WebSocket
from websocket import create_connection
from database import collection_db
from database import user_db
from custom_collection_handler import FilterEngine
import config_manager

import extensions
import emby_handler
logger = logging.getLogger(__name__)

# --- 【核心修改】---
# 不再使用字符串前缀，而是定义一个数字转换基数
# 这将把数据库ID (例如 7) 转换为一个唯一的、负数的、看起来像原生ID的数字 (例如 -900007)
MIMICKED_ID_BASE = 900000

MIMICKED_ID_BASE = 900000
def to_mimicked_id(db_id): return str(-(MIMICKED_ID_BASE + db_id))
def from_mimicked_id(mimicked_id): return -(int(mimicked_id)) - MIMICKED_ID_BASE
def is_mimicked_id(item_id):
    try: return isinstance(item_id, str) and item_id.startswith('-')
    except: return False
MIMICKED_ITEMS_RE = re.compile(r'/emby/Users/([^/]+)/Items/(-(\d+))')
MIMICKED_ITEM_DETAILS_RE = re.compile(r'emby/Users/([^/]+)/Items/(-(\d+))$')
def _get_real_emby_url_and_key():
    base_url = config_manager.APP_CONFIG.get("emby_server_url", "").rstrip('/')
    api_key = config_manager.APP_CONFIG.get("emby_api_key", "")
    if not base_url or not api_key: raise ValueError("Emby服务器地址或API Key未配置")
    return base_url, api_key

def handle_get_views():
    """
    【V12 - 极速裸奔最终版】
    - 移除所有动态库的空壳检查，将主页加载速度置于最高优先级。
    - 可见性现在只由两个核心条件决定：1. 库在Emby中真实存在。 2. 用户拥有访问权限。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        return "Proxy is not ready", 503

    try:
        user_id_match = re.search(r'/emby/Users/([^/]+)/Views', request.path)
        if not user_id_match:
            return "Could not determine user from request path", 400
        user_id = user_id_match.group(1)

        user_visible_native_libs = emby_handler.get_emby_libraries(
            config_manager.APP_CONFIG.get("emby_server_url", ""),
            config_manager.APP_CONFIG.get("emby_api_key", ""),
            user_id
        )
        if user_visible_native_libs is None: user_visible_native_libs = []

        collections = collection_db.get_all_active_custom_collections()
        fake_views_items = []
        for coll in collections:
            # 1. 物理检查：库在Emby里有实体吗？
            real_emby_collection_id = coll.get('emby_collection_id')
            if not real_emby_collection_id:
                logger.debug(f"  ➜ 虚拟库 '{coll['name']}' 被隐藏，原因: 无对应Emby实体")
                continue

            # 2. 权限检查：用户在不在邀请函上？
            allowed_users = coll.get('allowed_user_ids')
            if allowed_users and isinstance(allowed_users, list):
                if user_id not in allowed_users:
                    logger.debug(f"  ➜ 虚拟库 '{coll['name']}' 被隐藏，原因: 用户不在可见列表中 (权限)。")
                    continue
            
            # --- 所有检查通过，直接生成虚拟库 ---
            db_id = coll['id']
            mimicked_id = to_mimicked_id(db_id)
            image_tags = {"Primary": f"{real_emby_collection_id}?timestamp={int(time.time())}"}
            definition = coll.get('definition_json') or {}
            
            merged_libraries = definition.get('merged_libraries', [])
            name_suffix = f" (合并库: {len(merged_libraries)}个)" if merged_libraries else ""
            
            item_type_from_db = definition.get('item_type', 'Movie')
            collection_type = "mixed"
            if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
                 authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
                 collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

            fake_view = {
                "Name": coll['name'] + name_suffix, "ServerId": real_server_id, "Id": mimicked_id,
                "Guid": str(uuid.uuid4()), "Etag": f"{db_id}{int(time.time())}",
                "DateCreated": "2025-01-01T00:00:00.0000000Z", "CanDelete": False, "CanDownload": False,
                "SortName": coll['name'], "ExternalUrls": [], "ProviderIds": {}, "IsFolder": True,
                "ParentId": "2", "Type": "CollectionFolder", "PresentationUniqueKey": str(uuid.uuid4()),
                "DisplayPreferencesId": f"custom-{db_id}", "ForcedSortName": coll['name'],
                "Taglines": [], "RemoteTrailers": [],
                "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
                "ChildCount": coll.get('in_library_count', 1), # 给个默认值，避免显示为0
                "PrimaryImageAspectRatio": 1.7777777777777777, 
                "CollectionType": collection_type, "ImageTags": image_tags, "BackdropImageTags": [], 
                "LockedFields": [], "LockData": False
            }
            fake_views_items.append(fake_view)
        
        logger.debug(f"已为用户 {user_id} 生成 {len(fake_views_items)} 个可见的虚拟库。")

        # --- 原生库合并逻辑 (保持不变) ---
        native_views_items = []
        should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
        if should_merge_native:
            all_native_views = user_visible_native_libs
            raw_selection = config_manager.APP_CONFIG.get('proxy_native_view_selection', '')
            selected_native_view_ids = [x.strip() for x in raw_selection.split(',') if x.strip()] if isinstance(raw_selection, str) else raw_selection
            if not selected_native_view_ids:
                native_views_items = all_native_views
            else:
                native_views_items = [view for view in all_native_views if view.get("Id") in selected_native_view_ids]
        
        final_items = []
        native_order = config_manager.APP_CONFIG.get('proxy_native_view_order', 'before')
        if native_order == 'after':
            final_items.extend(fake_views_items)
            final_items.extend(native_views_items)
        else:
            final_items.extend(native_views_items)
            final_items.extend(fake_views_items)

        final_response = {"Items": final_items, "TotalRecordCount": len(final_items)}
        return Response(json.dumps(final_response), mimetype='application/json')
        
    except Exception as e:
        logger.error(f"[PROXY] 获取视图数据时出错: {e}", exc_info=True)
        return "Internal Proxy Error", 500

def handle_get_mimicked_library_details(user_id, mimicked_id):
    """
    【V2 - PG JSON 兼容版】
    - 修复了因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    """
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        coll = collection_db.get_custom_collection_by_id(real_db_id)
        if not coll: return "Not Found", 404

        real_server_id = extensions.EMBY_SERVER_ID
        real_emby_collection_id = coll.get('emby_collection_id')
        image_tags = {"Primary": real_emby_collection_id} if real_emby_collection_id else {}
        
        # ★★★ 核心修复：直接使用已经是字典的 definition_json 字段 ★★★
        definition = coll.get('definition_json') or {}
        item_type_from_db = definition.get('item_type', 'Movie')
        collection_type = "mixed"
        if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
             authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
             collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

        fake_library_details = {
            "Name": coll['name'], "ServerId": real_server_id, "Id": mimicked_id,
            "Type": "CollectionFolder",
            "CollectionType": collection_type, "IsFolder": True, "ImageTags": image_tags,
        }
        return Response(json.dumps(fake_library_details), mimetype='application/json')
    except Exception as e:
        logger.error(f"获取伪造库详情时出错: {e}", exc_info=True)
        return "Internal Server Error", 500

def handle_get_mimicked_library_image(path):
    try:
        tag_with_timestamp = request.args.get('tag') or request.args.get('Tag')
        if not tag_with_timestamp: return "Bad Request", 400
        real_emby_collection_id = tag_with_timestamp.split('?')[0]
        base_url, _ = _get_real_emby_url_and_key()
        image_url = f"{base_url}/Items/{real_emby_collection_id}/Images/Primary"
        headers = {key: value for key, value in request.headers if key.lower() != 'host'}
        headers['Host'] = urlparse(base_url).netloc
        resp = requests.get(image_url, headers=headers, stream=True, params=request.args)
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_headers]
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
    except Exception as e:
        return "Internal Proxy Error", 500

UNSUPPORTED_METADATA_ENDPOINTS = [
        '/Items/Prefixes', # A-Z 首字母索引
        '/Genres',         # 类型筛选
        '/Studios',        # 工作室筛选
        '/Tags',           # 标签筛选
        '/OfficialRatings',# 官方评级筛选
        '/Years'           # 年份筛选
    ]

# --- ★★★ 核心修复 #1：用下面这个通用的“万能翻译”函数，替换掉旧的 a_prefixes 函数 ★★★ ---
def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    【V3 - URL修正版】
    智能处理所有针对虚拟库的元数据类请求。
    """
    # 检查当前请求的路径是否在我们定义的“不支持列表”中
    if any(path.endswith(endpoint) for endpoint in UNSUPPORTED_METADATA_ENDPOINTS):
        logger.trace(f"检测到对虚拟库的不支持的元数据请求 '{path}'，将直接返回空列表以避免后端错误。")
        # 直接返回一个空的JSON数组，客户端会优雅地处理它（不显示相关筛选器）
        return Response(json.dumps([]), mimetype='application/json')

    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info or not collection_info.get('emby_collection_id'):
            return Response(json.dumps([]), mimetype='application/json')

        real_emby_collection_id = collection_info.get('emby_collection_id')
        
        base_url, api_key = _get_real_emby_url_and_key()
        
        # ★★★ 核心修复：在这里加上一个至关重要的斜杠！ ★★★
        target_url = f"{base_url}/{path}"
        
        headers = {k: v for k, v in request.headers if k.lower() not in ['host']}
        headers['Host'] = urlparse(base_url).netloc
        
        new_params = params.copy()
        new_params['ParentId'] = real_emby_collection_id
        new_params['api_key'] = api_key
        
        resp = requests.get(target_url, headers=headers, params=new_params, timeout=15)
        resp.raise_for_status()
        
        return Response(resp.content, resp.status_code, content_type=resp.headers.get('Content-Type'))

    except Exception as e:
        logger.error(f"处理虚拟库元数据请求 '{path}' 时出错: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')
    
def handle_get_mimicked_library_items(user_id, mimicked_id, params):
    """
    【V8.2 - DateLastContentAdded 排序增强版】+ V4.7 全局分块修复
    - 使用 _fetch_items_in_chunks 辅助函数来获取数据，代码更整洁。
    """
    try:
        # ... [获取 final_emby_ids_to_fetch 的逻辑保持不变，这里省略] ...
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')
        definition = collection_info.get('definition_json') or {}
        db_media_list = collection_info.get('generated_media_info_json') or []
        base_ordered_emby_ids = [item.get('emby_id') for item in db_media_list if item.get('emby_id')]
        if not base_ordered_emby_ids:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')
        final_emby_ids_to_fetch = base_ordered_emby_ids
        if definition.get('dynamic_filter_enabled'):
            dynamic_rules = definition.get('dynamic_rules', [])
            ids_from_local_db = user_db.get_item_ids_by_dynamic_rules(user_id, dynamic_rules)
            if ids_from_local_db is not None:
                final_emby_ids_set = set(base_ordered_emby_ids).intersection(set(ids_from_local_db))
                final_emby_ids_to_fetch = [emby_id for emby_id in base_ordered_emby_ids if emby_id in final_emby_ids_set]
            else:
                final_emby_ids_to_fetch = []
        if not final_emby_ids_to_fetch:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        final_items = []
        total_record_count = 0
        sort_by_field = definition.get('default_sort_by')
        base_url, api_key = _get_real_emby_url_and_key()

        if sort_by_field in ['none', 'DateLastContentAdded']:
            # ... [原生排序模式代码保持不变] ...
            logger.trace(f"检测到Emby原生排序模式: '{sort_by_field}'，请求将转发给Emby处理。")
            forward_params = {}
            if sort_by_field == 'DateLastContentAdded':
                sort_order = definition.get('default_sort_order', 'Descending')
                forward_params['SortBy'] = 'DateLastContentAdded'
                forward_params['SortOrder'] = sort_order
            else:
                if 'SortBy' in params: forward_params['SortBy'] = params['SortBy']
                if 'SortOrder' in params: forward_params['SortOrder'] = params['SortOrder']
            passthrough_params_whitelist = ['StartIndex', 'Limit', 'Fields', 'IncludeItemTypes', 'Recursive', 'EnableImageTypes', 'ImageTypeLimit']
            for param in passthrough_params_whitelist:
                if param in params: forward_params[param] = params[param]
            forward_params['Ids'] = ",".join(final_emby_ids_to_fetch)
            forward_params['api_key'] = api_key
            if 'Fields' not in forward_params:
                forward_params['Fields'] = "PrimaryImageAspectRatio,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount"
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            try:
                resp = requests.get(target_url, params=forward_params, timeout=30)
                resp.raise_for_status()
                response_data = resp.json()
                final_items = response_data.get("Items", [])
                total_record_count = response_data.get("TotalRecordCount", len(final_items))
            except Exception as e_pass:
                logger.error(f"在Emby原生排序模式下请求失败: {e_pass}", exc_info=True)
                final_items, total_record_count = [], 0
        else:
            # --- 模式B: 排序劫持 (V4.7 使用辅助函数) ---
            logger.trace(f"执行排序劫持模式: '{sort_by_field}'。调用分块请求辅助函数...")
            
            fields_to_fetch = "PrimaryImageAspectRatio,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount"
            live_items_unordered = _fetch_items_in_chunks(base_url, api_key, user_id, final_emby_ids_to_fetch, fields_to_fetch)
            
            logger.trace(f"排序劫持(分块GET)：成功获取到 {len(live_items_unordered)} 个项目的完整数据。")

            # ... [排序和分页逻辑保持不变] ...
            if sort_by_field == 'original':
                live_items_map = {item['Id']: item for item in live_items_unordered}
                final_items_sorted = [live_items_map[emby_id] for emby_id in final_emby_ids_to_fetch if emby_id in live_items_map]
            else:
                sort_order = definition.get('default_sort_order', 'Ascending')
                is_descending = (sort_order == 'Descending')
                def sort_key_func(item):
                    value = item.get(sort_by_field)
                    if value is None:
                        if sort_by_field in ['CommunityRating', 'ProductionYear']: return 0
                        if sort_by_field in ['PremiereDate', 'DateCreated']: return "1900-01-01T00:00:00.000Z"
                        return ""
                    try:
                        if sort_by_field == 'CommunityRating': return float(value)
                        if sort_by_field == 'ProductionYear': return int(value)
                    except (ValueError, TypeError): return 0
                    return value
                final_items_sorted = sorted(live_items_unordered, key=sort_key_func, reverse=is_descending)
            total_record_count = len(final_items_sorted)
            start_index = int(params.get('StartIndex', 0))
            limit = params.get('Limit')
            if limit:
                final_items = final_items_sorted[start_index : start_index + int(limit)]
            else:
                final_items = final_items_sorted[start_index:]
        
        final_response = {"Items": final_items, "TotalRecordCount": total_record_count}
        return Response(json.dumps(final_response), mimetype='application/json')

    except Exception as e:
        logger.error(f"处理混合虚拟库时发生严重错误: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields):
    """
    辅助函数：通过分块GET请求安全地获取大量媒体项的完整信息。
    """
    if not item_ids:
        return []

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    id_chunks = list(chunk_list(item_ids, 150))
    all_items = []
    target_url = f"{base_url}/emby/Users/{user_id}/Items"
    logger.trace(f"[_fetch_items_in_chunks] ID列表已分为 {len(id_chunks)} 块。")

    for i, chunk in enumerate(id_chunks):
        get_params = {
            'api_key': api_key,
            'Ids': ",".join(chunk),
            'Fields': fields
        }
        try:
            logger.trace(f"[_fetch_items_in_chunks] 正在请求第 {i+1}/{len(id_chunks)} 块数据...")
            resp = requests.get(target_url, params=get_params, timeout=20)
            resp.raise_for_status()
            chunk_items = resp.json().get("Items", [])
            all_items.extend(chunk_items)
        except Exception as e_chunk:
            logger.error(f"[_fetch_items_in_chunks] 获取第 {i+1} 块数据时失败: {e_chunk}")
            continue
    
    return all_items

def handle_get_latest_items(user_id, params):
    """
    【V4.7 - 全局分块修复版】
    - 修复了“最新”栏目在虚拟库项目过多时因URL过长导致的 414 错误。
    - 现在也使用 _fetch_items_in_chunks 辅助函数来安全地获取数据。
    """
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')

        if virtual_library_id and is_mimicked_id(virtual_library_id):
            logger.trace(f"处理针对虚拟库 '{virtual_library_id}' 的最新媒体请求 (V4.7 新逻辑)...")
            
            # 步骤 1: 获取此虚拟库对当前用户可见的、完整的媒体ID列表 (逻辑同上)
            try:
                real_db_id = from_mimicked_id(virtual_library_id)
            except (ValueError, TypeError):
                return Response(json.dumps([]), mimetype='application/json')

            collection_info = collection_db.get_custom_collection_by_id(real_db_id)
            if not collection_info: return Response(json.dumps([]), mimetype='application/json')

            db_media_list = collection_info.get('generated_media_info_json') or []
            base_ordered_emby_ids = [item.get('emby_id') for item in db_media_list if item.get('emby_id')]
            if not base_ordered_emby_ids: return Response(json.dumps([]), mimetype='application/json')

            definition = collection_info.get('definition_json') or {}
            final_emby_ids_to_fetch = base_ordered_emby_ids
            if definition.get('dynamic_filter_enabled'):
                dynamic_rules = definition.get('dynamic_rules', [])
                ids_from_local_db = user_db.get_item_ids_by_dynamic_rules(user_id, dynamic_rules)
                if ids_from_local_db is not None:
                    final_emby_ids_set = set(base_ordered_emby_ids).intersection(set(ids_from_local_db))
                    final_emby_ids_to_fetch = [emby_id for emby_id in base_ordered_emby_ids if emby_id in final_emby_ids_set]
                else:
                    final_emby_ids_to_fetch = []

            if not final_emby_ids_to_fetch: return Response(json.dumps([]), mimetype='application/json')

            # 步骤 2: ★★★ 核心修复：调用分块函数获取所有项目的基本信息 ★★★
            fields_to_fetch = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")
            all_items = _fetch_items_in_chunks(base_url, api_key, user_id, final_emby_ids_to_fetch, fields_to_fetch)

            # 步骤 3: 在内存中对获取到的完整数据进行排序和切片
            # Emby 的 /Latest 接口本质上就是按 DateCreated 降序排序
            all_items.sort(key=lambda x: x.get('DateCreated', ''), reverse=True)
            
            limit = int(params.get('Limit', '24'))
            final_items = all_items[:limit]
            
            return Response(json.dumps(final_items), mimetype='application/json')
        
        else:
            # 对于原生库，保持原有的直接转发逻辑
            # ... [这部分代码保持不变] ...
            target_url = f"{base_url}/{request.path.lstrip('/')}"
            forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
            forward_headers['Host'] = urlparse(base_url).netloc
            forward_params = request.args.copy()
            forward_params['api_key'] = api_key
            resp = requests.request(
                method=request.method, url=target_url, headers=forward_headers, params=forward_params,
                data=request.get_data(), stream=True, timeout=30.0
            )
            excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
            response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
            return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
            
    except Exception as e:
        logger.error(f"处理最新媒体时发生未知错误: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')

proxy_app = Flask(__name__)

@proxy_app.route('/', defaults={'path': ''})
@proxy_app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
def proxy_all(path):
    # --- 1. WebSocket 代理逻辑 ---
    if 'Upgrade' in request.headers and request.headers.get('Upgrade', '').lower() == 'websocket':
        # ... (WebSocket 代码保持不变, 这里省略以保持简洁) ...
        logger.info("--- 收到一个新的 WebSocket 连接请求 ---")
        ws_client = request.environ.get('wsgi.websocket')
        if not ws_client:
            logger.error("!!! WebSocket请求，但未找到 wsgi.websocket 对象。请确保以正确的 handler_class 运行。")
            return "WebSocket upgrade failed", 400

        try:
            logger.debug(f"  [C->P] 客户端路径: /{path}")
            base_url, _ = _get_real_emby_url_and_key()
            parsed_url = urlparse(base_url)
            ws_scheme = 'wss' if parsed_url.scheme == 'https' else 'ws'
            target_ws_url = urlunparse((ws_scheme, parsed_url.netloc, f'/{path}', '', request.query_string.decode(), ''))
            logger.info(f"  [P->S] 准备连接到目标 Emby WebSocket: {target_ws_url}")
            headers_to_server = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'upgrade', 'connection', 'sec-websocket-key', 'sec-websocket-version']}
            
            ws_server = None
            try:
                ws_server = create_connection(target_ws_url, header=headers_to_server, timeout=10)
                logger.info("  [P<->S] ✅ 成功连接到远程 Emby WebSocket 服务器。")
            except Exception as e_connect:
                logger.error(f"  [P<->S] ❌ 连接到远程 Emby WebSocket 失败! 错误: {e_connect}", exc_info=True)
                ws_client.close()
                return Response()

            def forward_to_server():
                try:
                    while not ws_client.closed and ws_server.connected:
                        message = ws_client.receive()
                        if message is not None:
                            ws_server.send(message)
                        else:
                            break
                finally:
                    if ws_server.connected: ws_server.close()

            def forward_to_client():
                try:
                    while ws_server.connected and not ws_client.closed:
                        message = ws_server.recv()
                        if message is not None:
                            ws_client.send(message)
                        else:
                            break
                finally:
                    if not ws_client.closed: ws_client.close()
            
            greenlets = [spawn(forward_to_server), spawn(forward_to_client)]
            from gevent.event import Event
            exit_event = Event()
            def on_exit(g): exit_event.set()
            for g in greenlets: g.link(on_exit)
            exit_event.wait()
            logger.info("--- WebSocket 会话结束 ---")

        except Exception as e:
            logger.error(f"WebSocket 代理主逻辑发生严重错误: {e}", exc_info=True)
        
        return Response()

    # --- 2. HTTP 代理逻辑 (V4.2 路由修复版) ---
    try:
        full_path = f'/{path}'

        # 规则 1: 获取主页媒体库列表 (/Views)
        if path.endswith('/Views') and path.startswith('emby/Users/'):
            return handle_get_views()

        # 规则 2: 获取最新项目 (/Items/Latest) - **最重要**的修复
        if path.endswith('/Items/Latest'):
            user_id_match = re.search(r'/emby/Users/([^/]+)/', full_path)
            if user_id_match:
                return handle_get_latest_items(user_id_match.group(1), request.args)

        # 规则 3: 获取虚拟库详情 (e.g., /Items/-900001)
        details_match = MIMICKED_ITEM_DETAILS_RE.search(full_path)
        if details_match:
            user_id = details_match.group(1)
            mimicked_id = details_match.group(2)
            return handle_get_mimicked_library_details(user_id, mimicked_id)

        # 规则 4: 获取虚拟库图片
        if path.startswith('emby/Items/') and '/Images/' in path:
            item_id = path.split('/')[2]
            if is_mimicked_id(item_id):
                return handle_get_mimicked_library_image(path)
        
        # 规则 5: 获取虚拟库的元数据筛选信息 (如类型、年代等)
        parent_id = request.args.get("ParentId")
        if parent_id and is_mimicked_id(parent_id):
            # 检查是否是元数据端点
            if any(path.endswith(endpoint) for endpoint in UNSUPPORTED_METADATA_ENDPOINTS + ['/Items/Prefixes', '/Genres', '/Studios', '/Tags', '/OfficialRatings', '/Years']):
                return handle_mimicked_library_metadata_endpoint(path, parent_id, request.args)
            
            # 规则 6: 获取虚拟库的内容 (最通用的规则，放在后面)
            user_id_match = re.search(r'emby/Users/([^/]+)/Items', path)
            if user_id_match:
                user_id = user_id_match.group(1)
                return handle_get_mimicked_library_items(user_id, parent_id, request.args)

        # --- 默认转发逻辑 ---
        logger.warning(f"反代服务收到了一个未处理的请求: '{path}'。这通常意味着Nginx配置有误，请检查路由规则。")
        return Response("Path not handled by virtual library proxy.", status=404, mimetype='text/plain')
        
    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500