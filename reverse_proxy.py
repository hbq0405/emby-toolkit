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
from gevent import spawn, joinall
from gevent.lock import RLock
from geventwebsocket.websocket import WebSocket
from websocket import create_connection
from database import collection_db, user_db, queries_db
import config_manager
from cachetools import TTLCache

import extensions
import emby_handler
logger = logging.getLogger(__name__)

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

def _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields):
    # ... V4.8 的并发版本，现在重新变得重要 ...
    if not item_ids: return []
    def chunk_list(lst, n):
        for i in range(0, len(lst), n): yield lst[i:i + n]
    id_chunks = list(chunk_list(item_ids, 150))
    target_url = f"{base_url}/emby/Users/{user_id}/Items"
    def fetch_chunk(chunk):
        params = {'api_key': api_key, 'Ids': ",".join(chunk), 'Fields': fields}
        try:
            resp = requests.get(target_url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json().get("Items", [])
        except Exception as e:
            logger.error(f"并发获取某分块数据时失败: {e}")
            return None
    greenlets = [spawn(fetch_chunk, chunk) for chunk in id_chunks]
    joinall(greenlets)
    all_items = []
    for g in greenlets:
        if g.value: all_items.extend(g.value)
    return all_items

def _get_final_item_ids_for_view(user_id, collection_info):
    """
    【V6.0 - 终极简化版】
    - 彻底移除实时权限检查。
    - 直接从 user_collection_cache 表中获取为该用户预计算好的、100%有权限的媒体列表。
    - 仍然保留了动态用户数据筛选的能力。
    """
    collection_id = collection_info['id']
    definition = collection_info.get('definition_json') or {}
    
    # ======================================================================
    # ★★★ 核心改造 2/4: 逻辑极度简化！★★★
    # ======================================================================
    
    # --- 步骤 1: 直接从“贵宾名册”中查询专属列表 ---
    from database.connection import get_db_connection
    base_ordered_emby_ids = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT visible_emby_ids_json FROM user_collection_cache WHERE user_id = %s AND collection_id = %s",
                    (user_id, collection_id)
                )
                row = cursor.fetchone()
                if row and row['visible_emby_ids_json']:
                    base_ordered_emby_ids = row['visible_emby_ids_json']
    except Exception as e:
        logger.error(f"查询用户 {user_id} 在合集 {collection_id} 的权限缓存时出错: {e}", exc_info=True)
        return []

    if not base_ordered_emby_ids:
        return []

    # --- 步骤 2: 在这个干净的列表上，再执行动态用户数据筛选 (如果需要) ---
    final_emby_ids_to_process = base_ordered_emby_ids
    if definition.get('dynamic_filter_enabled'):
        dynamic_rules = definition.get('dynamic_rules', [])
        ids_from_local_db = user_db.get_item_ids_by_dynamic_rules(user_id, dynamic_rules)
        
        if ids_from_local_db is not None:
            dynamic_ids_set = set(ids_from_local_db)
            final_emby_ids_to_process = [emby_id for emby_id in base_ordered_emby_ids if emby_id in dynamic_ids_set]
            logger.debug(f"用户个人行为数据过滤后，媒体项数量从 {len(base_ordered_emby_ids)} 变为 {len(final_emby_ids_to_process)}。")

    return final_emby_ids_to_process

def _fetch_items_from_emby(base_url, api_key, user_id, item_ids, fields):
    if not item_ids: return []
    target_url = f"{base_url}/emby/Users/{user_id}/Items"
    params = {'api_key': api_key, 'Ids': ",".join(item_ids), 'Fields': fields}
    try:
        resp = requests.get(target_url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("Items", [])
    except Exception as e:
        logger.error(f"向Emby请求媒体信息时失败: {e}")
        return []

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
    【V6.2 - 排序逻辑终极归正版】
    - 恢复“劫持”与“不劫持”的双分支逻辑。
    - 当排序为 'none' 时，将请求完美转发给 Emby 原生处理，彻底解决排序不一致问题。
    - 通过传递 ParentId 而非 Ids，从根本上避免了 414 URI Too Long 错误。
    """
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        final_visible_ids = _get_final_item_ids_for_view(user_id, collection_info)
        total_record_count = len(final_visible_ids)
        
        if not final_visible_ids:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        definition = collection_info.get('definition_json') or {}
        collection_sort_by = definition.get('default_sort_by', 'SortName')

        # ======================================================================
        # ★★★ 核心修复：恢复双分支逻辑 ★★★
        # ======================================================================

        if collection_sort_by == 'none':
            # --- 分支 A: “不劫持”模式，完全交由 Emby 处理 ---
            logger.debug(f"  ➜ 虚拟库 '{collection_info['name']}' 使用 Emby 原生排序 (客户端请求: SortBy={params.get('SortBy')})。")
            
            real_emby_collection_id = collection_info.get('emby_collection_id')
            if not real_emby_collection_id:
                # 如果没有对应的物理合集，无法转发，返回空
                return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

            base_url, api_key = _get_real_emby_url_and_key()
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            
            # 构造一个干净的、用于转发的参数字典
            emby_params = params.copy()
            emby_params['api_key'] = api_key
            # ★ 关键：我们不传 Ids，而是传 ParentId，让 Emby 自己去合集里找
            emby_params['ParentId'] = real_emby_collection_id
            
            try:
                resp = requests.get(target_url, params=emby_params, timeout=25)
                resp.raise_for_status()
                emby_response_data = resp.json()
                
                # Emby 返回的总数是合集内的总数，我们需要用我们自己计算的、用户可见的总数来覆盖它，
                # 这样客户端的分页栏才不会出错。
                emby_response_data['TotalRecordCount'] = total_record_count
                
                return Response(json.dumps(emby_response_data), mimetype='application/json')

            except Exception as e:
                logger.error(f"  ➜ 请求 Emby 原生排序时失败: {e}", exc_info=True)
                return Response(json.dumps({"Items": [], "TotalRecordCount": total_record_count}), mimetype='application/json')

        else:
            # --- 分支 B: “劫持”模式，使用本地数据库进行高性能排序 ---
            logger.debug(f"  ➜ 虚拟库 '{collection_info['name']}' 强制使用本地预设排序 (SortBy={collection_sort_by})。")
            
            final_sort_by = collection_sort_by
            final_sort_order = definition.get('default_sort_order', 'Ascending')
            
            limit = int(params.get('Limit', 50))
            offset = int(params.get('StartIndex', 0))
            
            paginated_ids = []
            if final_sort_by == 'original':
                paginated_ids = final_visible_ids[offset : offset + limit]
            else:
                primary_sort_by = final_sort_by.split(',')[0]
                paginated_ids = queries_db.get_sorted_and_paginated_ids(
                    final_visible_ids, primary_sort_by, final_sort_order, limit, offset
                )

            if not paginated_ids:
                return Response(json.dumps({"Items": [], "TotalRecordCount": total_record_count}), mimetype='application/json')

            base_url, api_key = _get_real_emby_url_and_key()
            full_fields = "PrimaryImageAspectRatio,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount"
            items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, paginated_ids, full_fields)
            
            items_map = {item['Id']: item for item in items_from_emby}
            final_items = [items_map[id] for id in paginated_ids if id in items_map]

            return Response(json.dumps({"Items": final_items, "TotalRecordCount": total_record_count}), mimetype='application/json')

    except Exception as e:
        logger.error(f"  ➜ 处理虚拟库 '{collection_info.get('name', mimicked_id)}' 时发生严重错误: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def handle_get_latest_items(user_id, params):
    """
    【V6.0 - “完全体”异步权限预计算版】
    - 无论是处理单个库还是全局最新，都直接从预计算好的专属列表中获取数据。
    """
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')

        # ======================================================================
        # 场景一：处理针对【单个】虚拟库的“最近添加”请求 (例如，进入虚拟库首页)
        # ======================================================================
        if virtual_library_id and is_mimicked_id(virtual_library_id):
            real_db_id = from_mimicked_id(virtual_library_id)
            collection_info = collection_db.get_custom_collection_by_id(real_db_id)
            if not collection_info: 
                return Response(json.dumps([]), mimetype='application/json')

            # 这个函数现在的作用就是去 user_collection_cache 表里查出专属列表
            final_visible_ids = _get_final_item_ids_for_view(user_id, collection_info)
            
            if not final_visible_ids: 
                return Response(json.dumps([]), mimetype='application/json')
            
            # --- 后续的排序和分页逻辑完全不变，因为它们本来就是对干净列表操作的 ---
            definition = collection_info.get('definition_json') or {}
            item_type_from_db = definition.get('item_type', ['Movie'])
            # 如果是混合库，则用DateCreated排序
            if len(item_type_from_db) > 1:
                sort_by_str = 'DateCreated'
            else:
                is_series_focused = 'Series' in item_type_from_db
                sort_by_str = 'DateLastContentAdded,DateCreated' if is_series_focused else 'DateCreated'
            sort_order = 'Descending'
            
            limit = int(params.get('Limit', 24))
            fields = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")
            
            # 使用我们已有的、高效的本地排序分页函数
            latest_ids = queries_db.get_sorted_and_paginated_ids(final_visible_ids, sort_by_str.split(',')[0], sort_order, limit, 0)

            if not latest_ids: 
                return Response(json.dumps([]), mimetype='application/json')
            
            # 只为最终的一页 ID 去 Emby 获取详情
            items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, latest_ids, fields)
            items_map = {item['Id']: item for item in items_from_emby}
            final_items = [items_map[id] for id in latest_ids if id in items_map]
            
            return Response(json.dumps(final_items), mimetype='application/json')

        # ======================================================================
        # 场景二：处理【全局】“最近添加”请求 (例如，Emby 主页最顶部的“最新媒体”)
        # ======================================================================
        elif not virtual_library_id:
            logger.debug(f"  ➜ 正在为用户 {user_id} 处理全局“最新媒体”请求...")
            
            # --- 不再遍历所有合集，而是直接从 user_collection_cache 聚合 ---
            all_possible_ids = set()
            from database.connection import get_db_connection
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # 一次查询，拿出这个用户所有可见合集的专属列表
                        cursor.execute(
                            "SELECT visible_emby_ids_json FROM user_collection_cache WHERE user_id = %s",
                            (user_id,)
                        )
                        rows = cursor.fetchall()
                        # 在内存中把所有列表合并成一个大的 set
                        for row in rows:
                            if row['visible_emby_ids_json']:
                                all_possible_ids.update(row['visible_emby_ids_json'])
            except Exception as e:
                logger.error(f"聚合用户 {user_id} 的所有可见媒体ID时出错: {e}", exc_info=True)
                return Response(json.dumps([]), mimetype='application/json')

            if not all_possible_ids:
                return Response(json.dumps([]), mimetype='application/json')

            limit = int(params.get('Limit', 100))
            # 全局最新，永远按 DateCreated 排序
            latest_ids = queries_db.get_sorted_and_paginated_ids(list(all_possible_ids), 'DateCreated', 'Descending', limit, 0)

            if not latest_ids: 
                return Response(json.dumps([]), mimetype='application/json')

            fields = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")
            items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, latest_ids, fields)
            items_map = {item['Id']: item for item in items_from_emby}
            final_items = [items_map[id] for id in latest_ids if id in items_map]
            
            logger.debug(f"  ➜ 为用户 {user_id} 的全局“最新媒体”请求成功返回 {len(final_items)} 个项目。")
            return Response(json.dumps(final_items), mimetype='application/json')
            
        # ======================================================================
        # 场景三：原生库的请求，直接转发 
        # ======================================================================
        else:
            target_url = f"{base_url}/{request.path.lstrip('/')}"
            forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
            forward_headers['Host'] = urlparse(base_url).netloc
            forward_params = request.args.copy()
            forward_params['api_key'] = api_key
            resp = requests.request(method=request.method, url=target_url, headers=forward_headers, params=forward_params, data=request.get_data(), stream=True, timeout=30.0)
            excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
            response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
            return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
            
    except Exception as e:
        logger.error(f"  ➜ 处理最新媒体时发生未知错误: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')

proxy_app = Flask(__name__)

@proxy_app.route('/', defaults={'path': ''})
@proxy_app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
def proxy_all(path):
    # --- 1. WebSocket 代理逻辑 (已添加超详细日志) ---
    if 'Upgrade' in request.headers and request.headers.get('Upgrade', '').lower() == 'websocket':
        logger.info("--- 收到一个新的 WebSocket 连接请求 ---")
        ws_client = request.environ.get('wsgi.websocket')
        if not ws_client:
            logger.error("!!! WebSocket请求，但未找到 wsgi.websocket 对象。请确保以正确的 handler_class 运行。")
            return "WebSocket upgrade failed", 400

        try:
            # 1. 记录客户端信息
            logger.debug(f"  [C->P] 客户端路径: /{path}")
            logger.debug(f"  [C->P] 客户端查询参数: {request.query_string.decode()}")
            logger.debug(f"  [C->P] 客户端 Headers: {dict(request.headers)}")

            # 2. 构造目标 URL
            base_url, _ = _get_real_emby_url_and_key()
            parsed_url = urlparse(base_url)
            ws_scheme = 'wss' if parsed_url.scheme == 'https' else 'ws'
            target_ws_url = urlunparse((ws_scheme, parsed_url.netloc, f'/{path}', '', request.query_string.decode(), ''))
            logger.info(f"  [P->S] 准备连接到目标 Emby WebSocket: {target_ws_url}")

            # 3. 提取 Headers 并尝试连接
            headers_to_server = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'upgrade', 'connection', 'sec-websocket-key', 'sec-websocket-version']}
            logger.debug(f"  [P->S] 转发给服务器的 Headers: {headers_to_server}")
            
            ws_server = None
            try:
                ws_server = create_connection(target_ws_url, header=headers_to_server, timeout=10)
                logger.info("  [P<->S] ✅ 成功连接到远程 Emby WebSocket 服务器。")
            except Exception as e_connect:
                logger.error(f"  [P<->S] ❌ 连接到远程 Emby WebSocket 失败! 错误: {e_connect}", exc_info=True)
                ws_client.close()
                return Response()

            # 4. 创建双向转发协程
            def forward_to_server():
                try:
                    while not ws_client.closed and ws_server.connected:
                        message = ws_client.receive()
                        if message is not None:
                            logger.trace(f"  [C->S] 转发消息: {message[:200] if message else 'None'}") # 只记录前200字符
                            ws_server.send(message)
                        else:
                            logger.info("  [C->P] 客户端连接已关闭 (receive返回None)。")
                            break
                except Exception as e_fwd_s:
                    logger.warning(f"  [C->S] 转发到服务器时出错: {e_fwd_s}")
                finally:
                    if ws_server.connected:
                        ws_server.close()
                        logger.info("  [P->S] 已关闭到服务器的连接。")

            def forward_to_client():
                try:
                    while ws_server.connected and not ws_client.closed:
                        message = ws_server.recv()
                        if message is not None:
                            logger.trace(f"  [S->C] 转发消息: {message[:200] if message else 'None'}") # 只记录前200字符
                            ws_client.send(message)
                        else:
                            logger.info("  [P<-S] 服务器连接已关闭 (recv返回None)。")
                            break
                except Exception as e_fwd_c:
                    logger.warning(f"  [S->C] 转发到客户端时出错: {e_fwd_c}")
                finally:
                    if not ws_client.closed:
                        ws_client.close()
                        logger.info("  [P->C] 已关闭到客户端的连接。")
            
            greenlets = [spawn(forward_to_server), spawn(forward_to_client)]
            from gevent.event import Event
            exit_event = Event()
            def on_exit(g): exit_event.set()
            for g in greenlets: g.link(on_exit)
            
            logger.info("  [P<->S] WebSocket 双向转发已启动。等待连接关闭...")
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