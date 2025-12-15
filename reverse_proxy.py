# reverse_proxy.py (最终完美版 V4 - 诊断增强版 - 无水印版)

import logging
import requests
import re
import random
import json
from flask import Flask, request, Response
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta
import time
import uuid 
from gevent import spawn, joinall
from websocket import create_connection
from database import collection_db, user_db, queries_db, media_db
from database.connection import get_db_connection
from handler.custom_collection import RecommendationEngine
import config_manager

import extensions
import handler.emby as emby
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
    # ... 并发获取分块数据 ...
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

def _fetch_sorted_items_via_emby_proxy(user_id, item_ids, sort_by, sort_order, limit, offset, fields, total_record_count):
    """
    - 引入“内存排序安全回退”机制。
    - 决策流程:
        1. [GET优先] 如果ID列表不超长，使用高效的GET请求让Emby排序。
        2. [安全回退] 如果ID列表超长，则分块获取所有媒体的详情(包含排序字段)，
           然后在内存中进行排序和分页，确保结果准确且无414错误。
    """
    base_url, api_key = _get_real_emby_url_and_key()
    
    # --- 核心规避逻辑：动态选择 GET 或 内存排序回退 ---
    estimated_ids_length = len(item_ids) * 40
    URL_LENGTH_THRESHOLD = 2000 

    try:
        if estimated_ids_length < URL_LENGTH_THRESHOLD:
            # --- 路径 A: ID列表较短，使用 GET 请求 (快速路径) ---
            logger.trace(f"  ➜ [Emby 代理排序] ID列表长度较短，使用 GET 方法。")
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            emby_params = {
                'api_key': api_key, 'Ids': ",".join(item_ids), 'Fields': fields,
                'SortBy': sort_by, 'SortOrder': sort_order,
                'StartIndex': offset, 'Limit': limit,
            }
            resp = requests.get(target_url, params=emby_params, timeout=25)
            resp.raise_for_status()
            emby_data = resp.json()
            emby_data['TotalRecordCount'] = total_record_count
            return emby_data
        else:
            # --- 路径 B: ID列表超长，启动内存排序安全回退 ---
            logger.trace(f"  ➜ [内存排序回退] ID列表超长 (估算 > {URL_LENGTH_THRESHOLD})，启动内存排序。")
            
            # 1. 获取所有媒体的详细信息，确保包含排序所需的字段
            primary_sort_by = sort_by.split(',')[0]
            fields_for_sorting = f"{fields},{primary_sort_by}"
            all_items_details = _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields_for_sorting)

            # 2. 在内存中排序
            try:
                is_desc = sort_order == 'Descending'
                # 为不同类型的排序字段提供安全的默认值
                if any(s in primary_sort_by.lower() for s in ['date', 'year']):
                    default_value = "1900-01-01T00:00:00.000Z"
                elif any(s in primary_sort_by.lower() for s in ['rating', 'count']):
                    default_value = 0
                else: # SortName, Name etc.
                    default_value = ""
                
                all_items_details.sort(key=lambda x: x.get(primary_sort_by, default_value) or default_value, reverse=is_desc)
            except Exception as sort_e:
                logger.error(f"  ➜ 内存排序时发生错误: {sort_e}", exc_info=True)
            
            # 3. 在内存中分页
            paginated_items = all_items_details[offset : offset + limit]
            
            return {"Items": paginated_items, "TotalRecordCount": total_record_count}

    except Exception as e:
        logger.error(f"  ➜ Emby代理排序或内存回退时失败: {e}", exc_info=True)
        return {"Items": [], "TotalRecordCount": total_record_count}

def _get_final_item_ids_for_view(user_id, collection_info):
    collection_id = collection_info['id']
    collection_type = collection_info.get('type')
    definition = collection_info.get('definition_json') or {}
    
    try:
        configured_limit = int(definition.get('limit', 50))
    except (ValueError, TypeError):
        configured_limit = 50
    
    final_emby_ids = []

    # ==================================================================
    # 分支 1: 个人推荐 (千人千面 - 实时向量)
    # ==================================================================
    if collection_type == 'ai_recommendation':
        try:
            api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
            if not api_key: return []

            engine = RecommendationEngine(api_key)
            
            # 1. 广撒网：请求 3 倍数量的候选池
            pool_size = max(configured_limit * 3, 60)
            candidate_pool = engine.generate_user_vector(user_id, limit=pool_size)
            
            if candidate_pool:
                # 2. 批量转换：将候选池全部转换为 Emby ID
                # 注意：这里我们先不随机，先把所有可能的 Emby ID 找出来
                items_to_lookup = []
                for item in candidate_pool:
                    tid = item.get('id') or item.get('tmdb_id')
                    mtype = item.get('type') or item.get('media_type')
                    if tid and mtype:
                        items_to_lookup.append({'tmdb_id': str(tid), 'media_type': mtype})
                
                all_candidate_emby_ids = []
                if items_to_lookup:
                    lookup_map = media_db.get_emby_ids_for_items(items_to_lookup)
                    # 保持向量搜索结果的顺序（虽然马上要随机了，但保持顺序是个好习惯）
                    for item in items_to_lookup:
                        key = f"{item['tmdb_id']}_{item['media_type']}"
                        if key in lookup_map:
                            all_candidate_emby_ids.append(lookup_map[key]['Id'])
                
                # 3. ★★★ 核心优化：先验票 (权限清洗) ★★★
                # 拿着这 100 多号人去 Emby 门口问问，谁有票能进？
                valid_candidate_ids = []
                if all_candidate_emby_ids:
                    try:
                        base_url, emby_api_key = _get_real_emby_url_and_key()
                        # 只请求 Id 字段，速度极快
                        valid_items = _fetch_items_in_chunks(
                            base_url, emby_api_key, user_id, all_candidate_emby_ids, fields='Id'
                        )
                        if valid_items:
                            valid_id_set = set(item['Id'] for item in valid_items)
                            # 过滤出有权限的 ID
                            valid_candidate_ids = [eid for eid in all_candidate_emby_ids if eid in valid_id_set]
                    except Exception as e:
                        logger.error(f"  ➜ [个人推荐] 权限预校验失败: {e}")
                        valid_candidate_ids = all_candidate_emby_ids

                # 4. ★★★ 从“合法”池子里随机抽取 ★★★
                if valid_candidate_ids:
                    count_to_pick = min(len(valid_candidate_ids), configured_limit)
                    
                    time_window = 300 
                    timestamp_key = int(time.time() / time_window)
                    seed_val = f"{user_id}_{timestamp_key}"
                    
                    rng = random.Random(seed_val)
                    final_emby_ids = rng.sample(valid_candidate_ids, count_to_pick)
                    
                    # ==========================================================
                    # ★★★ 抽取后，立即应用合集的默认排序 ★★★
                    # ==========================================================
                    default_sort_by = definition.get('default_sort_by')
                    default_sort_order = definition.get('default_sort_order', 'Ascending')

                    # 只有当配置了有效的排序字段时才执行
                    # 'original' 在这里没有意义（因为源头是随机的），所以也跳过
                    if default_sort_by and default_sort_by not in ['none', 'original']:
                        try:
                            # 利用现有的数据库查询工具进行排序
                            # 这一步非常快，因为只对 50 个 ID 进行排序
                            sorted_ids = queries_db.get_sorted_and_paginated_ids(
                                final_emby_ids,
                                default_sort_by,
                                default_sort_order,
                                limit=len(final_emby_ids), # 全排
                                offset=0
                            )
                            # 如果排序成功返回了数据，就覆盖掉原来的随机顺序
                            if sorted_ids:
                                final_emby_ids = sorted_ids
                                logger.debug(f"  ➜ [个人推荐] 已对随机结果应用默认排序: {default_sort_by} ({default_sort_order})")
                        except Exception as sort_e:
                            logger.warning(f"  ➜ [个人推荐] 应用默认排序失败，将保持随机顺序: {sort_e}")
                    # ==========================================================

                    logger.info(f"  ➜ [个人推荐] 用户 {user_id}: 候选 {len(candidate_pool)} -> 确权 {len(valid_candidate_ids)} -> 抽取并排序 {len(final_emby_ids)} (锚点: {timestamp_key})")

        except Exception as calc_e:
            logger.error(f"  ➜ 实时计算推荐时发生错误: {calc_e}", exc_info=True)
            return []

    # ==================================================================
    # 分支 2: 其他合集
    # ==================================================================
    else: 
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT visible_emby_ids_json FROM user_collection_cache WHERE user_id = %s AND collection_id = %s",
                        (user_id, collection_id)
                    )
                    row = cursor.fetchone()
                    if row and row['visible_emby_ids_json']:
                        final_emby_ids = row['visible_emby_ids_json']
        except Exception as e:
            logger.error(f"  ➜ 查询权限缓存出错: {e}")
            return []

    # ==================================================================
    # 通用后续处理: 动态过滤 & 权限清洗
    # ==================================================================
    if not final_emby_ids:
        return []

    # 1. 动态过滤 (已看/收藏)
    if definition.get('dynamic_filter_enabled'):
        dynamic_rules = definition.get('dynamic_rules', [])
        if dynamic_rules:
            ids_from_local_db = user_db.get_item_ids_by_dynamic_rules(user_id, dynamic_rules)
            if ids_from_local_db is not None:
                dynamic_ids_set = set(ids_from_local_db)
                final_emby_ids = [eid for eid in final_emby_ids if eid in dynamic_ids_set]

    return final_emby_ids

def handle_get_views():
    """
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

        user_visible_native_libs = emby.get_emby_libraries(
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
        
        logger.debug(f"  ➜ 已为用户 {user_id} 生成 {len(fake_views_items)} 个可见的虚拟库。")

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

def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    智能处理所有针对虚拟库的元数据类请求。
    """
    # 检查当前请求的路径是否在我们定义的“不支持列表”中
    if any(path.endswith(endpoint) for endpoint in UNSUPPORTED_METADATA_ENDPOINTS):
        logger.trace(f"  ➜ 检测到对虚拟库的不支持的元数据请求 '{path}'，将直接返回空列表以避免后端错误。")
        # 直接返回一个空的JSON数组，客户端会优雅地处理它（不显示相关筛选器）
        return Response(json.dumps([]), mimetype='application/json')

    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info or not collection_info.get('emby_collection_id'):
            return Response(json.dumps([]), mimetype='application/json')

        real_emby_collection_id = collection_info.get('emby_collection_id')
        
        base_url, api_key = _get_real_emby_url_and_key()
        
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
    - 将Emby代理排序的逻辑提取到独立的、健壮的辅助函数中。
    - 该函数能智能选择GET或POST，彻底解决因ID列表过长导致的414错误。
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
        
        defined_sort_by = definition.get('default_sort_by')
        if defined_sort_by and defined_sort_by != 'none':
            final_sort_by = defined_sort_by
            final_sort_order = definition.get('default_sort_order', 'Ascending')
        else:
            final_sort_by = params.get('SortBy') or 'SortName'
            final_sort_order = params.get('SortOrder') or 'Ascending'
        
        primary_sort_by = final_sort_by.split(',')[0]
        SUPPORTED_LOCAL_SORT_FIELDS = ['PremiereDate', 'DateCreated', 'CommunityRating', 'ProductionYear', 'SortName', 'original']
        
        limit = int(params.get('Limit', 50))
        offset = int(params.get('StartIndex', 0))

        use_local_sort = (primary_sort_by in SUPPORTED_LOCAL_SORT_FIELDS) and (defined_sort_by != 'none')

        if use_local_sort:
            # --- 分支 A: [快速路径] 本地高性能排序 (逻辑不变) ---
            logger.trace(f"  ➜ 使用本地数据库排序 (SortBy={primary_sort_by})。")
            paginated_ids = []
            if primary_sort_by == 'original':
                paginated_ids = final_visible_ids[offset : offset + limit]
            else:
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

        else:
            # --- 分支 B: [Emby 代理排序路径] - 调用新的健壮函数 ---
            logger.trace(f"  ➜ 使用 Emby 进行远程排序或内存排序 (SortBy={primary_sort_by})。")
            full_fields = "PrimaryImageAspectRatio,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount"
            
            sorted_data = _fetch_sorted_items_via_emby_proxy(
                user_id, final_visible_ids, final_sort_by, final_sort_order, limit, offset, full_fields, total_record_count
            )
            return Response(json.dumps(sorted_data), mimetype='application/json')

    except Exception as e:
        logger.error(f"  ➜ 处理虚拟库 '{collection_info.get('name', mimicked_id)}' 时发生严重错误: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def handle_get_latest_items(user_id, params):
    """
    - 直接从数据库筛选合集ID。
    - 同样调用新的健壮辅助函数来处理剧集的最新排序。
    """
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')

        # ======================================================================
        # 场景一：处理【单个】虚拟库的“最近添加”
        # ======================================================================
        if virtual_library_id and is_mimicked_id(virtual_library_id):
            real_db_id = from_mimicked_id(virtual_library_id)
            collection_info = collection_db.get_custom_collection_by_id(real_db_id)
            if not collection_info: 
                return Response(json.dumps([]), mimetype='application/json')

            definition = collection_info.get('definition_json') or {}
            
            if not definition.get('show_in_latest', True):
                logger.trace(f"  ➜ 虚拟库 '{collection_info['name']}' 已关闭“在首页显示最新”，为其返回空列表。")
                return Response(json.dumps([]), mimetype='application/json')

            final_visible_ids = _get_final_item_ids_for_view(user_id, collection_info)
            if not final_visible_ids: 
                return Response(json.dumps([]), mimetype='application/json')
            
            item_type_from_db = definition.get('item_type', ['Movie'])
            
            sort_by_str = 'DateCreated'
            if isinstance(item_type_from_db, list) and len(item_type_from_db) == 1 and item_type_from_db[0] == 'Series':
                sort_by_str = 'DateLastContentAdded,DateCreated'
            
            sort_order = 'Descending'
            limit = int(params.get('Limit', 24))
            fields = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")

            if 'DateLastContentAdded' in sort_by_str:
                sorted_data = _fetch_sorted_items_via_emby_proxy(
                    user_id, final_visible_ids, sort_by_str, sort_order, limit, 0, fields, len(final_visible_ids)
                )
                return Response(json.dumps(sorted_data.get("Items", [])), mimetype='application/json')
            else:
                latest_ids = queries_db.get_sorted_and_paginated_ids(final_visible_ids, sort_by_str.split(',')[0], sort_order, limit, 0)
                if not latest_ids: return Response(json.dumps([]), mimetype='application/json')
                
                items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, latest_ids, fields)
                items_map = {item['Id']: item for item in items_from_emby}
                final_items = [items_map[id] for id in latest_ids if id in items_map]
                return Response(json.dumps(final_items), mimetype='application/json')

        # ======================================================================
        # 场景二：处理【全局】“最近添加”请求 (例如，Emby 主页最顶部的“最新媒体”)
        # ======================================================================
        elif not virtual_library_id:
            logger.trace(f"  ➜ 正在为用户 {user_id} 处理全局“最新媒体”请求...")
            
            # ★★★ 核心性能优化: 直接从数据库调用新函数，一步到位获取符合条件的合集ID ★★★
            included_collection_ids = collection_db.get_active_collection_ids_for_latest_view()

            if not included_collection_ids:
                logger.trace(f"  ➜ 用户 {user_id} 没有任何开启了“在首页显示最新”的可见合集。")
                return Response(json.dumps([]), mimetype='application/json')

            # 后续逻辑不变，但现在它处理的是一个预先被高效筛选过的ID列表
            all_possible_ids = set()
            from database.connection import get_db_connection
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        sql = "SELECT visible_emby_ids_json FROM user_collection_cache WHERE user_id = %s AND collection_id = ANY(%s)"
                        cursor.execute(sql, (user_id, included_collection_ids))
                        rows = cursor.fetchall()
                        for row in rows:
                            if row['visible_emby_ids_json']:
                                all_possible_ids.update(row['visible_emby_ids_json'])
            except Exception as e:
                logger.error(f"  ➜ 聚合用户 {user_id} 的所有可见媒体ID时出错: {e}", exc_info=True)
                return Response(json.dumps([]), mimetype='application/json')

            if not all_possible_ids:
                return Response(json.dumps([]), mimetype='application/json')

            limit = int(params.get('Limit', 100))
            latest_ids = queries_db.get_sorted_and_paginated_ids(list(all_possible_ids), 'DateCreated', 'Descending', limit, 0)

            if not latest_ids: 
                return Response(json.dumps([]), mimetype='application/json')

            fields = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")
            items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, latest_ids, fields)
            items_map = {item['Id']: item for item in items_from_emby}
            final_items = [items_map[id] for id in latest_ids if id in items_map]
            
            logger.trace(f"  ➜ 为用户 {user_id} 的全局“最新媒体”请求成功返回 {len(final_items)} 个项目。")
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
        logger.trace(f"  ➜ 请求 '{path}' 未命中任何虚拟库规则，将直接转发至后端 Emby。")
        base_url, api_key = _get_real_emby_url_and_key()
        target_url = f"{base_url}/{path.lstrip('/')}"
        
        forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        forward_headers['Host'] = urlparse(base_url).netloc
        
        forward_params = request.args.copy()
        forward_params['api_key'] = api_key
        
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=forward_headers,
            params=forward_params,
            data=request.get_data(),
            stream=True,
            timeout=30.0
        )
        
        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
        
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
        
    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500