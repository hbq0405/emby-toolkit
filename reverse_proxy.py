# reverse_proxy.py (最终完美版 V4 - 诊断增强版 - 无水印版)

import logging
import requests
import re
import json
from flask import Flask, request, Response
from urllib.parse import urlparse, urlunparse
import time
import uuid # <-- 确保导入
from gevent import spawn
from geventwebsocket.websocket import WebSocket
from websocket import create_connection

from custom_collection_handler import FilterEngine
import config_manager
import db_handler
import extensions
import emby_handler
logger = logging.getLogger(__name__)

# --- 【核心修改】---
# 不再使用字符串前缀，而是定义一个数字转换基数
# 这将把数据库ID (例如 7) 转换为一个唯一的、负数的、看起来像原生ID的数字 (例如 -900007)
MIMICKED_ID_BASE = 900000

def to_mimicked_id(db_id):
    """将数据库内部ID转换为代理使用的外部ID"""
    return str(-(MIMICKED_ID_BASE + db_id))

def from_mimicked_id(mimicked_id):
    """将代理的外部ID转换回数据库内部ID"""
    return -(int(mimicked_id)) - MIMICKED_ID_BASE

def is_mimicked_id(item_id):
    """检查一个ID是否是我们的虚拟ID"""
    try:
        # 我们的ID是负数
        return isinstance(item_id, str) and item_id.startswith('-')
    except:
        return False

# --- 【核心修改】---
# 更新正则表达式以匹配新的数字ID格式（负数）
MIMICKED_ITEMS_RE = re.compile(r'/emby/Users/([^/]+)/Items/(-(\d+))')
MIMICKED_ITEM_DETAILS_RE = re.compile(r'emby/Users/([^/]+)/Items/(-(\d+))$')


def _get_real_emby_url_and_key():
    base_url = config_manager.APP_CONFIG.get("emby_server_url", "").rstrip('/')
    api_key = config_manager.APP_CONFIG.get("emby_api_key", "")
    if not base_url or not api_key:
        raise ValueError("Emby服务器地址或API Key未配置")
    return base_url, api_key

def handle_get_views():
    """
    【V2 - PG JSON 兼容版】
    - 修复了因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        return "Proxy is not ready", 503

    try:
        collections = db_handler.get_all_active_custom_collections()
        fake_views_items = []
        for coll in collections:
            real_emby_collection_id = coll.get('emby_collection_id')
            if not real_emby_collection_id:
                logger.debug(f"  -> 虚拟库 '{coll['name']}' (ID: {coll['id']}) 因无对应的真实Emby合集而被隐藏。")
                continue

            db_id = coll['id']
            mimicked_id = to_mimicked_id(db_id)
            image_tags = {"Primary": f"{real_emby_collection_id}?timestamp={int(time.time())}"}

            # ★★★ 核心修复：直接使用已经是字典的 definition_json 字段 ★★★
            definition = coll.get('definition_json') or {}
            
            merged_libraries = definition.get('merged_libraries', [])
            name_suffix = f" (合并库: {len(merged_libraries)}个)" if merged_libraries else ""
            
            item_type_from_db = definition.get('item_type', 'Movie')
            if isinstance(item_type_from_db, list) and len(item_type_from_db) > 1:
                collection_type = "mixed"
            else:
                authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
                collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

            fake_view = {
                "Name": coll['name'] + name_suffix, 
                "ServerId": real_server_id, 
                "Id": mimicked_id,
                "Guid": str(uuid.uuid4()), # 我们会用这个Guid
                "Etag": f"{db_id}{int(time.time())}",
                "DateCreated": "2025-01-01T00:00:00.0000000Z", 
                "CanDelete": False, 
                "CanDownload": False,
                "SortName": coll['name'], 
                "ExternalUrls": [], 
                "ProviderIds": {}, 
                "IsFolder": True,
                "ParentId": "2", 
                "Type": "CollectionFolder",  # 1. 改为 CollectionFolder
                "PresentationUniqueKey": str(uuid.uuid4()), # 2. 增加 PresentationUniqueKey
                "DisplayPreferencesId": f"custom-{db_id}", # 3. DisplayPreferencesId 保持不变或改为Guid都可以
                "ForcedSortName": coll['name'], # 4. 增加 ForcedSortName
                "Taglines": [], # 5. 增加空的 Taglines
                "RemoteTrailers": [], # 6. 增加空的 RemoteTrailers
                "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
                "ChildCount": 1, 
                "PrimaryImageAspectRatio": 1.7777777777777777, 
                "CollectionType": collection_type,
                "ImageTags": image_tags, 
                "BackdropImageTags": [], 
                "LockedFields": [], 
                "LockData": False
            }
            fake_views_items.append(fake_view)
        
        logger.debug(f"已生成 {len(fake_views_items)} 个虚拟库。")

        native_views_items = []
        should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
        if should_merge_native:
            user_id_match = re.search(r'/emby/Users/([^/]+)/Views', request.path)
            if user_id_match:
                user_id = user_id_match.group(1)
                all_native_views = emby_handler.get_emby_libraries(
                    config_manager.APP_CONFIG.get("emby_server_url", ""),
                    config_manager.APP_CONFIG.get("emby_api_key", ""),
                    user_id
                )
                if all_native_views is None: all_native_views = []
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
        coll = db_handler.get_custom_collection_by_id(real_db_id)
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

# --- ★★★ 核心修复 #1：用下面这个通用的“万能翻译”函数，替换掉旧的 a_prefixes 函数 ★★★ ---
def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    【V3 - URL修正版】
    智能处理所有针对虚拟库的元数据类请求。
    """
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = db_handler.get_custom_collection_by_id(real_db_id)
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
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = db_handler.get_custom_collection_by_id(real_db_id)
        if not collection_info or not collection_info.get('emby_collection_id'):
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        definition = collection_info.get('definition_json') or {}
        real_emby_collection_id = collection_info.get('emby_collection_id')

        # --- 阶段一：从真实的Emby合集获取基础内容 ---
        logger.trace(f"  -> 阶段1：正在从真实合集 '{collection_info['name']}' (ID: {real_emby_collection_id}) 获取基础内容...")
        base_url, api_key = _get_real_emby_url_and_key()
        
        base_items = emby_handler.get_emby_library_items(
            base_url=base_url, api_key=api_key, user_id=user_id,
            library_ids=[real_emby_collection_id],
            fields="PrimaryImageAspectRatio,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,RecursiveItemCount"
        )
        if not base_items:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')
        
        logger.trace(f"  -> 阶段1完成：获取到 {len(base_items)} 个基础媒体项。")

        # --- 阶段二：如果启用了动态筛选，就执行二次过滤 ---
        final_items = base_items
        
        if definition.get('dynamic_filter_enabled'):
            logger.trace("  -> 阶段2：检测到已启用实时用户筛选，开始二次过滤...")
            
            # ★★★ 核心性能优化：数据预加载 (备货) ★★★
            logger.debug("    -> 正在为动态筛选预加载用户数据...")
            all_user_data = db_handler.get_all_user_media_data_for_user(user_id)
            
            # ★★★ 核心性能优化：数据整理成快速查找表 ★★★
            items_map = {d['item_emby_id']: d for d in all_user_data}
            
            # 在应用层进行剧集状态聚合
            # 1. 从刚从Emby获取的 base_items 中创建一个剧集详情的快速查找表
            series_details_map = {item['Id']: item for item in base_items if item.get('Type') == 'Series'}

            # 2. 聚合本地数据库中的用户播放数据 (逻辑不变)
            series_episodes_map = {}
            for d in all_user_data:
                if d.get('series_emby_id'):
                    sid = d['series_emby_id']
                    if sid not in series_episodes_map:
                        series_episodes_map[sid] = []
                    series_episodes_map[sid].append(d)
            
            # 3. 结合两边的数据，进行最终状态计算
            series_aggregated_map = {}
            for series_id, episodes in series_episodes_map.items():
                # 从快速查找表中获取权威的剧集详情
                series_details = series_details_map.get(series_id)
                if not series_details:
                    # 如果这个剧集不在当前的视图中（例如被Emby的某些规则排除了），则跳过
                    continue

                # ★★★ 关键：使用来自Emby的 RecursiveItemCount 作为权威的总集数 ★★★
                total = series_details.get('RecursiveItemCount', 0)
                
                # 计算已播放和播放中的集数 (逻辑不变)
                played = sum(1 for ep in episodes if ep.get('is_played'))
                in_progress = sum(1 for ep in episodes if not ep.get('is_played') and ep.get('playback_position_ticks', 0) > 0)
                
                # 判断状态 (逻辑不变)
                status = 'unplayed'
                if total > 0 and played >= total: # 使用 >= 以增加容错性
                    status = 'played'
                elif played > 0 or in_progress > 0:
                    status = 'in_progress'
                
                series_aggregated_map[series_id] = {'playback_status': status}

            prefetched_user_data = {
                "items_map": items_map,
                "series_map": series_aggregated_map
            }
            logger.debug("    -> 用户数据预加载和聚合完成。")

            dynamic_definition = {
                'rules': definition.get('dynamic_rules', []),
                'logic': definition.get('dynamic_logic', 'AND')
            }
            
            engine = FilterEngine()
            # ★★★ 将预加载的数据传递给筛选引擎 ★★★
            final_items = engine.execute_dynamic_filter(base_items, dynamic_definition, user_id, prefetched_user_data)
            logger.trace(f"  -> 阶段2完成：二次过滤后，剩下 {len(final_items)} 个媒体项。")
        else:
            logger.trace("  -> 阶段2跳过：未启用实时用户筛选。")

        # --- 排序和返回 ---
        sort_by_field = definition.get('default_sort_by')
        if sort_by_field and sort_by_field != 'none' and sort_by_field != 'original':
            sort_order = definition.get('default_sort_order', 'Ascending')
            is_descending = (sort_order == 'Descending')
            logger.trace(f"执行虚拟库排序劫持: '{sort_by_field}' ({sort_order})")
            default_sort_value = 0 if sort_by_field in ['CommunityRating', 'ProductionYear'] else "0"
            try:
                final_items.sort(
                    key=lambda item: item.get(sort_by_field, default_sort_value),
                    reverse=is_descending
                )
            except TypeError:
                final_items.sort(key=lambda item: item.get('SortName', ''))

        final_response = {"Items": final_items, "TotalRecordCount": len(final_items)}
        return Response(json.dumps(final_response), mimetype='application/json')

    except Exception as e:
        logger.error(f"处理混合虚拟库时发生严重错误: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def handle_get_latest_items(user_id, params):
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')

        if virtual_library_id and is_mimicked_id(virtual_library_id): # <-- 【核心修改】
            logger.trace(f"处理针对虚拟库 '{virtual_library_id}' 的最新媒体请求...")
            try:
                virtual_library_db_id = from_mimicked_id(virtual_library_id) # <-- 【核心修改】
            except (ValueError, TypeError):
                return Response(json.dumps([]), mimetype='application/json')

            collection_info = db_handler.get_custom_collection_by_id(virtual_library_db_id)
            if not collection_info or not collection_info.get('emby_collection_id'):
                return Response(json.dumps([]), mimetype='application/json')

            real_emby_collection_id = collection_info.get('emby_collection_id')
            limit_value = params.get('Limit') or params.get('limit') or '20'
            
            latest_params = {
                "ParentId": real_emby_collection_id,
                "Limit": int(limit_value), # 使用我们兼容处理后的值
                "Fields": "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated",
                "SortBy": "DateCreated", 
                "SortOrder": "Descending",
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Series",
                'api_key': api_key,
            }
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            resp = requests.get(target_url, params=latest_params, timeout=15)
            resp.raise_for_status()
            items_data = resp.json()
            return Response(json.dumps(items_data.get("Items", [])), mimetype='application/json')
        else:
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

    # --- 2. HTTP 代理逻辑 (保持不变) ---
    try:
        # 1. 定义所有需要“翻译”ParentId的元数据端点
        METADATA_ENDPOINTS = [
            '/Items/Prefixes', '/Genres', '/Studios', 
            '/Tags', '/OfficialRatings', '/Years'
        ]

        # 2. 优先处理所有元数据请求
        if any(path.endswith(endpoint) for endpoint in METADATA_ENDPOINTS):
            parent_id = request.args.get("ParentId")
            if parent_id and is_mimicked_id(parent_id):
                # 所有这类请求，都交给我们的“万能翻译”函数处理
                return handle_mimicked_library_metadata_endpoint(path, parent_id, request.args)

        # 3. 其次，处理获取库“内容”的请求 (这个逻辑我们之前已经修复好了)
        parent_id = request.args.get("ParentId")
        if parent_id and is_mimicked_id(parent_id):
            user_id_match = re.search(r'/emby/Users/([^/]+)/Items', request.path)
            if user_id_match:
                user_id = user_id_match.group(1)
                return handle_get_mimicked_library_items(user_id, parent_id, request.args)
            else:
                # 这条日志现在只会在极少数未知情况下出现，是我们的最后防线
                logger.warning(f"无法从路径 '{request.path}' 中为虚拟库请求提取user_id。")

        if path.startswith('emby/Items/') and '/Images/' in path and is_mimicked_id(path.split('/')[2]):
             return handle_get_mimicked_library_image(path)

        # 检查是否是请求主页媒体库列表
        if path.endswith('/Views') and path.startswith('emby/Users/'):
            return handle_get_views()

        # 检查是否是请求虚拟库的详情
        details_match = MIMICKED_ITEM_DETAILS_RE.search(f'/{path}')
        if details_match:
            user_id = details_match.group(1)
            mimicked_id = details_match.group(2) # 注意，这里是 group(2)
            return handle_get_mimicked_library_details(user_id, mimicked_id)

        # 检查是否是请求最新项目
        if path.endswith('/Items/Latest'):
            user_id_match = re.search(r'/emby/Users/([^/]+)/', f'/{path}')
            if user_id_match:
                return handle_get_latest_items(user_id_match.group(1), request.args)

        # 捕获所有对虚拟库内容的请求
        items_match = MIMICKED_ITEMS_RE.match(f'/{path}')
        if items_match:
            user_id = items_match.group(1)
            mimicked_id = items_match.group(2) # 注意，这里是 group(2)
            return handle_get_mimicked_library_items(user_id, mimicked_id, request.args)

        # --- 默认转发逻辑 (保持不变) ---
        logger.warning(f"反代服务收到了一个未处理的请求: '{path}'。这通常意味着Nginx配置有误，请检查路由规则。")
        return Response("Path not handled by virtual library proxy.", status=404, mimetype='text/plain')
    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500