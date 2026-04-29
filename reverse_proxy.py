# reverse_proxy.py (最终完美版 V5 - 实时架构适配)

import logging
import requests
import re
import os
import json
from flask import Flask, request, Response, redirect, send_file
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timedelta
import time
import uuid 
from flask import send_file 
from handler.poster_generator import get_missing_poster
from gevent import spawn, joinall
from websocket import create_connection
from database import custom_collection_db, queries_db, media_db
from database.connection import get_db_connection
from handler.custom_collection import RecommendationEngine
import config_manager
import constants
from handler.p115_service import P115Service
from utils import extract_pickcode_from_strm_url

import extensions
import handler.emby as emby
logger = logging.getLogger(__name__)

MISSING_ID_PREFIX = "-800000_"

def to_missing_item_id(tmdb_id): 
    return f"{MISSING_ID_PREFIX}{tmdb_id}"

def is_missing_item_id(item_id):
    return isinstance(item_id, str) and item_id.startswith(MISSING_ID_PREFIX)

def parse_missing_item_id(item_id):
    # 从 -800000_12345 中提取出 12345
    return item_id.replace(MISSING_ID_PREFIX, "")
MIMICKED_ID_BASE = 900000
def to_mimicked_id(db_id): return str(-(MIMICKED_ID_BASE + db_id))
def from_mimicked_id(mimicked_id): return -(int(mimicked_id)) - MIMICKED_ID_BASE
def is_mimicked_id(item_id):
    try: return isinstance(item_id, str) and item_id.startswith('-')
    except: return False
MIMICKED_ITEMS_RE = re.compile(r'/emby/Users/([^/]+)/Items/(-(\d+))')
MIMICKED_ITEM_DETAILS_RE = re.compile(r'emby/Users/([^/]+)/Items/(-(\d+))$')

# ============================================================================
# 魔法日志：专门追踪 Emby 4.10 虚拟库入口、响应、注入结果
# 关闭方式：APP_CONFIG['proxy_magic_log'] = False
# ============================================================================
PROXY_MAGIC_PREFIX = "[PROXY-MAGIC]"

def _proxy_magic_enabled():
    return bool(config_manager.APP_CONFIG.get('proxy_magic_log', True))

def _redact_magic_value(key, value):
    key_l = str(key or "").lower()
    if any(x in key_l for x in ("token", "api_key", "apikey", "authorization", "x-emby-token")):
        return "***"
    if value is None:
        return value
    value = str(value)
    if len(value) > 180:
        return value[:180] + "...<cut>"
    return value

def _magic_args_dict(args=None):
    args = args or request.args
    try:
        return {k: _redact_magic_value(k, v) for k, v in args.items()}
    except Exception:
        return {}

def _magic_header_dict():
    watch_keys = (
        "X-Emby-Client", "X-Emby-Client-Version", "X-Emby-Device-Name",
        "X-Emby-Device-Id", "User-Agent", "Accept"
    )
    result = {}
    try:
        for k in watch_keys:
            v = request.headers.get(k)
            if v:
                result[k] = _redact_magic_value(k, v)
    except Exception:
        pass
    return result

def _magic_should_trace(path=None, args=None):
    path = (path or request.path or "").lower()
    args = args or request.args
    if any(x in path for x in (
        "/views", "/items", "/configuration", "/homescreen", "/home",
        "/users/", "/displaypreferences", "/scheduledtasks", "/system/info"
    )):
        return True

    joined_args = " ".join([str(k) + "=" + str(v) for k, v in (args or {}).items()]).lower()
    return any(x in joined_args for x in (
        "collectionfolder", "parentid", "includeitemtypes", "home", "orderedviews", "latestitemsexcludes"
    ))

def _magic_item_summary(items, limit=12):
    if not isinstance(items, list):
        return {"kind": type(items).__name__}

    summary = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            summary.append(str(type(item).__name__))
            continue
        summary.append({
            "Id": str(item.get("Id", ""))[:80],
            "Name": item.get("Name"),
            "Type": item.get("Type"),
            "CollectionType": item.get("CollectionType"),
            "ParentId": item.get("ParentId"),
            "LocationType": item.get("LocationType"),
        })

    type_counts = {}
    for item in items:
        if isinstance(item, dict):
            t = item.get("Type") or "?"
            type_counts[t] = type_counts.get(t, 0) + 1

    return {
        "count": len(items),
        "type_counts": type_counts,
        "first_items": summary,
    }

def _magic_log(stage, **kwargs):
    if not _proxy_magic_enabled():
        return

    try:
        safe = {}
        for k, v in kwargs.items():
            if k in ("args", "query"):
                safe[k] = {kk: _redact_magic_value(kk, vv) for kk, vv in dict(v).items()}
            elif k == "headers":
                safe[k] = {kk: _redact_magic_value(kk, vv) for kk, vv in dict(v).items()}
            elif isinstance(v, (dict, list)):
                safe[k] = v
            else:
                safe[k] = _redact_magic_value(k, v)

        logger.warning("%s %s %s", PROXY_MAGIC_PREFIX, stage, json.dumps(safe, ensure_ascii=False, default=str))
    except Exception as e:
        logger.warning("%s %s <log failed: %s>", PROXY_MAGIC_PREFIX, stage, e)

def _magic_log_request(stage="REQ", path=None):
    if not _proxy_magic_enabled():
        return
    if not _magic_should_trace(path or request.path, request.args):
        return

    _magic_log(
        stage,
        method=request.method,
        path=path or request.path,
        full_path=request.full_path,
        args=_magic_args_dict(),
        headers=_magic_header_dict(),
        remote_addr=request.headers.get("X-Forwarded-For") or request.remote_addr,
    )

def _magic_log_json_response(stage, data, status_code=None):
    if not _proxy_magic_enabled():
        return
    try:
        payload = {
            "status": status_code,
            "data_type": type(data).__name__,
        }
        if isinstance(data, dict):
            payload.update({
                "keys": list(data.keys())[:30],
                "TotalRecordCount": data.get("TotalRecordCount"),
                "Items": _magic_item_summary(data.get("Items")) if "Items" in data else None,
                "OrderedViews_len": len(data.get("OrderedViews")) if isinstance(data.get("OrderedViews"), list) else None,
                "HomeSections_len": len(data.get("HomeSections")) if isinstance(data.get("HomeSections"), list) else None,
                "HomeScreenSections_len": len(data.get("HomeScreenSections")) if isinstance(data.get("HomeScreenSections"), list) else None,
            })
        elif isinstance(data, list):
            payload["Items"] = _magic_item_summary(data)
        _magic_log(stage, **payload)
    except Exception as e:
        _magic_log(stage, error=f"json response log failed: {e}")


def _get_real_emby_url_and_key():
    base_url = config_manager.APP_CONFIG.get("emby_server_url", "").rstrip('/')
    api_key = config_manager.APP_CONFIG.get("emby_api_key", "")
    if not base_url or not api_key: raise ValueError("Emby服务器地址或API Key未配置")
    return base_url, api_key

def _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields):
    """
    并发分块获取 Emby 项目详情。
    """
    if not item_ids: return []
    
    # 去重
    unique_ids = list(dict.fromkeys(item_ids))
    
    def chunk_list(lst, n):
        for i in range(0, len(lst), n): yield lst[i:i + n]
    
    # 适当增大分块大小以减少请求数
    id_chunks = list(chunk_list(unique_ids, 200))
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
    [榜单类专用] 
    当我们需要对一组固定的 ID (来自榜单) 进行排序和分页时使用。
    利用 Emby 的 GET 请求能力，让 Emby 帮我们过滤权限并排序。
    如果 ID 太多，回退到内存排序。
    """
    base_url, api_key = _get_real_emby_url_and_key()
    
    # 估算 URL 长度
    estimated_ids_length = len(item_ids) * 33 # GUID 长度 + 逗号
    URL_LENGTH_THRESHOLD = 1800 # 保守阈值

    try:
        if estimated_ids_length < URL_LENGTH_THRESHOLD:
            # --- 路径 A: ID列表较短，直接请求 Emby (最快，且自动处理权限) ---
            logger.trace(f"  ➜ [Emby 代理排序] ID列表较短 ({len(item_ids)}个)，使用 GET 方法。")
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            emby_params = {
                'api_key': api_key, 'Ids': ",".join(item_ids), 'Fields': fields,
                'SortBy': sort_by, 'SortOrder': sort_order,
                'StartIndex': offset, 'Limit': limit,
            }
            resp = requests.get(target_url, params=emby_params, timeout=25)
            resp.raise_for_status()
            emby_data = resp.json()
            # 注意：Emby 返回的 TotalRecordCount 是经过权限过滤后的数量
            # 如果我们传入的 total_record_count 是全量的，这里可能需要修正，但为了分页条正常，通常直接用 Emby 返回的
            return emby_data
        else:
            # --- 路径 B: ID列表超长，内存排序 (安全回退) ---
            logger.trace(f"  ➜ [内存排序回退] ID列表超长 ({len(item_ids)}个)，启动内存排序。")
            
            # 1. 获取所有项目的详情 (Emby 会自动过滤掉无权访问的项目)
            # 我们需要获取用于排序的字段
            primary_sort_by = sort_by.split(',')[0]
            fields_for_sorting = f"{fields},{primary_sort_by}"
            
            all_items_details = _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields_for_sorting)
            
            # 更新总数 (过滤后的真实数量)
            real_total_count = len(all_items_details)

            # 2. 在内存中排序
            try:
                is_desc = sort_order == 'Descending'
                
                def get_sort_val(item):
                    val = item.get(primary_sort_by)
                    # 处理日期
                    if 'Date' in primary_sort_by or 'Year' in primary_sort_by:
                        return val or "1900-01-01T00:00:00.000Z"
                    # 处理数字
                    if 'Rating' in primary_sort_by or 'Count' in primary_sort_by:
                        return float(val) if val is not None else 0
                    # 处理字符串
                    return str(val or "").lower()

                all_items_details.sort(key=get_sort_val, reverse=is_desc)
            except Exception as sort_e:
                logger.error(f"  ➜ 内存排序时发生错误: {sort_e}", exc_info=True)
            
            # 3. 在内存中分页
            paginated_items = all_items_details[offset : offset + limit]
            
            return {"Items": paginated_items, "TotalRecordCount": real_total_count}

    except Exception as e:
        logger.error(f"  ➜ Emby代理排序或内存回退时失败: {e}", exc_info=True)
        return {"Items": [], "TotalRecordCount": 0}


# ============================================================================
# Emby 4.10 兼容层：虚拟库不再只依赖 /Users/{id}/Views
# 4.10 首页/媒体库入口会通过 /Users/{id}/Items?IncludeItemTypes=CollectionFolder
# 或用户配置 HomeSections/OrderedViews 来决定显示内容。
# ============================================================================
def _get_arg(params, *names, default=""):
    for name in names:
        value = params.get(name)
        if value is not None:
            return value
    return default


def _split_csv(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            result.extend(_split_csv(item))
        return result
    return [x.strip() for x in str(value).split(',') if x.strip()]


def _build_fake_view_items_for_user(user_id):
    """
    生成当前用户可见的虚拟媒体库 CollectionFolder 列表。
    独立成函数，供老 /Views 和 Emby 4.10 新 CollectionFolder 入口共用。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        _magic_log("FAKE-BUILD-NO-SERVER-ID", user_id=user_id)
        return []

    fake_views_items = []
    collections = custom_collection_db.get_all_active_custom_collections()
    _magic_log("FAKE-BUILD-START", user_id=user_id, server_id=real_server_id, collection_count=len(collections or []))

    for coll in collections:
        real_emby_collection_id = coll.get('emby_collection_id')
        if not real_emby_collection_id:
            _magic_log("FAKE-SKIP-NO-REAL-EMBY-COLLECTION", collection_id=coll.get('id'), name=coll.get('name'))
            continue

        allowed_users = coll.get('allowed_user_ids')
        if allowed_users and isinstance(allowed_users, list) and user_id not in allowed_users:
            _magic_log("FAKE-SKIP-NO-USER-PERMISSION", collection_id=coll.get('id'), name=coll.get('name'), user_id=user_id, allowed_users=allowed_users)
            continue

        db_id = coll['id']
        mimicked_id = to_mimicked_id(db_id)
        image_tags = {"Primary": f"{real_emby_collection_id}?timestamp={int(time.time())}"}
        definition = coll.get('definition_json') or {}
        if isinstance(definition, str):
            try:
                definition = json.loads(definition)
            except Exception:
                definition = {}

        item_type_from_db = definition.get('item_type', 'Movie')
        collection_type = "mixed"
        if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
            authoritative_type = (
                item_type_from_db[0]
                if isinstance(item_type_from_db, list) and item_type_from_db
                else item_type_from_db if isinstance(item_type_from_db, str)
                else 'Movie'
            )
            collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

        _magic_log("FAKE-ADD", collection_id=db_id, name=coll.get('name'), mimicked_id=mimicked_id, real_emby_collection_id=real_emby_collection_id, collection_type=collection_type)

        fake_views_items.append({
            "Name": coll['name'],
            "ServerId": real_server_id,
            "Id": mimicked_id,
            "Guid": str(uuid.uuid5(uuid.NAMESPACE_URL, f"etk-virtual-view-{db_id}")),
            "Etag": f"{db_id}{int(time.time())}",
            "DateCreated": "2025-01-01T00:00:00.0000000Z",
            "CanDelete": False,
            "CanDownload": False,
            "SortName": coll['name'],
            "ExternalUrls": [],
            "Path": "",
            "EnableMediaSourceDisplay": True,
            "ChannelId": None,
            "Taglines": [],
            "Genres": [],
            "PlayAccess": "Full",
            "RemoteTrailers": [],
            "ProviderIds": {},
            "IsFolder": True,
            "ParentId": "2",
            "Type": "CollectionFolder",
            "PresentationUniqueKey": f"etk-virtual-{db_id}",
            "DisplayPreferencesId": f"custom-{db_id}",
            "ForcedSortName": coll['name'],
            "UserData": {
                "PlaybackPositionTicks": 0,
                "PlayCount": 0,
                "IsFavorite": False,
                "Played": False,
            },
            "People": [],
            "Studios": [],
            "GenreItems": [],
            "LocalTrailerCount": 0,
            "ChildCount": coll.get('in_library_count', 1),
            "RecursiveItemCount": coll.get('in_library_count', 1),
            "PrimaryImageAspectRatio": 1.7777777777777777,
            "CollectionType": collection_type,
            "ImageTags": image_tags,
            "BackdropImageTags": [],
            "LockedFields": [],
            "LockData": False,
            "MediaType": "Unknown",
            "LocationType": "Virtual",
        })

    _magic_log("FAKE-BUILD-DONE", user_id=user_id, fake_count=len(fake_views_items), fake_items=_magic_item_summary(fake_views_items))
    return fake_views_items


def _merge_native_and_fake_views(native_views_items, fake_views_items):
    """
    保持原 /Views 的排序和原生库合并逻辑，避免 Emby 4.10 新入口和 4.9 老入口表现不一致。
    """
    native_views_items = native_views_items or []
    fake_views_items = fake_views_items or []

    _magic_log(
        "MERGE-START",
        native_count=len(native_views_items),
        fake_count=len(fake_views_items),
        native_items=_magic_item_summary(native_views_items),
        fake_items=_magic_item_summary(fake_views_items),
        proxy_merge_native_libraries=config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True),
        proxy_native_view_selection=config_manager.APP_CONFIG.get('proxy_native_view_selection', ''),
        proxy_native_view_order=config_manager.APP_CONFIG.get('proxy_native_view_order', 'before'),
    )

    selected_native_items = []
    should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
    if should_merge_native:
        raw_selection = config_manager.APP_CONFIG.get('proxy_native_view_selection', '')
        selected_native_view_ids = (
            [x.strip() for x in raw_selection.split(',') if x.strip()]
            if isinstance(raw_selection, str)
            else (raw_selection or [])
        )

        # 兼容原逻辑：只有明确选择的原生库才与虚拟库合并。
        if selected_native_view_ids:
            selected_set = {str(x) for x in selected_native_view_ids}
            selected_native_items = [view for view in native_views_items if str(view.get("Id")) in selected_set]
        else:
            selected_native_items = []

    final_items = []
    native_order = config_manager.APP_CONFIG.get('proxy_native_view_order', 'before')
    if native_order == 'after':
        final_items.extend(fake_views_items)
        final_items.extend(selected_native_items)
    else:
        final_items.extend(selected_native_items)
        final_items.extend(fake_views_items)

    seen_ids = set()
    deduped = []
    for item in final_items:
        item_id = str(item.get("Id", ""))
        if not item_id or item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        deduped.append(item)

    _magic_log("MERGE-DONE", final_count=len(deduped), final_items=_magic_item_summary(deduped))
    return deduped


def _is_collection_folder_list_query(params):
    include_types = ",".join(_split_csv(_get_arg(params, 'IncludeItemTypes', 'includeItemTypes'))).lower()
    parent_id = str(_get_arg(params, 'ParentId', 'parentId', default='') or '')

    if 'collectionfolder' not in include_types:
        _magic_log("COLLECTION-FOLDER-CHECK-NO", reason="IncludeItemTypes missing CollectionFolder", include_types=include_types, parent_id=parent_id)
        return False

    # 根层媒体库列表才注入；普通库内浏览不要误伤。
    if parent_id and parent_id.lower() not in {'2', 'root'}:
        _magic_log("COLLECTION-FOLDER-CHECK-NO", reason="ParentId is not root", include_types=include_types, parent_id=parent_id)
        return False

    _magic_log("COLLECTION-FOLDER-CHECK-YES", include_types=include_types, parent_id=parent_id)
    return True


def _forward_emby_json(path, params=None, timeout=20):
    base_url, api_key = _get_real_emby_url_and_key()
    target_url = f"{base_url}/{path.lstrip('/')}"
    _magic_log("FORWARD-JSON-REQ", target_url=target_url, path=path, args=_magic_args_dict(params or request.args))
    headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
    headers['Host'] = urlparse(base_url).netloc
    forward_params = (params or request.args).copy()
    forward_params['api_key'] = api_key

    resp = requests.request(
        method=request.method,
        url=target_url,
        headers=headers,
        params=forward_params,
        data=request.get_data(),
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    _magic_log_json_response("FORWARD-JSON-RESP", data, status_code=resp.status_code)
    return resp, data


def handle_get_collection_folder_items_410(user_id, path, params):
    _magic_log("ROUTE-MATCH-410-COLLECTION-FOLDERS", user_id=user_id, path=path, args=_magic_args_dict(params))
    """
    Emby 4.10 首页/媒体库页兼容：
    新版客户端可能不再依赖 /Users/{id}/Views，而是请求 /Users/{id}/Items + IncludeItemTypes=CollectionFolder。
    这里把虚拟库注入到新版 CollectionFolder 响应里。
    """
    try:
        resp, data = _forward_emby_json(path, params=params, timeout=20)
        native_items = data.get('Items', []) if isinstance(data, dict) else []

        _magic_log_json_response("410-NATIVE-COLLECTION-FOLDERS", data, status_code=resp.status_code)
        fake_items = _build_fake_view_items_for_user(user_id)
        final_items = _merge_native_and_fake_views(native_items, fake_items)

        if not isinstance(data, dict):
            data = {"Items": final_items, "TotalRecordCount": len(final_items)}
        else:
            data['Items'] = final_items
            data['TotalRecordCount'] = len(final_items)

        _magic_log_json_response("410-INJECTED-COLLECTION-FOLDERS", data, status_code=resp.status_code)
        logger.debug(f"  ➜ [Emby4.10] 已向 CollectionFolder 列表注入 {len(fake_items)} 个虚拟库。")
        return Response(json.dumps(data), status=resp.status_code, mimetype='application/json')

    except Exception as e:
        logger.error(f"  ➜ [Emby4.10] 注入 CollectionFolder 虚拟库失败: {e}", exc_info=True)
        fake_items = _build_fake_view_items_for_user(user_id)
        final_items = _merge_native_and_fake_views([], fake_items)
        return Response(json.dumps({"Items": final_items, "TotalRecordCount": len(final_items)}), mimetype='application/json')


def handle_get_mimicked_items_by_ids(user_id, ids_value):
    _magic_log("ROUTE-MATCH-MIMICKED-IDS", user_id=user_id, ids=ids_value)
    """
    兼容 Emby 4.10 可能出现的 /Users/{id}/Items?Ids=-900001 详情请求。
    """
    ids = _split_csv(ids_value)
    items = []

    for item_id in ids:
        if not is_mimicked_id(item_id):
            continue

        try:
            real_db_id = from_mimicked_id(item_id)
            coll = custom_collection_db.get_custom_collection_by_id(real_db_id)
            if not coll:
                continue

            allowed_users = coll.get('allowed_user_ids')
            if allowed_users and isinstance(allowed_users, list) and user_id not in allowed_users:
                continue

            real_server_id = extensions.EMBY_SERVER_ID
            real_emby_collection_id = coll.get('emby_collection_id')
            image_tags = {"Primary": real_emby_collection_id} if real_emby_collection_id else {}
            definition = coll.get('definition_json') or {}
            if isinstance(definition, str):
                try:
                    definition = json.loads(definition)
                except Exception:
                    definition = {}

            item_type_from_db = definition.get('item_type', 'Movie')
            collection_type = "mixed"
            if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
                authoritative_type = (
                    item_type_from_db[0]
                    if isinstance(item_type_from_db, list) and item_type_from_db
                    else item_type_from_db if isinstance(item_type_from_db, str)
                    else 'Movie'
                )
                collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

            items.append({
                "Name": coll['name'],
                "ServerId": real_server_id,
                "Id": item_id,
                "Type": "CollectionFolder",
                "CollectionType": collection_type,
                "IsFolder": True,
                "ImageTags": image_tags,
                "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
                "ChildCount": coll.get('in_library_count', 1),
                "RecursiveItemCount": coll.get('in_library_count', 1),
                "PrimaryImageAspectRatio": 1.7777777777777777,
                "DisplayPreferencesId": f"custom-{real_db_id}",
                "LocationType": "Virtual",
            })
        except Exception as e:
            logger.warning(f"  ➜ 构造虚拟库详情失败: {item_id} -> {e}")

    data = {"Items": items, "TotalRecordCount": len(items)}
    _magic_log_json_response("MIMICKED-IDS-RESP", data)
    return Response(json.dumps(data), mimetype='application/json')


def _remove_ids_from_list(value, ids_to_remove):
    if not isinstance(value, list):
        return value
    return [x for x in value if str(x) not in ids_to_remove]


def _patch_user_configuration_for_virtual_views(config_data, fake_view_ids):
    _magic_log(
        "PATCH-CONFIG-START",
        fake_view_ids=fake_view_ids,
        config_type=type(config_data).__name__,
        keys=list(config_data.keys())[:50] if isinstance(config_data, dict) else None,
        OrderedViews=config_data.get('OrderedViews') if isinstance(config_data, dict) else None,
        LatestItemsExcludes=config_data.get('LatestItemsExcludes') if isinstance(config_data, dict) else None,
        MyMediaExcludes=config_data.get('MyMediaExcludes') if isinstance(config_data, dict) else None,
    )
    """
    Emby 4.10 首页配置兼容。
    新版首页可能根据 OrderedViews / HomeSections 决定展示媒体库，确保虚拟库不被排除。
    """
    if not isinstance(config_data, dict) or not fake_view_ids:
        return config_data

    fake_view_ids = [str(x) for x in fake_view_ids]
    fake_view_id_set = set(fake_view_ids)

    ordered = config_data.get('OrderedViews')
    if isinstance(ordered, list):
        current = {str(x) for x in ordered}
        for vid in fake_view_ids:
            if vid not in current:
                ordered.append(vid)
                current.add(vid)

    for key in ('LatestItemsExcludes', 'MyMediaExcludes', 'HidePlayedInLatest'):
        if key in config_data:
            config_data[key] = _remove_ids_from_list(config_data.get(key), fake_view_id_set)

    for sections_key in ('HomeSections', 'HomeScreenSections', 'Sections'):
        sections = config_data.get(sections_key)
        if not isinstance(sections, list):
            continue

        for section in sections:
            if not isinstance(section, dict):
                continue
            for exclude_key in ('LibraryIdsToExclude', 'ExcludedLibraryIds', 'ViewIdsToExclude'):
                if exclude_key in section:
                    section[exclude_key] = _remove_ids_from_list(section.get(exclude_key), fake_view_id_set)

    _magic_log(
        "PATCH-CONFIG-DONE",
        fake_view_ids=fake_view_ids,
        OrderedViews=config_data.get('OrderedViews') if isinstance(config_data, dict) else None,
        LatestItemsExcludes=config_data.get('LatestItemsExcludes') if isinstance(config_data, dict) else None,
        MyMediaExcludes=config_data.get('MyMediaExcludes') if isinstance(config_data, dict) else None,
    )
    return config_data


def handle_get_user_configuration_410(user_id, path, params):
    _magic_log("ROUTE-MATCH-410-USER-CONFIG", user_id=user_id, path=path, args=_magic_args_dict(params))
    try:
        resp, data = _forward_emby_json(path, params=params, timeout=20)
        fake_ids = [item.get('Id') for item in _build_fake_view_items_for_user(user_id) if item.get('Id')]
        patched = _patch_user_configuration_for_virtual_views(data, fake_ids)
        _magic_log_json_response("410-USER-CONFIG-PATCHED", patched, status_code=resp.status_code)
        return Response(json.dumps(patched), status=resp.status_code, mimetype='application/json')
    except Exception as e:
        logger.warning(f"  ➜ [Emby4.10] 用户配置虚拟库兼容处理失败，回退透传: {e}")
        base_url, api_key = _get_real_emby_url_and_key()
        target_url = f"{base_url}/{path.lstrip('/')}"
        forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        forward_headers['Host'] = urlparse(base_url).netloc
        forward_params = params.copy()
        forward_params['api_key'] = api_key
        resp = requests.request(method=request.method, url=target_url, headers=forward_headers, params=forward_params, data=request.get_data(), stream=True, timeout=(10.0, 1800.0))
        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)

def handle_get_views():
    """
    获取用户的主页视图列表。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        return "Proxy is not ready", 503

    try:
        _magic_log("ROUTE-MATCH-VIEWS", request_path=request.path, full_path=request.full_path, args=_magic_args_dict())
        user_id_match = re.search(r'/(?:emby/)?Users/([^/]+)/Views', request.path, re.IGNORECASE)
        if not user_id_match:
            _magic_log("VIEWS-NO-USER-ID", request_path=request.path)
            return "Could not determine user from request path", 400
        user_id = user_id_match.group(1)
        _magic_log("VIEWS-USER", user_id=user_id)

        # 1. 获取原生库
        user_visible_native_libs = emby.get_emby_libraries(
            config_manager.APP_CONFIG.get("emby_server_url", ""),
            config_manager.APP_CONFIG.get("emby_api_key", ""),
            user_id
        )
        if user_visible_native_libs is None: user_visible_native_libs = []
        _magic_log("VIEWS-NATIVE", user_id=user_id, native_count=len(user_visible_native_libs), native_items=_magic_item_summary(user_visible_native_libs))

        # 2. 生成虚拟库
        collections = custom_collection_db.get_all_active_custom_collections()
        _magic_log("VIEWS-COLLECTIONS", user_id=user_id, collection_count=len(collections or []))
        fake_views_items = []
        
        for coll in collections:
            # 物理检查：库在Emby里有实体吗？
            real_emby_collection_id = coll.get('emby_collection_id')
            if not real_emby_collection_id:
                _magic_log("VIEWS-SKIP-NO-REAL-EMBY-COLLECTION", collection_id=coll.get('id'), name=coll.get('name'))
                continue

            # 权限检查：如果设置了 allowed_user_ids，则检查
            allowed_users = coll.get('allowed_user_ids')
            if allowed_users and isinstance(allowed_users, list):
                if user_id not in allowed_users:
                    _magic_log("VIEWS-SKIP-NO-USER-PERMISSION", collection_id=coll.get('id'), name=coll.get('name'), user_id=user_id, allowed_users=allowed_users)
                    continue
            
            # 生成虚拟库对象
            db_id = coll['id']
            mimicked_id = to_mimicked_id(db_id)
            # 使用时间戳强制刷新封面
            image_tags = {"Primary": f"{real_emby_collection_id}?timestamp={int(time.time())}"}
            definition = coll.get('definition_json') or {}
            
            item_type_from_db = definition.get('item_type', 'Movie')
            collection_type = "mixed"
            if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
                 authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
                 collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

            fake_view = {
                "Name": coll['name'], "ServerId": real_server_id, "Id": mimicked_id,
                "Guid": str(uuid.uuid4()), "Etag": f"{db_id}{int(time.time())}",
                "DateCreated": "2025-01-01T00:00:00.0000000Z", "CanDelete": False, "CanDownload": False,
                "SortName": coll['name'], "ExternalUrls": [], "ProviderIds": {}, "IsFolder": True,
                "ParentId": "2", "Type": "CollectionFolder", "PresentationUniqueKey": str(uuid.uuid4()),
                "DisplayPreferencesId": f"custom-{db_id}", "ForcedSortName": coll['name'],
                "Taglines": [], "RemoteTrailers": [],
                "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
                "ChildCount": coll.get('in_library_count', 1),
                "PrimaryImageAspectRatio": 1.7777777777777777, 
                "CollectionType": collection_type, "ImageTags": image_tags, "BackdropImageTags": [], 
                "LockedFields": [], "LockData": False
            }
            _magic_log("VIEWS-FAKE-ADD", collection_id=db_id, name=coll.get('name'), mimicked_id=mimicked_id, real_emby_collection_id=real_emby_collection_id, collection_type=collection_type)
            fake_views_items.append(fake_view)
        
        # 3. 合并与排序
        native_views_items = []
        should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
        if should_merge_native:
            all_native_views = user_visible_native_libs
            raw_selection = config_manager.APP_CONFIG.get('proxy_native_view_selection', '')
            selected_native_view_ids = [x.strip() for x in raw_selection.split(',') if x.strip()] if isinstance(raw_selection, str) else raw_selection
            
            if selected_native_view_ids:
                native_views_items = [view for view in all_native_views if view.get("Id") in selected_native_view_ids]
            else:
                native_views_items = []
        
        final_items = []
        native_order = config_manager.APP_CONFIG.get('proxy_native_view_order', 'before')
        if native_order == 'after':
            final_items.extend(fake_views_items)
            final_items.extend(native_views_items)
        else:
            final_items.extend(native_views_items)
            final_items.extend(fake_views_items)

        final_response = {"Items": final_items, "TotalRecordCount": len(final_items)}
        _magic_log_json_response("VIEWS-FINAL", final_response)
        return Response(json.dumps(final_response), mimetype='application/json')
        
    except Exception as e:
        logger.error(f"[PROXY] 获取视图数据时出错: {e}", exc_info=True)
        return "Internal Proxy Error", 500

def handle_get_mimicked_library_details(user_id, mimicked_id):
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        coll = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not coll: return "Not Found", 404

        real_server_id = extensions.EMBY_SERVER_ID
        real_emby_collection_id = coll.get('emby_collection_id')
        image_tags = {"Primary": real_emby_collection_id} if real_emby_collection_id else {}
        
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
        # '/Items/Prefixes', # Emby 不支持按前缀过滤虚拟库
        '/Genres',         
        '/Studios',        
        '/Tags',           
        '/OfficialRatings',
        '/Years'           
    ]

def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    处理虚拟库的元数据请求。
    """
    if any(path.endswith(endpoint) for endpoint in UNSUPPORTED_METADATA_ENDPOINTS):
        return Response(json.dumps([]), mimetype='application/json')

    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = custom_collection_db.get_custom_collection_by_id(real_db_id)
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
    【V8 - 实时架构 + 占位海报适配版 + 排序修复】
    支持：实时权限过滤、原生排序、榜单占位符、数量限制
    """
    try:
        # 1. 获取合集基础信息
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        definition = collection_info.get('definition_json') or {}
        if isinstance(definition, str):
            try: definition = json.loads(definition)
            except: definition = {}

        collection_type = collection_info.get('type')
        
        # 2. 获取分页和排序参数 (变量定义必须在此处)
        emby_limit = int(params.get('Limit', 50))
        offset = int(params.get('StartIndex', 0))
        
        defined_limit = definition.get('limit')
        if defined_limit:
            defined_limit = int(defined_limit)
        
        # --- 排序优先级逻辑 ---
        req_sort_by = params.get('SortBy')
        req_sort_order = params.get('SortOrder')
        
        defined_sort_by = definition.get('default_sort_by')
        defined_sort_order = definition.get('default_sort_order')

        # 逻辑：如果DB定义了且不是none，强制劫持；否则使用客户端请求
        if defined_sort_by and defined_sort_by != 'none':
            # 强制劫持模式
            sort_by = defined_sort_by
            sort_order = defined_sort_order or 'Descending'
            is_native_mode = False
        else:
            # 原生/客户端模式 (设置为 NONE 时)
            sort_by = req_sort_by or 'DateCreated'
            sort_order = req_sort_order or 'Descending'
            is_native_mode = True

        # 核心判断：是否需要 Emby 原生排序
        # 当使用原生排序(is_native_mode=True)时，如果排序字段不是数据库能完美处理的(如DateCreated)，
        # 必须强制走 Emby 代理排序。
        is_emby_proxy_sort_required = (
            collection_type in ['ai_recommendation', 'ai_recommendation_global'] or 
            'DateLastContentAdded' in sort_by or
            (is_native_mode and sort_by not in ['DateCreated', 'Random'])
        )

        # 3. 准备基础查询参数
        tmdb_ids_filter = None
        rules = definition.get('rules', [])
        logic = definition.get('logic', 'AND')
        item_types = definition.get('item_type', ['Movie'])
        target_library_ids = definition.get('target_library_ids', [])

        # 4. 分流处理逻辑
        
        # --- 场景 A: 榜单类 (需要处理占位符 + 严格权限过滤) ---
        if collection_type == 'list':
            show_placeholders = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_SHOW_MISSING_PLACEHOLDERS, False)
            raw_list_json = collection_info.get('generated_media_info_json')
            raw_list = json.loads(raw_list_json) if isinstance(raw_list_json, str) else (raw_list_json or [])
            
            if raw_list:
                # 1. 获取该榜单中所有涉及的 TMDb ID
                tmdb_ids_in_list = [str(i.get('tmdb_id')) for i in raw_list if i.get('tmdb_id')]
                
                # ★★★ 新增：获取父剧集映射，用于多季去重聚合 ★★★
                tmdb_to_parent_map = {}
                try:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                SELECT tmdb_id, COALESCE(parent_series_tmdb_id, tmdb_id) as series_id 
                                FROM media_metadata 
                                WHERE tmdb_id = ANY(%s)
                            """, (tmdb_ids_in_list,))
                            for row in cursor.fetchall():
                                tmdb_to_parent_map[str(row['tmdb_id'])] = str(row['series_id'])
                except Exception as e:
                    logger.error(f"获取父剧集映射失败: {e}")

                # 2. 【用户视图】获取当前用户有权看到的项目
                items_in_db, _ = queries_db.query_virtual_library_items(
                    rules=rules, logic=logic, user_id=user_id,
                    limit=2000, offset=0, 
                    sort_by='DateCreated', sort_order='Descending',
                    item_types=item_types, target_library_ids=target_library_ids,
                    tmdb_ids=tmdb_ids_in_list
                )
                
                # 3. 【全局视图】获取Emby中实际存在的项目（忽略用户权限，传入 user_id=None）
                global_existing_items, _ = queries_db.query_virtual_library_items(
                    rules=rules, logic=logic, user_id=None, 
                    limit=2000, offset=0,
                    item_types=item_types, target_library_ids=target_library_ids,
                    tmdb_ids=tmdb_ids_in_list
                )

                # 4. 建立映射表
                local_tmdb_map = {str(i['tmdb_id']): i['Id'] for i in items_in_db if i.get('tmdb_id')}
                local_emby_id_set = {str(i['Id']) for i in items_in_db}
                
                global_tmdb_set = {str(i['tmdb_id']) for i in global_existing_items if i.get('tmdb_id')}
                global_emby_id_set = {str(i['Id']) for i in global_existing_items}
                
                # ★★★ 新增：记录哪些剧集（Series）在库里至少有一季 ★★★
                series_with_existing_items = set()
                for tid in local_tmdb_map.keys():
                    series_with_existing_items.add(tmdb_to_parent_map.get(tid, tid))
                for tid in global_tmdb_set:
                    series_with_existing_items.add(tmdb_to_parent_map.get(tid, tid))

                # 5. 构造完整视图列表 (带严格去重逻辑)
                full_view_list = []
                seen_emby_ids = set()
                seen_series_tids = set()

                for raw_item in raw_list:
                    tid = str(raw_item.get('tmdb_id')) if raw_item.get('tmdb_id') else "None"
                    eid = str(raw_item.get('emby_id')) if raw_item.get('emby_id') else "None"

                    if (not tid or tid.lower() == "none") and (not eid or eid.lower() == "none"):
                        continue

                    series_tid = tmdb_to_parent_map.get(tid, tid) if tid != "None" else "None"

                    # ★ 提前拦截：如果这个剧集已经处理过了，直接跳过，防止多季重复导致 Emby 出现空白占位符
                    if series_tid != "None" and series_tid in seen_series_tids:
                        continue

                    added = False

                    # 分支 1: 用户有权查看
                    if tid != "None" and tid in local_tmdb_map:
                        real_eid = local_tmdb_map[tid]
                        if real_eid not in seen_emby_ids:
                            full_view_list.append({"is_missing": False, "id": real_eid, "tmdb_id": tid})
                            seen_emby_ids.add(real_eid)
                            added = True
                    elif eid != "None" and eid in local_emby_id_set:
                        if eid not in seen_emby_ids:
                            full_view_list.append({"is_missing": False, "id": eid, "tmdb_id": tid})
                            seen_emby_ids.add(eid)
                            added = True

                    # 分支 3: 项目存在于全局库，但用户无权查看 -> 【跳过，不显示占位符】
                    elif (tid != "None" and tid in global_tmdb_set) or (eid != "None" and eid in global_emby_id_set):
                        added = True # 标记为已处理，防止后续季变成占位符

                    # 分支 4: 项目确实缺失 -> 显示占位符
                    elif tid != "None":
                        # ★ 核心修复：如果当前季缺失，但该剧的其他季在库里，则跳过当前缺失季，等循环走到在库季时再展示
                        if series_tid in series_with_existing_items:
                            continue

                        if show_placeholders:
                            full_view_list.append({"is_missing": True, "tmdb_id": tid})
                            added = True

                    # 记录已处理的剧集 ID
                    if added and series_tid != "None":
                        seen_series_tids.add(series_tid)

                    if defined_limit and len(full_view_list) >= defined_limit:
                        break

                # 6. 分页
                paged_part = full_view_list[offset : offset + emby_limit]
                reported_total_count = len(full_view_list)

                # 7. 批量获取详情
                real_eids = [x['id'] for x in paged_part if not x['is_missing']]
                missing_tids = [x['tmdb_id'] for x in paged_part if x['is_missing']]
                
                status_map = queries_db.get_missing_items_metadata(missing_tids)
                
                base_url, api_key = _get_real_emby_url_and_key()
                full_fields = "PrimaryImageAspectRatio,ImageTags,HasPrimaryImage,ProviderIds,UserData,Name,ProductionYear,CommunityRating,Type"
                emby_details = _fetch_items_in_chunks(base_url, api_key, user_id, real_eids, full_fields)
                emby_map = {item['Id']: item for item in emby_details}

                final_items = []
                for entry in paged_part:
                    if not entry['is_missing']:
                        eid = entry['id']
                        if eid in emby_map:
                            final_items.append(emby_map[eid])
                    else:
                        # 占位符构造逻辑
                        tid = entry['tmdb_id']
                        meta = status_map.get(tid, {})
                        status = meta.get('subscription_status', 'WANTED')
                        db_item_type = meta.get('item_type', 'Movie')
                        
                        placeholder = {
                            "Name": meta.get('title', '未知内容'),
                            "ServerId": extensions.EMBY_SERVER_ID,
                            "Id": to_missing_item_id(tid),
                            "Type": db_item_type,
                            "ProductionYear": int(meta.get('release_year')) if meta.get('release_year') else None,
                            "ImageTags": {"Primary": f"missing_{status}_{tid}"},
                            "HasPrimaryImage": True,
                            "PrimaryImageAspectRatio": 0.6666666666666666,
                            "UserData": {"PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": False, "Played": False},
                            "ProviderIds": {"Tmdb": tid},
                            "LocationType": "Virtual"
                        }
                        r_date = meta.get('release_date')
                        r_year = meta.get('release_year')
                        if r_date:
                            try:
                                if hasattr(r_date, 'strftime'):
                                    placeholder["PremiereDate"] = r_date.strftime('%Y-%m-%dT00:00:00.0000000Z')
                                else:
                                    placeholder["PremiereDate"] = str(r_date)
                            except: pass
                        if "PremiereDate" not in placeholder and r_year:
                            placeholder["PremiereDate"] = f"{r_year}-01-01T00:00:00.0000000Z"
                        if db_item_type == 'Series':
                            placeholder["Status"] = "Released"

                        final_items.append(placeholder)
                
                return Response(json.dumps({"Items": final_items, "TotalRecordCount": reported_total_count}), mimetype='application/json')

        # --- 场景 B: 筛选/推荐类 (修复灰色占位符) ---
        else:
            if collection_type in ['ai_recommendation', 'ai_recommendation_global']:
                api_key = config_manager.APP_CONFIG.get("tmdb_api_key")
                if api_key:
                    engine = RecommendationEngine(api_key)
                    if collection_type == 'ai_recommendation':
                        candidate_pool = engine.generate_user_vector(user_id, limit=300, allowed_types=item_types)
                    else:
                        candidate_pool = engine.generate_global_vector(limit=300, allowed_types=item_types)
                    tmdb_ids_filter = [str(i['id']) for i in candidate_pool]

            # 执行 SQL 查询
            sql_limit = defined_limit if is_emby_proxy_sort_required and defined_limit else 5000 if is_emby_proxy_sort_required else min(emby_limit, defined_limit - offset) if (defined_limit and defined_limit > offset) else emby_limit
            sql_offset = 0 if is_emby_proxy_sort_required else offset
            sql_sort = 'Random' if 'ai_recommendation' in collection_type else sort_by

            items, total_count = queries_db.query_virtual_library_items(
                rules=rules, logic=logic, user_id=user_id,
                limit=sql_limit, offset=sql_offset,
                sort_by=sql_sort, sort_order=sort_order,
                item_types=item_types, target_library_ids=target_library_ids,
                tmdb_ids=tmdb_ids_filter
            )

            reported_total_count = min(total_count, defined_limit) if defined_limit else total_count

            if not items:
                return Response(json.dumps({"Items": [], "TotalRecordCount": reported_total_count}), mimetype='application/json')

            final_emby_ids = [i['Id'] for i in items]
            full_fields = "PrimaryImageAspectRatio,ImageTags,HasPrimaryImage,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount,BasicSyncInfo"

            if is_emby_proxy_sort_required:
                # 代理排序模式：将所有 ID 交给 Emby (或内存) 进行排序和分页
                sorted_data = _fetch_sorted_items_via_emby_proxy(
                    user_id, final_emby_ids, sort_by, sort_order, emby_limit, offset, full_fields, reported_total_count
                )
                return Response(json.dumps(sorted_data), mimetype='application/json')
            else:
                # SQL 排序模式：直接获取详情
                base_url, api_key = _get_real_emby_url_and_key()
                items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, final_emby_ids, full_fields)
                items_map = {item['Id']: item for item in items_from_emby}
                
                # 过滤掉 Emby 实际没有返回的项目
                final_items = [items_map[eid] for eid in final_emby_ids if eid in items_map]
                
                # --- 修复开始 ---
                expected_count = len(final_emby_ids)
                actual_count = len(final_items)
                
                if actual_count < expected_count:
                    diff = expected_count - actual_count
                    # 1. 先执行原本的减法修正
                    reported_total_count = max(0, reported_total_count - diff)
                    logger.debug(f"检测到权限过滤导致的数量差异: SQL={expected_count}, Emby={actual_count}. 初步修正 TotalRecordCount 为 {reported_total_count}")

                    # 2. 【新增】封底保险逻辑
                    if reported_total_count <= emby_limit:
                        reported_total_count = actual_count
                        logger.debug(f"修正后的总数小于分页限制，强制对齐 TotalRecordCount = {actual_count} 以消除灰块")

                return Response(json.dumps({"Items": final_items, "TotalRecordCount": reported_total_count}), mimetype='application/json')

    except Exception as e:
        logger.error(f"处理虚拟库 '{mimicked_id}' 失败: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def handle_get_latest_items(user_id, params):
    """
    获取最新项目。
    利用 queries_db 的排序能力，快速返回结果。
    【修复版】增加对榜单(list)和AI合集的类型判断，防止无规则合集泄露全局最新数据。
    """
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')
        limit = int(params.get('Limit', 20))
        fields = params.get('Fields', "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated,UserData")

        # --- 辅助函数：获取合集的过滤 ID ---
        def get_collection_filter_ids(coll_data):
            c_type = coll_data.get('type')
            # 1. 榜单类：必须限制在榜单包含的 TMDb ID 范围内
            if c_type == 'list':
                raw_json = coll_data.get('generated_media_info_json')
                raw_list = json.loads(raw_json) if isinstance(raw_json, str) else (raw_json or [])
                return [str(i.get('tmdb_id')) for i in raw_list if i.get('tmdb_id')]
            # 2. AI 推荐类：暂不支持“最新”视图 (因为是动态生成的)，返回一个不存在的 ID 防止泄露
            elif c_type in ['ai_recommendation', 'ai_recommendation_global']:
                return ["-1"] 
            # 3. 规则类：返回 None，表示不限制 ID，只走 Rules
            return None

        # 场景一：单个虚拟库的最新
        if virtual_library_id and is_mimicked_id(virtual_library_id):
            real_db_id = from_mimicked_id(virtual_library_id)
            collection_info = custom_collection_db.get_custom_collection_by_id(real_db_id)
            if not collection_info: return Response(json.dumps([]), mimetype='application/json')

            definition = collection_info.get('definition_json') or {}
            if isinstance(definition, str): definition = json.loads(definition)
            
            if not definition.get('show_in_latest', True):
                return Response(json.dumps([]), mimetype='application/json')

            # --- 修复核心：获取 ID 过滤器 ---
            tmdb_ids_filter = get_collection_filter_ids(collection_info)
            # 如果是 AI 合集返回了 ["-1"]，或者榜单为空，直接返回空结果
            if tmdb_ids_filter is not None and (len(tmdb_ids_filter) == 0 or tmdb_ids_filter == ["-1"]):
                 return Response(json.dumps([]), mimetype='application/json')

            # 确定排序
            item_types = definition.get('item_type', ['Movie'])
            is_series_only = isinstance(item_types, list) and len(item_types) == 1 and item_types[0] == 'Series'
            sort_by = 'DateLastContentAdded,DateCreated' if is_series_only else 'DateCreated'

            # SQL 过滤权限和规则
            items, total_count = queries_db.query_virtual_library_items(
                rules=definition.get('rules', []), logic=definition.get('logic', 'AND'),
                user_id=user_id, limit=500, offset=0,
                sort_by='DateCreated', sort_order='Descending',
                item_types=item_types, target_library_ids=definition.get('target_library_ids', []),
                tmdb_ids=tmdb_ids_filter  # <--- 传入 TMDb ID 限制
            )
            
            if not items: return Response(json.dumps([]), mimetype='application/json')
            final_emby_ids = [i['Id'] for i in items]

            # 统一调用代理排序
            sorted_data = _fetch_sorted_items_via_emby_proxy(
                user_id, final_emby_ids, sort_by, 'Descending', limit, 0, fields, len(final_emby_ids)
            )
            return Response(json.dumps(sorted_data.get("Items", [])), mimetype='application/json')

        # 场景二：全局最新 (所有可见合集的聚合)
        elif not virtual_library_id:
            # 获取所有开启了“显示最新”的合集 ID
            included_collection_ids = custom_collection_db.get_active_collection_ids_for_latest_view()
            if not included_collection_ids:
                return Response(json.dumps([]), mimetype='application/json')
            
            all_latest = []
            for coll_id in included_collection_ids:
                coll = custom_collection_db.get_custom_collection_by_id(coll_id)
                if not coll: continue
                
                # 检查权限
                allowed_users = coll.get('allowed_user_ids')
                if allowed_users and user_id not in allowed_users: continue

                # --- 修复核心：获取 ID 过滤器 ---
                tmdb_ids_filter = get_collection_filter_ids(coll)
                if tmdb_ids_filter is not None and (len(tmdb_ids_filter) == 0 or tmdb_ids_filter == ["-1"]):
                    continue

                definition = coll.get('definition_json')
                items, _ = queries_db.query_virtual_library_items(
                    rules=definition.get('rules', []),
                    logic=definition.get('logic', 'AND'),
                    user_id=user_id,
                    limit=limit, 
                    offset=0,
                    sort_by='DateCreated',
                    sort_order='Descending',
                    item_types=definition.get('item_type', ['Movie']),
                    target_library_ids=definition.get('target_library_ids', []),
                    tmdb_ids=tmdb_ids_filter # <--- 传入 TMDb ID 限制
                )
                all_latest.extend(items)
            
            # 去重并获取详情
            unique_ids = list({i['Id'] for i in all_latest})
            if not unique_ids: return Response(json.dumps([]), mimetype='application/json')
            
            # 批量获取详情
            items_details = _fetch_items_in_chunks(base_url, api_key, user_id, unique_ids, "DateCreated")
            # 内存排序
            items_details.sort(key=lambda x: x.get('DateCreated', ''), reverse=True)
            # 截取
            latest_ids = [i['Id'] for i in items_details[:limit]]

        else:
            # 原生库请求，直接转发
            target_url = f"{base_url}/{request.path.lstrip('/')}"
            forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
            forward_headers['Host'] = urlparse(base_url).netloc
            forward_params = request.args.copy()
            forward_params['api_key'] = api_key
            resp = requests.request(method=request.method, url=target_url, headers=forward_headers, params=forward_params, data=request.get_data(), stream=True, timeout=30.0)
            excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
            response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
            return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)

        if not latest_ids:
            return Response(json.dumps([]), mimetype='application/json')

        # 获取最终详情
        items_from_emby = _fetch_items_in_chunks(base_url, api_key, user_id, latest_ids, fields)
        items_map = {item['Id']: item for item in items_from_emby}
        final_items = [items_map[id] for id in latest_ids if id in items_map]
        
        return Response(json.dumps(final_items), mimetype='application/json')

    except Exception as e:
        logger.error(f"  ➜ 处理最新媒体时发生未知错误: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')

proxy_app = Flask(__name__)

@proxy_app.route('/', defaults={'path': ''})
@proxy_app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
def proxy_all(path):
    # --- 1. 验证Pro ---
    if not config_manager.APP_CONFIG.get('is_pro_active', False):
        # 允许一些基础的系统级请求通过（比如健康检查），防止误杀
        if not path.startswith('api/health'):
            logger.warning(f"  ⚠️ [免费版限制] 您的 Pro 订阅已过期，反代服务已拒绝本次请求: /{path}")
            # 返回 403 Forbidden，让客户端明白自己没权限了
            return Response(
                json.dumps({"error": "Pro subscription expired or not activated. 302 Proxy service is disabled."}), 
                status=403, 
                mimetype='application/json'
            )
    # --- 2. WebSocket 代理逻辑 ---
    if 'Upgrade' in request.headers and request.headers.get('Upgrade', '').lower() == 'websocket':
        ws_client = request.environ.get('wsgi.websocket')
        if not ws_client: return "WebSocket upgrade failed", 400

        try:
            base_url, _ = _get_real_emby_url_and_key()
            parsed_url = urlparse(base_url)
            ws_scheme = 'wss' if parsed_url.scheme == 'https' else 'ws'
            target_ws_url = urlunparse((ws_scheme, parsed_url.netloc, f'/{path}', '', request.query_string.decode(), ''))
            
            headers_to_server = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'upgrade', 'connection', 'sec-websocket-key', 'sec-websocket-version']}
            ws_server = create_connection(target_ws_url, header=headers_to_server, timeout=10)

            def forward_to_server():
                try:
                    while not ws_client.closed and ws_server.connected:
                        message = ws_client.receive()
                        if message is not None: ws_server.send(message)
                        else: break
                except: pass
                finally: ws_server.close()

            def forward_to_client():
                try:
                    while ws_server.connected and not ws_client.closed:
                        message = ws_server.recv()
                        if message is not None: ws_client.send(message)
                        else: break
                except: pass
                finally: ws_client.close()
            
            greenlets = [spawn(forward_to_server), spawn(forward_to_client)]
            joinall(greenlets)
            
            # WebSocket 结束后返回空响应
            return Response()

        except Exception as e:
            logger.error(f"WebSocket 代理错误: {e}")
            return Response(status=500)

    # --- 3. HTTP 代理逻辑 ---
    try:
        full_path = f'/{path}'
        normalized_path = path[5:] if path.lower().startswith('emby/') else path
        # ===== 魔法日志：只打印 Emby 4.10 虚拟库相关请求 =====
        _magic_log_request("REQ", path=full_path)
        
        # ====================================================================
        # ★★★ 拦截 H: 视频流请求 (stream, original, Download 等) ★★★
        # ====================================================================
        full_path_lower = full_path.lower()
        
        if ('/videos/' in full_path_lower and ('/stream' in full_path_lower or '/original' in full_path_lower)) or ('/items/' in full_path_lower and '/download' in full_path_lower):
            
            # 检测浏览器客户端
            user_agent = request.headers.get('User-Agent', '').lower()
            client_name = request.headers.get('X-Emby-Client', '').lower()
            is_browser = 'mozilla' in user_agent or 'chrome' in user_agent or 'safari' in user_agent
            native_clients = ['androidtv', 'infuse', 'emby for ios', 'emby for android', 'emby theater', 'senplayer', 'applecoremedia']
            if any(nc in client_name for nc in native_clients) or 'infuse' in user_agent or 'dalvik' in user_agent or 'applecoremedia' in user_agent:
                is_browser = False
            
            # 浏览器直接转发给 Emby 服务端，不做 302 重定向（115 直链存在跨域问题）
            if is_browser:
                base_url, api_key = _get_real_emby_url_and_key()
                target_url = f"{base_url}/{path.lstrip('/')}"
                forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
                forward_headers['Host'] = urlparse(base_url).netloc
                forward_params = request.args.copy()
                forward_params['api_key'] = api_key
                resp = requests.request(method=request.method, url=target_url, headers=forward_headers, params=forward_params, data=request.get_data(), timeout=(10.0, 1800.0), stream=True)
                excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
                response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
                return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
            
            # 客户端处理逻辑
            match = re.search(r'/(?:videos|items)/(\d+)/', full_path_lower)
            item_id = match.group(1) if match else ''
            play_session_id = request.args.get('PlaySessionId', '')
            
            pick_code = None
            real_115_url = None
            display_name = "未知文件" # ★ 新增：用于记录人看的文件名
            base_url, api_key = _get_real_emby_url_and_key()

            try:
                playback_info_url = f"{base_url}/emby/Items/{item_id}/PlaybackInfo"
                params = {
                    'api_key': api_key,
                    'UserId': request.args.get('UserId', ''),
                    'MaxStreamingBitrate': 140000000,
                    'PlaySessionId': play_session_id,
                }
                
                forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
                forward_headers['Host'] = urlparse(base_url).netloc
                
                resp = requests.get(playback_info_url, params=params, headers=forward_headers, timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    for source in data.get('MediaSources', []):
                        strm_url = source.get('Path', '')
                        
                        # ★ 优先从 Emby 的数据源里提取友好的文件名
                        name_from_emby = source.get('Name', '')
                        if name_from_emby:
                            display_name = name_from_emby
                        elif isinstance(strm_url, str) and strm_url:
                            display_name = os.path.basename(strm_url).replace('.strm', '')

                        if isinstance(strm_url, str):
                            pick_code = extract_pickcode_from_strm_url(strm_url)
                            if not pick_code:
                                pick_code = media_db.get_pickcode_by_emby_id(item_id)
                            if pick_code:
                                break # 找到 pick_code，跳出循环
            except Exception as e:
                logger.error(f"  ❌ [STREAM] 获取 PlaybackInfo 失败: {e}")
            
            # ====================================================================
            # ★ 核心逻辑 1：如果是 115 文件，进入“115”模式，彻底干掉中转！
            # ====================================================================
            if pick_code:
                player_ua = request.headers.get('User-Agent', 'Mozilla/5.0')
                client = P115Service.get_client()
                if not client:
                    return "115 Client not initialized", 500

                max_retries = 10 
                retry_count = 0
                
                # ★ 动态读取配置，决定首次尝试的接口
                api_priority = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_PLAYBACK_API_PRIORITY, 'openapi')
                use_openapi = (api_priority != 'cookie') 

                while retry_count < max_retries:
                    try:
                        if use_openapi:
                            real_115_url = client.openapi_downurl(pick_code, user_agent=player_ua)
                        else:
                            real_115_url = client.download_url(pick_code, user_agent=player_ua)
                            
                        if real_115_url:
                            break 
                        else:
                            logger.warning(f"  ⚠️ [获取直链] {'OpenAPI' if use_openapi else 'Cookie'} 未拿到直链，切换接口重试 ({retry_count+1}/{max_retries})...")
                    except Exception as e:
                        err_str = str(e)
                        if '405' in err_str or 'Method Not Allowed' in err_str:
                            logger.warning(f"  🛑 [获取直链] 触发 115 风控，切换接口重试 ({retry_count+1}/{max_retries})...")
                        else:
                            logger.warning(f"  ⚠️ [获取直链] 发生异常: {e}，切换接口重试 ({retry_count+1}/{max_retries})...")
                    
                    # 核心：切换接口，轮流尝试
                    use_openapi = not use_openapi
                    retry_count += 1
                    time.sleep(1.0) # 稍微休眠一下防止请求过频

                if real_115_url:
                    response = redirect(real_115_url, code=302)
                    response.headers['Access-Control-Allow-Origin'] = '*'
                    return response
                else:
                    logger.error(f"  💀 [致命错误] 115双接口均失败，拒绝回退中转，直接掐断请求！")
                    return Response("Failed to get 115 direct link after retries. Proxy fallback is disabled.", status=503)

            # ====================================================================
            # ★ 核心逻辑 2：如果没有 pick_code (例如本地硬盘文件)，走正常的 Emby 代理流式传输
            # ====================================================================
            logger.info(f"  ▶️ [本地/非115文件] 未检测到 pick_code，正常代理 Emby 视频流...")
            target_url = f"{base_url}/{path.lstrip('/')}"
            forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
            forward_headers['Host'] = urlparse(base_url).netloc
            forward_params = request.args.copy()
            forward_params['api_key'] = api_key
            
            # 必须开启 stream=True
            resp = requests.request(
                method=request.method, 
                url=target_url, 
                headers=forward_headers, 
                params=forward_params, 
                data=request.get_data(), 
                timeout=(10.0, 1800.0), 
                stream=True, 
                allow_redirects=False
            )
            
            # 透传 Emby 的 302 (比如 Emby 自己挂载了 rclone 直链)
            if resp.status_code in [301, 302]:
                redirect_url = resp.headers.get('Location', '')
                response = redirect(redirect_url, code=resp.status_code)
                for name, value in resp.headers.items():
                    if name.lower() not in ['content-length', 'connection']:
                        response.headers[name] = value
                return response
            
            # 流式返回本地文件数据
            excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
            response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
            return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
        
        # --- 4. 拦截 A: 虚拟项目海报图片 ---
        if path.startswith('emby/Items/') and '/Images/Primary' in path:
            item_id = path.split('/')[2]
            if is_missing_item_id(item_id):
                combined_id = parse_missing_item_id(item_id)
                real_tmdb_id = combined_id.split('_S_')[0] if '_S_' in combined_id else combined_id
                meta = queries_db.get_best_metadata_by_tmdb_id(real_tmdb_id)
                db_status = meta.get('subscription_status', 'WANTED')
                current_status = db_status if db_status in ['WANTED', 'SUBSCRIBED', 'PENDING_RELEASE', 'PAUSED', 'IGNORED'] else 'WANTED'
                
                from handler.poster_generator import get_missing_poster
                img_file_path = get_missing_poster(
                    tmdb_id=real_tmdb_id, 
                    status=current_status,
                    poster_path=meta.get('poster_path')
                )
                
                if img_file_path and os.path.exists(img_file_path):
                    resp = send_file(img_file_path, mimetype='image/jpeg')
                    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                    return resp

        # --- 拦截 B: 视图列表 (Views) ---
        if normalized_path.endswith('/Views') and normalized_path.startswith('Users/'):
            _magic_log("ROUTE-HIT-VIEWS", path=path, normalized_path=normalized_path)
            return handle_get_views()

        # --- 拦截 C: 最新项目 (Latest) ---
        if normalized_path.endswith('/Items/Latest'):
            user_id_match = re.search(r'/?Users/([^/]+)/', normalized_path, re.IGNORECASE)
            if user_id_match:
                _magic_log("ROUTE-HIT-LATEST", user_id=user_id_match.group(1), path=path, normalized_path=normalized_path)
                return handle_get_latest_items(user_id_match.group(1), request.args)

        # --- 拦截 C2: Emby 4.10 新媒体库入口 (/Users/{id}/Items?IncludeItemTypes=CollectionFolder) ---
        user_items_match = re.search(r'Users/([^/]+)/Items$', normalized_path, re.IGNORECASE)
        if user_items_match and request.method == 'GET':
            user_id_for_items = user_items_match.group(1)
            _magic_log("ROUTE-CANDIDATE-USER-ITEMS", user_id=user_id_for_items, path=path, normalized_path=normalized_path, args=_magic_args_dict())

            ids_param = request.args.get('Ids') or request.args.get('ids')
            if ids_param and any(is_mimicked_id(x) for x in _split_csv(ids_param)):
                _magic_log("ROUTE-HIT-MIMICKED-IDS", ids=ids_param)
                return handle_get_mimicked_items_by_ids(user_id_for_items, ids_param)

            if _is_collection_folder_list_query(request.args):
                _magic_log("ROUTE-HIT-410-COLLECTION-FOLDERS", user_id=user_id_for_items)
                return handle_get_collection_folder_items_410(user_id_for_items, path, request.args)

        # --- 拦截 C3: Emby 4.10 用户首页配置，确保虚拟库不被 HomeSections/OrderedViews 排除 ---
        user_config_match = re.search(r'Users/([^/]+)/Configuration$', normalized_path, re.IGNORECASE)
        if user_config_match and request.method == 'GET':
            _magic_log("ROUTE-HIT-410-USER-CONFIG", user_id=user_config_match.group(1), path=path, normalized_path=normalized_path)
            return handle_get_user_configuration_410(user_config_match.group(1), path, request.args)

        # --- 拦截 D: 虚拟库详情 (增强版拦截) ---
        # 修复 iOS 有时不带 /Users/xxx，直接请求 /emby/Items/-900001 的老六行为
        details_match = re.search(r'/Items/(-(\d+))(?:$|\?)', full_path)
        if details_match and '/Images/' not in full_path and '/PlaybackInfo' not in full_path:
            mimicked_id = details_match.group(1)
            # 尝试从路径或参数获取 user_id
            user_id_match = re.search(r'/Users/([^/]+)/', full_path)
            user_id = user_id_match.group(1) if user_id_match else request.args.get('UserId')
            return handle_get_mimicked_library_details(user_id, mimicked_id)

        # --- 拦截 E: 虚拟库图片 ---
        if normalized_path.startswith('Items/') and '/Images/' in normalized_path:
            item_id = normalized_path.split('/')[1]
            if is_mimicked_id(item_id):
                _magic_log("ROUTE-HIT-MIMICKED-IMAGE", item_id=item_id, path=path, normalized_path=normalized_path)
                return handle_get_mimicked_library_image(path)
        
        # --- 拦截 F: 虚拟库内容浏览 (Items) ---
        # 修复 iOS 传参大小写问题 (有时传 ParentId，有时传 parentId)
        parent_id = request.args.get("ParentId") or request.args.get("parentId")
        
        if parent_id and is_mimicked_id(parent_id):
            _magic_log("ROUTE-CANDIDATE-MIMICKED-PARENT", parent_id=parent_id, path=path, normalized_path=normalized_path, args=_magic_args_dict())
            # 1. 处理核心的内容列表请求 (严格匹配结尾，防止误伤 Filters)
            user_id_match = re.search(r'Users/([^/]+)/Items$', normalized_path, re.IGNORECASE)
            if user_id_match:
                user_id = user_id_match.group(1)
                _magic_log("ROUTE-HIT-MIMICKED-LIBRARY-ITEMS", user_id=user_id, parent_id=parent_id)
                return handle_get_mimicked_library_items(user_id, parent_id, request.args)

            # 2. 处理所有其他带有虚拟 ParentId 的请求 (如 /Filters, /Genres 等)
            _magic_log("ROUTE-HIT-MIMICKED-METADATA-ENDPOINT", parent_id=parent_id, path=path)
            return handle_mimicked_library_metadata_endpoint(path, parent_id, request.args)

        # 兜底逻辑
        base_url, api_key = _get_real_emby_url_and_key()
        target_url = f"{base_url}/{path.lstrip('/')}"

        forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        forward_headers['Host'] = urlparse(base_url).netloc

        forward_params = request.args.copy()
        forward_params['api_key'] = api_key

        _magic_log("FALLBACK-FORWARD", target_url=target_url, path=path, normalized_path=normalized_path, args=_magic_args_dict(forward_params))
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=forward_headers,
            params=forward_params,
            data=request.get_data(),
            stream=True,
            timeout=(10.0, 1800.0)
        )

        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]

        # 对疑似 4.10 媒体库/首页相关 JSON 响应做一次“窥探”，看客户端到底在请求什么。
        # 只对相关路径生效，避免视频流/图片流被读入内存。
        if _proxy_magic_enabled() and _magic_should_trace(full_path, request.args):
            content_type = (resp.headers.get('Content-Type') or '').lower()
            _magic_log("FALLBACK-RESP", status=resp.status_code, content_type=content_type, path=path, normalized_path=normalized_path)
            if 'json' in content_type:
                try:
                    body = resp.content
                    try:
                        data = json.loads(body.decode('utf-8', errors='replace')) if body else None
                        _magic_log_json_response("FALLBACK-JSON", data, status_code=resp.status_code)
                    except Exception:
                        preview = body[:800].decode('utf-8', errors='replace') if body else ''
                        _magic_log("FALLBACK-BODY-PREVIEW", preview=preview)
                    return Response(body, resp.status_code, response_headers)
                except Exception as log_e:
                    _magic_log("FALLBACK-RESP-LOG-FAILED", error=str(log_e))

        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)

    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500
