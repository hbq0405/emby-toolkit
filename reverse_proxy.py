# reverse_proxy.py (V7 - Emby 4.9.5 虚拟库增强版：精简可用标签页 + 类型/标签过滤)

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
import hashlib
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



def _normalize_definition(definition):
    """
    definition_json 可能是 dict，也可能是 JSON 字符串。
    统一转成 dict，避免后面直接 .get 报错。
    """
    if not definition:
        return {}
    if isinstance(definition, str):
        try:
            return json.loads(definition)
        except Exception:
            return {}
    if isinstance(definition, dict):
        return definition
    return {}


def _get_item_types_from_definition(definition):
    definition = _normalize_definition(definition)
    item_types = definition.get('item_type', ['Movie'])
    if isinstance(item_types, str):
        item_types = [item_types]
    if not isinstance(item_types, list) or not item_types:
        item_types = ['Movie']
    return [str(x) for x in item_types if x]


def _infer_collection_type(definition):
    """
    根据虚拟库定义推断 Emby 前端需要的 CollectionType。
    注意：这个只决定前端展示视图，不改变后端查询 item_type。
    """
    item_types = _get_item_types_from_definition(definition)

    if len(item_types) > 1:
        return 'mixed'

    if item_types and item_types[0] == 'Series':
        return 'tvshows'

    return 'movies'


def _get_target_library_ids(definition):
    """
    从虚拟库规则里拿真实媒体库 ID，比如电影库 260948。
    这里拿到的是媒体库 ID，不是自建合集 ID。
    """
    definition = _normalize_definition(definition)
    ids = definition.get('target_library_ids', [])

    if isinstance(ids, str):
        ids = [x.strip() for x in ids.split(',') if x.strip()]

    if not isinstance(ids, list):
        return []

    return [str(x) for x in ids if x]


def _default_subviews_for_type(collection_type):
    """
    Emby 4.9.x Web 的 videos 页面会读取 Subviews，并对它执行 includes。
    如果虚拟库缺这个字段，纯电影库会在 HomeVideosView.getTabs 里报错。
    """
    if collection_type == 'movies':
        return ['movies', 'tags', 'collections', 'genres', 'movies', 'folders']
    if collection_type == 'tvshows':
        return ['series', 'tags', 'genres', 'folders']
    return ['movies', 'series', 'tags', 'genres', 'folders']


def _get_virtual_supported_subviews(collection_type, source_subviews=None):
    """
    增强版策略：不再把真实电影库所有标签页照搬过来。
    只保留当前后端真正处理过的入口，避免“中看不中用”。

    可选：在 APP_CONFIG 里放 proxy_virtual_subviews = "movies,genres,tags" 自定义。
    """
    raw = config_manager.APP_CONFIG.get('proxy_virtual_subviews', '')
    if isinstance(raw, str) and raw.strip():
        wanted = [x.strip() for x in raw.split(',') if x.strip()]
    elif isinstance(raw, list) and raw:
        wanted = [str(x).strip() for x in raw if str(x).strip()]
    else:
        # 虚拟库不再展示“标签”页：电影/剧集只保留主入口 + 类型。
        # Emby Web 某些版本会前端硬编码生成标签页，后面还有 index.html 注入脚本兜底隐藏。
        if collection_type == 'movies':
            wanted = ['movies', 'genres']
        elif collection_type == 'tvshows':
            wanted = ['series', 'genres']
        else:
            wanted = ['movies', 'series', 'genres']

    if isinstance(source_subviews, list) and source_subviews:
        source_set = set(source_subviews)
        # movies/series 是核心入口，即便源 Subviews 里有重复也只保留一次。
        filtered = [x for x in wanted if x in source_set or x in ['movies', 'series', 'genres']]
        return list(dict.fromkeys(filtered)) or wanted

    return list(dict.fromkeys(wanted))


def _get_user_id_from_request_path_or_headers(path=None):
    """
    尽量从路径、参数、X-Emby-Authorization 中提取 UserId。
    某些客户端直接请求 /emby/Items/-900xxx，不带 /Users/{id}。
    """
    path = path or request.path or ''

    user_id_match = re.search(r'/Users/([^/]+)/', path)
    if user_id_match:
        return user_id_match.group(1)

    user_id = request.args.get('UserId') or request.args.get('userId')
    if user_id:
        return user_id

    auth = request.headers.get('X-Emby-Authorization') or request.headers.get('Authorization') or ''
    # 例：MediaBrowser ... UserId="xxx", ...
    auth_match = re.search(r'UserId="?([^",]+)"?', auth)
    if auth_match:
        return auth_match.group(1)

    return None


def _get_user_visible_native_libraries(user_id):
    if not user_id:
        return []
    try:
        libs = emby.get_emby_libraries(
            config_manager.APP_CONFIG.get('emby_server_url', ''),
            config_manager.APP_CONFIG.get('emby_api_key', ''),
            user_id
        )
        return libs or []
    except Exception as e:
        logger.warning(f'获取原生媒体库列表失败: {e}')
        return []


def _find_display_source_view(user_visible_native_libs, definition, collection_type):
    """
    找虚拟库应该继承的真实媒体库视图。

    你的架构里：
    - coll.emby_collection_id 是自建合集 ID，例如 652540，用于封面/实体合集。
    - definition.target_library_ids 里的 ID 才是真实媒体库 ID，例如电影库 260948。

    DisplayPreferencesId / Subviews 必须继承真实媒体库，不能使用自建合集 ID。
    """
    target_library_ids = _get_target_library_ids(definition)

    for lib_id in target_library_ids:
        for view in user_visible_native_libs:
            if str(view.get('Id')) == str(lib_id):
                return view

    # 兜底：如果规则里没有 target_library_ids，就找同 CollectionType 的第一个真实库
    for view in user_visible_native_libs:
        if view.get('CollectionType') == collection_type:
            return view

    return None


def _safe_array(value):
    return value if isinstance(value, list) else []


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _split_csv(value):
    """把 Emby 查询参数里的逗号分隔值统一转成字符串列表。"""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw = []
        for item in value:
            raw.extend(_split_csv(item))
        return raw
    return [x.strip() for x in str(value).split(',') if x.strip()]


def _stable_text_id(prefix, text):
    """
    给没有真实 Emby Id 的 Genre/Tag 生成稳定数字 ID。
    用于前端点击后我们本地过滤，不依赖 Emby 原生库的全局 ID。
    """
    raw = f'{prefix}:{text}'.encode('utf-8')
    return str(int(hashlib.md5(raw).hexdigest()[:8], 16))


def _get_query_param(params, *names):
    for name in names:
        if hasattr(params, 'get'):
            value = params.get(name)
            if value is not None and value != '':
                return value
        elif isinstance(params, dict):
            value = params.get(name)
            if value is not None and value != '':
                return value
    return None


def _has_virtual_item_filter_params(params):
    """
    判断是否是从“类型/标签/收藏/年份”等标签页进入的二级过滤。
    一旦有这些过滤，就不能只取当前页 SQL 结果，而要先拿虚拟库全量候选，再过滤分页。
    """
    filter_keys = [
        'GenreIds', 'genreIds', 'Genres', 'genres', 'Genre', 'genre',
        'TagIds', 'tagIds', 'Tags', 'tags',
        'Years', 'years', 'ProductionYear', 'productionYear',
        'OfficialRatings', 'officialRatings', 'OfficialRating', 'officialRating',
        'StudioIds', 'studioIds', 'Studios', 'studios',
        'IsFavorite', 'isFavorite', 'Filters', 'filters',
        'NameStartsWith', 'nameStartsWith', 'NameStartsWithOrGreater', 'nameStartsWithOrGreater'
    ]
    return any(_get_query_param(params, k) is not None for k in filter_keys)


def _get_virtual_library_candidate_ids(collection_info, user_id, max_items=5000):
    """
    取虚拟库候选 Emby Item Id。
    用于动态生成 Genres/Tags，以及点击类型/标签后在虚拟库范围内继续过滤。
    """
    definition = _normalize_definition(collection_info.get('definition_json') or {})
    collection_type = collection_info.get('type')

    rules = definition.get('rules', [])
    logic = definition.get('logic', 'AND')
    item_types = definition.get('item_type', ['Movie'])
    target_library_ids = definition.get('target_library_ids', [])
    tmdb_ids_filter = None

    if collection_type == 'list':
        raw_list_json = collection_info.get('generated_media_info_json')
        try:
            raw_list = json.loads(raw_list_json) if isinstance(raw_list_json, str) else (raw_list_json or [])
        except Exception:
            raw_list = []
        tmdb_ids_filter = [str(i.get('tmdb_id')) for i in raw_list if i.get('tmdb_id')]
        if not tmdb_ids_filter:
            return []

    elif collection_type in ['ai_recommendation', 'ai_recommendation_global']:
        # AI 推荐是动态向量，保持原有逻辑：这里只做保守返回，避免把全库暴露给标签页。
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        if api_key:
            try:
                engine = RecommendationEngine(api_key)
                if collection_type == 'ai_recommendation':
                    candidate_pool = engine.generate_user_vector(user_id, limit=300, allowed_types=item_types)
                else:
                    candidate_pool = engine.generate_global_vector(limit=300, allowed_types=item_types)
                tmdb_ids_filter = [str(i['id']) for i in candidate_pool if i.get('id')]
            except Exception as e:
                logger.warning(f'生成 AI 推荐候选失败: {e}')
                return []
        else:
            return []

    items, _ = queries_db.query_virtual_library_items(
        rules=rules,
        logic=logic,
        user_id=user_id,
        limit=max_items,
        offset=0,
        sort_by='DateCreated',
        sort_order='Descending',
        item_types=item_types,
        target_library_ids=target_library_ids,
        tmdb_ids=tmdb_ids_filter
    )

    return [str(i['Id']) for i in items if i.get('Id')]


def _extract_named_items_from_item(item, field_name, item_field_name, fallback_prefix):
    """
    从 Emby Item 里提取 Genres/Tags/Studios 等命名对象。
    优先使用 Emby 返回的真实 Id；没有 Id 时生成稳定虚拟 Id。
    """
    result = []

    named_items = item.get(item_field_name)
    if isinstance(named_items, list):
        for obj in named_items:
            if isinstance(obj, dict) and obj.get('Name'):
                name = str(obj.get('Name'))
                oid = str(obj.get('Id') or _stable_text_id(fallback_prefix, name))
                result.append({'Name': name, 'Id': oid})

    # 很多情况下 Emby 只返回 Genres/Tags 字符串数组，不返回 GenreItems/TagItems。
    values = item.get(field_name)
    if isinstance(values, list):
        existing_names = {x['Name'] for x in result}
        for name in values:
            if name and str(name) not in existing_names:
                result.append({'Name': str(name), 'Id': _stable_text_id(fallback_prefix, str(name))})

    return result


def handle_mimicked_library_facet_endpoint(path, mimicked_id, params, facet_type):
    """
    给虚拟库动态生成“类型/标签”等标签页数据。
    不再转发到真实电影库或自建合集，避免显示一堆不属于该虚拟库的全库标签。
    """
    empty = {'Items': [], 'TotalRecordCount': 0}
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info:
            return Response(json.dumps(empty), mimetype='application/json')

        user_id = _get_user_id_from_request_path_or_headers(path)
        item_ids = _get_virtual_library_candidate_ids(collection_info, user_id, max_items=5000)
        if not item_ids:
            return Response(json.dumps(empty), mimetype='application/json')

        base_url, api_key = _get_real_emby_url_and_key()
        fields = 'Genres,GenreItems,Tags,TagItems,Studios,ProductionYear,OfficialRating,UserData'
        details = _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, fields)

        counter = {}

        if facet_type == 'genres':
            for item in details:
                for obj in _extract_named_items_from_item(item, 'Genres', 'GenreItems', 'genre'):
                    key = obj['Name']
                    counter.setdefault(key, {'Name': obj['Name'], 'Id': obj['Id'], 'Count': 0})
                    counter[key]['Count'] += 1

        elif facet_type == 'tags':
            for item in details:
                for obj in _extract_named_items_from_item(item, 'Tags', 'TagItems', 'tag'):
                    key = obj['Name']
                    counter.setdefault(key, {'Name': obj['Name'], 'Id': obj['Id'], 'Count': 0})
                    counter[key]['Count'] += 1

        else:
            return Response(json.dumps(empty), mimetype='application/json')

        items = sorted(counter.values(), key=lambda x: x['Name'].lower())
        result = {'Items': items, 'TotalRecordCount': len(items)}
        return Response(json.dumps(result), mimetype='application/json')

    except Exception as e:
        logger.error(f'处理虚拟库 {facet_type} 标签页失败: {e}', exc_info=True)
        return Response(json.dumps(empty), mimetype='application/json')


def _item_matches_virtual_filters(item, params):
    """在虚拟库候选集内本地过滤 Genre/Tag/收藏/首字母等。"""
    genre_ids = set(_split_csv(_get_query_param(params, 'GenreIds', 'genreIds')))
    genre_names = set(_split_csv(_get_query_param(params, 'Genres', 'genres', 'Genre', 'genre')))
    tag_ids = set(_split_csv(_get_query_param(params, 'TagIds', 'tagIds')))
    tag_names = set(_split_csv(_get_query_param(params, 'Tags', 'tags')))
    years = set(_split_csv(_get_query_param(params, 'Years', 'years', 'ProductionYear', 'productionYear')))
    ratings = set(_split_csv(_get_query_param(params, 'OfficialRatings', 'officialRatings', 'OfficialRating', 'officialRating')))
    is_favorite = _get_query_param(params, 'IsFavorite', 'isFavorite')
    filters = set(x.lower() for x in _split_csv(_get_query_param(params, 'Filters', 'filters')))
    name_starts = _get_query_param(params, 'NameStartsWith', 'nameStartsWith')
    name_starts_ge = _get_query_param(params, 'NameStartsWithOrGreater', 'nameStartsWithOrGreater')

    if genre_ids or genre_names:
        genres = _extract_named_items_from_item(item, 'Genres', 'GenreItems', 'genre')
        item_genre_ids = {str(x['Id']) for x in genres}
        item_genre_names = {str(x['Name']) for x in genres}
        if genre_ids and not (genre_ids & item_genre_ids):
            return False
        if genre_names and not (genre_names & item_genre_names):
            return False

    if tag_ids or tag_names:
        tags = _extract_named_items_from_item(item, 'Tags', 'TagItems', 'tag')
        item_tag_ids = {str(x['Id']) for x in tags}
        item_tag_names = {str(x['Name']) for x in tags}
        if tag_ids and not (tag_ids & item_tag_ids):
            return False
        if tag_names and not (tag_names & item_tag_names):
            return False

    if years:
        item_year = str(item.get('ProductionYear') or '')
        if item_year not in years:
            return False

    if ratings:
        item_rating = str(item.get('OfficialRating') or '')
        if item_rating not in ratings:
            return False

    if is_favorite is not None:
        want = str(is_favorite).lower() == 'true'
        got = bool((item.get('UserData') or {}).get('IsFavorite'))
        if got != want:
            return False

    if 'isfavorite' in filters or 'favorite' in filters:
        if not bool((item.get('UserData') or {}).get('IsFavorite')):
            return False

    if name_starts:
        sort_name = str(item.get('SortName') or item.get('Name') or '').lower()
        if not sort_name.startswith(str(name_starts).lower()):
            return False

    if name_starts_ge:
        sort_name = str(item.get('SortName') or item.get('Name') or '').lower()
        if sort_name < str(name_starts_ge).lower():
            return False

    return True


def _sort_items_locally(items, sort_by, sort_order):
    primary_sort_by = (sort_by or 'SortName').split(',')[0]
    reverse = (sort_order or 'Ascending') == 'Descending'

    def get_sort_val(item):
        val = item.get(primary_sort_by)
        if primary_sort_by in ['SortName', 'Name']:
            return str(val or item.get('Name') or '').lower()
        if 'Date' in primary_sort_by:
            return str(val or '1900-01-01T00:00:00.000Z')
        if 'Year' in primary_sort_by:
            return int(val or 0)
        if 'Rating' in primary_sort_by or 'Count' in primary_sort_by:
            try:
                return float(val or 0)
            except Exception:
                return 0
        return str(val or '').lower()

    try:
        items.sort(key=get_sort_val, reverse=reverse)
    except Exception as e:
        logger.warning(f'虚拟库本地排序失败: {e}')
    return items


def _fetch_virtual_items_with_local_filters(user_id, item_ids, params, fields, limit, offset):
    """
    点击虚拟库“类型/标签/收藏/字母索引”等入口后，在虚拟库候选集内继续过滤。
    这比直接转发给真实库更准确：不会泄露真实库全量内容，也不会依赖自建合集 652540 的标签。
    """
    if not item_ids:
        return {'Items': [], 'TotalRecordCount': 0}

    base_url, api_key = _get_real_emby_url_and_key()
    extra_fields = 'Genres,GenreItems,Tags,TagItems,Studios,ProductionYear,OfficialRating,DateCreated,PremiereDate,SortName,UserData,CommunityRating,RecursiveItemCount,ChildCount,BasicSyncInfo'
    merged_fields = ','.join(dict.fromkeys([x.strip() for x in f'{fields},{extra_fields}'.split(',') if x.strip()]))
    details = _fetch_items_in_chunks(base_url, api_key, user_id, item_ids, merged_fields)

    filtered = [item for item in details if _item_matches_virtual_filters(item, params)]

    sort_by = _get_query_param(params, 'SortBy') or 'SortName'
    sort_order = _get_query_param(params, 'SortOrder') or 'Ascending'
    filtered = _sort_items_locally(filtered, sort_by, sort_order)

    total = len(filtered)
    paged = filtered[offset: offset + limit]
    return {'Items': paged, 'TotalRecordCount': total}


def _build_virtual_collection_folder(coll, mimicked_id, user_visible_native_libs, timestamp_image_tag=True):
    """
    构造虚拟媒体库 CollectionFolder。

    关键点：
    1. emby_collection_id 保持作为自建合集 ID，用于封面。
    2. DisplayPreferencesId 和 Subviews 从真实媒体库继承，避免 Emby 4.9.5 Web 的 getTabs 报错。
    3. CollectionType 仍保持 movies/tvshows/mixed，这样标签页和真实库一致。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    db_id = coll['id']
    real_emby_collection_id = coll.get('emby_collection_id')  # 自建合集 ID，例如 652540
    definition = _normalize_definition(coll.get('definition_json') or {})

    collection_type = _infer_collection_type(definition)
    source_view = _find_display_source_view(user_visible_native_libs, definition, collection_type)
    source_view = dict(source_view) if source_view else {}

    source_display_preferences_id = source_view.get('DisplayPreferencesId')
    source_subviews = source_view.get('Subviews')
    if not isinstance(source_subviews, list):
        source_subviews = _default_subviews_for_type(collection_type)
    source_subviews = _get_virtual_supported_subviews(collection_type, source_subviews)

    if real_emby_collection_id:
        primary_tag = f'{real_emby_collection_id}?timestamp={int(time.time())}' if timestamp_image_tag else str(real_emby_collection_id)
        image_tags = {'Primary': primary_tag}
    else:
        image_tags = {}

    dto = dict(source_view)
    dto.update({
        'Name': coll['name'],
        'ServerId': real_server_id,
        'Id': mimicked_id,
        'Guid': str(uuid.uuid5(uuid.NAMESPACE_DNS, f'custom-view-{db_id}')),
        'Etag': f'{db_id}{int(time.time())}',
        'DateCreated': dto.get('DateCreated', '2025-01-01T00:00:00.0000000Z'),
        'DateModified': dto.get('DateModified', '0001-01-01T00:00:00.0000000Z'),
        'CanDelete': False,
        'CanDownload': False,
        'SupportsSync': dto.get('SupportsSync', True),
        'SortName': coll['name'],
        'ForcedSortName': coll['name'],
        'ExternalUrls': _safe_array(dto.get('ExternalUrls')),
        'ProviderIds': _safe_dict(dto.get('ProviderIds')),
        'IsFolder': True,
        'ParentId': dto.get('ParentId', '2'),
        'Type': 'CollectionFolder',
        'PresentationUniqueKey': f'custom-view-{db_id}',

        # 关键修复：这里必须是真实媒体库的 DisplayPreferencesId，不能是自建合集 ID。
        'DisplayPreferencesId': source_display_preferences_id or f'custom-{db_id}',

        'Taglines': _safe_array(dto.get('Taglines')),
        'RemoteTrailers': _safe_array(dto.get('RemoteTrailers')),
        'UserData': _safe_dict(dto.get('UserData')) or {
            'PlaybackPositionTicks': 0,
            'IsFavorite': False,
            'Played': False
        },
        'ChildCount': coll.get('in_library_count', 1),
        'PrimaryImageAspectRatio': 1.7777777777777777,
        'CollectionType': collection_type,

        # 封面仍然来自自建合集 ID，例如 652540。
        'ImageTags': image_tags,

        'BackdropImageTags': _safe_array(dto.get('BackdropImageTags')),
        'LockedFields': _safe_array(dto.get('LockedFields')),
        'LockData': bool(dto.get('LockData', False)),
        'Tags': _safe_array(dto.get('Tags')),
        'Subviews': source_subviews,
    })

    # 一些客户端可能直接读取这些字段，补空数组避免 undefined.includes / undefined.map。
    for key in ['Genres', 'People', 'GenreItems', 'TagItems']:
        if key in dto and not isinstance(dto.get(key), list):
            dto[key] = []

    return dto


def handle_get_views():
    """
    获取用户的主页视图列表。
    """
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        return "Proxy is not ready", 503

    try:
        user_id_match = re.search(r'/emby/Users/([^/]+)/Views', request.path)
        if not user_id_match:
            return "Could not determine user from request path", 400
        user_id = user_id_match.group(1)

        # 1. 获取原生库。这里拿到的是真实媒体库 Views，例如电影库 260948。
        user_visible_native_libs = _get_user_visible_native_libraries(user_id)

        # 2. 生成虚拟库。
        collections = custom_collection_db.get_all_active_custom_collections()
        fake_views_items = []
        
        for coll in collections:
            # 物理检查：自建合集在 Emby 里是否有实体。
            # 注意：这个是自建合集 ID，例如 652540，不是真实电影库 ID。
            real_emby_collection_id = coll.get('emby_collection_id')
            if not real_emby_collection_id:
                continue

            # 权限检查。
            allowed_users = coll.get('allowed_user_ids')
            if allowed_users and isinstance(allowed_users, list):
                if user_id not in allowed_users:
                    continue
            
            db_id = coll['id']
            mimicked_id = to_mimicked_id(db_id)
            fake_view = _build_virtual_collection_folder(
                coll=coll,
                mimicked_id=mimicked_id,
                user_visible_native_libs=user_visible_native_libs,
                timestamp_image_tag=True
            )
            fake_views_items.append(fake_view)
        
        # 3. 合并与排序。
        native_views_items = []
        should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
        if should_merge_native:
            all_native_views = user_visible_native_libs
            raw_selection = config_manager.APP_CONFIG.get('proxy_native_view_selection', '')
            selected_native_view_ids = [x.strip() for x in raw_selection.split(',') if x.strip()] if isinstance(raw_selection, str) else raw_selection
            
            if selected_native_view_ids:
                native_views_items = [view for view in all_native_views if view.get('Id') in selected_native_view_ids]
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

        final_response = {'Items': final_items, 'TotalRecordCount': len(final_items)}
        return Response(json.dumps(final_response), mimetype='application/json')
        
    except Exception as e:
        logger.error(f"[PROXY] 获取视图数据时出错: {e}", exc_info=True)
        return "Internal Proxy Error", 500


def handle_get_mimicked_library_details(user_id, mimicked_id):
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        coll = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not coll:
            return "Not Found", 404

        user_id = user_id or _get_user_id_from_request_path_or_headers(request.path)
        user_visible_native_libs = _get_user_visible_native_libraries(user_id)

        fake_library_details = _build_virtual_collection_folder(
            coll=coll,
            mimicked_id=mimicked_id,
            user_visible_native_libs=user_visible_native_libs,
            timestamp_image_tag=False
        )

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



def handle_mimicked_display_preferences(path, mimicked_id):
    """
    处理少数客户端直接请求 /DisplayPreferences/-900xxx 的情况。

    正常情况下，虚拟库 JSON 里的 DisplayPreferencesId 已经被设置为真实媒体库的 GUID，
    前端会直接请求真实 DisplayPreferencesId；这个拦截只是兜底。
    """
    default_response = {
        'Id': mimicked_id,
        'SortBy': 'SortName',
        'SortOrder': 'Ascending',
        'ViewType': 'Poster',
        'CustomPrefs': {}
    }

    try:
        real_db_id = from_mimicked_id(mimicked_id)
        coll = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not coll:
            return Response(json.dumps(default_response), mimetype='application/json')

        user_id = _get_user_id_from_request_path_or_headers(path)
        definition = _normalize_definition(coll.get('definition_json') or {})
        collection_type = _infer_collection_type(definition)
        native_libs = _get_user_visible_native_libraries(user_id)
        source_view = _find_display_source_view(native_libs, definition, collection_type)
        source_display_preferences_id = source_view.get('DisplayPreferencesId') if source_view else None

        if not source_display_preferences_id:
            return Response(json.dumps(default_response), mimetype='application/json')

        base_url, api_key = _get_real_emby_url_and_key()
        target_path = re.sub(
            r'(DisplayPreferences/)[^/?]+',
            rf'\g<1>{source_display_preferences_id}',
            path,
            count=1,
            flags=re.IGNORECASE
        )
        target_url = f"{base_url}/{target_path.lstrip('/')}"

        headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        headers['Host'] = urlparse(base_url).netloc

        params = request.args.copy()
        params['api_key'] = api_key

        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            params=params,
            data=request.get_data(),
            timeout=15,
            stream=True
        )

        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]

        # GET 时把 Id 改回虚拟 ID，减少前端缓存串库的概率。
        content_type = resp.headers.get('Content-Type', '')
        if request.method.upper() == 'GET' and 'application/json' in content_type.lower():
            try:
                data = resp.json()
                if isinstance(data, dict):
                    data['Id'] = mimicked_id
                    if not isinstance(data.get('CustomPrefs'), dict):
                        data['CustomPrefs'] = {}
                    return Response(json.dumps(data), resp.status_code, mimetype='application/json')
            except Exception:
                pass

        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)

    except Exception as e:
        logger.error(f"处理虚拟库 DisplayPreferences 失败: {e}", exc_info=True)
        return Response(json.dumps(default_response), mimetype='application/json')

UNSUPPORTED_METADATA_ENDPOINTS = [
        # '/Items/Prefixes', # Emby 不支持按前缀过滤虚拟库
        # '/Genres' 和 '/Tags' 已在增强版中动态生成。
        '/Studios',        
        '/OfficialRatings',
        '/Years'           
    ]

def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    处理虚拟库的元数据请求。
    """
    # 【关键修复】Emby 期望返回的是 {"Items": [], "TotalRecordCount": 0}，而不是单纯的 []
    empty_response = json.dumps({"Items": [], "TotalRecordCount": 0})

    path_lower = '/' + path.lower().lstrip('/')
    if path_lower.endswith('/genres'):
        return handle_mimicked_library_facet_endpoint(path, mimicked_id, params, 'genres')
    if path_lower.endswith('/tags'):
        return handle_mimicked_library_facet_endpoint(path, mimicked_id, params, 'tags')
    
    if any(path.endswith(endpoint) for endpoint in UNSUPPORTED_METADATA_ENDPOINTS):
        return Response(empty_response, mimetype='application/json')

    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = custom_collection_db.get_custom_collection_by_id(real_db_id)
        if not collection_info or not collection_info.get('emby_collection_id'):
            return Response(empty_response, mimetype='application/json')

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
        return Response(empty_response, mimetype='application/json')
    
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

        # 点击“类型/标签/收藏/字母索引”等入口时，需要先取虚拟库全量候选，再过滤分页。
        has_virtual_filters = _has_virtual_item_filter_params(params)

        # 核心判断：是否需要 Emby 原生排序/全量候选。
        # 当使用原生排序(is_native_mode=True)时，如果排序字段不是数据库能完美处理的(如DateCreated)，
        # 必须强制走 Emby 代理排序。带虚拟过滤条件时也必须走全量候选。
        is_emby_proxy_sort_required = (
            has_virtual_filters or
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
            # 榜单类主页面继续保持原有顺序/占位符逻辑。
            # 但如果用户点了“类型/标签/收藏/字母索引”，则切到虚拟候选集内过滤。
            # 过滤视图不显示缺失占位符，只显示真实存在且当前用户有权访问的条目。
            if has_virtual_filters:
                candidate_ids = _get_virtual_library_candidate_ids(collection_info, user_id, max_items=5000)
                full_fields_for_filter = "PrimaryImageAspectRatio,ImageTags,HasPrimaryImage,ProviderIds,UserData,Name,ProductionYear,CommunityRating,DateCreated,PremiereDate,Type,RecursiveItemCount,SortName,ChildCount,BasicSyncInfo"
                filtered_data = _fetch_virtual_items_with_local_filters(
                    user_id, candidate_ids, params, full_fields_for_filter, emby_limit, offset
                )
                return Response(json.dumps(filtered_data), mimetype='application/json')

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
                if has_virtual_filters:
                    # 虚拟库二级过滤：类型/标签/收藏/字母索引等必须限制在虚拟库候选范围内。
                    sorted_data = _fetch_virtual_items_with_local_filters(
                        user_id, final_emby_ids, params, full_fields, emby_limit, offset
                    )
                else:
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


def _inject_virtual_library_tab_cleaner(html_text):
    """
    Emby Web 4.9.5 的 movies 顶部标签基本是前端硬编码生成的，
    单靠服务端返回 Subviews 不能真正隐藏“推荐/预告片/收藏/文件夹”。
    因此在 index.html 注入一个很小的前端清理脚本：
    仅当 URL 是 /videos 或 /tvshows 且 parentId 为负数虚拟库时，隐藏无后端实现价值的标签，并统一隐藏“标签 / Tags”页。
    """
    if not isinstance(html_text, str) or 'virtual-library-tab-cleaner' in html_text:
        return html_text

    script = """
<script id="virtual-library-tab-cleaner">
(function () {
  if (window.__virtualLibraryTabCleanerInstalled) return;
  window.__virtualLibraryTabCleanerInstalled = true;

  var HIDE_TEXTS = new Set([
    // 虚拟库放弃标签页：电影库、剧集库都隐藏“标签 / Tags”。
    '标签', 'Tags', 'Tag', 'TAGS', 'tags', 'tag', 'タグ',
    '推荐', '预告片', '合集', '收藏', '文件夹',
    'Recommendations', 'Recommendation', 'Suggested', 'Suggestions',
    'Trailers', 'Collections', 'Favorites', 'Folders',
    '予告編', 'コレクション', 'お気に入り', 'フォルダ'
  ]);

  function normText(el) {
    return ((el && (el.innerText || el.textContent)) || '').replace(/\\s+/g, '').trim();
  }

  function isVirtualLibraryPage() {
    var s = String(location.hash || '') + ' ' + String(location.search || '') + ' ' + String(location.href || '');
    // 电影虚拟库通常是 /videos，剧集虚拟库通常是 /tvshows。
    // 只要是负数 parentId 的虚拟库页面，就执行清理，避免影响真实库。
    return /parentId=-\\d+/.test(s) && (s.indexOf('/videos') !== -1 || s.indexOf('/tvshows') !== -1);
  }

  function restoreHiddenTabs() {
    document.querySelectorAll('[data-virtual-library-tab-hidden="1"]').forEach(function (el) {
      el.style.display = '';
      el.removeAttribute('data-virtual-library-tab-hidden');
    });
  }

  function hideUselessVirtualTabs() {
    if (!isVirtualLibraryPage()) {
      restoreHiddenTabs();
      return;
    }

    // Emby Web 不同版本/主题下 tab 可能是 button / a / role=tab，做宽松匹配。
    var candidates = document.querySelectorAll('button, a, div[role="tab"], span[role="tab"]');
    candidates.forEach(function (el) {
      var t = normText(el);
      if (!t) return;
      if (HIDE_TEXTS.has(t)) {
        el.style.display = 'none';
        el.setAttribute('data-virtual-library-tab-hidden', '1');
      }
    });
  }

  var timer = null;
  function scheduleClean() {
    if (timer) clearTimeout(timer);
    timer = setTimeout(hideUselessVirtualTabs, 80);
  }

  window.addEventListener('hashchange', scheduleClean);
  window.addEventListener('popstate', scheduleClean);
  document.addEventListener('viewshow', scheduleClean, true);
  document.addEventListener('pageshow', scheduleClean, true);

  new MutationObserver(scheduleClean).observe(document.documentElement, {
    childList: true,
    subtree: true
  });

  scheduleClean();
})();
</script>
"""
    if re.search(r'</body\s*>', html_text, flags=re.IGNORECASE):
        return re.sub(r'</body\s*>', script + '\n</body>', html_text, count=1, flags=re.IGNORECASE)
    return html_text + script


def handle_emby_web_index_with_virtual_tab_cleaner(path):
    """转发 Emby index.html，并注入虚拟库标签清理脚本。"""
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        target_url = f"{base_url}/{path.lstrip('/')}"
        headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        headers['Host'] = urlparse(base_url).netloc
        params = request.args.copy()
        params['api_key'] = api_key

        resp = requests.get(target_url, headers=headers, params=params, timeout=30)
        content_type = resp.headers.get('Content-Type', 'text/html; charset=utf-8')
        html = resp.text
        html = _inject_virtual_library_tab_cleaner(html)

        response = Response(html, status=resp.status_code, content_type=content_type)
        # 防止浏览器一直缓存旧 index.html，导致脚本不生效。
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        logger.error(f"注入虚拟库标签清理脚本失败，回退普通代理: {e}", exc_info=True)
        # 出错时让后面的兜底代理处理更安全
        raise

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


        # --- 拦截 W: Emby Web 首页，注入虚拟库无用标签隐藏脚本 ---
        if request.method == 'GET' and path.lower().rstrip('/') in ['web/index.html', 'web']:
            return handle_emby_web_index_with_virtual_tab_cleaner(path)

        # --- 拦截 X: 虚拟库 DisplayPreferences 兜底 ---
        display_prefs_match = re.search(
            r'(?:^|/)DisplayPreferences/(-\d+)(?:$|/|\?)',
            path,
            re.IGNORECASE
        )
        if display_prefs_match and is_mimicked_id(display_prefs_match.group(1)):
            return handle_mimicked_display_preferences(path, display_prefs_match.group(1))

        # ===== 调试日志：打印所有请求路径 =====
        # logger.info(f"[PROXY] 请求路径: {full_path}")
        
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
                api_priority = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_115_API_PRIORITY, 'openapi')
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
        if path.endswith('/Views') and path.startswith('emby/Users/'):
            return handle_get_views()

        # --- 拦截 C: 最新项目 (Latest) ---
        if path.endswith('/Items/Latest'):
            user_id_match = re.search(r'/emby/Users/([^/]+)/', full_path)
            if user_id_match:
                return handle_get_latest_items(user_id_match.group(1), request.args)

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
        if path.startswith('emby/Items/') and '/Images/' in path:
            item_id = path.split('/')[2]
            if is_mimicked_id(item_id):
                return handle_get_mimicked_library_image(path)
        
        # --- 拦截 F: 虚拟库内容浏览 (Items) ---
        # 修复 iOS 传参大小写问题 (有时传 ParentId，有时传 parentId)
        parent_id = request.args.get("ParentId") or request.args.get("parentId")
        
        if parent_id and is_mimicked_id(parent_id):
            # 1. 处理核心的内容列表请求 (严格匹配结尾，防止误伤 Filters)
            user_id_match = re.search(r'emby/Users/([^/]+)/Items$', path)
            if user_id_match:
                user_id = user_id_match.group(1)
                return handle_get_mimicked_library_items(user_id, parent_id, request.args)

            # 2. 处理所有其他带有虚拟 ParentId 的请求 (如 /Filters, /Genres 等)
            return handle_mimicked_library_metadata_endpoint(path, parent_id, request.args)

        # 兜底逻辑
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
            timeout=(10.0, 1800.0)
        )
        
        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
        
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
        
    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500
