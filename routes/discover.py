# routes/discover.py
import logging
from flask import Blueprint, jsonify, request, g, session
import time
from extensions import any_login_required
import handler.tmdb as tmdb
from utils import DEFAULT_KEYWORD_MAPPING, DEFAULT_STUDIO_MAPPING, contains_chinese, DEFAULT_LANGUAGE_MAPPING, DEFAULT_RATING_MAPPING
from database import media_db, settings_db, request_db
from tasks.discover import task_update_daily_theme, task_replenish_recommendation_pool
import task_manager

discover_bp = Blueprint('discover_bp', __name__, url_prefix='/api/discover')
logger = logging.getLogger(__name__)

def _expand_keyword_labels_to_ids(labels: list) -> str:
    """
    【AND 逻辑版】将中文标签展开为 TMDb 关键词 ID
    不同标签之间使用 ',' (AND)，标签内部 ID 使用 '|' (OR)
    """
    mapping_data = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
    
    # ★★★ 修复：兼容处理 List 格式 ★★★
    mapping = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping = mapping_data
    
    label_groups = []
    for label in labels:
        if label in mapping:
            ids = mapping[label].get('ids', [])
            if ids:
                # 同一个标签内的 ID（如“恐怖”口袋里的多个 ID）依然用 OR 连接
                label_groups.append("|".join([str(_id) for _id in ids]))
        elif str(label).isdigit():
            label_groups.append(str(label))
    
    # 不同标签组之间用逗号连接，实现 AND 逻辑
    return ",".join(label_groups)

def _expand_studio_labels_to_ids(labels: list, mode: str = 'movie') -> str:
    """
    【OR 逻辑版】将中文工作室标签展开为 TMDb 公司/电视网 ID
    mode: 'movie' (取 company_ids) 或 'tv' (取 network_ids)
    """
    mapping_data = settings_db.get_setting('studio_mapping') or DEFAULT_STUDIO_MAPPING
    
    mapping_dict = {}
    if isinstance(mapping_data, list):
        for item in mapping_data:
            if item.get('label'):
                mapping_dict[item['label']] = item
    elif isinstance(mapping_data, dict):
        mapping_dict = mapping_data
    
    all_ids = []
    for label in labels:
        if label in mapping_dict:
            item = mapping_dict[label]
            ids = []
            
            # ★★★ 核心修改：根据模式取不同字段 ★★★
            if mode == 'movie':
                # 搜电影：优先取 company_ids
                ids = item.get('company_ids')
                # 兼容旧数据：如果没有 company_ids 但有 ids，勉强用 ids
                if not ids: ids = item.get('ids')
            elif mode == 'tv':
                # 搜剧集：优先取 network_ids
                ids = item.get('network_ids')
                # 兼容旧数据
                if not ids: ids = item.get('ids')
            
            if ids:
                all_ids.extend([str(_id) for _id in ids])
        elif str(label).isdigit():
            all_ids.append(str(label))
    
    return "|".join(list(set(all_ids)))

def _get_tmdb_rating_params(label: str, item_type: str) -> dict:
    """
    将中文分级标签（如 '成人'）转换为 TMDb 的 US 分级参数。
    """
    if not label:
        return {}

    # 1. 从数据库读取映射表 (如果没有则使用默认值)
    mapping_data = settings_db.get_setting('rating_mapping') or DEFAULT_RATING_MAPPING
    
    # 2. 强制锁定使用 'US' 标准 (因为 TMDb 的 US 数据最全)
    us_rules = mapping_data.get('US', [])
    
    # 3. 找到所有匹配该 Label 的 Code (例如 '成人' -> ['R', 'TV-MA'])
    target_codes = [
        rule['code'] 
        for rule in us_rules 
        if rule.get('label') == label and rule.get('code')
    ]
    
    if not target_codes:
        return {}
    
    # 用管道符连接 (OR 逻辑)
    codes_str = "|".join(target_codes)
    
    params = {}
    if item_type == 'Movie':
        # 电影：必须指定 certification_country 才能让 certification 生效
        params['certification_country'] = 'US'
        params['certification'] = codes_str
    elif item_type == 'Series':
        # 剧集：使用 with_content_ratings，配合 watch_region 提高准确度
        params['watch_region'] = 'US'
        params['with_content_ratings'] = codes_str
        
    return params

def _filter_and_enrich_results(tmdb_data: dict, current_user_id: str, db_item_type: str) -> dict:
    """
    辅助函数：过滤TMDb结果，并附加数据库中的全局信息。
    """
    if not tmdb_data or not tmdb_data.get("results"):
        return {"results": [], "total_pages": 0}

    # 步骤 1: 过滤掉没有海报的结果
    original_results = tmdb_data.get("results", [])
    final_filtered_results = [item for item in original_results if item.get("poster_path")]

    if not final_filtered_results:
        return {"results": [], "total_pages": 0}

    # 步骤 3: 附加数据库信息
    tmdb_ids = [str(item.get("id")) for item in final_filtered_results]
    
    # 获取在库状态映射表
    library_items_map = media_db.check_tmdb_ids_in_library(tmdb_ids, item_type=db_item_type)
    
    # 获取订阅状态
    subscription_statuses = request_db.get_global_subscription_statuses_by_tmdb_ids(tmdb_ids, item_type=db_item_type)

    media_type_for_frontend = 'movie' if db_item_type == 'Movie' else 'tv'

    for item in final_filtered_results:
        tmdb_id_str = str(item.get("id"))
        
        lookup_key = f"{tmdb_id_str}_{db_item_type}"
        
        item["in_library"] = lookup_key in library_items_map
        item["emby_item_id"] = library_items_map.get(lookup_key)
        item["subscription_status"] = subscription_statuses.get(tmdb_id_str, None)
        item["media_type"] = media_type_for_frontend
    
    tmdb_data["results"] = final_filtered_results
    return tmdb_data

@discover_bp.route('/movie', methods=['POST'])
@any_login_required
def discover_movies():
    """
    根据前端传来的筛选条件，从 TMDb 发现电影。
    策略调整：成人模式下附加 NC-17 筛选，不再强制剔除非 adult=True 内容，以保证加载速度和列表密度。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)
    
    # 记录开始时间，用于超时控制
    start_time = time.time()

    try:
        # 1. 权限与用户校验
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # 2. 关键词标签 -> 纯关键词 IDs
        labels = data.get('with_keywords', [])
        if isinstance(labels, str): labels = labels.split(',')
        k_ids_str = _expand_keyword_labels_to_ids(labels)

        # 3. 工作室标签 -> IDs
        studio_labels = data.get('with_companies', [])
        if isinstance(studio_labels, str): studio_labels = studio_labels.split(',')
        c_ids_str = _expand_studio_labels_to_ids(studio_labels, mode='movie')

        # 4. 构建基础参数字典
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            # 'page': data.get('page', 1), # page 在循环中控制
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''),
            'with_keywords': k_ids_str,
            'with_companies': c_ids_str,
            'without_genres': data.get('without_genres', ''),
            'primary_release_date.gte': data.get('primary_release_date.gte', ''),
            'primary_release_date.lte': data.get('primary_release_date.lte', ''),
            'with_original_language': data.get('with_original_language', ''),
            'with_origin_country': data.get('with_origin_country', ''),
        }
        
        # 5. 处理分级筛选 
        rating_label = data.get('with_rating_label')
        
        allow_adult_config = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_INCLUDE_ADULT, False)
        mapping_data = settings_db.get_setting('rating_mapping') or DEFAULT_RATING_MAPPING
        adult_labels = set()
        if isinstance(mapping_data, dict):
            for country_rules in mapping_data.values():
                for rule in country_rules:
                    if rule.get('emby_value') == 15 and rule.get('label'):
                        adult_labels.add(rule['label'])
        if not adult_labels: adult_labels.add('成人')

        is_adult_search = False
        if allow_adult_config and rating_label in adult_labels:
            tmdb_params['include_adult'] = 'true'
            is_adult_search = True
            
            # --- ★★★ 核心修改：附加 NC-17 筛选 ★★★ ---
            # 这会把搜索范围限制在“美国分级为 NC-17”的电影中
            # 优点：TMDb 返回的全是符合条件的，不需要后端再清洗，速度极快，列表极满
            # 缺点：可能会漏掉一些无分级(Unrated)的纯成人片，但体验上比“便秘”好太多
            tmdb_params['certification_country'] = 'US'
            tmdb_params['certification'] = 'NC-17'
        else:
            tmdb_params['include_adult'] = 'false'
            
        # 普通分级逻辑 (成人模式下已手动指定了 NC-17，所以跳过这里)
        if rating_label and not is_adult_search:
            rating_params = _get_tmdb_rating_params(rating_label, 'Movie')
            tmdb_params.update(rating_params)
            
        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        # --- 翻页补货逻辑 ---
        
        final_results = []
        target_count = 20
        
        # 因为加了 NC-17 强过滤，TMDb 返回的数据密度会很高，
        # 所以不需要像之前那样扫 20 页，扫 3 页通常就绰绰有余了
        max_pages_to_scan = 3 if is_adult_search else 1 
        
        frontend_page = data.get('page', 1)
        start_tmdb_page = (frontend_page - 1) * max_pages_to_scan + 1
        current_tmdb_page = start_tmdb_page
        total_pages_from_tmdb = 0 
        
        TIMEOUT_LIMIT = 20 
        
        for i in range(max_pages_to_scan):
            if time.time() - start_time > TIMEOUT_LIMIT:
                break

            tmdb_params['page'] = current_tmdb_page
            tmdb_data = tmdb.discover_movie_tmdb(api_key, tmdb_params)
            
            batch_results = tmdb_data.get('results', [])
            total_pages_from_tmdb = tmdb_data.get('total_pages', 0)
            
            # --- ★★★ 核心修改：移除严格的 adult=True 过滤 ★★★ ---
            # 既然我们已经信任了 certification=NC-17 这个筛选条件，
            # 那么只要有海报，我们就认为它是合格的“成人/大尺度”内容。
            # 这样列表瞬间就能填满。
            filtered_batch = [item for item in batch_results if item.get('poster_path')]
            
            final_results.extend(filtered_batch)
            
            if len(final_results) >= target_count:
                break
            
            if current_tmdb_page >= total_pages_from_tmdb:
                break
            
            current_tmdb_page += 1
            
        final_results = final_results[:target_count]
        
        if max_pages_to_scan > 1:
            adjusted_total_pages = total_pages_from_tmdb // max_pages_to_scan
            if adjusted_total_pages == 0 and total_pages_from_tmdb > 0:
                adjusted_total_pages = 1
        else:
            adjusted_total_pages = total_pages_from_tmdb
        
        result_wrapper = {
            "results": final_results,
            "page": frontend_page,
            "total_pages": adjusted_total_pages,
            "total_results": len(final_results) 
        }
        
        processed_data = _filter_and_enrich_results(result_wrapper, current_user_id, 'Movie')
        
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 发现电影时发生严重错误: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取电影数据失败，请检查网络或配置。"}), 500

@discover_bp.route('/tv', methods=['POST'])
@any_login_required
def discover_tv_shows():
    """
    根据前端传来的筛选条件，从 TMDb 发现电视剧。
    策略调整：成人模式下附加 TV-MA 筛选，保证列表密度和加载速度。
    """
    data = request.json
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)
    
    # 记录开始时间，用于超时控制
    start_time = time.time()

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']

        # 1. 关键词
        labels = data.get('with_keywords', [])
        if isinstance(labels, str): labels = labels.split(',')
        k_ids_str = _expand_keyword_labels_to_ids(labels)

        # 2. 工作室/平台
        studio_labels = data.get('with_companies', [])
        if isinstance(studio_labels, str): studio_labels = studio_labels.split(',')
        c_ids_str = _expand_studio_labels_to_ids(studio_labels, mode='tv')

        # 3. 构建基础参数
        tmdb_params = {
            'sort_by': data.get('sort_by', 'popularity.desc'),
            # 'page': data.get('page', 1), # page 在循环中控制
            'vote_average.gte': data.get('vote_average.gte', 0),
            'with_genres': data.get('with_genres', ''),
            'with_keywords': k_ids_str,
            'with_networks': c_ids_str, 
            'without_genres': data.get('without_genres', ''),
            'first_air_date.gte': data.get('first_air_date.gte', ''),
            'first_air_date.lte': data.get('first_air_date.lte', ''),
            'with_original_language': data.get('with_original_language', ''),
            'with_origin_country': data.get('with_origin_country', ''),
        }
        
        # 4. 处理分级筛选 
        rating_label = data.get('with_rating_label')
        
        allow_adult_config = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_INCLUDE_ADULT, False)
        mapping_data = settings_db.get_setting('rating_mapping') or DEFAULT_RATING_MAPPING
        adult_labels = set()
        if isinstance(mapping_data, dict):
            for country_rules in mapping_data.values():
                for rule in country_rules:
                    if rule.get('emby_value') == 15 and rule.get('label'):
                        adult_labels.add(rule['label'])
        if not adult_labels: adult_labels.add('成人')

        is_adult_search = False
        if allow_adult_config and rating_label in adult_labels:
            tmdb_params['include_adult'] = 'true'
            is_adult_search = True
            
            # --- ★★★ 核心修改：TV 版附加 TV-MA 筛选 ★★★ ---
            # 类似于电影的 NC-17，TV-MA 是美国电视分级中的“成熟观众”级别
            tmdb_params['watch_region'] = 'US'
            tmdb_params['with_content_ratings'] = 'TV-MA'
        else:
            tmdb_params['include_adult'] = 'false'
            
        # 普通分级逻辑
        if rating_label and not is_adult_search:
            rating_params = _get_tmdb_rating_params(rating_label, 'Series')
            tmdb_params.update(rating_params)

        tmdb_params = {k: v for k, v in tmdb_params.items() if v is not None and v != ''}

        # --- 翻页补货逻辑 ---
        
        final_results = []
        target_count = 20
        
        # 同样，因为加了 TV-MA 强过滤，命中率很高，扫 3 页足够
        max_pages_to_scan = 3 if is_adult_search else 1 
        
        frontend_page = data.get('page', 1)
        start_tmdb_page = (frontend_page - 1) * max_pages_to_scan + 1
        current_tmdb_page = start_tmdb_page
        total_pages_from_tmdb = 0 
        
        TIMEOUT_LIMIT = 20 

        for i in range(max_pages_to_scan):
            if time.time() - start_time > TIMEOUT_LIMIT:
                break

            tmdb_params['page'] = current_tmdb_page
            tmdb_data = tmdb.discover_tv_tmdb(api_key, tmdb_params)
            
            batch_results = tmdb_data.get('results', [])
            total_pages_from_tmdb = tmdb_data.get('total_pages', 0)
            
            # 只过滤海报，不强制过滤 adult=True，保证列表丰富度
            filtered_batch = [item for item in batch_results if item.get('poster_path')]
            
            final_results.extend(filtered_batch)
            
            if len(final_results) >= target_count:
                break
            
            if current_tmdb_page >= total_pages_from_tmdb:
                break
            
            current_tmdb_page += 1

        final_results = final_results[:target_count]
        
        if max_pages_to_scan > 1:
            adjusted_total_pages = total_pages_from_tmdb // max_pages_to_scan
            if adjusted_total_pages == 0 and total_pages_from_tmdb > 0:
                adjusted_total_pages = 1
        else:
            adjusted_total_pages = total_pages_from_tmdb

        result_wrapper = {
            "results": final_results,
            "page": frontend_page,
            "total_pages": adjusted_total_pages,
            "total_results": len(final_results) 
        }
        
        processed_data = _filter_and_enrich_results(result_wrapper, current_user_id, 'Series')
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 发现电视剧时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 获取数据失败"}), 500


@discover_bp.route('/genres/<string:media_type>', methods=['GET'])
@any_login_required
def get_genres(media_type):
    """获取电影或电视剧的类型列表。"""
    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)
    try:
        if media_type == 'movie':
            genres = tmdb.get_movie_genres_tmdb(api_key)
        elif media_type == 'tv':
            genres = tmdb.get_tv_genres_tmdb(api_key)
        else:
            return jsonify({"status": "error", "message": "无效的媒体类型"}), 400
        return jsonify(genres)
    except Exception as e:
        logger.error(f"获取 TMDb 类型列表时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "获取类型列表失败"}), 500
    
# ★★★ 搜索接口 ★★★
@discover_bp.route('/search', methods=['POST'])
@any_login_required
def search_media_handler():
    """
    根据前端传来的搜索词，从 TMDb 搜索影视。
    """
    data = request.json
    query = data.get('query')
    media_type = data.get('media_type', 'movie')
    page = data.get('page', 1)

    if not query:
        return jsonify({"status": "error", "message": "搜索词不能为空"}), 400

    api_key = tmdb.config_manager.APP_CONFIG.get(tmdb.constants.CONFIG_OPTION_TMDB_API_KEY)

    try:
        if 'emby_user_id' not in session:
            return jsonify({"status": "error", "message": "此功能仅对 Emby 用户开放"}), 403
        current_user_id = session['emby_user_id']
        
        # ★★★ 核心修改 4: 第三次调用辅助函数 ★★★
        tmdb_data = tmdb.search_media_for_discover(query=query, api_key=api_key, item_type=media_type, page=page)
        db_item_type = 'Movie' if media_type == 'movie' else 'Series'
        processed_data = _filter_and_enrich_results(tmdb_data, current_user_id, db_item_type)
        return jsonify(processed_data)

    except Exception as e:
        logger.error(f"TMDb 搜索 {media_type} 时出错: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "从 TMDb 搜索数据失败"}), 500
    
@discover_bp.route('/config/languages', methods=['GET'])
@any_login_required
def api_get_discover_languages():
    """为影视探索页面提供专用的、友好的常用语言列表。"""
    try:
        # 1. 优先从数据库读取
        data = settings_db.get_setting('language_mapping')
        
        # 2. 回落到默认值
        if isinstance(data, list) and data:
            mapping_list = data
        else:
            mapping_list = DEFAULT_LANGUAGE_MAPPING

        # 3. 格式化返回
        options = []
        for item in mapping_list:
            if item.get('label') and item.get('value'):
                options.append({
                    "label": item['label'],
                    "value": item['value']
                })
        return jsonify(options)
    except Exception as e:
        logger.error(f"获取 Discover 语言列表时出错: {e}", exc_info=True)
        return jsonify([]), 500
    
@discover_bp.route('/config/keywords', methods=['GET'])
@any_login_required
def api_get_discover_keywords():
    try:
        mapping_data = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        
        if isinstance(mapping_data, list):
            # 如果是列表，直接按列表顺序返回，保留用户的拖拽排序
            options = [{"label": item['label'], "value": item['label']} for item in mapping_data]
            return jsonify(options)
        else:
            # 如果是旧版字典，按标签名排序
            options = [{"label": k, "value": k} for k in mapping_data.keys()]
            return jsonify(sorted(options, key=lambda x: x['label']))
    except Exception as e:
        return jsonify([]), 500
    
@discover_bp.route('/config/studios', methods=['GET'])
@any_login_required
def api_get_discover_studios():
    try:
        mapping_data = settings_db.get_setting('studio_mapping') or DEFAULT_STUDIO_MAPPING
        
        # 统一转为列表
        if isinstance(mapping_data, dict):
            mapping_list = [{"label": k, **v} for k, v in mapping_data.items()]
            mapping_list.sort(key=lambda x: x['label'])
        else:
            mapping_list = mapping_data

        options = []
        for item in mapping_list:
            # ★★★ 核心修改：计算 types 字段传给前端 ★★★
            supported_types = []
            
            # 有 company_ids -> 支持电影
            if item.get('company_ids'): 
                supported_types.append('movie')
            
            # 有 network_ids -> 支持电视剧
            if item.get('network_ids'): 
                supported_types.append('tv')
            
            # 兼容旧数据：如果只有 ids，默认两者都支持
            if not supported_types and item.get('ids'):
                supported_types = ['movie', 'tv']
            
            options.append({
                "label": item['label'],
                "value": item['label'],
                "types": supported_types # 前端根据这个字段进行过滤
            })
            
        return jsonify(options)
    except Exception as e:
        logger.error(f"获取 Discover 工作室列表失败: {e}")
        return jsonify([]), 500

@discover_bp.route('/daily_recommendation', methods=['GET'])
@any_login_required
def get_recommendation_pool():
    """
    【V4 - 动态主题适配版】
    读取推荐池，并根据索引从动态映射表中获取主题名称。
    """
    try:
        pool_data = settings_db.get_setting('recommendation_pool')
        theme_index = settings_db.get_setting('recommendation_theme_index')

        # 1. 基础检查
        if not pool_data:
            # 如果为空，尝试触发一次更新（可选）
            return jsonify({"error": "推荐池尚未生成或为空。"}), 404

        # ✨ 2. 核心修改：适配 List 结构的映射表 ✨
        mapping_data = settings_db.get_setting('keyword_mapping') or DEFAULT_KEYWORD_MAPPING
        
        # 将数据统一转换为列表格式: [{'label': '丧尸', 'ids': [...]}, ...]
        theme_list = []
        if isinstance(mapping_data, list):
            theme_list = [item for item in mapping_data if item.get('ids')]
        elif isinstance(mapping_data, dict):
            # 兼容旧数据
            theme_list = [{'label': k, **v} for k, v in mapping_data.items() if v.get('ids')]

        theme_name = "今日精选" # 默认兜底名称
        
        if theme_index is not None and theme_list:
            # 确保索引在范围内
            # 使用取模运算防止数组越界（例如删除了几个关键词导致索引超出）
            safe_index = theme_index % len(theme_list)
            theme_name = theme_list[safe_index]['label']

        response_data = {
            "theme_name": theme_name,
            "pool": pool_data
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"读取推荐池数据时出错: {e}", exc_info=True)
        return jsonify({"error": "获取推荐池失败"}), 500
    
@discover_bp.route('/trigger_recommendation_update', methods=['POST'])
@any_login_required
def trigger_recommendation_update():
    """
    手动触发一次“每日推荐”更新任务。
    这是一个异步操作，接口会立即返回。
    """
    try:
        logger.debug("  ➜ 收到前端请求，自动触发【每日推荐】更新任务...")
        # 使用 task_manager 提交任务到后台执行
        task_manager.submit_task(
            task_function=task_update_daily_theme,
            task_name="自动更新每日推荐",
            processor_type='media' # 这个任务需要 'media' 类型的处理器
        )
        return jsonify({"status": "ok", "message": "更新任务已在后台启动。"}), 202
    except Exception as e:
        logger.error(f"自动触发每日推荐任务时失败: {e}", exc_info=True)
        return jsonify({"error": "启动任务失败"}), 500
    
def check_and_replenish_pool():
    """
    【V2 - 修正版】
    检查推荐池库存，如果低于阈值则触发后台补充任务。
    这个函数应该在订阅成功后被调用。
    """
    try:
        # ★ 核心修正：分两步安全地获取推荐池数据
        # 1. 先用正确的单个参数获取设置
        pool_data = settings_db.get_setting('recommendation_pool')
        # 2. 如果返回的是 None (比如第一次运行还没有这个设置)，则视为空列表
        pool = pool_data or []
        
        # 定义库存阈值
        REPLENISH_THRESHOLD = 5 

        if len(pool) < REPLENISH_THRESHOLD:
            logger.debug(f"  ➜ 推荐池库存 ({len(pool)}) 低于阈值 ({REPLENISH_THRESHOLD})，触发后台补充任务。")
            task_manager.submit_task(
                task_function=task_replenish_recommendation_pool,
                task_name="补充每日推荐池",
                processor_type='media'
            )
        else:
            logger.debug(f"  ➜ 推荐池库存充足 ({len(pool)})，无需补充。")
            
    except Exception as e:
        logger.error(f"检查并补充推荐池时出错: {e}", exc_info=True)