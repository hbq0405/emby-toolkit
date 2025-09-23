# routes/custom_collections.py

from flask import Blueprint, request, jsonify
import logging
import json
import psycopg2
import pytz
from datetime import datetime
import constants
from database import user_db
from database import collection_db
from database import connection
from database import settings_db
import config_manager
import task_manager
import moviepilot_handler
import emby_handler
from extensions import login_required
from custom_collection_handler import FilterEngine
from utils import get_country_translation_map, UNIFIED_RATING_CATEGORIES, get_tmdb_country_options
from tmdb_handler import get_movie_genres_tmdb, get_tv_genres_tmdb, search_companies_tmdb, search_person_tmdb
# 1. 创建自定义合集蓝图
custom_collections_bp = Blueprint('custom_collections', __name__, url_prefix='/api/custom_collections')

logger = logging.getLogger(__name__)

GENRE_TRANSLATION_PATCH = {
    "Sci-Fi & Fantasy": "科幻&奇幻",
    "War & Politics": "战争&政治",
    # 以后如果发现其他未翻译的，也可以加在这里
}

# 2. 定义API路由

# ★★★ 获取 Emby 用户列表 ★★★
@custom_collections_bp.route('/config/emby_users', methods=['GET'])
@login_required
def api_get_emby_users():
    """为权限设置提供一个可选的 Emby 用户列表。"""
    try:
        # 1. 尝试从我们的本地缓存读取
        cached_users = user_db.get_all_emby_users()
        
        # 2. 如果缓存为空，则实时从 Emby 获取并更新缓存
        if not cached_users:
            logger.info("本地Emby用户缓存为空，正在从Emby实时获取...")
            emby_url = config_manager.APP_CONFIG.get('emby_server_url')
            emby_key = config_manager.APP_CONFIG.get('emby_api_key')
            
            if not all([emby_url, emby_key]):
                return jsonify({"error": "Emby服务器配置不完整"}), 500
            
            # 这个函数需要你自己添加到 emby_handler.py 中
            live_users = emby_handler.get_all_emby_users_from_server(emby_url, emby_key)
            
            if live_users is None:
                return jsonify({"error": "无法从Emby获取用户列表"}), 500
            
            # 更新缓存
            user_db.upsert_emby_users_batch(live_users)
            cached_users = live_users

        # 3. 格式化为前端需要的格式
        user_options = [{
            "label": user.get('name'),
            "value": user.get('id')
        } for user in cached_users]
        
        return jsonify(user_options)
    except Exception as e:
        logger.error(f"获取 Emby 用户列表时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取所有自定义合集定义 ---
@custom_collections_bp.route('', methods=['GET']) # 原为 '/'
@login_required
def api_get_all_custom_collections():
    """获取所有自定义合集定义 (V3.1 - 最终修正版)"""
    try:
        beijing_tz = pytz.timezone('Asia/Shanghai')
        collections_from_db = collection_db.get_all_custom_collections()
        processed_collections = []

        for collection in collections_from_db:
            # --- 处理 definition (这部分逻辑不变) ---
            definition_data = collection.get('definition_json')
            parsed_definition = {}
            if isinstance(definition_data, str):
                try:
                    obj = json.loads(definition_data)
                    if isinstance(obj, str): obj = json.loads(obj)
                    if isinstance(obj, dict): parsed_definition = obj
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"合集 ID {collection.get('id')} 的 definition_json 无法解析。")
            elif isinstance(definition_data, dict):
                parsed_definition = definition_data
            collection['definition'] = parsed_definition
            if 'definition_json' in collection:
                del collection['definition_json']

            # ==========================================================
            # ★★★ 修正后的时区转换逻辑 ★★★
            # ==========================================================
            
            # 使用从数据库截图中确认的正确字段名 'last_synced_at'
            key_for_timestamp = 'last_synced_at' 

            if key_for_timestamp in collection and collection[key_for_timestamp]:
                timestamp_val = collection[key_for_timestamp]
                utc_dt = None

                # 数据库字段是 "timestamp with time zone"，psycopg2 会将其转为带时区的 datetime 对象
                if isinstance(timestamp_val, datetime):
                    utc_dt = timestamp_val
                
                # 为防止意外，也兼容一下字符串格式
                elif isinstance(timestamp_val, str):
                    try:
                        ts_str_clean = timestamp_val.split('.')[0]
                        naive_dt = datetime.strptime(ts_str_clean, '%Y-%m-%d %H:%M:%S')
                        utc_dt = pytz.utc.localize(naive_dt)
                    except ValueError:
                        logger.warning(f"无法将字符串 '{timestamp_val}' 解析为时间，跳过转换。")

                # 如果成功获取到 UTC 时间对象，则进行转换
                if utc_dt:
                    beijing_dt = utc_dt.astimezone(beijing_tz)
                    collection[key_for_timestamp] = beijing_dt.strftime('%Y-%m-%d %H:%M:%S')

            processed_collections.append(collection)

        return jsonify(processed_collections)
    except Exception as e:
        logger.error(f"获取所有自定义合集时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 创建一个新的自定义合集定义 ---
@custom_collections_bp.route('', methods=['POST'])
@login_required
def api_create_custom_collection():
    """创建一个新的自定义合集定义"""
    data = request.json
    name = data.get('name')
    type = data.get('type')
    definition = data.get('definition')
    # ★★★ 新增：获取权限列表 ★★★
    allowed_user_ids = data.get('allowed_user_ids')

    if not all([name, type, definition]):
        return jsonify({"error": "请求无效: 缺少 name, type, 或 definition"}), 400
    
    definition_json = json.dumps(definition, ensure_ascii=False)
    # ★★★ 新增：处理权限列表 ★★★
    allowed_user_ids_json = json.dumps(allowed_user_ids) if isinstance(allowed_user_ids, list) else None
    
    try:
        collection_id = collection_db.create_custom_collection(name, type, definition_json, allowed_user_ids_json)
        new_collection = collection_db.get_custom_collection_by_id(collection_id)
        return jsonify(new_collection), 201
    except psycopg2.IntegrityError:
        return jsonify({"error": f"创建失败：名为 '{name}' 的合集已存在。"}), 409
    except Exception as e:
        logger.error(f"创建自定义合集 '{name}' 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "数据库操作失败，无法创建合集，请检查后端日志。"}), 500

# --- 更新一个自定义合集定义 ---
@custom_collections_bp.route('/<int:collection_id>', methods=['PUT'])
@login_required
def api_update_custom_collection(collection_id):
    """更新一个自定义合集定义"""
    try:
        data = request.json
        name = data.get('name')
        type = data.get('type')
        definition = data.get('definition')
        status = data.get('status')
        # ★★★ 新增：获取权限列表 ★★★
        allowed_user_ids = data.get('allowed_user_ids')

        if not all([name, type, definition, status]):
            return jsonify({"error": "请求无效: 缺少必要参数"}), 400
        
        definition_json = json.dumps(definition, ensure_ascii=False)
        # ★★★ 新增：处理权限列表 ★★★
        allowed_user_ids_json = json.dumps(allowed_user_ids) if isinstance(allowed_user_ids, list) else None
        
        success = collection_db.update_custom_collection(
            collection_id, name, type, definition_json, status, allowed_user_ids_json
        )
        
        if success:
            updated_collection = collection_db.get_custom_collection_by_id(collection_id)
            return jsonify(updated_collection)
        else:
            return jsonify({"error": "数据库操作失败，未找到或无法更新该合集"}), 404
            
    except Exception as e:
        logger.error(f"更新自定义合集 {collection_id} 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误，请检查后端日志"}), 500

# ★★★ 更新合集排序的API ★★★
@custom_collections_bp.route('/update_order', methods=['POST'])
@login_required
def api_update_custom_collections_order():
    """接收前端发来的新顺序并更新到数据库"""
    data = request.json
    ordered_ids = data.get('ids')

    if not isinstance(ordered_ids, list):
        return jsonify({"error": "请求无效: 需要一个ID列表。"}), 400

    try:
        success = collection_db.update_custom_collections_order(ordered_ids)
        if success:
            return jsonify({"message": "合集顺序已成功更新。"}), 200
        else:
            return jsonify({"error": "数据库操作失败，无法更新顺序。"}), 500
    except Exception as e:
        logger.error(f"更新自定义合集顺序时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 联动删除Emby合集 ---
@custom_collections_bp.route('/<int:collection_id>', methods=['DELETE'])
@login_required
def api_delete_custom_collection(collection_id):
    """【V8 - 最终决战版】通过清空所有成员来联动删除Emby合集"""
    try:
        # 步骤 1: 获取待删除合集的完整信息
        collection_to_delete = collection_db.get_custom_collection_by_id(collection_id)
        if not collection_to_delete:
            return jsonify({"error": "未找到要删除的合集"}), 404

        emby_id_to_empty = collection_to_delete.get('emby_collection_id')
        collection_name = collection_to_delete.get('name')

        # 步骤 2: 如果存在关联的Emby ID，则调用Emby Handler，清空其内容
        if emby_id_to_empty:
            logger.info(f"  -> 正在删除合集 '{collection_name}' (Emby ID: {emby_id_to_empty})...")
            
            # ★★★ 调用我们全新的、真正有效的清空函数 ★★★
            emby_handler.empty_collection_in_emby(
                collection_id=emby_id_to_empty,
                base_url=config_manager.APP_CONFIG.get('emby_server_url'),
                api_key=config_manager.APP_CONFIG.get('emby_api_key'),
                user_id=config_manager.APP_CONFIG.get('emby_user_id')
            )

        # 步骤 3: 无论Emby端是否成功，都删除本地数据库中的记录
        db_success = collection_db.delete_custom_collection(
            collection_id=collection_id
        )

        if db_success:
            return jsonify({"message": f"自定义合集 '{collection_name}' 已成功联动删除。"}), 200
        else:
            return jsonify({"error": "数据库删除操作失败，请查看日志。"}), 500

    except Exception as e:
        logger.error(f"删除自定义合集 {collection_id} 时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取单个自定义合集健康状态 ---
@custom_collections_bp.route('/<int:collection_id>/status', methods=['GET'])
@login_required
def api_get_custom_collection_status(collection_id):
    """
    【V3 - 最终健壮修复版】
    获取单个自定义合集的详情，确保 definition 字段始终为正确的对象格式。
    - 解决了编辑框因 definition 格式错误而无法加载规则的致命BUG。
    """
    try:
        collection_details = collection_db.get_custom_collection_by_id(collection_id)
        if not collection_details:
            return jsonify({"error": "未在自定义合集表中找到该合集"}), 404
        
        # 为“健康状态”弹窗准备 media_items 字段
        collection_details['media_items'] = collection_details.get('generated_media_info_json', [])
        
        # ★★★ 核心修复：确保 definition 是一个对象，而不是字符串 ★★★
        definition_data = collection_details.get('definition_json')
        
        if isinstance(definition_data, str):
            try:
                # 先尝试解析一次
                obj = json.loads(definition_data)
                # 如果解析结果依然是字符串，尝试再解析一次（双重序列化情况）
                if isinstance(obj, str):
                    obj = json.loads(obj)
                collection_details['definition'] = obj if isinstance(obj, dict) else {}
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"合集 {collection_id} 的 definition_json 字段无法被解析为JSON，内容: {definition_data}, 错误: {e}")
                collection_details['definition'] = {}  # 解析失败，返回空对象
        elif isinstance(definition_data, dict):
            # 如果它已经是字典（psycopg2自动解析JSONB字段），则直接使用
            collection_details['definition'] = definition_data
        else:
            # 其他意外情况（如None），提供一个空的默认值
            collection_details['definition'] = {}
        
        # 现在，我们可以安全地删除那些体积巨大或不再需要的原始字段
        if 'generated_media_info_json' in collection_details:
            del collection_details['generated_media_info_json']
        if 'definition_json' in collection_details:
            del collection_details['definition_json']
            
        return jsonify(collection_details)
            
    except Exception as e:
        logger.error(f"读取单个自定义合集状态 {collection_id} 时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 更新自定义合集中单个媒体项状态 ---
@custom_collections_bp.route('/<int:collection_id>/media_status', methods=['POST'])
@login_required
def api_update_custom_collection_media_status(collection_id):
    """更新自定义合集中单个媒体项的状态 (e.g., subscribed -> missing)"""
    data = request.json
    media_tmdb_id = data.get('tmdb_id')
    new_status = data.get('new_status')

    if not all([media_tmdb_id, new_status]):
        return jsonify({"error": "请求无效: 缺少 tmdb_id 或 new_status"}), 400

    try:
        success = collection_db.update_single_media_status_in_custom_collection(
            collection_id=collection_id,
            media_tmdb_id=str(media_tmdb_id),
            new_status=new_status
        )
        if success:
            return jsonify({"message": "状态更新成功"})
        else:
            return jsonify({"error": "更新失败，未找到对应的媒体项或合集"}), 404
    except Exception as e:
        logger.error(f"更新自定义合集 {collection_id} 中媒体状态时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 修正榜单合集中的媒体匹配 ---
@custom_collections_bp.route('/<int:collection_id>/fix_match', methods=['POST'])
@login_required
def api_fix_media_match_in_custom_collection(collection_id):
    """
    【新增】修正榜单合集中一个错误的媒体匹配项。
    """
    data = request.json
    old_tmdb_id = data.get('old_tmdb_id')
    new_tmdb_id = data.get('new_tmdb_id')

    if not all([old_tmdb_id, new_tmdb_id]):
        return jsonify({"error": "请求无效: 缺少 old_tmdb_id 或 new_tmdb_id"}), 400

    try:
        corrected_item = collection_db.apply_and_persist_media_correction(
            collection_id=collection_id,
            old_tmdb_id=str(old_tmdb_id),
            new_tmdb_id=str(new_tmdb_id)
        )
        if corrected_item:
            return jsonify({
                "message": "修正成功！",
                "corrected_item": corrected_item
            })
        else:
            return jsonify({"error": "修正失败，可能是TMDb ID无效或数据库错误"}), 404
    except Exception as e:
        logger.error(f"修正合集 {collection_id} 媒体匹配时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 手动订阅 ---
@custom_collections_bp.route('/subscribe', methods=['POST'])
@login_required
def api_subscribe_media_from_custom_collection():
    """
    【PG JSON 兼容 & 季号精确订阅修复版】从RSS榜单合集页面手动订阅。
    """
    data = request.json
    tmdb_id = data.get('tmdb_id')
    collection_id = data.get('collection_id')
    if not all([tmdb_id, collection_id]):
        return jsonify({"error": "请求无效: 缺少 tmdb_id 或 collection_id"}), 400
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT definition_json, generated_media_info_json FROM custom_collections WHERE id = %s", (collection_id,))
            collection_record = cursor.fetchone()
            if not collection_record:
                return jsonify({"error": "数据库错误: 找不到指定的合集。"}), 404
            definition = collection_record['definition_json']
            item_type_from_db = definition.get('item_type', 'Movie')
            authoritative_type = None
            if isinstance(item_type_from_db, list):
                if item_type_from_db:
                    authoritative_type = item_type_from_db[0]
            elif isinstance(item_type_from_db, str):
                authoritative_type = item_type_from_db
            
            if authoritative_type not in ['Movie', 'Series']:
                logger.warning(f"合集 {collection_id} 的 item_type 格式无法识别 ('{item_type_from_db}')，将默认使用 'Movie' 进行订阅。")
                authoritative_type = 'Movie'
            media_list = collection_record.get('generated_media_info_json') or []
            target_media_item = next((item for item in media_list if str(item.get('tmdb_id')) == str(tmdb_id)), None)
            if not target_media_item:
                return jsonify({"error": "订阅失败: 在该合集的媒体列表中未找到此项目。"}), 404
            
            authoritative_title = target_media_item.get('title')
            if not authoritative_title:
                return jsonify({"error": "订阅失败: 数据库中的媒体信息不完整（缺少标题）。"}), 500
            season_to_subscribe = target_media_item.get('season')

        # --- 新增: 配额检查 ---
        current_quota = settings_db.get_subscription_quota()
        if current_quota <= 0:
            logger.warning(f"API: 用户尝试订阅《{authoritative_title}》，但每日配额已用尽。")
            return jsonify({"error": "今日订阅配额已用尽，请明天再试。"}), 429

        type_map = {'Movie': '电影', 'Series': '电视剧'}
        
        # 更新日志记录，如果订阅的是带特定季号的剧集，则明确指出
        log_message_detail = ""
        if authoritative_type == 'Series' and season_to_subscribe is not None:
            log_message_detail = f" 第 {season_to_subscribe} 季"
        logger.info(f"  -> 依据合集定义，使用类型 '{type_map.get(authoritative_type, authoritative_type)}' 为《{authoritative_title}》{log_message_detail}(TMDb ID: {tmdb_id}) 发起订阅...")
        
        success = False
        if authoritative_type == 'Movie':
            movie_info = {"tmdb_id": tmdb_id, "title": authoritative_title}
            success = moviepilot_handler.subscribe_movie_to_moviepilot(movie_info, config_manager.APP_CONFIG)
        elif authoritative_type == 'Series':
            series_info = {"tmdb_id": tmdb_id, "title": authoritative_title}
            success = moviepilot_handler.subscribe_series_to_moviepilot(series_info, season_number=season_to_subscribe, config=config_manager.APP_CONFIG)
        
        if not success:
            return jsonify({"error": "提交到 MoviePilot 失败，请检查日志。"}), 500

        # 成功后扣配额
        settings_db.decrement_subscription_quota()

        target_media_item['status'] = 'subscribed'
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            new_missing_count = sum(1 for item in media_list if item.get('status') == 'missing')
            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
            new_media_info_json = json.dumps(media_list, ensure_ascii=False)
            cursor.execute(
                "UPDATE custom_collections SET generated_media_info_json = %s, health_status = %s, missing_count = %s WHERE id = %s",
                (new_media_info_json, new_health_status, new_missing_count, collection_id)
            )
            conn.commit()
            logger.info(f"  -> 已成功更新合集 {collection_id} 中《{authoritative_title}》的状态为 '订阅中'。")

        return jsonify({"message": f"《{authoritative_title}》已成功提交订阅，并已更新本地状态。"}), 200
    except Exception as e:
        logger.error(f"处理订阅请求时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "处理订阅时发生服务器内部错误。"}), 500
    
# --- 根据关键词搜索演员 ---
@custom_collections_bp.route('/search_actors') # 或者 @media_api_bp.route('/search_actors')
@login_required
def api_search_actors():
    search_term = request.args.get('q', '')
    if len(search_term) < 1:
        return jsonify([])
    
    try:
        actors = collection_db.search_unique_actors(search_term)
        # 返回简单的字符串列表
        return jsonify(actors)
    except Exception as e:
        logger.error(f"搜索演员API出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 提取国家列表 ---
@custom_collections_bp.route('/config/countries', methods=['GET'])
@login_required
def api_get_countries_for_filter():
    """【重构版】为筛选器提供一个纯中文的国家/地区列表。"""
    try:
        # get_country_translation_map 返回 {'英文': '中文', ...}
        # 我们需要的是所有的中文值
        full_map = get_country_translation_map()
        # 使用 set 去重，然后排序
        chinese_names = sorted(list(set(full_map.values())))
        return jsonify(chinese_names)
    except Exception as e:
        logger.error(f"获取国家/地区列表时出错: {e}", exc_info=True)
        return jsonify([]), 500
    
# --- 提取标签列表 ---
@custom_collections_bp.route('/config/tags', methods=['GET'])
@login_required
def api_get_tags_for_filter():
    """为筛选器提供一个标签列表。"""
    try:
        tags = collection_db.get_unique_tags()
        return jsonify(tags)
    except Exception as e:
        logger.error(f"获取标签列表时出错: {e}", exc_info=True)
        return jsonify([]), 500

@custom_collections_bp.route('/config/unified_ratings', methods=['GET'])
@login_required
def api_get_unified_ratings_for_filter():
    """为筛选器提供一个固定的、统一的分级列表。"""
    # 直接返回我们预定义好的分类列表
    return jsonify(UNIFIED_RATING_CATEGORIES)
# --- 获取 Emby 媒体库列表 ---
@custom_collections_bp.route('/config/emby_libraries', methods=['GET'])
@login_required
def api_get_emby_libraries_for_filter():
    """为筛选器提供一个可选的 Emby 媒体库列表。"""
    try:
        # 从配置中获取必要的 Emby 连接信息
        emby_url = config_manager.APP_CONFIG.get('emby_server_url')
        emby_key = config_manager.APP_CONFIG.get('emby_api_key')
        emby_user_id = config_manager.APP_CONFIG.get('emby_user_id')

        if not all([emby_url, emby_key, emby_user_id]):
            return jsonify({"error": "Emby 服务器配置不完整"}), 500

        # 调用 emby_handler 获取原始的媒体库/视图列表
        all_views = emby_handler.get_emby_libraries(emby_url, emby_key, emby_user_id)
        if all_views is None:
            return jsonify({"error": "无法从 Emby 获取媒体库列表"}), 500

        # 筛选出真正的媒体库（电影、电视剧类型）并格式化为前端需要的格式
        library_options = []
        for view in all_views:
            collection_type = view.get('CollectionType')
            if collection_type in ['movies', 'tvshows']:
                library_options.append({
                    "label": view.get('Name'),
                    "value": view.get('Id')
                })
        
        return jsonify(library_options)
    except Exception as e:
        logger.error(f"获取 Emby 媒体库列表时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 获取 TMDb 电影类型列表 ---
@custom_collections_bp.route('/config/tmdb_movie_genres', methods=['GET'])
@login_required
def api_get_tmdb_movie_genres():
    """【V2 - 汉化补丁版】为 TMDb 探索助手提供电影类型列表。"""
    try:
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        if not api_key:
            return jsonify({"error": "TMDb API Key 未配置"}), 500
        
        genres = get_movie_genres_tmdb(api_key)
        if genres is not None:
            # ★★★ 在这里应用汉化补丁 ★★★
            for genre in genres:
                if genre['name'] in GENRE_TRANSLATION_PATCH:
                    genre['name'] = GENRE_TRANSLATION_PATCH[genre['name']]
            return jsonify(genres)
        else:
            return jsonify({"error": "从 TMDb 获取电影类型失败"}), 500
    except Exception as e:
        logger.error(f"获取 TMDb 电影类型时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取 TMDb 电视剧类型列表 ---
@custom_collections_bp.route('/config/tmdb_tv_genres', methods=['GET'])
@login_required
def api_get_tmdb_tv_genres():
    """【V2 - 汉化补丁版】为 TMDb 探索助手提供电视剧类型列表。"""
    try:
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        if not api_key:
            return jsonify({"error": "TMDb API Key 未配置"}), 500
            
        genres = get_tv_genres_tmdb(api_key)
        if genres is not None:
            # ★★★ 在这里也应用汉化补丁 ★★★
            for genre in genres:
                if genre['name'] in GENRE_TRANSLATION_PATCH:
                    genre['name'] = GENRE_TRANSLATION_PATCH[genre['name']]
            return jsonify(genres)
        else:
            return jsonify({"error": "从 TMDb 获取电视剧类型失败"}), 500
    except Exception as e:
        logger.error(f"获取 TMDb 电视剧类型时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 搜索 TMDb 电影公司 ---
@custom_collections_bp.route('/config/tmdb_search_companies', methods=['GET'])
@login_required
def api_search_tmdb_companies():
    """为 TMDb 探索助手提供电影公司搜索功能。"""
    query = request.args.get('q', '')
    if len(query) < 1:
        return jsonify([])
    try:
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        results = search_companies_tmdb(api_key, query)
        return jsonify(results or [])
    except Exception as e:
        logger.error(f"搜索 TMDb 电影公司时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 搜索 TMDb 人物 (演员/导演) ---
@custom_collections_bp.route('/config/tmdb_search_persons', methods=['GET'])
@login_required
def api_search_tmdb_persons():
    """【V2 - 增强版】为 TMDb 探索助手提供带详细信息的人物搜索功能。"""
    query = request.args.get('q', '')
    if len(query) < 1:
        return jsonify([])
    try:
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        results = search_person_tmdb(query, api_key)
        
        if not results:
            return jsonify([])

        # ★★★ 核心升级：处理数据，返回前端需要的所有信息 ★★★
        processed_results = []
        for person in results:
            # 提取代表作的标题
            known_for_titles = [
                item.get('title') or item.get('name', '') 
                for item in person.get('known_for', [])
            ]
            # 过滤掉空标题并拼接
            known_for_string = '、'.join(filter(None, known_for_titles))

            processed_results.append({
                "id": person.get("id"),
                "name": person.get("name"),
                "profile_path": person.get("profile_path"),
                "department": person.get("known_for_department"),
                "known_for": known_for_string
            })
        
        return jsonify(processed_results)

    except Exception as e:
        logger.error(f"搜索 TMDb 人物时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 获取 TMDb 国家/地区选项列表 ---
@custom_collections_bp.route('/config/tmdb_countries', methods=['GET'])
@login_required
def api_get_tmdb_countries():
    """为 TMDb 探索助手提供国家/地区选项列表 (含ISO代码)。"""
    try:
        # 调用我们刚刚在 utils.py 中创建的新函数
        country_options = get_tmdb_country_options()
        return jsonify(country_options)
    except Exception as e:
        logger.error(f"获取 TMDb 国家/地区选项时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500