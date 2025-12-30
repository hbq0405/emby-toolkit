# routes/custom_collections.py

from flask import Blueprint, request, jsonify
import logging
from gevent import spawn_later
import json
import psycopg2
import pytz
from datetime import datetime
from database import custom_collection_db, user_db, connection, settings_db
import config_manager
import handler.emby as emby
from tasks.helpers import is_movie_subscribable
from extensions import admin_required, any_login_required, DELETING_COLLECTIONS
from utils import UNIFIED_RATING_CATEGORIES, DEFAULT_KEYWORD_MAPPING, DEFAULT_STUDIO_MAPPING, DEFAULT_COUNTRY_MAPPING, DEFAULT_LANGUAGE_MAPPING
from handler.tmdb import get_movie_genres_tmdb, get_tv_genres_tmdb, search_companies_tmdb, search_person_tmdb, search_tv_tmdb, get_tv_details
# 1. 创建自定义合集蓝图
custom_collections_bp = Blueprint('custom_collections', __name__, url_prefix='/api/custom_collections')

logger = logging.getLogger(__name__)

GENRE_TRANSLATION_PATCH = {
    "Sci-Fi & Fantasy": "科幻&奇幻",
    "War & Politics": "战争&政治",
    # 以后如果发现其他未翻译的，也可以加在这里
}

# 辅助函数：确保数据是列表格式 (用于兼容旧数据)
def ensure_list_format(data, default_list):
    if not data:
        return default_list
    # 如果是旧的字典格式，转换为列表 (按键名排序，因为字典无序)
    if isinstance(data, dict):
        converted = []
        for label, info in data.items():
            item = {"label": label}
            item.update(info)
            converted.append(item)
        # 旧数据转换时只能按名称排序，无法恢复用户想要的顺序
        return sorted(converted, key=lambda x: x['label'])
    # 如果已经是列表，直接返回 (保留顺序)
    if isinstance(data, list):
        return data
    return default_list

# ★★★ 获取 Emby 用户列表 ★★★
@custom_collections_bp.route('/config/emby_users', methods=['GET'])
@admin_required
def api_get_emby_users():
    """为权限设置提供一个可选的 Emby 用户列表。"""
    try:
        all_users = user_db.get_all_emby_users_with_template_info()
        if not all_users:
            # ... (从Emby获取用户的逻辑保持不变) ...
            all_users = user_db.get_all_emby_users_with_template_info()

        # ★★★ 核心修改：调用新的数据库函数，不再自己管理连接 ★★★
        template_source_ids = user_db.get_template_source_user_ids()
        
        filtered_users = []
        for user in all_users:
            is_source_user = user['id'] in template_source_ids
            is_bound_to_template = user['template_id'] is not None
            
            if is_source_user or not is_bound_to_template:
                option = {
                    "label": user.get('name'),
                    "value": user.get('id'),
                    "is_template_source": is_source_user 
                }
                filtered_users.append(option)
        
        return jsonify(filtered_users)
    except Exception as e:
        logger.error(f"获取 Emby 用户列表时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取所有自定义合集定义 ---
@custom_collections_bp.route('', methods=['GET']) # 原为 '/'
@admin_required
def api_get_all_custom_collections():
    """获取所有自定义合集定义 (V6 - 轻量化版)"""
    try:
        beijing_tz = pytz.timezone('Asia/Shanghai')
        collections_from_db = custom_collection_db.get_all_custom_collections()
        processed_collections = []

        for collection in collections_from_db:
            # 1. 解析 definition
            definition_data = collection.get('definition_json')
            parsed_definition = {}
            if isinstance(definition_data, str):
                try:
                    obj = json.loads(definition_data)
                    if isinstance(obj, str): obj = json.loads(obj)
                    if isinstance(obj, dict): parsed_definition = obj
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(definition_data, dict):
                parsed_definition = definition_data
            collection['definition'] = parsed_definition
            if 'definition_json' in collection:
                del collection['definition_json']

            # 2. 清理不再需要的字段
            if 'generated_media_info_json' in collection:
                del collection['generated_media_info_json']

            # 3. 时区转换
            key_for_timestamp = 'last_synced_at' 
            if key_for_timestamp in collection and collection[key_for_timestamp]:
                timestamp_val = collection[key_for_timestamp]
                utc_dt = None
                if isinstance(timestamp_val, datetime):
                    utc_dt = timestamp_val
                elif isinstance(timestamp_val, str):
                    try:
                        ts_str_clean = timestamp_val.split('.')[0]
                        naive_dt = datetime.strptime(ts_str_clean, '%Y-%m-%d %H:%M:%S')
                        utc_dt = pytz.utc.localize(naive_dt)
                    except ValueError: pass
                if utc_dt:
                    beijing_dt = utc_dt.astimezone(beijing_tz)
                    collection[key_for_timestamp] = beijing_dt.strftime('%Y-%m-%d %H:%M:%S')

            # ★★★ 移除 missing_count 和 health_status 计算 ★★★
            # 因为筛选类合集现在是实时的，没有“缺失”的概念。
            # 榜单类合集的健康状态可以在详情页单独获取，列表页不再展示以提高速度。
            collection['missing_count'] = 0
            collection['health_status'] = 'ok'

            processed_collections.append(collection)

        return jsonify(processed_collections)
    except Exception as e:
        logger.error(f"获取所有自定义合集时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 创建一个新的自定义合集定义 ---
@custom_collections_bp.route('', methods=['POST'])
@admin_required
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
    # ★★★ 核心修改：在保存前展开模板用户 ★★★
    expanded_user_ids = user_db.expand_template_user_ids(allowed_user_ids)
    allowed_user_ids_json = json.dumps(expanded_user_ids) if isinstance(expanded_user_ids, list) else None
    
    try:
        collection_id = custom_collection_db.create_custom_collection(name, type, definition_json, allowed_user_ids_json)
        new_collection = custom_collection_db.get_custom_collection_by_id(collection_id)
        return jsonify(new_collection), 201
    except psycopg2.IntegrityError:
        return jsonify({"error": f"创建失败：名为 '{name}' 的合集已存在。"}), 409
    except Exception as e:
        logger.error(f"创建自定义合集 '{name}' 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "数据库操作失败，无法创建合集，请检查后端日志。"}), 500

# --- 更新一个自定义合集定义 ---
@custom_collections_bp.route('/<int:collection_id>', methods=['PUT'])
@admin_required
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
        # ★★★ 核心修改：在保存前展开模板用户 ★★★
        expanded_user_ids = user_db.expand_template_user_ids(allowed_user_ids)
        allowed_user_ids_json = json.dumps(expanded_user_ids) if isinstance(expanded_user_ids, list) else None
        
        success = custom_collection_db.update_custom_collection(
            collection_id, name, type, definition_json, status, allowed_user_ids_json
        )
        
        if success:
            updated_collection = custom_collection_db.get_custom_collection_by_id(collection_id)
            return jsonify(updated_collection)
        else:
            return jsonify({"error": "数据库操作失败，未找到或无法更新该合集"}), 404
            
    except Exception as e:
        logger.error(f"  ➜ 更新自定义合集 '{name}' 时发生严重错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误，请检查后端日志"}), 500

# ★★★ 更新合集排序的API ★★★
@custom_collections_bp.route('/update_order', methods=['POST'])
@admin_required
def api_update_custom_collections_order():
    """接收前端发来的新顺序并更新到数据库"""
    data = request.json
    ordered_ids = data.get('ids')

    if not isinstance(ordered_ids, list):
        return jsonify({"error": "请求无效: 需要一个ID列表。"}), 400

    try:
        success = custom_collection_db.update_custom_collections_order(ordered_ids)
        if success:
            return jsonify({"message": "合集顺序已成功更新。"}), 200
        else:
            return jsonify({"error": "数据库操作失败，无法更新顺序。"}), 500
    except Exception as e:
        logger.error(f"更新自定义合集顺序时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 联动删除Emby合集 ---
@custom_collections_bp.route('/<int:collection_id>', methods=['DELETE'])
@admin_required
def api_delete_custom_collection(collection_id):
    """【V8 - 最终决战版】通过清空所有成员来联动删除Emby合集"""
    try:
        # 步骤 1: 获取待删除合集的完整信息
        collection_to_delete = custom_collection_db.get_custom_collection_by_id(collection_id)
        if not collection_to_delete:
            return jsonify({"error": "未找到要删除的合集"}), 404

        items_json = collection_to_delete.get('generated_media_info_json')
        tmdb_to_clean = []
        if items_json:
            try:
                items = json.loads(items_json) if isinstance(items_json, str) else items_json
                tmdb_to_clean = [str(i['tmdb_id']) for i in items if i.get('tmdb_id')]
            except: pass

        emby_id_to_empty = collection_to_delete.get('emby_collection_id')
        collection_name = collection_to_delete.get('name')

        # 步骤 2: 如果存在关联的Emby ID，则调用Emby Handler，清空其内容
        if emby_id_to_empty:
            logger.info(f"  ➜ 正在删除合集 '{collection_name}' (Emby ID: {emby_id_to_empty})...")
            
            # ★★★ 核心修改：加入“免打扰名单” ★★★
            # ==================================================================
            DELETING_COLLECTIONS.add(emby_id_to_empty)
            
            # 20秒后移除标记 (自建合集涉及大量成员移除，Webhook可能会飞一会儿，给长一点时间)
            def _clear_flag():
                DELETING_COLLECTIONS.discard(emby_id_to_empty)
            spawn_later(20, _clear_flag)
            emby.empty_collection_in_emby(
                collection_id=emby_id_to_empty,
                base_url=config_manager.APP_CONFIG.get('emby_server_url'),
                api_key=config_manager.APP_CONFIG.get('emby_api_key'),
                user_id=config_manager.APP_CONFIG.get('emby_user_id')
            )

        # 步骤 3: 无论Emby端是否成功，都删除本地数据库中的记录
        db_success = custom_collection_db.delete_custom_collection(
            collection_id=collection_id
        )

        if db_success:
            from handler.poster_generator import cleanup_placeholder
            for tid in tmdb_to_clean:
                cleanup_placeholder(tid) 
            return jsonify({"message": f"自定义合集 '{collection_name}' 已成功联动删除。"}), 200
        else:
            return jsonify({"error": "数据库删除操作失败，请查看日志。"}), 500

    except Exception as e:
        logger.error(f"删除自定义合集 {collection_id} 时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取单个自定义合集健康状态 ---
@custom_collections_bp.route('/<int:collection_id>/status', methods=['GET'])
@admin_required
def api_get_custom_collection_status(collection_id):
    """
    获取合集详情 (V6 - 适配 Series+Season 结构)
    """
    try:
        collection = custom_collection_db.get_custom_collection_by_id(collection_id)
        if not collection:
            return jsonify({"error": "未找到合集"}), 404
        
        c_type = collection.get('type')
        if c_type == 'filter':
            collection['media_items'] = []
            collection['missing_count'] = 0
            collection['health_status'] = 'dynamic'
            return jsonify(collection)
        
        definition_list = collection.get('generated_media_info_json') or []
        if not definition_list:
            collection['media_items'] = []
            return jsonify(collection)

        # 1. 收集查询条件
        # 我们需要查两类数据：
        # A. 普通电影/剧集: tmdb_id + item_type
        # B. 季: parent_series_tmdb_id + season_number
        
        tmdb_ids_to_query = [] # 用于查 Movie/Series
        season_criteria = []   # 用于查 Season: (parent_id, season_num)

        for item in definition_list:
            tid = str(item.get('tmdb_id'))
            mtype = item.get('media_type')
            snum = item.get('season')
            
            if not tid or tid.lower() == 'none': continue
            
            if mtype == 'Series' and snum is not None:
                season_criteria.append((tid, snum))
            else:
                tmdb_ids_to_query.append(tid)

        # 2. 批量查询数据库
        media_in_db_map = {} # Key: "tmdb_id_type" 或 "parent_id_S_season_num"
        
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # A. 查询普通项
                    if tmdb_ids_to_query:
                        cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = ANY(%s)", (tmdb_ids_to_query,))
                        for row in cursor.fetchall():
                            key = f"{row['tmdb_id']}_{row['item_type']}"
                            media_in_db_map[key] = dict(row)
                    
                    # B. 查询季项 (使用 OR 组合查询，或者分批查)
                    # 为了简单高效，我们构造一个 WHERE (parent, season) IN (...) 的查询
                    if season_criteria:
                        # 构造 SQL 元组列表字符串: (id1, s1), (id2, s2)...
                        values_str = ",".join([f"('{p}', {s})" for p, s in season_criteria])
                        sql = f"""
                            SELECT * FROM media_metadata 
                            WHERE item_type = 'Season' 
                              AND (parent_series_tmdb_id, season_number) IN ({values_str})
                        """
                        cursor.execute(sql)
                        for row in cursor.fetchall():
                            # ★★★ 构造专用 Key: 父ID_S_季号 ★★★
                            key = f"{row['parent_series_tmdb_id']}_S_{row['season_number']}"
                            media_in_db_map[key] = dict(row)

        except Exception as e_db:
            logger.error(f"查询媒体元数据失败: {e_db}")

        # 3. 组装返回列表
        final_media_list = []
        dynamic_missing_count = 0

        for item_def in definition_list:
            tmdb_id = str(item_def.get('tmdb_id')) if item_def.get('tmdb_id') else None
            media_type = item_def.get('media_type') or 'Movie'
            season_number = item_def.get('season')
            source_title = item_def.get('title')

            if not tmdb_id:
                # ... (未识别处理保持不变) ...
                continue

            # ★★★ 确定查找 Key ★★★
            if media_type == 'Series' and season_number is not None:
                target_key = f"{tmdb_id}_S_{season_number}"
            else:
                target_key = f"{tmdb_id}_{media_type}"
            
            db_record = media_in_db_map.get(target_key)

            if db_record:
                status = "missing"
                if db_record.get('in_library'): status = "in_library"
                elif db_record.get('subscription_status') == 'SUBSCRIBED': status = "subscribed"
                elif db_record.get('subscription_status') == 'PAUSED': status = "paused"
                elif db_record.get('subscription_status') == 'IGNORED': status = "ignored"
                elif db_record.get('subscription_status') == 'PENDING_RELEASE': status = "unreleased"
                
                if status not in ('in_library', 'ignored'):
                    dynamic_missing_count += 1

                r_date = db_record.get('release_date')
                r_date_str = r_date.strftime('%Y-%m-%d') if isinstance(r_date, datetime) else (str(r_date) if r_date else '')

                final_media_list.append({
                    "tmdb_id": tmdb_id, # 前端需要剧集ID来跳转
                    "emby_id": db_record.get('emby_item_id'),
                    "title": source_title, # 使用合集定义的标题 (纯剧名)
                    "original_title": db_record.get('original_title'),
                    "release_date": r_date_str,
                    "poster_path": db_record.get('poster_path'),
                    "status": status,
                    "media_type": media_type,
                    "season": season_number
                })
            else:
                # 缺失
                final_media_list.append({
                    "tmdb_id": tmdb_id,
                    "emby_id": None,
                    "title": source_title,
                    "release_date": item_def.get('release_date', ''),
                    "poster_path": item_def.get('poster_path'),
                    "status": "missing",
                    "media_type": media_type,
                    "season": season_number
                })
                dynamic_missing_count += 1
        
        collection['media_items'] = final_media_list
        collection.pop('generated_media_info_json', None)
        collection['missing_count'] = dynamic_missing_count
        collection['health_status'] = 'has_missing' if dynamic_missing_count > 0 else 'ok'

        return jsonify(collection)
            
    except Exception as e:
        logger.error(f"实时生成合集状态 {collection_id} 时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 修正榜单合集中的媒体匹配 ---
@custom_collections_bp.route('/<int:collection_id>/fix_match', methods=['POST'])
@admin_required
def api_fix_media_match_in_custom_collection(collection_id):
    """
    修正榜单合集中一个错误的媒体匹配项。
    支持通过 old_tmdb_id (修正错误匹配) 或 old_title (修正未识别) 进行定位。
    """
    data = request.json
    old_tmdb_id = data.get('old_tmdb_id')
    new_tmdb_id = data.get('new_tmdb_id')
    season_number = data.get('season_number')
    # ★★★ 新增：获取旧标题 ★★★
    old_title = data.get('old_title')

    # 校验：新ID必须有，旧ID和旧标题至少要有一个
    if not new_tmdb_id:
        return jsonify({"error": "请求无效: 缺少 new_tmdb_id"}), 400
    
    if not old_tmdb_id and not old_title:
        return jsonify({"error": "请求无效: 必须提供 old_tmdb_id 或 old_title 以定位项目"}), 400

    try:
        # ★★★ 将 old_title 也传给数据库函数 ★★★
        corrected_item = custom_collection_db.apply_and_persist_media_correction(
            collection_id=collection_id,
            old_tmdb_id=str(old_tmdb_id) if old_tmdb_id else None,
            new_tmdb_id=str(new_tmdb_id),
            season_number=season_number,
            old_title=old_title 
        )
        
        if corrected_item:
            return jsonify({
                "message": "修正成功！",
                "corrected_item": corrected_item
            })
        else:
            return jsonify({"error": "修正失败，未找到对应的媒体项"}), 404
    except Exception as e:
        logger.error(f"修正合集 {collection_id} 媒体匹配时出错: {e}", exc_info=True)
        return jsonify({"error": f"服务器内部错误: {str(e)}"}), 500

# --- 筛选器用的标签列表 ---
@custom_collections_bp.route('/config/tags', methods=['GET'])
@admin_required
def api_get_tags_for_filter():
    """为筛选器提供一个标签列表。"""
    try:
        tags = custom_collection_db.get_unique_tags()
        return jsonify(tags)
    except Exception as e:
        logger.error(f"获取标签列表时出错: {e}", exc_info=True)
        return jsonify([]), 500

# --- 筛选器用的分级列表 ---
@custom_collections_bp.route('/config/unified_ratings', methods=['GET'])
@admin_required
def api_get_unified_ratings_for_filter():
    """为筛选器提供一个固定的、统一的分级列表。"""
    # 直接返回我们预定义好的分类列表
    return jsonify(UNIFIED_RATING_CATEGORIES)

# --- 筛选器用的媒体库列表 ---
@custom_collections_bp.route('/config/emby_libraries', methods=['GET'])
@admin_required
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
        all_views = emby.get_emby_libraries(emby_url, emby_key, emby_user_id)
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
@any_login_required
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
@any_login_required
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
@any_login_required
def api_search_tmdb_companies():
    """
    搜索 TMDb 实体。
    模式 1 (默认): 搜制作公司 (Company) -> 适用于电影
    模式 2 (type=network): 搜剧集反查平台 (Network) -> 适用于电视剧
    """
    query = request.args.get('q', '')
    search_type = request.args.get('type', 'company') # 新增参数
    
    if len(query) < 1:
        return jsonify([])
        
    try:
        api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
        
        if search_type == 'network':
            # --- 模式 2: 通过搜索电视剧来找 Network ---
            # 1. 搜剧集
            tv_results = search_tv_tmdb(api_key, {'query': query})
            if not tv_results or not tv_results.get('results'):
                return jsonify([])
            
            # 2. 取前 3 个结果，获取详情中的 networks
            candidates = tv_results['results'][:3]
            networks_found = {}
            
            for tv in candidates:
                details = get_tv_details(tv['id'], api_key)
                if details and details.get('networks'):
                    for net in details['networks']:
                        # 去重
                        if net['id'] not in networks_found:
                            networks_found[net['id']] = {
                                "id": net['id'],
                                "name": net['name'],
                                "logo_path": net['logo_path'],
                                "origin_country": net['origin_country'],
                                # 标记来源剧集，方便用户确认
                                "source_show": tv.get('name') 
                            }
            
            # 格式化返回
            results = list(networks_found.values())
            return jsonify(results)
            
        else:
            # --- 模式 1: 原有的搜公司 ---
            results = search_companies_tmdb(api_key, query)
            return jsonify(results or [])
            
    except Exception as e:
        logger.error(f"搜索 TMDb {search_type} 时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 搜索 TMDb 人物 (演员/导演) ---
@custom_collections_bp.route('/config/tmdb_search_persons', methods=['GET'])
@any_login_required
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
    
# --- 提供电影类型映射的API ---
@custom_collections_bp.route('/config/movie_genres', methods=['GET'])
@admin_required
def api_get_movie_genres_config():
    """
    从媒体元数据缓存中动态获取所有唯一的电影类型。
    """
    try:
        genres = custom_collection_db.get_movie_genres()
        return jsonify(genres)
    except Exception as e:
        logger.error(f"动态获取电影类型时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 提供电视剧类型映射的API ---
@custom_collections_bp.route('/config/tv_genres', methods=['GET'])
@admin_required
def api_get_tv_genres_config():
    """
    从媒体元数据缓存中动态获取所有唯一的电视剧类型。
    """
    try:
        genres = custom_collection_db.get_tv_genres()
        return jsonify(genres)
    except Exception as e:
        logger.error(f"动态获取电影类型时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

 # --- 获取关键词映射表 ---   

# --- 获取关键词映射表 ---
@custom_collections_bp.route('/config/keyword_mapping', methods=['GET'])
def api_get_keyword_mapping():
    data = settings_db.get_setting('keyword_mapping')
    return jsonify(ensure_list_format(data, DEFAULT_KEYWORD_MAPPING))

# --- 保存关键词映射表 ---
@custom_collections_bp.route('/config/keyword_mapping', methods=['POST'])
@admin_required
def api_save_keyword_mapping():
    from database import settings_db
    data = request.json # 前端现在会发送 List
    settings_db.save_setting('keyword_mapping', data)
    return jsonify({"message": "保存成功"})

# --- 恢复默认关键词映射表 ---
@custom_collections_bp.route('/config/keyword_mapping/defaults', methods=['GET'])
@admin_required
def api_get_keyword_defaults():
    return jsonify(DEFAULT_KEYWORD_MAPPING)

# --- 筛选器用的关键词列表 ---
@custom_collections_bp.route('/config/keywords', methods=['GET'])
@any_login_required
def api_get_keywords_for_filter():
    try:
        data = settings_db.get_setting('keyword_mapping')
        
        # 使用辅助函数处理
        mapping_list = ensure_list_format(data, DEFAULT_KEYWORD_MAPPING)
            
        # 直接按列表顺序返回，不再强制 sort
        keyword_options = [
            {"label": item['label'], "value": item['label']}
            for item in mapping_list
        ]
        return jsonify(keyword_options)
    except Exception as e:
        logger.error(f"获取关键词列表失败: {e}")
        return jsonify([]), 500

# --- 获取工作室映射表 ---
@custom_collections_bp.route('/config/studio_mapping', methods=['GET'])
def api_get_studio_mapping():
    from database import settings_db
    data = settings_db.get_setting('studio_mapping')
    return jsonify(ensure_list_format(data, DEFAULT_STUDIO_MAPPING))

# --- 保存工作室映射表 ---
@custom_collections_bp.route('/config/studio_mapping', methods=['POST'])
@admin_required
def api_save_studio_mapping():
    from database import settings_db
    data = request.json
    settings_db.save_setting('studio_mapping', data)
    return jsonify({"message": "保存成功"})

# --- 恢复默认工作室映射表 ---
@custom_collections_bp.route('/config/studio_mapping/defaults', methods=['GET'])
@admin_required
def api_get_studio_defaults():
    return jsonify(DEFAULT_STUDIO_MAPPING)

# --- 筛选器用的工作室列表接口 ---
@custom_collections_bp.route('/config/studios', methods=['GET'])
@any_login_required
def api_get_studios_for_filter():
    """返回工作室映射的 Label 列表，供筛选下拉框使用"""
    try:
        from database import settings_db
        # 获取完整配置
        data = settings_db.get_setting('studio_mapping')
        # 使用之前的辅助函数确保是列表
        mapping_list = ensure_list_format(data, DEFAULT_STUDIO_MAPPING)
        
        # 转换为前端下拉框格式
        studio_options = [
            {"label": item['label'], "value": item['label']}
            for item in mapping_list
        ]
        return jsonify(studio_options)
    except Exception as e:
        logger.error(f"获取工作室列表失败: {e}")
        return jsonify([]), 500

# --- 获取国家映射表 ---
@custom_collections_bp.route('/config/country_mapping', methods=['GET'])
def api_get_country_mapping():
    data = settings_db.get_setting('country_mapping')
    # 如果数据库没数据，返回默认值
    return jsonify(ensure_list_format(data, DEFAULT_COUNTRY_MAPPING))

# --- 保存国家映射表 ---
@custom_collections_bp.route('/config/country_mapping', methods=['POST'])
@admin_required
def api_save_country_mapping():
    from database import settings_db
    data = request.json
    settings_db.save_setting('country_mapping', data)
    # 清除 utils 中的缓存 (如果有)
    # utils._country_map_cache = None 
    return jsonify({"message": "保存成功"})

# --- 恢复默认国家映射表 ---
@custom_collections_bp.route('/config/country_mapping/defaults', methods=['GET'])
@admin_required
def api_get_country_defaults():
    return jsonify(DEFAULT_COUNTRY_MAPPING)

# --- 筛选器用的国家列表 ---
@custom_collections_bp.route('/config/tmdb_countries', methods=['GET'])
@any_login_required
def api_get_tmdb_countries():
    """为 TMDb 探索助手提供国家/地区选项列表 (含ISO代码)。"""
    try:
        # 1. 优先从数据库读取
        data = settings_db.get_setting('country_mapping')
        # 2. 如果没有，使用默认预设
        config_list = ensure_list_format(data, DEFAULT_COUNTRY_MAPPING)
        
        # 3. 格式化为前端需要的 {label, value}
        options = []
        for item in config_list:
            if item.get('label') and item.get('value'):
                options.append({
                    "label": item['label'],
                    "value": item['value']
                })
        return jsonify(options)
    except Exception as e:
        logger.error(f"获取 TMDb 国家/地区选项时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# --- 获取语言映射表 ---
@custom_collections_bp.route('/config/language_mapping', methods=['GET'])
def api_get_language_mapping():
    from database import settings_db
    data = settings_db.get_setting('language_mapping')
    return jsonify(ensure_list_format(data, DEFAULT_LANGUAGE_MAPPING))

# --- 保存语言映射表 ---
@custom_collections_bp.route('/config/language_mapping', methods=['POST'])
@admin_required
def api_save_language_mapping():
    from database import settings_db
    data = request.json
    settings_db.save_setting('language_mapping', data)
    return jsonify({"message": "保存成功"})

# --- 恢复默认语言映射表 ---
@custom_collections_bp.route('/config/language_mapping/defaults', methods=['GET'])
@admin_required
def api_get_language_defaults():
    return jsonify(DEFAULT_LANGUAGE_MAPPING)

# --- 筛选器用的语言列表 ---
@custom_collections_bp.route('/config/languages', methods=['GET'])
@any_login_required
def api_get_languages_for_filter():
    """为筛选器提供语言选项列表。"""
    try:
        # 1. 优先从数据库读取
        data = settings_db.get_setting('language_mapping')
        # 2. 如果没有，使用默认预设
        config_list = ensure_list_format(data, DEFAULT_LANGUAGE_MAPPING)
        
        # 3. 格式化
        options = []
        for item in config_list:
            if item.get('label') and item.get('value'):
                options.append({
                    "label": item['label'],
                    "value": item['value']
                })
        return jsonify(options)
    except Exception as e:
        logger.error(f"获取语言列表时出错: {e}", exc_info=True)
        return jsonify([]), 500