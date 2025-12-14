# routes/custom_collections.py

from flask import Blueprint, request, jsonify
import logging
import json
import psycopg2
import pytz
from datetime import datetime
from database import user_db, collection_db, connection, media_db
import config_manager
import handler.emby as emby
from tasks.helpers import is_movie_subscribable
from extensions import admin_required, any_login_required
from handler.custom_collection import FilterEngine
from utils import get_country_translation_map, UNIFIED_RATING_CATEGORIES, get_tmdb_country_options, KEYWORD_TRANSLATION_MAP
from handler.tmdb import get_movie_genres_tmdb, get_tv_genres_tmdb, search_companies_tmdb, search_person_tmdb
# 1. 创建自定义合集蓝图
custom_collections_bp = Blueprint('custom_collections', __name__, url_prefix='/api/custom_collections')

logger = logging.getLogger(__name__)

GENRE_TRANSLATION_PATCH = {
    "Sci-Fi & Fantasy": "科幻&奇幻",
    "War & Politics": "战争&政治",
    # 以后如果发现其他未翻译的，也可以加在这里
}


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
    """获取所有自定义合集定义 (V5 - 精确类型匹配版)"""
    try:
        beijing_tz = pytz.timezone('Asia/Shanghai')
        collections_from_db = collection_db.get_all_active_custom_collections()
        processed_collections = []

        # ==========================================================
        # ★★★ 第一阶段：收集所有需要检查的 TMDB ID ★★★
        # ==========================================================
        all_tmdb_ids_to_check = []
        
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

            # 2. 解析 generated_media_info_json
            media_info = collection.get('generated_media_info_json')
            parsed_media_info = []
            if isinstance(media_info, str):
                try:
                    parsed_media_info = json.loads(media_info)
                except: pass
            elif isinstance(media_info, list):
                parsed_media_info = media_info
            
            collection['_parsed_media_info'] = parsed_media_info
            
            # 收集 ID (这一步只收集 ID 给 SQL 用，不需要类型)
            if parsed_media_info:
                for item in parsed_media_info:
                    tid = None
                    if isinstance(item, dict):
                        tid = item.get('tmdb_id')
                    elif isinstance(item, str):
                        tid = item
                    
                    if tid:
                        all_tmdb_ids_to_check.append(str(tid))

        # ==========================================================
        # ★★★ 第二阶段：批量查询数据库状态 (使用新函数) ★★★
        # ==========================================================
        # 返回格式: {'12345_Movie': True, '12345_Series': False}
        in_library_status_map = {}
        if all_tmdb_ids_to_check:
            # 调用我们刚写的新函数
            in_library_status_map = media_db.get_in_library_status_with_type_bulk(all_tmdb_ids_to_check)

        # ==========================================================
        # ★★★ 第三阶段：计算每个合集的缺失数 (精确匹配) ★★★
        # ==========================================================
        for collection in collections_from_db:
            c_type = collection.get('type')
            media_items = collection.get('_parsed_media_info') or []
            
            missing_count = 0
            
            if c_type in ['list', 'ai_recommendation', 'ai_recommendation_global'] and media_items:
                for item in media_items:
                    tmdb_id = None
                    media_type = 'Movie' # 默认为 Movie

                    if isinstance(item, dict):
                        tmdb_id = str(item.get('tmdb_id')) if item.get('tmdb_id') else None
                        media_type = item.get('media_type') or 'Movie'
                    elif isinstance(item, str):
                        tmdb_id = item
                        # 如果是旧数据只有字符串ID，默认当 Movie 处理，或者无法精确匹配
                    
                    # 1. 如果没有 ID，算缺失
                    if not tmdb_id or tmdb_id.lower() == 'none': 
                        missing_count += 1
                        continue

                    # 2. ★★★ 核心修复：使用组合键查询 ★★★
                    target_key = f"{tmdb_id}_{media_type}"
                    
                    # 查字典：必须 ID 和 类型 都匹配，且 in_library 为 True 才算在库
                    is_in_library = in_library_status_map.get(target_key, False)
                    
                    if not is_in_library:
                        missing_count += 1
            
            collection['missing_count'] = missing_count
            collection['health_status'] = 'has_missing' if missing_count > 0 else 'ok'
            
            # 清理临时字段
            if '_parsed_media_info' in collection:
                del collection['_parsed_media_info']
            if 'generated_media_info_json' in collection:
                del collection['generated_media_info_json']

            # --- 时区转换逻辑 (保持不变) ---
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
        
        success = collection_db.update_custom_collection(
            collection_id, name, type, definition_json, status, allowed_user_ids_json
        )
        
        if success:
            updated_collection = collection_db.get_custom_collection_by_id(collection_id)
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
@admin_required
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
            logger.info(f"  ➜ 正在删除合集 '{collection_name}' (Emby ID: {emby_id_to_empty})...")
            
            # ★★★ 调用我们全新的、真正有效的清空函数 ★★★
            emby.empty_collection_in_emby(
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
@admin_required
def api_get_custom_collection_status(collection_id):
    """
    获取合集详情 (V4 - 终极修复版)
    1. 修复了 ID 撞车问题 (使用组合键)。
    2. 修复了探索助手榜单无标题导致的“未知媒体”问题 (实时补全)。
    3. 修复了混合榜单类型判断错误的问题 (严格遵循 item_def)。
    """
    try:
        collection = collection_db.get_custom_collection_by_id(collection_id)
        if not collection:
            return jsonify({"error": "未找到合集"}), 404
        
        definition_list = collection.get('generated_media_info_json') or []
        if not definition_list:
            collection['media_items'] = []
            return jsonify(collection)

        # 1. 提取所有有效的 TMDB ID
        tmdb_ids = [str(item['tmdb_id']) for item in definition_list if item.get('tmdb_id') and str(item.get('tmdb_id')).lower() != 'none']
        
        # 2. 批量查询本地数据库，构建组合键映射
        media_in_db_map = {}
        if tmdb_ids:
            try:
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = ANY(%s)", (tmdb_ids,))
                        rows = cursor.fetchall()
                        for row in rows:
                            unique_key = f"{row['tmdb_id']}_{row['item_type']}"
                            media_in_db_map[unique_key] = dict(row)
            except Exception as e_db:
                logger.error(f"查询媒体元数据失败: {e_db}")

        # ==============================================================================
        # ★★★ 核心修复：实时补全缺失的元数据 ★★★
        # ==============================================================================
        # 找出那些在 definition_list 里有 ID，但在 media_in_db_map 里找不到对应类型记录的项目
        items_needing_fetch = []
        for item in definition_list:
            tmdb_id = str(item.get('tmdb_id'))
            if not tmdb_id or tmdb_id.lower() == 'none': continue
            
            media_type = item.get('media_type') or 'Movie'
            target_key = f"{tmdb_id}_{media_type}"
            
            if target_key not in media_in_db_map:
                items_needing_fetch.append(item)

        if items_needing_fetch:
            logger.info(f"  ➜ 合集详情：发现 {len(items_needing_fetch)} 个项目本地无缓存，正在实时获取元数据...")
            api_key = config_manager.APP_CONFIG.get('tmdb_api_key')
            
            # 限制实时获取数量，防止超时
            items_to_process = items_needing_fetch[:15]
            
            from handler.tmdb import get_movie_details, get_tv_details
            
            for item in items_to_process:
                tmdb_id = str(item.get('tmdb_id'))
                media_type = item.get('media_type') or 'Movie'
                
                try:
                    details = None
                    if media_type == 'Series':
                        details = get_tv_details(tmdb_id, api_key)
                    else:
                        details = get_movie_details(tmdb_id, api_key)
                    
                    if details:
                        temp_record = {
                            'tmdb_id': str(details.get('id')),
                            'item_type': media_type,
                            'title': details.get('title') or details.get('name'),
                            'original_title': details.get('original_title') or details.get('original_name'),
                            'release_date': datetime.strptime(details.get('release_date') or details.get('first_air_date') or '1900-01-01', '%Y-%m-%d') if (details.get('release_date') or details.get('first_air_date')) else None,
                            'poster_path': details.get('poster_path'),
                            'in_library': False,
                            'subscription_status': 'NONE',
                            'emby_item_id': None
                        }
                        # 补入 map
                        unique_key = f"{tmdb_id}_{media_type}"
                        media_in_db_map[unique_key] = temp_record
                        
                except Exception as e_fetch:
                    logger.warning(f"  ➜ 实时获取 TMDb ID {tmdb_id} ({media_type}) 详情失败: {e_fetch}")

        # ==============================================================================

        final_media_list = []
        dynamic_missing_count = 0

        for item_def in definition_list:
            # ... (获取 tmdb_id, media_type, season_number, source_title 的逻辑保持不变) ...
            tmdb_id = item_def.get('tmdb_id')
            if tmdb_id and str(tmdb_id).lower() != 'none':
                tmdb_id_str = str(tmdb_id)
            else:
                tmdb_id_str = None
            
            media_type = item_def.get('media_type') or 'Movie'
            season_number = item_def.get('season')
            source_title = item_def.get('title') 

            # 情况 1: 完全未识别
            if not tmdb_id_str:
                final_media_list.append({
                    "tmdb_id": None,
                    "emby_id": None,
                    "title": source_title or "未知标题",
                    "original_title": source_title,
                    "release_date": "",
                    "poster_path": None,
                    "status": "unidentified",
                    "media_type": media_type,
                    "season": season_number
                })
                dynamic_missing_count += 1 # 未识别也算缺失
                continue

            # ★★★ 使用组合键查找 ★★★
            target_key = f"{tmdb_id_str}_{media_type}"
            
            if target_key in media_in_db_map:
                db_record = media_in_db_map[target_key]
                
                status = "missing"
                if db_record.get('in_library'):
                    status = "in_library"
                elif db_record.get('subscription_status') == 'SUBSCRIBED':
                    status = "subscribed"
                elif db_record.get('subscription_status') == 'PAUSED':
                    status = "paused"
                elif db_record.get('subscription_status') == 'IGNORED':
                    status = "ignored"
                elif db_record.get('subscription_status') == 'PENDING_RELEASE':
                    status = "unreleased"
                
                # ★★★ 统计缺失项 (missing 或 unreleased 通常都算在健康检查关注范围内，视需求而定) ★★★
                # 这里我们定义：只要不是 in_library 或 ignored，都算作广义的“缺失/待处理”
                if status not in ('in_library', 'ignored'):
                    dynamic_missing_count += 1

                r_date = db_record.get('release_date')
                if isinstance(r_date, datetime):
                    r_date_str = r_date.strftime('%Y-%m-%d')
                else:
                    r_date_str = str(r_date) if r_date else ''

                final_media_list.append({
                    "tmdb_id": tmdb_id_str,
                    "emby_id": db_record.get('emby_item_id'),
                    "title": db_record.get('title') or db_record.get('original_title'),
                    "original_title": source_title, 
                    "release_date": r_date_str,
                    "poster_path": db_record.get('poster_path'),
                    "status": status,
                    "media_type": media_type,
                    "season": season_number
                })
            else:
                # 情况 3: 确实没查到
                final_media_list.append({
                    "tmdb_id": tmdb_id_str,
                    "emby_id": None,
                    "title": source_title or f"未知媒体 (ID: {tmdb_id_str})",
                    "original_title": source_title,
                    "release_date": item_def.get('release_date', ''),
                    "poster_path": item_def.get('poster_path'),
                    "status": "missing",
                    "media_type": media_type,
                    "season": season_number
                })
                dynamic_missing_count += 1
        
        collection['media_items'] = final_media_list
        collection.pop('generated_media_info_json', None)
        
        # ★★★ 核心修改：覆盖数据库中的静态值，返回动态计算结果 ★★★
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
        corrected_item = collection_db.apply_and_persist_media_correction(
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

# --- 提取国家列表 ---
@custom_collections_bp.route('/config/countries', methods=['GET'])
@any_login_required
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
@admin_required
def api_get_tags_for_filter():
    """为筛选器提供一个标签列表。"""
    try:
        tags = collection_db.get_unique_tags()
        return jsonify(tags)
    except Exception as e:
        logger.error(f"获取标签列表时出错: {e}", exc_info=True)
        return jsonify([]), 500

@custom_collections_bp.route('/config/unified_ratings', methods=['GET'])
@admin_required
def api_get_unified_ratings_for_filter():
    """为筛选器提供一个固定的、统一的分级列表。"""
    # 直接返回我们预定义好的分类列表
    return jsonify(UNIFIED_RATING_CATEGORIES)

# --- 获取 Emby 媒体库列表 ---
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
    
# --- 获取 TMDb 国家/地区选项列表 ---
@custom_collections_bp.route('/config/tmdb_countries', methods=['GET'])
@any_login_required
def api_get_tmdb_countries():
    """为 TMDb 探索助手提供国家/地区选项列表 (含ISO代码)。"""
    try:
        # 调用我们刚刚在 utils.py 中创建的新函数
        country_options = get_tmdb_country_options()
        return jsonify(country_options)
    except Exception as e:
        logger.error(f"获取 TMDb 国家/地区选项时出错: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# --- 提取关键词列表 ---
@custom_collections_bp.route('/config/keywords', methods=['GET'])
@any_login_required
def api_get_keywords_for_filter():
    """为筛选器提供一个中英对照的关键词列表。"""
    try:
        # KEYWORD_TRANSLATION_MAP 的格式是 {"中文": "english"}
        # 我们需要转换成 [{label: "中文", value: "english"}, ...]
        keyword_options = [
            {"label": chinese, "value": english}
            for chinese, english in KEYWORD_TRANSLATION_MAP.items()
        ]
        # 按中文标签的拼音排序，方便前端查找
        sorted_options = sorted(keyword_options, key=lambda x: x['label'])
        return jsonify(sorted_options)
    except Exception as e:
        logger.error(f"获取关键词列表时出错: {e}", exc_info=True)
        return jsonify([]), 500
    
# --- 提供电影类型映射的API ---
@custom_collections_bp.route('/config/movie_genres', methods=['GET'])
@admin_required
def api_get_movie_genres_config():
    """
    从媒体元数据缓存中动态获取所有唯一的电影类型。
    """
    try:
        genres = collection_db.get_movie_genres()
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
        genres = collection_db.get_tv_genres()
        return jsonify(genres)
    except Exception as e:
        logger.error(f"动态获取电影类型时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500