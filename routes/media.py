# routes/media.py

from flask import Blueprint, request, jsonify, Response, stream_with_context
import logging

import requests

import handler.emby as emby
import config_manager
import constants
import task_manager
import extensions
from database import custom_collection_db, media_db, user_db, request_db, settings_db
import handler.moviepilot as moviepilot
from extensions import admin_required, processor_ready_required
from urllib.parse import urlparse

# --- 蓝图 1：用于所有 /api/... 的路由 ---
media_api_bp = Blueprint('media_api', __name__, url_prefix='/api')

# --- 蓝图 2：用于不需要 /api 前缀的路由 ---
media_proxy_bp = Blueprint('media_proxy', __name__)

logger = logging.getLogger(__name__)

@media_api_bp.route('/search_emby_library', methods=['GET'])
@processor_ready_required
def api_search_emby_library():
    query = request.args.get('query', '')
    if not query.strip():
        return jsonify({"error": "搜索词不能为空"}), 400

    try:
        # ✨✨✨ 调用改造后的函数，并传入 search_term ✨✨✨
        search_results = emby.get_emby_library_items(
            base_url=extensions.media_processor_instance.emby_url,
            api_key=extensions.media_processor_instance.emby_api_key,
            user_id=extensions.media_processor_instance.emby_user_id,
            media_type_filter="Movie,Series",
            search_term=query
        )
        
        if search_results is None:
            return jsonify({"error": "搜索时发生服务器错误"}), 500

        # 将搜索结果转换为前端表格期望的格式 (这部分逻辑不变)
        formatted_results = []
        for item in search_results:
            formatted_results.append({
                "item_id": item.get("Id"),
                "item_name": item.get("Name"),
                "item_type": item.get("Type"),
                "failed_at": None,
                "error_message": f"来自 Emby 库的搜索结果 (年份: {item.get('ProductionYear', 'N/A')})",
                "score": None,
                # ★★★ 核心修复：把 ProviderIds 也传递给前端 ★★★
                "provider_ids": item.get("ProviderIds") 
            })
        
        return jsonify({
            "items": formatted_results,
            "total_items": len(formatted_results)
        })

    except Exception as e:
        logger.error(f"API /api/search_emby_library Error: {e}", exc_info=True)
        return jsonify({"error": "搜索时发生未知服务器错误"}), 500

@media_api_bp.route('/media_for_editing/<item_id>', methods=['GET'])
@admin_required
@processor_ready_required
def api_get_media_for_editing(item_id):
    # 直接调用 core_processor 的新方法
    data_for_editing = extensions.media_processor_instance.get_cast_for_editing(item_id)
    
    if data_for_editing:
        return jsonify(data_for_editing)
    else:
        return jsonify({"error": f"无法获取项目 {item_id} 的编辑数据，请检查日志。"}), 404

@media_api_bp.route('/update_media_cast_sa/<item_id>', methods=['POST'])
@admin_required
@processor_ready_required
def api_update_edited_cast_sa(item_id):
    from tasks import task_manual_update
    data = request.json
    if not data or "cast" not in data or not isinstance(data["cast"], list):
        return jsonify({"error": "请求体中缺少有效的 'cast' 列表"}), 400
    
    edited_cast = data["cast"]
    item_name = data.get("item_name", f"未知项目(ID:{item_id})")

    task_manager.submit_task(
        task_manual_update, # 传递包装函数
        f"手动更新: {item_name}",
        processor_type='media',
        item_id=item_id,
        manual_cast_list=edited_cast,
        item_name=item_name
        
    )
    
    return jsonify({"message": "手动更新任务已在后台启动。"}), 202

# ▼▼▼ 通用外部图片代理接口 ▼▼▼
@media_api_bp.route('/image_proxy', methods=['GET'])
def proxy_external_image():
    """
    一个安全的通用外部图片代理。
    【V2 - 修复版】增加了 User-Agent 和 Referer 头，以模拟真实浏览器请求，绕过反爬虫机制。
    """
    external_url = request.args.get('url')
    if not external_url:
        return jsonify({"error": "缺少 'url' 参数"}), 400

    try:
        # 1. 获取程序配置，以便使用统一的 User-Agent
        current_config = config_manager.APP_CONFIG
        user_agent = current_config.get('user_agent', 'Mozilla/5.0')

        # 2. 构造一个看起来更真实的请求头
        parsed_url = urlparse(external_url)
        headers = {
            'User-Agent': user_agent,
            'Referer': f"{parsed_url.scheme}://{parsed_url.netloc}/"
        }
        
        logger.debug(f"代理请求外部图片: URL='{external_url}', Headers={headers}")

        # 3. 带着伪装的请求头去获取图片
        response = requests.get(external_url, stream=True, timeout=10, headers=headers)

        response.raise_for_status()

        return Response(
            stream_with_context(response.iter_content(chunk_size=8192)),
            content_type=response.headers.get('Content-Type'),
            status=response.status_code
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"通用图片代理错误: 无法获取 URL '{external_url}'. 错误: {e}")
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png',
            status=404
        )
    except Exception as e:
        logger.error(f"通用图片代理发生未知错误 for URL '{external_url}'. 错误: {e}", exc_info=True)
        return jsonify({"error": "代理图片时发生内部错误"}), 500

# 图片代理路由
@media_proxy_bp.route('/image_proxy/<path:image_path>')
@processor_ready_required
def proxy_emby_image(image_path):
    """
    一个安全的、动态的 Emby 图片代理。
    【V2 - 完整修复版】确保 api_key 作为 URL 参数传递，适用于所有图片类型。
    """
    try:
        emby_url = extensions.media_processor_instance.emby_url.rstrip('/')
        emby_api_key = extensions.media_processor_instance.emby_api_key

        # 1. 构造基础 URL，包含路径和原始查询参数
        query_string = request.query_string.decode('utf-8')
        target_url = f"{emby_url}/{image_path}"
        if query_string:
            target_url += f"?{query_string}"
        
        # 2. ★★★ 核心修复：将 api_key 作为 URL 参数追加 ★★★
        # 判断是使用 '?' 还是 '&' 来追加 api_key
        separator = '&' if '?' in target_url else '?'
        target_url_with_key = f"{target_url}{separator}api_key={emby_api_key}"
        
        logger.trace(f"代理图片请求 (最终URL): {target_url_with_key}")

        # 3. 发送请求
        emby_response = requests.get(target_url_with_key, stream=True, timeout=20)
        emby_response.raise_for_status()

        # 4. 将 Emby 的响应流式传输回浏览器
        return Response(
            stream_with_context(emby_response.iter_content(chunk_size=8192)),
            content_type=emby_response.headers.get('Content-Type'),
            status=emby_response.status_code
        )
    except Exception as e:
        logger.error(f"代理 Emby 图片时发生严重错误: {e}", exc_info=True)
        # 返回一个1x1的透明像素点作为占位符，避免显示大的裂图图标
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png'
        )
    
# ✨✨✨ 一键翻译 ✨✨✨
@media_api_bp.route('/actions/translate_cast_sa', methods=['POST']) # 注意路径不同
@admin_required
@processor_ready_required
def api_translate_cast_sa():
    data = request.json
    current_cast = data.get('cast')
    if not isinstance(current_cast, list):
        return jsonify({"error": "请求体必须包含 'cast' 列表。"}), 400

    # 【★★★ 从请求中获取所有需要的上下文信息 ★★★】
    title = data.get('title')
    year = data.get('year')

    try:
        # 【★★★ 调用新的、需要完整上下文的函数 ★★★】
        translated_list = extensions.media_processor_instance.translate_cast_list_for_editing(
            cast_list=current_cast,
            title=title,
            year=year,
        )
        return jsonify(translated_list)
    except Exception as e:
        logger.error(f"一键翻译演员列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在翻译时发生内部错误。"}), 500
    
# ✨✨✨ 预览处理后的演员表 ✨✨✨
@media_api_bp.route('/preview_processed_cast/<item_id>', methods=['POST'])
@processor_ready_required
def api_preview_processed_cast(item_id):
    """
    一个轻量级的API，用于预览单个媒体项经过核心处理器处理后的演员列表。
    它只返回处理结果，不执行任何数据库更新或Emby更新。
    """
    logger.info(f"API: 收到为 ItemID {item_id} 预览处理后演员的请求。")

    # 步骤 1: 获取当前媒体的 Emby 详情
    try:
        item_details = emby.get_emby_item_details(
            item_id,
            extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key,
            extensions.media_processor_instance.emby_user_id
        )
        if not item_details:
            return jsonify({"error": "无法获取当前媒体的Emby详情"}), 404
    except Exception as e:
        logger.error(f"API /preview_processed_cast: 获取Emby详情失败 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": f"获取Emby详情时发生错误: {e}"}), 500

    # 步骤 2: 调用核心处理方法
    try:
        current_emby_cast_raw = item_details.get("People", [])
        
        # 直接调用 MediaProcessor 的核心方法
        processed_cast_result = extensions.media_processor_instance._process_cast_list(
            current_emby_cast_people=current_emby_cast_raw,
            media_info=item_details
        )
        
        # 步骤 3: 将处理结果转换为前端友好的格式
        # processed_cast_result 的格式是内部格式，我们需要转换为前端期望的格式
        # (embyPersonId, name, role, imdbId, doubanId, tmdbId)
        
        cast_for_frontend = []
        for actor_data in processed_cast_result:
            cast_for_frontend.append({
                "embyPersonId": actor_data.get("EmbyPersonId"),
                "name": actor_data.get("Name"),
                "role": actor_data.get("Role"),
                "imdbId": actor_data.get("ImdbId"),
                "doubanId": actor_data.get("DoubanCelebrityId"),
                "tmdbId": actor_data.get("TmdbPersonId"),
                "matchStatus": "已刷新" # 可以根据 actor_data['_source_comment'] 提供更详细的状态
            })

        logger.info(f"API: 成功为 ItemID {item_id} 预览了处理后的演员列表，返回 {len(cast_for_frontend)} 位演员。")
        return jsonify(cast_for_frontend)

    except Exception as e:
        logger.error(f"API /preview_processed_cast: 调用 _process_cast_list 时发生错误 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": "在服务器端处理演员列表时发生内部错误"}), 500   
    
# --- 获取emby媒体库 ---
@media_api_bp.route('/emby_libraries')
def api_get_emby_libraries():
    if not extensions.media_processor_instance or \
       not extensions.media_processor_instance.emby_url or \
       not extensions.media_processor_instance.emby_api_key:
        return jsonify({"error": "Emby配置不完整或服务未就绪"}), 500

    # 调用通用的函数，它会返回完整的列表
    full_libraries_list = emby.get_emby_libraries(
        extensions.media_processor_instance.emby_url,
        extensions.media_processor_instance.emby_api_key,
        extensions.media_processor_instance.emby_user_id
    )

    if full_libraries_list is not None:
        # ★★★ 核心修改：在这里进行数据精简，以满足前端UI的需求 ★★★
        simplified_libraries = [
            {'Name': item.get('Name'), 'Id': item.get('Id')}
            for item in full_libraries_list
            if item.get('Name') and item.get('Id')
        ]
        return jsonify(simplified_libraries)
    else:
        return jsonify({"error": "无法获取Emby媒体库列表，请检查连接和日志"}), 500
    
# --- 获取emby媒体库（反代用） ---
@media_api_bp.route('/emby/user/<user_id>/views', methods=['GET'])
def api_get_emby_user_views(user_id):
    """
    从真实Emby服务器获取指定用户的所有原生媒体库（Views）。
    需要在请求头或查询参数中携带 API Key。
    """
    if not extensions.media_processor_instance or \
       not extensions.media_processor_instance.emby_url:
        logger.warning("/api/emby/user/<user_id>/views: Emby配置不完整或服务未就绪。")
        return jsonify({"error": "Emby配置不完整或服务未就绪"}), 500
    
    # 尝试从请求头和查询参数获取用户令牌
    user_token = request.headers.get('X-Emby-Token') or request.args.get('api_key')
    
    if not user_token:
        return jsonify({"error": "缺少用户访问令牌(api_key或X-Emby-Token)"}), 400
    
    base_url = extensions.media_processor_instance.emby_url.rstrip('/')
    real_views_url = f"{base_url}/emby/Users/{user_id}/Views"
    
    try:
        # 复制请求头，剔除不必要的
        headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        headers['Host'] = urlparse(base_url).netloc
        headers['Accept-Encoding'] = 'identity'
        headers['X-Emby-Token'] = user_token  # 确保Token传递
        
        params = request.args.to_dict()
        params['api_key'] = user_token  # 兼容api_key参数
        
        resp = requests.get(real_views_url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        
        views_data = resp.json()
        return jsonify(views_data)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"/api/emby/user/{user_id}/views 调用真实Emby失败: {e}")
        return jsonify({"error": "无法从真实Emby服务器获取数据"}), 502
    except Exception as e:
        logger.error(f"/api/emby/user/{user_id}/views 发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500 

# ★★★ 提供工作室远程搜索的API ★★★
@media_api_bp.route('/search_studios', methods=['GET'])
@admin_required
def api_search_studios():
    """
    根据查询参数 'q' 动态搜索工作室列表。
    """
    search_term = request.args.get('q', '').strip()
    
    if not search_term:
        return jsonify([])
        
    try:
        studios = custom_collection_db.search_unique_studios(search_term)
        return jsonify(studios)
    except Exception as e:
        logger.error(f"搜索工作室时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# ======================================================================
# ★★★ 通用状态操作 API ★★★
# ======================================================================

@media_api_bp.route('/subscription/status', methods=['POST'])
@admin_required
def api_unified_subscription_status():
    """
    统一处理所有媒体项状态变更的唯一入口。
    """
    data = request.json
    requests_list = data.get('requests')

    # 参数校验
    if not isinstance(requests_list, list) or not requests_list:
        return jsonify({"error": "'requests' 必须是一个非空列表"}), 400

    processed_count = 0
    errors = []
    
    # 定义允许的状态
    ALLOWED_STATUSES = ['WANTED', 'SUBSCRIBED', 'IGNORED', 'NONE', 'PENDING_RELEASE']

    for req in requests_list:
        tmdb_id = req.get('tmdb_id')
        item_type = req.get('item_type')
        new_status = req.get('new_status')
        
        if not all([tmdb_id, item_type, new_status]):
            errors.append(f"无效请求项，缺少 tmdb_id, item_type 或 new_status: {req}")
            continue
            
        if new_status.upper() not in ALLOWED_STATUSES:
            errors.append(f"无效的状态 '{new_status}' for TMDb ID {tmdb_id}")
            continue

        try:
            # ==================================================================
            # ★★★ 核心修复：统一处理 MoviePilot 的取消订阅逻辑 ★★★
            # 无论是转为 NONE 还是 IGNORED，只要之前是 SUBSCRIBED，都要取消
            # ==================================================================
            if new_status.upper() in ['NONE', 'IGNORED']:
                # 1. 先查当前状态
                media_details_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
                current_details = media_details_map.get(tmdb_id, {})
                current_status = current_details.get('subscription_status')
                
                # 2. 如果当前是已订阅，则执行取消操作
                if current_status == 'SUBSCRIBED':
                    logger.info(f"  ➜ 检测到已订阅项 (TMDb ID: {tmdb_id}) 转为 {new_status}，正在取消 MoviePilot 订阅...")
                    
                    # 智能判断要发给 MoviePilot 的真实 ID
                    id_for_mp = tmdb_id 
                    season_for_mp = None 

                    if item_type == 'Season':
                        parent_id = current_details.get('parent_series_tmdb_id')
                        season_num = current_details.get('season_number')
                        
                        if parent_id and season_num is not None:
                            id_for_mp = parent_id
                            season_for_mp = season_num
                        else:
                            error_msg = f"处理季 (TMDb ID: {tmdb_id}) 失败：无法找到父剧集ID或季号。"
                            errors.append(error_msg)
                            logger.error(f"API /subscription/status: {error_msg}")
                            continue 
                    
                    config = config_manager.APP_CONFIG
                    if not moviepilot.cancel_subscription(id_for_mp, item_type, config, season_for_mp):
                        error_msg = f"处理 TMDb ID {tmdb_id} 失败：MoviePilot 取消订阅失败。"
                        errors.append(error_msg)
                        logger.error(f"API /subscription/status: {error_msg}")
                        continue
                    else:
                        logger.info(f"  ➜ MoviePilot 订阅已取消 (ID: {id_for_mp})")

            # ==================================================================
            # 本地数据库状态更新
            # ==================================================================
            if new_status.upper() == 'NONE':
                request_db.set_media_status_none(
                    tmdb_ids=[tmdb_id], item_type=item_type
                )
                processed_count += 1

            elif new_status.upper() == 'IGNORED':
                source = req.get('source', {"type": "manual_ignore"})
                ignore_reason = req.get('ignore_reason')
                if not ignore_reason:
                    ignore_reason = '手动忽略'

                request_db.set_media_status_ignored(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req],
                    ignore_reason=ignore_reason
                )
                processed_count += 1

            elif new_status.upper() == 'WANTED':
                source = req.get('source', {"type": "manual_add"})
                force_unignore = req.get('force_unignore', False)
                request_db.set_media_status_wanted(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req],
                    force_unignore=force_unignore
                )
                processed_count += 1

            elif new_status.upper() == 'SUBSCRIBED':
                source = req.get('source', {"type": "manual_subscribe"})
                request_db.set_media_status_subscribed(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req]
                )
                # 尝试恢复 MP 订阅状态 (S -> R)
                try:
                    config = config_manager.APP_CONFIG
                    mp_tmdb_id = tmdb_id
                    mp_season = None
                    
                    if item_type == 'Season':
                        # 这里可能需要重新查一次详情，或者复用上面的 current_details 如果存在
                        # 为安全起见，重新查一次或优化逻辑。这里简单处理：
                        media_details_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
                        details = media_details_map.get(tmdb_id, {})
                        if details.get('parent_series_tmdb_id'):
                            mp_tmdb_id = details['parent_series_tmdb_id']
                            mp_season = details.get('season_number')

                    if not moviepilot.update_subscription_status(int(mp_tmdb_id), mp_season, 'R', config):
                        logger.warning(f"  ➜ [状态同步] 切换 MP 状态失败，尝试重新提交订阅...")
                        payload = {
                            "tmdbid": int(mp_tmdb_id),
                            "type": "电影" if item_type == 'Movie' else "电视剧"
                        }
                        if mp_season is not None:
                            payload['season'] = mp_season
                        moviepilot.subscribe_with_custom_payload(payload, config)
                    else:
                        logger.info(f"  ➜ [状态同步] 已通知 MP 恢复搜索: {mp_tmdb_id}")

                except Exception as e_sync:
                    logger.error(f"  ➜ [状态同步] 恢复 MoviePilot 订阅状态时出错: {e_sync}")
                
                processed_count += 1

        except Exception as e:
            error_msg = f"处理 TMDb ID {tmdb_id} 状态变更时发生错误: {e}"
            errors.append(error_msg)
            logger.error(f"API /subscription/status 发生错误: {error_msg}", exc_info=True)

    if processed_count > 0:
        message = f"已成功提交 {processed_count} 个媒体项的状态变更请求。"
        if errors:
            message += f" 但有 {len(errors)} 个请求处理失败。"
        return jsonify({"message": message, "errors": errors}), 200
    else:
        return jsonify({"error": "没有有效的媒体项被成功处理。", "errors": errors}), 400

@media_api_bp.route('/subscriptions/all', methods=['GET'])
@admin_required
def api_get_all_subscriptions_for_management():
    """
    为前端“统一订阅”页面提供所有有订阅状态媒体项的数据。
    """
    try:
        # 1. 从数据库获取原始数据
        items = media_db.get_all_subscriptions()

        # 遍历每个媒体项，处理其来源信息
        for item in items:
            sources = item.get('subscription_sources_json')
            if isinstance(sources, list):
                for source in sources:
                    # 如果来源是用户请求，并且有 user_id
                    if source.get('type') == 'user_request' and (user_id := source.get('user_id')):
                        # 根据 user_id 查询用户名，并将其添加到 source 字典中
                        # 使用 'user' 作为键名，以匹配前端已有的逻辑
                        source['user'] = user_db.get_username_by_id(user_id) or '未知用户'

        # 3. 返回增强后的数据
        return jsonify(items)
    except Exception as e:
        logger.error(f"API /subscriptions/all 获取数据失败: {e}", exc_info=True)
        return jsonify({"error": "获取订阅列表时发生服务器内部错误"}), 500

@media_api_bp.route('/media/batch_delete', methods=['POST'])
@admin_required
def api_batch_delete_media():
    """
    接收包含 {tmdb_id, item_type} 的列表，从数据库物理删除这些记录。
    """
    data = request.json
    items_to_delete = data.get('items')

    if not isinstance(items_to_delete, list) or not items_to_delete:
        return jsonify({"error": "请求体必须包含非空的 'items' 列表"}), 400

    try:
        deleted_count = media_db.delete_media_metadata_batch(items_to_delete)
        logger.info(f"API: 已物理删除 {deleted_count} 条媒体元数据记录。")
        return jsonify({
            "message": f"成功删除了 {deleted_count} 条记录。",
            "deleted_count": deleted_count
        })
    except Exception as e:
        logger.error(f"API /media/batch_delete 发生错误: {e}", exc_info=True)
        return jsonify({"error": "删除操作发生内部错误"}), 500
    
@media_api_bp.route('/subscription/strategy', methods=['GET'])
@admin_required
def api_get_subscription_strategy():
    """获取订阅策略配置"""
    try:
        from database import settings_db
        config = settings_db.get_setting('subscription_strategy_config')
        
        # 如果数据库为空，返回默认值
        if not config:
            config = {
                'movie_protection_days': 180,
                'movie_search_window_days': 1,
                'movie_pause_days': 7,
                'delay_subscription_days': 30
            }
        return jsonify(config)
    except Exception as e:
        logger.error(f"获取订阅策略失败: {e}")
        return jsonify({"error": "获取配置失败"}), 500

@media_api_bp.route('/subscription/strategy', methods=['POST'])
@admin_required
def api_save_subscription_strategy():
    """保存订阅策略配置"""
    try:
        from database import settings_db
        data = request.json
        # 简单的校验
        if not isinstance(data, dict):
            return jsonify({"error": "无效的配置格式"}), 400
            
        settings_db.save_setting('subscription_strategy_config', data)
        return jsonify({"message": "策略配置已保存"})
    except Exception as e:
        logger.error(f"保存订阅策略失败: {e}")
        return jsonify({"error": "保存配置失败"}), 500
    
@media_api_bp.route('/auto_tagging/rules', methods=['GET'])
@admin_required
def get_tagging_rules():
    rules = settings_db.get_setting('auto_tagging_rules') or []
    return jsonify(rules)

@media_api_bp.route('/auto_tagging/rules', methods=['POST'])
@admin_required
def save_tagging_rules():
    rules = request.json
    settings_db.save_setting('auto_tagging_rules', rules)
    return jsonify({"message": "配置已保存"})

@media_api_bp.route('/auto_tagging/run_now', methods=['POST'])
@admin_required
@processor_ready_required
def run_tagging_now():
    from tasks import task_bulk_auto_tag
    data = request.json
    lib_ids = data.get('library_ids') 
    tags = data.get('tags')
    lib_name_display = data.get('library_name', "多个库")

    if not lib_ids or not tags:
        return jsonify({"error": "参数不完整"}), 400

    task_manager.submit_task(
        task_bulk_auto_tag,
        task_name=f"手动补打标签: {lib_name_display}",
        processor_type='media',
        library_ids=lib_ids, # 传列表
        tags=tags
    )
    return jsonify({"message": "批量打标任务已启动"})

@media_api_bp.route('/auto_tagging/clear_now', methods=['POST'])
@admin_required
@processor_ready_required
def clear_tagging_now():
    from tasks import task_bulk_remove_tags
    data = request.json
    lib_ids = data.get('library_ids')
    tags = data.get('tags')
    lib_name_display = data.get('library_name', "多个库")

    task_manager.submit_task(
        task_bulk_remove_tags,
        task_name=f"手动移除标签: {lib_name_display}",
        processor_type='media',
        library_ids=lib_ids,
        tags=tags
    )
    return jsonify({"message": "批量移除任务已启动"})