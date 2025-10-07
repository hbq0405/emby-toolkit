# routes/media.py

from flask import Blueprint, request, jsonify, Response, stream_with_context
import logging

import requests

import emby_handler
import config_manager
import task_manager
import extensions
from database import collection_db
from extensions import login_required, processor_ready_required
from urllib.parse import urlparse

# --- 蓝图 1：用于所有 /api/... 的路由 ---
media_api_bp = Blueprint('media_api', __name__, url_prefix='/api')

# --- 蓝图 2：用于不需要 /api 前缀的路由 ---
media_proxy_bp = Blueprint('media_proxy', __name__)

logger = logging.getLogger(__name__)

# ✨✨✨ 导入网页解析器 ✨✨✨
try:
    from web_parser import parse_cast_from_url, ParserError
    WEB_PARSER_AVAILABLE = True
except ImportError:
    logger.error("web_parser.py 未找到或无法导入，从URL提取功能将不可用。")
    WEB_PARSER_AVAILABLE = False


@media_api_bp.route('/search_emby_library', methods=['GET'])
@processor_ready_required
def api_search_emby_library():
    query = request.args.get('query', '')
    if not query.strip():
        return jsonify({"error": "搜索词不能为空"}), 400

    try:
        # ✨✨✨ 调用改造后的函数，并传入 search_term ✨✨✨
        search_results = emby_handler.get_emby_library_items(
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
@login_required
@processor_ready_required
def api_get_media_for_editing(item_id):
    # 直接调用 core_processor 的新方法
    data_for_editing = extensions.media_processor_instance.get_cast_for_editing(item_id)
    
    if data_for_editing:
        return jsonify(data_for_editing)
    else:
        return jsonify({"error": f"无法获取项目 {item_id} 的编辑数据，请检查日志。"}), 404

@media_api_bp.route('/update_media_cast_sa/<item_id>', methods=['POST'])
@login_required
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
    接收一个 'url' 参数，从该URL获取图片并将其流式传输回客户端。
    """
    external_url = request.args.get('url')
    if not external_url:
        return jsonify({"error": "缺少 'url' 参数"}), 400

    try:
        # 使用 stream=True 进行流式请求，这对于大文件更高效
        response = requests.get(external_url, stream=True, timeout=10)

        # 检查请求是否成功
        response.raise_for_status()

        # 将外部服务器的响应流式传输回客户端
        # stream_with_context 确保请求上下文在流式传输期间保持活动状态
        return Response(
            stream_with_context(response.iter_content(chunk_size=8192)),
            content_type=response.headers.get('Content-Type'),
            status=response.status_code
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"通用图片代理错误: 无法获取 URL '{external_url}'. 错误: {e}")
        # 返回一个1x1的透明像素点作为占位符
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
    
# ✨✨✨   生成外部搜索链接 ✨✨✨
@media_api_bp.route('/parse_cast_from_url', methods=['POST'])
def api_parse_cast_from_url():
    # 检查 web_parser 是否可用
    try:
        from web_parser import parse_cast_from_url, ParserError
    except ImportError:
        return jsonify({"error": "网页解析功能在服务器端不可用。"}), 501

    data = request.json
    url_to_parse = data.get('url')
    if not url_to_parse:
        return jsonify({"error": "请求中未提供 'url' 参数"}), 400

    try:
        current_config = config_manager.APP_CONFIG
        headers = {'User-Agent': current_config.get('user_agent', '')}
        parsed_cast = parse_cast_from_url(url_to_parse, custom_headers=headers)
        
        frontend_cast = [{"name": item['actor'], "role": item['character']} for item in parsed_cast]
        return jsonify(frontend_cast)

    except ParserError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"解析 URL '{url_to_parse}' 时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "解析时发生未知的服务器错误"}), 500
    
# ✨✨✨ 一键翻译 ✨✨✨
@media_api_bp.route('/actions/translate_cast_sa', methods=['POST']) # 注意路径不同
@login_required
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
        item_details = emby_handler.get_emby_item_details(
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
    full_libraries_list = emby_handler.get_emby_libraries(
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
@login_required
def api_search_studios():
    """
    根据查询参数 'q' 动态搜索工作室列表。
    """
    search_term = request.args.get('q', '').strip()
    
    if not search_term:
        return jsonify([])
        
    try:
        studios = collection_db.search_unique_studios(search_term)
        return jsonify(studios)
    except Exception as e:
        logger.error(f"搜索工作室时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
