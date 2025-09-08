# routes/system.py

from flask import Blueprint, jsonify, request, Response, stream_with_context
import logging
import json
import re
import requests
import os
import docker
# 导入底层模块
import task_manager
from logger_setup import frontend_log_queue
import config_manager
import db_handler
import emby_handler
# 导入共享模块
import extensions
from extensions import login_required, task_lock_required
import tasks
import constants
import github_handler
# 1. 创建蓝图
system_bp = Blueprint('system', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

# 2. 定义路由

# --- 任务状态与控制 ---
@system_bp.route('/status', methods=['GET'])
def api_get_task_status():
    status_data = task_manager.get_task_status()
    status_data['logs'] = list(frontend_log_queue)
    return jsonify(status_data)

@system_bp.route('/trigger_stop_task', methods=['POST'])
def api_handle_trigger_stop_task():
    logger.debug("API (Blueprint): Received request to stop current task.")
    stopped_any = False
    if extensions.media_processor_instance:
        extensions.media_processor_instance.signal_stop()
        stopped_any = True
    if extensions.watchlist_processor_instance:
        extensions.watchlist_processor_instance.signal_stop()
        stopped_any = True
    if extensions.actor_subscription_processor_instance:
        extensions.actor_subscription_processor_instance.signal_stop()
        stopped_any = True

    if stopped_any:
        return jsonify({"message": "已发送停止任务请求。"}), 200
    else:
        return jsonify({"error": "核心处理器未就绪"}), 503

# --- API 端点：获取当前配置 ---
@system_bp.route('/config', methods=['GET'])
def api_get_config():
    try:
        # ★★★ 确保这里正确解包了元组 ★★★
        current_config = config_manager.APP_CONFIG 
        
        if current_config:
            current_config['emby_server_id'] = extensions.EMBY_SERVER_ID
            custom_theme = config_manager.load_custom_theme()
            current_config['custom_theme'] = custom_theme
            logger.trace(f"API /api/config (GET): 成功加载并返回配置。")
            return jsonify(current_config)
        else:
            logger.error(f"API /api/config (GET): config_manager.APP_CONFIG 为空或未初始化。")
            return jsonify({"error": "无法加载配置数据"}), 500
    except Exception as e:
        logger.error(f"API /api/config (GET) 获取配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取配置信息时发生服务器内部错误"}), 500

# --- 代理测试 ---
@system_bp.route('/proxy/test', methods=['POST'])
def test_proxy_connection():
    """
    接收代理 URL，并从配置中读取 TMDB API Key，进行一个完整的连接和认证测试。
    """
    data = request.get_json()
    proxy_url = data.get('url')

    if not proxy_url:
        return jsonify({"success": False, "message": "错误：未提供代理 URL。"}), 400

    # ★★★ 1. 从全局配置中获取 TMDB API Key ★★★
    tmdb_api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)

    # 如果用户还没填 API Key，提前告知
    if not tmdb_api_key:
        return jsonify({"success": False, "message": "测试失败：请先在上方配置 TMDB API Key。"}), 400

    test_target_url = "https://api.themoviedb.org/3/configuration"
    proxies = {"http": proxy_url, "https": proxy_url}
    
    # ★★★ 2. 将 API Key 加入到请求参数中 ★★★
    params = {"api_key": tmdb_api_key}

    try:
        response = requests.get(test_target_url, proxies=proxies, params=params, timeout=10)
        
        # ★★★ 3. 严格检查状态码，并对 401 给出特定提示 ★★★
        response.raise_for_status() # 这会对所有非 2xx 的状态码抛出 HTTPError 异常
        
        # 如果代码能执行到这里，说明状态码是 200 OK
        return jsonify({"success": True, "message": "代理和 API Key 均测试成功！"}), 200

    except requests.exceptions.HTTPError as e:
        # 专门捕获 HTTP 错误，并判断是否是 401
        if e.response.status_code == 401:
            return jsonify({"success": False, "message": "代理连接成功，但 TMDB API Key 无效或错误。"}), 401
        else:
            # 其他 HTTP 错误 (如 404, 500 等)
            return jsonify({"success": False, "message": f"HTTP 错误: 代理连接成功，但 TMDB 返回了 {e.response.status_code} 状态码。"}), 500
            
    except requests.exceptions.ProxyError as e:
        return jsonify({"success": False, "message": f"代理错误: {e}"}), 500
    except requests.exceptions.ConnectTimeout:
        return jsonify({"success": False, "message": "连接代理服务器超时，请检查地址和端口。"}), 500
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "message": f"网络请求失败: {e}"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": f"发生未知错误: {e}"}), 500

# --- API 端点：保存配置 ---
@system_bp.route('/config', methods=['POST'])
def api_save_config():
    from web_app import save_config_and_reload
    try:
        new_config_data = request.json
        if not new_config_data:
            return jsonify({"error": "请求体中未包含配置数据"}), 400
        
        # User ID 校验 (保留)
        user_id_to_save = new_config_data.get("emby_user_id", "").strip()
        if not user_id_to_save:
            error_message = "Emby User ID 不能为空！"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message}")
            return jsonify({"error": error_message}), 400
        if not re.match(r'^[a-f0-9]{32}$', user_id_to_save, re.I):
            error_message = "Emby User ID 格式不正确！"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message}")
            return jsonify({"error": error_message}), 400
        
        logger.info(f"API /api/config (POST): 收到新的配置数据，准备全面净化并保存...")

        # ▼▼▼ 核心修正：全面净化逻辑 ▼▼▼
        
        # 1. 提取Emby连接信息，准备获取“白名单”
        emby_url = new_config_data.get('emby_server_url')
        emby_api_key = new_config_data.get('emby_api_key')
        user_id = new_config_data.get('emby_user_id')
        
        valid_library_ids = None
        if emby_url and emby_api_key and user_id:
            logger.info("正在从Emby获取有效媒体库列表以进行净化...")
            valid_libraries = emby_handler.get_emby_libraries(emby_url, emby_api_key, user_id)
            if valid_libraries is not None:
                valid_library_ids = {lib['Id'] for lib in valid_libraries}
            else:
                logger.warning("无法从Emby获取媒体库列表，本次保存将跳过净化步骤。")

        # 2. 如果成功获取到白名单，则对所有相关字段进行净化
        if valid_library_ids is not None:
            
            # --- 净化字段 1: libraries_to_process ---
            if 'libraries_to_process' in new_config_data and isinstance(new_config_data['libraries_to_process'], list):
                original_ids = new_config_data['libraries_to_process']
                cleaned_ids = [lib_id for lib_id in original_ids if lib_id in valid_library_ids]
                if len(cleaned_ids) < len(original_ids):
                    removed_ids = set(original_ids) - set(cleaned_ids)
                    logger.info(f"配置净化 (任务库): 已自动移除 {len(removed_ids)} 个无效ID: {removed_ids}。")
                new_config_data['libraries_to_process'] = cleaned_ids

            # --- 净化字段 2: proxy_native_view_selection (新增逻辑) ---
            if 'proxy_native_view_selection' in new_config_data and isinstance(new_config_data['proxy_native_view_selection'], list):
                original_ids = new_config_data['proxy_native_view_selection']
                cleaned_ids = [lib_id for lib_id in original_ids if lib_id in valid_library_ids]
                if len(cleaned_ids) < len(original_ids):
                    removed_ids = set(original_ids) - set(cleaned_ids)
                    logger.info(f"配置净化 (虚拟库): 已自动移除 {len(removed_ids)} 个无效ID: {removed_ids}。")
                new_config_data['proxy_native_view_selection'] = cleaned_ids
        
        # ▲▲▲ 净化逻辑结束 ▲▲▲

        save_config_and_reload(new_config_data)  
        
        logger.debug("API /api/config (POST): 全面净化后的配置已成功传递给保存函数。")
        return jsonify({"message": "配置已成功保存并自动净化！"})
        
    except Exception as e:
        logger.error(f"API /api/config (POST) 保存配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"保存配置时发生服务器内部错误: {str(e)}"}), 500
    
# ★★★ 保存用户的自定义主题 ★★★
@system_bp.route('/config/custom_theme', methods=['POST'])
@login_required
def api_save_custom_theme():
    """
    接收前端发来的自定义主题JSON对象，并将其保存到配置文件。
    """
    try:
        theme_data = request.json
        if not isinstance(theme_data, dict):
            return jsonify({"error": "无效的主题数据格式，必须是一个JSON对象。"}), 400
        
        # 调用 config_manager 中的新函数来保存
        config_manager.save_custom_theme(theme_data)
        
        logger.info("用户的自定义主题已成功保存。")
        return jsonify({"message": "你的专属主题已保存！"})
        
    except Exception as e:
        logger.error(f"保存自定义主题时发生错误: {e}", exc_info=True)
        return jsonify({"error": "保存自定义主题时发生服务器内部错误。"}), 500
    
# --- 调用文件删除函数的API端点 ---
@system_bp.route('/config/custom_theme', methods=['DELETE'])
@login_required
def api_delete_custom_theme():
    """
    删除 custom_theme.json 文件。
    """
    try:
        # ★★★ 核心修改：调用 config_manager 中的文件删除函数 ★★★
        success = config_manager.delete_custom_theme()
        
        if success:
            logger.info("API: 用户的自定义主题文件已成功删除。")
            return jsonify({"message": "自定义主题已删除。"})
        else:
            # 这种情况只在极端的权限问题下发生
            return jsonify({"error": "删除自定义主题文件时发生服务器内部错误。"}), 500

    except Exception as e:
        logger.error(f"删除自定义主题时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "删除自定义主题时发生服务器内部错误。"}), 500

# +++ 关于页面的信息接口 +++
@system_bp.route('/system/about_info', methods=['GET'])
def get_about_info():
    """
    【V2 - 支持认证版】获取关于页面的所有信息，包括当前版本和 GitHub releases。
    会从配置中读取 GitHub Token 用于认证，以提高 API 速率限制。
    """
    try:
        # ★★★ 1. 从全局配置中获取 GitHub Token ★★★
        github_token = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN)

        proxies = config_manager.get_proxies_for_requests()
        # ★★★ 2. 将 Token 传递给 get_github_releases 函数 ★★★
        releases = github_handler.get_github_releases(
            owner=constants.GITHUB_REPO_OWNER,
            repo=constants.GITHUB_REPO_NAME,
            token=github_token,  # <--- 将令牌作为参数传入
            proxies=proxies
        )

        if releases is None:
            # 即使获取失败，也返回一个正常的结构，只是 releases 列表为空
            releases = []
            logger.warning("API /system/about_info: 从 GitHub 获取 releases 失败，将返回空列表。")

        response_data = {
            "current_version": constants.APP_VERSION,
            "releases": releases
        }
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"API /system/about_info 发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取版本信息时发生服务器内部错误"}), 500

# --- 一键更新 ---
@system_bp.route('/system/update/stream', methods=['GET'])
@login_required
@task_lock_required
def stream_update_progress():
    """
    【V10 - 最终可行自更新版】
    通过启动一个临时的“更新器容器”来执行更新操作，解决了“进程自杀”的悖论。
    """
    def generate_progress():
        def send_event(data):
            yield f"data: {json.dumps(data)}\n\n"

        client = None
        proxies_config = config_manager.get_proxies_for_requests()
        old_env = os.environ.copy()
        try:
            if proxies_config and proxies_config.get('https'):
                proxy_url = proxies_config['https']
                os.environ['HTTPS_PROXY'] = proxy_url
                os.environ['HTTP_PROXY'] = proxy_url # 有些系统也需要设置 http_proxy
                yield from send_event({"status": f"检测到代理配置，将通过 {proxy_url} 拉取镜像...", "layers": {}})
            client = docker.from_env()
            container_name = config_manager.APP_CONFIG.get('container_name', 'emby-toolkit')
            image_name_tag = config_manager.APP_CONFIG.get('docker_image_name', 'hbq0405/emby-toolkit:latest')


            yield from send_event({"status": f"正在检查并拉取最新镜像: {image_name_tag}...", "layers": {}})
            
            # 使用低级 API 获取流式输出
            stream = client.api.pull(image_name_tag, stream=True, decode=True)
            
            # ★★★ 2. 重新设计状态跟踪变量 ★★★
            layers_status = {}
            is_new_image_pulled = False
            all_layer_ids = set()

            for line in stream:
                layer_id = line.get('id')
                status = line.get('status')

                # ★ 1. 改进对全局状态行的处理，并用它来识别所有层
                if not layer_id and status:
                    # 当Docker说 "Pulling fs layer" 时，它正在注册一个新的层
                    if 'Pulling fs layer' in status:
                        # 这时事件流里还没有ID，我们暂时无法获取，但可以预见会有新层
                        pass
                    yield from send_event({"status": status, "layers": layers_status})
                    continue

                # ★ 2. 过滤掉无效的层ID，比如 'latest'
                if not layer_id or len(layer_id) < 10: # 真实的层ID通常是一长串字符
                    continue
                
                # 记录所有出现过的真实层ID
                all_layer_ids.add(layer_id)

                # 初始化或更新层的状态
                if layer_id not in layers_status:
                    layers_status[layer_id] = {"status": "", "progress": 0, "detail": ""}
                
                layers_status[layer_id]['status'] = status

                # 处理进度详情
                if 'progressDetail' in line:
                    details = line['progressDetail']
                    current = details.get('current', 0)
                    total = details.get('total', 0)
                    layers_status[layer_id]['current_bytes'] = current
                    layers_status[layer_id]['total_bytes'] = total
                    if total > 0:
                        progress_percent = int((current / total) * 100)
                        layers_status[layer_id]['progress'] = progress_percent
                        # 将字节转换为易读的 MB
                        current_mb = round(current / (1024 * 1024), 2)
                        total_mb = round(total / (1024 * 1024), 2)
                        layers_status[layer_id]['detail'] = f"{current_mb}MB / {total_mb}MB"
                
                # 处理非下载状态
                if any(s in status for s in ["Pull complete", "Already exists", "Download complete"]):
                    layers_status[layer_id]['progress'] = 100
                    layers_status[layer_id]['detail'] = "" # 完成后清空详情

                completed_layers = 0
                for lid in all_layer_ids:
                    # 检查这个层是否已经出现在状态字典中，并且状态是完成状态
                    if lid in layers_status and layers_status[lid].get('progress') == 100:
                        completed_layers += 1
                
                total_layers = len(all_layer_ids)
                overall_progress = int((completed_layers / total_layers) * 100) if total_layers > 0 else 0
                
                # ★ 4. 每次循环都发送完整的状态对象
                yield from send_event({
                    "status": "正在拉取...", 
                    "layers": layers_status, 
                    "overall_progress": overall_progress
                })

                last_status_line = line
                # 检查是否有新内容被拉取
                if status == "Pull complete":
                    is_new_image_pulled = True
            
            # 检查最终状态
            final_status_line = line.get('status', '')
            if 'Status: Image is up to date' in final_status_line:
                 yield from send_event({"status": "当前已是最新版本。", "progress": 100})
                 yield from send_event({"event": "DONE", "message": "无需更新。"})
                 return

            # --- 2. ★★★ 核心：召唤并启动“更新器容器” ★★★ ---
            yield from send_event({"status": "准备应用更新...", "progress": 70})

            try:
                # 获取旧容器的完整配置，以便新容器可以重建它
                old_container = client.containers.get(container_name)
                
                # Watchtower 使用的官方工具镜像，非常小巧可靠
                updater_image = "containrrr/watchtower"
                
                # 构建传递给更新器容器的命令
                # --cleanup 会移除旧镜像，--run-once 会让它执行一次就退出
                command = [
                    "--cleanup",
                    "--run-once",
                    container_name # 明确告诉 watchtower 只更新我们自己
                ]

                # 启动更新器容器！
                logger.info(f"正在应用更新 '{container_name}'...")
                client.containers.run(
                    image=updater_image,
                    command=command,
                    remove=True,  # a.k.a. --rm，任务完成后自动删除自己
                    detach=True,  # 在后台运行
                    volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}}
                )
                
                yield from send_event({"status": "更新任务已成功交接给临时更新器！本容器将在后台被重启。", "progress": 90})
                yield from send_event({"status": "稍后手动刷新页面以访问新版本。", "progress": 100, "event": "DONE"})

            except docker.errors.NotFound:
                yield from send_event({"status": f"错误：找不到名为 '{container_name}' 的容器来更新。", "event": "ERROR"})
            except Exception as e_updater:
                error_msg = f"错误：启动临时更新器时失败: {e_updater}"
                logger.error(error_msg, exc_info=True)
                yield from send_event({"status": error_msg, "event": "ERROR"})

        except Exception as e:
            error_message = f"更新过程中发生未知错误: {str(e)}"
            logger.error(f"[Update Stream]: {error_message}", exc_info=True)
            yield from send_event({"status": error_message, "event": "ERROR"})
        finally:
            # ★★★ 3. 关键：无论成功还是失败，都恢复原始的环境变量 ★★★
            os.environ.clear()
            os.environ.update(old_env)
            logger.debug("已恢复原始环境变量。")

    return Response(stream_with_context(generate_progress()), mimetype='text/event-stream')

# +++ 重启容器 +++
@system_bp.route('/system/restart', methods=['POST'])
@login_required
def restart_container():
    """
    重启运行此应用的 Docker 容器。
    """
    try:
        client = docker.from_env()
        # 从配置中获取容器名，如果未配置则使用默认值
        container_name = config_manager.APP_CONFIG.get('container_name', 'emby-toolkit')
        
        if not container_name:
            logger.error("API: 尝试重启容器，但配置中未找到 'container_name'。")
            return jsonify({"error": "未在配置中指定容器名称。"}), 500

        logger.info(f"API: 收到重启容器 '{container_name}' 的请求。")
        container = client.containers.get(container_name)
        container.restart()
        
        return jsonify({"message": f"已向容器 '{container_name}' 发送重启指令。应用将在片刻后恢复。"}), 200

    except docker.errors.NotFound:
        error_msg = f"API: 尝试重启容器，但名为 '{container_name}' 的容器未找到。"
        logger.error(error_msg)
        return jsonify({"error": error_msg}), 404
    except Exception as e:
        error_msg = f"API: 重启容器时发生未知错误: {e}"
        logger.error(error_msg, exc_info=True)
        return jsonify({"error": f"发生意外错误: {str(e)}"}), 500

# ★★★ 提供电影类型映射的API ★★★
@system_bp.route('/config/genres', methods=['GET'])
@login_required
def api_get_genres_config():
    """
    (V2 - 数据库驱动版)
    从媒体元数据缓存中动态获取所有唯一的电影类型。
    """
    try:
        # ★★★ 核心修复：直接调用新的数据库函数 ★★★
        genres = db_handler.get_unique_genres()
        # API直接返回一个字符串列表，例如 ["动作", "喜剧", "科幻"]
        return jsonify(genres)
    except Exception as e:
        logger.error(f"动态获取电影类型时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
    
# ★★★ 提供电影工作室列表的API ★★★
@system_bp.route('/config/studios', methods=['GET'])
@login_required
def api_get_studios_config():
    """
    从媒体元数据缓存中动态获取所有唯一的工作室。
    """
    try:
        studios = db_handler.get_unique_studios()
        return jsonify(studios)
    except Exception as e:
        logger.error(f"动态获取工作室列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500