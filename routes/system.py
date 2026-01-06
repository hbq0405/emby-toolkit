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
import handler.emby as emby
# 导入共享模块
import extensions
from database import custom_collection_db
from extensions import admin_required, task_lock_required
import constants
import handler.github as github
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

# --- AI 测试 ---
@system_bp.route('/ai/test', methods=['POST'])
@admin_required
def api_test_ai_connection():
    """
    测试 AI 翻译配置是否有效。
    接收前端传来的临时配置，尝试翻译一个单词。
    """
    from ai_translator import AITranslator
    
    # 1. 获取前端传来的配置（可能是还没保存的）
    test_config = request.json
    if not test_config:
        return jsonify({"success": False, "message": "缺少配置数据"}), 400

    logger.info(f"  ➜ 收到 AI 测试请求，提供商: {test_config.get('ai_provider')}")

    try:
        # 2. 实例化一个临时的翻译器
        # 注意：AITranslator 初始化时会检查 API Key
        translator = AITranslator(test_config)
        
        # 3. 执行一个简单的翻译任务
        test_text = "Bald Qiang"
        # 使用 fast 模式进行测试
        result = translator.translate(test_text)
        
        if result and result != test_text:
            return jsonify({
                "success": True, 
                "message": f"连接成功！测试翻译结果: '{test_text}' ➜ '{result}'"
            })
        elif result == test_text:
             return jsonify({
                "success": True, 
                "message": f"连接成功，但 AI 返回了原词（可能模型认为无需翻译）。"
            })
        else:
            return jsonify({"success": False, "message": "AI 未返回有效结果。"}), 500

    except Exception as e:
        error_msg = str(e)
        logger.error(f"AI 测试失败: {error_msg}")
        return jsonify({"success": False, "message": f"测试失败: {error_msg}"}), 500

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
    
# --- Telegram 测试 ---
@system_bp.route('/telegram/test', methods=['POST'])
@admin_required
def api_test_telegram_connection():
    """
    测试 Telegram 机器人配置。
    接收前端传来的 Token 和 Chat ID，尝试发送一条测试消息。
    """
    data = request.json
    token = data.get('token')
    chat_id = data.get('chat_id')

    if not token or not chat_id:
        return jsonify({"success": False, "message": "缺少 Token 或 Chat ID"}), 400

    # 构造测试消息
    import time
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    text = f"🔔 *Emby Toolkit 测试消息*\n\n这是一条测试消息，证明您的机器人配置正确。\n⏱ 时间: `{current_time}`"
    
    # 处理 Chat ID (支持 @username)
    final_chat_id = str(chat_id).strip()
    if final_chat_id.startswith('https://t.me/'):
        username = final_chat_id.split('/')[-1]
        if username:
            final_chat_id = f'@{username}'

    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': final_chat_id,
        'text': text, 
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True
    }

    try:
        # 获取代理配置
        proxies = config_manager.get_proxies_for_requests()
        
        logger.info(f"正在测试发送 Telegram 消息至: {final_chat_id}")
        response = requests.post(api_url, json=payload, timeout=15, proxies=proxies)
        
        if response.status_code == 200:
            return jsonify({
                "success": True, 
                "message": "测试消息发送成功！请检查您的 Telegram。"
            })
        else:
            return jsonify({
                "success": False, 
                "message": f"发送失败 (HTTP {response.status_code}): {response.text}"
            }), 500

    except requests.exceptions.ProxyError:
        return jsonify({"success": False, "message": "代理连接失败，请检查网络代理设置。"}), 500
    except requests.exceptions.ConnectTimeout:
        return jsonify({"success": False, "message": "连接 Telegram 服务器超时。"}), 500
    except Exception as e:
        logger.error(f"Telegram 测试发生错误: {e}")
        return jsonify({"success": False, "message": f"发生未知错误: {str(e)}"}), 500

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
        
        logger.info(f"  ➜ 收到新的配置数据，准备全面净化并保存...")

        # ▼▼▼ 核心修正：全面净化逻辑 ▼▼▼
        
        # 1. 提取Emby连接信息，准备获取“白名单”
        emby_url = new_config_data.get('emby_server_url')
        emby_api_key = new_config_data.get('emby_api_key')
        user_id = new_config_data.get('emby_user_id')
        
        valid_library_ids = None
        if emby_url and emby_api_key and user_id:
            logger.trace("  ➜ 正在从Emby获取有效媒体库列表以进行净化...")
            valid_libraries = emby.get_emby_libraries(emby_url, emby_api_key, user_id)
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
@admin_required
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
@admin_required
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
        releases = github.get_github_releases(
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
@admin_required
@task_lock_required
def stream_update_progress():
    """
    【V11 - 简化UI版】
    通过启动一个临时的“更新器容器”来执行更新操作，并向前端提供简化的状态文本流。
    """
    def generate_progress():
        def send_event(data):
            # 确保发送的是 JSON 格式的字符串
            yield f"data: {json.dumps(data)}\n\n"

        client = None
        proxies_config = config_manager.get_proxies_for_requests()
        old_env = os.environ.copy()
        try:
            # 设置代理
            if proxies_config and proxies_config.get('https'):
                proxy_url = proxies_config['https']
                os.environ['HTTPS_PROXY'] = proxy_url
                os.environ['HTTP_PROXY'] = proxy_url
                yield from send_event({"status": f"检测到代理配置，将通过 {proxy_url} 拉取镜像..."})
            
            client = docker.from_env()
            container_name = config_manager.APP_CONFIG.get('container_name', 'emby-toolkit')
            image_name_tag = config_manager.APP_CONFIG.get('docker_image_name', 'hbq0405/emby-toolkit:latest')

            yield from send_event({"status": f"正在检查并拉取最新镜像: {image_name_tag}..."})
            
            # 使用流式 API 以保持连接并提供基本反馈
            stream = client.api.pull(image_name_tag, stream=True, decode=True)
            
            last_line = {}
            for line in stream:
                # 保持循环以防止超时，但我们不再向前端发送每一层的细节
                last_line = line

            # 检查最终状态
            final_status = last_line.get('status', '')
            if 'Status: Image is up to date' in final_status:
                 yield from send_event({"status": "当前已是最新版本。"})
                 yield from send_event({"event": "DONE", "message": "无需更新。"})
                 return
            
            # 如果没有明确的“up to date”消息，并且没有错误，我们假设拉取成功
            if 'errorDetail' in last_line:
                error_msg = f"拉取镜像失败: {last_line['errorDetail']['message']}"
                logger.error(error_msg)
                yield from send_event({"status": error_msg, "event": "ERROR"})
                return

            # --- 核心：召唤并启动“更新器容器” ---
            yield from send_event({"status": "镜像拉取完成，准备应用更新..."})

            try:
                updater_image = "containrrr/watchtower"
                
                # 确保 watchtower 镜像存在，如果不存在则拉取
                try:
                    client.images.get(updater_image)
                except docker.errors.ImageNotFound:
                    yield from send_event({"status": f"正在拉取更新器工具: {updater_image}..."})
                    client.images.pull(updater_image)

                command = ["--cleanup", "--run-once", container_name]

                logger.info(f"正在应用更新 '{container_name}'...")
                client.containers.run(
                    image=updater_image,
                    command=command,
                    remove=True,
                    detach=True,
                    volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}}
                )
                
                yield from send_event({"status": "更新任务已成功交接给临时更新器！本容器将在后台被重启。"})
                yield from send_event({"status": "稍后手动刷新页面以访问新版本。", "event": "DONE"})

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
            os.environ.clear()
            os.environ.update(old_env)
            logger.debug("已恢复原始环境变量。")

    return Response(stream_with_context(generate_progress()), mimetype='text/event-stream')

# +++ 重启容器 +++
@system_bp.route('/system/restart', methods=['POST'])
@admin_required
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