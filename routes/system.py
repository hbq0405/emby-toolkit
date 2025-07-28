# routes/system.py

from flask import Blueprint, jsonify, request
import logging
import threading
import re
import os
import time
import docker
import subprocess

# 导入底层模块
import task_manager
from logger_setup import frontend_log_queue
import config_manager
# 导入共享模块
import extensions
from extensions import login_required, processor_ready_required
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

# ✨✨✨ “立即执行”API接口 ✨✨✨
@system_bp.route('/tasks/trigger/<task_identifier>', methods=['POST'])
@extensions.login_required
@extensions.task_lock_required
def api_trigger_task_now(task_identifier: str):
    task_registry = tasks.get_task_registry()
    task_info = task_registry.get(task_identifier)
    if not task_info:
        return jsonify({"status": "error", "message": f"未知的任务标识符: {task_identifier}"}), 404

    task_function, task_name = task_info
    kwargs = {}
    if task_identifier == 'full-scan':
        data = request.get_json(silent=True) or {}
        kwargs['process_episodes'] = data.get('process_episodes', True)
    
    success = task_manager.submit_task(task_function, task_name, **kwargs)
    
    if success:
        return jsonify({"status": "success", "message": "任务已成功提交到后台队列。", "task_name": task_name}), 202
    else:
        return jsonify({"status": "error", "message": "提交任务失败，已有任务在运行。"}), 409
    
# --- API 端点：获取当前配置 ---
@system_bp.route('/config', methods=['GET'])
def api_get_config():
    try:
        # ★★★ 确保这里正确解包了元组 ★★★
        current_config = config_manager.APP_CONFIG 
        
        if current_config:
            current_config['emby_server_id'] = extensions.EMBY_SERVER_ID
            logger.trace(f"API /api/config (GET): 成功加载并返回配置。")
            return jsonify(current_config)
        else:
            logger.error(f"API /api/config (GET): config_manager.APP_CONFIG 为空或未初始化。")
            return jsonify({"error": "无法加载配置数据"}), 500
    except Exception as e:
        logger.error(f"API /api/config (GET) 获取配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取配置信息时发生服务器内部错误"}), 500


# --- API 端点：保存配置 ---
@system_bp.route('/config', methods=['POST'])
def api_save_config():
    from web_app import save_config_and_reload
    try:
        new_config_data = request.json
        if not new_config_data:
            return jsonify({"error": "请求体中未包含配置数据"}), 400
        
        # ★★★ 核心修改：在这里进行严格校验并“打回去” ★★★
        user_id_to_save = new_config_data.get("emby_user_id", "").strip()

        # 规则1：检查是否为空
        if not user_id_to_save:
            error_message = "Emby User ID 不能为空！这是获取媒体库列表的必需项。"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message}")
            return jsonify({"error": error_message}), 400

        # 规则2：检查格式是否正确
        if not re.match(r'^[a-f0-9]{32}$', user_id_to_save, re.I):
            error_message = "Emby User ID 格式不正确！它应该是一串32位的字母和数字。"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message} (输入值: '{user_id_to_save}')")
            return jsonify({"error": error_message}), 400
        # ★★★ 校验结束 ★★★

        logger.info(f"API /api/config (POST): 收到新的配置数据，准备保存...")
        
        # 校验通过后，才调用保存函数
        save_config_and_reload(new_config_data)  
        
        logger.debug("API /api/config (POST): 配置已成功传递给 save_config 函数。")
        return jsonify({"message": "配置已成功保存并已触发重新加载。"})
        
    except Exception as e:
        logger.error(f"API /api/config (POST) 保存配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"保存配置时发生服务器内部错误: {str(e)}"}), 500
    
# +++ 关于页面的信息接口 +++
@system_bp.route('/system/about_info', methods=['GET'])
def get_about_info():
    """
    获取关于页面的所有信息，包括当前版本和 GitHub releases。
    """
    try:
        # 从 GitHub 获取 releases
        releases = github_handler.get_github_releases(
            owner=constants.GITHUB_REPO_OWNER,
            repo=constants.GITHUB_REPO_NAME
        )

        if releases is None:
            # 即使获取失败，也返回一个正常的结构，只是 releases 列表为空
            releases = []

        response_data = {
            "current_version": constants.APP_VERSION,
            "releases": releases
        }
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"API /system/about_info 发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取版本信息时发生服务器内部错误"}), 500

# --- 一键更新容器 ---
@system_bp.route('/system/trigger_update', methods=['POST'])
@extensions.login_required
def trigger_self_update():
    container_name = os.environ.get('CONTAINER_NAME', 'emby-actor-processor')
    logger.info(f"api: 接收到更新请求，目标容器: '{container_name}'")

    def update_task():
        logger.info("[一键更新]: 后台更新线程已启动。")
        try:
            client = docker.from_env()
            container = client.containers.get(container_name)

            image_name_tag = os.environ.get('CONTAINER_IMAGE')
            if not image_name_tag:
                if container.image.tags:
                    image_name_tag = container.image.tags[0]
                else:
                    logger.error("[一键更新]: 无法获取镜像 tag，且未配置 CONTAINER_IMAGE 环境变量。")
                    return

            logger.info(f"[一键更新]: 正在拉取最新镜像: {image_name_tag}...")
            new_image = client.images.pull(image_name_tag)

            if container.image.id == new_image.id:
                logger.info("[一键更新]: 当前已是最新版本，无需更新。")
            else:
                logger.info("[一键更新]: 新镜像拉取完成。开始使用 docker-compose 重启容器...")

                # 动态使用持久化目录
                compose_dir = config_manager.PERSISTENT_DATA_PATH

                try:
                    subprocess.run(
                        ["docker-compose", "up", "-d", "--force-recreate", container_name],
                        check=True,
                        cwd=compose_dir
                    )
                    logger.info("[Update Worker]: 容器重启成功。")
                except subprocess.CalledProcessError as e:
                    logger.error(f"[Update Worker]: 容器重启失败: {e}")

        except Exception as e:
            logger.error(f"[Update Worker]: 后台更新线程发生错误: {e}", exc_info=True)

    try:
        docker.from_env().containers.get(container_name)

        update_thread = threading.Thread(target=update_task, daemon=True)
        update_thread.start()

        logger.trace("API: 后台更新任务已启动，立即返回 202 Accepted 响应。")
        return jsonify({
            "success": True,
            "message": "更新指令已发送！应用将在后台下载新版本并自动重启。"
        }), 202

    except docker.errors.NotFound:
        logger.error(f"API: 未找到名为 '{container_name}' 的容器！无法启动更新。")
        return jsonify({"success": False, "message": f"容器 '{container_name}' 未找到。"}), 404
    except Exception as e:
        logger.error(f"API: 启动更新线程时发生错误: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"启动更新失败: {str(e)}"}), 500