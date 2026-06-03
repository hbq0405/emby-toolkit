#tasks/system_update.py
import docker
import logging
import os
import time
import task_manager
import config_manager
import constants
import handler.github as github
logger = logging.getLogger(__name__)
DEFAULT_CONTAINER_NAME = 'emby-toolkit'
DEFAULT_IMAGE_NAME_TAG = 'hbq0405/emby-toolkit:latest'


def _clean_version_text(value, default=None):
    text = str(value or '').strip()
    return text or default


def _resolve_self_container_target(client):
    """尽量从当前运行中的容器上下文识别自身容器名和镜像名。"""
    hostname = _clean_version_text(os.environ.get('HOSTNAME'))
    if not hostname or not client:
        return {}

    try:
        current_container = client.containers.get(hostname)
    except Exception as e:
        logger.debug(f"无法根据 HOSTNAME={hostname} 识别当前容器: {e}")
        return {}

    image_name_tag = _clean_version_text(
        ((getattr(current_container, 'attrs', {}) or {}).get('Config') or {}).get('Image')
    )
    if not image_name_tag:
        tags = list(getattr(getattr(current_container, 'image', None), 'tags', None) or [])
        image_name_tag = _clean_version_text(tags[0] if tags else None)

    return {
        "container_name": _clean_version_text(getattr(current_container, 'name', None)),
        "docker_image_name": image_name_tag,
    }


def resolve_update_target(config_source=None, docker_client=None):
    """
    解析系统更新目标。
    优先级：
    1. 显式配置
    2. 环境变量 CONTAINER_NAME / DOCKER_IMAGE_NAME
    3. 当前运行容器自动识别
    4. 代码默认值
    """
    config_source = config_source or {}
    app_config = getattr(config_manager, 'APP_CONFIG', {}) or {}

    container_name = _clean_version_text(
        config_source.get('container_name') or app_config.get('container_name')
    )
    image_name_tag = _clean_version_text(
        config_source.get('docker_image_name') or app_config.get('docker_image_name')
    )

    env_container_name = _clean_version_text(os.environ.get('CONTAINER_NAME'))
    env_image_name_tag = _clean_version_text(os.environ.get('DOCKER_IMAGE_NAME'))
    if not container_name and env_container_name:
        container_name = env_container_name
    if not image_name_tag and env_image_name_tag:
        image_name_tag = env_image_name_tag

    client = docker_client
    if (not container_name or not image_name_tag) and client is None:
        try:
            client = docker.from_env()
        except Exception as e:
            logger.debug(f"自动识别更新目标时无法连接 Docker: {e}")

    if not container_name or not image_name_tag:
        runtime_target = _resolve_self_container_target(client)
        if not container_name:
            container_name = _clean_version_text(runtime_target.get('container_name'))
        if not image_name_tag:
            image_name_tag = _clean_version_text(runtime_target.get('docker_image_name'))

    return {
        "container_name": container_name or DEFAULT_CONTAINER_NAME,
        "docker_image_name": image_name_tag or DEFAULT_IMAGE_NAME_TAG,
    }


def get_system_update_version_info():
    """返回当前 ETK 版本和可见的最新发布版本。"""
    current_version = _clean_version_text(getattr(constants, 'APP_VERSION', None), '0.0.0')
    target_version = None

    try:
        github_token = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_GITHUB_TOKEN)
        releases = github.get_github_releases(
            owner=constants.GITHUB_REPO_OWNER,
            repo=constants.GITHUB_REPO_NAME,
            token=github_token,
            proxies=config_manager.get_proxies_for_requests(),
        ) or []
        if releases:
            target_version = _clean_version_text(releases[0].get('version'))
    except Exception as e:
        logger.debug(f"获取最新版本信息失败，将继续执行更新检查: {e}")

    return {
        "current_version": current_version,
        "target_version": target_version,
    }


def _update_process_generator(container_name, image_name_tag):
    """
    核心更新逻辑生成器。
    yield 返回字典格式的状态信息: {"status": "消息内容", "event": "可选事件类型(DONE/ERROR)"}
    """
    client = None
    proxies_config = config_manager.get_proxies_for_requests()
    old_env = os.environ.copy()
    version_info = get_system_update_version_info()
    current_version = version_info.get('current_version')
    target_version = version_info.get('target_version')
    try:
        # 设置代理环境变量，以便 docker sdk 使用
        if proxies_config and proxies_config.get('https'):
            proxy_url = proxies_config['https']
            os.environ['HTTPS_PROXY'] = proxy_url
            os.environ['HTTP_PROXY'] = proxy_url
            yield {"status": f"检测到代理配置，将通过 {proxy_url} 拉取镜像...", "current_version": current_version, "target_version": target_version}
        
        try:
            client = docker.from_env()
        except Exception as e:
            yield {"status": f"无法连接 Docker 守护进程: {e}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
            return

        try:
            target_container = client.containers.get(container_name)
        except docker.errors.NotFound:
            yield {
                "status": f"错误：找不到名为 '{container_name}' 的容器，请先检查系统设置中的容器名称。",
                "event": "ERROR",
                "current_version": current_version,
                "target_version": target_version,
            }
            return

        current_image_id = _clean_version_text(getattr(getattr(target_container, 'image', None), 'id', None))

        yield {"status": f"正在检查并拉取最新镜像: {image_name_tag}...", "current_version": current_version, "target_version": target_version}
        
        # 使用流式 API 拉取镜像
        try:
            stream = client.api.pull(image_name_tag, stream=True, decode=True)
            last_status = ''
            for line in stream:
                if not isinstance(line, dict):
                    continue
                if line.get('errorDetail') or line.get('error'):
                    error_detail = line.get('errorDetail') or {}
                    error_msg = error_detail.get('message') or line.get('error') or '未知错误'
                    yield {"status": f"拉取镜像失败: {error_msg}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
                    return
                status = str(line.get('status') or '').strip()
                if status:
                    last_status = status

            latest_image = client.images.get(image_name_tag)
            latest_image_id = _clean_version_text(getattr(latest_image, 'id', None))
            if not latest_image_id:
                yield {
                    "status": "拉取镜像后未能识别最新镜像 ID，已停止更新。",
                    "event": "ERROR",
                    "current_version": current_version,
                    "target_version": target_version,
                }
                return

            if current_image_id and current_image_id == latest_image_id:
                final_msg = "当前容器已运行最新镜像，无需更新。"
                if last_status:
                    final_msg = f"{final_msg} Docker 返回: {last_status}"
                yield {"status": final_msg, "event": "NO_UPDATE", "current_version": current_version, "target_version": target_version}
                return

        except Exception as e:
            yield {"status": f"拉取镜像过程中发生异常: {e}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
            return

        # --- 核心：召唤并启动“更新器容器” ---
        yield {"status": "镜像拉取完成，准备应用更新...", "current_version": current_version, "target_version": target_version}

        try:
            updater_image = "containrrr/watchtower"
            
            # 确保 watchtower 镜像存在
            try:
                client.images.get(updater_image)
            except docker.errors.ImageNotFound:
                yield {"status": f"正在拉取更新器工具: {updater_image}...", "current_version": current_version, "target_version": target_version}
                client.images.pull(updater_image)

            # Watchtower 命令：清理旧镜像，只运行一次，指定容器名
            command = ["--cleanup", "--run-once", container_name]

            yield {"status": f"正在启动 Watchtower 更新容器 '{container_name}'...", "current_version": current_version, "target_version": target_version}
            
            client.containers.run(
                image=updater_image,
                command=command,
                remove=True,
                detach=True,
                volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}}
            )
            
            yield {
                "status": "更新指令已发送！本容器即将重启...",
                "event": "RESTARTING",
                "current_version": current_version,
                "target_version": target_version,
            }
            yield {
                "status": "更新任务已成功交接给临时更新器。",
                "event": "DONE",
                "updated": True,
                "current_version": current_version,
                "target_version": target_version,
            }
        except Exception as e_updater:
            yield {"status": f"错误：启动临时更新器时失败: {e_updater}", "event": "ERROR", "current_version": current_version, "target_version": target_version}

    except Exception as e:
        yield {"status": f"更新过程中发生未知错误: {str(e)}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
    finally:
        # 恢复环境变量
        os.environ.clear()
        os.environ.update(old_env)

def task_check_and_update_container(processor):
    """
    【后台任务版】检查并更新容器。
    此函数适配 task_manager 的日志和进度更新方式。
    """
    update_target = resolve_update_target(getattr(processor, 'config', {}) or {})
    container_name = update_target['container_name']
    image_name_tag = update_target['docker_image_name']
    logger.trace(f"--- 开始执行系统更新检查 (容器: {container_name}) ---")
    task_manager.update_status_from_thread(0, "准备检查更新...")
    result = {
        "ok": False,
        "updated": False,
        "message": "",
        **get_system_update_version_info(),
    }

    # 调用生成器，消费消息并转换为日志
    generator = _update_process_generator(container_name, image_name_tag)

    try:
        for event in generator:
            msg = event.get('status', '')
            evt_type = event.get('event')
            if event.get('current_version'):
                result['current_version'] = event.get('current_version')
            if event.get('target_version'):
                result['target_version'] = event.get('target_version')
            
            if evt_type == 'ERROR':
                logger.error(f"  ➜ {msg}")
                task_manager.update_status_from_thread(-1, f"更新失败: {msg}")
                result.update({"ok": False, "updated": False, "message": msg, "status": "error"})
                return result
            
            logger.info(f"  ➜ {msg}")
            
            # 简单的进度模拟
            if "拉取" in msg:
                task_manager.update_status_from_thread(30, msg)
            elif "应用更新" in msg:
                task_manager.update_status_from_thread(80, msg)
            elif evt_type == 'NO_UPDATE':
                task_manager.update_status_from_thread(100, "已是最新版本")
                result.update({"ok": True, "updated": False, "message": msg, "status": "up_to_date"})
            
            if evt_type == 'RESTARTING':
                logger.warning("  ➜ 系统即将重启以应用更新...")
                task_manager.update_status_from_thread(100, "系统正在重启...")
                result.update({"ok": True, "updated": True, "message": msg, "status": "restarting"})
                # 给一点时间让日志写完
                time.sleep(3)
            elif evt_type == 'DONE':
                result.update({
                    "ok": True,
                    "updated": bool(event.get('updated', result.get('updated'))),
                    "message": msg,
                    "status": "done",
                })
                
    except Exception as e:
        logger.error(f"更新任务异常: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务异常")
        result.update({"ok": False, "updated": False, "message": f"任务异常: {e}", "status": "exception"})

    if not result.get('message'):
        result.update({"ok": True, "updated": False, "message": "更新检查完成。", "status": "done"})

    return result
