#tasks/system_update.py
import docker
import logging
import os
import json
import time
import task_manager
import config_manager
import extensions
logger = logging.getLogger(__name__)
def _update_process_generator(container_name, image_name_tag):
    """
    核心更新逻辑生成器。
    yield 返回字典格式的状态信息: {"status": "消息内容", "event": "可选事件类型(DONE/ERROR)"}
    """
    client = None
    proxies_config = config_manager.get_proxies_for_requests()
    old_env = os.environ.copy()
    try:
        # 设置代理环境变量，以便 docker sdk 使用
        if proxies_config and proxies_config.get('https'):
            proxy_url = proxies_config['https']
            os.environ['HTTPS_PROXY'] = proxy_url
            os.environ['HTTP_PROXY'] = proxy_url
            yield {"status": f"检测到代理配置，将通过 {proxy_url} 拉取镜像..."}
        
        try:
            client = docker.from_env()
        except Exception as e:
            yield {"status": f"无法连接 Docker 守护进程: {e}", "event": "ERROR"}
            return

        yield {"status": f"正在检查并拉取最新镜像: {image_name_tag}..."}
        
        # 使用流式 API 拉取镜像
        try:
            stream = client.api.pull(image_name_tag, stream=True, decode=True)
            last_line = {}
            for line in stream:
                last_line = line
                # 这里可以选择性 yield 详细进度，但为了通用性，我们只在最后检查结果
            
            # 检查最终状态
            final_status = last_line.get('status', '')
            if 'Status: Image is up to date' in final_status:
                yield {"status": "当前已是最新版本。"}
                yield {"status": "无需更新。", "event": "DONE"}
                return
            
            if 'errorDetail' in last_line:
                error_msg = f"拉取镜像失败: {last_line['errorDetail']['message']}"
                yield {"status": error_msg, "event": "ERROR"}
                return

        except Exception as e:
            yield {"status": f"拉取镜像过程中发生异常: {e}", "event": "ERROR"}
            return

        # --- 核心：召唤并启动“更新器容器” ---
        yield {"status": "镜像拉取完成，准备应用更新..."}

        try:
            updater_image = "containrrr/watchtower"
            
            # 确保 watchtower 镜像存在
            try:
                client.images.get(updater_image)
            except docker.errors.ImageNotFound:
                yield {"status": f"正在拉取更新器工具: {updater_image}..."}
                client.images.pull(updater_image)

            # Watchtower 命令：清理旧镜像，只运行一次，指定容器名
            command = ["--cleanup", "--run-once", container_name]

            yield {"status": f"正在启动 Watchtower 更新容器 '{container_name}'..."}
            
            client.containers.run(
                image=updater_image,
                command=command,
                remove=True,
                detach=True,
                volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}}
            )
            
            yield {"status": "更新指令已发送！本容器即将重启...", "event": "RESTARTING"}
            yield {"status": "更新任务已成功交接给临时更新器。", "event": "DONE"}

        except docker.errors.NotFound:
            yield {"status": f"错误：找不到名为 '{container_name}' 的容器来更新。", "event": "ERROR"}
        except Exception as e_updater:
            yield {"status": f"错误：启动临时更新器时失败: {e_updater}", "event": "ERROR"}

    except Exception as e:
        yield {"status": f"更新过程中发生未知错误: {str(e)}", "event": "ERROR"}
    finally:
        # 恢复环境变量
        os.environ.clear()
        os.environ.update(old_env)

def task_check_and_update_container(processor):
    """
    【后台任务版】检查并更新容器。
    此函数适配 task_manager 的日志和进度更新方式。
    """
    container_name = processor.config.get('container_name', 'emby-toolkit')
    image_name_tag = processor.config.get('docker_image_name', 'hbq0405/emby-toolkit:latest')
    logger.trace(f"--- 开始执行系统更新检查 (容器: {container_name}) ---")
    task_manager.update_status_from_thread(0, "准备检查更新...")

    # 调用生成器，消费消息并转换为日志
    generator = _update_process_generator(container_name, image_name_tag)

    try:
        for event in generator:
            msg = event.get('status', '')
            evt_type = event.get('event')
            
            if evt_type == 'ERROR':
                logger.error(f"  🚫 {msg}")
                task_manager.update_status_from_thread(-1, f"更新失败: {msg}")
                return
            
            logger.info(f"  ➜ {msg}")
            
            # 简单的进度模拟
            if "拉取" in msg:
                task_manager.update_status_from_thread(30, msg)
            elif "应用更新" in msg:
                task_manager.update_status_from_thread(80, msg)
            elif "无需更新" in msg:
                task_manager.update_status_from_thread(100, "已是最新版本")
            
            if evt_type == 'RESTARTING':
                logger.warning("  ➜ 系统即将重启以应用更新...")
                task_manager.update_status_from_thread(100, "系统正在重启...")
                # 给一点时间让日志写完
                time.sleep(3)
                
    except Exception as e:
        logger.error(f"更新任务异常: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, "任务异常")