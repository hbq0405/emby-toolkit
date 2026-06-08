import json
import logging
import os
import time
import posixpath

import docker

import config_manager
import constants
import handler.github as github
import task_manager

logger = logging.getLogger(__name__)

DEFAULT_CONTAINER_NAME = 'emby-toolkit'
DEFAULT_IMAGE_NAME_TAG = 'hbq0405/emby-toolkit:latest'
DEFAULT_UPDATE_STRATEGY = 'docker_helper'
DEFAULT_HELPER_IMAGE = 'hbq0405/emby-toolkit:latest'
UPDATE_STATUS_FILE = 'system_update_result.json'
DOCKER_HELPER_LABELS = {
    'com.embytoolkit.role': 'system-update-helper',
    'com.embytoolkit.target-container': DEFAULT_CONTAINER_NAME,
}

DOCKER_HELPER_SCRIPT = r"""
import json
import os
import sys
import time

import docker

STATUS_PATH = os.environ["ETK_UPDATE_STATUS_PATH"]
TARGET_CONTAINER = os.environ["ETK_TARGET_CONTAINER"]
TARGET_IMAGE = os.environ["ETK_TARGET_IMAGE"]
REQUESTED_BY = os.environ.get("ETK_REQUESTED_BY", "")
REQUEST_SOURCE = os.environ.get("ETK_REQUEST_SOURCE", "system-auto-update")
CURRENT_VERSION = os.environ.get("ETK_CURRENT_VERSION", "")
TARGET_VERSION = os.environ.get("ETK_TARGET_VERSION", "")


def write_status(payload):
    payload = dict(payload or {})
    payload.setdefault("container_name", TARGET_CONTAINER)
    payload.setdefault("image_name", TARGET_IMAGE)
    payload.setdefault("requested_by", REQUESTED_BY)
    payload.setdefault("request_source", REQUEST_SOURCE)
    payload.setdefault("current_version", CURRENT_VERSION)
    payload.setdefault("target_version", TARGET_VERSION)
    payload.setdefault("timestamp", int(time.time()))
    tmp_path = STATUS_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp_path, STATUS_PATH)


def build_binds(host_config):
    binds = {}
    for item in host_config.get("Binds") or []:
        spec = str(item or "").strip()
        if not spec:
            continue
        parts = spec.split(":")
        if len(parts) >= 3:
            source = parts[0]
            bind = parts[1]
            mode = ":".join(parts[2:])
        elif len(parts) == 2:
            source, bind = parts
            mode = "rw"
        else:
            continue
        entry = {"bind": bind, "mode": mode}
        if "," in mode:
            segments = mode.split(",")
            entry["mode"] = segments[0] or "rw"
            if len(segments) > 1 and segments[1]:
                entry["propagation"] = segments[1]
        binds[source] = entry
    return binds


def build_port_bindings(host_config):
    bindings = host_config.get("PortBindings") or {}
    result = {}
    for container_port, entries in bindings.items():
        if not entries:
            result[container_port] = None
            continue
        normalized = []
        for item in entries:
            host_ip = item.get("HostIp") or ""
            host_port = item.get("HostPort") or ""
            if host_ip and host_port:
                normalized.append((host_ip, int(host_port)))
            elif host_ip:
                normalized.append((host_ip,))
            elif host_port:
                normalized.append(int(host_port))
            else:
                normalized.append(None)
        result[container_port] = normalized[0] if len(normalized) == 1 else normalized
    return result


def build_restart_policy(host_config):
    policy = host_config.get("RestartPolicy") or {}
    name = policy.get("Name")
    if not name:
        return None
    return {
        "Name": name,
        "MaximumRetryCount": int(policy.get("MaximumRetryCount") or 0),
    }


def build_networking(client, container_attrs):
    host_config = container_attrs.get("HostConfig") or {}
    network_mode = host_config.get("NetworkMode") or "default"
    network_settings = (container_attrs.get("NetworkSettings") or {}).get("Networks") or {}
    networking_config = None
    endpoint_network_mode = (network_mode or "").split(":", 1)[0]

    if endpoint_network_mode not in {"host", "none", "container"} and network_settings:
        endpoints = {}
        for network_name, network_data in network_settings.items():
            endpoint_kwargs = {}
            aliases = list(network_data.get("Aliases") or [])
            if aliases:
                endpoint_kwargs["aliases"] = aliases
            is_user_defined = bool(network_data.get("NetworkID")) and network_name not in {"bridge", "host", "none"}
            ipv4_address = (network_data.get("IPAMConfig") or {}).get("IPv4Address")
            if not ipv4_address and is_user_defined:
                ipv4_address = network_data.get("IPAddress")
            if ipv4_address:
                endpoint_kwargs["ipv4_address"] = ipv4_address
            ipv6_address = (network_data.get("IPAMConfig") or {}).get("IPv6Address")
            if not ipv6_address and is_user_defined:
                ipv6_address = network_data.get("GlobalIPv6Address")
            if ipv6_address:
                endpoint_kwargs["ipv6_address"] = ipv6_address
            endpoints[network_name] = client.api.create_endpoint_config(**endpoint_kwargs)
        if endpoints:
            networking_config = client.api.create_networking_config(endpoints)

    return network_mode, networking_config


def collect_create_kwargs(client, container):
    attrs = container.attrs or {}
    config = attrs.get("Config") or {}
    host_config = attrs.get("HostConfig") or {}

    ports = list((config.get("ExposedPorts") or {}).keys()) or None
    volumes = list((config.get("Volumes") or {}).keys()) or None
    binds = build_binds(host_config)
    port_bindings = build_port_bindings(host_config)
    restart_policy = build_restart_policy(host_config)
    network_mode, networking_config = build_networking(client, attrs)
    network_mode_kind = (network_mode or "").split(":", 1)[0]
    if network_mode_kind in {"host", "container"}:
        port_bindings = None
        ports = None
    healthcheck = config.get("Healthcheck")
    host_cfg = client.api.create_host_config(
        auto_remove=bool(host_config.get("AutoRemove")),
        binds=binds or None,
        cap_add=host_config.get("CapAdd"),
        cap_drop=host_config.get("CapDrop"),
        dns=host_config.get("Dns"),
        dns_opt=host_config.get("DnsOptions"),
        dns_search=host_config.get("DnsSearch"),
        extra_hosts=host_config.get("ExtraHosts"),
        group_add=host_config.get("GroupAdd"),
        ipc_mode=host_config.get("IpcMode"),
        log_config=host_config.get("LogConfig"),
        network_mode=network_mode,
        pid_mode=host_config.get("PidMode"),
        pids_limit=host_config.get("PidsLimit"),
        port_bindings=port_bindings or None,
        privileged=bool(host_config.get("Privileged")),
        publish_all_ports=bool(host_config.get("PublishAllPorts")),
        read_only=bool(host_config.get("ReadonlyRootfs")),
        restart_policy=restart_policy,
        runtime=host_config.get("Runtime"),
        security_opt=host_config.get("SecurityOpt"),
        shm_size=host_config.get("ShmSize"),
        sysctls=host_config.get("Sysctls"),
        ulimits=host_config.get("Ulimits"),
    )

    return {
        "image": TARGET_IMAGE,
        "command": config.get("Cmd"),
        "hostname": config.get("Hostname") or None,
        "user": config.get("User") or None,
        "detach": True,
        "stdin_open": bool(config.get("OpenStdin")),
        "tty": bool(config.get("Tty")),
        "ports": ports,
        "environment": list(config.get("Env") or []),
        "volumes": volumes,
        "name": container.name,
        "entrypoint": config.get("Entrypoint"),
        "working_dir": config.get("WorkingDir") or None,
        "domainname": config.get("Domainname") or None,
        "labels": dict(config.get("Labels") or {}),
        "host_config": host_cfg,
        "networking_config": networking_config,
        "healthcheck": healthcheck,
        "stop_timeout": int(config.get("StopTimeout") or 10),
    }


def wait_for_health(container, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        container.reload()
        state = container.attrs.get("State") or {}
        health = state.get("Health") or {}
        status = health.get("Status")
        if status in {None, ""}:
            return True, "容器已启动（未配置健康检查）。"
        if status == "healthy":
            return True, "容器健康检查通过。"
        if status == "unhealthy":
            return False, "新容器健康检查失败。"
        time.sleep(2)
    return False, "等待新容器健康检查超时。"


def main():
    client = docker.from_env()
    try:
        container = client.containers.get(TARGET_CONTAINER)
    except Exception as exc:
        write_status({"ok": False, "updated": False, "message": f"helper 找不到目标容器: {exc}", "status": "error"})
        return 1

    old_container_id = container.id
    old_image_id = (container.attrs.get("Image") or "") if container.attrs else ""
    create_kwargs = collect_create_kwargs(client, container)
    backup_name = f"{container.name}-backup-{int(time.time())}"

    try:
        container.stop(timeout=int((container.attrs.get("Config") or {}).get("StopTimeout") or 10))
        container.rename(backup_name)
        response = client.api.create_container(**create_kwargs)
        new_container_id = response.get("Id")
        if not new_container_id:
            raise RuntimeError("Docker API 未返回新容器 ID。")
        client.api.start(new_container_id)
        new_container = client.containers.get(new_container_id)
        healthy, message = wait_for_health(new_container)
        if not healthy:
            raise RuntimeError(message)

        backup_container = client.containers.get(backup_name)
        backup_container.remove(force=True)
        new_container.reload()
        write_status({
            "ok": True,
            "updated": True,
            "message": message,
            "status": "updated",
            "old_container_id": old_container_id,
            "new_container_id": new_container.id,
            "old_image_id": old_image_id,
            "new_image_id": new_container.attrs.get("Image") or "",
        })
        return 0
    except Exception as exc:
        try:
            current = None
            try:
                current = client.containers.get(TARGET_CONTAINER)
            except Exception:
                current = None

            if current is not None and current.id != old_container_id:
                current.remove(force=True)

            backup_container = client.containers.get(backup_name)
            backup_container.rename(TARGET_CONTAINER)
            backup_container.start()
        except Exception as rollback_exc:
            write_status({
                "ok": False,
                "updated": False,
                "message": f"更新失败且回滚失败: {exc}; rollback={rollback_exc}",
                "status": "rollback_failed",
            })
            return 1

        write_status({
            "ok": False,
            "updated": False,
            "message": f"helper 更新失败，已回滚: {exc}",
            "status": "rolled_back",
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
"""


def _clean_version_text(value, default=None):
    text = str(value or '').strip()
    return text or default


def _log_trace(message):
    trace_logger = getattr(logger, 'trace', None)
    if callable(trace_logger):
        trace_logger(message)
        return
    logger.debug(message)


def _read_json_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning(f"读取 JSON 文件失败: {path}, err={e}")
        return None


def _write_json_file(path, payload):
    temp_path = f"{path}.tmp"
    with open(temp_path, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(temp_path, path)


def _get_update_status_path():
    persistent_root = getattr(config_manager, 'PERSISTENT_DATA_PATH', None)
    if persistent_root:
        return os.path.join(persistent_root, UPDATE_STATUS_FILE)
    app_data_dir = _clean_version_text(os.environ.get('APP_DATA_DIR'))
    if app_data_dir:
        return os.path.join(app_data_dir, UPDATE_STATUS_FILE)
    return os.path.join(os.getcwd(), UPDATE_STATUS_FILE)


def consume_post_update_status():
    status_path = _get_update_status_path()
    payload = _read_json_file(status_path)
    if not payload:
        return None
    try:
        os.remove(status_path)
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"删除更新状态文件失败: {status_path}, err={e}")
    return payload


def peek_post_update_status():
    return _read_json_file(_get_update_status_path())


def clear_post_update_status():
    status_path = _get_update_status_path()
    try:
        os.remove(status_path)
    except FileNotFoundError:
        return False
    except Exception as e:
        logger.warning(f"删除更新状态文件失败: {status_path}, err={e}")
        return False
    return True


def _normalize_container_path(path_value):
    path_text = _clean_version_text(path_value)
    if not path_text:
        return None
    normalized = posixpath.normpath(path_text)
    if not normalized.startswith('/'):
        normalized = f"/{normalized.lstrip('/')}"
    return normalized


def _is_container_subpath(path_value, parent_path):
    if path_value == parent_path:
        return True
    return path_value.startswith(parent_path.rstrip('/') + '/')


def _resolve_helper_status_volume(container, status_path):
    normalized_status_path = _normalize_container_path(status_path)
    if not normalized_status_path:
        return None, "系统更新状态文件路径无效。"

    container_attrs = getattr(container, 'attrs', {}) or {}
    mounts = container_attrs.get('Mounts') or []
    matched_mount = None
    matched_destination = None

    for mount in mounts:
        destination = _normalize_container_path(mount.get('Destination') or mount.get('Target'))
        if not destination or not _is_container_subpath(normalized_status_path, destination):
            continue
        if matched_destination and len(destination) <= len(matched_destination):
            continue
        matched_mount = mount
        matched_destination = destination

    if not matched_mount or not matched_destination:
        return None, f"无法为状态文件 '{normalized_status_path}' 定位目标容器的持久化挂载。"

    mount_type = _clean_version_text(matched_mount.get('Type')).lower()
    if mount_type == 'tmpfs':
        return None, f"状态文件所在挂载 '{matched_destination}' 是 tmpfs，容器重启后无法保留更新结果。"

    source = None
    if mount_type == 'volume':
        source = _clean_version_text(matched_mount.get('Name') or matched_mount.get('Source'))
    else:
        source = _clean_version_text(matched_mount.get('Source') or matched_mount.get('Name'))

    if not source:
        return None, f"状态文件所在挂载 '{matched_destination}' 缺少可复用的挂载源。"

    mode = _clean_version_text(matched_mount.get('Mode'))
    if not mode:
        mode = 'rw' if matched_mount.get('RW', True) else 'ro'
    mode_tokens = [token.strip() for token in mode.split(',') if token.strip()]
    access_mode = 'rw'
    if 'ro' in mode_tokens or matched_mount.get('RW') is False:
        access_mode = 'ro'

    if access_mode != 'rw':
        return None, f"状态文件所在挂载 '{matched_destination}' 为只读，无法写入更新结果。"

    return {
        source: {'bind': matched_destination, 'mode': 'rw'},
    }, None


def _resolve_self_container_target(client):
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


def resolve_update_strategy(config_source=None):
    config_source = config_source or {}
    app_config = getattr(config_manager, 'APP_CONFIG', {}) or {}
    strategy = _clean_version_text(
        config_source.get('system_update_strategy')
        or app_config.get('system_update_strategy')
        or os.environ.get('SYSTEM_UPDATE_STRATEGY'),
        DEFAULT_UPDATE_STRATEGY,
    )
    helper_image = _clean_version_text(
        config_source.get('system_update_helper_image')
        or app_config.get('system_update_helper_image')
        or os.environ.get('SYSTEM_UPDATE_HELPER_IMAGE'),
        DEFAULT_HELPER_IMAGE,
    )
    return {
        "strategy": strategy,
        "helper_image": helper_image,
    }


def get_system_update_version_info():
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


def _decode_container_output(output):
    if output is None:
        return ""
    if isinstance(output, (bytes, bytearray)):
        return output.decode('utf-8', errors='replace')
    if isinstance(output, str):
        return output
    if isinstance(output, (list, tuple)):
        return "\n".join(_decode_container_output(item) for item in output)
    return str(output)


def _read_image_label_version(image):
    labels = ((getattr(image, 'attrs', {}) or {}).get('Config') or {}).get('Labels') or {}
    return _clean_version_text(labels.get('org.opencontainers.image.version'))


def _build_helper_environment(status_path, container_name, image_name_tag, version_info):
    return {
        "ETK_UPDATE_STATUS_PATH": status_path,
        "ETK_TARGET_CONTAINER": container_name,
        "ETK_TARGET_IMAGE": image_name_tag,
        "ETK_REQUESTED_BY": "tg-or-web",
        "ETK_REQUEST_SOURCE": "system-auto-update",
        "ETK_CURRENT_VERSION": _clean_version_text(version_info.get('current_version')),
        "ETK_TARGET_VERSION": _clean_version_text(version_info.get('target_version')),
    }


def _build_helper_labels(container_name):
    labels = dict(DOCKER_HELPER_LABELS)
    labels['com.embytoolkit.target-container'] = _clean_version_text(container_name, DEFAULT_CONTAINER_NAME)
    return labels


def _ensure_helper_image(client, helper_image, version_info):
    try:
        client.images.get(helper_image)
    except docker.errors.ImageNotFound:
        yield {"status": f"正在拉取更新器工具: {helper_image}...", "current_version": version_info.get('current_version'), "target_version": version_info.get('target_version')}
        client.images.pull(helper_image)


def _run_docker_helper(client, helper_image, container_name, image_name_tag, version_info):
    status_path = _get_update_status_path()
    persistent_root = os.path.dirname(status_path)
    os.makedirs(persistent_root, exist_ok=True)
    try:
        os.remove(status_path)
    except FileNotFoundError:
        pass

    for event in _ensure_helper_image(client, helper_image, version_info):
        yield event

    try:
        target_container = client.containers.get(container_name)
    except Exception as e:
        yield {
            "status": f"启动 Docker Helper 失败: 无法定位目标容器 '{container_name}': {e}",
            "event": "ERROR",
            "current_version": version_info.get('current_version'),
            "target_version": version_info.get('target_version'),
        }
        return

    helper_status_volume, volume_error = _resolve_helper_status_volume(target_container, status_path)
    if volume_error:
        yield {
            "status": f"启动 Docker Helper 失败: {volume_error}",
            "event": "ERROR",
            "current_version": version_info.get('current_version'),
            "target_version": version_info.get('target_version'),
        }
        return

    env = _build_helper_environment(status_path, container_name, image_name_tag, version_info)
    volumes = {
        '/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'},
    }
    volumes.update(helper_status_volume)

    yield {
        "status": f"正在启动 Docker Helper 更新容器 '{container_name}'...",
        "current_version": version_info.get('current_version'),
        "target_version": version_info.get('target_version'),
    }

    try:
        output = client.containers.run(
            image=helper_image,
            entrypoint=["python", "-c", DOCKER_HELPER_SCRIPT],
            detach=False,
            auto_remove=True,
            environment=env,
            volumes=volumes,
            labels=_build_helper_labels(container_name),
        )
        logger.debug(f"Docker helper 输出: {_decode_container_output(output)}")
    except Exception as e:
        yield {
            "status": f"启动 Docker Helper 失败: {e}",
            "event": "ERROR",
            "current_version": version_info.get('current_version'),
            "target_version": version_info.get('target_version'),
        }
        return

    status_payload = _read_json_file(status_path)
    if not status_payload:
        yield {
            "status": "Docker Helper 已退出，但未写入更新结果。",
            "event": "ERROR",
            "current_version": version_info.get('current_version'),
            "target_version": version_info.get('target_version'),
        }
        return

    if not status_payload.get('ok'):
        yield {
            "status": str(status_payload.get('message') or 'Docker Helper 更新失败'),
            "event": "ERROR",
            "current_version": _clean_version_text(status_payload.get('current_version'), version_info.get('current_version')),
            "target_version": _clean_version_text(status_payload.get('target_version'), version_info.get('target_version')),
        }
        return

    yield {
        "status": "更新任务已交接给 Docker Helper，新容器启动后将补发最终结果通知。",
        "event": "RESTARTING",
        "updated": True,
        "current_version": _clean_version_text(status_payload.get('current_version'), version_info.get('current_version')),
        "target_version": _clean_version_text(status_payload.get('target_version'), version_info.get('target_version')),
    }


def _run_watchtower(client, container_name, version_info):
    updater_image = "containrrr/watchtower"
    try:
        client.images.get(updater_image)
    except docker.errors.ImageNotFound:
        yield {"status": f"正在拉取更新器工具: {updater_image}...", "current_version": version_info.get('current_version'), "target_version": version_info.get('target_version')}
        client.images.pull(updater_image)

    command = ["--cleanup", "--run-once", container_name]
    yield {
        "status": f"正在启动 Watchtower 更新容器 '{container_name}'...",
        "current_version": version_info.get('current_version'),
        "target_version": version_info.get('target_version'),
    }
    try:
        client.containers.run(
            image=updater_image,
            command=command,
            remove=True,
            detach=True,
            volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}},
        )
    except Exception as e:
        yield {
            "status": f"错误：启动 Watchtower 更新器失败: {e}",
            "event": "ERROR",
            "current_version": version_info.get('current_version'),
            "target_version": version_info.get('target_version'),
        }
        return

    yield {
        "status": "更新指令已发送！本容器即将重启...",
        "event": "RESTARTING",
        "current_version": version_info.get('current_version'),
        "target_version": version_info.get('target_version'),
    }
    yield {
        "status": "更新任务已成功交接给 Watchtower。",
        "event": "DONE",
        "updated": True,
        "current_version": version_info.get('current_version'),
        "target_version": version_info.get('target_version'),
    }


def _update_process_generator(container_name, image_name_tag, strategy=None, helper_image=None):
    client = None
    proxies_config = config_manager.get_proxies_for_requests()
    old_env = os.environ.copy()
    version_info = get_system_update_version_info()
    current_version = version_info.get('current_version')
    target_version = version_info.get('target_version')
    strategy_name = _clean_version_text(strategy, DEFAULT_UPDATE_STRATEGY)
    helper_image_name = _clean_version_text(helper_image, DEFAULT_HELPER_IMAGE)
    try:
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
        if not current_version:
            current_version = _read_image_label_version(getattr(target_container, 'image', None)) or current_version
            version_info['current_version'] = current_version

        yield {"status": f"正在检查并拉取最新镜像: {image_name_tag}...", "current_version": current_version, "target_version": target_version}

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

            version_info['target_version'] = _read_image_label_version(latest_image) or target_version

            if current_image_id and current_image_id == latest_image_id:
                final_msg = "当前容器已运行最新镜像，无需更新。"
                if last_status:
                    final_msg = f"{final_msg} Docker 返回: {last_status}"
                yield {"status": final_msg, "event": "NO_UPDATE", "current_version": current_version, "target_version": version_info.get('target_version')}
                return

        except Exception as e:
            yield {"status": f"拉取镜像过程中发生异常: {e}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
            return

        yield {
            "status": f"镜像拉取完成，准备通过 `{strategy_name}` 应用更新...",
            "current_version": current_version,
            "target_version": version_info.get('target_version'),
        }

        if strategy_name == 'watchtower':
            for event in _run_watchtower(client, container_name, version_info):
                yield event
            return

        if strategy_name != 'docker_helper':
            yield {
                "status": f"未支持的系统更新策略: {strategy_name}",
                "event": "ERROR",
                "current_version": current_version,
                "target_version": version_info.get('target_version'),
            }
            return

        for event in _run_docker_helper(client, helper_image_name, container_name, image_name_tag, version_info):
            yield event

    except Exception as e:
        yield {"status": f"更新过程中发生未知错误: {str(e)}", "event": "ERROR", "current_version": current_version, "target_version": target_version}
    finally:
        os.environ.clear()
        os.environ.update(old_env)


def task_check_and_update_container(processor):
    update_target = resolve_update_target(getattr(processor, 'config', {}) or {})
    strategy_info = resolve_update_strategy(getattr(processor, 'config', {}) or {})
    container_name = update_target['container_name']
    image_name_tag = update_target['docker_image_name']
    strategy_name = strategy_info['strategy']
    helper_image = strategy_info['helper_image']
    _log_trace(f"--- 开始执行系统更新检查 (容器: {container_name}, 策略: {strategy_name}) ---")
    task_manager.update_status_from_thread(0, "准备检查更新...")
    result = {
        "ok": False,
        "updated": False,
        "message": "",
        "strategy": strategy_name,
        "helper_image": helper_image,
        **get_system_update_version_info(),
    }

    generator = _update_process_generator(container_name, image_name_tag, strategy=strategy_name, helper_image=helper_image)

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

            if "拉取" in msg:
                task_manager.update_status_from_thread(30, msg)
            elif "应用更新" in msg or "Docker Helper" in msg or "Watchtower" in msg:
                task_manager.update_status_from_thread(80, msg)
            elif evt_type == 'NO_UPDATE':
                task_manager.update_status_from_thread(100, "已是最新版本")
                result.update({"ok": True, "updated": False, "message": msg, "status": "up_to_date"})

            if evt_type == 'RESTARTING':
                logger.warning("  ➜ 系统即将重启以应用更新...")
                task_manager.update_status_from_thread(100, "系统正在重启...")
                result.update({"ok": True, "updated": True, "message": msg, "status": "restarting"})
                time.sleep(1)
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
