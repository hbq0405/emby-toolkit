#!/bin/bash
set -e

# --- 权限设置 (以 root 身份执行) ---
echo ">>> Entrypoint script started as root."

# 1. 动态地将 myuser 加入到 docker 组
#    检查 docker.sock 是否被挂载进来
if [ -S /var/run/docker.sock ]; then
    # 获取 docker.sock 的 GID (Group ID)
    DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)
    
    # 在容器内创建一个名为 'docker' 的组，并使用相同的 GID
    # -f, --force: 如果组已存在，就什么都不做，避免错误
    groupadd -f -g "$DOCKER_GID" docker
    
    # 将我们的 myuser 用户追加到这个新的 'docker' 组中
    usermod -aG docker myuser
    
    echo ">>> User 'myuser' has been added to 'docker' group (GID: $DOCKER_GID) to access docker.sock."
fi

# 2. 确保持久化数据目录的所有权正确
#    $APP_DATA_DIR 的值由 docker run -e 传入，例如 "/config"
if [ -d "$APP_DATA_DIR" ]; then
    echo ">>> Ensuring ownership of $APP_DATA_DIR..."
    chown -R myuser:myuser "$APP_DATA_DIR"
fi


# --- 更新检查 (现在由 myuser 来执行，因为它有 docker 组权限了) ---
UPDATE_MARKER_FILE="$APP_DATA_DIR/.update_in_progress"
IMAGE_NAME="hbq0405/emby-actor-processor:latest"

# 使用 su-exec 或 gosu 来切换到 myuser 身份执行后续的检查和应用启动
# 这样做的好处是，即使是检查更新的逻辑，也尽可能用低权限用户执行
exec gosu myuser bash -c '
    set -e
    if [ -f "'"$UPDATE_MARKER_FILE"'" ]; then
        echo ">>> [as myuser] Update marker file found. Attempting to update the Docker image..."
        
        # 因为 myuser 现在在 docker 组里，所以它有权限执行 docker pull
        docker pull "'"$IMAGE_NAME"'"
        
        rm -f "'"$UPDATE_MARKER_FILE"'"
        echo ">>> [as myuser] Image updated successfully. Proceeding to start the application."
    else
        echo ">>> [as myuser] No update marker file found. Starting the application directly."
    fi

    echo ">>> [as myuser] Starting Emby Actor Processor..."
    # 最后的 exec，用 myuser 的身份启动 Python 应用
    exec python web_app.py
'