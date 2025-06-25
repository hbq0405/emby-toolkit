#!/bin/bash
set -e # 任何命令失败则立即退出

# 定义标记文件的路径，它应该在持久化数据目录中
UPDATE_MARKER_FILE="$APP_DATA_DIR/.update_in_progress"
IMAGE_NAME="hbq0405/emby-actor-processor:latest" # ★★★ 替换为您自己的镜像名 ★★★

# 检查更新标记文件是否存在
if [ -f "$UPDATE_MARKER_FILE" ]; then
    echo ">>> Update marker file found. Attempting to update the Docker image..."
    
    # 拉取最新的镜像
    docker pull "$IMAGE_NAME"
    
    # 删除标记文件，以便下次正常启动
    rm -f "$UPDATE_MARKER_FILE"
    
    echo ">>> Image updated successfully. Proceeding to start the application."
else
    echo ">>> No update marker file found. Starting the application directly."
fi

# 无论是否更新，最后都执行 Python 应用
# 使用 exec 可以让 Python 程序接管 PID 1，更好地处理信号
echo ">>> Starting Emby Actor Processor..."
exec python web_app.py