# --- 阶段 1: 构建前端 ---
FROM node:20-alpine AS frontend-build
WORKDIR /app/emby-actor-ui

# 复制前端源码并构建（合并多个操作到单个RUN层）
COPY emby-actor-ui/ ./
RUN npm cache clean --force && \
    npm install --no-fund --verbose --legacy-peer-deps && \
    npm run build

# --- 阶段 2: 构建最终的生产镜像 ---
FROM python:3.11-slim

# 设置环境变量
ENV LANG="C.UTF-8" \
    TZ="Asia/Shanghai" \
    HOME="/embyactor" \
    CONFIG_DIR="/config" \
    APP_DATA_DIR="/config" \
    TERM="xterm" \
    PUID=0 \
    PGID=0 \
    UMASK=000

WORKDIR /app

# 安装必要的系统依赖和 Node.js
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        nodejs \
        gettext-base \
        locales \
        procps \
        gosu \
        bash \
        wget \
        curl \
        dumb-init && \
    # 安装 docker-compose 官方二进制
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
        -o /usr/local/bin/docker-compose && \
    chmod +x /usr/local/bin/docker-compose && \
    apt-get clean && \
    rm -rf \
        /tmp/* \
        /var/lib/apt/lists/* \
        /var/tmp/*

# 复制所有应用文件（合并多个COPY操作）
COPY requirements.txt \
     web_app.py \
     core_processor.py \
     douban.py \
     tmdb_handler.py \
     emby_handler.py \
     utils.py \
     logger_setup.py \
     constants.py \
     web_parser.py \
     ai_translator.py \
     watchlist_processor.py \
     actor_sync_handler.py \
     actor_utils.py \
     actor_subscription_processor.py \
     moviepilot_handler.py \
     config_manager.py \
     task_manager.py \
     db_handler.py \
     extensions.py \
     tasks.py \
     github_handler.py \
     ./

COPY routes/ ./routes/
COPY templates/ ./templates/
COPY docker/entrypoint.sh /entrypoint.sh

# 从前端构建阶段拷贝编译好的静态文件
COPY --from=frontend-build /app/emby-actor-ui/dist/. /app/static/

# 安装Python依赖、设置权限、创建用户（合并多个RUN操作）
RUN pip install --no-cache-dir -r requirements.txt && \
    chmod +x /entrypoint.sh && \
    mkdir -p ${HOME} && \
    groupadd -r embyactor -g 918 && \
    useradd -r embyactor -g embyactor -d ${HOME} -s /bin/bash -u 918

# 声明 /config 目录为数据卷
VOLUME [ "${CONFIG_DIR}" ]

EXPOSE 5257

# 设置容器入口点
ENTRYPOINT [ "/entrypoint.sh" ]