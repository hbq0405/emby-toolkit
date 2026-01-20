# --- 阶段 1: 构建前端 ---
FROM node:20-alpine AS frontend-build
WORKDIR /app/emby-actor-ui
COPY emby-actor-ui/package*.json ./
RUN npm cache clean --force && \
    npm install --no-fund --verbose --legacy-peer-deps
COPY emby-actor-ui/ ./
RUN npm run build

# --- 阶段 2: 构建最终的生产镜像 (★ 优化后 ★) ---
FROM python:3.12-slim

ENV LANG="C.UTF-8" \
    TZ="Asia/Shanghai" \
    HOME="/embytoolkit" \
    CONFIG_DIR="/config" \
    APP_DATA_DIR="/config" \
    TERM="xterm" \
    PUID=0 \
    PGID=0 \
    UMASK=000

WORKDIR /app

# 1. 安装系统依赖 (移除 upgrade, 增加 --no-install-recommends 减小体积)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nginx \
        nodejs \
        gettext-base \
        locales \
        procps \
        gosu \
        bash \
        wget \
        curl \
        dumb-init && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ★★★ 核心优化点 1：把最稳定、最耗时的依赖安装提前 ★★★
# 只要 requirements.txt 文件内容不变，下面这两层就会被永久缓存！
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ★★★ 核心优化点 2：把最常变化的应用代码放在后面拷贝 ★★★
# 这样，你每次修改代码，只会让这一层和后面的缓存失效。
COPY web_app.py \
     core_processor.py \
     utils.py \
     logger_setup.py \
     constants.py \
     ai_translator.py \
     watchlist_processor.py \
     actor_utils.py \
     actor_subscription_processor.py \
     config_manager.py \
     task_manager.py \
     extensions.py \
     scheduler_manager.py \
     reverse_proxy.py \
     monitor_service.py \
     ./

COPY handler/ ./handler/
COPY database/ ./database/
COPY tasks/ ./tasks/
COPY fonts/ ./fonts/
COPY services/ ./services/
COPY routes/ ./routes/
COPY templates/ ./templates/
COPY docker/entrypoint.sh /entrypoint.sh

# 从前端构建阶段拷贝编译好的静态文件
COPY --from=frontend-build /app/emby-actor-ui/dist/. /app/static/

# 设置权限和用户 (这部分不变)
RUN chmod +x /entrypoint.sh && \
    mkdir -p ${HOME} && \
    groupadd -r embytoolkit -g 918 && \
    useradd -r embytoolkit -g embytoolkit -d ${HOME} -s /bin/bash -u 918

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -f http://localhost:5257/api/health || exit 1    

VOLUME [ "${CONFIG_DIR}" ]
EXPOSE 5257 8097 
ENTRYPOINT [ "/entrypoint.sh" ]
