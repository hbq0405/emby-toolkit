# --- 阶段 1: 构建前端 (保持不变，已经很好了) ---
FROM node:20-alpine AS frontend-build
WORKDIR /app
COPY emby-actor-ui/package*.json ./emby-actor-ui/
WORKDIR /app/emby-actor-ui
# 增加 --prefer-offline 可以利用缓存，加快重复构建速度
RUN npm install --prefer-offline --no-fund
COPY emby-actor-ui/ ./
RUN npm run build

# --- 阶段 2: 构建最终的生产镜像 ---
# 使用更具体的版本，如 python:3.11.9-slim，可以保证构建的可复现性
FROM python:3.11-slim

# 设置一些有用的环境变量
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ★★★ 1. 接收 PUID/PGID，并设置默认值 ★★★
ARG PUID=1000
ARG PGID=1000 # 通常 PUID 和 PGID 设置为相同的值，如 1000

# 设置工作目录
WORKDIR /app

# ★★★ 2. 安装系统依赖 (优化版) ★★★
# 我们不再需要 nodejs，因为前端已经编译好了
# 我们只需要一个干净的 Python 环境和一些基础工具
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tini \
        nodejs \
        npm \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝后端所有源码
# 使用 .dockerignore 文件来排除不必要的文件会更优雅
COPY . .

# 从前端构建阶段拷贝编译好的静态文件
# 确保目标目录存在
RUN mkdir -p /app/static
COPY --from=frontend-build /app/emby-actor-ui/dist /app/static

# ★★★ 3. 添加并设置入口脚本 (为“一键更新”做准备) ★★★
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ★★★ 4. 创建用户和组 (更简洁、更健壮的修复方案) ★★★
# 这个命令会智能地处理已存在的情况，无需手动删除
RUN groupadd -g ${PGID} myuser || groupmod -g ${PGID} myuser && \
    useradd -u ${PUID} -g myuser -s /bin/sh -m myuser || usermod -u ${PUID} myuser

# 声明 /config 目录为卷，用于持久化数据
VOLUME /config

# 确保新创建的用户对应用目录和持久化目录有所有权
# 注意：/config 目录在容器运行时才被创建和挂载，
# 所以我们在这里不能 chown 它。这应该在 entrypoint.sh 中处理。
RUN chown -R myuser:myuser /app

# 切换到这个新创建的非 root 用户
USER myuser

# 暴露端口
EXPOSE 5257 

# ★★★ 5. 使用 tini 和入口脚本来启动应用 ★★★
# tini 是一个轻量级的 init 系统，能更好地处理信号和僵尸进程
# entrypoint.sh 会负责检查更新和最终启动 Python 应用
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]