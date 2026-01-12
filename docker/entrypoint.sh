#!/bin/bash
# shellcheck shell=bash
# shellcheck disable=SC2016
# shellcheck disable=SC2155

Green="\033[32m"
Red="\033[31m"
Yellow='\033[33m'
Font="\033[0m"
INFO="[${Green}INFO${Font}]"
ERROR="[${Red}ERROR${Font}]"
WARN="[${Yellow}WARN${Font}]"
function INFO() {
    echo -e "${INFO} ${1}"
}
function ERROR() {
    echo -e "${ERROR} ${1}"
}
function WARN() {
    echo -e "${WARN} ${1}"
}

# 校正设置目录
CONFIG_DIR="${CONFIG_DIR:-/config}"

# 1. 设置用户和权限 (这部分不变)
INFO "→ 设置用户权限..."
groupmod -o -g "${PGID}" embytoolkit
usermod -o -u "${PUID}" embytoolkit
INFO "→ 快速设置持久化目录权限..."
chown embytoolkit:embytoolkit "${HOME}" "${CONFIG_DIR}"
if [ -d "${CONFIG_DIR}" ]; then
    find "${CONFIG_DIR}" -maxdepth 1 -mindepth 1 -exec chown embytoolkit:embytoolkit {} +
fi
umask "${UMASK}"

# 2. 生成 Nginx 配置文件
INFO "→ 生成 Nginx 配置文件..."
# ★★★ 核心修正 ★★★
# 移除 gosu，让此命令以 root 身份运行，这样它才有权限写入 /etc/nginx 目录
python3 /app/web_app.py generate-nginx-config
INFO "→ Nginx 配置文件生成完毕。"

# 3. 检查是否存在 docker.sock
if [ -S "/var/run/docker.sock" ]; then
    INFO "→ 检测到 Docker Socket，正在调整权限以允许非 Root 用户访问..."
    # 方法 A (推荐): 修改 Socket 组为当前 PGID (更安全)
    # chown root:${PGID} /var/run/docker.sock
    
    # 方法 B (暴力但最有效): 允许所有人读写 Socket (模拟 MP 的兼容性)
    # 因为这是在容器内部，只影响容器内映射的那个文件句柄，风险可控
    chmod 666 /var/run/docker.sock
fi

# 4. 启动 Nginx 服务 (修改了这里)
# 检查生成的配置文件，如果包含禁用标记，则不启动 Nginx
if grep -q "# Proxy disabled" /etc/nginx/conf.d/default.conf; then
    INFO "→ 检测到反向代理未启用，跳过启动 Nginx 服务。"
else
    INFO "→ 在后台启动 Nginx 服务..."
    nginx -g "daemon off;" &
fi

# 5. 启动主应用
# 在这里，我们才使用 gosu 将权限降级为普通用户，以保证应用运行时的安全
INFO "→ 启动 Emby Toolkit 主应用服务..."
if [ "${PUID}" -eq 0 ]; then
    # --- Root 模式 ---
    INFO "→ 检测到 PUID=0，以原生 Root 身份运行 (特权模式)..."
    # 直接运行，不使用 gosu，拥有最高权限，无视 docker.sock 权限问题
    exec dumb-init python3 /app/web_app.py
else
    # --- 普通用户模式 (LinuxServer 风格) ---
    INFO "→ 以普通用户 (UID:${PUID}, GID:${PGID}) 运行..."
    # 使用 gosu 降级身份
    exec dumb-init gosu embytoolkit:embytoolkit python3 /app/web_app.py
fi