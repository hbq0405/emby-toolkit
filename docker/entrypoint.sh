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

CONFIG_DIR="${CONFIG_DIR:-/config}"

# 1. Set runtime user and persistent directory ownership.
INFO "Setting user permissions..."
groupmod -o -g "${PGID}" embytoolkit
usermod -o -u "${PUID}" embytoolkit
INFO "Setting persistent directory permissions..."
chown embytoolkit:embytoolkit "${HOME}" "${CONFIG_DIR}"
if [ -d "${CONFIG_DIR}" ]; then
    find "${CONFIG_DIR}" -maxdepth 1 -mindepth 1 -exec chown embytoolkit:embytoolkit {} +
fi
umask "${UMASK}"

# 2. Generate nginx config as root because it writes under /etc/nginx.
INFO "Generating Nginx config..."
python3 /app/web_app.py generate-nginx-config
INFO "Nginx config generated."

# Dependencies may create HOME cache files while the root-only nginx config
# command imports application modules. Hand ownership back before the main app
# drops privileges, otherwise the runtime user cannot create file locks there.
P115_CACHE_DIR="${HOME}/.p115client.cache.d"
mkdir -p "${P115_CACHE_DIR}"
chown -R embytoolkit:embytoolkit "${HOME}"

# 3. Allow the non-root runtime user to access the mounted Docker socket.
if [ -S "/var/run/docker.sock" ]; then
    INFO "Detected Docker socket, adjusting permissions..."
    chmod 666 /var/run/docker.sock
fi

# 4. Start nginx unless proxy is disabled.
if grep -q "# Proxy disabled" /etc/nginx/conf.d/default.conf; then
    INFO "Reverse proxy disabled, skipping Nginx startup."
else
    INFO "Starting Nginx in background..."
    nginx -g "daemon off;" &
fi

# 5. Start the main application.
INFO "Starting Emby Toolkit..."
if [ "${PUID}" -eq 0 ]; then
    INFO "PUID=0 detected, running as root."
    exec dumb-init python3 /app/web_app.py
else
    INFO "Running as embytoolkit (UID:${PUID}, GID:${PGID})."
    exec dumb-init gosu embytoolkit:embytoolkit python3 /app/web_app.py
fi
