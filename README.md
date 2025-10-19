# Emby ToolKit (Emby 增强管理工具)

[![GitHub stars](https://img.shields.io/github/stars/hbq0405/emby-toolkit.svg%sstyle=social&label=Star)](https://github.com/hbq0405/emby-toolkit)
[![GitHub license](https://img.shields.io/github/license/hbq0405/emby-toolkit.svg)](https://github.com/hbq0405/emby-toolkit/blob/main/LICENSE)
<!-- 你可以添加更多的徽章，例如构建状态、Docker Hub 拉取次数等 -->

一个用于处理和增强 Emby 媒体库的工具，包括但不限于演员/角色名称翻译、信息补全（从豆瓣、TMDb等）、合集检查及订阅、智能追剧、演员订阅、自建合集、虚拟库。

## ✨ 功能特性

*   **演员信息处理**：自动翻译演员名、角色名、优先使用豆瓣角色名，没有的调用ai在线翻译。
*   **外部数据源集成**：从豆瓣获取Tmdb缺失的演员，补充进演员表。
*   **定时任务**：支持定时自动化按设置的顺序执行后台任务。
*   **实时处理新入库资源**：自动处理Emby新入库资源，需配置webhook:http://ip:5257/webhook/emby 请求内容类型：application/json 勾选：【新媒体已添加】、【媒体删除】以及神医插件才有的【按剧集和专辑对通知进行分组】、【媒体元数据更新】、【媒体图像更新】。
*   **合集检查**：扫描库存合集，检查缺失并订阅（需配置MoviePilot）。
*   **智能追剧**：监控媒体库所有剧集，智能判断并更新状态，对缺失的季以及新出的季进行订阅操作（需配置MoviePilot）。
*   **演员订阅**：追踪喜欢的演员，按配置订阅过去以及将来的资源（需配置MoviePilot）。
*   **封面生成**：实时合成媒体库封面以及自建合集封面。
*   **自建合集**：按各种规则筛选自己喜欢的合集并虚拟成媒体库展示在首页（实验性功能，目前已知手机端支持不好）。


## 🚀 快速开始

### 先决条件

*   已安装 Docker 和 Docker Compose (推荐)。
*   一个 Emby 服务器。
*   TMDb API Key。

### Docker 部署 (推荐)

这是最简单和推荐的部署方式。

1.  **准备持久化数据目录**：
    在你的服务器上（例如 NAS）创建一个目录，用于存放应用的配置文件和数据库。例如：
    ```bash
    mkdir -p /path/emby-toolkit
    ```
    请将 `/path/emby-toolkit` 替换为你实际的路径。

2.  **使用 `docker-compose.yml` (推荐)**：
    创建一个 `docker-compose.yml` 文件，内容如下：

    ```yaml
    version: '3.8'

    services:
      # --- 1. Emby-Toolkit 主程序 ---
      emby-toolkit:
        image: hbq0405/emby-toolkit:latest 
        container_name: emby-toolkit
        network_mode: bridge                          # 网络模式
        ports:
          - "5257:5257"                               # 管理端口
          - "8097:8097"                               # 反代端口，虚拟库用，冒号前面是实际访问端口，冒号后面是管理后台设置的反代监听端口
        volumes:
          - /path/emby-toolkit:/config                # 将宿主机的数据目录挂载到容器的 /config 目录
          - /path/tmdb:/tmdb                          # 映射神医本地TMDB目录，非神医Pro用户可以留空
          - /var/run/docker.sock:/var/run/docker.sock # 一键更新用，不需要可以不配置
        environment:
          - APP_DATA_DIR=/config                      # 持久化目录
          - TZ=Asia/Shanghai                          # 设置容器时区
          - AUTH_USERNAME=admin                       # 用户名可任意设置，密码在程序首次运行会生成初始密码：password
          - PUID=1000                                 # 设置为你的用户ID，建议与宿主机用户ID保持一致
          - PGID=1000                                 # 设置为DOCKER组ID (一键更新用，‘grep docker /etc/group’可以查询)
          - UMASK=022                                 # 设置文件权限掩码，建议022
          - DB_HOST=172.17.0.1                        # 数据库服务的地址 
          - DB_PORT=5433                              # 数据库服务的端口 
          - DB_USER=embytoolkit                       # !!! (可选) 修改为你自己的数据库用户名
          - DB_PASSWORD=embytoolkit                   # !!! (必填) 请修改为一个强密码 !!!
          - DB_NAME=embytoolkit                       # !!! (可选) 修改为你自己的数据库名
          - CONTAINER_NAME=emby-toolkit               # 以下两项都是一键更新用，不需要可以不配置
          - DOCKER_IMAGE_NAME=hbq0405/emby-toolkit:latest
        restart: unless-stopped
        depends_on:                                   # 确保主程序只在数据库健康检查通过后才启动 
          db:
            condition: service_healthy
      # --- 2. PostgreSQL 数据库服务 ---
      db:
        image: postgres:16-alpine
        container_name: emby-toolkit-db
        restart: unless-stopped
        network_mode: bridge
        volumes:
          # 将数据库的持久化数据存储在名为 'postgres_data' 的Docker卷中
          # 这可以确保即使删除了容器，数据库数据也不会丢失
          - postgres_data:/var/lib/postgresql/data
        environment:
          # --- 数据库认证配置 (核心) ---
          # 这些值必须与上面的 'emby-toolkit' 服务中的环境变量完全匹配
          - POSTGRES_USER=embytoolkit               # !!! (可选) 修改，与上面保持一致
          - POSTGRES_PASSWORD=embytoolkit           # !!! (必填) 修改，与上面保持一致 !!!
          - POSTGRES_DB=embytoolkit                 # !!! (可选) 修改，与上面保持一致
        ports:
          # 将数据库端口映射到宿主机，方便使用Navicat等工具连接调试
          - "5433:5432"
        healthcheck:
          # 健康检查，确保数据库服务已准备好接受连接
          test: ["CMD-SHELL", "pg_isready -U embytoolkit -d embytoolkit"]
          interval: 10s
          timeout: 5s
          retries: 5
    volumes:
      postgres_data:                                  # 创建一个Docker卷持久化保存数据库数据

    ```
    然后在 `docker-compose.yml` 文件所在的目录下运行：
    ```bash
    docker-compose up -d
    ```


3.  **首次配置**：
    *   容器启动后，通过浏览器访问 `http://<你的服务器IP>:5257`。
    *   首次启动请用默认密码：password 登陆。
    *   进入通用设置页面，填写必要的配置信息。
    *   **点击保存。** 这会在你挂载的 `/config` 目录下（即宿主机的 `/path/to/your/app_data/eemby-toolkit/config` 目录）创建 `config.ini` 文件。

## 🔒 用户权限管理

本应用采用了创建专用的非root用户权限管理机制，确保容器内的应用以非root用户运行，同时保持对挂载卷的正确访问权限。

### 环境变量说明

*   **PUID**：用户ID，建议设置为宿主机上拥有媒体文件访问权限的用户ID
*   **PGID**：组ID，建议设置为宿主机上拥有媒体文件访问权限的组ID，如需一键更新容器需配置成docker组ID。
*   **UMASK**：文件权限掩码，控制新创建文件的权限，建议022表示新文件权限为755(目录)和644(文件)
*   **APP_DATA_DIR**：应用数据目录，已在镜像中默认设置为`/config`，无需在运行时重复指定

### 工作原理

1. 容器内创建了一个固定UID/GID为918的`embyactor`用户
2. 启动时，entrypoint.sh脚本会根据环境变量PUID和PGID动态修改`embyactor`用户的UID和GID
3. 应用以`embyactor`用户身份运行，而不是root用户
4. 所有挂载的卷和应用目录的所有权会被更改为`embyactor`用户

### 权限问题排查

如果遇到权限问题，请尝试以下步骤：

1. 确认PUID和PGID设置正确，可以通过在宿主机上运行`id`命令查看当前用户的UID和GID
2. 检查挂载目录的权限，确保指定的PUID/PGID用户有权访问
3. 如果使用NAS或特殊文件系统，可能需要调整UMASK值，例如使用000以允许最大权限

## 📝 日志

*   应用日志默认会输出到数据看板，同时会在配置目录生成日志文件。
*   可以在数据看板查看历史日志，通过搜索定位完整处理过程。

