# Emby Actor Processor (Emby 演员管理工具)

[![GitHub stars](https://img.shields.io/github/stars/hbq0405/emby-actor-processor.svg?style=social&label=Star)](https://github.com/hbq0405/emby-actor-processor)
[![GitHub license](https://img.shields.io/github/license/hbq0405/emby-actor-processor.svg)](https://github.com/hbq0405/emby-actor-processor/blob/main/LICENSE)
<!-- 你可以添加更多的徽章，例如构建状态、Docker Hub 拉取次数等 -->

一个用于处理和增强 Emby 媒体库中演员信息的工具，包括但不限于演员名称翻译、信息补全（从豆瓣、TMDb等）、以及演员映射管理。

## ✨ 功能特性

*   **演员信息处理**：自动翻译演员名、角色名、从豆瓣数据源获取中文角色名。
*   **外部数据源集成**：从豆瓣获取更丰富的演员信息，通过TMDB比对（如TMDBID、IMDBID充实演员映射表）。
*   **演员映射管理**：允许用户手动或自动同步 Emby 演员与外部数据源演员的映射关系，还可以直接导入别人分享的演员映射表。
*   **处理质量评估**：程序综合各方面因素自动对处理后的演员信息进行打分，低于阈值（自行设置）的分数会列入待复核列表，方便用户手动重新处理，特别是外语影视，机翻的效果很尬的。
*   **定时任务**：支持定时全量扫描媒体库和同步人物映射表。
*   **Docker 支持**：易于通过 Docker 部署和运行。
*   **实时处理新片**：自动处理Emby新入库资源，需配置webhook:http://ip:5257/webhook/emby 请求内容类型：application/json 勾选：新媒体已添加


## 🚀 快速开始

### 先决条件

*   已安装 Docker 和 Docker Compose (推荐)。
*   一个 Emby 服务器。
*   TMDb API Key (v3 Auth)。

### Docker 部署 (推荐)

这是最简单和推荐的部署方式。

1.  **准备持久化数据目录**：
    在你的服务器上（例如 NAS）创建一个目录，用于存放应用的配置文件和数据库。例如：
    ```bash
    mkdir -p /path/app_data/emby_actor_processor_config
    ```
    请将 `/path/app_data/emby_actor_processor_config` 替换为你实际的路径。

2.  **使用 `docker-compose.yml` (推荐)**：
    创建一个 `docker-compose.yml` 文件，内容如下：

    ```yaml
    version: '3.8'

    services:
      emby-actor-processor:
        image: hbq0405/emby-actor-processor:latest 
        container_name: emby-actor-processor
        ports:
          - "5257:5257" # 将容器的 5257 端口映射到宿主机的 5257 端口 (左边可以改成你希望的宿主机端口)
        volumes:
          - /path/app_data/emby_actor_processor_config:/config # 将宿主机的数据目录挂载到容器的 /config 目录
          - /path/cache:/cache #可选，神医Pro版豆瓣缓存路径
        environment:
          - APP_DATA_DIR=/config # 告诉应用数据存储在 /config 目录
          - TZ=Asia/Shanghai     # (可选) 设置容器时区，例如亚洲/上海
          # - PUID=1000            # (可选) 如果需要指定运行用户ID
          # - PGID=1000            # (可选) 如果需要指定运行组ID
        restart: unless-stopped
    ```
    然后在 `docker-compose.yml` 文件所在的目录下运行：
    ```bash
    docker-compose up -d
    ```

3.  **或者使用 `docker run` 命令**：
    ```bash
    docker run -d \
      --name emby-actor-processor \
      -p 5257:5257 \
      -v /path/app_data/emby_actor_processor_config:/config \
      -v /path/cache:/cache \
      -e APP_DATA_DIR="/config" \
      -e TZ="Asia/Shanghai" \
      --restart unless-stopped \
      hbq0405/emby-actor-processor:latest
    ```
    同样，请替换占位符。

4.  **首次配置**：
    *   容器启动后，通过浏览器访问 `http://<你的服务器IP>:5257`。
    *   进入各个设置页面（Emby配置、数据源等），填写必要的 API Key 和服务器信息。
    *   **点击保存。** 这会在你挂载的 `/config` 目录下（即宿主机的 `/path/to/your/app_data/emby_actor_processor_config` 目录）创建 `config.ini` 文件和 `emby_actor_processor.sqlite` 数据库文件。


## ⚙️ 配置项说明

应用的主要配置通过 Web UI 进行，并保存在 `config.ini` 文件中。关键配置项包括：

*   **Emby 配置**:
    *   Emby 服务器 URL
    *   Emby API Key
    *   Emby 用户 ID 
    *   要处理的媒体库
    *   更新后是否刷新 Emby 媒体项
*   **数据源配置**:
    *   TMDb API Key (v3)
    *   翻译引擎顺序 (例如 `bing,google,baidu`)
    *   本地数据源路径 (神医Pro版本地豆瓣数据源 actor_data 目录，如果使用本地数据源模式)
    *   豆瓣数据源处理策略
*   **定时任务配置**:
    *   是否启用定时全量扫描及 CRON 表达式
    *   是否强制重处理所有项目 (定时任务)
    *   是否启用定时同步人物映射表及 CRON 表达式
*   **通用设置**:
    *   处理项目间的延迟
    *   豆瓣API默认冷却时间
    *   待复核的最低评分阈值

## 🛠️ 手动操作

Web UI 的“手动操作”页面提供了以下功能：

*   **全量媒体库扫描**: 扫描并处理所有选定媒体库中的项目。
    *   可选择是否“强制重新处理所有项目”。
*   **同步Emby人物映射表**: 从 Emby 服务器拉取所有人物信息，并更新到本地的 `person_identity_map` 数据库表中。
*   **处理单个媒体项**: 输入 Emby Item ID，单独处理指定的电影或剧集。
    *   可选择是否“强制重新处理此项目”。
*   **停止当前任务**: 尝试停止当前正在后台运行的任务。

## 📝 日志

*   应用日志默认会输出到控制台。
*   在 Docker 部署时，可以通过 `docker logs emby-actor-processor` 查看容器日志。
*   Web UI 的“手动操作”页面也会实时显示后台任务的关键日志。

