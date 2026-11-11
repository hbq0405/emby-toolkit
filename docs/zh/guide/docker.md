# Docker 部署

推荐 Docker Compose。下面的示例包含 ETKN 和 PostgreSQL，只对外发布默认媒体端口，适合首次部署；所有示例密码均需自行替换。

## Compose 示例

保存为 `compose.yml`：

```yaml
services:
  etkn:
    image: hbq0405/etkn:latest
    container_name: etkn
    restart: unless-stopped
    init: true
    volumes:
      - ./local_data:/config
      - ./media:/strm
      # 使用容器自更新功能时再添加 Docker socket 映射
      # - /var/run/docker.sock:/var/run/docker.sock
      # 本地视频、音乐另加所需目录，例如：
      # - /你的媒体目录:/media
    environment:
      APP_DATA_DIR: /config
      ETKN_CONFIG_DIR: /config
      ETKN_MEDIA_PORT: 8097
      ETKN_ADMIN_USERNAME: admin
      ETKN_ADMIN_PASSWORD: ""
      PUID: 1000
      PGID: 1000
      UMASK: "022"
      DB_HOST: db
      DB_PORT: 5432
      DB_USER: etkn
      DB_PASSWORD: change-this-database-password
      DB_NAME: etkn
      CONTAINER_NAME: etkn
      DOCKER_IMAGE_NAME: hbq0405/etkn:latest
      TZ: Asia/Shanghai
    ports:
      - "8097:8097"
    depends_on:
      db:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5257/api/health', timeout=3)"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 10m

  db:
    image: postgres:16-alpine
    container_name: etkn-db
    restart: unless-stopped
    environment:
      POSTGRES_USER: etkn
      POSTGRES_PASSWORD: change-this-database-password
      POSTGRES_DB: etkn
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etkn -d etkn"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

数据库密码在两个服务中必须一致。`PUID/PGID` 改为拥有配置和媒体目录读写权限的用户组。`ETKN_ADMIN_PASSWORD` 留空时首次启动生成随机初始密码并写入容器日志。

此示例可直接拉取公开镜像部署，不需要访问新版私有源码仓库。

## 启动和登录

```bash
docker compose up -d
docker compose ps
docker compose logs --tail=100 etkn
```

打开 `http://服务器IP:8097/web/`，登录后通过管理入口或 `/profile` 进入后台。公开健康检查为 `http://服务器IP:8097/api/health`。

容器内部健康检查仍访问 `127.0.0.1:5257`，这是内部 API 上游，不代表还要映射一个管理端口。

## 持久化目录

| 容器位置 | 用途 |
| --- | --- |
| `/config` | 授权辅助文件、图片、Telegram 会话及运行数据；默认播放缓存在其子目录 |
| `/strm` | 示例媒体主目录，保存生成的 STRM、外挂等内容 |
| PostgreSQL 数据卷 | 保存用户、配置、媒体、任务、订阅与整理记录 |
| 自选本地媒体挂载 | 已有视频、家庭视频、音乐；设置中使用容器内路径 |

媒体目录需要写外挂字幕时必须可写。播放缓存默认 `/config/playback-cache`，容量单独配置；可映射到其他磁盘，见 [缓存教程](/zh/guide/playback-cache)。

## 修改端口

- **首次 bridge 部署**：例如改成 `18097`，同时把 `ETKN_MEDIA_PORT` 和两端映射改成 `18097`。
- **已有配置**：后台保存的媒体端口优先于环境变量。先核对后台配置，再同步容器映射与客户端地址。
- **host 模式**：不写 `ports`，确保实际监听端口没有被占用；数据库地址按所选网络调整。

ETKN IP 填客户端可达的服务器地址。STRM 使用该地址与媒体端口组合，不能填容器内环回地址。需要域名与 HTTPS 时见 [反向代理](/zh/guide/reverse-proxy)。

## 升级与旧端口迁移

先备份数据库、`/config` 和媒体目录，再执行：

```bash
docker compose pull
docker compose up -d
docker compose ps
```

启动先执行数据库迁移。旧 STRM 地址迁移会把原内容备份到 `/config/migrations/strm-media-port-v1`，再按统一媒体端口改写符合条件的旧地址；备份和完成标记请保留。已有自定义域名与端口仍需按实际配置核对。

不要通过删除数据库卷或 `/config` 来解决一般升级问题。出现错误时，保留日志并查看 [故障排查](/zh/faq/troubleshooting)。

## 可选硬件转码

宿主机具备可用的 GPU 与驱动时，再添加相应设备映射；Intel 常见为 `/dev/dri:/dev/dri`。设备映射与转码开关是两步，完成后还要验证真实编码路径和播放效果，见 [内网缓存与转码](/zh/guide/playback-cache)。
