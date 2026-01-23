# Docker 部署

推荐使用 `docker-compose.yml` 方式部署，以下示例与仓库 README 一致，并补充关键说明。

## 目录准备

```bash
mkdir -p /path/emby-toolkit
```

## 示例 Compose

```yaml
services:
  emby-toolkit:
    image: hbq0405/emby-toolkit:latest
    container_name: emby-toolkit
    network_mode: bridge
    ports:
      - "5257:5257"  # Web 控制台
      - "8097:8097"  # 反向代理/虚拟库端口
    volumes:
      - /path/emby-toolkit:/config
      - /path/media:/media
      - /path/tmdb:/tmdb
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - APP_DATA_DIR=/config
      - TZ=Asia/Shanghai
      - PUID=1000
      - PGID=1000
      - UMASK=022
      - DB_HOST=172.17.0.1
      - DB_PORT=5433
      - DB_USER=embytoolkit
      - DB_PASSWORD=embytoolkit
      - DB_NAME=embytoolkit
      - CONTAINER_NAME=emby-toolkit
      - DOCKER_IMAGE_NAME=hbq0405/emby-toolkit:latest
    restart: unless-stopped
    depends_on:
      db:
        condition: service_healthy

  db:
    image: postgres:18
    container_name: emby-toolkit-db
    restart: unless-stopped
    network_mode: bridge
    volumes:
      - postgres_data:/var/lib/postgresql
    environment:
      - POSTGRES_USER=embytoolkit
      - POSTGRES_PASSWORD=embytoolkit
      - POSTGRES_DB=embytoolkit
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U embytoolkit -d embytoolkit"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

## 端口说明

- `5257`：主 Web 控制台（API 与前端 UI）。
- `8097`：反向代理端口（虚拟库/合并视图）。

## 持久化目录

- `/config`：配置、日志、数据库连接信息等持久化数据。
- `/media`：媒体库目录（实时监控与增量处理）。

## 启动

```bash
docker-compose up -d
```
