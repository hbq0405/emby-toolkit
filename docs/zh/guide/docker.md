# Docker 部署

ETKvNext 推荐使用 Unraid Compose 或 Docker Compose。开发分支默认构建 `linux/amd64` 镜像，正式版本标签才构建多架构镜像。

## Compose

下面的环境变量直接配置在 Compose 或 Unraid 容器环境变量中，不要求额外创建 `.env` 文件：

```yaml
services:
  etkn:
    image: hbq0405/etkn:latest
    container_name: etkn
    cap_add:
      - SYS_NICE
    security_opt:
      - no-new-privileges:true
    restart: unless-stopped
    init: true
    volumes:
      - ./local_data:/config
      - /path/strm:/strm
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - APP_DATA_DIR=/config
      - ETKN_CONFIG_DIR=/config
      - PUID=1000
      - PGID=1000
      - UMASK=022
      - ETKN_ADMIN_USERNAME=admin
      # 留空时首次启动自动生成随机密码并打印到容器日志
      - ETKN_ADMIN_PASSWORD=
      - DB_HOST=db
      - DB_PORT=5432
      - DB_USER=etkn
      - DB_PASSWORD=etkn
      - DB_NAME=etkn
      - CONTAINER_NAME=etkn
      - DOCKER_IMAGE_NAME=hbq0405/etkn:latest
      - TZ=Asia/Shanghai
    ports:
      - "5257:5257"
      - "8097:8097"
    networks:
      - etkn-net
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
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=etkn
      - POSTGRES_PASSWORD=etkn
      - POSTGRES_DB=etkn
    networks:
      - etkn-net
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etkn -d etkn"]
      interval: 10s
      timeout: 5s
      retries: 5

networks:
  etkn-net:
    driver: bridge

volumes:
  postgres_data:
```

`5257` 是管理端口，`8097` 是虚拟库反代端口。`/config` 必须持久化，频道监听会话、图片仓库和运行文件都保存在这里。
该模板仅供参考，建议数据库分开部署或借用已有的PG数据库。

## 启动与检查

```bash
docker compose up -d
docker compose ps
docker compose logs -f etkn
```

浏览器访问 `http://服务器IP:5257`。健康检查接口为 `/api/health`，应返回 `{"status":"ok"}`。

## 升级

```bash
docker compose pull
docker compose up -d
```

升级前不需要删除 `/config`。数据库迁移会在容器启动时自动执行，只创建或升级结构，不会读取旧版数据库。
