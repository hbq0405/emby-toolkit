# Docker 部署

ETKvNext 推荐使用 Docker Compose 或 Unraid Compose。当前模板包含一个 ETKN 服务和一个 PostgreSQL 服务。

## 当前 Compose 结构

核心配置如下：

```yaml
services:
  etkn:
    image: hbq0405/etkn:latest
    container_name: etkn
    restart: unless-stopped
    init: true
    volumes:
      - ./local_data:/config
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - APP_DATA_DIR=/config
      - ETKN_CONFIG_DIR=/config
      - DB_HOST=db
      - DB_PORT=5432
      - DB_USER=etkn
      - DB_PASSWORD=etkn
      - DB_NAME=etkn
      - CONTAINER_NAME=etkn
      - DOCKER_IMAGE_NAME=hbq0405/etkn:latest
      - TZ=Asia/Shanghai
      - ETKN_ADMIN_USERNAME=admin
      - ETKN_ADMIN_PASSWORD=
    ports:
      - "5257:5257"
      - "8097:8097"
    depends_on:
      db:
        condition: service_healthy

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

volumes:
  postgres_data:
```

完整默认值以新版仓库的 `compose.yml` 为准。生产环境请修改默认数据库密码，并把 `./local_data` 换成稳定的宿主机目录。

## 持久化什么

必须持久化 `/config`，它包含：

- 应用配置和授权辅助文件。
- 图片仓库和媒体处理缓存。
- Telegram 频道监听会话。
- 其他需要跨容器重建保留的运行数据。

PostgreSQL 的 `postgres_data` 也必须持久化。不要把数据库容器删掉再重新创建来处理普通升级问题。

如果要处理宿主机本地视频，还需要按自己的目录规划增加媒体目录映射，并在“设置中心 -> 媒体库”中选择容器内路径。容器内路径和宿主机路径不是同一个字符串时，要以容器内路径为准。

## 启动检查

```bash
docker compose up -d
docker compose ps
docker compose logs --tail=200 etkn
```

管理 API 健康检查：

```text
http://服务器IP:5257/api/health
```

正常时应返回状态为 `ok` 的 JSON。健康检查只能说明服务已启动，真正使用前还要登录控制台完成 115、TMDb 和媒体库配置。

## 升级

```bash
docker compose pull
docker compose up -d
docker compose ps
```

容器启动时会先执行版本化迁移，成功后才启动应用。升级不要删除 `/config` 或 `postgres_data`。如果升级后业务异常，先在任务中心和容器日志中确认具体失败阶段，不要直接清空数据库重来。

## 端口与网络

- `5257` 面向管理页面和 API。
- `8097` 面向内置媒体服务和虚拟媒体库。
- 修改 `8097` 时，要同时修改 Compose 端口映射和 ETKN“媒体服务器端口”，再重启容器。
- 需要给 STRM 或客户端访问时，在“媒体库”中填写客户端真正能访问的 ETKN 地址；不要填写容器内的 `127.0.0.1`。