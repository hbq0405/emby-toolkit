# 快速开始

本文按当前 Compose 模板说明，适合第一次部署 ETKvNext 的用户。

## 准备

- Docker / Docker Compose，或 Unraid 的 Compose 管理器。
- 一个可持久化的宿主机目录，例如 `/mnt/user/appdata/etkn`。
- 115 账号。没有 115 授权时仍可以先浏览控制台，但无法完成 115 整理和大多数资源入库。
- TMDb API Key。它用于影视识别、元数据和影视探索。
- 如果使用 MoviePilot、共享池、re0、Telegram 频道或 AI，再准备对应服务的地址和凭据。

默认 Compose 自带 PostgreSQL，不需要先手动创建数据库。

## 启动

在保存 `compose.yml` 的目录执行：

```bash
docker compose up -d
docker compose ps
docker compose logs -f etkn
```

看到 `etkn` 和 `etkn-db` 正常运行后，打开：

```text
http://服务器IP:5257
```

首次启动如果没有填写 `ETKN_ADMIN_PASSWORD`，系统会生成一次性管理员初始密码并写入 `etkn` 容器日志。登录后请立即修改密码并妥善保存。

## 第一次操作

1. 使用 ETKN 管理员账号登录，不是使用 115 密码登录。
2. 在“服务授权”完成 115 OpenAPI 或 Cookie 授权。
3. 在“设置中心”填写 TMDb Key，并按需配置网络代理、AI、Telegram 和其他来源。
4. 在“媒体库”配置媒体库主目录、内置媒体服务地址和端口。
5. 在“整理”配置待整理目录、分类、命名和洗版规则。
6. 放入一部测试电影或一整季剧集。
7. 在“任务中心”确认从资源获取/扫描到整理、元数据、媒体信息和媒体库发布的链路完成。

## 两个端口

- `5257`：Web 控制台、管理 API 和授权页面。
- `8097`：内置媒体服务。需要让 Emby/Jellyfin 客户端或 STRM 播放地址访问时，映射此端口并填写客户端可访问的 ETKN 地址。

## 不要混用两个版本

旧版 ETK 和 ETKvNext 不要同时处理同一个待整理目录，也不要让两个版本同时维护同一批媒体。新版启动时只执行自己的数据库迁移，不会自动把旧版数据库搬过来。