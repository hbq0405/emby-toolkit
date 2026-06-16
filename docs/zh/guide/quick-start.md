# 快速开始

本节面向第一次使用者，目标是尽快跑起来并完成基础配置。

## 先决条件

- Docker 与 Docker Compose（推荐）
- 可访问的 Emby 服务器
- TMDb API Key

## 最快路径

1. 使用 Docker Compose 启动服务。
2. 访问 `http://<服务器IP>:5257` 打开管理界面。
3. 填写 Emby、TMDb、MoviePilot（如需要）等配置并保存。
4. 在 Emby 配置 Webhook 指向 `http://<服务器IP>:5257/webhook/emby`。
5. 配置 115 网盘登录，用于整理、STRM、播放直链和共享资源。
6. 按需启用任务链、实时监控、反向代理等功能。
7. 如果要使用公共资源互助，继续阅读 [共享资源中心](/zh/guide/shared-resource)。

如果你需要完整的部署说明，请阅读 [Docker 部署](/zh/guide/docker)。

## 推荐的新手顺序

1. 先让 Web 控制台能打开。
2. 填好 Emby 和 TMDb。
3. 确认 115 登录正常。
4. 先跑一次基础任务，确认媒体能入库。
5. 再开启共享资源、求共享、自动秒传等高级功能。
