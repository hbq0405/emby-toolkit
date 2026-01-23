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
5. 按需启用任务链、实时监控、反向代理等功能。

如果你需要完整的部署说明，请阅读 [Docker 部署](/zh/guide/docker)。
