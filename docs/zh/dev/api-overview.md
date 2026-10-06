# API 概览

新版 API 使用 FastAPI，默认由 `5257` 提供。管理接口需要 ETKN 登录，部分媒体服务和图片接口按客户端协议开放。

## 认证和健康检查

- `POST /api/auth/login`：登录 ETKN 账号。
- `GET /api/health`：服务健康检查。
- `GET /api/modules`：查看模块是否已配置，不返回密钥。

## 配置和任务

- `/api/configuration`：模块配置读取和保存。
- `/api/organize-settings`：整理规则配置。
- `/api/tasks`、`/api/workflows`：任务目录、运行状态、历史、日志和诊断。
- `/api/automation-plans`：自动任务计划。

## 资源和订阅

- `/api/p115`：115 文件、目录、播放和整理记录。
- `/api/share-imports`：115 分享链接导入、检查和重试。
- `/api/shared-pool`：共享池资源、共享源、获取和虚拟入库。
- `/api/resources/moviepilot`：MoviePilot 状态、配置和连接测试。
- `/webhook`：MoviePilot 事件接收。
- `/api/re0`：re0 授权和资源获取。
- `/api/subscriptions`、`/api/watchlist`、`/api/actor-subscriptions`：统一订阅、智能追剧和演员订阅。
- `/api/virtual-libraries`：虚拟媒体库配置、刷新和条目匹配。

## 媒体和播放

- `/api/media-management`：媒体库、媒体项、刷新、重处理、MediaInfo、图片和版本操作。
- `/api/emby/events`、`/api/media-server/events`：内置媒体服务事件。
- `/api/media-server`：兼容 Emby/Jellyfin 的媒体服务路径和播放能力。
- `/api/playback`：管理员播放会话、最近播放记录和统计。

接口是实现细节，用户操作优先通过 Web 控制台完成。生产环境不要把数据库端口或管理 API 直接暴露到公网。
