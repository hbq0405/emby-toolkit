# 核心模块

下面按新版用户能看到的业务边界说明模块。源码目录可能继续调整，接口和配置名称以当前版本为准。

## 入口和运行时

- `src/etk_vnext/api/`：FastAPI 路由，按媒体、任务、资源、用户和系统功能拆分。
- `src/etk_vnext/runtime.py`：启动媒体监控、上传监控、频道监听、Telegram Bot、任务调度和后台工作线程。
- `frontend/src/`：管理控制台前端，主要入口包括资源中心、订阅中心、媒体管理、增强功能、任务中心、播放中心和整理记录。

## 业务模块

- `modules/core_ingest/`：核心整理和入库，保证不同资源来源最终进入同一条处理链。
- `modules/resource/`：115 分享入库、共享池、虚拟入库和 MoviePilot 资源处理。
- `modules/subscription/`：智能追剧、订阅需求、合集补齐和演员订阅。
- `modules/subscription_assistant/`：MoviePilot 订阅状态同步、完结守卫、下载巡检和快照维护。
- `modules/media_server/`：内置 Emby/Jellyfin 兼容媒体服务、媒体库查询和播放相关接口。
- `modules/virtual_library/`：列表、规则、推荐和路径型虚拟媒体库。
- `modules/notifications/`：Telegram 通知和管理员 Bot 交互。
- `modules/users/`：ETKN 账号、邀请、权限模板和个人偏好。

## 数据和配置

- `db/` 与 `migrations/`：PostgreSQL 连接、迁移和数据库访问。
- `platform/config/`：按模块保存配置，并通过 revision 防止并发覆盖。
- `modules/*/repository.py`：各业务域自己的持久化访问层。

## 外部连接

`integrations/` 包含 TMDb、Fanart、Bangumi、MoviePilot、115、Telegram、共享池、re0 和网络代理等连接。没有授权的外部服务不会阻止管理页面和内置媒体服务启动。
