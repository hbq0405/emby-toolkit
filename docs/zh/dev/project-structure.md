# 项目结构

新版代码位于 `src/etk_vnext/`，前端位于 `frontend/`，数据库迁移位于 `migrations/`。

```text
ETKvNext/
  src/etk_vnext/
    api/                 # FastAPI 路由
    db/                  # PostgreSQL 连接、迁移入口和数据库工具
    integrations/        # TMDb、115、MoviePilot、Telegram 等外部连接
    modules/             # 媒体、资源、订阅、播放、用户等业务模块
    platform/            # 配置、任务、权限和平台服务
    runtime.py           # 后台运行时、监控和工作线程
  frontend/
    src/views/           # 页面：资源、订阅、媒体管理、任务、播放等
    src/components/      # 业务组件
    src/api/             # 前端 API 封装
  migrations/            # 版本化 PostgreSQL 迁移
  compose.yml            # ETKN 与 PostgreSQL 的默认部署
  Dockerfile             # 镜像构建
```

## 常见入口

- 管理页面导航：`frontend/src/components/AppSidebar.vue`。
- API 应用和路由注册：`src/etk_vnext/api/app.py`。
- Telegram Bot：`src/etk_vnext/modules/notifications/telegram_bot.py`。
- 内置媒体服务事件：`src/etk_vnext/api/emby_events.py`。
- MoviePilot Webhook：`src/etk_vnext/api/moviepilot.py`。
- 任务目录和执行：`src/etk_vnext/platform/tasks/`。
- 虚拟媒体库：`src/etk_vnext/modules/virtual_library/`。

开发时优先沿 API -> Service -> Repository 追踪业务，不要按旧版 `web_app.py`、`routes/`、`core_processor.py` 的目录假设寻找新版实现。
