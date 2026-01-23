# API 概览

API 由多个蓝图组成，统一挂载在 `web_app.py` 中。

## 系统与状态

- `routes/system.py`：系统状态、配置读取/保存、任务控制、代理测试、AI 测试等。
- `routes/logs.py`：日志与前端日志流。

## 媒体与处理

- `routes/media.py`：媒体查询、代理、刷新相关接口。
- `routes/tasks.py`：任务触发与任务链控制。
- `routes/actions.py`：批量操作入口。

## 订阅与合集

- `routes/watchlist.py`：追剧管理与状态更新。
- `routes/actor_subscriptions.py`：演员订阅管理。
- `routes/custom_collections.py`：自建合集管理。
- `routes/tmdb_collections.py`：TMDb 合集处理。
- `routes/resubscribe.py`：补订阅/统一订阅。

## 用户与权限

- `routes/user_management.py`：用户同步与权限策略。
- `routes/user_portal.py`：用户门户相关接口。

## 其他

- `routes/cover_generator_config.py`：封面生成配置。
- `routes/database_admin.py`：数据库管理与维护。
- `routes/webhook.py`：Webhook 接入（`/webhook/emby`）。
