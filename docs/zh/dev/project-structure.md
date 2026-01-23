# 项目结构

顶层结构概览：

```
emby-toolkit/
  web_app.py                 # Web 服务入口
  core_processor.py          # 核心处理器
  watchlist_processor.py     # 追剧处理
  actor_subscription_processor.py # 演员订阅处理
  monitor_service.py         # 实时监控
  reverse_proxy.py           # 反向代理/虚拟库
  task_manager.py            # 任务队列与状态
  scheduler_manager.py       # 任务调度
  handler/                   # 外部服务与数据源集成
  routes/                    # API 路由
  tasks/                     # 任务与任务链
  database/                  # 数据库模型与初始化
  services/cover_generator/  # 封面生成
  emby-actor-ui/             # 前端 UI
  docker/entrypoint.sh       # 容器启动逻辑
```

## 关键目录说明

- `handler/`：Emby、TMDb、豆瓣、MoviePilot、Telegram 的 API 封装。
- `routes/`：按域划分的蓝图路由（系统、日志、订阅、合集等）。
- `tasks/`：可执行任务，任务链注册在 `tasks/core.py`。
- `database/`：表结构初始化与数据访问封装。
