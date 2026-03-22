# 核心模块

## Web 应用与路由

- **入口**：`web_app.py`，负责初始化配置、数据库、处理器与蓝图路由。
- **蓝图路由**：`routes/` 目录，提供系统配置、任务控制、媒体、日志、订阅、用户等 API。

## 处理器与业务逻辑

- **核心处理器**：`core_processor.py`，执行元数据处理、翻译、补全与回写。
- **追剧处理器**：`watchlist_processor.py`，维护剧集状态与缺失季订阅。
- **演员订阅处理器**：`actor_subscription_processor.py`，跟踪演员并提交订阅。

## 任务系统

- **任务队列**：`task_manager.py`，任务互斥与状态同步。
- **任务注册**：`tasks/core.py`，定义任务注册表与任务链。
- **任务模块**：`tasks/` 目录，按子域拆分功能（媒体、演员、订阅、清理、封面等）。

## 实时监控与反代

- **监控服务**：`monitor_service.py`，处理文件新增/移动/删除事件，支持排除路径与批处理。
- **反向代理**：`reverse_proxy.py`，合并虚拟库与原生库展示，支持虚拟项目 ID 机制。

## 数据与集成

- **数据库层**：`database/` 目录，表结构初始化与 CRUD 封装。
- **外部数据源**：`handler/` 目录，封装 TMDb、豆瓣、MoviePilot、Telegram 等外部接口。
- **封面服务**：`services/cover_generator/` 提供样式化封面生成。

## 115 网盘集成

- **115 服务**：`handler/p115_service.py`，提供 115 网盘 API 封装、智能整理引擎、STRM 生成器。
- **115 路由**：`routes/p115.py`，提供扫码登录、直链播放、目录管理、分类规则配置等 API。
- **文件系统缓存**：`database/` 中的 `p115_filesystem_cache` 表，缓存网盘目录结构提升查询性能。
- **整理记录**：`database/` 中的 `p115_organize_records` 表，记录文件整理历史与手动纠错信息。
