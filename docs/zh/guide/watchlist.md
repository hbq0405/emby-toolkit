# 智能追剧

智能追剧能力由 `watchlist_processor.py` 与相关任务实现，核心数据写入 `media_metadata` 表。

## 功能点

- 新剧入库自动加入追剧列表。
- 判断缺失季/集并触发订阅请求。
- 维护剧集状态：在追、暂停、完结等。

## 关联任务

- `process-watchlist`：刷新追剧列表
- `scan-library-gaps` / `scan-old-seasons-backfill`：扫描缺失季

## 依赖

- Emby 连接正常
- MoviePilot 配置完成（用于订阅）
