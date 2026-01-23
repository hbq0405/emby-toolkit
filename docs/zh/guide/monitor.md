# 实时监控

实时监控由 `monitor_service.py` 实现，依赖 `watchdog`，用于监听媒体文件的新增、移动与删除事件。

## 关键配置

- `monitor_enabled`：启用监控
- `monitor_paths`：需要监听的目录列表
- `monitor_extensions`：媒体扩展名（默认包含 `.mp4`、`.mkv` 等）
- `monitor_scan_lookback_days`：补扫回溯天数
- `monitor_exclude_dirs`：排除目录（命中后仅刷新，不刮削）

## 行为说明

- **新增/移动**：文件将进入批处理队列，按目录聚合取代表文件执行刮削。
- **删除**：进入删除队列，触发 Emby 刷新。
- **排除路径**：避免某些目录触发刮削，保留刷新逻辑。

## 常见建议

- 监控目录保持与 Emby 媒体库路径一致。
- 排除“临时下载/缓存”目录，避免误触发。
