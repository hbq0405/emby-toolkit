# 配置项总览

配置由 `config.ini` 与数据库 `app_settings` 共同组成：

- 启动所需的基础配置写入 `config.ini`。
- 动态配置写入数据库 `app_settings`（Web UI 保存）。

配置文件路径：

- Docker：`/config/config.ini`（由 `APP_DATA_DIR` 指定）
- 本地开发：`<项目>/local_data/config.ini`

## Database

| Key | 说明 |
| --- | --- |
| `db_host` | PostgreSQL 主机 |
| `db_port` | 端口 |
| `db_user` | 用户名 |
| `db_password` | 密码 |
| `db_name` | 数据库名 |

## Authentication

| Key | 说明 |
| --- | --- |
| `auth_enabled` | 是否启用认证（首次运行默认开启） |
| `username` | 管理员用户名 |

## Emby

| Key | 说明 |
| --- | --- |
| `emby_server_url` | Emby 服务器地址 |
| `emby_public_url` | Emby 公网地址 |
| `emby_api_key` | Emby API Key |
| `emby_user_id` | Emby 用户 ID |
| `emby_api_timeout` | API 超时（秒） |
| `libraries_to_process` | 处理的媒体库列表 |
| `emby_admin_user` | 管理员用户名（可选） |
| `emby_admin_pass` | 管理员密码（可选） |

## ReverseProxy

| Key | 说明 |
| --- | --- |
| `proxy_enabled` | 是否启用反向代理 |
| `proxy_port` | 反向代理端口 |
| `proxy_merge_native_libraries` | 是否合并原生库 |
| `proxy_native_view_selection` | 原生库筛选 |
| `proxy_native_view_order` | 原生库排列顺序 |
| `proxy_302_redirect_url` | 302 跳转地址 |
| `proxy_show_missing_placeholders` | 缺失项目占位 |

## TMDb / GitHub

| Key | 说明 |
| --- | --- |
| `tmdb_api_key` | TMDb API Key |
| `tmdb_api_base_url` | TMDb API 基地址 |
| `tmdb_include_adult` | 包含成人内容 |
| `tmdb_image_language_preference` | 图片语言偏好 |
| `github_token` | GitHub Token（用于版本检查） |

## DoubanAPI

| Key | 说明 |
| --- | --- |
| `api_douban_default_cooldown_seconds` | 冷却时间 |
| `douban_cookie` | 豆瓣 Cookie |
| `douban_enable_online_api` | 是否启用在线 API |

## MoviePilot

| Key | 说明 |
| --- | --- |
| `moviepilot_url` | 服务地址 |
| `moviepilot_username` | 用户名 |
| `moviepilot_password` | 密码 |
| `autosub_enabled` | 智能订阅总开关 |
| `resubscribe_daily_cap` | 每日订阅上限 |
| `resubscribe_delay_seconds` | 订阅请求间隔 |

## Monitor

| Key | 说明 |
| --- | --- |
| `monitor_enabled` | 启用实时监控 |
| `monitor_paths` | 监控目录列表 |
| `monitor_extensions` | 扩展名列表 |
| `monitor_scan_lookback_days` | 回溯扫描天数 |
| `monitor_exclude_dirs` | 排除路径 |

## LocalDataSource

| Key | 说明 |
| --- | --- |
| `local_data_path` | 本地元数据 JSON 根目录 |

## General

| Key | 说明 |
| --- | --- |
| `delay_between_items_sec` | 处理间隔 |
| `min_score_for_review` | 低分阈值 |
| `max_actors_to_process` | 单项目演员上限 |
| `remove_actors_without_avatars` | 移除无头像演员 |

## Network

| Key | 说明 |
| --- | --- |
| `network_proxy_enabled` | 是否启用网络代理 |
| `network_http_proxy_url` | HTTP 代理地址 |
| `user_agent` | 请求 UA |
| `accept_language` | Accept-Language |

## AITranslation

| Key | 说明 |
| --- | --- |
| `ai_translation_enabled` | 启用 AI 翻译 |
| `ai_provider` | 提供商 |
| `ai_api_key` | API Key |
| `ai_model_name` | 模型名称 |
| `ai_base_url` | API 基地址 |
| `ai_translation_mode` | 翻译模式 |

## Scheduler

| Key | 说明 |
| --- | --- |
| `task_chain_enabled` | 高频任务链开关 |
| `task_chain_cron` | 高频 Cron |
| `task_chain_sequence` | 高频任务序列 |
| `task_chain_max_runtime_minutes` | 高频最大运行时间 |
| `task_chain_low_freq_enabled` | 低频任务链开关 |
| `task_chain_low_freq_cron` | 低频 Cron |
| `task_chain_low_freq_sequence` | 低频任务序列 |
| `task_chain_low_freq_max_runtime_minutes` | 低频最大运行时间 |

## Actor

| Key | 说明 |
| --- | --- |
| `actor_role_add_prefix` | 角色名前缀 |
| `actor_main_role_only` | 仅处理主要角色 |

## Logging

| Key | 说明 |
| --- | --- |
| `log_rotation_size_mb` | 单文件大小阈值 |
| `log_rotation_backup_count` | 备份数 |

## Telegram

| Key | 说明 |
| --- | --- |
| `telegram_bot_token` | Bot Token |
| `telegram_channel_id` | 频道/群组 ID |
