# 数据库结构

数据库初始化由 `database/connection.py` 完成。以下为主要表结构摘要（非完整字段）。

## 日志与配置

- `processed_log`：已处理媒体项记录。
- `failed_log`：处理失败记录。
- `translation_cache`：翻译缓存。
- `app_settings`：动态配置（Web UI 保存）。

## 用户与行为

- `emby_users`：Emby 用户基础信息。
- `emby_users_extended`：扩展信息与策略。
- `user_media_data`：播放进度、收藏等行为数据。
- `user_templates` / `invitations`：模板与邀请相关数据。

## 媒体与合集

- `media_metadata`：核心媒体元数据与订阅/追剧状态。
- `collections_info`：原生 TMDb 合集缓存。
- `custom_collections`：自建合集定义与状态。

## 演员与订阅

- `person_identity_map`：演员身份映射。
- `actor_metadata`：演员元数据与别名。
- `actor_subscriptions`：演员订阅配置。

## 订阅与清理

- `resubscribe_rules` / `resubscribe_index`：订阅与补全规则缓存。
- `cleanup_index`：清理/维护相关记录。

## 索引说明

`media_metadata` 上包含多维索引（库状态、订阅状态、标签/关键词、日期排序等），用于加速筛选与排序。
