# 外部服务集成

## TMDb

- 用途：媒体元数据、演员信息、图片与评分基础信息。
- 必填：`tmdb_api_key`。

## 豆瓣

- 用途：补全角色中文名、演员信息与相关元数据。
- 建议配置：`douban_cookie` 以提升可用性。

## MoviePilot

- 用途：订阅缺失季/集、统一订阅与自动补全。
- 相关配置：`moviepilot_url`、`moviepilot_username`、`moviepilot_password`。

## NULLBR

- 用途：从115分享、磁力链、ED2K获取链接云下载至115网盘
- 相关配置：CMS URL、CMS TOKEN；115cookie

## Telegram

- 用途：新入库/追更通知。
- 相关配置：`telegram_bot_token`、`telegram_channel_id`。

## GitHub

- 用途：版本检查（提升 API 速率限制）。
- 配置：`github_token`。
