# 演员订阅

演员订阅由 `actor_subscription_processor.py` 与相关任务处理，适合追踪喜欢的演员并自动订阅其作品。

## 能力概览

- 按演员维度追踪作品更新
- 自动补全历史作品并触发订阅
- 支持订阅策略与每日请求上限

## 关联任务

- `actor-tracking`：刷新演员订阅
- `sync-person-map`：同步演员映射信息

## 依赖

- MoviePilot 已配置
- Emby 连接正常
