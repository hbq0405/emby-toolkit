# 自建合集

自建合集用于根据规则生成“虚拟库”，支持榜单类与过滤类两种模式，数据主要存储在 `custom_collections` 与 `collections_info` 表中。

## 合集类型

- **榜单类**：固定的 TMDb/外部榜单，入库时追加到 Emby 合集中。
- **过滤类**：基于 SQL/规则实时计算，不需要入库时更新。

## 关键行为

- 合集规则存储于数据库，定时任务会刷新合集内容。
- 反向代理会将自建合集合并到 Emby 主页视图。

## 关联任务

- `custom-collections`：刷新全部自建合集
- `refresh-collections`：刷新原生 TMDb 合集
