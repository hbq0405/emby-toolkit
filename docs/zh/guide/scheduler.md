# 任务与调度

调度器由 `scheduler_manager.py` 管理，任务定义集中在 `tasks/core.py`。

## 任务链

- **高频任务链**：`task_chain_enabled` + `task_chain_cron` + `task_chain_sequence`
- **低频任务链**：`task_chain_low_freq_enabled` + `task_chain_low_freq_cron` + `task_chain_low_freq_sequence`

任务链是有序列表，按序执行并写入任务状态与日志。

## 常见任务（节选）

- `sync-person-map`：同步演员数据
- `enrich-aliases`：演员数据补充
- `populate-metadata`：同步媒体元数据
- `role-translation`：角色名中文化
- `actor-translation`：演员名中文化
- `process-watchlist`：刷新智能追剧
- `actor-tracking`：刷新演员订阅
- `refresh-collections`：刷新原生合集
- `custom-collections`：刷新自建合集
- `auto-subscribe`：统一订阅处理
- `generate-all-covers`：生成封面

## 任务队列规则

- 任务互斥：同一时间只执行一个后台任务。
- 支持中止：通过 API 或 UI 发送停止任务请求。
- 进度反馈：任务执行过程写入前端日志队列。

## Cron 示例

```text
0 2 * * *     # 每天凌晨 2 点
0 5 * * 0     # 每周日凌晨 5 点
*/30 * * * *  # 每 30 分钟
```
