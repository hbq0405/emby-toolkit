# Webhook 接入

Webhook 用于接收 Emby 和 MoviePilot 事件并触发对应处理。

## 配置地址

```
http://<服务器IP>:5257/webhook/emby
请求内容类型：application/json
```

MoviePilot Webhook 也使用同一个地址。ETK 会根据 payload 里的 `type` 字段识别 MP 事件，根据 `Event` 字段识别 Emby 事件。

## 推荐事件

### Emby

- 媒体：已添加新媒体
- 播放：开始、停止
- 用户：添加到收藏、移出收藏、标记已播放、标记未播放、用户政策已更新
- 神医助手：媒体深度删除、元数据更新、图像更新

### MoviePilot

- `download.added`
- `subscribe.added`
- `subscribe.modified`
- `subscribe.deleted`
- `subscribe.complete`
- `transfer.complete`
- `transfer.subtitle.complete`

## 行为说明

- Webhook回流，补全实时监控预处理缺失的Emby_id和视频流数据。
- 用户事件：实时同步用户权限、播放记录。
- 元数据/图像更新：同步更新Emby修改图像、元数据到覆盖缓存和数据库。
- MoviePilot 订阅事件：订阅助手实时接管新增、修改、删除、完成事件，维护下载状态和完成快照。
- MoviePilot 下载事件：记录下载 hash、站点、集数等信息，供订阅助手下载巡检使用。
- MoviePilot 整理完成事件：进入 ETK 入库整理、追剧刷新、共享登记等后续流程。

## 速率控制

Webhook 内置去抖与批处理逻辑，可减少高频事件对 Emby 的压力。
