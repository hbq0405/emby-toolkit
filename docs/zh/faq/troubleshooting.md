# 故障排查

## Web UI 无法访问

- 检查容器是否运行，端口 `5257` 是否映射。
- 查看容器日志确认 Web 服务是否启动成功。

## Webhook 无响应

- 确认 Emby Webhook 地址为 `http://<服务器IP>:5257/webhook/emby`。
- 检查 Emby 事件类型是否勾选。

## 反向代理无虚拟库

- 确认 `proxy_enabled=true`。
- 检查 `proxy_port` 端口是否映射。
- 查看启动日志是否生成 Nginx 配置。

## 任务不执行

- 任务链开关是否开启。
- Cron 表达式是否合法。
- 同一时间只能执行一个后台任务。

## 元数据补全异常

- TMDb API Key 是否正确。
- 豆瓣 Cookie 是否配置（如遇登录限制）。
- 网络代理是否可用。
