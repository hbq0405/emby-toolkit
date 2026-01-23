# 反向代理与虚拟库

反向代理模块由 `reverse_proxy.py` 与 Nginx 配置生成逻辑配合实现，用于将“自建合集”作为虚拟库展示在 Emby 视图中。

## 启用方式

- 配置项：`proxy_enabled=true`
- 端口：`proxy_port`（默认 8097）
- Nginx 配置由启动脚本自动生成

## 工作机制

- 将虚拟库与原生库合并输出“视图”列表。
- 对虚拟项目使用特殊 ID（负数或前缀）进行映射。
- 对大量 ID 进行并发抓取与排序，必要时内存排序回退。

## 配置建议

- `proxy_merge_native_libraries`：是否合并原生库。
- `proxy_native_view_selection`：限定要显示的原生库。
- `proxy_native_view_order`：原生库显示顺序（before/after）。
- `proxy_show_missing_placeholders`：是否显示缺失项目占位。

## 常见问题

- 虚拟库未显示：检查 `proxy_enabled`、端口映射与 Nginx 配置是否生成成功。
- 权限异常：确保 Emby 用户可访问相关库。
