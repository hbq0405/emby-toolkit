# 用户与权限

系统会同步 Emby 用户信息到数据库，用于权限与偏好管理。

## 相关数据表

- `emby_users`：基础用户信息、管理员标记、头像信息。
- `emby_users_extended`：扩展字段（策略与附加信息）。
- `user_media_data`：用户观看记录与收藏状态。

## 相关接口

- 用户同步与模板策略更新由 `routes/user_management.py` 与 `tasks/users.py` 处理。
- 变更策略会触发自动同步逻辑。

## 使用建议

- 如需与 Emby 权限同步，请保持 Emby 管理账户配置正确。
- 批量变更建议通过任务链触发同步。
