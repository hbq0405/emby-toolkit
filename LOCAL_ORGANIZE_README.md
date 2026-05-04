# 本地文件整理模块 - 安装说明

## 已自动创建的文件

1. `constants.py` - 配置常量 (已添加)
2. `config_manager.py` - 配置定义 (已添加)
3. `tasks/local_organize.py` - 核心任务 (~450行)
4. `routes/local_organize.py` - API 路由 (~180行)
5. `task_manager.py` - 任务注册 (已添加)
6. `emby-actor-ui/src/components/LocalOrganizePage.vue` - WebUI页面
7. `emby-actor-ui/src/components/LocalOrganizeConfigModal.vue` - 配置弹窗
8. `emby-actor-ui/src/router/index.js` - 路由注册
9. `emby-actor-ui/src/MainLayout.vue` - 菜单注册

## 需要手动修改的文件

由于 `web_app.py` 是编译文件 (pyarmor)，需要手动添加：

### 1. 在其他 import 后面添加:
```python
from routes.local_organize import local_organize_bp
```

### 2. 在应用创建处添加:
```python
app.register_blueprint(local_organize_bp)
```

注意: 找到其他类似 `app.register_blueprint(p115_bp)` 的位置添加。

## 功能说明

### 配置项 (8项)
| 配置项 | 类型 | 默认 | 说明 |
|--------|------|------|------|
| local_organize_enabled | boolean | false | 总开关 |
| local_organize_source_movie | string | "" | 电影源目录 |
| local_organize_source_tv | string | "" | 电视剧源目录 |
| local_organize_source_mixed | string | "" | 混合源目录 |
| local_organize_target_base | string | "" | 目标根目录 |
| local_organize_mode | string | "hardlink" | hardlink/copy/move |
| local_organize_auto_scrape | boolean | true | 整理后自动刮削 |
| local_organize_max_workers | int | 5 | 并发线程数 |

### API 接口
| 接口 | 方法 | 说明 |
|------|------|------|
| /api/local_organize/status | GET | 获取配置和状态 |
| /api/local_organize/config | GET/POST | 配置管理 |
| /api/local_organize/start | POST | 手动触发全量整理 |
| /api/local_organize/monitor/start | POST | 启动监控 |
| /api/local_organize/monitor/stop | POST | 停止监控 |
| /api/local_organize/records | GET | 整理记录列表 |
| /api/local_organize/records/correct | POST | 手动纠错 |

### 触发模式
1. 手动触发 - 点击"立即整理"按钮
2. 监控模式 - 启动监控后自动处理新文件
3. 定时任务 - 复用任务中心

### 复用模块
- p115_sorting_rules - 分类规则
- p115_rename_config - 重命名配置
- p115_organize_records - 整理记录表

### WebUI
访问路径: `/local-organize` (在"整理记录"菜单下)