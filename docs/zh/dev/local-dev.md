# 本地开发

## 后端

- Python 版本参考 `Dockerfile`（3.12）。
- 安装依赖：`pip install -r requirements.txt`
- 启动服务：`python web_app.py`

数据目录默认写入 `local_data/`，配置文件为 `local_data/config.ini`。

## 前端

前端位于 `emby-actor-ui/`：

```bash
cd emby-actor-ui
npm install
npm run dev
```

构建产物会在 Docker 构建阶段打包并拷贝到 `/app/static/`。
