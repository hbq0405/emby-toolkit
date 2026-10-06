# 本地开发

## 后端

- Python 版本：3.12 或更高版本。
- 安装开发依赖：

```powershell
python -m pip install -e ".[dev]"
```

- 准备 PostgreSQL，并设置 `DATABASE_URL`，或使用本项目 Compose 启动数据库。
- 运行迁移：

```powershell
etk-db
```

- 启动 API：

```powershell
etk-api
```

默认管理 API 监听 `5257`。生产路径、账号和外部服务不要直接复用到本地测试环境。

## 前端

```powershell
Push-Location frontend
npm install
npm run dev
Pop-Location
```

本地前端默认使用 Vite。开发时把 `/api` 请求代理到本地后端；构建命令为：

```powershell
Push-Location frontend
npm run build
Pop-Location
```

## Wiki

Wiki 是独立的 VitePress 项目：

```powershell
npm ci
npm run docs:dev
npm run docs:build
```

构建输出位于 `docs/.vitepress/dist/`，推送 `main` 分支的文档改动后由 GitHub Pages 工作流发布。
