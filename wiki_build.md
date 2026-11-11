# ETKN 文档站本地开发与发布说明

用户文档继续由公开仓库 `hbq0405/emby-toolkit` 的 `main` 分支承载。新版私有源码不随文档发布；只提交用户指南、主题和站点配置，保留现有 `/emby-toolkit/` 基路径与教程 URL。

`docs/ai-context/`、`docs/zh/architecture/`、`docs/zh/data/` 和 `docs/zh/dev/` 已排除构建及搜索索引。发布前检查构建产物与链接，不将内部开发上下文复制到公开指南。

本文档说明如何在本地构建/预览 Wiki，以及 GitHub Pages 发布所需设置。

## 本地开发

在仓库根目录执行：

```bash
npm install
npm run docs:dev
```

然后访问终端提示的地址（通常为 `http://localhost:5173/`）。

## 本地构建与预览

```bash
npm run docs:build
npm run docs:preview
```

由于站点配置了 `base: /emby-toolkit/`，预览时请访问：

```
http://localhost:4173/emby-toolkit/
```

## 发布设置（GitHub Pages）

已配置工作流：`.github/workflows/deploy.yml`。

需要在 GitHub 仓库设置中确认：

1. 进入 `Settings` → `Pages`。
2. `Build and deployment` 的 `Source` 选择 **GitHub Actions**。
3. 确保主分支为 `main`（或同步修改工作流触发分支）。

发布触发条件：

- `docs/**`、`package.json`、`package-lock.json` 或工作流文件变更。
- 手动触发 `workflow_dispatch`。

站点基路径：

- 已在 `docs/.vitepress/config.mts` 设置 `base: /emby-toolkit/`。
- 若仓库名或 Pages 路径变化，请同步修改该配置。

## 发布前验证

运行 `npm run docs:build` 和 `git diff --check`，再检查桌面/手机、明暗主题、教程筛选及全文搜索。构建会校验 Markdown 站内链接；主题 Vue 中的链接还需检查静态目标及实际点击。

没有本轮明确的推送授权时，只创建本地提交。获得授权后再推送公开仓库 `main`，等待 Pages 工作流完成，并核对线上首页、教程与搜索。只改文档不应触发旧版 `dev` 分支的 Docker 构建。
