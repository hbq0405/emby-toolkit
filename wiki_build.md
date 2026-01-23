# Wiki 本地开发与发布说明

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

已配置工作流：`.github/workflows/docs-deploy.yml`。

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
