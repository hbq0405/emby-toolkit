export const categories = [
  { title: '开始使用', note: '从部署到第一部影片', items: [
    ['快速开始', 'quick-start', '安装 新手 首次 本地 云盘'],
    ['Docker 部署', 'docker', 'Compose NAS Unraid 升级 端口 8097'],
    ['首次配置', 'first-run', '登录 管理员 115 TMDb 授权'],
    ['配置项总览', 'config', '基础设置 网络 AI 数据源']
  ] },
  { title: '观看与聆听', note: '网页、电视与音乐', items: [
    ['网页客户端', 'web-client', 'Web 浏览器 海报墙 搜索 收藏 画质 音轨'],
    ['ETKN TV', 'tv-client', '安卓 电视 Android 遥控器 APK 原盘 ISO BDMV'],
    ['音乐库与歌单', 'music', '专辑 歌手 歌曲 文件夹 随机 循环'],
    ['内网缓存与转码', 'playback-cache', '卡顿 QSV 硬件 HDR 缓冲 磁盘'],
    ['字幕与 AI 翻译', 'subtitles', '中文 简体 繁体 双语 外挂 PGS ASS'],
    ['弹幕', 'danmaku', '御坂 自定义 API 弹弹 B站'],
    ['播放记录与统计', 'playback', '正在播放 历史 会话 302 来源']
  ] },
  { title: '资源与订阅', note: '找到想看，持续追更', items: [
    ['共享资源中心', 'shared-resource', '秒传 转存 一致版 共享池 定向推送 贡献点'],
    ['订阅中心', 'subscribe-assistant', '榜单 需求 来源 MoviePilot MP'],
    ['智能追剧', 'watchlist', '缺集 完结 片头 片尾'],
    ['演员订阅', 'actor-subscriptions', '人物 参演 作品'],
    ['115 网盘与整理', 'p115', '授权 Cookie OpenAPI STRM 分享入库'],
    ['洗版功能', 'washing', '优先级 版本 替换 质量']
  ] },
  { title: '管理媒体库', note: '有序归档，轻松查找', items: [
    ['媒体库、分组与分面', 'media-management', '原生 分组 配额 封面 题材 年份 演员 编辑'],
    ['虚拟媒体库', 'custom-collections', '榜单 规则 推荐 指定路径 家庭视频'],
    ['封面与增强功能', 'enhancements', '生成 上传 自动打标 去重 分享入库'],
    ['整理记录', 'organize-records', '纠错 重组 识别 未识别']
  ] },
  { title: '自动化与维护', note: '让日常流程自己运转', items: [
    ['Web 控制台', 'web-ui', '后台 菜单 管理 个人中心'],
    ['任务与调度', 'scheduler', '日志 CRON 定时 重试 智能排查'],
    ['实时监控', 'monitor', '目录 扫描 自动'],
    ['事件与 Webhook', 'webhook', 'MoviePilot 通知 事件'],
    ['外部服务集成', 'integrations', 'Telegram TG Bot re0 TMDb AI'],
    ['用户与权限', 'user-management', '邀请 模板 账号 个人 偏好'],
    ['外部反向代理', 'reverse-proxy', 'Nginx HTTPS 域名 公网']
  ] }
]

export const journeys = [
  { number: '01', title: '第一次使用', text: '部署、连接来源，完成第一次整理与播放。', link: 'quick-start', label: '从这里开始' },
  { number: '02', title: '我想直接看片', text: '在浏览器或电视上打开片库，接着上次看。', link: 'web-client', label: '选择播放方式' },
  { number: '03', title: '让片库自动更新', text: '接入共享资源、订阅追更，再交给任务中心。', link: 'subscribe-assistant', label: '配置自动追更' },
  { number: '04', title: '我遇到了问题', text: '连接、入库、播放，按现象找到排查顺序。', link: '../faq/troubleshooting', label: '查看排查指南' }
]

export const guideLink = (slug: string) => (slug.startsWith('../') ? '/zh/' + slug.slice(3) : '/zh/guide/' + slug) + '.html'
