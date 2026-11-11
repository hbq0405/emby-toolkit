import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'ETKN 文档',
  description: '从资源获取、自动整理到网页与电视播放，搭建你的私人媒体库。',
  lang: 'zh-CN',
  base: '/emby-toolkit/',
  appearance: 'dark',
  lastUpdated: true,
  srcExclude: ['ai-context/**', 'zh/architecture/**', 'zh/data/**', 'zh/dev/**'],
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/emby-toolkit/logo.svg' }],
    ['meta', { name: 'theme-color', content: '#101116' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:locale', content: 'zh_CN' }],
    ['meta', { property: 'og:site_name', content: 'ETKN 文档' }]
  ],
  themeConfig: {
    siteTitle: 'ETKN',
    logo: '/logo.svg',
    nav: [
      { text: '首页', link: '/zh/' },
      { text: '使用教程', link: '/zh/guide/', activeMatch: '/zh/guide/' },
      { text: '功能一览', link: '/zh/introduction/features' },
      { text: '常见问题', link: '/zh/faq/troubleshooting' },
      { text: '开始部署', link: '/zh/guide/docker' }
    ],
    sidebar: {
      '/zh/': [
        { text: '认识 ETKN', items: [
          { text: '全部教程', link: '/zh/guide/' },
          { text: '项目概览', link: '/zh/introduction/what-is' },
          { text: '功能一览', link: '/zh/introduction/features' }
        ] },
        { text: '01 · 开始使用', items: [
          { text: '快速开始', link: '/zh/guide/quick-start' },
          { text: 'Docker 部署', link: '/zh/guide/docker' },
          { text: '首次配置', link: '/zh/guide/first-run' },
          { text: '配置项总览', link: '/zh/guide/config' }
        ] },
        { text: '02 · 观看与聆听', items: [
          { text: '网页客户端', link: '/zh/guide/web-client' },
          { text: 'ETKN TV', link: '/zh/guide/tv-client' },
          { text: '音乐库与歌单', link: '/zh/guide/music' },
          { text: '内网缓存与转码', link: '/zh/guide/playback-cache' },
          { text: '字幕与 AI 翻译', link: '/zh/guide/subtitles' },
          { text: '弹幕', link: '/zh/guide/danmaku' },
          { text: '播放记录与统计', link: '/zh/guide/playback' }
        ] },
        { text: '03 · 资源与订阅', collapsed: false, items: [
          { text: '共享资源中心', link: '/zh/guide/shared-resource' },
          { text: '订阅中心', link: '/zh/guide/subscribe-assistant' },
          { text: '智能追剧', link: '/zh/guide/watchlist' },
          { text: '演员订阅', link: '/zh/guide/actor-subscriptions' },
          { text: '115 网盘与整理', link: '/zh/guide/p115' },
          { text: '洗版功能', link: '/zh/guide/washing' }
        ] },
        { text: '04 · 管理媒体库', collapsed: true, items: [
          { text: '媒体库、分组与分面', link: '/zh/guide/media-management' },
          { text: '虚拟媒体库', link: '/zh/guide/custom-collections' },
          { text: '封面与增强功能', link: '/zh/guide/enhancements' },
          { text: '整理记录', link: '/zh/guide/organize-records' }
        ] },
        { text: '05 · 自动化与维护', collapsed: true, items: [
          { text: 'Web 控制台', link: '/zh/guide/web-ui' },
          { text: '任务与调度', link: '/zh/guide/scheduler' },
          { text: '实时监控', link: '/zh/guide/monitor' },
          { text: '事件与 Webhook', link: '/zh/guide/webhook' },
          { text: '外部服务集成', link: '/zh/guide/integrations' },
          { text: '用户与权限', link: '/zh/guide/user-management' },
          { text: '外部反向代理', link: '/zh/guide/reverse-proxy' }
        ] },
        { text: '需要帮助？', items: [{ text: '常见问题与故障排查', link: '/zh/faq/troubleshooting' }] }
      ]
    },
    footer: { message: 'ETKN · 让媒体库回到观看本身', copyright: '© 2026 ETKN · 用户文档' },
    search: {
      provider: 'local',
      options: { translations: {
        button: { buttonText: '搜索教程', buttonAriaLabel: '搜索教程' },
        modal: {
          noResultsText: '没有找到相关教程', resetButtonTitle: '清空搜索',
          backButtonTitle: '返回', displayDetails: '显示详细结果',
          footer: { selectText: '选择', navigateText: '切换', closeText: '关闭' }
        }
      } }
    },
    editLink: { pattern: 'https://github.com/hbq0405/emby-toolkit/edit/main/docs/:path', text: '在 GitHub 上完善此页' },
    lastUpdated: { text: '最后更新', formatOptions: { dateStyle: 'short' } },
    docFooter: { prev: '上一页', next: '下一页' },
    outline: { label: '本页内容', level: [2, 3] },
    returnToTopLabel: '回到顶部', sidebarMenuLabel: '教程目录', darkModeSwitchLabel: '主题',
    lightModeSwitchTitle: '切换到浅色模式', darkModeSwitchTitle: '切换到深色模式'
  }
})
