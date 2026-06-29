import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Emby Toolkit Wiki',
  description: 'Emby Toolkit 项目文档',
  base: '/emby-toolkit/',
  ignoreDeadLinks: true,

  head: [
    ['meta', { name: 'theme-color', content: '#2563eb' }],
    ['meta', { name: 'og:type', content: 'website' }],
    ['meta', { name: 'og:locale', content: 'zh_CN' }],
    ['meta', { name: 'og:site_name', content: 'Emby Toolkit Wiki' }]
  ],

  themeConfig: {
    siteTitle: 'Emby Toolkit Wiki',

    nav: [
      { text: '首页', link: '/zh/' },
      { text: '快速开始', link: '/zh/guide/quick-start' },
      { text: '部署', link: '/zh/guide/docker' },
      { text: '配置', link: '/zh/guide/config' },
      { text: '共享资源', link: '/zh/guide/shared-resource' },
      { text: '洗版', link: '/zh/guide/washing' },
      { text: 'FAQ', link: '/zh/faq/troubleshooting' }
    ],

    sidebar: {
      '/zh/': [
        {
          text: '项目介绍',
          items: [
            { text: '项目概览', link: '/zh/introduction/what-is' },
            { text: '功能特色', link: '/zh/introduction/features' }
          ]
        },
        {
          text: '部署与配置',
          items: [
            { text: '快速开始', link: '/zh/guide/quick-start' },
            { text: 'Docker 部署', link: '/zh/guide/docker' },
            { text: '首次配置', link: '/zh/guide/first-run' },
            { text: '配置项总览', link: '/zh/guide/config' }
          ]
        },
        {
          text: '使用指南',
          items: [
            { text: 'Web 控制台', link: '/zh/guide/web-ui' },
            { text: '115 网盘', link: '/zh/guide/p115' },
            { text: '共享资源中心', link: '/zh/guide/shared-resource' },
            { text: '洗版功能', link: '/zh/guide/washing' },
            { text: '任务与调度', link: '/zh/guide/scheduler' },
            { text: '实时监控', link: '/zh/guide/monitor' },
            { text: 'Webhook 接入', link: '/zh/guide/webhook' },
            { text: '智能追剧', link: '/zh/guide/watchlist' },
            { text: '订阅助手增强版', link: '/zh/guide/subscribe-assistant' },
            { text: '演员订阅', link: '/zh/guide/actor-subscriptions' },
            { text: '自建合集', link: '/zh/guide/custom-collections' },
            { text: '封面生成', link: '/zh/guide/cover-generator' },
            { text: '外部服务集成', link: '/zh/guide/integrations' },
            { text: '用户与权限', link: '/zh/guide/user-management' },
            { text: '反向代理与虚拟库', link: '/zh/guide/reverse-proxy' }
          ]
        },
        {
          text: '常见问题',
          items: [
            { text: '故障排查', link: '/zh/faq/troubleshooting' }
          ]
        }
      ]
    },

    footer: {
      message: 'Emby Toolkit - Emby 增强管理工具',
      copyright: 'Copyright © 2026-present Emby Toolkit'
    },

    search: {
      provider: 'local',
      options: {
        locales: {
          zh: {
            translations: {
              button: {
                buttonText: '搜索文档',
                buttonAriaLabel: '搜索文档'
              },
              modal: {
                noResultsText: '无法找到相关结果',
                resetButtonTitle: '清除查询条件',
                footer: {
                  selectText: '选择',
                  navigateText: '切换'
                }
              }
            }
          }
        }
      }
    },

    editLink: {
      pattern: 'https://github.com/hbq0405/emby-toolkit/edit/main/docs/:path',
      text: '在 GitHub 上编辑此页'
    },

    lastUpdated: {
      text: '最后更新于',
      formatOptions: {
        dateStyle: 'short',
        timeStyle: 'medium'
      }
    },

    docFooter: {
      prev: '上一页',
      next: '下一页'
    },

    outline: {
      label: '页面导航'
    },

    returnToTopLabel: '回到顶部',
    sidebarMenuLabel: '菜单',
    darkModeSwitchLabel: '主题',
    lightModeSwitchTitle: '切换到浅色模式',
    darkModeSwitchTitle: '切换到深色模式'
  },

  locales: {
    root: {
      label: '简体中文',
      lang: 'zh-CN',
      link: '/zh/'
    }
  }
})
