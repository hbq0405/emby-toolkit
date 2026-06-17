// vite.config.js
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { VitePWA } from 'vite-plugin-pwa'
import { version } from './package.json'

export default defineConfig({
  plugins: [
    vue(),
    VitePWA({
      registerType: 'autoUpdate',
      includeAssets: ['favicon.ico', 'apple-touch-icon.png'],
      manifest: {
        name: 'Emby Toolkit',
        short_name: 'Emby Toolkit',
        description: 'Emby 媒体服务器增强管理工具',
        theme_color: '#1a1a2e',
        background_color: '#1a1a2e',
        display: 'standalone',
        start_url: '/',
        icons: [
          {
            src: 'pwa-192x192.png',
            sizes: '192x192',
            type: 'image/png'
          },
          {
            src: 'pwa-512x512.png',
            sizes: '512x512',
            type: 'image/png'
          },
          {
            src: 'pwa-512x512.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'maskable'
          }
        ]
      },
      workbox: {
        maximumFileSizeToCacheInBytes: 4 * 1024 * 1024,
        globPatterns: ['**/*.{js,css,html,ico,png,svg,woff2,ttf,otf}'],
        navigateFallback: '/index.html',
        runtimeCaching: [
          {
            urlPattern: /^https?:\/\/.*\/api\/.*/i,
            handler: 'NetworkFirst',
            options: {
              cacheName: 'api-cache',
              expiration: {
                maxEntries: 50,
                maxAgeSeconds: 60 * 60
              },
              networkTimeoutSeconds: 5
            }
          }
        ]
      }
    })
  ],
  define: {
    __APP_VERSION__: JSON.stringify(version)
  },
  server: {
    proxy: {
      // API 代理保持不变
      '/api': {
        target: 'http://localhost:5257',
        changeOrigin: true,
      },
      
      // ★★★ START: 3. 新增对 /image_proxy 的代理 ★★★
      // 这个规则专门用于代理图片请求
      '/image_proxy': {
        target: 'http://localhost:5257', // 目标仍然是我们的 Python 后端
        changeOrigin: true,
        // 这里不需要路径重写，因为后端的路由就是 /image_proxy/...
      }
      // ★★★ END: 3. ★★★
    }
  }
})
