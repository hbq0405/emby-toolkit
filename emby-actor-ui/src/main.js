// src/main.js

import { createApp } from 'vue';
import { createPinia } from 'pinia';
import Provider from './Provider.vue';
import router from './router';
import naive from 'naive-ui';
import { useAuthStore } from './stores/auth'; // ★★★ 1. 导入你的 auth store ★★★

// 全局样式
import './assets/global.css';

// --- 创建核心实例 ---
const pinia = createPinia();
const app = createApp(Provider);

// --- 严格按照顺序注册插件 ---
app.use(pinia);
// 注意：Router 的注册要移到下面，确保在 store 准备好之后

// --- ★★★ 核心修复：异步启动流程 ★★★
async function initializeApp() {
  // 2. 在应用启动的最初阶段，获取 auth store 实例
  const authStore = useAuthStore();

  try {
    // 3. 异步调用 checkAuthStatus，并等待它完成
    // 这会向后端 /api/auth/status 发送请求，并用权威结果更新 store
    await authStore.checkAuthStatus();
    console.log('认证状态已从后端同步完毕。');

  } catch (error) {
    // 如果连初始状态都获取失败（比如后端没开），可以在这里处理
    console.error('应用初始化失败：无法获取初始认证状态。');
    // 此时 authStore.initializationError 会被设置，你可以在 App.vue 中显示一个全局错误提示
  } finally {
    // 4. 无论 checkAuthStatus 成功还是失败，都要继续挂载应用
    // 因为路由守卫会根据 store 的最新状态（无论是成功获取的还是失败后重置的）来决定如何跳转
    app.use(router);
    app.use(naive);
    app.mount('#app');
    console.log('应用已挂载。');
  }
}

// 5. 调用异步启动函数
initializeApp();