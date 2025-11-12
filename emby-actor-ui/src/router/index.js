// src/router/index.js

import { createRouter, createWebHistory } from 'vue-router';
import { useAuthStore } from '../stores/auth';

// --- 1. 导入所有页面组件 ---
import DatabaseStats from '../components/DatabaseStats.vue';
import ReviewList from '../components/ReviewList.vue';
import SchedulerSettingsPage from '../components/settings/SchedulerSettingsPage.vue';
import GeneralSettingsPage from '../components/settings/GeneralSettingsPage.vue';
import WatchlistPage from '../components/WatchlistPage.vue';
import CollectionsPage from '../components/CollectionsPage.vue';
import ActorSubscriptionPage from '../components/ActorSubscriptionPage.vue';
import ReleasesPage from '../components/ReleasesPage.vue';
import Login from '../components/Login.vue'; 
import RegisterPage from '../components/RegisterPage.vue';
import CoverGeneratorConfig from '../components/CoverGeneratorConfig.vue';
import UserManagementPage from '../components/UserManagementPage.vue';
import DiscoverPage from '../components/DiscoverPage.vue';
import UserCenterPage from '../components/UserCenterPage.vue'

// --- 2. 定义路由规则 (带 meta.public 标签) ---
const routes = [
  {
    path: '/login',
    name: 'Login',
    component: Login,
    meta: { 
      requiresAuth: false,
      public: true // <-- ★★★ 在这里打上“公共页面”标签 ★★★
    },
  },
  {
    path: '/register/invite/:token',
    name: 'Register',
    component: RegisterPage,
    props: true,
    meta: { 
      requiresAuth: false,
      public: true // <-- ★★★ 在这里也打上“公共页面”标签 ★★★
    },
  },
  {
    path: '/',
    redirect: '/DatabaseStats' 
  },
  // --- 下面所有后台页面的路由，保持原样，不需要加 public 标签 ---
  {
    path: '/DatabaseStats',
    name: 'DatabaseStats',
    component: DatabaseStats,
    meta: { requiresAuth: true },
  },
  {
    path: '/review',
    name: 'ReviewList',
    component: ReviewList,
    meta: { requiresAuth: true },
  },
  {
    path: '/settings/scheduler',
    name: 'settings-scheduler',
    component: SchedulerSettingsPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/settings/general',
    name: 'settings-general',
    component: GeneralSettingsPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/watchlist',
    name: 'Watchlist',
    component: WatchlistPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/collections',
    name: 'Collections',
    component: CollectionsPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/custom-collections',
    name: 'CustomCollectionsManager',
    component: () => import('../components/CustomCollectionsManager.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/edit-media/:itemId',
    name: 'MediaEditPage',
    component: () => import('../components/MediaEditPage.vue'),
    props: true,
    meta: { requiresAuth: true },
  },
  { 
    path: '/actor-subscriptions',
    name: 'ActorSubscriptions',
    component: ActorSubscriptionPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/releases',
    name: 'Releases',
    component: ReleasesPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/settings/cover-generator',
    name: 'CoverGeneratorConfig',
    component: CoverGeneratorConfig,
    meta: { requiresAuth: true },
  },
  {
    path: '/resubscribe',
    name: 'ResubscribePage',
    component: () => import('../components/ResubscribePage.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/media-cleanup',
    name: 'MediaCleanupPage',
    component: () => import('../components/MediaCleanupPage.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/user-management',
    name: 'UserManagement',
    component: UserManagementPage,
    meta: { requiresAuth: true },
  },
  {
    path: '/unified-subscriptions',
    name: 'UnifiedSubscriptions',
    component: () => import('../components/UnifiedSubscriptionsPage.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/user-center',
    name: 'UserCenter',
    component: UserCenterPage,
    meta: { 
      requiresAuth: true // 这个页面必须登录才能访问
    },
  },
  {
    path: '/discover',
    name: 'Discover',
    component: DiscoverPage,
    meta: { requiresAuth: true }, // 必须登录才能访问
  },
];

// --- 3, 4, 5. 创建实例、路由守卫、导出 (保持不变) ---
const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
  scrollBehavior: () => ({ top: 0 })
});

router.beforeEach(async (to, from, next) => {
  const authStore = useAuthStore();

  // 规则1: 如果要去的是公共页面 (我们用 !requiresAuth 判断)，直接放行
  if (!to.meta.requiresAuth) {
    next();
    return;
  }

  // 规则2: 如果要去的是受保护页面，我们必须先确定身份
  // 如果前端状态已知是已登录，直接放行 (这是为了优化，避免每次都请求后端)
  if (authStore.isLoggedIn) {
    next();
    return;
  }

  // 规则3: 如果前端状态是未登录 (比如刚刷新页面)，必须向后端验证
  try {
    // 我们等待 checkAuthStatus 执行完毕
    await authStore.checkAuthStatus();

    // 验证完毕后，再次检查 store 的状态
    if (authStore.isLoggedIn) {
      // 如果后端说你确实登录了，放行
      next();
    } else {
      // 如果后端说你没登录，踢回登录页
      next({ name: 'Login' });
    }
  } catch (error) {
    // 如果 checkAuthStatus 本身就出错了 (比如网络问题或403)，
    // 说明 session 肯定无效，同样踢回登录页
    next({ name: 'Login' });
  }
});

export default router;