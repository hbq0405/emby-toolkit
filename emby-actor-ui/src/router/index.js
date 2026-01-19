// src/router/index.js

import { createRouter, createWebHistory } from 'vue-router';
import { useAuthStore } from '../stores/auth';

// --- 1. 导入所有页面组件 ---
import DatabaseStats from '../components/DatabaseStats.vue';
import ReviewList from '../components/ReviewList.vue';
import SchedulerSettingsPage from '../components/settings/SchedulerSettingsPage.vue';
import GeneralSettingsPage from '../components/settings/GeneralSettingsPage.vue';
import WatchlistPage from '../components/WatchlistPage.vue';
import TmdbCollectionsPage from '../components/TmdbCollectionsPage.vue';
import ActorSubscriptionPage from '../components/ActorSubscriptionPage.vue';
import ReleasesPage from '../components/ReleasesPage.vue';
import Login from '../components/Login.vue'; 
import RegisterPage from '../components/RegisterPage.vue';
import CoverGeneratorConfig from '../components/CoverGeneratorConfig.vue';
import UserManagementPage from '../components/UserManagementPage.vue';
import DiscoverPage from '../components/DiscoverPage.vue';
import UserCenterPage from '../components/UserCenterPage.vue'
import SetupPage from '../components/Setup.vue';
import NullbrPage from '../components/NullbrPage.vue';

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
    path: '/setup',
    name: 'Setup',
    component: SetupPage,
    meta: { 
      requiresAuth: false,
      public: true // 必须是公开的，否则死循环
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
    path: '/auto-tagging',
    name: 'AutoTaggingPage',
    component: () => import('../components/AutoTaggingPage.vue'),
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
    component: TmdbCollectionsPage,
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
  {
    path: '/nullbr',
    name: 'Nullbr',
    component: NullbrPage,
    meta: { 
      requiresAuth: true,
      adminOnly: true 
    },
  },
];

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
  scrollBehavior: () => ({ top: 0 })
});

// --- 3. 路由守卫 (核心修改) ---
router.beforeEach(async (to, from, next) => {
  const authStore = useAuthStore();

  // 1. 如果是公共页面 (Login, Setup, Register)，直接放行
  if (to.meta.public) {
    next();
    return;
  }

  // 2. 如果前端状态已知已登录，直接放行
  if (authStore.isLoggedIn) {
    next();
    return;
  }

  // 3. 前端状态未登录，向后端检查状态
  try {
    await authStore.checkAuthStatus();

    if (authStore.isLoggedIn) {
      next();
    } else {
      // 没登录，去登录页
      next({ name: 'Login' });
    }
  } catch (error) {
    // ★★★ 核心逻辑：捕获 SETUP_REQUIRED 错误 ★★★
    // 这个错误是由 auth.js 中的 checkAuthStatus 抛出的
    if (error.message === 'SETUP_REQUIRED') {
      // 如果目标已经是 Setup 页面，就不要再跳了，防止死循环（虽然上面 meta.public 已经挡了一层，但双重保险）
      if (to.name === 'Setup') {
        next();
      } else {
        next({ name: 'Setup' });
      }
    } else {
      // 其他错误（如网络错误、未登录），去登录页
      next({ name: 'Login' });
    }
  }
});

export default router;