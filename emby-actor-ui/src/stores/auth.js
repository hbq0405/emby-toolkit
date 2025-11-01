// src/stores/auth.js

import { defineStore } from 'pinia';
import { ref, computed } from 'vue'; // ★ 导入 computed
import axios from 'axios';

export const useAuthStore = defineStore('auth', () => {
  // --- State ---
  // ★ 1. 不再只存 username，而是存整个 user 对象和登录状态
  const isLoggedIn = ref(false);
  const user = ref({}); // { name: 'xxx', user_type: 'emby_user', is_admin: true, ... }

  // --- Getters (Computed Properties) ---
  // ★ 2. 创建一些方便的计算属性，让组件使用起来更简单
  const username = computed(() => user.value?.name || null);
  const userType = computed(() => user.value?.user_type || null);
  // ★ 智能判断是否为管理员：本地管理员 或 Emby管理员
  const isAdmin = computed(() => {
    if (userType.value === 'local_admin') return true;
    if (userType.value === 'emby_user' && user.value?.is_admin) return true;
    return false;
  });

  const canSubscribeWithoutReview = computed(() => user.value?.allow_unrestricted_subscriptions || false);

  // --- Actions ---
  async function checkAuthStatus() {
    try {
      const response = await axios.get('/api/status');
      isLoggedIn.value = response.data.logged_in;
      user.value = response.data.user || {};
      // ★★★ 如果后端明确说未登录，就抛出错误，让路由守卫能捕获到 ★★★
      if (!isLoggedIn.value) {
        throw new Error("User not logged in");
      }
    } catch (error) {
      console.error('检查认证状态失败:', error);
      // ★★★ 捕获到任何错误，都坚决地把状态设置为未登录 ★★★
      isLoggedIn.value = false;
      user.value = {};
      // 把错误继续抛出去
      throw error;
    }
  }

  async function login(credentials) {
    try {
      const response = await axios.post('/api/login', credentials);
      isLoggedIn.value = true;
      user.value = response.data.user || {};
    } catch (error) {
      // 登录失败时，确保状态被清理
      isLoggedIn.value = false;
      user.value = {};
      throw error; // 把错误继续抛出去给组件处理
    }
  }

  async function logout() {
    try {
      await axios.post('/api/logout');
    } catch (error) {
      console.error("登出时后端发生错误:", error);
    } finally {
      // 无论如何都清理前端状态
      isLoggedIn.value = false;
      user.value = {};
    }
  }

  // --- Return ---
  return {
    isLoggedIn,
    user,
    username, // 暴露计算属性
    userType, // 暴露计算属性
    isAdmin,  // 暴露计算属性
    canSubscribeWithoutReview,
    checkAuthStatus,
    login,
    logout,
  };
});