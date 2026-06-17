// src/stores/app.js

import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import axios from 'axios';

export const useAppStore = defineStore('app', () => {
  // --- State ---
  const currentVersion = ref('');
  const latestVersion = ref('');
  const releases = ref([]);

  // --- Getters (Computed) ---
  const isUpdateAvailable = computed(() => {
  // 1. 确保两个版本号都已获取
  if (!latestVersion.value || !currentVersion.value) {
    return false;
  }

  // 2. 规范化版本号：去掉可能存在的 'v' 前缀和首尾空格
  const normalizedLatest = latestVersion.value.replace(/^v/, '').trim();
  const normalizedCurrent = currentVersion.value.replace(/^v/, '').trim();

  // (可选) 添加一个 console.log 来调试
  // console.log(`版本比较: 最新='${normalizedLatest}', 当前='${normalizedCurrent}', 是否不同: ${normalizedLatest !== normalizedCurrent}`);

  // 3. 比较规范化后的版本号
  return normalizedLatest !== normalizedCurrent;
});

  // --- Actions ---
  async function fetchVersionInfo() {
    try {
      const response = await axios.get('/api/system/about_info');
      currentVersion.value = response.data.current_version;
      releases.value = response.data.releases;
      
      // 最新版本就是 release 列表的第一个
      if (response.data.releases && response.data.releases.length > 0) {
        latestVersion.value = response.data.releases[0].version;
      }
    } catch (error) {
      console.error('Failed to fetch version info:', error);
    }
  }

  return {
    currentVersion,
    latestVersion,
    releases,
    isUpdateAvailable,
    fetchVersionInfo,
  };
});