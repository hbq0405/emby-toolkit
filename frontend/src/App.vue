<!-- src/App.vue -->
<template>
  <n-config-provider :theme="isDarkTheme ? darkTheme : undefined" :theme-overrides="currentNaiveTheme" :locale="zhCN" :date-locale="dateZhCN">
    <n-message-provider placement="bottom-right">
      <n-dialog-provider>
        <AppContent />
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { NConfigProvider, NMessageProvider, NDialogProvider, darkTheme, zhCN, dateZhCN } from 'naive-ui';
import AppContent from './AppContent.vue';

const isDarkTheme = ref(localStorage.getItem('isDark') === 'true');
const currentNaiveTheme = ref({});

onMounted(() => {
    const app = document.getElementById('app');
    
    // 1. 监听来自 AppContent 的主题更新事件 (Naive UI 样式)
    app.addEventListener('update-naive-theme', (event) => {
        currentNaiveTheme.value = event.detail;
    });

    // 2. 监听来自 AppContent 的暗色模式切换事件
    app.addEventListener('update-dark-mode', (event) => {
        isDarkTheme.value = event.detail;
    });

    // ★★★ 3. 新增：在这里初始化全局卡片缩放 ★★★
    // 这样无论 AppContent 里的主题逻辑怎么变，这个缩放设置都会生效
    const savedScale = localStorage.getItem('global_card_scale');
    if (savedScale) {
      document.documentElement.style.setProperty('--card-scale', savedScale);
    } else {
      document.documentElement.style.setProperty('--card-scale', '1');
    }
});
</script>

<style>
/* 这里的样式是真正的“全局静态样式” */
html, body { height: 100vh; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; overflow: hidden; }
.fullscreen-container { display: flex; justify-content: center; align-items: center; height: 100vh; width: 100%; }
html.light .fullscreen-container { background-color: #f0f2f5; }
html.dark .fullscreen-container { background-color: #101014; }
</style>