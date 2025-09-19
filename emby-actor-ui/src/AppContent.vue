<!-- src/AppContent.vue -->
<template>
  <!-- 1. 如果当前路由需要后台布局，就显示 MainLayout -->
  <MainLayout 
    v-if="showMainLayout"
    :is-dark="isDarkTheme" 
    :selected-theme="selectedTheme"
    :task-status="backgroundTaskStatus"
    @update:is-dark="handleModeChange"
    @update:selected-theme="handleThemeChange"
    @edit-custom-theme="openThemeEditor"
  />
  <!-- 2. 否则，就显示一个简单的 router-view，用于公共页面（登录、注册） -->
  <router-view v-else />

  <!-- 3. 主题编辑器和加载动画的逻辑保持不变，它们是全局的 -->
  <ThemeEditor
    v-if="showThemeEditor"
    :show="showThemeEditor"
    :initial-theme="themeForEditor"
    :is-dark="isDarkTheme"
    @update:show="handleEditorClose"
    @save="handleSaveCustomTheme"
    @update:preview="handlePreviewUpdate"
    @delete-custom-theme="handleDeleteCustomTheme"
  />
  
  <!-- isReady 的逻辑也保持不变 -->
  <div v-if="!isReady" class="fullscreen-container">
    <n-spin size="large" />
  </div>
</template>

<script setup>
import { ref, watch, onBeforeUnmount, onMounted, computed, nextTick } from 'vue';
import { useRoute } from 'vue-router'; // <-- ★★★ 1. 引入 useRoute ★★★
import { useDialog, NSpin, useMessage } from 'naive-ui';
import { useAuthStore } from './stores/auth';
import MainLayout from './MainLayout.vue';
// Login 组件不再需要在这里导入，因为它由路由管理
// import Login from './components/Login.vue'; 
import ThemeEditor from './components/ThemeEditor.vue';
import { themes } from './theme.js';
import axios from 'axios';
import { cloneDeep } from 'lodash-es';

// --- ★★★ 2. 获取当前路由对象 ★★★ ---
const route = useRoute();

// --- ★★★ 3. 创建一个计算属性来判断是否需要显示后台布局 ★★★ ---
const showMainLayout = computed(() => {
  // 我们在路由元信息中添加一个 `public` 标志
  // 如果路由的 meta.public 为 true，就不显示后台布局
  return !route.meta.public;
});


// --- 下面所有的 script setup 内容，都和你原来的代码一模一样，原封不动地复制过来 ---
const authStore = useAuthStore();
const dialog = useDialog();
const message = useMessage();

const isDarkTheme = ref(localStorage.getItem('isDark') === 'true');
const selectedTheme = ref(localStorage.getItem('user-theme') || 'default');
const userCustomTheme = ref(null);
const showThemeEditor = ref(false);
const previewTheme = ref(null);
const isReady = ref(false);
const newThemeTemplate = ref(null);
const isOpeningEditor = ref(false);

const backgroundTaskStatus = ref({ is_running: false, current_action: '空闲' });
let statusIntervalId = null;

const app = document.getElementById('app');

const applyTheme = (themeKey, isDark) => {
  const root = document.documentElement;
  const themeMode = isDark ? 'dark' : 'light';
  let themeConfig;

  if (themeKey === 'custom') {
    if (!userCustomTheme.value || !userCustomTheme.value[themeMode]) {
      selectedTheme.value = 'default';
      localStorage.setItem('user-theme', 'default');
      themeConfig = themes.default[themeMode];
    } else {
      themeConfig = userCustomTheme.value[themeMode];
    }
  } else {
    themeConfig = themes[themeKey]?.[themeMode] || themes.default[themeMode];
  }

  app.dispatchEvent(new CustomEvent('update-naive-theme', { detail: themeConfig.naive }));
  for (const key in themeConfig.custom) {
    root.style.setProperty(key, themeConfig.custom[key]);
  }
  root.classList.remove('dark', 'light');
  root.classList.add(isDark ? 'dark' : 'light');
};

const themeForEditor = computed(() => {
    if (userCustomTheme.value) {
        return userCustomTheme.value;
    }
    return newThemeTemplate.value;
});

const openThemeEditor = () => { 
  if (userCustomTheme.value) {
    newThemeTemplate.value = null;
  }
  showThemeEditor.value = true; 
};

const handleThemeChange = (newTheme) => {
  if (newTheme !== 'custom' || (newTheme === 'custom' && userCustomTheme.value)) {
    selectedTheme.value = newTheme;
    localStorage.setItem('user-theme', newTheme);
    return;
  }

  if (newTheme === 'custom' && !userCustomTheme.value) {
    const currentTemplateSource = themes[selectedTheme.value] || themes.default;
    newThemeTemplate.value = {
        name: '自定义',
        light: cloneDeep(currentTemplateSource.light),
        dark: cloneDeep(currentTemplateSource.dark)
    };

    dialog.info({
      title: '欢迎来到主题设计工坊',
      content: `准备基于你当前的【${currentTemplateSource.name}】主题进行创作吗？`,
      positiveText: '立即设计',
      onPositiveClick: () => {
        isOpeningEditor.value = true;
        selectedTheme.value = 'custom';
        localStorage.setItem('user-theme', 'custom');
        openThemeEditor();
        nextTick(() => {
          isOpeningEditor.value = false;
        });
      }
    });
  }
};

const handleModeChange = (isDark) => {
  isDarkTheme.value = isDark;
  localStorage.setItem('isDark', String(isDark));
  app.dispatchEvent(new CustomEvent('update-dark-mode', { detail: isDark }));
};

const handleSaveCustomTheme = async (newThemeConfigForCurrentMode) => {
  try {
    const fullCustomTheme = cloneDeep(themeForEditor.value);
    if (isDarkTheme.value) {
        fullCustomTheme.dark = newThemeConfigForCurrentMode;
    } else {
        fullCustomTheme.light = newThemeConfigForCurrentMode;
    }
    await axios.post('/api/config/custom_theme', fullCustomTheme);
    userCustomTheme.value = fullCustomTheme;
    selectedTheme.value = 'custom';
    localStorage.setItem('user-theme', 'custom');
    showThemeEditor.value = false;
    previewTheme.value = null;
    newThemeTemplate.value = null;
    message.success('专属主题已保存！');
  } catch (error) { 
    message.error('保存自定义主题失败');
    console.error('保存自定义主题失败:', error); 
  }
};

const handleDeleteCustomTheme = async () => {
  try {
    await axios.delete('/api/config/custom_theme');
    userCustomTheme.value = null;
    selectedTheme.value = 'default';
    localStorage.setItem('user-theme', 'default');
    showThemeEditor.value = false;
    previewTheme.value = null;
    newThemeTemplate.value = null;
    message.success('自定义主题已删除，已切换回默认主题。');
  } catch (error) {
    message.error('删除自定义主题失败');
    console.error('删除自定义主题失败:', error);
  }
};

const handleEditorClose = (show) => {
    if (!show) {
        showThemeEditor.value = false;
        previewTheme.value = null;
        newThemeTemplate.value = null;
        applyTheme(selectedTheme.value, isDarkTheme.value);
    }
};

const handlePreviewUpdate = (themeConfig) => {
    previewTheme.value = themeConfig;
    app.dispatchEvent(new CustomEvent('update-naive-theme', { detail: themeConfig.naive }));
    const root = document.documentElement;
    for (const key in themeConfig.custom) {
        root.style.setProperty(key, themeConfig.custom[key]);
    }
};

watch([isDarkTheme, selectedTheme], ([isDark, themeKey]) => {
  if (isOpeningEditor.value) return; 
  if (!previewTheme.value) {
      applyTheme(themeKey, isDark);
  }
}, { deep: true });

watch(() => authStore.isLoggedIn, (isLoggedIn) => {
  if (isLoggedIn) {
    if (!statusIntervalId) {
      const fetchStatus = async () => {
        try {
          const response = await axios.get('/api/status');
          backgroundTaskStatus.value = response.data;
        } catch (error) { console.error('获取状态失败:', error); }
      };
      fetchStatus();
      statusIntervalId = setInterval(fetchStatus, 2000);
    }
  } else {
    if (statusIntervalId) { clearInterval(statusIntervalId); statusIntervalId = null; }
  }
}, { immediate: true });

onMounted(async () => {
  try {
    const response = await axios.get('/api/config');
    if (response.data.custom_theme && Object.keys(response.data.custom_theme).length > 0) {
      userCustomTheme.value = response.data.custom_theme;
    }
  } catch (error) {
    console.error("加载初始配置失败:", error);
  } finally {
    if (selectedTheme.value === 'custom' && !userCustomTheme.value) {
        selectedTheme.value = 'default';
        localStorage.setItem('user-theme', 'default');
    }
    applyTheme(selectedTheme.value, isDarkTheme.value);
    isReady.value = true;
  }
});

onBeforeUnmount(() => {
  if (statusIntervalId) clearInterval(statusIntervalId);
});
</script>