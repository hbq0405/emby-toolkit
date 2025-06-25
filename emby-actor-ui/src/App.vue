<!-- src/App.vue -->
<template>
  <div :class="isDarkTheme ? 'dark-mode' : 'light-mode'">
    <n-config-provider :theme="isDarkTheme ? darkTheme : undefined" :theme-overrides="themeOverridesComputed" :locale="zhCN" :date-locale="dateZhCN">
      <n-message-provider>
        
        <!-- 1. 如果需要登录，则显示全屏的登录页面 -->
        <div v-if="authStore.isAuthEnabled && !authStore.isLoggedIn" class="fullscreen-container">
          <Login />
        </div>

        <!-- 2. 否则，显示后台管理布局 -->
        <n-layout v-else style="height: 100vh;">
          <n-layout-header :bordered="false" class="app-header">
            <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
              <span>Emby 演员/角色中文化</span>
                <div style="display: flex; align-items: center; gap: 16px;">
                  <!-- 用户名下拉菜单 -->
                  <n-dropdown 
                    v-if="authStore.isAuthEnabled" 
                    trigger="hover" 
                    :options="userOptions" 
                    @select="handleUserSelect"
                  >
                    <div style="display: flex; align-items: center; cursor: pointer; gap: 4px;">
                      <span style="font-size: 14px;">欢迎, {{ authStore.username }}</span>
                      <!-- 可以加一个下拉小箭头图标，更美观 -->
                      <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24"><path fill="currentColor" d="m7 10l5 5l5-5z"></path></svg>
                    </div>
                  </n-dropdown>
                  <n-popconfirm
                    v-if="updateInfo.has_update"
                    @positive-click="triggerUpdate"
                    positive-text="立即重启更新"
                    negative-text="稍后"
                  >
                    <template #trigger>
                      <n-button type="warning" size="small" :loading="isUpdating" secondary strong>
                        更新到 {{ updateInfo.latest_version }}
                      </n-button>
                    </template>
                    确定要立即更新应用吗？服务将会短暂重启。
                  </n-popconfirm>

                  <span style="font-size: 12px; color: #999;">v{{ appVersion }}</span>
                  <n-switch v-model:value="isDarkTheme" size="small">
                    <template #checked>暗色</template>
                    <template #unchecked>亮色</template>
                  </n-switch>
                </div>
            </div>
          </n-layout-header>
          <n-layout has-sider>
            <n-layout-sider
              :bordered="false"
              collapse-mode="width"
              :collapsed-width="64"
              :width="240"
              show-trigger="arrow-circle"
              content-style="padding-top: 10px;"
              :native-scrollbar="false"
              :collapsed="collapsed"
              @update:collapsed="val => collapsed = val"
            >
              <n-menu
                :collapsed="collapsed"
                :collapsed-width="64"
                :collapsed-icon-size="22"
                :options="menuOptions"
                :value="activeMenuKey"
                @update:value="handleMenuUpdate"
              />
            </n-layout-sider>
            <n-layout-content
              class="app-main-content-wrapper"
              content-style="padding: 24px; transition: background-color 0.3s;"
              :style="{ backgroundColor: isDarkTheme ? (themeOverridesComputed?.common?.bodyColor || '#101014') : (themeOverridesComputed?.common?.bodyColor || '#f0f2f5') }"
              :native-scrollbar="false"
            >
              <div class="status-display-area" v-if="showStatusArea">
                <n-card size="small" :bordered="false" style="margin-bottom: 15px;">
                   <p style="margin: 0; font-size: 0.9em;">
                      <strong>任务状态:</strong>
                      <n-text :type="statusTypeComputed">{{ backgroundTaskStatus.current_action }}</n-text> -
                      <n-text :type="statusTypeComputed" :depth="2">{{ backgroundTaskStatus.message }}</n-text>
                      <n-progress
                          v-if="backgroundTaskStatus.is_running && backgroundTaskStatus.progress >= 0 && backgroundTaskStatus.progress < 100"
                          type="line"
                          :percentage="backgroundTaskStatus.progress"
                          :indicator-placement="'inside'"
                          processing
                          style="margin-top: 5px;"
                      />
                      <span v-else-if="backgroundTaskStatus.is_running"> ({{ backgroundTaskStatus.progress }}%)</span>
                   </p>
                </n-card>
              </div>
              <div class="page-content-inner-wrapper">
               <router-view v-slot="slotProps">
                <component :is="slotProps.Component" :task-status="backgroundTaskStatus" />
              </router-view>
              </div>
            </n-layout-content>
          </n-layout>
        </n-layout>
        <n-modal 
            v-model:show="showPasswordModal"
            preset="card"
            style="width: 90%; max-width: 500px;"
            title="修改密码"
            :bordered="false"
            size="huge"
          >
            <ChangePassword @password-changed="showPasswordModal = false" />
          </n-modal>

        </n-message-provider>
    </n-config-provider>
  </div>
</template>

<script setup>
import { ref, computed, h, onMounted, onBeforeUnmount, watch } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import {
  NConfigProvider, NLayout, NLayoutHeader, NLayoutSider, NLayoutContent,
  NMenu, NSwitch, NIcon, NCard, NText, NProgress,
  darkTheme, zhCN, dateZhCN, useMessage, NMessageProvider,
  NModal, NDropdown, useNotification, NBadge
} from 'naive-ui';
import axios from 'axios';
import { useAuthStore } from './stores/auth';
// [新增] 引入 useConfig 以获取全局配置
import { useConfig } from './composables/useConfig';
import Login from './components/Login.vue';
import ChangePassword from './components/settings/ChangePassword.vue';
import {
  PlayCircleOutline as ActionsIcon,
  ListOutline as ReviewListIcon,
  ServerOutline as EmbyIcon,
  KeyOutline as ApiIcon,
  TimerOutline as SchedulerIcon,
  OptionsOutline as GeneralIcon,
  LogOutOutline as LogoutIcon,
  HeartOutline as WatchlistIcon,
} from '@vicons/ionicons5';
import { Password24Regular as PasswordIcon } from '@vicons/fluent'

const router = useRouter(); 
const route = useRoute(); 
const authStore = useAuthStore();
// [新增] 调用 useConfig 获取配置模型
const { configModel } = useConfig();

// --- 状态定义 (Refs) ---
const showPasswordModal = ref(false);
const isDarkTheme = ref(localStorage.getItem('theme') !== 'light');
const collapsed = ref(false);
const backgroundTaskStatus = ref({ is_running: false, current_action: '空闲', message: '等待任务', progress: 0 });
const showStatusArea = ref(true);
const activeMenuKey = computed(() => route.name);
const appVersion = ref(__APP_VERSION__);
// ★★★ 新增：用于更新功能的状态 ★★★
const updateInfo = ref({ has_update: false, latest_version: null });
const isUpdating = ref(false);
// ★★★ 新增：检查更新的函数 ★★★
const checkForUpdates = async () => {
  // 如果用户未登录，则不检查更新
  if (!authStore.isLoggedIn) return;

  try {
    const response = await axios.get('/api/update_status');
    updateInfo.value = response.data;
    
    if (response.data.has_update) {
      notification.info({
        title: '发现新版本！',
        content: `检测到新版本 ${response.data.latest_version}。您可以点击右上角的按钮进行更新。`,
        duration: 15000, // 15秒后自动关闭
        closable: true,
      });
    }
  } catch (error) {
    console.error("检查更新失败:", error);
  }
};

// ★★★ 新增：触发更新的函数 ★★★
const triggerUpdate = async () => {
  isUpdating.value = true;
  try {
    const response = await axios.post('/api/actions/trigger_app_update');
    message.success(response.data.message || '更新请求已发送，请稍候...');
    // 这里可以进一步优化，比如显示一个全屏的遮罩层，提示用户“正在更新，请勿操作，页面稍后会自动刷新”
  } catch (error) {
    message.error(error.response?.data?.error || '发送更新请求失败！');
    isUpdating.value = false;
  }
};
watch(isDarkTheme, (newValue) => {
  localStorage.setItem('theme', newValue ? 'dark' : 'light');
});

const renderIcon = (iconComponent) => {
  return () => h(NIcon, null, { default: () => h(iconComponent) });
};

// --- 用户下拉菜单的逻辑 ---
const userOptions = computed(() => [
  {
    label: '修改密码',
    key: 'change-password',
    icon: renderIcon(PasswordIcon)
  },
  {
    label: '退出登录',
    key: 'logout',
    icon: renderIcon(LogoutIcon)
  }
]);

const handleUserSelect = async (key) => {
  if (key === 'change-password') {
    showPasswordModal.value = true;
  } else if (key === 'logout') {
    await authStore.logout();
    router.push({ name: 'Login' });
  }
};

// --- [修改] 侧边栏菜单的定义，使其动态化 ---
const menuOptions = computed(() => {
  // 基础菜单项，始终显示
  const options = [
    { label: 'Emby 配置', key: 'settings-emby', icon: renderIcon(EmbyIcon) },
    { label: '通用设置', key: 'settings-general', icon: renderIcon(GeneralIcon) },
    { type: 'divider', key: 'd1' },
    { label: '任务中心', key: 'actions-status', icon: renderIcon(ActionsIcon) },
    { label: '手动处理', key: 'ReviewList', icon: renderIcon(ReviewListIcon) },
  ];

  // 仅当配置加载完成且神医Pro模式开启时，才显示相关菜单
  // 我们需要检查 configModel.value 是否存在，以避免在初始加载时出错
  if (configModel.value && configModel.value.use_sa_mode) {
    options.push(
      { label: '追剧列表', key: 'Watchlist', icon: renderIcon(WatchlistIcon) }
    );
  }

  // 添加剩余的固定菜单项
  options.push(
    { label: '定时任务', key: 'settings-scheduler', icon: renderIcon(SchedulerIcon) }
  );

  return options;
});


// --- 菜单点击事件处理 ---
async function handleMenuUpdate(key) {
  router.push({ name: key });
}

const statusTypeComputed = computed(() => 'info');

const fetchStatus = async () => {
  try {
    const response = await axios.get('/api/status');
    backgroundTaskStatus.value = response.data;
  } catch (error) {
    if (error.response?.status !== 401) {
      console.error('获取状态失败:', error);
    }
  }
};

const themeOverridesComputed = computed(() => {
  const lightCardShadow = '0 1px 2px -2px rgba(0, 0, 0, 0.08), 0 3px 6px 0 rgba(0, 0, 0, 0.06), 0 5px 12px 4px rgba(0, 0, 0, 0.04)';
  const darkCardShadow = '0 1px 2px -2px rgba(0, 0, 0, 0.24), 0 3px 6px 0 rgba(0, 0, 0, 0.18), 0 5px 12px 4px rgba(0, 0, 0, 0.12)';

  if (!isDarkTheme.value) {
    return {
      common: { bodyColor: '#f0f2f5' },
      Card: { boxShadow: lightCardShadow }
    };
  }
  
  return {
    common: { 
      bodyColor: '#101014', 
      cardColor: '#1a1a1e', 
      inputColor: '#1a1a1e', 
      actionColor: '#242428', 
      borderColor: 'rgba(255, 255, 255, 0.12)' 
    },
    Card: { 
      color: '#1a1a1e', 
      titleTextColor: 'rgba(255, 255, 255, 0.92)',
      boxShadow: darkCardShadow,
    },
    DataTable: { 
      tdColor: '#1a1a1e', 
      thColor: '#1a1a1e', 
      tdColorStriped: '#202024' 
    },
    Input: { color: '#1a1a1e' },
    Select: { peers: { InternalSelection: { color: '#1a1a1e' } } }
  };
});

let statusIntervalId = null;

onMounted(() => {
  if (authStore.isLoggedIn) {
    fetchStatus();
    statusIntervalId = setInterval(fetchStatus, 1000);
  }
  checkForUpdates();
  setInterval(checkForUpdates, 6 * 60 * 60 * 1000); // 每6小时检查一次
});

onBeforeUnmount(() => {
  if (statusIntervalId) clearInterval(statusIntervalId);
});

watch(() => authStore.isLoggedIn, (newIsLoggedIn) => {
  if (newIsLoggedIn) {
    if (!statusIntervalId) {
      fetchStatus();
      statusIntervalId = setInterval(fetchStatus, 1000);
    }
  } else {
    if (statusIntervalId) {
      clearInterval(statusIntervalId);
      statusIntervalId = null;
    }
  }
  if (newIsLoggedIn) {
        checkForUpdates(); // 登录后也检查一次更新
    } else {
        updateInfo.value = { has_update: false, latest_version: null }; // 退出登录后重置更新状态
    }
});
</script>

<style>
html, body { height: 100vh; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; overflow: hidden; }
.app-header { padding: 0 24px; height: 60px; display: flex; align-items: center; font-size: 1.25em; font-weight: 600; flex-shrink: 0; }
.status-display-area .n-card .n-card__content { padding: 8px 12px !important; }
.status-display-area p { margin: 0; font-size: 0.9em; }
.app-main-content-wrapper { height: 100%; display: flex; flex-direction: column; }
.status-display-area { flex-shrink: 0; }
.page-content-inner-wrapper { flex-grow: 1; overflow-y: auto; }
.fullscreen-container { display: flex; justify-content: center; align-items: center; height: 100vh; width: 100%; background-color: #f0f2f5; }
.dark-mode .fullscreen-container { background-color: #101014; }
</style>