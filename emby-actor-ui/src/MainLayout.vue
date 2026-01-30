<!-- src/MainLayout.vue -->
<template>
  <n-layout style="height: 100vh; position: relative;">
    <n-layout-header :bordered="false" class="app-header">
      <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
        
        <!-- 左侧：Logo 与 菜单按钮 -->
        <div style="display: flex; align-items: center;">
          <!-- 移动端显示的汉堡菜单按钮 -->
          <n-button 
            v-if="isMobile" 
            text 
            style="font-size: 24px; margin-right: 12px;" 
            @click="collapsed = !collapsed"
          >
            <n-icon :component="MenuOutline" />
          </n-button>

          <span class="text-effect">
            <img
              :src="logo"
              alt="Logo"
              style="height: 1.5em; vertical-align: middle; margin-right: 0.3em;"
            />
            <span v-if="!isMobile || !collapsed">Emby Toolkit</span>
          </span>
        </div>

        <!-- 中间：任务状态 (仅桌面端显示) -->
        <div 
          v-if="!isMobile && authStore.isAdmin && props.taskStatus && props.taskStatus.current_action !== '空闲' && props.taskStatus.current_action !== '无'"
          class="header-task-status"
        >
          <div class="status-content">
            <n-text class="status-text">
              <n-spin 
                v-if="props.taskStatus.is_running" 
                size="small" 
                style="margin-right: 8px; vertical-align: middle;" 
              />
              <n-icon 
                v-else 
                :component="SchedulerIcon" 
                size="18" 
                style="margin-right: 8px; vertical-align: middle; opacity: 0.6;" 
              />
              <strong :style="{ color: props.taskStatus.is_running ? '#2080f0' : 'inherit' }">
                {{ props.taskStatus.current_action }}
              </strong>
              <span class="status-divider">-</span>
              <span class="status-message">{{ props.taskStatus.message }}</span>
            </n-text>
            
            <n-progress
              v-if="props.taskStatus.is_running && props.taskStatus.progress >= 0"
              type="line"
              :percentage="props.taskStatus.progress"
              :show-indicator="false"
              processing
              status="info"
              style="width: 100px; margin: 0 12px;"
            />

            <n-tooltip trigger="hover">
              <template #trigger>
                <n-button
                  v-if="props.taskStatus.is_running"
                  type="error"
                  size="tiny"
                  circle
                  secondary
                  @click="triggerStopTask"
                >
                  <template #icon><n-icon :component="StopIcon" /></template>
                </n-button>
              </template>
              停止任务
            </n-tooltip>
          </div>
        </div>

        <!-- 右侧：工具栏 -->
        <div style="display: flex; align-items: center; gap: 8px;">
            <!-- 桌面端显示的工具按钮 -->
            <template v-if="!isMobile">
              <n-button-group v-if="authStore.isAdmin" size="small">
                <n-tooltip>
                  <template #trigger>
                    <n-button @click="isRealtimeLogVisible = true" circle>
                      <template #icon><n-icon :component="ReaderOutline" /></template>
                    </n-button>
                  </template>
                  实时日志
                </n-tooltip>
                <n-tooltip>
                  <template #trigger>
                    <n-button @click="isHistoryLogVisible = true" circle>
                      <template #icon><n-icon :component="ArchiveOutline" /></template>
                    </n-button>
                  </template>
                  历史日志
                </n-tooltip>
              </n-button-group>
            </template>

            <!-- 用户名下拉菜单 (移动端简化显示) -->
            <n-dropdown 
              v-if="authStore.isLoggedIn" 
              trigger="hover" 
              :options="userOptions" 
              @select="handleUserSelect"
            >
              <div style="display: flex; align-items: center; cursor: pointer; gap: 4px;">
                <span style="font-size: 14px;">
                  {{ isMobile ? '' : `欢迎, ${authStore.username}` }}
                </span>
                <!-- 移动端只显示一个图标或头像占位 -->
                <n-icon v-if="isMobile" size="20" :component="UserCenterIcon" />
                <svg v-else xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24"><path fill="currentColor" d="m7 10l5 5l5-5z"></path></svg>
              </div>
            </n-dropdown>

            <!-- 桌面端显示版本号和主题 -->
            <template v-if="!isMobile">
              <span style="font-size: 12px; color: #999;">v{{ appVersion }}</span>

              <n-select
                :value="props.selectedTheme"
                @update:value="newValue => emit('update:selected-theme', newValue)"
                :options="themeOptions"
                size="small"
                style="width: 120px;"
              />
              
              <n-tooltip v-if="props.selectedTheme === 'custom'">
                <template #trigger>
                  <n-button @click="emit('edit-custom-theme')" circle size="small">
                    <template #icon><n-icon :component="PaletteIcon" /></template>
                  </n-button>
                </template>
                编辑我的专属主题
              </n-tooltip>

              <n-tooltip>
                <template #trigger>
                  <n-button @click="setRandomTheme" circle size="small">
                    <template #icon><n-icon :component="ShuffleIcon" /></template>
                  </n-button>
                </template>
                随机主题
              </n-tooltip>
            </template>

            <!-- 明暗模式切换器 (始终显示) -->
            <n-switch 
              :value="props.isDark" 
              @update:value="newValue => emit('update:is-dark', newValue)"
              size="small"
            >
              <template #checked-icon><n-icon :component="MoonIcon" /></template>
              <template #unchecked-icon><n-icon :component="SunnyIcon" /></template>
            </n-switch>
          </div>
      </div>
    </n-layout-header>
    
    <n-layout has-sider style="height: calc(100vh - 60px); position: relative;">
      <!-- 遮罩层：仅在移动端且侧边栏展开时显示，点击关闭侧边栏 -->
      <div 
        v-if="isMobile && !collapsed" 
        class="mobile-sider-mask"
        @click="collapsed = true"
      ></div>

      <n-layout-sider
        :bordered="false"
        collapse-mode="width"
        :collapsed-width="isMobile ? 0 : 64"
        :width="240"
        :show-trigger="isMobile ? false : 'arrow-circle'"
        content-style="padding-top: 10px;"
        :native-scrollbar="false"
        :collapsed="collapsed"
        @update:collapsed="val => collapsed = val"
        :class="{ 'mobile-sider': isMobile }"
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
        :native-scrollbar="false"
      >
      <div class="page-content-inner-wrapper">
          <router-view v-slot="slotProps">
            <component :is="slotProps.Component" :task-status="props.taskStatus" />
          </router-view>
        </div>
      </n-layout-content>
    </n-layout>
    
    <!-- 实时日志模态框 -->
    <n-modal v-model:show="isRealtimeLogVisible" preset="card" style="width: 95%; max-width: 900px;" title="实时任务日志" class="modal-card-lite">
       <n-log ref="logRef" :log="logContent" trim class="log-panel" style="height: 60vh; font-size: 13px; line-height: 1.6;"/>
    </n-modal>

    <!-- 历史日志模态框 -->
    <LogViewer v-model:show="isHistoryLogVisible" />
  </n-layout>
</template>

<script setup>
import { ref, computed, h, watch, nextTick, onMounted, onUnmounted } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import {
  NLayout, NLayoutHeader, NLayoutSider, NLayoutContent,
  NMenu, NSwitch, NIcon, NModal, NDropdown, NButton,
  NSelect, NTooltip, NCard, NText, NProgress, NButtonGroup, NLog,
  useMessage, useDialog
} from 'naive-ui';
import { useAuthStore } from './stores/auth';
import { themes } from './theme.js';
import LogViewer from './components/LogViewer.vue';
import {
  AnalyticsOutline as StatsIcon,
  ListOutline as ReviewListIcon,
  TimerOutline as SchedulerIcon,
  OptionsOutline as GeneralIcon,
  LogOutOutline as LogoutIcon,
  HeartOutline as WatchlistIcon,
  AlbumsOutline as CollectionsIcon,
  PeopleOutline as ActorSubIcon,
  InformationCircleOutline as AboutIcon,
  CreateOutline as CustomCollectionsIcon,
  ColorPaletteOutline as PaletteIcon,
  Stop as StopIcon,
  ShuffleOutline as ShuffleIcon,
  SyncOutline as RestartIcon,
  SparklesOutline as ResubscribeIcon,
  TrashBinOutline as CleanupIcon,
  PeopleCircleOutline as UserManagementIcon,
  PersonCircleOutline as UserCenterIcon,
  FilmOutline as DiscoverIcon,
  ArchiveOutline as UnifiedSubIcon,
  PricetagOutline as TagIcon,
  CloudDownloadOutline as NullbrIcon,
  CompassOutline,
  ReaderOutline,
  LibraryOutline, 
  BookmarksOutline, 
  SettingsOutline,
  ArchiveOutline,
  BookOutline as HelpIcon,
  MenuOutline, // 引入菜单图标
  Moon as MoonIcon,
  Sunny as SunnyIcon
} from '@vicons/ionicons5';
import axios from 'axios';
import logo from './assets/logo.png'

const message = useMessage();
const dialog = useDialog();

// --- 修改开始：使用原生 JS 判断移动端 ---
const isMobile = ref(false);

const checkMobile = () => {
  // 768px 通常是平板/手机的分界线
  isMobile.value = window.innerWidth < 768;
};

onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
});

onUnmounted(() => {
  window.removeEventListener('resize', checkMobile);
});

const triggerStopTask = async () => {
  try {
    await axios.post('/api/trigger_stop_task');
    message.info('已发送停止任务请求。');
  } catch (error) {
    message.error(error.response?.data?.error || '发送停止任务请求失败，请查看日志。');
  }
};

// 1. 定义 props 和 emits
const props = defineProps({
  isDark: Boolean,
  selectedTheme: String,
  taskStatus: Object
});
const emit = defineEmits(['update:is-dark', 'update:selected-theme', 'edit-custom-theme']);

// 2. 状态和路由
const router = useRouter(); 
const route = useRoute(); 
const authStore = useAuthStore();

// 侧边栏状态
const collapsed = ref(true);
const activeMenuKey = computed(() => route.name);
const appVersion = ref(__APP_VERSION__);

// 日志相关状态
const isRealtimeLogVisible = ref(false);
const isHistoryLogVisible = ref(false);
const logRef = ref(null);

// 监听路由变化，如果是移动端，跳转后自动收起侧边栏
watch(() => route.path, () => {
  if (isMobile.value) {
    collapsed.value = true;
  }
});

// 3. 从 theme.js 动态生成选项
const themeOptions = [
    ...Object.keys(themes).map(key => ({
        label: themes[key].name,
        value: key
    })),
    { type: 'divider', key: 'd1' },
    { label: '自定义', value: 'custom' }
];

// 4. 所有函数
const renderIcon = (iconComponent) => () => h(NIcon, null, { default: () => h(iconComponent) });

// 计算实时日志内容
const logContent = computed(() => props.taskStatus?.logs?.join('\n') || '等待任务日志...');

// 监听日志变化，自动滚动到底部
watch([() => props.taskStatus?.logs, isRealtimeLogVisible], async ([, isVisible]) => {
  if (isVisible) {
    await nextTick();
    logRef.value?.scrollTo({ position: 'bottom', slient: true });
  }
}, { deep: true });

const userOptions = computed(() => {
  const options = [];

  // 规则1: 只要是管理员，就能看到“重启容器”
  if (authStore.isAdmin) {
    options.push({
      label: '重启容器',
      key: 'restart-container',
      icon: renderIcon(RestartIcon)
    });
  }

  // 帮助文档
  options.push({
    label: '帮助文档',
    key: 'help-docs',
    icon: renderIcon(HelpIcon)
  });

  // 如果有任何管理项，就加一个分割线
  if (options.length > 0) {
    options.push({ type: 'divider', key: 'd1' });
  }

  // 规则2: 只要登录了，就能看到“退出登录”
  options.push({
    label: '退出登录',
    key: 'logout',
    icon: renderIcon(LogoutIcon)
  });

  return options;
});

const triggerRestart = async () => {
  message.info('正在发送重启指令...');
  try {
    await axios.post('/api/system/restart');
    message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
  } catch (error) {
    if (error.response) {
      message.error(error.response.data.error || '发送重启请求失败，请查看日志。');
    } else {
      message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
    }
  }
};

const handleUserSelect = async (key) => {
  if (key === 'restart-container') {
    dialog.warning({
      title: '确认重启容器',
      content: '确定要重启容器吗？应用将在短时间内无法访问，重启后需要手动刷新页面。',
      positiveText: '确定重启',
      negativeText: '取消',
      onPositiveClick: triggerRestart, 
    });
  } else if (key === 'help-docs') {
    window.open('https://hbq0405.github.io/emby-toolkit/zh/', '_blank');
  } else if (key === 'logout') {
    await authStore.logout();
    router.push({ name: 'Login' }); 
  }
};

const menuOptions = computed(() => {
  // 1. 先定义一个基础菜单组
  const discoveryGroup = { 
    label: '发现', 
    key: 'group-discovery', 
    icon: renderIcon(CompassOutline), 
    children: [] 
  };

  // 2. 根据用户类型，动态地往这个组里添加菜单项
  if (authStore.isAdmin) {
    discoveryGroup.children.push({ 
      label: '数据看板', 
      key: 'DatabaseStats', 
      icon: renderIcon(StatsIcon) 
    });
  }

  if (authStore.isLoggedIn) {
    discoveryGroup.children.push(
      { label: '用户中心', key: 'UserCenter', icon: renderIcon(UserCenterIcon) },
      { label: '影视探索', key: 'Discover', icon: renderIcon(DiscoverIcon) }
    );
    
    if (authStore.isAdmin) {
        discoveryGroup.children.push(
            { label: 'NULLBR', key: 'Nullbr', icon: renderIcon(NullbrIcon) }
        );
    }
  }

  // 3. 构建最终的菜单列表
  const finalMenu = [discoveryGroup];

  // 4. 如果是管理员，再把所有管理相关的菜单组加上去
  if (authStore.isAdmin) {
    finalMenu.push(
      { 
        label: '整理', 
        key: 'group-management', 
        icon: renderIcon(LibraryOutline), 
        children: [ 
          { label: '原生合集', key: 'Collections', icon: renderIcon(CollectionsIcon) }, 
          { label: '自建合集', key: 'CustomCollectionsManager', icon: renderIcon(CustomCollectionsIcon) }, 
          { label: '媒体去重', key: 'MediaCleanupPage', icon: renderIcon(CleanupIcon) },
          { label: '媒体整理', key: 'ResubscribePage', icon: renderIcon(ResubscribeIcon) },
          { label: '自动标签', key: 'AutoTaggingPage', icon: renderIcon(TagIcon) },  
          { label: '手动处理', key: 'ReviewList', icon: renderIcon(ReviewListIcon) }, 
        ] 
      },
      { 
        label: '订阅', 
        key: 'group-subscriptions', 
        icon: renderIcon(BookmarksOutline), 
        children: [ 
          { label: '智能追剧', key: 'Watchlist', icon: renderIcon(WatchlistIcon) }, 
          { label: '演员订阅', key: 'ActorSubscriptions', icon: renderIcon(ActorSubIcon) }, 
          { label: '统一订阅', key: 'UnifiedSubscriptions', icon: renderIcon(UnifiedSubIcon) },
        ] 
      },
      { 
        label: '系统', 
        key: 'group-system', 
        icon: renderIcon(SettingsOutline), 
        children: [ 
          { label: '通用设置', key: 'settings-general', icon: renderIcon(GeneralIcon) }, 
          { label: '用户管理', key: 'UserManagement', icon: renderIcon(UserManagementIcon) },
          { label: '任务中心', key: 'settings-scheduler', icon: renderIcon(SchedulerIcon) },
          { label: '封面生成', key: 'CoverGeneratorConfig', icon: renderIcon(PaletteIcon) }, 
          { label: '查看更新', key: 'Releases', icon: renderIcon(AboutIcon) }, 
        ] 
      }
    );
  }

  return finalMenu;
});

function handleMenuUpdate(key) {
  router.push({ name: key });
}

const setRandomTheme = () => {
  const otherThemes = themeOptions.filter(t => t.type !== 'divider' && t.value !== props.selectedTheme);
  if (otherThemes.length === 0) return;
  const randomIndex = Math.floor(Math.random() * otherThemes.length);
  const randomTheme = otherThemes[randomIndex];
  emit('update:selected-theme', randomTheme.value);
};
</script>

<style>
/* MainLayout 的样式 */
.app-header { padding: 0 16px; height: 60px; display: flex; align-items: center; font-size: 1.25em; font-weight: 600; flex-shrink: 0; }
.app-main-content-wrapper { height: 100%; display: flex; flex-direction: column; }
.page-content-inner-wrapper { flex-grow: 1; overflow-y: auto; }
.n-menu .n-menu-item-group-title { font-size: 12px; font-weight: 500; color: #8e8e93; padding-left: 24px; margin-top: 16px; margin-bottom: 8px; }
.n-menu .n-menu-item-group:first-child .n-menu-item-group-title { margin-top: 0; }
html.dark .n-menu .n-menu-item-group-title { color: #828287; }

/* 任务状态条样式 */
.header-task-status {
  flex: 2;
  display: flex;
  justify-content: center;
  align-items: center;
  margin: 0 20px;
  overflow: hidden;
  min-width: 0;
}

.status-content {
  display: flex;
  align-items: center;
  background-color: rgba(0, 0, 0, 0.03);
  padding: 4px 12px;
  border-radius: 20px;
  border: 1px solid rgba(0, 0, 0, 0.05);
  max-width: 100%;
}

html.dark .status-content {
  background-color: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.status-text {
  font-size: 13px;
  display: flex;
  align-items: center;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
  min-width: 0;
}

.status-divider {
  margin: 0 8px;
  opacity: 0.5;
  flex-shrink: 0;
}

.status-message {
  opacity: 0.8;
  max-width: 600px; 
  overflow: hidden;
  text-overflow: ellipsis;
  display: inline-block;
  vertical-align: bottom;
}

/* 移动端适配样式 */
@media (max-width: 768px) {
  .app-header {
    padding: 0 12px; /* 减小内边距 */
  }
  
  .status-message {
    max-width: 150px;
  }
  
  .header-task-status {
    margin: 0 8px;
    flex: 1;
  }

  /* 移动端侧边栏样式：悬浮在内容之上 */
  .mobile-sider {
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    z-index: 1000;
    height: 100%;
    box-shadow: 2px 0 8px rgba(0,0,0,0.15);
  }

  /* 移动端侧边栏遮罩 */
  .mobile-sider-mask {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background-color: rgba(0,0,0,0.4);
    z-index: 999;
    backdrop-filter: blur(2px);
  }
  
  /* 调整移动端内容区域内边距 */
  .n-layout-content .page-content-inner-wrapper {
    padding: 12px !important; /* 覆盖内联样式 */
  }
}
</style>