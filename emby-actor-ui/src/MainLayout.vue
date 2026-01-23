<!-- src/MainLayout.vue -->
<template>
  <n-layout style="height: 100vh;">
    <n-layout-header :bordered="false" class="app-header">
      <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
        <span class="text-effect">
          <img
            :src="logo"
            alt="Logo"
            style="height: 1.5em; vertical-align: middle; margin-right: 0.3em;"
          />
          Emby Toolkit
        </span>
        <!-- 任务状态 -->
        <div 
          v-if="authStore.isAdmin && props.taskStatus && props.taskStatus.current_action !== '空闲' && props.taskStatus.current_action !== '无'"
          class="header-task-status"
        >
          <div class="status-content">
            <n-text class="status-text">
              <!-- 1. 只有 is_running 为 true 时才转圈 -->
              <n-spin 
                v-if="props.taskStatus.is_running" 
                size="small" 
                style="margin-right: 8px; vertical-align: middle;" 
              />
              
              <!-- 2. 如果没运行（空闲/等待），显示一个静止的图标（这里复用了已引入的 SchedulerIcon） -->
              <n-icon 
                v-else 
                :component="SchedulerIcon" 
                size="18" 
                style="margin-right: 8px; vertical-align: middle; opacity: 0.6;" 
              />

              <!-- 3. 文字颜色也可以优化一下：运行时蓝色，空闲时灰色 -->
              <strong :style="{ color: props.taskStatus.is_running ? '#2080f0' : 'inherit' }">
                {{ props.taskStatus.current_action }}
              </strong>
              
              <span class="status-divider">-</span>
              <span class="status-message">{{ props.taskStatus.message }}</span>
            </n-text>
            
            <!-- 进度条 -->
            <n-progress
              v-if="props.taskStatus.is_running && props.taskStatus.progress >= 0"
              type="line"
              :percentage="props.taskStatus.progress"
              :show-indicator="false"
              processing
              status="info"
              style="width: 100px; margin: 0 12px;"
            />

            <!-- 停止按钮 -->
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
          <div style="display: flex; align-items: center; gap: 16px;">
            <!-- 只有管理员可见 -->
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
            <!-- 用户名下拉菜单 -->
            <n-dropdown 
              v-if="authStore.isLoggedIn" 
              trigger="hover" 
              :options="userOptions" 
              @select="handleUserSelect"
            >
              <div style="display: flex; align-items: center; cursor: pointer; gap: 4px;">
                <span style="font-size: 14px;">欢迎, {{ authStore.username }}</span>
                <svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24"><path fill="currentColor" d="m7 10l5 5l5-5z"></path></svg>
              </div>
            </n-dropdown>

            <span style="font-size: 12px; color: #999;">v{{ appVersion }}</span>

            <!-- 主题选择器 -->
            <n-select
              :value="props.selectedTheme"
              @update:value="newValue => emit('update:selected-theme', newValue)"
              :options="themeOptions"
              size="small"
              style="width: 120px;"
            />
            <!-- 编辑按钮入口 -->
            <n-tooltip v-if="props.selectedTheme === 'custom'">
              <template #trigger>
                <n-button @click="emit('edit-custom-theme')" circle size="small">
                  <template #icon><n-icon :component="PaletteIcon" /></template>
                </n-button>
              </template>
              编辑我的专属主题
            </n-tooltip>

            <!-- 随机主题按钮 -->
            <n-tooltip>
              <template #trigger>
                <n-button @click="setRandomTheme" circle size="small">
                  <template #icon><n-icon :component="ShuffleIcon" /></template>
                </n-button>
              </template>
              天灵灵地灵灵，给我来个好心情！
            </n-tooltip>

            <!-- 明暗模式切换器 -->
            <n-switch 
              :value="props.isDark" 
              @update:value="newValue => emit('update:is-dark', newValue)"
              size="small"
            >
              <template #checked>暗色</template>
              <template #unchecked>亮色</template>
            </n-switch>
          </div>
      </div>
    </n-layout-header>
    <n-layout has-sider style="height: calc(100vh - 60px);">
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
        :native-scrollbar="false"
      >
      <div class="page-content-inner-wrapper">
          <router-view v-slot="slotProps">
            <component :is="slotProps.Component" :task-status="props.taskStatus" />
          </router-view>
        </div>
      </n-layout-content>
    </n-layout>
    
    <!-- 移除了修改密码模态框，因为现在没有本地密码了 -->
    
    <!-- 实时日志模态框 -->
    <n-modal v-model:show="isRealtimeLogVisible" preset="card" style="width: 80%; max-width: 900px;" title="实时任务日志" class="modal-card-lite">
       <n-log ref="logRef" :log="logContent" trim class="log-panel" style="height: 60vh; font-size: 13px; line-height: 1.6;"/>
    </n-modal>

    <!-- 历史日志模态框 -->
    <LogViewer v-model:show="isHistoryLogVisible" />
  </n-layout>
</template>

<script setup>
import { ref, computed, h, watch, nextTick } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import {
  NLayout, NLayoutHeader, NLayoutSider, NLayoutContent,
  NMenu, NSwitch, NIcon, NModal, NDropdown, NButton,
  NSelect, NTooltip, NCard, NText, NProgress, NButtonGroup, NLog
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
  BookOutline as HelpIcon // 引入帮助图标
} from '@vicons/ionicons5';
import axios from 'axios';
import { useMessage, useDialog } from 'naive-ui';
import logo from './assets/logo.png'

const message = useMessage();
const dialog = useDialog();

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
// 修改：默认折叠侧边栏
const collapsed = ref(true);
const activeMenuKey = computed(() => route.name);
const appVersion = ref(__APP_VERSION__);
// 日志相关状态
const isRealtimeLogVisible = ref(false);
const isHistoryLogVisible = ref(false);
const logRef = ref(null);

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

  // <--- 新增：帮助文档 --->
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
    // <--- 新增：跳转到帮助文档 --->
    window.open('https://hbq0405.github.io/emby-toolkit/zh/', '_blank');
  } else if (key === 'logout') {
    await authStore.logout();
    router.push({ name: 'Login' }); 
  }
};

const menuOptions = computed(() => {
  // 1. 先定义一个基础菜单组
  // 修改：添加 icon，移除 type: 'group' 以保持与其他一级菜单风格一致（可折叠）
  const discoveryGroup = { 
    label: '发现', 
    key: 'group-discovery', 
    icon: renderIcon(CompassOutline), // 使用指南针图标
    // type: 'group', // 移除此行，使其变为带图标的子菜单样式
    children: [] 
  };

  // 2. 根据用户类型，动态地往这个组里添加菜单项
  
  // 规则1: 如果用户是管理员，就添加“数据看板”
  if (authStore.isAdmin) {
    discoveryGroup.children.push({ 
      label: '数据看板', 
      key: 'DatabaseStats', 
      icon: renderIcon(StatsIcon) 
    });
  }

  // 规则2: 只要是登录用户，都应该能看到“用户中心”和“影视探索”
  if (authStore.isLoggedIn) {
    discoveryGroup.children.push(
      { label: '用户中心', key: 'UserCenter', icon: renderIcon(UserCenterIcon) },
      { label: '影视探索', key: 'Discover', icon: renderIcon(DiscoverIcon) }
    );
    
    // ★★★ 修改：将 NULLBR 移出通用列表，单独判断管理员权限 ★★★
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

  // 5. 返回最终构建好的菜单
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
.app-header { padding: 0 24px; height: 60px; display: flex; align-items: center; font-size: 1.25em; font-weight: 600; flex-shrink: 0; }
.app-main-content-wrapper { height: 100%; display: flex; flex-direction: column; }
.page-content-inner-wrapper { flex-grow: 1; overflow-y: auto; }
.n-menu .n-menu-item-group-title { font-size: 12px; font-weight: 500; color: #8e8e93; padding-left: 24px; margin-top: 16px; margin-bottom: 8px; }
.n-menu .n-menu-item-group:first-child .n-menu-item-group-title { margin-top: 0; }
html.dark .n-menu .n-menu-item-group-title { color: #828287; }
/* ★★★ 新增样式：Header 中的任务状态条 ★★★ */
.header-task-status {
  flex: 2;
  display: flex;
  justify-content: center; /* 居中显示 */
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
  max-width: 100%; /* 允许撑满父容器 */
}

/* 暗色模式适配 */
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
  flex: 1; /* 让文字区域自动填充剩余空间 */
  min-width: 0; /* 关键：允许文字截断 */
}

.status-divider {
  margin: 0 8px;
  opacity: 0.5;
  flex-shrink: 0; /* 防止分隔符被挤压 */
}

.status-message {
  opacity: 0.8;
  /* ★★★ 核心修改：大幅增加最大宽度，或者直接设为 auto 配合 flex ★★★ */
  max-width: 600px; 
  overflow: hidden;
  text-overflow: ellipsis;
  display: inline-block;
  vertical-align: bottom;
}

/* 移动端适配：屏幕太窄时隐藏消息详情，只留动作和进度条 */
@media (max-width: 768px) {
  .status-message {
    max-width: 150px; /* 移动端还是限制一下 */
  }
  .header-task-status {
    margin: 0 8px;
    flex: 1;
  }
}
</style>