<!-- src/MainLayout.vue -->
<template>
  <n-layout style="height: 100vh; position: relative;">
    <!-- iOS 风格顶栏 -->
    <n-layout-header :bordered="false" class="app-header">
      <div class="app-header-inner">
        <!-- 左：菜单按钮 + Logo -->
        <div class="app-header-left">
          <n-button
            v-if="isMobile && authStore.isLoggedIn"
            text
            class="header-menu-btn"
            @click="toggleSidebar"
          >
            <n-icon :component="MenuOutline" size="24" />
          </n-button>
          <img :src="logo" alt="Logo" class="header-logo" />
          <span class="header-brand" v-if="!isMobile">Emby Toolkit</span>
        </div>

        <!-- 中：任务状态 (仅桌面端) -->
        <div
          v-if="!isMobile && authStore.isAdmin && props.taskStatus && props.taskStatus.current_action !== '空闲' && props.taskStatus.current_action !== '无'"
          class="header-task-status"
        >
          <div class="status-content">
            <n-spin v-if="props.taskStatus.is_running" size="small" style="margin-right: 8px;" />
            <n-icon v-else :component="SchedulerIcon" size="16" style="margin-right: 6px; opacity: 0.5;" />
            <span class="status-dot" :class="{ running: props.taskStatus.is_running }"></span>
            <span class="status-label">{{ props.taskStatus.current_action }}</span>
            <span class="status-divider">—</span>
            <span class="status-message">{{ props.taskStatus.message }}</span>

            <n-progress
              v-if="props.taskStatus.is_running && props.taskStatus.progress >= 0"
              type="line"
              :percentage="props.taskStatus.progress"
              :show-indicator="false"
              processing
              status="info"
              class="status-progress"
            />

            <n-button
              v-if="props.taskStatus.is_running"
              type="error"
              size="tiny"
              circle
              secondary
              @click="triggerStopTask"
              class="status-stop-btn"
            >
              <template #icon><n-icon :component="StopIcon" /></template>
            </n-button>
          </div>
        </div>

        <!-- 右：工具栏 -->
        <div class="app-header-right">
          <n-button-group v-if="authStore.isAdmin && !isMobile" size="small">
            <n-tooltip><template #trigger><n-button @click="isRealtimeLogVisible = true" circle><template #icon><n-icon :component="ReaderOutline" /></template></n-button></template>实时日志</n-tooltip>
            <n-tooltip><template #trigger><n-button @click="isHistoryLogVisible = true" circle><template #icon><n-icon :component="ArchiveOutline" /></template></n-button></template>历史日志</n-tooltip>
          </n-button-group>

          <n-dropdown v-if="authStore.isLoggedIn" trigger="hover" :options="userOptions" @select="handleUserSelect">
            <div class="user-menu-trigger">
              <n-icon size="20" :component="UserCenterIcon" />
              <span v-if="!isMobile" class="user-name">{{ authStore.username }}</span>
            </div>
          </n-dropdown>

          <template v-if="!isMobile">
            <span class="version-badge">v{{ appVersion }}</span>
            <n-select :value="props.selectedTheme" @update:value="v => emit('update:selected-theme', v)" :options="themeOptions" size="small" class="theme-selector" />
            <n-button v-if="props.selectedTheme === 'custom'" @click="emit('edit-custom-theme')" circle size="small"><template #icon><n-icon :component="PaletteIcon" /></template></n-button>
            <n-button @click="setRandomTheme" circle size="small"><template #icon><n-icon :component="ShuffleIcon" /></template></n-button>
          </template>

          <n-switch :value="props.isDark" @update:value="v => emit('update:is-dark', v)">
            <template #checked-icon><n-icon :component="MoonIcon" /></template>
            <template #unchecked-icon><n-icon :component="SunnyIcon" /></template>
          </n-switch>
        </div>
      </div>
    </n-layout-header>

    <!-- 主区域 -->
    <n-layout has-sider class="app-main">
      <!-- 移动端遮罩 -->
      <transition name="ios-fade">
        <div v-if="isMobile && !collapsed" class="mobile-sider-mask" @click="closeSidebar"></div>
      </transition>

      <!-- 侧边栏 -->
      <n-layout-sider
        :bordered="false"
        collapse-mode="width"
        :collapsed-width="isMobile ? 0 : 64"
        :width="240"
        :show-trigger="isMobile ? false : 'arrow-circle'"
        content-style="padding-top: 4px;"
        :native-scrollbar="false"
        :collapsed="collapsed"
        @update:collapsed="val => collapsed = val"
        :class="{ 'mobile-sider': isMobile, 'sider-open': !collapsed }"
      >
        <n-menu
          v-if="isMenuReady"
          :collapsed="collapsed"
          :collapsed-width="64"
          :collapsed-icon-size="22"
          :options="menuOptions"
          :value="activeMenuKey"
          :default-expanded-keys="defaultExpandedKeys"
          @update:value="handleMenuUpdate"
          class="ios-menu"
        />
      </n-layout-sider>

      <!-- 内容区 -->
      <n-layout-content class="app-main-content-wrapper" :content-style="{ padding: contentPadding }" :native-scrollbar="false">
        <div class="page-content-inner-wrapper">
          <router-view v-slot="slotProps">
            <component :is="slotProps.Component" :task-status="props.taskStatus" />
          </router-view>
        </div>
      </n-layout-content>
    </n-layout>

    <!-- 实时日志 -->
    <n-modal v-model:show="isRealtimeLogVisible" preset="card" style="width: 95%; max-width: 900px;" title="实时任务日志" class="modal-card-lite">
      <n-log ref="logRef" :log="logContent" trim class="log-panel" style="height: 60vh; font-size: 13px; line-height: 1.6;" />
    </n-modal>

    <LogViewer v-model:show="isHistoryLogVisible" />

    <!-- 菜单编辑器 -->
    <n-modal v-model:show="isMenuEditorVisible" preset="card" style="width: 95%; max-width: 650px;" title="自定义侧边栏菜单 (全局生效)">
      <div style="max-height: 60vh; overflow-y: auto; padding-right: 10px; user-select: none;">
        <div v-for="(group, gIndex) in menuConfigTree" :key="group.key" style="margin-bottom: 20px;">
          <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px; background: rgba(128,128,128,0.1); padding: 8px; border-radius: 6px;">
            <n-switch v-model:value="group.visible" size="small" />
            <n-input v-model:value="group.label" :placeholder="getOriginalLabel(group.key)" size="small" style="width: 180px;" />
            <span style="color: #888; font-size: 12px; flex: 1;">(一级菜单)</span>
            <div style="display: flex; align-items: center; gap: 4px;">
              <span style="font-size: 12px; color: #666;">默认展开:</span>
              <n-switch v-model:value="group.expanded" size="small" />
            </div>
          </div>
          <div style="padding-left: 24px; min-height: 10px;" @dragover.prevent="onDragOverEmpty($event, gIndex)" @drop="onDropEmpty($event, gIndex)">
            <div v-for="(child, cIndex) in group.children" :key="child.key" class="draggable-item" :class="{ 'drag-over-top': dragTarget?.gIndex === gIndex && dragTarget?.cIndex === cIndex && dragPosition === 'top', 'drag-over-bottom': dragTarget?.gIndex === gIndex && dragTarget?.cIndex === cIndex && dragPosition === 'bottom', 'is-dragging': dragSource?.gIndex === gIndex && dragSource?.cIndex === cIndex }" draggable="true" @dragstart="onDragStart($event, gIndex, cIndex)" @dragover.prevent="onDragOver($event, gIndex, cIndex)" @drop="onDrop($event, gIndex, cIndex)" @dragend="onDragEnd">
              <n-icon size="18" style="cursor: grab; color: #999;"><MenuOutline /></n-icon>
              <n-switch v-model:value="child.visible" size="small" />
              <n-input v-model:value="child.label" :placeholder="getOriginalLabel(child.key)" size="small" style="width: 200px;" />
            </div>
            <div v-if="group.children.length === 0" class="empty-group-dropzone">拖拽菜单至此处</div>
          </div>
        </div>
      </div>
      <template #footer>
        <n-space justify="space-between">
          <span style="font-size: 12px; color: #888; line-height: 1.5; display: inline-block; max-width: 380px;">
            提示：按住左侧图标可拖拽排序，支持跨组拖拽。<br/>
            <span style="color: #d0a000;">★ 关闭一级菜单显示，可将其下开启的二级菜单自动提升为一级。</span>
          </span>
          <n-space>
            <n-popconfirm @positive-click="resetMenuConfig" negative-text="取消" positive-text="确定">
              <template #trigger><n-button type="error" ghost :loading="isSavingMenu">恢复默认</n-button></template>
              确定要恢复所有菜单的默认名称、顺序和显示状态吗？
            </n-popconfirm>
            <n-button type="primary" @click="saveMenuConfig" :loading="isSavingMenu">保存并应用</n-button>
          </n-space>
        </n-space>
      </template>
    </n-modal>

  </n-layout>
</template>

<script setup>
import { ref, computed, h, watch, nextTick, onMounted, onUnmounted } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import {
  NLayout, NLayoutHeader, NLayoutSider, NLayoutContent,
  NMenu, NSwitch, NIcon, NModal, NDropdown, NButton,
  NSelect, NTooltip, NCard, NText, NProgress, NButtonGroup, NLog,
  useMessage, useDialog, NInput, NSpace, NPopconfirm
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
  CompassOutline,
  ReaderOutline,
  LibraryOutline, 
  BookmarksOutline, 
  SettingsOutline,
  ArchiveOutline,
  BookOutline as HelpIcon,
  MenuOutline, 
  Moon as MoonIcon,
  Sunny as SunnyIcon,
  PieChartOutline as EmbyStatsIcon,
  DocumentTextOutline as RecordsIcon,
  ListCircleOutline as MenuEditIcon,
  CloudCircleOutline as SharedResourceIcon
} from '@vicons/ionicons5';
import axios from 'axios';
import logo from './assets/logo.png'

const message = useMessage();
const dialog = useDialog();

import { useResponsive } from './composables/useResponsive';

const { isMobile, isTablet, isDesktop, isTouchDevice, contentPadding } = useResponsive();

// ================= 触控手势 (移动端侧栏) =================
let touchStartX = 0;
let touchStartY = 0;
const SWIPE_THRESHOLD = 60;

const onTouchStart = (e) => {
  touchStartX = e.touches[0].clientX;
  touchStartY = e.touches[0].clientY;
};

const onTouchMove = (e) => {
  // 只在侧栏关闭时检测右滑
  if (collapsed.value && isMobile.value) {
    const dx = e.touches[0].clientX - touchStartX;
    const dy = e.touches[0].clientY - touchStartY;
    if (dx > SWIPE_THRESHOLD && Math.abs(dy) < 80) {
      collapsed.value = false;
    }
  }
};

const onTouchEnd = () => {};

const toggleSidebar = () => {
  collapsed.value = !collapsed.value;
};

const closeSidebar = () => {
  collapsed.value = true;
};
// ========================================================

onMounted(() => {
  initMenuConfig();
  if (isTouchDevice.value) {
    document.addEventListener('touchstart', onTouchStart, { passive: true });
    document.addEventListener('touchmove', onTouchMove, { passive: true });
  }
});

onUnmounted(() => {
  if (isTouchDevice.value) {
    document.removeEventListener('touchstart', onTouchStart);
    document.removeEventListener('touchmove', onTouchMove);
  }
});

const triggerStopTask = async () => {
  try {
    await axios.post('/api/trigger_stop_task');
    message.info('已发送停止任务请求。');
  } catch (error) {
    message.error(error.response?.data?.error || '发送停止任务请求失败，请查看日志。');
  }
};

const props = defineProps({
  isDark: Boolean,
  selectedTheme: String,
  taskStatus: Object
});
const emit = defineEmits(['update:is-dark', 'update:selected-theme', 'edit-custom-theme']);

const router = useRouter(); 
const route = useRoute(); 
const authStore = useAuthStore();

// 侧边栏状态：移动端默认收起，桌面端读取上次的习惯（首次访问默认展开）
const collapsed = ref(isMobile.value ? true : localStorage.getItem('sidebar_collapsed') === 'true');

// 记住用户对侧边栏的展开/收起操作（仅限桌面端）
watch(collapsed, (newVal) => {
  if (!isMobile.value) {
    localStorage.setItem('sidebar_collapsed', String(newVal));
  }
});
const activeMenuKey = computed(() => route.name);
const appVersion = ref(__APP_VERSION__);

const isRealtimeLogVisible = ref(false);
const isHistoryLogVisible = ref(false);
const logRef = ref(null);

watch(() => route.path, () => {
  if (isMobile.value) {
    collapsed.value = true;
  }
});

const themeOptions = [
    ...Object.keys(themes).map(key => ({
        label: themes[key].name,
        value: key
    })),
    { type: 'divider', key: 'd1' },
    { label: '自定义', value: 'custom' }
];

const renderIcon = (iconComponent) => () => h(NIcon, null, { default: () => h(iconComponent) });

const logContent = computed(() => props.taskStatus?.logs?.join('\n') || '等待任务日志...');

watch([() => props.taskStatus?.logs, isRealtimeLogVisible], async ([, isVisible]) => {
  if (isVisible) {
    await nextTick();
    logRef.value?.scrollTo({ position: 'bottom', slient: true });
  }
}, { deep: true });

const userOptions = computed(() => {
  const options = [];

  if (authStore.isAdmin) {
    options.push({
      label: '自定义菜单',
      key: 'edit-menu',
      icon: renderIcon(MenuEditIcon)
    });

    options.push({
      label: '重启容器',
      key: 'restart-container',
      icon: renderIcon(RestartIcon)
    });
  }

  options.push({
    label: '帮助文档',
    key: 'help-docs',
    icon: renderIcon(HelpIcon)
  });

  if (options.length > 0) {
    options.push({ type: 'divider', key: 'd1' });
  }

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
  if (key === 'edit-menu') {
    isMenuEditorVisible.value = true;
  } else if (key === 'restart-container') {
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

// ================= 自定义菜单逻辑 (树形结构 + 拖拽) =================
const isMenuEditorVisible = ref(false);
const isSavingMenu = ref(false);
const isMenuReady = ref(false); // 控制 n-menu 渲染时机
const menuConfigTree = ref([]); // 树形配置数据

// 1. 基础菜单（包含所有权限允许的菜单，带有 defaultLabel 用于重置）
const baseMenuOptions = computed(() => {
  const discoveryGroup = { 
    label: '发现', defaultLabel: '发现', key: 'group-discovery', icon: renderIcon(CompassOutline), children: [] 
  };

  if (authStore.isAdmin) {
    discoveryGroup.children.push({ label: '数据看板', defaultLabel: '数据看板', key: 'DatabaseStats', icon: renderIcon(StatsIcon) });
  }

  if (authStore.isLoggedIn) {
    discoveryGroup.children.push(
      { label: '用户中心', defaultLabel: '用户中心', key: 'UserCenter', icon: renderIcon(UserCenterIcon) },
      { label: '影视探索', defaultLabel: '影视探索', key: 'Discover', icon: renderIcon(DiscoverIcon) }
    );
    if (authStore.isAdmin) {
        discoveryGroup.children.push(
            { label: '播放统计', defaultLabel: '播放统计', key: 'EmbyStats', icon: renderIcon(EmbyStatsIcon) }
        );
    }
  }

  const finalMenu = [discoveryGroup];

  if (authStore.isAdmin) {
    finalMenu.push(
      { 
        label: '整理', defaultLabel: '整理', key: 'group-management', icon: renderIcon(LibraryOutline), 
        children: [ 
          { label: '原生合集', defaultLabel: '原生合集', key: 'Collections', icon: renderIcon(CollectionsIcon) }, 
          { label: '自建合集', defaultLabel: '自建合集', key: 'CustomCollectionsManager', icon: renderIcon(CustomCollectionsIcon) }, 
          { label: '媒体去重', defaultLabel: '媒体去重', key: 'MediaCleanupPage', icon: renderIcon(CleanupIcon) },
          { label: '媒体整理', defaultLabel: '媒体整理', key: 'ResubscribePage', icon: renderIcon(ResubscribeIcon) },
          { label: '共享资源', defaultLabel: '共享资源', key: 'SharedResourceManager', icon: renderIcon(SharedResourceIcon) },
          { label: '自动标签', defaultLabel: '自动标签', key: 'AutoTaggingPage', icon: renderIcon(TagIcon) },
          { label: '整理记录', defaultLabel: '整理记录', key: 'OrganizeRecords', icon: renderIcon(RecordsIcon) },  
          { label: '手动处理', defaultLabel: '手动处理', key: 'ReviewList', icon: renderIcon(ReviewListIcon) }, 
        ] 
      },
      { 
        label: '订阅', defaultLabel: '订阅', key: 'group-subscriptions', icon: renderIcon(BookmarksOutline), 
        children: [ 
          { label: '智能追剧', defaultLabel: '智能追剧', key: 'Watchlist', icon: renderIcon(WatchlistIcon) }, 
          { label: '演员订阅', defaultLabel: '演员订阅', key: 'ActorSubscriptions', icon: renderIcon(ActorSubIcon) }, 
          { label: '统一订阅', defaultLabel: '统一订阅', key: 'UnifiedSubscriptions', icon: renderIcon(UnifiedSubIcon) },
        ] 
      },
      { 
        label: '系统', defaultLabel: '系统', key: 'group-system', icon: renderIcon(SettingsOutline), 
        children: [ 
          { label: '通用设置', defaultLabel: '通用设置', key: 'settings-general', icon: renderIcon(GeneralIcon) }, 
          { label: '用户管理', defaultLabel: '用户管理', key: 'UserManagement', icon: renderIcon(UserManagementIcon) },
          { label: '任务中心', defaultLabel: '任务中心', key: 'settings-scheduler', icon: renderIcon(SchedulerIcon) },
          { label: '封面生成', defaultLabel: '封面生成', key: 'CoverGeneratorConfig', icon: renderIcon(PaletteIcon) }, 
          { label: '查看更新', defaultLabel: '查看更新', key: 'Releases', icon: renderIcon(AboutIcon) }, 
        ] 
      }
    );
  }
  return finalMenu;
});

// 辅助函数：获取原始名称和图标
const baseMenuMap = computed(() => {
  const map = new Map();
  baseMenuOptions.value.forEach(group => {
    map.set(group.key, { ...group, isGroup: true });
    if (group.children) {
      group.children.forEach(child => {
        map.set(child.key, { ...child, isGroup: false });
      });
    }
  });
  return map;
});

const getOriginalLabel = (key) => {
  return baseMenuMap.value.get(key)?.defaultLabel || '';
};

// 核心合并算法：将后端保存的树与当前代码中的 baseMenuOptions 合并
const syncMenuConfig = (savedTree) => {
  const resultTree = [];
  const usedKeys = new Set();

  // 1. 遍历保存的树，保留存在的项
  if (Array.isArray(savedTree)) {
    savedTree.forEach(savedGroup => {
      const baseGroup = baseMenuMap.value.get(savedGroup.key);
      if (!baseGroup || !baseGroup.isGroup) return; // 废弃的组

      usedKeys.add(savedGroup.key);
      const newGroup = {
        key: savedGroup.key,
        label: savedGroup.label || baseGroup.defaultLabel,
        visible: savedGroup.visible !== false,
        expanded: savedGroup.expanded !== false, // 默认展开
        children: []
      };

      if (Array.isArray(savedGroup.children)) {
        savedGroup.children.forEach(savedChild => {
          const baseChild = baseMenuMap.value.get(savedChild.key);
          if (!baseChild || baseChild.isGroup) return; // 废弃的子项
          usedKeys.add(savedChild.key);
          newGroup.children.push({
            key: savedChild.key,
            label: savedChild.label || baseChild.defaultLabel,
            visible: savedChild.visible !== false
          });
        });
      }
      resultTree.push(newGroup);
    });
  }

  // 2. 找出 baseOptions 中有，但 savedTree 中没有的新增项
  baseMenuOptions.value.forEach(baseGroup => {
    if (!usedKeys.has(baseGroup.key)) {
      // 整个组都是新的
      resultTree.push({
        key: baseGroup.key,
        label: baseGroup.defaultLabel,
        visible: true,
        expanded: true,
        children: baseGroup.children ? baseGroup.children.map(c => ({
          key: c.key,
          label: c.defaultLabel,
          visible: true
        })) : []
      });
    } else {
      // 组存在，检查是否有新的子项
      const targetGroup = resultTree.find(g => g.key === baseGroup.key);
      if (baseGroup.children) {
        baseGroup.children.forEach(baseChild => {
          if (!usedKeys.has(baseChild.key)) {
            targetGroup.children.push({
              key: baseChild.key,
              label: baseChild.defaultLabel,
              visible: true
            });
          }
        });
      }
    }
  });

  return resultTree;
};

// 初始化配置
const initMenuConfig = async () => {
  isMenuReady.value = false;
  try {
    const response = await axios.get('/api/system/menu_config');
    const saved = response.data || [];
    menuConfigTree.value = syncMenuConfig(saved);
  } catch (error) {
    console.error("获取菜单配置失败", error);
    menuConfigTree.value = syncMenuConfig([]); // 失败时回退到默认
  } finally {
    // 确保数据加载完再渲染菜单
    nextTick(() => {
      isMenuReady.value = true;
    });
  }
};

// 监听基础菜单变化（权限变化）
watch(baseMenuOptions, () => {
  // 重新合并，保留用户已有的排序和名称，加入新权限的菜单
  menuConfigTree.value = syncMenuConfig(menuConfigTree.value);
}, { deep: true });

// 保存配置到后端
const saveMenuConfig = async () => {
  isSavingMenu.value = true;
  try {
    await axios.post('/api/system/menu_config', menuConfigTree.value);
    isMenuEditorVisible.value = false;
    message.success('菜单配置已保存，全局生效');
    
    // 强制重新渲染菜单以应用新的展开状态
    isMenuReady.value = false;
    nextTick(() => { isMenuReady.value = true; });
  } catch (error) {
    message.error(error.response?.data?.error || '保存菜单配置失败');
  } finally {
    isSavingMenu.value = false;
  }
};

// 恢复默认
const resetMenuConfig = async () => {
  isSavingMenu.value = true;
  try {
    await axios.post('/api/system/menu_config/reset');
    menuConfigTree.value = syncMenuConfig([]); // 传入空数组生成默认树
    message.success('已恢复默认菜单');
    
    isMenuReady.value = false;
    nextTick(() => { isMenuReady.value = true; });
  } catch (error) {
    message.error(error.response?.data?.error || '重置菜单配置失败');
  } finally {
    isSavingMenu.value = false;
  }
};

// 计算默认展开的 keys
const defaultExpandedKeys = computed(() => {
  return menuConfigTree.value.filter(g => g.expanded).map(g => g.key);
});

// 最终渲染的菜单
const menuOptions = computed(() => {
  if (menuConfigTree.value.length === 0) return [];

  const finalMenu = [];

  menuConfigTree.value.forEach(group => {
    const baseGroup = baseMenuMap.value.get(group.key);
    if (!baseGroup) return;

    // 1. 先筛选出该组下所有可见的二级菜单
    const visibleChildren = [];
    if (group.children && group.children.length > 0) {
      group.children.forEach(child => {
        if (child.visible) {
          const baseChild = baseMenuMap.value.get(child.key);
          if (baseChild) {
            visibleChildren.push({
              key: child.key,
              label: child.label || baseChild.defaultLabel,
              icon: baseChild.icon
            });
          }
        }
      });
    }

    // 如果该组下没有任何可见的子菜单，则直接跳过（不渲染组，也不渲染子项）
    if (visibleChildren.length === 0) return;

    if (group.visible) {
      // 2. 如果一级菜单可见，正常渲染为包含 children 的折叠组
      finalMenu.push({
        key: group.key,
        label: group.label || baseGroup.defaultLabel,
        icon: baseGroup.icon,
        children: visibleChildren
      });
    } else {
      // 3. 【核心逻辑】如果一级菜单不可见，将可见的二级菜单直接“扁平化”提升为一级菜单
      finalMenu.push(...visibleChildren);
    }
  });

  return finalMenu;
});

// ================= 原生拖拽逻辑 =================
const dragSource = ref(null); // { gIndex, cIndex }
const dragTarget = ref(null); // { gIndex, cIndex }
const dragPosition = ref(''); // 'top' 或 'bottom'

const onDragStart = (e, gIndex, cIndex) => {
  dragSource.value = { gIndex, cIndex };
  e.dataTransfer.effectAllowed = 'move';
  // 设置一个透明的拖拽图像，或者让浏览器默认处理
};

const onDragOver = (e, gIndex, cIndex) => {
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  
  // 计算鼠标在元素上半部还是下半部
  const rect = e.currentTarget.getBoundingClientRect();
  const midY = rect.top + rect.height / 2;
  dragPosition.value = e.clientY < midY ? 'top' : 'bottom';
  dragTarget.value = { gIndex, cIndex };
};

const onDragOverEmpty = (e, gIndex) => {
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  // 拖入空组
  if (menuConfigTree.value[gIndex].children.length === 0) {
    dragTarget.value = { gIndex, cIndex: -1 };
    dragPosition.value = 'inside';
  }
};

const onDrop = (e, targetGIndex, targetCIndex) => {
  e.preventDefault();
  if (!dragSource.value) return;

  const { gIndex: fromGIndex, cIndex: fromCIndex } = dragSource.value;
  
  // 如果是同一个元素，不操作
  if (fromGIndex === targetGIndex && fromCIndex === targetCIndex) {
    clearDragState();
    return;
  }

  // 1. 取出被拖拽的元素
  const item = menuConfigTree.value[fromGIndex].children.splice(fromCIndex, 1)[0];

  // 2. 计算插入位置
  let insertIndex = targetCIndex;
  
  // 如果在同一个组内往下拖，因为前面删除了一个元素，目标索引需要 -1
  if (fromGIndex === targetGIndex && fromCIndex < targetCIndex) {
    insertIndex -= 1;
  }
  
  // 如果放在下半部，插入到目标后面
  if (dragPosition.value === 'bottom') {
    insertIndex += 1;
  }

  // 3. 插入元素
  menuConfigTree.value[targetGIndex].children.splice(insertIndex, 0, item);
  
  clearDragState();
};

const onDropEmpty = (e, targetGIndex) => {
  e.preventDefault();
  if (!dragSource.value) return;
  
  // 只有当目标组真的为空时才处理
  if (menuConfigTree.value[targetGIndex].children.length === 0) {
    const { gIndex: fromGIndex, cIndex: fromCIndex } = dragSource.value;
    const item = menuConfigTree.value[fromGIndex].children.splice(fromCIndex, 1)[0];
    menuConfigTree.value[targetGIndex].children.push(item);
  }
  clearDragState();
};

const onDragEnd = () => {
  clearDragState();
};

const clearDragState = () => {
  dragSource.value = null;
  dragTarget.value = null;
  dragPosition.value = '';
};

// ==================================================

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
/* ================= iOS 风格顶栏 ================= */
.app-header {
  height: 52px;
  flex-shrink: 0;
  border-bottom: 1px solid var(--card-border-color);
  box-shadow: 0 0 20px -8px var(--accent-glow-color);
}

.app-header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 100%;
  padding: 0 16px;
}

.app-header-left {
  display: flex;
  align-items: center;
  gap: 10px;
}

.header-menu-btn {
  font-size: 22px !important;
  padding: 6px !important;
  margin: 0 !important;
}

.header-logo {
  height: 28px;
  width: 28px;
  border-radius: 6px;
}

.header-brand {
  font-size: 17px;
  font-weight: 640;
  letter-spacing: 0.2px;
  color: var(--text-color);
}

.app-header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

/* 状态指示器 */
.header-task-status {
  flex: 1;
  display: flex;
  justify-content: center;
  align-items: center;
  margin: 0 16px;
  min-width: 0;
}

.status-content {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 14px;
  border-radius: var(--radius-pill);
  background: rgba(128, 128, 128, 0.06);
  font-size: 13px;
  max-width: 100%;
  overflow: hidden;
}

.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: rgba(128, 128, 128, 0.3);
  flex-shrink: 0;
}

.status-dot.running {
  background: #34c759;
  box-shadow: 0 0 6px rgba(52, 199, 89, 0.6);
  animation: pulse-dot 1.5s ease-in-out infinite;
}

@keyframes pulse-dot {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.4; }
}

.status-label {
  font-weight: 590;
  white-space: nowrap;
  color: var(--text-color);
  opacity: 0.9;
}

.status-divider {
  opacity: 0.35;
  flex-shrink: 0;
}

.status-message {
  opacity: 0.65;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
  min-width: 0;
}

.status-progress {
  width: 80px !important;
  flex-shrink: 0;
}

.status-stop-btn {
  flex-shrink: 0;
}

.version-badge {
  font-size: 12px;
  color: var(--text-color);
  opacity: 0.4;
  font-weight: 460;
}

.theme-selector {
  width: 100px !important;
}

.user-menu-trigger {
  display: flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  padding: 4px 8px;
  border-radius: var(--radius-button);
  transition: background 0.2s;
}

.user-menu-trigger:hover {
  background: rgba(128, 128, 128, 0.08);
}

.user-name {
  font-size: 14px;
  font-weight: 460;
  color: var(--text-color);
  opacity: 0.85;
}

/* ================= 主区域 ================= */
.app-main {
  height: calc(100vh - 52px);
  position: relative;
}

.app-main-content-wrapper {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.page-content-inner-wrapper {
  flex-grow: 1;
  overflow-y: auto;
}

/* ================= iOS 侧边栏菜单 ================= */
.ios-menu {
  --n-item-height: 40px !important;
}

/* 分组标题 */
.ios-menu .n-menu-item-group-title {
  font-size: 12px;
  font-weight: 590;
  letter-spacing: 0.5px;
  text-transform: uppercase;
  color: var(--text-color) !important;
  opacity: 0.4;
  padding: 20px 20px 6px 20px !important;
}

.ios-menu .n-menu-item-group:first-child .n-menu-item-group-title {
  padding-top: 4px !important;
}

/* 菜单项 */
.ios-menu .n-menu-item {
  margin: 0 8px 4px 8px !important;
}

.ios-menu .n-menu-item-content {
  font-size: 13px !important;
  border-radius: 10px !important;
  padding: 0 12px !important;
  overflow: visible !important;
  position: relative !important;
  backdrop-filter: blur(16px) saturate(130%);
  -webkit-backdrop-filter: blur(16px) saturate(130%);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
}

/* 菜单项 hover — 极淡玻璃背景 */
.ios-menu .n-menu-item-content:hover {
  background: rgba(128, 128, 128, 0.05) !important;
}

/* 菜单项 active — ::before 伪元素不被裁剪 */
.ios-menu .n-menu-item-content--selected,
.ios-menu .n-menu-item-content--child-active {
  background: transparent !important;
  backdrop-filter: blur(24px) saturate(150%) !important;
  -webkit-backdrop-filter: blur(24px) saturate(150%) !important;
}

.ios-menu .n-menu-item-content--selected::before,
.ios-menu .n-menu-item-content--child-active::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0; bottom: 0;
  border-radius: inherit;
  pointer-events: none;
  z-index: 0;
  background: rgba(128, 128, 128, 0.12);
  backdrop-filter: blur(24px) saturate(150%);
  -webkit-backdrop-filter: blur(24px) saturate(150%);
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.06);
}

.ios-menu .n-menu-item-content--selected .n-menu-item-content__icon,
.ios-menu .n-menu-item-content--child-active .n-menu-item-content__icon {
  filter: drop-shadow(0 0 4px var(--accent-glow-color));
}

.ios-menu .n-menu-item-content--selected:hover,
.ios-menu .n-menu-item-content--child-active:hover {
  background: rgba(128, 128, 128, 0.18) !important;
}

.ios-menu .n-menu-item-content--selected .n-menu-item-content__icon,
.ios-menu .n-menu-item-content--child-active .n-menu-item-content__icon {
  filter: drop-shadow(0 0 4px var(--accent-glow-color));
}

/* 菜单文字 */
.ios-menu .n-menu-item-content__icon {
  font-size: 18px !important;
  color: var(--text-color) !important;
  opacity: 0.6;
  transition: opacity 0.2s;
}

.ios-menu .n-menu-item-content:hover .n-menu-item-content__icon {
  opacity: 0.85;
}

.ios-menu .n-menu-item-content--selected .n-menu-item-content__icon {
  opacity: 1;
  color: var(--accent-color) !important;
}

/* 折叠态 */
.ios-menu.n-menu--collapsed .n-menu-item {
  margin: 0 auto 4px auto !important;
}

.ios-menu.n-menu--collapsed .n-menu-item-content {
  justify-content: center;
  padding: 0 !important;
}

.ios-menu.n-menu--collapsed .n-menu-item-content__icon {
  font-size: 20px !important;
  opacity: 0.7;
}

/* ================= 移动端侧栏 (iOS Files 抽屉风格) ================= */
@media (max-width: 767px) {
  .app-header {
    height: 48px;
  }

  .app-header-inner {
    padding: 0 12px;
  }

  .header-logo {
    height: 24px;
    width: 24px;
  }

  .app-main {
    height: calc(100vh - 48px);
  }

  .mobile-sider {
    position: fixed !important;
    left: 0;
    top: 0;
    bottom: 0;
    z-index: 1200;
    height: 100vh;
    width: 280px !important;
    border-radius: 0 var(--radius-sidebar-mobile) var(--radius-sidebar-mobile) 0 !important;
    box-shadow: 4px 0 24px rgba(0, 0, 0, 0.2) !important;
    transform: translateX(-100%);
    transition: transform 0.35s cubic-bezier(0.22, 1, 0.36, 1) !important;
    overflow: hidden;
  }

  .mobile-sider.sider-open {
    transform: translateX(0) !important;
  }

  .mobile-sider-mask {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.35);
    z-index: 1199;
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
  }

  .ios-menu .n-menu-item {
    margin: 0 12px 4px 12px !important;
  }
}

/* ================= iOS 淡入动画 ================= */
.ios-fade-enter-active {
  transition: opacity 0.25s ease-out;
}
.ios-fade-leave-active {
  transition: opacity 0.2s ease-in;
}
.ios-fade-enter-from,
.ios-fade-leave-to {
  opacity: 0;
}

/* ================= 拖拽相关样式 ================= */
.draggable-item {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  padding: 4px 8px;
  border-radius: var(--radius-inner);
  transition: background-color 0.2s;
  border: 1px solid transparent;
}

.draggable-item:hover {
  background-color: rgba(128, 128, 128, 0.05);
}

.draggable-item.is-dragging {
  opacity: 0.4;
}

.drag-over-top {
  border-top: 2px solid var(--accent-color, #2080f0) !important;
}

.drag-over-bottom {
  border-bottom: 2px solid var(--accent-color, #2080f0) !important;
}

.empty-group-dropzone {
  height: 32px;
  border: 1px dashed var(--card-border-color);
  border-radius: var(--radius-inner);
  display: flex;
  align-items: center;
  justify-content: center;
  opacity: 0.4;
  font-size: 12px;
  margin-bottom: 8px;
}
</style>
