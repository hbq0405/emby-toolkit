<!-- src/MainLayout.vue -->
<template>
  <n-layout style="height: 100vh; position: relative;">
    <n-layout-header :bordered="false" class="app-header">
      <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
        
        <!-- 左侧：Logo 与 菜单按钮 -->
        <div style="display: flex; align-items: center;">
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
                <span style="font-size: 14px;">
                  {{ isMobile ? '' : `欢迎, ${authStore.username}` }}
                </span>
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

            <!-- 明暗模式切换器 -->
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
        <!-- ★ 增加 v-if 确保数据加载完成后再渲染，以使 default-expanded-keys 生效 -->
        <n-menu
          v-if="isMenuReady"
          :collapsed="collapsed"
          :collapsed-width="64"
          :collapsed-icon-size="22"
          :options="menuOptions"
          :value="activeMenuKey"
          :default-expanded-keys="defaultExpandedKeys"
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

    <!-- ★★★ 自定义菜单编辑器模态框 ★★★ -->
    <n-modal v-model:show="isMenuEditorVisible" preset="card" style="width: 95%; max-width: 650px;" title="自定义侧边栏菜单 (全局生效)">
      <div style="max-height: 60vh; overflow-y: auto; padding-right: 10px; user-select: none;">
        
        <div v-for="(group, gIndex) in menuConfigTree" :key="group.key" style="margin-bottom: 20px;">
          <!-- 一级菜单配置 -->
          <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px; background: rgba(128,128,128,0.1); padding: 8px; border-radius: 6px;">
            <n-switch v-model:value="group.visible" size="small" />
            <n-input v-model:value="group.label" :placeholder="getOriginalLabel(group.key)" size="small" style="width: 180px;" />
            <span style="color: #888; font-size: 12px; flex: 1;">(一级菜单)</span>
            
            <!-- 默认展开/折叠控制 -->
            <div style="display: flex; align-items: center; gap: 4px;">
              <span style="font-size: 12px; color: #666;">默认展开:</span>
              <n-switch v-model:value="group.expanded" size="small" />
            </div>
          </div>
          
          <!-- 二级菜单配置 (支持拖拽) -->
          <div 
            style="padding-left: 24px; min-height: 10px;"
            @dragover.prevent="onDragOverEmpty($event, gIndex)"
            @drop="onDropEmpty($event, gIndex)"
          >
            <div 
              v-for="(child, cIndex) in group.children" 
              :key="child.key" 
              class="draggable-item"
              :class="{
                'drag-over-top': dragTarget?.gIndex === gIndex && dragTarget?.cIndex === cIndex && dragPosition === 'top',
                'drag-over-bottom': dragTarget?.gIndex === gIndex && dragTarget?.cIndex === cIndex && dragPosition === 'bottom',
                'is-dragging': dragSource?.gIndex === gIndex && dragSource?.cIndex === cIndex
              }"
              draggable="true"
              @dragstart="onDragStart($event, gIndex, cIndex)"
              @dragover.prevent="onDragOver($event, gIndex, cIndex)"
              @drop="onDrop($event, gIndex, cIndex)"
              @dragend="onDragEnd"
            >
              <n-icon size="18" style="cursor: grab; color: #999;"><MenuOutline /></n-icon>
              <n-switch v-model:value="child.visible" size="small" />
              <n-input v-model:value="child.label" :placeholder="getOriginalLabel(child.key)" size="small" style="width: 200px;" />
            </div>
            
            <!-- 空组提示 -->
            <div v-if="group.children.length === 0" class="empty-group-dropzone">
              拖拽菜单至此处
            </div>
          </div>
        </div>

      </div>
      <template #footer>
        <n-space justify="space-between">
          <span style="font-size: 12px; color: #888; line-height: 34px;">提示：按住左侧图标可拖拽排序，支持跨组拖拽。</span>
          <n-space>
            <n-popconfirm @positive-click="resetMenuConfig" negative-text="取消" positive-text="确定">
              <template #trigger>
                <n-button type="error" ghost :loading="isSavingMenu">恢复默认</n-button>
              </template>
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
  ListCircleOutline as MenuEditIcon
} from '@vicons/ionicons5';
import axios from 'axios';
import logo from './assets/logo.png'

const message = useMessage();
const dialog = useDialog();

const isMobile = ref(false);

const checkMobile = () => {
  isMobile.value = window.innerWidth < 768;
};

onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  initMenuConfig(); 
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

const props = defineProps({
  isDark: Boolean,
  selectedTheme: String,
  taskStatus: Object
});
const emit = defineEmits(['update:is-dark', 'update:selected-theme', 'edit-custom-theme']);

const router = useRouter(); 
const route = useRoute(); 
const authStore = useAuthStore();

const collapsed = ref(true);
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

  const buildMenu = (treeNodes) => {
    return treeNodes.map(node => {
      if (!node.visible) return null;
      
      const baseNode = baseMenuMap.value.get(node.key);
      if (!baseNode) return null; // 防御性检查

      const result = {
        key: node.key,
        label: node.label || baseNode.defaultLabel,
        icon: baseNode.icon
      };

      if (node.children && node.children.length > 0) {
        result.children = buildMenu(node.children);
        if (result.children.length === 0) return null; // 子项全隐藏，父项也隐藏
      }
      return result;
    }).filter(Boolean);
  };

  return buildMenu(menuConfigTree.value);
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
/* ... (保留你原有的 style 代码不变) ... */
.app-header { padding: 0 16px; height: 60px; display: flex; align-items: center; font-size: 1.25em; font-weight: 600; flex-shrink: 0; }
.app-main-content-wrapper { height: 100%; display: flex; flex-direction: column; }
.page-content-inner-wrapper { flex-grow: 1; overflow-y: auto; }
.n-menu .n-menu-item-group-title { font-size: 12px; font-weight: 500; color: #8e8e93; padding-left: 24px; margin-top: 16px; margin-bottom: 8px; }
.n-menu .n-menu-item-group:first-child .n-menu-item-group-title { margin-top: 0; }
html.dark .n-menu .n-menu-item-group-title { color: #828287; }

.header-task-status { flex: 2; display: flex; justify-content: center; align-items: center; margin: 0 20px; overflow: hidden; min-width: 0; }
.status-content { display: flex; align-items: center; background-color: rgba(0, 0, 0, 0.03); padding: 4px 12px; border-radius: 20px; border: 1px solid rgba(0, 0, 0, 0.05); max-width: 100%; }
html.dark .status-content { background-color: rgba(255, 255, 255, 0.05); border: 1px solid rgba(255, 255, 255, 0.05); }
.status-text { font-size: 13px; display: flex; align-items: center; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1; min-width: 0; }
.status-divider { margin: 0 8px; opacity: 0.5; flex-shrink: 0; }
.status-message { opacity: 0.8; max-width: 600px; overflow: hidden; text-overflow: ellipsis; display: inline-block; vertical-align: bottom; }

@media (max-width: 768px) {
  .app-header { padding: 0 12px; }
  .status-message { max-width: 150px; }
  .header-task-status { margin: 0 8px; flex: 1; }
  .mobile-sider { position: absolute; left: 0; top: 0; bottom: 0; z-index: 1000; height: 100%; box-shadow: 2px 0 8px rgba(0,0,0,0.15); }
  .mobile-sider-mask { position: absolute; top: 0; left: 0; right: 0; bottom: 0; background-color: rgba(0,0,0,0.4); z-index: 999; backdrop-filter: blur(2px); }
  .n-layout-content .page-content-inner-wrapper { padding: 12px !important; }
}

/* ★★★ 拖拽相关样式 ★★★ */
.draggable-item {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  padding: 4px 8px;
  border-radius: 4px;
  transition: background-color 0.2s;
  border: 1px solid transparent;
}

.draggable-item:hover {
  background-color: rgba(128, 128, 128, 0.05);
}

/* 正在被拖拽的元素半透明 */
.draggable-item.is-dragging {
  opacity: 0.4;
}

/* 拖拽目标指示线 */
.drag-over-top {
  border-top: 2px solid #2080f0 !important;
}

.drag-over-bottom {
  border-bottom: 2px solid #2080f0 !important;
}

/* 空组的拖拽区域 */
.empty-group-dropzone {
  height: 32px;
  border: 1px dashed #ccc;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #999;
  font-size: 12px;
  margin-bottom: 8px;
}
html.dark .empty-group-dropzone {
  border-color: #555;
}
</style>