<!-- src/components/settings/SchedulerSettingsPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <!-- 加载状态 -->
    <div v-if="isLoading" class="center-container">
      <n-spin size="large" />
    </div>
    
    <n-space v-else-if="configModel" vertical :size="24" style="margin-top: 15px;">
      
      <!-- ======================================================================= -->
      <!-- 卡片 1: 高频刷新任务链 -->
      <!-- ======================================================================= -->
      <n-card v-if="!isMobile" :bordered="false" class="dashboard-card">
        <!-- ... 保持原有高频任务链代码不变 ... -->
        <template #header>
          <span class="card-title">高频刷新任务链</span>
        </template>
        <template #header-extra>
          <n-text depth="3">建议白天两小时执行一次，保证实时数据更新，及时订阅资源</n-text>
        </template>
        
        <n-grid cols="1 l:3" :x-gap="24" :y-gap="16" responsive="screen">
          <n-gi span="1">
            <n-space vertical>
              <n-space align="center" justify="space-between">
                <n-text strong>启用高频任务链</n-text>
                <n-switch v-model:value="configModel.task_chain_enabled" />
              </n-space>
              <n-form :model="configModel" label-placement="left" label-width="auto" class="mt-3" :show-feedback="false">
                <n-form-item label="定时执行 (CRON)">
                  <n-input v-model:value="configModel.task_chain_cron" :disabled="!configModel.task_chain_enabled" placeholder="例如: 0 7-23/2 * * *" />
                </n-form-item>
                <n-form-item label="最大运行时长 (分钟)">
                  <n-input-number 
                    v-model:value="configModel.task_chain_max_runtime_minutes" 
                    :min="0" 
                    :step="10" 
                    :disabled="!configModel.task_chain_enabled"
                    placeholder="0 代表不限制"
                    style="width: 100%;"
                  />
                </n-form-item>
                <n-form-item label="任务序列">
                  <n-button-group>
                    <n-button type="default" @click="showHighFreqChainConfigModal = true" :disabled="!configModel.task_chain_enabled">
                      <template #icon><n-icon :component="Settings24Regular" /></template>
                      配置
                    </n-button>
                  </n-button-group>
                </n-form-item>
              </n-form>
              <n-button 
                type="primary" 
                ghost 
                style="width: 100%; margin-top: 8px;" 
                @click="triggerTaskNow('task-chain-high-freq')"
                :loading="isTriggeringTask === 'task-chain-high-freq'"
                :disabled="isBackgroundTaskRunning"
              >
                <template #icon><n-icon :component="Play24Regular" /></template>
                立即执行任务链
              </n-button>
            </n-space>
          </n-gi>

          <n-gi span="2">
            <n-text strong>当前执行流程</n-text>
            <div class="flowchart-wrapper">
              <div v-if="enabledHighFreqTaskChain.length > 0" class="flowchart-container">
                <div v-for="task in enabledHighFreqTaskChain" :key="task.key" class="flowchart-node">
                  {{ task.name }}
                </div>
              </div>
              <div v-else class="flowchart-container empty">
                <n-text depth="3">暂未配置任何任务...</n-text>
              </div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>

      <!-- ======================================================================= -->
      <!-- 卡片 2: 低频维护任务链 -->
      <!-- ======================================================================= -->
      <n-card v-if="!isMobile" :bordered="false" class="dashboard-card">
        <!-- ... 保持原有低频任务链代码不变 ... -->
        <template #header>
          <span class="card-title">低频维护任务链</span>
        </template>
        <template #header-extra>
          <n-text depth="3">建议夜里空闲时间段执行并限制任务时长，处理资源密集型任务</n-text>
        </template>
        
        <n-grid cols="1 l:3" :x-gap="24" :y-gap="16" responsive="screen">
          <n-gi span="1">
            <n-space vertical>
              <n-space align="center" justify="space-between">
                <n-text strong>启用低频任务链</n-text>
                <n-switch v-model:value="configModel.task_chain_low_freq_enabled" />
              </n-space>
              <n-form :model="configModel" label-placement="left" label-width="auto" class="mt-3" :show-feedback="false">
                <n-form-item label="定时执行 (CRON)">
                  <n-input v-model:value="configModel.task_chain_low_freq_cron" :disabled="!configModel.task_chain_low_freq_enabled" placeholder="例如: 0 1 * * *" />
                </n-form-item>
                <n-form-item label="最大运行时长 (分钟)">
                  <n-input-number 
                    v-model:value="configModel.task_chain_low_freq_max_runtime_minutes" 
                    :min="0" 
                    :step="30" 
                    :disabled="!configModel.task_chain_low_freq_enabled"
                    placeholder="0 代表不限制"
                    style="width: 100%;"
                  />
                </n-form-item>
                <n-form-item label="任务序列">
                   <n-button type="default" @click="showLowFreqChainConfigModal = true" :disabled="!configModel.task_chain_low_freq_enabled">
                      <template #icon><n-icon :component="Settings24Regular" /></template>
                      配置
                    </n-button>
                </n-form-item>
              </n-form>
              <n-button 
                type="primary" 
                ghost 
                style="width: 100%; margin-top: 8px;" 
                @click="triggerTaskNow('task-chain-low-freq')"
                :loading="isTriggeringTask === 'task-chain-low-freq'"
                :disabled="isBackgroundTaskRunning"
              >
                <template #icon><n-icon :component="Play24Regular" /></template>
                立即执行任务链
              </n-button>
            </n-space>
          </n-gi>

          <n-gi span="2">
            <n-text strong>当前执行流程</n-text>
            <div class="flowchart-wrapper">
              <div v-if="enabledLowFreqTaskChain.length > 0" class="flowchart-container">
                <div v-for="task in enabledLowFreqTaskChain" :key="task.key" class="flowchart-node">
                  {{ task.name }}
                </div>
              </div>
              <div v-else class="flowchart-container empty">
                <n-text depth="3">暂未配置任何任务...</n-text>
              </div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>

      <!-- ======================================================================= -->
      <!-- ★★★ 卡片 3: Telegram 菜单配置 ★★★ -->
      <!-- ======================================================================= -->
      <n-card v-if="!isMobile" :bordered="false" class="dashboard-card">
        <template #header>
          <span class="card-title">Telegram 快捷菜单（Pro）</span>
        </template>
        <template #header-extra>
          <n-text depth="3">配置显示在 TG 机器人输入框左侧 Menu 按钮中的快捷任务 (保存后需重启应用生效)</n-text>
        </template>
        
        <n-grid cols="1 l:3" :x-gap="24" :y-gap="16" responsive="screen">
          <n-gi span="1">
            <n-space vertical>
              <n-button type="default" @click="showTgMenuConfigModal = true" style="width: 100%;">
                <template #icon><n-icon :component="Settings24Regular" /></template>
                配置 TG 菜单任务
              </n-button>
            </n-space>
          </n-gi>

          <n-gi span="2">
            <n-text strong>当前 TG 菜单列表</n-text>
            <div class="flowchart-wrapper">
              <div v-if="enabledTgMenuTasks.length > 0" class="flowchart-container">
                <div v-for="task in enabledTgMenuTasks" :key="task.key" class="flowchart-node" style="background-color: var(--n-action-color);">
                  {{ task.name }}
                </div>
              </div>
              <div v-else class="flowchart-container empty">
                <n-text depth="3">暂未配置任何 TG 菜单任务...</n-text>
              </div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>

      <!-- 保存按钮（通用） -->
       <n-button v-if="!isMobile" type="primary" @click="savePageConfig" :loading="savingConfig" size="large" style="width: 100%;">
          <template #icon><n-icon :component="Save24Regular" /></template>
          保存所有配置
        </n-button>

      <!-- 卡片 4: 临时任务 -->
      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <span class="card-title">临时任务</span>
        </template>
        <template #header-extra>
          <n-text depth="3">用于需要立即手动执行的场景</n-text>
        </template>
        <n-grid cols="1 m:2 l:5" :x-gap="24" :y-gap="16" responsive="screen">
          <n-gi v-for="task in availableTasksForManualRun" :key="task.key">
            <div class="temp-task-item">
              <n-text>{{ task.name }}</n-text>
              <n-button size="small" type="primary" ghost @click="triggerTaskNow(task.key)" :loading="isTriggeringTask === task.key" :disabled="isBackgroundTaskRunning">
                <template #icon><n-icon :component="Play24Regular" /></template>
                立即执行
              </n-button>
            </div>
          </n-gi>
        </n-grid>
      </n-card>
      
    </n-space>

    <!-- ======================================================================= -->
    <!-- 模态框 -->
    <!-- ======================================================================= -->

    <!-- 高频任务链配置模态框 -->
    <n-modal v-model:show="showHighFreqChainConfigModal" class="custom-card" preset="card" title="配置高频刷新任务链" style="width: 90%; max-width: 600px;" :mask-closable="false">
      <n-alert type="info" :show-icon="false" style="margin-bottom: 16px;">请勾选需要定时执行的任务，并拖动调整顺序。</n-alert>
      <div class="task-chain-list" ref="draggableContainerHighFreq">
        <div v-for="task in configuredHighFreqTaskSequence" :key="task.key" class="task-chain-item" :data-key="task.key">
          <n-icon :component="Drag24Regular" class="drag-handle" />
          <n-checkbox v-model:checked="task.enabled" style="flex-grow: 1;">{{ task.name }}</n-checkbox>
        </div>
      </div>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showHighFreqChainConfigModal = false">取消</n-button>
          <n-button type="primary" @click="saveHighFreqTaskChainConfig">保存</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- 低频任务链配置模态框 -->
    <n-modal v-model:show="showLowFreqChainConfigModal" class="custom-card" preset="card" title="配置低频维护任务链" style="width: 90%; max-width: 600px;" :mask-closable="false">
      <n-alert type="info" :show-icon="false" style="margin-bottom: 16px;">请勾选需要定时执行的任务，并拖动调整顺序。</n-alert>
      <div class="task-chain-list" ref="draggableContainerLowFreq">
        <div v-for="task in configuredLowFreqTaskSequence" :key="task.key" class="task-chain-item" :data-key="task.key">
          <n-icon :component="Drag24Regular" class="drag-handle" />
          <n-checkbox v-model:checked="task.enabled" style="flex-grow: 1;">{{ task.name }}</n-checkbox>
        </div>
      </div>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showLowFreqChainConfigModal = false">取消</n-button>
          <n-button type="primary" @click="saveLowFreqTaskChainConfig">保存</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- ★★★ 新增：TG 菜单配置模态框 ★★★ -->
    <n-modal v-model:show="showTgMenuConfigModal" class="custom-card" preset="card" title="配置 Telegram 快捷菜单" style="width: 90%; max-width: 600px;" :mask-closable="false">
      <n-alert type="info" :show-icon="false" style="margin-bottom: 16px;">
        请勾选需要在 Telegram 机器人菜单中显示的任务，并拖动调整顺序。<br/>
        注意：Telegram 限制菜单命令最多显示 100 个，建议只保留常用任务。
      </n-alert>
      <div class="task-chain-list" ref="draggableContainerTgMenu">
        <div v-for="task in configuredTgMenuSequence" :key="task.key" class="task-chain-item" :data-key="task.key">
          <n-icon :component="Drag24Regular" class="drag-handle" />
          <n-checkbox v-model:checked="task.enabled" style="flex-grow: 1;">{{ task.name }}</n-checkbox>
        </div>
      </div>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showTgMenuConfigModal = false">取消</n-button>
          <n-button type="primary" @click="saveTgMenuConfig">保存</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- 通用模式选择模态框 -->
    <n-modal v-model:show="showSyncModeModal" preset="dialog" title="选择处理模式" :mask-closable="false">
      <n-text>您希望如何执行此任务？</n-text>
      <template #action>
        <n-button @click="showSyncModeModal = false">取消</n-button>
        <n-button @click="runTaskFromModal(false)">快速模式（增量）</n-button>
        <n-button type="warning" @click="runTaskFromModal(true)">深度模式 (全量)</n-button>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch, nextTick, computed } from 'vue';
import {
  NForm, NFormItem, NInput, NCheckbox, NGrid, NGi, NAlert,
  NButton, NCard, NSpace, NSwitch, NIcon, NText, NInputNumber,
  useMessage, NLayout, NSpin, NModal, NButtonGroup
} from 'naive-ui';
import { Play24Regular, Settings24Regular, Drag24Regular, Save24Regular } from '@vicons/fluent';
import { useConfig } from '../../composables/useConfig.js';
import { useTaskStatus } from '../../composables/useTaskStatus.js';
import axios from 'axios';
import Sortable from 'sortablejs';

const message = useMessage();

// 双模任务列表
const DUAL_MODE_TASKS = [
  'role-translation',    
  'populate-metadata',
  'enrich-aliases',
  'restore_mediainfo'
];

// 默认的 TG 菜单任务（用于初始化）
const DEFAULT_TG_TASKS = [
  'task-chain-high-freq',
  'task-chain-low-freq',
  'scan-organize-115',
  'populate-metadata',
  'process-watchlist',
  'scan-cleanup-issues',
  'system-auto-update'
];

// --- Composable Hooks ---
const {
    configModel,
    loadingConfig: isLoading,
    handleSaveConfig,
    savingConfig,
    configError
} = useConfig();

const { isBackgroundTaskRunning } = useTaskStatus();

// --- State ---
const availableTasksForChain = ref([]); 
const availableTasksForManualRun = ref([]);
const isTriggeringTask = ref(null);

// 高频任务链状态
const showHighFreqChainConfigModal = ref(false);
const configuredHighFreqTaskSequence = ref([]);
const draggableContainerHighFreq = ref(null);
let sortableInstanceHighFreq = null;

// 低频任务链状态
const showLowFreqChainConfigModal = ref(false);
const configuredLowFreqTaskSequence = ref([]);
const draggableContainerLowFreq = ref(null);
let sortableInstanceLowFreq = null;

// ★★★ 新增：TG 菜单配置状态 ★★★
const showTgMenuConfigModal = ref(false);
const configuredTgMenuSequence = ref([]);
const draggableContainerTgMenu = ref(null);
let sortableInstanceTgMenu = null;

// 手动执行任务模态框状态
const showSyncModeModal = ref(false);
const taskToRunInModal = ref(null);

// --- Computed Properties ---
const enabledHighFreqTaskChain = computed(() => {
  if (!configuredHighFreqTaskSequence.value) return [];
  return configuredHighFreqTaskSequence.value.filter(t => t.enabled);
});

const enabledLowFreqTaskChain = computed(() => {
  if (!configuredLowFreqTaskSequence.value) return [];
  return configuredLowFreqTaskSequence.value.filter(t => t.enabled);
});

// ★★★ 新增：TG 菜单 Computed ★★★
const enabledTgMenuTasks = computed(() => {
  if (!configuredTgMenuSequence.value) return [];
  return configuredTgMenuSequence.value.filter(t => t.enabled);
});


// --- API Calls ---
const fetchAvailableTasks = async () => {
  try {
    const chainResponse = await axios.get('/api/tasks/available?context=chain');
    availableTasksForChain.value = chainResponse.data;

    const allResponse = await axios.get('/api/tasks/available?context=all');
    availableTasksForManualRun.value = allResponse.data;

  } catch (error) {
    message.error('获取可用任务列表失败！');
  }
};

const runTaskFromModal = async (isDeepMode) => {
  showSyncModeModal.value = false;
  const taskIdentifier = taskToRunInModal.value;
  if (!taskIdentifier) return;

  isTriggeringTask.value = taskIdentifier;

  try {
    const payload = { task_name: taskIdentifier, force_full_update: isDeepMode };
    const response = await axios.post('/api/tasks/run', payload);
    message.success(response.data.message || '任务已成功提交！');
  } catch (error) {
    const errorMessage = error.response?.data?.error || '请求后端接口失败。';
    message.error(errorMessage);
  } finally {
    isTriggeringTask.value = null;
    taskToRunInModal.value = null;
  }
};

const triggerTaskNow = async (taskIdentifier) => {
  if (isBackgroundTaskRunning.value) {
    message.warning('已有后台任务正在运行，请稍后再试。');
    return;
  }

  if (DUAL_MODE_TASKS.includes(taskIdentifier)) {
    taskToRunInModal.value = taskIdentifier; 
    showSyncModeModal.value = true;
    return; 
  }

  isTriggeringTask.value = taskIdentifier;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: taskIdentifier });
    message.success(response.data.message || `任务已成功提交！`);
  } catch (error) {
    const errorMessage = error.response?.data?.error || '请求后端接口失败。';
    message.error(errorMessage);
  } finally {
    isTriggeringTask.value = null;
  }
};

// --- Logic ---
const savePageConfig = async () => {
  if (configModel.value) {
    // 保存高频、低频和 TG 菜单三个序列
    configModel.value.task_chain_sequence = enabledHighFreqTaskChain.value.map(t => t.key);
    configModel.value.task_chain_low_freq_sequence = enabledLowFreqTaskChain.value.map(t => t.key);
    configModel.value.tg_menu_tasks = enabledTgMenuTasks.value.map(t => t.key);
  }
  const success = await handleSaveConfig();
  if (success) {
    message.success('配置已成功保存！(TG 菜单更新可能需要重启应用生效)');
  } else {
    message.error(configError.value || '配置保存失败。');
  }
};

const saveHighFreqTaskChainConfig = () => {
  showHighFreqChainConfigModal.value = false;
  message.info('高频任务链顺序已更新，请点击页面底部的“保存所有配置”按钮以生效。');
};

const saveLowFreqTaskChainConfig = () => {
  showLowFreqChainConfigModal.value = false;
  message.info('低频任务链顺序已更新，请点击页面底部的“保存所有配置”按钮以生效。');
};

const saveTgMenuConfig = () => {
  showTgMenuConfigModal.value = false;
  message.info('TG 菜单顺序已更新，请点击页面底部的“保存所有配置”按钮以生效。');
};

// ★★★ 修改：增加 sourceTasks 参数，以便复用逻辑 ★★★
const initializeSequence = (savedSequenceKeys, targetConfiguredSequence, sourceTasks) => {
  if (!sourceTasks || sourceTasks.length === 0) return;

  const savedSequenceSet = new Set(savedSequenceKeys);

  const enabledTasks = savedSequenceKeys
    .map(key => {
      const task = sourceTasks.find(t => t.key === key);
      return task ? { ...task, enabled: true } : null;
    })
    .filter(Boolean);

  const disabledTasks = sourceTasks
    .filter(task => !savedSequenceSet.has(task.key))
    .map(task => ({ ...task, enabled: false }));

  targetConfiguredSequence.value = [...enabledTasks, ...disabledTasks];
};

const initializeSortable = (container, sequenceRef, instanceRef) => {
  if (container) {
    instanceRef = Sortable.create(container, {
      animation: 150,
      handle: '.drag-handle',
      onEnd: (evt) => {
        const { oldIndex, newIndex } = evt;
        const item = sequenceRef.value.splice(oldIndex, 1)[0];
        sequenceRef.value.splice(newIndex, 0, item);
      },
    });
    return instanceRef;
  }
  return null;
};

const isMobile = ref(false);
const checkMobile = () => {
  isMobile.value = window.innerWidth < 768;
};

// --- Lifecycle and Watchers ---
onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  fetchAvailableTasks();
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', checkMobile);
});

watch(showHighFreqChainConfigModal, (newValue) => {
  if (newValue) {
    nextTick(() => {
      sortableInstanceHighFreq = initializeSortable(draggableContainerHighFreq.value, configuredHighFreqTaskSequence, sortableInstanceHighFreq);
    });
  } else if (sortableInstanceHighFreq) {
    sortableInstanceHighFreq.destroy();
    sortableInstanceHighFreq = null;
  }
});

watch(showLowFreqChainConfigModal, (newValue) => {
  if (newValue) {
    nextTick(() => {
      sortableInstanceLowFreq = initializeSortable(draggableContainerLowFreq.value, configuredLowFreqTaskSequence, sortableInstanceLowFreq);
    });
  } else if (sortableInstanceLowFreq) {
    sortableInstanceLowFreq.destroy();
    sortableInstanceLowFreq = null;
  }
});

// ★★★ 新增：监听 TG 菜单模态框 ★★★
watch(showTgMenuConfigModal, (newValue) => {
  if (newValue) {
    nextTick(() => {
      sortableInstanceTgMenu = initializeSortable(draggableContainerTgMenu.value, configuredTgMenuSequence, sortableInstanceTgMenu);
    });
  } else if (sortableInstanceTgMenu) {
    sortableInstanceTgMenu.destroy();
    sortableInstanceTgMenu = null;
  }
});

// ★★★ 修改：统一初始化三个列表 ★★★
watch([configModel, availableTasksForChain, availableTasksForManualRun], ([newConfig, chainTasks, allTasks]) => {
  if (newConfig) {
    if (chainTasks.length > 0) {
      initializeSequence(newConfig.task_chain_sequence || [], configuredHighFreqTaskSequence, chainTasks);
      initializeSequence(newConfig.task_chain_low_freq_sequence || [], configuredLowFreqTaskSequence, chainTasks);
    }
    if (allTasks.length > 0) {
      // TG 菜单使用 allTasks (包含所有任务)，如果没有配置则使用默认值
      initializeSequence(newConfig.tg_menu_tasks || DEFAULT_TG_TASKS, configuredTgMenuSequence, allTasks);
    }
  }
}, { immediate: true, deep: true });
</script>

<style scoped>
/* ... 保持原有样式不变 ... */
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: calc(100vh - 200px);
}
.mt-3 {
  margin-top: 12px;
}
.temp-task-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  border: 1px solid var(--n-border-color);
  border-radius: 4px;
}
.task-chain-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.task-chain-item {
  display: flex;
  align-items: center;
  padding: 10px;
  background-color: var(--n-action-color);
  border-radius: 4px;
  border: 1px solid var(--n-border-color);
  transition: background-color 0.3s;
}
.task-chain-item.sortable-ghost {
  background-color: var(--n-color-target-suppl);
}
.drag-handle {
  cursor: grab;
  margin-right: 12px;
  color: var(--n-text-color-disabled);
}
.drag-handle:active {
  cursor: grabbing;
}

/* --- 流程图核心样式 --- */
.flowchart-wrapper {
  margin-top: 12px;
  padding: 16px;
  border-radius: 4px;
  min-height: 100px;
  width: 100%;
}
.flowchart-container {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px 28px; 
}
.flowchart-container.empty {
  justify-content: center;
  align-items: center;
  display: flex;
  height: 100%;
  min-height: 80px;
}
.flowchart-node {
  background-color: var(--n-color);
  border: 1px solid var(--n-border-color);
  padding: 8px 16px;
  border-radius: 20px;
  text-align: center;
  white-space: nowrap;
  position: relative;
}
.flowchart-node:not(:last-child)::after {
  content: '';
  position: absolute;
  right: -24px;
  top: 50%;
  transform: translateY(-50%);
  width: 24px;
  height: 24px;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='currentColor'%3E%3Cpath d='M16.172 11l-5.364-5.364 1.414-1.414L20 12l-7.778 7.778-1.414-1.414L16.172 13H4v-2z'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: center;
  opacity: 0.5;
}

@media (max-width: 1200px) {
  .flowchart-container {
    flex-direction: column;
    align-items: flex-start;
    gap: 28px 8px;
  }
  .flowchart-node {
    width: fit-content;
  }
  .flowchart-node:not(:last-child)::after {
    right: auto;
    left: 50%;
    top: 100%;
    transform: translateX(-50%) translateY(4px);
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='currentColor'%3E%3Cpath d='M13 16.172l5.364-5.364 1.414 1.414L12 20l-7.778-7.778 1.414-1.414L11 16.172V4h2z'/%3E%3C/svg%3E");
  }
}
</style>