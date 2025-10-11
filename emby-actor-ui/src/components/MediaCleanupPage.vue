<template>
  <n-layout content-style="padding: 24px;">
    <div class="cleanup-page">
      <n-page-header>
        <template #title>
        <n-space align="center">
            <span>重复项清理</span>
            <n-tag v-if="!isLoading" type="info" round :bordered="false" size="small">
            {{ allTasks.length }} 组待处理
            </n-tag>
        </n-space>
        </template>
        <n-alert title="操作提示" type="warning" style="margin-top: 24px;">
        本模块用于查找并清理媒体库中的“重复项”问题（多个独立的媒体项指向了同一个电影/剧集）。<br />
        如果你的重复媒体被神医插件合并，请先使用“神医”插件的“一键拆分多版本”功能，再重新扫描。<br />
        **所有清理操作都会从 Emby 和硬盘中永久删除文件，是高危操作，请谨慎使用！**
        </n-alert>
        <template #extra>
          <n-space>
            <n-dropdown 
              trigger="click"
              :options="batchActions"
              @select="handleBatchAction"
            >
              <n-button type="error" :disabled="selectedTasks.size === 0">
                批量操作 ({{ selectedTasks.size }})
              </n-button>
            </n-dropdown>
            
            <n-button @click="showSettingsModal = true">
              <template #icon><n-icon :component="SettingsIcon" /></template>
              清理规则
            </n-button>

            <n-button 
              type="primary" 
              @click="triggerScan" 
              :loading="isTaskRunning('扫描媒体库重复项')"
            >
              <template #icon><n-icon :component="ScanIcon" /></template>
              扫描媒体库
            </n-button>
          </n-space>
        </template>
      </n-page-header>
      
      <!-- ★★★ START: 新增的筛选和排序控件 ★★★ -->
      <n-space justify="space-between" align="center" style="margin-top: 24px;">
        <n-space align="center">
          <n-input
            v-model:value="searchQuery"
            placeholder="按名称搜索..."
            clearable
            style="width: 200px;"
          />
          <n-select
            v-model:value="filterStatus"
            :options="statusOptions"
            style="width: 140px;"
          />
          <n-select
            v-model:value="filterSeries"
            :options="seriesOptions"
            placeholder="所有剧集"
            filterable
            clearable
            style="width: 220px;"
          />
        </n-space>
        <n-space align="center">
          <n-select
            v-model:value="sortBy"
            :options="sortOptions"
            style="width: 180px;"
          />
          <n-button-group>
            <n-button @click="sortOrder = 'asc'" :type="sortOrder === 'asc' ? 'primary' : 'default'">
              <template #icon><n-icon :component="ArrowUpIcon" /></template>
              升序
            </n-button>
            <n-button @click="sortOrder = 'desc'" :type="sortOrder === 'desc' ? 'primary' : 'default'">
              <template #icon><n-icon :component="ArrowDownIcon" /></template>
              降序
            </n-button>
          </n-button-group>
        </n-space>
      </n-space>
      <!-- ★★★ END: 新增的筛选和排序控件 ★★★ -->

      <n-divider />

      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <!-- ★★★ 修改点: 将 :data 绑定到新的计算属性 displayedTasks ★★★ -->
      <div v-else-if="displayedTasks.length > 0">
        <n-data-table
          :columns="columns"
          :data="displayedTasks"
          :pagination="pagination"
          :row-key="row => row.id"
          v-model:checked-row-keys="selectedTaskIds"
        />
      </div>
      <div v-else class="center-container">
        <n-empty description="太棒了！没有发现任何需要清理的项目。" size="huge" />
      </div>

      <n-modal 
        v-model:show="showSettingsModal" 
        preset="card" 
        style="width: 90%; max-width: 700px;" 
        title="媒体去重决策规则"
        :on-after-leave="fetchData"
        class="modal-card-lite" 
      >
        <MediaCleanupSettingsPage @on-close="showSettingsModal = false" />
      </n-modal>

    </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, computed, h } from 'vue';
import axios from 'axios';
import { 
  NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, 
  useMessage, NSpin, NAlert, NDataTable, NDropdown, useDialog, 
  NTooltip, NText, NModal, NInput, NSelect, NButtonGroup
} from 'naive-ui';
import { 
  ScanCircleOutline as ScanIcon, 
  TrashBinOutline as DeleteIcon, 
  CheckmarkCircleOutline as KeepIcon,
  SettingsOutline as SettingsIcon,
  // ★★★ 新增图标导入 ★★★
  ArrowUpOutline as ArrowUpIcon,
  ArrowDownOutline as ArrowDownIcon
} from '@vicons/ionicons5';
import MediaCleanupSettingsPage from './settings/MediaCleanupSettingsPage.vue';

const props = defineProps({ taskStatus: { type: Object, required: true } });
const message = useMessage();
const dialog = useDialog();

const allTasks = ref([]);
const isLoading = ref(true);
const error = ref(null);
const selectedTasks = ref(new Set());
const showSettingsModal = ref(false);

// ★★★ START: 新增用于筛选和排序的状态 ★★★
const searchQuery = ref('');
const filterStatus = ref('all');
const filterSeries = ref(null);
const sortBy = ref('id'); // 默认按检查时间（用ID代替）降序
const sortOrder = ref('desc');

// 筛选和排序的选项
const statusOptions = ref([
  { label: '所有状态', value: 'all' },
]);

const seriesOptions = computed(() => {
  const series = new Set(allTasks.value.map(task => task.item_name));
  const options = Array.from(series).map(name => ({ label: name, value: name }));
  // 按名称排序
  return options.sort((a, b) => a.label.localeCompare(b.label));
});

const sortOptions = ref([
  { label: '按上次检查时间', value: 'id' }, // 使用 ID 作为检查时间的代理
  { label: '按名称', value: 'item_name' },
]);

// ★★★ 核心逻辑: 创建一个计算属性来处理筛选和排序后的数据 ★★★
const displayedTasks = computed(() => {
  let tasks = [...allTasks.value];

  // 1. 按名称搜索
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    tasks = tasks.filter(task => task.item_name.toLowerCase().includes(query));
  }

  // 2. 按剧集筛选
  if (filterSeries.value) {
    tasks = tasks.filter(task => task.item_name === filterSeries.value);
  }
  
  // 3. 按状态筛选 (未来可扩展)
  // if (filterStatus.value !== 'all') { ... }

  // 4. 排序
  tasks.sort((a, b) => {
    const valA = a[sortBy.value];
    const valB = b[sortBy.value];
    
    let comparison = 0;
    if (typeof valA === 'string') {
      comparison = valA.localeCompare(valB);
    } else {
      comparison = valA > valB ? 1 : (valA < valB ? -1 : 0);
    }
    
    return sortOrder.value === 'desc' ? -comparison : comparison;
  });

  return tasks;
});
// ★★★ END: 新增逻辑 ★★★


const selectedTaskIds = computed({
  get: () => Array.from(selectedTasks.value),
  set: (keys) => { selectedTasks.value = new Set(keys); }
});

const isTaskRunning = (taskName) => props.taskStatus.is_running && props.taskStatus.current_action.includes(taskName);

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  selectedTasks.value.clear();
  try {
    const response = await axios.get('/api/cleanup/tasks');
    allTasks.value = response.data;
  } catch (err) {
    error.value = err.response?.data?.error || '获取重复项列表失败。';
  } finally {
    isLoading.value = false;
  }
};

const triggerScan = () => {
  dialog.info({
    title: '确认开始扫描',
    content: '扫描会检查全库媒体的重复项问题，根据媒体库大小可能需要一些时间。确定要开始吗？',
    positiveText: '开始扫描',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.post('/api/tasks/run', { 
          task_name: 'scan-cleanup-issues' 
        });
        message.success('扫描任务已提交到后台，请稍后查看任务状态。');
      } catch (err) {
        message.error(err.response?.data?.error || '提交扫描任务失败。');
      }
    }
  });
};

const batchActions = computed(() => [
  { label: `执行清理 (${selectedTasks.value.size}项)`, key: 'execute', props: { type: 'error' } },
  { label: `忽略 (${selectedTasks.value.size}项)`, key: 'ignore' },
  { label: `从列表移除 (${selectedTasks.value.size}项)`, key: 'delete' }
]);

const handleBatchAction = (key) => {
  const ids = Array.from(selectedTasks.value);
  if (ids.length === 0) return;

  if (key === 'execute') {
    dialog.warning({
      title: '高危操作确认',
      content: `确定要清理选中的 ${ids.length} 组重复项吗？此操作将永久删除多余的媒体文件，且不可恢复！`,
      positiveText: '我确定，执行清理！',
      negativeText: '取消',
      onPositiveClick: () => executeCleanup(ids)
    });
  } else if (key === 'ignore') {
    ignoreTasks(ids);
  } else if (key === 'delete') {
    deleteTasks(ids);
  }
};

const executeCleanup = async (ids) => {
  try {
    await axios.post('/api/cleanup/execute', { task_ids: ids });
    message.success('清理任务已提交到后台执行。');
    fetchData(); // 重新加载数据
  } catch (err) {
    message.error(err.response?.data?.error || '提交清理任务失败。');
  }
};

const ignoreTasks = async (ids) => {
  try {
    const response = await axios.post('/api/cleanup/ignore', { task_ids: ids });
    message.success(response.data.message);
    fetchData(); // 重新加载数据
  } catch (err) {
    message.error(err.response?.data?.error || '忽略任务失败。');
  }
};

const deleteTasks = async (ids) => {
  try {
    const response = await axios.post('/api/cleanup/delete', { task_ids: ids });
    message.success(response.data.message);
    fetchData(); // 重新加载数据
  } catch (err) {
    message.error(err.response?.data?.error || '删除任务失败。');
  }
};

const formatBytes = (bytes, decimals = 2) => {
  if (!bytes || bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
};

const formatEffectTagForDisplay = (tag) => {
  if (!tag) return 'SDR';
  const tag_lower = String(tag).toLowerCase();
  if (tag_lower === 'dovi_p8') return 'DoVi P8';
  if (tag_lower === 'dovi_p7') return 'DoVi P7';
  if (tag_lower === 'dovi_p5') return 'DoVi P5';
  if (tag_lower === 'dovi_other') return 'DoVi (Other)';
  if (tag_lower === 'hdr10+') return 'HDR10+';
  return tag_lower.toUpperCase();
};

const columns = computed(() => [
  { 
    type: 'selection',
  },
  { 
    title: '媒体项', 
    key: 'item_name',
    sorter: 'default',
    render(row) {
      return h('strong', null, row.item_name);
    }
  },
  {
    title: '版本详情',
    key: 'versions_info_json',
    render(row) {
      const versions = row.versions_info_json || [];
      
      const getVersionDisplayInfo = (v) => {
        return {
          resolution: v.resolution || 'Unknown',
          quality: (v.quality || 'Unknown').toUpperCase(),
          effect: formatEffectTagForDisplay(v.effect),
          size: formatBytes(v.filesize || 0)
        };
      };

      const sortedVersions = [...versions].sort((a, b) => {
        if (a.id === row.best_version_id) return -1;
        if (b.id === row.best_version_id) return 1;
        return 0;
      });

      return h(NSpace, { vertical: true, size: 'small' }, {
        default: () => sortedVersions.map(v => {
          const isBest = v.id === row.best_version_id;
          const icon = isBest ? KeepIcon : DeleteIcon;
          const iconColor = isBest ? 'var(--n-success-color)' : 'var(--n-error-color)';
          const tooltipText = isBest ? '保留此版本' : '删除此版本';
          const displayInfo = getVersionDisplayInfo(v);
          
          return h(NTooltip, null, {
            trigger: () => h('div', { style: 'display: flex; align-items: center; gap: 8px;' }, [
              h(NIcon, { component: icon, color: iconColor, size: 16 }),
              h(NSpace, { size: 'small' }, {
                default: () => [
                  h(NTag, { size: 'small', bordered: false }, { default: () => displayInfo.resolution }),
                  h(NTag, { size: 'small', bordered: false, type: 'info' }, { default: () => displayInfo.quality }),
                  h(NTag, { size: 'small', bordered: false, type: 'warning' }, { default: () => displayInfo.effect }),
                  h(NTag, { size: 'small', bordered: false, type: 'success' }, { default: () => displayInfo.size }),
                ]
              }),
              h(NText, { style: `font-weight: ${isBest ? 'bold' : 'normal'}; margin-left: 8px;` }, { 
                default: () => v.path
              })
            ]),
            default: () => tooltipText
          });
        })
      });
    }
  }
]);

// ★★★ 修改点: 让分页基于筛选后的数据 ★★★
const pagination = computed(() => {
  if (displayedTasks.value.length > 20) {
    return {
      pageSize: 20,
      pageSizes: [20, 50, 100, { label: '全部', value: displayedTasks.value.length }],
      showSizePicker: true,
    };
  }
  return false;
});

onMounted(fetchData);
</script>

<style scoped>
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: calc(100vh - 350px); /* 增加了筛选条的高度 */
}
</style>