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
          <li>本模块用于查找并清理媒体库中的多版本（一个媒体项有多个版本）和重复项（多个独立的媒体项指向了同一个电影/剧集）。</li>
          <li>首先按需配置清理规则，扫描的时候会自动标记出保留的唯一版本，其他所有版本会标记为待清理。</li>
          <li>扫描结束后，刷新本页即可展示待清理媒体项。可多选批量清理，也可以一键清理所有。</li>
        </n-alert>
        <template #extra>
          <n-space>
            <n-dropdown 
              trigger="click"
              :options="batchActions"
              @select="handleBatchAction"
            >
              <n-button type="error" :disabled="selectedSeriesNames.length === 0">
                批量操作 ({{ selectedSeriesNames.length }})
              </n-button>
            </n-dropdown>

            <n-button 
              type="warning" 
              @click="handleClearAllTasks" 
              :disabled="allTasks.length === 0"
            >
              <template #icon><n-icon :component="DeleteIcon" /></template>
              一键清理
            </n-button>
            
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
      <n-divider />

      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <div v-else-if="groupedTasks.length > 0">
        <n-data-table
          :columns="seriesColumns"
          :data="groupedTasks"
          :pagination="pagination"
          :row-key="row => `${row.isMovie ? 'movie' : 'series'}-${row.seriesName}`"
          v-model:checked-row-keys="selectedSeriesNames"
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
  NTooltip, NText, NModal
} from 'naive-ui';
import { 
  ScanCircleOutline as ScanIcon, 
  TrashBinOutline as DeleteIcon, 
  CheckmarkCircleOutline as KeepIcon,
  SettingsOutline as SettingsIcon,
  TvOutline as SeriesIcon,
  FilmOutline as MovieIcon
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
const selectedSeriesNames = ref([]); // 存储的是 mapKey

const selectedTaskIds = computed(() => {
  const ids = [];
  const selectedKeysSet = new Set(selectedSeriesNames.value); // 现在存储的是 mapKey
  
  groupedTasks.value.forEach(group => {
    const groupKey = `${group.isMovie ? 'movie' : 'series'}-${group.seriesName}`;
    if (selectedKeysSet.has(groupKey)) {
      group.episodes.forEach(task => {
        ids.push(task.id);
      });
    }
  });
  return ids;
});

const isTaskRunning = (taskName) => props.taskStatus.is_running && props.taskStatus.current_action.includes(taskName);

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  selectedSeriesNames.value = []; // 清空勾选
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

const groupedTasks = computed(() => {
  const seriesMap = new Map();

  allTasks.value.forEach(task => {
    const itemType = task.item_type ? String(task.item_type).toLowerCase() : '';
    let currentSeriesName;
    let isCurrentTaskMovie = false;
    let mapKey;

    // 检查 itemType 是否包含 'movie' 或 '电影' 字符串，以更健壮地识别电影
    if (itemType.includes('movie') || itemType.includes('电影')) {
      currentSeriesName = task.item_name;
      isCurrentTaskMovie = true;
      mapKey = `movie-${currentSeriesName}`; // 使用前缀区分电影
    } else { // 默认视为剧集
      const seriesNameMatch = task.item_name.match(/^(.*)\sS\d{2}E\d{2}/);
      currentSeriesName = seriesNameMatch ? seriesNameMatch[1] : task.item_name;
      mapKey = `series-${currentSeriesName}`; // 使用前缀区分剧集
    }

    if (!seriesMap.has(mapKey)) {
      seriesMap.set(mapKey, {
        seriesName: currentSeriesName,
        isMovie: isCurrentTaskMovie,
        episodes: []
      });
    }
    // 确保 isMovie 属性的正确性，如果当前任务是电影，则强制设置为电影
    if (isCurrentTaskMovie) {
        seriesMap.get(mapKey).isMovie = true;
    }
    seriesMap.get(mapKey).episodes.push(task);
  });

  return Array.from(seriesMap.values());
});

// --- 重新定义表格列 ---

// 内层展开表格的列定义
const episodeColumns = [
  {
    title: '媒体项',
    key: 'item_name',
    render(row) {
      const episodeNameMatch = row.item_name.match(/(S\d{2}E\d{2}.*)/);
      const displayName = episodeNameMatch ? episodeNameMatch[1] : row.item_name;
      
      const issueType = row.task_type === 'Duplicate' ? '重复项' : '多版本';
      const tagType = row.task_type === 'Duplicate' ? 'error' : 'info';

      return h('div', { style: 'display: flex; align-items: center; gap: 8px;' }, [
        h(NTag, {
            type: tagType,
            bordered: false,
            size: 'small'
        }, { default: () => issueType }),
        h('span', displayName)
      ]);
    }
  },
  {
    title: '版本详情',
    key: 'versions_info_json',
    // ★★★ 把原来那个功能完备的 render 函数完整地复制到这里 ★★★
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
              // 注意：这里可能需要从 v.Path 获取路径，如果你的数据结构是这样的话
              h(NText, { style: `font-weight: ${isBest ? 'bold' : 'normal'}; margin-left: 8px;` }, { 
                default: () => v.Path || v.path 
              })
            ]),
            default: () => tooltipText
          });
        })
      });
    }
  }
];

// 外层主表格的列定义
const seriesColumns = computed(() => [
  { type: 'selection' },
  { 
    type: 'expand',
    expandable: (rowData) => !rowData.isMovie, // 电影行不可展开
    renderExpand: (rowData) => {
      return h(NDataTable, {
        columns: episodeColumns,
        data: rowData.episodes,
        size: 'small',
        bordered: false,
        rowKey: row => row.id
      });
    }
  },
  {
    title: '剧集 / 电影',
    key: 'seriesName',
    render(row) {
      const iconComponent = row.isMovie ? MovieIcon : SeriesIcon;
      return h(NSpace, { align: 'center' }, {
        default: () => [
          h(NIcon, { component: iconComponent, size: 20 }),
          h('strong', row.seriesName),
          h(NTag, { type: 'info', round: true, size: 'small' }, { default: () => `${row.episodes.length} 项` })
        ]
      });
    }
  },
  {
    title: '版本详情',
    key: 'movieVersionsInfo', // 新增一个key
    render(row) {
      if (!row.isMovie || !row.episodes || row.episodes.length === 0) {
        return null; // 如果不是电影或者没有版本信息，则不显示
      }

      // 电影只有一个“剧集”项，直接取第一个
      const movieTask = row.episodes[0]; 
      const versions = movieTask.versions_info_json || [];
      
      const getVersionDisplayInfo = (v) => {
        return {
          resolution: v.resolution || 'Unknown',
          quality: (v.quality || 'Unknown').toUpperCase(),
          effect: formatEffectTagForDisplay(v.effect),
          size: formatBytes(v.filesize || 0)
        };
      };

      const sortedVersions = [...versions].sort((a, b) => {
        if (a.id === movieTask.best_version_id) return -1;
        if (b.id === movieTask.best_version_id) return 1;
        return 0;
      });

      return h(NSpace, { vertical: true, size: 'small' }, {
        default: () => sortedVersions.map(v => {
          const isBest = v.id === movieTask.best_version_id;
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
                default: () => v.Path || v.path 
              })
            ]),
            default: () => tooltipText
          });
        })
      });
    }
  }
]);

const batchActions = computed(() => [
  { label: `执行清理 (${selectedSeriesNames.value.length}项)`, key: 'execute', props: { type: 'error' } },
  { label: `忽略 (${selectedSeriesNames.value.length}项)`, key: 'ignore' },
  { label: `从列表移除 (${selectedSeriesNames.value.length}项)`, key: 'delete' }
]);

const handleBatchAction = (key) => {
  const ids = selectedTaskIds.value; // 使用新的计算属性
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
    fetchData(); // 操作成功后，重新加载数据，这是最简单可靠的方式
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

const handleClearAllTasks = () => {
  dialog.warning({
    title: '高危操作确认',
    content: '确定要一键清理所有重复项任务吗？此操作将永久删除多余的媒体文件，且不可恢复！',
    positiveText: '我确定，一键清理！',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const response = await axios.post('/api/cleanup/clear_all');
        message.success(response.data.message);
        fetchData(); // 清除成功后，重新加载数据
      } catch (err) {
        message.error(err.response?.data?.error || '一键清理任务失败。');
      }
    }
  });
};

const formatBytes = (bytes, decimals = 2) => {
  if (!bytes || bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
};

// ★★★ 核心修改 1/3: 新增一个特效标签的“翻译”函数 ★★★
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
      
      // ★★★ 核心修改 2/3: 简化 getVersionDisplayInfo 函数 ★★★
      const getVersionDisplayInfo = (v) => {
        // 后端已经计算好了所有标准化属性，前端只负责展示
        return {
          resolution: v.resolution || 'Unknown',
          quality: (v.quality || 'Unknown').toUpperCase(),
          // 调用新的翻译函数来显示特效
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
                  // ★★★ 核心修改 3/3: 直接使用 displayInfo 中的值 ★★★
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

const pagination = computed(() => {
  if (allTasks.value.length > 20) {
    return {
      pageSize: 20,
      pageSizes: [20, 50, 100, { label: '全部', value: allTasks.value.length }],
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
  height: calc(100vh - 300px);
}
</style>
