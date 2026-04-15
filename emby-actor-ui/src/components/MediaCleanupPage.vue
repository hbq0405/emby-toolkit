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
              扫描重复项
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
          :row-key="row => row.key"  
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
import { ref, onMounted, computed, h, watch } from 'vue';
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

// --- 1. 初始化和 Props ---
const props = defineProps({ taskStatus: { type: Object, required: true } });
const message = useMessage();
const dialog = useDialog();

// --- 2. 响应式状态定义 ---
const allTasks = ref([]);
const isLoading = ref(true);
const error = ref(null);
const showSettingsModal = ref(false);
const selectedSeriesNames = ref([]); 
const currentPage = ref(1);
const currentPageSize = ref(20);

// --- 3. 计算属性 ---

// 根据勾选的行，计算出所有需要操作的任务ID
const selectedTaskIds = computed(() => {
  const ids = [];
  const selectedKeysSet = new Set(selectedSeriesNames.value);
  
  groupedTasks.value.forEach(group => {
    if (selectedKeysSet.has(group.key)) { 
      group.episodes.forEach(task => {
        ids.push(task.id);
      });
    }
  });
  return ids;
});

// 判断特定名称的任务是否正在运行
const isTaskRunning = (taskName) => props.taskStatus.is_running && props.taskStatus.current_action.includes(taskName);

// 跟踪扫描任务的状态
const isScanTaskActive = computed(() => isTaskRunning('扫描媒体库重复项'));

// 将从后端获取的扁平任务列表，按剧集/电影进行分组
const groupedTasks = computed(() => {
  const seriesMap = new Map();

  allTasks.value.forEach(task => {
    let groupKey;
    let groupName;
    let isMovie = false;

    if (task.item_type === 'Movie') {
      groupName = task.item_name;
      groupKey = `movie-${task.tmdb_id}`; 
      isMovie = true;
    } else if (task.item_type === 'Episode') {
      groupName = task.parent_series_name || '未知剧集';
      groupKey = `series-${task.parent_series_tmdb_id}`;
      isMovie = false;
    } else { 
      groupName = task.item_name;
      groupKey = `series-${task.tmdb_id}`;
      isMovie = false;
    }

    if (!seriesMap.has(groupKey)) {
      seriesMap.set(groupKey, {
        key: groupKey,
        seriesName: groupName,
        isMovie: isMovie,
        episodes: []
      });
    }
    seriesMap.get(groupKey).episodes.push(task);
  });

  return Array.from(seriesMap.values());
});

// 主表格的列定义
const seriesColumns = computed(() => [
  { type: 'selection', width: 40 },
  { 
    type: 'expand',
    width: 40,
    expandable: (rowData) => !rowData.isMovie,
    renderExpand: (rowData) => {
      return h(NDataTable, {
        columns: episodeColumns,
        data: rowData.episodes,
        size: 'small',
        bordered: false,
        bottomBordered: false,
        showHeader: false,
        rowKey: row => row.id,
        style: { padding: '4px 24px 4px 48px', backgroundColor: 'rgba(255, 255, 255, 0.02)' } 
      });
    }
  },
  {
    title: '媒体详情',
    key: 'details',
    render(row) {
      const iconComponent = row.isMovie ? MovieIcon : SeriesIcon;
      const headerNode = h(NSpace, { align: 'center', style: 'margin-bottom: 12px;' }, {
        default: () => [
          h(NIcon, { component: iconComponent, size: 24, color: '#2080f0' }),
          h('span', { style: 'font-size: 18px; font-weight: bold;' }, row.seriesName),
          h(NTag, { type: 'info', round: true, size: 'small', bordered: false }, { default: () => `${row.episodes.length} 项` }),
        ]
      });

      let tableNode = null;
      if (row.isMovie && row.episodes && row.episodes.length > 0) {
        const movieTask = row.episodes[0];
        tableNode = renderVersions(movieTask);
      }

      return h('div', { style: 'padding: 8px 0;' }, [
        headerNode,
        tableNode
      ]);
    }
  }
]);

// 批量操作下拉菜单的选项
const batchActions = computed(() => [
  { label: `执行清理 (${selectedSeriesNames.value.length}项)`, key: 'execute', props: { type: 'error' } },
  { label: `忽略 (${selectedSeriesNames.value.length}项)`, key: 'ignore' },
  { label: `从列表移除 (${selectedSeriesNames.value.length}项)`, key: 'delete' }
]);

// 分页配置
const pagination = computed(() => {
  const totalItems = groupedTasks.value.length;
  if (totalItems === 0) return false;

  return {
    page: currentPage.value,
    pageSize: currentPageSize.value,
    pageSizes: [20, 50, 100, { label: '全部', value: totalItems > 0 ? totalItems : 1 }],
    showSizePicker: true,
    onUpdatePage: (page) => { currentPage.value = page; },
    onUpdatePageSize: (pageSize) => {
      currentPageSize.value = pageSize;
      currentPage.value = 1; 
    }
  };
});

const parseBestIds = (val) => {
  if (!val) return [];
  let ids = [];
  if (Array.isArray(val)) {
    ids = val;
  } else if (typeof val === 'string') {
    try {
      const trimmed = val.trim();
      if (trimmed.startsWith('[')) {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) ids = parsed;
      } else {
        ids = [trimmed];
      }
    } catch (e) {
      ids = [val];
    }
  } else {
    ids = [val];
  }
  return ids.map(id => String(id));
};

// ★★★ 手动删除单一版本的处理函数 ★★★
const handleDeleteVersion = (row) => {
  dialog.warning({
    title: '手动删除确认',
    content: `确定要永久删除该版本 (ID: ${row.id}) 吗？此操作将无视规则PK结果，直接删除对应的媒体文件！`,
    positiveText: '确定删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.post('/api/cleanup/delete_version', { 
          emby_id: String(row.id)
        });
        message.success('版本删除成功');
        fetchData(); // 删除成功后刷新列表
      } catch (err) {
        message.error(err.response?.data?.error || '删除失败');
      }
    }
  });
};

// 定义版本详情表格的列结构
const createVersionColumns = (bestVersionJson) => {
  const bestIds = parseBestIds(bestVersionJson);

  return [
    {
      title: '状态',
      key: 'status',
      width: 60,
      align: 'center',
      render(row) {
        const currentIdStr = String(row.id);
        const isBest = bestIds.includes(currentIdStr);
        return h(NIcon, { 
          component: isBest ? KeepIcon : DeleteIcon, 
          color: isBest ? 'var(--n-success-color)' : 'var(--n-error-color)', 
          size: 20 
        });
      }
    },
    {
      title: 'ID',
      key: 'id',
      width: 90,
      // ★★★ 恢复为普通的 Tag 显示，移除跳转逻辑 ★★★
      render: (row) => h(NTag, { size: 'small', bordered: false, type: 'default' }, { default: () => row.id })
    },
    {
      title: '分辨率',
      key: 'resolution',
      width: 90,
      render: (row) => row.resolution ? h(NTag, { size: 'small', bordered: false }, { default: () => row.resolution }) : '-'
    },
    {
      title: '质量',
      key: 'quality',
      width: 100,
      render: (row) => row.quality ? h(NTag, { size: 'small', bordered: false, type: 'info' }, { default: () => row.quality.toUpperCase() }) : '-'
    },
    {
      title: '特效',
      key: 'effect',
      width: 100,
      render: (row) => {
        const effect = formatEffectTagForDisplay(row.effect);
        return effect !== 'SDR' 
          ? h(NTag, { size: 'small', bordered: false, type: 'warning' }, { default: () => effect }) 
          : h(NText, { depth: 3, style: 'font-size: 12px' }, { default: () => 'SDR' });
      }
    },
    {
      title: '编码',
      key: 'codec',
      width: 80,
      render: (row) => row.codec ? h(NTag, { size: 'small', bordered: false, color: { color: '#f5f5f5', textColor: '#666' } }, { default: () => row.codec }) : '-'
    },
    {
      title: '字幕',
      key: 'subtitle_count',
      width: 100,
      render: (row) => {
        const count = row.subtitle_count || 0;
        const langs = row.subtitle_languages || [];
        const hasChinese = langs.includes('chi') || langs.includes('yue');
        
        let content;
        if (count === 0) {
          content = h(NText, { depth: 3, style: 'font-size: 12px' }, { default: () => '无' });
        } else {
          content = h(
            NTag, 
            { size: 'small', bordered: false, type: hasChinese ? 'success' : 'default' }, 
            { default: () => hasChinese ? '中文' : `${count}种` }
          );
        }

        if (count > 0) {
          return h(NTooltip, { trigger: 'hover' }, {
            trigger: () => content,
            default: () => langs.join(', ') || '未知语言'
          });
        }
        return content;
      }
    },
    {
      title: '码率',
      key: 'video_bitrate_mbps',
      width: 100,
      render: (row) => row.video_bitrate_mbps ? h(NTag, { size: 'small', bordered: false, color: { color: '#fafafa', textColor: '#333' } }, { default: () => `${row.video_bitrate_mbps} Mbps` }) : '-'
    },
    {
      title: '色深',
      key: 'bit_depth',
      width: 80,
      render: (row) => row.bit_depth ? h(NTag, { size: 'small', bordered: false, color: { color: '#e6f7ff', textColor: '#1890ff' } }, { default: () => `${row.bit_depth}bit` }) : '-'
    },
    {
      title: '帧率',
      key: 'frame_rate',
      width: 80,
      render: (row) => row.frame_rate ? h(NTag, { size: 'small', bordered: false, color: { color: '#fff7e6', textColor: '#fa8c16' } }, { default: () => `${Math.round(row.frame_rate)}fps` }) : '-'
    },
    {
      title: '时长',
      key: 'runtime_minutes',
      width: 90,
      render: (row) => row.runtime_minutes ? h(NTag, { size: 'small', bordered: false, color: { color: '#f6ffed', textColor: '#52c41a' } }, { default: () => `${row.runtime_minutes}min` }) : '-'
    },
    {
      title: '大小',
      key: 'filesize',
      width: 100,
      render: (row) => h(NTag, { size: 'small', bordered: false, type: 'success' }, { default: () => formatBytes(row.filesize) })
    },
    {
      title: '路径',
      key: 'path',
      minWidth: 200,
      ellipsis: { tooltip: true },
      render: (row) => h(NText, { depth: 3, style: 'font-size: 12px; font-family: monospace;' }, { default: () => row.path })
    },
    // ★★★ 手动操作列 ★★★
    {
      title: '操作',
      key: 'actions',
      width: 60,
      align: 'center',
      render: (row) => {
        return h(NButton, {
          size: 'small',
          type: 'error',
          quaternary: true,
          onClick: () => handleDeleteVersion(row)
        }, { icon: () => h(NIcon, null, { default: () => h(DeleteIcon) }) });
      }
    }
  ];
};

const renderVersions = (row) => {
  const versions = row.versions_info_json || [];
  const bestIds = parseBestIds(row.best_version_json);

  const sortedVersions = [...versions].sort((a, b) => {
    const aIsBest = bestIds.includes(String(a.id));
    const bIsBest = bestIds.includes(String(b.id));
    
    if (aIsBest && !bIsBest) return -1;
    if (!aIsBest && bIsBest) return 1;
    return 0;
  });

  return h(NDataTable, {
    columns: createVersionColumns(row.best_version_json), 
    data: sortedVersions,
    size: 'small',
    bordered: false,
    bottomBordered: false,
    rowKey: r => r.id
  });
};

const episodeColumns = [
  {
    key: 'item_name',
    width: 100, 
    align: 'center',
    render(row) {
      let displayName = row.item_name;
      if (row.item_type === 'Episode' && row.season_number !== undefined && row.episode_number !== undefined) {
          const season = String(row.season_number).padStart(2, '0');
          const episode = String(row.episode_number).padStart(2, '0');
          return h(NTag, { bordered: false, type: 'default' }, { default: () => `S${season}E${episode}` });
      }
      return h('span', displayName);
    }
  },
  {
    key: 'versions_info_json',
    render: renderVersions
  }
];

// --- 5. 方法和事件处理 ---

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  selectedSeriesNames.value = [];
  try {
    const response = await axios.get('/api/cleanup/tasks');
    allTasks.value = response.data;
  } catch (err) {
    error.value = err.response?.data?.error || '获取重复项列表失败。';
  } finally {
    isLoading.value = false;
  }
};

const triggerScan = async () => {
  try {
    await axios.post('/api/tasks/run', { task_name: 'scan-cleanup-issues' });
    message.success('扫描任务已提交到后台，请稍后查看任务状态。');
  } catch (err) {
    message.error(err.response?.data?.error || '提交扫描任务失败。');
  }
};

const handleBatchAction = (key) => {
  const ids = selectedTaskIds.value;
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
    fetchData();
  } catch (err) {
    message.error(err.response?.data?.error || '提交清理任务失败。');
  }
};

const ignoreTasks = async (ids) => {
  try {
    const response = await axios.post('/api/cleanup/ignore', { task_ids: ids });
    message.success(response.data.message);
    fetchData();
  } catch (err) {
    message.error(err.response?.data?.error || '忽略任务失败。');
  }
};

const deleteTasks = async (ids) => {
  try {
    const response = await axios.post('/api/cleanup/delete', { task_ids: ids });
    message.success(response.data.message);
    fetchData();
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
        fetchData();
      } catch (err) {
        message.error(err.response?.data?.error || '一键清理任务失败。');
      }
    }
  });
};

// --- 6. 格式化工具函数 ---

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
  if (tag_lower === 'dovi') return 'DoVi';
  if (tag_lower === 'hdr10+') return 'HDR10+';
  return tag_lower.toUpperCase();
};

// --- 7. 生命周期钩子和监听器 ---

watch(isScanTaskActive, (isActive, wasActive) => {
  if (wasActive && !isActive) {
    message.success('扫描已完成，正在自动刷新列表...');
    fetchData();
  }
});

onMounted(() => {
  fetchData();
});

</script>

<style scoped>
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: calc(100vh - 300px);
}
</style>