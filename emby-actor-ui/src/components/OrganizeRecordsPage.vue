<!-- src/components/OrganizeRecordsPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <!-- 顶部统计仪表盘 -->
    <n-grid :x-gap="16" :y-gap="16" cols="1 s:2 m:5" responsive="screen" style="margin-bottom: 24px;">
      <n-gi>
        <n-card class="stat-card" size="small">
          <n-statistic label="总处理记录">
            <template #prefix><n-icon :component="LayersIcon" color="#2080f0" /></template>
            {{ stats.total || 0 }}
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card class="stat-card" size="small">
          <n-statistic label="识别成功">
            <template #prefix><n-icon :component="CheckmarkCircleIcon" color="#18a058" /></template>
            {{ stats.success || 0 }}
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card class="stat-card" size="small">
          <n-statistic label="未识别 / 失败">
            <template #prefix><n-icon :component="HelpCircleIcon" color="#f0a020" /></template>
            {{ stats.unrecognized || 0 }}
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card class="stat-card" size="small">
          <n-statistic label="本周处理">
            <template #prefix><n-icon :component="TrendingUpIcon" color="#d03050" /></template>
            {{ stats.thisWeek || 0 }}
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card class="stat-card" size="small">
          <n-statistic label="命中中心缓存">
            <template #prefix><n-icon :component="CloudDoneIcon" color="#18a058" /></template>
            {{ stats.center_cached || 0 }}
          </n-statistic>
        </n-card>
      </n-gi>
    </n-grid>

    <n-card class="dashboard-card" :bordered="false" size="small">
      <!-- 搜索与过滤工具栏 -->
      <n-space style="margin-bottom: 20px;" align="center" justify="space-between">
        <n-space>
          <n-input
            v-model:value="searchQuery"
            placeholder="搜索原文件名、新文件名..."
            clearable
            @keyup.enter="handleFilter"
            @clear="handleFilter"
            style="width: 300px;"
          >
            <template #prefix><n-icon :component="SearchIcon" /></template>
          </n-input>
          <n-select
            v-model:value="statusFilter"
            :options="statusOptions"
            style="width: 140px;"
            @update:value="handleFilter"
          />
          <n-select
            v-model:value="categoryFilter"
            :options="categoryOptions"
            placeholder="所有分类"
            clearable
            style="width: 160px;"
            @update:value="handleFilter"
          />
        </n-space>
        
        <n-space>
          <n-button type="primary" :disabled="!realSelectedIds.length" @click="openBatchEditModal">
            <template #icon><n-icon :component="SparklesIcon" /></template>
            批量整理 ({{ realSelectedIds.length }})
          </n-button>
          <n-button type="error" :disabled="!realSelectedIds.length" @click="batchDelete">
            <template #icon><n-icon :component="TrashIcon" /></template>
            批量删除
          </n-button>
          <n-button type="primary" secondary @click="fetchRecords">
            <template #icon><n-icon :component="RefreshIcon" /></template>
            刷新
          </n-button>
        </n-space>
      </n-space>

      <!-- 数据表格 -->
      <n-data-table
        :columns="columns"
        :data="processedTableData"
        :loading="loading"
        :pagination="paginationProps"
        :bordered="false"
        v-model:checked-row-keys="checkedRowKeys"
        striped
        size="small"
        :row-key="row => row.id"
      />
    </n-card>

    <!-- 手动整理 / 纠错模态框 -->
    <n-modal v-model:show="showEditModal" preset="card" style="width: 500px;" title="手动整理 / 纠错" :bordered="false">
      <template #header-extra>
        <n-tag :type="editForm.status === 'success' ? 'info' : 'warning'" size="small">
          {{ editForm.ids.length > 1 ? '批量重组' : (editForm.status === 'success' ? '纠正信息' : '手动识别') }}
        </n-tag>
      </template>
      
      <n-alert v-if="editForm.status === 'success' || editForm.ids.length > 1" type="info" style="margin-bottom: 16px;">
        更改此项将触发 115 网盘和本地 STRM 的物理移动与重命名。
      </n-alert>
      
      <n-form ref="formRef" :model="editForm" label-placement="left" label-width="100">
        <n-form-item label="操作对象">
          <n-text depth="3" style="word-break: break-all;" :strong="editForm.ids.length > 1">
            {{ editForm.original_name }}
          </n-text>
        </n-form-item>
        
        <n-form-item label="TMDb ID" path="tmdb_id">
          <n-input v-model:value="editForm.tmdb_id" placeholder="输入数字 ID, 例如 12345" />
        </n-form-item>
        
        <n-form-item label="媒体类型" path="media_type">
          <n-radio-group v-model:value="editForm.media_type">
            <n-radio-button value="movie">电影 (Movie)</n-radio-button>
            <n-radio-button value="tv">剧集 (TV)</n-radio-button>
          </n-radio-group>
        </n-form-item>

        <!-- ★ 新增：季号输入框 (仅剧集模式显示) -->
        <n-form-item v-if="editForm.media_type === 'tv'" label="季号 (Season)" path="season_num">
          <n-input-number 
            v-model:value="editForm.season_num" 
            placeholder="留空自动提取 (如 1, 2, 3)" 
            :min="1" 
            clearable 
            style="width: 100%;" 
          />
        </n-form-item>
        
        <n-form-item label="目标分类" path="target_cid">
          <n-select v-model:value="editForm.target_cid" :options="categoryOptions.slice(1)" placeholder="选择 115 目标整理目录" />
        </n-form-item>
      </n-form>
      
      <template #footer>
        <n-space justify="end">
          <n-button @click="showEditModal = false">取消</n-button>
          <n-button type="primary" :loading="submitting" @click="submitCorrection">
            <template #icon><n-icon :component="SparklesIcon" /></template>
            开始重组
          </n-button>
        </n-space>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, computed, h } from 'vue';
import axios from 'axios';
import {
  NTag, NButton, NSpace, NText, NIcon, NTooltip, NEllipsis, NInputNumber, useMessage, useDialog
} from 'naive-ui';
import {
  LayersOutline as LayersIcon,
  CheckmarkCircleOutline as CheckmarkCircleIcon,
  HelpCircleOutline as HelpCircleIcon,
  TrendingUpOutline as TrendingUpIcon,
  SearchOutline as SearchIcon,
  RefreshOutline as RefreshIcon,
  SparklesOutline as SparklesIcon,
  ConstructOutline as EditIcon,
  TrashOutline as TrashIcon,
  FolderOpenOutline as FolderIcon,
  CloudDoneOutline as CloudDoneIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

// 状态变量
const loading = ref(false);
const submitting = ref(false);
const tableData = ref([]);
const checkedRowKeys = ref([]);
const totalItems = ref(0);
const currentPage = ref(1);
const itemsPerPage = ref(15);
const searchQuery = ref('');
const statusFilter = ref('all');
const categoryFilter = ref(null);
const stats = ref({ total: 0, success: 0, unrecognized: 0, thisWeek: 0 });

// 选项数据
const statusOptions = [
  { label: '全部状态', value: 'all' },
  { label: '识别成功', value: 'success' },
  { label: '未识别/异常', value: 'unrecognized' },
  { label: '命中中心缓存', value: 'center_cached' }
];
const categoryOptions = ref([{ label: '所有分类', value: null }]);

// 模态框数据
const showEditModal = ref(false);
const editForm = ref({
  ids: [],
  original_name: '',
  status: '',
  tmdb_id: '',
  media_type: 'movie',
  season_num: null, 
  target_cid: null
});

// 提取季号的正则工具
const getSeason = (name) => {
  if (!name) return '未知季';
  const match = name.match(/S(\d{1,2})/i) || name.match(/Season\s*(\d{1,2})/i) || name.match(/第(\d+)季/);
  return match ? `第 ${parseInt(match[1])} 季` : '未知季';
};

// 提取剧名的工具函数
const getSeriesName = (name) => {
  if (!name) return '未知剧集';
  
  // 优先从整理后的标准命名提取 (例如: 匹兹堡医护前线 (2025) - S02E09...)
  // 匹配 " - SxxExx" 前面的所有内容
  const matchStd = name.match(/^(.*?)\s*-\s*S\d{2}E\d{2}/i);
  if (matchStd) return matchStd[1].trim();
  
  // 兜底从原文件名提取 (遇到 S01, EP01, Season, 第x季 就截断)
  const matchOrig = name.match(/^(.*?)(?:S\d{1,2}|EP?\d{1,3}|Season|第)/i);
  return matchOrig ? matchOrig[1].replace(/[\.\-_]/g, ' ').trim() : '未知剧集';
};

// 将扁平数据按 TMDb ID 和 季号 智能折叠为树形结构
const processedTableData = computed(() => {
  const groups = {};
  const result = [];

  tableData.value.forEach(item => {
    if (item.media_type === 'tv' && item.tmdb_id && item.status === 'success') {
      const season = getSeason(item.renamed_name || item.original_name);
      const key = `tv_${item.tmdb_id}_${item.target_cid}_${season}`;
      if (!groups[key]) groups[key] = [];
      groups[key].push(item);
    } else {
      result.push(item);
    }
  });

  Object.keys(groups).forEach(key => {
    const children = groups[key];
    if (children.length > 1) {
      const first = children[0];
      const season = getSeason(first.renamed_name || first.original_name);
      const seriesName = getSeriesName(first.renamed_name || first.original_name);
      
      result.push({
        id: `group_${key}`,
        isGroup: true,
        original_name: `📺 ${seriesName} | ${season} | 共 ${children.length} 集`,
        renamed_name: `支持整季批量纠错 / 批量删除`,
        status: 'success',
        media_type: 'tv',
        target_cid: first.target_cid,
        category_name: first.category_name,
        processed_at: first.processed_at,
        children: children.sort((a, b) => a.original_name.localeCompare(b.original_name))
      });
    } else {
      result.push(children[0]);
    }
  });

  result.sort((a, b) => new Date(b.processed_at) - new Date(a.processed_at));
  return result;
});

const realSelectedIds = computed(() => {
  return checkedRowKeys.value.filter(key => !String(key).startsWith('group_'));
});

// 表格列定义
const columns = computed(() => [
  { type: 'selection', fixed: 'left' },
  {
    title: '状态',
    key: 'status',
    width: 100,
    align: 'center',
    render(row) {
      if (row.isGroup) {
        return h(NTag, { type: 'info', bordered: false, size: 'small', round: true }, {
          icon: () => h(NIcon, { component: FolderIcon }),
          default: () => '剧集包'
        });
      }
      const isSuccess = row.status === 'success';
      return h(NTag, {
        type: isSuccess ? 'success' : 'warning',
        bordered: false,
        size: 'small',
        round: true
      }, {
        icon: () => h(NIcon, { component: isSuccess ? CheckmarkCircleIcon : HelpCircleIcon }),
        default: () => isSuccess ? '已整理' : '未识别'
      });
    }
  },
  {
    title: '名称演变 (原文件 ➔ 整理后)',
    key: 'name_evolution',
    render(row) {
      return h('div', { 
        style: 'display: flex; flex-direction: column; gap: 8px; width: 100%; min-width: 300px;' 
      }, [
        h(NText, { 
          strong: row.isGroup, 
          depth: row.isGroup ? 1 : 3, 
          style: 'font-size: 13px; display: flex; align-items: center;' 
        }, { 
          default: () => [
            !row.isGroup ? h(NTag, { size: 'tiny', bordered: false, style: 'margin-right: 8px; flex-shrink: 0;' }, { default: () => '原' }) : null,
            h(NEllipsis, { tooltip: true, style: 'max-width: 100%;' }, { default: () => row.original_name })
          ]
        }),
        h(NText, { 
          strong: !row.isGroup, 
          type: row.status === 'success' ? 'primary' : 'default', 
          style: 'font-size: 13px; display: flex; align-items: center;' 
        }, { 
          default: () => [
            !row.isGroup ? h(NTag, { size: 'tiny', type: row.status === 'success' ? 'success' : 'warning', bordered: false, style: 'margin-right: 8px; flex-shrink: 0;' }, { default: () => '新' }) : null,
            h(NEllipsis, { tooltip: true, style: 'max-width: 100%;' }, { default: () => row.renamed_name || '等待分配 TMDb ID 手动整理...' })
          ]
        })
      ]);
    }
  },
  {
    title: '媒体信息',
    key: 'media_info',
    width: 200,
    render(row) {
      if (row.status !== 'success') return h(NText, { depth: 3 }, { default: () => '-' });
      
      const tags = [
        h(NTag, { size: 'small', type: 'info', bordered: false }, { default: () => row.media_type === 'tv' ? '剧集' : '电影' }),
        h(NTag, { size: 'small', bordered: false, style: 'cursor: pointer;', onClick: () => window.open(`https://www.themoviedb.org/${row.media_type}/${row.tmdb_id}`, '_blank') }, { default: () => `TMDb: ${row.tmdb_id}` })
      ];

      if (row.is_center_cached) {
        tags.push(
          h(NTooltip, null, {
            trigger: () => h(NTag, { size: 'small', type: 'success', bordered: false, round: true }, { 
              icon: () => h(NIcon, { component: CloudDoneIcon }),
              default: () => '中心缓存' 
            }),
            default: () => '该媒体的真实参数由 P115Center 中心服务器提供'
          })
        );
      }

      return h(NSpace, { size: 'small' }, () => tags);
    }
  },
  {
    title: '目标分类',
    key: 'category_name',
    width: 150,
    render(row) {
      return row.category_name 
        ? h(NTag, { type: 'primary', bordered: false, size: 'small' }, { default: () => row.category_name }) 
        : h(NText, { depth: 3 }, { default: () => '未指定' });
    }
  },
  {
    title: '处理时间',
    key: 'processed_at',
    width: 160,
    render(row) { return new Date(row.processed_at).toLocaleString('zh-CN', { hour12: false }); }
  },
  {
    title: '操作',
    key: 'actions',
    width: 120,
    align: 'center',
    fixed: 'right',
    render(row) {
      return h(NSpace, { justify: 'center' }, () => [
        h(NTooltip, null, {
          trigger: () => h(NButton, {
            size: 'small', type: 'primary', ghost: true, circle: true,
            onClick: () => openEditModal(row)
          }, { icon: () => h(NIcon, { component: EditIcon }) }),
          default: () => row.isGroup ? '整季批量纠错' : (row.status === 'success' ? '修改整理分类/纠错' : '手动分配ID整理')
        }),
        h(NTooltip, null, {
          trigger: () => h(NButton, {
            size: 'small', type: 'error', ghost: true, circle: true,
            onClick: () => deleteRecord(row)
          }, { icon: () => h(NIcon, { component: TrashIcon }) }),
          default: () => row.isGroup ? '整季批量删除记录' : '删除此记录 (仅删除记录不删文件)'
        })
      ]);
    }
  }
]);

const paginationProps = computed(() => ({
  pageSize: itemsPerPage.value,
  showSizePicker: true,
  pageSizes: [15, 30, 50, 100],
}));

const fetchRecords = async () => {
  loading.value = true;
  checkedRowKeys.value = [];
  try {
    const res = await axios.get('/api/p115/records', {
      params: {
        page: 1,
        per_page: 5000, 
        search: searchQuery.value,
        status: statusFilter.value,
        cid: categoryFilter.value
      }
    });
    tableData.value = res.data.items;
    stats.value = res.data.stats;
  } catch (error) {
    message.error('获取整理记录失败');
  } finally {
    loading.value = false;
  }
};

const fetchCategories = async () => {
  try {
    const res = await axios.get('/api/p115/sorting_rules');
    const rules = res.data.filter(r => r.enabled && r.cid && r.cid !== '0');
    categoryOptions.value = [
      { label: '所有分类', value: null },
      ...rules.map(r => ({ label: r.dir_name || r.name, value: r.cid }))
    ];
  } catch (error) {
    console.error('获取分类规则失败', error);
  }
};

const handleFilter = () => {
  currentPage.value = 1;
  fetchRecords();
};

const openEditModal = (row) => {
  let ids = [row.id];
  let name = row.original_name;
  let defaultSeason = null;
  
  // ★ 智能预填季号逻辑
  if (row.isGroup) {
    ids = row.children.map(c => c.id);
    name = `[整季批量操作] ${row.original_name}`;
    const match = row.original_name.match(/第 (\d+) 季/);
    if (match) defaultSeason = parseInt(match[1]);
  } else if (row.media_type === 'tv') {
    const match = row.renamed_name?.match(/S(\d{1,2})E/i) || row.original_name?.match(/S(\d{1,2})/i);
    if (match) defaultSeason = parseInt(match[1]);
  }

  editForm.value = {
    ids: ids,
    original_name: name,
    status: row.status,
    tmdb_id: row.tmdb_id || '',
    media_type: row.media_type || 'movie',
    season_num: defaultSeason, // 预填季号
    target_cid: row.target_cid || null
  };
  showEditModal.value = true;
};

const openBatchEditModal = () => {
  const ids = realSelectedIds.value;
  if (!ids.length) return;
  
  editForm.value = {
    ids: ids,
    original_name: `[全局批量操作] 已选中 ${ids.length} 个文件`,
    status: 'unrecognized',
    tmdb_id: '',
    media_type: 'tv',
    season_num: null,
    target_cid: null
  };
  showEditModal.value = true;
};

const submitCorrection = async () => {
  if (!editForm.value.tmdb_id || !editForm.value.target_cid) {
    message.warning('TMDb ID 和目标分类不能为空！');
    return;
  }
  submitting.value = true;
  try {
    const promises = editForm.value.ids.map(id => 
      axios.post('/api/p115/records/correct', {
        id: id,
        tmdb_id: editForm.value.tmdb_id,
        media_type: editForm.value.media_type,
        target_cid: editForm.value.target_cid,
        season_num: editForm.value.season_num // ★ 发送季号给后端
      })
    );
    await Promise.all(promises);
    
    message.success(`成功发送 ${promises.length} 个重组指令！`);
    showEditModal.value = false;
    checkedRowKeys.value = [];
    fetchRecords();
  } catch (error) {
    message.error('部分或全部操作失败，请检查后端日志');
  } finally {
    submitting.value = false;
  }
};

const deleteRecord = (row) => {
  let ids = [row.id];
  let text = `确定要删除记录 "${row.original_name}" 吗？`;
  
  if (row.isGroup) {
    ids = row.children.map(c => c.id);
    text = `确定要批量删除该季的 ${ids.length} 条记录吗？`;
  }

  dialog.warning({
    title: '删除记录',
    content: text + ' 这只会删除数据库记录，不会删除网盘文件。',
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const promises = ids.map(id => axios.delete(`/api/p115/records/${id}`));
        await Promise.all(promises);
        message.success(`成功删除 ${promises.length} 条记录`);
        fetchRecords();
      } catch (error) {
        message.error('删除失败');
      }
    }
  });
};

const batchDelete = () => {
  const ids = realSelectedIds.value;
  if (!ids.length) return;
  
  dialog.warning({
    title: '批量删除记录',
    content: `确定要删除选中的 ${ids.length} 条记录吗？这只会删除数据库记录，不会删除网盘文件。`,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const promises = ids.map(id => axios.delete(`/api/p115/records/${id}`));
        await Promise.all(promises);
        message.success(`成功删除 ${promises.length} 条记录`);
        checkedRowKeys.value = [];
        fetchRecords();
      } catch (error) {
        message.error('批量删除失败');
      }
    }
  });
};

onMounted(() => {
  fetchCategories();
  fetchRecords();
});
</script>

<style scoped>
.stat-card {
  transition: all 0.3s ease;
  border-radius: 8px;
}
.stat-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
}
</style>