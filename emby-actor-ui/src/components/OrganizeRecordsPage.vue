<template>
  <n-layout content-style="padding: 24px;">
    <!-- 顶部统计仪表盘 -->
    <n-grid :x-gap="16" :y-gap="16" cols="1 s:2 m:4" responsive="screen" style="margin-bottom: 24px;">
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
        
        <n-button type="primary" secondary @click="fetchRecords">
          <template #icon><n-icon :component="RefreshIcon" /></template>
          刷新
        </n-button>
      </n-space>

      <!-- 数据表格 -->
      <n-data-table
        :columns="columns"
        :data="tableData"
        :loading="loading"
        :pagination="paginationProps"
        :bordered="false"
        striped
        size="small"
        :row-key="row => row.id"
        remote
      />
    </n-card>

    <!-- 手动整理 / 纠错模态框 -->
    <n-modal v-model:show="showEditModal" preset="card" style="width: 500px;" title="手动整理 / 纠错" :bordered="false">
      <template #header-extra>
        <n-tag :type="editForm.status === 'success' ? 'info' : 'warning'" size="small">
          {{ editForm.status === 'success' ? '纠正信息' : '手动识别' }}
        </n-tag>
      </template>
      
      <n-alert v-if="editForm.status === 'success'" type="info" style="margin-bottom: 16px;">
        更改此项将触发 115 网盘和本地 STRM 的物理移动与重命名。
      </n-alert>
      
      <n-form ref="formRef" :model="editForm" label-placement="left" label-width="100">
        <n-form-item label="原文件名">
          <n-text depth="3" style="word-break: break-all;">{{ editForm.original_name }}</n-text>
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
  NTag, NButton, NSpace, NText, NIcon, NTooltip, NEllipsis, useMessage, useDialog
} from 'naive-ui';
import {
  LayersOutline as LayersIcon,
  CheckmarkCircleOutline as CheckmarkCircleIcon,
  HelpCircleOutline as HelpCircleIcon,
  TrendingUpOutline as TrendingUpIcon,
  SearchOutline as SearchIcon,
  RefreshOutline as RefreshIcon,
  SparklesOutline as SparklesIcon,
  ArrowForwardOutline as ArrowIcon,
  ConstructOutline as EditIcon,
  TrashOutline as TrashIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

// 状态变量
const loading = ref(false);
const submitting = ref(false);
const tableData = ref([]);
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
  { label: '未识别/异常', value: 'unrecognized' }
];
const categoryOptions = ref([{ label: '所有分类', value: null }]);

// 模态框数据
const showEditModal = ref(false);
const editForm = ref({
  id: null,
  original_name: '',
  status: '',
  tmdb_id: '',
  media_type: 'movie',
  target_cid: null
});

// 表格列定义
const columns = computed(() => [
  {
    title: '状态',
    key: 'status',
    width: 100,
    align: 'center',
    render(row) {
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
        // ★ 核心修复：移除 max-width，使用 min-width 并允许自由伸展
        style: 'display: flex; flex-direction: column; gap: 8px; width: 100%; min-width: 300px;' 
      }, [
        // 第一行：原文件 (带 tooltip 悬浮显示完整文本)
        h(NText, { depth: 3, style: 'font-size: 13px; display: flex; align-items: center;' }, { 
          default: () => [
            h(NTag, { size: 'tiny', bordered: false, style: 'margin-right: 8px; flex-shrink: 0;' }, { default: () => '原' }),
            h(NEllipsis, { tooltip: true, style: 'max-width: 100%;' }, { default: () => row.original_name })
          ]
        }),
        // 第二行：新文件
        h(NText, { strong: true, type: row.status === 'success' ? 'primary' : 'default', style: 'font-size: 13px; display: flex; align-items: center;' }, { 
          default: () => [
            h(NTag, { size: 'tiny', type: row.status === 'success' ? 'success' : 'warning', bordered: false, style: 'margin-right: 8px; flex-shrink: 0;' }, { default: () => '新' }),
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
      return h(NSpace, { size: 'small' }, () => [
        h(NTag, { size: 'small', type: 'info', bordered: false }, { default: () => row.media_type === 'tv' ? '剧集' : '电影' }),
        h(NTag, { size: 'small', bordered: false, style: 'cursor: pointer;', onClick: () => window.open(`https://www.themoviedb.org/${row.media_type}/${row.tmdb_id}`, '_blank') }, { default: () => `TMDb: ${row.tmdb_id}` })
      ]);
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
          default: () => row.status === 'success' ? '修改整理分类/纠错' : '手动分配ID整理'
        }),
        h(NTooltip, null, {
          trigger: () => h(NButton, {
            size: 'small', type: 'error', ghost: true, circle: true,
            onClick: () => deleteRecord(row)
          }, { icon: () => h(NIcon, { component: TrashIcon }) }),
          default: () => '删除此记录 (仅删除记录不删文件)'
        })
      ]);
    }
  }
]);

// 分页配置
const paginationProps = computed(() => ({
  page: currentPage.value,
  pageSize: itemsPerPage.value,
  itemCount: totalItems.value,
  showSizePicker: true,
  pageSizes: [15, 30, 50, 100],
  onChange: (page) => { currentPage.value = page; fetchRecords(); },
  onUpdatePageSize: (pageSize) => { itemsPerPage.value = pageSize; currentPage.value = 1; fetchRecords(); }
}));

// API 获取数据
const fetchRecords = async () => {
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/records', {
      params: {
        page: currentPage.value,
        per_page: itemsPerPage.value,
        search: searchQuery.value,
        status: statusFilter.value,
        cid: categoryFilter.value
      }
    });
    tableData.value = res.data.items;
    totalItems.value = res.data.total;
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
  editForm.value = {
    id: row.id,
    original_name: row.original_name,
    status: row.status,
    tmdb_id: row.tmdb_id || '',
    media_type: row.media_type || 'movie',
    target_cid: row.target_cid || null
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
    const res = await axios.post('/api/p115/records/correct', editForm.value);
    if (res.data.success) {
      message.success(res.data.message || '重组指令已发送！');
      showEditModal.value = false;
      fetchRecords(); // 刷新列表
    }
  } catch (error) {
    message.error(error.response?.data?.message || '操作失败');
  } finally {
    submitting.value = false;
  }
};

const deleteRecord = (row) => {
  dialog.warning({
    title: '删除记录',
    content: `确定要删除记录 "${row.original_name}" 吗？这只会删除数据库记录，不会删除网盘文件。`,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.delete(`/api/p115/records/${row.id}`);
        message.success('记录已删除');
        fetchRecords();
      } catch (error) {
        message.error('删除失败');
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