<template>
  <div class="shared-page">
    <n-space vertical :size="18">
      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <div class="page-header">
            <div>
              <div class="page-title">共享资源管理</div>
              <n-text depth="3">管理虚拟入库资源、临时转存文件，以及查看共享中心贡献值。</n-text>
            </div>
            <n-space>
              <n-button :loading="refreshingCredit" @click="refreshCredit">
                <template #icon><n-icon :component="RefreshIcon" /></template>
                刷新贡献值
              </n-button>
              <n-button type="primary" ghost :loading="loading" @click="loadAll">
                <template #icon><n-icon :component="SyncIcon" /></template>
                刷新列表
              </n-button>
            </n-space>
          </div>
        </template>

        <n-grid :cols="isMobile ? 2 : 6" :x-gap="12" :y-gap="12">
          <n-gi v-for="card in statCards" :key="card.key">
            <div class="stat-card">
              <div class="stat-label">{{ card.label }}</div>
              <div class="stat-value">{{ card.value }}</div>
              <div class="stat-desc">{{ card.desc }}</div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>

      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <n-space align="center" justify="space-between">
            <span class="card-title">虚拟入库资源</span>
            <n-text depth="3">删除会移除本地 STRM/媒体信息，并可同步删除 115 临时转存文件；转正会把临时文件移动到正式媒体目录。</n-text>
          </n-space>
        </template>

        <n-space class="toolbar" :vertical="isMobile" :size="12">
          <n-input v-model:value="filters.keyword" placeholder="搜索标题 / 文件名 / TMDb ID / SHA1" clearable @keyup.enter="loadItems">
            <template #prefix><n-icon :component="SearchIcon" /></template>
          </n-input>
          <n-select v-model:value="filters.status" :options="statusOptions" style="width: 160px" />
          <n-select v-model:value="filters.item_type" :options="typeOptions" style="width: 140px" />
          <n-button type="primary" :loading="loading" @click="loadItems">查询</n-button>
        </n-space>

        <n-data-table
          remote
          :loading="loading"
          :columns="columns"
          :data="items"
          :pagination="pagination"
          :row-key="row => row.virtual_id"
          :scroll-x="1180"
          @update:page="handlePageChange"
          @update:page-size="handlePageSizeChange"
        />
      </n-card>

      <n-card :bordered="false" class="dashboard-card">
        <template #header><span class="card-title">贡献值流水</span></template>
        <n-data-table
          :loading="ledgerLoading"
          :columns="ledgerColumns"
          :data="ledgerItems"
          :pagination="false"
          :scroll-x="900"
        />
      </n-card>
    </n-space>
  </div>
</template>

<script setup>
import { computed, h, onMounted, onUnmounted, reactive, ref } from 'vue';
import axios from 'axios';
import {
  NButton, NCard, NDataTable, NGi, NGrid, NIcon, NInput, NSelect,
  NSpace, NTag, NText, useDialog, useMessage
} from 'naive-ui';
import {
  RefreshOutline as RefreshIcon,
  SearchOutline as SearchIcon,
  SyncOutline as SyncIcon,
  TrashOutline as TrashIcon,
  CloudUploadOutline as PromoteIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

const isMobile = ref(false);
const checkMobile = () => { isMobile.value = window.innerWidth <= 768; };

const loading = ref(false);
const ledgerLoading = ref(false);
const refreshingCredit = ref(false);
const items = ref([]);
const total = ref(0);
const summary = ref({ local: {}, credit: {} });
const ledgerItems = ref([]);

const filters = reactive({ keyword: '', status: 'all', item_type: 'all' });
const pagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });

const statusOptions = [
  { label: '全部状态', value: 'all' },
  { label: '虚拟待播', value: 'virtual_ready' },
  { label: '已临时转存', value: 'cached' },
  { label: '已看过', value: 'watched' },
  { label: '已转正', value: 'promoted' },
  { label: '已删除', value: 'deleted' },
  { label: '异常', value: 'error' },
];

const typeOptions = [
  { label: '全部类型', value: 'all' },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' },
  { label: '季', value: 'Season' },
  { label: '单集', value: 'Episode' },
];

const statusMap = {
  virtual_ready: { text: '虚拟待播', type: 'info' },
  transferring: { text: '转存中', type: 'warning' },
  cached: { text: '已临时转存', type: 'success' },
  watched: { text: '已看过', type: 'warning' },
  promoted: { text: '已转正', type: 'success' },
  deleted: { text: '已删除', type: 'default' },
  error: { text: '异常', type: 'error' },
};

const fmtBytes = (value) => {
  const n = Number(value || 0);
  if (!n) return '-';
  if (n >= 1024 ** 4) return `${(n / 1024 ** 4).toFixed(2)} TB`;
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${n} B`;
};

const fmtDate = (value) => {
  if (!value) return '-';
  try { return new Date(value).toLocaleString(); } catch { return String(value); }
};

const statCards = computed(() => {
  const local = summary.value.local || {};
  const credit = summary.value.credit || {};
  return [
    { key: 'credit', label: '贡献值', value: credit.credit ?? 0, desc: credit.device_id ? `设备 ${credit.device_id}` : '未同步' },
    { key: 'total', label: '虚拟资源', value: local.total ?? 0, desc: '本地虚拟入库总数' },
    { key: 'cached', label: '临时转存', value: local.cached ?? 0, desc: fmtBytes(local.cached_size) },
    { key: 'promoted', label: '已转正', value: local.promoted ?? 0, desc: '转为永久收藏' },
    { key: 'remote_sources', label: '中心共享源', value: credit.shared_sources ?? 0, desc: `${credit.raw_ffprobe ?? 0} 条媒体信息` },
    { key: 'remote_gaps', label: '中心缺口', value: credit.wanted_gaps ?? 0, desc: `${credit.remote_devices ?? 0} 个设备` },
  ];
});

const renderStatus = (row) => {
  const meta = statusMap[row.status] || { text: row.status || '未知', type: 'default' };
  return h(NTag, { type: meta.type, size: 'small', round: true }, { default: () => meta.text });
};

const columns = [
  { title: '标题', key: 'title', minWidth: 230, render: row => h('div', [
      h('div', { class: 'main-title' }, row.title || row.file_name || row.tmdb_id),
      h('div', { class: 'sub-title' }, `${row.item_type || '-'} · TMDb ${row.tmdb_id || '-'}${row.season_number ? ` · S${String(row.season_number).padStart(2, '0')}` : ''}${row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : ''}`)
    ])
  },
  { title: '状态', key: 'status', width: 120, render: renderStatus },
  { title: '文件', key: 'file_name', minWidth: 260, ellipsis: { tooltip: true } },
  { title: '大小', key: 'size', width: 110, render: row => fmtBytes(row.size) },
  { title: '播放', key: 'play_count', width: 90, render: row => `${row.play_count || 0} 次` },
  { title: '临时到期', key: 'expires_at', width: 170, render: row => fmtDate(row.expires_at) },
  { title: '更新时间', key: 'updated_at', width: 170, render: row => fmtDate(row.updated_at) },
  { title: '操作', key: 'actions', width: 190, fixed: 'right', render: row => h(NSpace, { size: 8 }, {
    default: () => [
      h(NButton, {
        size: 'small', type: 'primary', ghost: true,
        disabled: !row.real_fid || row.status === 'promoted' || row.status === 'deleted',
        onClick: () => confirmPromote(row)
      }, { icon: () => h(NIcon, null, { default: () => h(PromoteIcon) }), default: () => '转正' }),
      h(NButton, {
        size: 'small', type: 'error', ghost: true,
        disabled: row.status === 'deleted' || row.status === 'promoted',
        onClick: () => confirmDelete(row)
      }, { icon: () => h(NIcon, null, { default: () => h(TrashIcon) }), default: () => '删除' }),
    ]
  }) },
];

const ledgerColumns = [
  { title: '时间', key: 'created_at', width: 180, render: row => fmtDate(row.created_at) },
  { title: '事件', key: 'event_type', width: 160 },
  { title: '变化', key: 'delta', width: 90, render: row => h(NTag, { type: Number(row.delta) >= 0 ? 'success' : 'error', size: 'small' }, { default: () => String(row.delta || 0) }) },
  { title: '标题', key: 'title', minWidth: 220, ellipsis: { tooltip: true } },
  { title: '原因', key: 'reason', minWidth: 260, ellipsis: { tooltip: true } },
];

const loadSummary = async () => {
  const res = await axios.get('/api/shared/resources/summary');
  summary.value = res.data?.data || { local: {}, credit: {} };
};

const loadItems = async () => {
  loading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/virtual', {
      params: { ...filters, page: pagination.page, page_size: pagination.pageSize }
    });
    items.value = res.data?.items || [];
    total.value = Number(res.data?.total || 0);
    pagination.itemCount = total.value;
  } catch (e) {
    message.error(e.response?.data?.message || e.response?.data?.error || '加载虚拟资源失败');
  } finally {
    loading.value = false;
  }
};

const loadLedger = async () => {
  ledgerLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/credit/ledger', { params: { limit: 50 } });
    ledgerItems.value = res.data?.items || [];
  } catch (e) {
    message.error('加载贡献值流水失败');
  } finally {
    ledgerLoading.value = false;
  }
};

const loadAll = async () => {
  await Promise.allSettled([loadSummary(), loadItems(), loadLedger()]);
};

const refreshCredit = async () => {
  refreshingCredit.value = true;
  try {
    await axios.post('/api/shared/resources/credit/refresh');
    message.success('贡献值已同步');
    await loadSummary();
  } catch (e) {
    message.error(e.response?.data?.message || '刷新贡献值失败');
  } finally {
    refreshingCredit.value = false;
  }
};

const confirmDelete = (row) => {
  dialog.warning({
    title: '删除虚拟资源',
    content: `确定删除《${row.title || row.file_name}》吗？如果已经播放转存，会同步删除 115 临时文件。`,
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/delete`, { delete_remote: true, delete_local: true });
      message.success('已删除');
      await loadAll();
    }
  });
};

const confirmPromote = (row) => {
  dialog.info({
    title: '转为永久转存',
    content: `确定将《${row.title || row.file_name}》从临时目录移动到正式媒体目录吗？`,
    positiveText: '转正',
    negativeText: '取消',
    onPositiveClick: async () => {
      const res = await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/promote`, {});
      if (res.data?.success) {
        message.success('已转为永久转存');
        await loadAll();
      } else {
        message.error(res.data?.message || '转正失败');
      }
    }
  });
};

const handlePageChange = (page) => { pagination.page = page; loadItems(); };
const handlePageSizeChange = (size) => { pagination.pageSize = size; pagination.page = 1; loadItems(); };

onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  loadAll();
});

onUnmounted(() => window.removeEventListener('resize', checkMobile));
</script>

<style scoped>
.shared-page { padding: 4px; }
.page-header { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.page-title { font-size: 20px; font-weight: 700; margin-bottom: 4px; }
.card-title { font-weight: 700; }
.toolbar { margin-bottom: 16px; }
.stat-card { padding: 14px; border-radius: 14px; background: rgba(128, 128, 128, 0.08); min-height: 96px; box-sizing: border-box; }
.stat-label { font-size: 12px; color: #888; margin-bottom: 8px; }
.stat-value { font-size: 24px; font-weight: 700; line-height: 1.1; }
.stat-desc { margin-top: 8px; color: #999; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.main-title { font-weight: 600; }
.sub-title { font-size: 12px; color: #888; margin-top: 3px; }
@media (max-width: 768px) {
  .page-header { flex-direction: column; }
  .shared-page { padding: 0; }
}
</style>
