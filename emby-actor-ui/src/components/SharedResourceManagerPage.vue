<template>
  <div class="shared-page">
    <n-space vertical :size="18">
      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <div class="page-header">
            <div>
              <div class="page-title">共享资源管理</div>
              <n-text depth="3">管理虚拟入库、我的共享，以及查看共享中心贡献值。</n-text>
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
        <n-tabs v-model:value="activeTab" animated type="line" @update:value="handleTabChange">
          <n-tab-pane name="virtual" tab="虚拟入库">
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="virtualFilters.keyword" placeholder="搜索标题 / 文件名 / TMDb ID / SHA1" clearable @keyup.enter="loadVirtualItems">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="virtualFilters.status" :options="virtualStatusOptions" style="width: 160px" />
              <n-select v-model:value="virtualFilters.item_type" :options="typeOptions" style="width: 140px" />
              <n-button type="primary" :loading="loading" @click="loadVirtualItems">查询</n-button>
            </n-space>
            <n-data-table
              remote
              :loading="loading"
              :columns="virtualColumns"
              :data="virtualItems"
              :pagination="virtualPagination"
              :row-key="row => row.virtual_id"
              :scroll-x="1180"
              @update:page="p => { virtualPagination.page = p; loadVirtualItems(); }"
              @update:page-size="s => { virtualPagination.pageSize = s; virtualPagination.page = 1; loadVirtualItems(); }"
            />
          </n-tab-pane>

          <n-tab-pane name="shares" tab="我的分享">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              测试阶段可手动创建分享。创建后先检查审核状态，显示“已通过”后再登记中心。剧集建议按季目录分享，不要按单集分享。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="shareFilters.keyword" placeholder="搜索标题 / 目录名 / 分享码 / TMDb ID" clearable @keyup.enter="loadShares">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="shareFilters.status" :options="shareStatusOptions" style="width: 170px" />
              <n-button type="primary" :loading="sharesLoading" @click="loadShares">查询</n-button>
              <n-button type="primary" @click="showManualShareModal = true">
                <template #icon><n-icon :component="ShareIcon" /></template>
                手动分享
              </n-button>
            </n-space>
            <n-data-table
              remote
              :loading="sharesLoading"
              :columns="shareColumns"
              :data="shareItems"
              :pagination="sharePagination"
              :row-key="row => row.id"
              :scroll-x="1350"
              @update:page="p => { sharePagination.page = p; loadShares(); }"
              @update:page-size="s => { sharePagination.pageSize = s; sharePagination.page = 1; loadShares(); }"
            />
          </n-tab-pane>

          <n-tab-pane name="ledger" tab="贡献值明细">
            <n-data-table
              :loading="ledgerLoading"
              :columns="ledgerColumns"
              :data="ledgerItems"
              :pagination="false"
              :scroll-x="900"
            />
          </n-tab-pane>
        </n-tabs>
      </n-card>
    </n-space>

    <n-modal v-model:show="showManualShareModal" preset="card" title="手动创建共享资源" style="width: 720px; max-width: 95vw;" class="modal-card-lite">
      <n-form :model="manualShareForm" label-placement="left" label-width="110">
        <n-form-item label="115目录/FID">
          <n-input v-model:value="manualShareForm.root_fid" placeholder="电影目录、季目录或单文件的 115 FID/CID" />
        </n-form-item>
        <n-form-item label="标题">
          <n-input v-model:value="manualShareForm.title" placeholder="例如：书卷一梦 / 某电影名" />
        </n-form-item>
        <n-form-item label="TMDb ID">
          <n-input v-model:value="manualShareForm.tmdb_id" placeholder="电影 TMDb ID 或剧集 TMDb ID" />
        </n-form-item>
        <n-form-item label="分享粒度">
          <n-select v-model:value="manualShareForm.share_type" :options="shareTypeOptions" />
        </n-form-item>
        <n-form-item label="媒体类型">
          <n-select v-model:value="manualShareForm.item_type" :options="manualItemTypeOptions" />
        </n-form-item>
        <n-form-item label="季号">
          <n-input-number v-model:value="manualShareForm.season_number" :min="1" clearable placeholder="按季分享时填写" />
        </n-form-item>
        <n-form-item label="年份">
          <n-input-number v-model:value="manualShareForm.release_year" :min="1900" :max="2100" clearable />
        </n-form-item>
        <n-form-item label="提取码">
          <n-input v-model:value="manualShareForm.receive_code" placeholder="留空则使用 115 自动生成；分享有效期固定永久" />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showManualShareModal = false">取消</n-button>
          <n-button type="primary" :loading="manualCreating" @click="manualCreateShare">创建永久分享</n-button>
        </n-space>
      </template>
    </n-modal>
  </div>
</template>

<script setup>
import { computed, h, onMounted, onUnmounted, reactive, ref } from 'vue';
import axios from 'axios';
import {
  NAlert, NButton, NCard, NDataTable, NForm, NFormItem, NGi, NGrid, NIcon, NInput,
  NInputNumber, NModal, NSelect, NSpace, NTabPane, NTabs, NTag, NText, useDialog, useMessage
} from 'naive-ui';
import {
  RefreshOutline as RefreshIcon,
  SearchOutline as SearchIcon,
  SyncOutline as SyncIcon,
  TrashOutline as TrashIcon,
  CloudUploadOutline as PromoteIcon,
  ShareSocialOutline as ShareIcon,
  CheckmarkCircleOutline as CheckIcon,
  CloudDoneOutline as ReportIcon,
  CloseCircleOutline as CancelIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

const isMobile = ref(false);
const checkMobile = () => { isMobile.value = window.innerWidth <= 768; };

const activeTab = ref('virtual');
const loading = ref(false);
const sharesLoading = ref(false);
const ledgerLoading = ref(false);
const refreshingCredit = ref(false);
const manualCreating = ref(false);
const showManualShareModal = ref(false);

const summary = ref({ local: {}, shares: {}, credit: {} });
const virtualItems = ref([]);
const shareItems = ref([]);
const ledgerItems = ref([]);

const virtualFilters = reactive({ keyword: '', status: 'all', item_type: 'all' });
const shareFilters = reactive({ keyword: '', status: 'all' });
const virtualPagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });
const sharePagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });

const manualShareForm = reactive({
  root_fid: '', title: '', tmdb_id: '', share_type: 'season_pack', item_type: 'Season',
  season_number: 1, release_year: null, receive_code: ''
});

const virtualStatusOptions = [
  { label: '全部状态', value: 'all' }, { label: '虚拟待播', value: 'virtual_ready' },
  { label: '已临时转存', value: 'cached' }, { label: '已看过', value: 'watched' },
  { label: '已转正', value: 'promoted' }, { label: '已删除', value: 'deleted' }, { label: '异常', value: 'error' },
];
const shareStatusOptions = [
  { label: '全部状态', value: 'all' }, { label: '审核中', value: 'pending_review' },
  { label: '已通过', value: 'alive' }, { label: '已登记中心', value: 'reported' },
  { label: '部分登记', value: 'partial' }, { label: '失败/异常', value: 'failed' },
  { label: '已取消', value: 'cancelled' },
];
const typeOptions = [
  { label: '全部类型', value: 'all' }, { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' }, { label: '季', value: 'Season' }, { label: '单集', value: 'Episode' },
];
const manualItemTypeOptions = [
  { label: '电影', value: 'Movie' }, { label: '季', value: 'Season' }, { label: '剧集', value: 'Series' },
];
const shareTypeOptions = [
  { label: '电影目录/单文件', value: 'movie_folder' },
  { label: '季目录', value: 'season_pack' },
  { label: '整剧目录', value: 'series_pack' },
];

const statusMap = {
  virtual_ready: { text: '虚拟待播', type: 'info' }, transferring: { text: '转存中', type: 'warning' },
  cached: { text: '已临时转存', type: 'success' }, watched: { text: '已看过', type: 'warning' },
  promoted: { text: '已转正', type: 'success' }, deleted: { text: '已删除', type: 'default' }, error: { text: '异常', type: 'error' },
  pending_review: { text: '审核中', type: 'warning' }, alive: { text: '已通过', type: 'success' },
  reported: { text: '已登记', type: 'success' }, partial: { text: '部分登记', type: 'warning' },
  failed: { text: '失败', type: 'error' }, rejected: { text: '未通过', type: 'error' }, cancelled: { text: '已取消', type: 'default' },
  not_reported: { text: '未登记', type: 'default' },
};

const fmtBytes = (value) => {
  const n = Number(value || 0);
  if (!n) return '-';
  if (n >= 1024 ** 4) return `${(n / 1024 ** 4).toFixed(2)} TB`;
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${n} B`;
};
const fmtDate = (value) => { if (!value) return '-'; try { return new Date(value).toLocaleString(); } catch { return String(value); } };
const tag = (value) => { const meta = statusMap[value] || { text: value || '未知', type: 'default' }; return h(NTag, { type: meta.type, size: 'small', round: true }, { default: () => meta.text }); };

const statCards = computed(() => {
  const local = summary.value.local || {};
  const shares = summary.value.shares || {};
  const credit = summary.value.credit || {};
  return [
    { key: 'credit', label: '贡献值', value: credit.credit ?? 0, desc: credit.device_id ? `设备 ${credit.device_id}` : '未同步' },
    { key: 'total', label: '虚拟资源', value: local.total ?? 0, desc: '本地虚拟入库总数' },
    { key: 'cached', label: '临时转存', value: local.cached ?? 0, desc: fmtBytes(local.cached_size) },
    { key: 'shares', label: '我的共享', value: shares.total ?? 0, desc: `${shares.alive ?? 0} 个已通过` },
    { key: 'remote_sources', label: '中心共享源', value: credit.shared_sources ?? 0, desc: `${credit.raw_ffprobe ?? 0} 条媒体信息` },
    { key: 'remote_gaps', label: '中心缺口', value: credit.wanted_gaps ?? 0, desc: `${credit.remote_devices ?? 0} 个设备` },
  ];
});

const virtualColumns = [
  { title: '标题', key: 'title', minWidth: 230, render: row => h('div', [h('div', { class: 'main-title' }, row.title || row.file_name || row.tmdb_id), h('div', { class: 'sub-title' }, `${row.item_type || '-'} · TMDb ${row.tmdb_id || '-'}${row.season_number ? ` · S${String(row.season_number).padStart(2, '0')}` : ''}${row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : ''}`)]) },
  { title: '状态', key: 'status', width: 120, render: row => tag(row.status) },
  { title: '文件', key: 'file_name', minWidth: 260, ellipsis: { tooltip: true } },
  { title: '大小', key: 'size', width: 110, render: row => fmtBytes(row.size) },
  { title: '播放', key: 'play_count', width: 90, render: row => `${row.play_count || 0} 次` },
  { title: '临时到期', key: 'expires_at', width: 170, render: row => fmtDate(row.expires_at) },
  { title: '更新时间', key: 'updated_at', width: 170, render: row => fmtDate(row.updated_at) },
  { title: '操作', key: 'actions', width: 190, fixed: 'right', render: row => h(NSpace, { size: 8 }, { default: () => [
    h(NButton, { size: 'small', type: 'primary', ghost: true, disabled: !row.real_fid || row.status === 'promoted' || row.status === 'deleted', onClick: () => confirmPromote(row) }, { icon: () => h(NIcon, null, { default: () => h(PromoteIcon) }), default: () => '转正' }),
    h(NButton, { size: 'small', type: 'error', ghost: true, disabled: row.status === 'deleted' || row.status === 'promoted', onClick: () => confirmDelete(row) }, { icon: () => h(NIcon, null, { default: () => h(TrashIcon) }), default: () => '删除' }),
  ]}) },
];

const shareColumns = [
  { title: '标题', key: 'title', minWidth: 240, render: row => h('div', [h('div', { class: 'main-title' }, row.title || row.root_name || row.share_code), h('div', { class: 'sub-title' }, `${row.share_type || '-'} · TMDb ${row.tmdb_id || '-'}${row.season_number ? ` · S${String(row.season_number).padStart(2, '0')}` : ''}`)]) },
  { title: '审核', key: 'review_status', width: 110, render: row => tag(row.review_status || row.status) },
  { title: '中心', key: 'center_status', width: 110, render: row => tag(row.center_status) },
  { title: '分享码', key: 'share_code', width: 140, ellipsis: { tooltip: true } },
  { title: '提取码', key: 'receive_code', width: 90 },
  { title: '文件数', key: 'item_count', width: 90, render: row => `${row.reported_count || 0}/${row.item_count || 0}` },
  { title: '创建时间', key: 'created_at', width: 170, render: row => fmtDate(row.created_at) },
  { title: '检查时间', key: 'last_checked_at', width: 170, render: row => fmtDate(row.last_checked_at) },
  { title: '错误', key: 'last_error', minWidth: 220, ellipsis: { tooltip: true } },
  { title: '操作', key: 'actions', width: 280, fixed: 'right', render: row => h(NSpace, { size: 8 }, { default: () => [
    h(NButton, { size: 'small', type: 'info', ghost: true, onClick: () => checkShare(row) }, { icon: () => h(NIcon, null, { default: () => h(CheckIcon) }), default: () => '检查' }),
    h(NButton, { size: 'small', type: 'primary', ghost: true, disabled: !['alive','reported'].includes(row.status) && row.review_status !== 'alive', onClick: () => reportShare(row) }, { icon: () => h(NIcon, null, { default: () => h(ReportIcon) }), default: () => '登记中心' }),
    h(NButton, { size: 'small', type: 'error', ghost: true, disabled: row.status === 'cancelled', onClick: () => cancelShare(row) }, { icon: () => h(NIcon, null, { default: () => h(CancelIcon) }), default: () => '取消' }),
  ]}) },
];

const ledgerColumns = [
  { title: '时间', key: 'created_at', width: 180, render: row => fmtDate(row.created_at) },
  { title: '事件', key: 'event_type', width: 180 },
  { title: '变化', key: 'delta', width: 90, render: row => h(NTag, { type: Number(row.delta) >= 0 ? 'success' : 'error', size: 'small' }, { default: () => String(row.delta || 0) }) },
  { title: '标题', key: 'title', minWidth: 220, ellipsis: { tooltip: true } },
  { title: '原因', key: 'reason', minWidth: 280, ellipsis: { tooltip: true } },
];

const loadSummary = async () => { const res = await axios.get('/api/shared/resources/summary'); summary.value = res.data?.data || { local: {}, shares: {}, credit: {} }; };
const loadVirtualItems = async () => { loading.value = true; try { const res = await axios.get('/api/shared/resources/virtual', { params: { ...virtualFilters, page: virtualPagination.page, page_size: virtualPagination.pageSize } }); virtualItems.value = res.data?.items || []; virtualPagination.itemCount = Number(res.data?.total || 0); } catch (e) { message.error(e.response?.data?.message || '加载虚拟资源失败'); } finally { loading.value = false; } };
const loadShares = async () => { sharesLoading.value = true; try { const res = await axios.get('/api/shared/resources/shares', { params: { ...shareFilters, page: sharePagination.page, page_size: sharePagination.pageSize } }); shareItems.value = res.data?.items || []; sharePagination.itemCount = Number(res.data?.total || 0); } catch (e) { message.error(e.response?.data?.message || '加载我的分享失败'); } finally { sharesLoading.value = false; } };
const loadLedger = async () => { ledgerLoading.value = true; try { const res = await axios.get('/api/shared/resources/credit/ledger', { params: { limit: 100 } }); ledgerItems.value = res.data?.items || []; } catch { message.error('加载贡献值流水失败'); } finally { ledgerLoading.value = false; } };
const loadAll = async () => { await Promise.allSettled([loadSummary(), loadVirtualItems(), loadShares(), loadLedger()]); };
const handleTabChange = (name) => { if (name === 'virtual') loadVirtualItems(); if (name === 'shares') loadShares(); if (name === 'ledger') loadLedger(); };
const refreshCredit = async () => { refreshingCredit.value = true; try { await axios.post('/api/shared/resources/credit/refresh'); message.success('贡献值已同步'); await loadSummary(); } catch (e) { message.error(e.response?.data?.message || '刷新贡献值失败'); } finally { refreshingCredit.value = false; } };

const manualCreateShare = async () => {
  if (!manualShareForm.root_fid) return message.warning('请填写 115 FID/CID');
  manualCreating.value = true;
  try {
    const payload = { ...manualShareForm };
    await axios.post('/api/shared/resources/shares/manual-create', payload);
    message.success('分享已创建，等待审核');
    showManualShareModal.value = false;
    activeTab.value = 'shares';
    await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || '创建分享失败');
  } finally { manualCreating.value = false; }
};

const checkShare = async (row) => { try { const res = await axios.post(`/api/shared/resources/shares/${row.id}/check`); message.success(res.data?.message || '检查完成'); await Promise.allSettled([loadShares(), loadSummary()]); } catch (e) { message.error(e.response?.data?.message || '检查失败'); } };
const reportShare = async (row) => { try { const res = await axios.post(`/api/shared/resources/shares/${row.id}/report-center`); message.success(res.data?.message || '已登记中心'); await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]); } catch (e) { message.error(e.response?.data?.message || '登记中心失败'); } };
const cancelShare = (row) => { dialog.warning({ title: '取消分享', content: `确定取消《${row.title || row.root_name}》的 115 分享吗？`, positiveText: '取消分享', negativeText: '保留', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/shares/${row.id}/cancel`); message.success('已取消分享'); await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]); } catch (e) { message.error(e.response?.data?.message || '取消失败'); } } }); };
const confirmDelete = (row) => { dialog.warning({ title: '删除虚拟资源', content: `确定删除《${row.title || row.file_name}》吗？如果已经播放转存，会同步删除 115 临时文件。`, positiveText: '删除', negativeText: '取消', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/delete`, { delete_remote: true, delete_local: true }); message.success('已删除'); await loadAll(); } catch (e) { message.error(e.response?.data?.message || '删除失败'); } } }); };
const confirmPromote = (row) => { dialog.info({ title: '转为永久转存', content: `确定将《${row.title || row.file_name}》从临时转存目录移动到正式媒体库吗？`, positiveText: '转正', negativeText: '取消', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/promote`); message.success('已转正'); await loadAll(); } catch (e) { message.error(e.response?.data?.message || '转正失败'); } } }); };

onMounted(() => { checkMobile(); window.addEventListener('resize', checkMobile); loadAll(); });
onUnmounted(() => window.removeEventListener('resize', checkMobile));
</script>

<style scoped>
.shared-page { padding: 0; }
.dashboard-card { border-radius: 14px; overflow: hidden; }
.page-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.page-title { font-size: 20px; font-weight: 700; margin-bottom: 6px; }
.card-title { font-size: 16px; font-weight: 700; }
.stat-card { background: rgba(128,128,128,0.08); border-radius: 12px; padding: 14px 16px; min-height: 82px; }
.stat-label { font-size: 12px; opacity: .65; margin-bottom: 8px; }
.stat-value { font-size: 24px; font-weight: 700; line-height: 1; }
.stat-desc { margin-top: 8px; font-size: 12px; opacity: .65; }
.toolbar { margin-bottom: 14px; }
.main-title { font-weight: 600; }
.sub-title { font-size: 12px; opacity: .6; margin-top: 3px; }
@media (max-width: 768px) { .page-header { flex-direction: column; } }
</style>
