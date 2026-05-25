<template>
  <div class="shared-page">
    <n-space vertical :size="18">
      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <div class="page-header">
            <div>
              <div class="page-title">共享资源管理</div>
              <n-text depth="3">集中管理共享资源：虚拟入库、本机分享、中心资源转存/入库和贡献值流水。</n-text>
            </div>
            <n-space>
              <n-button :type="hasCenterDevice ? 'default' : 'warning'" ghost :loading="registeringDevice" @click="registerCenterDevice">
                <template #icon><n-icon :component="SyncIcon" /></template>
                {{ hasCenterDevice ? '重置设备' : '注册设备' }}
              </n-button>
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

        <n-alert v-if="!hasCenterDevice" class="center-register-alert" type="warning" :bordered="false" style="margin-bottom: 12px;">
          共享资源中心尚未注册设备。点击右上角“注册设备”后，系统会向中心申请 device_token，并自动写入 p115_shared_device_token；之后才能同步贡献值、登记分享、转存或入库中心资源。
        </n-alert>

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
            <n-alert type="warning" :bordered="false" style="margin-bottom: 12px;">
              虚拟入库仅生成 STRM，播放时临时转存到临时缓存目录。值得收藏的资源请及时“转正”，以免临时缓存过期被删除后，无法再次转存。
            </n-alert>
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
              管理本机分享出去的资源。
“检查”用于同步 115 分享是否可用；“登记”用于把已通过的分享登记到共享中心，登记成功后可获得贡献值；“取消”会撤销 115 分享并同步清理中心记录。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="shareFilters.keyword" placeholder="搜索标题 / 目录名 / 分享码 / TMDb ID" clearable @keyup.enter="loadShares">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="shareFilters.status" :options="shareStatusOptions" style="width: 170px" />
              <n-button type="primary" :loading="sharesLoading" @click="loadShares">查询</n-button>
              <n-button type="primary" @click="openManualShareModal">
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


          <n-tab-pane name="center" tab="中心资源库">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              这里展示共享中心已收录的资源版本。
“转存”会把资源转存到你的 115 网盘；“入库”则仅生成虚拟 STRM，不占用网盘空间，播放时再临时转存。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="centerFilters.keyword" placeholder="搜索标题 / 文件名 / TMDb ID / SHA1" clearable @keyup.enter="loadCenterSources">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="centerFilters.item_type" :options="centerTypeOptions" style="width: 140px" />
              <n-select v-model:value="centerFilters.status" :options="centerStatusOptions" style="width: 150px" />
              <n-button type="primary" :loading="centerLoading" @click="loadCenterSources">查询中心</n-button>
              <n-button secondary :loading="maintenanceSubmitting" @click="triggerSharedMaintenance">执行维护任务</n-button>
            </n-space>
            <n-data-table
              remote
              :loading="centerLoading"
              :columns="centerColumns"
              :data="groupedCenterSources"
              :pagination="centerPagination"
              :row-key="row => row.group_key || row.source_id"
              :scroll-x="1480"
              @update:page="p => { centerPagination.page = p; loadCenterSources(); }"
              @update:page-size="s => { centerPagination.pageSize = s; centerPagination.page = 1; loadCenterSources(); }"
            />
          </n-tab-pane>

          <n-tab-pane name="ledger" tab="贡献值明细">
            <n-data-table
              :loading="ledgerLoading"
              :columns="ledgerColumns"
              :data="ledgerDisplayItems"
              :row-key="row => row.__row_key || row.id"
              :pagination="false"
              :scroll-x="900"
            />
          </n-tab-pane>
        </n-tabs>
      </n-card>
    </n-space>

    <n-modal v-model:show="showManualShareModal" preset="card" title="手动创建共享资源" style="width: 920px; max-width: 96vw;" class="modal-card-lite">
      <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
        直接输入片名搜索本地 media_metadata，系统会用已记录的 PC/SHA1 反查 p115_filesystem_cache，自动定位可分享的 115 目录或文件。剧集会优先按季目录分享，不创建单集分享。
      </n-alert>

      <n-space class="toolbar" :vertical="isMobile" :size="12">
        <n-input v-model:value="mediaSearchKeyword" placeholder="输入片名 / TMDb ID 搜索本地媒体库" clearable @keyup.enter="searchShareableMedia">
          <template #prefix><n-icon :component="SearchIcon" /></template>
        </n-input>
        <n-button type="primary" :loading="mediaSearchLoading" @click="searchShareableMedia">搜索</n-button>
      </n-space>

      <n-data-table
        size="small"
        :loading="mediaSearchLoading"
        :columns="mediaSearchColumns"
        :data="mediaCandidates"
        :pagination="{ pageSize: 8 }"
        :row-key="row => `${row.tmdb_id}-${row.item_type}-${row.season_number || ''}-${row.episode_number || ''}`"
        :scroll-x="980"
        style="margin-bottom: 14px;"
      />

      <div v-if="selectedMedia" class="selected-share-box">
        <div class="selected-title">已选择：{{ selectedMedia.display_title || selectedMedia.title }}</div>
        <div class="selected-desc">
          TMDb {{ manualShareForm.tmdb_id || '-' }} · {{ manualShareForm.item_type }} · {{ manualShareForm.share_type }} ·
          115 {{ manualShareForm.root_is_dir ? '目录' : '文件' }}：{{ manualShareForm.root_name || manualShareForm.root_fid }}
        </div>
        <div class="selected-desc" v-if="selectedMedia.message">{{ selectedMedia.message }}</div>
      </div>

      <n-form :model="manualShareForm" label-placement="left" label-width="90" style="margin-top: 12px;">
        <n-form-item label="提取码">
          <n-input v-model:value="manualShareForm.receive_code" placeholder="留空则使用 115 自动生成；分享有效期固定永久" />
        </n-form-item>
      </n-form>

      <template #footer>
        <n-space justify="space-between" align="center">
          <n-text depth="3">找不到候选时，先确认该媒体已入库且 media_metadata 中已有 PC/SHA1。</n-text>
          <n-space>
            <n-button @click="showManualShareModal = false">取消</n-button>
            <n-button type="primary" :disabled="!manualShareForm.root_fid" :loading="manualCreating" @click="manualCreateShare">创建永久分享</n-button>
          </n-space>
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
  CloseCircleOutline as CancelIcon,
  ChevronForwardOutline as ChevronForwardIcon,
  ChevronDownOutline as ChevronDownIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

const isMobile = ref(false);
const checkMobile = () => { isMobile.value = window.innerWidth <= 768; };

const activeTab = ref('virtual');
const loading = ref(false);
const sharesLoading = ref(false);
const ledgerLoading = ref(false);
const centerLoading = ref(false);
const maintenanceSubmitting = ref(false);
const refreshingCredit = ref(false);
const registeringDevice = ref(false);
const manualCreating = ref(false);
const showManualShareModal = ref(false);
const mediaSearchKeyword = ref('');
const mediaSearchLoading = ref(false);
const mediaCandidates = ref([]);
const selectedMedia = ref(null);

const summary = ref({ local: {}, shares: {}, credit: {} });
const virtualItems = ref([]);
const shareItems = ref([]);
const ledgerItems = ref([]);
const centerSources = ref([]);
const groupedCenterSources = computed(() => groupCenterSources(centerSources.value || []));
const ledgerCollapsedGroups = reactive({});

const virtualFilters = reactive({ keyword: '', status: 'all', item_type: 'all' });
const shareFilters = reactive({ keyword: '', status: 'all' });
const centerFilters = reactive({ keyword: '', status: 'alive,pending', item_type: 'all' });
const virtualPagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });
const sharePagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });
const centerPagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });

const manualShareForm = reactive({
  root_fid: '', root_name: '', root_is_dir: true, title: '', tmdb_id: '', parent_series_tmdb_id: '',
  share_type: 'season_pack', item_type: 'Season', season_number: 1, release_year: null, receive_code: ''
});

const virtualStatusOptions = [
  { label: '全部状态', value: 'all' }, { label: '虚拟待播', value: 'virtual_ready' },
  { label: '已临时转存', value: 'cached' }, { label: '已看过', value: 'watched' },
  { label: '已转正', value: 'promoted' }, { label: '已删除', value: 'deleted' }, { label: '异常', value: 'error' },
];
const shareStatusOptions = [
  { label: '全部状态', value: 'all' }, { label: '审核中', value: 'pending_review' },
  { label: '已通过', value: 'alive' }, { label: '已登记', value: 'reported' },
  { label: '部分登记', value: 'partial' }, { label: '失败/异常', value: 'failed' },
  { label: '已取消', value: 'cancelled' },
];

const centerStatusOptions = [
  { label: '可用/待验证', value: 'alive,pending' },
  { label: '仅可用', value: 'alive' },
  { label: '仅待验证', value: 'pending' },
  { label: '全部', value: '' },
];
const typeOptions = [
  { label: '全部类型', value: 'all' }, { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' }, { label: '季', value: 'Season' }, { label: '单集', value: 'Episode' },
];
const centerTypeOptions = [
  { label: '全部类型', value: 'all' },
  { label: '电影', value: 'Movie' },
  { label: '剧集包', value: 'Pack' },
  { label: '单集', value: 'Episode' },
];
const manualItemTypeOptions = [
  { label: '电影', value: 'Movie' }, { label: '季', value: 'Season' }, { label: '剧集', value: 'Series' },
];
const shareTypeOptions = [
  { label: '电影', value: 'movie_folder' },
  { label: '电影', value: 'movie_file' },
  { label: '剧集包', value: 'season_pack' },
  { label: '剧集包', value: 'series_pack' },
  { label: '单集', value: 'episode_file' },
];
const resourceTypeLabel = (value) => ({
  movie_file: '电影', movie_folder: '电影', Movie: '电影', movie: '电影', movies: '电影',
  season_pack: '剧集包', series_pack: '剧集包', Season: '剧集包', Series: '剧集包', season: '剧集包', series: '剧集包', Pack: '剧集包', pack: '剧集包',
  episode_file: '单集', Episode: '单集', episode: '单集', episodes: '单集',
}[value] || value || '-');
const shareTypeLabel = (value) => resourceTypeLabel(value) || shareTypeOptions.find(opt => opt.value === value)?.label || value || '-';
const isSuccessShareMessage = (value) => {
  const text = String(value || '').trim();
  if (!text) return true;
  return /^(分享可用|分享可访问|分享正常|可访问|正常|ok)$/i.test(text);
};
const shareErrorText = (row) => {
  const status = String(row.status || row.review_status || '').toLowerCase();
  const text = String(row.last_error || row.error || '').trim();
  if (!text || isSuccessShareMessage(text)) return '-';
  if (['alive', 'reported'].includes(status) && isSuccessShareMessage(text)) return '-';
  return text;
};

const statusMap = {
  virtual_ready: { text: '虚拟待播', type: 'info' }, transferring: { text: '转存中', type: 'warning' },
  cached: { text: '已临时转存', type: 'success' }, watched: { text: '已看过', type: 'warning' },
  promoted: { text: '已转正', type: 'success' }, deleted: { text: '已删除', type: 'default' }, error: { text: '异常', type: 'error' },
  pending_review: { text: '审核中', type: 'warning' }, alive: { text: '可用', type: 'success' },
  pending: { text: '待验证', type: 'warning' }, dead: { text: '失效', type: 'error' }, expired: { text: '已过期', type: 'default' },
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

const hasCenterDevice = computed(() => Boolean((summary.value.credit || {}).device_id));

const statCards = computed(() => {
  const local = summary.value.local || {};
  const shares = summary.value.shares || {};
  const credit = summary.value.credit || {};
  return [
    { key: 'credit', label: '贡献值', value: credit.credit ?? 0, desc: credit.device_id ? `设备 ${credit.device_id}` : '未同步' },
    { key: 'total', label: '虚拟资源', value: local.total ?? 0, desc: '本地虚拟入库总数' },
    { key: 'cached', label: '临时转存', value: local.cached ?? 0, desc: fmtBytes(local.cached_size) },
    { key: 'shares', label: '我的共享', value: shares.total ?? 0, desc: `${shares.alive ?? 0} 个已通过` },
    { key: 'remote_sources', label: '中心资源', value: credit.shared_sources ?? 0, desc: `${credit.raw_ffprobe ?? 0} 条媒体信息` },
    { key: 'remote_gaps', label: '待补资源', value: credit.wanted_gaps ?? 0, desc: `${credit.remote_devices ?? 0} 个设备` },
  ];
});

const virtualColumns = [
  { title: '标题', key: 'title', minWidth: 230, render: row => {
    const seasonText = row.season_number ? `S${String(row.season_number).padStart(2, '0')}` : '';
    const epText = row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : '';
    const packText = row.is_collapsed_pack ? ` · ${row.pack_item_count || 0}集包${row.pack_episode_numbers?.length ? ` · E${String(row.pack_episode_numbers[0]).padStart(2, '0')}-${String(row.pack_episode_numbers[row.pack_episode_numbers.length - 1]).padStart(2, '0')}` : ''}` : '';
    return h('div', [
      h('div', { class: 'main-title' }, row.title || row.file_name || row.tmdb_id),
      h('div', { class: 'sub-title' }, `${resourceTypeLabel(row.item_type)} · TMDb ${row.tmdb_id || '-'}${seasonText ? ` · ${seasonText}` : ''}${epText}${packText}`)
    ]);
  } },
  { title: '状态', key: 'status', width: 120, render: row => tag(row.status) },
  { title: '文件', key: 'file_name', minWidth: 260, ellipsis: { tooltip: true }, render: row => row.is_collapsed_pack ? `${row.pack_item_count || 0}集包` : (row.file_name || '-') },
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
  { title: '标题', key: 'title', minWidth: 240, render: row => {
    const seasonText = row.season_number ? ` · S${String(row.season_number).padStart(2, '0')}` : '';
    const episodeText = row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : '';
    return h('div', [
      h('div', { class: 'main-title' }, row.title || row.root_name || row.share_code),
      h('div', { class: 'sub-title' }, `${shareTypeLabel(row.share_type)} · TMDb ${row.tmdb_id || '-'}${seasonText}${episodeText}`)
    ]);
  } },
  { title: '审核', key: 'review_status', width: 110, render: row => tag(row.review_status || row.status) },
  { title: '中心', key: 'center_status', width: 110, render: row => tag(row.center_status) },
  { title: '分享码', key: 'share_code', width: 140, ellipsis: { tooltip: true } },
  { title: '提取码', key: 'receive_code', width: 90 },
  { title: '文件数', key: 'item_count', width: 90, render: row => `${row.reported_count || 0}/${row.item_count || 0}` },
  { title: '媒体信息', key: 'raw_uploaded_count', width: 110, render: row => {
    const missingSize = Number(row.size_missing_count || 0);
    const text = `${row.raw_uploaded_count || 0}/${row.item_count || 0}`;
    return h('div', [
      h('div', text),
      missingSize > 0 ? h('div', { class: 'sub-title warning-text' }, `缺大小 ${missingSize}`) : null
    ]);
  } },
  { title: '创建时间', key: 'created_at', width: 170, render: row => fmtDate(row.created_at) },
  { title: '检查时间', key: 'last_checked_at', width: 170, render: row => fmtDate(row.last_checked_at) },
  { title: '错误', key: 'last_error', minWidth: 220, ellipsis: { tooltip: true }, render: row => shareErrorText(row) },
  { title: '操作', key: 'actions', width: 300, fixed: 'right', render: row => h(NSpace, { size: 8 }, { default: () => [
    h(NButton, { size: 'small', type: 'info', ghost: true, onClick: () => checkShare(row) }, { icon: () => h(NIcon, null, { default: () => h(CheckIcon) }), default: () => '检查' }),
    h(NButton, { size: 'small', type: 'primary', ghost: true, disabled: !['alive','reported'].includes(row.status) && row.review_status !== 'alive', onClick: () => reportShare(row) }, { icon: () => h(NIcon, null, { default: () => h(ReportIcon) }), default: () => '登记' }),
    h(NButton, { size: 'small', type: 'error', ghost: true, disabled: row.status === 'cancelled', onClick: () => cancelShare(row) }, { icon: () => h(NIcon, null, { default: () => h(CancelIcon) }), default: () => '取消' }),
  ]}) },
];

const mediaSearchColumns = [
  { title: '媒体', key: 'display_title', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, row.display_title || row.title || row.tmdb_id),
    h('div', { class: 'sub-title' }, `${resourceTypeLabel(row.item_type)} · TMDb ${row.share_tmdb_id || row.tmdb_id || '-'}${row.release_year ? ` · ${row.release_year}` : ''}`)
  ]) },
  { title: '入库', key: 'in_library', width: 80, render: row => h(NTag, { size: 'small', type: row.in_library ? 'success' : 'default' }, { default: () => row.in_library ? '已入库' : '未入库' }) },
  { title: '可分享根目录/文件', key: 'root_name', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, row.root_name || '-'),
    h('div', { class: 'sub-title' }, row.root_fid ? `FID/CID: ${row.root_fid}` : (row.message || '未定位'))
  ]) },
  { title: '文件', key: 'file_count', width: 100, render: row => `${row.file_count || 0} 个` },
  { title: '分享粒度', key: 'share_type', width: 120, render: row => shareTypeLabel(row.share_type) },
  { title: '说明', key: 'message', minWidth: 220, ellipsis: { tooltip: true } },
  { title: '操作', key: 'actions', width: 100, fixed: 'right', render: row => h(NButton, {
    size: 'small', type: 'primary', ghost: true, disabled: !row.resolvable || !row.root_fid, onClick: () => chooseMediaCandidate(row)
  }, { default: () => row.resolvable ? '选择' : '不可用' }) },
];

const ledgerEventLabel = (eventType) => {
  const map = {
    center_initial_credit: '基础贡献值',
    center_source_registered: '中心登记共享源',
    center_source_registered_group: '登记汇总',
    center_shared_source_served: '共享被转存',
    center_shared_source_served_group: '分享加分汇总',
    center_shared_source_consumed: '转存共享资源',
    center_shared_source_consumed_group: '转存扣分汇总',
    share_created: '创建分享',
    share_reported_center: '登记',
    share_raw_uploaded: '媒体信息',
    share_cancelled: '取消分享',
    virtual_deleted: '删除虚拟资源',
    virtual_promoted: '虚拟资源转正',
  };
  return map[eventType] || eventType || '-';
};

const foldableLedgerEpisodeEvents = new Set([
  'center_source_registered',
  'center_shared_source_served',
  'center_shared_source_consumed',
]);

const ledgerGroupReasonPrefix = (eventType) => ({
  center_source_registered: '本季登记共享',
  center_shared_source_served: '本季分享加分',
  center_shared_source_consumed: '本季转存扣分',
}[eventType] || '本季贡献值');

const ledgerGroupEventType = (eventType) => `${eventType}_group`;

const formatDelta = (value) => {
  const n = Number(value || 0);
  return n > 0 ? `+${n}` : String(n);
};

const getLedgerCenterItem = (row) => {
  const raw = row?.raw_json || {};
  if (raw && typeof raw === 'object') return raw.center_item || raw;
  return {};
};

const stripEpisodeCode = (value) => String(value || '')
  .replace(/\s*S\d{1,3}\s*[._ -]*E\d{1,4}\b/ig, '')
  .replace(/\s*S\d{1,3}\b/ig, '')
  .replace(/\s+/g, ' ')
  .trim();

const parseLedgerEpisode = (row) => {
  const centerItem = getLedgerCenterItem(row);
  const title = String(row.title || centerItem.title || centerItem.file_name || '');
  const fileName = String(centerItem.file_name || row.file_name || '');
  let season = Number(centerItem.season_number || row.season_number || 0);
  let episode = Number(centerItem.episode_number || row.episode_number || 0);

  if (!season || !episode) {
    const m = title.match(/\bS(\d{1,3})\s*[._ -]*E(\d{1,4})\b/i)
      || fileName.match(/\bS(\d{1,3})\s*[._ -]*E(\d{1,4})\b/i);
    if (m) {
      season = season || Number(m[1]);
      episode = episode || Number(m[2]);
    }
  }
  if (!season || !episode) return null;

  const seriesTitle = stripEpisodeCode(centerItem.title || row.title || '') || '剧集';
  return {
    seriesTitle,
    season,
    episode,
    code: `S${String(season).padStart(2, '0')}E${String(episode).padStart(2, '0')}`
  };
};

const isFoldableLedgerEpisode = (row) => {
  if (!row || row.__group) return false;
  if (!foldableLedgerEpisodeEvents.has(row.event_type)) return false;
  return !!parseLedgerEpisode(row);
};

const ledgerDisplayItems = computed(() => {
  const result = [];
  const groups = new Map();

  for (const row of ledgerItems.value || []) {
    if (!isFoldableLedgerEpisode(row)) {
      result.push({ ...row, __row_key: `row:${row.id || row.ref_id || row.created_at || Math.random()}` });
      continue;
    }

    const ep = parseLedgerEpisode(row);
    const centerItem = getLedgerCenterItem(row);
    const tmdbKey = centerItem.parent_series_tmdb_id || centerItem.series_tmdb_id || row.parent_series_tmdb_id || centerItem.tmdb_id || row.tmdb_id || ep.seriesTitle;
    const seasonCode = `S${String(ep.season).padStart(2, '0')}`;
    const groupKey = `${row.event_type}:${tmdbKey}:${seasonCode}`;

    if (!groups.has(groupKey)) {
      const group = {
        __group: true,
        __row_key: `group:${groupKey}`,
        __group_key: groupKey,
        event_type: ledgerGroupEventType(row.event_type),
        source_event_type: row.event_type,
        title: `${ep.seriesTitle} ${seasonCode}`,
        delta: 0,
        reason: '',
        created_at: row.created_at,
        children: [],
        episodeCodes: [],
      };
      groups.set(groupKey, group);
      result.push(group);
    }

    const group = groups.get(groupKey);
    group.children.push({ ...row, __child: true, __row_key: `child:${groupKey}:${row.id || row.ref_id || ep.code}` });
    group.episodeCodes.push(ep.code);
    group.delta += Number(row.delta || 0);
    if (!group.created_at || new Date(row.created_at).getTime() > new Date(group.created_at).getTime()) {
      group.created_at = row.created_at;
    }
  }

  const expanded = [];
  for (const row of result) {
    if (!row.__group) {
      expanded.push(row);
      continue;
    }

    const sortedCodes = [...new Set(row.episodeCodes)].sort((a, b) => {
      const ma = String(a).match(/S(\d+)E(\d+)/i);
      const mb = String(b).match(/S(\d+)E(\d+)/i);
      const na = ma ? Number(ma[1]) * 10000 + Number(ma[2]) : 0;
      const nb = mb ? Number(mb[1]) * 10000 + Number(mb[2]) : 0;
      return na - nb;
    });
    const first = sortedCodes[0];
    const last = sortedCodes[sortedCodes.length - 1];
    const prefix = ledgerGroupReasonPrefix(row.source_event_type);
    row.reason = `${prefix} ${row.children.length} 条，贡献值 ${formatDelta(row.delta)}${first && last ? `，范围 ${first} - ${last}` : ''}`;
    expanded.push(row);

    if (ledgerCollapsedGroups[row.__group_key] === false) {
      expanded.push(...row.children);
    }
  }
  return expanded;
});

const toggleLedgerGroup = (key) => {
  ledgerCollapsedGroups[key] = ledgerCollapsedGroups[key] !== false ? false : true;
};


const centerTypeLabel = (value) => ({
  Movie: '电影', movie: '电影', movies: '电影', movie_file: '电影', movie_folder: '电影',
  Pack: '剧集包', pack: '剧集包', Season: '剧集包', season: '剧集包', Series: '剧集包', series: '剧集包', tv: '剧集包', season_pack: '剧集包', series_pack: '剧集包',
  Episode: '单集', episode: '单集', episodes: '单集', episode_file: '单集',
}[value] || value || '-');
const centerRowType = (row) => row.display_type || (row.is_collapsed_pack || row.pack_item_count ? 'Pack' : row.item_type);
const centerTitleText = (row) => {
  const title = row.title || row.media_title || '-';
  const year = row.release_year ? ` (${row.release_year})` : '';
  return `${title}${year}`;
};
const centerSeasonText = (row) => {
  const displayType = centerRowType(row);
  const s = row.season_number ? `S${String(row.season_number).padStart(2, '0')}` : '';
  const e = row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : '';
  const pack = row.pack_item_count ? `${row.pack_item_count}集包` : '';
  if (centerTypeLabel(displayType) === '电影') return '电影';
  if (centerTypeLabel(displayType) === '剧集包') return ['剧集包', s, pack].filter(Boolean).join(' · ');
  if (centerTypeLabel(displayType) === '单集') return ['单集', s && e ? `${s}${e}` : (s || e)].filter(Boolean).join(' · ');
  return [centerTypeLabel(displayType), s ? `${s}${e}` : '', pack].filter(Boolean).join(' · ') || '-';
};
const centerStatusTag = (row) => {
  const text = row.status_label || statusMap[row.status]?.text || row.status || '未知';
  const type = row.status_type || statusMap[row.status]?.type || 'default';
  return h(NTag, { type, size: 'small', round: true }, { default: () => text });
};
const versionSummaryText = (row) => {
  const v = row.version_summary || {};
  const parts = [v.resolution, v.effect, v.video_codec || v.codec, v.bit_depth ? `${v.bit_depth}bit` : '', v.fps].filter(Boolean);
  return parts.length ? parts.join(' · ') : (row.quality || '未知版本');
};
const formatCenterSize = (row) => {
  const gb = Number(row.version_summary?.size_gb || 0);
  if (gb > 0) return `${gb.toFixed(gb >= 10 ? 1 : 2)} GB`;
  const size = Number(row.size || 0);
  return size ? `${(size / 1024 / 1024 / 1024).toFixed(2)} GB` : '-';
};
const listCell = (items, limit = 3) => {
  const arr = (items || []).map(x => typeof x === 'string' ? x : (x.display || [x.language, x.codec, x.channels ? `${x.channels}ch` : '', x.title].filter(Boolean).join(' '))).filter(Boolean);
  if (!arr.length) return '-';
  const shown = arr.slice(0, limit);
  const more = arr.length > limit ? ` +${arr.length - limit}` : '';
  return h('div', { class: 'center-track-list', title: arr.join('\n') }, [
    ...shown.map((x, idx) => h('div', { class: 'center-track-line', key: idx }, x)),
    more ? h('div', { class: 'sub-title' }, more) : null
  ]);
};
const importCenterSource = (row, mode) => {
  const modeText = mode === 'virtual' ? '入库' : '转存';
  dialog.info({
    title: modeText,
    content: `确定将中心资源《${centerTitleText(row)}》执行${modeText}吗？`,
    positiveText: modeText,
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const sourceIds = Array.isArray(row.pack_source_ids) && row.pack_source_ids.length ? row.pack_source_ids : [row.source_id];
        const res = await axios.post('/api/shared/resources/center/import', {
          source_ids: sourceIds,
          mode,
          context: {
            title: row.title || '',
            tmdb_id: row.tmdb_id || '',
            item_type: centerRowType(row) || row.item_type || '',
            season_number: row.season_number ?? null,
            episode_number: row.episode_number ?? null,
            year: row.release_year || '',
            share_type: row.share_type || '',
          }
        });
        message.success(res.data?.message || '已提交');
        await Promise.allSettled([loadVirtualItems(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || `${modeText}失败`);
      }
    }
  });
};

const centerGroupKey = (row) => {
  const type = centerRowType(row);
  const tmdb = row.tmdb_id || row.share_tmdb_id || row.parent_series_tmdb_id || '';
  const title = row.title || row.media_title || '';
  const season = row.season_number || '';
  const episode = row.episode_number || '';
  const baseType = centerTypeLabel(type);
  if (baseType === '电影') return `movie:${tmdb || title}`;
  if (baseType === '剧集包') return `pack:${tmdb || title}:S${season || ''}`;
  if (baseType === '单集') return `ep:${tmdb || title}:S${season || ''}:E${episode || ''}`;
  return `${baseType}:${tmdb || title}:${season}:${episode}`;
};

const groupCenterSources = (items) => {
  const groups = [];
  const byKey = new Map();
  for (const item of (items || [])) {
    const key = centerGroupKey(item);
    let group = byKey.get(key);
    if (!group) {
      group = {
        group_key: key,
        title: item.title,
        media_title: item.media_title,
        tmdb_id: item.tmdb_id,
        share_tmdb_id: item.share_tmdb_id,
        release_year: item.release_year,
        season_number: item.season_number,
        episode_number: item.episode_number,
        display_type: centerRowType(item),
        versions: [],
      };
      byKey.set(key, group);
      groups.push(group);
    }
    group.versions.push(item);
  }
  return groups;
};

const lineStack = (items, renderFn, tooltipFn = null) => {
  const rows = (items || []).map((it, idx) => {
    const content = renderFn(it, idx);
    const title = tooltipFn ? tooltipFn(it, idx) : null;
    return h('div', { class: 'center-version-line', key: idx, title: title || undefined }, [content]);
  });
  return h('div', { class: 'center-version-stack' }, rows);
};

const trackListToStrings = (items) => (items || []).map(x => typeof x === 'string' ? x : (x.display || [x.language, x.codec, x.channels ? `${x.channels}ch` : '', x.title].filter(Boolean).join(' '))).filter(Boolean);
const defaultTrackText = (items) => {
  const arr = trackListToStrings(items);
  if (!arr.length) return '-';
  const found = arr.find(x => /默认/.test(x));
  return found || arr[0];
};
const allTrackTitle = (items) => {
  const arr = trackListToStrings(items);
  return arr.length ? arr.join('\n') : '';
};

const centerColumns = [
  { title: '片名', key: 'title', minWidth: 190, fixed: 'left', render: row => h('div', null, [
    h('div', { class: 'main-title' }, centerTitleText(row)),
    h('div', { class: 'sub-title' }, `TMDb ${row.tmdb_id || row.share_tmdb_id || '-'}`)
  ]) },
  { title: '类型', key: 'item_type', width: 130, render: row => centerSeasonText(row) },
  { title: '分辨率', key: 'resolution', width: 90, render: row => lineStack(row.versions, it => h('span', it.version_summary?.resolution || '-')) },
  { title: '视频编码', key: 'video_codec', width: 120, render: row => lineStack(row.versions, it => {
    const v = it.version_summary || {};
    return h('span', [v.video_codec || v.codec, v.bit_depth ? `${v.bit_depth}bit` : ''].filter(Boolean).join(' · ') || '-');
  }) },
  { title: 'HDR / 杜比', key: 'effect', width: 150, render: row => lineStack(row.versions, it => h('span', it.version_summary?.effect || '-'), it => it.version_summary?.effect || '') },
  { title: '帧率', key: 'fps', width: 110, render: row => lineStack(row.versions, it => h('span', it.version_summary?.fps || '-')) },
  { title: '音轨', key: 'audios', minWidth: 220, render: row => lineStack(row.versions, it => h('span', defaultTrackText(it.version_summary?.audio_list || it.version_summary?.audios)), it => allTrackTitle(it.version_summary?.audio_list || it.version_summary?.audios)) },
  { title: '字幕', key: 'subtitles', minWidth: 220, render: row => lineStack(row.versions, it => h('span', defaultTrackText(it.version_summary?.subtitle_list || it.version_summary?.subtitles)), it => allTrackTitle(it.version_summary?.subtitle_list || it.version_summary?.subtitles)) },
  { title: '大小', key: 'size', width: 95, render: row => lineStack(row.versions, it => h('span', formatCenterSize(it))) },
  { title: '可用性', key: 'status', width: 105, render: row => lineStack(row.versions, it => centerStatusTag(it)) },
  { title: '操作', key: 'actions', width: 190, fixed: 'right', render: row => lineStack(row.versions, it => h(NSpace, { size: 6 }, { default: () => [
    h(NButton, { size: 'small', type: 'primary', secondary: true, onClick: () => importCenterSource(it, 'permanent') }, { default: () => '转存' }),
    h(NButton, { size: 'small', secondary: true, onClick: () => importCenterSource(it, 'virtual') }, { default: () => '入库' })
  ] })) },
];

const ledgerColumns = [
  { title: '时间', key: 'created_at', width: 180, render: row => row.__group ? '本季汇总' : fmtDate(row.created_at) },
  { title: '事件', key: 'event_type', width: 190, render: row => {
    if (row.__group) {
      const collapsed = ledgerCollapsedGroups[row.__group_key] !== false;
      return h(NButton, { size: 'tiny', text: true, onClick: () => toggleLedgerGroup(row.__group_key) }, {
        icon: () => h(NIcon, null, { default: () => h(collapsed ? ChevronForwardIcon : ChevronDownIcon) }),
        default: () => ledgerEventLabel(row.event_type)
      });
    }
    if (row.__child) return h('span', { class: 'ledger-child-event' }, ledgerEventLabel(row.event_type));
    return ledgerEventLabel(row.event_type);
  } },
  { title: '变化', key: 'delta', width: 90, render: row => {
    const n = Number(row.delta || 0);
    return h(NTag, { type: n > 0 ? 'success' : (n < 0 ? 'error' : 'default'), size: 'small' }, { default: () => formatDelta(n) });
  } },
  { title: '标题', key: 'title', minWidth: 220, ellipsis: { tooltip: true }, render: row => h('span', { class: row.__child ? 'ledger-child-title' : (row.__group ? 'ledger-group-title' : '') }, row.title || '-') },
  { title: '原因', key: 'reason', minWidth: 360, ellipsis: { tooltip: true }, render: row => h('span', { class: row.__group ? 'ledger-group-reason' : '' }, row.reason || '-') },
];

const loadSummary = async () => { const res = await axios.get('/api/shared/resources/summary'); summary.value = res.data?.data || { local: {}, shares: {}, credit: {} }; };
const loadVirtualItems = async () => { loading.value = true; try { const res = await axios.get('/api/shared/resources/virtual', { params: { ...virtualFilters, page: virtualPagination.page, page_size: virtualPagination.pageSize } }); virtualItems.value = res.data?.items || []; virtualPagination.itemCount = Number(res.data?.total || 0); } catch (e) { message.error(e.response?.data?.message || '加载虚拟资源失败'); } finally { loading.value = false; } };
const loadShares = async () => { sharesLoading.value = true; try { const res = await axios.get('/api/shared/resources/shares', { params: { ...shareFilters, page: sharePagination.page, page_size: sharePagination.pageSize } }); shareItems.value = res.data?.items || []; sharePagination.itemCount = Number(res.data?.total || 0); } catch (e) { message.error(e.response?.data?.message || '加载我的分享失败'); } finally { sharesLoading.value = false; } };

const loadCenterSources = async () => {
  centerLoading.value = true;
  try {
    const params = {
      keyword: centerFilters.keyword,
      item_type: centerFilters.item_type === 'all' ? '' : centerFilters.item_type,
      status: centerFilters.status,
      limit: centerPagination.pageSize,
      offset: (centerPagination.page - 1) * centerPagination.pageSize,
    };
    const res = await axios.get('/api/shared/resources/center/sources', { params });
    centerSources.value = res.data?.items || [];
    centerPagination.itemCount = Number(res.data?.total || 0);
  } catch (e) {
    message.error(e.response?.data?.message || '加载中心资源库失败');
  } finally {
    centerLoading.value = false;
  }
};
const triggerSharedMaintenance = async () => {
  maintenanceSubmitting.value = true;
  try {
    const res = await axios.post('/api/shared/resources/tasks/maintenance');
    message.success(res.data?.message || '维护任务已提交');
  } catch (e) {
    message.error(e.response?.data?.message || '提交维护任务失败');
  } finally {
    maintenanceSubmitting.value = false;
  }
};

const loadLedger = async () => { ledgerLoading.value = true; try { const res = await axios.get('/api/shared/resources/credit/ledger', { params: { limit: 200, actual_only: 1, sync_center: 1 } }); ledgerItems.value = res.data?.items || []; } catch { message.error('加载贡献值流水失败'); } finally { ledgerLoading.value = false; } };
const loadAll = async () => { await Promise.allSettled([loadSummary(), loadVirtualItems(), loadShares(), loadLedger()]); };
const handleTabChange = (name) => { if (name === 'virtual') loadVirtualItems(); if (name === 'shares') loadShares(); if (name === 'center') loadCenterSources(); if (name === 'ledger') loadLedger(); };

const registerCenterDevice = async () => {
  const doRegister = async () => {
    registeringDevice.value = true;
    try {
      const res = await axios.post('/api/shared/resources/center/device/register', {});
      message.success(res.data?.message || '中心设备已注册');
      await Promise.allSettled([loadSummary(), loadLedger(), loadCenterSources()]);
    } catch (e) {
      message.error(e.response?.data?.message || '注册中心设备失败');
    } finally {
      registeringDevice.value = false;
    }
  };

  if (hasCenterDevice.value) {
    dialog.warning({
      title: '重置中心设备令牌',
      content: '这会重新向共享中心申请 device_token，并覆盖本地 p115_shared_device_token。通常只在 token 失效或迁移中心后使用。确定继续吗？',
      positiveText: '重置',
      negativeText: '取消',
      onPositiveClick: doRegister,
    });
    return;
  }
  await doRegister();
};

const refreshCredit = async () => { refreshingCredit.value = true; try { await axios.post('/api/shared/resources/credit/refresh'); message.success('贡献值已同步'); await Promise.allSettled([loadSummary(), loadLedger()]); } catch (e) { message.error(e.response?.data?.message || '刷新贡献值失败'); } finally { refreshingCredit.value = false; } };

const resetManualShareForm = () => {
  Object.assign(manualShareForm, {
    root_fid: '', root_name: '', root_is_dir: true, title: '', tmdb_id: '', parent_series_tmdb_id: '',
    share_type: 'season_pack', item_type: 'Season', season_number: 1, release_year: null, receive_code: manualShareForm.receive_code || ''
  });
  selectedMedia.value = null;
};

const openManualShareModal = () => {
  resetManualShareForm();
  mediaCandidates.value = [];
  mediaSearchKeyword.value = '';
  showManualShareModal.value = true;
};

const searchShareableMedia = async () => {
  const keyword = (mediaSearchKeyword.value || '').trim();
  if (!keyword) return message.warning('请输入片名或 TMDb ID');
  mediaSearchLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/media/search', { params: { keyword, limit: 30 } });
    mediaCandidates.value = res.data?.items || [];
    if (!mediaCandidates.value.length) message.info('没有搜索到本地媒体记录');
  } catch (e) {
    message.error(e.response?.data?.message || '搜索可分享媒体失败');
  } finally {
    mediaSearchLoading.value = false;
  }
};

const chooseMediaCandidate = (row) => {
  if (!row?.resolvable || !row.root_fid) {
    return message.warning(row?.message || '该媒体暂时无法自动定位 115 目录/FID');
  }
  selectedMedia.value = row;
  Object.assign(manualShareForm, {
    root_fid: row.root_fid || '',
    root_name: row.root_name || '',
    root_is_dir: row.root_is_dir !== false,
    title: row.display_title || row.title || row.root_name || '',
    tmdb_id: row.share_tmdb_id || row.tmdb_id || '',
    parent_series_tmdb_id: row.parent_series_tmdb_id || '',
    share_type: row.share_type || 'season_pack',
    item_type: row.share_item_type || row.item_type || 'Season',
    season_number: row.season_number || null,
    release_year: row.release_year || null,
  });
  message.success('已自动填充分享信息');
};

const manualCreateShare = async () => {
  if (!manualShareForm.root_fid) return message.warning('请先搜索并选择一个可分享媒体');
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
const reportShare = async (row) => { try { const res = await axios.post(`/api/shared/resources/shares/${row.id}/report-center`); message.success(res.data?.message || '已登记'); await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]); } catch (e) { message.error(e.response?.data?.message || '登记失败'); } };
const cancelShare = (row) => { dialog.warning({ title: '取消分享', content: `确定取消《${row.title || row.root_name}》的 115 分享吗？`, positiveText: '取消分享', negativeText: '保留', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/shares/${row.id}/cancel`); message.success('已取消分享'); await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]); } catch (e) { message.error(e.response?.data?.message || '取消失败'); } } }); };
const confirmDelete = (row) => { dialog.warning({ title: '删除虚拟资源', content: `确定删除《${row.title || row.file_name}》吗？如果已经播放转存，会同步删除 115 临时文件。`, positiveText: '删除', negativeText: '取消', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/delete`, { delete_remote: true, delete_local: true }); message.success('已删除'); await loadAll(); } catch (e) { message.error(e.response?.data?.message || '删除失败'); } } }); };
const confirmPromote = (row) => { dialog.info({ title: '转为正式资源', content: `确定将《${row.title || row.file_name}》从临时转存目录移动到正式媒体库吗？`, positiveText: '转正', negativeText: '取消', onPositiveClick: async () => { try { await axios.post(`/api/shared/resources/virtual/${row.virtual_id}/promote`); message.success('已转正'); await loadAll(); } catch (e) { message.error(e.response?.data?.message || '转正失败'); } } }); };

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
.pre-line { white-space: pre-line; line-height: 1.55; }
.selected-share-box { border: 1px solid rgba(128,128,128,.22); border-radius: 12px; padding: 12px 14px; background: rgba(128,128,128,.06); }
.selected-title { font-weight: 700; margin-bottom: 6px; }
.selected-desc { font-size: 12px; opacity: .68; line-height: 1.7; }
@media (max-width: 768px) { .page-header { flex-direction: column; } }
.warning-text { color: #d03050; font-size: 12px; }

.ledger-group-title { font-weight: 600; }
.ledger-group-reason { color: var(--text-color-2); }
.ledger-child-title { padding-left: 22px; }
.ledger-child-event { color: var(--text-color-3); }


.center-track-list {
  line-height: 1.45;
  font-size: 12px;
  white-space: normal;
}
.center-track-line {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 100%;
}


.center-version-stack { display: flex; flex-direction: column; gap: 8px; }
.center-version-line { min-height: 24px; display: flex; align-items: center; }

</style>
