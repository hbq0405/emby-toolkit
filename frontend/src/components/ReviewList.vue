<!-- src/components/ReviewList.vue -->
<template>
  <n-layout content-style="padding: 24px;">
  <n-card class="dashboard-card" :bordered="false" size="small">
    
    <n-alert title="手动处理操作提示" type="info" style="margin-top: 24px;">
          可在下方搜索框输入片名直接搜索处理，也可以搜索存量剧集点击 <n-icon :component="AddToWatchlistIcon" /> 加入智能追剧 <br />
          可点击待复核列表媒体项进入手动编辑页面、可一键清空待复核列表转到已处理记录。
    </n-alert>
    <n-alert 
      v-if="props.taskStatus?.is_running" 
      title="后台任务运行中" 
      type="warning" 
      style="margin-bottom: 20px;"
      closable
    >
      后台任务正在运行，此时“手动处理”等操作可能会失败。
    </n-alert>
    
    <div>
      <!-- ★ 新增：搜索框和筛选框放在同一行 -->
      <n-space style="margin-bottom: 20px;" align="center">
        <n-input
          v-model:value="searchQuery"
          placeholder="输入媒体名称搜索整个 Emby 库..."
          clearable
          @keyup.enter="handleSearch"
          @clear="handleSearch"
          style="width: 300px;"
        >
          <template #suffix>
            <n-icon :component="SearchIcon" @click="handleSearch" style="cursor: pointer;" />
          </template>
        </n-input>

        <n-select
          v-model:value="selectedReason"
          :options="reasonOptions"
          placeholder="按失败原因筛选"
          clearable
          style="width: 200px;"
          @update:value="handleReasonChange"
          :disabled="isShowingSearchResults"
        />

        <n-popconfirm
            @positive-click="clearAllReviewItems"
            :positive-button-props="{ type: 'error' }"
          >
            <template #trigger>
              <n-button type="error" ghost :disabled="tableData.length === 0 || loading || isShowingSearchResults">
                <template #icon><n-icon :component="TrashIcon" /></template>
                清空所有待复核项
              </n-button>
            </template>
            确定要清空所有 {{ totalItems }} 条待复核记录吗？此操作不可恢复。
        </n-popconfirm>

        <n-popconfirm
            @positive-click="reprocessAllReviewItems"
        >
            <template #trigger>
                <n-button type="warning" ghost :disabled="tableData.length === 0 || loading || props.taskStatus?.is_running || isShowingSearchResults">
                    <template #icon><n-icon :component="ReprocessIcon" /></template>
                    <!-- ★ 动态按钮文字 -->
                    {{ selectedReason ? `重新处理筛选出的 ${totalItems} 项` : '重新处理所有' }}
                </n-button>
            </template>
            确定要重新处理 {{ selectedReason ? '当前筛选出的' : '所有' }} {{ totalItems }} 条记录吗？
        </n-popconfirm>
      </n-space>

      <n-spin :show="loading">
        <div v-if="error" class="error-message">
          <n-alert title="加载错误" type="error">{{ error }}</n-alert>
        </div>
        <div v-else>
          <n-data-table
            v-if="tableData.length > 0"
            :columns="columns"
            :data="tableData"
            :pagination="paginationProps"
            :bordered="false"
            :single-line="false" 
            striped
            size="small"
            :row-key="row => row.item_id"
            :loading="loadingAction[currentRowId]"
            remote 
          />
          <n-empty 
            v-else-if="!loading && tableData.length === 0" 
            :description="isShowingSearchResults ? '在 Emby 库中未找到匹配项。' : '太棒了！没有需要手动处理的媒体项。'" 
            style="margin-top: 50px; margin-bottom: 30px;" 
          />
        </div>
      </n-spin>
    </div>
  </n-card>
  </n-layout>
</template>

<script setup>
import { useRouter } from 'vue-router';
import { ref, onMounted, computed, h } from 'vue';
import axios from 'axios';
import {
    NCard, NSpin, NAlert, NText, NDataTable, NButton, NSpace, NPopconfirm, NEmpty, NInput, NIcon, NSelect,
    useMessage
} from 'naive-ui';
import { HeartOutline as AddToWatchlistIcon } from '@vicons/ionicons5';
import { SearchOutline as SearchIcon, PlayForwardOutline as ReprocessIcon, CheckmarkCircleOutline as MarkDoneIcon, TrashOutline as TrashIcon } from '@vicons/ionicons5';

const props = defineProps({
  taskStatus: {
    type: Object,
    required: true,
    default: () => ({ is_running: false }) 
  }
});

const router = useRouter();
const message = useMessage();

const tableData = ref([]);
const loading = ref(true);
const error = ref(null);
const totalItems = ref(0);
const currentPage = ref(1);
const itemsPerPage = ref(15);
const searchQuery = ref('');
const loadingAction = ref({});
const currentRowId = ref(null);
const isShowingSearchResults = ref(false);

// ★ 新增：原因筛选相关状态
const selectedReason = ref(null);
const reasonOptions = ref([]);

const fetchReasons = async () => {
  try {
    const res = await axios.get('/api/review_reasons');
    if (res.data && res.data.data) {
      reasonOptions.value = res.data.data.map(reason => ({
        label: reason,
        value: reason
      }));
    }
  } catch (err) {
    console.error("获取原因列表失败:", err);
  }
};

const handleReasonChange = () => {
  currentPage.value = 1;
  fetchReviewItems();
};

const addToWatchlist = async (rowData) => {
  if (rowData.item_type !== 'Series') {
    message.warning('只有剧集类型才能添加到追剧列表。');
    return;
  }
  const tmdbId = rowData.provider_ids?.Tmdb;
  if (!tmdbId) {
      message.error('无法添加到追剧列表：此项目缺少TMDb ID。');
      return;
  }
  try {
    const payload = {
      item_id: rowData.item_id,
      tmdb_id: tmdbId,
      item_name: rowData.item_name,
      item_type: rowData.item_type,
    };
    const response = await axios.post('/api/watchlist/add', payload);
    message.success(response.data.message || '添加成功！');
  } catch (error) {
    message.error(error.response?.data?.error || '添加到追剧列表失败，可能已存在。');
  }
};

const formatDate = (timestamp) => {
  if (!timestamp) return 'N/A';
  try {
    const date = new Date(timestamp);
    if (isNaN(date.getTime())) return '无效日期';
    return date.toLocaleString('zh-CN', {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    });
  } catch (e) {
    return '日期格式化错误';
  }
};

const clearAllReviewItems = async () => {
  loading.value = true;
  try {
    const response = await axios.post('/api/actions/clear_review_items');
    message.success(response.data.message);
    await fetchReviewItems(); 
    await fetchReasons(); // 清空后刷新下拉框
  } catch (err) {
    console.error("清空待复核列表失败:", err);
    message.error(`操作失败: ${err.response?.data?.error || err.message}`);
  } finally {
    loading.value = false;
  }
};

const goToEditPage = (row) => {
  if (row && row.item_id) {
    router.push({ name: 'MediaEditPage', params: { itemId: row.item_id } });
  } else {
    message.error("无效的媒体项，无法跳转到编辑页面！");
  }
};

const handleMarkAsProcessed = async (row) => {
  currentRowId.value = row.item_id;
  loadingAction.value[row.item_id] = true;
  try {
    await axios.post(`/api/actions/mark_item_processed/${row.item_id}`);
    message.success(`项目 "${row.item_name}" 已标记为已处理。`);
    await fetchReviewItems();
    await fetchReasons(); // 标记处理后可能原因列表变了
  } catch (err) {
    console.error("标记为已处理失败:", err);
    message.error(`标记项目 "${row.item_name}" 为已处理失败: ${err.response?.data?.error || err.message}`);
  } finally {
    loadingAction.value[row.item_id] = false;
    currentRowId.value = null;
  }
};

const columns = computed(() => [
  {
    title: '媒体名称 (ID)',
    key: 'item_name',
    resizable: true,
    render(row) {
      return h('div', { 
        style: 'cursor: pointer; padding: 5px 0;',
        onClick: () => goToEditPage(row)
      }, [
        h(NText, { strong: true }, { default: () => row.item_name || '未知名称' }),
        h(NText, { depth: 3, style: 'font-size: 0.8em; display: block; margin-top: 2px;' }, { default: () => `(ID: ${row.item_id || 'N/A'})` })
      ]);
    }
  },
  { 
    title: '类型', 
    key: 'item_type', 
    width: 80, 
    resizable: true,
    render(row) {
      const typeMap = { 'Movie': '电影', 'Series': '电视剧', 'Episode': '剧集' };
      return typeMap[row.item_type] || row.item_type;
    }
  },
  {
    title: '记录时间',
    key: 'failed_at',
    width: 170,
    resizable: true,
    render(row) { return isShowingSearchResults.value ? 'N/A' : formatDate(row.failed_at); }
  },
  { title: '原因', key: 'reason', resizable: true, ellipsis: { tooltip: true } },
  {
    title: '评分',
    key: 'score',
    width: 80,
    resizable: true,
    render(row) {
      return row.score !== null && row.score !== undefined ? row.score.toFixed(1) : 'N/A';
    }
  },
  {
    title: '操作',
    key: 'actions',
    width: 280,
    align: 'center',
    fixed: 'right',
    render(row) {
      const actionButtons = [];
      
      actionButtons.push(
        h(NPopconfirm, { onPositiveClick: () => handleReprocessItem(row) }, {
            trigger: () => h(NButton, {
                size: 'small',
                type: 'warning',
                ghost: true,
                loading: loadingAction.value[row.item_id] && currentRowId.value === row.item_id,
                disabled: loadingAction.value[row.item_id] || props.taskStatus?.is_running
            }, {
                icon: () => h(NIcon, { component: ReprocessIcon }),
                default: () => '重新处理'
            }),
            default: () => `确定要重新处理 "${row.item_name}" 吗？`
        })
      );

      actionButtons.push(
        h(NButton, {
          size: 'small',
          type: 'primary',
          onClick: () => goToEditPage(row)
        }, { default: () => '手动编辑' })
      );

      if (!isShowingSearchResults.value) {
        actionButtons.push(
          h(NPopconfirm, { onPositiveClick: () => handleMarkAsProcessed(row) }, {
            trigger: () => h(NButton, {
              size: 'small',
              type: 'success',
              ghost: true,
              loading: loadingAction.value[row.item_id] && currentRowId.value === row.item_id,
              disabled: loadingAction.value[row.item_id] || props.taskStatus?.is_running
            }, {
              icon: () => h(NIcon, { component: MarkDoneIcon }),
            }),
            default: () => `确定要将 "${row.item_name}" 标记为已处理吗？`
          })
        );
      }
      
      if (row.item_type === 'Series') {
        actionButtons.push(
          h(NButton, {
            size: 'small',
            title: '添加到追剧列表',
            onClick: () => addToWatchlist(row)
          }, { icon: () => h(NIcon, { component: AddToWatchlistIcon }) })
        );
      }
      return h(NSpace, { justify: 'center' }, { default: () => actionButtons });
    }
  }
]);

const paginationProps = computed(() => ({
    disabled: isShowingSearchResults.value,
    page: currentPage.value,
    pageSize: itemsPerPage.value,
    itemCount: totalItems.value,
    showSizePicker: true,
    pageSizes: [10, 15, 20, 30, 50, 100],
    onChange: (page) => {
        currentPage.value = page;
        if (!isShowingSearchResults.value) {
            fetchReviewItems();
        }
    },
    onUpdatePageSize: (pageSize) => {
        itemsPerPage.value = pageSize;
        currentPage.value = 1;
        if (!isShowingSearchResults.value) {
            fetchReviewItems();
        }
    }
}));

const fetchReviewItems = async () => {
  loading.value = true;
  error.value = null;
  isShowingSearchResults.value = false;
  try {
    const response = await axios.get(`/api/review_items`, {
        params: {
            page: currentPage.value,
            per_page: itemsPerPage.value,
            reason: selectedReason.value || '' // ★ 传递筛选原因
        }
    });
    tableData.value = response.data.items;
    totalItems.value = response.data.total_items;
  } catch (err) {
    handleFetchError(err, "加载待处理列表失败。");
  } finally {
    loading.value = false;
  }
};

const searchEmbyLibrary = async () => {
  loading.value = true;
  error.value = null;
  isShowingSearchResults.value = true;
  selectedReason.value = null; // 搜索时清空筛选
  try {
    const response = await axios.get(`/api/search_emby_library`, {
        params: { query: searchQuery.value }
    });
    tableData.value = response.data.items.map(item => ({
        ...item,
        failed_at: null,
        reason: 'N/A',
    }));
    totalItems.value = response.data.total_items;
  } catch (err) {
    handleFetchError(err, "搜索 Emby 媒体库失败。");
  } finally {
    loading.value = false;
  }
};

const handleReprocessItem = async (row) => {
  currentRowId.value = row.item_id;
  loadingAction.value[row.item_id] = true;
  try {
    const response = await axios.post(`/api/actions/reprocess_item/${row.item_id}`, {
        reason: row.reason 
    });
    message.success(response.data.message || `项目 "${row.item_name}" 的重新处理任务已提交。`);
    if (!isShowingSearchResults.value) {
        await fetchReviewItems();
    }
  } catch (err) {
    console.error("重新处理失败:", err);
    message.error(`操作失败: ${err.response?.data?.error || err.message}`);
  } finally {
    loadingAction.value[row.item_id] = false;
    currentRowId.value = null;
  }
};

const reprocessAllReviewItems = async () => {
  try {
    // ★ 提交时带上当前选中的原因
    const response = await axios.post('/api/actions/reprocess_all_review_items', {
      reason: selectedReason.value || ''
    });
    message.success(response.data.message || '重新处理任务已成功提交！');
  } catch (err) {
    console.error("提交重新处理任务失败:", err);
    message.error(`操作失败: ${err.response?.data?.error || err.message}`);
  }
};

const handleFetchError = (err, defaultMessage) => {
    console.error(defaultMessage, err);
    error.value = defaultMessage + (err.response?.data?.error || err.message);
    message.error(error.value);
};

const handleSearch = () => {
  if (searchQuery.value.trim()) {
    currentPage.value = 1;
    searchEmbyLibrary();
  } else {
    currentPage.value = 1;
    fetchReviewItems();
  }
};

onMounted(() => {
  fetchReasons(); // ★ 初始化时获取原因列表
  fetchReviewItems();
});
</script>