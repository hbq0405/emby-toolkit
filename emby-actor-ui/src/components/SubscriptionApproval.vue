<!-- src/components/SubscriptionApproval.vue -->
<template>
  <div>
    <n-space justify="space-between" style="margin-bottom: 15px;">
      <n-h2 style="margin: 0;">待审订阅</n-h2>
      <n-space v-if="hasCheckedRows">
        <n-popconfirm @positive-click="handleBatchApprove">
          <template #trigger>
            <n-button type="primary" :loading="isProcessingBatch">批量批准 ({{ checkedRowKeys.length }})</n-button>
          </template>
          确认批准选中的 {{ checkedRowKeys.length }} 条订阅请求吗？
        </n-popconfirm>
        <n-button type="error" ghost :loading="isProcessingBatch" @click="openRejectModal(null)">批量拒绝 ({{ checkedRowKeys.length }})</n-button>
      </n-space>
    </n-space>

    <n-data-table
      :columns="columns"
      :data="requests"
      :loading="loading"
      :row-key="row => `${row.tmdb_id}-${row.item_type}`"
      @update:checked-row-keys="handleCheck"
      :checked-row-keys="checkedRowKeys"
    />
  </div>

  <n-modal v-model:show="showRejectModal" preset="card" style="width: 600px" :title="currentRowToReject ? '填写拒绝理由' : '填写批量拒绝理由'">
    <n-input
      v-model:value="rejectionReason"
      type="textarea"
      placeholder="请输入拒绝理由（选填）"
    />
    <template #footer>
      <n-space justify="end">
        <n-button @click="showRejectModal = false">取消</n-button>
        <n-button type="primary" :loading="!!processingId" @click="handleReject">确认拒绝</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, onMounted, h, computed } from 'vue';
import { NDataTable, NButton, NSpace, useMessage, NPopconfirm, NH2, NModal, NInput } from 'naive-ui';
import axios from 'axios';
import { format } from 'date-fns';

const message = useMessage();
const loading = ref(false);
const requests = ref([]);
const processingId = ref(null);
const showRejectModal = ref(false);
const rejectionReason = ref('');
const currentRowToReject = ref(null);
const checkedRowKeys = ref([]);

const openRejectModal = (row) => {
  currentRowToReject.value = row;
  rejectionReason.value = '';
  showRejectModal.value = true;
};

const hasCheckedRows = computed(() => checkedRowKeys.value.length > 0);
const isProcessingBatch = computed(() => processingId.value === 'batch');

const fetchData = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/admin/subscriptions/pending');
    requests.value = response.data;
  } catch (error) {
    message.error('加载待审列表失败');
  } finally {
    loading.value = false;
  }
};

onMounted(fetchData);

const handleCheck = (rowKeys) => {
  checkedRowKeys.value = rowKeys;
};

// ★★★ 核心修改 1/3: 创建一个辅助函数，用于从 rowKey 转换成后端需要的格式 ★★★
const getSelectedRequestsPayload = (keys) => {
  const selectedKeys = new Set(keys);
  return requests.value
    .filter(req => selectedKeys.has(`${req.tmdb_id}-${req.item_type}`))
    .map(req => ({
      tmdb_id: req.tmdb_id,
      item_type: req.item_type,
      title: req.title, // 方便后端发通知
      item_name: req.title // 兼容旧字段
    }));
};

const handleBatchApprove = async () => {
  if (checkedRowKeys.value.length === 0) return;
  processingId.value = 'batch';
  try {
    const response = await axios.post('/api/admin/subscriptions/batch-approve', {
      // ★★★ 使用新的辅助函数转换数据 ★★★
      requests: getSelectedRequestsPayload(checkedRowKeys.value)
    });
    message.success(response.data.message || `成功批准 ${checkedRowKeys.value.length} 条订阅！`);
    checkedRowKeys.value = [];
    await fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '批量批准失败');
  } finally {
    processingId.value = null;
  }
};

const handleReject = async () => {
  let requestsToProcess = [];
  if (currentRowToReject.value) {
    // 单个拒绝
    requestsToProcess = [{
      tmdb_id: currentRowToReject.value.tmdb_id,
      item_type: currentRowToReject.value.item_type,
      title: currentRowToReject.value.title,
      item_name: currentRowToReject.value.title
    }];
    processingId.value = `${currentRowToReject.value.tmdb_id}-${currentRowToReject.value.item_type}`;
  } else if (checkedRowKeys.value.length > 0) {
    // 批量拒绝
    requestsToProcess = getSelectedRequestsPayload(checkedRowKeys.value);
    processingId.value = 'batch';
  } else {
    return;
  }

  try {
    const response = await axios.post('/api/admin/subscriptions/batch-reject', {
      requests: requestsToProcess,
      reason: rejectionReason.value
    });
    message.success(response.data.message || `成功拒绝 ${requestsToProcess.length} 条订阅！`);
    showRejectModal.value = false;
    currentRowToReject.value = null;
    checkedRowKeys.value = [];
    await fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '拒绝失败');
  } finally {
    processingId.value = null;
  }
};

const handleApprove = async (row) => {
  processingId.value = `${row.tmdb_id}-${row.item_type}`;
  try {
    const response = await axios.post('/api/admin/subscriptions/batch-approve', {
      // ★★★ 单个批准也使用新的数据格式 ★★★
      requests: [{
        tmdb_id: row.tmdb_id,
        item_type: row.item_type,
        title: row.title,
        item_name: row.title
      }]
    });
    message.success(response.data.message || '订阅批准成功！');
    await fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '批准失败');
  } finally {
    processingId.value = null;
  }
};

// ★★★ 核心修改 2/3: 更新列定义，特别是 row-key 和操作按钮的 loading 逻辑 ★★★
const columns = [
  {
    type: 'selection',
    options: ['all', 'none']
  },
  {
    title: '媒体名称',
    key: 'title',
    render(row) {
      const url = `https://www.themoviedb.org/${row.item_type === 'Movie' ? 'movie' : 'tv'}/${row.tmdb_id}`;
      return h('a', { href: url, target: '_blank', style: 'color: inherit; text-decoration: none;' }, row.title);
    }
  },
  { title: '类型', key: 'item_type', render: (row) => (row.item_type === 'Movie' ? '电影' : '电视剧') },
  { title: '申请人', key: 'username' },
  { 
    title: '申请时间', 
    key: 'requested_at',
    render: (row) => {
      try {
        return format(new Date(row.requested_at), 'yyyy/MM/dd HH:mm:ss');
      } catch { return 'N/A'; }
    }
  },
  {
    title: '操作',
    key: 'actions',
    render(row) {
      const rowKey = `${row.tmdb_id}-${row.item_type}`;
      return h(NSpace, null, () => [
        h(NButton, {
          size: 'small',
          type: 'primary',
          loading: processingId.value === rowKey,
          onClick: () => handleApprove(row),
        }, { default: () => '批准' }),
        h(NButton, {
          size: 'small',
          type: 'error',
          ghost: true,
          loading: processingId.value === rowKey,
          onClick: () => openRejectModal(row),
        }, { default: () => '拒绝' }),
      ]);
    },
  },
];
</script>
