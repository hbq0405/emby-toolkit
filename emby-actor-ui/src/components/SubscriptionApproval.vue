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
      :row-key="row => row.id"
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
import { NDataTable, NButton, NSpace, useMessage, NPopconfirm } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const loading = ref(false);
const requests = ref([]);
const processingId = ref(null); // 用于跟踪正在处理的行
const showRejectModal = ref(false);
const rejectionReason = ref('');
const currentRowToReject = ref(null);
const checkedRowKeys = ref([]); // 新增：存储选中行的key

const openRejectModal = (row) => {
  currentRowToReject.value = row;
  rejectionReason.value = '';
  showRejectModal.value = true;
};

const hasCheckedRows = computed(() => checkedRowKeys.value.length > 0);
const isProcessingBatch = computed(() => processingId.value === 'batch');

// 获取数据
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

const handleBatchApprove = async () => {
  if (checkedRowKeys.value.length === 0) return;
  processingId.value = 'batch'; // 标记为批量处理
  try {
    const response = await axios.post('/api/admin/subscriptions/batch-approve', {
      ids: checkedRowKeys.value
    });
    message.success(response.data.message || `成功批准 ${checkedRowKeys.value.length} 条订阅！`);
    checkedRowKeys.value = []; // 清空选中
    fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '批量批准失败');
  } finally {
    processingId.value = null;
  }
};

const handleReject = async () => {
  let idsToReject = [];
  if (currentRowToReject.value) {
    idsToReject = [currentRowToReject.value.id];
  } else if (checkedRowKeys.value.length > 0) {
    idsToReject = checkedRowKeys.value;
  } else {
    return;
  }

  processingId.value = currentRowToReject.value ? currentRowToReject.value.id : 'batch';
  try {
    const response = await axios.post('/api/admin/subscriptions/batch-reject', {
      ids: idsToReject,
      reason: rejectionReason.value
    });
    message.success(response.data.message || `成功拒绝 ${idsToReject.length} 条订阅！`);
    showRejectModal.value = false;
    currentRowToReject.value = null;
    checkedRowKeys.value = []; // 清空选中
    fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '拒绝失败');
  } finally {
    processingId.value = null;
  }
};

const handleApprove = async (row) => {
  processingId.value = row.id;
  try {
    const response = await axios.post('/api/admin/subscriptions/approve', { id: row.id });
    message.success(response.data.message || '订阅批准成功！');
    fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '批准失败');
  } finally {
    processingId.value = null;
  }
};

// 表格列定义

const columns = [
  {
    type: 'selection',
    options: ['all', 'none']
  },
  { title: '媒体名称', key: 'item_name' },
  { title: '类型', key: 'item_type', render: (row) => (row.item_type === 'Movie' ? '电影' : '电视剧') },
  { title: '申请人', key: 'username' },
  { 
    title: '申请时间', 
    key: 'requested_at',
    render: (row) => new Date(row.requested_at).toLocaleString()
  },
  {
    title: '操作',
    key: 'actions',
    render(row) {
      return h(NSpace, null, () => [
        h(NButton, {
          size: 'small',
          type: 'primary',
          loading: processingId.value === row.id,
          onClick: () => handleApprove(row),
        }, { default: () => '批准' }),
        h(NButton, {
          size: 'small',
          type: 'error',
          ghost: true,
          loading: processingId.value === row.id,
          onClick: () => openRejectModal(row),
        }, { default: () => '拒绝' }),
      ]);
    },
  },
];
</script>
