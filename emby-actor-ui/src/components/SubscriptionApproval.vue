<!-- src/components/SubscriptionApproval.vue -->
<template>
  <div>
    <n-data-table
      :columns="columns"
      :data="requests"
      :loading="loading"
      :row-key="row => row.id"
    />
  </div>
  <n-modal v-model:show="showRejectModal" preset="card" style="width: 600px" title="填写拒绝理由">
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
import { ref, onMounted, h } from 'vue';
import { NDataTable, NButton, NSpace, useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const loading = ref(false);
const requests = ref([]);
const processingId = ref(null); // 用于跟踪正在处理的行
const showRejectModal = ref(false);
const rejectionReason = ref('');
const currentRowToReject = ref(null);

const openRejectModal = (row) => {
  currentRowToReject.value = row;
  rejectionReason.value = '';
  showRejectModal.value = true;
};

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

// 处理批准
const handleApprove = async (row) => {
  processingId.value = row.id;
  try {
    const response = await axios.post(`/api/admin/subscriptions/${row.id}/approve`);
    message.success(response.data.message || '批准成功！');
    fetchData(); // 刷新列表
  } catch (error) {
    message.error(error.response?.data?.message || '批准失败');
  } finally {
    processingId.value = null;
  }
};

// 处理拒绝
const handleReject = async () => {
  if (!currentRowToReject.value) return;
  processingId.value = currentRowToReject.value.id;
  try {
    // 在post请求中加入body
    const response = await axios.post(`/api/admin/subscriptions/${currentRowToReject.value.id}/reject`, {
      reason: rejectionReason.value
    });
    message.success(response.data.message || '已拒绝');
    showRejectModal.value = false;
    fetchData();
  } catch (error) {
    message.error(error.response?.data?.message || '拒绝失败');
  } finally {
    processingId.value = null;
  }
};

// 表格列定义
const columns = [
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