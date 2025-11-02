<!-- src/components/UserCenterPage.vue (账户信息版) -->
<template>
  <div style="padding: 24px;"> 
    <n-page-header :title="`欢迎回来, ${authStore.username}`" subtitle="在这里查看您的账户信息" />
    <n-grid cols="1 s:1 m:2 l:2" :x-gap="24" :y-gap="24" style="margin-top: 24px;">
      <n-gi>
        <n-card :loading="loading" title="账户详情" class="dashboard-card">
          <n-descriptions v-if="accountInfo" label-placement="left" bordered :column="1">
            <n-descriptions-item label="账户状态">
              <n-tag :type="statusType">{{ statusText }}</n-tag>
            </n-descriptions-item>
            
            <n-descriptions-item label="注册时间">
              {{ new Date(accountInfo.registration_date).toLocaleString() }}
            </n-descriptions-item>

            <n-descriptions-item label="到期时间">
              {{ accountInfo.expiration_date ? new Date(accountInfo.expiration_date).toLocaleString() : '永久有效' }}
            </n-descriptions-item>

            <n-descriptions-item label="账号等级">
              <strong>{{ accountInfo.template_name || '未分配' }}</strong>
            </n-descriptions-item>

            <n-descriptions-item label="等级说明">
              {{ accountInfo.template_description || '无' }}
            </n-descriptions-item>

            <n-descriptions-item label="订阅权限">
              <n-tag :type="accountInfo.allow_unrestricted_subscriptions ? 'success' : 'warning'">
                {{ accountInfo.allow_unrestricted_subscriptions ? '免审核订阅' : '需管理员审核' }}
              </n-tag>
            </n-descriptions-item>
          </n-descriptions>
          
          <n-empty v-else description="未能加载您的账户信息，请联系管理员。" />
        </n-card>
      </n-gi>
      <n-gi>
        <n-card :loading="loading" title="订阅历史" class="dashboard-card">
          <n-data-table
            :columns="historyColumns"
            :data="subscriptionHistory"
            :bordered="false"
            :single-line="false"
          />
        </n-card>
      </n-gi>
    </n-grid>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, h } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { NPageHeader, NCard, NDescriptions, NDescriptionsItem, NTag, NEmpty, NGrid, NGi, NStatistic } from 'naive-ui';

const authStore = useAuthStore();
const loading = ref(true);
const accountInfo = ref(null);
const subscriptionHistory = ref([]);

// 将状态文本映射到 Naive UI 的类型
const statusMap = {
  active: { text: '正常', type: 'success' },
  pending: { text: '待审批', type: 'warning' },
  expired: { text: '已过期', type: 'error' },
  disabled: { text: '已禁用', type: 'error' },
};

const statusText = computed(() => statusMap[accountInfo.value?.status]?.text || '未知');
const statusType = computed(() => statusMap[accountInfo.value?.status]?.type || 'default');

const historyColumns = [
  { title: '媒体名称', key: 'item_name' },
  { title: '类型', key: 'item_type', render: (row) => (row.item_type === 'Movie' ? '电影' : '电视剧') },
  {
    title: '状态',
    key: 'status',
    render(row) {
      const statusMap = {
        approved: { type: 'success', text: '已批准' },
        pending: { type: 'warning', text: '待审核' },
        rejected: { type: 'error', text: '已拒绝' },
      };
      const s = statusMap[row.status] || { type: 'default', text: '未知' };
      return h(NTag, { type: s.type }, { default: () => s.text });
    },
  },
  { title: '申请时间', key: 'requested_at', render: (row) => new Date(row.requested_at).toLocaleString() },
  { title: '备注', key: 'notes' },
];

onMounted(async () => {
  try {
    // 使用 Promise.all 并行加载两个接口的数据，提高效率
    const [accountResponse, statsResponse] = await Promise.all([
      axios.get('/api/portal/account-info'),
      axios.get('/api/portal/subscription-history')
    ]);
    accountInfo.value = accountResponse.data;
    subscriptionHistory.value = statsResponse.data;
  } catch (error) {
    console.error("加载用户中心数据失败:", error);
  } finally {
    loading.value = false;
  }
});
</script>