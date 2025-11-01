<!-- src/components/UserCenterPage.vue (账户信息版) -->
<template>
  <div>
    <n-page-header :title="`欢迎回来, ${authStore.username}`" subtitle="在这里查看您的账户信息" />

    <n-card :loading="loading" style="margin-top: 24px;">
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
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { NPageHeader, NCard, NDescriptions, NDescriptionsItem, NTag, NEmpty } from 'naive-ui';

const authStore = useAuthStore();
const loading = ref(true);
const accountInfo = ref(null);

// 将状态文本映射到 Naive UI 的类型
const statusMap = {
  active: { text: '正常', type: 'success' },
  pending: { text: '待审批', type: 'warning' },
  expired: { text: '已过期', type: 'error' },
  disabled: { text: '已禁用', type: 'error' },
};

const statusText = computed(() => statusMap[accountInfo.value?.status]?.text || '未知');
const statusType = computed(() => statusMap[accountInfo.value?.status]?.type || 'default');

onMounted(async () => {
  try {
    const response = await axios.get('/api/portal/account-info');
    accountInfo.value = response.data;
  } catch (error) {
    console.error("加载用户账户数据失败:", error);
  } finally {
    loading.value = false;
  }
});
</script>