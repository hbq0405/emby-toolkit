<!-- src/components/UserCenterPage.vue (账户信息版) -->
<template>
  <div style="padding: 24px;"> 
    <n-page-header :title="`欢迎回来, ${authStore.username}`" subtitle="在这里查看您的账户信息" />
    <n-grid cols="2" :x-gap="24" :y-gap="24" style="margin-top: 24px;">
      
      <!-- ==================== 左侧卡片: 账户详情 ==================== -->
      <n-gi :span="1">
        <n-card :loading="loading" title="账户详情" class="dashboard-card">
          
          <!-- ★★★ 核心修正: 使用 v-if 和 v-else 来切换显示内容 ★★★ -->
          <div v-if="accountInfo">
            <n-descriptions label-placement="left" bordered :column="1">
              <!-- 账户状态 -->
              <n-descriptions-item label="账户状态">
                <n-tag :type="statusType">{{ statusText }}</n-tag>
              </n-descriptions-item>
              <!-- 注册时间 -->
              <n-descriptions-item label="注册时间">
                {{ new Date(accountInfo.registration_date).toLocaleString() }}
              </n-descriptions-item>
              <!-- 到期时间 -->
              <n-descriptions-item label="到期时间">
                {{ accountInfo.expiration_date ? new Date(accountInfo.expiration_date).toLocaleString() : '永久有效' }}
              </n-descriptions-item>
              <!-- 账号等级 -->
              <n-descriptions-item label="账号等级">
                <strong>{{ accountInfo.template_name || '未分配' }}</strong>
              </n-descriptions-item>
              <!-- 等级说明 -->
              <n-descriptions-item label="等级说明">
                {{ accountInfo.template_description || '无' }}
              </n-descriptions-item>
              <!-- 订阅权限 -->
              <n-descriptions-item label="订阅权限">
                <n-tag :type="accountInfo.allow_unrestricted_subscriptions ? 'success' : 'warning'">
                  {{ accountInfo.allow_unrestricted_subscriptions ? '免审核订阅' : '需管理员审核' }}
                </n-tag>
              </n-descriptions-item>
              <!-- Telegram Chat ID -->
              <n-descriptions-item label="Telegram Chat ID">
                <n-input-group>
                  <n-input
                    v-model:value="telegramChatId"
                    placeholder="用于接收个人订阅通知"
                  />
                  <n-button
                    type="primary"
                    ghost
                    :loading="isSavingChatId"
                    @click="saveChatId"
                  >
                    保存
                  </n-button>
                </n-input-group>
              </n-descriptions-item>
              <n-descriptions-item v-if="accountInfo && accountInfo.telegram_channel_id" label="全局通知">
                <n-button 
                  text 
                  type="primary" 
                  tag="a"
                  :href="globalChannelLink"  
                  target="_blank"
                >
                  点击加入频道/群组
                </n-button>
                <template #feedback>
                  <n-text depth="3" style="font-size:0.8em;">
                    加入官方频道，获取新片入库等全局动态。
                  </n-text>
                </template>
              </n-descriptions-item>
            </n-descriptions>

            <!-- 引导文字区域 -->
            <div style="margin-top: 12px;">
              <n-text depth="3" style="font-size:0.8em; line-height: 1.6;">
                1. 点击按钮
                <n-button 
                  text 
                  type="primary" 
                  :loading="isFetchingBotLink"
                  @click="openBotChat" 
                  style="font-weight: bold; text-decoration: underline;"
                >
                  与机器人对话
                </n-button>
                并发送 <code>/start</code><br>
                2. 再向 <a href="https://t.me/userinfobot" target="_blank" style="color: var(--n-primary-color);">@userinfobot</a> 获取您的数字 ID 并粘贴于此。
              </n-text>
            </div>
          </div>
          
          <!-- 当 accountInfo 为空时，显示这个 n-empty -->
          <n-empty v-else description="未能加载您的账户信息，请联系管理员。" />

        </n-card>
      </n-gi>

      <!-- ==================== 右侧卡片: 订阅历史 ==================== -->
      <n-gi :span="1">
        <n-card :loading="loading" title="订阅历史" class="dashboard-card">
          <n-data-table
            :columns="historyColumns"
            :data="subscriptionHistory"
            :bordered="false"
            :single-line="false"
            :pagination="false"
          />
          <div v-if="totalRecords > pageSize" style="margin-top: 16px; display: flex; justify-content: center;">
            <n-pagination
              v-model:page="currentPage"
              :page-size="pageSize"
              :item-count="totalRecords"
              show-quick-jumper
              @update:page="fetchSubscriptionHistory"
            />
          </div>
        </n-card>
      </n-gi>

    </n-grid>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, h } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
// ★ 修正: 导入所有需要的组件，并移除不再使用的 NStatistic
import { 
  NPageHeader, NCard, NDescriptions, NDescriptionsItem, NTag, NEmpty, NGrid, NGi, 
  NDataTable, NInputGroup, NInput, NButton, NText, useMessage, NPagination 
} from 'naive-ui';
const authStore = useAuthStore();
const loading = ref(true);
const accountInfo = ref(null);
const subscriptionHistory = ref([]);
const telegramChatId = ref('');
const isSavingChatId = ref(false);
const message = useMessage();
const isFetchingBotLink = ref(false);

// 分页相关状态
const currentPage = ref(1);
const pageSize = ref(10); // 每页显示10条
const totalRecords = ref(0);

// 将状态文本映射到 Naive UI 的类型
const statusMap = {
  active: { text: '正常', type: 'success' },
  pending: { text: '待审批', type: 'warning' },
  expired: { text: '已过期', type: 'error' },
  disabled: { text: '已禁用', type: 'error' },
};

const statusText = computed(() => statusMap[accountInfo.value?.status]?.text || '未知');
const statusType = computed(() => statusMap[accountInfo.value?.status]?.type || 'default');

const globalChannelLink = computed(() => {
  if (!accountInfo.value || !accountInfo.value.telegram_channel_id) {
    return '#'; // 如果没有配置，返回一个无害的链接
  }
  
  const channelId = accountInfo.value.telegram_channel_id.trim();
  
  // 情况1: 如果填写的是完整的 URL
  if (channelId.startsWith('https://t.me/')) {
    return channelId;
  }
  
  // 情况2: 如果填写的是 @username
  if (channelId.startsWith('@')) {
    return `https://t.me/${channelId.substring(1)}`;
  }
  
  // 情况3: 如果只填了 username 或者私有频道的数字ID
  // (对于数字ID，Telegram 并没有官方的网页链接格式，但通常这样也能跳转)
  return `https://t.me/${channelId}`;
});

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

const saveChatId = async () => {
  isSavingChatId.value = true;
  try {
    const response = await axios.post('/api/portal/telegram-chat-id', { 
      chat_id: telegramChatId.value 
    });
    message.success(response.data.message || '保存成功！');
  } catch (error) {
    message.error(error.response?.data?.message || '保存失败');
  } finally {
    isSavingChatId.value = false;
  }
};

const openBotChat = async () => {
  isFetchingBotLink.value = true;
  try {
    const response = await axios.get('/api/portal/telegram-bot-info');
    const botName = response.data.bot_username;

    if (botName) {
      window.open(`https://t.me/${botName}`, '_blank');
    } else {
      // 如果获取失败，显示错误信息
      const errorMsg = response.data.error || '未能获取到机器人信息，请检查后台配置或网络。';
      message.error(errorMsg, { duration: 8000 });
    }
  } catch (error) {
    message.error('请求机器人信息失败，请检查网络连接。');
  } finally {
    isFetchingBotLink.value = false;
  }
};

// 获取订阅历史的函数，现在支持分页
const fetchSubscriptionHistory = async (page = 1) => {
  loading.value = true;
  try {
    const response = await axios.get('/api/portal/subscription-history', {
      params: {
        page: page,
        page_size: pageSize.value,
      },
    });
    subscriptionHistory.value = response.data.items;
    totalRecords.value = response.data.total_records;
    currentPage.value = page;
  } catch (error) {
    console.error("加载订阅历史失败:", error);
    message.error('加载订阅历史失败');
  } finally {
    loading.value = false;
  }
};

// ★ 修正: 完整的 onMounted 逻辑
onMounted(async () => {
  try {
    // ★★★ 核心修改: 只加载两个必要接口 ★★★
    const [accountResponse] = await Promise.all([
      axios.get('/api/portal/account-info'),
    ]);

    accountInfo.value = accountResponse.data;
    if (accountInfo.value) {
        telegramChatId.value = accountInfo.value.telegram_chat_id || '';
    }
    
    // 首次加载订阅历史
    await fetchSubscriptionHistory();

  } catch (error) {
    console.error("加载用户中心数据失败:", error);
    message.error('加载账户信息失败');
  } finally {
    loading.value = false;
  }
});
</script>
