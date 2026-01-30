<!-- src/components/UserCenterPage.vue -->
<template>
  <!-- 动态 Padding -->
  <div :style="{ padding: isMobile ? '12px' : '24px' }"> 
    
    <!-- 头部 -->
    <n-page-header :title="`欢迎回来, ${accountInfo?.name || authStore.username}`" subtitle="在这里查看您的账户信息" />
    
    <!-- ================================================================================== -->
    <!-- 视图 A: PC 端 (宽屏) -->
    <!-- ================================================================================== -->
    <template v-if="!isMobile">
      <!-- 统计栏 -->
      <n-grid :cols="5" style="margin-top: 24px; text-align: center;">
        <n-gi><n-statistic label="总申请" :value="stats.total" /></n-gi>
        <n-gi><n-statistic label="已完成" :value="stats.completed" style="--n-value-text-color: var(--n-success-color)" /></n-gi>
        <n-gi><n-statistic label="处理中" :value="stats.processing" style="--n-value-text-color: var(--n-info-color)" /></n-gi>
        <n-gi><n-statistic label="待审核" :value="stats.pending" style="--n-value-text-color: var(--n-warning-color)" /></n-gi>
        <n-gi><n-statistic label="未通过" :value="stats.failed" style="--n-value-text-color: var(--n-error-color)" /></n-gi>
      </n-grid>

      <n-grid cols="2" :x-gap="24" :y-gap="24" style="margin-top: 24px;">
        <!-- 左侧卡片: 账户详情 -->
        <n-gi :span="1">
          <n-card :bordered="false" class="dashboard-card">
            <template #header>
              <span class="card-title">账户详情</span>
              <n-spin v-if="loading" size="small" style="float: right" />
            </template>
            <div v-if="accountInfo">
              <div class="profile-layout">
                <!-- 头像区域 -->
                <div class="profile-avatar-section">
                  <n-tooltip trigger="hover" placement="right">
                    <template #trigger>
                      <div class="avatar-wrapper" @click="triggerFileUpload">
                        <n-avatar
                          :size="120"
                          :src="avatarUrl"
                          :fallback-src="null"
                          object-fit="cover"
                          style="cursor: pointer; background-color: transparent;"
                        >
                          <span v-if="!avatarUrl" style="font-size: 40px;">
                            {{ authStore.username ? authStore.username.charAt(0).toUpperCase() : 'U' }}
                          </span>
                        </n-avatar>
                        <div class="avatar-overlay">
                          <n-icon size="30" color="white"><CloudUploadOutline /></n-icon>
                        </div>
                        <input type="file" ref="fileInput" style="display: none" accept="image/png, image/jpeg, image/jpg" @change="handleAvatarChange" />
                      </div>
                    </template>
                    点击更换头像
                  </n-tooltip>
                  <div class="username-text">{{ accountInfo?.name || authStore.username }}</div>
                </div>

                <!-- 详细信息表格 -->
                <div class="profile-info">
                  <n-descriptions label-placement="left" bordered :column="1" size="small">
                    <n-descriptions-item label="账户状态"><n-tag :type="statusType" size="small">{{ statusText }}</n-tag></n-descriptions-item>
                    <n-descriptions-item label="注册时间">{{ new Date(accountInfo.registration_date).toLocaleString() }}</n-descriptions-item>
                    <n-descriptions-item label="到期时间">{{ accountInfo.expiration_date ? new Date(accountInfo.expiration_date).toLocaleString() : '永久有效' }}</n-descriptions-item>
                    <n-descriptions-item label="账户等级">
                      <strong v-if="authStore.isAdmin">管理员</strong>
                      <strong v-else>{{ accountInfo.template_name || '未分配' }}</strong>
                    </n-descriptions-item>
                    <n-descriptions-item label="等级说明">
                      <span v-if="authStore.isAdmin">拥有系统所有管理权限</span>
                      <span v-else>{{ accountInfo.template_description || '无' }}</span>
                    </n-descriptions-item>
                    <n-descriptions-item label="订阅权限">
                      <n-tag v-if="authStore.isAdmin" type="success" size="small">免审核订阅</n-tag>
                      <n-tag v-else :type="accountInfo.allow_unrestricted_subscriptions ? 'success' : 'warning'" size="small">
                        {{ accountInfo.allow_unrestricted_subscriptions ? '免审核订阅' : '需管理员审核' }}
                      </n-tag>
                    </n-descriptions-item>
                    <n-descriptions-item label="Telegram Chat ID">
                      <n-input-group>
                        <n-input v-model:value="telegramChatId" placeholder="用于接收通知" size="small" />
                        <n-button type="primary" ghost :loading="isSavingChatId" @click="saveChatId" size="small">保存</n-button>
                      </n-input-group>
                    </n-descriptions-item>
                    <n-descriptions-item v-if="accountInfo && accountInfo.telegram_channel_id" label="全局通知">
                      <n-button text type="primary" tag="a" :href="globalChannelLink" target="_blank" size="small">点击加入频道/群组</n-button>
                    </n-descriptions-item>
                  </n-descriptions>

                  <div style="margin-top: 12px;">
                    <n-text depth="3" style="font-size:0.8em; line-height: 1.6;">
                      1. 点击按钮 <n-button text type="primary" :loading="isFetchingBotLink" @click="openBotChat" style="font-weight: bold; text-decoration: underline;" size="small">与机器人对话</n-button> 并发送 <code>/start</code><br>
                      2. 再向 <a href="https://t.me/userinfobot" target="_blank" style="color: var(--n-primary-color);">@userinfobot</a> 获取您的数字 ID 并粘贴于此。
                    </n-text>
                  </div>
                </div>
              </div>
            </div>
            <n-empty v-else description="未能加载您的账户信息，请联系管理员。" />
          </n-card>
        </n-gi>

        <!-- 右侧卡片: 订阅历史 -->
        <n-gi :span="1">
          <n-card :bordered="false" class="dashboard-card">
            <template #header>
              <div style="display: flex; justify-content: space-between; align-items: center;">
                <span class="card-title">订阅历史</span>
                <n-radio-group v-model:value="filterStatus" size="small">
                  <n-radio-button value="all">全部</n-radio-button>
                  <n-radio-button value="completed">已完成</n-radio-button>
                  <n-radio-button value="processing">处理中</n-radio-button>
                  <n-radio-button value="pending">待审核</n-radio-button>
                  <n-radio-button value="failed">未通过</n-radio-button>
                </n-radio-group>
              </div>
            </template>
            <n-data-table :columns="historyColumns" :data="subscriptionHistory" :bordered="false" :single-line="false" :pagination="false" />
            <div v-if="totalRecords > pageSize" style="margin-top: 16px; display: flex; justify-content: center;">
              <n-pagination v-model:page="currentPage" :page-size="pageSize" :item-count="totalRecords" show-quick-jumper @update:page="fetchSubscriptionHistory" />
            </div>
          </n-card>
        </n-gi>
      </n-grid>
    </template>

    <!-- ================================================================================== -->
    <!-- 视图 B: Mobile 端 (手机) -->
    <!-- ================================================================================== -->
    <template v-else>
      <!-- 1. 统计概览 (小卡片) -->
      <n-grid :cols="3" :x-gap="8" :y-gap="8" style="margin-top: 16px;">
        <n-gi><n-card size="small" :bordered="false" class="mobile-stat-card"><n-statistic label="总申请"><span class="mobile-stat-value">{{ stats.total }}</span></n-statistic></n-card></n-gi>
        <n-gi><n-card size="small" :bordered="false" class="mobile-stat-card"><n-statistic label="已完成" style="--n-value-text-color: var(--n-success-color)"><span class="mobile-stat-value">{{ stats.completed }}</span></n-statistic></n-card></n-gi>
        <n-gi><n-card size="small" :bordered="false" class="mobile-stat-card"><n-statistic label="处理中" style="--n-value-text-color: var(--n-info-color)"><span class="mobile-stat-value">{{ stats.processing }}</span></n-statistic></n-card></n-gi>
      </n-grid>

      <!-- 2. 账户信息 (折叠面板或卡片) -->
      <n-card size="small" :bordered="false" title="账户信息" style="margin-top: 12px;">
        <div class="mobile-profile-header">
          <div class="avatar-wrapper" @click="triggerFileUpload" style="width: 64px; height: 64px;">
            <n-avatar :size="64" :src="avatarUrl" :fallback-src="null" object-fit="cover">
              <span v-if="!avatarUrl">{{ authStore.username ? authStore.username.charAt(0).toUpperCase() : 'U' }}</span>
            </n-avatar>
            <input type="file" ref="fileInput" style="display: none" accept="image/png, image/jpeg, image/jpg" @change="handleAvatarChange" />
          </div>
          <div class="mobile-profile-text">
            <div class="mobile-username">{{ accountInfo?.name || authStore.username }}</div>
            <n-tag :type="statusType" size="tiny" round>{{ statusText }}</n-tag>
          </div>
        </div>
        
        <n-divider style="margin: 12px 0;" />
        
        <div class="mobile-info-list">
          <div class="mobile-info-row"><span>等级</span><span>{{ authStore.isAdmin ? '管理员' : (accountInfo?.template_name || '未分配') }}</span></div>
          <div class="mobile-info-row"><span>到期</span><span>{{ accountInfo?.expiration_date ? new Date(accountInfo.expiration_date).toLocaleDateString() : '永久' }}</span></div>
          <div class="mobile-info-row" style="align-items: center;">
            <span>通知 ID</span>
            <div style="flex: 1; margin-left: 12px; display: flex; gap: 4px;">
              <n-input v-model:value="telegramChatId" placeholder="Chat ID" size="tiny" />
              <n-button type="primary" ghost size="tiny" :loading="isSavingChatId" @click="saveChatId">保存</n-button>
            </div>
          </div>
        </div>
      </n-card>

      <!-- 3. 订阅历史 (卡片列表) -->
      <n-card size="small" :bordered="false" title="订阅历史" style="margin-top: 12px;">
        <template #header-extra>
          <!-- 移动端使用下拉菜单筛选 -->
          <n-radio-group v-model:value="filterStatus" size="small">
             <n-radio-button value="all">全部</n-radio-button>
             <n-radio-button value="processing">进行中</n-radio-button>
          </n-radio-group>
        </template>

        <div v-if="subscriptionHistory.length > 0" class="mobile-history-list">
          <div v-for="item in subscriptionHistory" :key="item.id" class="mobile-history-item">
            <div class="history-item-header">
              <span class="history-title">{{ item.title }}</span>
              <n-tag :type="getStatusType(item.status)" size="tiny" :bordered="false">
                {{ getStatusText(item.status) }}
              </n-tag>
            </div>
            <div class="history-item-meta">
              <span>{{ item.item_type === 'Movie' ? '电影' : '剧集' }}</span>
              <span>{{ new Date(item.requested_at).toLocaleDateString() }}</span>
            </div>
            <div v-if="item.notes" class="history-item-notes">
              备注: {{ item.notes }}
            </div>
          </div>
          
          <!-- 移动端分页 -->
          <div v-if="totalRecords > pageSize" style="margin-top: 16px; display: flex; justify-content: center;">
            <n-pagination v-model:page="currentPage" :page-size="pageSize" :item-count="totalRecords" simple @update:page="fetchSubscriptionHistory" />
          </div>
        </div>
        <n-empty v-else description="暂无记录" size="small" />
      </n-card>
    </template>

  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, h, watch } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { 
  NPageHeader, NCard, NDescriptions, NDescriptionsItem, NTag, NEmpty, NGrid, NGi, 
  NDataTable, NInputGroup, NInput, NButton, NText, useMessage, NPagination, 
  NStatistic, NRadioGroup, NRadioButton, NAvatar, NIcon, NDivider, NTooltip, NSpin
} from 'naive-ui';
import { CloudUploadOutline } from '@vicons/ionicons5';

const authStore = useAuthStore();
const loading = ref(true);
const accountInfo = ref(null);
const subscriptionHistory = ref([]);
const telegramChatId = ref('');
const isSavingChatId = ref(false);
const message = useMessage();
const isFetchingBotLink = ref(false);

// 移动端检测
const isMobile = ref(false);
const checkMobile = () => {
  isMobile.value = window.innerWidth < 768;
};

// 分页相关状态
const currentPage = ref(1);
const pageSize = ref(10); 
const totalRecords = ref(0);
const stats = ref({ total: 0, completed: 0, processing: 0, pending: 0, failed: 0 });
const filterStatus = ref('all');
const fileInput = ref(null);

const avatarUrl = computed(() => {
  const tag = accountInfo.value?.profile_image_tag;
  const userId = accountInfo.value?.id;
  if (userId && tag) {
    return `/image_proxy/Users/${userId}/Images/Primary?tag=${tag}`;
  }
  return null;
});

const triggerFileUpload = () => {
  fileInput.value?.click();
};

const handleAvatarChange = async (event) => {
  const file = event.target.files[0];
  if (!file) return;
  if (!['image/jpeg', 'image/png', 'image/jpg'].includes(file.type)) {
    message.error('只支持 JPG/PNG 格式的图片');
    return;
  }
  const formData = new FormData();
  formData.append('avatar', file);
  const loadingMsg = message.loading('正在上传头像...', { duration: 0 });
  try {
    const res = await axios.post('/api/portal/upload-avatar', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
    loadingMsg.destroy();
    message.success('头像更新成功！');
    if (accountInfo.value && res.data.new_tag) {
      accountInfo.value.profile_image_tag = res.data.new_tag;
    }
  } catch (error) {
    loadingMsg.destroy();
    message.error(error.response?.data?.message || '上传失败');
  } finally {
    event.target.value = ''; 
  }
};

const statusMap = {
  active: { text: '正常', type: 'success' },
  pending: { text: '待审批', type: 'warning' },
  expired: { text: '已过期', type: 'error' },
  disabled: { text: '已禁用', type: 'error' },
};

const statusText = computed(() => statusMap[accountInfo.value?.status]?.text || '未知');
const statusType = computed(() => statusMap[accountInfo.value?.status]?.type || 'default');

const globalChannelLink = computed(() => {
  if (!accountInfo.value || !accountInfo.value.telegram_channel_id) return '#';
  const channelId = accountInfo.value.telegram_channel_id.trim();
  if (channelId.startsWith('https://t.me/')) return channelId;
  if (channelId.startsWith('@')) return `https://t.me/${channelId.substring(1)}`;
  return `https://t.me/${channelId}`;
});

// 状态辅助函数 (供 PC 和 Mobile 共用)
const getStatusInfo = (status) => {
  const map = {
    completed: { type: 'success', text: '已完成' },
    WANTED: { type: 'info', text: '处理中' }, 
    REQUESTED: { type: 'warning', text: '待审核' },
    IGNORED: { type: 'error', text: '已忽略' },
    SUBSCRIBED: { type: 'info', text: '已订阅' }, 
    PENDING_RELEASE: { type: 'error', text: '未上映' },
    NONE: { type: 'warning', text: '已取消' },
    PAUSED: { type: 'warning', text: '已暂停' },
  };
  return map[status] || { type: 'default', text: status };
};

const getStatusType = (status) => getStatusInfo(status).type;
const getStatusText = (status) => getStatusInfo(status).text;

const historyColumns = [
  { title: '媒体名称', key: 'title' },
  { title: '类型', key: 'item_type', render: (row) => (row.item_type === 'Movie' ? '电影' : '电视剧') },
  {
    title: '状态',
    key: 'status',
    render(row) {
      const s = getStatusInfo(row.status);
      return h(NTag, { type: s.type, bordered: false }, { default: () => s.text });
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
      const errorMsg = response.data.error || '未能获取到机器人信息';
      message.error(errorMsg, { duration: 8000 });
    }
  } catch (error) {
    message.error('请求机器人信息失败');
  } finally {
    isFetchingBotLink.value = false;
  }
};

const fetchStats = async () => {
  try {
    const res = await axios.get('/api/portal/subscription-stats');
    stats.value = res.data;
  } catch (e) {
    console.error("获取统计失败", e);
  }
};

const fetchSubscriptionHistory = async (page = 1) => {
  loading.value = true;
  try {
    const response = await axios.get('/api/portal/subscription-history', {
      params: {
        page: page,
        page_size: pageSize.value,
        status: filterStatus.value,
      },
    });
    subscriptionHistory.value = response.data.items;
    totalRecords.value = response.data.total_records;
    currentPage.value = page;
  } catch (error) {
    message.error('加载订阅历史失败');
  } finally {
    loading.value = false;
  }
};

watch(filterStatus, () => {
  fetchSubscriptionHistory(1); 
});

onMounted(async () => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  try {
    const [accountResponse] = await Promise.all([
      axios.get('/api/portal/account-info'),
    ]);
    accountInfo.value = accountResponse.data;
    if (accountInfo.value) {
        telegramChatId.value = accountInfo.value.telegram_chat_id || '';
    }
    fetchStats();
    await fetchSubscriptionHistory();
  } catch (error) {
    message.error('加载账户信息失败');
  } finally {
    loading.value = false;
  }
});

onUnmounted(() => {
  window.removeEventListener('resize', checkMobile);
});
</script>

<style scoped>
/* PC 端样式 */
.profile-layout {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 24px;
}
.profile-avatar-section {
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-bottom: 10px;
}
.profile-info { width: 100%; }
.username-text {
  margin-top: 16px;
  font-weight: bold;
  font-size: 1.4em;
  text-align: center;
  word-break: break-all;
}
.avatar-wrapper {
  position: relative;
  border-radius: 50%;
  overflow: hidden;
  transition: transform 0.2s;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  display: flex;
  justify-content: center;
  align-items: center;
  line-height: 0;
  width: fit-content;
  margin: 0 auto;
}
.avatar-wrapper:hover { transform: scale(1.05); }
.avatar-overlay {
  position: absolute;
  top: 0; left: 0; width: 100%; height: 100%;
  background-color: rgba(0, 0, 0, 0.4);
  display: flex;
  justify-content: center;
  align-items: center;
  opacity: 0;
  transition: opacity 0.2s;
  border-radius: 50%;
}
.avatar-wrapper :deep(img) { display: block !important; width: 100%; height: 100%; object-fit: cover; }
.avatar-wrapper:hover .avatar-overlay { opacity: 1; }

/* Mobile 端样式 */
.mobile-stat-value { font-size: 1.4em; font-weight: 600; }
.mobile-profile-header { display: flex; align-items: center; gap: 16px; }
.mobile-profile-text { display: flex; flex-direction: column; gap: 4px; }
.mobile-username { font-size: 1.2em; font-weight: bold; }
.mobile-info-list { display: flex; flex-direction: column; gap: 8px; }
.mobile-info-row { display: flex; justify-content: space-between; font-size: 13px; color: var(--n-text-color-2); }

/* 移动端历史记录卡片列表 */
.mobile-history-list { display: flex; flex-direction: column; gap: 12px; }
.mobile-history-item {
  background-color: rgba(0, 0, 0, 0.02); /* 轻微背景色区分 */
  border-radius: 8px;
  padding: 12px;
  border: 1px solid rgba(0, 0, 0, 0.05);
}
.history-item-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
.history-title { font-weight: 600; font-size: 14px; flex: 1; margin-right: 8px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.history-item-meta { display: flex; justify-content: space-between; font-size: 12px; color: var(--n-text-color-3); }
.history-item-notes { margin-top: 6px; font-size: 12px; color: var(--n-text-color-3); background: rgba(0,0,0,0.03); padding: 4px; border-radius: 4px; }

/* 暗色模式适配 */
html.dark .mobile-history-item { background-color: rgba(255, 255, 255, 0.03); border: 1px solid rgba(255, 255, 255, 0.05); }
html.dark .history-item-notes { background: rgba(255,255,255,0.05); }
</style>