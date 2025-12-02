<!-- src/components/UserCenterPage.vue (账户信息版) -->
<template>
  <div style="padding: 24px;"> 
    <n-page-header :title="`欢迎回来, ${authStore.username}`" subtitle="在这里查看您的账户信息" />
    <n-grid :cols="5" style="margin-top: 24px; text-align: center;">
      <n-gi><n-statistic label="总申请" :value="stats.total" /></n-gi>
      <n-gi><n-statistic label="已完成" :value="stats.completed" style="--n-value-text-color: var(--n-success-color)" /></n-gi>
      <n-gi><n-statistic label="处理中" :value="stats.processing" style="--n-value-text-color: var(--n-info-color)" /></n-gi>
      <n-gi><n-statistic label="待审核" :value="stats.pending" style="--n-value-text-color: var(--n-warning-color)" /></n-gi>
      <n-gi><n-statistic label="未通过" :value="stats.failed" style="--n-value-text-color: var(--n-error-color)" /></n-gi>
    </n-grid>
    <n-grid cols="2" :x-gap="24" :y-gap="24" style="margin-top: 24px;">
      
      <!-- ==================== 左侧卡片: 账户详情 ==================== -->
      <n-gi :span="1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">账户详情</span>
            <!-- 局部 Loading 指示器 -->
            <n-spin v-if="loading.core || loading.library || loading.system" size="small" style="float: right" />
          </template>
          <!-- ★★★ 核心修正: 使用 v-if 和 v-else 来切换显示内容 ★★★ -->
          <div v-if="accountInfo">
            
            <div class="profile-layout">
              
              <!-- 第一部分：头像显示与上传区域 (现在放在上面) -->
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
                      <!-- 悬浮遮罩 -->
                      <div class="avatar-overlay">
                        <n-icon size="30" color="white"><CloudUploadOutline /></n-icon>
                      </div>
                      <!-- 隐藏的文件输入框 -->
                      <input 
                        type="file" 
                        ref="fileInput" 
                        style="display: none" 
                        accept="image/png, image/jpeg, image/jpg"
                        @change="handleAvatarChange"
                      />
                    </div>
                  </template>
                  点击更换头像
                </n-tooltip>
                <div class="username-text">
                  {{ authStore.username }}
                </div>
              </div>

              <!-- 第二部分：详细信息表格 (现在放在下面) -->
              <div class="profile-info">
                <n-descriptions label-placement="left" bordered :column="1" size="small">
                  <!-- 账户状态 -->
                  <n-descriptions-item label="账户状态">
                    <n-tag :type="statusType" size="small">{{ statusText }}</n-tag>
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
                  <n-descriptions-item label="账户等级">
                    <strong v-if="authStore.isAdmin">管理员</strong>
                    <strong v-else>{{ accountInfo.template_name || '未分配' }}</strong>
                  </n-descriptions-item>
                  
                  <!-- 等级说明 -->
                  <n-descriptions-item label="等级说明">
                    <span v-if="authStore.isAdmin">拥有系统所有管理权限</span>
                    <span v-else>{{ accountInfo.template_description || '无' }}</span>
                  </n-descriptions-item>
                  
                  <!-- 订阅权限 -->
                  <n-descriptions-item label="订阅权限">
                    <n-tag v-if="authStore.isAdmin" type="success" size="small">
                      免审核订阅
                    </n-tag>
                    <n-tag v-else :type="accountInfo.allow_unrestricted_subscriptions ? 'success' : 'warning'" size="small">
                      {{ accountInfo.allow_unrestricted_subscriptions ? '免审核订阅' : '需管理员审核' }}
                    </n-tag>
                  </n-descriptions-item>
                  <!-- Telegram Chat ID -->
                  <n-descriptions-item label="Telegram Chat ID">
                    <n-input-group>
                      <n-input
                        v-model:value="telegramChatId"
                        placeholder="用于接收通知"
                        size="small"
                      />
                      <n-button
                        type="primary"
                        ghost
                        :loading="isSavingChatId"
                        @click="saveChatId"
                        size="small"
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
                      size="small"
                    >
                      点击加入频道/群组
                    </n-button>
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
                      size="small"
                    >
                      与机器人对话
                    </n-button>
                    并发送 <code>/start</code><br>
                    2. 再向 <a href="https://t.me/userinfobot" target="_blank" style="color: var(--n-primary-color);">@userinfobot</a> 获取您的数字 ID 并粘贴于此。
                  </n-text>
                </div>
              </div>
            </div>
          </div>
          <n-empty v-else description="未能加载您的账户信息，请联系管理员。" />
        </n-card>
      </n-gi>

      <!-- ==================== 右侧卡片: 订阅历史 ==================== -->
      <n-gi :span="1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span class="card-title">订阅历史</span>
              <!-- 新增筛选器 -->
              <n-radio-group v-model:value="filterStatus" size="small">
                <n-radio-button value="all">全部</n-radio-button>
                <n-radio-button value="completed">已完成</n-radio-button>
                <n-radio-button value="processing">处理中</n-radio-button>
                <n-radio-button value="pending">待审核</n-radio-button>
                <n-radio-button value="failed">未通过</n-radio-button>
              </n-radio-group>
            </div>
          </template>
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
  NDataTable, NInputGroup, NInput, NButton, NText, useMessage, NPagination, 
  NStatistic, NRadioGroup, NRadioButton, NAvatar, NIcon, NDivider, NTooltip
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


// 分页相关状态
const currentPage = ref(1);
const pageSize = ref(10); // 每页显示10条
const totalRecords = ref(0);
const stats = ref({ total: 0, completed: 0, processing: 0, pending: 0, failed: 0 });
const filterStatus = ref('all');
const fileInput = ref(null);

// ★★★ 计算头像 URL ★★★
const avatarUrl = computed(() => {
  const tag = accountInfo.value?.profile_image_tag;
  const userId = accountInfo.value?.id; // 确保后端返回了 id
  
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
    
    // 更新本地数据，触发头像刷新
    if (accountInfo.value && res.data.new_tag) {
      accountInfo.value.profile_image_tag = res.data.new_tag;
    }
    
  } catch (error) {
    loadingMsg.destroy();
    message.error(error.response?.data?.message || '上传失败');
  } finally {
    event.target.value = ''; // 清空 input
  }
};

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
  { title: '媒体名称', key: 'title' },
  { title: '类型', key: 'item_type', render: (row) => (row.item_type === 'Movie' ? '电影' : '电视剧') },
  {
    title: '状态',
    key: 'status',
    render(row) {
      const statusMap = {
        completed: { type: 'success', text: '已完成' },
        WANTED: { type: 'info', text: '处理中' }, 
        REQUESTED: { type: 'warning', text: '待审核' },
        IGNORED: { type: 'error', text: '已忽略' },
        SUBSCRIBED: { type: 'info', text: '已订阅' }, 
        PENDING_RELEASE: { type: 'error', text: '未上映' },
        NONE: { type: 'warning', text: '已取消' },
      };
      const s = statusMap[row.status] || { type: 'default', text: row.status }; // 如果有未知状态，直接显示
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

// 获取统计函数
const fetchStats = async () => {
  try {
    const res = await axios.get('/api/portal/subscription-stats');
    stats.value = res.data;
  } catch (e) {
    console.error("获取统计失败", e);
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
        status: filterStatus.value,
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

// 监听筛选变化 (放在 fetchSubscriptionHistory 定义之后)
import { watch } from 'vue';
watch(filterStatus, () => {
  fetchSubscriptionHistory(1); 
});

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
    fetchStats();
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
<style scoped>
/* ★★★ 修改布局样式为上下结构 ★★★ */
.profile-layout {
  display: flex;
  flex-direction: column; /* 垂直排列 */
  align-items: center;    /* 水平居中 */
  gap: 24px;              /* 上下间距 */
}

.profile-avatar-section {
  width: 100%;            /* 占满宽度 */
  display: flex;
  flex-direction: column;
  align-items: center;    /* 头像居中 */
  margin-bottom: 10px;
}

.profile-info {
  width: 100%;            /* 表格占满宽度 */
}

.username-text {
  margin-top: 16px;
  font-weight: bold;
  font-size: 1.4em;       /* 稍微加大字体 */
  text-align: center;
  word-break: break-all;
}

/* 头像包装器 */
.avatar-wrapper {
  position: relative;
  border-radius: 50%;     /* 建议：上下结构时圆形头像通常更好看，如果想保持方形可删掉此行 */
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

.avatar-wrapper:hover {
  transform: scale(1.05);
}

/* 悬浮遮罩 */
.avatar-overlay {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0, 0, 0, 0.4);
  display: flex;
  justify-content: center;
  align-items: center;
  opacity: 0;
  transition: opacity 0.2s;
  border-radius: 50%; /* 保持与 wrapper 一致，如果是方形请改为 0px */
}

/* 消除图片底部白边 */
.avatar-wrapper :deep(img) {
  display: block !important; 
  width: 100%;
  height: 100%;
  object-fit: cover; 
}

.avatar-wrapper:hover .avatar-overlay {
  opacity: 1;
}

</style>