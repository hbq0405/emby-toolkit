<!-- src/components/ActorSubscriptions.vue (最终健壮版 - 无限滚动) -->
<template>
  <n-layout content-style="padding: 24px;">
    <div>
      <n-page-header>
        <template #title>
          演员订阅
        </template>
        <template #extra>
          <n-space>
            <n-button @click="handleManageDefaultConfig">
              <template #icon>
                <n-icon><settings-icon /></n-icon>
              </template>
              默认订阅配置
            </n-button>
            <n-button @click="handleRefreshSubscriptions">
              <template #icon>
                <n-icon><sync-icon /></n-icon>
              </template>
              刷新订阅状态
            </n-button>
            <n-button type="primary" @click="handleAddSubscription">
              <template #icon>
                <n-icon><add-icon /></n-icon>
              </template>
              添加演员订阅
            </n-button>
          </n-space>
        </template>
        <template #footer>
          <n-alert title="操作提示" type="info" style="margin-top: 24px;">
           <li>首先按需配置【默认订阅配置】，新增的演员订阅会套用默认订阅配置。添加演员后确认配置是自己想要的可以点击【手动刷新】获取演员参演的所有作品数据。</li>
           <li>刷新完毕，再次点开演员卡片，作品会按状态归入不同的标签页。所有被订阅配置规则排除的媒体项会归入【已忽略】标签页，点击【恢复】可重新追踪该作品。</li>
           <li>缺失标签页可以点击【TMDb】按钮查看作品详情决定是否订阅或忽略。</li>
          </n-alert>
        </template>
      </n-page-header>

      <div v-if="loading" style="text-align: center; margin-top: 50px;">
        <n-spin size="large" />
      </div>

      <div v-else-if="subscriptions.length === 0" style="margin-top: 20px;">
        <n-empty description="还没有订阅任何演员">
          <template #extra>
            <n-button size="small" @click="handleAddSubscription">
              去添加一个
            </n-button>
          </template>
        </n-empty>
      </div>

      <!-- ✨ [核心修改] 应用无限滚动 -->
      <div v-else>
        <n-grid :x-gap="12" :y-gap="12" cols="3 s:5 m:6 l:7 xl:8 xxl:9" responsive="screen" style="margin-top: 20px;">
          <n-gi v-for="sub in renderedSubscriptions" :key="sub.id">
            <n-card class="dashboard-card actor-card" @click="handleCardClick(sub)">
              <template #cover>
                <img :src="getImageUrl(sub.profile_path)" class="actor-avatar">
              </template>
              <div class="actor-name">{{ sub.actor_name }}</div>
              <div class="actor-status">
                <n-tag :type="sub.status === 'active' ? 'success' : 'warning'" size="small" round>
                  {{ sub.status === 'active' ? '订阅中' : '已暂停' }}
                </n-tag>
              </div>
            </n-card>
          </n-gi>
        </n-grid>
        
        <!-- ✨ [核心修改] 添加加载触发器 -->
        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>
      </div>

      <add-subscription-modal
        :show="showAddModal"
        @update:show="showAddModal = $event"
        @subscription-added="onSubscriptionChange"
      />

      <subscription-details-modal
        :show="showDetailsModal"
        :subscription-id="selectedSubId"
        @update:show="showDetailsModal = $event"
        @subscription-updated="onSubscriptionChange" 
        @subscription-deleted="onSubscriptionChange"
      />
      <default-config-modal
        :show="showDefaultConfigModal"
        @update:show="showDefaultConfigModal = $event"
      />
    </div>
  </n-layout>
</template>

<script setup>
// ✨ [核心修改] 引入 onBeforeUnmount 和 watch
import { ref, onMounted, onBeforeUnmount, watch, computed } from 'vue';
import { NPageHeader, NButton, NIcon, NText, NGrid, NGi, NCard, NSpin, NEmpty, NTag, useMessage, NSpace } from 'naive-ui';
import { Add as AddIcon, SyncOutline as SyncIcon, SettingsOutline as SettingsIcon } from '@vicons/ionicons5';
import axios from 'axios';
import AddSubscriptionModal from './modals/AddSubscriptionModal.vue';
import SubscriptionDetailsModal from './modals/SubscriptionDetailsModal.vue';
import DefaultConfigModal from './modals/DefaultConfigModal.vue';

const loading = ref(true);
const subscriptions = ref([]);
const showAddModal = ref(false);
const message = useMessage();
const showDetailsModal = ref(false); 
const selectedSubId = ref(null); 
const showDefaultConfigModal = ref(false);

// ✨ [核心修改] 无限滚动相关状态
const displayCount = ref(40); // 演员卡片小，可以多显示一些
const INCREMENT = 40;
const loaderRef = ref(null);
let observer = null;

const fetchSubscriptions = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/actor-subscriptions');
    subscriptions.value = response.data;
    displayCount.value = 40; // 重置
  } catch (error) {
    console.error("获取演员订阅列表失败:", error);
    message.error('获取订阅列表失败，请稍后重试。');
  } finally {
    loading.value = false;
  }
};

// ✨ [核心修改] 新的计算属性
const renderedSubscriptions = computed(() => {
  return subscriptions.value.slice(0, displayCount.value);
});

const hasMore = computed(() => {
  return displayCount.value < subscriptions.value.length;
});

const loadMore = () => {
  if (hasMore.value) {
    displayCount.value += INCREMENT;
  }
};

const handleRefreshSubscriptions = async () => {
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'actor-tracking' });
    message.success(response.data.message || '演员订阅刷新任务已成功提交到后台！');
  } catch (error) {
    console.error("触发演员订阅刷新失败:", error);
    const errorMsg = error.response?.data?.message || error.response?.data?.error || '启动刷新任务失败，请检查后台或稍后再试。';
    message.error(errorMsg);
  }
};

const getImageUrl = (path) => {
  if (!path) {
    return 'https://via.placeholder.com/185x278.png?text=No+Image';
  }
  return `https://image.tmdb.org/t/p/w185${path}`;
};

const handleAddSubscription = () => {
  showAddModal.value = true;
};

const handleManageDefaultConfig = () => {
  showDefaultConfigModal.value = true;
};

const handleCardClick = (subscription) => {
  selectedSubId.value = subscription.id;
  showDetailsModal.value = true;
};

const onSubscriptionChange = () => {
  fetchSubscriptions(); 
};

// ✨ [核心修改] 设置和清理 Observer
onMounted(() => {
  fetchSubscriptions();
  observer = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting) {
        loadMore();
      }
    },
    { threshold: 1.0 }
  );
  if (loaderRef.value) {
    observer.observe(loaderRef.value);
  }
});

onBeforeUnmount(() => {
  if (observer) {
    observer.disconnect();
  }
});

watch(loaderRef, (newEl) => {
  if (observer && newEl) {
    observer.observe(newEl);
  }
});
</script>

<style scoped>
.actor-card {
  cursor: pointer;
  text-align: center;
  overflow: hidden;
}
.actor-avatar {
  width: 100%;
  height: auto;
  aspect-ratio: 2 / 3;
  object-fit: cover;
  background-color: #f0f2f5;
}
.dark-mode .actor-avatar {
  background-color: #2c2c32;
}
.actor-name {
  font-weight: 600;
  margin-top: 8px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.actor-status {
  margin-top: 4px;
  font-size: 12px;
}
/* ✨ [核心修改] 加载触发器样式 */
.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}
</style>