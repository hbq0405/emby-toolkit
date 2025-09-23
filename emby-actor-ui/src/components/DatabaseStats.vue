<!-- src/components/DatabaseStats.vue (精装修终版) -->
<template>
 <n-layout content-style="padding: 24px;">
  <div>
    <n-page-header class="card-title" title="数据看板" subtitle="了解您媒体库的核心数据统计" style="margin-bottom: 24px;"></n-page-header>
    
    <div v-if="loading" class="loading-container">
      <n-spin size="large" />
      <p>正在加载统计数据...</p>
    </div>

    <div v-else-if="error" class="error-container">
      <n-alert title="加载失败" type="error">{{ error }}</n-alert>
    </div>

    <n-grid v-else :x-gap="24" :y-gap="24" :cols="4" responsive="screen" item-responsive>
      <!-- 卡片1: 核心媒体库 -->
      <n-gi span="4 m:2 l:1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">核心媒体库</span>
          </template>
          <n-space vertical size="large" align="center">
            <n-statistic label="已索引媒体总数" class="centered-statistic">
              <span class="stat-value">{{ stats.media_metadata?.total }}</span>
            </n-statistic>
            <n-divider />
            <n-space justify="space-around" style="width: 100%;">
              <n-statistic label="电影" class="centered-statistic">
                <template #prefix>
                  <n-icon-wrapper :size="20" :border-radius="5" color="#3366FF44">
                    <n-icon :size="14" :component="FilmIcon" color="#3366FF" />
                  </n-icon-wrapper>
                </template>
                {{ stats.media_metadata?.movies }}
              </n-statistic>
              <n-statistic label="剧集" class="centered-statistic">
                <template #prefix>
                  <n-icon-wrapper :size="20" :border-radius="5" color="#33CC9944">
                    <n-icon :size="14" :component="TvIcon" color="#33CC99" />
                  </n-icon-wrapper>
                </template>
                {{ stats.media_metadata?.series }}
              </n-statistic>
            </n-space>
          </n-space>
        </n-card>
      </n-gi>

      <!-- 卡片2: 合集管理 -->
      <n-gi span="4 m:2 l:1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">合集管理</span>
          </template>
          <n-space vertical size="large" align="center">
            <n-statistic label="已识别TMDB合集" class="centered-statistic" :value="stats.collections_card?.total_tmdb_collections" />
            <n-divider />
            <n-statistic label="活跃的自建合集" class="centered-statistic">
              <span class="stat-value">{{ stats.collections_card?.total_custom_collections }}</span>
            </n-statistic>
          </n-space>
        </n-card>
      </n-gi>
      
      <!-- ★★★ 卡片3: 订阅中心 (全新精装修布局) ★★★ -->
      <n-gi span="4 m:4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">订阅中心</span>
          </template>
          <n-space vertical :size="24" class="subscription-center-card">
            <!-- 分组1: 媒体追踪 -->
            <div class="section-container">
              <div class="section-title">媒体追踪</div>
              <n-grid :cols="2" :x-gap="12">
                <n-gi class="stat-block">
                  <div class="stat-block-title">追剧订阅</div>
                  <div class="stat-item-group">
                    <div class="stat-item">
                      <div class="stat-item-label">追剧中</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.watchlist.watching }}</div>
                    </div>
                    <div class="stat-item">
                      <div class="stat-item-label">已暂停</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.watchlist.paused }}</div>
                    </div>
                  </div>
                </n-gi>
                <n-gi class="stat-block">
                  <div class="stat-block-title">演员订阅</div>
                  <div class="stat-item-group">
                    <div class="stat-item">
                      <div class="stat-item-label">已订阅</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.actors.subscriptions }}</div>
                    </div>
                    <div class="stat-item">
                      <div class="stat-item-label">作品入库</div>
                      <div class="stat-item-value">
                        {{ stats.subscriptions_card?.actors.tracked_in_library }} / {{ stats.subscriptions_card?.actors.tracked_total }}
                      </div>
                    </div>
                  </div>
                </n-gi>
              </n-grid>
            </div>

            <!-- 分组2: 自动化订阅 -->
            <div class="section-container">
              <div class="section-title">自动化订阅</div>
              <n-grid :cols="2" :x-gap="12">
                <n-gi class="stat-block">
                  <div class="stat-block-title">洗版任务</div>
                  <div class="stat-item">
                    <div class="stat-item-label">待洗版</div>
                    <div class="stat-item-value">{{ stats.subscriptions_card?.resubscribe.pending }}</div>
                  </div>
                </n-gi>
                <n-gi class="stat-block">
                  <div class="stat-block-title">合集补全</div>
                  <div class="stat-item">
                    <div class="stat-item-label">待补全合集</div>
                    <div class="stat-item-value">{{ stats.subscriptions_card?.collections.with_missing }}</div>
                  </div>
                </n-gi>
              </n-grid>
            </div>

            <n-divider />

            <!-- 分组3: 今日配额 -->
            <n-grid :cols="3" :x-gap="12" class="quota-grid">
              <n-gi class="quota-label-container">
                <span>订阅配额</span>
              </n-gi>
              <n-gi class="stat-block">
                <div class="stat-item">
                  <div class="stat-item-label">今日已用</div>
                  <div class="stat-item-value">{{ stats.subscriptions_card?.quota.consumed }}</div>
                </div>
              </n-gi>
              <n-gi class="stat-block">
                <div class="stat-item">
                  <div class="stat-item-label">今日剩余</div>
                  <div class="stat-item-value">{{ stats.subscriptions_card?.quota.available }}</div>
                </div>
              </n-gi>
            </n-grid>
          </n-space>
        </n-card>
      </n-gi>

      <!-- 卡片4: 系统与缓存 -->
      <n-gi span="4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">系统与缓存</span>
          </template>
          <n-grid :x-gap="12" :y-gap="16" :cols="4" item-responsive>
            <n-gi span="2 s:1">
              <n-statistic label="演员映射条目" class="centered-statistic" :value="stats.system?.actor_mappings_count" />
            </n-gi>
            <n-gi span="2 s:1">
              <n-statistic label="翻译缓存条目" class="centered-statistic" :value="stats.system?.translation_cache_count" />
            </n-gi>
            <n-gi span="2 s:1">
              <n-statistic label="已处理" class="centered-statistic" :value="stats.system?.processed_log_count" />
            </n-gi>
            <n-gi span="2 s:1">
              <n-statistic label="待复核" class="centered-statistic" :value="stats.system?.failed_log_count" />
            </n-gi>
          </n-grid>
        </n-card>
      </n-gi>

      <!-- 卡片5: 实时日志 -->
      <n-gi span="4 l:2">
        <n-card 
          :bordered="false" 
          class="content-card dashboard-card"
          style="display: flex; flex-direction: column; height: 100%;" 
          content-style="flex-grow: 1; display: flex; flex-direction: column; padding: 0 24px 24px 24px;"
          header-style="padding-bottom: 12px;"
        >
        <template #header>
            <span class="card-title">实时日志</span>
          </template>
          <template #header-extra>
            <n-button text @click="isLogViewerVisible = true" title="查看历史归档日志">
              <template #icon><n-icon :component="DocumentTextOutline" /></template>
              历史日志
            </n-button>
          </template>
          <n-log ref="logRef" :log="logContent" trim class="log-panel" style="flex-grow: 1;"/>
        </n-card>
      </n-gi>
    </n-grid>

    <!-- 历史日志查看器模态框 -->
    <LogViewer v-model:show="isLogViewerVisible" />
  </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, computed, watch, nextTick } from 'vue';
import axios from 'axios';
import { 
  NPageHeader, NGrid, NGi, NCard, NStatistic, NSpin, NAlert, NIcon, NSpace, NDivider, NIconWrapper,
  NLog, NButton
} from 'naive-ui';
import { FilmOutline as FilmIcon, TvOutline as TvIcon, DocumentTextOutline } from '@vicons/ionicons5';
import LogViewer from './LogViewer.vue';

const props = defineProps({
  taskStatus: {
    type: Object,
    required: true,
    default: () => ({
      is_running: false,
      current_action: '空闲',
      logs: []
    })
  }
});

const loading = ref(true);
const error = ref(null);
const stats = ref({});
const logRef = ref(null);
const isLogViewerVisible = ref(false);

const logContent = computed(() => props.taskStatus?.logs?.slice().reverse().join('\n') || '等待任务日志...');

watch(() => props.taskStatus.logs, async () => {
  await nextTick();
  logRef.value?.scrollTo({ position: 'top', slient: true });
}, { deep: true });

const fetchStats = async () => {
  loading.value = true;
  error.value = null;
  try {
    const response = await axios.get('/api/database/stats');
    if (response.data.status === 'success') {
      stats.value = response.data.data;
    } else {
      throw new Error(response.data.message || '获取统计数据失败');
    }
  } catch (e) {
    console.error('获取数据库统计失败:', e);
    error.value = e.message || '请求失败，请检查网络或联系管理员。';
  } finally {
    loading.value = false;
  }
};

onMounted(() => {
  fetchStats();
});
</script>

<style scoped>
.loading-container, .error-container {
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  height: 400px;
}
.centered-statistic {
  text-align: center;
}
.stat-value {
  font-size: 1.8em;
  font-weight: 600;
  line-height: 1.2;
}
.content-card {
  height: 100%;
}
.log-panel {
  font-size: 13px;
  line-height: 1.6;
  background-color: transparent;
}

/* --- 全新订阅中心样式 --- */
.subscription-center-card {
  width: 100%;
}
.section-container {
  width: 100%;
}
.section-title {
  font-size: 16px;
  font-weight: 600;
  color: var(--n-title-text-color);
  margin-bottom: 16px;
}
.stat-block {
  text-align: center;
}
.stat-block-title {
  font-size: 14px;
  color: var(--n-text-color-2);
  margin-bottom: 12px;
}
.stat-item-group {
  display: flex;
  justify-content: center;
  gap: 32px;
}
.stat-item {
  text-align: center;
}
.stat-item-label {
  font-size: 13px;
  color: var(--n-text-color-3);
  margin-bottom: 4px;
}
.stat-item-value {
  font-size: 24px;
  font-weight: 600;
  line-height: 1.1;
  color: var(--n-statistic-value-text-color);
}
.quota-grid {
  align-items: center;
}
.quota-label-container {
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 600;
  font-size: 14px;
  color: var(--n-text-color-2);
}
</style>