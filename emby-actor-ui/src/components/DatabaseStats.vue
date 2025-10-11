<!-- src/components/DatabaseStats.vue (最终修复版 - 正序日志) -->
<template>
 <n-layout content-style="padding: 24px;">
  <div>
    <n-page-header title="数据看板" subtitle="了解您媒体库的核心数据统计" style="margin-bottom: 24px;">
      <template #extra>
        <n-button-group>
          <n-button @click="isRealtimeLogVisible = true">
            <template #icon><n-icon :component="ReaderOutline" /></template>
            实时日志
          </n-button>
          <n-button @click="isHistoryLogVisible = true">
            <template #icon><n-icon :component="ArchiveOutline" /></template>
            历史日志
          </n-button>
        </n-button-group>
      </template>
    </n-page-header>
    
    <div v-if="loading" class="loading-container">
      <n-spin size="large" />
      <p>正在加载统计数据...</p>
    </div>

    <div v-else-if="error" class="error-container">
      <n-alert title="加载失败" type="error">{{ error }}</n-alert>
    </div>

    <n-grid v-else :x-gap="24" :y-gap="24" :cols="4" responsive="screen" item-responsive>
      
      <!-- 核心卡片 1: 核心数据缓存 -->
      <n-gi span="4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">核心数据</span>
          </template>
          <n-space vertical :size="20">
            <!-- 顶部关键指标 -->
            <n-grid :cols="2" :x-gap="12">
              <n-gi>
                <n-statistic label="已缓存媒体" class="centered-statistic">
                  <span class="stat-value">{{ stats.media_library?.cached_total || 0 }}</span>
                </n-statistic>
              </n-gi>
              <n-gi>
                <n-statistic label="已归档演员" class="centered-statistic">
                  <span class="stat-value">{{ (stats.system?.actor_mappings_total || 0) }}</span>
                </n-statistic>
              </n-gi>
            </n-grid>

            <n-divider />

            <!-- 媒体细分 -->
            <div>
              <div class="section-title">媒体</div>
              <n-space justify="space-around" style="width: 100%; margin-top: 12px;">
                <n-statistic label="电影" class="centered-statistic">
                  <template #prefix>
                    <n-icon-wrapper :size="20" :border-radius="5" color="#3366FF44">
                      <n-icon :size="14" :component="FilmIcon" color="#3366FF" />
                    </n-icon-wrapper>
                  </template>
                  {{ stats.media_library?.movies_in_library || 0 }}
                </n-statistic>
                <n-statistic label="剧集" class="centered-statistic">
                  <template #prefix>
                    <n-icon-wrapper :size="20" :border-radius="5" color="#33CC9944">
                      <n-icon :size="14" :component="TvIcon" color="#33CC99" />
                    </n-icon-wrapper>
                  </template>
                  {{ stats.media_library?.series_in_library || 0 }}
                </n-statistic>
                <n-statistic label="预缓存" class="centered-statistic">
                  <template #prefix>
                    <n-icon-wrapper :size="20" :border-radius="5" color="#FFCC3344">
                      <n-icon :size="14" :component="FolderOpenOutline" color="#FFCC33" />
                    </n-icon-wrapper>
                  </template>
                  {{ stats.media_library?.missing_total || 0 }}
                </n-statistic>
              </n-space>
            </div>

            <!-- 演员细分 -->
            <div>
              <div class="section-title">演员</div>
              <n-space justify="space-around" style="width: 100%; margin-top: 12px;">
                <n-statistic label="已关联" class="centered-statistic" :value="stats.system?.actor_mappings_linked || 0" />
                <n-statistic label="未关联" class="centered-statistic" :value="stats.system?.actor_mappings_unlinked || 0" />
              </n-space>
            </div>

            <n-divider />

            <!-- 系统日志与缓存 -->
            <div>
              <div class="section-title">系统日志与缓存</div>
              <n-space justify="space-around" style="width: 100%; margin-top: 12px;">
                <n-statistic label="翻译缓存" class="centered-statistic" :value="stats.system?.translation_cache_count || 0" />
                <n-statistic label="已处理" class="centered-statistic" :value="stats.system?.processed_log_count || 0" />
                <n-statistic label="待复核" class="centered-statistic" :value="stats.system?.failed_log_count || 0" />
              </n-space>
            </div>
          </n-space>
        </n-card>
      </n-gi>
      
      <!-- 核心卡片 2: 智能订阅 -->
      <n-gi span="4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">智能订阅</span>
          </template>
          <n-space vertical :size="24" class="subscription-center-card">
            <div class="section-container">
              <div class="section-title">媒体追踪</div>
              <n-grid :cols="2" :x-gap="12">
                <n-gi class="stat-block">
                  <div class="stat-block-title">追剧订阅</div>
                  <div class="stat-item-group">
                    <div class="stat-item">
                      <div class="stat-item-label">追剧中</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.watchlist.watching || 0 }}</div>
                    </div>
                    <div class="stat-item">
                      <div class="stat-item-label">已暂停</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.watchlist.paused || 0 }}</div>
                    </div>
                  </div>
                </n-gi>
                <n-gi class="stat-block">
                  <div class="stat-block-title">演员订阅</div>
                  <div class="stat-item-group">
                    <div class="stat-item">
                      <div class="stat-item-label">已订阅</div>
                      <div class="stat-item-value">{{ stats.subscriptions_card?.actors.subscriptions || 0 }}</div>
                    </div>
                    <div class="stat-item">
                      <div class="stat-item-label">作品入库</div>
                      <div class="stat-item-value">
                        {{ stats.subscriptions_card?.actors.tracked_in_library || 0 }} / {{ stats.subscriptions_card?.actors.tracked_total || 0 }}
                      </div>
                    </div>
                  </div>
                </n-gi>
              </n-grid>
            </div>
            <div class="section-container">
              <div class="section-title">自动化订阅</div>
              <n-grid :cols="2" :x-gap="12">
                <n-gi class="stat-block">
                  <div class="stat-block-title">洗版任务</div>
                  <div class="stat-item">
                    <div class="stat-item-label">待洗版</div>
                    <div class="stat-item-value">{{ stats.subscriptions_card?.resubscribe.pending || 0 }}</div>
                  </div>
                </n-gi>
                <n-gi class="stat-block">
                  <div class="stat-block-title">合集补全</div>
                  <div class="stat-item">
                    <div class="stat-item-label">待补全合集</div>
                    <div class="stat-item-value">{{ stats.subscriptions_card?.collections.with_missing || 0 }}</div>
                  </div>
                </n-gi>
              </n-grid>
            </div>
            <n-divider />
            <n-grid :cols="3" :x-gap="12" class="quota-grid">
              <n-gi class="quota-label-container">
                <span>订阅配额</span>
              </n-gi>
              <n-gi class="stat-block">
                <div class="stat-item">
                  <div class="stat-item-label">今日已用</div>
                  <div class="stat-item-value">{{ stats.subscriptions_card?.quota.consumed || 0 }}</div>
                </div>
              </n-gi>
              <n-gi class="stat-block">
                <div class="stat-item">
                  <div class="stat-item-label">今日剩余</div>
                  <div class="stat-item-value">{{ stats.subscriptions_card?.quota.available || 0 }}</div>
                </div>
              </n-gi>
            </n-grid>
          </n-space>
        </n-card>
      </n-gi>

    </n-grid>
    <!-- 实时日志查看器模态框 -->
    <n-modal v-model:show="isRealtimeLogVisible" preset="card" style="width: 80%; max-width: 900px;" title="实时任务日志" class="modal-card-lite">
       <n-log ref="logRef" :log="logContent" trim class="log-panel" style="height: 60vh;"/>
    </n-modal>

    <!-- 历史日志查看器组件 -->
    <LogViewer v-model:show="isHistoryLogVisible" />
  </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, computed, watch, nextTick } from 'vue';
import axios from 'axios';
import { 
  NPageHeader, NGrid, NGi, NCard, NStatistic, NSpin, NAlert, NIcon, NSpace, NDivider, NIconWrapper,
  NLog, NButton, NModal, NButtonGroup
} from 'naive-ui';
import { 
  FilmOutline as FilmIcon, 
  TvOutline as TvIcon, 
  FolderOpenOutline, 
  ReaderOutline, 
  ArchiveOutline 
} from '@vicons/ionicons5';
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

const isRealtimeLogVisible = ref(false);
const isHistoryLogVisible = ref(false);

// ★★★ 核心修改 1/2: 移除 .reverse()，让日志正序显示 ★★★
const logContent = computed(() => props.taskStatus?.logs?.join('\n') || '等待任务日志...');

// ★★★ 核心修改 2/2: 将滚动位置从 'top' 改为 'bottom' ★★★
// 当日志更新或实时日志模态框可见时，滚动到底部以查看最新日志
watch([() => props.taskStatus.logs, isRealtimeLogVisible], async ([, isVisible]) => {
  if (isVisible) {
    await nextTick();
    // 滚动到底部，而不是顶部
    logRef.value?.scrollTo({ position: 'bottom', slient: true });
  }
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
/* ... (样式部分保持不变) ... */
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
.log-panel {
  font-size: 13px;
  line-height: 1.6;
  background-color: transparent;
}
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