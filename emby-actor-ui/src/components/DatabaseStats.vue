<!-- src/components/DatabaseStats.vue (最终版 - 补全历史日志) -->
<template>
 <n-layout content-style="padding: 24px;">
  <div>
    <!-- ★★★ 核心修改1: 增加“历史日志”按钮，并用按钮组美化 ★★★ -->
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
      <!-- 核心媒体库卡片 -->
      <n-gi span="4 m:2 l:1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">核心媒体库</span>
          </template>
          <n-space vertical size="large" align="center">
            <n-statistic label="已缓存媒体" class="centered-statistic">
              <span class="stat-value">{{ stats.media_library?.cached_total }}</span>
            </n-statistic>
            <n-divider />
            <n-space justify="space-around" style="width: 100%;">
              <n-statistic label="电影" class="centered-statistic">
                <template #prefix>
                  <n-icon-wrapper :size="20" :border-radius="5" color="#3366FF44">
                    <n-icon :size="14" :component="FilmIcon" color="#3366FF" />
                  </n-icon-wrapper>
                </template>
                {{ stats.media_library?.movies_in_library }}
              </n-statistic>
              <n-statistic label="剧集" class="centered-statistic">
                <template #prefix>
                  <n-icon-wrapper :size="20" :border-radius="5" color="#33CC9944">
                    <n-icon :size="14" :component="TvIcon" color="#33CC99" />
                  </n-icon-wrapper>
                </template>
                {{ stats.media_library?.series_in_library }}
              </n-statistic>
              <n-statistic label="预缓存" class="centered-statistic">
                <template #prefix>
                  <n-icon-wrapper :size="20" :border-radius="5" color="#FFCC3344">
                    <n-icon :size="14" :component="FolderOpenOutline" color="#FFCC33" />
                  </n-icon-wrapper>
                </template>
                {{ stats.media_library?.missing_total }}
              </n-statistic>
            </n-space>
          </n-space>
        </n-card>
      </n-gi>

      <!-- 合集管理卡片 -->
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

      <!-- 用户与邀请卡片 -->
      <n-gi span="4 m:2 l:1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">用户管理</span>
          </template>
          <n-space vertical size="large" align="center">
             <n-statistic label="Emby用户总数" class="centered-statistic" :value="stats.user_management_card?.emby_users_total" />
             <n-divider />
             <n-space justify="space-around" style="width: 100%;">
                <n-statistic label="已激活" :value="stats.user_management_card?.emby_users_active" />
                <n-statistic label="已禁用" :value="stats.user_management_card?.emby_users_disabled" />
             </n-space>
          </n-space>
        </n-card>
      </n-gi>

      <!-- 自动化维护卡片 -->
      <n-gi span="4 m:2 l:1">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">自动化维护</span>
          </template>
          <n-space vertical size="large" align="center">
            <n-statistic label="待处理清理任务" class="centered-statistic" :value="stats.maintenance_card?.cleanup_tasks_pending" />
            <n-divider />
            <n-statistic label="已启用的洗版规则" class="centered-statistic">
              <span class="stat-value">{{ stats.maintenance_card?.resubscribe_rules_enabled }}</span>
            </n-statistic>
          </n-space>
        </n-card>
      </n-gi>
      
      <!-- 订阅中心卡片 -->
      <n-gi span="4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">智能订阅</span>
          </template>
          <!-- ... (内部结构保持不变) ... -->
          <n-space vertical :size="24" class="subscription-center-card">
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

      <!-- 系统与缓存卡片 -->
      <n-gi span="4 l:2">
        <n-card :bordered="false" class="dashboard-card">
          <template #header>
            <span class="card-title">演员与缓存</span>
          </template>
          <!-- ★★★ 核心修改：使用6列网格并重新排布统计项 ★★★ -->
          <n-grid :x-gap="12" :y-gap="16" :cols="6" item-responsive>
            
            <!-- 新增：已关联映射 -->
            <n-gi span="3 s:2">
              <n-statistic label="已关联演员" class="centered-statistic" :value="stats.system?.actor_mappings_linked" />
            </n-gi>
            
            <!-- 新增：未关联映射 -->
            <n-gi span="3 s:2">
              <n-statistic label="未关联演员" class="centered-statistic" :value="stats.system?.actor_mappings_unlinked" />
            </n-gi>
            
            <!-- 翻译缓存 (调整布局) -->
            <n-gi span="6 s:2">
              <n-statistic label="翻译缓存" class="centered-statistic" :value="stats.system?.translation_cache_count" />
            </n-gi>
            
            <!-- 已处理日志 (调整布局) -->
            <n-gi span="3 s:3">
              <n-statistic label="已处理日志" class="centered-statistic" :value="stats.system?.processed_log_count" />
            </n-gi>
            
            <!-- 待复核日志 (调整布局) -->
            <n-gi span="3 s:3">
              <n-statistic label="待复核日志" class="centered-statistic" :value="stats.system?.failed_log_count" />
            </n-gi>

          </n-grid>
        </n-card>
      </n-gi>
    </n-grid>
    <!-- 实时日志查看器模态框 -->
    <n-modal v-model:show="isRealtimeLogVisible" preset="card" style="width: 80%; max-width: 900px;" title="实时任务日志">
       <n-log ref="logRef" :log="logContent" trim class="log-panel" style="height: 60vh;"/>
    </n-modal>

    <!-- ★★★ 核心修改2: 重新引入历史日志查看器组件 ★★★ -->
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
// ★★★ 核心修改3: 引入新图标和 LogViewer 组件 ★★★
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

// ★★★ 核心修改4: 使用两个独立的 ref 变量来控制不同的模态框 ★★★
const isRealtimeLogVisible = ref(false); // 控制实时日志模态框
const isHistoryLogVisible = ref(false);  // 控制历史日志模态框

const logContent = computed(() => props.taskStatus?.logs?.slice().reverse().join('\n') || '等待任务日志...');

// 当日志更新或实时日志模态框可见时，滚动到顶部
watch([() => props.taskStatus.logs, isRealtimeLogVisible], async ([, isVisible]) => {
  if (isVisible) {
    await nextTick();
    logRef.value?.scrollTo({ position: 'top', slient: true });
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