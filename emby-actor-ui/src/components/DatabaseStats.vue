<!-- src/components/DatabaseStats.vue -->
<template>
  <!-- ================================================================================== -->
  <!-- 视图 A: PC 端 (宽屏) - 追求信息密度、整齐对齐、图表丰富 -->
  <!-- ================================================================================== -->
  <n-layout v-if="!isMobile" content-style="padding: 24px;">
    <div>
      <n-page-header title="数据看板" subtitle="了解您媒体库的核心数据统计" style="margin-bottom: 24px;">
      </n-page-header>
      
      <n-grid :x-gap="24" :y-gap="24" :cols="4">
        
        <!-- 左侧核心数据卡片 -->
        <n-gi :span="2">
          <n-card :bordered="false" class="dashboard-card">
            <template #header>
              <span class="card-title">核心数据</span>
              <n-spin v-if="loading.core || loading.library || loading.system" size="small" style="float: right" />
            </template>
            <n-space vertical :size="20">
              <!-- 顶部关键指标 -->
              <n-grid :cols="2" :x-gap="12">
                <n-gi>
                  <n-statistic label="已缓存媒体" class="centered-statistic">
                    <span class="stat-value">{{ stats.media_library.cached_total }}</span>
                  </n-statistic>
                </n-gi>
                <n-gi>
                  <n-statistic label="已归档演员" class="centered-statistic">
                    <span class="stat-value">{{ stats.system.actor_mappings_total }}</span>
                  </n-statistic>
                </n-gi>
              </n-grid>
  
              <n-divider />
  
              <!-- 媒体库概览 -->
              <div>
                <div class="section-title">媒体库概览</div>
                <n-grid :cols="2" :x-gap="24" style="margin-top: 12px; align-items: center;">
                  <n-gi>
                    <v-chart class="chart" :option="resolutionChartOptions" autoresize style="height: 180px;" />
                  </n-gi>
                  <n-gi>
                    <n-space vertical justify="center" style="height: 100%;">
                      <n-grid :cols="2" :x-gap="12" :y-gap="16">
                        <n-gi><n-statistic label="电影" :value="stats.media_library.movies_in_library" /></n-gi>
                        <n-gi><n-statistic label="剧集" :value="stats.media_library.series_in_library" /></n-gi>
                        <n-gi><n-statistic label="总集数" :value="stats.media_library.episodes_in_library" /></n-gi>
                        <n-gi>
                          <n-statistic label="演员">
                            <template #prefix>
                              <n-icon-wrapper :size="20" :border-radius="5" color="#FFCC3344">
                                <n-icon :size="14" :component="FolderOpenOutline" color="#FFCC33" />
                              </n-icon-wrapper>
                            </template>
                            {{ stats.system.actor_mappings_linked }}
                          </n-statistic>
                        </n-gi>
                      </n-grid>
                    </n-space>
                  </n-gi>
                </n-grid>
              </div>
              <n-divider />
  
              <!-- 系统日志与缓存 -->
              <div>
                <div class="section-title">系统日志与缓存</div>
                <n-space justify="space-around" style="width: 100%; margin-top: 12px;">
                  <n-statistic label="翻译缓存" class="centered-statistic" :value="stats.system.translation_cache_count" />
                  <n-statistic label="已处理" class="centered-statistic" :value="stats.system.processed_log_count" />
                  <n-statistic label="待复核" class="centered-statistic" :value="stats.system.failed_log_count" />
                </n-space>
              </div>
            </n-space>
          </n-card>
        </n-gi>
        
        <!-- 右侧智能订阅卡片 -->
        <n-gi :span="2">
          <n-card :bordered="false" class="dashboard-card">
            <template #header>
              <span class="card-title">智能订阅</span>
              <n-spin v-if="loading.subscription || loading.rankings" size="small" style="float: right" />
            </template>
            <n-space vertical :size="24" class="subscription-center-card">
              
              <div class="section-container">
                <div class="section-title">媒体追踪</div>
                <n-grid :cols="2" :x-gap="12">
                  <n-gi class="stat-block">
                    <div class="stat-block-title">追剧订阅</div>
                    <div class="stat-item-group" style="gap: 16px; justify-content: space-around;">
                      <div class="stat-item">
                        <div class="stat-item-label">追剧中</div>
                        <div class="stat-item-value" style="color: var(--n-primary-color);">
                          {{ stats.subscriptions_card.watchlist.watching }}
                        </div>
                      </div>
                      <div class="stat-item">
                        <div class="stat-item-label">已暂停</div>
                        <div class="stat-item-value" style="color: var(--n-warning-color);">
                          {{ stats.subscriptions_card.watchlist.paused }}
                        </div>
                      </div>
                      <div class="stat-item">
                        <div class="stat-item-label">已完结</div>
                        <div class="stat-item-value" style="color: var(--n-success-color);">
                          {{ stats.subscriptions_card.watchlist.completed }}
                        </div>
                      </div>
                    </div>
                  </n-gi>
                  <n-gi class="stat-block">
                    <div class="stat-block-title">演员订阅</div>
                    <div class="stat-item-group">
                      <div class="stat-item"><div class="stat-item-label">已订阅</div><div class="stat-item-value">{{ stats.subscriptions_card.actors.subscriptions }}</div></div>
                      <div class="stat-item"><div class="stat-item-label">作品入库</div><div class="stat-item-value">{{ stats.subscriptions_card.actors.tracked_in_library }}</div></div>
                    </div>
                  </n-gi>
                </n-grid>
              </div>
              <div class="section-container">
               <div class="section-title">自动化订阅</div>
                <n-grid :cols="3" :x-gap="12">
                  <n-gi class="stat-block">
                    <div class="stat-block-title">洗版任务</div>
                    <div class="stat-item"><div class="stat-item-label">待洗版</div><div class="stat-item-value">{{ stats.subscriptions_card.resubscribe.pending }}</div></div>
                  </n-gi>
                  <n-gi class="stat-block">
                    <div class="stat-block-title">原生合集</div>
                    <div class="stat-item-group">
                      <div class="stat-item"><div class="stat-item-label">总数</div><div class="stat-item-value">{{ stats.subscriptions_card.native_collections.total }}</div></div>
                      <div class="stat-item"><div class="stat-item-label">待补全</div><div class="stat-item-value">{{ stats.subscriptions_card.native_collections.count }}</div></div>
                      <div class="stat-item"><div class="stat-item-label">共缺失</div><div class="stat-item-value">{{ stats.subscriptions_card.native_collections.missing_items }}</div></div>
                    </div>
                  </n-gi>
                  <n-gi class="stat-block">
                    <div class="stat-block-title">自建合集</div>
                    <div class="stat-item-group">
                      <div class="stat-item"><div class="stat-item-label">总数</div><div class="stat-item-value">{{ stats.subscriptions_card.custom_collections.total }}</div></div>
                      <div class="stat-item"><div class="stat-item-label">待补全</div><div class="stat-item-value">{{ stats.subscriptions_card.custom_collections.count }}</div></div>
                      <div class="stat-item"><div class="stat-item-label">共缺失</div><div class="stat-item-value">{{ stats.subscriptions_card.custom_collections.missing_items }}</div></div>
                    </div>
                  </n-gi>
                </n-grid>
              </div>
              <n-divider />
              <n-grid :cols="3" :x-gap="12" class="quota-grid">
                <n-gi class="quota-label-container"><span>订阅配额</span></n-gi>
                <n-gi class="stat-block"><div class="stat-item"><div class="stat-item-label">今日已用</div><div class="stat-item-value">{{ stats.subscriptions_card.quota.consumed }}</div></div></n-gi>
                <n-gi class="stat-block"><div class="stat-item"><div class="stat-item-label">今日剩余</div><div class="stat-item-value">{{ stats.subscriptions_card.quota.available }}</div></div></n-gi>
              </n-grid>
              <n-divider />
  
              <!-- 发布组统计区 -->
              <div class="section-container">
                <n-grid :cols="2" :x-gap="24">
                  <!-- 左列：今日排行 -->
                  <n-gi>
                    <div class="section-title">今日发布组 (Top {{ stats.release_group_ranking.length }})</div>
                    <n-space vertical :size="12" style="width: 100%;">
                      <div v-if="stats.release_group_ranking.length === 0">
                        <n-empty description="今日暂无入库" />
                      </div>
                      <div v-else v-for="(group, index) in stats.release_group_ranking" :key="group.release_group" class="ranking-item">
                        <span class="ranking-index">{{ index + 1 }}</span>
                        <img 
                          :src="getIconPath(group.release_group)" 
                          class="site-icon"
                          @error="handleIconError"
                        />
                        <span class="ranking-name pc-ranking-name">{{ group.release_group }}</span>
                        <span class="ranking-count">{{ group.count }} 部</span>
                        <n-progress
                          type="line"
                          :percentage="(group.count / (stats.release_group_ranking[0]?.count || 1)) * 100"
                          :show-indicator="false"
                          :height="8"
                          style="flex-grow: 1; margin: 0 12px;"
                          :color="themeVars.primaryColor"
                        />
                      </div>
                    </n-space>
                  </n-gi>
  
                  <!-- 右列：历史排行 -->
                  <n-gi>
                    <div class="section-title">历史发布组 (Top {{ stats.historical_release_group_ranking.length }})</div>
                    <n-space vertical :size="12" style="width: 100%;">
                      <div v-if="stats.historical_release_group_ranking.length === 0">
                        <n-empty description="暂无历史数据" />
                      </div>
                      <div v-else v-for="(group, index) in stats.historical_release_group_ranking" :key="group.release_group" class="ranking-item">
                        <span class="ranking-index">{{ index + 1 }}</span>
                        <img 
                          :src="getIconPath(group.release_group)" 
                          class="site-icon"
                          @error="handleIconError"
                        />
                        <span class="ranking-name pc-ranking-name">{{ group.release_group }}</span>
                        <span class="ranking-count">{{ group.count }} 部</span>
                        <n-progress
                          type="line"
                          :percentage="(group.count / (stats.historical_release_group_ranking[0]?.count || 1)) * 100"
                          :show-indicator="false"
                          :height="8"
                          style="flex-grow: 1; margin: 0 12px;"
                          :color="themeVars.primaryColor"
                        />
                      </div>
                    </n-space>
                  </n-gi>
                </n-grid>
              </div>
            </n-space>
          </n-card>
        </n-gi>
  
      </n-grid>
    </div>
  </n-layout>

  <!-- ================================================================================== -->
  <!-- 视图 B: Mobile 端 (手机) - 追求垂直流、卡片化、大触控区 -->
  <!-- ================================================================================== -->
  <n-layout v-else content-style="padding: 12px; background-color: transparent;">
    <div class="mobile-container">
      <!-- 1. 核心数据概览 (小卡片并排) -->
      <n-grid :cols="2" :x-gap="12" :y-gap="12">
        <n-gi>
          <n-card size="small" :bordered="false" class="mobile-stat-card">
            <n-statistic label="已缓存媒体">
              <span class="mobile-stat-value">{{ stats.media_library.cached_total }}</span>
            </n-statistic>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card size="small" :bordered="false" class="mobile-stat-card">
            <n-statistic label="已归档演员">
              <span class="mobile-stat-value">{{ stats.system.actor_mappings_total }}</span>
            </n-statistic>
          </n-card>
        </n-gi>
      </n-grid>

      <!-- 2. 媒体库分布 (图表) -->
      <n-card size="small" :bordered="false" title="媒体库分布" style="margin-top: 12px;">
        <v-chart class="chart" :option="resolutionChartOptions" autoresize style="height: 200px;" />
        <n-grid :cols="3" style="text-align: center; margin-top: 10px;">
          <n-gi><n-statistic label="电影" :value="stats.media_library.movies_in_library" /></n-gi>
          <n-gi><n-statistic label="剧集" :value="stats.media_library.series_in_library" /></n-gi>
          <n-gi><n-statistic label="总集数" :value="stats.media_library.episodes_in_library" /></n-gi>
        </n-grid>
      </n-card>

      <!-- 3. 追剧状态 -->
      <n-card size="small" :bordered="false" title="追剧状态" style="margin-top: 12px;">
        <div style="display: flex; justify-content: space-between; text-align: center;">
          <div>
            <div class="mobile-label">追剧中</div>
            <div class="mobile-value" style="color: var(--n-primary-color);">{{ stats.subscriptions_card.watchlist.watching }}</div>
          </div>
          <div>
            <div class="mobile-label">已暂停</div>
            <div class="mobile-value" style="color: var(--n-warning-color);">{{ stats.subscriptions_card.watchlist.paused }}</div>
          </div>
          <div>
            <div class="mobile-label">已完结</div>
            <div class="mobile-value" style="color: var(--n-success-color);">{{ stats.subscriptions_card.watchlist.completed }}</div>
          </div>
        </div>
      </n-card>

      <!-- 4. 自动化任务 (列表式) -->
      <n-card size="small" :bordered="false" title="自动化任务" style="margin-top: 12px;">
        <n-space vertical>
          <div class="mobile-row">
            <span>洗版任务</span>
            <n-tag size="small" type="info">{{ stats.subscriptions_card.resubscribe.pending }} 待处理</n-tag>
          </div>
          <div class="mobile-row">
            <span>原生合集</span>
            <span>{{ stats.subscriptions_card.native_collections.count }} 待补 / {{ stats.subscriptions_card.native_collections.total }} 总</span>
          </div>
          <div class="mobile-row">
            <span>自建合集</span>
            <span>{{ stats.subscriptions_card.custom_collections.count }} 待补 / {{ stats.subscriptions_card.custom_collections.total }} 总</span>
          </div>
          <n-divider style="margin: 8px 0" />
          <div class="mobile-row">
            <span>今日配额</span>
            <span>{{ stats.subscriptions_card.quota.consumed }} 用 / {{ stats.subscriptions_card.quota.available }} 余</span>
          </div>
        </n-space>
      </n-card>

      <!-- 5. 今日发布组 (简化列表) -->
      <n-card size="small" :bordered="false" title="今日发布组 Top 5" style="margin-top: 12px;">
        <div v-if="stats.release_group_ranking.length === 0">
          <n-empty description="今日暂无" size="small" />
        </div>
        <div v-else v-for="(group, index) in stats.release_group_ranking.slice(0, 5)" :key="group.release_group" class="ranking-item" style="margin-bottom: 8px;">
          <span class="ranking-index">{{ index + 1 }}</span>
          <img :src="getIconPath(group.release_group)" class="site-icon" @error="handleIconError" />
          <span class="ranking-name mobile-ranking-name">{{ group.release_group }}</span>
          <span class="ranking-count">{{ group.count }}</span>
        </div>
      </n-card>

    </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, reactive } from 'vue';
import axios from 'axios';
import { 
  NPageHeader, NLayout, NGrid, NGi, NCard, NStatistic, NSpin, NIcon, NSpace, NDivider, NIconWrapper,
  NProgress, NEmpty, useThemeVars, NTag
} from 'naive-ui';
import { FolderOpenOutline } from '@vicons/ionicons5';
import { use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { PieChart } from 'echarts/charts';
import { TitleComponent, TooltipComponent, LegendComponent } from 'echarts/components';
import VChart from 'vue-echarts';

use([ CanvasRenderer, PieChart, TitleComponent, TooltipComponent, LegendComponent ]);

// --- 移动端检测 ---
const isMobile = ref(false);
const checkMobile = () => {
  isMobile.value = window.innerWidth < 768;
};

// --- 状态与数据 (共享) ---
const loading = reactive({
  core: true, library: true, system: true, subscription: true, rankings: true
});

const stats = reactive({
  media_library: { cached_total: 0, movies_in_library: 0, series_in_library: 0, episodes_in_library: 0, missing_total: 0, resolution_stats: [] },
  system: { actor_mappings_total: 0, actor_mappings_linked: 0, actor_mappings_unlinked: 0, translation_cache_count: 0, processed_log_count: 0, failed_log_count: 0 },
  subscriptions_card: {
    watchlist: { watching: 0, paused: 0, completed: 0 },
    actors: { subscriptions: 0, tracked_total: 0, tracked_in_library: 0 },
    resubscribe: { pending: 0 },
    native_collections: { total: 0, count: 0, missing_items: 0 },
    custom_collections: { total: 0, count: 0, missing_items: 0 },
    quota: { available: 0, consumed: 0 }
  },
  release_group_ranking: [],
  historical_release_group_ranking: []
});

const themeVars = useThemeVars();

// --- API 请求 (共享) ---
const fetchCore = async () => {
  try {
    const res = await axios.get('/api/database/stats/core');
    if (res.data.status === 'success') Object.assign(stats.media_library, { cached_total: res.data.data.media_cached_total });
    if (res.data.status === 'success') Object.assign(stats.system, { actor_mappings_total: res.data.data.actor_mappings_total });
  } catch (e) { console.error(e); } finally { loading.core = false; }
};
const fetchLibrary = async () => {
  try {
    const res = await axios.get('/api/database/stats/library');
    if (res.data.status === 'success') Object.assign(stats.media_library, res.data.data);
  } catch (e) { console.error(e); } finally { loading.library = false; }
};
const fetchSystem = async () => {
  try {
    const res = await axios.get('/api/database/stats/system');
    if (res.data.status === 'success') Object.assign(stats.system, res.data.data);
  } catch (e) { console.error(e); } finally { loading.system = false; }
};
const fetchSubscription = async () => {
  try {
    const res = await axios.get('/api/database/stats/subscription');
    if (res.data.status === 'success') Object.assign(stats.subscriptions_card, res.data.data);
  } catch (e) { console.error(e); } finally { loading.subscription = false; }
};
const fetchRankings = async () => {
  try {
    const res = await axios.get('/api/database/stats/rankings');
    if (res.data.status === 'success') {
      stats.release_group_ranking = res.data.data.release_group_ranking;
      stats.historical_release_group_ranking = res.data.data.historical_release_group_ranking;
    }
  } catch (e) { console.error(e); } finally { loading.rankings = false; }
};

// --- 计算属性 ---
const actorMappingPercentage = computed(() => {
  const total = stats.system.actor_mappings_total || 1;
  const linked = stats.system.actor_mappings_linked || 0;
  return (linked / total) * 100;
});

const resolutionChartOptions = computed(() => {
  const chartData = stats.media_library.resolution_stats || [];
  if (!chartData.length) {
    return { series: [{ type: 'pie', data: [{ value: 1, name: '无数据' }] }] };
  }
  return {
    color: [ '#5470C6', '#91CC75', '#FAC858', '#73C0DE' ],
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    // 移动端隐藏图例，PC端显示
    legend: {
      show: !isMobile.value,
      orient: 'vertical',
      left: 'left',
      top: 'center',
      textStyle: { color: '#ccc' },
      data: chartData.map(item => item.resolution || '未知')
    },
    series: [
      {
        name: '分辨率',
        type: 'pie',
        // 移动端居中，PC端靠右
        radius: isMobile.value ? ['40%', '60%'] : ['50%', '70%'],
        center: isMobile.value ? ['50%', '50%'] : ['70%', '50%'],
        avoidLabelOverlap: false,
        itemStyle: { borderRadius: 8, borderColor: '#18181c', borderWidth: 2 },
        label: { show: false },
        labelLine: { show: false },
        data: chartData.map(item => ({ value: item.count, name: item.resolution || '未知' }))
      }
    ]
  };
});

const getIconPath = (groupName) => groupName ? `/icons/site/${groupName}.png` : '';
const handleIconError = (e) => {
  const img = e.target;
  const currentSrc = img.src;
  const defaultIcon = '/icons/site/pt.ico';
  if (currentSrc.match(/\.png($|\?)/i)) {
    img.src = currentSrc.replace(/\.png/i, '.ico');
    return;
  }
  if (currentSrc.includes('pt.ico')) {
    img.style.display = 'none';
  } else {
    img.src = defaultIcon;
    img.style.display = 'inline-block';
  }
};

onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  fetchCore(); fetchLibrary(); fetchSystem(); fetchSubscription(); fetchRankings();
});
onUnmounted(() => {
  window.removeEventListener('resize', checkMobile);
});
</script>

<style scoped>
/* 通用样式 */
.centered-statistic { text-align: center; }
.stat-value { font-size: 1.8em; font-weight: 600; line-height: 1.2; }
.section-title { font-size: 16px; font-weight: 600; color: var(--n-title-text-color); margin-bottom: 16px; }
.site-icon { width: 18px; height: 18px; margin-right: 8px; object-fit: contain; border-radius: 2px; }
.ranking-item { display: flex; align-items: center; width: 100%; font-size: 14px; }
.ranking-index { font-weight: bold; color: var(--n-text-color-2); width: 25px; text-align: right; padding-right: 8px; flex-shrink: 0; }
.ranking-count { color: var(--n-text-color-3); text-align: right; padding-left: 8px; flex-shrink: 0; white-space: nowrap; }

/* PC 端专用样式 */
.pc-ranking-name { font-weight: 500; width: 100px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 0; }
.stat-block { text-align: center; }
.stat-block-title { font-size: 14px; color: var(--n-text-color-2); margin-bottom: 12px; }
.stat-item-group { display: flex; justify-content: center; gap: 32px; }
.stat-item { text-align: center; }
.stat-item-label { font-size: 13px; color: var(--n-text-color-3); margin-bottom: 4px; }
.stat-item-value { font-size: 24px; font-weight: 600; line-height: 1.1; color: var(--n-statistic-value-text-color); }
.quota-grid { align-items: center; }
.quota-label-container { display: flex; align-items: center; justify-content: center; font-weight: 600; font-size: 14px; color: var(--n-text-color-2); }

/* Mobile 端专用样式 */
.mobile-stat-value { font-size: 1.5em; font-weight: 600; }
.mobile-label { font-size: 12px; color: var(--n-text-color-3); }
.mobile-value { font-size: 18px; font-weight: 600; }
.mobile-row { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 6px; }
.mobile-ranking-name { font-weight: 500; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
</style>