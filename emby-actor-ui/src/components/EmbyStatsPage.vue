<!-- components/EmbyStatsPage.vue -->
<template>
  <div style="padding: 24px; background-color: var(--n-color-modal);">
    
    <!-- 顶部控制栏 -->
    <div class="header-bar">
      <div class="title-section">
        <n-icon size="32" color="#007bff"><StatsChart /></n-icon>
        <div style="margin-left: 12px;">
          <h2 style="margin: 0; font-size: 20px;">Emby 仪表盘</h2>
          <span style="font-size: 12px; color: gray;">全站播放数据统计</span>
        </div>
      </div>
      <div class="filter-section">
        <n-radio-group v-model:value="timeRange" size="small" @update:value="fetchData">
          <n-radio-button :value="7">7天</n-radio-button>
          <n-radio-button :value="30">30天</n-radio-button>
          <n-radio-button :value="90">90天</n-radio-button>
          <n-radio-button :value="365">全年</n-radio-button>
        </n-radio-group>
        <n-button circle size="small" secondary style="margin-left: 12px;" @click="fetchData">
          <template #icon><n-icon><Refresh /></n-icon></template>
        </n-button>
      </div>
    </div>

    <n-spin :show="loading">
      <!-- 1. 核心指标卡片 -->
      <n-grid :cols="4" :x-gap="16" :y-gap="16" style="margin-top: 24px;">
        <n-gi v-for="(item, index) in summaryCards" :key="index">
          <n-card :bordered="false" class="stat-card">
            <div class="stat-icon" :style="{ background: item.colorBg, color: item.color }">
              <n-icon :component="item.icon" />
            </div>
            <div class="stat-content">
              <div class="stat-label">{{ item.label }}</div>
              <div class="stat-value">{{ item.value }} <span class="stat-unit">{{ item.unit }}</span></div>
            </div>
          </n-card>
        </n-gi>
      </n-grid>

      <!-- 2. 图表区域 -->
      <n-grid :cols="2" :x-gap="16" :y-gap="16" style="margin-top: 24px;">
        <!-- 左侧：播放趋势 (折线+柱状) -->
        <n-gi :span="1">
          <n-card title="播放趋势" :bordered="false" class="chart-card">
            <div style="height: 350px;">
              <v-chart class="chart" :option="trendChartOption" autoresize />
            </div>
          </n-card>
        </n-gi>
        <!-- 右侧：用户排行 (条形图) -->
        <n-gi :span="1">
          <n-card title="用户观看时长 (Top 10)" :bordered="false" class="chart-card">
            <div style="height: 350px;">
              <v-chart class="chart" :option="userChartOption" autoresize />
            </div>
          </n-card>
        </n-gi>
      </n-grid>

      <!-- 3. 热门媒体 (海报墙) -->
      <n-card title="热门剧集/电影" :bordered="false" style="margin-top: 24px; border-radius: 12px;">
        <n-grid :cols="10" :x-gap="12" :y-gap="12" responsive="screen" item-responsive>
          <n-gi span="5 m:2 l:1" v-for="(item, index) in statsData.media_rank" :key="item.id">
           <div class="poster-wrapper" @click="openEmbyItem(item.id)">
            <div class="poster-wrapper">
              <!-- 排名角标 -->
              <div class="rank-badge" :class="'rank-' + (index + 1)">{{ index + 1 }}</div>
              <!-- 播放次数角标 -->
              <div class="play-count-badge">{{ item.count }}次</div>
              
              <!-- 海报图片 -->
              <n-image
                lazy
                preview-disabled
                :src="getPosterUrl(item)" 
                fallback-src="https://via.placeholder.com/300x450?text=No+Image"
                object-fit="cover"
                class="poster-img"
              />
              <div class="poster-title">{{ item.name }}</div>
            </div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>
    </n-spin>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue';
import axios from 'axios';
import { useMessage } from 'naive-ui';
import { 
  StatsChart, Refresh, Play, Time, People, Videocam 
} from '@vicons/ionicons5';

// 引入 ECharts
import VChart from 'vue-echarts';
import { use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { BarChart, LineChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, LegendComponent, TitleComponent } from 'echarts/components';

use([CanvasRenderer, BarChart, LineChart, GridComponent, TooltipComponent, LegendComponent, TitleComponent]);

const message = useMessage();
const loading = ref(false);
const timeRange = ref(30);
const statsData = ref({
  total_plays: 0,
  total_duration_hours: 0,
  active_users: 0,
  watched_items: 0,
  chart_trend: { dates: [], counts: [], hours: [] },
  chart_users: { names: [], hours: [] },
  emby_url: '',
  emby_server_id: '',
  media_rank: []
});

// 顶部卡片配置
const summaryCards = computed(() => [
  { label: '播放次数', value: statsData.value.total_plays, unit: '', icon: Play, color: '#007bff', colorBg: '#e6f2ff' },
  { label: '播放时长', value: statsData.value.total_duration_hours, unit: '小时', icon: Time, color: '#fd7e14', colorBg: '#fff0e6' },
  { label: '活跃用户', value: statsData.value.active_users, unit: '', icon: People, color: '#28a745', colorBg: '#e6f9ec' },
  { label: '观看内容', value: statsData.value.watched_items, unit: '部', icon: Videocam, color: '#e83e8c', colorBg: '#fcebf3' },
]);

// 趋势图配置 (ECharts)
const trendChartOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  legend: { 
      data: ['播放次数', '时长(小时)'], 
      bottom: 0,
      textStyle: { color: '#999' } // ★ 修复图例文字颜色
  },
  grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
  xAxis: { 
      type: 'category', 
      data: statsData.value.chart_trend.dates,
      axisLabel: { color: '#999' } // ★ 修复X轴文字颜色
  },
  yAxis: [
    { 
        type: 'value', 
        name: '次数',
        nameTextStyle: { color: '#999' },
        axisLabel: { color: '#999' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } // ★ 修复分割线颜色
    },
    { 
        type: 'value', 
        name: '小时', 
        splitLine: { show: false },
        nameTextStyle: { color: '#999' },
        axisLabel: { color: '#999' }
    }
  ],
  series: [
    {
      name: '播放次数',
      type: 'bar',
      data: statsData.value.chart_trend.counts,
      itemStyle: { color: '#007bff', borderRadius: [4, 4, 0, 0] },
      barMaxWidth: 30
    },
    {
      name: '时长(小时)',
      type: 'line',
      yAxisIndex: 1,
      data: statsData.value.chart_trend.hours,
      itemStyle: { color: '#28a745' },
      smooth: true,
      areaStyle: { opacity: 0.1, color: '#28a745' }
    }
  ]
}));

// 用户排行图配置 (ECharts)
const userChartOption = computed(() => ({
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
  xAxis: { 
      type: 'value', 
      name: '小时',
      axisLabel: { color: '#999' },
      splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
  },
  yAxis: { 
      type: 'category', 
      data: statsData.value.chart_users.names, 
      inverse: true,
      axisLabel: { color: '#999' } // ★ 修复Y轴用户名颜色
  }, 
  series: [
    {
      name: '观看时长',
      type: 'bar',
      data: statsData.value.chart_users.hours,
      itemStyle: { color: '#007bff', borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: 'right', formatter: '{c} h' },
      barMaxWidth: 20
    }
  ]
}));

// 获取海报图片 URL
const getPosterUrl = (item) => {
  // 1. 优先使用 TMDb 海报 (走后端通用代理)
  if (item.poster_path) {
    // 拼接 TMDb 原图地址
    const tmdbUrl = `https://image.tmdb.org/t/p/w300${item.poster_path}`;
    
    // ★★★ 核心修改：通过 encodeURIComponent 编码后传给后端代理 ★★★
    // 这样服务器就会使用你在 config.ini 里配置的代理去下载图片
    return `/api/image_proxy?url=${encodeURIComponent(tmdbUrl)}`;
  }
  
  // 2. 回退：使用 Emby 代理 (如果本地数据库没封面)
  // 这里的 item.id 已经是聚合后的 Emby ID (剧集ID)
  return `/image_proxy/Items/${item.id}/Images/Primary?maxWidth=300`;
};


const fetchData = async () => {
  loading.value = true;
  try {
    const res = await axios.get(`/api/portal/dashboard-stats?days=${timeRange.value}`);
    statsData.value = res.data;
  } catch (error) {
    message.error("加载数据失败");
    console.error(error);
  } finally {
    loading.value = false;
  }
};

// 跳转函数 
const openEmbyItem = (itemId) => {
  const embyServerUrl = statsData.value.emby_url;
  
  if (!embyServerUrl || !itemId) {
    message.warning("未配置 Emby 地址");
    return;
  }

  // 1. 处理 URL 末尾斜杠
  const baseUrl = embyServerUrl.endsWith('/') ? embyServerUrl.slice(0, -1) : embyServerUrl;
  
  // 2. 获取 Server ID
  const serverId = statsData.value.emby_server_id;
  
  // 3. 拼接最终 URL (完全复刻你提供的逻辑)
  let finalUrl = `${baseUrl}/web/index.html#!/item?id=${itemId}${serverId ? `&serverId=${serverId}` : ''}`;
  
  window.open(finalUrl, '_blank');
};

onMounted(() => {
  fetchData();
});
</script>

<style scoped>
.header-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}
.title-section {
  display: flex;
  align-items: center;
}

/* 统计卡片样式 */
.stat-card {
  border-radius: 12px;
  transition: transform 0.2s;
}
.stat-card:hover { transform: translateY(-3px); }
.stat-card :deep(.n-card__content) {
  display: flex;
  align-items: center;
  padding: 20px;
}
.stat-icon {
  width: 56px;
  height: 56px;
  border-radius: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 28px;
  margin-right: 16px;
}
.stat-label { font-size: 14px; color: gray; margin-bottom: 4px; }
.stat-value { font-size: 24px; font-weight: bold; color: var(--n-text-color); }
.stat-unit { font-size: 12px; font-weight: normal; color: gray; margin-left: 4px; }

/* 图表卡片 */
.chart-card {
  border-radius: 12px;
  height: 100%;
}

/* 海报墙样式 */
.poster-wrapper {
  position: relative;
  border-radius: 8px;
  overflow: hidden;
  aspect-ratio: 2/3;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  transition: transform 0.2s;
  cursor: pointer;
}
.poster-wrapper:hover { transform: scale(1.03); }
.poster-img {
  width: 100%;
  height: 100%;
  display: block;
}
.poster-title {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  background: linear-gradient(to top, rgba(0,0,0,0.9), transparent);
  color: white;
  padding: 20px 8px 8px;
  font-size: 12px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  text-align: center;
}
.rank-badge {
  position: absolute;
  top: 0;
  left: 0;
  width: 24px;
  height: 24px;
  background: rgba(0,0,0,0.6);
  color: white;
  font-weight: bold;
  font-size: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-bottom-right-radius: 8px;
  z-index: 2;
}
.rank-1 { background: #ffc107; color: black; }
.rank-2 { background: #adb5bd; }
.rank-3 { background: #cd7f32; }

.play-count-badge {
  position: absolute;
  top: 4px;
  right: 4px;
  background: rgba(0,123,255,0.9);
  color: white;
  font-size: 10px;
  padding: 2px 6px;
  border-radius: 4px;
  z-index: 2;
}
</style>