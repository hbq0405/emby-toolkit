<!-- src/components/WatchlistPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="watchlist-page">
      <n-page-header>
        <template #title>
          <n-space align="center">
            <span>智能追剧列表</span>
            <n-tag v-if="filteredWatchlist.length > 0" type="info" round :bordered="false" size="small">
              {{ filteredWatchlist.length }} 部
            </n-tag>
          </n-space>
        </template>
        <n-alert title="操作提示" type="info" style="margin-top: 24px;">
          <li>本模块高度自动化，几乎无需人工干涉。新入库剧集，会自动判断是否完结，未完结剧集会自动更新集简介、检查是否缺失季、集，缺失的季会自动选择洗版订阅或普通订阅，缺集的季可设置洗版订阅或普通订阅。</li>
          <li>当剧集完结且所有集元数据完整后，会转入已完结列表，同时状态变更为待回归，后台定期会检查待回归剧集有新季上线会自动转成追剧中，并从上线之日开始自动订阅新季。</li>
          <li>所有缺失可由【统一订阅处理】任务自动订阅。</li>
        </n-alert>
        <template #extra>
          <n-space>
            <n-dropdown
              v-if="selectedItems.length > 0"
              trigger="click"
              :options="batchActions"
              @select="handleBatchAction"
            >
              <n-button type="primary">
                批量操作 ({{ selectedItems.length }})
                <template #icon><n-icon :component="CaretDownIcon" /></template>
              </n-button>
            </n-dropdown>
            <n-radio-group v-model:value="currentView" size="small">
              <n-radio-button value="inProgress">追剧中</n-radio-button>
              <n-radio-button value="completed">已完结</n-radio-button>
            </n-radio-group>
            <n-popconfirm @positive-click="addAllSeriesToWatchlist">
              <template #trigger>
                <n-button size="small" :loading="isAddingAll">
                  <template #icon><n-icon :component="ScanIcon" /></template>
                  一键扫描
                </n-button>
              </template>
              确定要扫描 Emby 选定的媒体库中的所有剧集吗？<br />
              此操作会忽略已在列表中的剧集，只添加新的。
            </n-popconfirm>

            <n-button size="small" @click="triggerGapScan" :loading="isGapScanning">
              <template #icon><n-icon :component="DownloadIcon" /></template>
              扫描缺集
            </n-button>

            <n-button size="small" @click="triggerAllWatchlistUpdate" :loading="isBatchUpdating">
              <template #icon><n-icon :component="SyncOutline" /></template>
              刷新追剧
            </n-button>
          </n-space>
        </template>
      </n-page-header>
      <n-divider />

      <n-space :wrap="true" :size="[20, 12]" style="margin-bottom: 20px;">
        <n-input v-model:value="searchQuery" placeholder="按名称搜索..." clearable style="min-width: 200px;" />
        
        <n-select
          v-if="currentView === 'inProgress'"
          v-model:value="filterStatus"
          :options="statusFilterOptions"
          style="min-width: 140px;"
        />
        
        <n-select
          v-model:value="filterMissing"
          :options="missingFilterOptions"
          style="min-width: 140px;"
        />
        
        <n-select
          v-if="currentView === 'completed'"
          v-model:value="filterGaps"
          :options="gapsFilterOptions"
          style="min-width: 140px;"
        />
        
        <n-select
          v-model:value="sortKey"
          :options="sortKeyOptions"
          style="min-width: 160px;"
        />
        
        <n-button-group>
          <n-button @click="sortOrder = 'asc'" :type="sortOrder === 'asc' ? 'primary' : 'default'" ghost>
            <template #icon><n-icon :component="ArrowUpIcon" /></template>
            升序
          </n-button>
          <n-button @click="sortOrder = 'desc'" :type="sortOrder === 'desc' ? 'primary' : 'default'" ghost>
            <template #icon><n-icon :component="ArrowDownIcon" /></template>
            降序
          </n-button>
        </n-button-group>
      </n-space>

      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error" style="max-width: 500px;">{{ error }}</n-alert></div>
      <div v-else-if="filteredWatchlist.length > 0">
        
        <!-- Grid 容器 -->
        <div class="responsive-grid">
          <div 
            v-for="(item, i) in renderedWatchlist" 
            :key="item.tmdb_id" 
            class="grid-item"
          >
            <n-card class="dashboard-card series-card" :bordered="false">
              <n-checkbox
                :checked="selectedItems.includes(item.tmdb_id)"
                @update:checked="(checked, event) => toggleSelection(item.tmdb_id, event, i)"
                class="card-checkbox"
              />
              
              <!-- ★★★ 核心结构：card-inner-layout 包裹层 ★★★ -->
              <div class="card-inner-layout">
                
                <!-- 左侧海报 -->
                <div class="card-poster-container">
                  <n-image lazy :src="getPosterUrl(item.emby_item_ids_json)" class="card-poster" object-fit="cover">
                    <template #placeholder><div class="poster-placeholder"><n-icon :component="TvIcon" size="32" /></div></template>
                  </n-image>
                  
                  <!-- 海报上的集数浮层 -->
                  <div class="poster-overlay">
                    <div class="overlay-content" @click.stop>
                      <n-popover trigger="click" placement="top" style="padding: 10px;">
                        <template #trigger>
                          <span class="episode-count clickable-count" title="点击修正总集数">
                            {{ item.collected_count || 0 }} / {{ item.total_count || 0 }}
                          </span>
                        </template>
                        
                        <!-- 弹出层内容保持不变 -->
                        <div style="display: flex; flex-direction: column; gap: 8px; width: 180px;">
                          <n-text strong depth="1">修正总集数</n-text>
                          <n-input-number 
                            v-model:value="tempTotalEpisodes" 
                            size="small" 
                            :min="item.collected_count || 0"
                            placeholder="输入实际集数"
                          />
                          <n-space justify="end" size="small">
                            <n-button size="tiny" @click="tempTotalEpisodes = item.collected_count">
                              填入当前
                            </n-button>
                            <n-button type="primary" size="tiny" @click="saveTotalEpisodes(item)">
                              保存
                            </n-button>
                          </n-space>
                          <n-text depth="3" style="font-size: 12px;">
                            * 保存后将锁定该数字。
                          </n-text>
                        </div>
                      </n-popover>
                    </div>
                  </div>
                </div>

                <!-- 右侧内容 -->
                <div class="card-content-container">
                  <div class="card-header">
                    <n-ellipsis class="card-title" :tooltip="{ style: { maxWidth: '300px' } }">{{ item.item_name }}</n-ellipsis>
                    <n-popconfirm @positive-click="() => removeFromWatchlist(item.parent_tmdb_id, item.item_name)">
                      <template #trigger><n-button text type="error" circle title="移除" size="tiny"><template #icon><n-icon :component="TrashIcon" /></template></n-button></template>
                      确定要从追剧列表中移除《{{ item.item_name }}》吗？
                    </n-popconfirm>
                  </div>
                  <div class="card-status-area">
                    <n-space vertical size="small">
                      <!-- 1. 顶部状态按钮 (保持不变) -->
                      <n-space align="center" :wrap="false">
                        <!-- 已完结视图 (聚合卡片) -->
                        <template v-if="currentView === 'completed'">
                          <n-tag round size="small" :bordered="false" :type="getSeriesStatusUI(item).type">
                            <template #icon><n-icon :component="getSeriesStatusUI(item).icon" /></template>
                            {{ getSeriesStatusUI(item).text }}
                          </n-tag>
                        </template>
                        <!-- 追剧中视图 (分季卡片) -->
                        <template v-else>
                          <n-button round size="tiny" :type="statusInfo(item.status).type" @click="() => updateStatus(item.tmdb_id, statusInfo(item.status).next)" :title="`点击切换到 '${statusInfo(item.status).nextText}'`">
                            <template #icon><n-icon :component="statusInfo(item.status).icon" /></template>
                            {{ statusInfo(item.status).text }}
                          </n-button>
                        </template>
                      </n-space>

                      <!-- ★★★ 2. 聚合信息展示区 (仅聚合卡片显示) ★★★ -->
                      <template v-if="item.is_aggregated">
                        <!-- A. 包含 (已入库) -->
                        <div v-if="item.seasons_contains && item.seasons_contains.length > 0" class="info-line">
                          <n-icon :component="CollectionsIcon" class="icon-fix" />
                          <n-text :depth="3">
                            包含: {{ item.seasons_contains.length }} 个季度 ({{ formatSeasonRange(item.seasons_contains) }})
                          </n-text>
                        </div>

                        <!-- B. 连载 (在库且活跃) - 绿色高亮 -->
                        <div v-if="item.seasons_airing && item.seasons_airing.length > 0" class="info-line">
                          <n-icon :component="WatchingIcon" class="icon-fix" style="color: var(--n-success-color)" />
                          <n-text :depth="3" style="color: var(--n-success-color)">
                            连载: {{ item.seasons_airing.length }} 个季度 ({{ formatSeasonRange(item.seasons_airing) }})
                          </n-text>
                        </div>

                        <!-- C. 缺失 (未入库) - 红色高亮 -->
                        <div v-if="item.seasons_missing && item.seasons_missing.length > 0" class="info-line">
                          <n-icon :component="DownloadIcon" class="icon-fix" style="color: var(--n-error-color)" />
                          <n-text :depth="3" style="color: var(--n-error-color)">
                            缺失: {{ item.seasons_missing.length }} 个季度 ({{ formatSeasonRange(item.seasons_missing) }})
                          </n-text>
                        </div>
                      </template>

                      <!-- ★★★ 3. 单季详细信息 (仅非聚合卡片显示) ★★★ -->
                      <template v-else>
                        <!-- 待播集数 -->
                        <div v-if="nextEpisode(item)?.name" class="info-line">
                          <n-icon :component="TvIcon" class="icon-fix" />
                          <n-text :depth="3" style="flex: 1; min-width: 0;">
                            <n-ellipsis>待播集: {{ nextEpisode(item).name }}</n-ellipsis>
                          </n-text>
                        </div>

                        <!-- 播出时间 -->
                        <div v-if="nextEpisode(item)?.name" class="info-line">
                          <n-icon :component="CalendarIcon" class="icon-fix" />
                          <n-text :depth="3">
                            播出时间: {{ nextEpisode(item).air_date ? formatAirDate(nextEpisode(item).air_date) : '待定' }}
                          </n-text>
                        </div>

                        <!-- 上次检查 -->
                        <div class="info-line">
                          <n-icon :component="TimeIcon" class="icon-fix" />
                          <n-text :depth="3">上次检查: {{ formatTimestamp(item.last_checked_at) }}</n-text>
                        </div>
                      </template>
                    </n-space>
                  </div>
                  
                  <!-- 进度条作为分隔线 -->
                  <div class="progress-separator">
                    <n-progress 
                      type="line" 
                      :percentage="calculateProgress(item)" 
                      :status="getProgressStatus(item)"
                      :color="getProgressColor(item)"
                      :height="2" 
                      :show-indicator="false"
                      :border-radius="0"
                      :processing="calculateProgress(item) < 100"
                    />
                  </div>

                  <!-- 底部按钮 -->
                  <div class="card-actions">
                    <!-- 只有 hasMissing 为真时才显示，且颜色改为 warning -->
                    <n-tooltip v-if="hasMissing(item)">
                      <template #trigger>
                        <n-button
                          type="warning"
                          size="small"
                          circle
                          @click="() => openMissingInfoModal(item)"
                        >
                          <template #icon><n-icon :component="EyeIcon" /></template>
                        </n-button>
                      </template>
                      <!-- 悬停时显示具体缺什么，例如：缺 1 季 | 有分集缺失 -->
                      {{ getMissingCountText(item) }} (点击查看详情)
                    </n-tooltip>
                    <n-tooltip>
                      <template #trigger>
                        <n-button
                          circle
                          :loading="refreshingItems[item.parent_tmdb_id]" 
                          @click="() => triggerSingleRefresh(item.parent_tmdb_id, item.item_name)"
                        >
                          <template #icon><n-icon :component="SyncOutline" /></template>
                        </n-button>
                      </template>
                      立即刷新此剧集
                    </n-tooltip>
                    <n-tooltip>
                      <template #trigger><n-button text @click="openInEmby(item.emby_item_ids_json)"><template #icon><n-icon :component="EmbyIcon" size="18" /></template></n-button></template>
                      在 Emby 中打开
                    </n-tooltip>
                    <n-tooltip>
                      <template #trigger><n-button text tag="a" :href="`https://www.themoviedb.org/tv/${item.parent_tmdb_id}`" target="_blank"><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button></template>
                      在 TMDb 中打开
                    </n-tooltip>
                  </div>
                </div>
              </div>
              <!-- 布局结束 -->

            </n-card>
          </div>
        </div>
        <!-- Grid 结束 -->

        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>
      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>
    <n-modal v-model:show="showModal" preset="card" style="width: 90%; max-width: 900px;" :title="selectedSeries ? `缺失详情 - ${selectedSeries.item_name}` : ''" :bordered="false" size="huge">
      <div v-if="selectedSeries && missingData">
        <n-tabs type="line" animated v-model:value="activeTab">
          <n-tab-pane name="seasons" :tab="`缺季 (${missingData.missing_seasons.length})`" :disabled="missingData.missing_seasons.length === 0">
            <n-list bordered>
              <n-list-item v-for="season in missingData.missing_seasons" :key="season.season_number">
                <template #prefix><n-tag type="warning">S{{ season.season_number }}</n-tag></template>
                <n-ellipsis>{{ season.name }} ({{ season.episode_count }}集, {{ formatAirDate(season.air_date) }})</n-ellipsis>
              </n-list-item>
            </n-list>
          </n-tab-pane>
          <n-tab-pane name="gaps" :tab="`缺集的季 (${missingData.seasons_with_gaps.length})`" :disabled="missingData.seasons_with_gaps.length === 0">
            <n-list bordered>
              <n-list-item v-for="gap in missingData.seasons_with_gaps" :key="gap.season">
                <n-space vertical>
                  <div><n-tag type="error">第 {{ gap.season }} 季</n-tag> 存在分集缺失</div>
                  <n-text :depth="3">具体缺失的集号: {{ gap.missing.join(', ') }}</n-text>
                </n-space>
              </n-list-item>
            </n-list>
          </n-tab-pane>
        </n-tabs>
      </div>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, h, computed, watch } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NPopconfirm, NTooltip, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NModal, NTabs, NTabPane, NList, NListItem, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup, NProgress, useThemeVars, NPopover, NInputNumber } from 'naive-ui';
import { SyncOutline, TvOutline as TvIcon, TrashOutline as TrashIcon, EyeOutline as EyeIcon, CalendarOutline as CalendarIcon, TimeOutline as TimeIcon, PlayCircleOutline as WatchingIcon, PauseCircleOutline as PausedIcon, CheckmarkCircleOutline as CompletedIcon, ScanCircleOutline as ScanIcon, CaretDownOutline as CaretDownIcon, FlashOffOutline as ForceEndIcon, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, DownloadOutline as DownloadIcon, AlbumsOutline as CollectionsIcon } from '@vicons/ionicons5';
import { format, parseISO } from 'date-fns';
import { useConfig } from '../composables/useConfig.js';

// ... (Script 逻辑保持不变) ...
const EmbyIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 48 48", width: "18", height: "18" }, [
  h('path', { d: "M24,4.2c-11,0-19.8,8.9-19.8,19.8S13,43.8,24,43.8s19.8-8.9,19.8-19.8S35,4.2,24,4.2z M24,39.8c-8.7,0-15.8-7.1-15.8-15.8S15.3,8.2,24,8.2s15.8,7.1,15.8,15.8S32.7,39.8,24,39.8z", fill: "currentColor" }),
  h('polygon', { points: "22.2,16.4 22.2,22.2 16.4,22.2 16.4,25.8 22.2,25.8 22.2,31.6 25.8,31.6 25.8,25.8 31.6,31.6 31.6,22.2 25.8,22.2 25.8,16.4 ", fill: "currentColor" })
]);
const TMDbIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 512 512", width: "18", height: "18" }, [
  h('path', { d: "M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zM133.2 176.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zM133.2 262.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8z", fill: "#01b4e4" })
]);

const { configModel } = useConfig();
const message = useMessage();
const dialog = useDialog();
const props = defineProps({ taskStatus: { type: Object, required: true } });

const rawWatchlist = ref([]);
const currentView = ref('inProgress');
const isLoading = ref(true);
const isBatchUpdating = ref(false);
const error = ref(null);
const showModal = ref(false);
const isAddingAll = ref(false);
const isGapScanning = ref(false);
const selectedSeries = ref(null);
const refreshingItems = ref({});
const isTaskRunning = computed(() => props.taskStatus.is_running);
const displayCount = ref(30);
const INCREMENT = 30;
const loaderRef = ref(null);
let observer = null;
const themeVars = useThemeVars();
const selectedItems = ref([]);
const lastSelectedIndex = ref(null);

const searchQuery = ref('');
const filterStatus = ref('all');
const filterMissing = ref('all');
const filterGaps = ref('all');
const sortKey = ref('last_checked_at');
const sortOrder = ref('desc');
const tempTotalEpisodes = ref(0);
const activeTab = ref('seasons');

const hasMissingSeasons = (item) => {
  const data = item.missing_info;
  return data?.missing_seasons?.length > 0;
};

const hasGaps = (item) => {
  const data = item.missing_info;
  return Array.isArray(data?.seasons_with_gaps) && data.seasons_with_gaps.length > 0;
};

const hasMissing = (item) => {
  return hasMissingSeasons(item) || hasGaps(item);
};

const getMissingCountText = (item) => {
  if (!hasMissing(item)) return '';
  const data = item.missing_info;
  const season_count = data?.missing_seasons?.length || 0;
  const gaps_count = (Array.isArray(data?.seasons_with_gaps) && data.seasons_with_gaps.length > 0) ? 1 : 0;
  
  let parts = [];
  if (season_count > 0) parts.push(`缺 ${season_count} 季`);
  if (gaps_count > 0) parts.push(`有分集缺失`);
  return parts.join(' | ');
};

const statusFilterOptions = [
  { label: '所有状态', value: 'all' },
  { label: '追剧中', value: 'Watching' },
  { label: '已暂停', value: 'Paused' },
];
const missingFilterOptions = computed(() => {
    return [
      { label: '缺季筛选', value: 'all' },
      { label: '有缺季', value: 'yes' },
      { label: '无缺季', value: 'no' },
    ];
});
const gapsFilterOptions = [
    { label: '缺集筛选', value: 'all' },
    { label: '有缺集', value: 'yes' },
    { label: '无缺集', value: 'no' },
];
const sortKeyOptions = [
  { label: '按上次检查时间', value: 'last_checked_at' },
  { label: '按剧集名称', value: 'item_name' },
  { label: '按添加时间', value: 'added_at' },
  { label: '按发行年份', value: 'release_year' },
];

const batchActions = computed(() => {
  const removeAction = {
    label: '批量移除',
    key: 'remove',
    icon: () => h(NIcon, { component: TrashIcon })
  };

  if (currentView.value === 'inProgress') {
    return [
      {
        label: '强制完结',
        key: 'forceEnd',
        icon: () => h(NIcon, { component: ForceEndIcon })
      },
      removeAction
    ];
  } 
  else if (currentView.value === 'completed') {
    const actions = [
      {
        label: '重新追剧',
        key: 'rewatch',
        icon: () => h(NIcon, { component: WatchingIcon })
      }
    ];

    const hasGapsInSelection = filteredWatchlist.value
      .filter(item => selectedItems.value.includes(item.tmdb_id))
      .some(hasGaps);

    actions.push(removeAction);
    return actions;
  }

  return []; 
});

const filteredWatchlist = computed(() => {
  let list = rawWatchlist.value;

  // 1. 基础过滤：搜索 (通用)
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    list = list.filter(item => item.item_name.toLowerCase().includes(query));
  }

  // 2. 视图分流
  if (currentView.value === 'inProgress') {
    // --- 追剧中视图 ---
    list = list.filter(item => item.status === 'Watching' || item.status === 'Paused');
    
    if (filterStatus.value !== 'all') {
      list = list.filter(item => item.status === filterStatus.value);
    }
    // 追剧中：应用缺季筛选
    if (filterMissing.value !== 'all') {
      const hasMissingValue = filterMissing.value === 'yes';
      list = list.filter(item => hasMissingSeasons(item) === hasMissingValue);
    }
    // 追剧中：应用缺集筛选
    if (filterGaps.value !== 'all') {
       const hasGapsValue = filterGaps.value === 'yes';
       list = list.filter(item => hasGaps(item) === hasGapsValue);
    }

  } else if (currentView.value === 'completed') {
    // --- 已完结视图：聚合逻辑 ---
    
    let completedSeasons = list.filter(item => item.status === 'Completed');

    const groups = {};
    completedSeasons.forEach(season => {
      const pid = season.parent_tmdb_id;
      if (!groups[pid]) {
        groups[pid] = { 
          ...season, 
          item_name: season.item_name.replace(/ 第 \d+ 季$/, ''),
          collected_count: season.series_collected_count || 0,
          total_count: season.series_total_episodes || 0,
          status: season.series_status, 
          is_aggregated: true,
          
          // ★★★ 初始化分类数组 ★★★
          seasons_contains: [], // 已入库 (in_library=TRUE)
          seasons_missing: [],  // 缺失 (in_library=FALSE)
          seasons_airing: []    // 连载中 (in_library=TRUE 且 状态活跃)
        };
      }
      
      // ★★★ 分类逻辑 ★★★
      // 1. 判断是否在库：只要收集数 > 0 就算在库 (或者你可以用 season.collected_count === season.total_count 来判断全收集)
      // 这里按你的要求：in_library=TRUE (即 collected_count > 0)
      const isInLibrary = (season.collected_count > 0);
      
      if (isInLibrary) {
        groups[pid].seasons_contains.push(season.season_number);
        
        // 2. 判断是否连载中：在库 且 状态是 Watching/Paused
        if (season.status === 'Watching' || season.status === 'Paused') {
           groups[pid].seasons_airing.push(season.season_number);
        }
      } else {
        // 3. 缺失：完全没入库
        groups[pid].seasons_missing.push(season.season_number);
      }
      
      // 更新时间取最新的
      if (new Date(season.last_checked_at) > new Date(groups[pid].last_checked_at)) {
        groups[pid].last_checked_at = season.last_checked_at;
      }
    });

    // C. 转回数组
    list = Object.values(groups);

    // ★★★ 修复：在聚合后，重新应用筛选逻辑 ★★★
    
    // 1. 缺季筛选 (Missing Seasons)
    // 注意：missing_info 来自父剧集，所以聚合对象的 missing_info 是准确的
    if (filterMissing.value !== 'all') {
      const hasMissingValue = filterMissing.value === 'yes';
      list = list.filter(item => hasMissingSeasons(item) === hasMissingValue);
    }

    // 2. 缺集筛选 (Gaps)
    if (filterGaps.value !== 'all') {
       const hasGapsValue = filterGaps.value === 'yes';
       list = list.filter(item => hasGaps(item) === hasGapsValue);
    }
  }

  // 3. 排序 (通用)
  list.sort((a, b) => {
    let valA, valB;
    switch (sortKey.value) {
      case 'item_name':
        valA = a.item_name || '';
        valB = b.item_name || '';
        return sortOrder.value === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      case 'added_at':
        valA = a.added_at ? new Date(a.added_at).getTime() : 0;
        valB = b.added_at ? new Date(b.added_at).getTime() : 0;
        break;
      case 'release_year':
        valA = a.release_year || 0;
        valB = b.release_year || 0;
        break;
      case 'last_checked_at':
      default:
        valA = a.last_checked_at ? new Date(a.last_checked_at).getTime() : 0;
        valB = b.last_checked_at ? new Date(b.last_checked_at).getTime() : 0;
        break;
    }
    return sortOrder.value === 'asc' ? valA - valB : valB - valA;
  });

  return list;
});

// 辅助函数：将数字数组格式化为范围字符串 (如 "S1-S4, S6")
const formatSeasonRange = (numbers) => {
  if (!numbers || numbers.length === 0) return '';
  // 排序
  const sorted = [...numbers].sort((a, b) => a - b);
  const ranges = [];
  let start = sorted[0];
  let prev = sorted[0];

  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i] === prev + 1) {
      prev = sorted[i];
    } else {
      ranges.push(start === prev ? `S${start}` : `S${start}-S${prev}`);
      start = sorted[i];
      prev = sorted[i];
    }
  }
  ranges.push(start === prev ? `S${start}` : `S${start}-S${prev}`);
  return ranges.join(', ');
};

// 计算剧集层面的精致状态 
const getSeriesStatusUI = (item) => {
  const tmdbStatus = item.tmdb_status;
  // 注意：这里的 item.status 已经是我们在聚合时赋值的 series_status 了
  const internalStatus = item.status; 

  // 1. 只要内部状态是 追剧中(Watching) 或 暂停中(Paused) -> 视为【已回归】
  if (internalStatus === 'Watching' || internalStatus === 'Paused') {
    return { text: '已回归', type: 'success', icon: WatchingIcon, color: undefined };
  }

  // 2. 如果内部状态是 Completed
  if (internalStatus === 'Completed') {
      // 2.1 TMDb 是 Returning Series -> 【待回归】
      if (tmdbStatus === 'Returning Series' || tmdbStatus === 'In Production' || tmdbStatus === 'Planned') {
          return { text: '待回归', type: 'warning', icon: PausedIcon, color: undefined };
      }
      // 2.2 TMDb 是 Ended -> 【已完结】
      else {
          return { text: '已完结', type: 'default', icon: CompletedIcon, color: undefined };
      }
  }

  // 兜底
  return { text: '已完结', type: 'default', icon: CompletedIcon, color: undefined };
};

const renderedWatchlist = computed(() => {
  return filteredWatchlist.value.slice(0, displayCount.value);
});
const hasMore = computed(() => {
  return displayCount.value < filteredWatchlist.value.length;
});
const emptyStateDescription = computed(() => {
  if (rawWatchlist.value.length > 0 && filteredWatchlist.value.length === 0) {
    return '没有匹配当前筛选条件的剧集。';
  }
  if (currentView.value === 'inProgress') {
    return '追剧列表为空，快去“手动处理”页面搜索并添加你正在追的剧集吧！';
  }
  return '还没有已完结的剧集。';
});

const missingData = computed(() => {
  const defaults = { 
    missing_seasons: [], 
    seasons_with_gaps: []
  };
  const infoFromServer = selectedSeries.value?.missing_info;
  
  if (infoFromServer && !Array.isArray(infoFromServer.seasons_with_gaps)) {
    infoFromServer.seasons_with_gaps = [];
  }

  return { ...defaults, ...infoFromServer };
});

const nextEpisode = (item) => {
  return item.next_episode_to_air || null;
};

const toggleSelection = (itemId, event, index) => {
  if (!event) return;
  if (event.shiftKey && lastSelectedIndex.value !== null) {
    const start = Math.min(lastSelectedIndex.value, index);
    const end = Math.max(lastSelectedIndex.value, index);
    const idsInRange = renderedWatchlist.value.slice(start, end + 1).map(i => i.tmdb_id);
    const isCurrentlySelected = selectedItems.value.includes(itemId);
    const willSelect = !isCurrentlySelected;
    if (willSelect) {
      const newSet = new Set(selectedItems.value);
      idsInRange.forEach(id => newSet.add(id));
      selectedItems.value = Array.from(newSet);
    } else {
      selectedItems.value = selectedItems.value.filter(id => !idsInRange.includes(id));
    }
  } else {
    const idx = selectedItems.value.indexOf(itemId);
    if (idx > -1) {
      selectedItems.value.splice(idx, 1);
    } else {
      selectedItems.value.push(itemId);
    }
  }
  lastSelectedIndex.value = index;
};

const handleBatchAction = (key) => {
  // ★★★ 通用逻辑：将选中的“季ID”转换为去重后的“父剧集ID” ★★★
  const getParentIds = () => {
    // 1. 在原始列表中找到选中的那些项目
    const selectedItemObjects = rawWatchlist.value.filter(i => selectedItems.value.includes(i.tmdb_id));
    // 2. 提取 parent_tmdb_id 并去重 (Set)
    return [...new Set(selectedItemObjects.map(i => i.parent_tmdb_id))];
  };

  if (key === 'forceEnd') {
    const parentIds = getParentIds(); // 获取剧集ID
    dialog.warning({
      title: '确认操作',
      content: `确定要将选中的 ${parentIds.length} 部剧集标记为“强制完结”吗？`,
      positiveText: '确定',
      negativeText: '取消',
      onPositiveClick: async () => {
        try {
          // ★★★ 传 parentIds 给后端 ★★★
          const response = await axios.post('/api/watchlist/batch_force_end', { item_ids: parentIds });
          message.success(response.data.message || '批量操作成功！');
          await fetchWatchlist();
          selectedItems.value = [];
        } catch (err) {
          message.error(err.response?.data?.error || '批量操作失败。');
        }
      }
    });
  }
  else if (key === 'rewatch') {
    const parentIds = getParentIds(); // 获取剧集ID
    dialog.info({
      title: '确认操作',
      content: `确定要将选中的 ${parentIds.length} 部剧集的状态改回“追剧中”吗？`,
      positiveText: '确定',
      negativeText: '取消',
      onPositiveClick: async () => {
        try {
          // ★★★ 传 parentIds 给后端 ★★★
          const response = await axios.post('/api/watchlist/batch_update_status', { item_ids: parentIds, new_status: 'Watching' });
          message.success(response.data.message || '批量操作成功！');
          // 重新追剧后，通常希望留在当前视图或刷新列表，这里简单刷新即可
          await fetchWatchlist(); 
          selectedItems.value = [];
          currentView.value = 'inProgress'; // 自动切回追剧中视图方便查看
        } catch (err) {
          message.error(err.response?.data?.error || '批量操作失败。');
        }
      }
    });
  }
  else if (key === 'remove') {
    const parentIds = getParentIds(); // 获取剧集ID
    dialog.warning({
      title: '确认移除',
      content: `确定要从追剧列表中移除选中的 ${parentIds.length} 个项目吗？此操作不可恢复。`,
      positiveText: '确定移除',
      negativeText: '取消',
      onPositiveClick: async () => {
        try {
          // ★★★ 传 parentIds 给后端 ★★★
          const response = await axios.post('/api/watchlist/batch_remove', {
            item_ids: parentIds
          });
          message.success(response.data.message || '批量移除成功！');
          await fetchWatchlist();
          selectedItems.value = [];
        } catch (err) {
          message.error(err.response?.data?.error || '批量移除失败。');
        }
      }
    });
  }
};

const addAllSeriesToWatchlist = async () => {
  isAddingAll.value = true;
  try {
    const response = await axios.post('/api/actions/add_all_series_to_watchlist');
    message.success(response.data.message || '任务已成功提交！');
  } catch (err) {
    message.error(err.response?.data?.error || '启动扫描任务失败。');
  } finally {
    isAddingAll.value = false;
  }
};

const triggerGapScan = async () => {
  isGapScanning.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'scan-library-gaps' });
    message.success(response.data.message || '媒体库缺集扫描任务已成功提交！');
  } catch (err) {
    message.error(err.response?.data?.error || '启动缺集扫描任务失败。');
  } finally {
    isGapScanning.value = false;
  }
};

const triggerSingleRefresh = async (itemId, itemName) => {
  refreshingItems.value[itemId] = true;
  try {
    await axios.post(`/api/watchlist/refresh/${itemId}`);
    message.success(`《${itemName}》的刷新任务已提交！`);
    setTimeout(() => { fetchWatchlist(); }, 5000);
  } catch (err) {
    message.error(err.response?.data?.error || '启动刷新失败。');
  } finally {
    setTimeout(() => { refreshingItems.value[itemId] = false; }, 5000);
  }
};

watch(currentView, () => {
  displayCount.value = 30;
  selectedItems.value = [];
  lastSelectedIndex.value = null;
  searchQuery.value = '';
  filterStatus.value = 'all';
  filterMissing.value = 'all';
  filterGaps.value = 'all';
});

const loadMore = () => {
  if (hasMore.value) {
    displayCount.value = Math.min(displayCount.value + INCREMENT, filteredWatchlist.value.length);
  }
};

const formatTimestamp = (timestamp) => {
  if (!timestamp) return '从未';
  try {
    const localDate = new Date(timestamp);
    return format(localDate, 'MM-dd HH:mm');
  }
  catch (e) {
    return 'N/A';
  }
};

const formatAirDate = (dateString) => {
  if (!dateString) return '待定';
  try { return format(parseISO(dateString), 'yyyy-MM-dd'); }
  catch (e) { return 'N/A'; }
};

const getPosterUrl = (embyIds) => {
  const itemId = embyIds?.[0];
  if (!itemId) return '';
  return `/image_proxy/Items/${itemId}/Images/Primary?maxHeight=480&tag=1`;
};

const openInEmby = (embyIds) => {
  const itemId = embyIds?.[0];
  const embyServerUrl = configModel.value?.emby_server_url;
  if (!embyServerUrl || !itemId) return;
  const baseUrl = embyServerUrl.endsWith('/') ? embyServerUrl.slice(0, -1) : embyServerUrl;
  const serverId = configModel.value?.emby_server_id;
  let finalUrl = `${baseUrl}/web/index.html#!/item?id=${itemId}${serverId ? `&serverId=${serverId}` : ''}`;
  window.open(finalUrl, '_blank');
};

const statusInfo = (status) => {
  const map = {
    'Watching': { type: 'success', text: '追剧中', icon: WatchingIcon, next: 'Paused', nextText: '暂停' },
    'Paused': { type: 'warning', text: '已暂停', icon: PausedIcon, next: 'Watching', nextText: '继续追' },
    'Completed': { type: 'default', text: '已完结', icon: CompletedIcon, next: 'Watching', nextText: '重新追' },
  };
  return map[status] || map['Paused'];
};

const translateTmdbStatus = (status) => {
  const statusMap = {
    "Returning Series": "连载中", "Ended": "已完结", "Canceled": "已取消",
    "In Production": "制作中", "Planned": "计划中", "Pilot": "试播"
  };
  return statusMap[status] || status;
};

const getSmartTMDbStatusText = (item) => {
  if (item.status === 'Completed' && (item.tmdb_status === 'Ended' || item.tmdb_status === 'Canceled')) {
    return '待回归';
  }
  return translateTmdbStatus(item.tmdb_status);
};

const getSmartTMDbStatusType = (item) => {
  return getSmartTMDbStatusText(item) === '待回归' ? 'info' : 'default';
};

const fetchWatchlist = async () => {
  isLoading.value = true;
  error.value = null;
  try {
    const response = await axios.get('/api/watchlist');
    rawWatchlist.value = response.data;
  } catch (err) {
    error.value = err.response?.data?.error || '获取追剧列表失败。';
  } finally {
    isLoading.value = false;
  }
};

const updateStatus = async (itemId, newStatus) => {
  const item = rawWatchlist.value.find(i => i.tmdb_id === itemId);
  if (!item) return;
  const oldStatus = item.status;
  item.status = newStatus;
  try {
    await axios.post('/api/watchlist/update_status', { item_id: itemId, new_status: newStatus });
    message.success('状态更新成功！');
  } catch (err) {
    item.status = oldStatus;
    message.error(err.response?.data?.error || '更新状态失败。');
  }
};

const removeFromWatchlist = async (seriesId, itemName) => {
  try {
    await axios.post(`/api/watchlist/remove/${seriesId}`);
    message.success(`已将《${itemName}》从追剧列表移除。`);
    
    // ★★★ 修正：比对 parent_tmdb_id，把该剧的所有季都移出视图 ★★★
    rawWatchlist.value = rawWatchlist.value.filter(i => i.parent_tmdb_id !== seriesId);
    
    // 如果是在已完结视图（聚合模式），也需要确保清理干净
    selectedItems.value = []; 
  } catch (err) {
    message.error(err.response?.data?.error || '移除失败。');
  }
};

const triggerAllWatchlistUpdate = async () => {
  isBatchUpdating.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'process-watchlist' });
    message.success(response.data.message || '所有追剧项目更新任务已启动！');
  } catch (err) {
    message.error(err.response?.data?.error || '启动更新任务失败。');
  } finally {
    isBatchUpdating.value = false;
  }
};

const openMissingInfoModal = (item) => {
  selectedSeries.value = item;
  const info = item.missing_info || {};
  if (info.missing_seasons && info.missing_seasons.length > 0) {
    activeTab.value = 'seasons';
  } else if (info.seasons_with_gaps && info.seasons_with_gaps.length > 0) {
    activeTab.value = 'gaps';
  } else {
    activeTab.value = 'seasons';
  }
  showModal.value = true;
};

watch(() => props.taskStatus.is_running, (isRunning, wasRunning) => {
  if (wasRunning && !isRunning) {
    const lastAction = props.taskStatus.last_action;
    const relevantActions = ['追剧', '扫描', '刷新'];
    if (lastAction && relevantActions.some(action => lastAction.includes(action))) {
      message.info('相关后台任务已结束，正在刷新追剧列表...');
      fetchWatchlist();
    }
  }
});

// 计算进度百分比
const calculateProgress = (item) => {
  const total = item.total_count || 0;
  const collected = item.collected_count || 0;
  if (total === 0) return 0;
  const percent = (collected / total) * 100;
  return Math.min(percent, 100); // 封顶 100%
};

// 根据进度返回颜色状态
const getProgressStatus = (item) => {
  const p = calculateProgress(item);
  if (p >= 100) return 'success';
  return 'default'; // 使用默认主题色 (Primary)
};

const getProgressColor = (item) => {
  const p = calculateProgress(item);
  // 如果进度 >= 100，返回 undefined，让组件自动变绿 (Success状态)
  if (p >= 100) return undefined;
  
  // 如果未完成，返回当前主题的主色调
  return themeVars.value.primaryColor;
};

// 修正总集数
const saveTotalEpisodes = async (item) => {
  const newTotal = tempTotalEpisodes.value || item.collected_count;
  
  try {
    await axios.post('/api/watchlist/update_total_episodes', {
      tmdb_id: item.tmdb_id,
      total_episodes: newTotal,
      item_type: 'Season' // ★★★ 明确指定我们要改的是季 ★★★
    });
    message.success(`已将《${item.item_name}》总集数修正为 ${newTotal}`);
    
    // 更新视图
    item.total_count = newTotal;
    // 强制刷新进度条状态
    item.total_episodes_locked = true; 
  } catch (err) {
    message.error('修正失败');
  }
};

onMounted(() => {
  fetchWatchlist();
  observer = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting) {
        loadMore();
      }
    },
    { root: null, rootMargin: '0px', threshold: 0.1 }
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

watch(loaderRef, (newEl, oldEl) => {
  if (oldEl && observer) observer.unobserve(oldEl);
  if (newEl && observer) observer.observe(newEl);
});
</script>

<style scoped>
/* 页面基础 */
.watchlist-page { padding: 0 10px; }
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 200px); }

/* ★★★ Grid 布局 ★★★ */
.responsive-grid {
  display: grid;
  gap: 16px;
  /* 320px 基准宽度 */
  grid-template-columns: repeat(auto-fill, minmax(calc(320px * var(--card-scale, 1)), 1fr));
}

.grid-item {
  height: 100%;
  min-width: 0;
}

/* ★★★ 卡片容器 ★★★ */
.series-card {
  cursor: pointer;
  transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
  height: 100%;
  position: relative;
  
  /* ★★★ 核心 1：设定基准字号，所有内部元素都将基于此缩放 ★★★ */
  font-size: calc(14px * var(--card-scale, 1)); 
  
  border-radius: calc(12px * var(--card-scale, 1));
  overflow: hidden; 
  border: 1px solid var(--n-border-color);
}

.series-card:hover {
  transform: translateY(-4px);
}

.card-selected {
  outline: 2px solid var(--n-color-primary);
  outline-offset: -2px;
}

/* ★★★ 核心 2：强制 Naive UI 组件跟随缩放 ★★★ */
/* 这段代码强制卡片内的所有文本、按钮、标签都继承上面的 font-size */
.series-card :deep(.n-card__content),
.series-card :deep(.n-button),
.series-card :deep(.n-tag),
.series-card :deep(.n-text),
.series-card :deep(.n-ellipsis) {
  font-size: inherit !important; 
}

/* 调整图标大小以适应缩放 */
.series-card :deep(.n-icon) {
  font-size: 1.2em !important; 
}

/* 恢复内边距 */
.series-card.dashboard-card > :deep(.n-card__content) {
  padding: calc(10px * var(--card-scale, 1)) !important; 
  display: flex !important;
  flex-direction: column !important;
  height: 100% !important;
}

/* ★★★ 内部布局：左右拉伸 ★★★ */
.card-inner-layout {
  display: flex;
  flex-direction: row;
  height: 100%;
  width: 100%;
  /* 关键：让海报和内容等高 */
  align-items: stretch; 
  gap: calc(12px * var(--card-scale, 1));
}

/* ★★★ 海报区域 ★★★ */
.card-poster-container {
  flex-shrink: 0; 
  /* 宽度随比例缩放 */
  width: calc(130px * var(--card-scale, 1));
  /* 关键：高度设为 100%，让它自动填满父容器（父容器高度由右侧文字撑开） */
  height: auto; 
  min-height: 100%; 
  
  position: relative;
  background-color: rgba(0,0,0,0.1);
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

/* 新增：海报浮层 */
.poster-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 20px 6px 4px 6px; /* 上方留出空间给渐变 */
  background: linear-gradient(to top, rgba(0,0,0,0.8) 0%, rgba(0,0,0,0.4) 60%, transparent 100%);
  color: white;
  font-size: 0.85em;
  font-weight: 600;
  text-align: right; /* 数字靠右显示 */
  pointer-events: none; /* 确保不阻挡点击 */
}
/* ★★★ 新增：恢复内部内容的点击响应 ★★★ */
.overlay-content {
  pointer-events: auto; /* 恢复鼠标响应 */
  display: inline-block; /* 保持布局紧凑 */
}

.episode-count {
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  font-family: monospace; /* 等宽字体让数字对齐更好看 */
}

/* ★★★ 进度条分隔线 ★★★ */
.progress-separator {
  margin-top: auto; /* 将进度条推到底部 */
  padding-top: 8px;
  width: 100%;
  opacity: 0.8;
}

.card-poster {
  width: 100%;
  height: 100%;
  display: block;
}

.card-poster :deep(img) {
  width: 100%;
  height: 100%;
  /* 关键：Cover 模式，确保填满且不变形 */
  object-fit: cover !important; 
  display: block;
  border-radius: 0 !important;
}

.poster-placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  background-color: var(--n-action-color);
  color: var(--n-text-color-disabled);
}

/* ★★★ 内容区域 ★★★ */
.card-content-container {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-width: 0;
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: calc(4px * var(--card-scale, 1));
}

.card-title {
  font-weight: 600;
  /* 标题稍微大一点 */
  font-size: 1.1em !important; 
  line-height: 1.3;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.card-status-area {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  gap: 4px; /* 元素间距 */
}

.last-checked-text, .next-episode-text, .info-text {
  display: flex;
  align-items: center;
  gap: 4px;
  /* 辅助文字稍微小一点 */
  font-size: 0.9em !important; 
  line-height: 1.4;
  opacity: 0.8;
}

/* ★★★ 底部按钮区域 ★★★ */
.card-actions {
  margin-top: auto; 
  padding-top: 6px;
  display: flex;
  justify-content: center; 
  align-items: center;
  gap: calc(8px * var(--card-scale, 1));
}

/* 强制按钮变小以适应 */
.card-actions :deep(.n-button) {
  padding: 0 6px;
  height: 24px; /* 强制限制高度，防止撑开 */
  font-size: 0.9em !important;
}

/* 复选框 */
.card-checkbox {
  position: absolute;
  top: 6px;
  left: 6px;
  z-index: 10;
  background-color: rgba(255, 255, 255, 0.9);
  border-radius: 50%;
  padding: 2px;
  opacity: 0;
  transition: opacity 0.2s;
  box-shadow: 0 2px 5px rgba(0,0,0,0.2);
}

.series-card:hover .card-checkbox, 
.card-checkbox.n-checkbox--checked { 
  opacity: 1; 
  visibility: visible; 
}

.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}

.info-line {
  display: flex;
  align-items: center; /* 垂直居中对齐 */
  line-height: 1.5;    /* 适当的行高 */
}

.icon-fix {
  margin-right: 6px;   /* 图标和文字之间的间距 */
  font-size: 14px;     /* 调整图标大小与文字协调，根据需要微调 */
  color: var(--n-text-color-3); /* 让图标颜色也跟随 depth=3 变淡，或者直接删掉这行用默认色 */
  opacity: 0.6;        /* 或者用透明度来模拟 depth=3 的效果 */
}

/* ★★★ 修复进度条背景色在亮色模式下看不清的问题 ★★★ */
.progress-separator :deep(.n-progress-graph-line-rail) {
  /* 亮色模式：使用深一点的灰色 (黑色的 15% 透明度) */
  background-color: rgba(0, 0, 0, 0.15) !important; 
}

/* 暗色模式适配 */
html.dark .progress-separator :deep(.n-progress-graph-line-rail) {
  /* 暗色模式：使用浅一点的半透明白 (白色的 20% 透明度) */
  background-color: rgba(255, 255, 255, 0.2) !important;
}

.clickable-count {
  cursor: pointer;
  border-bottom: 1px dashed rgba(255,255,255,0.5); /* 加个虚线底边提示可点 */
  transition: all 0.2s;
}
.clickable-count:hover {
  color: var(--n-color-primary);
  border-bottom-color: var(--n-color-primary);
}

/* 手机端适配 */
@media (max-width: 600px) {
  .responsive-grid { grid-template-columns: 1fr !important; }
  .card-poster-container { width: 100px; min-height: 150px; }
}
</style>