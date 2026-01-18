<!-- src/components/UnifiedSubscriptionsPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="unified-subscriptions-page">
      <n-page-header>
        <template #title>
          <n-space align="center">
            <span>统一订阅管理</span>
            <n-tag v-if="filteredItems.length > 0" type="info" round :bordered="false" size="small">
              {{ filteredItems.length }} 项
            </n-tag>
          </n-space>
        </template>
        <n-alert title="管理说明" type="info" style="margin-top: 24px;">
          <li>这里汇总了所有通过“用户请求”、“演员订阅”、“合集补全”、“智能追剧”等方式进入待处理队列，但尚未入库的媒体项。</li>
          <li><b>待审核:</b> 在这里处理普通用户的订阅请求。</li>
          <li><b>待订阅:</b> 由自动化模块提交的订阅请求。</li>
          <li><b>已订阅:</b> 已经提交给MP订阅的媒体项。</li>
          <li><b>未上映:</b> 等待上映后，会自动订阅的项目。</li>
          <li><b>已忽略:</b> 被手动或规则忽略的项目，后台任务会自动跳过它们。</li>
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
            <n-popconfirm
              v-if="filterStatus === 'IGNORED' && filteredItems.length > 0"
              @positive-click="handleClearAllIgnored"
              negative-text="取消"
              positive-text="确定删除"
            >
              <template #trigger>
                <n-button type="error" ghost>
                  <template #icon><n-icon :component="TrashIcon" /></template>
                  清空当前列表 ({{ filteredItems.length }})
                </n-button>
              </template>
              确定要从数据库中<b style="color: red">物理删除</b>当前筛选出的 {{ filteredItems.length }} 条记录吗？<br/>
              <span style="font-size: 12px; color: gray;">删除后无法恢复，且下次扫描可能会再次发现这些项目。</span>
            </n-popconfirm>
            <n-radio-group v-model:value="filterStatus" size="small">
              <n-radio-button value="REQUESTED">待审核</n-radio-button>
              <n-radio-button value="WANTED">待订阅</n-radio-button>
              <n-radio-button value="SUBSCRIBED">已订阅</n-radio-button> 
              <n-radio-button value="PAUSED">已暂停</n-radio-button>
              <n-radio-button value="PENDING_RELEASE">未上映</n-radio-button>
              <n-radio-button value="IGNORED">已忽略</n-radio-button>
            </n-radio-group>
            <n-button @click="showStrategyModal = true" type="warning" ghost>
              <template #icon><n-icon :component="SettingsIcon" /></template>
              策略配置
            </n-button>
          </n-space>
        </template>
      </n-page-header>
      <n-divider />

      <n-space :wrap="true" :size="[20, 12]" style="margin-bottom: 20px;">
        <n-input v-model:value="searchQuery" placeholder="按名称搜索..." clearable style="min-width: 200px;" />
        <n-select v-model:value="filterType" :options="typeFilterOptions" style="min-width: 140px;" />
        <n-select v-model:value="filterSource" :options="sourceFilterOptions" style="min-width: 160px;" clearable placeholder="按来源筛选" />
        <n-select v-model:value="sortKey" :options="sortKeyOptions" style="min-width: 160px;" />
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
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <div v-else-if="filteredItems.length > 0">
        
        <!-- ★★★ Grid 容器 ★★★ -->
        <div class="responsive-grid">
          <div 
            v-for="(item, i) in renderedItems" 
            :key="item.tmdb_id + item.item_type" 
            class="grid-item"
          >
            <n-card class="dashboard-card series-card" :bordered="false">
              <!-- 绝对定位元素 -->
              <n-checkbox
                :checked="selectedItems.some(sel => sel.tmdb_id === item.tmdb_id && sel.item_type === item.item_type)"
                @update:checked="(checked, event) => toggleSelection(item, event, i)"
                class="card-checkbox"
              />
              <div class="card-type-icon">
                <n-tooltip trigger="hover">
                  <template #trigger>
                    <n-icon :component="item.item_type === 'Movie' ? FilmIcon : TvIcon" size="16" />
                  </template>
                  {{ item.item_type === 'Movie' ? '电影' : '剧集' }}
                </n-tooltip>
              </div>

              <!-- ★★★ 核心修复：必须有这个 card-inner-layout 包裹层，才能实现左右布局 ★★★ -->
              <div class="card-inner-layout">
                
                <!-- 左侧：海报 -->
                <div class="card-poster-container">
                  <n-image lazy :src="getPosterUrl(item.poster_path)" class="card-poster" object-fit="cover">
                    <template #placeholder><div class="poster-placeholder"><n-icon :component="TvIcon" size="32" /></div></template>
                  </n-image>
                </div>

                <!-- 右侧：内容 -->
                <div class="card-content-container">
                  <div class="card-header">
                    <n-ellipsis class="card-title" :tooltip="{ style: { maxWidth: '300px' } }">{{ item.title }}</n-ellipsis>
                  </div>
                  <div class="card-status-area">
                    <n-space vertical size="small">
                      <n-tag round size="tiny" :type="statusInfo(item.subscription_status).type">
                        <template #icon><n-icon :component="statusInfo(item.subscription_status).icon" /></template>
                        {{ statusInfo(item.subscription_status).text }}
                      </n-tag>
                      <n-tag v-if="item.subscription_status === 'IGNORED' && item.ignore_reason" type="error" size="small" round>
                        原因: {{ item.ignore_reason }}
                      </n-tag>
                      <n-text :depth="3" class="info-text">
                        <n-icon :component="CalendarIcon" /> {{ formatAirDate(item.release_date) }}
                      </n-text>
                      <n-text v-if="item.subscription_status === 'SUBSCRIBED'" :depth="3" class="info-text">
                        <n-icon :component="TimeIcon" /> 订阅于: {{ formatTimestamp(item.last_subscribed_at) }}
                      </n-text>
                      <n-text v-else-if="item.subscription_status === 'PAUSED'" :depth="3" class="info-text" style="color: var(--n-warning-color);">
                        <n-icon :component="TimeIcon" /> 复活于: {{ formatTimestamp(item.paused_until) }}
                      </n-text>
                      <n-text v-else :depth="3" class="info-text">
                        <n-icon :component="TimeIcon" /> 请求于: {{ formatTimestamp(item.first_requested_at) }}
                      </n-text>
                      <n-ellipsis :tooltip="{ style: { maxWidth: '300px' } }" :line-clamp="1" class="info-text">
                        <n-icon :component="SourceIcon" /> {{ formatSources(item.subscription_sources_json) }}
                      </n-ellipsis>
                    </n-space>
                  </div>
                  
                  <!-- 底部按钮 -->
                  <div class="card-actions">
                    <n-button-group size="small">
                      <template v-if="item.subscription_status === 'REQUESTED'">
                        <n-button @click="() => subscribeItem(item)" type="primary" ghost>
                          批准
                        </n-button>
                        <n-button @click="() => updateItemStatus(item, 'IGNORED')" type="error" ghost>
                          忽略
                        </n-button>
                      </template>
                      <template v-if="item.subscription_status === 'WANTED'">
                        <n-button @click="() => subscribeItem(item)" type="primary" ghost>
                          订阅
                        </n-button>
                        <n-button @click="() => updateItemStatus(item, 'IGNORED')" type="error" ghost>
                          忽略
                        </n-button>
                      </template>

                      <template v-else-if="item.subscription_status === 'SUBSCRIBED' || item.subscription_status === 'PENDING_RELEASE'">
                        <n-button @click="() => updateItemStatus(item, 'IGNORED')" type="error" ghost>
                          取消订阅 (忽略)
                        </n-button>
                      </template>

                      <template v-else-if="item.subscription_status === 'PAUSED'">
                        <n-button @click="() => updateItemStatus(item, 'SUBSCRIBED')" type="primary" ghost>
                          恢复搜索
                        </n-button>
                        <n-button @click="() => updateItemStatus(item, 'IGNORED')" type="error" ghost>
                          取消订阅 (忽略)
                        </n-button>
                      </template>

                      <template v-else-if="item.subscription_status === 'IGNORED'">
                        <n-button @click="() => updateItemStatus(item, 'WANTED', true)" type="primary" ghost>
                          取消忽略
                        </n-button>
                      </template>
                    </n-button-group>
                    <n-tooltip>
                      <template #trigger><n-button text tag="a" :href="getTMDbLink(item)" target="_blank"><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button></template>
                      在 TMDb 中打开
                    </n-tooltip>
                  </div>
                </div>
              </div>
              <!-- 布局结束 -->

            </n-card>
          </div>
        </div>
        
        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>
      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>
    <n-modal v-model:show="showStrategyModal" preset="card" title="订阅策略配置" style="width: 600px;">
      <n-form label-placement="left" label-width="auto" require-mark-placement="right-hanging">
        
        <n-divider title-placement="left">电影订阅策略 (剧集由智能追剧策略管理)</n-divider>
        <n-alert type="info" :show-icon="false" style="margin-bottom: 16px;">
          <li>新片，采用“搜索 N 天 -> 暂停 M 天”的循环机制，大幅降低 MoviePilot 搜索压力。</li>
          <li>老片，采用“搜索 N 天 -> 取消订阅”</li>
        </n-alert>
        
        <n-form-item label="新片保护期 (天)">
          <n-input-number v-model:value="strategyConfig.movie_protection_days" :min="0" />
          <template #feedback>发布时间在此天数内的电影启用间歇性搜索机制。超过此天数则视为老片，不再暂停，直接取消订阅。</template>
        </n-form-item>
        
        <n-form-item label="搜索窗口期 (天)">
          <n-input-number v-model:value="strategyConfig.movie_search_window_days" :min="1" />
          <template #feedback>新增订阅以及每次复活后，连续搜索的天数 (建议 1 天)。</template>
        </n-form-item>
        
        <n-form-item label="暂停周期 (天)">
          <n-input-number v-model:value="strategyConfig.movie_pause_days" :min="1" />
          <template #feedback>搜索无果后，暂停搜索的天数 (建议 7 天)。</template>
        </n-form-item>

        <n-form-item label="延迟订阅 (天)">
          <n-input-number v-model:value="strategyConfig.delay_subscription_days" :min="0" />
          <template #feedback>电影上映后 N 天才允许订阅 (0 表示不延迟)。</template>
        </n-form-item>

        <n-form-item label="超时复活 (天)">
          <n-input-number v-model:value="strategyConfig.timeout_revive_days" :min="0" />
          <template #feedback>
            因“订阅超时”被移除的项目，在 N 天后自动复活并重新尝试订阅。<br/>
            <b>0 表示不复活 (默认)</b>。适用于给老片或冷门资源第二次机会。
          </template>
        </n-form-item>
      </n-form>
      
      <template #footer>
        <n-space justify="end">
          <n-button @click="showStrategyModal = false">取消</n-button>
          <n-button type="primary" @click="saveStrategyConfig" :loading="savingStrategy">保存配置</n-button>
        </n-space>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, h, computed, watch } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NTooltip, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup } from 'naive-ui';
import { FilmOutline as FilmIcon, TvOutline as TvIcon, CalendarOutline as CalendarIcon, TimeOutline as TimeIcon, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, CaretDownOutline as CaretDownIcon, CheckmarkCircleOutline as WantedIcon, HourglassOutline as PendingIcon, BanOutline as IgnoredIcon, DownloadOutline as SubscribedIcon, PersonCircleOutline as SourceIcon, TrashOutline as TrashIcon, SettingsOutline as SettingsIcon, PauseCircleOutline as PausedIcon, ReaderOutline as AuditIcon } from '@vicons/ionicons5';
import { format } from 'date-fns'

// 图标定义
const TMDbIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 512 512", width: "18", height: "18" }, [
  h('path', { d: "M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zM133.2 176.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zM133.2 262.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8z", fill: "#01b4e4" })
]);

const message = useMessage();
const dialog = useDialog();

const rawItems = ref([]);
const isLoading = ref(true);
const error = ref(null);
const displayCount = ref(30);
const INCREMENT = 30;
const loaderRef = ref(null);
let observer = null;

const selectedItems = ref([]);
const lastSelectedIndex = ref(null);

// 筛选和排序状态
const searchQuery = ref('');
const filterStatus = ref('REQUESTED');
const filterType = ref('all');
const filterSource = ref(null);
const sortKey = ref('first_requested_at');
const sortOrder = ref('desc');

const showStrategyModal = ref(false);
const savingStrategy = ref(false);
const strategyConfig = ref({
  movie_protection_days: 180,
  movie_search_window_days: 1,
  movie_pause_days: 7,
  delay_subscription_days: 0,
  timeout_revive_days: 0,
});

// 加载配置
const loadStrategyConfig = async () => {
  try {
    const res = await axios.get('/api/subscription/strategy');
    strategyConfig.value = res.data;
  } catch (e) {
    message.error('加载策略配置失败');
  }
};

// 保存配置
const saveStrategyConfig = async () => {
  savingStrategy.value = true;
  try {
    await axios.post('/api/subscription/strategy', strategyConfig.value);
    message.success('策略配置已保存');
    showStrategyModal.value = false;
  } catch (e) {
    message.error('保存失败');
  } finally {
    savingStrategy.value = false;
  }
};

const typeFilterOptions = [
  { label: '所有类型', value: 'all' },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Season' },
];
const sortKeyOptions = computed(() => [
  { 
    label: filterStatus.value === 'SUBSCRIBED' ? '按订阅时间' : '按请求时间', 
    value: 'first_requested_at' // value 保持不变，作为排序逻辑的 key
  },
  { label: '按媒体名称', value: 'title' },
  { label: '按发行日期', value: 'release_date' },
]);

const SOURCE_TYPE_MAP = {
  'user_request': '用户请求',
  'actor_subscription': '演员订阅',
  'custom_collection': '自建合集',
  'native_collection': '原生合集',
  'manual_add': '手动添加',
  'revive_from_timeout': '超时复活',
  'watchlist': '智能追剧',
  'resubscribe': '自动洗版',
  'manual_ignore': '手动忽略',
  'manual_subscribe': '手动订阅',
  'manual_admin_op': '手动处理',
  'auto_ignored': '自动忽略',
  'gap_scan': '缺集的季',
  'scan_old_seasons_backfill': '补全旧季'
};

const sourceFilterOptions = computed(() => {
  const sources = new Set();
  rawItems.value.forEach(item => {
    item.subscription_sources_json?.forEach(source => {
      if (source.type) {
        sources.add(source.type);
      }
    });
  });
  const options = Array.from(sources).map(type => ({
    label: SOURCE_TYPE_MAP[type] || type,
    value: type
  }));
  options.sort((a, b) => a.label.localeCompare(b.label));
  return options;
});

const batchActions = computed(() => {
  switch (filterStatus.value) {
    case 'WANTED':
      return [
        { label: '批量订阅', key: 'subscribe', icon: () => h(NIcon, { component: SubscribedIcon }) },
        { label: '批量忽略', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
      ];
    case 'REQUESTED': 
      return [
        { label: '批量批准', key: 'subscribe', icon: () => h(NIcon, { component: SubscribedIcon }) },
        { label: '批量忽略', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
      ];
    case 'SUBSCRIBED':
    case 'PENDING_RELEASE':
      return [
        { label: '批量取消订阅 (忽略)', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
      ];
    case 'PAUSED':
      return [
        { label: '批量恢复', key: 'resume', icon: () => h(NIcon, { component: SubscribedIcon }) }, 
        { label: '批量取消订阅 (忽略)', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
      ];
    case 'IGNORED':
      return [
        { label: '批量取消忽略', key: 'unignore', icon: () => h(NIcon, { component: WantedIcon }) },
      ];
    default:
      return [];
  }
});

const filteredItems = computed(() => {
  let list = rawItems.value.filter(item => item.subscription_status === filterStatus.value);

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    list = list.filter(item => item.title.toLowerCase().includes(query));
  }

  if (filterType.value !== 'all') {
    list = list.filter(item => item.item_type === filterType.value);
  }

  if (filterSource.value) {
    list = list.filter(item => 
      item.subscription_sources_json?.some(source => source.type === filterSource.value)
    );
  }

  list.sort((a, b) => {
    let valA, valB;
    switch (sortKey.value) {
      case 'title':
        valA = a.title || '';
        valB = b.title || '';
        return sortOrder.value === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      
      case 'release_date':
        valA = a.release_date ? new Date(a.release_date).getTime() : 0;
        valB = b.release_date ? new Date(b.release_date).getTime() : 0;
        return sortOrder.value === 'asc' ? valA - valB : valB - valA;

      case 'first_requested_at':
      default:
        valA = (a.subscription_status === 'SUBSCRIBED' && a.last_subscribed_at)
          ? new Date(a.last_subscribed_at).getTime()
          : (a.first_requested_at ? new Date(a.first_requested_at).getTime() : 0);
        
        valB = (b.subscription_status === 'SUBSCRIBED' && b.last_subscribed_at)
          ? new Date(b.last_subscribed_at).getTime()
          : (b.first_requested_at ? new Date(b.first_requested_at).getTime() : 0);
          
        return sortOrder.value === 'asc' ? valA - valB : valB - valA;
    }
  });

  return list;
});

const renderedItems = computed(() => filteredItems.value.slice(0, displayCount.value));
const hasMore = computed(() => displayCount.value < filteredItems.value.length);
const emptyStateDescription = computed(() => {
  if (rawItems.value.length > 0 && filteredItems.value.length === 0) {
    return '没有匹配当前筛选条件的媒体项。';
  }
  return '当前列表为空。';
});

const getTMDbLink = (item) => {
  if (item.item_type === 'Movie') {
    return `https://www.themoviedb.org/movie/${item.tmdb_id}`;
  }
  if (item.series_tmdb_id) {
    return `https://www.themoviedb.org/tv/${item.series_tmdb_id}`;
  }
  return `https://www.themoviedb.org/`;
};

const toggleSelection = (item, event, index) => {
  if (!event) return;
  const key = { tmdb_id: item.tmdb_id, item_type: item.item_type };
  
  if (event.shiftKey && lastSelectedIndex.value !== null) {
    const start = Math.min(lastSelectedIndex.value, index);
    const end = Math.max(lastSelectedIndex.value, index);
    const itemsInRange = renderedItems.value.slice(start, end + 1);
    const isCurrentlySelected = selectedItems.value.some(sel => sel.tmdb_id === key.tmdb_id && sel.item_type === key.item_type);
    
    if (!isCurrentlySelected) {
      const newSelected = [...selectedItems.value];
      itemsInRange.forEach(rangeItem => {
        if (!newSelected.some(sel => sel.tmdb_id === rangeItem.tmdb_id && sel.item_type === rangeItem.item_type)) {
          newSelected.push({ tmdb_id: rangeItem.tmdb_id, item_type: rangeItem.item_type });
        }
      });
      selectedItems.value = newSelected;
    } else {
      const idsToRemove = new Set(itemsInRange.map(i => `${i.tmdb_id}-${i.item_type}`));
      selectedItems.value = selectedItems.value.filter(sel => !idsToRemove.has(`${sel.tmdb_id}-${sel.item_type}`));
    }
  } else {
    const idx = selectedItems.value.findIndex(sel => sel.tmdb_id === key.tmdb_id && sel.item_type === key.item_type);
    if (idx > -1) {
      selectedItems.value.splice(idx, 1);
    } else {
      selectedItems.value.push(key);
    }
  }
  lastSelectedIndex.value = index;
};

const handleBatchAction = (key) => {
  const actionMap = {
    'subscribe': { 
      title: '批量订阅', 
      content: `确定要将选中的 ${selectedItems.value.length} 个媒体项提交到后台订阅吗？`, 
      task_name: 'manual_subscribe_batch',
      getParams: () => {
        const fullSelectedItems = rawItems.value.filter(item => 
          selectedItems.value.some(sel => sel.tmdb_id === item.tmdb_id && sel.item_type === item.item_type)
        );
        return { subscribe_requests: fullSelectedItems };
      },
      optimistic_status: 'SUBSCRIBED'
    },
    'ignore': { 
      title: '批量忽略', 
      content: `确定要忽略选中的 ${selectedItems.value.length} 个媒体项吗？`, 
      endpoint: '/api/subscription/status', 
      getParams: () => ({ requests: selectedItems.value.map(item => ({...item, new_status: 'IGNORED', ignore_reason: '手动忽略'})) }),
      optimistic_status: 'IGNORED'
    },
    'cancel': { 
      title: '批量取消', 
      content: `确定要取消订阅选中的 ${selectedItems.value.length} 个媒体项吗？`, 
      endpoint: '/api/subscription/status',
      getParams: () => ({ requests: selectedItems.value.map(item => ({...item, new_status: 'NONE'})) }),
      optimistic_status: 'NONE'
    },
    'resume': { 
      title: '批量恢复', 
      content: `确定要立即唤醒选中的 ${selectedItems.value.length} 个媒体项并重新开始搜索吗？`, 
      endpoint: '/api/subscription/status',
      // 恢复 = 设置为 SUBSCRIBED
      getParams: () => ({ requests: selectedItems.value.map(item => ({...item, new_status: 'SUBSCRIBED'})) }),
      optimistic_status: 'SUBSCRIBED'
    },
    'unignore': { 
      title: '批量取消忽略', 
      content: `确定要取消忽略选中的 ${selectedItems.value.length} 个媒体项吗？`, 
      endpoint: '/api/subscription/status',
      getParams: () => ({ requests: selectedItems.value.map(item => ({...item, new_status: 'WANTED', force_unignore: true})) }),
      optimistic_status: 'WANTED'
    },
  };

  const action = actionMap[key];
  if (!action) return;

  dialog.warning({
    title: action.title,
    content: action.content,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        let response;
        if (action.task_name) {
          response = await axios.post('/api/tasks/run', {
            task_name: action.task_name,
            ...action.getParams()
          });
        } else if (action.endpoint) {
          response = await axios.post(action.endpoint, action.getParams());
        } else {
          throw new Error("Action spec 未定义 task_name 或 endpoint");
        }

        message.success(response.data.message || '批量操作任务已提交！');
        
        const selectedKeys = new Set(selectedItems.value.map(item => `${item.tmdb_id}-${item.item_type}`));
        
        if (action.optimistic_status === 'NONE') {
          rawItems.value = rawItems.value.filter(item => !selectedKeys.has(`${item.tmdb_id}-${item.item_type}`));
        } else {
          rawItems.value.forEach(item => {
            if (selectedKeys.has(`${item.tmdb_id}-${item.item_type}`)) {
              item.subscription_status = action.optimistic_status;
              if (action.optimistic_status === 'IGNORED') {
                item.ignore_reason = '手动忽略';
              }
            }
          });
        }
        
        selectedItems.value = [];

      } catch (err) {
        message.error(err.response?.data?.error || '批量操作失败。');
      }
    }
  });
};

const subscribeItem = async (item) => {
  try {
    const request_item = { 
      tmdb_id: item.tmdb_id, 
      item_type: item.item_type,
      title: item.title
    };
    if (item.item_type === 'Season' && item.season_number) {
      request_item.season_number = item.season_number;
    }

    const taskParams = {
      subscribe_requests: [request_item]
    };
    
    const response = await axios.post('/api/tasks/run', {
      task_name: 'manual_subscribe_batch',
      ...taskParams
    });
    message.success(response.data.message || '订阅任务已提交到后台！');
    
    const index = rawItems.value.findIndex(i => i.tmdb_id === item.tmdb_id && i.item_type === item.item_type);
    if (index > -1) {
      rawItems.value[index].subscription_status = 'SUBSCRIBED';
    }
  } catch (err) {
    message.error(err.response?.data?.error || '提交订阅任务失败。');
  }
};

const updateItemStatus = async (item, newStatus, forceUnignore = false) => {
  try {
    const requestItem = {
      tmdb_id: item.tmdb_id,
      item_type: item.item_type,
      new_status: newStatus,
      source: { type: 'manual_admin_op' },
      force_unignore: forceUnignore
    };
    
    if (newStatus === 'IGNORED') {
      requestItem.ignore_reason = '手动忽略';
    }

    await axios.post('/api/subscription/status', { requests: [requestItem] });
    message.success('状态更新成功！');

    const index = rawItems.value.findIndex(i => i.tmdb_id === item.tmdb_id && i.item_type === item.item_type);
    if (index > -1) {
      if (newStatus === 'NONE') {
        rawItems.value.splice(index, 1);
      } else {
        rawItems.value[index].subscription_status = newStatus;
        if (newStatus === 'IGNORED') {
          rawItems.value[index].ignore_reason = '手动忽略';
        }
      }
    }
  } catch (err) {
    message.error(err.response?.data?.error || '更新状态失败。');
  }
};

watch(filterStatus, () => {
  displayCount.value = 30;
  selectedItems.value = [];
  lastSelectedIndex.value = null;
});

const loadMore = () => {
  if (hasMore.value) {
    displayCount.value = Math.min(displayCount.value + INCREMENT, filteredItems.value.length);
  }
};

const formatTimestamp = (timestamp) => {
  if (!timestamp) return 'N/A';
  try {
    return format(new Date(timestamp), 'yyyy-MM-dd');
  } catch (e) { return 'N/A'; }
};

const formatSources = (sources) => {
  if (!sources || sources.length === 0) return '来源: 未知';
  const firstSource = sources[0];
  const typeText = SOURCE_TYPE_MAP[firstSource.type] || firstSource.type;
  const detail = firstSource.user || firstSource.name || firstSource.collection_name || '';
  return `来源: ${typeText}${detail ? ` - ${detail}` : ''}`;
};

// ★★★ 新增：处理一键清空逻辑 ★★★
const handleClearAllIgnored = async () => {
  // 1. 获取当前筛选出的所有项目的 ID
  const itemsToDelete = filteredItems.value.map(item => ({
    tmdb_id: item.tmdb_id,
    item_type: item.item_type
  }));

  if (itemsToDelete.length === 0) return;

  try {
    isLoading.value = true; // 显示加载状态
    
    // 2. 发送请求
    const response = await axios.post('/api/media/batch_delete', {
      items: itemsToDelete
    });

    message.success(response.data.message || '删除成功');

    // 3. 更新本地数据 (Optimistic UI Update)
    // 创建一个 Set 用于快速查找已删除的 ID
    const deletedKeys = new Set(itemsToDelete.map(i => `${i.tmdb_id}-${i.item_type}`));
    
    // 从 rawItems 中移除这些项
    rawItems.value = rawItems.value.filter(item => 
      !deletedKeys.has(`${item.tmdb_id}-${item.item_type}`)
    );
    
    // 清空选择
    selectedItems.value = [];

  } catch (err) {
    message.error(err.response?.data?.error || '批量删除失败');
  } finally {
    isLoading.value = false;
  }
};

const formatAirDate = (dateString) => {
  if (!dateString) return 'N/A';
  try {
    return format(new Date(dateString), 'yyyy-MM-dd');
  } catch (e) { return 'N/A'; }
};

const getPosterUrl = (posterPath) => posterPath ? `/api/image_proxy?url=https://image.tmdb.org/t/p/w500${posterPath}` : '/placeholder.png';

const statusInfo = (status) => {
  const map = {
    'WANTED': { type: 'success', text: '待订阅', icon: WantedIcon },
    'SUBSCRIBED': { type: 'primary', text: '已订阅', icon: SubscribedIcon },
    'PENDING_RELEASE': { type: 'info', text: '未上映', icon: PendingIcon },
    'IGNORED': { type: 'error', text: '已忽略', icon: IgnoredIcon },
    'PAUSED': { type: 'warning', text: '已暂停', icon: PausedIcon },
    'REQUESTED': { type: 'warning', text: '待审核', icon: AuditIcon },
  };
  return map[status] || { type: 'default', text: '未知', icon: TvIcon };
};


const TAB_PRIORITY = ['REQUESTED', 'WANTED', 'SUBSCRIBED', 'PAUSED', 'PENDING_RELEASE', 'IGNORED'];
const fetchData = async (autoSwitchTab = false) => {
  isLoading.value = true;
  error.value = null;
  try {
    const response = await axios.get('/api/subscriptions/all');
    rawItems.value = response.data;

    // ★★★ 新增：自动切换到第一个有内容的标签页 ★★★
    if (autoSwitchTab) {
      for (const status of TAB_PRIORITY) {
        // 检查当前状态是否有对应的媒体项
        const hasItem = rawItems.value.some(item => item.subscription_status === status);
        if (hasItem) {
          filterStatus.value = status;
          break; // 找到优先级最高的有内容的标签后，立即停止
        }
      }
      // 如果所有状态都没数据，默认会停留在初始值（通常是 REQUESTED 或 WANTED）
    }

  } catch (err) {
    error.value = err.response?.data?.error || '获取订阅列表失败。';
  } finally {
    isLoading.value = false;
  }
};

onMounted(() => {
  // 传入 true，表示这是首次加载，需要自动判断标签页
  fetchData(true); 
  
  loadStrategyConfig();
  observer = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting) loadMore();
    },
    { root: null, rootMargin: '0px', threshold: 0.1 }
  );
  if (loaderRef.value) observer.observe(loaderRef.value);
});

onBeforeUnmount(() => {
  if (observer) observer.disconnect();
});

watch(loaderRef, (newEl, oldEl) => {
  if (oldEl && observer) observer.unobserve(oldEl);
  if (newEl && observer) observer.observe(newEl);
});
</script>

<style scoped>
/* 页面基础 */
.watchlist-page, .unified-subscriptions-page { padding: 0 10px; }
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

.content-top {
  display: flex;
  flex-direction: column;
  gap: calc(4px * var(--card-scale, 1));
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
.card-actions, .card-actions-bottom {
  margin-top: auto; 
  padding-top: calc(8px * var(--card-scale, 1));
  border-top: 1px solid var(--n-border-color);
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

.card-type-icon {
  position: absolute;
  top: 6px;
  right: 6px;
  z-index: 10;
  background-color: rgba(0, 0, 0, 0.6);
  color: white;
  border-radius: 4px;
  padding: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  line-height: 1;
  backdrop-filter: blur(2px);
}

.series-card:hover .card-checkbox, 
.card-checkbox.n-checkbox--checked { 
  opacity: 1; 
  visibility: visible; 
}

/* 信息网格 (ResubscribePage用) */
.meta-info-grid {
  display: flex;
  flex-direction: column;
  gap: 2px;
  margin-top: calc(4px * var(--card-scale, 1));
}

.ellipsis {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
}

/* 印章样式 */
.poster-stamp {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%) rotate(-15deg);
  font-weight: 900;
  font-size: 1.1em !important; 
  letter-spacing: 1px;
  padding: 2px 8px;
  border-radius: 6px;
  background-color: rgba(255, 255, 255, 0.95);
  box-shadow: 0 4px 15px rgba(0,0,0,0.3);
  z-index: 5;
  pointer-events: none;
  white-space: nowrap;
  border: 3px solid;
}
.stamp-needed { border-color: #d03050; color: #d03050; }
.stamp-ignored { border-color: #888; color: #888; transform: translate(-50%, -50%) rotate(10deg); font-size: 1rem; }
.stamp-subscribed { border-color: #2080f0; color: #2080f0; transform: translate(-50%, -50%) rotate(-5deg); }
.stamp-auto { border-color: #8a2be2; color: #8a2be2; transform: translate(-50%, -50%) rotate(5deg); }

/* 状态文字颜色 */
.reason-text-wrapper { display: flex; align-items: center; gap: 4px; font-size: 0.9em !important; font-weight: 500; }
.text-needed { color: #d03050; }
.text-ignored { color: #999; text-decoration: line-through; }
.text-subscribed { color: #2080f0; }
.text-auto { color: #8a2be2; }

.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}

/* 手机端适配 */
@media (max-width: 600px) {
  .responsive-grid { grid-template-columns: 1fr !important; }
  .card-poster-container { width: 100px; min-height: 150px; }
}
</style>