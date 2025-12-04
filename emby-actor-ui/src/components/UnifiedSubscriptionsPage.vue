<!-- src/components/UnifiedSubscriptionsPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="unified-subscriptions-page">
      <n-page-header>
        <template #title>
          <n-space align="center">
            <span>统一订阅管理</span>
            <!-- 显示总数 -->
            <n-tag v-if="rawItems.length > 0" type="info" round :bordered="false" size="small">
              共 {{ rawItems.length }} 项
            </n-tag>
          </n-space>
        </template>
        <n-alert title="管理说明" type="info" style="margin-top: 24px;">
          <li>这里汇总了所有通过“用户请求”、“演员订阅”、“合集补全”、“智能追剧”等方式进入待处理队列，但尚未入库的媒体项。</li>
          <li><b>待订阅 (WANTED):</b> 点击“订阅”可立即提交给下载器。或等待后台“统一订阅任务”处理。</li>
          <li><b>已忽略 (IGNORED):</b> 被手动或规则忽略的项目，后台任务会自动跳过它们。</li>
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
            
            <!-- 状态筛选 (核心服务端过滤) -->
            <n-radio-group v-model:value="filterStatus" size="small">
              <n-radio-button value="WANTED">待订阅</n-radio-button>
              <n-radio-button value="SUBSCRIBED">已订阅</n-radio-button> 
              <n-radio-button value="PENDING_RELEASE">未上映</n-radio-button>
              <n-radio-button value="IGNORED">已忽略</n-radio-button>
            </n-radio-group>
            
            <n-tooltip>
              <template #trigger>
                <n-button @click="fetchData" :loading="isLoading" circle>
                  <template #icon><n-icon :component="SyncOutline" /></template>
                </n-button>
              </template>
              刷新列表
            </n-tooltip>
          </n-space>
        </template>
      </n-page-header>
      <n-divider />

      <!-- 客户端筛选工具栏 (针对已加载的数据) -->
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

      <!-- 列表区域 -->
      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <div v-else-if="filteredItems.length > 0 || rawItems.length > 0">
        
        <!-- Grid 容器 -->
        <div class="responsive-grid">
          <div 
            v-for="(item, i) in filteredItems" 
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

              <div class="card-inner-layout">
                <!-- 左侧：海报 -->
                <div class="card-poster-container">
                  <n-image lazy :src="getPosterUrl(item.poster_path)" class="card-poster" object-fit="cover" :intersection-observer-options="{ rootMargin: '200px' }">
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
                          忽略
                        </n-button>
                        <n-button @click="() => updateItemStatus(item, 'NONE')">
                          取消订阅
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
            </n-card>
          </div>
        </div>
        
        <!-- 已移除无限滚动触发器 -->
      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, h, computed, watch } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NTooltip, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup, NText } from 'naive-ui';
import { SyncOutline, TvOutline as TvIcon, FilmOutline as FilmIcon, CalendarOutline as CalendarIcon, TimeOutline as TimeIcon, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, CaretDownOutline as CaretDownIcon, CheckmarkCircleOutline as WantedIcon, HourglassOutline as PendingIcon, BanOutline as IgnoredIcon, DownloadOutline as SubscribedIcon, PersonCircleOutline as SourceIcon } from '@vicons/ionicons5';
import { format } from 'date-fns'

// 图标定义
const TMDbIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 512 512", width: "18", height: "18" }, [
  h('path', { d: "M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zM133.2 176.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zM133.2 262.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8z", fill: "#01b4e4" })
]);

const message = useMessage();
const dialog = useDialog();

// ★★★ 数据状态管理 (无分页) ★★★
const rawItems = ref([]); // 当前加载的所有数据
const isLoading = ref(false);
const error = ref(null);

const selectedItems = ref([]);
const lastSelectedIndex = ref(null);

// 筛选和排序状态
const searchQuery = ref('');
const filterStatus = ref('WANTED'); // 核心服务端筛选
const filterType = ref('all');
const filterSource = ref(null);
const sortKey = ref('first_requested_at');
const sortOrder = ref('desc');

const typeFilterOptions = [
  { label: '所有类型', value: 'all' },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' },
];
const sortKeyOptions = computed(() => [
  { 
    label: filterStatus.value === 'SUBSCRIBED' ? '按订阅时间' : '按请求时间', 
    value: 'first_requested_at' 
  },
  { label: '按媒体名称', value: 'title' },
  { label: '按发行日期', value: 'release_date' },
]);

const SOURCE_TYPE_MAP = {
  'user_request': '用户请求',
  'actor_subscription': '演员订阅',
  'collection': '自建合集',
  'native_collection': '原生合集',
  'manual_add': '手动添加',
  'watchlist': '智能追剧',
  'resubscribe': '自动洗版',
  'admin_approval': '管理员审核',
  'batch_admin_op': '批量处理',
  'api_unified_status_change_ignored': '手动忽略',
  'manual_admin_op': '手动处理',
  'auto_ignored': '自动忽略',
  'gap_scan': '缺集的季',
  'admin_rejection': '管理员拒绝'
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
    case 'SUBSCRIBED':
    case 'PENDING_RELEASE':
      return [
        { label: '批量忽略', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
        { label: '批量取消订阅', key: 'cancel', icon: () => h(NIcon, { component: TvIcon }) },
      ];
    case 'IGNORED':
      return [
        { label: '批量取消忽略', key: 'unignore', icon: () => h(NIcon, { component: WantedIcon }) },
      ];
    default:
      return [];
  }
});

// ★★★ 客户端筛选逻辑 (针对已加载的全量数据) ★★★
const filteredItems = computed(() => {
  let list = [...rawItems.value];

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

  // 客户端排序
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

const emptyStateDescription = computed(() => {
  if (rawItems.value.length > 0 && filteredItems.value.length === 0) {
    return '没有匹配当前筛选条件的媒体项。';
  }
  return '当前列表为空。';
});

// ★★★ 核心数据获取逻辑 (全量获取) ★★★
const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  selectedItems.value = []; // 刷新时清空选中，防止操作已消失的项
  
  try {
    // 调用接口，不再传递分页参数
    const response = await axios.get('/api/subscriptions/list', {
      params: {
        status: filterStatus.value, // 核心：传给后端进行过滤
      }
    });

    const { items } = response.data;
    // 直接全量替换
    rawItems.value = items;

  } catch (err) {
    error.value = err.response?.data?.error || '获取订阅列表失败。';
  } finally {
    isLoading.value = false;
  }
};

// 监听状态切换，重新加载
watch(filterStatus, () => {
  fetchData();
});

// --- 以下为辅助函数和操作逻辑 ---

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
    const itemsInRange = filteredItems.value.slice(start, end + 1); // 使用 filteredItems
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
        }

        message.success(response.data.message || '批量操作任务已提交！');
        
        // 乐观更新 UI
        const selectedKeys = new Set(selectedItems.value.map(item => `${item.tmdb_id}-${item.item_type}`));
        
        // 如果操作后的状态不等于当前过滤状态，则从列表中移除
        if (action.optimistic_status !== filterStatus.value) {
           rawItems.value = rawItems.value.filter(item => !selectedKeys.has(`${item.tmdb_id}-${item.item_type}`));
        } else {
           // 状态没变，或者只是更新属性
           rawItems.value.forEach(item => {
            if (selectedKeys.has(`${item.tmdb_id}-${item.item_type}`)) {
              item.subscription_status = action.optimistic_status;
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

    await axios.post('/api/tasks/run', {
      task_name: 'manual_subscribe_batch',
      subscribe_requests: [request_item]
    });
    message.success('订阅任务已提交到后台！');
    
    // 乐观更新
    if (filterStatus.value !== 'SUBSCRIBED') {
      const index = rawItems.value.findIndex(i => i.tmdb_id === item.tmdb_id && i.item_type === item.item_type);
      if (index > -1) {
        rawItems.value.splice(index, 1);
      }
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

    // 乐观更新
    if (newStatus !== filterStatus.value) {
      const index = rawItems.value.findIndex(i => i.tmdb_id === item.tmdb_id && i.item_type === item.item_type);
      if (index > -1) {
        rawItems.value.splice(index, 1);
      }
    }
  } catch (err) {
    message.error(err.response?.data?.error || '更新状态失败。');
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
  };
  return map[status] || { type: 'default', text: '未知', icon: TvIcon };
};

onMounted(() => {
  fetchData(); // 初始加载
});
</script>

<style scoped>
/* 保持原有样式不变 */
.watchlist-page, .unified-subscriptions-page { padding: 0 10px; }
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 200px); }

.responsive-grid {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fill, minmax(calc(320px * var(--card-scale, 1)), 1fr));
}

.grid-item {
  height: 100%;
  min-width: 0;
}

.series-card {
  cursor: pointer;
  transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
  height: 100%;
  position: relative;
  font-size: calc(14px * var(--card-scale, 1)); 
  border-radius: calc(12px * var(--card-scale, 1));
  overflow: hidden; 
  border: 1px solid var(--n-border-color);
}

.series-card:hover {
  transform: translateY(-4px);
}

.series-card :deep(.n-card__content),
.series-card :deep(.n-button),
.series-card :deep(.n-tag),
.series-card :deep(.n-text),
.series-card :deep(.n-ellipsis) {
  font-size: inherit !important; 
}

.series-card :deep(.n-icon) {
  font-size: 1.2em !important; 
}

.series-card.dashboard-card > :deep(.n-card__content) {
  padding: calc(10px * var(--card-scale, 1)) !important; 
  display: flex !important;
  flex-direction: column !important;
  height: 100% !important;
}

.card-inner-layout {
  display: flex;
  flex-direction: row;
  height: 100%;
  width: 100%;
  align-items: stretch; 
  gap: calc(12px * var(--card-scale, 1));
}

.card-poster-container {
  flex-shrink: 0; 
  width: calc(130px * var(--card-scale, 1));
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
  gap: 4px;
}

.info-text {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 0.9em !important; 
  line-height: 1.4;
  opacity: 0.8;
}

.card-actions {
  margin-top: auto; 
  padding-top: calc(8px * var(--card-scale, 1));
  border-top: 1px solid var(--n-border-color);
  display: flex;
  justify-content: center; 
  align-items: center;
  gap: calc(8px * var(--card-scale, 1));
}

.card-actions :deep(.n-button) {
  padding: 0 6px;
  height: 24px; 
  font-size: 0.9em !important;
}

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

@media (max-width: 600px) {
  .responsive-grid { grid-template-columns: 1fr !important; }
  .card-poster-container { width: 100px; min-height: 150px; }
}
</style>