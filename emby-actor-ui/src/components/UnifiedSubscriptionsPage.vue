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
          <li>这里汇总了所有通过“用户请求”、“演员订阅”、“合集补全”等方式进入待处理队列，但尚未入库的媒体项。</li>
          <li><b>待订阅 (WANTED):</b> 等待后台“统一订阅任务”处理的项目。</li>
          <li><b>未上映 (PENDING):</b> 等待上映后，会自动转为“待订阅”的项目。</li>
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

      <n-space :wrap="true" :size="[20, 12]" style="margin-bottom: 20px;">
        <n-input v-model:value="searchQuery" placeholder="按名称搜索..." clearable style="min-width: 200px;" />
        <n-select v-model:value="filterType" :options="typeFilterOptions" style="min-width: 140px;" />
        <!-- ▼▼▼ 新增的来源筛选器 ▼▼▼ -->
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
        <n-grid cols="1 s:1 m:2 l:3 xl:4" :x-gap="20" :y-gap="20" responsive="screen">
          <n-gi v-for="(item, i) in renderedItems" :key="item.tmdb_id + item.item_type">
            <n-card class="dashboard-card series-card" :bordered="false">
              <n-checkbox
                :checked="selectedItems.some(sel => sel.tmdb_id === item.tmdb_id && sel.item_type === item.item_type)"
                @update:checked="(checked, event) => toggleSelection(item, event, i)"
                class="card-checkbox"
              />
              <div class="card-poster-container">
                <n-image lazy :src="getPosterUrl(item.poster_path)" class="card-poster" object-fit="cover">
                  <template #placeholder><div class="poster-placeholder"><n-icon :component="TvIcon" size="32" /></div></template>
                </n-image>
              </div>
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
                    <n-text :depth="3" class="info-text">
                      <n-icon :component="TimeIcon" /> 请求于: {{ formatTimestamp(item.first_requested_at) }}
                    </n-text>
                  </n-space>
                </div>
                <div class="card-actions">
                  <!-- ★★★ 核心修改：根据状态动态显示不同的按钮组 ★★★ -->
                  <n-button-group size="small">
                    
                    <!-- 规则1: 只有“已忽略”状态才显示“取消忽略”按钮 -->
                    <n-button 
                      v-if="item.subscription_status === 'IGNORED'" 
                      @click="() => updateItemStatus(item, 'WANTED', true)" 
                      type="primary" 
                      ghost>
                      取消忽略
                    </n-button>
                    
                    <!-- 规则2: 只要不是“已忽略”，就显示“忽略”按钮 -->
                    <n-button 
                      v-if="item.subscription_status !== 'IGNORED'" 
                      @click="() => updateItemStatus(item, 'IGNORED')" 
                      type="error" 
                      ghost>
                      忽略
                    </n-button>
                    
                    <!-- 规则3: “取消订阅”按钮总是显示，用于彻底移除 -->
                    <n-button @click="() => updateItemStatus(item, 'NONE')">
                      取消订阅
                    </n-button>

                  </n-button-group>
                  <n-tooltip>
                    <template #trigger><n-button text tag="a" :href="`https://www.themoviedb.org/${item.item_type === 'Movie' ? 'movie' : 'tv'}/${item.tmdb_id}`" target="_blank"><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button></template>
                    在 TMDb 中打开
                  </n-tooltip>
                </div>
              </div>
            </n-card>
          </n-gi>
        </n-grid>
        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>
      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, h, computed, watch } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NTooltip, NGrid, NGi, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup } from 'naive-ui';
import { SyncOutline, TvOutline as TvIcon, CalendarOutline as CalendarIcon, TimeOutline as TimeIcon, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, CaretDownOutline as CaretDownIcon, CheckmarkCircleOutline as WantedIcon, HourglassOutline as PendingIcon, BanOutline as IgnoredIcon, DownloadOutline as SubscribedIcon } from '@vicons/ionicons5';
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
const filterStatus = ref('WANTED');
const filterType = ref('all');
const filterSource = ref(null);
const sortKey = ref('first_requested_at');
const sortOrder = ref('desc');

const typeFilterOptions = [
  { label: '所有类型', value: 'all' },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' },
];
const sortKeyOptions = [
  { label: '按请求时间', value: 'first_requested_at' },
  { label: '按媒体名称', value: 'title' },
  { label: '按发行日期', value: 'release_date' },
];

const SOURCE_TYPE_MAP = {
  'user_request': '用户请求',
  'actor_subscription': '演员订阅',
  'collection': '自定义合集',
  'native_collection': '原生合集',
  'manual_add': '手动添加',
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
  // 按 label 排序
  options.sort((a, b) => a.label.localeCompare(b.label));
  return options;
});

const batchActions = computed(() => [
  { label: '批量忽略', key: 'ignore', icon: () => h(NIcon, { component: IgnoredIcon }) },
  { label: '批量取消订阅', key: 'cancel', icon: () => h(NIcon, { component: TvIcon }) },
]);

const filteredItems = computed(() => {
  let list = rawItems.value.filter(item => item.subscription_status === filterStatus.value);

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    list = list.filter(item => item.title.toLowerCase().includes(query));
  }

  if (filterType.value !== 'all') {
    list = list.filter(item => item.item_type === filterType.value);
  }

  // 应用新的来源筛选
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
        valA = a.release_date || '0';
        valB = b.release_date || '0';
        return sortOrder.value === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      case 'first_requested_at':
      default:
        valA = a.first_requested_at ? new Date(a.first_requested_at).getTime() : 0;
        valB = b.first_requested_at ? new Date(b.first_requested_at).getTime() : 0;
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
    'ignore': { title: '批量忽略', content: `确定要忽略选中的 ${selectedItems.value.length} 个媒体项吗？`, new_status: 'IGNORED' },
    'cancel': { title: '批量取消', content: `确定要取消订阅选中的 ${selectedItems.value.length} 个媒体项吗？`, new_status: 'NONE' },
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
        const requests = selectedItems.value.map(item => ({
          tmdb_id: item.tmdb_id,
          item_type: item.item_type,
          new_status: action.new_status,
          source: { type: 'batch_admin_op' }
        }));
        const response = await axios.post('/api/subscription/status', { requests });
        message.success(response.data.message || '批量操作成功！');
        await fetchData();
        selectedItems.value = [];
      } catch (err) {
        message.error(err.response?.data?.error || '批量操作失败。');
      }
    }
  });
};

const updateItemStatus = async (item, newStatus, forceUnignore = false) => {
  try {
    const requests = [{
      tmdb_id: item.tmdb_id,
      item_type: item.item_type,
      new_status: newStatus,
      source: { type: 'manual_admin_op' },
      force_unignore: forceUnignore
    }];
    await axios.post('/api/subscription/status', { requests });
    message.success('状态更新成功！');
    // 乐观更新UI
    const index = rawItems.value.findIndex(i => i.tmdb_id === item.tmdb_id && i.item_type === item.item_type);
    if (index > -1) {
      if (newStatus === 'NONE') {
        rawItems.value.splice(index, 1);
      } else {
        rawItems.value[index].subscription_status = newStatus;
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
    // 使用 new Date()，它对多种格式更宽容
    return format(new Date(timestamp), 'yyyy-MM-dd HH:mm');
  } catch (e) { return 'N/A'; }
};

const formatAirDate = (dateString) => {
  if (!dateString) return 'N/A';
  try {
    // 使用 new Date()
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

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  try {
    const response = await axios.get('/api/subscriptions/all');
    rawItems.value = response.data;
  } catch (err) {
    error.value = err.response?.data?.error || '获取订阅列表失败。';
  } finally {
    isLoading.value = false;
  }
};

onMounted(() => {
  fetchData();
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
.watchlist-page { padding: 0 10px; }
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 200px); }
/* 卡片样式，为 checkbox 定位做准备 */
.series-card {
  position: relative;
}
/* 【修改】Checkbox 样式，默认隐藏，鼠标悬浮或已选中时显示 */
.card-checkbox {
  position: absolute;
  top: 8px;
  left: 8px;
  z-index: 10;
  background-color: rgba(255, 255, 255, 0.7);
  border-radius: 50%;
  padding: 4px;
  --n-color-checked: var(--n-color-primary-hover);
  --n-border-radius: 50%;
  /* 默认隐藏并添加过渡效果 */
  opacity: 0;
  visibility: hidden;
  transition: opacity 0.2s ease-in-out, visibility 0.2s ease-in-out;
}
/* 鼠标悬浮于卡片上时，或当多选框自身被勾选时，显示它 */
/* 注意: .n-checkbox--checked 是 Naive UI 内部用于标记“已选中”状态的类 */
.series-card:hover .card-checkbox,
.card-checkbox.n-checkbox--checked {
  opacity: 1;
  visibility: visible;
}
/* 【终极修复】为海报容器添加 overflow: hidden，裁剪掉溢出的图片部分，防止其挤压右侧内容 */
.card-poster-container {
  flex-shrink: 0;
  width: 160px;
  height: 240px;
  overflow: hidden;
}
.card-poster {
  width: 100%;
  height: 100%;
}
.poster-placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  background-color: var(--n-action-color);
}
/* 【布局优化】减小右侧内边距，给内容更多空间 */
.card-content-container {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  padding: 12px 8px 12px 0;
  min-width: 0;
}
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
  flex-shrink: 0;
}
.card-title {
  font-weight: 600;
  font-size: 1.1em;
  line-height: 1.3;
}
.card-status-area {
  flex-grow: 1;
  padding-top: 8px;
}
.last-checked-text {
  display: block;
  font-size: 0.8em;
  margin-top: 6px;
}
.next-episode-text {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 0.8em;
}
/* 【最终优化】将按钮改为环绕对齐，使其均匀分布 */
.card-actions {
  border-top: 1px solid var(--n-border-color);
  padding-top: 8px;
  margin-top: 8px;
  display: flex;
  justify-content: space-around;
  align-items: center;
  flex-shrink: 0;
}
.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}
/*
  【布局终极修正】
  此样式块专门用于对抗 .dashboard-card 的全局布局设置。
  它使用 :deep() 来穿透组件，并用 !important 强制覆盖，
  确保追剧列表的卡片内容区（.n-card__content）采用我们期望的水平布局。
*/
.series-card.dashboard-card > :deep(.n-card__content) {
  /* 核心：强制将 flex 方向从全局的 "column" 改为 "row" */
  flex-direction: row !important;
  /* 
    重置对齐方式。
    全局的 "space-between" 在水平布局下会导致元素被拉开，
    我们把它改回默认的起始对齐。
  */
  justify-content: flex-start !important;
  /* 
    重置内边距和间距，以匹配你在 template 中最初的设定。
    这确保了海报和右侧内容区之间有正确的空隙。
  */
  padding: 12px !important;
  gap: 12px !important;
}
</style>