<!-- src/components/ResubscribePage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="resubscribe-page">
      <n-page-header>
        <template #title>
          <n-space align="center">
            <span>媒体整理</span>
            <n-tag v-if="allItems.length > 0" type="info" round :bordered="false" size="small">
              {{ filteredItems.length }} / {{ allItems.length }} 项
            </n-tag>
          </n-space>
        </template>
        <n-alert title="操作提示" type="info" style="margin-top: 24px;">
          <li>先设定规则，然后点击刷新按钮扫描全库。</li>
          <li>点击 <b>“整理”</b> 按钮将根据匹配到的规则执行操作：可能是 <b>洗版订阅</b>，也可能是 <b>直接删除</b>（取决于规则设定）。</li>
          <li>按住 Shift 键可以进行多选。</li>
        </n-alert>
        <template #extra>
          <n-space>
            <n-dropdown 
              trigger="click"
              :options="batchActions"
              @select="handleBatchAction"
            >
              <n-button>
                批量操作 ({{ selectedItems.size }})
              </n-button>
            </n-dropdown>

            <n-radio-group v-model:value="filter" size="small">
              <n-radio-button value="all">全部</n-radio-button>
              <n-radio-button value="needed">需处理</n-radio-button>
              <n-radio-button value="auto">自动处理</n-radio-button>
              <n-radio-button value="ignored">已忽略</n-radio-button>
            </n-radio-group>
            <n-button @click="showSettingsModal = true">规则设定</n-button>
            <n-button type="warning" @click="triggerResubscribeAll" :loading="isTaskRunning('全库媒体洗版')">一键整理全部</n-button>
            <n-tooltip trigger="hover">
              <template #trigger>
                <n-button type="primary" @click="triggerRefreshStatus" :loading="isTaskRunning('刷新媒体整理')" circle>
                  <template #icon><n-icon :component="SyncOutline" /></template>
                </n-button>
              </template>
              扫描媒体库
            </n-tooltip>
          </n-space>
        </template>
      </n-page-header>

      <!-- 筛选栏 -->
      <n-space justify="space-between" align="center" style="margin-top: 24px; margin-bottom: -12px;">
        <n-input
          v-model:value="searchQuery"
          placeholder="按名称搜索..."
          clearable
          style="width: 240px;"
        />
        <n-space align="center">
          <n-select
            v-model:value="mediaTypeFilter"
            :options="mediaTypeOptions"
            placeholder="按类型筛选"
            style="width: 120px;"
          />
          <n-select
            v-model:value="ruleFilter"
            :options="ruleOptions"
            placeholder="按规则筛选"
            style="width: 200px;"
          />
          <n-select
            v-model:value="sortBy"
            :options="sortOptions"
            style="width: 150px;"
          />
          <n-button-group>
            <n-button @click="sortOrder = 'asc'" :type="sortOrder === 'asc' ? 'primary' : 'default'">
              <template #icon><n-icon :component="ArrowUpIcon" /></template>
              升序
            </n-button>
            <n-button @click="sortOrder = 'desc'" :type="sortOrder === 'desc' ? 'primary' : 'default'">
              <template #icon><n-icon :component="ArrowDownIcon" /></template>
              降序
            </n-button>
          </n-button-group>
        </n-space>
      </n-space>

      <n-divider />

      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <div v-else-if="displayedItems.length > 0">
      
      <!-- ★★★ Grid 容器 ★★★ -->
      <div class="responsive-grid">
        <div 
          v-for="(item, index) in displayedItems" 
          :key="item.item_id" 
          class="grid-item"
        >
          <n-card 
            class="dashboard-card series-card" 
            :bordered="false"
            :class="{ 'card-selected': selectedItems.has(item.item_id) }"
            @click="handleCardClick($event, item, index)"
          >
            <n-checkbox
              class="card-checkbox"
              :checked="selectedItems.has(item.item_id)"
            />
            
            <!-- ★★★ 核心结构：card-inner-layout 包裹层 ★★★ -->
            <div class="card-inner-layout">
              
              <!-- 左侧海报 -->
              <div class="card-poster-container" @click.stop="handleCardClick($event, item, index)">
                <n-image 
                  lazy 
                  :src="getPosterUrl(item)" 
                  class="card-poster" 
                  object-fit="cover"
                  preview-disabled
                >
                  <template #placeholder><div class="poster-placeholder"></div></template>
                </n-image>
                
                <!-- 印章 -->
                <div v-if="item.status === 'needed'" class="poster-stamp stamp-needed">需处理</div>
                <div v-else-if="item.status === 'ignored'" class="poster-stamp stamp-ignored">已忽略</div>
                <div v-else-if="item.status === 'subscribed'" class="poster-stamp stamp-subscribed">处理中</div>
                <div v-else-if="item.status === 'auto_subscribed'" class="poster-stamp stamp-auto">自动中</div>
              </div>

              <!-- 右侧内容 -->
              <div class="card-content-container">
                
                <div class="content-top">
                  <div class="card-header">
                    <n-ellipsis class="card-title" :tooltip="{ style: { maxWidth: '300px' } }">
                      {{ item.item_name }}
                    </n-ellipsis>
                  </div>
                  
                  <div class="card-status-area">
                    <n-space vertical size="small" :wrap="false">
                      <!-- 状态文本 -->
                      <div v-if="item.status === 'needed'" class="reason-text-wrapper text-needed">
                        <n-icon :component="AlertCircleOutline" />
                        <n-ellipsis :tooltip="true">{{ item.reason }}</n-ellipsis>
                      </div>
                      <div v-else-if="item.status === 'ignored'" class="reason-text-wrapper text-ignored">
                        <n-icon :component="AlertCircleOutline" />
                        <n-ellipsis :tooltip="true">(已忽略) {{ item.reason }}</n-ellipsis>
                      </div>
                      <div v-else-if="item.status === 'subscribed'" class="reason-text-wrapper text-subscribed">
                        <n-icon :component="SyncOutline" />
                        <n-ellipsis :tooltip="true">(处理中) {{ item.reason }}</n-ellipsis>
                      </div>
                      <div v-else-if="item.status === 'auto_subscribed'" class="reason-text-wrapper text-auto">
                        <n-icon :component="SyncOutline" />
                        <n-ellipsis :tooltip="true">(自动) {{ item.reason }}</n-ellipsis>
                      </div>
                      <n-tag v-else :type="getStatusInfo(item.status).type" size="small" round>
                        {{ getStatusInfo(item.status).text }}
                      </n-tag>

                      <!-- 媒体信息 (紧凑展示) -->
                      <div class="meta-info-grid">
                        <n-text :depth="3" class="info-text">分辨率: {{ item.resolution_display }}</n-text>
                        
                        <n-tooltip trigger="hover" placement="top-start" :disabled="!item.release_group_raw || item.release_group_raw.length === 0">
                            <template #trigger>
                              <n-text :depth="3" class="info-text">质量: {{ item.quality_display }}</n-text>
                            </template>
                            发布组: {{ item.release_group_raw ? item.release_group_raw.join(', ') : '' }}
                        </n-tooltip>

                        <n-text :depth="3" class="info-text">编码: {{ item.codec_display }}</n-text>
                        <n-text :depth="3" class="info-text">特效: {{ Array.isArray(item.effect_display) ? item.effect_display.join(', ') : item.effect_display }}</n-text>
                        
                        <n-tooltip trigger="hover" placement="top-start">
                            <template #trigger><n-text :depth="3" class="info-text ellipsis full-width-item">音轨: {{ item.audio_display }}</n-text></template>
                            {{ item.audio_display }}
                        </n-tooltip>
                        
                        <n-tooltip trigger="hover" placement="top-start">
                            <template #trigger><n-text :depth="3" class="info-text ellipsis full-width-item">字幕: {{ item.subtitle_display }}</n-text></template>
                            {{ item.subtitle_display }}
                        </n-tooltip>
                      </div>
                    </n-space>
                  </div>
                </div>

                <!-- 底部按钮 -->
                <div class="card-actions-bottom">
                  <n-space align="center" justify="center" size="small" :wrap="false">
                      <n-button 
                        v-if="item.status === 'needed'" 
                        size="tiny" 
                        :type="getActionInfo(item).type" 
                        ghost 
                        @click.stop="resubscribeItem(item)" 
                        :loading="subscribing[item.item_id]"
                      >
                        {{ getActionInfo(item).text }}
                      </n-button>
                      <n-button v-if="item.status === 'needed'" size="tiny" @click.stop="ignoreItem(item)">忽略</n-button>
                      <n-button v-if="item.status === 'ignored'" size="tiny" @click.stop="unignoreItem(item)">恢复</n-button>
                      <n-button text @click.stop="openInEmby(item)"><n-icon :component="EmbyIcon" size="18" /></n-button>
                      <n-button text tag="a" :href="`https://www.themoviedb.org/${item.item_type === 'Movie' ? 'movie' : 'tv'}/${item.tmdb_id}`" target="_blank" @click.stop><n-icon :component="TMDbIcon" size="18" /></n-button>
                  </n-space>
                </div>

              </div>
            </div>
            <!-- 布局结束 -->

          </n-card>
        </div>
      </div>
      <!-- Grid 结束 -->

      <div ref="loaderTrigger" class="loader-trigger">
        <n-spin v-if="displayedItems.length < filteredItems.length" size="small" />
      </div>
    </div>
      <div v-else class="center-container"><n-empty description="缓存为空，或当前筛选条件下无项目。" size="huge" /></div>
    </div>

    <n-modal v-model:show="showSettingsModal" preset="card" style="width: 90%; max-width: 800px;" title="规则设定">
      <ResubscribeSettingsPage @saved="handleSettingsSaved" />
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, h, watch, nextTick } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, NGrid, NGi, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NModal, NTooltip, NText, NDropdown, useDialog, NCheckbox, NInput, NSelect, NButtonGroup } from 'naive-ui';
import { SyncOutline, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, AlertCircleOutline } from '@vicons/ionicons5';
import { useConfig } from '../composables/useConfig.js';
import ResubscribeSettingsPage from './settings/ResubscribeSettingsPage.vue';

const EmbyIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 48 48", width: "18", height: "18" }, [ h('path', { d: "M24,4.2c-11,0-19.8,8.9-19.8,19.8S13,43.8,24,43.8s19.8-8.9,19.8-19.8S35,4.2,24,4.2z M24,39.8c-8.7,0-15.8-7.1-15.8-15.8S15.3,8.2,24,8.2s15.8,7.1,15.8,15.8S32.7,39.8,24,39.8z", fill: "currentColor" }), h('polygon', { points: "22.2,16.4 22.2,22.2 16.4,22.2 16.4,25.8 22.2,25.8 22.2,31.6 25.8,31.6 25.8,25.8 31.6,31.6 31.6,22.2 25.8,22.2 25.8,16.4 ", fill: "currentColor" })]);
const TMDbIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 512 512", width: "18", height: "18" }, [ h('path', { d: "M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zM133.2 176.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zM133.2 262.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8z", fill: "#01b4e4" })]);

const { configModel } = useConfig();
const message = useMessage();
const dialog = useDialog();
const props = defineProps({ taskStatus: { type: Object, required: true } });

const allItems = ref([]); 
const displayedItems = ref([]); 
const filter = ref('all');
const isLoading = ref(true);
const error = ref(null);
const showSettingsModal = ref(false);
const subscribing = ref({});
const loaderTrigger = ref(null); 
const PAGE_SIZE = 24; 

const selectedItems = ref(new Set());
const lastSelectedIndex = ref(-1);

const searchQuery = ref('');
const sortBy = ref('item_name');
const sortOrder = ref('asc');
const mediaTypeFilter = ref(null); 
const mediaTypeOptions = ref([ 
  { label: '全部类型', value: null },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' },
]);
const ruleFilter = ref(null); 
const ruleOptions = ref([{ label: '全部规则', value: null }]); 

const sortOptions = ref([
  { label: '按名称', value: 'item_name' },
]);

const isTaskRunning = (taskName) => props.taskStatus.is_running && props.taskStatus.current_action.includes(taskName);

const filteredItems = computed(() => {
  let items = [...allItems.value];

  if (filter.value === 'needed') {
    items = items.filter(item => item.status === 'needed');
  } else if (filter.value === 'ignored') {
    items = items.filter(item => item.status === 'ignored');
  } else if (filter.value === 'auto') { 
    items = items.filter(item => item.status === 'auto_subscribed');
  }

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    items = items.filter(item => item.item_name.toLowerCase().includes(query));
  }

  if (mediaTypeFilter.value) {
    items = items.filter(item => item.conceptual_type === mediaTypeFilter.value);
  }

  if (ruleFilter.value !== null) {
    items = items.filter(item => item.matched_rule_id === ruleFilter.value);
  }

  items.sort((a, b) => {
    const valA = a[sortBy.value];
    const valB = b[sortBy.value];
    
    let comparison = 0;
    if (typeof valA === 'string' && typeof valB === 'string') {
      comparison = valA.localeCompare(valB, 'zh-Hans-CN');
    } else {
      comparison = valA > valB ? 1 : (valA < valB ? -1 : 0);
    }
    
    return sortOrder.value === 'desc' ? -comparison : comparison;
  });

  return items;
});

const getStatusInfo = (status) => {
  switch (status) {
    case 'needed': return { text: '需处理', type: 'warning' };
    case 'subscribed': return { text: '已提交', type: 'info' };
    case 'auto_subscribed': return { text: '自动处理', type: 'primary' };
    case 'ignored': return { text: '已忽略', type: 'tertiary' };
    case 'ok': default: return { text: '已达标', type: 'success' };
  }
};

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  selectedItems.value.clear(); 
  lastSelectedIndex.value = -1;
  try {
    const response = await axios.get('/api/resubscribe/library_status');
    allItems.value = response.data;
  } catch (err)
 {
    error.value = err.response?.data?.error || '获取状态失败。';
  } finally {
    isLoading.value = false;
  }
};

const loadMore = () => {
  if (isLoading.value || displayedItems.value.length >= filteredItems.value.length) return;
  const currentLength = displayedItems.value.length;
  const nextItems = filteredItems.value.slice(currentLength, currentLength + PAGE_SIZE);
  displayedItems.value.push(...nextItems);
};

let observer = null;
const setupObserver = () => {
  if (observer) observer.disconnect();
  nextTick(() => {
    if (loaderTrigger.value) {
      observer = new IntersectionObserver((entries) => {
        if (entries[0].isIntersecting) loadMore();
      }, { rootMargin: '200px' });
      observer.observe(loaderTrigger.value);
    }
  });
};

watch(filteredItems, (newFilteredItems) => {
  displayedItems.value = newFilteredItems.slice(0, PAGE_SIZE);
  selectedItems.value.clear();
  lastSelectedIndex.value = -1;
  setupObserver();
}, { immediate: true });

onMounted(async () => {
  isLoading.value = true;
  try {
    await Promise.all([
      fetchData(),
      fetchRules()
    ]);
  } catch (e) {
    console.error("初始化数据加载失败", e);
  } finally {
    isLoading.value = false;
  }
});
onUnmounted(() => { if (observer) observer.disconnect(); });

const fetchRules = async () => {
  try {
    const response = await axios.get('/api/resubscribe/rules');
    ruleOptions.value = [{ label: '全部规则', value: null }, ...response.data.map(rule => ({
      label: rule.name,
      value: rule.id
    }))];
  } catch (err) {
    message.error('获取规则列表失败。');
  }
};

const handleCardClick = (event, item, index) => {
  const itemId = item.item_id;
  const isSelected = selectedItems.value.has(itemId);

  const displayedIndex = displayedItems.value.findIndex(d => d.item_id === itemId);

  if (event.shiftKey && lastSelectedIndex.value !== -1) {
    const start = Math.min(lastSelectedIndex.value, displayedIndex);
    const end = Math.max(lastSelectedIndex.value, displayedIndex);
    for (let i = start; i <= end; i++) {
      const idInRange = displayedItems.value[i].item_id;
      selectedItems.value.add(idInRange);
    }
  } else {
    if (isSelected) {
      selectedItems.value.delete(itemId);
    } else {
      selectedItems.value.add(itemId);
    }
  }
  lastSelectedIndex.value = displayedIndex;
};

const batchActions = computed(() => {
  const actions = [];
  const noSelection = selectedItems.value.size === 0;

  if (filter.value === 'ignored') {
    actions.push({ label: '批量取消忽略', key: 'unignore', disabled: noSelection });
  } else {
    // 核心修改：重命名为批量整理
    actions.push({ label: '批量整理', key: 'subscribe', disabled: noSelection });
    actions.push({ label: '批量忽略', key: 'ignore', disabled: noSelection });
  }
  // 移除批量删除
  actions.push({ type: 'divider', key: 'd1' });

  if (filter.value === 'needed') {
    actions.push({ label: '一键忽略当前页所有“需处理”项', key: 'oneclick-ignore' });
  }
  if (filter.value === 'ignored') {
    actions.push({ label: '一键取消忽略当前页所有项', key: 'oneclick-unignore' });
  }
  
  // 移除一键删除
  
  return actions;
});

const handleBatchAction = (key) => {
  let ids = [];
  let actionKey = key;
  let isOneClick = false;

  if (key.startsWith('oneclick-')) {
    isOneClick = true;
    actionKey = key.split('-')[1]; // 提取 'ignore' 或 'unignore'
    
    // ★★★ 核心修改：这里不再留空，而是直接获取当前筛选后的所有 ID ★★★
    // filteredItems 就是你当前页面看到的所有数据（包含搜索、筛选后的结果）
    ids = filteredItems.value.map(item => item.item_id);
    
    if (ids.length === 0) {
        message.warning("当前筛选条件下没有可操作的项目。");
        return;
    }
    
    // 增加一个确认弹窗，防止误操作（因为数量可能很大）
    dialog.warning({
        title: '批量操作确认',
        content: `确定要对当前视图下的 ${ids.length} 个项目执行“${actionKey === 'ignore' ? '忽略' : '取消忽略'}”操作吗？`,
        positiveText: '确定',
        negativeText: '取消',
        onPositiveClick: () => {
            executeBatchAction(actionKey, ids, isOneClick);
        }
    });
    return; // 这里的 return 是为了等待 Dialog 回调，不直接执行下面的代码
  } else {
    // 普通的多选操作
    ids = Array.from(selectedItems.value);
  }
  
  if (ids.length === 0) return;
  executeBatchAction(actionKey, ids, isOneClick);
};

const sendBatchActionRequest = async (actionKey, ids, isOneClick) => {
  const actionMap = { subscribe: 'subscribe', ignore: 'ignore', unignore: 'ok' };
  const action = actionMap[actionKey];

  try {
    const response = await axios.post('/api/resubscribe/batch_action', {
      item_ids: ids, action: action, is_one_click: isOneClick, filter: filter.value
    });
    message.success(response.data.message);
    
    if (!isOneClick) {
      const optimisticStatusMap = { subscribe: 'subscribed', ignore: 'ignored', unignore: 'ok' };
      const optimisticStatus = optimisticStatusMap[actionKey];
      if (optimisticStatus === 'ok') {
        allItems.value = allItems.value.filter(i => !ids.includes(i.item_id));
      } else {
        ids.forEach(id => {
          const item = allItems.value.find(i => i.item_id === id);
          if (item) item.status = optimisticStatus;
        });
      }
      selectedItems.value.clear();
    } else {
      fetchData();
    }
  } catch (err) {
    message.error(err.response?.data?.error || `批量操作失败。`);
  }
};

const executeBatchAction = async (actionKey, ids, isOneClick) => {
  // 移除删除确认逻辑，直接发送请求（因为现在是“整理”，具体行为由后端规则决定）
  sendBatchActionRequest(actionKey, ids, isOneClick);
};

const ignoreItem = async (item) => {
  try {
    await axios.post('/api/resubscribe/batch_action', { item_ids: [item.item_id], action: 'ignore' });
    message.success(`《${item.item_name}》已忽略。`);
    const itemInList = allItems.value.find(i => i.item_id === item.item_id);
    if (itemInList) {
      itemInList.status = 'ignored';
    }
  } catch (err) {
    message.error(err.response?.data?.error || '忽略失败。');
  }
};

const unignoreItem = async (item) => {
  try {
    await axios.post('/api/resubscribe/batch_action', { item_ids: [item.item_id], action: 'ok' });
    message.success(`《${item.item_name}》已取消忽略。`);
    allItems.value = allItems.value.filter(i => i.item_id !== item.item_id);
  } catch (err) {
    message.error(err.response?.data?.error || '取消忽略失败。');
  }
};

// 移除 deleteItem 函数

const triggerRefreshStatus = async () => {
  try {
    await axios.post('/api/resubscribe/refresh_status');
    message.success('刷新任务已提交，请稍后查看任务状态。');
  } catch (err) {
    message.error(err.response?.data?.error || '提交刷新任务失败。');
  }
};
const triggerResubscribeAll = async () => { try { await axios.post('/api/resubscribe/resubscribe_all'); message.success('一键整理任务已提交，请稍后查看任务状态。'); } catch (err) { message.error(err.response?.data?.error || '提交一键整理任务失败。'); }};
const resubscribeItem = async (item) => {
  subscribing.value[item.item_id] = true;
  try {
    const response = await axios.post('/api/resubscribe/batch_action', {
      item_ids: [item.item_id],
      action: 'subscribe' 
    });
    
    message.success("整理任务已提交");

    const itemInList = allItems.value.find(i => i.item_id === item.item_id);
    if (itemInList) {
      // 暂时设为 'subscribed'，前端会显示为“处理中”
      // 等后台任务跑完，如果是删除操作，刷新后该条目会自动消失
      itemInList.status = 'subscribed'; 
    }
  } catch (err) {
    message.error(err.response?.data?.error || '整理请求失败。');
  } finally {
    subscribing.value[item.item_id] = false;
  }
};
const getPosterUrl = (item) => {
  if (item.poster_path) {
    return `https://image.tmdb.org/t/p/w500${item.poster_path}`;
  }
  return ''; 
};
const openInEmby = (item) => {
  const embyServerUrl = configModel.value?.emby_server_url;
  const serverId = configModel.value?.emby_server_id;
  if (!embyServerUrl) {
    message.error('Emby服务器地址未配置，无法跳转。');
    return;
  }

  let targetEmbyId = null;
  if (item.item_type === 'Movie') {
    targetEmbyId = item.emby_item_id;
  } else if (item.item_type === 'Season') {
    targetEmbyId = item.series_emby_id;
  }

  if (!targetEmbyId) {
    message.error('无法确定此项目的有效Emby ID，无法跳转。');
    return;
  }

  const baseUrl = embyServerUrl.endsWith('/') ? embyServerUrl.slice(0, -1) : embyServerUrl;
  let finalUrl = `${baseUrl}/web/index.html#!/item?id=${targetEmbyId}`;
  if (serverId) {
    finalUrl += `&serverId=${serverId}`;
  }
  window.open(finalUrl, '_blank');
};
const getActionInfo = (item) => {
  // 如果后端返回的字段名不是 'action'，请在此处修改 (例如 item.rule_mode)
  if (item.action === 'delete') {
    return { text: '删除', type: 'error' };
  }
  return { text: '洗版', type: 'primary' };
};
const handleSettingsSaved = async (payload = {}) => {
  showSettingsModal.value = false; // 关闭弹窗
  
  // 如果是删除规则（或者调整顺序），后端数据已经变了
  if (payload.needsRefresh) {
    // 直接重新拉取列表（这是一个极快的读库操作，瞬间完成）
    await fetchData(); 
    message.success('规则已更新，列表已刷新。');
  } else {
    // 如果只是修改了规则内容（但没删），可能需要提示用户手动扫描
    message.success('规则已保存。如需应用新规则，请点击“扫描媒体库”按钮。');
  }
};

watch(() => props.taskStatus, (newStatus, oldStatus) => {
  if (oldStatus.is_running && !newStatus.is_running) {
    const relevantActions = [
      '刷新媒体整理', 
      '批量媒体整理',
      '批量删除媒体'   
    ];
    
    if (relevantActions.some(action => oldStatus.current_action.includes(action))) {
      message.info('相关后台任务已结束，正在刷新海报墙...');
      fetchData();
    }
  }
}, { deep: true }); 
</script>

<style scoped>
/* 页面基础 */
.resubscribe-page { padding: 0 10px; }
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
.card-actions-bottom :deep(.n-button) {
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

/* 信息网格 (ResubscribePage用) */
.meta-info-grid {
  display: grid;
  grid-template-columns: 1fr 1fr; /* 强制分为两列，宽度相等 */
  gap: 2px 8px; /* 行间距 2px，列间距 8px */
  margin-top: calc(4px * var(--card-scale, 1));
}

/* 新增：让音轨和字幕跨越两列 */
.full-width-item {
  grid-column: span 2;
}

.info-text {
  min-width: 0; 
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