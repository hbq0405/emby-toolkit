<!-- src/components/ResubscribePage.vue (生产力终极版) -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="resubscribe-page">
      <n-page-header>
        <template #title>
          <n-space align="center">
            <span>媒体洗版</span>
            <n-tag v-if="allItems.length > 0" type="info" round :bordered="false" size="small">
              {{ filteredItems.length }} / {{ allItems.length }} 项
            </n-tag>
          </n-space>
        </template>
        <n-alert title="操作提示" type="info" style="margin-top: 24px;">
          <li>先进行洗版规则设定，然后点击刷新按钮扫描全库媒体项，扫描完刷新页面会展示所有媒体项并按照预设的洗版规则显示是否需要洗版。</li>
          <li>按住Shift键可以进行多选然后批量操作。</li>
          <li>本模块所有涉及删除的功能都是删除Emby的媒体项和媒体文件，危险操作，慎用！！！</li>
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
              <n-radio-button value="needed">需洗版</n-radio-button>
              <n-radio-button value="ignored">已忽略</n-radio-button>
            </n-radio-group>
            <n-button @click="showSettingsModal = true">洗版规则设定</n-button>
            <n-button type="warning" @click="triggerResubscribeAll" :loading="isTaskRunning('全库媒体洗版')">一键洗版全部</n-button>
            <n-tooltip trigger="hover">
              <template #trigger>
                <n-button type="primary" @click="triggerRefreshStatus" :loading="isTaskRunning('刷新媒体洗版状态')" circle>
                  <template #icon><n-icon :component="SyncOutline" /></template>
                </n-button>
              </template>
              扫描媒体库
            </n-tooltip>
          </n-space>
        </template>
      </n-page-header>

      <!-- ★★★ START: 新增的筛选和排序控件 ★★★ -->
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
      <!-- ★★★ END: 新增的筛选和排序控件 ★★★ -->

      <n-divider />

      <div v-if="isLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
      <div v-else-if="displayedItems.length > 0">
        <n-grid cols="1 s:2 m:2 l:3 xl:4 2xl:5" :x-gap="20" :y-gap="20" responsive="screen">
          <n-gi v-for="(item, index) in displayedItems" :key="item.item_id">
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
              
              <div class="card-body-wrapper">
                <div class="card-poster-container" @click.stop="handleCardClick($event, item, index)">
                  <n-image lazy :src="getPosterUrl(item)" class="card-poster" object-fit="cover" />
                  <div v-if="item.status === 'needed'" class="poster-stamp">
                    不通过
                  </div>
                </div>

                <div class="card-content-container">
                  <div class="card-header">
                    <n-ellipsis class="card-title">{{ item.item_name }}</n-ellipsis>
                  </div>
                  <div class="card-status-area">
                    <n-space vertical size="small">
                      
                      <div v-if="item.status === 'needed'" class="reason-text-wrapper">
                        <n-icon :component="AlertCircleOutline" color="#d03050" style="margin-right: 4px; flex-shrink: 0;" />
                        <n-ellipsis :tooltip="true" style="max-width: 100%">
                          {{ item.reason }}
                        </n-ellipsis>
                      </div>

                      <!-- 其他状态保持原样显示 Tag -->
                      <n-tag v-else :type="getStatusInfo(item.status).type" size="small">
                        {{ getStatusInfo(item.status).text }}
                      </n-tag>

                      <n-divider style="margin: 4px 0;" />
                      <n-text :depth="2" class="info-text">分辨率: {{ item.resolution_display }}</n-text>
                      <n-tooltip :disabled="!item.release_group_raw || item.release_group_raw.length === 0">
                        <template #trigger>
                          <n-text :depth="2" class="info-text">质量: {{ item.quality_display }}</n-text>
                        </template>
                        发布组: {{ item.release_group_raw.join(', ') }}
                      </n-tooltip>
                      <n-text :depth="2" class="info-text">编码: {{ item.codec_display }}</n-text>
                      <n-text :depth="2" class="info-text">特效: {{ Array.isArray(item.effect_display) ? item.effect_display.join(', ') : item.effect_display }}</n-text>
                      <n-tooltip><template #trigger><n-text :depth="2" class="info-text" :line-clamp="1">音轨: {{ item.audio_display }}</n-text></template>{{ item.audio_display }}</n-tooltip>
                      <n-tooltip><template #trigger><n-text :depth="2" class="info-text" :line-clamp="1">字幕: {{ item.subtitle_display }}</n-text></template>{{ item.subtitle_display }}</n-tooltip>
                    </n-space>
                  </div>
                </div>
              </div>

              <div class="card-actions">
                <n-space align="center">
                  <n-button v-if="item.status === 'needed'" size="small" type="primary" @click.stop="resubscribeItem(item)" :loading="subscribing[item.item_id]">洗版</n-button>
                  <n-button v-if="item.status === 'needed'" size="small" @click.stop="ignoreItem(item)">忽略</n-button>
                  <n-button v-if="item.status === 'subscribed'" size="small" type="info" disabled>已订阅</n-button>
                  <n-button v-if="item.status === 'ignored'" size="small" type="tertiary" @click.stop="unignoreItem(item)">取消忽略</n-button>
                  
                  <n-divider v-if="item.status !== 'ok'" vertical />

                  <n-tooltip>
                    <template #trigger>
                      <n-button text type="error" @click.stop="deleteItem(item)">
                        <template #icon><n-icon :component="TrashOutline" size="18" /></template>
                      </n-button>
                    </template>
                    删除
                  </n-tooltip>
                  <n-button text @click.stop="openInEmby(item)"><template #icon><n-icon :component="EmbyIcon" size="18" /></template></n-button>
                  <n-button text tag="a" :href="`https://www.themoviedb.org/${item.item_type === 'Movie' ? 'movie' : 'tv'}/${item.tmdb_id}`" target="_blank" @click.stop><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button>
                </n-space>
              </div>
            </n-card>
          </n-gi>
        </n-grid>
        <div ref="loaderTrigger" class="loader-trigger">
          <n-spin v-if="displayedItems.length < filteredItems.length" />
        </div>
      </div>
      <div v-else class="center-container"><n-empty description="缓存为空，或当前筛选条件下无项目。" size="huge" /></div>
    </div>

    <n-modal v-model:show="showSettingsModal" preset="card" style="width: 90%; max-width: 800px;" title="洗版规则设定">
      <ResubscribeSettingsPage @saved="handleSettingsSaved" />
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, h, watch, nextTick } from 'vue';
import axios from 'axios';
// ★★★ 1/4: 导入新增的 Naive UI 组件 ★★★
import { NLayout, NPageHeader, NDivider, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, NGrid, NGi, NCard, NImage, NEllipsis, NSpin, NAlert, NRadioGroup, NRadioButton, NModal, NTooltip, NText, NDropdown, useDialog, NCheckbox, NInput, NSelect, NButtonGroup } from 'naive-ui';
// ★★★ 2/4: 导入新增的图标 ★★★
import { SyncOutline, TrashOutline, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, AlertCircleOutline } from '@vicons/ionicons5';
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

// ★★★ 3/4: 新增用于筛选和排序的状态 ★★★
const searchQuery = ref('');
const sortBy = ref('item_name');
const sortOrder = ref('asc');
const mediaTypeFilter = ref(null); // 新增：媒体类型筛选
const mediaTypeOptions = ref([ // 新增：媒体类型选项
  { label: '全部类型', value: null },
  { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' },
]);
const ruleFilter = ref(null); // 新增：洗版规则筛选
const ruleOptions = ref([{ label: '全部规则', value: null }]); // 新增：洗版规则选项

const sortOptions = ref([
  { label: '按名称', value: 'item_name' },
]);

const isTaskRunning = (taskName) => props.taskStatus.is_running && props.taskStatus.current_action.includes(taskName);

// ★★★ 4/4: 核心修改 - 增强 filteredItems 计算属性，集成所有筛选和排序逻辑 ★★★
const filteredItems = computed(() => {
  let items = [...allItems.value];

  // 1. 按状态筛选 (原有逻辑)
  if (filter.value === 'needed') {
    items = items.filter(item => item.status === 'needed');
  } else if (filter.value === 'ignored') {
    items = items.filter(item => item.status === 'ignored');
  }

  // 2. 按名称搜索 (新增逻辑)
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    items = items.filter(item => item.item_name.toLowerCase().includes(query));
  }

  // 3. 按媒体类型筛选 (新增逻辑)
  if (mediaTypeFilter.value) {
    items = items.filter(item => item.conceptual_type === mediaTypeFilter.value);
  }

  // 4. 按洗版规则筛选 (新增逻辑)
  if (ruleFilter.value !== null) {
    items = items.filter(item => item.matched_rule_id === ruleFilter.value);
  }

  // 5. 排序 (新增逻辑)
  items.sort((a, b) => {
    const valA = a[sortBy.value];
    const valB = b[sortBy.value];
    
    let comparison = 0;
    // 优先处理中文字符串排序
    if (typeof valA === 'string' && typeof valB === 'string') {
      comparison = valA.localeCompare(valB, 'zh-Hans-CN');
    } else {
      // 其他类型的备用排序
      comparison = valA > valB ? 1 : (valA < valB ? -1 : 0);
    }
    
    // 根据升序或降序返回结果
    return sortOrder.value === 'desc' ? -comparison : comparison;
  });

  return items;
});

const getStatusInfo = (status) => {
  switch (status) {
    case 'needed': return { text: '需洗版', type: 'warning' };
    case 'subscribed': return { text: '已订阅', type: 'info' };
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
    error.value = err.response?.data?.error || '获取洗版状态失败。';
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

// 监听 filteredItems 的变化，当筛选或排序条件改变时，重置 displayedItems 并重新设置观察者
watch(filteredItems, (newFilteredItems) => {
  displayedItems.value = newFilteredItems.slice(0, PAGE_SIZE);
  // 重置多选状态
  selectedItems.value.clear();
  lastSelectedIndex.value = -1;
  setupObserver();
}, { immediate: true });

onMounted(async () => {
  // 使用 Promise.all 并行发送请求，减少总等待时间
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
    message.error('获取洗版规则列表失败。');
  }
};

const handleCardClick = (event, item, index) => {
  const itemId = item.item_id;
  const isSelected = selectedItems.value.has(itemId);

  // 查找当前 item 在 displayedItems 中的实际索引
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
    actions.push({ label: '批量订阅', key: 'subscribe', disabled: noSelection });
    actions.push({ label: '批量忽略', key: 'ignore', disabled: noSelection });
  }
  actions.push({ label: '批量删除', key: 'delete', props: { type: 'error' }, disabled: noSelection });
  actions.push({ type: 'divider', key: 'd1' });

  if (filter.value === 'needed') {
    actions.push({ label: '一键忽略当前页所有“需洗版”项', key: 'oneclick-ignore' });
  }
  if (filter.value === 'ignored') {
    actions.push({ label: '一键取消忽略当前页所有项', key: 'oneclick-unignore' });
  }
  
  if (filter.value === 'needed' || filter.value === 'ignored') {
      actions.push({ 
          label: `一键删除当前页所有“${filter.value === 'needed' ? '需洗版' : '已忽略'}”项`, 
          key: 'oneclick-delete',
          props: { type: 'error' } 
      });
  }
  
  return actions;
});

const handleBatchAction = (key) => {
  let ids = Array.from(selectedItems.value);
  let actionKey = key;
  let isOneClick = false;

  if (key.startsWith('oneclick-')) {
    isOneClick = true;
    actionKey = key.split('-')[1];
    ids = []; 
  }
  
  if (!isOneClick && ids.length === 0) return;
  executeBatchAction(actionKey, ids, isOneClick);
};

const sendBatchActionRequest = async (actionKey, ids, isOneClick) => {
  const actionMap = { subscribe: 'subscribe', ignore: 'ignore', unignore: 'ok', delete: 'delete' };
  const action = actionMap[actionKey];

  try {
    const response = await axios.post('/api/resubscribe/batch_action', {
      item_ids: ids, action: action, is_one_click: isOneClick, filter: filter.value
    });
    message.success(response.data.message);
    
    if (!isOneClick) {
      const optimisticStatusMap = { subscribe: 'subscribed', ignore: 'ignored', unignore: 'ok' };
      const optimisticStatus = optimisticStatusMap[actionKey];
      if (optimisticStatus === 'ok' || actionKey === 'delete') {
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
  if (actionKey === 'delete' || actionKey === 'oneclick-delete') {
    const countText = isOneClick ? `当前视图下所有` : `${ids.length}`;
    dialog.warning({
      title: '高危操作确认',
      content: `确定要永久删除选中的 ${countText} 个媒体项吗？此操作会从 Emby 和硬盘中删除文件，且不可恢复！`,
      positiveText: '我确定，删除！',
      negativeText: '取消',
      onPositiveClick: () => { sendBatchActionRequest(actionKey, ids, isOneClick); }
    });
  } else {
    sendBatchActionRequest(actionKey, ids, isOneClick);
  }
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

const deleteItem = (item) => {
  dialog.warning({
    title: '高危操作确认',
    content: `确定要永久删除《${item.item_name}》吗？此操作会从 Emby 和硬盘中删除文件，且不可恢复！`,
    positiveText: '我确定，删除！',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.post('/api/resubscribe/batch_action', { item_ids: [item.item_id], action: 'delete' });
        message.success(`《${item.item_name}》已删除。`);
        allItems.value = allItems.value.filter(i => i.item_id !== item.item_id);
        selectedItems.value.delete(item.item_id);
      } catch (err) {
        message.error(err.response?.data?.error || '删除失败。');
      }
    }
  });
};

const triggerRefreshStatus = async () => {
  try {
    await axios.post('/api/resubscribe/refresh_status');
    message.success('刷新任务已提交，请稍后查看任务状态。');
  } catch (err) {
    message.error(err.response?.data?.error || '提交刷新任务失败。');
  }
};
const triggerResubscribeAll = async () => { try { await axios.post('/api/resubscribe/resubscribe_all'); message.success('一键洗版任务已提交，请稍后查看任务状态。'); } catch (err) { message.error(err.response?.data?.error || '提交一键洗版任务失败。'); }};
const resubscribeItem = async (item) => { subscribing.value[item.item_id] = true; try { const response = await axios.post('/api/resubscribe/resubscribe_item', { item_id: item.item_id, item_name: item.item_name, tmdb_id: item.tmdb_id, item_type: item.item_type, }); message.success(response.data.message); const itemInList = allItems.value.find(i => i.item_id === item.item_id); if (itemInList) { itemInList.status = 'subscribed'; } } catch (err) { message.error(err.response?.data?.error || '洗版订阅失败。'); } finally { subscribing.value[item.item_id] = false; }};
const getPosterUrl = (item) => {
  // ★★★ 核心修复：使用 item.poster_path 拼接 TMDB 图片地址 ★★★
  if (item.poster_path) {
    // 您可以根据需要选择不同的图片尺寸，w500 是一个常用的尺寸
    return `https://image.tmdb.org/t/p/w500${item.poster_path}`;
  }
  // 如果没有海报路径，可以返回一个占位图或者空字符串
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
  // 判断媒体类型来决定使用哪个ID
  if (item.item_type === 'Movie') {
    // 如果是电影，直接使用它自己的 emby_item_id
    targetEmbyId = item.emby_item_id;
  } else if (item.item_type === 'Season') {
    // 如果是季，使用我们从后端新获取的父剧集ID (series_emby_id)
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
const handleSettingsSaved = (payload = {}) => {
  showSettingsModal.value = false;
  if (payload.needsRefresh) {
    message.info('规则已删除，正在刷新媒体列表...');
    fetchData();
  } else {
    message.info('洗版规则已更新。请在需要时手动扫描媒体库以应用更改。');
  }
};

watch(() => props.taskStatus, (newStatus, oldStatus) => {
  if (oldStatus.is_running && !newStatus.is_running) {
    const relevantActions = [
      '刷新媒体洗版状态', 
      '全库媒体洗版',   
    ];
    
    if (relevantActions.some(action => oldStatus.current_action.includes(action))) {
      message.info('相关后台任务已结束，正在刷新海报墙...');
      fetchData();
    }
  }
}, { deep: true }); 
</script>

<style scoped>
.card-checkbox {
  position: absolute;
  top: 8px;
  left: 8px;
  z-index: 10;
  background-color: rgba(24, 24, 28, 0.75);
  border-radius: 50%;
  padding: 4px;
  --n-color-checked: var(--n-color-primary-hover);
  --n-border-radius: 50%;
  
  opacity: 0;
  visibility: hidden;
  transition: opacity 0.2s ease-in-out, visibility 0.2s ease-in-out;
}
.series-card:hover .card-checkbox,
.card-selected .card-checkbox {
  opacity: 1;
  visibility: visible;
}
.card-selected {
  outline: 2px solid var(--n-color-primary-hover);
  outline-offset: 2px;
}
.series-card {
  cursor: pointer;
  transition: transform 0.2s ease-in-out;
  display: flex; /* 使用flex布局 */
  flex-direction: column; /* 垂直排列 */
  height: 100%; /* 确保卡片填满网格单元 */
}
.series-card:hover {
  transform: translateY(-4px);
}
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 250px); }

.card-body-wrapper {
  display: flex;
  gap: 12px;
  flex-grow: 1; /* 让内容区占据剩余空间 */
  min-height: 0;
}

.card-poster-container { 
  flex-shrink: 0; 
  width: 150px; /* 这里调整海报宽度 */
  height: 225px; /* 这里调整海报高度 */
  overflow: hidden; 
  border-radius: 4px; 
} 
.card-poster { width: 100%; height: 100%; }
.card-content-container { 
  flex-grow: 1; 
  display: flex; 
  flex-direction: column; 
  padding: 0;
  min-width: 0; 
}
.card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; flex-shrink: 0; }
.card-title { font-weight: 600; font-size: 1.05em; line-height: 1.3; }
.card-status-area { flex-grow: 1; padding-top: 8px; }
.reason-text { font-size: 0.85em; }
.info-text { font-size: 0.85em; }
.loader-trigger { height: 50px; display: flex; justify-content: center; align-items: center; }

.card-actions { 
  border-top: 1px solid var(--n-border-color); 
  padding-top: 12px; 
  margin-top: auto; /* 关键：将操作区推到底部 */
  flex-shrink: 0; 
  display: flex;
  justify-content: center;
}

.series-card.dashboard-card > :deep(.n-card__content) { 
  display: flex; /* 确保内容区也是flex */
  flex-direction: column !important;
  justify-content: flex-start !important; 
  padding: 12px !important; 
  gap: 12px !important; /* 调整内部间距 */
  height: 100%; /* 继承高度 */
}

/* 响应式调整，让小屏幕下卡片更好看 */
@media (max-width: 640px) {
  .card-body-wrapper {
    flex-direction: column;
    align-items: center;
  }
  .card-poster-container {
    width: 160px;
    height: 240px;
  }
  .card-content-container {
    width: 100%;
    align-items: center;
    text-align: center;
  }
}
.card-poster-container { 
  flex-shrink: 0; 
  width: 150px; 
  height: 225px; 
  overflow: hidden; 
  border-radius: 4px; 
  position: relative; /* 确保印章可以绝对定位 */
} 

/* ★★★ 新增：印章样式 ★★★ */
.poster-stamp {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%) rotate(-15deg); /* 居中并旋转 */
  
  border: 3px solid #d03050; /* 红色边框 */
  color: #d03050;            /* 红色文字 */
  font-weight: 900;
  font-size: 1.4rem;
  letter-spacing: 2px;
  padding: 4px 12px;
  border-radius: 8px;
  
  background-color: rgba(255, 255, 255, 0.85); /* 半透明白色背景，增加对比度 */
  box-shadow: 0 4px 15px rgba(0,0,0,0.3);
  
  z-index: 5;
  pointer-events: none; /* 让点击事件穿透印章，不影响点击海报 */
  white-space: nowrap;
  user-select: none;
  
  /* 增加一点类似印章的做旧感（可选） */
  opacity: 0.9;
}

/* ★★★ 新增：原因文字样式 ★★★ */
.reason-text-wrapper {
  display: flex;
  align-items: center;
  color: #d03050; /* 醒目的红色 */
  font-weight: bold;
  font-size: 0.9em;
  line-height: 1.2;
}
</style>
