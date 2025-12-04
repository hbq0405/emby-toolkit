<!-- src/components/modals/SubscriptionDetailsModal.vue -->
<template>
  <n-modal
    :show="props.show"
    @update:show="val => emit('update:show', val)"
    preset="card"
    style="width: 95%; max-width: 1200px;"
    :title="subscriptionData ? `订阅详情 - ${subscriptionData.actor_name}` : '加载中...'"
    :bordered="false"
    size="huge"
  >
    <div v-if="loading" style="text-align: center; padding: 50px 0;"><n-spin size="large" /></div>
    <div v-else-if="error" style="text-align: center; padding: 50px 0;"><n-alert title="加载失败" type="error">{{ error }}</n-alert></div>
    <div v-else-if="subscriptionData">
      <n-tabs type="line" animated default-value="tracking">
        <n-tab-pane name="tracking" tab="追踪列表">
          <div v-if="subscriptionData.tracked_media && subscriptionData.tracked_media.length > 0">
            <!-- ▼▼▼ 核心修正：移除已忽略标签页 ▼▼▼ -->
            <n-tabs type="segment" size="small" v-model:value="activeTab" animated>
              
              <!-- 待处理 (WANTED, MISSING) -->
              <n-tab-pane v-if="pendingMedia.length > 0" name="pending" :tab="`待处理 (${pendingMedia.length})`">
                <n-data-table :columns="createColumns()" :data="pendingMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 已处理 (IN_LIBRARY, SUBSCRIBED) -->
              <n-tab-pane v-if="processedMedia.length > 0" name="processed" :tab="`已处理 (${processedMedia.length})`">
                <n-data-table :columns="createColumns()" :data="processedMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 待发行 (PENDING_RELEASE) -->
              <n-tab-pane v-if="pendingReleaseMedia.length > 0" name="pending-release" :tab="`待发行 (${pendingReleaseMedia.length})`">
                <n-data-table :columns="createColumns()" :data="pendingReleaseMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

            </n-tabs>
          </div>
          <!-- 如果没有任何追踪作品，显示空状态 -->
          <div v-else>
            <n-empty description="该演员没有追踪任何作品" style="padding: 40px 0;" />
          </div>
        </n-tab-pane>
        <n-tab-pane name="config" tab="订阅配置">
          <div style="max-width: 600px; margin: 0 auto; padding: 20px 0;">
            <p style="margin-bottom: 20px;">在这里可以修改订阅配置，保存后将对未来的扫描生效。</p>
            <subscription-config-form v-model="editableConfig" />
            <n-space justify="end" style="margin-top: 20px;">
              <n-button @click="resetConfig">重置更改</n-button>
              <n-button type="primary" @click="saveConfig">保存配置</n-button>
            </n-space>
          </div>
        </n-tab-pane>
      </n-tabs>
    </div>
    <template #footer>
      <n-space justify="space-between">
        <n-space>
        <n-popconfirm @positive-click="handleDelete">
          <template #trigger>
            <n-button type="error" ghost>删除此订阅</n-button>
          </template>
          确定要删除对该演员的订阅吗？所有追踪记录将一并清除。
        </n-popconfirm>
        <n-button
            v-if="subscriptionData"
            :type="subscriptionData.status === 'active' ? 'warning' : 'success'"
            ghost
            @click="handleToggleStatus"
        >
            {{ subscriptionData.status === 'active' ? '暂停订阅' : '恢复订阅' }}
        </n-button>
        </n-space>
        <n-button type="primary" @click="handleRefresh">手动刷新</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, watch, h, computed, nextTick } from 'vue';
import { NModal, NSpin, NAlert, NTabs, NTabPane, NDataTable, NTag, NButton, NSpace, NPopconfirm, useMessage, NImage, NTooltip, NEmpty } from 'naive-ui';
import axios from 'axios';
import SubscriptionConfigForm from './SubscriptionConfigForm.vue';

const props = defineProps({
  show: Boolean,
  subscriptionId: Number,
});
const emit = defineEmits(['update:show', 'subscription-updated', 'subscription-deleted']);

const message = useMessage();
const loading = ref(false);
const error = ref(null);
const subscriptionData = ref(null);
const editableConfig = ref({});
const activeTab = ref('pending') // 默认打开“待处理”

// ▼▼▼ 核心修正：移除 ignoredMedia 计算属性 ▼▼▼
const processedMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => ['IN_LIBRARY', 'SUBSCRIBED'].includes(m.status)) || []
);
const pendingMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => ['WANTED', 'MISSING'].includes(m.status)) || []
);
const pendingReleaseMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'PENDING_RELEASE') || []
);

const createColumns = () => {
  const columns = [
    {
      title: '海报',
      key: 'poster_path',
      width: 65,
      render(row) {
        const url = row.poster_path ? `https://image.tmdb.org/t/p/w92${row.poster_path}` : 'https://via.placeholder.com/92x138.png?text=N/A';
        return h(NImage, { src: url, width: "45", style: 'border-radius: 3px; display: block;' });
      }
    },
    {
      title: '标题',
      key: 'title',
      ellipsis: { tooltip: true },
      render(row) {
        if (row.parsed_season_number && !isNaN(row.parsed_season_number)) {
          return `${row.title} 第 ${row.parsed_season_number} 季`;
        }
        return row.title;
      }
    },
    { 
      title: '类型', 
      key: 'media_type', 
      width: 80,
      render(row) {
        const typeMap = { 'Series': '电视剧', 'Movie': '电影' };
        return typeMap[row.media_type] || row.media_type;
      }
    },
    {
      title: '发行日期',
      key: 'release_date',
      width: 120,
      render(row) {
        if (!row.release_date) return '';
        return new Date(row.release_date).toLocaleDateString('zh-CN');
      }
    },
    {
      title: '状态',
      key: 'status',
      width: 100,
      render(row) {
        const statusMap = {
          'IN_LIBRARY': { type: 'success', text: '已入库' },
          'SUBSCRIBED': { type: 'success', text: '已订阅' },
          'WANTED': { type: 'warning', text: '等待订阅' },
          'PENDING_RELEASE': { type: 'default', text: '待发行' },
          'MISSING': { type: 'error', text: '缺失' },
          // 虽然不再显示列表，但保留映射以防万一有旧数据混入其他列表
          'IGNORED': { type: 'default', text: '已忽略' },
        };
        const info = statusMap[row.status] || { type: 'error', text: '未知' };
        return h(NTag, { type: info.type, size: 'small', round: true }, { default: () => info.text });
      }
    }
  ];

  // ▼▼▼ 核心修正：移除 ignore_reason 列的添加逻辑 ▼▼▼

  columns.push({
    title: '操作',
    key: 'actions',
    width: 120,
    render(row) {
      const buttons = [];

      // --- 按钮 1: Emby 链接 ---
      if (
        row.status === 'IN_LIBRARY' && 
        row.emby_item_id && 
        subscriptionData.value.emby_server_url &&
        subscriptionData.value.emby_server_id
      ) {
        const embyItemUrl = `${subscriptionData.value.emby_server_url}/web/index.html#!/item?id=${row.emby_item_id}&serverId=${subscriptionData.value.emby_server_id}`;
        buttons.push(h(
          'a',
          { href: embyItemUrl, target: '_blank' },
          [h(NButton, { size: 'tiny', type: 'info', ghost: true }, { default: () => 'Emby' })]
        ));
      }

      // --- 按钮 2: TMDb 链接 ---
      if (row.tmdb_media_id) {
        const mediaTypeForUrl = row.media_type.toLowerCase() === 'series' ? 'tv' : 'movie';
        const tmdbUrl = `https://www.themoviedb.org/${mediaTypeForUrl}/${row.tmdb_media_id}`;
        buttons.push(h(
          'a',
          { href: tmdbUrl, target: '_blank' },
          [h(NButton, { size: 'tiny', type: 'tertiary' }, { default: () => 'TMDb' })]
        ));
      }

      return h(NSpace, null, { default: () => buttons });
    },
  });

  return columns;
};


const fetchDetails = async (id) => {
  if (!id) return;
  loading.value = true;
  error.value = null;
  subscriptionData.value = null;
  try {
    const response = await axios.get(`/api/actor-subscriptions/${id}`);
    subscriptionData.value = response.data;
    resetConfig();

    await nextTick();

    // ▼▼▼ 核心修正：移除 ignored 标签页的优先级判断 ▼▼▼
    const tabPriority = [
      { name: 'pending', data: pendingMedia.value },
      { name: 'processed', data: processedMedia.value },
      { name: 'pending-release', data: pendingReleaseMedia.value },
    ];
    // ▲▲▲ 修正结束 ▲▲▲

    const firstAvailableTab = tabPriority.find(tab => tab.data.length > 0);
    activeTab.value = firstAvailableTab ? firstAvailableTab.name : 'pending';

  } catch (err) {
    error.value = err.response?.data?.error || '加载订阅详情失败。';
  } finally {
    loading.value = false;
  }
};

const resetConfig = () => {
  if (!subscriptionData.value || !subscriptionData.value.config) return;
  const config = subscriptionData.value.config;
  editableConfig.value = {
    start_year: config.start_year || 1900,
    media_types: config.media_types || ['Movie', 'TV'],
    genres_include_json: config.genres_include_json || [],
    genres_exclude_json: config.genres_exclude_json || [],
    min_rating: config.min_rating,
    main_role_only: config.main_role_only || false,
    min_vote_count: config.min_vote_count === undefined ? 10 : config.min_vote_count,
  };
};

const saveConfig = async () => {
  if (!props.subscriptionId) return;
  try {
    const payload = {
      config: editableConfig.value
    };
    await axios.put(`/api/actor-subscriptions/${props.subscriptionId}`, payload);
    
    message.success('配置已成功保存！');
    emit('subscription-updated');
    fetchDetails(props.subscriptionId);
  } catch (err) {
    message.error(err.response?.data?.error || '保存配置失败。');
  }
};

const handleDelete = async () => {
  if (!props.subscriptionId) return;
  try {
    await axios.delete(`/api/actor-subscriptions/${props.subscriptionId}`);
    message.success('订阅已成功删除！');
    emit('subscription-deleted');
    emit('update:show', false);
  } catch (err) {
    message.error(err.response?.data?.error || '删除订阅失败。');
  }
};

const handleRefresh = async () => {
  if (!props.subscriptionId) return;
  try {
    await axios.post(`/api/actor-subscriptions/${props.subscriptionId}/refresh`);
    message.success('手动刷新任务已提交到后台！请稍后查看任务状态。');
    emit('update:show', false);
  } catch (err) {
    message.error(err.response?.data?.error || '启动刷新任务失败。');
  }
};

const handleToggleStatus = async () => {
  if (!props.subscriptionId || !subscriptionData.value || !subscriptionData.value.config) return;
  const newStatus = subscriptionData.value.status === 'active' ? 'paused' : 'active';
  const actionText = newStatus === 'paused' ? '暂停' : '恢复';
  try {
    const currentConfig = subscriptionData.value.config;
    const payload = {
      status: newStatus,
      config: {
        start_year: currentConfig.start_year,
        media_types: currentConfig.media_types, // 前端直接发送数组
        genres_include_json: currentConfig.genres_include_json,
        genres_exclude_json: currentConfig.genres_exclude_json,
        min_rating: currentConfig.min_rating
      }
    };
    await axios.put(`/api/actor-subscriptions/${props.subscriptionId}`, payload);
    message.success(`订阅已成功${actionText}！`);
    emit('subscription-updated');
    await fetchDetails(props.subscriptionId);
  } catch (err) {
    message.error(err.response?.data?.error || `${actionText}订阅失败。`);
  }
};

watch(() => props.subscriptionId, (newId) => {
  if (newId && props.show) {
    fetchDetails(newId);
  }
});

watch(() => props.show, (newVal) => {
  if (newVal && props.subscriptionId) {
    fetchDetails(props.subscriptionId);
  }
});
</script>