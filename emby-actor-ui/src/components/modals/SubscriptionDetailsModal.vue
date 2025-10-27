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
          <!-- ★★★ 核心修改：用标签页系统替换单一表格 ★★★ -->
          <div v-if="subscriptionData.tracked_media && subscriptionData.tracked_media.length > 0">
            <n-tabs type="segment" size="small" v-model:value="activeTab" animated>
              
              <!-- 缺失 -->
              <n-tab-pane v-if="missingMedia.length > 0" name="missing" :tab="`缺失 (${missingMedia.length})`">
                <n-data-table :columns="createColumns()" :data="missingMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 已入库 -->
              <n-tab-pane v-if="inLibraryMedia.length > 0" name="in-library" :tab="`已入库 (${inLibraryMedia.length})`">
                <n-data-table :columns="createColumns()" :data="inLibraryMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 已订阅 -->
              <n-tab-pane v-if="subscribedMedia.length > 0" name="subscribed" :tab="`已订阅 (${subscribedMedia.length})`">
                <n-data-table :columns="createColumns()" :data="subscribedMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 待发行 -->
              <n-tab-pane v-if="pendingReleaseMedia.length > 0" name="pending" :tab="`待发行 (${pendingReleaseMedia.length})`">
                <n-data-table :columns="createColumns()" :data="pendingReleaseMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
              </n-tab-pane>

              <!-- 已忽略 -->
              <n-tab-pane v-if="ignoredMedia.length > 0" name="ignored" :tab="`已忽略 (${ignoredMedia.length})`">
                <n-data-table :columns="createColumns()" :data="ignoredMedia" :pagination="{ pageSize: 10 }" :bordered="false" size="small" />
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
import { NModal, NSpin, NAlert, NTabs, NTabPane, NDataTable, NTag, NButton, NSpace, NPopconfirm, useMessage, NImage, useDialog, NTooltip, NEmpty } from 'naive-ui';
import axios from 'axios';
import SubscriptionConfigForm from './SubscriptionConfigForm.vue';

const props = defineProps({
  show: Boolean,
  subscriptionId: Number,
});
const emit = defineEmits(['update:show', 'subscription-updated', 'subscription-deleted']);

const message = useMessage();
const dialog = useDialog();
const loading = ref(false);
const error = ref(null);
const subscriptionData = ref(null);
const editableConfig = ref({});
const activeTab = ref('missing')

// ★★★ 为每个状态创建一个计算属性 ★★★
const missingMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'MISSING') || []
);
const inLibraryMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'IN_LIBRARY') || []
);
const subscribedMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'SUBSCRIBED') || []
);
const pendingReleaseMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'PENDING_RELEASE') || []
);
const ignoredMedia = computed(() => 
  subscriptionData.value?.tracked_media.filter(m => m.status === 'IGNORED') || []
);

// ★★★ 新增：用于跟踪正在订阅的媒体项ID，以显示行内加载状态 ★★★
const subscribingMediaId = ref(null);

// ★★★ 新增：手动订阅单个作品的函数 ★★★
const handleSubscribe = async (mediaId) => {
  subscribingMediaId.value = mediaId;
  try {
    const response = await axios.post(`/api/actor-subscriptions/media/${mediaId}/subscribe`);
    message.success(response.data.message || '订阅成功！');
    
    // 实时更新UI：找到对应的媒体项并将其状态改为'SUBSCRIBED'
    const mediaIndex = subscriptionData.value.tracked_media.findIndex(m => m.id === mediaId);
    if (mediaIndex !== -1) {
      subscriptionData.value.tracked_media[mediaIndex].status = 'SUBSCRIBED';
    }
  } catch (err) {
    console.error("订阅失败:", err);
    const errorMsg = err.response?.data?.error || '订阅失败，请检查后台日志。';
    message.error(errorMsg);
  } finally {
    subscribingMediaId.value = null;
  }
};

// ★★★ 忽略单个作品的函数 ★★★
const handleIgnore = async (mediaId, currentStatus) => {
  if (currentStatus === 'MISSING') {
    // 从 缺失 -> 忽略，需要弹窗确认
    dialog.warning({
      title: '确认忽略',
      content: '确定要忽略这个作品吗？忽略后将不会被自动订阅。',
      positiveText: '确认',
      negativeText: '取消',
      onPositiveClick: async () => {
        await updateMediaStatus(mediaId, 'MISSING', '忽略成功！');
      },
    });
  } else {
    // 从 忽略 -> 缺失，直接操作，无需确认
    await updateMediaStatus(mediaId, 'IGNORED', '已恢复为缺失状态！');
  }
};

// ★★★ 一个通用的状态更新函数，用于代码复用 ★★★
const updateMediaStatus = async (mediaId, currentStatus, successMessage) => {
  // 如果当前是 IGNORED，则目标状态是 MISSING，反之亦然
  const newStatus = currentStatus === 'IGNORED' ? 'MISSING' : 'IGNORED';
  
  try {
    await axios.post(`/api/actor-subscriptions/media/${mediaId}/status`, { status: newStatus });
    message.success(successMessage || `状态已更新为 ${newStatus}`);
    
    // 实时更新UI
    const mediaIndex = subscriptionData.value.tracked_media.findIndex(m => m.id === mediaId);
    if (mediaIndex !== -1) {
      subscriptionData.value.tracked_media[mediaIndex].status = newStatus;
    }
  } catch (err) {
    const errorMsg = err.response?.data?.error || '操作失败，请检查后台日志。';
    message.error(errorMsg);
  }
};

// ★★★ 将静态的 columns 定义改为动态创建的函数 ★★★
const createColumns = () => [
  {
    title: '海报',
    key: 'poster_path',
    width: 65,
    render(row) {
      const url = row.poster_path ? `https://image.tmdb.org/t/p/w92${row.poster_path}` : 'https://via.placeholder.com/92x138.png?text=N/A';
      return h(NImage, { src: url, width: "45", style: 'border-radius: 3px; display: block;' });
    }
  },
  { title: '标题', key: 'title', ellipsis: { tooltip: true } },
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
        'SUBSCRIBED': { type: 'info', text: '已订阅' },
        'PENDING_RELEASE': { type: 'default', text: '待发行' },
        'MISSING': { type: 'warning', text: '缺失' },
        'IGNORED': { type: 'default', text: '已忽略' },
      };
      const info = statusMap[row.status] || { type: 'error', text: '未知' };
      return h(NTag, { type: info.type, size: 'small', round: true }, { default: () => info.text });
    }
  },
  {
    title: '操作',
    key: 'actions',
    width: 180,
    render(row) {
      const buttons = [];

      // --- 按钮 1: 订阅 / Emby ---
      if (row.status === 'MISSING') {
        buttons.push(h(
          NTooltip,
          { trigger: 'hover' },
          {
            trigger: () => h(
              NButton,
              {
                size: 'tiny', type: 'primary', ghost: true,
                loading: subscribingMediaId.value === row.id,
                disabled: !!subscribingMediaId.value,
                onClick: () => handleSubscribe(row.id),
              },
              { default: () => '订阅' }
            ),
            default: () => '使用 MoviePilot 订阅此媒体'
          }
        ));
      } else if (
        row.status === 'IN_LIBRARY' && 
        row.emby_item_id && 
        subscriptionData.value.emby_server_url &&
        subscriptionData.value.emby_server_id // <-- 确保 server_id 也存在
      ) {
        // ★★★ 核心修改：拼接包含 serverId 的标准 Emby Web URL ★★★
        const embyItemUrl = `${subscriptionData.value.emby_server_url}/web/index.html#!/item?id=${row.emby_item_id}&serverId=${subscriptionData.value.emby_server_id}`;

        buttons.push(h(
          NTooltip,
          { trigger: 'hover' },
          {
            trigger: () => h(
              'a',
              {
                href: embyItemUrl, // <--- 使用最终拼接好的 URL
                target: '_blank',
              },
              [h(NButton, { size: 'tiny', type: 'info', ghost: true }, { default: () => 'Emby' })]
            ),
            default: () => '在 Emby 中打开'
          }
        ));
      }

      // --- 按钮 2: 忽略 / 恢复 ---
      if (row.status === 'MISSING') {
        buttons.push(h(
          NTooltip,
          { trigger: 'hover' },
          {
            trigger: () => h(
              NButton,
              {
                size: 'tiny', type: 'default',
                onClick: () => handleIgnore(row.id, 'MISSING'),
              },
              { default: () => '忽略' }
            ),
            default: () => '将此媒体标记为已忽略'
          }
        ));
      } else if (row.status === 'IGNORED') {
        buttons.push(h(
          NTooltip,
          { trigger: 'hover' },
          {
            trigger: () => h(
              NButton,
              {
                size: 'tiny', type: 'warning', ghost: true,
                onClick: () => handleIgnore(row.id, 'IGNORED'),
              },
              { default: () => '恢复' }
            ),
            default: () => '将此媒体恢复为缺失状态'
          }
        ));
      }

      // --- 按钮 3: TMDb 链接 ---
      if (row.tmdb_media_id) {
        const mediaTypeForUrl = row.media_type.toLowerCase() === 'series' ? 'tv' : 'movie';
        const tmdbUrl = `https://www.themoviedb.org/${mediaTypeForUrl}/${row.tmdb_media_id}`;
        buttons.push(h(
          NTooltip,
          { trigger: 'hover' },
          {
            trigger: () => h(
              'a',
              { href: tmdbUrl, target: '_blank' },
              [h(NButton, { size: 'tiny', type: 'tertiary' }, { default: () => 'TMDb' })]
            ),
            default: () => '在 TMDb 上查看详情'
          }
        ));
      }

      return h(NSpace, null, { default: () => buttons });
    },
  },
];


const fetchDetails = async (id) => {
  if (!id) return;
  loading.value = true;
  error.value = null;
  subscriptionData.value = null;
  try {
    const response = await axios.get(`/api/actor-subscriptions/${id}`);
    subscriptionData.value = response.data;
    resetConfig();

    // ★★★ 核心新增逻辑：在数据加载后，智能选择默认标签页 ★★★
    await nextTick(); // 等待 DOM 更新完成，确保 computed 属性已计算完毕

    const tabPriority = [
      { name: 'missing', data: missingMedia.value },
      { name: 'in-library', data: inLibraryMedia.value },
      { name: 'subscribed', data: subscribedMedia.value },
      { name: 'pending', data: pendingReleaseMedia.value },
      { name: 'ignored', data: ignoredMedia.value },
    ];

    // 寻找第一个有数据的标签页
    const firstAvailableTab = tabPriority.find(tab => tab.data.length > 0);

    if (firstAvailableTab) {
      activeTab.value = firstAvailableTab.name;
    } else {
      // 如果所有列表都为空，可以默认回到 'missing' 或保持不动
      activeTab.value = 'missing'; 
    }

  } catch (err) {
    error.value = err.response?.data?.error || '加载订阅详情失败。';
  } finally {
    loading.value = false;
  }
};

// ... (您其他的 script setup 内容保持完全不变)
const resetConfig = () => {
  if (!subscriptionData.value || !subscriptionData.value.config) return;
  const config = subscriptionData.value.config;
  editableConfig.value = {
    start_year: config.start_year || 1900,
    media_types: config.media_types || ['Movie', 'TV'],
    genres_include_json: config.genres_include_json || [],
    genres_exclude_json: config.genres_exclude_json || [],
    min_rating: config.min_rating || 6.0,
    main_role_only: config.main_role_only || false, // 新增：初始化 main_role_only
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
