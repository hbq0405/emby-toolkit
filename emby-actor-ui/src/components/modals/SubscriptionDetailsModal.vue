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
          <!-- ★★★ 核心修改 1：将静态 columns 改为动态函数 createColumns() ★★★ -->
          <n-data-table
            :columns="createColumns()"
            :data="subscriptionData.tracked_media"
            :pagination="{ pageSize: 10 }"
            :bordered="false"
            size="small"
          />
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
import { ref, watch, h } from 'vue';
// ★★★ 引入 NImage 以便在表格中渲染海报 ★★★
import { NModal, NSpin, NAlert, NTabs, NTabPane, NDataTable, NTag, NButton, NSpace, NPopconfirm, useMessage, NImage } from 'naive-ui';
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

// ★★★ 核心修改 2：将静态的 columns 定义改为动态创建的函数 ★★★
const createColumns = () => [
  {
    title: '海报',
    key: 'poster_path',
    width: 65,
    render(row) {
      const url = row.poster_path ? `https://image.tmdb.org/t/p/w92${row.poster_path}` : 'https://via.placeholder.com/92x138.png?text=N/A';
      // 使用 NImage 组件以支持懒加载和预览
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
      // 保持您原来的日期格式化逻辑
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
        'MISSING': { type: 'warning', text: '缺失' }, // 将缺失状态改为 warning，更符合语义
      };
      const info = statusMap[row.status] || { type: 'error', text: '未知' };
      return h(NTag, { type: info.type, size: 'small', round: true }, { default: () => info.text });
    }
  },
  // ★★★ 新增“操作”列 ★★★
  {
    title: '操作',
    key: 'actions',
    width: 100,
    render(row) {
      // 只在状态为 'MISSING' 时显示按钮
      if (row.status === 'MISSING') {
        return h(
          NButton,
          {
            size: 'small',
            type: 'primary',
            ghost: true,
            loading: subscribingMediaId.value === row.id, // 绑定行内加载状态
            disabled: !!subscribingMediaId.value, // 当有任何一项在订阅时，禁用其他按钮
            onClick: () => handleSubscribe(row.id),
          },
          { default: () => '订阅' }
        );
      }
      return null; // 其他状态不渲染任何内容
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