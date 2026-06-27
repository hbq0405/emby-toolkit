<template>
  <n-modal v-model:show="showModal" preset="card" title="小号 Cookie 池" style="width: 760px; max-width: calc(100vw - 24px);" :mask-closable="false" class="custom-modal glass-modal">
    <n-space vertical :size="16">
      <n-alert type="info" :show-icon="true">
        有可用小号时，播放链路会优先使用小号秒传并获取直链；没有可用小号会回退主账号播放。
      </n-alert>

      <div class="play-pool-toolbar">
        <n-space align="center">
          <n-switch v-model:value="playPoolConfig.enabled" @update:value="savePlayPoolSettings">
            <template #checked>启用小号池</template>
            <template #unchecked>关闭小号池</template>
          </n-switch>
          <n-tag :type="playPoolConfig.usable_count > 0 ? 'success' : 'default'" size="small">
            可用 {{ playPoolConfig.usable_count || 0 }} / {{ playPoolConfig.accounts.length }}
          </n-tag>
          <n-tag size="small" :bordered="false">{{ playPoolConfig.temp_dir_name || 'ETK小号播放临时目录' }}</n-tag>
        </n-space>
        <n-button size="small" tertiary @click="loadPlayPoolConfig" :loading="playPoolLoading">
          <template #icon><n-icon :component="RefreshIcon" /></template>
          刷新
        </n-button>
      </div>

      <div class="play-pool-global-settings">
        <n-space align="center" :vertical="isMobile" :size="12">
          <n-switch v-model:value="playPoolConfig.auto_speedtest_enabled" @update:value="savePlayPoolSettings">
            <template #checked>自动测速</template>
            <template #unchecked>关闭测速</template>
          </n-switch>
          <n-input-number
            v-model:value="playPoolConfig.auto_speedtest_threshold_mbps"
            :min="0"
            :precision="2"
            placeholder="测速阈值 MB/s"
            style="width: 150px;"
            @blur="savePlayPoolSettings"
          />
          <n-input-number
            v-model:value="playPoolConfig.daily_traffic_limit_gb"
            :min="0"
            :precision="0"
            placeholder="单日上限 GB"
            style="width: 150px;"
            @blur="savePlayPoolSettings"
          />
        </n-space>
      </div>

      <n-card :bordered="false" size="small" class="play-pool-form-card">
        <n-space vertical>
          <n-space :vertical="isMobile" :size="12" style="width: 100%;">
            <n-input v-model:value="playPoolAccountForm.alias" placeholder="小号别名，例如：小号A" style="min-width: 180px;" />
            <n-select
              v-model:value="playPoolAccountForm.app_type"
              :options="cookieAppOptions"
              style="width: 180px;"
              @update:value="handlePlayPoolAppTypeChange"
            />
            <n-select
              v-model:value="playPoolAccountForm.owner_type"
              :options="ownerTypeOptions"
              style="width: 140px;"
            />
            <n-switch v-model:value="playPoolAccountForm.enabled">
              <template #checked>启用</template>
              <template #unchecked>停用</template>
            </n-switch>
            <n-switch v-model:value="playPoolAccountForm.shared" :disabled="playPoolAccountForm.owner_type !== 'user'">
              <template #checked>共享</template>
              <template #unchecked>仅本人</template>
            </n-switch>
          </n-space>

          <n-input
            v-model:value="playPoolAccountForm.cookie"
            type="textarea"
            placeholder="粘贴小号 Cookie，或扫码后自动回填：UID=...; CID=...; SEID=..."
            :rows="3"
          />

          <div class="play-pool-user-scope">
            <n-text depth="2" class="play-pool-field-label">指定用户或用户组（模板源）</n-text>
            <n-select
              v-model:value="playPoolAccountForm.allowed_user_ids"
              multiple
              filterable
              clearable
              placeholder="默认对所有用户可用"
              :options="embyUserOptions"
              :loading="embyUsersLoading"
              :render-label="renderUserOption"
            />
          </div>

          <n-space justify="space-between" :vertical="isMobile">
            <n-space>
              <n-button secondary @click="refreshPlayPoolQrcode" :loading="playPoolQrcodeLoading">
                <template #icon><n-icon :component="RefreshIcon" /></template>
                扫码获取 Cookie
              </n-button>
              <n-button v-if="playPoolAccountForm.id" tertiary @click="resetPlayPoolAccountForm">取消编辑</n-button>
            </n-space>
            <n-button type="primary" @click="savePlayPoolAccount" :loading="playPoolSaving">
              <template #icon><n-icon :component="AddIcon" /></template>
              {{ playPoolAccountForm.id ? '保存小号' : '添加小号' }}
            </n-button>
          </n-space>

          <div v-if="playPoolQrcodeStatus !== 'idle'" class="play-pool-qrcode">
            <n-spin v-if="playPoolQrcodeStatus === 'loading'" size="small">
              <template #description>正在获取二维码...</template>
            </n-spin>
            <template v-else-if="playPoolQrcodeStatus === 'waiting' || playPoolQrcodeStatus === 'success'">
              <n-qr-code v-if="playPoolQrcodeUrl" :value="playPoolQrcodeUrl" :size="160" />
              <n-alert v-if="playPoolQrcodeStatus === 'waiting'" type="info" :show-icon="true">
                使用 115 生活 APP 扫码，成功后 Cookie 会回填到上方输入框。
              </n-alert>
              <n-alert v-if="playPoolQrcodeStatus === 'success'" type="success" :show-icon="true">
                Cookie 已获取，请确认别名后保存。
              </n-alert>
            </template>
            <n-result v-else-if="playPoolQrcodeStatus === 'expired'" status="warning" title="二维码已过期">
              <template #footer>
                <n-button type="primary" @click="refreshPlayPoolQrcode">重新获取</n-button>
              </template>
            </n-result>
          </div>
        </n-space>
      </n-card>

      <div class="play-pool-list">
        <div v-if="!playPoolConfig.accounts.length" class="play-pool-empty">
          暂未配置小号 Cookie
        </div>
        <div v-for="account in playPoolConfig.accounts" :key="account.id" class="play-pool-account">
          <div class="play-pool-account-main">
            <n-space align="center" :size="8">
              <strong>{{ account.alias || '小号' }}</strong>
              <n-tag size="small" :type="account.enabled && account.cookie_mask ? 'success' : 'default'">
                {{ account.enabled ? '启用' : '停用' }}
              </n-tag>
              <n-tag v-if="account.daily_traffic_limited" size="small" type="warning">今日达限</n-tag>
              <n-tag size="small" :bordered="false">{{ account.cookie_mask || '未配置 Cookie' }}</n-tag>
              <n-tag size="small" :bordered="false" type="info">{{ accountScopeText(account) }}</n-tag>
            </n-space>
            <n-space class="play-pool-stats" :size="12">
              <span>速度：{{ account.last_speed_text || '未测速' }}</span>
              <span>播放：{{ account.play_count || 0 }} 次</span>
              <span>今日：{{ account.daily_traffic_text || '0 B' }}{{ account.daily_traffic_limit_text ? ` / ${account.daily_traffic_limit_text}` : '' }}</span>
              <span>历史：{{ account.traffic_text || '0 B' }}</span>
            </n-space>
            <n-text v-if="account.last_error" type="error" depth="3" class="play-pool-error">{{ account.last_error }}</n-text>
          </div>
          <n-space>
            <n-button size="small" tertiary @click="editPlayPoolAccount(account)">编辑</n-button>
            <n-button size="small" secondary @click="speedtestPlayPoolAccount(account)" :loading="playPoolSpeedTestingId === account.id">
              测速
            </n-button>
            <n-button size="small" tertiary type="error" @click="deletePlayPoolAccount(account)">删除</n-button>
          </n-space>
        </div>
      </div>
    </n-space>

    <template #footer>
      <n-button @click="close">关闭</n-button>
    </template>
  </n-modal>
</template>

<script setup>
import { h, ref, onMounted, onUnmounted } from 'vue';
import { NTag, useMessage, useDialog } from 'naive-ui';
import { Add as AddIcon, RefreshOutline as RefreshIcon } from '@vicons/ionicons5';
import axios from 'axios';

const emit = defineEmits(['updated']);
const message = useMessage();
const dialog = useDialog();

const showModal = ref(false);
const isMobile = ref(false);
const playPoolLoading = ref(false);
const playPoolSaving = ref(false);
const playPoolSpeedTestingId = ref('');
const playPoolQrcodeUrl = ref('');
const playPoolQrcodeStatus = ref('idle');
const playPoolQrcodeLoading = ref(false);
const playPoolQrcodePolling = ref(null);
const playPoolQrcodeUid = ref('');
const embyUsersLoading = ref(false);
const embyUserOptions = ref([]);
const playPoolConfig = ref({
  enabled: false,
  usable_count: 0,
  temp_dir_name: 'ETK小号播放临时目录',
  auto_speedtest_enabled: true,
  auto_speedtest_threshold_mbps: null,
  daily_traffic_limit_gb: null,
  accounts: []
});
const playPoolAccountForm = ref({
  id: '',
  alias: '',
  cookie: '',
  app_type: 'alipaymini',
  owner_type: 'admin',
  shared: true,
  enabled: true,
  allowed_user_ids: []
});

const cookieAppOptions = [
  { label: '支付宝小程序', value: 'alipaymini' },
  { label: '网页版', value: 'web' },
  { label: '微信小程序', value: 'wechatmini' },
  { label: '安卓电视端', value: 'tv' }
];

const ownerTypeOptions = [
  { label: '管理员小号', value: 'admin' },
  { label: '用户自有', value: 'user' }
];

const updateViewportState = () => {
  if (typeof window === 'undefined') return;
  isMobile.value = window.innerWidth <= 768;
};

const renderUserOption = (option) => {
  if (!option.is_template_source) return option.label;
  return [
    option.label,
    h(
      NTag,
      { type: 'success', size: 'small', bordered: false, style: 'margin-left: 8px;' },
      { default: () => '模板源' }
    )
  ];
};

const accountScopeText = (account) => {
  if (account?.owner_type === 'user' && !account?.shared) {
    return `仅本人：${account.owner_user_name || account.owner_user_id || '未知用户'}`;
  }
  if (account?.owner_type === 'user' && account?.shared) {
    return '所有用户';
  }
  const allowed = Array.isArray(account?.allowed_user_ids) ? account.allowed_user_ids : [];
  if (!allowed.length) return '所有用户';
  const optionMap = new Map(embyUserOptions.value.map(item => [item.value, item.label]));
  const names = allowed.map(id => optionMap.get(id) || id).filter(Boolean);
  if (!names.length) return `指定 ${allowed.length} 人`;
  return names.length <= 2 ? names.join('、') : `${names.slice(0, 2).join('、')} 等 ${names.length} 人`;
};

const loadEmbyUsers = async () => {
  embyUsersLoading.value = true;
  try {
    const res = await axios.get('/api/custom_collections/config/emby_users');
    embyUserOptions.value = Array.isArray(res.data) ? res.data : [];
  } catch (e) {
    message.error('加载 Emby 用户失败: ' + (e.response?.data?.message || e.message));
  } finally {
    embyUsersLoading.value = false;
  }
};

const applyConfig = (data) => {
  playPoolConfig.value = {
    enabled: Boolean(data?.enabled),
    usable_count: Number(data?.usable_count || 0),
    temp_dir_name: data?.temp_dir_name || 'ETK小号播放临时目录',
    auto_speedtest_enabled: data?.auto_speedtest_enabled !== false,
    auto_speedtest_threshold_mbps: Number(data?.auto_speedtest_threshold_mbps || 0) || null,
    daily_traffic_limit_gb: Number(data?.daily_traffic_limit_gb || 0) || null,
    accounts: Array.isArray(data?.accounts) ? data.accounts : []
  };
};

const loadPlayPoolConfig = async () => {
  playPoolLoading.value = true;
  try {
    const res = await axios.get('/api/p115/play_pool');
    if (res.data?.success && res.data.data) {
      applyConfig(res.data.data);
      emit('updated', res.data.data);
    }
  } catch (e) {
    message.error('加载小号池失败: ' + (e.response?.data?.message || e.message));
  } finally {
    playPoolLoading.value = false;
  }
};

const open = async () => {
  showModal.value = true;
  await Promise.all([loadPlayPoolConfig(), loadEmbyUsers()]);
};

const close = () => {
  stopPlayPoolQrcodePolling();
  playPoolQrcodeStatus.value = 'idle';
  playPoolQrcodeUrl.value = '';
  playPoolQrcodeUid.value = '';
  showModal.value = false;
};

const resetPlayPoolAccountForm = () => {
  stopPlayPoolQrcodePolling();
  playPoolQrcodeStatus.value = 'idle';
  playPoolQrcodeUrl.value = '';
  playPoolQrcodeUid.value = '';
  playPoolAccountForm.value = {
    id: '',
    alias: '',
    cookie: '',
    app_type: 'alipaymini',
    owner_type: 'admin',
    shared: true,
    enabled: true,
    allowed_user_ids: []
  };
};

const savePlayPoolSettings = async () => {
  try {
    const res = await axios.post('/api/p115/play_pool', {
      enabled: Boolean(playPoolConfig.value.enabled),
      auto_speedtest_enabled: Boolean(playPoolConfig.value.auto_speedtest_enabled),
      auto_speedtest_threshold_mbps: Number(playPoolConfig.value.auto_speedtest_threshold_mbps || 0),
      daily_traffic_limit_gb: Number(playPoolConfig.value.daily_traffic_limit_gb || 0)
    });
    if (res.data?.success && res.data.data) {
      applyConfig(res.data.data);
      emit('updated', res.data.data);
    }
    message.success('小号池配置已保存');
  } catch (e) {
    message.error('保存小号池配置失败: ' + (e.response?.data?.message || e.message));
    await loadPlayPoolConfig();
  }
};

const savePlayPoolAccount = async () => {
  const form = playPoolAccountForm.value;
  const cookie = String(form.cookie || '').trim();
  if (!form.id && !cookie) {
    message.warning('请先扫码或粘贴小号 Cookie');
    return;
  }
  playPoolSaving.value = true;
  try {
    const payload = {
      alias: String(form.alias || '小号').trim() || '小号',
      app_type: form.app_type || 'alipaymini',
      owner_type: form.owner_type || 'admin',
      shared: form.owner_type === 'user' ? Boolean(form.shared) : true,
      enabled: Boolean(form.enabled),
      allowed_user_ids: Array.isArray(form.allowed_user_ids) ? form.allowed_user_ids : []
    };
    if (cookie) payload.cookie = cookie;
    if (form.id) {
      await axios.put(`/api/p115/play_pool/accounts/${form.id}`, payload);
      message.success('小号已保存');
    } else {
      await axios.post('/api/p115/play_pool/accounts', payload);
      message.success('小号已添加');
    }
    resetPlayPoolAccountForm();
    await loadPlayPoolConfig();
  } catch (e) {
    message.error('保存小号失败: ' + (e.response?.data?.message || e.message));
  } finally {
    playPoolSaving.value = false;
  }
};

const editPlayPoolAccount = (account) => {
  stopPlayPoolQrcodePolling();
  playPoolQrcodeStatus.value = 'idle';
  playPoolQrcodeUrl.value = '';
  playPoolQrcodeUid.value = '';
  playPoolAccountForm.value = {
    id: account.id,
    alias: account.alias || '小号',
    cookie: '',
    app_type: account.app_type || 'alipaymini',
    owner_type: account.owner_type || 'admin',
    shared: Boolean(account.shared),
    enabled: Boolean(account.enabled),
    allowed_user_ids: Array.isArray(account.allowed_user_ids) ? [...account.allowed_user_ids] : []
  };
};

const deletePlayPoolAccount = (account) => {
  dialog.warning({
    title: '删除小号',
    content: `确定删除“${account.alias || '小号'}”吗？`,
    positiveText: '删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.delete(`/api/p115/play_pool/accounts/${account.id}`);
        message.success('小号已删除');
        await loadPlayPoolConfig();
      } catch (e) {
        message.error('删除失败: ' + (e.response?.data?.message || e.message));
      }
    }
  });
};

const speedtestPlayPoolAccount = async (account) => {
  playPoolSpeedTestingId.value = account.id;
  try {
    const res = await axios.post(`/api/p115/play_pool/accounts/${account.id}/speedtest`);
    message.success(`小号测速完成：${res.data?.data?.speed_text || '已完成'}`);
    await loadPlayPoolConfig();
  } catch (e) {
    message.error('小号测速失败: ' + (e.response?.data?.message || e.message));
  } finally {
    playPoolSpeedTestingId.value = '';
  }
};

const refreshPlayPoolQrcode = async () => {
  stopPlayPoolQrcodePolling();
  playPoolQrcodeStatus.value = 'loading';
  playPoolQrcodeLoading.value = true;
  playPoolQrcodeUid.value = '';
  try {
    const appType = playPoolAccountForm.value.app_type || 'alipaymini';
    const res = await axios.get(`/api/p115/play_pool/cookie_qrcode?app=${appType}`);
    if (res.data?.success) {
      playPoolQrcodeUrl.value = res.data.data.qrcode;
      playPoolQrcodeUid.value = res.data.data.uid || '';
      playPoolQrcodeStatus.value = 'waiting';
      startPlayPoolQrcodePolling();
    } else {
      playPoolQrcodeStatus.value = 'error';
      message.error(res.data?.message || '获取小号二维码失败');
    }
  } catch (e) {
    playPoolQrcodeStatus.value = 'error';
    message.error('获取小号二维码失败: ' + (e.response?.data?.message || e.message));
  } finally {
    playPoolQrcodeLoading.value = false;
  }
};

const startPlayPoolQrcodePolling = () => {
  stopPlayPoolQrcodePolling();
  playPoolQrcodePolling.value = setInterval(async () => {
    try {
      const appType = playPoolAccountForm.value.app_type || 'alipaymini';
      const uid = playPoolQrcodeUid.value ? `&uid=${encodeURIComponent(playPoolQrcodeUid.value)}` : '';
      const res = await axios.get(`/api/p115/play_pool/cookie_qrcode/status?app=${appType}${uid}`);
      if (res.data?.status === 'success') {
        playPoolAccountForm.value.cookie = res.data.data?.cookie || '';
        playPoolAccountForm.value.app_type = res.data.data?.app_type || appType;
        playPoolQrcodeStatus.value = 'success';
        stopPlayPoolQrcodePolling();
        message.success('小号 Cookie 获取成功');
      } else if (res.data?.status === 'expired') {
        playPoolQrcodeStatus.value = 'expired';
        stopPlayPoolQrcodePolling();
      }
    } catch (e) {
      console.error('检查小号 Cookie 二维码状态失败', e);
    }
  }, 2000);
};

const stopPlayPoolQrcodePolling = () => {
  if (playPoolQrcodePolling.value) {
    clearInterval(playPoolQrcodePolling.value);
    playPoolQrcodePolling.value = null;
  }
};

const handlePlayPoolAppTypeChange = () => {
  if (playPoolQrcodeStatus.value !== 'idle') {
    refreshPlayPoolQrcode();
  }
};

onMounted(() => {
  updateViewportState();
  if (typeof window !== 'undefined') {
    window.addEventListener('resize', updateViewportState, { passive: true });
  }
});

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    window.removeEventListener('resize', updateViewportState);
  }
  stopPlayPoolQrcodePolling();
});

defineExpose({ open });
</script>

<style scoped>
.play-pool-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.play-pool-form-card {
  background: var(--n-action-color);
}
.play-pool-user-scope {
  display: grid;
  gap: 6px;
}
.play-pool-field-label {
  font-size: 13px;
}
.play-pool-qrcode {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 12px;
  border: 1px solid var(--n-divider-color);
  border-radius: 6px;
}
.play-pool-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.play-pool-empty {
  padding: 20px;
  text-align: center;
  color: var(--n-text-color-3);
  border: 1px dashed var(--n-divider-color);
  border-radius: 6px;
}
.play-pool-account {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding: 12px;
  border: 1px solid var(--n-divider-color);
  border-radius: 6px;
  background: var(--n-color-modal);
}
.play-pool-account-main {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.play-pool-stats {
  color: var(--n-text-color-3);
  font-size: 12px;
  flex-wrap: wrap;
}
.play-pool-error {
  font-size: 12px;
}

@media (max-width: 768px) {
  .play-pool-account,
  .play-pool-qrcode {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>
