<template>
  <n-modal v-model:show="showModal" preset="card" title="灏忓彿 Cookie 姹? style="width: 760px; max-width: calc(100vw - 24px);" :mask-closable="false" class="custom-modal glass-modal">
    <n-space vertical :size="16">
      <n-alert type="info" :show-icon="true">
        鏈夊彲鐢ㄥ皬鍙锋椂锛屾挱鏀鹃摼璺細浼樺厛浣跨敤灏忓彿绉掍紶骞惰幏鍙栫洿閾撅紱娌℃湁鍙敤灏忓彿浼氬洖閫€涓昏处鍙锋挱鏀俱€?      </n-alert>

      <div class="play-pool-toolbar">
        <n-space align="center">
          <n-switch v-model:value="playPoolConfig.enabled" @update:value="savePlayPoolEnabled">
            <template #checked>鍚敤灏忓彿姹?/template>
            <template #unchecked>鍏抽棴灏忓彿姹?/template>
          </n-switch>
          <n-tag :type="playPoolConfig.usable_count > 0 ? 'success' : 'default'" size="small">
            鍙敤 {{ playPoolConfig.usable_count || 0 }} / {{ playPoolConfig.accounts.length }}
          </n-tag>
          <n-tag size="small" :bordered="false">{{ playPoolConfig.temp_dir_name || 'ETK灏忓彿鎾斁涓存椂鐩綍' }}</n-tag>
        </n-space>
        <n-button size="small" tertiary @click="loadPlayPoolConfig" :loading="playPoolLoading">
          <template #icon><n-icon :component="RefreshIcon" /></template>
          鍒锋柊
        </n-button>
      </div>

      <n-card :bordered="false" size="small" class="play-pool-form-card">
        <n-space vertical>
          <n-space :vertical="isMobile" :size="12" style="width: 100%;">
            <n-input v-model:value="playPoolAccountForm.alias" placeholder="灏忓彿鍒悕锛屼緥濡傦細灏忓彿A" style="min-width: 180px;" />
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
              <template #checked>鍚敤</template>
              <template #unchecked>鍋滅敤</template>
            </n-switch>
            <n-switch v-model:value="playPoolAccountForm.shared" :disabled="playPoolAccountForm.owner_type !== 'user'">
              <template #checked>共享</template>
              <template #unchecked>仅本人</template>
            </n-switch>
            <n-input-number
              v-model:value="playPoolAccountForm.auto_speedtest_threshold_mbps"
              :min="0"
              :precision="2"
              placeholder="测速阈值 MB/s"
              style="width: 150px;"
            />
            <n-switch v-model:value="playPoolAccountForm.auto_speedtest_enabled">
              <template #checked>自动测速</template>
              <template #unchecked>关闭</template>
            </n-switch>
            <n-input-number
              v-model:value="playPoolAccountForm.daily_traffic_limit_gb"
              :min="0"
              :precision="0"
              placeholder="鍗曟棩涓婇檺 GB"
              style="width: 140px;"
            />
          </n-space>

          <n-input
            v-model:value="playPoolAccountForm.cookie"
            type="textarea"
            placeholder="绮樿创灏忓彿 Cookie锛屾垨鎵爜鍚庤嚜鍔ㄥ洖濉細UID=...; CID=...; SEID=..."
            :rows="3"
          />

          <div class="play-pool-user-scope">
            <n-text depth="2" class="play-pool-field-label">鎸囧畾鐢ㄦ埛鎴栫敤鎴风粍锛堟ā鏉挎簮锛?/n-text>
            <n-select
              v-model:value="playPoolAccountForm.allowed_user_ids"
              multiple
              filterable
              clearable
              placeholder="榛樿瀵规墍鏈夌敤鎴峰彲鐢?
              :options="embyUserOptions"
              :loading="embyUsersLoading"
              :render-label="renderUserOption"
            />
          </div>

          <n-space justify="space-between" :vertical="isMobile">
            <n-space>
              <n-button secondary @click="refreshPlayPoolQrcode" :loading="playPoolQrcodeLoading">
                <template #icon><n-icon :component="RefreshIcon" /></template>
                鎵爜鑾峰彇 Cookie
              </n-button>
              <n-button v-if="playPoolAccountForm.id" tertiary @click="resetPlayPoolAccountForm">鍙栨秷缂栬緫</n-button>
            </n-space>
            <n-button type="primary" @click="savePlayPoolAccount" :loading="playPoolSaving">
              <template #icon><n-icon :component="AddIcon" /></template>
              {{ playPoolAccountForm.id ? '淇濆瓨灏忓彿' : '娣诲姞灏忓彿' }}
            </n-button>
          </n-space>

          <div v-if="playPoolQrcodeStatus !== 'idle'" class="play-pool-qrcode">
            <n-spin v-if="playPoolQrcodeStatus === 'loading'" size="small">
              <template #description>姝ｅ湪鑾峰彇浜岀淮鐮?..</template>
            </n-spin>
            <template v-else-if="playPoolQrcodeStatus === 'waiting' || playPoolQrcodeStatus === 'success'">
              <n-qr-code v-if="playPoolQrcodeUrl" :value="playPoolQrcodeUrl" :size="160" />
              <n-alert v-if="playPoolQrcodeStatus === 'waiting'" type="info" :show-icon="true">
                浣跨敤 115 鐢熸椿 APP 鎵爜锛屾垚鍔熷悗 Cookie 浼氬洖濉埌涓婃柟杈撳叆妗嗐€?              </n-alert>
              <n-alert v-if="playPoolQrcodeStatus === 'success'" type="success" :show-icon="true">
                Cookie 宸茶幏鍙栵紝璇风‘璁ゅ埆鍚嶅悗淇濆瓨銆?              </n-alert>
            </template>
            <n-result v-else-if="playPoolQrcodeStatus === 'expired'" status="warning" title="浜岀淮鐮佸凡杩囨湡">
              <template #footer>
                <n-button type="primary" @click="refreshPlayPoolQrcode">閲嶆柊鑾峰彇</n-button>
              </template>
            </n-result>
          </div>
        </n-space>
      </n-card>

      <div class="play-pool-list">
        <div v-if="!playPoolConfig.accounts.length" class="play-pool-empty">
          鏆傛湭閰嶇疆灏忓彿 Cookie
        </div>
        <div v-for="account in playPoolConfig.accounts" :key="account.id" class="play-pool-account">
          <div class="play-pool-account-main">
            <n-space align="center" :size="8">
              <strong>{{ account.alias || '灏忓彿' }}</strong>
              <n-tag size="small" :type="account.enabled && account.cookie_mask ? 'success' : 'default'">
                {{ account.enabled ? '鍚敤' : '鍋滅敤' }}
              </n-tag>
              <n-tag v-if="account.daily_traffic_limited" size="small" type="warning">浠婃棩杈鹃檺</n-tag>
              <n-tag size="small" :bordered="false">{{ account.cookie_mask || '鏈厤缃?Cookie' }}</n-tag>
              <n-tag size="small" :bordered="false" type="info">{{ accountScopeText(account) }}</n-tag>
            </n-space>
            <n-space class="play-pool-stats" :size="12">
              <span>閫熷害锛歿{ account.last_speed_text || '鏈祴閫? }}</span>
              <span>鎾斁锛歿{ account.play_count || 0 }} 娆?/span>
              <span>浠婃棩锛歿{ account.daily_traffic_text || '0 B' }}{{ account.daily_traffic_limit_text ? ` / ${account.daily_traffic_limit_text}` : '' }}</span>
              <span>鍘嗗彶锛歿{ account.traffic_text || '0 B' }}</span>
            </n-space>
            <n-text v-if="account.last_error" type="error" depth="3" class="play-pool-error">{{ account.last_error }}</n-text>
          </div>
          <n-space>
            <n-button size="small" tertiary @click="editPlayPoolAccount(account)">缂栬緫</n-button>
            <n-button size="small" secondary @click="speedtestPlayPoolAccount(account)" :loading="playPoolSpeedTestingId === account.id">
              娴嬮€?            </n-button>
            <n-button size="small" tertiary type="error" @click="deletePlayPoolAccount(account)">鍒犻櫎</n-button>
          </n-space>
        </div>
      </div>
    </n-space>

    <template #footer>
      <n-button @click="close">鍏抽棴</n-button>
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
  temp_dir_name: 'ETK灏忓彿鎾斁涓存椂鐩綍',
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
  daily_traffic_limit_gb: null,
  auto_speedtest_enabled: true,
  auto_speedtest_threshold_mbps: null,
  allowed_user_ids: []
});

const cookieAppOptions = [
  { label: '鏀粯瀹濆皬绋嬪簭', value: 'alipaymini' },
  { label: '缃戦〉鐗?, value: 'web' },
  { label: '寰俊灏忕▼搴?, value: 'wechatmini' },
  { label: '瀹夊崜鐢佃绔?, value: 'tv' }
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
      { default: () => '妯℃澘婧? }
    )
  ];
};

const accountScopeText = (account) => {
  const allowed = Array.isArray(account?.allowed_user_ids) ? account.allowed_user_ids : [];
  if (!allowed.length) return '鎵€鏈夌敤鎴?;
  const optionMap = new Map(embyUserOptions.value.map(item => [item.value, item.label]));
  const names = allowed.map(id => optionMap.get(id) || id).filter(Boolean);
  if (!names.length) return `鎸囧畾 ${allowed.length} 浜篳;
  return names.length <= 2 ? names.join('銆?) : `${names.slice(0, 2).join('銆?)} 绛?${names.length} 浜篳;
};

const loadEmbyUsers = async () => {
  embyUsersLoading.value = true;
  try {
    const res = await axios.get('/api/custom_collections/config/emby_users');
    embyUserOptions.value = Array.isArray(res.data) ? res.data : [];
  } catch (e) {
    message.error('鍔犺浇 Emby 鐢ㄦ埛澶辫触: ' + (e.response?.data?.message || e.message));
  } finally {
    embyUsersLoading.value = false;
  }
};

const applyConfig = (data) => {
  playPoolConfig.value = {
    enabled: Boolean(data?.enabled),
    usable_count: Number(data?.usable_count || 0),
    temp_dir_name: data?.temp_dir_name || 'ETK灏忓彿鎾斁涓存椂鐩綍',
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
    message.error('鍔犺浇灏忓彿姹犲け璐? ' + (e.response?.data?.message || e.message));
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
    daily_traffic_limit_gb: null,
    auto_speedtest_enabled: true,
    auto_speedtest_threshold_mbps: null,
    allowed_user_ids: []
  };
};

const savePlayPoolEnabled = async (enabled) => {
  try {
    const res = await axios.post('/api/p115/play_pool', { enabled });
    if (res.data?.success && res.data.data) {
      applyConfig(res.data.data);
      emit('updated', res.data.data);
    }
    message.success(enabled ? '灏忓彿姹犲凡鍚敤' : '灏忓彿姹犲凡鍏抽棴');
  } catch (e) {
    message.error('淇濆瓨灏忓彿姹犲紑鍏冲け璐? ' + (e.response?.data?.message || e.message));
    await loadPlayPoolConfig();
  }
};

const savePlayPoolAccount = async () => {
  const form = playPoolAccountForm.value;
  const cookie = String(form.cookie || '').trim();
  if (!form.id && !cookie) {
    message.warning('璇峰厛鎵爜鎴栫矘璐村皬鍙?Cookie');
    return;
  }
  playPoolSaving.value = true;
  try {
    const payload = {
      alias: String(form.alias || '灏忓彿').trim() || '灏忓彿',
      app_type: form.app_type || 'alipaymini',
      owner_type: form.owner_type || 'admin',
      shared: form.owner_type === 'user' ? Boolean(form.shared) : true,
      enabled: Boolean(form.enabled),
      daily_traffic_limit_gb: Number(form.daily_traffic_limit_gb || 0),
      auto_speedtest_enabled: Boolean(form.auto_speedtest_enabled),
      auto_speedtest_threshold_mbps: Number(form.auto_speedtest_threshold_mbps || 0),
      allowed_user_ids: Array.isArray(form.allowed_user_ids) ? form.allowed_user_ids : []
    };
    if (cookie) payload.cookie = cookie;
    if (form.id) {
      await axios.put(`/api/p115/play_pool/accounts/${form.id}`, payload);
      message.success('灏忓彿宸蹭繚瀛?);
    } else {
      await axios.post('/api/p115/play_pool/accounts', payload);
      message.success('灏忓彿宸叉坊鍔?);
    }
    resetPlayPoolAccountForm();
    await loadPlayPoolConfig();
  } catch (e) {
    message.error('淇濆瓨灏忓彿澶辫触: ' + (e.response?.data?.message || e.message));
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
    alias: account.alias || '灏忓彿',
    cookie: '',
    app_type: account.app_type || 'alipaymini',
    owner_type: account.owner_type || 'admin',
    shared: Boolean(account.shared),
    enabled: Boolean(account.enabled),
    daily_traffic_limit_gb: Number(account.daily_traffic_limit_gb || 0) || null,
    auto_speedtest_enabled: account.auto_speedtest_enabled !== false,
    auto_speedtest_threshold_mbps: Number(account.auto_speedtest_threshold_mbps || 0) || null,
    allowed_user_ids: Array.isArray(account.allowed_user_ids) ? [...account.allowed_user_ids] : []
  };
};

const deletePlayPoolAccount = (account) => {
  dialog.warning({
    title: '鍒犻櫎灏忓彿',
    content: `纭畾鍒犻櫎鈥?{account.alias || '灏忓彿'}鈥濆悧锛焋,
    positiveText: '鍒犻櫎',
    negativeText: '鍙栨秷',
    onPositiveClick: async () => {
      try {
        await axios.delete(`/api/p115/play_pool/accounts/${account.id}`);
        message.success('灏忓彿宸插垹闄?);
        await loadPlayPoolConfig();
      } catch (e) {
        message.error('鍒犻櫎澶辫触: ' + (e.response?.data?.message || e.message));
      }
    }
  });
};

const speedtestPlayPoolAccount = async (account) => {
  playPoolSpeedTestingId.value = account.id;
  try {
    const res = await axios.post(`/api/p115/play_pool/accounts/${account.id}/speedtest`);
    message.success(`灏忓彿娴嬮€熷畬鎴愶細${res.data?.data?.speed_text || '宸插畬鎴?}`);
    await loadPlayPoolConfig();
  } catch (e) {
    message.error('灏忓彿娴嬮€熷け璐? ' + (e.response?.data?.message || e.message));
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
      message.error(res.data?.message || '鑾峰彇灏忓彿浜岀淮鐮佸け璐?);
    }
  } catch (e) {
    playPoolQrcodeStatus.value = 'error';
    message.error('鑾峰彇灏忓彿浜岀淮鐮佸け璐? ' + (e.response?.data?.message || e.message));
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
        message.success('灏忓彿 Cookie 鑾峰彇鎴愬姛');
      } else if (res.data?.status === 'expired') {
        playPoolQrcodeStatus.value = 'expired';
        stopPlayPoolQrcodePolling();
      }
    } catch (e) {
      console.error('妫€鏌ュ皬鍙?Cookie 浜岀淮鐮佺姸鎬佸け璐?, e);
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


