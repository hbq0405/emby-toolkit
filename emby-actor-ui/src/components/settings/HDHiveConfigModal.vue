<!-- src/components/settings/HDHiveConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 影巢 (HDHive)" style="width: 680px;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="110">
        <div style="padding: 12px; background-color: rgba(24, 160, 88, 0.06); border-radius: 8px; border: 1px dashed var(--n-success-color); margin-bottom: 16px;">
          <n-text depth="3" style="display: block; font-size: 12px; line-height: 1.8;">
            影巢已切换为第三方应用授权模式，点击授权后会跳转到影巢官方页面获取授权信息。
          </n-text>
        </div>

        <n-form-item label="授权状态">
          <n-space align="center" wrap>
            <n-tag :type="authorized ? 'success' : 'warning'" :bordered="false">
              {{ authorized ? '已授权' : '未授权或授权已过期' }}
            </n-tag>
            <n-tag type="info" :bordered="false" v-if="permissionText">
              权限：{{ permissionText }}
            </n-tag>
            <n-button type="primary" color="#f0a020" @click="openAuthorize" :loading="authorizing">
              前往影巢授权
            </n-button>
            <n-button secondary @click="open" :loading="loading">
              刷新状态
            </n-button>
            <n-popconfirm
              v-if="authorized"
              positive-text="确认清除"
              negative-text="取消"
              @positive-click="clearAuthorization"
            >
              <template #trigger>
                <n-button tertiary type="error" :loading="clearingAuthorization">
                  清除授权
                </n-button>
              </template>
              清除后当前影巢授权会失效，需要重新点击“前往影巢授权”。
            </n-popconfirm>
          </n-space>
        </n-form-item>

        <div v-if="userInfo" style="margin-bottom: 16px;">
          <n-space align="center" :size="16" wrap>
            <n-tag type="success" :bordered="false">
              用户：{{ userDisplayName }}
            </n-tag>
            <n-tag type="info" :bordered="false" v-if="userLevelText">
              等级：{{ userLevelText }}
            </n-tag>
            <n-tag type="info" :bordered="false" v-if="quotaInfo">
              今日剩余请求：{{ quotaInfo.endpoint_remaining ?? '未知' }}
            </n-tag>
          </n-space>
        </div>

        <n-form-item label="自动签到方式" feedback="后台定时签到任务会按这里选择的方式执行，默认普通签到。">
          <n-select
            v-model:value="hdhiveCheckinMode"
            :options="[
              { label: '普通签到', value: 'normal' },
              { label: '赌狗签到', value: 'gambler' }
            ]"
            style="max-width: 220px;"
          />
        </n-form-item>

        <n-form-item label="解锁频率限制" feedback="本地二次保护。服务端返回 429 时仍以 Retry-After 为准。">
          <n-space align="center">
            <n-input-number v-model:value="unlockLimitCount" :min="1" placeholder="次数" style="width: 120px;">
              <template #suffix>次</template>
            </n-input-number>
            <span>/</span>
            <n-input-number v-model:value="unlockLimitWindow" :min="1" placeholder="秒数" style="width: 120px;">
              <template #suffix>秒</template>
            </n-input-number>
          </n-space>
        </n-form-item>

        <n-divider title-placement="left">资源筛选规则</n-divider>

        <div style="padding: 12px; background-color: rgba(240, 160, 32, 0.05); border-radius: 8px; border: 1px dashed var(--n-warning-color); margin-bottom: 16px;">
          <n-text depth="3" style="display: block; margin-bottom: 12px; font-size: 12px;">
            防止一键整理/影巢优先订阅误扣高额积分或下载超大资源。
          </n-text>

          <n-grid :x-gap="12" :y-gap="0" :cols="2">
            <n-grid-item>
              <n-form-item label="仅免费">
                <n-switch v-model:value="hdhiveFreeOnly" size="small" />
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="分辨率偏好">
                <n-select
                  v-model:value="hdhiveResolution"
                  size="small"
                  :options="[
                    { label: '不限制', value: 'All' },
                    { label: '仅 4K', value: '4K' },
                    { label: '仅 1080p', value: '1080p' }
                  ]"
                />
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="最大积分">
                <n-input-number
                  v-model:value="hdhiveMaxPoints"
                  size="small"
                  :min="0"
                  :disabled="hdhiveFreeOnly"
                >
                  <template #suffix>分</template>
                </n-input-number>
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="最大体积">
                <n-input-number v-model:value="hdhiveMaxSizeGb" size="small" :min="1">
                  <template #suffix>GB</template>
                </n-input-number>
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="仅含中文字幕">
                <n-switch v-model:value="hdhiveZhSubOnly" size="small" />
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="排除原盘">
                <n-switch v-model:value="hdhiveExcludeIso" size="small" />
              </n-form-item>
            </n-grid-item>
          </n-grid>
        </div>

        <n-form-item>
          <n-button type="primary" color="#f0a020" @click="saveConfig" :loading="saving" block>
            保存配置
          </n-button>
        </n-form-item>

        <n-space align="center" v-if="authorized">
          <n-button type="primary" secondary @click="doCheckin(false)" :loading="checkingIn">
            每日签到
          </n-button>
          <n-button type="error" secondary @click="doCheckin(true)" :loading="checkingIn">
            赌狗签到
          </n-button>
        </n-space>
      </n-form>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { computed, ref } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);
const checkingIn = ref(false);
const authorizing = ref(false);
const clearingAuthorization = ref(false);

const relayStatus = ref(null);
const authorizeUrl = ref('');

const hdhiveCheckinMode = ref('normal');

const hdhiveFreeOnly = ref(false);
const hdhiveMaxPoints = ref(10);
const hdhiveMaxSizeGb = ref(120);
const hdhiveResolution = ref('All');
const hdhiveZhSubOnly = ref(true);
const hdhiveExcludeIso = ref(false);

const unlockLimitCount = ref(3);
const unlockLimitWindow = ref(60);
const userInfo = ref(null);
const quotaInfo = ref(null);

let authPollTimer = null;

const authorized = computed(() => {
  return Boolean(relayStatus.value?.has_access_token || userInfo.value);
});

const scopeLabelMap = {
  meta: '元信息',
  query: '资源查询',
  unlock: '资源解锁',
  vip: '用户信息与签到',
  write: '签到/写入'
};

const normalizeScopes = (value) => {
  if (Array.isArray(value)) {
    return value.filter(Boolean);
  }
  return String(value || '')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
};

const permissionText = computed(() => {
  const scopes = relayStatus.value?.scopes?.length
    ? relayStatus.value.scopes
    : normalizeScopes(relayStatus.value?.scope);

  const uniqueScopes = [...new Set(scopes)];
  return uniqueScopes.map((item) => scopeLabelMap[item] || item).join('、');
});

const userDisplayName = computed(() => {
  const info = userInfo.value || {};
  return info.username || info.nickname || info.name || (info.id ? `用户 ${info.id}` : '未知用户');
});

const userLevelText = computed(() => {
  const level = userInfo.value?.level;
  const map = {
    normal: '普通用户',
    vip: 'VIP 用户',
    premium: 'Premium 用户',
    forever_vip: '长期 VIP',
    forever: '长期 VIP',
    blocked: '已封禁'
  };
  return map[level] || level || '';
});

const stopAuthPolling = () => {
  if (authPollTimer) {
    clearInterval(authPollTimer);
    authPollTimer = null;
  }
};

const startAuthPolling = () => {
  stopAuthPolling();
  let count = 0;

  authPollTimer = setInterval(async () => {
    count += 1;
    await open();

    if (authorized.value) {
      stopAuthPolling();
      message.success('影巢授权已完成');
      return;
    }

    if (count >= 30) {
      stopAuthPolling();
    }
  }, 2000);
};

const open = async () => {
  showModal.value = true;
  loading.value = true;

  try {
    const res = await axios.get('/api/subscription/hdhive/config');
    if (res.data.success) {
      relayStatus.value = res.data.relay_status || null;
      authorizeUrl.value = res.data.authorize_url || '';

      hdhiveCheckinMode.value = res.data.hdhive_checkin_mode || 'normal';
      unlockLimitCount.value = res.data.unlock_limit_count || 3;
      unlockLimitWindow.value = res.data.unlock_limit_window || 60;
      userInfo.value = res.data.user_info || null;
      quotaInfo.value = res.data.quota_info || null;

      hdhiveFreeOnly.value = res.data.hdhive_free_only ?? false;
      hdhiveMaxPoints.value = res.data.hdhive_max_points ?? 10;
      hdhiveMaxSizeGb.value = res.data.hdhive_max_size_gb ?? 120;
      hdhiveResolution.value = res.data.hdhive_resolution || 'All';
      hdhiveZhSubOnly.value = res.data.hdhive_zh_sub_only ?? true;
      hdhiveExcludeIso.value = res.data.hdhive_exclude_iso ?? false;
    }
  } catch (e) {
    message.error('获取影巢配置失败');
  } finally {
    loading.value = false;
  }
};

const openAuthorize = async () => {
  authorizing.value = true;
  try {
    let url = authorizeUrl.value;
    if (!url) {
      const res = await axios.get('/api/subscription/hdhive/authorize_url');
      if (res.data.success) {
        url = res.data.authorize_url;
      }
    }

    if (!url) {
      message.error('生成影巢授权链接失败');
      return;
    }

    window.open(url, '_blank', 'noopener,noreferrer');
    startAuthPolling();
    message.info('授权完成后返回本页会自动刷新状态');
  } catch (e) {
    message.error('打开影巢授权失败');
  } finally {
    authorizing.value = false;
  }
};

const saveConfig = async () => {
  saving.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/config', {
      hdhive_checkin_mode: hdhiveCheckinMode.value,
      unlock_limit_count: unlockLimitCount.value,
      unlock_limit_window: unlockLimitWindow.value,

      hdhive_free_only: hdhiveFreeOnly.value,
      hdhive_max_points: hdhiveMaxPoints.value,
      hdhive_max_size_gb: hdhiveMaxSizeGb.value,
      hdhive_resolution: hdhiveResolution.value,
      hdhive_zh_sub_only: hdhiveZhSubOnly.value,
      hdhive_exclude_iso: hdhiveExcludeIso.value
    });
    if (res.data.success) {
      message.success(res.data.message || '保存成功');
      relayStatus.value = res.data.relay_status || relayStatus.value;
      authorizeUrl.value = res.data.authorize_url || authorizeUrl.value;
      hdhiveCheckinMode.value = res.data.hdhive_checkin_mode || hdhiveCheckinMode.value;
      userInfo.value = res.data.user_info || null;
      quotaInfo.value = res.data.quota_info || null;
    } else {
      message.error(res.data.message || '保存失败');
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    saving.value = false;
  }
};

const clearAuthorization = async () => {
  clearingAuthorization.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/clear_authorization');
    if (res.data.success) {
      stopAuthPolling();
      relayStatus.value = res.data.relay_status || null;
      authorizeUrl.value = res.data.authorize_url || authorizeUrl.value;
      userInfo.value = null;
      quotaInfo.value = null;
      message.success(res.data.message || '影巢授权已清除');
    } else {
      message.error(res.data.message || '清除授权失败');
    }
  } catch (e) {
    message.error('清除授权失败');
  } finally {
    clearingAuthorization.value = false;
  }
};

const doCheckin = async (isGambler) => {
  checkingIn.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/checkin', { is_gambler: isGambler });
    if (res.data.success) {
      message.success(res.data.message, { duration: 5000 });
      open();
    } else {
      message.warning(res.data.message);
    }
  } catch (e) {
    message.error('签到请求失败');
  } finally {
    checkingIn.value = false;
  }
};

defineExpose({ open });
</script>
