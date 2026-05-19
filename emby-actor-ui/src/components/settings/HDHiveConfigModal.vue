<!-- src/components/settings/HDHiveConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 影巢 (HDHive)" style="width: 720px;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="120">
        <n-divider title-placement="left">第三方应用授权中转</n-divider>

        <div style="padding: 12px; background-color: rgba(24, 160, 88, 0.06); border-radius: 8px; border: 1px dashed var(--n-success-color); margin-bottom: 16px;">
          <n-text depth="3" style="display: block; font-size: 12px; line-height: 1.8;">
            推荐模式：EmbyToolKit 访问你的 VPS 授权中转，由 VPS 使用固定出口 IP 调用影巢 OpenAPI。<br>
            中转地址示例：https://hdhive.847977.xyz；密钥对应 VPS .env 里的 ETK_SHARED_SECRET。
          </n-text>
        </div>

        <n-form-item label="中转地址">
          <n-input
            v-model:value="relayBaseUrl"
            placeholder="例如：https://hdhive.847977.xyz"
            clearable
          />
        </n-form-item>

        <n-form-item label="中转密钥">
          <n-input
            v-model:value="relaySecret"
            type="password"
            :placeholder="relaySecretConfigured ? '已配置，留空则保持不变' : '输入 ETK_SHARED_SECRET'"
            show-password-on="click"
          />
        </n-form-item>

        <n-form-item label="授权状态">
          <n-space align="center" wrap>
            <n-tag :type="relayAuthorized ? 'success' : 'warning'" :bordered="false">
              {{ relayAuthorized ? '已授权' : '未授权或状态未知' }}
            </n-tag>
            <n-tag type="info" :bordered="false" v-if="relayStatus?.scope">
              Scope: {{ relayStatus.scope }}
            </n-tag>
            <n-button
              secondary
              type="primary"
              size="small"
              :disabled="!authorizeUrl"
              @click="openAuthorize"
            >
              前往影巢授权
            </n-button>
          </n-space>
        </n-form-item>

        <n-collapse>
          <n-collapse-item title="兼容旧版个人 API Key（可选，不推荐第三方应用使用）" name="legacy">
            <n-form-item label="X-API-Key">
              <n-input v-model:value="apiKey" type="password" placeholder="个人 API Key / 绑定用户的应用 Key" show-password-on="click" />
            </n-form-item>
          </n-collapse-item>
        </n-collapse>

        <n-form-item label="解锁频率限制" feedback="本地二次保护。服务端返回 429 时仍应以 Retry-After 为准。">
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
            保存并连接
          </n-button>
        </n-form-item>
      </n-form>

      <div v-if="userInfo" style="margin-top: 16px;">
        <n-divider style="margin: 12px 0;" />
        <n-space vertical size="large">
          <n-space align="center" :size="24">
            <n-tag type="success" :bordered="false">
              用户: {{ userInfo.nickname || '未知用户' }}
            </n-tag>
            <n-tag type="warning" :bordered="false">
              积分: {{ userInfo.user_meta?.points || '未知 (需Premium)' }}
            </n-tag>
            <n-tag type="info" :bordered="false" v-if="quotaInfo">
              今日剩余请求: {{ quotaInfo.endpoint_remaining ?? '未知' }}
            </n-tag>
          </n-space>

          <n-space align="center">
            <n-button type="primary" secondary @click="doCheckin(false)" :loading="checkingIn">
              每日签到
            </n-button>
            <n-button type="error" secondary @click="doCheckin(true)" :loading="checkingIn">
              赌狗签到
            </n-button>
          </n-space>
        </n-space>
      </div>
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

const relayBaseUrl = ref('');
const relaySecret = ref('');
const relaySecretConfigured = ref(false);
const relayStatus = ref(null);
const authorizeUrl = ref('');

const apiKey = ref('');

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

const relayAuthorized = computed(() => {
  return Boolean(relayStatus.value?.has_access_token || userInfo.value);
});

const open = async () => {
  showModal.value = true;
  loading.value = true;
  relaySecret.value = '';

  try {
    const res = await axios.get('/api/subscription/hdhive/config');
    if (res.data.success) {
      apiKey.value = res.data.api_key || '';

      relayBaseUrl.value = res.data.relay_base_url || '';
      relaySecretConfigured.value = Boolean(res.data.relay_secret_configured);
      relayStatus.value = res.data.relay_status || null;
      authorizeUrl.value = res.data.authorize_url || '';

      unlockLimitCount.value = res.data.unlock_limit_count || 3;
      unlockLimitWindow.value = res.data.unlock_limit_window || 60;
      userInfo.value = res.data.user_info;
      quotaInfo.value = res.data.quota_info;

      hdhiveFreeOnly.value = res.data.hdhive_free_only ?? false;
      hdhiveMaxPoints.value = res.data.hdhive_max_points ?? 10;
      hdhiveMaxSizeGb.value = res.data.hdhive_max_size_gb ?? 120;
      hdhiveResolution.value = res.data.hdhive_resolution || 'All';
      hdhiveZhSubOnly.value = res.data.hdhive_zh_sub_only ?? true;
      hdhiveExcludeIso.value = res.data.hdhive_exclude_iso ?? false;
    }
  } catch (e) {
    message.error('获取配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  if (!relayBaseUrl.value && !apiKey.value) {
    return message.warning('请输入影巢中转地址，或填写旧版 API Key');
  }

  if (relayBaseUrl.value && !relaySecret.value && !relaySecretConfigured.value) {
    return message.warning('请输入中转密钥 ETK_SHARED_SECRET');
  }

  saving.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/config', {
      api_key: apiKey.value,

      relay_base_url: relayBaseUrl.value,
      relay_secret: relaySecret.value,

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
      message.success(res.data.message);
      userInfo.value = res.data.user_info;
      quotaInfo.value = res.data.quota_info;
      relayStatus.value = res.data.relay_status || relayStatus.value;
      authorizeUrl.value = res.data.authorize_url || authorizeUrl.value;
      relaySecret.value = '';
      relaySecretConfigured.value = Boolean(relayBaseUrl.value);
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    saving.value = false;
  }
};

const openAuthorize = () => {
  if (!authorizeUrl.value) {
    return message.warning('请先保存中转地址和密钥');
  }
  window.open(authorizeUrl.value, '_blank');
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
