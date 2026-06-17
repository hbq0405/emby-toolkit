<!-- src/components/settings/HDHiveConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 影巢 (HDHive)" style="width: 720px;" class="custom-modal glass-modal">
    <n-spin :show="loading">
      
      <!-- 顶部提示 -->
      <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
        影巢已切换为第三方应用授权模式，点击授权后会跳转到影巢官方页面获取授权信息。
      </n-alert>

      <!-- 模块1：账号与授权 -->
      <n-card size="small" title="账号与授权" style="margin-bottom: 16px;" :bordered="true">
        <template #header-extra>
          <n-space>
            <n-button v-if="!authorized" type="primary" color="#f0a020" size="small" @click="openAuthorize" :loading="authorizing">
              前往授权
            </n-button>
            <n-popconfirm v-else positive-text="确认清除" negative-text="取消" @positive-click="clearAuthorization">
              <template #trigger>
                <n-button type="error" ghost size="small" :loading="clearingAuth">清除授权</n-button>
              </template>
              清除授权后需要重新前往影巢授权，是否继续？
            </n-popconfirm>
            <n-button size="small" @click="open" :loading="loading">刷新状态</n-button>
          </n-space>
        </template>

        <n-descriptions label-placement="left" :column="2" bordered size="small">
          <n-descriptions-item label="授权状态">
            <n-tag :type="authorized ? 'success' : 'warning'" size="small" :bordered="false">
              {{ authorized ? '已授权' : '未授权或已过期' }}
            </n-tag>
          </n-descriptions-item>
          
          <n-descriptions-item label="当前用户">
            <template v-if="userInfo">
              {{ displayUsername }}
              <n-tag type="info" size="small" :bordered="false" v-if="displayUserLevel" style="margin-left: 6px;">
                {{ displayUserLevel }}
              </n-tag>
            </template>
            <n-text depth="3" v-else>暂无</n-text>
          </n-descriptions-item>

          <n-descriptions-item label="VIP 权益" :span="2" v-if="vipInfo">
            <n-space align="center" :size="16">
              <n-tag type="warning" size="small" :bordered="false">
                {{ vipInfo.is_forever_vip ? '永久 Premium' : 'Premium' }}
              </n-tag>
              
              <n-text>
                每周免费解锁: 
                <n-text type="success" strong v-if="vipInfo.weekly_free_quota_unlimited">无限</n-text>
                <!-- ★ 修改点：分母加上了 bonus_quota -->
                <n-text type="success" strong v-else>
                  {{ vipInfo.weekly_free_quota_remaining }} / {{ vipInfo.weekly_free_quota + (vipInfo.bonus_quota || 0) }}
                </n-text>
              </n-text>
              
              <!-- ★ 修改点：优化了文案，加上括号作为补充说明 -->
              <n-text v-if="vipInfo.bonus_quota > 0" depth="3" style="font-size: 12px;">
                (含累积奖励: <n-text type="info" strong>{{ vipInfo.bonus_quota }}</n-text>)
              </n-text>
            </n-space>
          </n-descriptions-item>

          <n-descriptions-item label="授权范围" :span="2" v-if="authorized && scopeDisplayText">
            <n-text depth="2">{{ scopeDisplayText }}</n-text>
          </n-descriptions-item>

          <n-descriptions-item label="今日用量" :span="2" v-if="usageToday">
            <n-space align="center" :size="16">
              <n-text>总计: <n-text strong>{{ usageToday.total_calls || 0 }}</n-text></n-text>
              <n-text>成功: <n-text type="success" strong>{{ usageToday.success_calls || 0 }}</n-text></n-text>
              <n-text>失败: <n-text :type="(usageToday.failed_calls || 0) > 0 ? 'error' : 'default'" strong>{{ usageToday.failed_calls || 0 }}</n-text></n-text>
              <n-text>平均耗时: <n-text strong>{{ Math.round(usageToday.avg_latency || 0) }}</n-text> 毫秒</n-text>
              
              <n-text depth="3" style="font-size: 12px; margin-left: 8px;">(应用级数据，非个人账号配额)</n-text>
            </n-space>
          </n-descriptions-item>
        </n-descriptions>
      </n-card>

      <n-form label-placement="left" label-width="110">
        <!-- 模块2：基础配置 -->
        <n-card size="small" title="基础配置" style="margin-bottom: 16px;" :bordered="true">
          <n-form-item label="自动签到方式" feedback="后台定时签到任务会按这里选择的方式执行，默认普通签到。">
            <n-select
              v-model:value="hdhiveCheckinMode"
              :options="[{ label: '普通签到', value: 'normal' }, { label: '赌狗签到', value: 'gambler' }]"
              style="max-width: 220px;"
            />
          </n-form-item>

          <n-form-item label="解锁频率限制" feedback="本地二次保护。服务端返回 429 时仍以 Retry-After 为准。">
            <n-space align="center">
              <n-input-number v-model:value="unlockLimitCount" :min="1" placeholder="次数" style="width: 120px;">
                <template #suffix>次</template>
              </n-input-number>
              <n-text depth="3">/</n-text>
              <n-input-number v-model:value="unlockLimitWindow" :min="1" placeholder="秒数" style="width: 120px;">
                <template #suffix>秒</template>
              </n-input-number>
            </n-space>
          </n-form-item>
        </n-card>

        <!-- 模块3：资源筛选规则 -->
        <n-card size="small" title="资源筛选规则" :bordered="true">
          <template #header-extra>
            <n-text depth="3" style="font-size: 12px;">防止误扣高额积分或下载超大资源</n-text>
          </template>
          
          <n-grid :x-gap="24" :y-gap="8" :cols="2">
            <n-grid-item>
              <n-form-item label="仅免费">
                <n-switch v-model:value="hdhiveFreeOnly" />
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="分辨率偏好">
                <n-select
                  v-model:value="hdhiveResolution"
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
                <n-input-number v-model:value="hdhiveMaxPoints" :min="0" :disabled="hdhiveFreeOnly">
                  <template #suffix>分</template>
                </n-input-number>
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="最大体积">
                <n-input-number v-model:value="hdhiveMaxSizeGb" :min="1">
                  <template #suffix>GB</template>
                </n-input-number>
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="仅含中文字幕">
                <n-switch v-model:value="hdhiveZhSubOnly" />
              </n-form-item>
            </n-grid-item>

            <n-grid-item>
              <n-form-item label="排除原盘">
                <n-switch v-model:value="hdhiveExcludeIso" />
              </n-form-item>
            </n-grid-item>
          </n-grid>
        </n-card>
      </n-form>

      <!-- 底部操作区 -->
      <n-space justify="end" style="margin-top: 24px;">
        <template v-if="authorized">
          <n-button secondary type="primary" @click="doCheckin(false)" :loading="checkingIn">每日签到</n-button>
          <n-button secondary type="error" @click="doCheckin(true)" :loading="checkingIn">赌狗签到</n-button>
        </template>
        <n-button type="primary" color="#f0a020" @click="saveConfig" :loading="saving" style="margin-left: 12px;">
          保存配置
        </n-button>
      </n-space>

    </n-spin>
  </n-modal>
</template>

<script setup>
import { computed, onBeforeUnmount, ref } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);
const checkingIn = ref(false);
const authorizing = ref(false);
const clearingAuth = ref(false);

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
const usageToday = ref(null);
const vipInfo = ref(null); 

let authPollTimer = null;

const authorized = computed(() => {
  return Boolean(relayStatus.value?.has_access_token || userInfo.value);
});

const displayUsername = computed(() => {
  if (!userInfo.value) return '未知用户';
  return (
    userInfo.value.username ||
    userInfo.value.nickname ||
    userInfo.value.name ||
    (userInfo.value.id ? `用户 ${userInfo.value.id}` : '未知用户')
  );
});

const displayUserLevel = computed(() => {
  const level = userInfo.value?.level || userInfo.value?.user_level || '';
  const map = {
    normal: '普通用户',
    vip: 'VIP 用户',
    forever_vip: '永久 VIP',
    lifetime_vip: '永久 VIP',
    premium: 'Premium',
  };
  return map[level] || level || '';
});

const scopeLabelMap = {
  meta: '用量与配额',
  query: '查询资源',
  unlock: '解锁资源',
  vip: 'VIP 信息',
  write: '签到/写入',
};

const normalizeScopes = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  return String(value || '')
    .split(/\s+/)
    .map(s => s.trim())
    .filter(Boolean);
};

const scopeDisplayText = computed(() => {
  const scopes = normalizeScopes(
    relayStatus.value?.scopes || relayStatus.value?.scope || ''
  );

  const order = ['meta', 'query', 'unlock', 'vip', 'write'];
  const sorted = [
    ...order.filter(s => scopes.includes(s)),
    ...scopes.filter(s => !order.includes(s)),
  ];

  return sorted
    .map(s => scopeLabelMap[s] || s)
    .join('、');
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
    await open(false);

    if (authorized.value) {
      stopAuthPolling();
      message.success('影巢授权已完成');
    }

    if (count >= 30) {
      stopAuthPolling();
    }
  }, 2000);
};

const open = async (showLoading = true) => {
  showModal.value = true;
  if (showLoading) loading.value = true;

  try {
    const res = await axios.get('/api/subscription/hdhive/config');
    if (res.data.success) {
      relayStatus.value = res.data.relay_status || null;
      authorizeUrl.value = res.data.authorize_url || '';

      hdhiveCheckinMode.value = res.data.hdhive_checkin_mode || 'normal';
      unlockLimitCount.value = res.data.unlock_limit_count || 3;
      unlockLimitWindow.value = res.data.unlock_limit_window || 60;
      userInfo.value = res.data.user_info || null;
      usageToday.value = res.data.usage_today || null; 
      vipInfo.value = res.data.vip_info || null;

      hdhiveFreeOnly.value = res.data.hdhive_free_only ?? false;
      hdhiveMaxPoints.value = res.data.hdhive_max_points ?? 10;
      hdhiveMaxSizeGb.value = res.data.hdhive_max_size_gb ?? 120;
      hdhiveResolution.value = res.data.hdhive_resolution || 'All';
      hdhiveZhSubOnly.value = res.data.hdhive_zh_sub_only ?? true;
      hdhiveExcludeIso.value = res.data.hdhive_exclude_iso ?? false;
    } else {
      message.error(res.data.message || '获取影巢配置失败');
    }
  } catch (e) {
    message.error('获取影巢配置失败');
  } finally {
    if (showLoading) loading.value = false;
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
    message.info('授权完成后会自动刷新状态，或手动点击“刷新状态”');
    startAuthPolling();
  } catch (e) {
    message.error('打开影巢授权失败');
  } finally {
    authorizing.value = false;
  }
};

const clearAuthorization = async () => {
  clearingAuth.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/clear_authorization');
    if (res.data.success) {
      message.success(res.data.message || '影巢授权已清除');
      stopAuthPolling();
      relayStatus.value = null;
      userInfo.value = null;
      usageToday.value = null;
      vipInfo.value = null; 
      await open(false);
    } else {
      message.error(res.data.message || '清除授权失败');
    }
  } catch (e) {
    message.error('清除授权失败');
  } finally {
    clearingAuth.value = false;
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
      hdhive_exclude_iso: hdhiveExcludeIso.value,
    });

    if (res.data.success) {
      message.success(res.data.message || '保存成功');
      relayStatus.value = res.data.relay_status || relayStatus.value;
      authorizeUrl.value = res.data.authorize_url || authorizeUrl.value;
      hdhiveCheckinMode.value = res.data.hdhive_checkin_mode || hdhiveCheckinMode.value;
      userInfo.value = res.data.user_info || userInfo.value;
      usageToday.value = res.data.usage_today || usageToday.value;
      vipInfo.value = res.data.vip_info || vipInfo.value; 
    } else {
      message.error(res.data.message || '保存失败');
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    saving.value = false;
  }
};

const doCheckin = async (isGambler) => {
  checkingIn.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/checkin', { is_gambler: isGambler });
    if (res.data.success) {
      message.success(res.data.message, { duration: 5000 });
      await open(false);
    } else {
      message.warning(res.data.message || '签到失败');
    }
  } catch (e) {
    message.error('签到请求失败');
  } finally {
    checkingIn.value = false;
  }
};

onBeforeUnmount(() => {
  stopAuthPolling();
});

defineExpose({ open });
</script>

<style scoped>
/* 移除了旧的自定义样式，全部采用 naive-ui 的 n-card 和 n-alert 组件来实现更统一的视觉效果 */
</style>
