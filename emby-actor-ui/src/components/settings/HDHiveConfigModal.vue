<!-- src/components/settings/HDHiveConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 影巢 (HDHive)" style="width: 600px;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="100">
        <n-form-item label="X-API-Key">
          <n-input v-model:value="apiKey" type="password" placeholder="输入影巢 X-API-Key" show-password-on="click" />
        </n-form-item>
        
        <n-form-item label="解锁频率限制" feedback="防止触发影巢 429 限制，多线程下载时会自动排队等待">
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
              今日剩余请求: {{ quotaInfo.endpoint_remaining ?? '无限' }}
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
import { ref } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);
const checkingIn = ref(false);

const apiKey = ref('');
const unlockLimitCount = ref(3);
const unlockLimitWindow = ref(60);
const userInfo = ref(null);
const quotaInfo = ref(null);

const open = async () => {
  showModal.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/subscription/hdhive/config');
    if (res.data.success) {
      apiKey.value = res.data.api_key;
      unlockLimitCount.value = res.data.unlock_limit_count || 3;
      unlockLimitWindow.value = res.data.unlock_limit_window || 60;
      userInfo.value = res.data.user_info;
      quotaInfo.value = res.data.quota_info;
    }
  } catch (e) {
    message.error('获取配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  if (!apiKey.value) return message.warning("请输入 API Key");
  saving.value = true;
  try {
    const res = await axios.post('/api/subscription/hdhive/config', { 
      api_key: apiKey.value,
      unlock_limit_count: unlockLimitCount.value,
      unlock_limit_window: unlockLimitWindow.value
    });
    if (res.data.success) {
      message.success(res.data.message);
      userInfo.value = res.data.user_info;
      quotaInfo.value = res.data.quota_info;
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error("保存失败");
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
      open(); // 刷新积分
    } else {
      message.warning(res.data.message);
    }
  } catch (e) {
    message.error("签到请求失败");
  } finally {
    checkingIn.value = false;
  }
};

defineExpose({ open });
</script>