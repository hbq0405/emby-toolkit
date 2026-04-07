<!-- src/components/settings/HDHiveConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 影巢 (HDHive)" style="width: 600px;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="100">
        <n-form-item label="X-API-Key">
          <n-input-group>
            <n-input v-model:value="apiKey" type="password" placeholder="输入影巢 X-API-Key" show-password-on="click" />
            <n-button type="primary" color="#f0a020" @click="saveConfig" :loading="saving">保存并连接</n-button>
          </n-input-group>
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
const userInfo = ref(null);
const quotaInfo = ref(null);

const open = async () => {
  showModal.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/subscription/hdhive/config');
    if (res.data.success) {
      apiKey.value = res.data.api_key;
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
    const res = await axios.post('/api/subscription/hdhive/config', { api_key: apiKey.value });
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