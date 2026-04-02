<!-- src/components/settings/TGMonitorModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="频道订阅监听配置 (Pro)" style="width: 600px; max-width: 95vw;">
    <n-spin :show="isLoading">
      <n-form label-placement="left" label-width="120">
        
        <n-form-item label="启用监听" path="enabled">
          <n-switch v-model:value="config.enabled" />
        </n-form-item>

        <n-form-item label="订阅类型" path="monitor_types">
          <n-checkbox-group v-model:value="config.monitor_types">
            <n-space>
              <n-checkbox value="movie" label="电影" />
              <n-checkbox value="tv" label="电视剧" />
            </n-space>
          </n-checkbox-group>
        </n-form-item>

        <n-form-item label="API ID" path="api_id">
          <n-input v-model:value="config.api_id" placeholder="例如: 1234567" />
        </n-form-item>
        
        <n-form-item label="API Hash" path="api_hash">
          <n-input v-model:value="config.api_hash" type="password" show-password-on="click" />
        </n-form-item>
        
        <n-form-item label="手机号" path="phone">
          <n-input v-model:value="config.phone" placeholder="带国家代码，例如: +8613800138000" />
        </n-form-item>
        
        <n-form-item label="两步验证(2FA)" path="password">
          <n-input v-model:value="config.password" type="password" show-password-on="click" placeholder="如果没有设置请留空" />
        </n-form-item>
        
        <n-form-item label="白名单频道" path="channels">
          <n-select v-model:value="config.channels" multiple filterable tag placeholder="输入频道 Username 或 ID 并回车 (如 hdtv115)" :options="[]" />
        </n-form-item>

        <n-divider title-placement="left">登录授权</n-divider>
        <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
          修改 API 信息后，请务必先点击右下角的 <b>"保存配置"</b>，然后再获取验证码登录。
        </n-alert>

        <n-form-item label="授权状态">
          <n-space align="center">
            <n-tag :type="userBotStatus === 'authorized' ? 'success' : 'error'">
              {{ userBotStatus === 'authorized' ? '已登录 (监听中)' : '未登录' }}
            </n-tag>
            
            <n-button v-if="userBotStatus !== 'authorized'" type="primary" size="small" @click="sendUserBotCode" :loading="isSendingCode">
              获取验证码
            </n-button>
            <n-button v-else type="error" ghost size="small" @click="logoutUserBot">
              注销账号
            </n-button>
          </n-space>
        </n-form-item>

        <!-- 验证码输入框 -->
        <n-form-item v-if="showCodeInput" label="输入验证码">
          <n-input-group>
            <n-input v-model:value="userBotCode" placeholder="输入 TG 收到的验证码" />
            <n-button type="primary" @click="submitUserBotCode" :loading="isSubmittingCode">确认登录</n-button>
          </n-input-group>
        </n-form-item>

      </n-form>
    </n-spin>

    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">关闭</n-button>
        <n-button type="primary" @click="saveConfig" :loading="isSaving">保存配置</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { 
  NModal, NSpin, NForm, NFormItem, NInput, NSwitch, NCheckboxGroup, NCheckbox, 
  NSpace, NSelect, NDivider, NAlert, NTag, NButton, NInputGroup, useMessage 
} from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const isLoading = ref(false);
const isSaving = ref(false);

const config = ref({
  enabled: false,
  api_id: '',
  api_hash: '',
  phone: '',
  password: '',
  channels: [],
  monitor_types: ['movie', 'tv']
});

// 授权状态
const userBotStatus = ref('unauthorized');
const showCodeInput = ref(false);
const userBotCode = ref('');
const isSendingCode = ref(false);
const isSubmittingCode = ref(false);

// 暴露给父组件调用的方法
const open = async () => {
  showModal.value = true;
  showCodeInput.value = false;
  userBotCode.value = '';
  await fetchConfig();
  await checkUserBotStatus();
};

const fetchConfig = async () => {
  isLoading.value = true;
  try {
    const res = await axios.get('/api/tg_userbot/config');
    if (res.data.success) {
      config.value = res.data.data;
    }
  } catch (e) {
    message.error('读取配置失败');
  } finally {
    isLoading.value = false;
  }
};

const saveConfig = async () => {
  isSaving.value = true;
  try {
    const res = await axios.post('/api/tg_userbot/config', config.value);
    if (res.data.success) {
      message.success(res.data.message);
      await checkUserBotStatus(); // 保存后重新刷新状态
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    isSaving.value = false;
  }
};

// --- 授权相关 API ---
const checkUserBotStatus = async () => {
  try {
    const res = await axios.get('/api/tg_userbot/status');
    if (res.data.success) {
      userBotStatus.value = res.data.data.status;
    }
  } catch (e) {}
};

const sendUserBotCode = async () => {
  if (!config.value.api_id || !config.value.phone) {
    return message.warning('请先填写 API ID 和手机号，并保存配置');
  }
  isSendingCode.value = true;
  try {
    const res = await axios.post('/api/tg_userbot/send_code');
    if (res.data.success) {
      message.success(res.data.message);
      showCodeInput.value = true;
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error(e.response?.data?.message || '请求失败');
  } finally {
    isSendingCode.value = false;
  }
};

const submitUserBotCode = async () => {
  if (!userBotCode.value) return message.warning('请输入验证码');
  isSubmittingCode.value = true;
  try {
    const res = await axios.post('/api/tg_userbot/login', { code: userBotCode.value });
    if (res.data.success) {
      message.success(res.data.message);
      showCodeInput.value = false;
      await checkUserBotStatus();
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error(e.response?.data?.message || '登录失败');
  } finally {
    isSubmittingCode.value = false;
  }
};

const logoutUserBot = async () => {
  try {
    await axios.post('/api/tg_userbot/logout');
    message.success('已注销');
    await checkUserBotStatus();
  } catch (e) {
    message.error('注销失败');
  }
};

defineExpose({ open });
</script>