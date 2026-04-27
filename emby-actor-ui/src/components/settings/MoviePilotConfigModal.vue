<!-- src/components/settings/MoviePilotConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 MoviePilot" style="width: 600px;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="140">
        <n-form-item label="MoviePilot URL">
          <n-input v-model:value="formModel.moviepilot_url" placeholder="例如: http://192.168.1.100:3000"/>
        </n-form-item>
        <n-form-item label="用户名">
          <n-input v-model:value="formModel.moviepilot_username" placeholder="登录用户名"/>
        </n-form-item>
        <n-form-item label="密码">
          <n-input type="password" show-password-on="mousedown" v-model:value="formModel.moviepilot_password" placeholder="登录密码"/>
        </n-form-item>
        <n-form-item label="辅助识别">
          <n-switch v-model:value="formModel.moviepilot_recognition" />
          <template #feedback>
            <n-text depth="3" style="font-size:0.8em;">
              开启后，整理网盘资源时，当正则无法识别文件名时，将优先调用 MP 的接口进行识别。
            </n-text>
          </template>
        </n-form-item>
        <n-divider title-placement="left" style="margin: 10px 0 20px 0;">联动删除（待开发）</n-divider>
        <n-form-item label="删除整理记录">
          <n-switch v-model:value="formModel.link_delete_transfer_history" />
          <template #feedback>
            <n-text depth="3" style="font-size:0.8em;">
              神医深度删除通知触发时，同步删除 MoviePilot 中匹配的整理记录。
            </n-text>
          </template>
        </n-form-item>
        <n-form-item label="删除种子及源文件">
          <n-switch v-model:value="formModel.link_delete_download_files" />
          <template #feedback>
            <n-text depth="3" style="font-size:0.8em;">
              神医深度删除通知触发时，根据整理记录关联的 Hash 清理 MP 下载任务，并请求删除下载源文件。
            </n-text>
          </template>
        </n-form-item>
        <n-divider title-placement="left" style="margin: 10px 0 20px 0;">每日订阅额度</n-divider>
        <n-form-item label="每日订阅上限">
          <n-input-number v-model:value="formModel.resubscribe_daily_cap" :min="1" style="width: 100%;" />
          <template #feedback><n-text depth="3" style="font-size:0.8em;">超过数量停止任务，0点重置。</n-text></template>
        </n-form-item>
        <n-form-item label="订阅请求间隔 (秒)">
          <n-input-number v-model:value="formModel.resubscribe_delay_seconds" :min="0.1" :step="0.1" style="width: 100%;" />
          <template #feedback><n-text depth="3" style="font-size:0.8em;">避免请求过快冲击服务器。</n-text></template>
        </n-form-item>
      </n-form>
    </n-spin>
    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">取消</n-button>
        <n-button type="primary" @click="saveConfig" :loading="saving">保存配置</n-button>
      </n-space>
    </template>
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

const formModel = ref({
  moviepilot_url: '',
  moviepilot_username: '',
  moviepilot_password: '',
  moviepilot_recognition: false,
  link_delete_transfer_history: false,
  link_delete_download_files: false,
  resubscribe_daily_cap: 10,
  resubscribe_delay_seconds: 2.0
});

const open = async () => {
  showModal.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/subscription/mp/config');
    if (res.data.success) {
      formModel.value = res.data.data;
    }
  } catch (e) {
    message.error('获取配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  saving.value = true;
  try {
    const res = await axios.post('/api/subscription/mp/config', formModel.value);
    if (res.data.success) {
      message.success(res.data.message);
      showModal.value = false;
    }
  } catch (e) {
    message.error('保存配置失败');
  } finally {
    saving.value = false;
  }
};

defineExpose({ open });
</script>