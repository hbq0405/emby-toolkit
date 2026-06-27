<!-- src/components/settings/MoviePilotConfigModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="配置 MoviePilot" style="width: min(920px, 92vw);" class="custom-modal glass-modal">
    <n-spin :show="loading">
      <div class="mp-config-grid">
        <n-form label-placement="left" label-width="132" class="mp-config-column">
          <n-divider title-placement="left" style="margin: 0 0 20px 0;">基础配置</n-divider>
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
          <n-divider title-placement="left" style="margin: 10px 0 20px 0;">联动删除</n-divider>
          <n-form-item label="删除整理记录">
            <n-switch v-model:value="formModel.link_delete_transfer_history" />
            <template #feedback>
              <n-text depth="3" style="font-size:0.8em;">
                Emby删除媒体项时，同步删除 MoviePilot 中匹配的整理记录。
              </n-text>
            </template>
          </n-form-item>
          <n-form-item label="删除种子及源文件">
            <n-switch v-model:value="formModel.link_delete_download_files" />
            <template #feedback>
              <n-text depth="3" style="font-size:0.8em;">
                Emby删除媒体项时，同步删除下载器的种子和源文件（含辅种）。
              </n-text>
            </template>
          </n-form-item>
          <n-divider title-placement="left" style="margin: 10px 0 20px 0;">每日订阅额度</n-divider>
          <n-form-item label="每日订阅上限">
            <n-input-number v-model:value="formModel.resubscribe_daily_cap" :min="1" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">超过数量停止任务，0点重置。</n-text></template>
          </n-form-item>
          <n-form-item label="请求间隔 (秒)">
            <n-input-number v-model:value="formModel.resubscribe_delay_seconds" :min="0.1" :step="0.1" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">避免请求过快冲击服务器。</n-text></template>
          </n-form-item>
        </n-form>
        <n-form label-placement="left" label-width="132" class="mp-config-column">
          <n-divider title-placement="left" style="margin: 0 0 20px 0;">策略配置</n-divider>
          <n-form-item label="新片保护期 (天)">
            <n-input-number v-model:value="formModel.movie_protection_days" :min="0" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">发布时间在此天数内的电影启用间歇性搜索机制。超过此天数则视为老片。</n-text></template>
          </n-form-item>
          <n-form-item label="搜索窗口期 (天)">
            <n-input-number v-model:value="formModel.movie_search_window_days" :min="1" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">新增订阅以及每次复活后，连续搜索的天数。</n-text></template>
          </n-form-item>
          <n-form-item label="暂停周期 (天)">
            <n-input-number v-model:value="formModel.movie_pause_days" :min="1" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">搜索无果后，暂停搜索的天数。</n-text></template>
          </n-form-item>
          <n-form-item label="延迟订阅 (天)">
            <n-input-number v-model:value="formModel.delay_subscription_days" :min="0" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">电影上映后 N 天才允许订阅，0 表示不延迟。</n-text></template>
          </n-form-item>
          <n-form-item label="超时复活 (天)">
            <n-input-number v-model:value="formModel.timeout_revive_days" :min="0" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">订阅超时移除的项目在 N 天后自动复活，0 表示关闭。</n-text></template>
          </n-form-item>
          <n-form-item label="下载超时重订 (小时)">
            <n-input-number v-model:value="formModel.download_timeout_hours" :min="0" style="width: 100%;" />
            <template #feedback><n-text depth="3" style="font-size:0.8em;">下载队列超过 N 小时未完成则删除并重新订阅，0 表示关闭。</n-text></template>
          </n-form-item>
        </n-form>
      </div>
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
  resubscribe_delay_seconds: 2.0,
  movie_protection_days: 180,
  movie_search_window_days: 1,
  movie_pause_days: 7,
  delay_subscription_days: 0,
  timeout_revive_days: 0,
  download_timeout_hours: 0
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

<style scoped>
.mp-config-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 24px;
  align-items: start;
}

.mp-config-column {
  min-width: 0;
}

@media (max-width: 760px) {
  .mp-config-grid {
    grid-template-columns: 1fr;
    gap: 8px;
  }
}
</style>
