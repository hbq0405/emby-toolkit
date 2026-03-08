<!-- src/components/settings/P115ShareMountModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="115 分享挂载 (STRM)" style="width: 650px;">
    <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
      自动解析 115 分享链接，利用 P115Center 中心缓存极速生成本地 STRM 文件。播放时将自动动态转存。
    </n-alert>
    <n-form label-placement="left" label-width="140">
      <n-form-item label="启用分享挂载">
        <n-switch v-model:value="config.p115_share_enabled" />
      </n-form-item>
      
      <!-- ★ 新增：分享转存目录 -->
      <n-form-item label="分享转存目录">
        <n-input-group>
          <n-input
            :value="config.p115_share_transfer_name || config.p115_share_transfer_cid"
            placeholder="选择用于存放转存文件的目录"
            readonly
            @click="$emit('openFolderSelector', 'share_transfer', config.p115_share_transfer_cid)"
          >
            <template #prefix><n-icon :component="FolderIcon" /></template>
          </n-input>
          <n-button type="primary" ghost @click="$emit('openFolderSelector', 'share_transfer', config.p115_share_transfer_cid)">
            选择
          </n-button>
        </n-input-group>
        <template #feedback>
          <n-text depth="3" style="font-size:0.8em;">播放时，视频会自动转存到此目录并获取直链。</n-text>
        </template>
      </n-form-item>

      <n-form-item label="STRM 保存根目录">
        <n-input v-model:value="config.p115_share_local_dir" placeholder="例如: /mnt/media/shares" :disabled="!config.p115_share_enabled" />
        <template #feedback>
          <n-text depth="3" style="font-size:0.8em;">分享生成的 STRM 将保存在此目录下，按分享标题建文件夹。</n-text>
        </template>
      </n-form-item>
      <n-form-item label="分享链接列表">
        <n-dynamic-input 
          v-model:value="config.p115_share_links" 
          placeholder="输入 115 分享链接 (例如: https://115.com/s/xxxx?password=yyyy)" 
          :min="0"
          :disabled="!config.p115_share_enabled"
        />
      </n-form-item>
    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">取消</n-button>
        <n-button type="primary" @click="runShareSync" :loading="isSyncing" :disabled="!config.p115_share_enabled">
          保存并立即同步
        </n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { useMessage, NModal, NAlert, NForm, NFormItem, NSwitch, NInput, NInputGroup, NText, NDynamicInput, NSpace, NButton, NIcon } from 'naive-ui';
import { FolderOutline as FolderIcon } from '@vicons/ionicons5';
import axios from 'axios';

const props = defineProps({
  config: {
    type: Object,
    required: true
  }
});
const emit = defineEmits(['saveConfig', 'openFolderSelector']);

const showModal = ref(false);
const isSyncing = ref(false);
const message = useMessage();

const open = () => {
  if (!props.config.p115_share_links) {
    props.config.p115_share_links = [];
  }
  showModal.value = true;
};

const runShareSync = async () => {
  if (!props.config.p115_share_transfer_cid) {
    message.warning('请选择分享转存目录！');
    return;
  }
  if (!props.config.p115_share_local_dir) {
    message.warning('请填写 STRM 保存根目录！');
    return;
  }
  if (!props.config.p115_share_links || props.config.p115_share_links.length === 0) {
    message.warning('请至少添加一个分享链接！');
    return;
  }

  isSyncing.value = true;
  try {
    emit('saveConfig');
    const response = await axios.post('/api/p115/share_sync');
    if (response.data.success) {
      message.success(response.data.message || '分享同步任务已在后台启动！');
      showModal.value = false;
    } else {
      message.error(response.data.message || '启动失败');
    }
  } catch (error) {
    message.error(error.response?.data?.message || '请求失败，请检查后端日志');
  } finally {
    isSyncing.value = false;
  }
};

defineExpose({ open });
</script>