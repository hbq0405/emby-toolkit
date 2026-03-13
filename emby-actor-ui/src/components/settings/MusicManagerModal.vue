<!-- src/components/settings/MusicManagerModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="音乐库管理" style="width: 600px;">
    <n-space vertical :size="20">
      <n-alert type="success" :show-icon="true">
        音乐库独立于影视分类规则。上传的音乐将自动在本地 <b>/音乐库</b> 目录下生成 1:1 的 STRM 文件结构。
      </n-alert>

      <n-form label-placement="left" label-width="120">
        <n-form-item label="音乐库根目录">
          <n-input-group>
            <n-input 
              :value="musicConfig.p115_music_root_name || musicConfig.p115_music_root_cid" 
              placeholder="请选择 115 网盘中的音乐主目录" 
              readonly 
              @click="emitOpenFolderSelector"
            >
              <template #prefix><n-icon :component="FolderIcon" color="#f0a020" /></template>
            </n-input>
            <n-button type="primary" ghost @click="emitOpenFolderSelector">选择</n-button>
          </n-input-group>
        </n-form-item>
      </n-form>

      <n-divider title-placement="left" style="margin: 0;">上传音乐</n-divider>

      <n-upload
        multiple
        directory-dnd
        :custom-request="handleUpload"
        :disabled="!musicConfig.p115_music_root_cid || musicConfig.p115_music_root_cid === '0'"
      >
        <n-upload-dragger>
          <div style="margin-bottom: 12px">
            <n-icon size="48" :depth="3" :component="CloudUploadIcon" />
          </div>
          <n-text style="font-size: 16px">点击或者拖动文件/文件夹到该区域来上传</n-text>
          <n-p depth="3" style="margin: 8px 0 0 0">
            支持批量上传文件夹，将自动在 115 和本地创建对应的目录结构并生成 STRM。
          </n-p>
        </n-upload-dragger>
      </n-upload>

      <n-divider title-placement="left" style="margin: 0;">全局操作</n-divider>

      <n-button 
        block 
        type="primary" 
        size="large" 
        @click="triggerSync" 
        :loading="isSyncing"
        :disabled="!musicConfig.p115_music_root_cid || musicConfig.p115_music_root_cid === '0'"
      >
        <template #icon><n-icon :component="SyncIcon" /></template>
        全量同步音乐库 STRM
      </n-button>
    </n-space>

    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">关闭</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { NModal, NSpace, NAlert, NForm, NFormItem, NInputGroup, NInput, NButton, NIcon, NDivider, NUpload, NUploadDragger, NText, NP, useMessage } from 'naive-ui';
import { Folder as FolderIcon, CloudUploadOutline as CloudUploadIcon, SyncOutline as SyncIcon } from '@vicons/ionicons5';
import axios from 'axios';

const emit = defineEmits(['open-folder-selector']);
const message = useMessage();

const showModal = ref(false);
const isSyncing = ref(false);
const musicConfig = ref({
  p115_music_root_cid: '0',
  p115_music_root_name: ''
});

// 暴露给父组件的方法
const open = async () => {
  showModal.value = true;
  await loadConfig();
};

const updateFolder = async (cid, name) => {
  musicConfig.value.p115_music_root_cid = cid;
  musicConfig.value.p115_music_root_name = name;
  try {
    await axios.post('/api/p115/music/config', musicConfig.value);
    message.success('音乐库目录已保存');
  } catch (e) {
    message.error('保存失败');
  }
};

defineExpose({ open, updateFolder });

const emitOpenFolderSelector = () => {
  emit('open-folder-selector', musicConfig.value.p115_music_root_cid);
};

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/p115/music/config');
    if (res.data.success) {
      musicConfig.value = res.data.data;
    }
  } catch (e) {
    console.error('加载音乐配置失败', e);
  }
};

const handleUpload = async ({ file, onFinish, onError, onProgress }) => {
  const formData = new FormData();
  formData.append('file', file.file);
  formData.append('target_cid', musicConfig.value.p115_music_root_cid);
  // file.fullPath 包含了文件夹上传时的相对路径
  formData.append('relative_path', file.fullPath || file.name);

  try {
    const res = await axios.post('/api/p115/music/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: ({ percent }) => {
        onProgress({ percent: Math.ceil(percent) });
      }
    });
    if (res.data.success) {
      message.success(res.data.message);
      onFinish();
    } else {
      message.error(res.data.message);
      onError();
    }
  } catch (e) {
    message.error(`上传失败: ${e.response?.data?.message || e.message}`);
    onError();
  }
};

const triggerSync = async () => {
  isSyncing.value = true;
  try {
    const res = await axios.post('/api/p115/music/sync');
    if (res.data.success) {
      message.success(res.data.message);
    }
  } catch (e) {
    message.error('触发同步失败');
  } finally {
    setTimeout(() => { isSyncing.value = false; }, 2000);
  }
};
</script>