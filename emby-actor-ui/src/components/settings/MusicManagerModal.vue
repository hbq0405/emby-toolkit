<template>
  <n-modal v-model:show="showModal" preset="card" title="独立音乐库管理" style="width: 600px;">
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
              @click="emitOpenFolderSelector('music_root', musicConfig.p115_music_root_cid)"
            >
              <template #prefix><n-icon :component="FolderIcon" color="#f0a020" /></template>
            </n-input>
            <n-button type="primary" ghost @click="emitOpenFolderSelector('music_root', musicConfig.p115_music_root_cid)">选择</n-button>
          </n-input-group>
        </n-form-item>
      </n-form>

      <n-divider title-placement="left" style="margin: 0;">上传音乐</n-divider>

      <n-form label-placement="left" label-width="120">
        <n-form-item label="上传目标目录">
          <n-input-group>
            <n-input 
              :value="uploadTargetName || uploadTargetCid" 
              placeholder="请选择上传的具体目标目录" 
              readonly 
              @click="emitOpenFolderSelector('music_upload_target', uploadTargetCid)"
            >
              <template #prefix><n-icon :component="FolderIcon" color="#18a058" /></template>
            </n-input>
            <n-button type="primary" ghost @click="emitOpenFolderSelector('music_upload_target', uploadTargetCid)">选择</n-button>
          </n-input-group>
        </n-form-item>
      </n-form>

      <!-- ★ 增加 ref，用于调用清空列表方法 -->
      <n-upload
        ref="uploadRef"
        multiple
        directory-dnd
        :custom-request="handleUpload"
        :disabled="!uploadTargetCid || uploadTargetCid === '0'"
      >
        <n-upload-dragger>
          <div style="margin-bottom: 12px">
            <n-icon size="48" :depth="3" :component="CloudUploadIcon" />
          </div>
          <n-text style="font-size: 16px">点击或者拖动文件/文件夹到该区域来上传</n-text>
          <n-p depth="3" style="margin: 8px 0 0 0">
            支持批量上传文件夹。上传成功后列表会显示<strong style="color: #18a058;">绿勾</strong>，失败显示<strong style="color: #d03050;">红叉</strong>。
          </n-p>
        </n-upload-dragger>
      </n-upload>
      
      <!-- ★ 增加清空列表按钮 -->
      <n-space justify="end" style="margin-top: -10px;">
        <n-button size="small" @click="clearUploadList" type="default" dashed>
          清空上传列表
        </n-button>
      </n-space>

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
import { ref, watch } from 'vue';
import { NModal, NSpace, NAlert, NForm, NFormItem, NInputGroup, NInput, NButton, NIcon, NDivider, NUpload, NUploadDragger, NText, NP, useMessage } from 'naive-ui';
import { Folder as FolderIcon, CloudUploadOutline as CloudUploadIcon, SyncOutline as SyncIcon } from '@vicons/ionicons5';
import axios from 'axios';

const emit = defineEmits(['open-folder-selector']);
const message = useMessage();

const showModal = ref(false);
const isSyncing = ref(false);
const uploadRef = ref(null); // ★ 绑定 upload 组件

const musicConfig = ref({
  p115_music_root_cid: '0',
  p115_music_root_name: ''
});

const uploadTargetCid = ref('0');
const uploadTargetName = ref('');

watch(() => musicConfig.value.p115_music_root_cid, (newVal) => {
  if (newVal && newVal !== '0' && (!uploadTargetCid.value || uploadTargetCid.value === '0')) {
    uploadTargetCid.value = newVal;
    uploadTargetName.value = musicConfig.value.p115_music_root_name;
  }
});

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
    
    if (!uploadTargetCid.value || uploadTargetCid.value === '0') {
      uploadTargetCid.value = cid;
      uploadTargetName.value = name;
    }
  } catch (e) {
    message.error('保存失败');
  }
};

const updateUploadTarget = (cid, name) => {
  uploadTargetCid.value = cid;
  uploadTargetName.value = name;
};

defineExpose({ open, updateFolder, updateUploadTarget });

const emitOpenFolderSelector = (context, cid) => {
  emit('open-folder-selector', context, cid);
};

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/p115/music/config');
    if (res.data.success) {
      musicConfig.value = res.data.data;
      if (!uploadTargetCid.value || uploadTargetCid.value === '0') {
        uploadTargetCid.value = musicConfig.value.p115_music_root_cid;
        uploadTargetName.value = musicConfig.value.p115_music_root_name;
      }
    }
  } catch (e) {
    console.error('加载音乐配置失败', e);
  }
};

// ★ 清空上传列表方法
const clearUploadList = () => {
  if (uploadRef.value) {
    uploadRef.value.clear();
  }
};

const handleUpload = async ({ file, onFinish, onError, onProgress }) => {
  const formData = new FormData();
  formData.append('file', file.file);
  formData.append('target_cid', uploadTargetCid.value);
  formData.append('relative_path', file.fullPath || file.name);

  // ★ 优化：给个初始进度 10%，表示已进入后端流控排队队列
  onProgress({ percent: 10 });

  try {
    const res = await axios.post('/api/p115/music/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: ({ loaded, total }) => {
        // ★ 优化：将真实的上传进度映射到 10% ~ 95% 之间，留 5% 给后端生成 STRM 的时间
        const percent = 10 + Math.floor((loaded / total) * 85);
        onProgress({ percent });
      }
    });
    
    if (res.data.success) {
      onProgress({ percent: 100 });
      onFinish(); // ★ 调用这个，列表里就会打上绿色的勾！
      // ★ 优化：取消单文件成功的弹窗，防止批量上传时满屏弹窗
    } else {
      message.error(`${file.name} 失败: ${res.data.message}`);
      onError(); // ★ 调用这个，列表里就会标红打叉
    }
  } catch (e) {
    message.error(`${file.name} 失败: ${e.response?.data?.message || e.message}`);
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