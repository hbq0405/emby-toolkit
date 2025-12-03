<template>
  <!-- 1. 使用 n-layout 包裹，适配暗色/亮色主题背景 -->
  <n-layout class="setup-layout">
    <div class="setup-container">
      
      <!-- 2. 加上 dashboard-card 类实现辉光，同时保留 setup-card 用于控制尺寸 -->
      <n-card class="dashboard-card setup-card" :bordered="false" size="large">
        
        <!-- 头部区域：Logo 和 标题 -->
        <div class="setup-header">
          <img src="../assets/logo.png" alt="Logo" class="setup-logo" />
          <h2 class="setup-title">Emby Toolkit</h2>
          <p class="setup-subtitle">初始化配置向导</p>
        </div>

        <div class="intro-text">
          <p>请配置 Emby 服务器连接信息。</p>
          <p class="tip">这是系统运行的基础，请确保信息正确。</p>
        </div>

        <n-form ref="formRef" :model="formModel" :rules="rules" size="large">
          <n-form-item label="服务器地址 (URL)" path="url">
            <n-input 
              v-model:value="formModel.url" 
              placeholder="例如: http://192.168.1.10:8096" 
              @keydown.enter="handleSave"
            />
          </n-form-item>
          
          <n-form-item label="API 密钥 (API Key)" path="api_key">
            <n-input 
              v-model:value="formModel.api_key" 
              type="password" 
              show-password-on="mousedown"
              placeholder="在 Emby 控制台 -> 高级 -> API 密钥 中生成" 
              @keydown.enter="handleSave"
            />
          </n-form-item>

          <n-space vertical style="margin-top: 20px;">
            <n-button type="primary" block @click="handleSave" :loading="loading" size="large" class="setup-btn">
              测试并保存
            </n-button>
          </n-space>
        </n-form>
      </n-card>
    </div>
  </n-layout>
</template>

<script setup>
import { ref } from 'vue';
import { useRouter } from 'vue-router';
import { NLayout, NCard, NForm, NFormItem, NInput, NButton, NSpace, useMessage } from 'naive-ui';
import axios from 'axios';

const router = useRouter();
const message = useMessage();
const loading = ref(false);
const formRef = ref(null);

const formModel = ref({
  url: '',
  api_key: ''
});

const rules = {
  url: { required: true, message: '请输入服务器地址', trigger: 'blur' },
  api_key: { required: true, message: '请输入 API 密钥', trigger: 'blur' }
};

async function handleSave() {
  try {
    await formRef.value?.validate();
    loading.value = true;
    
    const response = await axios.post('/api/auth/setup', formModel.value);
    
    if (response.data.status === 'ok') {
      message.success('配置成功！即将跳转登录页...');
      setTimeout(() => {
        router.push({ name: 'Login' });
      }, 1500);
    }
  } catch (error) {
    const msg = error.response?.data?.message || '连接测试失败，请检查 URL 和密钥';
    message.error(msg);
  } finally {
    loading.value = false;
  }
}
</script>

<style scoped>
/* 布局容器，撑满全屏 */
.setup-layout {
  height: 100vh;
  width: 100vw;
}

.setup-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  padding: 20px;
}

.setup-card {
  width: 100%;
  max-width: 600px; /* 保持较宽的宽度，因为 URL 可能很长 */
  border-radius: 12px;
  
  /* 🔥🔥🔥 关键：强制重置高度，防止 dashboard-card 把它拉成巨人 🔥🔥🔥 */
  height: auto !important;
  min-height: auto !important;
  flex: none !important;
}

/* 头部样式，保持和 Login 一致 */
.setup-header {
  text-align: center;
  margin-bottom: 24px;
}

.setup-logo {
  height: 60px;
  margin-bottom: 10px;
}

.setup-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0 0 8px 0;
}

.setup-subtitle {
  font-size: 14px;
  color: #999;
  margin: 0;
}

.intro-text {
  text-align: center;
  margin-bottom: 30px;
  color: #666;
  font-size: 15px;
}

.tip {
  font-size: 13px;
  color: #999;
  margin-top: 5px;
}

.setup-btn {
  font-weight: bold;
}
</style>