<!-- src/components/RegisterPage.vue (UI 重构版) -->
<template>
  <!-- 使用 n-layout 自动适配主题背景色，与登录页保持一致 -->
  <n-layout class="register-layout">
    <div class="register-container">
      <n-card class="dashboard-card register-card" :bordered="false" size="large">
        
        <!-- 头部区域 -->
        <div class="register-header">
          <img src="../assets/logo.png" alt="Logo" class="register-logo" />
          <h2 class="register-title">Emby Toolkit</h2>
          <p class="register-subtitle">创建您的专属账户</p>
        </div>

        <!-- 状态 1: 验证中 -->
        <div v-if="validationState === 'validating'" class="status-container">
          <n-spin size="large" />
          <p style="margin-top: 16px; color: #666;">正在验证邀请链接...</p>
        </div>

        <!-- 状态 2: 验证失败 -->
        <div v-else-if="validationState === 'invalid'" class="status-container">
          <n-alert title="链接无效" type="error" :bordered="false">
            {{ validationError }}
          </n-alert>
          <n-button style="margin-top: 20px;" block @click="goToLogin">
            返回登录
          </n-button>
        </div>

        <!-- 状态 3: 注册表单 -->
        <div v-else>
          <n-form ref="registerFormRef" :model="formModel" :rules="formRules" size="large">
            <n-form-item path="username" label="用户名">
              <n-input v-model:value="formModel.username" placeholder="请输入您的用户名" />
            </n-form-item>
            <n-form-item path="password" label="密码">
              <n-input 
                v-model:value="formModel.password" 
                type="password" 
                show-password-on="mousedown" 
                placeholder="请输入您的密码" 
              />
            </n-form-item>
            <n-form-item path="confirmPassword" label="确认密码">
              <n-input 
                v-model:value="formModel.confirmPassword" 
                type="password" 
                show-password-on="mousedown" 
                placeholder="请再次输入密码" 
                @keydown.enter="handleRegister"
              />
            </n-form-item>
            
            <n-button 
              @click="handleRegister" 
              type="primary" 
              block 
              :loading="loading" 
              size="large" 
              class="register-btn"
            >
              立即注册
            </n-button>
          </n-form>
        </div>
      </n-card>
    </div>

    <!-- ★★★ 注册成功模态框 ★★★ -->
    <n-modal 
      v-model:show="showSuccessModal" 
      preset="card" 
      style="width: 90%; max-width: 480px;"
      :mask-closable="false"
      :close-on-esc="false"
    >
      <n-result
        status="success"
        title="注册成功！"
        :description="`欢迎加入，${registrationResult?.username}！`"
      >
        <template #footer>
          <n-descriptions
            label-placement="left"
            bordered
            :column="1"
            style="margin-bottom: 24px;"
          >
            <n-descriptions-item label="您的账号">
              {{ registrationResult?.username }}
            </n-descriptions-item>
            <n-descriptions-item label="账号类型">
              {{ registrationResult?.template_description }}
            </n-descriptions-item>
            <n-descriptions-item label="账号有效期">
              {{ registrationResult?.expiration_info }}
            </n-descriptions-item>
          </n-descriptions>
          
          <!-- ★★★ 双按钮布局 ★★★ -->
          <n-space vertical>
            <n-button type="primary" block size="large" @click="goToEmby">
              前往 Emby 观影
            </n-button>
            <n-button block size="large" @click="goToLogin">
              登录用户中心
            </n-button>
          </n-space>
        </template>
      </n-result>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { 
  NLayout, NCard, NForm, NFormItem, NInput, NButton, NSpin, NAlert, useMessage,
  NModal, NResult, NDescriptions, NDescriptionsItem, NSpace
} from 'naive-ui';
import axios from 'axios';

const route = useRoute();
const router = useRouter();
const message = useMessage();

const token = route.params.token;
const validationState = ref('validating');
const validationError = ref('');
const loading = ref(false);

const registerFormRef = ref(null);
const formModel = ref({
  username: '',
  password: '',
  confirmPassword: '',
  token: token,
});

const showSuccessModal = ref(false);
const registrationResult = ref(null);

const validatePasswordSame = (rule, value) => value === formModel.value.password;

const formRules = {
  username: { required: true, message: '请输入用户名', trigger: 'blur' },
  password: { required: true, message: '请输入密码', trigger: 'blur' },
  confirmPassword: [
    { required: true, message: '请再次输入密码', trigger: 'blur' },
    { validator: validatePasswordSame, message: '两次输入的密码不一致', trigger: 'blur' }
  ],
};

const validateToken = async () => {
  try {
    await axios.get(`/api/register/invite/validate/${token}`);
    validationState.value = 'valid';
  } catch (error) {
    validationError.value = error.response?.data?.reason || '邀请链接验证失败';
    validationState.value = 'invalid';
  }
};

const handleRegister = () => {
  registerFormRef.value?.validate(async (errors) => {
    if (!errors) {
      loading.value = true;
      try {
        const response = await axios.post('/api/register/invite', formModel.value);
        registrationResult.value = response.data.data;
        showSuccessModal.value = true;
      } catch (error) {
        message.error(error.response?.data?.message || '注册失败');
      } finally {
        loading.value = false;
      }
    }
  });
};

// 跳转到 Emby
const goToEmby = () => {
  if (registrationResult.value?.redirect_url) {
    window.location.href = registrationResult.value.redirect_url;
  }
};

// 跳转到本系统登录页
const goToLogin = () => {
  router.push({ name: 'Login' });
};

onMounted(validateToken);
</script>

<style scoped>
/* 复用 Login 页面的布局样式 */
.register-layout {
  height: 100vh;
  width: 100vw;
}

.register-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  padding: 20px;
}

.register-card {
  width: 100%;
  max-width: 420px;
  border-radius: 12px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
  height: auto !important;
  min-height: auto !important;
  flex: none !important;
}

/* 暗色模式适配 */
:deep(.n-card.n-card--bordered) {
  border: 1px solid rgba(255, 255, 255, 0.09);
}

.register-header {
  text-align: center;
  margin-bottom: 30px;
}

.register-logo {
  height: 60px;
  margin-bottom: 10px;
}

.register-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0 0 8px 0;
}

.register-subtitle {
  font-size: 14px;
  color: #999;
  margin: 0;
}

.register-btn {
  margin-top: 10px;
  font-weight: bold;
}

.status-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 20px 0;
}
</style>