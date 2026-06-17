<template>
  <!-- 使用 n-layout 自动适配主题背景色 -->
  <n-layout class="login-layout">
    <div class="login-container">
      <n-card class="dashboard-card login-card" :bordered="false" size="large">
        
        <div class="login-header">
          <img src="../assets/logo.png" alt="Logo" class="login-logo" />
          <h2 class="login-title">Emby Toolkit</h2>
          <p class="login-subtitle">请使用 Emby 账号登录</p>
        </div>

        <n-form @submit.prevent="handleLogin" size="large">
          <n-form-item-row label="用户名">
            <n-input 
              v-model:value="credentials.username" 
              placeholder="请输入用户名" 
              @keydown.enter="handleLogin"
            >
              <template #prefix>
                <n-icon :component="PersonOutline" />
              </template>
            </n-input>
          </n-form-item-row>
          
          <n-form-item-row label="密码">
            <n-input
              type="password"
              show-password-on="mousedown"
              v-model:value="credentials.password"
              placeholder="请输入密码"
              @keydown.enter="handleLogin"
            >
              <template #prefix>
                <n-icon :component="LockClosedOutline" />
              </template>
            </n-input>
          </n-form-item-row>
          
          <n-button type="primary" attr-type="submit" block :loading="loading" size="large" class="login-btn">
            登 录
          </n-button>
          
          <div class="footer-links">
            <n-button text type="error" size="small" @click="showRecoveryModal = true">
              无法连接服务器 / 重置配置?
            </n-button>
          </div>
        </n-form>
      </n-card>
    </div>

    <!-- 灾难恢复模态框 (保持逻辑不变) -->
    <n-modal v-model:show="showRecoveryModal" preset="dialog" title="重置连接配置">
      <template #header>
        <div>重置连接配置</div>
      </template>
      
      <div v-if="!tokenSent">
        <p>如果 Emby 服务器地址变更或无法连接，您可以使用此功能重置配置。</p>
        <p style="color: #d03050; font-weight: bold; margin-top: 10px;">
          点击确定后，系统将在 Docker 控制台日志中生成一个安全令牌。
        </p>
      </div>
      
      <div v-else>
        <p>安全令牌已发送至控制台日志。</p>
        <n-input v-model:value="recoveryToken" placeholder="请输入 6 位安全令牌" />
      </div>

      <template #action>
        <n-button v-if="!tokenSent" type="error" @click="requestToken" :loading="recoveryLoading">
          生成令牌
        </n-button>
        <n-button v-else type="primary" @click="verifyToken" :loading="recoveryLoading">
          验证并重置
        </n-button>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref } from 'vue';
import { useRouter } from 'vue-router';
import { 
  NLayout, NCard, NForm, NFormItemRow, NInput, NButton, 
  useMessage, NModal, NIcon 
} from 'naive-ui';
import { PersonOutline, LockClosedOutline } from '@vicons/ionicons5';
import { useAuthStore } from '../stores/auth';
import axios from 'axios';

const router = useRouter();
const message = useMessage();
const authStore = useAuthStore();

const credentials = ref({ username: '', password: '' });
const loading = ref(false);

// 恢复模式相关
const showRecoveryModal = ref(false);
const tokenSent = ref(false);
const recoveryToken = ref('');
const recoveryLoading = ref(false);

async function handleLogin() {
  if (!credentials.value.username) {
    message.warning('请输入用户名');
    return;
  }
  loading.value = true;
  try {
    await authStore.login(credentials.value);
    message.success('登录成功');
    router.push({ name: 'UserCenter' });
  } catch (error) {
    if (error.response && error.response.status === 428) {
      message.warning('系统尚未配置，请先完成设置');
      router.push({ name: 'Setup' });
    } else {
      const msg = error.response?.data?.message || '登录失败';
      message.error(msg);
    }
  } finally {
    loading.value = false;
  }
}

async function requestToken() {
  recoveryLoading.value = true;
  try {
    await axios.post('/api/auth/request_recovery');
    tokenSent.value = true;
    message.info('令牌已生成，请查看服务器日志');
  } catch (e) {
    message.error('请求失败');
  } finally {
    recoveryLoading.value = false;
  }
}

async function verifyToken() {
  if (!recoveryToken.value) return;
  recoveryLoading.value = true;
  try {
    await axios.post('/api/auth/verify_recovery', { token: recoveryToken.value });
    message.success('验证成功，正在跳转设置页...');
    showRecoveryModal.value = false;
    router.push({ name: 'Setup' });
  } catch (e) {
    message.error('令牌无效或已过期');
  } finally {
    recoveryLoading.value = false;
  }
}
</script>

<style scoped>
/* 使用 n-layout 撑满全屏，背景色会自动跟随主题 */
.login-layout {
  height: 100vh;
  width: 100vw;
}

.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  padding: 20px;
}

.login-card {
  width: 100%;
  max-width: 420px; /* 经典的宽度 */
  border-radius: 12px; /* 圆角 */
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08); /* 柔和的阴影 */
  height: auto !important;
  min-height: auto !important;
  
  /* 如果 dashboard-card 里有 flex 属性，可能也需要重置 */
  flex: none !important;
}

/* 暗色模式下的阴影调整 */
:deep(.n-card.n-card--bordered) {
  border: 1px solid rgba(255, 255, 255, 0.09);
}

.login-header {
  text-align: center;
  margin-bottom: 30px;
}

.login-logo {
  height: 60px;
  margin-bottom: 10px;
}

.login-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0 0 8px 0;
  /* 字体颜色会自动适配主题 */
}

.login-subtitle {
  font-size: 14px;
  color: #999; /* 副标题保持灰色 */
  margin: 0;
}

.login-btn {
  margin-top: 10px;
  font-weight: bold;
}

.footer-links {
  margin-top: 24px;
  text-align: center;
}
</style>