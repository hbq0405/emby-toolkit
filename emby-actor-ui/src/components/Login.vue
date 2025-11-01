<template>
  <div class="login-container">
    <n-card title="登录" class="login-card">
      <n-form @submit.prevent="handleLogin">
        <!-- ★ 1. 在这里添加登录方式的单选框 ★ -->
        <n-form-item-row label="登录方式">
          <n-radio-group v-model:value="loginType" name="logintype-group">
            <n-space>
              <n-radio value="emby" label="Emby 登录" />
              <n-radio value="local" label="本地管理员" />
            </n-space>
          </n-radio-group>
        </n-form-item-row>

        <n-form-item-row label="用户名">
          <n-input v-model:value="credentials.username" placeholder="请输入用户名" />
        </n-form-item-row>
        <n-form-item-row label="密码">
          <n-input
            type="password"
            show-password-on="mousedown"
            v-model:value="credentials.password"
            placeholder="请输入密码"
          />
        </n-form-item-row>
        <n-button type="primary" attr-type="submit" block :loading="loading">
          登 录
        </n-button>
      </n-form>
    </n-card>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { useRouter } from 'vue-router';
// ★ 2. 从 naive-ui 导入新增的组件 ★
import { NCard, NForm, NFormItemRow, NInput, NButton, useMessage, NRadioGroup, NRadio, NSpace } from 'naive-ui';
import { useAuthStore } from '../stores/auth';

const router = useRouter();
const credentials = ref({
  username: '',
  password: '',
});
const loading = ref(false);
const message = useMessage();
const authStore = useAuthStore();

// ★ 3. 创建一个 ref 来绑定单选框的值，默认选中 'emby' ★
const loginType = ref('emby');

async function handleLogin() {
  if (!credentials.value.username || !credentials.value.password) {
    message.error('请输入用户名和密码');
    return;
  }
  loading.value = true;
  try {
    // ★ 4. 在调用 store 时，将登录类型也一起传过去 ★
    await authStore.login({
      ...credentials.value, // 包含 username 和 password
      loginType: loginType.value, // 附加上选择的登录类型
    });

    message.success('登录成功！');
    router.push({ name: 'DatabaseStats' }); 

  } catch (error) {
    const errorMessage = error.response?.data?.message || error.message || '登录失败，请检查网络或联系管理员';
    message.error(errorMessage);
  } finally {
    loading.value = false;
  }
}
</script>

<style scoped>
.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
}
.login-card {
  width: 100%;
  max-width: 400px;
}
</style>