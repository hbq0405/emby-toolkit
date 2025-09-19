<!-- src/components/RegisterPage.vue -->
<template>
  <n-card :title="cardTitle" style="width: 90%; max-width: 400px;">
    <div v-if="validationState === 'validating'">
      <n-spin size="large" />
      <p style="text-align: center; margin-top: 10px;">正在验证邀请链接...</p>
    </div>
    <div v-else-if="validationState === 'invalid'">
      <n-alert :title="validationError" type="error" />
    </div>
    <div v-else>
      <n-form ref="registerFormRef" :model="formModel" :rules="formRules">
        <n-form-item path="username" label="用户名">
          <n-input v-model:value="formModel.username" placeholder="请输入您的用户名" />
        </n-form-item>
        <n-form-item path="password" label="密码">
          <n-input v-model:value="formModel.password" type="password" show-password-on="click" placeholder="请输入您的密码" />
        </n-form-item>
        <n-form-item path="confirmPassword" label="确认密码">
          <n-input v-model:value="formModel.confirmPassword" type="password" show-password-on="click" placeholder="请再次输入密码" />
        </n-form-item>
        <n-button @click="handleRegister" type="primary" block :loading="loading">
          立即注册
        </n-button>
      </n-form>
    </div>
  </n-card>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { NCard, NForm, NFormItem, NInput, NButton, NSpin, NAlert, useMessage } from 'naive-ui';
import axios from 'axios';

const route = useRoute();
// ★★★ 不再需要 useRouter，因为我们将直接跳转 ★★★
// const router = useRouter(); 
const message = useMessage();

const token = route.params.token;
const cardTitle = ref('创建您的 Emby 账户');
const validationState = ref('validating'); // validating, valid, invalid
const validationError = ref('');
const loading = ref(false);

const registerFormRef = ref(null);
const formModel = ref({
  username: '',
  password: '',
  confirmPassword: '',
  token: token,
});

const validatePasswordSame = (rule, value) => {
  return value === formModel.value.password;
};

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
        // ★★★ 核心修改点 ★★★
        const response = await axios.post('/api/register/invite', formModel.value);
        const redirectUrl = response.data?.redirect_url;

        message.success('注册成功！即将跳转...');
        
        // 延迟一小段时间，让用户看到成功提示
        setTimeout(() => {
          if (redirectUrl) {
            // 如果后端返回了地址，直接让浏览器跳转过去
            window.location.href = redirectUrl;
          } else {
            // 如果因为某些原因后端没返回地址，提供一个备用方案
            message.warning('未获取到跳转地址，请手动访问您的 Emby。');
          }
        }, 1500);

      } catch (error) {
        message.error(error.response?.data?.message || '注册失败');
        loading.value = false; // 失败时需要手动停止 loading
      } 
      // 成功时不需要停止 loading，因为页面会直接跳转
    }
  });
};

onMounted(() => {
  validateToken();
});
</script>