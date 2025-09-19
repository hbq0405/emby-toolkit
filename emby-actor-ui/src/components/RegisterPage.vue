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
import { useRoute, useRouter } from 'vue-router';
import { NCard, NForm, NFormItem, NInput, NButton, NSpin, NAlert, useMessage } from 'naive-ui';
import axios from 'axios';

const route = useRoute();
const router = useRouter();
const message = useMessage();

const token = route.params.token;
const cardTitle = ref('创建您的 Emby Toolkit 账户');
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
        await axios.post('/api/register/invite', formModel.value);
        message.success('注册成功！现在您可以登录了。');
        router.push({ name: 'Login' }); // 注册成功后跳转到登录页
      } catch (error) {
        message.error(error.response?.data?.message || '注册失败');
      } finally {
        loading.value = false;
      }
    }
  });
};

onMounted(() => {
  validateToken();
});
</script>