<!-- src/components/RegisterPage.vue (已升级) -->
<template>
  <div>
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

    <!-- ★★★ 成功提示模态框 ★★★ -->
    <n-modal v-model:show="showSuccessModal" preset="card" style="width: 90%; max-width: 450px;">
      <n-result
        status="success"
        title="注册成功！"
        :description="`欢迎加入，${registrationResult?.username}！`"
      >
        <template #footer>
          <!-- ★★★ 核心修改：更新这里的描述项 ★★★ -->
          <n-descriptions
            label-placement="left"
            bordered
            :column="1"
            style="margin-bottom: 20px;"
          >
            <n-descriptions-item label="您的账号">
              <!-- 这里我们仍然可以显示用户名，让用户确认 -->
              {{ registrationResult?.username }}
            </n-descriptions-item>
            <n-descriptions-item label="账号类型">
              <!-- 将模板描述作为“账号类型”或“权限说明”来显示 -->
              {{ registrationResult?.template_description }}
            </n-descriptions-item>
            <n-descriptions-item label="账号有效期">
              {{ registrationResult?.expiration_info }}
            </n-descriptions-item>
          </n-descriptions>
          <n-button type="primary" block @click="goToEmby">
            立即前往 Emby 观影
          </n-button>
        </template>
      </n-result>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { 
  NCard, NForm, NFormItem, NInput, NButton, NSpin, NAlert, useMessage,
  NModal, NResult, NDescriptions, NDescriptionsItem
} from 'naive-ui';
import axios from 'axios';

const route = useRoute();
const message = useMessage();

const token = route.params.token;
const cardTitle = ref('创建您的 Emby 账户');
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

// ★★★ 新增模态框状态 ★★★
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
        // ★★★ 核心修改：不再直接跳转，而是处理后端返回的数据 ★★★
        const response = await axios.post('/api/register/invite', formModel.value);
        
        // 1. 将后端返回的成功信息存起来
        registrationResult.value = response.data.data;
        
        // 2. 显示成功模态框
        showSuccessModal.value = true;

      } catch (error) {
        message.error(error.response?.data?.message || '注册失败');
      } finally {
        loading.value = false;
      }
    }
  });
};

// ★★★ 新增跳转函数 ★★★
const goToEmby = () => {
  if (registrationResult.value?.redirect_url) {
    window.location.href = registrationResult.value.redirect_url;
  }
};

onMounted(validateToken);
</script>