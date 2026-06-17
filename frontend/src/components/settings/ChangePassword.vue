<template>
  <n-card title="修改密码" size="medium">
    <n-form @submit.prevent="handleChangePassword">
      <n-form-item-row label="当前密码">
        <n-input
          type="password"
          show-password-on="mousedown"
          v-model:value="passwords.current_password"
          placeholder="请输入当前使用的密码"
        />
      </n-form-item-row>
      <n-form-item-row label="新密码">
        <n-input
          type="password"
          show-password-on="mousedown"
          v-model:value="passwords.new_password"
          placeholder="请输入新密码 (至少6位)"
        />
      </n-form-item-row>
      <n-form-item-row label="确认新密码">
        <n-input
          type="password"
          show-password-on="mousedown"
          v-model:value="passwords.confirm_password"
          placeholder="请再次输入新密码"
          :status="confirmPasswordStatus"
        />
      </n-form-item-row>
      <n-button type="primary" attr-type="submit" :loading="loading" block>
        确认修改
      </n-button>
    </n-form>
  </n-card>
</template>

<script setup>
import { ref, computed } from 'vue';
import { NCard, NForm, NFormItemRow, NInput, NButton, useMessage } from 'naive-ui';
import axios from 'axios';

const emit = defineEmits(['password-changed']);
const message = useMessage();
const loading = ref(false);
const passwords = ref({
  current_password: '',
  new_password: '',
  confirm_password: '',
});

const confirmPasswordStatus = computed(() => {
  if (passwords.value.confirm_password && passwords.value.new_password !== passwords.value.confirm_password) {
    return 'error';
  }
  return undefined;
});

async function handleChangePassword() {
  if (!passwords.value.current_password || !passwords.value.new_password || !passwords.value.confirm_password) {
    message.error('所有字段均为必填项');
    return;
  }
  if (passwords.value.new_password.length < 6) {
    message.error('新密码长度不能少于6位');
    return;
  }
  if (passwords.value.new_password !== passwords.value.confirm_password) {
    message.error('两次输入的新密码不一致');
    return;
  }

  loading.value = true;
  try {
    const payload = {
      current_password: passwords.value.current_password,
      new_password: passwords.value.new_password,
    };
    await axios.post('/api/auth/change_password', payload);
    message.success('密码修改成功！');
    emit('password-changed');
    // 清空表单
    passwords.value = {
      current_password: '',
      new_password: '',
      confirm_password: '',
    };
  } catch (error) {
    if (error.response && error.response.data.error) {
      message.error(`修改失败: ${error.response.data.error}`);
    } else {
      message.error('修改失败，请检查网络或联系管理员');
    }
  } finally {
    loading.value = false;
  }
}
</script>