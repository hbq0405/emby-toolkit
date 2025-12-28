<!-- src/components/modals/DefaultConfigModal.vue -->
<template>
  <n-modal
    :show="show"
    preset="card"
    title="默认订阅配置"
    style="width: 600px;"
    @update:show="$emit('update:show', $event)"
  >
    <n-spin :show="loading">
      <n-text depth="3" style="margin-bottom: 20px; display: block;">
        这里的配置将作为所有新添加演员订阅的默认设置，你仍然可以在订阅后对单个演员进行独立修改。
      </n-text>
      
      <!-- 复用现有的表单组件 -->
      <subscription-config-form v-if="!loading" v-model="config" />
    </n-spin>

    <!-- 修复：将 footer 移到 n-spin 组件外部，作为 n-modal 的直接子节点 -->
    <template #footer>
      <n-space justify="end">
        <n-button @click="$emit('update:show', false)">取消</n-button>
        <n-button type="primary" :loading="saving" @click="handleSave">保存</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, watch } from 'vue';
import { NModal, NSpin, NText, NButton, NSpace, useMessage } from 'naive-ui';
import axios from 'axios';
// 假设 SubscriptionConfigForm.vue 在同一目录下或已正确配置路径
import SubscriptionConfigForm from './SubscriptionConfigForm.vue';

const props = defineProps({
  show: Boolean,
});
const emit = defineEmits(['update:show']);

const message = useMessage();
const loading = ref(false);
const saving = ref(false);
const config = ref({});

// 获取默认配置
const fetchDefaultConfig = async () => {
  loading.value = true;
  try {
    // 假设这是获取默认配置的 API
    const response = await axios.get('/api/actor-subscriptions/default-config');
    config.value = response.data;
  } catch (error) {
    console.error("获取默认订阅配置失败:", error);
    message.error('获取默认配置失败，请稍后重试。');
  } finally {
    loading.value = false;
  }
};

// 保存默认配置
const handleSave = async () => {
  saving.value = true;
  try {
    // 假设这是保存默认配置的 API
    await axios.post('/api/actor-subscriptions/default-config', config.value);
    message.success('默认配置已保存！');
    await fetchDefaultConfig();
    emit('update:show', false);
  } catch (error) {
    console.error("保存默认订阅配置失败:", error);
    const errorMsg = error.response?.data?.message || '保存失败，请检查后台日志。';
    message.error(errorMsg);
  } finally {
    saving.value = false;
  }
};

// 当模态框显示时，加载数据
watch(() => props.show, (newVal) => {
  if (newVal) {
    fetchDefaultConfig();
  }
});
</script>
