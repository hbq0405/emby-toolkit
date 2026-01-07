<!-- src/components/UserTemplates.vue -->
<template>
  <div>
    <n-button
      type="primary"
      @click="handleCreate"
      style="margin-bottom: 16px"
    >
      <template #icon><n-icon :component="AddIcon" /></template>
      创建新模板
    </n-button>

    <n-data-table
      :columns="columns"
      :data="templates"
      :loading="loading"
      :row-key="row => row.id"
    />

    <n-modal
      v-model:show="isModalVisible"
      preset="card"
      style="width: 600px"
      :title="modalTitle"
      :bordered="false"
      size="huge"
    >
      <n-form ref="formRef" :model="formModel" :rules="rules" label-placement="left" label-width="auto">
        <n-form-item label="模板名称" path="name">
          <n-input v-model:value="formModel.name" placeholder="例如：标准会员" />
        </n-form-item>
        <n-form-item label="模板描述" path="description">
          <n-input
            v-model:value="formModel.description"
            type="textarea"
            placeholder="简单描述这个模板的权限，方便用户注册成功后大致了解自己账号的权限。"
          />
        </n-form-item>
        <n-form-item label="默认有效期(天)" path="default_expiration_days">
          <n-input-number
            v-model:value="formModel.default_expiration_days"
            :min="0"
            style="width: 100%"
          />
          <template #feedback>
            设置为 0 表示永久有效。
          </template>
        </n-form-item>
        <n-form-item v-if="!isEditMode" label="源 Emby 用户" path="source_emby_user_id">
          <n-select v-model:value="formModel.source_emby_user_id" placeholder="选择一个用户作为权限样板" :options="embyUserOptions" filterable />
          <template #feedback>重要：创建后不可更改。模板权限将完全复制源用户。</template>
        </n-form-item>
        <n-form-item v-if="!isEditMode" label="同步首选项" path="include_configuration">
          <n-switch v-model:value="formModel.include_configuration" />
          <template #feedback>创建后不可更改。将包含源用户的个性化设置。</template>
        </n-form-item>

        <n-form-item label="免审订阅" path="allow_unrestricted_subscriptions">
          <n-switch v-model:value="formModel.allow_unrestricted_subscriptions" />
          <template #feedback>开启后，使用此模板的用户提交订阅请求时，将无需管理员审核，直接提交订阅。</template>
        </n-form-item>
      </n-form>
      <template #footer>
        <n-button @click="isModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleOk" :loading="isSubmitting">{{ isEditMode ? '更新' : '创建' }}</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted, h, computed } from 'vue';
import {
  NButton, NDataTable, NModal, NForm, NFormItem, NSelect, NInputNumber,
  NIcon, NInput, useMessage, NPopconfirm, NSpace
} from 'naive-ui';
import { Add as AddIcon, TrashOutline as DeleteIcon, CreateOutline as EditIcon } from '@vicons/ionicons5';

// --- API ---
const api = {
  getUserTemplates: () => fetch('/api/admin/user_templates'),
  getEmbyUsers: () => fetch('/api/admin/users'),
  createTemplate: (data) => fetch('/api/admin/user_templates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }),
  deleteTemplate: (templateId) => fetch(`/api/admin/user_templates/${templateId}`, { method: 'DELETE' }),
  updateTemplate: (id, data) => fetch(`/api/admin/user_templates/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }),
};

// --- 状态和Hooks ---
const message = useMessage();
const templates = ref([]);
const embyUsers = ref([]);
const loading = ref(false);
const isModalVisible = ref(false);
const isSubmitting = ref(false);
const formRef = ref(null);
const isEditMode = ref(false);
const modalTitle = computed(() => isEditMode.value ? '编辑用户模板' : '创建新的用户模板');
const formModel = ref({
  name: '',
  description: '',
  default_expiration_days: 30,
  source_emby_user_id: null,
  include_configuration: true,
  allow_unrestricted_subscriptions: false,
});

const rules = {
  name: { required: true, message: '请输入模板名称', trigger: 'blur' },
  default_expiration_days: { type: 'number', required: true, message: '请输入默认有效期' },
  source_emby_user_id: { required: true, message: '请选择一个源用户', trigger: 'change' },
};

const embyUserOptions = computed(() =>
  embyUsers.value.map(u => ({ label: u.Name, value: u.Id }))
);

// --- 数据获取 ---
const fetchData = async () => {
  loading.value = true;
  try {
    const [templatesResponse, usersResponse] = await Promise.all([
      api.getUserTemplates(),
      api.getEmbyUsers(),
    ]);

    if (!templatesResponse.ok || !usersResponse.ok) {
      throw new Error('Failed to fetch initial data.');
    }
    
    const [templatesData, usersData] = await Promise.all([
      templatesResponse.json(),
      usersResponse.json(),
    ]);

    templates.value = templatesData;
    embyUsers.value = usersData;

  } catch (error) {
    message.error('加载模板或 Emby 用户列表失败');
    console.error(error);
  } finally {
    loading.value = false;
  }
};

onMounted(fetchData);

// --- 事件处理 ---
const handleCreate = () => {
  formModel.value = {
    name: '',
    description: '',
    default_expiration_days: 30,
    source_emby_user_id: null,
    include_configuration: true,
    allow_unrestricted_subscriptions: false, 
  };
  isEditMode.value = false;
  isModalVisible.value = true;
};

const handleEdit = (template) => {
  isEditMode.value = true;
  formModel.value = { ...template }; 
  isModalVisible.value = true;
};

const handleOk = (e) => {
  e.preventDefault();
  formRef.value?.validate(async (errors) => {
    if (!errors) {
      isSubmitting.value = true;
      try {
        const apiCall = isEditMode.value 
          ? api.updateTemplate(formModel.value.id, formModel.value)
          : api.createTemplate(formModel.value);
        
        const response = await apiCall;
        const data = await response.json();

        if (response.ok) {
          message.success(`模板${isEditMode.value ? '更新' : '创建'}成功！`);
          isModalVisible.value = false;
          fetchData();
        } else {
          throw new Error(data.message || '操作失败');
        }
      } catch (error) {
        message.error(`操作失败: ${error.message}`);
      } finally {
        isSubmitting.value = false;
      }
    }
  });
};

const handleDelete = async (templateId) => {
  try {
    const response = await api.deleteTemplate(templateId);
    const data = await response.json();
    if (response.ok) {
      message.success('模板已删除');
      fetchData();
    } else {
      throw new Error(data.message || '删除失败');
    }
  } catch (error) {
    message.error(`删除失败: ${error.message}`);
  }
};

// --- 表格列定义 ---
const columns = [
  { title: '模板名称', key: 'name' },
  { title: '描述', key: 'description' },
  { 
    title: '免审订阅', 
    key: 'allow_unrestricted_subscriptions',
    render: (row) => row.allow_unrestricted_subscriptions ? '是' : '否'
  },
  { 
    title: '默认有效期(天)', 
    key: 'default_expiration_days',
    render(row) {
        return row.default_expiration_days === 0 ? '永久' : row.default_expiration_days;
    }
  },
  {
    title: '操作',
    key: 'actions',
    render(row) {
      return h(NSpace, null, () => [
        h(NButton, { size: 'small', onClick: () => handleEdit(row) }, { default: () => '编辑', icon: () => h(NIcon, { component: EditIcon }) }),
        
        // 删除按钮 (使用 Popconfirm 包裹)
        h(NPopconfirm, {
            onPositiveClick: () => handleDelete(row.id),
            positiveText: '确认删除',
            negativeText: '取消'
          }, {
            trigger: () => h(NButton, {
              size: 'small',
              type: 'error',
              ghost: true,
            }, { 
              default: () => '删除',
              icon: () => h(NIcon, { component: DeleteIcon })
            }),
            default: () => `确定要删除模板 “${row.name}” 吗？`,
          }
        )
      ]);
    },
  },
];
</script>