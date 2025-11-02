<!-- src/components/UserTemplates.vue (已增加同步功能) -->
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
      title="创建新的用户模板"
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
        <!-- ★★★ 1. 在编辑模式下，禁用源用户和首选项的修改 ★★★ -->
        <n-form-item v-if="!isEditMode" label="源 Emby 用户" path="source_emby_user_id">
          <n-select v-model:value="formModel.source_emby_user_id" placeholder="选择一个用户作为权限样板" :options="embyUserOptions" filterable />
          <template #feedback>重要：创建后不可更改。模板权限将完全复制源用户。</template>
        </n-form-item>
        <n-form-item v-if="!isEditMode" label="同步首选项" path="include_configuration">
          <n-switch v-model:value="formModel.include_configuration" />
          <template #feedback>创建后不可更改。将包含源用户的个性化设置。</template>
        </n-form-item>

        <!-- ★★★ 2. “免审订阅”开关 ★★★ -->
        <n-form-item label="免审订阅" path="allow_unrestricted_subscriptions">
          <n-switch v-model:value="formModel.allow_unrestricted_subscriptions" />
          <template #feedback>开启后，使用此模板的用户提交订阅请求时，将无需管理员审核，直接提交给下载器。</template>
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
import { Add as AddIcon, TrashOutline as DeleteIcon, SyncOutline as SyncIcon, CreateOutline as EditIcon } from '@vicons/ionicons5';

// --- API ---
const api = {
  getUserTemplates: () => fetch('/api/admin/user_templates'), // <-- 移除 .then(...)
  getEmbyUsers: () => fetch('/api/admin/users'), // <-- 移除 .then(...)
  createTemplate: (data) => fetch('/api/admin/user_templates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }), // <-- 移除 .then(...)
  deleteTemplate: (templateId) => fetch(`/api/admin/user_templates/${templateId}`, { method: 'DELETE' }),
  syncTemplate: (templateId) => fetch(`/api/admin/user_templates/${templateId}/sync`, {
    method: 'POST',
  }),
  updateTemplate: (id, data) => fetch(`/api/admin/user_templates/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }), // <-- 移除 .then(...)
};

// --- 状态和Hooks ---
const message = useMessage();
const templates = ref([]);
const embyUsers = ref([]);
const loading = ref(false);
const isModalVisible = ref(false);
const isSubmitting = ref(false);
const syncingTemplateId = ref(null);
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
    // ★★★ 核心修正：分两步处理，先 fetch，再解析 json ★★★

    // 步骤 1: 并发发起所有网络请求
    const [templatesResponse, usersResponse] = await Promise.all([
      api.getUserTemplates(),
      api.getEmbyUsers(),
    ]);

    // 步骤 2: 检查每个响应是否成功，然后并发解析它们的 JSON 内容
    if (!templatesResponse.ok || !usersResponse.ok) {
      // 如果任一请求失败，则抛出错误
      throw new Error('Failed to fetch initial data.');
    }
    
    const [templatesData, usersData] = await Promise.all([
      templatesResponse.json(),
      usersResponse.json(),
    ]);

    // 步骤 3: 将解析后的正确数据赋值给 ref
    templates.value = templatesData;
    embyUsers.value = usersData;

  } catch (error) {
    message.error('加载模板或 Emby 用户列表失败');
    console.error(error); // 在控制台打印详细错误，方便调试
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
  isEditMode.value = false; // 确保切换到创建模式
  isModalVisible.value = true;
};

const handleEdit = (template) => {
  isEditMode.value = true;
  
  // ★ 核心修正：使用对象扩展运算符 (...) 完整地复制整个 template 对象 ★
  // 这样，无论 template 里有多少字段，都会被完整地填充到表单模型中
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

// ★★★ 3. 新增 handleSyncTemplate 事件处理函数 ★★★
const handleSyncTemplate = async (template) => {
  syncingTemplateId.value = template.id; // 设置当前正在同步的模板ID，用于显示加载状态
  try {
    const response = await api.syncTemplate(template.id);
    const data = await response.json();
    if (response.ok) {
      message.success(`模板 “${template.name}” 已成功同步最新权限！`);
      // 同步成功后，可以重新获取一次数据以防万一有其他信息变更
      fetchData();
    } else {
      throw new Error(data.message || '同步失败');
    }
  } catch (error) {
    message.error(`同步失败: ${error.message}`);
  } finally {
    syncingTemplateId.value = null; // 清除加载状态
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
      // ★★★ 4. 在操作列中增加“同步权限”按钮 ★★★
      return h(NSpace, null, () => [
        h(NButton, { size: 'small', onClick: () => handleEdit(row) }, { default: () => '编辑', icon: () => h(NIcon, { component: EditIcon }) }),
        // 同步按钮
        h(NButton, {
          size: 'small',
          type: 'info',
          ghost: true,
          // 只有记录了源用户的模板才能同步
          disabled: !row.source_emby_user_id,
          // 显示加载状态
          loading: syncingTemplateId.value === row.id,
          onClick: () => handleSyncTemplate(row),
          title: row.source_emby_user_id ? '从源用户更新权限' : '旧版模板，无法同步'
        }, { 
          default: () => '同步权限',
          icon: () => h(NIcon, { component: SyncIcon })
        }),
        
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