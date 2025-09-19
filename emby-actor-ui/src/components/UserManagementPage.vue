<!-- src/components/UserManagementPage.vue -->
<template>
  <div>
    <n-page-header title="会员管理中心" subtitle="管理用户模板、生成邀请链接" />

    <n-grid :x-gap="24" :y-gap="24" :cols="2" style="margin-top: 24px;">
      <!-- 左侧：用户模板管理 -->
      <n-gi>
        <n-card title="用户权限模板" :bordered="false">
          <template #header-extra>
            <n-button @click="showCreateTemplateModal = true" type="primary" size="small">
              <template #icon><n-icon :component="AddIcon" /></template>
              新建模板
            </n-button>
          </template>
          
          <n-data-table
            :columns="templateColumns"
            :data="templates"
            :loading="templateLoading"
            :pagination="false"
            :bordered="false"
          />
        </n-card>
      </n-gi>

      <!-- 右侧：生成邀请链接 -->
      <n-gi>
        <n-card title="生成邀请链接" :bordered="false">
          <n-form ref="inviteFormRef" :model="inviteFormModel" :rules="inviteFormRules">
            <n-form-item path="template_id" label="选择权限模板">
              <n-select
                v-model:value="inviteFormModel.template_id"
                placeholder="请选择一个模板"
                :options="templateOptions"
                :loading="templateLoading"
              />
            </n-form-item>
            <n-form-item path="expiration_days" label="用户有效期 (天)">
              <n-input-number 
                v-model:value="inviteFormModel.expiration_days" 
                placeholder="留空则使用模板默认值"
                clearable 
                style="width: 100%;"
              />
            </n-form-item>
            <n-form-item path="link_expires_in_days" label="邀请链接有效期 (天)">
              <n-input-number 
                v-model:value="inviteFormModel.link_expires_in_days" 
                :min="1"
                style="width: 100%;"
              />
            </n-form-item>
            <n-button @click="handleGenerateInvite" type="primary" block :loading="generatingInvite">
              生成邀请链接
            </n-button>
          </n-form>
          
          <n-alert v-if="generatedLink" title="邀请链接已生成" type="success" style="margin-top: 20px;">
            <n-input :value="generatedLink" readonly>
              <template #suffix>
                <n-button @click="copyLink" text>
                  <template #icon><n-icon :component="CopyIcon" /></template>
                </n-button>
              </template>
            </n-input>
          </n-alert>
        </n-card>
      </n-gi>
    </n-grid>

    <!-- 新建模板的弹窗 -->
    <n-modal v-model:show="showCreateTemplateModal" preset="card" title="新建权限模板" style="width: 600px;">
      <n-form ref="templateFormRef" :model="templateFormModel" :rules="templateFormRules">
        <n-form-item path="name" label="模板名称">
          <n-input v-model:value="templateFormModel.name" placeholder="例如：朋友专用" />
        </n-form-item>
        <n-form-item path="source_emby_user_id" label="从 Emby 用户导入权限">
          <n-select
            v-model:value="templateFormModel.source_emby_user_id"
            placeholder="选择一个已配置好权限的 Emby 用户作为模板"
            :options="embyUsers"
            :loading="embyUserLoading"
            filterable
          />
        </n-form-item>
        <n-form-item path="default_expiration_days" label="默认用户有效期 (天)">
          <n-input-number v-model:value="templateFormModel.default_expiration_days" :min="0" style="width: 100%;" />
          <template #feedback>填 0 表示永久有效</template>
        </n-form-item>
        <n-form-item path="description" label="描述 (可选)">
          <n-input v-model:value="templateFormModel.description" type="textarea" placeholder="简单描述这个模板的用途" />
        </n-form-item>
        <n-form-item>
          <n-button @click="handleCreateTemplate" type="primary" block :loading="creatingTemplate">
            创建模板
          </n-button>
        </n-form-item>
      </n-form>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, h, watch } from 'vue'; // <-- 1. 引入 watch
import { 
  NPageHeader, NGrid, NGi, NCard, NButton, NIcon, NDataTable, NModal, NForm, 
  NFormItem, NInput, NSelect, NInputNumber, NAlert, useMessage 
} from 'naive-ui';
import { 
  AddOutline as AddIcon, 
  CopyOutline as CopyIcon,
  TrashOutline as DeleteIcon,
  PencilOutline as EditIcon,
} from '@vicons/ionicons5';
import axios from 'axios';

const message = useMessage();

// --- 模板管理部分 (无变化) ---
const showCreateTemplateModal = ref(false);
const templateFormRef = ref(null);
const templateFormModel = ref({
  name: '',
  description: '',
  source_emby_user_id: null,
  default_expiration_days: 30,
});
const templateFormRules = {
  name: { required: true, message: '请输入模板名称', trigger: 'blur' },
  source_emby_user_id: { required: true, message: '请选择一个源 Emby 用户', trigger: 'change' },
};
const creatingTemplate = ref(false);
const templates = ref([]);
const templateLoading = ref(false);
const embyUsers = ref([]);
const embyUserLoading = ref(false);

const templateColumns = [
  { title: '模板名称', key: 'name' },
  { title: '默认有效期 (天)', key: 'default_expiration_days' },
  { title: '描述', key: 'description' },
];

const fetchTemplates = async () => {
  templateLoading.value = true;
  try {
    const response = await axios.get('/api/admin/user_templates');
    templates.value = response.data;
  } catch (error) {
    message.error('获取模板列表失败');
  } finally {
    templateLoading.value = false;
  }
};

const fetchEmbyUsers = async () => {
  embyUserLoading.value = true;
  try {
    const response = await axios.get('/api/custom_collections/config/emby_users');
    embyUsers.value = response.data;
  } catch (error) {
    message.error('获取 Emby 用户列表失败');
  } finally {
    embyUserLoading.value = false;
  }
};

const handleCreateTemplate = () => {
  templateFormRef.value?.validate(async (errors) => {
    if (!errors) {
      creatingTemplate.value = true;
      try {
        await axios.post('/api/admin/user_templates', templateFormModel.value);
        message.success('模板创建成功！');
        showCreateTemplateModal.value = false;
        fetchTemplates();
      } catch (error) {
        message.error(error.response?.data?.message || '创建模板失败');
      } finally {
        creatingTemplate.value = false;
      }
    }
  });
};

// --- 邀请链接部分 (有修改) ---
const inviteFormRef = ref(null);
const inviteFormModel = ref({
  template_id: null,
  expiration_days: null,
  link_expires_in_days: 7,
});

// ★★★ 核心修复：修改验证触发时机 ★★★
const inviteFormRules = {
  template_id: { 
    required: true, 
    message: '请选择一个模板', 
    trigger: ['blur', 'change'], // 在失焦和改变时都触发验证
    type: 'number' // 确保验证的是数字类型
  },
};
const generatingInvite = ref(false);
const generatedLink = ref('');

const templateOptions = computed(() => 
  templates.value.map(t => ({ label: t.name, value: t.id }))
);

// ★★★ 新增功能：监听模板选择，自动填充默认有效期 ★★★
watch(() => inviteFormModel.value.template_id, (newTemplateId) => {
  if (newTemplateId) {
    const selectedTemplate = templates.value.find(t => t.id === newTemplateId);
    if (selectedTemplate) {
      // 自动填充，但如果用户已经手动输入了值，就不覆盖
      if (inviteFormModel.value.expiration_days === null) {
        inviteFormModel.value.expiration_days = selectedTemplate.default_expiration_days;
      }
    }
  } else {
    // 如果清空了模板选择，也清空有效期，让 placeholder 重新显示
    inviteFormModel.value.expiration_days = null;
  }
});

const handleGenerateInvite = () => {
  inviteFormRef.value?.validate(async (errors) => {
    if (!errors) {
      generatingInvite.value = true;
      generatedLink.value = '';
      try {
        const response = await axios.post('/api/admin/invitations', inviteFormModel.value);
        generatedLink.value = response.data.invite_link;
        message.success('邀请链接已生成！');
      } catch (error) {
        message.error(error.response?.data?.message || '生成邀请链接失败');
      } finally {
        generatingInvite.value = false;
      }
    }
  });
};

const copyLink = () => {
  navigator.clipboard.writeText(generatedLink.value).then(() => {
    message.success('已复制到剪贴板');
  });
};

// --- 生命周期 (无变化) ---
onMounted(() => {
  fetchTemplates();
  fetchEmbyUsers();
});
</script>