<!-- src/components/UserList.vue (已升级) -->
<template>
  <div>
    <n-data-table
      :columns="columns"
      :data="users"
      :loading="loading"
      :row-key="row => row.Id"
    />

    <!-- 续期模态框 (保持不变) -->
    <n-modal
      v-model:show="isExpirationModalVisible"
      preset="card"
      style="width: 500px"
      :title="`设置用户 “${currentUser?.Name}” 的有效期`"
      :bordered="false"
      size="huge"
    >
      <n-form ref="expirationFormRef" :model="expirationFormModel">
        <n-form-item label="新的到期日期" path="expiration_date_ts">
          <n-date-picker
            v-model:value="expirationFormModel.expiration_date_ts"
            type="date"
            clearable
            style="width: 100%"
            placeholder="选择一个未来的日期"
          />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="isExpirationModalVisible = false">取消</n-button>
          <n-button type="warning" ghost @click="handleSetPermanent">设为永久</n-button>
          <n-button type="primary" @click="handleExpirationOk">保存日期</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- ★★★ 新增：切换模板模态框 ★★★ -->
    <n-modal
      v-model:show="isTemplateModalVisible"
      preset="card"
      style="width: 500px"
      :title="`切换用户 “${currentUser?.Name}” 的模板`"
      :bordered="false"
      size="huge"
    >
      <n-form ref="templateFormRef" :model="templateFormModel" :rules="templateFormRules">
        <n-form-item label="选择新模板" path="template_id">
          <n-select
            v-model:value="templateFormModel.template_id"
            placeholder="请选择一个新的权限模板"
            :options="templateOptions"
            filterable
          />
        </n-form-item>
      </n-form>
       <template #footer>
        <n-button @click="isTemplateModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleChangeTemplateOk" :loading="isSubmittingTemplate">确认切换</n-button>
      </template>
    </n-modal>

  </div>
</template>

<script setup>
import { ref, onMounted, h, computed } from 'vue';
import {
  NDataTable, NSwitch, NTag, NSpace, NButton, NPopconfirm, NModal,
  NForm, NFormItem, NDatePicker, useMessage, NSelect
} from 'naive-ui';
import dayjs from 'dayjs';

// --- API ---
const api = {
  getUsers: () => fetch('/api/admin/users').then(res => res.json()),
  setUserStatus: (userId, disable) => fetch(`/api/admin/users/${userId}/status`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ disable }),
  }),
  setUserExpiration: (userId, expirationDate) => fetch(`/api/admin/users/${userId}/expiration`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ expiration_date: expirationDate }),
  }),
  deleteUser: (userId) => fetch(`/api/admin/users/${userId}`, { method: 'DELETE' }),
  // ★★★ 新增 API 调用 ★★★
  getUserTemplates: () => fetch('/api/admin/user_templates').then(res => res.json()),
  changeUserTemplate: (userId, templateId) => fetch(`/api/admin/users/${userId}/template`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ template_id: templateId }),
  }),
};

// --- 状态和Hooks ---
const message = useMessage();
const users = ref([]);
const loading = ref(false);
const currentUser = ref(null);

// 续期模态框状态
const isExpirationModalVisible = ref(false);
const expirationFormRef = ref(null);
const expirationFormModel = ref({ expiration_date_ts: null });

// ★★★ 新增：切换模板模态框状态 ★★★
const isTemplateModalVisible = ref(false);
const templateFormRef = ref(null);
const templateFormModel = ref({ template_id: null });
const allTemplates = ref([]);
const isSubmittingTemplate = ref(false);
const templateFormRules = {
  template_id: { type: 'number', required: true, message: '请选择一个模板', trigger: ['blur', 'change'] }
};
const templateOptions = computed(() => 
  allTemplates.value.map(t => ({ label: t.name, value: t.id }))
);


// --- 数据获取 ---
const fetchData = async () => {
  loading.value = true;
  try {
    // ★★★ 同时获取用户和模板列表 ★★★
    const [usersData, templatesData] = await Promise.all([
      api.getUsers(),
      api.getUserTemplates(),
    ]);
    const formattedData = usersData.map(u => ({ ...u, IsDisabled: u.Policy?.IsDisabled ?? false }));
    users.value = formattedData;
    allTemplates.value = templatesData;
  } catch (error) {
    message.error('加载用户或模板列表失败');
  } finally {
    loading.value = false;
  }
};

onMounted(fetchData);

// --- 事件处理 ---
const handleStatusChange = async (user, value) => {
  const disable = !value;
  try {
    const response = await api.setUserStatus(user.Id, disable);
    if (response.ok) {
      message.success(`用户 ${user.Name} 已${disable ? '禁用' : '启用'}`);
      fetchData();
    } else { throw new Error('操作失败'); }
  } catch (error) {
    message.error('更新用户状态失败');
    fetchData();
  }
};

const handleDelete = async (user) => {
  try {
    const response = await api.deleteUser(user.Id);
    if (response.ok) {
      message.success(`用户 ${user.Name} 已被彻底删除`);
      fetchData();
    } else { throw new Error('删除失败'); }
  } catch (error) {
    message.error('删除用户失败');
  }
};

// 续期模态框处理
const showExpirationModal = (user) => {
  currentUser.value = user;
  expirationFormModel.value.expiration_date_ts = user.expiration_date ? dayjs(user.expiration_date).valueOf() : null;
  isExpirationModalVisible.value = true;
};

const handleExpirationOk = async () => {
  try {
    const expirationDate = expirationFormModel.value.expiration_date_ts
      ? new Date(expirationFormModel.value.expiration_date_ts).toISOString()
      : null;
    const response = await api.setUserExpiration(currentUser.value.Id, expirationDate);
    if (response.ok) {
      message.success('用户有效期已更新');
      isExpirationModalVisible.value = false;
      fetchData();
    } else { throw new Error('更新失败'); }
  } catch (error) {
    message.error('更新有效期失败');
  }
};

// ★★★ 新增“设为永久”的处理函数 ★★★
const handleSetPermanent = async () => {
  try {
    // 直接发送 null 到后端
    const response = await api.setUserExpiration(currentUser.value.Id, null);
    if (response.ok) {
      message.success('用户已成功设为永久有效！');
      isExpirationModalVisible.value = false;
      fetchData();
    } else {
      throw new Error('设置失败');
    }
  } catch (error) {
    message.error('设置为永久有效失败');
  }
};

// ★★★ 新增：切换模板模态框处理 ★★★
const showChangeTemplateModal = (user) => {
  currentUser.value = user;
  templateFormModel.value.template_id = user.template_id || null;
  isTemplateModalVisible.value = true;
};

const handleChangeTemplateOk = async () => {
  templateFormRef.value?.validate(async (errors) => {
    if (!errors) {
      isSubmittingTemplate.value = true;
      try {
        const response = await api.changeUserTemplate(currentUser.value.Id, templateFormModel.value.template_id);
        const data = await response.json();
        if (response.ok) {
          message.success('用户模板已成功切换！');
          isTemplateModalVisible.value = false;
          fetchData();
        } else {
          throw new Error(data.message || '切换失败');
        }
      } catch (error) {
        message.error(`切换模板失败: ${error.message}`);
      } finally {
        isSubmittingTemplate.value = false;
      }
    }
  });
};


// --- 表格列定义 ---
const createColumns = () => [
  { title: '用户名', key: 'Name' },
  {
    title: '状态',
    key: 'IsDisabled',
    render: (row) => h(NSwitch, {
      value: !row.IsDisabled,
      onUpdateValue: (value) => handleStatusChange(row, value),
    })
  },
  // ★★★ 新增“所属模板”列 ★★★
  {
    title: '所属模板',
    key: 'template_name',
    render: (row) => row.template_name || h(NTag, { size: 'small', type: 'warning' }, () => '无')
  },
  {
    title: '到期时间',
    key: 'expiration_date',
    render: (row) => row.expiration_date
      ? dayjs(row.expiration_date).format('YYYY-MM-DD')
      : h(NTag, { size: 'small' }, () => '永久')
  },
  {
    title: '最近活动',
    key: 'LastActivityDate',
    render: (row) => row.LastActivityDate ? dayjs(row.LastActivityDate).format('YYYY-MM-DD HH:mm') : '无记录'
  },
  {
    title: '操作',
    key: 'actions',
    render: (row) => h(NSpace, null, () => [
      h(NButton, { size: 'small', onClick: () => showExpirationModal(row) }, () => '续期'),
      // ★★★ 新增“切换模板”按钮 ★★★
      h(NButton, { size: 'small', onClick: () => showChangeTemplateModal(row) }, () => '切换模板'),
      h(NPopconfirm, {
        onPositiveClick: () => handleDelete(row),
        negativeText: '取消',
        positiveText: '确定删除',
        positiveButtonProps: { type: 'error' }
      }, {
        trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, () => '删除'),
        default: () => `确定要彻底删除用户 ${row.Name} 吗？此操作不可恢复！`
      })
    ])
  }
];

const columns = createColumns();
</script>