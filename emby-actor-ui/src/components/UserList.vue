<!-- src/components/UserList.vue (已升级) -->
<template>
  <div>
    <!-- ★★★ 新增：筛选与批量操作区域 ★★★ -->
    <n-space vertical>
      <!-- 批量操作 -->
      <n-space v-if="checkedRowKeys.length > 0" align="center">
        <span>已选择 {{ checkedRowKeys.length }} 项</span>
        <n-button type="primary" size="small" @click="showBulkChangeTemplateModal">批量切换模板</n-button>
        <n-popconfirm
          @positive-click="handleBulkDelete"
          :positive-button-props="{ type: 'error' }"
        >
          <template #trigger>
            <n-button type="error" size="small" ghost>批量删除</n-button>
          </template>
          确定要彻底删除选中的 {{ checkedRowKeys.length }} 个用户吗？此操作不可恢复！
        </n-popconfirm>
      </n-space>

      <!-- 筛选器 -->
      <n-grid :x-gap="12" :y-gap="8" :cols="4" item-responsive>
        <n-gi>
          <n-input v-model:value="filterName" placeholder="按用户名筛选" clearable />
        </n-gi>
        <n-gi>
          <n-select
            v-model:value="filterStatus"
            placeholder="按状态筛选"
            :options="statusOptions"
            clearable
          />
        </n-gi>
        <n-gi>
          <n-select
            v-model:value="filterTemplateId"
            placeholder="按模板筛选"
            :options="templateOptions"
            clearable
            filterable
          />
        </n-gi>
      </n-grid>
    </n-space>

    <!-- 数据表格 -->
    <n-data-table
      :columns="columns"
      :data="filteredAndSortedUsers"
      :loading="loading"
      :row-key="row => row.Id"
      v-model:checked-row-keys="checkedRowKeys"
      @update:sorter="handleSorterChange"
      @row-click="handleRowClick"
      style="margin-top: 12px;"
    />

    <!-- 续期模态框 (保持不变) -->
    <n-modal v-model:show="isExpirationModalVisible" preset="card" style="width: 500px" :title="`设置用户 “${currentUser?.Name}” 的有效期`" :bordered="false" size="huge">
      <n-form ref="expirationFormRef" :model="expirationFormModel">
        <n-form-item label="新的到期日期" path="expiration_date_ts">
          <n-date-picker v-model:value="expirationFormModel.expiration_date_ts" type="date" clearable style="width: 100%" placeholder="选择一个未来的日期" />
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

    <!-- 切换模板模态框 (单用户) -->
    <n-modal v-model:show="isTemplateModalVisible" preset="card" style="width: 500px" :title="`切换用户 “${currentUser?.Name}” 的模板`" :bordered="false" size="huge">
      <n-form ref="templateFormRef" :model="templateFormModel" :rules="templateFormRules">
        <n-form-item label="选择新模板" path="template_id">
          <n-select v-model:value="templateFormModel.template_id" placeholder="请选择一个新的权限模板" :options="templateOptions" filterable />
        </n-form-item>
      </n-form>
       <template #footer>
        <n-button @click="isTemplateModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleChangeTemplateOk" :loading="isSubmittingTemplate">确认切换</n-button>
      </template>
    </n-modal>

    <!-- ★★★ 新增：批量切换模板模态框 ★★★ -->
    <n-modal v-model:show="isBulkTemplateModalVisible" preset="card" style="width: 500px" :title="`为 ${checkedRowKeys.length} 个用户批量切换模板`" :bordered="false" size="huge">
      <n-form ref="bulkTemplateFormRef" :model="bulkTemplateFormModel" :rules="templateFormRules">
        <n-form-item label="选择新模板" path="template_id">
          <n-select v-model:value="bulkTemplateFormModel.template_id" placeholder="请选择一个新的权限模板" :options="templateOptions" filterable />
        </n-form-item>
      </n-form>
       <template #footer>
        <n-button @click="isBulkTemplateModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleBulkChangeTemplateOk" :loading="isSubmittingTemplate">确认切换</n-button>
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
  setUserStatus: (userId, disable) => fetch(`/api/admin/users/${userId}/status`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ disable }) }),
  setUserExpiration: (userId, expirationDate) => fetch(`/api/admin/users/${userId}/expiration`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ expiration_date: expirationDate }) }),
  deleteUser: (userId) => fetch(`/api/admin/users/${userId}`, { method: 'DELETE' }),
  getUserTemplates: () => fetch('/api/admin/user_templates').then(res => res.json()),
  changeUserTemplate: (userId, templateId) => fetch(`/api/admin/users/${userId}/template`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ template_id: templateId }) }),
  // ★★★ 新增批量API (前端封装) ★★★
  bulkDeleteUsers: (userIds) => Promise.allSettled(userIds.map(id => api.deleteUser(id))),
  bulkChangeUserTemplate: (userIds, templateId) => Promise.allSettled(userIds.map(id => api.changeUserTemplate(id, templateId))),
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
const checkedRowKeys = ref([]);
const filterName = ref('');
const filterStatus = ref(null);
const filterTemplateId = ref(null);
const sorter = ref(null);
const lastCheckedIndex = ref(-1);

const statusOptions = [
  { label: '已启用', value: 'enabled' },
  { label: '已禁用', value: 'disabled' },
];

// ★★★ 新增：批量切换模板模态框状态 ★★★
const isBulkTemplateModalVisible = ref(false);
const bulkTemplateFormRef = ref(null);
const bulkTemplateFormModel = ref({ template_id: null });

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

const filteredAndSortedUsers = computed(() => {
  let data = [...users.value];

  // 1. 筛选
  if (filterName.value) {
    data = data.filter(u => u.Name.toLowerCase().includes(filterName.value.toLowerCase()));
  }
  if (filterStatus.value) {
    const isDisabled = filterStatus.value === 'disabled';
    data = data.filter(u => u.IsDisabled === isDisabled);
  }
  if (filterTemplateId.value) {
    data = data.filter(u => u.template_id === filterTemplateId.value);
  }

  // 2. 排序
  if (sorter.value && sorter.value.order) {
    // ★★★ 核心修正：使用 columnKey 并重命名为 key ★★★
    const { columnKey: key, order } = sorter.value;
    const multiplier = order === 'ascend' ? 1 : -1;

    data.sort((a, b) => {
      const valA = a[key];
      const valB = b[key];

      // --- 开始使用健壮的排序逻辑 ---

      // 规则1: null 或 undefined 的值总是排在最后面
      if (valA === null || typeof valA === 'undefined') return 1;
      if (valB === null || typeof valB === 'undefined') return -1;

      // 规则2: 如果是日期字段，按时间戳比较
      if (key === 'expiration_date' || key === 'LastActivityDate') {
        // 确保即使值不是有效日期也不会导致程序崩溃
        const dateA = new Date(valA).getTime() || 0;
        const dateB = new Date(valB).getTime() || 0;
        return (dateA - dateB) * multiplier;
      }

      // 规则3: 如果是字符串，使用 localeCompare 进行准确比较
      if (typeof valA === 'string') {
        return valA.localeCompare(valB) * multiplier;
      }

      // 规则4: 默认按数字进行比较
      return (valA - valB) * multiplier;
    });
  }
  return data;
});

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

const handleSorterChange = (newSorter) => {
  sorter.value = newSorter;
};

// ★★★ 新增：Shift多选处理 ★★★
const handleRowClick = (row, event) => {
  const currentIndex = filteredAndSortedUsers.value.findIndex(u => u.Id === row.Id);
  if (event.shiftKey && lastCheckedIndex.value !== -1) {
    const start = Math.min(lastCheckedIndex.value, currentIndex);
    const end = Math.max(lastCheckedIndex.value, currentIndex);
    const idsToSelect = filteredAndSortedUsers.value.slice(start, end + 1).map(u => u.Id);
    
    // 合并选择，避免重复
    const currentSelection = new Set(checkedRowKeys.value);
    idsToSelect.forEach(id => currentSelection.add(id));
    checkedRowKeys.value = Array.from(currentSelection);
  }
  lastCheckedIndex.value = currentIndex;
};

// ★★★ 新增：批量操作处理 ★★★
const handleBulkDelete = async () => {
  const userIds = [...checkedRowKeys.value];
  const results = await api.bulkDeleteUsers(userIds);
  const failedCount = results.filter(r => r.status === 'rejected').length;
  if (failedCount > 0) {
    message.error(`${failedCount} 个用户删除失败，其余成功。`);
  } else {
    message.success(`成功删除 ${userIds.length} 个用户。`);
  }
  checkedRowKeys.value = [];
  fetchData();
};

const showBulkChangeTemplateModal = () => {
  bulkTemplateFormModel.value.template_id = null;
  isBulkTemplateModalVisible.value = true;
};

const handleBulkChangeTemplateOk = () => {
  bulkTemplateFormRef.value?.validate(async (errors) => {
    if (!errors) {
      isSubmittingTemplate.value = true;
      const userIds = [...checkedRowKeys.value];
      try {
        const results = await api.bulkChangeUserTemplate(userIds, bulkTemplateFormModel.value.template_id);
        const failedCount = results.filter(r => r.status === 'rejected').length;
        if (failedCount > 0) {
          message.error(`${failedCount} 个用户切换模板失败，其余成功。`);
        } else {
          message.success(`成功为 ${userIds.length} 个用户切换了模板。`);
        }
        isBulkTemplateModalVisible.value = false;
        checkedRowKeys.value = [];
        fetchData();
      } catch (error) {
        message.error('批量切换模板时发生未知错误');
      } finally {
        isSubmittingTemplate.value = false;
      }
    }
  });
};

// --- 表格列定义 ---
const createColumns = () => [
  // ★★★ 新增：复选框列 ★★★
  { type: 'selection' },
  { title: '用户名', key: 'Name', sorter: true },
  { title: '状态', key: 'IsDisabled', render: (row) => h(NSwitch, { value: !row.IsDisabled, onUpdateValue: (value) => handleStatusChange(row, value) }) },
  { title: '所属模板', key: 'template_name', sorter: true, render: (row) => row.template_name || h(NTag, { size: 'small', type: 'warning' }, () => '无') },
  { title: '到期时间', key: 'expiration_date', sorter: true, render: (row) => row.expiration_date ? dayjs(row.expiration_date).format('YYYY-MM-DD') : h(NTag, { size: 'small' }, () => '永久') },
  { title: '最近活动', key: 'LastActivityDate', sorter: true, render: (row) => row.LastActivityDate ? dayjs(row.LastActivityDate).format('YYYY-MM-DD HH:mm') : '无记录' },
  { title: '操作', key: 'actions', render: (row) => h(NSpace, null, () => [ h(NButton, { size: 'small', onClick: () => showExpirationModal(row) }, () => '续期'), h(NButton, { size: 'small', onClick: () => showChangeTemplateModal(row) }, () => '切换模板'), h(NPopconfirm, { onPositiveClick: () => handleDelete(row), negativeText: '取消', positiveText: '确定删除', positiveButtonProps: { type: 'error' } }, { trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, () => '删除'), default: () => `确定要彻底删除用户 ${row.Name} 吗？此操作不可恢复！` }) ]) }
];

const columns = createColumns();
</script>