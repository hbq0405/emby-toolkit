<!-- src/components/UserList.vue -->
<template>
  <div>
    <n-data-table
      :columns="columns"
      :data="users"
      :loading="loading"
      :row-key="row => row.Id"
    />

    <n-modal
      v-model:show="isModalVisible"
      preset="card"
      style="width: 500px"
      :title="`设置用户 “${currentUser?.Name}” 的有效期`"
      :bordered="false"
      size="huge"
    >
      <n-form ref="formRef" :model="formModel">
        <n-form-item label="新的到期日期" path="expiration_date">
          <n-date-picker
            v-model:value="formModel.expiration_date_ts"
            type="date"
            clearable
            style="width: 100%"
            placeholder="清空并保存，即可设为永久"
          />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-button @click="isModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleExpirationOk">保存</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted, h } from 'vue';
import {
  NDataTable, NSwitch, NTag, NSpace, NButton, NPopconfirm, NModal,
  NForm, NFormItem, NDatePicker, useMessage
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
};

// --- 状态和Hooks ---
const message = useMessage();
const users = ref([]);
const loading = ref(false);
const isModalVisible = ref(false);
const currentUser = ref(null);
const formRef = ref(null);
const formModel = ref({
  expiration_date_ts: null,
});

// --- 数据获取 ---
const fetchData = async () => {
  loading.value = true;
  try {
    const data = await api.getUsers();
    const formattedData = data.map(u => ({ ...u, IsDisabled: u.Policy?.IsDisabled ?? false }));
    users.value = formattedData;
  } catch (error) {
    message.error('加载用户列表失败');
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
    } else {
      throw new Error('操作失败');
    }
  } catch (error) {
    message.error('更新用户状态失败');
    // 状态切换失败时，把开关拨回去
    fetchData();
  }
};

const handleDelete = async (user) => {
  try {
    const response = await api.deleteUser(user.Id);
    if (response.ok) {
      message.success(`用户 ${user.Name} 已被彻底删除`);
      fetchData();
    } else {
      throw new Error('删除失败');
    }
  } catch (error) {
    message.error('删除用户失败');
  }
};

const showExpirationModal = (user) => {
  currentUser.value = user;
  formModel.value.expiration_date_ts = user.expiration_date ? dayjs(user.expiration_date).valueOf() : null;
  isModalVisible.value = true;
};

const handleExpirationOk = async () => {
  try {
    const expirationDate = formModel.value.expiration_date_ts
      ? new Date(formModel.value.expiration_date_ts).toISOString()
      : null;
    const response = await api.setUserExpiration(currentUser.value.Id, expirationDate);
    if (response.ok) {
      message.success('用户有效期已更新');
      isModalVisible.value = false;
      fetchData();
    } else {
      throw new Error('更新失败');
    }
  } catch (error) {
    message.error('更新有效期失败');
  }
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
      h(NButton, {
        size: 'small',
        onClick: () => showExpirationModal(row)
      }, () => '续期'),
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