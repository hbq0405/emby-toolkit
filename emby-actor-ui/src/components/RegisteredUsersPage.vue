<!-- src/components/RegisteredUsersPage.vue -->
<template>
  <div>
    <n-page-header title="已注册用户管理" subtitle="管理所有 Emby 用户及其状态" />

    <n-card :bordered="false" style="margin-top: 24px;">
      <template #header-extra>
        <n-button @click="fetchUsers" size="small" circle>
          <template #icon><n-icon :component="RefreshIcon" /></template>
        </n-button>
      </template>
      <n-data-table
        :columns="userColumns"
        :data="users"
        :loading="loading"
        :pagination="{ pageSize: 15 }"
      />
    </n-card>

    <!-- 编辑有效期的弹窗 -->
    <n-modal v-model:show="showExpirationModal" preset="card" title="设置用户有效期" style="width: 400px;">
      <p>为用户 <strong>{{ currentUser?.Name }}</strong> 设置新的到期时间。</p>
      <n-date-picker 
        v-model:value="newExpirationTimestamp" 
        type="datetime" 
        clearable 
        style="width: 100%;"
      />
      <template #footer>
        <n-button @click="handleSetExpiration" type="primary">保存</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted, h } from 'vue';
import { 
  NPageHeader, NCard, NButton, NIcon, NDataTable, NTag, NSwitch, NTooltip, 
  NModal, NDatePicker, useMessage, useDialog 
} from 'naive-ui';
import { 
  RefreshOutline as RefreshIcon,
  CalendarOutline as CalendarIcon,
  TrashOutline as DeleteIcon,
} from '@vicons/ionicons5';
import axios from 'axios';
import { format } from 'date-fns';

const message = useMessage();
const dialog = useDialog();

const users = ref([]);
const loading = ref(false);
const showExpirationModal = ref(false);
const currentUser = ref(null);
const newExpirationTimestamp = ref(null);

const fetchUsers = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/admin/users');
    users.value = response.data;
  } catch (error) {
    message.error('获取用户列表失败');
  } finally {
    loading.value = false;
  }
};

const handleStatusChange = async (row, disabled) => {
  try {
    await axios.post(`/api/admin/users/${row.Id}/status`, { disable: disabled });
    message.success(`用户 ${row.Name} 已${disabled ? '禁用' : '启用'}`);
    // 直接在前端更新状态，避免重新请求
    const user = users.value.find(u => u.Id === row.Id);
    if (user) user.IsDisabled = disabled;
  } catch (error) {
    message.error('操作失败');
  }
};

const openExpirationModal = (row) => {
  currentUser.value = row;
  newExpirationTimestamp.value = row.expiration_date ? new Date(row.expiration_date).getTime() : null;
  showExpirationModal.value = true;
};

const handleSetExpiration = async () => {
  try {
    const dateToSend = newExpirationTimestamp.value ? new Date(newExpirationTimestamp.value).toISOString() : null;
    await axios.post(`/api/admin/users/${currentUser.value.Id}/expiration`, {
      expiration_date: dateToSend,
    });
    message.success('有效期已更新');
    showExpirationModal.value = false;
    fetchUsers(); // 刷新列表
  } catch (error) {
    message.error('更新失败');
  }
};

const handleDeleteUser = (row) => {
  dialog.warning({
    title: '确认删除用户',
    content: `这是一个高危操作！确定要从 Emby 和本系统中彻底删除用户【${row.Name}】吗？该用户的所有观看历史将丢失且无法恢复！`,
    positiveText: '我明白，删除',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.delete(`/api/admin/users/${row.Id}`);
        message.success(`用户 ${row.Name} 已被删除`);
        fetchUsers();
      } catch (error) {
        message.error('删除失败');
      }
    },
  });
};

const userColumns = [
  { 
    title: '用户名', 
    key: 'Name',
    render: (row) => h('strong', null, row.Name)
  },
  {
    title: '状态',
    key: 'IsDisabled',
    render(row) {
      return h(NSwitch, {
        value: row.IsDisabled,
        onUpdateValue: (value) => handleStatusChange(row, value),
        checkedValue: true,
        uncheckedValue: false,
      }, { checked: () => '禁用', unchecked: () => '正常' });
    }
  },
  {
    title: '有效期至',
    key: 'expiration_date',
    render: (row) => row.expiration_date ? format(new Date(row.expiration_date), 'yyyy-MM-dd HH:mm') : h(NTag, { size: 'small' }, { default: () => '永久' })
  },
  { title: '上次登录', key: 'LastLoginDate', render: (row) => row.LastLoginDate ? format(new Date(row.LastLoginDate), 'yyyy-MM-dd') : '从未' },
  {
    title: '操作',
    key: 'actions',
    render(row) {
      return h('div', { style: 'display: flex; gap: 8px;' }, [
        h(NTooltip, null, {
          trigger: () => h(NButton, { size: 'small', circle: true, onClick: () => openExpirationModal(row) }, { icon: () => h(NIcon, { component: CalendarIcon }) }),
          default: () => '设置有效期'
        }),
        h(NTooltip, null, {
          trigger: () => h(NButton, { size: 'small', circle: true, type: 'error', ghost: true, onClick: () => handleDeleteUser(row) }, { icon: () => h(NIcon, { component: DeleteIcon }) }),
          default: () => '彻底删除用户'
        }),
      ]);
    }
  }
];

onMounted(fetchUsers);
</script>