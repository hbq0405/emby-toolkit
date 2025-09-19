<!-- src/components/InvitationList.vue -->
<template>
  <div>
    <n-button
      type="primary"
      @click="handleCreate"
      style="margin-bottom: 16px"
    >
      <template #icon><n-icon :component="AddIcon" /></template>
      创建邀请
    </n-button>

    <n-data-table
      :columns="columns"
      :data="invitations"
      :loading="loading"
      :row-key="row => row.id"
    />

    <n-modal
      v-model:show="isModalVisible"
      preset="card"
      style="width: 500px"
      title="创建新的邀请链接"
      :bordered="false"
      size="huge"
    >
      <n-form ref="formRef" :model="formModel" :rules="rules" label-placement="left" label-width="auto">
        <n-form-item label="用户模板" path="template_id">
          <n-select
            v-model:value="formModel.template_id"
            placeholder="选择一个模板"
            :options="templateOptions"
          />
        </n-form-item>
        <n-form-item label="用户有效期(天)" path="expiration_days">
          <n-input-number
            v-model:value="formModel.expiration_days"
            placeholder="留空则使用模板默认值"
            :min="1"
            clearable
            style="width: 100%"
          />
        </n-form-item>
        <n-form-item label="链接有效期(天)" path="link_expires_in_days">
          <n-input-number
            v-model:value="formModel.link_expires_in_days"
            :min="1"
            style="width: 100%"
          />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-button @click="isModalVisible = false">取消</n-button>
        <n-button type="primary" @click="handleOk">创建</n-button>
      </template>
    </n-modal>
  </div>
</template>

<script setup>
import { ref, onMounted, h, computed } from 'vue';
import {
  NButton, NDataTable, NModal, NForm, NFormItem, NSelect, NInputNumber,
  NIcon, NTag, NSpace, NTooltip, NPopconfirm, NInput, useMessage, useDialog
} from 'naive-ui';
import {
  Add as AddIcon, TrashOutline as DeleteIcon, CopyOutline as CopyIcon
} from '@vicons/ionicons5';
import dayjs from 'dayjs';

// --- API (可以移到单独的文件) ---
const api = {
  getInvitations: () => fetch('/api/admin/invitations').then(res => res.json()),
  deleteInvitation: (id) => fetch(`/api/admin/invitations/${id}`, { method: 'DELETE' }),
  getUserTemplates: () => fetch('/api/admin/user_templates').then(res => res.json()),
  createInvitation: (data) => fetch('/api/admin/invitations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }).then(res => res.json()),
};

// --- 状态和Hooks ---
const message = useMessage();
const dialog = useDialog();
const invitations = ref([]);
const templates = ref([]);
const loading = ref(false);
const isModalVisible = ref(false);
const formRef = ref(null);
const formModel = ref({
  template_id: null,
  expiration_days: null,
  link_expires_in_days: 7,
});

// ★★★★★★★★★★★★★★★★★★★
// ★★★   核心修复点   ★★★
// ★★★★★★★★★★★★★★★★★★★
const rules = {
  template_id: {
    type: 'number', // 明确类型为 number，这样 null 就无法通过验证
    required: true,
    message: '请选择一个用户模板',
    trigger: ['blur', 'change'],
  },
};

const templateOptions = computed(() =>
  templates.value.map(t => ({ label: t.name, value: t.id }))
);

// --- 数据获取 ---
const fetchData = async () => {
  loading.value = true;
  try {
    const [invitationsData, templatesData] = await Promise.all([
      api.getInvitations(),
      api.getUserTemplates(),
    ]);
    invitations.value = invitationsData;
    templates.value = templatesData;
  } catch (error) {
    message.error('加载邀请链接或模板失败');
  } finally {
    loading.value = false;
  }
};

onMounted(fetchData);

// --- 事件处理 ---
const handleDelete = async (id) => {
  try {
    const response = await api.deleteInvitation(id);
    if (response.ok) {
      message.success('邀请链接已删除');
      fetchData();
    } else {
      throw new Error('删除失败');
    }
  } catch (error) {
    message.error('删除邀请链接失败');
  }
};

const handleCreate = () => {
  formModel.value = {
    template_id: null,
    expiration_days: null,
    link_expires_in_days: 7,
  };
  isModalVisible.value = true;
};

const handleOk = (e) => {
  e.preventDefault();
  formRef.value?.validate(async (errors) => {
    if (!errors) {
      try {
        const response = await api.createInvitation(formModel.value);
        if (response.status === 'ok') {
          const newLink = response.invite_link;
          dialog.success({
            title: '创建成功！',
            content: () => h('div', null, [
              h('p', null, '新的邀请链接已生成：'),
              h(NInput, {
                value: newLink,
                readonly: true,
                suffix: () => h(NTooltip, null, {
                  trigger: () => h(NButton, {
                    text: true,
                    onClick: () => {
                      navigator.clipboard.writeText(newLink);
                      message.success('已复制到剪贴板');
                    }
                  }, { icon: () => h(NIcon, { component: CopyIcon }) }),
                  default: () => '复制链接'
                })
              })
            ]),
          });
          isModalVisible.value = false;
          fetchData();
        } else {
          throw new Error(response.message || '创建失败');
        }
      } catch (error) {
        message.error(`创建失败: ${error.message}`);
      }
    }
  });
};

// --- 表格列定义 ---
const createColumns = () => [
  {
    title: '邀请链接',
    key: 'token',
    render: (row) => {
      const fullLink = `${window.location.origin}/register/invite/${row.token}`;
      return h(NSpace, { align: 'center' }, () => [
        h('span', { style: { maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } }, fullLink),
        h(NTooltip, null, {
          trigger: () => h(NButton, {
            size: 'small',
            circle: true,
            onClick: () => {
              navigator.clipboard.writeText(fullLink);
              message.success('已复制到剪贴板');
            }
          }, { icon: () => h(NIcon, { component: CopyIcon }) }),
          default: () => '复制链接'
        })
      ]);
    }
  },
  {
    title: '状态',
    key: 'status',
    render: (row) => {
      let type = 'default';
      let text = row.status;
      if (row.status === 'active') { type = 'success'; text = '可用'; }
      if (row.status === 'used') { type = 'info'; text = '已用'; }
      if (row.status === 'expired') { type = 'error'; text = '过期'; }
      return h(NTag, { type }, () => text.toUpperCase());
    }
  },
  { title: '关联模板', key: 'template_name' },
  { title: '用户有效期(天)', key: 'expiration_days' },
  {
    title: '链接到期时间',
    key: 'expires_at',
    render: (row) => row.expires_at ? dayjs(row.expires_at).format('YYYY-MM-DD HH:mm') : '-'
  },
  {
    title: '操作',
    key: 'action',
    render: (row) => {
      if (row.status === 'active') {
        return h(NPopconfirm, {
          onPositiveClick: () => handleDelete(row.id),
        }, {
          trigger: () => h(NButton, { type: 'error', size: 'small', ghost: true }, { default: () => '删除' }),
          default: () => '确定要删除这个邀请链接吗？'
        });
      }
      return null;
    }
  }
];

const columns = createColumns();
</script>