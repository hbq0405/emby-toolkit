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
      :pagination="{ pageSize: 10 }" 
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
        
        <!-- ★★★ 新增：生成数量 ★★★ -->
        <n-form-item label="生成数量" path="count">
          <n-input-number
            v-model:value="formModel.count"
            :min="1"
            :max="100"
            style="width: 100%"
            placeholder="一次生成多少个链接"
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

// --- API ---
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
  count: 1, // ★★★ 默认数量为 1
  expiration_days: null,
  link_expires_in_days: 7,
});

const rules = {
  template_id: {
    type: 'number',
    required: true,
    message: '请选择一个用户模板',
    trigger: ['blur', 'change'],
  },
  count: {
    type: 'number',
    required: true,
    min: 1,
    message: '数量至少为 1',
    trigger: ['blur', 'change'],
  }
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
    count: 1, // 重置为 1
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
          // 1. 确保获取到的是数组
          const links = response.invite_links || (response.invite_link ? [response.invite_link] : []);
          
          if (links.length === 0) {
            throw new Error('后端未返回任何链接');
          }

          // 2. 显式生成要显示的文本
          const finalLinksText = links.join('\n');
          const count = links.length;

          // 3. 弹窗显示
          dialog.success({
            title: `成功创建 ${count} 个邀请链接！`,
            content: () => h('div', null, [
              h('p', null, '请复制以下链接：'),
              // 显示链接的文本域
              h(NInput, {
                value: finalLinksText, // 绑定生成的文本
                type: 'textarea',
                autosize: { minRows: 2, maxRows: 10 },
                readonly: true,
              }),
              // 按钮区域
              h('div', { style: 'margin-top: 10px; text-align: right;' }, [
                 h(NButton, {
                    type: 'primary',
                    size: 'small',
                    // ★★★ 核心修复：直接使用闭包中确定的 finalLinksText 变量 ★★★
                    onClick: () => {
                      // 再次确认内容不为空
                      if (finalLinksText) {
                        copyToClipboard(finalLinksText);
                      } else {
                        message.error('没有内容可复制');
                      }
                    }
                  }, { 
                    icon: () => h(NIcon, { component: CopyIcon }),
                    default: () => '一键复制所有' 
                  })
              ])
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

const copyToClipboard = (textToCopy) => {
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(textToCopy)
      .then(() => {
        message.success('已复制到剪贴板！');
      })
      .catch(err => {
        message.error('自动复制失败，请手动复制。');
        console.error('Clipboard API failed: ', err);
      });
  } else {
    const textArea = document.createElement('textarea');
    textArea.value = textToCopy;
    textArea.style.position = 'absolute';
    textArea.style.left = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    try {
      document.execCommand('copy');
      message.success('已复制到剪贴板！');
    } catch (err) {
      message.error('复制失败，请手动复制。');
    } finally {
      document.body.removeChild(textArea);
    }
  }
};

// --- 表格列定义 ---
const createColumns = () => [
  {
    title: '邀请链接',
    key: 'token',
    render: (row) => {
      const fullLink = `${window.location.origin}/register/invite/${row.token}`;
      return h(NSpace, { align: 'center' }, () => [
        h('span', { style: { maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'inline-block' } }, fullLink),
        h(NTooltip, null, {
          trigger: () => h(NButton, {
            size: 'small',
            circle: true,
            onClick: () => copyToClipboard(fullLink)
          }, { icon: () => h(NIcon, { component: CopyIcon }) }),
          default: () => '复制链接'
        })
      ]);
    }
  },
  {
    title: '状态',
    key: 'status',
    width: 80,
    render: (row) => {
      let type = 'default';
      let text = row.status;
      if (row.status === 'active') { type = 'success'; text = '可用'; }
      if (row.status === 'used') { type = 'info'; text = '已用'; }
      if (row.status === 'expired') { type = 'error'; text = '过期'; }
      return h(NTag, { type }, () => text.toUpperCase());
    }
  },
  { title: '关联模板', key: 'template_name', width: 120, ellipsis: true },
  { title: '用户有效期(天)', key: 'expiration_days', width: 120 },
  {
    title: '链接到期时间',
    key: 'expires_at',
    width: 160,
    render: (row) => row.expires_at ? dayjs(row.expires_at).format('YYYY-MM-DD HH:mm') : '-'
  },
  {
    title: '操作',
    key: 'action',
    width: 80,
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