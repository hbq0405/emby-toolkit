<template>
  <n-space vertical size="large">
    <n-card title="🏷️ 自动标签管理" subtitle="根据媒体库自动为入库影片追加 Emby 标签">
      <template #header-extra>
        <n-button type="primary" @click="addRule">添加规则</n-button>
      </template>
      
      <n-alert title="操作提示" type="info" style="margin-top: 24px;">
        <li><b>实时打标:</b> 实时入库的媒体项命中配置的媒体库会自动打标。</li>
        <li><b>追加标签:</b> 把配置的标签追加到选定的媒体库内所有媒体项。</li>
        <li><b>移除标签:</b> 把配置的标签从选定的媒体库内所有媒体项移除。</li>
      </n-alert>

      <n-table :bordered="false" :single-line="false">
        <thead>
          <tr>
            <th>目标媒体库</th>
            <th>追加标签 (逗号分隔)</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(rule, index) in rules" :key="index">
            <td style="width: 400px"> 
            <n-select
                v-model:value="rule.library_ids" 
                :options="libraryOptions"
                placeholder="选择一个或多个媒体库"
                filterable
                multiple
                clearable
                collapse-tags
            />
            </td>
            <td>
              <n-dynamic-tags v-model:value="rule.tags" />
            </td>
            <td>
              <n-space>
                <n-button size="small" type="primary" secondary @click="appendNow(rule)">
                    追加标签
                </n-button>
                <n-button size="small" type="error" secondary @click="clearNow(rule)">
                    移除标签
                </n-button>
                <n-button size="small" type="error" ghost @click="removeRule(index)">
                    删除规则
                </n-button>
            </n-space>
            </td>
          </tr>
        </tbody>
      </n-table>

      <template #footer>
        <n-space justify="end">
          <n-button type="primary" @click="saveRules" :loading="saving">保存配置</n-button>
        </n-space>
      </template>
    </n-card>
  </n-space>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import { useMessage, useDialog } from 'naive-ui';

const message = useMessage();
const dialog = useDialog();
const rules = ref([]);
const libraryOptions = ref([]);
const saving = ref(false);

const fetchLibraries = async () => {
  try {
    // 确保路径是 /api/emby_libraries
    const res = await axios.get('/api/emby_libraries');
    // ★★★ 修正这里：用 .value 而不是 .ref ★★★
    libraryOptions.value = res.data.map(l => ({ 
      label: l.Name, 
      value: l.Id 
    }));
    console.log('加载媒体库成功:', libraryOptions.value);
  } catch (e) { 
    console.error('获取媒体库失败:', e);
    message.error('获取媒体库失败，请检查后端 API'); 
  }
};

const fetchRules = async () => {
  const res = await axios.get('/api/auto_tagging/rules');
  rules.value = res.data;
};

const addRule = () => {
  rules.value.push({ library_ids: [], tags: [] });
};

const removeRule = (index) => {
  rules.value.splice(index, 1);
};

const saveRules = async () => {
  saving.value = true;
  try {
    await axios.post('/api/auto_tagging/rules', rules.value);
    message.success('配置已保存');
  } finally { saving.value = false; }
};

const appendNow = (rule) => {
  // 1. 基础校验
  if (!rule.library_ids || !rule.library_ids.length || !rule.tags.length) {
    return message.warning('请先选择媒体库并填写标签');
  }

  // 2. 格式化库名用于显示
  const libNames = libraryOptions.value
    .filter(o => rule.library_ids.includes(o.value))
    .map(o => o.label)
    .join(', ');

  // 3. 弹出确认模态框 (补回这里！)
  dialog.info({
    title: '确认追加标签',
    content: `确定要为媒体库 [${libNames}] 的所有存量影片追加标签 [${rule.tags.join(', ')}] 吗？`,
    positiveText: '开始运行',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.post('/api/auto_tagging/run_now', {
          library_ids: rule.library_ids,
          tags: rule.tags,
          library_name: libNames
        });
        message.success('追加任务已提交到后台');
      } catch (e) {
        message.error('启动任务失败');
      }
    }
  });
};
const clearNow = (rule) => {
  if (!rule.library_ids?.length || !rule.tags?.length) {
    return message.warning('请先选择媒体库并填写要移除的标签');
  }

  const libNames = libraryOptions.value
    .filter(o => rule.library_ids.includes(o.value))
    .map(o => o.label).join(', ');

  dialog.warning({
    title: '确认移除标签',
    content: `确定要从 [${libNames}] 中移除标签 [${rule.tags.join(', ')}] 吗？这不会影响影片的其他标签。`,
    positiveText: '立即移除',
    onPositiveClick: async () => {
      await axios.post('/api/auto_tagging/clear_now', {
        library_ids: rule.library_ids,
        tags: rule.tags,
        library_name: libNames
      });
      message.success('移除任务已提交');
    }
  });
};
onMounted(() => {
  fetchLibraries();
  fetchRules();
});
</script>