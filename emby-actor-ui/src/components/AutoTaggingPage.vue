<template>
  <n-space vertical size="large">
    <n-card title="🏷️ 自动标签管理" subtitle="根据媒体库和分级自动为入库影片追加 Emby 标签">
      <template #header-extra>
        <n-button type="primary" @click="addRule">添加规则</n-button>
      </template>
      
      <n-alert title="操作提示" type="info" style="margin-top: 24px;">
        <li><b>实时打标:</b> 实时入库的媒体项命中配置的媒体库（且符合分级）会自动打标。</li>
        <li><b>分级过滤:</b> 如果选择了分级，只有<b>映射后</b>的中文分级（如“限制级”）匹配时才会打标；留空则不限制。</li>
        <li><b>追加/移除:</b> 手动操作也会遵循配置的分级过滤条件。</li>
      </n-alert>

      <n-table :bordered="false" :single-line="false">
        <thead>
          <tr>
            <th style="width: 250px">目标媒体库</th>
            <th style="width: 200px">目标分级 (留空即全选)</th>
            <th>追加标签 (逗号分隔)</th>
            <th style="width: 280px">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(rule, index) in rules" :key="index">
            <!-- 媒体库选择 -->
            <td> 
              <n-select
                  v-model:value="rule.library_ids" 
                  :options="libraryOptions"
                  placeholder="留空即全选" 
                  filterable
                  multiple
                  clearable
                  collapse-tags
              />
            </td>
            
            <!-- 分级选择 (新增) -->
            <td>
              <n-select
                  v-model:value="rule.rating_filters"
                  :options="ratingOptions"
                  placeholder="不限分级"
                  multiple
                  clearable
                  collapse-tags
              />
            </td>

            <!-- 标签输入 -->
            <td>
              <n-dynamic-tags v-model:value="rule.tags" />
            </td>

            <!-- 操作按钮 -->
            <td>
              <n-space>
                <n-button size="small" type="primary" secondary @click="appendNow(rule)">
                    追加
                </n-button>
                <n-button size="small" type="error" secondary @click="clearNow(rule)">
                    移除
                </n-button>
                <n-button size="small" type="error" ghost @click="removeRule(index)">
                    删除
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
const ratingOptions = ref([]); // 新增：分级选项
const saving = ref(false);

// 获取媒体库列表
const fetchLibraries = async () => {
  try {
    const res = await axios.get('/api/emby_libraries');
    libraryOptions.value = res.data.map(l => ({ 
      label: l.Name, 
      value: l.Id 
    }));
  } catch (e) { 
    console.error('获取媒体库失败:', e);
    message.error('获取媒体库失败，请检查后端 API'); 
  }
};

// 新增：获取分级选项 (从映射表中提取唯一的中文标签)
const fetchRatingOptions = async () => {
  try {
    // 直接调用现有的后端接口，该接口已包含数据库读取和默认值兜底逻辑
    const res = await axios.get('/api/custom_collections/config/rating_mapping'); 
    const mapping = res.data || {};
    
    const uniqueLabels = new Set();
    
    // 遍历所有国家的映射规则，提取 label
    // mapping 结构: { "US": [{code: "R", label: "限制级"}, ...], "CN": [...] }
    Object.values(mapping).forEach(countryRules => {
      if (Array.isArray(countryRules)) {
        countryRules.forEach(r => {
          if (r.label) uniqueLabels.add(r.label);
        });
      }
    });

    // 将 Set 转换为前端 Select 组件需要的 options 格式
    ratingOptions.value = Array.from(uniqueLabels).map(label => ({
      label: label,
      value: label
    }));
    
    console.log('加载分级选项成功:', ratingOptions.value);
    
  } catch (e) {
    console.error('获取分级映射失败:', e);
    message.error('无法加载分级选项，请检查网络或后端日志');
    // 出错时清空选项，避免误导用户
    ratingOptions.value = [];
  }
};

const fetchRules = async () => {
  const res = await axios.get('/api/auto_tagging/rules');
  // 确保旧数据也有 rating_filters 字段
  rules.value = res.data.map(rule => ({
    ...rule,
    rating_filters: rule.rating_filters || []
  }));
};

const addRule = () => {
  rules.value.push({ library_ids: [], tags: [], rating_filters: [] });
};

const removeRule = (index) => {
  rules.value.splice(index, 1);
};

const saveRules = async () => {
  // ★★★ 新增校验逻辑 ★★★
  for (let i = 0; i < rules.value.length; i++) {
    const rule = rules.value[i];
    const noLib = !rule.library_ids || rule.library_ids.length === 0;
    const noRating = !rule.rating_filters || rule.rating_filters.length === 0;
    
    if (noLib && noRating) {
      message.error(`第 ${i + 1} 条规则无效：【目标媒体库】和【目标分级】不能同时为空！否则会误伤全站视频。`);
      return;
    }
    
    if (!rule.tags || rule.tags.length === 0) {
      message.error(`第 ${i + 1} 条规则无效：必须填写【追加标签】。`);
      return;
    }
  }

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

  if ((!rule.library_ids || !rule.library_ids.length) && (!rule.rating_filters || !rule.rating_filters.length)) {
     return message.error('为了安全，不能对“所有媒体库”且“不限分级”执行批量操作！');
  }

  // 2. 格式化显示信息
  const libNames = libraryOptions.value
    .filter(o => rule.library_ids.includes(o.value))
    .map(o => o.label)
    .join(', ');
  
  const ratingInfo = rule.rating_filters && rule.rating_filters.length > 0 
    ? ` (仅限: ${rule.rating_filters.join(', ')})` 
    : ' (所有分级)';

  // 3. 弹出确认模态框
  dialog.info({
    title: '确认追加标签',
    content: `确定要为媒体库 [${libNames}]${ratingInfo} 的所有影片追加标签 [${rule.tags.join(', ')}] 吗？`,
    positiveText: '开始运行',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await axios.post('/api/auto_tagging/run_now', {
          library_ids: rule.library_ids,
          tags: rule.tags,
          rating_filters: rule.rating_filters, // 传递分级参数
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
  // 1. 基础校验
  if (!rule.tags?.length) {
    return message.warning('请填写要移除的标签');
  }
  
  // ★★★ 新增：防止手动运行时也是全空 ★★★
  if ((!rule.library_ids || !rule.library_ids.length) && (!rule.rating_filters || !rule.rating_filters.length)) {
     return message.error('为了安全，不能对“所有媒体库”且“不限分级”执行批量操作！');
  }

  const libNames = (!rule.library_ids || !rule.library_ids.length)
    ? '所有媒体库'
    : libraryOptions.value
        .filter(o => rule.library_ids.includes(o.value))
        .map(o => o.label).join(', ');
    
  const ratingInfo = rule.rating_filters && rule.rating_filters.length > 0 
    ? ` (仅限: ${rule.rating_filters.join(', ')})` 
    : '';

  dialog.warning({
    title: '确认移除标签',
    content: `确定要从 [${libNames}]${ratingInfo} 中移除标签 [${rule.tags.join(', ')}] 吗？`,
    positiveText: '立即移除',
    onPositiveClick: async () => {
      await axios.post('/api/auto_tagging/clear_now', {
        library_ids: rule.library_ids,
        tags: rule.tags,
        rating_filters: rule.rating_filters, // 传递分级参数
        library_name: libNames
      });
      message.success('移除任务已提交');
    }
  });
};

onMounted(() => {
  fetchLibraries();
  fetchRatingOptions(); // 加载分级选项
  fetchRules();
});
</script>