<!-- src/components/modals/SubscriptionConfigForm.vue (防无限循环最终版) -->
<template>
  <n-form label-placement="left" label-width="auto" :model="localConfig">
    <n-form-item label="起始年份">
      <n-input-number v-model:value="localConfig.start_year" placeholder="例如: 2010" />
      <n-text depth="3" style="margin-left: 10px;">只追踪此年份之后的作品</n-text>
    </n-form-item>
    <n-form-item label="媒体类型">
      <n-checkbox-group v-model:value="localConfig.media_types">
        <n-space>
          <n-checkbox value="Movie" label="电影" />
          <n-checkbox value="TV" label="电视剧" />
        </n-space>
      </n-checkbox-group>
    </n-form-item>
    <n-form-item label="包含的题材">
      <n-select
        v-model:value="localConfig.genres_include_json"
        multiple
        placeholder="留空则包含所有题材"
        :options="genreOptions"
      />
    </n-form-item>
    <n-form-item label="排除的题材">
      <n-select
        v-model:value="localConfig.genres_exclude_json"
        multiple
        placeholder="选择要排除的题材"
        :options="genreOptions"
      />
    </n-form-item>
    <n-form-item label="最低评分">
      <n-input-number v-model:value="localConfig.min_rating" :min="0" :max="10" :step="0.1" style="width: 100%;" placeholder="0.0"/>
      <template #feedback>
        <n-text depth="3">
          设置为 0 表示不筛选。系统将自动对发布未满6个月的新影片豁免此规则。
        </n-text>
      </template>
    </n-form-item>
    <n-form-item label="仅限主演">
      <n-checkbox v-model:checked="localConfig.main_role_only">
        只筛选该演员在演员表中排名前三的作品
      </n-checkbox>
      <template #feedback>
        <n-text depth="3">
          勾选后将尝试过滤掉客串、龙套等非主要角色。注意：此功能依赖的数据可能不完整，可能会误过滤部分联合主演的作品。
        </n-text>
      </template>
    </n-form-item>
  </n-form>
</template>

<script setup>
import { ref, watch } from 'vue';
import { NForm, NFormItem, NInputNumber, NCheckboxGroup, NCheckbox, NSelect, NSpace, NText } from 'naive-ui';

// 题材列表保持不变
const genreOptions = [
  { label: '动作', value: 28 }, { label: '冒险', value: 12 }, { label: '动画', value: 16 },
  { label: '喜剧', value: 35 }, { label: '犯罪', value: 80 }, { label: '纪录', value: 99 },
  { label: '剧情', value: 18 }, { label: '家庭', value: 10751 }, { label: '奇幻', value: 14 },
  { label: '历史', value: 36 }, { label: '恐怖', value: 27 }, { label: '音乐', value: 10402 },
  { label: '悬疑', value: 9648 }, { label: '爱情', value: 10749 }, { label: '科幻', value: 878 },
  { label: '电视电影', value: 10770 }, { label: '惊悚', value: 53 }, { label: '战争', value: 10752 },
  { label: '西部', value: 37 }, { label: '综艺-真人秀', value: 10767 }, { label: '综艺-脱口秀', value: 10764 },
];

const props = defineProps({
  modelValue: Object,
});
const emit = defineEmits(['update:modelValue']);

// 本地 ref 保持不变
const localConfig = ref({});

// ★★★ 核心修复：使用卫语句打破无限循环 ★★★

// 监视器 1: 从父组件到子组件的同步
watch(() => props.modelValue, (parentValue) => {
  // 只有当父组件的数据和本地数据不一致时，才进行更新
  // 使用 JSON.stringify 进行简单的深比较，足以应对此场景
  if (parentValue && JSON.stringify(parentValue) !== JSON.stringify(localConfig.value)) {
    console.log('Parent data changed, updating local component state.');
    localConfig.value = JSON.parse(JSON.stringify(parentValue)); // 深拷贝
  }
}, {
  deep: true,
  immediate: true,
});

// 监视器 2: 从子组件到父组件的同步
watch(localConfig, (localValue) => {
  // 只有当本地数据和父组件数据不一致时，才通知父组件
  // 这可以防止因父组件更新而触发的“回声”
  if (localValue && JSON.stringify(localValue) !== JSON.stringify(props.modelValue)) {
    console.log('Local data changed by user, emitting update to parent.');
    emit('update:modelValue', localValue);
  }
}, {
  deep: true,
});
</script>