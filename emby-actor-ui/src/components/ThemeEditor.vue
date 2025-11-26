<!-- src/components/ThemeEditor.vue -->
<template>
  <n-modal :show="show" preset="card" style="width: 90%; max-width: 800px;" title="主题设计工坊" :bordered="false" size="huge" :mask-closable="false" @update:show="handleClose">
    <template #header-extra>
      <n-space>
        <n-popconfirm
          @positive-click="handleDelete"
          :positive-button-props="{ type: 'error' }"
          positive-text="确认删除"
          negative-text="我再想想"
        >
          <template #trigger>
            <n-button type="error" size="small" ghost>删除自定义主题</n-button>
          </template>
          确定要永久删除你的专属主题吗？<br>删除后将无法恢复，并自动切换回默认主题。
        </n-popconfirm>
        
        <n-button @click="handleReset" size="small" secondary>撤销本次更改</n-button>
      </n-space>
    </template>
    
    <div v-if="editableTheme && editableTheme.naive && editableTheme.custom">
      <n-tabs type="line" animated>
        <n-tab-pane name="colors" tab="核心色彩">
          <n-grid :cols="2" :x-gap="24">
            <n-gi><n-form-item label="UI主色调 (Naive UI)"><n-color-picker v-model:value="editableTheme.naive.common.primaryColor" /></n-form-item></n-gi>
            <n-gi><n-form-item label="卡片标题色 (自定义)"><n-color-picker v-model:value="editableTheme.custom['--accent-color']" /></n-form-item></n-gi>
            <n-gi><n-form-item label="主题辉光色 (Glow)"><n-color-picker v-model:value="editableTheme.custom['--accent-glow-color']" /></n-form-item></n-gi>
          </n-grid>
        </n-tab-pane>
        <n-tab-pane name="sidebar" tab="侧边栏与菜单">
          <n-grid :cols="2" :x-gap="24">
            <n-gi><n-form-item label="侧边栏背景"><n-color-picker v-model:value="editableTheme.naive.Layout.siderColor" /></n-form-item></n-gi>
            <n-gi><n-form-item label="菜单文字"><n-color-picker v-model:value="editableTheme.naive.Menu.itemTextColor" /></n-form-item></n-gi>
            <n-gi><n-form-item label="菜单图标"><n-color-picker v-model:value="editableTheme.naive.Menu.itemIconColor" /></n-form-item></n-gi>
            <n-gi><n-form-item label="菜单激活文字"><n-color-picker v-model:value="editableTheme.naive.Menu.itemTextColorActive" /></n-form-item></n-gi>
          </n-grid>
        </n-tab-pane>
        <n-tab-pane name="cards" tab="卡片样式">
           <n-grid :cols="2" :x-gap="24">
            <!-- ★★★ 新增：卡片大小滑块 (跨两列) ★★★ -->
            <n-gi :span="2">
              <n-form-item label="卡片整体缩放 (0.8x - 1.2x)">
                <n-slider 
                  v-model:value="cardScale" 
                  :min="0.8" 
                  :max="1.2" 
                  :step="0.05" 
                  :marks="{ 1: '默认' }"
                  :format-tooltip="(v) => `${Math.round(v * 100)}%`"
                />
              </n-form-item>
            </n-gi>
            
            <n-gi><n-form-item label="卡片背景"><n-color-picker v-model:value="editableTheme.custom['--card-bg-color']" /></n-form-item></n-gi>
            <n-gi><n-form-item label="卡片边框"><n-color-picker v-model:value="editableTheme.custom['--card-border-color']" /></n-form-item></n-gi>
            <n-gi><n-form-item label="卡片阴影"><n-color-picker v-model:value="editableTheme.custom['--card-shadow-color']" /></n-form-item></n-gi>
            <n-gi><n-form-item label="卡片文字"><n-color-picker v-model:value="editableTheme.custom['--text-color']" /></n-form-item></n-gi>
          </n-grid>
        </n-tab-pane>
      </n-tabs>
    </div>
    <div v-else class="fullscreen-container"><n-spin size="large" /><p style="margin-left: 12px;">正在准备刻刀...</p></div>
    <template #footer><n-space justify="end"><n-button @click="handleClose">取消</n-button><n-button type="primary" @click="handleSave">保存并应用</n-button></n-space></template>
  </n-modal>
</template>

<script setup>
import { ref, watch, computed } from 'vue'; // ★★★ 引入 computed
import { NModal, NButton, NSpace, NTabs, NTabPane, NGrid, NGi, NFormItem, NColorPicker, NSpin, useMessage, NPopconfirm, NSlider } from 'naive-ui'; // ★★★ 引入 NSlider
import { cloneDeep } from 'lodash-es';

const props = defineProps({
  show: Boolean,
  initialTheme: Object,
  isDark: Boolean,
});

const emit = defineEmits(['update:show', 'save', 'update:preview', 'delete-custom-theme']);
const message = useMessage();
const editableTheme = ref(null);

// ★★★ 新增：计算属性处理卡片缩放 ★★★
// 作用：将 CSS 变量 (可能是字符串或未定义) 转换为 Slider 需要的数字，反之亦然
const cardScale = computed({
  get() {
    if (!editableTheme.value || !editableTheme.value.custom) return 1;
    const val = editableTheme.value.custom['--card-scale'];
    // 如果未定义，默认为 1
    return val ? parseFloat(val) : 1;
  },
  set(val) {
    if (editableTheme.value && editableTheme.value.custom) {
      editableTheme.value.custom['--card-scale'] = val;
    }
  }
});

watch(() => props.show, (newVal) => {
  if (newVal && props.initialTheme) {
    const currentModeTheme = props.initialTheme[props.isDark ? 'dark' : 'light'];
    editableTheme.value = cloneDeep(currentModeTheme);
  }
}, { immediate: true });

watch(editableTheme, (newVal) => {
  if (props.show && newVal) {
    emit('update:preview', newVal);
  }
}, { deep: true });

const handleSave = () => {
  emit('save', editableTheme.value);
};

const handleClose = () => {
  emit('update:show', false);
};

const handleReset = () => {
    const originalTheme = props.initialTheme[props.isDark ? 'dark' : 'light'];
    editableTheme.value = cloneDeep(originalTheme);
    message.info('已撤销本次更改。');
};

const handleDelete = () => {
  emit('delete-custom-theme');
};
</script>