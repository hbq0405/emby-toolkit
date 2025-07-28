<template>
  <n-config-provider :theme="isDarkTheme ? darkTheme : undefined" :theme-overrides="themeOverridesComputed" :locale="zhCN" :date-locale="dateZhCN">
    <n-message-provider>
      <n-dialog-provider>
        <!-- 
          在这里，我们创建一个 Content 组件，
          它将成为 n-dialog-provider 的直接子组件，
          从而可以安全地使用 useDialog 和 useMessage。
        -->
        <Content />
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup>
import { defineComponent, h, computed, ref, watchEffect } from 'vue';
import { 
  NConfigProvider, 
  NMessageProvider, 
  NDialogProvider, 
  darkTheme, 
  zhCN, 
  dateZhCN,
  useDialog, // 我们在这里导入 useDialog 和 useMessage
  useMessage
} from 'naive-ui';
import App from './App.vue'; // 导入我们原来的 App.vue

// 这是一个内部组件，它现在处于 Provider 的包裹之下
const Content = defineComponent({
  setup() {
    // 在这里注入 dialog 和 message 的 API 到 window 对象上
    // 这样，在 App.vue 或其他任何地方都可以通过 window.$dialog 访问
    window.$dialog = useDialog();
    window.$message = useMessage();
  },
  render() {
    // 渲染我们真正的 App 组件
    return h(App);
  }
});

// --- 以下是从 App.vue 移动过来的主题和样式逻辑 ---

const isDarkTheme = ref(localStorage.getItem('theme') !== 'light');

watchEffect(() => {
  const html = document.documentElement;
  html.classList.remove('dark', 'light');
  html.classList.add(isDarkTheme.value ? 'dark' : 'light');
  localStorage.setItem('theme', isDarkTheme.value ? 'dark' : 'light');
});

const themeOverridesComputed = computed(() => {
  const lightCardShadow = '0 1px 2px -2px rgba(0, 0, 0, 0.08), 0 3px 6px 0 rgba(0, 0, 0, 0.06), 0 5px 12px 4px rgba(0, 0, 0, 0.04)';
  const darkCardShadow = '0 1px 2px -2px rgba(0, 0, 0, 0.24), 0 3px 6px 0 rgba(0, 0, 0, 0.18), 0 5px 12px 4px rgba(0, 0, 0, 0.12)';

  if (!isDarkTheme.value) {
    return {
      common: { bodyColor: '#f0f2f5' },
      Card: { boxShadow: lightCardShadow }
    };
  }
  
  return {
    common: { 
      bodyColor: '#101014', 
      cardColor: '#1a1a1e', 
      inputColor: '#1a1a1e', 
      actionColor: '#242428', 
      borderColor: 'rgba(255, 255, 255, 0.12)' 
    },
    Card: { 
      color: '#1a1a1e', 
      titleTextColor: 'rgba(255, 255, 255, 0.92)',
      boxShadow: darkCardShadow,
    },
    DataTable: { 
      tdColor: '#1a1a1e', 
      thColor: '#1a1a1e', 
      tdColorStriped: '#202024' 
    },
    Input: { color: '#1a1a1e' },
    Select: { peers: { InternalSelection: { color: '#1a1a1e' } } }
  };
});
</script>