// src/composables/useTaskStatus.js

import { ref, computed, onMounted, onUnmounted } from 'vue';
import axios from 'axios';

// 将状态变量放在函数外部，确保它们在整个应用中是单例的
const backgroundTaskStatus = ref({
  is_running: false,
  current_action: '无',
  progress: 0,
  message: '等待任务'
});

let statusInterval = null;

// 获取状态的函数
const fetchStatus = async () => {
  try {
    const response = await axios.get('/api/status');
    backgroundTaskStatus.value = response.data;
  } catch (error) {
    // 在这里可以静默处理错误，或者只在控制台打印
    // console.error("获取后台状态失败:", error);
  }
};

export function useTaskStatus() {
  // onMounted 会在组件第一次使用这个 composable 时被调用
  onMounted(() => {
    // 只有在没有定时器的情况下才启动一个新的，防止重复启动
    if (!statusInterval) {
      fetchStatus(); // 立即获取一次
      statusInterval = setInterval(fetchStatus, 2000); // 每2秒获取一次
    }
  });

  // onUnmounted 会在组件销毁时被调用
  onUnmounted(() => {
    // 实际上，对于全局状态，我们通常不希望它停止
    // 但为了代码的完整性，保留这个逻辑
    // 如果你希望状态轮询在离开页面后停止，就保留它
    // 如果希望它一直运行，可以注释掉下面的 clearInterval
    // clearInterval(statusInterval);
    // statusInterval = null;
  });

  // 创建一个易于使用的计算属性
  const isBackgroundTaskRunning = computed(() => {
    return backgroundTaskStatus.value.is_running;
  });

  // 返回所有需要被外部使用的状态和变量
  return {
    backgroundTaskStatus,
    isBackgroundTaskRunning
  };
}