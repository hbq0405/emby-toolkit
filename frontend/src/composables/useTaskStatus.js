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

const TASK_STATUS_EVENT = 'etk-task-status';
let statusTimer = null;
let statusAbortController = null;
let statusFetchInFlight = false;
let subscriberCount = 0;
let listenerAttached = false;
let lastExternalUpdateAt = 0;

const applyStatus = (status) => {
  if (status && typeof status === 'object') {
    backgroundTaskStatus.value = status;
  }
};

const handleExternalStatus = (event) => {
  lastExternalUpdateAt = Date.now();
  applyStatus(event.detail);
};

const clearStatusTimer = () => {
  if (statusTimer) {
    clearTimeout(statusTimer);
    statusTimer = null;
  }
};

const scheduleStatusFetch = (delay = 2500) => {
  if (subscriberCount <= 0) return;
  clearStatusTimer();
  statusTimer = setTimeout(fetchStatus, delay);
};

// 获取状态的函数。主布局已经在轮询时，这里只做兜底，避免重复请求。
const fetchStatus = async () => {
  if (subscriberCount <= 0 || statusFetchInFlight) return;
  if (Date.now() - lastExternalUpdateAt < 5000) {
    scheduleStatusFetch(3000);
    return;
  }
  statusFetchInFlight = true;
  statusAbortController = new AbortController();
  try {
    const response = await axios.get('/api/status', {
      timeout: 8000,
      signal: statusAbortController.signal,
    });
    applyStatus(response.data);
  } catch (error) {
    // 在这里可以静默处理错误，或者只在控制台打印
    // console.error("获取后台状态失败:", error);
  } finally {
    statusFetchInFlight = false;
    statusAbortController = null;
    scheduleStatusFetch(3000);
  }
};

export function useTaskStatus() {
  // onMounted 会在组件第一次使用这个 composable 时被调用
  onMounted(() => {
    subscriberCount += 1;
    if (!listenerAttached && typeof window !== 'undefined') {
      window.addEventListener(TASK_STATUS_EVENT, handleExternalStatus);
      listenerAttached = true;
    }
    fetchStatus();
  });

  // onUnmounted 会在组件销毁时被调用
  onUnmounted(() => {
    subscriberCount = Math.max(0, subscriberCount - 1);
    if (subscriberCount > 0) return;
    clearStatusTimer();
    if (statusAbortController) {
      statusAbortController.abort();
      statusAbortController = null;
    }
    statusFetchInFlight = false;
    if (listenerAttached && typeof window !== 'undefined') {
      window.removeEventListener(TASK_STATUS_EVENT, handleExternalStatus);
      listenerAttached = false;
    }
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
