<template>
  <n-layout content-style="padding: 24px;">
    <n-page-header title="查看更新">
      <template #extra>
        <n-tooltip>
          <template #trigger>
            <n-button @click="showSponsorModal = true" type="primary" ghost>
              <template #icon><n-icon :component="CafeIcon" /></template>
              请我喝杯奶茶
            </n-button>
          </template>
          用爱发电不易，您的支持是项目前进的最大动力！
        </n-tooltip>
        <n-button tag="a" :href="`https://github.com/${githubRepo}/issues`" target="_blank" secondary>
          <template #icon><n-icon :component="LogoGithub" /></template>
          反馈问题
        </n-button>
        
        <n-button 
          v-if="appStore.isUpdateAvailable" 
          type="success" 
          @click="handleUpdate"
          :loading="isUpdating"
        >
          立即更新
        </n-button>
      </template>
    </n-page-header>
    <n-divider />

    <div v-if="isLoading" class="dashboard-card"><n-spin size="large" /></div>
    <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error">{{ error }}</n-alert></div>
    
    <div v-else>
      <n-list hoverable clickable>
        <n-list-item v-for="(release, index) in appStore.releases" :key="release.version">
          <n-thing>
            <template #header>
              <n-space align="center">
                <a :href="release.url" target="_blank" class="version-link">{{ release.version }}</a>
                <n-tag v-if="index === 0" type="success" size="small" round>最新软件版本</n-tag>
                <n-tag v-if="release.version === appStore.currentVersion" type="info" size="small" round>当前版本</n-tag>
              </n-space>
            </template>
            <template #header-extra>
              <n-text :depth="3">{{ formatReleaseDate(release.published_at) }}</n-text>
            </template>
            <div class="changelog-content" v-html="renderMarkdown(release.changelog)"></div>
          </n-thing>
        </n-list-item>
      </n-list>
    </div>

    <!-- 支持开发者 模态框 -->
    <n-modal v-model:show="showSponsorModal" preset="card" style="width: 90%; max-width: 400px;" title="支持开发者" :bordered="false">
      <div class="sponsor-content">
        <n-p>
          用ai发电也不易，喝杯奶茶行不行！
        </n-p>
        <n-p>
          您的支持，哪怕是一点点，都是我持续更新的最大动力。感谢您的慷慨！
        </n-p>
        <n-divider />
        <div class="qr-code-item">
          <n-image width="200" src="/img/wechat_pay.png" />
          <n-text strong style="margin-top: 10px;">推荐使用微信支付</n-text>
        </div>
      </div>
    </n-modal>

    <!-- ▼▼▼【优化后】更新进度模态框 ▼▼▼ -->
    <n-modal
      v-model:show="showUpdateModal"
      :mask-closable="false"
      preset="card"
      title="正在更新应用"
      style="width: 90%; max-width: 500px;"
    >
      <n-space align="center" style="margin-top: 20px; margin-bottom: 20px;">
        <!-- 动态加载动画 -->
        <n-spin size="small" />
        <!-- 状态文本 -->
        <n-text>{{ updateStatusText }}</n-text>
      </n-space>

      <template #footer>
        <div style="text-align: right;">
          <n-button @click="showUpdateModal = false" :disabled="!isUpdateFinished">
            关闭
          </n-button>
        </div>
      </template>
    </n-modal>
    <!-- ▲▲▲ 优化结束 ▲▲▲ -->

  </n-layout>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue';
import { marked } from 'marked';
import { formatDistanceToNow, parseISO } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import { 
  NLayout, NPageHeader, NDivider, NSpin, NAlert, NList, NListItem, NThing, 
  NTag, NSpace, NButton, NIcon, NText, NModal, NTooltip, useDialog,
  NImage, NP // 确保导入了 NImage 和 NP
} from 'naive-ui';
import { LogoGithub, CafeOutline as CafeIcon } from '@vicons/ionicons5';
import { useAppStore } from '../stores/app';

const dialog = useDialog();
const appStore = useAppStore();

const githubRepoOwner = 'hbq0405';
const githubRepoName = 'emby-toolkit';
const githubRepo = computed(() => `${githubRepoOwner}/${githubRepoName}`);

const isLoading = ref(false);
const error = ref(null);
const showSponsorModal = ref(false);

// --- 更新状态相关的响应式变量 ---
const isUpdating = ref(false);
const showUpdateModal = ref(false);
const updateStatusText = ref('');
const isUpdateFinished = ref(false);
let eventSource = null;

const handleUpdate = () => {
  dialog.warning({
    title: '确认更新',
    content: '这将拉取最新的镜像并重启应用，期间服务将短暂中断。确定要继续吗？',
    positiveText: '立即更新',
    negativeText: '取消',
    onPositiveClick: () => {
      // 重置状态
      showUpdateModal.value = true;
      isUpdateFinished.value = false;
      isUpdating.value = true;
      updateStatusText.value = '正在连接到更新服务...';

      eventSource = new EventSource('/api/system/update/stream');

      eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        // 只需更新状态文本
        if (data.status) {
          updateStatusText.value = data.status;
        }
        
        // 检查更新流是否结束
        if (data.event === 'DONE' || data.event === 'ERROR') {
          isUpdateFinished.value = true;
          isUpdating.value = false;
          eventSource.close();
        }
      };

      eventSource.onerror = (err) => {
        console.error('EventSource failed:', err);
        updateStatusText.value = '与服务器的连接中断。可能正在重启，请稍后刷新。';
        isUpdateFinished.value = true;
        isUpdating.value = false;
        if (eventSource) {
          eventSource.close();
        }
      };
    },
  });
};

const fetchData = async () => {
  isLoading.value = true;
  error.value = null;
  try {
    await appStore.fetchVersionInfo();
  } catch (err) {
    error.value = '无法获取版本信息，请检查网络或后端服务。';
  } finally {
    isLoading.value = false;
  }
};

const renderMarkdown = (markdownText) => {
  if (!markdownText) return '';
  return marked.parse(markdownText, { gfm: true, breaks: true });
};

const formatReleaseDate = (dateString) => {
  if (!dateString) return '';
  return formatDistanceToNow(parseISO(dateString), { addSuffix: true, locale: zhCN });
};

onMounted(fetchData);
</script>

<style scoped>
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: calc(100vh - 200px);
}
.version-link {
  font-size: 1.2em;
  font-weight: 600;
  color: var(--n-text-color);
  text-decoration: none;
}
.version-link:hover {
  text-decoration: underline;
}
.changelog-content {
  margin-top: 8px;
  padding-left: 4px;
  color: var(--n-text-color-2);
}
.changelog-content :deep(ul) {
  padding-left: 20px;
  margin: 0;
}
.changelog-content :deep(li) {
  margin-bottom: 4px;
}
.changelog-content :deep(pre) {
  background-color: rgba(128, 128, 128, 0.1);
  padding: 12px 16px;
  border-radius: 6px;
  overflow-x: auto;
  margin: 10px 0;
}
.changelog-content :deep(code) {
  font-family: Consolas, Monaco, 'Andale Mono', 'Ubuntu Mono', monospace;
  font-size: 0.9em;
}
.sponsor-content {
  text-align: center;
}
.qr-code-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 10px;
  padding-top: 10px;
}
</style>