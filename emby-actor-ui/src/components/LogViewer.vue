<!-- src/components/LogViewer.vue -->
<template>
  <n-drawer
    :show="show"
    :width="900"
    @update:show="$emit('update:show', $event)"
    placement="right"
    resizable
  >
    <!-- 
      1. body-content-style: 强制 Drawer 内部主体变成 Flex 列布局，且高度 100%
      2. :native-scrollbar="false": 禁用 Drawer 自带的滚动条，交给我们内部容器自己管理滚动
    -->
    <n-drawer-content 
      title="历史日志查看器" 
      closable 
      :native-scrollbar="false"
      body-content-style="height: 100%; display: flex; flex-direction: column; padding: 0;"
    >
      <!-- 内部包裹层，负责统一的 Padding -->
      <div class="drawer-inner-wrapper">
        
        <!-- 顶部控制区 -->
        <n-space vertical class="header-section">
          <n-input-group>
            <n-input
              v-model:value="searchQuery"
              placeholder="在所有日志文件中搜索..."
              clearable
              @keyup.enter="executeSearch"
              :disabled="isLoading"
            />
            <n-button type="primary" @click="executeSearch" :loading="isSearching">
              搜索
            </n-button>
          </n-input-group>

          <n-radio-group v-model:value="searchMode" name="search-mode-radio">
            <n-radio-button value="filter" :disabled="isLoading">
              筛选模式 (JSON列表)
            </n-radio-button>
            <n-radio-button value="context" :disabled="isLoading">
              定位模式 (沉浸视图)
            </n-radio-button>
          </n-radio-group>
        </n-space>

        <n-divider style="margin: 10px 0;" />

        <!-- 
          核心修改区：
          给 n-spin 加上 class="flex-spin"，配合下方的 CSS 穿透，
          强行打通高度传递链。
        -->
        <n-spin :show="isLoading" class="flex-spin">
          
          <!-- 场景 1: 定位模式 (HTML iframe) -->
          <div v-if="isSearchMode && searchMode === 'context'" class="view-container">
             <div class="toolbar">
               <n-button @click="clearSearch" size="tiny" secondary>
                 <template #icon><n-icon :component="ArrowBackOutline" /></template>
                 返回
               </n-button>
               <span class="tip">已隐藏日期与模块名，仅显示核心流</span>
             </div>
            
            <div v-if="htmlContent" class="iframe-wrapper">
              <iframe 
                :srcdoc="htmlContent" 
                frameborder="0" 
                width="100%" 
                height="100%"
              ></iframe>
            </div>
            <n-empty v-else description="未找到匹配的完整处理流程。" style="margin-top: 50px;" />
          </div>

          <!-- 场景 2: 筛选模式 (JSON) -->
          <div v-else-if="isSearchMode && searchMode === 'filter'" class="view-container">
            <n-button @click="clearSearch" size="tiny" secondary style="margin-bottom: 5px;">
              <template #icon><n-icon :component="ArrowBackOutline" /></template>
              返回
            </n-button>
            
            <div v-if="hasSearchResults" class="log-text-area">
              <div 
                v-for="(line, index) in parsedLogResults" 
                :key="index" 
                class="log-line"
                :class="line.type === 'log' ? line.level.toLowerCase() : 'raw'"
              >
                <template v-if="line.type === 'log'">
                  <span class="timestamp">{{ line.timestamp }}</span>
                  <span class="level">{{ line.level }}</span>
                  <span class="message">{{ line.message }}</span>
                </template>
                <template v-else>{{ line.content }}</template>
              </div>
            </div>
            <n-empty v-else description="未找到匹配的日志记录。" style="margin-top: 50px;" />
          </div>

          <!-- 场景 3: 文件浏览 (默认) -->
          <div v-else class="view-container">
            <n-select
              v-model:value="selectedFile"
              placeholder="请选择日志文件"
              :options="fileOptions"
              :loading="isLoadingFiles"
              @update:value="fetchLogContent"
              size="small"
              style="margin-bottom: 5px;"
            />

            <div v-if="logContent" class="log-text-area">
              <div 
                v-for="(line, index) in parsedLogContent" 
                :key="index" 
                class="log-line"
                :class="line.type === 'log' ? line.level.toLowerCase() : 'raw'"
              >
                <template v-if="line.type === 'log'">
                  <span class="timestamp">{{ line.timestamp }}</span>
                  <span class="level">{{ line.level }}</span>
                  <span class="message">{{ line.message }}</span>
                </template>
                <template v-else>{{ line.content }}</template>
              </div>
            </div>
            <n-empty v-else description="无数据" style="margin-top: 50px;" />
          </div>
          <template #description>{{ loadingText }}</template>
        </n-spin>
      </div>
    </n-drawer-content>
  </n-drawer>
</template>

<script setup>
import { ref, watch, computed } from 'vue';
import axios from 'axios';
import { 
  useMessage, NDrawer, NDrawerContent, NSelect, NSpace, NSpin, 
  NInput, NInputGroup, NButton, NDivider, NEmpty, NIcon,
  NRadioGroup, NRadioButton
} from 'naive-ui';
import { ArrowBackOutline } from '@vicons/ionicons5';

const props = defineProps({ show: { type: Boolean, default: false } });
const emit = defineEmits(['update:show']);

const message = useMessage();
const isLoadingFiles = ref(false);
const isLoadingContent = ref(false);
const isSearching = ref(false);
const logFiles = ref([]);
const selectedFile = ref(null);
const logContent = ref('');
const searchQuery = ref('');
const searchResults = ref([]);
const htmlContent = ref('');
const isSearchMode = ref(false);
const searchMode = ref('context');

const isLoading = computed(() => isLoadingFiles.value || isLoadingContent.value || isSearching.value);
const hasSearchResults = computed(() => searchResults.value.length > 0);
const fileOptions = computed(() => logFiles.value.map(file => ({ label: file, value: file })));
const loadingText = computed(() => {
  if (isLoadingFiles.value) return '正在获取文件列表...';
  if (isLoadingContent.value) return '正在加载日志内容...';
  if (isSearching.value) return `正在以 [${searchMode.value === 'context' ? '定位' : '筛选'}] 模式搜索...`;
  return '';
});

const parseLogLine = (line) => {
  const match = line.match(/^(\d{4}-\d{2}-\d{2}\s(\d{2}:\d{2}:\d{2})),\d+\s-\s.+?\s-\s(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s-\s(.*)$/);
  if (match) {
    return { type: 'log', timestamp: match[2], level: match[3], message: match[4].trim() };
  }
  return { type: 'raw', content: line };
};

const parsedLogContent = computed(() => {
  if (!logContent.value) return [];
  return logContent.value.split('\n').map(parseLogLine);
});

const parsedLogResults = computed(() => {
  if (!hasSearchResults.value || searchMode.value === 'context') return [];
  const finalLines = [];
  finalLines.push(`以“筛选”模式找到 ${searchResults.value.length} 条结果:`);
  let lastFile = '';
  searchResults.value.forEach(result => {
    if (result.file !== lastFile) {
      if (finalLines.length > 1) finalLines.push('');
      finalLines.push(`--- [ 文件: ${result.file} ] ---`);
      lastFile = result.file;
    }
    finalLines.push(result.content);
  });
  return finalLines.map(parseLogLine);
});

const fetchLogFiles = async () => {
  isLoadingFiles.value = true;
  try {
    const response = await axios.get('/api/logs/list');
    logFiles.value = response.data;
    if (!isSearchMode.value && logFiles.value.length > 0 && !selectedFile.value) {
      selectedFile.value = logFiles.value[0];
      await fetchLogContent(selectedFile.value);
    }
  } catch (error) {
    message.error('获取日志文件列表失败！');
  } finally {
    isLoadingFiles.value = false;
  }
};

const fetchLogContent = async (filename) => {
  if (!filename) return;
  isLoadingContent.value = true;
  logContent.value = `正在加载 ${filename}...`;
  try {
    const response = await axios.get('/api/logs/view', { params: { filename } });
    logContent.value = response.data || '（文件为空）';
  } catch (error) {
    message.error(`加载日志 ${filename} 失败！`);
  } finally {
    isLoadingContent.value = false;
  }
};

const executeSearch = async () => {
  if (!searchQuery.value.trim()) {
    message.warning('请输入搜索关键词。');
    return;
  }
  isSearching.value = true;
  isSearchMode.value = true;
  searchResults.value = [];
  htmlContent.value = '';

  try {
    if (searchMode.value === 'context') {
      const response = await axios.get('/api/logs/search_context', { 
        params: { q: searchQuery.value, format: 'html' },
        responseType: 'text'
      });
      if (response.data && response.data.includes('class="log-block"')) {
        htmlContent.value = response.data;
      } else {
        htmlContent.value = '';
      }
    } else {
      const response = await axios.get('/api/logs/search', { params: { q: searchQuery.value } });
      searchResults.value = response.data;
    }
  } catch (error) {
    console.error(error);
    message.error('搜索失败，请检查后台日志。');
  } finally {
    isSearching.value = false;
  }
};

const clearSearch = () => {
  isSearchMode.value = false;
  searchQuery.value = '';
  searchResults.value = [];
  htmlContent.value = '';
  if (selectedFile.value && !logContent.value) {
    fetchLogContent(selectedFile.value);
  }
};

watch(() => props.show, (newVal) => {
  if (newVal) {
    fetchLogFiles();
  } else {
    clearSearch();
    selectedFile.value = null;
    logFiles.value = [];
    logContent.value = '';
  }
});
</script>

<style scoped>
/* 1. 确保 Drawer 内部的最外层容器撑满 */
.drawer-inner-wrapper {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 15px;
  box-sizing: border-box;
}

.header-section {
  flex-shrink: 0; /* 头部不许压缩 */
}

/* 2. 强力修复 n-spin 高度塌陷问题 */
.flex-spin {
  flex: 1; /* 占据剩余所有空间 */
  display: flex;
  flex-direction: column;
  overflow: hidden; /* 防止溢出 */
  min-height: 0; /* Flex 嵌套滚动的关键 */
}

/* ★★★ CSS 穿透：强制 n-spin 内部容器也撑满 ★★★ */
:deep(.n-spin-content) {
  height: 100%;
  display: flex;
  flex-direction: column;
}

/* 3. 视图容器：撑满 n-spin 的内部 */
.view-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.toolbar {
  flex-shrink: 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.tip {
  font-size: 12px;
  color: #999;
}

/* 4. iframe 容器：绝对撑满，不留白 */
.iframe-wrapper {
  flex: 1;
  border: 1px solid #333;
  border-radius: 4px;
  overflow: hidden;
  background-color: #1e1e1e;
  /* 确保 iframe 容器本身有高度 */
  height: 100%; 
  display: flex;
}

iframe {
  flex: 1;
  display: block;
}

/* 5. 普通日志文本区域 */
.log-text-area {
  flex: 1;
  background-color: #282c34;
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
  padding: 10px 15px;
  border-radius: 4px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

/* 日志行样式 */
.log-line { line-height: 1.6; padding: 1px 0; color: #abb2bf; }
.log-line.info { color: #98c379; }
.log-line.warning { color: #e5c07b; }
.log-line.error, .log-line.critical { color: #e06c75; }
.log-line.debug { color: #56b6c2; }
.log-line.raw { color: #95a5a6; font-style: italic; }
.timestamp { color: #61afef; margin-right: 1em; }
.level { font-weight: bold; margin-right: 1em; text-transform: uppercase; }
</style>