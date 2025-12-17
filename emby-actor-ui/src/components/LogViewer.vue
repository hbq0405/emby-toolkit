<!-- src/components/LogViewer.vue -->
<template>
  <n-drawer
    :show="show"
    :width="900"
    @update:show="$emit('update:show', $event)"
    placement="right"
    resizable
  >
    <n-drawer-content title="历史日志查看器" closable>
      <n-space vertical style="height: 100%">
        <!-- 搜索栏 -->
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

        <!-- 搜索模式切换 -->
        <n-radio-group v-model:value="searchMode" name="search-mode-radio">
          <n-radio-button value="filter" :disabled="isLoading">
            筛选模式 (JSON列表)
          </n-radio-button>
          <n-radio-button value="context" :disabled="isLoading">
            定位模式 (美化视图)
          </n-radio-button>
        </n-radio-group>

        <n-divider style="margin-top: 5px; margin-bottom: 5px;" />

        <!-- 结果展示区 -->
        <n-spin :show="isLoading" style="height: calc(100vh - 180px)">
          
          <!-- ★★★ 场景 1: 定位模式 (HTML iframe 渲染) ★★★ -->
          <div v-if="isSearchMode && searchMode === 'context'" style="height: 100%">
             <n-button @click="clearSearch" size="small" style="margin-bottom: 10px;">
              <template #icon><n-icon :component="ArrowBackOutline" /></template>
              返回文件浏览
            </n-button>
            
            <div v-if="htmlContent" class="iframe-container">
              <!-- 使用 srcdoc 直接渲染后端返回的 HTML 字符串 -->
              <iframe 
                :srcdoc="htmlContent" 
                frameborder="0" 
                width="100%" 
                height="100%"
                style="background-color: #1e1e1e;"
              ></iframe>
            </div>
            <n-empty v-else description="未找到匹配的完整处理流程。" style="margin-top: 50px;" />
          </div>

          <!-- ★★★ 场景 2: 筛选模式 (原有的 JSON 渲染) ★★★ -->
          <div v-else-if="isSearchMode && searchMode === 'filter'">
            <n-button @click="clearSearch" size="small" style="margin-bottom: 10px;">
              <template #icon><n-icon :component="ArrowBackOutline" /></template>
              返回文件浏览
            </n-button>
            
            <div v-if="hasSearchResults" class="log-viewer-container">
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
                <template v-else>
                  {{ line.content }}
                </template>
              </div>
            </div>
            <n-empty v-else description="未找到匹配的日志记录。" style="margin-top: 50px;" />
          </div>

          <!-- ★★★ 场景 3: 文件浏览视图 (默认) ★★★ -->
          <div v-else style="height: 100%; display: flex; flex-direction: column;">
            <n-select
              v-model:value="selectedFile"
              placeholder="请选择一个日志文件"
              :options="fileOptions"
              :loading="isLoadingFiles"
              @update:value="fetchLogContent"
            />

            <div v-if="logContent" class="log-viewer-container" style="margin-top: 10px; flex: 1;">
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
                <template v-else>
                  {{ line.content }}
                </template>
              </div>
            </div>
            <n-empty v-else description="无数据" style="margin-top: 50px;" />
          </div>
          <template #description>{{ loadingText }}</template>
        </n-spin>
      </n-space>
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

// --- Props, Emits ---
const props = defineProps({ show: { type: Boolean, default: false } });
const emit = defineEmits(['update:show']);

// --- Refs ---
const message = useMessage();
const isLoadingFiles = ref(false);
const isLoadingContent = ref(false);
const isSearching = ref(false);
const logFiles = ref([]);
const selectedFile = ref(null);
const logContent = ref('');
const searchQuery = ref('');
const searchResults = ref([]);
const htmlContent = ref(''); // ★ 新增：用于存储后端返回的 HTML 字符串
const isSearchMode = ref(false);
const searchMode = ref('context');

// --- Computed ---
const isLoading = computed(() => isLoadingFiles.value || isLoadingContent.value || isSearching.value);
const hasSearchResults = computed(() => searchResults.value.length > 0);
const fileOptions = computed(() => logFiles.value.map(file => ({ label: file, value: file })));
const loadingText = computed(() => {
  if (isLoadingFiles.value) return '正在获取文件列表...';
  if (isLoadingContent.value) return '正在加载日志内容...';
  if (isSearching.value) return `正在以 [${searchMode.value === 'context' ? '定位' : '筛选'}] 模式搜索...`;
  return '';
});

// 日志行解析函数 (保持不变，用于普通模式)
const parseLogLine = (line) => {
  const match = line.match(/^(\d{4}-\d{2}-\d{2}\s(\d{2}:\d{2}:\d{2})),\d+\s-\s.+?\s-\s(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s-\s(.*)$/);
  if (match) {
    return {
      type: 'log',
      timestamp: match[2],
      level: match[3],
      message: match[4].trim(),
    };
  }
  return { type: 'raw', content: line };
};

const parsedLogContent = computed(() => {
  if (!logContent.value) return [];
  return logContent.value.split('\n').map(parseLogLine);
});

// ★ 修改：parsedLogResults 只在 filter 模式下工作了
const parsedLogResults = computed(() => {
  if (!hasSearchResults.value || searchMode.value === 'context') return [];
  
  // 仅处理 filter 模式的逻辑
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

// --- Methods ---
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

// ★★★ 核心修改：executeSearch ★★★
const executeSearch = async () => {
  if (!searchQuery.value.trim()) {
    message.warning('请输入搜索关键词。');
    return;
  }
  isSearching.value = true;
  isSearchMode.value = true;
  searchResults.value = [];
  htmlContent.value = ''; // 清空旧 HTML

  try {
    if (searchMode.value === 'context') {
      // 1. 定位模式：请求 HTML 格式
      const response = await axios.get('/api/logs/search_context', { 
        params: { 
          q: searchQuery.value,
          format: 'html' // 告诉后端我要 HTML
        },
        responseType: 'text' // 告诉 axios 不要尝试解析 JSON
      });
      
      // 检查是否返回了空结果（后端可能返回空的 HTML 结构，或者我们需要判断长度）
      // 简单的判断：如果包含 "log-block" 类名，说明有结果
      if (response.data && response.data.includes('class="log-block"')) {
        htmlContent.value = response.data;
      } else {
        htmlContent.value = ''; // 显示空状态
      }

    } else {
      // 2. 筛选模式：保持原有的 JSON 逻辑
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
/* 保持原有的样式不变，用于普通文件浏览和筛选模式 */
.log-viewer-container {
  background-color: #282c34;
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
  padding: 10px 15px;
  border-radius: 6px;
  height: 100%; /* 撑满 */
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

.log-line { line-height: 1.6; padding: 1px 0; color: #abb2bf; }
.log-line.info { color: #98c379; }
.log-line.warning { color: #e5c07b; }
.log-line.error, .log-line.critical { color: #e06c75; }
.log-line.debug { color: #56b6c2; }
.log-line.raw { color: #95a5a6; font-style: italic; }
.timestamp { color: #61afef; margin-right: 1em; }
.level { font-weight: bold; margin-right: 1em; text-transform: uppercase; }

/* ★ 新增：iframe 容器样式 */
.iframe-container {
  width: 100%;
  height: calc(100% - 40px); /* 减去返回按钮的高度 */
  border-radius: 6px;
  overflow: hidden;
  border: 1px solid #333;
}
</style>