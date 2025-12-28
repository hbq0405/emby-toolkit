<!-- src/components/modals/MappingManager.vue -->
<template>
  <div class="mapping-manager">
    <n-tabs type="segment" animated v-model:value="activeTab">
      
      <!-- ================= 关键词映射 Tab ================= -->
      <n-tab-pane name="keywords" tab="关键词映射">
        <n-alert type="info" :bordered="false" class="mb-4">
          将中文标签映射到 TMDb 的英文关键词或 ID。拖动行可调整排序。
        </n-alert>
        
        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文标签</div>
          <div class="col-en">英文关键词 (逗号分隔)</div>
          <div class="col-ids">TMDb IDs (逗号分隔)</div>
          <div class="col-action">操作</div>
        </div>

        <div ref="keywordListRef" class="sortable-list">
          <div v-for="(item, index) in keywordList" :key="item.id" class="list-item" :data-id="item.id">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label">
              <n-input v-model:value="item.label" placeholder="例如：丧尸" />
            </div>
            <div class="col-en">
              <n-input v-model:value="item.en" placeholder="zombie" />
            </div>
            <div class="col-ids">
              <n-input v-model:value="item.ids" placeholder="12377" />
            </div>
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(keywordList, index)">
                <n-icon :component="DeleteIcon" />
              </n-button>
            </div>
          </div>
        </div>

        <n-button dashed block class="mt-4" @click="addItem(keywordList)">
          <template #icon><n-icon :component="AddIcon" /></template>
          添加关键词映射
        </n-button>
      </n-tab-pane>

      <!-- ================= 工作室映射 Tab (改为手动输入) ================= -->
      <n-tab-pane name="studios" tab="工作室/平台映射">
        <n-alert type="info" :bordered="false" class="mb-4">
          配置电影的<b>出品公司</b>或电视剧的<b>播出平台</b>。手动填写英文原名或 TMDb ID。
        </n-alert>

        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文简称</div>
          <div class="col-en">英文原名 (逗号分隔)</div>
          <div class="col-ids">TMDb IDs (逗号分隔)</div>
          <div class="col-action">操作</div>
        </div>

        <div ref="studioListRef" class="sortable-list">
          <div v-for="(item, index) in studioList" :key="item.id" class="list-item" :data-id="item.id">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label">
              <n-input v-model:value="item.label" placeholder="例如：爱奇艺" />
            </div>
            
            <!-- 改为手动输入 -->
            <div class="col-en">
              <n-input v-model:value="item.en" placeholder="iQIYI" />
            </div>
            <div class="col-ids">
              <n-input v-model:value="item.ids" placeholder="例如：12345" />
            </div>
            
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(studioList, index)">
                <n-icon :component="DeleteIcon" />
              </n-button>
            </div>
          </div>
        </div>

        <n-button dashed block class="mt-4" @click="addItem(studioList)">
          <template #icon><n-icon :component="AddIcon" /></template>
          添加映射
        </n-button>
      </n-tab-pane>
    </n-tabs>

    <div class="footer-actions">
      <n-button ghost type="warning" @click="handleRestoreDefaults">
        <template #icon><n-icon :component="RefreshIcon" /></template>
        恢复默认预设
      </n-button>
      <n-button type="primary" :loading="isSaving" @click="handleSave">
        保存所有配置
      </n-button>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick, watch, onUnmounted } from 'vue';
import axios from 'axios';
import Sortable from 'sortablejs';
import { useMessage, useDialog } from 'naive-ui';
import { 
  AddOutline as AddIcon, 
  TrashOutline as DeleteIcon, 
  ReorderFourOutline as DragIcon,
  RefreshOutline as RefreshIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();

const activeTab = ref('keywords');

// 数据列表
const keywordList = ref([]);
const studioList = ref([]);
const isSaving = ref(false);

// 拖拽 DOM 引用
const keywordListRef = ref(null);
const studioListRef = ref(null);

let keywordSortableInstance = null;
let studioSortableInstance = null;

const generateId = () => '_' + Math.random().toString(36).substr(2, 9);

// 数据处理函数：后端 -> 前端
const processBackendData = (data) => {
  let list = [];
  if (Array.isArray(data)) {
    list = data;
  } else if (typeof data === 'object' && data !== null) {
    list = Object.entries(data).map(([label, info]) => ({ label, ...info }));
  }
  return list.map(item => ({
    id: generateId(),
    label: item.label,
    en: Array.isArray(item.en) ? item.en.join(', ') : (item.en || ''),
    ids: Array.isArray(item.ids) ? item.ids.join(', ') : (item.ids || '')
  }));
};

// 数据处理函数：前端 -> 后端
const processFrontendData = (list) => {
  return list.map(item => {
    if (!item.label || !item.label.trim()) return null;
    return {
      label: item.label.trim(),
      en: item.en.split(',').map(s => s.trim()).filter(s => s),
      ids: item.ids.toString().split(',').map(s => s.trim()).filter(s => s).map(Number)
    };
  }).filter(item => item !== null);
};

// 初始化 Sortable
const createSortable = (el, listRef, instanceVar) => {
  if (!el) return null;
  if (instanceVar) instanceVar.destroy();
  return Sortable.create(el, {
    handle: '.drag-handle',
    animation: 150,
    ghostClass: 'sortable-ghost',
    onEnd: (evt) => {
      const { oldIndex, newIndex } = evt;
      if (oldIndex === newIndex) return;
      const item = listRef.value.splice(oldIndex, 1)[0];
      listRef.value.splice(newIndex, 0, item);
    }
  });
};

watch(keywordListRef, (el) => {
  if (el) keywordSortableInstance = createSortable(el, keywordList, keywordSortableInstance);
});

watch(studioListRef, (el) => {
  if (el) studioSortableInstance = createSortable(el, studioList, studioSortableInstance);
});

// 初始化数据
const fetchData = async () => {
  try {
    const [kwRes, stRes] = await Promise.all([
      axios.get('/api/custom_collections/config/keyword_mapping'),
      axios.get('/api/custom_collections/config/studio_mapping')
    ]);
    keywordList.value = processBackendData(kwRes.data);
    studioList.value = processBackendData(stRes.data);
    
    nextTick(() => {
      if (keywordListRef.value) keywordSortableInstance = createSortable(keywordListRef.value, keywordList, keywordSortableInstance);
      if (studioListRef.value) studioSortableInstance = createSortable(studioListRef.value, studioList, studioSortableInstance);
    });
  } catch (e) {
    message.error('加载配置失败');
  }
};

const addItem = (list) => {
  list.push({ id: generateId(), label: '', en: '', ids: '' });
};

const removeItem = (list, index) => {
  list.splice(index, 1);
};

const handleRestoreDefaults = () => {
  dialog.warning({
    title: '恢复默认预设',
    content: '这将覆盖当前所有的自定义映射，确定要继续吗？',
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const [kwRes, stRes] = await Promise.all([
          axios.get('/api/custom_collections/config/keyword_mapping/defaults'),
          axios.get('/api/custom_collections/config/studio_mapping/defaults')
        ]);
        keywordList.value = processBackendData(kwRes.data);
        studioList.value = processBackendData(stRes.data);
        message.success('已加载默认预设，请点击保存以生效');
      } catch (e) {
        message.error('获取默认预设失败');
      }
    }
  });
};

const handleSave = async () => {
  isSaving.value = true;
  try {
    const kwPayload = processFrontendData(keywordList.value);
    const stPayload = processFrontendData(studioList.value);
    await Promise.all([
      axios.post('/api/custom_collections/config/keyword_mapping', kwPayload),
      axios.post('/api/custom_collections/config/studio_mapping', stPayload)
    ]);
    message.success('所有映射配置已保存');
    await fetchData();
  } catch (e) {
    message.error('保存失败');
  } finally {
    isSaving.value = false;
  }
};

onMounted(() => {
  fetchData();
});

onUnmounted(() => {
  if (keywordSortableInstance) keywordSortableInstance.destroy();
  if (studioSortableInstance) studioSortableInstance.destroy();
});
</script>

<style scoped>
.mapping-manager {
  padding: 0 4px;
}
.mb-4 { margin-bottom: 16px; }
.mt-4 { margin-top: 16px; }

.list-header {
  display: flex;
  gap: 12px;
  padding: 8px 12px;
  background: var(--n-color-modal);
  font-weight: bold;
  color: var(--n-text-color-3);
  border-bottom: 1px solid var(--n-border-color);
}

.list-item {
  display: flex;
  gap: 12px;
  padding: 8px 12px;
  align-items: center;
  background: var(--n-card-color);
  border-bottom: 1px solid var(--n-border-color);
  transition: background 0.2s;
}
.list-item:hover {
  background: rgba(var(--n-primary-color-rgb), 0.05);
}

.sortable-ghost {
  opacity: 0.5;
  background: var(--n-primary-color);
}

.sortable-list {
  max-height: 60vh;
  overflow-y: auto;
  padding-right: 8px;
  display: flex;
  flex-direction: column;
}
.sortable-list::-webkit-scrollbar {
  width: 6px;
}
.sortable-list::-webkit-scrollbar-thumb {
  background-color: rgba(255, 255, 255, 0.2);
  border-radius: 3px;
}
.sortable-list::-webkit-scrollbar-track {
  background-color: rgba(0, 0, 0, 0.1);
}

/* 统一列宽 */
.col-handle { width: 30px; display: flex; align-items: center; cursor: grab; color: var(--n-text-color-3); }
.col-handle:active { cursor: grabbing; }
.col-label { width: 160px; } /* 稍微加宽一点 */
.col-en { flex: 2; }
.col-ids { flex: 1; }
.col-action { width: 40px; display: flex; justify-content: center; }

.footer-actions {
  margin-top: 24px;
  display: flex;
  justify-content: space-between;
  padding-top: 16px;
  border-top: 1px solid var(--n-border-color);
}
</style>