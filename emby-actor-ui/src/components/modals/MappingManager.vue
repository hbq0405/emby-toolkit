<!-- src/components/modals/MappingManager.vue -->
<template>
  <div class="mapping-manager">
    <n-tabs type="segment" animated v-model:value="activeTab">
      
      <!-- Tab 1: 关键词 -->
      <n-tab-pane name="keywords" tab="关键词">
        <n-alert type="info" :bordered="false" class="mb-4">
          将中文标签映射到 TMDb 的英文关键词或 ID。
        </n-alert>
        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文标签</div>
          <div class="col-en">英文关键词</div>
          <div class="col-ids">TMDb IDs</div>
          <div class="col-action">操作</div>
        </div>
        <div ref="keywordListRef" class="sortable-list">
          <div v-for="(item, index) in keywordList" :key="item.id" class="list-item">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label"><n-input v-model:value="item.label" placeholder="例如：丧尸" /></div>
            <div class="col-en"><n-input v-model:value="item.en" placeholder="zombie" /></div>
            <div class="col-ids"><n-input v-model:value="item.ids" placeholder="12377" /></div>
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(keywordList, index)"><n-icon :component="DeleteIcon" /></n-button>
            </div>
          </div>
        </div>
        <n-button dashed block class="mt-4" @click="addItem(keywordList)">添加关键词</n-button>
      </n-tab-pane>

      <!-- Tab 2: 工作室 -->
      <n-tab-pane name="studios" tab="工作室">
        <n-alert type="info" :bordered="false" class="mb-4">
          配置出品公司或播出平台。
        </n-alert>
        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文简称</div>
          <div class="col-en">英文原名</div>
          <div class="col-ids">TMDb IDs</div>
          <div class="col-action">操作</div>
        </div>
        <div ref="studioListRef" class="sortable-list">
          <div v-for="(item, index) in studioList" :key="item.id" class="list-item">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label"><n-input v-model:value="item.label" placeholder="例如：爱奇艺" /></div>
            <div class="col-en"><n-input v-model:value="item.en" placeholder="iQIYI" /></div>
            <div class="col-ids"><n-input v-model:value="item.ids" placeholder="12345" /></div>
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(studioList, index)"><n-icon :component="DeleteIcon" /></n-button>
            </div>
          </div>
        </div>
        <n-button dashed block class="mt-4" @click="addItem(studioList)">添加工作室</n-button>
      </n-tab-pane>

      <!-- Tab 3: 国家/地区 -->
      <n-tab-pane name="countries" tab="国家/地区">
        <n-alert type="info" :bordered="false" class="mb-4">
          配置国家/地区代码映射。<b>ISO代码</b>用于API筛选，<b>别名</b>用于元数据翻译匹配。
        </n-alert>
        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文名称</div>
          <div class="col-en">ISO 代码 (如 US)</div>
          <div class="col-extra">英文别名 (逗号分隔)</div>
          <div class="col-action">操作</div>
        </div>
        <div ref="countryListRef" class="sortable-list">
          <div v-for="(item, index) in countryList" :key="item.id" class="list-item">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label"><n-input v-model:value="item.label" placeholder="例如：美国" /></div>
            <div class="col-en"><n-input v-model:value="item.value" placeholder="US" /></div>
            <div class="col-extra"><n-input v-model:value="item.aliases" placeholder="USA, United States" /></div>
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(countryList, index)"><n-icon :component="DeleteIcon" /></n-button>
            </div>
          </div>
        </div>
        <n-button dashed block class="mt-4" @click="addItem(countryList, 'country')">添加国家/地区</n-button>
      </n-tab-pane>

      <!-- Tab 4: 原语言 -->
      <n-tab-pane name="languages" tab="原语言">
        <n-alert type="info" :bordered="false" class="mb-4">
          配置语言代码映射。用于筛选器的语言选项。
        </n-alert>
        <div class="list-header">
          <div class="col-handle"></div>
          <div class="col-label">中文名称</div>
          <div class="col-en">ISO 代码 (如 en)</div>
          <div class="col-empty"></div>
          <div class="col-action">操作</div>
        </div>
        <div ref="languageListRef" class="sortable-list">
          <div v-for="(item, index) in languageList" :key="item.id" class="list-item">
            <div class="col-handle drag-handle"><n-icon :component="DragIcon" /></div>
            <div class="col-label"><n-input v-model:value="item.label" placeholder="例如：英语" /></div>
            <div class="col-en"><n-input v-model:value="item.value" placeholder="en" /></div>
            <div class="col-empty"></div>
            <div class="col-action">
              <n-button circle text type="error" @click="removeItem(languageList, index)"><n-icon :component="DeleteIcon" /></n-button>
            </div>
          </div>
        </div>
        <n-button dashed block class="mt-4" @click="addItem(languageList, 'language')">添加语言</n-button>
      </n-tab-pane>

    </n-tabs>

    <div class="footer-actions">
      <n-button ghost type="warning" @click="handleRestoreDefaults">
        <template #icon><n-icon :component="RefreshIcon" /></template>
        恢复当前页默认
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
const countryList = ref([]);
const languageList = ref([]);
const isSaving = ref(false);

// 拖拽 DOM 引用
const keywordListRef = ref(null);
const studioListRef = ref(null);
const countryListRef = ref(null);
const languageListRef = ref(null);

let sortables = [];

const generateId = () => '_' + Math.random().toString(36).substr(2, 9);

// 通用数据处理：后端 -> 前端
const processBackendData = (data, type) => {
  let list = Array.isArray(data) ? data : [];
  return list.map(item => {
    const base = { id: generateId(), label: item.label || '' };
    
    if (type === 'country') {
      base.value = item.value || '';
      base.aliases = Array.isArray(item.aliases) ? item.aliases.join(', ') : (item.aliases || '');
    } else if (type === 'language') {
      base.value = item.value || '';
    } else {
      base.en = Array.isArray(item.en) ? item.en.join(', ') : (item.en || '');
      base.ids = Array.isArray(item.ids) ? item.ids.join(', ') : (item.ids || '');
    }
    return base;
  });
};

// 通用数据处理：前端 -> 后端
const processFrontendData = (list, type) => {
  return list.map(item => {
    if (!item.label || !item.label.trim()) return null;
    const base = { label: item.label.trim() };

    if (type === 'country') {
      base.value = item.value ? item.value.trim() : '';
      base.aliases = item.aliases ? item.aliases.split(',').map(s => s.trim()).filter(s => s) : [];
    } else if (type === 'language') {
      base.value = item.value ? item.value.trim() : '';
    } else {
      base.en = item.en ? item.en.split(',').map(s => s.trim()).filter(s => s) : [];
      base.ids = item.ids ? item.ids.toString().split(',').map(s => s.trim()).filter(s => s).map(Number) : [];
    }
    return base;
  }).filter(item => item !== null);
};

// 初始化 Sortable
const initSortable = (el, listRef) => {
  if (!el) return null;
  const s = Sortable.create(el, {
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
  sortables.push(s);
  return s;
};

// 监听 DOM 变化初始化拖拽
const setupSortables = () => {
  // 销毁旧实例
  sortables.forEach(s => s?.destroy());
  sortables = [];
  
  nextTick(() => {
    // 尝试初始化所有存在的列表 Ref
    // 注意：在 Naive UI 中，只有当前激活的 Tab Ref 才有值，其他的通常为 null
    if (keywordListRef.value) initSortable(keywordListRef.value, keywordList);
    if (studioListRef.value) initSortable(studioListRef.value, studioList);
    if (countryListRef.value) initSortable(countryListRef.value, countryList);
    if (languageListRef.value) initSortable(languageListRef.value, languageList);
  });
};

// ★★★ 核心修复：监听 Tab 切换，重新绑定拖拽 ★★★
watch(activeTab, () => {
  setupSortables();
});

// 初始化数据
const fetchData = async () => {
  try {
    const [kwRes, stRes, cnRes, lgRes] = await Promise.all([
      axios.get('/api/custom_collections/config/keyword_mapping'),
      axios.get('/api/custom_collections/config/studio_mapping'),
      axios.get('/api/custom_collections/config/country_mapping'),
      axios.get('/api/custom_collections/config/language_mapping')
    ]);
    keywordList.value = processBackendData(kwRes.data, 'keyword');
    studioList.value = processBackendData(stRes.data, 'studio');
    countryList.value = processBackendData(cnRes.data, 'country');
    languageList.value = processBackendData(lgRes.data, 'language');
    
    setupSortables();
  } catch (e) {
    message.error('加载配置失败');
  }
};

const addItem = (list, type = 'normal') => {
  const item = { id: generateId(), label: '' };
  if (type === 'country') {
    item.value = ''; item.aliases = '';
  } else if (type === 'language') {
    item.value = '';
  } else {
    item.en = ''; item.ids = '';
  }
  list.push(item);
};

const removeItem = (list, index) => {
  list.splice(index, 1);
};

const handleRestoreDefaults = () => {
  const typeMap = {
    'keywords': { url: 'keyword_mapping', list: keywordList, type: 'keyword' },
    'studios': { url: 'studio_mapping', list: studioList, type: 'studio' },
    'countries': { url: 'country_mapping', list: countryList, type: 'country' },
    'languages': { url: 'language_mapping', list: languageList, type: 'language' }
  };
  
  const current = typeMap[activeTab.value];
  
  dialog.warning({
    title: '恢复默认预设',
    content: `确定要恢复【${activeTab.value}】的默认预设吗？当前未保存的修改将丢失。`,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const res = await axios.get(`/api/custom_collections/config/${current.url}/defaults`);
        current.list.value = processBackendData(res.data, current.type);
        message.success('已加载默认预设，请点击保存以生效');
        // 恢复默认后也要重新绑定拖拽，因为 DOM 可能会重绘
        setupSortables();
      } catch (e) {
        message.error('获取默认预设失败');
      }
    }
  });
};

const handleSave = async () => {
  isSaving.value = true;
  try {
    await Promise.all([
      axios.post('/api/custom_collections/config/keyword_mapping', processFrontendData(keywordList.value, 'keyword')),
      axios.post('/api/custom_collections/config/studio_mapping', processFrontendData(studioList.value, 'studio')),
      axios.post('/api/custom_collections/config/country_mapping', processFrontendData(countryList.value, 'country')),
      axios.post('/api/custom_collections/config/language_mapping', processFrontendData(languageList.value, 'language'))
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
  sortables.forEach(s => s?.destroy());
});
</script>

<style scoped>
.mapping-manager { padding: 0 4px; }
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
.list-item:hover { background: rgba(var(--n-primary-color-rgb), 0.05); }

.sortable-ghost { opacity: 0.5; background: var(--n-primary-color); }

.sortable-list {
  max-height: 60vh;
  overflow-y: auto;
  padding-right: 8px;
  display: flex;
  flex-direction: column;
}
.sortable-list::-webkit-scrollbar { width: 6px; }
.sortable-list::-webkit-scrollbar-thumb { background-color: rgba(255, 255, 255, 0.2); border-radius: 3px; }
.sortable-list::-webkit-scrollbar-track { background-color: rgba(0, 0, 0, 0.1); }

/* 列宽定义 */
.col-handle { width: 30px; display: flex; align-items: center; cursor: grab; color: var(--n-text-color-3); }
.col-handle:active { cursor: grabbing; }
.col-label { width: 140px; }
.col-en { flex: 2; }
.col-ids { flex: 1; }
.col-extra { flex: 2; } /* 用于国家别名 */
.col-empty { flex: 2; } /* 用于语言占位 */
.col-action { width: 40px; display: flex; justify-content: center; }

.footer-actions {
  margin-top: 24px;
  display: flex;
  justify-content: space-between;
  padding-top: 16px;
  border-top: 1px solid var(--n-border-color);
}
</style>