<!-- src/components/settings/RuleManagerModal.vue -->
<template>
  <!-- 规则管理列表模态框 -->
  <n-modal 
    v-model:show="showRuleManagerModal" 
    preset="card" 
    title="115 分类规则管理" 
    style="width: 800px; max-width: 95%; height: 80vh;"
    content-style="display: flex; flex-direction: column; overflow: hidden;" 
  >
    <template #header-extra>
      <n-space align="center">
        <n-radio-group v-model:value="ruleFilterType" size="small">
          <n-radio-button value="all">全部</n-radio-button>
          <n-radio-button value="movie">电影</n-radio-button>
          <n-radio-button value="tv">剧集</n-radio-button>
          <n-radio-button value="mixed">混合</n-radio-button>
        </n-radio-group>
        <n-divider vertical />
        <n-tag v-if="ruleFilterType === 'all'" type="warning" size="small" :bordered="false">拖拽可调整优先级</n-tag>
      </n-space>
    </template>
    
    <div style="display: flex; flex-direction: column; flex: 1; min-height: 0;">
      <div style="flex: 1; overflow-y: auto; padding-right: 4px; margin-bottom: 16px;">
        <div class="rules-container">
          <!-- 拖拽列表 (仅全部模式) -->
          <draggable 
            v-if="ruleFilterType === 'all'"
            v-model="sortingRules" 
            item-key="id" 
            handle=".drag-handle" 
            @end="saveSortingRules"
            :animation="200"
          >
            <template #item="{ element: rule }">
              <div class="rule-item">
                <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
                <div class="rule-info">
                  <div style="display: flex; align-items: center; gap: 8px;">
                    <div class="rule-name">{{ rule.name }}</div>
                    <n-tag v-if="!rule.enabled" size="tiny" type="error" :bordered="false">已禁用</n-tag>
                    <n-tag v-if="rule.media_type === 'movie'" size="tiny" type="info" :bordered="false">电影</n-tag>
                    <n-tag v-else-if="rule.media_type === 'tv'" size="tiny" type="success" :bordered="false">剧集</n-tag>
                    <n-tag v-else size="tiny" :bordered="false">混合</n-tag>
                    <n-tag v-if="rule.watching_status === 'watching'" size="tiny" type="warning" :bordered="false">追剧中</n-tag>
                  </div>
                  <div class="rule-desc">
                      <n-tag size="tiny" :bordered="false" type="warning" style="opacity: 0.8;">目录: {{ rule.dir_name }}</n-tag>
                      <span style="margin-left: 8px; font-size: 12px; opacity: 0.7;">{{ getRuleSummary(rule) }}</span>
                  </div>
                </div>
                <div class="rule-actions">
                  <n-switch v-model:value="rule.enabled" size="small" @update:value="saveSortingRules" />
                  <n-divider vertical />
                  <n-button text size="medium" @click="editRule(rule)"><n-icon :component="EditIcon" color="#18a058" /></n-button>
                  <n-button text size="medium" @click="deleteRule(rule)"><n-icon :component="DeleteIcon" color="#d03050" /></n-button>
                </div>
              </div>
            </template>
          </draggable>

          <!-- 普通列表 (筛选模式) -->
          <div v-else>
            <div v-for="rule in filteredSortingRules" :key="rule.id" class="rule-item">
              <div style="width: 24px; margin-right: 12px;"></div> 
              <div class="rule-info">
                <div style="display: flex; align-items: center; gap: 8px;">
                  <div class="rule-name">{{ rule.name }}</div>
                  <n-tag v-if="!rule.enabled" size="tiny" type="error" :bordered="false">已禁用</n-tag>
                  <n-tag v-if="rule.media_type === 'movie'" size="tiny" type="info" :bordered="false">电影</n-tag>
                  <n-tag v-else-if="rule.media_type === 'tv'" size="tiny" type="success" :bordered="false">剧集</n-tag>
                  <n-tag v-else size="tiny" :bordered="false">混合</n-tag>
                  <n-tag v-if="rule.watching_status === 'watching'" size="tiny" type="warning" :bordered="false">追剧中</n-tag>
                </div>
                <div class="rule-desc">
                    <n-tag size="tiny" :bordered="false" type="warning" style="opacity: 0.8;">目录: {{ rule.dir_name }}</n-tag>
                    <span style="margin-left: 8px; font-size: 12px; opacity: 0.7;">{{ getRuleSummary(rule) }}</span>
                </div>
              </div>
              <div class="rule-actions">
                <n-switch v-model:value="rule.enabled" size="small" @update:value="saveSortingRules" />
                <n-divider vertical />
                <n-button text size="medium" @click="editRule(rule)"><n-icon :component="EditIcon" color="#18a058" /></n-button>
                <n-button text size="medium" @click="deleteRule(rule)"><n-icon :component="DeleteIcon" color="#d03050" /></n-button>
              </div>
            </div>
          </div>
          <n-empty v-if="filteredSortingRules.length === 0" description="暂无规则" style="margin: 40px 0;" />
        </div>
      </div>
      <div style="border-top: 1px solid var(--n-divider-color); padding-top: 16px; flex-shrink: 0;">
        <n-button type="primary" dashed block @click="addRule">
          <template #icon><n-icon :component="AddIcon" /></template>
          添加新规则
        </n-button>
      </div>
    </div>
  </n-modal>

  <!-- 规则编辑模态框 -->
  <n-modal v-model:show="showRuleModal" preset="card" title="编辑分类规则" style="width: 650px;">
    <n-form label-placement="left" label-width="100">
      <n-form-item label="规则名称">
        <n-input v-model:value="currentRule.name" placeholder="例如：漫威电影宇宙" />
      </n-form-item>
      <n-form-item label="目标目录">
        <n-input-group>
          <n-input 
            :value="currentRule.dir_name || currentRule.cid" 
            readonly 
            placeholder="点击选择目录" 
            @click="$emit('open-folder-selector', 'rule', currentRule.cid)"
          >
            <template #prefix><n-icon :component="FolderIcon" color="#f0a020" /></template>
          </n-input>
          <n-button type="primary" ghost @click="$emit('open-folder-selector', 'rule', currentRule.cid)">
            选择
          </n-button>
        </n-input-group>
      </n-form-item>
      
      <n-divider title-placement="left" style="font-size: 12px; color: #999;">匹配条件 (满足所有勾选条件时命中)</n-divider>
      
      <n-form-item label="媒体类型">
        <n-radio-group v-model:value="currentRule.media_type">
          <n-radio-button value="all">不限</n-radio-button>
          <n-radio-button value="movie">仅电影</n-radio-button>
          <n-radio-button value="tv">仅剧集</n-radio-button>
        </n-radio-group>
      </n-form-item>

      <n-form-item label="追剧状态 (仅对剧集有效)">
        <n-radio-group v-model:value="currentRule.watching_status">
          <n-radio-button value="all">不限 (常规分类)</n-radio-button>
          <n-radio-button value="watching">仅限 追剧中</n-radio-button>
        </n-radio-group>
        <template #feedback>
          <n-text depth="3" style="font-size: 12px;">
            💡 <b>高阶玩法：</b>请将“追剧中”的规则拖拽到常规规则的<b>下方</b>，避免完结剧进入追剧目录！
          </n-text>
        </template>
      </n-form-item>

      <n-form-item label="包含演员">
        <n-select
          v-model:value="currentRule.actors"
          multiple filterable remote
          placeholder="输入以搜索并添加演员"
          :options="actorOptions"
          :loading="isSearchingActors"
          @search="handlePersonSearch"
          label-field="name"
          value-field="id"
          value-type="object"
          :render-option="renderPersonOption"
          :render-tag="renderPersonTag"
        />
      </n-form-item>

      <n-form-item label="类型/风格">
        <n-select v-model:value="currentRule.genres" multiple filterable :options="computedGenreOptions" placeholder="包含任一类型即可" />
      </n-form-item>
      
      <n-form-item label="国家/地区">
        <n-select v-model:value="currentRule.countries" multiple filterable :options="countryOptions" placeholder="包含任一国家即可" />
      </n-form-item>

      <n-form-item label="原始语言">
        <n-select v-model:value="currentRule.languages" multiple filterable :options="languageOptions" placeholder="包含任一语言即可" />
      </n-form-item>

      <n-form-item label="工作室">
        <n-select v-model:value="currentRule.studios" multiple filterable :options="computedStudioOptions" placeholder="包含任一工作室即可" />
      </n-form-item>

      <n-form-item label="关键词">
         <n-select v-model:value="currentRule.keywords" multiple filterable tag :options="keywordOptions" placeholder="包含任一关键词即可" />
      </n-form-item>

      <n-form-item label="分级">
         <n-select v-model:value="currentRule.ratings" multiple filterable :options="ratingOptions" placeholder="包含任一分级即可" />
      </n-form-item>

      <n-form-item label="年份范围">
        <n-input-group>
          <n-input-number v-model:value="currentRule.year_min" :min="1900" :max="2099" placeholder="起始" :show-button="false" style="width: 50%" />
          <n-input-group-label style="border-left: 0; border-right: 0;">至</n-input-group-label>
          <n-input-number v-model:value="currentRule.year_max" :min="1900" :max="2099" placeholder="结束" :show-button="false" style="width: 50%" />
        </n-input-group>
      </n-form-item>

      <n-form-item label="时长 (分钟)">
        <n-input-group>
          <n-input-number v-model:value="currentRule.runtime_min" :min="0" placeholder="0" :show-button="false" style="width: 50%" />
          <n-input-group-label style="border-left: 0; border-right: 0;">至</n-input-group-label>
          <n-input-number v-model:value="currentRule.runtime_max" :min="0" placeholder="∞" :show-button="false" style="width: 50%" />
        </n-input-group>
      </n-form-item>

      <n-form-item label="最低评分">
        <n-input-number v-model:value="currentRule.min_rating" :min="0" :max="10" :step="0.1" placeholder="0" style="width: 100%">
          <template #suffix>分</template>
        </n-input-number>
      </n-form-item>

    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button @click="showRuleModal = false">取消</n-button>
        <n-button type="primary" @click="confirmSaveRule">保存</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, computed, h } from 'vue';
import axios from 'axios';
import draggable from 'vuedraggable';
import { 
  NModal, NSpace, NRadioGroup, NRadioButton, NDivider, NTag, NButton, NIcon, NEmpty, 
  NForm, NFormItem, NInput, NInputGroup, NSelect, NInputNumber, useMessage, NText, NAvatar
} from 'naive-ui';
import { 
  Menu as DragHandleIcon, CreateOutline as EditIcon, TrashOutline as DeleteIcon, 
  AddOutline as AddIcon, Folder as FolderIcon 
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['open-folder-selector']);

const showRuleManagerModal = ref(false);
const showRuleModal = ref(false);
const sortingRules = ref([]);
const ruleFilterType = ref('all');
const currentRule = ref({});

// 选项数据
const rawMovieGenres = ref([]); 
const rawTvGenres = ref([]);    
const rawStudios = ref([]);     
const countryOptions = ref([]); 
const languageOptions = ref([]);
const keywordOptions = ref([]);
const ratingOptions = ref([]);

// 演员搜索相关
const actorOptions = ref([]);
const isSearchingActors = ref(false);
let personSearchTimeout = null;

const handlePersonSearch = (query) => {
  if (!query) return;
  isSearchingActors.value = true;
  clearTimeout(personSearchTimeout);
  personSearchTimeout = setTimeout(async () => {
    try {
      const res = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      actorOptions.value = res.data || [];
    } finally {
      isSearchingActors.value = false;
    }
  }, 300);
};

const renderPersonOption = ({ node, option }) => {
  node.children = [
    h(NAvatar, { src: option.profile_path ? `https://image.tmdb.org/t/p/w92${option.profile_path}` : '', size: 'small', style: 'margin-right: 8px;', round: true }),
    h('div', { style: 'display: flex; flex-direction: column;' }, [
      h(NText, null, { default: () => option.name }),
      h(NText, { depth: 3, style: 'font-size: 12px;' }, { default: () => `代表作: ${option.known_for || '暂无'}` })
    ])
  ];
  return node;
};

const renderPersonTag = ({ option, handleClose }) => {
  return h(NTag, { type: 'info', closable: true, onClose: (e) => { e.stopPropagation(); handleClose(); }, style: { display: 'flex', alignItems: 'center', padding: '0 6px 0 2px', height: '24px' }, round: true }, {
    default: () => [
      h(NAvatar, { src: option.profile_path ? `https://image.tmdb.org/t/p/w92${option.profile_path}` : '', size: 'small', style: 'margin-right: 5px;', round: true }),
      option.name 
    ]
  });
};

// 暴露给父组件
defineExpose({
  open: async () => {
    showRuleManagerModal.value = true;
    // 模态框显示后再加载数据，避免打开慢
    await loadOptions();
    await loadSortingRules();
  },
  updateFolder: (cid, name) => {
    currentRule.value.cid = cid;
    currentRule.value.dir_name = name;
  }
});

const loadOptions = async () => {
  try {
    const [mGenres, tGenres, sRes, cRes, lRes, kRes, rRes] = await Promise.all([
      axios.get('/api/custom_collections/config/tmdb_movie_genres'),
      axios.get('/api/custom_collections/config/tmdb_tv_genres'),
      axios.get('/api/custom_collections/config/studios'),
      axios.get('/api/custom_collections/config/tmdb_countries'),
      axios.get('/api/custom_collections/config/languages'),
      axios.get('/api/custom_collections/config/keywords'),
      axios.get('/api/custom_collections/config/unified_ratings_options')
    ]);
    rawMovieGenres.value = (mGenres.data || []).map(g => ({ label: g.name, value: g.id }));
    rawTvGenres.value = (tGenres.data || []).map(g => ({ label: g.name, value: g.id }));
    rawStudios.value = (sRes.data || []).map(s => ({ label: s.label, value: s.value, is_movie: (s.types||[]).includes('movie'), is_tv: (s.types||[]).includes('tv') }));
    countryOptions.value = cRes.data;
    languageOptions.value = lRes.data;
    keywordOptions.value = kRes.data;
    ratingOptions.value = (rRes.data || []).map(r => ({ label: r, value: r }));
  } catch (e) { console.error("加载选项失败", e); }
};

const loadSortingRules = async () => {
  try {
    const res = await axios.get('/api/p115/sorting_rules');
    let data = res.data;
    if (typeof data === 'string') try { data = JSON.parse(data); } catch(e) {}
    sortingRules.value = Array.isArray(data) ? data : [];
  } catch (e) { sortingRules.value = []; }
};

const saveSortingRules = async () => {
  try {
    await axios.post('/api/p115/sorting_rules', sortingRules.value);
  } catch (e) { message.error('保存规则失败'); }
};

const addRule = () => {
  currentRule.value = { 
    id: Date.now(), name: '', cid: '', enabled: true, 
    media_type: 'all', genres: [], countries: [], languages: [], 
    studios: [], keywords: [], ratings: [], actors: [], watching_status: 'all',
    year_min: null, year_max: null, runtime_min: null, runtime_max: null, min_rating: 0
  };
  actorOptions.value = [];
  showRuleModal.value = true;
};

const editRule = (rule) => {
  currentRule.value = JSON.parse(JSON.stringify(rule));
  if (!currentRule.value.watching_status) currentRule.value.watching_status = 'all';
  if (!currentRule.value.actors) currentRule.value.actors = [];
  actorOptions.value = currentRule.value.actors; // 回显已选演员
  showRuleModal.value = true;
};

const deleteRule = (rule) => {
  sortingRules.value = sortingRules.value.filter(r => r.id !== rule.id);
  saveSortingRules();
};

const confirmSaveRule = () => {
  if (!currentRule.value.name || !currentRule.value.cid) {
    message.error('名称和 CID 必填');
    return;
  }
  const idx = sortingRules.value.findIndex(r => r.id === currentRule.value.id);
  if (idx > -1) sortingRules.value[idx] = currentRule.value;
  else sortingRules.value.push(currentRule.value);
  
  saveSortingRules();
  showRuleModal.value = false;
};

const filteredSortingRules = computed(() => {
  if (ruleFilterType.value === 'all') return sortingRules.value;
  return sortingRules.value.filter(rule => {
    if (ruleFilterType.value === 'movie') return rule.media_type === 'movie';
    if (ruleFilterType.value === 'tv') return rule.media_type === 'tv';
    if (ruleFilterType.value === 'mixed') return rule.media_type === 'all';
    return true;
  });
});

const computedGenreOptions = computed(() => {
  const type = currentRule.value.media_type;
  if (type === 'movie') return rawMovieGenres.value;
  else if (type === 'tv') return rawTvGenres.value;
  else {
    const map = new Map();
    [...rawMovieGenres.value, ...rawTvGenres.value].forEach(g => map.set(g.value, g));
    return Array.from(map.values());
  }
});

const computedStudioOptions = computed(() => {
  const type = currentRule.value.media_type;
  return rawStudios.value.filter(item => {
    if (type === 'all') return true;
    if (type === 'movie') return item.is_movie;
    if (type === 'tv') return item.is_tv;
    return true;
  });
});

const getRuleSummary = (rule) => {
  const parts = [];
  if (rule.actors?.length) parts.push(`演员:${rule.actors.map(a=>a.name).join(',')}`);
  if (rule.studios?.length) parts.push(`工作室:${rule.studios.join(',')}`);
  if (rule.keywords?.length) parts.push(`关键词:${rule.keywords.join(',')}`);
  if (rule.ratings?.length) parts.push(`分级:${rule.ratings.join(',')}`);
  if (rule.year_min || rule.year_max) parts.push(`年份`);
  return parts.join(' + ') || '无条件';
};
</script>

<style scoped>
.rules-container { background: transparent; border: none; padding: 0; }
.rule-item {
  display: flex; align-items: center; background-color: var(--n-action-color); 
  border: 1px solid var(--n-divider-color); padding: 12px; margin-bottom: 8px; border-radius: 6px; transition: all 0.2s;
}
.rule-item:hover { border-color: var(--n-primary-color); background-color: var(--n-hover-color); }
.drag-handle { cursor: grab; color: #999; margin-right: 12px; padding: 4px; }
.drag-handle:active { cursor: grabbing; }
.rule-info { flex: 1; }
.rule-name { font-weight: bold; font-size: 13px; color: var(--n-text-color-1); }
.rule-desc span { color: var(--n-text-color-3); }
.rule-actions { display: flex; align-items: center; gap: 4px; }
</style>
