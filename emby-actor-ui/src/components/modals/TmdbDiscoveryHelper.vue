<template>
  <n-modal
    :show="show"
    @update:show="(val) => emit('update:show', val)"
    preset="card"
    style="width: 90%; max-width: 700px;"
    title="TMDb 探索助手 ✨"
    :bordered="false"
    size="huge"
  >
    <n-space vertical :size="24">
      <!-- 1. 类型与排序 -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item label="类型">
            <n-radio-group v-model:value="params.type" style="width: 100%">
              <n-radio-button value="movie" style="width: 50%; text-align: center;">电影</n-radio-button>
              <n-radio-button value="tv" style="width: 50%; text-align: center;">电视剧</n-radio-button>
            </n-radio-group>
          </n-form-item>
        </n-gi>
        <n-gi>
          <n-form-item label="排序方式">
            <n-select v-model:value="params.sort_by" :options="sortOptions" />
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 2. 年份范围 -->
      <n-form-item label="发行/首播年份">
        <n-input-group>
          <n-input-number v-model:value="params.year_gte" placeholder="起始年份 (如 1990)" :show-button="false" style="width: 50%;" />
          <n-input-number v-model:value="params.year_lte" placeholder="结束年份 (如 2025)" :show-button="false" style="width: 50%;" />
        </n-input-group>
      </n-form-item>

      <!-- 3. 类型 (Genres) -->
      <n-form-item label="包含/排除类型">
        <n-grid :cols="2" :x-gap="12">
          <n-gi>
            <n-select
              v-model:value="params.with_genres"
              multiple filterable
              placeholder="包含类型"
              :options="currentGenreOptions"
              :loading="loading.genres"
            />
          </n-gi>
          <n-gi>
            <n-select
              v-model:value="params.without_genres"
              multiple filterable
              placeholder="排除类型"
              :options="currentGenreOptions"
              :loading="loading.genres"
            />
          </n-gi>
        </n-grid>
      </n-form-item>

      <!-- 4. ★★★ 映射集成：工作室与关键词 ★★★ -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item label="制作公司/工作室 (基于映射)">
            <n-select
              v-model:value="params.with_companies_labels"
              multiple filterable
              placeholder="选择已映射的工作室"
              :options="studioOptions"
              :loading="loading.mappings"
            />
            <template #feedback>
              <n-text depth="3" style="font-size: 12px;">
                选中“漫威”将自动转换为 ID 420。如需更多，请去“映射管理”添加。
              </n-text>
            </template>
          </n-form-item>
        </n-gi>
        <n-gi>
          <n-form-item label="关键词 (基于映射)">
            <n-select
              v-model:value="params.with_keywords_labels"
              multiple filterable
              placeholder="选择已映射的关键词"
              :options="keywordOptions"
              :loading="loading.mappings"
            />
            <template #feedback>
              <n-text depth="3" style="font-size: 12px;">
                选中“丧尸”将自动转换为 ID 12377。
              </n-text>
            </template>
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 5. 人员搜索 (保持远程搜索) -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item label="演员">
            <n-select
              v-model:value="params.with_cast"
              multiple filterable remote
              placeholder="搜演员 (如: 周星驰)"
              :options="actorOptions"
              :loading="loading.actors"
              @search="handleActorSearch"
              label-field="name"
              value-field="id"
            />
          </n-form-item>
        </n-gi>
        <n-gi>
          <n-form-item label="导演">
            <n-select
              v-model:value="params.with_crew"
              multiple filterable remote
              placeholder="搜导演 (如: 诺兰)"
              :options="directorOptions"
              :loading="loading.directors"
              @search="handleDirectorSearch"
              label-field="name"
              value-field="id"
            />
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 6. 地区与语言 -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item label="国家/地区">
            <n-select
              v-model:value="params.region"
              filterable clearable
              placeholder="出品国家"
              :options="countryOptions"
              :loading="loading.countries"
            />
          </n-form-item>
        </n-gi>
        <n-gi>
          <n-form-item label="原始语言">
            <n-select
              v-model:value="params.language"
              :options="languageOptions"
              filterable clearable
              placeholder="对白语言"
            />
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 7. 评分过滤 -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item :label="`最低评分: ${params.vote_average}`">
            <n-slider v-model:value="params.vote_average" :step="0.5" :min="0" :max="10" />
          </n-form-item>
        </n-gi>
        <n-gi>
          <n-form-item :label="`最少评价数: ${params.vote_count}`">
            <n-slider v-model:value="params.vote_count" :step="50" :min="0" :max="2000" />
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 8. 结果预览 -->
      <n-form-item label="生成的 URL (实时预览)">
        <n-input 
          :value="generatedUrl" 
          type="textarea" 
          :autosize="{ minRows: 2, maxRows: 4 }" 
          readonly 
          placeholder="配置参数后自动生成..."
        />
      </n-form-item>
    </n-space>

    <template #footer>
      <n-space justify="end">
        <n-button @click="emit('update:show', false)">取消</n-button>
        <n-button type="primary" @click="handleConfirm">
          <template #icon><n-icon :component="CheckIcon" /></template>
          使用此 URL
        </n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue';
import axios from 'axios';
import { CheckmarkCircleOutline as CheckIcon } from '@vicons/ionicons5';

const props = defineProps({
  show: Boolean
});

const emit = defineEmits(['update:show', 'confirm']);

// --- 状态定义 ---
const params = ref({
  type: 'movie',
  sort_by: 'popularity.desc',
  year_gte: null,
  year_lte: null,
  with_genres: [],
  without_genres: [],
  with_companies_labels: [], // 存中文 Label
  with_keywords_labels: [],  // 存中文 Label
  with_cast: [],             // 存 ID
  with_crew: [],             // 存 ID
  region: null,
  language: null,
  vote_average: 0,
  vote_count: 0
});

const loading = ref({
  genres: false,
  countries: false,
  mappings: false,
  actors: false,
  directors: false
});

// --- 选项数据 ---
const movieGenres = ref([]);
const tvGenres = ref([]);
const countryOptions = ref([]);
const actorOptions = ref([]);
const directorOptions = ref([]);

// 映射数据 (Label -> IDs)
const keywordMapping = ref({}); // { "丧尸": [12377], ... }
const studioMapping = ref({});  // { "漫威": [420], ... }

// 下拉框选项 (Label -> Label)
const keywordOptions = computed(() => Object.keys(keywordMapping.value).map(k => ({ label: k, value: k })));
const studioOptions = computed(() => Object.keys(studioMapping.value).map(k => ({ label: k, value: k })));

const currentGenreOptions = computed(() => {
  const list = params.value.type === 'movie' ? movieGenres.value : tvGenres.value;
  return list.map(g => ({ label: g.name, value: g.id }));
});

const sortOptions = computed(() => {
  const dateField = params.value.type === 'movie' ? 'primary_release_date' : 'first_air_date';
  return [
    { label: '热度降序', value: 'popularity.desc' },
    { label: '热度升序', value: 'popularity.asc' },
    { label: '评分降序', value: 'vote_average.desc' },
    { label: '评分升序', value: 'vote_average.asc' },
    { label: '日期降序', value: `${dateField}.desc` },
    { label: '日期升序', value: `${dateField}.asc` },
    { label: '票房/营收降序', value: 'revenue.desc' }
  ];
});

const languageOptions = [
  { label: '不限', value: null },
  { label: '英语 (en)', value: 'en' },
  { label: '中文 (zh)', value: 'zh' },
  { label: '日语 (ja)', value: 'ja' },
  { label: '韩语 (ko)', value: 'ko' },
  { label: '法语 (fr)', value: 'fr' }
];

// --- 数据获取 ---

// 1. 获取基础配置 (类型、国家)
const fetchBasicConfigs = async () => {
  loading.value.genres = true;
  loading.value.countries = true;
  try {
    const [mvRes, tvRes, cRes] = await Promise.all([
      axios.get('/api/custom_collections/config/tmdb_movie_genres'),
      axios.get('/api/custom_collections/config/tmdb_tv_genres'),
      axios.get('/api/custom_collections/config/tmdb_countries')
    ]);
    movieGenres.value = mvRes.data;
    tvGenres.value = tvRes.data;
    countryOptions.value = cRes.data;
  } finally {
    loading.value.genres = false;
    loading.value.countries = false;
  }
};

// 2. ★★★ 获取映射配置 (核心) ★★★
const fetchMappings = async () => {
  loading.value.mappings = true;
  try {
    // 调用获取完整映射字典的接口
    const [kwRes, stRes] = await Promise.all([
      axios.get('/api/custom_collections/config/keyword_mapping'),
      axios.get('/api/custom_collections/config/studio_mapping')
    ]);
    
    // 处理后端返回的数据 (可能是 List 或 Dict，统一转为 Dict: Label -> IDs)
    const process = (data) => {
      const map = {};
      // 兼容数组格式
      const list = Array.isArray(data) ? data : Object.entries(data).map(([k, v]) => ({ label: k, ...v }));
      
      list.forEach(item => {
        if (item.label && item.ids) {
          // 确保 ids 是数组
          const ids = Array.isArray(item.ids) ? item.ids : [item.ids];
          map[item.label] = ids;
        }
      });
      return map;
    };

    keywordMapping.value = process(kwRes.data);
    studioMapping.value = process(stRes.data);
  } finally {
    loading.value.mappings = false;
  }
};

// 3. 人员搜索
let searchTimer = null;
const searchPerson = (query, targetRef, loadingKey) => {
  if (!query) return;
  loading.value[loadingKey] = true;
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(async () => {
    try {
      const { data } = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      targetRef.value = data;
    } finally {
      loading.value[loadingKey] = false;
    }
  }, 500);
};
const handleActorSearch = (q) => searchPerson(q, actorOptions, 'actors');
const handleDirectorSearch = (q) => searchPerson(q, directorOptions, 'directors');

// --- URL 生成逻辑 ---
const generatedUrl = computed(() => {
  const p = params.value;
  const baseUrl = `https://www.themoviedb.org/discover/${p.type}`;
  const query = new URLSearchParams();

  query.append('sort_by', p.sort_by);

  // 日期
  if (p.type === 'movie') {
    if (p.year_gte) query.append('primary_release_date.gte', `${p.year_gte}-01-01`);
    if (p.year_lte) query.append('primary_release_date.lte', `${p.year_lte}-12-31`);
  } else {
    if (p.year_gte) query.append('first_air_date.gte', `${p.year_gte}-01-01`);
    if (p.year_lte) query.append('first_air_date.lte', `${p.year_lte}-12-31`);
  }

  // 类型
  if (p.with_genres.length) query.append('with_genres', p.with_genres.join(','));
  if (p.without_genres.length) query.append('without_genres', p.without_genres.join(','));

  // ★★★ 映射转换：关键词 Label -> IDs ★★★
  if (p.with_keywords_labels.length) {
    const ids = new Set();
    p.with_keywords_labels.forEach(label => {
      const mappedIds = keywordMapping.value[label];
      if (mappedIds) mappedIds.forEach(id => ids.add(id));
    });
    if (ids.size) query.append('with_keywords', Array.from(ids).join(',')); // OR 逻辑用逗号? TMDb API: comma=AND, pipe=OR. 
    // 通常 Discovery 想要的是 "包含这些关键词中的任意一个" 还是 "所有"? 
    // TMDb 网页版默认行为：逗号是 AND，管道符 | 是 OR。
    // 这里我们假设用户选多个关键词是想找交集 (AND)，或者我们可以做个开关。
    // 暂时使用逗号 (AND)，更精准。
  }

  // ★★★ 映射转换：工作室 Label -> IDs ★★★
  if (p.with_companies_labels.length) {
    const ids = new Set();
    p.with_companies_labels.forEach(label => {
      const mappedIds = studioMapping.value[label];
      if (mappedIds) mappedIds.forEach(id => ids.add(id));
    });
    if (ids.size) query.append('with_companies', Array.from(ids).join('|')); // 公司通常用 OR (比如 漫威 OR 迪士尼)
  }

  // 人员
  if (p.with_cast.length) query.append('with_cast', p.with_cast.join(','));
  if (p.with_crew.length) query.append('with_crew', p.with_crew.join(','));

  // 其他
  if (p.region) query.append('with_origin_country', p.region);
  if (p.language) query.append('with_original_language', p.language);
  if (p.vote_average > 0) query.append('vote_average.gte', p.vote_average);
  if (p.vote_count > 0) query.append('vote_count.gte', p.vote_count);

  return `${baseUrl}?${query.toString()}`;
});

// --- 生命周期 ---
watch(() => props.show, (val) => {
  if (val) {
    // 每次打开时刷新映射，确保是最新的
    fetchMappings();
    // 如果还没加载过基础配置，加载一次
    if (movieGenres.value.length === 0) fetchBasicConfigs();
  }
});

const handleConfirm = () => {
  emit('confirm', generatedUrl.value, params.value.type); // 传回 URL 和类型
  emit('update:show', false);
};
</script>