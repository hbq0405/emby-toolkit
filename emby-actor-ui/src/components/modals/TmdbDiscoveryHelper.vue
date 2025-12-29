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

      <!-- 2. ★★★ 新增：即将上线 (新剧雷达) ★★★ -->
      <n-form-item>
        <template #label>
          <n-space align="center">
            <span>📅 即将上线 (未来 N 天)</span>
            <n-tag type="success" size="small" round v-if="params.next_days > 0">已启用</n-tag>
          </n-space>
        </template>
        <n-grid :cols="4" :x-gap="12">
          <n-gi :span="3">
            <n-slider v-model:value="params.next_days" :min="0" :max="90" :step="1" />
          </n-gi>
          <n-gi :span="1">
            <n-input-number v-model:value="params.next_days" size="small" placeholder="0 = 禁用" :min="0" />
          </n-gi>
        </n-grid>
        <template #feedback>
          <n-text depth="3" style="font-size: 12px;">
            设置后将忽略下方的年份筛选。例如设置 7 天，将筛选从今天开始一周内首播的内容。
          </n-text>
        </template>
      </n-form-item>

      <!-- 3. 年份范围 (当启用即将上线时禁用) -->
      <n-form-item label="发行/首播年份" :disabled="params.next_days > 0">
        <n-input-group>
          <n-input-number 
            v-model:value="params.year_gte" 
            placeholder="起始年份 (如 1990)" 
            :show-button="false" 
            style="width: 50%;" 
            :disabled="params.next_days > 0"
          />
          <n-input-number 
            v-model:value="params.year_lte" 
            placeholder="结束年份 (如 2025)" 
            :show-button="false" 
            style="width: 50%;" 
            :disabled="params.next_days > 0"
          />
        </n-input-group>
      </n-form-item>

      <!-- 4. 类型 (Genres) -->
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

      <!-- 5. ★★★ 映射集成：工作室/平台 与 关键词 ★★★ -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <!-- 动态 Label -->
          <n-form-item :label="params.type === 'tv' ? '播出平台/电视网 (Networks)' : '制作公司 (Companies)'">
            <n-select
              v-model:value="params.with_companies_labels"
              multiple filterable
              :placeholder="params.type === 'tv' ? '选择 Netflix, HBO 等' : '选择 漫威, A24 等'"
              :options="studioOptions"
              :loading="loading.mappings"
            />
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
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 6. 人员搜索 -->
      <n-grid :cols="2" :x-gap="12">
        <n-gi>
          <n-form-item label="演员">
            <n-select
              v-model:value="params.with_cast"
              multiple filterable remote
              placeholder="搜演员"
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
              placeholder="搜导演"
              :options="directorOptions"
              :loading="loading.directors"
              @search="handleDirectorSearch"
              label-field="name"
              value-field="id"
            />
          </n-form-item>
        </n-gi>
      </n-grid>

      <!-- 7. 地区与语言 -->
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

      <!-- 8. 评分过滤 -->
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

      <!-- 9. 结果预览 -->
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
import { ref, computed, watch } from 'vue';
import axios from 'axios';
import { CheckmarkCircleOutline as CheckIcon } from '@vicons/ionicons5';

const props = defineProps({
  show: Boolean
});

const emit = defineEmits(['update:show', 'confirm']);

// --- 状态定义 ---
const params = ref({
  type: 'tv', // 默认改成 TV 方便测试
  sort_by: 'popularity.desc',
  year_gte: null,
  year_lte: null,
  next_days: 0, // ★★★ 新增：未来多少天 ★★★
  with_genres: [],
  without_genres: [],
  with_companies_labels: [], 
  with_keywords_labels: [],  
  with_cast: [],             
  with_crew: [],             
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
const keywordMapping = ref({}); 
const studioMapping = ref({});  

// 下拉框选项
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

// --- 辅助函数：格式化日期 YYYY-MM-DD ---
const formatDate = (date) => {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

// --- URL 生成逻辑 ---
const generatedUrl = computed(() => {
  const p = params.value;
  const baseUrl = `https://www.themoviedb.org/discover/${p.type}`;
  const query = new URLSearchParams();

  query.append('sort_by', p.sort_by);

  // ★★★ 日期逻辑：优先处理“未来 N 天” ★★★
  const dateField = p.type === 'movie' ? 'primary_release_date' : 'first_air_date';
  
  if (p.next_days > 0) {
    // 计算未来日期范围
    const today = new Date();
    
    // ★★★ 修改：起始日期设为明天 (Today + 1) ★★★
    const startDate = new Date(today);
    startDate.setDate(today.getDate() + 1);
    
    // 结束日期设为 明天 + N 天 (或者 Today + 1 + N)
    // 这里我们定义 next_days 为“从明天开始往后数几天”
    const endDate = new Date(startDate);
    endDate.setDate(startDate.getDate() + p.next_days);
    
    query.append(`${dateField}.gte`, formatDate(startDate));
    query.append(`${dateField}.lte`, formatDate(endDate));
  } else {
    // 使用手动年份
    if (p.year_gte) query.append(`${dateField}.gte`, `${p.year_gte}-01-01`);
    if (p.year_lte) query.append(`${dateField}.lte`, `${p.year_lte}-12-31`);
  }

  // 类型
  if (p.with_genres.length) query.append('with_genres', p.with_genres.join(','));
  if (p.without_genres.length) query.append('without_genres', p.without_genres.join(','));

  // 关键词
  if (p.with_keywords_labels.length) {
    const ids = new Set();
    p.with_keywords_labels.forEach(label => {
      const mappedIds = keywordMapping.value[label];
      if (mappedIds) mappedIds.forEach(id => ids.add(id));
    });
    if (ids.size) query.append('with_keywords', Array.from(ids).join(',')); 
  }

  // ★★★ 核心修改：工作室/平台逻辑 ★★★
  // 如果是 TV，使用 with_networks；如果是 Movie，使用 with_companies
  if (p.with_companies_labels.length) {
    const ids = new Set();
    p.with_companies_labels.forEach(label => {
      const mappedIds = studioMapping.value[label];
      if (mappedIds) mappedIds.forEach(id => ids.add(id));
    });
    
    if (ids.size) {
      const idStr = Array.from(ids).join('|'); // 使用 OR 逻辑
      if (p.type === 'tv') {
        // 电视剧：查 Network (播出平台)
        query.append('with_networks', idStr);
      } else {
        // 电影：查 Company (制作公司)
        query.append('with_companies', idStr);
      }
    }
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

// --- 数据获取 (保持不变) ---
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

const fetchMappings = async () => {
  loading.value.mappings = true;
  try {
    const [kwRes, stRes] = await Promise.all([
      axios.get('/api/custom_collections/config/keyword_mapping'),
      axios.get('/api/custom_collections/config/studio_mapping')
    ]);
    const process = (data) => {
      const map = {};
      const list = Array.isArray(data) ? data : Object.entries(data).map(([k, v]) => ({ label: k, ...v }));
      list.forEach(item => {
        if (item.label && item.ids) {
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

watch(() => props.show, (val) => {
  if (val) {
    fetchMappings();
    if (movieGenres.value.length === 0) fetchBasicConfigs();
  }
});

const handleConfirm = () => {
  emit('confirm', generatedUrl.value, params.value.type);
  emit('update:show', false);
};
</script>