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
            设置后将忽略下方的年份筛选。例如设置 7 天，将筛选从明天开始一周内首播的内容。
          </n-text>
        </template>
        <div style="margin-top: 8px; font-size: 12px; color: #666; background: #f5f5f5; padding: 8px; border-radius: 4px;">
          <span v-if="params.next_days > 0">
            🔍 筛选范围: 
            <strong>{{ calculatedDateRange.start }}</strong> 至 
            <strong>{{ calculatedDateRange.end }}</strong>
          </span>
          <span v-else>
            ⚠️ "即将上线"模式未启用，当前使用年份筛选。
          </span>
        </div>
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
              :render-label="renderPersonLabel"
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
              :render-label="renderPersonLabel"
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
import { ref, computed, watch, h } from 'vue';
import { NAvatar, NText } from 'naive-ui';
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

// 自定义人员选项渲染函数 
const renderPersonLabel = (option) => {
  // option 是当前遍历到的演职人员数据对象
  return h(
    'div',
    {
      style: {
        display: 'flex',
        alignItems: 'center',
        padding: '4px 0'
      }
    },
    [
      // 1. 头像部分
      h(NAvatar, {
        round: true,
        size: 'small',
        // 如果有 profile_path 就拼接 TMDb 图片地址，否则 undefined (显示默认占位)
        src: option.profile_path 
             ? `https://image.tmdb.org/t/p/w45${option.profile_path}` 
             : undefined,
        style: {
          marginRight: '12px',
          flexShrink: 0 // 防止头像被挤压
        }
      }),
      
      // 2. 名字 + 额外信息部分 (可选：可以加个 known_for_department 辅助区分)
      h('div', { style: { display: 'flex', flexDirection: 'column' } }, [
        h('span', option.name),
        // 如果想显示更多区分信息（如职业），可以取消下面注释
        // h('span', { style: { fontSize: '12px', color: '#999' } }, option.known_for_department)
      ])
    ]
  );
};

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
const formatDateUTC = (date) => {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

// 辅助函数：格式化日期为 YYYY-MM-DD (直接操作本地日期对象，简单粗暴且有效)
const formatDateSimple = (date) => {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

// 新增：用于 UI 展示和 URL 生成的统一日期计算
const calculatedDateRange = computed(() => {
  const now = new Date();
  
  // 计算开始日期：今天 + 1天 (即明天)
  const start = new Date(now);
  start.setDate(now.getDate() + 1);
  
  // 计算结束日期：开始日期 + N天
  const end = new Date(start);
  end.setDate(start.getDate() + params.value.next_days);
  
  return {
    start: formatDateSimple(start),
    end: formatDateSimple(end)
  };
});

// --- URL 生成逻辑 ---
const generatedUrl = computed(() => {
  const p = params.value;
  const baseUrl = `https://www.themoviedb.org/discover/${p.type}`;
  const query = new URLSearchParams();

  query.append('sort_by', p.sort_by);

  const dateField = p.type === 'movie' ? 'primary_release_date' : 'first_air_date';
  
  if (p.next_days > 0) {
    // ★★★ 核心修改：不再写入死日期，而是写入动态占位符 ★★★
    // 后端解析时：
    // {tomorrow} -> 运行时日期的明天
    // {tomorrow+N} -> 运行时日期的明天 + N天
    query.append(`${dateField}.gte`, '{tomorrow}');
    query.append(`${dateField}.lte`, `{tomorrow+${p.next_days}}`);
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

  // 1. 先生成标准的编码 URL
  let finalUrl = `${baseUrl}?${query.toString()}`;

  // 2. ★★★ 核心修复：手动还原被编码的动态占位符 ★★★
  // 将 %7B 还原为 {
  // 将 %7D 还原为 }
  // 将 %2B 还原为 +
  finalUrl = finalUrl
    .replace(/%7B/g, '{')
    .replace(/%7D/g, '}')
    .replace(/%2B/g, '+');

  return finalUrl;
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