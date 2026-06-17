<!-- src/components/modals/TmdbDiscoveryHelper.vue -->
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
            ➜ "即将上线"模式未启用，当前使用年份筛选。
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
import { ref, computed, watch, h, nextTick } from 'vue';
import { NAvatar, NText } from 'naive-ui';
import axios from 'axios';
import { CheckmarkCircleOutline as CheckIcon } from '@vicons/ionicons5';

const props = defineProps({
  show: Boolean,
  initialUrl: String
});

const emit = defineEmits(['update:show', 'confirm']);

// 定义默认参数，方便重置
const defaultParams = {
  type: 'tv',
  sort_by: 'popularity.desc',
  year_gte: null,
  year_lte: null,
  next_days: 0,
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
};

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
const languageOptions = ref([]);
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
const studioOptions = computed(() => {
  const type = params.value.type; // 'movie' or 'tv'
  
  return Object.entries(studioMapping.value)
    .filter(([label, data]) => {
      if (type === 'movie') {
        // 电影模式：只显示有 company_ids 的
        return data.company_ids && data.company_ids.length > 0;
      } else {
        // 电视模式：只显示有 network_ids 的
        return data.network_ids && data.network_ids.length > 0;
      }
    })
    .map(([label, data]) => ({ label: label, value: label }));
});

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

  // 工作室/平台逻辑 
  if (p.with_companies_labels.length) {
    const ids = new Set();
    
    p.with_companies_labels.forEach(label => {
      const data = studioMapping.value[label];
      if (data) {
        // 根据当前模式取对应的 ID 列表
        const targetIds = p.type === 'tv' ? data.network_ids : data.company_ids;
        
        if (targetIds && targetIds.length > 0) {
          targetIds.forEach(id => ids.add(id));
        }
      }
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
  loading.value.countries = true; // 复用 loading 状态
  try {
    // ★★★ 修改 2: 调用 custom_collections 的接口 ★★★
    const [mvRes, tvRes, cRes, lRes] = await Promise.all([
      axios.get('/api/custom_collections/config/tmdb_movie_genres'),
      axios.get('/api/custom_collections/config/tmdb_tv_genres'),
      axios.get('/api/custom_collections/config/tmdb_countries'), // 获取国家
      axios.get('/api/custom_collections/config/languages')       // 获取语言
    ]);
    
    movieGenres.value = mvRes.data;
    tvGenres.value = tvRes.data;
    
    // 格式化国家选项
    countryOptions.value = cRes.data.map(item => ({
      label: item.label,
      value: item.value
    }));

    // ★★★ 修改 3: 格式化语言选项 (显示为 "中文 (zh)" 格式) ★★★
    languageOptions.value = lRes.data.map(item => ({
      label: `${item.label} (${item.value})`,
      value: item.value
    }));
    
    // 添加一个“不限”选项在最前面
    languageOptions.value.unshift({ label: '不限', value: null });

  } catch (e) {
    console.error('获取基础配置失败:', e);
  } finally {
    loading.value.genres = false;
    loading.value.countries = false;
  }
};

// 辅助：从 URL 参数中提取数组 (支持逗号或竖线分隔)
const extractArray = (val) => {
  if (!val) return [];
  // 解码，处理可能存在的 | 或 ,
  const decoded = decodeURIComponent(val);
  return decoded.split(/[,|]/).map(item => item.trim());
};

// 核心：将 URL 解析回 params
const parseUrlToParams = (urlStr) => {
  if (!urlStr) return;

  try {
    // 1. 处理基础 URL 和 类型
    // 这里的 urlStr 可能是完整的 https://... 或者是相对路径
    // 为了方便解析，如果是相对路径，补全一个 dummy host
    const fullUrl = urlStr.startsWith('http') ? urlStr : `https://www.themoviedb.org${urlStr}`;
    const urlObj = new URL(fullUrl);
    
    // 还原类型
    if (urlObj.pathname.includes('/movie')) {
      params.value.type = 'movie';
    } else {
      params.value.type = 'tv';
    }

    const sp = urlObj.searchParams;

    // 2. 还原基础字段
    if (sp.get('sort_by')) params.value.sort_by = sp.get('sort_by');
    if (sp.get('with_origin_country')) params.value.region = sp.get('with_origin_country');
    if (sp.get('with_original_language')) params.value.language = sp.get('with_original_language');
    if (sp.get('vote_average.gte')) params.value.vote_average = parseFloat(sp.get('vote_average.gte'));
    if (sp.get('vote_count.gte')) params.value.vote_count = parseInt(sp.get('vote_count.gte'));

    // 3. 还原日期 / 动态占位符
    const dateField = params.value.type === 'movie' ? 'primary_release_date' : 'first_air_date';
    
    // URLSearchParams.get() 会自动解码，但有个坑：它通常会把 URL 里的 '+' 解析为空格 ' '
    // 例如：{tomorrow+7} 解析后可能会变成 {tomorrow 7}
    const gteVal = sp.get(`${dateField}.gte`) || '';
    const lteVal = sp.get(`${dateField}.lte`) || '';

    // 宽松判断：只要包含 'tomorrow' 关键字就认为是动态日期
    if (gteVal.includes('tomorrow')) {
      // ★★★ 核心修复 ★★★
      // 正则改为：/tomorrow[\+\s](\d+)/
      // [\+\s] 表示匹配 "+" 或者 "空格"
      const match = lteVal.match(/tomorrow[\+\s](\d+)/);
      
      if (match && match[1]) {
        params.value.next_days = parseInt(match[1]);
      } else {
        // 如果没匹配到数字，可能是解析异常或只有 {tomorrow}，归零
        params.value.next_days = 0; 
      }
      
      // 清空静态年份，避免 UI 冲突
      params.value.year_gte = null;
      params.value.year_lte = null;
    } else {
      // 静态年份还原 (假设格式为 YYYY-01-01)
      params.value.next_days = 0;
      if (gteVal && gteVal.length >= 4) params.value.year_gte = parseInt(gteVal.substring(0, 4));
      if (lteVal && lteVal.length >= 4) params.value.year_lte = parseInt(lteVal.substring(0, 4));
    }

    // 4. 还原类型 (Genres) - ID 转数字
    const withGenres = extractArray(sp.get('with_genres'));
    params.value.with_genres = withGenres.map(Number);
    
    const withoutGenres = extractArray(sp.get('without_genres'));
    params.value.without_genres = withoutGenres.map(Number);

    // 5. 还原人员 (Cast/Crew) - 保持 ID
    // 注意：这里只能还原 ID，无法还原名字显示在 UI 上，除非调用 API 反查
    // 为了体验，UI 会显示 ID，用户可以删除重搜。
    params.value.with_cast = extractArray(sp.get('with_cast')).map(Number); // 尝试转数字
    params.value.with_crew = extractArray(sp.get('with_crew')).map(Number);

    // 6. ★★★ 反向映射：关键词 (ID -> Label) ★★★
    const keywordIds = extractArray(sp.get('with_keywords'));
    if (keywordIds.length > 0 && Object.keys(keywordMapping.value).length > 0) {
      const foundLabels = [];
      // 遍历映射表寻找 ID
      for (const [label, ids] of Object.entries(keywordMapping.value)) {
        // 如果映射表里的 ID 存在于 URL 参数中
        // 注意：映射表里的 ids 是数组，URL 里的也是数组，取交集
        const mapIds = Array.isArray(ids) ? ids.map(String) : [String(ids)];
        // 只要有一个 ID 匹配，就认为选中了这个 Label
        if (mapIds.some(id => keywordIds.includes(id))) {
          foundLabels.push(label);
        }
      }
      params.value.with_keywords_labels = foundLabels;
    }

    // 7. ★★★ 反向映射：工作室/平台 (ID -> Label) ★★★
    // 电影用 with_companies, 电视用 with_networks
    const companyIds = extractArray(sp.get('with_companies'));
    const networkIds = extractArray(sp.get('with_networks'));
    const allStudioIds = [...companyIds, ...networkIds];

    if (allStudioIds.length > 0 && Object.keys(studioMapping.value).length > 0) {
      const foundLabels = [];
      for (const [label, data] of Object.entries(studioMapping.value)) {
        const cIds = data.company_ids ? data.company_ids.map(String) : [];
        const nIds = data.network_ids ? data.network_ids.map(String) : [];
        const targetIds = [...cIds, ...nIds];

        if (targetIds.some(id => allStudioIds.includes(id))) {
          foundLabels.push(label);
        }
      }
      params.value.with_companies_labels = foundLabels;
    }

  } catch (e) {
    console.error("解析 URL 失败:", e);
    // 解析失败则保持默认或部分状态
  }
};

const fetchMappings = async () => {
  loading.value.mappings = true;
  try {
    const [kwRes, stRes] = await Promise.all([
      axios.get('/api/custom_collections/config/keyword_mapping'),
      axios.get('/api/custom_collections/config/studio_mapping')
    ]);

    // 处理关键词 (保持不变)
    const processKeywords = (data) => {
      const map = {};
      const list = Array.isArray(data) ? data : Object.entries(data).map(([k, v]) => ({ label: k, ...v }));
      list.forEach(item => {
        if (item.label && item.ids) {
          map[item.label] = Array.isArray(item.ids) ? item.ids : [item.ids];
        }
      });
      return map;
    };
    keywordMapping.value = processKeywords(kwRes.data);

    // ★★★ 修改：处理工作室 (支持分离 ID) ★★★
    const processStudios = (data) => {
      const map = {};
      const list = Array.isArray(data) ? data : Object.entries(data).map(([k, v]) => ({ label: k, ...v }));
      
      list.forEach(item => {
        if (!item.label) return;
        
        // 提取 company_ids
        let c_ids = [];
        if (item.company_ids) c_ids = Array.isArray(item.company_ids) ? item.company_ids : [item.company_ids];
        // 兼容旧数据：如果没有 company_ids 但有 ids，且没有 network_ids，暂且认为是 company
        else if (item.ids && !item.network_ids) c_ids = Array.isArray(item.ids) ? item.ids : [item.ids];

        // 提取 network_ids
        let n_ids = [];
        if (item.network_ids) n_ids = Array.isArray(item.network_ids) ? item.network_ids : [item.network_ids];
        
        map[item.label] = {
          company_ids: c_ids,
          network_ids: n_ids
        };
      });
      return map;
    };
    studioMapping.value = processStudios(stRes.data);

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
watch(() => params.value.type, () => {
  params.value.with_companies_labels = [];
});
watch(() => props.show, async (val) => {
  if (val) {
    // 1. 先重置参数
    params.value = JSON.parse(JSON.stringify(defaultParams));
    
    // 2. 并行加载配置和映射
    const promises = [fetchMappings()];
    if (movieGenres.value.length === 0 || languageOptions.value.length === 0) {
      promises.push(fetchBasicConfigs());
    }
    await Promise.all(promises);

    // 3. ★★★ 如果有 initialUrl，执行解析 ★★★
    if (props.initialUrl) {
      console.log("检测到初始 URL，开始解析...", props.initialUrl);
      parseUrlToParams(props.initialUrl);
    }
  }
});

const handleConfirm = () => {
  emit('confirm', generatedUrl.value, params.value.type);
  emit('update:show', false);
};
</script>