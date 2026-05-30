<template>
  <n-modal v-model:show="visible" preset="card" title="共享池求分享" style="width: 980px; max-width: 96vw;" class="custom-modal glass-modal">
    <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
      先搜索并确认 TMDb 目标，再选择电影 / 全剧 / 单季 / 单集和可自动校验的媒体参数。中心端会按参数自动计算需要冻结的贡献值。
    </n-alert>

    <n-space class="toolbar" :vertical="isMobile" :size="12">
      <n-radio-group v-model:value="requestMediaType" size="small" class="media-type-switch">
        <n-radio-button value="movie">电影</n-radio-button>
        <n-radio-button value="tv">剧集</n-radio-button>
      </n-radio-group>
      <n-input v-model:value="searchKeyword" :placeholder="requestMediaType === 'movie' ? '搜索电影' : '搜索剧集'" clearable @keyup.enter="searchTmdb">
        <template #prefix><n-icon :component="SearchIcon" /></template>
      </n-input>
      <n-button type="primary" :loading="searchLoading" @click="searchTmdb">搜索 TMDb</n-button>
    </n-space>

    <n-data-table
      v-if="searchItems.length"
      size="small"
      :loading="searchLoading"
      :columns="searchColumns"
      :data="searchItems"
      :pagination="{ pageSize: 5 }"
      :row-key="row => `${row.media_type}-${row.tmdb_id}`"
      style="margin-bottom: 14px;"
    />

    <div v-if="selectedMedia" class="selected-share-box">
      <div class="selected-title">已选择：{{ appendYear(selectedMedia.title, selectedMedia.release_year) }}</div>
      <div class="selected-desc">
        {{ selectedMedia.media_type === 'movie' ? '电影' : '剧集' }} · TMDb {{ selectedMedia.tmdb_id }}
      </div>
    </div>

    <n-form :model="form" label-placement="left" label-width="105" style="margin-top: 12px;">
      <n-divider title-placement="left">求分享目标</n-divider>
      <n-form-item label="目标类型">
        <n-radio-group v-model:value="form.target_type">
          <n-space>
            <n-radio v-for="opt in targetOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</n-radio>
          </n-space>
        </n-radio-group>
      </n-form-item>
      <n-grid v-if="form.media_type === 'tv'" :cols="isMobile ? 1 : 3" :x-gap="12">
        <n-gi v-if="['season','episode'].includes(form.target_type)">
          <n-form-item label="季号">
            <n-input-number v-model:value="form.season_number" :min="1" :max="999" style="width: 100%;" />
          </n-form-item>
        </n-gi>
        <n-gi v-if="form.target_type === 'episode'">
          <n-form-item label="集号">
            <n-input-number v-model:value="form.episode_number" :min="1" :max="9999" style="width: 100%;" />
          </n-form-item>
        </n-gi>
      </n-grid>

      <n-divider title-placement="left">匹配参数</n-divider>
      <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
        参数取值来自视频流标准媒体分析结果，和媒体库资产数据保持一致，可以精确求取所需资源，而非文件名虚标的参数。
      </n-alert>
      <n-grid :cols="isMobile ? 1 : 3" :x-gap="12">
        <n-gi><n-form-item label="分辨率"><n-select v-model:value="form.params.resolution" :options="paramOptions.resolution" clearable placeholder="不限" /></n-form-item></n-gi>
        <n-gi><n-form-item label="编码"><n-select v-model:value="form.params.codec" :options="paramOptions.codec" clearable placeholder="不限" /></n-form-item></n-gi>
        <n-gi><n-form-item label="HDR/杜比"><n-select v-model:value="form.params.effect" :options="paramOptions.effect" clearable placeholder="不限" /></n-form-item></n-gi>
        <n-gi><n-form-item label="帧率"><n-select v-model:value="form.params.frame_rate" :options="paramOptions.frame_rate" clearable placeholder="不限" /></n-form-item></n-gi>
        <n-gi><n-form-item label="音轨"><n-select v-model:value="form.params.audio" :options="paramOptions.audio" clearable placeholder="不限" /></n-form-item></n-gi>
        <n-gi><n-form-item label="字幕"><n-select v-model:value="form.params.subtitle" :options="paramOptions.subtitle" clearable placeholder="不限" /></n-form-item></n-gi>
      </n-grid>
      <n-form-item label="体积范围">
        <n-input v-model:value="form.params.size_range" placeholder="例如 ≤10GB、20-40GB，留空不限" />
      </n-form-item>

      <n-divider title-placement="left">悬赏与有效期</n-divider>
      <n-grid :cols="isMobile ? 1 : 3" :x-gap="12">
        <n-gi><n-form-item label="有效期"><n-input-number v-model:value="form.expires_days" :min="1" :max="365" style="width: 100%;"><template #suffix>天</template></n-input-number></n-form-item></n-gi>
        <n-gi><n-form-item label="超时加倍"><n-switch v-model:value="form.auto_escalation"><template #checked>开启</template><template #unchecked>关闭</template></n-switch></n-form-item></n-gi>
        <n-gi v-if="form.auto_escalation"><n-form-item label="加倍周期"><n-input-number v-model:value="form.escalation_interval_hours" :min="1" :max="168" style="width: 100%;"><template #suffix>小时</template></n-input-number></n-form-item></n-gi>
      </n-grid>
    </n-form>

    <div class="share-request-quote-box">
      <div class="quote-title">预计悬赏：{{ quote?.current_bounty || '-' }}，最高冻结：{{ quote?.max_bounty || '-' }}</div>
      <div v-if="quote?.auto_escalation" class="quote-note">超时加倍：每 {{ quote.escalation_interval_hours }} 小时加倍，按有效期最多 {{ quote.escalation_rounds }} 轮自动冻结。</div>
      <div class="quote-breakdown">
        <span v-for="it in (quote?.breakdown || [])" :key="it.key" class="quote-chip">{{ it.label }} +{{ it.delta }}</span>
      </div>
    </div>

    <template #footer>
      <n-space justify="space-between" align="center">
        <n-text depth="3">最高冻结由中心自动计算；关闭超时加倍时只冻结当前悬赏。</n-text>
        <n-space>
          <n-button @click="visible = false">取消</n-button>
          <n-button type="primary" :disabled="!selectedMedia" :loading="submitting" @click="submit">发布求分享</n-button>
        </n-space>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { computed, h, onMounted, onUnmounted, reactive, ref, watch } from 'vue';
import axios from 'axios';
import {
  NAlert, NButton, NDataTable, NDivider, NForm, NFormItem, NGi, NGrid, NIcon, NInput,
  NInputNumber, NModal, NRadio, NRadioButton, NRadioGroup, NSelect, NSpace, NSwitch, NText, useMessage
} from 'naive-ui';
import { SearchOutline as SearchIcon } from '@vicons/ionicons5';

const props = defineProps({
  show: { type: Boolean, default: false },
  initialMedia: { type: Object, default: null },
  initialTarget: { type: Object, default: null },
  initialParams: { type: Object, default: null },
});
const emit = defineEmits(['update:show', 'created']);
const message = useMessage();

const visible = computed({
  get: () => props.show,
  set: value => emit('update:show', value),
});

const isMobile = ref(false);
const checkMobile = () => { isMobile.value = window.innerWidth <= 768; };
const searchLoading = ref(false);
const submitting = ref(false);
const searchKeyword = ref('');
const searchItems = ref([]);
const selectedMedia = ref(null);
const requestMediaType = ref('movie');
const quote = ref(null);

const defaultParamOptions = () => ({
  resolution: [
    { label: '4K', value: '4k' }, { label: '1080p', value: '1080p' },
    { label: '720p', value: '720p' }, { label: '480p', value: '480p' },
  ],
  codec: [
    { label: 'HEVC', value: 'HEVC' }, { label: 'H.264', value: 'H.264' },
    { label: 'AV1', value: 'AV1' }, { label: 'VP9', value: 'VP9' },
  ],
  effect: [
    { label: 'DoVi P8', value: 'DoVi_P8' }, { label: 'DoVi P7', value: 'DoVi_P7' },
    { label: 'DoVi P5', value: 'DoVi_P5' }, { label: 'DoVi', value: 'DoVi' },
    { label: 'HDR10+', value: 'HDR10+' }, { label: 'HDR', value: 'HDR' }, { label: 'SDR', value: 'SDR' },
  ],
  frame_rate: [
    { label: '≥ 60 fps', value: '60' }, { label: '≥ 50 fps', value: '50' },
    { label: '≥ 30 fps', value: '30' }, { label: '24 fps', value: '24' },
  ],
  audio: [
    { label: '国语', value: '国语' }, { label: '粤语', value: '粤语' }, { label: '英语', value: '英语' },
    { label: '日语', value: '日语' }, { label: '韩语', value: '韩语' },
  ],
  subtitle: [
    { label: '简体', value: '简体' }, { label: '繁体', value: '繁体' }, { label: '英文', value: '英文' },
    { label: '日文', value: '日文' }, { label: '韩文', value: '韩文' }, { label: '无', value: '无' },
  ],
});
const paramOptions = reactive(defaultParamOptions());

const defaultParams = () => ({
  resolution: null,
  codec: null,
  effect: null,
  frame_rate: null,
  audio: null,
  subtitle: null,
  size_range: '',
});

const form = reactive({
  tmdb_id: '',
  media_type: 'movie',
  target_type: 'movie',
  title: '',
  release_year: null,
  poster_path: '',
  overview: '',
  season_number: 1,
  episode_number: 1,
  params: defaultParams(),
  expires_days: 7,
  auto_escalation: false,
  escalation_interval_hours: 24,
});

const targetOptions = computed(() => {
  if (requestMediaType.value === 'movie') return [{ label: '电影', value: 'movie' }];
  return [
    { label: '全剧', value: 'series' },
    { label: '单季', value: 'season' },
    { label: '单集', value: 'episode' },
  ];
});

const appendYear = (title, year) => {
  const base = String(title || '').trim() || '-';
  const y = year ? String(year).trim() : '';
  if (!y || base === '-') return base;
  return new RegExp(`\\(${y}\\)\\s*$`).test(base) ? base : `${base} (${y})`;
};

const compactParams = () => {
  const allowed = new Set(['resolution', 'codec', 'effect', 'frame_rate', 'audio', 'subtitle', 'size_range', 'complete_season']);
  const params = {};
  Object.entries(form.params || {}).forEach(([key, value]) => {
    if (!allowed.has(key) || value == null) return;
    const text = String(value).trim();
    if (text) params[key] = text;
  });
  return params;
};

const buildPayload = () => {
  const episodeNumbers = form.target_type === 'episode' && form.episode_number ? [Number(form.episode_number)] : [];
  return {
    tmdb_id: form.tmdb_id,
    media_type: form.media_type,
    target_type: form.target_type,
    title: form.title,
    release_year: form.release_year,
    poster_path: form.poster_path,
    overview: form.overview,
    season_number: ['season','episode'].includes(form.target_type) ? form.season_number : null,
    episode_number: form.target_type === 'episode' ? form.episode_number : null,
    episode_numbers: episodeNumbers,
    params_json: compactParams(),
    expires_days: form.expires_days || 7,
    auto_escalation: Boolean(form.auto_escalation),
    escalation_interval_hours: form.escalation_interval_hours || 24,
  };
};

let quoteTimer = null;
const refreshQuote = async () => {
  if (!selectedMedia.value) return;
  try {
    const res = await axios.post('/api/shared/resources/share-requests/quote', buildPayload());
    const data = res.data?.data || null;
    quote.value = data;
  } catch (e) {
    console.warn('share request quote failed', e);
  }
};
const scheduleQuote = () => {
  clearTimeout(quoteTimer);
  quoteTimer = setTimeout(refreshQuote, 260);
};

const reset = () => {
  searchKeyword.value = '';
  searchItems.value = [];
  selectedMedia.value = null;
  requestMediaType.value = 'movie';
  quote.value = null;
  Object.assign(form, {
    tmdb_id: '', media_type: 'movie', target_type: 'movie', title: '', release_year: null,
    poster_path: '', overview: '', season_number: 1, episode_number: 1,
    params: defaultParams(), expires_days: 7, auto_escalation: false,
    escalation_interval_hours: 24,
  });
};

const applyMedia = async (row) => {
  if (!row) return;
  selectedMedia.value = row;
  const mediaType = row.media_type === 'movie' ? 'movie' : 'tv';
  requestMediaType.value = mediaType;
  Object.assign(form, {
    tmdb_id: row.tmdb_id || '',
    media_type: mediaType,
    target_type: mediaType === 'movie' ? 'movie' : 'season',
    title: row.title || '',
    release_year: row.release_year || null,
    poster_path: row.poster_path || '',
    overview: row.overview || '',
    season_number: 1,
    episode_number: 1,
  });
  if (props.initialTarget && typeof props.initialTarget === 'object') {
    Object.assign(form, {
      target_type: props.initialTarget.target_type || form.target_type,
      season_number: props.initialTarget.season_number || form.season_number,
      episode_number: props.initialTarget.episode_number || form.episode_number,
    });
    if (Array.isArray(props.initialTarget.episode_numbers) && props.initialTarget.episode_numbers.length) {
      if (props.initialTarget.episode_numbers.length === 1) {
        form.target_type = 'episode';
        form.episode_number = Number(props.initialTarget.episode_numbers[0]) || form.episode_number;
      } else {
        // 115 分享没有“指定集数范围”的能力，多集缺口统一按季包求分享。
        form.target_type = 'season';
      }
    }
  }
  if (props.initialParams && typeof props.initialParams === 'object') {
    form.params = { ...defaultParams(), ...props.initialParams };
  }
  await refreshQuote();
};

const searchTmdb = async () => {
  const keyword = String(searchKeyword.value || '').trim();
  if (!keyword) return message.warning('请输入要搜索的片名');
  searchLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/share-requests/tmdb/search', { params: { keyword, media_type: requestMediaType.value } });
    searchItems.value = res.data?.items || [];
    if (!searchItems.value.length) message.info('TMDb 没有搜索到结果');
  } catch (e) {
    message.error(e.response?.data?.message || 'TMDb 搜索失败');
  } finally {
    searchLoading.value = false;
  }
};

const searchColumns = [
  { title: '媒体', key: 'title', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, appendYear(row.title, row.release_year)),
    h('div', { class: 'sub-title' }, `${row.media_type === 'movie' ? '电影' : '剧集'} · TMDb ${row.tmdb_id}`),
  ]) },
  { title: '简介', key: 'overview', minWidth: 340, ellipsis: { tooltip: true } },
  { title: '操作', key: 'actions', width: 96, render: row => h(NButton, {
    size: 'small', type: 'primary', ghost: true, onClick: () => applyMedia(row),
  }, { default: () => '选择' }) },
];

const loadParamOptions = async () => {
  try {
    const res = await axios.get('/api/shared/resources/share-requests/param-options');
    const data = res.data?.data || {};
    Object.keys(paramOptions).forEach(key => {
      if (Array.isArray(data[key]) && data[key].length) paramOptions[key] = data[key];
    });
  } catch (e) {
    console.warn('load share request param options failed', e);
  }
};

const submit = async () => {
  if (!selectedMedia.value) return message.warning('请先搜索并选择 TMDb 目标');
  if (form.media_type === 'tv' && ['season','episode'].includes(form.target_type) && !form.season_number) {
    return message.warning('请填写季号');
  }
  if (form.target_type === 'episode' && !form.episode_number) return message.warning('请填写集号');
  submitting.value = true;
  try {
    const payload = buildPayload();
    const res = await axios.post('/api/shared/resources/share-requests', payload);
    message.success(res.data?.message || '求分享已发布');
    visible.value = false;
    emit('created', res.data?.data || payload);
  } catch (e) {
    message.error(e.response?.data?.message || '发布求分享失败');
  } finally {
    submitting.value = false;
  }
};


watch(requestMediaType, (value) => {
  const mediaType = value === 'tv' ? 'tv' : 'movie';
  form.media_type = mediaType;
  if (mediaType === 'movie') {
    form.target_type = 'movie';
    form.season_number = 1;
    form.episode_number = 1;
    } else if (form.target_type === 'movie') {
    form.target_type = 'season';
  }
  if (selectedMedia.value && ((selectedMedia.value.media_type === 'movie' ? 'movie' : 'tv') !== mediaType)) {
    selectedMedia.value = null;
    form.tmdb_id = '';
    form.title = '';
    form.release_year = null;
    form.poster_path = '';
    form.overview = '';
    quote.value = null;
  }
  searchItems.value = [];
  scheduleQuote();
});

watch(() => props.show, async (show) => {
  if (!show) return;
  reset();
  await loadParamOptions();
  if (props.initialMedia) await applyMedia(props.initialMedia);
});

watch(
  () => [
    selectedMedia.value?.tmdb_id,
    form.target_type,
    form.season_number,
    form.episode_number,
    form.params.resolution,
    form.params.codec,
    form.params.effect,
    form.params.frame_rate,
    form.params.audio,
    form.params.subtitle,
    form.params.size_range,
    form.auto_escalation,
    form.escalation_interval_hours,
  ],
  () => scheduleQuote(),
);

onMounted(() => { checkMobile(); window.addEventListener('resize', checkMobile); loadParamOptions(); });
onUnmounted(() => { window.removeEventListener('resize', checkMobile); clearTimeout(quoteTimer); });
</script>

<style scoped>
.toolbar { margin-bottom: 12px; }
.media-type-switch { flex: 0 0 auto; }
.selected-share-box {
  padding: 12px 14px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.12);
  margin-bottom: 8px;
}
.selected-title { font-weight: 700; font-size: 15px; }
.selected-desc, .sub-title { font-size: 12px; opacity: .72; margin-top: 3px; }
.main-title { font-weight: 700; }
.share-request-quote-box {
  margin-top: 12px;
  padding: 12px 14px;
  border-radius: 14px;
  background: rgba(24, 160, 88, 0.10);
  border: 1px solid rgba(24, 160, 88, 0.22);
}
.quote-title { font-weight: 700; margin-bottom: 8px; }
.quote-note { font-size: 12px; opacity: .78; margin-bottom: 8px; }
.quote-breakdown { display: flex; flex-wrap: wrap; gap: 6px; }
.quote-chip {
  font-size: 12px;
  padding: 3px 8px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.12);
}
</style>
