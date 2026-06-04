<!-- src/components/HDHiveResourceModal.vue -->
<template>
  <n-modal
    v-model:show="isVisible"
    preset="card"
    :title="`云资源搜索: ${mediaTitle}`"
    style="width: 920px; max-width: 96%;"
  >
    <n-spin :show="loading">
      <n-space vertical size="medium">
        <n-alert v-if="summaryText" type="info" :bordered="false">
          {{ summaryText }}
        </n-alert>

        <n-alert
          v-for="warning in warningMessages"
          :key="warning"
          type="warning"
          :bordered="false"
        >
          {{ warning }}
        </n-alert>

        <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
          <n-input
            v-model:value="searchTitle"
            size="small"
            clearable
            placeholder="搜索标题，默认使用当前影视名称"
            style="width: 260px;"
            @keyup.enter="fetchResources"
          />
          <n-button size="small" :loading="loading" @click="fetchResources">
            重新搜索
          </n-button>
          <n-button-group size="small">
            <n-button :type="sourceFilter === 'all' ? 'primary' : 'default'" @click="sourceFilter = 'all'">
              全部 {{ resources.length }}
            </n-button>
            <n-button :type="sourceFilter === 'hdhive' ? 'primary' : 'default'" @click="sourceFilter = 'hdhive'">
              影巢 {{ hdhiveCount }}
            </n-button>
            <n-button :type="sourceFilter === 'channel' ? 'primary' : 'default'" @click="sourceFilter = 'channel'">
              频道 {{ channelCount }}
            </n-button>
          </n-button-group>
        </div>

        <n-empty
          v-if="displayResources.length === 0 && !loading"
          description="暂无云资源。可检查影巢授权、TG UserBot 登录状态，以及频道监听列表。"
        />

        <n-space vertical v-else>
          <n-card
            v-for="res in displayResources"
            :key="getResourceKey(res)"
            size="small"
            hoverable
            class="cloud-resource-card"
          >
            <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 16px;">
              <div style="min-width: 0; flex: 1;">
                <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 6px;">
                  <n-tag
                    size="small"
                    :type="isChannel(res) ? 'info' : 'warning'"
                    :bordered="false"
                  >
                    {{ isChannel(res) ? '频道' : '影巢' }}
                  </n-tag>
                  <div style="font-weight: 700; font-size: 15px; line-height: 1.4; word-break: break-all;">
                    {{ res.title || res.name || '未命名资源' }}
                  </div>
                </div>

                <n-space size="small" style="font-size: 12px; margin-bottom: 6px;" wrap>
                  <n-tag size="small" :type="getPanTypeColor(res.pan_type)" :bordered="false">
                    {{ formatPanType(res.pan_type) }}
                  </n-tag>

                  <n-tag size="small" type="default" :bordered="true" v-if="res.share_size">
                    {{ res.share_size }}
                  </n-tag>

                  <n-tag size="small" type="success" :bordered="false" v-if="formatResolution(res)">
                    {{ formatResolution(res) }}
                  </n-tag>

                  <n-tag size="small" type="warning" :bordered="false" v-if="formatSource(res)">
                    {{ formatSource(res) }}
                  </n-tag>

                  <n-tag size="small" type="info" :bordered="false" v-if="res.source_channel">
                    来源：{{ res.source_channel }}
                  </n-tag>

                  <n-tag size="small" type="info" :bordered="false" v-if="res._season_match_label">
                    {{ res._season_match_label }}
                  </n-tag>

                  <n-tag size="small" type="success" :bordered="false" v-if="res._completion_label">
                    {{ res._completion_label }}
                  </n-tag>
                </n-space>

                <div v-if="formatQuality(res)" style="font-size: 13px; color: #555; line-height: 1.5; margin-bottom: 4px;">
                  📦 {{ formatQuality(res) }}
                </div>

                <div v-if="res.remark" style="font-size: 12px; color: #777; line-height: 1.5; word-break: break-all;">
                  📝 {{ res.remark }}
                </div>

                <div v-if="res.message_date" style="font-size: 12px; color: #999; margin-top: 4px;">
                  🕘 {{ res.message_date }}
                </div>
              </div>

              <div style="flex-shrink: 0; min-width: 92px; text-align: right;">
                <div style="font-size: 12px; color: #f0a020; margin-bottom: 6px;">
                  <span v-if="isChannel(res)">可转存</span>
                  <span v-else-if="res.already_owned">已解锁</span>
                  <span v-else-if="res.unlock_points === 0 || res.unlock_points === null">免费</span>
                  <span v-else>需 {{ res.unlock_points }} 积分</span>
                </div>

                <n-button
                  type="primary"
                  :color="isChannel(res) ? '#2080f0' : '#f0a020'"
                  size="small"
                  @click="download(res)"
                  :loading="downloadingKey === getResourceKey(res)"
                >
                  {{ isOffline(res.pan_type) || res.magnet_url ? '离线下载' : '一键转存' }}
                </n-button>
              </div>
            </div>
          </n-card>
        </n-space>
      </n-space>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import {
  NModal,
  NSpin,
  NEmpty,
  NSpace,
  NCard,
  NTag,
  NButton,
  NButtonGroup,
  NInput,
  NAlert,
  useMessage
} from 'naive-ui';
import axios from 'axios';

const props = defineProps({
  show: Boolean,
  media: Object,
  seasonNumber: Number
});

const emit = defineEmits(['update:show', 'download-success']);

const message = useMessage();

const isVisible = computed({
  get: () => props.show,
  set: (val) => emit('update:show', val)
});

const loading = ref(false);
const resources = ref([]);
const stats = ref(null);
const downloadingKey = ref(null);
const sourceFilter = ref('all');
const searchTitle = ref('');

const rawTitle = computed(() => {
  return props.media?.title || props.media?.name || props.media?.series_title || props.media?.parent_title || '';
});

const stripSeasonSuffix = (value) => {
  const text = String(value || '').trim();
  if (!text) return '';

  return text
    .replace(/\s*(?:第\s*\d+\s*季|S\d{1,3}|Season\s*\d{1,3})\s*$/i, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
};

const mediaTitle = computed(() => {
  if (!props.media) return '';

  let title = stripSeasonSuffix(rawTitle.value) || '未知影视';
  const sNum = props.seasonNumber || props.media.season_number;

  if (sNum && !title.includes('季')) {
    title += ` 第 ${sNum} 季`;
  }

  return title;
});

const mediaYear = computed(() => {
  const media = props.media || {};
  const value = media.year || media.release_year || media.first_air_year || media.release_date || media.first_air_date;
  const match = String(value || '').match(/\d{4}/);
  return match ? match[0] : '';
});

const hdhiveCount = computed(() => resources.value.filter((item) => !isChannel(item)).length);
const channelCount = computed(() => resources.value.filter((item) => isChannel(item)).length);

const displayResources = computed(() => {
  if (sourceFilter.value === 'hdhive') {
    return resources.value.filter((item) => !isChannel(item));
  }
  if (sourceFilter.value === 'channel') {
    return resources.value.filter((item) => isChannel(item));
  }
  return resources.value;
});

const warningMessages = computed(() => stats.value?.warnings || []);
const summaryText = computed(() => {
  if (!stats.value) return '';
  return `影巢 ${stats.value.hdhive_filtered || 0}/${stats.value.hdhive_total || 0} 条，频道 ${stats.value.channel_total || 0} 条，当前展示 ${stats.value.shown || resources.value.length} 条。剧集云搜索默认不按季过滤，方便肉眼挑选。`;
});

const formatPanType = (type) => {
  if (!type) return '115网盘';

  const t = String(type).toLowerCase();

  if (t === '115') return '115网盘';
  if (t === 'magnet') return '磁力链接';
  if (t === 'ed2k') return '电驴 ED2K';
  if (t === 'bt') return 'BT 种子';
  if (t === '离线') return '离线资源';

  return String(type).toUpperCase();
};

const isOffline = (type) => {
  if (!type) return false;
  return ['magnet', 'ed2k', 'bt', '离线'].includes(String(type).toLowerCase());
};

const getPanTypeColor = (type) => {
  if (!type || String(type).toLowerCase() === '115') return 'primary';
  return 'info';
};

const normalizeMediaType = (value) => {
  const raw = String(value || '').trim().toLowerCase();

  if (['movie', 'movies', 'film', 'films'].includes(raw)) return 'movie';
  if (['tv', 'series', 'season', 'episode', 'show', 'shows', 'tvshow', 'tvshows'].includes(raw)) return 'tv';

  return raw ? 'tv' : 'movie';
};

const getMediaType = () => {
  return normalizeMediaType(props.media?.media_type || props.media?.item_type);
};

const getTmdbId = () => {
  const mediaType = getMediaType();

  // 剧集/季入口必须用剧集 TMDB ID 查询云资源，不能用 Season 自己的 tmdb_id。
  if (mediaType === 'tv') {
    return (
      props.media?.parent_series_tmdb_id ||
      props.media?.series_tmdb_id ||
      props.media?.parent_tmdb_id ||
      props.media?.tmdb_id ||
      props.media?.id
    );
  }

  return props.media?.tmdb_id || props.media?.id;
};

const isChannel = (resource) => {
  const source = String(resource?.source_type || resource?._cloud_source || resource?.source || '').toLowerCase();
  return source === 'channel' || source === 'tg' || Boolean(resource?.target_link || resource?.magnet_url) && !resource?.slug;
};

const getResourceKey = (resource) => {
  return (
    resource?.unique_id ||
    `${resource?.source_type || 'res'}:${resource?.slug || resource?.message_link || resource?.target_link || resource?.magnet_url || resource?.title}`
  );
};

const formatResolution = (resource) => {
  const values = resource?.video_resolution || resource?.resolution;
  if (Array.isArray(values)) return values.filter(Boolean).join(', ');
  return values || '';
};

const formatSource = (resource) => {
  const values = resource?.source;
  if (Array.isArray(values)) return values.filter(Boolean).join(', ');
  if (typeof values === 'string' && values !== 'channel') return values;
  return '';
};

const formatQuality = (resource) => {
  const values = resource?.quality || resource?.source_detail || '';
  return Array.isArray(values) ? values.filter(Boolean).join(', ') : values;
};

const fetchResources = async () => {
  if (!props.media) return;

  loading.value = true;
  resources.value = [];
  stats.value = null;

  try {
    const params = {
      tmdb_id: getTmdbId(),
      media_type: getMediaType(),
      title: searchTitle.value || rawTitle.value || mediaTitle.value,
      year: mediaYear.value,
      limit: 80,
      hdhive_limit: 50,
      channel_limit: 50
    };

    const season = props.seasonNumber || props.media.season_number;
    if (season !== null && season !== undefined) {
      params.season = season;
    }

    const res = await axios.get('/api/subscription/cloud/resources', { params });

    if (res.data.success) {
      resources.value = res.data.data || [];
      stats.value = res.data.stats || null;
      return;
    }

    message.error(res.data.message || '获取云资源失败');
  } catch (e) {
    message.error(e.response?.data?.message || '获取云资源失败');
  } finally {
    loading.value = false;
  }
};

watch(() => props.show, (newVal) => {
  if (newVal) {
    searchTitle.value = stripSeasonSuffix(rawTitle.value) || rawTitle.value || mediaTitle.value;
    sourceFilter.value = 'all';
    fetchResources();
  }
});

const download = async (resource) => {
  const key = getResourceKey(resource);
  downloadingKey.value = key;

  try {
    const payload = {
      source_type: resource.source_type || resource._cloud_source || (resource.slug ? 'hdhive' : 'channel'),
      resource,
      slug: resource.slug,
      tmdb_id: getTmdbId(),
      media_type: getMediaType(),
      title: mediaTitle.value,
      year: mediaYear.value
    };

    const res = await axios.post('/api/subscription/cloud/download', payload);

    if (res.data.success) {
      message.success(res.data.message);
      emit('download-success');
      setTimeout(() => {
        isVisible.value = false;
      }, 1500);
    } else {
      message.error(res.data.message || '触发下载失败');
    }
  } catch (e) {
    message.error(e.response?.data?.message || '触发下载失败');
  } finally {
    downloadingKey.value = null;
  }
};
</script>

<style scoped>
.cloud-resource-card :deep(.n-card__content) {
  padding: 12px 14px;
}
</style>
