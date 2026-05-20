<!-- src/components/HDHiveResourceModal.vue -->
<template>
  <n-modal
    v-model:show="isVisible"
    preset="card"
    :title="`影巢资源: ${mediaTitle}`"
    style="width: 800px; max-width: 95%;"
  >
    <n-spin :show="loading">
      <n-empty
        v-if="resources.length === 0 && !loading"
        description="影巢暂无该资源，请尝试使用 MoviePilot 常规订阅。"
      />

      <n-space vertical v-else>
        <n-card
          v-for="res in resources"
          :key="res.slug"
          size="small"
          hoverable
        >
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <div style="min-width: 0;">
              <div style="font-weight: bold; font-size: 15px; margin-bottom: 4px;">
                {{ res.title || '未命名资源' }}
              </div>

              <n-space size="small" style="font-size: 12px;" wrap>
                <n-tag size="small" :type="getPanTypeColor(res.pan_type)" :bordered="false">
                  {{ formatPanType(res.pan_type) }}
                </n-tag>

                <n-tag size="small" type="default" :bordered="true" v-if="res.share_size">
                  {{ res.share_size }}
                </n-tag>

                <n-tag size="small" type="success" :bordered="false" v-if="res.video_resolution?.length">
                  {{ res.video_resolution.join(', ') }}
                </n-tag>

                <n-tag size="small" type="warning" :bordered="false" v-if="res.source?.length">
                  {{ res.source.join(', ') }}
                </n-tag>

                <n-tag size="small" type="info" :bordered="false" v-if="res._season_match_label">
                  {{ res._season_match_label }}
                </n-tag>

                <n-tag size="small" type="success" :bordered="false" v-if="res._completion_label">
                  {{ res._completion_label }}
                </n-tag>

                <span style="color: #888;" v-if="res.remark">
                  {{ res.remark }}
                </span>
              </n-space>
            </div>

            <div style="flex-shrink: 0; margin-left: 16px; text-align: right;">
              <div style="font-size: 12px; color: #f0a020; margin-bottom: 4px;">
                <span v-if="res.already_owned">已解锁</span>
                <span v-else-if="res.unlock_points === 0 || res.unlock_points === null">免费</span>
                <span v-else>需 {{ res.unlock_points }} 积分</span>
              </div>

              <n-button
                type="primary"
                color="#f0a020"
                size="small"
                @click="download(res)"
                :loading="downloadingSlug === res.slug"
              >
                {{ isOffline(res.pan_type) ? '离线下载' : '一键转存' }}
              </n-button>
            </div>
          </div>
        </n-card>
      </n-space>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import { NModal, NSpin, NEmpty, NSpace, NCard, NTag, NButton, useMessage } from 'naive-ui';
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
const downloadingSlug = ref(null);

const mediaTitle = computed(() => {
  if (!props.media) return '';

  let title = props.media.title || props.media.name || '未知影视';
  const sNum = props.seasonNumber || props.media.season_number;

  if (sNum && !title.includes('季')) {
    title += ` 第 ${sNum} 季`;
  }

  return title;
});

const formatPanType = (type) => {
  if (!type) return '115网盘';

  const t = String(type).toLowerCase();

  if (t === '115') return '115网盘';
  if (t === 'magnet') return '磁力链接';
  if (t === 'ed2k') return '电驴 ED2K';
  if (t === 'bt') return 'BT 种子';

  return String(type).toUpperCase();
};

const isOffline = (type) => {
  if (!type) return false;
  return ['magnet', 'ed2k', 'bt'].includes(String(type).toLowerCase());
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

  // 剧集/季入口必须用剧集 TMDB ID 查询影巢，不能用 Season 自己的 tmdb_id。
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

const fetchResources = async () => {
  if (!props.media) return;

  loading.value = true;
  resources.value = [];

  try {
    const params = {
      tmdb_id: getTmdbId(),
      media_type: getMediaType()
    };

    const season = props.seasonNumber || props.media.season_number;
    if (season !== null && season !== undefined) {
      params.season = season;
    }

    const res = await axios.get('/api/subscription/hdhive/resources', { params });

    if (res.data.success) {
      resources.value = res.data.data || [];
      return;
    }

    message.error(res.data.message || '获取影巢资源失败');
  } catch (e) {
    message.error(e.response?.data?.message || '获取影巢资源失败');
  } finally {
    loading.value = false;
  }
};

watch(() => props.show, (newVal) => {
  if (newVal) {
    fetchResources();
  }
});

const download = async (resource) => {
  downloadingSlug.value = resource.slug;

  try {
    const payload = {
      slug: resource.slug,
      tmdb_id: getTmdbId(),
      media_type: getMediaType(),
      title: mediaTitle.value
    };

    const res = await axios.post('/api/subscription/hdhive/download', payload);

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
    downloadingSlug.value = null;
  }
};
</script>
