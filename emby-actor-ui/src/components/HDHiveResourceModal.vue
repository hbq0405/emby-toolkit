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
              全部 {{ allDisplayCount }}
            </n-button>
            <n-button :type="sourceFilter === 'shared_pool' ? 'primary' : 'default'" @click="sourceFilter = 'shared_pool'">
              共享池 {{ sharedPoolDisplayCount }}
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
                    :type="isSharedPool(res) ? 'success' : (isChannel(res) ? 'info' : 'warning')"
                    :bordered="false"
                  >
                    {{ isSharedPool(res) ? '共享池' : (isChannel(res) ? '频道' : '影巢') }}
                  </n-tag>
                  <div style="font-weight: 700; font-size: 15px; line-height: 1.4; word-break: break-all;">
                    {{ sharedPoolCardTitle(res) }}
                  </div>
                </div>

                <n-space size="small" style="font-size: 12px; margin-bottom: 6px;" wrap>
                  <n-tag size="small" :type="getPanTypeColor(res.pan_type)" :bordered="false">
                    {{ formatPanType(res.pan_type) }}
                  </n-tag>

                  <n-tag size="small" type="default" :bordered="true" v-if="isSharedPoolGroup(res)">
                    资源版本 {{ res._shared_pool_versions.length }}
                  </n-tag>

                  <n-tag size="small" type="default" :bordered="true" v-if="res.share_size && !isSharedPoolGroup(res)">
                    {{ res.share_size }}
                  </n-tag>

                  <n-tag size="small" type="success" :bordered="false" v-if="formatResolution(res) && !isSharedPoolGroup(res)">
                    {{ formatResolution(res) }}
                  </n-tag>

                  <n-tag size="small" type="warning" :bordered="false" v-if="formatSource(res) && !isSharedPoolGroup(res)">
                    {{ formatSource(res) }}
                  </n-tag>

                  <n-tag size="small" type="info" :bordered="false" v-if="res.source_channel">
                    来源：{{ res.source_channel }}
                  </n-tag>

                  <n-tag size="small" type="info" :bordered="false" v-if="res._season_match_label">
                    {{ res._season_match_label }}
                  </n-tag>

                  <n-tag size="small" type="default" :bordered="true" v-if="res._shared_pool_source_label && !isSharedPoolGroup(res)">
                    {{ res._shared_pool_source_label }}
                  </n-tag>


                  <n-tag size="small" type="success" :bordered="false" v-if="res._completion_label">
                    {{ res._completion_label }}
                  </n-tag>

                  <n-tag
                    v-for="tag in cloudFeatureTags(res)"
                    :key="tag.label"
                    size="small"
                    :type="tag.type || 'default'"
                    :bordered="tag.bordered !== false"
                  >
                    {{ tag.label }}
                  </n-tag>
                </n-space>

                <div v-if="isSharedPoolGroup(res)" class="cloud-version-list">
                  <div
                    v-for="(version, index) in res._shared_pool_versions"
                    :key="getResourceKey(version)"
                    class="cloud-version-row"
                  >
                    <div class="cloud-version-main">
                      <n-space size="small" wrap>
                        <n-tag size="small" type="default" :bordered="false">
                          {{ sharedPoolVersionLabel(version, index) }}
                        </n-tag>
                        <n-tag size="small" :type="getPanTypeColor(version.pan_type)" :bordered="false">
                          {{ formatPanType(version.pan_type) }}
                        </n-tag>
                        <n-tag size="small" type="default" :bordered="true" v-if="version.share_size">
                          {{ version.share_size }}
                        </n-tag>
                        <n-tag size="small" type="success" :bordered="false" v-if="formatResolution(version)">
                          {{ formatResolution(version) }}
                        </n-tag>
                        <n-tag size="small" type="warning" :bordered="false" v-if="formatSource(version)">
                          {{ formatSource(version) }}
                        </n-tag>
                        <n-tag size="small" type="default" :bordered="true" v-if="version._shared_pool_source_label">
                          {{ version._shared_pool_source_label }}
                        </n-tag>
                        <n-tag
                          v-for="tag in cloudFeatureTags(version)"
                          :key="`${getResourceKey(version)}:${tag.label}`"
                          size="small"
                          :type="tag.type || 'default'"
                          :bordered="tag.bordered !== false"
                        >
                          {{ tag.label }}
                        </n-tag>
                      </n-space>
                    </div>
                    <div class="cloud-version-action">
                      <div class="cloud-version-action-hint">
                        {{ sharedPoolActionText(version).hint }}
                      </div>
                      <n-button
                        type="primary"
                        size="small"
                        :color="sharedPoolActionText(version).color"
                        :loading="downloadingKey === getResourceKey(version)"
                        @click="download(version)"
                      >
                        {{ sharedPoolActionText(version).button }}
                      </n-button>
                    </div>
                  </div>
                </div>

                <div v-if="formatQuality(res) && !isSharedPool(res)" style="font-size: 13px; color: #555; line-height: 1.5; margin-bottom: 4px;">
                  📦 {{ formatQuality(res) }}
                </div>

                <div v-if="res.remark && !isSharedPool(res)" style="font-size: 12px; color: #777; line-height: 1.5; word-break: break-all;">
                  📝 {{ res.remark }}
                </div>

                <div v-if="res.message_date" style="font-size: 12px; color: #999; margin-top: 4px;">
                  🕘 {{ res.message_date }}
                </div>
              </div>

              <div v-if="!isSharedPoolGroup(res)" style="flex-shrink: 0; min-width: 92px; text-align: right;">
                <div style="font-size: 12px; color: #f0a020; margin-bottom: 6px;">
                  <span v-if="isSharedPool(res)">{{ sharedPoolActionText(res).hint }}</span>
                  <span v-else-if="isChannel(res)">可转存</span>
                  <span v-else-if="res.already_owned">已解锁</span>
                  <span v-else-if="res.unlock_points === 0 || res.unlock_points === null">免费</span>
                  <span v-else>需 {{ res.unlock_points }} 积分</span>
                </div>

                <n-button
                  type="primary"
                  :color="isSharedPool(res) ? sharedPoolActionText(res).color : (isChannel(res) ? '#2080f0' : '#f0a020')"
                  size="small"
                  @click="download(res)"
                  :loading="downloadingKey === getResourceKey(res)"
                >
                  {{ isSharedPool(res) ? sharedPoolActionText(res).button : (isOffline(res.pan_type) || res.magnet_url ? '离线下载' : '一键转存') }}
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

const mediaYear = computed(() => {
  const media = props.media || {};
  const value = media.year || media.release_year || media.first_air_year || media.release_date || media.first_air_date;
  const match = String(value || '').match(/\d{4}/);
  return match ? match[0] : '';
});

const formatTitleWithYearAndSeason = (raw, year, seasonNumber) => {
  let title = stripSeasonSuffix(raw) || '未知影视';
  const y = String(year || '').match(/\d{4}/)?.[0] || '';
  if (y && !new RegExp(`[（(]\\s*${y}\\s*[）)]`).test(title)) {
    title += `（${y}）`;
  }
  const sNum = Number(seasonNumber || 0);
  if (sNum > 0 && !/第\s*\d+\s*季/.test(title)) {
    title += `第 ${sNum} 季`;
  }
  return title;
};

const mediaTitle = computed(() => {
  if (!props.media) return '';
  const sNum = props.seasonNumber || props.media.season_number;
  return formatTitleWithYearAndSeason(rawTitle.value, mediaYear.value, sNum);
});

const isSharedPool = (resource) => {
  const source = String(resource?.source_type || resource?._cloud_source || resource?.source || '').toLowerCase();
  return source === 'shared_pool' || source === 'shared' || source === 'shared_center' || source === 'center';
};

const isHDHive = (resource) => {
  const source = String(resource?.source_type || resource?._cloud_source || resource?.source || '').toLowerCase();
  return !isSharedPool(resource) && !isChannel(resource) && (source === 'hdhive' || source === 'hive' || Boolean(resource?.slug));
};

const hasSharedPoolShareTransfer = (resource) => {
  if (!isSharedPool(resource)) return false;
  const mode = String(resource?.preferred_transfer_mode || resource?.transfer_mode || '').toLowerCase();
  const status = String(resource?.share_channel_status || resource?.share_channel?.status || resource?.logical_season_share_channel?.status || '').toLowerCase();
  return Boolean(
    resource?.share_transfer_available ||
    resource?.has_valid_share_channel ||
    mode === 'share' ||
    status === 'valid'
  );
};

const sharedPoolActionText = (resource) => {
  if (hasSharedPoolShareTransfer(resource)) {
    return { hint: '可转存', button: '转存', color: '#2080f0' };
  }
  return { hint: '可秒传', button: '秒传', color: '#18a058' };
};

const isSharedPoolGroup = (resource) => {
  return Boolean(resource?._shared_pool_group && Array.isArray(resource?._shared_pool_versions) && resource._shared_pool_versions.length > 0);
};

const sharedPoolCardTitle = (resource) => {
  if (isSharedPoolGroup(resource)) {
    return resource._shared_pool_group_title || resource.title || resource.name || '共享池资源';
  }
  return resource?.title || resource?.name || '未命名资源';
};

const sharedPoolVersionLabel = (resource, index) => {
  const label = String(resource?._shared_pool_version_label || '').trim();
  if (label) return label;
  return `版本 ${index + 1}`;
};

const hdhiveCount = computed(() => resources.value.filter((item) => isHDHive(item)).length);
const channelCount = computed(() => resources.value.filter((item) => isChannel(item)).length);
const sharedPoolCount = computed(() => resources.value.filter((item) => isSharedPool(item)).length);

const parseSeasonNumber = (resource) => {
  const direct = Number(resource?.season_number || resource?._shared_pool_season_number || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const text = `${resource?.title || ''} ${resource?.name || ''} ${resource?.remark || ''}`;
  const matched = text.match(/第\s*(\d{1,3})\s*季/i) || text.match(/\bS(\d{1,3})\b/i) || text.match(/Season\s*(\d{1,3})/i);
  const parsed = Number(matched?.[1] || 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

const parseSharedPoolVersionIndex = (resource) => {
  const direct = Number(resource?._shared_pool_version_index || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const matched = String(resource?._shared_pool_version_label || '').match(/版本\s*(\d{1,3})\s*\//);
  const parsed = Number(matched?.[1] || 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

const sourceRank = (resource) => {
  if (isSharedPool(resource)) return 0;
  if (isHDHive(resource)) return 1;
  if (isChannel(resource)) return 2;
  return 9;
};

const sortCloudResources = (list) => {
  return [...(list || [])]
    .map((item, index) => ({ item, index }))
    .sort((a, b) => {
      const rankDiff = sourceRank(a.item) - sourceRank(b.item);
      if (rankDiff) return rankDiff;
      if (isSharedPool(a.item) && isSharedPool(b.item)) {
        const seasonA = parseSeasonNumber(a.item) || 9999;
        const seasonB = parseSeasonNumber(b.item) || 9999;
        if (seasonA !== seasonB) return seasonA - seasonB;
        const versionA = parseSharedPoolVersionIndex(a.item) || 9999;
        const versionB = parseSharedPoolVersionIndex(b.item) || 9999;
        if (versionA !== versionB) return versionA - versionB;
      }
      return a.index - b.index;
    })
    .map(({ item }) => item);
};

const sharedPoolSeasonGroupKey = (resource) => {
  if (!isSharedPool(resource)) return '';
  const season = parseSeasonNumber(resource);
  if (!season) return '';
  const tmdb = String(resource?.tmdb_id || '').trim();
  const title = stripSeasonSuffix(resource?.title || resource?.name || '').toLowerCase();
  return `${tmdb || title}:s${season}`;
};

const groupSharedPoolResources = (list) => {
  const groups = new Map();
  const output = [];

  (list || []).forEach((item) => {
    const key = sharedPoolSeasonGroupKey(item);
    if (!key) {
      output.push(item);
      return;
    }
    if (!groups.has(key)) {
      const group = {
        ...item,
        unique_id: `shared_pool_group:${key}`,
        _shared_pool_group: true,
        _shared_pool_group_title: item.title || item.name || '共享池资源',
        _shared_pool_versions: [],
      };
      groups.set(key, group);
      output.push(group);
    }
    groups.get(key)._shared_pool_versions.push(item);
  });

  return output.map((item) => {
    if (!isSharedPoolGroup(item) || item._shared_pool_versions.length <= 1) {
      return isSharedPoolGroup(item) ? item._shared_pool_versions[0] : item;
    }
    item._shared_pool_versions = sortCloudResources(item._shared_pool_versions);
    item._shared_pool_source_label = `资源版本 ${item._shared_pool_versions.length}`;
    return item;
  });
};

const resourcesForFilter = (filter) => {
  if (filter === 'hdhive') return resources.value.filter((item) => isHDHive(item));
  if (filter === 'channel') return resources.value.filter((item) => isChannel(item));
  if (filter === 'shared_pool') return resources.value.filter((item) => isSharedPool(item));
  return resources.value;
};

const displayListForFilter = (filter) => groupSharedPoolResources(sortCloudResources(resourcesForFilter(filter)));

const allDisplayCount = computed(() => displayListForFilter('all').length);
const sharedPoolDisplayCount = computed(() => displayListForFilter('shared_pool').length);

const displayResources = computed(() => {
  return displayListForFilter(sourceFilter.value);
});

const warningMessages = computed(() => stats.value?.warnings || []);
const summaryText = computed(() => {
  if (!stats.value) return '';
  return `共享池 ${stats.value.shared_pool_total || 0} 条，影巢 ${stats.value.hdhive_filtered || 0}/${stats.value.hdhive_total || 0} 条，频道 ${stats.value.channel_total || 0} 条，当前展示 ${displayResources.value.length} 张卡片。共享池按季聚合展示，影巢/频道保留宽松搜索。`;
});

const formatPanType = (type) => {
  if (!type) return '115网盘';

  const t = String(type).toLowerCase();

  if (t === '115') return '115网盘';
  if (t === 'rapid115' || t === 'rapid' || t === 'shared_pool') return '115秒传';
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
  const t = String(type || '').toLowerCase();
  if (t === 'rapid115' || t === 'rapid' || t === 'shared_pool') return 'success';
  if (!type || t === '115') return 'primary';
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
  if (isSharedPool(resource)) return false;
  const source = String(resource?.source_type || resource?._cloud_source || resource?.source || '').toLowerCase();
  return source === 'channel' || source === 'tg' || (Boolean(resource?.target_link || resource?.magnet_url) && !resource?.slug);
};

const getResourceKey = (resource) => {
  return (
    resource?.unique_id ||
    `${resource?.source_type || 'res'}:${resource?.source_kind || ''}:${resource?.source_id || resource?.source_ref_id || ''}:${resource?.sha1 || resource?.manifest_hash || resource?.slug || resource?.message_link || resource?.target_link || resource?.magnet_url || resource?.title}`
  );
};

const formatResolution = (resource) => {
  const values = resource?.video_resolution || resource?.resolution;
  if (Array.isArray(values)) return values.filter(Boolean).join(', ');
  return values || '';
};

const formatSource = (resource) => {
  const values = resource?.source;
  const normalize = (value) => {
    const text = String(value || '').trim();
    if (!text || text === 'channel' || text === '共享秒传' || text === '可秒传') return '';
    return text;
  };
  if (Array.isArray(values)) return values.map(normalize).filter(Boolean).join(', ');
  return normalize(values);
};


const normalizeTrackTexts = (value) => {
  const out = [];
  const add = (text) => {
    const valueText = String(text || '').trim();
    if (valueText && !out.includes(valueText)) out.push(valueText);
  };
  const walk = (node) => {
    if (node === null || node === undefined || node === '') return;
    if (Array.isArray(node)) {
      node.slice(0, 80).forEach(walk);
      return;
    }
    if (typeof node === 'object') {
      [
        'display', 'DisplayTitle', 'title', 'Title', 'name', 'Name',
        'language', 'Language', 'lang', 'DisplayLanguage',
        'codec', 'Codec', 'channel_layout', 'channels'
      ].forEach((key) => {
        if (Object.prototype.hasOwnProperty.call(node, key)) walk(node[key]);
      });
      return;
    }
    add(node);
  };
  walk(value);
  return out;
};

const metadataContainers = (resource) => {
  const item = resource || {};
  return [
    item,
    item.version_summary,
    item.summary_json,
    item.media_signature_json,
    item.raw_summary_json,
    item.rapid_meta_json
  ].filter((x) => x && typeof x === 'object');
};

const trackTexts = (resource, kind) => {
  const keys = kind === 'audio'
    ? [
        'audio_list', 'audios', 'audio_tracks', 'audio', 'audio_track',
        'default_audio', 'default_audio_track', 'audio_languages', 'languages'
      ]
    : [
        'subtitle_list', 'subtitles', 'subtitle_tracks', 'subtitle', 'subtitles_text',
        'default_subtitle', 'default_subtitle_track', 'subtitle_languages'
      ];
  const texts = [];
  metadataContainers(resource).forEach((container) => {
    keys.forEach((key) => {
      normalizeTrackTexts(container[key]).forEach((text) => {
        if (text && !texts.includes(text)) texts.push(text);
      });
    });
  });
  return texts;
};

const containsMandarinAudioText = (text) => {
  const value = String(text || '').trim();
  if (!value) return false;
  return value.includes('国语') || value.includes('普通话') || value.includes('普通話');
};

const containsChineseSubtitleText = (text) => {
  const value = String(text || '').trim();
  if (!value) return false;
  return ['中文', '简中', '繁中', '简体', '繁体', '中英'].some((token) => value.includes(token));
};

const containsSpecialEffectSubtitleText = (text) => {
  const value = String(text || '').trim();
  return !!value && value.includes('特效');
};

const containsBilingualSubtitleText = (text) => {
  const value = String(text || '').trim();
  return !!value && (value.includes('双语') || value.includes('雙語'));
};

const hasMandarinAudio = (resource) => {
  if (resource?.has_mandarin_audio === true) return true;
  return trackTexts(resource, 'audio').some(containsMandarinAudioText);
};

const hasChineseSubtitle = (resource) => {
  if (resource?.has_chinese_subtitle === true) return true;
  return trackTexts(resource, 'subtitle').some(containsChineseSubtitleText);
};

const hasSpecialEffectSubtitle = (resource) => {
  if (resource?.has_special_effect_subtitle === true) return true;
  return trackTexts(resource, 'subtitle').some(containsSpecialEffectSubtitleText);
};

const hasBilingualSubtitle = (resource) => {
  if (resource?.has_bilingual_subtitle === true) return true;
  return trackTexts(resource, 'subtitle').some(containsBilingualSubtitleText);
};


const sharedPoolTags = (resource) => {
  if (!isSharedPool(resource)) return [];
  const raw = resource?._shared_pool_tag_labels || resource?._shared_pool_tags || [];
  const seen = new Set();
  const tags = [];
  const pushTag = (tag, fallbackType = 'default', bordered = true) => {
    const obj = typeof tag === 'string' ? { label: tag, type: fallbackType, bordered } : { ...(tag || {}) };
    const label = String(obj.label || obj.name || obj.text || '').trim();
    if (!label || seen.has(label)) return;
    if (label === resource?._completion_label) return;
    seen.add(label);
    tags.push({ label, type: obj.type || fallbackType, bordered: obj.bordered !== undefined ? obj.bordered : bordered });
  };
  if (Array.isArray(raw)) {
    raw.forEach((tag) => pushTag(tag));
  }
  if (resource?.is_clean_version) pushTag({ label: '纯净版', type: 'warning', bordered: false });
  if (resource?.is_short_drama) pushTag({ label: '短剧', type: 'info', bordered: false });
  if (resource?.is_animation) pushTag({ label: '动漫', type: 'success', bordered: false });
  const labels = resource?.tag_labels;
  if (Array.isArray(labels)) {
    labels.forEach((label) => pushTag(String(label), 'default', true));
  }
  return tags;
};

const cloudFeatureTags = (resource) => {
  const tags = isSharedPool(resource) ? sharedPoolTags(resource) : [];
  const seen = new Set(tags.map((tag) => String(tag?.label || '').trim()).filter(Boolean));
  const push = (label, type = 'default', bordered = false) => {
    if (!label || seen.has(label)) return;
    seen.add(label);
    tags.push({ label, type, bordered });
  };

  if (hasMandarinAudio(resource)) push('国语', 'success', false);
  if (hasChineseSubtitle(resource)) push('中字', 'info', false);
  if (hasSpecialEffectSubtitle(resource)) push('特效', 'warning', false);
  if (hasBilingualSubtitle(resource)) push('双语', 'info', false);

  return tags;
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
      limit: 100,
      hdhive_limit: 50,
      channel_limit: 50,
      shared_limit: 100
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
      source_type: resource.source_type || resource._cloud_source || (isSharedPool(resource) ? 'shared_pool' : (resource.slug ? 'hdhive' : 'channel')),
      resource,
      slug: resource.slug,
      tmdb_id: getTmdbId(),
      media_type: getMediaType(),
      title: mediaTitle.value,
      year: mediaYear.value,
      mode: isSharedPool(resource) ? (hasSharedPoolShareTransfer(resource) ? 'share' : 'rapid') : undefined
    };

    const res = await axios.post('/api/subscription/cloud/download', payload);

    if (res.data.success) {
      message.success(res.data.message);
      emit('download-success');
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

.cloud-version-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 10px;
}

.cloud-version-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  border-radius: 8px;
  background: rgba(12, 18, 42, .42);
  border: 1px solid rgba(148, 177, 255, .14);
}

.cloud-version-main {
  min-width: 0;
  flex: 1;
}

.cloud-version-action {
  flex: 0 0 auto;
  min-width: 72px;
  text-align: right;
}

.cloud-version-action-hint {
  font-size: 12px;
  color: #f0a020;
  margin-bottom: 6px;
}

@media (max-width: 640px) {
  .cloud-version-row {
    flex-direction: column;
  }

  .cloud-version-action {
    text-align: left;
  }
}
</style>
