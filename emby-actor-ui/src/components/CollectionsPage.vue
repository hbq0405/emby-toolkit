<!-- src/components/CollectionsPage.vue (状态显示优化最终版) -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="collections-page">
      <n-page-header>
        <template #title>
          原生合集
        </template>
        <template #footer>
          <n-space align="center" size="large">
            <n-tag :bordered="false" round>
              共 {{ globalStats.totalCollections }} 合集
            </n-tag>
            <n-tag v-if="globalStats.totalMissingMovies > 0" type="warning" :bordered="false" round>
              {{ globalStats.collectionsWithMissing }} 合集缺失 {{ globalStats.totalMissingMovies }} 部
            </n-tag>
            <n-tag v-if="globalStats.totalUnreleased > 0" type="info" :bordered="false" round>
              {{ globalStats.totalUnreleased }} 部未上映
            </n-tag>
            <n-tag v-if="globalStats.totalSubscribed > 0" type="default" :bordered="false" round>
              {{ globalStats.totalSubscribed }} 部已订阅
            </n-tag>
            <n-tag v-if="globalStats.totalMissingMovies === 0 && globalStats.totalCollections > 0" type="success" :bordered="false" round>
              所有合集均无缺失
            </n-tag>
          </n-space>
        </template>
        <template #extra>
          <n-space>
            <!-- ★ 批量操作按钮 -->
            <n-dropdown
              v-if="selectedCollectionIds.length > 0"
              trigger="click"
              :options="batchActions"
              @select="handleBatchAction"
            >
              <n-button type="primary">
                批量操作 ({{ selectedCollectionIds.length }})
                <template #icon><n-icon :component="CaretDownIcon" /></template>
              </n-button>
            </n-dropdown>

            <n-popconfirm @positive-click="subscribeAllMissingMovies" :disabled="globalStats.totalMissingMovies === 0">
              <template #trigger>
                <n-tooltip>
                  <template #trigger>
                    <n-button circle :loading="isSubscribingAll" :disabled="globalStats.totalMissingMovies === 0">
                      <template #icon><n-icon :component="CloudDownloadIcon" /></template>
                    </n-button>
                  </template>
                  一键订阅所有缺失
                </n-tooltip>
              </template>
              确定要将所有 {{ globalStats.totalMissingMovies }} 部缺失的电影提交到 MoviePilot 订阅吗？
            </n-popconfirm>

            <n-tooltip>
              <template #trigger>
                <n-button @click="triggerFullRefresh" :loading="isRefreshing" circle>
                  <template #icon><n-icon :component="SyncOutline" /></template>
                </n-button>
              </template>
              刷新所有合集信息
            </n-tooltip>
          </n-space>
        </template>
        <n-alert title="操作提示" type="info" style="margin-top: 24px;">
          <li>点击 <n-icon :component="SyncOutline" /> 可扫描Emby所有原生合集并显示缺失。</li><br />
          <li>点击 <n-icon :component="CloudDownloadIcon" /> 可一键订阅所有缺失，也可以通过任务中心智能订阅定期检查缺失并订阅。</li>
        </n-alert>
      </n-page-header>

      <!-- ★★★ 排序和筛选控件 ★★★ -->
      <n-space :wrap="true" :size="[20, 12]" style="margin-top: 24px; margin-bottom: 24px;">
        <n-input v-model:value="searchQuery" placeholder="按名称搜索..." clearable style="min-width: 200px;" />
        
        <n-select
          v-model:value="filterStatus"
          :options="statusFilterOptions"
          style="min-width: 160px;"
        />
        
        <n-select
          v-model:value="sortKey"
          :options="sortKeyOptions"
          style="min-width: 180px;"
        />
        
        <n-button-group>
          <n-button @click="sortOrder = 'asc'" :type="sortOrder === 'asc' ? 'primary' : 'default'" ghost>
            <template #icon><n-icon :component="ArrowUpIcon" /></template>
            升序
          </n-button>
          <n-button @click="sortOrder = 'desc'" :type="sortOrder === 'desc' ? 'primary' : 'default'" ghost>
            <template #icon><n-icon :component="ArrowDownIcon" /></template>
            降序
          </n-button>
        </n-button-group>
      </n-space>

      <div v-if="isInitialLoading" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="error" class="center-container"><n-alert title="加载错误" type="error" style="max-width: 500px;">{{ error }}</n-alert></div>
      
      <div v-else-if="filteredAndSortedCollections.length > 0">
        <n-grid cols="1 s:2 m:3 l:4 xl:5" :x-gap="20" :y-gap="20" responsive="screen">
          <n-gi v-for="(item, i) in renderedCollections" :key="item.emby_collection_id">
            <n-card 
              class="dashboard-card series-card" 
              :bordered="false" 
              content-style="display: flex; padding: 0; gap: 16px;"
              :class="{ selected: selectedCollectionIds.includes(item.emby_collection_id) }"
              @click="toggleSelection(item.emby_collection_id, $event, i)"
              hoverable
            >
              <n-checkbox
                :checked="selectedCollectionIds.includes(item.emby_collection_id)"
                @update:checked="(checked, event) => toggleSelection(item.emby_collection_id, event, i)"
                class="card-checkbox"
              />
              <div class="card-poster-container"><n-image lazy :src="getCollectionPosterUrl(item.poster_path)" class="card-poster" object-fit="cover"><template #placeholder><div class="poster-placeholder"><n-icon :component="AlbumsIcon" size="32" /></div></template></n-image></div>
              <div class="card-content-container">
                <div class="card-header"><n-ellipsis class="card-title" :tooltip="{ style: { maxWidth: '300px' } }">{{ item.name }}</n-ellipsis></div>
                <div class="card-status-area">
                  <n-space align="center">
                    <n-tooltip :disabled="!isTooltipNeeded(item)">
                    <template #trigger>
                      <n-tag :type="getStatusTagType(item)" round>
                        {{ getShortStatusText(item) }}
                      </n-tag>
                    </template>
                    {{ getFullStatusText(item) }}
                  </n-tooltip>
                    <n-text :depth="3" class="last-checked-text">上次检查: {{ formatTimestamp(item.last_checked_at) }}</n-text>
                  </n-space>
                </div>
                <div class="card-actions">
                  <n-tooltip>
                    <template #trigger>
                      <n-button type="primary" size="small" circle @click.stop="() => openMissingMoviesModal(item)">
                        <template #icon><n-icon :component="EyeIcon" /></template>
                      </n-button>
                    </template>
                    查看详情
                  </n-tooltip>
                  <n-tooltip><template #trigger><n-button text @click.stop="openInEmby(item.emby_collection_id)"><template #icon><n-icon :component="EmbyIcon" size="18" /></template></n-button></template>在 Emby 中打开</n-tooltip>
                  <n-tooltip><template #trigger><n-button text tag="a" :href="`https://www.themoviedb.org/collection/${item.tmdb_collection_id}`" target="_blank" :disabled="!item.tmdb_collection_id" @click.stop><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button></template>在 TMDb 中打开</n-tooltip>
                </div>
              </div>
            </n-card>
          </n-gi>
        </n-grid>

        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>

      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>

    <n-modal v-model:show="showModal" preset="card" style="width: 90%; max-width: 1200px;" :title="selectedCollection ? `详情 - ${selectedCollection.name}` : ''" :bordered="false" size="huge">
      <div v-if="selectedCollection">
        <n-tabs type="line" animated>
          <n-tab-pane name="missing" :tab="`缺失影片 (${missingMoviesInModal.length})`">
            <n-empty v-if="missingMoviesInModal.length === 0" description="太棒了！没有已上映的缺失影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="movie in missingMoviesInModal" :key="movie.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ movie.title }}<br />({{ extractYear(movie.release_date) || '未知年份' }})</div></div>
                  <template #action>
                    <!-- ★★★ 核心修改：使用按钮组 ★★★ -->
                    <n-button-group style="width: 100%;">
                      <n-button @click="subscribeMovie(movie)" type="primary" size="small" :loading="subscribing[movie.tmdb_id]" style="width: 50%;">
                        <template #icon><n-icon :component="CloudDownloadIcon" /></template>
                        订阅
                      </n-button>
                      <n-tooltip>
                        <template #trigger>
                          <n-button @click="ignoreMovie(movie)" type="tertiary" size="small" ghost style="width: 50%;">
                            <template #icon><n-icon :component="BanIcon" /></template>
                            忽略
                          </n-button>
                        </template>
                        忽略后，此电影将不再被视为缺失
                      </n-tooltip>
                    </n-button-group>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>
          
          <n-tab-pane name="in_library" :tab="`已入库 (${inLibraryMoviesInModal.length})`">
             <n-empty v-if="inLibraryMoviesInModal.length === 0" description="该合集在媒体库中没有任何影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="movie in inLibraryMoviesInModal" :key="movie.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ movie.title }}<br />({{ extractYear(movie.release_date) || '未知年份' }})</div></div>
                   <template #action>
                    <n-tag type="success" size="small" style="width: 100%; justify-content: center;">
                      <template #icon><n-icon :component="CheckmarkCircle" /></template>
                      已在库
                    </n-tag>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <n-tab-pane name="unreleased" :tab="`未上映 (${unreleasedMoviesInModal.length})`">
            <n-empty v-if="unreleasedMoviesInModal.length === 0" description="该合集没有已知的未上映影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="movie in unreleasedMoviesInModal" :key="movie.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster"></template>
                  <div class="movie-info"><div class="movie-title">{{ movie.title }}<br />({{ extractYear(movie.release_date) || '未知年份' }})</div></div>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <n-tab-pane name="subscribed" :tab="`已订阅 (${subscribedMoviesInModal.length})`">
            <n-empty v-if="subscribedMoviesInModal.length === 0" description="你没有订阅此合集中的任何影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="movie in subscribedMoviesInModal" :key="movie.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ movie.title }}<br />({{ extractYear(movie.release_date) || '未知年份' }})</div></div>
                  <template #action>
                    <n-button @click="unsubscribeMovie(movie)" type="warning" size="small" block ghost>
                      <template #icon><n-icon :component="CloseCircleIcon" /></template>
                      取消订阅
                    </n-button>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>
        </n-tabs>
      </div>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, computed, watch, h } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NTooltip, NGrid, NGi, NCard, NImage, NEllipsis, NSpin, NAlert, NModal, NTabs, NTabPane, NPopconfirm, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup } from 'naive-ui';
import { SyncOutline, AlbumsOutline as AlbumsIcon, EyeOutline as EyeIcon, CloudDownloadOutline as CloudDownloadIcon, CloseCircleOutline as CloseCircleIcon, CheckmarkCircleOutline as CheckmarkCircle, CaretDownOutline as CaretDownIcon, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, BanOutline as BanIcon } from '@vicons/ionicons5';
import { format } from 'date-fns';
import { useConfig } from '../composables/useConfig.js';

const props = defineProps({ taskStatus: { type: Object, required: true } });
const { configModel } = useConfig();
const message = useMessage();
const dialog = useDialog();
const isTaskRunning = computed(() => props.taskStatus.is_running);
const EmbyIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 48 48", width: "18", height: "18" }, [ h('path', { d: "M24,4.2c-11,0-19.8,8.9-19.8,19.8S13,43.8,24,43.8s19.8-8.9,19.8-19.8S35,4.2,24,4.2z M24,39.8c-8.7,0-15.8-7.1-15.8-15.8S15.3,8.2,24,8.2s15.8,7.1,15.8,15.8S32.7,39.8,24,39.8z", fill: "currentColor" }), h('polygon', { points: "22.2,16.4 22.2,22.2 16.4,22.2 16.4,25.8 22.2,25.8 22.2,31.6 25.8,31.6 25.8,25.8 31.6,31.6 31.6,22.2 25.8,22.2 25.8,16.4 ", fill: "currentColor" }) ]);
const TMDbIcon = () => h('svg', { xmlns: "http://www.w3.org/2000/svg", viewBox: "0 0 512 512", width: "18", height: "18" }, [ h('path', { d: "M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zM133.2 176.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zM133.2 262.6a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8zm63.3-22.4a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm74.8 108.2c-27.5-3.3-50.2-26-53.5-53.5a8 8 0 0 1 16-.6c2.3 19.3 18.8 34 38.1 31.7a8 8 0 0 1 7.4 8c-2.3.3-4.5.4-6.8.4zm-74.8-108.2a22.4 22.4 0 1 1 44.8 0 22.4 22.4 0 1 1 -44.8 0zm149.7 22.4a22.4 22.4 0 1 1 0-44.8 22.4 22.4 0 1 1 0 44.8z", fill: "#01b4e4" }) ]);

const collections = ref([]);
const isInitialLoading = ref(true);
const isRefreshing = ref(false);
const error = ref(null);
const subscribing = ref({});
const showModal = ref(false);
const selectedCollection = ref(null);
const isSubscribingAll = ref(false);
const displayCount = ref(50);
const INCREMENT = 50;
const loaderRef = ref(null);
let observer = null;

const selectedCollectionIds = ref([]);
const lastSelectedIndex = ref(null);

const searchQuery = ref('');
const filterStatus = ref('all');
const sortKey = ref('missing_count');
const sortOrder = ref('desc');

const statusFilterOptions = [
  { label: '所有合集', value: 'all' },
  { label: '有缺失', value: 'has_missing' },
  { label: '已完整', value: 'complete' },
  { label: '有已订阅', value: 'has_subscribed' },
  { label: '有未上映', value: 'has_unreleased' },
];
const sortKeyOptions = [
  { label: '按缺失数量', value: 'missing_count' },
  { label: '按合集名称', value: 'name' },
  { label: '按上次检查时间', value: 'last_checked_at' },
];

const toggleSelection = (collectionId, event, index) => {
  if (!event) return;
  
  if (event.target.closest('.n-checkbox')) {
    event.stopPropagation();
  }

  if (event.shiftKey && lastSelectedIndex.value !== null) {
    const start = Math.min(lastSelectedIndex.value, index);
    const end = Math.max(lastSelectedIndex.value, index);
    const idsInRange = renderedCollections.value.slice(start, end + 1).map(c => c.emby_collection_id);
    
    const isCurrentlySelected = selectedCollectionIds.value.includes(collectionId);
    const willSelect = !isCurrentlySelected;

    if (willSelect) {
      const newSet = new Set(selectedCollectionIds.value);
      idsInRange.forEach(id => newSet.add(id));
      selectedCollectionIds.value = Array.from(newSet);
    } else {
      selectedCollectionIds.value = selectedCollectionIds.value.filter(id => !idsInRange.includes(id));
    }
  } else {
    const idx = selectedCollectionIds.value.indexOf(collectionId);
    if (idx > -1) {
      selectedCollectionIds.value.splice(idx, 1);
    } else {
      selectedCollectionIds.value.push(collectionId);
    }
  }
  lastSelectedIndex.value = index;
};

const batchActions = computed(() => [
  {
    label: '标记为已订阅',
    key: 'markAsSubscribed',
    icon: () => h(NIcon, { component: CheckmarkCircle })
  }
]);

const handleBatchAction = (key) => {
  if (key === 'markAsSubscribed') {
    const selectedWithMissing = collections.value.filter(c => 
      selectedCollectionIds.value.includes(c.emby_collection_id) && c.has_missing
    );
    if (selectedWithMissing.length === 0) {
      message.info('选中的合集中没有需要标记的缺失电影。');
      return;
    }
    
    dialog.warning({
      title: '确认操作',
      content: `确定要将选中的 ${selectedWithMissing.length} 个合集中的所有“缺失”电影的状态标记为“已订阅”吗？`,
      positiveText: '确定',
      negativeText: '取消',
      onPositiveClick: async () => {
        const tmdbIdsToMark = [];
        selectedWithMissing.forEach(c => {
          c.movies.forEach(m => {
            if (m.status === 'missing') {
              tmdbIdsToMark.push(m.tmdb_id);
            }
          });
        });

        if (tmdbIdsToMark.length === 0) {
          message.info('没有找到需要标记的缺失电影。');
          return;
        }

        try {
          await axios.post('/api/media/batch_update_status', {
            tmdb_ids: tmdbIdsToMark,
            item_type: 'Movie',
            new_status: 'WANTED'
          });
          message.success(`成功将 ${tmdbIdsToMark.length} 部电影标记为已订阅！`);
          await loadCachedData();
          selectedCollectionIds.value = [];
        } catch (err) {
          message.error(err.response?.data?.error || '批量标记失败。');
        }
      }
    });
  }
};

const getMovieCountByStatus = (collection, status) => {
  if (!collection || !Array.isArray(collection.movies)) return 0;
  return collection.movies.filter(m => m.status === status).length;
};

const globalStats = computed(() => {
  const stats = {
    totalCollections: 0,
    collectionsWithMissing: 0,
    totalMissingMovies: 0,
    totalUnreleased: 0,
    totalSubscribed: 0,
  };

  // ★★★ 第二重保险：在使用前检查 collections.value 是否为数组 ★★★
  if (!Array.isArray(collections.value)) {
    return stats;
  }

  stats.totalCollections = collections.value.length;

  for (const collection of collections.value) {
    const missingCount = getMovieCountByStatus(collection, 'missing');
    if (missingCount > 0) {
      stats.collectionsWithMissing++;
      stats.totalMissingMovies += missingCount;
    }
    stats.totalUnreleased += getMovieCountByStatus(collection, 'unreleased');
    stats.totalSubscribed += getMovieCountByStatus(collection, 'subscribed');
  }
  return stats;
});

const filteredAndSortedCollections = computed(() => {
  // ★★★ 第二重保险：在使用前检查 collections.value 是否为数组 ★★★
  if (!Array.isArray(collections.value)) {
    return [];
  }
  
  let list = [...collections.value];

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    list = list.filter(item => item.name.toLowerCase().includes(query));
  }

  switch (filterStatus.value) {
    case 'has_missing':
      list = list.filter(item => item.has_missing);
      break;
    case 'complete':
      list = list.filter(item => !item.has_missing && item.status !== 'unlinked' && item.status !== 'tmdb_error');
      break;
    case 'has_subscribed':
      list = list.filter(item => getMovieCountByStatus(item, 'subscribed') > 0);
      break;
    case 'has_unreleased':
      list = list.filter(item => getMovieCountByStatus(item, 'unreleased') > 0);
      break;
  }

  list.sort((a, b) => {
    let valA, valB;
    switch (sortKey.value) {
      case 'name':
        valA = a.name || '';
        valB = b.name || '';
        return sortOrder.value === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(a);
      case 'last_checked_at':
        valA = a.last_checked_at ? new Date(a.last_checked_at).getTime() : 0;
        valB = b.last_checked_at ? new Date(b.last_checked_at).getTime() : 0;
        break;
      case 'missing_count':
      default:
        valA = a.missing_count || 0;
        valB = b.missing_count || 0;
        break;
    }
    return sortOrder.value === 'asc' ? valA - valB : valB - valA;
  });

  return list;
});

const renderedCollections = computed(() => filteredAndSortedCollections.value.slice(0, displayCount.value));
const hasMore = computed(() => displayCount.value < filteredAndSortedCollections.value.length);
const loadMore = () => { if (hasMore.value) displayCount.value += INCREMENT; };

const emptyStateDescription = computed(() => {
  if (collections.value && collections.value.length > 0 && filteredAndSortedCollections.value.length === 0) {
    return '没有匹配当前筛选条件的合集。';
  }
  return '没有找到任何电影合集。';
});

const inLibraryMoviesInModal = computed(() => {
  if (!selectedCollection.value || !Array.isArray(selectedCollection.value.movies)) return [];
  return selectedCollection.value.movies.filter(movie => movie.status === 'in_library');
});
const missingMoviesInModal = computed(() => {
  if (!selectedCollection.value || !Array.isArray(selectedCollection.value.movies)) return [];
  return selectedCollection.value.movies.filter(movie => movie.status === 'missing');
});
const unreleasedMoviesInModal = computed(() => {
  if (!selectedCollection.value || !Array.isArray(selectedCollection.value.movies)) return [];
  return selectedCollection.value.movies.filter(movie => movie.status === 'unreleased');
});
const subscribedMoviesInModal = computed(() => {
  if (!selectedCollection.value || !Array.isArray(selectedCollection.value.movies)) return [];
  return selectedCollection.value.movies.filter(movie => movie.status === 'subscribed');
});

const loadCachedData = async () => {
  if (collections.value.length === 0) isInitialLoading.value = true;
  error.value = null;
  try {
    const response = await axios.get('/api/collections/status', { headers: { 'Cache-Control': 'no-cache' } });
    collections.value = response.data;
    displayCount.value = 50;
  } catch (err) {
    error.value = err.response?.data?.error || '无法加载合集数据。';
    // ★★★ 第一重保险：如果加载失败，确保 collections 是一个安全的空数组 ★★★
    collections.value = [];
  } finally {
    isInitialLoading.value = false;
  }
};

const subscribeAllMissingMovies = async () => {
  isSubscribingAll.value = true;
  try {
    const response = await axios.post('/api/collections/subscribe_missing');
    message.success(response.data.message || '操作成功！');
    await loadCachedData();
  } catch (err) {
    message.error(err.response?.data?.error || '一键订阅操作失败。');
  } finally {
    isSubscribingAll.value = false;
  }
};

const triggerFullRefresh = async () => {
  isRefreshing.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'refresh-collections' });
    message.success(response.data.message || '刷新任务已在后台启动！');
  } catch (err) {
    message.error(err.response?.data?.error || '启动刷新任务失败。');
  } finally {
    isRefreshing.value = false;
  }
};

onMounted(() => {
  loadCachedData();
  observer = new IntersectionObserver((entries) => { if (entries[0].isIntersecting) loadMore(); }, { threshold: 1.0 });
  if (loaderRef.value) observer.observe(loaderRef.value);
});
onBeforeUnmount(() => { if (observer) observer.disconnect(); });
watch(loaderRef, (newEl) => { if (observer && newEl) observer.observe(newEl); });
watch(isTaskRunning, (isRunning, wasRunning) => {
  if (wasRunning && !isRunning) {
    const lastAction = props.taskStatus.last_action;
    if (lastAction && lastAction.includes('合集')) {
      message.info('后台合集任务已结束，正在刷新数据...');
      loadCachedData();
    }
  }
});

watch([searchQuery, filterStatus, sortKey, sortOrder], () => {
  displayCount.value = 50;
  selectedCollectionIds.value = [];
  lastSelectedIndex.value = null;
});

const openMissingMoviesModal = (collection) => {
  selectedCollection.value = collection;
  showModal.value = true;
};

const updateMovieSubscriptionStatus = async (movie, newStatus) => {
  try {
    await axios.post('/api/subscription/request', {
      tmdb_id: movie.tmdb_id,
      item_type: 'Movie',
      source: { type: 'manual_request', user: 'admin' } // 假设是管理员手动操作
    });
    movie.status = 'subscribed';
    message.success(`《${movie.title}》已加入订阅队列`);
  } catch (err) {
    message.error(err.response?.data?.error || '操作失败');
  }
};

const cancelMovieSubscription = async (movie) => {
  try {
    await axios.post('/api/subscription/cancel', {
      tmdb_id: movie.tmdb_id,
      item_type: 'Movie'
    });
    movie.status = 'missing';
    message.success(`已取消对《${movie.title}》的订阅`);
  } catch (err) {
    message.error(err.response?.data?.error || '操作失败');
  }
};

const subscribeMovie = async (movie) => {
  subscribing.value[movie.tmdb_id] = true;
  try {
    await updateMovieSubscriptionStatus(movie, 'WANTED');
  } finally {
    subscribing.value[movie.tmdb_id] = false;
  }
};

const unsubscribeMovie = (movie) => {
  cancelMovieSubscription(movie);
};

const ignoreMovie = async (movie) => {
  // 使用 dialog 再次确认，防止误操作
  dialog.warning({
    title: '确认忽略',
    content: `确定要忽略《${movie.title}》吗？忽略后，它将不会再出现在任何缺失列表中。`,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        // 调用我们刚刚创建的后端 API
        await axios.post('/api/subscription/ignore', {
          tmdb_id: movie.tmdb_id,
          item_type: 'Movie'
        });
        
        // 从列表中移除，实现即时反馈
        const index = selectedCollection.value.movies.indexOf(movie);
        if (index > -1) {
          selectedCollection.value.movies.splice(index, 1);
        }
        
        message.success(`已忽略《${movie.title}》`);
      } catch (err) {
        message.error(err.response?.data?.error || '忽略操作失败');
      }
    }
  });
};

const getEmbyUrl = (itemId) => {
  const embyServerUrl = configModel.value?.emby_server_url;
  const serverId = configModel.value?.emby_server_id;
  if (!embyServerUrl || !itemId) return '#';
  const baseUrl = embyServerUrl.endsWith('/') ? embyServerUrl.slice(0, -1) : embyServerUrl;
  let finalUrl = `${baseUrl}/web/index.html#!/item?id=${itemId}`;
  if (serverId) { finalUrl += `&serverId=${serverId}`; }
  return finalUrl;
};
const openInEmby = (itemId) => {
  const url = getEmbyUrl(itemId);
  if (url !== '#') { window.open(url, '_blank'); }
};

// ★★★ 额外修复：修正时间戳格式化函数 ★★★
const formatTimestamp = (timestamp) => {
  if (!timestamp) return '从未';
  try {
    // 后端现在返回 ISO 字符串, new Date() 可以直接解析
    return format(new Date(timestamp), 'MM-dd HH:mm');
  } catch (e) {
    return 'N/A';
  }
};

const getCollectionPosterUrl = (posterPath) => {
  if (!posterPath) {
    return '/img/poster-placeholder.png';
  }
  // 1. 构建完整的 TMDB 图片 URL
  const fullTmdbUrl = `https://image.tmdb.org/t/p/w300${posterPath}`;
  // 2. 使用通用的、正确的代理接口来请求这个 URL
  return `/api/image_proxy?url=${encodeURIComponent(fullTmdbUrl)}`;
};
const getTmdbImageUrl = (posterPath) => posterPath ? `https://image.tmdb.org/t/p/w300${posterPath}` : '/img/poster-placeholder.png';

const getStatusTagType = (collection) => {
  if (collection.status === 'unlinked' || collection.status === 'tmdb_error') return 'error';
  if (collection.has_missing) return 'warning';
  if (getMovieCountByStatus(collection, 'subscribed') > 0) return 'default';
  if (getMovieCountByStatus(collection, 'unreleased') > 0) return 'info';
  return 'success';
};

const getFullStatusText = (collection) => {
  if (collection.status === 'unlinked') return '未关联TMDb';
  if (collection.status === 'tmdb_error') return 'TMDb错误';
  
  const missingCount = collection.missing_count || 0;
  if (missingCount > 0) {
    return `缺失 ${missingCount} 部`;
  }

  const parts = [];
  const inLibraryCount = collection.in_library_count || 0;
  const unreleasedCount = getMovieCountByStatus(collection, 'unreleased');
  const subscribedCount = getMovieCountByStatus(collection, 'subscribed');
  
  if (inLibraryCount > 0) parts.push(`已入库 ${inLibraryCount} 部`);
  if (unreleasedCount > 0) parts.push(`未上映 ${unreleasedCount} 部`);
  if (subscribedCount > 0) parts.push(`已订阅 ${subscribedCount} 部`);
  
  return parts.join(' | ') || '已完整';
};

const getShortStatusText = (collection) => {
  if (collection.status === 'unlinked') return '未关联TMDb';
  if (collection.status === 'tmdb_error') return 'TMDb错误';

  const missingCount = collection.missing_count || 0;
  if (missingCount > 0) {
    return `缺失 ${missingCount} 部`;
  }

  const subscribedCount = getMovieCountByStatus(collection, 'subscribed');
  if (subscribedCount > 0) {
    return `已订阅 ${subscribedCount} 部`;
  }

  const unreleasedCount = getMovieCountByStatus(collection, 'unreleased');
  if (unreleasedCount > 0) {
    return `未上映 ${unreleasedCount} 部`;
  }

  const inLibraryCount = collection.in_library_count || 0;
  return `已入库 ${inLibraryCount} 部`;
};

const isTooltipNeeded = (collection) => getFullStatusText(collection) !== getShortStatusText(collection);
const extractYear = (dateStr) => {
  if (!dateStr) return null;
  return dateStr.substring(0, 4);
};
</script>

<style scoped>
.collections-page { padding: 0 10px; }
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 200px); }

.series-card {
  position: relative;
  transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
}
.series-card.selected {
  transform: translateY(-4px);
  box-shadow: 0 4px 12px 0 var(--n-color-target);
}
.card-checkbox {
  position: absolute;
  top: 8px;
  left: 8px;
  z-index: 10;
  background-color: rgba(255, 255, 255, 0.7);
  border-radius: 50%;
  padding: 4px;
  --n-color-checked: var(--n-color-primary-hover);
  --n-border-radius: 50%;
  opacity: 0;
  visibility: hidden;
  transition: opacity 0.2s ease-in-out, visibility 0.2s ease-in-out;
}
.series-card:hover .card-checkbox,
.series-card.selected .card-checkbox {
  opacity: 1;
  visibility: visible;
}

.card-poster-container { flex-shrink: 0; width: 120px; height: 180px; }
.card-poster { width: 100%; height: 100%; }
.poster-placeholder { display: flex; align-items: center; justify-content: center; width: 100%; height: 100%; background-color: var(--n-action-color); }
.card-content-container { flex-grow: 1; display: flex; flex-direction: column; padding: 12px 12px 12px 0; min-width: 0; }
.card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; flex-shrink: 0; }
.card-title { font-weight: 600; font-size: 1.1em; line-height: 1.3; }
.card-status-area { flex-grow: 1; padding-top: 8px; }
.last-checked-text { display: block; font-size: 0.8em; margin-top: 6px; }
.card-actions { border-top: 1px solid var(--n-border-color); padding-top: 8px; margin-top: 8px; display: flex; justify-content: space-around; align-items: center; flex-shrink: 0; }
.modal-header { display: flex; justify-content: space-between; align-items: center; width: 100%; }
.movie-card { overflow: hidden; border-radius: 8px; }
.movie-poster { width: 100%; height: auto; aspect-ratio: 2 / 3; object-fit: cover; background-color: #eee; }
.movie-info { padding: 8px; text-align: center; height: 70px; display: flex; align-items: center; justify-content: center; }
.movie-title {
  font-weight: bold;
  max-width: 100%;
  word-break: break-word;
  white-space: normal;
  line-height: 1.3;
}
.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}
.series-card.dashboard-card > :deep(.n-card__content) {
  flex-direction: row !important;
  justify-content: flex-start !important;
  padding: 12px !important;
  gap: 12px !important;
}
</style>