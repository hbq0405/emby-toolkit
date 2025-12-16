<!-- src/components/CollectionsPage.vue -->
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
            <div style="display: flex; align-items: center; margin-right: 12px;">
              <n-tooltip trigger="hover">
                <template #trigger>
                  <div style="display: flex; align-items: center;">
                    <n-text depth="3" style="margin-right: 8px; font-size: 12px;">自动发现合集</n-text>
                    <n-switch 
                      :value="autoCompleteEnabled" 
                      @update:value="handleAutoCompleteChange" 
                      :loading="isUpdatingSettings"
                      size="small"
                    >
                      <template #checked>开启</template>
                      <template #unchecked>关闭</template>
                    </n-switch>
                  </div>
                </template>
                开启后，当单部电影入库时，会自动检测其所属系列，<br>并自动订阅该系列中缺失的其他影片。
              </n-tooltip>
            </div>
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
          <li>点击 <n-icon :component="SyncOutline" /> 可扫描Emby所有原生合集并显示缺失。</li>
        </n-alert>
      </n-page-header>

      <!-- 排序和筛选控件 -->
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
        
        <!-- 合集卡片 -->
        <div class="responsive-grid">
          <div 
            v-for="(item, i) in renderedCollections" 
            :key="item.emby_collection_id"
            class="grid-item"
          >
            <n-card 
              class="dashboard-card series-card" 
              :bordered="false" 
              hoverable
              @click="openMissingMoviesModal(item)"
            >
              
              <!-- ★★★ 内部布局：左右结构 ★★★ -->
              <div class="card-inner-layout">
                
                <!-- 左侧：海报 -->
                <div class="card-poster-container">
                  <n-image lazy :src="getCollectionPosterUrl(item.poster_path)" class="card-poster" object-fit="cover">
                    <template #placeholder><div class="poster-placeholder"><n-icon :component="AlbumsIcon" size="32" /></div></template>
                  </n-image>
                </div>

                <!-- 右侧：内容 -->
                <div class="card-content-container">
                  <div class="card-header">
                    <n-ellipsis class="card-title" :tooltip="{ style: { maxWidth: '300px' } }">{{ item.name }}</n-ellipsis>
                  </div>

                  <!-- 统计数据展示区 -->
                  <div class="card-stats-grid">
                    <div class="stat-item missing" v-if="(item.statistics?.missing || 0) > 0">
                      <span class="stat-label">缺失</span>
                      <span class="stat-value">{{ item.statistics.missing }}</span>
                    </div>
                    <div class="stat-item in-library" v-if="(item.statistics?.in_library || 0) > 0">
                      <span class="stat-label">入库</span>
                      <span class="stat-value">{{ item.statistics.in_library }}</span>
                    </div>
                    <div class="stat-item subscribed" v-if="(item.statistics?.subscribed || 0) > 0">
                      <span class="stat-label">订阅</span>
                      <span class="stat-value">{{ item.statistics.subscribed }}</span>
                    </div>
                    <div class="stat-item unreleased" v-if="(item.statistics?.unreleased || 0) > 0">
                      <span class="stat-label">未映</span>
                      <span class="stat-value">{{ item.statistics.unreleased }}</span>
                    </div>
                    <div class="stat-item complete" v-if="(item.statistics?.missing || 0) === 0 && (item.statistics?.unreleased || 0) === 0 && (item.statistics?.subscribed || 0) === 0 && (item.statistics?.in_library || 0) > 0">
                      <n-icon :component="CheckmarkCircle" /> <span style="margin-left: 4px;">已完整</span>
                    </div>
                  </div>

                  <div class="card-status-area">
                    <n-text :depth="3" class="last-checked-text">上次检查: {{ formatTimestamp(item.last_checked_at) }}</n-text>
                  </div>

                  <!-- 底部按钮 -->
                  <div class="card-actions">
                    <n-tooltip>
                      <template #trigger>
                        <n-button type="primary" ghost size="small" @click.stop="() => openMissingMoviesModal(item)">
                          <template #icon><n-icon :component="EyeIcon" /></template>
                          详情
                        </n-button>
                      </template>
                      查看详情
                    </n-tooltip>
                    
                    <!-- 外部链接按钮需要 @click.stop 防止触发卡片点击 -->
                    <n-tooltip><template #trigger><n-button text @click.stop="openInEmby(item.emby_collection_id)"><template #icon><n-icon :component="EmbyIcon" size="18" /></template></n-button></template>在 Emby 中打开</n-tooltip>
                    <n-tooltip><template #trigger><n-button text tag="a" :href="`https://www.themoviedb.org/collection/${item.tmdb_collection_id}`" target="_blank" :disabled="!item.tmdb_collection_id" @click.stop><template #icon><n-icon :component="TMDbIcon" size="18" /></template></n-button></template>在 TMDb 中打开</n-tooltip>
                    <n-popconfirm @positive-click="handleDeleteCollection(item)" @click.stop>
                      <template #trigger>
                        <n-button text type="error" @click.stop>
                          <template #icon><n-icon :component="TrashIcon" size="18" /></template>
                        </n-button>
                      </template>
                      <div style="max-width: 240px;">
                        <p style="margin-bottom: 5px; font-weight: bold;">确定要删除此合集吗？</p>
                        <p style="font-size: 12px; color: gray;">这将清空合集内的所有影片关联，并从 Emby 中永久删除该合集条目。（不会删除影片文件）</p>
                      </div>
                    </n-popconfirm>
                  </div>
                </div>
              </div>
            </n-card>
          </div>
        </div>

        <div ref="loaderRef" class="loader-trigger">
          <n-spin v-if="hasMore" size="small" />
        </div>

      </div>
      <div v-else class="center-container"><n-empty :description="emptyStateDescription" size="huge" /></div>
    </div>

    <!-- 详情模态框 (保持不变) -->
    <n-modal 
      v-model:show="showModal" 
      preset="card" 
      style="width: 90%; max-width: 1200px; height: 80vh;" 
      content-style="padding: 0; overflow: hidden; display: flex; flex-direction: column;"
      :title="selectedCollection ? `详情 - ${selectedCollection.name}` : ''" 
      :bordered="false" 
      size="huge"
    >
      <!-- ... (模态框内容保持不变，包含之前的 Tab 样式修复) ... -->
      <div class="dashboard-card" v-if="selectedCollection" style="display: flex; flex-direction: column; height: 100%;">
        <n-tabs 
          type="line" 
          animated 
          style="height: 100%; display: flex; flex-direction: column;" 
        >
          <!-- 缺失影片 Tab -->
          <n-tab-pane name="missing" :tab="`缺失影片 (${missingMoviesInModal.length})`">
            <n-empty v-if="missingMoviesInModal.length === 0" description="太棒了！没有已上映的缺失影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="movie in missingMoviesInModal" :key="movie.tmdb_id">
                <div class="movie-card">
                  <div class="status-badge missing">待订阅</div>
                  <n-image lazy :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" object-fit="cover" preview-disabled>
                    <template #placeholder><div class="poster-placeholder"><n-icon :component="AlbumsIcon" size="32" /></div></template>
                  </n-image>
                  <div class="movie-info-overlay">
                    <div class="movie-title">{{ movie.title }}</div>
                    <div class="movie-year">{{ extractYear(movie.release_date) || '未知年份' }}</div>
                  </div>
                  <div class="movie-actions-overlay">
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <n-button circle color="#ffffff" text-color="#000000" tag="a" :href="`https://www.themoviedb.org/movie/${movie.tmdb_id}`" target="_blank">
                          <template #icon><n-icon :component="SearchIcon" /></template>
                        </n-button>
                      </template>
                      在 TMDb 查看
                    </n-tooltip>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>
          
          <!-- 已入库 Tab -->
          <n-tab-pane name="in_library" :tab="`已入库 (${inLibraryMoviesInModal.length})`">
             <n-empty v-if="inLibraryMoviesInModal.length === 0" description="该合集在媒体库中没有任何影片。" style="margin-top: 40px;"></n-empty>
             <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="movie in inLibraryMoviesInModal" :key="movie.tmdb_id">
                <div class="movie-card">
                  <div class="status-badge in_library">已入库</div>
                  <n-image lazy :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" object-fit="cover" preview-disabled />
                  <div class="movie-info-overlay">
                    <div class="movie-title">{{ movie.title }}</div>
                    <div class="movie-year">{{ extractYear(movie.release_date) || '未知年份' }}</div>
                  </div>
                  <div class="movie-actions-overlay">
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <n-button 
                          v-if="movie.emby_id"
                          circle color="#ffffff" text-color="#000000" tag="a" :href="getEmbyUrl(movie.emby_id)" target="_blank"
                        >
                          <template #icon><n-icon :component="EmbyIcon" /></template>
                        </n-button>
                        <n-button 
                          v-else
                          circle color="#ffffff" text-color="#000000" tag="a" :href="`https://www.themoviedb.org/movie/${movie.tmdb_id}`" target="_blank"
                        >
                          <template #icon><n-icon :component="SearchIcon" /></template>
                        </n-button>
                      </template>
                      {{ movie.emby_id ? '在 Emby 中查看' : '在 TMDb 查看' }}
                    </n-tooltip>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 未上映 Tab -->
          <n-tab-pane name="unreleased" :tab="`未上映 (${unreleasedMoviesInModal.length})`">
            <n-empty v-if="unreleasedMoviesInModal.length === 0" description="该合集没有已知的未上映影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="movie in unreleasedMoviesInModal" :key="movie.tmdb_id">
                <div class="movie-card">
                  <div class="status-badge unreleased">未上映</div>
                  <n-image lazy :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" object-fit="cover" preview-disabled />
                  <div class="movie-info-overlay">
                    <div class="movie-title">{{ movie.title }}</div>
                    <div class="movie-year">{{ extractYear(movie.release_date) || '未知年份' }}</div>
                  </div>
                  <div class="movie-actions-overlay">
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <n-button circle color="#ffffff" text-color="#000000" tag="a" :href="`https://www.themoviedb.org/movie/${movie.tmdb_id}`" target="_blank">
                          <template #icon><n-icon :component="SearchIcon" /></template>
                        </n-button>
                      </template>
                      在 TMDb 查看
                    </n-tooltip>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 已订阅 Tab -->
          <n-tab-pane name="subscribed" :tab="`已订阅 (${subscribedMoviesInModal.length})`">
            <n-empty v-if="subscribedMoviesInModal.length === 0" description="你没有订阅此合集中的任何影片。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="movie in subscribedMoviesInModal" :key="movie.tmdb_id">
                <div class="movie-card">
                  <div class="status-badge subscribed">已订阅</div>
                  <n-image lazy :src="getTmdbImageUrl(movie.poster_path)" class="movie-poster" object-fit="cover" preview-disabled />
                  <div class="movie-info-overlay">
                    <div class="movie-title">{{ movie.title }}</div>
                    <div class="movie-year">{{ extractYear(movie.release_date) || '未知年份' }}</div>
                  </div>
                  <div class="movie-actions-overlay">
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <n-button circle color="#ffffff" text-color="#000000" tag="a" :href="`https://www.themoviedb.org/movie/${movie.tmdb_id}`" target="_blank">
                          <template #icon><n-icon :component="SearchIcon" /></template>
                        </n-button>
                      </template>
                      在 TMDb 查看
                    </n-tooltip>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>
        </n-tabs>
      </div>
    </n-modal>
  </n-layout>
</template>

<script setup>
// ... (Script 部分保持不变，逻辑无需修改) ...
import { ref, onMounted, onBeforeUnmount, computed, watch, h } from 'vue';
import axios from 'axios';
import { NLayout, NPageHeader, NEmpty, NTag, NButton, NSpace, NIcon, useMessage, useDialog, NTooltip, NGrid, NGi, NCard, NImage, NEllipsis, NSpin, NAlert, NModal, NTabs, NTabPane, NPopconfirm, NCheckbox, NDropdown, NInput, NSelect, NButtonGroup } from 'naive-ui';
import { SyncOutline, AlbumsOutline as AlbumsIcon, EyeOutline as EyeIcon, CloudDownloadOutline as CloudDownloadIcon, CheckmarkCircleOutline as CheckmarkCircle, ArrowUpOutline as ArrowUpIcon, ArrowDownOutline as ArrowDownIcon, SearchOutline as SearchIcon, TrashOutline as TrashIcon, SettingsOutline as SettingsIcon } from '@vicons/ionicons5';
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
const showModal = ref(false);
const selectedCollection = ref(null);
const displayCount = ref(50);
const INCREMENT = 50;
const loaderRef = ref(null);
let observer = null;


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
  if (!Array.isArray(collections.value)) return stats;
  stats.totalCollections = collections.value.length;
  for (const collection of collections.value) {
    const s = collection.statistics || { missing: 0, unreleased: 0, subscribed: 0 };
    if (s.missing > 0) {
      stats.collectionsWithMissing++;
      stats.totalMissingMovies += s.missing;
    }
    stats.totalUnreleased += s.unreleased;
    stats.totalSubscribed += s.subscribed;
  }
  return stats;
});

const handleDeleteCollection = async (collection) => {
  if (!collection || !collection.emby_collection_id) return;
  
  const d = message.loading('正在删除合集，请稍候...', { duration: 0 });
  
  try {
    await axios.delete(`/api/collections/${collection.emby_collection_id}`);
    d.destroy();
    message.success(`合集 "${collection.name}" 删除成功！`);
    
    // 从本地列表中移除，避免需要刷新页面
    collections.value = collections.value.filter(c => c.emby_collection_id !== collection.emby_collection_id);
    
  } catch (err) {
    d.destroy();
    message.error(err.response?.data?.error || '删除失败，请查看日志。');
  }
};

const filteredAndSortedCollections = computed(() => {
  if (!Array.isArray(collections.value)) return [];
  let list = [...collections.value];
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    list = list.filter(item => item.name && item.name.toLowerCase().includes(query));
  }
  switch (filterStatus.value) {
    case 'has_missing':
      list = list.filter(item => (item.statistics?.missing || 0) > 0);
      break;
    case 'complete':
      list = list.filter(item => (item.statistics?.missing || 0) === 0);
      break;
    case 'has_subscribed':
      list = list.filter(item => (item.statistics?.subscribed || 0) > 0);
      break;
    case 'has_unreleased':
      list = list.filter(item => (item.statistics?.unreleased || 0) > 0);
      break;
  }
  list.sort((a, b) => {
    let valA, valB;
    switch (sortKey.value) {
      case 'name':
        valA = a.name || '';
        valB = b.name || '';
        return sortOrder.value === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
      case 'last_checked_at':
        valA = a.last_checked_at ? new Date(a.last_checked_at).getTime() : 0;
        valB = b.last_checked_at ? new Date(b.last_checked_at).getTime() : 0;
        break;
      case 'missing_count':
      default:
        valA = a.statistics?.missing || 0;
        valB = b.statistics?.missing || 0;
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
  return selectedCollection.value.movies.filter(movie => movie.status === 'subscribed' || movie.status === 'paused');
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
    collections.value = [];
  } finally {
    isInitialLoading.value = false;
  }
};

// ★★★ 1. 新增状态变量 ★★★
const autoCompleteEnabled = ref(false);
const isUpdatingSettings = ref(false);

// ★★★ 2. 加载设置 ★★★
const loadSettings = async () => {
  try {
    const response = await axios.get('/api/collections/settings');
    autoCompleteEnabled.value = response.data.collection_auto_complete_enabled;
  } catch (e) {
    console.error("加载合集设置失败", e);
  }
};

// ★★★ 3. 切换开关 ★★★
const handleAutoCompleteChange = async (value) => {
  isUpdatingSettings.value = true;
  try {
    await axios.post('/api/collections/settings', {
      collection_auto_complete_enabled: value
    });
    autoCompleteEnabled.value = value;
    if (value) {
      message.success("已开启自动补全：新电影入库将自动触发所属系列的订阅。");
    } else {
      message.info("已关闭自动补全。");
    }
  } catch (e) {
    message.error("保存设置失败");
    // 回滚状态
    autoCompleteEnabled.value = !value;
  } finally {
    isUpdatingSettings.value = false;
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
  loadSettings();
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
});

const openMissingMoviesModal = (collection) => {
  selectedCollection.value = collection;
  showModal.value = true;
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

const formatTimestamp = (timestamp) => {
  if (!timestamp) return '从未';
  try {
    return format(new Date(timestamp), 'MM-dd HH:mm');
  } catch (e) {
    return 'N/A';
  }
};

const getCollectionPosterUrl = (posterPath) => {
  if (!posterPath) {
    return '/img/poster-placeholder.png';
  }
  const fullTmdbUrl = `https://image.tmdb.org/t/p/w300${posterPath}`;
  return `/api/image_proxy?url=${encodeURIComponent(fullTmdbUrl)}`;
};
const getTmdbImageUrl = (posterPath) => posterPath ? `https://image.tmdb.org/t/p/w300${posterPath}` : '/img/poster-placeholder.png';

const extractYear = (dateStr) => {
  if (!dateStr) return null;
  return dateStr.substring(0, 4);
};
</script>

<style scoped>
.collections-page { padding: 0 10px; }
.center-container { display: flex; justify-content: center; align-items: center; height: calc(100vh - 200px); }

/* ★★★ Grid 布局 ★★★ */
.responsive-grid {
  display: grid;
  gap: 16px;
  /* 320px 基准宽度 */
  grid-template-columns: repeat(auto-fill, minmax(calc(320px * var(--card-scale, 1)), 1fr));
}

.grid-item {
  height: 100%;
  min-width: 0;
}

/* ★★★ 卡片容器 ★★★ */
.series-card {
  cursor: pointer;
  transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
  height: 100%;
  position: relative;
  
  /* ★★★ 核心 1：设定基准字号，所有内部元素都将基于此缩放 ★★★ */
  font-size: calc(14px * var(--card-scale, 1)); 
  
  border-radius: calc(12px * var(--card-scale, 1));
  overflow: hidden; 
  border: 1px solid var(--n-border-color);
}

.series-card:hover {
  transform: translateY(-4px);
}

/* ★★★ 核心 2：强制 Naive UI 组件跟随缩放 ★★★ */
.series-card :deep(.n-card__content),
.series-card :deep(.n-button),
.series-card :deep(.n-tag),
.series-card :deep(.n-text),
.series-card :deep(.n-ellipsis) {
  font-size: inherit !important; 
}

/* 调整图标大小以适应缩放 */
.series-card :deep(.n-icon) {
  font-size: 1.2em !important; 
}

/* 恢复内边距 */
.series-card.dashboard-card > :deep(.n-card__content) {
  padding: calc(10px * var(--card-scale, 1)) !important; 
  display: flex !important;
  flex-direction: column !important;
  height: 100% !important;
}

/* ★★★ 内部布局：左右拉伸 ★★★ */
.card-inner-layout {
  display: flex;
  flex-direction: row;
  height: 100%;
  width: 100%;
  align-items: stretch; 
  gap: calc(12px * var(--card-scale, 1));
}

/* ★★★ 海报区域 ★★★ */
.card-poster-container {
  flex-shrink: 0; 
  width: calc(130px * var(--card-scale, 1));
  height: auto; 
  min-height: 100%; 
  position: relative;
  background-color: rgba(0,0,0,0.1);
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

.card-poster {
  width: 100%;
  height: 100%;
  display: block;
}

.card-poster :deep(img) {
  width: 100%;
  height: 100%;
  object-fit: cover !important; 
  display: block;
  border-radius: 0 !important;
}

.poster-placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  background-color: var(--n-action-color);
  color: var(--n-text-color-disabled);
}

/* ★★★ 内容区域 ★★★ */
.card-content-container {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-width: 0;
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: calc(4px * var(--card-scale, 1));
}

.card-title {
  font-weight: 600;
  font-size: 1.1em !important; 
  line-height: 1.3;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

/* 统计数据网格 */
.card-stats-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
  margin-bottom: 4px;
}

.stat-item {
  display: flex;
  align-items: center;
  font-size: 0.9em !important; /* 跟随缩放 */
  padding: 2px 8px;
  border-radius: 4px;
  font-weight: 500;
  line-height: 1.5;
}

.stat-label { margin-right: 4px; opacity: 0.8; }
.stat-value { font-weight: bold; }

.stat-item.missing { background-color: rgba(208, 48, 80, 0.1); color: #d03050; }
.stat-item.in-library { background-color: rgba(24, 160, 88, 0.1); color: #18a058; }
.stat-item.subscribed { background-color: rgba(240, 160, 32, 0.1); color: #f0a020; }
.stat-item.unreleased { background-color: rgba(32, 128, 240, 0.1); color: #2080f0; }
.stat-item.complete { background-color: rgba(24, 160, 88, 0.1); color: #18a058; width: 100%; justify-content: center; }

.card-status-area {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  justify-content: flex-end; /* 底部对齐 */
  gap: 4px;
}

.last-checked-text {
  font-size: 0.85em !important;
  opacity: 0.8;
}

/* ★★★ 底部按钮区域 ★★★ */
.card-actions {
  margin-top: auto; 
  padding-top: calc(8px * var(--card-scale, 1));
  border-top: 1px solid var(--n-border-color);
  display: flex;
  justify-content: space-around; 
  align-items: center;
  gap: calc(4px * var(--card-scale, 1));
}

.card-actions :deep(.n-button) {
  padding: 0 6px;
  height: 24px;
  font-size: 0.9em !important;
}

.loader-trigger {
  height: 50px;
  display: flex;
  justify-content: center;
  align-items: center;
}

/* 手机端适配 */
@media (max-width: 600px) {
  .responsive-grid { grid-template-columns: 1fr !important; }
  .card-poster-container { width: 100px; min-height: 150px; }
}

/* =========================================
   ▼▼▼ 模态框内的电影海报墙样式 ▼▼▼
   ========================================= */

/* 1. 强制 Tabs 的内容包装器占满剩余高度 */
:deep(.n-tabs-pane-wrapper) {
  flex: 1;
  overflow: hidden;
}

/* 2. 给滚动区域增加内边距 */
:deep(.n-tab-pane) {
  height: 100%;
  overflow-y: auto;
  padding: 20px 24px !important; 
  box-sizing: border-box;
}

/* 3. 滚动条样式 */
:deep(.n-tab-pane)::-webkit-scrollbar {
  width: 6px;
}
:deep(.n-tab-pane)::-webkit-scrollbar-thumb {
  background-color: rgba(255, 255, 255, 0.2);
  border-radius: 3px;
}
:deep(.n-tab-pane)::-webkit-scrollbar-track {
  background-color: transparent;
}

/* 卡片容器：强制 2:3 比例 */
.movie-card {
  border-radius: 8px;
  overflow: hidden;
  position: relative;
  aspect-ratio: 2 / 3; 
  background-color: #202023;
  transition: transform 0.2s, box-shadow 0.2s;
  cursor: default;
}

.movie-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5);
  z-index: 2;
}

.movie-poster {
  width: 100%;
  height: 100%;
  display: block;
}
.movie-poster :deep(img) {
  width: 100%;
  height: 100%;
  object-fit: cover;
  transition: transform 0.3s;
}
.movie-card:hover .movie-poster :deep(img) {
  transform: scale(1.05);
}

/* 底部渐变遮罩 */
.movie-info-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 60px 10px 10px 10px;
  background: linear-gradient(to top, rgba(0, 0, 0, 0.95) 0%, rgba(0, 0, 0, 0.7) 60%, transparent 100%);
  color: #fff;
  pointer-events: none;
  z-index: 10;
}

.movie-title {
  font-size: 14px;
  font-weight: bold;
  line-height: 1.3;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
}

.movie-year {
  font-size: 12px;
  color: #ddd;
  margin-top: 2px;
  font-weight: 500;
}

/* 悬停操作层 */
.movie-actions-overlay {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(2px);
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  gap: 12px;
  opacity: 0;
  transition: opacity 0.2s ease-in-out;
  z-index: 20;
}

.movie-card:hover .movie-actions-overlay {
  opacity: 1;
}

/* 左上角状态角标 */
.status-badge {
  position: absolute;
  top: 10px;
  left: -30px;
  width: 100px;
  height: 24px;
  background-color: #666;
  color: #fff;
  font-size: 12px;
  font-weight: bold;
  display: flex;
  align-items: center;
  justify-content: center;
  transform: rotate(-45deg);
  box-shadow: 0 2px 4px rgba(0,0,0,0.3);
  z-index: 15;
  pointer-events: none;
}

.status-badge.in_library { background-color: #63e2b7; color: #000; }
.status-badge.missing { background-color: #e88080; }
.status-badge.subscribed { background-color: #f2c97d; color: #000; }
.status-badge.unreleased { background-color: #8a8a8a; }
</style>