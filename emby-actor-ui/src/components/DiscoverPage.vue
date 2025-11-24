<!-- src/components/DiscoverPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
  <div>
    <n-page-header title="影视探索" subtitle="发现您感兴趣的下一部作品" />
      <n-grid :x-gap="24" :y-gap="24" cols="2" style="margin-top: 24px;">
        <!-- 左侧筛选面板 (占1列) -->
        <n-gi :span="1">
          <n-card :bordered="false" class="dashboard-card">
            <template #header>
              <span class="card-title">筛选条件</span>
            </template>
            <n-space vertical size="large">
              <n-space align="center">
                <label>搜索:</label>
                <n-input
                  v-model:value="searchQuery"
                  placeholder="输入片名搜索..."
                  clearable
                  style="min-width: 300px;"
                />
              </n-space>
              <n-space align="center">
                <label>类型:</label>
                <n-radio-group v-model:value="mediaType" :disabled="isSearchMode">
                  <n-radio-button value="movie" label="电影" />
                  <n-radio-button value="tv" label="电视剧" />
                </n-radio-group>
              </n-space>
              <n-space align="center">
                <label>排序:</label>
                <n-radio-group v-model:value="filters['sort_by']" :disabled="isSearchMode">
                  <n-radio-button value="popularity.desc" label="热度降序" />
                  <n-radio-button value="popularity.asc" label="热度升序" />
                  <n-radio-button :value="mediaType === 'movie' ? 'primary_release_date.desc' : 'first_air_date.desc'" label="上映日期降序" />
                  <n-radio-button :value="mediaType === 'movie' ? 'primary_release_date.asc' : 'first_air_date.asc'" label="上映日期升序" />
                  <n-radio-button value="vote_average.desc" label="评分降序" />
                  <n-radio-button value="vote_average.asc" label="评分升序" />
                </n-radio-group>
              </n-space>
              <n-space align="center">
                <label>风格:</label>
                <!-- 新增的“包含/排除”切换器 -->
                <n-radio-group v-model:value="genreFilterMode" :disabled="isSearchMode">
                  <n-radio-button value="include" label="包含" />
                  <n-radio-button value="exclude" label="排除" />
                </n-radio-group>
                <n-select
                  v-model:value="selectedGenres"
                  :disabled="isSearchMode"
                  multiple
                  filterable
                  :placeholder="genreFilterMode === 'include' ? '选择要包含的风格' : '选择要排除的风格'"
                  :options="genreOptions"
                  style="min-width: 300px;"
                />
              </n-space>
              <n-space align="center">
              <label>地区:</label>
              <n-select
                  v-model:value="selectedRegions"
                  :disabled="isSearchMode"
                  multiple
                  filterable
                  placeholder="选择国家/地区"
                  :options="countryOptions"
                  style="min-width: 300px;"
              />
              </n-space>
              <n-space align="center">
                <label>语言:</label>
                <n-select
                  v-model:value="selectedLanguage"
                  :disabled="isSearchMode"
                  filterable
                  clearable
                  placeholder="选择原始语言"
                  :options="languageOptions"
                  style="min-width: 300px;"
                />
              </n-space>
              <n-space align="center">
                <label>发行年份:</label>
                <n-input-group>
                  <n-input-number
                    v-model:value="yearFrom"
                    :disabled="isSearchMode"
                    :show-button="false"
                    placeholder="从 (例如 1990)"
                    clearable
                    style="width: 150px;"
                  />
                  <n-input-number
                    v-model:value="yearTo"
                    :disabled="isSearchMode"
                    :show-button="false"
                    placeholder="到 (例如 1999)"
                    clearable
                    style="width: 150px;"
                  />
                </n-input-group>
              </n-space>
              <n-space align="center">
                <label>关键词:</label>
                <n-select
                  v-model:value="selectedKeywords"
                  :disabled="isSearchMode"
                  multiple
                  filterable
                  placeholder="选择关键词"
                  :options="keywordOptions"
                  style="min-width: 300px;"
                />
              </n-space>
              <n-space align="center">
                <label>评分不低于:</label>
                <n-input-number
                  v-model:value="filters.vote_average_gte"
                  :disabled="isSearchMode"
                  :step="0.5"
                  :min="0"
                  :max="10"
                  placeholder="最低评分"
                  style="width: 120px;"
                />
              </n-space>
            </n-space>
          </n-card>
        </n-gi>
        <!-- ★★★ 右侧“每日推荐”面板 ★★★ -->
        <n-gi :span="1">
          <n-card :bordered="false" class="dashboard-card recommendation-card">
            <!-- ★ 1. 修改卡片头，加入“换一个”按钮 -->
            <template #header>
              <span class="card-title">
                {{ recommendationThemeName === '每日推荐' ? '每日推荐' : `今日主题：${recommendationThemeName}` }} ✨
              </span>
            </template>
            <template #header-extra>
              <n-tooltip trigger="hover">
                <template #trigger>
                  <n-button circle size="small" @click="pickRandomRecommendation">
                    <template #icon><n-icon :component="DiceIcon" /></template>
                  </n-button>
                </template>
                换一个
              </n-tooltip>
            </template>
            <n-skeleton v-if="isPoolLoading" text :repeat="8" />
              <div v-if="!isPoolLoading && currentRecommendation" class="recommendation-content">
                <!-- 新的布局容器 -->
                <div class="recommendation-grid">
                    <!-- ★ 左栏：海报 -->
                    <div class="poster-column">
                        <img :src="`https://image.tmdb.org/t/p/w500${currentRecommendation.poster_path}`" class="recommendation-poster" />
                    </div>

                    <!-- ★ 右栏：所有信息 -->
                    <div class="details-column">
                        <!-- 标题 -->
                        <n-h3 style="margin-top: 0; margin-bottom: 8px;">{{ currentRecommendation.title }}</n-h3>
                        
                        <!-- 评分和年份 -->
                        <n-space align="center" size="small" style="color: #888; margin-bottom: 16px;">
                            <n-icon :component="StarIcon" color="#f7b824" />
                            <span>{{ currentRecommendation.vote_average?.toFixed(1) }}</span>
                            <span>·</span>
                            <span>{{ new Date(currentRecommendation.release_date).getFullYear() }}</span>
                        </n-space>

                        <!-- 简介 -->
                        <n-ellipsis :line-clamp="4" :tooltip="false" class="overview-text">
                            {{ currentRecommendation.overview }}
                        </n-ellipsis>

                        <!-- “想看这个”按钮 -->
                        <n-button 
                          type="primary" 
                          block 
                          @click="handleSubscribe(currentRecommendation)" 
                          :loading="subscribingId === currentRecommendation.id"
                          :disabled="isTaskRunning"
                          style="margin-top: 24px;"
                        >
                          <template #icon><n-icon :component="HeartOutline" /></template>
                          {{ isTaskRunning ? '后台任务处理中...' : '想看这个' }}
                        </n-button>
                    </div>
                </div>

                <!-- 演员列表 (现在放在布局容器下方) -->
                <div v-if="currentRecommendation.cast && currentRecommendation.cast.length > 0">
                    <n-divider style="margin-top: 24px; margin-bottom: 16px;" />
                    <n-h4 style="margin: 0 0 16px 0;">主要演员</n-h4>
                    <div class="actor-list-container">
                        <div v-for="actor in currentRecommendation.cast" :key="actor.id" class="actor-card">
                            <img 
                            :src="actor.profile_path ? `https://image.tmdb.org/t/p/w185${actor.profile_path}` : '/default-avatar.png'" 
                            class="actor-avatar"
                            @error="onImageError"
                            />
                            <div class="actor-name">{{ actor.name }}</div>
                            <div class="actor-character">{{ actor.character }}</div>
                        </div>
                    </div>
                </div>
              </div>
            <n-empty v-if="!isRecommendationLoading && !currentRecommendation" description="太棒了！热门电影似乎都在您的库中，今日无特别推荐。" />
          </n-card>
        </n-gi>
      </n-grid>

    <!-- 结果展示区域 -->
    <n-spin :show="loading && results.length === 0">
      <n-grid :x-gap="16" :y-gap="24" responsive="screen" cols="10" style="margin-top: 24px;">
        <n-gi v-for="media in results" :key="media.id">
          <n-card class="dashboard-card media-card" content-style="padding: 0; position: relative; overflow: hidden;" @click="handleClickCard(media)">
            <img :src="media.poster_path ? `https://image.tmdb.org/t/p/w500${media.poster_path}` : '/path/to/default/image.png'" class="media-poster">
            <div v-if="media.in_library" class="ribbon"><span>已入库</span></div>
            <div v-if="media.vote_average" class="rating-badge">
              {{ media.vote_average.toFixed(1) }}
            </div>
            <div class="hover-overlay">
              <div class="overlay-info">
                <span class="media-title">{{ media.title || media.name }}</span>
                <span class="media-year">{{ getYear(media) }}</span>
              </div>
              <div v-if="!media.in_library" 
                  class="action-icon" 
                  :class="{ 'is-disabled': isTaskRunning }"
                  @click.stop="handleSubscribe(media)">
                <n-spin :show="subscribingId === media.id" size="small">
                  <n-icon size="24">
                    
                    <!-- 1. 沙漏图标 (不可点击状态) -->
                    <!-- 条件：已订阅 OR 待上映 OR (普通用户 AND (已请求 OR 想看)) -->
                    <HourglassOutline 
                      v-if="['SUBSCRIBED', 'PENDING_RELEASE'].includes(media.subscription_status) || 
                            (!isPrivilegedUser && ['REQUESTED', 'WANTED'].includes(media.subscription_status))" 
                      color="#888" 
                      style="cursor: not-allowed;" 
                    />

                    <!-- 2. 闪电图标 (管理员可操作状态) -->
                    <!-- 条件：管理员 AND (想看 OR 已请求) -->
                    <LightningIcon 
                      v-else-if="isPrivilegedUser && ['WANTED', 'REQUESTED'].includes(media.subscription_status)" 
                      color="#f0a020" 
                    />

                    <!-- 3. 爱心图标 (默认可订阅状态) -->
                    <HeartOutline v-else />

                  </n-icon>
                </n-spin>
              </div>
            </div>
          </n-card>
        </n-gi>
      </n-grid>
    </n-spin>

    <div v-if="isLoadingMore" style="text-align: center; padding: 20px;">
      <n-spin size="medium" />
    </div>
    <div v-if="results.length > 0 && filters.page >= totalPages" style="text-align: center; padding: 20px; color: #888;">
      已经到底啦~
    </div>

    <!-- ★★★ 核心改造 1: 添加 IntersectionObserver 的“哨兵”元素 ★★★ -->
    <div ref="sentinel" style="height: 50px;"></div>

  </div>
  </n-layout>
</template>

<script setup>
import { ref, reactive, watch, onMounted, onUnmounted, computed } from 'vue';
import { useRouter } from 'vue-router';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { 
  NPageHeader, NCard, NSpace, NRadioGroup, NRadioButton, NSelect,
  NInputNumber, NSpin, NGrid, NGi, NButton, NThing, useMessage, NIcon, 
  NInput, NInputGroup, NSkeleton, NEllipsis, NEmpty, NDivider, NH4, NH3, NTooltip
} from 'naive-ui';
import { Heart, HeartOutline, HourglassOutline, Star as StarIcon, FlashOutline as LightningIcon, DiceOutline as DiceIcon } from '@vicons/ionicons5';

// ... (所有顶部的 import 和 ref 定义保持不变) ...
const authStore = useAuthStore();
const message = useMessage();
const router = useRouter(); 
const isPrivilegedUser = computed(() => {
  return authStore.isAdmin || authStore.user?.allow_unrestricted_subscriptions;
});
const embyServerUrl = ref('');
const embyServerId = ref('');
const loading = ref(false);
// ★★★ 接收全局任务状态 ★★★
const props = defineProps({
  taskStatus: Object
});

const isTaskRunning = computed(() => props.taskStatus && props.taskStatus.is_running);
const subscribingId = ref(null);
const mediaType = ref('movie');
const genres = ref([]);
const selectedGenres = ref([]);
const countryOptions = ref([]); 
const selectedRegions = ref([]);
const languageOptions = ref([]);
const selectedLanguage = ref(null);
const keywordOptions = ref([]); 
const selectedKeywords = ref([]); 
const genreFilterMode = ref('include'); 
const yearFrom = ref(null);
const yearTo = ref(null);
const recommendationPool = ref([]); 
const currentRecommendation = ref(null); 
const isPoolLoading = ref(true); 
const recommendationThemeName = ref('每日推荐');
const filters = reactive({
  sort_by: 'popularity.desc',
  vote_average_gte: 0,
  page: 1,
});
const results = ref([]);
const totalPages = ref(0);
const isLoadingMore = ref(false);
const searchQuery = ref('');
const isSearchMode = computed(() => searchQuery.value.trim() !== '');
const sentinel = ref(null);

// ... (所有 fetch* 和其他辅助函数保持不变) ...
const getYear = (media) => {
  const dateStr = media.release_date || media.first_air_date;
  if (!dateStr) return '';
  return new Date(dateStr).getFullYear();
};
const genreOptions = computed(() => {
  return genres.value.map(item => ({
    label: item.name, // 显示的文字 (例如: 动作)
    value: item.id    // 绑定的值 (例如: 28)
  }));
});
const fetchGenres = async () => {  
  try {
    const endpoint = mediaType.value === 'movie' 
      ? '/api/custom_collections/config/tmdb_movie_genres' 
      : '/api/custom_collections/config/tmdb_tv_genres';
    const response = await axios.get(endpoint);
    genres.value = response.data;
  } catch (error) {
    message.error('加载类型列表失败');
  }
};
const fetchCountries = async () => {  
  try {
    const response = await axios.get('/api/custom_collections/config/tmdb_countries');
    countryOptions.value = response.data;
  } catch (error) {
    message.error('加载国家列表失败');
  }
};
const fetchLanguages = async () => {
  try {
    const response = await axios.get('/api/discover/config/languages');
    languageOptions.value = response.data;
  } catch (error) {
    message.error('加载语言列表失败');
  }
};
const fetchKeywords = async () => {
  try {
    const response = await axios.get('/api/discover/config/keywords');
    keywordOptions.value = response.data;
  } catch (error) {
    message.error('加载关键词列表失败');
  }
};
const fetchDiscoverData = async () => {
  if (isLoadingMore.value || loading.value) return;
  if (filters.page === 1) { loading.value = true; } else { isLoadingMore.value = true; }
  try {
    let response;
    if (isSearchMode.value) {
      response = await axios.post('/api/discover/search', {
        query: searchQuery.value,
        media_type: mediaType.value,
        page: filters.page,
      });
    } else {
      const apiParams = {
        'sort_by': filters.sort_by,
        'page': filters.page,
        'vote_average.gte': filters.vote_average_gte,
        'with_origin_country': selectedRegions.value.join('|'),
        'with_original_language': selectedLanguage.value,
        'with_keywords': selectedKeywords.value.join(','),
      };
      if (selectedGenres.value.length > 0) {
        if (genreFilterMode.value === 'include') { apiParams.with_genres = selectedGenres.value.join(','); } 
        else { apiParams.without_genres = selectedGenres.value.join(','); }
      }
      const yearGteParam = mediaType.value === 'movie' ? 'primary_release_date.gte' : 'first_air_date.gte';
      const yearLteParam = mediaType.value === 'movie' ? 'primary_release_date.lte' : 'first_air_date.lte';
      if (yearFrom.value) { apiParams[yearGteParam] = `${yearFrom.value}-01-01`; }
      if (yearTo.value) { apiParams[yearLteParam] = `${yearTo.value}-12-31`; }
      const cleanedParams = Object.fromEntries(Object.entries(apiParams).filter(([_, v]) => v !== null && v !== ''));
      response = await axios.post(`/api/discover/${mediaType.value}`, cleanedParams);
    }
    if (filters.page === 1) { results.value = response.data.results; } 
    else { results.value.push(...response.data.results); }
    totalPages.value = response.data.total_pages;
  } catch (error) {
    message.error('加载影视数据失败');
    if (filters.page === 1) { results.value = []; }
  } finally {
    loading.value = false;
    isLoadingMore.value = false;
  }
};
const fetchEmbyConfig = async () => {
  try {
    const response = await axios.get('/api/config');
    embyServerUrl.value = response.data.emby_server_url;
    embyServerId.value = response.data.emby_server_id;
  } catch (error) {
    console.error('获取 Emby 配置失败:', error);
    message.error('获取 Emby 配置失败');
  }
};
const pickRandomRecommendation = () => {
  if (!recommendationPool.value || recommendationPool.value.length === 0) {
    currentRecommendation.value = null;
    return;
  }
  if (recommendationPool.value.length === 1) {
    currentRecommendation.value = recommendationPool.value[0];
    return;
  }
  let newRecommendation;
  do {
    const randomIndex = Math.floor(Math.random() * recommendationPool.value.length);
    newRecommendation = recommendationPool.value[randomIndex];
  } while (newRecommendation.id === currentRecommendation.value?.id);
  currentRecommendation.value = newRecommendation;
};
const fetchRecommendationPool = async () => {
  isPoolLoading.value = true;
  try {
    const response = await axios.get('/api/discover/daily_recommendation');
    recommendationPool.value = response.data.pool || [];
    recommendationThemeName.value = response.data.theme_name || '每日推荐';
    pickRandomRecommendation();
    isPoolLoading.value = false;
  } catch (error) {
    if (error.response && error.response.status === 404) {
      try {
        await axios.post('/api/discover/trigger_recommendation_update');
        let attempts = 0;
        const maxAttempts = 10;
        const pollInterval = 3000;
        const intervalId = setInterval(async () => {
          if (attempts >= maxAttempts) {
            clearInterval(intervalId);
            message.error("获取今日推荐超时，请稍后刷新。");
            isPoolLoading.value = false;
            return;
          }
          try {
            const pollResponse = await axios.get('/api/discover/daily_recommendation');
            if (pollResponse.data && pollResponse.data.pool && pollResponse.data.pool.length > 0) {
              clearInterval(intervalId);
              recommendationPool.value = pollResponse.data.pool;
              recommendationThemeName.value = pollResponse.data.theme_name;
              pickRandomRecommendation();
              isPoolLoading.value = false;
            }
          } catch (pollError) {}
          attempts++;
        }, pollInterval);
      } catch (triggerError) {
        message.error("启动推荐任务失败。");
        isPoolLoading.value = false;
      }
    } else {
      console.error('加载推荐池失败:', error);
      message.error("加载今日推荐失败。");
      isPoolLoading.value = false;
    }
  }
};

// 定义更新状态的辅助函数 (放在 handleSubscribe 上面)
const updateMediaStatus = (mediaId, newStatus) => {
  // 1. 更新结果列表 (results)
  const index = results.value.findIndex(m => m.id === mediaId);
  if (index !== -1) {
    // ★ 关键点：创建一个新对象来替换旧对象，确保 Vue 能检测到变化
    results.value[index] = { 
      ...results.value[index], 
      subscription_status: newStatus 
    };
  }

  // 2. 更新每日推荐 (currentRecommendation)
  if (currentRecommendation.value && currentRecommendation.value.id === mediaId) {
    currentRecommendation.value = {
      ...currentRecommendation.value,
      subscription_status: newStatus
    };
  }
};

const handleSubscribe = async (media) => {
  // 1. 基础拦截
  if (isTaskRunning.value) {
    message.warning('后台有任务正在运行，请稍后再试。');
    return;
  }
  if (subscribingId.value === media.id) return;

  const originalStatus = media.subscription_status || 'NONE';

  // 2. 状态拦截
  if (originalStatus === 'SUBSCRIBED' || originalStatus === 'PENDING_RELEASE') {
    return;
  }
  if (!isPrivilegedUser.value && (originalStatus === 'REQUESTED' || originalStatus === 'WANTED')) {
    return;
  }

  // 3. 乐观更新 (点击瞬间变图标)
  subscribingId.value = media.id;
  const optimisticStatus = isPrivilegedUser.value ? 'WANTED' : 'REQUESTED';
  updateMediaStatus(media.id, optimisticStatus);

  try {
    // 4. 发送请求
    const itemTypeForApi = (media.media_type === 'tv' ? 'Series' : 'Movie') || (mediaType.value === 'movie' ? 'Movie' : 'Series');
    
    const portalResponse = await axios.post('/api/portal/subscribe', {
      tmdb_id: media.id,
      item_type: itemTypeForApi,
      item_name: media.title || media.name,
    });

    message.success(portalResponse.data.message);
    
    // ★★★ 核心修复开始 ★★★
    // 不要盲目信任后端返回的 status，因为它可能是旧的或者空的。
    // 如果是普通用户，且请求成功了，那么状态一定是 'REQUESTED'。
    let finalStatus = portalResponse.data.status;

    if (!isPrivilegedUser.value) {
      // 强制修正普通用户的状态
      finalStatus = 'REQUESTED';
    } else {
      // 特权用户如果后端没返回有效状态，兜底为 WANTED
      if (!finalStatus || finalStatus === 'NONE') {
        finalStatus = 'WANTED';
      }
    }
    
    // 应用最终状态
    updateMediaStatus(media.id, finalStatus);
    // ★★★ 核心修复结束 ★★★

    // 5. (仅管理员) 立即触发后台任务
    const shouldTriggerTaskImmediately = isPrivilegedUser.value && ['WANTED', 'REQUESTED', 'NONE', 'IGNORED'].includes(originalStatus);
    
    if (shouldTriggerTaskImmediately) {
      message.info('已提交到后台立即处理...');
      const requestItem = { tmdb_id: media.id, item_type: itemTypeForApi, title: media.title || media.name };
      const taskPayload = { task_name: 'manual_subscribe_batch', subscribe_requests: [requestItem] };
      
      // 再次更新为已订阅
      updateMediaStatus(media.id, 'SUBSCRIBED');
      
      axios.post('/api/tasks/run', taskPayload)
        .catch(taskError => {
          // 任务失败回滚到上一步的状态
          updateMediaStatus(media.id, finalStatus); 
          message.error(taskError.response?.data?.message || '提交立即处理任务失败。');
        });
    }

    // 移除每日推荐
    if (currentRecommendation.value && currentRecommendation.value.id === media.id) {
      const poolIndex = recommendationPool.value.findIndex(item => item.id === media.id);
      if (poolIndex !== -1) { recommendationPool.value.splice(poolIndex, 1); }
      pickRandomRecommendation();
    }

  } catch (error) {
    // 6. 错误回滚
    console.error(error);
    updateMediaStatus(media.id, originalStatus);
    message.error(error.response?.data?.message || '提交请求失败');
  } finally {
    subscribingId.value = null;
  }
};

// ... (所有剩余的辅助函数和生命周期钩子保持不变) ...
const onImageError = (e) => { e.target.src = '/default-avatar.png'; };
const handleClickCard = (media) => {
  if (media.in_library && embyServerUrl.value && media.emby_item_id && embyServerId.value) {
    const embyDetailUrl = `${embyServerUrl.value}/web/index.html#!/item?id=${media.emby_item_id}&serverId=${embyServerId.value}`;
    window.open(embyDetailUrl, '_blank');
  } else {
    const mediaTypeForUrl = media.media_type || mediaType.value;
    const tmdbDetailUrl = `https://www.themoviedb.org/${mediaTypeForUrl}/${media.id}`;
    window.open(tmdbDetailUrl, '_blank');
  }
};
let debounceTimer = null;
const fetchDiscoverDataDebounced = () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => { fetchDiscoverData(); }, 300);
};
const loadMore = () => {
  if (isLoadingMore.value || loading.value || filters.page >= totalPages.value) { return; }
  filters.page++;
  fetchDiscoverData();
};
const resetAndFetch = () => {
  results.value = [];
  filters.page = 1;
  totalPages.value = 0;
  fetchDiscoverDataDebounced();
};
watch(mediaType, () => {
  selectedGenres.value = [];
  filters['sort_by'] = 'popularity.desc';
  fetchGenres();
  resetAndFetch();
});
watch(searchQuery, (newValue) => { resetAndFetch(); });
watch([() => filters.sort_by, () => filters.vote_average_gte, selectedGenres, selectedRegions, selectedLanguage, selectedKeywords, genreFilterMode, yearFrom, yearTo], () => { resetAndFetch(); }, { deep: true });
let observer = null;
onMounted(() => {
  fetchGenres();
  fetchCountries();
  fetchLanguages();
  fetchKeywords();
  fetchEmbyConfig(); 
  fetchRecommendationPool();
  resetAndFetch();
  observer = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting) { loadMore(); }
  }, { root: null, threshold: 0.1 });
  if (sentinel.value) { observer.observe(sentinel.value); }
});
onUnmounted(() => { if (observer) { observer.disconnect(); } });
</script>

<style scoped>
/* 样式代码完全不变 */
.media-card {
  cursor: pointer;
  transition: transform 0.3s ease, box-shadow 0.3s ease;
  border-radius: 8px;
  box-shadow: 0 4px 8px rgba(0,0,0,0.1);
  overflow: hidden;
}
.media-card:hover {
  transform: translateY(-8px);
  box-shadow: 0 8px 16px rgba(0,0,0,0.2);
}
.media-poster {
  width: 100%;
  aspect-ratio: 2 / 3;
  object-fit: cover;
  display: block;
}
.ribbon {
  position: absolute;
  top: -4px;
  left: -4px;
  width: 60px;
  height: 60px;
  overflow: hidden;
}
.ribbon span {
  position: absolute;
  display: block;
  width: 85px;
  padding: 4px 0;
  background-color: #67c23a;
  box-shadow: 0 5px 10px rgba(0,0,0,0.1);
  color: #fff;
  font-size: 9px;
  font-weight: bold;
  text-shadow: 0 1px 1px rgba(0,0,0,0.2);
  text-transform: uppercase;
  text-align: center;
  left: -20px;
  top: 14px;
  transform: rotate(-45deg);
}
.rating-badge {
  position: absolute;
  top: 6px;
  right: 6px;
  background-color: rgba(0, 0, 0, 0.75);
  color: #fff;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 14px;
  font-weight: bold;
  backdrop-filter: blur(4px);
}
.hover-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  background: linear-gradient(to top, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0) 100%);
  color: white;
  padding: 40px 12px 12px 12px;
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  transform: translateY(100%);
  opacity: 0;
  transition: transform 0.3s ease, opacity 0.3s ease;
}
.media-card:hover .hover-overlay {
  transform: translateY(0);
  opacity: 1;
}
.overlay-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
  width: calc(100% - 30px);
}
.media-title {
  font-weight: bold;
  font-size: 1.2em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.media-year {
  font-size: 1em;
  color: #ccc;
}
.action-icon {
  color: #fff;
  transition: transform 0.2s ease;
}
.action-icon:hover {
  transform: scale(1.2);
}
/* ★★★ “每日推荐”的专属样式 ★★★ */
/* 1. 卡片和内容区的基础设置 (不变) */
.recommendation-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

/* 1. 两栏布局的网格容器 */
.recommendation-grid {
  display: flex;
  gap: 24px; /* 控制左右两栏的间距 */
}

/* 2. 左栏：海报 */
.poster-column {
  flex-shrink: 0; /* 防止海报被压缩 */
}
.recommendation-poster {
  width: 150px;
  height: 225px;
  border-radius: 8px;
  object-fit: cover;
  box-shadow: 0 4px 12px rgba(0,0,0,0.2);
  display: block;
}

/* 3. 右栏：详情信息 */
.details-column {
  display: flex;
  flex-direction: column; /* 让右栏内部的元素垂直排列 */
  min-width: 0; /* 防止 flex 布局溢出 */
}

/* 4. 简介文本样式 */
.overview-text {
  flex-grow: 1; /* ★ 核心：让简介部分占据所有剩余空间，将按钮推到底部 */
  /* 如果简介内容过少，按钮不会紧贴着它，而是会被推到卡片底部 */
}

/* 5. 演员列表区域的样式 (基本不变) */
.actor-list-container {
  display: flex;
  gap: 16px;
  overflow-x: auto;
  padding-bottom: 10px;
  scrollbar-width: thin;
  scrollbar-color: #555 #333;
}
.actor-list-container::-webkit-scrollbar { height: 6px; }
.actor-list-container::-webkit-scrollbar-track { background: #333; border-radius: 3px; }
.actor-list-container::-webkit-scrollbar-thumb { background: #555; border-radius: 3px; }
.actor-list-container::-webkit-scrollbar-thumb:hover { background: #777; }

/* 6. 单个演员卡片的样式 (不变) */
.actor-card {
  flex-shrink: 0;
  width: 90px;
  text-align: center;
}
.actor-avatar {
  width: 90px;
  height: 135px;
  border-radius: 8px;
  object-fit: cover;
  margin-bottom: 8px;
  background-color: #333;
}
.actor-name {
  font-weight: bold;
  font-size: 0.9em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.actor-character {
  font-size: 0.8em;
  color: #888;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
/* ★ 新增：用于在任务运行时禁用图标点击的样式 ★ */
.action-icon.is-disabled {
  cursor: not-allowed;
  pointer-events: none;
  opacity: 0.5;
}
</style>