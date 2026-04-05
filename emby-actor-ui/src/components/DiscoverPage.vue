<!-- src/components/DiscoverPage.vue -->
<template>
  <n-layout :content-style="{ padding: isMobile ? '12px' : '24px' }">
  <div>
    <n-page-header title="影视探索" subtitle="发现您感兴趣的下一部作品" />
      <!-- ★★★ 新增：影巢 (HDHive) 配置与信息面板 ★★★ -->
      <n-card class="dashboard-card" style="margin-top: 16px;">
        <n-space align="center" justify="space-between" :wrap="isMobile">
          <n-space align="center">
            <n-icon :component="CloudDownloadIcon" size="28" color="#f0a020" />
            <span style="font-weight: bold; font-size: 16px; color: #f0a020;">影巢 (HDHive) 极速秒传</span>
          </n-space>
          <n-space align="center">
            <n-input v-model:value="hdhiveApiKey" type="password" placeholder="输入 X-API-Key" show-password-on="click" style="width: 250px;" />
            <n-button type="primary" color="#f0a020" @click="saveHDHiveConfig" :loading="savingHdhive">保存并连接</n-button>
          </n-space>
        </n-space>
        
        <div v-if="hdhiveUserInfo" style="margin-top: 16px;">
          <n-divider style="margin: 12px 0;" />
          <n-space align="center" justify="space-between" :wrap="isMobile">
            <!-- 左侧：用户信息 -->
            <n-space align="center" :size="24">
              <n-tag type="success" :bordered="false">
                <template #icon><n-icon :component="PersonIcon" /></template>
                用户: {{ hdhiveUserInfo.nickname || '未知用户' }}
              </n-tag>
              <n-tag type="warning" :bordered="false">
                <template #icon><n-icon :component="StarIcon" /></template>
                积分: {{ hdhiveUserInfo.user_meta?.points || '未知 (需Premium)' }}
              </n-tag>
              <n-tag type="info" :bordered="false" v-if="hdhiveQuotaInfo">
                <template #icon><n-icon :component="TicketIcon" /></template>
                今日剩余请求: {{ hdhiveQuotaInfo.endpoint_remaining ?? '无限' }}
              </n-tag>
            </n-space>
            
            <!-- 右侧：签到按钮 -->
            <n-space align="center">
              <n-button size="small" type="primary" secondary @click="doHDHiveCheckin(false)" :loading="checkingIn">
                <template #icon><n-icon><CalendarIcon/></n-icon></template>
                每日签到
              </n-button>
              <n-button size="small" type="error" secondary @click="doHDHiveCheckin(true)" :loading="checkingIn">
                <template #icon><n-icon><DiceIcon/></n-icon></template>
                赌狗签到
              </n-button>
            </n-space>
          </n-space>
        </div>
      </n-card>
      <n-grid :x-gap="24" :y-gap="24" :cols="isMobile ? 1 : 2" style="margin-top: 24px;">
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
                <label>分级:</label>
                <n-select
                  v-model:value="selectedRating"
                  :disabled="isSearchMode"
                  clearable
                  placeholder="选择内容分级"
                  :options="ratingOptions"
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
                <label>{{ studioLabel }}:</label>
                <n-select
                  v-model:value="selectedStudios"
                  :disabled="isSearchMode"
                  multiple
                  filterable
                  :placeholder="`选择${studioLabel} (映射)`"
                  :options="studioOptions"
                  style="min-width: 300px;"
                />
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
        <!-- 右侧“每日推荐”面板 -->
        <n-gi :span="1" v-if="!isMobile">
          <n-card :bordered="false" class="dashboard-card recommendation-card">
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
                <div class="recommendation-grid">
                    <div class="poster-column">
                        <img :src="`https://image.tmdb.org/t/p/w500${currentRecommendation.poster_path}`" class="recommendation-poster" />
                    </div>
                    <div class="details-column">
                        <n-h3 style="margin-top: 0; margin-bottom: 8px;">{{ currentRecommendation.title }}</n-h3>
                        <n-space align="center" size="small" style="color: #888; margin-bottom: 16px;">
                            <n-icon :component="StarIcon" color="#f7b824" />
                            <span>{{ currentRecommendation.vote_average?.toFixed(1) }}</span>
                            <span>·</span>
                            <span>{{ new Date(currentRecommendation.release_date).getFullYear() }}</span>
                        </n-space>
                        <n-ellipsis :line-clamp="4" :tooltip="false" class="overview-text">
                            {{ currentRecommendation.overview }}
                        </n-ellipsis>
                        <n-button 
                          type="primary" 
                          block 
                          @click="handleSubscribe(currentRecommendation)" 
                          :loading="subscribingId === currentRecommendation.id"
                          style="margin-top: 24px;"
                        >
                          <!-- ★ 修改：根据类型显示不同图标和文字 -->
                          <template #icon><n-icon :component="currentRecommendation.media_type === 'tv' ? ListIcon : HeartOutline" /></template>
                          {{ currentRecommendation.media_type === 'tv' ? '选择季' : '想看这个' }}
                        </n-button>
                    </div>
                </div>
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
      <div class="responsive-grid">
        <div 
          v-for="media in results" 
          :key="media.id" 
          class="grid-item"
        >
          <n-card class="dashboard-card media-card" content-style="padding: 0; position: relative;" @click="handleClickCard(media)">
            <div class="poster-wrapper">
              <img :src="media.poster_path ? `https://image.tmdb.org/t/p/w300${media.poster_path}` : '/default-poster.png'" class="media-poster" @error="onImageError">
              
              <div v-if="media.in_library" class="ribbon ribbon-green"><span>已入库</span></div>
              <div v-else-if="media.subscription_status === 'SUBSCRIBED'" class="ribbon ribbon-blue"><span>已订阅</span></div>
              <div v-else-if="media.subscription_status === 'PAUSED'" class="ribbon ribbon-blue"><span>已暂停</span></div>
              <div v-else-if="media.subscription_status === 'WANTED'" class="ribbon ribbon-purple"><span>待订阅</span></div>
              <div v-else-if="media.subscription_status === 'REQUESTED'" class="ribbon ribbon-orange"><span>待审核</span></div>
              <div v-else-if="media.subscription_status === 'PENDING_RELEASE'" class="ribbon ribbon-grey"><span>未发行</span></div>
              <div v-else-if="media.subscription_status === 'IGNORED'" class="ribbon ribbon-dark"><span>已忽略</span></div>

              <div v-if="media.vote_average" class="rating-badge">
                {{ media.vote_average.toFixed(1) }}
              </div>

              <div class="overlay-info">
                <div class="text-content">
                  <div class="media-title" :title="media.title || media.name">{{ media.title || media.name }}</div>
                  <div class="media-meta-row">
                    <span class="media-year">{{ getYear(media) }}</span>
                    <span v-if="getYear(media) && getGenreNames(media.genre_ids)" class="media-dot">·</span>
                    <span class="media-genres">{{ getGenreNames(media.genre_ids) }}</span>
                  </div>
                </div>

                <div class="actions-container">
                  <!-- ★ 核心修改：剧集始终显示列表图标，电影未入库时显示心形图标 -->
                  <div 
                    v-if="media.media_type === 'tv' || (!media.in_library && ((isPrivilegedUser && media.subscription_status === 'REQUESTED') || (!media.subscription_status || media.subscription_status === 'NONE')))"
                    class="action-btn"
                    @click.stop="handleSubscribe(media)"
                    :title="media.media_type === 'tv' ? '选择季' : (isPrivilegedUser ? '订阅' : '想看')"
                  >
                    <n-spin :show="subscribingId === media.id" size="small">
                      <n-icon size="18" color="#fff" class="shadow-icon">
                        <ListIcon v-if="media.media_type === 'tv'" />
                        <LightningIcon v-else-if="isPrivilegedUser && media.subscription_status === 'REQUESTED'" color="#f0a020" />
                        <HeartOutline v-else />
                      </n-icon>
                    </n-spin>
                  </div>
                </div>
              </div>

            </div>
          </n-card>
        </div>
      </div>
    </n-spin>

    <div v-if="isLoadingMore" style="text-align: center; padding: 20px;">
      <n-spin size="medium" />
    </div>
    <div v-if="results.length > 0 && filters.page >= totalPages" style="text-align: center; padding: 20px; color: #888;">
      已经到底啦~
    </div>

    <div ref="sentinel" style="height: 50px;"></div>
    
    <!-- ★★★ 季选择模态框 (加入影巢按钮) ★★★ -->
    <n-modal v-model:show="showSeasonModal" preset="card" title="选择要订阅的季" style="width: 600px; max-width: 95%;">
      <n-spin :show="loadingSeasons">
        <div v-if="seasonList.length === 0 && !loadingSeasons" style="text-align: center; color: #888; padding: 20px;">未找到季信息</div>
        <n-space vertical v-else>
          <!-- ★ 修复：去掉了 style="background: #222;"，自适应浅色/深色主题 -->
          <n-card v-for="season in seasonList" :key="season.id" size="small" hoverable>
            <div style="display: flex; align-items: center; gap: 12px;">
              <img v-if="season.poster_path" :src="`https://image.tmdb.org/t/p/w92${season.poster_path}`" style="width: 40px; border-radius: 4px;" />
              <div v-else style="width: 40px; height: 60px; background: #333; border-radius: 4px;"></div>
              <div style="flex-grow: 1;">
                <div style="font-weight: bold;">{{ season.name }}</div>
                <div style="font-size: 12px; color: #888;">{{ season.episode_count }} 集 <span v-if="season.air_date">· {{ season.air_date }}</span></div>
              </div>
              <div style="display: flex; gap: 8px; align-items: center;">
                <n-tag v-if="season.in_library" type="success" size="small">已入库</n-tag>
                <template v-else>
                  <!-- 仅保留 MP 常规订阅按钮 -->
                  <n-button size="small" type="primary" secondary @click="submitSeasonSubscription(season)" :loading="subscribingSeasonId === season.id" :disabled="season.subscription_status === 'SUBSCRIBED' || season.subscription_status === 'WANTED'">
                    <template #icon><n-icon><LightningIcon/></n-icon></template> MP订阅
                  </n-button>
                </template>
              </div>
            </div>
          </n-card>
          <n-divider style="margin: 12px 0;" />
          <n-space vertical>
            <n-button block type="primary" @click="submitAllSeasonsSubscription" :loading="subscribingAllSeasons">
              一键提交整剧到 MoviePilot
            </n-button>
            <n-button block color="#f0a020" secondary @click="openHDHiveResourceModal(currentSeriesForSearch, null)">
              <template #icon><n-icon><CloudDownloadIcon/></n-icon></template>
              从 影巢 (HDHive) 搜索整剧资源
            </n-button>
          </n-space>
        </n-space>
      </n-spin>
    </n-modal>

    <!-- ★★★ 电影订阅方式选择模态框 ★★★ -->
    <n-modal v-model:show="showMovieChoiceModal" preset="card" title="选择获取方式" style="width: 400px;">
      <n-space vertical size="large">
        <n-button block type="primary" size="large" secondary @click="submitMovieToMP">
          <template #icon><n-icon size="20"><LightningIcon/></n-icon></template>
          提交到 MoviePilot (常规挂机)
        </n-button>
        <n-button block color="#f0a020" size="large" secondary @click="openHDHiveResourceModal(currentMovieForChoice, null)">
          <template #icon><n-icon size="20"><CloudDownloadIcon/></n-icon></template>
          从 影巢 (HDHive) 极速秒传
        </n-button>
      </n-space>
    </n-modal>

    <!-- ★★★ 影巢资源列表模态框 (独立组件) ★★★ -->
    <HDHiveResourceModal 
      v-model:show="showHDHiveResourceModal" 
      :media="currentHDHiveMedia" 
      :season-number="currentHDHiveSeason"
      @download-success="fetchHDHiveConfig" 
    />
    
  </div>
  </n-layout>
</template>

<script setup>
import { ref, reactive, watch, onMounted, onUnmounted, computed } from 'vue';
import { useRouter } from 'vue-router';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import HDHiveResourceModal from './HDHiveResourceModal.vue';
import { 
  NPageHeader, NCard, NSpace, NRadioGroup, NRadioButton, NSelect,
  NInputNumber, NSpin, NGrid, NGi, NButton, NThing, useMessage, NIcon, 
  NInput, NInputGroup, NSkeleton, NEllipsis, NEmpty, NDivider, NH4, NH3, NTooltip, NModal, NTag
} from 'naive-ui';
import { Heart, HeartOutline, HourglassOutline, Star as StarIcon, FlashOutline as LightningIcon, DiceOutline as DiceIcon, ListOutline as ListIcon, CloudDownloadOutline as CloudDownloadIcon, PersonOutline as PersonIcon, TicketOutline as TicketIcon, CalendarOutline as CalendarIcon } from '@vicons/ionicons5';
const authStore = useAuthStore();
const message = useMessage();
const router = useRouter(); 
const isPrivilegedUser = computed(() => {
  return authStore.isAdmin || authStore.user?.allow_unrestricted_subscriptions;
});
const embyServerUrl = ref('');
const embyServerId = ref('');
const registrationRedirectUrl = ref('');

const loading = ref(false);
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
const allStudios = ref([]); 
const selectedStudios = ref([]);
const studioOptions = computed(() => {
  if (!allStudios.value || allStudios.value.length === 0) return [];

  return allStudios.value
    .filter(item => {
      if (item.types && Array.isArray(item.types)) {
        return item.types.includes(mediaType.value);
      }
      return true;
    })
    .map(item => ({
      label: item.label,
      value: item.value 
    }));
});
const genreFilterMode = ref('include'); 
const yearFrom = ref(null);
const yearTo = ref(null);
const recommendationPool = ref([]); 
const currentRecommendation = ref(null); 
const isPoolLoading = ref(true); 
const ratingOptions = ref([]);
const selectedRating = ref(null);
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
const isMobile = ref(false);
const checkMobile = () => {
  isMobile.value = window.innerWidth < 768;
};

// ★★★ 季选择模态框相关状态 ★★★
const showSeasonModal = ref(false);
const loadingSeasons = ref(false);
const seasonList = ref([]);
const currentSeriesForSearch = ref(null);
const subscribingSeasonId = ref(null);
const subscribingAllSeasons = ref(false);

const studioLabel = computed(() => {
  return mediaType.value === 'movie' ? '出品公司' : '播出平台';
});

const getGenreNames = (genreIds) => {
  if (!genreIds || genreIds.length === 0 || genres.value.length === 0) return '';
  
  return genreIds
    .map(id => genres.value.find(g => g.id === id)?.name)
    .filter(Boolean) 
    .slice(0, 2)    
    .join(' / ');
};

const getYear = (media) => {
  const dateStr = media.release_date || media.first_air_date;
  if (!dateStr) return '';
  return new Date(dateStr).getFullYear();
};
const genreOptions = computed(() => {
  return genres.value.map(item => ({
    label: item.name, 
    value: item.id    
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
const fetchStudios = async () => {
  try {
    const response = await axios.get('/api/discover/config/studios');
    allStudios.value = response.data;
  } catch (error) {
    message.error('加载工作室列表失败');
  }
};
const fetchRatings = async () => {
  try {
    const response = await axios.get('/api/custom_collections/config/unified_ratings_options');
    ratingOptions.value = response.data.map(label => ({
      label: label,
      value: label
    }));
  } catch (error) {
    message.error('加载分级列表失败');
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
        'with_keywords': selectedKeywords.value,
        'with_companies': selectedStudios.value,
        'with_rating_label': selectedRating.value
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
    registrationRedirectUrl.value = response.data.emby_public_url;
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

const updateMediaStatus = (mediaId, newStatus) => {
  const index = results.value.findIndex(m => m.id === mediaId);
  if (index !== -1) {
    results.value[index] = { 
      ...results.value[index], 
      subscription_status: newStatus 
    };
  }

  if (currentRecommendation.value && currentRecommendation.value.id === mediaId) {
    currentRecommendation.value = {
      ...currentRecommendation.value,
      subscription_status: newStatus
    };
  }
};

// ★★★ 影巢相关状态 ★★★
const hdhiveApiKey = ref('');
const hdhiveUserInfo = ref(null);
const hdhiveQuotaInfo = ref(null);
const savingHdhive = ref(false);

const showMovieChoiceModal = ref(false);
const currentMovieForChoice = ref(null);

const showHDHiveResourceModal = ref(false);
const loadingHDHiveResources = ref(false);
const hdhiveResources = ref([]);
const currentHDHiveMedia = ref(null);
const downloadingSlug = ref(null);
const currentHDHiveSeason = ref(null);

// ★★★ 影巢 API 调用逻辑 ★★★
const fetchHDHiveConfig = async () => {
  try {
    const res = await axios.get('/api/hdhive/config');
    if (res.data.success) {
      hdhiveApiKey.value = res.data.api_key;
      hdhiveUserInfo.value = res.data.user_info;
      hdhiveQuotaInfo.value = res.data.quota_info;
    }
  } catch (e) {
    console.error("获取影巢配置失败", e);
  }
};

const checkingIn = ref(false);

const doHDHiveCheckin = async (isGambler) => {
  if (!hdhiveApiKey.value) return message.warning("请先配置 API Key");
  
  checkingIn.value = true;
  try {
    const res = await axios.post('/api/hdhive/checkin', { is_gambler: isGambler });
    if (res.data.success) {
      // 签到成功，弹出后端返回的 message（里面包含本次获得的积分）
      message.success(res.data.message, { duration: 5000 });
      // ★ 签到成功后，刷新一下用户信息，让积分变动实时显示出来
      fetchHDHiveConfig(); 
    } else {
      // 可能是已经签到过了
      message.warning(res.data.message);
    }
  } catch (e) {
    message.error("签到请求失败");
  } finally {
    checkingIn.value = false;
  }
};

const saveHDHiveConfig = async () => {
  if (!hdhiveApiKey.value) return message.warning("请输入 API Key");
  savingHdhive.value = true;
  try {
    const res = await axios.post('/api/hdhive/config', { api_key: hdhiveApiKey.value });
    if (res.data.success) {
      message.success(res.data.message);
      hdhiveUserInfo.value = res.data.user_info;
      hdhiveQuotaInfo.value = res.data.quota_info;
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error("保存失败");
  } finally {
    savingHdhive.value = false;
  }
};

// 打开影巢资源列表弹窗
const openHDHiveResourceModal = async (media, seasonNumber = null) => {
  if (!hdhiveApiKey.value) {
    message.warning("请先在页面顶部配置影巢 API Key！");
    return;
  }
  
  showMovieChoiceModal.value = false; // 如果是从电影弹窗来的，关掉它
  currentHDHiveMedia.value = media;
  currentHDHiveSeason.value = seasonNumber; // 传递季号
  showHDHiveResourceModal.value = true;
};

// ★★★ 改造原有的 handleSubscribe ★★★
const handleSubscribe = async (media) => {
  // 1. 如果是剧集，弹出季选择模态框 (逻辑不变，模板里已经加了影巢按钮)
  if (media.media_type === 'tv' || mediaType.value === 'tv') {
    currentSeriesForSearch.value = media;
    showSeasonModal.value = true;
    loadingSeasons.value = true;
    seasonList.value = [];
    try {
      const res = await axios.get(`/api/discover/tmdb/tv/${media.id}`);
      if (res.data && res.data.seasons) {
        seasonList.value = res.data.seasons
          .filter(s => s.season_number > 0)
          .sort((a, b) => a.season_number - b.season_number);
      }
    } catch (e) {
      message.warning("获取季信息失败");
      seasonList.value = [];
    } finally {
      loadingSeasons.value = false;
    }
    return;
  }

  // 2. 如果是电影，弹出选择模态框 (MP or 影巢)
  currentMovieForChoice.value = media;
  showMovieChoiceModal.value = true;
};

// 电影：提交到 MP 的实际逻辑 (从原 handleSubscribe 拆分出来)
const submitMovieToMP = async () => {
  const media = currentMovieForChoice.value;
  showMovieChoiceModal.value = false;
  
  if (subscribingId.value === media.id) return;
  const originalStatus = media.subscription_status || 'NONE';
  subscribingId.value = media.id;
  const optimisticStatus = isPrivilegedUser.value ? 'WANTED' : 'REQUESTED';
  updateMediaStatus(media.id, optimisticStatus);

  try {
    const portalResponse = await axios.post('/api/portal/subscribe', {
      tmdb_id: media.id,
      item_type: 'Movie',
      item_name: media.title || media.name,
    });
    message.success(portalResponse.data.message);
    let finalStatus = portalResponse.data.status;
    if (!isPrivilegedUser.value) finalStatus = 'REQUESTED';
    else if (!finalStatus || finalStatus === 'NONE') finalStatus = 'WANTED';
    
    if (isPrivilegedUser.value && finalStatus === 'approved') updateMediaStatus(media.id, 'SUBSCRIBED');
    else updateMediaStatus(media.id, finalStatus);

    if (currentRecommendation.value && currentRecommendation.value.id === media.id) {
      const poolIndex = recommendationPool.value.findIndex(item => item.id === media.id);
      if (poolIndex !== -1) recommendationPool.value.splice(poolIndex, 1);
      pickRandomRecommendation();
    }
  } catch (error) {
    updateMediaStatus(media.id, originalStatus);
    message.error(error.response?.data?.message || '提交请求失败');
  } finally {
    subscribingId.value = null;
  }
};

// ★★★ 提交单季订阅 ★★★
const submitSeasonSubscription = async (season) => {
  subscribingSeasonId.value = season.id;
  try {
    const portalResponse = await axios.post('/api/portal/subscribe', {
      tmdb_id: currentSeriesForSearch.value.id, // 传剧集ID
      item_type: 'Season',
      season_number: season.season_number,
      season_tmdb_id: season.id,
      item_name: `${currentSeriesForSearch.value.title || currentSeriesForSearch.value.name} 第 ${season.season_number} 季`
    });
    message.success(portalResponse.data.message);
    // 更新模态框内该季的状态
    season.subscription_status = portalResponse.data.status === 'approved' ? 'SUBSCRIBED' : 'REQUESTED';
  } catch (error) {
    message.error(error.response?.data?.message || '订阅失败');
  } finally {
    subscribingSeasonId.value = null;
  }
};

// ★★★ 一键订阅所有缺失季 ★★★
const submitAllSeasonsSubscription = async () => {
  subscribingAllSeasons.value = true;
  try {
    const missingSeasons = seasonList.value.filter(s => !s.in_library && s.subscription_status !== 'SUBSCRIBED' && s.subscription_status !== 'WANTED');
    if (missingSeasons.length === 0) {
      message.info("所有季均已入库或已订阅");
      return;
    }
    
    // 直接提交整剧订阅，后端会自动遍历并订阅所有缺失季
    const portalResponse = await axios.post('/api/portal/subscribe', {
      tmdb_id: currentSeriesForSearch.value.id,
      item_type: 'Series',
      item_name: currentSeriesForSearch.value.title || currentSeriesForSearch.value.name
    });
    message.success("一键订阅已提交");
    
    // 更新模态框内状态
    missingSeasons.forEach(s => {
      s.subscription_status = portalResponse.data.status === 'approved' ? 'SUBSCRIBED' : 'REQUESTED';
    });
    
    // 更新外部卡片状态
    updateMediaStatus(currentSeriesForSearch.value.id, portalResponse.data.status === 'approved' ? 'SUBSCRIBED' : 'REQUESTED');
    
    setTimeout(() => { showSeasonModal.value = false; }, 1000);
  } catch (error) {
    message.error(error.response?.data?.message || '一键订阅失败');
  } finally {
    subscribingAllSeasons.value = false;
  }
};

const onImageError = (e) => { e.target.src = '/default-avatar.png'; };
const handleClickCard = (media) => {
  if (media.in_library && media.emby_item_id && embyServerId.value) {
    let baseUrl = registrationRedirectUrl.value || embyServerUrl.value;

    if (baseUrl) {
      baseUrl = baseUrl.replace(/\/+$/, '');
      const embyDetailUrl = `${baseUrl}/web/index.html#!/item?id=${media.emby_item_id}&serverId=${embyServerId.value}`;
      window.open(embyDetailUrl, '_blank');
    }
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
  selectedStudios.value = []; 
  filters['sort_by'] = 'popularity.desc';
  fetchGenres();
  resetAndFetch();
});
watch(searchQuery, (newValue) => { resetAndFetch(); });
watch([() => filters.sort_by, () => filters.vote_average_gte, selectedGenres, selectedRegions, selectedLanguage, selectedKeywords, selectedStudios, genreFilterMode, yearFrom, yearTo, selectedRating], () => { resetAndFetch(); }, { deep: true });
let observer = null;
onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  fetchGenres();
  fetchCountries();
  fetchLanguages();
  fetchKeywords();
  fetchStudios();
  fetchRatings();
  fetchEmbyConfig(); 
  fetchHDHiveConfig();
  fetchRecommendationPool();
  resetAndFetch();
  observer = new IntersectionObserver((entries) => {
    if (entries[0].isIntersecting) { loadMore(); }
  }, { root: null, threshold: 0.1 });
  if (sentinel.value) { observer.observe(sentinel.value); }
});
onUnmounted(() => { 
  window.removeEventListener('resize', checkMobile);
  if (observer) { observer.disconnect(); } 
});
</script>

<style scoped>
.responsive-grid {
  display: grid;
  gap: 16px; 
  margin-top: 24px;
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
}

.grid-item {
  min-width: 0; 
  height: 100%;
}

.media-card {
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  overflow: hidden;
  height: 100%;
  background-color: #222; 
  display: flex;
  flex-direction: column;
}
.media-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 16px rgba(0,0,0,0.3);
  z-index: 10;
}

.poster-wrapper {
  position: relative;
  width: 100%;
  aspect-ratio: 2 / 3; 
  overflow: hidden;
}
.media-poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  transition: transform 0.3s ease;
}
.media-card:hover .media-poster {
  transform: scale(1.05);
}

.overlay-info {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  background: linear-gradient(to top, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0.6) 50%, transparent 100%);
  padding: 40px 8px 8px 8px; 
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  pointer-events: none; 
}

.text-content {
  flex: 1;
  min-width: 0;
  margin-right: 4px;
}

.media-title {
  color: #fff;
  font-weight: bold;
  font-size: 0.95em;
  line-height: 1.2;
  margin-bottom: 2px;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.media-meta-row {
  display: flex;
  align-items: center;
  color: rgba(255, 255, 255, 0.85);
  font-size: 0.8em;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  overflow: hidden; 
}

.media-year {
  flex-shrink: 0; 
}

.media-dot {
  margin: 0 4px;
  flex-shrink: 0;
}

.media-genres {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis; 
}

.media-title {
  margin-bottom: 1px; 
}

.rating-badge {
  position: absolute;
  top: 6px;
  right: 6px;
  background-color: rgba(0, 0, 0, 0.65);
  color: #f7b824;
  padding: 2px 5px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: bold;
  backdrop-filter: blur(2px);
  box-shadow: 0 1px 2px rgba(0,0,0,0.3);
  z-index: 5;
}
.actions-container {
  display: flex;
  gap: 8px; 
  align-items: center;
}
.action-btn {
  pointer-events: auto;
  width: 30px;
  height: 30px;
  background-color: transparent; 
  backdrop-filter: none;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: transform 0.2s ease;
  flex-shrink: 0;
}

.action-btn:hover {
  transform: scale(1.2);
  background-color: transparent;
}

.shadow-icon {
  filter: drop-shadow(0 0 3px rgba(0,0,0,0.9));
}

.ribbon {
  position: absolute;
  top: -3px;
  left: -3px;
  width: 60px;
  height: 60px;
  overflow: hidden;
  z-index: 5;
}
.ribbon span {
  position: absolute;
  display: block;
  width: 85px;
  padding: 3px 0;
  box-shadow: 0 2px 4px rgba(0,0,0,0.2);
  color: #fff;
  font-size: 10px;
  font-weight: bold;
  text-shadow: 0 1px 1px rgba(0,0,0,0.3);
  text-transform: uppercase;
  text-align: center;
  left: -16px;
  top: 10px;
  transform: rotate(-45deg);
}

.ribbon-green span { background-color: #67c23a; }
.ribbon-blue span { background-color: #409eff; }
.ribbon-purple span { background-color: #722ed1; }
.ribbon-orange span { background-color: #e6a23c; }
.ribbon-grey span { background-color: #909399; }
.ribbon-dark span { background-color: #303133; }

.recommendation-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.recommendation-grid {
  display: flex;
  gap: 24px; 
}

.poster-column {
  flex-shrink: 0; 
}
.recommendation-poster {
  width: 150px;
  height: 225px;
  border-radius: 8px;
  object-fit: cover;
  box-shadow: 0 4px 12px rgba(0,0,0,0.2);
  display: block;
}

.details-column {
  display: flex;
  flex-direction: column; 
  min-width: 0; 
}

.overview-text {
  flex-grow: 1; 
}

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
.action-icon.is-disabled {
  cursor: not-allowed;
  pointer-events: none;
  opacity: 0.5;
}
</style>