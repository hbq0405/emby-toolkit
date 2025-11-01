<!-- src/components/DiscoverPage.vue (状态精确版) -->
<template>
  <div>
    <n-page-header title="影视探索" subtitle="发现您感兴趣的下一部作品" />

    <n-card :bordered="false" style="margin-top: 24px;">
      <!-- 筛选区域代码完全不变 -->
      <n-space vertical size="large">
        <n-space align="center">
          <label>类型:</label>
          <n-radio-group v-model:value="mediaType">
            <n-radio-button value="movie" label="电影" />
            <n-radio-button value="tv" label="电视剧" />
          </n-radio-group>
        </n-space>
        <n-space align="center">
          <label>排序:</label>
          <n-radio-group v-model:value="filters['sort_by']">
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
          <n-select
            v-model:value="selectedGenres"
            multiple
            filterable
            placeholder="选择风格/类型"
            :options="genreOptions"
            style="min-width: 300px;"
          />
        </n-space>
        <n-space align="center">
        <label>地区:</label>
        <n-select
            v-model:value="selectedRegions"
            multiple
            filterable
            placeholder="选择国家/地区"
            :options="countryOptions"
            style="min-width: 300px;"
        />
        </n-space>
        <n-space align="center">
          <label>评分不低于:</label>
          <n-input-number
            v-model:value="filters.vote_average_gte"
            :step="0.5"
            :min="0"
            :max="10"
            placeholder="最低评分"
            style="width: 120px;"
          />
        </n-space>
      </n-space>
    </n-card>

    <n-spin :show="loading">
      <n-grid :x-gap="16" :y-gap="24" responsive="screen" cols="2 s:3 m:4 l:5 xl:6 2xl:7" style="margin-top: 24px;">
        <n-gi v-for="media in results" :key="media.id">
          <n-card class="dashboard-card media-card" content-style="padding: 0; position: relative; overflow: hidden;">
            
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
              
              <!-- ★★★ 核心改造 1: 图标逻辑完全由 subscription_status 驱动 ★★★ -->
              <div v-if="!media.in_library" class="action-icon" @click="handleSubscribe(media)">
                <n-spin :show="subscribingId === media.id" size="small">
                  <n-icon size="24">
                    <!-- 状态1: 已批准 (approved) -->
                    <Heart v-if="media.subscription_status === 'approved'" color="#ff4d4f" />
                    <!-- 状态2: 待审核 (pending) -->
                    <HourglassOutline v-else-if="media.subscription_status === 'pending'" color="#e6a23c" />
                    <!-- 状态3: 未订阅 (默认) -->
                    <HeartOutline v-else />
                  </n-icon>
                </n-spin>
              </div>
            </div>

          </n-card>
        </n-gi>
      </n-grid>
    </n-spin>
  </div>
</template>

<script setup>
import { ref, reactive, watch, onMounted, computed } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { 
  NPageHeader, NCard, NSpace, NRadioGroup, NRadioButton, NSelect,
  NInputNumber, NSpin, NGrid, NGi, NButton, NRate, useMessage, NIcon
} from 'naive-ui';
import { Heart, HeartOutline, HourglassOutline } from '@vicons/ionicons5';

const authStore = useAuthStore();
const message = useMessage();

// --- 状态管理 (不变) ---
const loading = ref(false);
const subscribingId = ref(null);
const mediaType = ref('movie');
const genres = ref([]);
const selectedGenres = ref([]);
const countryOptions = ref([]); 
const selectedRegions = ref([]);
const filters = reactive({
  'sort_by': 'popularity.desc',
  'vote_average_gte': 0,
  'with_genres': '',
  'with_origin_country': '',
  'page': 1,
});
const results = ref([]);
const totalPages = ref(0);

const genreOptions = computed(() => 
  genres.value.map(g => ({ label: g.name, value: g.id }))
);

const getYear = (media) => {
  const dateStr = media.release_date || media.first_air_date;
  if (!dateStr) return '';
  return new Date(dateStr).getFullYear();
};

// --- API 调用 ---
const fetchGenres = async () => { /* ...代码不变... */ 
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
const fetchCountries = async () => { /* ...代码不变... */ 
  try {
    const response = await axios.get('/api/custom_collections/config/tmdb_countries');
    countryOptions.value = response.data;
  } catch (error) {
    message.error('加载国家列表失败');
  }
};

const fetchDiscoverData = async () => {
  loading.value = true;
  try {
    const apiParams = {
      ...filters,
      'vote_average.gte': filters.vote_average_gte,
      'with_genres': selectedGenres.value.join(','),
      'with_origin_country': selectedRegions.value.join('|'),
    };
    delete apiParams.vote_average_gte;

    const response = await axios.post(`/api/discover/${mediaType.value}`, apiParams);
    
    // ★★★ 核心改造 2: 直接使用后端返回的数据，不再手动添加 is_subscribed ★★★
    results.value = response.data.results;
    totalPages.value = response.data.total_pages;

  } catch (error) {
    message.error('加载影视数据失败');
    results.value = [];
  } finally {
    loading.value = false;
  }
};

// ★★★ 核心改造 3: handleSubscribe 使用后端返回的真实状态更新UI ★★★
const handleSubscribe = async (media) => {
  // 防止在已有订阅或正在请求时重复点击
  if (subscribingId.value || media.subscription_status) return;

  subscribingId.value = media.id;
  try {
    const response = await axios.post('/api/portal/subscribe', {
      tmdb_id: media.id,
      item_type: mediaType.value === 'movie' ? 'Movie' : 'Series',
      item_name: media.title || media.name,
    });
    message.success(response.data.message);

    // 找到刚刚操作的媒体项
    const targetMedia = results.value.find(r => r.id === media.id);
    if (targetMedia && response.data.status) {
      // 使用后端返回的最新状态来更新该项的 status
      targetMedia.subscription_status = response.data.status;
    }

  } catch (error) {
    message.error(error.response?.data?.message || '提交请求失败');
  } finally {
    subscribingId.value = null;
  }
};

let debounceTimer = null;
const fetchDiscoverDataDebounced = () => { /* ...代码不变... */ 
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    fetchDiscoverData();
  }, 300);
};

// --- 监听器和生命周期 (不变) ---
watch(mediaType, () => {
  selectedGenres.value = [];
  filters['sort_by'] = 'popularity.desc';
  fetchGenres();
  fetchDiscoverData();
});
watch([() => filters['sort_by'], () => filters.vote_average_gte, selectedGenres, selectedRegions], () => {
  fetchDiscoverDataDebounced();
}, { deep: true });
onMounted(() => {
  fetchGenres();
  fetchCountries();
  fetchDiscoverData();
});
</script>

<style scoped>
/* 样式代码完全不变 */
.media-card {
  cursor: pointer;
  transition: transform 0.3s ease, box-shadow 0.3s ease;
  border-radius: 8px;
  box-shadow: 0 4px 8px rgba(0,0,0,0.1);
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
  top: -6px;
  left: -6px;
  width: 90px;
  height: 90px;
  overflow: hidden;
}
.ribbon span {
  position: absolute;
  display: block;
  width: 125px;
  padding: 8px 0;
  background-color: #67c23a;
  box-shadow: 0 5px 10px rgba(0,0,0,0.1);
  color: #fff;
  font-size: 12px;
  font-weight: bold;
  text-shadow: 0 1px 1px rgba(0,0,0,0.2);
  text-transform: uppercase;
  text-align: center;
  left: -30px;
  top: 20px;
  transform: rotate(-45deg);
}
.rating-badge {
  position: absolute;
  top: 8px;
  right: 8px;
  background-color: rgba(0, 0, 0, 0.75);
  color: #fff;
  padding: 4px 8px;
  border-radius: 4px;
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
  font-size: 1em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.media-year {
  font-size: 0.8em;
  color: #ccc;
}
.action-icon {
  color: #fff;
  transition: transform 0.2s ease;
}
.action-icon:hover {
  transform: scale(1.2);
}
</style>