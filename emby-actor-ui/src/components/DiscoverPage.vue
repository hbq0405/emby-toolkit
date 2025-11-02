<!-- src/components/DiscoverPage.vue (终极修复版：修复订阅逻辑 + 升级无限滚动) -->
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

    <!-- 结果展示区域 -->
    <n-spin :show="loading && results.length === 0">
      <n-grid :x-gap="16" :y-gap="24" responsive="screen" cols="2 s:3 m:4 l:5 xl:6 2xl:7" style="margin-top: 24px;">
        <n-gi v-for="media in results" :key="media.id">
          <n-card class="media-card" content-style="padding: 0; position: relative; overflow: hidden;">
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
              <div v-if="!media.in_library" class="action-icon" @click="handleSubscribe(media)">
                <n-spin :show="subscribingId === media.id" size="small">
                  <n-icon size="24">
                    <Heart v-if="media.subscription_status === 'approved'" color="#ff4d4f" />
                    <HourglassOutline v-else-if="media.subscription_status === 'pending'" color="#e6a23c" />
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
</template>

<script setup>
import { ref, reactive, watch, onMounted, onUnmounted, computed } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { 
  NPageHeader, NCard, NSpace, NRadioGroup, NRadioButton, NSelect,
  NInputNumber, NSpin, NGrid, NGi, NButton, NRate, useMessage, NIcon
} from 'naive-ui';
import { Heart, HeartOutline, HourglassOutline } from '@vicons/ionicons5';

const authStore = useAuthStore();
const message = useMessage();

// --- 状态管理 ---
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
const isLoadingMore = ref(false);

// ★★★ 核心改造 2: 为“哨兵”元素创建一个 ref ★★★
const sentinel = ref(null);


const genreOptions = computed(() => 
  genres.value.map(g => ({ label: g.name, value: g.id }))
);

const getYear = (media) => {
  const dateStr = media.release_date || media.first_air_date;
  if (!dateStr) return '';
  return new Date(dateStr).getFullYear();
};

// --- API 调用 ---
const fetchGenres = async () => { /* ...代码不变... */ };
const fetchCountries = async () => { /* ...代码不变... */ };

const fetchDiscoverData = async () => {
  if (isLoadingMore.value || loading.value) return;

  if (filters.page === 1) {
    loading.value = true;
  } else {
    isLoadingMore.value = true;
  }

  try {
    const apiParams = {
      ...filters,
      'vote_average.gte': filters.vote_average_gte,
      'with_genres': selectedGenres.value.join(','),
      'with_origin_country': selectedRegions.value.join('|'),
    };
    delete apiParams.vote_average_gte;

    const response = await axios.post(`/api/discover/${mediaType.value}`, apiParams);
    
    if (filters.page === 1) {
      results.value = response.data.results;
    } else {
      results.value.push(...response.data.results);
    }
    totalPages.value = response.data.total_pages;

  } catch (error) {
    message.error('加载影视数据失败');
    if (filters.page === 1) {
      results.value = [];
    }
  } finally {
    loading.value = false;
    isLoadingMore.value = false;
  }
};

// ★★★ 核心改造 3: 恢复完整、正确的 handleSubscribe 函数逻辑 ★★★
const handleSubscribe = async (media) => {
  // 如果正在提交，或者已经有订阅状态了（pending 或 approved），则直接返回，防止重复点击
  if (subscribingId.value || media.subscription_status) {
    logger.debug("订阅请求被阻止：已有订阅状态或正在提交中。");
    return;
  }

  subscribingId.value = media.id;
  try {
    const response = await axios.post('/api/portal/subscribe', {
      tmdb_id: media.id,
      item_type: mediaType.value === 'movie' ? 'Movie' : 'Series',
      item_name: media.title || media.name,
    });
    message.success(response.data.message);

    const targetMedia = results.value.find(r => r.id === media.id);
    if (targetMedia && response.data.status) {
      targetMedia.subscription_status = response.data.status;
    }

  } catch (error) {
    message.error(error.response?.data?.message || '提交请求失败');
  } finally {
    subscribingId.value = null;
  }
};

let debounceTimer = null;
const fetchDiscoverDataDebounced = () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    fetchDiscoverData();
  }, 300);
};

// ★★★ 核心改造 4: 加载更多的逻辑 ★★★
const loadMore = () => {
  // 如果正在加载，或者已经没有更多页了，则不执行
  if (isLoadingMore.value || loading.value || filters.page >= totalPages.value) {
    return;
  }
  filters.page++; // 页数加一
  fetchDiscoverData(); // 获取下一页数据
};

const resetAndFetch = () => {
  results.value = [];
  filters.page = 1;
  totalPages.value = 0;
  // 确保DOM更新后哨兵元素可见，以便可以重新触发加载
  // 但首次加载由 fetchDiscoverDataDebounced 保证，所以这里不需要特殊处理
  fetchDiscoverDataDebounced();
};

watch(mediaType, () => {
  selectedGenres.value = [];
  filters['sort_by'] = 'popularity.desc';
  fetchGenres();
  resetAndFetch();
});

watch([() => filters['sort_by'], () => filters.vote_average_gte, selectedGenres, selectedRegions], () => {
  resetAndFetch();
}, { deep: true });


// ★★★ 核心改造 5: 在生命周期中设置和销毁 IntersectionObserver ★★★
let observer = null;
onMounted(() => {
  fetchGenres();
  fetchCountries();
  resetAndFetch();

  // 创建观察器
  observer = new IntersectionObserver(
    (entries) => {
      // 如果哨兵元素进入视口
      if (entries[0].isIntersecting) {
        loadMore();
      }
    },
    {
      root: null, // 相对于浏览器视口
      threshold: 0.1, // 哨兵元素可见10%时触发
    }
  );

  // 开始观察哨兵元素
  if (sentinel.value) {
    observer.observe(sentinel.value);
  }
});

onUnmounted(() => {
  // 组件销毁时，停止观察，防止内存泄漏
  if (observer) {
    observer.disconnect();
  }
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