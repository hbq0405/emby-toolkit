<!-- src/views/UserCenter.vue -->
<template>
  <div>
    <n-page-header :title="`欢迎回来, ${authStore.username}`" subtitle="在这里探索您的观影世界" />
    
    <n-grid :cols="2" :x-gap="24" style="margin-top: 24px;">
      <n-gi>
        <n-card title="我的观影历史">
          <n-list hoverable clickable>
            <n-list-item v-for="item in history" :key="item.item_id">
              <n-thing :title="item.title || item.original_title" :description="`年份: ${item.release_year}`">
                <template #footer>
                  最近观看: {{ new Date(item.last_played_date).toLocaleString() }} | 播放次数: {{ item.play_count }}
                </template>
              </n-thing>
            </n-list-item>
          </n-list>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card title="媒体库热门排行">
          <n-list hoverable clickable>
            <n-list-item v-for="(item, index) in rankings" :key="item.item_id">
              <n-thing>
                <template #header>
                  <n-badge :value="index + 1" color="grey" />
                  {{ item.title || item.original_title }}
                </template>
                <template #description>
                  年份: {{ item.release_year }}
                </template>
                <template #footer>
                  总播放: {{ item.total_play_count }} 次 | 观看人数: {{ item.total_viewers }} 人
                </template>
              </n-thing>
            </n-list-item>
          </n-list>
        </n-card>
      </n-gi>
    </n-grid>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import { useAuthStore } from '../stores/auth';
import { NPageHeader, NGrid, NGi, NCard, NList, NListItem, NThing, NBadge } from 'naive-ui';

const authStore = useAuthStore();
const history = ref([]);
const rankings = ref([]);

onMounted(async () => {
  try {
    // 并发请求两个接口
    const [historyRes, rankingsRes] = await Promise.all([
      axios.get('/api/portal/history'),
      axios.get('/api/portal/rankings')
    ]);
    history.value = historyRes.data;
    rankings.value = rankingsRes.data;
  } catch (error) {
    console.error("加载用户中心数据失败:", error);
  }
});
</script>