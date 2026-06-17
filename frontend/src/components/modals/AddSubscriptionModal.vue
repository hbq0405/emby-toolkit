<template>
  <n-modal
    :show="props.show"
    @update:show="val => emit('update:show', val)"
    preset="card"
    style="width: 90%; max-width: 600px;"
    title="添加演员订阅"
    :bordered="false"
    size="huge"
    :on-after-leave="resetState"
  >
    <n-input
      v-model:value="searchQuery"
      placeholder="输入演员姓名进行搜索..."
      clearable
      @update:value="debouncedSearch"
    >
      <template #prefix>
        <n-icon><search-icon /></n-icon>
      </template>
    </n-input>

    <n-spin :show="searching" style="margin-top: 20px; width: 100%;">
      <div v-if="!searchQuery" class="empty-state">
        请输入演员名开始搜索
      </div>
      <div v-else-if="searchResults.length > 0" class="results-list">
        <n-list hoverable clickable>
          <n-list-item v-for="person in searchResults" :key="person.id" @click="handleSelectPerson(person)">
            <template #prefix>
              <n-avatar :size="48" :src="getImageUrl(person.profile_path)" object-fit="cover" />
            </template>
            <n-thing :title="person.name" :description="`主要领域: ${person.known_for_department}`">
              <template #footer>
                <n-text depth="3" style="font-size: 12px;">代表作: {{ person.known_for }}</n-text>
              </template>
            </n-thing>
            <template #suffix>
              <n-button type="primary" ghost>选择</n-button>
            </template>
          </n-list-item>
        </n-list>
      </div>
      <div v-else-if="!searching && searchQuery" class="empty-state">
        <n-empty :description="`找不到名为 “${searchQuery}” 的演员`" />
      </div>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { NModal, NInput, NIcon, NSpin, NList, NListItem, NThing, NAvatar, NText, NButton, NEmpty, useMessage } from 'naive-ui';
import { SearchOutline as SearchIcon } from '@vicons/ionicons5';
import axios from 'axios';
import { useDebounceFn } from '@vueuse/core';

// <<< 核心修改点 1：明确 props 和 emits >>>
const props = defineProps({
  show: Boolean,
});
const emit = defineEmits(['update:show', 'subscription-added']);

const message = useMessage();
const searchQuery = ref('');
const searching = ref(false);
const searchResults = ref([]);

const resetState = () => {
  searchQuery.value = '';
  searchResults.value = [];
  searching.value = false;
};

const searchActors = async () => {
  if (!searchQuery.value.trim()) {
    searchResults.value = [];
    return;
  }
  searching.value = true;
  try {
    const response = await axios.get('/api/actor-subscriptions/search', {
      params: { name: searchQuery.value },
    });
    searchResults.value = response.data;
  } catch (error) {
    console.error("搜索演员失败:", error);
    message.error('搜索演员失败，请检查后端日志。');
  } finally {
    searching.value = false;
  }
};

const debouncedSearch = useDebounceFn(searchActors, 300);

const getImageUrl = (path) => {
  return path ? `https://image.tmdb.org/t/p/w92${path}` : 'https://via.placeholder.com/92x138.png?text=N/A';
};

const handleSelectPerson = async (person) => {
  try {
    const response = await axios.post('/api/actor-subscriptions', {
      tmdb_person_id: person.id,
      actor_name: person.name,
      profile_path: person.profile_path,
    });
    message.success(response.data.message || '订阅成功！');
    
    // <<< 核心修改点 2：通知父组件，而不是自己做事 >>>
    emit('subscription-added'); // 通知父组件：“我添加成功了！”
    emit('update:show', false);   // 请求父组件关闭我
  } catch (error) {
    console.error("添加订阅失败:", error);
    message.error(error.response?.data?.error || '添加订阅失败，请稍后再试。');
  }
};
</script>

<style scoped>
.empty-state {
  padding: 40px 0;
  text-align: center;
  color: #999;
}
.results-list {
  max-height: 60vh;
  overflow-y: auto;
}
</style>