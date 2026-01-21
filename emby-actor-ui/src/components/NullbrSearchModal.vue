<!-- src/components/NullbrSearchModal.vue -->
<template>
  <n-modal
    v-model:show="showModal"
    preset="card"
    :title="currentItemTitle || 'NULLBR 资源搜索'"
    style="width: 800px; max-width: 90%;"
  >
    <n-spin :show="pushing">
      <n-tabs type="segment" v-model:value="activeResourceTab" @update:value="handleTabChange">
        <!-- 115 Tab -->
        <n-tab-pane name="115" tab="115网盘">
            <div v-if="loadingSource === '115'" class="loading-box">
                <n-spin size="medium" /><div class="loading-text">正在搜索 115...</div>
            </div>
            <n-empty v-else-if="!resourcesMap['115']?.length" description="未找到 115 资源" class="empty-box" />
            <ResourceList v-else :list="resourcesMap['115']" @push="confirmPush" />
        </n-tab-pane>

        <!-- 磁力 Tab -->
        <n-tab-pane name="magnet" tab="磁力链">
            <div v-if="loadingSource === 'magnet'" class="loading-box">
                <n-spin size="medium" /><div class="loading-text">正在搜索磁力链...</div>
            </div>
            <n-empty v-else-if="!resourcesMap['magnet']?.length" description="未找到磁力资源" class="empty-box" />
            <ResourceList v-else :list="resourcesMap['magnet']" @push="confirmPush" />
        </n-tab-pane>

        <!-- Ed2k Tab -->
        <n-tab-pane name="ed2k" tab="电驴(Ed2k)" :disabled="currentItemType === 'tv'">
            <div v-if="loadingSource === 'ed2k'" class="loading-box">
                <n-spin size="medium" /><div class="loading-text">正在搜索 Ed2k...</div>
            </div>
            <n-empty v-else-if="!resourcesMap['ed2k']?.length" description="未找到 Ed2k 资源" class="empty-box" />
            <ResourceList v-else :list="resourcesMap['ed2k']" @push="confirmPush" />
        </n-tab-pane>
      </n-tabs>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { ref, reactive, defineExpose, h, defineComponent } from 'vue';
import axios from 'axios';
import { useMessage, NModal, NSpin, NTabs, NTabPane, NEmpty, NList, NListItem, NThing, NSpace, NEllipsis, NTag, NButton, NIcon } from 'naive-ui';
import { PaperPlaneOutline as SendIcon } from '@vicons/ionicons5';

const message = useMessage();

// 状态变量
const showModal = ref(false);
const pushing = ref(false);
const currentItemTitle = ref('');
const currentItemType = ref('movie');
const currentItemId = ref('');
const activeResourceTab = ref('115');
const resourcesMap = reactive({ '115': [], 'magnet': [], 'ed2k': [] });
const loadedSources = reactive({ '115': false, 'magnet': false, 'ed2k': false });
const loadingSource = ref(null);

// 简单的配置检查，实际项目中建议用 Pinia 或全局状态管理
const apiKeyCheck = async () => {
    try {
        const res = await axios.get('/api/nullbr/config');
        if (!res.data?.api_key) {
            message.error("未配置 NULLBR API Key，请先去配置页面填写。");
            return false;
        }
        return true;
    } catch (e) {
        return true; // 接口报错暂不阻断，让后端拦截
    }
};

// ★★★ 暴露给父组件的方法 ★★★
const open = async (item) => {
    // 1. 检查配置 (可选)
    const hasKey = await apiKeyCheck(); 
    if (!hasKey) return;

    // 2. 初始化
    currentItemTitle.value = item.title || item.name; // 兼容 title 和 name
    currentItemId.value = item.tmdb_id || item.id;

    // ★★★ 核心修复：统一类型转换逻辑 ★★★
    // 目标：转换为 'movie' 或 'tv'
    
    let type = item.media_type || item.item_type || 'movie'; // 获取原始类型
    
    // 统一转小写处理
    type = type.toLowerCase();

    if (type === 'season' || type === 'series' || type === 'tv' || type === 'episode') {
        currentItemType.value = 'tv';
    } else {
        currentItemType.value = 'movie';
    }
    
    // 打印日志方便调试 (F12 Console)
    console.log(`[NullbrSearch] 打开模态框: ${currentItemTitle.value}, 原始类型: ${item.media_type || item.item_type}, 转换后: ${currentItemType.value}, ID: ${currentItemId.value}`);

    // ... (重置数据和 fetchResources 逻辑) ...
    resourcesMap['115'] = []; resourcesMap['magnet'] = []; resourcesMap['ed2k'] = [];
    loadedSources['115'] = false; loadedSources['magnet'] = false; loadedSources['ed2k'] = false;
    
    activeResourceTab.value = '115';
    showModal.value = true;
    fetchResources('115', true);
};

// 获取资源逻辑
const fetchResources = async (sourceType, autoCascade = false) => {
    if (loadedSources[sourceType]) return;
    
    loadingSource.value = sourceType;
    try {
        const res = await axios.post('/api/nullbr/resources', {
            tmdb_id: currentItemId.value,
            media_type: currentItemType.value,
            source_type: sourceType
        });
        
        const list = res.data.data || [];
        resourcesMap[sourceType] = list;
        loadedSources[sourceType] = true;

        // 级联加载
        if (list.length === 0 && autoCascade) {
            if (sourceType === '115') {
                activeResourceTab.value = 'magnet';
                await fetchResources('magnet', true);
            } else if (sourceType === 'magnet') {
                if (currentItemType.value === 'movie') {
                    activeResourceTab.value = 'ed2k';
                    await fetchResources('ed2k', false);
                }
            }
        }
    } catch (error) {
        message.error(`获取 ${sourceType} 失败: ` + (error.response?.data?.message || error.message));
    } finally {
        loadingSource.value = null;
    }
};

const handleTabChange = (value) => {
    activeResourceTab.value = value;
    fetchResources(value, false);
};

const confirmPush = async (resource) => {
  pushing.value = true;
  try {
    await axios.post('/api/nullbr/push', {
      link: resource.link,
      title: resource.title || currentItemTitle.value
    });
    message.success('已推送任务');
    // showModal.value = false; // 推送成功后是否关闭弹窗？看个人喜好，这里暂时不关
  } catch (error) {
    message.error('推送失败: ' + (error.response?.data?.message || error.message));
  } finally {
    pushing.value = false;
  }
};

// 内部列表组件
const ResourceList = defineComponent({
    props: ['list'],
    emits: ['push'],
    components: { NList, NListItem, NThing, NSpace, NTag, NEllipsis, NButton, NIcon },
    setup(props, { emit }) {
        return { SendIcon, handlePush: (res) => emit('push', res) }
    },
    template: `
        <n-list hoverable clickable>
          <n-list-item v-for="(res, index) in list" :key="index">
            <n-thing>
              <template #header>
                <n-space align="center">
                  <n-ellipsis style="max-width: 450px">{{ res.title }}</n-ellipsis>
                </n-space>
              </template>
              <template #description>
                <n-space size="small" align="center" style="margin-top: 4px;">
                  <n-tag type="warning" size="small" :bordered="false">{{ res.size }}</n-tag>
                  <n-tag v-if="res.resolution" size="small" :bordered="false">{{ res.resolution }}</n-tag>
                  <template v-if="Array.isArray(res.quality)">
                    <n-tag v-for="q in res.quality" :key="q" size="small" :bordered="false" style="opacity: 0.8;">{{ q }}</n-tag>
                  </template>
                  <n-tag v-else-if="res.quality" size="small" :bordered="false" style="opacity: 0.8;">{{ res.quality }}</n-tag>
                  <n-tag v-if="res.is_zh_sub" type="success" size="small" :bordered="false">中字</n-tag>
                </n-space>
              </template>
            </n-thing>
            <template #suffix>
              <n-button size="small" type="primary" @click="handlePush(res)">
                <template #icon><n-icon :component="SendIcon" /></template>
                推送
              </n-button>
            </template>
          </n-list-item>
        </n-list>
    `
});

// 暴露 open 方法
defineExpose({ open });
</script>

<style scoped>
.loading-box { padding: 40px; text-align: center; }
.loading-text { margin-top: 8px; color: #999; }
.empty-box { margin: 40px 0; }
</style>