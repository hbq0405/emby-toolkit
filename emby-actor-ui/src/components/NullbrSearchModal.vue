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
import { PaperPlaneOutline as SendIcon, CopyOutline } from '@vicons/ionicons5';

const message = useMessage();

// 状态变量
const showModal = ref(false);
const pushing = ref(false);
const currentItemTitle = ref('');
const currentItemType = ref('movie');
const currentItemId = ref('');
const currentSeasonNumber = ref(null);
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
    // 1. 打印原始数据，方便排查 (请按 F12 看控制台)
    console.log('[NullbrSearch] 接收到的原始 Item:', item);

    // 2. 初始化基础信息
    currentItemTitle.value = item.title || item.name;
    
    // ★★★ 关键修复 1：必须先重置季号！防止残留上次的值 ★★★
    currentSeasonNumber.value = null;

    // 3. 统一类型转换
    let type = (item.media_type || item.item_type || 'movie').toLowerCase();
    if (['season', 'series', 'tv', 'episode'].includes(type)) {
        currentItemType.value = 'tv';
    } else {
        currentItemType.value = 'movie';
    }

    // 4. 智能识别 ID 和 季号
    // 优先检查是否有 series_tmdb_id (这通常意味着它是季或集)
    if (item.series_tmdb_id) {
        currentItemId.value = item.series_tmdb_id;
        
        // ★★★ 关键修复 2：更严谨的季号判断 (防止 0 被当成 false) ★★★
        if (item.season_number !== undefined && item.season_number !== null) {
            currentSeasonNumber.value = parseInt(item.season_number);
            console.log(`[Nullbr] 识别到季号: ${currentSeasonNumber.value}`);
        } else {
            console.warn('[Nullbr] 有 series_id 但缺少 season_number');
        }
    } else {
        // 电影或剧集主体
        currentItemId.value = item.tmdb_id || item.id;
        
        // ★★★ 兜底逻辑：如果是季类型，但没 series_id，尝试用 tmdb_id 搜 (虽然可能不准)
        // 但如果此时有 season_number，还是带上比较好
        if (type === 'season' && item.season_number !== undefined) {
             console.warn('[Nullbr] 警告: 季类型缺少 series_tmdb_id，搜索结果可能不准确');
             currentSeasonNumber.value = parseInt(item.season_number);
        }
    }
    
    console.log(`[NullbrSearch] 最终参数 -> ID: ${currentItemId.value}, Type: ${currentItemType.value}, Season: ${currentSeasonNumber.value}`);

    // 5. 重置列表状态
    resourcesMap['115'] = []; resourcesMap['magnet'] = []; resourcesMap['ed2k'] = [];
    loadedSources['115'] = false; loadedSources['magnet'] = false; loadedSources['ed2k'] = false;
    
    // 6. 打开并加载
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
            source_type: sourceType,
            season_number: currentSeasonNumber.value
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
    // 记得在 components 里加上 CopyOutline 如果你是用非 setup 语法，但在 setup 返回里直接用就行
    components: { NList, NListItem, NThing, NSpace, NTag, NEllipsis, NButton, NIcon },
    setup(props, { emit }) {
        // 在子组件内重新获取 message 实例，确保弹窗能显示
        const message = useMessage();

        const handleCopy = async (link) => {
            try {
                await navigator.clipboard.writeText(link);
                message.success('链接已复制到剪贴板');
            } catch (err) {
                console.error(err);
                message.error('复制失败，请手动复制');
            }
        };

        return { 
            SendIcon, 
            CopyOutline, // 返回图标
            handlePush: (res) => emit('push', res),
            handleCopy   // 返回复制方法
        }
    },
    template: `
        <n-list hoverable clickable>
          <n-list-item v-for="(res, index) in list" :key="index">
            <n-thing>
              <template #header>
                <n-space align="center">
                  <n-ellipsis style="max-width: 400px">{{ res.title }}</n-ellipsis>
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
              <n-space size="small">
                <!-- 新增：复制按钮 -->
                <n-button size="small" secondary type="info" @click.stop="handleCopy(res.link)">
                    <template #icon><n-icon :component="CopyOutline" /></template>
                    复制
                </n-button>
                <!-- 原有：推送按钮 -->
                <n-button size="small" type="primary" @click.stop="handlePush(res)">
                    <template #icon><n-icon :component="SendIcon" /></template>
                    推送
                </n-button>
              </n-space>
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