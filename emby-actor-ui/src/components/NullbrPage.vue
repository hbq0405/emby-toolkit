<!-- src/components/NullbrPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <!-- 顶部标题栏 -->
    <n-page-header title="NULLBR 资源库" subtitle="连接 115 专属资源网络">
      <template #extra>
        <n-tooltip trigger="hover">
          <template #trigger>
            <n-tag :type="quotaColor" round :bordered="false" style="margin-right: 8px; cursor: help;">
              <template #icon>
                <n-icon :component="PulseIcon" />
              </template>
              今日剩余: {{ remainingQuota }} / {{ config.daily_limit }}
            </n-tag>
          </template>
          API 调用配额 (仅获取下载链接时消耗)
        </n-tooltip>
        <n-button @click="showConfig = !showConfig" size="small" secondary>
          <template #icon><n-icon :component="SettingsIcon" /></template>
          配置
        </n-button>
      </template>
    </n-page-header>

    <!-- 配置面板 (移除多余的 template 标签) -->
    <n-collapse-transition :show="showConfig">
      <n-card :bordered="false" class="dashboard-card" style="margin-top: 16px; margin-bottom: 16px;">
        <!-- 顶部提示 -->
        <template #header>
          <n-space align="center" justify="space-between">
            <span>接入配置</span>
            <n-button tag="a" href="https://nullbr.com/manage" target="_blank" secondary type="primary" size="small">
              <template #icon><n-icon><LinkIcon /></n-icon></template>
              获取 NULLBR Key
            </n-button>
          </n-space>
        </template>
        
        <n-alert type="info" style="margin-bottom: 20px;" :show-icon="true">
          NULLBR 是一个第三方资源索引服务，配置下方信息后可转存资源到115网盘。
        </n-alert>

        <n-form label-placement="top" :model="config" label-width="auto">
          <!-- 改为两列布局：左侧基础设施，右侧业务规则 -->
          <n-grid cols="1 1000:2" :x-gap="40" :y-gap="24">
            
            <!-- ================= 左侧：连接与账号设置 ================= -->
            <n-gi>
              <div class="section-title">
                <n-icon><ServerIcon /></n-icon> 基础连接
              </div>
              
              <n-form-item label="NULLBR API Key">
                <n-input v-model:value="config.api_key" type="password" show-password-on="click" placeholder="请输入您的 API Key" />
              </n-form-item>

              <n-grid :cols="2" :x-gap="12">
                <n-gi>
                  <n-form-item label="每日调用上限">
                    <n-input-number v-model:value="config.daily_limit" :min="10" placeholder="100" style="width: 100%" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="请求间隔 (秒)">
                    <n-input-number v-model:value="config.request_interval" :min="1" :step="0.5" placeholder="5" style="width: 100%">
                      <template #suffix>秒</template>
                    </n-input-number>
                  </n-form-item>
                </n-gi>
              </n-grid>

              <n-form-item label="启用数据源 (多选)">
                <n-checkbox-group v-model:value="config.enabled_sources">
                  <n-space item-style="display: flex;">
                    <n-checkbox value="115" label="115网盘" />
                    <n-checkbox value="magnet" label="磁力链" />
                    <n-checkbox value="ed2k" label="电驴(Ed2k)" />
                  </n-space>
                </n-checkbox-group>
                <template #feedback>程序自动从前往后搜索资源。</template>
              </n-form-item>

              <!-- 115 模块 -->
              <div class="sub-module">
                <div class="sub-module-header">
                  <span class="title">115 网盘设置</span>
                  <n-button size="tiny" secondary type="success" @click="check115Status" :loading="loading115Info">
                    检查连通性
                  </n-button>
                </div>
                
                <n-collapse-transition :show="!!p115Info">
                  <n-alert type="success" :show-icon="true" style="margin-bottom: 12px; padding: 8px 12px;">
                    {{ p115Info?.msg || 'Cookie 有效' }}
                  </n-alert>
                </n-collapse-transition>
                
                <n-form-item label="Cookies" :show-feedback="false" style="margin-bottom: 12px;">
                  <n-input v-model:value="config.p115_cookies" type="textarea" placeholder="UID=...; CID=...; SEID=..." :rows="3" size="small"/>
                </n-form-item>
                
                <n-form-item label="保存目录 CID">
                  <n-input v-model:value="config.p115_save_path_cid" placeholder="0 为根目录" />
                  <template #feedback>网页版文件夹 URL 最后那串数字</template>
                </n-form-item>
              </div>

              <!-- CMS 模块 -->
              <div class="sub-module">
                <div class="sub-module-header">
                  <span class="title">CMS 通知 (可选)</span>
                  <n-tag size="small" :bordered="false">自动整理</n-tag>
                </div>
                <n-text depth="3" style="font-size: 12px; display: block; margin-bottom: 10px;">
                  任务添加成功后，通知 CMS 生成 strm 文件。
                </n-text>
                <n-grid :cols="2" :x-gap="12">
                  <n-gi>
                    <n-form-item label="CMS 地址" :show-feedback="false">
                      <n-input v-model:value="config.cms_url" placeholder="http://ip:port" />
                    </n-form-item>
                  </n-gi>
                  <n-gi>
                    <n-form-item label="CMS Token" :show-feedback="false">
                      <n-input v-model:value="config.cms_token" type="password" show-password-on="click" placeholder="Token" />
                    </n-form-item>
                  </n-gi>
                </n-grid>
              </div>
            </n-gi>

            <!-- ================= 右侧：过滤与片单 ================= -->
            <n-gi>
              <div class="section-title">
                <n-icon><FilterIcon /></n-icon> 资源过滤规则
              </div>

              <div class="filter-box">
                <n-grid :cols="2" :x-gap="24">
                  <n-gi>
                    <n-form-item label="分辨率偏好">
                        <n-checkbox-group v-model:value="config.filters.resolutions">
                          <n-space vertical :size="4">
                            <n-checkbox value="2160p" label="4K (2160p)" />
                            <n-checkbox value="1080p" label="1080p" />
                            <n-checkbox value="720p" label="720p" />
                          </n-space>
                        </n-checkbox-group>
                    </n-form-item>
                  </n-gi>
                  <n-gi>
                    <n-form-item label="质量/版本">
                        <n-checkbox-group v-model:value="config.filters.qualities">
                          <n-space vertical :size="4">
                            <n-checkbox value="Remux" label="Remux (原盘)" />
                            <n-checkbox value="HDR10" label="HDR" />
                            <n-checkbox value="Dolby Vision" label="Dolby Vision" />
                            <n-checkbox value="WEB-DL" label="WEB-DL" />
                          </n-space>
                        </n-checkbox-group>
                    </n-form-item>
                  </n-gi>
                </n-grid>
                
                <n-divider style="margin: 12px 0" />

                <n-form-item label="容器格式 (仅电影)">
                  <n-space align="center">
                      <n-checkbox-group v-model:value="config.filters.containers">
                        <n-space>
                          <n-checkbox value="mkv" label="MKV" />
                          <n-checkbox value="mp4" label="MP4" />
                          <n-checkbox value="iso" label="ISO" />
                        </n-space>
                      </n-checkbox-group>
                      <n-divider vertical />
                      <n-switch v-model:value="config.filters.require_zh" size="small">
                        <template #checked>必须含中文字幕</template>
                        <template #unchecked>不限字幕</template>
                      </n-switch>
                  </n-space>
                </n-form-item>

                <n-grid :cols="2" :x-gap="12">
                  <n-gi>
                    <n-form-item label="电影大小 (GB)">
                      <n-input-group>
                        <n-input-number v-model:value="config.filters.movie_min_size" :min="0" placeholder="0" :show-button="false" />
                        <n-input-group-label style="background: transparent; border-left: 0; border-right: 0;">-</n-input-group-label>
                        <n-input-number v-model:value="config.filters.movie_max_size" :min="0" placeholder="∞" :show-button="false" />
                      </n-input-group>
                    </n-form-item>
                  </n-gi>
                  <n-gi>
                    <n-form-item label="剧集大小 (GB/集)">
                      <n-input-group>
                        <n-input-number v-model:value="config.filters.tv_min_size" :min="0" placeholder="0" :show-button="false" />
                        <n-input-group-label style="background: transparent; border-left: 0; border-right: 0;">-</n-input-group-label>
                        <n-input-number v-model:value="config.filters.tv_max_size" :min="0" placeholder="∞" :show-button="false" />
                      </n-input-group>
                    </n-form-item>
                  </n-gi>
                </n-grid>
              </div>

              <div class="section-title" style="margin-top: 24px;">
                <n-icon><ListIcon /></n-icon> 自定义精选片单
              </div>
              
              <div class="preset-container">
                <n-dynamic-input v-model:value="config.presets" :on-create="onCreatePreset">
                  <template #default="{ value }">
                    <div class="preset-item">
                      <n-input v-model:value="value.name" placeholder="片单名称" style="flex: 1;" />
                      <n-input v-model:value="value.id" placeholder="ID" style="width: 100px; text-align: center;" />
                    </div>
                  </template>
                </n-dynamic-input>
                <div v-if="!config.presets || config.presets.length === 0" style="text-align: center; color: #666; padding: 20px;">
                  暂无片单，请点击下方按钮添加
                </div>
              </div>
              <n-space justify="end" style="margin-top: 8px;">
                <n-button size="tiny" tertiary @click="resetPresets">恢复默认推荐</n-button>
              </n-space>

            </n-gi>
          </n-grid>

          <n-divider />
          
          <n-space justify="end">
            <n-button type="primary" size="large" @click="saveConfig" :loading="saving">
              <template #icon><n-icon><SaveIcon /></n-icon></template>
              保存全部配置
            </n-button>
          </n-space>
        </n-form>
      </n-card>
    </n-collapse-transition>

    <!-- 主体内容 Tabs -->
    <n-tabs type="line" animated style="margin-top: 16px;">
      <n-tab-pane name="search" tab="🔍 资源搜索">
        <n-card :bordered="false" class="dashboard-card">
          <n-input-group>
            <n-input v-model:value="searchKeyword" placeholder="输入电影/剧集名称..." @keyup.enter="handleSearch" />
            <n-button type="primary" ghost @click="handleSearch" :loading="searching">
              <template #icon><n-icon :component="SearchIcon" /></template>
              搜索
            </n-button>
          </n-input-group>
          <div style="margin-top: 20px;">
             <n-spin :show="searching">
                <n-empty v-if="!searchResults.length && !searching" description="暂无数据" />
                <div class="responsive-grid">
                  <div v-for="item in searchResults" :key="item.id" class="grid-item">
                      <MediaCard :item="item" :loading="loadingResourcesId === item.id" @click="openResourceModal(item)" />
                  </div>
                </div>
             </n-spin>
          </div>
        </n-card>
      </n-tab-pane>
      <n-tab-pane name="lists" tab="✨ 精选片单">
        <n-layout has-sider style="min-height: 600px; background: none;">
          <n-layout-sider width="260" content-style="padding-right: 16px; background: none;" :native-scrollbar="false">
            <n-menu :options="presetMenuOptions" :value="currentListId" @update:value="handleListChange" />
          </n-layout-sider>
          <n-layout-content content-style="padding-left: 4px; background: none;">
            <n-spin :show="loadingList">
              <div v-if="listItems.length > 0">
                <div class="responsive-grid">
                  <div v-for="item in listItems" :key="item.id" class="grid-item">
                    <MediaCard :item="item" :loading="loadingResourcesId === item.id" @click="openResourceModal(item)" />
                  </div>
                </div>
                <div style="display: flex; justify-content: center; margin-top: 20px; margin-bottom: 20px;">
                   <n-button v-if="hasMore" @click="loadMoreList" :loading="loadingMore" size="small">加载更多</n-button>
                   <n-text v-else depth="3" style="font-size: 12px;">没有更多了</n-text>
                </div>
              </div>
              <n-empty v-else description="选择一个片单开始浏览" style="margin-top: 100px;" />
            </n-spin>
          </n-layout-content>
        </n-layout>
      </n-tab-pane>
    </n-tabs>
    <!-- 资源选择弹窗 -->
    <NullbrSearchModal ref="nullbrModalRef" />
  </n-layout>
</template>

<script setup>
import { ref, reactive, onMounted, h, defineComponent, computed } from 'vue';
import axios from 'axios';
import { useMessage, NIcon, NTag, NEllipsis, NSpace, NImage, NButton, NText, NDynamicInput, NTooltip, NCheckbox, NCheckboxGroup, NInputNumber, NSwitch, NSpin, NRadioGroup, NRadioButton, NCollapseTransition, NSelect, NTabs, NTabPane, NList, NListItem, NThing, NModal, NLayout, NLayoutSider, NLayoutContent, NPageHeader, NCard, NAlert, NForm, NFormItem, NGrid, NGi, NDivider, NInput, NInputGroup, NInputGroupLabel, NMenu, NEmpty } from 'naive-ui';
import { useClipboard } from '@vueuse/core';
import NullbrSearchModal from './modals/NullbrSearchModal.vue';
import { 
  SettingsOutline as SettingsIcon, 
  Search as SearchIcon, 
  ListOutline as ListIcon,
  PaperPlaneOutline as SendIcon,
  PulseOutline as PulseIcon,
  RefreshOutline as RefreshIcon,
  // ★★★ 补全缺失的图标 ★★★
  ServerOutline as ServerIcon,
  FilterOutline as FilterIcon,
  LinkOutline as LinkIcon,
  SaveOutline as SaveIcon
} from '@vicons/ionicons5';

const message = useMessage();
const { copy } = useClipboard();

// --- 配置相关 ---
const showConfig = ref(false);
const currentUsage = ref(0);
const config = reactive({
  api_key: '',
  p115_cookies: '',
  p115_save_path_cid: '',
  cms_url: '',    
  cms_token: '',
  daily_limit: 100, 
  request_interval: 5,
  enabled_sources: ['115', 'magnet', 'ed2k'], 
  presets: [],
  filters: { resolutions: [], qualities: [], containers: [], require_zh: false, movie_min_size: 0, movie_max_size: 0, tv_min_size: 0, tv_max_size: 0 }
});

const remainingQuota = computed(() => {
  const left = config.daily_limit - currentUsage.value;
  return left < 0 ? 0 : left;
});
const quotaColor = computed(() => {
  const ratio = remainingQuota.value / config.daily_limit;
  if (ratio <= 0) return 'error';
  if (ratio < 0.2) return 'warning';
  return 'success';
});
const saving = ref(false);
const p115Info = ref(null);
const loading115Info = ref(false);

const check115Status = async () => {
    if (!config.p115_cookies) return;
    loading115Info.value = true;
    try {
        const res = await axios.get('/api/nullbr/115/status');
        if (res.data && res.data.data) p115Info.value = res.data.data;
    } catch (e) { p115Info.value = null; } finally { loading115Info.value = false; }
};

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/nullbr/config');
    if (res.data) {
      Object.assign(config, res.data);
      currentUsage.value = res.data.current_usage || 0;
    }
    const resPresets = await axios.get('/api/nullbr/presets');
    if (resPresets.data) config.presets = resPresets.data;
  } catch (error) {}
  if (config.p115_cookies) check115Status();
};

const saveConfig = async () => {
  saving.value = true;
  try {
    await axios.post('/api/nullbr/config', config);
    await axios.post('/api/nullbr/presets', { presets: config.presets });
    message.success('全部配置已保存');
    showConfig.value = false;
    loadPresets(); 
  } catch (error) { message.error('保存失败'); } finally { saving.value = false; }
  // 总是检查 115 状态
  check115Status();
};

const onCreatePreset = () => ({ name: '', id: '' });
const resetPresets = async () => {
  try {
    const res = await axios.delete('/api/nullbr/presets');
    if (res.data && res.data.data) {
      config.presets = res.data.data; 
      presetLists.value = res.data.data;
      presetMenuOptions.value = res.data.data.map(list => ({ label: list.name, key: list.id, icon: () => h(NIcon, null, { default: () => h(ListIcon) }) }));
      message.success('已恢复默认片单');
    }
  } catch (error) { message.error('重置失败'); }
};

// --- 搜索与列表 ---
const searchKeyword = ref('');
const searching = ref(false);
const searchResults = ref([]);
const presetLists = ref([]);
const currentListId = ref(null);
const listItems = ref([]);
const loadingList = ref(false);
const listPage = ref(1);
const hasMore = ref(true);
const loadingMore = ref(false);
const presetMenuOptions = ref([]);

const handleSearch = async () => {
  if (!searchKeyword.value) return;
  searching.value = true;
  searchResults.value = [];
  try {
    const res = await axios.post('/api/nullbr/search', { keyword: searchKeyword.value, page: 1 });
    if (res.data && res.data.data && res.data.data.list) {
      searchResults.value = res.data.data.list.map(mapApiItemToUi);
      message.success(`找到 ${res.data.data.total} 个资源`);
    }
  } catch (error) { message.error('搜索失败: ' + (error.response?.data?.message || error.message)); } finally { searching.value = false; }
};

const loadPresets = async () => {
  try {
    const res = await axios.get('/api/nullbr/presets');
    presetLists.value = res.data;
    presetMenuOptions.value = res.data.map(list => ({
        label: () => h(NTooltip, { placement: 'right', keepAliveOnHover: false }, { trigger: () => h('span', null, list.name), default: () => list.name }),
        key: list.id,
        icon: () => h(NIcon, null, { default: () => h(ListIcon) })
    }));
    if (presetLists.value.length > 0) handleListChange(presetLists.value[0].id);
  } catch (e) { message.error('加载片单列表失败'); }
};

const handleListChange = async (key) => {
  currentListId.value = key;
  listPage.value = 1;
  listItems.value = [];
  hasMore.value = true;
  await fetchListContent();
};

const loadMoreList = async () => {
    listPage.value++;
    loadingMore.value = true;
    await fetchListContent();
    loadingMore.value = false;
}

const fetchListContent = async () => {
  if (listPage.value === 1) loadingList.value = true;
  try {
    const res = await axios.post('/api/nullbr/list', { list_id: currentListId.value, page: listPage.value });
    if (res.data && res.data.data && res.data.data.list) {
      const newItems = res.data.data.list.map(mapApiItemToUi);
      if (newItems.length === 0) hasMore.value = false;
      else listItems.value.push(...newItems);
    }
  } catch (error) { message.error('获取片单内容失败'); } finally { loadingList.value = false; }
};

const mapApiItemToUi = (item) => ({
  id: item.tmdbid || item.id,
  tmdb_id: item.tmdbid || item.id,
  title: item.title || item.name,
  poster: item.poster, 
  media_type: item.media_type || 'movie',
  overview: item.overview,
  vote: item.vote || item.vote_average,
  year: item.release_date ? item.release_date.substring(0, 4) : '',
  in_library: item.in_library,
  subscription_status: item.subscription_status
});

const nullbrModalRef = ref(null);
const loadingResourcesId = ref(null);

const openResourceModal = (item) => {
  if (nullbrModalRef.value) {
    nullbrModalRef.value.open(item);
  }
};

const MediaCard = defineComponent({
  props: ['item', 'loading'],
  components: { NImage, NEllipsis, NSpace, NTag, NText, NSpin, NIcon },
  template: `
    <div class="media-card" @mouseenter="hover=true" @mouseleave="hover=false">
      <div v-if="loading" class="loading-overlay"><n-spin size="medium" stroke="#ffffff" /></div>
      <div class="poster-wrapper">
        <img :src="item.poster ? 'https://image.tmdb.org/t/p/w300' + item.poster : '/default-poster.png'" class="media-poster" loading="lazy"/>
        <div v-if="item.in_library" class="ribbon ribbon-green"><span>已入库</span></div>
        <div v-else-if="item.subscription_status === 'SUBSCRIBED'" class="ribbon ribbon-blue"><span>已订阅</span></div>
        <div v-else-if="item.subscription_status === 'PAUSED'" class="ribbon ribbon-blue"><span>已暂停</span></div>
        <div v-else-if="item.subscription_status === 'WANTED'" class="ribbon ribbon-purple"><span>待订阅</span></div>
        <div v-else-if="item.subscription_status === 'REQUESTED'" class="ribbon ribbon-orange"><span>待审核</span></div>
        <div v-if="item.vote" class="rating-badge">{{ Number(item.vote).toFixed(1) }}</div>
        <div class="overlay-info">
          <div class="text-content">
            <div class="media-title" :title="item.title">{{ item.title }}</div>
            <div class="media-meta-row"><span class="media-year">{{ item.year }}</span><span class="media-dot">·</span><span class="media-type">{{ item.media_type === 'tv' ? '剧集' : '电影' }}</span></div>
          </div>
        </div>
      </div>
    </div>
  `,
  data() { return { hover: false } }
});

onMounted(() => {
  loadConfig();
  loadPresets();
});
</script>

<style scoped>
/* 样式保持不变 */
.dashboard-card { height: 100%; }
.responsive-grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); }
.grid-item { min-width: 0; height: 100%; }
:deep(.media-card) { cursor: pointer; transition: transform 0.2s ease, box-shadow 0.2s ease; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; height: 100%; background-color: #222; display: flex; flex-direction: column; position: relative; }
:deep(.media-card:hover) { transform: translateY(-4px); box-shadow: 0 8px 16px rgba(0,0,0,0.3); z-index: 10; }
:deep(.poster-wrapper) { position: relative; width: 100%; aspect-ratio: 2 / 3; overflow: hidden; }
:deep(.media-poster) { width: 100%; height: 100%; object-fit: cover; display: block; transition: transform 0.3s ease; }
:deep(.media-card:hover .media-poster) { transform: scale(1.05); }
:deep(.loading-overlay) { position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 20; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; border-radius: 4px; }
:deep(.overlay-info) { position: absolute; bottom: 0; left: 0; right: 0; background: linear-gradient(to top, rgba(0,0,0,0.9) 0%, rgba(0,0,0,0.6) 50%, transparent 100%); padding: 40px 8px 8px 8px; display: flex; justify-content: space-between; align-items: flex-end; pointer-events: none; }
:deep(.text-content) { flex: 1; min-width: 0; }
:deep(.media-title) { color: #fff; font-weight: bold; font-size: 0.9em; line-height: 1.2; margin-bottom: 2px; text-shadow: 0 1px 2px rgba(0,0,0,0.8); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
:deep(.media-meta-row) { display: flex; align-items: center; color: rgba(255, 255, 255, 0.85); font-size: 0.75em; text-shadow: 0 1px 2px rgba(0,0,0,0.8); }
:deep(.media-dot) { margin: 0 4px; }
:deep(.rating-badge) { position: absolute; top: 6px; right: 6px; background-color: rgba(0, 0, 0, 0.65); color: #f7b824; padding: 2px 5px; border-radius: 4px; font-size: 10px; font-weight: bold; backdrop-filter: blur(2px); box-shadow: 0 1px 2px rgba(0,0,0,0.3); z-index: 5; }
:deep(.ribbon) { position: absolute; top: -3px; left: -3px; width: 60px; height: 60px; overflow: hidden; z-index: 5; }
:deep(.ribbon span) { position: absolute; display: block; width: 85px; padding: 3px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.2); color: #fff; font-size: 10px; font-weight: bold; text-shadow: 0 1px 1px rgba(0,0,0,0.3); text-transform: uppercase; text-align: center; left: -16px; top: 10px; transform: rotate(-45deg); }
:deep(.ribbon-green span) { background-color: #67c23a; }
:deep(.ribbon-blue span) { background-color: #409eff; }
:deep(.ribbon-purple span) { background-color: #722ed1; }
:deep(.ribbon-orange span) { background-color: #e6a23c; }
/* 标题样式 */
.section-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 16px;
  color: var(--n-text-color);
  border-left: 4px solid var(--n-primary-color);
  padding-left: 10px;
}

/* 子模块卡片样式 (115, CMS) */
.sub-module {
  background-color: rgba(128, 128, 128, 0.05);
  border: 1px solid rgba(128, 128, 128, 0.1);
  border-radius: 8px;
  padding: 16px;
  margin-top: 16px;
}

.sub-module-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.sub-module-header .title {
  font-weight: bold;
  font-size: 14px;
}

/* 过滤区样式 */
.filter-box {
  padding: 0 4px;
}

/* 片单列表样式 */
.preset-container {
  background-color: rgba(128, 128, 128, 0.03);
  border-radius: 8px;
  padding: 12px;
  max-height: 400px;
  overflow-y: auto;
  border: 1px solid rgba(128, 128, 128, 0.1);
}

.preset-item {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
}
</style>