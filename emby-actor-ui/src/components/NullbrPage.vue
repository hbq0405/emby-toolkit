<!-- src/components/NullbrPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <n-page-header title="NULLBR 资源库" subtitle="连接 115 专属资源网络 (Beta)">
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

    <!-- 配置面板 -->
    <n-collapse-transition :show="showConfig">
      <n-card title="接入配置" :bordered="false" class="dashboard-card" style="margin-top: 16px; margin-bottom: 16px;">
        <n-alert type="info" style="margin-bottom: 16px;">
          NULLBR 是一个第三方资源索引服务，您需要先<n-button tag="a" href="https://nullbr.online/manage" target="_blank" secondary size="small">注册账号</n-button>获取 API Key。
        </n-alert>

        <n-form label-placement="top">
          <!-- ★★★ 修改点：改为三列并排布局 (响应式：小屏1列，中屏2列，大屏3列) ★★★ -->
          <n-grid cols="1 850:2 1300:3" :x-gap="32" :y-gap="24">
            
            <!-- 第一列：基础与推送设置 -->
            <n-gi>
              <n-divider title-placement="left" style="margin-top: 0; font-size: 14px;">基础与推送设置</n-divider>
              
              <n-form-item label="NULLBR API Key">
                <n-input 
                  v-model:value="config.api_key" 
                  type="password" 
                  show-password-on="click"
                  placeholder="请输入 NULLBR API Key" 
                />
              </n-form-item>

              <n-form-item label="启用数据源 (节省配额)">
                <n-checkbox-group v-model:value="config.enabled_sources">
                  <n-space>
                    <n-checkbox value="115" label="115网盘" />
                    <n-checkbox value="magnet" label="磁力链" />
                    <n-checkbox value="ed2k" label="电驴(Ed2k)" />
                  </n-space>
                </n-checkbox-group>
                <template #feedback>
                    <span style="font-size: 12px; color: #999;">每开启一个源，点击资源时消耗 1 次配额。只选 115 可最省配额。</span>
                </template>
              </n-form-item>

              <!-- API 限制设置  -->
              <n-grid :cols="2" :x-gap="12">
                <n-gi>
                    <n-form-item label="每日调用上限">
                        <n-input-number v-model:value="config.daily_limit" :min="10" placeholder="默认100" />
                    </n-form-item>
                </n-gi>
                <n-gi>
                    <n-form-item label="请求间隔 (秒)">
                        <n-input-number v-model:value="config.request_interval" :min="1" :step="0.5" placeholder="默认5" />
                    </n-form-item>
                </n-gi>
              </n-grid>

              <n-form-item label="CMS 地址">
                <n-input v-model:value="config.cms_url" placeholder="例如: http://192.168.1.5:9527" />
              </n-form-item>

              <n-form-item label="CMS Token">
                <n-input v-model:value="config.cms_token" type="password" show-password-on="click" placeholder="cloud_media_sync" />
                <template #feedback>
                    <span style="font-size: 12px; color: #888;">CMS token</span>
                </template>
              </n-form-item>
            </n-gi>

            <!-- 第二列：资源过滤设置 (移至中间) -->
            <n-gi>
               <n-divider title-placement="left" style="margin-top: 0; font-size: 14px;">资源过滤设置</n-divider>
               <!-- 使用垂直布局适应列宽 -->
               <n-space vertical size="medium">
                  
                  <!-- 分辨率 -->
                  <n-form-item label="分辨率" :show-feedback="false">
                    <n-checkbox-group v-model:value="config.filters.resolutions">
                      <n-space>
                        <n-checkbox value="2160p" label="4K" />
                        <n-checkbox value="1080p" label="1080p" />
                        <n-checkbox value="720p" label="720p" />
                      </n-space>
                    </n-checkbox-group>
                  </n-form-item>

                  <!-- 质量 -->
                  <n-form-item label="质量/版本" :show-feedback="false">
                    <n-checkbox-group v-model:value="config.filters.qualities">
                      <n-space>
                        <n-checkbox value="Remux" label="Remux" />
                        <n-checkbox value="HDR10" label="HDR" />
                        <n-checkbox value="Dolby Vision" label="DoVi" />
                        <n-checkbox value="BluRay" label="BluRay" />
                        <n-checkbox value="WEB-DL" label="WEB-DL" />
                      </n-space>
                    </n-checkbox-group>
                  </n-form-item>

                  <!-- 容器 & 字幕 -->
                  <n-form-item label="容器" :show-feedback="false">
                    <n-space vertical>
                        <n-checkbox-group v-model:value="config.filters.containers">
                            <n-space>
                                <n-checkbox value="mkv" label="MKV" />
                                <n-checkbox value="mp4" label="MP4" />
                                <n-checkbox value="ts" label="TS" />
                                <n-checkbox value="iso" label="ISO" />
                            </n-space>
                        </n-checkbox-group>
                        <n-switch v-model:value="config.filters.require_zh">
                          <template #checked>中文字幕</template>
                          <template #unchecked>不限制字幕</template>
                        </n-switch>
                    </n-space>
                  </n-form-item>

                  <!-- 大小限制 -->
                  <n-form-item label="文件大小 (GB)">
                    <n-input-group>
                      <n-input-number v-model:value="config.filters.min_size" :min="0" placeholder="Min" :show-button="false" style="width: 50%" />
                      <n-input-group-label>-</n-input-group-label>
                      <n-input-number v-model:value="config.filters.max_size" :min="0" placeholder="Max" :show-button="false" style="width: 50%" />
                    </n-input-group>
                    <template #feedback><span style="font-size: 12px; color: #999;">0 表示不限制</span></template>
                  </n-form-item>

               </n-space>
            </n-gi>

            <!-- 第三列：自定义精选片单 -->
            <n-gi>
              <n-divider title-placement="left" style="margin-top: 0; font-size: 14px;">自定义精选片单</n-divider>
              <n-alert type="info" style="margin-bottom: 12px;" :show-icon="false">
                添加您喜欢的 NULLBR 片单 ID。
              </n-alert>

              <!-- 增加最大高度和滚动条，防止列表过长破坏三列平衡 -->
              <div style="max-height: 450px; overflow-y: auto; padding-right: 4px;">
                  <n-dynamic-input v-model:value="config.presets" :on-create="onCreatePreset">
                    <template #default="{ value }">
                      <div style="display: flex; align-items: center; width: 100%; gap: 8px;">
                        <n-input v-model:value="value.name" placeholder="名称" style="flex: 1; min-width: 0;" />
                        <n-input v-model:value="value.id" placeholder="ID" style="width: 110px; flex-shrink: 0;" />
                      </div>
                    </template>
                  </n-dynamic-input>
              </div>

              <n-space justify="end" style="margin-top: 10px;">
                <n-button size="tiny" @click="resetPresets">恢复默认片单</n-button>
              </n-space>
            </n-gi>

          </n-grid>

          <!-- 底部按钮 -->
          <n-divider style="margin: 16px 0;" />
          <n-space justify="space-between">
            <n-button tag="a" href="https://nullbr.online/manage" target="_blank" secondary size="small">
              获取 NULLBR Key
            </n-button>
            <n-button type="primary" @click="saveConfig" :loading="saving">
              保存全部配置
            </n-button>
          </n-space>
        </n-form>
      </n-card>
    </n-collapse-transition>

    <!-- Tabs 切换搜索和片单 (保持不变) -->
    <n-tabs type="line" animated style="margin-top: 16px;">
      <!-- ... (后续代码保持不变) ... -->
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
                <n-grid cols="3 520:4 800:5 1000:6 1400:8" :x-gap="12" :y-gap="12">
                   <n-gi v-for="item in searchResults" :key="item.id">
                      <MediaCard 
                        :item="item" 
                        :loading="loadingResourcesId === item.id" 
                        @click="openResourceModal(item)" 
                      />
                   </n-gi>
                </n-grid>
             </n-spin>
          </div>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="lists" tab="✨ 精选片单">
        <n-layout has-sider style="min-height: 600px; background: none;">
          <n-layout-sider width="260" content-style="padding-right: 16px; background: none;" :native-scrollbar="false">
            <n-menu
              :options="presetMenuOptions"
              :value="currentListId"
              @update:value="handleListChange"
            />
          </n-layout-sider>

          <n-layout-content content-style="padding-left: 4px; background: none;">
            <n-spin :show="loadingList">
              <div v-if="listItems.length > 0">
                <n-grid cols="3 520:4 800:5 1000:6 1400:8" :x-gap="12" :y-gap="12">
                  <n-gi v-for="item in listItems" :key="item.id">
                    <MediaCard 
                      :item="item" 
                      :loading="loadingResourcesId === item.id" 
                      @click="openResourceModal(item)" 
                    />
                  </n-gi>
                </n-grid>
                
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

    <!-- 资源选择弹窗 (保持不变) -->
    <n-modal
      v-model:show="showModal"
      preset="card"
      title="选择资源版本"
      style="width: 800px; max-width: 90%;"
    >
      <n-spin :show="pushing">
        <n-empty v-if="currentResources.length === 0" description="该条目暂无资源 (或被过滤)" />
        
        <n-list v-else hoverable clickable>
          <n-list-item v-for="(res, index) in currentResources" :key="index">
            <n-thing>
              <template #header>
                <n-space align="center">
                  <n-tag 
                    :type="res.source_type === '115' ? 'success' : (res.source_type === 'MAGNET' ? 'error' : 'info')" 
                    size="small" 
                    round
                  >
                    {{ res.source_type }}
                  </n-tag>
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
                </n-space>
              </template>
            </n-thing>
            
            <template #suffix>
              <n-space>
                <n-button size="small" type="primary" @click="confirmPush(res)">
                  <template #icon><n-icon :component="SendIcon" /></template>
                  推送
                </n-button>
              </n-space>
            </template>
          </n-list-item>
        </n-list>
      </n-spin>
    </n-modal>
  </n-layout>
</template>

<script setup>
// ... (Script 部分保持不变，请确保包含上一步中增加的 filters 逻辑) ...
import { ref, reactive, onMounted, h, defineComponent, computed } from 'vue';
import axios from 'axios';
import { useMessage, NIcon, NTag, NEllipsis, NSpace, NImage, NButton, NText, NDynamicInput, NTooltip, NCheckbox, NCheckboxGroup, NInputNumber, NSwitch, NSpin } from 'naive-ui';
import { useClipboard } from '@vueuse/core';
import { 
  SettingsOutline as SettingsIcon, 
  Search as SearchIcon, 
  ListOutline as ListIcon,
  PaperPlaneOutline as SendIcon,
  PulseOutline as PulseIcon
} from '@vicons/ionicons5';

const message = useMessage();
const { copy } = useClipboard();

// --- 配置相关 ---
const showConfig = ref(false);
const currentUsage = ref(0);
const config = reactive({
  api_key: '',
  cms_url: '',    
  cms_token: '',
  daily_limit: 100, 
  request_interval: 5,
  enabled_sources: ['115', 'magnet', 'ed2k'], 
  presets: [],
  filters: {
      resolutions: [],
      qualities: [],
      containers: [],
      require_zh: false,
      min_size: 0,
      max_size: 0
  }
});
// 计算属性 
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

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/nullbr/config');
    if (res.data) {
      config.api_key = res.data.api_key || '';
      config.cms_url = res.data.cms_url || '';       
      config.cms_token = res.data.cms_token || '';
      config.daily_limit = res.data.daily_limit || 100; 
      config.request_interval = res.data.request_interval || 5;
      currentUsage.value = res.data.current_usage || 0;
      config.enabled_sources = res.data.enabled_sources || ['115', 'magnet', 'ed2k'];
      
      const f = res.data.filters || {};
      config.filters.resolutions = f.resolutions || [];
      config.filters.qualities = f.qualities || [];
      config.filters.containers = f.containers || [];
      config.filters.require_zh = !!f.require_zh;
      config.filters.min_size = f.min_size || 0;
      config.filters.max_size = f.max_size || 0;
    }
    const resPresets = await axios.get('/api/nullbr/presets');
    if (resPresets.data) {
      config.presets = resPresets.data;
    }
  } catch (error) {}
};

const saveConfig = async () => {
  saving.value = true;
  try {
    await axios.post('/api/nullbr/config', {
        api_key: config.api_key,
        cms_url: config.cms_url,       
        cms_token: config.cms_token,
        daily_limit: config.daily_limit, 
        request_interval: config.request_interval,
        enabled_sources: config.enabled_sources,
        filters: config.filters
    });
    await axios.post('/api/nullbr/presets', { presets: config.presets });
    message.success('全部配置已保存');
    showConfig.value = false;
    loadPresets(); 
  } catch (error) {
    message.error('保存失败');
  } finally {
    saving.value = false;
  }
};

const onCreatePreset = () => {
  return { name: '', id: '' };
};

const resetPresets = async () => {
  try {
    const res = await axios.delete('/api/nullbr/presets');
    if (res.data && res.data.data) {
      config.presets = res.data.data; 
      presetLists.value = res.data.data;
      presetMenuOptions.value = res.data.data.map(list => ({
        label: list.name,
        key: list.id,
        icon: () => h(NIcon, null, { default: () => h(ListIcon) })
      }));
      message.success('已恢复默认片单');
    }
  } catch (error) {
    message.error('重置失败');
  }
};

// ... (其余搜索、片单、弹窗逻辑保持不变) ...
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
const showModal = ref(false);
const currentResources = ref([]);
const loadingResourcesId = ref(null);
const pushing = ref(false);
const currentItemTitle = ref('');

const handleSearch = async () => {
  if (!searchKeyword.value) return;
  searching.value = true;
  searchResults.value = [];
  try {
    const res = await axios.post('/api/nullbr/search', {
      keyword: searchKeyword.value,
      page: 1
    });
    if (res.data && res.data.data && res.data.data.list) {
      searchResults.value = res.data.data.list.map(mapApiItemToUi);
      message.success(`找到 ${res.data.data.total} 个资源`);
    }
  } catch (error) {
    message.error('搜索失败: ' + (error.response?.data?.message || error.message));
  } finally {
    searching.value = false;
  }
};

const loadPresets = async () => {
  try {
    const res = await axios.get('/api/nullbr/presets');
    presetLists.value = res.data;
    presetMenuOptions.value = res.data.map(list => ({
    label: () => h(
        NTooltip,
        { placement: 'right', keepAliveOnHover: false },
        {
        trigger: () => h('span', null, list.name),
        default: () => list.name
        }
    ),
    key: list.id,
    icon: () => h(NIcon, null, { default: () => h(ListIcon) })
    }));
    if (presetLists.value.length > 0) {
      handleListChange(presetLists.value[0].id);
    }
  } catch (e) {
    message.error('加载片单列表失败');
  }
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
    const res = await axios.post('/api/nullbr/list', {
      list_id: currentListId.value,
      page: listPage.value
    });
    if (res.data && res.data.data && res.data.data.list) {
      const newItems = res.data.data.list.map(mapApiItemToUi);
      if (newItems.length === 0) {
          hasMore.value = false;
      } else {
          listItems.value.push(...newItems);
      }
    }
  } catch (error) {
    message.error('获取片单内容失败');
  } finally {
    loadingList.value = false;
  }
};

const mapApiItemToUi = (item) => ({
  id: item.tmdbid || item.id,
  tmdb_id: item.tmdbid || item.id,
  title: item.title || item.name,
  poster: item.poster, 
  media_type: item.media_type || 'movie',
  overview: item.overview,
  vote: item.vote || item.vote_average,
  year: item.release_date ? item.release_date.substring(0, 4) : ''
});

const openResourceModal = async (item) => {
  loadingResourcesId.value = item.id;
  currentItemTitle.value = item.title;
  currentResources.value = [];
  try {
    const res = await axios.post('/api/nullbr/resources', {
      tmdb_id: item.tmdb_id,
      media_type: item.media_type
    });
    
    loadConfig(); 

    if (res.data && res.data.data) {
      currentResources.value = res.data.data;
      showModal.value = true;
    } else {
      message.warning('未找到相关资源');
    }
  } catch (error) {
    message.error('获取资源列表失败: ' + (error.response?.data?.message || error.message));
    loadConfig();
  } finally {
    loadingResourcesId.value = null;
  }
};

const confirmPush = async (resource) => {
  pushing.value = true;
  try {
    await axios.post('/api/nullbr/push', {
      link: resource.link,
      title: resource.title || currentItemTitle.value
    });
    message.success('已推送');
  } catch (error) {
    message.error('推送失败: ' + (error.response?.data?.message || error.message));
  } finally {
    pushing.value = false;
  }
};

const MediaCard = defineComponent({
  props: ['item', 'loading'], // 1. 新增 loading 属性
  components: { NImage, NEllipsis, NSpace, NTag, NText, NSpin }, // 2. 注册 NSpin 组件
  template: `
    <div class="media-card" style="cursor: pointer; position: relative; transition: transform 0.2s;" @mouseenter="hover=true" @mouseleave="hover=false" :style="{ transform: hover ? 'translateY(-3px)' : 'none' }">
      
      <!-- 3. 新增加载遮罩层 -->
      <div v-if="loading" style="position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 10; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; border-radius: 4px;">
        <n-spin size="medium" stroke="#ffffff" />
      </div>

      <n-image 
        preview-disabled 
        :src="item.poster ? 'https://image.tmdb.org/t/p/w300' + item.poster : '/default-poster.png'" 
        object-fit="cover"
        style="width: 100%; aspect-ratio: 2/3; border-radius: 4px; box-shadow: 0 2px 6px rgba(0,0,0,0.1);" 
      />
      <div style="margin-top: 4px;">
        <n-ellipsis style="font-weight: 600; font-size: 12px; line-height: 1.3;">{{ item.title }}</n-ellipsis>
        <n-space justify="space-between" align="center" style="margin-top: 1px;">
           <n-text depth="3" style="font-size: 11px;">{{ item.year }}</n-text>
           <n-tag v-if="item.vote" type="warning" size="tiny" round :bordered="false" style="font-size: 9px; height: 16px; padding: 0 4px;">{{ Number(item.vote).toFixed(1) }}</n-tag>
        </n-space>
      </div>
      <n-tag 
        style="position: absolute; top: 3px; right: 3px; opacity: 0.9; font-size: 9px; height: 16px; padding: 0 3px;" 
        size="tiny" 
        :type="item.media_type === 'tv' ? 'success' : 'info'"
      >
        {{ item.media_type === 'tv' ? '剧' : '影' }}
      </n-tag>
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
.dashboard-card {
  height: 100%;
}
</style>