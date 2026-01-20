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

              <n-form-item label="推送方式">
                <n-radio-group v-model:value="config.push_mode" name="pushmode">
                  <n-radio-button value="cms">CMS</n-radio-button>
                  <n-radio-button value="115">115</n-radio-button>
                </n-radio-group>
              </n-form-item>

              <!-- ★★★ 115 配置区域 (仅当选中 115 时显示) ★★★ -->
              <!-- ★★★ 115 配置区域 (极简版) ★★★ -->
              <n-collapse-transition :show="config.push_mode === '115'">
                <div style="background: rgba(255,255,255,0.05); padding: 12px; border-radius: 8px; margin-bottom: 18px;">
                    
                    <!-- 状态栏：只显示有效/无效 -->
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <n-text depth="3" style="font-size: 12px;">账号状态</n-text>
                        <n-button size="tiny" secondary @click="check115Status" :loading="loading115Info">
                            <template #icon><n-icon><RefreshIcon /></n-icon></template>
                            检查连通性
                        </n-button>
                    </div>

                    <n-collapse-transition :show="!!p115Info">
                        <n-alert type="success" :show-icon="true" style="margin-bottom: 12px;">
                            <span style="font-weight: bold;">{{ p115Info?.msg || 'Cookie 有效' }}</span>
                        </n-alert>
                    </n-collapse-transition>
                    
                    <n-collapse-transition :show="!p115Info && config.p115_cookies && !loading115Info">
                         <n-alert type="warning" :show-icon="true" style="margin-bottom: 12px;">
                            <span style="font-size: 12px;">状态未知或 Cookie 无效，请检查。</span>
                        </n-alert>
                    </n-collapse-transition>

                    <!-- Cookies 输入框 (移除扫码按钮) -->
                    <n-form-item label="115 Cookies">
                        <n-input 
                            v-model:value="config.p115_cookies" 
                            type="textarea" 
                            placeholder="UID=...; CID=...; SEID=..." 
                            :rows="3"
                        />
                        <template #feedback>
                            <span style="font-size: 12px; color: #999;">请在本地浏览器登录 115 后抓取 Cookie 填入。</span>
                        </template>
                    </n-form-item>

                    <!-- 保存目录 -->
                    <n-form-item label="保存目录 CID">
                        <n-input 
                            v-model:value="config.p115_save_path_cid" 
                            placeholder="0 为根目录，请直接粘贴长数字" 
                            style="width: 100%" 
                        />
                        <template #feedback>
                            <span style="font-size: 12px; color: #999;">文件夹 ID (打开网页版文件夹，URL 最后那串数字)</span>
                        </template>
                    </n-form-item>
                </div>
              </n-collapse-transition>
              <!-- CMS 配置区域-->
              <n-collapse-transition :show="config.push_mode === 'cms'">
                  <div style="background: rgba(255,255,255,0.05); padding: 12px; border-radius: 8px; margin-bottom: 18px;">
                      <n-form-item label="CMS 地址">
                        <n-input v-model:value="config.cms_url" placeholder="例如: http://192.168.1.5:9527" />
                      </n-form-item>

                      <n-form-item label="CMS Token">
                        <n-input v-model:value="config.cms_token" type="password" show-password-on="click" placeholder="cloud_media_sync" />
                      </n-form-item>
                  </div>
              </n-collapse-transition>
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
                  <n-form-item label="容器（仅电影）" :show-feedback="false">
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
                  <n-form-item label="电影大小限制 (GB)">
                    <n-input-group>
                      <n-input-number v-model:value="config.filters.movie_min_size" :min="0" placeholder="Min" :show-button="false" style="width: 50%" />
                      <n-input-group-label>-</n-input-group-label>
                      <n-input-number v-model:value="config.filters.movie_max_size" :min="0" placeholder="Max" :show-button="false" style="width: 50%" />
                    </n-input-group>
                  </n-form-item>

                  <n-form-item label="剧集大小限制 (GB)">
                    <n-input-group>
                      <n-input-number v-model:value="config.filters.tv_min_size" :min="0" placeholder="Min" :show-button="false" style="width: 50%" />
                      <n-input-group-label>-</n-input-group-label>
                      <n-input-number v-model:value="config.filters.tv_max_size" :min="0" placeholder="Max" :show-button="false" style="width: 50%" />
                    </n-input-group>
                    <template #feedback><span style="font-size: 12px; color: #999;">0 表示不限制。剧集通常指单集或单季包大小。</span></template>
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
                <div class="responsive-grid">
                  <div v-for="item in searchResults" :key="item.id" class="grid-item">
                      <MediaCard 
                        :item="item" 
                        :loading="loadingResourcesId === item.id" 
                        @click="openResourceModal(item)" 
                      />
                  </div>
                </div>
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
                <div class="responsive-grid">
                  <div v-for="item in listItems" :key="item.id" class="grid-item">
                    <MediaCard 
                      :item="item" 
                      :loading="loadingResourcesId === item.id" 
                      @click="openResourceModal(item)" 
                    />
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
                  <n-tag v-if="res.is_zh_sub" type="success" size="small" :bordered="false">中字</n-tag>
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
import { useMessage, NIcon, NTag, NEllipsis, NSpace, NImage, NButton, NText, NDynamicInput, NTooltip, NCheckbox, NCheckboxGroup, NInputNumber, NSwitch, NSpin, NRadioGroup, NRadioButton, NCollapseTransition, NSelect } from 'naive-ui';
import { useClipboard } from '@vueuse/core';
import { 
  SettingsOutline as SettingsIcon, 
  Search as SearchIcon, 
  ListOutline as ListIcon,
  PaperPlaneOutline as SendIcon,
  PulseOutline as PulseIcon,
  QrCodeOutline as QrCodeIcon, 
  CheckmarkCircleOutline as CheckmarkCircleIcon,
  RefreshOutline as RefreshIcon,
  PersonCircleOutline as UserIcon
} from '@vicons/ionicons5';

const message = useMessage();
const { copy } = useClipboard();

// --- 配置相关 ---
const showConfig = ref(false);
const currentUsage = ref(0);
const config = reactive({
  api_key: '',
  push_mode: 'cms', 
  p115_cookies: '',
  p115_save_path_cid: '',
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
      movie_min_size: 0, movie_max_size: 0,
      tv_min_size: 0, tv_max_size: 0
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

const p115Info = ref(null);
const loading115Info = ref(false);

// 添加获取状态的方法
const check115Status = async () => {
    if (!config.p115_cookies) return;
    loading115Info.value = true;
    try {
        const res = await axios.get('/api/nullbr/115/status');
        if (res.data && res.data.data) {
            p115Info.value = res.data.data;
        }
    } catch (e) {
        p115Info.value = null;
        // 不弹窗报错了，以免打扰，只在控制台记录
        console.error('获取115状态失败', e);
    } finally {
        loading115Info.value = false;
    }
};

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/nullbr/config');
    if (res.data) {
      config.api_key = res.data.api_key || '';
      config.push_mode = res.data.push_mode || 'cms';
      config.p115_cookies = res.data.p115_cookies || '';
      config.p115_save_path_cid = res.data.p115_save_path_cid || 0;
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
      config.filters.movie_min_size = f.movie_min_size || f.min_size || 0;
      config.filters.movie_max_size = f.movie_max_size || f.max_size || 0;
      config.filters.tv_min_size = f.tv_min_size || f.min_size || 0;
      config.filters.tv_max_size = f.tv_max_size || f.max_size || 0;
    }
    const resPresets = await axios.get('/api/nullbr/presets');
    if (resPresets.data) {
      config.presets = resPresets.data;
    }
  } catch (error) {}
    if (config.p115_cookies) {
        check115Status();
    }
};

const saveConfig = async () => {
  saving.value = true;
  try {
    await axios.post('/api/nullbr/config', {
        api_key: config.api_key,
        push_mode: config.push_mode,
        p115_cookies: config.p115_cookies,
        p115_save_path_cid: config.p115_save_path_cid,
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
  if (config.push_mode === '115') {
        check115Status();
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
  year: item.release_date ? item.release_date.substring(0, 4) : '',
  in_library: item.in_library,
  subscription_status: item.subscription_status
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
  props: ['item', 'loading'],
  components: { NImage, NEllipsis, NSpace, NTag, NText, NSpin, NIcon },
  // 引入需要的图标
  setup() {
      return { 
          CloudDownloadOutline:  h(NIcon, null, { default: () => h(import('@vicons/ionicons5').then(m => m.CloudDownloadOutline)) }) 
      }
  },
  template: `
    <div class="media-card" @mouseenter="hover=true" @mouseleave="hover=false">
      
      <!-- Loading 遮罩 -->
      <div v-if="loading" class="loading-overlay">
        <n-spin size="medium" stroke="#ffffff" />
      </div>

      <!-- 海报容器 -->
      <div class="poster-wrapper">
        <img 
            :src="item.poster ? 'https://image.tmdb.org/t/p/w300' + item.poster : '/default-poster.png'" 
            class="media-poster"
            loading="lazy"
        />
        
        <!-- ★★★ 状态缎带 ★★★ -->
        <div v-if="item.in_library" class="ribbon ribbon-green"><span>已入库</span></div>
        <div v-else-if="item.subscription_status === 'SUBSCRIBED'" class="ribbon ribbon-blue"><span>已订阅</span></div>
        <div v-else-if="item.subscription_status === 'PAUSED'" class="ribbon ribbon-blue"><span>已暂停</span></div>
        <div v-else-if="item.subscription_status === 'WANTED'" class="ribbon ribbon-purple"><span>待订阅</span></div>
        <div v-else-if="item.subscription_status === 'REQUESTED'" class="ribbon ribbon-orange"><span>待审核</span></div>
        
        <!-- 评分角标 -->
        <div v-if="item.vote" class="rating-badge">
          {{ Number(item.vote).toFixed(1) }}
        </div>

        <!-- 底部遮罩信息区 -->
        <div class="overlay-info">
          <div class="text-content">
            <div class="media-title" :title="item.title">{{ item.title }}</div>
            <div class="media-meta-row">
              <span class="media-year">{{ item.year }}</span>
              <span class="media-dot">·</span>
              <span class="media-type">{{ item.media_type === 'tv' ? '剧集' : '电影' }}</span>
            </div>
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
.dashboard-card {
  height: 100%;
}

/* Grid 容器在父组件模板中，不需要 deep */
.responsive-grid {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
}

.grid-item {
  min-width: 0;
  height: 100%;
}

/* ★★★ 关键修复：给所有 MediaCard 内部样式加上 :deep() ★★★ */

:deep(.media-card) {
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  overflow: hidden;
  height: 100%;
  background-color: #222;
  display: flex;
  flex-direction: column;
  position: relative;
}

:deep(.media-card:hover) {
  transform: translateY(-4px);
  box-shadow: 0 8px 16px rgba(0,0,0,0.3);
  z-index: 10;
}

:deep(.poster-wrapper) {
  position: relative;
  width: 100%;
  aspect-ratio: 2 / 3;
  overflow: hidden;
}

:deep(.media-poster) {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  transition: transform 0.3s ease;
}

:deep(.media-card:hover .media-poster) {
  transform: scale(1.05);
}

:deep(.loading-overlay) {
  position: absolute; 
  top: 0; left: 0; right: 0; bottom: 0; 
  z-index: 20; 
  background: rgba(0,0,0,0.4); 
  display: flex; 
  align-items: center; 
  justify-content: center; 
  border-radius: 4px;
}

:deep(.overlay-info) {
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

:deep(.text-content) {
  flex: 1;
  min-width: 0;
}

:deep(.media-title) {
  color: #fff;
  font-weight: bold;
  font-size: 0.9em;
  line-height: 1.2;
  margin-bottom: 2px;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

:deep(.media-meta-row) {
  display: flex;
  align-items: center;
  color: rgba(255, 255, 255, 0.85);
  font-size: 0.75em;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
}

:deep(.media-dot) {
  margin: 0 4px;
}

:deep(.rating-badge) {
  position: absolute;
  top: 6px;
  right: 6px;
  background-color: rgba(0, 0, 0, 0.65);
  color: #f7b824;
  padding: 2px 5px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: bold;
  backdrop-filter: blur(2px);
  box-shadow: 0 1px 2px rgba(0,0,0,0.3);
  z-index: 5;
}

:deep(.ribbon) {
  position: absolute;
  top: -3px;
  left: -3px;
  width: 60px;
  height: 60px;
  overflow: hidden;
  z-index: 5;
}
:deep(.ribbon span) {
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
.qr-overlay {
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(255,255,255,0.9);
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: #333;
    border-radius: 8px;
}
:deep(.ribbon-green span) { background-color: #67c23a; }
:deep(.ribbon-blue span) { background-color: #409eff; }
:deep(.ribbon-purple span) { background-color: #722ed1; }
:deep(.ribbon-orange span) { background-color: #e6a23c; }
</style>