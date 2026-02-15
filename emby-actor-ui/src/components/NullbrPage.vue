<!-- src/components/NullbrPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <!-- 顶部标题栏 -->
    <n-page-header title="NULLBR 资源库" subtitle="连接 115 专属资源网络">
      <template #extra>
        <n-space align="center">
          <!-- 顶部简略配额显示 -->
          <n-tooltip trigger="hover" v-if="userProfile">
            <template #trigger>
              <n-tag :type="quotaColor" round :bordered="false" style="cursor: help;">
                <template #icon><n-icon :component="PulseIcon" /></template>
                今日剩余: {{ userProfile.daily_quota - userProfile.daily_used }}
              </n-tag>
            </template>
            <div>
              <div>等级: {{ userProfile.sub_name }}</div>
              <div>今日已用: {{ userProfile.daily_used }} / {{ userProfile.daily_quota }}</div>
              <div>本月已用: {{ userProfile.monthly_used }} / {{ userProfile.monthly_quota }}</div>
            </div>
          </n-tooltip>
          
          <n-button @click="refreshUserInfo" size="small" circle secondary>
            <template #icon><n-icon :component="RefreshIcon" /></template>
          </n-button>

          <n-button @click="showConfig = !showConfig" size="small" secondary>
            <template #icon><n-icon :component="SettingsIcon" /></template>
            配置
          </n-button>
        </n-space>
      </template>
    </n-page-header>

    <!-- 配置面板 -->
    <n-collapse-transition :show="showConfig">
      <n-card :bordered="false" class="dashboard-card" style="margin-top: 16px; margin-bottom: 16px;">
        <template #header>
          <n-space align="center" justify="space-between">
            <span>接入配置</span>
            <n-button tag="a" href="https://nullbr.com/manage" target="_blank" secondary type="primary" size="small">
              <template #icon><n-icon><LinkIcon /></n-icon></template>
              获取 NULLBR Key
            </n-button>
          </n-space>
        </template>
        
        <n-form label-placement="top" :model="config" label-width="auto">
          <n-grid cols="1 1000:2" :x-gap="40" :y-gap="24">
            
            <!-- ================= 左侧：账户与连接 ================= -->
            <n-gi>
              <div class="section-title">
                <n-icon><ServerIcon /></n-icon> 账户与连接
              </div>
              
              <n-form-item label="NULLBR API Key">
                <n-input-group>
                  <n-input v-model:value="config.api_key" type="password" show-password-on="click" placeholder="请输入 API Key" />
                  <n-button type="primary" ghost @click="saveConfigAndRefresh">保存并刷新</n-button>
                </n-input-group>
              </n-form-item>

              <!-- 用户信息卡片 -->
              <div v-if="userProfile" class="user-card">
                <div class="user-card-header">
                  <div class="user-level">
                    <n-tag :type="levelColor" size="small">{{ userProfile.sub_name.toUpperCase() }}</n-tag>
                    <span class="expire-date" v-if="userProfile.expires_at">到期: {{ userProfile.expires_at }}</span>
                    <span class="expire-date" v-else>永久免费</span>
                  </div>
                </div>
                <div class="quota-info">
                  <div class="quota-row">
                    <span>今日配额</span>
                    <span>{{ userProfile.daily_used }} / {{ userProfile.daily_quota }}</span>
                  </div>
                  <n-progress 
                    type="line" 
                    :percentage="Math.min((userProfile.daily_used / userProfile.daily_quota) * 100, 100)" 
                    :color="quotaColorCode"
                    :height="6"
                    :show-indicator="false"
                  />
                  <div class="quota-row" style="margin-top: 8px;">
                    <span>本月配额</span>
                    <span>{{ userProfile.monthly_used }} / {{ userProfile.monthly_quota }}</span>
                  </div>
                  <n-progress 
                    type="line" 
                    :percentage="Math.min((userProfile.monthly_used / userProfile.monthly_quota) * 100, 100)" 
                    status="success"
                    :height="6"
                    :show-indicator="false"
                  />
                </div>
                
                <!-- 兑换码区域 -->
                <n-divider style="margin: 12px 0;" />
                <n-input-group size="small">
                  <n-input v-model:value="redeemCodeInput" placeholder="输入兑换码升级/续费...1-2分钟生效" />
                  <n-button type="primary" secondary @click="handleRedeem" :loading="redeeming">
                    兑换
                  </n-button>
                  <!-- ★★★ 新增：购买按钮 ★★★ -->
                  <n-button type="warning" @click="showBuyModal = true">
                    <template #icon><n-icon><CartIcon /></n-icon></template>
                    购买/续费
                  </n-button>
                </n-input-group>
              </div>
              <n-alert v-else type="warning" style="margin-bottom: 16px;">
                未获取到用户信息。请检查 API Key 是否正确，或检查网络连接。
              </n-alert>

              <n-form-item label="启用数据源">
                <n-checkbox-group v-model:value="config.enabled_sources">
                  <n-space item-style="display: flex;">
                    <n-checkbox value="115" label="115网盘" />
                    <n-checkbox value="magnet" label="磁力链" />
                    <n-checkbox value="ed2k" label="电驴(Ed2k)" />
                  </n-space>
                </n-checkbox-group>
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
                </n-form-item>
              </div>

              <!-- CMS 模块 -->
              <div class="sub-module">
                <div class="sub-module-header">
                  <span class="title">CMS 通知 (可选)</span>
                  <n-tag size="small" :bordered="false">自动整理</n-tag>
                </div>
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
    <!-- 购买套餐模态框 -->
    <n-modal v-model:show="showBuyModal" style="width: 900px; max-width: 95%;">
      <n-card :bordered="false" size="huge" role="dialog" aria-modal="true" style="background: #1a1a1a; color: #fff;">
        <template #header>
          <div style="text-align: center; color: #fff; font-size: 24px;">选择订阅计划</div>
        </template>
        
        <div class="pricing-container">
          <!-- 免费版 -->
          <div class="pricing-card free">
            <div class="plan-name">FREE</div>
            <div class="plan-price">免费</div>
            <div class="plan-features">
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 每日详情页: 50次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API每分钟请求: 25/次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API配额: 50/日，1,500/月</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 浏览影视库</div>
              <div class="feature disabled"><n-icon color="#f56c6c"><CloseIcon /></n-icon> 个人列表同步</div>
              <div class="feature disabled"><n-icon color="#f56c6c"><CloseIcon /></n-icon> MCP功能</div>
            </div>
            <n-button disabled block ghost>当前计划</n-button>
          </div>

          <!-- 白银版 -->
          <div class="pricing-card silver">
            <div class="plan-badge">推荐</div>
            <div class="plan-name">SILVER</div>
            <div class="plan-price">¥15 <span class="period">/月</span></div>
            <div class="plan-features">
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 每日详情页: 200次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API每分钟请求: 60/次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API配额: 300/日，1万/月</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 浏览影视库</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 个人列表同步</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> MCP功能</div>
            </div>
            <n-button type="info" block @click="selectPlan('Silver', 15)">购买兑换码</n-button>
          </div>

          <!-- 黄金版 -->
          <div class="pricing-card golden">
            <div class="plan-name">GOLDEN</div>
            <div class="plan-price">¥25 <span class="period">/月</span></div>
            <div class="plan-features">
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 每日详情页: 500次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API每分钟请求: 100/次</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> API配额: 1000/日，1万/月</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 浏览影视库</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> 个人列表同步</div>
              <div class="feature"><n-icon color="#67c23a"><CheckIcon /></n-icon> MCP功能</div>
            </div>
            <n-button type="warning" block @click="selectPlan('Golden', 25)">购买兑换码</n-button>
          </div>
        </div>

        <!-- 支付弹层 (覆盖在卡片上) -->
        <n-collapse-transition :show="!!selectedPlanName">
          <div class="payment-overlay" v-if="selectedPlanName">
            <n-divider />
            <div class="payment-box">
              <div class="payment-left">
                <div class="pay-title">扫码支付 <span style="color: #e6a23c">{{ selectedPlanPrice }}元</span></div>
                <div class="pay-desc">请使用微信扫码，备注您的联系方式</div>
                <img src="/img/wechat_pay.png" class="qr-code" alt="收款码" />
              </div>
              <div class="payment-right">
                <div class="step-item">
                  <div class="step-num">1</div>
                  <div class="step-text">扫描左侧二维码支付 <b>{{ selectedPlanPrice }}元</b></div>
                </div>
                <div class="step-item">
                  <div class="step-num">2</div>
                  <div class="step-text">支付成功后，截图保存凭证</div>
                </div>
                <div class="step-item">
                  <div class="step-num">3</div>
                  <div class="step-text">点击下方按钮联系发码</div>
                </div>
                <div class="step-item">
                  <div class="step-num">4</div>
                  <div class="step-text">输入兑换码点击兑换，1-2分钟生效</div>
                </div>
                
                <!-- 替换成你的 TG 链接 -->
                <n-button type="primary" size="large" tag="a" href="https://t.me/hbq0405" target="_blank" style="margin-top: 20px;">
                  <template #icon><n-icon><PaperPlaneIcon /></n-icon></template>
                  联系 TG 发货
                </n-button>
                <n-button size="small" text style="margin-top: 10px; color: #999;" @click="selectedPlanName = null">
                  返回选择其他计划
                </n-button>
              </div>
            </div>
          </div>
        </n-collapse-transition>

      </n-card>
    </n-modal>
    <!-- 资源选择弹窗 -->
    <NullbrSearchModal ref="nullbrModalRef" />
  </n-layout>
</template>

<script setup>
import { ref, reactive, onMounted, h, defineComponent, computed } from 'vue';
import axios from 'axios';
import { useMessage, NIcon, NTag, NEllipsis, NSpace, NImage, NButton, NText, NDynamicInput, NTooltip, NCheckbox, NCheckboxGroup, NInputNumber, NSwitch, NSpin, NCollapseTransition, NTabs, NTabPane, NModal, NLayout, NLayoutSider, NLayoutContent, NPageHeader, NCard, NAlert, NForm, NFormItem, NGrid, NGi, NDivider, NInput, NInputGroup, NInputGroupLabel, NMenu, NEmpty, NProgress } from 'naive-ui';
import { useClipboard } from '@vueuse/core';
import NullbrSearchModal from './modals/NullbrSearchModal.vue';
import { 
  SettingsOutline as SettingsIcon, 
  Search as SearchIcon, 
  ListOutline as ListIcon,
  PulseOutline as PulseIcon,
  RefreshOutline as RefreshIcon,
  ServerOutline as ServerIcon,
  FilterOutline as FilterIcon,
  LinkOutline as LinkIcon,
  SaveOutline as SaveIcon,
  CartOutline as CartIcon,
  CheckmarkCircleOutline as CheckIcon,
  CloseCircleOutline as CloseIcon,
  PaperPlaneOutline as PaperPlaneIcon
} from '@vicons/ionicons5';

const message = useMessage();

// --- 配置相关 ---
const showConfig = ref(false);
const config = reactive({
  api_key: '',
  p115_cookies: '',
  p115_save_path_cid: '',
  cms_url: '',    
  cms_token: '',
  enabled_sources: ['115', 'magnet', 'ed2k'], 
  presets: [],
  filters: { resolutions: [], qualities: [], containers: [], require_zh: false, movie_min_size: 0, movie_max_size: 0, tv_min_size: 0, tv_max_size: 0 }
});

// 用户信息与兑换
const userProfile = ref(null);
const redeemCodeInput = ref('');
const redeeming = ref(false);

const quotaColor = computed(() => {
  if (!userProfile.value) return 'default';
  const ratio = (userProfile.value.daily_quota - userProfile.value.daily_used) / userProfile.value.daily_quota;
  if (ratio <= 0) return 'error';
  if (ratio < 0.2) return 'warning';
  return 'success';
});

const quotaColorCode = computed(() => {
  if (!userProfile.value) return '#18a058';
  const ratio = (userProfile.value.daily_quota - userProfile.value.daily_used) / userProfile.value.daily_quota;
  if (ratio <= 0) return '#d03050';
  if (ratio < 0.2) return '#f0a020';
  return '#18a058';
});

const levelColor = computed(() => {
    const name = userProfile.value?.sub_name?.toLowerCase() || '';
    if (name.includes('gold')) return 'warning';
    if (name.includes('silver')) return 'info';
    return 'default';
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

const refreshUserInfo = async () => {
    if (!config.api_key) return;
    try {
        const res = await axios.get('/api/nullbr/user/info');
        if (res.data && res.data.data) {
            userProfile.value = res.data.data;
            // 成功获取后清除错误提示（如果有状态变量控制的话）
        }
    } catch (e) {
        userProfile.value = null;
        const errMsg = e.response?.data?.message || e.message;
        // 如果是 401/403，说明 Key 错；如果是 500，可能是网络问题
        if (e.response?.status === 401 || e.response?.status === 403) {
             message.error("API Key 无效，请检查");
        } else {
             message.warning(`获取用户信息失败: ${errMsg}`);
        }
    }
};

const handleRedeem = async () => {
    if (!redeemCodeInput.value) return;
    redeeming.value = true;
    try {
        const res = await axios.post('/api/nullbr/user/redeem', { code: redeemCodeInput.value });
        if (res.data && res.data.status === 'success') {
            message.success(res.data.data.message || '兑换成功');
            redeemCodeInput.value = '';
            refreshUserInfo(); // 刷新信息
        } else {
            message.error(res.data.message || '兑换失败');
        }
    } catch (e) {
        message.error(e.response?.data?.message || '请求失败');
    } finally {
        redeeming.value = false;
    }
};

const loadConfig = async () => {
  try {
    const res = await axios.get('/api/nullbr/config');
    if (res.data) {
      Object.assign(config, res.data);
    }
    const resPresets = await axios.get('/api/nullbr/presets');
    if (resPresets.data) config.presets = resPresets.data;
    
    // 加载完配置后，获取用户信息
    refreshUserInfo();
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
  check115Status();
};

const saveConfigAndRefresh = async () => {
    await axios.post('/api/nullbr/config', config);
    message.success('API Key 已保存');
    refreshUserInfo();
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

// --- 搜索与列表 (保持原有逻辑) ---
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
      refreshUserInfo(); // 搜索后刷新配额
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
      refreshUserInfo(); // 刷新配额
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

// 购买兑换码相关逻辑 
const showBuyModal = ref(false);
const selectedPlanName = ref(null);
const selectedPlanPrice = ref(0);

const selectPlan = (name, price) => {
  selectedPlanName.value = name;
  selectedPlanPrice.value = price;
};

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

.filter-box { padding: 0 4px; }

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

/* 用户卡片样式 */
.user-card {
  background: linear-gradient(135deg, rgba(24, 160, 88, 0.1) 0%, rgba(32, 128, 240, 0.1) 100%);
  border: 1px solid rgba(128, 128, 128, 0.1);
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 16px;
}
.user-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}
.user-level {
  display: flex;
  align-items: center;
  gap: 8px;
}
.expire-date {
  font-size: 12px;
  color: #666;
}
.quota-info {
  font-size: 12px;
}
.quota-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
  color: #555;
}
/* ★★★ 新增：定价表样式 ★★★ */
.pricing-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 20px;
  margin-top: 20px;
}

.pricing-card {
  background: #252525;
  border-radius: 12px;
  padding: 24px;
  text-align: center;
  border: 1px solid #333;
  position: relative;
  transition: transform 0.3s;
}

.pricing-card:hover {
  transform: translateY(-5px);
  border-color: #555;
}

.pricing-card.silver { border-top: 4px solid #409eff; }
.pricing-card.golden { border-top: 4px solid #e6a23c; }

.plan-badge {
  position: absolute;
  top: 0;
  right: 0;
  background: #409eff;
  color: white;
  font-size: 12px;
  padding: 2px 8px;
  border-bottom-left-radius: 8px;
  border-top-right-radius: 8px;
}

.plan-name {
  font-size: 18px;
  font-weight: bold;
  color: #fff;
  margin-bottom: 10px;
}

.plan-price {
  font-size: 32px;
  font-weight: bold;
  color: #fff;
  margin-bottom: 20px;
}

.plan-price .period {
  font-size: 14px;
  color: #999;
  font-weight: normal;
}

.plan-features {
  text-align: left;
  margin-bottom: 24px;
  font-size: 13px;
  color: #ccc;
}

.feature {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.feature.disabled {
  color: #666;
  text-decoration: line-through;
}

/* 支付弹层样式 */
.payment-box {
  display: flex;
  flex-wrap: wrap;
  gap: 30px;
  background: #2c2c2c;
  padding: 20px;
  border-radius: 8px;
}

.payment-left {
  flex: 1;
  text-align: center;
  min-width: 200px;
  border-right: 1px solid #444;
}

.payment-right {
  flex: 1.5;
  display: flex;
  flex-direction: column;
  justify-content: center;
  min-width: 250px;
}

.pay-title { font-size: 18px; font-weight: bold; margin-bottom: 5px; }
.pay-desc { font-size: 12px; color: #999; margin-bottom: 15px; }
.qr-code { width: 180px; height: 180px; border-radius: 8px; }

.step-item {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 15px;
}

.step-num {
  width: 24px;
  height: 24px;
  background: #409eff;
  color: white;
  border-radius: 50%;
  text-align: center;
  line-height: 24px;
  font-size: 12px;
  font-weight: bold;
}

.step-text { font-size: 14px; color: #ddd; }
</style>