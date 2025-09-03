<!-- src/components/CustomCollectionsManager.vue (V3.1 - 修复UI切换BUG版) -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="custom-collections-manager">
      <!-- 1. 页面头部 -->
      <n-page-header>
        <template #title>
          自建合集
        </template>
        <template #extra>
          <n-space>
            <n-tooltip>
              <template #trigger>
                <n-button @click="triggerMetadataSync" :loading="isSyncingMetadata" circle>
                  <template #icon><n-icon :component="SyncIcon" /></template>
                </n-button>
              </template>
              快速同步媒体元数据
            </n-tooltip>
            <n-button type="primary" ghost @click="handleSyncAll" :loading="isSyncingAll">
              <template #icon><n-icon :component="GenerateIcon" /></template>
              一键生成所有
            </n-button>
            <n-button type="primary" @click="handleCreateClick">
              <template #icon><n-icon :component="AddIcon" /></template>
              创建新合集
            </n-button>
          </n-space>
        </template>
        <template #footer>
          <n-alert title="操作提示" type="info" :bordered="false">
            <ul style="margin: 0; padding-left: 20px;">
              <li>自建合集是虚拟库的虚拟来源，任何通过规则筛选、RSS导入的合集都可以被虚拟成媒体库展示在首页（需通过配置的反代端口访问）。内置猫眼榜单提取自MP插件，感谢<a
                  href="https://github.com/baozaodetudou"
                  target="_blank"
                  style="font-size: 0.85em; margin-left: 8px; color: var(--n-primary-color); text-decoration: underline;"
                >逗猫佬</a>。</li>
              <li>在创建或生成“筛选规则”合集前，请先同步演员映射然后点击 <n-icon :component="SyncIcon" /> 按钮快速同步一次最新的媒体库元数据。修改媒体标签等不会变更Emby最后更新时间戳的需要到任务中心运行同步媒体数据并采用深度模式。</li>
              <li>您可以通过拖动每行最左侧的 <n-icon :component="DragHandleIcon" /> 图标来对合集进行排序，Emby虚拟库实时联动更新排序。</li>
            </ul>
          </n-alert>
        </template>
      </n-page-header>

      <!-- 2. 数据表格 -->
      <n-data-table
        ref="tableRef"
        :columns="columns"
        :data="collections"
        :loading="isLoading || isSavingOrder"
        :bordered="false"
        :single-line="false"
        style="margin-top: 24px;"
        :row-key="row => row.id"
      />
    </div>

    <!-- 3. 创建/编辑模态框 -->
    <n-modal
      v-model:show="showModal"
      preset="card"
      style="width: 90%; max-width: 850px;"
      :title="isEditing ? '编辑合集' : '创建新合集'"
      :bordered="false"
      size="huge"
    >
      <n-form
        ref="formRef"
        :model="currentCollection"
        :rules="formRules"
        label-placement="left"
        label-width="auto"
      >
        <n-form-item label="合集名称" path="name">
          <n-input v-model:value="currentCollection.name" placeholder="例如：周星驰系列" />
        </n-form-item>
        
        <n-form-item label="合集类型" path="type">
          <n-select
            v-model:value="currentCollection.type"
            :options="typeOptions"
            :disabled="isEditing"
            placeholder="请选择合集类型"
          />
        </n-form-item>

        <n-form-item v-if="currentCollection.type" label="合集内容" path="definition.item_type">
          <n-checkbox-group 
            v-model:value="currentCollection.definition.item_type"
            :disabled="isContentTypeLocked"
          >
            <n-space>
              <n-checkbox value="Movie">电影</n-checkbox>
              <n-checkbox value="Series">电视剧</n-checkbox>
            </n-space>
          </n-checkbox-group>
        </n-form-item>

        <!-- 榜单导入 (List) 类型的表单 -->
        <div v-if="currentCollection.type === 'list'">
          <!-- ★★★ 核心修正：移除 path="definition.source" 属性 ★★★ -->
          <n-form-item label="榜单来源">
            <n-select
              v-model:value="selectedBuiltInList"
              :options="builtInLists"
              placeholder="选择一个内置榜单或自定义"
            />
          </n-form-item>
          <n-form-item label="榜单URL" path="definition.url">
              <n-input 
                v-model:value="currentCollection.definition.url" 
                :placeholder="urlInputPlaceholder"
                :disabled="selectedBuiltInList !== 'custom'"
              />
              <template #feedback>
                选择内置榜单时URL将自动填充；选择自定义时请在此处输入RSS地址。
              </template>
          </n-form-item>
          <n-form-item label="数量限制" path="definition.limit">
            <n-input-number 
              v-model:value="currentCollection.definition.limit" 
              placeholder="留空不限制" 
              :min="1" 
              clearable 
              style="width: 100%;"
            />
            <template #feedback>
              仅导入榜单中的前 N 个项目。例如：输入 20 表示只处理 TOP 20。
            </template>
          </n-form-item>
        </div>

        <!-- 筛选规则 (Filter) 类型的表单 -->
        <div v-if="currentCollection.type === 'filter'">
          <n-form-item label="匹配逻辑">
            <n-radio-group v-model:value="currentCollection.definition.logic">
              <n-space>
                <n-radio value="AND">满足所有条件 (AND)</n-radio>
                <n-radio value="OR">满足任一条件 (OR)</n-radio>
              </n-space>
            </n-radio-group>
          </n-form-item>

          <n-form-item label="筛选规则" path="definition.rules">
            <div style="width: 100%;">
              <n-space v-for="(rule, index) in currentCollection.definition.rules" :key="index" style="margin-bottom: 12px;" align="center">
                <n-select v-model:value="rule.field" :options="staticFieldOptions" placeholder="字段" style="width: 150px;" clearable />
                <n-select v-model:value="rule.operator" :options="getOperatorOptionsForRow(rule)" placeholder="操作" style="width: 120px;" :disabled="!rule.field" clearable />
                <template v-if="rule.field === 'genres'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple filterable
                    placeholder="选择一个或多个类型"
                    :options="genreOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 180px;"
                  />
                  <n-select
                    v-else
                    v-model:value="rule.value"
                    filterable
                    placeholder="选择类型"
                    :options="genreOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1;"
                  />
                </template>
                <template v-else-if="rule.field === 'countries'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple filterable
                    placeholder="选择一个或多个地区"
                    :options="countryOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 180px;"
                  />
                  <n-select
                    v-else
                    v-model:value="rule.value"
                    filterable
                    placeholder="选择地区"
                    :options="countryOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1;"
                  />
                </template>
                <template v-else-if="rule.field === 'studios'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple
                    filterable
                    remote
                    placeholder="输入以搜索并添加工作室"
                    :options="studioOptions"
                    :loading="isSearchingStudios"
                    @search="handleStudioSearch"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                  />
                  <n-auto-complete
                    v-else
                    v-model:value="rule.value"
                    :options="studioOptions"
                    :loading="isSearchingStudios"
                    placeholder="边输入边搜索工作室"
                    @update:value="handleStudioSearch"
                    :disabled="!rule.operator"
                    clearable
                  />
                </template>
                <template v-else-if="rule.field === 'tags'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple
                    filterable
                    tag
                    placeholder="选择或输入标签"
                    :options="tagOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                  />
                  <n-select
                    v-else
                    v-model:value="rule.value"
                    filterable
                    tag
                    placeholder="选择或输入一个标签"
                    :options="tagOptions"
                    :disabled="!rule.operator"
                    clearable
                    style="flex-grow: 1;"
                  />
                </template>
                <template v-else-if="rule.field === 'unified_rating'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple
                    placeholder="选择一个或多个家长分级"
                    :options="unifiedRatingOptions" 
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                  />
                  <n-select
                    v-else
                    v-model:value="rule.value"
                    placeholder="选择一个家长分级"
                    :options="unifiedRatingOptions" 
                    :disabled="!rule.operator"
                    clearable
                    style="flex-grow: 1;"
                  />
                </template>
                <template v-else-if="rule.field === 'actors'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple filterable remote
                    placeholder="输入以搜索并添加演员"
                    :options="actorOptions"
                    :loading="isSearchingActors"
                    @search="handleActorSearch"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                  />
                  <n-auto-complete
                    v-else
                    v-model:value="rule.value"
                    :options="actorOptions"
                    :loading="isSearchingActors"
                    placeholder="边输入边搜索演员"
                    @update:value="handleActorSearch"
                    :disabled="!rule.operator"
                    clearable
                  />
                </template>
                <template v-else-if="rule.field === 'directors'">
                  <n-dynamic-tags
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    :disabled="!rule.operator"
                    style="flex-grow: 1;"
                  />
                  <n-input
                    v-else
                    v-model:value="rule.value"
                    placeholder="输入导演名称"
                    :disabled="!rule.operator"
                  />
                </template>
                <n-input-number
                  v-else-if="['release_date', 'date_added'].includes(rule.field)"
                  v-model:value="rule.value"
                  placeholder="天数"
                  :disabled="!rule.operator"
                  style="width: 180px;"
                >
                  <template #suffix>天内</template>
                </n-input-number>
                <n-input-number
                  v-else-if="['rating', 'release_year'].includes(rule.field)"
                  v-model:value="rule.value"
                  placeholder="数值"
                  :disabled="!rule.operator"
                  :show-button="false"
                  style="width: 180px;"
                />
                <n-input v-else v-model:value="rule.value" placeholder="值" :disabled="!rule.operator" />
                <n-button text type="error" @click="removeRule(index)">
                  <template #icon><n-icon :component="DeleteIcon" /></template>
                </n-button>
              </n-space>
              <n-button @click="addRule" dashed block>
                <template #icon><n-icon :component="AddIcon" /></template>
                添加条件
              </n-button>
            </div>
          </n-form-item>
        </div>
        <n-form-item label="内容排序">
            <n-input-group>
              <n-select
                v-model:value="currentCollection.definition.default_sort_by"
                :options="sortFieldOptions"
                placeholder="排序字段"
                style="width: 50%"
              />
              <n-select
                v-model:value="currentCollection.definition.default_sort_order"
                :options="sortOrderOptions"
                placeholder="排序顺序"
                style="width: 50%"
              />
            </n-input-group>
          </n-form-item>
          <n-divider title-placement="left" style="margin-top: 15px;">
            附加实时筛选 (可选)
          </n-divider>

          <n-form-item>
            <template #label>
              <n-space align="center">
                <span>启用实时用户数据筛选</span>
                <n-tooltip trigger="hover">
                  <template #trigger>
                    <n-icon :component="HelpIcon" />
                  </template>
                  开启后，此合集将根据每个用户的观看状态、收藏等实时变化。
                </n-tooltip>
              </n-space>
            </template>
            <n-switch v-model:value="currentCollection.definition.dynamic_filter_enabled" />
          </n-form-item>

          <div v-if="currentCollection.definition.dynamic_filter_enabled">
            <n-form-item label="动态筛选规则" path="definition.dynamic_rules">
              <div style="width: 100%;">
                <n-space v-for="(rule, index) in currentCollection.definition.dynamic_rules" :key="index" style="margin-bottom: 12px;" align="center">
                  
                  <!-- 1. 字段选择：使用 dynamicFieldOptions，这部分已正确 -->
                  <n-select v-model:value="rule.field" :options="dynamicFieldOptions" placeholder="字段" style="width: 150px;" clearable />
                  
                  <!-- 2. 操作符选择：使用 getOperatorOptionsForRow，这部分已正确 -->
                  <n-select v-model:value="rule.operator" :options="getOperatorOptionsForRow(rule)" placeholder="操作" style="width: 120px;" :disabled="!rule.field" clearable />
                  
                  <!-- 3. 值选择：根据不同的字段，渲染不同的输入组件 -->
                  <template v-if="rule.field === 'playback_status'">
                    <n-select
                      v-model:value="rule.value"
                      placeholder="选择播放状态"
                      :options="[
                        { label: '未播放', value: 'unplayed' },
                        { label: '播放中', value: 'in_progress' },
                        { label: '已播放', value: 'played' }
                      ]"
                      :disabled="!rule.operator"
                      style="flex-grow: 1; min-width: 180px;"
                    />
                  </template>
                  <template v-else-if="rule.field === 'is_favorite'">
                    <n-select
                      v-model:value="rule.value"
                      placeholder="选择收藏状态"
                      :options="[
                        { label: '已收藏', value: true },
                        { label: '未收藏', value: false }
                      ]"
                      :disabled="!rule.operator"
                      style="flex-grow: 1; min-width: 180px;"
                    />
                  </template>

                  <n-button text type="error" @click="removeDynamicRule(index)">
                    <template #icon><n-icon :component="DeleteIcon" /></template>
                  </n-button>
                </n-space>
                <n-button @click="addDynamicRule" dashed block>
                  <template #icon><n-icon :component="AddIcon" /></template>
                  添加动态条件
                </n-button>
              </div>
            </n-form-item>
          </div>
        <n-form-item label="状态" path="status" v-if="isEditing">
            <n-radio-group v-model:value="currentCollection.status">
                <n-space>
                    <n-radio value="active">启用</n-radio>
                    <n-radio value="paused">暂停</n-radio>
                </n-space>
            </n-radio-group>
        </n-form-item>

      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showModal = false">取消</n-button>
          <n-button type="primary" @click="handleSave" :loading="isSaving">保存</n-button>
        </n-space>
      </template>
    </n-modal>
    
    <n-modal v-model:show="showDetailsModal" preset="card" style="width: 90%; max-width: 1200px;" :title="detailsModalTitle" :bordered="false" size="huge">
      <div v-if="isLoadingDetails" class="center-container"><n-spin size="large" /></div>
      <div v-else-if="selectedCollectionDetails">
        <n-tabs type="line" animated>
          <n-tab-pane name="missing" :tab="`缺失${mediaTypeName} (${missingMediaInModal.length})`">
            <n-empty v-if="missingMediaInModal.length === 0" :description="`太棒了！没有已上映的缺失${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="media in missingMediaInModal" :key="media.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ media.title }}<br />({{ extractYear(media.release_date) || '未知年份' }})</div></div>
                  <template #action>
                    <n-button @click="subscribeMedia(media)" type="primary" size="small" block :loading="subscribing[media.tmdb_id]">
                      <template #icon><n-icon :component="CloudDownloadIcon" /></template>
                      订阅
                    </n-button>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>
          
          <n-tab-pane name="in_library" :tab="`已入库 (${inLibraryMediaInModal.length})`">
             <n-empty v-if="inLibraryMediaInModal.length === 0" :description="`该合集在媒体库中没有任何${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="media in inLibraryMediaInModal" :key="media.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ media.title }}<br />({{ extractYear(media.release_date) || '未知年份' }})</div></div>
                   <template #action>
                    <n-tag type="success" size="small" style="width: 100%; justify-content: center;">
                      <template #icon><n-icon :component="CheckmarkCircle" /></template>
                      已在库
                    </n-tag>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <n-tab-pane name="unreleased" :tab="`未上映 (${unreleasedMediaInModal.length})`">
            <n-empty v-if="unreleasedMediaInModal.length === 0" :description="`该合集没有已知的未上映${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="media in unreleasedMediaInModal" :key="media.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster"></template>
                  <div class="movie-info"><div class="movie-title">{{ media.title }}<br />({{ extractYear(media.release_date) || '未知年份' }})</div></div>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <n-tab-pane name="subscribed" :tab="`已订阅 (${subscribedMediaInModal.length})`">
            <n-empty v-if="subscribedMediaInModal.length === 0" :description="`你没有订阅此合集中的任何${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="16" :y-gap="16" responsive="screen">
              <n-gi v-for="media in subscribedMediaInModal" :key="media.tmdb_id">
                <n-card class="movie-card" content-style="padding: 0;">
                  <template #cover><img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" /></template>
                  <div class="movie-info"><div class="movie-title">{{ media.title }}<br />({{ extractYear(media.release_date) || '未知年份' }})</div></div>
                  <template #action>
                    <n-button @click="updateMediaStatus(media, 'missing')" type="warning" size="small" block ghost>
                      <template #icon><n-icon :component="CloseCircleIcon" /></template>
                      取消订阅
                    </n-button>
                  </template>
                </n-card>
              </n-gi>
            </n-grid>
          </n-tab-pane>
        </n-tabs>
      </div>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, h, computed, watch, nextTick } from 'vue';
import axios from 'axios';
import Sortable from 'sortablejs';
import { 
  NLayout, NPageHeader, NButton, NIcon, NText, NDataTable, NTag, NSpace,
  useMessage, NPopconfirm, NModal, NForm, NFormItem, NInput, NSelect,
  NAlert, NRadioGroup, NRadio, NTooltip, NSpin, NGrid, NGi, NCard, NEmpty, NTabs, NTabPane, NCheckboxGroup, NCheckbox, NInputNumber, NAutoComplete, NDynamicTags, NInputGroup
} from 'naive-ui';
import { 
  AddOutline as AddIcon, 
  CreateOutline as EditIcon, 
  TrashOutline as DeleteIcon,
  SyncOutline as SyncIcon,
  EyeOutline as EyeIcon,
  PlayOutline as GenerateIcon,
  CloudDownloadOutline as CloudDownloadIcon,
  CheckmarkCircleOutline as CheckmarkCircle,
  CloseCircleOutline as CloseCircleIcon,
  ReorderFourOutline as DragHandleIcon,
  HelpCircleOutline as HelpIcon
} from '@vicons/ionicons5';
import { format } from 'date-fns';

const message = useMessage();
const collections = ref([]);
const isLoading = ref(true);
const showModal = ref(false);
const isEditing = ref(false);
const isSaving = ref(false);
const formRef = ref(null);
const tableRef = ref(null);
const syncLoading = ref({});
const isSyncingMetadata = ref(false);
const countryOptions = ref([]);
const isSyncingAll = ref(false);
const genreOptions = ref([]);
const studioOptions = ref([]);
const isSearchingStudios = ref(false);
const tagOptions = ref([]);
const showDetailsModal = ref(false);
const isLoadingDetails = ref(false);
const selectedCollectionDetails = ref(null);
const subscribing = ref({});
const actorOptions = ref([]); 
const isSearchingActors = ref(false); 
const isSavingOrder = ref(false);
let sortableInstance = null;

const builtInLists = [
  { label: '自定义RSS源', value: 'custom' },
  { type: 'group', label: '猫眼电影榜单', key: 'maoyan-movie' },
  { label: '电影票房榜', value: 'maoyan://movie', contentType: ['Movie'] },
  { type: 'group', label: '猫眼全网热度榜', key: 'maoyan-all' },
  { label: '全网 - 电视剧', value: 'maoyan://web-heat', contentType: ['Series'] },
  { label: '全网 - 网剧', value: 'maoyan://web-tv', contentType: ['Series'] },
  { label: '全网 - 综艺', value: 'maoyan://zongyi', contentType: ['Series'] },
  { label: '全网 - 全类型', value: 'maoyan://web-heat,web-tv,zongyi', contentType: ['Series'] },
  { type: 'group', label: '猫眼腾讯视频热度榜', key: 'maoyan-tencent' },
  { label: '腾讯 - 电视剧', value: 'maoyan://web-heat-tencent', contentType: ['Series'] },
  { label: '腾讯 - 网剧', value: 'maoyan://web-tv-tencent', contentType: ['Series'] },
  { label: '腾讯 - 综艺', value: 'maoyan://zongyi-tencent', contentType: ['Series'] },
  { type: 'group', label: '猫眼爱奇艺热度榜', key: 'maoyan-iqiyi' },
  { label: '爱奇艺 - 电视剧', value: 'maoyan://web-heat-iqiyi', contentType: ['Series'] },
  { label: '爱奇艺 - 网剧', value: 'maoyan://web-tv-iqiyi', contentType: ['Series'] },
  { label: '爱奇艺 - 综艺', value: 'maoyan://zongyi-iqiyi', contentType: ['Series'] },
  { type: 'group', label: '猫眼优酷热度榜', key: 'maoyan-youku' },
  { label: '优酷 - 电视剧', value: 'maoyan://web-heat-youku', contentType: ['Series'] },
  { label: '优酷 - 网剧', value: 'maoyan://web-tv-youku', contentType: ['Series'] },
  { label: '优酷 - 综艺', value: 'maoyan://zongyi-youku', contentType: ['Series'] },
  { type: 'group', label: '猫眼芒果TV热度榜', key: 'maoyan-mango' },
  { label: '芒果TV - 电视剧', value: 'maoyan://web-heat-mango', contentType: ['Series'] },
  { label: '芒果TV - 网剧', value: 'maoyan://web-tv-mango', contentType: ['Series'] },
  { label: '芒果TV - 综艺', value: 'maoyan://zongyi-mango', contentType: ['Series'] },
];

const selectedBuiltInList = ref('custom');

const isContentTypeLocked = computed(() => {
  return selectedBuiltInList.value !== 'custom' && currentCollection.value.type === 'list';
});

const urlInputPlaceholder = computed(() => {
  return selectedBuiltInList.value === 'custom'
    ? '请输入RSS源的URL'
    : '已选择内置榜单，URL自动填充';
});

watch(selectedBuiltInList, (newValue) => {
  if (newValue && newValue !== 'custom') {
    currentCollection.value.definition.url = newValue;
  } else {
    if (!isEditing.value) {
      currentCollection.value.definition.url = '';
    }
  }

  const selectedOption = builtInLists.find(opt => opt.value === newValue);
  if (selectedOption && selectedOption.contentType) {
    currentCollection.value.definition.item_type = selectedOption.contentType;
  }
});

const sortFieldOptions = computed(() => {
  const options = [
    { label: '不设置 (使用Emby原生排序)', value: 'none' },
    { label: '名称', value: 'SortName' },
    { label: '添加日期', value: 'DateCreated' },
    { label: '上映日期', value: 'PremiereDate' },
    { label: '社区评分', value: 'CommunityRating' },
    { label: '制作年份', value: 'ProductionYear' },
  ];
  // 仅当合集类型为 'list' 时，才插入“榜单原始顺序”选项
  if (currentCollection.value.type === 'list') {
    options.splice(1, 0, { label: '榜单原始顺序', value: 'original' });
  }
  return options;
});

const sortOrderOptions = ref([
  { label: '升序', value: 'Ascending' },
  { label: '降序', value: 'Descending' },
]);

const getInitialFormModel = () => ({
  id: null,
  name: '',
  type: 'list',
  status: 'active',
  definition: {
    item_type: ['Movie'],
    url: '',
    limit: null,
    default_sort_by: 'original',
    default_sort_order: 'Ascending',
    // --- 新增字段的默认值 ---
    dynamic_filter_enabled: false, 
    dynamic_logic: 'AND', // 动态筛选目前只支持 AND，但保留字段以备将来扩展
    dynamic_rules: []      
  }
});
const currentCollection = ref(getInitialFormModel());

watch(() => currentCollection.value.type, (newType) => {
  if (isEditing.value) { return; }
  if (newType === 'filter') {
    currentCollection.value.definition = {
      item_type: ['Movie'],
      logic: 'AND',
      rules: [{ field: null, operator: null, value: '' }],
      // --- ▼▼▼ 核心修改 3/3：为筛选类型设置不同的默认排序 ▼▼▼ ---
      default_sort_by: 'PremiereDate', // 筛选类型默认按上映日期降序
      default_sort_order: 'Descending'
      // --- ▲▲▲ 修改结束 ▲▲▲ ---
    };
  } else if (newType === 'list') {
    currentCollection.value.definition = { 
      item_type: ['Movie'],
      url: '',
      limit: null,
      default_sort_by: 'original', // 榜单类型默认使用原始顺序
      default_sort_order: 'Ascending'
    };
    selectedBuiltInList.value = 'custom';
  }
});

const fetchCountryOptions = async () => {
  try {
    const response = await axios.get('/api/custom_collections/config/countries');
    const countryList = response.data; 
    countryOptions.value = countryList.map(name => ({
      label: name,
      value: name
    }));
  } catch (error) {
    message.error('获取国家/地区列表失败。');
  }
};

const fetchGenreOptions = async () => {
  try {
    const response = await axios.get('/api/config/genres');
    const genreList = response.data; 
    genreOptions.value = genreList.map(name => ({
      label: name,
      value: name
    }));
  } catch (error) {
    message.error('获取电影类型列表失败。');
  }
};

const fetchTagOptions = async () => {
  try {
    const response = await axios.get('/api/custom_collections/config/tags');
    tagOptions.value = response.data.map(name => ({
      label: name,
      value: name
    }));
  } catch (error) {
    message.error('获取标签列表失败。');
  }
};

let actorSearchTimeout = null;
const handleActorSearch = (query) => {
  if (!query) {
    actorOptions.value = [];
    return;
  }
  isSearchingActors.value = true;
  if (actorSearchTimeout) clearTimeout(actorSearchTimeout);
  actorSearchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/custom_collections/search_actors?q=${query}`);
      actorOptions.value = response.data.map(name => ({ label: name, value: name }));
    } catch (error) {
      console.error('搜索演员失败:', error);
      actorOptions.value = [];
    } finally {
      isSearchingActors.value = false;
    }
  }, 300);
};

let searchTimeout = null;
const handleStudioSearch = (query) => {
  if (!query) {
    studioOptions.value = [];
    return;
  }
  isSearchingStudios.value = true;
  if (searchTimeout) clearTimeout(searchTimeout);
  searchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/search_studios?q=${query}`);
      studioOptions.value = response.data.map(name => ({ label: name, value: name }));
    } catch (error) {
      console.error('搜索工作室失败:', error);
      studioOptions.value = [];
    } finally {
      isSearchingStudios.value = false;
    }
  }, 300);
};

watch(() => currentCollection.value.definition.rules, (newRules) => {
  if (Array.isArray(newRules)) {
    newRules.forEach(rule => {
      const validOperators = getOperatorOptionsForRow(rule).map(opt => opt.value);
      if (rule.operator && !validOperators.includes(rule.operator)) {
        rule.operator = null;
        rule.value = '';
      }
      if (rule.field && !rule.operator) {
        const options = getOperatorOptionsForRow(rule);
        if (options && options.length === 1) rule.operator = options[0].value;
      }
      const isMultiValueOp = ['is_one_of', 'is_none_of'].includes(rule.operator);
      if (isMultiValueOp && !Array.isArray(rule.value)) {
        rule.value = [];
      } else if (!isMultiValueOp && Array.isArray(rule.value)) {
        rule.value = '';
      }
    });
  }
}, { deep: true });

// 侦听动态规则的变化，以便在字段切换时重置值
watch(() => currentCollection.value.definition.dynamic_rules, (newRules) => {
  if (Array.isArray(newRules)) {
    newRules.forEach(rule => {
      // 当字段切换为“是否收藏”且当前值不是布尔值时，重置
      if (rule.field === 'is_favorite' && typeof rule.value !== 'boolean') {
        rule.value = true; // 默认设置为 true (已收藏)
      } 
      // 当字段切换为“播放状态”且当前值无效时，重置
      else if (rule.field === 'playback_status' && !['unplayed', 'in_progress', 'played'].includes(rule.value)) {
        rule.value = 'unplayed'; // 默认设置为 'unplayed' (未播放)
      }
    });
  }
}, { deep: true });

const ruleConfig = {
  // --- 静态规则 (无变化) ---
  title: { label: '标题', type: 'text', operators: ['contains', 'does_not_contain', 'starts_with', 'ends_with'] },
  actors: { label: '演员', type: 'text', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  directors: { label: '导演', type: 'text', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  release_year: { label: '年份', type: 'number', operators: ['gte', 'lte', 'eq'] },
  rating: { label: '评分', type: 'number', operators: ['gte', 'lte'] },
  genres: { label: '类型', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  countries: { label: '国家/地区', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] },
  studios: { label: '工作室', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] },
  tags: { label: '标签', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  unified_rating: { label: '家长分级', type: 'select', operators: ['is_one_of', 'is_none_of', 'eq'] },
  release_date: { label: '上映于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  date_added: { label: '入库于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  
  // --- ★★★ 动态规则的定义 ★★★ ---
  // 我们把它们和静态规则放在一起，用一个新的 'type' 属性来区分
  playback_status: { label: '播放状态', type: 'user_data', operators: ['is', 'is_not'] },
  is_favorite: { label: '是否收藏', type: 'user_data', operators: ['is', 'is_not'] },
};

const operatorLabels = {
  contains: '包含', does_not_contain: '不包含', starts_with: '开头是', ends_with: '结尾是',    
  gte: '大于等于', lte: '小于等于', eq: '等于',
  in_last_days: '最近N天内', not_in_last_days: 'N天以前',
  is_one_of: '是其中之一', is_none_of: '不是任何一个',
  // ★★★ 新增：动态规则操作符的标签 ★★★
  is: '是',
  is_not: '不是'
};

// 新的 ref 和获取分级选项的函数
const unifiedRatingOptions = ref([]);
const fetchUnifiedRatingOptions = async () => {
  try {
    // 调用我们新的API
    const response = await axios.get('/api/custom_collections/config/unified_ratings');
    unifiedRatingOptions.value = response.data.map(name => ({
      label: name,
      value: name
    }));
  } catch (error) {
    message.error('获取家长分级列表失败。');
  }
};

const staticFieldOptions = computed(() => 
  Object.keys(ruleConfig)
    .filter(key => ruleConfig[key].type !== 'user_data') // 只保留非用户数据的字段
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const dynamicFieldOptions = computed(() => 
  Object.keys(ruleConfig)
    .filter(key => ruleConfig[key].type === 'user_data') // 只保留用户数据的字段
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const getOperatorOptionsForRow = (rule) => {
  if (!rule.field) return [];
  // 直接返回通过 ruleConfig 生成的选项
  return (ruleConfig[rule.field]?.operators || []).map(op => ({ label: operatorLabels[op] || op, value: op }));
};

const addRule = () => {
  currentCollection.value.definition.rules?.push({ field: null, operator: null, value: '' });
};

const removeRule = (index) => {
  currentCollection.value.definition.rules?.splice(index, 1);
};

const typeOptions = [
  { label: '通过榜单导入 (RSS/内置)', value: 'list' },
  { label: '通过筛选规则生成', value: 'filter' }
];

const formRules = computed(() => {
  const baseRules = {
    name: { required: true, message: '请输入合集名称', trigger: 'blur' },
    type: { required: true, message: '请选择合集类型' },
    'definition.item_type': { type: 'array', required: true, message: '请至少选择一种合集内容类型' }
  };
  if (currentCollection.value.type === 'list') {
    baseRules['definition.url'] = { required: true, message: '请选择一个内置榜单或输入一个自定义URL', trigger: 'blur' };
  } else if (currentCollection.value.type === 'filter') {
    baseRules['definition.rules'] = {
      type: 'array', required: true,
      validator: (rule, value) => {
        if (!value || value.length === 0) return new Error('请至少添加一条筛选规则');
        if (value.some(r => !r.field || !r.operator || (Array.isArray(r.value) ? r.value.length === 0 : (r.value === null || r.value === '')))) {
          return new Error('请将所有规则填写完整');
        }
        return true;
      },
      trigger: 'change'
    };
  }
  return baseRules;
});

const authoritativeCollectionType = computed(() => {
    const collection = selectedCollectionDetails.value;
    if (!collection || !collection.item_type) return 'Movie';
    try {
        const parsedTypes = JSON.parse(collection.item_type);
        if (Array.isArray(parsedTypes) && parsedTypes.includes('Series')) return 'Series';
        return 'Movie';
    } catch (e) {
        if (collection.item_type === 'Series') return 'Series';
        return 'Movie';
    }
});

const detailsModalTitle = computed(() => {
  if (!selectedCollectionDetails.value) return '';
  const typeLabel = authoritativeCollectionType.value === 'Series' ? '电视剧合集' : '电影合集';
  return `${typeLabel}详情 - ${selectedCollectionDetails.value.name}`;
});

const mediaTypeName = computed(() => {
  if (!selectedCollectionDetails.value) return '媒体';
  return authoritativeCollectionType.value === 'Series' ? '剧集' : '影片';
});

const filterMediaByStatus = (status) => {
  if (!selectedCollectionDetails.value || !Array.isArray(selectedCollectionDetails.value.media_items)) return [];
  return selectedCollectionDetails.value.media_items.filter(media => media.status === status);
};

const missingMediaInModal = computed(() => filterMediaByStatus('missing'));
const inLibraryMediaInModal = computed(() => filterMediaByStatus('in_library'));
const unreleasedMediaInModal = computed(() => filterMediaByStatus('unreleased'));
const subscribedMediaInModal = computed(() => filterMediaByStatus('subscribed'));

const fetchCollections = async () => {
  isLoading.value = true;
  try {
    const response = await axios.get('/api/custom_collections');
    collections.value = response.data;
    nextTick(() => {
      initSortable();
    });
  } catch (error) {
    message.error('加载自定义合集列表失败。');
  } finally {
    isLoading.value = false;
  }
};

const initSortable = () => {
  if (sortableInstance) {
    sortableInstance.destroy();
  }
  const tbody = tableRef.value?.$el.querySelector('tbody');
  if (tbody) {
    sortableInstance = Sortable.create(tbody, {
      handle: '.drag-handle',
      animation: 150,
      onEnd: handleDragEnd,
    });
  }
};

const handleDragEnd = async (event) => {
  const { oldIndex, newIndex } = event;
  if (oldIndex === newIndex) return;

  const movedItem = collections.value.splice(oldIndex, 1)[0];
  collections.value.splice(newIndex, 0, movedItem);

  const orderedIds = collections.value.map(c => c.id);
  isSavingOrder.value = true;

  try {
    await axios.post('/api/custom_collections/update_order', { ids: orderedIds });
    message.success('合集顺序已保存。');
  } catch (error) {
    message.error(error.response?.data?.error || '保存顺序失败，请刷新页面重试。');
    fetchCollections();
  } finally {
    isSavingOrder.value = false;
  }
};


const openDetailsModal = async (collection) => {
  showDetailsModal.value = true;
  isLoadingDetails.value = true;
  selectedCollectionDetails.value = null;
  try {
    const response = await axios.get(`/api/custom_collections/${collection.id}/status`);
    selectedCollectionDetails.value = response.data;
  } catch (error) {
    message.error('获取合集详情失败。');
    showDetailsModal.value = false;
  } finally {
    isLoadingDetails.value = false;
  }
};

const subscribeMedia = async (media) => {
  if (!selectedCollectionDetails.value?.id) {
    message.error("无法确定当前操作的合集ID，请重试。");
    return;
  }
  subscribing.value[media.tmdb_id] = true;
  try {
    await axios.post('/api/custom_collections/subscribe', {
      collection_id: selectedCollectionDetails.value.id,
      tmdb_id: media.tmdb_id,
    });
    message.success(`《${media.title}》已成功提交订阅并更新状态！`);
    media.status = 'subscribed'; 
  } catch (err) {
    message.error(err.response?.data?.error || '订阅失败，请检查后端日志。');
  } finally {
    subscribing.value[media.tmdb_id] = false;
  }
};

const handleSync = async (row) => {
  syncLoading.value[row.id] = true;
  try {
    const payload = {
      task_name: 'process-single-custom-collection', 
      custom_collection_id: row.id 
    };
    const response = await axios.post('/api/tasks/run', payload);
    message.success(response.data.message || `已提交同步任务: ${row.name}`);
  } catch (error) {
    message.error(error.response?.data?.error || '提交同步任务失败。');
  } finally {
    syncLoading.value[row.id] = false;
  }
};

const handleSyncAll = async () => {
  isSyncingAll.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'process_all_custom_collections' });
    message.success(response.data.message || '已提交一键生成任务！');
  } catch (error) {
    message.error(error.response?.data?.error || '提交任务失败。');
  } finally {
    isSyncingAll.value = false;
  }
};

const updateMediaStatus = async (media, newStatus) => {
  try {
    await axios.post(`/api/custom_collections/${selectedCollectionDetails.value.id}/media_status`, {
      tmdb_id: media.tmdb_id,
      new_status: newStatus
    });
    media.status = newStatus;
    message.success(`状态已更新为: ${newStatus}`);
  } catch (err) {
    message.error(err.response?.data?.error || '更新状态失败');
  }
};

const triggerMetadataSync = async () => {
  isSyncingMetadata.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'populate-metadata' });
    message.success(response.data.message || '快速同步元数据任务已在后台启动！');
  } catch (error) {
    message.error(error.response?.data?.error || '启动任务失败。');
  } finally {
    isSyncingMetadata.value = false;
  }
};

const handleCreateClick = () => {
  isEditing.value = false;
  currentCollection.value = getInitialFormModel();
  selectedBuiltInList.value = 'custom';
  showModal.value = true;
};

const handleEditClick = (row) => {
  isEditing.value = true;
  // 深拷贝从表格行获取的数据
  const rowCopy = JSON.parse(JSON.stringify(row));

  // ★★★ 核心修复：不再需要手动解析 definition_json ★★★
  // 后端现在直接提供了名为 'definition' 的对象，我们只需确保它存在即可。
  if (!rowCopy.definition || typeof rowCopy.definition !== 'object') {
    // 如果 definition 意外丢失，提供一个安全的回退
    console.error("合集定义 'definition' 丢失或格式不正确:", row);
    rowCopy.definition = rowCopy.type === 'filter'
      ? { item_type: ['Movie'], logic: 'AND', rules: [] }
      : { item_type: ['Movie'], url: '' };
  }

  // 确保旧数据也兼容排序字段，避免出错
  if (!rowCopy.definition.default_sort_by) {
    rowCopy.definition.default_sort_by = 'none';
  }
  if (!rowCopy.definition.default_sort_order) {
    rowCopy.definition.default_sort_order = 'Ascending';
  }

  // 将处理好的数据赋值给表单模型
  currentCollection.value = rowCopy;

  // 如果是榜单类型，需要额外处理UI以下拉框正确显示
  if (rowCopy.type === 'list') {
    const url = rowCopy.definition.url || '';
    const isBuiltIn = builtInLists.some(item => item.value === url);
    if (isBuiltIn) {
      selectedBuiltInList.value = url;
    } else {
      selectedBuiltInList.value = 'custom';
    }
  }

  showModal.value = true;
};

const handleDelete = async (row) => {
  try {
    await axios.delete(`/api/custom_collections/${row.id}`);
    message.success(`合集 "${row.name}" 已删除。`);
    fetchCollections();
  } catch (error) {
    message.error('删除失败。');
  }
};

const handleSave = () => {
  formRef.value?.validate(async (errors) => {
    if (errors) return;
    isSaving.value = true;
    const dataToSend = JSON.parse(JSON.stringify(currentCollection.value));
    try {
      if (isEditing.value) {
        await axios.put(`/api/custom_collections/${dataToSend.id}`, dataToSend);
        message.success('合集更新成功！');
      } else {
        await axios.post('/api/custom_collections', dataToSend);
        message.success('合集创建成功！');
      }
      showModal.value = false;
      fetchCollections();
    } catch (error) {
      message.error(error.response?.data?.error || '保存失败。');
    } finally {
      isSaving.value = false;
    }
  });
};

const columns = [
  {
    key: 'drag',
    width: 50,
    render: () => h(NIcon, {
      component: DragHandleIcon,
      class: 'drag-handle',
      style: { cursor: 'grab' },
      size: 20
    })
  },
  { title: '名称', key: 'name', width: 250, ellipsis: { tooltip: true } },
  { 
    title: '类型', key: 'type', width: 180,
    render: (row) => {
      let label = '未知';
      let tagType = 'default';
      if (row.type === 'list') {
        let url = '';
        try {
          const def = JSON.parse(row.definition_json);
          url = def.url || '';
        } catch(e) {}
        
        const matchedOption = builtInLists.find(opt => opt.value === url);
        if (matchedOption) {
            label = matchedOption.label;
        } else if (url) {
            label = '榜单导入 (RSS)';
        } else {
            label = '榜单导入';
        }
        tagType = 'info';

      } else if (row.type === 'filter') {
        label = '筛选生成';
        tagType = 'default';
      }
      return h(NTag, { type: tagType, bordered: false }, { default: () => label });
    }
  },
  {
    title: '内容', key: 'item_type', width: 120,
    render: (row) => {
        let itemTypes = [];
        try {
            if (row.definition_json) {
                const definition = JSON.parse(row.definition_json);
                itemTypes = definition.item_type || ['Movie'];
            } else if (row.item_type) {
                itemTypes = JSON.parse(row.item_type);
            }
        } catch (e) {
            itemTypes = ['Movie'];
        }
        if (!Array.isArray(itemTypes)) itemTypes = [itemTypes];
        let label = '电影';
        const hasMovie = itemTypes.includes('Movie');
        const hasSeries = itemTypes.includes('Series');
        if (hasMovie && hasSeries) label = '电影、电视剧';
        else if (hasSeries) label = '电视剧';
        return h(NTag, { bordered: false }, { default: () => label });
    }
  },
  {
    title: '健康检查', key: 'health_check', width: 150,
    render(row) {
      if (row.type !== 'list') {
        return h(NText, { depth: 3 }, { default: () => 'N/A' });
      }
      const missingText = row.missing_count > 0 ? ` (${row.missing_count}缺失)` : '';
      const buttonType = row.missing_count > 0 ? 'warning' : 'default';
      return h(NButton, {
        size: 'small', type: buttonType, ghost: true,
        onClick: () => openDetailsModal(row)
      }, { default: () => `查看详情${missingText}`, icon: () => h(NIcon, { component: EyeIcon }) });
    }
  },
  { 
    title: '状态', key: 'status', width: 90,
    render: (row) => h(NTag, { type: row.status === 'active' ? 'success' : 'warning', bordered: false }, { default: () => row.status === 'active' ? '启用' : '暂停' })
  },
  { 
    title: '上次同步', key: 'last_synced_at', width: 180,
    render: (row) => row.last_synced_at ? format(new Date(row.last_synced_at), 'yyyy-MM-dd HH:mm') : '从未'
  },
  {
    title: '操作', key: 'actions', fixed: 'right', width: 220,
    render: (row) => h(NSpace, null, {
      default: () => [
        h(NButton, { size: 'small', type: 'primary', ghost: true, loading: syncLoading.value[row.id], onClick: () => handleSync(row) }, { icon: () => h(NIcon, { component: GenerateIcon }), default: () => '生成' }),
        h(NButton, { size: 'small', onClick: () => handleEditClick(row) }, { icon: () => h(NIcon, { component: EditIcon }), default: () => '编辑' }),
        h(NPopconfirm, { onPositiveClick: () => handleDelete(row) }, {
          trigger: () => h(NButton, { size: 'small', type: 'error', ghost: true }, { icon: () => h(NIcon, { component: DeleteIcon }), default: () => '删除' }),
          default: () => `确定删除合集 "${row.name}" 吗？`
        })
      ]
    })
  }
];

const getTmdbImageUrl = (posterPath) => posterPath ? `https://image.tmdb.org/t/p/w300${posterPath}` : '/img/poster-placeholder.png';
const extractYear = (dateStr) => dateStr ? dateStr.substring(0, 4) : null;

const addDynamicRule = () => {
  if (!currentCollection.value.definition.dynamic_rules) {
    currentCollection.value.definition.dynamic_rules = [];
  }
  // 为新规则设置一个合理的默认值，避免UI显示为空
  currentCollection.value.definition.dynamic_rules.push({ field: 'playback_status', operator: 'is', value: 'unplayed' });
};

const removeDynamicRule = (index) => {
  currentCollection.value.definition.dynamic_rules.splice(index, 1);
};

watch(() => currentCollection.value.definition.dynamic_rules, (newRules) => {
  if (Array.isArray(newRules)) {
    newRules.forEach(rule => {
      // 当字段切换为“是否收藏”且当前值不是布尔值时，重置
      if (rule.field === 'is_favorite' && typeof rule.value !== 'boolean') {
        rule.value = true; // 默认设置为 true (已收藏)
      } 
      // 当字段切换为“播放状态”且当前值无效时，重置
      else if (rule.field === 'playback_status' && !['unplayed', 'in_progress', 'played'].includes(rule.value)) {
        rule.value = 'unplayed'; // 默认设置为 'unplayed' (未播放)
      }
    });
  }
}, { deep: true });

onMounted(() => {
  fetchCollections();
  fetchCountryOptions();
  fetchGenreOptions();
  fetchTagOptions();
  fetchUnifiedRatingOptions();
});
</script>

<style scoped>
.custom-collections-manager {
  padding: 0 10px;
}
.movie-card {
  overflow: hidden;
  border-radius: 8px;
}
.movie-poster {
  width: 100%;
  height: auto;
  aspect-ratio: 2 / 3;
  object-fit: cover;
}
.movie-info {
  padding: 8px;
  text-align: center;
}
.movie-title {
  font-size: 13px;
  line-height: 1.3;
  height: 3.9em; /* 3 lines */
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  line-clamp: 3;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
}
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 200px;
}
.drag-handle:hover {
  color: #2080f0;
}
</style>