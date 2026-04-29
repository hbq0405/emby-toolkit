<!-- src/components/MediaEditPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <div class="media-edit-page">
      <n-page-header @back="goBack">
        <template #title>
          手动编辑媒体信息
        </template>
      </n-page-header>

      <n-divider />

      <div v-if="isLoading" class="loading-container">
        <n-spin size="large" />
        <p style="text-align: center; margin-top: 10px;">正在加载媒体详情...</p>
      </div>

      <div v-else-if="itemDetails && itemDetails.item_name">
        <n-grid cols="1 l:4" :x-gap="24" responsive="screen">
          <!-- 左侧信息栏 (海报) -->
          <n-grid-item span="1 l:1">
            <n-space vertical :size="24">
              <n-card :title="itemDetails.item_name" :bordered="false">
                <template #cover>
                  <n-image
                    :src="posterUrl"
                    lazy
                    object-fit="cover"
                    class="media-poster"
                    :fallback-src="fallbackAvatar"
                  >
                    <template #placeholder>
                      <div class="poster-placeholder">
                        <n-icon :component="ImageIcon" size="48" :depth="3" />
                      </div>
                    </template>
                  </n-image>
                </template>
                <template #header-extra>
                  <n-tag :type="itemDetails.item_type === 'Movie' ? 'info' : 'success'" size="small" round>
                    {{ itemTypeInChinese }}
                  </n-tag>
                </template>
                <n-descriptions label-placement="left" bordered :column="1" size="small">
                  <n-descriptions-item label="Emby ItemID">
                    {{ itemDetails.item_id }}
                  </n-descriptions-item>
                  <n-descriptions-item label="原始记录评分" v-if="itemDetails.original_score !== null && itemDetails.original_score !== undefined">
                    <n-tag type="warning" size="small">{{ itemDetails.original_score }}</n-tag>
                  </n-descriptions-item>
                  <n-descriptions-item label="待复核原因" v-if="itemDetails.review_reason">
                    <n-text type="error">{{ itemDetails.review_reason }}</n-text>
                  </n-descriptions-item>
                </n-descriptions>
                
                <!-- ★★★ 编辑图像按钮 ★★★ -->
                <template #action>
                  <n-space vertical>
                    <n-button block type="primary" secondary @click="showImageEditor = true">
                      <template #icon>
                        <n-icon :component="ImagesIcon" />
                      </template>
                      编辑图像
                    </n-button>
                    <!-- ▼▼▼ 编辑媒体信息按钮 ▼▼▼ -->
                    <n-button 
                      block 
                      type="info" 
                      secondary 
                      @click="handleMediaInfoButtonClick"
                    >
                      <template #icon>
                        <n-icon :component="DocumentTextIcon" />
                      </template>
                      编辑媒体信息 (音轨/字幕)
                    </n-button>
                  </n-space>
                </template>
              </n-card>

              <n-card title="辅助工具" :bordered="false">
                <n-space vertical>
                  <n-form-item label="数据操作" label-placement="top">
                    <n-space>
                      <n-button-group>
                      <n-button 
                        tag="a" 
                        :href="searchLinks.baidu"
                        target="_blank" 
                        :disabled="!searchLinks.baidu"
                        :loading="isLoading"
                      >
                        百度搜索
                      </n-button>
                      <n-dropdown
                        trigger="click"
                        :options="searchDropdownOptions"
                        @select="handleSearchDropdownSelect"
                      >
                        <n-button :disabled="searchDropdownOptions.length === 0">
                          <template #icon>
                            <n-icon :component="ChevronDownIcon" />
                          </template>
                        </n-button>
                      </n-dropdown>
                    </n-button-group>
                      <n-button
                        type="info"
                        @click="translateAllFields" 
                        :loading="isTranslating" 
                        :disabled="isLoading"
                      >
                        一键翻译
                      </n-button>
                    </n-space>
                  </n-form-item>
                </n-space>
              </n-card>
            </n-space>
          </n-grid-item>

          <!-- 右侧演员列表 -->
          <n-grid-item span="1 l:3">
            <n-card :bordered="false" class="dashboard-card">
              <template #header>
                <span class="card-title">演员列表</span>
              </template>
              <n-form label-placement="left" label-width="auto">
                <draggable
                  v-model="editableCast"
                  tag="div"
                  item-key="_temp_id"
                  class="actor-grid-container"
                  handle=".drag-handle"
                  animation="300"
                >
                  <template #item="{ element: actor, index }">
                    <div class="actor-card-header">
                      <n-card size="small" class="dashboard-card actor-edit-card" content-style="padding: 16px;">
                        <template #header>
                          <div class="actor-card-header">
                            <n-avatar
                              round
                              size="small"
                              :style="{ backgroundColor: getAvatarColor(actor.name) }"
                            >
                              {{ index + 1 }}
                            </n-avatar>
                            <span class="actor-name-title" :title="actor.name">{{ actor.name || '新演员' }}</span>
                          </div>
                        </template>
                        <template #header-extra>
                          <n-space>
                            <n-button text class="drag-handle">
                              <n-icon :component="DragHandleIcon" />
                            </n-button>
                            <n-popconfirm @positive-click="removeActor(index)">
                              <template #trigger>
                                <n-button text type="error">
                                  <n-icon :component="TrashIcon" />
                                </n-button>
                              </template>
                              确定要删除演员 “{{ actor.name || '新演员' }}” 吗？
                            </n-popconfirm>
                          </n-space>
                        </template>
                        
                        <div class="actor-card-content">
                          <n-image
                            :src="getActorImageUrl(actor)"
                            lazy
                            object-fit="cover"
                            class="actor-avatar-image"
                          >
                            <template #placeholder>
                              <div class="avatar-placeholder">
                                <n-icon :component="PersonIcon" size="24" :depth="3" />
                              </div>
                            </template>
                          </n-image>
                          
                          <div class="actor-inputs">
                            <n-form-item label="演员" label-placement="left" label-width="40" class="compact-form-item">
                              <n-input v-model:value="actor.name" placeholder="演员名" size="small" style="width: 100%;" />
                            </n-form-item>
                            <n-form-item label="角色" label-placement="left" label-width="40" class="compact-form-item">
                              <n-input v-model:value="actor.role" placeholder="角色名" size="small" style="width: 100%;" />
                            </n-form-item>
                          </div>
                        </div>
                      </n-card>
                    </div>
                  </template>
                </draggable>
              </n-form>
              
              <div class="sticky-actions">
                <n-space>
                  <n-button @click="showAddActorModal = true" type="default" secondary>
                    <template #icon>
                      <n-icon :component="AddIcon" />
                    </template>
                    添加演员
                  </n-button>
                  <n-button @click="goBack">返回列表</n-button>
                  <n-button type="primary" @click="handleSaveChanges" :loading="isSaving">
                    保存修改
                  </n-button>
                </n-space>
              </div>
            </n-card>
          </n-grid-item>
        </n-grid>
      </div>

      <div v-else class="error-container">
        <n-alert title="错误" type="error">
          无法加载媒体详情，或指定的媒体项不存在。请检查后端日志或确认该媒体项有效。
          <n-button text @click="goBack" style="margin-left: 10px;">返回列表</n-button>
        </n-alert>
      </div>
    </div>

    <!-- ★★★ 剧集分集选择模态框 ★★★ -->
    <n-modal
      v-model:show="showEpisodeSelector"
      preset="card"
      style="width: 980px; max-width: 95vw;"
      title="选择要编辑的集"
      :bordered="false"
      size="huge"
    >
      <n-spin :show="isFetchingEpisodes">
        <div v-if="episodesList.length > 0" class="episode-selector-board">
          <div class="episode-selector-summary">
            共 {{ episodesList.length }} 集，按季列队。鼠标悬停看标题，点击集号直接开改。
          </div>

          <section
            v-for="season in episodeSeasonGroups"
            :key="season.season_number"
            class="episode-season-block"
          >
            <div class="episode-season-header">
              <span class="episode-season-title">{{ formatSeasonLabel(season.season_number) }}</span>
              <span class="episode-season-count">{{ season.episodes.length }} 集</span>
            </div>

            <div class="episode-parade-grid">
              <button
                v-for="ep in season.episodes"
                :key="ep.emby_id"
                type="button"
                class="episode-chip"
                :title="getEpisodeTooltip(ep)"
                @click="handleEpisodeChipClick(ep)"
              >
                {{ formatEpisodeLabel(ep) }}
              </button>
            </div>
          </section>
        </div>

        <n-empty v-else description="未找到在库的分集，请确认该剧集已入库" style="padding: 40px 0;" />
      </n-spin>
    </n-modal>

    <!-- 搜索演员模态框 -->
    <n-modal
      v-model:show="showAddActorModal"
      preset="card"
      style="width: 600px"
      title="搜索并添加演员"
      :bordered="false"
      size="huge"
      :segmented="{ content: 'soft', footer: 'soft' }"
    >
      <n-input
        v-model:value="actorSearchQuery"
        placeholder="输入演员名进行搜索..."
        clearable
        @update:value="debouncedSearchActors"
      />
      <n-spin :show="isSearchingActors" style="margin-top: 20px; min-height: 150px;">
        <n-list hoverable clickable>
          <n-list-item v-for="actor in actorSearchResults" :key="actor.id" @click="selectActor(actor)">
            <template #prefix>
              <n-avatar
                :src="getTMDbImageUrl(actor.profile_path, 'w92')"
                :fallback-src="fallbackAvatar"
                size="large"
                object-fit="cover"
              />
            </template>
            <n-thing :title="actor.name">
              <template #description>
                <n-text depth="3" v-if="actor.known_for">
                  代表作: {{ actor.known_for }}
                </n-text>
                <n-text depth="3" v-else>
                  {{ actor.department || '表演' }}
                </n-text>
              </template>
            </n-thing>
            <template #suffix>
              <n-button size="small" type="primary">选择</n-button>
            </template>
          </n-list-item>
        </n-list>
        <n-empty v-if="!isSearchingActors && actorSearchResults.length === 0 && actorSearchQuery" description="未找到相关人物" style="padding: 20px 0;" />
      </n-spin>
    </n-modal>

    <!-- ★★★ 图像编辑模态框 (Emby 排版风格 - 单行等大卡片) ★★★ -->
    <n-modal
      v-model:show="showImageEditor"
      preset="card"
      style="width: 1100px; max-width: 95vw;"
      title="编辑图像"
      :bordered="false"
      size="huge"
    >
      <div class="emby-image-grid">
        <div v-for="img in imageTypes" :key="img.type" class="emby-image-card">
          <!-- 图片展示区 (统一使用 16:9 比例，图片 contain 缩放) -->
          <div class="emby-card-image-container">
            <n-image
              :src="getDynamicImageUrl(img.embyType, img.type)"
              lazy
              object-fit="contain"
              class="full-image"
              :fallback-src="fallbackAvatar"
            >
              <template #placeholder>
                <div class="image-placeholder">
                  <n-icon :component="ImageIcon" size="40" :depth="3" />
                </div>
              </template>
            </n-image>
          </div>
          
          <!-- 信息与操作区 -->
          <div class="emby-card-footer">
            <div class="emby-card-title">{{ img.label }}</div>
            <div class="emby-card-actions">
              <n-tooltip trigger="hover" placement="bottom">
                <template #trigger>
                  <n-button text class="emby-action-btn" @click="openTmdbSelector(img.type, img.tmdbKey)">
                    <n-icon :component="SearchIcon" size="22" />
                  </n-button>
                </template>
                搜索新图像
              </n-tooltip>

              <n-tooltip trigger="hover" placement="bottom">
                <template #trigger>
                  <n-button text class="emby-action-btn" @click="triggerFileUpload(img.type)">
                    <n-icon :component="CloudUploadIcon" size="24" />
                  </n-button>
                </template>
                上传图像文件
              </n-tooltip>

              <n-tooltip trigger="hover" placement="bottom">
                <template #trigger>
                  <n-button text class="emby-action-btn" @click="openUrlPrompt(img.type)">
                    <n-icon :component="LinkIcon" size="22" />
                  </n-button>
                </template>
                设置网络图像
              </n-tooltip>
            </div>
          </div>
        </div>
      </div>

      <!-- 隐藏的文件上传组件 -->
      <input 
        type="file" 
        ref="fileInputRef" 
        style="display: none" 
        accept="image/jpeg, image/png, image/webp" 
        @change="handleFileUpload"
      >
    </n-modal>

    <!-- ★★★ TMDb 备选图模态框 (Emby 排版风格 - 完整显示不裁剪) ★★★ -->
    <n-modal
      v-model:show="showTmdbSelector"
      preset="card"
      style="width: 1200px; max-width: 95vw;"
      :title="`搜索图像 - ${currentTmdbImageLabel}`"
      :bordered="false"
      size="huge"
    >
      <div v-if="isFetchingTmdbImages" class="tmdb-loading-state">
        <n-spin size="large" />
        <div style="margin-top: 16px; color: var(--n-text-color-3);">正在从 TheMovieDb 拉取数据...</div>
      </div>
      
      <div v-else-if="currentTmdbImages.length === 0" class="tmdb-loading-state">
        <n-empty description="未找到相关图像" />
      </div>

      <div v-else class="emby-tmdb-grid">
        <div 
          v-for="(img, index) in currentTmdbImages" 
          :key="index" 
          class="emby-tmdb-card" 
          @click="selectTmdbImage(img.original)"
        >
          <!-- 动态比例容器，内部图片 contain -->
          <div class="tmdb-card-image-wrapper" :style="{ aspectRatio: currentTmdbImageAspect }">
            <n-image
              :src="img.preview"
              lazy
              object-fit="contain"
              preview-disabled
              class="full-image"
            />
          </div>
          <div class="tmdb-card-info">
            <div class="tmdb-meta" v-if="img.width && img.height">
              {{ img.width }} × {{ img.height }}
            </div>
          </div>
        </div>
      </div>
    </n-modal>

    <!-- 输入图片URL模态框 -->
    <n-modal v-model:show="showUrlPrompt" preset="dialog" title="设置网络图片">
      <n-input 
        v-model:value="imageUrlInput" 
        type="text" 
        placeholder="请粘贴以 http/https 开头的图片直链..." 
      />
      <template #action>
        <n-button @click="showUrlPrompt = false">取消</n-button>
        <n-button type="primary" @click="submitUrlImage" :loading="isUploadingImage">确定</n-button>
      </template>
    </n-modal>
    <!-- ★★★ 媒体信息编辑模态框 ★★★ -->
    <n-modal
      v-model:show="showMediaInfoEditor"
      preset="card"
      style="width: 800px; max-width: 95vw;"
      title="编辑媒体信息 (音轨/字幕)"
      :bordered="false"
      size="huge"
    >
      <n-alert type="info" style="margin-bottom: 16px;">
        修改语言标签后保存，系统将自动覆盖底层指纹文件并通知 Emby 重新加载。<br/>
        <b style="color: #d03050;">注意：</b>字幕的标题 (Title) 必须包含 <b>“简体”</b> 或 <b>“繁体”</b> 字样，Emby 才能正确识别并显示为 Chinese Simplified/Traditional。
      </n-alert>
      
      <!-- ★★★ 硬字幕标记按钮 ★★★ -->
      <n-card title="标记硬字幕 (写入视频流 Title)" size="small" style="margin-bottom: 16px;">
        <n-space>
          <n-button @click="setHardcodedSubtitle('简中硬字幕')" secondary>简中</n-button>
          <n-button @click="setHardcodedSubtitle('繁中硬字幕')" secondary>繁中</n-button>
          <n-button @click="setHardcodedSubtitle('简英硬字幕')" secondary>简英</n-button>
          <n-button @click="setHardcodedSubtitle('繁英硬字幕')" secondary>繁英</n-button>
          <n-button @click="clearHardcodedSubtitle()" secondary>清除标记</n-button>
        </n-space>
      </n-card>
      
      <n-table :bordered="true" :single-line="false" size="small">
        <thead>
          <tr>
            <th style="width: 80px;">类型</th>
            <th>当前标题 (Title)</th>
            <th style="width: 200px;">修正语言 (Language)</th>
            <th style="width: 80px; text-align: center;">默认</th>
          </tr>
        </thead>
        <tbody>
          <!-- ▼▼▼ 只显示音轨和字幕，隐藏视频流 ▼▼▼ -->
          <tr v-for="(stream, index) in displayStreams" :key="index">
            <td>
              <n-tag 
                :type="stream.Type === 'Audio' ? 'info' : 'success'" 
                size="small"
              >
                {{ stream.Type === 'Audio' ? '音轨' : '字幕' }}
              </n-tag>
            </td>
            <td>
              <n-input 
                v-model:value="stream.Title" 
                placeholder="无标题" 
                size="small" 
              />
            </td>
            <td>
              <n-select 
                v-model:value="stream.Language" 
                :options="stream.Type === 'Audio' ? audioLanguageOptions : subtitleLanguageOptions" 
                placeholder="选择语言"
                filterable
                clearable
                size="small"
                @update:value="(val, option) => handleLanguageChange(stream, val, option)"
              />
            </td>
            <td style="text-align: center;">
              <n-checkbox 
                v-model:checked="stream.IsDefault" 
                @update:checked="(val) => handleDefaultChange(stream, val)"
              />
            </td>
          </tr>
          <tr v-if="displayStreams.length === 0">
            <td colspan="4" style="text-align: center; padding: 20px;">
              <n-text depth="3">未解析到媒体流</n-text>
            </td>
          </tr>
        </tbody>
      </n-table>
      
      <template #action>
        <n-space justify="end">
          <n-button @click="showMediaInfoEditor = false">取消</n-button>
          <n-button type="primary" @click="saveMediaInfo" :loading="isSavingMediaInfo">保存并刷新</n-button>
        </n-space>
      </template>
    </n-modal>
  </n-layout>
</template>

<script setup>
import { ref, shallowRef, onMounted, watch, computed, nextTick } from 'vue';
import draggable from 'vuedraggable';
import { NIcon, NInput, NGrid, NGridItem, NFormItem, NTag, NAvatar, NPopconfirm, NImage, NModal, NList, NListItem, NThing, NEmpty, NButtonGroup, NDropdown, NTooltip } from 'naive-ui';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import { NPageHeader, NDivider, NSpin, NCard, NDescriptions, NDescriptionsItem, NButton, NSpace, NAlert, useMessage } from 'naive-ui';
import {
  MoveOutline as DragHandleIcon,
  TrashOutline as TrashIcon,
  ImageOutline as ImageIcon,
  ImagesOutline as ImagesIcon,
  PersonOutline as PersonIcon,
  AddOutline as AddIcon,
  ChevronDownOutline as ChevronDownIcon,
  CloudUploadOutline as CloudUploadIcon,
  LinkOutline as LinkIcon,
  SearchOutline as SearchIcon,
  DocumentTextOutline as DocumentTextIcon
} from '@vicons/ionicons5';
import { debounce } from 'lodash-es';

const route = useRoute();
const router = useRouter();
const message = useMessage();

const itemId = ref(null);
const isLoading = ref(true);
const itemDetails = ref(null);
const editableCast = ref([]);
const isSaving = ref(false);

const searchLinks = ref({});
const isTranslating = ref(false);

const showAddActorModal = ref(false);
const actorSearchQuery = ref('');
const actorSearchResults = ref([]);
const isSearchingActors = ref(false);
const fallbackAvatar = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

// ★★★ 图像编辑相关状态 ★★★
const showImageEditor = ref(false);
const showUrlPrompt = ref(false);
const imageUrlInput = ref('');
const currentEditImageType = ref('');
const fileInputRef = ref(null);
const isUploadingImage = ref(false);

// 用于强制刷新图片的随机时间戳
const imageRefreshTokens = ref({
  poster: Date.now(),
  clearlogo: Date.now(),
  fanart: Date.now(),
  landscape: Date.now()
});

const imageTypes = [
  { type: 'poster', label: '海报 (Poster)', embyType: 'Primary', aspect: '2/3', tmdbKey: 'posters' },
  { type: 'clearlogo', label: '标志 (Logo)', embyType: 'Logo', aspect: '16/9', tmdbKey: 'logos' },
  { type: 'fanart', label: '艺术图 (Fanart)', embyType: 'Backdrop', aspect: '16/9', tmdbKey: 'backdrops' },
  { type: 'landscape', label: '缩略图 (Landscape)', embyType: 'Thumb', aspect: '16/9', tmdbKey: 'backdrops' } // 缩略图通常用背景图代替
];

// 动态获取图片URL (带时间戳防缓存)
const getDynamicImageUrl = (embyType, typeKey) => {
  if (!itemDetails.value?.item_id) return '';
  // 如果是 Thumb，Emby 有时没有 Thumb 会用 Backdrop 代替，这里统一请求
  return `/image_proxy/Items/${itemDetails.value.item_id}/Images/${embyType}?quality=90&t=${imageRefreshTokens.value[typeKey]}`;
};

const posterUrl = computed(() => {
  return getDynamicImageUrl('Primary', 'poster');
});

// ★★★ 图像上传逻辑 ★★★
const triggerFileUpload = (type) => {
  currentEditImageType.value = type;
  if (fileInputRef.value) {
    fileInputRef.value.click();
  }
};

const handleFileUpload = async (event) => {
  const file = event.target.files[0];
  if (!file) return;

  const formData = new FormData();
  formData.append('image_type', currentEditImageType.value);
  formData.append('file', file);

  await uploadImagePayload(formData);
  // 清空 input 以便下次选择同名文件
  event.target.value = ''; 
};

const openUrlPrompt = (type) => {
  currentEditImageType.value = type;
  imageUrlInput.value = '';
  showUrlPrompt.value = true;
};

const submitUrlImage = async () => {
  if (!imageUrlInput.value.startsWith('http')) {
    message.error("请输入有效的 http/https 图片链接");
    return;
  }
  
  const payload = {
    image_type: currentEditImageType.value,
    image_url: imageUrlInput.value
  };
  
  await uploadImagePayload(payload);
  showUrlPrompt.value = false;
};

const uploadImagePayload = async (payload) => {
  isUploadingImage.value = true;
  const loadingMsg = message.loading("正在替换并通知 Emby 刷新...", { duration: 0 });
  
  try {
    const config = payload instanceof FormData 
      ? { headers: { 'Content-Type': 'multipart/form-data' } } 
      : {};
      
    const res = await axios.post(`/api/update_media_image/${itemId.value}`, payload, config);
    message.success(res.data.message || "图片替换成功！");
    
    // 刷新对应图片的时间戳，强制前端重新加载图片
    imageRefreshTokens.value[currentEditImageType.value] = Date.now();
    
  } catch (error) {
    console.error("图片替换失败:", error);
    message.error(error.response?.data?.error || "图片替换失败，请检查后端日志。");
  } finally {
    loadingMsg.destroy();
    isUploadingImage.value = false;
  }
};
// ★★★ 图像编辑逻辑结束 ★★★

// =========================================================
// ★★★ 媒体信息 (MediaInfo) 编辑相关状态与逻辑 ★★★
// =========================================================
const showMediaInfoEditor = ref(false);
const isSavingMediaInfo = ref(false);
const mediaInfoData = shallowRef(null); 
const mediaStreams = ref([]);    
const mediaInfoContext = ref({}); 
const currentEditMediaId = ref(null); // ★ 新增：记录当前正在编辑的具体媒体ID (电影ID或具体的集ID)

// ★ 新增：剧集分集选择相关状态
const showEpisodeSelector = ref(false);
const isFetchingEpisodes = ref(false);
const episodesList = ref([]); 

// ★★★ 剧集选择：按季分组，做成“阅兵式”方阵 ★★★
const episodeSeasonGroups = computed(() => {
  const groupMap = new Map();

  episodesList.value.forEach((ep) => {
    const seasonNo = Number(ep.season_number ?? 0);
    const safeSeasonNo = Number.isFinite(seasonNo) ? seasonNo : 0;

    if (!groupMap.has(safeSeasonNo)) {
      groupMap.set(safeSeasonNo, []);
    }
    groupMap.get(safeSeasonNo).push(ep);
  });

  return Array.from(groupMap.entries())
    .sort(([a], [b]) => a - b)
    .map(([season_number, episodes]) => ({
      season_number,
      episodes: episodes.slice().sort((a, b) => {
        const aNo = Number(a.episode_number ?? 0);
        const bNo = Number(b.episode_number ?? 0);
        return (Number.isFinite(aNo) ? aNo : 0) - (Number.isFinite(bNo) ? bNo : 0);
      })
    }));
});

const formatSeasonLabel = (seasonNumber) => {
  const n = Number(seasonNumber);
  if (n === 0) return '特别篇';
  return `第 ${Number.isFinite(n) ? n : seasonNumber} 季`;
};

const formatEpisodeLabel = (ep) => {
  const n = Number(ep?.episode_number);
  if (!Number.isFinite(n)) return ep?.episode_number || '?';
  return String(n).padStart(2, '0');
};

const getEpisodeTooltip = (ep) => {
  const episodeNo = ep?.episode_number ?? '?';
  const title = ep?.title || '未知标题';
  return `${formatSeasonLabel(ep?.season_number)} 第 ${episodeNo} 集：${title}`;
};

const handleEpisodeChipClick = (ep) => {
  showEpisodeSelector.value = false;
  openMediaInfoEditor(ep.emby_id);
};

// ★★★ 拆分音轨和字幕的下拉选项 ★★★
const audioLanguageOptions = ref([]);
const subtitleLanguageOptions = ref([]);


// ★ 新增：点击编辑按钮的分流逻辑
const handleMediaInfoButtonClick = async () => {
  if (itemDetails.value.item_type === 'Series') {
    // 如果是剧集，先拉取分集列表并打开选择框
    showEpisodeSelector.value = true;
    isFetchingEpisodes.value = true;
    try {
      const res = await axios.get(`/api/media_info/series/${itemId.value}/episodes`);
      episodesList.value = res.data;
    } catch (e) {
      message.error(e.response?.data?.error || "获取剧集分集列表失败");
      showEpisodeSelector.value = false;
    } finally {
      isFetchingEpisodes.value = false;
    }
  } else {
    // 如果是电影，直接打开编辑器
    openMediaInfoEditor(itemId.value);
  }
};
// 1. 获取语言映射表
const fetchLanguageMapping = async () => {
  try {
    const res = await axios.get('/api/custom_collections/config/language_mapping');
    const rawData = res.data;

    // 音轨选项：保持原样 (国语、粤语、英语等)
    audioLanguageOptions.value = rawData.map(item => {
      const value = (item.aliases && item.aliases.length > 0) ? item.aliases[0] : item.value;
      return { label: item.label, value };
    });

    // 字幕选项：将国语/粤语转换为简体/繁体
    subtitleLanguageOptions.value = rawData.map(item => {
      const label = item.label;
      const value = (item.aliases && item.aliases.length > 0) ? item.aliases[0] : item.value;
      
      if (label === '国语' || label === '普通话') {
        return { label: '简体', value };
      } else if (label === '粤语' || label === '广东话') {
        return { label: '繁体', value };
      }
      return { label, value };
    });
  } catch (e) {
    console.error("获取语言映射失败", e);
  }
};

// 2. 打开编辑器并获取数据 (修改为接收 targetId 参数)
const openMediaInfoEditor = async (targetId) => {
  currentEditMediaId.value = targetId; // 记录当前正在编辑的 ID
  
  // 检查其中一个数组是否为空即可
  if (audioLanguageOptions.value.length === 0) {
    await fetchLanguageMapping();
  }
  
  const loadingMsg = message.loading("正在读取底层媒体指纹...", { duration: 0 });
  try {
    const res = await axios.get(`/api/media_info/edit/${targetId}`);
    mediaInfoContext.value = {
      sha1: res.data.sha1,
      media_path: res.data.media_path,
      mediainfo_path: res.data.mediainfo_path
    };
    mediaInfoData.value = res.data.mediainfo;
    
    let streams = [];
    if (Array.isArray(mediaInfoData.value) && mediaInfoData.value.length > 0) {
      if (mediaInfoData.value[0].MediaSourceInfo) {
        streams = mediaInfoData.value[0].MediaSourceInfo.MediaStreams || [];
      } else {
        streams = mediaInfoData.value[0].MediaStreams || [];
      }
    } else if (mediaInfoData.value && mediaInfoData.value.MediaStreams) {
      streams = mediaInfoData.value.MediaStreams;
    }
    
    // 过滤出视频、音轨和字幕
    mediaStreams.value = streams.filter(s => s.Type === 'Video' || s.Type === 'Audio' || s.Type === 'Subtitle');
    
    showMediaInfoEditor.value = true;
  } catch (e) {
    message.error(e.response?.data?.error || "获取媒体信息失败");
  } finally {
    loadingMsg.destroy();
  }
};

// 3. 保存修改 (修改为使用 currentEditMediaId)
const saveMediaInfo = async () => {
  isSavingMediaInfo.value = true;
  const loadingMsg = message.loading("正在覆盖指纹并通知 Emby 重新加载...", { duration: 0 });
  
  try {
    const payload = {
      ...mediaInfoContext.value,
      mediainfo: mediaInfoData.value 
    };
    
    // 使用 currentEditMediaId 而不是全局的 itemId
    const res = await axios.post(`/api/media_info/edit/${currentEditMediaId.value}`, payload);
    message.success(res.data.message || "媒体信息已更新！");
    showMediaInfoEditor.value = false;
  } catch (e) {
    message.error(e.response?.data?.error || "保存失败，请检查后端日志");
  } finally {
    loadingMsg.destroy();
    isSavingMediaInfo.value = false;
  }
};

// 4. 处理语言选择变更，自动同步标题 (带智能转换)
const handleLanguageChange = (stream, val, option) => {
  if (option && option.label) {
    let newTitle = option.label;
    
    if (stream.Type === 'Subtitle') {
      // ★★★ 核心修复：字幕流强制使用 chi，靠 Title 区分简繁 ★★★
      if (['chi', 'yue', 'zh', 'zh-CN', 'zh-TW'].includes(val)) {
         // 所有中文字幕都使用 chi 作为底层语言
         stream.Language = 'chi';
      }

      const origTitle = (stream.Title || '').toLowerCase();
      
      // 智能保留双语特征
      if (origTitle.includes('chs&eng') || origTitle.includes('简英')) {
        newTitle = '简英双语 (简体)';
      } else if (origTitle.includes('cht&eng') || origTitle.includes('繁英')) {
        newTitle = '繁英双语 (繁体)';
      } 
      // 基础转换逻辑：将语言选项转化为简繁体
      else if (['国语', '普通话', '简中', '简体中文', 'zh-CN', 'chi'].includes(option.label) || val === 'zh-CN') {
        newTitle = '简体';
      } else if (['粤语', '广东话', '繁中', '繁体中文', 'zh-TW', 'yue'].includes(option.label) || val === 'zh-TW') {
        newTitle = '繁体';
      } else if (newTitle.endsWith('语')) {
        newTitle = newTitle.slice(0, -1) + '文';
      }

      // 兜底：如果选了中文，但标题里既没有简体也没有繁体，强制追加
      if (['chi', 'yue', 'zh', 'zh-CN', 'zh-TW'].includes(val) && !newTitle.includes('简体') && !newTitle.includes('繁体')) {
         newTitle += ' (简体)'; 
      }
    }
    
    stream.Title = newTitle;
  } else if (!val) {
    stream.Title = '';
  }
};

// 5. 处理默认勾选变更（同类型单选互斥）
const handleDefaultChange = (changedStream, isChecked) => {
  if (isChecked) {
    mediaStreams.value.forEach(s => {
      if (s !== changedStream && s.Type === changedStream.Type) {
        s.IsDefault = false;
      }
    });
  }
};

// 6. 计算属性：只显示音轨和字幕（过滤掉视频流）
const displayStreams = computed(() => {
  return mediaStreams.value.filter(s => s.Type === 'Audio' || s.Type === 'Subtitle');
});

// 7. 设置硬字幕标记（写入视频流 Title）
const setHardcodedSubtitle = (subtitleType) => {
  // 找到视频流
  const videoStream = mediaStreams.value.find(s => s.Type === 'Video');
  if (videoStream) {
    videoStream.Title = subtitleType;
    message.success(`已设置硬字幕标记：${subtitleType}`);
  } else {
    message.warning('未找到视频流');
  }
};

// 8. 清除硬字幕标记
const clearHardcodedSubtitle = () => {
  const videoStream = mediaStreams.value.find(s => s.Type === 'Video');
  if (videoStream) {
    videoStream.Title = '';
    message.success('已清除硬字幕标记');
  }
};

// ★★★ TMDb 选图相关状态 ★★★
const showTmdbSelector = ref(false);
const isFetchingTmdbImages = ref(false);
const tmdbImagesCache = ref(null); // 缓存拉取到的所有图片
const currentTmdbImages = ref([]); // 当前展示的图片列表
const currentTmdbImageLabel = ref('');
const currentTmdbImageAspect = ref('2/3');

// 打开 TMDb 选图框
const openTmdbSelector = async (type, tmdbKey) => {
  currentEditImageType.value = type;
  
  // 设置 UI 标题和比例
  const imgConfig = imageTypes.find(i => i.type === type);
  currentTmdbImageLabel.value = imgConfig.label;
  currentTmdbImageAspect.value = imgConfig.aspect;
  
  showTmdbSelector.value = true;

  // 如果还没拉取过，去后端拉取
  if (!tmdbImagesCache.value) {
    isFetchingTmdbImages.value = true;
    try {
      const res = await axios.get(`/api/tmdb_images/${itemId.value}`);
      tmdbImagesCache.value = res.data;
    } catch (error) {
      message.error("获取 TMDb 图片失败");
      showTmdbSelector.value = false;
      isFetchingTmdbImages.value = false;
      return;
    }
    isFetchingTmdbImages.value = false;
  }

  // 根据类型展示对应的图片列表
  currentTmdbImages.value = tmdbImagesCache.value[tmdbKey] || [];
};

// 选中 TMDb 图片并提交
const selectTmdbImage = async (originalUrl) => {
  showTmdbSelector.value = false; // 关闭选图框
  
  // 直接复用之前的上传逻辑，把 TMDb 原图直链发给后端
  const payload = {
    image_type: currentEditImageType.value,
    image_url: originalUrl
  };
  
  await uploadImagePayload(payload);
};

// 格式化语言显示
const formatLang = (langCode) => {
  if (!langCode || langCode === 'null') return '未分级/无文字';
  const langMap = {
    'zh': 'Chinese', 'zh-CN': 'Chinese (Simplified)', 'zh-TW': 'Chinese (Traditional)',
    'en': 'English', 'ja': 'Japanese', 'ko': 'Korean'
  };
  return langMap[langCode] || langCode.toUpperCase();
};

const searchDropdownOptions = computed(() => {
  const options = [];
  if (searchLinks.value.wikipedia) {
    options.push({
      label: 'Google (维基百科)',
      key: 'wikipedia'
    });
  }
  if (searchLinks.value.google) {
    options.push({
      label: 'Google 搜索',
      key: 'google'
    });
  }
  return options;
});

const handleSearchDropdownSelect = (key) => {
  const url = searchLinks.value[key];
  if (url) {
    window.open(url, '_blank');
  }
};

const getActorImageUrl = (actor) => {
  if (actor.imageUrl) {
    return `/api/image_proxy?url=${encodeURIComponent(actor.imageUrl)}`;
  }
  return ''; 
};

const itemTypeInChinese = computed(() => {
  if (!itemDetails.value || !itemDetails.value.item_type) {
    return '';
  }
  switch (itemDetails.value.item_type) {
    case 'Movie': return '电影';
    case 'Series': return '电视剧';
    default: return itemDetails.value.item_type;
  }
});

const getAvatarColor = (name) => {
  const colors = ['#f56a00', '#7265e6', '#ffbf00', '#00a2ae', '#4caf50', '#2196f3'];
  if (!name || name.length === 0) return colors[0];
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  const index = Math.abs(hash % colors.length);
  return colors[index];
};

const getTMDbImageUrl = (path, size = 'w185') => {
  if (!path) return '';
  return `https://image.tmdb.org/t/p/${size}${path}`;
};

watch(() => itemDetails.value, (newItemDetails) => {
  if (newItemDetails?.current_emby_cast) {
    editableCast.value = newItemDetails.current_emby_cast.map((actor, index) => ({
      ...actor,
      _temp_id: `actor-${Date.now()}-${index}`,
    }));
  } else {
    editableCast.value = [];
  }
}, { deep: true });

const removeActor = (index) => {
  editableCast.value.splice(index, 1);
  message.info("已从编辑列表移除一个演员（尚未保存）。");
};

const searchActors = async () => {
  if (actorSearchQuery.value.length < 1) {
    actorSearchResults.value = [];
    return;
  }
  isSearchingActors.value = true;
  try {
    const response = await axios.get('/api/custom_collections/config/tmdb_search_persons', {
      params: { q: actorSearchQuery.value }
    });
    actorSearchResults.value = response.data;
  } catch (error) {
    console.error("搜索演员失败:", error);
    message.error("搜索演员时出错，请检查后端日志。");
  } finally {
    isSearchingActors.value = false;
  }
};

const debouncedSearchActors = debounce(searchActors, 300);

const selectActor = (actor) => {
  if (editableCast.value.some(a => a.tmdbId === actor.id)) {
    message.warning(`演员 "${actor.name}" 已经在列表中了。`);
    return;
  }

  const newActor = {
    tmdbId: actor.id,
    name: actor.name,
    role: '',
    imageUrl: getTMDbImageUrl(actor.profile_path),
    emby_person_id: null,
    _temp_id: `new-actor-${Date.now()}`
  };

  editableCast.value.push(newActor);
  message.success(`已添加演员 "${actor.name}"，请为他/她填写角色名。`);

  showAddActorModal.value = false;
  actorSearchQuery.value = '';
  actorSearchResults.value = [];
};

const translateAllFields = async () => {
  try {
    const payload = { 
      cast: editableCast.value,
      title: itemDetails.value.item_name,
      year: itemDetails.value.production_year,
    };

    const response = await axios.post('/api/actions/translate_cast_sa', payload);
    const translatedList = response.data;

    editableCast.value = translatedList.map((actor, index) => ({
      ...actor,
      _temp_id: `translated-actor-${Date.now()}-${index}`
    }));
    
    message.success("智能翻译完成！");

  } catch (error) {
    console.error("一键翻译失败:", error);
    message.error(error.response?.data?.error || "翻译失败，请检查后端日志。");
  } finally {
    isTranslating.value = false;
  }
};

const fetchMediaDetails = async () => {
  isLoading.value = true;
  try {
    const response = await axios.get(`/api/media_for_editing/${itemId.value}`);
    itemDetails.value = response.data;

    if (response.data && response.data.search_links) {
      searchLinks.value = response.data.search_links;
    }

  } catch (error) {
    message.error(error.response?.data?.error || "获取媒体详情失败。");
    itemDetails.value = null;
  } finally {
    isLoading.value = false;
  }
};

onMounted(() => {
  itemId.value = route.params.itemId;
  
  if (itemId.value) {
    fetchMediaDetails();
  } else {
    message.error("未提供媒体项ID！");
    isLoading.value = false;
  }
});

const goBack = () => {
  router.push({ name: 'ReviewList' });
};

const handleSaveChanges = async () => {
  if (!itemDetails.value?.item_id) return;
  isSaving.value = true;
  try {
    await nextTick();

    const castPayload = editableCast.value.map(actor => {
      return {
        tmdbId: actor.tmdbId,
        name: actor.name,
        role: actor.role,
        emby_person_id: actor.emby_person_id
      };
    });

    const payload = {
      cast: castPayload,
      item_name: itemDetails.value.item_name,
    };
    
    await axios.post(`/api/update_media_cast_sa/${itemDetails.value.item_id}`, payload);
    
    message.success("修改已保存，Emby将自动刷新。");
    setTimeout(() => {
      goBack();
    }, 1500);

  } catch (error) {
    console.error("保存修改失败:", error);
    message.error(error.response?.data?.error || "保存修改失败，请检查后端日志。");
  } finally {
    isSaving.value = false;
  }
};
</script>

<style scoped>
.media-edit-page {
  padding: 0 24px 24px 24px;
  transition: all 0.3s;
}

.loading-container, .error-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 200px;
}

.media-poster {
  width: 100%;
  height: auto;
  background-color: var(--n-card-color);
  aspect-ratio: 2 / 3;
}

.poster-placeholder, .avatar-placeholder {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: var(--n-action-color);
}

.actor-grid-container {
  display: grid;
  grid-template-columns: repeat(1, 1fr);
  gap: 16px;
}
@media (min-width: 640px) { .actor-grid-container { grid-template-columns: repeat(2, 1fr); } }
@media (min-width: 768px) { .actor-grid-container { grid-template-columns: repeat(2, 1fr); } }
@media (min-width: 1024px) { .actor-grid-container { grid-template-columns: repeat(3, 1fr); } }
@media (min-width: 1280px) { .actor-grid-container { grid-template-columns: repeat(4, 1fr); } }

.actor-edit-card:hover {
  transform: translateY(-4px);
  box-shadow: var(--n-box-shadow-hover) !important;
}

.actor-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
}

.actor-name-title {
  font-weight: 600;
  flex-grow: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.actor-card-content {
  display: flex;
  align-items: center;
  gap: 12px;
}

.actor-avatar-image {
  width: 100px;
  height: 100px;
  border-radius: var(--n-border-radius);
  flex-shrink: 0;
}

.actor-inputs {
  display: flex;
  flex-direction: column;
  gap: 8px;
  flex-grow: 1;
  flex-basis: 0;
  min-width: 0;
}

.compact-form-item {
  margin-bottom: 0 !important;
}

.sticky-actions {
  position: sticky;
  bottom: -24px;
  left: 0;
  right: 0;
  padding: 16px 24px;
  background-color: var(--n-color);
  border-top: 1px solid var(--n-border-color);
  display: flex;
  justify-content: flex-end;
  z-index: 10;
  margin: 24px -24px 0;
}

.drag-handle {
  cursor: grab;
}
.drag-handle:active {
  cursor: grabbing;
}

.sortable-ghost {
  opacity: 0.4;
  background: var(--n-action-color);
  border: 1px dashed var(--n-border-color);
}
.sortable-drag {
  opacity: 1 !important;
  transform: rotate(2deg);
  box-shadow: 0 10px 20px rgba(0,0,0,0.2);
  z-index: 99;
}

/* =========================================================
   ★★★ 分集选择模态框：季集号阅兵式方阵 ★★★
   ========================================================= */
.episode-selector-board {
  max-height: 70vh;
  overflow-y: auto;
  padding-right: 8px;
}

.episode-selector-summary {
  margin-bottom: 12px;
  color: var(--n-text-color-3);
  font-size: 13px;
}

.episode-season-block {
  margin-bottom: 14px;
  padding: 12px;
  background-color: var(--n-color-embedded);
  border: 1px solid var(--n-border-color);
  border-radius: 8px;
}

.episode-season-block:last-child {
  margin-bottom: 0;
}

.episode-season-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
}

.episode-season-title {
  font-size: 14px;
  font-weight: 700;
  color: var(--n-text-color);
}

.episode-season-count {
  padding: 1px 7px;
  border-radius: 999px;
  font-size: 12px;
  color: var(--n-text-color-3);
  background-color: var(--n-action-color);
}

.episode-parade-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, 34px);
  gap: 6px;
  align-items: center;
}

.episode-chip {
  width: 34px;
  height: 28px;
  padding: 0;
  border: 1px solid var(--n-border-color);
  border-radius: 5px;
  background-color: var(--n-color);
  color: var(--n-text-color-2);
  font-size: 12px;
  font-weight: 700;
  line-height: 1;
  cursor: pointer;
  transition: border-color 0.15s, color 0.15s, background-color 0.15s, transform 0.1s;
}

.episode-chip:hover {
  border-color: var(--n-primary-color);
  background-color: var(--n-primary-color-hover);
  color: #fff;
  transform: translateY(-1px);
}

.episode-chip:active {
  transform: translateY(0);
}

.episode-selector-board::-webkit-scrollbar {
  width: 6px;
}
.episode-selector-board::-webkit-scrollbar-track {
  background: transparent;
}
.episode-selector-board::-webkit-scrollbar-thumb {
  background: var(--n-border-color);
  border-radius: 4px;
}
.episode-selector-board::-webkit-scrollbar-thumb:hover {
  background: var(--n-text-color-3);
}

@media (max-width: 640px) {
  .episode-season-block {
    padding: 10px;
  }

  .episode-parade-grid {
    grid-template-columns: repeat(auto-fill, 30px);
    gap: 5px;
  }

  .episode-chip {
    width: 30px;
    height: 26px;
    font-size: 11px;
  }
}

/* =========================================================
   ★★★ 图像编辑模态框 (Emby 排版风格) ★★★
   ========================================================= */
.emby-image-grid {
  display: grid;
  /* 强制 4 列，保证 4 张图永远在同一行 */
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 20px;
  padding: 10px 0;
}

.emby-image-card {
  background-color: var(--n-color-embedded);
  border: 1px solid var(--n-border-color);
  border-radius: 8px;
  padding: 16px;
  display: flex;
  flex-direction: column;
  transition: all 0.2s;
}

.emby-image-card:hover {
  box-shadow: var(--n-box-shadow-hover);
  border-color: var(--n-primary-color);
}

.emby-card-image-container {
  width: 100%;
  /* 强制所有卡片的图片区域为 16:9 横版比例 */
  aspect-ratio: 16 / 9;
  background-color: var(--n-action-color);
  border-radius: 4px;
  overflow: hidden;
  position: relative;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  display: flex;
  align-items: center;
  justify-content: center;
}

/* 确保 n-image 组件本身撑满容器 */
.full-image {
  width: 100%;
  height: 100%;
  display: block; /* ⚠️ 删掉原来的 display: flex */
}

/* ⚠️ 关键修复：穿透到内部真实的 img 标签，强制它 100% 宽高并 contain */
.full-image :deep(img) {
  width: 100%;
  height: 100%;
  object-fit: contain !important;
}

/* 修复占位符居中问题（因为去掉了 full-image 的 flex） */
.image-placeholder {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: var(--n-action-color);
  position: absolute;
  top: 0;
  left: 0;
}

.emby-card-footer {
  margin-top: 16px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
}

.emby-card-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--n-text-color);
}

.emby-card-actions {
  display: flex;
  gap: 16px;
  justify-content: center;
  width: 100%;
  margin-top: 4px;
}

.emby-action-btn {
  color: var(--n-text-color-3) !important;
  transition: color 0.2s, transform 0.1s;
}

.emby-action-btn:hover {
  color: var(--n-primary-color) !important;
  transform: scale(1.1);
}

/* =========================================================
   ★★★ TMDb 搜索模态框 (Emby 排版风格) ★★★
   ========================================================= */
.tmdb-loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 300px;
}

.emby-tmdb-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 24px 16px;
  padding: 10px 0;
  max-height: 65vh;
  overflow-y: auto;
  padding-right: 8px;
}

.emby-tmdb-grid::-webkit-scrollbar {
  width: 6px;
}
.emby-tmdb-grid::-webkit-scrollbar-track {
  background: transparent; 
}
.emby-tmdb-grid::-webkit-scrollbar-thumb {
  background: var(--n-border-color); 
  border-radius: 4px;
}
.emby-tmdb-grid::-webkit-scrollbar-thumb:hover {
  background: var(--n-text-color-3); 
}

.emby-tmdb-card {
  cursor: pointer;
  display: flex;
  flex-direction: column;
  transition: transform 0.2s;
}

.emby-tmdb-card:hover .tmdb-card-image-wrapper {
  transform: scale(1.03);
  box-shadow: 0 0 0 2px var(--n-primary-color);
}

.tmdb-card-image-wrapper {
  width: 100%;
  background-color: var(--n-action-color);
  border-radius: 6px;
  overflow: hidden;
  transition: all 0.2s;
  box-shadow: var(--n-box-shadow);
  display: flex;
  align-items: center;
  justify-content: center;
}

.tmdb-card-info {
  margin-top: 10px;
  text-align: center;
  line-height: 1.5;
}

.tmdb-provider {
  font-size: 13px;
  font-weight: 500;
  color: var(--n-text-color);
}

.tmdb-meta {
  font-size: 12px;
  color: var(--n-text-color-3);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.tmdb-score {
  font-size: 12px;
  color: var(--n-text-color-3);
}
</style>
