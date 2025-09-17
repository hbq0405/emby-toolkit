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
            <n-button type="default" @click="handleGenerateAllCovers" :loading="isGeneratingCovers">
              <template #icon><n-icon :component="CoverIcon" /></template>
              生成所有封面
            </n-button>
            <n-button type="primary" ghost @click="handleSyncAll" :loading="isSyncingAll">
              <template #icon><n-icon :component="GenerateIcon" /></template>
              生成所有合集
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
        
        <n-form-item label="可见用户" path="allowed_user_ids">
          <template #label>
            可见用户
            <n-tooltip trigger="hover">
              <template #trigger>
                <n-icon :component="HelpIcon" style="margin-left: 4px;" />
              </template>
              指定哪些Emby用户可以在首页看到这个虚拟库。如果留空，则默认对所有用户可见。
            </n-tooltip>
          </template>
          <n-select
            v-model:value="currentCollection.allowed_user_ids"
            multiple
            filterable
            clearable
            placeholder="留空则对所有用户可见"
            :options="embyUserOptions"
            :loading="isLoadingEmbyUsers"
          />
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
            <n-input-group>
              <n-input 
                v-model:value="currentCollection.definition.url" 
                placeholder="可手动输入URL，或通过右侧助手生成"
              />
              <n-button type="primary" ghost @click="openDiscoverHelper">
                TMDb 探索助手
              </n-button>
            </n-input-group>
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
          <n-form-item label="筛选范围" path="definition.library_ids">
            <template #label>
              筛选范围
              <n-tooltip trigger="hover">
                <template #trigger>
                  <n-icon :component="HelpIcon" style="margin-left: 4px;" />
                </template>
                指定此规则仅在选定的媒体库中生效。如果留空，则默认筛选所有媒体库。
              </n-tooltip>
            </template>
            <n-select
              v-model:value="currentCollection.definition.library_ids"
              multiple
              filterable
              clearable
              placeholder="留空则筛选所有媒体库"
              :options="embyLibraryOptions"
              :loading="isLoadingLibraries"
            />
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
                  
                  <!-- 修复：使用 dynamicFieldOptions -->
                  <n-select v-model:value="rule.field" :options="dynamicFieldOptions" placeholder="字段" style="width: 150px;" clearable />
                  
                  <n-select v-model:value="rule.operator" :options="getOperatorOptionsForRow(rule)" placeholder="操作" style="width: 120px;" :disabled="!rule.field" clearable />
                  
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
    <n-modal
      v-model:show="showDiscoverHelper"
      preset="card"
      style="width: 90%; max-width: 700px;"
      title="TMDb 探索助手 ✨"
      :bordered="false"
    >
      <n-space vertical :size="24">
        <n-form-item label="类型" label-placement="left">
          <n-radio-group v-model:value="discoverParams.type">
            <n-radio-button value="movie">电影</n-radio-button>
            <n-radio-button value="tv">电视剧</n-radio-button>
          </n-radio-group>
        </n-form-item>

        <n-form-item label="排序" label-placement="left">
          <n-select v-model:value="discoverParams.sort_by" :options="tmdbSortOptions" />
        </n-form-item>

        <n-form-item label="发行年份" label-placement="left">
          <n-input-group>
            <n-input-number v-model:value="discoverParams.release_year_gte" placeholder="从 (例如 1990)" :show-button="false" clearable style="width: 50%;" />
            <n-input-number v-model:value="discoverParams.release_year_lte" placeholder="到 (例如 1999)" :show-button="false" clearable style="width: 50%;" />
          </n-input-group>
        </n-form-item>

        <n-form-item label="类型 (可多选)" label-placement="left">
          <n-select
            v-model:value="discoverParams.with_genres"
            multiple filterable
            placeholder="选择或搜索类型"
            :options="tmdbGenreOptions"
            :loading="isLoadingTmdbGenres"
          />
        </n-form-item>

        <n-form-item label="排除类型 (可多选)" label-placement="left">
          <n-select
            v-model:value="discoverParams.without_genres"
            multiple
            filterable
            placeholder="排除不想要的类型，例如：纪录片, 综艺"
            :options="tmdbGenreOptions"
            :loading="isLoadingTmdbGenres"
          />
        </n-form-item>

        <n-form-item v-if="discoverParams.type === 'tv'" label="单集时长 (分钟)" label-placement="left">
          <n-input-group>
            <n-input-number v-model:value="discoverParams.with_runtime_gte" placeholder="最短" :min="0" :show-button="false" clearable style="width: 50%;" />
            <n-input-number v-model:value="discoverParams.with_runtime_lte" placeholder="最长" :min="0" :show-button="false" clearable style="width: 50%;" />
          </n-input-group>
        </n-form-item>

        <n-form-item label="国家/地区" label-placement="left">
          <n-select
            v-model:value="discoverParams.with_origin_country"
            filterable
            clearable
            placeholder="筛选特定的出品国家或地区"
            :options="tmdbCountryOptions"
            :loading="isLoadingTmdbCountries"
          />
        </n-form-item>

        <!-- 公司/网络 -->
        <n-form-item label="公司/网络" label-placement="left">
          <n-input
            v-model:value="companySearchText"
            placeholder="搜索电影公司或电视网络，例如：A24, HBO"
            @update:value="handleCompanySearch"
            clearable
          />
          <div v-if="isSearchingCompanies || companyOptions.length > 0" class="search-results-box">
            <n-spin v-if="isSearchingCompanies" size="small" />
            <div v-else v-for="option in companyOptions" :key="option.value" class="search-result-item" @click="handleCompanySelect(option)">
              {{ option.label }}
            </div>
          </div>
          <n-dynamic-tags v-model:value="selectedCompanies" style="margin-top: 8px;" />
        </n-form-item>

        <!-- 演员 -->
        <n-form-item label="演员" label-placement="left">
          <n-input
            v-model:value="actorSearchText"
            placeholder="搜索演员，例如：周星驰"
            @update:value="handleActorSearch" 
            clearable
          />
          <div v-if="isSearchingActors || actorOptions.length > 0" class="search-results-box person-results">
            <n-spin v-if="isSearchingActors" size="small" /> 
            <div v-else v-for="option in actorOptions" :key="option.id" class="search-result-item person-item" @click="handleActorSelect(option)"> 
              <n-avatar :size="40" :src="getTmdbImageUrl(option.profile_path, 'w92')" style="margin-right: 12px;" />
              <div class="person-info">
                <n-text>{{ option.name }}</n-text>
                <n-text :depth="3" class="known-for">代表作: {{ option.known_for || '暂无' }}</n-text>
              </div>
            </div>
          </div>
          <n-dynamic-tags v-model:value="selectedActors" style="margin-top: 8px;" />
        </n-form-item>

        <!-- 导演 -->
        <n-form-item label="导演" label-placement="left">
          <n-input
            v-model:value="directorSearchText"
            placeholder="搜索导演，例如：克里斯托弗·诺兰"
            @update:value="handleDirectorSearch" 
            clearable
          />
          <div v-if="isSearchingDirectors || directorOptions.length > 0" class="search-results-box person-results"> 
            <n-spin v-if="isSearchingDirectors" size="small" /> 
            <div v-else v-for="option in directorOptions" :key="option.id" class="search-result-item person-item" @click="handleDirectorSelect(option)"> 
              <n-avatar :size="40" :src="getTmdbImageUrl(option.profile_path, 'w92')" style="margin-right: 12px;" />
              <div class="person-info">
                <n-text>{{ option.name }}</n-text>
                <n-text :depth="3" class="known-for">领域: {{ option.department || '未知' }}</n-text>
              </div>
            </div>
          </div>
          <n-dynamic-tags v-model:value="selectedDirectors" style="margin-top: 8px;" />
        </n-form-item>

        <n-form-item label="语言" label-placement="left">
          <n-select v-model:value="discoverParams.with_original_language" :options="tmdbLanguageOptions" filterable clearable placeholder="不限" />
        </n-form-item>
        
        <n-form-item :label="`最低评分 (当前: ${discoverParams.vote_average_gte})`" label-placement="left">
           <n-slider v-model:value="discoverParams.vote_average_gte" :step="0.5" :min="0" :max="10" />
        </n-form-item>

        <n-form-item :label="`最低评分人数 (当前: ${discoverParams.vote_count_gte})`" label-placement="left">
           <n-slider v-model:value="discoverParams.vote_count_gte" :step="50" :min="0" :max="1000" />
        </n-form-item>

        <n-form-item label="生成的URL (实时预览)">
          <n-input :value="generatedDiscoverUrl" type="textarea" :autosize="{ minRows: 3 }" readonly />
        </n-form-item>
      </n-space>
      <template #footer>
        <n-space justify="end">
          <n-button @click="showDiscoverHelper = false">取消</n-button>
          <n-button type="primary" @click="confirmDiscoverUrl">使用这个URL</n-button>
        </n-space>
      </template>
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
  NAlert, NRadioGroup, NRadio, NTooltip, NSpin, NGrid, NGi, NCard, NEmpty, NTabs, NTabPane, NCheckboxGroup, NCheckbox, NInputNumber, NAutoComplete, NDynamicTags, NInputGroup, NRadioButton, NSlider, NAvatar
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
  HelpCircleOutline as HelpIcon,
  ImageOutline as CoverIcon,
} from '@vicons/ionicons5';
import { format } from 'date-fns';

// ===================================================================
// ▼▼▼ 确保所有 ref 变量都在这里定义好 ▼▼▼
// ===================================================================
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
const embyLibraryOptions = ref([]);
const isLoadingLibraries = ref(false);
const isGeneratingCovers = ref(false);
let sortableInstance = null;

// --- TMDb 探索助手相关状态 ---
const showDiscoverHelper = ref(false);
const isLoadingTmdbGenres = ref(false);
const tmdbMovieGenres = ref([]);
const tmdbTvGenres = ref([]);
const companySearchText = ref('');
const selectedCompanies = ref([]);
const actorSearchText = ref('');
const selectedActors = ref([]);
const directorSearchText = ref('');
const selectedDirectors = ref([]);
const isSearchingCompanies = ref(false);
const companyOptions = ref([]);
const isSearchingDirectors = ref(false);
const directorOptions = ref([]);
const tmdbCountryOptions = ref([]);
const isLoadingTmdbCountries = ref(false);

// ... (emby用户) ...
const embyUserOptions = ref([]);
const isLoadingEmbyUsers = ref(false);

const getInitialDiscoverParams = () => ({
  type: 'movie',
  sort_by: 'popularity.desc',
  release_year_gte: null,
  release_year_lte: null,
  with_genres: [],
  without_genres: [],
  with_runtime_gte: null, 
  with_runtime_lte: null,
  with_companies: [],
  with_cast: [],
  with_crew: [],
  with_origin_country: null,
  with_original_language: null,
  vote_average_gte: 0,
  vote_count_gte: 0,
});
const discoverParams = ref(getInitialDiscoverParams());

// ===================================================================
// ▼▼▼ 确保所有函数和计算属性都在这里 ▼▼▼
// ===================================================================

// ★★★ 获取 Emby 用户列表的函数 ★★★
const fetchEmbyUsers = async () => {
  isLoadingEmbyUsers.value = true;
  try {
    const response = await axios.get('/api/custom_collections/config/emby_users');
    embyUserOptions.value = response.data;
  } catch (error) {
    message.error('获取Emby用户列表失败。');
  } finally {
    isLoadingEmbyUsers.value = false;
  }
};

// ★★★ 封面生成处理函数 ★★★
const handleGenerateAllCovers = async () => {
  isGeneratingCovers.value = true;
  try {
    const response = await axios.post('/api/tasks/run', { task_name: 'generate-custom-collection-covers' });
    message.success(response.data.message || '已提交一键生成自建合集封面任务！');
  } catch (error) {
    message.error(error.response?.data?.error || '提交任务失败。');
  } finally {
    isGeneratingCovers.value = false;
  }
};

const builtInLists = [
  { label: '自定义RSS/URL源', value: 'custom' },
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
    ? '请输入RSS、TMDb片单或Discover链接'
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

  const itemTypes = currentCollection.value.definition?.item_type || [];
  if (Array.isArray(itemTypes) && itemTypes.includes('Series')) {
    // ▼▼▼ 核心修改：更新标签和值 ▼▼▼
    options.splice(4, 0, { label: '最后一集更新时间', value: 'last_synced_at' });
    // ▲▲▲ 修改结束 ▲▲▲
  }

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
  allowed_user_ids: [], 
  definition: {
    item_type: ['Movie'],
    url: '',
    limit: null,
    target_library_ids: [], 
    default_sort_by: 'original',
    default_sort_order: 'Ascending',
    dynamic_filter_enabled: false,
    dynamic_logic: 'AND',
    dynamic_rules: []
  }
});
const currentCollection = ref(getInitialFormModel());

watch(() => currentCollection.value.type, (newType) => {
  if (isEditing.value) { return; }
  const sharedProps = {
    item_type: ['Movie'],
    default_sort_by: 'none',
    default_sort_order: 'Ascending',
    dynamic_filter_enabled: false,
    dynamic_logic: 'AND',
    dynamic_rules: []
  };
  if (newType === 'filter') {
    currentCollection.value.definition = {
      ...sharedProps,
      logic: 'AND',
      rules: [{ field: null, operator: null, value: '' }],
      target_library_ids: [],
      default_sort_by: 'PremiereDate', 
    };
  } else if (newType === 'list') {
    currentCollection.value.definition = { 
      ...sharedProps,
      url: '',
      limit: null,
      default_sort_by: 'original', 
    };
    selectedBuiltInList.value = 'custom';
  }
});

const fetchEmbyLibraries = async () => {
  isLoadingLibraries.value = true;
  try {
    const response = await axios.get('/api/custom_collections/config/emby_libraries');
    embyLibraryOptions.value = response.data;
  } catch (error) {
    message.error('获取Emby媒体库列表失败。');
  } finally {
    isLoadingLibraries.value = false;
  }
};

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

watch(() => currentCollection.value.definition.dynamic_rules, (newRules) => {
  if (Array.isArray(newRules)) {
    newRules.forEach(rule => {
      if (rule.field === 'is_favorite' && typeof rule.value !== 'boolean') {
        rule.value = true;
      } 
      else if (rule.field === 'playback_status' && !['unplayed', 'in_progress', 'played'].includes(rule.value)) {
        rule.value = 'unplayed';
      }
    });
  }
}, { deep: true });

const ruleConfig = {
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
  // 动态筛选规则
  playback_status: { label: '播放状态', type: 'user_data_select', operators: ['is', 'is_not'] },
  is_favorite: { label: '是否收藏', type: 'user_data_bool', operators: ['is', 'is_not'] },
};

const operatorLabels = {
  contains: '包含', does_not_contain: '不包含', starts_with: '开头是', ends_with: '结尾是',    
  gte: '大于等于', lte: '小于等于', eq: '等于',
  in_last_days: '最近N天内', not_in_last_days: 'N天以前',
  is_one_of: '是其中之一', is_none_of: '不是任何一个',
  is: '是',
  is_not: '不是'
};

const createRuleWatcher = (rulesRef) => {
  watch(rulesRef, (newRules) => {
    if (!Array.isArray(newRules)) return;
    newRules.forEach(rule => {
      const config = ruleConfig[rule.field];
      if (!config) return;
      const validOperators = config.operators;
      if (rule.operator && !validOperators.includes(rule.operator)) {
        rule.operator = null;
        rule.value = null;
      }
      if (rule.field && !rule.operator && validOperators.length > 0) {
          rule.operator = validOperators[0];
      }
      if (rule.field === 'is_favorite' && typeof rule.value !== 'boolean') {
        rule.value = true;
      } else if (rule.field === 'playback_status' && !['unplayed', 'in_progress', 'played'].includes(rule.value)) {
        rule.value = 'unplayed';
      }
    });
  }, { deep: true });
};

const unifiedRatingOptions = ref([]);
const fetchUnifiedRatingOptions = async () => {
  try {
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
    .filter(key => !ruleConfig[key].type.startsWith('user_data')) 
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const dynamicFieldOptions = computed(() => 
  Object.keys(ruleConfig)
    .filter(key => ruleConfig[key].type.startsWith('user_data')) // <-- 使用更通用的 startsWith
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const getOperatorOptionsForRow = (rule) => {
  if (!rule.field) return [];
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
  const rowCopy = JSON.parse(JSON.stringify(row));

  if (!Array.isArray(rowCopy.allowed_user_ids)) {
    rowCopy.allowed_user_ids = [];
  }

  if (!rowCopy.definition || typeof rowCopy.definition !== 'object') {
    console.error("合集定义 'definition' 丢失或格式不正确:", row);
    rowCopy.definition = rowCopy.type === 'filter'
      ? { item_type: ['Movie'], logic: 'AND', rules: [] }
      : { item_type: ['Movie'], url: '' };
  }

  if (!rowCopy.definition.default_sort_by) {
    rowCopy.definition.default_sort_by = 'none';
  }
  if (!rowCopy.definition.default_sort_order) {
    rowCopy.definition.default_sort_order = 'Ascending';
  }

  currentCollection.value = rowCopy;

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
        let url = row.definition?.url || '';
        
        // ★★★ 核心修改：在这里增加判断逻辑 ★★★
        if (url.startsWith('maoyan://')) {
            label = '猫眼榜单';
            tagType = 'error';
        } else if (url.includes('douban.com/doulist')) {
            label = '豆瓣豆列';
            tagType = 'success'; // 用绿色区分
        } else if (url.includes('themoviedb.org/discover/')) {
            label = '探索助手';
            tagType = 'warning';
        } else {
            label = '榜单导入';
            tagType = 'info';
        }

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
        let itemTypes = row.definition?.item_type || ['Movie'];
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
    render: (row) => row.last_synced_at || '从未'
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

const getTmdbImageUrl = (posterPath, size = 'w300') => posterPath ? `https://image.tmdb.org/t/p/${size}${posterPath}` : '/img/poster-placeholder.png';
const extractYear = (dateStr) => dateStr ? dateStr.substring(0, 4) : null;

const addDynamicRule = () => {
  if (!currentCollection.value.definition.dynamic_rules) {
    currentCollection.value.definition.dynamic_rules = [];
  }
  currentCollection.value.definition.dynamic_rules.push({ field: 'is_favorite', operator: 'is', value: true });
};

const removeDynamicRule = (index) => {
  currentCollection.value.definition.dynamic_rules.splice(index, 1);
};

// --- TMDb 探索助手函数 ---
const tmdbSortOptions = computed(() => {
  if (discoverParams.value.type === 'movie') {
    // 如果是电影，使用电影的排序参数
    return [
      { label: '热度降序', value: 'popularity.desc' },
      { label: '热度升序', value: 'popularity.asc' },
      { label: '评分降序', value: 'vote_average.desc' },
      { label: '评分升序', value: 'vote_average.asc' },
      { label: '上映日期降序', value: 'primary_release_date.desc' },
      { label: '上映日期升序', value: 'primary_release_date.asc' },
    ];
  } else {
    // 如果是电视剧，使用电视剧的排序参数
    return [
      { label: '热度降序', value: 'popularity.desc' },
      { label: '热度升序', value: 'popularity.asc' },
      { label: '评分降序', value: 'vote_average.desc' },
      { label: '评分升序', value: 'vote_average.asc' },
      { label: '首播日期降序', value: 'first_air_date.desc' }, // ◀◀◀ 核心修正
      { label: '首播日期升序', value: 'first_air_date.asc' },  // ◀◀◀ 核心修正
    ];
  }
});

const tmdbLanguageOptions = [
    { label: '中文', value: 'zh' },
    { label: '英文', value: 'en' },
    { label: '日文', value: 'ja' },
    { label: '韩文', value: 'ko' },
    { label: '法语', value: 'fr' },
    { label: '德语', value: 'de' },
];

const tmdbGenreOptions = computed(() => {
  const source = discoverParams.value.type === 'movie' ? tmdbMovieGenres.value : tmdbTvGenres.value;
  return source.map(g => ({ label: g.name, value: g.id }));
});

// 计算属性：实时生成URL (精简可靠版)
const generatedDiscoverUrl = computed(() => {
  const params = discoverParams.value;
  const base = `https://www.themoviedb.org/discover/${params.type}`;
  const query = new URLSearchParams();
  
  query.append('sort_by', params.sort_by);

  // 年份
  if (params.type === 'movie') {
    if (params.release_year_gte) query.append('primary_release_date.gte', `${params.release_year_gte}-01-01`);
    if (params.release_year_lte) query.append('primary_release_date.lte', `${params.release_year_lte}-12-31`);
  } else {
    if (params.release_year_gte) query.append('first_air_date.gte', `${params.release_year_gte}-01-01`);
    if (params.release_year_lte) query.append('first_air_date.lte', `${params.release_year_lte}-12-31`);
  }

  // 各种ID类参数
  if (params.with_genres?.length) query.append('with_genres', params.with_genres.join(','));
  if (params.without_genres?.length) query.append('without_genres', params.without_genres.join(','));
  if (params.with_companies?.length) query.append('with_companies', params.with_companies.join(','));
  if (params.with_cast?.length) query.append('with_cast', params.with_cast.join(','));
  if (params.with_crew?.length) query.append('with_crew', params.with_crew.join(','));
  
  // 各种代码类参数
  if (params.with_origin_country) {
    query.append('with_origin_country', params.with_origin_country);
  }
  if (params.with_original_language) {
    query.append('with_original_language', params.with_original_language);
  }
  
  // 评分和评分人数
  if (params.vote_average_gte > 0) {
    query.append('vote_average.gte', params.vote_average_gte);
  }
  if (params.vote_count_gte > 0) {
    query.append('vote_count.gte', params.vote_count_gte);
  }
  
  // 时长
  if (params.type === 'tv') {
    if (params.with_runtime_gte) { // 只要有值就添加
      query.append('with_runtime.gte', params.with_runtime_gte);
    }
    if (params.with_runtime_lte) { // 只要有值就添加
      query.append('with_runtime.lte', params.with_runtime_lte);
    }
  }
  
  return `${base}?${query.toString()}`;
});

const fetchTmdbGenres = async () => {
  isLoadingTmdbGenres.value = true;
  try {
    const [movieRes, tvRes] = await Promise.all([
      axios.get('/api/custom_collections/config/tmdb_movie_genres'),
      axios.get('/api/custom_collections/config/tmdb_tv_genres')
    ]);
    tmdbMovieGenres.value = movieRes.data;
    tmdbTvGenres.value = tvRes.data;
  } catch (error) {
    console.error("获取TMDb类型列表失败，后端返回错误:", error.response?.data || error.message);
    message.error('获取TMDb类型列表失败，请检查后端日志。');
  } finally {
    isLoadingTmdbGenres.value = false;
  }
};

const fetchTmdbCountries = async () => {
  isLoadingTmdbCountries.value = true;
  try {
    const response = await axios.get('/api/custom_collections/config/tmdb_countries');
    tmdbCountryOptions.value = response.data;
  } catch (error) {
    message.error('获取国家/地区列表失败。');
  } finally {
    isLoadingTmdbCountries.value = false;
  }
};

const openDiscoverHelper = () => {
  discoverParams.value = getInitialDiscoverParams();
  // 清理工作区，确保每次打开都是全新的状态
  selectedCompanies.value = [];
  selectedActors.value = [];
  selectedDirectors.value = [];
  
  // 清理可能残留的搜索结果和文本
  companySearchText.value = '';
  companyOptions.value = [];
  actorSearchText.value = '';
  actorOptions.value = [];
  directorSearchText.value = '';
  directorOptions.value = [];

  showDiscoverHelper.value = true;
};

const confirmDiscoverUrl = () => {
  currentCollection.value.definition.url = generatedDiscoverUrl.value;
  const itemType = discoverParams.value.type === 'movie' ? 'Movie' : 'Series';
  if (!currentCollection.value.definition.item_type.includes(itemType)) {
      currentCollection.value.definition.item_type = [itemType];
  }
  showDiscoverHelper.value = false;
};

watch(() => discoverParams.value.type, () => {
    discoverParams.value.with_genres = [];
    discoverParams.value.with_runtime_gte = null;
    discoverParams.value.with_runtime_lte = null;
});

let companySearchTimeout = null;
const handleCompanySearch = (query) => {
  companySearchText.value = query;
  if (!query.length) {
    companyOptions.value = [];
    return;
  }
  isSearchingCompanies.value = true;
  if (companySearchTimeout) clearTimeout(companySearchTimeout);
  companySearchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/custom_collections/config/tmdb_search_companies?q=${query}`);
      companyOptions.value = response.data.map(c => ({ label: c.name, value: c.id }));
    } finally {
      isSearchingCompanies.value = false;
    }
  }, 300);
};
const handleCompanySelect = (option) => {
  if (!selectedCompanies.value.some(c => c.value === option.value)) {
    selectedCompanies.value.push(option);
  }
  companySearchText.value = '';
  companyOptions.value = [];
};

// --- 演员搜索 (独立版) ---
let actorSearchTimeout = null;
const handleActorSearch = (query) => {
  actorSearchText.value = query;
  if (!query.length) {
    actorOptions.value = [];
    return;
  }
  isSearchingActors.value = true;
  if (actorSearchTimeout) clearTimeout(actorSearchTimeout);
  actorSearchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      actorOptions.value = response.data;
    } finally {
      isSearchingActors.value = false;
    }
  }, 300);
};

// --- 导演搜索 (独立版) ---
let directorSearchTimeout = null;
const handleDirectorSearch = (query) => {
  directorSearchText.value = query;
  if (!query.length) {
    directorOptions.value = [];
    return;
  }
  isSearchingDirectors.value = true;
  if (directorSearchTimeout) clearTimeout(directorSearchTimeout);
  directorSearchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      directorOptions.value = response.data;
    } finally {
      isSearchingDirectors.value = false;
    }
  }, 300);
};

const handleActorSelect = (person) => {
  const selection = { label: person.name, value: person.id };
  if (!selectedActors.value.some(a => a.value === selection.value)) {
    selectedActors.value.push(selection);
  }
  actorSearchText.value = '';
  actorOptions.value = [];
};

const handleDirectorSelect = (person) => {
  const selection = { label: person.name, value: person.id };
  if (!selectedDirectors.value.some(d => d.value === selection.value)) {
    selectedDirectors.value.push(selection);
  }
  directorSearchText.value = '';
  directorOptions.value = [];
};

watch(selectedCompanies, (newValue) => {
  discoverParams.value.with_companies = newValue.map(c => c.value);
}, { deep: true });

watch(selectedActors, (newValue) => {
  discoverParams.value.with_cast = newValue.map(a => a.value);
}, { deep: true });

watch(selectedDirectors, (newValue) => {
  discoverParams.value.with_crew = newValue.map(d => d.value);
}, { deep: true });

createRuleWatcher(() => currentCollection.value.definition.rules);
createRuleWatcher(() => currentCollection.value.definition.dynamic_rules);

onMounted(() => {
  fetchCollections();
  fetchCountryOptions();
  fetchGenreOptions();
  fetchTagOptions();
  fetchUnifiedRatingOptions();
  fetchEmbyLibraries();
  fetchTmdbGenres();
  fetchTmdbCountries();
  fetchEmbyUsers();
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

.search-results-box {
  width: 100%;
  max-height: 220px;
  overflow-y: auto;
  border: 1px solid #48484e;
  border-radius: 3px;
  margin-top: 4px;
  background: #242428;
}
.search-result-item {
  padding: 8px 12px;
  cursor: pointer;
  transition: background-color 0.2s;
}
.search-result-item:hover {
  background-color: #3a3a40;
}
.person-item {
  display: flex;
  align-items: center;
}
.person-info {
  display: flex;
  flex-direction: column;
}
.known-for {
  font-size: 12px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 250px; /* 防止代表作过长撑开布局 */
}
</style>