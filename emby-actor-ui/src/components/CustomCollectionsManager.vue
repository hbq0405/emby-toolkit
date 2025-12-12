<!-- src/components/CustomCollectionsManager.vue -->
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
      class="modal-card-lite"
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
              指定哪些Emby用户可以看到此虚拟库。选择“模板源”用户将自动包含其所有绑定用户。
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
            :render-label="renderSelectOptionWithTag"
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
          <n-form-item label="榜单来源">
            <n-select
              v-model:value="selectedBuiltInLists"
              multiple
              filterable
              clearable
              placeholder="可多选，例如同时选择猫眼电影和电视剧"
              :options="filteredBuiltInLists"
            />
          </n-form-item>
          
          <n-form-item label="自定义榜单URL">
            <div style="width: 100%;">
              <div v-for="(urlItem, index) in customUrlList" :key="index" style="margin-bottom: 10px;">
                <n-input-group>
                  <n-input 
                    v-model:value="urlItem.value" 
                    placeholder="请输入RSS、TMDb片单或Discover链接"
                  />
                  <!-- 只有第一个输入框显示 TMDb 助手，或者你可以做成每个都显示 -->
                  <n-button v-if="index === 0" type="primary" ghost @click="openDiscoverHelper">
                    TMDb 探索
                  </n-button>
                  <n-button type="error" ghost @click="removeCustomUrl(index)" :disabled="customUrlList.length === 1 && !urlItem.value">
                    <template #icon><n-icon :component="DeleteIcon" /></template>
                  </n-button>
                </n-input-group>
              </div>
              <n-button dashed block @click="addCustomUrl">
                <template #icon><n-icon :component="AddIcon" /></template>
                添加更多 URL
              </n-button>
            </div>
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
              仅导入榜单中的前 N 个项目。如果选择了多个榜单，将从每个榜单各取前 N 个。
            </template>
          </n-form-item>
          <n-divider title-placement="left" style="margin-top: 15px;">
            <n-space align="center">
              <n-icon :component="SparklesIcon" color="#f2c97d" /> <!-- 找个星星图标 -->
              <span>AI 智能审阅 (实验性)</span>
            </n-space>
          </n-divider>

          <n-form-item path="definition.ai_enabled">
            <template #label>
              <n-space align="center">
                <span>启用 AI 选片</span>
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon :component="HelpIcon" /></template>
                  开启后，系统会将榜单抓取到的前 50-100 部影片元数据发送给 AI，由 AI 根据你的指令进行二次筛选。
                </n-tooltip>
              </n-space>
            </template>
            <n-switch v-model:value="currentCollection.definition.ai_enabled" />
          </n-form-item>

          <n-collapse-transition :show="currentCollection.definition.ai_enabled">
            <n-form-item label="AI 选片指令 (Prompt)" path="definition.ai_prompt">
              <n-input
                v-model:value="currentCollection.definition.ai_prompt"
                type="textarea"
                placeholder="例如：只保留评分 7.0 以上的科幻片；不要恐怖片；如果是国产剧只保留古装类；优先保留近 3 年的作品。"
                :autosize="{ minRows: 3, maxRows: 6 }"
              />
            </n-form-item>
          </n-collapse-transition>
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
          <n-form-item label="筛选范围" path="definition.target_library_ids">
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
              v-model:value="currentCollection.definition.target_library_ids"
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
                <template v-else-if="rule.field === 'keywords'">
                  <n-select
                    v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                    v-model:value="rule.value"
                    multiple
                    filterable
                    placeholder="选择一个或多个关键词"
                    :options="keywordOptions"
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                  />
                  <n-select
                    v-else
                    v-model:value="rule.value"
                    filterable
                    placeholder="选择一个关键词"
                    :options="keywordOptions"
                    :disabled="!rule.operator"
                    clearable
                    style="flex-grow: 1;"
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
                <template v-else-if="rule.field === 'actors' || rule.field === 'directors'">
                  <n-select
                    :value="getPersonIdsFromRule(rule.value)"
                    @update:value="(ids, options) => updatePersonRuleValue(rule, options)"
                    multiple
                    filterable
                    remote
                    :placeholder="rule.field === 'actors' ? '输入以搜索并添加演员' : '输入以搜索并添加导演'"
                    :options="actorOptions"
                    :loading="isSearchingActors"
                    @search="(query) => handlePersonSearch(query, rule)"  
                    :disabled="!rule.operator"
                    style="flex-grow: 1; min-width: 220px;"
                    label-field="name"
                    value-field="id"
                    :render-option="renderPersonOption"
                    :render-tag="renderPersonTag"
                  />
                </template>
                <template v-else-if="ruleConfig[rule.field]?.type === 'single_select_boolean'">
                  <n-select
                    v-model:value="rule.value"
                    @update:value="rule.operator = 'is'"
                    placeholder="选择状态"
                    :options="[
                      { label: '连载中', value: true },
                      { label: '已完结', value: false }
                    ]"
                    :disabled="!rule.field"
                    style="flex-grow: 1; min-width: 180px;"
                  />
                  <div style="width: 120px;"></div>
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
                <n-input 
                    v-else-if="!['actors', 'directors'].includes(rule.field)" 
                    v-model:value="rule.value" 
                    placeholder="值" 
                    :disabled="!rule.operator" 
                />
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
        <!-- AI 推荐类型的表单 -->
        <div v-if="currentCollection.type === 'ai_recommendation'">
          <n-alert type="info" show-icon style="margin-bottom: 16px;">
            AI 将分析指定用户的观看历史（收藏或高分播放），自动生成推荐片单。
          </n-alert>

          <n-form-item label="目标用户" path="definition.target_user_id">
            <n-select
              v-model:value="currentCollection.definition.target_user_id"
              :options="embyUserOptions"
              placeholder="选择要分析口味的用户"
              filterable
              :render-label="renderSelectOptionWithTag"
            />
          </n-form-item>
          
          <n-form-item label="推荐倾向 (Prompt)" path="definition.ai_prompt">
            <n-input
              v-model:value="currentCollection.definition.ai_prompt"
              type="textarea"
              placeholder="留空则默认推荐。你可以微调，例如：'最近心情不好，多推点喜剧' 或 '只要电影，不要剧集'。"
              :autosize="{ minRows: 3, maxRows: 6 }"
            />
          </n-form-item>
          
          <n-form-item label="推荐数量" path="definition.limit">
            <n-input-number v-model:value="currentCollection.definition.limit" :default-value="20" :min="5" :max="50" />
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
            附加功能 (可选)
          </n-divider>
          <n-form-item>
            <template #label>
              <n-space align="center">
                <span>在首页显示最新内容</span>
                <n-tooltip trigger="hover">
                  <template #trigger>
                    <n-icon :component="HelpIcon" />
                  </template>
                  开启后，此合集的内容将出现在 Emby 首页的“最新媒体”栏目中。
                </n-tooltip>
              </n-space>
            </template>
            <n-switch v-model:value="currentCollection.definition.show_in_latest" />
          </n-form-item>
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
          <!-- 1. 未识别 -->
          <n-tab-pane name="unidentified" :tab="`未识别 (${unidentifiedMediaInModal.length})`">
            <n-empty v-if="unidentifiedMediaInModal.length === 0" description="完美！所有项目都已成功识别。" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in unidentifiedMediaInModal" :key="index">
                <div class="movie-card">
                  <!-- 角标 -->
                  <div class="status-badge unidentified">未识别</div>
                  
                  <!-- 占位图 -->
                  <div class="poster-placeholder">
                    <n-icon :component="HelpIcon" size="48" />
                  </div>

                  <!-- 底部文字遮罩 -->
                  <div class="movie-info-overlay">
                    <div class="movie-title">
                      {{ media.title }}
                      <span v-if="media.season"> 第 {{ media.season }} 季</span>
                    </div>
                    <div class="movie-year">匹配失败</div>
                  </div>

                  <!-- 悬停操作层 -->
                  <div class="movie-actions-overlay">
                    <n-button circle type="primary" @click="openTmdbSearch(media.title)">
                      <template #icon><n-icon :component="SearchIcon" /></template>
                    </n-button>
                    <n-button round type="warning" @click="handleFixMatchClick(media)">
                      修正匹配
                    </n-button>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 2. 缺失 (Missing) -->
          <n-tab-pane name="missing" :tab="`缺失${mediaTypeName} (${missingMediaInModal.length})`">
            <n-empty v-if="missingMediaInModal.length === 0" :description="`太棒了！没有已上映的缺失${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
            <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in missingMediaInModal" :key="index">
                <div class="movie-card">
                  <!-- 角标 -->
                  <div class="status-badge missing">缺失</div>

                  <!-- 海报 -->
                  <img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" loading="lazy" />

                  <!-- 底部文字遮罩 -->
                  <div class="movie-info-overlay">
                    <!-- 标题 + 季号 -->
                    <div class="movie-title" :title="media.title">
                      {{ media.title }}<span v-if="media.season"> 第 {{ media.season }} 季</span>
                    </div>
                    <!-- 年份 -->
                    <div class="movie-year">
                      {{ extractYear(media.release_date) || '未知年份' }}
                    </div>
                    <!-- 原始标题 (仅当不一致时显示) -->
                    <div v-if="media.original_title && media.original_title !== media.title" class="original-source-title">
                      {{ media.original_title }}
                    </div>
                  </div>

                  <!-- 悬停操作层 -->
                  <div class="movie-actions-overlay">
                    <n-space>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle color="#ffffff" text-color="#000000" @click="openTmdbSearch(media.title)">
                            <template #icon><n-icon :component="SearchIcon" /></template>
                          </n-button>
                        </template>
                        TMDb 搜索
                      </n-tooltip>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle type="primary" @click="handleFixMatchClick(media)">
                            <template #icon><n-icon :component="FixIcon" /></template>
                          </n-button>
                        </template>
                        修正匹配
                      </n-tooltip>
                    </n-space>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 3. 已入库 (In Library) -->
          <n-tab-pane name="in_library" :tab="`已入库 (${inLibraryMediaInModal.length})`">
             <n-empty v-if="inLibraryMediaInModal.length === 0" :description="`该合集在媒体库中没有任何${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
             <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in inLibraryMediaInModal" :key="index">
                <div class="movie-card">
                  <div class="status-badge in_library">已入库</div>
                  <img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" loading="lazy" />
                  
                  <div class="movie-info-overlay">
                    <div class="movie-title">
                      {{ media.title }}<span v-if="media.season"> 第 {{ media.season }} 季</span>
                    </div>
                    <div class="movie-year">{{ extractYear(media.release_date) || '未知年份' }}</div>
                    <div v-if="media.original_title && media.original_title !== media.title" class="original-source-title">
                      {{ media.original_title }}
                    </div>
                  </div>

                  <!-- ★★★ 悬停操作层：包含搜索和修正 ★★★ -->
                  <div class="movie-actions-overlay">
                    <n-space>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle color="#ffffff" text-color="#000000" @click="openTmdbSearch(media.title)">
                            <template #icon><n-icon :component="SearchIcon" /></template>
                          </n-button>
                        </template>
                        TMDb 搜索
                      </n-tooltip>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle type="primary" @click="handleFixMatchClick(media)">
                            <template #icon><n-icon :component="FixIcon" /></template>
                          </n-button>
                        </template>
                        修正匹配
                      </n-tooltip>
                    </n-space>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 4. 未上映 (Unreleased) -->
          <n-tab-pane name="unreleased" :tab="`未上映 (${unreleasedMediaInModal.length})`">
            <n-empty v-if="unreleasedMediaInModal.length === 0" :description="`该合集没有已知的未上映${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
             <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in unreleasedMediaInModal" :key="index">
                <div class="movie-card">
                  <div class="status-badge unreleased">未上映</div>
                  <img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" loading="lazy" />
                  
                  <div class="movie-info-overlay">
                    <div class="movie-title">
                      {{ media.title }}<span v-if="media.season"> 第 {{ media.season }} 季</span>
                    </div>
                    <div class="movie-year">{{ extractYear(media.release_date) || '未知年份' }}</div>
                    <div v-if="media.original_title && media.original_title !== media.title" class="original-source-title">
                      {{ media.original_title }}
                    </div>
                  </div>

                  <!-- ★★★ 悬停操作层：包含搜索和修正 ★★★ -->
                  <div class="movie-actions-overlay">
                    <n-space>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle color="#ffffff" text-color="#000000" @click="openTmdbSearch(media.title)">
                            <template #icon><n-icon :component="SearchIcon" /></template>
                          </n-button>
                        </template>
                        TMDb 搜索
                      </n-tooltip>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle type="primary" @click="handleFixMatchClick(media)">
                            <template #icon><n-icon :component="FixIcon" /></template>
                          </n-button>
                        </template>
                        修正匹配
                      </n-tooltip>
                    </n-space>
                  </div>
                </div>
              </n-gi>
            </n-grid>
          </n-tab-pane>

          <!-- 5. 已订阅 (Subscribed) -->
          <n-tab-pane name="subscribed" :tab="`已订阅 (${subscribedMediaInModal.length})`">
            <n-empty v-if="subscribedMediaInModal.length === 0" :description="`你没有订阅此合集中的任何${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
             <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in subscribedMediaInModal" :key="index">
                <div class="movie-card">
                  <div class="status-badge subscribed">已订阅</div>
                  <img :src="getTmdbImageUrl(media.poster_path)" class="movie-poster" loading="lazy" />
                  
                  <div class="movie-info-overlay">
                    <div class="movie-title">
                      {{ media.title }}<span v-if="media.season"> 第 {{ media.season }} 季</span>
                    </div>
                    <div class="movie-year">{{ extractYear(media.release_date) || '未知年份' }}</div>
                    <div v-if="media.original_title && media.original_title !== media.title" class="original-source-title">
                      {{ media.original_title }}
                    </div>
                  </div>

                  <!-- ★★★ 悬停操作层：包含搜索和修正 ★★★ -->
                  <div class="movie-actions-overlay">
                    <n-space>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle color="#ffffff" text-color="#000000" @click="openTmdbSearch(media.title)">
                            <template #icon><n-icon :component="SearchIcon" /></template>
                          </n-button>
                        </template>
                        TMDb 搜索
                      </n-tooltip>
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-button circle type="primary" @click="handleFixMatchClick(media)">
                            <template #icon><n-icon :component="FixIcon" /></template>
                          </n-button>
                        </template>
                        修正匹配
                      </n-tooltip>
                    </n-space>
                  </div>
                </div>
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
            @update:value="(query) => handlePersonSearch(query, null)" 
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
            @update:value="(query) => handlePersonSearch(query, null)" 
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
  NAlert, NRadioGroup, NRadio, NTooltip, NSpin, NGrid, NGi, NCard, NEmpty, useDialog, NTabs, NTabPane, NCheckboxGroup, NCheckbox, NInputNumber, NAutoComplete, NDynamicTags, NInputGroup, NRadioButton, NSlider, NAvatar
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
  BuildOutline as FixIcon,
  SearchOutline as SearchIcon,
} from '@vicons/ionicons5';

// ===================================================================
// ▼▼▼ 所有 ref 变量定义 ▼▼▼
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
const keywordOptions = ref([]);
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
const embyUserOptions = ref([]);
const isLoadingEmbyUsers = ref(false);
const dialog = useDialog();
const newTmdbId = ref('');
const newSeasonNumber = ref(null); // ★★★ 新增：为修正匹配弹窗的“季号”输入框创建 ref
let sortableInstance = null;

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
const unidentifiedMediaInModal = computed(() => filterMediaByStatus('unidentified'));

const openTmdbSearch = (mediaOrTitle) => {
  let query = '';
  
  if (typeof mediaOrTitle === 'string') {
    // 如果传的是字符串 (旧写法)，直接用
    query = mediaOrTitle;
  } else if (mediaOrTitle && typeof mediaOrTitle === 'object') {
    // 如果传的是对象 (新写法)，优先用 original_title
    query = mediaOrTitle.original_title || mediaOrTitle.title;
  }

  if (!query) {
    message.warning('没有可搜索的标题');
    return;
  }
  
  // 移除可能干扰搜索的季号信息 (例如 "怪奇物语 第五季" -> "怪奇物语")
  const cleanTitle = query.replace(/\s*(第\s*\d+\s*季|Season\s*\d+).*/i, '').trim();
  
  window.open(`https://www.themoviedb.org/search?query=${encodeURIComponent(cleanTitle)}`, '_blank');
};

const getInitialDiscoverParams = () => ({
  type: 'movie', sort_by: 'popularity.desc', release_year_gte: null, release_year_lte: null,
  with_genres: [], without_genres: [], with_runtime_gte: null, with_runtime_lte: null,
  with_companies: [], with_cast: [], with_crew: [], with_origin_country: null,
  with_original_language: null, vote_average_gte: 0, vote_count_gte: 0,
});
const discoverParams = ref(getInitialDiscoverParams());

// ===================================================================
// ▼▼▼ 所有函数和计算属性 ▼▼▼
// ===================================================================
const handleFixMatchClick = (media) => {
  // ★★★ 每次打开弹窗时，重置所有输入框的值
  newTmdbId.value = '';
  newSeasonNumber.value = null;

  const isSeries = authoritativeCollectionType.value === 'Series';

  dialog.create({
    title: `修正《${media.title}》的匹配`,
    // ★★★ 使用 NForm 和 NFormItem 优化弹窗布局
    content: () => h(NForm, { labelPlacement: 'left', labelWidth: 'auto' }, () => [
      h(NFormItem, { label: '当前错误ID' }, () => h(NText, { code: true }, () => media.tmdb_id)),
      h(NFormItem, { label: '正确TMDb ID', required: true }, () => 
        h(NInput, {
          placeholder: '请输入正确的 TMDb ID',
          value: newTmdbId.value,
          'onUpdate:value': (value) => { newTmdbId.value = value; },
          autofocus: true
        })
      ),
      // ★★★ 核心增强：如果是剧集，则显示季号输入框
      isSeries && h(NFormItem, { label: '季号 (可选)' }, () => 
        h(NInputNumber, {
          placeholder: '输入季号，如 2',
          value: newSeasonNumber.value,
          'onUpdate:value': (value) => { newSeasonNumber.value = value; },
          min: 0,
          clearable: true,
          style: { width: '100%' }
        })
      )
    ]),
    positiveText: '确认修正',
    negativeText: '取消',
    onPositiveClick: async () => {
      if (!newTmdbId.value || !/^\d+$/.test(newTmdbId.value)) {
        message.error('请输入一个有效的纯数字 TMDb ID。');
        return false;
      }
      
      // ★★★ 核心修改：构造 Payload ★★★
      const payload = {
        new_tmdb_id: newTmdbId.value,
      };

      // 检查当前的 tmdb_id 是否有效
      // 如果是 null, undefined, 空字符串, 或者字符串 "None"，则视为无效
      const currentId = media.tmdb_id;
      const isValidId = currentId && String(currentId).toLowerCase() !== 'none';

      if (isValidId) {
        // 情况 A: 修正已识别但错误的匹配 -> 传 old_tmdb_id
        payload.old_tmdb_id = currentId;
      } else {
        // 情况 B: 修正未识别的项目 -> 传 old_title
        // 优先使用 original_title (源标题)，如果没有则使用 title
        payload.old_title = media.original_title || media.title;
      }

      if (isSeries && newSeasonNumber.value !== null && newSeasonNumber.value !== '') {
        payload.season_number = newSeasonNumber.value;
      }

      await submitFixMatch(payload);
    }
  });
};

// ★★★ 函数现在接收一个完整的 payload 对象
const submitFixMatch = async (payload) => {
  if (!selectedCollectionDetails.value?.id) return;
  try {
    const response = await axios.post(`/api/custom_collections/${selectedCollectionDetails.value.id}/fix_match`, payload);
    message.success(response.data.message || '修正成功！正在刷新合集详情...');

    // ★★★ 核心修正：调用刷新逻辑，而不是手动 splice ★★★
    isLoadingDetails.value = true; // 显示加载动画
    try {
      const refreshResponse = await axios.get(`/api/custom_collections/${selectedCollectionDetails.value.id}/status`);
      selectedCollectionDetails.value = refreshResponse.data;
    } catch (refreshError) {
      message.error('刷新合集详情失败，请重新打开弹窗。');
      showDetailsModal.value = false; // 出错时直接关闭弹窗
    } finally {
      isLoadingDetails.value = false; // 隐藏加载动画
    }

  } catch (error) {
    message.error(error.response?.data?.error || '修正失败，请检查后端日志。');
  }
};

const ruleConfig = {
  title: { label: '标题', type: 'text', operators: ['contains', 'does_not_contain', 'starts_with', 'ends_with'] },
  actors: { label: '演员', type: 'text', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] }, 
  directors: { label: '导演', type: 'text', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] },
  runtime: { label: '时长(分钟)', type: 'number', operators: ['gte', 'lte'] }, 
  release_year: { label: '年份', type: 'number', operators: ['gte', 'lte', 'eq'] },
  rating: { label: '评分', type: 'number', operators: ['gte', 'lte'] },
  genres: { label: '类型', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] }, 
  countries: { label: '国家/地区', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] },
  studios: { label: '工作室', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] },
  keywords: { label: '关键词', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] },
  tags: { label: '标签', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  unified_rating: { label: '家长分级', type: 'select', operators: ['is_one_of', 'is_none_of', 'eq'] },
  release_date: { label: '上映于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  date_added: { label: '入库于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  is_in_progress: { label: '追剧状态', type: 'single_select_boolean', operators: ['is'] },
  playback_status: { label: '播放状态', type: 'user_data_select', operators: ['is', 'is_not'] },
  is_favorite: { label: '是否收藏', type: 'user_data_bool', operators: ['is', 'is_not'] },
};

const operatorLabels = {
  contains: '包含', does_not_contain: '不包含', starts_with: '开头是', ends_with: '结尾是',    
  gte: '大于等于', lte: '小于等于', eq: '等于',
  in_last_days: '最近N天内', not_in_last_days: 'N天以前',
  is_one_of: '是其中之一', is_none_of: '不是任何一个',
  is: '是', is_not: '不是',
  is_primary: '主要是' 
};

const fetchKeywordOptions = async () => {
  try {
    const response = await axios.get('/api/custom_collections/config/keywords');
    keywordOptions.value = response.data;
  } catch (error) {
    message.error('获取关键词列表失败。');
  }
};

const staticFieldOptions = computed(() => 
  Object.keys(ruleConfig)
    .filter(key => !ruleConfig[key].type.startsWith('user_data'))
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const dynamicFieldOptions = computed(() => 
  Object.keys(ruleConfig)
    .filter(key => ruleConfig[key].type.startsWith('user_data'))
    .map(key => ({ label: ruleConfig[key].label, value: key }))
);

const getOperatorOptionsForRow = (rule) => {
  if (!rule.field) return [];
  return (ruleConfig[rule.field]?.operators || []).map(op => ({ label: operatorLabels[op] || op, value: op }));
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

// 自定义渲染下拉选项的函数
const renderPersonOption = ({ node, option }) => {
  // 直接将一个 VNode 数组赋值给 node.children
  // 这样既能自定义内容，又能保留 node 自身的所有交互事件
  node.children = [
    h(NAvatar, {
      src: getTmdbImageUrl(option.profile_path, 'w92'),
      size: 'small',
      style: 'margin-right: 8px;',
      round: true,
    }),
    h('div', { style: 'display: flex; flex-direction: column;' }, [
      h(NText, null, { default: () => option.name }),
      h(NText, { depth: 3, style: 'font-size: 12px;' }, { default: () => `代表作: ${option.known_for || '暂无'}` })
    ])
  ];
  // 务必返回修改后的原始 node
  return node;
};

// 自定义渲染已选中标签的函数
const renderPersonTag = ({ option, handleClose }) => {
  return h(
    NTag,
    {
      type: 'info',
      closable: true,
      onClose: (e) => {
        e.stopPropagation();
        handleClose();
      },
      // 添加一点样式让头像和文字垂直居中
      style: {
        display: 'flex',
        alignItems: 'center',
        padding: '0 6px 0 2px', // 微调内边距
        height: '24px'
      },
      round: true // 让标签也变圆角，更美观
    },
    {
      // default 插槽返回一个数组，包含头像和名字
      default: () => [
        h(NAvatar, {
          src: getTmdbImageUrl(option.profile_path, 'w92'),
          size: 'small',
          style: 'margin-right: 5px;',
          round: true,
        }),
        option.name // 演员的名字
      ]
    }
  );
};

// ★★★ 自定义 Select 选项的渲染函数 ★★★
const renderSelectOptionWithTag = (option) => {
  // option 对象就是我们从后端接收到的 { label, value, is_template_source }
  if (option.is_template_source) {
    // 如果是模板源，我们返回一个包含标签的 VNode
    return h(
      'div', 
      { style: 'display: flex; justify-content: space-between; align-items: center; width: 100%;' },
      [
        h('span', null, option.label), // 用户名
        h(NTag, { type: 'success', size: 'small', bordered: false }, { default: () => '模板源' }) // 标签
      ]
    );
  }
  // 如果不是模板源，就只渲染用户名
  return option.label;
};

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
const filteredBuiltInLists = computed(() => {
  const result = [];
  let currentGroup = null;

  builtInLists.forEach(item => {
    // 1. 过滤掉 'custom' 选项
    if (item.value === 'custom') return;

    // 2. 如果是分组标题，创建一个新的分组对象
    if (item.type === 'group') {
      currentGroup = { 
        type: 'group', 
        label: item.label, 
        key: item.key, 
        children: [] 
      };
      result.push(currentGroup);
    } 
    // 3. 如果是普通选项
    else {
      if (currentGroup) {
        // 如果当前有分组，加入到分组的 children 中
        currentGroup.children.push(item);
      } else {
        // 如果没有分组（比如第一项），直接加入结果数组
        result.push(item);
      }
    }
  });

  return result;
});
const selectedBuiltInLists = ref([]);
const customUrlList = ref([{ value: '' }]);
// 计算属性：判断是否为多源模式
const isMultiSource = computed(() => {
  const builtInCount = selectedBuiltInLists.value.length;
  const customCount = customUrlList.value.filter(u => u.value.trim()).length;
  return (builtInCount + customCount) > 1;
});

const isContentTypeLocked = computed(() => {
  // 如果选择了任何内置榜单，且当前类型是 list，则锁定内容类型选择
  // (或者你可以直接返回 false，允许用户在多选模式下自由修改类型)
  return selectedBuiltInLists.value.length > 0 && currentCollection.value.type === 'list';
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
    options.splice(4, 0, { label: '最后一集更新时间', value: 'DateLastContentAdded' });
  }

  if (currentCollection.value.type === 'list') {
    // ★★★ 只有单源时才显示原始顺序 ★★★
    if (!isMultiSource.value) {
      options.splice(1, 0, { label: '榜单原始顺序', value: 'original' });
    }
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
    dynamic_rules: [],
    show_in_latest: true,
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
    dynamic_rules: [],
    show_in_latest: true,
  };

  if (newType === 'filter') {
    currentCollection.value.definition = {
      ...sharedProps,
      logic: 'AND',
      rules: [{ field: null, operator: null, value: '' }],
      target_library_ids: [],
      default_sort_by: 'PremiereDate', 
    };
  } else if (newType === 'ai_recommendation') {
    currentCollection.value.definition = {
        ...sharedProps,
        target_user_id: null,
        ai_prompt: '',
        limit: 20,
        item_type: ['Movie', 'Series'] // 推荐通常混合类型
    };
  } else if (newType === 'list') {
    currentCollection.value.definition = { 
      ...sharedProps,
      url: [], // ★★★ 必须是数组
      limit: null,
      default_sort_by: 'original', 
    };
    
    // ★★★ 修正点：这里必须用复数 selectedBuiltInLists，且设为空数组 ★★★
    selectedBuiltInLists.value = []; 
    customUrlList.value = [{ value: '' }];
  }
});

const addCustomUrl = () => {
  customUrlList.value.push({ value: '' });
};

const removeCustomUrl = (index) => {
  if (customUrlList.value.length > 1) {
    customUrlList.value.splice(index, 1);
  } else {
    customUrlList.value[0].value = ''; 
  }
};

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

let personSearchTimeout = null;

const handlePersonSearch = (query, rule) => {
  // 区分是来自筛选规则还是探索助手
  const isFilterRule = !!rule; 
  
  if (!query) {
    // 如果清空了搜索框，选项列表里应该只保留已选中的演员
    if (isFilterRule) {
      actorOptions.value = Array.isArray(rule.value) ? rule.value : [];
    } else {
      actorOptions.value = []; // 探索助手的逻辑
    }
    return;
  }

  isSearchingActors.value = true;
  if (personSearchTimeout) clearTimeout(personSearchTimeout);

  personSearchTimeout = setTimeout(async () => {
    try {
      const response = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      const searchResults = response.data || [];
      
      if (isFilterRule) {
        // ★★★ 筛选规则的逻辑：合并“已选项”和“新搜索结果” ★★★
        const selectedPersons = Array.isArray(rule.value) ? rule.value : [];
        const selectedIds = new Set(selectedPersons.map(p => p.id));
        const newResults = searchResults.filter(result => !selectedIds.has(result.id));
        actorOptions.value = [...selectedPersons, ...newResults];
      } else {
        // ★★★ 探索助手的逻辑：直接显示搜索结果 ★★★
        actorOptions.value = searchResults;
        // 同样，导演搜索也用这个结果
        directorOptions.value = searchResults;
      }

    } catch (error) {
      console.error('搜索人物失败:', error);
      if (isFilterRule) {
        actorOptions.value = Array.isArray(rule.value) ? rule.value : [];
      } else {
        actorOptions.value = [];
        directorOptions.value = [];
      }
    } finally {
      isSearchingActors.value = false;
      // 探索助手的导演搜索加载状态也一并处理
      isSearchingDirectors.value = false; 
    }
  }, 300);
};

// 函数1: 从我们的对象数组中，提取出纯 ID 数组，给 n-select 的 :value 使用
const getPersonIdsFromRule = (value) => {
  if (!Array.isArray(value)) return [];
  // 确保 value 里的每个元素都是对象，避免对数字调用 .id 出错
  return value.filter(p => typeof p === 'object' && p !== null).map(p => p.id);
};

// 函数2: 核心！当选项改变时，用 n-select 提供的【完整对象数组】来更新我们的数据
const updatePersonRuleValue = (rule, selectedOptions) => {
  // @update:value 传来的第二个参数 (options) 是完整的对象数组
  // 我们直接用它来覆盖 rule.value，这样就不会丢失任何信息
  rule.value = selectedOptions;
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

const addRule = () => {
  currentCollection.value.definition.rules?.push({ field: null, operator: null, value: '' });
};

const removeRule = (index) => {
  currentCollection.value.definition.rules?.splice(index, 1);
};

const typeOptions = [
  { label: '通过榜单导入 (RSS/内置)', value: 'list' },
  { label: '通过筛选规则生成', value: 'filter' },
  { label: 'AI 猜你喜欢 (指定用户)', value: 'ai_recommendation' }
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
  if (
    !selectedCollectionDetails.value ||
    !Array.isArray(selectedCollectionDetails.value.media_items)
  ) return [];

  if (Array.isArray(status)) {
    return selectedCollectionDetails.value.media_items.filter(media =>
      status.includes(media.status)
    );
  } else {
    return selectedCollectionDetails.value.media_items.filter(media => media.status === status);
  }
};

const missingMediaInModal = computed(() => filterMediaByStatus('missing'));
const inLibraryMediaInModal = computed(() => filterMediaByStatus('in_library'));
const unreleasedMediaInModal = computed(() => filterMediaByStatus('unreleased'));
const subscribedMediaInModal = computed(() => filterMediaByStatus(['subscribed', 'paused']));

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
  
  // 重置多选组件
  selectedBuiltInLists.value = [];
  customUrlList.value = [{ value: '' }];
  
  showModal.value = true;
};

const handleEditClick = (row) => {
  isEditing.value = true;
  const rowCopy = JSON.parse(JSON.stringify(row));

  if (Array.isArray(rowCopy.allowed_user_ids)) {
    const availableOptionsSet = new Set(embyUserOptions.value.map(opt => opt.value));
    
    rowCopy.allowed_user_ids = rowCopy.allowed_user_ids.filter(id => availableOptionsSet.has(id));
  } else {
    rowCopy.allowed_user_ids = [];
  }

  if (!rowCopy.definition || typeof rowCopy.definition !== 'object') {
    rowCopy.definition = rowCopy.type === 'filter'
      ? { item_type: ['Movie'], logic: 'AND', rules: [] }
      : { item_type: ['Movie'], url: '' };
  }

  if (typeof rowCopy.definition.show_in_latest === 'undefined') {
    rowCopy.definition.show_in_latest = true;
  }

  if (!rowCopy.definition.default_sort_by) {
    rowCopy.definition.default_sort_by = 'none';
  }
  if (!rowCopy.definition.default_sort_order) {
    rowCopy.definition.default_sort_order = 'Ascending';
  }

  currentCollection.value = rowCopy;

  // ★★★ 新增逻辑：为已存在的演员/导演规则，预加载选项数据 ★★★
  if (rowCopy.type === 'filter' && rowCopy.definition?.rules) {
    // 1. 从所有规则中提取出所有已选的演员/导演
    const initialPersons = rowCopy.definition.rules
      .filter(rule => (rule.field === 'actors' || rule.field === 'directors') && Array.isArray(rule.value))
      .flatMap(rule => rule.value);
    
    // 2. 去重，防止同一个演员在多个规则中出现导致重复
    const uniquePersons = Array.from(new Map(initialPersons.map(p => [p.id, p])).values());
    
    // 3. 将这些演员信息设置为 actorOptions 的初始值
    actorOptions.value = uniquePersons;
  } else {
    // 如果不是筛选类型或没有规则，清空选项
    actorOptions.value = [];
  }

  if (rowCopy.type === 'list') {
    let urls = rowCopy.definition.url;
    
    // 兼容旧数据：如果是字符串，转为数组
    if (typeof urls === 'string') {
      urls = urls ? [urls] : [];
    } else if (!Array.isArray(urls)) {
      urls = [];
    }

    // 分离“内置榜单”和“自定义URL”
    const builtInValues = new Set(builtInLists.map(i => i.value));
    
    const foundBuiltIns = [];
    const foundCustoms = [];

    urls.forEach(u => {
      if (builtInValues.has(u)) {
        foundBuiltIns.push(u);
      } else {
        foundCustoms.push({ value: u });
      }
    });

    // 赋值给 UI 变量
    selectedBuiltInLists.value = foundBuiltIns;
    
    // 自定义 URL 至少保留一个空框
    if (foundCustoms.length === 0) {
      customUrlList.value = [{ value: '' }];
    } else {
      customUrlList.value = foundCustoms;
    }
  } else {
    // 如果不是 list 类型，重置为空
    selectedBuiltInLists.value = [];
    customUrlList.value = [{ value: '' }];
  }

  currentCollection.value = rowCopy;
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
        let urls = row.definition?.url || [];

        // 兼容旧数据字符串，转数组
        if (typeof urls === 'string' && urls.trim() !== '') {
          urls = [urls.trim()];
        } else if (!Array.isArray(urls)) {
          urls = [];
        }

        if (urls.length > 1) {
          // 多个URL或榜单，视为混合榜单
          label = '混合榜单';
          tagType = 'warning'; // 你可以改成其它颜色
        } else if (urls.length === 1) {
          const url = urls[0];
          if (url.startsWith('maoyan://')) {
            label = '猫眼榜单';
            tagType = 'error';
          } else if (url.includes('douban.com/doulist')) {
            label = '豆瓣豆列';
            tagType = 'success';
          } else if (url.includes('themoviedb.org/discover/')) {
            label = '探索助手';
            tagType = 'warning';
          } else {
            label = '榜单导入';
            tagType = 'info';
          }
        } else {
          // 没有URL时，默认标记
          label = '无榜单URL';
          tagType = 'default';
        }
      } else if (row.type === 'filter') {
        label = '筛选生成';
        tagType = 'default';
      } else if (row.type === 'ai_recommendation') {
        label = '猜你喜欢';
        tagType = 'primary';
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
      if (row.type !== 'list' && row.type !== 'ai_recommendation') {
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

const tmdbSortOptions = computed(() => {
  if (discoverParams.value.type === 'movie') {
    return [
      { label: '热度降序', value: 'popularity.desc' }, { label: '热度升序', value: 'popularity.asc' },
      { label: '评分降序', value: 'vote_average.desc' }, { label: '评分升序', value: 'vote_average.asc' },
      { label: '上映日期降序', value: 'primary_release_date.desc' }, { label: '上映日期升序', value: 'primary_release_date.asc' },
    ];
  } else {
    return [
      { label: '热度降序', value: 'popularity.desc' }, { label: '热度升序', value: 'popularity.asc' },
      { label: '评分降序', value: 'vote_average.desc' }, { label: '评分升序', value: 'vote_average.asc' },
      { label: '首播日期降序', value: 'first_air_date.desc' }, { label: '首播日期升序', value: 'first_air_date.asc' },
    ];
  }
});

const tmdbLanguageOptions = [
    { label: '中文', value: 'zh' }, { label: '英文', value: 'en' }, { label: '日文', value: 'ja' },
    { label: '韩文', value: 'ko' }, { label: '法语', value: 'fr' }, { label: '德语', value: 'de' },
];

const tmdbGenreOptions = computed(() => {
  const source = discoverParams.value.type === 'movie' ? tmdbMovieGenres.value : tmdbTvGenres.value;
  return source.map(g => ({ label: g.name, value: g.id }));
});

const generatedDiscoverUrl = computed(() => {
  const params = discoverParams.value;
  const base = `https://www.themoviedb.org/discover/${params.type}`;
  const query = new URLSearchParams();
  query.append('sort_by', params.sort_by);
  if (params.type === 'movie') {
    if (params.release_year_gte) query.append('primary_release_date.gte', `${params.release_year_gte}-01-01`);
    if (params.release_year_lte) query.append('primary_release_date.lte', `${params.release_year_lte}-12-31`);
  } else {
    if (params.release_year_gte) query.append('first_air_date.gte', `${params.release_year_gte}-01-01`);
    if (params.release_year_lte) query.append('first_air_date.lte', `${params.release_year_lte}-12-31`);
  }
  if (params.with_genres?.length) query.append('with_genres', params.with_genres.join(','));
  if (params.without_genres?.length) query.append('without_genres', params.without_genres.join(','));
  if (params.with_companies?.length) query.append('with_companies', params.with_companies.join(','));
  if (params.with_cast?.length) query.append('with_cast', params.with_cast.join(','));
  if (params.with_crew?.length) query.append('with_crew', params.with_crew.join(','));
  if (params.with_origin_country) query.append('with_origin_country', params.with_origin_country);
  if (params.with_original_language) query.append('with_original_language', params.with_original_language);
  if (params.vote_average_gte > 0) query.append('vote_average.gte', params.vote_average_gte);
  if (params.vote_count_gte > 0) query.append('vote_count.gte', params.vote_count_gte);
  if (params.type === 'tv') {
    if (params.with_runtime_gte) query.append('with_runtime.gte', params.with_runtime_gte);
    if (params.with_runtime_lte) query.append('with_runtime.lte', params.with_runtime_lte);
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
  selectedCompanies.value = [];
  selectedActors.value = [];
  selectedDirectors.value = [];
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

watch(isMultiSource, (isMulti) => {
  if (isMulti) {
    // 如果切换到了多源模式，且当前排序是“原始顺序”，则强制重置为“不设置”
    if (currentCollection.value.definition.default_sort_by === 'original') {
      currentCollection.value.definition.default_sort_by = 'none';
      message.info('检测到多个榜单源，排序已自动重置为“不设置” (多榜单无法保持原始顺序)');
    }
  }
});

watch([selectedBuiltInLists, customUrlList], () => {
  const builtIns = selectedBuiltInLists.value;
  // 过滤掉空的自定义 URL
  const customs = customUrlList.value.map(i => i.value.trim()).filter(v => v);
  
  // 合并结果
  const combinedUrls = [...builtIns, ...customs];

  // 存入 definition.url
  // 注意：为了兼容性，如果只有一个且是字符串，也可以存字符串，但为了多源聚合，建议统一存数组
  currentCollection.value.definition.url = combinedUrls;
  
  // 自动设置 item_type (如果选择了内置榜单，尝试自动推断类型)
  // 简单的逻辑：只要有一个是 Series，就加上 Series
  const newItemTypes = new Set(currentCollection.value.definition.item_type || ['Movie']);
  builtIns.forEach(url => {
    const option = builtInLists.find(opt => opt.value === url);
    if (option && option.contentType) {
      option.contentType.forEach(t => newItemTypes.add(t));
    }
  });
  currentCollection.value.definition.item_type = Array.from(newItemTypes);

}, { deep: true });

onMounted(() => {
  fetchCollections();
  fetchCountryOptions();
  fetchGenreOptions();
  fetchTagOptions();
  fetchKeywordOptions();
  fetchUnifiedRatingOptions();
  fetchEmbyLibraries();
  fetchTmdbGenres();
  fetchTmdbCountries();
  fetchEmbyUsers();
});

createRuleWatcher(() => currentCollection.value.definition.rules);
createRuleWatcher(() => currentCollection.value.definition.dynamic_rules);

</script>

<style scoped>
.custom-collections-manager {
  padding: 0 10px;
}

/* ★★★ 卡片容器：强制 2:3 比例，去除内边距 ★★★ */
.movie-card {
  border-radius: 8px;
  overflow: hidden;
  position: relative;
  aspect-ratio: 2 / 3; /* 强制海报比例 */
  background-color: #202023;
  transition: transform 0.2s, box-shadow 0.2s;
  cursor: default;
}

.movie-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5);
  z-index: 2;
}

/* ★★★ 海报图片：铺满容器 ★★★ */
.movie-poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  transition: transform 0.3s;
}

/* 悬停时海报微放大，增加呼吸感 */
.movie-card:hover .movie-poster {
  transform: scale(1.05);
}

/* ★★★ 底部渐变遮罩 (核心) ★★★ */
.movie-info-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 60px 10px 10px 10px; /* 上方留出空间给渐变 */
  /* 黑色渐变：从透明到黑色，保证文字清晰 */
  background: linear-gradient(to top, rgba(0, 0, 0, 0.95) 0%, rgba(0, 0, 0, 0.7) 60%, transparent 100%);
  color: #fff;
  pointer-events: none; /* 让鼠标事件穿透到下层，防止遮挡点击 */
  z-index: 10;
}

/* ★★★ 标题样式 ★★★ */
.movie-title {
  font-size: 14px;
  font-weight: bold;
  line-height: 1.3;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  
  /* 限制最多显示 2 行 */
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
}

/* ★★★ 年份样式 ★★★ */
.movie-year {
  font-size: 12px;
  color: #ddd; /* 稍微灰一点 */
  margin-top: 2px;
  font-weight: 500;
}

/* ★★★ 原始标题样式 (优雅的第二行) ★★★ */
.original-source-title {
  font-size: 11px;
  color: #aaa;
  margin-top: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  opacity: 0.8;
}

/* ★★★ 悬停操作层 (默认隐藏) ★★★ */
.movie-actions-overlay {
  position: absolute;
  inset: 0; /* 铺满整个卡片 */
  background: rgba(0, 0, 0, 0.6); /* 半透明黑底 */
  backdrop-filter: blur(2px); /* 轻微毛玻璃 */
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  gap: 12px;
  opacity: 0;
  transition: opacity 0.2s ease-in-out;
  z-index: 20;
}

.movie-card:hover .movie-actions-overlay {
  opacity: 1;
}

/* ★★★ 左上角状态角标 (仿参考图) ★★★ */
.status-badge {
  position: absolute;
  top: 10px;
  left: -30px;
  width: 100px;
  height: 24px;
  background-color: #666;
  color: #fff;
  font-size: 12px;
  font-weight: bold;
  display: flex;
  align-items: center;
  justify-content: center;
  transform: rotate(-45deg); /* 旋转45度 */
  box-shadow: 0 2px 4px rgba(0,0,0,0.3);
  z-index: 15;
  pointer-events: none;
}

/* 不同状态的颜色 */
.status-badge.in_library { background-color: #63e2b7; color: #000; } /* Naive UI Success Green */
.status-badge.missing { background-color: #e88080; } /* Naive UI Error Red */
.status-badge.subscribed { background-color: #f2c97d; color: #000; } /* Naive UI Warning */
.status-badge.unreleased { background-color: #8a8a8a; }
.status-badge.unidentified { background-color: #d03050; }

/* 占位符样式 */
.poster-placeholder {
  width: 100%;
  height: 100%;
  background-color: #2d2d30;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  color: #666;
}
</style>