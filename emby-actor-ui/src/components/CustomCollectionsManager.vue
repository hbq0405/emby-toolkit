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
            <n-button @click="showMappingModal = true" secondary type="info">
              <template #icon><n-icon :component="SparklesIcon" /></template>
              映射管理
            </n-button>
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
              <li>您可以通过拖动卡片来对合集进行排序，Emby虚拟库实时联动更新排序。</li>
            </ul>
          </n-alert>
        </template>
      </n-page-header>

      <!-- 2. 数据展示区 (Grid 布局) -->
      <div v-if="isLoading" class="center-container">
        <n-spin size="large" />
      </div>
      <div v-else class="custom-grid" ref="gridRef">
        <div 
          v-for="item in collections" 
          :key="item.id" 
          class="grid-item"
          :data-id="item.id"
        >
          <div 
            class="collection-card dashboard-card" 
            :class="{ 'is-paused': item.status === 'paused' }"
            @click="handleEditClick(item)"
          >
            <!-- 背景层：图片或渐变 -->
            <div class="card-bg">
              <n-image 
                v-if="getCardImageUrl(item)" 
                :src="getCardImageUrl(item)" 
                class="bg-image" 
                object-fit="cover" 
                preview-disabled
                lazy
              >
                <!-- 图片加载失败时显示渐变色兜底 -->
                <template #placeholder>
                  <div class="bg-placeholder" :style="{ background: stringToGradient(item.name) }"></div>
                </template>
              </n-image>
              
              <!-- 如果完全没有图片 URL，直接显示渐变色 -->
              <div v-else class="bg-placeholder" :style="{ background: stringToGradient(item.name) }">
                <span class="placeholder-text">{{ item.name.substring(0, 1) }}</span>
              </div>
              <!-- ★★★ 修改结束 ★★★ -->

              <div class="bg-overlay"></div>
            </div>

            <!-- 右上角：内容类型图标 -->
            <div class="card-type-icons">
              <n-icon v-if="hasContentType(item, 'Movie')" :component="FilmIcon" title="包含电影" />
              <n-icon v-if="hasContentType(item, 'Series')" :component="TvIcon" title="包含剧集" />
            </div>

            <!-- 左下角：主要信息 -->
            <div class="card-info">
              <div v-if="!getCardImageUrl(item)" class="card-title" :title="item.name">{{ item.name }}</div>
              <div class="card-meta">
                <n-tag size="small" :bordered="false" :type="getTypeTagColor(item.type)" class="mini-tag">
                  {{ getTypeLabel(item.type) }}
                </n-tag>
                <span class="sync-time">
                  <n-icon :component="TimeIcon" style="vertical-align: -1px; margin-right: 2px;" />
                  {{ formatTime(item.last_synced_at) }}
                </span>
              </div>
            </div>
            <!-- 右下角：操作按钮 (悬浮显示) -->
            <div class="card-actions" @click.stop>
              <!-- 详情 (仅榜单/推荐类显示) -->
              <n-tooltip v-if="['list', 'ai_recommendation_global'].includes(item.type)">
                <template #trigger>
                  <n-button circle size="small" secondary type="info" @click="openDetailsModal(item)">
                    <template #icon><n-icon :component="EyeIcon" /></template>
                  </n-button>
                </template>
                查看详情/缺失
              </n-tooltip>

              <!-- 生成 -->
              <n-tooltip>
                <template #trigger>
                  <n-button circle size="small" secondary type="primary" :loading="syncLoading[item.id]" @click="handleSync(item)">
                    <template #icon><n-icon :component="GenerateIcon" /></template>
                  </n-button>
                </template>
                立即生成/同步
              </n-tooltip>

              <!-- 删除 -->
              <n-popconfirm @positive-click="handleDelete(item)">
                <template #trigger>
                  <n-button circle size="small" secondary type="error">
                    <template #icon><n-icon :component="DeleteIcon" /></template>
                  </n-button>
                </template>
                确定删除合集 "{{ item.name }}" 吗？
              </n-popconfirm>
            </div>

            <!-- 暂停状态遮罩文字 -->
            <div v-if="item.status === 'paused'" class="paused-overlay">
              PAUSED
            </div>
          </div>
        </div>
      </div>
      
      <!-- 空状态 -->
      <n-empty v-if="!isLoading && collections.length === 0" description="还没有创建任何合集" style="margin-top: 100px;" />

    </div>

    <!-- 3. 创建/编辑模态框 -->
    <n-modal
      v-model:show="showModal"
      preset="card"
      style="width: 90%; max-width: 900px;"
      :title="isEditing ? '编辑合集配置' : '创建新合集'"
      :bordered="false"
      size="huge"
      class="modal-card-lite custom-modal"
    >
      <!-- 头部类型选择区 (仅在新建或未锁定时显示，或者你想一直显示也可以，这里做成卡片式) -->
      <div v-if="!isEditing" class="type-selection-section">
        <div class="section-title">请选择合集类型</div>
        <n-grid :cols="2" :x-gap="16" :y-gap="16" responsive="screen" item-responsive>
          <n-gi span="2 s:1" v-for="opt in typeOptions" :key="opt.value">
            <div 
              class="type-card" 
              :class="{ active: currentCollection.type === opt.value }"
              @click="currentCollection.type = opt.value"
            >
              <div class="type-icon">
                <n-icon :component="opt.icon" size="24" />
              </div>
              <div class="type-info">
                <div class="type-title">{{ opt.label }}</div>
                <div class="type-desc">{{ opt.desc }}</div>
              </div>
              <div class="type-check" v-if="currentCollection.type === opt.value">
                <n-icon :component="CheckmarkCircleIcon" />
              </div>
            </div>
          </n-gi>
        </n-grid>
        <n-divider style="margin: 24px 0;" />
      </div>

      <n-form
        ref="formRef"
        :model="currentCollection"
        :rules="formRules"
        label-placement="top" 
        label-width="auto"
        require-mark-placement="right-hanging"
      >
        <!-- ★★★ 基础信息区块 (包含名称、用户、内容类型) ★★★ -->
        <n-grid :cols="2" :x-gap="24" :y-gap="12">
          <!-- 第一行：名称 和 可见用户 -->
          <n-gi>
            <n-form-item label="合集名称" path="name">
              <n-input v-model:value="currentCollection.name" placeholder="起个好听的名字，例如：豆瓣高分科幻" size="large" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item path="allowed_user_ids">
              <template #label>
                可见用户
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon :component="HelpIcon" style="margin-left: 4px; color: var(--n-text-color-3);" /></template>
                  指定哪些Emby用户可以看到此虚拟库。
                </n-tooltip>
              </template>
              <n-select
                v-model:value="currentCollection.allowed_user_ids"
                multiple filterable clearable
                placeholder="默认对所有用户可见"
                :options="embyUserOptions"
                :loading="isLoadingEmbyUsers"
                :render-label="renderSelectOptionWithTag"
                size="large"
              />
            </n-form-item>
          </n-gi>

          <!-- ★★★ 第二行：内容类型 (移动到这里，所有类型通用) ★★★ -->
          <n-gi :span="2">
            <n-form-item label="合集内容" path="definition.item_type">
              <n-checkbox-group v-model:value="currentCollection.definition.item_type" :disabled="isContentTypeLocked">
                <n-space size="large" align="center" style="background-color: var(--n-action-color); padding: 10px 16px; border-radius: 6px; width: 100%;">
                  <n-checkbox value="Movie">
                    <span style="font-weight: 500;">电影 (Movie)</span>
                  </n-checkbox>
                  <n-checkbox value="Series">
                    <span style="font-weight: 500;">电视剧 (Series)</span>
                  </n-checkbox>
                  <n-text depth="3" style="font-size: 12px; margin-left: auto;">
                    <n-icon :component="HelpIcon" style="vertical-align: -2px; margin-right: 4px;" />
                    决定此合集包含哪种媒体，也影响筛选规则的字段。
                  </n-text>
                </n-space>
              </n-checkbox-group>
            </n-form-item>
          </n-gi>
        </n-grid>

        <!-- 核心配置区块：根据类型变化 -->
        <n-card :bordered="false" embedded class="config-card">
          <!-- 1. 榜单导入 (List) -->
          <div v-if="currentCollection.type === 'list'">
            <n-grid :cols="1" :y-gap="12">
              <n-gi>
                <n-form-item label="榜单来源 (内置/自定义)">
                  <n-space vertical style="width: 100%">
                    <n-select
                      v-model:value="selectedBuiltInLists"
                      multiple filterable clearable
                      placeholder="选择内置榜单 (如猫眼、腾讯热度)"
                      :options="filteredBuiltInLists"
                    />
                    <!-- 自定义 URL 列表 -->
                    <div class="custom-url-list">
                      <div v-for="(urlItem, index) in customUrlList" :key="index" class="url-row">
                        <n-input-group>
                          <n-input 
                            v-model:value="urlItem.value" 
                            placeholder="输入 RSS、TMDb 片单或 Discover 链接"
                          >
                            <template #prefix><n-icon :component="LinkIcon" /></template>
                          </n-input>
                          <n-button type="primary" ghost @click="openDiscoverHelper(index)">
                            <template #icon><n-icon :component="SearchIcon" /></template>
                            TMDb 探索
                          </n-button>
                          <n-button type="error" ghost @click="removeCustomUrl(index)" :disabled="customUrlList.length === 1 && !urlItem.value">
                            <n-icon :component="DeleteIcon" />
                          </n-button>
                        </n-input-group>
                      </div>
                      <n-button dashed block @click="addCustomUrl" class="add-url-btn">
                        <template #icon><n-icon :component="AddIcon" /></template>
                        添加更多自定义链接
                      </n-button>
                    </div>
                  </n-space>
                </n-form-item>
              </n-gi>
              
              <n-gi>
                <!-- ★★★ 原来的内容类型选择框已从这里移除 ★★★ -->
                <n-form-item label="数量限制" path="definition.limit">
                  <n-input-number v-model:value="currentCollection.definition.limit" placeholder="留空不限" :min="1" style="width: 100%;">
                    <template #suffix>项</template>
                  </n-input-number>
                </n-form-item>
              </n-gi>
            </n-grid>
          </div>

          <!-- 2. 筛选规则 (Filter) (保持不变) -->
          <div v-if="currentCollection.type === 'filter'">
             <!-- ... 筛选规则代码保持不变 ... -->
             <n-grid :cols="2" :x-gap="24">
              <n-gi>
                <n-form-item label="匹配逻辑">
                  <n-radio-group v-model:value="currentCollection.definition.logic" name="logic-group">
                    <n-radio-button value="AND">满足所有 (AND)</n-radio-button>
                    <n-radio-button value="OR">满足任一 (OR)</n-radio-button>
                  </n-radio-group>
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="筛选范围 (媒体库)">
                  <n-select
                    v-model:value="currentCollection.definition.target_library_ids"
                    multiple filterable clearable
                    placeholder="默认筛选所有库"
                    :options="embyLibraryOptions"
                    :loading="isLoadingLibraries"
                  />
                </n-form-item>
              </n-gi>
            </n-grid>

            <n-form-item label="筛选规则配置" path="definition.rules" style="margin-top: 8px;">
              <div class="rules-container">
                <div v-for="(rule, index) in currentCollection.definition.rules" :key="index" class="rule-row">
                  <div class="rule-index">{{ index + 1 }}</div>
                  <n-select v-model:value="rule.field" :options="staticFieldOptions" placeholder="字段" class="rule-field" />
                  <n-select v-model:value="rule.operator" :options="getOperatorOptionsForRow(rule)" placeholder="操作" :disabled="!rule.field" class="rule-op" />
                  
                  <div class="rule-value">
                    <template v-if="rule.field === 'genres'">
                      <n-select
                        v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                        v-model:value="rule.value"
                        multiple filterable
                        :placeholder="isGenreSelectionDisabled ? '不建议混合库应用类型筛选' : '选择一个或多个类型'"
                        :options="genreOptions"
                        :disabled="!rule.operator || isGenreSelectionDisabled"
                        style="flex-grow: 1; min-width: 180px;"
                      />
                      <n-select
                        v-else
                        v-model:value="rule.value"
                        filterable
                        :placeholder="isGenreSelectionDisabled ? '不建议混合库应用类型筛选' : '选择类型'"
                        :options="genreOptions"
                        :disabled="!rule.operator || isGenreSelectionDisabled"
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
                    <template v-else-if="rule.field === 'original_language'">
                      <n-select
                        v-model:value="rule.value"
                        :multiple="rule.operator === 'is_one_of'"
                        :options="filterLanguageOptions"
                        placeholder="选择原始语言"
                        clearable
                        filterable
                        style="flex-grow: 1; min-width: 180px;"
                      />
                    </template>
                    <template v-else-if="rule.field === 'studios'">
                      <n-select
                        v-if="['is_one_of', 'is_none_of'].includes(rule.operator)"
                        v-model:value="rule.value"
                        multiple
                        filterable
                        placeholder="选择已配置的工作室"
                        :options="studioMappingOptions"
                        :disabled="!rule.operator"
                        style="flex-grow: 1; min-width: 220px;"
                      />
                      <n-select
                        v-else
                        v-model:value="rule.value"
                        filterable
                        placeholder="选择已配置的工作室"
                        :options="studioMappingOptions"
                        :disabled="!rule.operator"
                        clearable
                        style="flex-grow: 1;"
                      />
                      <!-- 提示用户去配置 -->
                      <n-tooltip trigger="hover">
                        <template #trigger>
                          <n-icon :component="HelpIcon" style="margin-left: 8px; cursor: help; color: var(--n-text-color-3);" />
                        </template>
                        工作室筛选基于“映射管理”中的配置。如需筛选未列出的工作室，请先去添加映射。
                      </n-tooltip>
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
                    <!-- 分辨率\质量\特效\音轨 -->
                    <template v-else-if="['resolution', 'quality', 'effect', 'audio_lang'].includes(rule.field)">
                      <n-select
                        v-model:value="rule.value"
                        :multiple="['is_one_of', 'is_none_of'].includes(rule.operator)"
                        :options="
                          rule.field === 'resolution' ? resolutionOptions :
                          rule.field === 'quality' ? qualityOptions :
                          rule.field === 'effect' ? effectOptions :
                          audioLangOptions
                        "
                        :placeholder="'选择' + ruleConfig[rule.field].label"
                        clearable
                        filterable
                        style="flex-grow: 1; min-width: 200px;" 
                        @update:value="() => { if(!rule.operator) rule.operator = 'eq' }"
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
                      v-else-if="['rating', 'release_year', 'runtime'].includes(rule.field)"
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
                  </div>

                  <n-button text type="error" class="rule-delete" @click="removeRule(index)">
                    <n-icon :component="DeleteIcon" size="18" />
                  </n-button>
                </div>
                
                <n-button dashed block class="add-rule-btn" @click="addRule">
                  <template #icon><n-icon :component="AddIcon" /></template>
                  添加筛选条件
                </n-button>
              </div>
            </n-form-item>
          </div>

          <!-- 3. AI 推荐 (Recommendation) -->
          <div v-if="['ai_recommendation', 'ai_recommendation_global'].includes(currentCollection.type)">
             <div class="ai-hero-section" :class="{ global: currentCollection.type === 'ai_recommendation_global' }">
                <div class="ai-icon-large">
                  <n-icon :component="SparklesIcon" />
                </div>
                <div class="ai-hero-content">
                  <div class="ai-hero-title">
                    {{ currentCollection.type === 'ai_recommendation_global' ? '全局智能推荐 (LLM + 向量)' : '个人专属推荐 (实时向量)' }}
                  </div>
                  <div class="ai-hero-desc">
                    {{ currentCollection.type === 'ai_recommendation_global' 
                      ? 'AI 将分析全站热度数据，结合大模型知识库，生成一份适合大众口味的“猜你喜欢”片单（包含库内未收录的新片）。' 
                      : '基于访问者的实时观看历史，利用本地向量数据库进行毫秒级匹配。每个用户看到的都是为其量身定制的内容（千人千面）。' 
                    }}
                  </div>
                </div>
             </div>

             <n-grid :cols="1" :y-gap="24" style="margin-top: 20px;">
                <!-- 通用设置：推荐数量 -->
                <n-gi>
                   <n-form-item label="推荐数量限制" path="definition.limit">
                    <n-input-number v-model:value="currentCollection.definition.limit" :default-value="20" :min="5" :max="100" style="width: 100%;">
                        <template #suffix>部</template>
                    </n-input-number>
                  </n-form-item>
                </n-gi>

                <!-- ★★★ 只有【全局推荐】才显示以下高级 LLM 选项 ★★★ -->
                <template v-if="currentCollection.type === 'ai_recommendation_global'">
                    <n-gi>
                      <n-form-item path="definition.ai_discovery_ratio">
                        <template #label>
                          新片探索比例 (LLM 权重)
                          <n-tooltip trigger="hover">
                            <template #trigger><n-icon :component="HelpIcon" style="margin-left: 4px;" /></template>
                            比例越高，AI 越倾向于推荐你库里没有的新片（会触发订阅下载）；<br/>
                            比例越低，越倾向于从你现有的库存中挖掘相似影片。
                          </n-tooltip>
                        </template>
                        <n-grid :cols="12" :x-gap="12" style="width: 100%">
                          <n-gi :span="10">
                            <n-slider 
                              v-model:value="currentCollection.definition.ai_discovery_ratio" 
                              :step="0.05" :min="0" :max="1"
                              :format-tooltip="val => `${(val * 100).toFixed(0)}%`"
                            />
                          </n-gi>
                          <n-gi :span="2">
                            <span style="line-height: 34px; margin-left: 8px;">{{ (currentCollection.definition.ai_discovery_ratio * 100).toFixed(0) }}%</span>
                          </n-gi>
                        </n-grid>
                      </n-form-item>
                    </n-gi>
                    <n-gi>
                       <n-form-item label="推荐倾向 (Prompt)" path="definition.ai_prompt">
                        <n-input
                          v-model:value="currentCollection.definition.ai_prompt"
                          type="textarea"
                          placeholder="可选：微调 LLM 的推荐逻辑，例如 '最近心情不好，多推点喜剧' 或 '只要电影，不要剧集'。"
                          :autosize="{ minRows: 2, maxRows: 4 }"
                        />
                      </n-form-item>
                    </n-gi>
                </template>
                
                <!-- ★★★ 【个人推荐】显示提示信息 ★★★ -->
                <template v-else>
                    <n-gi>
                        <n-alert type="info" :bordered="false">
                            无需配置目标用户。系统会在用户访问此合集时，自动读取该用户的观看历史并实时生成推荐结果。
                        </n-alert>
                    </n-gi>
                </template>

             </n-grid>
          </div>
        </n-card>

        <!-- 高级设置 / 底部选项 -->
        <n-collapse arrow-placement="right" style="margin-top: 16px;">
          <n-collapse-item title="高级设置 & 排序" name="advanced">
            <n-card :bordered="false" embedded>
              <n-grid :cols="2" :x-gap="24">
                <n-gi>
                  <n-form-item label="内容排序">
                    <n-input-group>
                      <n-select v-model:value="currentCollection.definition.default_sort_by" :options="sortFieldOptions" style="width: 60%" />
                      <n-select v-model:value="currentCollection.definition.default_sort_order" :options="sortOrderOptions" style="width: 40%" />
                    </n-input-group>
                  </n-form-item>
                </n-gi>
                <n-gi>
                   <n-form-item label="状态" v-if="isEditing">
                      <n-radio-group v-model:value="currentCollection.status">
                          <n-radio-button value="active">启用</n-radio-button>
                          <n-radio-button value="paused">暂停</n-radio-button>
                      </n-radio-group>
                  </n-form-item>
                </n-gi>
              </n-grid>
              
              <n-divider style="margin: 12px 0;" />
              
              <n-space justify="space-between" align="center">
                <n-form-item :show-label="false" style="margin-bottom: 0;">
                   <n-checkbox v-model:checked="currentCollection.definition.show_in_latest">
                     在首页“最新媒体”中显示
                   </n-checkbox>
                </n-form-item>
                <n-form-item :show-label="false" style="margin-bottom: 0;">
                   <n-checkbox v-model:checked="currentCollection.definition.dynamic_filter_enabled">
                     启用实时用户状态过滤 (已看/收藏)
                   </n-checkbox>
                </n-form-item>
              </n-space>

              <!-- 动态过滤规则 (仅当启用时显示) -->
              <div v-if="currentCollection.definition.dynamic_filter_enabled" style="margin-top: 16px; background: rgba(0,0,0,0.03); padding: 12px; border-radius: 8px;">
                 <n-text depth="3" style="font-size: 12px; margin-bottom: 8px; display: block;">动态规则：根据每个用户的实时状态过滤内容</n-text>
                 <div v-for="(rule, index) in currentCollection.definition.dynamic_rules" :key="index" style="display: flex; gap: 8px; margin-bottom: 8px;">
                    <n-select v-model:value="rule.field" :options="dynamicFieldOptions" size="small" style="width: 140px;" />
                    <n-select v-model:value="rule.operator" :options="getOperatorOptionsForRow(rule)" size="small" style="width: 100px;" />
                    <n-select v-if="rule.field === 'playback_status'" v-model:value="rule.value" :options="[{label:'未播放',value:'unplayed'},{label:'播放中',value:'in_progress'},{label:'已播放',value:'played'}]" size="small" style="flex: 1;" />
                    <n-select v-else-if="rule.field === 'is_favorite'" v-model:value="rule.value" :options="[{label:'已收藏',value:true},{label:'未收藏',value:false}]" size="small" style="flex: 1;" />
                    <n-button size="small" text type="error" @click="removeDynamicRule(index)"><n-icon :component="DeleteIcon" /></n-button>
                 </div>
                 <n-button size="small" dashed block @click="addDynamicRule">+ 添加动态条件</n-button>
              </div>
            </n-card>
          </n-collapse-item>
        </n-collapse>

      </n-form>

      <template #footer>
        <div class="modal-footer-custom">
          <div class="footer-left">
             <n-text depth="3" style="font-size: 12px;" v-if="isEditing">ID: {{ currentCollection.id }}</n-text>
          </div>
          <n-space>
            <n-button @click="showModal = false" size="large">取消</n-button>
            <n-button type="primary" @click="handleSave" :loading="isSaving" size="large" style="padding-left: 32px; padding-right: 32px;">
              保存配置
            </n-button>
          </n-space>
        </div>
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
          <!-- 6. 已忽略 (Subscribed) -->
          <n-tab-pane name="ignored" :tab="`已忽略 (${ignoredMediaInModal.length})`">
            <n-empty v-if="ignoredMediaInModal.length === 0" :description="`你没有订阅此合集中的任何${mediaTypeName}。`" style="margin-top: 40px;"></n-empty>
             <n-grid v-else cols="2 s:3 m:4 l:5 xl:6" :x-gap="12" :y-gap="12" responsive="screen">
              <n-gi v-for="(media, index) in ignoredMediaInModal" :key="index">
                <div class="movie-card">
                  <div class="status-badge ignored">已忽略</div>
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
    <!-- 映射管理模态框 (包裹新组件) -->
    <n-modal
      v-model:show="showMappingModal"
      preset="card"
      title="映射规则管理"
      style="width: 900px; max-width: 95%;"
      :bordered="false"
    >
      <!-- 探索助手 -->
      <MappingManager @close="showMappingModal = false" />
    </n-modal>
    <TmdbDiscoveryHelper
      v-model:show="showDiscoverHelper"
      @confirm="handleDiscoverConfirm"
    />

  </n-layout>
</template>

<script setup>
import { ref, onMounted, h, computed, watch, nextTick } from 'vue';
import axios from 'axios';
import { useConfig } from '../composables/useConfig.js';
import Sortable from 'sortablejs';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import MappingManager from './modals/MappingManager.vue';
import TmdbDiscoveryHelper from './modals/TmdbDiscoveryHelper.vue';
import { 
  NLayout, NPageHeader, NButton, NIcon, NText, NTag, NSpace,
  useMessage, NPopconfirm, NModal, NForm, NFormItem, NInput, NSelect,
  NAlert, NRadioGroup, NRadio, NTooltip, NSpin, NGrid, NGi, NCard, NEmpty, useDialog, NTabs, NTabPane, NCheckboxGroup, NCheckbox, NInputNumber, NAutoComplete, NDynamicTags, NInputGroup, NRadioButton, NSlider, NAvatar, NImage
} from 'naive-ui';
import { 
  AddOutline as AddIcon, 
  CreateOutline as EditIcon, 
  TrashOutline as DeleteIcon,
  SyncOutline as SyncIcon,
  EyeOutline as EyeIcon,
  PlayOutline as GenerateIcon,
  HelpCircleOutline as HelpIcon,
  ImageOutline as CoverIcon,
  BuildOutline as FixIcon,
  SearchOutline as SearchIcon,
  SparklesOutline as SparklesIcon,
  ListOutline as ListIcon,
  FilterOutline as FilterIcon,
  PersonOutline as PersonIcon,
  GlobeOutline as GlobeIcon,
  LinkOutline as LinkIcon,
  CheckmarkCircle as CheckmarkCircleIcon,
  FilmOutline as FilmIcon,
  TvOutline as TvIcon,
  TimeOutline as TimeIcon
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
const gridRef = ref(null); // Grid 容器引用
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
const newSeasonNumber = ref(null);
let sortableInstance = null;

const unidentifiedMediaInModal = computed(() => filterMediaByStatus('unidentified'));
const { configModel } = useConfig();
const showMappingModal = ref(false);
const studioMappingOptions = ref([]);
// ★★★ 1. 探索助手控制状态
const showDiscoverHelper = ref(false);
const editingUrlIndex = ref(0); // 记录当前正在编辑哪一行 URL

// ★★★ 2. 打开助手
const openDiscoverHelper = (index = 0) => {
  editingUrlIndex.value = index;
  showDiscoverHelper.value = true;
};

// ★★★ 3. 确认回调
const handleDiscoverConfirm = (url, type) => {
  // 1. 填入 URL
  if (customUrlList.value[editingUrlIndex.value]) {
    customUrlList.value[editingUrlIndex.value].value = url;
  } else {
    customUrlList.value.push({ value: url });
  }

  // 2. 自动勾选对应的类型 (Movie/Series)
  const itemType = type === 'movie' ? 'Movie' : 'Series';
  if (!currentCollection.value.definition.item_type.includes(itemType)) {
    currentCollection.value.definition.item_type.push(itemType);
    message.success(`已自动勾选“${itemType === 'Movie' ? '电影' : '电视剧'}”类型`);
  }
};

// ===================================================================
// ▼▼▼ 辅助函数 ▼▼▼
// ===================================================================

// 判断内容类型
const hasContentType = (item, type) => {
  const types = item.definition?.item_type || ['Movie'];
  return Array.isArray(types) ? types.includes(type) : types === type;
};

// 获取类型标签文本
const getTypeLabel = (type) => {
  const map = {
    'list': '榜单',
    'filter': '筛选',
    'ai_recommendation': '个人推荐',
    'ai_recommendation_global': '全局推荐'
  };
  return map[type] || '未知';
};

// 获取类型标签颜色
const getTypeTagColor = (type) => {
  const map = {
    'list': 'warning',
    'filter': 'info',
    'ai_recommendation': 'success',
    'ai_recommendation_global': 'primary'
  };
  return map[type] || 'default';
};

// 格式化时间
const formatTime = (timeStr) => {
  if (!timeStr) return '从未同步';
  try {
    return formatDistanceToNow(new Date(timeStr), { addSuffix: true, locale: zhCN });
  } catch (e) {
    return timeStr;
  }
};

// 根据字符串生成随机渐变色
const stringToGradient = (str) => {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const c1 = Math.abs(hash % 360);
  const c2 = (c1 + 40) % 360;
  return `linear-gradient(135deg, hsl(${c1}, 60%, 40%), hsl(${c2}, 60%, 30%))`;
};

const openTmdbSearch = (mediaOrTitle) => {
  let query = '';
  if (typeof mediaOrTitle === 'string') {
    query = mediaOrTitle;
  } else if (mediaOrTitle && typeof mediaOrTitle === 'object') {
    query = mediaOrTitle.original_title || mediaOrTitle.title;
  }
  if (!query) {
    message.warning('没有可搜索的标题');
    return;
  }
  const cleanTitle = query.replace(/\s*(第\s*\d+\s*季|Season\s*\d+).*/i, '').trim();
  window.open(`https://www.themoviedb.org/search?query=${encodeURIComponent(cleanTitle)}`, '_blank');
};

const getInitialDiscoverParams = () => ({
  type: 'movie', sort_by: 'popularity.desc', release_year_gte: null, release_year_lte: null,
  with_genres: [], without_genres: [], with_runtime_gte: null, with_runtime_lte: null,
  with_companies: [], with_cast: [], with_crew: [], with_origin_country: null,
  with_original_language: null, vote_average_gte: 0, vote_count_gte: 0,
  with_keywords_labels: [],
});

// ===================================================================
// ▼▼▼ 核心逻辑函数 ▼▼▼
// ===================================================================

const handleFixMatchClick = (media) => {
  newTmdbId.value = '';
  newSeasonNumber.value = null;
  const isSeries = authoritativeCollectionType.value === 'Series';
  dialog.create({
    title: `修正《${media.title}》的匹配`,
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
      const payload = { new_tmdb_id: newTmdbId.value };
      const currentId = media.tmdb_id;
      const isValidId = currentId && String(currentId).toLowerCase() !== 'none';
      if (isValidId) {
        payload.old_tmdb_id = currentId;
      } else {
        payload.old_title = media.original_title || media.title;
      }
      if (isSeries && newSeasonNumber.value !== null && newSeasonNumber.value !== '') {
        payload.season_number = newSeasonNumber.value;
      }
      await submitFixMatch(payload);
    }
  });
};

// 3. 新增：智能获取封面 URL 函数 (替换原有的 getTmdbImageUrl 调用逻辑)
const getCardImageUrl = (item) => {
  // 优先级 1: 如果已同步到 Emby，直接使用 Emby 的 Primary 封面 (这就是生成的拼图封面)
  if (item.emby_collection_id && configModel.value?.emby_server_url && configModel.value?.emby_api_key) {
    const baseUrl = configModel.value.emby_server_url.replace(/\/$/, '');
    const apiKey = configModel.value.emby_api_key;
    // 添加 maxHeight 限制大小，添加 tag 防止缓存旧图 (如果有 emby_image_tag 字段最好，没有也无妨)
    return `${baseUrl}/Items/${item.emby_collection_id}/Images/Primary?api_key=${apiKey}&maxHeight=600&quality=90`;
  }

  // 优先级 2: 使用 TMDb 的背景图 (Backdrop) - 适合 16:9
  if (item.backdrop_path) {
    return `https://image.tmdb.org/t/p/w780${item.backdrop_path}`;
  }

  // 优先级 3: 使用 TMDb 的海报图 (Poster)
  if (item.poster_path) {
    return `https://image.tmdb.org/t/p/w780${item.poster_path}`;
  }

  // 都没有则返回 null，显示渐变色
  return null;
};

const submitFixMatch = async (payload) => {
  if (!selectedCollectionDetails.value?.id) return;
  try {
    const response = await axios.post(`/api/custom_collections/${selectedCollectionDetails.value.id}/fix_match`, payload);
    message.success(response.data.message || '修正成功！正在刷新合集详情...');
    isLoadingDetails.value = true; 
    try {
      const refreshResponse = await axios.get(`/api/custom_collections/${selectedCollectionDetails.value.id}/status`);
      selectedCollectionDetails.value = refreshResponse.data;
    } catch (refreshError) {
      message.error('刷新合集详情失败，请重新打开弹窗。');
      showDetailsModal.value = false; 
    } finally {
      isLoadingDetails.value = false; 
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
  original_language: { label: '原始语言', type: 'select', operators: ['eq', 'is_one_of'] },
  studios: { label: '工作室', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of', 'is_primary'] },
  keywords: { label: '关键词', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] },
  tags: { label: '标签', type: 'select', operators: ['contains', 'is_one_of', 'is_none_of'] }, 
  unified_rating: { label: '家长分级', type: 'select', operators: ['is_one_of', 'is_none_of'] },
  release_date: { label: '上映于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  date_added: { label: '入库于', type: 'date', operators: ['in_last_days', 'not_in_last_days'] },
  is_in_progress: { label: '追剧状态', type: 'single_select_boolean', operators: ['is'] },
  resolution: { label: '分辨率', type: 'select', operators: ['eq', 'is_one_of', 'is_none_of'] },
  quality: { label: '质量标签', type: 'select', operators: ['eq', 'is_one_of', 'is_none_of'] },
  effect: { label: '视频特效', type: 'select', operators: ['eq', 'is_one_of', 'is_none_of'] },
  audio_lang: { label: '音轨语言', type: 'select', operators: ['contains', 'is_one_of'] },
  playback_status: { label: '播放状态', type: 'user_data_select', operators: ['is', 'is_not'] },
  is_favorite: { label: '是否收藏', type: 'user_data_bool', operators: ['is', 'is_not'] },
};

const resolutionOptions = [
  { label: '4K', value: '4k' },
  { label: '1080P', value: '1080p' },
  { label: '720P', value: '720p' },
  { label: '480P', value: '480p' }
];

const qualityOptions = [
  { label: 'Remux', value: 'Remux' },
  { label: 'BluRay', value: 'BluRay' },
  { label: 'WEB-DL', value: 'WEB-DL' },
  { label: 'WEBrip', value: 'WEBrip' },
  { label: 'HDTV', value: 'HDTV' },
  { label: 'DVDrip', value: 'DVDrip' }
];

const effectOptions = [
  { label: 'SDR', value: 'SDR' },
  { label: 'HDR', value: 'HDR' },
  { label: 'HDR10+', value: 'HDR10+' },
  { label: '杜比视界 (所有)', value: 'DoVi' },
  { label: 'DoVi P5', value: 'DoVi_P5' },
  { label: 'DoVi P7', value: 'DoVi_P7' },
  { label: 'DoVi P8', value: 'DoVi_P8' }
];

const audioLangOptions = [
  { label: '国语', value: '国语' },
  { label: '粤语', value: '粤语' },
  { label: '英语', value: '英语' },
  { label: '日语', value: '日语' },
  { label: '韩语', value: '韩语' }
];

const filterLanguageOptions = [
  { label: '英语 (en)', value: 'en' },
  { label: '中文 (zh)', value: 'zh' },
  { label: '日语 (ja)', value: 'ja' },
  { label: '韩语 (ko)', value: 'ko' },
  { label: '法语 (fr)', value: 'fr' },
  { label: '德语 (de)', value: 'de' },
  { label: '俄语 (ru)', value: 'ru' },
  { label: '西班牙语 (es)', value: 'es' },
  { label: '意大利语 (it)', value: 'it' },
  { label: '泰语 (th)', value: 'th' },
  { label: '粤语 (cn)', value: 'cn' },
  { label: '印地语 (hi)', value: 'hi' }
];

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
    console.error('获取关键词失败:', error);
  }
};

const fetchStudioMappingOptions = async () => {
  try {
    const response = await axios.get('/api/custom_collections/config/studios');
    studioMappingOptions.value = response.data;
  } catch (error) {
    console.error('获取工作室映射失败:', error);
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

const renderPersonOption = ({ node, option }) => {
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
  return node;
};

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
      style: {
        display: 'flex',
        alignItems: 'center',
        padding: '0 6px 0 2px', 
        height: '24px'
      },
      round: true 
    },
    {
      default: () => [
        h(NAvatar, {
          src: getTmdbImageUrl(option.profile_path, 'w92'),
          size: 'small',
          style: 'margin-right: 5px;',
          round: true,
        }),
        option.name 
      ]
    }
  );
};

const renderSelectOptionWithTag = (option) => {
  if (option.is_template_source) {
    return h(
      'div', 
      { style: 'display: flex; justify-content: space-between; align-items: center; width: 100%;' },
      [
        h('span', null, option.label), 
        h(NTag, { type: 'success', size: 'small', bordered: false }, { default: () => '模板源' }) 
      ]
    );
  }
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
    if (item.value === 'custom') return;
    if (item.type === 'group') {
      currentGroup = { 
        type: 'group', 
        label: item.label, 
        key: item.key, 
        children: [] 
      };
      result.push(currentGroup);
    } else {
      if (currentGroup) {
        currentGroup.children.push(item);
      } else {
        result.push(item);
      }
    }
  });
  return result;
});
const selectedBuiltInLists = ref([]);
const customUrlList = ref([{ value: '' }]);
const isMultiSource = computed(() => {
  const builtInCount = selectedBuiltInLists.value.length;
  const customCount = customUrlList.value.filter(u => u.value.trim()).length;
  return (builtInCount + customCount) > 1;
});

const isContentTypeLocked = computed(() => {
  return false
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
    show_in_latest: false,
  }
});
const currentCollection = ref(getInitialFormModel());

watch(() => currentCollection.value.type, (newType) => {
  if (isEditing.value) { return; }
  const sharedProps = {
    item_type: ['Movie'],
    default_sort_order: 'Ascending',
    dynamic_filter_enabled: false,
    dynamic_logic: 'AND',
    dynamic_rules: [],
    show_in_latest: false,
  };
  if (newType === 'filter') {
    currentCollection.value.definition = {
      ...sharedProps,
      logic: 'AND',
      rules: [{ field: null, operator: null, value: '' }],
      target_library_ids: [],
      default_sort_by: 'none', 
    };
  } else if (newType === 'ai_recommendation') {
    currentCollection.value.definition = {
        ...sharedProps,
        limit: 50,
        item_type: ['Movie', 'Series'],
        is_global_mode: false,
        default_sort_by: 'PremiereDate', 
        default_sort_order: 'Descending', 
    };
  } else if (newType === 'ai_recommendation_global') {
    currentCollection.value.definition = {
        ...sharedProps,
        ai_prompt: '',
        limit: 20,
        item_type: ['Movie', 'Series'],
        is_global_mode: true,
        ai_discovery_ratio: 0.2,
        default_sort_by: 'PremiereDate', 
        default_sort_order: 'Descending', 
    };
  } else if (newType === 'list') {
    currentCollection.value.definition = { 
      ...sharedProps,
      url: [], 
      limit: null,
      default_sort_by: 'original', 
    };
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

const isGenreSelectionDisabled = computed(() => {
  const types = currentCollection.value.definition?.item_type || [];
  return types.length > 1;
});

const fetchGenreOptions = async () => {
  const types = currentCollection.value.definition?.item_type || [];
  if (types.length !== 1) {
    genreOptions.value = [];
    return;
  }
  const type = types[0];
  let url = '';
  if (type === 'Movie') {
    url = '/api/custom_collections/config/movie_genres';
  } else if (type === 'Series') {
    url = '/api/custom_collections/config/tv_genres';
  }
  if (!url) return;
  try {
    const response = await axios.get(url);
    const genreList = response.data; 
    genreOptions.value = genreList.map(name => ({
      label: name,
      value: name
    }));
  } catch (error) {
    console.warn('获取类型列表失败:', error);
    genreOptions.value = [];
  }
};

watch(() => currentCollection.value.definition?.item_type, () => {
  fetchGenreOptions();
}, { deep: true });

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
  const isFilterRule = !!rule; 
  if (!query) {
    if (isFilterRule) {
      actorOptions.value = Array.isArray(rule.value) ? rule.value : [];
    } else {
      actorOptions.value = []; 
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
        const selectedPersons = Array.isArray(rule.value) ? rule.value : [];
        const selectedIds = new Set(selectedPersons.map(p => p.id));
        const newResults = searchResults.filter(result => !selectedIds.has(result.id));
        actorOptions.value = [...selectedPersons, ...newResults];
      } else {
        actorOptions.value = searchResults;
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
      isSearchingDirectors.value = false; 
    }
  }, 300);
};

const getPersonIdsFromRule = (value) => {
  if (!Array.isArray(value)) return [];
  return value.filter(p => typeof p === 'object' && p !== null).map(p => p.id);
};

const updatePersonRuleValue = (rule, selectedOptions) => {
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
  { label: '榜单导入', value: 'list', desc: 'RSS / 猫眼 / 豆瓣 / TMDb', icon: ListIcon },
  { label: '规则筛选', value: 'filter', desc: '按类型、年代、演员等自动筛选', icon: FilterIcon },
  { label: '个人推荐', value: 'ai_recommendation', desc: 'AI 分析用户口味生成推荐', icon: PersonIcon },
  { label: '全局推荐', value: 'ai_recommendation_global', desc: '基于全站热度的大众推荐', icon: GlobeIcon }
];

const formRules = computed(() => {
  const baseRules = {
    name: { required: true, message: '请输入合集名称', trigger: 'blur' },
    type: { required: true, message: '请选择合集类型' },
    'definition.item_type': { type: 'array', required: true, message: '请至少选择一种合集内容' }
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
const ignoredMediaInModal = computed(() => filterMediaByStatus('ignored'));

const fetchCollections = async () => {
  isLoading.value = true;
  try {
    const response = await axios.get('/api/custom_collections');
    collections.value = response.data;
    
    // 1. 先让 loading 结束，触发 v-else 渲染 grid 容器
    isLoading.value = false; 
    
    // 2. 等待 DOM 更新完毕
    await nextTick(); 
    
    // 3. 此时 gridRef.value 才有值，可以安全绑定
    initSortable();
    
  } catch (error) {
    message.error('加载自定义合集列表失败。');
    isLoading.value = false; // 出错也要关闭 loading
  }
};

const initSortable = () => {
  if (sortableInstance) {
    sortableInstance.destroy();
  }
  const gridEl = gridRef.value;
  if (!gridEl) return;

  sortableInstance = Sortable.create(gridEl, {
    animation: 200,
    draggable: '.grid-item',
    handle: '.collection-card',
    
    filter: '.card-actions, button, .n-button, .n-icon', 
    preventOnFilter: false, // 允许按钮的点击事件正常触发

    forceFallback: true, 
    ghostClass: 'sortable-ghost',
    dragClass: 'sortable-drag',
    delay: 10, 
    delayOnTouchOnly: false,
    onEnd: handleDragEnd,
  });
};

const handleDragEnd = async (event) => {
  const { oldIndex, newIndex } = event;
  // 如果位置没变，直接返回
  if (oldIndex === newIndex) return;

  // 1. 修改本地数组顺序 (让 UI 立即响应)
  const movedItem = collections.value.splice(oldIndex, 1)[0];
  collections.value.splice(newIndex, 0, movedItem);

  // 2. 提取新的 ID 顺序发送给后端
  const orderedIds = collections.value.map(c => c.id);
  isSavingOrder.value = true; // 可以加个 loading 状态防止连续操作

  try {
    // 调用后端 API 更新顺序
    await axios.post('/api/custom_collections/update_order', { ids: orderedIds });
    message.success('顺序已更新');
  } catch (error) {
    message.error('保存顺序失败，正在还原...');
    // 失败时重新拉取列表还原顺序
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
    rowCopy.definition.show_in_latest = false;
  }

  if (!rowCopy.definition.default_sort_by) {
    rowCopy.definition.default_sort_by = 'none';
  }
  if (!rowCopy.definition.default_sort_order) {
    rowCopy.definition.default_sort_order = 'Ascending';
  }

  currentCollection.value = rowCopy;

  if (rowCopy.type === 'filter' && rowCopy.definition?.rules) {
    const initialPersons = rowCopy.definition.rules
      .filter(rule => (rule.field === 'actors' || rule.field === 'directors') && Array.isArray(rule.value))
      .flatMap(rule => rule.value);
    const uniquePersons = Array.from(new Map(initialPersons.map(p => [p.id, p])).values());
    actorOptions.value = uniquePersons;
  } else {
    actorOptions.value = [];
  }

  if (rowCopy.type === 'list') {
    let urls = rowCopy.definition.url;
    if (typeof urls === 'string') {
      urls = urls ? [urls] : [];
    } else if (!Array.isArray(urls)) {
      urls = [];
    }
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
    selectedBuiltInLists.value = foundBuiltIns;
    if (foundCustoms.length === 0) {
      customUrlList.value = [{ value: '' }];
    } else {
      customUrlList.value = foundCustoms;
    }
  } else {
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

watch(() => discoverParams.value.type, () => {
    discoverParams.value.with_genres = [];
    discoverParams.value.with_runtime_gte = null;
    discoverParams.value.with_runtime_lte = null;
});

watch(isMultiSource, (isMulti) => {
  if (isMulti) {
    if (currentCollection.value.definition.default_sort_by === 'original') {
      currentCollection.value.definition.default_sort_by = 'none';
      message.info('检测到多个榜单源，排序已自动重置为“不设置” (多榜单无法保持原始顺序)');
    }
  }
});

watch([selectedBuiltInLists, customUrlList], () => {
  const builtIns = selectedBuiltInLists.value;
  const customs = customUrlList.value.map(i => i.value.trim()).filter(v => v);
  const combinedUrls = [...builtIns, ...customs];
  currentCollection.value.definition.url = combinedUrls;
  const newItemTypes = new Set(currentCollection.value.definition.item_type || ['Movie']);
  builtIns.forEach(url => {
    const option = builtInLists.find(opt => opt.value === url);
    if (option && option.contentType) {
      option.contentType.forEach(t => newItemTypes.add(t));
    }
  });
  currentCollection.value.definition.item_type = Array.from(newItemTypes);
}, { deep: true });

watch(showMappingModal, (newVal) => {
  if (!newVal) {
    // 模态框关闭时，刷新下拉框数据
    fetchKeywordOptions();
    fetchStudioMappingOptions();
  }
});

onMounted(() => {
  fetchCollections();
  fetchCountryOptions();
  fetchTagOptions();
  fetchKeywordOptions(); 
  fetchStudioMappingOptions(); 
  fetchUnifiedRatingOptions();
  fetchEmbyLibraries();
  fetchEmbyUsers();
  if (currentCollection.value.definition?.item_type) {
      fetchGenreOptions();
  }
});

createRuleWatcher(() => currentCollection.value.definition.rules);
createRuleWatcher(() => currentCollection.value.definition.dynamic_rules);

</script>

<style scoped>
.custom-collections-manager {
  padding: 0 10px;
}
.card-actions, .n-button {
  cursor: pointer !important;
}
/* 拖拽手柄样式 */
.drag-handle {
  position: absolute;
  top: 10px;
  left: 10px; /* 放在左上角，或者你喜欢的任何位置 */
  z-index: 10;
  color: rgba(255, 255, 255, 0.6);
  cursor: grab;
  padding: 4px;
  background: rgba(0,0,0,0.3);
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color 0.2s, background 0.2s;
}

.drag-handle:hover {
  color: #fff;
  background: rgba(0,0,0,0.6);
}

.drag-handle:active {
  cursor: grabbing;
}

/* ★★★ Grid 布局核心 ★★★ */
.custom-grid {
  display: grid;
  /* 基础宽度 280px，根据 cardScale 缩放 */
  grid-template-columns: repeat(auto-fill, minmax(calc(280px * var(--card-scale, 1)), 1fr));
  gap: 20px;
  margin-top: 24px;
}

.grid-item {
  /* 确保拖拽时占位正确 */
  position: relative;
  touch-action: none;
}

/* ★★★ 卡片样式 ★★★ */
.collection-card {
  position: relative;
  width: 100%;
  aspect-ratio: 16 / 9; /* 强制 16:9 */
  border-radius: 12px;
  overflow: hidden;
  cursor: pointer;
  cursor: grab;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  transition: transform 0.2s, box-shadow 0.2s, filter 0.3s;
  background-color: #202023;
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.collection-card:active {
  cursor: grabbing;
}

.collection-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 12px 24px rgba(0, 0, 0, 0.3);
  z-index: 2;
}

/* ★★★ 暂停状态：黑白滤镜 ★★★ */
.collection-card.is-paused {
  filter: grayscale(100%);
  opacity: 0.8;
}
.collection-card.is-paused:hover {
  filter: grayscale(0%); /* 悬停时恢复彩色，方便操作 */
  opacity: 1;
}

/* 背景层 */
.card-bg {
  position: absolute;
  inset: 0;
  z-index: 0;
}

.bg-image {
  width: 100%;
  height: 100%;
  pointer-events: none;
}
/* 深度选择器确保 naive-ui image 组件内部 img 填满 */
.bg-image :deep(img) {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.bg-placeholder {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
}
.placeholder-text {
  font-size: 48px;
  font-weight: bold;
  color: rgba(255, 255, 255, 0.2);
  text-transform: uppercase;
}

/* 渐变遮罩：保证文字清晰 */
.bg-overlay {
  position: absolute;
  inset: 0;
  background: linear-gradient(to top, rgba(0, 0, 0, 0.9) 0%, rgba(0, 0, 0, 0.4) 50%, rgba(0, 0, 0, 0.1) 100%);
}

/* ★★★ 右上角图标 ★★★ */
.card-type-icons {
  position: absolute;
  top: 10px;
  right: 10px;
  z-index: 2;
  display: flex;
  gap: 6px;
  color: rgba(255, 255, 255, 0.9);
  background: rgba(0, 0, 0, 0.5);
  padding: 4px 8px;
  border-radius: 20px;
  backdrop-filter: blur(4px);
}

/* ★★★ 左下角信息 ★★★ */
.card-info {
  position: absolute;
  bottom: 12px;
  left: 12px;
  right: 12px; /* 留出右侧空间给按钮 */
  z-index: 2;
  pointer-events: none; /* 让点击穿透 */
}

.card-title {
  font-size: calc(16px * var(--card-scale, 1));
  font-weight: bold;
  color: #fff;
  text-shadow: 0 2px 4px rgba(0, 0, 0, 0.8);
  margin-bottom: 6px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.card-meta {
  display: flex;
  align-items: center;
  gap: 8px;
}

.mini-tag {
  height: 20px;
  font-size: 11px;
  opacity: 0.9;
}

.sync-time {
  font-size: 11px;
  color: rgba(255, 255, 255, 0.7);
}

/* ★★★ 右下角操作按钮 ★★★ */
.card-actions {
  position: absolute;
  bottom: 10px;
  right: 10px;
  z-index: 3;
  display: flex;
  gap: 8px;
  opacity: 0; /* 默认隐藏 */
  transform: translateY(10px);
  transition: all 0.2s ease;
}

/* 悬停卡片时显示按钮 */
.collection-card:hover .card-actions {
  opacity: 1;
  transform: translateY(0);
}

/* 暂停状态遮罩文字 */
.paused-overlay {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%) rotate(-15deg);
  font-size: 24px;
  font-weight: 900;
  color: rgba(255, 255, 255, 0.3);
  border: 4px solid rgba(255, 255, 255, 0.3);
  padding: 4px 12px;
  border-radius: 8px;
  pointer-events: none;
  z-index: 1;
  letter-spacing: 2px;
}

/* SortableJS 拖拽样式 */
.sortable-ghost {
  opacity: 0.4;
  background: rgba(255, 255, 255, 0.1);
  border: 2px dashed var(--n-primary-color);
  border-radius: 12px;
  pointer-events: none !important;
}
.sortable-drag {
  cursor: grabbing;
  opacity: 1;
  background: #202023; /* 保持背景色，防止透明 */
  box-shadow: 0 16px 32px rgba(0,0,0,0.5); /* 增加阴影，营造浮起感 */
  transform: scale(1.05); /* 稍微放大 */
  z-index: 9999; /* 确保在最上层 */
  pointer-events: none !important;
}

/* 居中加载容器 */
.center-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 400px;
}

/* ... (保留原有的模态框样式) ... */
.movie-card {
  border-radius: 8px;
  overflow: hidden;
  position: relative;
  aspect-ratio: 2 / 3; 
  background-color: #202023;
  transition: transform 0.2s, box-shadow 0.2s;
  cursor: default;
}

.movie-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5);
  z-index: 2;
}

.movie-poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  transition: transform 0.3s;
}

.movie-card:hover .movie-poster {
  transform: scale(1.05);
}

.movie-info-overlay {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 60px 10px 10px 10px; 
  background: linear-gradient(to top, rgba(0, 0, 0, 0.95) 0%, rgba(0, 0, 0, 0.7) 60%, transparent 100%);
  color: #fff;
  pointer-events: none; 
  z-index: 10;
}

.movie-title {
  font-size: 14px;
  font-weight: bold;
  line-height: 1.3;
  text-shadow: 0 1px 2px rgba(0,0,0,0.8);
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
}

.movie-year {
  font-size: 12px;
  color: #ddd; 
  margin-top: 2px;
  font-weight: 500;
}

.original-source-title {
  font-size: 11px;
  color: #aaa;
  margin-top: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  opacity: 0.8;
}

.movie-actions-overlay {
  position: absolute;
  inset: 0; 
  background: rgba(0, 0, 0, 0.6); 
  backdrop-filter: blur(2px); 
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
  transform: rotate(-45deg); 
  box-shadow: 0 2px 4px rgba(0,0,0,0.3);
  z-index: 15;
  pointer-events: none;
}

.status-badge.in_library { background-color: #63e2b7; color: #000; } 
.status-badge.missing { background-color: #e88080; } 
.status-badge.subscribed { background-color: #f2c97d; color: #000; } 
.status-badge.unreleased { background-color: #8a8a8a; }
.status-badge.unidentified { background-color: #d03050; }
.status-badge.ignored { background-color: #69ace2; }

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

.type-selection-section {
  margin-bottom: 20px;
}
.section-title {
  font-size: 16px;
  font-weight: bold;
  margin-bottom: 12px;
  color: var(--n-text-color-1);
}
.type-card {
  border: 1px solid var(--n-border-color);
  border-radius: 8px;
  padding: 16px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 12px;
  transition: all 0.2s ease;
  position: relative;
  background-color: var(--n-card-color);
}
.type-card:hover {
  border-color: var(--n-primary-color);
  background-color: rgba(var(--n-primary-color-rgb), 0.05);
  transform: translateY(-2px);
}
.type-card.active {
  border-color: var(--n-primary-color);
  background-color: rgba(var(--n-primary-color-rgb), 0.1);
  box-shadow: 0 0 0 1px var(--n-primary-color) inset;
}
.type-icon {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background-color: var(--n-color-modal);
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--n-text-color-2);
}
.type-card.active .type-icon {
  background-color: var(--n-primary-color);
  color: #fff;
}
.type-info {
  flex: 1;
}
.type-title {
  font-weight: bold;
  font-size: 14px;
}
.type-desc {
  font-size: 12px;
  color: var(--n-text-color-3);
  margin-top: 2px;
}
.type-check {
  position: absolute;
  top: 8px;
  right: 8px;
  color: var(--n-primary-color);
}

.config-card {
  background-color: var(--n-action-color); 
  border-radius: 8px;
  margin-top: 8px;
}

.custom-url-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 8px;
}
.add-url-btn {
  margin-top: 4px;
  border-style: dashed;
}

.rules-container {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.rule-row {
  display: flex;
  align-items: center;
  gap: 8px;
  background-color: var(--n-card-color);
  padding: 8px 12px;
  border-radius: 6px;
  border: 1px solid var(--n-border-color);
}
.rule-index {
  width: 20px;
  color: var(--n-text-color-3);
  font-size: 12px;
  font-weight: bold;
}
.rule-field { width: 140px; }
.rule-op { width: 110px; }
.rule-value { flex: 1; display: flex; }
.rule-delete { margin-left: 4px; }

.ai-section {
  margin-top: 20px;
  border: 1px solid rgba(242, 201, 125, 0.3); 
  background: rgba(242, 201, 125, 0.05);
  border-radius: 8px;
  overflow: hidden;
}
.ai-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  background: rgba(242, 201, 125, 0.1);
  font-weight: bold;
  color: #f2c97d;
}
.ai-content {
  padding: 16px;
}

.ai-hero-section {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 20px;
  background: linear-gradient(135deg, rgba(var(--n-primary-color-rgb), 0.1) 0%, rgba(var(--n-primary-color-rgb), 0) 100%);
  border-radius: 8px;
  border: 1px solid rgba(var(--n-primary-color-rgb), 0.2);
}
.ai-hero-section.global {
  background: linear-gradient(135deg, rgba(100, 200, 255, 0.1) 0%, rgba(100, 200, 255, 0) 100%);
  border-color: rgba(100, 200, 255, 0.2);
}
.ai-icon-large {
  font-size: 40px;
  color: var(--n-primary-color);
}
.ai-hero-title {
  font-size: 18px;
  font-weight: bold;
}
.ai-hero-desc {
  color: var(--n-text-color-3);
  margin-top: 4px;
}

.modal-footer-custom {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>