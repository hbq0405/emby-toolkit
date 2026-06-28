<!-- frontend/src/components/SharedResourceManagerPage.vue -->
<template>
  <div class="shared-page">
    <n-space vertical :size="18">
      <n-card :bordered="false" class="dashboard-card">
        <template #header>
          <div class="page-header">
            <div>
              <div class="page-title">共享资源管理</div>
              <n-text depth="3">集中管理共享资源：本机秒传源、中心资源秒传和贡献点流水。</n-text>
            </div>
            <n-space>
              <n-button secondary @click="openSharedConfigModal">
                <template #icon><n-icon :component="SettingsIcon" /></template>
                配置
              </n-button>
              <n-button v-if="needsCenterServerId" type="warning" ghost :loading="registeringDevice" @click="registerCenterDevice">
                <template #icon><n-icon :component="SyncIcon" /></template>
                {{ centerDeviceRegisterButtonText }}
              </n-button>
              <n-button :loading="refreshingCredit" @click="refreshCredit">
                <template #icon><n-icon :component="RefreshIcon" /></template>
                刷新贡献点
              </n-button>
              <n-button type="primary" ghost :loading="loading" @click="loadAll(true)">
                <template #icon><n-icon :component="SyncIcon" /></template>
                刷新列表
              </n-button>
            </n-space>
          </div>
        </template>

        <n-alert v-if="needsCenterServerId" class="center-register-alert" type="warning" :bordered="false" style="margin-bottom: 12px;">
          {{ centerServerIdAlertText }}
        </n-alert>

        <n-grid class="stat-grid" :cols="isMobile ? 2 : 5" :x-gap="12" :y-gap="12">
          <n-gi v-for="card in statCards" :key="card.key">
            <div class="stat-card">
              <div class="stat-label">{{ card.label }}</div>
              <div class="stat-value">{{ card.value }}</div>
              <div class="stat-desc">{{ card.desc }}</div>
            </div>
          </n-gi>
        </n-grid>
      </n-card>

      <n-card :bordered="false" class="dashboard-card shared-list-card">
        <n-tabs v-model:value="activeTab" animated type="line" @update:value="handleTabChange">
          <n-tab-pane name="shares" tab="我的共享源">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              管理本机登记到共享中心的秒传资源。客户端只上传电影/分集秒传资产；完结季由中心逻辑季包统一归类、认证和派发 115 文件列表分享。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="shareFilters.keyword" placeholder="搜索标题 / 文件名 / TMDb ID / SHA1" clearable @keyup.enter="loadShares">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="shareFilters.status" :options="shareStatusOptions" style="width: 170px" />
              <n-button type="primary" :loading="sharesLoading" @click="loadShares">查询</n-button>
              <n-button type="primary" @click="openManualShareModal">
                <template #icon><n-icon :component="ShareIcon" /></template>
                手动登记
              </n-button>
              <n-button type="warning" secondary :loading="shareAllLoading" @click="confirmShareAllLibrary">
                <template #icon><n-icon :component="ShareIcon" /></template>
                一键登记媒体库
              </n-button>
            </n-space>
            <n-data-table
              remote
              :loading="sharesLoading"
              :columns="shareColumns"
              :data="shareItems"
              :pagination="sharePagination"
              :row-key="row => row.id"
              :scroll-x="1350"
              @update:page="p => { sharePagination.page = p; loadShares(); }"
              @update:page-size="s => { sharePagination.pageSize = s; sharePagination.page = 1; loadShares(); }"
            />
          </n-tab-pane>


          <n-tab-pane name="center" tab="中心资源库">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              这里展示共享中心已收录的资源版本。
有可用 115 文件列表分享的逻辑完结季会显示“转存”；没有可用分享时仍显示“秒传”并走 Rapid 兜底。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="centerFilters.keyword" placeholder="搜索标题 / 文件名 / TMDb ID / SHA1" clearable @keyup.enter="resetCenterSources()">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-button type="primary" :loading="centerLoading" @click="resetCenterSources()">查询中心</n-button>
              <n-button v-if="centerHomeMode" secondary @click="openCenterHomeSettingsModal">
                <template #icon><n-icon :component="SettingsIcon" /></template>
                列表设置
              </n-button>
              <n-button secondary :loading="maintenanceSubmitting" @click="triggerSharedMaintenance">执行维护任务</n-button>
            </n-space>
            <n-spin :show="centerLoading && !centerAppendLoading">
              <template v-if="centerHomeMode && centerHomeSections.length">
                <div v-for="section in centerHomeSections" :key="section.key" class="center-home-section">
                  <div class="center-home-section-head">
                    <div class="center-home-section-title">{{ section.title }}</div>
                  </div>
                  <div class="center-card-grid">
                    <div
                      v-for="(row, centerIndex) in section.items"
                      :key="`${section.key}:${centerTableRowKey(row)}`"
                      class="center-card-item"
                    >
                      <n-card class="center-media-card poster-wall-card" :bordered="false" content-style="padding: 0; position: relative;" @click="openCenterDetail(row)">
                        <div class="center-poster-wrapper poster-wall-wrapper">
                          <img v-bind="centerPosterImgAttrs(row, 'w185', centerIndex)" class="center-poster" @error="onCenterPosterError" />
                          <div v-if="centerRibbonText(row)" :class="['center-ribbon', centerRibbonClass(row)]">
                            <span>{{ centerRibbonText(row) }}</span>
                          </div>
                          <div class="center-card-overlay poster-wall-overlay">
                            <div class="center-card-text poster-wall-text">
                              <div class="poster-wall-title-line" :title="centerPosterWallFullTitle(row)">{{ centerPosterWallPrimaryTitle(row) }}</div>
                              <div v-if="centerPosterWallYear(row)" class="poster-wall-year-line">{{ centerPosterWallYear(row) }}</div>
                            </div>
                          </div>
                        </div>
                      </n-card>
                    </div>
                  </div>
                </div>
              </template>
              <div v-else-if="groupedCenterSources.length" class="center-card-grid">
                <div
                  v-for="(row, centerIndex) in groupedCenterSources"
                  :key="centerTableRowKey(row)"
                  class="center-card-item"
                >
                  <n-card class="center-media-card poster-wall-card" :bordered="false" content-style="padding: 0; position: relative;" @click="openCenterDetail(row)">
                    <div class="center-poster-wrapper poster-wall-wrapper">
                      <img v-bind="centerPosterImgAttrs(row, 'w185', centerIndex)" class="center-poster" @error="onCenterPosterError" />

                      <div v-if="centerRibbonText(row)" :class="['center-ribbon', centerRibbonClass(row)]">
                        <span>{{ centerRibbonText(row) }}</span>
                      </div>

                      <div class="center-card-overlay poster-wall-overlay">
                        <div class="center-card-text poster-wall-text">
                          <div class="poster-wall-title-line" :title="centerPosterWallFullTitle(row)">{{ centerPosterWallPrimaryTitle(row) }}</div>
                          <div v-if="centerPosterWallYear(row)" class="poster-wall-year-line">{{ centerPosterWallYear(row) }}</div>
                        </div>
                      </div>
                    </div>
                  </n-card>
                </div>
              </div>
              <div v-else class="center-empty-card">
                <n-text depth="3">暂无中心资源。可以换个关键词搜索。</n-text>
              </div>

              <div ref="centerInfiniteSentinel" class="center-infinite-sentinel">
                <n-spin v-if="centerAppendLoading" size="small" />
                <n-text v-else-if="!centerHomeMode && centerHasMore" depth="3">继续下滑加载更多</n-text>
                <n-text v-else-if="!centerHomeMode && groupedCenterSources.length" depth="3">已加载全部 {{ centerPagination.itemCount || groupedCenterSources.length }} 个资源</n-text>
              </div>
            </n-spin>
          </n-tab-pane>

          <n-tab-pane name="virtual" tab="虚拟入库">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              管理已虚拟入库的共享资源。辞退会移除本地虚拟 STRM；转正会执行正式秒传/转存入库。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="virtualFilters.keyword" placeholder="搜索标题 / TMDb ID / 源 ID" clearable @keyup.enter="loadVirtualImports">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="virtualFilters.item_type" :options="virtualTypeOptions" style="width: 140px" />
              <n-button type="primary" :loading="virtualLoading" @click="loadVirtualImports">查询</n-button>
            </n-space>
            <n-data-table
              remote
              :loading="virtualLoading"
              :columns="virtualColumns"
              :data="virtualItems"
              :pagination="virtualPagination"
              :row-key="row => row.id"
              :scroll-x="980"
              @update:page="p => { virtualPagination.page = p; loadVirtualImports(); }"
              @update:page-size="s => { virtualPagination.pageSize = s; virtualPagination.page = 1; loadVirtualImports(); }"
            />
          </n-tab-pane>

          <n-tab-pane name="requests" tab="求共享">
            <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
              求共享是共享池悬赏需求：发起时冻结贡献点，参数越精确悬赏越高；其他用户可“同求”助力，也可以点“我有资源”从本地媒体库登记共享源。
            </n-alert>
            <n-space class="toolbar" :vertical="isMobile" :size="12">
              <n-input v-model:value="requestFilters.keyword" placeholder="搜索片名 / TMDb ID" clearable @keyup.enter="loadShareRequests">
                <template #prefix><n-icon :component="SearchIcon" /></template>
              </n-input>
              <n-select v-model:value="requestFilters.status" :options="requestStatusOptions" style="width: 130px" />
              <n-select v-model:value="requestFilters.media_type" :options="requestMediaTypeOptions" style="width: 130px" />
              <n-select v-model:value="requestFilters.target_type" :options="requestTargetTypeFilterOptions" style="width: 140px" />
              <n-button type="primary" :loading="requestLoading" @click="loadShareRequests">查询</n-button>
              <n-button type="primary" @click="openShareRequestModal">
                <template #icon><n-icon :component="ShareIcon" /></template>
                求共享
              </n-button>
            </n-space>

            <n-spin class="share-request-spin" :show="requestLoading">
              <div v-if="shareRequests.length" class="share-request-grid">
                <n-card v-for="req in shareRequests" :key="req.group_id" size="small" :bordered="false" class="share-request-card">
                  <div class="share-request-card-body">
                    <img class="share-request-poster" :src="requestPosterUrl(req)" @error="onRequestPosterError" />
                    <div class="share-request-info">
                      <div class="share-request-title">{{ appendYear(req.title, req.release_year) }}</div>
                      <div class="share-request-meta">{{ requestTargetText(req) }} · TMDb {{ req.tmdb_id || '-' }}</div>
                      <div class="share-request-condition">{{ requestConditionText(req) }}</div>
                      <div class="share-request-tags">
                        <n-tag size="small" round type="warning">悬赏 {{ req.bounty_total || req.current_bounty || 0 }}</n-tag>
                        <n-tag size="small" round type="info">同求 ×{{ req.co_request_count || 0 }}</n-tag>
                        <n-tag size="small" round :type="req.status === 'open' ? 'success' : 'default'">{{ requestStatusLabel(req.status) }}</n-tag>
                      </div>
                      <div class="share-request-time">{{ fmtDate(req.created_at) }} 发起 · {{ req.expires_at ? fmtDate(req.expires_at) + ' 到期' : '长期有效' }}</div>
                    </div>
                  </div>
                  <div class="share-request-footer">
                    <span class="share-request-owner">{{ requestParticipationText(req) }}</span>
                    <div class="share-request-actions">
                      <n-button size="tiny" secondary :disabled="!canProvideShareRequest(req)" @click="openLocalShareForRequest(req)">我有资源</n-button>
                      <n-button size="tiny" type="primary" secondary :disabled="req.status !== 'open' || req.joined_by_me || req.my_role === 'owner'" @click="confirmCoRequest(req)">同求</n-button>
                      <n-button v-if="req.joined_by_me && req.status === 'open'" size="tiny" type="error" ghost @click="confirmCancelShareRequest(req)">取消</n-button>
                    </div>
                  </div>
                </n-card>
              </div>
              <n-card v-else :bordered="false" class="empty-request-card">
                <n-text depth="3">暂无求共享。可以点击“求共享”发布一个悬赏需求。</n-text>
              </n-card>
            </n-spin>
          </n-tab-pane>

          <n-tab-pane name="ledger" tab="贡献点明细">
            <n-data-table
              :loading="ledgerLoading"
              :columns="ledgerColumns"
              :data="ledgerDisplayItems"
              :row-key="row => row.__row_key || row.id"
              :pagination="false"
              :scroll-x="900"
            />
          </n-tab-pane>
        </n-tabs>
      </n-card>
    </n-space>

    <n-modal v-model:show="showSharedConfigModal" preset="card" title="共享资源配置" style="width: 880px; max-width: 96vw;" class="custom-modal glass-modal">
      <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
        这里集中管理共享资源中心配置；全局配置页中的共享资源配置已迁移到这里。
      </n-alert>
      <n-spin :show="sharedConfigLoading">
        <n-form :model="sharedConfigForm" label-placement="left" label-width="150">
          <n-divider title-placement="left">共享资源中心</n-divider>
          <n-form-item label="共享资源">
            <n-switch v-model:value="sharedConfigForm.p115_shared_resource_enabled">
              <template #checked>启用共享池</template>
              <template #unchecked>关闭</template>
            </n-switch>
          </n-form-item>
          <n-form-item label="中心设备状态">
            <n-spin :show="centerDeviceStatusLoading" size="small">
              <div class="center-device-status-panel">
                <div class="center-device-status-head">
                  <n-tag size="small" round :type="centerDeviceStatusTagType(centerConfigDeviceStatus.status)">
                    {{ centerConfigDeviceStatusLabel }}
                  </n-tag>
                  <span class="center-device-status-name">ServerID Hash：{{ centerConfigServerIdHash || '-' }}</span>
                </div>
                <n-alert v-if="centerConfigForcedOfflineReason" type="error" :bordered="false" class="center-device-status-alert">
                  封禁理由：{{ centerConfigForcedOfflineReason }}
                </n-alert>
                <n-text v-else-if="centerDeviceStatusError" type="error">{{ centerDeviceStatusError }}</n-text>
              </div>
            </n-spin>
          </n-form-item>
          <n-form-item label="禁止单集秒传">
            <n-switch v-model:value="sharedConfigForm.p115_shared_disable_episode_transfer">
              <template #checked>不秒传单集</template>
              <template #unchecked>允许秒传</template>
            </n-switch>
            <template #feedback>仅影响共享池中心源消费；开启后会过滤 `Episode` 单集资源，电影和季包不受影响。</template>
          </n-form-item>
          <n-form-item label="禁止纯净版秒传">
            <n-switch v-model:value="sharedConfigForm.p115_shared_block_clean_version_transfer">
              <template #checked>不秒传纯净版</template>
              <template #unchecked>允许秒传</template>
            </n-switch>
            <template #feedback>仅检测中心逻辑季包/剧集资源：普通剧多数集实际时长比 TMDb 官方时长短约 3 分钟会识别为纯净版；短剧片头更短，阈值改为约 1 分钟。自动/手动秒传都会按该开关拦截。</template>
          </n-form-item>
          <n-form-item label="禁止短剧秒传">
            <n-switch v-model:value="sharedConfigForm.p115_shared_block_short_drama_transfer">
              <template #checked>不秒传短剧</template>
              <template #unchecked>允许秒传</template>
            </n-switch>
            <template #feedback>中心资源被标记为短剧时跳过秒传；短剧按单个视频实际时长低于 25 分钟识别，季包按包内多数集统计。</template>
          </n-form-item>
          <n-form-item label="共享片头">
            <n-switch v-model:value="sharedConfigForm.p115_shared_intro_enabled">
              <template #checked>上传并复用片头</template>
              <template #unchecked>关闭</template>
            </n-switch>
            <template #feedback>开启后会上传片头数据，并在入库时从中心匹配片头数据。</template>
          </n-form-item>
          <n-form-item label="自动响应求共享">
            <n-switch v-model:value="sharedConfigForm.p115_shared_auto_share_requests_enabled">
              <template #checked>自动共享别人所求</template>
              <template #unchecked>关闭</template>
            </n-switch>
            <template #feedback>维护任务会拉取中心“求共享”列表，跳过自己发起/同求的需求，按参数匹配本地媒体库，命中后直接登记本机秒传索引到中心。</template>
          </n-form-item>
          <n-divider title-placement="left">虚拟入库</n-divider>
          <n-form-item label="虚拟入库">
            <n-switch v-model:value="sharedConfigForm.p115_shared_virtual_import_enabled">
              <template #checked>开启</template>
              <template #unchecked>关闭</template>
            </n-switch>
            <template #feedback>开启后中心资源仅生成虚拟 STRM 和媒体信息；点播时会秒传到临时目录，即播即删。</template>
          </n-form-item>
          <n-form-item label="剧集自动转正">
            <n-input-number v-model:value="sharedConfigForm.p115_shared_virtual_auto_promote_episodes" :min="0" :precision="0" style="width: 180px" />
            <template #feedback>观看达到 N 集后自动转正；填 0 表示不自动转正。</template>
          </n-form-item>
          <n-form-item label="电影自动转正">
            <n-input-number v-model:value="sharedConfigForm.p115_shared_virtual_auto_promote_movie_percent" :min="0" :max="100" :precision="0" style="width: 180px" />
            <template #feedback>观看进度达到 N% 后自动转正；填 0 表示不自动转正。</template>
          </n-form-item>

        </n-form>
      </n-spin>
      <template #footer>
        <n-space justify="space-between" align="center">
          <n-text depth="3">共享池命中后使用本机 115 CK 秒传入库，中心不保存 CK。</n-text>
          <n-space>
            <n-button @click="showSharedConfigModal = false">取消</n-button>
            <n-button type="primary" :loading="sharedConfigSaving" @click="saveSharedConfig">保存配置</n-button>
          </n-space>
        </n-space>
      </template>
    </n-modal>

    <n-modal v-model:show="showCenterHomeSettingsModal" preset="card" title="中心资源库列表设置" style="width: 1080px; max-width: 96vw;" class="custom-modal glass-modal">
      <n-alert type="info" :bordered="false" style="margin-bottom: 12px;">
        列表配置会保存到共享资源配置库；拖动调整顺序，关闭后中心端不会查询该列表。筛选项留空就是不限。
      </n-alert>
      <draggable
        v-model="centerHomeSettingSections"
        item-key="key"
        handle=".center-home-setting-drag"
        animation="180"
        class="center-home-setting-list"
      >
        <template #item="{ element, index }">
          <div class="center-home-setting-item">
            <n-icon class="center-home-setting-drag" :component="MenuIcon" size="18" />
            <n-input v-model:value="element.title" size="small" placeholder="列表标题" class="center-home-setting-title-input" />
            <n-select v-model:value="element.display_type" size="small" :options="centerHomeDisplayTypeOptions" class="center-home-setting-select" />
            <n-select v-model:value="element.genre_id" size="small" :options="centerHomeGenreOptions(element.display_type)" clearable filterable placeholder="TMDb 类型" class="center-home-setting-select" />
            <n-select v-model:value="element.tags" size="small" :options="centerHomeTagOptions" multiple clearable placeholder="标签" class="center-home-setting-tags" />
            <n-select v-model:value="element.order_by" size="small" :options="centerHomeOrderOptions" class="center-home-setting-select" />
            <n-select v-model:value="element.limit" size="small" :options="centerHomeLimitOptions" class="center-home-setting-limit" />
            <n-switch v-model:value="element.enabled" size="small">
              <template #checked>显示</template>
              <template #unchecked>隐藏</template>
            </n-switch>
            <n-button size="tiny" quaternary circle type="error" @click="removeCenterHomeSettingSection(index)">
              <template #icon><n-icon :component="CancelIcon" /></template>
            </n-button>
          </div>
        </template>
      </draggable>
      <template #footer>
        <n-space justify="space-between">
          <n-button secondary @click="addCenterHomeSettingSection">新增列表</n-button>
          <n-space>
            <n-button @click="showCenterHomeSettingsModal = false">取消</n-button>
            <n-button type="primary" :loading="sharedConfigSaving" @click="saveCenterHomeSettings">保存</n-button>
          </n-space>
        </n-space>
      </template>
    </n-modal>

    <n-modal v-model:show="showManualShareModal" preset="card" :title="manualShareModalTitle" style="width: 920px; max-width: 96vw;" class="custom-modal glass-modal">
      <n-alert v-if="activeCenterReplenishSource" type="success" :bordered="false" style="margin-bottom: 12px;">
        正在补充中心待补充资源：{{ appendYear(centerTitleText(activeCenterReplenishSource), activeCenterReplenishSource.release_year) }}。系统已按中心 SHA1 精确匹配本机完全相同资源，并自动填入下方手动共享表单；确认无误后点击“登记共享源”。
      </n-alert>
      <n-alert v-else-if="!activeLocalShareRequest" type="info" :bordered="false" style="margin-bottom: 12px;">
        直接输入片名搜索本地 media_metadata。登记时只上传本机可秒传的视频资产；中心端会根据分辨率、编码和杜比/HDR 自动归类、凑整季，不再由客户端做季包一致性判断。
      </n-alert>
      <n-alert v-if="activeLocalShareRequest" type="warning" :bordered="false" style="margin-bottom: 12px;">
        正在响应求共享：{{ appendYear(activeLocalShareRequest.title, activeLocalShareRequest.release_year) }} · {{ requestTargetText(activeLocalShareRequest) }}。系统会自动检索本地库并按求共享参数硬过滤，不符合画质/编码/HDR/帧率/音轨/字幕/体积的资源不会显示。没有候选就是本地没有符合条件的资源。
      </n-alert>

      <n-space v-if="!activeLocalShareRequest && !activeCenterReplenishSource" class="toolbar" :vertical="isMobile" :size="12">
        <n-input v-model:value="mediaSearchKeyword" placeholder="输入片名 / TMDb ID 搜索本地媒体库" clearable @keyup.enter="searchShareableMedia">
          <template #prefix><n-icon :component="SearchIcon" /></template>
        </n-input>
        <n-button type="primary" :loading="mediaSearchLoading" @click="searchShareableMedia">搜索</n-button>
      </n-space>

      <n-data-table
        size="small"
        :loading="mediaSearchLoading"
        :columns="mediaSearchColumns"
        :data="mediaCandidates"
        :pagination="{ pageSize: 8 }"
        :row-key="row => `${row.tmdb_id}-${row.item_type}-${row.season_number || ''}-${row.episode_number || ''}`"
        :scroll-x="980"
        style="margin-bottom: 14px;"
      />

      <div v-if="selectedMedia" class="selected-share-box">
        <div class="selected-title">已选择：{{ selectedMedia.display_title || selectedMedia.title }}</div>
        <div class="selected-desc">
          TMDb {{ manualShareForm.tmdb_id || '-' }} · {{ manualShareForm.item_type }} · {{ rapidShareTypeText(manualShareForm.share_type) }} ·
          115 {{ manualShareForm.root_is_dir ? '目录' : '文件' }}：{{ manualShareForm.root_name || manualShareForm.root_fid }}
        </div>
        <div class="selected-desc" v-if="selectedMedia.message">{{ selectedMedia.message }}</div>
        <n-alert
          v-if="manualShareValidationLoading || manualShareValidation"
          :type="manualShareValidationAlertType"
          :bordered="false"
          class="share-validation-alert"
        >
          <template #header>{{ manualShareValidationTitle }}</template>
          <div>{{ manualShareValidationMessage }}</div>
        </n-alert>
      </div>

      <template #footer>
        <n-space justify="space-between" align="center">
          <n-text depth="3">{{ activeCenterReplenishSource ? '补充会复用手动登记流程：直接上传秒传索引到中心；不会创建 115 分享/共享码，也不等待审核。' : '找不到候选时，先确认该媒体已入库且 media_metadata 中已有 PC/SHA1。' }}</n-text>
          <n-space>
            <n-button @click="showManualShareModal = false">取消</n-button>
            <n-button type="primary" :disabled="manualCreateDisabled" :loading="manualCreating" @click="manualCreateShare">登记共享源</n-button>
          </n-space>
        </n-space>
      </template>
    </n-modal>

    <n-modal v-model:show="showCenterDetailModal" preset="card" style="width: 1040px; max-width: 96vw;" class="custom-modal glass-modal center-detail-modal">
      <n-spin :show="centerDetailLoading">
        <div v-if="activeCenterDetailRow" class="center-detail-body">
          <!-- ★ 新增：图文并茂的头部信息区 -->
          <div class="center-detail-header-new">
            <img v-bind="centerPosterImgAttrs(activeCenterDetailRow, 'w500')" class="detail-poster" @error="onCenterPosterError" />
            <div class="detail-info">
              <div class="detail-title">
                {{ centerPosterWallPrimaryTitle(activeCenterDetailRow) }}
                <span class="detail-year" v-if="centerDisplayYear(activeCenterDetailRow)">({{ centerDisplayYear(activeCenterDetailRow) }})</span>
              </div>
              <div class="detail-meta">
                {{ centerCardMetaText(activeCenterDetailRow) }} · 包含 {{ centerDetailVersions.length }} 个版本
                <span v-if="centerDisplayGenres(activeCenterDetailRow)" class="detail-genres">
                  · {{ centerDisplayGenres(activeCenterDetailRow) }}
                </span>
                <span v-if="centerTmdbMeta(activeCenterDetailRow).vote_average" class="detail-rating">
                  ⭐ {{ Number(centerTmdbMeta(activeCenterDetailRow).vote_average).toFixed(1) }}
                </span>
              </div>
              <div class="detail-overview">
                {{ centerTmdbMeta(activeCenterDetailRow).overview || activeCenterDetailRow.overview || '暂无简介' }}
              </div>
              <div v-if="centerDetailPeople(activeCenterDetailRow).length" class="detail-credits">
                <div class="detail-people-row">
                  <div v-for="person in centerDetailPeople(activeCenterDetailRow)" :key="centerPersonKey(person)" class="detail-person-card" :title="centerPersonTooltip(person)">
                    <img v-bind="centerProfileImgAttrs(person)" class="detail-person-avatar" @error="onCenterProfileError" />
                    <div class="detail-person-info">
                      <div class="detail-person-name">{{ centerPersonName(person) }}</div>
                      <div class="detail-person-role">{{ centerPersonRoleText(person) }}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          <n-divider style="margin: 4px 0 12px 0;" />

          <!-- 版本列表 -->
          <div class="center-version-detail-list">
            <div v-if="centerDetailSeasonProgressVisible" class="center-season-progress">
              <div class="center-season-progress-head">
                <span>整体进度</span>
                <span>{{ centerDetailSeasonProgressText }}</span>
              </div>
              <n-progress
                class="center-season-progress-bar"
                type="line"
                :percentage="centerDetailSeasonProgressPercent"
                :show-indicator="false"
                :height="12"
                :border-radius="6"
                :color="centerTransferButtonColor"
                :style="{ '--season-progress-color': centerTransferButtonColor }"
                processing
              />
            </div>
            <div
              v-for="version in centerDetailVersions"
              :key="centerVersionKey(version)"
              class="center-version-detail-card"
              :class="{ 'center-version-detail-card-expandable': centerVersionCanExpandEpisodes(version) }"
              @click="toggleCenterVersionEpisodes(version)"
            >
              <div class="center-version-main">
                <div class="center-version-tags">
                  <n-tag v-for="tagItem in centerVersionTags(version, centerDetailProgressScope)" :key="tagItem.key" size="small" round :type="tagItem.type || 'default'" :bordered="false">
                    {{ tagItem.label }}
                  </n-tag>
                </div>
                <div v-if="centerVersionCanExpandEpisodes(version) && centerVersionEpisodesExpanded(version)" class="center-episode-matrix" @click.stop>
                  <n-button
                    v-for="episode in centerVersionEpisodeItems(version)"
                    :key="episode.key"
                    size="tiny"
                    round
                    secondary
                    :type="episode.asset ? 'primary' : 'default'"
                    :disabled="!episode.asset || Boolean(importingMap[episode.loadingKey])"
                    :loading="Boolean(importingMap[episode.loadingKey])"
                    @click="importCenterLogicalEpisode(version, episode)"
                  >{{ episode.label }}</n-button>
                </div>
              </div>
              <div class="center-version-action" @click.stop>
                <n-button
                  class="center-version-transfer-button"
                  size="small"
                  type="primary"
                  :color="centerTransferButtonColor"
                  text-color="#fff"
                  round
                  :loading="importingMap[version.source_id] === 'permanent'"
                  :disabled="centerVersionActionDisabled(version) || isCenterReplenishRow(version) || Boolean(importingMap[version.source_id])"
                  @click="importCenterSource(version, 'permanent')"
                >{{ centerTransferActionText(version) }}</n-button>
              </div>
            </div>
          </div>
        </div>
      </n-spin>
    </n-modal>

    <ShareRequestCreateModal
      v-model:show="showShareRequestModal"
      @created="handleShareRequestCreated"
    />
  </div>
</template>

<script setup>
import { computed, h, nextTick, onMounted, onUnmounted, reactive, ref, watch } from 'vue';
import axios from 'axios';
import draggable from 'vuedraggable';
import {
  NAlert, NButton, NCard, NDataTable, NDivider, NForm, NFormItem, NGi, NGrid, NIcon, NInput,
  NInputGroup, NInputNumber, NModal, NSelect, NSpace, NSpin, NSwitch,
  NTabPane, NTabs, NTag, NText, NTooltip, useDialog, useMessage, useThemeVars
} from 'naive-ui';
import {
  RefreshOutline as RefreshIcon,
  SearchOutline as SearchIcon,
  SyncOutline as SyncIcon,
  SettingsOutline as SettingsIcon,
  MenuOutline as MenuIcon,
  ShareSocialOutline as ShareIcon,
  CloseCircleOutline as CancelIcon
} from '@vicons/ionicons5';
import ShareRequestCreateModal from './ShareRequestCreateModal.vue';

const message = useMessage();
const dialog = useDialog();
const themeVars = useThemeVars();
const centerTransferButtonColor = computed(() => themeVars.value.primaryColor || '#e91e63');

const isMobile = ref(false);
const checkMobile = () => { isMobile.value = window.innerWidth <= 768; };

const activeTab = ref('shares');
const loading = ref(false);
const sharesLoading = ref(false);
const ledgerLoading = ref(false);
const centerLoading = ref(false);
const virtualLoading = ref(false);
const requestLoading = ref(false);
const shareRequestSearchLoading = ref(false);
const shareRequestSubmitting = ref(false);
const maintenanceSubmitting = ref(false);
const refreshingCredit = ref(false);
const registeringDevice = ref(false);
const shareAllLoading = ref(false);
const manualCreating = ref(false);
const showSharedConfigModal = ref(false);
const sharedConfigLoading = ref(false);
const sharedConfigSaving = ref(false);
const centerDeviceStatusLoading = ref(false);
const centerDeviceStatusError = ref('');
const centerDeviceStatusData = ref({});
const sharedConfigForm = reactive({
  p115_shared_resource_enabled: false,
  p115_shared_center_url: 'https://shared.55565576.xyz',
  p115_shared_resource_mode: 'rapid',
  p115_shared_disable_episode_transfer: false,
  p115_shared_block_clean_version_transfer: false,
  p115_shared_block_short_drama_transfer: false,
  p115_shared_intro_enabled: false,
  p115_shared_auto_share_requests_enabled: false,
  p115_shared_virtual_import_enabled: false,
  p115_shared_virtual_auto_promote_episodes: 0,
  p115_shared_virtual_auto_promote_movie_percent: 0,
  p115_shared_center_home_sections: [],
});
const showManualShareModal = ref(false);
const showShareRequestModal = ref(false);
const activeLocalShareRequest = ref(null);
const activeCenterReplenishSource = ref(null);
const mediaSearchKeyword = ref('');
const mediaSearchLoading = ref(false);
const mediaCandidates = ref([]);
const selectedMedia = ref(null);
const importingMap = reactive({});

const summary = ref({ shares: {}, credit: {} });
const shareItems = ref([]);
const ledgerItems = ref([]);
const centerSources = ref([]);
const virtualItems = ref([]);
const centerHomeSections = ref([]);
const CENTER_HOME_SECTION_DEFAULTS = [
  { key: 'latest', title: '最新资源', display_type: 'all', order_by: 'pool_time', genre_id: '', tags: [], limit: 10, enabled: true },
  { key: 'popular', title: '热门共享', display_type: 'all', order_by: 'popular', genre_id: '', tags: [], limit: 10, enabled: true },
  { key: 'movies', title: '电影', display_type: 'movie', order_by: 'pool_time', genre_id: '', tags: [], limit: 10, enabled: true },
  { key: 'series', title: '剧集', display_type: 'tv', order_by: 'pool_time', genre_id: '', tags: [], limit: 10, enabled: true },
];
const showCenterHomeSettingsModal = ref(false);
const centerHomeSettingSections = ref([]);
const centerHomeMovieGenres = ref([]);
const centerHomeTvGenres = ref([]);
const centerBackendGrouped = ref(false);
const centerExpandedRowKeys = ref([]);
const centerChildrenLoading = reactive({});
const centerVersionExpandedMap = reactive({});
const centerHasMore = ref(true);
const centerAppendLoading = ref(false);
const centerInfiniteSentinel = ref(null);
const showCenterDetailModal = ref(false);
const activeCenterDetailRow = ref(null);
const centerDetailLoading = ref(false);
const centerDetailActiveSeason = ref(null);
const shareRequests = ref([]);
const shareRequestSearchKeyword = ref('');
const shareRequestSearchItems = ref([]);
const selectedShareRequestMedia = ref(null);
const shareRequestQuote = ref(null);
const shareRequestEpisodeText = ref('');
const groupedCenterSources = computed(() => (
  centerBackendGrouped.value
    ? normalizeBackendCenterSources(centerSources.value || [])
    : groupCenterSources(centerSources.value || [], 'latest')
));
const shareFilters = reactive({ keyword: '', status: 'usable', order_by: 'created_desc' });
const centerFilters = reactive({ keyword: '' });
const virtualFilters = reactive({ keyword: '', status: 'virtual', item_type: 'all' });
const centerHomeMode = computed(() => !String(centerFilters.keyword || '').trim());
const requestFilters = reactive({ keyword: '', status: 'open', media_type: 'all', target_type: 'all' });
const requestStatusOptions = [
  { label: '求共享中', value: 'open' },
  { label: '全部状态', value: 'all' },
  { label: '已完成', value: 'fulfilled' },
  { label: '已过期', value: 'expired' },
  { label: '已取消', value: 'cancelled' },
];
const requestMediaTypeOptions = [
  { label: '全部媒体', value: 'all' },
  { label: '电影', value: 'movie' },
  { label: '剧集', value: 'tv' },
];
const requestTargetTypeFilterOptions = [
  { label: '全部目标', value: 'all' },
  { label: '电影', value: 'movie' },
  { label: '全剧', value: 'series' },
  { label: '单季', value: 'season' },
];
const sharePagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });
const centerPagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });
const virtualPagination = reactive({ page: 1, pageSize: 30, itemCount: 0, showSizePicker: true, pageSizes: [20, 30, 50, 100] });

const manualShareForm = reactive({
  root_fid: '', root_name: '', root_is_dir: true, title: '', tmdb_id: '', parent_series_tmdb_id: '',
  share_type: 'movie_folder', item_type: 'Movie', season_number: 1, release_year: null, receive_code: '',
  season_status: '', watching_status: '', expected_episode_count: null, total_episodes: null, is_completed: false,
  center_replenish_source_id: '', center_replenish_payload: null
});
const manualShareModalTitle = computed(() => activeCenterReplenishSource.value ? '补充中心待补充资源' : (activeLocalShareRequest.value ? '响应求共享' : '手动登记共享源'));
const manualShareValidation = ref(null);
const manualShareValidationLoading = ref(false);
let manualShareValidationSeq = 0;
const rapidShareTypeText = (value) => {
  const text = String(value || '').toLowerCase();
  if (text === 'movie_file' || text === 'movie_folder') return '电影源';
  if (text === 'season_pack') return '剧集资源';
  if (text === 'series_pack') return '全剧源';
  if (text === 'episode_file') return '分集资源';
  return value || '-';
};
const manualShareValidationAlertType = computed(() => {
  if (manualShareValidationLoading.value) return 'info';
  if (!manualShareValidation.value) return 'info';
  return manualShareValidation.value.valid ? 'success' : 'error';
});
const manualShareValidationTitle = computed(() => {
  if (manualShareValidationLoading.value) return '正在预校验共享资源';
  if (!manualShareValidation.value) return '';
  return manualShareValidation.value.valid ? '共享源预校验通过' : '共享源预校验未通过';
});
const manualShareValidationMessage = computed(() => {
  if (manualShareValidationLoading.value) return '正在读取 115 文件列表并检查 RAW/summary_json。';
  if (!manualShareValidation.value) return '';
  const fileCount = manualShareValidation.value.file_count;
  const prefix = fileCount ? `已定位 ${fileCount} 个视频文件。` : '';
  const msg = String(manualShareValidation.value.message || '').trim();
  const details = [];
  if (manualShareValidation.value.valid !== true) {
    const reasonCode = manualShareValidation.value.reason || '';
    if (reasonCode && !msg.includes(reasonCode) && !details.join('；').includes(reasonCode)) details.push(`原因代码：${reasonCode}`);
    const missingRaw = manualShareValidation.value.missing_raw || [];
    if (missingRaw.length) details.push(`RAW/summary_json 缺失 ${missingRaw.length} 个`);
  }
  return `${prefix}${[msg, ...details].filter(Boolean).join('；')}`.trim();
});
const manualCreateDisabled = computed(() => {
  if (!manualShareForm.root_fid) return true;
  if (manualShareValidationLoading.value) return true;
  if (!manualShareValidation.value) return true;
  return manualShareValidation.value.valid !== true;
});

const defaultShareRequestParams = () => ({
  resolution: null,
  codec: null,
  effect: null,
  frame_rate: null,
  audio: null,
  subtitle: null,
  size_range: '',
});
const shareRequestForm = reactive({
  tmdb_id: '',
  media_type: 'movie',
  target_type: 'movie',
  title: '',
  release_year: null,
  poster_path: '',
  overview: '',
  season_number: 1,
  episode_number: 1,
  params: defaultShareRequestParams(),
  expires_days: 7,
  auto_escalation: false,
  escalation_interval_hours: 24,
});

const shareStatusOptions = [
  { label: '有效共享', value: 'usable' },
  { label: '全部状态', value: 'all' },
  { label: '已上报中心', value: 'reported' },
  { label: '已有115分享', value: 'with_share' },
  { label: '分享可转存', value: 'share_valid' },
  { label: '分享待审核', value: 'share_pending' },
  { label: '无115分享', value: 'without_share' },
  { label: '本地未上报', value: 'local' },
  { label: '不合格/异常', value: 'failed' },
  { label: 'RAW缺失', value: 'raw_missing' },
  { label: '已停用', value: 'disabled' },
];
const virtualTypeOptions = [
  { label: '全部类型', value: 'all' },
  { label: '电影', value: 'movie' },
  { label: '剧集', value: 'tv' },
];

const typeOptions = [
  { label: '全部类型', value: 'all' }, { label: '电影', value: 'Movie' },
  { label: '剧集', value: 'Series' }, { label: '季', value: 'Season' }, { label: '单集', value: 'Episode' },
];
const manualItemTypeOptions = [
  { label: '电影', value: 'Movie' }, { label: '分集', value: 'Episode' }, { label: '剧集', value: 'Season' },
];
const shareTypeOptions = [
  { label: '电影目录', value: 'movie_folder' },
  { label: '剧集资源', value: 'season_pack' },
  { label: '分集资源', value: 'episode_file' },
];
const centerHomeDisplayTypeOptions = [
  { label: '全部', value: 'all' },
  { label: '电影', value: 'movie' },
  { label: '剧集', value: 'tv' },
];
const centerHomeOrderOptions = [
  { label: '入池时间', value: 'pool_time' },
  { label: '发行日期', value: 'release_year' },
  { label: '热门', value: 'popular' },
  { label: '体积', value: 'size' },
  { label: '名称', value: 'name' },
];
const CENTER_HOME_TAG_FALLBACK_OPTIONS = [
  { label: '已完结', value: 'completed_certified' },
  { label: '连载中', value: 'ongoing' },
  { label: '短剧', value: 'short_drama' },
  { label: '纯净版', value: 'clean_version' },
  { label: '片头', value: 'intro' },
  { label: '原盘', value: 'original_disc' },
  { label: '国语', value: 'mandarin_audio' },
  { label: '中字', value: 'chinese_subtitle' },
  { label: '特效', value: 'effect_subtitle' },
];
const centerHomeTagOptions = ref([...CENTER_HOME_TAG_FALLBACK_OPTIONS]);
const centerHomeLimitOptions = [6, 8, 10, 12, 16, 20].map(value => ({ label: `${value} 个`, value }));
const normalizeCenterHomeTagOptions = (items) => {
  const seen = new Set();
  return (Array.isArray(items) ? items : [])
    .map(item => ({
      label: String(item?.label || item?.name || '').trim(),
      value: String(item?.value || item?.key || '').trim(),
    }))
    .filter(item => {
      if (!item.label || !/^[A-Za-z0-9_:-]{1,40}$/.test(item.value) || seen.has(item.value)) return false;
      seen.add(item.value);
      return true;
    });
};
const loadCenterHomeTagOptions = async () => {
  try {
    const res = await axios.get('/api/shared/resources/center/sources/tags');
    const options = normalizeCenterHomeTagOptions(res.data?.items);
    centerHomeTagOptions.value = options.length ? options : [...CENTER_HOME_TAG_FALLBACK_OPTIONS];
  } catch (e) {
    centerHomeTagOptions.value = [...CENTER_HOME_TAG_FALLBACK_OPTIONS];
  }
};
const normalizeCenterHomeGenreOptions = (items) => (Array.isArray(items) ? items : [])
  .map(item => ({
    label: String(item?.name || item?.label || '').trim(),
    value: String(item?.id || item?.value || '').trim(),
  }))
  .filter(item => item.label && item.value);
const loadCenterHomeGenreOptions = async () => {
  if (centerHomeMovieGenres.value.length && centerHomeTvGenres.value.length) return;
  const [movieRes, tvRes] = await Promise.allSettled([
    axios.get('/api/custom_collections/config/tmdb_movie_genres'),
    axios.get('/api/custom_collections/config/tmdb_tv_genres'),
  ]);
  if (movieRes.status === 'fulfilled') centerHomeMovieGenres.value = normalizeCenterHomeGenreOptions(movieRes.value.data);
  if (tvRes.status === 'fulfilled') centerHomeTvGenres.value = normalizeCenterHomeGenreOptions(tvRes.value.data);
};
const centerHomeGenreOptions = (displayType) => {
  const type = String(displayType || '').toLowerCase();
  if (type === 'movie') return centerHomeMovieGenres.value;
  if (['tv', 'series', 'season', 'pack'].includes(type)) return centerHomeTvGenres.value;
  const seen = new Set();
  return [...centerHomeMovieGenres.value, ...centerHomeTvGenres.value].filter(item => {
    if (seen.has(item.value)) return false;
    seen.add(item.value);
    return true;
  });
};
const resourceTypeLabel = (value) => ({
  movie_file: '电影', movie_folder: '电影', Movie: '电影', movie: '电影', movies: '电影',
  season_pack: '剧集资源', series_pack: '全剧包', Season: '剧集', Series: '全剧包', season: '剧集', series: '全剧包', Pack: '剧集', pack: '剧集',
  episode_file: '分集资源', Episode: '分集', episode: '分集', episodes: '分集',
}[value] || value || '-');
const virtualImportTitle = (row = {}) => {
  const title = row.title || row.series_title || row.source_id || '-';
  const type = String(row.item_type || '').toLowerCase();
  const season = Number(row.season_number || 0);
  if (season > 0 && ['series', 'season', 'episode', 'tv', 'pack'].includes(type)) {
    return `${appendYear(title, row.release_year)} 第 ${season} 季`;
  }
  return appendYear(title, row.release_year);
};
const shareTypeLabel = (value) => resourceTypeLabel(value) || shareTypeOptions.find(opt => opt.value === value)?.label || value || '-';
const isSuccessShareMessage = (value) => {
  const text = String(value || '').trim();
  if (!text) return true;
  return /^(共享可用|共享可访问|共享正常|可访问|正常|ok)$/i.test(text);
};
const pickShareMetaText = (value) => {
  if (value == null) return '';
  if (typeof value === 'object') {
    return [
      value.source_provider, value.source_provider_label, value.source_label, value.provider, value.origin,
      value.share_source, value.share_origin, value.source_type, value.share_type, value.create_mode,
      value.created_by, value.creator_type, value.task_source, value.task_type, value.register_from,
      value.register_source, value.label, value.name, value.message, value.reason,
    ].map(pickShareMetaText).filter(Boolean).join(' ');
  }
  return String(value).trim();
};
const shareFailureReasonText = (row) => {
  const statusParts = [row?.status, row?.review_status, row?.center_status].map(v => String(v || '').toLowerCase()).filter(Boolean);
  const failedStatus = statusParts.find(v => ['failed', 'error', 'dead', 'expired', 'rejected', 'raw_missing', 'dirty_raw', 'dirty_summary', 'dirty_meta'].includes(v));
  const rawErrorText = [
    row?.last_error, row?.error, row?.error_message, row?.failure_reason, row?.fail_reason,
  ].map(v => String(v || '').trim()).find(v => v && !isSuccessShareMessage(v));

  const disabledStatus = statusParts.find(v => ['disabled', 'cancelled', 'canceled', 'deleted'].includes(v));
  // 维护任务留下的 last_error 不能污染仍然有效/已登记的共享源备注。
  if (rawErrorText && (failedStatus || disabledStatus)) {
    if (rawErrorText === 'source_file_sha1_not_in_library') return '本地文件不在媒体库';
    return rawErrorText;
  }

  if (failedStatus) {
    const rawReasonText = [row?.reason, row?.message, row?.status_message, row?.review_message]
      .map(v => String(v || '').trim()).find(v => v && !isSuccessShareMessage(v));
    if (rawReasonText) return rawReasonText;

    const label = statusMap[failedStatus]?.text || row?.status_label || row?.review_status_label || '共享失败';
    return label === '共享失败' ? label : `共享失败：${label}`;
  }

  return '';
};
const shareSourceText = (row) => {
  const raw = (row?.raw_json && typeof row.raw_json === 'object') ? row.raw_json : {};
  const providerText = pickShareMetaText([
    row?.source_provider,
    row?.share_source,
    row?.create_mode,
    raw?.source_provider,
    raw?.share_source,
    raw?.create_mode,
  ]).toLowerCase().replace(/[\s-]+/g, '_');
  const labelText = pickShareMetaText([
    row?.source_provider_label,
    row?.source_label,
    raw?.source_provider_label,
    raw?.source_label,
  ]).toLowerCase();

  const rawBackup = Boolean(raw?.auto_backup_share || raw?.backup_share || raw?.backup_instruction || raw?.backup_mirror || raw?.backup_fingerprint);
  const rawManual = Boolean(raw?.manual_payload || raw?.manual_share || raw?.manual_create || raw?.manual_created || raw?.manual_context);
  const rawAuto = Boolean(raw?.auto_gap || raw?.auto_payload || raw?.auto_task || raw?.maintenance_payload || raw?.maintenance_task || raw?.auto_share_payload || raw?.auto_context);
  const providerBackup = /(backup_mirror|backup_share|auto_backup_share|backup)/i.test(providerText) || /(备份共享|备份源|镜像共享)/.test(labelText);
  const providerAuto = /(rapid_auto_library|rapid_all_library|auto_gap_share|auto_share|auto_task|maintenance_task|maintenance_share|scheduler|scheduled_share|gap_share|watching_gap_share)/i.test(providerText) || /(自动共享|自动登记|入库自动|一键全库)/.test(labelText);
  const providerManual = /(user_share|manual_share|manual|local_manual|manual_create|manual_created|logical_season)/i.test(providerText) || /(手动共享|完结季源|完结季收藏)/.test(labelText);

  // 备份共享是中心下发的特殊来源，必须优先于自动/手动兜底判断。
  if (row?.is_backup_share || row?.backup_share || row?.auto_backup_share || rawBackup || providerBackup) return '备份共享';
  // Rapid v2 入库自动登记/一键全库优先于 manual 兜底；logical_season 只是完结季源类型，不等于自动共享。
  if (row?.is_auto_share || row?.auto_created || row?.created_by_task || row?.from_auto_task || row?.is_gap_share || row?.is_auto_created || row?.auto_share || row?.auto_registered || row?.from_maintenance || row?.created_from_maintenance || rawAuto || providerAuto) return '自动共享';
  if (row?.is_manual_share || row?.manual_created || row?.created_by_user || rawManual || providerManual) return '手动共享';

  return '本机共享';
};
const shareRemarkNode = (row) => {
  const reason = shareFailureReasonText(row);
  if (reason) {
    return h('span', { class: 'share-remark-text share-remark-error', title: reason }, reason);
  }
  const source = shareSourceText(row);
  const type = source === '自动共享' ? 'warning' : (source === '备份共享' ? 'info' : 'default');
  return h(NTag, { type, size: 'small', round: true }, { default: () => source });
};
const localLogicalShareChannel = (row) => row?.completed_share_channel || {};
const localShareChannelStatus = (row) => String(row?.share_channel_status || localLogicalShareChannel(row)?.status || 'none').toLowerCase();
const localShareChannelTag = (row) => {
  const status = localShareChannelStatus(row);
  const meta = {
    valid: ['可转存', 'success'],
    pending_review: ['待审核', 'warning'],
    creating: ['创建中', 'warning'],
    review_failed: ['审核失败', 'error'],
    expired: ['已失效', 'default'],
    import_failed: ['转存失败', 'error'],
    disabled: ['已取消', 'default'],
    source_unavailable: ['源不可用', 'error'],
    failed: ['失败', 'error'],
    none: ['无', 'default'],
  }[status] || [status || '无', 'default'];
  return h(NTag, { type: meta[1], size: 'small', round: true }, { default: () => meta[0] });
};
const canCancelLogicalShare = () => false;
const isAutoShareRow = (row) => shareSourceText(row) === '自动共享';
const normalizedShareStatuses = (row) => [
  row?.status,
  row?.review_status,
  row?.center_status,
].map(v => String(v || '').trim().toLowerCase()).filter(Boolean);
const shareInactiveStatuses = new Set(['disabled', 'cancelled', 'canceled', 'deleted']);
const shareProblemStatuses = new Set(['failed', 'error', 'dead', 'expired', 'rejected', 'raw_missing', 'dirty_raw', 'dirty_summary', 'dirty_meta', 'inconsistent', 'incomplete']);
const shareUsableStatuses = new Set(['active', 'available', 'alive', 'reported', 'partial', 'usable']);
const isInactiveShareRow = (row) => normalizedShareStatuses(row).some(v => shareInactiveStatuses.has(v));
const isProblemShareRow = (row) => normalizedShareStatuses(row).some(v => shareProblemStatuses.has(v));
const isEffectiveShareRow = (row) => {
  const statuses = normalizedShareStatuses(row);
  if (isInactiveShareRow(row) || isProblemShareRow(row)) return false;
  return statuses.some(v => shareUsableStatuses.has(v));
};
const deleteShareDisabledTitle = (row) => isEffectiveShareRow(row)
  ? '有效共享会先同步中心取消登记，再删除本地记录'
  : (isProblemShareRow(row) ? '删除异常/识别已变更的共享记录' : '删除本地共享记录');

const statusMap = {
  transferring: { text: '秒传中', type: 'warning' }, deleted: { text: '已删除', type: 'default' }, error: { text: '异常', type: 'error' },
  active: { text: '本地可用', type: 'success' }, available: { text: '可用', type: 'success' }, alive: { text: '可用', type: 'success' },
  pending: { text: '待验证', type: 'warning' }, replenish: { text: '待补充', type: 'error' }, dead: { text: '失效', type: 'error' }, expired: { text: '已过期', type: 'default' },
  reported: { text: '已登记', type: 'success' }, local: { text: '本地未登记', type: 'default' }, partial: { text: '部分登记', type: 'warning' },
  inconsistent: { text: '不一致', type: 'error' }, incomplete: { text: '不完整', type: 'warning' }, raw_missing: { text: '媒体信息缺失', type: 'error' },
  dirty_raw: { text: '识别已变更', type: 'warning' }, dirty_summary: { text: '摘要需重建', type: 'warning' }, dirty_meta: { text: '元数据已变更', type: 'warning' },
  disabled: { text: '已停用', type: 'default' },
  failed: { text: '失败', type: 'error' }, rejected: { text: '未通过', type: 'error' }, cancelled: { text: '已取消', type: 'default' },
  not_reported: { text: '未登记', type: 'default' },
  open: { text: '求共享中', type: 'success' }, fulfilled: { text: '已完成', type: 'success' },
};

const fmtBytes = (value) => {
  const n = Number(value || 0);
  if (!n) return '-';
  if (n >= 1024 ** 4) return `${(n / 1024 ** 4).toFixed(2)} TB`;
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${n} B`;
};
const fmtDate = (value) => { if (!value) return '-'; try { return new Date(value).toLocaleString(); } catch { return String(value); } };
const tag = (value) => { const meta = statusMap[value] || { text: value || '未知', type: 'default' }; return h(NTag, { type: meta.type, size: 'small', round: true }, { default: () => meta.text }); };

const appendYear = (title, year) => {
  const base = String(title || '').trim() || '-';
  const y = year ? String(year).trim() : '';
  if (!y || base === '-') return base;
  return new RegExp(`\\(${y}\\)\\s*$`).test(base) ? base : `${base} (${y})`;
};
const standardTitleText = (row, fallback = '') => appendYear(row?.title || row?.standard_title || row?.media_title || fallback || row?.file_name || row?.root_name || row?.tmdb_id, row?.release_year);
const tmdbIdForRow = (row) => String(row?.parent_series_tmdb_id || row?.share_tmdb_id || row?.tmdb_id || '').trim();
const tmdbMediaKind = (row) => {
  const type = String(centerRowTypeSafe(row) || row?.display_type || row?.item_type || row?.share_type || '').toLowerCase();
  if (type.includes('movie') || type === 'film' || type === '电影') return 'movie';
  return 'tv';
};
const centerRowTypeSafe = (row) => row?.display_type || (row?.source_kind === 'season_hub' ? 'Pack' : (row?.is_collapsed_pack || row?.pack_item_count ? 'Pack' : row?.item_type));
const tmdbHref = (row) => {
  const id = tmdbIdForRow(row);
  if (!id) return '';
  return `https://www.themoviedb.org/${tmdbMediaKind(row)}/${encodeURIComponent(id)}`;
};
const openTmdb = (row) => {
  const href = tmdbHref(row);
  if (!href) return;
  const win = window.open(href, '_blank', 'noopener,noreferrer');
  if (win) win.opener = null;
};
const tmdbLink = (row, labelPrefix = 'TMDb') => {
  const id = tmdbIdForRow(row);
  if (!id) return `${labelPrefix} -`;
  return h('span', {
    class: 'tmdb-pill',
    role: 'link',
    tabindex: 0,
    title: `打开 TMDb ${id}`,
    style: {
      '--tmdb-color': themeVars.value.primaryColor,
      '--tmdb-color-hover': themeVars.value.primaryColorHover || themeVars.value.primaryColor,
    },
    onClick: e => { e.stopPropagation(); openTmdb(row); },
    onKeydown: e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        e.stopPropagation();
        openTmdb(row);
      }
    },
  }, [
    h('span', { class: 'tmdb-pill-label' }, labelPrefix),
    h('span', { class: 'tmdb-pill-id' }, id),
  ]);
};

const centerCreatedTime = (row) => {
  const sortTs = Number(row?.sort_timestamp || 0);
  if (Number.isFinite(sortTs) && sortTs > 0) return sortTs * 1000;
  const t = new Date(row?.created_at || 0).getTime();
  return Number.isFinite(t) ? t : 0;
};
const metaLine = (row, parts = []) => h('div', { class: 'sub-title' }, [tmdbLink(row), ...parts.filter(Boolean)]);

const centerDeviceId = computed(() => String((summary.value.credit || {}).device_id || '').trim());
const hasCenterDevice = computed(() => Boolean(centerDeviceId.value));
const needsCenterServerId = computed(() => !centerConfigServerIdHash.value && !hasCenterDevice.value);
const centerDeviceRegisterButtonText = computed(() => centerDeviceId.value ? '重新连接' : '连接中心');
const centerServerIdAlertText = computed(() => {
  if (hasCenterDevice.value) {
    return '共享资源中心设备记录还在，但本机 ServerID 状态未确认。点击“重新连接”会使用 Emby ServerID 取回同一中心身份。';
  }
  return '共享资源中心尚未连接。点击“连接中心”后，系统会使用 Emby ServerID 注册中心身份。';
});

const firstFiniteNumber = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null || value === '') continue;
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
};

const centerResourceStats = computed(() => {
  const credit = summary.value.credit || {};
  const rawStats = credit?.raw_json?.stats || {};
  const mediaStats = credit.media_stats || rawStats.media_stats || {};
  const movieCount = firstFiniteNumber(rawStats.movie_sources, credit.display_movie_count, credit.center_movie_count, mediaStats.movie_count, rawStats.display_movie_count);
  const seriesCount = firstFiniteNumber(
    credit.display_series_count,
    credit.center_series_count,
    mediaStats.series_count,
    rawStats.display_series_count,
    rawStats.series_count,
    mediaStats.season_count,
    rawStats.display_season_count,
    rawStats.logical_season_groups,
    rawStats.season_groups,
    0
  );
  const videoCount = firstFiniteNumber(
    credit.video_count,
    mediaStats.video_count,
    rawStats.video_count,
    firstFiniteNumber(rawStats.movie_sources, 0) + firstFiniteNumber(rawStats.episode_sources, 0)
  );
  return { movieCount, seriesCount, videoCount };
});


const proQuotaStats = computed(() => {
  const credit = summary.value.credit || {};
  const rawStats = credit?.raw_json?.stats || {};
  const quota = credit.pro_quota || rawStats.pro_quota || credit?.raw_json?.me?.pro_quota || {};
  const n = (value, fallback = 0) => {
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  };
  const tier = String(quota.pro_tier || quota.tier || '').trim().toUpperCase();
  const label = quota.pro_label || ({ M: '月卡', Y: '年卡', L: '终身' }[tier] || tier);
  return {
    active: Boolean(quota.pro_active || quota.pro_valid),
    usable: Boolean(quota.quota_usable),
    tier,
    label,
    dailyGrant: n(quota.daily_grant),
    balance: n(quota.quota_balance ?? quota.balance),
    cap: n(quota.balance_cap ?? quota.cap),
    expireTime: quota.pro_expire_time || '',
    lastGrantDate: quota.last_grant_date || '',
    lastVerifiedAt: quota.last_verified_at || '',
  };
});

const creditCardDesc = computed(() => {
  const quota = proQuotaStats.value;
  if (!quota.active || !quota.tier) return '普通贡献点';
  const grant = quota.dailyGrant ? `今日 +${quota.dailyGrant}` : '今日未发放';
  const capText = quota.cap ? `${quota.balance}/${quota.cap}` : `${quota.balance}`;
  return `Pro ${quota.label || quota.tier} ${grant}，累计 ${capText}`;
});

const centerDeviceStats = computed(() => {
  const credit = summary.value.credit || {};
  const rawStats = credit?.raw_json?.stats || {};
  const total = firstFiniteNumber(rawStats.devices, credit.remote_devices, 0);
  const online = Math.min(firstFiniteNumber(rawStats.online_devices, 0), total || Number.MAX_SAFE_INTEGER);
  return { online, total };
});

const localShareSeriesCount = computed(() => {
  const shares = summary.value.shares || {};
  return firstFiniteNumber(
    shares.alive_series_count,
    shares.series_count,
    shares.alive_series,
    0
  );
});

const localShareSeasonCount = computed(() => {
  const shares = summary.value.shares || {};
  return firstFiniteNumber(
    shares.alive_season_count,
    shares.season_count,
    shares.display_season_count,
    shares.alive_series_count,
    shares.series_count,
    0
  );
});

const activeShareRequestCount = computed(() => {
  const credit = summary.value.credit || {};
  const rawStats = credit?.raw_json?.stats || {};
  return firstFiniteNumber(
    credit.share_requests,
    credit.active_share_requests,
    credit.wanted_gaps,
    rawStats.active_share_requests,
    rawStats.wanted_gaps,
    rawStats.active_gap_devices,
    Array.isArray(shareRequests.value) ? shareRequests.value.length : 0
  );
});

const statCards = computed(() => {
  const shares = summary.value.shares || {};
  const centerStats = centerResourceStats.value;
  const deviceStats = centerDeviceStats.value;
  return [
    { key: 'credit', label: '贡献点', value: summary.value.credit?.credit ?? 0, desc: creditCardDesc.value },
    { key: 'devices', label: '在线设备', value: `${deviceStats.online}/${deviceStats.total}`, desc: '30 分钟内有心跳的设备' },
    {
      key: 'shares',
      label: '我的共享',
      value: `电影 ${shares.alive_movies ?? 0} · 剧 ${localShareSeriesCount.value} · 季 ${localShareSeasonCount.value}`,
      desc: `共计视频 ${shares.alive_videos ?? 0} 个`,
    },
    { key: 'remote_sources', label: '中心资源', value: `电影 ${centerStats.movieCount} · 季 ${centerStats.seriesCount}`, desc: `共计视频 ${centerStats.videoCount} 个` },
    { key: 'share_requests', label: '求共享', value: activeShareRequestCount.value, desc: '活跃求共享需求' },
  ];
});

const shareColumns = [
  { title: '标题', key: 'title', minWidth: 280, render: row => {
    const seasonText = row.season_number ? ` · S${String(row.season_number).padStart(2, '0')}` : '';
    const episodeText = row.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : '';
    const aggregateText = Number(row.aggregated_source_count || 0) > 1 ? ` · ${row.aggregated_source_count} 个源` : '';
    return h('div', [
      h('div', { class: 'main-title' }, standardTitleText(row, row.root_name || row.file_name || row.title)),
      metaLine(row, [` · ${shareTypeLabel(row.share_type)}`, seasonText, episodeText, aggregateText])
    ]);
  } },
  { title: '中心', key: 'center_status', width: 110, render: row => tag(row.center_status) },
  { title: '文件数', key: 'item_count', width: 90, render: row => `${row.reported_count || 0}/${row.item_count || 0}` },
  { title: '媒体信息', key: 'raw_uploaded_count', width: 110, render: row => {
    const missingSize = Number(row.size_missing_count || 0);
    const text = `${row.raw_uploaded_count || 0}/${row.item_count || 0}`;
    return h('div', [
      h('div', text),
      missingSize > 0 ? h('div', { class: 'sub-title warning-text' }, `缺大小 ${missingSize}`) : null
    ]);
  } },
  { title: '115分享', key: 'share_channel_status', width: 105, render: row => localShareChannelTag(row) },
  { title: '创建时间', key: 'created_at', width: 170, render: row => fmtDate(row.created_at) },
  { title: '备注', key: 'share_remark', minWidth: 220, ellipsis: { tooltip: true }, render: row => shareRemarkNode(row) },
  { title: '操作', key: 'actions', width: 300, fixed: 'right', render: row => h(NSpace, { size: 8, align: 'center', wrap: false }, { default: () => [
    h(NButton, {
      size: 'small',
      type: 'primary',
      secondary: true,
      title: '重新上传 RAW/summary_json，并重新登记中心',
      onClick: () => reregisterShare(row),
    }, { icon: () => h(NIcon, null, { default: () => h(ShareIcon) }), default: () => '重新登记' }),
    h(NButton, {
      size: 'small',
      type: 'warning',
      secondary: true,
      disabled: !canCancelLogicalShare(row),
      title: canCancelLogicalShare(row) ? '逻辑季分享由中心端维护' : '逻辑季分享由中心端维护',
      onClick: () => cancelLogicalSeasonShare(row),
    }, { icon: () => h(NIcon, null, { default: () => h(CancelIcon) }), default: () => '取消分享' }),
    h(NButton, {
      size: 'small',
      type: 'error',
      ghost: true,
      disabled: row.status === 'cancelled' || row.status === 'disabled' || isAutoShareRow(row),
      title: isAutoShareRow(row) ? '自动共享源由入库自动维护，不能手动停用' : '',
      onClick: () => cancelShare(row),
    }, { icon: () => h(NIcon, null, { default: () => h(CancelIcon) }), default: () => '停用' }),
    h(NButton, {
      size: 'small',
      type: 'error',
      secondary: true,
      title: deleteShareDisabledTitle(row),
      onClick: () => deleteShare(row),
    }, { icon: () => h(NIcon, null, { default: () => h(CancelIcon) }), default: () => '删除' }),
  ]}) },
];

const virtualColumns = [
  { title: '标题', key: 'title', minWidth: 280, render: row => h('div', [
    h('div', { class: 'main-title' }, virtualImportTitle(row)),
    h('div', { class: 'sub-title' }, [
      resourceTypeLabel(row.item_type),
      row.tmdb_id ? ` · TMDb ${row.tmdb_id}` : '',
    ].filter(Boolean).join(''))
  ]) },
  { title: '文件', key: 'file_count', width: 90, render: row => `${row.file_count || 0} 个` },
  { title: '观看', key: 'watched_count', width: 120, render: row => `${row.watched_count || 0} 次 / ${Math.round(Number(row.played_percent || 0))}%` },
  { title: '创建时间', key: 'created_at', width: 170, render: row => fmtDate(row.created_at) },
  { title: '操作', key: 'actions', width: 170, fixed: 'right', render: row => h(NSpace, { size: 8, align: 'center', wrap: false }, { default: () => [
    h(NButton, {
      size: 'small',
      type: 'primary',
      secondary: true,
      disabled: row.status === 'promoted',
      onClick: () => promoteVirtualImport(row),
    }, { default: () => '转正' }),
    h(NButton, {
      size: 'small',
      type: 'error',
      ghost: true,
      onClick: () => dismissVirtualImport(row),
    }, { default: () => '辞退' }),
  ]}) },
];

const mediaSearchColumns = [
  { title: '媒体', key: 'display_title', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, appendYear(row.display_title || row.standard_title || row.title || row.tmdb_id, row.release_year)),
    metaLine(row, [` · ${resourceTypeLabel(row.item_type)}`])
  ]) },
  { title: '入库', key: 'in_library', width: 80, render: row => h(NTag, { size: 'small', type: row.in_library ? 'success' : 'default' }, { default: () => row.in_library ? '已入库' : '未入库' }) },
  { title: '可共享根目录/文件', key: 'root_name', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, row.root_name || '-'),
    h('div', { class: 'sub-title' }, row.root_fid ? `FID/CID: ${row.root_fid}` : (row.message || '未定位'))
  ]) },
  { title: '文件', key: 'file_count', width: 100, render: row => `${row.file_count || 0} 个` },
  { title: '源类型', key: 'share_type', width: 120, render: row => shareTypeLabel(row.share_type) },
  { title: '说明', key: 'message', minWidth: 220, ellipsis: { tooltip: true } },
  { title: '操作', key: 'actions', width: 100, fixed: 'right', render: row => h(NButton, {
    size: 'small', type: 'primary', ghost: true, disabled: !row.resolvable || !row.root_fid, onClick: () => chooseMediaCandidate(row)
  }, { default: () => row.resolvable ? '选择' : '不可用' }) },
];


const shareRequestTargetOptions = computed(() => {
  if (shareRequestForm.media_type === 'movie') return [{ label: '电影', value: 'movie' }];
  return [
    { label: '全剧', value: 'series' },
    { label: '单季', value: 'season' },
    ];
});

const requestStatusLabel = (status) => statusMap[status]?.text || status || '未知';
const requestParticipationText = (row = {}) => {
  if (row.my_role === 'owner') return '我发起的求共享';
  if (row.joined_by_me) return '我已同求';
  return '别人发布的求共享';
};
const canProvideShareRequest = (row = {}) => row.status === 'open' && row.my_role !== 'owner';
const requestTargetTypeLabel = (value) => ({
  movie: '电影', series: '全剧', season: '单季', episode: '单季', episode_batch: '单季',
}[String(value || '').toLowerCase()] || value || '-');
const requestTargetText = (row) => {
  const target = String(row?.target_type || '').toLowerCase();
  const season = row?.season_number ? `S${String(row.season_number).padStart(2, '0')}` : '';
  const episode = row?.episode_number ? `E${String(row.episode_number).padStart(2, '0')}` : '';
  if (target === 'season') return `${requestTargetTypeLabel(target)} ${season || ''}`.trim();
  if (target === 'episode') return `${requestTargetTypeLabel(target)} ${season}${episode}`.trim();
  if (target === 'episode_batch') return `${requestTargetTypeLabel(target)} ${season || ''}`.trim();
  return requestTargetTypeLabel(target || row?.media_type);
};
const requestConditionText = (row) => {
  const params = (row?.params_json && typeof row.params_json === 'object') ? row.params_json : {};
  const parts = [params.resolution, params.codec, params.effect, params.frame_rate ? `${params.frame_rate}fps` : '', params.audio, params.subtitle, params.size_range].filter(Boolean);
  return parts.length ? parts.join(' · ') : '不限参数';
};
const parseShareRequestJsonObject = (value) => {
  if (value && typeof value === 'object') return value;
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  }
  return {};
};
const requestSeasonCountValue = (row) => {
  const price = parseShareRequestJsonObject(row?.price_breakdown);
  const value = row?.season_count || row?.number_of_seasons || price.season_count;
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : '';
};
const shareRequestSearchFilterParams = (row) => {
  if (!row) return {};
  const paramsJson = row.params_json && typeof row.params_json === 'object' ? row.params_json : {};
  const eps = Array.isArray(row.episode_numbers) ? row.episode_numbers : [];
  return {
    request_tmdb_id: row.tmdb_id || '',
    request_media_type: row.media_type || '',
    request_target_type: row.target_type || '',
    request_season_number: row.season_number || '',
    request_season_count: requestSeasonCountValue(row),
    request_episode_number: row.episode_number || '',
    request_episode_numbers_json: JSON.stringify(eps),
    request_params_json: JSON.stringify(paramsJson),
  };
};
const requestPosterUrl = (row) => {
  const p = String(row?.poster_path || '').trim();
  if (!p) return '/default-poster.png';
  if (/^https?:\/\//i.test(p)) return p;
  return `https://image.tmdb.org/t/p/w185${p}`;
};
const onRequestPosterError = (event) => { if (event?.target) event.target.src = '/default-poster.png'; };

const shareRequestSearchColumns = [
  { title: '媒体', key: 'title', minWidth: 260, render: row => h('div', [
    h('div', { class: 'main-title' }, appendYear(row.title, row.release_year)),
    h('div', { class: 'sub-title' }, `${row.media_type === 'movie' ? '电影' : '剧集'} · TMDb ${row.tmdb_id || '-'}`)
  ]) },
  { title: '首播/上映', key: 'release_date', width: 120, render: row => row.release_date || row.release_year || '-' },
  { title: '简介', key: 'overview', minWidth: 320, ellipsis: { tooltip: true } },
  { title: '操作', key: 'actions', width: 100, fixed: 'right', render: row => h(NButton, {
    size: 'small', type: 'primary', ghost: true, onClick: () => chooseShareRequestMedia(row)
  }, { default: () => '选择' }) },
];

const ledgerEventLabel = (eventType) => {
  const text = String(eventType || '').trim();
  const map = {
    center_initial_credit: '基础贡献点',
    center_source_registered: '中心登记共享源',
    center_source_registered_group: '中心登记共享源',
    center_backup_source_registered: '备份共享入池',
    center_backup_source_registered_group: '备份共享入池',
    center_deleted_shared_source_summary: '已删除共享源',
    center_shared_source_served: '共享被秒传',
    center_shared_source_served_group: '共享被秒传',
    rapid_source_served: '共享视频被秒传',
    center_shared_source_consumed: '秒传共享资源',
    center_shared_source_consumed_group: '秒传共享资源',
    rapid_source_consumed: '秒传共享视频',
    center_rapid_source_registered: '中心登记秒传源',
    center_rapid_source_registered_group: '中心登记秒传源',
    center_rapid_source_served: '共享资源被秒传',
    center_rapid_source_served_group: '共享资源被秒传',
    center_rapid_source_consumed: '秒传共享资源',
    center_rapid_source_consumed_group: '秒传共享资源',
    center_daily_grant: 'Pro每日赠送额度',
    center_rapid_quota_consumed: 'Pro额度抵扣',
    center_tier_cap_adjust: 'Pro等级上限调整',
    center_pro_expired_clear: 'Pro过期清空额度',
    center_pro_inactive_clear: 'Pro认证失效清空额度',
    center_rapid_sign_success: '秒传签名成功',
    center_rapid_sign_failed: '秒传签名失败',
    center_rapid_sign_timeout: '秒传签名超时',
    center_rapid_sign_job_success: '秒传签名成功',
    center_rapid_sign_job_failed: '秒传签名失败',
    center_rapid_raw_uploaded: '上传媒体信息',
    center_rapid_raw_ffprobe_uploaded: '上传媒体信息',
    share_created: '登记共享源',
    share_reported_center: '登记中心',
    share_raw_uploaded: '上传媒体信息',
    share_cancelled: '取消共享',
    share_request_escrow: '求共享冻结',
    share_request_refund: '求共享退款',
    share_request_bounty_paid: '求共享悬赏支付',
    share_request_bounty_received: '求共享悬赏收入',
    share_request_service_fee: '求共享服务费',
    center_share_request_escrow: '求共享冻结',
    center_share_request_refund: '求共享退款',
    center_share_request_bounty_paid: '求共享悬赏支付',
    center_share_request_bounty_received: '求共享悬赏收入',
    center_share_request_service_fee: '求共享服务费',
  };
  if (map[text]) return map[text];
  const low = text.toLowerCase();
  if (low.includes('rapid') && low.includes('sign')) {
    if (low.includes('fail') || low.includes('error')) return '秒传签名失败';
    if (low.includes('timeout')) return '秒传签名超时';
    return '秒传签名';
  }
  if (low.includes('rapid') && low.includes('consume')) return '秒传共享资源';
  if (low.includes('rapid') && low.includes('serv')) return '共享资源被秒传';
  if (low.includes('source') && low.includes('register')) return '登记共享源';
  return text || '-';
};

const formatDelta = (value) => {
  const n = Number(value || 0);
  return n > 0 ? `+${n}` : String(n);
};

const ledgerReasonCodeLabel = (value) => ({
  rapid_sign_success: '响应中心秒传签名成功',
  rapid_sign_failed: '响应中心秒传签名失败',
  rapid_sign_timeout: '响应中心秒传签名超时',
  rapid_source_consumed: '从共享中心秒传资源',
  rapid_source_served: '本机共享资源被他人秒传',
  source_registered: '共享资源登记入池',
  backup_source_registered: '备份共享入池',
  shared_source_served: '共享资源被他人秒传',
  shared_source_consumed: '从共享中心秒传资源',
  daily_grant: 'Pro每日赠送额度',
  rapid_quota_consumed: 'Pro额度抵扣',
  tier_cap_adjust: 'Pro等级上限调整',
  pro_expired_clear: 'Pro过期清空额度',
  pro_inactive_clear: 'Pro认证失效清空额度',
  share_request_escrow: '求共享冻结贡献点',
  share_request_refund: '求共享退回贡献点',
  share_request_bounty_paid: '求共享悬赏支付',
  share_request_bounty_received: '求共享悬赏收入',
  share_request_service_fee: '求共享服务费',
}[value] || '');

const shortLedgerHash = (value, length = 12) => {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.length > length ? `${text.slice(0, length)}...` : text;
};

const looksLikeShareRequestId = (value) => /^srq_[0-9a-f]/i.test(String(value || '').trim());
const ledgerCode = (row) => String(row?.event_type || row?.reason || '').trim().toLowerCase();
const ledgerReasonCode = (row) => String(row?.reason || row?.event_type || '').trim().toLowerCase().replace(/^center_/, '');
const isLedgerSignRow = (row) => ledgerCode(row).includes('rapid_sign') || ledgerReasonCode(row).startsWith('rapid_sign');
const isLedgerConsumedRow = (row) => ledgerCode(row).includes('rapid_source_consumed') || ledgerReasonCode(row) === 'rapid_source_consumed' || ledgerCode(row).includes('shared_source_consumed') || ledgerReasonCode(row) === 'shared_source_consumed';
const isLedgerServedRow = (row) => ledgerCode(row).includes('rapid_source_served') || ledgerReasonCode(row) === 'rapid_source_served' || ledgerCode(row).includes('shared_source_served') || ledgerReasonCode(row) === 'shared_source_served';
const isLedgerProQuotaRow = (row) => {
  const code = ledgerCode(row);
  const reason = ledgerReasonCode(row);
  const ledgerType = String(row?.ledger_type || '').trim().toLowerCase();
  return ledgerType === 'pro_quota' || ['daily_grant', 'rapid_quota_consumed', 'tier_cap_adjust', 'pro_expired_clear', 'pro_inactive_clear'].includes(reason) || ['center_daily_grant', 'center_rapid_quota_consumed', 'center_tier_cap_adjust', 'center_pro_expired_clear', 'center_pro_inactive_clear'].includes(code);
};
const ledgerSha1 = (row = {}) => {
  const raw = ledgerRawJson(row);
  const center = (raw.center_ledger && typeof raw.center_ledger === 'object') ? raw.center_ledger : {};
  const values = [
    row?.ledger_sha1, row?.sha1, row?.ref_id, row?.source_id,
    center.sha1, center.ref_id, center.source_id,
    raw.sha1, raw.file_sha1, raw.ref_id, raw.source_id,
  ];
  for (const value of values) {
    const match = String(value || '').match(/[A-Fa-f0-9]{40}/);
    if (match) return match[0].toUpperCase();
  }
  return '';
};
const ledgerSxx = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 ? `S${String(Math.trunc(n)).padStart(2, '0')}` : '';
};
const ledgerExx = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 ? `E${String(Math.trunc(n)).padStart(2, '0')}` : '';
};
const ledgerContext = (row = {}) => {
  const raw = ledgerRawJson(row);
  const center = (raw.center_ledger && typeof raw.center_ledger === 'object') ? raw.center_ledger : {};
  const source = (raw.source && typeof raw.source === 'object') ? raw.source : {};
  const sharedSource = (raw.shared_source && typeof raw.shared_source === 'object') ? raw.shared_source : {};
  const media = (raw.media && typeof raw.media === 'object') ? raw.media : {};
  const job = (raw.job && typeof raw.job === 'object') ? raw.job : {};
  const first = (...values) => values.find(v => v !== undefined && v !== null && String(v).trim() !== '');
  return {
    title: first(center.series_title, row.series_title, media.series_title, source.series_title, sharedSource.series_title, job.series_title, raw.series_title, center.title, row.title, media.title, source.title, sharedSource.title, job.title, raw.title, center.file_name, row.file_name, raw.file_name),
    series_title: first(center.series_title, row.series_title, media.series_title, source.series_title, sharedSource.series_title, job.series_title, raw.series_title),
    file_name: first(center.file_name, row.file_name, media.file_name, source.file_name, sharedSource.file_name, job.file_name, raw.file_name),
    tmdb_id: first(center.tmdb_id, row.tmdb_id, media.tmdb_id, source.tmdb_id, sharedSource.tmdb_id, raw.tmdb_id),
    item_type: first(center.item_type, row.item_type, media.item_type, source.item_type, sharedSource.item_type, raw.item_type),
    source_kind: first(center.source_kind, row.source_kind, source.source_kind, sharedSource.source_kind, raw.source_kind),
    season_number: first(center.season_number, row.season_number, media.season_number, source.season_number, sharedSource.season_number, raw.season_number),
    episode_number: first(center.episode_number, row.episode_number, media.episode_number, source.episode_number, sharedSource.episode_number, raw.episode_number),
    sha1: first(row.sha1, row.ledger_sha1, center.sha1, raw.sha1, raw.file_sha1, source.sha1, sharedSource.sha1, media.sha1),
  };
};
const cleanLedgerTitleText = (value) => {
  const text = String(value || '').trim();
  if (!text || looksLikeShareRequestId(text)) return '';
  if (/^(?:rapid_sign:)?[A-Fa-f0-9]{40}(?::.*)?$/.test(text)) return '';
  if (text.toLowerCase().startsWith('rapid_sign:')) return '';
  return text;
};
const cleanLedgerBaseTitle = (base, row) => {
  let text = cleanLedgerTitleText(base);
  if (!text) return '';
  const ctx = ledgerContext(row);
  const sxx = ledgerSxx(ctx.season_number);
  const exx = ledgerExx(ctx.episode_number);
  if (sxx && exx) {
    text = text
      .replace(new RegExp(`\\s*${sxx}\\s*${exx}\\s*$`, 'i'), '')
      .replace(new RegExp(`\\s*${sxx}${exx}\\s*$`, 'i'), '');
  }
  if (sxx) text = text.replace(new RegExp(`\\s*${sxx}\\s*$`, 'i'), '');
  text = text.replace(/\s+/g, ' ').replace(/[\s\-·._]+$/g, '').trim();
  return text || cleanLedgerTitleText(base);
};
const appendLedgerSeasonEpisode = (base, row, { aggregate = false } = {}) => {
  base = cleanLedgerBaseTitle(base, row);
  if (!base) return '';
  const ctx = ledgerContext(row);
  const sxx = ledgerSxx(ctx.season_number);
  const exx = ledgerExx(ctx.episode_number);
  if (aggregate && isLedgerConsumedRow(row) && sxx) return `${base} ${sxx}`;
  if (isLedgerSignRow(row) && sxx && exx) return `${base} ${sxx}${exx}`;
  if ((String(ctx.item_type || '').toLowerCase() === 'episode' || String(ctx.source_kind || '').toLowerCase() === 'episode') && sxx && exx) return `${base} ${sxx}${exx}`;
  if ((String(ctx.item_type || '').toLowerCase() === 'season' || String(ctx.source_kind || '').toLowerCase() === 'logical_season') && sxx) return `${base} ${sxx}`;
  return base;
};
const ledgerCreditText = (row, rows = null) => {
  const delta = rows ? rows.reduce((sum, item) => sum + Number(item?.delta || 0), 0) : Number(row?.delta || 0);
  const count = rows ? rows.length : 0;
  const proRows = rows ? rows.every(item => isLedgerProQuotaRow(item)) : isLedgerProQuotaRow(row);
  const unitName = proRows ? 'Pro额度' : '贡献点';
  if (rows && count > 1 && rows.every(item => Number(item?.delta || 0) === Number(rows[0]?.delta || 0))) {
    const unit = Number(rows[0]?.delta || 0);
    return `${unitName} ${formatDelta(unit)}*${count}`;
  }
  if (proRows && Math.abs(delta) > 1 && ledgerReasonCode(row) === 'rapid_quota_consumed') {
    return `${unitName} ${delta > 0 ? '+1' : '-1'}*${Math.abs(delta)}`;
  }
  if (!proRows && (isLedgerConsumedRow(row) || isLedgerServedRow(row) || isLedgerSignRow(row)) && Math.abs(delta) > 1) {
    return `${unitName} ${delta > 0 ? '+1' : '-1'}*${Math.abs(delta)}`;
  }
  return `${unitName} ${formatDelta(delta)}`;
};
const usableLedgerTitle = (value) => {
  const text = String(value || '').trim();
  return text && text !== '-' ? text : '';
};
const ledgerDisplayTitle = (row) => {
  if (row?.title_display) {
    const fixed = usableLedgerTitle(appendLedgerSeasonEpisode(row.title_display, row, { aggregate: Boolean(row.__ledger_aggregated && isLedgerConsumedRow(row)) }));
    if (fixed && !fixed.startsWith('秒传签名：') && !/^未知资源\s+[A-Fa-f0-9]/.test(fixed)) return fixed;
  }
  if (row?.ledger_aggregate_title && row.__ledger_aggregated) {
    const aggregateTitle = usableLedgerTitle(row.ledger_aggregate_title);
    if (aggregateTitle) return aggregateTitle;
  }
  const ctx = ledgerContext(row);
  const title = usableLedgerTitle(appendLedgerSeasonEpisode(ctx.title || ctx.file_name, row, { aggregate: Boolean(row.__ledger_aggregated && isLedgerConsumedRow(row)) }));
  if (title && !title.startsWith('秒传签名：') && !/^未知资源\s+[A-Fa-f0-9]/.test(title)) return title;
  const raw = ledgerRawJson(row);
  const event = String(row?.event_type || '').toLowerCase();
  if (isLedgerProQuotaRow(row)) {
    const tier = String(row?.pro_tier || ledgerRawJson(row)?.center_ledger?.pro_tier || '').trim().toUpperCase();
    const label = ({ M: '月卡', Y: '年卡', L: '终身' }[tier] || tier || '');
    if (event.includes('daily_grant')) return label ? `Pro${label}` : 'Pro每日赠送额度';
    if (event.includes('quota_consumed')) return title || 'Pro额度抵扣';
    return label ? `Pro${label}` : 'Pro额度';
  }
  if (event.includes('share_request')) return '求共享';
  if (isLedgerSignRow(row)) {
    const sha = ctx.sha1 || raw.sha1 || raw.file_sha1 || raw.sign_check || '';
    return sha ? `秒传签名：${shortLedgerHash(sha)}` : '秒传签名任务';
  }
  return usableLedgerTitle(row?.title) || '-';
};

const ledgerReasonDisplay = (row) => {
  if (row?.reason_display) return row.reason_display;
  const event = String(row?.event_type || '');
  const reason = String(row?.reason || '').trim();
  const deltaText = ledgerCreditText(row);
  const title = ledgerDisplayTitle(row);
  const reasonMap = {
    share_request_escrow: `求共享冻结：${title}，${deltaText}`,
    center_share_request_escrow: `求共享冻结：${title}，${deltaText}`,
    share_request_refund: `求共享退款：${title}，${deltaText}`,
    center_share_request_refund: `求共享退款：${title}，${deltaText}`,
    share_request_bounty_paid: `求共享悬赏支付：${title}，${deltaText}`,
    center_share_request_bounty_paid: `求共享悬赏支付：${title}，${deltaText}`,
    share_request_bounty_received: `求共享悬赏收入：${title}，${deltaText}`,
    center_share_request_bounty_received: `求共享悬赏收入：${title}，${deltaText}`,
    share_request_service_fee: `求共享服务费：${title}，${deltaText}`,
    center_share_request_service_fee: `求共享服务费：${title}，${deltaText}`,
    center_backup_source_registered: `备份共享入池：${title}，${deltaText}`,
    center_backup_source_registered_group: `备份共享入池：${title}，${deltaText}`,
    center_rapid_sign_success: `响应中心秒传签名成功：${title}，${deltaText}`,
    center_rapid_sign_job_success: `响应中心秒传签名成功：${title}，${deltaText}`,
    center_rapid_sign_failed: `响应中心秒传签名失败：${title}，${deltaText}`,
    center_rapid_sign_job_failed: `响应中心秒传签名失败：${title}，${deltaText}`,
    center_rapid_source_consumed: `从共享中心秒传资源：${title}，${deltaText}`,
    center_rapid_source_consumed_group: `从共享中心秒传资源：${title}，${deltaText}`,
    center_rapid_source_served: `本机共享资源被他人秒传：${title}，${deltaText}`,
    center_rapid_source_served_group: `本机共享资源被他人秒传：${title}，${deltaText}`,
    center_rapid_source_registered: `共享资源登记入池：${title}，${deltaText}`,
    center_rapid_source_registered_group: `共享资源登记入池：${title}，${deltaText}`,
    center_daily_grant: `Pro每日赠送额度：${title}，${deltaText}`,
    center_rapid_quota_consumed: `Pro额度抵扣：${title}，${deltaText}`,
    center_tier_cap_adjust: `Pro等级上限调整：${title}，${deltaText}`,
    center_pro_expired_clear: `Pro过期清空额度：${title}，${deltaText}`,
    center_pro_inactive_clear: `Pro认证失效清空额度：${title}，${deltaText}`,
  };
  if (reasonMap[event]) {
    const balance = row?.balance_after ?? ledgerRawJson(row)?.center_ledger?.balance_after;
    return isLedgerProQuotaRow(row) && balance !== undefined && balance !== null && balance !== '' ? `${reasonMap[event]}，余额 ${balance}` : reasonMap[event];
  }
  const reasonLabel = ledgerReasonCodeLabel(reason) || ledgerReasonCodeLabel(event.replace(/^center_/, ''));
  if (reasonLabel) {
    const balance = row?.balance_after ?? ledgerRawJson(row)?.center_ledger?.balance_after;
    const suffix = isLedgerProQuotaRow(row) && balance !== undefined && balance !== null && balance !== '' ? `，余额 ${balance}` : '';
    return `${reasonLabel}：${title}，${deltaText}${suffix}`;
  }
  return reason || `${ledgerEventLabel(event)}：${title}，${deltaText}`;
};

const isDeletedCenterSourceLedgerRow = (row) => {
  const eventType = String(row?.event_type || '');
  const title = String(row?.title || '').trim();
  return (
    title === '已删除共享源' &&
    (eventType === 'center_shared_source_served' || eventType === 'center_shared_source_consumed')
  );
};

const buildDeletedCenterSourceSummaryRow = (rows) => {
  if (!rows.length) return null;
  const sorted = [...rows].sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime());
  const latest = sorted[0] || {};
  const delta = rows.reduce((sum, row) => sum + Number(row?.delta || 0), 0);
  const servedCount = rows.filter(row => row?.event_type === 'center_shared_source_served').length;
  const consumedCount = rows.filter(row => row?.event_type === 'center_shared_source_consumed').length;
  return {
    ...latest,
    id: `deleted-center-source-summary:${latest.created_at || '0'}`,
    event_type: 'center_deleted_shared_source_summary',
    title: `已删除共享源（汇总 ${rows.length} 条）`,
    delta,
    reason: `已汇总展示 ${rows.length} 条历史共享源积分变化；共享被秒传 ${servedCount} 条，秒传共享资源 ${consumedCount} 条。`,
    raw_json: {
      ...(latest.raw_json || {}),
      deleted_source_summary: {
        item_count: rows.length,
        served_count: servedCount,
        consumed_count: consumedCount,
        delta,
      },
    },
  };
};


const normalizeLedgerKeyPart = (value) => String(value ?? '').trim().replace(/\s+/g, ' ');
const ledgerTimeValue = (row) => {
  const t = new Date(row?.created_at || 0).getTime();
  return Number.isFinite(t) ? t : 0;
};
const ledgerRawJson = (row) => (row?.raw_json && typeof row.raw_json === 'object') ? row.raw_json : {};
const ledgerFileKey = (row) => {
  const raw = ledgerRawJson(row);
  const source = (raw.source && typeof raw.source === 'object') ? raw.source : {};
  const sharedSource = (raw.shared_source && typeof raw.shared_source === 'object') ? raw.shared_source : {};
  const media = (raw.media && typeof raw.media === 'object') ? raw.media : {};
  const candidates = [
    row?.source_id, row?.shared_source_id, row?.center_source_id,
    raw.source_id, raw.shared_source_id, raw.center_source_id, raw.shared_source_key,
    raw.sha1, raw.file_sha1, raw.pc, raw.pick_code, raw.file_id, raw.fid, raw.cid, raw.root_fid,
    source.source_id, source.sha1, source.file_sha1, source.pc, source.pick_code, source.file_id, source.fid, source.cid,
    sharedSource.source_id, sharedSource.sha1, sharedSource.file_sha1, sharedSource.pc, sharedSource.pick_code, sharedSource.file_id, sharedSource.fid, sharedSource.cid,
    media.sha1, media.file_sha1, media.pc, media.pick_code, media.file_id, media.fid, media.cid,
  ].map(normalizeLedgerKeyPart).filter(Boolean);
  if (candidates.length) return candidates[0];
  return normalizeLedgerKeyPart(row?.title || raw.title || raw.file_name || raw.name);
};
const shouldAggregateLedgerRow = (row) => {
  const eventType = normalizeLedgerKeyPart(row?.event_type);
  const title = normalizeLedgerKeyPart(row?.title || ledgerRawJson(row).title || ledgerRawJson(row).file_name || ledgerRawJson(row).name);
  const fileKey = ledgerFileKey(row);
  return Boolean(
    eventType &&
    fileKey &&
    title &&
    !eventType.endsWith('_group') &&
    eventType !== 'center_deleted_shared_source_summary' &&
    !isDeletedCenterSourceLedgerRow(row)
  );
};
const ledgerSeasonAggregateKey = (row) => {
  const ctx = ledgerContext(row);
  const tmdb = normalizeLedgerKeyPart(ctx.tmdb_id);
  const season = ledgerSxx(ctx.season_number) || normalizeLedgerKeyPart(ctx.season_number);
  if (tmdb && season) return `${tmdb}:${season}`;
  return ledgerFileKey(row);
};
const ledgerAggregateKey = (row) => {
  const event = normalizeLedgerKeyPart(row?.event_type || row?.reason);
  if (isLedgerSignRow(row)) {
    const sha1 = ledgerSha1(row);
    if (sha1) return `${event}::sign:sha1:${sha1}`;
  }
  if (row?.ledger_aggregate_key) return `${event}::${row.ledger_aggregate_key}`;
  if (isLedgerConsumedRow(row)) return `${event}::consume-season::${ledgerSeasonAggregateKey(row)}`;
  return `${event}::${ledgerFileKey(row)}`;
};
const ledgerAggregateTitle = (latest, rows) => {
  if (latest?.ledger_aggregate_title && isLedgerConsumedRow(latest)) return latest.ledger_aggregate_title;
  if (isLedgerConsumedRow(latest)) return appendLedgerSeasonEpisode(ledgerContext(latest).title || ledgerContext(latest).file_name, latest, { aggregate: true }) || ledgerDisplayTitle(latest);
  return ledgerDisplayTitle(latest);
};
const buildAggregatedLedgerReason = (latest, rows, totalDelta, titleOverride = '') => {
  const creditText = ledgerCreditText(latest, rows);
  const title = titleOverride || ledgerDisplayTitle(latest) || '-';
  if (isLedgerSignRow(latest)) return `响应中心秒传签名成功：${title}，${creditText}`;
  if (isLedgerConsumedRow(latest)) return `从共享中心秒传资源：${title}，${creditText}`;
  if (isLedgerServedRow(latest)) return `本机共享资源被他人秒传：${title}，${creditText}`;
  const reason = String(latest?.reason_display || latest?.reason || '').trim();
  if (reason) {
    const replaced = reason.replace(/贡献点\s*[+-]?\d+(?:\.\d+)?(?:\*\d+)?(?=\s*[，,。；;、]?$)/, creditText);
    if (replaced !== reason) return replaced;
    return `${reason}，${creditText}`;
  }
  return `${ledgerEventLabel(latest?.event_type)}：${title}，${creditText}`;
};
const buildAggregatedLedgerRow = (rows, index) => {
  const sorted = [...rows].sort((a, b) => ledgerTimeValue(b) - ledgerTimeValue(a));
  const latest = sorted[0] || {};
  const delta = rows.reduce((sum, row) => sum + Number(row?.delta || 0), 0);
  const aggregateTitle = ledgerAggregateTitle(latest, rows);
  const aggregateReason = buildAggregatedLedgerReason(latest, rows, delta, aggregateTitle);
  return {
    ...latest,
    id: `ledger-aggregate:${ledgerAggregateKey(latest)}:${index}`,
    created_at: latest.created_at,
    delta,
    title_display: aggregateTitle,
    ledger_aggregate_title: aggregateTitle,
    reason: aggregateReason,
    reason_display: aggregateReason,
    __ledger_aggregated: true,
    __ledger_count: rows.length,
    __ledger_records: sorted,
  };
};
const aggregateLedgerRows = (rows) => {
  const groups = new Map();
  const passthrough = [];
  rows.forEach((row) => {
    if (!shouldAggregateLedgerRow(row)) {
      passthrough.push(row);
      return;
    }
    const key = ledgerAggregateKey(row);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  });
  const aggregated = [];
  let index = 0;
  groups.forEach((items) => {
    if (items.length > 1) aggregated.push(buildAggregatedLedgerRow(items, index++));
    else passthrough.push(items[0]);
  });
  return [...passthrough, ...aggregated];
};

const ledgerDisplayItems = computed(() => {
  const rows = Array.isArray(ledgerItems.value) ? ledgerItems.value : [];
  const deletedRows = rows.filter(isDeletedCenterSourceLedgerRow);
  const normalRows = rows.filter(row => !isDeletedCenterSourceLedgerRow(row));
  const summaryRow = buildDeletedCenterSourceSummaryRow(deletedRows);
  const merged = summaryRow ? [...aggregateLedgerRows(normalRows), summaryRow] : aggregateLedgerRows(normalRows);
  merged.sort((a, b) => ledgerTimeValue(b) - ledgerTimeValue(a));
  return merged.map((row, index) => ({
    ...row,
    __row_key: row.__ledger_aggregated
      ? row.id
      : `row:${row.id || row.ref_id || row.created_at || index}`,
  }));
});

const ledgerTooltipContent = (row) => h('div', { class: 'ledger-detail-tooltip' }, [
  h('div', { class: 'ledger-detail-title' }, `已聚合 ${row.__ledger_count || 0} 条详细记录`),
  ...(row.__ledger_records || []).map((item, index) => h('div', { class: 'ledger-detail-item' }, [
    h('div', { class: 'ledger-detail-meta' }, `${index + 1}. ${fmtDate(item.created_at)} ｜ ${item.event_label || ledgerEventLabel(item.event_type)} ｜ ${formatDelta(item.delta)}`),
    h('div', { class: 'ledger-detail-reason' }, ledgerReasonDisplay(item)),
  ])),
]);
const withLedgerTooltip = (row, node, extraClass = '') => {
  if (!row?.__ledger_aggregated) return node;
  return h(NTooltip, { trigger: 'hover', placement: 'top-start', style: { maxWidth: '760px' } }, {
    trigger: () => h('span', { class: ['ledger-tooltip-trigger', extraClass].filter(Boolean).join(' ') }, [node]),
    default: () => ledgerTooltipContent(row),
  });
};


const centerTypeLabel = (value) => ({
  Movie: '电影', movie: '电影', movies: '电影', movie_file: '电影', movie_folder: '电影',
  Pack: '季', pack: '季', Season: '季', season: '季', Series: '剧集', series: '剧集', tv: '剧集', season_pack: '季', series_pack: '剧集',
  Episode: '单集', episode: '单集', episodes: '单集', episode_file: '单集',
}[value] || value || '-');
const centerRowType = centerRowTypeSafe;
const centerSeasonNumber = (row) => {
  const raw = row?.season_number;
  if (raw === undefined || raw === null || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
};
const centerIsSeriesGroup = () => false;
const centerIsSeasonLike = (row) => {
  if (centerIsSeriesGroup(row)) return false;
  const label = centerTypeLabel(centerRowType(row));
  const kind = String(row?.source_kind || row?.lazy_children_kind || '').trim().toLowerCase();
  const type = String(row?.item_type || row?.display_type || '').trim().toLowerCase();
  return label === '季' || kind === 'season_hub' || kind === 'logical_season' || ['season', 'pack'].includes(type);
};
const centerIsSpecialSeason = (row) => centerSeasonNumber(row) === 0 && centerIsSeasonLike(row);
const appendCenterSpecialSeasonSuffix = (title) => {
  const text = String(title || '').trim();
  if (!text) return '特别篇';
  if (/(特别篇|特别季|特别节目|番外|SP|Specials?|Special)$/i.test(text)) return text;
  return `${text} 特别篇`;
};
const centerSeasonBaseTitle = (title, row = {}) => {
  const text = stripCenterSeasonFromTitle(title, row);
  return centerIsSpecialSeason(row) ? appendCenterSpecialSeasonSuffix(text) : text;
};
const stripCenterSeasonFromTitle = (title, row = {}) => {
  let text = String(title || '').trim();
  if (!text) return '';
  const season = Number(row?.season_number || 0);
  if (season > 0) {
    const seasonText = String(season);
    const season02 = seasonText.padStart(2, '0');
    const patterns = [
      new RegExp(`\\s*(?:[-·—–_]+\\s*)?S0?${seasonText}E\\d{1,3}\\s*$`, 'i'),
      new RegExp(`\\s*(?:[-·—–_]+\\s*)?S${season02}E\\d{1,3}\\s*$`, 'i'),
      new RegExp(`\\s*(?:[-·—–_]+\\s*)?(?:S0?${seasonText}|S${season02})\\s*$`, 'i'),
      new RegExp(`\\s*(?:[-·—–_]+\\s*)?Season\\s*0?${seasonText}\\s*$`, 'i'),
      new RegExp(`\\s*(?:[-·—–_]+\\s*)?第\\s*0?${seasonText}\\s*季\\s*$`, 'i'),
    ];
    for (const pattern of patterns) text = text.replace(pattern, '').trim();
  }
  // 兜底清理中心端历史标题里混入的季号；季号统一放到“类型”列显示。
  text = text
    .replace(/\s*(?:[-·—–_]+\s*)?S\d{1,3}E\d{1,3}\s*第\s*\d{1,3}\s*季\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?第\s*\d{1,3}\s*季\s*S\d{1,3}E\d{1,3}\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?S\d{1,3}E\d{1,3}\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?第\s*\d{1,3}\s*集\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?S\d{1,3}\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?Season\s*\d{1,3}\s*$/i, '')
    .replace(/\s*(?:[-·—–_]+\s*)?第\s*\d{1,3}\s*季\s*$/i, '')
    .trim();
  return text || String(title || '').trim();
};
const centerTitleText = (row) => {
  const rawTitle = row?.title || row?.standard_title || row?.media_title || row?.root_name || row?.file_name || row?.tmdb_id || '';
  return appendYear(centerSeasonBaseTitle(rawTitle, row), row?.release_year);
};
const centerSeriesSeasonRows = (row) => Array.isArray(row?.seasons) ? row.seasons.filter(x => x && typeof x === 'object') : [];
const centerSeasonIsSpecialNumber = (row) => centerSeasonTabNumber(row) === 0;
const centerStatusText = (row) => String(
  row?.center_ribbon_text
  || row?.ribbon_text
  || row?.season_status_label
  || row?.display_status_label
  || row?.status_label
  || ''
).trim();
const centerStatusCode = (row) => String(
  row?.season_resource_status
  || row?.season_status
  || row?.display_status
  || row?.watching_status
  || ''
).trim().toLowerCase();
const centerSeasonResourceStatus = (row) => {
  const code = centerStatusCode(row);
  if (['consistent', 'completed', 'ongoing'].includes(code)) return code;
  const label = centerStatusText(row);
  if (/一致版/.test(label)) return 'consistent';
  if (/已完结|完结/.test(label)) return 'completed';
  if (/缺集|未完整/.test(label)) return 'missing';
  if (/连载中|更新中|未完结/.test(label)) return 'ongoing';
  return '';
};
const centerIsCompletedByServer = (row) => {
  if (!row || typeof row !== 'object' || centerSeasonIsSpecialNumber(row)) return false;
  const resourceStatus = centerSeasonResourceStatus(row);
  if (resourceStatus === 'consistent' || resourceStatus === 'completed') return true;
  if (resourceStatus === 'ongoing') return false;
  const label = centerStatusText(row);
  const code = centerStatusCode(row);
  return Boolean(
    row.is_completed
    || row.is_completed_certified
    || code === 'completed'
    || code === 'ended'
    || /已完结|完结|一致版/.test(label)
  );
};
const centerIsOngoingHub = (row) => {
  if (!row || typeof row !== 'object' || centerIsSpecialSeason(row)) return false;
  const resourceStatus = centerSeasonResourceStatus(row);
  if (resourceStatus === 'ongoing') return true;
  if (resourceStatus === 'consistent' || resourceStatus === 'completed') return false;
  if (centerIsCompletedByServer(row)) return false;
  const label = centerStatusText(row);
  const code = centerStatusCode(row);
  return Boolean(
    row.is_ongoing_series
    || row.is_ongoing_hub
    || code === 'ongoing'
    || code === 'airing'
    || /连载中|更新中|未完结/.test(label)
  );
};
const centerPositiveNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? num : 0;
};
const centerProgressParts = (row) => {
  const text = String(row?.progress_text || '').trim();
  const match = text.match(/(\d+)\s*\/\s*(\d+)/);
  const textCurrent = match ? centerPositiveNumber(match[1]) : 0;
  const textTotal = match ? centerPositiveNumber(match[2]) : 0;
  const current = centerPositiveNumber(row?.progress_current || row?.pack_item_count || row?.file_count) || textCurrent;
  const total = centerPositiveNumber(row?.progress_total || row?.expected_episode_count || row?.episode_total || row?.total_episodes || row?.episode_count || row?.logical_group?.episode_total) || textTotal || current;
  return { current, total };
};
const centerProgressText = (row, scope = null) => {
  const parts = centerProgressParts(row);
  const current = centerPositiveNumber(scope?.current) || parts.current;
  const total = centerPositiveNumber(scope?.total) || parts.total;
  if (current > 0 && total > 0) return `${current}/${total}`;
  return current > 0 ? String(current) : '';
};
const centerCleanVersionMeta = (row) => {
  const meta = row?.clean_version_meta_json || row?.clean_version_meta || row?.version_summary?.clean_version_meta_json || {};
  return meta && typeof meta === 'object' ? meta : {};
};
const isCenterCleanVersion = (row) => Boolean(row?.is_clean_version || centerCleanVersionMeta(row).is_clean_version);
const centerNestedParts = (row) => {
  const parts = [];
  if (row && typeof row === 'object') parts.push(row);
  for (const key of ['version_summary', 'summary_json', 'media_signature_json', 'raw_summary_json', 'rapid_meta_json', 'clean_version_meta_json', 'short_drama_meta_json', 'animation_meta_json', 'completed_certified_meta_json']) {
    const v = row?.[key];
    if (v && typeof v === 'object') parts.push(v);
  }
  for (const key of ['children', 'pack_items', 'versions']) {
    if (Array.isArray(row?.[key])) {
      row[key].forEach(x => {
        if (x && typeof x === 'object') {
          parts.push(x);
          if (x.version_summary && typeof x.version_summary === 'object') parts.push(x.version_summary);
          if (x.summary_json && typeof x.summary_json === 'object') parts.push(x.summary_json);
          if (x.media_signature_json && typeof x.media_signature_json === 'object') parts.push(x.media_signature_json);
          if (x.rapid_meta_json && typeof x.rapid_meta_json === 'object') parts.push(x.rapid_meta_json);
          if (x.animation_meta_json && typeof x.animation_meta_json === 'object') parts.push(x.animation_meta_json);
          if (x.completed_certified_meta_json && typeof x.completed_certified_meta_json === 'object') parts.push(x.completed_certified_meta_json);
        }
      });
    }
  }
  const bestAssetMap = row?.best_asset_map;
  if (bestAssetMap && typeof bestAssetMap === 'object' && !Array.isArray(bestAssetMap)) {
    Object.values(bestAssetMap).forEach(x => {
      if (x && typeof x === 'object') parts.push(x);
    });
  }
  return parts;
};
const centerCompletedCertifiedMeta = (row) => {
  for (const part of centerNestedParts(row)) {
    const meta = part?.completed_certified_meta_json || part?.completed_certified_meta || {};
    if (part?.is_completed_certified || meta?.is_completed_certified) {
      return meta && typeof meta === 'object' ? { ...meta, is_completed_certified: true } : { is_completed_certified: true };
    }
  }
  return {};
};
const isCenterCompletedCertified = (row) => Boolean(centerCompletedCertifiedMeta(row).is_completed_certified);
const centerAnimationMeta = (row) => {
  for (const part of centerNestedParts(row)) {
    const meta = part?.animation_meta_json || part?.animation_meta || {};
    if (part?.is_animation || meta?.is_animation || part?.genres_json_contains_animation) return meta && typeof meta === 'object' ? meta : { is_animation: true };
  }
  return {};
};
const isCenterAnimation = (row) => Boolean(centerAnimationMeta(row).is_animation || row?.is_animation);
const centerShortDramaMeta = (row) => {
  for (const part of centerNestedParts(row)) {
    const meta = part?.short_drama_meta_json || part?.short_drama_meta || {};
    if (part?.is_short_drama || meta?.is_short_drama) return meta && typeof meta === 'object' ? meta : { is_short_drama: true };
  }
  return {};
};
const isCenterShortDrama = (row) => Boolean(centerShortDramaMeta(row).is_short_drama || row?.is_short_drama);
const isCenterOriginalDisc = (row) => Boolean(row?.is_original_disc || centerNestedParts(row).some(part => /\.iso(?:$|[\s?#])/i.test(String(part?.file_name || part?.filename || part?.name || ''))));
const centerShareChannel = (row) => row?.share_channel
  || row?.logical_season_share_channel
 
  || row?.logical_group?.share_channel
  || row?.logical_group?.logical_season_share_channel
 
  || {};
const centerLooksLogicalGroupId = (value) => /^(svg_|lsg_|logical_season_)/i.test(String(value || '').trim());
const centerLogicalGroupId = (row) => {
  const logicalGroup = row?.logical_group && typeof row.logical_group === 'object' ? row.logical_group : {};
  for (const value of [row?.logical_group_id, row?.group_id, logicalGroup?.group_id, logicalGroup?.source_id, row?.logical_season_group_id]) {
    const text = String(value || '').trim();
    if (text) return text;
  }
  for (const value of [row?.source_id, row?.source_ref_id, row?.center_source_id]) {
    const text = String(value || '').trim();
    if (centerLooksLogicalGroupId(text)) return text;
  }
  return '';
};
const centerIsLogicalSeasonRow = (row) => {
  const kind = String(row?.source_kind || row?.resource_type || '').trim().toLowerCase();
  if (kind === 'logical_season') return true;
  const groupId = centerLogicalGroupId(row);
  if (!groupId) return false;
  return Boolean(
    row?.logical_shadow_only
    || row?.logical_import_available
    || row?.logical_pool_complete
    || row?.pool_complete
    || row?.logical_group_id
    || row?.group_id
    || row?.logical_group
    || row?.best_asset_map
    || false
  );
};
const centerIsLogicalShadowOnly = (row) => centerIsLogicalSeasonRow(row) && !row?.logical_import_available;
const centerHasLogicalGroup = (row) => Boolean(centerLogicalGroupId(row) || row?.pool_complete || row?.logical_pool_complete);
const centerHasValidShareChannel = (row) => {
  const status = String(centerShareChannel(row)?.status || row?.share_channel_status || row?.logical_group?.share_channel_status || '').toLowerCase();
  return Boolean(
    row?.share_transfer_available
    || row?.has_valid_share_channel
    || row?.preferred_transfer_mode === 'share'
    || row?.transfer_mode === 'share'
    || row?.logical_group?.share_transfer_available
    || row?.logical_group?.has_valid_share_channel
    || row?.logical_group?.preferred_transfer_mode === 'share'
    || row?.logical_group?.transfer_mode === 'share'
    || status === 'valid'
  );
};
const centerTransferActionText = (row) => sharedConfigForm.p115_shared_virtual_import_enabled ? '入库' : (centerHasValidShareChannel(row) ? '转存' : '秒传');
const centerVersionActionDisabled = (row) => false;
const formatCenterSize = (row) => {
  // 修复：优先使用外层的 size，对于季包来说，外层 size 是 SQL SUM 出来的整包总大小
  const size = Number(row.size || 0);
  if (size > 0) return `${(size / 1024 / 1024 / 1024).toFixed(2)} GB`;
  
  // 兜底：如果外层没有，再尝试使用媒体信息里的单集大小
  const gb = Number(centerVersionSummary(row)?.size_gb || row.version_summary?.size_gb || 0);
  if (gb > 0) return `${gb.toFixed(gb >= 10 ? 1 : 2)} GB`;
  
  return '-';
};
const isCenterReplenishRow = (row) => String(row?.status || '').trim().toLowerCase() === 'replenish';

const inferRapidSourceKind = (row) => {
  const direct = String(row?.source_kind || row?.kind || '').trim().toLowerCase();
  if (centerIsLogicalSeasonRow(row)) return 'logical_season';
  if (direct) return direct;
  const typeText = centerTypeLabel(centerRowType(row));
  if (typeText === '电影') return 'movie';
  if (typeText === '单集') return 'episode';
  if (typeText === '季') return row?.source_kind === 'season_hub' ? 'season_hub' : 'logical_season';
  return '';
};

const centerRapidSourceId = (row) => {
  if (centerIsLogicalSeasonRow(row)) {
    const groupId = centerLogicalGroupId(row);
    if (groupId) return groupId;
  }
  return String(row?.source_id || row?.source_ref_id || row?.episode_source_id || row?.center_source_id || '').trim();
};

const buildCenterImportSourcePayload = (row) => {
  const sourceId = centerRapidSourceId(row);
  const sourceKind = inferRapidSourceKind(row);
  const childSourceIds = (row?.children || row?.pack_items || [])
    .map(x => centerRapidSourceId(x) || x?.source_id || x?.source_ref_id)
    .filter(Boolean);
  return {
    ...row,
    source_kind: sourceKind,
    source_id: sourceId,
    source_ids: sourceKind === 'season_hub' ? childSourceIds : undefined,
    source_ref_id: sourceId,
    logical_group_id: sourceKind === 'logical_season' ? sourceId : (row?.logical_group_id || ''),
    title: row?.title || '',
    tmdb_id: row?.tmdb_id || row?.share_tmdb_id || '',
    parent_series_tmdb_id: row?.parent_series_tmdb_id || row?.series_tmdb_id || '',
    item_type: row?.item_type || row?.share_item_type || centerRowType(row) || '',
    display_type: centerRowType(row) || '',
    season_number: row?.season_number ?? null,
    episode_number: row?.episode_number ?? null,
    year: row?.release_year || '',
    share_type: row?.share_type || '',
    status: row?.status || '',
  };
};

const centerDeviceForcedOffline = (device) => device?.forced_offline === true || String(device?.forced_offline || '').toLowerCase() === 'true';
const centerDeviceStatusTagType = (status) => {
  const value = String(status || '').toLowerCase();
  if (value === 'banned') return 'error';
  if (value === 'active') return 'success';
  if (value === 'offline') return 'warning';
  return 'default';
};
const centerConfigDeviceStatus = computed(() => centerDeviceStatusData.value?.device || {});
const centerConfigLocalServerHash = computed(() => String(centerDeviceStatusData.value?.local_server_id_hash || '').trim());
const centerConfigServerIdHash = computed(() => String(centerConfigDeviceStatus.value?.server_id_hash || centerConfigLocalServerHash.value || '').trim());
const centerConfigForcedOfflineReason = computed(() => {
  const device = centerConfigDeviceStatus.value || {};
  if (!centerDeviceForcedOffline(device)) return '';
  return String(device.ban_reason || device.forced_offline_reason || '').trim() || '未填写';
});
const centerConfigDeviceStatusLabel = computed(() => {
  const device = centerConfigDeviceStatus.value || {};
  if (centerDeviceForcedOffline(device)) return '已封禁/强制离线';
  return statusMap[String(device.status || '').toLowerCase()]?.text || device.status || '未知';
});

const executeImport = async (row, mode) => {
  const modeText = centerTransferActionText(row);
  if (isCenterReplenishRow(row)) {
    message.warning(`该资源处于待补充状态，不能${modeText}`);
    return;
  }
  let importRow = row;
  const originalKey = centerTableRowKey(row);
  if (centerNeedsLoadChildren(row)) {
    const loadedChildren = await loadCenterSourceChildren(row);
    await nextTick();
    const latest = findCenterGroupByKey(groupedCenterSources.value || [], originalKey);
    importRow = latest || (loadedChildren ? {
      ...row,
      children: loadedChildren.children || [],
      pack_items: loadedChildren.pack_items || loadedChildren.children || [],
      children_loaded: true,
      _center_children_loaded: true,
    } : row);
  }
  const sourcePayload = buildCenterImportSourcePayload(importRow);
  if (!sourcePayload.source_kind || !sourcePayload.source_id) {
    message.error('中心源缺少 source_kind/source_id，刷新中心资源库后重试。');
    return;
  }
  const loadingKey = sourcePayload.source_id || row?.group_key;
  importingMap[loadingKey] = mode;
  try {
    const requestBody = {
      mode,
      source: sourcePayload,
      context: sourcePayload,
    };
    if (Array.isArray(sourcePayload.source_ids) && sourcePayload.source_ids.length) {
      requestBody.source_ids = sourcePayload.source_ids;
    } else {
      requestBody.source_id = sourcePayload.source_id;
    }
    const res = await axios.post('/api/shared/resources/center/import', requestBody);
    message.success(res.data?.message || '已提交');
    await Promise.allSettled([loadCenterSources(), loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || `${modeText}失败`);
  } finally {
    delete importingMap[loadingKey];
  }
};

const importCenterSource = (row, mode) => {
  const modeText = centerTransferActionText(row);
  if (isCenterReplenishRow(row)) {
    message.warning(`该资源处于待补充状态，不能${modeText}`);
    return;
  }
  dialog.info({
    title: modeText,
    content: `确定将中心资源《${centerTitleText(row)}》执行${modeText}吗？`,
    positiveText: modeText,
    negativeText: '取消',
    // 注意：这里去掉了 async，让函数同步返回，这样弹窗会立刻关闭，不卡界面
    onPositiveClick: () => {
      executeImport(row, mode);
    }
  });
};



const centerGroupKey = (row) => {
  const type = centerRowType(row);
  const baseType = centerTypeLabel(type);
  
  // 强力提取 TMDB ID，剧集优先找 parent_series_tmdb_id
  let tmdb = '';
  if (baseType === '电影') {
    tmdb = row.tmdb_id || row.share_tmdb_id || '';
  } else {
    tmdb = row.parent_series_tmdb_id || row.series_tmdb_id || row.tmdb_id || row.share_tmdb_id || '';
  }
  
  // 如果没有 TMDB ID，用清理过的纯净标题兜底
  const title = centerBaseTitle(row) || row.title || row.media_title || '';
  const season = Number(row.season_number) || 0;
  const episode = Number(row.episode_number) || 0;

  if (baseType === '电影') return `movie:${tmdb || title}`;
  if (baseType === '剧集') return `series:${tmdb || title}`;
  if (baseType === '季') return `pack:${tmdb || title}:S${season}`;
  if (baseType === '单集') return `ep:${tmdb || title}:S${season}:E${episode}`;
  return `${baseType}:${tmdb || title}:${season}:${episode}`;
};

const centerTableRowKey = (row) => String(
  row?.group_key || row?.display_group_key || row?.source_id || row?.source_ref_id || row?.hub_id || row?.episode_source_id || row?.source_file_id || centerGroupKey(row)
).trim();

const centerIsLazyPlaceholder = (row) => Boolean(row?.__center_lazy_placeholder);
const centerChildCount = (row) => Number(row?.children_count || row?.child_count || row?.pack_item_count || row?.file_count || 0) || 0;
const centerCanLazyLoadChildren = (row) => {
  if (!row || centerIsLazyPlaceholder(row)) return false;
  const typeLabel = centerTypeLabel(centerRowType(row));
  const kind = String(row?.source_kind || '').toLowerCase();
  if (typeLabel !== '季' && !['logical_season', 'season_hub'].includes(kind) && !row?.is_collapsed_pack) return false;
  return Boolean(row?.has_children || row?.lazy_children_kind || centerChildCount(row) > 0 || kind === 'logical_season' || kind === 'season_hub');
};
const centerChildrenAreLoaded = (row) => Boolean(row?.children_loaded || row?._center_children_loaded || (Array.isArray(row?.children) && row.children.length && !row.children.some(centerIsLazyPlaceholder)));
const centerNeedsLoadChildren = (row) => centerCanLazyLoadChildren(row) && !centerChildrenAreLoaded(row);
const centerLazyPlaceholder = (row) => {
  const key = centerTableRowKey(row);
  const count = centerChildCount(row);
  const loading = Boolean(centerChildrenLoading[key]);
  return {
    __center_lazy_placeholder: true,
    group_key: `${key}:lazy-children`,
    source_id: `${key}:lazy-children`,
    item_type: 'Episode',
    display_type: 'Episode',
    title: loading ? '正在加载集明细…' : `展开后加载集明细${count ? `（${count} 集）` : ''}`,
    status: loading ? 'loading' : 'pending',
    status_label: loading ? '加载中' : '待加载',
    versions: [],
  };
};

const normalizeBackendCenterSources = (items) => (items || []).filter(item => item && typeof item === 'object').map(item => {
  const group = {
    ...item,
    group_key: item.group_key || item.display_group_key || centerGroupKey(item),
  };
  group.versions = Array.isArray(item.versions) && item.versions.length
    ? item.versions
    : [{ ...item, versions: undefined, children: undefined, pack_items: undefined, resources: undefined }];
  if (centerNeedsLoadChildren(group)) {
    group.children = Array.isArray(item.children) && item.children.length ? item.children : [centerLazyPlaceholder(group)];
  } else {
    group.children = Array.isArray(item.children) ? item.children : [];
  }
  group.pack_items = Array.isArray(item.pack_items) ? item.pack_items : [];
  return group;
});

const groupCenterSources = (items, orderBy = 'latest') => {
  const normSha1 = (value) => {
    const text = String(value || '').trim().toUpperCase();
    return /^[A-F0-9]{40}$/.test(text) ? text : '';
  };
  const sourceIdentity = (row) => String(
    row?.source_file_id || row?.source_id || row?.source_ref_id || row?.episode_source_id || row?.center_source_id || row?.id || ''
  ).trim();
  const uniquePush = (arr, value) => {
    const text = String(value || '').trim();
    if (text && !arr.includes(text)) arr.push(text);
  };
  const childRowsForSignature = (row) => {
    const children = Array.isArray(row?.children) ? row.children : [];
    const packItems = Array.isArray(row?.pack_items) ? row.pack_items : [];
    return children.length ? children : packItems;
  };
  const packManifestKey = (row) => {
    // 中心端 v7 会下发“集号 + SHA1 清单”的物理版本 key。优先使用它，
    // 避免旧 manifest_hash 因文件名/目录名不同，把完全相同 SHA1 的季包拆成多版本。
    const physicalKey = String(row?.physical_version_key || row?.manifest_sha1_hash || row?.sha1_manifest_hash || row?.pack_manifest_sha1_hash || '').trim();
    if (physicalKey) return physicalKey.startsWith('pack:') ? physicalKey : `logical:${physicalKey}`;

    const parts = [];
    for (const child of childRowsForSignature(row)) {
      const sha1 = normSha1(child?.sha1);
      if (!sha1) continue;
      const epRaw = child?.episode_number ?? '';
      const epNum = Number(epRaw);
      const epKey = Number.isFinite(epNum) && epNum > 0 ? String(epNum).padStart(4, '0') : String(epRaw || '').trim();
      parts.push(`${epKey}:${sha1}`);
    }
    if (parts.length) return [...new Set(parts)].sort().join('|');
    const manifestHash = String(row?.manifest_hash || '').trim();
    return manifestHash ? `manifest:${manifestHash}` : '';
  };
  const versionMergeKey = (row) => {
    const typeLabel = centerTypeLabel(centerRowType(row));
    const sourceKind = String(row?.source_kind || '').trim().toLowerCase();
    const isPack = typeLabel === '季' || sourceKind === 'logical_season' || sourceKind === 'season_hub' || row?.is_collapsed_pack;
    if (sourceKind === 'logical_season') {
      // 逻辑季包的 group_id 就是中心端版本 ID。不能再按 hub/manifest 合并，
      // 否则同一季多个逻辑版本会被前端吃成一条。
      const gid = String(row?.logical_group_id || row?.group_id || row?.source_id || row?.source_ref_id || '').trim();
      if (gid) return `logical:${gid}`;
      if (row?.version_key) return `logical-version:${row.version_key}`;
    }
    if (isPack) {
      const manifest = packManifestKey(row);
      // 季包只有“每一集 SHA1 全部一致”才算同一版本；任意一集不一致就是另一个版本。
      if (manifest) return `pack:${manifest}`;
    }
    const sha1 = normSha1(row?.sha1);
    if (sha1) return `sha1:${sha1}`;
    return `source:${sourceIdentity(row) || JSON.stringify([row?.tmdb_id, row?.season_number, row?.episode_number, row?.file_name || row?.title || ''])}`;
  };
  const mergeChildLists = (left, right) => {
    const out = [];
    const seen = new Set();
    for (const item of [...(Array.isArray(left) ? left : []), ...(Array.isArray(right) ? right : [])]) {
      if (!item || typeof item !== 'object') continue;
      const key = sourceIdentity(item) || `${item.episode_number ?? ''}:${normSha1(item.sha1)}:${item.file_name || ''}`;
      // 只按具体中心子源去重，不按 SHA1 去重；相同 SHA1 的不同来源要留给后续 mergeVersions 累加资源数。
      if (key && seen.has(key)) continue;
      if (key) seen.add(key);
      out.push(item);
    }
    return out;
  };
  const countKeys = ['resource_count', 'usable_resource_count', 'available_holder_count', 'holder_count'];
  const maxKnownCount = (a, b, fallback = 1) => {
    const nums = [fallback];
    for (const row of [a, b]) {
      for (const key of countKeys) {
        const value = Number(row?.[key]);
        if (Number.isFinite(value) && value > 0) nums.push(value);
      }
    }
    return Math.max(...nums);
  };

  // 合并相同版本：电影/单集按 SHA1；季包按“完整 episode_number:SHA1 manifest”。
  // 相同版本只显示一行，资源数按不同中心源/holder 合并；不同 SHA1/manifest 才显示多版本行。
  const mergeVersions = (versions) => {
    const merged = [];
    const versionMap = new Map();
    for (const raw of (versions || [])) {
      if (!raw || typeof raw !== 'object') continue;
      const key = versionMergeKey(raw);
      const srcId = sourceIdentity(raw);
      if (key && versionMap.has(key)) {
        const existing = versionMap.get(key);
        existing._merged_source_ids = Array.isArray(existing._merged_source_ids) ? existing._merged_source_ids : [];
        uniquePush(existing._merged_source_ids, srcId);
        for (const id of (raw._merged_source_ids || [])) uniquePush(existing._merged_source_ids, id);
        const mergeCount = Math.max(existing._merged_source_ids.length || 1, maxKnownCount(existing, raw, 1));
        for (const countKey of countKeys) existing[countKey] = mergeCount;
        existing.source_merge_count = mergeCount;
        existing.version_count = mergeCount;
        existing.success_count = (existing.success_count || 0) + (raw.success_count || 0);
        existing.fail_count = (existing.fail_count || 0) + (raw.fail_count || 0);
        existing.size = Math.max(Number(existing.size || existing.total_size || 0), Number(raw.size || raw.total_size || 0)) || existing.size || raw.size;
        existing.total_size = Math.max(Number(existing.total_size || existing.size || 0), Number(raw.total_size || raw.size || 0)) || existing.total_size || raw.total_size;
        existing.children = mergeChildLists(existing.children, raw.children);
        existing.pack_items = mergeChildLists(existing.pack_items, raw.pack_items);
      } else {
        const item = {
          ...raw,
          _version_merge_key: key,
          _merged_source_ids: [],
          children: Array.isArray(raw.children) ? [...raw.children] : raw.children,
          pack_items: Array.isArray(raw.pack_items) ? [...raw.pack_items] : raw.pack_items,
        };
        uniquePush(item._merged_source_ids, srcId);
        const initialCount = Math.max(item._merged_source_ids.length || 1, maxKnownCount(item, null, 1));
        if (initialCount > 0) {
          for (const countKey of countKeys) item[countKey] = initialCount;
          item.source_merge_count = initialCount;
        }
        if (key) versionMap.set(key, item);
        merged.push(item);
      }
    }
    return merged;
  };

  // 展开后的子集按集号聚合；同一集内部再按 SHA1 合并资源数。
  const groupChildren = (children) => {
    if (!children || !children.length) return undefined;
    const groups = new Map();
    for (const child of children) {
      const epNum = Number(child?.episode_number || 0) || 0;
      const key = epNum || child?.file_name || child?.sha1 || sourceIdentity(child);
      if (!groups.has(key)) {
        groups.set(key, {
          ...child,
          group_key: child.group_key || `ep_${key}`,
          versions: [],
        });
      }
      const childVersions = Array.isArray(child.versions) && child.versions.length ? child.versions : [child];
      groups.get(key).versions.push(...childVersions);
    }

    const result = [];
    for (const group of groups.values()) {
      group.versions = mergeVersions(group.versions);
      // 让子行自身也继承第一条版本的展示字段，避免树表标题/状态列读取空值。
      if (group.versions[0]) {
        Object.assign(group, {
          ...group,
          source_id: group.source_id || group.versions[0].source_id,
          source_ref_id: group.source_ref_id || group.versions[0].source_ref_id,
          sha1: group.sha1 || group.versions[0].sha1,
          size: group.size || group.versions[0].size,
          version_summary: group.version_summary || group.versions[0].version_summary,
          summary_json: group.summary_json || group.versions[0].summary_json,
          media_signature_json: group.media_signature_json || group.versions[0].media_signature_json,
        });
      }
      result.push(group);
    }
    result.sort((a, b) => (Number(a.episode_number || 0) - Number(b.episode_number || 0)) || String(a.file_name || '').localeCompare(String(b.file_name || '')));
    return result;
  };

  let processedGroups = [];

  if ((items || []).some(item => Array.isArray(item?.versions))) {
    processedGroups = (items || []).map(item => {
      const topVersions = Array.isArray(item.versions) && item.versions.length ? item.versions : [item];
      const allChildren = [];
      for (const v of topVersions) {
        allChildren.push(...(Array.isArray(v?.children) ? v.children : []));
        // children 和 pack_items 通常内容相同；只有 children 为空时才用 pack_items，避免重复翻倍。
        if (!Array.isArray(v?.children) || !v.children.length) {
          allChildren.push(...(Array.isArray(v?.pack_items) ? v.pack_items : []));
        }
      }
      if (Array.isArray(item.children)) allChildren.push(...item.children);
      else if (Array.isArray(item.pack_items)) allChildren.push(...item.pack_items);
      return {
        ...item,
        group_key: item.group_key || item.display_group_key || centerGroupKey(item),
        versions: mergeVersions(topVersions),
        children: groupChildren(allChildren),
      };
    });
  } else {
    const byKey = new Map();
    for (const item of (items || [])) {
      const key = centerGroupKey(item);
      let group = byKey.get(key);
      if (!group) {
        group = {
          ...item,
          group_key: key,
          children: item.children,
          pack_items: item.pack_items,
          versions: [],
        };
        byKey.set(key, group);
        processedGroups.push(group);
      }
      group.versions.push(item);
      group.children = mergeChildLists(group.children, item.children);
      group.pack_items = mergeChildLists(group.pack_items, item.pack_items);
    }
    for (const group of processedGroups) {
      group.versions = mergeVersions(group.versions);
      const allChildren = Array.isArray(group.children) && group.children.length ? group.children : group.pack_items;
      group.children = groupChildren(allChildren);
    }
  }

  // ★ 新增判断：如果后端已经分好组并排好序（带有 versions 数组），前端绝对不能再重新跨页排序
  // 否则会导致不同页的数据因为前端 sort_val 计算差异而互相穿插、乱跳。
  const isBackendGrouped = (items || []).some(item => Array.isArray(item?.versions));
  
  if (!isBackendGrouped) {
    for (const group of processedGroups) {
      if (orderBy === 'popular') {
        group.versions.sort((a, b) => (b.success_count || 0) - (a.success_count || 0));
        group.sort_val = Math.max(...group.versions.map(v => v.success_count || 0));
      } else if (orderBy === 'size') {
        group.versions.sort((a, b) => (b.size || b.total_size || 0) - (a.size || a.total_size || 0));
        group.sort_val = Math.max(...group.versions.map(v => v.size || v.total_size || 0));
      } else if (orderBy === 'name') {
        group.versions.sort((a, b) => (a.title || '').localeCompare(b.title || ''));
        group.sort_val = group.title || '';
      } else {
        group.versions.sort((a, b) => centerCreatedTime(b) - centerCreatedTime(a));
        group.sort_val = centerCreatedTime(group.versions[0]);
      }
      group.created_at = group.versions[0]?.created_at || group.created_at;
      group.updated_at = group.versions[0]?.updated_at || group.updated_at;
    }

    processedGroups.sort((a, b) => {
      if (orderBy === 'popular' || orderBy === 'size') return b.sort_val - a.sort_val;
      if (orderBy === 'name') return String(a.sort_val).localeCompare(String(b.sort_val));
      return b.sort_val - a.sort_val;
    });
  }

  for (const group of processedGroups) {
    if (centerNeedsLoadChildren(group) && (!Array.isArray(group.children) || !group.children.length)) {
      group.children = [centerLazyPlaceholder(group)];
    }
  }

  return processedGroups;
};

const trackListToArray = (items) => {
  if (!Array.isArray(items)) return items ? [items] : [];
  return items;
};

const fullCenterEffectText = (value) => {
  let text = String(value || '').trim();
  if (!text || text === '-') return '-';

  text = text
    .replace(/\b(?:dolby\s*vision|dovi|dv)\s*(?:profile\s*)?p?\s*(5|7|8(?:\.\d+)?)/ig, 'Dolby Vision P$1')
    .replace(/\b(?:dovi|dv)\b/ig, 'Dolby Vision')
    .replace(/\bprofile\s*(5|7|8(?:\.\d+)?)\b/ig, 'P$1')
    .replace(/^\s*P(5|7|8(?:\.\d+)?)(?=\s*(?:[/／|,，、]|$))/i, 'Dolby Vision P$1')
    .replace(/\s*([/／|,，、])\s*/g, ' $1 ')
    .replace(/\s+/g, ' ')
    .trim();

  return text || '-';
};

const centerVersionSummary = (it) => {
  const sig = it?.media_signature_json || it?.media_signature || {};
  const raw = it?.summary_json || it?.raw_summary_json || {};
  const v = { ...(raw || {}), ...(sig || {}), ...(it?.version_summary || {}) };
  if (!v.resolution) v.resolution = sig.resolution_display || sig.resolution || raw.resolution || '';
  if (!v.effect) v.effect = sig.effect_display || sig.effect_key || sig.effect || raw.effect || '';
  if (!v.video_codec && !v.codec) v.video_codec = sig.video_codec || sig.codec_display || sig.codec || raw.video_codec || raw.codec || '';
  if (!v.fps) v.fps = sig.fps || sig.frame_rate || raw.fps || raw.frame_rate || '';
  return v;
};
const centerTmdbMeta = (row) => {
  if (!row || typeof row !== 'object') return {};
  const meta = (row.tmdb_meta && typeof row.tmdb_meta === 'object') ? row.tmdb_meta : {};
  const merged = { ...row, ...meta };
  merged.vote_average = meta.vote_average ?? meta.rating ?? row.vote_average ?? row.rating;
  merged.rating = meta.rating ?? row.rating ?? row.vote_average;
  merged.genres = meta.genres ?? meta.genres_json ?? row.genres ?? row.genres_json;
  return merged;
};
const centerStripYear = (text) => String(text || '').replace(/\s*[（(]\s*(?:19|20)\d{2}\s*[）)]\s*$/g, '').trim();
const centerBaseTitle = (row) => {
  const meta = centerTmdbMeta(row);
  const rawTitle = row?.display_title || meta.display_title || row?.series_title || meta.series_title || row?.title || row?.standard_title || row?.media_title || row?.root_name || row?.file_name || row?.tmdb_id || '';
  return centerStripYear(centerSeasonBaseTitle(rawTitle, row));
};
const centerDisplayYear = (row) => centerTmdbMeta(row).year || row?.release_year || '';
const centerDisplayTitle = (row) => {
  const base = appendYear(centerBaseTitle(row), centerDisplayYear(row));
  const typeLabel = centerTypeLabel(centerRowType(row));
  const season = Number(row?.season_number || 0);
  const episode = Number(row?.episode_number || 0);
  if (typeLabel === '剧集') return base;
  if (typeLabel === '单集') {
    const se = [season ? `S${String(season).padStart(2, '0')}` : '', episode ? `E${String(episode).padStart(2, '0')}` : ''].join('');
    return se ? `${base} ${se}` : base;
  }
  return base;
};
const centerDisplayGenres = (row) => {
  const meta = centerTmdbMeta(row);
  let genres = meta.genres;
  if (!genres) return '';
  
  // 兼容本地数据库存的字符串格式 (如 "科幻,冒险")
  if (typeof genres === 'string') {
    try {
      genres = JSON.parse(genres);
    } catch (e) {
      genres = genres.split(/[,，、/]/);
    }
  }
  
  // 提取并截取前 3 个
  if (Array.isArray(genres)) {
    const names = genres.map(g => typeof g === 'object' ? g.name : g).filter(Boolean);
    return names.slice(0, 3).join(' / ');
  }
  return '';
};

const parseCenterJsonArray = (value) => {
  if (Array.isArray(value)) return value.filter(x => x && typeof x === 'object');
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed.filter(x => x && typeof x === 'object') : [];
    } catch (_) {
      return [];
    }
  }
  return [];
};
const centerPeopleList = (value) => parseCenterJsonArray(value);
const centerPersonName = (p) => String(p?.name || p?.primary_name || p?.original_name || '').trim();
const centerPersonCharacter = (p) => String(p?.character || p?.character_name || '').trim();
const centerCharacterHasRolePrefix = (text) => /^(饰|配|配音|声演|CV|Voice|voice)\s*/.test(String(text || '').trim());
const centerRolePrefixForRow = (row) => (row?._is_animation || isCenterAnimation(row)) ? '配' : '饰';
const centerCharacterRoleText = (character, rowOrPerson = {}) => {
  const text = String(character || '').trim();
  if (!text) return '';
  if (centerCharacterHasRolePrefix(text)) return text;
  return `${centerRolePrefixForRow(rowOrPerson)} ${text}`;
};
const centerDetailCreditsText = (row) => {
  if (!row || typeof row !== 'object') return '';
  const actors = centerDetailPeople(row).filter(p => p._credit_role !== 'director');
  const directors = centerDetailPeople(row).filter(p => p._credit_role === 'director');
  const parts = [];
  if (actors.length) {
    const text = actors.slice(0, 9).map(p => {
      const name = centerPersonName(p);
      const roleText = centerCharacterRoleText(centerPersonCharacter(p), p);
      return name ? (roleText ? `${name} ${roleText}` : name) : '';
    }).filter(Boolean).join('；');
    if (text) parts.push(`主演：${text}`);
  }
  if (directors.length) {
    const names = directors.map(centerPersonName).filter(Boolean).slice(0, 1).join('、');
    if (names) parts.push(`导演：${names}`);
  }
  return parts.join('  ·  ');
};
const centerDetailPeople = (row) => {
  if (!row || typeof row !== 'object') return [];
  const meta = centerTmdbMeta(row);
  const animation = isCenterAnimation(row);
  const actors = centerPeopleList(row.actors || meta.actors)
    .slice(0, 9)
    .map(p => ({ ...p, _credit_role: 'actor', _is_animation: animation }))
    .filter(centerPersonName);
  const directors = centerPeopleList(row.directors || meta.directors)
    .slice(0, 1)
    .map(p => ({ ...p, _credit_role: 'director', _is_animation: animation }))
    .filter(centerPersonName);
  if (!actors.length && !directors.length) {
    const people = centerPeopleList(row.people_json || meta.people_json);
    const credits = centerPeopleList(row.credits_json || meta.credits_json);
    if (people.length && credits.length) {
      const peopleMap = new Map(
        people.map(p => [String(p?.tmdb_person_id || p?.id || ''), p]).filter(([id]) => id)
      );
      return credits
        .map(credit => {
          const id = String(credit?.tmdb_person_id || credit?.id || '');
          const person = peopleMap.get(id) || {};
          return {
            ...person,
            ...credit,
            character: credit?.character || credit?.character_name || person?.character || '',
            _credit_role: String(credit?.credit_type || '').toLowerCase() === 'director' ? 'director' : 'actor',
            _is_animation: animation,
          };
        })
        .filter(centerPersonName)
        .slice(0, 10);
    }
  }
  // 展示顺序：主演在前，导演最后。
  return [...actors, ...directors];
};
const centerPersonRoleText = (p) => {
  if (!p) return '';
  if (p._credit_role === 'director' || String(p.credit_type || '').toLowerCase() === 'director') return '导演';
  const character = centerPersonCharacter(p);
  return character ? centerCharacterRoleText(character, p) : '主演';
};
const centerPersonKey = (p) => `${p?._credit_role || p?.credit_type || 'person'}:${p?.tmdb_person_id || p?.id || centerPersonName(p)}:${centerPersonCharacter(p)}`;
const centerPersonTooltip = (p) => {
  const name = centerPersonName(p);
  const role = centerPersonRoleText(p);
  return [name, role].filter(Boolean).join(' · ');
};
const centerProfileFallbackSvg = 'data:image/svg+xml;utf8,' + encodeURIComponent(`
<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96">
  <defs><linearGradient id="g" x1="0" x2="1" y1="0" y2="1"><stop stop-color="#263866"/><stop offset="1" stop-color="#111827"/></linearGradient></defs>
  <rect width="96" height="96" rx="18" fill="url(#g)"/>
  <circle cx="48" cy="35" r="16" fill="#94a3b8" opacity=".72"/>
  <path d="M20 82c4-18 16-28 28-28s24 10 28 28" fill="#94a3b8" opacity=".72"/>
</svg>`);
const centerProfileUrl = (p, size = 'w185') => tmdbPosterUrl(p?.profile_path || p?.profile_url || p?.avatar || '', size) || centerProfileFallbackSvg;
const centerProfileImgAttrs = (p) => ({
  src: centerProfileUrl(p, 'w185'),
  alt: centerPersonName(p) || '人物头像',
  title: centerPersonTooltip(p),
  loading: 'lazy',
  decoding: 'async',
  referrerpolicy: 'no-referrer',
  draggable: 'false',
});
const onCenterProfileError = (event) => {
  const target = event?.target;
  if (target && target.src !== centerProfileFallbackSvg) target.src = centerProfileFallbackSvg;
};

const centerPosterWallPrimaryTitle = (row) => {
  const base = centerBaseTitle(row) || '未知资源';
  return base;
};
const centerAvailableSeasonNumbers = (row) => {
  const nums = [];
  const push = (value) => {
    const n = centerSeasonTabNumber(value);
    if (n !== null && !nums.includes(n)) nums.push(n);
  };
  if (Array.isArray(row?.available_season_numbers)) row.available_season_numbers.forEach(push);
  if (Array.isArray(row?.season_numbers)) row.season_numbers.forEach(push);
  if (Array.isArray(row?.seasons)) row.seasons.forEach(push);
  const direct = centerSeasonNumber(row);
  if (!nums.length && direct !== null) push(direct);
  return nums.sort((a, b) => (a === 0 ? -1 : a) - (b === 0 ? -1 : b));
};
const centerSingleSeasonNumber = (row) => {
  const nums = centerAvailableSeasonNumbers(row);
  return nums.length === 1 ? nums[0] : null;
};
const centerSingleSeasonLabel = (row) => {
  const n = centerSingleSeasonNumber(row);
  if (n === null || n === 1) return '';
  return n === 0 ? '特别篇' : `第 ${n} 季`;
};
const centerPosterWallSeasonLabel = (row) => {
  const n = centerSingleSeasonNumber(row);
  if (n === null) return '';
  return n === 0 ? '特别篇' : `第 ${n} 季`;
};
const centerSeasonCount = (row) => {
  const explicit = Number(row?.season_count || row?.number_of_seasons || 0);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  return centerAvailableSeasonNumbers(row).length;
};
const centerPosterWallYear = (row) => {
  const year = centerDisplayYear(row) || '';
  const isSeries = centerIsSeriesGroup(row);
  const isSeriesResource = isSeries || centerIsSeasonLike(row) || centerTypeLabel(centerRowType(row)) === '单集';
  const seasonCount = isSeries ? centerSeasonCount(row) : 0;
  const singleSeasonLabel = isSeriesResource ? centerPosterWallSeasonLabel(row) : '';
  const parts = [];
  if (year) parts.push(year);
  if (seasonCount > 1) parts.push(`共 ${seasonCount} 季`);
  else if (singleSeasonLabel) parts.push(singleSeasonLabel);
  return parts.join(' · ');
};
const centerPosterWallFullTitle = (row) => {
  const title = centerPosterWallPrimaryTitle(row);
  const year = centerPosterWallYear(row);
  return year ? `${title}（${year}）` : title;
};

const centerPosterUrlCache = new Map();

const tmdbPosterUrl = (value, size = 'w300') => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw === '/default-poster.png' || raw.startsWith('data:')) return raw;

  const cacheKey = `${size}:${raw}`;
  if (centerPosterUrlCache.has(cacheKey)) return centerPosterUrlCache.get(cacheKey);

  let path = raw;
  const proxyMatch = raw.match(/^\/api\/discover\/tmdb\/image\/[^/]+\/(.+)$/i);
  if (proxyMatch) {
    path = proxyMatch[1] || '';
  } else if (/^https?:\/\//i.test(raw)) {
    const tmdbMatch = raw.match(/image\.tmdb\.org\/t\/p\/[^/]+\/(.+)$/i);
    if (!tmdbMatch) {
      centerPosterUrlCache.set(cacheKey, raw);
      return raw;
    }
    path = tmdbMatch[1] || '';
  }

  path = String(path || '').replace(/^\/+/, '');
  const directUrl = path ? `https://image.tmdb.org/t/p/${size}/${encodeURI(path)}` : '';
  centerPosterUrlCache.set(cacheKey, directUrl);
  return directUrl;
};

const centerPosterCandidates = (row) => {
  const meta = centerTmdbMeta(row);
  const versionPosters = (Array.isArray(row?.versions) ? row.versions : [])
    .flatMap(v => [v?.poster_path, v?.poster_url, v?.poster, v?.image, v?.cover]);
  return [
    row?.poster_path,
    row?.poster_url,
    row?.poster,
    row?.image,
    row?.cover,
    ...versionPosters,
    meta.poster_path,
    meta.poster_url,
  ].map(v => String(v || '').trim()).filter(Boolean);
};
const centerPosterUrl = (row, size = 'w185') => {
  for (const value of centerPosterCandidates(row)) {
    const url = tmdbPosterUrl(value, size);
    if (url) return url;
  }
  return '/default-poster.png';
};
const centerPosterImgAttrs = (row, size = 'w185', index = 0) => {
  const title = centerPosterWallFullTitle(row) || centerDisplayTitle(row) || '共享资源海报';
  const eager = Number(index || 0) < 8;
  return {
    src: centerPosterUrl(row, size),
    alt: title,
    title,
    loading: eager ? 'eager' : 'lazy',
    decoding: 'async',
    referrerpolicy: 'no-referrer',
    fetchpriority: eager ? 'high' : 'auto',
    draggable: 'false',
  };
};
const onCenterPosterError = (event) => {
  const target = event?.target;
  if (!target) return;
  if (!target.src.endsWith('/default-poster.png')) target.src = '/default-poster.png';
};
const centerRibbonText = (row) => {
  const direct = String(row?.center_ribbon_text || row?.ribbon_text || '').trim();
  if (direct) return direct;
  if (isCenterReplenishRow(row)) return '待补充';
  const resourceStatus = centerSeasonResourceStatus(row);
  if (resourceStatus === 'consistent') return '一致版';
  if (resourceStatus === 'completed') return '已完结';
  if (resourceStatus === 'missing') return '缺集';
  if (resourceStatus === 'ongoing') return '连载中';
  if (isCenterCompletedCertified(row)) return '一致版';
  if (centerIsCompletedByServer(row)) return '已完结';
  if (centerIsOngoingHub(row)) return '连载中';
  return '';
};
const centerRibbonClass = (row) => {
  const direct = String(row?.center_ribbon_class || row?.ribbon_class || '').trim();
  if (direct) return direct;
  if (isCenterReplenishRow(row)) return 'center-ribbon-warning';
  const resourceStatus = centerSeasonResourceStatus(row);
  if (resourceStatus === 'consistent') return 'center-ribbon-green';
  if (resourceStatus === 'completed') return 'center-ribbon-dark';
  if (resourceStatus === 'missing') return 'center-ribbon-warning';
  if (resourceStatus === 'ongoing') return 'center-ribbon-blue';
  if (isCenterCompletedCertified(row)) return 'center-ribbon-green';
  if (centerIsCompletedByServer(row)) return 'center-ribbon-dark';
  if (centerIsOngoingHub(row)) return 'center-ribbon-blue';
  return 'center-ribbon-dark';
};
const centerCardMetaText = (row) => {
  const typeLabel = centerTypeLabel(centerRowType(row));
  const parts = [typeLabel];
  const season = centerSeasonNumber(row);
  const episode = Number(row?.episode_number || 0);
  if (typeLabel === '剧集') {
    const seasonCount = centerSeasonCount(row);
    const singleSeasonLabel = centerSingleSeasonLabel(row);
    if (seasonCount > 1) parts.push(`共 ${seasonCount} 季`);
    else if (singleSeasonLabel) parts.push(singleSeasonLabel);
  } else if (typeLabel === '季' && centerIsSpecialSeason(row)) {
    parts.push('特别篇');
  } else if (typeLabel === '季' && season > 0) {
    parts.push(`S${String(season).padStart(2, '0')}`);
  }
  if (typeLabel === '单集' && (season || episode)) parts.push(`${season ? `S${String(season).padStart(2, '0')}` : ''}${episode ? `E${String(episode).padStart(2, '0')}` : ''}`);
  const tmdb = tmdbIdForRow(row);
  if (tmdb) parts.push(`TMDb ${tmdb}`);
  return parts.join(' · ');
};
const centerTagPush = (arr, label, type = 'default', key = '') => {
  const text = String(label || '').trim();
  if (!text || text === '-') return;
  if (arr.some(x => x.label === text)) return;
  arr.push({ key: key || text, label: text, type });
};
const centerProvidedTags = (row) => {
  const labels = Array.isArray(row?.tag_labels) ? row.tag_labels : [];
  return labels
    .map(item => {
      if (item && typeof item === 'object') {
        const label = String(item.label || item.name || item.text || '').trim();
        return label ? { label, type: item.type || 'default', key: item.key || `center-${label}` } : null;
      }
      const label = String(item || '').trim();
      return label ? { label, type: 'default', key: `center-${label}` } : null;
    })
    .filter(Boolean);
};
const centerTagColorPalette = ['primary', 'info', 'success', 'warning', 'error'];
const centerStableHash = (value) => {
  const text = String(value || '');
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
};
const colorizeCenterTags = (tags, row) => {
  const used = new Set();
  const seed = centerVersionKey(row);
  return (tags || []).map((tag, index) => {
    let colorIndex = centerStableHash(`${seed}:${tag.key || tag.label}:${index}`) % centerTagColorPalette.length;
    if (used.size < centerTagColorPalette.length) {
      for (let i = 0; i < centerTagColorPalette.length && used.has(centerTagColorPalette[colorIndex]); i += 1) {
        colorIndex = (colorIndex + 1) % centerTagColorPalette.length;
      }
    }
    const type = centerTagColorPalette[colorIndex];
    used.add(type);
    return { ...tag, type };
  });
};
const centerSeasonTabNumber = (season) => {
  const raw = (season && typeof season === 'object')
    ? (season.season_number ?? season.active_season_number ?? season.default_season_number)
    : season;
  if (raw === undefined || raw === null || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
};
const centerSeasonTabSortValue = (season) => {
  const n = centerSeasonTabNumber(season);
  if (n === null) return 99999;
  // 特别篇标签按惯例放最前面；默认资源列表仍由 centerDefaultDetailSeason 固定优先第一季。
  return n === 0 ? -1 : n;
};
const centerDetailSeasonListFromRow = (row) => {
  const raw = Array.isArray(row?.seasons) ? row.seasons.filter(Boolean) : [];
  let list = raw.length ? raw : [];
  if (!list.length && centerIsSeriesGroup(row)) {
    list = centerAvailableSeasonNumbers(row).map(n => ({
      season_number: n,
      season_label: n === 0 ? '特别篇' : `第 ${n} 季`,
      season_title: n === 0 ? '特别篇' : `第 ${n} 季`,
      display_season_title: n === 0 ? '特别篇' : `第 ${n} 季`,
    }));
  }
  if (!list.length && centerIsSeasonLike(row)) {
    const season = centerSeasonNumber(row);
    if (season !== null) list = [{ ...row, season_number: season }];
  }
  const seen = new Set();
  return list
    .map(item => (item && typeof item === 'object') ? item : { season_number: item })
    .filter(item => {
      const n = centerSeasonTabNumber(item);
      const key = String(n ?? centerVersionKey(item));
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => centerSeasonTabSortValue(a) - centerSeasonTabSortValue(b));
};
const centerDefaultDetailSeason = (row) => {
  const list = centerDetailSeasonListFromRow(row);
  const nums = list.map(centerSeasonTabNumber).filter(n => n !== null);
  const regular = nums.filter(n => n > 0);
  const firstRegular = nums.includes(1) ? 1 : (regular.length ? regular[0] : null);
  const explicit = centerSeasonTabNumber(row?.active_season_number ?? row?.default_season_number);
  // 打开详情默认看第一季资源；如果中心旧缓存把特别篇设成默认，前端兜底纠正。
  if (explicit !== null && !(explicit === 0 && firstRegular !== null)) return explicit;
  if (firstRegular !== null) return firstRegular;
  if (nums.length) return nums[0];
  return centerSeasonNumber(row);
};
const centerDetailActiveSeasonRow = computed(() => {
  const row = activeCenterDetailRow.value || {};
  if (!centerIsSeriesGroup(row)) return row;
  const active = centerSeasonTabNumber(centerDetailActiveSeason.value ?? centerDefaultDetailSeason(row));
  const seasons = centerDetailSeasonListFromRow(row);
  const selected = seasons.find(item => centerSeasonTabNumber(item) === active) || seasons[0] || {};
  return {
    ...selected,
    tmdb_id: row.tmdb_id || selected.tmdb_id,
    title: row.title || selected.title,
    poster_path: row.poster_path || selected.poster_path,
    overview: row.overview || selected.overview,
    backdrop_path: row.backdrop_path || selected.backdrop_path,
    release_year: row.release_year || selected.release_year,
    actors: row.actors || selected.actors,
    directors: row.directors || selected.directors,
    tmdb_meta: { ...(centerTmdbMeta(row) || {}), ...(centerTmdbMeta(selected) || {}) },
  };
});

const centerVersionKey = (row) => String(centerTableRowKey(row) || row?._version_merge_key || row?.sha1 || row?.manifest_hash || row?.file_name || Math.random());

const parseCenterJsonObject = (value) => {
  if (!value) return {};
  if (typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (_) {
      return {};
    }
  }
  return {};
};
const centerVersionExpandKey = (row) => String(row?.logical_group_id || row?.logical_group?.group_id || row?.group_id || centerVersionKey(row));
const centerVersionBestAssetMap = (row) => {
  const direct = parseCenterJsonObject(row?.best_asset_map);
  if (Object.keys(direct).length) return direct;
  return parseCenterJsonObject(row?.logical_group?.best_asset_map);
};
const centerVersionAssetAvailable = (asset) => {
  if (!asset || typeof asset !== 'object') return false;
  if (!asset.asset_id) return false;
  const status = String(asset.status || asset.source_status || asset.backing_status || 'alive').toLowerCase();
  return ['alive', 'available', 'updating', 'incomplete', 'pool_complete', 'pool_partial'].includes(status);
};
const centerVersionEpisodeTotal = (row) => Math.max(
  centerPositiveNumber(row?.episode_total),
  centerPositiveNumber(row?.progress_total),
  centerPositiveNumber(row?.logical_group?.episode_total),
  centerPositiveNumber(row?.expected_episode_count),
  centerPositiveNumber(row?.total_episodes),
  centerPositiveNumber(row?.episode_count),
  centerProgressParts(row).total,
);
const centerVersionAvailableEpisodeNumbers = (row) => {
  const assetMap = centerVersionBestAssetMap(row);
  const numbers = new Set();
  Object.keys(assetMap)
    .map(key => ({ key, num: Number(key) }))
    .filter(({ key, num }) => Number.isFinite(num) && num > 0 && centerVersionAssetAvailable(assetMap[key]))
    .forEach(({ num }) => numbers.add(num));
  const childRows = [
    ...(Array.isArray(row?.children) ? row.children : []),
    ...(Array.isArray(row?.pack_items) ? row.pack_items : []),
  ];
  childRows
    .map(item => Number(item?.episode_number || 0))
    .filter(n => Number.isFinite(n) && n > 0)
    .forEach(n => numbers.add(n));
  return [...numbers];
};
const centerVersionAvailableEpisodeMax = (row) => {
  const numbers = centerVersionAvailableEpisodeNumbers(row);
  return Math.max(...numbers, numbers.length ? 0 : centerProgressParts(row).current, 0);
};
const centerVersionAvailableEpisodeCount = (row) => centerVersionAvailableEpisodeNumbers(row).length || centerProgressParts(row).current;
const centerVersionExistingProgressText = (row, scope = null) => {
  const current = centerVersionAvailableEpisodeCount(row);
  const total = centerPositiveNumber(scope?.total) || centerVersionEpisodeTotal(row) || current;
  if (current > 0 && total > 0) return `${current}/${total}`;
  return current > 0 ? String(current) : '';
};
const centerVersionEpisodeItems = (row) => {
  const assetMap = centerVersionBestAssetMap(row);
  const maxEp = Math.max(centerVersionEpisodeTotal(row), centerVersionAvailableEpisodeMax(row), 0);
  if (!maxEp) return [];
  const pad = maxEp >= 10 ? 2 : 1;
  const items = [];
  for (let ep = 1; ep <= maxEp; ep += 1) {
    const rawAsset = assetMap[String(ep)] || assetMap[ep] || null;
    const asset = centerVersionAssetAvailable(rawAsset) ? rawAsset : null;
    items.push({
      episode_number: ep,
      label: String(ep).padStart(pad, '0'),
      asset,
      key: `${centerVersionExpandKey(row)}:ep:${ep}`,
      loadingKey: asset?.asset_id ? `logical_episode:${asset.asset_id}` : `${centerVersionExpandKey(row)}:missing:${ep}`,
    });
  }
  return items;
};
const centerVersionCanExpandEpisodes = (row) => centerVersionEpisodeItems(row).some(item => item.asset && item.asset.asset_id);
const centerVersionEpisodesExpanded = (row) => Boolean(centerVersionExpandedMap[centerVersionExpandKey(row)]);
const toggleCenterVersionEpisodes = (row) => {
  if (!centerVersionCanExpandEpisodes(row)) return;
  const key = centerVersionExpandKey(row);
  centerVersionExpandedMap[key] = !centerVersionExpandedMap[key];
};
const buildLogicalEpisodeImportSource = (version, episode) => {
  const asset = episode?.asset || {};
  const activeRow = activeCenterDetailRow.value || {};
  const activeSeason = centerSeasonTabNumber(centerDetailActiveSeason.value ?? centerDefaultDetailSeason(activeRow));
  const sourceId = String(asset.asset_id || '').trim();
  const rapidMeta = {
    ...(asset.rapid_meta_json && typeof asset.rapid_meta_json === 'object' ? asset.rapid_meta_json : {}),
    preid: asset.preid || '',
    pick_code: asset.pick_code || asset.pickcode || '',
    file_id: asset.file_id || asset.fid || '',
    source_kind: 'logical_episode',
    source_id: sourceId,
    original_source_kind: asset.source_kind || '',
    original_source_ref_id: asset.source_ref_id || '',
    logical_group_id: version?.logical_group_id || version?.group_id || version?.logical_group?.group_id || '',
  };
  return {
    source_kind: 'logical_episode',
    source_id: sourceId,
    source_ref_id: sourceId,
    title: centerTitleText(activeRow) || centerTitleText(version) || version?.title || '',
    file_name: asset.file_name || asset.name || '',
    tmdb_id: version?.tmdb_id || activeRow?.tmdb_id || '',
    parent_series_tmdb_id: version?.tmdb_id || activeRow?.tmdb_id || '',
    item_type: 'Episode',
    display_type: 'Episode',
    season_number: version?.season_number ?? activeSeason ?? null,
    episode_number: episode?.episode_number ?? asset.episode_number ?? null,
    sha1: asset.sha1 || '',
    preid: asset.preid || '',
    size: asset.size || 0,
    file_size: asset.size || 0,
    pick_code: asset.pick_code || asset.pickcode || '',
    version_summary: version?.version_summary || version?.summary_json || version?.media_signature_json || {},
    summary_json: version?.summary_json || version?.version_summary || {},
    media_signature_json: version?.media_signature_json || version?.version_summary || {},
    rapid_meta_json: rapidMeta,
    logical_group_id: version?.logical_group_id || version?.group_id || version?.logical_group?.group_id || '',
    logical_episode_asset: asset,
  };
};
const importCenterLogicalEpisode = async (version, episode) => {
  const source = buildLogicalEpisodeImportSource(version, episode);
  if (!source.source_id || !source.sha1) {
    message.error('逻辑单集缺少 asset_id 或 SHA1，不能秒传');
    return;
  }
  const loadingKey = episode.loadingKey || `logical_episode:${source.source_id}`;
  importingMap[loadingKey] = 'permanent';
  try {
    const res = await axios.post('/api/shared/resources/center/import', {
      mode: 'permanent',
      source_id: source.source_id,
      source,
      context: source,
    });
    message.success(res.data?.message || `第 ${episode.episode_number} 集秒传完成`);
    await Promise.allSettled([loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || `第 ${episode.episode_number} 集秒传失败`);
  } finally {
    delete importingMap[loadingKey];
  }
};
const centerDetailVersions = computed(() => {
  const row = activeCenterDetailRow.value || {};
  const host = centerIsSeriesGroup(row) ? (centerDetailActiveSeasonRow.value || row) : row;
  const pick = (key) => Array.isArray(host[key]) ? host[key].filter(Boolean) : [];
  // 详情页资源列表只展示“电影源 / 季包源”这一层。
  // children / pack_items 是包内单集，详情页不兜底展示；秒传时再懒加载。
  let versions = pick('resources');
  if (!versions.length) versions = pick('versions');
  if (!versions.length) versions = pick('items');
  if (!versions.length && !centerIsSeriesGroup(row)) versions = [host];
  const seen = new Set();
  return versions
    .filter(v => v && !centerIsLazyPlaceholder(v))
    .filter(v => {
      const key = centerIsLogicalSeasonRow(v)
        ? `logical:${String(v?.logical_group_id || v?.group_id || v?.source_id || v?.source_ref_id || v?.version_key || JSON.stringify(v).slice(0, 80))}`
        : String(v?.source_id || v?.source_ref_id || v?.hub_id || v?.sha1 || v?.manifest_hash || v?.file_name || JSON.stringify(v).slice(0, 80));
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
});
const centerDetailProgressScope = computed(() => {
  const versions = centerDetailVersions.value || [];
  const current = Math.max(...versions.map(centerVersionAvailableEpisodeMax), 0);
  const total = Math.max(...versions.map(centerVersionEpisodeTotal), current, 0);
  return { current, total };
});
const centerDetailSeasonProgressVisible = computed(() => {
  const scope = centerDetailProgressScope.value;
  if (!scope.total || !scope.current) return false;
  const host = centerDetailActiveSeasonRow.value || activeCenterDetailRow.value || {};
  return centerIsSeasonLike(host) || (centerDetailVersions.value || []).some(v => centerIsOngoingHub(v) || centerIsSeasonLike(v));
});
const centerDetailSeasonProgressPercent = computed(() => {
  const scope = centerDetailProgressScope.value;
  if (!scope.total) return 0;
  return Math.min(100, Math.round((scope.current / scope.total) * 100));
});
const centerDetailSeasonProgressText = computed(() => {
  const scope = centerDetailProgressScope.value;
  return scope.current && scope.total ? `更新至 ${scope.current}/${scope.total} 集` : '';
});

const centerVersionTags = (row, progressScope = null) => {
  const summary = centerVersionSummary(row) || {};
  const tags = [];
  
  // 1. 进度显示；集明细由资源行点击触发，不再占用标签位。
  const scopedProgress = progressScope?.value || progressScope;
  const progress = centerIsOngoingHub(row)
    ? centerVersionExistingProgressText(row, scopedProgress)
    : centerProgressText(row);
  if (progress) {
    const progressLabel = centerIsOngoingHub(row) ? `已有 ${progress} 集` : progress;
    centerTagPush(tags, progressLabel, 'info', 'progress');
  }

  // 共享池完整 / 候选 / 资产 / 可建分享属于中心端调试信息，不在用户前端展示。

  // 2. 基础参数
  centerTagPush(tags, formatCenterSize(row), 'info', 'size'); // 改为 info
  centerTagPush(tags, summary.resolution, 'success', 'resolution');
  centerTagPush(tags, fullCenterEffectText(summary.effect), 'warning', 'effect');
  const codec = [summary.video_codec || summary.codec, summary.bit_depth ? `${summary.bit_depth}bit` : ''].filter(Boolean).join(' · ');
  centerTagPush(tags, codec, 'info', 'codec'); // 改为 info
  
  // 3. 彻底修复 FPS 叠词
  if (summary.fps) {
    const cleanFps = String(summary.fps).replace(/fps/ig, '').trim();
    if (cleanFps) centerTagPush(tags, `${cleanFps} fps`, 'info', 'fps'); // 改为 info
  }
  
  // 4. 业务标签只展示中心端口径。
  const centerLabels = centerProvidedTags(row);
  centerLabels.forEach(t => centerTagPush(tags, t.label, t.type, t.key));
  return colorizeCenterTags(tags, row);
};
const mergeCenterDetailPayload = (base, payload) => {
  const row = { ...(base || {}) };
  const data = payload?.data && typeof payload.data === 'object' ? payload.data : (payload || {});
  const detailItem = data.item && typeof data.item === 'object' ? data.item : {};
  const detailData = {
    ...Object.fromEntries(Object.entries(data).filter(([k]) => !['data', 'item'].includes(k))),
    ...detailItem,
  };
  const meta = detailData.media_meta || detailData.tmdb_meta || detailData.meta || {};
  const oldMeta = centerTmdbMeta(row) || {};
  const keepSeasonPoster = !centerIsSeriesGroup(row) && centerIsSeasonLike(row);
  const basePoster = {
    poster_path: row.poster_path,
    poster_url: row.poster_url,
    poster: row.poster,
    image: row.image,
    cover: row.cover,
  };
  const merged = {
    ...row,
    ...detailData,
    tmdb_meta: { ...oldMeta, ...meta },
  };
  if (keepSeasonPoster) {
    for (const [key, value] of Object.entries(basePoster)) {
      if (value) merged[key] = value;
    }
  }
  for (const field of ['poster_path', 'backdrop_path', 'overview', 'title', 'release_year']) {
    if (!merged[field] && meta[field]) merged[field] = meta[field];
  }
  if (!merged.actors && Array.isArray(detailData.actors)) merged.actors = detailData.actors;
  if (!merged.directors && Array.isArray(detailData.directors)) merged.directors = detailData.directors;
  if (!merged.people_json && Array.isArray(detailData.people_json)) merged.people_json = detailData.people_json;
  if (!merged.credits_json && Array.isArray(detailData.credits_json)) merged.credits_json = detailData.credits_json;

  const activeSeason = centerSeasonTabNumber(
    detailData.active_season_number ?? detailData.default_season_number ?? detailData.season_number ?? centerDetailActiveSeason.value ?? row.active_season_number ?? row.default_season_number
  );
  if (Array.isArray(detailData.seasons) && detailData.seasons.length) {
    merged.seasons = detailData.seasons;
  } else if (Array.isArray(row.seasons) && row.seasons.length) {
    merged.seasons = row.seasons;
  }

  for (const key of ['resources', 'versions', 'items']) {
    if (Array.isArray(data[key]) && data[key].length) merged[key] = data[key];
  }

  if (centerIsSeriesGroup(merged)) {
    if (activeSeason !== null) merged.active_season_number = activeSeason;
    if (merged.default_season_number === undefined || merged.default_season_number === null || merged.default_season_number === '') {
      merged.default_season_number = centerDefaultDetailSeason(merged);
    }
    const resources = Array.isArray(data.resources) && data.resources.length
      ? data.resources
      : (Array.isArray(data.versions) && data.versions.length ? data.versions : (Array.isArray(data.items) ? data.items : []));
    if (activeSeason !== null && resources.length) {
      let seasons = centerDetailSeasonListFromRow(merged);
      if (!seasons.some(s => centerSeasonTabNumber(s) === activeSeason)) {
        seasons = [...seasons, { season_number: activeSeason, season_label: activeSeason === 0 ? '特别篇' : `第 ${activeSeason} 季` }];
      }
      merged.seasons = seasons.map(season => centerSeasonTabNumber(season) === activeSeason
        ? { ...season, resources, versions: resources, items: resources, children: [], pack_items: [] }
        : season);
    }
  }

  // 详情页不接收包内单集，避免旧中心/旧缓存把 children/pack_items 带回弹窗。
  delete merged.children;
  delete merged.pack_items;
  return merged;
};

const centerDetailParams = (row, seasonOverride = null) => {
  const overrideProvided = seasonOverride !== null && seasonOverride !== undefined && seasonOverride !== '';
  const season = centerSeasonTabNumber(overrideProvided ? seasonOverride : centerSeasonNumber(row));
  return {
    source_kind: row?.source_kind || row?.lazy_children_kind || '',
    source_id: row?.source_id || row?.source_ref_id || '',
    hub_id: row?.hub_id || '',
    tmdb_id: tmdbIdForRow(row) || row?.tmdb_id || '',
    item_type: centerRowType(row) || row?.item_type || '',
    season_number: season ?? '',
    // 详情页只取展示元数据 + 版本壳；包内集列表在秒传确认后再请求。
    limit: 120,
    include_people: 1,
  };
};

const loadCenterSourceDetail = async (row, seasonOverride = null) => {
  const res = await axios.get('/api/shared/resources/center/sources/detail', { params: centerDetailParams(row, seasonOverride) });
  if (res.data?.success === false) throw new Error(res.data?.message || '加载详情失败');
  return res.data?.data || res.data || {};
};

const applyCenterDetailPayload = (base, payload, seasonOverride = null) => {
  const merged = mergeCenterDetailPayload(base, payload);
  if (centerIsSeriesGroup(merged)) {
    let active = centerSeasonTabNumber(payload?.active_season_number ?? payload?.data?.active_season_number ?? seasonOverride ?? merged.active_season_number ?? centerDefaultDetailSeason(merged));
    // 打开详情时不显式指定季号；即使旧中心返回 active=0，也不要默认落到特别篇。
    if (active === 0 && (seasonOverride === null || seasonOverride === undefined || seasonOverride === '')) {
      const fallback = centerDefaultDetailSeason({ ...merged, active_season_number: null, default_season_number: null });
      if (fallback !== null && fallback > 0) active = fallback;
    }
    if (active !== null) {
      merged.active_season_number = active;
      centerDetailActiveSeason.value = active;
    }
  }
  return merged;
};

const openCenterDetail = async (row) => {
  if (!row) return;
  centerDetailActiveSeason.value = centerIsSeriesGroup(row) ? centerDefaultDetailSeason(row) : centerSeasonNumber(row);
  activeCenterDetailRow.value = row;
  showCenterDetailModal.value = true;
  centerDetailLoading.value = true;
  try {
    try {
      const requestedSeason = centerDetailActiveSeason.value;
      const detailPayload = await loadCenterSourceDetail(row, requestedSeason);
      activeCenterDetailRow.value = applyCenterDetailPayload(row, detailPayload, requestedSeason);
    } catch (e) {
      console.warn('[共享资源] 加载中心详情失败，退回列表壳:', e);
    }
  } finally {
    centerDetailLoading.value = false;
  }
};

const ledgerColumns = [
  { title: '时间', key: 'created_at', width: 180, render: row => withLedgerTooltip(row, fmtDate(row.created_at)) },
  { title: '事件', key: 'event_type', width: 190, render: row => withLedgerTooltip(row, row.event_label || ledgerEventLabel(row.event_type)) },
  { title: '变化', key: 'delta', width: 90, render: row => {
    const n = Number(row.delta || 0);
    const node = h(NTag, { type: n > 0 ? 'success' : (n < 0 ? 'error' : 'default'), size: 'small' }, { default: () => formatDelta(n) });
    return withLedgerTooltip(row, node);
  } },
  { title: '标题', key: 'title', minWidth: 220, ellipsis: { tooltip: true }, render: row => withLedgerTooltip(row, ledgerDisplayTitle(row)) },
  { title: '原因', key: 'reason', minWidth: 360, ellipsis: { tooltip: true }, render: row => withLedgerTooltip(row, ledgerReasonDisplay(row)) },
];


const applySharedConfig = (data = {}) => {
  Object.assign(sharedConfigForm, {
    p115_shared_resource_enabled: Boolean(data.p115_shared_resource_enabled),
    p115_shared_center_url: data.p115_shared_center_url || 'https://shared.55565576.xyz',
    p115_shared_resource_mode: 'rapid',
    p115_shared_disable_episode_transfer: Boolean(data.p115_shared_disable_episode_transfer),
    p115_shared_block_clean_version_transfer: Boolean(data.p115_shared_block_clean_version_transfer),
    p115_shared_block_short_drama_transfer: Boolean(data.p115_shared_block_short_drama_transfer),
    p115_shared_intro_enabled: Boolean(data.p115_shared_intro_enabled),
    p115_shared_auto_share_requests_enabled: Boolean(data.p115_shared_auto_share_requests_enabled),
    p115_shared_virtual_import_enabled: Boolean(data.p115_shared_virtual_import_enabled),
    p115_shared_virtual_auto_promote_episodes: Number(data.p115_shared_virtual_auto_promote_episodes || 0),
    p115_shared_virtual_auto_promote_movie_percent: Number(data.p115_shared_virtual_auto_promote_movie_percent || 0),
    p115_shared_center_home_sections: normalizeCenterHomeSections(data.p115_shared_center_home_sections),
  });
};

const loadSharedConfig = async () => {
  sharedConfigLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/config');
    applySharedConfig(res.data?.data || {});
  } catch (e) {
    message.error(e.response?.data?.message || '加载共享资源配置失败');
  } finally {
    sharedConfigLoading.value = false;
  }
};

const loadCenterDeviceStatus = async () => {
  centerDeviceStatusLoading.value = true;
  centerDeviceStatusError.value = '';
  try {
    const res = await axios.get('/api/shared/resources/center/device/status');
    centerDeviceStatusData.value = res.data?.data || res.data || {};
  } catch (e) {
    centerDeviceStatusData.value = e.response?.data?.data || {};
    centerDeviceStatusError.value = e.response?.data?.message || '读取中心设备状态失败';
  } finally {
    centerDeviceStatusLoading.value = false;
  }
};

const openSharedConfigModal = async () => {
  showSharedConfigModal.value = true;
  await loadSharedConfig();
  await loadCenterDeviceStatus();
};

const saveSharedConfig = async () => {
  sharedConfigSaving.value = true;
  try {
    sharedConfigForm.p115_shared_resource_mode = 'rapid';
    const res = await axios.post('/api/shared/resources/config', { ...sharedConfigForm });
    applySharedConfig(res.data?.data || sharedConfigForm);
    message.success(res.data?.message || '共享资源配置已保存');
    showSharedConfigModal.value = false;
    await Promise.allSettled([loadSummary(), loadCenterDeviceStatus()]);
  } catch (e) {
    message.error(e.response?.data?.message || '保存共享资源配置失败');
  } finally {
    sharedConfigSaving.value = false;
  }
};

const loadSummary = async () => { const res = await axios.get('/api/shared/resources/summary'); summary.value = res.data?.data || { shares: {}, credit: {} }; };
const loadShares = async () => { sharesLoading.value = true; try { const res = await axios.get('/api/shared/resources/shares', { params: { ...shareFilters, page: sharePagination.page, page_size: sharePagination.pageSize } }); shareItems.value = res.data?.items || []; sharePagination.itemCount = Number(res.data?.total || 0); } catch (e) { message.error(e.response?.data?.message || '加载我的共享源失败'); } finally { sharesLoading.value = false; } };
const loadVirtualImports = async () => {
  virtualLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/virtual-imports', {
      params: { ...virtualFilters, page: virtualPagination.page, page_size: virtualPagination.pageSize },
    });
    virtualItems.value = res.data?.items || [];
    virtualPagination.itemCount = Number(res.data?.total || 0);
  } catch (e) {
    message.error(e.response?.data?.message || '加载虚拟入库失败');
  } finally {
    virtualLoading.value = false;
  }
};
const dismissVirtualImport = (row) => {
  dialog.warning({
    title: '辞退虚拟资源',
    content: `确定辞退《${row.title || row.source_id || '该资源'}》吗？本地虚拟 STRM 和媒体信息文件会被移除。`,
    positiveText: '辞退',
    negativeText: '保留',
    onPositiveClick: async () => {
      try {
        const res = await axios.delete(`/api/shared/resources/virtual-imports/${row.id}`);
        message.success(res.data?.message || '已辞退');
        await Promise.allSettled([loadVirtualImports(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '辞退失败');
      }
    },
  });
};
const promoteVirtualImport = (row) => {
  dialog.info({
    title: '虚拟资源转正',
    content: `确定将《${row.title || row.source_id || '该资源'}》转正吗？会执行正式秒传/转存。`,
    positiveText: '转正',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const res = await axios.post(`/api/shared/resources/virtual-imports/${row.id}/promote`, {});
        message.success(res.data?.message || '已转正');
        await Promise.allSettled([loadVirtualImports(), loadCenterSources(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '转正失败');
      }
    },
  });
};
const normalizeCenterHomeSection = (section = {}, index = 0) => ({
  key: String(section.key || `custom_${Date.now()}_${index}`).trim() || `custom_${Date.now()}_${index}`,
  title: String(section.title || '自定义列表').trim() || '自定义列表',
  display_type: ['all', 'movie', 'tv', 'series', 'season', 'pack'].includes(String(section.display_type || '').toLowerCase()) ? String(section.display_type).toLowerCase() : 'all',
  order_by: (() => {
    const value = String(section.order_by || 'pool_time').toLowerCase();
    return value === 'latest' ? 'pool_time' : (['pool_time', 'release_year', 'popular', 'size', 'name'].includes(value) ? value : 'pool_time');
  })(),
  genre_id: String(section.genre_id || '').trim(),
  tags: Array.isArray(section.tags)
    ? section.tags.map(tag => String(tag || '').trim()).filter(tag => /^[A-Za-z0-9_:-]{1,40}$/.test(tag))
    : String(section.status || '').split(',').map(tag => String(tag || '').trim()).filter(tag => /^[A-Za-z0-9_:-]{1,40}$/.test(tag) && !['alive', 'available'].includes(tag)),
  limit: Math.max(1, Math.min(Number(section.limit || 10), 20)),
  enabled: section.enabled !== false,
});
const normalizeCenterHomeSections = (sections) => {
  const list = Array.isArray(sections) && sections.length ? sections : CENTER_HOME_SECTION_DEFAULTS;
  return list.map((section, index) => normalizeCenterHomeSection(section, index));
};
const openCenterHomeSettingsModal = async () => {
  await Promise.allSettled([loadSharedConfig(), loadCenterHomeGenreOptions(), loadCenterHomeTagOptions()]);
  centerHomeSettingSections.value = normalizeCenterHomeSections(sharedConfigForm.p115_shared_center_home_sections).map(section => ({ ...section }));
  showCenterHomeSettingsModal.value = true;
};
const addCenterHomeSettingSection = () => {
  centerHomeSettingSections.value.push(normalizeCenterHomeSection({ key: `custom_${Date.now()}`, title: '自定义列表' }, centerHomeSettingSections.value.length));
};
const removeCenterHomeSettingSection = (index) => {
  centerHomeSettingSections.value.splice(index, 1);
};
const saveCenterHomeSettings = async () => {
  sharedConfigSaving.value = true;
  try {
    sharedConfigForm.p115_shared_center_home_sections = normalizeCenterHomeSections(centerHomeSettingSections.value);
    const res = await axios.post('/api/shared/resources/config', { ...sharedConfigForm });
    applySharedConfig(res.data?.data || sharedConfigForm);
    message.success('列表设置已保存');
    showCenterHomeSettingsModal.value = false;
    resetCenterSources(false).catch(e => message.error(e.response?.data?.message || '刷新中心资源库失败'));
  } catch (e) {
    message.error(e.response?.data?.message || '保存列表设置失败');
  } finally {
    sharedConfigSaving.value = false;
  }
};
const loadCenterSources = async (forceRefresh = false, append = false) => {
  if (append) centerAppendLoading.value = true;
  else centerLoading.value = true;
  try {
    if (!append) {
      centerPagination.page = 1;
      centerExpandedRowKeys.value = [];
      clearCenterChildrenLoading();
      centerHasMore.value = true;
    }
    if (!append && centerHomeMode.value) {
      const params = { limit_per_section: 10 };
      if (forceRefresh) params.force_refresh = 1;
      const res = await axios.get('/api/shared/resources/center/sources/home', { params });
      centerHomeSections.value = Array.isArray(res.data?.sections) ? res.data.sections : [];
      centerSources.value = [];
      centerBackendGrouped.value = true;
      centerPagination.itemCount = Number(res.data?.total || 0);
      centerHasMore.value = false;
      setupCenterInfiniteObserver();
      return;
    }
    centerHomeSections.value = [];
    const params = {
      keyword: centerFilters.keyword,
      limit: centerPagination.pageSize,
      offset: (centerPagination.page - 1) * centerPagination.pageSize,
    };
    if (forceRefresh) params.force_refresh = 1;
    const res = await axios.get('/api/shared/resources/center/sources', { params });
    const items = res.data?.items || [];
    centerBackendGrouped.value = Boolean(res.data?.backend_grouped);
    centerSources.value = append ? [...(centerSources.value || []), ...items] : items;
    centerPagination.itemCount = Number(res.data?.total || 0);
    const total = Number(centerPagination.itemCount || 0);
    centerHasMore.value = Boolean(items.length) && (total ? centerSources.value.length < total : items.length >= centerPagination.pageSize);
    setupCenterInfiniteObserver();
  } catch (e) {
    message.error(e.response?.data?.message || '加载中心资源库失败');
    if (append) centerPagination.page = Math.max(1, centerPagination.page - 1);
  } finally {
    centerLoading.value = false;
    centerAppendLoading.value = false;
  }
};

const resetCenterSources = async (forceRefresh = false) => {
  centerPagination.page = 1;
  await loadCenterSources(forceRefresh, false);
};

const loadMoreCenterSources = async () => {
  if (activeTab.value !== 'center' || centerLoading.value || centerAppendLoading.value || !centerHasMore.value) return;
  centerPagination.page += 1;
  await loadCenterSources(false, true);
};


const clearCenterChildrenLoading = () => {
  Object.keys(centerChildrenLoading).forEach(key => delete centerChildrenLoading[key]);
};

const collectCenterSourceIds = (row) => {
  const ids = [];
  const push = (value) => {
    const text = String(value || '').trim();
    if (text && !ids.includes(text) && !text.includes(':lazy-children')) ids.push(text);
  };
  const visit = (item) => {
    if (!item || typeof item !== 'object') return;
    (Array.isArray(item._merged_source_ids) ? item._merged_source_ids : []).forEach(push);
    push(item.source_id || item.source_ref_id || item.center_source_id);
  };
  visit(row);
  (Array.isArray(row?.versions) ? row.versions : []).forEach(visit);
  return ids.filter(id => !id.startsWith('hub_'));
};

const findCenterGroupByKey = (rows, key) => {
  const target = String(key || '');
  for (const row of rows || []) {
    if (centerTableRowKey(row) === target) return row;
    const found = findCenterGroupByKey(row?.children || [], target);
    if (found) return found;
  }
  return null;
};

const centerItemMatchesLazyTarget = (item, target, sourceIds, hubId, targetKey) => {
  if (!item || typeof item !== 'object') return false;
  if (centerTableRowKey(item) === targetKey || centerGroupKey(item) === targetKey) return true;
  const itemHub = String(item.hub_id || item.source_id || item.source_ref_id || '').trim();
  if (hubId && itemHub === hubId) return true;
  const itemIds = collectCenterSourceIds(item);
  if (String(item.source_id || item.source_ref_id || '').trim()) itemIds.push(String(item.source_id || item.source_ref_id).trim());
  return sourceIds.some(id => itemIds.includes(id));
};

const applyCenterLoadedChildren = (target, children, packItems) => {
  const sourceIds = collectCenterSourceIds(target);
  const hubId = String(target?.hub_id || (target?.source_kind === 'season_hub' ? (target.source_id || target.source_ref_id) : '') || '').trim();
  const targetKey = centerTableRowKey(target);
  const normalizedChildren = Array.isArray(children) ? children : [];
  const normalizedPackItems = Array.isArray(packItems) ? packItems : normalizedChildren;
  centerSources.value = (centerSources.value || []).map(item => {
    if (centerItemMatchesLazyTarget(item, target, sourceIds, hubId, targetKey)) {
      return {
        ...item,
        children: normalizedChildren,
        pack_items: normalizedPackItems,
        children_loaded: true,
        _center_children_loaded: true,
        has_children: normalizedChildren.length > 0,
      };
    }
    return item;
  });
};

const loadCenterSourceChildren = async (row) => {
  const existingChildren = Array.isArray(row?.children) ? row.children.filter(x => x && !centerIsLazyPlaceholder(x)) : [];
  const existingPackItems = Array.isArray(row?.pack_items) ? row.pack_items.filter(x => x && !centerIsLazyPlaceholder(x)) : [];
  if (!centerNeedsLoadChildren(row)) return { children: existingChildren, pack_items: existingPackItems };
  const key = centerTableRowKey(row);
  if (!key || centerChildrenLoading[key]) return { children: existingChildren, pack_items: existingPackItems };
  centerChildrenLoading[key] = true;
  try {
    const sourceKind = String(row?.source_kind || row?.lazy_children_kind || '').toLowerCase();
    const isHub = sourceKind === 'season_hub' || row?.is_ongoing_hub;
    const isLogical = centerIsLogicalSeasonRow(row);
    const sourceIds = collectCenterSourceIds(row);
    const params = {
      source_kind: isHub ? 'season_hub' : (isLogical ? 'logical_season' : sourceKind),
      source_id: isHub ? (row?.hub_id || row?.source_id || row?.source_ref_id || '') : (row?.logical_group_id || row?.group_id || sourceIds[0] || row?.source_id || row?.source_ref_id || ''),
      source_ids: isHub || isLogical ? '' : sourceIds.join(','),
      hub_id: row?.hub_id || '',
      limit: 5000,
    };
    const res = await axios.get('/api/shared/resources/center/sources/children', { params });
    const children = res.data?.children || res.data?.items || [];
    const packItems = res.data?.pack_items || children;
    applyCenterLoadedChildren(row, children, packItems);
    return { children, pack_items: packItems };
  } catch (e) {
    message.error(e.response?.data?.message || '加载季包集明细失败');
    return { children: existingChildren, pack_items: existingPackItems };
  } finally {
    delete centerChildrenLoading[key];
  }
};

const handleCenterExpandedRowKeys = async (keys) => {
  const oldKeys = new Set(centerExpandedRowKeys.value || []);
  centerExpandedRowKeys.value = keys || [];
  const newlyExpanded = (keys || []).filter(key => !oldKeys.has(key));
  for (const key of newlyExpanded) {
    const row = findCenterGroupByKey(groupedCenterSources.value || [], key);
    if (row && centerNeedsLoadChildren(row)) await loadCenterSourceChildren(row);
  }
};

const parseEpisodeText = (text) => {
  const out = [];
  String(text || '').split(/[，,\s]+/).filter(Boolean).forEach(part => {
    const m = part.match(/^(\d{1,4})\s*[-~]\s*(\d{1,4})$/);
    if (m) {
      let a = Number(m[1]); let b = Number(m[2]);
      if (a > b) [a, b] = [b, a];
      for (let n = a; n <= b && out.length < 200; n += 1) if (!out.includes(n)) out.push(n);
    } else {
      const n = Number(part);
      if (Number.isFinite(n) && n > 0 && !out.includes(n)) out.push(Math.floor(n));
    }
  });
  return out.sort((a, b) => a - b);
};

const compactRequestParams = () => {
  const params = {};
  Object.entries(shareRequestForm.params || {}).forEach(([key, value]) => {
    if (value == null) return;
    const text = String(value).trim();
    if (text) params[key] = text;
  });
  return params;
};

const buildShareRequestPayload = () => {
  return {
    tmdb_id: shareRequestForm.tmdb_id,
    media_type: shareRequestForm.media_type,
    target_type: shareRequestForm.target_type,
    title: shareRequestForm.title,
    release_year: shareRequestForm.release_year,
    poster_path: shareRequestForm.poster_path,
    overview: shareRequestForm.overview,
    season_number: shareRequestForm.target_type === 'season' ? shareRequestForm.season_number : null,
    params_json: compactRequestParams(),
    expires_days: shareRequestForm.expires_days || 7,
    auto_escalation: Boolean(shareRequestForm.auto_escalation),
    escalation_interval_hours: shareRequestForm.escalation_interval_hours || 24,
  };
};

let quoteTimer = null;
const refreshShareRequestQuote = async () => {
  if (!selectedShareRequestMedia.value) return;
  try {
    const payload = buildShareRequestPayload();
    const res = await axios.post('/api/shared/resources/share-requests/quote', payload);
    const q = res.data?.data || null;
    shareRequestQuote.value = q;
  } catch (e) {
    // 报价失败不弹爆，只在提交时提示。
    console.warn('share request quote failed', e);
  }
};
const scheduleShareRequestQuote = () => {
  clearTimeout(quoteTimer);
  quoteTimer = setTimeout(refreshShareRequestQuote, 260);
};

const resetShareRequestForm = () => {
  selectedShareRequestMedia.value = null;
  shareRequestSearchItems.value = [];
  shareRequestSearchKeyword.value = '';
  shareRequestEpisodeText.value = '';
  shareRequestQuote.value = null;
  Object.assign(shareRequestForm, {
    tmdb_id: '', media_type: 'movie', target_type: 'movie', title: '', release_year: null,
    poster_path: '', overview: '', season_number: 1, episode_number: 1,
    params: defaultShareRequestParams(), expires_days: 7, auto_escalation: false,
    escalation_interval_hours: 24,
  });
};

const openShareRequestModal = () => {
  resetShareRequestForm();
  showShareRequestModal.value = true;
};

const handleShareRequestCreated = async () => {
  activeTab.value = 'requests';
  await Promise.allSettled([loadShareRequests(), loadSummary(), loadLedger()]);
};

const searchShareRequestTmdb = async () => {
  const keyword = String(shareRequestSearchKeyword.value || '').trim();
  if (!keyword) return message.warning('请输入要搜索的片名');
  shareRequestSearchLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/share-requests/tmdb/search', { params: { keyword } });
    shareRequestSearchItems.value = res.data?.items || [];
    if (!shareRequestSearchItems.value.length) message.info('TMDb 没有搜索到结果');
  } catch (e) {
    message.error(e.response?.data?.message || 'TMDb 搜索失败');
  } finally {
    shareRequestSearchLoading.value = false;
  }
};

const chooseShareRequestMedia = async (row) => {
  selectedShareRequestMedia.value = row;
  const mediaType = row.media_type === 'movie' ? 'movie' : 'tv';
  Object.assign(shareRequestForm, {
    tmdb_id: row.tmdb_id || '',
    media_type: mediaType,
    target_type: mediaType === 'movie' ? 'movie' : 'season',
    title: row.title || '',
    release_year: row.release_year || null,
    poster_path: row.poster_path || '',
    overview: row.overview || '',
    season_number: 1,
    episode_number: 1,
  });
  shareRequestEpisodeText.value = '';
  await refreshShareRequestQuote();
  message.success('已选择求共享目标');
};

const loadShareRequests = async () => {
  requestLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/share-requests', { params: {
      keyword: requestFilters.keyword,
      status: requestFilters.status,
      media_type: requestFilters.media_type === 'all' ? '' : requestFilters.media_type,
      target_type: requestFilters.target_type === 'all' ? '' : requestFilters.target_type,
      limit: 80,
      offset: 0,
    } });
    const payload = (res.data?.data && typeof res.data.data === 'object') ? res.data.data : res.data;
    shareRequests.value = Array.isArray(payload?.items) ? payload.items : [];
  } catch (e) {
    message.error(e.response?.data?.message || '加载求共享失败');
  } finally {
    requestLoading.value = false;
  }
};

const submitShareRequest = async () => {
  if (!selectedShareRequestMedia.value) return message.warning('请先搜索并选择 TMDb 目标');
  if (shareRequestForm.media_type === 'tv' && shareRequestForm.target_type === 'season' && !shareRequestForm.season_number) {
    return message.warning('请填写季号');
  }
  shareRequestSubmitting.value = true;
  try {
    const payload = buildShareRequestPayload();
    const res = await axios.post('/api/shared/resources/share-requests', payload);
    message.success(res.data?.message || '求共享已发布');
    showShareRequestModal.value = false;
    activeTab.value = 'requests';
    await Promise.allSettled([loadShareRequests(), loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || '发布求共享失败');
  } finally {
    shareRequestSubmitting.value = false;
  }
};

const confirmCoRequest = (row) => {
  const cost = Number(row.max_bounty || row.current_bounty || row.bounty_total || 0);
  dialog.warning({
    title: '同求助力',
    content: `助力求共享将冻结 ${cost} 贡献点。资源成功共享并秒传后，对应贡献点会支付给共享者；未成交取消/过期会退回。确定同求吗？`,
    positiveText: '确认同求',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        const res = await axios.post(`/api/shared/resources/share-requests/${row.group_id}/co-request`, {});
        message.success(res.data?.message || '同求成功');
        await Promise.allSettled([loadShareRequests(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '同求失败');
      }
    }
  });
};

const confirmCancelShareRequest = (row) => {
  dialog.warning({
    title: '取消求共享',
    content: row.my_role === 'owner' ? '发起人取消会关闭该求共享并退回所有参与者未使用贡献点，确定继续吗？' : '确定取消你的同求并退回未使用贡献点吗？',
    positiveText: '取消求共享',
    negativeText: '保留',
    onPositiveClick: async () => {
      try {
        const res = await axios.post(`/api/shared/resources/share-requests/${row.group_id}/cancel`, {});
        message.success(res.data?.message || '已取消求共享');
        await Promise.allSettled([loadShareRequests(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '取消求共享失败');
      }
    }
  });
};

const openLocalShareForRequest = async (row) => {
  activeCenterReplenishSource.value = null;
  resetManualShareForm();
  mediaCandidates.value = [];
  activeLocalShareRequest.value = row || null;
  mediaSearchKeyword.value = '';
  showManualShareModal.value = true;
  message.info('正在自动匹配本地符合条件的可共享资源。');
  await searchShareableMedia();
};

const triggerSharedMaintenance = async () => {
  maintenanceSubmitting.value = true;
  try {
    const res = await axios.post('/api/tasks/run', {
      task_name: 'shared-resource-maintenance'
    });
    message.success(res.data?.message || '共享资源维护任务已提交');
  } catch (e) {
    message.error(e.response?.data?.error || e.response?.data?.message || '提交维护任务失败');
  } finally {
    maintenanceSubmitting.value = false;
  }
};

const loadLedger = async () => {
  ledgerLoading.value = true;
  try {
    const res = await axios.get('/api/shared/resources/credit/ledger', {
      params: { limit: 200, actual_only: 1, sync_center: 1 },
    });
    ledgerItems.value = res.data?.items || [];
  } catch {
    message.error('加载贡献点流水失败');
  } finally {
    ledgerLoading.value = false;
  }
};
const loadAll = async (forceRefresh = false) => {
  await loadSharedConfig();
  const tasks = [loadSummary(), loadLedger()];
  if (activeTab.value === 'center') tasks.push(resetCenterSources(forceRefresh));
  else if (activeTab.value === 'virtual') tasks.push(loadVirtualImports());
  else if (activeTab.value === 'requests') tasks.push(loadShareRequests());
  else tasks.push(loadShares());
  await Promise.allSettled(tasks);
};
const handleTabChange = async (name) => {
  if (name === 'shares') loadShares();
  if (name === 'center') {
    await resetCenterSources();
    await nextTick();
    setupCenterInfiniteObserver();
  }
  if (name === 'virtual') loadVirtualImports();
  if (name === 'requests') loadShareRequests();
  if (name === 'ledger') loadLedger();
};

let centerInfiniteObserver = null;
const disconnectCenterInfiniteObserver = () => {
  if (centerInfiniteObserver) {
    centerInfiniteObserver.disconnect();
    centerInfiniteObserver = null;
  }
};
const setupCenterInfiniteObserver = () => {
  disconnectCenterInfiniteObserver();
  const target = centerInfiniteSentinel.value;
  if (!target) return;
  centerInfiniteObserver = new IntersectionObserver((entries) => {
    if (entries[0]?.isIntersecting) loadMoreCenterSources();
  }, { root: null, rootMargin: '560px 0px', threshold: 0.01 });
  centerInfiniteObserver.observe(target);
};

const registerCenterDevice = async () => {
  registeringDevice.value = true;
  try {
    const res = await axios.post('/api/shared/resources/center/device/register', {});
    message.success(res.data?.message || (centerDeviceId.value ? '共享资源中心已重新连接' : '共享资源中心已连接'));
    await Promise.allSettled([loadSharedConfig(), loadSummary(), loadLedger(), loadCenterSources()]);
  } catch (e) {
    message.error(e.response?.data?.message || '注册/恢复中心设备失败');
  } finally {
    registeringDevice.value = false;
  }
};

const refreshCredit = async () => {
  if (needsCenterServerId.value) {
    message.warning('共享资源中心未连接，请先连接中心。');
    return;
  }
  refreshingCredit.value = true;
  try {
    await axios.post('/api/shared/resources/credit/refresh');
    message.success('贡献点已同步');
    await Promise.allSettled([loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || '刷新贡献点失败');
  } finally {
    refreshingCredit.value = false;
  }
};

const resetManualShareForm = () => {
  manualShareValidationSeq += 1;
  manualShareValidation.value = null;
  manualShareValidationLoading.value = false;
  Object.assign(manualShareForm, {
    root_fid: '', root_name: '', root_is_dir: true, title: '', tmdb_id: '', parent_series_tmdb_id: '',
    share_type: 'movie_folder', item_type: 'Movie', season_number: 1, release_year: null, receive_code: manualShareForm.receive_code || '',
    season_status: '', watching_status: '', expected_episode_count: null, total_episodes: null, is_completed: false,
    center_replenish_source_id: '', center_replenish_payload: null
  });
  selectedMedia.value = null;
};

const openManualShareModal = () => {
  activeLocalShareRequest.value = null;
  activeCenterReplenishSource.value = null;
  resetManualShareForm();
  mediaCandidates.value = [];
  mediaSearchKeyword.value = '';
  showManualShareModal.value = true;
};

const searchShareableMedia = async () => {
  const requestRow = activeLocalShareRequest.value || null;
  const keyword = requestRow
    ? String(requestRow.title || requestRow.tmdb_id || '').trim()
    : String(mediaSearchKeyword.value || '').trim();
  if (!keyword) return message.warning(requestRow ? '求共享缺少片名或 TMDb ID，无法自动匹配本地资源' : '请输入片名或 TMDb ID');
  mediaSearchLoading.value = true;
  try {
    const params = { keyword, limit: requestRow ? 100 : 30 };
    if (requestRow) Object.assign(params, shareRequestSearchFilterParams(requestRow));
    const res = await axios.get('/api/shared/resources/media/search', { params });
    mediaCandidates.value = res.data?.items || [];
    if (!mediaCandidates.value.length) {
      message.info(requestRow ? '本地没有符合该求共享参数的可共享资源' : '没有搜索到本地媒体记录');
    }
  } catch (e) {
    message.error(e.response?.data?.message || '搜索可共享媒体失败');
  } finally {
    mediaSearchLoading.value = false;
  }
};

const buildManualSharePayload = () => {
  const payload = { ...manualShareForm };
  if (activeLocalShareRequest.value) {
    payload.share_request_group_id = activeLocalShareRequest.value.group_id || '';
    payload.share_request_payload = {
      group_id: activeLocalShareRequest.value.group_id,
      tmdb_id: activeLocalShareRequest.value.tmdb_id,
      media_type: activeLocalShareRequest.value.media_type,
      target_type: activeLocalShareRequest.value.target_type,
      season_number: activeLocalShareRequest.value.season_number,
      episode_number: activeLocalShareRequest.value.episode_number,
      episode_numbers: activeLocalShareRequest.value.episode_numbers || [],
      params_json: activeLocalShareRequest.value.params_json || {},
    };
  }
  return payload;
};

const validateManualShareSelection = async () => {
  const seq = ++manualShareValidationSeq;
  manualShareValidation.value = null;
  if (!manualShareForm.root_fid) return null;
  manualShareValidationLoading.value = true;
  try {
    const res = await axios.post('/api/shared/resources/shares/manual-validate', buildManualSharePayload());
    if (seq !== manualShareValidationSeq) return null;
    const data = res.data?.data || {};
    manualShareValidation.value = {
      valid: data.valid === true,
      message: data.message || res.data?.message || (data.valid ? '校验通过' : '校验未通过'),
      file_count: data.file_count || 0,
      missing_raw: data.missing_raw || [],
      consistency: null,
      completed_consistency_gate: null,
      reason: data.reason || '',
    };
    return manualShareValidation.value;
  } catch (e) {
    if (seq !== manualShareValidationSeq) return null;
    manualShareValidation.value = {
      valid: false,
      message: e.response?.data?.message || '预校验失败，请稍后重试',
      file_count: 0,
      reason: e.response?.data?.data?.reason || '',
      consistency: null,
      completed_consistency_gate: null,
    };
    return manualShareValidation.value;
  } finally {
    if (seq === manualShareValidationSeq) manualShareValidationLoading.value = false;
  }
};

const chooseMediaCandidate = (row) => {
  if (!row?.resolvable || !row.root_fid) {
    return message.warning(row?.message || '该媒体暂时无法自动定位 115 目录/FID');
  }
  manualShareValidationSeq += 1;
  manualShareValidation.value = null;
  manualShareValidationLoading.value = false;
  selectedMedia.value = row;
  Object.assign(manualShareForm, {
    root_fid: row.root_fid || '',
    root_name: row.root_name || '',
    root_is_dir: row.root_is_dir !== false,
    title: row.standard_title || row.series_title || row.title || row.display_title || row.root_name || '',
    tmdb_id: row.share_tmdb_id || row.tmdb_id || '',
    parent_series_tmdb_id: row.parent_series_tmdb_id || '',
    share_type: row.share_type || 'movie_folder',
    item_type: row.share_item_type || row.item_type || 'Movie',
    season_number: row.season_number || null,
    release_year: row.release_year || null,
    season_status: row.watching_status || row.season_status || '',
    watching_status: row.watching_status || '',
    expected_episode_count: row.total_episodes || row.expected_episode_count || row.episode_count || null,
    total_episodes: row.total_episodes || row.expected_episode_count || row.episode_count || null,
    is_completed: String(row.watching_status || '').trim().toLowerCase() === 'completed',
    center_replenish_source_id: row.center_replenish_source_id || '',
    center_replenish_payload: row.center_replenish_payload || null,
  });
  message.success('已自动填充共享信息，开始预校验');
  validateManualShareSelection();
};


const confirmShareAllLibrary = () => {
  dialog.warning({
    title: '一键登记媒体库',
    content: '将扫描当前本机媒体库并把可秒传资源索引登记到共享中心。不会创建 115 共享，也不会上传 CK；但会上传 SHA1、大小、文件名和媒体信息摘要。媒体库很大时会在后台运行较长时间，确认继续吗？',
    positiveText: '开始共享全库',
    negativeText: '取消',
    onPositiveClick: async () => {
      shareAllLoading.value = true;
      try {
        const res = await axios.post('/api/tasks/run', {
          task_name: 'share-all-library'
        });
        message.success(res.data?.message || '一键登记媒体库任务已提交');
      } catch (e) {
        message.error(e.response?.data?.error || e.response?.data?.message || '启动一键登记媒体库失败');
      } finally {
        shareAllLoading.value = false;
      }
    }
  });
};

const manualCreateShare = async () => {
  if (!manualShareForm.root_fid) return message.warning('请先搜索并选择一个可共享媒体');
  if (manualShareValidationLoading.value) return message.warning('正在预校验共享源，请稍候');
  if (!manualShareValidation.value || manualShareValidation.value.valid !== true) {
    const result = await validateManualShareSelection();
    if (!result || result.valid !== true) {
      return message.error(result?.message || '预校验未通过，不能登记共享源');
    }
  }
  manualCreating.value = true;
  try {
    const payload = buildManualSharePayload();
    await axios.post('/api/shared/resources/shares/manual-create', payload);
    message.success('资源索引已登记中心');
    showManualShareModal.value = false;
    activeLocalShareRequest.value = null;
    activeCenterReplenishSource.value = null;
    activeTab.value = 'shares';
    await Promise.allSettled([loadShares(), loadCenterSources(), loadSummary(), loadLedger()]);
  } catch (e) {
    message.error(e.response?.data?.message || '登记资源失败');
  } finally { manualCreating.value = false; }
};


const reregisterShare = (row) => {
  const ids = Array.isArray(row.source_ids) ? row.source_ids.filter(Boolean) : [];
  const isBatch = ids.length > 1;
  const title = row.title || row.root_name || row.file_name || '该资源';
  const countText = isBatch ? `该聚合项下 ${ids.length} 个本机源` : '该本机源';
  dialog.warning({
    title: '重新登记共享源',
    content: `确定重新登记《${title}》的${countText}吗？系统会重新上传 RAW/summary_json，并向中心重新登记；可用于修复中心 RAW 缺失导致的不可用状态。`,
    positiveText: '重新登记',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        if (isBatch) {
          await axios.post('/api/shared/resources/shares/reregister-batch', { ids });
        } else {
          await axios.post(`/api/shared/resources/shares/${row.id}/reregister`, {});
        }
        message.success('已重新登记共享源');
        await Promise.allSettled([loadShares(), loadCenterSources(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '重新登记失败');
      }
    }
  });
};

const cancelLogicalSeasonShare = () => message.warning('逻辑季分享由中心端维护');


const cancelShare = (row) => {
  if (isAutoShareRow(row)) {
    return message.warning('自动共享源由入库自动维护，不能手动停用');
  }
  const ids = Array.isArray(row.source_ids) ? row.source_ids.filter(Boolean) : [];
  const isBatch = ids.length > 1;
  const title = row.title || row.root_name || row.file_name || '该资源';
  const countText = isBatch ? `该聚合项下 ${ids.length} 个本机源` : '该本机源';
  dialog.warning({
    title: '停用共享源',
    content: `确定停用《${title}》的${countText}吗？停用后不会再向中心供给这些资源。`,
    positiveText: '停用共享',
    negativeText: '保留',
    onPositiveClick: async () => {
      try {
        if (isBatch) {
          await axios.post('/api/shared/resources/shares/cancel-batch', { ids });
        } else {
          await axios.post(`/api/shared/resources/shares/${row.id}/cancel`);
        }
        message.success('已停用共享源');
        await Promise.allSettled([loadShares(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '停用失败');
      }
    }
  });
};


const deleteShare = (row) => {
  const ids = Array.isArray(row.source_ids) ? row.source_ids.filter(Boolean) : [];
  const isBatch = ids.length > 1;
  const title = row.title || row.root_name || row.file_name || '该资源';
  const alreadyDisabled = isInactiveShareRow(row) || isProblemShareRow(row);
  const countText = isBatch ? `该聚合项下 ${ids.length} 个本机源` : '该本机源';
  dialog.warning({
    title: '删除共享源',
    content: alreadyDisabled
      ? `确定彻底删除《${title}》的${countText}本地记录吗？该资源已停用或已不可用，不会再请求中心。`
      : `确定删除《${title}》的${countText}吗？有效共享会先同步中心取消登记，成功后再删除本地数据。`,
    positiveText: alreadyDisabled ? '删除本地数据' : '取消登记并删除',
    negativeText: '保留',
    onPositiveClick: async () => {
      try {
        if (isBatch) {
          await axios.post('/api/shared/resources/shares/delete-batch', { ids });
        } else {
          await axios.post(`/api/shared/resources/shares/${row.id}/delete`);
        }
        message.success(alreadyDisabled ? '已删除本地共享记录' : '已取消登记并删除共享记录');
        await Promise.allSettled([loadShares(), loadCenterSources(), loadSummary(), loadLedger()]);
      } catch (e) {
        message.error(e.response?.data?.message || '删除失败');
      }
    }
  });
};


watch(
  () => [
    selectedShareRequestMedia.value?.tmdb_id,
    shareRequestForm.target_type,
    shareRequestForm.season_number,
    shareRequestForm.episode_number,
    shareRequestEpisodeText.value,
    shareRequestForm.params.resolution,
    shareRequestForm.params.codec,
    shareRequestForm.params.effect,
    shareRequestForm.params.frame_rate,
    shareRequestForm.params.audio,
    shareRequestForm.params.subtitle,
    shareRequestForm.params.size_range,
    shareRequestForm.auto_escalation,
    shareRequestForm.escalation_interval_hours,
  ],
  () => scheduleShareRequestQuote(),
);

onMounted(() => {
  checkMobile();
  window.addEventListener('resize', checkMobile);
  loadAll();
  nextTick(setupCenterInfiniteObserver);
});
onUnmounted(() => {
  window.removeEventListener('resize', checkMobile);
  disconnectCenterInfiniteObserver();
});
</script>

<style scoped>
.shared-page { padding: 0; }
.dashboard-card { border-radius: 14px; overflow: hidden; }
.shared-list-card { overflow: visible; }
.shared-list-card :deep(.n-card__content),
.shared-list-card :deep(.n-tabs),
.shared-list-card :deep(.n-tab-pane),
.share-request-spin,
.share-request-spin :deep(.n-spin-container),
.share-request-spin :deep(.n-spin-content) { overflow: visible; }
.page-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.page-title { font-size: 20px; font-weight: 700; margin-bottom: 6px; }
.card-title { font-size: 16px; font-weight: 700; }
.stat-grid { width: 100%; }
.stat-card { height: 100%; box-sizing: border-box; background: rgba(128,128,128,0.08); border-radius: 12px; padding: 14px 16px; min-height: 88px; }
.stat-label { font-size: 12px; opacity: .65; margin-bottom: 8px; }
.stat-value { font-size: 24px; font-weight: 700; line-height: 1; }
.stat-desc { margin-top: 8px; font-size: 12px; opacity: .65; }
.toolbar { margin-bottom: 14px; }
.main-title { font-weight: 600; }
.center-title-cell {
  max-width: 220px;
  min-width: 0;
}
.center-title-ellipsis {
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: help;
}

.sub-title {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  font-size: 12px;
  color: var(--n-text-color-3, rgba(128,128,128,.78));
  margin-top: 5px;
  opacity: 1;
}
.tmdb-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 650;
  line-height: 18px;
  color: var(--tmdb-color, #e91e63) !important;
  background: rgba(233, 30, 99, .10);
  background: color-mix(in srgb, currentColor 11%, transparent);
  border: 1px solid rgba(233, 30, 99, .26);
  border-color: color-mix(in srgb, currentColor 28%, transparent);
  text-decoration: none !important;
  cursor: pointer;
  user-select: none;
  vertical-align: middle;
  transition: background-color .16s ease, border-color .16s ease, color .16s ease, transform .16s ease;
}
.tmdb-pill:hover {
  color: var(--tmdb-color-hover, var(--tmdb-color, #e91e63)) !important;
  background: rgba(233, 30, 99, .16);
  background: color-mix(in srgb, currentColor 17%, transparent);
  border-color: rgba(233, 30, 99, .42);
  border-color: color-mix(in srgb, currentColor 46%, transparent);
  transform: translateY(-1px);
}
.tmdb-pill:active { transform: translateY(0); }
.tmdb-pill:focus-visible {
  outline: 2px solid var(--tmdb-color, #e91e63);
  outline-offset: 2px;
}
.tmdb-pill-label { opacity: .74; letter-spacing: .02em; }
.tmdb-pill-id { font-variant-numeric: tabular-nums; }
.pre-line { white-space: pre-line; line-height: 1.55; }
.selected-share-box { border: 1px solid rgba(128,128,128,.22); border-radius: 12px; padding: 12px 14px; background: rgba(128,128,128,.06); }
.selected-title { font-weight: 700; margin-bottom: 6px; }
.selected-desc { font-size: 12px; opacity: .68; line-height: 1.7; }
.share-validation-alert { margin-top: 10px; }


/* 中心资源库海报墙：卡面只保留海报、片名年份季号和状态缎带，详情点开再看 */
.center-card-grid {
  grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(142px * var(--card-scale, 1))), 1fr));
  gap: calc(18px * var(--card-scale, 1));
  padding: calc(10px * var(--card-scale, 1)) 0 calc(20px * var(--card-scale, 1));
}
.center-home-section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.center-home-setting-list { display: flex; flex-direction: column; gap: 8px; }
.center-home-setting-item {
  display: grid;
  grid-template-columns: 24px minmax(150px, 1.4fr) 96px 150px minmax(180px, 1.4fr) 96px 82px 68px 32px;
  gap: 8px;
  align-items: center;
  padding: 8px;
  border: 1px solid rgba(128,128,128,.18);
  border-radius: 8px;
  background: rgba(128,128,128,.055);
}
.center-home-setting-drag { cursor: grab; color: var(--n-text-color-3, rgba(128,128,128,.72)); }
.center-home-setting-drag:active { cursor: grabbing; }
.center-home-setting-title-input,
.center-home-setting-select,
.center-home-setting-tags,
.center-home-setting-limit { min-width: 0; }
.poster-wall-card {
  border-radius: calc(12px * var(--card-scale, 1)) !important;
  background: rgba(9, 16, 42, .78) !important;
  box-shadow: 0 10px 24px rgba(0, 0, 0, .22);
}
.poster-wall-wrapper {
  border-radius: calc(12px * var(--card-scale, 1));
  background: #0b1230 url('/default-poster.png') center / cover no-repeat;
}
.poster-wall-wrapper::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(to top, rgba(0, 0, 0, .86) 0%, rgba(0, 0, 0, .44) 34%, rgba(0, 0, 0, .04) 70%);
  pointer-events: none;
}
.poster-wall-overlay {
  min-height: 34%;
  padding: calc(54px * var(--card-scale, 1)) calc(8px * var(--card-scale, 1)) calc(9px * var(--card-scale, 1));
  background: linear-gradient(to top, rgba(0,0,0,.94) 0%, rgba(0,0,0,.58) 54%, rgba(0,0,0,0) 100%);
  justify-content: flex-start;
  z-index: 1;
}
.poster-wall-title-line {
  max-width: 100%;
  font-size: calc(13px * var(--card-scale, 1));
  line-height: 1.25;
  font-weight: 800;
  color: #fff;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  text-shadow: 0 2px 5px rgba(0,0,0,.75);
}
.poster-wall-year-line {
  margin-top: calc(2px * var(--card-scale, 1));
  font-size: calc(12px * var(--card-scale, 1));
  line-height: 1.15;
  font-weight: 700;
  color: rgba(255,255,255,.86);
  text-shadow: 0 2px 5px rgba(0,0,0,.75);
}

@media (max-width: 768px) { .page-header { flex-direction: column; } }
@media (max-width: 768px) {
  .center-home-setting-item {
    grid-template-columns: 24px 1fr auto;
  }
  .center-home-setting-title-input,
  .center-home-setting-select,
  .center-home-setting-tags,
  .center-home-setting-limit {
    grid-column: 2 / -1;
  }
}

.share-request-grid {
  box-sizing: border-box;
  width: 100%;
  padding: calc(6px * var(--card-scale, 1)) calc(12px * var(--card-scale, 1)) calc(22px * var(--card-scale, 1));
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(330px * var(--card-scale, 1))), 1fr));
  gap: calc(12px * var(--card-scale, 1));
}
.share-request-card {
  height: 100%;
  min-height: calc(174px * var(--card-scale, 1));
  background: rgba(128,128,128,.055);
  border-radius: calc(14px * var(--card-scale, 1));
  overflow: hidden;
  font-size: calc(13px * var(--card-scale, 1));
}
.share-request-card :deep(.n-card__content) {
  padding: calc(10px * var(--card-scale, 1)) !important;
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: calc(8px * var(--card-scale, 1));
}
.share-request-card :deep(.n-button),
.share-request-card :deep(.n-tag) { font-size: inherit !important; }
.share-request-card-body { display: flex; gap: calc(10px * var(--card-scale, 1)); min-height: 0; flex: 1; }
.share-request-poster {
  width: calc(66px * var(--card-scale, 1));
  height: calc(96px * var(--card-scale, 1));
  object-fit: cover;
  border-radius: calc(9px * var(--card-scale, 1));
  background: rgba(128,128,128,.16);
  flex: 0 0 auto;
}
.share-request-info { min-width: 0; flex: 1; }
.share-request-title {
  font-size: 1.08em;
  font-weight: 700;
  line-height: 1.28;
  margin-bottom: 3px;
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
}
.share-request-meta, .share-request-condition, .share-request-time {
  font-size: .88em;
  color: var(--n-text-color-3, rgba(128,128,128,.78));
  line-height: 1.45;
}
.share-request-condition {
  margin-top: 3px;
  min-height: 1.35em;
  word-break: break-all;
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
}
.share-request-tags { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }
.share-request-time { margin-top: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.share-request-footer { display: flex; align-items: center; justify-content: space-between; gap: 8px; border-top: 1px solid rgba(128,128,128,.12); padding-top: 8px; }
.share-request-owner { color: var(--n-text-color-3, rgba(128,128,128,.78)); font-size: .88em; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.share-request-actions { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; justify-content: flex-end; }
.empty-request-card { text-align: center; padding: 24px; background: rgba(128,128,128,.055); border-radius: 14px; }
.share-request-quote-box { border: 1px solid rgba(128,128,128,.20); border-radius: 12px; padding: 12px 14px; background: rgba(128,128,128,.065); margin-top: 12px; }
.quote-title { font-weight: 700; margin-bottom: 8px; }
.quote-breakdown { display: flex; flex-wrap: wrap; gap: 8px; }
.quote-chip { display: inline-flex; align-items: center; padding: 3px 8px; border-radius: 999px; font-size: 12px; background: rgba(128,128,128,.13); }

.warning-text { color: #d03050; font-size: 12px; }
.share-remark-text { display: inline-block; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: middle; }
.share-remark-error { color: #d03050; }



/* 共享资源管理：表格玻璃化 */
.shared-page :deep(.n-data-table) {
  --n-th-color: rgba(255, 255, 255, 0.045) !important;
  --n-td-color: transparent !important;
  --n-td-color-hover: rgba(255, 255, 255, 0.055) !important;
  --n-border-color: rgba(148, 177, 255, 0.11) !important;
  --n-merged-border-color: rgba(148, 177, 255, 0.11) !important;
  background: transparent !important;
}

/* 表格外壳 */
.shared-page :deep(.n-data-table-wrapper),
.shared-page :deep(.n-data-table-base-table),
.shared-page :deep(.n-data-table-base-table-body),
.shared-page :deep(.n-data-table-table) {
  background: transparent !important;
}

/* 表头 */
.shared-page :deep(.n-data-table-th) {
  background-color: rgba(255, 255, 255, 0.045) !important;
  border-color: rgba(148, 177, 255, 0.11) !important;
}

/* 单元格 */
.shared-page :deep(.n-data-table-td) {
  background-color: transparent !important;
  border-color: rgba(148, 177, 255, 0.11) !important;
}

/* hover 行 */
.shared-page :deep(.n-data-table-tr:hover .n-data-table-td) {
  background-color: rgba(255, 255, 255, 0.055) !important;
}

/* 空数据 / loading 区域 */
.shared-page :deep(.n-data-table-empty),
.shared-page :deep(.n-data-table-loading) {
  background: transparent !important;
}

/* 分页区域也别实心 */
.shared-page :deep(.n-data-table .n-pagination) {
  background: transparent !important;
}

/* 弹窗里的 n-data-table 也玻璃化 */
.custom-modal .n-data-table {
  --n-th-color: rgba(255, 255, 255, 0.045) !important;
  --n-td-color: transparent !important;
  --n-td-color-hover: rgba(255, 255, 255, 0.055) !important;
  --n-border-color: rgba(148, 177, 255, 0.11) !important;
  --n-merged-border-color: rgba(148, 177, 255, 0.11) !important;
  background: transparent !important;
}

.custom-modal .n-data-table-wrapper,
.custom-modal .n-data-table-base-table,
.custom-modal .n-data-table-base-table-body,
.custom-modal .n-data-table-table,
.custom-modal .n-data-table-empty,
.custom-modal .n-data-table-loading {
  background: transparent !important;
}

.custom-modal .n-data-table-th {
  background-color: rgba(255, 255, 255, 0.045) !important;
}

.custom-modal .n-data-table-td {
  background-color: transparent !important;
}

.custom-modal .n-data-table-tr:hover .n-data-table-td {
  background-color: rgba(255, 255, 255, 0.055) !important;
}


.ledger-tooltip-trigger {
  display: inline-flex;
  max-width: 100%;
  align-items: center;
  cursor: help;
}
.ledger-detail-tooltip {
  max-width: 740px;
  line-height: 1.55;
  white-space: normal;
}
.ledger-detail-title {
  font-weight: 700;
  margin-bottom: 8px;
}
.ledger-detail-item {
  padding: 6px 0;
  border-top: 1px solid rgba(255, 255, 255, .12);
}
.ledger-detail-meta {
  font-size: 12px;
  opacity: .86;
  margin-bottom: 3px;
}
.ledger-detail-reason {
  font-size: 12px;
  opacity: .72;
  word-break: break-all;
}


/* 中心资源库：海报卡片 + 无限滚动 */
.center-card-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(148px * var(--card-scale, 1))), 1fr));
  gap: calc(16px * var(--card-scale, 1));
  padding: calc(8px * var(--card-scale, 1)) calc(2px * var(--card-scale, 1)) calc(18px * var(--card-scale, 1));
}
.center-card-item {
  min-width: 0;
  content-visibility: auto;
  contain-intrinsic-size: 222px 333px;
}
.center-media-card {
  cursor: pointer;
  border-radius: calc(12px * var(--card-scale, 1));
  overflow: hidden;
  height: 100%;
  background: rgba(12, 18, 42, .66) !important;
  border: 1px solid rgba(148, 177, 255, .14) !important;
  transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease;
}
.center-media-card:hover {
  transform: translateY(-4px);
  border-color: rgba(91, 140, 255, .45) !important;
  box-shadow: 0 14px 28px rgba(0,0,0,.28);
  z-index: 3;
}
.center-poster-wrapper {
  position: relative;
  width: 100%;
  aspect-ratio: 2 / 3;
  overflow: hidden;
  background: radial-gradient(circle at 20% 18%, rgba(75, 184, 255, .22), transparent 35%), linear-gradient(145deg, rgba(23, 43, 92, .92), rgba(18, 18, 52, .96));
}
.center-poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  transition: transform .28s ease;
}
.center-media-card:hover .center-poster { transform: scale(1.045); }
.center-card-overlay {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  min-height: 46%;
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: calc(8px * var(--card-scale, 1));
  padding: calc(56px * var(--card-scale, 1)) calc(8px * var(--card-scale, 1)) calc(8px * var(--card-scale, 1));
  background: linear-gradient(to top, rgba(0,0,0,.94) 0%, rgba(0,0,0,.68) 56%, rgba(0,0,0,0) 100%);
  pointer-events: none;
}
.center-card-text { min-width: 0; flex: 1; }
.center-version-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
}
.center-version-tags :deep(.n-tag) {
  max-width: none;
}
.center-version-tags :deep(.n-tag__content) {
  overflow: visible;
  text-overflow: clip;
  white-space: nowrap;
}
.center-ribbon {
  position: absolute;
  left: -31px;
  top: 10px;
  width: 106px;
  height: 22px;
  transform: rotate(-45deg);
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 11px;
  font-weight: 800;
  color: #fff;
  z-index: 2;
  box-shadow: 0 2px 8px rgba(0,0,0,.25);
}
.center-ribbon-green { background: linear-gradient(135deg, #22c55e, #16a34a); }
.center-ribbon-blue { background: linear-gradient(135deg, #38bdf8, #2563eb); }
.center-ribbon-warning { background: linear-gradient(135deg, #f59e0b, #d97706); }
.center-ribbon-dark { background: linear-gradient(135deg, #64748b, #334155); }
.center-empty-card {
  text-align: center;
  padding: 42px 12px;
  border-radius: 14px;
  background: rgba(128,128,128,.055);
}
.center-infinite-sentinel {
  min-height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 4px 0 2px;
}
.center-detail-body {
  --center-detail-title-color: var(--n-primary-color, var(--accent-color, var(--n-title-text-color, rgba(15, 23, 42, .94))));
  --center-detail-meta-color: var(--n-text-color-2, rgba(71, 85, 105, .86));
  --center-detail-text-color: var(--n-text-color, rgba(30, 41, 59, .9));
  --center-detail-muted-color: var(--n-text-color-3, rgba(100, 116, 139, .9));
  --center-detail-panel-bg: var(--n-color, var(--card-bg-color, transparent));
  --center-detail-soft-bg: var(--n-color-embedded, var(--n-color, var(--card-bg-color, transparent)));
  --center-detail-person-bg: var(--n-color-embedded, var(--n-color, var(--card-bg-color, transparent)));
  --center-detail-border: var(--n-border-color, var(--card-border-color, rgba(128, 128, 128, .24)));
  --center-detail-avatar-bg: var(--n-color-embedded, var(--n-color, rgba(128, 128, 128, .12)));
  --center-detail-shadow: var(--n-box-shadow, none);
}
.center-detail-body { display: flex; flex-direction: column; gap: 14px; }
.center-detail-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 14px;
  padding: 12px 14px;
  border-radius: 14px;
  background: var(--center-detail-soft-bg);
  border: 1px solid var(--center-detail-border);
}
.center-detail-title { font-size: 18px; font-weight: 800; line-height: 1.35; }
.center-detail-sub { margin-top: 4px; font-size: 12px; opacity: .68; }
.center-version-detail-list { display: flex; flex-direction: column; gap: 10px; }
.center-season-progress {
  padding: 10px 12px 12px;
  border-radius: 10px;
  background: var(--center-detail-soft-bg);
  border: 1px solid var(--center-detail-border);
}
.center-season-progress-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 8px;
  font-size: 12px;
  color: var(--center-detail-meta-color);
}
.center-season-progress-bar :deep(.n-progress-graph-line-fill) {
  position: relative;
  overflow: hidden;
  box-shadow: 0 0 14px color-mix(in srgb, var(--season-progress-color) 42%, transparent);
}
.center-season-progress-bar :deep(.n-progress-graph-line-fill)::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(110deg, transparent 0%, rgba(255,255,255,.18) 38%, rgba(255,255,255,.72) 50%, rgba(255,255,255,.18) 62%, transparent 100%);
  transform: translateX(-120%);
  animation: center-season-progress-marquee 1.15s linear infinite;
}
@keyframes center-season-progress-marquee {
  to { transform: translateX(120%); }
}
.center-version-detail-card {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  padding: 9px 12px;
  border-radius: 18px;
  background: var(--center-detail-person-bg);
  border: 1px solid var(--center-detail-border);
  box-shadow: var(--center-detail-shadow);
}
.center-version-main { min-width: 0; flex: 1; }
.center-version-title { font-weight: 800; line-height: 1.35; color: var(--center-detail-title-color); }
.center-version-tracks,
.center-version-episodes {
  margin-top: 7px;
  font-size: 12px;
  color: var(--n-text-color-3, rgba(128,128,128,.78));
  line-height: 1.55;
  word-break: break-all;
}
.center-version-action { flex: 0 0 auto; display: flex; align-items: center; gap: 8px; }
.center-version-detail-card-expandable { cursor: pointer; }
.center-version-detail-card-expandable:hover {
  border-color: var(--n-primary-color, var(--accent-color, var(--center-detail-border)));
}
.center-episode-matrix {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--center-detail-border);
}
.center-episode-matrix :deep(.n-button) { min-width: 34px; }
@media (max-width: 768px) {
  .center-card-grid { grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(118px * var(--card-scale, 1))), 1fr)); gap: calc(12px * var(--card-scale, 1)); }
  .center-detail-head,
  .center-version-detail-card { flex-direction: column; }
  .center-version-action { align-items: flex-start; }
}

/* 海报墙最终覆盖：保持影视探索式密集海报墙 */
.center-card-grid {
  grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(142px * var(--card-scale, 1))), 1fr));
  gap: calc(18px * var(--card-scale, 1));
  padding: calc(10px * var(--card-scale, 1)) 0 calc(20px * var(--card-scale, 1));
}
.poster-wall-card {
  border-radius: calc(12px * var(--card-scale, 1)) !important;
  background: rgba(9, 16, 42, .78) !important;
  box-shadow: 0 10px 24px rgba(0, 0, 0, .22);
}
.poster-wall-wrapper {
  border-radius: calc(12px * var(--card-scale, 1));
  background: #0b1230 url('/default-poster.png') center / cover no-repeat;
}
.poster-wall-wrapper::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(to top, rgba(0, 0, 0, .86) 0%, rgba(0, 0, 0, .44) 34%, rgba(0, 0, 0, .04) 70%);
  pointer-events: none;
}
.poster-wall-overlay {
  min-height: 34%;
  padding: calc(54px * var(--card-scale, 1)) calc(8px * var(--card-scale, 1)) calc(9px * var(--card-scale, 1));
  background: linear-gradient(to top, rgba(0,0,0,.94) 0%, rgba(0,0,0,.58) 54%, rgba(0,0,0,0) 100%);
  justify-content: flex-start;
  z-index: 1;
}
.poster-wall-title {
  font-size: calc(13px * var(--card-scale, 1));
  line-height: 1.25;
  -webkit-line-clamp: 3;
  line-clamp: 3;
}
@media (max-width: 768px) {
  .center-card-grid { grid-template-columns: repeat(auto-fill, minmax(min(100%, calc(118px * var(--card-scale, 1))), 1fr)); gap: calc(12px * var(--card-scale, 1)); }
}
/* 弹窗头部图文排版 */
.center-detail-header-new {
  display: flex;
  gap: 24px;
  margin-bottom: 4px;
}

.detail-poster {
  width: 160px;
  height: 240px;
  border-radius: 10px;
  object-fit: cover;
  box-shadow: 0 8px 22px rgba(0,0,0,0.36);
  flex: 0 0 160px;
  background-color: var(--center-detail-avatar-bg);
}
.detail-info {
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex: 1;
  min-width: 0;
}
.detail-title {
  font-size: 24px;
  font-weight: 800;
  line-height: 1.2;
  color: var(--center-detail-title-color);
}
.detail-year {
  font-size: 18px;
  font-weight: normal;
  opacity: 0.7;
  margin-left: 6px;
}
.detail-meta {
  font-size: 13px;
  color: var(--center-detail-meta-color);
  display: flex;
  align-items: center;
  gap: 12px;
}
.detail-rating {
  color: #f7b824;
  font-weight: bold;
  background: rgba(247, 184, 36, 0.15);
  padding: 2px 8px;
  border-radius: 12px;
}
.detail-overview {
  font-size: 13px;
  line-height: 1.6;
  color: var(--text-color) !important; 
  display: -webkit-box;
  -webkit-line-clamp: 5;
  line-clamp: 5;
  -webkit-box-orient: vertical;
  overflow: hidden;
  text-align: justify;
}
.detail-credits,
.detail-person-name,
.detail-person-role {
  color: var(--center-detail-text-color);
}
.detail-credits {
  background: var(--center-detail-soft-bg);
  border-radius: 10px;
  padding: 8px 10px;
  border: 1px solid var(--center-detail-border);
  box-shadow: var(--center-detail-shadow);
}
.detail-people-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.detail-person-card {
  display: flex;
  align-items: center;
  gap: 7px;
  width: 136px;
  flex: 0 0 136px;
  padding: 4px 7px 4px 4px;
  border-radius: 999px;
  background: var(--center-detail-person-bg);
  border: 1px solid var(--center-detail-border);
}
.detail-person-avatar {
  width: 34px;
  height: 34px;
  border-radius: 50%;
  object-fit: cover;
  background: var(--center-detail-avatar-bg);
  flex: 0 0 auto;
}
.detail-person-info {
  min-width: 0;
  line-height: 1.2;
}
.detail-person-name {
  font-size: 12px;
  font-weight: 700;
  color: var(--text-color) !important; 
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.detail-person-role {
  margin-top: 2px;
  font-size: 11px;
  color: var(--center-detail-muted-color);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
