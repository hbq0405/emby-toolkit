<!-- src/components/settings/GeneralSettingsPage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <n-space vertical :size="24" style="margin-top: 15px;">
      
      <!-- ★★★ 最终修正: v-if, v-else-if, v-else 现在是正确的同级兄弟关系 ★★★ -->
      <div v-if="configModel">
        <n-form
          ref="formRef"
          :rules="formRules"
          @submit.prevent="save"
          label-placement="left"
          label-width="200"
          label-align="right"
          :model="configModel"
        >
          <n-tabs type="line" animated size="large" pane-style="padding: 20px; box-sizing: border-box;">
            <!-- ================== 标签页 1: 通用设置 ================== -->
            <n-tab-pane name="general" tab="通用设置">
              <n-grid cols="1 l:3" :x-gap="24" :y-gap="24" responsive="screen">
                <n-gi span="1 l:3">
                  <n-card :bordered="false" class="dashboard-card" style="background: linear-gradient(135deg, #fffcf8 0%, #fff 100%); border: 1px solid #ffe5c4;">
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px;">
                      <div>
                        <!-- 动态图标和标题 -->
                        <div style="font-size: 18px; font-weight: bold; display: flex; align-items: center; gap: 8px;" :style="{ color: proStatusInfo.color }">
                          <span style="font-size: 22px;">{{ proStatusInfo.icon }}</span>
                          Emby Toolkit {{ proStatusInfo.text }}
                        </div>
                        
                        <!-- 动态描述和到期时间 -->
                        <div style="font-size: 13px; color: #888; margin-top: 6px;">
                          {{ proStatusInfo.desc }}<br/>
                          <span v-if="configModel?.is_pro_active" style="color: #d48806; font-weight: bold; margin-top: 4px; display: inline-block;">
                            {{ configModel?.pro_expire_time?.startsWith('2099') ? '到期时间：永久有效' : '到期时间：' + configModel?.pro_expire_time?.split('T')[0] }}
                          </span>
                        </div>
                      </div>
                      
                      <!-- 按钮状态 -->
                      <n-space align="center">
                        <n-tag v-if="configModel?.is_pro_active" type="warning" size="large" round :bordered="false" style="font-weight: bold; font-size: 14px; padding: 0 15px;">
                          已激活
                        </n-tag>
                        <n-button
                          type="warning"
                          size="large"
                          strong
                          @click="showProModal = true"
                        >
                          <template #icon><n-icon><DiamondIcon /></n-icon></template>
                          {{ configModel?.is_pro_active ? '续期 Pro' : '升级 Pro' }}
                        </n-button>
                      </n-space>
                    </div>
                  </n-card>
                </n-gi>
                <!-- 左侧列 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">基础设置</span></template>
                    <n-form-item-grid-item label="处理项目间的延迟 (秒)" path="delay_between_items_sec">
                      <n-input-number v-model:value="configModel.delay_between_items_sec" :min="0" :step="0.1" placeholder="例如: 0.5"/>
                    </n-form-item-grid-item>
                    
                    <n-form-item-grid-item label="需手动处理的最低评分阈值" path="min_score_for_review">
                      <n-input-number v-model:value="configModel.min_score_for_review" :min="0.0" :max="10" :step="0.1" placeholder="例如: 6.0"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">处理质量评分低于此值的项目将进入待复核列表。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="最大演员数" path="max_actors_to_process">
                      <n-input-number v-model:value="configModel.max_actors_to_process" :min="10" :step="10" placeholder="建议 30-100"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">处理后最终演员表数量，超过会截断，优先保留有头像演员。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="为角色名添加前缀" path="actor_role_add_prefix">
                      <n-switch v-model:value="configModel.actor_role_add_prefix" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">角色名前加上“饰 ”或“配 ”。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="移除无头像的演员" path="remove_actors_without_avatars">
                      <n-switch v-model:value="configModel.remove_actors_without_avatars" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          在最终演员表移除那些找不到任何可用头像的演员。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="关键词写入标签" path="keyword_to_tags">
                      <n-switch v-model:value="configModel.keyword_to_tags" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          将映射后的中文关键词写入标签。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="工作室中文化" path="studio_to_chinese">
                      <n-switch v-model:value="configModel.studio_to_chinese" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          将工作室名称转换为中文。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="启用合集" path="generate_collection_nfo">
                      <n-switch v-model:value="configModel.generate_collection_nfo" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          在电影NFO中生成合集信息。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                  </n-card>
                </n-gi>
                <!-- 第二列：实时监控 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header>
                      <div style="display: flex; align-items: center; gap: 8px;">
                        <span class="card-title">实时监控</span>
                      </div>
                    </template>
                    
                    <n-form-item label="启用文件系统监控" path="monitor_enabled">
                      <n-switch v-model:value="configModel.monitor_enabled">
                        <template #checked>开启</template>
                        <template #unchecked>关闭</template>
                      </n-switch>
                    </n-form-item>

                    <n-form-item label="监控路径" path="monitor_paths">
                      <n-input-group>
                        <n-select
                          v-model:value="configModel.monitor_paths"
                          multiple
                          filterable
                          tag
                          :show-arrow="false"
                          placeholder="输入路径并回车，或点击右侧选择"
                          :options="[]" 
                          style="flex: 1;"
                        />
                        <n-button type="primary" ghost @click="openLocalFolderSelector('monitor_paths', true)">
                          <template #icon><n-icon :component="FolderIcon" /></template>
                        </n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          请保持和 Emby 媒体库路径映射一致。
                        </n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="排除路径" path="monitor_exclude_dirs">
                      <n-input-group>
                        <n-select
                          v-model:value="configModel.monitor_exclude_dirs"
                          multiple
                          filterable
                          tag
                          :show-arrow="false"
                          placeholder="输入路径并回车，或点击右侧选择"
                          :options="[]" 
                          style="flex: 1;"
                        />
                        <n-button type="primary" ghost @click="openLocalFolderSelector('monitor_exclude_dirs', true)">
                          <template #icon><n-icon :component="FolderIcon" /></template>
                        </n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          命中这些路径的文件将<b>跳过刮削流程</b>，仅刷新。<br/>
                        </n-text>
                      </template>
                    </n-form-item>
                    
                    <!-- 排除刷新延迟 -->
                    <n-form-item label="排除刷新延迟" path="monitor_exclude_refresh_delay">
                      <n-input-number 
                        v-model:value="configModel.monitor_exclude_refresh_delay" 
                        :min="0" 
                        :step="10"
                        placeholder="0" 
                        style="width: 100%" 
                      >
                        <template #suffix>秒</template>
                      </n-input-number>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          仅对<b>排除路径</b>生效。设为 0 则立即刷新。<br/>
                        </n-text>
                      </template>
                    </n-form-item>

                    <!-- 定时扫描回溯天数 -->
                    <n-form-item label="定时扫描回溯" path="monitor_scan_lookback_days">
                      <n-input-number 
                        v-model:value="configModel.monitor_scan_lookback_days" 
                        :min="0" 
                        :max="365" 
                        placeholder="1" 
                        style="width: 100%" 
                      >
                        <template #suffix>天</template>
                      </n-input-number>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          仅检查最近 N 天内创建或修改过的文件，设为 0 则全量扫描。
                        </n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="监控扩展名" path="monitor_extensions">
                      <n-select
                        v-model:value="configModel.monitor_extensions"
                        multiple
                        filterable
                        tag
                        placeholder="输入扩展名并回车"
                        :options="[]" 
                      />
                      <!-- 注意：options 设为空数组配合 tag 模式允许用户自由输入 -->
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          仅处理这些后缀的文件，输入扩展名并回车添加新的监控文件类型。
                        </n-text>
                      </template>
                    </n-form-item>
                    <n-form-item label="图片语言偏好" path="tmdb_image_language_preference">
                      <n-radio-group v-model:value="configModel.tmdb_image_language_preference" name="image_lang_group">
                        <n-space>
                          <n-radio value="zh">简体中文优先</n-radio>
                          <n-radio value="original">原语言优先</n-radio>
                        </n-space>
                      </n-radio-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          控制下载 海报 等图片时的语言优先级。
                        </n-text>
                      </template>
                    </n-form-item>
                    <n-form-item label="SHA1检测" path="monitor_sha1_pc_search">
                      <n-switch v-model:value="configModel.monitor_sha1_pc_search" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          文件SHA1查询，非115网盘勿启用。
                        </n-text>
                      </template>
                    </n-form-item>
                  </n-card>
                </n-gi>
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">数据源与API</span></template>
                    <n-form-item label="TMDB API Key" path="tmdb_api_key">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.tmdb_api_key" placeholder="输入你的 TMDB API Key" />
                    </n-form-item>
                    <n-form-item label="TMDB API Base URL" path="tmdb_api_base_url">
                      <n-input v-model:value="configModel.tmdb_api_base_url" placeholder="https://api.themoviedb.org/3" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">TMDb API的基础URL，通常不需要修改。</n-text></template>
                    </n-form-item>
                    <n-form-item label="成人内容探索" path="tmdb_include_adult">
                      <n-space align="center">
                        <n-switch v-model:value="configModel.tmdb_include_adult" />
                        <n-text depth="3" style="font-size: 0.9em; margin-left: 8px;">
                          控制影视探索是否返回成人内容。
                        </n-text>
                      </n-space>
                    </n-form-item>
                    <n-form-item label="GitHub 个人访问令牌" path="github_token">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.github_token" placeholder="可选，用于提高API请求频率限制"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;"><a href="https://github.com/settings/tokens/new" target="_blank" style="font-size: 1.3em; margin-left: 4px; color: var(--n-primary-color); text-decoration: underline;">免费申请GithubTOKEN</a></n-text></template>
                    </n-form-item>
                    <n-form-item label="启用在线豆瓣API" path="douban_enable_online_api">
                      <n-space align="center">
                        <n-switch v-model:value="configModel.douban_enable_online_api" />
                        <n-text depth="3" style="font-size: 0.9em; margin-left: 8px;">
                          关闭后仅使用本地缓存。
                        </n-text>
                      </n-space>
                    </n-form-item>
                    <n-form-item-grid-item label="豆瓣API冷却时间 (秒)" path="api_douban_default_cooldown_seconds">
                      <n-input-number v-model:value="configModel.api_douban_default_cooldown_seconds" :min="0.1" :step="0.1" placeholder="例如: 1.0"/>
                    </n-form-item-grid-item>
                    <n-form-item label="豆瓣登录 Cookie" path="douban_cookie">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.douban_cookie" placeholder="从浏览器开发者工具中获取"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">非必要不用配置，作用有限。</n-text></template>
                    </n-form-item>
                  </n-card>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ★★★ 115 网盘设置 ★★★ -->
            <n-tab-pane name="115_drive" tab="115 网盘">
              <n-grid cols="1 l:3" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- 左侧：账户与连接 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card" style="height: 100%;">
                    <template #header>
                      <div style="display: flex; align-items: center; gap: 8px;">
                        <span class="card-title">账户与连接</span>
                        <n-tag v-if="p115Info" :type="p115Info.valid ? 'success' : 'error'" size="small" round>
                          {{ p115Info.msg || (p115Info.valid ? '连接正常' : '连接异常') }}
                        </n-tag>
                        <n-tag v-else type="warning" size="small" round>未检查</n-tag>
                      </div>
                    </template>
                    <template #header-extra>
                      <n-space align="center" :size="12">
                        <n-button size="small" secondary type="success" @click="check115Status" :loading="loading115Info">
                          检查连通性
                        </n-button>
                      </n-space>
                    </template>

                    <!-- ★★★ 用户信息展示卡片 ★★★ -->
                    <div v-if="p115Info && p115Info.user_info" style="margin-bottom: 16px; padding: 12px; background: var(--n-action-color); border-radius: 8px; display: flex; align-items: center; gap: 12px;">
                      <n-avatar :src="p115Info.user_info.user_face_m" round size="large" />
                      <div style="flex: 1; overflow: hidden;">
                        <div style="font-weight: bold; font-size: 14px; display: flex; align-items: center; gap: 6px;">
                          {{ p115Info.user_info.user_name }}
                          <n-tag size="tiny" type="warning" :bordered="false">{{ p115Info.user_info.vip_info?.level_name || '普通用户' }}</n-tag>
                        </div>
                        <div style="font-size: 12px; color: var(--n-text-color-3); margin-top: 4px;">
                          剩余空间: {{ p115Info.user_info.rt_space_info?.all_remain?.size_format || '未知' }}
                        </div>
                      </div>
                    </div>

                    <!-- 授权方式选择 -->
                    <n-form-item label="授权方式" path="p115_auth_method">
                      <n-radio-group v-model:value="configModel.p115_auth_method" name="auth_method_group">
                        <n-space>
                          <n-radio value="web">网页登录授权 (推荐)</n-radio>
                          <n-radio value="qrcode">自定义 AppID 扫码</n-radio>
                        </n-space>
                      </n-radio-group>
                    </n-form-item>

                    <!-- 方式一：网页登录授权 -->
                    <n-form-item label="登录授权" v-if="configModel.p115_auth_method === 'web' || !configModel.p115_auth_method">
                      <n-space vertical :size="8" style="width: 100%;">
                        <n-space align="center" justify="space-between">
                          <n-tag :type="p115Info?.has_token ? 'success' : 'default'" size="small">
                            <template #icon>
                              <n-icon :component="p115Info?.has_token ? CheckIcon : CloseIcon" />
                            </template>
                            {{ p115Info?.has_token ? '已授权' : '未授权 (请登录)' }}
                          </n-tag>
                          <n-button size="small" type="warning" @click="startWebAuth" :loading="isWebAuthing">
                            {{ p115Info?.has_token ? '重新登录' : '登录授权' }}
                          </n-button>
                        </n-space>
                        <n-text depth="3" style="font-size:0.8em;">
                          用于网盘整理和视频播放。请点击“登录授权”获取授权。
                        </n-text>
                      </n-space>
                    </n-form-item>

                    <!-- 方式二：自定义 AppID 扫码 -->
                    <n-form-item label="扫码授权" path="p115_app_id" v-if="configModel.p115_auth_method === 'qrcode'">
                      <n-space vertical :size="8" style="width: 100%;">
                        <n-space align="center" justify="space-between">
                          <n-tag :type="p115Info?.has_token ? 'success' : 'default'" size="small">
                            <template #icon>
                              <n-icon :component="p115Info?.has_token ? CheckIcon : CloseIcon" />
                            </template>
                            {{ p115Info?.has_token ? '已授权' : '未授权 (请扫码)' }}
                          </n-tag>
                          <n-button size="small" type="primary" @click="handleOpenQrcodeModal">
                            {{ p115Info?.has_token ? '重新扫码' : '扫码授权' }}
                          </n-button>
                        </n-space>
                        <n-input-group>
                          <n-input v-model:value="configModel.p115_app_id" placeholder="先保存自定义AppID再扫码" />
                        </n-input-group>
                        <template #feedback>
                          <n-text depth="3" style="font-size:0.8em;">
                            请先填写 AppID 并点击底部保存设置，然后再扫码授权。
                          </n-text>
                        </template>
                      </n-space>
                    </n-form-item>

                    <!--  Cookie  -->
                    <n-form-item label="Cookie">
                      <n-space vertical :size="8" style="width: 100%;">
                        <n-space align="center" justify="space-between">
                          <n-tag :type="p115Info?.has_cookie ? 'success' : 'default'" size="small">
                            <template #icon>
                              <n-icon :component="p115Info?.has_cookie ? CheckIcon : CloseIcon" />
                            </template>
                            {{ p115Info?.has_cookie ? '已配置' : '未配置' }}
                          </n-tag>
                          <n-button size="small" type="primary" @click="openCookieModal">
                            {{ p115Info?.has_cookie ? '重新获取' : '扫码获取' }}
                          </n-button>
                        </n-space>
                        <n-text depth="3" style="font-size:0.8em;">
                          用于TG、影巢转存和视频播放。
                        </n-text>
                      </n-space>
                    </n-form-item>

                    <n-form-item label="播放接口" path="p115_playback_api_priority">
                      <n-radio-group v-model:value="configModel.p115_playback_api_priority" name="api_priority_group">
                        <n-space>
                          <n-radio value="openapi">优先 OpenAPI</n-radio>
                          <n-radio value="cookie">优先 Cookie</n-radio>
                        </n-space>
                      </n-radio-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">获取直链时首次尝试的接口，失败会自动回退到另一个。</n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="API 请求间隔 (秒)" path="p115_request_interval">
                      <n-input-number v-model:value="configModel.p115_request_interval" :min="0.1" :step="0.1" placeholder="0.5" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          115 官方对 API 调用频率有严格限制，建议保持 0.5 秒以上。
                        </n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="API 并发线程数" path="p115_max_workers">
                      <n-input-number v-model:value="configModel.p115_max_workers" :min="1" :max="20" :step="1" placeholder="3" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          控制扫描和整理时的并发数量。如果出现异常，调低此值。
                        </n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="STRM 链接地址" path="etk_server_url">
                        <n-input v-model:value="configModel.etk_server_url" placeholder="http://192.168.X.X:5257" />
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">支持http或挂载路径。</n-text>
                        </template>
                    </n-form-item>

                    <n-form-item label="需要整理的扩展名" path="p115_extensions">
                      <n-select
                        v-model:value="configModel.p115_extensions"
                        multiple
                        filterable
                        tag
                        placeholder="输入扩展名并回车 (如 mkv)"
                        :options="[]" 
                      />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          只有包含在列表中的文件类型才会被整理。
                        </n-text>
                      </template>
                    </n-form-item>
                    <n-form-item label="批量替换 STRM" path="">
                        <n-button @click="openReplaceStrmModal" type="warning" ghost>
                            批量替换本地 STRM 链接
                        </n-button>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">支持普通字符串替换和正则表达式替换。</n-text>
                        </template>
                    </n-form-item>
                  </n-card>
                </n-gi>

                <!-- 中间：路径配置 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card" style="height: 100%;">
                    <template #header><span class="card-title">整理与路径</span></template>
                    <n-form-item label="待整理目录" path="p115_save_path_cid">
                      <n-input-group>
                        <n-input 
                          :value="configModel.p115_save_path_name || configModel.p115_save_path_cid" 
                          placeholder="选择待整理目录" readonly 
                          @click="openFolderSelector('save_path', configModel.p115_save_path_cid)"
                        >
                          <template #prefix><n-icon :component="FolderIcon" /></template>
                        </n-input>
                        <n-button type="primary" ghost @click="openFolderSelector('save_path', configModel.p115_save_path_cid)">选择</n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">MP下载或网盘转存的初始目录</n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="未识别目录" path="p115_unrecognized_cid">
                      <n-input-group>
                        <n-input 
                          :value="configModel.p115_unrecognized_name || configModel.p115_unrecognized_cid" 
                          placeholder="选择未识别目录" readonly 
                          @click="openFolderSelector('unrecognized_path', configModel.p115_unrecognized_cid)"
                        >
                          <template #prefix><n-icon :component="FolderIcon" /></template>
                        </n-input>
                        <n-button type="primary" ghost @click="openFolderSelector('unrecognized_path', configModel.p115_unrecognized_cid)">选择</n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">无法识别或不符合规则的文件将被移入此固定目录</n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="网盘媒体库根目录" path="p115_media_root_cid">
                      <n-input-group>
                        <n-input 
                          :value="configModel.p115_media_root_name || configModel.p115_media_root_cid" 
                          placeholder="选择网盘媒体库主目录" readonly 
                          @click="openFolderSelector('media_root', configModel.p115_media_root_cid)"
                        >
                          <template #prefix><n-icon :component="FolderIcon" /></template>
                        </n-input>
                        <n-button type="primary" ghost @click="openFolderSelector('media_root', configModel.p115_media_root_cid)">选择</n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">整理目标主目录，分类规则的目录都在它下面</n-text>
                      </template>
                    </n-form-item>

                    <n-form-item label="本地 STRM 根目录" path="local_strm_root">
                      <n-input-group>
                        <n-input 
                          v-model:value="configModel.local_strm_root" 
                          placeholder="例如: /mnt/media" 
                          @click="openLocalFolderSelector('local_strm_root', false)"
                        >
                          <template #prefix><n-icon :component="FolderIcon" /></template>
                        </n-input>
                        <n-button type="primary" ghost @click="openLocalFolderSelector('local_strm_root', false)">选择</n-button>
                      </n-input-group>
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">ETK 自动在此目录生成与网盘对应的 .strm 文件</n-text>
                      </template>
                    </n-form-item>
                    
                    <n-form-item label="智能整理开关" path="p115_enable_organize">
                        <n-switch v-model:value="configModel.p115_enable_organize">
                            <template #checked>整理并生成STRM</template>
                            <template #unchecked>仅转存</template>
                        </n-switch>
                    </n-form-item>
                    <n-form-item label="忽略小视频" path="p115_min_video_size">
                        <n-input-number 
                            v-model:value="configModel.p115_min_video_size" 
                            :min="0" 
                            :step="10" 
                            style="width: 150px;"
                        >
                            <template #suffix>MB</template>
                        </n-input-number>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">小于此体积的视频将被判定为花絮/样本/广告，打入未识别目录 (设为0则不忽略)。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="生活事件监控" path="p115_life_monitor_enabled">
                        <n-switch v-model:value="configModel.p115_life_monitor_enabled">
                            <template #checked>开启监控</template>
                            <template #unchecked>关闭监控</template>
                        </n-switch>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">通过115操作记录实现增量生成STRM。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="事件检查间隔 (分钟)" path="p115_life_monitor_interval" v-if="configModel.p115_life_monitor_enabled">
                        <n-input-number v-model:value="configModel.p115_life_monitor_interval" :min="5" :step="1" placeholder="5" style="width: 100%;" />
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">最短5分钟。过短可能触发风控。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="媒体信息中心化" path="p115_mediainfo_center">
                        <n-switch v-model:value="configModel.p115_mediainfo_center">
                            <template #checked>共享媒体信息</template>
                            <template #unchecked>本地媒体信息</template>
                        </n-switch>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">在线从中心服务器获取媒体信息数据，需要神医Pro。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="同步下载字幕" path="p115_download_subs">
                        <n-switch v-model:value="configModel.p115_download_subs">
                            <template #checked>下载到本地</template>
                            <template #unchecked>跳过字幕</template>
                        </n-switch>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">整理或全量生成 STRM 时会自动将 115 上的字幕文件下载到本地同级目录。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="全量同步时清理本地" path="p115_local_cleanup">
                        <n-switch v-model:value="configModel.p115_local_cleanup">
                            <template #checked>清理失效文件</template>
                            <template #unchecked>保留本地文件</template>
                        </n-switch>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">全量生成 STRM 时，会自动删除本地存在但网盘已不存在的 .strm 和字幕文件。</n-text>
                        </template>
                    </n-form-item>
                    <n-form-item label="联动删除网盘文件" path="p115_enable_sync_delete">
                        <n-switch v-model:value="configModel.p115_enable_sync_delete">
                            <template #checked>删除网盘源文件</template>
                            <template #unchecked>仅移除本地缓存</template>
                        </n-switch>
                        <template #feedback>
                            <n-text depth="3" style="font-size:0.8em;">在 Emby 中删除媒体时，将同时删除 115 网盘上的文件。</n-text>
                        </template>
                    </n-form-item>
                  </n-card>
                </n-gi>
                <!-- 右侧：分类规则与重命名 -->
                <n-gi>
                  <n-space vertical :size="24" style="height: 100%;">
                    
                    <!-- 卡片 1：分类规则 -->
                    <n-card :bordered="false" class="dashboard-card">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">智能分类规则</span>
                          <n-button secondary type="primary" @click="ruleManagerRef?.open()">
                            <template #icon><n-icon :component="ListIcon" /></template>
                            管理分类规则
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="info" :show-icon="true">
                        当开启“整理”时，系统将按顺序匹配规则。命中规则后，资源将被移动到指定的 115 目录中。
                        <br>未命中的资源将移动到“未识别”目录。
                      </n-alert>
                    </n-card>

                    <!-- ★ 卡片 2：自定义重命名 (移到这里) -->
                    <n-card :bordered="false" class="dashboard-card" style="flex: 1;">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">自定义重命名</span>
                          <n-button secondary type="primary" @click="renameModalRef?.open()">
                            <template #icon>
                              <n-icon :component="ColorWandIcon" />
                            </template>
                            配置命名规则
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="success" :show-icon="true">
                        打造强迫症专属的完美媒体库命名格式。支持自定义主目录、季目录及文件的中英文、年份、TMDb标签等。
                      </n-alert>
                    </n-card>

                    <!-- ★ 卡片 3：独立音乐库管理 -->
                    <n-card :bordered="false" class="dashboard-card" style="flex: 1;">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">音乐库管理</span>
                          <n-button secondary type="primary" @click="musicModalRef?.open()">
                            <template #icon>
                              <n-icon :component="FolderIcon" />
                            </template>
                            打开音乐库
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="success" :show-icon="true">
                        简单音乐管理器，支持直接上传文件夹、自动创建 115 目录并同步生成本地 STRM 文件，全量生成音乐库STRM。
                      </n-alert>
                    </n-card>

                    <!-- ★ 卡片 4：第三方 STRM 兼容 -->
                    <n-card :bordered="false" class="dashboard-card" style="flex: 1;">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">第三方 STRM 兼容</span>
                          <n-button secondary type="primary" @click="openCustomRegexModal">
                            <template #icon>
                              <n-icon :component="BuildIcon" />
                            </template>
                            配置提取正则
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="info" :show-icon="true">
                        支持第三方工具生成的 STRM 文件，通过自定义正则表达式，让 ETK 实时拦截并提取 PC 码实现302播放。内置已支持CMS、MH、MP115strm。
                      </n-alert>
                    </n-card>

                  </n-space>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 2: Emby (紧凑双列版) ================== -->
            <n-tab-pane name="emby" tab="Emby & 302反代">
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">

                <!-- ########## 左侧卡片: Emby 连接设置 ########## -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">Emby 连接设置</span></template>
                    
                    <!-- ★★★ 调整点1: 恢复双列，但减小间距 x-gap="12" ★★★ -->
                    <n-grid cols="1 m:2" :x-gap="12" :y-gap="12" responsive="screen">
                      
                      <!-- 1. Emby URL (左) -->
                      <!-- ★★★ 调整点2: label-width="100" 覆盖全局的200，让输入框更长、更紧凑 ★★★ -->
                      <n-form-item-grid-item label-width="100">
                        <template #label>
                          <div style="display: flex; align-items: center; justify-content: flex-end; width: 100%;">
                            <span>Emby URL</span>
                            <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" />
                              </template>
                              此项修改需要重启容器才能生效。
                            </n-tooltip>
                          </div>
                        </template>
                        <n-input v-model:value="configModel.emby_server_url" placeholder="http://localhost:8096" />
                      </n-form-item-grid-item>

                      <!-- 2. 外网访问 URL (右) -->
                      <n-form-item-grid-item label="外网URL" path="emby_public_url" label-width="100">
                        <n-input v-model:value="configModel.emby_public_url" placeholder="留空则不开启" />
                      </n-form-item-grid-item>

                      <!-- 3. API Key (左) -->
                      <n-form-item-grid-item label="APIKey" path="emby_api_key" label-width="100">
                        <n-input v-model:value="configModel.emby_api_key" type="password" show-password-on="click" placeholder="输入 API Key" />
                      </n-form-item-grid-item>

                      <!-- 4. 用户 ID (右) -->
                      <n-form-item-grid-item label="用户ID" :rule="embyUserIdRule" path="emby_user_id" label-width="100">
                        <n-input v-model:value="configModel.emby_user_id" placeholder="32位用户ID" />
                        <template #feedback>
                          <div v-if="isInvalidUserId" style="color: #e88080; font-size: 12px;">格式错误！ID应为32位。</div>
                        </template>
                      </n-form-item-grid-item>

                      <!-- 分割线 (占满一行) -->
                      <n-gi span="1 m:2">
                        <n-divider title-placement="left" style="margin: 8px 0; font-size: 0.9em; color: gray;">管理员凭证 (选填)</n-divider>
                      </n-gi>

                      <!-- 5. 管理员用户 (左) -->
                      <n-form-item-grid-item label="用户名" path="emby_admin_user" label-width="100">
                        <n-input v-model:value="configModel.emby_admin_user" placeholder="管理员用户名" />
                      </n-form-item-grid-item>

                      <!-- 6. 管理员密码 (右) -->
                      <n-form-item-grid-item label="密码" path="emby_admin_pass" label-width="100">
                        <n-input v-model:value="configModel.emby_admin_pass" type="password" show-password-on="click" placeholder="管理员密码" />
                      </n-form-item-grid-item>

                      <!-- 7. 超时时间 (占满一行，保持长标签) -->
                      <n-form-item-grid-item label="Emby API 超时时间 (秒)" path="emby_api_timeout" span="1 m:2" label-width="200">
                        <n-input-number v-model:value="configModel.emby_api_timeout" :min="15" :step="5" placeholder="建议 30-90" style="width: 100%;" />
                      </n-form-item-grid-item>

                      <!-- 分割线 -->
                      <n-gi span="1 m:2">
                        <n-divider title-placement="left" style="margin-top: 10px;">选择要处理的媒体库</n-divider>
                      </n-gi>

                      <!-- 8. 媒体库选择 -->
                      <n-form-item-grid-item label-placement="top" span="1 m:2">
                        <n-spin :show="loadingLibraries">
                          <n-checkbox-group v-model:value="configModel.libraries_to_process">
                            <n-space item-style="display: flex; flex-wrap: wrap;">
                              <n-checkbox v-for="lib in availableLibraries" :key="lib.Id" :value="lib.Id" :label="lib.Name" />
                            </n-space>
                          </n-checkbox-group>
                          <n-text depth="3" v-if="!loadingLibraries && availableLibraries.length === 0 && (configModel.emby_server_url && configModel.emby_api_key)">
                            未找到媒体库。请检查 Emby URL 和 API Key。
                          </n-text>
                          <div v-if="libraryError" style="color: red; margin-top: 5px;">{{ libraryError }}</div>
                        </n-spin>
                      </n-form-item-grid-item>

                    </n-grid>
                  </n-card>
                </n-gi>

                <!-- ########## 右侧卡片: 虚拟库 (反向代理) ########## -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">302反代（Pro）</span></template>
                    
                    <!-- 同样使用紧凑双列 -->
                    <n-grid cols="1 m:2" :x-gap="12" :y-gap="12" responsive="screen">

                      <!-- 1. 启用开关 -->
                      <n-form-item-grid-item label="启用" path="proxy_enabled" label-width="100">
                        <n-switch v-model:value="configModel.proxy_enabled" />
                      </n-form-item-grid-item>

                      <!-- 2. 端口 -->
                      <n-form-item-grid-item label-width="100">
                        <template #label>
                          <div style="display: flex; align-items: center; justify-content: flex-end; width: 100%;">
                            <span>端口</span>
                            <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" style="margin-left: 4px;" />
                              </template>
                              需重启容器生效
                            </n-tooltip>
                          </div>
                        </template>
                        <n-input-number v-model:value="configModel.proxy_port" :min="1025" :max="65535" :disabled="!configModel.proxy_enabled" style="width: 100%;" placeholder="8096"/>
                      </n-form-item-grid-item>

                      <!-- 第三方302 URL -->
                      <n-form-item-grid-item span="1 m:2" label-width="100">
                        <template #label>
                          <div style="display: flex; align-items: center; justify-content: flex-end; width: 100%;">
                            <span>302重定向</span>
                            <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" style="margin-left: 4px;" />
                              </template>
                              需重启容器生效
                            </n-tooltip>
                          </div>
                        </template>
                        <n-input 
                          v-model:value="configModel.proxy_302_redirect_url" 
                          placeholder="例如: http://192.168.31.177:9096" 
                          :disabled="!configModel.proxy_enabled"
                        />
                      </n-form-item-grid-item>
                      
                      <!-- 3. 缺失占位符 (占满一行，因为说明文字较长) -->
                      <n-form-item-grid-item label="缺失占位符" path="proxy_show_missing_placeholders" span="1 m:2" label-width="100">
                         <n-space align="center">
                            <n-switch v-model:value="configModel.proxy_show_missing_placeholders" :disabled="!configModel.proxy_enabled"/>
                            <n-text depth="3" style="font-size: 0.8em;">在榜单中显示未入库海报</n-text>
                         </n-space>
                      </n-form-item-grid-item>

                      <!-- 5. 合并原生库 -->
                      <n-form-item-grid-item label="合并原生库" path="proxy_merge_native_libraries" label-width="100">
                        <n-switch v-model:value="configModel.proxy_merge_native_libraries" :disabled="!configModel.proxy_enabled"/>
                      </n-form-item-grid-item>

                      <!-- 6. 显示位置 -->
                      <n-form-item-grid-item label="显示位置" path="proxy_native_view_order" label-width="100">
                        <n-radio-group v-model:value="configModel.proxy_native_view_order" :disabled="!configModel.proxy_enabled || !configModel.proxy_merge_native_libraries">
                          <n-radio value="before">在前</n-radio>
                          <n-radio value="after">在后</n-radio>
                        </n-radio-group>
                      </n-form-item-grid-item>

                      <!-- 分割线 -->
                      <n-gi span="1 m:2">
                        <n-divider title-placement="left" style="margin-top: 10px;">选择合并显示的原生媒体库</n-divider>
                      </n-gi>

                      <!-- 7. 原生库选择 -->
                      <n-form-item-grid-item 
                        v-if="configModel.proxy_enabled && configModel.proxy_merge_native_libraries" 
                        path="proxy_native_view_selection" 
                        label-placement="top"
                        span="1 m:2"
                      >
                        <n-spin :show="loadingNativeLibraries">
                          <n-checkbox-group v-model:value="configModel.proxy_native_view_selection">
                            <n-space item-style="display: flex; flex-wrap: wrap;">
                              <n-checkbox v-for="lib in nativeAvailableLibraries" :key="lib.Id" :value="lib.Id" :label="lib.Name"/>
                            </n-space>
                          </n-checkbox-group>
                          <n-text depth="3" v-if="!loadingNativeLibraries && nativeAvailableLibraries.length === 0 && (configModel.emby_server_url && configModel.emby_api_key && configModel.emby_user_id)">
                            未找到原生媒体库。请检查 Emby URL、API Key 和 用户ID。
                          </n-text>
                          <div v-if="nativeLibraryError" style="color: red; margin-top: 5px;">{{ nativeLibraryError }}</div>
                        </n-spin>
                      </n-form-item-grid-item>

                    </n-grid>
                  </n-card>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 3: 智能服务  ================== -->
            <n-tab-pane name="services" tab="智能服务 & 订阅源">
              <!-- ★★★ 修改点1: cols 改为 "1 l:3"，总共3列 ★★★ -->
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- 左侧: AI翻译 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card" style="height: 100%;">
                    <template #header><span class="card-title">AI 增强</span></template>
                    <template #header-extra>
                      <n-space align="center">
                        <n-button 
                          size="tiny" 
                          type="info" 
                          ghost 
                          @click="openPromptModal"
                        >
                          配置提示词
                        </n-button>
                        <n-button 
                          size="tiny" 
                          type="primary" 
                          ghost 
                          @click="testAI" 
                          :loading="isTestingAI"
                          :disabled="!configModel.ai_api_key"
                        >
                          测试连接
                        </n-button>
                        <!-- [移除] 总开关 n-switch -->
                        <a href="https://cloud.siliconflow.cn/i/GXIrubbL" target="_blank" style="font-size: 0.85em; color: var(--n-primary-color); text-decoration: underline;">注册硅基流动</a>
                      </n-space>
                    </template>
                    
                    <!-- 移除 content-disabled 类，因为不再有总开关控制禁用 -->
                    <div class="ai-settings-wrapper">
                      
                      <!-- 1. 基础配置 (上移，因为它们是前提) -->
                      <n-form-item label="AI 服务商" path="ai_provider">
                        <n-select v-model:value="configModel.ai_provider" :options="aiProviderOptions" />
                      </n-form-item>
                      <n-form-item label="API Key" path="ai_api_key">
                        <n-input type="password" show-password-on="mousedown" v-model:value="configModel.ai_api_key" placeholder="输入你的 API Key" />
                      </n-form-item>
                      <n-form-item label="模型名称" path="ai_model_name">
                        <n-input v-model:value="configModel.ai_model_name" placeholder="例如: gpt-3.5-turbo, glm-4" />
                      </n-form-item>
                      <n-form-item label="API Base URL (可选)" path="ai_base_url">
                        <n-input v-model:value="configModel.ai_base_url" placeholder="用于代理或第三方兼容服务" />
                      </n-form-item>

                      <n-divider style="margin: 10px 0; font-size: 0.9em; color: gray;">功能开关</n-divider>

                      <!-- 2. 功能细分开关 -->
                      <n-form-item label="启用功能">
                        <n-grid :cols="2" :y-gap="8">
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_translate_actor_role">
                              翻译演员与角色
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_translate_title">
                              翻译片名
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_translate_overview">
                              翻译简介
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_translate_episode_overview">
                              翻译分集简介
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_vector">
                              生成媒体向量
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_recognition">
                              辅助识别
                            </n-checkbox>
                          </n-gi>
                          <n-gi>
                            <n-checkbox v-model:checked="configModel.ai_joke_fallback">
                              无简介小笑话占位
                            </n-checkbox>
                          </n-gi>
                        </n-grid>
                      </n-form-item>

                      <!-- 3. 高级选项 -->
                      <n-form-item label="翻译模式" path="ai_translation_mode" v-if="configModel.ai_translate_actor_role || configModel.ai_translate_title_overview">
                        <n-radio-group v-model:value="configModel.ai_translation_mode" name="ai_translation_mode">
                          <n-space vertical>
                            <n-radio value="fast">快速模式 (仅翻译)</n-radio>
                            <n-radio value="quality">顾问模式 (结合剧情上下文)</n-radio>
                          </n-space>
                        </n-radio-group>
                      </n-form-item>
                      
                    </div>
                  </n-card>
                </n-gi>

                <!-- 右侧: 订阅源配置 (独立卡片布局) -->
                <n-gi>
                  <n-space vertical :size="24" style="height: 100%;">
                    
                    <!-- 卡片 1：MoviePilot -->
                    <n-card :bordered="false" class="dashboard-card">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">MoviePilot</span>
                          <n-button secondary type="primary" @click="mpModalRef?.open()">
                            <template #icon><n-icon :component="ListIcon" /></template>
                            配置
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="info" :show-icon="true">
                        配置 MoviePilot 订阅源，实现自动化追剧、洗版及下载管理。
                      </n-alert>
                    </n-card>

                    <!-- 卡片 2：影巢 (HDHive) -->
                    <n-card :bordered="false" class="dashboard-card">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">影巢 (HDHive)</span>
                          <n-button secondary type="warning" @click="hdhiveModalRef?.open()">
                            <template #icon><n-icon :component="CloudDownloadIcon" /></template>
                            配置
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="success" :show-icon="true">
                        配置影巢 API，解锁 115 网盘极速秒传，海量资源一键入库。
                      </n-alert>
                    </n-card>

                    <!-- 卡片 3：TG 频道监听 -->
                    <n-card :bordered="false" class="dashboard-card">
                      <template #header>
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                          <span class="card-title">TG 频道监听 (Pro)</span>
                          <n-button secondary type="info" @click="tgMonitorModalRef?.open()">
                            <template #icon><n-icon :component="PaperPlaneIcon" /></template>
                            配置
                          </n-button>
                        </div>
                      </template>
                      <n-alert type="warning" :show-icon="true">
                        自动监听 Telegram 频道消息，根据订阅规则选择性转存 115 资源。
                      </n-alert>
                    </n-card>

                  </n-space>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 4: 高级 (核心修改区域) ================== -->
            <n-tab-pane name="advanced" tab="高级">
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- 卡片 1: 网络代理 (左上) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">网络代理</span></template>
                    <template #header-extra><a href="https://api-flowercloud.com/aff.php?aff=8652" target="_blank" style="font-size: 0.85em; color: var(--n-primary-color); text-decoration: underline;">推荐机场</a></template>
                    <n-form-item-grid-item label="启用网络代理" path="network_proxy_enabled">
                      <n-switch v-model:value="configModel.network_proxy_enabled" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">为 TMDb 等外部API请求启用 HTTP/HTTPS 代理。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="HTTP 代理地址" path="network_http_proxy_url">
                      <n-input-group>
                        <n-input v-model:value="configModel.network_http_proxy_url" placeholder="例如: http://127.0.0.1:7890" :disabled="!configModel.network_proxy_enabled"/>
                        <n-button type="primary" ghost @click="testProxy" :loading="isTestingProxy" :disabled="!configModel.network_proxy_enabled || !configModel.network_http_proxy_url">测试连接</n-button>
                      </n-input-group>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">请填写完整的代理 URL，支持 http 和 https。</n-text></template>
                    </n-form-item-grid-item>
                  </n-card>
                </n-gi>

                <!-- 卡片 2: 日志配置 (右上) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">日志配置</span></template>
                    <n-form-item-grid-item>
                      <template #label>
                        <n-space align="center">
                          <span>单个日志文件大小 (MB)</span>
                          <n-tooltip trigger="hover">
                            <template #trigger>
                              <n-icon :component="AlertIcon" class="info-icon" />
                            </template>
                            此项修改需要重启容器才能生效。
                          </n-tooltip>
                        </n-space>
                      </template>
                      <n-input-number v-model:value="configModel.log_rotation_size_mb" :min="1" :step="1" placeholder="例如: 5"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">设置 app.log 文件的最大体积，超限后会轮转。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item>
                      <template #label>
                        <n-space align="center">
                          <span>日志备份数量</span>
                          <n-tooltip trigger="hover">
                            <template #trigger>
                              <n-icon :component="AlertIcon" class="info-icon" />
                            </template>
                            此项修改需要重启容器才能生效。
                          </n-tooltip>
                        </n-space>
                      </template>
                      <n-input-number v-model:value="configModel.log_rotation_backup_count" :min="1" :step="1" placeholder="例如: 10"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">保留最近的日志文件数量 (app.log.1, app.log.2 ...)。</n-text></template>
                    </n-form-item-grid-item>
                  </n-card>
                </n-gi>

                <!-- 卡片 3: 数据管理 (左下) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">数据管理</span></template>
                    <n-space vertical>
                      <n-space align="center">
                        <n-button @click="showExportModal" :loading="isExporting" class="action-button"><template #icon><n-icon :component="ExportIcon" /></template>导出数据</n-button>
                        <n-upload :custom-request="handleCustomImportRequest" :show-file-list="false" accept=".json.gz"><n-button :loading="isImporting" class="action-button"><template #icon><n-icon :component="ImportIcon" /></template>导入数据</n-button></n-upload>
                        <n-button @click="showClearTablesModal" :loading="isClearing" class="action-button" type="error" ghost><template #icon><n-icon :component="ClearIcon" /></template>清空指定表</n-button>
                        <n-popconfirm @positive-click="handleCleanupOfflineMedia">
                          <template #trigger>
                            <n-button type="warning" ghost :loading="isCleaningOffline" class="action-button">
                              <template #icon><n-icon :component="OfflineIcon" /></template>
                              清理离线媒体
                            </n-button>
                          </template>
                          <div style="max-width: 300px">
                            <p style="margin: 0 0 4px 0">确定要清理离线媒体数据吗？</p>
                            <p style="margin: 0 0 4px 0">这将删除所有 <b>不在库</b> 的元数据缓存。</p>
                            <span style="font-size: 0.9em; color: gray;">此操作用于数据库瘦身，不会影响已入库媒体项。</span>
                          </div>
                        </n-popconfirm>
                        <n-popconfirm @positive-click="handleClearVectors">
                          <template #trigger>
                            <n-button type="warning" ghost :loading="isClearingVectors" class="action-button">
                              <template #icon><n-icon :component="FlashIcon" /></template>
                              清空向量数据
                            </n-button>
                          </template>
                          <div style="max-width: 300px">
                            <p style="margin: 0 0 4px 0; font-weight: bold;">确定要清空所有 AI 向量数据吗？</p>
                            <p style="margin: 0 0 4px 0;">如果您更换了 <b>Embedding 模型</b>（例如从 OpenAI 更换为本地模型），<span style="color: #d03050;">必须执行此操作</span>。</p>
                            <span style="font-size: 0.9em; color: gray;">不同模型生成的向量不兼容，混用会导致推荐结果完全错误。清空后需重新扫描生成。</span>
                          </div>
                        </n-popconfirm>
                        <n-popconfirm @positive-click="handleCorrectSequences">
                          <template #trigger>
                            <n-button type="warning" ghost :loading="isCorrecting" class="action-button">
                              <template #icon><n-icon :component="BuildIcon" /></template>
                              校准ID计数器
                            </n-button>
                          </template>
                          确定要校准所有表的ID自增计数器吗？<br />
                          这是一个安全的操作，用于修复导入数据后无法新增条目的问题。
                        </n-popconfirm>
                        <!-- ### 重置演员映射表 ### -->
                        <n-button 
                          type="warning" 
                          ghost 
                          :loading="isResettingMappings" 
                          class="action-button"
                          @click="showResetMappingsModal"
                        >
                          <template #icon><n-icon :component="SyncIcon" /></template>
                          重置Emby数据
                        </n-button>
                      </n-space>
                      <p class="description-text"><b>导出：</b>将数据库中的一个或多个表备份为 JSON.GZ 文件。<br><b>导入：</b>从 JSON.GZ 备份文件中恢复数据。<br><b>清空：</b>删除指定表中的所有数据，此操作不可逆。<br><b>清空向量：</b>更换ai后，必须执行此操作。不同模型生成的向量不兼容，混用会导致推荐结果完全错误。清空后需重新扫描生成。<br><b>清理离线：</b>移除已删除且无订阅状态的残留记录，给数据库瘦身。<br><b>校准：</b>修复导入数据可能引起的自增序号错乱的问题。<br><b>重置：</b>在重建 Emby 媒体库后，使用此功能清空所有旧的 Emby 关联数据（用户、合集、播放状态等），并保留核心元数据，以便后续重新扫描和关联。</p>
                    </n-space>
                  </n-card>
                </n-gi>

                <!-- 卡片 4: Telegram 设置 (右下) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">Telegram 设置</span></template>
                    
                    <template #header-extra>
                      <n-button size="tiny" type="primary" ghost @click="testTelegram" :loading="isTestingTelegram" :disabled="!configModel.telegram_bot_token || !configModel.telegram_channel_id">
                        发送测试
                      </n-button>
                    </template>

                    <n-form-item-grid-item label="Telegram Bot Token" path="telegram_bot_token">
                      <n-input v-model:value="configModel.telegram_bot_token" type="password" show-password-on="click" placeholder="从 @BotFather 获取" />
                    </n-form-item-grid-item>
                    
                    <n-form-item-grid-item label="全局通知频道 ID" path="telegram_channel_id">
                      <n-input v-model:value="configModel.telegram_channel_id" placeholder="例如: -100123456789" />
                    </n-form-item-grid-item>

                    <n-form-item-grid-item label="通知类型" path="telegram_notify_types">
                      <n-checkbox-group v-model:value="configModel.telegram_notify_types">
                        <n-space>
                          <n-checkbox value="library_new" label="入库通知" />
                          <n-checkbox value="transfer_success" label="转存通知" />
                          <n-checkbox value="playback" label="播放通知" />
                          <n-checkbox value="recognize_fail" label="识别失败" />
                          <n-checkbox value="hdhive_checkin" label="影巢签到通知" />
                        </n-space>
                      </n-checkbox-group>
                    </n-form-item-grid-item>
                  </n-card>
                </n-gi>

              </n-grid>
            </n-tab-pane>
          </n-tabs>


          <!-- 页面底部的统一保存按钮 -->
          <n-button type="primary" attr-type="submit" :loading="savingConfig" block size="large" style="margin-top: 24px;">
            保存所有设置
          </n-button>
        </n-form>
      </div>
      
      <n-alert v-else-if="configError" title="加载配置失败" type="error">
        {{ configError }}
      </n-alert>

      <div v-else>
        正在加载配置...
      </div>

    </n-space>
    
    <!-- ★★★ Cookie 扫码获取弹窗 ★★★ -->
    <n-modal v-model:show="showCookieModal" preset="card" title="扫码获取 Cookie" style="width: 400px;" :mask-closable="false">
      <n-space vertical>
        <n-alert type="info" :show-icon="true" style="margin-bottom: 10px;">
          请选择扫码的客户端类型。推荐使用 <b>支付宝小程序</b>，风控概率最低。
        </n-alert>
        
        <n-space align="center">
          <n-select 
            v-model:value="cookieAppType" 
            :options="cookieAppOptions" 
            style="width: 220px;" 
            @update:value="refreshCookieQrcode"
          />
          <n-button type="primary" @click="refreshCookieQrcode" :loading="cookieQrcodeLoading">
            刷新二维码
          </n-button>
        </n-space>

        <div style="text-align: center; padding: 20px 0; min-height: 250px;">
          <n-spin v-if="cookieQrcodeStatus === 'loading'" size="large">
            <template #description>正在获取二维码...</template>
          </n-spin>
          
          <div v-else-if="cookieQrcodeStatus === 'waiting' || cookieQrcodeStatus === 'success'">
            <n-qr-code 
              v-if="cookieQrcodeUrl" 
              :value="cookieQrcodeUrl" 
              :size="200"
              style="margin: 0 auto 20px;"
            />
            <n-alert v-if="cookieQrcodeStatus === 'waiting'" type="info" :show-icon="true">
              请选择不常用客户端，然后使用115生活APP扫码
            </n-alert>
            <n-alert v-if="cookieQrcodeStatus === 'success'" type="success" :show-icon="true">
              获取成功！Cookie 已自动保存到数据库
            </n-alert>
          </div>
          
          <div v-else-if="cookieQrcodeStatus === 'expired'">
            <n-result status="warning" title="二维码已过期">
              <template #footer>
                <n-button type="primary" @click="refreshCookieQrcode">重新获取</n-button>
              </template>
            </n-result>
          </div>
        </div>
        
        <n-divider style="margin: 0;" />
        <n-collapse>
          <n-collapse-item title="手动粘贴 Cookie (备用方案)" name="manual">
            <n-input 
              v-model:value="tempCookieInput" 
              type="textarea" 
              placeholder="UID=...; CID=...; SEID=..." 
              :rows="3" 
            />
            <n-button type="primary" size="small" style="margin-top: 8px;" @click="saveManualCookie">
              保存手动输入的 Cookie
            </n-button>
          </n-collapse-item>
        </n-collapse>
      </n-space>
      
      <template #footer>
        <n-button @click="closeCookieModal" v-if="cookieQrcodeStatus !== 'success'">关闭</n-button>
      </template>
    </n-modal>

    <!-- ★★★ 115 扫码登录弹窗 ★★★ -->
    <n-modal v-model:show="showQrcodeModal" preset="card" title="115 扫码登录" style="width: 350px;" :mask-closable="false">
      <div style="text-align: center; padding: 20px 0;">
        <!-- 加载中状态 -->
        <n-spin v-if="qrcodeStatus === 'loading'" size="large">
          <template #description>正在获取二维码...</template>
        </n-spin>
        
        <!-- 二维码显示 -->
        <div v-else-if="qrcodeStatus === 'waiting' || qrcodeStatus === 'success'">
          <n-qr-code 
            v-if="qrcodeUrl" 
            :value="qrcodeUrl" 
            :size="200"
            style="margin: 0 auto 20px;"
          />
          <n-alert v-if="qrcodeStatus === 'waiting'" type="info" :show-icon="true">
            请使用 115 APP 扫描二维码登录
          </n-alert>
          <n-alert v-if="qrcodeStatus === 'success'" type="success" :show-icon="true">
            登录成功！ Cookies 已自动保存
          </n-alert>
        </div>
        
        <!-- 过期状态 -->
        <div v-else-if="qrcodeStatus === 'expired'">
          <n-result status="warning" title="二维码已过期">
            <template #footer>
              <n-button type="primary" @click="openQrcodeModal">重新获取二维码</n-button>
            </template>
          </n-result>
        </div>
        
        <!-- 错误状态 -->
        <div v-else-if="qrcodeStatus === 'error'">
          <n-result status="error" title="获取二维码失败">
            <template #footer>
              <n-button type="primary" @click="openQrcodeModal">重试</n-button>
            </template>
          </n-result>
        </div>
      </div>
      <template #footer>
        <n-button @click="closeQrcodeModal" v-if="qrcodeStatus !== 'success'">关闭</n-button>
      </template>
    </n-modal>
    <!-- ★★★ 移植：115 目录选择器 Modal ★★★ -->
    <n-modal v-model:show="showFolderPopover" preset="card" title="选择 115 目录" style="width: 450px;" :bordered="false">
      <div class="folder-browser">
        <!-- 顶部导航 -->
        <div class="browser-header">
          <div class="nav-left">
            <n-button text size="small" @click="load115Folders('0')">
              <template #icon><n-icon size="18"><HomeIcon /></n-icon></template>
            </n-button>
            <n-divider vertical />
            <div class="breadcrumbs">
              <span v-if="currentBrowserCid === '0'">根目录</span>
              <template v-else>
                <span class="crumb-item" @click="load115Folders('0')">...</span>
                <span class="separator">/</span>
                <span class="crumb-item current">{{ currentBrowserFolderName }}</span>
              </template>
            </div>
          </div>
          <!-- 新建文件夹 -->
          <n-popover trigger="click" placement="bottom-end" :show="showCreateFolderInput" @update:show="v => showCreateFolderInput = v">
            <template #trigger>
              <n-button size="tiny" secondary type="primary">
                <template #icon><n-icon><AddIcon /></n-icon></template>
                新建
              </n-button>
            </template>
            <div style="padding: 8px; width: 200px;">
              <n-input v-model:value="newFolderName" placeholder="文件夹名称" size="small" @keyup.enter="handleCreateFolder" />
              <n-button block type="primary" size="small" style="margin-top: 8px;" @click="handleCreateFolder">确定</n-button>
            </div>
          </n-popover>
        </div>

        <!-- 搜索栏 -->
        <div style="padding: 8px 16px; border-bottom: 1px solid var(--n-divider-color); background-color: var(--n-color-modal);">
          <n-input 
            v-model:value="searchKeyword" 
            placeholder="在当前目录下搜索文件夹 (回车搜索)" 
            size="small" 
            clearable 
            @keyup.enter="handleSearchFolders"
            @clear="handleClearSearch" 
          >
            <template #prefix><n-icon><SearchIcon /></n-icon></template>
          </n-input>
        </div>

        <!-- 文件夹列表 -->
        <div class="folder-list-container">
          <n-spin :show="loadingFolders">
            <div class="folder-list">
              <n-empty v-if="folderList.length === 0 && !loadingFolders" description="空文件夹或未搜到结果" size="small" style="padding: 40px 0;" />
              <div 
                v-for="folder in folderList" 
                :key="folder.id" 
                class="folder-item"
                @click="enterFolder(folder)" 
              >
                <!-- 注意上面改成了 @click="enterFolder(folder)" -->
                <div class="folder-icon-wrapper">
                  <n-icon size="22" color="#ffca28"><FolderIcon /></n-icon>
                </div>
                <span class="folder-name">{{ folder.name }}</span>
                <n-icon size="16" color="#ccc"><ChevronRightIcon /></n-icon>
              </div>
            </div>
          </n-spin>
        </div>

        <!-- 底部确认 -->
        <div class="browser-footer">
          <div class="current-info">
            <span style="color: #666; font-size: 12px;">已选: {{ currentBrowserFolderName }}</span>
          </div>
          <n-space>
            <n-button size="small" @click="showFolderPopover = false">取消</n-button>
            <n-button type="primary" size="small" @click="confirmFolderSelection">
              确定选择
            </n-button>
          </n-space>
        </div>
      </div>
    </n-modal>

    <!-- ★★★ 本地物理目录选择器弹窗 ★★★ -->
    <n-modal v-model:show="showLocalFolderModal" preset="card" title="选择本地路径" style="width: 600px; max-width: 95vw;">
      <n-spin :show="loadingLocalFolders">
        <n-space vertical>
          <!-- 顶部路径输入与刷新 -->
          <n-input-group>
            <n-input v-model:value="currentLocalPath" placeholder="当前路径" @keyup.enter="fetchLocalFolders(currentLocalPath)" />
            <n-button type="primary" @click="fetchLocalFolders(currentLocalPath)">
              <template #icon><n-icon :component="RefreshIcon" /></template>
            </n-button>
          </n-input-group>
          
          <!-- 目录列表 -->
          <n-list hoverable clickable bordered style="max-height: 400px; overflow-y: auto; border-radius: 6px;">
            <n-list-item v-for="folder in localFolders" :key="folder.path" @click="selectLocalFolder(folder)">
              <template #prefix>
                <n-icon :component="folder.is_parent ? ArrowUpIcon : FolderIcon" size="22" :color="folder.is_parent ? '#888' : '#f0a020'" />
              </template>
              <span :style="{ fontWeight: folder.is_parent ? 'bold' : 'normal' }">{{ folder.name }}</span>
            </n-list-item>
            <n-empty v-if="localFolders.length === 0" description="空目录或无权限访问" style="margin-top: 30px; margin-bottom: 30px;" />
          </n-list>
          
          <!-- 底部按钮 -->
          <div style="display: flex; justify-content: flex-end; gap: 12px; margin-top: 16px;">
            <n-button @click="showLocalFolderModal = false">取消</n-button>
            <n-button type="primary" @click="confirmLocalFolder">确定选择此目录</n-button>
          </div>
        </n-space>
      </n-spin>
    </n-modal>
    
    <!-- ★ 引入自定义重命名模态框 -->
    <RenameConfigModal ref="renameModalRef" />
    <!-- ★ 引入音乐库管理模态框 -->
    <MusicManagerModal 
      ref="musicModalRef" 
      @open-folder-selector="(context, cid) => openFolderSelector(context, cid)" 
    />
    <!-- ★ 引入规则管理模态框 -->
    <RuleManagerModal 
      ref="ruleManagerRef" 
      @open-folder-selector="(context, cid) => openFolderSelector(context, cid)" 
    />
    <!-- 订阅源配置模态框 -->
    <MoviePilotConfigModal ref="mpModalRef" />
    <HDHiveConfigModal ref="hdhiveModalRef" />
  </n-layout>
  
  <!-- 导出选项模态框 -->
  <n-modal v-model:show="exportModalVisible" preset="dialog" title="选择要导出的数据表">
    <n-space justify="end" style="margin-bottom: 10px;">
      <n-button text type="primary" @click="selectAllForExport">全选</n-button>
      <n-button text type="primary" @click="deselectAllForExport">全不选</n-button>
    </n-space>
    <n-checkbox-group v-model:value="tablesToExport" vertical>
      <n-grid :y-gap="8" :cols="2">
        <n-gi v-for="table in allDbTables" :key="table">
          <n-checkbox :value="table">
            {{ tableInfo[table]?.cn || table }}
            <span v-if="tableInfo[table]?.isSharable" class="sharable-label"> [可共享数据]</span>
          </n-checkbox>
        </n-gi>
      </n-grid>
    </n-checkbox-group>
    <template #action>
      <n-button @click="exportModalVisible = false">取消</n-button>
      <n-button type="primary" @click="handleExport" :disabled="tablesToExport.length === 0">确认导出</n-button>
    </template>
  </n-modal>
  <!-- 导入选项模态框 -->
  <n-modal v-model:show="importModalVisible" preset="dialog" title="恢复数据库备份">
    <n-space vertical>
      <div><p><strong>文件名:</strong> {{ fileToImport?.name }}</p></div>
      
      <!-- ★★★ 核心修改：动态显示警告信息 ★★★ -->
      <n-alert v-if="importMode === 'overwrite'" title="高危操作警告" type="warning">
        此操作将使用备份文件中的数据 <strong class="warning-text">覆盖</strong> 数据库中对应的表。这是一个 <strong class="warning-text">不可逆</strong> 的过程！<br>
        <strong>请确保您正在使用自己导出的备份文件</strong>，否则可能因服务器ID不匹配而被拒绝，或导致数据错乱。
      </n-alert>
      <n-alert v-else-if="importMode === 'share'" title="共享模式导入" type="info">
        检测到备份文件来自不同的服务器。为保护您的数据安全，将以 <strong>共享模式</strong> 进行恢复。<br>
        此模式只会导入 <strong>可共享的数据</strong> (如演员元数据、翻译缓存等)，不会覆盖您现有的用户、订阅、日志等个性化配置。
      </n-alert>
      
      <div>
        <n-text strong>选择要恢复的表 (从文件中自动读取)</n-text>
        <n-space style="margin-left: 20px; display: inline-flex; vertical-align: middle;">
          <n-button size="tiny" text type="primary" @click="selectAllForImport">全选</n-button>
          <n-button size="tiny" text type="primary" @click="deselectAllForImport">全不选</n-button>
        </n-space>
      </div>
      <n-checkbox-group 
        v-model:value="tablesToImport" 
        @update:value="handleImportSelectionChange" 
        vertical 
        style="margin-top: 8px;"
      >
        <n-grid :y-gap="8" :cols="2">
          <n-gi v-for="table in tablesInBackupFile" :key="table">
            <!-- ★★★ 核心修改：根据模式禁用不可共享的表 ★★★ -->
            <n-checkbox :value="table" :disabled="isTableDisabledForImport(table)">
              {{ tableInfo[table]?.cn || table }}
              <span v-if="tableInfo[table]?.isSharable" class="sharable-label"> [可共享数据]</span>
            </n-checkbox>
          </n-gi>
        </n-grid>
      </n-checkbox-group>
    </n-space>
    <template #action>
      <n-button @click="cancelImport">取消</n-button>
      <n-button type="primary" @click="confirmImport" :disabled="tablesToImport.length === 0">确认并开始恢复</n-button>
    </template>
  </n-modal>

  <!-- 清空指定表模态框 -->
  <n-modal v-model:show="clearTablesModalVisible" preset="dialog" title="清空指定数据表">
    <n-space justify="end" style="margin-bottom: 10px;">
      <n-button text type="primary" @click="selectAllForClear">全选</n-button>
      <n-button text type="primary" @click="deselectAllForClear">全不选</n-button>
    </n-space>
    <n-alert title="高危操作警告" type="error" style="margin-bottom: 15px;">
      此操作将 <strong class="warning-text">永久删除</strong> 所选表中的所有数据，且 <strong class="warning-text">不可恢复</strong>！请务必谨慎操作。
    </n-alert>
    <n-checkbox-group 
        v-model:value="tablesToClear" 
        @update:value="handleClearSelectionChange" 
        vertical
      >
      <n-grid :y-gap="8" :cols="2">
        <n-gi v-for="table in allDbTables" :key="table">
          <n-checkbox :value="table">
            {{ tableInfo[table]?.cn || table }}
          </n-checkbox>
        </n-gi>
      </n-grid>
    </n-checkbox-group>
    <template #action>
      <n-button @click="clearTablesModalVisible = false">取消</n-button>
      <n-button type="error" @click="handleClearTables" :disabled="tablesToClear.length === 0" :loading="isClearing">确认清空</n-button>
    </template>
  </n-modal>

  <!-- ★★★ 自定义 STRM 正则模态框 ★★★ -->
    <n-modal v-model:show="showCustomRegexModal" preset="card" title="配置自定义提取正则" style="width: 650px;">
      <n-alert type="warning" :show-icon="true" style="margin-bottom: 16px;">
        <b>正则编写规则：</b><br/>
        必须使用小括号 <code>()</code> 将 115 的 PC 码包裹起来作为<b>第一个捕获组</b>。<br/>
        例如，链接为 <code>http://xxx/play?id=abcde123</code>，正则应写为：<code>id=([a-zA-Z0-9]+)</code>
      </n-alert>

      <n-dynamic-input 
        v-model:value="customRegexRules" 
        placeholder="输入正则表达式" 
        :min="0"
        style="margin-bottom: 24px;"
      />

      <n-divider title-placement="left" style="font-size: 12px; color: #999;">实时效果测试</n-divider>
      
      <n-form label-placement="left" label-width="100">
        <n-form-item label="测试链接">
          <n-input v-model:value="regexTestUrl" placeholder="输入一个未知的第三方 STRM 链接" />
        </n-form-item>
        <n-form-item label="提取结果">
          <n-alert :type="regexTestResult.type" :show-icon="true" style="width: 100%;">
            {{ regexTestResult.text }}
          </n-alert>
        </n-form-item>
      </n-form>

      <template #footer>
        <n-space justify="end">
          <n-button @click="showCustomRegexModal = false">取消</n-button>
          <n-button type="primary" @click="saveCustomRegex" :loading="isSavingRegex">保存配置</n-button>
        </n-space>
      </template>
    </n-modal>

  <!-- ★★★ 批量替换 STRM 模态框 ★★★ -->
    <n-modal v-model:show="showReplaceStrmModal" preset="card" title="批量替换本地 STRM 链接" style="width: 650px;">
      
      <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
        <b>ETK 标准格式示例 (不带文件名后缀)：</b><br/>
        <code>http://192.168.1.100:5257/api/p115/play/abcde12345</code><br/>
        <span style="font-size: 0.85em; color: gray;">(注意：标准格式以 115 的 PC 码结尾，不带斜杠和 .mkv 等后缀)</span>
      </n-alert>

      <n-form label-placement="left" label-width="100">
        <n-form-item label="替换模式">
          <n-radio-group v-model:value="replaceStrmForm.mode">
            <n-space>
              <n-radio value="plain">普通替换</n-radio>
              <n-radio value="regex">正则替换</n-radio>
            </n-space>
          </n-radio-group>
        </n-form-item>
        <n-form-item label="查找内容">
          <n-input v-model:value="replaceStrmForm.search" placeholder="例如: 192.168.1.100:5257 或 http://(.*)/api" />
        </n-form-item>
        <n-form-item label="替换为">
          <n-input v-model:value="replaceStrmForm.replace" placeholder="例如: 10.0.0.5:8080 或 https://new-domain.com/api" />
        </n-form-item>
        
        <n-divider title-placement="left" style="font-size: 12px; color: #999;">实时效果预览</n-divider>
        
        <n-form-item label="测试原始链接">
          <n-input v-model:value="replaceStrmForm.testUrl" placeholder="输入一个现有的 STRM 链接用于测试" />
        </n-form-item>
        <n-form-item label="替换后结果">
          <n-space vertical style="width: 100%;">
            <n-alert :type="previewResult.type" :show-icon="true" style="width: 100%; word-break: break-all;">
              {{ previewResult.text }}
            </n-alert>
            <!-- 实时标准格式校验提示 -->
            <n-text 
              v-if="previewResult.type === 'success' || previewResult.type === 'warning'" 
              :type="previewResult.isStandard ? 'success' : 'error'" 
              style="font-size: 0.9em; font-weight: bold; display: flex; align-items: center; gap: 4px;"
            >
              <n-icon :component="previewResult.isStandard ? CheckIcon : CloseIcon" />
              {{ previewResult.standardMsg }}
            </n-text>
          </n-space>
        </n-form-item>
      </n-form>
      
      <template #footer>
        <n-space justify="end">
          <n-button @click="showReplaceStrmModal = false">取消</n-button>
          <n-popconfirm @positive-click="submitReplaceStrm">
            <template #trigger>
              <n-button type="primary" :loading="isReplacingStrm" :disabled="!replaceStrmForm.search">确认执行替换</n-button>
            </template>
            确定要遍历本地所有 .strm 文件并执行替换吗？此操作不可逆！
          </n-popconfirm>
        </n-space>
      </template>
    </n-modal>
  <!-- 重置演员映射模态框 -->
  <n-modal 
    v-model:show="resetMappingsModalVisible" 
    preset="dialog" 
    title="确认重置Emby数据"
  >
    <n-alert title="高危操作警告" type="warning" style="margin-bottom: 15px;">
      <p style="margin: 0 0 8px 0;">此操作将 <strong>清空所有Emby相关数据</strong>。</p>
      <p style="margin: 0 0 8px 0;">它会保留宝贵的 元数据以及演员映射，以便在全量扫描后自动重新关联。</p>
      <p class="warning-text" style="margin: 0;"><strong>请仅在您已经或将要重建 Emby 媒体库时执行此操作。</strong></p>
    </n-alert>
    <template #action>
      <n-button @click="resetMappingsModalVisible = false">取消</n-button>
      <n-button type="warning" @click="handleResetActorMappings" :loading="isResettingMappings">确认重置</n-button>
    </template>
  </n-modal>
  <!-- AI 提示词配置模态框 -->
  <n-modal v-model:show="promptModalVisible" preset="dialog" title="配置 AI 提示词" style="width: 800px; max-width: 90%;">
    <n-alert type="info" style="margin-bottom: 16px;">
      您可以自定义发送给 AI 的系统指令（System Prompt）。<br>
      <b>注意：</b> 请保留关键的 JSON 输出格式要求，否则会导致解析失败。支持使用 <code>{title}</code> 等占位符。
    </n-alert>
    
    <n-spin :show="loadingPrompts">
      <n-tabs type="segment" animated>
        <n-tab-pane name="fast_mode" tab="快速模式 (人名)">
          <n-input
            v-model:value="promptsModel.fast_mode"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
        </n-tab-pane>
        <n-tab-pane name="quality_mode" tab="顾问模式 (人名)">
          <n-input
            v-model:value="promptsModel.quality_mode"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
        </n-tab-pane>
        <n-tab-pane name="overview_translation" tab="简介翻译">
          <n-input
            v-model:value="promptsModel.overview_translation"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
          <n-text depth="3" style="font-size: 12px;">可用变量: {title}, {overview}</n-text>
        </n-tab-pane>
        <n-tab-pane name="title_translation" tab="标题翻译">
          <n-input
            v-model:value="promptsModel.title_translation"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
          <n-text depth="3" style="font-size: 12px;">可用变量: {media_type}, {title}, {year}</n-text>
        </n-tab-pane>
        <n-tab-pane name="transliterate_mode" tab="音译模式">
          <n-input
            v-model:value="promptsModel.transliterate_mode"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
        </n-tab-pane>
        <n-tab-pane name="filename_parsing" tab="辅助识别">
          <n-input
            v-model:value="promptsModel.filename_parsing"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
        </n-tab-pane>
        <n-tab-pane name="batch_joke_fallback" tab="占位简介">
          <n-input
            v-model:value="promptsModel.batch_joke_fallback"
            type="textarea"
            :autosize="{ minRows: 10, maxRows: 20 }"
            placeholder="输入提示词..."
            style="font-family: monospace;"
          />
        </n-tab-pane>
      </n-tabs>
    </n-spin>

    <template #action>
      <n-space justify="space-between" style="width: 100%">
        <n-popconfirm @positive-click="resetPrompts">
          <template #trigger>
            <n-button type="warning" ghost :loading="savingPrompts">恢复默认</n-button>
          </template>
          确定要丢弃所有自定义修改，恢复到系统默认提示词吗？
        </n-popconfirm>
        
        <n-space>
          <n-button @click="promptModalVisible = false">取消</n-button>
          <n-button type="primary" @click="savePrompts" :loading="savingPrompts">保存配置</n-button>
        </n-space>
      </n-space>
    </template>
  </n-modal>
  <!-- ★★★ Pro 激活模态框 ★★★ -->
  <n-modal v-model:show="showProModal" preset="card" :title="isTransferMode ? '🔄 换绑 Pro 设备' : (configModel?.is_pro_active ? '💎 续期 Pro 高级版' : '💎 升级 Pro 高级版')" style="width: 500px;">
      <n-space vertical :size="20">
        
        <!-- 正常激活/续期模式显示的 UI -->
        <template v-if="!isTransferMode">
          <n-radio-group v-model:value="proTier" name="pro_tier_group" style="width: 100%;">
            <n-grid :cols="3" :x-gap="12">
              <n-gi>
                <n-radio-button value="month" style="width: 100%; text-align: center; padding: 10px 0; height: auto;">
                  <div style="font-size: 16px; font-weight: bold;">月付</div>
                  <div style="color: #d48806; font-size: 18px; margin-top: 4px;">￥8</div>
                </n-radio-button>
              </n-gi>
              <n-gi>
                <n-radio-button value="year" style="width: 100%; text-align: center; padding: 10px 0; height: auto;">
                  <div style="font-size: 16px; font-weight: bold;">年付</div>
                  <div style="color: #d48806; font-size: 18px; margin-top: 4px;">￥68</div>
                </n-radio-button>
              </n-gi>
              <n-gi>
                <n-radio-button value="lifetime" style="width: 100%; text-align: center; padding: 10px 0; height: auto;">
                  <div style="font-size: 16px; font-weight: bold;">终身</div>
                  <div style="color: #d48806; font-size: 18px; margin-top: 4px;">￥188(含部署)</div>
                </n-radio-button>
              </n-gi>
            </n-grid>
          </n-radio-group>

          <div style="text-align: center; background: #fafafa; padding: 15px; border-radius: 8px; border: 1px dashed #eee;">
            <n-image width="180" src="/img/wechat_pay.jpg" fallback-src="https://via.placeholder.com/180?text=WeChat+Pay" />
            <div style="margin-top: 10px; font-size: 14px;">
              请使用微信扫码支付 <b style="color: #d03050; font-size: 18px;">￥{{ proPrice }}</b>
            </div>
            <div style="margin-top: 8px; font-size: 13px; color: #666;">
              支付成功后，请截图发送给 TG: <a href="https://t.me/hbq0405" target="_blank" style="color: var(--n-primary-color); font-weight: bold;">@https://t.me/hbq0405</a> 索取激活码。
            </div>
          </div>
        </template>

        <!-- 换绑模式显示的 UI -->
        <template v-else>
          <n-alert type="info" :show-icon="true">
            如果您重装了 Emby 导致 Server ID 变更，可以在此输入您<b>最后一次购买/使用过</b>的激活码。<br/>
            系统将自动把剩余的 Pro 时长转移到当前新设备上。
          </n-alert>
        </template>

        <n-divider style="margin: 0;" />

        <n-form-item :label="isTransferMode ? '请输入曾用过的激活码：' : '请输入获取到的激活码：'">
          <n-input
            v-model:value="licenseKey"
            placeholder="例如: ETK-M-A1B2C3D4"
            size="large"
            clearable
            @keyup.enter="handleActivatePro"
          />
        </n-form-item>

        <!-- 模式切换按钮 -->
        <div style="text-align: center; font-size: 13px; color: #888;">
          <a v-if="!isTransferMode && !configModel?.is_pro_active" @click="isTransferMode = true" style="cursor: pointer; color: var(--n-primary-color); text-decoration: underline;">重装了 Emby？点击这里换绑设备</a>
          <a v-if="isTransferMode" @click="isTransferMode = false" style="cursor: pointer; color: var(--n-primary-color); text-decoration: underline;">返回激活/续期</a>
        </div>
      </n-space>

      <template #footer>
        <n-space justify="end">
          <n-button @click="showProModal = false">取消</n-button>
          <n-button type="warning" @click="handleActivatePro" :loading="isActivating" :disabled="!licenseKey">
            {{ isTransferMode ? '确认换绑' : '验证并激活' }}
          </n-button>
        </n-space>
      </template>
    </n-modal>
    <!-- ★ 引入频道监听模态框 -->
    <TGMonitorModal ref="tgMonitorModalRef" />
</template>

<script setup>
import { ref, watch, computed, onMounted, onUnmounted, nextTick, isShallow } from 'vue'; 
import TGMonitorModal from './TGMonitorModal.vue';
import { 
  NCard, NForm, NFormItem, NInputNumber, NSwitch, NButton, NGrid, NGi, 
  NSpin, NAlert, NInput, NSelect, NSpace, useMessage, useDialog,
  NFormItemGridItem, NCheckboxGroup, NCheckbox, NText, NRadioGroup, NRadio,
  NTag, NIcon, NUpload, NModal, NDivider, NInputGroup, NTabs, NTabPane, NTooltip,
  NQrCode, NResult
} from 'naive-ui';
import { 
  DownloadOutline as ExportIcon, 
  CloudUploadOutline as ImportIcon,
  TrashOutline as ClearIcon,
  BuildOutline as BuildIcon,
  AlertCircleOutline as AlertIcon,
  SyncOutline as SyncIcon,
  CloudOfflineOutline as OfflineIcon,
  FlashOutline as FlashIcon,
  Folder as FolderIcon,
  HomeOutline as HomeIcon, 
  ChevronForward as ChevronRightIcon, 
  Add as AddIcon,
  CheckmarkCircleOutline as CheckIcon,
  CloseCircleOutline as CloseIcon,
  ListOutline as ListIcon, 
  ColorWandOutline as ColorWandIcon,
  SearchOutline as SearchIcon,
  DiamondOutline as DiamondIcon,
  ArrowUpOutline as ArrowUpIcon,
  RefreshOutline as RefreshIcon,
  PaperPlaneOutline as PaperPlaneIcon,
  CloudDownloadOutline as CloudDownloadIcon
} from '@vicons/ionicons5';
import { useConfig } from '../../composables/useConfig.js';
import RenameConfigModal from './RenameConfigModal.vue';
import MusicManagerModal from './MusicManagerModal.vue';
import RuleManagerModal from './RuleManagerModal.vue'; 
import axios from 'axios';
import MoviePilotConfigModal from './MoviePilotConfigModal.vue';
import HDHiveConfigModal from './HDHiveConfigModal.vue';

const mpModalRef = ref(null);
const hdhiveModalRef = ref(null);
const tgMonitorModalRef = ref(null);
const renameModalRef = ref(null);
const musicModalRef = ref(null);
const ruleManagerRef = ref(null);
const promptModalVisible = ref(false);
const loadingPrompts = ref(false);
const savingPrompts = ref(false);
const promptsModel = ref({
  fast_mode: '',
  quality_mode: '',
  overview_translation: '',
  title_translation: '',
  transliterate_mode: '',
  filename_parsing: '',
  batch_joke_fallback: ''
});

// ★★★ 批量替换 STRM 状态与逻辑 ★★★
const showReplaceStrmModal = ref(false);
const isReplacingStrm = ref(false);
const replaceStrmForm = ref({
  mode: 'plain',
  search: '',
  replace: '',
  testUrl: 'http://192.168.1.100:5257/api/p115/play/abcde12345'
});

const openReplaceStrmModal = () => {
  if (configModel.value?.etk_server_url) {
    replaceStrmForm.value.testUrl = `${configModel.value.etk_server_url}/api/p115/play/abcde12345`;
  }
  showReplaceStrmModal.value = true;
};

const previewResult = computed(() => {
  const { mode, search, replace, testUrl } = replaceStrmForm.value;
  if (!search) return { type: 'default', text: '请输入查找内容以查看预览', isStandard: false };
  if (!testUrl) return { type: 'default', text: '请输入测试原始链接', isStandard: false };

  try {
    let resultUrl = testUrl;
    if (mode === 'plain') {
      // 普通全局替换
      resultUrl = testUrl.split(search).join(replace);
    } else if (mode === 'regex') {
      // 正则替换
      const regex = new RegExp(search, 'g');
      resultUrl = testUrl.replace(regex, replace);
    }
    
    // ★ 校验是否符合 ETK 标准格式 (不带文件名后缀)
    // 规则: http(s)://域名或IP:端口/api/p115/play/字母数字组合
    const standardRegex = /^https?:\/\/[^\/]+\/api\/p115\/play\/[a-zA-Z0-9]+$/;
    const isStandard = standardRegex.test(resultUrl);
    const standardMsg = isStandard 
      ? '校验通过：符合 ETK 标准格式' 
      : '校验失败：不符合 ETK 标准格式 (可能带有文件名后缀、路径错误或非 http 协议)';

    if (resultUrl === testUrl) {
      return { type: 'warning', text: '未发生匹配，链接保持不变：\n' + resultUrl, isStandard, standardMsg };
    }
    return { type: 'success', text: resultUrl, isStandard, standardMsg };
  } catch (e) {
    return { type: 'error', text: '正则表达式语法错误: ' + e.message, isStandard: false };
  }
});

const submitReplaceStrm = async () => {
  if (!replaceStrmForm.value.search) {
    message.warning('请输入查找内容');
    return;
  }
  isReplacingStrm.value = true;
  try {
    const response = await axios.post('/api/p115/replace_strm', {
      mode: replaceStrmForm.value.mode,
      search: replaceStrmForm.value.search,
      replace: replaceStrmForm.value.replace
    });
    if (response.data.success) {
      message.success(response.data.message);
      showReplaceStrmModal.value = false;
    } else {
      message.error(response.data.message);
    }
  } catch (error) {
    message.error(error.response?.data?.message || '请求失败');
  } finally {
    isReplacingStrm.value = false;
  }
};

const tableInfo = {
  'app_settings': { cn: '基础配置', isSharable: false },
  'person_identity_map': { cn: '演员映射表', isSharable: true },
  'actor_metadata': { cn: '演员元数据', isSharable: true },
  'translation_cache': { cn: '翻译缓存', isSharable: true },
  'actor_subscriptions': { cn: '演员订阅配置', isSharable: false },
  'collections_info': { cn: '原生合集', isSharable: false },
  'processed_log': { cn: '已处理日志', isSharable: false },
  'failed_log': { cn: '待复核日志', isSharable: false },
  'custom_collections': { cn: '自建合集', isSharable: false },
  'media_metadata': { cn: '媒体元数据', isSharable: true },
  'resubscribe_rules': { cn: '媒体洗版规则', isSharable: false },
  'resubscribe_index': { cn: '媒体洗版缓存', isSharable: false },
  'cleanup_index': { cn: '媒体去重缓存', isSharable: false },
  'emby_users': { cn: 'Emby用户', isSharable: false },
  'user_media_data': { cn: 'Emby用户数据', isSharable: false },
  'user_templates': { cn: '用户权限模板', isSharable: false },
  'invitations': { cn: '邀请链接', isSharable: false },
  'emby_users_extended': { cn: 'Emby用户扩展信息', isSharable: false },
  'p115_filesystem_cache': { cn: '115目录缓存', isSharable: false },
  'p115_mediainfo_cache': {cn: '媒体信息备份', isSharable: true },
  'p115_organize_records': {cn: '115整理记录', isSharable: false }
};
const tableDependencies = {
  'emby_users': ['user_media_data', 'emby_users_extended'],
  'user_templates': ['invitations']
};
const reverseTableDependencies = {};
for (const parent in tableDependencies) {
  for (const child of tableDependencies[parent]) {
    reverseTableDependencies[child] = parent;
  }
}
const handleClearSelectionChange = (currentSelection) => {
  const selectionSet = new Set(currentSelection);
  for (const parentTable in tableDependencies) {
    if (selectionSet.has(parentTable)) {
      const children = tableDependencies[parentTable];
      for (const childTable of children) {
        if (!selectionSet.has(childTable)) {
          selectionSet.add(childTable);
        }
      }
    }
  }
  if (selectionSet.size !== tablesToClear.value.length) {
    tablesToClear.value = Array.from(selectionSet);
  }
};
const handleImportSelectionChange = (currentSelection) => {
  const selectionSet = new Set(currentSelection);
  let changed = true;
  while (changed) {
    changed = false;
    const originalSize = selectionSet.size;
    for (const parentTable in tableDependencies) {
      if (selectionSet.has(parentTable)) {
        for (const childTable of tableDependencies[parentTable]) {
          selectionSet.add(childTable);
        }
      }
    }
    for (const childTable in reverseTableDependencies) {
      if (selectionSet.has(childTable)) {
        const parentTable = reverseTableDependencies[childTable];
        selectionSet.add(parentTable);
      }
    }
    if (selectionSet.size > originalSize) {
      changed = true;
    }
  }
  if (selectionSet.size !== tablesToImport.value.length) {
    tablesToImport.value = Array.from(selectionSet);
  }
};

const formRef = ref(null);
const formRules = { trigger: ['input', 'blur'] };
const { configModel, loadingConfig, savingConfig, configError, handleSaveConfig } = useConfig();
const message = useMessage();
const dialog = useDialog();
const isResettingMappings = ref(false);
const resetMappingsModalVisible = ref(false);
const availableLibraries = ref([]);
const loadingLibraries = ref(false);
const libraryError = ref(null);
const componentIsMounted = ref(false);
const nativeAvailableLibraries = ref([]);
const loadingNativeLibraries = ref(false);
const nativeLibraryError = ref(null);
let unwatchGlobal = null;
let unwatchEmbyConfig = null;
const isTestingProxy = ref(false);
const embyUserIdRegex = /^[a-f0-9]{32}$/i;
const isCleaningOffline = ref(false);
const isClearingVectors = ref(false);
const isTestingAI = ref(false);
// ★★★ Pro 激活相关状态与逻辑 ★★★
const showProModal = ref(false);
const licenseKey = ref('');
const isActivating = ref(false);
const proTier = ref('year'); // 默认选中年付
const isTransferMode = ref(false);

const proPrice = computed(() => {
  if (proTier.value === 'month') return '8.00';
  if (proTier.value === 'year') return '68.00';
  if (proTier.value === 'lifetime') return '188.00';
  return '0.00';
});

const handleActivatePro = async () => {
  if (!licenseKey.value.trim()) {
    message.warning('请输入激活码');
    return;
  }
  isActivating.value = true;
  try {
    const wasProBefore = configModel.value?.is_pro_active;
    
    // ★ 智能判断请求哪个接口
    const endpoint = isTransferMode.value ? '/api/system/transfer_pro' : '/api/system/activate_pro';

    const response = await axios.post(endpoint, {
      license_key: licenseKey.value.trim()
    });
    
    if (response.data.success) {
      showProModal.value = false;
      
      // 如果是换绑，或者首次激活，都需要重启容器加载 302 服务
      if (isTransferMode.value || !wasProBefore) {
        dialog.success({
          title: isTransferMode.value ? '🎉 换绑成功' : '🎉 激活成功',
          content: 'Pro 高级功能已就绪。系统将在 3 秒后自动重启以加载 302 反代服务...',
          positiveText: '好的',
          closable: false,
          maskClosable: false,
          onPositiveClick: () => {}
        });
        
        setTimeout(() => {
          triggerRestart();
        }, 3000);
      } else {
        // 续期：直接刷新页面
        dialog.success({
          title: '🎉 续期成功',
          content: '感谢您的持续支持！您的 Pro 高级版有效期已成功延长。',
          positiveText: '好的',
          onPositiveClick: () => {
            window.location.reload(); 
          }
        });
      }
      
    } else {
      message.error(response.data.message || '操作失败，请检查激活码');
    }
  } catch (error) {
    message.error(error.response?.data?.message || '连接验证服务器失败，请检查网络');
  } finally {
    isActivating.value = false;
  }
};

// ★★★ 智能判断 Pro 用户的尊贵等级 ★★★
const proStatusInfo = computed(() => {
  if (!configModel.value?.is_pro_active) {
    return { icon: '💎', text: '免费基础版', color: '#888', desc: '升级 Pro 版，解锁  302 直链 (虚拟库)。' };
  }
  
  const key = configModel.value.pro_license_key || '';
  
  if (key.includes('-L-')) {
    return { icon: '💎', text: 'Pro 终身高级版', color: '#d48806', desc: '尊贵的终身 Pro 用户，您已永久解锁全部功能！' };
  } else if (key.includes('-Y-')) {
    return { icon: '☀️', text: 'Pro 年费高级版', color: '#d48806', desc: '尊贵的年费 Pro 用户，您已解锁全部功能！' };
  } else if (key.includes('-M-')) {
    return { icon: '🌙', text: 'Pro 月费高级版', color: '#d48806', desc: '尊贵的月费 Pro 用户，您已解锁全部功能！' };
  } else {
    return { icon: '💎', text: 'Pro 高级版', color: '#d48806', desc: '尊贵的 Pro 用户，您已解锁全部功能！' };
  }
});

const isInvalidUserId = computed(() => {
  if (!configModel.value || !configModel.value.emby_user_id) return false;
  return configModel.value.emby_user_id.trim() !== '' && !embyUserIdRegex.test(configModel.value.emby_user_id);
});
const embyUserIdRule = {
  trigger: ['input', 'blur'],
  validator(rule, value) {
    if (value && !embyUserIdRegex.test(value)) {
      return new Error('ID格式不正确，应为32位。');
    }
    return true;
  }
};
const showResetMappingsModal = () => { resetMappingsModalVisible.value = true; };
const handleResetActorMappings = async () => {
  isResettingMappings.value = true;
  try {
    const response = await axios.post('/api/actions/prepare-for-library-rebuild');
    message.success(response.data.message || 'Emby数据已成功重置！');
    resetMappingsModalVisible.value = false;
  } catch (error) {
    message.error(error.response?.data?.error || '重置失败，请检查后端日志。');
  } finally {
    isResettingMappings.value = false;
  }
};
const testProxy = async () => {
  if (!configModel.value.network_http_proxy_url) {
    message.warning('请先填写 HTTP 代理地址再进行测试。');
    return;
  }
  isTestingProxy.value = true;
  try {
    const response = await axios.post('/api/proxy/test', { url: configModel.value.network_http_proxy_url });
    if (response.data.success) {
      message.success(response.data.message);
    } else {
      message.error(`测试失败: ${response.data.message}`);
    }
  } catch (error) {
    const errorMsg = error.response?.data?.message || error.message;
    message.error(`测试请求失败: ${errorMsg}`);
  } finally {
    isTestingProxy.value = false;
  }
};
const testAI = async () => {
  if (!configModel.value.ai_api_key) {
    message.warning('请先填写 API Key 再进行测试。');
    return;
  }

  isTestingAI.value = true;
  try {
    // 将当前的 configModel 传给后端进行即时测试
    const response = await axios.post('/api/ai/test', configModel.value);
    
    if (response.data.success) {
      // 使用 dialog 弹出详细结果，看起来更专业
      dialog.success({
        title: 'AI 测试成功',
        content: response.data.message,
        positiveText: '太棒了'
      });
    } else {
      message.error(`测试失败: ${response.data.message}`);
    }
  } catch (error) {
    const errorMsg = error.response?.data?.message || error.message;
    dialog.error({
      title: 'AI 测试失败',
      content: errorMsg,
      positiveText: '好吧'
    });
  } finally {
    isTestingAI.value = false;
  }
};
const openPromptModal = async () => {
  promptModalVisible.value = true;
  loadingPrompts.value = true;
  try {
    const response = await axios.get('/api/ai/prompts');
    promptsModel.value = response.data;
  } catch (error) {
    message.error('加载提示词失败');
  } finally {
    loadingPrompts.value = false;
  }
};

const savePrompts = async () => {
  savingPrompts.value = true;
  try {
    await axios.post('/api/ai/prompts', promptsModel.value);
    message.success('提示词已保存');
    promptModalVisible.value = false;
  } catch (error) {
    message.error('保存失败');
  } finally {
    savingPrompts.value = false;
  }
};

const resetPrompts = async () => {
  savingPrompts.value = true;
  try {
    const response = await axios.post('/api/ai/prompts/reset');
    promptsModel.value = response.data.prompts;
    message.success('已恢复默认提示词');
  } catch (error) {
    message.error('重置失败');
  } finally {
    savingPrompts.value = false;
  }
};
const fetchNativeViewsSimple = async () => {
  if (!configModel.value?.emby_server_url || !configModel.value?.emby_api_key || !configModel.value?.emby_user_id) {
    nativeAvailableLibraries.value = [];
    return;
  }
  loadingNativeLibraries.value = true;
  nativeLibraryError.value = null;
  try {
    const userId = configModel.value.emby_user_id;
    const response = await axios.get(`/api/emby/user/${userId}/views`, { headers: { 'X-Emby-Token': configModel.value.emby_api_key } });
    const items = response.data?.Items || [];
    nativeAvailableLibraries.value = items.map(i => ({ Id: i.Id, Name: i.Name, CollectionType: i.CollectionType }));
    if (nativeAvailableLibraries.value.length === 0) nativeLibraryError.value = "未找到原生媒体库。";
  } catch (err) {
    nativeAvailableLibraries.value = [];
    nativeLibraryError.value = `获取原生媒体库失败: ${err.response?.data?.error || err.message}`;
  } finally {
    loadingNativeLibraries.value = false;
  }
};
watch(() => configModel.value?.refresh_emby_after_update, (isRefreshEnabled) => {
  if (configModel.value && !isRefreshEnabled) {
    configModel.value.auto_lock_cast_after_update = false;
  }
});
watch(() => [configModel.value?.proxy_enabled, configModel.value?.proxy_merge_native_libraries, configModel.value?.emby_server_url, configModel.value?.emby_api_key, configModel.value?.emby_user_id], ([proxyEnabled, mergeNative, url, apiKey, userId]) => {
  if (proxyEnabled && mergeNative && url && apiKey && userId) {
    fetchNativeViewsSimple();
  } else {
    nativeAvailableLibraries.value = [];
  }
}, { immediate: true });
const aiProviderOptions = ref([
  { label: 'OpenAI (及兼容服务)', value: 'openai' },
  { label: '智谱AI (ZhipuAI)', value: 'zhipuai' },
  { label: 'Google Gemini', value: 'gemini' },
]);
const isExporting = ref(false);
const exportModalVisible = ref(false);
const allDbTables = ref([]);
const tablesToExport = ref([]);
const isImporting = ref(false);
const importModalVisible = ref(false);
const fileToImport = ref(null);
const tablesInBackupFile = ref([]);
const tablesToImport = ref([]);
const clearTablesModalVisible = ref(false);
const tablesToClear = ref([]);
const isClearing = ref(false);
const isCorrecting = ref(false);
const importMode = ref('overwrite');
const isTableDisabledForImport = (table) => {
  return importMode.value === 'share' && !tableInfo[table]?.isSharable;
};
const showClearTablesModal = async () => {
  try {
    const response = await axios.get('/api/database/tables');
    allDbTables.value = response.data;
    tablesToClear.value = [];
    clearTablesModalVisible.value = true;
  } catch (error) {
    message.error('无法获取数据库表列表，请检查后端日志。');
  }
};
const handleClearTables = async () => {
  if (tablesToClear.value.length === 0) {
    message.warning('请至少选择一个要清空的数据表。');
    return;
  }
  isClearing.value = true;
  try {
    const response = await axios.post('/api/actions/clear_tables', { tables: tablesToClear.value });
    message.success(response.data.message || '成功清空所选数据表！');
    clearTablesModalVisible.value = false;
    tablesToClear.value = [];
  } catch (error) {
    const errorMsg = error.response?.data?.error || '清空操作失败，请检查后端日志。';
    message.error(errorMsg);
  } finally {
    isClearing.value = false;
  }
};
const selectAllForClear = () => tablesToClear.value = [...allDbTables.value];
const deselectAllForClear = () => tablesToClear.value = [];
const initialRestartableConfig = ref(null);
const triggerRestart = async () => {
  message.info('正在发送重启指令...');
  try {
    await axios.post('/api/system/restart');
    message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
  } catch (error) {
    if (error.response) {
      message.error(error.response.data.error || '发送重启请求失败，请查看日志。');
    } else {
      message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
    }
  }
};

// ★★★ 115 相关逻辑 ★★★
const p115Info = ref(null);
const loading115Info = ref(false);
const showFolderPopover = ref(false);
const loadingFolders = ref(false);
const folderList = ref([]);
const currentBrowserCid = ref('0');
const currentBrowserFolderName = ref('根目录');
const newFolderName = ref('');
const showCreateFolderInput = ref(false);
const selectorContext = ref(''); 
const searchKeyword = ref('');

// ★★★ Cookie 扫码获取逻辑 ★★★
const showCookieModal = ref(false);
const tempCookieInput = ref('');
const cookieAppType = ref('alipaymini');
const cookieAppOptions = [
  { label: '115生活(支付宝小程序)', value: 'alipaymini' },
  { label: '网页版', value: 'web' },
  { label: '115生活(微信小程序)', value: 'wechatmini' },
  { label: '115生活(Android端)', value: 'android' },
  { label: '115生活(iOS端)', value: 'ios' },
  { label: '115网盘(Android电视端)', value: 'tv' }
];

const cookieQrcodeUrl = ref('');
const cookieQrcodeStatus = ref('idle'); 
const cookieQrcodeLoading = ref(false);
const cookieQrcodePolling = ref(null);

const openCookieModal = () => {
  showCookieModal.value = true;
  tempCookieInput.value = '';
  refreshCookieQrcode();
};

const refreshCookieQrcode = async () => {
  stopCookiePolling();
  cookieQrcodeStatus.value = 'loading';
  cookieQrcodeLoading.value = true;
  
  try {
    const res = await axios.get(`/api/p115/cookie_qrcode?app=${cookieAppType.value}`);
    if (res.data && res.data.success) {
      cookieQrcodeUrl.value = res.data.data.qrcode;
      cookieQrcodeStatus.value = 'waiting';
      startCookiePolling();
    } else {
      cookieQrcodeStatus.value = 'error';
      message.error(res.data?.message || '获取二维码失败');
    }
  } catch (e) {
    cookieQrcodeStatus.value = 'error';
    message.error('获取二维码失败: ' + (e.response?.data?.message || e.message));
  } finally {
    cookieQrcodeLoading.value = false;
  }
};

const startCookiePolling = () => {
  cookieQrcodePolling.value = setInterval(async () => {
    try {
      const res = await axios.get(`/api/p115/cookie_qrcode/status?app=${cookieAppType.value}`);
      const data = res.data;
      
      if (data.status === 'success') {
        cookieQrcodeStatus.value = 'success';
        message.success('Cookie 获取成功！');
        stopCookiePolling();
        setTimeout(() => {
          showCookieModal.value = false;
          check115Status(); // 刷新状态显示
        }, 1500);
      } else if (data.status === 'expired') {
        cookieQrcodeStatus.value = 'expired';
        stopCookiePolling();
      }
    } catch (e) {
      console.error('检查 Cookie 二维码状态失败', e);
    }
  }, 2000);
};

const stopCookiePolling = () => {
  if (cookieQrcodePolling.value) {
    clearInterval(cookieQrcodePolling.value);
    cookieQrcodePolling.value = null;
  }
};

const closeCookieModal = () => {
  stopCookiePolling();
  showCookieModal.value = false;
  cookieQrcodeStatus.value = 'idle';
};

const saveManualCookie = async () => {
  if (!tempCookieInput.value.trim()) {
    message.warning('请输入 Cookie');
    return;
  }
  try {
    const res = await axios.post('/api/p115/cookie', { cookie: tempCookieInput.value.trim() });
    if (res.data.success) {
      message.success('手动 Cookie 保存成功');
      showCookieModal.value = false;
      check115Status();
    }
  } catch (e) {
    message.error('保存失败: ' + (e.response?.data?.message || e.message));
  }
};

// --- 本地目录浏览器状态 ---
const showLocalFolderModal = ref(false)
const currentLocalPath = ref('')
const localFolders = ref([])
const loadingLocalFolders = ref(false)
const currentLocalTargetField = ref('')
const isCurrentLocalTargetArray = ref(false)

// 打开本地目录浏览器 (增强版)
const openLocalFolderSelector = (targetField, isArray = false) => {
    currentLocalTargetField.value = targetField
    isCurrentLocalTargetArray.value = isArray
    
    // 决定初始路径
    let startPath = '/'
    if (!isArray && configModel.value[targetField]) {
        startPath = configModel.value[targetField]
    } else if (isArray && configModel.value[targetField] && configModel.value[targetField].length > 0) {
        // 如果是数组且有值，取最后一个值的父目录作为起点，方便连续添加
        const lastPath = configModel.value[targetField][configModel.value[targetField].length - 1]
        startPath = lastPath.substring(0, lastPath.lastIndexOf('/')) || '/'
    }
    
    currentLocalPath.value = startPath
    showLocalFolderModal.value = true
    fetchLocalFolders(currentLocalPath.value)
}

// 获取本地目录列表 (保持不变)
const fetchLocalFolders = async (path) => {
    loadingLocalFolders.value = true
    try {
        const res = await axios.get('/api/p115/system/directories', { params: { path } })
        if (res.data.code === 200) {
            localFolders.value = res.data.data
            if (res.data.current_path !== undefined) {
                currentLocalPath.value = res.data.current_path
            }
        } else {
            message.error(res.data.message || '获取目录失败')
        }
    } catch (error) {
        if (error.response && error.response.status === 403) {
            message.error('没有权限访问该目录！')
        } else if (error.response && error.response.status === 404) {
            message.error('目录不存在！')
        } else {
            message.error('请求目录失败: ' + (error.response?.data?.message || error.message))
        }
    } finally {
        loadingLocalFolders.value = false
    }
}

// 点击列表中的文件夹 (保持不变)
const selectLocalFolder = (folder) => {
    fetchLocalFolders(folder.path)
}

// 确认选择 (增强版)
const confirmLocalFolder = () => {
    const field = currentLocalTargetField.value
    const path = currentLocalPath.value
    
    if (isCurrentLocalTargetArray.value) {
        // 如果是多选数组 (如 monitor_paths)
        if (!configModel.value[field]) {
            configModel.value[field] = []
        }
        // 防止重复添加
        if (!configModel.value[field].includes(path)) {
            configModel.value[field].push(path)
            message.success(`已追加路径: ${path}`)
        } else {
            message.warning('该路径已存在列表中')
        }
    } else {
        // 如果是单选字符串 (如 local_strm_root)
        configModel.value[field] = path
        message.success(`已选择路径: ${path}`)
    }
    
    showLocalFolderModal.value = false
}

// ★★★ 115 扫码授权 Modal 逻辑 ★★★
const showQrcodeModal = ref(false);
const qrcodeUrl = ref('');
const qrcodeStatus = ref('idle'); // idle, waiting, success, expired, error
const qrcodeLoading = ref(false);
const qrcodePolling = ref(null);
const handleOpenQrcodeModal = () => {
  if (!configModel.value.p115_app_id || !configModel.value.p115_app_id.trim()) {
    message.warning('请先填写自定义 AppID 并点击底部保存设置，然后再扫码授权！');
    return;
  }
  openQrcodeModal();
};
const openQrcodeModal = async () => {
  showQrcodeModal.value = true;
  qrcodeStatus.value = 'loading';
  qrcodeLoading.value = true;
  
  try {
    const res = await axios.post('/api/p115/qrcode');
    if (res.data && res.data.success) {
      qrcodeUrl.value = res.data.data.qrcode;
      qrcodeStatus.value = 'waiting';
      startPolling();
    } else {
      qrcodeStatus.value = 'error';
      message.error(res.data?.message || '获取二维码失败');
    }
  } catch (e) {
    qrcodeStatus.value = 'error';
    message.error('获取二维码失败: ' + (e.response?.data?.message || e.message));
  } finally {
    qrcodeLoading.value = false;
  }
};

const startPolling = () => {
  // 每2秒检查一次二维码状态
  qrcodePolling.value = setInterval(async () => {
    try {
      const res = await axios.get('/api/p115/qrcode/status');
      const data = res.data;
      
      if (data.status === 'success') {
        qrcodeStatus.value = 'success';
        message.success('登录成功！授权已自动保存');
        stopPolling();
        setTimeout(() => {
          showQrcodeModal.value = false;
          check115Status(); // 刷新状态显示
        }, 1500);
      } else if (data.status === 'expired') {
        qrcodeStatus.value = 'expired';
        message.warning('二维码已过期');
        stopPolling();
      }
      // waiting 状态继续轮询
    } catch (e) {
      console.error('检查二维码状态失败', e);
    }
  }, 2000);
};

const stopPolling = () => {
  if (qrcodePolling.value) {
    clearInterval(qrcodePolling.value);
    qrcodePolling.value = null;
  }
};

const closeQrcodeModal = () => {
  stopPolling();
  showQrcodeModal.value = false;
  qrcodeUrl.value = '';
  qrcodeStatus.value = 'idle';
};

// ★★★ 全自动网页授权 (授权码模式) 逻辑 ★★★
const isWebAuthing = ref(false);
let webAuthPolling = null;

const startWebAuth = () => {
  // 1. 获取当前 ETK 的访问地址 (例如 http://192.168.1.100:5257)
  const etkHost = window.location.origin; 
  // 2. 拼接回调地址
  const callbackUrl = `${etkHost}/api/p115/auto_save_auth`;
  // 3. 拼接最终的 Worker 登录地址
  const authUrl = `https://115.55565576.xyz/login?callback_url=${encodeURIComponent(callbackUrl)}`;
  
  // 4. 弹出一个居中的小窗口供用户登录
  const width = 500;
  const height = 600;
  const left = (window.screen.width / 2) - (width / 2);
  const top = (window.screen.height / 2) - (height / 2);
  window.open(authUrl, '115AuthWindow', `width=${width},height=${height},top=${top},left=${left}`);
  
  isWebAuthing.value = true;
  message.info('请在新弹出的窗口中完成 115 授权...', { duration: 5000 });
  
  // 5. 开始后台轮询，检查 Token 是否已经悄悄保存成功了
  webAuthPolling = setInterval(async () => {
    try {
      const res = await axios.get('/api/p115/status');
      if (res.data && res.data.data && res.data.data.has_token) {
        // 发现 Token 已经有了！
        clearInterval(webAuthPolling);
        isWebAuthing.value = false;
        message.success('🎉 网页授权成功！Token 已自动保存。');
        check115Status(); // 刷新界面状态
      }
    } catch (e) {
      // 轮询期间的错误静默忽略
    }
  }, 2000);
  
  // 6. 设置一个 3 分钟的超时，防止用户关了窗口导致一直转圈
  setTimeout(() => {
    if (isWebAuthing.value) {
      clearInterval(webAuthPolling);
      isWebAuthing.value = false;
      message.warning('授权等待超时，请重试。');
    }
  }, 180000);
};

// ★★★ 自定义 STRM 正则状态与逻辑 ★★★
const showCustomRegexModal = ref(false);
const customRegexRules = ref([]);
const regexTestUrl = ref('');
const isSavingRegex = ref(false);

const openCustomRegexModal = async () => {
  try {
    const res = await axios.get('/api/p115/custom_strm_regex');
    if (res.data.success) {
      customRegexRules.value = res.data.data || [];
    }
  } catch (e) {
    message.error('加载正则配置失败');
  }
  showCustomRegexModal.value = true;
};

const saveCustomRegex = async () => {
  isSavingRegex.value = true;
  try {
    const res = await axios.post('/api/p115/custom_strm_regex', {
      rules: customRegexRules.value
    });
    if (res.data.success) {
      message.success(res.data.message);
      showCustomRegexModal.value = false;
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    isSavingRegex.value = false;
  }
};

// 实时测试计算属性
const regexTestResult = computed(() => {
  const url = regexTestUrl.value.trim();
  if (!url) return { type: 'default', text: '请输入测试链接' };

  // 1. 模拟内置规则测试
  if (url.includes('/p115/play/')) {
    const pc = url.split('/p115/play/').pop().split('/')[0].split('?')[0].trim();
    return { type: 'success', text: `[内置 ETK 规则命中] 提取到 PC 码: ${pc}` };
  }
  let match = url.match(/pick_?code=([a-zA-Z0-9]+)/i);
  if (match) return { type: 'success', text: `[内置 MP 规则命中] 提取到 PC 码: ${match[1]}` };
  
  match = url.match(/\/d\/([a-zA-Z0-9]+)[.?/]/) || url.match(/\/d\/([a-zA-Z0-9]+)$/);
  if (match) return { type: 'success', text: `[内置 CMS 规则命中] 提取到 PC 码: ${match[1]}` };
  
  match = url.match(/fileid=([a-zA-Z0-9]+)/i);
  if (match) return { type: 'success', text: `[内置 MH 规则命中] 提取到 PC 码: ${match[1]}` };

  // 2. 模拟用户自定义规则测试
  for (let i = 0; i < customRegexRules.value.length; i++) {
    const rule = customRegexRules.value[i];
    if (!rule) continue;
    try {
      const regex = new RegExp(rule, 'i');
      const customMatch = url.match(regex);
      if (customMatch && customMatch[1]) {
        return { type: 'success', text: `[自定义规则 ${i + 1} 命中] 提取到 PC 码: ${customMatch[1]}` };
      }
    } catch (e) {
      return { type: 'error', text: `规则 ${i + 1} 语法错误: ${e.message}` };
    }
  }

  return { type: 'warning', text: '未命中任何规则，提取失败。请检查正则是否包含 () 捕获组。' };
});

// 检查 115 状态
const check115Status = async () => {
    loading115Info.value = true;
    try {
        // 纯粹查状态，不再触发 handleSaveConfig，彻底切断前端对后端的污染
        const res = await axios.get('/api/p115/status');
        if (res.data && res.data.data) {
            p115Info.value = res.data.data;
            message.success('115 状态刷新成功！');
        } else {
            p115Info.value = null;
        }
    } catch (e) { 
        p115Info.value = null; 
        message.error('状态获取失败: ' + (e.response?.data?.message || e.message));
    } finally { 
        loading115Info.value = false; 
    }
};

const openFolderSelector = (context, initialCid = '0') => {
  selectorContext.value = context;
  showFolderPopover.value = true;
  searchKeyword.value = ''; 
  const targetCid = (initialCid && initialCid !== '0') ? initialCid : '0';
  load115Folders(targetCid);
};

const enterFolder = (folder) => {
  searchKeyword.value = ''; 
  load115Folders(folder.id, folder.name);
};

const load115Folders = async (cid, folderName = null, isSearch = false) => {
  loadingFolders.value = true;
  try {
    const params = { cid };
    if (isSearch && searchKeyword.value) {
      params.search = searchKeyword.value;
    }
    
    const res = await axios.get('/api/p115/dirs', { params });
    if (res.data && res.data.success) {
      folderList.value = res.data.data;
      currentBrowserCid.value = cid;
      if (folderName) currentBrowserFolderName.value = folderName;
      if (cid === '0') currentBrowserFolderName.value = '根目录';
    }
  } catch (e) {
    message.error("加载目录失败: " + (e.response?.data?.message || e.message));
  } finally {
    loadingFolders.value = false;
  }
};

const handleSearchFolders = () => {
  if (!searchKeyword.value) {
    load115Folders(currentBrowserCid.value, currentBrowserFolderName.value);
  } else {
    load115Folders(currentBrowserCid.value, currentBrowserFolderName.value, true);
  }
};

const handleClearSearch = () => {
  searchKeyword.value = '';
  load115Folders(currentBrowserCid.value, currentBrowserFolderName.value);
};

const handleCreateFolder = async () => {
  if (!newFolderName.value) return;
  try {
    const res = await axios.post('/api/p115/mkdir', {
      pid: currentBrowserCid.value,
      name: newFolderName.value
    });
    if (res.data && res.data.status === 'success') {
      message.success('创建成功');
      newFolderName.value = '';
      showCreateFolderInput.value = false;
      load115Folders(currentBrowserCid.value, currentBrowserFolderName.value);
    } else {
      message.error(res.data.message || '创建失败');
    }
  } catch (e) {
    message.error("请求失败: " + e.message);
  }
};

const confirmFolderSelection = () => {
  const cid = currentBrowserCid.value;
  const name = cid === '0' ? '/' : currentBrowserFolderName.value;
  
  if (selectorContext.value === 'save_path') {
    configModel.value.p115_save_path_cid = cid;
    configModel.value.p115_save_path_name = name;
  } else if (selectorContext.value === 'unrecognized_path') {
    configModel.value.p115_unrecognized_cid = cid;
    configModel.value.p115_unrecognized_name = name;
  } else if (selectorContext.value === 'media_root') {
    configModel.value.p115_media_root_cid = cid;
    configModel.value.p115_media_root_name = name;
  } else if (selectorContext.value === 'rule') {
    ruleManagerRef.value?.updateFolder(cid, name);
  } else if (selectorContext.value === 'share_transfer') {
    shareMountModalRef.value?.updateTransferFolder(cid, name);
  } else if (selectorContext.value === 'music_root') {
    musicModalRef.value?.updateFolder(cid, name);
  } else if (selectorContext.value === 'music_upload_target') { 
    musicModalRef.value?.updateUploadTarget(cid, name);
  }
  
  message.success(`已选择: ${name}`);
  showFolderPopover.value = false;
};

// 辅助函数：获取规则摘要
const genreOptions = computed(() => {
  const map = new Map();
  [...rawMovieGenres.value, ...rawTvGenres.value].forEach(g => { if (g && g.value) map.set(g.value, g); });
  return Array.from(map.values());
});

const save = async () => {
  try {
    await formRef.value?.validate();
    const cleanConfigPayload = JSON.parse(JSON.stringify(configModel.value));
    if (configModel.value) {
        cleanConfigPayload.libraries_to_process = configModel.value.libraries_to_process;
        cleanConfigPayload.proxy_native_view_selection = configModel.value.proxy_native_view_selection;
    }
    const restartNeeded = initialRestartableConfig.value && (cleanConfigPayload.proxy_port !== initialRestartableConfig.value.proxy_port || cleanConfigPayload.log_rotation_size_mb !== initialRestartableConfig.value.log_rotation_size_mb || cleanConfigPayload.log_rotation_backup_count !== initialRestartableConfig.value.log_rotation_backup_count || cleanConfigPayload.emby_server_url !== initialRestartableConfig.value.emby_server_url);
    const performSaveAndUpdateState = async () => {
      const success = await handleSaveConfig(cleanConfigPayload);
      if (success) {
        message.success('所有设置已成功保存！');
        initialRestartableConfig.value = {
          proxy_port: cleanConfigPayload.proxy_port,
          log_rotation_size_mb: cleanConfigPayload.log_rotation_size_mb,
          log_rotation_backup_count: cleanConfigPayload.log_rotation_backup_count,
          emby_server_url: cleanConfigPayload.emby_server_url,
        };
      } else {
        message.error(configError.value || '配置保存失败，请检查后端日志。');
      }
      return success;
    };
    if (restartNeeded) {
      dialog.warning({
        title: '需要重启容器',
        content: '您修改了需要重启容器才能生效的设置（如Emby URL、虚拟库端口、日志配置等）。请选择如何操作：',
        positiveText: '保存并重启',
        negativeText: '仅保存',
        onPositiveClick: async () => {
          const saved = await performSaveAndUpdateState();
          if (saved) { await triggerRestart(); }
        },
        onNegativeClick: async () => { await performSaveAndUpdateState(); }
      });
    } else {
      await performSaveAndUpdateState();
    }
  } catch (errors) {
    message.error('请检查表单中的必填项或错误项！');
  }
};
const fetchEmbyLibrariesInternal = async () => {
  if (!configModel.value.emby_server_url || !configModel.value.emby_api_key) {
    availableLibraries.value = [];
    return;
  }
  if (loadingLibraries.value) return;
  loadingLibraries.value = true;
  libraryError.value = null;
  try {
    const response = await axios.get(`/api/emby_libraries`);
    availableLibraries.value = response.data || [];
    if (availableLibraries.value.length === 0) libraryError.value = "获取到的媒体库列表为空。";
  } catch (err) {
    availableLibraries.value = [];
    libraryError.value = `获取 Emby 媒体库失败: ${err.response?.data?.error || err.message}`;
  } finally {
    loadingLibraries.value = false;
  }
};
const showExportModal = async () => {
  try {
    const response = await axios.get('/api/database/tables');
    allDbTables.value = response.data;
    tablesToExport.value = [...response.data];
    exportModalVisible.value = true;
  } catch (error) {
    message.error('无法获取数据库表列表，请检查后端日志。');
  }
};
const handleExport = async () => {
  isExporting.value = true;
  exportModalVisible.value = false;
  try {
    const response = await axios.post('/api/database/export', { tables: tablesToExport.value }, { responseType: 'blob' });
    const contentDisposition = response.headers['content-disposition'];
    let filename = 'database_backup.json';
    if (contentDisposition) {
      const match = contentDisposition.match(/filename="?(.+?)"?$/);
      if (match?.[1]) filename = match[1];
    }
    const blobUrl = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = blobUrl;
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(blobUrl);
    message.success('数据已开始导出下载！');
  } catch (err) {
    message.error('导出数据失败，请查看日志。');
  } finally {
    isExporting.value = false;
  }
};
const selectAllForExport = () => tablesToExport.value = [...allDbTables.value];
const deselectAllForExport = () => tablesToExport.value = [];

const handleCustomImportRequest = async ({ file }) => {
  const rawFile = file.file;
  if (!rawFile) {
    message.error("未能获取到文件对象。");
    return;
  }

  const msgReactive = message.loading('正在解析备份文件...', { duration: 0 });
  
  try {
    const formData = new FormData();
    formData.append('file', rawFile);
    // ★★★ 调用我们刚刚修改过的后端预览接口 ★★★
    const response = await axios.post('/api/database/preview-backup', formData);

    msgReactive.destroy();

    const tables = response.data.tables;
    if (!tables || tables.length === 0) {
      message.error('备份文件有效，但其中不包含任何数据表。');
      return;
    }

    // ★★★ 核心修改：保存从后端获取的导入模式，并根据模式筛选默认勾选的表 ★★★
    fileToImport.value = rawFile;
    tablesInBackupFile.value = tables;
    importMode.value = response.data.import_mode || 'overwrite'; // 保存模式

    if (importMode.value === 'share') {
      // 如果是共享模式，默认只勾选可共享的表
      tablesToImport.value = tables.filter(t => tableInfo[t]?.isSharable);
      message.info("已进入共享导入模式，默认仅选择可共享的数据。");
    } else {
      // 否则，默认全选
      tablesToImport.value = [...tables];
    }
    
    importModalVisible.value = true;

  } catch (error) {
    msgReactive.destroy();
    const errorMsg = error.response?.data?.error || '解析备份文件失败，请检查文件是否有效。';
    message.error(errorMsg);
  }
};

// ★★★ 新增：Telegram 测试状态和函数 ★★★
const isTestingTelegram = ref(false);

const testTelegram = async () => {
  if (!configModel.value.telegram_bot_token || !configModel.value.telegram_channel_id) {
    message.warning('请先填写 Bot Token 和 频道 ID。');
    return;
  }

  isTestingTelegram.value = true;
  try {
    // 发送当前输入框中的配置进行测试，无需先保存
    const response = await axios.post('/api/telegram/test', {
      token: configModel.value.telegram_bot_token,
      chat_id: configModel.value.telegram_channel_id
    });
    
    if (response.data.success) {
      message.success(response.data.message);
    } else {
      message.error(`测试失败: ${response.data.message}`);
    }
  } catch (error) {
    const errorMsg = error.response?.data?.message || error.message;
    message.error(`请求失败: ${errorMsg}`);
  } finally {
    isTestingTelegram.value = false;
  }
};

const cancelImport = () => {
  importModalVisible.value = false;
  fileToImport.value = null;
};

const confirmImport = () => {
  importModalVisible.value = false; 
  startImportProcess();   
};

const startImportProcess = () => {
  if (!fileToImport.value) {
    message.error("没有要上传的文件。");
    return;
  }
  isImporting.value = true;
  const msgReactive = message.loading('正在上传并恢复数据...', { duration: 0 });

  const formData = new FormData();
  formData.append('file', fileToImport.value);
  formData.append('tables', tablesToImport.value.join(','));

  axios.post('/api/database/import', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  })
  .then(response => {
    msgReactive.destroy();
    message.success(response.data?.message || '恢复任务已成功提交！');
  })
  .catch(error => {
    msgReactive.destroy();
    const errorMsg = error.response?.data?.error || '恢复失败，未知错误。';
    message.error(errorMsg, { duration: 8000 });
  })
  .finally(() => {
    isImporting.value = false;
    fileToImport.value = null;
  });
};

// <--- 清理离线媒体
const handleCleanupOfflineMedia = async () => {
  isCleaningOffline.value = true;
  try {
    const response = await axios.post('/api/actions/cleanup-offline-media');
    const stats = response.data.data || {};
    const deletedCount = stats.media_metadata_deleted || 0;
    
    if (deletedCount > 0) {
      message.success(`瘦身成功！已清除 ${deletedCount} 条无效的离线记录。`);
    } else {
      message.success('数据库非常干净，没有发现需要清理的离线记录。');
    }
  } catch (error) {
    message.error(error.response?.data?.error || '清理失败，请检查后端日志。');
  } finally {
    isCleaningOffline.value = false;
  }
};

// <--- 清理向量数据
const handleClearVectors = async () => {
  isClearingVectors.value = true;
  try {
    const response = await axios.post('/api/actions/clear-vectors');
    message.success(response.data.message || '向量数据已清空！');
  } catch (error) {
    message.error(error.response?.data?.error || '操作失败，请检查后端日志。');
  } finally {
    isClearingVectors.value = false;
  }
};

const selectAllForImport = () => tablesToImport.value = [...tablesInBackupFile.value];
const deselectAllForImport = () => tablesToImport.value = [];

const handleCorrectSequences = async () => {
  isCorrecting.value = true;
  try {
    const response = await axios.post('/api/database/correct-sequences');
    message.success(response.data.message || 'ID计数器校准成功！');
  } catch (error) {
    message.error(error.response?.data?.error || '校准失败，请检查后端日志。');
  } finally {
    isCorrecting.value = false;
  }
};

onMounted(async () => {
  componentIsMounted.value = true;
  unwatchGlobal = watch(loadingConfig, (isLoading) => {
    if (!isLoading && componentIsMounted.value && configModel.value) {
      check115Status();
      if (!configModel.value.p115_auth_method) configModel.value.p115_auth_method = 'web';
      if (configModel.value.emby_server_url && configModel.value.emby_api_key) {
        fetchEmbyLibrariesInternal();
      }
      initialRestartableConfig.value = {
        proxy_port: configModel.value.proxy_port,
        log_rotation_size_mb: configModel.value.log_rotation_size_mb,
        log_rotation_backup_count: configModel.value.log_rotation_backup_count,
        emby_server_url: configModel.value.emby_server_url,
      };
      if (unwatchGlobal) { unwatchGlobal(); }
    }
  }, { immediate: true });
  unwatchEmbyConfig = watch(() => [configModel.value?.emby_server_url, configModel.value?.emby_api_key], (newValues, oldValues) => {
    if (componentIsMounted.value && oldValues) {
      if (newValues[0] !== oldValues[0] || newValues[1] !== oldValues[1]) {
        fetchEmbyLibrariesInternal();
      }
    }
  });
  checkUserBotStatus();
});
onUnmounted(() => {
  componentIsMounted.value = false;
  if (unwatchGlobal) unwatchGlobal();
  if (unwatchEmbyConfig) unwatchEmbyConfig();
});
</script>

<style scoped>
/* 禁用AI设置时的遮罩效果 */
.ai-settings-wrapper {
  transition: opacity 0.3s ease;
}
.content-disabled {
  opacity: 0.6;
}

/* 翻译引擎标签样式 */
.engine-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.engine-tag {
  cursor: grab;
}
.engine-tag:active {
  cursor: grabbing;
}
.drag-handle {
  margin-right: 6px;
  vertical-align: -0.15em;
}

/* ★★★ 新增的样式 ★★★ */
.description-text {
  font-size: 0.85em;
  color: var(--n-text-color-3);
  margin: 0;
  line-height: 1.6;
}
.warning-text {
  color: var(--n-warning-color-suppl); /* 使用 Naive UI 的警告色 */
  font-weight: bold;
}
.sharable-label {
  color: var(--n-info-color-suppl);
  font-size: 0.9em;
  margin-left: 4px;
  font-weight: normal;
}
.glass-section {
  background-color: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border-radius: 8px;
  border: 1px solid rgba(255, 255, 255, 0.2);
}
.info-icon {
  color: var(--n-info-color);
  cursor: help;
  font-size: 16px;
  vertical-align: middle;
}
/* ★★★ 新增：文件夹浏览器样式 ★★★ */
.folder-browser {
  display: flex;
  flex-direction: column;
  height: 500px;
  background-color: var(--n-color-modal); 
  color: var(--n-text-color);
  border-radius: 4px;
  overflow: hidden;
  border: 1px solid var(--n-divider-color);
}
.browser-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 16px;
  border-bottom: 1px solid var(--n-divider-color);
  background-color: var(--n-action-color); 
  flex-shrink: 0;
}
.nav-left { display: flex; align-items: center; flex: 1; overflow: hidden; }
.breadcrumbs {
  flex: 1; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  margin-left: 8px; display: flex; align-items: center; color: var(--n-text-color-3);
}
.crumb-item { cursor: pointer; transition: color 0.2s; }
.crumb-item:hover { color: var(--n-primary-color); }
.crumb-item.current { color: var(--n-text-color-1); font-weight: 600; cursor: default; }
.separator { margin: 0 6px; color: var(--n-text-color-disabled); }
.folder-list-container { flex: 1; overflow-y: auto; position: relative; }
.folder-list { padding: 4px 0; }
.folder-item {
  display: flex; align-items: center; padding: 10px 16px; cursor: pointer;
  transition: background-color 0.2s; border-bottom: 1px solid var(--n-divider-color);
  color: var(--n-text-color-2);
}
.folder-item:hover { background-color: var(--n-hover-color); }
.folder-icon-wrapper { display: flex; align-items: center; margin-right: 12px; }
.folder-name { flex: 1; font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--n-text-color-1); }
.browser-footer {
  padding: 12px 16px; border-top: 1px solid var(--n-divider-color);
  display: flex; justify-content: space-between; align-items: center;
  background-color: var(--n-color-modal); flex-shrink: 0;
}
.rules-container { background: transparent; border: none; padding: 0; }
.rule-item {
  display: flex; align-items: center; background-color: var(--n-action-color); 
  border: 1px solid var(--n-divider-color); padding: 12px; margin-bottom: 8px; border-radius: 6px; transition: all 0.2s;
}
.rule-item:hover { border-color: var(--n-primary-color); background-color: var(--n-hover-color); }
.drag-handle { cursor: grab; color: #999; margin-right: 12px; padding: 4px; }
.drag-handle:active { cursor: grabbing; }
.rule-info { flex: 1; }
.rule-name { font-weight: bold; font-size: 13px; color: var(--n-text-color-1); }
.rule-desc span { color: var(--n-text-color-3); }
.rule-actions { display: flex; align-items: center; gap: 4px; }
</style>
