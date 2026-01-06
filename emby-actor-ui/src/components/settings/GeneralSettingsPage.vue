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
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                <!-- 左侧列 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">基础设置</span></template>
                    <n-form-item-grid-item label="处理项目间的延迟 (秒)" path="delay_between_items_sec">
                      <n-input-number v-model:value="configModel.delay_between_items_sec" :min="0" :step="0.1" placeholder="例如: 0.5"/>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="豆瓣API默认冷却时间 (秒)" path="api_douban_default_cooldown_seconds">
                      <n-input-number v-model:value="configModel.api_douban_default_cooldown_seconds" :min="0.1" :step="0.1" placeholder="例如: 1.0"/>
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
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">开启后，角色名前会加上“饰 ”或“配 ”。关闭则直接显示角色名。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="移除无头像的演员" path="remove_actors_without_avatars">
                      <n-switch v-model:value="configModel.remove_actors_without_avatars" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          开启后，在最终演员表移除那些找不到任何可用头像的演员。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                  </n-card>
                </n-gi>
                <!-- 右侧列 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">数据源与API</span></template>
                    <n-form-item label="本地数据源路径" path="local_data_path">
                      <n-input v-model:value="configModel.local_data_path" placeholder="神医TMDB缓存目录 (cache和override的上层)" />
                    </n-form-item>
                    <n-form-item label="TMDB API Key" path="tmdb_api_key">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.tmdb_api_key" placeholder="输入你的 TMDB API Key" />
                    </n-form-item>
                    <n-form-item label="TMDB API Base URL" path="tmdb_api_base_url">
                      <n-input v-model:value="configModel.tmdb_api_base_url" placeholder="https://api.themoviedb.org/3" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">TMDb API的基础URL，通常不需要修改。可通过环境变量 TMDB_API_BASE_URL 设置。</n-text></template>
                    </n-form-item>
                    <n-form-item label="允许成人内容探索" path="tmdb_include_adult">
                      <n-space align="center">
                        <n-switch v-model:value="configModel.tmdb_include_adult" />
                        <n-text depth="3" style="font-size: 0.9em; margin-left: 8px;">
                          开启后，仅当在探索页面筛选“成人”分级时，才会返回 TMDb 成人内容。
                        </n-text>
                      </n-space>
                    </n-form-item>
                    <n-form-item label="GitHub 个人访问令牌" path="github_token">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.github_token" placeholder="可选，用于提高API请求频率限制"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;"><a href="https://github.com/settings/tokens/new" target="_blank" style="font-size: 1.3em; margin-left: 8px; color: var(--n-primary-color); text-decoration: underline;">免费申请GithubTOKEN</a></n-text></template>
                    </n-form-item>
                    <n-form-item label="豆瓣登录 Cookie" path="douban_cookie">
                      <n-input type="password" show-password-on="mousedown" v-model:value="configModel.douban_cookie" placeholder="从浏览器开发者工具中获取"/>
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">非必要不用配置，当日志频繁出现“豆瓣API请求失败: 需要登录...”的提示时再配置。</n-text></template>
                    </n-form-item>
                  </n-card>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 2: Emby (紧凑双列版) ================== -->
            <n-tab-pane name="emby" tab="Emby & 虚拟库">
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
                    <template #header><span class="card-title">虚拟库</span></template>
                    
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
                      
                      <!-- 3. 缺失占位符 (占满一行，因为说明文字较长) -->
                      <n-form-item-grid-item label="缺失占位符" path="proxy_show_missing_placeholders" span="1 m:2" label-width="100">
                         <n-space align="center">
                            <n-switch v-model:value="configModel.proxy_show_missing_placeholders" :disabled="!configModel.proxy_enabled"/>
                            <n-text depth="3" style="font-size: 0.8em;">在榜单中显示未入库海报</n-text>
                         </n-space>
                      </n-form-item-grid-item>

                      <!-- 4. 302重定向 (占满一行，URL通常较长) -->
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
            <n-tab-pane name="services" tab="智能服务">
              <!-- ★★★ 修改点1: cols 改为 "1 l:3"，总共3列 ★★★ -->
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- 左侧: AI翻译 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card" style="height: 100%;">
                    <template #header><span class="card-title">AI翻译</span></template>
                    <template #header-extra>
                      <n-space align="center">
                        <n-button 
                          size="tiny" 
                          type="primary" 
                          ghost 
                          @click="testAI" 
                          :loading="isTestingAI"
                          :disabled="!configModel.ai_translation_enabled || !configModel.ai_api_key"
                        >
                          测试连接
                        </n-button>
                        <n-switch v-model:value="configModel.ai_translation_enabled" />
                        <a href="https://cloud.siliconflow.cn/i/GXIrubbL" target="_blank" style="font-size: 0.85em; color: var(--n-primary-color); text-decoration: underline;">注册硅基流动</a>
                      </n-space>
                    </template>
                    <div class="ai-settings-wrapper" :class="{ 'content-disabled': !configModel.ai_translation_enabled }">
                      <n-form-item label="AI翻译模式" path="ai_translation_mode">
                        <n-radio-group v-model:value="configModel.ai_translation_mode" name="ai_translation_mode" :disabled="!configModel.ai_translation_enabled">
                          <n-space vertical><n-radio value="fast">翻译模式 (速度优先)</n-radio><n-radio value="quality">顾问模式 (质量优先)</n-radio></n-space>
                        </n-radio-group>
                        <template #feedback><n-text depth="3" style="font-size:0.8em;"><b>顾问模式：</b>结合上下文翻译，准确率更高，但无缓存，耗时且成本高。</n-text></template>
                      </n-form-item>
                      <n-form-item label="AI 服务商" path="ai_provider"><n-select v-model:value="configModel.ai_provider" :options="aiProviderOptions" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="API Key" path="ai_api_key"><n-input type="password" show-password-on="mousedown" v-model:value="configModel.ai_api_key" placeholder="输入你的 API Key" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="模型名称" path="ai_model_name"><n-input v-model:value="configModel.ai_model_name" placeholder="例如: gpt-3.5-turbo, glm-4" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="API Base URL (可选)" path="ai_base_url"><n-input v-model:value="configModel.ai_base_url" placeholder="用于代理或第三方兼容服务" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                    </div>
                  </n-card>
                </n-gi>

                <!-- 右侧: MoviePilot订阅 -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card" style="height: 100%;">
                    <template #header><span class="card-title">MoviePilot订阅</span></template>
                    
                    <!-- ★★★ 修改点3: 内部使用 Grid 布局实现双列内容，压缩高度 ★★★ -->
                    <n-grid cols="1 m:2" :x-gap="24" responsive="screen">
                      
                      <!-- 1. 连接设置区域 -->
                      <n-gi span="1 m:2">
                        <n-form-item-grid-item label="MoviePilot URL" path="moviepilot_url">
                          <n-input v-model:value="configModel.moviepilot_url" placeholder="例如: http://192.168.1.100:3000"/>
                        </n-form-item-grid-item>
                      </n-gi>
                      <n-gi span="1 m:2">
                        <n-form-item-grid-item label="用户名" path="moviepilot_username">
                          <n-input v-model:value="configModel.moviepilot_username" placeholder="登录用户名"/>
                        </n-form-item-grid-item>
                      </n-gi>
                      <n-gi span="1 m:2">
                        <n-form-item-grid-item label="密码" path="moviepilot_password">
                          <n-input type="password" show-password-on="mousedown" v-model:value="configModel.moviepilot_password" placeholder="登录密码"/>
                        </n-form-item-grid-item>
                      </n-gi>

                      <n-gi span="1 m:2">
                        <n-form-item-grid-item label="启用智能订阅" path="autosub_enabled">
                          <n-switch v-model:value="configModel.autosub_enabled" :disabled="!isMoviePilotConfigured" />
                          <template #feedback><n-text depth="3" style="font-size:0.8em;">总开关。开启后定时任务才会执行。</n-text></template>
                        </n-form-item-grid-item>
                      </n-gi>

                      <!-- 分割线 -->
                      <n-gi span="1 m:2">
                        <n-divider title-placement="left" style="margin: 10px 0 20px 0;">每日订阅额度</n-divider>
                      </n-gi>

                      <!-- 3. 额度区域 (左右并排) -->
                      <n-gi>
                        <n-form-item-grid-item label="每日订阅上限" path="resubscribe_daily_cap">
                          <n-input-number v-model:value="configModel.resubscribe_daily_cap" :min="1" :disabled="!isMoviePilotConfigured" style="width: 100%;" />
                          <template #feedback><n-text depth="3" style="font-size:0.8em;">超过数量停止任务，0点重置。</n-text></template>
                        </n-form-item-grid-item>
                      </n-gi>
                      <n-gi>
                        <n-form-item-grid-item label="订阅请求间隔 (秒)" path="resubscribe_delay_seconds">
                          <n-input-number v-model:value="configModel.resubscribe_delay_seconds" :min="0.1" :step="0.1" :disabled="!isMoviePilotConfigured" style="width: 100%;" />
                          <template #feedback><n-text depth="3" style="font-size:0.8em;">避免请求过快冲击服务器。</n-text></template>
                        </n-form-item-grid-item>
                      </n-gi>
                    </n-grid>
                  </n-card>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 4: 高级 (核心修改区域) ================== -->
            <n-tab-pane name="advanced" tab="高级">
              <!-- ★★★ 核心修改：将4个卡片平铺在Grid中，每个卡片一个 n-gi ★★★ -->
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- 卡片 1: 网络代理 (左上) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">网络代理</span></template>
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

                <!-- 卡片 4: 用户注册设置 (右下) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">通知设置</span></template>
                    <n-form-item-grid-item label="Telegram Bot Token" path="telegram_bot_token">
                      <n-input 
                        v-model:value="configModel.telegram_bot_token" 
                        type="password" 
                        show-password-on="click"
                        placeholder="从 @BotFather 获取" 
                      />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          用于发送通知的 Telegram 机器人令牌。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="全局通知频道 ID" path="telegram_channel_id">
                      <n-input 
                        v-model:value="configModel.telegram_channel_id" 
                        placeholder="例如: -100123456789" 
                      />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          用于发送全局入库等通知的公开频道或群组的 Chat ID。
                        </n-text>
                      </template>
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
</template>

<script setup>
import { ref, watch, computed, onMounted, onUnmounted, nextTick, isShallow } from 'vue'; 
import draggable from 'vuedraggable';
import { 
  NCard, NForm, NFormItem, NInputNumber, NSwitch, NButton, NGrid, NGi, 
  NSpin, NAlert, NInput, NSelect, NSpace, useMessage, useDialog,
  NFormItemGridItem, NCheckboxGroup, NCheckbox, NText, NRadioGroup, NRadio,
  NTag, NIcon, NUpload, NModal, NDivider, NInputGroup, NTabs, NTabPane, NTooltip
} from 'naive-ui';
import { 
  MoveOutline as DragHandleIcon,
  DownloadOutline as ExportIcon, 
  CloudUploadOutline as ImportIcon,
  TrashOutline as ClearIcon,
  BuildOutline as BuildIcon,
  AlertCircleOutline as AlertIcon,
  SyncOutline as SyncIcon,
  CloudOfflineOutline as OfflineIcon,
  FlashOutline as FlashIcon
} from '@vicons/ionicons5';
import { useConfig } from '../../composables/useConfig.js';
import axios from 'axios';

// ... (从 tableInfo 到 handleImportSelectionChange 的所有代码保持不变) ...
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
  'emby_users_extended': { cn: 'Emby用户扩展信息', isSharable: false }
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
const isMoviePilotConfigured = computed(() => {
  if (!configModel.value) return false;
  return !!(configModel.value.moviepilot_url && configModel.value.moviepilot_username && configModel.value.moviepilot_password);
});
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
const save = async () => {
  try {
    await formRef.value?.validate();
    const cleanConfigPayload = JSON.parse(JSON.stringify(configModel.value));
    if (configModel.value) {
        cleanConfigPayload.libraries_to_process = configModel.value.libraries_to_process;
        cleanConfigPayload.proxy_native_view_selection = configModel.value.proxy_native_view_selection;
    }
    const restartNeeded = initialRestartableConfig.value && (cleanConfigPayload.proxy_port !== initialRestartableConfig.value.proxy_port || cleanConfigPayload.proxy_302_redirect_url !== initialRestartableConfig.value.proxy_302_redirect_url || cleanConfigPayload.log_rotation_size_mb !== initialRestartableConfig.value.log_rotation_size_mb || cleanConfigPayload.log_rotation_backup_count !== initialRestartableConfig.value.log_rotation_backup_count || cleanConfigPayload.emby_server_url !== initialRestartableConfig.value.emby_server_url);
    const performSaveAndUpdateState = async () => {
      const success = await handleSaveConfig(cleanConfigPayload);
      if (success) {
        message.success('所有设置已成功保存！');
        initialRestartableConfig.value = {
          proxy_port: cleanConfigPayload.proxy_port,
          proxy_302_redirect_url: cleanConfigPayload.proxy_302_redirect_url,
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
onMounted(() => {
  componentIsMounted.value = true;
  unwatchGlobal = watch(loadingConfig, (isLoading) => {
    if (!isLoading && componentIsMounted.value && configModel.value) {
      if (configModel.value.emby_server_url && configModel.value.emby_api_key) {
        fetchEmbyLibrariesInternal();
      }
      initialRestartableConfig.value = {
        proxy_port: configModel.value.proxy_port,
        proxy_302_redirect_url: configModel.value.proxy_302_redirect_url,
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
</style>