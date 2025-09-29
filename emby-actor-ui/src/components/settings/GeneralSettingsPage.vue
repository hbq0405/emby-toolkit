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
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">最大演员表数量。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="为角色名添加前缀" path="actor_role_add_prefix">
                      <n-switch v-model:value="configModel.actor_role_add_prefix" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">开启后，角色名前会加上“饰 ”或“配 ”。关闭则直接显示角色名。</n-text></template>
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="更新后刷新 Emby 媒体项">
                      <n-switch v-model:value="configModel.refresh_emby_after_update" />
                    </n-form-item-grid-item>
                    <n-form-item-grid-item label="自动锁定演员表" path="auto_lock_cast_after_update">
                      <n-switch v-model:value="configModel.auto_lock_cast_after_update" :disabled="!configModel.refresh_emby_after_update"/>
                      <template #feedback>【酌情开启】开启后，处理完演员表后，会自动将该项目的“演员”字段锁定，防止被后续刷新操作覆盖。</template>
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

            <!-- ================== 标签页 2: Emby (最终版 - 左右卡片布局) ================== -->
            <n-tab-pane name="emby" tab="Emby & 虚拟库">
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                
                <!-- ########## 左侧卡片: Emby 连接设置 ########## -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">Emby 连接设置</span></template>
                    <n-space vertical :size="18">
                      <!-- ▼▼▼ 修改点1: Emby URL 增加重启提示 ▼▼▼ -->
                      <n-form-item-grid-item>
                        <template #label>
                          <n-space align="center">
                            <span>Emby 服务器 URL</span>
                            <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" />
                              </template>
                              此项修改需要重启容器才能生效。
                            </n-tooltip>
                          </n-space>
                        </template>
                        <n-input v-model:value="configModel.emby_server_url" placeholder="例如: http://localhost:8096" />
                      </n-form-item-grid-item>
                      <n-form-item-grid-item label="Emby API Key" path="emby_api_key">
                        <n-input v-model:value="configModel.emby_api_key" type="password" show-password-on="click" placeholder="输入你的 Emby API Key" />
                      </n-form-item-grid-item>
                      <n-form-item-grid-item label="Emby 用户 ID" :rule="embyUserIdRule" path="emby_user_id">
                        <n-input v-model:value="configModel.emby_user_id" placeholder="请输入32位的用户ID" />
                        <template #feedback>
                          <div v-if="isInvalidUserId" style="color: #e88080; font-size: 12px;">格式错误！ID应为32位字母和数字。</div>
                          <div v-else style="font-size: 12px; color: #888;">提示：请从 Emby 后台用户管理页的地址栏复制 userId。</div>
                        </template>
                      </n-form-item-grid-item>
                      <n-divider title-placement="left" style="margin: 10px 0;">管理员登录凭证 (用于删除等高级操作)</n-divider>
                      <n-form-item-grid-item label="Emby 管理员用户名" path="emby_admin_user">
                        <n-input v-model:value="configModel.emby_admin_user" placeholder="输入用于登录的管理员用户名" />
                      </n-form-item-grid-item>

                      <n-form-item-grid-item label="Emby 管理员密码" path="emby_admin_pass">
                        <n-input 
                          v-model:value="configModel.emby_admin_pass" 
                          type="password" 
                          show-password-on="click" 
                          placeholder="输入对应的密码" 
                        />
                        <template #feedback>
                          <n-text depth="3" style="font-size:0.8em;">
                            此凭证仅用于执行删除媒体等需要临时令牌的高级操作，不会被用于常规扫描。
                          </n-text>
                        </template>
                      </n-form-item-grid-item>

                      <n-divider style="margin: 10px 0;" />
                      <n-form-item-grid-item label="Emby API 超时时间 (秒)" path="emby_api_timeout">
                        <n-input-number v-model:value="configModel.emby_api_timeout" :min="15" :step="5" placeholder="建议 30-90" style="width: 100%;" />
                        <template #feedback>
                          <n-text depth="3" style="font-size:0.8em;">
                            当Emby服务器性能较差或媒体库巨大时，适当增加此值可防止网络请求失败。
                          </n-text>
                        </template>
                      </n-form-item-grid-item>
                      <n-divider title-placement="left" style="margin-top: 10px;">选择要处理的媒体库</n-divider>
                      
                      <n-form-item-grid-item label-placement="top">
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
                    </n-space>
                  </n-card>
                </n-gi>

                <!-- ########## 右侧卡片: 虚拟库 (反向代理) ########## -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">虚拟库</span></template>
                    <n-space vertical :size="18">
                      <n-form-item-grid-item label="启用" path="proxy_enabled">
                        <n-switch v-model:value="configModel.proxy_enabled" />
                        <template #feedback><n-text depth="3" style="font-size:0.8em;">开启后，自动将自建合集虚拟成媒体库，用下方设置的端口访问。</n-text></template>
                      </n-form-item-grid-item>
                      <n-form-item-grid-item label="合并原生媒体库" path="proxy_merge_native_libraries">
                        <n-switch v-model:value="configModel.proxy_merge_native_libraries" :disabled="!configModel.proxy_enabled"/>
                        <template #feedback><n-text depth="3" style="font-size:0.8em;">开启后，将在虚拟库列表合并显示您在 Emby 中配置的原生媒体库。</n-text></template>
                      </n-form-item-grid-item>
                      <n-form-item-grid-item label="原生媒体库显示位置" path="proxy_native_view_order">
                        <n-radio-group v-model:value="configModel.proxy_native_view_order" :disabled="!configModel.proxy_enabled || !configModel.proxy_merge_native_libraries">
                          <n-radio value="before">显示在虚拟库前面</n-radio>
                          <n-radio value="after">显示在虚拟库后面</n-radio>
                        </n-radio-group>
                      </n-form-item-grid-item>
                      <n-form-item-grid-item>
                        <template #label>
                          <n-space align="center">
                            <span>虚拟库访问端口</span>
                            <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" />
                              </template>
                              此项修改需要重启容器才能生效。
                            </n-tooltip>
                          </n-space>
                        </template>
                        <n-input-number v-model:value="configModel.proxy_port" :min="1025" :max="65535" :disabled="!configModel.proxy_enabled"/>
                        <template #feedback><n-text depth="3" style="font-size:0.8em;">非host模式需要映射。</n-text></template>
                      </n-form-item-grid-item>
                      <n-form-item-grid-item>
                        <template #label>
                          <n-space align="center">
                            <span>302重定向服务地址</span>
                             <n-tooltip trigger="hover">
                              <template #trigger>
                                <n-icon :component="AlertIcon" class="info-icon" />
                              </template>
                              此项修改需要重启容器才能生效。
                            </n-tooltip>
                          </n-space>
                        </template>
                        <n-input 
                          v-model:value="configModel.proxy_302_redirect_url" 
                          placeholder="例如: http://192.168.31.177:9096" 
                          :disabled="!configModel.proxy_enabled"
                        />
                        <template #feedback>
                          <n-text depth="3" style="font-size:0.8em;">
                            填写独立的302重定向服务URL。所有视频播放请求直接转发到此地址。
                          </n-text>
                        </template>
                      </n-form-item-grid-item>

                      <n-divider title-placement="left" style="margin-top: 10px;">选择合并显示的原生媒体库</n-divider>

                      <n-form-item-grid-item 
                        v-if="configModel.proxy_enabled && configModel.proxy_merge_native_libraries" 
                        path="proxy_native_view_selection" 
                        label-placement="top"
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
                    </n-space>
                  </n-card>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <!-- ================== 标签页 3: 智能服务 ================== -->
            <n-tab-pane name="services" tab="智能服务">
              <n-grid cols="1 l:2" :x-gap="24" :y-gap="24" responsive="screen">
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">AI翻译</span></template>
                    <template #header-extra>
                      <n-space align="center">
                        <n-switch v-model:value="configModel.ai_translation_enabled" />
                        <a href="https://cloud.siliconflow.cn/i/GXIrubbL" target="_blank" style="font-size: 0.85em; color: var(--n-primary-color); text-decoration: underline;">注册硅基流动，新人送2000万tokens</a>
                      </n-space>
                    </template>
                    <div class="ai-settings-wrapper" :class="{ 'content-disabled': !configModel.ai_translation_enabled }">
                      <n-form-item label="AI翻译模式" path="ai_translation_mode">
                        <n-radio-group v-model:value="configModel.ai_translation_mode" name="ai_translation_mode" :disabled="!configModel.ai_translation_enabled">
                          <n-space><n-radio value="fast">翻译模式 (速度优先)</n-radio><n-radio value="quality">顾问模式 (质量优先)</n-radio></n-space>
                        </n-radio-group>
                        <template #feedback><n-text depth="3" style="font-size:0.8em;"><b>翻译模式：</b>采用三段式翻译，依次用普通模式、音译模式、顾问模式进行翻译。<br><b>顾问模式：</b>结合上下文翻译，准确率更高，但无缓存，耗时且成本高，适合手动处理单项媒体。</n-text></template>
                      </n-form-item>
                      <n-form-item label="AI 服务商" path="ai_provider"><n-select v-model:value="configModel.ai_provider" :options="aiProviderOptions" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="API Key" path="ai_api_key"><n-input type="password" show-password-on="mousedown" v-model:value="configModel.ai_api_key" placeholder="输入你的 API Key" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="模型名称" path="ai_model_name"><n-input v-model:value="configModel.ai_model_name" placeholder="例如: gpt-3.5-turbo, glm-4" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                      <n-form-item label="API Base URL (可选)" path="ai_base_url"><n-input v-model:value="configModel.ai_base_url" placeholder="用于代理或第三方兼容服务" :disabled="!configModel.ai_translation_enabled"/></n-form-item>
                    </div>
                  </n-card>
                </n-gi>
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">MoviePilot订阅</span></template>
                    <n-form-item-grid-item label="MoviePilot URL" path="moviepilot_url"><n-input v-model:value="configModel.moviepilot_url" placeholder="例如: http://192.168.1.100:3000"/></n-form-item-grid-item>
                    <n-form-item-grid-item label="用户名" path="moviepilot_username"><n-input v-model:value="configModel.moviepilot_username" placeholder="输入 MoviePilot 的登录用户名"/></n-form-item-grid-item>
                    <n-form-item-grid-item label="密码" path="moviepilot_password"><n-input type="password" show-password-on="mousedown" v-model:value="configModel.moviepilot_password" placeholder="输入 MoviePilot 的登录密码"/></n-form-item-grid-item>
                    
                    <n-divider title-placement="left" style="margin-top: 20px; margin-bottom: 20px;">智能订阅与洗版</n-divider>
                    
                    <n-form-item-grid-item label="启用智能订阅" path="autosub_enabled">
                      <n-switch v-model:value="configModel.autosub_enabled" :disabled="!isMoviePilotConfigured" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">总开关。开启后，智能订阅定时任务才会真正执行订阅操作。</n-text></template>
                    </n-form-item-grid-item>

                    <n-form-item-grid-item label="启用自定义洗版订阅" path="use_custom_resubscribe">
                      <n-switch v-model:value="configModel.use_custom_resubscribe" :disabled="!isMoviePilotConfigured" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          <b>开启：</b>根据“媒体洗版”页面的规则，发送带分辨率/质量等参数的精确订阅。<br>
                          <b>关闭：</b>使用 MoviePilot 的全局洗版功能，忽略本地规则中的具体参数。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>

                    <n-form-item-grid-item label="对缺集的已完结剧集触发洗版" path="resubscribe_completed_on_missing">
                      <n-switch v-model:value="configModel.resubscribe_completed_on_missing" :disabled="!isMoviePilotConfigured" />
                      <template #feedback><n-text depth="3" style="font-size:0.8em;">开启后，“洗版缺集的季”任务会检查所有已完结剧集，若本地文件不完整，则向MoviePilot提交整季的洗版订阅 。</n-text></template>
                    </n-form-item-grid-item>

                    <!-- ★★★ 核心修改：在这里添加阀门设置 ★★★ -->
                    <n-divider title-placement="left" style="margin-top: 20px; margin-bottom: 20px;">每日订阅额度</n-divider>

                    <n-form-item-grid-item label="每日订阅上限" path="resubscribe_daily_cap">
                      <n-input-number v-model:value="configModel.resubscribe_daily_cap" :min="1" :disabled="!isMoviePilotConfigured" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          若每日订阅的项目超过此数量，任务将自动中止，每天0点重置。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>

                    <n-form-item-grid-item label="订阅请求间隔 (秒)" path="resubscribe_delay_seconds">
                      <n-input-number v-model:value="configModel.resubscribe_delay_seconds" :min="0.1" :step="0.1" :disabled="!isMoviePilotConfigured" />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          每次成功提交订阅后，等待指定的秒数再提交下一个，以避免对MoviePilot服务器造成冲击。
                        </n-text>
                      </template>
                    </n-form-item-grid-item>

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
                        <n-upload :custom-request="handleCustomImportRequest" :show-file-list="false" accept=".json"><n-button :loading="isImporting" class="action-button"><template #icon><n-icon :component="ImportIcon" /></template>导入数据</n-button></n-upload>
                        <n-button @click="showClearTablesModal" :loading="isClearing" class="action-button" type="error" ghost><template #icon><n-icon :component="ClearIcon" /></template>清空指定表</n-button>
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
                          重置演员映射
                        </n-button>
                      </n-space>
                      <p class="description-text"><b>导出：</b>将数据库中的一个或多个表备份为 JSON 文件。<br><b>导入：</b>从 JSON 备份文件中恢复数据。<br><b>清空：</b>删除指定表中的所有数据，此操作不可逆。<br><b>校准：</b>修复导入数据可能引起的自增序号错乱的问题。<br><b>重置：</b>在重建 Emby 媒体库后，使用此功能清空旧的 Emby 演员ID，然后执行一次“同步演员映射”任务即可重新建立关联。</p>
                    </n-space>
                  </n-card>
                </n-gi>

                <!-- 卡片 4: 用户注册设置 (右下) -->
                <n-gi>
                  <n-card :bordered="false" class="dashboard-card">
                    <template #header><span class="card-title">用户注册设置</span></template>
                    <n-form-item-grid-item label="注册成功跳转地址" path="registration_redirect_url">
                      <n-input 
                        v-model:value="configModel.registration_redirect_url" 
                        placeholder="留空则默认跳转到 Emby 服务器地址" 
                      />
                      <template #feedback>
                        <n-text depth="3" style="font-size:0.8em;">
                          新用户通过邀请链接注册成功后，将自动跳转到您在此处设置的 URL。
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
      
      <!-- ★ 修改点: 增加醒目的警告提示 -->
      <n-alert title="高危操作警告" type="warning">
        此操作将使用备份文件中的数据 <strong class="warning-text">覆盖</strong> 数据库中对应的表。这是一个 <strong class="warning-text">不可逆</strong> 的过程！<br>
        <strong>请确保您正在使用自己导出的备份文件</strong>，否则可能因服务器ID不匹配而被拒绝，或导致数据错乱。
      </n-alert>

      <!-- ★ 修改点: 移除了导入模式的单选框 -->
      
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
            <n-checkbox :value="table">
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
    title="确认重置演员映射表"
  >
    <n-alert title="高危操作警告" type="warning" style="margin-bottom: 15px;">
      <p style="margin: 0 0 8px 0;">此操作将 <strong>清空所有演员的 Emby Person ID</strong>。</p>
      <p style="margin: 0 0 8px 0;">它会保留宝贵的 TMDb/IMDb/Douban 映射关系，以便在全量扫描后自动重新关联。</p>
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
  SyncOutline as SyncIcon
} from '@vicons/ionicons5';
import { useConfig } from '../../composables/useConfig.js';
import axios from 'axios';

const tableInfo = {
  'app_settings': { cn: '基础配置', isSharable: false },
  'person_identity_map': { cn: '演员映射表', isSharable: true },
  'actor_metadata': { cn: '演员元数据', isSharable: true },
  'translation_cache': { cn: '翻译缓存', isSharable: true },
  'watchlist': { cn: '追剧列表', isSharable: false },
  'actor_subscriptions': { cn: '演员订阅配置', isSharable: false },
  'tracked_actor_media': { cn: '已追踪的演员作品', isSharable: false },
  'collections_info': { cn: '原生合集', isSharable: false },
  'processed_log': { cn: '已处理日志', isSharable: false },
  'failed_log': { cn: '待复核日志', isSharable: false },
  'users': { cn: '用户账户', isSharable: false },
  'custom_collections': { cn: '自建合集', isSharable: false },
  'media_metadata': { cn: '媒体元数据', isSharable: true },
  'resubscribe_rules': { cn: '媒体洗版规则', isSharable: false },
  'resubscribe_cache': { cn: '媒体洗版缓存', isSharable: false },
  'media_cleanup_tasks': { cn: '媒体去重缓存', isSharable: false },
  'emby_users': { cn: 'Emby用户', isSharable: false },
  'user_media_data': { cn: 'Emby用户数据', isSharable: false },
  'user_templates': { cn: '用户权限模板', isSharable: false },
  'invitations': { cn: '邀请链接', isSharable: false },
  'emby_users_extended': { cn: 'Emby用户扩展信息', isSharable: false },
};

// ★★★ START: 新增的依赖关系自动勾选逻辑 ★★★

// 定义表之间的依赖关系
// 键 (key) 是父表，值 (value) 是必须随之一起勾选的子表数组
// 定义表之间的依赖关系 (这个可以保留或重新添加)
const tableDependencies = {
  // 演员订阅 -> 已追踪作品 (硬依赖)
  'actor_subscriptions': ['tracked_actor_media'],
  // Emby用户 -> 用户扩展信息 & 用户媒体数据 (硬依赖)
  'emby_users': ['user_media_data', 'emby_users_extended'],
  // 权限模板 -> 邀请链接 (硬依赖)
  'user_templates': ['invitations']
};

// ★ 新增: 创建一个反向依赖映射，用于快速查找父表 (e.g., { 'media_metadata': 'person_identity_map' })
const reverseTableDependencies = {};
for (const parent in tableDependencies) {
  for (const child of tableDependencies[parent]) {
    reverseTableDependencies[child] = parent;
  }
}

/**
 * 当清空表的复选框组选项变化时触发此函数。
 * @param {string[]} currentSelection - 组件传递过来的、当前所有已勾选项的数组。
 */
const handleClearSelectionChange = (currentSelection) => {
  // 使用 Set 数据结构可以更高效地处理和避免重复项
  const selectionSet = new Set(currentSelection);

  // 遍历依赖规则
  for (const parentTable in tableDependencies) {
    // 检查父表是否在当前的勾选集合中
    if (selectionSet.has(parentTable)) {
      const children = tableDependencies[parentTable];
      // 遍历所有必须一起勾选的子表
      for (const childTable of children) {
        // 如果子表不在集合中，就添加进去
        if (!selectionSet.has(childTable)) {
          selectionSet.add(childTable);
        }
      }
    }
  }

  // 检查 Set 的大小是否和原始数组长度不同。
  // 如果不同，说明我们添加了新的子表，需要更新 v-model。
  if (selectionSet.size !== tablesToClear.value.length) {
    // 将 Set 转回数组，并更新到 ref 上
    tablesToClear.value = Array.from(selectionSet);
  }
};

/**
 * ★ 新增: 当【导入表】的复选框组选项变化时触发。
 * 逻辑：双向依赖。
 * 1. 勾选父表 -> 自动勾选子表。
 * 2. 勾选子表 -> 自动勾选父表。
 * @param {string[]} currentSelection - 当前所有已勾选项的数组。
 */
const handleImportSelectionChange = (currentSelection) => {
  const selectionSet = new Set(currentSelection);
  let changed = true;

  // 使用循环来处理嵌套依赖，直到一次循环中不再有任何变化
  while (changed) {
    changed = false;
    const originalSize = selectionSet.size;

    // 1. 正向依赖检查 (父 -> 子)
    for (const parentTable in tableDependencies) {
      if (selectionSet.has(parentTable)) {
        for (const childTable of tableDependencies[parentTable]) {
          selectionSet.add(childTable);
        }
      }
    }

    // 2. 反向依赖检查 (子 -> 父)
    for (const childTable in reverseTableDependencies) {
      if (selectionSet.has(childTable)) {
        const parentTable = reverseTableDependencies[childTable];
        selectionSet.add(parentTable);
      }
    }
    
    // 如果集合大小发生变化，说明本轮循环添加了新项目，需要再循环一次
    if (selectionSet.size > originalSize) {
      changed = true;
    }
  }
  
  // 如果最终的集合与 v-model 的值不一致，则更新 v-model
  if (selectionSet.size !== tablesToImport.value.length) {
    tablesToImport.value = Array.from(selectionSet);
  }
};

// =================================================================
// ★★★ END: 依赖关系联动勾选逻辑 ★★★
// =================================================================


const formRef = ref(null);
const formRules = {
    trigger: ['input', 'blur']
};
const { configModel, loadingConfig, savingConfig, configError, handleSaveConfig } = useConfig();
const message = useMessage();
const dialog = useDialog();
const isResettingMappings = ref(false);
const resetMappingsModalVisible = ref(false);

// --- Emby 相关的 Refs ---
const availableLibraries = ref([]);
const loadingLibraries = ref(false);
const libraryError = ref(null);
const componentIsMounted = ref(false);
// --- 反代相关
const nativeAvailableLibraries = ref([]);
const loadingNativeLibraries = ref(false);
const nativeLibraryError = ref(null);

let unwatchGlobal = null;
let unwatchEmbyConfig = null;

// --- 代理测试 ---
const isTestingProxy = ref(false);

// --- Emby 用户ID 校验逻辑 ---
const embyUserIdRegex = /^[a-f0-9]{32}$/i;
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

// 新增：显示重置映射表模态框的函数
const showResetMappingsModal = () => {
  resetMappingsModalVisible.value = true;
};

// 修改：原有的处理函数，现在需要增加关闭模态框的逻辑
const handleResetActorMappings = async () => {
  isResettingMappings.value = true;
  try {
    const response = await axios.post('/api/actions/reset_actor_mappings');
    message.success(response.data.message || '演员映射已成功重置！');
    // 成功后关闭模态框
    resetMappingsModalVisible.value = false;
  } catch (error) {
    message.error(error.response?.data?.error || '重置失败，请检查后端日志。');
    // 失败时保持模态框打开，方便用户重试
  } finally {
    isResettingMappings.value = false;
  }
};

// ★★★ 新增计算属性，用于判断 MoviePilot 是否配置完整 ★★★
const isMoviePilotConfigured = computed(() => {
  if (!configModel.value) return false;
  return !!(
    configModel.value.moviepilot_url &&
    configModel.value.moviepilot_username &&
    configModel.value.moviepilot_password
  );
});

// ★★★ 新增：测试代理连接的方法 ★★★
const testProxy = async () => {
  if (!configModel.value.network_http_proxy_url) {
    message.warning('请先填写 HTTP 代理地址再进行测试。');
    return;
  }

  isTestingProxy.value = true;
  try {
    const response = await axios.post('/api/proxy/test', {
      url: configModel.value.network_http_proxy_url
    });

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

// --- 反代 合并媒体库 ---
const fetchNativeViewsSimple = async () => {
  if (!configModel.value?.emby_server_url || !configModel.value?.emby_api_key || !configModel.value?.emby_user_id) {
    nativeAvailableLibraries.value = [];
    return;
  }
  loadingNativeLibraries.value = true;
  nativeLibraryError.value = null;
  try {
    const userId = configModel.value.emby_user_id;
    const response = await axios.get(`/api/emby/user/${userId}/views`, {
      headers: {
        'X-Emby-Token': configModel.value.emby_api_key,
      },
    });
    const items = response.data?.Items || [];
    nativeAvailableLibraries.value = items.map(i => ({
      Id: i.Id,
      Name: i.Name,
      CollectionType: i.CollectionType,
    }));
    if (nativeAvailableLibraries.value.length === 0) nativeLibraryError.value = "未找到原生媒体库。";
  } catch (err) {
    nativeAvailableLibraries.value = [];
    nativeLibraryError.value = `获取原生媒体库失败: ${err.response?.data?.error || err.message}`;
  } finally {
    loadingNativeLibraries.value = false;
  }
};

// ★★★ 新增代码：添加这个 watch 监听 ★★★
watch(
  () => configModel.value?.refresh_emby_after_update,
  (isRefreshEnabled) => {
    // 确保 configModel 已经加载
    if (configModel.value) {
      // 如果“刷新”开关被关闭了
      if (!isRefreshEnabled) {
        // 自动将“锁定”开关也关闭
        configModel.value.auto_lock_cast_after_update = false;
      }
    }
  }
);

watch(
  () => [
    configModel.value?.proxy_enabled,
    configModel.value?.proxy_merge_native_libraries,
    configModel.value?.emby_server_url,
    configModel.value?.emby_api_key,
    configModel.value?.emby_user_id,
  ],
  ([proxyEnabled, mergeNative, url, apiKey, userId]) => {
    if (proxyEnabled && mergeNative && url && apiKey && userId) {
      fetchNativeViewsSimple();
    } else {
      nativeAvailableLibraries.value = [];
    }
  },
  { immediate: true }
);

// --- AI 服务商逻辑 ---
const aiProviderOptions = ref([
  { label: 'OpenAI (及兼容服务)', value: 'openai' },
  { label: '智谱AI (ZhipuAI)', value: 'zhipuai' },
  { label: 'Google Gemini', value: 'gemini' },
]);

// --- 数据管理逻辑 ---
const isExporting = ref(false);
const exportModalVisible = ref(false);
const allDbTables = ref([]);
const tablesToExport = ref([]);
const isImporting = ref(false);
const importModalVisible = ref(false);
const fileToImport = ref(null);
const tablesInBackupFile = ref([]);
const tablesToImport = ref([]);

// 清空指定表相关状态
const clearTablesModalVisible = ref(false);
const tablesToClear = ref([]);

// 新增状态变量，表示清空操作中
const isClearing = ref(false);
const isCorrecting = ref(false);

// 显示清空指定表模态框
const showClearTablesModal = async () => {
  console.log('showClearTablesModal called');
  try {
    const response = await axios.get('/api/database/tables');
    allDbTables.value = response.data;
    tablesToClear.value = []; // 默认不选择任何表
    clearTablesModalVisible.value = true;
    console.log('Fetched tables:', allDbTables.value);
  } catch (error) {
    console.error('Error fetching database tables:', error);
    message.error('无法获取数据库表列表，请检查后端日志。');
  }
};

// 处理清空指定表操作
const handleClearTables = async () => {
  // 1. 检查用户是否选择了任何表。
  if (tablesToClear.value.length === 0) {
    // 理论上按钮是禁用的，但作为安全措施，还是加上判断。
    message.warning('请至少选择一个要清空的数据表。');
    return;
  }

  // 2. 立即开始加载状态
  isClearing.value = true;

  try {
    // 3. ★ 直接调用后端API，不再有任何中间弹窗 ★
    const response = await axios.post('/api/actions/clear_tables', {
      tables: tablesToClear.value
    });

    // 4. 操作成功
    message.success(response.data.message || '成功清空所选数据表！');
    
    // 5. 关闭模态框并清空选择
    clearTablesModalVisible.value = false;
    tablesToClear.value = [];

  } catch (error) {
    // 6. 操作失败，显示错误信息
    console.error('清空操作API调用失败:', error);
    const errorMsg = error.response?.data?.error || '清空操作失败，请检查后端日志。';
    message.error(errorMsg);
    // 失败时，模态框可以保持打开，方便用户重试

  } finally {
    // 7. 无论成功还是失败，最终都结束加载状态
    isClearing.value = false;
  }
};

const selectAllForClear = () => tablesToClear.value = [...allDbTables.value];
const deselectAllForClear = () => tablesToClear.value = [];

// ▼▼▼ 修改点2: 优化重启逻辑 ▼▼▼
const initialRestartableConfig = ref(null);

const triggerRestart = async () => {
  message.info('正在发送重启指令...');
  try {
    await axios.post('/api/system/restart');
    // 由于服务器会立即重启，这个请求很可能会因网络错误而失败。
    // 我们不依赖于它的成功响应，而是直接显示成功消息。
    message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
  } catch (error) {
    // 如果错误有响应体，说明是后端在重启前就返回了明确的错误（如权限问题）
    if (error.response) {
      message.error(error.response.data.error || '发送重启请求失败，请查看日志。');
    } else {
      // 如果没有响应体，这很可能是预期的网络中断错误，是重启成功的标志。
      message.success('重启指令已发送，应用正在后台重启。请稍后手动刷新页面。', { duration: 10000 });
    }
  }
};

const save = async () => {
  try {
    await formRef.value?.validate();

    // ▼▼▼ 核心修正：在这里创建一个干净的、临时的配置对象副本 ▼▼▼
    // 使用 JSON.parse(JSON.stringify(...)) 是一个简单有效的深拷贝方法，
    // 它可以断开所有响应式引用，得到一个纯净的 JavaScript 对象。
    const cleanConfigPayload = JSON.parse(JSON.stringify(configModel.value));
    
    // ★★★ 关键净化步骤 ★★★
    // 无论 configModel 内部发生了什么污染，我们在这里强制用当前组件的
    // v-model 的值来覆盖 payload 中对应的字段，确保其绝对正确。
    // 这一步看似多余，但它正是修复这个隐蔽BUG的关键。
    if (configModel.value) {
        cleanConfigPayload.libraries_to_process = configModel.value.libraries_to_process;
        cleanConfigPayload.proxy_native_view_selection = configModel.value.proxy_native_view_selection;
    }
    // ▲▲▲ 修正结束 ▲▲▲

    const restartNeeded =
      initialRestartableConfig.value && (
        cleanConfigPayload.proxy_port !== initialRestartableConfig.value.proxy_port ||
        cleanConfigPayload.proxy_302_redirect_url !== initialRestartableConfig.value.proxy_302_redirect_url ||
        cleanConfigPayload.log_rotation_size_mb !== initialRestartableConfig.value.log_rotation_size_mb ||
        cleanConfigPayload.log_rotation_backup_count !== initialRestartableConfig.value.log_rotation_backup_count ||
        cleanConfigPayload.emby_server_url !== initialRestartableConfig.value.emby_server_url
      );

    const performSaveAndUpdateState = async () => {
      // ▼▼▼ 核心修正：将净化后的 cleanConfigPayload 传递给保存函数 ▼▼▼
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
          if (saved) {
            await triggerRestart();
          }
        },
        onNegativeClick: async () => {
          await performSaveAndUpdateState();
        }
      });
    } else {
      await performSaveAndUpdateState();
    }
  } catch (errors) {
    console.log('表单验证失败:', errors);
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
    tablesToExport.value = [...response.data]; // 默认全选所有表
    exportModalVisible.value = true;
  } catch (error) {
    message.error('无法获取数据库表列表，请检查后端日志。');
  }
};

const handleExport = async () => {
  isExporting.value = true;
  exportModalVisible.value = false;
  try {
    const response = await axios.post('/api/database/export', {
      tables: tablesToExport.value
    }, {
      responseType: 'blob',
    });

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

const handleCustomImportRequest = ({ file }) => {
  const reader = new FileReader();
  reader.onload = (e) => {
    try {
      const content = JSON.parse(e.target.result);
      if (!content.data || typeof content.data !== 'object') {
        message.error('备份文件格式不正确：缺少 "data" 对象。');
        return;
      }
      tablesInBackupFile.value = Object.keys(content.data);
      if (tablesInBackupFile.value.length === 0) {
        message.error('备份文件格式不正确： "data" 对象为空。');
        return;
      }
      
      // 默认全选所有在备份文件中的表
      tablesToImport.value = [...tablesInBackupFile.value];
      
      fileToImport.value = file.file;
      importModalVisible.value = true;
    } catch (err) {
      message.error('无法解析JSON文件，请确保文件格式正确。');
    }
  };
  reader.readAsText(file.file);
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
  isImporting.value = true;
  message.loading('正在上传并处理文件...', { duration: 0 });

  const formData = new FormData();
  formData.append('file', fileToImport.value);
  // ★ 核心修改: 硬编码 mode 为 'overwrite' 以匹配后端逻辑
  formData.append('mode', 'overwrite');
  formData.append('tables', tablesToImport.value.join(','));

  axios.post('/api/database/import', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  })
  .then(response => {
    message.destroyAll();
    message.success(response.data?.message || '恢复任务已提交！');
  })
  .catch(error => {
    message.destroyAll();
    // ★ 核心修改: 简化错误处理，直接显示后端返回的错误信息
    const errorMsg = error.response?.data?.error || '恢复失败，未知错误。';
    message.error(errorMsg, { duration: 8000 }); // 延长显示时间，方便用户阅读
  })
  .finally(() => {
    isImporting.value = false;
  });
};
const selectAllForImport = () => tablesToImport.value = [...tablesInBackupFile.value];
const deselectAllForImport = () => tablesToImport.value = [];

// ▼▼▼ 一键矫正 ▼▼▼
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

// --- 生命周期钩子 ---
onMounted(() => {
  componentIsMounted.value = true;

  unwatchGlobal = watch(loadingConfig, (isLoading) => {
    if (!isLoading && componentIsMounted.value && configModel.value) {
      if (configModel.value.emby_server_url && configModel.value.emby_api_key) {
        fetchEmbyLibrariesInternal();
      }
      // ▼▼▼ 修改点5: 存储需要重启的配置项的初始值，包含 emby_server_url ▼▼▼
      initialRestartableConfig.value = {
        proxy_port: configModel.value.proxy_port,
        proxy_302_redirect_url: configModel.value.proxy_302_redirect_url,
        log_rotation_size_mb: configModel.value.log_rotation_size_mb,
        log_rotation_backup_count: configModel.value.log_rotation_backup_count,
        emby_server_url: configModel.value.emby_server_url,
      };

      if (unwatchGlobal) {
        unwatchGlobal();
      }
    }
  }, { immediate: true });

  unwatchEmbyConfig = watch(
    () => [configModel.value?.emby_server_url, configModel.value?.emby_api_key],
    (newValues, oldValues) => {
      if (componentIsMounted.value && oldValues) {
        if (newValues[0] !== oldValues[0] || newValues[1] !== oldValues[1]) {
          fetchEmbyLibrariesInternal();
        }
      }
    }
  );
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