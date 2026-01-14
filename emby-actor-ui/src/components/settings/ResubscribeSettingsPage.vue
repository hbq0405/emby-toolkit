<!-- src/components/settings/ResubscribeSettingsPage.vue -->
<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <n-card :bordered="false">
        <template #header>
          <span style="font-size: 1.2em; font-weight: bold;">媒体库规则管理</span>
        </template>
        <template #header-extra>
          <n-button type="primary" @click="openRuleModal()">
            <template #icon><n-icon :component="AddIcon" /></template>
            新增规则
          </n-button>
        </template>
        <p style="margin-top: 0; color: #888;">
          规则不仅用于洗版，也可以用于自动化清理低质量或低评分的媒体。如果同一个媒体项符合多个规则，则以排在上面的规则为准。
        </p>
      </n-card>

      <!-- 规则列表 -->
      <draggable v-model="rules" item-key="id" handle=".drag-handle" @end="onDragEnd" class="rules-list">
        <template #item="{ element: rule }">
          <n-card class="rule-card" :key="rule.id" size="small">
            <div class="rule-content">
              <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
              <div class="rule-details">
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span class="rule-name">{{ rule.name }}</span>
                  <!-- 类型标签 -->
                  <n-tag v-if="rule.rule_type === 'delete'" type="error" size="small" round>仅删除</n-tag>
                  <n-tag v-else type="primary" size="small" round>洗版</n-tag>
                </div>
                <n-space size="small" style="margin-top: 4px;">
                  <!-- 显示复合筛选摘要 -->
                  <n-tag :type="getLibraryTagType(rule)" size="small" :bordered="false">
                    {{ getLibraryCountText(rule) }}
                  </n-tag>
                  <n-tag v-if="rule.rule_type !== 'delete' && rule.auto_resubscribe" type="info" size="small" bordered>自动洗版</n-tag>
                  <n-tag v-if="rule.filter_rating_enabled" type="warning" size="small" bordered>评分&lt;{{ rule.filter_rating_min }}</n-tag>
                </n-space>
              </div>
              <n-space class="rule-actions">
                <n-switch v-model:value="rule.enabled" @update:value="toggleRuleStatus(rule)" size="small">
                   <template #checked>启用</template>
                   <template #unchecked>禁用</template>
                </n-switch>
                <n-button text @click="openRuleModal(rule)">
                  <template #icon><n-icon :component="EditIcon" /></template>
                </n-button>
                <n-popconfirm @positive-click="deleteRule(rule.id)">
                  <template #trigger>
                    <n-button text type="error"><template #icon><n-icon :component="DeleteIcon" /></template></n-button>
                  </template>
                  确定要删除规则 “{{ rule.name }}” 吗？
                </n-popconfirm>
              </n-space>
            </div>
          </n-card>
        </template>
      </draggable>
      <n-empty v-if="rules.length === 0" description="暂无规则" />

      <!-- 规则弹窗 -->
      <n-modal v-model:show="showModal" preset="card" style="width: 900px;" :title="modalTitle">
        <n-form ref="formRef" :model="currentRule" :rules="formRules" label-placement="top">
          
          <!-- 1. 顶部：基础信息与模式选择 -->
          <n-grid :x-gap="24" :y-gap="24" :cols="2">
            <n-gi :span="2">
              <n-card size="small" embedded :bordered="false" style="background: var(--n-action-color);">
                <n-grid :cols="2" :x-gap="24">
                  <n-gi>
                    <n-form-item path="name" label="规则名称">
                      <n-input v-model:value="currentRule.name" placeholder="例如：清理低分烂片 / 4K洗版" />
                    </n-form-item>
                  </n-gi>
                  
                  <n-gi>
                    <n-form-item label="规则模式">
                      <n-radio-group v-model:value="currentRule.rule_type" name="ruleTypeGroup" size="large">
                        <n-radio-button value="resubscribe">
                          <n-icon :component="SyncIcon" style="vertical-align: text-bottom; margin-right: 4px;" />
                          洗版模式
                        </n-radio-button>
                        <n-radio-button value="delete">
                          <n-icon :component="TrashIcon" style="vertical-align: text-bottom; margin-right: 4px;" />
                          删除模式
                        </n-radio-button>
                      </n-radio-group>
                      <template #feedback>
                        <span v-if="currentRule.rule_type === 'resubscribe'" style="color: var(--n-text-color-3);">
                          检测到不达标时，自动或手动触发 MoviePilot 订阅以获取更好版本。
                        </span>
                        <span v-else style="color: var(--n-error-color);">
                          检测到符合条件（如低分、低画质）时，直接执行删除操作，<b>不进行订阅</b>。
                        </span>
                      </template>
                    </n-form-item>
                  </n-gi>

                  <!-- ★★★ 核心修改区域：通用筛选构建器 ★★★ -->
                  <n-gi :span="2">
                    <n-card title="限定范围 (通用筛选)" size="small" style="margin-bottom: 12px; margin-top: 12px;">
                      <template #header-extra>
                        <n-tag type="info" size="small" :bordered="false">条件之间为“与”关系 (AND)</n-tag>
                      </template>
                      
                      <div class="rules-container">
                        <div v-for="(rule, index) in currentRule.scope_rules" :key="index" class="rule-row">
                          <!-- 1. 字段选择 -->
                          <n-select 
                            v-model:value="rule.field" 
                            :options="scopeFieldOptions" 
                            placeholder="选择条件" 
                            class="rule-field" 
                            @update:value="handleFieldChange(rule)"
                          />
                          
                          <!-- 2. 操作符 -->
                          <n-select 
                            v-model:value="rule.operator" 
                            :options="getOperatorOptionsForRow(rule)" 
                            placeholder="操作" 
                            :disabled="!rule.field" 
                            class="rule-op" 
                          />
                          
                          <!-- 3. 值选择 (动态组件) -->
                          <div class="rule-value">
                             <!-- 媒体库 -->
                             <n-select
                                v-if="rule.field === 'library'"
                                v-model:value="rule.value"
                                multiple filterable
                                :options="allEmbyLibraries"
                                placeholder="选择媒体库"
                             />
                             <!-- 演员 (远程搜索 + 头像) -->
                              <n-select
                                v-else-if="rule.field === 'actors'"
                                v-model:value="rule.value"
                                multiple
                                filterable
                                remote
                                placeholder="搜索演员姓名"
                                :options="actorOptions"
                                :loading="isSearchingActors"
                                @search="handlePersonSearch"
                                label-field="name"
                                :render-option="renderPersonOption"
                                :render-tag="renderPersonTag"
                                @focus="() => {
                                  if (Array.isArray(rule.value)) {
                                      // 把已选的对象合并到选项列表中，防止回显空白
                                      const existingIds = new Set(actorOptions.value.map(o => o.id));
                                      rule.value.forEach(v => {
                                          if (v && v.id && !existingIds.has(v.id)) {
                                              actorOptions.value.push(v);
                                          }
                                      });
                                  }
                                }"
                              />
                             <!-- 类型 -->
                             <n-select
                                v-else-if="rule.field === 'genres'"
                                v-model:value="rule.value"
                                multiple filterable
                                :options="genreOptions"
                                placeholder="选择类型"
                             />
                             <!-- 国家 -->
                             <n-select
                                v-else-if="rule.field === 'countries'"
                                v-model:value="rule.value"
                                multiple filterable
                                :options="countryOptions"
                                placeholder="选择国家"
                             />
                             <!-- 工作室 -->
                             <n-select
                                v-else-if="rule.field === 'studios'"
                                v-model:value="rule.value"
                                multiple filterable
                                :options="studioOptions"
                                placeholder="选择工作室"
                             />
                             <!-- 关键词 -->
                             <n-select
                                v-else-if="rule.field === 'keywords'"
                                v-model:value="rule.value"
                                multiple filterable
                                :options="keywordOptions"
                                placeholder="选择关键词"
                             />
                             <!-- 入库时间 -->
                              <n-input-number
                                v-else-if="rule.field === 'date_added'"
                                v-model:value="rule.value"
                                placeholder="天数"
                                :min="1"
                              >
                                <template #suffix>天</template>
                              </n-input-number>
                             <!-- 年份 -->
                             <n-input-number
                                v-else-if="rule.field === 'release_year'"
                                v-model:value="rule.value"
                                placeholder="输入年份"
                                :show-button="false"
                             />
                             <!-- 评分 -->
                             <n-input-number
                                v-else-if="rule.field === 'rating'"
                                v-model:value="rule.value"
                                placeholder="输入评分"
                                :step="0.1"
                             />
                             <!-- 默认文本框 -->
                             <n-input 
                                v-else 
                                v-model:value="rule.value" 
                                placeholder="输入值" 
                             />
                          </div>

                          <n-button text type="error" class="rule-delete" @click="removeScopeRule(index)">
                            <n-icon :component="DeleteIcon" size="18" />
                          </n-button>
                        </div>
                        
                        <n-button dashed block class="add-rule-btn" @click="addScopeRule">
                          <template #icon><n-icon :component="AddIcon" /></template>
                          添加限定条件
                        </n-button>
                      </div>
                    </n-card>
                  </n-gi>
                </n-grid>
              </n-card>
            </n-gi>

            <!-- 2. 左侧列：筛选条件 (Condition) -->
            <n-gi>
              <n-card title="筛选条件 (命中规则的条件)" size="small" segmented>
                <template #header-extra>
                  <n-tag type="warning" size="small" :bordered="false">满足任一条件即命中</n-tag>
                </template>
                
                <n-space vertical size="large">
                  <!-- 评分过滤 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.filter_rating_enabled">
                      <span style="font-weight: bold;">按评分筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.filter_rating_enabled" class="filter-content">
                      <n-form-item label="当评分低于此值时" :show-feedback="false">
                        <n-input-number v-model:value="currentRule.filter_rating_min" :min="0" :max="10" :step="0.1" style="width: 100%;">
                          <template #suffix>分</template>
                        </n-input-number>
                      </n-form-item>
                      
                      <!-- 动态提示文案 -->
                      <div class="tip" style="margin-top: 8px;">
                        <template v-if="currentRule.rule_type === 'delete'">
                          <n-tag type="error" size="small" :bordered="false">删除模式</n-tag>
                          命中规则：评分低于设定值时，<b>执行删除</b>。
                        </template>
                        <template v-else>
                          <n-tag type="info" size="small" :bordered="false">洗版模式</n-tag>
                          豁免规则：评分低于设定值时，<b>忽略该片</b>（即使画质不达标也不洗版）。
                        </template>
                      </div>

                      <div style="margin-top: 8px;">
                        <n-checkbox v-model:checked="currentRule.filter_rating_ignore_zero">
                          <span style="font-size: 12px;">
                            {{ currentRule.rule_type === 'delete' ? '忽略 0 分 (保护无评分的新片不被删)' : '忽略 0 分 (允许无评分的新片洗版)' }}
                          </span>
                        </n-checkbox>
                      </div>
                    </div>
                  </div>

                  <!-- 分辨率 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_resolution_enabled">
                      <span style="font-weight: bold;">按分辨率筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_resolution_enabled" class="filter-content">
                      <n-select v-model:value="currentRule.resubscribe_resolution_threshold" :options="resolutionOptions" placeholder="选择阈值" />
                      <div class="tip">当分辨率低于此值时命中</div>
                    </div>
                  </div>

                  <!-- 质量 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_quality_enabled">
                      <span style="font-weight: bold;">按质量筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_quality_enabled" class="filter-content">
                      <n-select v-model:value="currentRule.resubscribe_quality_include" multiple tag :options="qualityOptions" placeholder="选择质量" />
                      <div class="tip">当质量低于选中的最高项时命中</div>
                    </div>
                  </div>

                  <!-- 编码 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_codec_enabled">
                      <span style="font-weight: bold;">按编码筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_codec_enabled" class="filter-content">
                      <n-select v-model:value="currentRule.resubscribe_codec_include" multiple tag :options="codecOptions" placeholder="选择编码" />
                    </div>
                  </div>

                  <!-- 特效 (仅洗版模式建议开启，但删除模式也可以用) -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_effect_enabled">
                      <span style="font-weight: bold;">按特效筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_effect_enabled" class="filter-content">
                      <n-select v-model:value="currentRule.resubscribe_effect_include" multiple tag :options="effectOptions" />
                    </div>
                  </div>
                  
                  <!-- 文件大小 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_filesize_enabled">
                      <span style="font-weight: bold;">按文件大小筛选</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_filesize_enabled" class="filter-content">
                      <n-input-group>
                        <n-select v-model:value="currentRule.resubscribe_filesize_operator" :options="filesizeOperatorOptions" style="width: 30%;" />
                        <n-input-number v-model:value="currentRule.resubscribe_filesize_threshold_gb" :step="0.1" style="width: 70%;">
                          <template #suffix>GB</template>
                        </n-input-number>
                      </n-input-group>
                    </div>
                  </div>
                  <!-- 按音轨筛选 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_audio_enabled">
                      <span style="font-weight: bold;">按音轨筛选 (缺音轨)</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_audio_enabled" class="filter-content">
                      <n-form-item label="当缺少以下音轨时命中" :show-feedback="false">
                        <n-select
                          v-model:value="currentRule.resubscribe_audio_missing_languages"
                          multiple tag
                          :options="languageOptions"
                          placeholder="选择语言 (如: 国语)"
                        />
                      </n-form-item>
                    </div>
                  </div>

                  <!-- 按字幕筛选 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.resubscribe_subtitle_enabled">
                      <span style="font-weight: bold;">按字幕筛选 (缺字幕)</span>
                    </n-checkbox>
                    <div v-if="currentRule.resubscribe_subtitle_enabled" class="filter-content">
                      <n-form-item label="当缺少以下字幕时命中" :show-feedback="false">
                        <n-select
                          v-model:value="currentRule.resubscribe_subtitle_missing_languages"
                          multiple tag
                          :options="subtitleLanguageOptions"
                          placeholder="选择语言 (如: 简体中文)"
                        />
                      </n-form-item>
                      
                      <!-- 字幕豁免规则 -->
                      <div style="margin-top: 8px;">
                        <n-checkbox v-model:checked="currentRule.resubscribe_subtitle_skip_if_audio_exists">
                          <span style="font-size: 12px;">豁免：如果已存在同语言音轨则忽略缺字幕</span>
                        </n-checkbox>
                        <div class="tip" style="margin-left: 24px;">
                          例如：缺中字，但已有国语音轨，则视为达标。
                        </div>
                      </div>
                    </div>
                  </div>
                  <!-- 筛选缺集的季 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.filter_missing_episodes_enabled">
                      <span style="font-weight: bold;">筛选缺集的季 (仅剧集)</span>
                    </n-checkbox>
                    <div v-if="currentRule.filter_missing_episodes_enabled" class="filter-content">
                      <div class="tip">
                        当检测到某季存在中间断档（如只有第1、3集，缺第2集）时命中。<br>
                        <span style="color: var(--n-warning-color);" v-if="currentRule.rule_type === 'delete'">
                          适合转存大包清理残缺剧集。
                        </span>
                      </div>
                    </div>
                  </div>
                  <!-- 剧集一致性筛选 -->
                  <div class="filter-item">
                    <n-checkbox v-model:checked="currentRule.consistency_check_enabled">
                      <span style="font-weight: bold;">剧集一致性筛选 (仅剧集)</span>
                    </n-checkbox>
                    <div v-if="currentRule.consistency_check_enabled" class="filter-content">
                      <div class="tip" style="margin-bottom: 8px;">当检测到季内版本混杂时命中规则：</div>
                      <n-space vertical>
                        <n-checkbox v-model:checked="currentRule.consistency_must_match_resolution">
                          分辨率不统一 (如 4K 与 1080p 混杂)
                        </n-checkbox>
                        <n-checkbox v-model:checked="currentRule.consistency_must_match_group">
                          制作组不统一 (如不同压制组混杂)
                        </n-checkbox>
                        <n-checkbox v-model:checked="currentRule.consistency_must_match_codec">
                          编码不统一 (如 HEVC 与 AVC 混杂)
                        </n-checkbox>
                      </n-space>
                    </div>
                  </div>
                </n-space>
              </n-card>
            </n-gi>

            <!-- 3. 右侧列：执行动作 (Action) -->
            <n-gi>
              <n-card title="执行动作" size="small" segmented style="height: 100%;">
                
                <!-- 模式 A: 洗版设置 -->
                <div v-if="currentRule.rule_type === 'resubscribe'">
                  <n-form-item label="自动洗版">
                    <n-space align="center">
                      <n-switch v-model:value="currentRule.auto_resubscribe" />
                      <span class="tip">命中后自动加入订阅队列</span>
                    </n-space>
                  </n-form-item>
                  <n-form-item label="自定义洗版">
                    <n-space vertical>
                      <n-space align="center">
                        <n-switch 
                          v-model:value="currentRule.custom_resubscribe_enabled" 
                        />
                        <span class="tip">开启后，将根据规则生成订阅参数，关闭则采用MP洗版规则处理订阅。</span>
                      </n-space>
                      
                      <!-- 子选项：特效字幕 (仅在自定义洗版开启时显示) -->
                      <div v-if="currentRule.custom_resubscribe_enabled" style="margin-left: 34px; margin-top: 4px; padding: 8px; background: var(--n-color-embedded); border-radius: 4px;">
                        <n-checkbox v-model:checked="currentRule.resubscribe_subtitle_effect_only">
                          要求包含特效字幕 (正则匹配)
                        </n-checkbox>
                        <div class="tip" style="margin-top: 4px;">
                          勾选后，生成的订阅请求将强制包含“特效”关键词。
                        </div>
                      </div>
                    </n-space>
                  </n-form-item>
                  
                  <n-form-item label="洗版后删除旧文件">
                    <n-space align="center">
                      <n-switch v-model:value="currentRule.delete_after_resubscribe" :disabled="!isEmbyAdminConfigured" />
                      <span class="tip">仅手动洗版生效。</span>
                      <span class="tip" v-if="!isEmbyAdminConfigured" style="color: var(--n-warning-color);">需配置 Emby 管理员账号</span>
                    </n-space>
                  </n-form-item>
                </div>

                <!-- 模式 B: 删除设置 -->
                <div v-else>
                  <n-alert type="error" :show-icon="true" style="margin-bottom: 16px;">
                    危险操作：符合左侧条件的项目将被直接删除！
                  </n-alert>

                  <n-form-item label="删除策略">
                    <n-radio-group v-model:value="currentRule.delete_mode">
                      <n-space vertical>
                        <n-radio value="episode">
                          逐集删除 (安全模式)
                          <div class="tip">
                            推荐。找出该季下的所有单集，<b>一集一集</b>地删除。<br>
                            配合下方的延迟设置，可有效避免网盘API风控。
                          </div>
                        </n-radio>
                        <n-radio value="series">
                          整季/剧删除 (快速模式)
                          <div class="tip">
                            直接删除整季或整部剧。<br>
                            速度快，但一次性删除大量文件可能触发网盘限制。
                          </div>
                        </n-radio>
                      </n-space>
                    </n-radio-group>
                  </n-form-item>

                  <n-form-item label="删除间隔延迟 (秒)">
                    <n-input-number v-model:value="currentRule.delete_delay_seconds" :min="0" :step="1" />
                    <template #feedback>
                      <span class="tip">每删除一个文件后等待的时间。网盘用户建议设置 5-10 秒以上。</span>
                    </template>
                  </n-form-item>
                </div>

              </n-card>
            </n-gi>
          </n-grid>

        </n-form>
        <template #footer>
          <n-space justify="end">
            <n-button @click="showModal = false">取消</n-button>
            <n-button type="primary" @click="saveRule" :loading="saving">保存规则</n-button>
          </n-space>
        </template>
      </n-modal>

    </n-space>
  </n-spin>
</template>

<script setup>
import { ref, onMounted, computed, nextTick } from 'vue';
import axios from 'axios';
import { 
  useMessage, NTag, NIcon, NGrid, NGi, NRadioGroup, NRadioButton, NRadio, NInputGroup, NCheckbox, NAlert,
  NCard, NSpace, NButton, NSwitch, NPopconfirm, NModal, NForm, NFormItem, NInput, NSelect, NInputNumber, NAvatar, NText
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Add as AddIcon, Pencil as EditIcon, Trash as DeleteIcon, Move as DragHandleIcon, 
  Sync as SyncIcon, TrashBin as TrashIcon
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['saved']);
const embyAdminUser = ref('');
const embyAdminPass = ref('');

const isEmbyAdminConfigured = computed(() => embyAdminUser.value && embyAdminPass.value);
const loading = ref(true);
const saving = ref(false);
const showModal = ref(false);

const rules = ref([]);
const currentRule = ref({});
const formRef = ref(null);
const allEmbyLibraries = ref([]);

const isEditing = computed(() => currentRule.value && currentRule.value.id);
const modalTitle = computed(() => isEditing.value ? '编辑规则' : '新增规则');

const formRules = {
  name: { required: true, message: '请输入规则名称', trigger: 'blur' },
};

// 选项定义
const filesizeOperatorOptions = ref([
  { label: '小于', value: 'lt' },
  { label: '大于', value: 'gt' },
]);

const resolutionOptions = ref([
  { label: '低于 4K (3840px)', value: 3840 },
  { label: '低于 1080p (1920px)', value: 1920 },
  { label: '低于 720p (1280px)', value: 1280 },
]);

const qualityOptions = ref([
  { label: 'Remux', value: 'Remux' },
  { label: 'BluRay', value: 'BluRay' },
  { label: 'WEB-DL', value: 'WEB-DL' },
  { label: 'HDTV', value: 'HDTV' },
]);

const codecOptions = ref([
  { label: 'HEVC (H.265)', value: 'hevc' },
  { label: 'H.264 (AVC)', value: 'h264' },
]);

const effectOptions = ref([
  { label: 'DoVi Profile 8 (HDR10 兼容)', value: 'dovi_p8' },
  { label: 'DoVi Profile 7 (蓝光标准)', value: 'dovi_p7' },
  { label: 'DoVi Profile 5 (SDR 兼容)', value: 'dovi_p5' },
  { label: 'DoVi (其他)', value: 'dovi_other' },
  { label: 'HDR10+', value: 'hdr10+' },
  { label: 'HDR', value: 'hdr' },
]);

const languageOptions = ref([
    { label: '国语 (chi)', value: 'chi' }, 
    { label: '粤语 (yue)', value: 'yue' },
    { label: '英语 (eng)', value: 'eng' }, 
    { label: '日语 (jpn)', value: 'jpn' },
    { label: '韩语 (kor)', value: 'kor' }, 
]);
const subtitleLanguageOptions = ref([
    { label: '简体 (chi)', value: 'chi' }, 
    { label: '繁体 (yue)', value: 'yue' }, 
    { label: '英文 (eng)', value: 'eng' }, 
    { label: '日文 (jpn)', value: 'jpn' }, 
    { label: '韩文 (kor)', value: 'kor' }, 
]);

// 动态选项
const countryOptions = ref([]);
const genreOptions = ref([]);
const studioOptions = ref([]);
const keywordOptions = ref([]);

// 字段定义
const scopeFieldOptions = [
  { label: '媒体库', value: 'library' },
  { label: '演员', value: 'actors' },
  { label: '类型', value: 'genres' },
  { label: '国家/地区', value: 'countries' },
  { label: '年份', value: 'release_year' },
  { label: '评分', value: 'rating' },
  { label: '工作室', value: 'studios' },
  { label: '关键词', value: 'keywords' },
  { label: '入库时间', value: 'date_added' },
];

const loadData = async () => {
  loading.value = true;
  try {
    const [rulesRes, configRes, libsRes] = await Promise.all([
      axios.get('/api/resubscribe/rules'),
      axios.get('/api/config'),
      axios.get('/api/config/cover_generator/libraries')
    ]);
    rules.value = rulesRes.data;
    embyAdminUser.value = configRes.data.emby_admin_user;
    embyAdminPass.value = configRes.data.emby_admin_pass;
    allEmbyLibraries.value = libsRes.data;

    loadExtraOptions();

  } catch (error) {
    message.error('加载数据失败');
  } finally {
    loading.value = false;
  }
};

const loadExtraOptions = async () => {
  try {
    const countryRes = await axios.get('/api/custom_collections/config/tmdb_countries');
    countryOptions.value = countryRes.data;
  } catch (e) {}

  try {
    const resM = await axios.get('/api/custom_collections/config/movie_genres');
    const resT = await axios.get('/api/custom_collections/config/tv_genres');
    const genreMap = new Map();
    [...(resM.data||[]), ...(resT.data||[])].forEach(g => {
      const name = (typeof g === 'object' && g !== null) ? g.name : g;
      if (name) genreMap.set(name, name);
    });
    genreOptions.value = Array.from(genreMap.keys())
      .sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
      .map(name => ({ label: name, value: name }));
  } catch (e) {}

  try {
      const studioRes = await axios.get('/api/custom_collections/config/studios');
      studioOptions.value = studioRes.data;
      const kwRes = await axios.get('/api/custom_collections/config/keywords');
      keywordOptions.value = kwRes.data;
  } catch (e) {}
};

const openRuleModal = async (rule = null) => {
  if (rule) {
    currentRule.value = JSON.parse(JSON.stringify(rule));
    if (!currentRule.value.scope_rules) currentRule.value.scope_rules = [];
    if (currentRule.value.scope_rules) {
        currentRule.value.scope_rules.forEach(r => {
            if (r.field === 'actors' && Array.isArray(r.value)) {
                // 把保存的对象塞回 actorOptions，这样 Select 就能显示名字和头像了
                r.value.forEach(actor => {
                    if (actor && actor.id) {
                        // 简单去重
                        if (!actorOptions.value.find(o => o.id === actor.id)) {
                            actorOptions.value.push(actor);
                        }
                    }
                });
            }
        });
    }
  } else {
    currentRule.value = {
      name: '', enabled: true, rule_type: 'resubscribe',
      scope_rules: [],
      filter_rating_enabled: false, filter_rating_min: 0, filter_rating_ignore_zero: false,
      resubscribe_resolution_enabled: false, resubscribe_resolution_threshold: 1920,
      resubscribe_quality_enabled: false, resubscribe_quality_include: [],
      resubscribe_codec_enabled: false, resubscribe_codec_include: [],
      resubscribe_effect_enabled: false, resubscribe_effect_include: [],
      resubscribe_filesize_enabled: false, resubscribe_filesize_operator: 'lt', resubscribe_filesize_threshold_gb: null,
      filter_missing_episodes_enabled: false,
      auto_resubscribe: false, custom_resubscribe_enabled: false, delete_after_resubscribe: false,
      resubscribe_subtitle_effect_only: false,
      consistency_check_enabled: false, consistency_must_match_resolution: false, consistency_must_match_group: false,
      delete_mode: 'episode', delete_delay_seconds: 5
    };
    addScopeRule();
  }
  showModal.value = true;
};

const saveRule = async () => {
  formRef.value?.validate(async (errors) => {
    if (!errors) {
      saving.value = true;
      try {
        const api = isEditing.value ? axios.put : axios.post;
        const url = isEditing.value ? `/api/resubscribe/rules/${currentRule.value.id}` : '/api/resubscribe/rules';
        await api(url, currentRule.value);
        message.success('规则保存成功');
        showModal.value = false;
        loadData();
        emit('saved', { needsRefresh: false });
      } catch (error) {
        message.error('保存失败');
      } finally {
        saving.value = false;
      }
    }
  });
};

const deleteRule = async (id) => {
  try {
    await axios.delete(`/api/resubscribe/rules/${id}`);
    message.success('规则已删除');
    loadData();
  } catch (e) { message.error('删除失败'); }
};

const toggleRuleStatus = async (rule) => {
  try {
    await axios.put(`/api/resubscribe/rules/${rule.id}`, { enabled: rule.enabled });
    message.success('状态已更新');
  } catch (e) { rule.enabled = !rule.enabled; }
};

const onDragEnd = async () => {
  try {
    await axios.post('/api/resubscribe/rules/order', rules.value.map(r => r.id));
    message.success('顺序已更新');
  } catch (e) {}
};

const addScopeRule = () => {
  currentRule.value.scope_rules.push({ field: 'library', operator: 'is_one_of', value: [] });
};

const removeScopeRule = (index) => {
  currentRule.value.scope_rules.splice(index, 1);
};

const handleFieldChange = (rule) => {
    rule.value = null;
    const ops = getOperatorOptionsForRow(rule);
    if (ops.length > 0) rule.operator = ops[0].value;
};

const getOperatorOptionsForRow = (rule) => {
  const listOps = [
      { label: '包含任意', value: 'is_one_of' },
      { label: '不包含', value: 'is_none_of' }
  ];
  const numOps = [
      { label: '大于等于', value: 'gte' },
      { label: '小于等于', value: 'lte' },
      { label: '等于', value: 'eq' }
  ];
  // 入库时间操作符
  if (['date_added'].includes(rule.field)) {
      return [
          { label: '最近N天内', value: 'in_last_days' },
          { label: 'N天以前', value: 'not_in_last_days' }
      ];
  }
  
  // 演员操作符
  if (['actors'].includes(rule.field)) {
      return [
          { label: '包含任意', value: 'is_one_of' },
          { label: '不包含', value: 'is_none_of' },
          { label: '主要是(前3)', value: 'is_primary' }
      ];
  }

  if (['library', 'genres', 'countries', 'studios', 'keywords'].includes(rule.field)) {
      return listOps;
  }
  if (['release_year', 'rating'].includes(rule.field)) {
      return numOps;
  }
  return [{ label: '等于', value: 'eq' }];
};

// 3. 演员搜索逻辑 (复用自建合集)
const actorOptions = ref([]);
const isSearchingActors = ref(false);
let personSearchTimeout = null;

const handlePersonSearch = (query) => {
  if (!query) {
    actorOptions.value = [];
    return;
  }
  isSearchingActors.value = true;
  if (personSearchTimeout) clearTimeout(personSearchTimeout);
  
  personSearchTimeout = setTimeout(async () => {
    try {
      // 调用后端 TMDb 搜索接口
      const response = await axios.get(`/api/custom_collections/config/tmdb_search_persons?q=${query}`);
      actorOptions.value = response.data || [];
    } catch (error) {
      console.error('搜索人物失败:', error);
      actorOptions.value = [];
    } finally {
      isSearchingActors.value = false;
    }
  }, 300);
};

// 辅助函数：生成 TMDb 图片 URL
const getTmdbImageUrl = (path) => {
  return path ? `https://image.tmdb.org/t/p/w92${path}` : null;
};

// 自定义渲染函数：下拉选项 (带头像)
const renderPersonOption = ({ node, option }) => {
  return h('div', { style: 'display: flex; align-items: center;' }, [
    h(NAvatar, {
      src: getTmdbImageUrl(option.profile_path),
      size: 'small',
      round: true,
      style: 'margin-right: 8px; flex-shrink: 0;'
    }),
    h('div', [
      h('div', option.name),
      h(NText, { depth: 3, style: 'font-size: 12px;' }, { default: () => option.known_for || '暂无代表作' })
    ])
  ]);
};

// 自定义渲染函数：选中标签 (带头像)
const renderPersonTag = ({ option, handleClose }) => {
  return h(
    NTag,
    {
      type: 'info',
      closable: true,
      onClose: (e) => { e.stopPropagation(); handleClose(); },
      round: true,
      style: 'padding: 0 6px 0 2px; display: flex; align-items: center;'
    },
    {
      default: () => [
        h(NAvatar, {
          src: getTmdbImageUrl(option.profile_path),
          size: 18,
          round: true,
          style: 'margin-right: 4px;'
        }),
        option.name
      ]
    }
  );
};

const getLibraryCountText = (rule) => {
    if (!rule.scope_rules || rule.scope_rules.length === 0) return '无限制';
    
    const parts = [];
    rule.scope_rules.forEach(r => {
        const fieldMap = {
            'library': '库', 'genres': '类', 'countries': '国',
            'release_year': '年', 'rating': '分', 'studios': '厂', 'keywords': '词'
        };
        const suffix = fieldMap[r.field] || r.field;
        
        if (Array.isArray(r.value)) {
            parts.push(`${r.value.length}${suffix}`);
        } else {
            parts.push(`${suffix}:${r.value}`);
        }
    });
    return parts.join(' + ');
};

const getLibraryTagType = (rule) => {
  return (rule.scope_rules && rule.scope_rules.length > 0) ? 'default' : 'warning';
};

onMounted(loadData);
</script>

<style scoped>
.rules-list { display: flex; flex-direction: column; gap: 12px; }
.rule-card { cursor: move; }
.rule-content { display: flex; align-items: center; gap: 16px; }
.drag-handle { cursor: grab; color: #888; }
.rule-details { flex-grow: 1; }
.rule-name { font-weight: bold; font-size: 1.05em; }
.rule-actions { margin-left: auto; }

/* Filter Styles */
.filter-item { margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px dashed #eee; }
.filter-item:last-child { border-bottom: none; margin-bottom: 0; padding-bottom: 0; }
.filter-content { margin-top: 8px; margin-left: 24px; }
.tip { font-size: 12px; color: #999; margin-top: 4px; }

/* Scope Rules */
.rules-container { display: flex; flex-direction: column; gap: 8px; }
.rule-row { display: flex; gap: 8px; align-items: center; }
.rule-field { width: 120px !important; flex-shrink: 0; }
.rule-op { width: 110px !important; flex-shrink: 0; }
.rule-value { flex: 1; min-width: 0; }
.rule-value > .n-select, .rule-value > .n-input-number, .rule-value > .n-input { width: 100% !important; }
</style>