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
                  <n-gi :span="2">
                    <n-grid :cols="2" :x-gap="24">
                      <n-gi>
                        <n-form-item label="应用范围" path="scope_type">
                          <n-select 
                            v-model:value="currentRule.scope_type" 
                            :options="scopeTypeOptions" 
                            @update:value="handleScopeTypeChange"
                            placeholder="选择筛选维度"
                          />
                        </n-form-item>
                      </n-gi>
                      
                      <n-gi>
                        <n-form-item label="选择范围内容" path="scope_value">
                          <!-- 1. 媒体库选择 -->
                          <n-select 
                            v-if="currentRule.scope_type === 'library'"
                            v-model:value="currentRule.scope_value" 
                            :options="allEmbyLibraries" 
                            multiple
                            placeholder="请选择媒体库"
                          />
                          
                          <!-- 2. 国家/地区选择 -->
                          <n-select 
                            v-else-if="currentRule.scope_type === 'country'"
                            v-model:value="currentRule.scope_value" 
                            :options="countryOptions" 
                            multiple 
                            filterable
                            placeholder="请选择国家/地区 (如: 中国大陆, 日本)"
                          />
                          
                          <!-- 3. 类型选择 -->
                          <n-select 
                            v-else-if="currentRule.scope_type === 'genre'"
                            v-model:value="currentRule.scope_value" 
                            :options="genreOptions" 
                            multiple 
                            filterable
                            placeholder="请选择类型 (如: 动画, 动作)"
                          />
                          
                          <!-- 兜底 -->
                          <n-input v-else v-model:value="currentRule.scope_value" placeholder="请输入值" />
                        </n-form-item>
                      </n-gi>
                    </n-grid>
                  </n-gi>
                  <n-gi :span="2">
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
                    <n-space align="center">
                      <n-switch v-model:value="currentRule.custom_resubscribe_enabled" />
                      <span class="tip">开启后，将根据规则生成订阅参数。</span>
                    </n-space>
                  </n-form-item>
                  
                  <n-form-item label="洗版后删除旧文件">
                    <n-space align="center">
                      <n-switch v-model:value="currentRule.delete_after_resubscribe" :disabled="!isEmbyAdminConfigured" />
                      <span class="tip">仅手动洗版生效。</span>
                      <span class="tip" v-if="!isEmbyAdminConfigured" style="color: var(--n-warning-color);">需配置 Emby 管理员账号</span>
                    </n-space>
                  </n-form-item>

                  <n-form-item label="特效字幕">
                    <n-checkbox v-model:checked="currentRule.resubscribe_subtitle_effect_only">订阅时要求包含特效字幕</n-checkbox>
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
  useMessage, NTag, NIcon, NGrid, NGi, NRadioGroup, NRadioButton, NRadio, NInputGroup, NCheckbox, NAlert
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Add as AddIcon, Pencil as EditIcon, Trash as DeleteIcon, Move as DragHandleIcon, 
  Sync as SyncIcon, TrashBin as TrashIcon
} from '@vicons/ionicons5';

// ... (引入其他必要的组件)

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
  target_library_ids: { type: 'array', required: true, message: '请至少选择一个媒体库', trigger: 'change' },
};

// 选项定义
const scopeTypeOptions = [
  { label: '媒体库', value: 'library' },
  { label: '国家/地区', value: 'country' },
  { label: '电影/剧集类型', value: 'genre' },
];

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

const countryOptions = ref([]);
const genreOptions = ref([]);

// 修改 loadData 函数，增加加载国家和类型的逻辑
const loadData = async () => {
  loading.value = true;
  try {
    const [rulesRes, configRes, libsRes] = await Promise.all([
      axios.get('/api/resubscribe/rules'),
      axios.get('/api/config'),
      axios.get('/api/config/cover_generator/libraries') // 提前加载媒体库
    ]);
    rules.value = rulesRes.data;
    embyAdminUser.value = configRes.data.emby_admin_user;
    embyAdminPass.value = configRes.data.emby_admin_pass;
    allEmbyLibraries.value = libsRes.data;

    // ★★★ 新增：加载国家和类型数据 ★★★
    loadExtraOptions();

  } catch (error) {
    message.error('加载数据失败');
  } finally {
    loading.value = false;
  }
};

// 新增辅助函数：加载额外选项
const loadExtraOptions = async () => {
  try {
    // 1. 加载国家
    const countryRes = await axios.get('/api/custom_collections/config/tmdb_countries');
    countryOptions.value = countryRes.data;

    // 2. 加载类型 (合并电影和电视)
    let movieGenres = [];
    let tvGenres = [];

    try {
      const res = await axios.get('/api/custom_collections/config/movie_genres');
      movieGenres = res.data || [];
    } catch (e) {
      console.warn("加载电影类型失败", e);
    }

    try {
      const res = await axios.get('/api/custom_collections/config/tv_genres');
      tvGenres = res.data || [];
    } catch (e) {
      console.warn("加载电视剧类型失败", e);
    }

    // 合并去重
    const genreMap = new Map();
    [...movieGenres, ...tvGenres].forEach(g => {
      // ★★★ 核心修复：兼容字符串和对象两种格式 ★★★
      // 如果 g 是对象(来自TMDb)，取 g.name；如果 g 是字符串(来自数据库)，直接用 g
      const name = (typeof g === 'object' && g !== null) ? g.name : g;
      
      if (name) {
        genreMap.set(name, name);
      }
    });
    
    genreOptions.value = Array.from(genreMap.keys())
      .sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
      .map(name => ({ label: name, value: name }));
      
  } catch (e) {
    console.error("加载额外选项主流程失败", e);
  }
};

const openRuleModal = async (rule = null) => {
  if (rule) {
    currentRule.value = JSON.parse(JSON.stringify(rule));
    
    // ★★★ 兼容旧数据 ★★★
    if (!currentRule.value.scope_type) {
      currentRule.value.scope_type = 'library';
      // 如果旧数据存在 target_library_ids 且 scope_value 为空，则迁移过来显示
      if (!currentRule.value.scope_value && currentRule.value.target_library_ids) {
        currentRule.value.scope_value = currentRule.value.target_library_ids;
      }
    }
    // 兼容旧数据
    if (!currentRule.value.rule_type) currentRule.value.rule_type = 'resubscribe';
  } else {
    currentRule.value = {
      name: '', enabled: true, 
      scope_type: 'library', scope_value: [], // 新增默认值
      target_library_ids: [], // 保留旧字段以防万一
      rule_type: 'resubscribe', // 默认为洗版
      
      // 筛选条件
      filter_rating_enabled: false, filter_rating_min: 0, filter_rating_ignore_zero: false,
      resubscribe_resolution_enabled: false, resubscribe_resolution_threshold: 1920,
      resubscribe_quality_enabled: false, resubscribe_quality_include: [],
      resubscribe_codec_enabled: false, resubscribe_codec_include: [],
      resubscribe_effect_enabled: false, resubscribe_effect_include: [],
      resubscribe_filesize_enabled: false, resubscribe_filesize_operator: 'lt', resubscribe_filesize_threshold_gb: null,
      filter_missing_episodes_enabled: false,
      
      // 洗版动作
      auto_resubscribe: false, custom_resubscribe_enabled: false, delete_after_resubscribe: false,
      resubscribe_subtitle_effect_only: false,
      consistency_check_enabled: false, consistency_must_match_resolution: false, consistency_must_match_group: false,

      // 删除动作
      delete_mode: 'episode', delete_delay_seconds: 5
    };
  }
  showModal.value = true;
};

const handleScopeTypeChange = () => {
  currentRule.value.scope_value = []; 
};

const availableLibraryOptions = computed(() => {
  if (!rules.value || !allEmbyLibraries.value) return [];
  const assignedIds = new Set(rules.value.filter(r => r.id !== currentRule.value.id).flatMap(r => r.target_library_ids || []));
  return allEmbyLibraries.value.filter(lib => !assignedIds.has(lib.value));
});

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

const getLibraryCountText = (rule) => {
  const type = rule.scope_type || 'library';
  const val = rule.scope_value || rule.target_library_ids;
  
  if (!val || val.length === 0) return '未指定';
  
  if (type === 'library') return `${val.length} 个库`;
  if (type === 'country') return `国家: ${val.length} 个`;
  if (type === 'genre') return `类型: ${val.length} 个`;
  return `${val.length} 项`;
};

// 修改 getLibraryTagType
const getLibraryTagType = (rule) => {
  const val = rule.scope_value || rule.target_library_ids;
  return (!val || val.length === 0) ? 'error' : 'default';
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
</style>