<template>
  <n-modal
    v-model:show="show"
    preset="card"
    title="配置季集号识别正则"
    :style="modalStyle"
    class="episode-regex-modal"
  >
    <n-space vertical :size="14">
      <n-alert type="warning" :show-icon="true">
        <b>用途：</b>用于处理动漫、番剧、字幕组资源等各种非标准命名。<br />
        <b>优先级：</b>按列表顺序从上到下匹配，<b>命中即返回</b>，不再继续走后续硬编码识别。<br />
        <b>建议：</b>精确规则放前面，泛匹配规则放后面，避免误识别。
      </n-alert>

      <n-alert type="info" :show-icon="true">
        <b>推荐两种模式：</b><br />
        1. <code>季 + 集</code>：正则中分别捕获季号和集号。<br />
        2. <code>仅集号</code>：只需要保留一个“集号捕获组”，切换后会自动固定为第 1 组，季号使用默认季；特别篇 / Specials 可填 <b>0</b>。
      </n-alert>

      <n-space justify="space-between" align="center" class="rule-toolbar">
        <n-space align="center" :size="8">
          <n-text depth="3">共 {{ rules.length }} 条规则</n-text>
          <n-text depth="3" class="toolbar-tip">已保存规则默认折叠，点击编辑展开。</n-text>
        </n-space>

        <n-space align="center" :size="8">
          <n-button size="small" secondary :disabled="rules.length === 0" @click="exportRulesToClipboard">
            导出到剪贴板
          </n-button>
          <n-button size="small" secondary @click="importRulesFromClipboard">
            从剪贴板导入
          </n-button>
          <n-button size="small" secondary @click="openImportDialog">
            粘贴导入
          </n-button>
          <n-button size="small" type="primary" ghost @click="addRule">
            <template #icon><n-icon :component="AddIcon" /></template>
            新增规则
          </n-button>
        </n-space>
      </n-space>

      <n-empty
        v-if="rules.length === 0"
        description="暂无规则，点击右上角新增，或者从剪贴板导入"
        style="padding: 24px 0;"
      />

      <n-card
        v-for="(rule, index) in rules"
        :key="rule.id || index"
        size="small"
        :bordered="true"
        class="rule-card"
        :class="{ 'rule-card--compact': !rule._editing }"
        :content-style="ruleCardContentStyle"
      >
        <template #header>
          <div class="rule-card-header">
            <n-space align="center" :size="8" class="rule-title-wrap">
              <n-tag size="small" type="primary" :bordered="false">规则 {{ index + 1 }}</n-tag>
              <n-tag size="small" :type="rule.enabled ? 'success' : 'default'" :bordered="false">
                {{ rule.enabled ? '启用' : '停用' }}
              </n-tag>
              <span class="rule-title">
                {{ rule.name?.trim() || '未命名规则' }}
              </span>
              <span v-if="!rule._editing" class="rule-brief">
                {{ buildRuleBrief(rule) }}
              </span>
            </n-space>

            <n-space align="center" :size="8" class="rule-actions">
              <n-switch v-model:value="rule.enabled" size="small">
                <template #checked>启用</template>
                <template #unchecked>停用</template>
              </n-switch>
              <n-button size="small" secondary @click="toggleRuleEditing(rule)">
                {{ rule._editing ? '收起' : '编辑' }}
              </n-button>
              <n-button size="small" tertiary type="error" @click="removeRule(index)">
                <template #icon><n-icon :component="TrashIcon" /></template>
                删除
              </n-button>
            </n-space>
          </div>
        </template>

        <template v-if="!rule._editing">
          <div class="compact-rule-body" @click="toggleRuleEditing(rule)">
            <n-tag size="small" :bordered="false">
              {{ rule.mode === 'episode_only' ? '仅集号' : '季 + 集' }}
            </n-tag>
            <n-tag v-if="rule.mode === 'episode_only'" size="small" :bordered="false" type="info">
              默认 S{{ String(toNonNegativeInt(rule.default_season, 1)).padStart(2, '0') }}
            </n-tag>
            <n-tag v-else size="small" :bordered="false" type="info">
              季组 {{ toPositiveInt(rule.season_group, 1) }} / 集组 {{ toPositiveInt(rule.episode_group, 2) }}
            </n-tag>
            <code class="compact-pattern">{{ rule.pattern || '未填写正则' }}</code>
          </div>
        </template>

        <template v-else>
          <n-grid cols="1 s:2" :x-gap="16" :y-gap="8" responsive="screen">
            <n-gi>
              <n-form-item label="规则名称">
                <n-input v-model:value="rule.name" placeholder="例如：AI-Raws 单季番剧" />
              </n-form-item>
            </n-gi>

            <n-gi>
              <n-form-item label="匹配模式">
                <n-radio-group
                  v-model:value="rule.mode"
                  @update:value="() => handleRuleModeChange(rule)"
                >
                  <n-space>
                    <n-radio value="season_episode">季 + 集</n-radio>
                    <n-radio value="episode_only">仅集号</n-radio>
                  </n-space>
                </n-radio-group>
              </n-form-item>
            </n-gi>

            <n-gi span="1 s:2">
              <n-form-item label="识别正则">
                <n-input
                  v-model:value="rule.pattern"
                  type="textarea"
                  :autosize="{ minRows: 2, maxRows: 4 }"
                  placeholder="例如：S(\d{1,2})E(\d{1,3}) 或 #(\d{1,3})"
                />
              </n-form-item>
            </n-gi>

            <n-gi v-if="rule.mode === 'season_episode'">
              <n-form-item label="季号捕获组">
                <n-input-number
                  v-model:value="rule.season_group"
                  :min="1"
                  :step="1"
                  style="width: 100%;"
                />
              </n-form-item>
            </n-gi>

            <n-gi v-if="rule.mode === 'season_episode'">
              <n-form-item label="集号捕获组">
                <n-input-number
                  v-model:value="rule.episode_group"
                  :min="1"
                  :step="1"
                  style="width: 100%;"
                />
              </n-form-item>
            </n-gi>

            <template v-else>
              <n-gi>
                <n-form-item label="集号捕获组">
                  <n-input-number
                    :value="1"
                    :min="1"
                    :step="1"
                    disabled
                    style="width: 100%;"
                  />
                </n-form-item>
              </n-gi>

              <n-gi>
                <n-form-item label="默认季号">
                  <n-input-number
                    v-model:value="rule.default_season"
                    :min="0"
                    :step="1"
                    style="width: 100%;"
                  />
                </n-form-item>
              </n-gi>

              <n-gi span="1 s:2">
                <n-alert type="default" :show-icon="false">
                  当前为“仅集号”模式：前端会自动将 <b>集号捕获组固定为 1</b>；默认季号填 <b>0</b> 就是特别篇 / Specials。
                </n-alert>
              </n-gi>
            </template>

            <n-gi span="1 s:2">
              <n-text depth="3" class="desc-text">
                说明：正则里只需要把数字部分放进捕获组即可。比如
                <code>S(\d+)E(\d+)</code>、
                <code>第(\d+)季.*?第(\d+)话</code>、
                <code>#(\d{1,3})</code>。
              </n-text>
            </n-gi>
          </n-grid>
        </template>
      </n-card>

      <n-divider title-placement="left" class="test-divider">
        实时测试
      </n-divider>

      <n-form label-placement="left" label-width="100">
        <n-form-item label="测试文件名">
          <n-input
            v-model:value="testFilename"
            placeholder="例如：[AI-Raws] ベルセルク 黄金時代篇 MEMORIAL EDITION #04 (BD HEVC 2560x1080 FLAC).mkv"
          />
        </n-form-item>
        <n-form-item label="识别结果">
          <n-alert :type="preview.type" :show-icon="true" class="preview-alert">
            {{ preview.text }}
          </n-alert>
        </n-form-item>
      </n-form>
    </n-space>

    <template #footer>
      <n-space justify="end">
        <n-button @click="show = false">取消</n-button>
        <n-button type="primary" :loading="saving" @click="saveRules">保存配置</n-button>
      </n-space>
    </template>
  </n-modal>

  <n-modal
    v-model:show="showImportDialog"
    preset="card"
    title="导入季集号识别规则"
    :style="importModalStyle"
  >
    <n-space vertical :size="12">
      <n-alert type="info" :show-icon="true">
        支持粘贴“导出到剪贴板”生成的完整 JSON，也支持只粘贴规则数组。小白复制别人发来的内容，原样贴进来即可。
      </n-alert>

      <n-radio-group v-model:value="importMode">
        <n-space>
          <n-radio value="append">追加到现有规则后面</n-radio>
          <n-radio value="replace">覆盖当前规则</n-radio>
        </n-space>
      </n-radio-group>

      <n-input
        v-model:value="importText"
        type="textarea"
        :autosize="{ minRows: 10, maxRows: 18 }"
        placeholder='粘贴规则 JSON，例如：{"type":"ETK_P115_EPISODE_REGEX_RULES","rules":[...]}'
      />
    </n-space>

    <template #footer>
      <n-space justify="end">
        <n-button @click="showImportDialog = false">取消</n-button>
        <n-button type="primary" @click="confirmImportRules">导入</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { computed, ref } from 'vue';
import axios from 'axios';
import {
  NAlert,
  NButton,
  NCard,
  NDivider,
  NEmpty,
  NForm,
  NFormItem,
  NGrid,
  NGi,
  NIcon,
  NInput,
  NInputNumber,
  NModal,
  NRadio,
  NRadioGroup,
  NSpace,
  NSwitch,
  NTag,
  NText,
  useMessage,
  useThemeVars
} from 'naive-ui';
import {
  AddOutline as AddIcon,
  TrashOutline as TrashIcon
} from '@vicons/ionicons5';

const API_URL = '/api/p115/episode_regex_rules';
const EXPORT_TYPE = 'ETK_P115_EPISODE_REGEX_RULES';

const show = ref(false);
const saving = ref(false);
const rules = ref([]);
const testFilename = ref('');
const showImportDialog = ref(false);
const importText = ref('');
const importMode = ref('append');
const message = useMessage();
const themeVars = useThemeVars();

const modalStyle = computed(() => ({
  width: '960px',
  maxWidth: '96vw'
}));

const importModalStyle = computed(() => ({
  width: '760px',
  maxWidth: '94vw'
}));

const ruleCardContentStyle = computed(() => ({
  backgroundColor: themeVars.value.actionColor,
  borderRadius: themeVars.value.borderRadius,
  transition: 'background-color .2s ease'
}));

const createEmptyRule = () => ({
  enabled: true,
  name: '',
  pattern: '',
  mode: 'season_episode',
  season_group: 1,
  episode_group: 2,
  default_season: 1,
  _editing: true
});

const toPositiveInt = (value, fallback) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? Math.trunc(num) : fallback;
};

const toNonNegativeInt = (value, fallback) => {
  const num = Number(value);
  return Number.isFinite(num) && num >= 0 ? Math.trunc(num) : fallback;
};

const parseCapturedNumber = (value) => {
  const normalized = String(value ?? '').trim().replace(/^0+(?=\d)/, '');
  if (!normalized) return null;
  const num = Number(normalized);
  return Number.isFinite(num) && num >= 0 ? num : null;
};

const normalizeRule = (rule = {}, options = {}) => {
  const normalized = {
    id: rule.id ? String(rule.id) : undefined,
    enabled: rule.enabled !== false,
    name: String(rule.name || '').trim(),
    pattern: String(rule.pattern || '').trim(),
    mode: rule.mode === 'episode_only' ? 'episode_only' : 'season_episode',
    season_group: toPositiveInt(rule.season_group, 1),
    episode_group: toPositiveInt(rule.episode_group, 2),
    default_season: toNonNegativeInt(rule.default_season, 1),
    _editing: Boolean(options.editing ?? rule._editing)
  };

  if (normalized.mode === 'episode_only') {
    normalized.episode_group = 1;
    normalized.season_group = 1;
  }

  return normalized;
};

const stripUiFields = (rule) => {
  const normalized = normalizeRule(rule);
  const clean = {
    enabled: normalized.enabled,
    name: normalized.name,
    pattern: normalized.pattern,
    mode: normalized.mode,
    season_group: normalized.season_group,
    episode_group: normalized.episode_group,
    default_season: normalized.default_season
  };

  if (normalized.id) {
    clean.id = normalized.id;
  }

  return clean;
};

const handleRuleModeChange = (rule) => {
  if (!rule) return;

  if (rule.mode === 'episode_only') {
    rule.episode_group = 1;
    rule.season_group = 1;
    rule.default_season = toNonNegativeInt(rule.default_season, 1);
  } else {
    rule.season_group = toPositiveInt(rule.season_group, 1);
    rule.episode_group = toPositiveInt(rule.episode_group, 2);
  }
};

const buildRuleBrief = (rule) => {
  const normalized = normalizeRule(rule);
  if (normalized.mode === 'episode_only') {
    return `仅集号 / 默认 S${String(normalized.default_season).padStart(2, '0')}`;
  }
  return `季组 ${normalized.season_group} / 集组 ${normalized.episode_group}`;
};

const toggleRuleEditing = (rule) => {
  if (!rule) return;
  rule._editing = !rule._editing;
};

const preview = computed(() => {
  const filename = testFilename.value.trim();
  if (!filename) {
    return { type: 'default', text: '请输入测试文件名' };
  }

  for (let i = 0; i < rules.value.length; i += 1) {
    const rule = normalizeRule(rules.value[i]);
    if (!rule.enabled || !rule.pattern) continue;

    try {
      const regex = new RegExp(rule.pattern, 'i');
      const match = filename.match(regex);
      if (!match) continue;

      if (rule.mode === 'episode_only') {
        const rawEpisode = match[1];
        const episode = parseCapturedNumber(rawEpisode);
        if (episode == null) {
          return {
            type: 'warning',
            text: `规则 ${i + 1} 已命中，但“仅集号”模式固定读取第 1 捕获组，第 1 组不是有效数字：${rawEpisode ?? '空'}`
          };
        }

        const season = toNonNegativeInt(rule.default_season, 1);
        return {
          type: 'success',
          text: `命中规则 ${i + 1}${rule.name ? `（${rule.name}）` : ''}：识别为 S${String(season).padStart(2, '0')}E${String(episode).padStart(2, '0')}`
        };
      }

      const seasonGroup = toPositiveInt(rule.season_group, 1);
      const episodeGroup = toPositiveInt(rule.episode_group, 2);

      const rawSeason = match[seasonGroup];
      const season = parseCapturedNumber(rawSeason);
      if (season == null) {
        return {
          type: 'warning',
          text: `规则 ${i + 1} 已命中，但第 ${seasonGroup} 捕获组不是有效数字：${rawSeason ?? '空'}`
        };
      }

      const rawEpisode = match[episodeGroup];
      const episode = parseCapturedNumber(rawEpisode);
      if (episode == null) {
        return {
          type: 'warning',
          text: `规则 ${i + 1} 已命中，但第 ${episodeGroup} 捕获组不是有效数字：${rawEpisode ?? '空'}`
        };
      }

      return {
        type: 'success',
        text: `命中规则 ${i + 1}${rule.name ? `（${rule.name}）` : ''}：识别为 S${String(season).padStart(2, '0')}E${String(episode).padStart(2, '0')}`
      };
    } catch (error) {
      return {
        type: 'error',
        text: `规则 ${i + 1} 正则语法错误：${error.message}`
      };
    }
  }

  return {
    type: 'warning',
    text: '未命中任何启用中的规则。'
  };
});

const addRule = () => {
  rules.value.push(createEmptyRule());
};

const removeRule = (index) => {
  rules.value.splice(index, 1);
};

const normalizeRulesForSave = () => {
  return rules.value.map((rule) => stripUiFields(rule));
};

const buildExportText = () => {
  return JSON.stringify({
    type: EXPORT_TYPE,
    version: 1,
    exported_at: new Date().toISOString(),
    rules: normalizeRulesForSave()
  }, null, 2);
};

const writeTextToClipboard = async (text) => {
  if (!navigator.clipboard?.writeText) {
    throw new Error('当前浏览器不支持自动写入剪贴板');
  }
  await navigator.clipboard.writeText(text);
};

const readTextFromClipboard = async () => {
  if (!navigator.clipboard?.readText) {
    throw new Error('当前浏览器不支持自动读取剪贴板');
  }
  return navigator.clipboard.readText();
};

const exportRulesToClipboard = async () => {
  try {
    await writeTextToClipboard(buildExportText());
    message.success('规则已导出到剪贴板，直接发给别人复制导入即可');
  } catch (error) {
    importText.value = buildExportText();
    showImportDialog.value = true;
    message.warning('自动写入剪贴板失败，已把导出内容放到弹窗里，请手动复制');
  }
};

const parseImportedRules = (text) => {
  const rawText = String(text || '').trim();
  if (!rawText) {
    throw new Error('导入内容为空');
  }

  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch (error) {
    throw new Error(`JSON 格式错误：${error.message}`);
  }

  const rawRules = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.rules)
      ? parsed.rules
      : null;

  if (!rawRules) {
    throw new Error('没有找到 rules 数组');
  }

  const importedRules = rawRules
    .filter((item) => item && typeof item === 'object')
    .map((item) => normalizeRule(item, { editing: false }))
    .filter((item) => item.pattern);

  if (importedRules.length === 0) {
    throw new Error('没有可导入的有效规则');
  }

  return importedRules;
};

const applyImportedRules = (importedRules) => {
  if (importMode.value === 'replace') {
    rules.value = importedRules;
  } else {
    rules.value = [...rules.value, ...importedRules];
  }
};

const importRulesFromClipboard = async () => {
  try {
    const text = await readTextFromClipboard();
    const importedRules = parseImportedRules(text);
    applyImportedRules(importedRules);
    message.success(`已导入 ${importedRules.length} 条规则，记得点击“保存配置”落库`);
  } catch (error) {
    showImportDialog.value = true;
    message.warning(error.message || '读取剪贴板失败，请手动粘贴导入');
  }
};

const openImportDialog = () => {
  importText.value = '';
  importMode.value = 'append';
  showImportDialog.value = true;
};

const confirmImportRules = () => {
  try {
    const importedRules = parseImportedRules(importText.value);
    applyImportedRules(importedRules);
    showImportDialog.value = false;
    importText.value = '';
    message.success(`已导入 ${importedRules.length} 条规则，记得点击“保存配置”落库`);
  } catch (error) {
    message.error(error.message || '导入失败');
  }
};

const loadRules = async () => {
  const res = await axios.get(API_URL);
  const data = Array.isArray(res.data?.data) ? res.data.data : [];
  rules.value = data.length > 0
    ? data.map((item) => normalizeRule({ ...createEmptyRule(), ...item }, { editing: false }))
    : [];
};

const open = async () => {
  try {
    await loadRules();
  } catch (error) {
    message.error('加载季集号识别正则失败');
    rules.value = [];
  }
  show.value = true;
};

const saveRules = async () => {
  const payload = normalizeRulesForSave();

  for (let i = 0; i < payload.length; i += 1) {
    const rule = payload[i];

    if (!rule.pattern) {
      message.warning(`规则 ${i + 1} 的正则不能为空`);
      return;
    }

    try {
      new RegExp(rule.pattern);
    } catch (error) {
      message.error(`规则 ${i + 1} 正则语法错误：${error.message}`);
      return;
    }
  }

  saving.value = true;
  try {
    const res = await axios.post(API_URL, {
      rules: payload
    });

    if (res.data?.success) {
      message.success(res.data.message || '保存成功');
      rules.value = payload.map((item) => normalizeRule(item, { editing: false }));
      show.value = false;
      return;
    }

    message.error(res.data?.message || '保存失败');
  } catch (error) {
    message.error(error.response?.data?.message || '保存失败');
  } finally {
    saving.value = false;
  }
};

defineExpose({ open });
</script>

<style scoped>
.rule-toolbar {
  gap: 10px;
}

.toolbar-tip {
  font-size: 12px;
}

.rule-card {
  transition: background-color .2s ease, border-color .2s ease;
}

.rule-card--compact :deep(.n-card__content) {
  padding-top: 8px;
  padding-bottom: 8px;
}

.rule-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.rule-title-wrap {
  min-width: 0;
  flex: 1;
}

.rule-title {
  font-weight: 600;
  white-space: nowrap;
}

.rule-brief {
  color: var(--n-text-color-3);
  font-size: 12px;
  white-space: nowrap;
}

.rule-actions {
  flex-shrink: 0;
}

.compact-rule-body {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  cursor: pointer;
}

.compact-pattern {
  min-width: 0;
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 12px;
  opacity: .82;
}

.desc-text {
  font-size: 12px;
}

.test-divider {
  font-size: 12px;
}

.preview-alert {
  width: 100%;
  word-break: break-all;
}

@media (max-width: 760px) {
  .rule-toolbar,
  .rule-card-header {
    flex-direction: column;
    align-items: flex-start;
  }

  .compact-rule-body {
    align-items: flex-start;
    flex-direction: column;
  }

  .rule-actions {
    width: 100%;
  }
}
</style>
