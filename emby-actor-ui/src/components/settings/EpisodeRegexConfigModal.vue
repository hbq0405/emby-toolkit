<template>
  <n-modal
    v-model:show="show"
    preset="card"
    title="配置季集号识别正则"
    :style="modalStyle"
    class="episode-regex-modal"
  >
    <n-space vertical :size="16">
      <n-alert type="warning" :show-icon="true">
        <b>用途：</b>用于处理动漫、番剧、字幕组资源等各种非标准命名。<br />
        <b>优先级：</b>按列表顺序从上到下匹配，<b>命中即返回</b>，不再继续走后续硬编码识别。<br />
        <b>建议：</b>精确规则放前面，泛匹配规则放后面，避免误识别。
      </n-alert>

      <n-alert type="info" :show-icon="true">
        <b>推荐两种模式：</b><br />
        1. <code>季 + 集</code>：正则中分别捕获季号和集号。<br />
        2. <code>仅集号</code>：只需要保留一个“集号捕获组”，切换后会自动固定为第 1 组，季号使用默认季（通常填 1）。
      </n-alert>

      <n-space justify="space-between" align="center">
        <n-text depth="3">共 {{ rules.length }} 条规则</n-text>
        <n-button type="primary" ghost @click="addRule">
          <template #icon><n-icon :component="AddIcon" /></template>
          新增规则
        </n-button>
      </n-space>

      <n-empty
        v-if="rules.length === 0"
        description="暂无规则，点击右上角新增"
        style="padding: 24px 0;"
      />

      <n-card
        v-for="(rule, index) in rules"
        :key="index"
        size="small"
        :bordered="true"
        class="rule-card"
        :content-style="ruleCardContentStyle"
      >
        <template #header>
          <div class="rule-card-header">
            <n-space align="center" :size="10">
              <n-tag size="small" type="primary" :bordered="false">规则 {{ index + 1 }}</n-tag>
              <span class="rule-title">
                {{ rule.name?.trim() || '未命名规则' }}
              </span>
            </n-space>
            <n-space align="center" :size="8">
              <n-switch v-model:value="rule.enabled">
                <template #checked>启用</template>
                <template #unchecked>停用</template>
              </n-switch>
              <n-button tertiary type="error" @click="removeRule(index)">
                <template #icon><n-icon :component="TrashIcon" /></template>
                删除
              </n-button>
            </n-space>
          </div>
        </template>

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
                当前为“仅集号”模式：前端会自动将 <b>集号捕获组固定为 1</b>，避免误填成 2 之类的值。
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

const show = ref(false);
const saving = ref(false);
const rules = ref([]);
const testFilename = ref('');
const message = useMessage();
const themeVars = useThemeVars();

const modalStyle = computed(() => ({
  width: '900px',
  maxWidth: '96vw'
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
  default_season: 1
});

const toPositiveInt = (value, fallback) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? Math.trunc(num) : fallback;
};

const parseCapturedNumber = (value) => {
  const normalized = String(value ?? '').trim().replace(/^0+(?=\d)/, '');
  if (!normalized) return null;
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
};

const normalizeRule = (rule = {}) => {
  const normalized = {
    enabled: rule.enabled !== false,
    name: String(rule.name || '').trim(),
    pattern: String(rule.pattern || '').trim(),
    mode: rule.mode === 'episode_only' ? 'episode_only' : 'season_episode',
    season_group: toPositiveInt(rule.season_group, 1),
    episode_group: toPositiveInt(rule.episode_group, 2),
    default_season: toPositiveInt(rule.default_season, 1)
  };

  if (normalized.mode === 'episode_only') {
    normalized.episode_group = 1;
    normalized.season_group = 1;
  }

  return normalized;
};

const handleRuleModeChange = (rule) => {
  if (!rule) return;

  if (rule.mode === 'episode_only') {
    rule.episode_group = 1;
    rule.default_season = toPositiveInt(rule.default_season, 1);
  } else {
    rule.season_group = toPositiveInt(rule.season_group, 1);
    rule.episode_group = toPositiveInt(rule.episode_group, 2);
  }
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

        const season = toPositiveInt(rule.default_season, 1);
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
  return rules.value.map((rule) => normalizeRule(rule));
};

const loadRules = async () => {
  const res = await axios.get(API_URL);
  const data = Array.isArray(res.data?.data) ? res.data.data : [];
  rules.value = data.length > 0
    ? data.map((item) => normalizeRule({ ...createEmptyRule(), ...item }))
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
.rule-card {
  transition: background-color .2s ease, border-color .2s ease;
}

.rule-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.rule-title {
  font-weight: 600;
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

@media (max-width: 640px) {
  .rule-card-header {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>
