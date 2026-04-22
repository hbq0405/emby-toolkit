<template>
  <n-modal
    v-model:show="show"
    preset="card"
    title="配置季集号识别正则"
    style="width: 900px; max-width: 96vw;"
  >
    <n-space vertical :size="16">
      <n-alert type="warning" :show-icon="true">
        <b>用途：</b>用于处理动漫、番剧、外挂组资源等各种非标准命名。<br />
        <b>规则：</b>按列表顺序从上到下匹配，<b>命中即返回</b>，不再继续走后续硬编码识别。<br />
        <b>建议：</b>优先写更精确的规则，把泛匹配规则放到后面，避免误识别。
      </n-alert>

      <n-alert type="info" :show-icon="true">
        <b>推荐两种模式：</b><br />
        1. <code>季+集</code>：正则中分别捕获季号和集号。<br />
        2. <code>仅集号</code>：只捕获集号，季号固定使用默认季（一般填 1）。
      </n-alert>

      <n-space justify="space-between" align="center">
        <n-text depth="3">共 {{ rules.length }} 条规则</n-text>
        <n-button type="primary" ghost @click="addRule">
          <template #icon><n-icon :component="AddIcon" /></template>
          新增规则
        </n-button>
      </n-space>

      <n-empty v-if="rules.length === 0" description="暂无规则，点击右上角新增" style="padding: 24px 0;" />

      <n-card
        v-for="(rule, index) in rules"
        :key="index"
        size="small"
        :bordered="true"
        style="background: #fafafa;"
      >
        <template #header>
          <div style="display: flex; align-items: center; justify-content: space-between; gap: 12px;">
            <n-space align="center" :size="10">
              <n-tag size="small" type="primary" :bordered="false">规则 {{ index + 1 }}</n-tag>
              <span style="font-weight: 600;">
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
              <n-input v-model:value="rule.name" placeholder="例如：VCB-Studio 单季番剧" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="匹配模式">
              <n-radio-group v-model:value="rule.mode">
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
                placeholder="例如：S(\d{1,2})E(\d{1,3}) 或 \[(\d{1,3})\]"
              />
            </n-form-item>
          </n-gi>

          <n-gi v-if="rule.mode === 'season_episode'">
            <n-form-item label="季号捕获组">
              <n-input-number v-model:value="rule.season_group" :min="1" :step="1" style="width: 100%;" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item :label="rule.mode === 'season_episode' ? '集号捕获组' : '集号捕获组'">
              <n-input-number v-model:value="rule.episode_group" :min="1" :step="1" style="width: 100%;" />
            </n-form-item>
          </n-gi>
          <n-gi v-if="rule.mode === 'episode_only'">
            <n-form-item label="默认季号">
              <n-input-number v-model:value="rule.default_season" :min="1" :step="1" style="width: 100%;" />
            </n-form-item>
          </n-gi>
          <n-gi span="1 s:2">
            <n-text depth="3" style="font-size: 12px;">
              说明：正则里只需要把数字部分放进捕获组即可。比如 <code>S(\d+)E(\d+)</code>、<code>第(\d+)季.*?第(\d+)话</code>、<code> - (\d{2,3}) </code>。
            </n-text>
          </n-gi>
        </n-grid>
      </n-card>

      <n-divider title-placement="left" style="font-size: 12px; color: #999;">实时测试</n-divider>

      <n-form label-placement="left" label-width="100">
        <n-form-item label="测试文件名">
          <n-input
            v-model:value="testFilename"
            placeholder="例如：[VCB-Studio] Kusuriya no Hitorigoto [03][Ma10p_1080p].mkv"
          />
        </n-form-item>
        <n-form-item label="识别结果">
          <n-alert :type="preview.type" :show-icon="true" style="width: 100%; word-break: break-all;">
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
  useMessage
} from 'naive-ui';
import {
  AddOutline as AddIcon,
  TrashOutline as TrashIcon
} from '@vicons/ionicons5';

const show = ref(false);
const saving = ref(false);
const rules = ref([]);
const testFilename = ref('');
const message = useMessage();

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

const preview = computed(() => {
  const filename = testFilename.value.trim();
  if (!filename) {
    return { type: 'default', text: '请输入测试文件名' };
  }

  for (let i = 0; i < rules.value.length; i += 1) {
    const rule = rules.value[i];
    if (!rule?.enabled || !rule.pattern?.trim()) continue;

    try {
      const regex = new RegExp(rule.pattern, 'i');
      const match = filename.match(regex);
      if (!match) continue;

      const episodeGroup = toPositiveInt(rule.episode_group, 1);
      const rawEpisode = match[episodeGroup];
      const episode = parseCapturedNumber(rawEpisode);
      if (episode == null) {
        return {
          type: 'warning',
          text: `规则 ${i + 1} 已命中，但第 ${episodeGroup} 捕获组不是有效数字：${rawEpisode ?? '空'}`
        };
      }

      if (rule.mode === 'episode_only') {
        const season = toPositiveInt(rule.default_season, 1);
        return {
          type: 'success',
          text: `命中规则 ${i + 1}${rule.name ? `（${rule.name}）` : ''}：识别为 S${String(season).padStart(2, '0')}E${String(episode).padStart(2, '0')}`
        };
      }

      const seasonGroup = toPositiveInt(rule.season_group, 1);
      const rawSeason = match[seasonGroup];
      const season = parseCapturedNumber(rawSeason);
      if (season == null) {
        return {
          type: 'warning',
          text: `规则 ${i + 1} 已命中，但第 ${seasonGroup} 捕获组不是有效数字：${rawSeason ?? '空'}`
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
  return rules.value.map((rule) => ({
    enabled: rule.enabled !== false,
    name: String(rule.name || '').trim(),
    pattern: String(rule.pattern || '').trim(),
    mode: rule.mode === 'episode_only' ? 'episode_only' : 'season_episode',
    season_group: toPositiveInt(rule.season_group, 1),
    episode_group: toPositiveInt(rule.episode_group, 1),
    default_season: toPositiveInt(rule.default_season, 1)
  }));
};

const loadRules = async () => {
  const res = await axios.get('/api/p115/episode_regex_rules');
  const data = Array.isArray(res.data?.data) ? res.data.data : [];
  rules.value = data.length > 0
    ? data.map((item) => ({ ...createEmptyRule(), ...item }))
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
    const res = await axios.post('/api/p115/episode_regex_rules', {
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
