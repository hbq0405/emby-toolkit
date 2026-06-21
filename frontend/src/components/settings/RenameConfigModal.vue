<!-- src/components/settings/RenameConfigModal.vue -->
<template>
  <n-modal v-model:show="isVisible" preset="card" title="自定义重命名规则" style="width: 960px; max-width: 95%;" class="custom-modal glass-modal">
    <n-spin :show="loading">
      <n-tabs type="segment" animated size="small">
        <n-tab-pane name="template" tab="模板规则">
          <n-alert type="info" style="margin-bottom: 12px;">
            底层使用 Jinja2。可以用积木插入变量，也可以直接粘贴 MoviePilot 的重命名模板。
          </n-alert>

          <div class="template-grid">
            <n-form-item label="主目录模板">
              <n-input
                ref="mainInputRef"
                v-model:value="config.main_dir_template"
                type="textarea"
                :autosize="{ minRows: 2, maxRows: 4 }"
                @focus="activeTemplate = 'main_dir_template'"
              />
            </n-form-item>

            <n-form-item label="季目录模板">
              <n-input
                ref="seasonInputRef"
                v-model:value="config.season_dir_template"
                type="textarea"
                :autosize="{ minRows: 2, maxRows: 4 }"
                @focus="activeTemplate = 'season_dir_template'"
              />
            </n-form-item>

            <n-form-item v-if="!config.keep_original_name" label="文件名模板">
              <n-input
                ref="fileInputRef"
                v-model:value="config.file_template"
                type="textarea"
                :autosize="{ minRows: 3, maxRows: 6 }"
                @focus="activeTemplate = 'file_template'"
              />
            </n-form-item>

            <n-form-item label="插入到">
              <n-select v-model:value="activeTemplate" :options="templateTargetOptions" style="max-width: 220px;" />
            </n-form-item>
          </div>

          <div class="lego-container">
            <div class="lego-header">
              <span>积木变量</span>
              <span class="header-tip">点击后插入到当前模板</span>
            </div>
            <div class="block-pool">
              <n-tag v-for="block in templateBlocks" :key="block.label" type="info" class="lego-block" @click="insertSnippet(block.snippet)">
                + {{ block.label }}
              </n-tag>
            </div>
          </div>

          <div class="lego-container">
            <div class="lego-header"><span>MoviePilot 模板导入</span></div>
            <n-input
              v-model:value="mpTemplate"
              type="textarea"
              placeholder="粘贴 MP 的整段重命名模板，例如：{{title}} ({% raw %}{{year}}{% endraw %}) {tmdb={{tmdbid}}}/Season {{season_no}}/{{title}} - {{season_episode}}{{fileExt}}"
              :autosize="{ minRows: 3, maxRows: 6 }"
            />
            <n-space style="margin-top: 8px;">
              <n-button size="small" secondary type="primary" @click="importMpTemplate('movie')">按电影模板导入</n-button>
              <n-button size="small" secondary type="primary" @click="importMpTemplate('tv')">按剧集模板导入</n-button>
            </n-space>
          </div>
        </n-tab-pane>

        <n-tab-pane name="file" tab="文件设置">
          <n-form-item
            label="保留原始文件名"
            label-placement="left"
            style="margin-bottom: 12px; background: rgba(24, 160, 88, 0.05); padding: 8px 12px; border-radius: 6px;"
          >
            <n-switch v-model:value="config.keep_original_name" />
            <template #feedback>
              <span style="font-size: 12px; color: gray;">
                开启后仅保留最终文件名；主目录、季目录、STRM 路径和缓存路径仍按目录模板生成。
              </span>
            </template>
          </n-form-item>
        </n-tab-pane>

        <n-tab-pane name="adv" tab="高级设置">
          <n-form label-placement="left" size="small" style="margin-top: 16px;">
            <n-form-item label="同集/同电影覆盖模式">
              <n-radio-group v-model:value="config.conflict_mode">
                <n-space vertical>
                  <n-radio value="replace">
                    <b>洗版</b>
                    <div style="font-size: 12px; color: gray;">删除目标目录中同一集/同一电影的旧版本，移入新版本。</div>
                  </n-radio>
                  <n-radio value="keep_both">
                    <b>共存</b>
                    <div style="font-size: 12px; color: gray;">只要文件名不同，同一集的不同版本将共存。</div>
                  </n-radio>
                  <n-radio value="skip">
                    <b>跳过</b>
                    <div style="font-size: 12px; color: gray;">只要目标目录已有该集/该电影，新文件直接丢入未识别。</div>
                  </n-radio>
                </n-space>
              </n-radio-group>
            </n-form-item>
            <n-divider style="margin: 12px 0;" />

            <n-form-item label="STRM 链接格式">
              <n-radio-group v-model:value="config.strm_url_fmt">
                <n-space vertical>
                  <n-radio value="standard">标准格式 (/api/p115/play/xxx)</n-radio>
                  <n-radio value="with_name">带文件名后缀 (/api/p115/play/xxx/文件名.mkv)</n-radio>
                </n-space>
              </n-radio-group>
            </n-form-item>
          </n-form>
        </n-tab-pane>
      </n-tabs>

      <div class="preview-container">
        <div class="preview-header">
          <n-icon size="18" color="#18a058" style="margin-right: 6px;"><EyeIcon /></n-icon>
          实时效果预览
        </div>

        <div class="preview-content">
          <n-grid cols="1 m:2" :x-gap="24">
            <n-gi>
              <div class="section-title">电影示例</div>
              <div class="tree-node">
                <n-icon color="#f0a020" size="16"><FolderIcon /></n-icon>
                <span class="node-text">{{ previewMovieDir }}</span>
              </div>
              <div class="tree-node child">
                <n-icon color="#2080f0" size="16"><DocumentIcon /></n-icon>
                <span class="node-text">{{ previewMovieFile }}</span>
              </div>
              <div class="tree-node grandchild">
                <n-icon color="#888" size="14"><LinkIcon /></n-icon>
                <span class="node-text" style="color: #888; font-size: 11px;">{{ previewMovieStrm }}</span>
              </div>
            </n-gi>
            <n-gi>
              <div class="section-title">剧集示例</div>
              <div class="tree-node">
                <n-icon color="#f0a020" size="16"><FolderIcon /></n-icon>
                <span class="node-text">{{ previewTvDir }}</span>
              </div>
              <div class="tree-node child">
                <n-icon color="#f0a020" size="16"><FolderIcon /></n-icon>
                <span class="node-text">{{ previewTvSeason }}</span>
              </div>
              <div class="tree-node grandchild">
                <n-icon color="#2080f0" size="16"><DocumentIcon /></n-icon>
                <span class="node-text">{{ previewTvFile }}</span>
              </div>
              <div class="tree-node grandchild" style="padding-left: 72px;">
                <n-icon color="#888" size="14"><LinkIcon /></n-icon>
                <span class="node-text" style="color: #888; font-size: 11px;">{{ previewTvStrm }}</span>
              </div>
            </n-gi>
          </n-grid>
        </div>
      </div>
    </n-spin>

    <template #footer>
      <n-space justify="end">
        <n-button @click="isVisible = false">取消</n-button>
        <n-button type="primary" @click="saveConfig" :loading="saving">保存规则</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, computed } from 'vue';
import { NModal, NGrid, NGi, NTabs, NTabPane, NForm, NFormItem, NRadioGroup, NSwitch, NSpace, NButton, NIcon, NSpin, NTag, useMessage, NRadio, NAlert, NInput, NSelect, NDivider } from 'naive-ui';
import { Folder as FolderIcon, DocumentTextOutline as DocumentIcon, EyeOutline as EyeIcon, LinkOutline as LinkIcon } from '@vicons/ionicons5';
import axios from 'axios';

const message = useMessage();
const isVisible = ref(false);
const loading = ref(false);
const saving = ref(false);
const activeTemplate = ref('main_dir_template');
const mpTemplate = ref('');

const defaultConfig = {
  keep_original_name: false,
  conflict_mode: 'replace',
  main_dir_template: '{{title}}{% if year %} ({{year}}){% endif %} {tmdb={{tmdbid}}}',
  season_dir_template: 'Season {{season_no}}',
  file_template: '{{title}}{% if year %} ({{year}}){% endif %}{% if season_episode %} · {{season_episode}}{% endif %}{% if resolution %} · {{resolution}}{% endif %}{% if videoCodec %} · {{videoCodec | upper}}{% endif %}{% if audioCodec %} · {{audioCodec}}{% endif %}{% if releaseGroup %} · {{releaseGroup}}{% endif %}{{fileExt}}',
  main_dir_format: ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'],
  season_dir_format: ['season_name_en'],
  file_format: ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio_count', 'sep_space', 'audio', 'sep_middot_space', 'group'],
  strm_url_fmt: 'standard'
};

const config = ref({ ...defaultConfig });

const templateTargetOptions = [
  { label: '主目录模板', value: 'main_dir_template' },
  { label: '季目录模板', value: 'season_dir_template' },
  { label: '文件名模板', value: 'file_template' }
];

const templateBlocks = [
  { label: '中文片名', snippet: '{{title}}' },
  { label: '英文片名', snippet: '{{title_en}}' },
  { label: '原文片名', snippet: '{{title_orig}}' },
  { label: '年份', snippet: '{{year}}' },
  { label: '年份括号', snippet: '{% if year %} ({{year}}){% endif %}' },
  { label: 'TMDb ID', snippet: '{{tmdbid}}' },
  { label: 'TMDb {tmdb=ID}', snippet: '{tmdb={{tmdbid}}}' },
  { label: 'TMDb {tmdb-ID}', snippet: '{tmdb-{{tmdbid}}}' },
  { label: '季集 S01E01', snippet: '{{season_episode}}' },
  { label: '季号 01', snippet: '{{season_no}}' },
  { label: '集号 01', snippet: '{{episode_no}}' },
  { label: '中文季号 (第 1 季)', snippet: '{{season_name_zh}}' },
  { label: '中文集号 (第 1 集)', snippet: '{{episode_name_zh}}' },
  { label: '中文季集号 (第 1 季 1 集)', snippet: '{{season_episode_zh}}' },
  { label: '分辨率', snippet: '{{resolution}}' },
  { label: '来源', snippet: '{{videoFormat}}' },
  { label: '视频编码', snippet: '{{videoCodec | upper}}' },
  { label: '音频编码', snippet: '{{audioCodec}}' },
  { label: '发布组', snippet: '{{releaseGroup}}' },
  { label: '文件扩展名', snippet: '{{fileExt}}' },
  { label: '空格', snippet: ' ' },
  { label: '中圆点', snippet: ' · ' },
  { label: '短横线', snippet: ' - ' },
  { label: '目录分层', snippet: '/' }
];

const mockMovie = {
  title: '寄生虫',
  title_en: 'Parasite',
  title_orig: '기생충',
  year: '2019',
  tmdbid: '496243',
  resolution: '1080p',
  videoFormat: 'BluRay',
  videoCodec: 'AVC',
  audioCodec: 'DDP 5.1',
  releaseGroup: 'CMCT',
  fileExt: '.mkv',
  originalFile: 'Parasite.2019.REMASTERED.1080p.BluRay.x264.mkv'
};

const mockTv = {
  title: '绝命毒师',
  title_en: 'Breaking Bad',
  title_orig: 'Breaking Bad',
  year: '2008',
  tmdbid: '1396',
  season: 1,
  episode: 1,
  season_no: '01',
  episode_no: '01',
  season_episode: 'S01E01',
  season_name_zh: '第 1 季',
  episode_name_zh: '第 1 集',
  season_episode_zh: '第 1 季 1 集',
  resolution: '2160p',
  videoFormat: 'WEB-DL',
  videoCodec: 'HEVC',
  audioCodec: 'Atmos',
  releaseGroup: 'HHWEB',
  fileExt: '.mp4',
  originalFile: 'Breaking.Bad.S01E01.2160p.WEB-DL.x265.mp4'
};

const insertSnippet = (snippet) => {
  const key = activeTemplate.value;
  config.value[key] = `${config.value[key] || ''}${snippet}`;
};

const normalizeMpTemplate = (template) => {
  return String(template || '').replace(/{{\s*([A-Za-z_]\w*)\s*\|\s*string\s*}\s*\.zfill\((\d+)\)\s*}}/g, '{{ ($1|string).zfill($2) }}');
};

const splitTemplateFromRight = (template, separatorCount) => {
  const parts = [];
  let end = template.length;
  for (let i = 0; i < separatorCount; i++) {
    const index = template.lastIndexOf('/', end - 1);
    if (index < 0) return null;
    parts.unshift(template.slice(index + 1, end).trim());
    end = index;
  }
  parts.unshift(template.slice(0, end).trim());
  return parts;
};

const importMpTemplate = (type) => {
  const raw = normalizeMpTemplate(mpTemplate.value).trim();
  if (!raw) {
    message.warning('请先粘贴 MP 模板');
    return;
  }
  if (type === 'movie') {
    const parts = splitTemplateFromRight(raw, 1);
    if (!parts || parts.some(part => !part)) {
      message.warning('电影模板至少需要包含 主目录/文件名 两段');
      return;
    }
    config.value.main_dir_template = parts[0];
    config.value.file_template = parts[1];
  } else {
    const parts = splitTemplateFromRight(raw, 2);
    if (!parts || parts.some(part => !part)) {
      message.warning('剧集模板至少需要包含 主目录/季目录/文件名 三段');
      return;
    }
    config.value.main_dir_template = parts[0];
    config.value.season_dir_template = parts[1];
    config.value.file_template = parts[2];
  }
  message.success('已导入模板');
};

const valueByPath = (data, name) => {
  const key = name.trim();
  if (key === 'season|string') return String(data.season || '');
  return data[key] ?? '';
};

const renderExpression = (expr, data) => {
  let text = expr.trim();
  const zfillMatch = text.match(/^\((\w+)\|string\)\.zfill\((\d+)\)$/);
  if (zfillMatch) {
    return String(data[zfillMatch[1]] ?? '').padStart(Number(zfillMatch[2]), '0');
  }
  const pipeParts = text.split('|').map(v => v.trim());
  let value = valueByPath(data, pipeParts[0]);
  for (const filter of pipeParts.slice(1)) {
    if (filter === 'upper') value = String(value).toUpperCase();
    if (filter === 'string') value = String(value);
  }
  return value;
};

const renderTemplate = (template, data) => {
  let output = normalizeMpTemplate(template);
  for (let i = 0; i < 8; i++) {
    const next = output.replace(/{%\s*if\s+(\w+)\s*%}([\s\S]*?){%\s*endif\s*%}/g, (_, key, body) => data[key] ? body : '');
    if (next === output) break;
    output = next;
  }
  output = output.replace(/{{\s*([^}]+?)\s*}}/g, (_, expr) => renderExpression(expr, data));
  return output.replace(/[\\:*?"<>|]/g, '').trim();
};

const withExt = (name, data) => {
  if (!name) return data.originalFile;
  if (/\{\{\s*(fileExt|file_ext)\s*\}\}/.test(config.value.file_template || '') || name.toLowerCase().endsWith(data.fileExt.toLowerCase())) {
    return name;
  }
  return `${name}${data.fileExt}`;
};

const previewMovieDir = computed(() =>
  renderTemplate(config.value.main_dir_template, mockMovie) || '未配置主目录规则'
);

const previewTvDir = computed(() =>
  renderTemplate(config.value.main_dir_template, mockTv) || '未配置主目录规则'
);

const previewTvSeason = computed(() =>
  renderTemplate(config.value.season_dir_template, mockTv) || '未配置季目录规则'
);

const previewMovieFile = computed(() =>
  config.value.keep_original_name
    ? mockMovie.originalFile
    : withExt(renderTemplate(config.value.file_template, mockMovie), mockMovie)
);

const previewTvFile = computed(() =>
  config.value.keep_original_name
    ? mockTv.originalFile
    : withExt(renderTemplate(config.value.file_template, mockTv), mockTv)
);

const previewMovieStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/abc123xyz';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewMovieFile.value}` : baseUrl;
});

const previewTvStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/def456uvw';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewTvFile.value}` : baseUrl;
});

const ensureTemplateDefaults = (data) => {
  const next = { ...defaultConfig, ...(data || {}) };
  if (!next.main_dir_template) next.main_dir_template = defaultConfig.main_dir_template;
  if (!next.season_dir_template) next.season_dir_template = defaultConfig.season_dir_template;
  if (!next.file_template) next.file_template = defaultConfig.file_template;
  return next;
};

const open = async () => {
  isVisible.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/rename_config');
    if (res.data.success) {
      config.value = ensureTemplateDefaults(res.data.data);
    }
  } catch (e) {
    message.error('加载配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  if (!String(config.value.main_dir_template || '').trim()) {
    message.warning('主目录模板不能为空');
    return;
  }
  if (!String(config.value.season_dir_template || '').trim()) {
    message.warning('季目录模板不能为空');
    return;
  }
  if (!config.value.keep_original_name && !String(config.value.file_template || '').trim()) {
    message.warning('文件名模板不能为空；如需跳过文件重命名，请开启“保留原始文件名”');
    return;
  }

  saving.value = true;
  try {
    const res = await axios.post('/api/p115/rename_config', config.value);
    if (res.data.success) {
      message.success('重命名规则已保存');
      isVisible.value = false;
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    saving.value = false;
  }
};

defineExpose({ open });
</script>

<style scoped>
.template-grid {
  display: grid;
  gap: 8px;
}

.lego-container {
  background: rgba(0, 0, 0, 0.02);
  border: 1px dashed var(--n-divider-color);
  border-radius: 8px;
  padding: 16px;
  margin-top: 12px;
}

.lego-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 13px;
  color: var(--n-text-color-3);
  margin-bottom: 12px;
  font-weight: bold;
}

.header-tip {
  font-size: 12px;
  font-weight: normal;
}

.block-pool {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  min-height: 34px;
}

.lego-block {
  cursor: pointer;
  transition: all 0.2s;
}

.lego-block:hover {
  transform: translateY(-2px);
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

.preview-container {
  margin-top: 20px;
  background: rgba(0, 0, 0, 0.02);
  border-radius: 8px;
  padding: 16px;
}

.preview-header {
  display: flex;
  align-items: center;
  font-weight: bold;
  margin-bottom: 12px;
}

.preview-content {
  background: var(--n-color-modal);
  padding: 12px;
  border-radius: 6px;
}

.section-title {
  font-weight: bold;
  font-size: 13px;
  margin-bottom: 8px;
  color: var(--n-text-color-2);
}

.tree-node {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
  min-width: 0;
}

.tree-node.child {
  padding-left: 24px;
}

.tree-node.grandchild {
  padding-left: 48px;
}

.node-text {
  font-family: monospace;
  font-size: 12px;
  word-break: break-all;
}
</style>
