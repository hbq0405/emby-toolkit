<!-- src/components/settings/RenameConfigModal.vue -->
<template>
  <n-modal v-model:show="isVisible" preset="card" title="自定义重命名规则" style="width: 850px; max-width: 95%;">
    <n-spin :show="loading">
      <n-grid cols="1 m:2" :x-gap="24" :y-gap="24" responsive="screen">
        
        <!-- 左侧：配置表单 -->
        <n-gi>
          <n-tabs type="segment" animated size="small">
            
            <!-- 标签页 1：目录命名 -->
            <n-tab-pane name="dir" tab="目录命名">
              <n-form label-placement="left" label-width="90" size="small" style="margin-top: 10px;">
                <n-divider title-placement="left" style="margin-top: 0; font-size: 13px; color: #888;">主目录 (电影/剧集)</n-divider>
                
                <n-form-item label="片名语言">
                  <n-radio-group v-model:value="config.main_title_lang">
                    <n-radio-button value="zh">中文优先</n-radio-button>
                    <n-radio-button value="original">原名优先</n-radio-button>
                  </n-radio-group>
                </n-form-item>
                
                <n-form-item label="附加年份">
                  <n-switch v-model:value="config.main_year_en" />
                </n-form-item>
                
                <n-form-item label="TMDb 标签">
                  <n-select v-model:value="config.main_tmdb_fmt" :options="tmdbOptions" />
                </n-form-item>

                <n-divider title-placement="left" style="font-size: 13px; color: #888;">季目录 (仅剧集)</n-divider>
                
                <n-form-item label="命名格式">
                  <n-select v-model:value="config.season_fmt" :options="seasonOptions" />
                </n-form-item>
              </n-form>
            </n-tab-pane>

            <!-- 标签页 2：文件命名 -->
            <n-tab-pane name="file" tab="文件命名">
              <n-form label-placement="left" label-width="90" size="small" style="margin-top: 10px;">
                <n-form-item label="片名语言">
                  <n-radio-group v-model:value="config.file_title_lang">
                    <n-radio-button value="zh">中文优先</n-radio-button>
                    <n-radio-button value="original">原名优先</n-radio-button>
                  </n-radio-group>
                </n-form-item>
                
                <n-form-item label="附加年份">
                  <n-switch v-model:value="config.file_year_en" />
                </n-form-item>
                
                <n-form-item label="TMDb 标签">
                  <n-select v-model:value="config.file_tmdb_fmt" :options="tmdbOptions" />
                </n-form-item>

                <n-form-item label="连接符号">
                  <n-select v-model:value="config.file_sep" :options="sepOptions" />
                </n-form-item>

                <n-form-item label="视频参数">
                  <n-switch v-model:value="config.file_params_en" />
                  <template #feedback>
                    <span style="font-size: 12px; color: gray;">保留分辨率、编码、特效等信息 (如 1080p · H265)</span>
                  </template>
                </n-form-item>
                <n-divider title-placement="left" style="font-size: 13px; color: #888;">STRM 链接设置</n-divider>
                <n-form-item label="链接格式">
                  <n-select v-model:value="config.strm_url_fmt" :options="strmUrlOptions" />
                  <template #feedback>
                    <span style="font-size: 12px; color: gray;">该选项对第三方STRM无效</span>
                  </template>
                </n-form-item>
              </n-form>
            </n-tab-pane>
          </n-tabs>
        </n-gi>

        <!-- 右侧：实时预览 -->
        <n-gi>
          <div class="preview-container">
            <div class="preview-header">
              <n-icon size="18" color="#18a058" style="margin-right: 6px;"><EyeIcon /></n-icon>
              实时效果预览
            </div>
            
            <div class="preview-content">
              <!-- 电影预览 -->
              <div class="preview-section">
                <div class="section-title">🎬 电影示例 (The Dark Knight)</div>
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
              </div>

              <n-divider style="margin: 16px 0;" />

              <!-- 剧集预览 -->
              <div class="preview-section">
                <div class="section-title">📺 剧集示例 (Breaking Bad)</div>
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
              </div>
            </div>
          </div>
        </n-gi>
      </n-grid>
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
import { NModal, NGrid, NGi, NTabs, NTabPane, NForm, NFormItem, NRadioGroup, NRadioButton, NSwitch, NSelect, NDivider, NSpace, NButton, NIcon, NSpin, useMessage } from 'naive-ui';
import { Folder as FolderIcon, DocumentTextOutline as DocumentIcon, EyeOutline as EyeIcon } from '@vicons/ionicons5';
import axios from 'axios';

const message = useMessage();
const isVisible = ref(false);
const loading = ref(false);
const saving = ref(false);

// 默认配置
const config = ref({
  main_title_lang: 'zh',
  main_year_en: true,
  main_tmdb_fmt: '{tmdb=ID}',
  season_fmt: 'Season {02}',
  file_title_lang: 'zh',
  file_year_en: false,
  file_tmdb_fmt: 'none',
  file_params_en: true,
  file_sep: ' - ',
  strm_url_fmt: 'standard'
});

// 2. 新增下拉选项
const strmUrlOptions = [
  { label: '标准格式 (/api/p115/play/xxx)', value: 'standard' },
  { label: '带文件名后缀 (/api/p115/play/xxx/文件名.mkv)', value: 'with_name' }
];

// 3. 新增计算属性用于预览
const previewMovieStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/abc123xyz';
  if (config.value.strm_url_fmt === 'with_name') {
    return `${baseUrl}/${previewMovieFile.value}`;
  }
  return baseUrl;
});

const previewTvStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/def456uvw';
  if (config.value.strm_url_fmt === 'with_name') {
    return `${baseUrl}/${previewTvFile.value}`;
  }
  return baseUrl;
});

// 选项字典
const tmdbOptions = [
  { label: '不添加', value: 'none' },
  { label: '{tmdb=ID} (Emby标准)', value: '{tmdb=ID}' },
  { label: '[tmdbid=ID] (TMM标准)', value: '[tmdbid=ID]' },
  { label: 'tmdb-ID', value: 'tmdb-ID' }
];

const seasonOptions = [
  { label: 'Season 01 (补零)', value: 'Season {02}' },
  { label: 'Season 1 (不补零)', value: 'Season {1}' },
  { label: 'S01 (简写补零)', value: 'S{02}' },
  { label: 'S1 (简写不补零)', value: 'S{1}' },
  { label: '第1季 (中文)', value: '第{1}季' }
];

const sepOptions = [
  { label: '空格 - 空格 ( - )', value: ' - ' },
  { label: '点 (.)', value: '.' },
  { label: '下划线 (_)', value: '_' },
  { label: '空格 ( )', value: ' ' }
];

// 模拟数据
const mockMovie = { zh: '蝙蝠侠：黑暗骑士', en: 'The Dark Knight', year: '2008', tmdb: '155', params: '1080p · H264', ext: '.mkv' };
const mockTv = { zh: '绝命毒师', en: 'Breaking Bad', year: '2008', tmdb: '1396', s: '1', e: '1', params: '2160p · HDR · H265', ext: '.mp4' };

// 实时预览计算属性
const previewMovieDir = computed(() => {
  let name = config.value.main_title_lang === 'zh' ? mockMovie.zh : mockMovie.en;
  if (config.value.main_year_en) name += ` (${mockMovie.year})`;
  if (config.value.main_tmdb_fmt !== 'none') name += ` ${config.value.main_tmdb_fmt.replace('ID', mockMovie.tmdb)}`;
  return name;
});

const previewMovieFile = computed(() => {
  let baseTitle = config.value.file_title_lang === 'zh' ? mockMovie.zh : mockMovie.en;
  if (config.value.file_year_en) baseTitle += ` (${mockMovie.year})`;
  
  let parts = [baseTitle];
  if (config.value.file_tmdb_fmt !== 'none') parts.push(config.value.file_tmdb_fmt.replace('ID', mockMovie.tmdb));
  
  let name = parts.join(config.value.file_sep);
  if (config.value.file_params_en) {
    name += config.value.file_sep === '.' ? `.${mockMovie.params.replace(/ · /g, '.')}` : ` · ${mockMovie.params}`;
  }
  return name + mockMovie.ext;
});

const previewTvDir = computed(() => {
  let name = config.value.main_title_lang === 'zh' ? mockTv.zh : mockTv.en;
  if (config.value.main_year_en) name += ` (${mockTv.year})`;
  if (config.value.main_tmdb_fmt !== 'none') name += ` ${config.value.main_tmdb_fmt.replace('ID', mockTv.tmdb)}`;
  return name;
});

const previewTvSeason = computed(() => {
  return config.value.season_fmt.replace('{02}', '01').replace('{1}', '1');
});

const previewTvFile = computed(() => {
  let baseTitle = config.value.file_title_lang === 'zh' ? mockTv.zh : mockTv.en;
  if (config.value.file_year_en) baseTitle += ` (${mockTv.year})`;
  
  let parts = [baseTitle];
  if (config.value.file_tmdb_fmt !== 'none') parts.push(config.value.file_tmdb_fmt.replace('ID', mockTv.tmdb));
  
  parts.push(`S0${mockTv.s}E0${mockTv.e}`);
  
  let name = parts.join(config.value.file_sep);
  if (config.value.file_params_en) {
    name += config.value.file_sep === '.' ? `.${mockTv.params.replace(/ · /g, '.')}` : ` · ${mockTv.params}`;
  }
  return name + mockTv.ext;
});

// 暴露给父组件的方法
const open = async () => {
  isVisible.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/rename_config');
    if (res.data.success) {
      config.value = res.data.data;
    }
  } catch (e) {
    message.error('加载配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
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
.preview-container {
  background-color: var(--n-color-modal);
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
  height: 100%;
  display: flex;
  flex-direction: column;
}

.preview-header {
  padding: 12px 16px;
  border-bottom: 1px solid var(--n-divider-color);
  font-weight: bold;
  display: flex;
  align-items: center;
  background-color: rgba(24, 160, 88, 0.05);
  color: var(--n-text-color-1);
}

.preview-content {
  padding: 16px;
  flex: 1;
  font-family: monospace;
  font-size: 13px;
}

.section-title {
  color: var(--n-text-color-3);
  margin-bottom: 12px;
  font-size: 12px;
  font-weight: bold;
}

.tree-node {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
  color: var(--n-text-color-2);
}

.tree-node.child {
  padding-left: 24px;
  position: relative;
}

.tree-node.child::before {
  content: "└─";
  position: absolute;
  left: 6px;
  color: var(--n-divider-color);
}

.tree-node.grandchild {
  padding-left: 48px;
  position: relative;
}

.tree-node.grandchild::before {
  content: "└─";
  position: absolute;
  left: 30px;
  color: var(--n-divider-color);
}

.node-text {
  margin-left: 8px;
  word-break: break-all;
}
</style>