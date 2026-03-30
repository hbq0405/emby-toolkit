<!-- src/components/settings/RenameConfigModal.vue -->
<template>
  <n-modal v-model:show="isVisible" preset="card" title="自定义重命名规则" style="width: 900px; max-width: 95%;">
    <n-spin :show="loading">
      
      <!-- 顶部：配置区域 -->
      <div class="config-section">
        <n-tabs type="segment" animated size="small">
          
          <!-- 标签页 1：目录命名 (乐高轨道) -->
          <n-tab-pane name="dir" tab="目录命名 (拖拽排序)">
            <n-form-item label="保留原名" label-placement="left" style="margin-bottom: 12px; background: rgba(24, 160, 88, 0.05); padding: 8px 12px; border-radius: 6px;">
              <n-switch v-model:value="config.keep_original_name" />
              <template #feedback>
                <span style="font-size: 12px; color: gray;">开启后仅做归类，原目录架构和文件名原封不动 (覆盖下方所有规则)</span>
              </template>
            </n-form-item>

            <div class="lego-container" v-if="!config.keep_original_name">
              <div class="lego-header"><span>📁 主目录轨道 (支持使用 / 创建多级目录)</span></div>
              <div class="block-pool">
                <n-tag v-for="block in getAvailableBlocks('main_dir')" :key="block.id" :type="block.isSep ? 'warning' : 'info'" class="lego-block" @click="addBlock(block, 'main_dir')">
                  + {{ block.label }}
                </n-tag>
              </div>
              <div class="active-track" style="margin-top: 8px; margin-bottom: 16px;">
                <transition-group name="list">
                  <div v-for="(block, index) in activeTracks.main_dir" :key="block.uid" :class="['track-item', block.isSep ? 'is-sep' : '']" draggable="true" @dragstart="dragStart($event, index, 'main_dir')" @dragover.prevent @drop="drop($event, index, 'main_dir')" @click="removeBlock(index, 'main_dir')">
                    <n-icon size="14" class="drag-handle"><MenuIcon /></n-icon>{{ block.label }}
                  </div>
                </transition-group>
                <div v-if="activeTracks.main_dir.length === 0" class="empty-tip">轨道为空</div>
              </div>

              <div class="lego-header"><span>📂 季目录轨道 (仅剧集有效)</span></div>
              <div class="block-pool">
                <n-tag v-for="block in getAvailableBlocks('season_dir')" :key="block.id" :type="block.isSep ? 'warning' : 'info'" class="lego-block" @click="addBlock(block, 'season_dir')">
                  + {{ block.label }}
                </n-tag>
              </div>
              <div class="active-track" style="margin-top: 8px;">
                <transition-group name="list">
                  <div v-for="(block, index) in activeTracks.season_dir" :key="block.uid" :class="['track-item', block.isSep ? 'is-sep' : '']" draggable="true" @dragstart="dragStart($event, index, 'season_dir')" @dragover.prevent @drop="drop($event, index, 'season_dir')" @click="removeBlock(index, 'season_dir')">
                    <n-icon size="14" class="drag-handle"><MenuIcon /></n-icon>{{ block.label }}
                  </div>
                </transition-group>
                <div v-if="activeTracks.season_dir.length === 0" class="empty-tip">轨道为空</div>
              </div>
            </div>
          </n-tab-pane>

          <!-- 标签页 2：文件命名 (乐高轨道) -->
          <n-tab-pane name="file" tab="文件命名 (拖拽排序)">
            <div class="lego-container" v-if="!config.keep_original_name">
              <div class="lego-header"><span>📄 文件名轨道</span></div>
              <div class="block-pool">
                <n-tag v-for="block in getAvailableBlocks('file')" :key="block.id" :type="block.isSep ? 'warning' : 'info'" class="lego-block" @click="addBlock(block, 'file')">
                  + {{ block.label }}
                </n-tag>
              </div>
              <div class="active-track" style="margin-top: 8px;">
                <transition-group name="list">
                  <div v-for="(block, index) in activeTracks.file" :key="block.uid" :class="['track-item', block.isSep ? 'is-sep' : '']" draggable="true" @dragstart="dragStart($event, index, 'file')" @dragover.prevent @drop="drop($event, index, 'file')" @click="removeBlock(index, 'file')">
                    <n-icon size="14" class="drag-handle"><MenuIcon /></n-icon>{{ block.label }}
                  </div>
                </transition-group>
                <div v-if="activeTracks.file.length === 0" class="empty-tip">轨道为空</div>
              </div>
            </div>
            <n-alert v-else type="info" style="margin-top: 20px;">已开启保留原名，文件重命名被禁用。</n-alert>
          </n-tab-pane>

          <!-- 标签页 3：高级设置 -->
          <n-tab-pane name="adv" tab="高级设置">
            <n-form label-placement="left" size="small" style="margin-top: 16px;">
              
              <!-- ★★★ 新增：覆盖模式选项 ★★★ -->
              <n-form-item label="同集/同电影覆盖模式">
                <n-radio-group v-model:value="config.conflict_mode">
                  <n-space vertical>
                    <n-radio value="replace">
                      <b>仅保留最新 (洗版推荐)</b>
                      <div style="font-size: 12px; color: gray;">删除目标目录中同一集/同一电影的所有旧版本，移入新版本。</div>
                    </n-radio>
                    <n-radio value="keep_both">
                      <b>多版本共存</b>
                      <div style="font-size: 12px; color: gray;">只要文件名不同，同一集的不同版本(如 1080p 和 4K)将共存。</div>
                    </n-radio>
                    <n-radio value="skip">
                      <b>跳过 (防重复下载)</b>
                      <div style="font-size: 12px; color: gray;">只要目标目录已有该集/该电影，新文件直接丢弃打入未识别。</div>
                    </n-radio>
                  </n-space>
                </n-radio-group>
              </n-form-item>
              <n-divider style="margin: 12px 0;" />

              <n-form-item label="智能重命名">
                <n-switch v-model:value="config.enable_smart_rename" />
                <template #feedback>
                  <span style="font-size: 12px; color: gray;">开启后将通过中心服务器查询媒体信息补齐缺失的参数（较慢）。关闭则仅通过文件名猜测(极速)。</span>
                </template>
              </n-form-item>
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
      </div>

      <!-- 底部：实时预览 -->
      <div class="preview-container">
        <div class="preview-header">
          <n-icon size="18" color="#18a058" style="margin-right: 6px;"><EyeIcon /></n-icon>
          实时效果预览
        </div>
        
        <div class="preview-content">
          <n-grid cols="1 m:2" :x-gap="24">
            <n-gi>
              <div class="section-title">🎬 电影示例</div>
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
              <div class="section-title">📺 剧集示例</div>
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
import { ref, computed, watch } from 'vue';
import { NModal, NGrid, NGi, NTabs, NTabPane, NForm, NFormItem, NRadioGroup, NSwitch, NSpace, NButton, NIcon, NSpin, NTag, useMessage, NRadio, NAlert } from 'naive-ui';
import { Folder as FolderIcon, DocumentTextOutline as DocumentIcon, EyeOutline as EyeIcon, Menu as MenuIcon, LinkOutline as LinkIcon } from '@vicons/ionicons5';
import axios from 'axios';

const message = useMessage();
const isVisible = ref(false);
const loading = ref(false);
const saving = ref(false);

// 默认配置
const config = ref({
  keep_original_name: false,
  enable_smart_rename: true,
  conflict_mode: 'replace',
  main_dir_format: ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'],
  season_dir_format: ['season_name_en'],
  file_format: ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio_count', 'sep_space', 'audio', 'sep_middot_space', 'group'],
  strm_url_fmt: 'standard'
});

// 乐高模块定义
const allBlocks = [
  { id: 'title_zh', label: '中文片名' },
  { id: 'title_en', label: '英文/原名' },
  { id: 'year', label: '年份 (2008)' },
  { id: 'year_pure', label: '纯年份 2008' },
  { id: 's_e', label: '季集号 (S01E01)' },
  { id: 'season_name_en', label: '季名 (Season 01)' },
  { id: 'season_name_en_no0', label: '季名 (Season 1)' },
  { id: 'season_name_zh', label: '季名 (第1季)' },
  { id: 'season_name_s', label: '季名 (S01)' },
  { id: 'season_name_s_no0', label: '季名 (S1)' },
  { id: 'resolution', label: '分辨率' },
  { id: 'source', label: '来源 (WEB-DL等)' },
  { id: 'stream', label: '流媒体 (NF等)' },
  { id: 'effect', label: '特效 (HDR/DV)' },
  { id: 'codec', label: '视频编码' },
  { id: 'audio_count', label: '音轨数' },
  { id: 'audio', label: '音频格式' },
  { id: 'fps', label: '帧率' },
  { id: 'group', label: '发布组' },
  { id: 'tmdb_bracket', label: 'TMDb {tmdb=ID}' },
  { id: 'tmdb_square', label: 'TMDb [tmdbid=ID]' },
  { id: 'tmdb_dash', label: 'TMDb tmdb-ID' },
  { id: 'original_name', label: '保留原名' },
  { id: 'sep_slash', label: '目录分层 ( / )', isSep: true },
  { id: 'sep_dash_space', label: '分隔符 ( - )', isSep: true },
  { id: 'sep_middot_space', label: '分隔符 ( · )', isSep: true },
  { id: 'sep_middot', label: '中圆点 (·)', isSep: true },
  { id: 'sep_dot', label: '点 (.)', isSep: true },
  { id: 'sep_dash', label: '短横线 (-)', isSep: true },
  { id: 'sep_underline', label: '下划线 (_)', isSep: true },
  { id: 'sep_space', label: '空格 ( )', isSep: true }
];

const activeTracks = ref({
  main_dir: [],
  season_dir: [],
  file: []
});

const initTrack = (formatArray) => {
  if (!formatArray) return [];
  return formatArray.map((id, index) => {
    const baseId = id.replace(/_\d+$/, '');
    const blockDef = allBlocks.find(b => b.id === baseId);
    return blockDef ? { ...blockDef, uid: `${baseId}_${index}_${Date.now()}`, originalId: id } : null;
  }).filter(Boolean);
};

watch(() => config.value, (newConf) => {
  activeTracks.value.main_dir = initTrack(newConf.main_dir_format);
  activeTracks.value.season_dir = initTrack(newConf.season_dir_format);
  activeTracks.value.file = initTrack(newConf.file_format);
}, { immediate: true });

const getAvailableBlocks = (trackName) => {
  const activeBaseIds = activeTracks.value[trackName].map(b => b.id);
  return allBlocks.filter(b => b.isSep || !activeBaseIds.includes(b.id));
};

const addBlock = (block, trackName) => {
  const track = activeTracks.value[trackName];
  const newId = block.isSep ? `${block.id}_${Date.now()}` : block.id;
  track.push({ ...block, uid: newId, originalId: newId });
  updateConfigFormat(trackName);
};

const removeBlock = (index, trackName) => {
  activeTracks.value[trackName].splice(index, 1);
  updateConfigFormat(trackName);
};

const updateConfigFormat = (trackName) => {
  const formatKey = trackName === 'file' ? 'file_format' : `${trackName}_format`;
  config.value[formatKey] = activeTracks.value[trackName].map(b => b.originalId);
};

const dragStart = (event, index, trackName) => {
  event.dataTransfer.effectAllowed = 'move';
  event.dataTransfer.setData('dragIndex', index);
  event.dataTransfer.setData('trackName', trackName);
};

const drop = (event, dropIndex, targetTrackName) => {
  const dragIndex = event.dataTransfer.getData('dragIndex');
  const sourceTrackName = event.dataTransfer.getData('trackName');
  if (dragIndex === null || sourceTrackName !== targetTrackName || dragIndex == dropIndex) return;
  
  const track = activeTracks.value[targetTrackName];
  const item = track.splice(dragIndex, 1)[0];
  track.splice(dropIndex, 0, item);
  updateConfigFormat(targetTrackName);
};

// 模拟数据
const mockMovie = { zh: '蝙蝠侠：黑暗骑士', en: 'The Dark Knight', year: '2008', tmdb: '155', res: '1080p', src: 'BluRay', codec: 'AVC', audio: 'DDP 5.1', group: 'CMCT', orig: 'The.Dark.Knight.2008.REMASTERED.1080p', ext: '.mkv' };
const mockTv = { zh: '绝命毒师', en: 'Breaking Bad', year: '2008', tmdb: '1396', s: '1', e: '1', res: '2160p', src: 'WEB-DL', stream: 'NF', effect: 'HDR', codec: 'HEVC', audio_count: '2Audios', audio: 'Atmos', fps: '60fps', group: 'HHWEB', orig: 'Breaking.Bad.S01E01.2160p.NF.WEB-DL', ext: '.mp4' };
const mockOriginalMovieDir = "The.Dark.Knight.2008.REMASTERED.1080p.BluRay.x264";
const mockOriginalMovieFile = "The.Dark.Knight.2008.REMASTERED.1080p.BluRay.x264.mkv";
const mockOriginalTvDir = "Breaking.Bad.S01.2160p.WEB-DL.x265";
const mockOriginalTvFile = "Breaking.Bad.S01E01.2160p.WEB-DL.x265.mp4";

// 统一的名称生成引擎
const buildName = (mockData, formatArray, isTv) => {
  if (config.value.keep_original_name) return ''; 
  if (!formatArray) return '';
  
  let evaluated = [];
  for (const rawId of formatArray) {
    const blockId = rawId.replace(/_\d+$/, '');
    let val = null;
    let isSep = false;
    
    if (blockId === 'title_zh') val = mockData.zh;
    else if (blockId === 'title_en') val = mockData.en;
    else if (blockId === 'year') val = `(${mockData.year})`;
    else if (blockId === 'year_pure') val = mockData.year;
    else if (blockId === 's_e' && isTv) val = `S0${mockData.s}E0${mockData.e}`;
    else if (blockId === 'season_name_en' && isTv) val = `Season 0${mockData.s}`;
    else if (blockId === 'season_name_en_no0' && isTv) val = `Season ${mockData.s}`;
    else if (blockId === 'season_name_zh' && isTv) val = `第${mockData.s}季`;
    else if (blockId === 'season_name_s' && isTv) val = `S0${mockData.s}`;
    else if (blockId === 'season_name_s_no0' && isTv) val = `S${mockData.s}`;
    else if (blockId === 'tmdb_bracket') val = `{tmdb=${mockData.tmdb}}`;
    else if (blockId === 'tmdb_square') val = `[tmdbid=${mockData.tmdb}]`;
    else if (blockId === 'tmdb_dash') val = `tmdb-${mockData.tmdb}`;
    else if (blockId === 'original_name') val = mockData.orig;
    else if (blockId === 'resolution' && mockData.res) val = mockData.res;
    else if (blockId === 'source' && mockData.src) val = mockData.src;
    else if (blockId === 'stream' && mockData.stream) val = mockData.stream;
    else if (blockId === 'effect' && mockData.effect) val = mockData.effect;
    else if (blockId === 'codec' && mockData.codec) val = mockData.codec;
    else if (blockId === 'audio_count' && mockData.audio_count) val = mockData.audio_count;
    else if (blockId === 'audio' && mockData.audio) val = mockData.audio;
    else if (blockId === 'fps' && mockData.fps) val = mockData.fps;
    else if (blockId === 'group' && mockData.group) val = mockData.group;
    else if (blockId.startsWith('sep_')) {
      isSep = true;
      if (blockId === 'sep_slash') val = '/';
      else if (blockId === 'sep_dash_space') val = ' - ';
      else if (blockId === 'sep_middot_space') val = ' · ';
      else if (blockId === 'sep_middot') val = '·';
      else if (blockId === 'sep_dot') val = '.';
      else if (blockId === 'sep_dash') val = '-';
      else if (blockId === 'sep_underline') val = '_';
      else if (blockId === 'sep_space') val = ' ';
    }
    
    if (val) evaluated.push({ val: isSep ? val : String(val).trim(), isSep });
  }
  
  let finalParts = [];
  for (let i = 0; i < evaluated.length; i++) {
    let item = evaluated[i];
    if (item.isSep) {
      let hasContentBefore = evaluated.slice(0, i).some(x => !x.isSep);
      let hasContentAfter = evaluated.slice(i + 1).some(x => !x.isSep);
      let isLastSepInGroup = i + 1 >= evaluated.length || !evaluated[i+1].isSep;
      if (hasContentBefore && hasContentAfter && isLastSepInGroup) finalParts.push(item.val);
    } else {
      finalParts.push(item.val);
    }
  }
  return finalParts.join('');
};

const previewMovieDir = computed(() => config.value.keep_original_name ? mockOriginalMovieDir : buildName(mockMovie, config.value.main_dir_format, false));
const previewTvDir = computed(() => config.value.keep_original_name ? mockOriginalTvDir : buildName(mockTv, config.value.main_dir_format, true));
const previewTvSeason = computed(() => config.value.keep_original_name ? "(保留原始子目录)" : buildName(mockTv, config.value.season_dir_format, true));
const previewMovieFile = computed(() => config.value.keep_original_name ? mockOriginalMovieFile : buildName(mockMovie, config.value.file_format, false) + mockMovie.ext);
const previewTvFile = computed(() => config.value.keep_original_name ? mockOriginalTvFile : buildName(mockTv, config.value.file_format, true) + mockTv.ext);

const previewMovieStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/abc123xyz';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewMovieFile.value}` : baseUrl;
});

const previewTvStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/def456uvw';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewTvFile.value}` : baseUrl;
});

const open = async () => {
  isVisible.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/rename_config');
    if (res.data.success) {
      // 兼容旧配置
      const data = res.data.data;
      if (!data.main_dir_format) data.main_dir_format = ['title_zh', 'sep_space', 'year', 'sep_space', 'tmdb_bracket'];
      if (!data.season_dir_format) data.season_dir_format = ['season_name_en'];
      config.value = data;
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
.config-section {
  margin-bottom: 20px;
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

.active-track {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  min-height: 40px;
  padding: 12px;
  background: var(--n-color-modal);
  border: 1px solid var(--n-divider-color);
  border-radius: 6px;
  align-items: center;
}

.track-item {
  display: flex;
  align-items: center;
  background-color: #18a058; 
  color: #ffffff; 
  padding: 4px 12px;
  border-radius: 16px;
  font-size: 13px;
  cursor: grab;
  user-select: none;
  transition: all 0.2s;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
.track-item.is-sep {
  background-color: #f0a020; 
}
.track-item:active {
  cursor: grabbing;
  transform: scale(0.95);
}
.track-item:hover {
  background-color: #d03050; 
}

.drag-handle {
  margin-right: 6px;
  cursor: grab;
  opacity: 0.7;
}

.empty-tip {
  font-size: 12px;
  color: var(--n-text-color-disabled);
  font-style: italic;
}

.list-enter-active, .list-leave-active {
  transition: all 0.3s ease;
}
.list-enter-from, .list-leave-to {
  opacity: 0;
  transform: scale(0.8);
}

.preview-container {
  background-color: var(--n-color-modal);
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
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