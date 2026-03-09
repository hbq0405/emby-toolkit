<template>
  <n-modal v-model:show="isVisible" preset="card" title="自定义重命名规则" style="width: 900px; max-width: 95%;">
    <n-spin :show="loading">
      
      <!-- 顶部：配置区域 -->
      <div class="config-section">
        <n-tabs type="segment" animated size="small">
          
          <!-- 标签页 1：文件命名 (乐高轨道) -->
          <n-tab-pane name="file" tab="文件命名 (拖拽排序)">
            <div class="lego-container">
              <div class="lego-header">
                <span>📦 备选模块 (点击添加到轨道)</span>
              </div>
              
              <!-- 备选池 -->
              <div class="block-pool">
                <n-tag 
                  v-for="block in availableBlocks" 
                  :key="block.id" 
                  :type="block.isSep ? 'warning' : 'info'" 
                  class="lego-block"
                  @click="addBlock(block)"
                >
                  + {{ block.label }}
                </n-tag>
              </div>

              <div class="lego-header" style="margin-top: 16px;">
                <span>🛤️ 当前命名轨道 (拖拽排序，点击移除)</span>
              </div>

              <!-- 激活轨道 (原生拖拽) -->
              <div class="active-track">
                <transition-group name="list">
                  <div 
                    v-for="(block, index) in activeBlocks" 
                    :key="block.uid"
                    :class="['track-item', block.isSep ? 'is-sep' : '']"
                    draggable="true"
                    @dragstart="dragStart($event, index)"
                    @dragover.prevent
                    @drop="drop($event, index)"
                    @click="removeBlock(index)"
                  >
                    <n-icon size="14" class="drag-handle"><MenuIcon /></n-icon>
                    {{ block.label }}
                  </div>
                </transition-group>
                <div v-if="activeBlocks.length === 0" class="empty-tip">轨道为空，请从上方添加模块</div>
              </div>
            </div>
          </n-tab-pane>

          <!-- 标签页 2：目录命名 -->
          <n-tab-pane name="dir" tab="目录命名">
            <n-form-item label="保留原名" label-placement="left" style="margin-bottom: 12px; background: rgba(24, 160, 88, 0.05); padding: 8px 12px; border-radius: 6px;">
              <n-switch v-model:value="config.keep_original_name" />
              <template #feedback>
                <span style="font-size: 12px; color: gray;">开启后仅做归类，原目录架构和文件名原封不动 (覆盖下方所有规则)</span>
              </template>
            </n-form-item>
            <n-form inline label-placement="left" size="small" style="margin-top: 10px;">
              <n-form-item label="主目录语言">
                <n-radio-group v-model:value="config.main_title_lang">
                  <n-radio-button value="zh">中文</n-radio-button>
                  <n-radio-button value="original">原文</n-radio-button>
                </n-radio-group>
              </n-form-item>
              <n-form-item label="附加年份">
                <n-switch v-model:value="config.main_year_en" />
              </n-form-item>
              <n-form-item label="TMDb 标签">
                <n-select v-model:value="config.main_tmdb_fmt" :options="tmdbOptions" style="width: 150px;" />
              </n-form-item>
              <n-form-item label="季目录格式">
                <n-select v-model:value="config.season_fmt" :options="seasonOptions" style="width: 150px;" />
              </n-form-item>
            </n-form>
          </n-tab-pane>

          <!-- 标签页 3：高级设置 -->
          <n-tab-pane name="adv" tab="高级设置">
            <n-form label-placement="left" size="small" style="margin-top: 16px;">
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
import { NModal, NGrid, NGi, NTabs, NTabPane, NForm, NFormItem, NRadioGroup, NRadioButton, NSwitch, NSelect, NSpace, NButton, NIcon, NSpin, NTag, useMessage, NRadio } from 'naive-ui';
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
  main_title_lang: 'zh',
  main_year_en: true,
  main_tmdb_fmt: '{tmdb=ID}',
  season_fmt: 'Season {02}',
  file_format: ['title_zh', 'sep_dash_space', 'year', 'sep_middot_space', 's_e', 'sep_middot_space', 'resolution', 'sep_middot_space', 'codec', 'sep_middot_space', 'audio_count', 'sep_space', 'audio', 'sep_middot_space', 'group'],
  file_tmdb_fmt: 'none',
  strm_url_fmt: 'standard'
});

// 乐高模块定义 (加入无限量供应的连接符)
const allBlocks = [
  { id: 'title_zh', label: '中文片名' },
  { id: 'title_en', label: '英文/原名' },
  { id: 'year', label: '年份' },
  { id: 's_e', label: '季集号 (S01E01)' },
  { id: 'resolution', label: '分辨率' },
  { id: 'source', label: '来源 (WEB-DL等)' },
  { id: 'stream', label: '流媒体 (NF/AMZN等)' },
  { id: 'effect', label: '特效 (HDR/DV)' },
  { id: 'codec', label: '视频编码' },
  { id: 'audio_count', label: '音轨数 (2Audios)' },
  { id: 'audio', label: '音频格式 (DTS/AC3)' },
  { id: 'fps', label: '帧率 (fps)' },
  { id: 'group', label: '发布组' },
  { id: 'tmdb', label: 'TMDb标签' },
  { id: 'original_name', label: '原文件名(保留原名)' },
  { id: 'sep_dash_space', label: '分隔符 ( - )', isSep: true },
  { id: 'sep_middot_space', label: '分隔符 ( · )', isSep: true },
  { id: 'sep_middot', label: '中圆点 (·)', isSep: true },
  { id: 'sep_dot', label: '点 (.)', isSep: true },
  { id: 'sep_dash', label: '短横线 (-)', isSep: true },
  { id: 'sep_underline', label: '下划线 (_)', isSep: true },
  { id: 'sep_space', label: '空格 ( )', isSep: true }
];

const activeBlocks = ref([]);

// 备选池：普通模块用过就隐藏，连接符永远显示
const availableBlocks = computed(() => {
  const activeBaseIds = activeBlocks.value.map(b => b.id);
  return allBlocks.filter(b => b.isSep || !activeBaseIds.includes(b.id));
});

// 监听 config 初始化轨道
watch(() => config.value.file_format, (newFormat) => {
  if (newFormat && newFormat.length > 0) {
    activeBlocks.value = newFormat.map((id, index) => {
      // 处理带有唯一后缀的连接符 ID
      const baseId = id.replace(/_\d+$/, '');
      const blockDef = allBlocks.find(b => b.id === baseId);
      if (blockDef) {
        return { ...blockDef, uid: `${baseId}_${index}_${Date.now()}`, originalId: id };
      }
      return null;
    }).filter(Boolean);
  } else {
    activeBlocks.value = [];
  }
}, { immediate: true });

// 乐高交互方法
const addBlock = (block) => {
  if (block.id === 'original_name') {
    activeBlocks.value = [{ ...block, uid: `orig_${Date.now()}`, originalId: block.id }];
  } else {
    const origIndex = activeBlocks.value.findIndex(b => b.id === 'original_name');
    if (origIndex !== -1) activeBlocks.value.splice(origIndex, 1);
    
    // 如果是连接符，生成唯一 ID
    const newId = block.isSep ? `${block.id}_${Date.now()}` : block.id;
    activeBlocks.value.push({ ...block, uid: newId, originalId: newId });
  }
  updateConfigFormat();
};

const removeBlock = (index) => {
  activeBlocks.value.splice(index, 1);
  updateConfigFormat();
};

const updateConfigFormat = () => {
  config.value.file_format = activeBlocks.value.map(b => b.originalId);
};

// 原生拖拽排序
const dragStart = (event, index) => {
  event.dataTransfer.effectAllowed = 'move';
  event.dataTransfer.setData('dragIndex', index);
};

const drop = (event, dropIndex) => {
  const dragIndex = event.dataTransfer.getData('dragIndex');
  if (dragIndex === null || dragIndex === dropIndex) return;
  
  const item = activeBlocks.value.splice(dragIndex, 1)[0];
  activeBlocks.value.splice(dropIndex, 0, item);
  updateConfigFormat();
};

// 选项字典
const tmdbOptions = [
  { label: '不添加', value: 'none' },
  { label: '{tmdb=ID}', value: '{tmdb=ID}' },
  { label: '[tmdbid=ID]', value: '[tmdbid=ID]' },
  { label: 'tmdb-ID', value: 'tmdb-ID' }
];

const seasonOptions = [
  { label: 'Season 01', value: 'Season {02}' },
  { label: 'Season 1', value: 'Season {1}' },
  { label: 'S01', value: 'S{02}' },
  { label: 'S1', value: 'S{1}' },
  { label: '第1季', value: '第{1}季' }
];

const strmUrlOptions = [
  { label: '标准格式 (/api/p115/play/xxx)', value: 'standard' },
  { label: '带文件名后缀 (/api/p115/play/xxx/文件名.mkv)', value: 'with_name' }
];

// 模拟数据
const mockMovie = { zh: '蝙蝠侠：黑暗骑士', en: 'The Dark Knight', year: '2008', tmdb: '155', res: '1080p', src: 'BluRay', codec: 'H264', audio: 'DDP 5.1', group: 'CMCT', orig: 'The.Dark.Knight.2008.REMASTERED.1080p', ext: '.mkv' };
const mockTv = { zh: '绝命毒师', en: 'Breaking Bad', year: '2008', tmdb: '1396', s: '1', e: '1', res: '2160p', src: 'WEB-DL', stream: 'NF', effect: 'HDR', codec: 'H265', audio_count: '2Audios', audio: 'Atmos', fps: '60fps', group: 'HHWEB', orig: 'Breaking.Bad.S01E01.2160p.NF.WEB-DL', ext: '.mp4' };
const mockOriginalMovieDir = "The.Dark.Knight.2008.REMASTERED.1080p.BluRay.x264";
const mockOriginalMovieFile = "The.Dark.Knight.2008.REMASTERED.1080p.BluRay.x264.mkv";
const mockOriginalTvDir = "Breaking.Bad.S01.2160p.WEB-DL.x265";
const mockOriginalTvFile = "Breaking.Bad.S01E01.2160p.WEB-DL.x265.mp4";

// 预览计算
const previewMovieStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/abc123xyz';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewMovieFile.value}` : baseUrl;
});

const previewTvStrm = computed(() => {
  const baseUrl = 'http://127.0.0.1:5257/api/p115/play/def456uvw';
  return config.value.strm_url_fmt === 'with_name' ? `${baseUrl}/${previewTvFile.value}` : baseUrl;
});

const previewMovieDir = computed(() => {
  if (config.value.keep_original_name) return mockOriginalMovieDir;
  let name = config.value.main_title_lang === 'zh' ? mockMovie.zh : mockMovie.en;
  if (config.value.main_year_en) name += ` (${mockMovie.year})`;
  if (config.value.main_tmdb_fmt !== 'none') name += ` ${config.value.main_tmdb_fmt.replace('ID', mockMovie.tmdb)}`;
  return name;
});

const previewTvDir = computed(() => {
  if (config.value.keep_original_name) return mockOriginalTvDir;
  let name = config.value.main_title_lang === 'zh' ? mockTv.zh : mockTv.en;
  if (config.value.main_year_en) name += ` (${mockTv.year})`;
  if (config.value.main_tmdb_fmt !== 'none') name += ` ${config.value.main_tmdb_fmt.replace('ID', mockTv.tmdb)}`;
  return name;
});

const previewTvSeason = computed(() => {
  if (config.value.keep_original_name) return "(保留原始子目录)";
  return config.value.season_fmt.replace('{02}', '01').replace('{1}', '1');
});

// 前端智能消除算法 (与后端 Python 逻辑完全一致)
const buildFileName = (mockData, isTv) => {
  if (config.value.keep_original_name) return isTv ? mockOriginalTvFile : mockOriginalMovieFile;

  let evaluated = [];
  for (const rawId of config.value.file_format) {
    const blockId = rawId.replace(/_\d+$/, ''); // 去除后缀
    let val = null;
    let isSep = false;
    
    if (blockId === 'title_zh') val = mockData.zh;
    else if (blockId === 'title_en') val = mockData.en;
    else if (blockId === 'year') val = `(${mockData.year})`;
    else if (blockId === 'tmdb' && config.value.file_tmdb_fmt !== 'none') val = config.value.file_tmdb_fmt.replace('ID', mockData.tmdb);
    else if (blockId === 's_e' && isTv) val = `S0${mockData.s}E0${mockData.e}`;
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
      if (blockId === 'sep_dash_space') val = ' - ';
      else if (blockId === 'sep_middot_space') val = ' · ';
      else if (blockId === 'sep_middot') val = '·';
      else if (blockId === 'sep_dot') val = '.';
      else if (blockId === 'sep_dash') val = '-';
      else if (blockId === 'sep_underline') val = '_';
      else if (blockId === 'sep_space') val = ' ';
    }
    
    if (val) {
      evaluated.push({ val: isSep ? val : String(val).trim(), isSep });
    }
  }
  
  let finalParts = [];
  for (let i = 0; i < evaluated.length; i++) {
    let item = evaluated[i];
    if (item.isSep) {
      let hasContentBefore = evaluated.slice(0, i).some(x => !x.isSep);
      let hasContentAfter = evaluated.slice(i + 1).some(x => !x.isSep);
      // 检查后面紧挨着的是不是也是连接符 (如果是连续连接符，只保留最后一个)
      let isLastSepInGroup = true;
      if (i + 1 < evaluated.length && evaluated[i+1].isSep) {
        isLastSepInGroup = false;
      }
      
      if (hasContentBefore && hasContentAfter && isLastSepInGroup) {
        finalParts.push(item.val);
      }
    } else {
      finalParts.push(item.val);
    }
  }
  
  return finalParts.join('') + mockData.ext;
};

const previewMovieFile = computed(() => buildFileName(mockMovie, false));
const previewTvFile = computed(() => buildFileName(mockTv, true));

// API 交互
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

/* 强制焊死颜色，无视亮色/暗色主题 */
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
  background-color: #f0a020; /* 连接符用橙色区分 */
}
.track-item:active {
  cursor: grabbing;
  transform: scale(0.95);
}
.track-item:hover {
  background-color: #d03050; /* 悬停变红提示可删除 */
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

/* 列表过渡动画 */
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