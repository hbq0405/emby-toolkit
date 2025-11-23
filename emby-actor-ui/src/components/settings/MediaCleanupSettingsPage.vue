<!-- src/components/settings/GeneralSettingsPage.vue -->
<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <!-- 规则说明 -->
      <n-card :bordered="false" style="background-color: transparent;">
        <template #header>
          <span style="font-size: 1.2em; font-weight: bold;">媒体去重决策规则</span>
        </template>
        <p style="margin-top: 0; color: #888;">
          当检测到重复项时，系统将按照以下规则顺序（从上到下）进行比较。<br>
          您可以拖拽调整<strong>优先规则</strong>的顺序。如果所有优先规则都无法区分优劣，将使用底部的<strong>兜底规则</strong>决定结果。
        </p>
      </n-card>

      <!-- ★★★ 第一部分：可拖拽的优先规则列表 ★★★ -->
      <draggable
        v-model="draggableRules"
        item-key="id"
        handle=".drag-handle"
        class="rules-list"
      >
        <template #item="{ element: rule }">
          <n-card class="rule-card" :key="rule.id">
            <div class="rule-content">
              <!-- 拖拽手柄 -->
              <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
              
              <div class="rule-details">
                <span class="rule-name">{{ getRuleDisplayName(rule.id) }}</span>
                <n-text :depth="3" class="rule-description">{{ getRuleDescription(rule.id) }}</n-text>
              </div>
              
              <n-space class="rule-actions" align="center">
                <!-- 保大保小切换 -->
                <n-radio-group 
                  v-if="['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate'].includes(rule.id)" 
                  v-model:value="rule.priority" 
                  size="small" 
                  style="margin-right: 12px;"
                >
                  <n-radio-button value="desc">{{ getDescLabel(rule.id) }}</n-radio-button>
                  <n-radio-button value="asc">{{ getAscLabel(rule.id) }}</n-radio-button>
                </n-radio-group>

                <!-- 编辑按钮 -->
                <n-button v-if="rule.priority && Array.isArray(rule.priority)" text @click="openEditModal(rule)">
                  <template #icon><n-icon :component="EditIcon" /></template>
                </n-button>
                
                <n-switch v-model:value="rule.enabled" />
              </n-space>
            </div>
          </n-card>
        </template>
      </draggable>

      <!-- ★★★ 第二部分：固定的兜底规则区域 ★★★ -->
      <div v-if="fallbackRule">
        <n-divider style="margin: 24px 0 12px 0; font-size: 0.9em; color: #999;">兜底策略 (固定)</n-divider>
        
        <n-card class="rule-card fallback-card">
          <div class="rule-content">
            <!-- 这里的图标换成锁，表示不可拖拽 -->
            <n-icon :component="LockIcon" size="20" style="color: #ccc; margin-right: 4px;" />
            
            <div class="rule-details">
              <span class="rule-name">{{ getRuleDisplayName(fallbackRule.id) }}</span>
              <n-text :depth="3" class="rule-description">{{ getRuleDescription(fallbackRule.id) }}</n-text>
            </div>
            
            <n-space class="rule-actions" align="center">
              <n-radio-group 
                v-model:value="fallbackRule.priority" 
                size="small" 
                style="margin-right: 12px;"
              >
                <n-radio-button value="desc">保留最新</n-radio-button>
                <n-radio-button value="asc">保留最早</n-radio-button>
              </n-radio-group>
              
              <!-- 兜底规则通常建议常开，但如果你想允许关闭也可以 -->
              <n-switch v-model:value="fallbackRule.enabled" />
            </n-space>
          </div>
        </n-card>
      </div>
      <n-divider title-placement="left" style="margin-top: 24px;">高级策略</n-divider>
      
      <n-card size="small" :bordered="false" style="background: rgba(0,0,0,0.02);">
        <n-space align="center" justify="space-between">
          <div>
            <div style="font-weight: bold;">保留每种分辨率的最佳版本</div>
            <div style="font-size: 0.9em; color: #888;">
              开启后，系统会分别计算 4K、1080p 等不同分辨率下的最佳版本并保留。<br>
              例如：同时拥有 4K Remux 和 1080p Web-DL 时，两者都会被保留，不会互相删除。
            </div>
          </div>
          <n-switch v-model:value="keepOnePerRes" />
        </n-space>
      </n-card>
      <!-- 扫描范围设置 -->
      <n-divider title-placement="left" style="margin-top: 24px;">扫描范围</n-divider>
      <n-form-item label-placement="left">
        <template #label>
          指定媒体库
          <n-tooltip trigger="hover">
            <template #trigger>
              <n-icon :component="HelpIcon" style="margin-left: 4px; cursor: help; color: #888;" />
            </template>
            留空则扫描所有电影和剧集类型的媒体库。指定后，仅扫描选中的媒体库。
          </n-tooltip>
        </template>
        <n-select
          v-model:value="selectedLibraryIds"
          multiple
          filterable
          placeholder="不选择则默认扫描所有媒体库"
          :options="allLibraries"
          :loading="isLibrariesLoading"
          clearable
        />
      </n-form-item>

      <!-- 底部按钮 -->
      <div style="display: flex; justify-content: flex-end; gap: 12px; margin-top: 16px;">
        <n-button @click="fetchSettings">重置更改</n-button>
        <n-button type="primary" @click="saveSettings" :loading="saving">保存设置</n-button>
      </div>

      <!-- 优先级编辑弹窗 -->
      <n-modal v-model:show="showEditModal" preset="card" style="width: 500px;" title="编辑优先级">
        <p style="margin-top: 0; color: #888;">
          拖拽下方的标签来调整关键字的优先级。排在越上面的关键字，代表版本越好。
        </p>
        <draggable
          v-model="currentEditingRule.priority"
          item-key="item"
          class="priority-tags-list"
        >
          <template #item="{ element: tag }">
            <n-tag class="priority-tag" type="info" size="large">{{ tag }}</n-tag>
          </template>
        </draggable>
        <template #footer>
          <n-button @click="showEditModal = false">完成</n-button>
        </template>
      </n-modal>

    </n-space>
  </n-spin>
</template>

<script setup>
import { ref, onMounted, defineEmits, computed } from 'vue';
import axios from 'axios';
import { 
  NCard, NSpace, NSwitch, NButton, useMessage, NSpin, NIcon, NModal, NTag, NText,
  NSelect, NFormItem, NDivider, NTooltip, NRadioGroup, NRadioButton
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Pencil as EditIcon, 
  Move as DragHandleIcon,
  HelpCircleOutline as HelpIcon,
  LockClosedOutline as LockIcon
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['on-close']);

// --- 状态定义 ---
const saving = ref(false);
const showEditModal = ref(false);
const keepOnePerRes = ref(false);

// 规则数据拆分
const draggableRules = ref([]); // 可排序的优先规则
const fallbackRule = ref(null); // 固定的兜底规则

const currentEditingRule = ref({ priority: [] });
const allLibraries = ref([]);
const selectedLibraryIds = ref([]);

const isRulesLoading = ref(true);
const isLibrariesLoading = ref(true);
const loading = computed(() => isRulesLoading.value || isLibrariesLoading.value);

// --- 常量定义 ---
const RULE_METADATA = {
  runtime: { name: "按时长", description: "按视频时长选择。" },
  effect: { name: "按特效", description: "比较视频的特效等级 (如 DoVi Profile 8, HDR)。" },
  resolution: { name: "按分辨率", description: "比较视频的分辨率 (如 2160p, 1080p)。" },
  bit_depth: { name: "按色深", description: "按色深选择。" },
  bitrate: { name: "按码率", description: "优先保留视频码率更高的版本 (画质更好)。" },
  quality: { name: "按质量", description: "比较文件名中的质量标签 (如 Remux, BluRay)。" },
  frame_rate: { name: "按帧率", description: "优先保留高帧率版本 (如 60fps > 24fps)。" },
  filesize: { name: "按文件大小", description: "如果以上规则都无法区分，则保留文件体积更大的版本。" },
  codec: { name: "按编码", description: "比较视频编码格式 (如 AV1, HEVC, H.264)。" },
  date_added: { name: "按入库时间", description: "最终兜底规则。根据入库时间（或ID大小）决定去留。" }
};

// --- 辅助函数 ---

const getRuleDisplayName = (id) => RULE_METADATA[id]?.name || id;
const getRuleDescription = (id) => RULE_METADATA[id]?.description || '未知规则';

// 获取“降序”按钮的文案 (desc)
const getDescLabel = (id) => {
  switch (id) {
    case 'filesize': return '保留最大';
    case 'runtime': return '保留最长';
    case 'bitrate': return '保留最高';
    case 'bit_depth': return '保留高位';
    case 'frame_rate': return '保留高帧';
    default: return '保留大/高';
  }
};

// 获取“升序”按钮的文案 (asc)
const getAscLabel = (id) => {
  switch (id) {
    case 'filesize': return '保留最小';
    case 'runtime': return '保留最短';
    case 'bitrate': return '保留最低';
    case 'bit_depth': return '保留低位';
    case 'frame_rate': return '保留低帧';
    default: return '保留小/低';
  }
};

// 特效优先级格式化
const formatEffectPriority = (priorityArray, to = 'display') => {
    return priorityArray.map(p => {
        let p_lower = String(p).toLowerCase().replace(/\s/g, '_');
        if (p_lower === 'dovi' || p_lower === 'dovi_other' || p_lower === 'dovi(other)') {
            p_lower = 'dovi_other';
        }
        if (to === 'display') {
            if (p_lower === 'dovi_p8') return 'DoVi P8';
            if (p_lower === 'dovi_p7') return 'DoVi P7';
            if (p_lower === 'dovi_p5') return 'DoVi P5';
            if (p_lower === 'dovi_other') return 'DoVi (Other)';
            if (p_lower === 'hdr10+') return 'HDR10+';
            return p_lower.toUpperCase();
        } else {
            return p_lower;
        }
    });
};

// --- 核心逻辑 ---

const fetchSettings = async () => {
  isRulesLoading.value = true;
  isLibrariesLoading.value = true;
  try {
    const [settingsRes, librariesRes] = await Promise.all([
      axios.get('/api/cleanup/settings'),
      axios.get('/api/resubscribe/libraries') 
    ]);

    // 1. 处理规则数据
    let loadedRules = settingsRes.data.rules || [];

    keepOnePerRes.value = settingsRes.data.keep_one_per_res || false;
    
    // 预处理：格式化特效优先级，初始化默认 priority
    loadedRules = loadedRules.map(rule => {
        // 处理特效显示的格式化
        if (rule.id === 'effect' && Array.isArray(rule.priority)) {
            return { ...rule, priority: formatEffectPriority(rule.priority, 'display') };
        }
        // 为数值型规则设置默认排序方向
        const numericRules = ['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate', 'date_added'];
        if (numericRules.includes(rule.id) && !rule.priority) {
            // date_added 默认为 asc (保留最早)，其他默认为 desc (保留最大)
            return { ...rule, priority: rule.id === 'date_added' ? 'asc' : 'desc' };
        }
        return rule;
    });

    // 拆分规则：提取 date_added 到 fallbackRule，其余到 draggableRules
    const foundFallback = loadedRules.find(r => r.id === 'date_added');
    if (foundFallback) {
        fallbackRule.value = foundFallback;
    } else {
        // 如果后端没返回，手动创建一个默认的
        fallbackRule.value = { id: 'date_added', enabled: true, priority: 'asc' };
    }
    draggableRules.value = loadedRules.filter(r => r.id !== 'date_added');

    // 2. 处理媒体库数据
    allLibraries.value = librariesRes.data || [];
    selectedLibraryIds.value = settingsRes.data.library_ids || [];

  } catch (error) {
    message.error('加载设置失败！请确保后端服务正常。');
    // 加载失败时的默认兜底配置
    const defaultRules = [
        { id: 'runtime', enabled: true, priority: 'desc' },
        { id: 'effect', enabled: true, priority: ['DoVi P8', 'DoVi P7', 'DoVi P5', 'DoVi (Other)', 'HDR10+', 'HDR', 'SDR'] },
        { id: 'resolution', enabled: true, priority: ['4K', '1080p', '720p', '480p'] },
        { id: 'bit_depth', enabled: true, priority: 'desc' },
        { id: 'bitrate', enabled: true, priority: 'desc' },
        { id: 'codec', enabled: true, priority: ['AV1', 'HEVC', 'H.264', 'VP9'] },
        { id: 'quality', enabled: true, priority: ['Remux', 'BluRay', 'WEB-DL', 'HDTV'] },
        { id: 'frame_rate', enabled: false, priority: 'desc' },
        { id: 'filesize', enabled: true, priority: 'desc' },
    ];
    draggableRules.value = defaultRules;
    fallbackRule.value = { id: 'date_added', enabled: true, priority: 'asc' };
  } finally {
    isRulesLoading.value = false;
    isLibrariesLoading.value = false;
  }
};

const saveSettings = async () => {
  saving.value = true;
  try {
    // 合并规则：draggableRules + fallbackRule
    const allRules = [...draggableRules.value];
    if (fallbackRule.value) {
        allRules.push(fallbackRule.value);
    }

    // 格式化处理：将特效优先级转回保存格式
    const rulesToSave = allRules.map(rule => {
      if (rule.id === 'effect' && Array.isArray(rule.priority)) {
        return { ...rule, priority: formatEffectPriority(rule.priority, 'save') };
      }
      return rule;
    });

    const payload = {
      rules: rulesToSave,
      library_ids: selectedLibraryIds.value,
      keep_one_per_res: keepOnePerRes.value
    };

    await axios.post('/api/cleanup/settings', payload);
    message.success('清理设置已成功保存！');
    
    emit('on-close');

  } catch (error) {
    message.error('保存设置失败，请检查后端日志。');
  } finally {
    saving.value = false;
  }
};

const openEditModal = (rule) => {
  currentEditingRule.value = rule;
  showEditModal.value = true;
};

onMounted(fetchSettings);
</script>

<style scoped>
.rules-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.rule-card {
  cursor: pointer;
  transition: box-shadow 0.2s;
}
/* 给兜底规则卡片一点特殊样式，比如背景色稍微不同，或者边框 */
.fallback-card {
  cursor: default; /* 兜底规则不可点击/拖拽 */
  background-color: rgba(0, 0, 0, 0.02); /* 极淡的背景色区分 */
  border: 1px dashed var(--n-border-color); /* 虚线边框表示特殊区域 */
}
.rule-content {
  display: flex;
  align-items: center;
  gap: 16px;
}
.drag-handle {
  cursor: grab;
  color: #888;
}
.rule-details {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
}
.rule-name {
  font-weight: bold;
}
.rule-description {
  font-size: 0.9em;
}
.rule-actions {
  margin-left: auto;
}
.priority-tags-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  background-color: var(--n-color-embedded);
  padding: 12px;
  border-radius: 8px;
}
.priority-tag {
  cursor: grab;
  width: 100%;
  justify-content: center;
}
</style>