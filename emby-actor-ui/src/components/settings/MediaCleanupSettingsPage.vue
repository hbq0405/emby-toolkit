<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <!-- 头部说明 -->
      <n-card :bordered="false" style="background-color: transparent;">
        <template #header>
          <span style="font-size: 1.2em; font-weight: bold;">媒体去重决策规则</span>
        </template>
        <p style="margin-top: 0; color: #888;">
          当检测到重复项时，系统将按照以下规则顺序（从上到下）进行比较。<br>
          您可以拖拽调整<strong>优先规则</strong>的顺序。如果所有优先规则都无法区分优劣，将使用底部的<strong>兜底规则</strong>决定结果。
        </p>
      </n-card>

      <!-- ★★★ 第一部分：可拖拽的优先规则列表 (UI重构) ★★★ -->
      <draggable
        v-model="draggableRules"
        item-key="id"
        handle=".drag-handle"
        class="rules-list"
      >
        <template #item="{ element: rule }">
          <n-card class="rule-card" :key="rule.id" size="small">
            <div class="rule-content">
              <!-- 左侧：拖拽手柄 + 信息 -->
              <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
              
              <div class="rule-details">
                <div class="rule-name">{{ getRuleDisplayName(rule.id) }}</div>
                <div class="rule-description">{{ getRuleDescription(rule.id) }}</div>
              </div>
              
              <!-- 右侧：操作控件 -->
              <div class="rule-actions">
                <!-- 排序切换 (仅部分规则显示) -->
                <n-radio-group 
                  v-if="['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate'].includes(rule.id)" 
                  v-model:value="rule.priority" 
                  size="small" 
                  class="action-item"
                >
                  <n-radio-button value="desc">{{ getDescLabel(rule.id) }}</n-radio-button>
                  <n-radio-button value="asc">{{ getAscLabel(rule.id) }}</n-radio-button>
                </n-radio-group>

                <!-- 编辑按钮 (仅列表类规则显示) -->
                <n-button 
                  v-if="rule.priority && Array.isArray(rule.priority)" 
                  text 
                  class="action-item"
                  @click="openEditModal(rule)"
                >
                  <template #icon><n-icon :component="EditIcon" size="18" /></template>
                  编辑优先级
                </n-button>
                
                <!-- 开关 -->
                <n-switch v-model:value="rule.enabled" size="small" class="action-item">
                   <template #checked>启用</template>
                   <template #unchecked>禁用</template>
                </n-switch>
              </div>
            </div>
          </n-card>
        </template>
      </draggable>

      <!-- ★★★ 第二部分：固定的兜底规则区域 (UI重构) ★★★ -->
      <div v-if="fallbackRule">
        <n-divider style="margin: 24px 0 12px 0; font-size: 0.9em; color: #999;">兜底策略 (固定)</n-divider>
        
        <n-card class="rule-card fallback-card" size="small">
          <div class="rule-content">
            <n-icon :component="LockIcon" size="20" style="color: #ccc;" />
            
            <div class="rule-details">
              <div class="rule-name">{{ getRuleDisplayName(fallbackRule.id) }}</div>
              <div class="rule-description">{{ getRuleDescription(fallbackRule.id) }}</div>
            </div>
            
            <div class="rule-actions">
              <n-radio-group 
                v-model:value="fallbackRule.priority" 
                size="small" 
                class="action-item"
              >
                <n-radio-button value="desc">保留最新</n-radio-button>
                <n-radio-button value="asc">保留最早</n-radio-button>
              </n-radio-group>
              <n-switch v-model:value="fallbackRule.enabled" size="small" class="action-item" />
            </div>
          </div>
        </n-card>
      </div>
      
      <!-- ★★★ 第三部分：高级策略 & 删除策略 ★★★ -->
      <n-grid :x-gap="24" :y-gap="24" :cols="1" responsive="screen" item-responsive>
        <n-gi span="1">
           <n-divider title-placement="left" style="margin-top: 24px;">高级策略</n-divider>
           <n-card size="small" :bordered="false" style="background: rgba(0,0,0,0.02);">
            <n-space vertical size="large">
              
              <!-- 保留每种分辨率 -->
              <div class="setting-row">
                <div class="setting-info">
                  <div class="setting-title">保留每种分辨率的最佳版本</div>
                  <div class="setting-desc">
                    开启后，系统会分别计算 4K、1080p 等不同分辨率下的最佳版本并保留。<br>
                    例如：同时拥有 4K Remux 和 1080p Web-DL 时，两者都会被保留，不会互相删除。
                  </div>
                </div>
                <n-switch v-model:value="keepOnePerRes" />
              </div>

              <n-divider style="margin: 0" />

              <!-- ★★★ 新增：删除策略 (针对网盘用户) ★★★ -->
              <div class="setting-row">
                <div class="setting-info">
                  <div class="setting-title">
                    删除间隔延迟
                    <n-tag type="warning" size="small" :bordered="false" style="margin-left: 8px;">网盘防风控</n-tag>
                  </div>
                  <div class="setting-desc">
                    在执行批量去重时，每删除一个文件后等待的时间（秒）。<br>
                    建议网盘用户设置为 <b>5-10 秒</b>，以防止因并发删除请求过多触发 API 限制。
                  </div>
                </div>
                <n-input-number v-model:value="deleteDelay" :min="0" :step="1" style="width: 120px;">
                   <template #suffix>秒</template>
                </n-input-number>
              </div>

            </n-space>
          </n-card>
        </n-gi>
      </n-grid>

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

      <div style="display: flex; justify-content: flex-end; gap: 12px; margin-top: 16px;">
        <n-button @click="fetchSettings">重置更改</n-button>
        <n-button type="primary" @click="saveSettings" :loading="saving">保存设置</n-button>
      </div>

      <!-- 编辑优先级弹窗 -->
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
  NSelect, NFormItem, NDivider, NTooltip, NRadioGroup, NRadioButton, NGrid, NGi, NInputNumber
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

const saving = ref(false);
const showEditModal = ref(false);
const keepOnePerRes = ref(false);
const deleteDelay = ref(0); // 新增：删除延迟
const draggableRules = ref([]);
const fallbackRule = ref(null);
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
  bitrate: { name: "按码率", description: "按码率选择。" },
  quality: { name: "按质量", description: "比较文件名中的质量标签 (如 Remux, BluRay)。" },
  frame_rate: { name: "按帧率", description: "按帧率选择。" },
  filesize: { name: "按文件大小", description: "按视频大小选择。" },
  codec: { name: "按编码", description: "比较视频编码格式 (如 AV1, HEVC, H.264)。" },
  date_added: { name: "按入库时间", description: "最终兜底规则。根据入库时间（或ID大小）决定去留。" },
  subtitle: { name: "按字幕", description: "优先保留包含中文字幕的版本。" }
};

const getRuleDisplayName = (id) => RULE_METADATA[id]?.name || id;
const getRuleDescription = (id) => RULE_METADATA[id]?.description || '未知规则';

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

const fetchSettings = async () => {
  isRulesLoading.value = true;
  isLibrariesLoading.value = true;
  try {
    const [settingsRes, librariesRes] = await Promise.all([
      axios.get('/api/cleanup/settings'),
      axios.get('/api/resubscribe/libraries') 
    ]);

    let loadedRules = settingsRes.data.rules || [];
    keepOnePerRes.value = settingsRes.data.keep_one_per_res || false;
    deleteDelay.value = settingsRes.data.delete_delay || 0; // 加载删除延迟
    
    loadedRules = loadedRules.map(rule => {
        if (rule.id === 'effect' && Array.isArray(rule.priority)) {
            return { ...rule, priority: formatEffectPriority(rule.priority, 'display') };
        }
        const numericRules = ['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate', 'date_added'];
        if (numericRules.includes(rule.id) && !rule.priority) {
            return { ...rule, priority: rule.id === 'date_added' ? 'asc' : 'desc' };
        }
        return rule;
    });

    const foundFallback = loadedRules.find(r => r.id === 'date_added');
    if (foundFallback) {
        fallbackRule.value = foundFallback;
    } else {
        fallbackRule.value = { id: 'date_added', enabled: true, priority: 'asc' };
    }
    draggableRules.value = loadedRules.filter(r => r.id !== 'date_added');

    allLibraries.value = librariesRes.data || [];
    selectedLibraryIds.value = settingsRes.data.library_ids || [];

  } catch (error) {
    message.error('加载设置失败！请确保后端服务正常。');
    // ... (默认规则逻辑保持不变)
  } finally {
    isRulesLoading.value = false;
    isLibrariesLoading.value = false;
  }
};

const saveSettings = async () => {
  saving.value = true;
  try {
    const allRules = [...draggableRules.value];
    if (fallbackRule.value) {
        allRules.push(fallbackRule.value);
    }

    const rulesToSave = allRules.map(rule => {
      if (rule.id === 'effect' && Array.isArray(rule.priority)) {
        return { ...rule, priority: formatEffectPriority(rule.priority, 'save') };
      }
      return rule;
    });

    const payload = {
      rules: rulesToSave,
      library_ids: selectedLibraryIds.value,
      keep_one_per_res: keepOnePerRes.value,
      delete_delay: deleteDelay.value // 保存删除延迟
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
  cursor: move; /* 整个卡片可拖拽 */
  transition: box-shadow 0.2s, transform 0.2s;
}
.rule-card:hover {
  box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}

.fallback-card {
  cursor: default;
  background-color: rgba(0, 0, 0, 0.02);
  border: 1px dashed var(--n-border-color);
}

/* 核心布局：Flex 左右分布 */
.rule-content {
  display: flex;
  align-items: center;
  gap: 16px;
  width: 100%;
}

.drag-handle {
  cursor: grab;
  color: #bbb;
  flex-shrink: 0;
}
.rule-card:active .drag-handle {
  cursor: grabbing;
}

.rule-details {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  min-width: 0; /* 防止文本溢出 */
}

.rule-name {
  font-weight: bold;
  font-size: 1.05em;
  margin-bottom: 2px;
}

.rule-description {
  font-size: 0.85em;
  color: #888;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.rule-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-shrink: 0;
}

.action-item {
  flex-shrink: 0;
}

/* 设置行样式 */
.setting-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 24px;
}
.setting-info {
  flex: 1;
}
.setting-title {
  font-weight: 500;
  margin-bottom: 4px;
  display: flex;
  align-items: center;
}
.setting-desc {
  font-size: 0.9em;
  color: #888;
  line-height: 1.4;
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