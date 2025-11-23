<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <!-- 规则说明 -->
      <n-card :bordered="false" style="background-color: transparent;">
        <template #header>
          <span style="font-size: 1.2em; font-weight: bold;">媒体去重决策规则</span>
        </template>
        <p style="margin-top: 0; color: #888;">
          当检测到重复项时，系统将按照以下规则顺序（从上到下）进行比较，以决定保留哪个版本。<br>
          拖拽规则可以调整优先级。第一个能区分出优劣的规则将决定结果。
        </p>
      </n-card>

      <!-- 规则列表 -->
      <draggable
        v-model="rules"
        item-key="id"
        handle=".drag-handle"
        class="rules-list"
      >
        <template #item="{ element: rule }">
          <n-card class="rule-card" :key="rule.id">
            <div class="rule-content">
              <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
              <div class="rule-details">
                <span class="rule-name">{{ getRuleDisplayName(rule.id) }}</span>
                <n-text :depth="3" class="rule-description">{{ getRuleDescription(rule.id) }}</n-text>
              </div>
              <n-space class="rule-actions">
                <n-radio-group 
                  v-if="['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate'].includes(rule.id)" 
                  v-model:value="rule.priority" 
                  size="small" 
                  style="margin-right: 12px;"
                >
                  <n-radio-button value="desc">{{ getDescLabel(rule.id) }}</n-radio-button>
                  <n-radio-button value="asc">{{ getAscLabel(rule.id) }}</n-radio-button>
                </n-radio-group>
                <!-- 只有拥有 priority 属性的规则才显示编辑按钮 -->
                <n-button v-if="rule.priority && Array.isArray(rule.priority)" text @click="openEditModal(rule)">
                  <template #icon><n-icon :component="EditIcon" /></template>
                </n-button>
                <n-switch v-model:value="rule.enabled" />
              </n-space>
            </div>
          </n-card>
        </template>
      </draggable>

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
  NSelect, NFormItem, NDivider, NTooltip
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Pencil as EditIcon, 
  Move as DragHandleIcon,
  HelpCircleOutline as HelpIcon 
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['on-close']);

// --- 状态定义 ---
const saving = ref(false);
const showEditModal = ref(false);
const rules = ref([]);
const currentEditingRule = ref({ priority: [] });
const allLibraries = ref([]);
const selectedLibraryIds = ref([]);

const isRulesLoading = ref(true);
const isLibrariesLoading = ref(true);
const loading = computed(() => isRulesLoading.value || isLibrariesLoading.value);

const RULE_METADATA = {
  runtime: { name: "按时长", description: "根据视频时长选择。" },
  effect: { name: "按特效", description: "比较视频的特效等级 (如 DoVi Profile 8, HDR)。" },
  resolution: { name: "按分辨率", description: "比较视频的分辨率 (如 2160p, 1080p)。" },
  bit_depth: { name: "按色深", description: "优先保留 10bit/12bit 版本，减少色彩断层。" },
  bitrate: { name: "按码率", description: "根据码率大小选择。" },
  quality: { name: "按质量", description: "比较文件名中的质量标签 (如 Remux, BluRay)。" },
  frame_rate: { name: "按帧率", description: "根据帧率版本选择。" },
  filesize: { name: "按文件大小", description: "根据视频文件大小选择。" }
};

const getRuleDisplayName = (id) => RULE_METADATA[id]?.name || id;
const getRuleDescription = (id) => RULE_METADATA[id]?.description || '未知规则';

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

// ★★★ 新增：根据规则ID返回人性化的按钮文案 ★★★
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

const fetchSettings = async () => {
  isRulesLoading.value = true;
  isLibrariesLoading.value = true;
  try {
    const [settingsRes, librariesRes] = await Promise.all([
      axios.get('/api/cleanup/settings'),
      axios.get('/api/resubscribe/libraries') 
    ]);

    // 处理规则
    const loadedRules = settingsRes.data.rules || [];
    rules.value = loadedRules.map(rule => {
        if (rule.id === 'effect' && Array.isArray(rule.priority)) {
            return { ...rule, priority: formatEffectPriority(rule.priority, 'display') };
        }
        
        const numericRules = ['runtime', 'filesize', 'bitrate', 'bit_depth', 'frame_rate'];
        if (numericRules.includes(rule.id) && !rule.priority) {
            return { ...rule, priority: 'desc' }; // 默认为降序（保大）
        }
        return rule;
    });

    // 处理媒体库
    allLibraries.value = librariesRes.data || [];
    selectedLibraryIds.value = settingsRes.data.library_ids || [];

  } catch (error) {
    message.error('加载设置失败！请确保后端服务正常。');
    // ★★★ 核心修改 2: 更新默认规则列表，包含新规则 ★★★
    rules.value = [
        { id: 'runtime', enabled: true, priority: 'desc' },
        { id: 'effect', enabled: true, priority: ['DoVi P8', 'DoVi P7', 'DoVi P5', 'DoVi (Other)', 'HDR10+', 'HDR', 'SDR'] },
        { id: 'resolution', enabled: true, priority: ['4K', '1080p', '720p', '480p'] },
        { id: 'bit_depth', enabled: true, priority: 'desc' },
        { id: 'bitrate', enabled: true, priority: 'desc' },
        { id: 'quality', enabled: true, priority: ['Remux', 'BluRay', 'WEB-DL', 'HDTV'] },
        { id: 'frame_rate', enabled: false, priority: 'desc' },
        { id: 'filesize', enabled: true, priority: 'desc' },
    ];
  } finally {
    isRulesLoading.value = false;
    isLibrariesLoading.value = false;
  }
};

const saveSettings = async () => {
  saving.value = true;
  try {
    const rulesToSave = rules.value.map(rule => {
      if (rule.id === 'effect' && Array.isArray(rule.priority)) {
        return { ...rule, priority: formatEffectPriority(rule.priority, 'save') };
      }
      return rule;
    });

    const payload = {
      rules: rulesToSave,
      library_ids: selectedLibraryIds.value
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