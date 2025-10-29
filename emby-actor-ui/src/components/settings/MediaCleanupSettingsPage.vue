<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <n-card :bordered="false">
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
                <n-button v-if="rule.id !== 'filesize'" text @click="openEditModal(rule)">
                  <template #icon><n-icon :component="EditIcon" /></template>
                </n-button>
                <n-switch v-model:value="rule.enabled" />
              </n-space>
            </div>
          </n-card>
        </template>
      </draggable>

      <div style="display: flex; justify-content: flex-end; gap: 12px; margin-top: 16px;">
        <n-button @click="fetchRules">重置更改</n-button>
        <n-button type="primary" @click="saveRules" :loading="saving">保存规则</n-button>
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
import { ref, onMounted, defineEmits } from 'vue';
import axios from 'axios';
import { 
  NCard, NSpace, NSwitch, NButton, useMessage, NSpin, NIcon, NModal, NTag, NText
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Pencil as EditIcon, Move as DragHandleIcon,
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['on-close']);

const loading = ref(true);
const saving = ref(false);
const showEditModal = ref(false);

const rules = ref([]);
const currentEditingRule = ref({ priority: [] });

// ★★★ 核心修改 1/3: 更新元数据描述，使其更精确 ★★★
const RULE_METADATA = {
  quality: { name: "按质量", description: "比较文件名中的质量标签 (如 Remux, BluRay)。" },
  resolution: { name: "按分辨率", description: "比较视频的分辨率 (如 2160p, 1080p)。" },
  effect: { name: "按特效", description: "比较视频的特效等级 (如 DoVi Profile 8, HDR)。" },
  filesize: { name: "按文件大小", description: "如果以上规则都无法区分，则保留文件体积更大的版本。" }
};

const getRuleDisplayName = (id) => RULE_METADATA[id]?.name || id;
const getRuleDescription = (id) => RULE_METADATA[id]?.description || '未知规则';

// ★★★ 核心修改 2/3: 彻底重写特效标签的“翻译”函数 ★★★
const formatEffectPriority = (priorityArray, to = 'display') => {
    return priorityArray.map(p => {
        let p_lower = String(p).toLowerCase().replace(/\s/g, '_'); // 标准化输入

        // 进一步标准化 'dovi_other' 的各种变体
        if (p_lower === 'dovi' || p_lower === 'dovi_other' || p_lower === 'dovi(other)') {
            p_lower = 'dovi_other';
        }
        
        if (to === 'display') { // 转换为用户友好的显示文本
            if (p_lower === 'dovi_p8') return 'DoVi P8';
            if (p_lower === 'dovi_p7') return 'DoVi P7';
            if (p_lower === 'dovi_p5') return 'DoVi P5';
            if (p_lower === 'dovi_other') return 'DoVi (Other)';
            if (p_lower === 'hdr10+') return 'HDR10+';
            if (p_lower === 'hdr') return 'HDR';
            if (p_lower === 'sdr') return 'SDR';
            return p_lower.toUpperCase(); // 作为最后的通用回退
        } else { // (to === 'save') 转换为后端需要的存储格式
            // 确保保存时也进行标准化，以防前端有非标准输入
            if (p_lower === 'dovi_p8') return 'dovi_p8';
            if (p_lower === 'dovi_p7') return 'dovi_p7';
            if (p_lower === 'dovi_p5') return 'dovi_p5';
            if (p_lower === 'dovi_other') return 'dovi_other';
            if (p_lower === 'hdr10+') return 'hdr10+';
            if (p_lower === 'hdr') return 'hdr';
            if (p_lower === 'sdr') return 'sdr';
            return p_lower; // 如果是其他未知标签，则原样保存
        }
    });
};

const fetchRules = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/cleanup/rules');
    const loadedRules = response.data || [];
    
    rules.value = loadedRules.map(rule => {
        if (rule.id === 'effect' && Array.isArray(rule.priority)) {
            // 使用新的翻译函数，将 'dovi_p8' 格式化为 'DoVi P8'
            return { ...rule, priority: formatEffectPriority(rule.priority, 'display') };
        }
        return rule;
    });
  } catch (error) {
    message.error('加载清理规则失败！将使用默认规则。');
    // ★★★ 核心修改 3/3: 更新默认规则，以匹配新的显示格式 ★★★
    rules.value = [
        { id: 'quality', enabled: true, priority: ['Remux', 'BluRay', 'WEB-DL', 'HDTV'] },
        { id: 'resolution', enabled: true, priority: ['2160p', '1080p', '720p'] },
        { id: 'effect', enabled: true, priority: ['DoVi P8', 'DoVi P7', 'DoVi P5', 'DoVi (Other)', 'HDR10+', 'HDR', 'SDR'] },
        { id: 'filesize', enabled: true, priority: 'desc' },
    ];
  } finally {
    loading.value = false;
  }
};


const saveRules = async () => {
  saving.value = true;
  try {
    const rulesToSave = rules.value.map(rule => {
      if (rule.id === 'effect' && Array.isArray(rule.priority)) {
        // 保存时，使用翻译函数将 'DoVi P8' 转换回 'dovi_p8'
        return { ...rule, priority: formatEffectPriority(rule.priority, 'save') };
      }
      return rule;
    });
    await axios.post('/api/cleanup/rules', rulesToSave);
    message.success('清理规则已成功保存！');
    
    emit('on-close');

  } catch (error) {
    message.error('保存规则失败，请检查后端日志。');
  } finally {
    saving.value = false;
  }
};

const openEditModal = (rule) => {
  currentEditingRule.value = rule;
  showEditModal.value = true;
};

onMounted(fetchRules);
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
