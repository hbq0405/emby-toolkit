<template>
  <n-spin :show="loading">
    <n-space vertical :size="24">
      <n-card :bordered="false">
        <template #header>
          <span style="font-size: 1.2em; font-weight: bold;">媒体洗版规则</span>
        </template>
        <template #header-extra>
          <n-button type="primary" @click="openRuleModal()">
            <template #icon><n-icon :component="AddIcon" /></template>
            新增规则
          </n-button>
        </template>
        <p style="margin-top: 0; color: #888;">
          规则将按列表顺序（从上到下）进行匹配，媒体项会应用匹配到的第一个规则。拖拽规则可以调整优先级。
        </p>
      </n-card>

      <!-- 规则列表 -->
      <draggable
        v-model="rules"
        item-key="id"
        handle=".drag-handle"
        @end="onDragEnd"
        class="rules-list"
      >
        <template #item="{ element: rule }">
          <n-card class="rule-card" :key="rule.id">
            <div class="rule-content">
              <n-icon class="drag-handle" :component="DragHandleIcon" size="20" />
              <div class="rule-details">
                <span class="rule-name">{{ rule.name }}</span>
                <n-tag :type="getLibraryTagType(rule.target_library_ids)" size="small">
                  {{ getLibraryCountText(rule.target_library_ids) }}
                </n-tag>
              </div>
              <n-space class="rule-actions">
                <n-switch v-model:value="rule.enabled" @update:value="toggleRuleStatus(rule)" />
                <n-button text @click="openRuleModal(rule)">
                  <template #icon><n-icon :component="EditIcon" /></template>
                </n-button>
                <n-popconfirm @positive-click="deleteRule(rule.id)">
                  <template #trigger>
                    <n-button text type="error">
                      <template #icon><n-icon :component="DeleteIcon" /></template>
                    </n-button>
                  </template>
                  确定要删除规则 “{{ rule.name }}” 吗？
                </n-popconfirm>
              </n-space>
            </div>
          </n-card>
        </template>
      </draggable>
      <n-empty v-if="rules.length === 0" description="还没有任何规则，快新增一个吧！" />

      <!-- 规则编辑/新增弹窗 -->
      <n-modal v-model:show="showModal" preset="card" style="width: 600px;" :title="modalTitle">
        <n-form ref="formRef" :model="currentRule" :rules="formRules">
          <n-form-item path="name" label="规则名称">
            <n-input v-model:value="currentRule.name" placeholder="例如：4K Remux 收藏规则" />
          </n-form-item>
          <n-form-item path="target_library_ids" label="应用到以下媒体库">
            <n-select
              v-model:value="currentRule.target_library_ids"
              multiple
              filterable
              :options="availableLibraryOptions"
              placeholder="选择一个或多个媒体库"
            />
          </n-form-item>
          <n-form-item>
            <template #label>
              <n-space align="center">
                <span>订阅成功后删除Emby中的媒体项</span>
                <n-tooltip trigger="hover" v-if="!isEmbyAdminConfigured">
                  <template #trigger>
                    <n-icon :component="AlertIcon" style="color: var(--n-warning-color);" />
                  </template>
                  请先在 设置 -> Emby & 虚拟库 标签页中，配置“管理员登录凭证”。
                </n-tooltip>
              </n-space>
            </template>
            <n-switch 
              v-model:value="currentRule.delete_after_resubscribe"
              :disabled="!isEmbyAdminConfigured"
              @update:value="handleDeleteSwitchChange"
            />
          </n-form-item>
          <n-form-item>
            <template #label>
              <n-space align="center">
                <span>特效字幕</span>
                <n-tooltip trigger="hover">
                  <template #trigger>
                    <n-icon :component="AlertIcon" style="color: var(--n-info-color);" />
                  </template>
                  勾选后，订阅时将要求字幕包含“特效”关键字，需开启自定义洗版订阅。<br>
                  注意：此选项仅影响订阅参数，不影响筛选逻辑。
                </n-tooltip>
              </n-space>
            </template>
            <n-switch 
              v-model:value="currentRule.resubscribe_subtitle_effect_only"
            />
          </n-form-item>
          <n-divider />

          <!-- 洗版条件 -->
          <n-collapse>
            <n-collapse-item title="按分辨率洗版">
              <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_resolution_enabled" @click.stop />
              </template>
              <n-form-item label="洗版分辨率阈值 (宽度)" label-placement="left">
                <n-select
                  v-model:value="currentRule.resubscribe_resolution_threshold"
                  :options="resolutionOptions"
                  :disabled="!currentRule.resubscribe_resolution_enabled"
                />
              </n-form-item>
            </n-collapse-item>
            
            <n-collapse-item title="按质量洗版">
               <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_quality_enabled" @click.stop />
              </template>
              <n-form-item label="当媒体质量【低于】所选最高等级时洗版" label-placement="top">
                <n-select
                  v-model:value="currentRule.resubscribe_quality_include"
                  multiple tag filterable placeholder="可选择或自由输入"
                  :options="qualityOptions"
                  :disabled="!currentRule.resubscribe_quality_enabled"
                />
              </n-form-item>
            </n-collapse-item>

            <n-collapse-item title="按编码洗版">
               <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_codec_enabled" @click.stop />
              </template>
              <n-form-item label="当视频编码【低于】所选最高等级时洗版" label-placement="top">
                <n-select
                  v-model:value="currentRule.resubscribe_codec_include"
                  multiple tag filterable placeholder="选择期望的编码等级"
                  :options="codecOptions"
                  :disabled="!currentRule.resubscribe_codec_enabled"
                />
              </n-form-item>
            </n-collapse-item>

            <n-collapse-item title="按特效洗版">
               <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_effect_enabled" @click.stop />
              </template>
              <n-form-item label="当媒体特效【低于】所选最高等级时洗版" label-placement="top">
                <n-select
                  v-model:value="currentRule.resubscribe_effect_include"
                  multiple tag filterable placeholder="选择期望的特效等级"
                  :options="effectOptions"
                  :disabled="!currentRule.resubscribe_effect_enabled"
                />
              </n-form-item>
            </n-collapse-item>

            <n-collapse-item title="按文件大小洗版">
              <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_filesize_enabled" @click.stop />
              </template>
              <n-space vertical>
                <n-form-item label="洗版条件" label-placement="left">
                  <n-select
                    v-model:value="currentRule.resubscribe_filesize_operator"
                    :options="filesizeOperatorOptions"
                    :disabled="!currentRule.resubscribe_filesize_enabled"
                    style="width: 120px; margin-right: 8px;"
                  />
                  <n-input-number
                    v-model:value="currentRule.resubscribe_filesize_threshold_gb"
                    :min="0"
                    :step="0.1"
                    :disabled="!currentRule.resubscribe_filesize_enabled"
                    placeholder="输入大小"
                  >
                    <template #suffix>GB</template>
                  </n-input-number>
                </n-form-item>
                <p style="margin: 0; font-size: 12px; color: #888;">
                  提示：对于剧集，将按单集平均大小进行判断。
                </p>
              </n-space>
            </n-collapse-item>

            <n-collapse-item title="按音轨洗版">
               <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_audio_enabled" @click.stop />
              </template>
              <n-form-item label="当缺少以下音轨时洗版 (3字母代码)" label-placement="top">
                <n-select
                  v-model:value="currentRule.resubscribe_audio_missing_languages"
                  multiple tag
                  :options="languageOptions"
                  :disabled="!currentRule.resubscribe_audio_enabled"
                />
              </n-form-item>
            </n-collapse-item>

            <n-collapse-item title="按字幕洗版">
               <template #header-extra>
                <n-switch v-model:value="currentRule.resubscribe_subtitle_enabled" @click.stop />
              </template>
              <n-form-item label="当缺少以下字幕时洗版" label-placement="top">
                <n-select
                  v-model:value="currentRule.resubscribe_subtitle_missing_languages"
                  multiple tag
                  :options="subtitleLanguageOptions"
                  :disabled="!currentRule.resubscribe_subtitle_enabled"
                />
              </n-form-item>
              <n-form-item :disabled="!currentRule.resubscribe_subtitle_enabled">
                <template #label>
                  <n-space align="center">
                    <span>豁免规则</span>
                    <n-tooltip trigger="hover">
                      <template #trigger>
                        <n-icon :component="AlertIcon" style="color: var(--n-info-color);" />
                      </template>
                      开启后，如果媒体文件已包含与所缺字幕语言相同的音轨（如已有国语音轨），<br>
                      则系统将不再认为它缺少中文字幕。
                    </n-tooltip>
                  </n-space>
                </template>
                <n-switch 
                  v-model:value="currentRule.resubscribe_subtitle_skip_if_audio_exists"
                />
              </n-form-item>
            </n-collapse-item>
          </n-collapse>
        </n-form>
        <template #footer>
          <n-button @click="showModal = false">取消</n-button>
          <n-button type="primary" @click="saveRule" :loading="saving">保存</n-button>
        </template>
      </n-modal>

    </n-space>
  </n-spin>
</template>

<script setup>
import { ref, onMounted, computed, nextTick } from 'vue';
import axios from 'axios';
import { 
  NCard, NSpace, NSwitch, NButton, useMessage, NSpin, NIcon, NPopconfirm, NModal, NForm, 
  NFormItem, NInput, NSelect, NDivider, NCollapse, NCollapseItem, NTag, NEmpty, NTooltip, NInputNumber
} from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Add as AddIcon, Pencil as EditIcon, Trash as DeleteIcon, Move as DragHandleIcon, AlertCircleOutline as AlertIcon, 
} from '@vicons/ionicons5';

const message = useMessage();
const emit = defineEmits(['saved']);
const embyAdminUser = ref('');
const embyAdminPass = ref('');

const isEmbyAdminConfigured = computed(() => {
  return embyAdminUser.value && embyAdminPass.value;
});
const loading = ref(true);
const saving = ref(false);
const showModal = ref(false);

const rules = ref([]);
const currentRule = ref({});
const formRef = ref(null);
const allEmbyLibraries = ref([]);

const isEditing = computed(() => currentRule.value && currentRule.value.id);
const modalTitle = computed(() => isEditing.value ? '编辑规则' : '新增规则');

const formRules = {
  name: { required: true, message: '请输入规则名称', trigger: 'blur' },
  target_library_ids: { type: 'array', required: true, message: '请至少选择一个媒体库', trigger: 'change' },
};

const filesizeOperatorOptions = ref([
  { label: '小于', value: 'lt' },
  { label: '大于', value: 'gt' },
]);

const resolutionOptions = ref([
  { label: '低于 4K (3840px)', value: 3840 },
  { label: '低于 1080p (1920px)', value: 1920 },
  { label: '低于 720p (1280px)', value: 1280 },
]);

const qualityOptions = ref([
  { label: 'Remux', value: 'Remux' },
  { label: 'BluRay', value: 'BluRay' },
  { label: 'WEB-DL', value: 'WEB-DL' },
  { label: 'HDTV', value: 'HDTV' },
]);

const codecOptions = ref([
  { label: 'HEVC (H.265)', value: 'hevc' },
  { label: 'H.264 (AVC)', value: 'h264' },
]);

const effectOptions = ref([
  { label: 'DoVi Profile 8 (HDR10 兼容)', value: 'dovi_p8' },
  { label: 'DoVi Profile 7 (蓝光标准)', value: 'dovi_p7' },
  { label: 'DoVi Profile 5 (SDR 兼容)', value: 'dovi_p5' },
  { label: 'DoVi (其他)', value: 'dovi_other' },
  { label: 'HDR10+', value: 'hdr10+' },
  { label: 'HDR', value: 'hdr' },
]);

const languageOptions = ref([
    { label: '国语 (chi)', value: 'chi' }, { label: '粤语 (yue)', value: 'yue' },
    { label: '英语 (eng)', value: 'eng' }, { label: '日语 (jpn)', value: 'jpn' },
    { label: '韩语 (kor)', value: 'kor' }, // <--- ★★★ 新增韩语音轨
]);
const subtitleLanguageOptions = ref([
    { label: '简体 (chi)', value: 'chi' }, // <--- (建议) 修改标签以匹配后端
    { label: '繁体 (yue)', value: 'yue' }, // <--- (建议) 补上繁体
    { label: '英文 (eng)', value: 'eng' }, // <--- (建议) 修改标签以匹配后端
    { label: '日文 (jpn)', value: 'jpn' }, // <--- (建议) 补上日文
    { label: '韩文 (kor)', value: 'kor' }, // <--- ★★★ 新增韩文字幕
]);

const loadData = async () => {
  loading.value = true;
  try {
    const [rulesRes, configRes] = await Promise.all([
      axios.get('/api/resubscribe/rules'),
      axios.get('/api/config')
    ]);
    
    rules.value = rulesRes.data;
    embyAdminUser.value = configRes.data.emby_admin_user;
    embyAdminPass.value = configRes.data.emby_admin_pass;

  } catch (error) {
    message.error('加载基础数据失败，请检查网络或后端日志。');
  } finally {
    loading.value = false;
  }
};

const handleDeleteSwitchChange = (value) => {
  if (value && !isEmbyAdminConfigured.value) {
    message.warning('请先在 设置 -> Emby & 虚拟库 标签页中，配置“管理员登录凭证”，否则删除功能无法生效。');
    nextTick(() => {
      currentRule.value.delete_after_resubscribe = false;
    });
  }
};

const openRuleModal = async (rule = null) => {
  saving.value = true; 
  
  try {
    const libsRes = await axios.get('/api/config/cover_generator/libraries');
    allEmbyLibraries.value = libsRes.data;
  } catch (error) {
    message.error('获取媒体库列表失败！');
    saving.value = false;
    return;
  } finally {
    saving.value = false;
  }

  if (rule) {
    currentRule.value = JSON.parse(JSON.stringify(rule));
  } else {
    currentRule.value = {
      name: '', enabled: true, target_library_ids: [],
      delete_after_resubscribe: false, resubscribe_resolution_enabled: false,
      resubscribe_resolution_threshold: 1920, resubscribe_audio_enabled: false,
      resubscribe_audio_missing_languages: [], resubscribe_subtitle_enabled: false,
      resubscribe_subtitle_missing_languages: [], resubscribe_quality_enabled: false,
      resubscribe_quality_include: [], 
      
      resubscribe_codec_enabled: false,
      resubscribe_codec_include: [],

      resubscribe_effect_enabled: false,
      resubscribe_effect_include: [],
      resubscribe_subtitle_effect_only: false,
      resubscribe_filesize_enabled: false,
      resubscribe_filesize_operator: 'lt', 
      resubscribe_filesize_threshold_gb: null,
      resubscribe_subtitle_skip_if_audio_exists: false, 
    };
  }

  if (currentRule.value.resubscribe_effect_include) {
    const legacyMap = { '杜比视界': 'dovi_other', 'HDR': 'hdr' };
    currentRule.value.resubscribe_effect_include = currentRule.value.resubscribe_effect_include.map(
      val => legacyMap[val] || val
    );
  }

  showModal.value = true;
};

const availableLibraryOptions = computed(() => {
  if (!rules.value || !allEmbyLibraries.value) {
    return [];
  }

  const assignedIdsByOtherRules = new Set(
    rules.value
      .filter(rule => rule.id !== currentRule.value.id)
      .flatMap(rule => rule.target_library_ids || [])
  );

  return allEmbyLibraries.value.filter(lib => !assignedIdsByOtherRules.has(lib.value));
});

const saveRule = async () => {
  formRef.value?.validate(async (errors) => {
    if (!errors) {
      saving.value = true;
      try {
        if (isEditing.value) {
          await axios.put(`/api/resubscribe/rules/${currentRule.value.id}`, currentRule.value);
          message.success('规则已更新！');
        } else {
          await axios.post('/api/resubscribe/rules', currentRule.value);
          message.success('规则已创建！');
        }
        showModal.value = false;
        loadData();
        emit('saved', { needsRefresh: false });
      } catch (error) {
        message.error(error.response?.data?.error || '保存失败，请检查后端日志。');
      } finally {
        saving.value = false;
      }
    }
  });
};

const deleteRule = async (ruleId) => {
  try {
    await axios.delete(`/api/resubscribe/rules/${ruleId}`);
    message.success('规则已删除！');
    loadData();
    emit('saved', { needsRefresh: true });
  } catch (error) {
    message.error('删除失败，请检查后端日志。');
  }
};

const toggleRuleStatus = async (rule) => {
  try {
    await axios.put(`/api/resubscribe/rules/${rule.id}`, { enabled: rule.enabled });
    message.success(`规则 “${rule.name}” 已${rule.enabled ? '启用' : '禁用'}`);
    emit('saved', { needsRefresh: false });
  } catch (error) {
    message.error('状态更新失败');
    rule.enabled = !rule.enabled;
  }
};

const onDragEnd = async () => {
  const orderedIds = rules.value.map(r => r.id);
  try {
    await axios.post('/api/resubscribe/rules/order', orderedIds);
    message.success('规则优先级已更新！');
    emit('saved', { needsRefresh: false });
  } catch (error) {
    message.error('顺序保存失败，将刷新列表。');
    loadData();
  }
};

const getLibraryCountText = (libs) => {
  if (!libs || libs.length === 0) return '未指定媒体库';
  return `应用于 ${libs.length} 个媒体库`;
};
const getLibraryTagType = (libs) => {
  return (!libs || libs.length === 0) ? 'error' : 'success';
};

onMounted(loadData);
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
.rule-actions {
  margin-left: auto;
}
</style>