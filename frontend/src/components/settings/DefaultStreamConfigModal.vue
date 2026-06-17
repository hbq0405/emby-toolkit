<template>
  <n-modal
    v-model:show="showModal"
    preset="card"
    title="默认音轨与字幕配置"
    class="stream-config-modal custom-modal glass-modal"
    style="width: 1120px; max-width: 96vw;"
  >
    <n-spin :show="loading">
      <div class="config-grid">
        <!-- 左侧：音轨配置 -->
        <section class="config-panel audio-panel">
          <div class="panel-header compact-header">
            <div>
              <div class="panel-title">默认音轨设置</div>
              <div class="panel-desc">先按语言筛选，再按“大类优先级”比较特色词与物理参数。</div>
            </div>
          </div>

          <n-form label-placement="top" size="small">
            <n-form-item label="音轨语言优先级" class="compact-form-item">
              <div class="full-width">
                <n-input-group class="feature-input">
                  <n-select
                    v-model:value="selectedAudioLangToAdd"
                    :options="availableAudioLangOptions"
                    placeholder="选择语言后添加"
                    clearable
                  />
                  <n-button type="primary" @click="addAudioLangPriority">添加</n-button>
                </n-input-group>

                <n-alert v-if="config.audio_lang_priority.length === 0" type="info" :show-icon="false" class="tiny-alert">
                  未设置时保留文件原始默认音轨；添加后按从上到下顺序筛选，例如：国语 → 原语言 → 英语。
                </n-alert>

                <draggable
                  v-model="config.audio_lang_priority"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list compact-list lang-priority-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item compact-item">
                      <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                      <span class="item-name">{{ getAudioLangLabel(element.id) }}</span>
                      <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">优先级 {{ index + 1 }}</n-tag>
                      <n-button size="tiny" quaternary circle type="error" @click="removeAudioLangPriority(index)">
                        <template #icon><n-icon><CloseIcon /></n-icon></template>
                      </n-button>
                    </div>
                  </template>
                </draggable>

                <n-text depth="3" class="helper-text inline-helper">
                  没有命中时自动尝试下一档。
                </n-text>
              </div>
            </n-form-item>

            <div class="macro-section">
              <div class="mini-section-head macro-head">
                <div>
                  <div class="mini-title">音轨大类优先级</div>
                  <div class="mini-desc">拖动决定“特色词”和“物理参数”谁先比较。</div>
                </div>
              </div>

              <draggable
                v-model="config.audio_priority_order"
                item-key="id"
                handle=".drag-handle"
                animation="200"
                class="group-priority-list"
              >
                <template #item="{ element, index }">
                  <div class="group-priority-item">
                    <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                    <div class="group-main">
                      <span class="group-title">{{ getAudioPriorityGroupLabel(element.id) }}</span>
                      <span class="group-desc">{{ getAudioPriorityGroupDesc(element.id) }}</span>
                    </div>
                    <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">第 {{ index + 1 }} 层</n-tag>
                  </div>
                </template>
              </draggable>
            </div>

            <div class="audio-priority-grid">
              <div class="mini-section">
                <div class="mini-section-head">
                  <div>
                    <div class="mini-title">特色词优先级</div>
                    <div class="mini-desc">公映、国配、上译、导评等版本词。</div>
                  </div>
                </div>

                <n-input-group class="feature-input">
                  <n-input
                    v-model:value="newAudioFeature"
                    placeholder="如：上译"
                    @keyup.enter="addAudioFeature"
                  />
                  <n-button type="primary" @click="addAudioFeature">添加</n-button>
                </n-input-group>

                <draggable
                  v-model="config.audio_features"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list compact-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item compact-item">
                      <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                      <span class="item-name">{{ element.text }}</span>
                      <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">{{ index + 1 }}</n-tag>
                      <n-button size="tiny" quaternary circle type="error" @click="removeAudioFeature(index)">
                        <template #icon><n-icon><CloseIcon /></n-icon></template>
                      </n-button>
                    </div>
                  </template>
                </draggable>
              </div>

              <div class="mini-section">
                <div class="mini-section-head">
                  <div>
                    <div class="mini-title">物理参数优先级</div>
                    <div class="mini-desc">Atmos、DTS、7.1、5.1 等硬指标。</div>
                  </div>
                  <n-button size="tiny" tertiary @click="resetAudioParams">默认</n-button>
                </div>

                <n-alert type="info" :show-icon="false" class="tiny-alert">
                  上下拖动，越靠上越优先；不需要的参数可删除，恢复默认可一键找回。
                </n-alert>

                <draggable
                  v-model="config.audio_param_priority"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list compact-list param-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item compact-item param-item">
                      <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                      <div class="param-main">
                        <span class="item-name">{{ getAudioParamLabel(element.id) }}</span>
                        <span class="param-desc">{{ getAudioParamDesc(element.id) }}</span>
                      </div>
                      <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">{{ index + 1 }}</n-tag>
                      <n-button size="tiny" quaternary circle type="error" @click="removeAudioParam(index)">
                        <template #icon><n-icon><CloseIcon /></n-icon></template>
                      </n-button>
                    </div>
                  </template>
                </draggable>
              </div>
            </div>
          </n-form>
        </section>

        <!-- 右侧：字幕配置 -->
        <section class="config-panel subtitle-panel">
          <div class="panel-header compact-header">
            <div>
              <div class="panel-title">默认字幕设置</div>
              <div class="panel-desc">控制简繁方向和字幕类型排序，智能跟随音轨仍是最高优先级。</div>
            </div>
          </div>

          <n-form label-placement="top" size="small">
            
            <!-- ★ 新增：实时字幕流语言嗅探 开关 -->
            <n-form-item label="未知文本字幕语言嗅探" class="compact-form-item">
              <div class="full-width">
                <n-space align="center" style="margin-bottom: 6px;">
                  <n-switch v-model:value="config.realtime_sub_detect" />
                  <span style="font-size: 13px; color: var(--n-text-color-2); font-weight: 500;">开启实时字幕语言识别</span>
                </n-space>
                <n-alert type="warning" :show-icon="false" class="tiny-alert" style="margin-top: 0;">
                  缺失语言标签时，尝试解析字幕流识别语言。
                </n-alert>
              </div>
            </n-form-item>

            <n-form-item label="字幕语言优先级" class="compact-form-item">
              <div class="full-width">
                <n-input-group class="feature-input">
                  <n-select
                    v-model:value="selectedSubtitleLangToAdd"
                    :options="availableSubtitleLangOptions"
                    placeholder="选择语言后添加"
                    clearable
                  />
                  <n-button type="primary" @click="addSubtitleLangPriority">添加</n-button>
                </n-input-group>

                <n-alert v-if="config.subtitle_lang_priority.length === 0" type="info" :show-icon="false" class="tiny-alert">
                  未设置时只按下方字幕类型排序；添加后按从上到下顺序筛选，例如：简体 → 原语言 → 英文。
                </n-alert>

                <draggable
                  v-model="config.subtitle_lang_priority"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list compact-list lang-priority-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item compact-item">
                      <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                      <span class="item-name">{{ getSubtitleLangLabel(element.id) }}</span>
                      <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">优先级 {{ index + 1 }}</n-tag>
                      <n-button size="tiny" quaternary circle type="error" @click="removeSubtitleLangPriority(index)">
                        <template #icon><n-icon><CloseIcon /></n-icon></template>
                      </n-button>
                    </div>
                  </template>
                </draggable>

                <n-text depth="3" class="helper-text inline-helper">
                  语言优先级高于特效/双语类型排序，避免繁体特效越过你指定的简体。
                </n-text>
              </div>
            </n-form-item>

            <n-form-item label="字幕优先级" class="compact-form-item">
              <div class="full-width">
                <n-alert type="info" :show-icon="false" class="tiny-alert">
                  拖动排序。特效只是字幕特征，不会越过上方简繁偏好。
                </n-alert>

                <draggable
                  v-model="config.sub_priority"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list compact-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item compact-item">
                      <n-icon class="drag-handle" size="18"><MenuIcon /></n-icon>
                      <span class="item-name">{{ getSubLabel(element.id) }}</span>
                      <n-tag size="tiny" :type="index === 0 ? 'success' : 'default'">{{ index + 1 }}</n-tag>
                    </div>
                  </template>
                </draggable>
              </div>
            </n-form-item>
          </n-form>
        </section>
      </div>
    </n-spin>

    <template #footer>
      <div class="footer-actions">
        <n-button tertiary @click="restoreDefaults">一键恢复默认</n-button>
        <n-space justify="end">
          <n-button @click="showModal = false">取消</n-button>
          <n-button type="primary" @click="saveConfig" :loading="saving">保存配置</n-button>
        </n-space>
      </div>
    </template>
  </n-modal>
</template>

<script setup>
import { computed, ref } from 'vue';
import { NModal, NForm, NFormItem, NSelect, NInput, NInputGroup, NAlert, NSpin, NSpace, NButton, NIcon, NTag, NText, NSwitch, useMessage } from 'naive-ui';
import { MenuOutline as MenuIcon, CloseOutline as CloseIcon } from '@vicons/ionicons5';
import draggable from 'vuedraggable';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);

const newAudioFeature = ref('');
const selectedAudioLangToAdd = ref(null);
const selectedSubtitleLangToAdd = ref(null);

const audioLangOptions = [
  { label: '国语', value: 'chi' },
  { label: '粤语', value: 'yue' },
  { label: '原语言', value: 'original' },
  { label: '英语', value: 'eng' },
  { label: '日语', value: 'jpn' },
  { label: '韩语', value: 'kor' }
];

const subtitleLangOptions = [
  { label: '中文简体', value: 'chs' },
  { label: '中文繁体', value: 'cht' },
  { label: '原语言', value: 'original' },
  { label: '英文', value: 'eng' },
  { label: '日文', value: 'jpn' },
  { label: '韩文', value: 'kor' }
];

const subTypeMap = {
  effect: '特效字幕',
  chs: '简体中文',
  cht: '繁体中文',
  chs_eng: '简英双语',
  cht_eng: '繁英双语',
  chs_jpn: '简日双语',
  cht_jpn: '繁日双语',
  chs_kor: '简韩双语',
  cht_kor: '繁韩双语'
};

const defaultAudioPriorityOrder = ['param', 'feature'];
const audioPriorityGroupMap = {
  param: { label: '物理参数优先', desc: 'Atmos / DTS-HD MA / 7.1 / 2.0' },
  feature: { label: '特色词优先', desc: '公映 / 国配 / 上译 / 导评' }
};

const defaultAudioFeatures = ['公映', '上译', '京译', '央视', '长译', '八一', '国配', '台配', '国语', '粤语'];
const defaultAudioParamPriority = [
  'atmos',
  'dts_x',
  'truehd',
  'dts_hd_ma',
  'dts_hd_hra',
  'ddp',
  'dts',
  'flac',
  'ac3',
  'aac',
  '7_1',
  '5_1',
  '2_0'
];
const defaultSubPriority = ['effect', 'chs', 'cht', 'chs_eng', 'cht_eng', 'chs_jpn', 'cht_jpn', 'chs_kor', 'cht_kor'];

const audioParamMap = {
  atmos: { label: '杜比全景声 Atmos', desc: 'TrueHD / DDP Atmos' },
  dts_x: { label: 'DTS:X', desc: 'DTS 沉浸声' },
  truehd: { label: 'TrueHD', desc: '无损杜比' },
  dts_hd_ma: { label: 'DTS-HD MA', desc: '无损 DTS' },
  dts_hd_hra: { label: 'DTS-HD HRA', desc: '高解析 DTS' },
  ddp: { label: 'DDP / EAC3', desc: '流媒体常见' },
  dts: { label: 'DTS', desc: '核心 DTS' },
  flac: { label: 'FLAC', desc: '无损音频' },
  ac3: { label: 'AC3', desc: 'Dolby Digital' },
  aac: { label: 'AAC', desc: '常规压缩' },
  '7_1': { label: '7.1 声道', desc: '8 channels' },
  '5_1': { label: '5.1 声道', desc: '6 channels' },
  '2_0': { label: '2.0 / Stereo', desc: '双声道' },
  stereo: { label: 'Stereo', desc: '立体声' }
};

const config = ref({
  audio_lang: '',
  subtitle_lang: '',
  audio_lang_priority: [],
  subtitle_lang_priority: [],
  audio_priority_order: [],
  audio_features: [],
  audio_param_priority: [],
  sub_priority: [],
  realtime_sub_detect: false // ★ 新增初始值
});

const getSubLabel = (id) => subTypeMap[id] || id;
const getAudioParamLabel = (id) => audioParamMap[id]?.label || id;
const getAudioParamDesc = (id) => audioParamMap[id]?.desc || '';
const getAudioPriorityGroupLabel = (id) => audioPriorityGroupMap[id]?.label || id;
const getAudioPriorityGroupDesc = (id) => audioPriorityGroupMap[id]?.desc || '';
const optionLabelMap = (options) => Object.fromEntries(options.map(opt => [opt.value, opt.label]));
const audioLangLabelMap = optionLabelMap(audioLangOptions);
const subtitleLangLabelMap = optionLabelMap(subtitleLangOptions);
const getAudioLangLabel = (id) => audioLangLabelMap[id] || id;
const getSubtitleLangLabel = (id) => subtitleLangLabelMap[id] || id;

const availableAudioLangOptions = computed(() => {
  const used = new Set(config.value.audio_lang_priority.map(item => item.id));
  return audioLangOptions.filter(opt => !used.has(opt.value));
});

const availableSubtitleLangOptions = computed(() => {
  const used = new Set(config.value.subtitle_lang_priority.map(item => item.id));
  return subtitleLangOptions.filter(opt => !used.has(opt.value));
});

const uniqueIds = (ids) => {
  const seen = new Set();
  const cleanIds = [];
  (ids || []).forEach((id) => {
    if (id && !seen.has(id)) {
      seen.add(id);
      cleanIds.push(id);
    }
  });
  return cleanIds;
};

const makeIdItems = (ids) => uniqueIds(ids).map(id => ({ id }));
const makeLangPriorityItems = (ids, allowedOptions) => {
  const allowed = new Set(allowedOptions.map(opt => opt.value));
  return uniqueIds(ids).filter(id => allowed.has(id)).map(id => ({ id }));
};

const legacyToPriorityList = (priorityList, legacyValue) => {
  if (Array.isArray(priorityList) && priorityList.length > 0) {
    return priorityList;
  }
  return legacyValue ? [legacyValue] : [];
};

const makeAudioPriorityGroupItems = (ids) => {
  const cleanIds = uniqueIds(ids).filter(id => ['param', 'feature'].includes(id));
  defaultAudioPriorityOrder.forEach((id) => {
    if (!cleanIds.includes(id)) cleanIds.push(id);
  });
  return cleanIds.map(id => ({ id }));
};

const resetAudioParams = () => {
  config.value.audio_param_priority = makeIdItems(defaultAudioParamPriority);
  message.success('已恢复物理参数默认排序');
};

const addAudioLangPriority = () => {
  const val = selectedAudioLangToAdd.value;
  if (!val) return;
  if (!config.value.audio_lang_priority.some(item => item.id === val)) {
    config.value.audio_lang_priority.push({ id: val });
  }
  selectedAudioLangToAdd.value = null;
};

const removeAudioLangPriority = (index) => {
  config.value.audio_lang_priority.splice(index, 1);
};

const addSubtitleLangPriority = () => {
  const val = selectedSubtitleLangToAdd.value;
  if (!val) return;
  if (!config.value.subtitle_lang_priority.some(item => item.id === val)) {
    config.value.subtitle_lang_priority.push({ id: val });
  }
  selectedSubtitleLangToAdd.value = null;
};

const removeSubtitleLangPriority = (index) => {
  config.value.subtitle_lang_priority.splice(index, 1);
};

const restoreDefaults = () => {
  config.value = {
    audio_lang: '',
    subtitle_lang: '',
    audio_lang_priority: [],
    subtitle_lang_priority: [],
    audio_priority_order: makeAudioPriorityGroupItems(defaultAudioPriorityOrder),
    audio_features: defaultAudioFeatures.map((text, index) => ({ id: `default_audio_${index}_${Date.now()}`, text })),
    audio_param_priority: makeIdItems(defaultAudioParamPriority),
    sub_priority: makeIdItems(defaultSubPriority),
    realtime_sub_detect: false // ★ 重置开关
  };
  message.success('已恢复默认配置，保存后生效');
};

const addAudioFeature = () => {
  const val = newAudioFeature.value.trim();
  if (val) {
    if (!config.value.audio_features.some(item => item.text === val)) {
      config.value.audio_features.push({ id: Date.now().toString() + Math.random(), text: val });
    } else {
      message.warning('该特征词已存在');
    }
    newAudioFeature.value = '';
  }
};

const removeAudioFeature = (index) => {
  config.value.audio_features.splice(index, 1);
};

const removeAudioParam = (index) => {
  config.value.audio_param_priority.splice(index, 1);
};

const loadConfig = async () => {
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/default_stream_config');
    if (res.data.success) {
      const data = res.data.data || {};
      config.value.audio_lang = data.audio_lang || '';
      config.value.subtitle_lang = data.subtitle_lang || '';
      config.value.audio_lang_priority = makeLangPriorityItems(
        legacyToPriorityList(data.audio_lang_priority, data.audio_lang),
        audioLangOptions
      );
      config.value.subtitle_lang_priority = makeLangPriorityItems(
        legacyToPriorityList(data.subtitle_lang_priority, data.subtitle_lang),
        subtitleLangOptions
      );

      config.value.audio_priority_order = makeAudioPriorityGroupItems(data.audio_priority_order || defaultAudioPriorityOrder);
      config.value.audio_features = (Array.isArray(data.audio_features) ? data.audio_features : defaultAudioFeatures).map((text, index) => ({
        id: `audio_${index}_${Date.now()}`,
        text
      }));
      config.value.audio_param_priority = makeIdItems(Array.isArray(data.audio_param_priority) ? data.audio_param_priority : defaultAudioParamPriority);
      config.value.sub_priority = makeIdItems(Array.isArray(data.sub_priority) ? data.sub_priority : defaultSubPriority);
      
      // ★ 加载嗅探开关值
      config.value.realtime_sub_detect = !!data.realtime_sub_detect;
    }
  } catch (error) {
    message.error('加载配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  saving.value = true;
  try {
    const audioLangPriority = config.value.audio_lang_priority.map(item => item.id);
    const subtitleLangPriority = config.value.subtitle_lang_priority.map(item => item.id);
    const payload = {
      audio_lang: audioLangPriority[0] || '',
      subtitle_lang: subtitleLangPriority[0] || '',
      audio_lang_priority: audioLangPriority,
      subtitle_lang_priority: subtitleLangPriority,
      audio_priority_order: config.value.audio_priority_order.map(item => item.id),
      audio_features: config.value.audio_features.map(item => item.text),
      audio_param_priority: config.value.audio_param_priority.map(item => item.id),
      sub_priority: config.value.sub_priority.map(item => item.id),
      // ★ 提交开关状态
      realtime_sub_detect: config.value.realtime_sub_detect
    };
    const res = await axios.post('/api/p115/default_stream_config', payload);
    if (res.data.success) {
      message.success(res.data.message);
      showModal.value = false;
    }
  } catch (error) {
    message.error('保存配置失败');
  } finally {
    saving.value = false;
  }
};

const open = () => {
  showModal.value = true;
  loadConfig();
};

defineExpose({ open });
</script>

<style scoped>
:deep(.stream-config-modal) {
  width: 1120px;
  max-width: 96vw;
}

.config-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(360px, 0.75fr);
  gap: 16px;
  align-items: start;
}

.config-panel {
  min-width: 0;
  padding: 16px;
  border: 1px solid var(--n-divider-color);
  border-radius: 12px;
  background: var(--n-card-color);
}

.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--n-divider-color);
}

.compact-header {
  margin-bottom: 12px;
}

.panel-title {
  font-size: 16px;
  font-weight: 700;
  line-height: 1.35;
}

.panel-desc {
  margin-top: 3px;
  font-size: 12px;
  line-height: 1.45;
  color: var(--n-text-color-3);
}

.compact-form-item {
  margin-bottom: 12px;
}

.full-width {
  width: 100%;
}

.inline-helper {
  display: block;
  margin-top: 2px;
  font-size: 12px;
  line-height: 1.45;
}

.audio-priority-grid {
  display: grid;
  grid-template-columns: minmax(0, 0.95fr) minmax(0, 1.05fr);
  gap: 12px;
  align-items: start;
}

.macro-section,
.mini-section {
  min-width: 0;
  padding: 12px;
  border: 1px solid var(--n-divider-color);
  border-radius: 10px;
  background: var(--n-action-color);
}

.macro-section {
  margin-bottom: 12px;
  padding-bottom: 10px;
}

.mini-section-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 10px;
}

.macro-head {
  margin-bottom: 8px;
}

.mini-title {
  font-size: 13px;
  font-weight: 700;
  line-height: 1.3;
}

.mini-desc {
  margin-top: 2px;
  font-size: 11px;
  line-height: 1.35;
  color: var(--n-text-color-3);
}

.group-priority-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.group-priority-item {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  min-height: 42px;
  padding: 8px 10px;
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
  background: var(--n-card-color);
}

.group-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.group-title {
  font-size: 13px;
  font-weight: 700;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.group-desc {
  color: var(--n-text-color-3);
  font-size: 11px;
  line-height: 1.2;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.feature-input {
  margin-bottom: 10px;
}

.tiny-alert {
  margin-bottom: 10px;
  font-size: 12px;
  line-height: 1.45;
}

.priority-list {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.compact-list {
  max-height: 390px;
  overflow: auto;
  padding-right: 2px;
}

.lang-priority-list {
  max-height: 160px;
}

.subtitle-panel .compact-list {
  max-height: 520px;
}

.priority-item {
  display: flex;
  align-items: center;
  min-height: 40px;
  padding: 8px 10px;
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
  background-color: var(--n-card-color);
  transition: border-color 0.2s, background-color 0.2s, transform 0.2s;
}

.compact-item {
  min-height: 36px;
  padding: 7px 9px;
}

.priority-item:hover,
.group-priority-item:hover {
  border-color: var(--n-primary-color);
}

.drag-handle {
  flex: 0 0 auto;
  cursor: grab;
  margin-right: 8px;
  color: #999;
}

.group-priority-item .drag-handle {
  margin-right: 0;
}

.drag-handle:active {
  cursor: grabbing;
}

.item-name {
  flex: 1;
  min-width: 0;
  font-weight: 600;
  font-size: 13px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.param-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.param-desc {
  min-width: 0;
  color: var(--n-text-color-3);
  font-size: 11px;
  line-height: 1.2;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.priority-item .n-tag {
  margin-left: 8px;
  flex: 0 0 auto;
}

.priority-item .n-button {
  margin-left: 4px;
  flex: 0 0 auto;
}

.footer-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

@media (max-width: 980px) {
  .config-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 760px) {
  .audio-priority-grid,
  .group-priority-list {
    grid-template-columns: 1fr;
  }
}
</style>
