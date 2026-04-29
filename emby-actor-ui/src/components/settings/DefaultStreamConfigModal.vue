<template>
  <n-modal
    v-model:show="showModal"
    preset="card"
    title="默认音轨与字幕配置"
    class="stream-config-modal"
    style="width: 1120px; max-width: 96vw;"
  >
    <n-spin :show="loading">
      <div class="config-grid">
        <!-- 左侧：音轨配置 -->
        <section class="config-panel audio-panel">
          <div class="panel-header compact-header">
            <div>
              <div class="panel-title">默认音轨设置</div>
              <div class="panel-desc">先按语言筛选，再按“特色词”与“物理参数”选择默认音轨。</div>
            </div>
          </div>

          <n-form label-placement="top" size="small">
            <n-form-item label="首选语言" class="compact-form-item">
              <n-select v-model:value="config.audio_lang" :options="audioLangOptions" />
              <template #feedback>
                <n-text depth="3" class="helper-text inline-helper">
                  例如选“国语”后，只在国语音轨里比较公映、DTS-HD MA、7.1 等优先级。
                </n-text>
              </template>
            </n-form-item>

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
                  上下拖动，越靠上越优先；会与特色词叠加打分。
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
            <n-form-item label="字幕偏好" class="compact-form-item">
              <n-select v-model:value="config.subtitle_lang" :options="subtitleLangOptions" />
              <template #feedback>
                <n-text depth="3" class="helper-text inline-helper">
                  简体/繁体偏好会压制另一方，避免繁体双语偷家。
                </n-text>
              </template>
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
      <n-space justify="end">
        <n-button @click="showModal = false">取消</n-button>
        <n-button type="primary" @click="saveConfig" :loading="saving">保存配置</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { NModal, NForm, NFormItem, NSelect, NInput, NInputGroup, NAlert, NSpin, NSpace, NButton, NIcon, NTag, NText, useMessage } from 'naive-ui';
import { MenuOutline as MenuIcon, CloseOutline as CloseIcon } from '@vicons/ionicons5';
import draggable from 'vuedraggable';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);

const newAudioFeature = ref('');

const audioLangOptions = [
  { label: '不修改 (保留文件原始默认)', value: '' },
  { label: '优先国语', value: 'chi' },
  { label: '优先粤语', value: 'yue' },
  { label: '优先英语', value: 'eng' },
  { label: '优先日语', value: 'jpn' },
  { label: '优先韩语', value: 'kor' }
];

const subtitleLangOptions = [
  { label: '不修改 (只按下面优先级排序)', value: '' },
  { label: '优先简体', value: 'chs' },
  { label: '优先繁体', value: 'cht' }
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
  audio_features: [],
  audio_param_priority: [],
  sub_priority: []
});

const getSubLabel = (id) => subTypeMap[id] || id;
const getAudioParamLabel = (id) => audioParamMap[id]?.label || id;
const getAudioParamDesc = (id) => audioParamMap[id]?.desc || '';

const makeAudioParamItems = (ids) => {
  const seen = new Set();
  const cleanIds = [];

  (ids || []).forEach((id) => {
    if (id && !seen.has(id)) {
      seen.add(id);
      cleanIds.push(id);
    }
  });

  defaultAudioParamPriority.forEach((id) => {
    if (!seen.has(id)) {
      seen.add(id);
      cleanIds.push(id);
    }
  });

  return cleanIds.map(id => ({ id }));
};

const resetAudioParams = () => {
  config.value.audio_param_priority = makeAudioParamItems(defaultAudioParamPriority);
  message.success('已恢复物理参数默认排序');
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

const loadConfig = async () => {
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/default_stream_config');
    if (res.data.success) {
      const data = res.data.data;
      config.value.audio_lang = data.audio_lang || '';
      config.value.subtitle_lang = data.subtitle_lang || '';

      config.value.audio_features = (data.audio_features || []).map((text, index) => ({
        id: `audio_${index}_${Date.now()}`,
        text
      }));
      config.value.audio_param_priority = makeAudioParamItems(data.audio_param_priority || defaultAudioParamPriority);
      config.value.sub_priority = (data.sub_priority || []).map(id => ({ id }));
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
    const payload = {
      audio_lang: config.value.audio_lang,
      subtitle_lang: config.value.subtitle_lang,
      audio_features: config.value.audio_features.map(item => item.text),
      audio_param_priority: config.value.audio_param_priority.map(item => item.id),
      sub_priority: config.value.sub_priority.map(item => item.id)
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

.mini-section {
  min-width: 0;
  padding: 12px;
  border: 1px solid var(--n-divider-color);
  border-radius: 10px;
  background: var(--n-action-color);
}

.mini-section-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 10px;
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
  max-height: 430px;
  overflow: auto;
  padding-right: 2px;
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

.priority-item:hover {
  border-color: var(--n-primary-color);
}

.drag-handle {
  flex: 0 0 auto;
  cursor: grab;
  margin-right: 8px;
  color: #999;
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

@media (max-width: 980px) {
  .config-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 760px) {
  .audio-priority-grid {
    grid-template-columns: 1fr;
  }
}
</style>
