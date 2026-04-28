<template>
  <n-modal v-model:show="showModal" preset="card" title="默认音轨与字幕配置" class="stream-config-modal" style="width: 980px; max-width: 95vw;">
    <n-spin :show="loading">
      <div class="config-grid">
        <!-- 左侧：音轨配置 -->
        <section class="config-panel">
          <div class="panel-header">
            <div>
              <div class="panel-title">默认音轨设置</div>
              <div class="panel-desc">先按语言筛选，再按特征词优先级选择默认音轨。</div>
            </div>
          </div>

          <n-form label-placement="top">
            <n-form-item label="首选语言">
              <n-select v-model:value="config.audio_lang" :options="audioLangOptions" />
            </n-form-item>

            <n-form-item label="音轨特征词">
              <div class="full-width">
                <n-input-group class="feature-input">
                  <n-input
                    v-model:value="newAudioFeature"
                    placeholder="输入特征词，如：上译"
                    @keyup.enter="addAudioFeature"
                  />
                  <n-button type="primary" @click="addAudioFeature">添加</n-button>
                </n-input-group>

                <n-text depth="3" class="helper-text">
                  包含这些词的音轨将被优先选中，上下拖动调整优先级（越靠上优先级越高）。字幕会最高优先级跟随匹配。
                </n-text>

                <draggable
                  v-model="config.audio_features"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item">
                      <n-icon class="drag-handle" size="20"><MenuIcon /></n-icon>
                      <span class="item-name">{{ element.text }}</span>
                      <n-tag size="small" :type="index === 0 ? 'success' : 'default'">
                        优先级 {{ index + 1 }}
                      </n-tag>
                      <n-button size="tiny" quaternary circle type="error" @click="removeAudioFeature(index)">
                        <template #icon><n-icon><CloseIcon /></n-icon></template>
                      </n-button>
                    </div>
                  </template>
                </draggable>
              </div>
            </n-form-item>
          </n-form>
        </section>

        <!-- 右侧：字幕配置 -->
        <section class="config-panel">
          <div class="panel-header">
            <div>
              <div class="panel-title">默认字幕设置</div>
              <div class="panel-desc">控制简繁方向和字幕类型排序，智能跟随音轨仍是最高优先级。</div>
            </div>
          </div>

          <n-form label-placement="top">
            <n-form-item label="字幕偏好">
              <n-select v-model:value="config.subtitle_lang" :options="subtitleLangOptions" />
              <template #feedback>
                <n-text depth="3" class="helper-text">
                  只控制简体/繁体方向；双语字幕仍由下方优先级决定。
                </n-text>
              </template>
            </n-form-item>

            <n-form-item label="字幕优先级">
              <div class="full-width">
                <n-alert type="info" :show-icon="true" class="priority-alert">
                  上下拖动调整优先级（越靠上优先级越高）。<br />
                  <b>注意：</b>智能跟随音轨为最高硬编码优先级，不受此列表影响。
                </n-alert>

                <draggable
                  v-model="config.sub_priority"
                  item-key="id"
                  handle=".drag-handle"
                  animation="200"
                  class="priority-list"
                >
                  <template #item="{ element, index }">
                    <div class="priority-item">
                      <n-icon class="drag-handle" size="20"><MenuIcon /></n-icon>
                      <span class="item-name">{{ getSubLabel(element.id) }}</span>
                      <n-tag size="small" :type="index === 0 ? 'success' : 'default'">
                        优先级 {{ index + 1 }}
                      </n-tag>
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
  'effect': '特效字幕',
  'chs': '简体中文',
  'cht': '繁体中文',
  'chs_eng': '简英双语',
  'cht_eng': '繁英双语',
  'chs_jpn': '简日双语',
  'cht_jpn': '繁日双语',
  'chs_kor': '简韩双语',
  'cht_kor': '繁韩双语'
};

const config = ref({
  audio_lang: '',
  subtitle_lang: '',
  audio_features: [],
  sub_priority: []
});

const getSubLabel = (id) => subTypeMap[id] || id;

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
      config.value.audio_lang = data.audio_lang;
      config.value.subtitle_lang = data.subtitle_lang || '';
      
      // 将字符串数组转为 vuedraggable 需要的对象数组
      config.value.audio_features = (data.audio_features || []).map((text, index) => ({
        id: `audio_${index}_${Date.now()}`,
        text
      }));
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
      // 将对象数组还原为字符串数组
      audio_features: config.value.audio_features.map(item => item.text),
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
  width: 980px;
  max-width: 95vw;
}

.config-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 20px;
  align-items: start;
}

.config-panel {
  min-width: 0;
  padding: 18px;
  border: 1px solid var(--n-divider-color);
  border-radius: 10px;
  background: var(--n-card-color);
}

.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--n-divider-color);
}

.panel-title {
  font-size: 16px;
  font-weight: 700;
  line-height: 1.4;
}

.panel-desc {
  margin-top: 4px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--n-text-color-3);
}

.full-width {
  width: 100%;
}

.feature-input {
  margin-bottom: 10px;
}

.helper-text {
  display: block;
  margin-bottom: 12px;
  font-size: 12px;
  line-height: 1.6;
}

.priority-alert {
  margin-bottom: 12px;
}

.priority-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.priority-item {
  display: flex;
  align-items: center;
  min-height: 42px;
  padding: 9px 12px;
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
  background-color: var(--n-action-color);
  transition: border-color 0.2s, background-color 0.2s;
}

.priority-item:hover {
  border-color: var(--n-primary-color);
}

.drag-handle {
  flex: 0 0 auto;
  cursor: grab;
  margin-right: 10px;
  color: #999;
}

.drag-handle:active {
  cursor: grabbing;
}

.item-name {
  flex: 1;
  min-width: 0;
  font-weight: 600;
  font-size: 14px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.priority-item .n-tag {
  margin-left: 10px;
  margin-right: 4px;
  flex: 0 0 auto;
}

@media (max-width: 760px) {
  .config-grid {
    grid-template-columns: 1fr;
  }
}
</style>
