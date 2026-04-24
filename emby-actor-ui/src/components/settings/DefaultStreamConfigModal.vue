<template>
  <n-modal v-model:show="showModal" preset="card" title="默认音轨与字幕配置" style="width: 600px; max-width: 95vw;">
    <n-spin :show="loading">
      <n-form label-placement="left" label-width="100" label-align="right">
        
        <n-divider title-placement="left" style="margin-top: 0;">默认音轨设置</n-divider>
        
        <n-form-item label="首选语言">
          <n-select v-model:value="config.audio_lang" :options="langOptions" />
        </n-form-item>
        
        <n-form-item label="音轨特征词">
          <n-dynamic-tags v-model:value="config.audio_features" />
          <template #feedback>
            <n-text depth="3" style="font-size: 12px;">
              包含这些词的音轨将被优先选中，且<b>字幕会最高优先级跟随匹配</b>（如：音轨命中"上译"，则优先选择带"上译"的字幕）。
            </n-text>
          </template>
        </n-form-item>

        <n-divider title-placement="left">默认字幕优先级</n-divider>
        
        <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
          上下拖动调整优先级（越靠上优先级越高）。<br/>
          <b>注意：</b>智能跟随音轨为最高硬编码优先级，不受此列表影响。
        </n-alert>

        <draggable 
          v-model="config.sub_priority" 
          item-key="id" 
          handle=".drag-handle" 
          animation="200"
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

      </n-form>
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
import { NModal, NForm, NFormItem, NSelect, NDynamicTags, NDivider, NAlert, NSpin, NSpace, NButton, NIcon, NTag, NText, useMessage } from 'naive-ui';
import { MenuOutline as MenuIcon } from '@vicons/ionicons5';
import draggable from 'vuedraggable';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);

const langOptions = [
  { label: '不修改 (保留文件原始默认)', value: '' },
  { label: '优先国语/简体 (chi)', value: 'chi' },
  { label: '优先粤语/繁体 (yue)', value: 'yue' },
  { label: '优先英语 (eng)', value: 'eng' },
  { label: '优先日语 (jpn)', value: 'jpn' }
];

const subTypeMap = {
  'effect': '特效字幕 (ASS / SSA / PGSSUB)',
  'chs_eng': '简英双语',
  'cht_eng': '繁英双语',
  'chs': '简体中文',
  'cht': '繁体中文'
};

const config = ref({
  audio_lang: '',
  audio_features: [],
  sub_priority: []
});

const getSubLabel = (id) => subTypeMap[id] || id;

const loadConfig = async () => {
  loading.value = true;
  try {
    const res = await axios.get('/api/p115/default_stream_config');
    if (res.data.success) {
      const data = res.data.data;
      config.value.audio_lang = data.audio_lang;
      config.value.audio_features = data.audio_features;
      // 将字符串数组转为 vuedraggable 需要的对象数组
      config.value.sub_priority = data.sub_priority.map(id => ({ id }));
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
      audio_features: config.value.audio_features,
      // 将对象数组还原为字符串数组
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
.priority-item {
  display: flex;
  align-items: center;
  padding: 10px 16px;
  border: 1px solid var(--n-divider-color);
  margin-bottom: 8px;
  border-radius: 6px;
  background-color: var(--n-action-color);
  transition: all 0.2s;
}
.priority-item:hover {
  border-color: var(--n-primary-color);
}
.drag-handle {
  cursor: grab;
  margin-right: 12px;
  color: #999;
}
.drag-handle:active {
  cursor: grabbing;
}
.item-name {
  flex: 1;
  font-weight: bold;
  font-size: 14px;
}
</style>