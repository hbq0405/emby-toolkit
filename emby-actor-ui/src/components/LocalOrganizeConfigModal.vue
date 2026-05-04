<!-- src/components/LocalOrganizeConfigModal.vue -->
<template>
  <n-modal v-model:show="show" preset="card" style="width: 600px;" title="本地文件整理配置" :bordered="false">
    <n-form ref="formRef" :model="form" label-placement="left" label-width="120">
      <n-form-item label="启用功能">
        <n-switch v-model:value="form.enabled" />
      </n-form-item>

      <n-form-item label="电影源目录">
        <n-input v-model:value="form.source_movie" placeholder="/path/to/movies" />
      </n-form-item>

      <n-form-item label="电视剧源目录">
        <n-input v-model:value="form.source_tv" placeholder="/path/to/tvshows" />
      </n-form-item>

      <n-form-item label="混合源目录">
        <n-input v-model:value="form.source_mixed" placeholder="/path/to/mixed" />
      </n-form-item>

      <n-form-item label="目标根目录">
        <n-input v-model:value="form.target_base" placeholder="/mnt/media" />
      </n-form-item>

      <n-form-item label="整理模式">
        <n-radio-group v-model:value="form.mode">
          <n-radio-button value="hardlink">硬链接</n-radio-button>
          <n-radio-button value="copy">复制</n-radio-button>
          <n-radio-button value="move">移动</n-radio-button>
        </n-radio-group>
      </n-form-item>

      <n-form-item label="自动刮削">
        <n-switch v-model:value="form.auto_scrape" />
      </n-form-item>

      <n-form-item label="并发数">
        <n-input-number v-model:value="form.max_workers" :min="1" :max="10" />
      </n-form-item>
    </n-form>

    <template #footer>
      <n-space justify="end">
        <n-button @click="show = false">取消</n-button>
        <n-button type="primary" @click="saveConfig" :loading="saving">保存</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, reactive, watch } from 'vue'
import { NModal, NForm, NFormItem, NSwitch, NInput, NInputNumber, NRadioGroup, NRadioButton, NSpace, NButton } from 'naive-ui'
import axios from 'axios'

const props = defineProps({
  show: Boolean
})

const emit = defineEmits(['update:show', 'saved'])

const formRef = ref(null)
const saving = ref(false)

const form = reactive({
  enabled: false,
  source_movie: '',
  source_tv: '',
  source_mixed: '',
  target_base: '',
  mode: 'hardlink',
  auto_scrape: true,
  max_workers: 5
})

watch(() => props.show, async (val) => {
  if (val) {
    await loadConfig()
  }
})

async function loadConfig() {
  try {
    const res = await axios.get('/api/local_organize/config')
    if (res.data.success) {
      Object.assign(form, res.data.data)
    }
  } catch (e) {
    console.error(e)
  }
}

async function saveConfig() {
  saving.value = true
  try {
    const res = await axios.post('/api/local_organize/config', form)
    if (res.data.success) {
      window.$message.success('配置已保存')
      emit('saved')
    } else {
      window.$message.error('保存失败')
    }
  } finally {
    saving.value = false
  }
}
</script>