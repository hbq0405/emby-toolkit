<!-- src/components/LocalOrganizePage.vue -->
<template>
  <n-layout content-style="padding: 24px;">
    <n-grid :x-gap="16" :y-gap="16" cols="1 s:2 m:4" responsive="screen" style="margin-bottom: 24px;">
      <n-gi><n-card class="stat-card" size="small"><n-statistic label="总处理记录"><template #prefix><n-icon :component="LayersIcon" color="#2080f0" /></template>{{ stats.total || 0 }}</n-statistic></n-card></n-gi>
      <n-gi><n-card class="stat-card" size="small"><n-statistic label="识别成功"><template #prefix><n-icon :component="CheckmarkCircleIcon" color="#18a058" /></template>{{ stats.success || 0 }}</n-statistic></n-card></n-gi>
      <n-gi><n-card class="stat-card" size="small"><n-statistic label="未识别"><template #prefix><n-icon :component="HelpCircleIcon" color="#f0a020" /></template>{{ stats.unrecognized || 0 }}</n-statistic></n-card></n-gi>
      <n-gi><n-card class="stat-card" size="small"><n-statistic label="本周处理"><template #prefix><n-icon :component="TrendingUpIcon" color="#2080f0" /></template>{{ stats.thisWeek || 0 }}</n-statistic></n-card></n-gi>
    </n-grid>

    <n-card :bordered="false" size="small">
      <template #header>
        <n-space align="center" justify="space-between" style="width: 100%;">
          <n-text strong style="font-size: 16px;">本地文件整理</n-text>
          <n-space>
            <n-button type="primary" @click="openConfigModal" size="small">
              <template #icon><n-icon :component="SettingsIcon" /></template>
              配置
            </n-button>
            <n-button v-if="!monitorRunning" type="success" @click="startMonitor" :loading="startingMonitor" size="small">
              <template #icon><n-icon :component="PlayIcon" /></template>
              启动监控
            </n-button>
            <n-button v-else type="warning" @click="stopMonitor" :loading="stoppingMonitor" size="small">
              <template #icon><n-icon :component="StopIcon" /></template>
              停止监控
            </n-button>
            <n-button type="primary" @click="startOrganize" :loading="organizing" size="small">
              <template #icon><n-icon :component="FlashIcon" /></template>
              立即整理
            </n-button>
          </n-space>
        </n-space>
      </template>

      <n-space style="margin-bottom: 20px;" align="center" justify="space-between">
        <n-space>
          <n-input v-model:value="searchQuery" placeholder="搜索原文件名..." clearable @keyup.enter="fetchRecords" @clear="fetchRecords" style="width: 280px;">
            <template #prefix><n-icon :component="SearchIcon" /></template>
          </n-input>
          <n-select v-model:value="statusFilter" :options="statusOptions" style="width: 120px;" @update:value="fetchRecords" />
        </n-space>
        <n-button type="primary" secondary @click="fetchRecords">
          <template #icon><n-icon :component="RefreshIcon" /></template>
          刷新
        </n-button>
      </n-space>

      <n-data-table
        :columns="columns"
        :data="records"
        :loading="loading"
        :pagination="paginationReactive"
        :bordered="false"
        striped
        size="small"
        :row-key="row => row.id"
      />
    </n-card>

    <LocalOrganizeConfigModal v-model:show="showConfigModal" @saved="onConfigSaved" />
  </n-layout>
</template>

<script setup>
import { ref, reactive, onMounted, h } from 'vue'
import { NButton, NDataTable, NSpace, NInput, NSelect, NCard, NGrid, NGi, NStatistic, NIcon, NText, NTag } from 'naive-ui'
import { Layers as LayersIcon, CheckmarkCircle as CheckmarkCircleIcon, HelpCircle as HelpCircleIcon, TrendingUp as TrendingUpIcon, Search as SearchIcon, Refresh as RefreshIcon, Play as PlayIcon, Stop as StopIcon, Flash as FlashIcon, Settings as SettingsIcon } from '@vicons/ionicons5'
import axios from 'axios'
import LocalOrganizeConfigModal from './LocalOrganizeConfigModal.vue'

const loading = ref(false)
const organizing = ref(false)
const startingMonitor = ref(false)
const stoppingMonitor = ref(false)
const showConfigModal = ref(false)
const monitorRunning = ref(false)

const searchQuery = ref('')
const statusFilter = ref('all')

const records = ref([])
const stats = reactive({ total: 0, success: 0, unrecognized: 0, thisWeek: 0 })

const paginationReactive = reactive({ page: 1, pageSize: 15 })

const statusOptions = [
  { label: '全部', value: 'all' },
  { label: '成功', value: 'success' },
  { label: '未识别', value: 'unrecognized' },
]

const columns = [
  { title: 'ID', key: 'id', width: 60 },
  { title: '原文件名', key: 'original_name', ellipsis: { tooltip: true } },
  { title: '新文件名', key: 'renamed_name', ellipsis: { tooltip: true } },
  { title: '状态', key: 'status', width: 80,
    render(row) {
      return h(NTag, { type: row.status === 'success' ? 'success' : 'warning', size: 'small' }, () => row.status === 'success' ? '成功' : '未识别')
    }
  },
  { title: 'TMDb', key: 'tmdb_id', width: 70 },
  { title: '分类', key: 'category_name', ellipsis: { tooltip: true } },
  { title: '处理时间', key: 'processed_at', width: 150,
    render(row) {
      return row.processed_at ? new Date(row.processed_at).toLocaleString() : '-'
    }
  },
]

async function fetchRecords() {
  loading.value = true
  try {
    const params = { page: paginationReactive.page, per_page: paginationReactive.pageSize }
    if (searchQuery.value) params.search = searchQuery.value
    if (statusFilter.value !== 'all') params.status = statusFilter.value
    
    const res = await axios.get('/api/local_organize/records', { params })
    if (res.data.success) {
      records.value = res.data.items || []
      stats.total = res.data.total
      stats.success = res.data.stats?.success || 0
      stats.unrecognized = res.data.stats?.unrecognized || 0
      stats.thisWeek = res.data.stats?.thisWeek || 0
    }
  } finally {
    loading.value = false
  }
}

async function fetchStatus() {
  try {
    const res = await axios.get('/api/local_organize/status')
    if (res.data.success) {
      monitorRunning.value = res.data.data?.monitor_running || false
    }
  } catch (e) {
    console.error(e)
  }
}

async function startOrganize() {
  organizing.value = true
  try {
    await axios.post('/api/local_organize/start')
    window.$message.success('任务已提交')
  } catch (e) {
    window.$message.error('提交失败')
  } finally {
    organizing.value = false
  }
}

async function startMonitor() {
  startingMonitor.value = true
  try {
    const res = await axios.post('/api/local_organize/monitor/start')
    if (res.data.success) {
      monitorRunning.value = true
      window.$message.success('监控已启动')
    } else {
      window.$message.warning(res.data.message || '启动失败')
    }
  } finally {
    startingMonitor.value = false
  }
}

async function stopMonitor() {
  stoppingMonitor.value = true
  try {
    const res = await axios.post('/api/local_organize/monitor/stop')
    if (res.data.success) {
      monitorRunning.value = false
      window.$message.success('监控已停止')
    }
  } finally {
    stoppingMonitor.value = false
  }
}

function openConfigModal() {
  showConfigModal.value = true
}

function onConfigSaved() {
  showConfigModal.value = false
}

onMounted(() => {
  fetchRecords()
  fetchStatus()
})
</script>

<style scoped>
.stat-card {
  text-align: center;
}
</style>