<!-- src/components/settings/WashingPriorityModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="阶梯洗版优先级配置" style="width: 1000px; max-width: 95vw;">
    <n-spin :show="loading">
      <n-layout has-sider style="height: 600px; border: 1px solid var(--n-divider-color); border-radius: 8px;">
        
        <!-- 左侧：规则组列表 -->
        <n-layout-sider width="240" bordered style="background: var(--n-color-modal);">
          <div style="padding: 12px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--n-divider-color);">
            <span style="font-weight: bold;">规则组</span>
            <n-button size="tiny" type="primary" ghost @click="addGroup">
              <template #icon><n-icon :component="AddIcon" /></template>
              新增
            </n-button>
          </div>
          
          <draggable v-model="groups" item-key="id" handle=".drag-handle" @end="saveGroups" style="padding: 8px;">
            <template #item="{ element: group, index }">
              <div 
                class="group-item" 
                :class="{ active: activeGroupId === group.id }"
                @click="activeGroupId = group.id"
              >
                <n-icon class="drag-handle" :component="MenuIcon" />
                <div class="group-info">
                  <div class="group-name">{{ group.name || '未命名规则组' }}</div>
                  <div class="group-desc">{{ group.media_type === 'Movie' ? '电影' : '剧集' }}</div>
                </div>
                <n-popconfirm @positive-click.stop="deleteGroup(index)">
                  <template #trigger>
                    <n-button size="tiny" text type="error" @click.stop><n-icon :component="TrashIcon" /></n-button>
                  </template>
                  确定删除此规则组吗？
                </n-popconfirm>
              </div>
            </template>
          </draggable>
        </n-layout-sider>

        <!-- 右侧：规则组详情编辑 -->
        <n-layout-content style="padding: 20px; background: var(--n-body-color);">
          <div v-if="activeGroup">
            <n-form label-placement="left" label-width="100">
              <n-grid :cols="2" :x-gap="24">
                <n-gi>
                  <n-form-item label="规则组名称">
                    <n-input v-model:value="activeGroup.name" placeholder="例如: 欧美电影" @blur="saveGroups" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="媒体类型">
                    <n-select v-model:value="activeGroup.media_type" :options="[{label:'电影', value:'Movie'}, {label:'剧集', value:'Series'}]" @update:value="saveGroups" />
                  </n-form-item>
                </n-gi>
                <n-gi span="2">
                  <n-form-item label="适用分类目录">
                    <n-select 
                      v-model:value="activeGroup.target_cids" 
                      multiple 
                      :options="categoryOptions" 
                      placeholder="留空则适用于所有目录" 
                      @update:value="saveGroups"
                    />
                  </n-form-item>
                </n-gi>
              </n-grid>
            </n-form>

            <n-divider style="margin: 12px 0;" />
            
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
              <span style="font-weight: bold; font-size: 16px;">优先级阶梯 (从上到下匹配)</span>
              <n-button size="small" type="primary" @click="addPriority">
                <template #icon><n-icon :component="AddIcon" /></template>
                添加优先级
              </n-button>
            </div>

            <!-- 优先级卡片列表 -->
            <draggable v-model="activeGroup.priorities" item-key="_uid" handle=".priority-drag-handle" @end="saveGroups" style="display: flex; flex-direction: column; gap: 16px;">
              <template #item="{ element: priority, index }">
                <n-card size="small" style="background: var(--n-action-color); border: 1px solid var(--n-divider-color);">
                  <template #header>
                    <div style="display: flex; align-items: center; gap: 8px;">
                      <n-icon class="priority-drag-handle" :component="MenuIcon" style="cursor: grab; color: #999;" />
                      <span style="font-weight: bold; color: var(--n-primary-color);">优先级 {{ index + 1 }}</span>
                    </div>
                  </template>
                  <template #header-extra>
                    <n-button size="tiny" text type="error" @click="deletePriority(index)">
                      <template #icon><n-icon :component="TrashIcon" /></template>
                    </n-button>
                  </template>
                  
                  <n-grid :cols="2" :x-gap="16" :y-gap="12">
                    <n-gi>
                      <n-select v-model:value="priority.resolution" multiple tag :options="resOptions" placeholder="分辨率 (如 4K, 1080p)" @update:value="saveGroups" />
                    </n-gi>
                    <n-gi>
                      <n-select v-model:value="priority.codec" multiple tag :options="codecOptions" placeholder="编码 (如 HEVC)" @update:value="saveGroups" />
                    </n-gi>
                    <n-gi>
                      <n-select v-model:value="priority.effect" multiple tag :options="effectOptions" placeholder="特效 (如 DoVi P8)" @update:value="saveGroups" />
                    </n-gi>
                    <n-gi>
                      <n-select v-model:value="priority.audio" multiple tag :options="audioOptions" placeholder="必须包含的音轨 (如 chi)" @update:value="saveGroups" />
                    </n-gi>
                    <n-gi>
                      <n-select v-model:value="priority.subtitle" multiple tag :options="subOptions" placeholder="必须包含的字幕 (如 chi)" @update:value="saveGroups" />
                    </n-gi>
                    <n-gi>
                      <!-- 占位，保持布局对称 -->
                    </n-gi>
                    <n-gi>
                      <n-input-group>
                        <n-input-group-label>最小体积</n-input-group-label>
                        <n-input-number v-model:value="priority.min_size_gb" :min="0" :step="1" placeholder="GB" @blur="saveGroups" style="width: 100%;" />
                      </n-input-group>
                    </n-gi>
                    <n-gi>
                      <n-input-group>
                        <n-input-group-label>最大体积</n-input-group-label>
                        <n-input-number v-model:value="priority.max_size_gb" :min="0" :step="1" placeholder="GB" @blur="saveGroups" style="width: 100%;" />
                      </n-input-group>
                    </n-gi>
                  </n-grid>
                </n-card>
              </template>
            </draggable>
            
            <n-empty v-if="!activeGroup.priorities || activeGroup.priorities.length === 0" description="暂无优先级规则，请添加" style="margin-top: 40px;" />

          </div>
          <n-empty v-else description="请在左侧选择或新建一个规则组" style="margin-top: 100px;" />
        </n-layout-content>
      </n-layout>
    </n-spin>
  </n-modal>
</template>

<script setup>
import { ref, computed } from 'vue';
import axios from 'axios';
import { c, useMessage } from 'naive-ui';
import draggable from 'vuedraggable';
import { Add as AddIcon, Menu as MenuIcon, TrashOutline as TrashIcon } from '@vicons/ionicons5';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);

const groups = ref([]);
const activeGroupId = ref(null);
const categoryOptions = ref([]);

const activeGroup = computed(() => groups.value.find(g => g.id === activeGroupId.value));

// 选项字典
const resOptions = [{label:'4K/2160p', value:'4k'}, {label:'1080p', value:'1080p'}, {label:'720p', value:'720p'}];
const codecOptions = [{label:'HEVC/H.265', value:'hevc'}, {label:'AVC/H.264', value:'avc'}];
const effectOptions = [{label:'DoVi P8', value:'dovi_p8'}, {label:'DoVi P7', value:'dovi_p7'}, {label:'DoVi P5', value:'dovi_p5'}, {label:'HDR10+', value:'hdr10+'}, {label:'HDR', value:'hdr'}, {label:'SDR', value:'sdr'}];
const audioOptions = [{label:'国语', value:'chi'}, {label:'英语', value:'eng'}, {label:'日语', value:'jpn'}, {label:'韩语', value:'kor'}];
const subOptions = [{label:'中文', value:'chi'}, {label:'英文', value:'eng'}, {label:'日文', value:'jpn'}, {label:'韩文', value:'kor'}];
const open = async () => {
  showModal.value = true;
  loading.value = true;
  try {
    // 1. 获取 115 分类目录供选择
    const resRules = await axios.get('/api/p115/sorting_rules');
    const rules = resRules.data.filter(r => r.enabled && r.cid && r.cid !== '0');
    categoryOptions.value = rules.map(r => ({ label: r.dir_name || r.name, value: r.cid }));

    // 2. 获取洗版优先级组
    const resGroups = await axios.get('/api/p115/washing_priority_groups');
    groups.value = resGroups.data.data || [];
    
    // 为 priorities 添加内部唯一 ID 供拖拽使用
    groups.value.forEach(g => {
      if (g.priorities) {
        g.priorities.forEach(p => p._uid = Math.random().toString(36).substr(2, 9));
      }
    });

    if (groups.value.length > 0) activeGroupId.value = groups.value[0].id;
  } catch (e) {
    message.error('加载数据失败');
  } finally {
    loading.value = false;
  }
};

const saveGroups = async () => {
  try {
    // 移除内部 _uid
    const payload = JSON.parse(JSON.stringify(groups.value));
    payload.forEach(g => {
      if (g.priorities) g.priorities.forEach(p => delete p._uid);
    });
    await axios.post('/api/p115/washing_priority_groups', payload);
  } catch (e) {
    message.error('保存失败');
  }
};

const addGroup = () => {
  const newGroup = {
    id: Date.now(), // 临时 ID
    name: '新规则组',
    media_type: 'Movie',
    target_cids: [],
    priorities: []
  };
  groups.value.push(newGroup);
  activeGroupId.value = newGroup.id;
  saveGroups();
};

const deleteGroup = (index) => {
  groups.value.splice(index, 1);
  if (groups.value.length > 0) activeGroupId.value = groups.value[0].id;
  else activeGroupId.value = null;
  saveGroups();
};

const addPriority = () => {
  if (!activeGroup.value) return;
  if (!activeGroup.value.priorities) activeGroup.value.priorities = [];
  activeGroup.value.priorities.push({
    _uid: Math.random().toString(36).substr(2, 9),
    resolution: [], codec: [], effect: [], audio: [], subtitle: [], min_size_gb: null, max_size_gb: null
  });
  saveGroups();
};

const deletePriority = (index) => {
  if (!activeGroup.value) return;
  activeGroup.value.priorities.splice(index, 1);
  saveGroups();
};

defineExpose({ open });
</script>

<style scoped>
.group-item {
  display: flex; align-items: center; padding: 10px 12px; cursor: pointer;
  border-radius: 6px; margin-bottom: 4px; transition: background 0.2s;
}
.group-item:hover { background: var(--n-hover-color); }
.group-item.active { background: var(--n-primary-color-suppl); }
.drag-handle { cursor: grab; margin-right: 8px; color: #999; }
.group-info { flex: 1; overflow: hidden; }
.group-name { font-size: 14px; font-weight: bold; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.group-desc { font-size: 12px; color: var(--n-text-color-3); }
</style>