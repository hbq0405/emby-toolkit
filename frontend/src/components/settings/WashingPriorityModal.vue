<!-- src/components/settings/WashingPriorityModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="阶梯洗版优先级配置" style="width: 1000px; max-width: 95vw;" class="custom-modal glass-modal">
    <n-spin :show="loading">
      <div class="washing-mode-panel">
        <n-form label-placement="left" size="small">
          <n-form-item label="同集/同电影覆盖模式">
            <n-radio-group v-model:value="config.conflict_mode">
              <div class="mode-options">
                <n-radio value="replace" class="mode-radio">
                  <span class="mode-title">洗版</span>
                  <n-tooltip trigger="hover" placement="top" style="max-width: 320px;">
                    <template #trigger>
                      <button type="button" class="mode-help" @click.stop>?</button>
                    </template>
                    删除目标目录中同一集/同一电影的旧版本，移入新版本，并按下方优先级规则评估资源。
                  </n-tooltip>
                </n-radio>
                <n-radio value="keep_both" class="mode-radio">
                  <span class="mode-title">共存</span>
                  <n-tooltip trigger="hover" placement="top" style="max-width: 320px;">
                    <template #trigger>
                      <button type="button" class="mode-help" @click.stop>?</button>
                    </template>
                    只要文件名不同，同一集的不同版本将共存，不使用洗版优先级规则。
                  </n-tooltip>
                </n-radio>
                <n-radio value="skip" class="mode-radio">
                  <span class="mode-title">跳过</span>
                  <n-tooltip trigger="hover" placement="top" style="max-width: 320px;">
                    <template #trigger>
                      <button type="button" class="mode-help" @click.stop>?</button>
                    </template>
                    按下方跳过范围判断是否已有该集/该电影；命中后新文件直接丢入未识别，不使用洗版优先级规则。
                  </n-tooltip>
                </n-radio>
              </div>
            </n-radio-group>
          </n-form-item>
          <n-form-item v-if="config.conflict_mode === 'skip'" label="跳过范围">
            <n-radio-group v-model:value="config.skip_scope">
              <div class="mode-options compact">
                <n-radio value="directory" class="mode-radio">
                  <span class="mode-title">同目录</span>
                  <n-tooltip trigger="hover" placement="top" style="max-width: 320px;">
                    <template #trigger>
                      <button type="button" class="mode-help" @click.stop>?</button>
                    </template>
                    仅目标目录中已有同集/同电影时跳过，当前默认逻辑。
                  </n-tooltip>
                </n-radio>
                <n-radio value="library" class="mode-radio">
                  <span class="mode-title">全库</span>
                  <n-tooltip trigger="hover" placement="top" style="max-width: 320px;">
                    <template #trigger>
                      <button type="button" class="mode-help" @click.stop>?</button>
                    </template>
                    只要媒体库已存在同集/同电影就跳过，不限定当前目标目录。
                  </n-tooltip>
                </n-radio>
              </div>
            </n-radio-group>
          </n-form-item>
        </n-form>
      </div>

      <n-layout v-if="config.conflict_mode === 'replace'" has-sider style="height: 650px; border: 1px solid var(--n-divider-color); border-radius: 8px;">
        
        <!-- 左侧：规则组列表 -->
        <n-layout-sider width="240" bordered style="background: var(--n-color-modal);">
          <div style="padding: 12px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--n-divider-color);">
            <span style="font-weight: bold;">规则组</span>
            <n-button size="tiny" type="primary" ghost @click="addGroup">
              <template #icon><n-icon :component="AddIcon" /></template>
              新增
            </n-button>
          </div>
          
          <draggable v-model="groups" item-key="id" handle=".drag-handle" style="padding: 8px;">
            <template #item="{ element: group, index }">
              <div 
                class="group-item" 
                :class="{ active: activeGroupId === group.id }"
                @click="switchGroup(group.id)"
              >
                <n-icon class="drag-handle" :component="MenuIcon" />
                <div class="group-info">
                  <div class="group-name">{{ group.name || '未命名规则组' }}</div>
                  <div class="group-desc">{{ group.media_type === 'All' ? '通用 (电影+剧集)' : (group.media_type === 'Movie' ? '电影' : '剧集') }}</div>
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
        <n-layout-content style="padding: 20px; background: var(--n-body-color); display: flex; flex-direction: column;">
          <div v-if="activeGroup" style="flex: 1; display: flex; flex-direction: column;">
            <!-- 顶部：规则组基础配置 -->
            <n-form label-placement="left" label-width="100">
              <n-grid :cols="2" :x-gap="24">
                <n-gi>
                  <n-form-item label="规则组名称">
                    <n-input v-model:value="activeGroup.name" placeholder="例如: 欧美电影" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="媒体类型">
                    <n-select 
                      v-model:value="activeGroup.media_type" 
                      :options="[{label:'通用 (电影+剧集)', value:'All'}, {label:'电影', value:'Movie'}, {label:'剧集', value:'Series'}]" 
                    />
                  </n-form-item>
                </n-gi>
                <n-gi span="2">
                  <n-form-item label="适用分类">
                    <n-select 
                      v-model:value="activeGroup.target_cids" 
                      multiple 
                      :options="categoryOptions" 
                      placeholder="留空则适用于所有分类" 
                    />
                  </n-form-item>
                </n-gi>
              </n-grid>
            </n-form>

            <n-divider style="margin: 4px 0 16px 0;" />
            
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
              <span style="font-weight: bold; font-size: 16px;">优先级阶梯 (从上到下匹配)</span>
              <n-button size="small" type="primary" @click="addPriority">
                <template #icon><n-icon :component="AddIcon" /></template>
                添加优先级
              </n-button>
            </div>

            <!-- 优先级卡片列表 (可滚动区域) -->
            <div style="flex: 1; overflow-y: auto; padding-right: 8px; margin-right: -8px;">
              <draggable v-model="activeGroup.priorities" item-key="_uid" handle=".priority-drag-handle" style="display: flex; flex-direction: column; gap: 12px; padding-bottom: 20px;">
                <template #item="{ element: priority, index }">
                  <n-card 
                    size="small" 
                    :style="{ 
                      borderColor: editingUid === priority._uid ? 'var(--n-primary-color)' : (priority.is_exclude ? 'var(--n-error-color)' : 'var(--n-divider-color)'),
                      boxShadow: editingUid === priority._uid ? '0 0 0 1px var(--n-primary-color)' : 'none',
                      backgroundColor: priority.is_exclude ? 'rgba(208, 48, 80, 0.03)' : 'var(--n-action-color)'
                    }"
                    style="transition: all 0.3s;"
                  >
                    <!-- 卡片头部 -->
                    <template #header>
                      <div style="display: flex; align-items: center; gap: 8px;">
                        <n-icon class="priority-drag-handle" :component="MenuIcon" style="cursor: grab; color: #999;" />
                        <span :style="{ fontWeight: 'bold', color: priority.is_exclude ? 'var(--n-error-color)' : 'var(--n-primary-color)' }">
                          {{ priority.is_exclude ? '排除规则' : '优先级 ' + (index + 1) }}
                        </span>
                      </div>
                    </template>
                    <template #header-extra>
                      <n-space :size="8">
                        <n-button v-if="editingUid !== priority._uid" size="tiny" secondary type="primary" @click="editPriority(priority._uid)">
                          <template #icon><n-icon :component="EditIcon" /></template>
                          编辑
                        </n-button>
                        <n-button v-else size="tiny" type="primary" @click="finishEdit">
                          <template #icon><n-icon :component="CheckIcon" /></template>
                          完成
                        </n-button>
                        <n-popconfirm @positive-click="deletePriority(index)">
                          <template #trigger>
                            <n-button size="tiny" text type="error">
                              <template #icon><n-icon :component="TrashIcon" /></template>
                            </n-button>
                          </template>
                          确定删除此规则吗？
                        </n-popconfirm>
                      </n-space>
                    </template>
                    
                    <!-- 视图模式：简略 Tag 展示 -->
                    <div v-if="editingUid !== priority._uid" class="summary-view" @click="editPriority(priority._uid)">
                      <n-space :size="[8, 8]">
                        <n-tag v-for="(tag, tIdx) in getPrioritySummary(priority)" :key="tIdx" :type="tag.type" size="small" round>
                          <template v-if="tag.icon" #icon><n-icon :component="tag.icon" /></template>
                          {{ tag.label }}
                        </n-tag>
                        <n-text v-if="getPrioritySummary(priority).length === (priority.is_exclude ? 1 : 0)" depth="3" style="font-size: 13px; font-style: italic;">
                          未配置任何条件，保存时将被自动忽略
                        </n-text>
                      </n-space>
                    </div>

                    <!-- 编辑模式：完整表单 -->
                    <div v-else class="edit-view">
                      <n-grid :cols="2" :x-gap="16" :y-gap="12">
                        <n-gi span="2">
                          <n-alert v-if="priority.is_exclude" type="error" :show-icon="false" style="margin-bottom: 8px; padding: 8px 12px;">
                            <div style="display: flex; align-items: center; justify-content: space-between;">
                              <span style="font-weight: bold;">⛔ 排除模式：命中以下任意条件的资源将被直接丢弃，不再参与后续洗版。</span>
                              <n-switch v-model:value="priority.is_exclude">
                                <template #checked>排除模式</template>
                                <template #unchecked>普通模式</template>
                              </n-switch>
                            </div>
                          </n-alert>
                          <div v-else style="display: flex; justify-content: flex-end; margin-bottom: 8px;">
                            <n-switch v-model:value="priority.is_exclude">
                              <template #checked>排除模式</template>
                              <template #unchecked>设为排除规则</template>
                            </n-switch>
                          </div>
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.resolution" multiple tag :options="resOptions" placeholder="分辨率 (如 4K, 1080p)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.codec" multiple tag :options="codecOptions" placeholder="编码 (如 HEVC)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.source" multiple tag :options="sourceOptions" placeholder="来源 (如 WEB-DL)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.effect" multiple tag :options="effectOptions" placeholder="特效 (如 DoVi P8)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.audio" multiple tag :options="audioOptions" placeholder="必须包含的音轨 (如 chi)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.subtitle" multiple tag :options="subOptions" placeholder="必须包含的字幕 (如 chi)" />
                        </n-gi>
                        <n-gi>
                          <n-select v-model:value="priority.release_group" multiple tag filterable :options="releaseGroupOptions" placeholder="发布组 (如 观众、天空)" />
                        </n-gi>
                        <n-gi span="2">
                          <div class="switch-row">
                            <div class="switch-item">
                              <n-switch v-model:value="priority.subtitle_effect" size="small" />
                              <span><strong>特效字幕</strong></span>
                            </div>
                            <div class="switch-item">
                            <n-switch v-model:value="priority.exempt_original_lang" size="small" />
                              <span><strong>原产国豁免音轨/字幕规则</strong></span>
                            </div>
                            <div class="switch-item">
                            <n-switch v-model:value="priority.clean_version" size="small" />
                              <span><strong>纯净版</strong></span>
                            </div>
                          </div>
                        </n-gi>
                        <n-gi>
                          <n-input-group>
                            <n-input-group-label>最小体积</n-input-group-label>
                            <n-input-number v-model:value="priority.min_size_gb" :min="0" :step="1" placeholder="GB" style="width: 100%;" />
                          </n-input-group>
                        </n-gi>
                        <n-gi>
                          <n-input-group>
                            <n-input-group-label>最大体积</n-input-group-label>
                            <n-input-number v-model:value="priority.max_size_gb" :min="0" :step="1" placeholder="GB" style="width: 100%;" />
                          </n-input-group>
                        </n-gi>
                      </n-grid>
                    </div>
                  </n-card>
                </template>
              </draggable>
              
              <n-empty v-if="!activeGroup.priorities || activeGroup.priorities.length === 0" description="暂无优先级规则，请添加" style="margin-top: 40px;" />
            </div>

          </div>
          <n-empty v-else description="请在左侧选择或新建一个规则组" style="margin-top: 100px;" />
        </n-layout-content>
      </n-layout>

      <n-alert v-else type="info" :show-icon="false" style="margin-top: 12px;">
        当前覆盖模式不执行洗版，优先级规则已隐藏。切回“洗版”后可继续编辑和重算优先级。
      </n-alert>
    </n-spin>
    
    <!-- 底部操作栏 -->
    <template #action>
      <n-space justify="space-between" style="width: 100%;">
        <n-button v-if="config.conflict_mode === 'replace'" secondary type="warning" :loading="recalcLoading" @click="confirmRecalculate">
          一键重算媒体库优先级
        </n-button>
        <span v-else></span>
        <n-space>
          <n-button @click="showModal = false">取消</n-button>
          <n-button type="primary" :loading="loading" @click="saveGroups">保存配置</n-button>
        </n-space>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref, computed } from 'vue';
import axios from 'axios';
import { useDialog, useMessage } from 'naive-ui';
import draggable from 'vuedraggable';
import { 
  Add as AddIcon, 
  Menu as MenuIcon, 
  TrashOutline as TrashIcon,
  CreateOutline as EditIcon,
  CheckmarkOutline as CheckIcon,
  BanOutline as BanIcon
} from '@vicons/ionicons5';

const message = useMessage();
const dialog = useDialog();
const showModal = ref(false);
const loading = ref(false);
const recalcLoading = ref(false);

const groups = ref([]);
const activeGroupId = ref(null);
const categoryOptions = ref([]);
const releaseGroupOptions = ref([]);
const config = ref({
  conflict_mode: 'replace',
  skip_scope: 'directory'
});

// 当前正在编辑的优先级卡片 UID
const editingUid = ref(null);

const activeGroup = computed(() => groups.value.find(g => g.id === activeGroupId.value));

// 选项字典
const resOptions = [{label:'4K/2160p', value:'4k'}, {label:'1080p', value:'1080p'}, {label:'720p', value:'720p'}];
const codecOptions = [{label:'HEVC/H.265', value:'hevc'}, {label:'AVC/H.264', value:'avc'}];
const sourceOptions = [
  { label: '蓝光原盘', value: 'BluRay原盘' },
  { label: 'Remux', value: 'Remux' },
  { label: 'UHD BluRay', value: 'UHD BluRay' },
  { label: 'BluRay', value: 'BluRay' },
  { label: 'UHD', value: 'UHD' },
  { label: 'WEB-DL', value: 'WEB-DL' },
  { label: 'HDTV', value: 'HDTV' }
];
const effectOptions = [{label:'DoVi P8', value:'dovi_p8'}, {label:'DoVi P7', value:'dovi_p7'}, {label:'DoVi P5', value:'dovi_p5'}, {label:'HDR10+', value:'hdr10+'}, {label:'HDR', value:'hdr'}, {label:'SDR', value:'sdr'}];
const audioOptions = [{label:'国语', value:'chi'}, {label:'粤语', value:'yue'}, {label:'英语', value:'eng'}, {label:'日语', value:'jpn'}, {label:'韩语', value:'kor'}];
const subOptions = [{label:'简体', value:'chi'}, {label:'繁体', value:'yue'}, {label:'英文', value:'eng'}, {label:'日文', value:'jpn'}, {label:'韩文', value:'kor'}];

// 辅助函数：将 value 转换为 label
const getLabels = (values, options) => {
  if (!values || !values.length) return [];
  return values.map(v => {
    const opt = options.find(o => o.value === v);
    return opt ? opt.label : v;
  });
};

// 生成简略展示的 Tag 列表
const getPrioritySummary = (p) => {
  const tags = [];
  
  if (p.is_exclude) {
    tags.push({ type: 'error', label: '排除以下条件', icon: BanIcon });
  }
  
  const resLabels = getLabels(p.resolution, resOptions);
  if (resLabels.length) tags.push({ type: 'success', label: resLabels.join(' | ') });
  
  const codecLabels = getLabels(p.codec, codecOptions);
  if (codecLabels.length) tags.push({ type: 'info', label: codecLabels.join(' | ') });

  const sourceLabels = getLabels(p.source, sourceOptions);
  if (sourceLabels.length) tags.push({ type: 'primary', label: '源: ' + sourceLabels.join(' | ') });
  
  const effectLabels = getLabels(p.effect, effectOptions);
  if (effectLabels.length) tags.push({ type: 'warning', label: effectLabels.join(' | ') });
  
  const audioLabels = getLabels(p.audio, audioOptions);
  if (audioLabels.length) tags.push({ type: 'error', label: '音: ' + audioLabels.join(', ') });
  
  const subLabels = getLabels(p.subtitle, subOptions);
  if (subLabels.length) tags.push({ type: 'default', label: '字: ' + subLabels.join(', ') });

  const releaseGroupValues = Array.isArray(p.release_group) ? p.release_group : (p.release_group ? [p.release_group] : []);
  const releaseGroupLabels = getLabels(releaseGroupValues, releaseGroupOptions.value);
  if (releaseGroupLabels.length) tags.push({ type: 'info', label: '组: ' + releaseGroupLabels.join(', ') });

  if (p.subtitle_effect) tags.push({ type: 'warning', label: '特效字幕' });
  if (p.clean_version) tags.push({ type: 'success', label: '纯净版' });
  
  let sizeStr = '';
  if (p.is_exclude) {
    if (p.min_size_gb && p.max_size_gb) sizeStr = `< ${p.min_size_gb}G 或 > ${p.max_size_gb}G`;
    else if (p.min_size_gb) sizeStr = `< ${p.min_size_gb}G`;
    else if (p.max_size_gb) sizeStr = `> ${p.max_size_gb}G`;
  } else {
    if (p.min_size_gb && p.max_size_gb) sizeStr = `${p.min_size_gb}G - ${p.max_size_gb}G`;
    else if (p.min_size_gb) sizeStr = `> ${p.min_size_gb}G`;
    else if (p.max_size_gb) sizeStr = `< ${p.max_size_gb}G`;
  }
  
  if (sizeStr) tags.push({ type: 'primary', label: sizeStr });

  return tags;
};

const open = async () => {
  showModal.value = true;
  loading.value = true;
  editingUid.value = null; // 重置编辑状态
  try {
    // 1. 获取 115 分类目录供选择
    const resRules = await axios.get('/api/p115/sorting_rules');
    const rules = resRules.data.filter(r => r.enabled && r.cid && r.cid !== '0');
    categoryOptions.value = rules.map(r => ({ label: r.dir_name || r.name, value: r.cid }));

    const resReleaseGroups = await axios.get('/api/p115/release_groups');
    releaseGroupOptions.value = resReleaseGroups.data?.data || [];

    // 2. 获取洗版覆盖模式配置
    const resConfig = await axios.get('/api/p115/washing_priority_config');
    config.value = {
      conflict_mode: resConfig.data?.data?.conflict_mode || 'replace',
      skip_scope: resConfig.data?.data?.skip_scope || 'directory'
    };

    // 3. 获取洗版优先级组
    const resGroups = await axios.get('/api/p115/washing_priority_groups');
    groups.value = resGroups.data.data || [];
    
    // 为 priorities 添加内部唯一 ID 供拖拽和编辑状态使用
    groups.value.forEach(g => {
      if (g.priorities) {
        g.priorities.forEach(p => {
          p._uid = Math.random().toString(36).substr(2, 9);
          p.source = Array.isArray(p.source) ? p.source : (p.source ? [p.source] : []);
          p.release_group = Array.isArray(p.release_group) ? p.release_group : (p.release_group ? [p.release_group] : []);
        });
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
    loading.value = true;
    const payload = JSON.parse(JSON.stringify(groups.value));
    
    // 过滤空规则并移除内部 _uid
    payload.forEach(g => {
      if (g.priorities) {
        g.priorities = g.priorities.filter(p => {
          // 只要配置了任意一项条件，就认为是有效规则
          return (p.resolution && p.resolution.length > 0) ||
                 (p.source && p.source.length > 0) ||
                 (p.codec && p.codec.length > 0) ||
                 (p.effect && p.effect.length > 0) ||
                 (p.audio && p.audio.length > 0) ||
                 (p.subtitle && p.subtitle.length > 0) ||
                 (p.release_group && p.release_group.length > 0) ||
                 p.subtitle_effect ||
                 p.clean_version ||
                 (p.min_size_gb !== null && p.min_size_gb !== undefined) ||
                 (p.max_size_gb !== null && p.max_size_gb !== undefined);
        });
        g.priorities.forEach(p => delete p._uid);
      }
    });

    await axios.post('/api/p115/washing_priority_config', config.value);
    await axios.post('/api/p115/washing_priority_groups', payload);
    message.success('保存成功');
    if (config.value.conflict_mode !== 'replace') {
      showModal.value = false;
      return;
    }
    dialog.warning({
      title: '建议重算媒体库优先级',
      content: '洗版优先级规则已经保存。若规则有调整，旧媒体项记录的优先级可能已经过期，建议立即重算。',
      positiveText: '立即重算',
      negativeText: '稍后',
      onPositiveClick: async () => {
        await triggerRecalculate();
        showModal.value = false;
      },
      onNegativeClick: () => {
        showModal.value = false;
      }
    });
  } catch (e) {
    message.error('保存失败');
  } finally {
    loading.value = false;
  }
};


const triggerRecalculate = async () => {
  try {
    recalcLoading.value = true;
    const res = await axios.post('/api/p115/washing_priority_recalculate', {
      item_type: 'all',
      background: true
    });
    if (res.data?.success) {
      message.success(res.data.message || '洗版优先级重算任务已启动');
    } else {
      message.error(res.data?.message || '启动重算任务失败');
      throw new Error(res.data?.message || '启动重算任务失败');
    }
  } catch (e) {
    const msg = e.response?.data?.message || e.message || '启动重算任务失败';
    message.error(msg);
    throw e;
  } finally {
    recalcLoading.value = false;
  }
};

const confirmRecalculate = () => {
  dialog.warning({
    title: '重算媒体库洗版优先级',
    content: '将扫描当前已入库的电影和分集，按当前已保存的洗版优先级规则重新计算 washing_level，并写回 115 文件缓存与媒体元数据。大库会在后台执行。确认开始？',
    positiveText: '开始重算',
    negativeText: '取消',
    onPositiveClick: triggerRecalculate
  });
};

const switchGroup = (id) => {
  activeGroupId.value = id;
  editingUid.value = null; // 切换组时收起所有编辑面板
};

const addGroup = () => {
  const newGroup = {
    id: Date.now(),
    name: '新规则组',
    media_type: 'All',
    target_cids: [],
    priorities: []
  };
  groups.value.push(newGroup);
  switchGroup(newGroup.id);
};

const deleteGroup = (index) => {
  groups.value.splice(index, 1);
  if (groups.value.length > 0) switchGroup(groups.value[0].id);
  else switchGroup(null);
};

const addPriority = () => {
  if (!activeGroup.value) return;
  if (!activeGroup.value.priorities) activeGroup.value.priorities = [];
  
  const newUid = Math.random().toString(36).substr(2, 9);
  activeGroup.value.priorities.push({
    _uid: newUid,
    is_exclude: false,
    exempt_original_lang: false,
    clean_version: false,
    subtitle_effect: false,
    resolution: [], source: [], codec: [], effect: [], audio: [], subtitle: [], release_group: [], min_size_gb: null, max_size_gb: null
  });
  
  // 新增后自动展开编辑
  editingUid.value = newUid;
  
  // 延迟一下滚动到底部
  setTimeout(() => {
    const container = document.querySelector('.n-layout-content > div > div:last-child');
    if (container) container.scrollTop = container.scrollHeight;
  }, 100);
};

const editPriority = (uid) => {
  editingUid.value = uid;
};

const finishEdit = () => {
  editingUid.value = null;
};

const deletePriority = (index) => {
  if (!activeGroup.value) return;
  const deletedUid = activeGroup.value.priorities[index]._uid;
  if (editingUid.value === deletedUid) editingUid.value = null;
  
  activeGroup.value.priorities.splice(index, 1);
};

defineExpose({ open });
</script>

<style scoped>
.washing-mode-panel {
  margin-bottom: 12px;
  padding: 12px 14px;
  background: rgba(0, 0, 0, 0.02);
  border: 1px solid var(--n-divider-color);
  border-radius: 8px;
}

.mode-options {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 24px;
}

.mode-radio :deep(.n-radio__label) {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.mode-title {
  font-weight: 600;
}

.mode-help {
  width: 16px;
  height: 16px;
  padding: 0;
  border: 1px solid var(--n-border-color);
  border-radius: 50%;
  background: transparent;
  color: var(--n-text-color-3);
  cursor: help;
  font-size: 11px;
  line-height: 14px;
  text-align: center;
}

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

.summary-view {
  padding: 8px 4px;
  cursor: pointer;
  border-radius: 4px;
  transition: background-color 0.2s;
}
.summary-view:hover {
  background-color: var(--n-hover-color);
}
.edit-view {
  padding: 8px 0;
}

.switch-row {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  margin-top: 4px;
}

.switch-item {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 34px;
  padding: 8px;
  background: var(--n-color-modal);
  border-radius: 4px;
  color: var(--n-text-color-3);
  font-size: 12px;
}

.switch-item span {
  min-width: 0;
  line-height: 1.25;
}
</style>
