<template>
  <n-layout content-style="padding: 24px;">
    <n-spin :show="isLoading">
      <div class="cover-generator-config">
        <n-page-header>
          <template #title>媒体库封面生成</template>
          <template #extra>
            <n-space>
              <n-button @click="runGenerateAllTask" :loading="isGenerating">
                <template #icon><n-icon :component="ImagesIcon" /></template>
                立即生成所有媒体库封面
              </n-button>
              <n-button type="primary" @click="saveConfig" :loading="isSaving">
                <template #icon><n-icon :component="SaveIcon" /></template>
                保存设置
              </n-button>
            </n-space>
          </template>
          <n-alert title="操作提示" type="info" style="margin-top: 24px;">
          本功能提取自MP插件，感谢作者<a
                  href="https://github.com/justzerock/MoviePilot-Plugins/"
                  target="_blank"
                  style="font-size: 0.85em; margin-left: 8px; color: var(--n-primary-color); text-decoration: underline;"
                >justzerock</a><br />
          开启监控新入库可实时生成封面，包括原生媒体库、自建合集。
        </n-alert>
        </n-page-header>

        <!-- ★★★ 核心修改：使用 n-grid 重新排版 ★★★ -->
        <n-card class="content-card, dashboard-card" style="margin-top: 24px;">
          <template #header>
            <!-- 将 card-title 类应用到标题文本的容器上 -->
            <span class="card-title">基础设置</span>
          </template>
          <n-grid :cols="4" :x-gap="24" :y-gap="16" responsive="screen"> <!-- 建议加一个 y-gap -->
            <!-- 第一列 -->
            <n-gi>
              <n-form-item label="启用">
                <n-switch v-model:value="configData.enabled" />
              </n-form-item>
            </n-gi>
            <!-- 第二列 -->
            <n-gi>
              <n-form-item label="监控新入库">
                <n-switch v-model:value="configData.transfer_monitor" />
                <template #feedback>新媒体入库后自动更新所在库封面</template>
              </n-form-item>
            </n-gi>
            <!-- 第三列 -->
            <n-gi>
              <n-form-item label="在封面上显示媒体统计数字">
                <n-switch v-model:value="configData.show_item_count" />
                <template #feedback>在封面左上角显示媒体项总数</template>
              </n-form-item>
            </n-gi>
            <!-- 第四列 -->
            <n-gi>
              <n-form-item label="封面图片来源排序">
                <n-select v-model:value="configData.sort_by" :options="sortOptions" />
              </n-form-item>
            </n-gi>

            <!-- ★★★ 新增的分割线 ★★★ -->
            <n-gi :span="4">
              <n-divider style="margin-top: 8px; margin-bottom: 8px;" />
            </n-gi>
            
            <!-- 忽略媒体库部分 -->
            <n-gi :span="4"> <!-- ★ 确保这里也是 span="4" -->
              <n-form-item label="选择要【忽略】的媒体库">
                <n-checkbox-group 
                  v-model:value="configData.exclude_libraries"
                  style="display: flex; flex-wrap: wrap; gap: 8px 16px;"
                >
                  <n-checkbox 
                    v-for="lib in libraryOptions" 
                    :key="lib.value" 
                    :value="lib.value" 
                    :label="lib.label" 
                  />
                </n-checkbox-group>
              </n-form-item>
            </n-gi>
          </n-grid>
          <div v-if="configData.show_item_count" style="margin-top: 16px;">
          <n-divider /> <!-- 一条分割线，让界面更清晰 -->
          <n-grid :cols="2" :x-gap="24">
            <!-- 子选项1：样式选择 -->
            <n-gi>
              <n-form-item label="数字样式">
                <n-radio-group v-model:value="configData.badge_style">
                  <n-radio-button value="badge">徽章</n-radio-button>
                  <n-radio-button value="ribbon">缎带</n-radio-button>
                </n-radio-group>
              </n-form-item>
            </n-gi>
            <!-- 子选项2：大小滑块 -->
            <n-gi>
              <n-form-item label="数字大小">
                <n-slider 
                  v-model:value="configData.badge_size_ratio" 
                  :step="0.01" 
                  :min="0.08" 
                  :max="0.20" 
                  :format-tooltip="value => `${(value * 100).toFixed(0)}%`"
                />
              </n-form-item>
            </n-gi>
          </n-grid>
        </div>
        </n-card>

        <!-- ... 其余的 n-card 和 n-tabs 保持不变 ... -->
        <n-card class="content-card, dashboard-card" style="margin-top: 24px;">
          <n-tabs v-model:value="configData.tab" type="line" animated>
            <n-tab-pane name="style-tab" tab="封面风格">
              <n-spin :show="isPreviewLoading"> <!-- 添加一个加载动画，提升体验 -->
                <n-radio-group v-model:value="configData.cover_style" name="cover-style-group">
                  <n-grid :cols="3" :x-gap="16" :y-gap="16" responsive="screen">
                    <!-- 【【【关键修改：src 绑定到动态的 ref】】】 -->
                    <n-gi v-for="style in styles" :key="style.value">
                      <n-card class="dashboard-card style-card">
                        <template #cover><img :src="stylePreviews[style.value]" class="style-img" /></template>
                        <n-radio :value="style.value" :label="style.title" />
                      </n-card>
                    </n-gi>
                  </n-grid>
                </n-radio-group>
              </n-spin>
            </n-tab-pane>

            <n-tab-pane name="title-tab" tab="封面标题">
              <n-space vertical>
                <!-- 表头，用于引导用户 -->
                <n-grid :cols="10" :x-gap="12" style="padding: 0 8px; margin-bottom: 4px;">
                  <n-gi :span="3"><span style="font-weight: 500;">媒体库名称</span></n-gi>
                  <n-gi :span="3"><span style="font-weight: 500;">中文标题</span></n-gi>
                  <n-gi :span="3"><span style="font-weight: 500;">英文标题</span></n-gi>
                  <n-gi :span="1"></n-gi> <!-- 操作区占位 -->
                </n-grid>

                <!-- 动态表单项 -->
                <div v-for="(item, index) in titleConfigs" :key="item.id">
                  <n-grid :cols="10" :x-gap="12" :y-gap="8">
                    <n-gi :span="3">
                      <n-input v-model:value="item.library" placeholder="与媒体库名称完全一致" />
                    </n-gi>
                    <n-gi :span="3">
                      <n-input v-model:value="item.zh" placeholder="封面上显示的中文" />
                    </n-gi>
                    <n-gi :span="3">
                      <n-input v-model:value="item.en" placeholder="封面上显示的英文" />
                    </n-gi>
                    <n-gi :span="1" style="display: flex; align-items: center;">
                      <n-button type="error" dashed @click="removeTitleConfig(index)">
                        <template #icon><n-icon :component="TrashIcon" /></template>
                      </n-button>
                    </n-gi>
                  </n-grid>
                </div>

                <!-- 操作按钮 -->
                <n-button @click="addTitleConfig" type="primary" dashed style="margin-top: 16px;">
                  <template #icon><n-icon :component="AddIcon" /></template>
                  新增配置
                </n-button>
              </n-space>
            </n-tab-pane>

            <n-tab-pane name="single-tab" tab="单图风格设置">
              <n-alert type="info" :bordered="false" style="margin-bottom: 20px;">
                若字体无法下载，建议在主程序的网络设置中配置GitHub代理，或手动下载字体后填写本地路径。
              </n-alert>
              <n-grid :cols="2" :x-gap="24" :y-gap="12" responsive="screen">
                <n-gi>
                  <n-form-item label="中文字体（本地路径）">
                    <n-input v-model:value="configData.zh_font_path_local" placeholder="留空使用预设字体" />
                    <template #feedback>本地路径优先于下载链接</template>
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体（本地路径）">
                    <n-input v-model:value="configData.en_font_path_local" placeholder="留空使用预设字体" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="中文字体（下载链接）">
                    <n-input v-model:value="configData.zh_font_url" placeholder="留空使用预设字体" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体（下载链接）">
                    <n-input v-model:value="configData.en_font_url" placeholder="留空使用预设字体" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="中文字体大小比例">
                    <n-input-number v-model:value="configData.zh_font_size" :step="0.1" placeholder="1.0" />
                    <template #feedback>相对于预设尺寸的比例，1为原始大小</template>
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体大小比例">
                    <n-input-number v-model:value="configData.en_font_size" :step="0.1" placeholder="1.0" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="背景模糊程度">
                    <n-input-number v-model:value="configData.blur_size" placeholder="50" />
                    <template #feedback>数字越大越模糊，默认 50</template>
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="背景颜色混合占比">
                    <n-input-number v-model:value="configData.color_ratio" :step="0.1" placeholder="0.8" />
                     <template #feedback>颜色所占的比例，0-1，默认 0.8</template>
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="优先使用海报图">
                    <n-switch v-model:value="configData.single_use_primary" />
                    <template #feedback>不启用则优先使用背景图</template>
                  </n-form-item>
                </n-gi>
              </n-grid>
            </n-tab-pane>

            <n-tab-pane name="multi-1-tab" tab="多图风格设置">
              <n-grid :cols="2" :x-gap="24" :y-gap="12" responsive="screen">
                <n-gi :span="2">
                  <n-alert type="info" :bordered="false">
                    此页为“多图风格1”的专属设置。
                  </n-alert>
                </n-gi>
                <n-gi>
                  <n-form-item label="中文字体（本地路径）">
                    <n-input v-model:value="configData.zh_font_path_multi_1_local" placeholder="留空使用预设字体" :disabled="configData.multi_1_use_main_font" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体（本地路径）">
                    <n-input v-model:value="configData.en_font_path_multi_1_local" placeholder="留空使用预设字体" :disabled="configData.multi_1_use_main_font" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="中文字体（下载链接）">
                    <n-input v-model:value="configData.zh_font_url_multi_1" placeholder="留空使用预设字体" :disabled="configData.multi_1_use_main_font" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体（下载链接）">
                    <n-input v-model:value="configData.en_font_url_multi_1" placeholder="留空使用预设字体" :disabled="configData.multi_1_use_main_font" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="中文字体大小比例">
                    <n-input-number v-model:value="configData.zh_font_size_multi_1" :step="0.1" placeholder="1.0" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="英文字体大小比例">
                    <n-input-number v-model:value="configData.en_font_size_multi_1" :step="0.1" placeholder="1.0" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="背景模糊程度">
                    <n-input-number v-model:value="configData.blur_size_multi_1" placeholder="50" :disabled="!configData.multi_1_blur" />
                    <template #feedback>需启用模糊背景</template>
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="背景颜色混合占比">
                    <n-input-number v-model:value="configData.color_ratio_multi_1" :step="0.1" placeholder="0.8" :disabled="!configData.multi_1_blur" />
                    <template #feedback>需启用模糊背景</template>
                  </n-form-item>
                </n-gi>
                 <n-gi :span="2">
                  <n-space>
                    <n-form-item label="启用模糊背景">
                      <n-switch v-model:value="configData.multi_1_blur" />
                      <template #feedback>不启用则使用纯色渐变背景</template>
                    </n-form-item>
                    <n-form-item label="使用单图风格字体">
                      <n-switch v-model:value="configData.multi_1_use_main_font" />
                       <template #feedback>启用后将忽略本页的字体路径和链接设置</template>
                    </n-form-item>
                    <n-form-item label="优先使用海报图">
                      <n-switch v-model:value="configData.multi_1_use_primary" />
                       <template #feedback>多图风格建议开启</template>
                    </n-form-item>
                  </n-space>
                </n-gi>
              </n-grid>
            </n-tab-pane>
            
            <n-tab-pane name="others-tab" tab="其他设置">
              <n-grid :cols="2" :x-gap="24">
                <n-gi>
                  <n-form-item label="自定义图片目录（可选）">
                    <n-input v-model:value="configData.covers_input" placeholder="/path/to/custom/images" />
                  </n-form-item>
                </n-gi>
                <n-gi>
                  <n-form-item label="封面另存目录（可选）">
                    <n-input v-model:value="configData.covers_output" placeholder="/path/to/save/covers" />
                  </n-form-item>
                </n-gi>
              </n-grid>
            </n-tab-pane>
          </n-tabs>
        </n-card>
      </div>
    </n-spin>
  </n-layout>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue';
import axios from 'axios';
import { 
  useMessage, NLayout, NPageHeader, NButton, NIcon, NCard, NGrid, NGi, 
  NFormItem, NSwitch, NSelect, NTabs, NTabPane, NCheckboxGroup, NCheckbox, 
  NSpin, NSpace, NInput, NInputNumber, NRadioGroup, NRadioButton, NSlider, 
  NDivider, NAlert 
} from 'naive-ui';
import { 
  SaveOutline as SaveIcon, 
  ImagesOutline as ImagesIcon,
  TrashOutline as TrashIcon, // ★ 新增：删除图标
  AddOutline as AddIcon      // ★ 新增：添加图标
} from '@vicons/ionicons5';
import * as yaml from 'js-yaml'; // ★ 新增：导入 js-yaml

// 导入静态图片数据
import { single_1, single_2, multi_1 } from '../assets/cover_styles/images.js';
const stylePreviews = ref({
  single_1: single_1,
  single_2: single_2,
  multi_1: multi_1,
});

const styles = [
  { title: "单图 1", value: "single_1" },
  { title: "单图 2", value: "single_2" },
  { title: "多图 1", value: "multi_1" }
];

const message = useMessage();
const isLoading = ref(true);
const isSaving = ref(false);
const isGenerating = ref(false);
const configData = ref({});

// ★ 新增：用于封面标题UI的结构化数据
const titleConfigs = ref([]);

const libraryOptions = ref([]);

const sortOptions = [
  { label: "最新添加", value: "Latest" },
  { label: "随机", value: "Random" },
];

// ★ 新增：将YAML字符串解析为结构化数组
const parseYamlToData = (yamlString) => {
  try {
    if (!yamlString || yamlString.trim() === '') {
      titleConfigs.value = [];
      return;
    }
    const data = yaml.load(yamlString);
    titleConfigs.value = Object.entries(data).map(([library, titles], index) => ({
      id: Date.now() + index, // 使用时间戳+索引确保key的唯一性
      library: library,
      zh: titles[0] || '',
      en: titles[1] || ''
    }));
  } catch (e) {
    message.error('封面标题配置 (YAML) 格式解析失败，请检查。');
    console.error("YAML Parse Error:", e);
    titleConfigs.value = []; // 解析失败则清空，避免UI出错
  }
};

// ★ 新增：将结构化数组转换回YAML字符串
const convertDataToYaml = () => {
  try {
    const dataObject = titleConfigs.value.reduce((acc, item) => {
      // 过滤掉媒体库名称为空的无效配置
      if (item.library && item.library.trim() !== '') {
        acc[item.library.trim()] = [item.zh || '', item.en || ''];
      }
      return acc;
    }, {});

    if (Object.keys(dataObject).length === 0) {
      return ''; // 如果没有有效配置，返回空字符串
    }
    
    return yaml.dump(dataObject);
  } catch (e) {
    message.error('生成封面标题配置失败。');
    console.error("YAML Dump Error:", e);
    return configData.value.title_config; // 转换失败则返回原始值，防止数据丢失
  }
};

// ★ 新增：添加一行新的标题配置
const addTitleConfig = () => {
  titleConfigs.value.push({
    id: Date.now(),
    library: '',
    zh: '',
    en: ''
  });
};

// ★ 新增：移除指定索引的标题配置
const removeTitleConfig = (index) => {
  titleConfigs.value.splice(index, 1);
};


const fetchConfig = async () => {
  isLoading.value = true;
  try {
    const response = await axios.get('/api/config/cover_generator');
    configData.value = response.data;
    // ★ 修改：获取配置后，立即解析YAML
    parseYamlToData(configData.value.title_config);
  } catch (error) {
    message.error('加载封面生成器配置失败。');
  } finally {
    isLoading.value = false;
  }
};

const fetchLibraryOptions = async () => {
  try {
    const response = await axios.get('/api/config/cover_generator/libraries');
    libraryOptions.value = response.data;
  } catch (error) {
    message.error('获取媒体库列表失败，请检查后端。');
  }
};

const saveConfig = async () => {
  isSaving.value = true;
  // ★ 修改：保存前，将结构化数据转换回YAML字符串
  configData.value.title_config = convertDataToYaml();
  
  try {
    await axios.post('/api/config/cover_generator', configData.value);
    message.success('配置已成功保存！');
  } catch (error) {
    message.error('保存配置失败。');
  } finally {
    isSaving.value = false;
  }
};

const runGenerateAllTask = async () => {
  isGenerating.value = true;
  try {
    await axios.post('/api/tasks/run', { task_name: 'generate-all-covers' });
    message.success('已成功触发“立即生成所有媒体库封面”任务，请在任务队列中查看进度。');
  } catch (error) {
    message.error('触发任务失败，请检查后端日志。');
  } finally {
    isGenerating.value = false;
  }
};

// --- 实时预览部分保持不变 ---
let previewUpdateTimeout = null;
const isPreviewLoading = ref(false);

function debounceUpdatePreview() {
  isPreviewLoading.value = true;
  if (previewUpdateTimeout) {
    clearTimeout(previewUpdateTimeout);
  }
  previewUpdateTimeout = setTimeout(updateAllPreviews, 500);
}

async function updateAllPreviews() {
  if (!configData.value.show_item_count) {
    stylePreviews.value.single_1 = single_1;
    stylePreviews.value.single_2 = single_2;
    stylePreviews.value.multi_1 = multi_1;
    isPreviewLoading.value = false;
    return;
  }

  try {
    const previewsToUpdate = [
      { key: 'single_1', base_image: single_1 },
      { key: 'single_2', base_image: single_2 },
      { key: 'multi_1', base_image: multi_1 },
    ];

    const promises = previewsToUpdate.map(p => 
      axios.post('/api/config/cover_generator/preview', {
        base_image: p.base_image,
        badge_style: configData.value.badge_style,
        badge_size_ratio: configData.value.badge_size_ratio,
      })
    );

    const results = await Promise.all(promises);
    
    stylePreviews.value.single_1 = results[0].data.image;
    stylePreviews.value.single_2 = results[1].data.image;
    stylePreviews.value.multi_1 = results[2].data.image;

  } catch (error) {
    message.error("实时预览失败");
  } finally {
    isPreviewLoading.value = false;
  }
}

watch(
  () => [
    configData.value.show_item_count, 
    configData.value.badge_style, 
    configData.value.badge_size_ratio
  ],
  () => {
    debounceUpdatePreview();
  },
  { deep: true } // 建议对复杂对象监听开启deep
);

onMounted(() => {
  fetchConfig();
  fetchLibraryOptions();
});
</script>

<style scoped>
/* 样式部分保持不变 */
.style-card {
  cursor: pointer;
  text-align: center;
}
.style-img {
  width: 100%;
  aspect-ratio: 16 / 9;
  object-fit: cover;
  border-bottom: 1px solid #eee;
}
.n-radio {
  margin-top: 12px;
  justify-content: center;
  width: 100%;
}
</style>