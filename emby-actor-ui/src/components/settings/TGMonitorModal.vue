<!-- src/components/settings/TGMonitorModal.vue -->
<template>
  <n-modal v-model:show="showModal" preset="card" title="频道订阅监听配置 (Pro)" style="width: 600px; max-width: 95vw;">
    <n-spin :show="isLoading">
      <n-form label-placement="left" label-width="120">
        
        <n-form-item label="启用监听" path="enabled">
          <n-switch v-model:value="config.enabled" />
        </n-form-item>

        <n-form-item label="订阅类型" path="monitor_types">
          <n-checkbox-group v-model:value="config.monitor_types">
            <n-space>
              <n-checkbox value="movie" label="电影" />
              <n-checkbox value="tv" label="电视剧" />
              <n-checkbox value="all" label="无脑转存" />
            </n-space>
          </n-checkbox-group>
        </n-form-item>
        
        <!-- 当勾选无脑转存时显示警告提示 -->
        <n-alert v-if="config.monitor_types && config.monitor_types.includes('all')" type="warning" style="margin-bottom: 24px;" :show-icon="true">
          <b>警告：</b>开启“无脑转存”后，将无视您的订阅列表、追剧状态和本地去重逻辑，全盘接收频道发布的所有 115 资源！<br/>
          这可能会快速消耗您的 115 空间配额和影巢积分，请谨慎使用。
        </n-alert>

        <n-form-item label="API ID" path="api_id">
          <n-input v-model:value="config.api_id" placeholder="例如: 1234567" />
        </n-form-item>
        
        <n-form-item label="API Hash" path="api_hash">
          <n-input v-model:value="config.api_hash" type="password" show-password-on="click" />
        </n-form-item>
        
        <n-form-item label="手机号" path="phone">
          <n-input v-model:value="config.phone" placeholder="带国家代码，例如: +8613800138000" />
        </n-form-item>
        
        <n-form-item label="两步验证(2FA)" path="password">
          <n-input v-model:value="config.password" type="password" show-password-on="click" placeholder="如果没有设置请留空" />
        </n-form-item>
        
        <n-form-item label="白名单频道" path="channels">
          <n-select v-model:value="config.channels" multiple filterable tag placeholder="输入频道 Username 或 ID 并回车 (如 hdtv115)" :options="[]" />
        </n-form-item>

        <n-form-item label="拦截关键词" path="block_keywords">
          <n-select v-model:value="config.block_keywords" multiple filterable tag placeholder="输入关键词并回车 (如: 合集, 原盘, 大包)" :options="[]" />
        </n-form-item>

        <n-divider title-placement="left">登录授权</n-divider>
        <n-alert type="info" :show-icon="true" style="margin-bottom: 16px;">
          修改 API 信息后，请务必先点击右下角的 <b>"保存配置"</b>，然后再获取验证码登录。
        </n-alert>

        <n-form-item label="授权状态">
          <n-space align="center">
            <n-tag :type="userBotStatus === 'authorized' ? 'success' : 'error'">
              {{ userBotStatus === 'authorized' ? '已登录 (监听中)' : '未登录' }}
            </n-tag>
            
            <n-button v-if="userBotStatus !== 'authorized'" type="primary" size="small" @click="sendUserBotCode" :loading="isSendingCode">
              获取验证码
            </n-button>
            <n-button v-else type="error" ghost size="small" @click="logoutUserBot">
              注销账号
            </n-button>
          </n-space>
        </n-form-item>

        <!-- 验证码输入框 -->
        <n-form-item v-if="showCodeInput" label="输入验证码">
          <n-input-group>
            <n-input v-model:value="userBotCode" placeholder="输入 TG 收到的验证码" />
            <n-button type="primary" @click="submitUserBotCode" :loading="isSubmittingCode">确认登录</n-button>
          </n-input-group>
        </n-form-item>

        <!-- ★★★ 高级设置：自定义正则 ★★★ -->
        <n-collapse style="margin-top: 24px; border-top: 1px solid rgba(128,128,128,0.2); padding-top: 16px;">
          <n-collapse-item title="高级设置：自定义正则提取 (点击展开)" name="1">
            <n-alert type="info" style="margin-bottom: 16px; font-size: 13px;">
              当默认规则无法识别某些奇葩频道的格式时，可在此添加自定义正则表达式。<br/>
              <b>注意：</b>必须使用 <code>()</code> 捕获组来提取目标内容。自定义正则优先级高于系统默认。
            </n-alert>

            <n-form-item>
              <template #label>
                TMDB ID 提取
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon style="margin-left:4px; cursor:help;"><HelpCircleOutline /></n-icon></template>
                  需 1 个捕获组。例如：<code>TMDB:\s*(\d+)</code>
                </n-tooltip>
              </template>
              <n-dynamic-input v-model:value="config.custom_regex.tmdb" placeholder="输入正则表达式" :min="0" />
            </n-form-item>

            <n-form-item>
              <template #label>
                标题与年份提取
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon style="margin-left:4px; cursor:help;"><HelpCircleOutline /></n-icon></template>
                  需 2 个捕获组 (标题, 年份)。例如：<code>名称:\s*(.*?)\s*\((\d{4})\)</code>
                </n-tooltip>
              </template>
              <n-dynamic-input v-model:value="config.custom_regex.title_year" placeholder="输入正则表达式" :min="0" />
            </n-form-item>

            <n-form-item>
              <template #label>
                季与集提取
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon style="margin-left:4px; cursor:help;"><HelpCircleOutline /></n-icon></template>
                  需 2 个捕获组 (季, 集) 或 1 个捕获组 (集)。例如：<code>S(\d+)E(\d+)</code>
                </n-tooltip>
              </template>
              <n-dynamic-input v-model:value="config.custom_regex.season_episode" placeholder="输入正则表达式" :min="0" />
            </n-form-item>

            <n-form-item>
              <template #label>
                提取码(密码)提取
                <n-tooltip trigger="hover">
                  <template #trigger><n-icon style="margin-left:4px; cursor:help;"><HelpCircleOutline /></n-icon></template>
                  需 1 个捕获组。例如：<code>密码:\s*([a-zA-Z0-9]{4})</code>
                </n-tooltip>
              </template>
              <n-dynamic-input v-model:value="config.custom_regex.password" placeholder="输入正则表达式" :min="0" />
            </n-form-item>
          </n-collapse-item>
        </n-collapse>

      </n-form>
    </n-spin>

    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">关闭</n-button>
        <n-button type="primary" @click="saveConfig" :loading="isSaving">保存配置</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { HelpCircleOutline } from '@vicons/ionicons5';
import { 
  NModal, NSpin, NForm, NFormItem, NInput, NSwitch, NCheckboxGroup, NCheckbox, 
  NSpace, NSelect, NDivider, NAlert, NTag, NButton, NInputGroup, useMessage,
  NCollapse, NCollapseItem, NTooltip, NIcon, NDynamicInput 
} from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const isLoading = ref(false);
const isSaving = ref(false);

const config = ref({
  enabled: false,
  api_id: '',
  api_hash: '',
  phone: '',
  password: '',
  channels: [],
  monitor_types: ['movie', 'tv'],
  block_keywords: [],
  custom_regex: {
    tmdb: [],
    title_year: [],
    season_episode: [],
    password: []
  }
});

// 授权状态
const userBotStatus = ref('unauthorized');
const showCodeInput = ref(false);
const userBotCode = ref('');
const isSendingCode = ref(false);
const isSubmittingCode = ref(false);

// 暴露给父组件调用的方法
const open = async () => {
  showModal.value = true;
  showCodeInput.value = false;
  userBotCode.value = '';
  await fetchConfig();
  await checkUserBotStatus();
};

const fetchConfig = async () => {
  isLoading.value = true;
  try {
    const res = await axios.get('/api/subscription/tg_userbot/config');
    if (res.data.success) {
      config.value = res.data.data;
      if (!config.value.custom_regex) {
        config.value.custom_regex = { tmdb: [], title_year: [], season_episode: [], password: [] };
      }
    }
  } catch (e) {
    message.error('读取配置失败');
  } finally {
    isLoading.value = false;
  }
};

const saveConfig = async () => {
  isSaving.value = true;
  try {
    const res = await axios.post('/api/subscription/tg_userbot/config', config.value);
    if (res.data.success) {
      message.success(res.data.message);
      await checkUserBotStatus(); // 保存后重新刷新状态
    }
  } catch (e) {
    message.error('保存失败');
  } finally {
    isSaving.value = false;
  }
};

// --- 授权相关 API ---
const checkUserBotStatus = async () => {
  try {
    const res = await axios.get('/api/subscription/tg_userbot/status');
    if (res.data.success) {
      userBotStatus.value = res.data.data.status;
    }
  } catch (e) {}
};

const sendUserBotCode = async () => {
  if (!config.value.api_id || !config.value.phone) {
    return message.warning('请先填写 API ID 和手机号，并保存配置');
  }
  isSendingCode.value = true;
  try {
    const res = await axios.post('/api/subscription/tg_userbot/send_code');
    if (res.data.success) {
      message.success(res.data.message);
      showCodeInput.value = true;
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error(e.response?.data?.message || '请求失败');
  } finally {
    isSendingCode.value = false;
  }
};

const submitUserBotCode = async () => {
  if (!userBotCode.value) return message.warning('请输入验证码');
  isSubmittingCode.value = true;
  try {
    const res = await axios.post('/api/subscription/tg_userbot/login', { code: userBotCode.value });
    if (res.data.success) {
      message.success(res.data.message);
      showCodeInput.value = false;
      await checkUserBotStatus();
    } else {
      message.error(res.data.message);
    }
  } catch (e) {
    message.error(e.response?.data?.message || '登录失败');
  } finally {
    isSubmittingCode.value = false;
  }
};

const logoutUserBot = async () => {
  try {
    await axios.post('/api/subscription/tg_userbot/logout');
    message.success('已注销');
    await checkUserBotStatus();
  } catch (e) {
    message.error('注销失败');
  }
};

defineExpose({ open });
</script>