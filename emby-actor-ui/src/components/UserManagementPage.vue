<!-- src/components/UserManagementPage.vue (已更新) -->
<template>
  <div class="user-management-page">
    <n-page-header>
      <template #title>
        <h1>用户管理</h1>
      </template>
    </n-page-header>

    <!-- ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ -->
    <!-- ★★★ 在这里添加您需要的操作提示 ★★★ -->
    <!-- ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ -->
    <n-alert title="操作流程引导" type="info" style="margin-top: 15px; margin-bottom: 20px;">
      <template #default>
        <ul>
          <li><strong>第一步：创建模板。</strong>请先在【用户模板】标签页中创建一个权限模板。模板是新用户权限的“样板”，决定了他们注册后的所有设置。</li>
          <li><strong>第二步：生成链接。</strong>创建模板后，在【邀请链接】标签页中，选择一个模板来生成一个专属的注册链接并分享给您的朋友。</li>
          <li><strong>第三步：管理用户。</strong>新用户通过链接注册成功后，会出现在【用户列表】中，您可以在此对他们进行禁用、续期或删除等操作。</li>
        </ul>
      </template>
    </n-alert>

    <n-tabs type="line" animated default-value="user-list">
      <n-tab-pane name="user-list" tab="用户列表">
        <UserList />
      </n-tab-pane>
      <n-tab-pane name="invitation-list" tab="邀请链接">
        <InvitationList />
      </n-tab-pane>
      <n-tab-pane name="user-templates" tab="用户模板">
        <UserTemplates />
      </n-tab-pane>
    </n-tabs>
  </div>
</template>

<script setup>
// ★★★ 确保导入了 NAlert ★★★
import { NTabs, NTabPane, NPageHeader, NAlert } from 'naive-ui';
import UserList from './UserList.vue';
import InvitationList from './InvitationList.vue';
import UserTemplates from './UserTemplates.vue';
</script>

<style scoped>
.user-management-page {
  padding: 10px;
}
h1 {
  font-size: 24px;
  margin: 0;
}
/* ★★★ 为列表添加一点样式，让提示更美观 ★★★ */
ul {
  padding-left: 20px;
  margin: 0;
}
li {
  margin-bottom: 8px;
}
li:last-child {
  margin-bottom: 0;
}
</style>