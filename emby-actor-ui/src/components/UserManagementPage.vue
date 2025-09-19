<!-- src/components/UserManagementPage.vue -->
<template>
  <div class="user-management-page">
    <n-page-header>
      <template #title>
        <h1>用户管理</h1>
      </template>
    </n-page-header>

    <n-tabs type="line" animated default-value="user-list">
      <n-tab-pane name="user-list" tab="用户列表">
        <UserList />
      </n-tab-pane>
      <n-tab-pane name="invitation-list" tab="邀请链接">
        <InvitationList />
      </n-tab-pane>
      <!-- ★★★ 新增的选项卡 ★★★ -->
      <n-tab-pane name="user-templates" tab="用户模板">
        <UserTemplates />
      </n-tab-pane>
    </n-tabs>
  </div>
</template>

<script setup>
import { NTabs, NTabPane, NPageHeader } from 'naive-ui';
import UserList from './UserList.vue';
import InvitationList from './InvitationList.vue';
// ★★★ 引入新的组件 ★★★
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
</style>