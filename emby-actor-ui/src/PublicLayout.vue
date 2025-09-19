<!-- src/PublicLayout.vue -->
<template>
  <n-layout style="height: 100vh;">
    <n-layout-content style="display: flex; align-items: center; justify-content: center;">
      <router-view />
    </n-layout-content>
  </n-layout>
</template>

<script setup>
import { NLayout, NLayoutContent } from 'naive-ui';
</script>