<script setup>
import { computed, ref } from 'vue'
import { withBase } from 'vitepress'
import { categories, journeys, guideLink } from './catalog'
const query = ref('')
const filtered = computed(() => categories.map(category => ({ ...category, items: category.items.filter(item => (category.title + ' ' + item.join(' ')).toLowerCase().includes(query.value.trim().toLowerCase())) })).filter(category => category.items.length))
const count = computed(() => filtered.value.reduce((total, category) => total + category.items.length, 0))
</script>

<template>
  <main class="guide-hub">
    <header class="hub-hero"><p class="eyebrow">ETKN / 使用教程</p><h1>从你想做的事开始。</h1><p>第一次部署、自动追更，或者只是想换个设备接着看。<br />这里有对应的操作路径。</p></header>
    <div class="hub-search"><label for="guide-filter">查找教程</label><div><span aria-hidden="true">⌕</span><input id="guide-filter" v-model="query" type="search" placeholder="试试：字幕、缓存、TV、音乐、端口…" /><kbd>标题 / 关键词</kbd></div><p role="status" aria-live="polite">{{ query.trim() ? `找到 ${count} 篇教程` : '按标题与关键词筛选；搜索正文请使用顶部「搜索教程」。' }}</p></div>
    <section v-if="!query.trim()" class="hub-journeys" aria-label="按使用阶段开始"><a v-for="item in journeys" :key="item.number" class="journey-card" :href="withBase(guideLink(item.link))"><span class="card-number">{{ item.number }}</span><h2>{{ item.title }}</h2><p>{{ item.text }}</p><span class="card-link">{{ item.label }} ↗</span></a></section>
    <div class="hub-directory"><section v-for="category in filtered" :key="category.title" class="directory-group"><header><h2>{{ category.title }}</h2><p>{{ category.note }}</p></header><ul><li v-for="item in category.items" :key="item[1]"><a :href="withBase(guideLink(item[1]))"><span>{{ item[0] }}</span><span aria-hidden="true">↗</span></a></li></ul></section><div v-if="!count" class="hub-empty"><h2>暂时没有匹配的教程</h2><p>换个关键词，或使用顶部搜索查找正文。</p><button class="wiki-button" @click="query = ''">查看全部教程</button></div></div>
    <aside class="hub-support"><span>还是没有找到？</span><a :href="withBase('/zh/faq/troubleshooting.html')">先按故障现象排查 →</a><a :href="withBase('/zh/guide/scheduler.html')">学会查看任务日志 →</a></aside>
  </main>
</template>
