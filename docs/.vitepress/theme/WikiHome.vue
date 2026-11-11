<script setup>
import { withBase } from 'vitepress'
import { journeys, guideLink } from './catalog'
const highlights = [
  ['01 / WEB', '打开浏览器，就在影院', '海报墙、续播、人物搜索与多版本选择，手机和电脑都能看。', 'web-client'],
  ['02 / TV', '大屏看片，遥控器就够', '原生 Android TV 客户端，续播、音轨、字幕与原盘播放。', 'tv-client'],
  ['03 / PLAYBACK', '按设备，选择合适的播放方式', '内网按需缓存，官方客户端可配置转码，查看实际播放来源。', 'playback-cache'],
  ['04 / SUBTITLE', '字幕，也能更合心意', '优先用已有字幕，支持文本字幕翻译、简繁转换与双语输出。', 'subtitles'],
  ['05 / DANMAKU', '一个人看，也有一起看的热闹', '内置平台或自定义弹幕服务，在播放器里调整显示效果。', 'danmaku'],
  ['06 / MUSIC', '电影之外，把音乐也收好', '按专辑、歌手或文件夹浏览，把喜欢的歌曲放进自己的歌单。', 'music']
]
</script>

<template>
  <div class="wiki-home">
    <section class="home-hero" aria-labelledby="home-title">
      <div class="hero-copy">
        <p class="eyebrow"><span class="status-dot" />你的媒体，你的观看方式</p>
        <h1 id="home-title">把想看的，<br /><em>变成随时能看的。</em></h1>
        <p class="hero-description">从找片、追更到整理入库，<br class="desktop-break" />再到网页、电视上的那一次播放。<br />ETKN 把它们连在一起。</p>
        <div class="hero-actions">
          <a class="wiki-button primary" :href="withBase(guideLink('quick-start'))">开始使用 <span aria-hidden="true">↗</span></a>
          <a class="wiki-button" :href="withBase('/zh/guide/')">浏览全部教程 <span aria-hidden="true">→</span></a>
        </div>
        <p class="hero-footnote">Docker 部署 <span>·</span> 内置媒体服务 <span>·</span> 网页与 TV 客户端</p>
      </div>
      <div class="library-scene" role="img" aria-label="流程示意：资源经过订阅和整理，进入媒体库，再通过网页或电视播放">
        <div class="scene-topline"><span><span class="status-dot" />ETKN / 你的私人片库</span><span class="scene-badge">流程示意</span></div>
        <div class="scene-title">今晚，留一点时间<br />给喜欢的故事。</div>
        <div class="scene-posters" aria-hidden="true">
          <div class="scene-poster poster-a"><span>EXPLORE</span><strong>山海<br />之间</strong><small>电影 / 收藏</small></div>
          <div class="scene-poster poster-b"><span>CONTINUE</span><strong>光的<br />另一面</strong><small>剧集 / 续播</small></div>
          <div class="scene-poster poster-c"><span>LISTEN</span><strong>慢慢<br />听</strong><small>音乐 / 歌单</small></div>
        </div>
        <div class="scene-pipeline"><span>找到资源</span><i>→</i><span>自动整理</span><i>→</i><span>打开就看</span></div>
        <div class="scene-note"><span class="note-check">✓</span><div><strong>让追更和整理，留在幕后</strong><small>把时间留给观看本身</small></div><span class="note-spark" aria-hidden="true">✦</span></div>
      </div>
    </section>
    <section class="home-section journey-section" aria-labelledby="journey-title">
      <div class="section-heading"><div><p class="eyebrow">从你的目标出发</p><h2 id="journey-title">你想先完成什么？</h2></div><p>不用从头读完整本文档。<br />选一条最接近你的路线。</p></div>
      <div class="journey-grid">
        <a v-for="item in journeys" :key="item.number" class="journey-card" :href="withBase(guideLink(item.link))"><span class="card-number">{{ item.number }}</span><h3>{{ item.title }}</h3><p>{{ item.text }}</p><span class="card-link">{{ item.label }} <span aria-hidden="true">↗</span></span></a>
      </div>
    </section>
    <section class="home-section" aria-labelledby="features-title">
      <div class="section-heading"><div><p class="eyebrow">不止整理，也为了好好观看</p><h2 id="features-title">这些功能，现在都有教程了。</h2></div><a class="text-link" :href="withBase('/zh/introduction/features.html')">查看完整功能一览 ↗</a></div>
      <div class="highlight-grid">
        <a v-for="item in highlights" :key="item[0]" class="highlight-card" :href="withBase(guideLink(item[3]))"><span class="eyebrow">{{ item[0] }}</span><h3>{{ item[1] }}</h3><p>{{ item[2] }}</p><span class="highlight-arrow" aria-hidden="true">↗</span></a>
      </div>
    </section>
    <section class="home-section flow-section" aria-labelledby="flow-title">
      <div><p class="eyebrow">从资源到播放</p><h2 id="flow-title">一条流程，<br />让片库持续生长。</h2><p>先跑通第一部影片，<br />再按自己的习惯开启自动化。</p></div>
      <ol class="home-flow">
        <li><span>01</span><div><h3>找到想看的</h3><p>共享池、频道、MoviePilot、re0 或手动链接。</p><a :href="withBase(guideLink('shared-resource'))">了解资源获取 →</a></div></li>
        <li><span>02</span><div><h3>整理成自己的片库</h3><p>识别、归类、生成 STRM，补齐元数据与媒体信息。</p><a :href="withBase(guideLink('p115'))">了解整理入库 →</a></div></li>
        <li><span>03</span><div><h3>换个屏幕，接着看</h3><p>网页与 TV 浏览媒体库，按账号记录观看进度。</p><a :href="withBase(guideLink('web-client'))">了解多端播放 →</a></div></li>
      </ol>
    </section>
    <section class="home-help"><div><p class="eyebrow">文档，也要跟得上功能</p><h2>少一点摸索，多一点开箱即用。</h2><p>本轮内容核对：2026 年 11 月 11 日。功能入口与可用性以安装版本和当前权益为准。</p></div><a class="wiki-button primary" :href="withBase('/zh/guide/')">进入使用教程 <span aria-hidden="true">→</span></a></section>
  </div>
</template>
