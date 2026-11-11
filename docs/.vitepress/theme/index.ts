import DefaultTheme from 'vitepress/theme'
import WikiHome from './WikiHome.vue'
import GuideHub from './GuideHub.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('WikiHome', WikiHome)
    app.component('GuideHub', GuideHub)
  }
}
