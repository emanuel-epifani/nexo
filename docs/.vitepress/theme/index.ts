import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import NexoHome from './NexoHome.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('NexoHome', NexoHome)
  },
} satisfies Theme
