import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Nexo',
  description: 'The High-Performance All-in-One Broker',
  
  head: [
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    ['link', { href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap', rel: 'stylesheet' }],
  ],

  themeConfig: {
    logo: undefined,
    siteTitle: 'Nexo',

    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'Reference', link: '/reference/sdks' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Getting Started',
          items: [
            { text: 'Introduction', link: '/guide/introduction' },
            { text: 'Quick Start', link: '/guide/quickstart' },
            { text: 'Architecture', link: '/guide/architecture' },
          ],
        },
        {
          text: 'Brokers',
          items: [
            { text: 'Store', link: '/guide/store' },
            { text: 'Queue', link: '/guide/queue' },
            { text: 'Pub/Sub', link: '/guide/pubsub' },
            { text: 'Stream', link: '/guide/stream' },
          ],
        },
        {
          text: 'Advanced',
          items: [
            { text: 'Binary Payloads', link: '/guide/binary' },
            { text: 'Deployment', link: '/guide/deployment' },
          ],
        },
      ],
      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'Client SDKs', link: '/reference/sdks' },
          ],
        },
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/emanuel-epifani/nexo' },
      { icon: 'linkedin', link: 'https://www.linkedin.com/in/emanuel-epifani/' },
    ],

    footer: {
      message: 'Built by <a href="https://www.linkedin.com/in/emanuel-epifani/" target="_blank">Emanuel Epifani</a>',
      copyright: 'MIT License',
    },

    search: {
      provider: 'local',
    },

    editLink: {
      pattern: 'https://github.com/emanuel-epifani/nexo/edit/main/nexo-docs/:path',
      text: 'Edit this page on GitHub',
    },
  },
})
