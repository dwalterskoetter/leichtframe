import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'LeichtFrame',
  tagline: 'High-performance .NET DataFrame for Backend Services',
  favicon: 'img/logo.svg',

  url: 'https://dwalterskoetter.github.io', 
  baseUrl: '/leichtframe/', 

  organizationName: 'dwalterskoetter', 
  projectName: 'leichtframe', 

  onBrokenLinks: 'warn', 
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          
        },
        blog: false, 
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: 'LeichtFrame',
        logo: {
          alt: 'LeichtFrame Logo',
          src: 'img/logo.svg', 
        },
        items: [
          {
            to: 'docs/LeichtFrame/Core', 
            label: 'Core', 
            position: 'left'
          },
          {
            to: 'docs/LeichtFrame/IO', 
            label: 'IO', 
            position: 'left'
          },
          {
            href: 'https://github.com/dwalterskoetter/leichtframe',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        copyright: `Copyright © ${new Date().getFullYear()} The LeichtFrame Authors. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['csharp'],
      },
    }),
};

export default config;