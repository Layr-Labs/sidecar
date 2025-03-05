// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

import type * as Preset from "@docusaurus/preset-classic";
import type { Config } from "@docusaurus/types";
import type * as Plugin from "@docusaurus/types/src/plugin";
import type * as OpenApiPlugin from "docusaurus-plugin-openapi-docs";
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';

const config: Config = {
	title: 'EigenLayer Sidecar',
	tagline: '',
	favicon: 'img/eigenlayer-logo.png',

	// Set the production url of your site here
	url: 'https://layr-labs.github.io',
	// Set the /<baseUrl>/ pathname under which your site is served
	// For GitHub pages deployment, it is often '/<projectName>/'
	baseUrl: '/sidecar/',

	// GitHub pages deployment config.
	// If you aren't using GitHub pages, you don't need these.
	organizationName: 'Layr-Labs', // Usually your GitHub org/user name.
	projectName: 'sidecar', // Usually your repo name.
	deploymentBranch: 'master',

	onBrokenLinks: 'warn',
	onBrokenMarkdownLinks: 'warn',
	trailingSlash: false,

	// Even if you don't use internationalization, you can use this field to set
	// useful metadata like html lang. For example, if your site is Chinese, you
	// may want to replace "en" with "zh-Hans".
	// i18n: {
	// 	defaultLocale: 'en',
	// 	locales: ['en'],
	// },

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: require.resolve("./sidebars.ts"),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
			breadcrumbs: true,
			//sidebarPath: undefined,
			remarkPlugins: [remarkMath],
			rehypePlugins: [rehypeKatex],
			sidebarCollapsible: false,
          docItemComponent: "@theme/ApiItem", // Derived from docusaurus-theme-openapi
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            "https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/",
          onInlineAuthors: "ignore",
          onUntruncatedBlogPosts: "ignore",
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig:
    {
      docs: {
        sidebar: {
			autoCollapseCategories: false
        },
      },
      navbar: {
		  title: 'EigenLayer Sidecar',
		  logo: {
			  alt: 'EigenLayer Sidecar',
			  src: 'img/eigenlayer-logo.png',
		  },
        items: [
          {
            type: "doc",
            docId: "sidecar/intro",
			to: "/docs/sidecar",
            position: "left",
            label: "Docs",
          },
          {
            label: "Public API",
            position: "left",
            to: "/docs/category/public-api",
          }, {
            label: "Full API",
            position: "left",
            to: "/docs/category/api",
          },
			{
				href: 'https://github.com/Layr-Labs/sidecar',
				label: 'GitHub',
				position: 'right',
			},
        ],
      },
		footer: {
			style: "dark",
			links: [
				{
					title: "EigenLayer",
					items: [
						{
							label: "About",
							href: "https://www.eigenlayer.xyz/",
						},
						{
							label: "Privacy Policy",
							href: "https://docs.eigenlayer.xyz/eigenlayer/legal/privacy-policy",
						},
						{
							label: "Terms of Service",
							href: "https://docs.eigenlayer.xyz/eigenlayer/legal/terms-of-service",
						},
						{
							label: "Disclaimers",
							href: "https://docs.eigenlayer.xyz/eigenlayer/legal/disclaimers",
						},
					],
				},
				{
					title: "Community",
					items: [
						{
							label: "Support",
							href: "https://docs.eigenlayer.xyz/eigenlayer/overview/support",
						},
						{
							label: "Forum",
							href: "https://forum.eigenlayer.xyz/",
						},
						{
							label: "Discord",
							href: "https://discord.com/invite/eigenlayer",
						},
						{
							label: "Twitter",
							href: "https://twitter.com/eigenlayer",
						},
					],
				},
				{
					title: "More",
					items: [
						{
							label: "GitHub",
							href: "https://github.com/Layr-Labs",
						},
						{
							label: "Youtube",
							href: "https://www.youtube.com/@EigenLayer",
						},
					],
				},
			],
			copyright: `Copyright © ${new Date().getFullYear()} Eigen Labs, Inc.`,
		},
      prism: {
        additionalLanguages: [
          "ruby",
          "csharp",
          "php",
          "java",
          "powershell",
          "json",
          "bash",
          "dart",
          "objectivec",
          "r",
        ],
      },
      languageTabs: [
		  {
			  highlight: "bash",
			  language: "curl",
			  logoClass: "curl",
		  },
		  {
			  highlight: "go",
			  language: "go",
			  logoClass: "go",
		  },
        {
          highlight: "python",
          language: "python",
          logoClass: "python",
        },

        {
          highlight: "javascript",
          language: "nodejs",
          logoClass: "nodejs",
        },
        {
          highlight: "javascript",
          language: "javascript",
          logoClass: "javascript",
        },
        {
          highlight: "rust",
          language: "rust",
          logoClass: "rust",
        },
      ],
    } satisfies Preset.ThemeConfig,

  plugins: [
	 [
		 '@docusaurus/plugin-client-redirects',
		 {
			 redirects: [
				 {
					 from: '/',
					 to: '/docs/sidecar/intro'
				 }
		 ]
		 }
	 ],
    [
      "docusaurus-plugin-openapi-docs",
      {
        id: "openapi",
        docsPluginId: "classic",
        config: {
          public: {
            specPath: "openapi/api.public.json",
            outputDir: "docs/public-api",
            downloadUrl: "https://raw.githubusercontent.com/Layr-Labs/protocol-apis/refs/heads/master/gen/openapi/api.public.swagger.json",
            sidebarOptions: {
              groupPathsBy: "tag",
              categoryLinkSource: "tag",
            },
			  hideSendButton: false,
			  showSchemas: true,
          } satisfies OpenApiPlugin.Options,
		  api: {
            specPath: "openapi/api.json",
            outputDir: "docs/api",
            downloadUrl: "https://raw.githubusercontent.com/Layr-Labs/protocol-apis/refs/heads/master/gen/openapi/api.swagger.json",
            sidebarOptions: {
              groupPathsBy: "tag",
              categoryLinkSource: "tag",
            },
          } satisfies OpenApiPlugin.Options,
        } satisfies Plugin.PluginOptions,
      },
    ],
  ],

  themes: ["docusaurus-theme-openapi-docs"],
};

export default async function createConfig() {
  return config;
}
