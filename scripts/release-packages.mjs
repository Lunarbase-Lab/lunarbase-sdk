export const releaseCrates = Object.freeze([
  { name: "lunarbase-pmm-v2-math", workspaceDependency: "lunarbase-math" },
  { name: "lunarbase-pmm-v2-client", workspaceDependency: "lunarbase-client" },
  { name: "lunarbase-pmm-v2-source-evm", workspaceDependency: "lunarbase-source-evm" },
  { name: "lunarbase-pmm-v2-source-monad", workspaceDependency: "lunarbase-source-monad" },
  { name: "lunarbase-pmm-v2-source-arbitrum", workspaceDependency: "lunarbase-source-arbitrum" },
]);

export const releaseNpmPackages = Object.freeze([
  { name: "@lunarbase-lab/pmm-v2-math", directory: "packages/math" },
  { name: "@lunarbase-lab/pmm-v2-client", directory: "packages/client" },
  { name: "@lunarbase-lab/pmm-v2-source-evm", directory: "packages/source-evm" },
  { name: "@lunarbase-lab/pmm-v2-source-monad", directory: "packages/source-monad" },
  { name: "@lunarbase-lab/pmm-v2-source-arbitrum", directory: "packages/source-arbitrum" },
]);
