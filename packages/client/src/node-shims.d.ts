declare module "node:assert" {
  export const strict: { deepEqual: (...args: unknown[]) => void; equal: (...args: unknown[]) => void; };
}
declare module "node:test" { export function test(name: string, callback: () => void): void; }
declare module "node:fs" { export function readFileSync(path: URL, encoding: "utf8"): string; }
