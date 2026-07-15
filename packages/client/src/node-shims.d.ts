declare module "node:assert" {
  export const strict: { deepEqual: (...args: unknown[]) => void; equal: (...args: unknown[]) => void; };
}
declare module "node:test" { export function test(name: string, callback: () => void): void; }
