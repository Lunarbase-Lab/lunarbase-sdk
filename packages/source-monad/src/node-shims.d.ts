declare module "node:assert" {
  export const strict: {
    equal: (...args: unknown[]) => void;
    ok: (...args: unknown[]) => void;
    throws: (...args: unknown[]) => void;
  };
}
declare module "node:fs/promises" {
  export function readFile(path: URL | string, encoding: "utf8"): Promise<string>;
}
declare module "node:test" {
  export function test(name: string, callback: () => void | Promise<void>): void;
}
