declare module "node:assert" {
  export const strict: {
    equal: (...args: unknown[]) => void;
    deepEqual: (...args: unknown[]) => void;
    throws: (...args: unknown[]) => void;
  };
}

declare module "node:test" {
  export function test(name: string, callback: () => void): void;
}

declare module "node:fs" {
  export function readFileSync(path: URL, encoding: "utf8"): string;
}
