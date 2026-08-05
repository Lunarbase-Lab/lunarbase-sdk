declare module "node:assert" {
  export const strict: {
    equal: (...args: unknown[]) => void;
    deepEqual: (...args: unknown[]) => void;
    rejects: (...args: unknown[]) => Promise<void>;
  };
}
declare module "node:test" {
  export function test(name: string, callback: () => void | Promise<void>): void;
}
