declare module "node:assert" {
  export const strict: {
    equal: (...args: unknown[]) => void;
    deepEqual: (...args: unknown[]) => void;
    ok: (...args: unknown[]) => void;
    rejects: (...args: unknown[]) => Promise<void>;
    throws: (...args: unknown[]) => void;
    doesNotThrow: (...args: unknown[]) => void;
  };
}

declare module "node:test" {
  export function test(name: string, callback: () => void | Promise<void>): void;
}
