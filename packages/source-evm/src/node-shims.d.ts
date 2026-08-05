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

declare module "node:fs/promises" {
  export function readFile(path: URL, encoding: "utf8"): Promise<string>;
}

declare module "node:test" {
  export interface MockTimers {
    enable(options: { apis: readonly string[] }): void;
    tick(milliseconds: number): void;
    reset(): void;
  }

  export interface TestContext {
    readonly mock: { readonly timers: MockTimers };
  }

  export function test(name: string, callback: (context: TestContext) => void | Promise<void>): void;
}
