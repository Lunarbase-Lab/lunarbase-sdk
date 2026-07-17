declare module "node:assert" {
  export const strict: {
    equal: (...args: unknown[]) => void;
    throws: (...args: unknown[]) => void;
  };
}
declare module "node:test" {
  export function test(name: string, callback: () => void): void;
}
