import { rm } from "node:fs/promises";
import { resolve } from "node:path";

const targets = process.argv.slice(2);
if (targets.length === 0) {
  throw new Error("clean-dist requires at least one output directory");
}

for (const target of targets) {
  const resolved = resolve(process.cwd(), target);
  await rm(resolved, { recursive: true, force: true });
}
