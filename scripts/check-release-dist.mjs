import { access, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

const packagesRoot = resolve(process.cwd(), "packages");
const packages = await readdir(packagesRoot, { withFileTypes: true });
const report = {};

for (const entry of packages) {
  if (!entry.isDirectory()) continue;
  const dist = join(packagesRoot, entry.name, "dist");
  await access(join(dist, "index.js"));
  await access(join(dist, "index.d.ts"));
  const files = await readdir(dist, { recursive: true });
  report[entry.name] = files.filter((file) => file.endsWith(".js")).length;
}

console.log(JSON.stringify(report));
