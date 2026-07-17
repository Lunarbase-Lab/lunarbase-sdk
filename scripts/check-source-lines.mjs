import { readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const ROOTS = ["crates", "packages", "scripts"];
const SOURCE_EXTENSIONS = new Set([".rs", ".ts", ".mjs"]);
const IGNORED_DIRECTORIES = new Set(["dist", "node_modules", "target"]);
const MAX_LINES = 500;

function sourceFiles(directory) {
  const files = [];
  for (const entry of readdirSync(directory)) {
    if (IGNORED_DIRECTORIES.has(entry)) continue;
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) {
      files.push(...sourceFiles(path));
    } else if (SOURCE_EXTENSIONS.has(extname(path))) {
      files.push(path);
    }
  }
  return files;
}

const oversized = ROOTS.flatMap(sourceFiles)
  .map((path) => ({
    path: relative(process.cwd(), path),
    lines: readFileSync(path, "utf8").split(/\r?\n/u).length - 1,
  }))
  .filter(({ lines }) => lines > MAX_LINES)
  .sort((left, right) => right.lines - left.lines);

if (oversized.length > 0) {
  for (const { path, lines } of oversized) {
    console.error(`${path}: ${lines} lines (maximum ${MAX_LINES})`);
  }
  process.exitCode = 1;
} else {
  console.log(`All Rust/TypeScript source files are within ${MAX_LINES} lines.`);
}
