import { readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const ROOTS = ["crates", "packages", "examples", "scripts"];
const SOURCE_EXTENSIONS = new Set([".rs", ".ts", ".mjs"]);
const IGNORED_DIRECTORIES = new Set(["dist", "node_modules", "target"]);
const MAX_LINES = 500;

function codeLineCount(source) {
  let inBlockComment = false;
  let count = 0;

  for (const line of source.split(/\r?\n/u)) {
    let offset = 0;
    let hasCode = false;

    while (offset < line.length) {
      if (inBlockComment) {
        const end = line.indexOf("*/", offset);
        if (end === -1) break;
        inBlockComment = false;
        offset = end + 2;
        continue;
      }

      while (offset < line.length && /\s/u.test(line[offset])) offset += 1;
      if (offset >= line.length || line.startsWith("//", offset)) break;
      if (line.startsWith("/*", offset)) {
        inBlockComment = true;
        offset += 2;
        continue;
      }

      hasCode = true;
      break;
    }

    if (hasCode) count += 1;
  }

  return count;
}

const counterFixture = `
/// A documentation-only line.
// A regular comment-only line.
/*
 * A block comment.
 */
const value = 1;
`;
if (codeLineCount(counterFixture) !== 1) {
  throw new Error("source line counter must exclude comments and blank lines");
}

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
    lines: codeLineCount(readFileSync(path, "utf8")),
  }))
  .filter(({ lines }) => lines > MAX_LINES)
  .sort((left, right) => right.lines - left.lines);

if (oversized.length > 0) {
  for (const { path, lines } of oversized) {
    console.error(`${path}: ${lines} code lines (maximum ${MAX_LINES})`);
  }
  process.exitCode = 1;
} else {
  console.log(
    `All Rust/TypeScript source files are within ${MAX_LINES} non-comment code lines.`,
  );
}
