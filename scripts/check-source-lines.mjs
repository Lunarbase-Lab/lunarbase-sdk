import { readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const ROOTS = ["crates", "packages", "examples", "scripts"];
const SOURCE_EXTENSIONS = new Set([".rs", ".ts", ".mjs"]);
const IGNORED_DIRECTORIES = new Set(["dist", "node_modules", "target"]);
const MAX_LINES = 500;
const BASELINE_PATH = join(process.cwd(), "scripts/source-size-baseline.json");
const BASELINE = JSON.parse(readFileSync(BASELINE_PATH, "utf8"));

for (const [path, lines] of Object.entries(BASELINE)) {
  if (!Number.isInteger(lines) || lines <= MAX_LINES) {
    throw new Error(`invalid source-size baseline for ${path}: ${lines}`);
  }
}

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
const oversizedByPath = new Map(oversized.map((entry) => [entry.path, entry.lines]));

const regressions = oversized.filter(({ path, lines }) => lines > (BASELINE[path] ?? MAX_LINES));
const staleBaseline = Object.keys(BASELINE).filter((path) => !oversizedByPath.has(path));

for (const path of staleBaseline) {
  console.error(`${path}: no longer exceeds ${MAX_LINES}; remove its baseline entry`);
}
for (const { path, lines } of regressions) {
  const allowed = BASELINE[path] ?? MAX_LINES;
  console.error(`${path}: ${lines} code lines (allowed ${allowed})`);
}

if (staleBaseline.length > 0 || regressions.length > 0) {
  process.exitCode = 1;
} else {
  console.log(
    `Source-size policy passed: maximum ${MAX_LINES}, ${Object.keys(BASELINE).length} approved baseline exceptions.`,
  );
}
