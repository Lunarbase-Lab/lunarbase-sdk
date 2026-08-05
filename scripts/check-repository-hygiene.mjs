import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, extname, isAbsolute, relative, resolve } from "node:path";

import { checkPackagePolicy } from "./check-package-policy.mjs";

const root = process.cwd();
const failures = [];

const requiredPaths = [
  "LICENSE",
  "SECURITY.md",
  "CONTRIBUTING.md",
  ".github/dependabot.yml",
  "docs/INTEGRATION.md",
  "examples/indexer/.env.example",
];

const retiredPaths = [
  "abi/Core.json",
  "docs/INTEGRATION_SPECIFICATION.tex",
  "schemas/normalized-events/v1.schema.json",
  "schemas/quote-state/v3.schema.json",
];

const formattingTargets = String.raw`"**/*.{ts,mts,cts,js,mjs,cjs,json,yml,yaml,md}"`;

const expectedRootScripts = new Map([
  ["format", `prettier --write ${formattingTargets}`],
  ["format:check", `prettier --check ${formattingTargets}`],
  ["lint", "eslint packages examples/typescript scripts --max-warnings=0"],
]);

const requiredComposeEnvironment = [
  "LUNARBASE_CORE",
  "LUNARBASE_ROUTER",
  "LUNARBASE_DEPLOYMENT_BLOCK",
  "LUNARBASE_EXPECTED_IMPLEMENTATION",
  "LUNARBASE_EXPECTED_IMPLEMENTATION_CODE_HASH",
];

const wordingRules = [
  {
    id: "legacy",
    label: "legacy wording",
    expression: /\blegacy\b/i,
  },
  {
    id: "historical",
    label: "historical implementation wording",
    expression: /\bhistorical(?:ly)?\b/i,
  },
  {
    id: "experimental",
    label: "experimental status wording",
    expression: /\bexperimental\b/i,
  },
  {
    id: "release-candidate",
    label: "release-candidate wording",
    expression: /\brelease[-\s]+candidate\b/i,
  },
  {
    id: "repository-only",
    label: "repository-only wording",
    expression: /\brepository[-\s]+only\b/i,
  },
  {
    id: "pinned-revision",
    label: "pinned commit or revision wording",
    expression:
      /\b(?:pinned[-\s]+(?:commit|revision)|(?:commit|revision)[-\s]+pinned|pinned\s+to\s+(?:a\s+)?(?:commit|revision))\b/i,
  },
  {
    id: "commit-url",
    label: "commit-specific repository URL",
    expression: /https?:\/\/[^\s)>\]]+\/(?:commit|commits|blob|tree)\/[0-9a-f]{7,40}(?=[/#?\s)>\]]|$)/i,
  },
  {
    id: "organizational-repository",
    label: "organizational repository wording",
    expression: /\b(?:private|internal)\s+(?:contracts\s+)?repository\b/i,
  },
  {
    id: "unix-developer-path",
    label: "absolute developer home path",
    expression: /\/(?:Users|home)\/[A-Za-z0-9._-]+(?:[\\/]|$)/,
  },
  {
    id: "windows-developer-path",
    label: "absolute developer home path",
    expression: /\b[A-Za-z]:\\Users\\[^\\\s]+(?:\\|$)/i,
  },
];

const proseExtensions = new Set([".adoc", ".markdown", ".md", ".mdx", ".rst", ".tex", ".txt"]);

const slashCommentExtensions = new Set([
  ".c",
  ".cc",
  ".cjs",
  ".cpp",
  ".h",
  ".hpp",
  ".js",
  ".jsx",
  ".mjs",
  ".rs",
  ".sol",
  ".ts",
  ".tsx",
]);

const hashCommentExtensions = new Set([".bash", ".sh", ".toml", ".yaml", ".yml", ".zsh"]);

function addFailure(message) {
  failures.push(message);
}

function repoPath(file) {
  return relative(root, file).split("\\").join("/");
}

function isAllowedHistoricalContext(line) {
  return /\b(?:chain\s+history|historical(?:ly)?\s+(?:block|blocks|chain|data|event|events|log|logs|query|queries|range|ranges|read|reads|state))\b/i.test(
    line,
  );
}

function checkWording(file, lineNumber, text) {
  for (const rule of wordingRules) {
    if (!rule.expression.test(text)) continue;
    if (rule.id === "historical" && isAllowedHistoricalContext(text)) {
      continue;
    }
    addFailure(`${file}:${lineNumber}: ${rule.label}: ${text.trim().slice(0, 180)}`);
  }
}

function slashCommentLines(source, extension) {
  const comments = [];
  const lines = source.split(/\r?\n/);
  let inBlock = false;

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex];
    const fragments = [];
    let quote = null;
    let escaped = false;

    for (let index = 0; index < line.length; index += 1) {
      const character = line[index];
      const next = line[index + 1];

      if (inBlock) {
        const end = line.indexOf("*/", index);
        if (end === -1) {
          fragments.push(line.slice(index));
          index = line.length;
        } else {
          fragments.push(line.slice(index, end));
          inBlock = false;
          index = end + 1;
        }
        continue;
      }

      if (quote !== null) {
        if (escaped) {
          escaped = false;
        } else if (character === "\\") {
          escaped = true;
        } else if (character === quote) {
          quote = null;
        }
        continue;
      }

      if (character === '"' || character === "`") {
        quote = character;
        continue;
      }

      if (character === "'") {
        const rustLifetime = extension === ".rs" && /[A-Za-z_]/.test(next ?? "") && line[index + 2] !== "'";
        if (!rustLifetime) quote = character;
        continue;
      }

      if (character === "/" && next === "/") {
        fragments.push(line.slice(index + 2));
        break;
      }

      if (character === "/" && next === "*") {
        inBlock = true;
        index += 1;
      }
    }

    const text = fragments.join(" ").trim();
    if (text.length > 0) {
      comments.push({ line: lineIndex + 1, text });
    }
  }

  return comments;
}

function hashCommentLines(source) {
  const comments = [];
  const lines = source.split(/\r?\n/);

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex];
    if (lineIndex === 0 && line.startsWith("#!")) continue;

    let quote = null;
    let escaped = false;

    for (let index = 0; index < line.length; index += 1) {
      const character = line[index];

      if (quote !== null) {
        if (escaped) {
          escaped = false;
        } else if (character === "\\") {
          escaped = true;
        } else if (character === quote) {
          quote = null;
        }
        continue;
      }

      if (character === "'" || character === '"') {
        quote = character;
        continue;
      }

      if (character === "#") {
        const text = line.slice(index + 1).trim();
        if (text.length > 0) {
          comments.push({ line: lineIndex + 1, text });
        }
        break;
      }
    }
  }

  return comments;
}

function scanPublicProseAndComments(files) {
  for (const file of files) {
    const extension = extname(file).toLowerCase();
    const name = basename(file);
    const display = repoPath(file);
    let source;

    if (
      proseExtensions.has(extension) ||
      slashCommentExtensions.has(extension) ||
      hashCommentExtensions.has(extension) ||
      name === "Dockerfile" ||
      name === "Makefile" ||
      name.endsWith(".env.example") ||
      display.startsWith(".githooks/")
    ) {
      source = readFileSync(file, "utf8");
    } else {
      continue;
    }

    if (proseExtensions.has(extension)) {
      source.split(/\r?\n/).forEach((line, index) => {
        checkWording(display, index + 1, line);
      });
      continue;
    }

    const extracted = slashCommentExtensions.has(extension)
      ? slashCommentLines(source, extension)
      : hashCommentLines(source);

    for (const comment of extracted) {
      checkWording(display, comment.line, comment.text);
    }
  }
}

function markdownLinkTargets(source) {
  const links = [];
  const inline = /!?\[[^\]]*\]\(\s*(<[^>]+>|[^)\s]+)(?:\s+["'][^"']*["'])?\s*\)/g;
  const references = /^\s*\[[^\]]+\]:\s*(<[^>]+>|\S+)/gm;

  for (const expression of [inline, references]) {
    for (const match of source.matchAll(expression)) {
      let target = match[1];
      if (target.startsWith("<") && target.endsWith(">")) {
        target = target.slice(1, -1);
      }
      const line = source.slice(0, match.index).split(/\r?\n/).length;
      links.push({ line, target });
    }
  }

  return links;
}

function checkMarkdownLinks(files) {
  for (const file of files) {
    if (![".markdown", ".md", ".mdx"].includes(extname(file).toLowerCase())) {
      continue;
    }

    const source = readFileSync(file, "utf8");
    const seen = new Set();

    for (const link of markdownLinkTargets(source)) {
      let target = link.target.trim().replaceAll("\\ ", " ");
      if (
        target.length === 0 ||
        target.startsWith("#") ||
        target.startsWith("//") ||
        /^[A-Za-z][A-Za-z0-9+.-]*:/.test(target)
      ) {
        continue;
      }

      target = target.split("#", 1)[0].split("?", 1)[0];
      if (target.length === 0) continue;

      try {
        target = decodeURIComponent(target);
      } catch {
        addFailure(`${repoPath(file)}:${link.line}: malformed local Markdown link: ${link.target}`);
        continue;
      }

      const key = `${link.line}:${target}`;
      if (seen.has(key)) continue;
      seen.add(key);

      const resolved = isAbsolute(target) ? resolve(root, `.${target}`) : resolve(dirname(file), target);

      if (!existsSync(resolved)) {
        addFailure(`${repoPath(file)}:${link.line}: missing local Markdown link target: ${link.target}`);
      }
    }
  }
}

function checkToolingSurface() {
  const manifest = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  for (const [name, expected] of expectedRootScripts) {
    if (manifest.scripts?.[name] !== expected) {
      addFailure(`package.json: scripts.${name} must match the repository tooling surface`);
    }
  }
}

function checkDependencyAutomation() {
  const configPath = resolve(root, ".github/dependabot.yml");
  if (!existsSync(configPath)) return;

  const source = readFileSync(configPath, "utf8");
  if (!/^version:\s*2\s*$/m.test(source)) {
    addFailure(".github/dependabot.yml: configuration version must be 2");
  }

  const matches = [...source.matchAll(/^ {2}- package-ecosystem:\s*["']?([a-z-]+)["']?\s*$/gm)];
  const ecosystems = matches.map((match) => match[1]).sort();
  const expected = ["cargo", "docker", "docker-compose", "github-actions", "npm"];

  if (ecosystems.join("\n") !== expected.join("\n")) {
    addFailure(
      ".github/dependabot.yml: ecosystems must be exactly: " +
        expected.join(", ") +
        "; found: " +
        ecosystems.join(", "),
    );
  }

  matches.forEach((match, index) => {
    const next = matches[index + 1]?.index ?? source.length;
    const block = source.slice(match.index, next);
    const ecosystem = match[1];

    const directoryExpression =
      ecosystem === "docker-compose"
        ? /^ {4}directory:\s*"?\/examples\/indexer"?\s*$/m
        : /^ {4}directory:\s*"?\/"?\s*$/m;

    for (const [label, expression] of [
      ["configured directory", directoryExpression],
      ["weekly schedule", /^ {6}interval:\s*["']?weekly["']?\s*$/m],
      ["open pull request limit of 5", /^ {4}open-pull-requests-limit:\s*5\s*$/m],
      ["patch update group", /^ {10}- ["']?patch["']?\s*$/m],
    ]) {
      if (!expression.test(block)) {
        addFailure(".github/dependabot.yml: " + ecosystem + " must define " + label);
      }
    }

    if (ecosystem !== "cargo" && !/^ {10}- ["']?minor["']?\s*$/m.test(block)) {
      addFailure(".github/dependabot.yml: " + ecosystem + " must define minor update grouping");
    }
    if (
      ecosystem === "npm" &&
      (!/^ {8}dependency-type:\s*["']?production["']?\s*$/m.test(block) ||
        !/^ {8}dependency-type:\s*["']?development["']?\s*$/m.test(block))
    ) {
      addFailure(".github/dependabot.yml: npm must group production and development updates separately");
    }
    if (ecosystem === "docker" && !/^ {6}- dependency-name:\s*"?rust"?\s*$/m.test(block)) {
      addFailure(".github/dependabot.yml: Docker updates must keep the synchronized Rust image pin ignored");
    }
    if (ecosystem === "github-actions" && !/^ {6}- dependency-name:\s*"?dtolnay\/rust-toolchain"?\s*$/m.test(block)) {
      addFailure(".github/dependabot.yml: GitHub Actions must keep the synchronized Rust toolchain pin ignored");
    }
  });
}

function checkComposeExample() {
  const compose = readFileSync(resolve(root, "examples/indexer/docker-compose.yml"), "utf8");
  for (const name of requiredComposeEnvironment) {
    if (!compose.includes(`\${${name}:?`)) {
      addFailure(`examples/indexer/docker-compose.yml: ${name} must be required explicitly`);
    }
  }

  const profile = readFileSync(resolve(root, "examples/indexer/config/production.base.toml"), "utf8");
  for (const field of [
    "core",
    "router",
    "deployment_block",
    "expected_implementation",
    "expected_implementation_code_hash",
  ]) {
    if (new RegExp(`^${field}\\s*=`, "m").test(profile)) {
      addFailure(`examples/indexer/config/production.base.toml: ${field} must be supplied by the operator`);
    }
  }
}

function checkRequiredAndRetiredPaths() {
  for (const path of requiredPaths) {
    if (!existsSync(resolve(root, path))) {
      addFailure(`${path}: required release-readiness file is missing`);
    }
  }

  for (const path of retiredPaths) {
    if (existsSync(resolve(root, path))) {
      addFailure(`${path}: retired release artifact must be removed`);
    }
  }
}

function checkQuoteFixture() {
  const fixturePath = resolve(root, "fixtures/quote-vectors.json");
  let fixture;

  try {
    fixture = JSON.parse(readFileSync(fixturePath, "utf8"));
  } catch (error) {
    addFailure(`fixtures/quote-vectors.json: invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
    return;
  }

  if (fixture.schemaVersion !== "1") {
    addFailure(`fixtures/quote-vectors.json: schemaVersion must be 1`);
  }

  if (fixture.mathCompatibilityVersion !== "lunarbase-pmm-v2") {
    addFailure(`fixtures/quote-vectors.json: mathCompatibilityVersion must be lunarbase-pmm-v2`);
  }

  if (!Array.isArray(fixture.vectors)) {
    addFailure(`fixtures/quote-vectors.json: vectors must be an array`);
    return;
  }

  fixture.vectors.forEach((vector, index) => {
    if (vector !== null && typeof vector === "object" && Object.hasOwn(vector, "router")) {
      addFailure(`fixtures/quote-vectors.json: vectors[${index}].router is not part of the pure-math schema`);
    }
  });
}

function repositoryFiles() {
  const output = execFileSync("git", ["ls-files", "-co", "--exclude-standard", "-z"], { cwd: root, encoding: "utf8" });

  return [...new Set(output.split("\0").filter(Boolean))]
    .map((path) => resolve(root, path))
    .filter((path) => {
      try {
        return statSync(path).isFile();
      } catch {
        return false;
      }
    })
    .sort();
}

const files = repositoryFiles();

checkRequiredAndRetiredPaths();
checkQuoteFixture();
checkPackagePolicy({ root, addFailure, checkWording, repoPath });
checkToolingSurface();
checkDependencyAutomation();
checkComposeExample();
scanPublicProseAndComments(files);
checkMarkdownLinks(files);

if (failures.length > 0) {
  console.error("Repository hygiene check failed:");
  for (const failure of failures.sort()) {
    console.error(`- ${failure}`);
  }
  process.exitCode = 1;
} else {
  console.log(`Repository hygiene check passed (${files.length} tracked and nonignored files checked).`);
}
