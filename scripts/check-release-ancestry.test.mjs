import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

const root = process.cwd();
const workflowPath = resolve(root, ".github/workflows/release.yml");

function git(directory, ...args) {
  return execFileSync("git", args, { cwd: directory, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
}

function commit(directory, value, message) {
  writeFileSync(join(directory, "value.txt"), `${value}\n`);
  git(directory, "add", "value.txt");
  git(directory, "commit", "-m", message);
  return git(directory, "rev-parse", "HEAD");
}

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), "lunarbase-release-ancestry-"));
  t.after(() => rmSync(directory, { force: true, recursive: true }));
  const origin = join(directory, "origin.git");
  const worktree = join(directory, "worktree");

  git(directory, "init", "--bare", origin);
  git(directory, "init", "--initial-branch=main", worktree);
  git(worktree, "config", "user.email", "release-test@example.invalid");
  git(worktree, "config", "user.name", "Release Test");
  const base = commit(worktree, "base", "base");
  git(worktree, "tag", "v1.2.3");
  git(worktree, "remote", "add", "origin", origin);
  git(worktree, "push", "--set-upstream", "origin", "main");
  git(worktree, "push", "origin", "refs/tags/v1.2.3");

  return { base, worktree };
}

function fetchMain(worktree) {
  git(worktree, "fetch", "--no-tags", "origin", "+refs/heads/main:refs/remotes/origin/main");
}

function releaseGuard() {
  const workflow = readFileSync(workflowPath, "utf8");
  const step = workflow.indexOf("      - name: Verify release ancestry");
  assert.notEqual(step, -1);
  const runMarker = "        run: |\n";
  const run = workflow.indexOf(runMarker, step);
  assert.notEqual(run, -1);
  const lines = workflow.slice(run + runMarker.length).split("\n");
  const body = [];
  for (const line of lines) {
    if (line.startsWith("          ")) {
      body.push(line.slice(10));
      continue;
    }
    if (line === "") {
      body.push("");
      continue;
    }
    if (line.startsWith("      - ")) break;
    throw new Error(`unexpected workflow indentation: ${line}`);
  }
  return body.join("\n");
}

function check(worktree, tag) {
  return spawnSync("bash", ["--noprofile", "--norc", "-c", releaseGuard()], {
    cwd: worktree,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      GITHUB_TOKEN: "fixture-token",
      GIT_CONFIG_NOSYSTEM: "1",
      HOME: worktree,
      LANG: "C.UTF-8",
      PATH: "/usr/local/bin:/usr/bin:/bin",
      RELEASE_TAG: tag,
    },
  });
}

test("release ancestry accepts a checked-out tag contained in origin/main", (t) => {
  const { worktree } = fixture(t);
  commit(worktree, "main", "main");
  git(worktree, "push", "origin", "main");
  git(worktree, "checkout", "--detach", "v1.2.3");
  fetchMain(worktree);

  const result = check(worktree, "v1.2.3");
  assert.equal(result.status, 0, result.stderr);
});

test("release ancestry rejects a checkout that differs from the tag", (t) => {
  const { worktree } = fixture(t);
  commit(worktree, "main", "main");
  git(worktree, "push", "origin", "main");
  fetchMain(worktree);
  assert.notEqual(git(worktree, "rev-parse", "HEAD^{commit}"), git(worktree, "rev-parse", "v1.2.3^{commit}"));

  const result = check(worktree, "v1.2.3");
  assert.notEqual(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stderr, /checked out commit does not match/);
});

test("release ancestry rejects a tag outside origin/main", (t) => {
  const { base, worktree } = fixture(t);
  commit(worktree, "main", "main");
  git(worktree, "push", "origin", "main");
  git(worktree, "checkout", "--detach", base);
  commit(worktree, "side", "side");
  git(worktree, "tag", "v2.0.0");
  fetchMain(worktree);
  const precondition = spawnSync("git", ["merge-base", "--is-ancestor", "v2.0.0^{commit}", "origin/main^{commit}"], {
    cwd: worktree,
  });
  assert.notEqual(precondition.status, 0);

  const result = check(worktree, "v2.0.0");
  assert.notEqual(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stderr, /is not contained in origin\/main/);
});

test("publish workflow runs trusted ancestry checks before registry credentials", () => {
  const workflow = readFileSync(workflowPath, "utf8");
  const publishStart = workflow.indexOf("\n  publish:");
  assert.notEqual(publishStart, -1);
  const publish = workflow.slice(publishStart);
  const ancestryStep = publish.indexOf("- name: Verify release ancestry");
  const cargoCredential = publish.indexOf("CARGO_REGISTRY_TOKEN:");
  const npmCredential = publish.indexOf("NODE_AUTH_TOKEN:");
  const beforeCredentials = publish.slice(0, cargoCredential);
  const guard = releaseGuard();

  assert.match(workflow, /run: make [^\n]*\bdocker-image-check\b/);
  assert.match(publish, /environment: release/);
  assert.ok(publish.includes("CARGO_REGISTRY_TOKEN: " + "$" + "{{ secrets.CARGO_REGISTRY_TOKEN }}"));
  assert.ok(publish.includes("NPM_TOKEN: " + "$" + "{{ secrets.NPM_TOKEN }}"));
  assert.ok(ancestryStep > publish.indexOf("uses: actions/checkout@v6"));
  assert.ok(ancestryStep < cargoCredential);
  assert.ok(ancestryStep < npmCredential);
  assert.match(publish.slice(0, ancestryStep), /persist-credentials: false/);
  assert.match(publish.slice(0, ancestryStep), /ref: refs\/tags\/\$\{\{ env\.RELEASE_TAG \}\}/);
  assert.doesNotMatch(
    beforeCredentials,
    /\b(?:bash|sh|node|npm|pnpm|cargo|make)\s+(?:\.\/|scripts\/|packages\/|crates\/)/,
  );
  assert.doesNotMatch(beforeCredentials, /uses:\s+\.\//);
  assert.match(guard, /git .+ fetch --no-tags origin/s);
  assert.match(guard, /git rev-parse --verify "\$\{release_ref\}\^\{commit\}"/);
  assert.match(guard, /git rev-parse --verify 'HEAD\^\{commit\}'/);
  assert.match(guard, /git rev-parse --verify 'refs\/remotes\/origin\/main\^\{commit\}'/);
  assert.match(guard, /git merge-base --is-ancestor "\$\{release_commit\}" "\$\{main_commit\}"/);
  assert.ok(publish.indexOf("scripts/publish-crates.sh") > cargoCredential);
  assert.ok(publish.indexOf("scripts/publish-npm.mjs") > npmCredential);
});
