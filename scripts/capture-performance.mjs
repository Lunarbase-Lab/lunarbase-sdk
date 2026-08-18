import { mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const settings = parseArguments(process.argv.slice(2));
const output = resolve(settings.output);
mkdirSync(output, { recursive: true });
if (readdirSync(output).length !== 0) fail(`output directory must be empty: ${output}`);

const cargo = process.env.CARGO || "cargo";
const benchmarkEnvironment = fingerprintEnvironment(cargo);
build([]);
for (const mode of ["timing", "mixed"]) {
  for (const lanes of [15, 64]) {
    for (const batch of [1, 16, 256]) {
      for (let run = 1; run <= settings.repetitions; run += 1) {
        capture(mode, lanes, batch, 128, run, settings.repetitions);
      }
    }
  }
}

build(["--features", "allocation-stats"]);
for (const lanes of [15, 64]) {
  for (const batch of [1, 16, 256]) {
    for (let run = 1; run <= settings.allocationRepetitions; run += 1) {
      capture("allocations", lanes, batch, 1, run, settings.allocationRepetitions);
    }
  }
}

function build(extra) {
  run(cargo, ["build", "--locked", "--release", "-p", "lunarbase-tools", "--bin", "lunarbase-quote-bench", ...extra]);
}

function capture(mode, lanes, batch, concurrency, runNumber, totalRuns) {
  const binary = resolve(
    process.env.CARGO_TARGET_DIR || "target",
    "release",
    process.platform === "win32" ? "lunarbase-quote-bench.exe" : "lunarbase-quote-bench",
  );
  const benchmarkArguments = [
    "--mode",
    mode,
    "--lanes",
    String(lanes),
    "--pairs",
    "100",
    "--batch-size",
    String(batch),
    "--concurrency",
    String(concurrency),
    "--warmup-calls",
    String(settings.warmupCalls),
    "--measured-quotes",
    String(settings.measuredQuotes),
    "--minimum-duration-ms",
    String(settings.minimumDurationMs),
    "--minimum-latency-samples",
    String(settings.minimumLatencySamples),
    "--latency-sample-capacity",
    String(settings.latencySampleCapacity),
    "--minimum-mixed-updates",
    String(settings.minimumMixedUpdates),
    "--minimum-mixed-rate-bps",
    String(settings.minimumMixedRateBps),
    "--minimum-mixed-applied-during-bps",
    String(settings.minimumMixedAppliedDuringBps),
  ];
  if (mode === "allocations") {
    benchmarkArguments.push("--allocation-calls", String(settings.allocationCalls));
  } else {
    benchmarkArguments.push("--mixed-events-per-second", String(settings.mixedEventsPerSecond));
  }
  const result = run(binary, benchmarkArguments, true, benchmarkEnvironment.variables);
  let report;
  try {
    report = JSON.parse(result.stdout);
  } catch (error) {
    fail(`benchmark returned invalid JSON: ${error.message}\n${result.stdout}`);
  }
  if (
    report.schemaVersion !== 2 ||
    report.mode !== mode ||
    report.buildProfile !== "release" ||
    report.scenarioId !== `${mode}-lanes${lanes}-pairs100-batch${batch}-c${concurrency}`
  ) {
    fail(`benchmark report identity mismatch: ${JSON.stringify(report)}`);
  }
  validateReport(report, mode, batch);
  const suffix = String(runNumber).padStart(String(totalRuns).length, "0");
  writeFileSync(resolve(output, `${report.scenarioId}-run${suffix}.json`), `${JSON.stringify(report, null, 2)}\n`, {
    flag: "wx",
  });
}

function run(command, commandArguments, captureOutput = false, extraEnvironment = {}) {
  const result = spawnSync(command, commandArguments, {
    cwd: process.cwd(),
    encoding: "utf8",
    stdio: captureOutput ? ["ignore", "pipe", "pipe"] : "inherit",
    maxBuffer: 8 * 1024 * 1024,
    env: { ...process.env, ...extraEnvironment },
  });
  if (result.error) fail(`${command} failed to start: ${result.error.message}`);
  if (result.status !== 0) {
    fail(`${command} exited with ${result.status}: ${result.stderr || ""}`);
  }
  return result;
}

function parseArguments(arguments_) {
  const values = new Map();
  for (let index = 0; index < arguments_.length; index += 2) {
    const key = arguments_[index];
    const value = arguments_[index + 1];
    if (!key?.startsWith("--") || value === undefined) fail("arguments must be --name value pairs");
    values.set(key.slice(2), value);
  }
  const outputValue = values.get("output");
  if (!outputValue) fail("--output is required");
  return {
    output: outputValue,
    repetitions: positiveInteger(values.get("repetitions") || process.env.PERF_REPETITIONS || "10", "repetitions"),
    allocationRepetitions: positiveInteger(
      values.get("allocation-repetitions") || process.env.PERF_ALLOCATION_REPETITIONS || "1",
      "allocation-repetitions",
    ),
    measuredQuotes: positiveInteger(process.env.MEASURED_QUOTES || "262144", "MEASURED_QUOTES"),
    allocationCalls: positiveInteger(process.env.ALLOCATION_CALLS || "4096", "ALLOCATION_CALLS"),
    warmupCalls: positiveInteger(process.env.WARMUP_CALLS || "1024", "WARMUP_CALLS"),
    mixedEventsPerSecond: positiveInteger(process.env.MIXED_EVENTS_PER_SECOND || "1000", "MIXED_EVENTS_PER_SECOND"),
    minimumDurationMs: positiveInteger(process.env.MINIMUM_DURATION_MS || "2000", "MINIMUM_DURATION_MS"),
    minimumLatencySamples: positiveInteger(process.env.MINIMUM_LATENCY_SAMPLES || "32768", "MINIMUM_LATENCY_SAMPLES"),
    latencySampleCapacity: positiveInteger(process.env.LATENCY_SAMPLE_CAPACITY || "65536", "LATENCY_SAMPLE_CAPACITY"),
    minimumMixedUpdates: positiveInteger(process.env.MINIMUM_MIXED_UPDATES || "1500", "MINIMUM_MIXED_UPDATES"),
    minimumMixedRateBps: boundedRate(process.env.MINIMUM_MIXED_RATE_BPS || "8000"),
    minimumMixedAppliedDuringBps: boundedRate(
      process.env.MINIMUM_MIXED_APPLIED_DURING_BPS || "8000",
      "MINIMUM_MIXED_APPLIED_DURING_BPS",
    ),
  };
}

function validateReport(report, mode, batch) {
  const policy = report.measurementPolicy;
  const expectedPolicy = {
    minimumDurationNs: settings.minimumDurationMs * 1_000_000,
    minimumLatencySamples: settings.minimumLatencySamples,
    latencySampleCapacity: settings.latencySampleCapacity,
    minimumMeasuredQuotes: settings.measuredQuotes,
    allocationCalls: settings.allocationCalls,
    minimumMixedUpdates: settings.minimumMixedUpdates,
    minimumMixedRateBps: settings.minimumMixedRateBps,
    minimumMixedAppliedDuringBps: settings.minimumMixedAppliedDuringBps,
    mixedPublisherRuntime: "dedicated-deadline",
  };
  if (JSON.stringify(policy) !== JSON.stringify(expectedPolicy))
    fail(`${report.scenarioId}: measurement policy mismatch`);
  const environment = report.environment;
  if (
    environment?.cpuModel !== benchmarkEnvironment.cpuModel ||
    environment?.rustcVersion !== benchmarkEnvironment.rustcVersion ||
    environment?.cargoVersion !== benchmarkEnvironment.cargoVersion ||
    environment?.harnessId !== "lunarbase-quote-hot-path-v2" ||
    !Number.isSafeInteger(environment?.logicalCpus) ||
    environment.logicalCpus <= 0
  ) {
    fail(`${report.scenarioId}: incomplete or mismatched environment fingerprint`);
  }
  if (report.measuredQuotes !== report.measuredCalls * batch) fail(`${report.scenarioId}: inconsistent measured work`);
  if (mode === "allocations") {
    if (report.measuredCalls !== settings.allocationCalls || report.timing !== null)
      fail(`${report.scenarioId}: allocation run was not exact`);
    return;
  }
  if (
    report.timing?.durationNs < expectedPolicy.minimumDurationNs ||
    report.timing?.latencyNs?.samples < expectedPolicy.minimumLatencySamples ||
    report.timing.latencyNs.samples > expectedPolicy.latencySampleCapacity ||
    report.measuredQuotes < expectedPolicy.minimumMeasuredQuotes
  ) {
    fail(`${report.scenarioId}: timing run did not meet sustained sampling bounds`);
  }
  if (mode === "mixed") validateMixed(report);
}

function validateMixed(report) {
  const load = report.eventLoad;
  const policy = report.measurementPolicy;
  const achievedRate = (load?.publishedUpdates * 1_000_000_000) / load?.durationNs;
  const requiredRate = (load?.configuredEventsPerSecond * policy.minimumMixedRateBps) / 10_000;
  const appliedDuringRatio = load?.appliedDuringMeasurement / load?.publishedDuringMeasurement;
  if (
    load?.durationNs < policy.minimumDurationNs ||
    load?.publishedUpdates < policy.minimumMixedUpdates ||
    load?.appliedUpdates !== load?.publishedUpdates ||
    !Number.isFinite(achievedRate) ||
    achievedRate < requiredRate ||
    !Number.isFinite(appliedDuringRatio) ||
    load?.appliedDuringMeasurement > load?.publishedDuringMeasurement ||
    appliedDuringRatio * 10_000 < policy.minimumMixedAppliedDuringBps
  ) {
    fail(`${report.scenarioId}: mixed publisher did not meet its duration, volume, rate, or apply contract`);
  }
}

function fingerprintEnvironment(cargoCommand) {
  const cargoVersion = versionLine(cargoCommand, ["-vV"]);
  const rustc = findRustc(cargoCommand);
  const rustcVersion = versionLine(rustc, ["-vV"]);
  const cpuModel = cpus()[0]?.model?.trim();
  if (!cpuModel) fail("could not determine CPU model");
  return {
    cpuModel,
    rustcVersion,
    cargoVersion,
    variables: {
      LUNARBASE_BENCH_CPU_MODEL: cpuModel,
      LUNARBASE_BENCH_RUSTC_VERSION: rustcVersion,
      LUNARBASE_BENCH_CARGO_VERSION: cargoVersion,
    },
  };
}

function findRustc(cargoCommand) {
  for (const candidate of [
    process.env.RUSTC,
    "rustc",
    cargoCommand.includes("/") ? resolve(dirname(cargoCommand), "rustc") : null,
  ]) {
    if (candidate && tryCommand(candidate, ["-vV"]).status === 0) return candidate;
  }
  const rustup = cargoCommand.includes("/") ? resolve(dirname(cargoCommand), "rustup") : "rustup";
  const result = tryCommand(rustup, ["which", "rustc"]);
  if (result.status === 0 && result.stdout.trim()) return result.stdout.trim();
  fail("could not locate rustc for the environment fingerprint");
}

function versionLine(command, arguments_) {
  const result = tryCommand(command, arguments_);
  if (result.status !== 0)
    fail(`could not fingerprint ${command}: ${result.stderr || result.error?.message || "unknown error"}`);
  return result.stdout.split(/\r?\n/, 1)[0].trim();
}

function tryCommand(command, arguments_) {
  return spawnSync(command, arguments_, { cwd: process.cwd(), encoding: "utf8" });
}

function boundedRate(value, name = "MINIMUM_MIXED_RATE_BPS") {
  const parsed = positiveInteger(value, name);
  if (parsed > 10_000) fail(`${name} must not exceed 10000`);
  return parsed;
}

function positiveInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) fail(`${name} must be a positive safe integer`);
  return parsed;
}

function fail(message) {
  console.error(`capture-performance: ${message}`);
  process.exit(1);
}
