import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { URL } from "node:url";

const root = new URL("../", import.meta.url);
const readJson = async (path) => JSON.parse(await readFile(new URL(path, root), "utf8"));

const schema = await readJson("schemas/durable-events/v2.schema.json");
const reorgFixture = await readJson("fixtures/durable-events-v2/reorg-cycle.json");
const gapFixture = await readJson("fixtures/durable-events-v2/terminal-gap.json");

const pattern = (name) => new RegExp(schema.$defs[name].pattern);
const idPattern = pattern("id");
const addressPattern = pattern("address");
const hashPattern = pattern("hash");
const uint64Pattern = pattern("uint64");
const positivePattern = pattern("positive");
const allowedFields = new Set(Object.keys(schema.properties));
const commonFields = schema.required;
const forbiddenPayloadDuplicates = ["rawLog", "topic0", "eventName", "arguments", "decodeError"];

const logFields = [
  "logicalLogId",
  "lifecycleRevision",
  "commitment",
  "blockNumber",
  "executionBlockNumber",
  "blockHash",
  "transactionHash",
  "transactionIndex",
  "logIndex",
  "topics",
  "data",
];

const reorgFields = [
  "reorgId",
  "ancestorBlockNumber",
  "ancestorExecutionBlockNumber",
  "ancestorBlockHash",
  "oldTipBlockNumber",
  "oldTipExecutionBlockNumber",
  "oldTipBlockHash",
  "oldTipParentHash",
  "newTipBlockNumber",
  "newTipExecutionBlockNumber",
  "newTipBlockHash",
  "newTipParentHash",
  "finalizedBlockNumber",
  "finalizedBlockHash",
  "revertedLogCount",
  "appliedLogCount",
];

function requireFields(record, fields) {
  for (const field of fields) {
    assert.ok(Object.hasOwn(record, field), `missing ${field}`);
  }
}

function validateRecord(record) {
  requireFields(record, commonFields);
  assert.equal(record.schemaVersion, "2");
  assert.match(record.recordId, idPattern);
  assert.match(record.chainId, uint64Pattern);
  assert.match(record.core, addressPattern);
  for (const field of Object.keys(record)) {
    assert.ok(allowedFields.has(field), `unknown field ${field}`);
  }
  for (const field of forbiddenPayloadDuplicates) {
    assert.equal(Object.hasOwn(record, field), false);
  }

  if (record.recordType === "log") {
    requireFields(record, logFields);
    assert.ok(["applied", "reverted"].includes(record.operation));
    assert.match(record.logicalLogId, idPattern);
    assert.match(record.lifecycleRevision, positivePattern);
    assert.match(record.blockHash, hashPattern);
    if (record.parentHash !== undefined) assert.match(record.parentHash, hashPattern);
    assert.match(record.transactionHash, hashPattern);
    const topics = JSON.parse(record.topics);
    assert.ok(topics.length >= 1 && topics.length <= 4);
    topics.forEach((topic) => assert.match(topic, hashPattern));
    if (record.operation === "reverted") {
      assert.match(record.reorgId, idPattern);
    }
    return;
  }

  if (record.recordType === "reorg") {
    requireFields(record, reorgFields);
    assert.ok(["begin", "commit"].includes(record.operation));
    assert.match(record.reorgId, idPattern);
    return;
  }

  assert.equal(record.recordType, "gap");
  assert.equal(record.operation, "halt");
  requireFields(record, ["gapReason", "lastTrustedBlockNumber", "lastTrustedBlockHash"]);
  assert.match(record.lastTrustedBlockNumber, uint64Pattern);
  assert.match(record.lastTrustedBlockHash, hashPattern);
}

test("schema v2 keeps the common path flat and payload-allocation safe", () => {
  assert.equal(schema.additionalProperties, false);
  assert.equal(schema.properties.schemaVersion.const, "2");
  assert.equal(schema.properties.topics.type, "string");
  for (const field of forbiddenPayloadDuplicates) {
    assert.equal(Object.hasOwn(schema.properties, field), false);
  }
});

test("multi-log fork fixture has exact correction ordering and identities", () => {
  const records = reorgFixture.records;
  records.forEach(validateRecord);
  const beginIndex = records.findIndex((record) => record.operation === "begin");
  const commitIndex = records.findIndex((record) => record.operation === "commit");
  assert.equal(beginIndex, 2);
  assert.equal(commitIndex, records.length - 1);

  const initial = records.slice(0, beginIndex);
  const correction = records.slice(beginIndex + 1, commitIndex);
  const reverted = correction.filter((record) => record.operation === "reverted");
  const applied = correction.filter((record) => record.operation === "applied");
  const begin = records[beginIndex];
  const commit = records[commitIndex];

  assert.deepEqual(
    reverted.map((record) => record.logicalLogId),
    initial.map((record) => record.logicalLogId).reverse(),
  );
  assert.deepEqual(
    applied.map((record) => record.logIndex),
    ["0", "1"],
  );
  assert.ok(correction.every((record) => record.reorgId === begin.reorgId));
  assert.equal(commit.reorgId, begin.reorgId);
  for (const field of reorgFields.filter((field) => field !== "reorgId")) {
    assert.equal(commit[field], begin[field], `barrier mismatch: ${field}`);
  }
  assert.equal(Number(begin.revertedLogCount), reverted.length);
  assert.equal(Number(begin.appliedLogCount), applied.length);
  assert.notEqual(initial[0].recordId, reverted[1].recordId);
  assert.equal(initial[0].logicalLogId, reverted[1].logicalLogId);
});

test("terminal gap fixture is bounded and cannot masquerade as a log", () => {
  validateRecord(gapFixture);
  assert.ok(gapFixture.gapDetails.length <= 512);
  assert.equal(Object.hasOwn(gapFixture, "logicalLogId"), false);
  assert.equal(Object.hasOwn(gapFixture, "reorgId"), false);
});
