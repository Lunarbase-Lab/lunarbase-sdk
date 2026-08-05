import { strict as assert } from "node:assert";
import { test } from "node:test";
import type { Hex } from "ox/Hex";
import { quoteCriticalTopics } from "../protocol/abi.js";
import { validateFilterTopics } from "./filter.js";

const UNKNOWN = `0x${"99".repeat(32)}` as Hex;

test("client filter accepts only an empty or complete quote-critical topic set", () => {
  const required = quoteCriticalTopics();
  assert.doesNotThrow(() => validateFilterTopics([]));
  assert.doesNotThrow(() => validateFilterTopics([...required].reverse()));

  const invalid: readonly (readonly Hex[])[] = [
    required.slice(0, -1),
    [...required.slice(0, -1), UNKNOWN],
    [...required.slice(0, -1), required[0]!],
  ];
  for (const topics of invalid) assert.throws(() => validateFilterTopics(topics), { code: "SOURCE" });
});
