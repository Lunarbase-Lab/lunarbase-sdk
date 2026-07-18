import assert from "node:assert/strict";
import test from "node:test";
import { parseAddress } from "@lunarbase/math";
import { buildQuoteRequests } from "./quotes.js";

test("builds two directions for every lane", () => {
  const cash = parseAddress("0x0000000000000000000000000000000000000001");
  const first = parseAddress("0x0000000000000000000000000000000000000002");
  const second = parseAddress("0x0000000000000000000000000000000000000003");
  const requests = buildQuoteRequests(cash, [first, second], 42n);

  assert.equal(requests.length, 4);
  assert.deepEqual(requests[0], { assetIn: first, assetOut: cash, amount: 42n, mode: "ExactIn" });
  assert.deepEqual(requests[1], { assetIn: cash, assetOut: first, amount: 42n, mode: "ExactIn" });
  assert.deepEqual(requests[2], { assetIn: second, assetOut: cash, amount: 42n, mode: "ExactIn" });
  assert.deepEqual(requests[3], { assetIn: cash, assetOut: second, amount: 42n, mode: "ExactIn" });
});
