import assert from "node:assert/strict";
import test from "node:test";
import { decodeErrorResult, encodeErrorResult, getAbiItem, toEventSelector } from "viem";
import { CORE_ACTOR_ABI, MOCK_TOKEN_ABI } from "./abi.js";

test("decodes errors bubbled from Core linked libraries", () => {
  const cases = [
    ["0x5c24a8b1", "Core__SwapUnavailable"],
    ["0x1ce6ec0a", "Core__ReserveUnderflow"],
    ["0x35278d12", "Overflow"],
    ["0xae47f702", "FullMulDivFailed"],
    ["0xad251c27", "MulDivFailed"],
    ["0x7939f424", "TransferFromFailed"],
    ["0x90b8ec18", "TransferFailed"],
  ] as const;

  for (const [data, errorName] of cases) {
    assert.equal(decodeErrorResult({ abi: CORE_ACTOR_ABI, data }).errorName, errorName);
  }
});

test("decodes Core and token errors with arguments", () => {
  const assetIn = "0x0000000000000000000000000000000000000001";
  const assetOut = "0x0000000000000000000000000000000000000002";
  const laneData = encodeErrorResult({
    abi: CORE_ACTOR_ABI,
    errorName: "LaneDoesNotExist",
    args: [assetIn, assetOut],
  });
  const laneError = decodeErrorResult({ abi: CORE_ACTOR_ABI, data: laneData });
  assert.equal(laneError.errorName, "LaneDoesNotExist");
  assert.deepEqual(laneError.args, [assetIn, assetOut]);

  const spender = "0x11116c60551889C6c01DDAD3A1fB3Cc95CbeBBbB";
  const allowanceData = encodeErrorResult({
    abi: MOCK_TOKEN_ABI,
    errorName: "ERC20InsufficientAllowance",
    args: [spender, 1n, 2n],
  });
  const allowanceError = decodeErrorResult({ abi: MOCK_TOKEN_ABI, data: allowanceData });
  assert.equal(allowanceError.errorName, "ERC20InsufficientAllowance");
  assert.deepEqual(allowanceError.args, [spender, 1n, 2n]);
});

test("keeps the deployed SwapExecuted event shape", () => {
  const event = getAbiItem({ abi: CORE_ACTOR_ABI, name: "SwapExecuted" });
  assert.equal(event.type, "event");
  assert.equal(toEventSelector(event), "0x108e8e1727f5a4319e8ca475dc4b99ed2ee0233818c8788b17aae0a8dfd647e9");
  assert.deepEqual(
    event.inputs.map((input) => [input.name, input.type, "indexed" in input ? input.indexed : false]),
    [
      ["router", "address", true],
      ["assetIn", "address", true],
      ["assetOut", "address", true],
      ["exactIn", "bool", false],
      ["amountIn", "uint256", false],
      ["amountOut", "uint256", false],
      ["feeAsset", "address", false],
      ["feeAmount", "uint256", false],
      ["partnerFee", "uint256", false],
      ["treasuryFee", "uint256", false],
    ],
  );
});
