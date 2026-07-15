// SPDX-License-Identifier: MIT
pragma solidity 0.8.35;

import { Test } from "@forge-std/src/Test.sol";
import { QuoteReferenceHarness } from "../src/QuoteReferenceHarness.sol";
import { UpdateCalldata } from "@/interfaces/ILanes.sol";
import { QuoteResult } from "@/libraries/LanesLib.sol";
import { Asset } from "@/types/Asset.sol";
import { BPS } from "@/utils/Constants.sol";

contract DifferentialVectorsTest is Test {
    QuoteReferenceHarness internal core;
    Asset internal cash = Asset.wrap(address(1));
    Asset internal asset = Asset.wrap(address(2));
    address internal owner = address(100);
    address internal router = address(3);
    address[8] internal operators;

    function setUp() public {
        for (uint256 index; index < operators.length; ++index) operators[index] = address(uint160(10 + index));
        core = new QuoteReferenceHarness(cash, owner, operators);
        vm.prank(owner);
        core.addLane(asset);
        vm.prank(owner);
        core.setSlippageKBps(asset, 0);
        vm.prank(operators[0]);
        core.update_0x01e44214(_update(asset, 2 ether, 10_000, 10_000));
        core.creditPrincipal(asset, 1_000_000);
        vm.prank(owner);
        core.setBlacklistFeeMultiplier(1);
        vm.prank(owner);
        core.setPartnerFee(router, asset, 500_000);
    }

    function test_directCashToAssetExactIn_matchesSharedVector() public {
        vm.prank(router);
        QuoteResult memory result = core.quoteExactInResult(100, cash, asset);
        assertEq(result.amountIn, 100);
        assertEq(result.amountOut, 49);
        assertEq(Asset.unwrap(result.feeAsset), Asset.unwrap(asset));
        assertEq(result.feeAmount, 1);
        assertEq(result.partnerFee, 1);
        assertEq(result.treasuryFee, 0);
    }

    function test_directAssetToCashExactOut_matchesSharedVector() public {
        vm.prank(router);
        QuoteResult memory result = core.quoteExactOutResult(asset, 100, cash);
        assertEq(result.amountIn, 51);
        assertEq(result.amountOut, 100);
        assertEq(Asset.unwrap(result.feeAsset), Asset.unwrap(asset));
        assertEq(result.feeAmount, 1);
        assertEq(result.partnerFee, 1);
        assertEq(result.treasuryFee, 0);
    }

    function _update(Asset lane, uint256 price, uint256 askFee, uint256 bidFee)
        internal
        pure
        returns (UpdateCalldata[] memory data)
    {
        data = new UpdateCalldata[](1);
        data[0] = UpdateCalldata({ asset: lane, price: uint112(price), fees: uint40(askFee | (bidFee << 20)) });
    }
}
