// SPDX-License-Identifier: MIT
pragma solidity 0.8.35;

import { Core } from "@/Core.sol";
import { LanesLib, QuoteResult } from "@/libraries/LanesLib.sol";
import { PartnersLib } from "@/libraries/PartnersLib.sol";
import { ReservesLib } from "@/libraries/ReservesLib.sol";
import { Asset } from "@/types/Asset.sol";
import { Word } from "@/types/Word.sol";

/// Thin oracle boundary around the pinned Solidity libraries. The tests call
/// the real Core/LanesLib implementation and expose the complete QuoteResult;
/// no quote formula is duplicated in this repository.
contract QuoteReferenceHarness is Core {
    using LanesLib for LanesLib.State;
    using PartnersLib for PartnersLib.State;
    using ReservesLib for ReservesLib.State;

    constructor(Asset cash, address owner, address[8] memory operators) Core(cash, owner, operators) { }

    function creditPrincipal(Asset asset, uint256 amount) external {
        ReservesLib.state().creditTotalPrincipalAmount(asset, amount);
    }

    function setSlot0(Asset asset, Word slot0) external {
        LanesLib.state().lanes[asset].slot0 = slot0;
    }

    function quoteExactInResult(uint256 amountIn, Asset assetIn, Asset assetOut)
        external
        view
        returns (QuoteResult memory)
    {
        return LanesLib.state().quoteExactIn(
            PartnersLib.state(), ReservesLib.state(), CASH, msg.sender, amountIn, assetIn, assetOut
        );
    }

    function quoteExactOutResult(Asset assetIn, uint256 amountOut, Asset assetOut)
        external
        view
        returns (QuoteResult memory)
    {
        return LanesLib.state().quoteExactOut(
            PartnersLib.state(), ReservesLib.state(), CASH, msg.sender, assetIn, assetOut, amountOut
        );
    }
}
