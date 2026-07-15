# Solidity differential reference

This Foundry project imports the pinned local checkout of
`lunarbase-contracts` at commit `24db47b866e8150a0d91cffd80efe49df85179b5`.
`QuoteReferenceHarness` exposes the complete Solidity `QuoteResult`; it is an
oracle boundary, not a second implementation of the quote formulas.

Run from this directory:

```text
forge test
```

If the contracts checkout is moved, update the remappings together with the
contract commit recorded in `SPECIFICATION.md`. A commit/code-hash mismatch
must fail CI rather than silently changing the oracle.
