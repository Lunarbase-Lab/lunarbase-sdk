# Normalized events schema

Version 1 is the transport-independent boundary shared by Base Flashblocks,
Monad execution-events, and Arbitrum Nitro adapters. `sourceSequence` and
`sourceSubIndex` are opaque ordering metadata; reducers use the EVM cursor and
must never assume that a filtered stream has contiguous source sequence values.

`Gap` is terminal for the current source session. The consumer must preserve
the last canonical checkpoint, resnapshot/backfill, and only then publish a
fresh ready state.
