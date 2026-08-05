# Base Flashblocks WebSocket fixtures

`ws-frames.jsonl` follows the official Flashblocks application-facing RPC
surface: filtered `pendingLogs` plus progressive `newHeads`. The pending log
has populated transaction and log indices with a zero-valued `blockHash`.
Same-height heads change `hash` while preserving `parentHash`, modeling
successive ~200 ms Flashblocks within one L2 block.

Sources: [pendingLogs](https://docs.base.org/base-chain/api-reference/flashblocks-api/pendingLogs)
and the [Flashblocks API overview](https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview).
