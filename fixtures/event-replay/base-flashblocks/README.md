# Base Flashblocks WebSocket fixtures

`ws-frames.jsonl` follows the official Flashblocks application-facing RPC
surface: filtered `pendingLogs` plus standard `newHeads`. Same-height head
frames intentionally change `hash` while preserving `parentHash`; this models
successive ~200 ms Flashblocks within one L2 block.

Source: <https://docs.base.org/base-chain/api-reference/flashblocks-api/pendingLogs>
and the Flashblocks API overview. The runtime never consumes the unstable raw
infrastructure `newFlashblocks` payload.
