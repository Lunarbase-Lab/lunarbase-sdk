# `@lunarbase-lab/pmm-v2-client`

Realtime TypeScript client for LunarBase quotes.

Status: **fully supported**.

## Install

```bash
npm install @lunarbase-lab/pmm-v2-math @lunarbase-lab/pmm-v2-client
```

## Use

```ts
import { connect } from "@lunarbase-lab/pmm-v2-client";

const client = await connect(config, dataSource, optionalCheckpoint);
const quote = client.quote(request);
const quotes = client.quoteMany(requests);
const health = client.health();
await client.shutdown(); // bounded to 10 seconds by default
```

## Guarantees

- `quote` and `quoteMany` read one coherent in-memory state snapshot.
- `ChainDataSource` covers bootstrap, backfill, ordered updates, and checkpoint validation.
- Gaps and canonical mismatches suspend readiness until recovery completes.
- `queueBound` and `queueByteBound` jointly cap the source/reducer handoff;
  overflow fails closed into canonical recovery.
- `sourceOperationTimeoutMilliseconds` bounds subscription, snapshot, and
  recovery source calls. Caller-owned deployment and filter objects are copied
  and frozen before the first asynchronous boundary.
- Runtime queues use fixed-capacity O(1) ring buffers. Shutdown stops ingestion,
  drains accepted reducer updates, and rejects if its deadline is exceeded.
