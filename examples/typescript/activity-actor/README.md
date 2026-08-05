# LunarBase BSC Testnet activity actor

A bounded example trader for a configured LunarBase Core deployment on BSC Testnet
(chain ID 97). The actor is read-only by default and stops when deployment,
runtime-code, balance, price, or safety checks fail.

Use a dedicated testnet key. Never fund or reuse it on mainnet.

## Setup

Install dependencies and generate a local wallet:

```sh
make install
make activity-actor-wallet
```

The wallet command creates .env with mode 0600, prints only the actor address,
and refuses to overwrite an existing file. Replace every deployment placeholder
in .env with values verified for the intended BSC Testnet deployment. See
[.env.example](./.env.example) for all settings.

Inspect the configured deployment without sending transactions:

```sh
make activity-actor-inspect
```

Run one dry decision:

```sh
corepack pnpm@9.15.0 --filter @lunarbase-lab/example-activity-actor build
corepack pnpm@9.15.0 --filter @lunarbase-lab/example-activity-actor once
```

To enable writes, set BROADCAST=true and run:

```sh
make activity-actor
```

Both BROADCAST=true and the --live command argument are required.

## Safety guarantees

- The process is locked to BSC Testnet and validates the configured Core
  implementation and runtime-code hashes before each write.
- Transactions are sequential and protected by a local process lock.
- Every write is simulated, submitted, and confirmed before the next write.
- Unknown transaction outcomes stop the process for manual nonce and receipt
  reconciliation.
- Token approvals are finite and swaps are capped by per-swap and per-session
  reserve limits.
- Each opening swap requires a confirmed reverse leg; restart recovery uses
  finalized on-chain events and a bounded replay window.
- Gas balance, gas price, lane lifetime, retry count, and total successful swaps
  are bounded. The default run limit is 50 swaps.
- SIGINT and SIGTERM request graceful shutdown.

## Required deployment settings

The following values have no operational defaults and must be supplied:

- CORE_ADDRESS, CASH_ADDRESS, ASSET1_ADDRESS, and ASSET2_ADDRESS
- PAIRING_START_BLOCK
- EXPECTED_IMPLEMENTATION
- EXPECTED_IMPLEMENTATION_CODE_HASH and EXPECTED_PROXY_CODE_HASH

Keep BROADCAST=false until make activity-actor-inspect succeeds. If a
transaction result is unknown, do not restart until the submitted hash and both
the latest and pending account nonces have been reconciled.
