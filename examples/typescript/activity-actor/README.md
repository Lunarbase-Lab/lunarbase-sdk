# LunarBase BSC Testnet activity actor

This private workspace example creates low-volume swap activity for one
LunarBase BSC Testnet Core. It is deliberately locked to chain ID 97 and uses
the deployed addresses below:

| Role   | Address                                      |
| ------ | -------------------------------------------- |
| Core   | `0x11116c60551889C6c01DDAD3A1fB3Cc95CbeBBbB` |
| CASH   | `0x2c10647a0D96cab7fE26044CA6d3F854280dC906` |
| ASSET1 | `0x21f52a1d45DAb30b518b31CA8e44f91B588A8DEC` |
| ASSET2 | `0xcCE41dEACC72cd4C7b92358bf824eCA1f33Ec269` |

The actor is a trader/router. It is not one of the three immutable Core price
operators and cannot update prices or unpause lanes.

## Safety model

- A new key is generated locally with viem's CSPRNG and written only to
  `.env` with mode `0600`.
- The private key is never printed. Only the derived address is shown.
- Transactions require both `BROADCAST=true` and the `--live` argument.
  The gate is checked again inside every write method.
- Every write follows `simulateContract -> send -> confirmed receipt`.
  If a sent hash times out, all writes stop; restart also refuses to run while
  the actor has a pending nonce.
- Transactions are sequential; `.actor.lock` prevents two local live
  processes from sharing the nonce stream.
- Every inspection and write pins the expected ERC-1967 implementation,
  implementation runtime hash, and proxy runtime hash.
- Exact approvals are used instead of unlimited ERC-20 allowances.
- A confirmed opening swap creates one mandatory reverse leg. Until that
  reverse confirms, the actor never falls back to another route.
- The reverse input starts from the actual `SwapExecuted.amountOut`, not the
  preflight quote. It may be below `MIN_SWAP_AMOUNT`, is never auto-minted, and
  is halved until both reserve budgets accept it.
- On startup the actor replays its confirmed `SwapExecuted` history from
  `PAIRING_START_BLOCK`. A mismatched pair, an unconfirmed actor swap, or a
  changed history anchor stops the run.
- The Core ABI includes CoreUUPS and linked-library custom errors, while the
  token ABI includes IERC-6093 errors, so viem decodes simulation reverts by
  name (including `Core__SwapUnavailable()`).
- A lane needs configurable block headroom beyond the current head, avoiding
  transactions that are quoted at the price TTL boundary.
- The service stops below the configured tBNB reserve or above the configured
  gas-price cap and opens a circuit breaker after repeated deterministic
  failures.
- Each swap is capped to a fraction of the current output reserve. Cumulative
  confirmed output is also capped per token against the first positive reserve
  observed during a process run.
- Runs are bounded to 50 successful swaps by default. Restarting creates a new
  session budget, so do not automate restarts without an external daily limit.
- `quoteExactIn` is called from the actor address because Core fee behavior is
  router-dependent.
- All three routes are ERC-20 routes, so swap `msg.value` is always zero.
  tBNB is used only for gas.

Never fund this key on mainnet and never reuse it outside BSC Testnet.

## Setup

Install the locked workspace dependencies, build the actor, and generate its
wallet:

```sh
make install
make activity-actor-wallet
```

The second command refuses to overwrite an existing `.env`. It prints the
actor address while keeping the private key in
`examples/typescript/activity-actor/.env`.

Fund the printed actor with approximately `0.05-0.1 tBNB`, not the entire
funder balance. The current defaults stop when the actor falls below
`0.01 tBNB`.

Read the deployment without sending transactions:

```sh
make activity-actor-inspect
```

Run one dry activity decision:

```sh
corepack pnpm@9.15.0 --filter @lunarbase/example-activity-actor build
corepack pnpm@9.15.0 --filter @lunarbase/example-activity-actor once
```

After the pool is active, set `BROADCAST=true` in the local `.env` and run:

```sh
make activity-actor
```

Both the environment flag and the Makefile's `--live` argument are required.
Use `SIGINT` or `SIGTERM` for graceful shutdown. If a process is killed
without cleanup, first verify that no actor process or pending actor
transaction remains, then remove the local `.actor.lock`.

If the process reports `transaction_outcome_unknown`, reconcile the logged
hash when one is available and always compare the actor's latest and pending
nonce before any restart. Do not treat a submission or receipt timeout as a
failed transaction.

## Current deployment readiness

The ASSET1 route has active principal/reserve and has already executed
testnet swaps. Price pushes must continue: a lane is rejected whenever its
remaining block TTL is below `MIN_LANE_HEADROOM_BLOCKS`. ASSET2 is selected
only after it has positive principal/reserve and a non-zero guarded quote.

Minting mock tokens to the actor does not create pool principal. All three
deployed tokens expose permissionless `mint(address)`, but minting is allowed
only for a newly selected opening input. A mandatory reverse leg uses only the
balance received by the preceding swap.

## Configuration

| Variable                            | Default                | Meaning                                              |
| ----------------------------------- | ---------------------- | ---------------------------------------------------- |
| `RPC_URL`                           | Public BSC Testnet RPC | HTTP JSON-RPC endpoint                               |
| `PAIRING_START_BLOCK`               | `123101134`            | First block governed by strict paired-history replay |
| `BROADCAST`                         | `false`                | First live-write gate                                |
| `AUTO_MINT`                         | `true`                 | Mint to actor only when selected input is short      |
| `MIN_SWAP_AMOUNT`                   | `0.001`                | Minimum input in token units                         |
| `MAX_SWAP_AMOUNT`                   | `0.01`                 | Maximum input in token units                         |
| `SLIPPAGE_PPM`                      | `5000`                 | 0.5% minimum-output tolerance                        |
| `MAX_OUTPUT_RESERVE_PPM`            | `1000`                 | At most 0.1% of current output reserve per swap      |
| `MAX_SESSION_OUTPUT_RESERVE_PPM`    | `10000`                | At most 1% of baseline output reserve per session    |
| `MIN_LANE_HEADROOM_BLOCKS`          | `2`                    | Required price-TTL blocks beyond the inspected head  |
| `MIN_DELAY_SECONDS`                 | `20`                   | Minimum post-iteration delay                         |
| `MAX_DELAY_SECONDS`                 | `90`                   | Maximum post-iteration delay                         |
| `RETRY_DELAY_SECONDS`               | `30`                   | Delay after a deterministic pre-broadcast failure    |
| `DEADLINE_SECONDS`                  | `180`                  | Swap deadline after latest block timestamp           |
| `MIN_GAS_BALANCE_TBNB`              | `0.01`                 | Write floor for the actor gas balance                |
| `MAX_GAS_PRICE_GWEI`                | `1`                    | Maximum accepted legacy gas price                    |
| `MAX_SWAPS`                         | `50`                   | Required finite successful-swap limit                |
| `CONFIRMATIONS`                     | `2`                    | Required receipt confirmations                       |
| `EXPECTED_IMPLEMENTATION`           | Pinned deployment      | Expected ERC-1967 implementation                     |
| `EXPECTED_IMPLEMENTATION_CODE_HASH` | Pinned deployment      | Expected implementation runtime keccak256            |
| `EXPECTED_PROXY_CODE_HASH`          | Pinned deployment      | Expected proxy runtime keccak256                     |

The actor uses exact-input swaps only. Its local ABI pins the current Core
tuple/event surface and merges every custom error reachable through CoreUUPS,
linked swap libraries, and MockToken calls. A legitimate Core upgrade must be
reviewed; the ABI, implementation address, runtime hashes, and pairing epoch
must be updated deliberately before writes resume.
