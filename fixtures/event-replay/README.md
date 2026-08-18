# Event replay fixtures

Fixtures are line-delimited JSON provider and parser messages consumed by
source tests. The Monad sample skips global sequence values between filtered
log notifications; this is valid. An explicit `subscriptionGap` remains
terminal.

The source integration suites combine these fixtures with local socket tests:
Monad covers competing proposals, abandonment, resume, and explicit parser
gaps; EVM covers applied/removed logs, competing block heads, reorg ordering,
and finalized paged delivery. Redis crash/replay belongs to the real-process
`lunarbase-e2e` suite because durability cannot be established by a JSON fixture.
