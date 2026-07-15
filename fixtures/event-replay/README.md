# Event replay fixtures

The fixtures are line-delimited JSON so the same stream can be consumed by a
Rust test, a TypeScript test, or a sidecar smoke tool without loading an
unbounded array. The Monad sample deliberately skips global ring sequence
values between filtered log notifications; that is valid and must not be
treated as a ring gap. The explicit `subscriptionGap` remains terminal.
