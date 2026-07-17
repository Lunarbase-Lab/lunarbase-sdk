# Quote checkpoint schema

Version 3 describes the single JSON DTO stored by `lunarbase-indexer`.
Embeddable clients do not depend on this persistence format.

The complete DTO is atomically replaced under one deployment-specific Redis
key and has no TTL. Monetary U256 values are decimal strings. Lane principal
and packed-field views retain their actual storage widths. Arrays are sorted
by address before serialization to keep diagnostics deterministic.

Versions 1 and 2 are intentionally ignored after the breaking `0.2.0` cleanup.
