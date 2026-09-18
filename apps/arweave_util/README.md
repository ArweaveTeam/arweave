# arweave_util

`arweave_util` is a library application, not a service with a single facade.
It starts no application process. Callers use the public module for the concept
they need directly.

## Public modules

| Module | Purpose |
|---|---|
| `arweave_util` | General-purpose collection, encoding, formatting and other utility functions. |
| `ar_intervals` | Immutable sets of non-overlapping intervals and set operations. |
| `ar_ets_intervals` | Interval sets in caller-owned ETS tables for shared access. |

There are no public headers. Other modules and headers are internal unless
explicitly added to this list. An exported function alone does not make its
module public across application boundaries.

The existing interval module names remain unchanged. New modules should use the
`arweave_util_` prefix to indicate ownership, but that prefix does not imply a
public API. Each public module should represent a cohesive concept; unrelated
helpers do not automatically become public.

See [application boundaries](../../doc/agents/application-boundaries.md) for the
shared service/library convention.
