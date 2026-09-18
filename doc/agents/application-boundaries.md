# Application boundaries

Use one cohesive API per public module. The number of public modules depends on
whether the application provides a service or a library.

## Service applications

Service apps such as `arweave_sync` and `arweave_entropy` expose their contract
through the app-named module and its public header, when needed. Other modules
and headers are internal. Keep process management, caches and implementation
details behind that interface.

### Test-only interfaces

Cross-app tests that need controlled access beyond the production API use
`internal_*` functions on the owning app's public module. The prefix marks access
outside the supported public API, not a test case. Guard both the exports and
implementations with `-ifdef(AR_TEST)`. Keep them narrow; do not expose internal
records or messages just to rename a call.
Non-trivial fixtures belong in the owning app's test directory, behind thin
facade wrappers. Tests within that app may call its internals directly.

## Library applications

A library app may expose several public modules, each covering a cohesive
concept. Call those modules directly; do not add facade wrappers solely to route
every call through the app-named module.

- Declare the public modules and headers explicitly in the app's README.
- Mark public library modules with a short module-header comment.
- Everything not declared public is internal, even if it exports functions.
- An app prefix indicates ownership, not visibility. Prefer it for new modules;
  preserve existing names unless a rename is part of the requested work.

`arweave_util` is a library app. Its public modules are listed in
[its README](../../apps/arweave_util/README.md), including the legacy-named
`ar_intervals` and `ar_ets_intervals`. This is an explicit exception to the
single-facade service convention, not permission to expose every utility helper.

Apply these boundaries to new and extracted apps. Do not use this convention as
a reason to sweepingly rename or reclassify unrelated legacy modules.
