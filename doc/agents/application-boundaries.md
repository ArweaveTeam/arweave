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

## External dependencies

An Arweave application calls two kinds of code directly: its own modules and
`arweave_lib` (see below), plus OTP and the third-party libraries it wraps.
Every module of another Arweave application, including the node (`arweave`)
and sibling apps such as `arweave_storage`, `arweave_config` and
`arweave_metrics`, is reached through the app's deps module, `<app>_deps`. The
deps module names those dependencies in one place and lets tests and
simulators swap them out.

- `<app>_deps` exports one zero-arity function per module, returning the
  module: `kv() -> ar_kv.` Name each function after the service it provides,
  and reuse the name other apps use for the same module (`config`, `metrics`,
  `storage`, `clock`).
- Call those modules through the `?DEP(Name)` macro, defined in the app's
  internal header `include/<app>_deps.hrl`: `?DEP(kv):get(Database, Key)`.
  Keep the macro out of the app's public header, since other apps include it
  and each app's `?DEP` points at a different deps module.
- The boundary is per module: code may call any function of a selected module.
- Keep logic out of the deps module. Code that combines calls belongs in the
  app's own modules.
- When tests replace whole modules, as `arweave_sync`'s simulator does,
  `<app>_deps` dispatches each function to an implementation module: the
  production selectors in `<app>_deps_mainnet`, or, in test builds, a module
  installed with `override_module/1`.
- The node, `arweave_tools` and `arweave_sim` wire the deps together and call
  other applications directly.

`arweave_lib_boundaries_SUITE` checks every other application against this
rule.

## The library application

`arweave_lib` holds the protocol constants and the pure helpers every
application shares. Its functions depend only on their arguments and build
constants: no processes, no state of its own, no IO, no clock. That is why
callers need no deps module for it. Helpers with side effects belong to the
node (`ar_util`) or to the service that owns the side effect.

- Name every module `arweave_lib_<concept>`, so that a call site shows it
  needs no deps module.
- Declare the public modules and headers in
  [its README](../../apps/arweave_lib/README.md), and mark each public module
  with a short module-header comment. Everything else is internal, even if it
  exports functions.
- `arweave_lib_SUITE` enforces purity: the app depends only on OTP, `jiffy` and
  `b64fast`, includes no header from another Arweave application, and calls no
  file, OS, network, timer, process-registry, logging or `gen_*` function.

Apply these boundaries to new and extracted apps. Do not use this convention as
a reason to sweepingly rename or reclassify unrelated legacy modules.
