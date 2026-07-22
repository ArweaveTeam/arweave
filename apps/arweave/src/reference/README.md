# `src/reference/`

Legacy reference implementations kept **only** for us in tests and benchmarks.
These modules are not used in production any more.

## Rules for this directory

- **No production references.** If a live module ever needs something here, that
  code isn't reference code — promote it back to `src/`.
- **No inline tests.** CI's eunit discovery (`scripts/list_test_modules.sh`)
  scans `src/` and `test/` at depth 1 only, so a `*_test_/0` head placed here
  would silently never run. Correctness and performance checks live in `test/`
  and in the benchmark module and *reference* these.
- **These still ship in the release.** They compile into the app's `ebin/` like
  any other `src/` module — which is required, because the shipping
  `ar_bench_account_tree` loads `ar_patricia_tree_legacy` when the `legacy`
  benchmark is run on a node. "Reference" means non-production, not
  non-shipped.
- **Keep them frozen.** Don't refactor these to track changes in the live code;
  their value is being a fixed point to compare against.
