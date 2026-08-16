# Testing

Read this before running, adding, modifying, or disabling any test.

## Running tests

Use `./bin/test` for EUnit-style test modules:

```bash
# Run all tests in a module
./bin/test ar_unconfirmed_chunk_tests

# Run a specific test
./bin/test ar_unconfirmed_chunk_tests:get_unconfirmed_chunk_from_disk_pool_test_

# Multiple modules and/or tests together
./bin/test ar_mining_io_tests ar_data_sync_root_tests:data_roots_syncs_from_peer_test_
```

### Sandboxed agent environments

`./bin/test` always starts the BEAM as a named distributed Erlang node, even for
local `fast` modules such as `ar_sync_sim_tests`. The launcher and EPMD must bind
loopback TCP sockets. In an agent execution sandbox that restricts socket
creation, request unsandboxed execution for `./bin/test` on the first attempt
instead of waiting for the restricted run to fail. The usual pre-test symptom is
an `inet_tcp` register/listen `eacces` error; it does not indicate a simulator or
test failure.

### Concurrent test runs

When more than one `./bin/test` run may execute on the same host, set a unique
`ARWEAVE_NAMESPACE` for each run. The launcher adds the namespace to the main
and peer Erlang node names so the runs do not collide:

```bash
ARWEAVE_NAMESPACE=review-a ./bin/test ar_mining_io_tests
ARWEAVE_NAMESPACE=review-b ./bin/test ar_node_tests
```

The namespace must be unique among concurrent runs and may contain letters,
digits, `.`, `_`, and `-`. Setting the same namespace in two runs does not
isolate them.

`ARWEAVE_NAMESPACE` isolates Erlang node names, not log paths. When concurrent
runs need separate artifacts, give each one a distinct `AR_TEST_LOG_DIR` too:

```bash
ARWEAVE_NAMESPACE=review-a AR_TEST_LOG_DIR=logs/eunit-review-a \
    ./bin/test ar_mining_io_tests
```

Use Common Test (`*_SUITE.erl`) through `./bin/ct`, not `./bin/test`. `./bin/test`
may report "There were no tests to run" for CT suites because it goes through the
EUnit path.

```bash
# Run one Common Test suite
./bin/ct --suite apps/arweave_config/test/arweave_config_full_load_SUITE.erl

# Run multiple suites: comma-separated, ONE --suite flag.
# (Repeating the --suite flag silently runs only the first suite.)
./bin/ct --suite apps/arweave_config/test/arweave_config_full_load_SUITE.erl,apps/arweave_config/test/arweave_config_format_SUITE.erl

# Or run every suite in a directory
./bin/ct --dir apps/arweave_config/test
```

The test profile uses smaller values for constants such as `?PARTITION_SIZE` and
`?REPLICA_2_9_ENTROPY_COUNT` — see `rebar.config`.

## CI test categories

CI discovers EUnit test modules by scanning `apps/*/{src,test}/*.erl` for files
containing a `*_test/0` or `*_test_/0` function head — there is no maintained
list of modules. See `scripts/list_test_modules.sh`.

Test behavior is driven by two module attributes placed directly below the
`-module(...)` declaration: `-test_category([...])` and `-test_peers([...])`.

```erlang
%%% @doc Pure utility module — safe to batch with siblings.
-module(arweave_util).
-test_category([fast]).
```

Categories are comma-separated; keep the attribute on a single line:

```erlang
-module(ar_merkle).
-test_category([fast, vdf]).
```

| Category | Effect |
|---|---|
| (none — the default) | Module runs in its own shard in the main CI matrix (the `slow` path). Gets a fresh BEAM; peers are booted only if the module declares `-test_peers`. |
| `fast` | Module runs in one of a small number of batched fast shards. Tests still get their own BEAM per module, but multiple modules share an artifact download. Use only when the module's tests don't share global state with siblings. |
| `vdf` | Module is part of the macOS VDF workflow's subset (see `x-test-vdf.yml`). Use for tests whose correctness matters to a VDF deployment. Orthogonal to `fast`/`slow`. |
| `canary` | Module is run only by `x-test-canary.yml` (the always-fails canary check). Excluded from the main matrices. |

## Declaring peers

Tests run on a `main` node that is always started. Additional peer nodes
(`peer1`–`peer4`) are booted only when a module asks for them via `-test_peers`:

```erlang
-module(ar_info_tests).
-test_peers([peer1, peer2]).
```

- Absent attribute ⇒ no peers. Most modules need none.
- A run boots the **union** of `-test_peers` across the modules it runs. CI runs
  one module per shard; `./bin/test mod1 mod2` unions both; `./bin/test mod:test`
  boots that module's whole declared set.
- Peers reached *indirectly* count too: `ar_test_node:start_coordinated/1` uses
  `peer1`–`peer4`, and the no-`Node` `sign_tx/1,2` / `sign_v1_tx/1,2` helpers talk
  to `peer1`. Declare those peers even though the atom never appears in the test.
- There is no runtime check that a test only touches declared peers — an
  undeclared peer surfaces as a `{badrpc, nodedown}` failure. Keep the annotation
  in sync with what the test actually uses.

## Adding a new test

Every magic number in a test — bounds, floors, margins, and expected values —
must carry a brief comment deriving it. For example, explain that four equal
peers split a 100 cps budget at roughly 25 each, so 10 is a wide floor under
that split. A reviewer should never have to reverse-engineer where a constant
came from.

Just write it. The discovery script picks up any new file with EUnit test exports
on the next CI run. New tests default to `slow` (their own shard) and to no
peers. If the tests are pure and would be safe to batch, add
`-test_category([fast])`; if they drive a peer cluster, declare the peers with
`-test_peers([...])`.

## Disabling a test

There's no `skip` annotation. To disable:

- **Single test:** comment out the function with a reason (preferred — keeps the
  reason colocated and is easy to re-enable).
  ```erlang
  %% Disabled — see issue #1234. Re-enable once <thing> is fixed.
  %% my_broken_test() ->
  %%     ?assertEqual(...).
  ```
- **Whole module:** comment out all tests, or rename so the function no longer
  matches `*_test/0` / `*_test_/0`. Prefer commenting out.

Renaming to `disabled_my_test()` (prefix) still matches the discovery pattern.
Use a suffix like `my_test_disabled()`, or just comment the function out.

## Test helpers: `ar_test_node` vs `ar_test_util`

Two helper modules; the difference governs whether your module can be tagged
`fast`:

| Module | What it is | When to use |
|---|---|---|
| `ar_test_node` | Distributed test infrastructure: spawns peer nodes, propagates mocks across peers via `remote_call`, runs queries against peer state. | Tests that need a peer cluster — declare the peers via `-test_peers`. |
| `ar_test_util` | Local-only helpers (e.g. `with_mocked/2,3`) that run inside a single BEAM. | Tests tagged `-test_category([fast])`. |

If a fast-tagged module needs to mock a function, use `ar_test_util:with_mocked/3`
— it's the batch-safe equivalent of
`ar_test_node:test_with_mocked_functions/2,3`. Using the `ar_test_node` version
from a fast module is a bug: it tries to broadcast the mock to peer nodes that
aren't booted.

```erlang
%% In a `-test_category([fast])' module:
state_transition_test_() ->
    ar_test_util:with_mocked([
        {ar_block, strict_data_split_threshold, fun() -> 700_000 end}
    ], fun test_state_transitions/0, 30).
```

If you find yourself reaching for an `ar_test_node:*` helper from a fast module,
that's a signal — either move the test back to slow, or add a local equivalent to
`ar_test_util` (only when the operation really doesn't need peers).

## Waiting on async conditions

Never hand-roll a poll loop or `timer:sleep` to wait for a condition. All wait
logic goes through `ar_test_await`:

- Prefer a **predefined named wait** (`ar_test_await:node_joined/1`,
  `chunk_recorded/3`, etc.) — they read as intent and centralize the timeout and
  poll cadence.
- For a one-off condition, use the generic
  `ar_test_await:until(Name, fun() -> Bool end)` (`/3` for a custom timeout).
  `Name` is an atom naming the condition; it appears in the
  `{error, {timeout, Name}}` result. Match `ok = ...` so a timeout fails the test.

  ```erlang
  ok = ar_test_await:until(dispatcher_drained, fun() ->
      {ok, S} = gen_server:call(Pid, get_state),
      gb_sets:is_empty(S#state.task_queue)
  end).
  ```

The moment the *same* generic `until/2,3` condition is needed in more than one
place, promote it to a predefined named helper in `ar_test_await` and call that
instead — don't copy the predicate.

`timer:sleep` is reserved for deliberately time-based behaviour (e.g. exercising
a retry backoff), never as a stand-in for a condition wait.

## Assertions and pattern matching

Use the EUnit assertion macros (`?assertEqual`, `?assertMatch`, …) via
`eunit.hrl`.

Never wrap assert macros in reused helper functions, and don't push a
badmatch-forcing pattern match down into a helper either. The assertion belongs
in the testcase, where a failure names the test that failed.

Do:
```erlang
testcase() ->
    ?assertMatch(pattern, helper_function_call()).

helper_function() ->
    function_producing_value().
```

instead of:
```erlang
testcase() ->
    helper_function().

helper_function() ->
    ?assertMatch(pattern, function_producing_value()).
```

Similarly, do:
```erlang
testcase() ->
    pattern = helper_function_call().

helper_function() ->
    function_producing_value().
```

instead of:
```erlang
testcase() ->
    helper_function().

helper_function() ->
    pattern = function_producing_value().
```

## Common Test suites

### Exports

Do not export Common Test functions in separate statements. Use:

```erlang
-compile([export_all, nowarn_export_all]).
```

This is the one place `export_all` is acceptable — see
[erlang-style.md](erlang-style.md) for the rule elsewhere.

### Comment policy

For `*_SUITE.erl` files, keep comments minimal:

- Add this banner immediately before the first testcase in each suite:
  ```erlang
  %%====================================================================
  %% Test cases
  %%====================================================================
  ```
- Keep exported testcase functions together directly under the `Test cases` banner.
- Put private helper functions after the testcases under a single final banner:
  ```erlang
  %%====================================================================
  %% Helpers
  %%====================================================================
  ```
- Do not add testcase-level `%% @doc` when the testcase name already explains
  intent. Add `%% @doc` only when the purpose is not obvious from the name.
- Prefer short inline `%%` comments on the specific non-obvious line, instead of
  a block comment above the testcase.
- Skip comments for straightforward 2-5 line happy-path tests.
- Keep comments that document surprising behavior, safety constraints, or
  setup/cleanup hazards.
- Do not add a module header comment for a Common Test module.
