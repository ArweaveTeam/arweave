# Arweave Development

## Workspace conventions

- **`scripts/`** is for scripts intended to be committed to the repo
  (CI helpers, release tooling, etc.). Treat anything you put there as
  part of the codebase — name it clearly, give it a header comment,
  make it work for everyone.
- **`tmp/`** is for throwaway scratch — one-off diagnostic scripts,
  experiments, ad-hoc data dumps. Put anything you don't intend to
  commit here. Files under `tmp/` are gitignored.
- Planning notes and investigation writeups should also go under
  `tmp/` unless the user explicitly asks to add them to the repo.
- When in doubt, start in `tmp/` and graduate to `scripts/` only once
  the script's role is permanent.

## Committing

Don't run `git commit` (or `git push`) unless the user has explicitly
asked you to. Leave the working tree dirty so the user can review the
diff and stage / message / split commits themselves. This also
applies after a multi-step task: finish, summarize what you changed,
and stop — wait for an explicit "commit" before invoking git.

## Building

Use `./ar-rebar3` instead of `./rebar3` for building:
```bash
./ar-rebar3 test compile
```

## Runtime Compatibility

This repo does not use hot code reloading. Do not add compatibility
branches solely for old in-memory terms or old function contracts
coexisting with new code in the same BEAM. Prefer a single current
contract unless the code must read persisted data or externally
provided input from an older release.

## Parsing integers from external input

Never call `binary_to_integer/1` directly on a binary that came from an
external source (a JSON body, a URL path segment, an HTTP header, a peeresponse). Use `ar_serialize:parse_integer/1` (or
`ar_serialize:parse_integer_or_infinity/1` where the `infinity`
atom is a valid input) instead — it rejects inputs longer than
`?MAX_INTEGER_DIGITS`=155 before building the bignum out of them.

This does not cover bare (unquoted) JSON numbers - `jiffy:decode` handles them.

## Running Tests

Use `./bin/test` for EUnit-style test modules:

```bash
# Run all tests in a module
./bin/test test_module

# Run a specific test
./bin/test test_module:test
```

For example:
```bash
./bin/test ar_unconfirmed_chunk_tests
./bin/test ar_unconfirmed_chunk_tests:get_unconfirmed_chunk_from_disk_pool_test_
```

Use Common Test (`*_SUITE.erl`) through `./bin/ct`, not `./bin/test`.
`./bin/test` may report "There were no tests to run" for CT suites because it
goes through the EUnit path.

```bash
# Run one Common Test suite
./bin/ct --suite apps/arweave_config/test/arweave_config_full_load_SUITE.erl

# Run multiple Common Test suites
./bin/ct --suite apps/arweave_config/test/arweave_config_full_load_SUITE.erl --suite apps/arweave_config/test/arweave_config_format_SUITE.erl
```

## CI test categories

CI discovers eunit test modules by scanning `apps/*/{src,test}/*.erl` for
files containing a `*_test/0` or `*_test_/0` function head — no
maintained list of modules. See `scripts/list_test_modules.sh`.

Test behavior is driven by two module attributes placed directly below
the `-module(...)` declaration: `-test_category([...])` (CI category)
and `-test_peers([...])` (peer nodes the tests need — see "Declaring
peers" below).

To opt a module into a non-default category, add `-test_category`:

```erlang
%%% @doc Pure utility module — safe to batch with siblings.
-module(ar_util).
-test_category([fast]).
```

Categories are comma-separated; keep the attribute on a single line:

```erlang
-module(ar_merkle).
-test_category([fast, vdf]).
```

### Categories

| Category | Effect |
|---|---|
| (none — the default) | Module runs in its own shard in the main CI matrix (the `slow` path). Gets a fresh BEAM; peers are booted only if the module declares `-test_peers` (see "Declaring peers"). |
| `fast` | Module runs in one of a small number of batched fast shards. Tests still get their own BEAM per module, but multiple modules share an artifact download. Use only when the module's tests don't share global state with siblings. |
| `vdf` | Module is part of the macOS VDF workflow's subset (see `x-test-vdf.yml`). Use for tests whose correctness matters to a VDF deployment. Orthogonal to `fast`/`slow`. |
| `canary` | Module is run only by `x-test-canary.yml` (the always-fails canary check). Excluded from the main matrices. |

### Declaring peers

Tests run on a `main` node that is always started. Additional peer
nodes (`peer1`–`peer4`) are booted only when a module asks for them via
`-test_peers`:

```erlang
-module(ar_info_tests).
-test_peers([peer1, peer2]).
```

- Absent attribute ⇒ no peers. Most modules need none.
- A run boots the **union** of `-test_peers` across the modules it runs.
  CI runs one module per shard; `./bin/test mod1 mod2` unions both;
  `./bin/test mod:test` boots that module's whole declared set.
- Peers reached *indirectly* count too:
  `ar_test_node:start_coordinated/1` uses `peer1`–`peer4`, and the
  no-`Node` `sign_tx/1,2` / `sign_v1_tx/1,2` helpers talk to `peer1`.
  Declare those peers even though the atom never appears in the test.
- There is no runtime check that a test only touches declared peers — an
  undeclared peer surfaces as a `{badrpc, nodedown}` failure. Keep the
  annotation in sync with what the test actually uses.

### Disabling a test

There's no `skip` annotation. To disable:

- **Single test:** comment out the function with a reason (preferred —
  keeps the reason colocated, easy to re-enable).
  ```erlang
  %% Disabled — see issue #1234. Re-enable once <thing> is fixed.
  %% my_broken_test() ->
  %%     ?assertEqual(...).
  ```
- **Whole module:** comment out all tests, or rename so the function
  no longer matches `*_test/0` / `*_test_/0`. Prefer commenting out.

Note: renaming to `disabled_my_test()` (prefix) still matches the
discovery pattern. Use a suffix like `my_test_disabled()`, or just
comment the function out.

### Adding a new test

Just write it. The discovery script picks up any new file with eunit
test exports on the next CI run. New tests default to `slow` (their own
shard) and to no peers. If the tests are pure and would be safe to
batch, add `-test_category([fast])`; if they drive a peer cluster,
declare the peers with `-test_peers([...])`.

### Test helpers: `ar_test_node` vs `ar_test_util`

Two helper modules; the difference governs whether your module can
be tagged `fast`:

| Module | What it is | When to use |
|---|---|---|
| `ar_test_node` | Distributed test infrastructure: spawns peer nodes, propagates mocks across peers via `remote_call`, runs queries against peer state, etc. | Tests that need a peer cluster — declare the peers via `-test_peers`. |
| `ar_test_util` | Local-only helpers (e.g. `with_mocked/2,3`) that run inside a single BEAM. | Tests tagged `-test_category([fast])`. |

If a fast-tagged module needs to mock a function, use
`ar_test_util:with_mocked/3` — it's the batch-safe equivalent of
`ar_test_node:test_with_mocked_functions/2,3`. Using the
`ar_test_node` version from a fast module is a bug: it tries to
broadcast the mock to peer nodes that aren't booted.

```erlang
%% In a `-test_category([fast])' module:
state_transition_test_() ->
    ar_test_util:with_mocked([
        {ar_block, strict_data_split_threshold, fun() -> 700_000 end}
    ], fun test_state_transitions/0, 30).
```

If you find yourself reaching for an `ar_test_node:*` helper from a
fast module, that's a signal — either move the test back to slow, or
add a local equivalent to `ar_test_util` (only when the operation
really doesn't need peers).

### Waiting on async conditions in tests

Never hand-roll a poll loop or `timer:sleep` to wait for a condition.
All wait logic goes through `ar_test_await`:

- Prefer a **predefined named wait** (`ar_test_await:node_joined/1`,
  `chunk_recorded/3`, etc.) — they read as intent and centralize the
  timeout/poll cadence.
- For a one-off condition, use the generic
  `ar_test_await:until(Name, fun() -> Bool end)` (`/3` for a custom
  timeout). `Name` is an atom that names the condition (it appears in
  the `{error, {timeout, Name}}` result). Match `ok = ...` so a
  timeout fails the test.

  ```erlang
  ok = ar_test_await:until(dispatcher_drained, fun() ->
      {ok, S} = gen_server:call(Pid, get_state),
      gb_sets:is_empty(S#state.task_queue)
  end).
  ```

The moment the *same* generic `until/2,3` condition is needed in more
than one place, promote it to a predefined named helper in
`ar_test_await` and call that instead — don't copy the predicate.

`timer:sleep` is reserved for deliberately time-based behaviour (e.g.
exercising a retry backoff), never as a stand-in for a condition wait.

## Erlang style

Follow OTP-style whitespace. Use a single space around `->`, `=`, `?=`, and other binary operators — do not pad with extra spaces to column-align tokens across related clauses. Alignment via multiple spaces is harder to maintain (any clause growing past the column forces a re-pad of every sibling) and noisier in diffs.

```erlang
%% Good — single-space:
case Result of
    ok -> {ok, Value};
    {ok, V} -> {ok, V};
    {ok, V, _} -> {ok, V};
    Else -> {error, Else}
end.

%% Bad — column-aligned with multi-space padding:
case Result of
    ok          -> {ok, Value};
    {ok, V}     -> {ok, V};
    {ok, V, _}  -> {ok, V};
    Else        -> {error, Else}
end.
```

Same rule for function-clause groups, map/record fields, and `?=` in `maybe` expressions.

### Acronyms

In **variable names**, acronyms are always written in full caps. `PeerID` not `PeerId`, `URL` not `Url`, `HTTPKeepalive` not `HttpKeepalive`, `JSON` not `Json`, `IP` not `Ip`, `CLI` not `Cli`, `VDF` not `Vdf`, `CM` not `Cm`, `API` not `Api`, `TCP` not `Tcp`, `SHA` not `Sha`, etc.

```erlang
%% Good:
PeerID = ...
{ok, JSON} = jiffy:encode(Map),
TCPKeepalive = arweave_config:get([network, client, tcp, keepalive]),

%% Bad — Pascal-cased acronyms in variables:
PeerId = ...
{ok, Json} = jiffy:encode(Map),
TcpKeepalive = arweave_config:get([network, client, tcp, keepalive]),
```

Atom/function/module names follow Erlang convention (`tcp_keepalive`, `parse_url`) and are unaffected. This rule applies to new variables; existing code following the older convention is left alone unless the surrounding diff already touches it.

### Naming

Never use the term "overlay" when naming things (variables, functions, macros, modules, records). Pick a word that names the concept directly.

### Function comments

Function comments (where needed) start with `%% @doc`. Don't use plain `%%` for documentation that describes what a function does.

```erlang
%% @doc Look up the canonical option_key for a legacy field name.
option_key_for(LegacyField) -> ...
```

### Common Test comment policy

For `*_SUITE.erl` files, keep comments minimal:

- Add this banner immediately before the first testcase in each suite:
  ```erlang
  %%====================================================================
  %% Test cases
  %%====================================================================
  ```
- Keep exported testcase functions together directly under the `Test cases` banner.
- Put private helper functions after the testcases under a single final helper banner:
  ```erlang
  %%====================================================================
  %% Helpers
  %%====================================================================
  ```
- Do not add testcase-level `%% @doc` when the testcase name already explains intent.
- Add `%% @doc` only when the testcase purpose is not obvious from the name.
- Prefer short inline `%%` comments on the specific non-obvious line, instead of a block comment above the testcase.
- Skip comments for straightforward 2-5 line happy-path tests.
- Keep comments that document surprising behavior, safety constraints, or setup/cleanup hazards.
- Do not add module header comment for the common test module.

### Common test exports

Do not export Common Test functions in separate statement rather use `export_all` the following way: `-compile([export_all, nowarn_export_all]).`

### Private worker naming

When a public function delegates to a private worker that does most of the actual work, prefix the worker with `do_`. Don't use `apply_` for this pattern — reserve `apply_*` for cases where "apply" carries domain meaning (applying a rule, applying a state transition, etc.).

```erlang
%% Good — public set/2 delegates to private do_set/2:
set(Option, Value) ->
    ...
    do_set(Option, Value).

do_set(Option, Value) ->
    ...
```

The same pattern extends to pipeline stages: `do_set` → `do_set_runtime` → `do_set_parameter` etc., not `apply_set_runtime` etc.

`apply_*` IS appropriate when the function name describes the action (e.g. `apply_rule/4` applies a field rule, `apply_external_update/2` applies a peer update to local state). The test: if you remove "apply" and the name still describes the action, the function is probably `do_*`.

### In-module test section

When EUnit tests live in the same module as the code they cover, separate them from the rest of the module with a banner immediately before the first test function:

```erlang
%%%===================================================================
%%% Tests.
%%%===================================================================
```

If the module already has a weaker test marker (`%% Tests.` or similar), replace it with the banner. The banner makes the test/non-test boundary visually obvious in long modules.

### -spec placement

No blank line between a `-spec` and its function declaration.

```erlang
%% Good:
-spec option_key_for(LegacyField) -> Return when
    LegacyField :: atom(),
    Return :: {ok, list()} | {error, not_found}.
option_key_for(LegacyField) when is_atom(LegacyField) ->
    ...

%% Bad — blank line between spec and head:
-spec option_key_for(LegacyField) -> Return when
    LegacyField :: atom(),
    Return :: {ok, list()} | {error, not_found}.

option_key_for(LegacyField) when is_atom(LegacyField) ->
    ...
```

## PR reviews

After completing a PR review, ask the user whether to write unit tests covering the issues you flagged. Keep each test as small and simple as possible — one test per issue, just enough to make the problem reproducible. The set of tests doubles as a concise description of what's wrong and as a checklist the PR author can work through.
