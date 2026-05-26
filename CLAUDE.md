# Arweave Development

## Building

Use `./ar-rebar3` instead of `./rebar3` for building:
```bash
./ar-rebar3 test compile
```

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
