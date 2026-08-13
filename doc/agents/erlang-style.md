# Erlang style

The canonical style reference for this repo, for humans and agents alike.
`AGENTS.md` carries the handful of rules that apply to almost every edit; this
document is the complete set.

Existing code that predates a rule is left alone unless the diff you are already
writing touches it.

## Whitespace and layout

### Single-space OTP style

Use a single space around `->`, `=`, `?=`, and other binary operators. Do not pad
with extra spaces to column-align tokens across related clauses. Alignment is
harder to maintain — any clause growing past the column forces a re-pad of every
sibling — and it is noisier in diffs.

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

The same rule applies to function-clause groups, map and record fields, and `?=`
in `maybe` expressions.

### Indentation

Four spaces. Never tabs.

### Line length

Aim for a maximum of 80 characters. This is a real constraint, not an
aspiration — 90% of lines in `apps/*/src` are within it. Break long calls by
putting each argument on its own line:

```erlang
%% Bad
example() ->
    TotalTime = lists:foldl(fun(X, Acc) -> X + Acc end, 0, [12, 15, 8, 21, 35]),
    ...

%% Good
example() ->
    TotalTime = lists:foldl(
        fun(X, Acc) -> X + Acc end,
        0,
        [12, 15, 8, 21, 35]
    ),
    ...
```

### Tuples, lists, records

Space after each comma in a tuple; spaces around `|` in list deconstruction; no
space between the record name and the brace.

```erlang
%% Good
{one, two, three, A, B, C}
[Head | Tail]
State#state{first = "hello", second = "world"}

%% Bad
{ one,two,three, A, B, C}
[Head|Tail]
State#state { first = "hello", second = "world" }
```

## Naming

### Acronyms

In **variable names**, acronyms are always written in full caps. `PeerID` not
`PeerId`, `URL` not `Url`, `HTTPKeepalive` not `HttpKeepalive`, `JSON` not
`Json`, `IP` not `Ip`, `CLI` not `Cli`, `VDF` not `Vdf`, `CM` not `Cm`, `API`
not `Api`, `TCP` not `Tcp`, `SHA` not `Sha`.

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

Atom, function, and module names follow Erlang convention (`tcp_keepalive`,
`parse_url`) and are unaffected.

### Never use "overlay"

Never use the term "overlay" when naming anything — variables, functions, macros,
modules, records. Pick a word that names the concept directly.

### Private worker naming

When a public function delegates to a private worker that does most of the actual
work, prefix the worker with `do_`. Don't use `apply_` for this pattern.

```erlang
%% Good — public set/2 delegates to private do_set/2:
set(Option, Value) ->
    ...
    do_set(Option, Value).

do_set(Option, Value) ->
    ...
```

The pattern extends to pipeline stages: `do_set` → `do_set_runtime` →
`do_set_parameter`, not `apply_set_runtime`.

`apply_*` IS appropriate when the function name describes the action (e.g.
`apply_rule/4` applies a field rule, `apply_external_update/2` applies a peer
update to local state). The test: if you remove "apply" and the name still
describes the action, the function is probably `do_*`.

### Minimal descriptive function names

Function names should explain the high-level purpose while staying short enough
not to hinder readability. The module name carries some of the meaning.

```erlang
%% Bad
ar_tx:generate_data_segment_for_signing(TX).
arweave_util:pretty_print_internal_ip_representation(IPAddr).

%% Good
ar_tx:to_binary(TX).
ar_serialize:block_to_json_struct(Block).
```

### Atoms

Lowercase, words separated by underscores. `block_not_found`, not `'iAtom'` or
`'block not found'`.

### Variables

Name variables for the data they hold, so a block can be understood without a
comment explaining it.

```erlang
%% Bad
sign_data(X, Y) ->
    {A, B} = X,
    sign(A, Y).

%% Good
sign_data(Keypair, Data) ->
    {Priv, _Pub} = Keypair,
    sign(Priv, Data).
```

## Structure

### Avoid `if`

An Erlang `if` selects the first guard that succeeds; it does not test one
distinguished condition as `if` does in many languages, and `true ->` is often
used as its catch-all clause. Prefer `case` or function clauses so the value
being examined and the alternatives are explicit.

### Avoid deeply nested code

Deeply nested code masks alternative code paths and is hard to debug. Aim for a
single level of nesting in `case` and `receive`, two at most. Prefer extra
function clauses over nesting.

```erlang
%% Bad
contains_data_tx([]) -> false;
contains_data_tx(TXList) ->
    [TX | Rest] = TXList,
    case is_record(TX, tx) of
        true ->
            case byte_size(TX#tx.data) > 0 of
                true -> true;
                false -> contains_data_tx(Rest)
            end;
        false -> error_not_tx
    end.

%% Good
contains_data_tx([]) -> false;
contains_data_tx([TX | Rest]) when is_record(TX, tx) ->
    case byte_size(TX#tx.data) > 0 of
        true -> true;
        false -> contains_data_tx(Rest)
    end;
contains_data_tx(_) ->
    error_not_tx.
```

### `maybe` for sequential steps that can each fail

`maybe` is enabled repo-wide in `rebar.config`, so no per-module `-feature`
directive is needed. Use it in place of nested `case` **whenever it makes the
code simpler**. Where the two come out about equally complex, use `case`.

It pays off when a function runs several steps in order, each returning
`{ok, _}` or an error, and any error should abort the rest. Nesting a `case` per
step buries the happy path and repeats the error clause; `maybe` reads as a
straight list of steps.

```erlang
%% Good — the happy path is the whole body, errors propagate unchanged:
convert(Format, InputFilename, OutputFilename) ->
    maybe
        {ok, Encoder} ?= encoder(Format),
        {ok, Data} ?= read_input(InputFilename),
        {ok, Nested} ?= legacy_to_nested(Data),
        {ok, Encoded} ?= Encoder:encode(Nested),
        write_output(OutputFilename, Encoded)
    end.
```

Add an `else` only to translate the failure values. When the errors are already
what the caller should receive, leave it off — an `else` that just maps each
error back to itself is noise.

Reach for `case` when there is a single decision, when branches do real work
rather than pass a value along, or when the alternatives are genuinely
alternatives rather than a failure to bail out on. A `maybe` with one `?=`, or
one whose `else` reimplements the branching a `case` expressed directly, is the
harder read.

### Deconstruct arguments in the function header

Deconstruct as much as possible in the clause header rather than the body. This
makes the arguments explicit, and wrong input fails with a function_clause rather
than proceeding.

### Minimal exports

Export only what is externally required, and order the exports logically. The
export list is the module's interface — it tells other engineers what they need
to understand.

```erlang
%% Good
-export([sign/2, verify/3]).
-export([to_address/1]).
```

`-compile(export_all)` is not acceptable in `src/` modules. Common Test suites
are the one exception — see [testing.md](testing.md).

### No `-spec` attributes

This repo does not use `-spec`. Do not add one to a new or modified function.

The specs still in the tree are legacy and are being removed incrementally, so
treat one you come across as pending deletion rather than as a pattern to copy.
Name the function and its arguments well enough that the signature carries the
same information.

### Remove redundant code

When new work makes existing code redundant, delete it. Do not leave it behind as
code or as commented-out blocks; version control already has it.

## Comments

### Module headers

Each module gets a short comment describing the set of functions it contains,
prefixed with `%%%`.

```erlang
-module(ar_serialize).
-export([...]).

%%% Serialisation/deserialisation utilities for the HTTP server.
```

### Function comments

Function comments start with `%% @doc`, placed above the function header. Don't
use plain `%%` for documentation that describes what a function does.

```erlang
%% @doc Look up the canonical option_key for a legacy field name.
option_key_for(LegacyField) -> ...
```

Keep them to one tight sentence: what it does and, if not obvious, why. Skip
caller lists and implementation rationale. Comment exported functions first; a
function whose signature explains itself needs no comment.

### Comments inside function bodies

Only where the code is genuinely complex enough that tracing it would take real
time. Before adding one, check whether a better variable name or a small
refactor would remove the need.

```erlang
%% Bad
sign_verify_test(Keypair) ->
    % Deconstructs keypair into two separate terms, Pub and Priv.
    {Priv, Pub} = Keypair,
    % Generates an integer between 1 and 100 to sign and then verify.
    Data = floor(rand:uniform() * 100),
    SignedData = sign(Priv, Data),
    verify(Pub, SignedData).

%% Good
sign_verify_test({Priv, Pub}) ->
    Data = floor(rand:uniform() * 100),
    SignedData = sign(Priv, Data),
    verify(Pub, SignedData).
```

### Record field descriptions

Document each field inline with a single `%`, as concisely as possible.

```erlang
-record(tx, {
    id = <<>>,        % TX UID (hash of signature).
    last_tx = <<>>,   % Wallet's last TX hash.
    owner = <<>>,     % Public key of transaction owner.
    quantity = 0,     % Amount of Winston to send.
    signature = <<>>  % Transaction signature.
}).
```

## Logging

Use the `?LOG_*` macros for log entries — `?LOG_DEBUG`, `?LOG_INFO`,
`?LOG_WARNING`, `?LOG_ERROR`. They are the convention throughout the codebase.

```erlang
?LOG_WARNING([{event, could_not_retrieve_block}, {retry_in, ?REJOIN_TIMEOUT}]),
```

Use `ar:console/1,2` for output intended for the operator watching the console.
It is also written to the log file.

```erlang
ar:console("Started mining on block height ~B", [Height]),
```

Don't log huge terms. Truncate with `~P`:

```erlang
?LOG_WARNING("Invalid Block Hash List: ~P", [BI, 100]),
```

## Error handling

Do not choose a return shape solely because a function has side effects. Follow
the established API contract; common results include `ok`, `{ok, Value}`, and
`{error, Reason}`. Handle expected failures explicitly. Pattern match on a
success result only when failure should terminate the current operation.

Use `try/catch` at deliberate error boundaries where an unexpected exception
must be translated, logged, or isolated, such as the HTTP event loop. Do not use
it to hide ordinary return-value errors.

## Tests in the module

New tests belong in a dedicated `X_tests.erl` alongside `X.erl` — separate files
keep tests out of search results when you are looking for real call sites. Tests
do also live inside the module they cover in parts of this codebase; when they
do, separate them with a banner immediately before the first test function:

```erlang
%%%===================================================================
%%% Tests.
%%%===================================================================
```

If a module carries a weaker marker (`%% Tests.` or similar), replace it with the
banner. The banner makes the test/non-test boundary obvious in long modules.

Everything else about writing tests — CI categories, peer declarations, helper
modules, waiting on async conditions — is in [testing.md](testing.md).
