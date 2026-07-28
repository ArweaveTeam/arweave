%%% @doc CT coverage for `arweave_config_cli` — the entry point used
%%% by `./bin/arweave config get/set`. Validates that user-facing
%%% string keys (canonical-looking, dotted, legacy aliases) translate
%%% to the canonical option_key before the registry is touched, and
%%% that the return shape matches what `erl_call -a` will render to
%%% the operator's terminal.
-module(arweave_config_cli_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    ok = arweave_config:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop().

all() ->
    [
        get_canonical_single_segment,
        get_canonical_dotted,
        get_legacy_alias_not_translated,
        get_formats_binary_as_text,
        get_formats_list_of_binaries,
        get_unknown_atom_segment_returns_error,
        get_malformed_key_returns_error,
        set_canonical_dotted_coerces_value,
        set_json_array_sets_list_option,
        set_json_fallback_keeps_scalar_semantics,
        set_legacy_alias_not_translated,
        set_unknown_returns_error,
        set_bad_value_returns_error,
        set_load_only_in_runtime_returns_error
    ].

%%====================================================================
%% Test cases
%%====================================================================

get_canonical_single_segment(_Config) ->
    "false" = arweave_config_cli:get("debug"),
    ok = arweave_config:set([debug], true),
    "true" = arweave_config_cli:get("debug").

get_canonical_dotted(_Config) ->
    "false" = arweave_config_cli:get("mining.enabled"),
    ok = arweave_config:set([mining, enabled], true),
    "true" = arweave_config_cli:get("mining.enabled").

%% The CLI accepts canonical keys only — `mine` and `mining_addr` are
%% legacy aliases of `[mining, enabled]` / `[mining, address]`, not
%% canonical keys, so they don't resolve.
get_legacy_alias_not_translated(_Config) ->
    "undefined" = arweave_config_cli:get("mine"),
    "undefined" = arweave_config_cli:get("mining_addr").

%% Binaries render as their text — the whole reason get/1 formats:
%% `erl_call -a' shows raw binaries as opaque `#Bin<...>' dumps.
get_formats_binary_as_text(_Config) ->
    ok = arweave_config:set([data_dir], <<"/opt/data">>),
    "/opt/data" = arweave_config_cli:get("data_dir").

get_formats_list_of_binaries(_Config) ->
    ok = arweave_config:set(
        [transactions, blocklist, urls],
        [<<"http://a.example/x.txt">>, <<"http://b.example/y.txt">>]),
    "[http://a.example/x.txt, http://b.example/y.txt]" =
        arweave_config_cli:get("transactions.blocklist.urls").

%% The parser uses `binary_to_existing_atom` for each segment, so a
%% key containing an atom that no spec ever loads is rejected up front
%% rather than silently coerced into an unknown canonical key.
get_unknown_atom_segment_returns_error(_Config) ->
    %% The atoms below are unique enough that no other module in the
    %% test VM will have interned them — required because the shared-VM
    %% test runner keeps the atom table populated across suites and
    %% common atoms (`does`, `not`, `exist`) leak in from elsewhere.
    Err1 = arweave_config_cli:get("xyz_unknown.zzz_segment"),
    {match, _} = re:run(Err1, "error.*invalid_key"),
    Err2 = arweave_config_cli:get("xyz_totally_made_up_alias"),
    {match, _} = re:run(Err2, "error.*invalid_key").

get_malformed_key_returns_error(_Config) ->
    %% Trailing separator — the parser rejects.
    {match, _} = re:run(arweave_config_cli:get("mining."), "error"),
    %% Double separator.
    {match, _} = re:run(arweave_config_cli:get("mining..enabled"), "error").

%% JSON-shaped values decode to real terms so list-typed runtime
%% options are settable from the CLI (a bare string would coerce to a
%% singleton list and silently replace the whole value).
set_json_array_sets_list_option(_Config) ->
    ok = arweave_config_cli:set(
        "transactions.blocklist.urls",
        "[\"http://a.example/x.txt\", \"http://b.example/y.txt\"]"),
    [<<"http://a.example/x.txt">>, <<"http://b.example/y.txt">>] =
        arweave_config:get([transactions, blocklist, urls]).

%% Values that merely look like JSON but fail to decode (or that are
%% plain scalars) keep the historical raw-string path.
set_json_fallback_keeps_scalar_semantics(_Config) ->
    ok = arweave_config_cli:set("port", "1985"),
    1985 = arweave_config:get([port]),
    %% Broken JSON falls back to the raw string, which then fails the
    %% option's type coercion — not a JSON error.
    {error, _} = arweave_config_cli:set("transactions.blocklist.urls", "[oops"),
    [] = arweave_config:get([transactions, blocklist, urls]).

set_canonical_dotted_coerces_value(_Config) ->
    %% String value flows through the spec's type coercion.
    ok = arweave_config_cli:set("port", "1985"),
    1985 = arweave_config:get([port]).

set_legacy_alias_not_translated(_Config) ->
    {error, _} = arweave_config_cli:set("mine", "true"),
    %% Sanity-check the real canonical key wasn't somehow updated.
    false = arweave_config:get([mining, enabled]).

set_unknown_returns_error(_Config) ->
    %% See note in `get_unknown_atom_segment_returns_error/1` — pick a
    %% segment that no other module is likely to intern.
    {error, #{ reason := invalid_key }} =
        arweave_config_cli:set("xyz_unknown.zzz_segment", "1").

set_bad_value_returns_error(_Config) ->
    %% `port` is a pos_integer; a non-numeric string must be rejected.
    {error, _} = arweave_config_cli:set("port", "not_a_number"),
    %% The previous value (the spec default) is untouched.
    1984 = arweave_config:get([port]).

%% Once the lifecycle has flipped, sets against load-only specs are
%% refused — this exercises the same path the operator hits with a
%% live node.
set_load_only_in_runtime_returns_error(_Config) ->
    ok = arweave_config_options_registry:set_runtime(true),
    {error, #{ reason := parameter_not_runtime_writable }} =
        arweave_config_cli:set("port", "1985").
