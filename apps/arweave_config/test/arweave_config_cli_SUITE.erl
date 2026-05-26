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
        get_unknown_atom_segment_returns_error,
        get_malformed_key_returns_error,
        set_canonical_dotted_coerces_value,
        set_legacy_alias_not_translated,
        set_unknown_returns_error,
        set_bad_value_returns_error,
        set_load_only_in_runtime_returns_error
    ].

%%====================================================================
%% Test cases
%%====================================================================

get_canonical_single_segment(_Config) ->
    false = arweave_config_cli:get("debug"),
    {ok, true} = arweave_config:set([debug], true),
    true = arweave_config_cli:get("debug").

get_canonical_dotted(_Config) ->
    false = arweave_config_cli:get("mining.enabled"),
    {ok, true} = arweave_config:set([mining, enabled], true),
    true = arweave_config_cli:get("mining.enabled").

%% The CLI accepts canonical keys only — `mine` and `mining_addr` are
%% legacy aliases of `[mining, enabled]` / `[mining, address]`, not
%% canonical keys, so they don't resolve.
get_legacy_alias_not_translated(_Config) ->
    undefined = arweave_config_cli:get("mine"),
    undefined = arweave_config_cli:get("mining_addr").

%% The parser uses `binary_to_existing_atom` for each segment, so a
%% key containing an atom that no spec ever loads is rejected up front
%% rather than silently coerced into an unknown canonical key.
get_unknown_atom_segment_returns_error(_Config) ->
    %% The atoms below are unique enough that no other module in the
    %% test VM will have interned them — required because the shared-VM
    %% test runner keeps the atom table populated across suites and
    %% common atoms (`does`, `not`, `exist`) leak in from elsewhere.
    {error, #{ reason := invalid_key }} =
        arweave_config_cli:get("xyz_unknown.zzz_segment"),
    {error, #{ reason := invalid_key }} =
        arweave_config_cli:get("xyz_totally_made_up_alias").

get_malformed_key_returns_error(_Config) ->
    %% Trailing separator — the parser rejects.
    {error, _} = arweave_config_cli:get("mining."),
    %% Double separator.
    {error, _} = arweave_config_cli:get("mining..enabled").

set_canonical_dotted_coerces_value(_Config) ->
    %% String value flows through the spec's type coercion.
    {ok, 1985} = arweave_config_cli:set("port", "1985"),
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
