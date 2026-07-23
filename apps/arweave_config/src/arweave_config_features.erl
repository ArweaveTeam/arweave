%%% @doc Feature flags.
%%%
%%% Each feature flag is a boolean toggle declared once in
%%% `catalog/0`. The catalog is the source of truth for:
%%%
%%%   - the set of known flag names (`names/0`);
%%%   - per-flag defaults (`default/1`);
%%%   - per-flag descriptions surfaced by spec metadata.
%%%
%%% A flag's value lives at `[features, <name>]` in the spec store.
%%%
%%% Removing a flag without replacement: drop it from `catalog/0` and
%%% add an entry to `tombstones/0` with a one-line message describing
%%% the removal. Operators with the flag still set in `config.json`
%%% receive a precise warning at startup; the node continues to start.
%%% Tombstone entries can be deleted after at least two minor releases.
-module(arweave_config_features).
-export([
    catalog/0,
    classify_legacy_flag/2,
    enabled/1,
    names/0,
    tombstone_message/1,
    validate/0
]).
-include("arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").

%% @doc Whether the named feature flag is enabled. Reads
%% `[features, Flag]` from the options registry; the per-flag spec
%% contributor surfaces the catalog default when nothing has been
%% written. Flags not in the catalog return `false`.
-spec enabled(atom()) -> boolean().
enabled(Flag) ->
    case lists:member(Flag, names()) of
        true ->
            case arweave_config:get([features, Flag]) of
                B when is_boolean(B) -> B;
                _ -> default(Flag)
            end;
        false ->
            false
    end.

%% @doc Catalog of declared feature flags. Each entry is
%% `#{ name => atom(), default => boolean(), description => binary() }`.
%% Adding a flag: append one entry. Removing a flag: drop the entry
%% AND add a `tombstones/0` entry so operators get a precise migration
%% message.
-spec catalog() -> [#{name := atom(),
                      default := boolean(),
                      runtime => boolean(),
                      description := binary()}].
catalog() ->
    [
        #{ name => disk_logging,
           default => false,
           runtime => true,
           description =>
               <<"Log block writes to disk in the storage layer. "
                 "Verbose; debug only.">> },
        #{ name => double_check_nonce_limiter,
           default => false,
           runtime => true,
           description =>
               <<"Re-verify each nonce-limiter step value before "
                 "accepting it. Debug aid; substantial overhead.">> },
        #{ name => extended_block_validation_trace,
           default => false,
           runtime => true,
           description =>
               <<"Dump a detailed trace when block pre-validation "
                 "fails. Debug aid.">> },
        #{ name => http_logging,
           default => false,
           runtime => true,
           description =>
               <<"Log HTTP request and response bodies. Verbose; "
                 "debug only.">> },
        #{ name => miner_logging,
           default => true,
           runtime => true,
           description =>
               <<"Emit mining-event logs from the watchdog process.">> },
        #{ name => pack_served_chunks,
           default => false,
           runtime => true,
           description =>
               <<"Repack chunks before serving them via the data-sync "
                 "HTTP API.">> },
        #{ name => remove_orphaned_storage_module_data,
           default => false,
           description =>
               <<"At startup, trim storage-module data above the weave "
                 "offset.">> },
        #{ name => serve_html_data,
           default => true,
           runtime => true,
           description =>
               <<"Allow the HTTP API to serve transaction data as "
                 "HTML.">> },
        #{ name => serve_tx_data_without_limits,
           default => false,
           runtime => true,
           description =>
               <<"Lift the per-request transaction-data size cap when "
                 "serving via the HTTP API.">> },
        #{ name => serve_wallet_lists,
           default => false,
           runtime => true,
           description =>
               <<"Serve wallet lists via the HTTP API for blocks "
                 "after the 2.2 fork.">> },
        #{ name => subfield_queries,
           default => false,
           runtime => true,
           description =>
               <<"Enable `tx/<id>/<field>' subfield-query "
                 "endpoints.">> },
        #{ name => time_syncing,
           default => true,
           description =>
               <<"Validate clock skew against trusted peers at "
                 "startup.">> }
    ].

%% @doc Sorted list of every declared flag name.
-spec names() -> [atom()].
names() ->
    lists:sort([Name || #{ name := Name } <- catalog()]).

%% Compile-time default for a given flag name. Crashes on unknown
%% flags by design — callers must guard with `lists:member/2` over
%% `names/0` first if they cannot guarantee the flag is declared.
default(Name) ->
    [Default] = [D || #{ name := N, default := D } <- catalog(), N =:= Name],
    Default.

%% Tombstones for flags removed from the catalog. Each entry is
%% `{Flag, MigrationMessage}`. The legacy bridge consults this list
%% before falling back to the generic "unknown flag" warning so
%% operators get a precise message.
%%
%% Lifecycle: when removing a flag from `catalog/0`, add a tombstone
%% entry here at the same time. Drop the tombstone entry after at
%% least two minor releases — by then operators will have seen the
%% precise warning across multiple upgrades and any remaining
%% appearance is treated as a generic typo.
-spec tombstones() -> [{atom(), binary()}].
tombstones() ->
    [
        %% Format: {flag_name, <<"Reason / migration message.">>}.
        {tx_poller,
            <<"Promoted to a proper option: "
              "use `gossip.tx.polling_enabled = false' "
              "(or the legacy `disable: [\"tx_poller\"]' which "
              "is recognized one more release before being "
              "removed in Arweave 2.9.7 or later).">>}
    ].

%% @doc Lookup a tombstone message for a flag. Returns
%% `{ok, Message}` if the flag is tombstoned, otherwise `not_found`.
-spec tombstone_message(atom()) -> {ok, binary()} | not_found.
tombstone_message(Flag) ->
    case lists:keyfind(Flag, 1, tombstones()) of
        {Flag, Msg} -> {ok, Msg};
        false -> not_found
    end.

%% @doc Reject entries under `[features, ...]` whose name isn't
%% declared in `names/0`.
-spec validate() -> ok | {error, binary()}.
validate() ->
    Items = arweave_config:get_all_with_prefix([features]),
    validate_each(Items, names()).

validate_each([], _Known) ->
    ok;
validate_each([{[features, Name], _Value} | Rest], Known)
        when is_atom(Name) ->
    case lists:member(Name, Known) of
        true ->
            validate_each(Rest, Known);
        false ->
            {error, format_error(<<"unknown feature flag">>,
                atom_to_binary(Name))}
    end;
validate_each([{Key, _Value} | _Rest], _Known) ->
    {error, format_error(<<"unexpected key">>,
        list_to_binary(io_lib:format("~p", [Key])))}.

format_error(What, Detail) ->
    <<"features: ", What/binary, " (", Detail/binary, ")">>.

%%%===================================================================
%%% Legacy enable / disable list classifier.
%%%===================================================================

%% @doc Classify one entry from a legacy `enable' / `disable' list and
%% apply it to the options registry. Called once per entry directly from the
%% legacy CLI / JSON parsers — no intermediate staging.
%%
%% Resolution order (first match wins):
%%
%%   1. Promotion table — flags that have been promoted to a dedicated
%%      spec option (e.g. `randomx_jit` → `[randomx, jit]`). Writes the
%%      promoted field and prints a deprecation hint pointing at the
%%      new key.
%%   2. Feature catalog — known feature flags get written as
%%      `[features, Flag] = true | false`, with a deprecation hint
%%      asking the operator to use `features.X = true|false`.
%%   3. Tombstones — removed flags get the per-flag migration message.
%%   4. Unknown — generic "unknown flag" warning; entry is dropped.
%%
%% REMOVE IN ARWEAVE 2.9.7 OR LATER along with the legacy
%% `enable' / `disable' list syntax.
-spec classify_legacy_flag(atom(), enable | disable) -> ok.
classify_legacy_flag(Flag, ListName) when is_atom(Flag),
        (ListName =:= enable orelse ListName =:= disable) ->
    case apply_promotion(Flag, ListName) of
        applied -> ok;
        not_promoted ->
            case lists:member(Flag, names()) of
                true ->
                    Value = list_value(ListName),
                    warn_legacy_list_format(Flag, ListName, Value),
                    _ = arweave_config:set([features, Flag], Value),
                    ok;
                false ->
                    case tombstone_message(Flag) of
                        {ok, Message} ->
                            warn_tombstoned_flag(Flag, ListName, Message);
                        not_found ->
                            warn_unknown_legacy_flag(Flag, ListName)
                    end
            end
    end.

list_value(enable) -> true;
list_value(disable) -> false.

%% Promotion table: `{LegacyFlag, LegacyList, CanonicalKey, DefaultValue}`.
%% Maps each promoted legacy `enable' / `disable' entry to a write at
%% its canonical option_key.
promotions() ->
    [
        %% VDF group.
        {compute_own_vdf, disable, [vdf, compute], false},
        {compute_own_vdf, enable, [vdf, compute], true},
        {public_vdf_server, enable, [vdf, is_public_server], true},
        {vdf_server_pull, disable, [vdf, pull], false},
        %% RandomX group.
        {randomx_jit, disable, [randomx, jit], false},
        {randomx_hardware_aes, disable, [randomx, hardware_aes], false},
        {randomx_large_pages, enable, [randomx, large_pages], true},
        %% Gossip group.
        {tx_poller, disable, [gossip, tx, polling_enabled], false},
        {tx_poller, enable, [gossip, tx, polling_enabled], true}
    ].

apply_promotion(Flag, ListName) ->
    Lookup = [{{F, L}, K, V} || {F, L, K, V} <- promotions()],
    case lists:keyfind({Flag, ListName}, 1, Lookup) of
        {{Flag, ListName}, OptionKey, Value} ->
            warn_promoted_flag(Flag, ListName, OptionKey, Value),
            _ = arweave_config:set(OptionKey, Value),
            applied;
        false ->
            not_promoted
    end.

warn_legacy_list_format(Flag, ListName, Value) ->
    io:format(
        "~nWARNING: feature flag `~p' in the legacy `~p' list is "
        "deprecated and the list format will be removed in Arweave "
        "2.9.7 or later. Use `features.~p = ~p' instead.~n",
        [Flag, ListName, Flag, Value]),
    ?LOG_WARNING([
        {event, deprecated_legacy_features_list_format},
        {flag, Flag},
        {list, ListName},
        {replacement_value, Value},
        {removal_target, <<"Arweave 2.9.7 or later">>}
    ]).

warn_promoted_flag(Flag, ListName, OptionKey, Value) ->
    Replacement = replacement_hint(OptionKey, Value),
    io:format(
        "~nWARNING: ~p is deprecated as an entry in the `~p' list "
        "and will be removed in Arweave 2.9.7 or later. "
        "Use `~s' instead.~n",
        [Flag, ListName, Replacement]),
    ?LOG_WARNING([
        {event, deprecated_legacy_flag},
        {flag, Flag},
        {list, ListName},
        {replacement, Replacement},
        {removal_target, <<"Arweave 2.9.7 or later">>}
    ]).

replacement_hint(OptionKey, Value) ->
    <<
        (arweave_config_parser:format_key(OptionKey))/binary,
        " = ",
        (format_replacement_value(Value))/binary
    >>.

format_replacement_value(true) -> <<"true">>;
format_replacement_value(false) -> <<"false">>.

warn_tombstoned_flag(Flag, ListName, Message) ->
    io:format(
        "~nWARNING: feature flag `~p' in the legacy `~p' list is no "
        "longer recognized. ~s~n",
        [Flag, ListName, Message]),
    ?LOG_WARNING([
        {event, tombstoned_feature_flag},
        {flag, Flag},
        {list, ListName},
        {message, Message}
    ]).

warn_unknown_legacy_flag(Flag, ListName) ->
    io:format(
        "~nWARNING: unknown feature flag `~p' in the legacy `~p' "
        "list — typo or removed in an old release? Ignored.~n",
        [Flag, ListName]),
    ?LOG_WARNING([
        {event, unknown_feature_flag},
        {flag, Flag},
        {list, ListName}
    ]).
