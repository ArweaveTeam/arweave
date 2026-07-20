%%% @doc Specs for the `verify` option group. Options for
%%% checking stored data before the node joins normal operation.
-module(arweave_config_options_verify).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0, normalize/0]).
-export([set_mode/4, set_samples/4]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [verify, mode],
            default => false,
            legacy => verify,
            short_description =>
                <<"Run the storage verification scan at startup: "
                  "`purge` or `log`.">>,
            long_description =>
                <<"The node runs several checks on all listed "
                  "storage_modules and flags any errors. In `log` "
                  "mode errors are just logged; in `purge` mode the "
                  "chunks are invalidated so they have to be "
                  "repacked. After completing a full verification "
                  "cycle, restart the node in normal mode to have it "
                  "resync and/or repack any flagged chunks. Several "
                  "flags are disallowed in verify mode — see node "
                  "output for details.">>,
            handle_set => fun arweave_config_options_verify:set_mode/4
        },
        #{
            enabled => true,
            option_key => [verify, samples],
            default => ?SAMPLE_CHUNK_COUNT,
            legacy => verify_samples,
            short_description =>
                <<"Number of chunks to sample and unpack during "
                  "`verify`, or the atom `all` to verify every "
                  "chunk.">>,
            handle_set =>
                fun arweave_config_options_verify:set_samples/4
        }
    ].

%% @doc Cross-cutting: also reads [mining, enabled] and the repack-modules list.
validate() ->
    case arweave_config:get([verify, mode]) of
        false ->
            ok;
        _ ->
            case arweave_config:get([mining, enabled]) of
                true ->
                    {error, <<"The verify flag cannot be set together with "
                            "the mine flag.">>};
                _ ->
                    case arweave_config:get([repack_modules]) of
                        [] ->
                            ok;
                        _ ->
                            {error, <<"The verify flag cannot be set together with "
                                    "[repack_modules].">>}
                    end
            end
    end.

group_description() ->
    <<"Run and tune startup data verification.">>.

%% @doc Post-parse normalization for the verify namespace.
%% Cross-cutting: when verify mode is enabled, force a host of
%% legacy options that would otherwise interfere with verification.
%% Writes flags across [join], [sync], [gossip], [mining], [cm],
%% [peers], and [vdf] namespaces — verify mode is a "node-wide
%% safety override," so this fan-out is by design.
normalize() ->
    case arweave_config:get([verify, mode]) of
        Mode when Mode =:= false; Mode =:= undefined ->
            ok;
        _ ->
            force_verify_flags()
    end.

force_verify_flags() ->
    io:format("~n~nWARNING: The verify flag is set. Forcing the following options:"),
    io:format("~n  - [join, auto] false"),
    io:format("~n  - [join, start_from_latest_state] true"),
    io:format("~n  - [sync, jobs] 0"),
    io:format("~n  - [gossip, block, pollers] 0"),
    io:format("~n  - [gossip, header_sync_jobs] 0"),
    io:format("~n  - [gossip, tx, polling_enabled] false"),
    io:format("~n  - [packing, entropy, workers] 0"),
    io:format("~n  - [gossip, tx, max_peers] 0"),
    io:format("~n  - [gossip, block, max_peers] 0"),
    io:format("~n  - [cm, enabled] false"),
    io:format("~n  - cm_peer and cm_exit peer lists cleared"),
    io:format("~n  - all VDF features disabled"),
    disable_vdf(),
    _ = arweave_config:set([join, auto], false),
    _ = arweave_config:set([join, start_from_latest_state], true),
    _ = arweave_config:set([sync, jobs], 0),
    _ = arweave_config:set([gossip, block, pollers], 0),
    _ = arweave_config:set([gossip, header_sync_jobs], 0),
    _ = arweave_config:set([gossip, tx, polling_enabled], false),
    _ = arweave_config:set([packing, entropy, workers], 0),
    _ = arweave_config:set([cm, enabled], false),
    _ = arweave_config:set([peers, cm_peer], []),
    _ = arweave_config:set([peers, cm_exit], not_set),
    _ = arweave_config:set([gossip, tx, max_peers], 0),
    _ = arweave_config:set([gossip, block, max_peers], 0),
    ok.

disable_vdf() ->
    _ = arweave_config:set([peers, vdf_client], []),
    _ = arweave_config:set([peers, vdf_server], []),
    _ = arweave_config:set([vdf, compute], false),
    _ = arweave_config:set([vdf, is_public_server], false),
    ok.

%% @doc `verify.mode` transform: accepts `false | purge | log` as
%% either atoms or binaries.
set_mode(_K, V, _S, _A) ->
    case decode_mode(V) of
        {ok, Decoded} -> {store, Decoded};
        {error, _} = Err -> Err
    end.

decode_mode(false) -> {ok, false};
decode_mode(purge) -> {ok, purge};
decode_mode(log) -> {ok, log};
decode_mode(<<"purge">>) -> {ok, purge};
decode_mode(<<"log">>) -> {ok, log};
decode_mode(V) -> {error, {bad_verify_mode, V}}.

%% @doc `verify.samples` transform: integer count or the atom `all`.
set_samples(_K, V, _S, _A) ->
    case decode_samples(V) of
        {ok, Decoded} -> {store, Decoded};
        {error, _} = Err -> Err
    end.

decode_samples(N) when is_integer(N), N >= 0 -> {ok, N};
decode_samples(all) -> {ok, all};
decode_samples(<<"all">>) -> {ok, all};
decode_samples(V) when is_binary(V) ->
    %% Env vars arrive as binary strings; parse numerics here since
    %% the spec has no `type' field to coerce them.
    try binary_to_integer(V) of
        N when N >= 0 -> {ok, N};
        _ -> {error, {bad_verify_samples, V}}
    catch
        _:_ -> {error, {bad_verify_samples, V}}
    end;
decode_samples(V) -> {error, {bad_verify_samples, V}}.
