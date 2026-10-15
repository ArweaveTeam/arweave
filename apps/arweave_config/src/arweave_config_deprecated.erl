%%% @doc Options that earlier releases accepted and this one renamed or
%%% removed. A configuration that still sets one keeps loading: the value
%%% moves to the option that replaced it, or is dropped, with a warning.
-module(arweave_config_deprecated).
-compile(warnings_as_errors).
-export([set/3, translate/3, translate_map/1, long_arguments/0,
    environments/0, ignore_removed_legacy_option/1]).
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").

%% @doc Set the current options that replace the deprecated option Key, which
%% the operator configured as Name.
set(Name, Key, Value) ->
    case translate(Name, Key, Value) of
        not_deprecated ->
            ok;
        Options ->
            lists:foreach(
                fun({NewKey, NewValue}) ->
                    _ = arweave_config:set(NewKey, NewValue)
                end,
                Options)
    end.

%% @doc Return the current options that a deprecated option's value sets, or
%% not_deprecated. Logs a warning that names the option as Name.
translate(Name, Key, Value) ->
    case lookup(Key) of
        {ok, Replacement} -> replace(Name, Replacement, Value);
        not_found -> not_deprecated
    end.

%% @doc Replace the deprecated options in a leaf map with the current ones.
%% A current option set in the same map keeps its value.
translate_map(Map) ->
    Present = sets:from_list([key_name(Key) || Key <- maps:keys(Map)]),
    maps:fold(
        fun(Key, Value, Acc) ->
            case translate(key_name(Key), Key, Value) of
                not_deprecated ->
                    Acc;
                Options ->
                    lists:foldl(
                        fun({NewKey, NewValue}, Acc2) ->
                            case sets:is_element(key_name(NewKey), Present) of
                                true -> Acc2;
                                false -> Acc2#{NewKey => NewValue}
                            end
                        end,
                        maps:remove(Key, Acc),
                        Options)
            end
        end,
        Map,
        Map).

%% @doc Return the command-line flags of the deprecated options, each with a
%% spec that parses its value.
long_arguments() ->
    [{arweave_config_options_spec:long_argument_convert(Key),
        argument_spec(Key, Replacement)}
        || {Key, Replacement} <- options()].

%% @doc Return the environment variables of the deprecated options.
environments() ->
    [{arweave_config_options_spec:environment_convert(Key), Key}
        || {Key, _Replacement} <- options()].

%% @doc Return true, with a warning, if Name, a binary, names an option of the
%% legacy configuration file or command line that 2.9.6 accepted and that no
%% longer has an effect.
ignore_removed_legacy_option(Name) ->
    case [Option || Option <- removed_legacy_options(),
            atom_to_binary(Option) =:= Name] of
        [Option] ->
            warn(Option, ignored, <<"The option no longer exists.">>),
            true;
        [] ->
            false
    end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

options() ->
    [
        {[disk_pool, jobs], {renamed, [disk_pool, workers]}},
        {[gossip, header_sync_jobs], {renamed, [gossip, header, workers]}},
        {[gossip, header_cache_size],
            {renamed, [gossip, header, cache_size]}},
        {[network, server, transport, idle_timeout],
            {renamed, [network, server, http, idle_timeout]}},
        {[network, server, shutdown_connection_timeout],
            {renamed, [network, server, shutdown, connection_timeout]}},
        {[network, server, shutdown_mode],
            {renamed, [network, server, shutdown, mode]}},
        {[network, server, socket_backend],
            {renamed, [network, server, socket, backend]}},
        {[sync, jobs], sync_jobs},
        {[sync, cache_size_limit], sync_cache_size_limit},
        {[sync, max_concurrent_peer_scans], removed}
    ]
    ++ [{[network, client, tcp, Name],
            {renamed, [network, client, socket, Name]}}
        || Name <- [delay_send, keepalive, linger, linger_timeout, nodelay,
            send_timeout, send_timeout_close]]
    ++ [{[network, server, tcp, Name],
            {renamed, [network, server, socket, Name]}}
        || Name <- [backlog, delay_send, keepalive, linger, linger_timeout,
            nodelay, send_timeout, send_timeout_close]]
    ++ [{[network, server, tcp, Name],
            {renamed, [network, server, http, Name]}}
        || Name <- [listener_shutdown, max_connections, num_acceptors]].

removed_legacy_options() ->
    [cm_in_batch_timeout, custom_domains, disk_space, gateway,
        max_gateway_connections, max_poa_option_depth,
        mining_server_chunk_cache_size_limit, p3, packing_rate,
        requests_per_minute_limit, requests_per_minute_limit_by_ip,
        tx_propagation_parallelization, tx_validators].

lookup(Key) ->
    Name = key_name(Key),
    case [Replacement || {Deprecated, Replacement} <- options(),
            key_name(Deprecated) =:= Name] of
        [Replacement] -> {ok, Replacement};
        [] -> not_found
    end.

key_name(Key) ->
    case arweave_config_parser:key(Key) of
        {ok, Option} -> arweave_config_parser:format_key(Option);
        _ -> Key
    end.

replace(Name, {renamed, NewKey}, Value) ->
    NewName = arweave_config_parser:format_key(NewKey),
    warn(Name, renamed, <<"Use ", NewName/binary, ".">>),
    [{NewKey, Value}];
replace(Name, sync_jobs, Value) ->
    case arweave_config_type:non_neg_integer(Value) of
        {ok, 0} ->
            warn(Name, converted, <<"0 now sets sync.max_download_rate to 0, "
                "which stops fetching chunks from peers. Peer discovery and "
                "copying chunks between local storage modules keep "
                "running.">>),
            [{[sync, max_download_rate], 0}];
        _ ->
            warn(Name, ignored, <<"Fetch concurrency is sized automatically. "
                "Use sync.max_download_rate to limit sync throughput.">>),
            []
    end;
replace(Name, sync_cache_size_limit, Value) ->
    case arweave_config_type:non_neg_integer(Value) of
        {ok, Chunks} when is_integer(Chunks) ->
            warn(Name, converted, <<"The limit counts 256 KiB chunks. It now "
                "sets packing.cache_size, the chunk cache shared by syncing, "
                "copying and packing, in MiB.">>),
            [{[packing, cache_size], ?LEGACY_CHUNKS_TO_CACHE_MIB(Chunks)}];
        _ ->
            warn(Name, ignored, <<"Use packing.cache_size (MiB).">>),
            []
    end;
replace(Name, removed, _Value) ->
    warn(Name, ignored, <<"The option no longer exists.">>),
    [].

warn(Name, Action, Reason) ->
    ?LOG_WARNING([{event, deprecated_config_option}, {option, Name},
        {action, Action}, {reason, Reason}]).

argument_spec(Key, {renamed, NewKey}) ->
    case arweave_config_options_registry:type(NewKey) of
        {ok, Type} -> #{option_key => Key, type => Type};
        error -> #{option_key => Key}
    end;
argument_spec(Key, _Replacement) ->
    #{option_key => Key, type => non_neg_integer}.
