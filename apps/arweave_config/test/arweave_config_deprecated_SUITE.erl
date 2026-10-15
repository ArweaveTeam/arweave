-module(arweave_config_deprecated_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        replacements_are_current_options,
        config_file_renamed_options,
        config_file_current_option_wins,
        cli_deprecated_options,
        env_deprecated_options,
        sync_jobs_zero_stops_chunk_fetching,
        sync_jobs_count_is_ignored,
        legacy_sync_jobs,
        legacy_peer_scans_ignored,
        removed_legacy_options_ignored,
        sync_cache_size_limit_converted
    ].

init_per_testcase(_TestCase, Config) ->
    ok = arweave_config:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = arweave_config:stop().

%%====================================================================
%% Test cases
%%====================================================================

replacements_are_current_options(_Config) ->
    lists:foreach(
        fun({_Flag, #{option_key := Key}}) ->
            ?assertEqual({error, not_found},
                arweave_config_options_registry:resolve(Key)),
            lists:foreach(
                fun({NewKey, _Value}) ->
                    ?assertMatch({ok, NewKey, _, _},
                        arweave_config_options_registry:resolve(NewKey))
                end,
                arweave_config_deprecated:translate(Key, Key, 1))
        end,
        arweave_config_deprecated:long_arguments()).

config_file_renamed_options(_Config) ->
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            {ok, LeafMap} = arweave_config_format_json:parse(jiffy:encode(#{
                network => #{
                    client => #{tcp => #{nodelay => false}},
                    server => #{tcp => #{max_connections => 123}}
                },
                disk_pool => #{jobs => 3}
            })),
            ok = arweave_config:load(LeafMap),
            ?assertEqual(false,
                arweave_config:get([network, client, socket, nodelay])),
            ?assertEqual(123,
                arweave_config:get([network, server, http, max_connections])),
            ?assertEqual(3, arweave_config:get([disk_pool, workers]))
        end),
        ?assertEqual(
            [{<<"disk_pool.jobs">>, renamed},
                {<<"network.client.tcp.nodelay">>, renamed},
                {<<"network.server.tcp.max_connections">>, renamed}],
            lists:sort(Warnings()))
    end).

config_file_current_option_wins(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        {ok, LeafMap} = arweave_config_format_json:parse(jiffy:encode(#{
            disk_pool => #{jobs => 3, workers => 5}
        })),
        ok = arweave_config:load(LeafMap),
        ?assertEqual(5, arweave_config:get([disk_pool, workers]))
    end).

cli_deprecated_options(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        {ok, Map} = arweave_config_format_cli:parse([
            "--network.client.tcp.nodelay", "false",
            "--sync.jobs", "0"
        ]),
        ok = arweave_config:load(Map),
        ?assertEqual(false,
            arweave_config:get([network, client, socket, nodelay])),
        ?assertEqual(0, arweave_config:get([sync, max_download_rate]))
    end).

env_deprecated_options(_Config) ->
    Vars = [{"AR_NETWORK_SERVER_TCP_MAX_CONNECTIONS", "123"},
        {"AR_GOSSIP_HEADER_SYNC_JOBS", "3"}],
    arweave_config:internal_with_test_config(fun() ->
        try
            [true = os:putenv(Name, Value) || {Name, Value} <- Vars],
            ok = arweave_config:load(arweave_config_format_env:parse()),
            ?assertEqual(123,
                arweave_config:get([network, server, http, max_connections])),
            ?assertEqual(3, arweave_config:get([gossip, header, workers]))
        after
            [os:unsetenv(Name) || {Name, _Value} <- Vars]
        end
    end).

sync_jobs_zero_stops_chunk_fetching(_Config) ->
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            ok = arweave_config:load(#{[sync, jobs] => 0}),
            ?assertEqual(0, arweave_config:get([sync, max_download_rate]))
        end),
        ?assertEqual([{<<"sync.jobs">>, converted}], Warnings())
    end).

sync_jobs_count_is_ignored(_Config) ->
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            ok = arweave_config:load(#{[sync, jobs] => 100}),
            ?assertEqual(infinity,
                arweave_config:get([sync, max_download_rate]))
        end),
        ?assertEqual([{<<"sync.jobs">>, ignored}], Warnings())
    end).

legacy_sync_jobs(_Config) ->
    lists:foreach(
        fun({Jobs, Rate}) ->
            arweave_config:internal_with_test_config(fun() ->
                ok = arweave_config_format_legacy_cli:parse(
                    ["sync_jobs", integer_to_list(Jobs)]),
                ?assertEqual(Rate,
                    arweave_config:get([sync, max_download_rate]))
            end),
            arweave_config:internal_with_test_config(fun() ->
                {ok, ok} = arweave_config_format_legacy_json:parse(
                    jiffy:encode(#{sync_jobs => Jobs})),
                ?assertEqual(Rate,
                    arweave_config:get([sync, max_download_rate]))
            end)
        end,
        [{0, 0}, {100, infinity}]).

legacy_peer_scans_ignored(_Config) ->
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            ?assertEqual({ok, ok}, arweave_config_format_legacy_json:parse(
                jiffy:encode(#{data_discovery_max_concurrent_peer_scans => 8})))
        end),
        ?assertEqual([{data_discovery_max_concurrent_peer_scans, ignored}],
            Warnings())
    end).

%% @doc A legacy configuration file or command line that sets options 2.9.6
%% accepted and that no longer exist still loads, with a warning for each.
removed_legacy_options_ignored(_Config) ->
    JSONOptions = [cm_in_batch_timeout, custom_domains, disk_space, gateway,
        max_gateway_connections, max_poa_option_depth,
        mining_server_chunk_cache_size_limit, p3, packing_rate,
        requests_per_minute_limit, requests_per_minute_limit_by_ip,
        tx_propagation_parallelization, tx_validators],
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            ?assertEqual({ok, ok}, arweave_config_format_legacy_json:parse(
                jiffy:encode(maps:from_list(
                    [{Option, 1} || Option <- JSONOptions]))))
        end),
        ?assertEqual(lists:sort([{Option, ignored} || Option <- JSONOptions]),
            lists:sort(Warnings()))
    end),
    %% 2.9.6 took custom_domains and p3 from the configuration file only.
    CLIOptions = JSONOptions -- [custom_domains, p3],
    with_warnings(fun(Warnings) ->
        arweave_config:internal_with_test_config(fun() ->
            ?assertEqual(ok, arweave_config_format_legacy_cli:parse(
                lists:append([[atom_to_list(Option), "1"]
                    || Option <- CLIOptions])))
        end),
        ?assertEqual([{Option, ignored} || Option <- CLIOptions], Warnings())
    end),
    ?assertMatch({error, _},
        arweave_config_format_legacy_cli:parse(["no_such_option", "1"])),
    ?assertMatch({error, unknown, _},
        arweave_config_format_legacy_json:parse(
            jiffy:encode(#{no_such_option => 1}))).

sync_cache_size_limit_converted(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        ok = arweave_config:load(#{[sync, cache_size_limit] => 5}),
        %% Five 256 KiB chunks round up to two MiB.
        ?assertEqual(2, arweave_config:get([packing, cache_size]))
    end).

%%====================================================================
%% Helpers
%%====================================================================

%% Run Fun with a function that returns the {Option, Action} pairs of the
%% deprecation warnings logged so far.
with_warnings(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Filter = fun
        (#{level := warning, msg := {report, Report}}, _)
                when is_list(Report) ->
            case proplists:get_value(event, Report) of
                deprecated_config_option ->
                    Parent ! {Ref, {proplists:get_value(option, Report),
                        proplists:get_value(action, Report)}};
                _ ->
                    ok
            end,
            ignore;
        (_, _) ->
            ignore
    end,
    ok = logger:add_primary_filter(?MODULE, {Filter, undefined}),
    try
        Fun(fun() -> receive_warnings(Ref, []) end)
    after
        ok = logger:remove_primary_filter(?MODULE)
    end.

receive_warnings(Ref, Warnings) ->
    receive
        {Ref, Warning} -> receive_warnings(Ref, [Warning | Warnings])
    after 0 ->
        lists:reverse(Warnings)
    end.
