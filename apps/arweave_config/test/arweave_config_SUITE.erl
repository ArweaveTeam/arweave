%%% @doc
-module(arweave_config_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").

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
        arweave_config,
        load,
        force_config_runtime_guard,
        peer_role_operations,
        clear_list_value,
        list_value_writes,
        list_root_set_get,
        with_test_config_isolation,
        runtime_rejects_on_validator_error
    ].

%%====================================================================
%% Test cases
%%====================================================================

arweave_config(_Config) ->
    _ = arweave_config:get([debug]),

    undefined = arweave_config:get([missing, option]),

    ok = arweave_config:set([debug], true),
    true = arweave_config:get([debug]),

    ok = arweave_config:set([debug], false),
    false = arweave_config:get([debug]),
    ok.

load(_Config) ->
    ok = arweave_config:load(#{
        [data_dir] => <<"/tmp/test_load">>,
        [port] => 1985,
        [debug] => true
    }),
    "/tmp/test_load" = arweave_config:get([data_dir]),
    1985 = arweave_config:get([port]),
    true = arweave_config:get([debug]),

    ok = arweave_config:load(#{
        [network, server, tcp, backlog] => 2048
    }),
    2048 = arweave_config:get([network, server, tcp, backlog]),

    ok = arweave_config:load(#{
        [log_dir] => "/tmp/test_logs",
        [network, server, tcp, max_connections] => 1234
    }),
    "/tmp/test_logs" = arweave_config:get([log_dir]),
    1234 = arweave_config:get(
        [network, server, tcp, max_connections]),

    ok = arweave_config:load(#{}),

    ok.

peer_role_operations(_Config) ->
    arweave_config:with_test_config(fun() ->
        Peer1 = {127,0,0,1,1984},
        Peer2 = {127,0,0,2,1984},

        ok = arweave_config:set([peers, trusted],
            [Peer1, Peer2]),
        2 = length(arweave_config:get([peers, trusted])),

        ok = arweave_config:set([peers, trusted], [Peer2]),
        Trusted = arweave_config:get([peers, trusted]),
        1 = length(Trusted),
        true = lists:member(Peer2, Trusted),
        false = lists:member(Peer1, Trusted),

        ok = arweave_config:set([peers, trusted], []),
        [] = arweave_config:get([peers, trusted])
    end),
    ok.

force_config_runtime_guard(_Config) ->
    false = arweave_config:is_runtime(),
    ok = arweave_config:runtime(),
    true = arweave_config:is_runtime(),

    %% `[data_dir]` is a `runtime => false` spec. Without
    %% `force_config/1` it would be rejected because the lifecycle is
    %% in runtime mode.
    ok = arweave_config:force_config(#{
        [data_dir] => <<"/tmp/test_force_config">>
    }),
    "/tmp/test_force_config" = arweave_config:get([data_dir]),

    true = arweave_config:is_runtime(),

    ok.

clear_list_value(_Config) ->
    arweave_config:with_test_config(fun() ->
        Peers = [{127,0,0,1,1984}, {127,0,0,2,1984}],
        ok = arweave_config:set([peers, trusted], Peers),
        2 = length(arweave_config:get([peers, trusted])),

        ok = arweave_config:set([peers, trusted], []),
        [] = arweave_config:get([peers, trusted])
    end),
    ok.

list_value_writes(_Config) ->
    arweave_config:with_test_config(fun() ->
        OldPeers = [{127,0,0,1,1984}, {127,0,0,2,1984}],
        ok = arweave_config:set([peers, trusted], OldPeers),
        2 = length(arweave_config:get([peers, trusted])),

        NewPeers = [{10,0,0,1,1984}],
        ok = arweave_config:set([peers, trusted], NewPeers),
        Trusted = arweave_config:get([peers, trusted]),
        1 = length(Trusted),
        true = lists:member({10,0,0,1,1984}, Trusted),
        false = lists:member({127,0,0,1,1984}, Trusted),
        false = lists:member({127,0,0,2,1984}, Trusted),

        StorageModule = {0, 100, unpacked},
        ok = arweave_config:set([storage_modules], []),
        ok = arweave_config_options_storage_modules:write_legacy_storage_module(StorageModule),
        [StorageModule] = arweave_config_options_storage_modules:storage_modules(),

        RepackModule = {{0, 100, unpacked}, {replica_2_9, <<0:256>>}},
        ok = arweave_config:set([repack_modules], []),
        ok = arweave_config_options_repack_modules:write_legacy_repack_module(RepackModule),
        [RepackModule] = arweave_config_options_repack_modules:repack_modules(full),

        Webhook = #{url => <<"http://127.0.0.1/hook">>, events => [<<"block">>], headers => #{}},
        ok = arweave_config:set([webhooks], []),
        ok = arweave_config_options_webhooks:write_legacy_webhook(test_hook, Webhook),
        [Webhook] = arweave_config_options_webhooks:legacy_list()
    end),
    ok.

list_root_set_get(_Config) ->
    arweave_config:with_test_config(fun() ->
        PartitionSize = ar_block:partition_size(),
        StorageMap = #{
            partition => 0,
            packing_format => unpacked,
            defrag => false
        },
        ok = arweave_config:set([storage_modules], [StorageMap]),
        [StorageMap] = arweave_config:get([storage_modules]),
        [{0, PartitionSize, unpacked}] =
            arweave_config_options_storage_modules:storage_modules(),
        ok = arweave_config:set([storage_modules], []),

        Addr = <<0:256>>,
        RepackMap = #{
            partition => 1,
            from_format => unpacked,
            to_format => replica_2_9,
            to_address => Addr
        },
        ok = arweave_config:set([repack_modules], [RepackMap]),
        [RepackMap] = arweave_config:get([repack_modules]),
        DoublePartitionSize = 2 * PartitionSize,
        [{{PartitionSize, DoublePartitionSize, unpacked}, {replica_2_9, Addr}}] =
            arweave_config_options_repack_modules:repack_modules(full),
        ok = arweave_config:set([repack_modules], []),

        WebhookMap = #{
            enabled => true,
            url => <<"http://127.0.0.1/hook">>,
            events => [<<"block">>],
            headers => #{}
        },
        ok = arweave_config:set([webhooks], [WebhookMap]),
        [WebhookMap] = arweave_config:get([webhooks]),
        ExpectedWebhook = maps:without([enabled], WebhookMap),
        [ExpectedWebhook] = arweave_config_options_webhooks:legacy_list(),
        ok = arweave_config:set([webhooks], []),
        {error, not_found} =
            arweave_config:set([webhooks, 1, url],
                <<"http://127.0.0.1/hook">>)
    end),
    ok.

with_test_config_isolation(_Config) ->
    OriginalDebug = arweave_config:get([debug]),

    arweave_config:with_test_config(fun() ->
        ok = arweave_config:set([debug], true),
        true = arweave_config:get([debug])
    end),

    OriginalDebug = arweave_config:get([debug]),

    ok.

runtime_rejects_on_validator_error(_Config) ->
    false = arweave_config:is_runtime(),

    ok = arweave_config:set([cm, enabled], true),
    %% Sanity check: api_secret is not_set by default.
    not_set = arweave_config:get([cm, api_secret]),

    {error, _} = arweave_config:runtime(),
    false = arweave_config:is_runtime(),

    ok = arweave_config:set([cm, enabled], false),

    ok.
