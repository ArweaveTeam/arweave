-module(ar_peers_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

inbound_headers_test_() ->
    ar_test_util:with_mocked([
        {ar_peers, add_peer, fun(Peer, Release) ->
            self() ! {inbound, Peer, Release}, ok
        end}
    ], fun() ->
        Peer = {1, 2, 3, 4, 1984},
        ?assertEqual(none, observation(#{})),
        ?assertEqual({inbound, Peer, 101}, observation(#{
            <<"x-p2p-port">> => <<"1984">>, <<"x-release">> => <<"101">>})),
        ?assertEqual({inbound, Peer, -1}, observation(#{
            <<"x-p2p-port">> => <<"1984">>})),
        Huge = binary:copy(<<"9">>, 1000),
        ?assertEqual({inbound, Peer, -1}, observation(#{
            <<"x-p2p-port">> => <<"1984">>, <<"x-release">> => Huge})),
        [?assertEqual({inbound, Peer, 101}, observation(#{
            <<"x-p2p-port">> => Port, <<"x-release">> => <<"101">>}))
            || Port <- [<<"0">>, <<"65536">>, <<"bad">>, Huge]],
        ?assertEqual(none, observation(#{<<"x-network">> => <<"other">>,
            <<"x-p2p-port">> => <<"1984">>}))
    end).

outbound_does_not_count_test_() ->
    with_inbound_peers(fun test_outbound_does_not_count/0).

test_outbound_does_not_count() ->
    Peer = {1, 2, 3, 4, 1984},
    ar_peers:rate_fetched_data(Peer, block, 1000, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertMatch(#{peers := []}, ar_peers:get_inbound_peers()),
    ar_peers:add_peer(Peer, 101),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertMatch(#{peers := [#{peer := Peer, release := 101}]},
        ar_peers:get_inbound_peers()),
    ar_peers:remove_peer(test, Peer).

deduplication_and_release_change_test_() ->
    with_inbound_peers(fun() ->
        Now = erlang:monotonic_time(second),
        Peer = {1, 2, 3, 4, 1984},
        ar_peers:observe_inbound_peer(Peer, 90, Now - 1),
        ar_peers:observe_inbound_peer(Peer, 90, Now),
        ?assertEqual(1, count(90)),
        ar_peers:observe_inbound_peer(Peer, 101, Now),
        ?assertEqual(0, count(90)),
        ?assertEqual(1, count(101)),
        ar_peers:observe_inbound_peer(Peer, 82, Now - 2),
        ?assertEqual(1, count(101)),
        ar_peers:observe_inbound_peer({1, 2, 3, 4, 1985}, -1, Now),
        ?assertEqual(1, count(unknown)),
        ?assertMatch(#{window_seconds := 3600, peers := [_, _]},
            ar_peers:get_inbound_peers())
    end).

hour_window_and_expiry_test_() ->
    with_inbound_peers(fun() ->
        Now = erlang:monotonic_time(second),
        ar_peers:observe_inbound_peer({1, 1, 1, 1, 1984}, 101, Now - 3590),
        ar_peers:observe_inbound_peer({2, 2, 2, 2, 1984}, 82, Now - 3600),
        ar_peers:observe_inbound_peer({3, 3, 3, 3, 1984}, 90, Now + 60),
        ?assertEqual(1, count(101)),
        ?assertEqual(0, count(82)),
        ?assertEqual(0, count(90)),
        ?assertEqual(3, ets:info(ar_inbound_peers, size)),
        ar_peers:expire_inbound_peers(),
        ?assertEqual(2, ets:info(ar_inbound_peers, size))
    end).

capacity_and_label_bounds_test_() ->
    with_inbound_peers(fun() ->
        Now = erlang:monotonic_time(second),
        ets:insert(ar_inbound_peers, [{{1, 1, 1, 1, N}, N rem 100, Now}
            || N <- lists:seq(1, 10000)]),
        ar_peers:observe_inbound_peer({2, 2, 2, 2, 1984}, 101, Now),
        ?assertEqual(10000, ets:info(ar_inbound_peers, size)),
        ?assertEqual(34, length(ar_peers:get_inbound_peer_counts())),
        ?assertEqual(10000, lists:sum([N || {_, N}
            <- ar_peers:get_inbound_peer_counts()])),
        ?assertEqual(6800, count(other)),
        ?assertEqual(100, count(0)),
        ?assertEqual(100, count(31)),
        ?assertEqual(0, count(32)),
        ar_peers:observe_inbound_peer({1, 1, 1, 1, 1}, 101, Now),
        ?assertMatch([{_, 101, _}],
            ets:lookup(ar_inbound_peers, {1, 1, 1, 1, 1})),
        ets:delete_all_objects(ar_inbound_peers),
        ar_peers:observe_inbound_peer({1, 1, 1, 1, 1}, 65536, Now),
        ?assertEqual(1, count(unknown)),
        ?assertEqual(2, length(ar_peers:get_inbound_peer_counts()))
    end).

release_count_order_test_() ->
    with_inbound_peers(fun() ->
        Now = erlang:monotonic_time(second),
        ar_peers:observe_inbound_peer({1, 1, 1, 1, 1984}, 101, Now),
        ar_peers:observe_inbound_peer({2, 2, 2, 2, 1984}, 101, Now),
        ar_peers:observe_inbound_peer({3, 3, 3, 3, 1984}, 90, Now),
        ar_peers:observe_inbound_peer({4, 4, 4, 4, 1984}, 100, Now),
        ar_peers:observe_inbound_peer({5, 5, 5, 5, 1984}, -1, Now),
        ?assertEqual([{101, 2}, {90, 1}, {100, 1},
            {unknown, 1}, {other, 0}], ar_peers:get_inbound_peer_counts())
    end).

table_survives_peer_restart_test_() ->
    ar_test_util:with_mocked([
        {ar_storage, read_term, fun
            (peers) -> not_found;
            (Name) -> meck:passthrough([Name])
        end},
        {ar_storage, write_term, fun
            (peers, _) -> ok;
            (Name, Term) -> meck:passthrough([Name, Term])
        end}
    ], with_inbound_peers({setup,
        fun() -> whereis(ar_peers) end,
        fun(PID) ->
            [ar_timer:cancel(Ref)
                || {{timer, Ref}, #{pid := Owner, module := ar_peers}}
                    <- ets:tab2list(ar_timer), Owner =:= PID,
                    not is_process_alive(Owner)],
            case whereis(ar_peers) of
                undefined ->
                    {ok, _} = supervisor:restart_child(ar_sup, ar_peers);
                _ -> ok
            end
        end,
        fun test_table_survives_peer_restart/0})).

test_table_survives_peer_restart() ->
    ets:delete_all_objects(ar_peers),
    Peer = {1, 1, 1, 1, 1984},
    ar_peers:add_peer(Peer, 101),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(1, count(101)),
    ?assertEqual(whereis(ar_sup), ets:info(ar_inbound_peers, owner)),
    PeerProcess = whereis(ar_peers),
    ok = supervisor:terminate_child(ar_sup, ar_peers),
    try
        ?assertEqual(1, count(101))
    after
        {ok, _} = supervisor:restart_child(ar_sup, ar_peers)
    end,
    ?assertNotEqual(PeerProcess, whereis(ar_peers)),
    ?assertEqual(1, count(101)),
    ar_peers:add_peer(Peer, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(0, count(101)),
    ?assertEqual(1, count(100)).

prometheus_output_test_() ->
    with_inbound_peers({setup,
        fun() ->
            prometheus_registry:register_collector(inbound_test,
                ar_metrics_collector)
        end,
        fun(_) ->
            prometheus_registry:deregister_collector(inbound_test,
                ar_metrics_collector)
        end,
        fun() ->
            ar_peers:observe_inbound_peer({1, 1, 1, 1, 1984}, 101,
                erlang:monotonic_time(second)),
            Output = prometheus_text_format:format(inbound_test),
            ?assertNotEqual(nomatch, binary:match(Output,
                <<"# TYPE arweave_inbound_peer_count gauge">>)),
            ?assertNotEqual(nomatch, binary:match(Output,
                <<"arweave_inbound_peer_count{release=\"101\"} 1">>)),
            ets:delete_all_objects(ar_inbound_peers),
            ?assertEqual(nomatch, binary:match(
                prometheus_text_format:format(inbound_test),
                <<"arweave_inbound_peer_count{release=\"101\"}">>))
        end}).

observation(Headers) ->
    Req = #{headers => maps:merge(#{<<"x-network">> => <<?NETWORK_NAME>>},
                                 Headers), peer => {{1, 2, 3, 4}, 34567},
        method => <<"GET">>},
    ar_network_middleware:execute(Req, #{}),
    receive
        {Kind, Peer, Release} -> {Kind, Peer, Release}
    after 0 -> none
    end.

count(Release) ->
    proplists:get_value(Release, ar_peers:get_inbound_peer_counts(), 0).

%% @doc Restore only the inbound observations changed by these tests.
with_inbound_peers(Test) ->
    {setup,
        fun() ->
            Rows = ets:tab2list(ar_inbound_peers),
            ets:delete_all_objects(ar_inbound_peers),
            Rows
        end,
        fun(Rows) ->
            sys:get_state(ar_peers), %% Wait for updates; ignore the state.
            ets:delete_all_objects(ar_inbound_peers),
            ets:insert(ar_inbound_peers, Rows)
        end,
        Test}.

connected_peer_test() ->
    ets:delete_all_objects(ar_peers),
    Peer = {100, 117, 109, 98, 1234},

    ?assertEqual(undefined, ar_peers:get_connection_timestamp_peer(Peer)),

    ar_peers:set_ranked_peers(lifetime, [Peer]),
    ar_peers:set_ranked_peers(current, [Peer]),
    ?assertEqual(false, ar_peers:is_connected_peer(Peer)),
    ?assertEqual(undefined, ar_peers:get_connection_timestamp_peer(Peer)),

    ar_peers:connected_peer(Peer),
    Timestamp = ar_peers:get_connection_timestamp_peer(Peer),
    ?assertEqual(true, ar_peers:is_connected_peer(Peer)),
    ?assertEqual(Timestamp, ar_peers:get_connection_timestamp_peer(Peer)),
    ?assertNotEqual(undefined, ar_peers:get_connection_timestamp_peer(Peer)),
    ?assertEqual([Peer], ar_peers:get_peers(lifetime)),
    ?assertEqual([Peer], ar_peers:get_peers(current)),

    ar_peers:disconnected_peer(Peer),
    ?assertEqual(false, ar_peers:is_connected_peer(Peer)),
    ?assertNotEqual(undefined, ar_peers:get_connection_timestamp_peer(Peer)),
    ?assertEqual([Peer], ar_peers:get_peers(lifetime)),
    ?assertEqual([Peer], ar_peers:get_peers(current)),

    Time = erlang:system_time(second),
    Limit = Time - ((?CURRENT_PEERS_LIST_FILTER + 10) * 60 * 60 * 24),
    ar_peers:set_tag(Peer, {connection, last}, Limit),
    ?assertEqual([], ar_peers:get_peers(current)),
    ?assertEqual([Peer], ar_peers:get_peers(lifetime)).

rotate_peer_ports_test() ->
    ets:delete_all_objects(ar_peers),
    Peer = {2, 2, 2, 2, 1},
    ar_peers:maybe_rotate_peer_ports(Peer),
    [{_, {PortMap, 1}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(1, element(1, PortMap)),
    ar_peers:remove_peer(test, Peer),
    ?assertEqual([], ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}})),
    ar_peers:maybe_rotate_peer_ports(Peer),
    Peer2 = {2, 2, 2, 2, 2},
    ar_peers:maybe_rotate_peer_ports(Peer2),
    [{_, {PortMap2, 2}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(1, element(1, PortMap2)),
    ?assertEqual(2, element(2, PortMap2)),
    ar_peers:remove_peer(test, Peer),
    [{_, {PortMap3, 2}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(empty_slot, element(1, PortMap3)),
    ?assertEqual(2, element(2, PortMap3)),
    Peer3 = {2, 2, 2, 2, 3},
    Peer4 = {2, 2, 2, 2, 4},
    Peer5 = {2, 2, 2, 2, 5},
    Peer6 = {2, 2, 2, 2, 6},
    Peer7 = {2, 2, 2, 2, 7},
    Peer8 = {2, 2, 2, 2, 8},
    Peer9 = {2, 2, 2, 2, 9},
    Peer10 = {2, 2, 2, 2, 10},
    Peer11 = {2, 2, 2, 2, 11},
    ar_peers:maybe_rotate_peer_ports(Peer3),
    ar_peers:maybe_rotate_peer_ports(Peer4),
    ar_peers:maybe_rotate_peer_ports(Peer5),
    ar_peers:maybe_rotate_peer_ports(Peer6),
    ar_peers:maybe_rotate_peer_ports(Peer7),
    ar_peers:maybe_rotate_peer_ports(Peer8),
    ar_peers:maybe_rotate_peer_ports(Peer9),
    ar_peers:maybe_rotate_peer_ports(Peer10),
    [{_, {PortMap4, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(empty_slot, element(1, PortMap4)),
    ?assertEqual(2, element(2, PortMap4)),
    ?assertEqual(10, element(10, PortMap4)),
    ar_peers:maybe_rotate_peer_ports(Peer8),
    ar_peers:maybe_rotate_peer_ports(Peer9),
    ar_peers:maybe_rotate_peer_ports(Peer10),
    [{_, {PortMap5, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(empty_slot, element(1, PortMap5)),
    ?assertEqual(2, element(2, PortMap5)),
    ?assertEqual(3, element(3, PortMap5)),
    ?assertEqual(9, element(9, PortMap5)),
    ?assertEqual(10, element(10, PortMap5)),
    ar_peers:maybe_rotate_peer_ports(Peer11),
    [{_, {PortMap6, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(element(2, PortMap5), element(1, PortMap6)),
    ?assertEqual(3, element(2, PortMap6)),
    ?assertEqual(4, element(3, PortMap6)),
    ?assertEqual(5, element(4, PortMap6)),
    ?assertEqual(11, element(10, PortMap6)),
    ar_peers:maybe_rotate_peer_ports(Peer11),
    [{_, {PortMap7, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(element(2, PortMap5), element(1, PortMap7)),
    ?assertEqual(3, element(2, PortMap7)),
    ?assertEqual(4, element(3, PortMap7)),
    ?assertEqual(5, element(4, PortMap7)),
    ?assertEqual(11, element(10, PortMap7)),
    ar_peers:remove_peer(test, Peer4),
    [{_, {PortMap8, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(empty_slot, element(3, PortMap8)),
    ?assertEqual(3, element(2, PortMap8)),
    ?assertEqual(5, element(4, PortMap8)),
    ar_peers:remove_peer(test, Peer2),
    ar_peers:remove_peer(test, Peer3),
    ar_peers:remove_peer(test, Peer5),
    ar_peers:remove_peer(test, Peer6),
    ar_peers:remove_peer(test, Peer7),
    ar_peers:remove_peer(test, Peer8),
    ar_peers:remove_peer(test, Peer9),
    ar_peers:remove_peer(test, Peer10),
    [{_, {PortMap9, 10}}] = ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}}),
    ?assertEqual(11, element(10, PortMap9)),
    ar_peers:remove_peer(test, Peer11),
    ?assertEqual([], ets:lookup(ar_peers, {peer_ip, {2, 2, 2, 2}})).

update_rating_test() ->
    ets:delete_all_objects(ar_peers),
    Peer1 = {1, 2, 3, 4, 1984},
    Peer2 = {5, 6, 7, 8, 1984},

    ?assertEqual(#performance{}, ar_peers:get_or_init_performance(Peer1)),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:update_rating(Peer1, true),
    ?assertEqual(#performance{}, ar_peers:get_or_init_performance(Peer1)),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:update_rating(Peer1, false),
    ?assertEqual(rounded_performance(#performance{average_success = 0.965}),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:update_rating(Peer1, 1000, 100, false),
    ?assertEqual(rounded_performance(#performance{average_success = 0.9312}),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:update_rating(Peer1, 1000, 100, true),
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 100,
        total_throughput = 0.1,
        total_transfers = 1,
        average_throughput = 0.005,
        average_success = 0.9336
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0934, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1)), 4)),
    ?assertEqual(0.0047, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1)), 4)),

    ar_peers:update_rating(Peer1, 1000, 50, true),
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 150,
        total_throughput = 0.15,
        total_transfers = 2,
        average_throughput = 0.0073,
        average_success = 0.936
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0702, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1)), 4)),
    ?assertEqual(0.0068, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1)), 4)),

    ar_peers:update_rating(Peer2, 1000, 100, true),
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 100,
        total_throughput = 0.1,
        total_transfers = 1,
        average_throughput = 0.005,
        average_success = 1
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer2))),
    ?assertEqual(0.1, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer2)), 4)),
    ?assertEqual(0.005, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer2)), 4)).

block_rejected_test_() ->
    with_inbound_peers({setup,
        fun ar_blacklist_middleware:reset/0,
        fun(_) -> ar_blacklist_middleware:reset() end,
        {timeout, 30, fun test_block_rejected/0}}).

test_block_rejected() ->
    Peer = {127, 0, 0, 1, ar_test_node:get_unused_port()},
    ar_peers:add_peer(Peer, -1),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.

    send_block_event({rejected, invalid_signature, <<>>, Peer}),

    ?assertEqual(#{Peer => #performance{}},
        ar_peers:get_peer_performances([Peer])),
    ?assertEqual(not_banned, ar_blacklist_middleware:is_peer_banned(Peer)),

    send_block_event({rejected, failed_to_fetch_first_chunk, <<>>, Peer}),

    ?assertEqual(
       #{Peer => #performance{ average_success = 0.965 }},
       ar_peers:get_peer_performances([Peer])),
    ?assertEqual(not_banned, ar_blacklist_middleware:is_peer_banned(Peer)),

    send_block_event({rejected, invalid_previous_solution_hash, <<>>, Peer}),

    ?assertEqual(#{Peer => #performance{}},
        ar_peers:get_peer_performances([Peer])),
    ?assertEqual(banned, ar_blacklist_middleware:is_peer_banned(Peer)).

%% Legacy 6/7-arity performance tuples stored the completed AVERAGE rating;
%% the loader must scale it by the transfer count so the derived lifetime
%% rating (total_throughput / total_transfers) reloads unchanged — not as
%% Rating/Transfers.
legacy_performance_load_test() ->
    ets:delete_all_objects(ar_peers),
    Peer = {5, 6, 7, 8, 1984},
    Rating = 750.0,
    Transfers = 12000,
    meck:new(ar_http_iface_client, [passthrough]),
    meck:expect(ar_http_iface_client, get_info,
        fun(_, network) -> <<?NETWORK_NAME>> end),
    try
        ar_peers:load_peer({Peer, {performance, 1000, 0, Transfers, 0, Rating}}),
        Loaded = ar_peers:get_or_init_performance(Peer),
        ?assertEqual(Rating, ar_peers:get_peer_rating(lifetime, Loaded)),
        ?assertEqual(Rating, ar_peers:get_peer_rating(current, Loaded)),
        ar_peers:load_peer({Peer, {performance, 1000, 0, Transfers, 0, Rating, 66}}),
        Loaded2 = ar_peers:get_or_init_performance(Peer),
        ?assertEqual(66, Loaded2#performance.release),
        ?assertEqual(Rating, ar_peers:get_peer_rating(lifetime, Loaded2))
    after
        meck:unload(ar_http_iface_client)
    end.

rate_data_test() ->
    ets:delete_all_objects(ar_peers),
    Peer1 = {1, 2, 3, 4, 1984},

    ?assertEqual(#performance{}, ar_peers:get_or_init_performance(Peer1)),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:rate_fetched_data(Peer1, chunk, {error, timeout}, 1000000, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(rounded_performance(#performance{average_success = 0.965}),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0, ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1))),

    ar_peers:rate_fetched_data(Peer1, block, 1000000, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 100,
        total_throughput = 0.1,
        total_transfers = 1,
        average_throughput = 0.005,
        average_success = 0.9662
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0966, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1)), 4)),
    ?assertEqual(0.0048, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1)), 4)),

    ar_peers:rate_fetched_data(Peer1, tx, ok, 1000000, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 200,
        total_throughput = 0.2,
        total_transfers = 2,
        average_throughput = 0.0098,
        average_success = 0.9674
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0967, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1)), 4)),
    ?assertEqual(0.0094, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1)), 4)),

    ar_peers:rate_gossiped_data(Peer1, block, 1000000, 100),
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ?assertEqual(rounded_performance(#performance{
        total_bytes = 300,
        total_throughput = 0.3,
        total_transfers = 3,
        average_throughput = 0.0143,
        average_success = 0.9685
    }),
        rounded_performance(ar_peers:get_or_init_performance(Peer1))),
    ?assertEqual(0.0969, round(ar_peers:get_peer_rating(lifetime,
        ar_peers:get_or_init_performance(Peer1)), 4)),
    ?assertEqual(0.0138, round(ar_peers:get_peer_rating(current,
        ar_peers:get_or_init_performance(Peer1)), 4)).

%% @doc Wait for the event dispatcher and peer process to handle an event.
send_block_event(Event) ->
    ar_events:send(block, Event),
    sys:replace_state(ar_events:event_to_process(block), fun(State) ->
        %% The event and this barrier reach ar_peers from the same sender.
        sys:get_state(ar_peers), %% Wait for updates; ignore the state.
        State
    end),
    %% Handling a rejection can queue a warning back to the peer process.
    sys:get_state(ar_peers), %% Wait for updates; ignore the state.
    ok.

rounded_performance(Performance) ->
    Performance#performance{
        total_throughput = round(Performance#performance.total_throughput, 4),
        average_throughput = round(Performance#performance.average_throughput, 4),
        average_success = round(Performance#performance.average_success, 4)
    }.

round(Float, N) ->
    Multiplier = math:pow(10, N),
    round(Float * Multiplier) / Multiplier.
