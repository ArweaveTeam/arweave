%%% @doc Direct tests for the `arweave_throttling_group'
%%% gen_server, exercised without the supervisor.
%%% @end
-module(arweave_throttling_group_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_group).

%% Note: `blocking_call' waits for an exhausted-quota reset of 29s
%% plus client-side overhead, so the timetrap has to clear that.
suite() -> [{userdata, [description()]}, {timetrap, {seconds, 90}}].

description() ->
    {description, "arweave_throttling_group gen_server"}.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Spec = #{id => general},

    ok = meck:new([prometheus_counter, prometheus_histogram], [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_histogram, observe, 3, ok),

    {ok, Pid} = ?M:start_link(Spec),
    [{group_pid, Pid}, {spec, Spec} | Config].

end_per_testcase(_TestCase, _Config) ->
    case whereis(arweave_throttling_group_general) of
        undefined -> ok;
        _ -> ok = ?M:stop(general)
    end,
    ok = meck:unload([prometheus_counter, prometheus_histogram]),
    ok.

all() ->
    [
     start_stop,
     independent_peer_state,
     pending_helper,
     remote_peer_reverted_no_more_headers,
     update_before_first_throttle,
     blocking_call
    ].


%% @doc Verify the worker is registered under the expected name.
start_stop(Config) ->
    Pid = proplists:get_value(group_pid, Config),
    Pid = whereis(arweave_throttling_group_general),
    true = is_process_alive(Pid),
    ok.

%% @doc State is maintained independently per peer.
initial_peer_state(_Config) ->
    PeerA = {1, 1, 1, 1, 1984},
    PeerB = {2, 2, 2, 2, 1984},

    ok = ?M:throttle(general, PeerA),

    {ok, SA} = ?M:status(general, PeerA),
    {ok, SB} = ?M:status(general, PeerB),
    ?assertEqual(infinity, maps:get(remaining, SA)),
    ?assertEqual(infinity, maps:get(remaining, SB)),
    ?assertEqual(0, maps:get(queue_length, SB)),
    ok.

%% @doc State is maintained independently per peer.
independent_peer_state(_Config) ->
    PeerA = {1, 1, 1, 1, 1984},
    PeerB = {2, 2, 2, 2, 1984},

    ok = ?M:update_quota(general, PeerA,
                #{id => general,
                    total => 1,
                    remaining => 1,
                    reset_seconds => 0}),
    ok = ?M:update_quota(general, PeerB,
                #{id => general,
                    total => 10,
                    remaining => 10,
                    reset_seconds => 0}),

    ok = ?M:throttle(general, PeerA),

    {ok, SA} = ?M:status(general, PeerA),
    {ok, SB} = ?M:status(general, PeerB),
    ?assertEqual(0, maps:get(remaining, SA)),
    ?assertEqual(10, maps:get(remaining, SB)),
    ?assertEqual(0, maps:get(queue_length, SB)),
    ok.

%% @doc The `pending/2' helper reports the queue length.
pending_helper(_Config) ->
    Peer = {3, 3, 3, 3, 1984},
    Parent = self(),

    ok = ?M:update_quota(general, Peer,
                #{total => 1,
                    remaining => 1,
                    reset_seconds => 0}),

    0 = ?M:pending(general, Peer),

    ok = ?M:throttle(general, Peer),
    spawn(fun() ->
        ok = ?M:throttle(general, Peer),
        Parent ! done
    end),
    ok = wait_until(fun() ->
        ?M:pending(general, Peer) =:= 1
    end),

    ok = ?M:update_quota(general, Peer,
                #{total => 10, remaining => 1, reset_seconds => 1}),
    receive done -> ok after 10000 -> ct:fail(not_released) end,
    0 = ?M:pending(general, Peer),
    ok.

remote_peer_reverted_no_more_headers(_Config) ->
    Peer = {1, 2, 3, 4, 1984},

    ok = ?M:update_quota(general, Peer, #{total => 10, remaining => 9, reset_seconds => 0}),
    ok = ?M:throttle(general, Peer),
    {ok, S1} = ?M:status(general, Peer),
    ?assertMatch(#{total := 10, remaining := 8}, S1),
    ok = ?M:update_quota(general, Peer, #{total => 10, remaining => 8, reset_seconds => 0}),
    {ok, S2} = ?M:status(general, Peer),
    ?assertMatch(#{total := 10, remaining := 8}, S2),
    %% So far everything is going alright. Let's do another one.
    ok = ?M:throttle(general, Peer),
    {ok, S3} = ?M:status(general, Peer),
    ?assertMatch(#{total := 10, remaining := 7}, S3),
    ok = ?M:reset_peer(general, Peer),
    {ok, S4} = ?M:status(general, Peer),
    ?assertMatch(#{total := infinity}, S4),
    ok.

%% @doc An update arriving before any throttle/2 must initialise the
%% peer state with the reported quota values.
%% @end
update_before_first_throttle(_Config) ->
    Peer = {4, 4, 4, 4, 1984},

    ok = ?M:update_quota(general, Peer,
                        #{total => 50, remaining => 7, reset_seconds => 0}),
    ok = wait_until(fun() ->
                case ?M:status(general, Peer) of
                    {ok, S} ->
                        (maps:get(remaining, S) =:= 7)
                        andalso (maps:get(total, S) =:= 50);
                    _ ->
                        false
                end
            end),
    ok.

%%% @doc This is a long test, making sure that a potentially extreme wait can
%%% work.
blocking_call(_Config) ->
    Peer = {5, 5, 5, 5, 1984},
    ok = ?M:update_quota(general, Peer,
                #{total => 1, remaining => 0, reset_seconds => 29}),
    {Time, Return} =
        timer:tc(?M, throttle, [general, Peer]),

    ?assertEqual(ok, Return),
    ?assert(Time > 29000000),
    ok.

%% Helpers
wait_until(Fun) -> wait_until(Fun, 50).

wait_until(_Fun, 0) -> {error, timeout};
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ ->
            timer:sleep(20),
            wait_until(Fun, N - 1)
    end.
