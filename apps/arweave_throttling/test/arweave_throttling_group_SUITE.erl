%%% @doc Direct tests for the `arweave_throttling_group'
%%% gen_server, exercised without the supervisor.
%%% @end
-module(arweave_throttling_group_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_group).
-define(GROUPID_GENERAL, "general").

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
    Spec = #{id => "general"},
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    ok = arweave_config:start(),

    ok = meck:new([prometheus_counter, prometheus_histogram], [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_histogram, observe, 3, ok),

    ok = arweave_throttling_process:init(),

    {ok, Pid} = ?M:start_link(Spec),
    true = ets:insert(arweave_throttling_process, {list_to_binary(?GROUPID_GENERAL), Pid}),

    [{group_pid, Pid}, {spec, Spec}, {apps_before, AppsBefore} | Config].

end_per_testcase(_TestCase, Config) ->
    case whereis(arweave_throttling_group_general) of
        undefined -> ok;
        _ -> ok = ?M:stop(?GROUPID_GENERAL)
    end,
    ok = meck:unload([prometheus_counter, prometheus_histogram]),
    ok = arweave_throttling_process:cleanup(),
    AppsBefore = ?config(apps_before, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    lists:foreach(fun application:stop/1, AppsNow -- AppsBefore),
    ok.

all() ->
    [
     independent_peer_state,
     pending_helper,
     remote_peer_reverted_no_more_headers,
     update_before_first_throttle,
     blocking_and_draining_to_reset_amount,
     blocking_call
    ].

%% @doc State is maintained independently per peer.
initial_peer_state(_Config) ->
    PeerA = {1, 1, 1, 1, 1984},
    PeerB = {2, 2, 2, 2, 1984},

    ok = ?M:throttle(?GROUPID_GENERAL, PeerA),

    {ok, SA} = ?M:status(?GROUPID_GENERAL, PeerA),
    {ok, SB} = ?M:status(?GROUPID_GENERAL, PeerB),
    ?assertEqual(infinity, maps:get(remaining, SA)),
    ?assertEqual(infinity, maps:get(remaining, SB)),
    ?assertEqual(0, maps:get(queue_length, SB)),
    ok.

%% @doc State is maintained independently per peer.
independent_peer_state(Config) ->
    PeerA = {1, 1, 1, 1, 1984},
    PeerB = {2, 2, 2, 2, 1984},

    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, PeerA,
                #{id => general,
                  total => 1,
                  remaining => 1,
                  reset_amount => 0,
                  reset_seconds => 0}),
    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, PeerB,
                         #{id => general,
                           total => 10,
                           remaining => 10,
                           reset_amount => 0,
                           reset_seconds => 0}),

    ok = ?M:throttle(?GROUPID_GENERAL, PeerA),

    {ok, SA} = ?M:status(?GROUPID_GENERAL, PeerA),
    {ok, SB} = ?M:status(?GROUPID_GENERAL, PeerB),
    ?assertEqual(0, maps:get(remaining, SA)),
    ?assertEqual(10, maps:get(remaining, SB)),
    ?assertEqual(0, maps:get(queue_length, SB)),
    ok.

%% @doc The `pending/2' helper reports the queue length.
pending_helper(Config) ->
    Peer = {3, 3, 3, 3, 1984},
    Parent = self(),
    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer,
                         #{total => 1,
                           remaining => 1,
                           reset_amount => 0,
                           reset_seconds => 0}),
    
    0 = ?M:pending(?GROUPID_GENERAL, Peer),

    ok = ?M:throttle(?GROUPID_GENERAL, Peer),
    spawn(fun() ->
        ok = ?M:throttle(?GROUPID_GENERAL, Peer),
        Parent ! done
    end),
    ok = wait_until(fun() ->
        ?M:pending(?GROUPID_GENERAL, Peer) =:= 1
    end),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer,
                         #{total => 10,
                           remaining => 1,
                           reset_amount => 9,
                           reset_seconds => 1}),
    receive done -> ok after 10000 -> ct:fail(not_released) end,
    0 = ?M:pending(?GROUPID_GENERAL, Peer),
    ok.

remote_peer_reverted_no_more_headers(Config) ->
    Peer = {1, 2, 3, 4, 1984},
    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer, #{total => 10, remaining => 9,
                                          reset_amount => 1, reset_seconds => 0}),
    ok = ?M:throttle(?GROUPID_GENERAL, Peer),
    {ok, S1} = ?M:status(?GROUPID_GENERAL, Peer),
    ?assertMatch(#{total := 10, remaining := 8}, S1),
    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer, #{total => 10, remaining => 8,
                                          reset_amount => 2, reset_seconds => 0}),
    {ok, S2} = ?M:status(?GROUPID_GENERAL, Peer),
    ?assertMatch(#{total := 10, remaining := 8}, S2),
    %% So far everything is going alright. Let's do another one.
    ok = ?M:throttle(?GROUPID_GENERAL, Peer),
    {ok, S3} = ?M:status(?GROUPID_GENERAL, Peer),
    ?assertMatch(#{total := 10, remaining := 7}, S3),
    ok = ?M:reset_peer(?GROUPID_GENERAL, Peer),
    {ok, S4} = ?M:status(?GROUPID_GENERAL, Peer),
    ?assertMatch(#{total := infinity}, S4),
    ok.

%% @doc An update arriving before any throttle/2 must initialise the
%% peer state with the reported quota values.
%% @end
update_before_first_throttle(Config) ->
    Peer = {4, 4, 4, 4, 1984},
    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer,
                        #{total => 50,
                          remaining => 7,
                          reset_amount => 43,
                          reset_seconds => 0}),
    ok = wait_until(fun() ->
                case ?M:status(?GROUPID_GENERAL, Peer) of
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
blocking_call(Config) ->
    Peer = {5, 5, 5, 5, 1984},
    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer,
                #{total => 1, remaining => 0, reset_amount => 1, reset_seconds => 29}),
    {Time, Return} =
        timer:tc(?M, throttle, [?GROUPID_GENERAL, Peer]),

    ?assertEqual(ok, Return),
    ?assert(Time > 29000000),
    ok.

blocking_and_draining_to_reset_amount(Config) ->
    Peer = {6, 6, 6, 6, 1984},
    Pid = ?config(group_pid, Config),

    ok = ?M:update_quota(Pid, ?GROUPID_GENERAL, Peer,
                         #{total => 100, remaining => 0,
                           reset_amount => 20, reset_seconds => 2}),

    Parent = self(),
    SeqNos = lists:seq(1,5),
    lists:foreach(
        fun(N) ->
                spawn(
                    fun() ->
                            ok = ?M:throttle(?GROUPID_GENERAL, Peer),
                            Parent ! {done, N}
                  end)
        end, SeqNos),

    ok = wait_until(fun() -> ?M:pending(?GROUPID_GENERAL, Peer) == 5 end),

    %% 5requests get throttled and queued.
    ?assertEqual(5, ?M:pending(?GROUPID_GENERAL, Peer)),

    lists:foreach(
        fun(N) ->
            receive
                {done, N} ->
                    ok
            after 3000 ->
                    %% reset seconds is 2, so if we have to wait 3 for
                    %% the throttled/queued requests to progress
                    %% something is wrong
                    erlang:error({receive_timeout, [{seqno, N}]})
            end
        end, SeqNos),

    %% By the time we received the messages, none should be pending
    ?assertEqual(0, ?M:pending(?GROUPID_GENERAL, Peer)),
    %% The status should reflect 20-5 remaining, 0 reset seconds, 100 total.
    %% 20 was reset, and then 5 waiters were drained, and served
    ?assertMatch(
       {ok, #{total := 100,remaining := 15,queue_length := 0,
              reset_seconds := 0, last_update_ts := _}},
       ?M:status(?GROUPID_GENERAL, Peer)),
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
