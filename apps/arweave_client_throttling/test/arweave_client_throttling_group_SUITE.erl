%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Direct tests for the `arweave_client_throttling_group'
%%% gen_server, exercised without the supervisor.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_group_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
         start_stop/1,
         independent_peer_state/1,
         pending_helper/1,
         update_before_first_throttle/1,
         blocking_call/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_client_throttling_group).

%% Note: `blocking_call' waits for an exhausted-quota reset of 29s
%% plus client-side overhead, so the timetrap has to clear that.
suite() -> [{userdata, [description()]}, {timetrap, {seconds, 90}}].

description() ->
    {description, "arweave_client_throttling_group gen_server"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    Spec = arweave_client_throttling_config:normalize_group(
             #{id => general,
               initial_remaining => 1,
               max_queue_length => 4,
               concurrency_window_ms => 50}),
    {ok, Pid} = ?M:start_link(Spec),
    [{group_pid, Pid}, {spec, Spec} | Config].

end_per_testcase(_TestCase, _Config) ->
    case whereis(arweave_client_throttling_group_general) of
        undefined -> ok;
        _ -> ok = ?M:stop(general)
    end.

all() ->
    [
     start_stop,
     independent_peer_state,
     pending_helper,
     update_before_first_throttle,
     blocking_call
    ].

%%--------------------------------------------------------------------
%% @doc Verify the worker is registered under the expected name.
%% @end
%%--------------------------------------------------------------------
start_stop(Config) ->
    Pid = proplists:get_value(group_pid, Config),
    Pid = whereis(arweave_client_throttling_group_general),
    true = is_process_alive(Pid),
    ok.

%%--------------------------------------------------------------------
%% @doc State is maintained independently per peer.
%% @end
%%--------------------------------------------------------------------
independent_peer_state(_Config) ->
    PeerA = {1, 1, 1, 1, 1984},
    PeerB = {2, 2, 2, 2, 1984},

    ok = ?M:throttle(general, PeerA),

    {ok, SA} = ?M:status(general, PeerA),
    {ok, SB} = ?M:status(general, PeerB),
    0 = maps:get(remaining, SA),
    1 = maps:get(remaining, SB),
    0 = maps:get(queue_length, SB),
    ok.

%%--------------------------------------------------------------------
%% @doc The `pending/2' helper reports the queue length.
%% @end
%%--------------------------------------------------------------------
pending_helper(_Config) ->
    Peer = {3, 3, 3, 3, 1984},
    Parent = self(),

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
                         #{total => 10, remaining => 1, reset_seconds => 0}),
    receive done -> ok after 1000 -> ct:fail(not_released) end,
    0 = ?M:pending(general, Peer),
    ok.

%%--------------------------------------------------------------------
%% @doc An update arriving before any throttle/2 must initialise the
%% peer state with the reported quota values.
%% @end
%%--------------------------------------------------------------------
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


blocking_call(_Config) ->
    Peer = {5, 5, 5, 5, 1984},
    ok = ?M:update_quota(general, Peer,
                         #{total => 1, remaining => 0, reset_seconds => 29}),
    {Time, Return} =
        timer:tc(?M, throttle, [general, Peer]),

    ?assertEqual(ok, Return),
    ?assert(Time > 29000000),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
wait_until(Fun) -> wait_until(Fun, 50).

wait_until(_Fun, 0) -> {error, timeout};
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ ->
            timer:sleep(20),
            wait_until(Fun, N - 1)
    end.
