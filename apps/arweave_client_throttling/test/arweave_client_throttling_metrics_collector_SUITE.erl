%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Tests for `arweave_client_throttling_metrics_collector'.
%%%
%%% The collector exposes a single `arweave_client_throttling_peers'
%%% gauge metric family. Each test case starts the application with
%%% a single group whose `initial_remaining' is 0 so that every
%%% `throttle/2' call is queued (and therefore registers the peer
%%% in the group's peers map), then asserts how many peers the
%%% collector reports.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_metrics_collector_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
    no_peers_reported/1,
    one_peer_reported/1,
    two_hundred_peers_reported/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_client_throttling_metrics_collector).
-define(GROUP, general).
-define(PATH, "some/path/that/lead/to/general").

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_client_throttling_metrics_collector"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    %% A single group whose budget is exhausted from the start, so
    %% every `throttle/2' call enqueues the caller (and therefore
    %% registers the peer in the group's peers map). A generous
    %% `max_queue_length' keeps the 200-peer case from being
    %% rejected. `concurrency_window_ms' is irrelevant here since
    %% the suite does not call `update_quota'.
    application:set_env(arweave_client_throttling, groups, [
        #{id => ?GROUP,
          initial_remaining => 0,
          max_queue_length => 10000,
          concurrency_window_ms => 50}
    ]),
    ct:pal(info, 1, "start arweave_client_throttling"),
    ok = arweave_client_throttling:start(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% `reset/1' sends `{request_ready, _}' to every queued waiter,
    %% so the test-spawned callers return from `throttle/2' and
    %% exit cleanly without leaking.
    _ = catch arweave_client_throttling:reset(?GROUP),
    ok = arweave_client_throttling:stop(),
    arweave_client_throttling_metrics:cleanup(),
    application:unset_env(arweave_client_throttling, groups),
    ok.

all() ->
    [
        no_peers_reported,
        one_peer_reported,
        two_hundred_peers_reported
    ].

%% @doc With no `throttle/2' calls issued, the gauge family
%% returned by the collector reports zero peers for the configured
%% group.
no_peers_reported(_Config) ->
    [{arweave_client_throttling_peers, gauge, _Help, MetricsList}] =
        ?M:metrics(),
    ?assertEqual([{[{group_id, ?GROUP}], 0}], MetricsList),
    ok.

%% @doc After a single `throttle/2' call against a single peer the
%% collector reports exactly one peer for the group.
one_peer_reported(_Config) ->
    Peer = {127, 0, 0, 1, 1984},
    _ = spawn(fun() ->
                      arweave_client_throttling:throttle(Peer, ?PATH)
              end),
    ok = wait_peer_count(?GROUP, 1),
    [{arweave_client_throttling_peers, gauge, _Help, MetricsList}] =
        ?M:metrics(),
    ?assertEqual([{[{group_id, ?GROUP}], 1}], MetricsList),
    ok.

%% @doc Two hundred distinct peers must all show up in the gauge.
%% This case also exercises the queue under load: every spawned
%% caller stays parked on `{request_ready, _}' until
%% `end_per_testcase' resets the group.
two_hundred_peers_reported(_Config) ->
    Peers = [{10, 0, X div 256, X rem 256, 1984}
             || X <- lists:seq(1, 200)],
    [spawn(fun() -> arweave_client_throttling:throttle(P, ?PATH) end)
     || P <- Peers],
    ok = wait_peer_count(?GROUP, 200),
    [{arweave_client_throttling_peers, gauge, _Help, MetricsList}] =
        ?M:metrics(),
    ?assertEqual([{[{group_id, ?GROUP}], 200}], MetricsList),
    ok.

%% Helpers

wait_peer_count(GroupId, N) ->
    wait_until(fun() ->
                       case arweave_client_throttling_group:info(GroupId) of
                           #{peers := N} -> true;
                           _ -> false
                       end
               end, 200).

wait_until(Fun, 0) ->
    case Fun() of
        true -> ok;
        _ -> {error, timeout}
    end;
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ ->
            timer:sleep(20),
            wait_until(Fun, N - 1)
    end.
