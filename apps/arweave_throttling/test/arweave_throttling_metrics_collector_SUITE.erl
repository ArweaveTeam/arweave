%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Tests for `arweave_throttling_metrics_collector'.
%%%
%%% The collector exposes a single `arweave_throttling_peers'
%%% gauge metric family. Each test case starts the application with
%%% a single group whose `initial_remaining' is 0 so that every
%%% `throttle/2' call is queued (and therefore registers the peer
%%% in the group's peers map), then asserts how many peers the
%%% collector reports.
%%% @end
%%%===================================================================
-module(arweave_throttling_metrics_collector_SUITE).
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

-define(M, arweave_throttling_metrics_collector).
-define(GROUP, general).
-define(PATH, "some/path/that/lead/to/general").

-define(POLICIES, #{id => atom_to_list(?GROUP),
			concurrency => #{limit => 500},
			sliding_window => #{limit => 0,
						window_seconds => 1},
		        leaky_bucket   => #{burst => 450,
						tick_ms => 30000,
						tick_reduction => 450}}).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
	{description, "arweave_throttling_metrics_collector"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],

	ct:pal(info, 1, "start arweave_throttling"),
	ok = arweave_throttling:start(),

	Groups = [block_index,
			chunk,
			data_sync_record,
			general,
			get_previous_vdf_session,
			get_vdf,
			get_vdf_session,
			recent_hash_list_diff,
			wallet_list],

	ok = lists:foreach(fun arweave_throttling_sup:start_throttling_group/1, Groups),
   
	timer:sleep(1000),

	[{apps_before,AppsBefore},
	{config, Config}].

end_per_testcase(_TestCase, Config) ->
	%% `reset/1' sends `{request_ready, _}' to every queued waiter,
	%% so the test-spawned callers return from `throttle/2' and
	%% exit cleanly without leaking.
	_ = catch arweave_throttling:reset(?GROUP),
	ok = arweave_throttling:stop(),

	arweave_throttling_metrics:cleanup(),

	AppsBefore = proplists:get_value(apps_before, Config),
	AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
	AppsStartedForTest = AppsNow -- AppsBefore,
	lists:foreach(fun application:stop/1, AppsStartedForTest),
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
	[{arweave_throttling_peers, gauge, _Help, MetricsListP},
	{arweave_throttling_queued_requests, gauge, _Help2, MetricsListQ}] =
		lists:sort(?M:metrics()),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],0},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListP)),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],0},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListQ)),
	ok.

%% @doc After a single `throttle/2' call against a single peer the
%% collector reports exactly one peer for the group.
one_peer_reported(_Config) ->
	Peer = {127, 0, 0, 1, 1984},
	Headers = arweave_limiter_http_headers:to_http_headers(
				{register, leaky,
				#{expiring_limit => 450,
				remaining      => 450, %% This isn't really lifelike here
				reset_seconds  => 0,
				policies => ?POLICIES}
				}),
	ct:pal("path (~p) maps to: ~p group (pathkey: ~p)", [?PATH,
														arweave_throttling_path:path_to_group_id(Peer, ?PATH),
														arweave_throttling_path:path_to_path_key(?PATH)]),

	ok = arweave_throttling:update_quota(Peer, ?PATH, Headers),
	timer:sleep(100),
	ct:pal("path (~p) maps to: ~p group - after (pathkey:~p)", 
		[?PATH, arweave_throttling_path:path_to_group_id(Peer, ?PATH),
		        arweave_throttling_path:path_to_path_key(?PATH)]),
	_ = spawn(fun() ->
			arweave_throttling:throttle(Peer, ?PATH)
			end),
	ok = wait_peer_count(?GROUP, 1),
	[{arweave_throttling_peers, gauge, _Help, MetricsListP},
	{arweave_throttling_queued_requests, gauge, _Help2, MetricsListQ}] =
		lists:sort(?M:metrics()),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],1},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListP)),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],0},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListQ)),
	ok.


%% @doc Two hundred distinct peers must all show up in the gauge.
%% This case also exercises the queue under load: every spawned
%% caller stays parked on `{request_ready, _}' until
%% `end_per_testcase' resets the group.
two_hundred_peers_reported(_Config) ->
	Headers = arweave_limiter_http_headers:to_http_headers(
				{register, leaky,
				#{expiring_limit => 450,
				remaining      => 450, %% This isn't really valid here
				reset_seconds  => 0,
				policies => ?POLICIES}
				}),
	Peers = [{10, 0, X div 256, X rem 256, 1984}
			|| X <- lists:seq(1, 200)],
	[spawn(fun() ->
				arweave_throttling:update_quota(P, ?PATH, Headers),
				arweave_throttling:throttle(P, ?PATH)
		end)
	|| P <- Peers],
	ok = wait_peer_count(?GROUP, 200),
	[{arweave_throttling_peers, gauge, _Help, MetricsListP},
	{arweave_throttling_queued_requests, gauge, _Help2, MetricsListQ}] =
		?M:metrics(),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],200},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListP)),
	?assertEqual([{[{group_id,block_index}],0},
				{[{group_id,chunk}],0},
				{[{group_id,data_sync_record}],0},
				{[{group_id,general}],0},
				{[{group_id,get_previous_vdf_session}],0},
				{[{group_id,get_vdf}],0},
				{[{group_id,get_vdf_session}],0},
				{[{group_id,recent_hash_list_diff}],0},
				{[{group_id,wallet_list}],0}], lists:sort(MetricsListQ)),
	ok.

%% Helpers
wait_peer_count(GroupId, N) ->
	wait_until(fun() ->
			case arweave_throttling_group:info(GroupId) of
				#{peers := N} -> true;
				#{peers := _M} -> false
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
