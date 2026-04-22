%% Directory-level tests for the peer interval cache and cursor-driven
%% prefetch. Exercises the `get_peer_intervals' / `get_peer_footprint_intervals'
%% + `advance_cursor' + `invalidate' API that replaced the old synchronous
%% fetch-and-cast flow in step 4 of the discovery refactor.
-module(ar_peer_intervals_discovery_test).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar_data_discovery.hrl").
-include("ar.hrl").

%% Helper to build a peer() tuple the ar_peers API accepts.
-define(PEER(N), {10, 0, 0, N, 1984}).

%% ---- cache-only reads (no workers, no prefetch) -----------------------

cache_miss_returns_cache_miss_test() ->
	ar_data_discovery:clear_interval_cache(),
	Peer = ?PEER(1),
	Mocks = [
		{ar_rate_limiter, is_on_cooldown, fun(_, _) -> false end}
	],
	ar_test_node:test_with_mocked_functions(Mocks, fun() ->
		?assertEqual({error, cache_miss},
			ar_data_discovery:get_peer_intervals(Peer, 0, infinity)),
		?assertEqual({error, cache_miss},
			ar_data_discovery:get_peer_footprint_intervals(Peer, 0, 0))
	end).

cooldown_shortcircuits_read_test() ->
	ar_data_discovery:clear_interval_cache(),
	Peer = ?PEER(2),
	Mocks = [
		{ar_rate_limiter, is_on_cooldown, fun(_, _) -> true end}
	],
	ar_test_node:test_with_mocked_functions(Mocks, fun() ->
		?assertEqual({error, cooldown},
			ar_data_discovery:get_peer_intervals(Peer, 0, infinity)),
		?assertEqual({error, cooldown},
			ar_data_discovery:get_peer_footprint_intervals(Peer, 0, 0))
	end).

%% ---- cursor-driven prefetch populates the cache -----------------------

prefetch_normal_populates_cache_test_() ->
	{timeout, 15, fun prefetch_normal_populates_cache/0}.

prefetch_normal_populates_cache() ->
	ar_data_discovery:clear_interval_cache(),
	Peer = ?PEER(3),
	StoreID = test_store_normal,
	Intervals = ar_intervals:from_list([
		{?QUERY_RANGE_STEP_SIZE div 2, 0}
	]),
	Mocks = [
		{ar_rate_limiter, is_on_cooldown, fun(_, _) -> false end},
		{ar_peers, get_peer_release, fun(_) -> ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE end},
		{ar_http_iface_client, get_sync_record, fun(_Peer, _Left, _Right, _Limit) ->
			{ok, Intervals}
		end},
		{?MODULE, get_bucket_peers, fun(_Bucket) -> [Peer] end}
	],
	%% Bypass the get_bucket_peers mock by writing directly to the ETS
	%% table the real function reads. (Mocking the directory from within
	%% itself is painful; fake the bucket membership instead.)
	ar_data_discovery:clear_interval_cache(),
	Bucket = 0,
	case ets:info(ar_data_discovery) of
		undefined -> ok;
		_ -> ets:insert(ar_data_discovery, {{Bucket, 1, Peer}})
	end,
	try
		ar_test_node:test_with_mocked_functions(lists:keydelete(?MODULE, 1, Mocks), fun() ->
			ar_data_discovery:advance_cursor(StoreID, 0, normal),
			%% Poll for cache population with a 10s deadline.
			ok = wait_for_cache_hit({Peer, 0, normal}, 10_000)
		end)
	after
		case ets:info(ar_data_discovery) of
			undefined -> ok;
			_ -> ets:delete(ar_data_discovery, {Bucket, 1, Peer})
		end
	end.

%% ---- invalidate drops cache rows -------------------------------------

invalidate_drops_row_test() ->
	ar_data_discovery:clear_interval_cache(),
	Peer = ?PEER(4),
	%% Seed a row directly via fetch_peer_intervals_http's mocked success
	%% path.
	Mocks = [
		{ar_rate_limiter, is_on_cooldown, fun(_, _) -> false end},
		{ar_peers, get_peer_release, fun(_) -> ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE end},
		{ar_http_iface_client, get_sync_record, fun(_Peer, _Left, _Right, _Limit) ->
			{ok, ar_intervals:from_list([{100, 0}])}
		end}
	],
	ar_test_node:test_with_mocked_functions(Mocks, fun() ->
		%% Trigger a prefetch that fills the cache for {Peer, 0, normal}.
		_ = ets:insert(ar_data_discovery, {{0, 1, Peer}}),
		ar_data_discovery:advance_cursor(test_store_invalidate, 0, normal),
		ok = wait_for_cache_hit({Peer, 0, normal}, 5_000),
		%% Now invalidate and confirm the row is gone.
		ar_data_discovery:invalidate(Peer, {?QUERY_RANGE_STEP_SIZE, 0}),
		ok = wait_for_cache_miss({Peer, 0, normal}, 2_000),
		_ = ets:delete(ar_data_discovery, {0, 1, Peer})
	end).

%% ---- helpers ---------------------------------------------------------

wait_for_cache_hit(Key, TimeoutMs) ->
	wait_for(fun() ->
		case ets:lookup(ar_data_discovery_intervals, Key) of
			[_] -> true;
			[] -> false
		end
	end, TimeoutMs).

wait_for_cache_miss(Key, TimeoutMs) ->
	wait_for(fun() ->
		case ets:lookup(ar_data_discovery_intervals, Key) of
			[] -> true;
			_ -> false
		end
	end, TimeoutMs).

wait_for(Pred, TimeoutMs) ->
	Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
	wait_loop(Pred, Deadline).

wait_loop(Pred, Deadline) ->
	case Pred() of
		true -> ok;
		false ->
			case erlang:monotonic_time(millisecond) >= Deadline of
				true -> ?assert(false, "Timed out waiting for predicate");
				false ->
					timer:sleep(50),
					wait_loop(Pred, Deadline)
			end
	end.
