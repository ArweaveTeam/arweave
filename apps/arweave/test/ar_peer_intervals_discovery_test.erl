-module(ar_peer_intervals_discovery_test).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").
-include("ar_data_discovery.hrl").

-include("ar.hrl").

no_unsynced_intervals_test_() ->
	TestCase = #{
		synced => [{0, 10}],
		peer1 => [{0, 5}],
		peer2 => [{3, 8}]
	},
	test_interval_discovery(TestCase, footprint, "No unsynced intervals").

basic_interval_discovery_test_() ->
	TestCase = #{
		synced => [],
		peer1 => [{0, 3}]
	},
	test_interval_discovery(TestCase, footprint, "Three chunks").

overlapping_intervals_test_() ->
	TestCase = #{
		synced => [{8, 12}],
		peer1 => [{0, 1}],
		peer2 => [{3, 10}],
		peer3 => [{6, 13}]
	},
	test_interval_discovery(TestCase, footprint, "Overlapping intervals").

test_interval_discovery(TestCase, Mode, Title) ->
	SyncedChunks = maps:get(synced, TestCase, []),
	PeerChunksData = maps:remove(synced, TestCase),

	%% Convert to bytes
	SyncedBytes = chunks_to_bytes(SyncedChunks),
	PeerBytesData = maps:map(fun(_K, V) -> chunks_to_bytes(V) end, PeerChunksData),

	TestRangeEnd = 50 * ?DATA_CHUNK_SIZE,
	UnsyncedBytes = calculate_unsynced_from_synced(SyncedBytes, TestRangeEnd),

	Peers = maps:keys(PeerBytesData),

	ExpectedIntervals = calculate_expected_intervals(UnsyncedBytes, PeerBytesData),

	Mocks = create_test_mocks(Peers),

	TestConfig = #config{ sync_from_local_peers_only = false, local_peers = [] },
	arweave_config:set_env(TestConfig),

	setup_sync_record_servers(SyncedBytes, PeerBytesData),

	ar_test_node:test_with_mocked_functions(Mocks, fun() ->
		Start = 0,
		End = TestRangeEnd,
		StoreID = test_store_id,

		ar_peer_intervals:fetch(Start, Start, End, StoreID, Mode),

		%% Verify we get the expected intervals enqueued
		case maps:size(ExpectedIntervals) == 0 of
			true ->
				receive
					{'$gen_cast', {enqueue_intervals, []}} -> ok
				after 100 -> ok
				end;
			false ->
				AllEnqueueIntervals = collect_enqueue_intervals(#{}, StoreID, Mode),

				FlattenedIntervals = lists:flatten(AllEnqueueIntervals),
				verify_enqueued_intervals(FlattenedIntervals, ExpectedIntervals, Title)
		end
	end).

collect_enqueue_intervals(Acc, StoreID, Mode) ->
	receive
		{'$gen_cast', {enqueue_intervals, EnqueueIntervals}} ->
			Acc2 = update_peer_intervals(EnqueueIntervals, Acc),
			collect_enqueue_intervals(Acc2, StoreID, Mode);
		{'$gen_cast', {collect_peer_intervals, Offset, _Start, End, _}} when Offset >= End ->
			maps:to_list(Acc);
		{'$gen_cast', {collect_peer_intervals, Offset, Start, End, _}} ->
			ar_peer_intervals:fetch(Offset, Start, End, StoreID, Mode),
			collect_enqueue_intervals(Acc, StoreID, Mode)
	after 10_000 ->
		?assert(false, "No enqueue_intervals messages received")
	end.

update_peer_intervals([], Acc) ->
	Acc;
update_peer_intervals([{Peer, Intervals, _FootprintKey} | Rest], Acc) ->
	PeerIntervals = maps:get(Peer, Acc, ar_intervals:new()),
	PeerIntervals2 = ar_intervals:union(PeerIntervals, Intervals),
	update_peer_intervals(Rest, maps:put(Peer, PeerIntervals2, Acc)).

create_test_mocks(Peers) ->
	[
		{ar_data_discovery, get_footprint_bucket_peers, fun(_FootprintBucket) -> Peers end},
		{ar_tx_blacklist, get_blacklisted_intervals, fun(_Start, _End) -> ar_intervals:new() end},
		{ar_http_iface_client, get_footprints, fun(Peer, Partition, Footprint) ->
			Intervals = ar_footprint_record:get_intervals(Partition, Footprint, Peer),
			{ok, Intervals}
		end},
		{ar_data_sync, name, fun(_StoreID) -> self() end},
		{ar_peers, get_peer_release, fun(_Peer) -> ?GET_FOOTPRINT_RECORD_SUPPORT_RELEASE end},
		{ar_rate_limiter, is_on_cooldown, fun(_Peer, _Key) -> false end},
		{ar_rate_limiter, is_throttled, fun(_Peer, _Path) -> false end}
	].

verify_enqueued_intervals(EnqueueIntervals, ExpectedIntervals, Title) ->
	?assert(is_list(EnqueueIntervals)),

	EnqueuedByPeer = maps:from_list(EnqueueIntervals),

	%% Verify each expected peer has intervals
	maps:fold(fun(Peer, ExpectedPeerIntervals, _) ->
		?assert(maps:is_key(Peer, EnqueuedByPeer),
			lists:flatten(
				io_lib:format("Expected peer ~p not found in enqueued intervals", [Peer]))),

		ActualPeerIntervals = maps:get(Peer, EnqueuedByPeer),

		?assertEqual(ar_intervals:to_list(ExpectedPeerIntervals), ar_intervals:to_list(ActualPeerIntervals), Title)
	end, ok, ExpectedIntervals).

chunks_to_bytes(ChunkIntervals) ->
	lists:map(fun({Start, End}) ->
		StartBytes = trunc(Start * ?DATA_CHUNK_SIZE),
		EndBytes = trunc(End * ?DATA_CHUNK_SIZE),
		{EndBytes, StartBytes}
	end, ChunkIntervals).

%% Calculate unsynced intervals as gaps in synced intervals within the test range
calculate_unsynced_from_synced(SyncedBytes, TestRangeEnd) ->
	case SyncedBytes of
		[] ->
			%% Nothing synced, everything is unsynced
			[{TestRangeEnd, 0}];
		_ ->
			%% Find gaps in synced intervals
			SyncedIntervals = ar_intervals:from_list(SyncedBytes),
			TestRange = ar_intervals:from_list([{TestRangeEnd, 0}]),
			UnsyncedIntervals = ar_intervals:outerjoin(SyncedIntervals, TestRange),
			ar_intervals:to_list(UnsyncedIntervals)
	end.

calculate_expected_intervals(UnsyncedBytes, PeerBytesData) ->
	%% For each peer, calculate intersection with unsynced intervals
	UnsyncedIntervals = ar_intervals:from_list(UnsyncedBytes),
	ExpectedByPeer = maps:map(fun(_Peer, PeerIntervals) ->
		PeerIntervalsObj = ar_intervals:from_list(PeerIntervals),
		Intersection = ar_intervals:intersection(UnsyncedIntervals, PeerIntervalsObj),
		Intersection
	end, PeerBytesData),
	maps:filter(fun(_Peer, Intervals) -> not ar_intervals:is_empty(Intervals) end, ExpectedByPeer).

setup_sync_record_servers(SyncedBytes, PeerBytesData) ->
	case ets:info(sync_records) of
		undefined ->
			ets:new(sync_records, [named_table, public, {read_concurrency, true}]);
		_ ->
			ets:delete_all_objects(sync_records)
	end,

	Packing = unpacked,
	SyncedStoreID = test_store_id,
	ProcessName = ar_sync_record:name(SyncedStoreID),
	case whereis(ProcessName) of
		undefined -> ar_sync_record:start_link(ProcessName, SyncedStoreID);
		_ -> ok
	end,
	add_bytes_to_footprint(SyncedBytes, Packing, SyncedStoreID),

	maps:foreach(
		fun(Peer, PeerBytes) ->
			PeerProcessName = ar_sync_record:name(Peer),
			case whereis(PeerProcessName) of
				undefined -> ar_sync_record:start_link(PeerProcessName, Peer);
				_ -> ok
			end,
			add_bytes_to_footprint(PeerBytes, Packing, Peer)
		end,
		PeerBytesData
	).

add_bytes_to_footprint([], _Packing, _StoreID) -> ok;
add_bytes_to_footprint([{End, Start} | Rest], Packing, StoreID) ->
	lists:foreach(
		fun(Offset) ->
			ar_footprint_record:add(Offset, Packing, StoreID) end,
		lists:seq(Start + ?DATA_CHUNK_SIZE, End, ?DATA_CHUNK_SIZE)
	),
	add_bytes_to_footprint(Rest, Packing, StoreID).