-module(ar_peer_intervals).

-export([fetch/4, do_fetch/4]).

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_data_discovery.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% The size of the span of the weave we search at a time.
%% By searching we mean asking peers about the intervals they have in the given span
%% and finding the intersection with the unsynced intervals.
-ifdef(AR_TEST).
-define(QUERY_RANGE_STEP_SIZE, 10_000_000). % 10 MB
-else.
-define(QUERY_RANGE_STEP_SIZE, 1_000_000_000). % 1 GB
-endif.

%% Fetch at most this many sync intervals from a peer at a time.
-ifdef(AR_TEST).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

%% The number of peers to fetch sync intervals from in parallel at a time.
-define(GET_SYNC_RECORD_BATCH_SIZE, 2).
-define(GET_SYNC_RECORD_COOLDOWN_MS, 60 * 1000).
-define(GET_SYNC_RECORD_RPM_KEY, data_sync_record).
-define(GET_FOOTPRINT_RECORD_RPM_KEY, footprints).
-define(GET_FOOTPRINT_RECORD_COOLDOWN_MS, 60 * 1000).
-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).
-define(GET_FOOTPRINT_RECORD_PATH, [<<"footprints">>]).

%% The number of the release adding support for the
%% GET /data_sync_record/[start]/[end]/[limit] endpoint.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 83).

%% The number of the release adding support for the
%% GET /footprints/[partition]/[footprint] endpoint.
-define(GET_FOOTPRINT_RECORD_SUPPORT_RELEASE, 87).

%%%===================================================================
%%% Public interface.
%%%===================================================================

fetch(Start, End, StoreID, Type) when Start >= End ->
	?LOG_DEBUG([{event, fetch_peer_intervals_end},
			{store_id, StoreID},
			{range_start, Start},
			{range_end, End},
			{type, Type}]),
	gen_server:cast(ar_data_sync:name(StoreID), {collect_peer_intervals, Start, End, Type});
fetch(Start, End, StoreID, Type) ->
	Parent = ar_data_sync:name(StoreID),
	spawn_link(fun() ->
		{End2, EnqueueIntervals} = do_fetch(Start, End, StoreID, Type),
		gen_server:cast(Parent, {enqueue_intervals, EnqueueIntervals}),
		gen_server:cast(Parent, {collect_peer_intervals, End2, End, Type})
	end).

do_fetch(Start, End, StoreID, normal) ->
	Parent = ar_data_sync:name(StoreID),
	try
		End2 = min(Start + ?QUERY_RANGE_STEP_SIZE, End),
		UnsyncedIntervals = get_unsynced_intervals(Start, End2, StoreID),

		Bucket = Start div ?NETWORK_DATA_BUCKET_SIZE,
		{ok, Config} = arweave_config:get_env(),
		AllPeers =
			case Config#config.sync_from_local_peers_only of
				true ->
					Config#config.local_peers;
				false ->
					ar_data_discovery:get_bucket_peers(Bucket)
			end,
		HotPeers = [
			Peer || Peer <- AllPeers,
			not ar_rate_limiter:is_on_cooldown(Peer, ?GET_SYNC_RECORD_RPM_KEY) andalso
			not ar_rate_limiter:is_throttled(Peer, ?GET_SYNC_RECORD_PATH)
		],
		Peers = ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT),

		{End4, EnqueueIntervals} =
			case ar_intervals:is_empty(UnsyncedIntervals) of
				true ->
					{End2, []};
				false ->
					{End3, EnqueueIntervals2} = fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals),
					{min(End2, End3), EnqueueIntervals2}
			end,
		%% Schedule the next sync bucket. The cast handler logic will pause collection
		%% if needed.
		{End4, EnqueueIntervals}
	catch
		Class:Reason:Stacktrace ->
			?LOG_WARNING([{event, fetch_peers_process_exit},
					{store_id, StoreID},
					{range_start, Start},
					{range_end, End},
					{type, normal},
					{class, Class},
					{reason, Reason},
					{stacktrace, Stacktrace}]),
			{Start, []}
	end;

do_fetch(Start, End, StoreID, footprint) ->
	Parent = ar_data_sync:name(StoreID),
	try
		Partition = ar_replica_2_9:get_entropy_partition(Start + ?DATA_CHUNK_SIZE),
		Footprint = ar_footprint_record:get_footprint(Start + ?DATA_CHUNK_SIZE),

		FootprintBucket = ar_footprint_record:get_footprint_bucket(Start + ?DATA_CHUNK_SIZE),
		{ok, Config} = arweave_config:get_env(),
		AllPeers =
			case Config#config.sync_from_local_peers_only of
				true ->
					Config#config.local_peers;
				false ->
					ar_data_discovery:get_footprint_bucket_peers(FootprintBucket)
			end,
		HotPeers = [
			Peer || Peer <- AllPeers,
			not ar_rate_limiter:is_on_cooldown(Peer, ?GET_FOOTPRINT_RECORD_RPM_KEY) andalso
			not ar_rate_limiter:is_throttled(Peer, ?GET_FOOTPRINT_RECORD_PATH)
		],
		Peers = ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT),

		UnsyncedIntervals = ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID),

		EnqueueIntervals =
			case ar_intervals:is_empty(UnsyncedIntervals) of
				true ->
					[];
				false ->
					fetch_peer_footprint_intervals(Parent, Partition, Footprint, Start, End, Peers, UnsyncedIntervals)
			end,
		Partition = ar_replica_2_9:get_entropy_partition(Start + ?DATA_CHUNK_SIZE),
		{_PartitionStart, PartitionEnd} = ar_replica_2_9:get_entropy_partition_range(Partition),
		Start2 =
			case Start + 2 * ?DATA_CHUNK_SIZE > PartitionEnd of
				true ->
					PartitionEnd;
				false ->
					Start + ?DATA_CHUNK_SIZE
			end,
		Start3 = min(Start2, End),
		%% Schedule the next sync bucket. The cast handler logic will pause collection if needed.
		{Start3, EnqueueIntervals}
	catch
		Class:Reason:Stacktrace ->
			?LOG_WARNING([{event, fetch_footprint_intervals_process_exit},
					{store_id, StoreID},
					{range_start, Start},
					{range_end, End},
					{type, footprint},
					{class, Class},
					{reason, Reason},
					{stacktrace, Stacktrace}]),
			{Start, []}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Collect the unsynced intervals between Start and End excluding the blocklisted
%% intervals.
get_unsynced_intervals(Start, End, StoreID) ->
	UnsyncedIntervals = get_unsynced_intervals(Start, End, ar_intervals:new(), StoreID),
	BlacklistedIntervals = ar_tx_blacklist:get_blacklisted_intervals(Start, End),
	ar_intervals:outerjoin(BlacklistedIntervals, UnsyncedIntervals).

get_unsynced_intervals(Start, End, Intervals, _StoreID) when Start >= End ->
	Intervals;
get_unsynced_intervals(Start, End, Intervals, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ar_data_sync, StoreID) of
		not_found ->
			ar_intervals:add(Intervals, End, Start);
		{End2, Start2} ->
			case Start2 > Start of
				true ->
					End3 = min(Start2, End),
					get_unsynced_intervals(End2, End,
							ar_intervals:add(Intervals, End3, Start), StoreID);
				_ ->
					get_unsynced_intervals(End2, End, Intervals, StoreID)
			end
	end.

fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals) ->
	Intervals =
		ar_util:batch_pmap(
			fun(Peer) ->
				case maybe_get_peer_intervals(Peer, Start, UnsyncedIntervals) of
					{ok, SoughtIntervals, PeerRightBound} ->
						{Peer, SoughtIntervals, PeerRightBound};
					{error, cooldown} ->
						%% Skipping peer because we hit a 429 and put it on cooldown.
						ok;
					{error, Reason} ->
						ar_http_iface_client:log_failed_request(Reason, [{event, failed_to_fetch_peer_intervals},
							{parent, Parent},
							{peer, ar_util:format_peer(Peer)},
							{reason, io_lib:format("~p", [Reason])}]),
						ok
				end
			end,
			Peers,
			?GET_SYNC_RECORD_BATCH_SIZE, % fetch sync intervals from so many peers at a time
			%% We'll rely on the timeout to also flag when we are approaching a peer's RPM
			%% limit. As we approach the limit we will self-throttle the requests. Eventually this
			%% throttling will exceed 60s and we'll timout the batch_pmap and flag the peer for
			%% cooldown.
			60 * 1000 
		),
	{EnqueueIntervals, MinRightBound} =
		lists:foldl(
			fun	({error, batch_pmap_timeout, Peer}, Acc) ->
					?LOG_DEBUG([{event, failed_to_fetch_peer_intervals},
						{parent, Parent},
						{peer, ar_util:format_peer(Peer)},
						{reason, batch_pmap_timeout}]),
					ar_rate_limiter:set_cooldown(
						Peer, ?GET_SYNC_RECORD_RPM_KEY, ?GET_SYNC_RECORD_COOLDOWN_MS),
					Acc;
				({Peer, SoughtIntervals, RightBound}, {IntervalsAcc, RightBoundAcc}) ->
					case ar_intervals:is_empty(SoughtIntervals) of
						true ->
							{IntervalsAcc, RightBoundAcc};
						false ->
							{[{Peer, SoughtIntervals} | IntervalsAcc],
								min(RightBound, RightBoundAcc)}
					end;
				(ok, Acc) ->
					Acc;
				(Error, Acc) ->
					ar_http_iface_client:log_failed_request(Error, [{event, failed_to_fetch_peer_intervals},
						{parent, Parent},
						{peer, unknown},
						{reason, io_lib:format("~p", [Error])}]),
					Acc
			end,
			{[], infinity},
			Intervals
		),
	{MinRightBound, EnqueueIntervals}.

%% @doc
%% @return {ok, Intervals, PeerRightBound} | Error
%% Intervals: the intersection of the intervals we are looking for and the intervals that
%%				the peer advertised inside the recently queried range
%% PeerRightBound: the right bound of the intervals the peer advertised; for example,
%%				we may ask for at most 100 continuous intervals inside the given gigabyte,
%%				but the peer may have this region very fractured and 100 intervals will
%%				not be all intervals covering this gigabyte, so we take the right bound
%%				to know where to query next
maybe_get_peer_intervals(Peer, Left, SoughtIntervals) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_SYNC_RECORD_RPM_KEY) of
		true ->
			{error, cooldown};
		false ->
			get_peer_intervals(Peer, Left, SoughtIntervals)
	end.

get_peer_intervals(Peer, Left, SoughtIntervals) ->
	Limit = ?QUERY_SYNC_INTERVALS_COUNT_LIMIT,
	Right = element(1, ar_intervals:largest(SoughtIntervals)),
	PeerReply =
		case ar_peers:get_peer_release(Peer) >= ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE of
			true ->
				ar_http_iface_client:get_sync_record(Peer, Left + 1, Right, Limit);
			false ->
				ar_http_iface_client:get_sync_record(Peer, Left + 1, Limit)
		end,
	case PeerReply of
		{ok, PeerIntervals2} ->
			PeerRightBound =
				case ar_intervals:is_empty(PeerIntervals2) of
					true ->
						infinity;
					false ->
						element(1, ar_intervals:largest(PeerIntervals2))
				end,
			{ok, ar_intervals:intersection(PeerIntervals2, SoughtIntervals), PeerRightBound};
		{error, too_many_requests} = Error ->
			ar_rate_limiter:set_cooldown(Peer,
				?GET_SYNC_RECORD_RPM_KEY, ?GET_SYNC_RECORD_COOLDOWN_MS),
			Error;
		Error ->
			Error
	end.

fetch_peer_footprint_intervals(Parent, Partition, Footprint, Start, End, Peers, UnsyncedIntervals) ->
	Intervals =
		ar_util:batch_pmap(
			fun(Peer) ->
				case maybe_get_peer_footprint_intervals(Peer, Partition, Footprint, UnsyncedIntervals) of
					{ok, SoughtIntervals} ->
						{Peer, SoughtIntervals};
					{error, cooldown} ->
						%% Skipping peer because we hit a 429 and put it on cooldown.
						ok;
					{error, Reason} ->
						?LOG_DEBUG([{event, failed_to_fetch_peer_footprint_intervals},
							{parent, Parent},
							{peer, ar_util:format_peer(Peer)},
							{reason, io_lib:format("~p", [Reason])}]),
						ok
				end
			end,
			Peers,
			?GET_SYNC_RECORD_BATCH_SIZE, % fetch sync intervals from so many peers at a time
			%% We'll rely on the timeout to also flag when we are approaching a peer's RPM
			%% limit. As we approach the limit we will self-throttle the requests. Eventually this
			%% throttling will exceed 60s and we'll timout the batch_pmap and flag the peer for
			%% cooldown.
			60 * 1000
		),
	EnqueueIntervals =
		lists:foldl(
			fun	({error, batch_pmap_timeout, Peer}, Acc) ->
					?LOG_DEBUG([{event, failed_to_fetch_peer_footprint_intervals},
						{parent, Parent},
						{peer, ar_util:format_peer(Peer)},
						{reason, batch_pmap_timeout}]),
					ar_rate_limiter:set_cooldown(
						Peer, ?GET_FOOTPRINT_RECORD_RPM_KEY, ?GET_FOOTPRINT_RECORD_COOLDOWN_MS),
					Acc;
				({Peer, SoughtIntervals}, IntervalsAcc) ->
					case ar_intervals:is_empty(SoughtIntervals) of
						true ->
							IntervalsAcc;
						false ->
							ByteIntervals =
								ar_footprint_record:get_intervals_from_footprint_intervals(SoughtIntervals),
							%% SoughtIntervals may include intervals beyond
							%% the storage module boundaries.
							%% This is because during the
							%% UnsyncedIntervals -> SoughtIntervals process we
							%% end up querying all advertised intervals belonging
							%% to a footprint that interseects UnsyncedIntervals.
							%% This can cause this node to try to
							%% store a chunk that lies beyond its configured
							%% storage module range. To avoid this we explicitly remove
							%% all intervals beyond the provided boundaries.
							ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
							PaddedStart =
								case ar_block:get_chunk_padded_offset(Start) of
									Start ->
										Start;
									Offset ->
										Offset - ?DATA_CHUNK_SIZE
								end,
							ByteIntervals3 = ar_intervals:outerjoin(ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2),
							[{Peer, ByteIntervals3} | IntervalsAcc]
					end;
				(ok, Acc) ->
					Acc;
				(Error, Acc) ->
					?LOG_DEBUG([{event, failed_to_fetch_peer_footprint_intervals},
						{parent, Parent},
						{peer, unknown},
						{reason, io_lib:format("~p", [Error])}]),
					Acc
			end,
			[],
			Intervals
		),
	EnqueueIntervals.

maybe_get_peer_footprint_intervals(Peer, Partition, Footprint, SoughtIntervals) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_FOOTPRINT_RECORD_RPM_KEY) of
		true ->
			{error, cooldown};
		false ->
			get_peer_footprint_intervals(Peer, Partition, Footprint, SoughtIntervals)
	end.

get_peer_footprint_intervals(Peer, Partition, Footprint, SoughtIntervals) ->
	PeerReply =
		case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_RECORD_SUPPORT_RELEASE of
			true ->
				ar_http_iface_client:get_footprints(Peer, Partition, Footprint);
			false ->
				%% We expect to get here only if the peer is upgraded and then downgraded again,
				%% because we check the peer release at the bucket collection stage.
				not_found
		end,
	case PeerReply of
		{ok, Intervals} ->
			{ok, ar_intervals:intersection(Intervals, SoughtIntervals)};
		not_found ->
			{ok, ar_intervals:new()};
		{error, too_many_requests} = Error ->
			ar_rate_limiter:set_cooldown(Peer,
				?GET_FOOTPRINT_RECORD_RPM_KEY, ?GET_FOOTPRINT_RECORD_COOLDOWN_MS),
			Error;
		Error ->
			Error
	end.

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(AR_TEST).

test_no_unsynced_intervals_test_() ->
	TestCase = #{
		synced => [{0, 10}],
		peer1 => [{0, 5}],
		peer2 => [{3, 8}]
	},
	test_interval_discovery(TestCase, footprint, "No unsynced intervals").

test_basic_interval_discovery_test_() ->
	TestCase = #{
		synced => [],
		peer1 => [{0, 3}]
	},
	test_interval_discovery(TestCase, footprint, "Three chunks").

test_overlapping_intervals_test_() ->
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

		fetch(Start, End, StoreID, Mode),

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
		{'$gen_cast', {collect_peer_intervals, Start, End, _}} when Start >= End ->
			maps:to_list(Acc);
		{'$gen_cast', {collect_peer_intervals, Start, End, _}} ->
			fetch(Start, End, StoreID, Mode),
			collect_enqueue_intervals(Acc, StoreID, Mode)
	after 10_000 ->
		?assert(false, "No enqueue_intervals messages received")
	end.

update_peer_intervals([], Acc) ->
	Acc;
update_peer_intervals([{Peer, Intervals} | Rest], Acc) ->
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
		{ar_peers, get_peer_release, fun(_Peer) -> 87 end}
	].

verify_enqueued_intervals(EnqueueIntervals, ExpectedIntervals, Title) ->
	?assert(is_list(EnqueueIntervals)),

	EnqueuedByPeer = maps:from_list(EnqueueIntervals),

	%% Verify each expected peer has intervals
	maps:fold(fun(Peer, ExpectedPeerIntervals, _) ->
		?assert(maps:is_key(Peer, EnqueuedByPeer),
			io_lib:format("Expected peer ~p not found in enqueued intervals", [Peer])),

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

-endif.