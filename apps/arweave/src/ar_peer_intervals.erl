-module(ar_peer_intervals).

-export([fetch/5]).

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
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

%%%===================================================================
%%% Public interface.
%%%===================================================================

fetch(Offset, Start, End, StoreID, Type) when Offset >= End ->
	?LOG_DEBUG([{event, fetch_peer_intervals_end},
			{store_id, StoreID},
			{offset, Offset},
			{range_start, Start},
			{range_end, End},
			{type, Type}]),
	gen_server:cast(ar_data_sync:name(StoreID),
		{collect_peer_intervals, Offset, Start, End, Type});
fetch(Offset, Start, End, StoreID, Type) ->
	Parent = ar_data_sync:name(StoreID),
	spawn_link(fun() ->
		case do_fetch(Offset, Start, End, StoreID, Type) of
			{End2, EnqueueIntervals} ->
				gen_server:cast(Parent, {enqueue_intervals, EnqueueIntervals}),
				gen_server:cast(Parent, {collect_peer_intervals, End2, Start, End, Type});
			wait ->
				ar_util:cast_after(1000, Parent,
					{collect_peer_intervals, Offset, Start, End, Type})
		end
	end).

do_fetch(Offset, Start, End, StoreID, normal) ->
	Parent = ar_data_sync:name(StoreID),
	try
		case get_peers(Offset, normal) of
			wait ->
				wait;
			Peers ->
				End2 = min(Offset + ?QUERY_RANGE_STEP_SIZE, End),
				UnsyncedIntervals = get_unsynced_intervals(Offset, End2, StoreID),
				%% Schedule the next sync bucket. The cast handler logic will pause collection
				%% if needed.
				case ar_intervals:is_empty(UnsyncedIntervals) of
					true ->
						{End2, []};
					false ->
						{End3, EnqueueIntervals2} =
							fetch_peer_intervals(Parent, Offset, Peers, UnsyncedIntervals),
						{min(End2, End3), EnqueueIntervals2}
				end
		end
	catch
		Class:Reason:Stacktrace ->
			?LOG_WARNING([{event, fetch_peers_process_exit},
					{store_id, StoreID},
					{offset, Offset},
					{range_start, Start},
					{range_end, End},
					{type, normal},
					{class, Class},
					{reason, Reason},
					{stacktrace, Stacktrace}]),
			{Offset, []}
	end;

do_fetch(Offset, Start, End, StoreID, footprint) ->
	Parent = ar_data_sync:name(StoreID),
	try
		case get_peers(Offset, footprint) of
			wait ->
				wait;
			Peers ->
				Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
				Footprint = ar_footprint_record:get_footprint(Offset + ?DATA_CHUNK_SIZE),
				UnsyncedIntervals =
					ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID),

				EnqueueIntervals =
					case ar_intervals:is_empty(UnsyncedIntervals) of
						true ->
							[];
						false ->
							fetch_peer_footprint_intervals(
								Parent, Partition, Footprint, Offset, End, Peers, UnsyncedIntervals)
					end,
				Offset2 = get_next_fetch_offset(Offset, Start, End),
				%% Schedule the next sync bucket. The cast handler logic will pause collection if needed.
				{Offset2, EnqueueIntervals}
		end
	catch
		Class:Reason:Stacktrace ->
			?LOG_WARNING([{event, fetch_footprint_intervals_process_exit},
					{store_id, StoreID},
					{offset, Offset},
					{range_start, Start},
					{range_end, End},
					{type, footprint},
					{class, Class},
					{reason, Reason},
					{stacktrace, Stacktrace}]),
			{Offset, []}
	end.

%% @doc Calculate the next fetch start position after processing a sector.
%% Advances by one chunk within a sector, or jumps to the next partition boundary
%% when near the sector end.
get_next_fetch_offset(Offset, Start, End) ->
	SectorSize = ar_block:get_replica_2_9_entropy_sector_size(),
	Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
	{PartitionStart, PartitionEnd} = ar_replica_2_9:get_entropy_partition_range(Partition),
	SectorStart = max(Start, PartitionStart),
	SectorEnd = min(PartitionEnd, SectorStart + SectorSize),
	Offset2 =
		case Offset + 2 * ?DATA_CHUNK_SIZE > SectorEnd of
			true ->
				PartitionEnd;
			false ->
				Offset + ?DATA_CHUNK_SIZE
		end,
	min(Offset2, End).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_peers(Offset, normal) ->
	Bucket = Offset div ?NETWORK_DATA_BUCKET_SIZE,
	get_peers2(Bucket,
		fun(B) -> ar_data_discovery:get_bucket_peers(B) end,
		?GET_SYNC_RECORD_RPM_KEY,
		?GET_SYNC_RECORD_PATH);
get_peers(Offset, footprint) ->
	FootprintBucket = ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE),
	get_peers2(FootprintBucket,
		fun(B) -> ar_data_discovery:get_footprint_bucket_peers(B) end,
		?GET_FOOTPRINT_RECORD_RPM_KEY,
		?GET_FOOTPRINT_RECORD_PATH).

get_peers2(Bucket, GetPeersFun, RPMKey, Path) ->
	{ok, Config} = arweave_config:get_env(),
	AllPeers =
		case Config#config.sync_from_local_peers_only of
			true ->
				Config#config.local_peers;
			false ->
				GetPeersFun(Bucket)
		end,
	HotPeers = [
		Peer || Peer <- AllPeers,
		not ar_rate_limiter:is_on_cooldown(Peer, RPMKey) andalso
		not ar_rate_limiter:is_throttled(Peer, Path)
	],
	case length(AllPeers) > 0 andalso length(HotPeers) == 0 of
		true ->
			% There are peers for this Offset, but they are all on cooldown/throttled, so
			% we'll give them time to recover.
			wait;
		false ->
			ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT)
	end.

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
							%% FootprintKey = none for normal syncing
							{[{Peer, SoughtIntervals, none} | IntervalsAcc],
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
				case maybe_get_peer_footprint_intervals(
						Peer, Partition, Footprint, UnsyncedIntervals) of
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
								cut_peer_footprint_intervals(SoughtIntervals, Start, End),
							?LOG_DEBUG([{event, fetch_peer_intervals},
								{function, fetch_peer_footprint_intervals},
								{peer, ar_util:format_peer(Peer)},
								{partition, Partition},
								{footprint, Footprint},
								{unsynced_intervals, ar_intervals:sum(UnsyncedIntervals)},
								{sought_intervals, ar_intervals:sum(SoughtIntervals)},
								{intervals, length(Intervals)},
								{byte_intervals, ar_intervals:sum(ByteIntervals)}]),
							FootprintKey = {Partition, Footprint, Peer},
							[{Peer, ByteIntervals, FootprintKey} | IntervalsAcc]
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
		case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
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

%% @doc The intervals returned by a peer may include intervals beyond the
%% storage module boundaries. This is because we end up querying all advertised 
%% intervals belonging to a footprint that intersects this node's unsynced
%% intervals. This can cause this node to try to store a chunk that lies beyond
%% its configured storage module range. To avoid this we explicitly remove all
%% intervals beyond the provided boundaries.
cut_peer_footprint_intervals(FootprintIntervals, Start, End) -> 
	ByteIntervals =
		ar_footprint_record:get_intervals_from_footprint_intervals(FootprintIntervals),
	ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
	PaddedStart =
		case ar_block:get_chunk_padded_offset(Start) of
			Start ->
				Start;
			Offset ->
				Offset - ?DATA_CHUNK_SIZE
		end,
	ar_intervals:outerjoin(
		ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2).

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(AR_TEST).

cut_peer_footprint_intervals_test() ->

	?assertEqual(
		ar_intervals:from_list([{786432, 524288}, {1310720, 1048576}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{4, 0}]), 262144, 1572864),
		"Full Footprint 0, aligned boundaries"),

	?assertEqual(
		ar_intervals:from_list([{524288,262144}, {1048576,786432}, {1572864,1310720}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{8, 4}]), 262144, 1572864),
		"Full Footprint 1 cut to aligned boundaries"),

	?assertEqual(
		ar_intervals:from_list([
			{262144,200000}, {786432, 524288}, {1310720, 1048576}, {1600000,1572864}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{4, 0}]), 200000, 1600000),
		"Full Footprint 0, unaligned boundaries, pre-strict"),

	?assertEqual(
		ar_intervals:from_list([{524288,262144}, {1048576, 786432}, {1572864, 1310720}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{8, 4}]), 200000, 1600000),
		"Full Footprint 1, unaligned boundaries, pre-strict"),

	?assertEqual(
		ar_intervals:from_list([{2883584,2621440}, {3407872,3145728}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{12, 8}]), 2400000, 3500000),
		"Full Footprint 2, unaligned boundaries, post-strict"),
	
	?assertEqual(
		ar_intervals:from_list([{2621440,2359296}, {3145728,2883584}, {3500000,3407872}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{16, 12}]), 2400000, 3500000),
		"Full Footprint 3, unaligned boundaries, post-strict"),

	?assertEqual(
		ar_intervals:from_list([{2621440,2359296}, {3500000,3407872}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{16, 14}, {13, 12}]), 2400000, 3500000),
		"Partial Footprint 3, unaligned boundaries, post-strict"),

	ok.

%% Tests for get_next_fetch_offset/4
%% 4 binary conditions (shown as debug output 0/1 for each):
%%   1. Start > PartitionStart
%%   2. PartitionEnd > SectorStart + SectorSize
%%   3. Offset + 2*CHUNK > SectorEnd
%%   4. Offset2 > End
%% Pattern labeled 0-F in hex (e.g., 0101 = 5)
%% Note: 0xxx (cond1=0, cond2=0) requires partition < SectorSize, impossible in tests
get_next_fetch_offset_test() ->
	SectorSize = ar_block:get_replica_2_9_entropy_sector_size(),
	{P0Start, P0End} = ar_replica_2_9:get_entropy_partition_range(0),
	Chunk = ?DATA_CHUNK_SIZE,

	?assertEqual(P0Start + Chunk,
		get_next_fetch_offset(P0Start, P0Start, P0End),
		"simple advance"),

	?assertEqual(P0Start + 1000,
		get_next_fetch_offset(P0Start, P0Start, P0Start + 1000),
		"simple advance, limited by End"),

	?assertEqual(P0End,
		get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0End),
		"jump to PartitionEnd"),

	?assertEqual(P0Start + SectorSize,
		get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0Start + SectorSize),
		"jump to PartitionEnd, limited by End"),

	Start8 = P0End - SectorSize,
	?assertEqual(Start8 + Chunk,
		get_next_fetch_offset(Start8, Start8, P0End),
		"simple advance, mid-partition Start"),

	Start9 = P0End - SectorSize,
	?assertEqual(Start9 + 1000,
		get_next_fetch_offset(Start9, Start9, Start9 + 1000),
		"simple advance, mid-partition Start, limited by End"),

	StartA = P0End - SectorSize,
	?assertEqual(P0End,
		get_next_fetch_offset(StartA + Chunk, StartA, P0End),
		"jump to PartitionEnd, mid-partition Start"),

	StartB = P0End - SectorSize,
	SmallEndB = P0End - Chunk,
	?assertEqual(SmallEndB,
		get_next_fetch_offset(StartB + Chunk, StartB, SmallEndB),
		"jump to PartitionEnd, mid-partition Start, limited by End"),

	MidStart = P0Start + SectorSize,
	?assertEqual(MidStart + Chunk,
		get_next_fetch_offset(MidStart, MidStart, P0End),
		"simple advance, mid-partition Start, SectorEnd past PartitionEnd"),

	?assertEqual(MidStart + 1000,
		get_next_fetch_offset(MidStart, MidStart, MidStart + 1000),
		"simple advance, mid-partition Start, SectorEnd past PartitionEnd, limited by End"),

	?assertEqual(P0End,
		get_next_fetch_offset(MidStart + Chunk, MidStart, P0End),
		"jump to PartitionEnd, mid-partition Start, SectorEnd past PartitionEnd"),

	SmallEndF = MidStart + SectorSize,
	?assertEqual(SmallEndF,
		get_next_fetch_offset(MidStart + Chunk, MidStart, SmallEndF),
		"jump to PartitionEnd, mid-partition Start, SectorEnd past PartitionEnd, limited by End"),

	ok.

-endif.