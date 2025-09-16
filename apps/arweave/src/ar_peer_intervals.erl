-module(ar_peer_intervals).

-export([fetch/3]).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_data_discovery.hrl").

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
-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).

%% The number of the release adding support for the
%% GET /data_sync_record/[start]/[end]/[limit] endpoint.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 83).

%%%===================================================================
%%% Public interface.
%%%===================================================================

fetch(Start, End, StoreID) when Start >= End ->
	?LOG_DEBUG([{event, fetch_peer_intervals_end},
			{store_id, StoreID},
			{range_start, Start},
			{range_end, End}]),
	gen_server:cast(ar_data_sync:name(StoreID), {collect_peer_intervals, Start, End});
fetch(Start, End, StoreID) ->
	Parent = ar_data_sync:name(StoreID),
	spawn_link(fun() ->
		try
			End2 = min(Start + ?QUERY_RANGE_STEP_SIZE, End),
			UnsyncedIntervals = get_unsynced_intervals(Start, End2, StoreID),

			Bucket = Start div ?NETWORK_DATA_BUCKET_SIZE,
			AllPeers =
			case arweave_config:get(sync_from_local_peers_only) of
					true ->
						arweave_config:get(local_peers);
					false ->
						ar_data_discovery:get_bucket_peers(Bucket)
				end,
			HotPeers = [
				Peer || Peer <- AllPeers,
				not ar_rate_limiter:is_on_cooldown(Peer, ?GET_SYNC_RECORD_RPM_KEY) andalso
				not ar_rate_limiter:is_throttled(Peer, ?GET_SYNC_RECORD_PATH)
			],
			Peers = ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT),
			End3 =
				case ar_intervals:is_empty(UnsyncedIntervals) of
					true ->
						End2;
					false ->
						min(End2, fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals))
				end,
			%% Schedule the next sync bucket. The cast handler logic will pause collection
			%% if needed.
			gen_server:cast(Parent, {collect_peer_intervals, End3, End})
		catch
			Class:Reason ->
				?LOG_WARNING([{event, fetch_peers_process_exit},
						{store_id, StoreID},
						{range_start, Start},
						{range_end, End},
						{class, Class},
						{reason, Reason}]),
				gen_server:cast(Parent, {collect_peer_intervals, Start, End})
		end
	end).

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
	gen_server:cast(Parent, {enqueue_intervals, EnqueueIntervals}),
	MinRightBound.

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
