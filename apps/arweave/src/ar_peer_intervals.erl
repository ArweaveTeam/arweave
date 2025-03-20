-module(ar_peer_intervals).

-export([fetch/3]).

-include("../include/ar.hrl").
-include("../include/ar_config.hrl").
-include("../include/ar_data_discovery.hrl").

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

%% The number of the release adding support for the
%% GET /data_sync_record/[start]/[end]/[limit] endpoint.
-define(GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE, 82).

%%%===================================================================
%%% Public interface.
%%%===================================================================

fetch(Start, End, StoreID) when Start >= End ->
	%% We've reached the end of the range, next time through we'll start with a clear cache.
	?LOG_DEBUG([{event, fetch_peer_intervals_end}, {pid, StoreID}, {store_id, StoreID},
		{start, Start}]),
	gen_server:cast(ar_data_sync:name(StoreID), {update_all_peers_intervals, #{}});
fetch(Start, End, StoreID) ->
	spawn_link(fun() ->
		try
			End2 = min(Start + ?QUERY_RANGE_STEP_SIZE, End),
			UnsyncedIntervals = get_unsynced_intervals(Start, End2, StoreID),

			Bucket = Start div ?NETWORK_DATA_BUCKET_SIZE,
			{ok, Config} = application:get_env(arweave, config),
			Peers =
				case Config#config.sync_from_local_peers_only of
					true ->
						Config#config.local_peers;
					false ->
						ar_data_discovery:get_bucket_peers(Bucket)
				end,

			%% The updated AllPeersIntervals cache is returned so it can be added to the State
			Parent = ar_data_sync:name(StoreID),
			End3 =
				case ar_intervals:is_empty(UnsyncedIntervals) of
					true ->
						End2;
					false ->
						min(End2,
							fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals))
				end,

			%% Schedule the next sync bucket. The cast handler logic will pause collection if needed.
			gen_server:cast(Parent, {collect_peer_intervals, End3, End})
		catch
			Class:Reason ->
				?LOG_INFO([{event, fetch_peers_process_exit}, {pid, StoreID},
					{store_id, StoreID}, {start, Start}, {class, Class}, {reason, Reason}])
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
				case get_peer_intervals(Peer, Start, UnsyncedIntervals) of
					{ok, SoughtIntervals, PeerRightBound} ->
						{Peer, SoughtIntervals, PeerRightBound};
					{error, Reason} ->
						?LOG_DEBUG([{event, failed_to_fetch_peer_intervals},
								{peer, ar_util:format_peer(Peer)},
								{reason, io_lib:format("~p", [Reason])}]),
						ok
				end
			end,
			Peers,
			?GET_SYNC_RECORD_BATCH_SIZE % fetch sync intervals from so many peers at a time
		),
	{EnqueueIntervals, MinRightBound} =
		lists:foldl(
			fun	({Peer, SoughtIntervals, RightBound}, {IntervalsAcc, RightBoundAcc}) ->
					case ar_intervals:is_empty(SoughtIntervals) of
						true ->
							{IntervalsAcc, RightBoundAcc};
						false ->
							{[{Peer, SoughtIntervals} | IntervalsAcc],
								min(RightBound, RightBoundAcc)}
					end;
				(_, Acc) ->
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
%%            the peer advertised inside the recently queried range
%% PeerRightBound: the right bound of the intervals the peer advertised; for example,
%%                 we may ask for at most 100 continuous intervals inside the given gigabyte,
%%                 but the peer may have this region very fractured and 100 intervals will
%%                 not be all intervals covering this gigabyte, so we take the right bound
%%                 to know where to query next
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
		Error ->
			Error
	end.