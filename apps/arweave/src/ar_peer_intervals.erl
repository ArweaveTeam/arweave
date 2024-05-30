-module(ar_peer_intervals).

-export([fetch/4]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

fetch(Start, End, StoreID, _AllPeersIntervals) when Start >= End ->
	%% We've reached the end of the range, next time through we'll start with a clear cache.
	?LOG_DEBUG([{event, fetch_peer_intervals_end}, {pid, StoreID}, {store_id, StoreID},
		{start, Start}]),
	gen_server:cast(ar_data_sync:name(StoreID), {update_all_peers_intervals, #{}});
fetch(Start, End, StoreID, AllPeersIntervals) ->
	spawn_link(fun() ->
		process_flag(trap_exit, true),
		try
			End2 = min(ar_util:ceil_int(Start, ?NETWORK_DATA_BUCKET_SIZE), End),
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
			case ar_intervals:is_empty(UnsyncedIntervals) of
				true ->
					ok;
				false ->
					fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals, AllPeersIntervals)
			end,

			%% Schedule the next sync bucket. The cast handler logic will pause collection if needed.
			gen_server:cast(Parent, {collect_peer_intervals, End2, End})
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

fetch_peer_intervals(Parent, Start, Peers, UnsyncedIntervals, AllPeersIntervals) ->
	Intervals =
		ar_util:pmap(
			fun(Peer) ->
				case get_peer_intervals(Peer, Start, UnsyncedIntervals, AllPeersIntervals) of
					{ok, SoughtIntervals, PeerIntervals, Left} ->
						{Peer, SoughtIntervals, PeerIntervals, Left};
					{error, Reason} ->
						?LOG_DEBUG([{event, failed_to_fetch_peer_intervals},
								{peer, ar_util:format_peer(Peer)},
								{reason, io_lib:format("~p", [Reason])}]),
						ok
				end
			end,
			Peers
		),
	EnqueueIntervals =
		lists:foldl(
			fun	({Peer, SoughtIntervals, _, _}, Acc) ->
					case ar_intervals:is_empty(SoughtIntervals) of
						true ->
							Acc;
						false ->
							[{Peer, SoughtIntervals} | Acc]
					end;
				(_, Acc) ->
					Acc
			end,
			[],
			Intervals
		),
	gen_server:cast(Parent, {enqueue_intervals, EnqueueIntervals}),

	AllPeersIntervals2 = lists:foldl(
		fun	({Peer, _, PeerIntervals, Left}, Acc) ->
				case ar_intervals:is_empty(PeerIntervals) of
					true ->
						Acc;
					false ->
						Right = element(1, ar_intervals:largest(PeerIntervals)),
						maps:put(Peer, {Right, Left, PeerIntervals}, Acc)
				end;
			(_, Acc) ->
				Acc
		end,
		AllPeersIntervals,
		Intervals
	),
	gen_server:cast(Parent, {update_all_peers_intervals, AllPeersIntervals2}).

%% @doc
%% @return {ok, Intervals, PeerIntervals, Left} | Error
%% Intervals: the intersection of the intervals we are looking for and the intervals that
%%            the peer advertises
%% PeerIntervals: all of the intervals (up to ?MAX_SHARED_SYNCED_INTERVALS_COUNT total
%%                intervals) that the peer advertises starting at offset Left.
get_peer_intervals(Peer, Left, SoughtIntervals, AllPeersIntervals) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	Right = element(1, ar_intervals:largest(SoughtIntervals)),
	case maps:get(Peer, AllPeersIntervals, not_found) of
		{Right2, Left2, PeerIntervals} when Right2 >= Right, Left2 =< Left ->
			{ok, ar_intervals:intersection(PeerIntervals, SoughtIntervals), PeerIntervals,
					Left2};
		_ ->
			case ar_http_iface_client:get_sync_record(Peer, Left + 1, Limit) of
				{ok, PeerIntervals2} ->
					{ok, ar_intervals:intersection(PeerIntervals2, SoughtIntervals),
							PeerIntervals2, Left};
				Error ->
					Error
			end
	end.
