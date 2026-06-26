-module(ar_data_root_sync).

-behaviour(gen_server).

-export([start_link/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").

-record(state, {
	store_id,
	range_start,
	range_end,
	scan_cursor
}).

-define(DATA_ROOTS_SYNC_RELEASE_NUMBER, 91).

%% How long we wait before (re-)scanning our range for unsynced data roots.
-ifdef(AR_TEST).
-define(DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, 2000).
-else.
-define(DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, 600_000). % 10 minutes.
-endif.

%% Emit a log every time this many missing blocks have been fetched.
-define(DATA_ROOTS_SYNC_PROGRESS_BLOCKS, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(StoreID) ->
	Name = name(StoreID),
	gen_server:start_link({local, Name}, ?MODULE, [StoreID], []).
	
name(StoreID) ->
	list_to_atom("ar_data_root_sync_" ++ ar_storage_module:label(StoreID)).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([StoreID]) ->
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	gen_server:cast(self(), sync),
	{ok, #state{ store_id = StoreID,
			range_start = RangeStart,
			range_end = RangeEnd,
			scan_cursor = RangeStart }}.

handle_cast(sync, State) ->
	case ar_node:is_joined() of
		false ->
			ar_util:cast_after(500, self(), sync),
			{noreply, State};
		true ->
			SyncingEnabled = arweave_config:get(
				[gossip, data_roots, syncing_enabled]),
			{Delay, State2} =
				case SyncingEnabled of
					true ->
						sync_block_data_roots(State);
					false ->
						{?DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, State}
				end,
			ar_util:cast_after(Delay, self(), sync),
			{noreply, State2}
	end;

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ignored, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

sync_block_data_roots(#state{ store_id = StoreID, range_start = RangeStart,
	range_end = RangeEnd, scan_cursor = Cursor } = State) ->
	End = min(RangeEnd, ar_disk_pool:get_threshold()),
	{ok, Cursor2} = sync_block_data_roots(StoreID, Cursor, End, RangeStart, {0, 0, 0}),
	{Delay, Cursor3} =
		case Cursor2 >= End of
			true ->
				{?DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, RangeStart};
			false ->
				{0, Cursor2}
		end,
	{Delay, State#state{ scan_cursor = Cursor3 }}.

sync_block_data_roots(StoreID, Cursor, End, _RangeStart, Stats) when Cursor >= End ->
	{Scanned, Synced, Missing}  = Stats,
	?LOG_INFO([{event, data_root_sync_pass_complete}, {store_id, StoreID},
			{blocks_total, Scanned}, {blocks_synced, Synced}, {blocks_missing, Missing},
			{synced_pct, synced_pct(Synced, Scanned)}]),
	{ok, Cursor};
sync_block_data_roots(StoreID, Cursor, End, RangeStart, {Scanned, Synced, Missing} = Stats) ->
	{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Cursor),
	case BlockStart >= End of
		true ->
			sync_block_data_roots(StoreID, End, End, RangeStart, Stats);
		false ->
			Stats2 =
				case ar_data_roots:are_synced(BlockStart, BlockEnd, TXRoot, ?DEFAULT_MODULE) of
					true ->
						{Scanned + 1, Synced + 1, Missing};
					false ->
						maybe_fetch_and_store(BlockStart, BlockEnd),
						FetchedStats = {Scanned + 1, Synced, Missing + 1},
						maybe_log_data_root_sync_progress(StoreID, BlockEnd, End, RangeStart,
								FetchedStats),
						FetchedStats
				end,
			sync_block_data_roots(StoreID, BlockEnd, End, RangeStart, Stats2)
	end.

%% @doc Emit a progress log every ?DATA_ROOTS_SYNC_PROGRESS_BLOCKS
%% missing blocks. Missing blocks are the blocks the data root syncing progress fetches.
maybe_log_data_root_sync_progress(StoreID, Cursor, End, RangeStart,
		{Scanned, Synced, Missing}) ->
	case Missing > 0 andalso Missing rem ?DATA_ROOTS_SYNC_PROGRESS_BLOCKS =:= 0 of
		true ->
			?LOG_INFO([{event, data_root_sync_progress}, {store_id, StoreID},
					{blocks_scanned, Scanned}, {blocks_synced, Synced},
					{blocks_missing, Missing},
					{range_pct, range_pct(Cursor, RangeStart, End)}, {cursor, Cursor}]);
		false ->
			ok
	end.

%% @doc Percentage (0-100) of the scan range covered by the given offset.
range_pct(Offset, RangeStart, End)
		when is_integer(Offset), is_integer(RangeStart), is_integer(End),
			End > RangeStart, Offset > RangeStart ->
	min(100, (Offset - RangeStart) * 100 div (End - RangeStart));
range_pct(_Offset, _RangeStart, _End) ->
	0.

%% @doc Percentage (0-100) of scanned blocks whose data roots have been synced by the data root syncing process.
synced_pct(_Synced, 0) ->
	100;
synced_pct(Synced, Total) ->
	min(100, Synced * 100 div Total).

maybe_fetch_and_store(BlockStart, BlockEnd) ->
	Peers = ar_peers:get_peers(current),
	%% Shuffle eligible peers so repeated fetches do not always hit them in the same order.
	Peers2 = ar_util:shuffle_list(lists:filter(
		fun(Peer) ->
			ar_peers:get_peer_release(Peer) >= ?DATA_ROOTS_SYNC_RELEASE_NUMBER
		end,
		Peers
	)),
	case fetch_data_roots_from_peers(Peers2, BlockStart) of
		{ok, {TXRoot, BlockSize, DataRootEntries}} ->
			BlockSize = BlockEnd - BlockStart,
			ar_data_roots:store_block_async(
				BlockStart, BlockEnd, TXRoot, DataRootEntries, ?DEFAULT_MODULE);
		_ ->
			ok
	end.

fetch_data_roots_from_peers([], _Offset) ->
	{error, not_found};
fetch_data_roots_from_peers([Peer | Rest], Offset) ->
	case ar_http_iface_client:get_data_roots(Peer, Offset) of
		{ok, _} = Reply ->
			Reply;
		{error, Error} ->
			?LOG_DEBUG([{event, fetch_data_roots_from_peers_error},
					{peer, Peer},
					{offset, Offset},
					{error, io_lib:format("~p", [Error])}]),
			fetch_data_roots_from_peers(Rest, Offset)
	end.
