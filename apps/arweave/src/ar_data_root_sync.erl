-module(ar_data_root_sync).

-behaviour(gen_server).

-export([start_link/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_sup.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-record(state, {
	store_id,
	range_start,
	range_end,
	scan_cursor
}).

-define(DATA_ROOTS_SYNC_RELEASE_NUMBER, 89).

%% How long we wait before (re-)scanning our range for unsynced data roots.
-ifdef(AR_TEST).
-define(DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, 2000).
-else.
-define(DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, 600_000). % 10 minutes.
-endif.

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
			{Delay, State2} = sync_block_data_roots(State),
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
	End = min(RangeEnd, ar_data_sync:get_disk_pool_threshold()),
	{ok, Cursor2} = sync_block_data_roots(StoreID, Cursor, End),
	{Delay, Cursor3} =
		case Cursor2 >= End of
			true ->
				{?DATA_ROOTS_SYNC_SCAN_INTERVAL_MS, RangeStart};
			false ->
				{0, Cursor2}
		end,
	{Delay, State#state{ scan_cursor = Cursor3 }}.

sync_block_data_roots(_StoreID, Cursor, RangeEnd) when Cursor >= RangeEnd ->
	{ok, Cursor};
sync_block_data_roots(StoreID, Cursor, RangeEnd) ->
	{BlockStart, BlockEnd, _} = ar_block_index:get_block_bounds(Cursor),
	Cursor2 =
		case BlockStart >= RangeEnd of
			true ->
				RangeEnd;
			false ->
				case ar_sync_record:is_recorded(Cursor + 1, data_roots, ?DEFAULT_MODULE) of
					true ->
						BlockEnd;
					false ->
						maybe_fetch_and_store(BlockStart, BlockEnd),
						BlockEnd
				end
		end,
	sync_block_data_roots(StoreID, Cursor2, RangeEnd).

maybe_fetch_and_store(BlockStart, BlockEnd) ->
	Peers = ar_peers:get_peers(current),
	Peers2 = lists:filter(
		fun(Peer) ->
			ar_peers:get_peer_release(Peer) >= ?DATA_ROOTS_SYNC_RELEASE_NUMBER
		end,
		Peers
	),
	case fetch_data_roots_from_peers(Peers2, BlockStart) of
		{ok, {TXRoot, BlockSize, Entries}} ->
			BlockSize = BlockEnd - BlockStart,
			send_to_storage(BlockStart, BlockEnd, TXRoot, Entries);
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

send_to_storage(BlockStart, BlockEnd, TXRoot, Entries) ->
	gen_server:cast(ar_data_sync_default, {store_data_roots,
		BlockStart, BlockEnd, TXRoot, Entries}).