-module(ar_data_root_sync).

-behaviour(gen_server).

-export([start_link/1, name/1, store_data_roots/4, store_data_roots_sync/4, validate_data_roots/4]).
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

%% @doc Store the given data roots.
store_data_roots(BlockStart, BlockEnd, TXRoot, Entries) ->
	gen_server:cast(ar_data_sync_default, {store_data_roots,
		BlockStart, BlockEnd, TXRoot, Entries}).

%% @doc Store the given data roots synchronously.
store_data_roots_sync(BlockStart, BlockEnd, TXRoot, Entries) ->
	gen_server:call(ar_data_sync_default, {store_data_roots_sync,
		BlockStart, BlockEnd, TXRoot, Entries}, 120000).

%% @doc Validate the given data roots against the local block index.
%% Also recompute the TXRoot from entries and verify Merkle paths.
validate_data_roots(TXRoot, BlockSize, Entries, Offset) ->
	{BlockStart, BlockEnd, ExpectedTXRoot} = ar_block_index:get_block_bounds(Offset),
	CheckBlockBounds =
		case Offset >= BlockStart andalso Offset < BlockEnd of
			false ->
				{error, invalid_block_bounds};
			true ->
				ok
		end,
	CheckBlockSize =
		case CheckBlockBounds of
			ok ->
				case BlockSize == BlockEnd - BlockStart of
					false ->
						{error, invalid_block_size};
					true ->
						ok
				end;
			Error ->
				Error
		end,
	PrepareDataRootPairs =
		case CheckBlockSize of
			ok ->
				prepare_data_root_pairs(Entries, BlockStart, BlockSize);
			Error2 ->
				Error2
		end,
	ValidateTXRoot =
		case PrepareDataRootPairs of
			{ok, Triplets} ->
				case TXRoot == ExpectedTXRoot of
					false ->
						{error, invalid_tx_root};
					true ->
						{ok, Triplets}
				end;
			{error, _} = Error3 ->
				Error3
		end,
	case ValidateTXRoot of
		{ok, Triplets2} ->
			case verify_tx_paths(Triplets2, TXRoot, BlockStart, BlockEnd, 0) of
				ok ->
					{ok, {TXRoot, BlockSize, Entries}};
				Error4 ->
					Error4
			end;
		Error5 ->
			Error5
	end.

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
			store_data_roots(BlockStart, BlockEnd, TXRoot, Entries);
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

prepare_data_root_pairs(Entries, BlockStart, BlockSize) ->
	Result = lists:foldr(
		fun
			(_, {error, _} = Error) ->
				Error;
			({_DataRoot, 0, _TXStartOffset, _TXPath}, _Acc) ->
				{error, invalid_zero_tx_size};
			({DataRoot, TXSize, TXStartOffset, TXPath}, {ok, {Total, Acc}}) ->
				MerkleLabel = TXStartOffset + TXSize - BlockStart,
				case MerkleLabel >= 0 of
					true ->
						PaddedSize = get_padded_size(TXSize, BlockStart),
						{ok, {Total + PaddedSize, [{DataRoot, MerkleLabel, TXPath} | Acc]}};
					false ->
						{error, invalid_entry_merkle_label}
				end
		end,
		{ok, {0,[]}},
		Entries
	),
	case Result of
		{ok, {Total, Entries2}} ->
			case Total == BlockSize of
				true ->
					{ok, Entries2};
				false ->
					{error, invalid_total_tx_size}
			end;
		Error6 ->
			Error6
	end.

get_padded_size(TXSize, BlockStart) ->
	case BlockStart >= ar_block:strict_data_split_threshold() of
		true ->
			ar_poa:get_padded_offset(TXSize, 0);
		false ->
			TXSize
	end.

verify_tx_paths([], _TXRoot, _BlockStart, _BlockEnd, _TXStartOffset) ->
	ok;
verify_tx_paths([Entry | Entries], TXRoot, BlockStart, BlockEnd, TXStartOffset) ->
	{DataRoot, TXEndOffset, TXPath} = Entry,
	BlockSize = BlockEnd - BlockStart,
	case ar_merkle:validate_path(TXRoot, TXEndOffset - 1, BlockSize, TXPath) of
		false ->
			{error, invalid_tx_path};
		{DataRoot, TXStartOffset, TXEndOffset} ->
			PaddedEndOffset = get_padded_size(TXEndOffset, BlockStart),
			verify_tx_paths(Entries, TXRoot, BlockStart, BlockEnd, PaddedEndOffset);
		_ ->
			{error, invalid_tx_path}
	end.
