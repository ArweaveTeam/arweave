-module(ar_data_sync_storage).

-behaviour(gen_server).

-export([start_link/2, name/1, update_chunks_index/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%% Let at least this many chunks stack up, per storage module, then write them
%% on disk in the ascending order, to reduce out-of-order disk writes causing
%% fragmentation.
-ifdef(DEBUG).
-define(STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD, 2).
-else.
-define(STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD, 100). % ~ 25 MB worth of chunks.
-endif.

%% If a chunk spends longer than this in the store queue, write it on disk
%% without waiting for ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD chunks
%% to stack up.
-ifdef(DEBUG).
-define(STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD, 1000).
-else.
-define(STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD, 2_000). % 2 seconds.
-endif.

-record(state, {
	store_id,
	chunks_index_db,
	chunk_data_db,
	packing_map = #{},
	%% The priority queue of chunks sorted by offset. The motivation is to have
	%% chunks stack up, per storage module, before writing them on disk so that
	%% we can write them in the ascending order and reduce out-of-order disk
	%% writes causing fragmentation.
	store_chunk_queue = gb_sets:new(),
	%% The length of the store chunk queue.
	store_chunk_queue_len = 0,
	%% The threshold controlling the brief accumuluation of the chunks in the
	%% queue before the actual disk dump, to reduce the chance of out-of-order
	%% write causing disk fragmentation.
	store_chunk_queue_threshold = ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

name(StoreID) ->
	list_to_atom("ar_data_sync_writer_"
			++ ar_storage_module:label_by_id(StoreID)).

start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(StoreID) ->
	?LOG_INFO([{event, ar_data_sync_storage_start}, {store_id, StoreID}]),
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	gen_server:cast(self(), process_store_chunk_queue),
	{ok, #state{
		store_id = StoreID,
		%% Initialized in ar_data_sync:init_kv.
		chunks_index_db = {chunks_index, StoreID},
		chunk_data_db = {chunk_data_db, StoreID}
	}}.

handle_cast({expire_repack_chunk_request, Key}, State) ->
	#state{ store_id = StoreID, packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, {_, DataPath, Offset, DataRoot, _, _, _}} ->
			ar_data_sync:decrement_chunk_cache_size(StoreID),
			DataPathHash = crypto:hash(sha256, DataPath),
			?LOG_DEBUG([{event, expired_repack_chunk_request},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)},
					{relative_offset, Offset}]),
			{noreply, State#state{
					packing_map = maps:remove(Key, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast({packed_chunk_promise, Key, Args}, State) ->
	#state{ packing_map = Map } = State,
	Map2 = maps:put(Key, Args, Map),
	{noreply, State#state{ packing_map = Map2 }};

handle_cast(process_store_chunk_queue, State) ->
	ar_util:cast_after(200, self(), process_store_chunk_queue),
	{noreply, process_store_chunk_queue(State)};

handle_cast({store_chunk, ChunkArgs, Args}, State) ->
	{noreply, store_chunk(ChunkArgs, Args, State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_info({chunk, {packed, Offset, ChunkArgs}}, State) ->
	#state{ packing_map = PackingMap } = State,
	Packing = element(1, ChunkArgs),
	Key = {Offset, Packing},
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, Args} when element(1, Args) == Packing ->
			State2 = State#state{ packing_map = maps:remove(Key, PackingMap) },
			{noreply, store_chunk(ChunkArgs, Args, State2)};
		_ ->
			{noreply, State}
	end;

handle_info({chunk, _}, State) ->
	{noreply, State};

handle_info(Message,  #state{ store_id = StoreID } = State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE},
			{store_id, StoreID}, {message, Message}]),
	{noreply, State}.

terminate(Reason, #state{ store_id = StoreID }) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE},
			{store_id, StoreID}, {reason, io_lib:format("~p", [Reason])}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

process_store_chunk_queue(#state{ store_chunk_queue_len = 0 } = State, StartLen) ->
	log_stored_chunks(State, StartLen),
	State;
process_store_chunk_queue(State, StartLen) ->
	#state{ store_chunk_queue = Q, store_chunk_queue_len = Len,
			store_chunk_queue_threshold = Threshold,
		    store_id = StoreID } = State,
	Timestamp = element(2, gb_sets:smallest(Q)),
	Now = os:system_time(millisecond),
	Threshold2 =
		case Threshold < ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD of
			true ->
				Threshold;
			false ->
				case Len > Threshold of
					true ->
						0;
					false ->
						Threshold
				end
		end,
	case Len > Threshold2
			orelse Now - Timestamp > ?STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD of
		true ->
			{{_Offset, _Timestamp, _Ref, ChunkArgs, Args}, Q2} = gb_sets:take_smallest(Q),
			store_chunk2(ChunkArgs, Args, State),
			ar_data_sync:decrement_chunk_cache_size(StoreID),
			State2 = State#state{ store_chunk_queue = Q2,
					store_chunk_queue_len = Len - 1,
					store_chunk_queue_threshold = min(Threshold2 + 1,
							?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD) },
			process_store_chunk_queue(State2);
		false ->
			log_stored_chunks(State, StartLen),
			State
	end.

log_stored_chunks(State, StartLen) ->
	#state{ store_chunk_queue_len = EndLen, store_id = StoreID } = State,
	StoredCount = StartLen - EndLen,
	case StoredCount > 0 of
		true ->
			?LOG_DEBUG([{event, stored_chunks}, {count, StoredCount},
					{store_id, StoreID}]);
		false ->
			ok
	end.

process_store_chunk_queue(#state{ store_chunk_queue_len = StartLen } = State) ->
	process_store_chunk_queue(State, StartLen).

store_chunk(ChunkArgs, Args, State) ->
	%% Let at least N chunks stack up, then write them in the ascending order,
	%% to reduce out-of-order disk writes causing fragmentation.
	#state{ store_chunk_queue = Q, store_chunk_queue_len = Len } = State,
	Now = os:system_time(millisecond),
	Offset = element(3, ChunkArgs),
	Q2 = gb_sets:add_element({Offset, Now, make_ref(), ChunkArgs, Args}, Q),
	State2 = State#state{ store_chunk_queue = Q2,
			store_chunk_queue_len = Len + 1 },
	process_store_chunk_queue(State2).

store_chunk2(ChunkArgs, Args, State) ->
	#state{ store_id = StoreID, chunks_index_db = ChunksIndexDB } = State,
	{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = ChunkArgs,
	{_Packing, DataPath, Offset, DataRoot, TXPath,
			OriginStoreID, OriginChunkDataKey} = Args,
	PaddedOffset = ar_data_sync:get_chunk_padded_offset(AbsoluteOffset),
	UnpaddedStartOffset = AbsoluteOffset - ChunkSize,
	StartOffset = ar_data_sync:get_chunk_padded_offset(UnpaddedStartOffset),
	DataPathHash = crypto:hash(sha256, DataPath),
	case ar_sync_record:delete(PaddedOffset, StartOffset,
			ar_data_sync, StoreID) of
		{error, Reason} ->
			log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset,
					DataRoot, DataPathHash, StoreID),
			{error, Reason};
		ok ->
			DataPathHash = crypto:hash(sha256, DataPath),
			ChunkDataKey =
				case StoreID == OriginStoreID of
					true ->
						OriginChunkDataKey;
					_ ->
						ar_data_sync:get_chunk_data_key(DataPathHash)
				end,
			case write_chunk(AbsoluteOffset, ChunkDataKey, Chunk,
					ChunkSize, DataPath, Packing, State) of
				ok ->
					case update_chunks_index({AbsoluteOffset, Offset,
							ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
							Packing, StoreID, ChunksIndexDB}) of
						ok ->
							ok;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, AbsoluteOffset,
									Offset, DataRoot, DataPathHash, StoreID),
							{error, Reason}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, AbsoluteOffset,
							Offset, DataRoot, DataPathHash, StoreID),
					{error, Reason}
			end
	end.

log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset,
		DataRoot, DataPathHash, StoreID) ->
	?LOG_ERROR([{event, failed_to_store_chunk},
			{reason, io_lib:format("~p", [Reason])},
			{absolute_end_offset, AbsoluteOffset, Offset},
			{relative_offset, Offset},
			{data_path_hash, ar_util:encode(DataPathHash)},
			{data_root, ar_util:encode(DataRoot)},
			{store_id, StoreID}]).

write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, State) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			ok;
		false ->
			Args = {Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing,
					State},
			write_not_blacklisted_chunk(Args)
	end.

write_not_blacklisted_chunk(Args) ->
	{Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, State} = Args,
	#state{ chunk_data_db = ChunkDataDB, store_id = StoreID } = State,
	ShouldStoreInChunkStorage
		= should_store_in_chunk_storage(Offset, ChunkSize, Packing),
	Result =
		case ShouldStoreInChunkStorage of
			true ->
				PaddedOffset = ar_data_sync:get_chunk_padded_offset(Offset),
				ar_chunk_storage:put(PaddedOffset, Chunk, StoreID);
			false ->
				ok
		end,
	case Result of
		ok ->
			case ShouldStoreInChunkStorage of
				false ->
					ar_kv:put(ChunkDataDB, ChunkDataKey,
							term_to_binary({Chunk, DataPath}));
				true ->
					ar_kv:put(ChunkDataDB, ChunkDataKey,
							term_to_binary(DataPath))
			end;
		_ ->
			Result
	end.

%% @doc 256 KiB chunks are stored in the blob storage optimized for read speed.
%% Return true if we want to place the chunk there.
should_store_in_chunk_storage(Offset, ChunkSize, Packing) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			%% All chunks above ?STRICT_DATA_SPLIT_THRESHOLD are placed in
			%% 256 KiB buckets so technically can be stored in ar_chunk_storage.
			%% However, to avoid managing padding in ar_chunk_storage for
			%% unpacked chunks smaller than 256 KiB
			%% (we do not need fast random access to unpacked chunks after
			%% ?STRICT_DATA_SPLIT_THRESHOLD anyways), we put them to RocksDB.
			Packing /= unpacked orelse ChunkSize == (?DATA_CHUNK_SIZE);
		false ->
			ChunkSize == (?DATA_CHUNK_SIZE)
	end.

update_chunks_index(Args) ->
	AbsoluteChunkOffset = element(1, Args),
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2(Args)
	end.

update_chunks_index2(Args) ->
	{AbsoluteOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing, StoreID, ChunksIndexDB} = Args,
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	case ar_kv:put(ChunksIndexDB, Key, term_to_binary(Value)) of
		ok ->
			UnpaddedStartOffset = AbsoluteOffset - ChunkSize,
			StartOffset
				= ar_data_sync:get_chunk_padded_offset(UnpaddedStartOffset),
			PaddedOffset
				= ar_data_sync:get_chunk_padded_offset(AbsoluteOffset),
			case ar_sync_record:add(PaddedOffset, StartOffset, Packing,
					ar_data_sync, StoreID) of
				ok ->
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_update_sync_record},
							{reason, io_lib:format("~p", [Reason])},
							{chunk, ar_util:encode(ChunkDataKey)},
							{absolute_end_offset, AbsoluteOffset},
							{data_root, ar_util:encode(DataRoot)},
							{store_id, StoreID}]),
					{error, Reason}
			end;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_update_chunk_index},
					{reason, io_lib:format("~p", [Reason])},
					{chunk_data_key, ar_util:encode(ChunkDataKey)},
					{data_root, ar_util:encode(DataRoot)},
					{absolute_end_offset, AbsoluteOffset},
					{store_id, StoreID}]),
			{error, Reason}
	end.