-module(ar_data_sync).

-behaviour(gen_server).

-export([name/1, start_link/2, register_workers/0, join/1, add_tip_block/2, add_block/2,
		invalidate_bad_data_record/4, is_chunk_proof_ratio_attractive/3,
		add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
		get_chunk/2, get_chunk_data/2, get_chunk_proof/2, get_tx_data/1, get_tx_data/2,
		get_tx_offset/1, get_tx_offset_data_in_range/2, has_data_root/2,
		request_tx_data_removal/3, request_data_removal/4, record_disk_pool_chunks_count/0,
		record_chunk_cache_size_metric/0, is_chunk_cache_full/0, is_disk_space_sufficient/1,
		get_chunk_by_byte/2, advance_chunks_index_cursor/1,
		read_chunk/3, write_chunk/5, read_data_path/2,
		increment_chunk_cache_size/0, decrement_chunk_cache_size/0,
		get_chunk_metadata_range/3, get_merkle_rebase_threshold/0,
		is_footprint_record_supported/3, get_data_roots_for_offset/1, are_data_roots_synced/3,
		get_disk_pool_threshold/0]).

-export([add_chunk_to_disk_pool/5]).

-export([debug_get_disk_pool_chunks/0]).

%% For data-doctor tools
-export([init_kv/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-export([enqueue_intervals/3, remove_expired_disk_pool_data_roots/0]).

-include("ar.hrl").
-include("ar_sup.hrl").
-include("ar_consensus.hrl").
-include("ar_poa.hrl").
-include("ar_data_discovery.hrl").
-include("ar_data_sync.hrl").
-include("ar_sync_buckets.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% The key for storing migration cursor in the migrations_index database.
-define(FOOTPRINT_MIGRATION_CURSOR_KEY, <<"footprint_migration_cursor">>).

-ifdef(AR_TEST).
-define(COLLECT_SYNC_INTERVALS_FREQUENCY_MS, 1_000).
-else.
-define(COLLECT_SYNC_INTERVALS_FREQUENCY_MS, 10_000).
-endif.

-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%% The number of chunks to migrate per batch during footprint migration.
-ifdef(AR_TEST).
-define(FOOTPRINT_MIGRATION_BATCH_SIZE, 10).
-else.
-define(FOOTPRINT_MIGRATION_BATCH_SIZE, 100).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

name(StoreID) ->
	list_to_atom("ar_data_sync_" ++ ar_storage_module:label(StoreID)).

start_link(Name, Args) ->
	gen_server:start_link({local, Name}, ?MODULE, Args, []).

%% @doc Register the workers that will be monitored by ar_data_sync_sup.erl.
register_workers() ->
	{ok, Config} = arweave_config:get_env(),
	StorageModuleWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			StoreLabel = ar_storage_module:label(StoreID),
			Name = list_to_atom("ar_data_sync_" ++ StoreLabel),
			?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, none}])
		end,
		Config#config.storage_modules
	),
	DefaultStorageModuleWorker = ?CHILD_WITH_ARGS(ar_data_sync, worker,
		ar_data_sync_default, [ar_data_sync_default, {?DEFAULT_MODULE, none}]),
	RepackInPlaceWorkers = lists:map(
		fun({StorageModule, TargetPacking}) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = ar_data_sync:name(StoreID),
			?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, TargetPacking}])
		end,
		Config#config.repack_in_place_storage_modules
	),
	StorageModuleWorkers ++ [DefaultStorageModuleWorker] ++ RepackInPlaceWorkers.

%% @doc Notify the server the node has joined the network on the given block index.
join(RecentBI) ->
	gen_server:cast(ar_data_sync_default, {join, RecentBI}).

%% @doc Notify the server about the new tip block.
add_tip_block(BlockTXPairs, RecentBI) ->
	gen_server:cast(ar_data_sync_default, {add_tip_block, BlockTXPairs, RecentBI}).

invalidate_bad_data_record(AbsoluteEndOffset, ChunkSize, StoreID, Case) ->
	gen_server:cast(name(StoreID), {invalidate_bad_data_record,
		{AbsoluteEndOffset, ChunkSize, StoreID, Case}}).

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification. A small chunk
%% is accepted if it is the last chunk of the corresponding transaction - such chunks
%% may be produced by ar_tx:chunk_binary/1, the legacy splitting method used to split
%% v1 data or determine the data root of a v2 tx when data is uploaded via the data field.
%% Due to the block limit we can only get up to 1k such chunks per block.
is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) ->
	DataPathSize = byte_size(DataPath),
	case DataPathSize of
		0 ->
			false;
		_ ->
			case catch ar_merkle:extract_note(DataPath) of
				{'EXIT', _} ->
					false;
				Offset ->
					Offset == TXSize orelse DataPathSize =< ChunkSize
			end
	end.

%% @doc Store the given chunk if the proof is valid.
%% Called when a chunk is pushed to the node via POST /chunk.
%% The chunk is placed in the disk pool. The periodic process
%% scanning the disk pool will later record it as synced.
%% The item is removed from the disk pool when the chunk's offset
%% drops below the disk pool threshold.
add_chunk_to_disk_pool(DataRoot, DataPath, Chunk, Offset, TXSize) ->
	DataRootIndex = {data_root_index, ?DEFAULT_MODULE},
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	DiskPoolChunksIndex = {disk_pool_chunks_index, ?DEFAULT_MODULE},
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootOffsetReply = get_data_root_offset(DataRootKey, ?DEFAULT_MODULE),
	DataRootInDiskPool = ets:lookup(ar_disk_pool_data_roots, DataRootKey),
	ChunkSize = byte_size(Chunk),
	{ok, Config} = arweave_config:get_env(),
	DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * 1024 * 1024,
	DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	CheckDiskPool =
		case {DataRootOffsetReply, DataRootInDiskPool} of
			{not_found, []} ->
				?LOG_WARNING([{event, failed_to_add_chunk_to_disk_pool},
					{reason, data_root_not_found}, {offset, Offset},
					{data_root, ar_util:encode(DataRoot)}]),
				{error, data_root_not_found};
			{not_found, [{_, {Size, Timestamp, TXIDSet}}]} ->
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						?LOG_WARNING([{event, failed_to_add_chunk_to_disk_pool},
							{reason, exceeds_disk_pool_size_limit1}, {offset, Offset},
							{data_root_size, Size}, {chunk_size, ChunkSize},
							{data_root_limit, DataRootLimit}, {disk_pool_size, DiskPoolSize},
							{disk_pool_limit, DiskPoolLimit}]),
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {Size + ChunkSize, Timestamp, TXIDSet}}
				end;
			_ ->
				case DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						?LOG_WARNING([{event, failed_to_add_chunk_to_disk_pool},
							{reason, exceeds_disk_pool_size_limit2}, {offset, Offset},
							{chunk_size, ChunkSize}, {disk_pool_size, DiskPoolSize},
							{disk_pool_limit, DiskPoolLimit}]),
						{error, exceeds_disk_pool_size_limit};
					false ->
						Timestamp =
							case DataRootInDiskPool of
								[] ->
									os:system_time(microsecond);
								[{_, {_, Timestamp2, _}}] ->
									Timestamp2
							end,
						{ok, {ChunkSize, Timestamp, not_set}}
				end
		end,
	ValidateProof =
		case CheckDiskPool of
			{error, _} = Error ->
				Error;
			{ok, DiskPoolDataRootValue} ->
				case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
					false ->
						?LOG_WARNING([{event, failed_to_add_chunk_to_disk_pool},
							{reason, invalid_proof}, {offset, Offset}]),
						{error, invalid_proof};
					{true, PassesBase, PassesStrict, PassesRebase, EndOffset} ->
						{ok, {EndOffset, PassesBase, PassesStrict, PassesRebase,
								DiskPoolDataRootValue}}
				end
		end,
	CheckSynced =
		case ValidateProof of
			{error, _} = Error2 ->
				Error2;
			{ok, {EndOffset2, _PassesBase2, _PassesStrict2, _PassesRebase2,
					{_, Timestamp3, _}} = PassedState2} ->
				DataPathHash = crypto:hash(sha256, DataPath),
				DiskPoolChunkKey = << Timestamp3:256, DataPathHash/binary >>,
				case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
					{ok, _DiskPoolChunk} ->
						%% The chunk is already in disk pool.
						{synced_disk_pool, EndOffset2};
					not_found ->
						case DataRootOffsetReply of
							not_found ->
								{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}};
							{ok, {TXStartOffset, _}} ->
								case chunk_offsets_synced(DataRootIndex, DataRootKey,
										%% The same data may be uploaded several times.
										%% Here we only accept the chunk if any of the
										%% last 5 instances of this data is not filled in
										%% yet.
										EndOffset2, TXStartOffset, 5) of
									true ->
										synced;
									false ->
										{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}}
								end
						end;
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_read_chunk_from_disk_pool},
								{reason, io_lib:format("~p", [Reason])},
								{data_path_hash, ar_util:encode(DataPathHash)},
								{data_root, ar_util:encode(DataRoot)},
								{relative_offset, EndOffset2}]),
						{error, failed_to_store_chunk}
				end
		end,
	case CheckSynced of
		synced ->
			ok;
		{synced_disk_pool, EndOffset4} ->
			case is_estimated_long_term_chunk(DataRootOffsetReply, EndOffset4) of
				false ->
					temporary;
				true ->
					ok
			end;
		{error, _} = Error4 ->
			Error4;
		{ok, {DataPathHash2, DiskPoolChunkKey2, {EndOffset3, PassesBase3, PassesStrict3,
				PassesRebase3, DiskPoolDataRootValue2}}} ->
			ChunkDataKey = get_chunk_data_key(DataPathHash2),
			case put_chunk_data(ChunkDataKey, ?DEFAULT_MODULE, {Chunk, DataPath}) of
				{error, Reason2} ->
					?LOG_WARNING([{event, failed_to_store_chunk_in_disk_pool},
						{reason, io_lib:format("~p", [Reason2])},
						{data_path_hash, ar_util:encode(DataPathHash2)},
						{data_root, ar_util:encode(DataRoot)},
						{relative_offset, EndOffset3}]),
					{error, failed_to_store_chunk};
				ok ->
					DiskPoolChunkValue = term_to_binary({EndOffset3, ChunkSize, DataRoot,
							TXSize, ChunkDataKey, PassesBase3, PassesStrict3, PassesRebase3}),
					case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey2,
							DiskPoolChunkValue) of
						{error, Reason3} ->
							?LOG_WARNING([{event, failed_to_record_chunk_in_disk_pool},
								{reason, io_lib:format("~p", [Reason3])},
								{data_path_hash, ar_util:encode(DataPathHash2)},
								{data_root, ar_util:encode(DataRoot)},
								{relative_offset, EndOffset3}]),
							{error, failed_to_store_chunk};
						ok ->
							ets:insert(ar_disk_pool_data_roots,
									{DataRootKey, DiskPoolDataRootValue2}),
							ets:update_counter(ar_data_sync_state, disk_pool_size,
									{2, ChunkSize}),
							prometheus_gauge:inc(pending_chunks_size, ChunkSize),
							case is_estimated_long_term_chunk(DataRootOffsetReply, EndOffset3) of
								false ->
									temporary;
								true ->
									ok
							end
					end
			end
	end.

%% @doc Store the given value in the chunk data DB.
-spec put_chunk_data(
	ChunkDataKey :: binary(),
	StoreID :: term(),
	Value :: DataPath :: binary() | {Chunk :: binary(), DataPath :: binary()}) ->
		ok | {error, term()}.
put_chunk_data(ChunkDataKey, StoreID, Value) ->
	ar_kv:put({chunk_data_db, StoreID}, ChunkDataKey, term_to_binary(Value)).

get_chunk_data(ChunkDataKey, StoreID) ->
	ar_kv:get({chunk_data_db, StoreID}, ChunkDataKey).

delete_chunk_data(ChunkDataKey, StoreID) ->
	ar_kv:delete({chunk_data_db, StoreID}, ChunkDataKey).

-spec put_chunk_metadata(
	AbsoluteOffset :: non_neg_integer(),
	StoreID :: term(),
	Metadata :: term()) -> ok | {error, term()}.
put_chunk_metadata(AbsoluteOffset, StoreID,
	{_ChunkDataKey, _TXRoot, _DataRoot, _TXPath, _Offset, _ChunkSize} = Metadata) ->
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	ar_kv:put({chunks_index, StoreID}, Key, term_to_binary(Metadata)).

get_chunk_metadata(AbsoluteOffset, StoreID) ->
	case ar_kv:get({chunks_index, StoreID}, << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			not_found
	end.

delete_chunk_metadata(AbsoluteOffset, StoreID) ->
	ar_kv:delete({chunks_index, StoreID}, << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>).

%% @doc Return {ok, Map} | {error, Error} where
%% Map is
%% AbsoluteEndOffset => {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize}
%% map with all the chunk metadata found within the given range AbsoluteEndOffset >= Start,
%% AbsoluteEndOffset =< End. Return the empty map if no metadata is found.
get_chunk_metadata_range(Start, End, StoreID) ->
	case ar_kv:get_range({chunks_index, StoreID},
			<< Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>) of
		{ok, Map} ->
			{ok, maps:fold(
					fun(K, V, Acc) ->
						<< Offset:?OFFSET_KEY_BITSIZE >> = K,
						maps:put(Offset, binary_to_term(V), Acc)
					end,
					#{},
					Map)};
		Error ->
			Error
	end.
delete_chunk_metadata_range(Start, End, State) ->
	#sync_data_state{ chunks_index = ChunksIndex } = State,
	ar_kv:delete_range(ChunksIndex, << (Start + 1):?OFFSET_KEY_BITSIZE >>,
			<< (End + 1):?OFFSET_KEY_BITSIZE >>).

%% @doc Return true if we expect the chunk with the given data root index value and
%% relative end offset to end up in one of the configured storage modules.
is_estimated_long_term_chunk(DataRootOffsetReply, EndOffset) ->
	WeaveSize = ar_node:get_current_weave_size(),
	case DataRootOffsetReply of
		not_found ->
			%% A chunk from a pending transaction.
			is_offset_vicinity_covered(WeaveSize);
		{ok, {TXStartOffset, _}} ->
			WeaveSize = ar_node:get_current_weave_size(),
			Size = ar_node:get_recent_max_block_size(),
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case AbsoluteEndOffset > WeaveSize - Size * 4 of
				true ->
					%% A relatively recent offset - do not expect this chunk to be
					%% persisted unless we have some storage modules configured for
					%% the space ahead (the data may be rearranged during after a reorg).
					is_offset_vicinity_covered(AbsoluteEndOffset);
				false ->
					ar_storage_module:has_any(AbsoluteEndOffset)
			end
	end.

is_offset_vicinity_covered(Offset) ->
	Size = ar_node:get_recent_max_block_size(),
	ar_storage_module:has_range(max(0, Offset - Size * 2), Offset + Size * 2).

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ets:insert(ar_disk_pool_data_roots, {Key,
					{0, os:system_time(microsecond), sets:from_list([TXID])}});
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots,
					{Key, {Size, Timestamp, sets:add_element(TXID, TXIDSet)}})
	end,
	ok.

%% @doc Notify the server the given data root has been removed from the mempool.
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ok;
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDs}}] ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					ok;
				TXIDs2 ->
					case sets:size(TXIDs2) of
						0 ->
							ets:delete(ar_disk_pool_data_roots, Key);
						_ ->
							ets:insert(ar_disk_pool_data_roots, {Key,
									{Size, Timestamp, TXIDs2}})
					end
			end
	end,
	ok.

%% @doc Fetch the chunk corresponding to Offset. When Offset is less than or equal to
%% the strict split data threshold, the chunk returned contains the byte with the given
%% Offset (the indexing is 1-based). Otherwise, the chunk returned ends in the same 256 KiB
%% bucket as Offset counting from the first 256 KiB after the strict split data threshold.
%% The strict split data threshold is weave_size of the block preceding the fork 2.5 block.
%%
%% Options:
%%	_________________________________________________________________________________________
%%	packing				| required; spora_2_5 or unpacked or {spora_2_6, <Mining Address>}
%%							or {composite, <Mining Address>, <Difficulty>}
%%							or {replica_2_9, <Mining Address>}
%%	_________________________________________________________________________________________
%%	pack				| if false and a packed chunk is requested but stored unpacked or
%%						| an unpacked chunk is requested but stored packed, return
%%						| {error, chunk_not_found} instead of packing/unpacking;
%%						| true by default;
%%	_________________________________________________________________________________________
%%	bucket_based_offset	| does not play a role for the offsets before
%%						| strict_data_split_threshold (weave_size of the block preceding
%%						| the fork 2.5 block); if true, return the chunk which ends in
%%						| the same 256 KiB bucket starting from
%%						| strict_data_split_threshold where borders belong to the
%%						| buckets on the left; true by default.
get_chunk(Offset, #{ packing := Packing } = Options) ->
	Pack = maps:get(pack, Options, true),
	RequestOrigin = maps:get(origin, Options, unknown),
	IsRecorded =
		case {RequestOrigin, Pack} of
			{miner, _} ->
				StorageModules = ar_storage_module:get_all(Offset),
				ar_sync_record:is_recorded_any(Offset, ar_data_sync, StorageModules);
			{_, false} ->
				ar_sync_record:is_recorded(Offset, {ar_data_sync, Packing});
			{_, true} ->
				ar_sync_record:is_recorded(Offset, ar_data_sync)
		end,
	SeekOffset =
		case maps:get(bucket_based_offset, Options, true) of
			true ->
				ar_chunk_storage:get_chunk_seek_offset(Offset);
			false ->
				Offset
		end,
	case IsRecorded of
		{{true, StoredPacking}, StoreID} ->
			get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
				RequestOrigin);
		{true, StoreID} ->
			UnpackedReply = ar_sync_record:is_recorded(Offset, {ar_data_sync, unpacked}),
			log_chunk_error(RequestOrigin, chunk_record_not_associated_with_packing,
					[{store_id, StoreID}, {seek_offset, SeekOffset},
					{is_recorded_unpacked, io_lib:format("~p", [UnpackedReply])}]),
			{error, chunk_not_found};
		Reply ->
			UnpackedReply = ar_sync_record:is_recorded(Offset, {ar_data_sync, unpacked}),
			Modules = ar_storage_module:get_all(Offset),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			RootRecords = [ets:lookup(sync_records, {ar_data_sync, ID})
					|| ID <- ModuleIDs],
			case RequestOrigin of
				miner ->
					log_chunk_error(RequestOrigin, chunk_record_not_found,
							[{modules_covering_offset, ModuleIDs},
							{root_sync_records, RootRecords},
							{seek_offset, SeekOffset},
							{reply, io_lib:format("~p", [Reply])},
							{is_recorded_unpacked, io_lib:format("~p", [UnpackedReply])}]);
				_ ->
					ok
			end,
			{error, chunk_not_found}
	end.

%% @doc Fetch the merkle proofs for the chunk corresponding to Offset.
get_chunk_proof(Offset, Options) ->
	RequestOrigin = maps:get(origin, Options, unknown),
	IsRecorded = ar_sync_record:is_recorded(Offset, ar_data_sync),
	SeekOffset =
		case maps:get(bucket_based_offset, Options, true) of
			true ->
				ar_chunk_storage:get_chunk_seek_offset(Offset);
			false ->
				Offset
		end,
	case IsRecorded of
		{{true, StoredPacking}, StoreID} ->
			get_chunk_proof(Offset, SeekOffset, StoredPacking, StoreID, RequestOrigin);
		_ ->
			{error, chunk_not_found}
	end.

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than ?MAX_SERVED_TX_DATA_SIZE, unless the limitation
%% is disabled in the configuration.
get_tx_data(TXID) ->
	{ok, Config} = arweave_config:get_env(),
	SizeLimit =
		case lists:member(serve_tx_data_without_limits, Config#config.enable) of
			true ->
				infinity;
			false ->
				?MAX_SERVED_TX_DATA_SIZE
		end,
	get_tx_data(TXID, SizeLimit).

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than SizeLimit.
get_tx_data(TXID, SizeLimit) ->
	case get_tx_offset(TXID) of
		{error, not_found} ->
			{error, not_found};
		{error, failed_to_read_tx_offset} ->
			{error, failed_to_read_tx_data};
		{ok, {Offset, Size}} ->
			case Size > SizeLimit of
				true ->
					{error, tx_data_too_big};
				false ->
					{ok, Config} = arweave_config:get_env(),
					Pack = lists:member(pack_served_chunks, Config#config.enable),
					get_tx_data(Offset - Size, Offset, [], Pack)
			end
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	TXIndex = {tx_index, ?DEFAULT_MODULE},
	get_tx_offset(TXIndex, TXID).

%% @doc Return {ok, [{TXID, AbsoluteStartOffset, AbsoluteEndOffset}, ...]}
%% where AbsoluteStartOffset, AbsoluteEndOffset are transaction borders
%% (not clipped by the given range) for all TXIDs intersecting the given range.
get_tx_offset_data_in_range(Start, End) ->
	TXIndex = {tx_index, ?DEFAULT_MODULE},
	TXOffsetIndex = {tx_offset_index, ?DEFAULT_MODULE},
	get_tx_offset_data_in_range(TXOffsetIndex, TXIndex, Start, End).

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool
%% or in the index.
has_data_root(DataRoot, DataSize) ->
	DataRootKey = << DataRoot:32/binary, DataSize:256 >>,
	case ets:member(ar_disk_pool_data_roots, DataRootKey) of
		true ->
			true;
		false ->
			case get_data_root_offset(DataRootKey, ?DEFAULT_MODULE) of
				{ok, _} ->
					true;
				_ ->
					false
			end
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:call(ar_data_sync_default, {add_block, B, SizeTaggedTXs}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID, Ref, ReplyTo) ->
	TXIndex = {tx_index, ?DEFAULT_MODULE},
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			remove_range(End - Size, End, Ref, ReplyTo);
		not_found ->
			?LOG_WARNING([{event, tx_offset_not_found}, {tx, ar_util:encode(TXID)}]),
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_fetch_blacklisted_tx_offset},
					{tx, ar_util:encode(TXID)}, {reason, Reason}]),
			ok
	end.

%% @doc Request the removal of the given byte range.
request_data_removal(Start, End, Ref, ReplyTo) ->
	remove_range(Start, End, Ref, ReplyTo).

%% @doc Return true if the in-memory data chunk cache is full. Return not_initialized
%% if there is no information yet.
is_chunk_cache_full() ->
	case ets:lookup(ar_data_sync_state, chunk_cache_size_limit) of
		[{_, Limit}] ->
			case ets:lookup(ar_data_sync_state, chunk_cache_size) of
				[{_, Size}] when Size > Limit ->
					true;
				_ ->
					false
			end;
		_ ->
			not_initialized
	end.

-ifdef(AR_TEST).
is_disk_space_sufficient(StoreID) ->
	%% When testing, disk space is always sufficient *unless* the storage module has not
	%% been properly initialized.
	case is_disk_space_sufficient2(StoreID) of
		not_initialized ->
			not_initialized;
		_ ->
			true
	end.
-else.
%% @doc Return true if we have sufficient disk space to write new data for the
%% given StoreID. Return not_initialized if there is no information yet.
is_disk_space_sufficient(StoreID) ->
	is_disk_space_sufficient2(StoreID).
-endif.

%% @doc Return true if we have sufficient disk space to write new data for the
%% given StoreID. Return not_initialized if there is no information yet.
is_disk_space_sufficient2(StoreID) ->
	case ets:lookup(ar_data_sync_state, {is_disk_space_sufficient, StoreID}) of
		[{_, false}] ->
			false;
		[{_, true}] ->
			true;
		_ ->
			not_initialized
	end.

get_chunk_by_byte(Byte, StoreID) ->
	Result = ar_kv:get_next_by_prefix({chunks_index, StoreID}, ?OFFSET_KEY_PREFIX_BITSIZE,
		?OFFSET_KEY_BITSIZE, << Byte:?OFFSET_KEY_BITSIZE >>),
	case Result of
		{error, Reason} ->
			{error, Reason};
		{ok, Key, Metadata} ->
			<< AbsoluteOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{
				ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize
			} = binary_to_term(Metadata),
			FullMetaData = {AbsoluteOffset, ChunkDataKey, TXRoot, DataRoot, TXPath,
				RelativeOffset, ChunkSize},
			{ok, Key, FullMetaData}
	end.

%% @doc: handle situation where get_chunks_by_byte returns invalid_iterator, so we can't
%% use the chunk's end offset to advance the cursor.
%%
%% get_chunk_by_byte looks for a key with the same prefix or the next prefix.
%% Therefore, if there is no such key, it does not make sense to look for any
%% key smaller than the prefix + 2 in the next iteration.
advance_chunks_index_cursor(Cursor) ->
	PrefixSpaceSize = trunc(math:pow(2, ?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
	((Cursor div PrefixSpaceSize) + 2) * PrefixSpaceSize.

read_chunk(Offset, ChunkDataKey, StoreID) ->
	case get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{Chunk, DataPath} ->
					{ok, {Chunk, DataPath}};
				DataPath ->
					case ar_chunk_storage:get(Offset - 1, StoreID) of
						not_found ->
							not_found;
						{_EndOffset, Chunk} ->
							{ok, {Chunk, DataPath}}
					end
			end;
		Error ->
			Error
	end.

write_chunk(Offset, ChunkMetadata, Chunk, Packing, StoreID) ->
	#chunk_metadata{
		chunk_data_key = ChunkDataKey,
		chunk_size = ChunkSize,
		data_path = DataPath
	} = ChunkMetadata,
	write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, StoreID).

read_data_path(ChunkDataKey, StoreID) ->
	read_data_path(undefined, ChunkDataKey, StoreID).
%% The first argument is introduced to match the read_chunk/3 signature.
read_data_path(_Offset, ChunkDataKey, StoreID) ->
	case get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{_Chunk, DataPath} ->
					{ok, DataPath};
				DataPath ->
					{ok, DataPath}
			end;
		Error ->
			Error
	end.

decrement_chunk_cache_size() ->
	ets:update_counter(ar_data_sync_state, chunk_cache_size, {2, -1}, {chunk_cache_size, 0}).

increment_chunk_cache_size() ->
	ets:update_counter(ar_data_sync_state, chunk_cache_size, {2, 1}, {chunk_cache_size, 1}).

debug_get_disk_pool_chunks() ->
	debug_get_disk_pool_chunks(first).

debug_get_disk_pool_chunks(Cursor) ->
	case ar_kv:get_next({disk_pool_chunks_index, ?DEFAULT_MODULE}, Cursor) of
		none ->
			[];
		{ok, K, V} ->
			K2 = << K/binary, <<"a">>/binary >>,
			[{K, V} | debug_get_disk_pool_chunks(K2)]
	end.

%% @doc Check if the footprint record should be updated for the given chunk.
%% The footprint record stores multiples of ?DATA_CHUNK_SIZE.
is_footprint_record_supported(AbsoluteOffset, ChunkSize, Packing) ->
	IsMatchingPacking =
		case Packing of
			{replica_2_9, _Addr} ->
				true;
			unpacked_padded ->
				true;
			_ ->
				false
	end,
	IsMatchingPacking andalso (AbsoluteOffset > ar_block:strict_data_split_threshold() orelse ChunkSize == ?DATA_CHUNK_SIZE).

%% @doc Return the disk pool threshold, a byte offset where
%% the disk pool begins - the data above this offset is considered
%% to belong to the disk pool. For example, we do not store the
%% disk pool data in the storage modules due to the risk of orphans.
get_disk_pool_threshold() ->
	case ets:lookup(ar_data_sync_state, disk_pool_threshold) of
		[] ->
			0;
		[{_, DiskPoolThreshold}] ->
			DiskPoolThreshold
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({?DEFAULT_MODULE = StoreID, _}) ->
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	{ok, Config} = arweave_config:get_env(),
	[ok, ok] = ar_events:subscribe([node_state, disksup]),
	State = init_kv(StoreID),
	move_disk_pool_index(State),
	move_data_root_index(State),
	{ok, _} = ar_timer:apply_interval(
		?RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS,
		ar_data_sync,
		record_disk_pool_chunks_count,
		[],
		#{ skip_on_shutdown => false }
	),

	StateMap = read_data_sync_state(),
	CurrentBI = maps:get(block_index, StateMap),
	%% Maintain a map of pending, recently uploaded, and orphaned data roots.
	%% << DataRoot:32/binary, TXSize:256 >> => {Size, Timestamp, TXIDSet}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% New chunks for these data roots are accepted until the corresponding size reaches
	%% #config.max_disk_pool_data_root_buffer_mb or the total size of added pending and
	%% seeded chunks reaches #config.max_disk_pool_buffer_mb. When a data root is orphaned,
	%% its timestamp is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they do not belong to any storage module - from storage.
	%% TXIDSet keeps track of pending transaction identifiers - if all pending transactions
	%% with the << DataRoot:32/binary, TXSize:256 >> key are dropped from the mempool,
	%% the corresponding entry is removed from DiskPoolDataRoots. When a data root is
	%% confirmed, TXIDSet is set to not_set - from this point on, the key is only dropped
	%% after expiration.
	DiskPoolDataRoots = maps:get(disk_pool_data_roots, StateMap),
	recalculate_disk_pool_size(DiskPoolDataRoots, State),
	DiskPoolThreshold = maps:get(disk_pool_threshold, StateMap),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = State#sync_data_state{
		block_index = CurrentBI,
		weave_size = maps:get(weave_size, StateMap),
		disk_pool_cursor = first,
		disk_pool_threshold = DiskPoolThreshold,
		store_id = StoreID,
		sync_status = init_sync_status(StoreID)
	},
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID},
		{range_start, State2#sync_data_state.range_start},
		{range_end, State2#sync_data_state.range_end}]),
	{ok, _} = ar_timer:apply_interval(
		?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS,
		?MODULE,
		remove_expired_disk_pool_data_roots,
		[],
		#{ skip_on_shutdown => false }
	),
	lists:foreach(
		fun(_DiskPoolJobNumber) ->
			gen_server:cast(self(), process_disk_pool_item)
		end,
		lists:seq(1, Config#config.disk_pool_jobs)
	),
	gen_server:cast(self(), store_sync_state),
	{ok, Config} = arweave_config:get_env(),
	Limit =
		case Config#config.data_cache_size_limit of
			undefined ->
				Free = proplists:get_value(free_memory, memsup:get_system_memory_data(),
						2000000000),
				Limit2 = min(1000, erlang:ceil(Free * 0.9 / 3 / 262144)),
				Limit3 = ar_util:ceil_int(Limit2, 100),
				Limit3;
			Limit2 ->
				Limit2
		end,
	ar:console("~nSetting the data chunk cache size limit to ~B chunks.~n", [Limit]),
	ets:insert(ar_data_sync_state, {chunk_cache_size_limit, Limit}),
	ets:insert(ar_data_sync_state, {chunk_cache_size, 0}),
	{ok, _} = ar_timer:apply_interval(
		200,
		?MODULE,
		record_chunk_cache_size_metric,
		[],
		#{ skip_on_shutdown => false }
	),
	gen_server:cast(self(), process_store_chunk_queue),
	{ok, State2};
init({StoreID, RepackInPlacePacking}) ->
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe([node_state, disksup]),

	State = init_kv(StoreID),

	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	RangeStart2 = max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
	RangeEnd2 = ar_block:get_chunk_padded_offset(RangeEnd),
	State2 = State#sync_data_state{
		store_id = StoreID,
		range_start = RangeStart2,
		range_end = RangeEnd2,
		%% weave_size and disk_pool_threshold will be set on join
		weave_size = 0,
		disk_pool_threshold = 0
	},

	case RepackInPlacePacking of
		none ->
			gen_server:cast(self(), process_store_chunk_queue),
			State3 = State2#sync_data_state{
				sync_status = init_sync_status(StoreID)
			},
			%% Start syncing immediately. For replica_2_9 packing, chunks will be
			%% written as unpacked_padded first and upgraded once entropy arrives.
			gen_server:cast(self(), sync_intervals),
			gen_server:cast(self(), sync_data),
			maybe_run_footprint_record_initialization(State3),
			{ok, State3};
		_ ->
			State3 = State2#sync_data_state{
				sync_status = off
			},
			ar_device_lock:set_device_lock_metric(StoreID, sync, off),
			{ok, State3}
	end.

handle_cast({move_data_root_index, Cursor, N}, State) ->
	move_data_root_index(Cursor, N, State),
	{noreply, State};

handle_cast({store_data_roots, BlockStart, BlockEnd, TXRoot, Entries}, State) ->
	#sync_data_state{ store_id = ?DEFAULT_MODULE } = State,
	DataRootIndexKeySet = sets:from_list([
		<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>
		|| {DataRoot, TXSize, _TXStart, _TXPath} <- Entries
	]),
	BlockSize = BlockEnd - BlockStart,
	lists:foreach(
		fun({DataRoot, TXSize, TXStartOffset, TXPath}) ->
			ok = update_data_root_index(DataRoot, TXSize, TXStartOffset, TXPath, ?DEFAULT_MODULE)
		end,
		Entries
	),
	ok = ar_kv:put({data_root_offset_index, ?DEFAULT_MODULE},
			<< BlockStart:?OFFSET_KEY_BITSIZE >>,
			term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})),
	{noreply, State};

handle_cast(process_store_chunk_queue, State) ->
	ar_util:cast_after(200, self(), process_store_chunk_queue),
	{noreply, process_store_chunk_queue(State)};

handle_cast({initialize_footprint_record, Cursor, Packing}, State) ->
	State2 = initialize_footprint_record(Cursor, Packing, State),
	{noreply, State2};

handle_cast({join, RecentBI}, State) ->
	#sync_data_state{ block_index = CurrentBI, store_id = StoreID } = State,
	[{_, WeaveSize, _} | _] = RecentBI,
	case {CurrentBI, ar_block_index:get_intersection(CurrentBI)} of
		{[], _} ->
			ok;
		{_, no_intersection} ->
			io:format("~nWARNING: the stored block index of the data syncing module "
					"has no intersection with the new one "
					"in the most recent blocks. If you have just started a new weave using "
					"the init option, restart from the local state "
					"or specify some peers.~n~n"),
			init:stop(1);
		{_, {_H, Offset, _TXRoot}} ->
			PreviousWeaveSize = element(2, hd(CurrentBI)),
			{ok, OrphanedDataRoots} = remove_orphaned_data(State, Offset, PreviousWeaveSize),
			{ok, Config} = arweave_config:get_env(),
			[gen_server:cast(name(ar_storage_module:id(Module)),
					{cut, Offset}) || Module <- Config#config.storage_modules],
			ok = ar_chunk_storage:cut(Offset, StoreID),
			ok = ar_sync_record:cut(Offset, ar_data_sync, StoreID),
			ar_events:send(sync_record, {global_cut, Offset}),
			reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots)
	end,
	BI = ar_block_index:get_list_by_hash(element(1, lists:last(RecentBI))),
	repair_data_root_offset_index(BI, State),
	DiskPoolThreshold = get_disk_pool_threshold(RecentBI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = store_sync_state(
		State#sync_data_state{
			weave_size = WeaveSize,
			block_index = RecentBI,
			disk_pool_threshold = DiskPoolThreshold
		}),
	{noreply, State2};

handle_cast({cut, Start}, #sync_data_state{ store_id = StoreID,
		range_end = End } = State) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ar_data_sync, StoreID) of
		not_found ->
			ok;
		_Interval ->
			{ok, Config} = arweave_config:get_env(),
			case lists:member(remove_orphaned_storage_module_data, Config#config.enable) of
				false ->
					ar:console("The storage module ~s contains some orphaned data above the "
							"weave offset ~B. Make sure you are joining the network through "
							"trusted in-sync peers and restart with "
							"`enable remove_orphaned_storage_module_data`.~n",
							[StoreID, Start]),
					timer:sleep(2000),
					init:stop(1);
				true ->
					ok = delete_chunk_metadata_range(Start, End, State),
					ok = ar_chunk_storage:cut(Start, StoreID),
					ok = ar_sync_record:cut(Start, ar_data_sync, StoreID)
			end
	end,
	{noreply, State};

handle_cast({add_tip_block, BlockTXPairs, BI}, State) ->
	#sync_data_state{ store_id = StoreID, weave_size = CurrentWeaveSize,
			block_index = CurrentBI } = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{WeaveSize, AddedDataRoots} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots}) ->
				{ok, DataRoots} = add_block_data_roots(SizeTaggedTXs, StartOffset, StoreID),
				ok = update_tx_index(SizeTaggedTXs, StartOffset, StoreID),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots)}
		end,
		{BlockStartOffset, sets:new()},
		Blocks
	),
	add_block_data_roots_to_disk_pool(AddedDataRoots),
	reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots),
	ok = ar_chunk_storage:cut(BlockStartOffset, StoreID),
	ok = ar_sync_record:cut(BlockStartOffset, ar_data_sync, StoreID),
	ar_events:send(sync_record, {global_cut, BlockStartOffset}),
	DiskPoolThreshold = get_disk_pool_threshold(BI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = store_sync_state(
		State#sync_data_state{
			weave_size = WeaveSize,
			block_index = BI,
			disk_pool_threshold = DiskPoolThreshold
		}),
	{noreply, State2};

handle_cast(sync_data, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	Status = ar_device_lock:acquire_lock(sync, StoreID, State#sync_data_state.sync_status),
	State2 = State#sync_data_state{ sync_status = Status },
	State3 = case Status of
		active ->
			do_sync_data(State2);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), sync_data),
			State2;
		_ ->
			State2
	end,
	{noreply, State3};

handle_cast(sync_data2, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	Status = ar_device_lock:acquire_lock(sync, StoreID, State#sync_data_state.sync_status),
	State2 = State#sync_data_state{ sync_status = Status },
	State3 = case Status of
		active ->
			do_sync_data2(State2);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), sync_data2),
			State2;
		_ ->
			State2
	end,
	{noreply, State3};

%% Schedule syncing of the unsynced intervals. Choose a peer for each of the intervals.
%% There are two message payloads:
%% 1. collect_peer_intervals
%%    Start the collection process over the full storage_module range.
%% 2. {collect_peer_intervals, Start, End}
%%    Collect intervals for the specified range. This interface is used to pick up where
%%    we left off after a pause. There are 2 main conditions that can trigger a pause:
%%    a. Insufficient disk space. Will pause until disk space frees up
%%    b. Sync queue is busy. Will pause until previously queued intervals are scheduled to the
%%       ar_data_sync_worker_master for syncing.
handle_cast(collect_peer_intervals, State) ->
	#sync_data_state{ range_start = Start, range_end = End,
			disk_pool_threshold = DiskPoolThreshold,
			sync_phase = SyncPhase,
			migrations_index = MI } = State,
	?LOG_DEBUG([{event, collect_peer_intervals_start},
		{function, collect_peer_intervals},
		{store_id, State#sync_data_state.store_id},
		{s, Start}, {e, End}]),
	CheckIsJoined =
		case ar_node:is_joined() of
			false ->
				ar_util:cast_after(1000, self(), collect_peer_intervals),
				false;
			true ->
				true
		end,
	IsFootprintRecordMigrated =
		case ar_kv:get(MI, ?FOOTPRINT_MIGRATION_CURSOR_KEY) of
			{ok, <<"complete">>} ->
				true;
			_ ->
				ar_util:cast_after(5_000, self(), collect_peer_intervals),
				false
		end,
	IntersectsDiskPool =
		case CheckIsJoined andalso IsFootprintRecordMigrated of
			false ->
				noop;
			true ->
				End > DiskPoolThreshold
		end,
	%% Alternate between "normal" and footprint-based syncing.
	%% Footprint-based syncing downloads replica 2.9 chunks footprint by footprint
	%% to avoid redundant entropy generations for unpacking. "Normal" syncing ignores
	%% replica 2.9 data and mostly downloads unpacked data from peers storing it.
	SyncPhase2 =
		case SyncPhase of
			undefined ->
				%% Start with normal syncing.
				normal;
			normal ->
				footprint;
			footprint ->
				normal
		end,
	State2 =
		case IntersectsDiskPool of
			noop ->
				State;
			true ->
				case SyncPhase2 of
					footprint ->
						End2 = min(End, DiskPoolThreshold),
						gen_server:cast(self(), {collect_peer_intervals, Start, End2, footprint}),
						State#sync_data_state{ sync_phase = footprint };
					_ ->
						%% The disk pool is only synced during the "normal" phase.
						gen_server:cast(self(), {collect_peer_intervals, Start, End, normal}),
						State#sync_data_state{ sync_phase = normal }
				end;
			false ->
				gen_server:cast(self(), {collect_peer_intervals, Start, End, SyncPhase2}),
				State#sync_data_state{ sync_phase = SyncPhase2 }
		end,
	{noreply, State2};

handle_cast({collect_peer_intervals, Start, End, Type}, State) when Start >= End ->
	%% We've finished collecting intervals for the whole storage_module range. Schedule
	%% the collection process to restart in ?COLLECT_SYNC_INTERVALS_FREQUENCY_MS.
	?LOG_DEBUG([{event, collect_peer_intervals_done},
		{function, collect_peer_intervals},
		{store_id, State#sync_data_state.store_id},
		{s, Start}, {e, End}, {type, Type}]),
	ar_util:cast_after(?COLLECT_SYNC_INTERVALS_FREQUENCY_MS, self(), collect_peer_intervals),
	{noreply, State};
handle_cast({collect_peer_intervals, Start, End, Type}, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			store_id = StoreID, weave_size = WeaveSize } = State,
	IsDiskSpaceSufficient =
		case is_disk_space_sufficient(StoreID) of
			true ->
				true;
			IsSufficient ->
				Delay = case IsSufficient of
					false ->
						30_000;
					not_initialized ->
						1000
				end,
				ar_util:cast_after(Delay, self(), {collect_peer_intervals, Start, End, Type}),
				false
		end,
	IsSyncQueueBusy =
		case IsDiskSpaceSufficient of
			false ->
				true;
			true ->
				%% Q contains chunks we've already queued for syncing. We need
				%% to manage the queue length.
				%% 1. Periodically sync_intervals will pull from Q and send work to
				%%    ar_data_sync_worker_master. We need to make sure Q is long enough so
				%%    that we never starve ar_data_sync_worker_master of work.
				%% 2. On the flip side we don't want Q to get so long as to trigger an
				%%    out-of-memory condition. In the extreme case we could collect and
				%%    enqueue all chunks in the entire storage module (usually 3.6 TB).
				%%    A Q of this length would have a roughly 500 MB memory footprint per
				%%    storage module. For a node that is syncing multiple storage modules,
				%%    this can add up fast.
				%% 3. We also want to make sure we are using the most up to date information
				%%    we can. Every time we add a task to the Q we're locking in a specific
				%%    view of Peer data availability. If that peer goes offline before we
				%%    get to the task it can result in wasted work or syncing stalls. A
				%%    shorter queue helps ensure we're always syncing from the "best" peers
				%%    at any point in time.
				%%
				%% With all that in mind, we'll pause collection once the Q hits roughly
				%% a bucket size worth of chunks. This number is slightly arbitrary and we
				%% should feel free to adjust as necessary.
				IntervalsQueueSize = gb_sets:size(Q),
				StoreIDLabel = ar_storage_module:label(StoreID),
				prometheus_gauge:set(sync_intervals_queue_size,
					[StoreIDLabel], IntervalsQueueSize),
				case IntervalsQueueSize > (?NETWORK_DATA_BUCKET_SIZE / ?DATA_CHUNK_SIZE) of
					true ->
						ar_util:cast_after(500, self(), {collect_peer_intervals, Start, End, Type}),
						true;
					false ->
						false
				end
		end,
	case IsSyncQueueBusy of
		true ->
			?LOG_DEBUG([{event, collect_peer_intervals_skipped},
					{function, collect_peer_intervals},
					{store_id, StoreID},
					{s, Start}, {e, End},
					{weave_size, WeaveSize},
					{is_disk_space_sufficient, IsDiskSpaceSufficient},
					{is_sync_queue_busy, IsSyncQueueBusy}]),
			ok;
		false ->
			End2 = min(End, WeaveSize),
			case Start >= End2 of
				true ->
					ar_util:cast_after(500, self(), {collect_peer_intervals, Start, End, Type});
				false ->
					%% All checks have passed, find and enqueue intervals for one
					%% sync bucket worth of chunks starting at offset Start.
					Footprint =
						case Type of
							footprint ->
								ar_footprint_record:get_footprint(Start + ?DATA_CHUNK_SIZE);
							_ ->
								ignore
						end,
					?LOG_DEBUG([{event, fetch_peer_intervals},
							{function, collect_peer_intervals},
							{store_id, StoreID},
							{s, Start}, {e, End2},
							{type, Type},
							{footprint, Footprint}]),
					ar_peer_intervals:fetch(Start, End2, StoreID, Type)
			end
	end,

	{noreply, State};

handle_cast({enqueue_intervals, []}, State) ->
	{noreply, State};
handle_cast({enqueue_intervals, Intervals}, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals } = State,
	%% When enqueuing intervals, we want to distribute the intervals among many peers,
	%% so that:
	%% 1. We can better saturate our network bandwidth without overwhelming any one peer.
	%% 2. So that we limit the risk of blocking on one particularly slow peer.
	%%
	%% We do a probabilistic distribution:
	%% 1. We shuffle the peers list so that the ordering differs from call to call
	%% 2. We cap the number of chunks to enqueue per peer - at roughly 50% more than
	%%    their "fair" share (i.e. ?DEFAULT_SYNC_BUCKET_SIZE / NumPeers).
	%%
	%% The compute overhead of these 2 steps is minimal and results in a pretty good
	%% distribution of sync requests among peers.

	%% This is an approximation. The intent is to enqueue one sync_bucket at a time - but
	%% due to the selection of each peer's intervals, the total number of bytes may be
	%% less than a full sync_bucket. But for the purposes of distributing requests among
	%% many peers - the approximation is fine (and much cheaper to calculate than taking
	%% the sum of all the peer intervals).
	TotalChunksToEnqueue = ?DEFAULT_SYNC_BUCKET_SIZE div ?DATA_CHUNK_SIZE,
	NumPeers = length(Intervals),
	%% Allow each Peer to sync slightly more chunks than their strict share - this allows
	%% us to more reliably sync the full set of requested intervals.
	ScalingFactor = 1.5,
	ChunksPerPeer = trunc(((TotalChunksToEnqueue + NumPeers - 1) div NumPeers) * ScalingFactor),

	{Q2, QIntervals2} = enqueue_intervals(
		ar_util:shuffle_list(Intervals), ChunksPerPeer, {Q, QIntervals}),

	?LOG_DEBUG([{event, enqueue_intervals}, {pid, self()},
		{queue_before, gb_sets:size(Q)}, {queue_after, gb_sets:size(Q2)},
		{num_peers, NumPeers}, {chunks_per_peer, ChunksPerPeer},
		{intervals, Intervals}, {q_intervals_before, ar_intervals:to_list(QIntervals)},
		{q_intervals_after, ar_intervals:to_list(QIntervals2)}]),

	{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
			sync_intervals_queue_intervals = QIntervals2 }};

handle_cast(sync_intervals, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	Status = ar_device_lock:acquire_lock(sync, StoreID, State#sync_data_state.sync_status),
	State2 = State#sync_data_state{ sync_status = Status },
	State3 = case Status of
		active ->
			do_sync_intervals(State2);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), sync_intervals),
			State2;
		_ ->
			State2
	end,
	{noreply, State3};

handle_cast({invalidate_bad_data_record, Args}, State) ->
	invalidate_bad_data_record(Args),
	{noreply, State};

handle_cast({pack_and_store_chunk, Args} = Cast,
			#sync_data_state{ store_id = StoreID } = State) ->
	case is_disk_space_sufficient(StoreID) of
		true ->
			pack_and_store_chunk(Args, State);
		_ ->
			ar_util:cast_after(30000, self(), Cast),
			{noreply, State}
	end;

handle_cast({store_fetched_chunk, Peer, Byte, Proof} = Cast, State) ->
	{store_fetched_chunk, Peer, Byte, Proof} = Cast,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	SeekByte = ar_chunk_storage:get_chunk_seek_offset(Byte + 1) - 1,
	case validate_proof(SeekByte, Proof) of
		{need_unpacking, AbsoluteEndOffset, ChunkProof2} ->
			#chunk_proof{
				block_start_offset = BlockStartOffset,
				tx_start_offset = TXStartOffset,
				tx_end_offset = TXEndOffset,
				chunk_end_offset = ChunkEndOffset,
				chunk_id = ChunkID,
				metadata = #chunk_metadata{
					tx_root = TXRoot,
					data_root = DataRoot,
					chunk_size = ChunkSize
				}
			} = ChunkProof2,
			TXSize = TXEndOffset - TXStartOffset,
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			ChunkArgs = {Packing, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Peer, Byte},
			unpack_fetched_chunk(Cast, AbsoluteEndOffset, ChunkArgs, Args, State);
		false ->
			decrement_chunk_cache_size(),
			process_invalid_fetched_chunk(Peer, Byte, State);
		{true, ChunkProof2} ->
			#chunk_proof{
				block_start_offset = BlockStartOffset,
				tx_start_offset = TXStartOffset,
				tx_end_offset = TXEndOffset,
				chunk_end_offset = ChunkEndOffset,
				chunk_id = ChunkID,
				metadata = #chunk_metadata{
					tx_root = TXRoot,
					data_root = DataRoot,
					chunk_size = ChunkSize
				}
			} = ChunkProof2,
			TXSize = TXEndOffset - TXStartOffset,
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			AbsoluteEndOffset = AbsoluteTXStartOffset + ChunkEndOffset,
			ChunkArgs = {unpacked, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Peer, Byte},
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end;

handle_cast(process_disk_pool_item, #sync_data_state{ disk_pool_scan_pause = true } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{ disk_pool_cursor = Cursor, disk_pool_chunks_index = DiskPoolChunksIndex,
			disk_pool_full_scan_start_key = FullScanStartKey,
			disk_pool_full_scan_start_timestamp = Timestamp,
			currently_processed_disk_pool_keys = CurrentlyProcessedDiskPoolKeys } = State,
	NextKey =
		case ar_kv:get_next(DiskPoolChunksIndex, Cursor) of
			{ok, Key1, Value1} ->
				case sets:is_element(Key1, CurrentlyProcessedDiskPoolKeys) of
					true ->
						none;
					false ->
						{ok, Key1, Value1}
				end;
			none ->
				case ar_kv:get_next(DiskPoolChunksIndex, first) of
					none ->
						none;
					{ok, Key2, Value2} ->
						case sets:is_element(Key2, CurrentlyProcessedDiskPoolKeys) of
							true ->
								none;
							false ->
								{ok, Key2, Value2}
						end
				end
		end,
	case NextKey of
		none ->
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), resume_disk_pool_scan),
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = first,
					disk_pool_full_scan_start_key = none, disk_pool_scan_pause = true }};
		{ok, Key3, Value3} ->
			case FullScanStartKey of
				none ->
					process_disk_pool_item(State#sync_data_state{
							disk_pool_full_scan_start_key = Key3,
							disk_pool_full_scan_start_timestamp = erlang:timestamp() },
							Key3, Value3);
				Key3 ->
					TimePassed = timer:now_diff(erlang:timestamp(), Timestamp),
					case TimePassed < (?DISK_POOL_SCAN_DELAY_MS) * 1000 of
						true ->
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									resume_disk_pool_scan),
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									process_disk_pool_item),
							{noreply, State#sync_data_state{ disk_pool_cursor = first,
									disk_pool_full_scan_start_key = none,
									disk_pool_scan_pause = true }};
						false ->
							process_disk_pool_item(State, Key3, Value3)
					end;
				_ ->
					process_disk_pool_item(State, Key3, Value3)
			end
	end;

handle_cast(resume_disk_pool_scan, State) ->
	{noreply, State#sync_data_state{ disk_pool_scan_pause = false }};

handle_cast({process_disk_pool_chunk_offsets, Iterator, MayConclude, Args}, State) ->
	{Offset, _, _, _, _, _, Key, _, _, _} = Args,
	%% Place the chunk under its last 10 offsets in the weave (the same data
	%% may be uploaded several times).
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			State2 =
				case MayConclude of
					true ->
						Iterator2 = data_root_index_reset(Iterator),
						delete_disk_pool_chunk(Iterator2, Args, State),
						maybe_reset_disk_pool_full_scan_key(Key, State);
					false ->
						State
				end,
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, deregister_currently_processed_disk_pool_key(Key, State2)};
		{TXArgs, Iterator2} ->
			State2 = register_currently_processed_disk_pool_key(Key, State),
			{TXStartOffset, TXRoot, TXPath} = TXArgs,
			AbsoluteOffset = TXStartOffset + Offset,
			process_disk_pool_chunk_offset(Iterator2, TXRoot, TXPath, AbsoluteOffset,
					MayConclude, Args, State2)
	end;

handle_cast({remove_range, End, Cursor, Ref, PID}, State) when Cursor > End ->
	PID ! {removed_range, Ref},
	{noreply, State};
handle_cast({remove_range, End, Cursor, Ref, PID}, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	case get_chunk_by_byte(Cursor, StoreID) of
		{ok, _Key, {AbsoluteOffset, _, _, _, _, _, _}}
				when AbsoluteOffset > End ->
			PID ! {removed_range, Ref},
			{noreply, State};
		{ok, _Key, {AbsoluteOffset, _, _, _, _, _, ChunkSize}} ->
			PaddedStartOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
			PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
			%% 1) store updated sync record
			%% 2) remove chunk
			%% 3) update chunks_index
			%%
			%% The order is important - in case the VM crashes,
			%% we will not report false positives to peers,
			%% and the chunk can still be removed upon retry.
			RemoveFromFootprint = ar_footprint_record:delete(PaddedOffset, StoreID),
			RemoveFromSyncRecord =
				case RemoveFromFootprint of
					ok ->
						ar_sync_record:delete(PaddedOffset,
								PaddedStartOffset, ar_data_sync, StoreID);
					Error ->
						Error
				end,
			RemoveFromChunkStorage =
				case RemoveFromSyncRecord of
					ok ->
						ar_chunk_storage:delete(PaddedOffset, StoreID);
					Error2 ->
						Error2
				end,
			RemoveFromChunksIndex =
				case RemoveFromChunkStorage of
					ok ->
						delete_chunk_metadata(AbsoluteOffset, StoreID);
					Error3 ->
						Error3
				end,
			case RemoveFromChunksIndex of
				ok ->
					NextCursor = AbsoluteOffset + 1,
					gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID});
				{error, Reason} ->
					?LOG_ERROR([{event,
							data_removal_aborted_since_failed_to_remove_chunk},
							{offset, Cursor},
							{reason, io_lib:format("~p", [Reason])}])
			end,
			{noreply, State};
		{error, invalid_iterator} ->
			NextCursor = advance_chunks_index_cursor(Cursor),
			gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID}),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([{event, data_removal_aborted_since_failed_to_query_chunk},
					{offset, Cursor}, {reason, io_lib:format("~p", [Reason])}]),
			{noreply, State}
	end;

handle_cast({expire_repack_request, Key}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, {_, DataPath, Offset, DataRoot, _, _, _}} ->
			decrement_chunk_cache_size(),
			DataPathHash = crypto:hash(sha256, DataPath),
			?LOG_DEBUG([{event, expired_repack_chunk_request},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)},
					{relative_offset, Offset}]),
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			{noreply, State2};
		_ ->
			{noreply, State}
	end;

handle_cast({expire_unpack_request, Key}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{unpack_fetched_chunk, _Args} ->
			decrement_chunk_cache_size(),
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			{noreply, State2};
		_ ->
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	store_sync_state(State),
	ar_util:cast_after(?STORE_STATE_FREQUENCY_MS, self(), store_sync_state),
	{noreply, State};

handle_cast({remove_recently_processed_disk_pool_offset, Offset, ChunkDataKey}, State) ->
	{noreply, remove_recently_processed_disk_pool_offset(Offset, ChunkDataKey, State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, State}.

handle_call({add_block, B, SizeTaggedTXs}, _From, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{reply, add_block(B, SizeTaggedTXs, StoreID), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
	{reply, ok, State}.

handle_info({event, node_state, {initialized, B}}, State) ->
	{noreply, State#sync_data_state{ weave_size = B#block.weave_size }};

handle_info({event, node_state, {new_tip, B, _PrevB}}, State) ->
	{noreply, State#sync_data_state{ weave_size = B#block.weave_size }};

handle_info({event, node_state, {search_space_upper_bound, Bound}}, State) ->
	{noreply, State#sync_data_state{ disk_pool_threshold = Bound }};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({chunk, {unpacked, Key, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{unpack_fetched_chunk, Args} ->
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			process_unpacked_chunk(ChunkArgs, Args, State2);
		_ ->
			{noreply, State}
	end;

handle_info({chunk, {unpack_error, Key, ChunkArgs, Error}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{unpack_fetched_chunk, Args} ->
			{Packing, _Chunk1, AbsoluteEndOffset, _TXRoot, ChunkSize} = ChunkArgs,
			{_AbsoluteTXStartOffset, _TXSize, _DataPath, _TXPath, _DataRoot,
					_Chunk2, _ChunkID, _ChunkEndOffset, Peer, _Byte} = Args,
			?LOG_WARNING([{event, got_invalid_packed_chunk},
					{peer, ar_util:format_peer(Peer)},
					{absolute_end_offset, AbsoluteEndOffset},
					{packing, ar_serialize:encode_packing(Packing, true)},
					{chunk_size, ChunkSize},
					{error, io_lib:format("~p", [Error])}]),
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			ar_peers:issue_warning(Peer, chunk, Error),
			{noreply, State2};
		_ ->
			{noreply, State}
	end;

handle_info({chunk, {packed, Key, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	Packing = element(1, ChunkArgs),
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, Args} when element(1, Args) == Packing ->
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			{noreply, store_chunk(ChunkArgs, Args, State2)};
		_ ->
			{noreply, State}
	end;

handle_info({chunk, _}, State) ->
	{noreply, State};

handle_info({event, disksup, {remaining_disk_space, StoreID, false, Percentage, _Bytes}},
		#sync_data_state{ store_id = StoreID } = State) ->
	case Percentage < 0.01 of
		true ->
			case is_disk_space_sufficient(StoreID) of
				false ->
					ok;
				_ ->
					log_insufficient_disk_space(StoreID)
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, false});
		false ->
			case Percentage > 0.05 of
				true ->
					case is_disk_space_sufficient(StoreID) of
						false ->
							log_sufficient_disk_space(StoreID);
						_ ->
							ok
					end;
				false ->
					ok
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, true})
	end,
	{noreply, State};
handle_info({event, disksup, {remaining_disk_space, StoreID, true, _Percentage, Bytes}},
		#sync_data_state{ store_id = StoreID } = State) ->
	{ok, Config} = arweave_config:get_env(),
	%% Default values:
	%% max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB = 100_000
	%% disk_cache_size = ?DISK_CACHE_SIZE = 5_120
	%% DiskPoolSize = ~100GB
	%% DisckCacheSize = ~5GB
	%% BufferSize = ~10GB
	DiskPoolSize = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	DiskCacheSize = Config#config.disk_cache_size * 1024 * 1024,
	BufferSize = 10_000_000_000,
	case Bytes < DiskPoolSize + DiskCacheSize + (BufferSize div 2) of
		true ->
			ar:console("error: Not enough disk space left on 'data_dir' disk for "
				"the requested 'disk_pool_size' ~Bmb and 'disk_cache_size' ~Bmb "
				"either lower these values or add more disk space.~n",
			[Config#config.max_disk_pool_buffer_mb, Config#config.disk_cache_size]),
			case is_disk_space_sufficient(StoreID) of
				false ->
					ok;
				_ ->
					log_insufficient_disk_space(StoreID)
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, false});
		false ->
			case Bytes > DiskPoolSize + DiskCacheSize + BufferSize of
				true ->
					case is_disk_space_sufficient(StoreID) of
						false ->
							log_sufficient_disk_space(StoreID);
						_ ->
							ok
					end;
				false ->
					ok
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, true})
	end,
	{noreply, State};

handle_info({event, disksup, _}, State) ->
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info({'DOWN', _,  process, _, normal}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, noproc}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, Reason},  #sync_data_state{ store_id = StoreID } = State) ->
	?LOG_WARNING([{event, collect_intervals_job_failed},
			{reason, io_lib:format("~p", [Reason])}, {action, spawning_another_one},
			{store_id, StoreID}]),
	gen_server:cast(self(), collect_peer_intervals),
	{noreply, State};

handle_info(Message,  #sync_data_state{ store_id = StoreID } = State) ->
	?LOG_WARNING([{event, unhandled_info}, {store_id, StoreID}, {message, Message}]),
	{noreply, State}.

terminate(Reason, #sync_data_state{ store_id = StoreID } = State) ->
	store_sync_state(State),
	?LOG_INFO([{event, terminate}, {store_id, StoreID},
			{reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

init_sync_status(StoreID) ->
	SyncStatus = case ar_data_sync_worker_master:is_syncing_enabled() of
		true -> paused;
		false -> off
	end,
	ar_device_lock:set_device_lock_metric(StoreID, sync, SyncStatus),
	SyncStatus.
do_log_chunk_error(LogType, Event, ExtraLogData) ->
	LogData = [{event, Event}, {tags, [solution_proofs]} | ExtraLogData],
	case LogType of
		error ->
			?LOG_ERROR(LogData);
		info ->
			?LOG_INFO(LogData)
	end.

log_chunk_error(http, _, _) ->
	ok;
log_chunk_error(tx_data, _, _) ->
	ok;
log_chunk_error(verify, Event, ExtraLogData) ->
	do_log_chunk_error(info, Event, [{request_origin, verify} | ExtraLogData]);
log_chunk_error(RequestOrigin, Event, ExtraLogData) ->
	do_log_chunk_error(error, Event, [{request_origin, RequestOrigin} | ExtraLogData]).

do_sync_intervals(State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals, store_id = StoreID } = State,
	IsQueueEmpty =
		case gb_sets:is_empty(Q) of
			true ->
				ar_util:cast_after(500, self(), sync_intervals),
				true;
			false ->
				false
		end,
	IsDiskSpaceSufficient =
		case IsQueueEmpty of
			true ->
				false;
			false ->
				case is_disk_space_sufficient(StoreID) of
					true ->
						true;
					_ ->
						ar_util:cast_after(30000, self(), sync_intervals),
						false
				end
		end,
	IsChunkCacheFull =
		case IsDiskSpaceSufficient of
			false ->
				true;
			true ->
				case is_chunk_cache_full() of
					true ->
						ar_util:cast_after(1000, self(), sync_intervals),
						true;
					false ->
						false
				end
		end,
	AreSyncWorkersBusy =
		case IsChunkCacheFull of
			true ->
				true;
			false ->
				case ar_data_sync_worker_master:ready_for_work() of
					false ->
						ar_util:cast_after(200, self(), sync_intervals),
						true;
					true ->
						false
				end
		end,
	case AreSyncWorkersBusy of
		true ->
			State;
		false ->
			gen_server:cast(self(), sync_intervals),
			{{Start, End, Peer}, Q2} = gb_sets:take_smallest(Q),
			I2 = ar_intervals:delete(QIntervals, End, Start),
			gen_server:cast(ar_data_sync_worker_master,
					{sync_range, {Start, End, Peer, StoreID}}),
			State#sync_data_state{ sync_intervals_queue = Q2,
					sync_intervals_queue_intervals = I2 }
	end.

do_sync_data(State) ->
	#sync_data_state{ store_id = StoreID, range_start = RangeStart, range_end = RangeEnd,
			disk_pool_threshold = DiskPoolThreshold } = State,
	%% See if any of StoreID's unsynced intervals can be found in the "default"
	%% storage_module
	Intervals = get_unsynced_intervals_from_other_storage_modules(
		StoreID, ?DEFAULT_MODULE, RangeStart, min(RangeEnd, DiskPoolThreshold)),
	gen_server:cast(self(), sync_data2),
	%% Find all storage_modules that might include the target chunks (e.g. neighboring
	%% storage_modules with an overlap, or unpacked copies used for packing, etc...)
	OtherStorageModules = [ar_storage_module:id(Module)
			|| Module <- ar_storage_module:get_all(RangeStart, RangeEnd),
			ar_storage_module:id(Module) /= StoreID],
	?LOG_INFO([{event, sync_data}, {stage, copy_from_default_storage_module},
		{store_id, StoreID}, {range_start, RangeStart}, {range_end, RangeEnd},
		{range_end, RangeEnd}, {disk_pool_threshold, DiskPoolThreshold},
		{default_intervals, length(Intervals)},
		{other_storage_modules, length(OtherStorageModules)}]),
	State#sync_data_state{
		unsynced_intervals_from_other_storage_modules = Intervals,
		other_storage_modules_with_unsynced_intervals = OtherStorageModules
	}.

%% @doc No unsynced overlap intervals, proceed with syncing
do_sync_data2(#sync_data_state{
		unsynced_intervals_from_other_storage_modules = [],
		other_storage_modules_with_unsynced_intervals = [] } = State) ->
	#sync_data_state{ store_id = StoreID,
		range_start = RangeStart, range_end = RangeEnd } = State,
	?LOG_INFO([{event, sync_data}, {stage, complete},
		{store_id, StoreID}, {range_start, RangeStart}, {range_end, RangeEnd}]),
	ar_util:cast_after(2000, self(), collect_peer_intervals),
	State;
%% @doc Check to see if a neighboring storage_module may have already synced one of our
%% unsynced intervals
do_sync_data2(#sync_data_state{
			store_id = StoreID, range_start = RangeStart, range_end = RangeEnd,
			unsynced_intervals_from_other_storage_modules = [],
			other_storage_modules_with_unsynced_intervals = [OtherStoreID | OtherStoreIDs]
		} = State) ->
	Packing = ar_storage_module:get_packing(StoreID),
	OtherPacking = ar_storage_module:get_packing(OtherStoreID),
	Intervals = get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID,
			RangeStart, RangeEnd),
	?LOG_INFO([{event, sync_data}, {stage, copy_from_other_storage_modules},
		{store_id, StoreID}, {other_store_id, OtherStoreID}, 
		{range_start, RangeStart}, {range_end, RangeEnd},
		{found_intervals, length(Intervals)}]),
	gen_server:cast(self(), sync_data2),
	State#sync_data_state{
		unsynced_intervals_from_other_storage_modules = Intervals,
		other_storage_modules_with_unsynced_intervals = OtherStoreIDs
	};
%% @doc Read an unsynced interval from the disk of a neighboring storage_module
do_sync_data2(#sync_data_state{
		store_id = StoreID,
		unsynced_intervals_from_other_storage_modules =
			[{OtherStoreID, {Start, End}} | Intervals]
		} = State) ->
	State2 =
		case ar_chunk_copy:read_range(Start, End, OtherStoreID, StoreID) of
			true ->
				State#sync_data_state{
					unsynced_intervals_from_other_storage_modules = Intervals };
			false ->
				State
		end,
	ar_util:cast_after(50, self(), sync_data2),
	State2.

remove_expired_disk_pool_data_roots() ->
	Now = os:system_time(microsecond),
	{ok, Config} = arweave_config:get_env(),
	ExpirationTime = Config#config.disk_pool_data_root_expiration_time * 1000000,
	ets:foldl(
		fun({Key, {_Size, Timestamp, _TXIDSet}}, _Acc) ->
			case Timestamp + ExpirationTime > Now of
				true ->
					ok;
				false ->
					ets:delete(ar_disk_pool_data_roots, Key),
					ok
			end
		end,
		ok,
		ar_disk_pool_data_roots
	).

get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID, RequestOrigin) ->
	case read_chunk_with_metadata(Offset, SeekOffset, StoredPacking, StoreID, true,
			RequestOrigin) of
		{error, Reason} ->
			{error, Reason};
		{ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath} ->
			ChunkID =
				case validate_fetched_chunk({AbsoluteOffset, DataPath, TXPath, TXRoot,
						ChunkSize, StoreID, RequestOrigin}) of
					{true, ID} ->
						ID;
					false ->
						error
				end,
			PackResult =
				case {ChunkID, Packing == StoredPacking, Pack} of
					{error, _, _} ->
						%% Chunk was read but could not be validated.
						{error, chunk_failed_validation};
					{_, false, false} ->
						%% Requested and stored chunk are in different formats,
						%% and repacking is disabled.
						{error, chunk_stored_in_different_packing_only};
					_ ->
						ar_packing_server:repack(
							Packing, StoredPacking, AbsoluteOffset, TXRoot, Chunk, ChunkSize)
				end,
			case {PackResult, ChunkID} of
				{{error, Reason}, _} ->
					log_chunk_error(RequestOrigin, failed_to_repack_chunk,
							[{packing, ar_serialize:encode_packing(Packing, true)},
							{stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
							{absolute_end_offset, AbsoluteOffset},
							{store_id, StoreID},
							{error, io_lib:format("~p", [Reason])}]),
					{error, Reason};
				{{ok, PackedChunk, none}, _} ->
					%% PackedChunk is the requested format.
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
							data_path => DataPath, tx_path => TXPath,
							absolute_end_offset => AbsoluteOffset,
							chunk_size => ChunkSize },
					{ok, Proof};
				{{ok, PackedChunk, MaybeUnpackedChunk}, none} ->
					%% PackedChunk is the requested format, but the ChunkID could
					%% not be determined
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
							data_path => DataPath, tx_path => TXPath,
							absolute_end_offset => AbsoluteOffset,
							chunk_size => ChunkSize },
					case MaybeUnpackedChunk of
						none ->
							{ok, Proof};
						_ ->
							{ok, Proof#{ unpacked_chunk => MaybeUnpackedChunk }}
					end;
				{{ok, PackedChunk, MaybeUnpackedChunk}, _} ->
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
							data_path => DataPath, tx_path => TXPath,
							absolute_end_offset => AbsoluteOffset,
							chunk_size => ChunkSize },
					case MaybeUnpackedChunk of
						none ->
							{ok, Proof};
						_ ->
							ComputedChunkID = ar_tx:generate_chunk_id(MaybeUnpackedChunk),
							case ComputedChunkID == ChunkID of
								true ->
									{ok, Proof#{ unpacked_chunk => MaybeUnpackedChunk }};
								false ->
									log_chunk_error(RequestOrigin, get_chunk_invalid_id,
											[{chunk_size, ChunkSize},
											{actual_chunk_size, byte_size(MaybeUnpackedChunk)},
											{requested_packing,
												ar_serialize:encode_packing(Packing, true)},
											{stored_packing,
												ar_serialize:encode_packing(StoredPacking, true)},
											{absolute_end_offset, AbsoluteOffset},
											{offset, Offset},
											{seek_offset, SeekOffset},
											{store_id, StoreID},
											{expected_chunk_id, ar_util:encode(ChunkID)},
											{chunk_id, ar_util:encode(ComputedChunkID)},
											{actual_chunk, binary:part(MaybeUnpackedChunk, 0, 32)}]),
									invalidate_bad_data_record({AbsoluteOffset, ChunkSize,
										StoreID, get_chunk_invalid_id}),
									{error, chunk_not_found}
							end
					end
			end
	end.

get_chunk_proof(Offset, SeekOffset, StoredPacking, StoreID, RequestOrigin) ->
	case read_chunk_with_metadata(
			Offset, SeekOffset, StoredPacking, StoreID, false, RequestOrigin) of
		{error, Reason} ->
			{error, Reason};
		{ok, DataPath, AbsoluteOffset, TXRoot, ChunkSize, TXPath} ->
			CheckProof =
				case validate_fetched_chunk({AbsoluteOffset, DataPath, TXPath, TXRoot,
						ChunkSize, StoreID, false}) of
					{true, ID} ->
						ID;
					false ->
						error
				end,
			case CheckProof of
				error ->
					%% Proof was read but could not be validated.
					log_chunk_error(RequestOrigin, chunk_proof_failed_validation,
							[{offset, Offset},
							{seek_offset, SeekOffset},
							{stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
							{store_id, StoreID}]),
					{error, chunk_not_found};
				_ ->
					Proof = #{ data_path => DataPath, tx_path => TXPath },
					{ok, Proof}
			end
	end.

%% @doc Read the chunk metadata and optionally the chunk itself.
%%
%% When ReadChunk=true, the response is of the format:
%% {ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
%%
%% Otherwise, the format is
%% {ok, DataPath, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
read_chunk_with_metadata(
		Offset, SeekOffset, unpacked_padded, StoreID, _ReadChunk, RequestOrigin) ->
	%% unpacked_padded is an intermediate format and should not be read. Since not all
	%% the records and indices have been fully setup, trying to read the chunk can cause
	%% its offset to be invalidated.
	log_chunk_error(RequestOrigin, read_unpacked_padded_chunk,
			[{seek_offset, SeekOffset},
			{offset, Offset},
			{store_id, StoreID},
			{stored_packing, unpacked_padded}]),
	{error, chunk_not_found};
read_chunk_with_metadata(
		Offset, SeekOffset, StoredPacking, StoreID, ReadChunk, RequestOrigin) ->
	case get_chunk_by_byte(SeekOffset, StoreID) of
		{error, invalid_iterator} ->
			%% No error log needed since this is expected behavior when the chunk simply
			%% isn't stored.
			{error, chunk_not_found};
		{error, Err} ->
			Modules = ar_storage_module:get_all(SeekOffset),
			ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
			log_chunk_error(RequestOrigin, failed_to_fetch_chunk_metadata,
				[{seek_offset, SeekOffset},
				{store_id, StoreID},
				{stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
				{modules_covering_seek_offset, ModuleIDs},
				{error, io_lib:format("~p", [Err])}]),
			{error, chunk_not_found};
		{ok, _, {AbsoluteOffset, _, _, _, _, _, ChunkSize}}
				when AbsoluteOffset - SeekOffset >= ChunkSize ->
			log_chunk_error(RequestOrigin, chunk_offset_mismatch,
					[{absolute_offset, AbsoluteOffset},
					{seek_offset, SeekOffset},
					{store_id, StoreID},
					{stored_packing, ar_serialize:encode_packing(StoredPacking, true)}]),
			{error, chunk_not_found};
		{ok, _, {AbsoluteOffset, ChunkDataKey, TXRoot, _, TXPath, _, ChunkSize}} ->
			ReadFun =
				case ReadChunk of
					true ->
						fun read_chunk/3;
					_ ->
						fun read_data_path/3
				end,
			case ReadFun(AbsoluteOffset, ChunkDataKey, StoreID) of
				not_found ->
					Modules = ar_storage_module:get_all(SeekOffset),
					ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
					log_chunk_error(RequestOrigin, failed_to_read_chunk_data_path,
						[{seek_offset, SeekOffset},
						{absolute_offset, AbsoluteOffset},
						{store_id, StoreID},
						{stored_packing,
							ar_serialize:encode_packing(StoredPacking, true)},
						{modules_covering_seek_offset, ModuleIDs},
						{chunk_data_key, ar_util:encode(ChunkDataKey)},
						{read_fun, ReadFun}]),
					invalidate_bad_data_record({AbsoluteOffset, ChunkSize, StoreID,
						failed_to_read_chunk_data_path}),
					{error, chunk_not_found};
				{error, Error} ->
					log_chunk_error(RequestOrigin, failed_to_read_chunk,
							[{reason, io_lib:format("~p", [Error])},
							{chunk_data_key, ar_util:encode(ChunkDataKey)},
							{absolute_end_offset, Offset}]),
					{error, failed_to_read_chunk};
				{ok, {Chunk, DataPath}} ->
					case ar_sync_record:is_recorded(Offset, StoredPacking, ar_data_sync,
							StoreID) of
						false ->
							Modules = ar_storage_module:get_all(SeekOffset),
							ModuleIDs = [ar_storage_module:id(Module) || Module <- Modules],
							RootRecords = [ets:lookup(sync_records, {ar_data_sync, ID})
									|| ID <- ModuleIDs],
							log_chunk_error(RequestOrigin, chunk_metadata_read_sync_record_race_condition,
								[{seek_offset, SeekOffset},
								{storeID, StoreID},
								{modules_covering_seek_offset, ModuleIDs},
								{root_sync_records, RootRecords},
								{stored_packing,
									ar_serialize:encode_packing(StoredPacking, true)}]),
							%% The chunk should have been re-packed
							%% in the meantime - very unlucky timing.
							{error, chunk_not_found};
						true ->
							{ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
					end;
				{ok, DataPath} ->
					{ok, DataPath, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
			end
	end.

invalidate_bad_data_record({AbsoluteEndOffset, ChunkSize, StoreID, Type}) ->
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case AbsoluteEndOffset > T of
		true ->
			[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
			case AbsoluteEndOffset > T of
				true ->
					%% Do not invalidate fresh records - a reorg may be in progress.
					ok;
				false ->
					invalidate_bad_data_record2({AbsoluteEndOffset, ChunkSize, StoreID, Type})
			end;
		false ->
			invalidate_bad_data_record2({AbsoluteEndOffset, ChunkSize, StoreID, Type})
	end.

invalidate_bad_data_record2({AbsoluteEndOffset, ChunkSize, StoreID, Type}) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),
	StartOffset = AbsoluteEndOffset - ChunkSize,
	?LOG_WARNING([{event, invalidating_bad_data_record}, {type, Type},
			{range_start, StartOffset}, {range_end, PaddedEndOffset},
			{store_id, StoreID}]),
	case remove_invalid_sync_records(PaddedEndOffset, StartOffset, StoreID) of
		ok ->
			ar_sync_record:add(PaddedEndOffset, StartOffset, invalid_chunks, StoreID),
			case delete_invalid_metadata(AbsoluteEndOffset, StoreID) of
				ok ->
					ok;
				Error2 ->
					?LOG_WARNING([{event, failed_to_remove_chunks_index_key},
							{absolute_end_offset, AbsoluteEndOffset},
							{error, io_lib:format("~p", [Error2])}])
			end;
		Error ->
			?LOG_WARNING([{event, failed_to_remove_sync_record_range},
					{range_end, PaddedEndOffset}, {range_start, StartOffset},
					{error, io_lib:format("~p", [Error])}])
	end.

remove_invalid_sync_records(PaddedEndOffset, StartOffset, StoreID) ->
	Remove1 = ar_footprint_record:delete(PaddedEndOffset, StoreID),
	Remove2 =
		case Remove1 of
			ok ->
				ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_data_sync, StoreID);
			Error ->
				Error
		end,
	IsSmallChunkBeforeThreshold = PaddedEndOffset - StartOffset < ?DATA_CHUNK_SIZE,
	Remove3 =
		case {Remove2, IsSmallChunkBeforeThreshold} of
			{ok, false} ->
				ar_sync_record:delete(PaddedEndOffset, StartOffset,
						ar_chunk_storage, StoreID);
			_ ->
				Remove2
		end,
	Remove4 =
		case {Remove3, IsSmallChunkBeforeThreshold} of
			{ok, false} ->
				ar_entropy_storage:delete_record(PaddedEndOffset, StartOffset, StoreID);
			_ ->
				Remove3
		end,
	case {Remove4, IsSmallChunkBeforeThreshold} of
		{ok, false} ->
			ar_sync_record:delete(PaddedEndOffset, StartOffset,
					ar_chunk_storage_replica_2_9_1_unpacked, StoreID);
		_ ->
			Remove4
	end.

delete_invalid_metadata(AbsoluteEndOffset, StoreID) ->
	case get_chunk_metadata(AbsoluteEndOffset, StoreID) of
		not_found ->
			ok;
		{ok, Metadata} ->
			{ChunkDataKey, _, _, _, _, _} = Metadata,
			delete_chunk_data(ChunkDataKey, StoreID),
			delete_chunk_metadata(AbsoluteEndOffset, StoreID)
	end.

validate_fetched_chunk(Args) ->
	{Offset, DataPath, TXPath, TXRoot, ChunkSize, StoreID, RequestOrigin} = Args,
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case Offset > T orelse not ar_node:is_joined() of
		true ->
			case RequestOrigin of
				miner ->
					log_chunk_error(RequestOrigin, miner_requested_disk_pool_chunk,
							[{disk_pool_threshold, T}, {end_offset, Offset}]);
				_ ->
					ok
			end,
			{true, none};
		false ->
			case ar_block_index:get_block_bounds(Offset - 1) of
				{BlockStart, BlockEnd, TXRoot} ->

					ChunkOffset = Offset - BlockStart - 1,
					case validate_proof2(TXRoot, TXPath, DataPath, BlockStart, BlockEnd,
							ChunkOffset, ChunkSize, RequestOrigin) of
						{true, ChunkID} ->
							{true, ChunkID};
						false ->
							log_chunk_error(RequestOrigin, failed_to_validate_chunk_proofs,
								[{absolute_end_offset, Offset}, {store_id, StoreID}]),
							invalidate_bad_data_record({Offset, ChunkSize, StoreID,
								failed_to_validate_chunk_proofs}),
							false
					end;
				{_BlockStart, _BlockEnd, TXRoot2} ->
					log_chunk_error(RequestOrigin, stored_chunk_invalid_tx_root,
						[{end_offset, Offset}, {tx_root, ar_util:encode(TXRoot2)},
						{stored_tx_root, ar_util:encode(TXRoot)}, {store_id, StoreID}]),
					invalidate_bad_data_record({Offset, ChunkSize, StoreID,
						stored_chunk_invalid_tx_root}),
					false
			end
	end.


get_tx_offset(TXIndex, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_tx_offset},
					{reason, io_lib:format("~p", [Reason])},
					{tx, ar_util:encode(TXID)}]),
			{error, failed_to_read_offset}
	end.

get_tx_offset_data_in_range(TXOffsetIndex, TXIndex, Start, End) ->
	case ar_kv:get_prev(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>) of
		none ->
			get_tx_offset_data_in_range2(TXOffsetIndex, TXIndex, Start, End);
		{ok, << Start2:?OFFSET_KEY_BITSIZE >>, _} ->
			get_tx_offset_data_in_range2(TXOffsetIndex, TXIndex, Start2, End);
		Error ->
			Error
	end.

get_tx_offset_data_in_range2(TXOffsetIndex, TXIndex, Start, End) ->
	case ar_kv:get_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, []};
		{ok, Map} ->
			case maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, Acc) ->
						case get_tx_offset(TXIndex, TXID) of
							{ok, {EndOffset, Size}} ->
								case EndOffset =< Start of
									true ->
										Acc;
									false ->
										[{TXID, EndOffset - Size, EndOffset} | Acc]
								end;
							not_found ->
								Acc;
							Error ->
								Error
						end
				end,
				[],
				Map
			) of
				{error, _} = Error ->
					Error;
				List ->
					{ok, lists:reverse(List)}
			end;
		Error ->
			Error
	end.

get_tx_data(Start, End, Chunks, _Pack) when Start >= End ->
	{ok, iolist_to_binary(Chunks)};
get_tx_data(Start, End, Chunks, Pack) ->
	case get_chunk(Start + 1, #{ pack => Pack, packing => unpacked,
			bucket_based_offset => false, origin => tx_data }) of
		{ok, #{ chunk := Chunk }} ->
			get_tx_data(Start + byte_size(Chunk), End, [Chunks | Chunk], Pack);
		{error, chunk_not_found} ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data},
					{reason, io_lib:format("~p", [Reason])}]),
			{error, failed_to_get_tx_data}
	end.

get_data_root_offset(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	DataRootIndex = {data_root_index, StoreID},
	case ar_kv:get_prev(DataRootIndex, << DataRoot:32/binary,
			(ar_serialize:encode_int(TXSize, 8))/binary, <<"a">>/binary >>) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, Offset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {Offset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

remove_range(Start, End, Ref, ReplyTo) ->
	ReplyFun =
		fun(Fun, StorageRefs) ->
			case sets:is_empty(StorageRefs) of
				true ->
					ReplyTo ! {removed_range, Ref},
					ar_events:send(sync_record, {global_remove_range, Start, End});
				false ->
					receive
						{removed_range, StorageRef} ->
							Fun(Fun, sets:del_element(StorageRef, StorageRefs))
					after 10000 ->
						?LOG_DEBUG([{event,
								waiting_for_data_range_removal_longer_than_ten_seconds}]),
						Fun(Fun, StorageRefs)
					end
			end
		end,
	StorageModules = ar_storage_module:get_all(Start, End),
	StoreIDs = [?DEFAULT_MODULE | [ar_storage_module:id(M) || M <- StorageModules]],
	RefL = [make_ref() || _ <- StoreIDs],
	PID = spawn(fun() -> ReplyFun(ReplyFun, sets:from_list(RefL)) end),
	lists:foreach(
		fun({StoreID, R}) ->
			gen_server:cast(name(StoreID), {remove_range, End, Start + 1, R, PID})
		end,
		lists:zip(StoreIDs, RefL)
	).

init_kv(StoreID) ->
	BasicOpts = [{max_open_files, 10000}],
	BloomFilterOpts = [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true}, % Keep bloom filters in memory.
			{bloom_filter_policy, 10} % ~1% false positive probability.
		]},
		{optimize_filters_for_hits, true}
	],
	PrefixBloomFilterOpts =
		BloomFilterOpts ++ [
			{prefix_extractor, {capped_prefix_transform, ?OFFSET_KEY_PREFIX_BITSIZE div 8}}],
	ColumnFamilyDescriptors = [
		{"default", BasicOpts},
		{"chunks_index", BasicOpts ++ PrefixBloomFilterOpts},
		{"data_root_index", BasicOpts ++ BloomFilterOpts},
		{"data_root_offset_index", BasicOpts},
		{"tx_index", BasicOpts ++ BloomFilterOpts},
		{"tx_offset_index", BasicOpts},
		{"disk_pool_chunks_index", BasicOpts ++ BloomFilterOpts},
		{"migrations_index", BasicOpts}
	],
	Dir =
		case StoreID of
			?DEFAULT_MODULE ->
				?ROCKS_DB_DIR;
			_ ->
				filename:join(["storage_modules", StoreID, ?ROCKS_DB_DIR])
		end,
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_db"), ColumnFamilyDescriptors, [],
			[{ar_data_sync, StoreID}, {chunks_index, StoreID}, {data_root_index_old, StoreID},
			{data_root_offset_index, StoreID}, {tx_index, StoreID}, {tx_offset_index, StoreID},
			{disk_pool_chunks_index_old, StoreID}, {migrations_index, StoreID}]),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_chunk_db"), [{max_open_files, 10000},
			{max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}], {chunk_data_db, StoreID}),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_disk_pool_chunks_index_db"), [
			{max_open_files, 1000}, {max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}] ++ BloomFilterOpts,
			{disk_pool_chunks_index, StoreID}),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_data_root_index_db"), [
			{max_open_files, 100}, {max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}] ++ BloomFilterOpts,
			{data_root_index, StoreID}),
	#sync_data_state{
		chunks_index = {chunks_index, StoreID},
		data_root_index = {data_root_index, StoreID},
		data_root_index_old = {data_root_index_old, StoreID},
		data_root_offset_index = {data_root_offset_index, StoreID},
		chunk_data_db = {chunk_data_db, StoreID},
		tx_index = {tx_index, StoreID},
		tx_offset_index = {tx_offset_index, StoreID},
		disk_pool_chunks_index = {disk_pool_chunks_index, StoreID},
		disk_pool_chunks_index_old = {disk_pool_chunks_index_old, StoreID},
		migrations_index = {migrations_index, StoreID}
	}.

move_disk_pool_index(State) ->
	move_disk_pool_index(first, State).

move_disk_pool_index(Cursor, State) ->
	#sync_data_state{ disk_pool_chunks_index_old = Old,
			disk_pool_chunks_index = New } = State,
	case ar_kv:get_next(Old, Cursor) of
		none ->
			ok;
		{ok, Key, Value} ->
			ok = ar_kv:put(New, Key, Value),
			ok = ar_kv:delete(Old, Key),
			move_disk_pool_index(Key, State)
	end.

move_data_root_index(#sync_data_state{ migrations_index = MI,
		data_root_index_old = DI } = State) ->
	case ar_kv:get(MI, <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
			ok;
		{ok, Cursor} ->
			move_data_root_index(Cursor, 1, State);
		not_found ->
			case ar_kv:get_next(DI, last) of
				none ->
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, Key, _} ->
					move_data_root_index(Key, 1, State)
			end
	end.

move_data_root_index(Cursor, N, State) ->
	#sync_data_state{ migrations_index = MI, data_root_index_old = Old,
			data_root_index = New } = State,
	case N rem 50000 of
		0 ->
			?LOG_DEBUG([{event, moving_data_root_index}, {moved_keys, N}]),
			ok = ar_kv:put(MI, <<"move_data_root_index">>, Cursor),
			gen_server:cast(self(), {move_data_root_index, Cursor, N + 1});
		_ ->
			case ar_kv:get_prev(Old, Cursor) of
				none ->
					ok = ar_kv:put(MI, <<"move_data_root_index">>, <<"complete">>),
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value),
					move_data_root_index(DataRoot, TXSize, data_root_index_iterator(M), New),
					PrevKey = << DataRoot:32/binary, (TXSize - 1):?OFFSET_KEY_BITSIZE >>,
					move_data_root_index(PrevKey, N + 1, State);
				{ok, Key, _} ->
					%% The empty data root key (from transactions without data) was
					%% unnecessarily recorded in the index.
					PrevKey = binary:part(Key, 0, byte_size(Key) - 1),
					move_data_root_index(PrevKey, N + 1, State)
			end
	end.

move_data_root_index(DataRoot, TXSize, Iterator, DB) ->
	case data_root_index_next(Iterator, infinity) of
		none ->
			ok;
		{{Offset, _TXRoot, TXPath}, Iterator2} ->
			Key = data_root_key_v2(DataRoot, TXSize, Offset),
			ok = ar_kv:put(DB, Key, TXPath),
			move_data_root_index(DataRoot, TXSize, Iterator2, DB)
	end.

data_root_key_v2(DataRoot, TXSize, Offset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(Offset, 8))/binary >>.

record_disk_pool_chunks_count() ->
	DB = {disk_pool_chunks_index, ?DEFAULT_MODULE},
	case ar_kv:count(DB) of
		Count when is_integer(Count) ->
			prometheus_gauge:set(disk_pool_chunks_count, Count);
		Error ->
			?LOG_WARNING([{event, failed_to_read_disk_pool_chunks_count},
					{error, io_lib:format("~p", [Error])}])
	end.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, #{ block_index := RecentBI } = M} ->
			maps:merge(M, #{
				weave_size => case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
				disk_pool_threshold => maps:get(disk_pool_threshold, M,
						get_disk_pool_threshold(RecentBI)) });
		not_found ->
			#{ block_index => [], disk_pool_data_roots => #{}, disk_pool_size => 0,
					weave_size => 0, packing_2_5_threshold => infinity,
					disk_pool_threshold => 0 }
	end.

recalculate_disk_pool_size(DataRootMap, State) ->
	#sync_data_state{ disk_pool_chunks_index = Index } = State,
	DataRootMap2 = maps:map(fun(_DataRootKey, {_Size, Timestamp, TXIDSet}) ->
			{0, Timestamp, TXIDSet} end, DataRootMap),
	recalculate_disk_pool_size(Index, DataRootMap2, first, 0).

recalculate_disk_pool_size(Index, DataRootMap, Cursor, Sum) ->
	case ar_kv:get_next(Index, Cursor) of
		none ->
			prometheus_gauge:set(pending_chunks_size, Sum),
			maps:map(fun(DataRootKey, V) -> ets:insert(ar_disk_pool_data_roots,
					{DataRootKey, V}) end, DataRootMap),
			ets:insert(ar_data_sync_state, {disk_pool_size, Sum});
		{ok, Key, Value} ->
			DecodedValue = binary_to_term(Value),
			ChunkSize = element(2, DecodedValue),
			DataRoot = element(3, DecodedValue),
			TXSize = element(4, DecodedValue),
			DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
			DataRootMap2 =
				case maps:get(DataRootKey, DataRootMap, not_found) of
					not_found ->
						DataRootMap;
					{Size, Timestamp, TXIDSet} ->
						maps:put(DataRootKey, {Size + ChunkSize, Timestamp, TXIDSet},
								DataRootMap)
				end,
			Cursor2 = << Key/binary, <<"a">>/binary >>,
			recalculate_disk_pool_size(Index, DataRootMap2, Cursor2, Sum + ChunkSize)
	end.

get_disk_pool_threshold([]) ->
	0;
get_disk_pool_threshold(BI) ->
	ar_node:get_partition_upper_bound(BI).

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_tx_index_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots} = remove_data_root_index_range(BlockStartOffset, WeaveSize, State),
	ok = remove_data_root_offset_index_range(BlockStartOffset, WeaveSize, State),
	ok = delete_chunk_metadata_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots}.

remove_tx_index_range(Start, End, State) ->
	#sync_data_state{ tx_offset_index = TXOffsetIndex, tx_index = TXIndex } = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, ok) ->
						ar_kv:delete(TXIndex, TXID),
						ar_tx_blacklist:norify_about_orphaned_tx(TXID)
				end,
				ok,
				Map
			);
		Error ->
			Error
	end,
	ar_kv:delete_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

remove_data_root_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex,
			data_root_index = DataRootIndex } = State,
	case ar_kv:get_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, RemovedDataRoots}) ->
						{_TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = Key,
										{ok, Removed}) ->
									case remove_data_root(DataRootIndex, Key, Start, End) of
										removed ->
											{ok, sets:add_element(Key, Removed)};
										ok ->
											{ok, Removed};
										Error ->
											Error
									end;
								(_, Acc) ->
									Acc
							end,
							{ok, RemovedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);
		Error ->
			Error
	end.

remove_data_root(DataRootIndex, DataRootKey, Start, End) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = data_root_key_v2(DataRoot, TXSize, Start),
	EndKey = data_root_key_v2(DataRoot, TXSize, End),
	case ar_kv:delete_range(DataRootIndex, StartKey, EndKey) of
		ok ->
			case ar_kv:get_prev(DataRootIndex, StartKey) of
				{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
						_Rest/binary >>, _} ->
					ok;
				{ok, _, _} ->
					removed;
				none ->
					removed;
				{error, _} = Error ->
					Error
			end;
		Error ->
			Error
	end.

remove_data_root_offset_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex } = State,
	ar_kv:delete_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

repair_data_root_offset_index(BI, State) ->
	#sync_data_state{ migrations_index = DB } = State,
	case ar_kv:get(DB, <<"repair_data_root_offset_index">>) of
		not_found ->
			?LOG_INFO([{event, starting_data_root_offset_index_scan}]),
			ReverseBI = lists:reverse(BI),
			ResyncBlocks = repair_data_root_offset_index(ReverseBI, <<>>, 0, [], State),
			[ar_header_sync:remove_block(Height) || Height <- ResyncBlocks],
			ok = ar_kv:put(DB, <<"repair_data_root_offset_index">>, <<>>),
			?LOG_INFO([{event, data_root_offset_index_scan_complete}]);
		_ ->
			ok
	end.

repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks, State) ->
	#sync_data_state{ data_root_offset_index = DRI } = State,
	case ar_kv:get_next(DRI, Cursor) of
		none ->
			ResyncBlocks;
		{ok, Key, Value} ->
			<< BlockStart:?OFFSET_KEY_BITSIZE >> = Key,
			{TXRoot, BlockSize, _DataRootKeys} = binary_to_term(Value),
			BlockEnd = BlockStart + BlockSize,
			case shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks, BI) of
				{ok, {Height2, BI2}} ->
					Cursor2 = << (BlockStart + 1):?OFFSET_KEY_BITSIZE >>,
					repair_data_root_offset_index(BI2, Cursor2, Height2, ResyncBlocks, State);
				{bad_key, []} ->
					ResyncBlocks;
				{bad_key, ResyncBlocks2} ->
					?LOG_INFO([{event, removing_data_root_index_range},
							{range_start, BlockStart}, {range_end, BlockEnd}]),
					ok = remove_tx_index_range(BlockStart, BlockEnd, State),
					{ok, _} = remove_data_root_index_range(BlockStart, BlockEnd, State),
					ok = remove_data_root_offset_index_range(BlockStart, BlockEnd, State),
					repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks2, State)
			end
	end.

shift_block_index(_TXRoot, _BlockStart, _BlockEnd, _Height, ResyncBlocks, []) ->
	{bad_key, ResyncBlocks};
shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks,
		[{_H, WeaveSize, _TXRoot} | BI]) when BlockEnd > WeaveSize ->
	ResyncBlocks2 = case BlockStart < WeaveSize of true -> [Height | ResyncBlocks];
			_ -> ResyncBlocks end,
	shift_block_index(TXRoot, BlockStart, BlockEnd, Height + 1, ResyncBlocks2, BI);
shift_block_index(TXRoot, _BlockStart, WeaveSize, Height, _ResyncBlocks,
		[{_H, WeaveSize, TXRoot} | BI]) ->
	{ok, {Height + 1, BI}};
shift_block_index(_TXRoot, _BlockStart, _WeaveSize, Height, ResyncBlocks, _BI) ->
	{bad_key, [Height | ResyncBlocks]}.

add_block(B, SizeTaggedTXs, StoreID) ->
	#block{ indep_hash = H, weave_size = WeaveSize, tx_root = TXRoot } = B,
	case ar_block_index:get_element_by_height(B#block.height) of
		{H, WeaveSize, TXRoot} ->
			BlockStart = B#block.weave_size - B#block.block_size,
			case ar_kv:get({data_root_offset_index, StoreID},
					<< BlockStart:?OFFSET_KEY_BITSIZE >>) of
				not_found ->
					{ok, _} = add_block_data_roots(SizeTaggedTXs, BlockStart, StoreID),
					ok = update_tx_index(SizeTaggedTXs, BlockStart, StoreID),
					ok;
				_ ->
					ok
			end;
		_ ->
			ok
	end.

update_tx_index([], _BlockStartOffset, _StoreID) ->
	ok;
update_tx_index(SizeTaggedTXs, BlockStartOffset, StoreID) ->
	lists:foldl(
		fun ({_, Offset}, Offset) ->
				Offset;
			({{padding, _}, Offset}, _) ->
				Offset;
			({{TXID, _}, TXEndOffset}, PreviousOffset) ->
				AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
				TXSize = TXEndOffset - PreviousOffset,
				AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
				case ar_kv:put({tx_offset_index, StoreID},
						<< AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>, TXID) of
					ok ->
						case ar_kv:put({tx_index, StoreID}, TXID,
								term_to_binary({AbsoluteEndOffset, TXSize})) of
							ok ->
								ar_events:send(tx, {registered_offset, TXID, AbsoluteEndOffset,
										TXSize}),
								ar_tx_blacklist:notify_about_added_tx(TXID, AbsoluteEndOffset,
										AbsoluteStartOffset),
								TXEndOffset;
							{error, Reason} ->
								?LOG_ERROR([{event, failed_to_update_tx_index},
										{reason, io_lib:format("~p", [Reason])},
										{tx, ar_util:encode(TXID)}]),
								TXEndOffset
						end;
					{error, Reason} ->
						?LOG_ERROR([{event, failed_to_update_tx_offset_index},
								{reason, io_lib:format("~p", [Reason])},
								{tx, ar_util:encode(TXID)}]),
						TXEndOffset
				end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

add_block_data_roots([], _CurrentWeaveSize, _StoreID) ->
	{ok, sets:new()};
add_block_data_roots(SizeTaggedTXs, CurrentWeaveSize, StoreID) ->
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootIndexKeySet, Args} = lists:foldl(
		fun ({_, Offset}, {Offset, _, _} = Acc) ->
				Acc;
			({{padding, _}, Offset}, {_, Acc1, Acc2}) ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, Offset}, {_, Acc1, Acc2}) when byte_size(DataRoot) < 32 ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, TXEndOffset}, {PrevOffset, CurrentDataRootSet, CurrentArgs}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + PrevOffset,
				TXSize = TXEndOffset - PrevOffset,
				DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet),
						[{DataRoot, TXSize, TXOffset, TXPath} | CurrentArgs]}
		end,
		{0, sets:new(), []},
		SizeTaggedTXs
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put({data_root_offset_index, StoreID},
					<< CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
					term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})),
			lists:foreach(
				fun({DataRoot, TXSize, TXOffset, TXPath}) ->
					ok = update_data_root_index(DataRoot, TXSize, TXOffset, TXPath, StoreID)
				end,
				Args
			);
		false ->
			do_not_update_data_root_offset_index
	end,
	{ok, DataRootIndexKeySet}.

update_data_root_index(DataRoot, TXSize, AbsoluteTXStartOffset, TXPath, StoreID) ->
	ar_kv:put({data_root_index, StoreID},
			data_root_key_v2(DataRoot, TXSize, AbsoluteTXStartOffset), TXPath).

add_block_data_roots_to_disk_pool(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, Timeout, _}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, Timeout, not_set}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

reset_orphaned_data_roots_disk_pool_timestamps(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, _, TXIDSet}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, T, TXIDSet}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

store_sync_state(#sync_data_state{ store_id = ?DEFAULT_MODULE } = State) ->
	#sync_data_state{ block_index = BI } = State,
	DiskPoolDataRoots = ets:foldl(
			fun({DataRootKey, V}, Acc) -> maps:put(DataRootKey, V, Acc) end, #{},
			ar_disk_pool_data_roots),
	StoredState = #{ block_index => BI, disk_pool_data_roots => DiskPoolDataRoots,
			%% Storing it for backwards-compatibility.
			strict_data_split_threshold => ar_block:strict_data_split_threshold() },
	case ar_storage:write_term(data_sync_state, StoredState) of
		{error, enospc} ->
			?LOG_WARNING([{event, failed_to_dump_state}, {reason, disk_full},
					{store_id, ?DEFAULT_MODULE}]),
			ok;
		ok ->
			ok
	end,
	State;
store_sync_state(State) ->
	State.

%% @doc Look to StoreID to find data that TargetStoreID is missing.
%% Args:
%%   StoreID - The ID of the storage module to sync to (this module is missing data)
%%   OtherStoreID - The ID of the storage module to sync from (this module might have the data)
%%   RangeStart - The start offset of the range to check
%%   RangeEnd - The end offset of the range to check
get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID, RangeStart,
		RangeEnd) ->
	get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID, RangeStart,
			RangeEnd, []).

get_unsynced_intervals_from_other_storage_modules(_StoreID, _OtherStoreID, RangeStart,
		RangeEnd, Intervals) when RangeStart >= RangeEnd ->
	Intervals;
get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID, RangeStart,
		RangeEnd, Intervals) ->
	FindNextMissing =
		case ar_sync_record:get_next_synced_interval(RangeStart, RangeEnd, ar_data_sync,
		StoreID) of
			not_found ->
				{request, {RangeStart, RangeEnd}};
			{End, Start} when Start =< RangeStart ->
				{skip, End};
			{_End, Start} ->
				{request, {RangeStart, Start}}
		end,
	case FindNextMissing of
		{skip, End2} ->
			get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID, End2,
					RangeEnd, Intervals);
		{request, {Cursor, RightBound}} ->
			case ar_sync_record:get_next_synced_interval(Cursor, RightBound, ar_data_sync,
					OtherStoreID) of
				not_found ->
					get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID,
							RightBound, RangeEnd, Intervals);
				{End2, Start2} ->
					Start3 = max(Start2, Cursor),
					Intervals2 = [{OtherStoreID, {Start3, End2}} | Intervals],
					get_unsynced_intervals_from_other_storage_modules(StoreID, OtherStoreID,
							End2, RangeEnd, Intervals2)
			end
	end.

enqueue_intervals([], _ChunksToEnqueue, {Q, QIntervals}) ->
	{Q, QIntervals};
enqueue_intervals([{Peer, Intervals} | Rest], ChunksToEnqueue, {Q, QIntervals}) ->
	{Q2, QIntervals2} = enqueue_peer_intervals(Peer, Intervals, ChunksToEnqueue, {Q, QIntervals}),
	enqueue_intervals(Rest, ChunksToEnqueue, {Q2, QIntervals2}).

enqueue_peer_intervals(Peer, Intervals, ChunksToEnqueue, {Q, QIntervals}) ->
	%% Only keep unique intervals. We may get some duplicates for two
	%% reasons:
	%% 1) find_peer_intervals might choose the same interval several
	%%    times in a row even when there are other unsynced intervals
	%%    to pick because it is probabilistic.
	%% 2) We ask many peers simultaneously about the same interval
	%%    to make finding of the relatively rare intervals quicker.
	OuterJoin = ar_intervals:outerjoin(QIntervals, Intervals),
	?LOG_DEBUG([{event, enqueue_peer_intervals}, {pid, self()},
		{peer, ar_util:format_peer(Peer)},
		{num_intervals, gb_sets:size(Intervals)},
		{num_q_intervals, gb_sets:size(QIntervals)},
		{num_outer_join_intervals, gb_sets:size(OuterJoin)}]),
	{_, {Q2, QIntervals2}}  = ar_intervals:fold(
		fun	(_, {0, {QAcc, QIAcc}}) ->
				{0, {QAcc, QIAcc}};
			({End, Start}, {ChunksToEnqueue2, {QAcc, QIAcc}}) ->
				RangeEnd = min(End, Start + (ChunksToEnqueue2 * ?DATA_CHUNK_SIZE)),
				ChunkOffsets = lists:seq(Start, RangeEnd - 1, ?DATA_CHUNK_SIZE),
				ChunksEnqueued = length(ChunkOffsets),
				{ChunksToEnqueue2 - ChunksEnqueued,
					enqueue_peer_range(Peer, Start, RangeEnd, ChunkOffsets, {QAcc, QIAcc})}
		end,
		{ChunksToEnqueue, {Q, QIntervals}},
		OuterJoin
	),
	{Q2, QIntervals2}.

enqueue_peer_range(Peer, RangeStart, RangeEnd, ChunkOffsets, {Q, QIntervals}) ->
	Q2 = lists:foldl(
		fun(ChunkStart, QAcc) ->
			gb_sets:add_element(
				{ChunkStart, min(ChunkStart + ?DATA_CHUNK_SIZE, RangeEnd), Peer},
				QAcc)
		end,
		Q,
		ChunkOffsets
	),
	QIntervals2 = ar_intervals:add(QIntervals, RangeEnd, RangeStart),
	{Q2, QIntervals2}.

unpack_fetched_chunk(Cast, AbsoluteOffset, ChunkArgs, Args, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:is_key({AbsoluteOffset, unpacked}, PackingMap) of
		true ->
			decrement_chunk_cache_size(),
			{noreply, State};
		false ->
			case ar_packing_server:is_buffer_full() of
				true ->
					ar_util:cast_after(1000, self(), Cast),
					{noreply, State};
				false ->
					ar_packing_server:request_unpack({AbsoluteOffset, unpacked}, ChunkArgs),
					{noreply, State#sync_data_state{
							packing_map = PackingMap#{
								{AbsoluteOffset, unpacked} => {unpack_fetched_chunk,
										Args} } }}
			end
	end.

validate_proof(SeekByte, Proof) ->
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,

	ChunkMetadata = #chunk_metadata{
		tx_path = TXPath,
		data_path = DataPath
	},

	ChunkProof = ar_poa:chunk_proof(ChunkMetadata, SeekByte, get_merkle_rebase_threshold()),
	case ar_poa:validate_paths(ChunkProof) of
		{false, _} ->
			false;
		{true, ChunkProof2} ->
			#chunk_proof{
				metadata = Metadata,
				chunk_id = ChunkID,
				block_start_offset = BlockStartOffset,
				chunk_end_offset = ChunkEndOffset,
				tx_start_offset = TXStartOffset
			} = ChunkProof2,
			#chunk_metadata{
				chunk_size = ChunkSize
			} = Metadata,
			AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
			case Packing of
				unpacked ->
					case ar_tx:generate_chunk_id(Chunk) == ChunkID of
						false ->
							false;
						true ->
							case ChunkSize == byte_size(Chunk) of
								true ->
									{true, ChunkProof2};
								false ->
									false
							end
					end;
				_ ->
					{need_unpacking, AbsoluteEndOffset, ChunkProof2}
			end
	end.

validate_proof2(
		TXRoot, TXPath, DataPath, BlockStartOffset, BlockEndOffset, BlockRelativeOffset,
		ExpectedChunkSize, RequestOrigin) ->
	ChunkMetadata = #chunk_metadata{
		tx_root = TXRoot,
		tx_path = TXPath,
		data_path = DataPath
	},
	ValidateDataPathRuleset = ar_poa:get_data_path_validation_ruleset(
		BlockStartOffset, get_merkle_rebase_threshold()),
	AbsoluteOffset = BlockStartOffset + BlockRelativeOffset,
	ChunkProof = ar_poa:chunk_proof(ChunkMetadata, BlockStartOffset, BlockEndOffset, AbsoluteOffset, ValidateDataPathRuleset),
	{IsValid, ChunkProof2} = ar_poa:validate_paths(ChunkProof),
	case IsValid of
		true ->
			#chunk_proof{
				chunk_id = ChunkID,
				chunk_start_offset = ChunkStartOffset,
				chunk_end_offset = ChunkEndOffset
			} = ChunkProof2,
			case ChunkEndOffset - ChunkStartOffset == ExpectedChunkSize of
				false ->
					log_chunk_error(RequestOrigin, failed_to_validate_data_path_offset,
							[{chunk_end_offset, ChunkEndOffset},
							{chunk_start_offset, ChunkStartOffset},
							{chunk_size, ExpectedChunkSize}]),
					false;
				true ->
					{true, ChunkID}
			end;
		false ->
			#chunk_proof{
				tx_path_is_valid = TXPathIsValid,
				data_path_is_valid = DataPathIsValid
			} = ChunkProof2,
			case {TXPathIsValid, DataPathIsValid} of
				{invalid, _} ->
					log_chunk_error(RequestOrigin, failed_to_validate_tx_path,
							[{block_start_offset, BlockStartOffset},
							{block_end_offset, BlockEndOffset},
							{block_relative_offset, BlockRelativeOffset}]),
					false;
				{_, invalid} ->
					log_chunk_error(RequestOrigin, failed_to_validate_data_path,
							[{block_start_offset, BlockStartOffset},
							{block_end_offset, BlockEndOffset},
							{block_relative_offset, BlockRelativeOffset}]),
					false
			end
	end.

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	Base = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath, strict_borders_ruleset),
	Strict = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath,
			strict_data_split_ruleset),
	Rebase = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath,
			offset_rebase_support_ruleset),
	Result =
		case {Base, Strict, Rebase} of
			{false, false, false} ->
				false;
			{_, {_, _, _} = StrictResult, _} ->
				StrictResult;
			{_, _, {_, _, _} = RebaseResult} ->
				RebaseResult;
			{{_, _, _} = BaseResult, _, _} ->
				BaseResult
		end,
	case Result of
		false ->
			false;
		{ChunkID, StartOffset, EndOffset} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							PassesBase = not (Base == false),
							PassesStrict = not (Strict == false),
							PassesRebase = not (Rebase == false),
							{true, PassesBase, PassesStrict, PassesRebase, EndOffset};
						false ->
							false
					end
			end
	end.

chunk_offsets_synced(_, _, _, _, N) when N == 0 ->
	true;
chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset, TXStartOffset, N) ->
	case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ar_data_sync) of
		{{true, _}, _StoreID} ->
			case TXStartOffset of
				0 ->
					true;
				_ ->
					<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
					Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
					case ar_kv:get_prev(DataRootIndex, Key) of
						none ->
							true;
						{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
								TXStartOffset2Size:8,
								TXStartOffset2:(TXStartOffset2Size * 8) >>, _} ->
							chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset,
									TXStartOffset2, N - 1);
						{ok, _, _} ->
							true;
						_ ->
							false
					end
			end;
		false ->
			false
	end.

%% @doc Return a storage reference to the chunk proof (and possibly the chunk itself).
get_chunk_data_key(DataPathHash) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary >>.

write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, StoreID) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			{ok, Packing};
		false ->
			write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath,
					Packing, StoreID)
	end.

write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing,
		StoreID) ->
	ShouldStoreInChunkStorage =
		ar_chunk_storage:is_storage_supported(Offset, ChunkSize, Packing),
	case {ShouldStoreInChunkStorage, is_binary(DataPath)} of
		{true, true} ->
			PaddedOffset = ar_block:get_chunk_padded_offset(Offset),
			case ar_chunk_storage:put(PaddedOffset, Chunk, Packing, StoreID) of
				{ok, NewPacking} ->
					case put_chunk_data(ChunkDataKey, StoreID, DataPath) of
						ok -> {ok, NewPacking};
						Error -> Error
					end;
				Other -> Other
			end;
		{true, false} ->
			%% If ar_data_sync:write_chunk/7 is called directly without a DataPath, we
			%% should just update chunk storage without modifying chunk_data_db. This
			%% can happen, for example, durin grepack in place.
			PaddedOffset = ar_block:get_chunk_padded_offset(Offset),
			ar_chunk_storage:put(PaddedOffset, Chunk, Packing, StoreID);
		{false, true} ->
			case put_chunk_data(ChunkDataKey, StoreID, {Chunk, DataPath}) of
				ok ->
					PackingLabel = ar_storage_module:packing_label(Packing),
					StoreIDLabel = ar_storage_module:label(StoreID),
					prometheus_counter:inc(chunks_stored, [PackingLabel, StoreIDLabel]),
					{ok, Packing};
				Error -> Error
			end;
		{false, false} ->
			%% For chunks which are only stored in chunk_data_db, we currently require that
			%% both the Chunk and the DataPath are present.
			{error, invalid_data_path}
	end.

update_chunks_index(Args, UpdateFootprint, State) ->
	AbsoluteChunkOffset = element(1, Args),
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2(Args, UpdateFootprint, State)
	end.

update_chunks_index2(Args, UpdateFootprint, State) ->
	{AbsoluteOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing} = Args,
	#sync_data_state{ store_id = StoreID } = State,
	Metadata = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	case put_chunk_metadata(AbsoluteOffset, StoreID, Metadata) of
		ok ->
			StartOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
			PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
			case ar_sync_record:add(PaddedOffset, StartOffset, Packing, ar_data_sync, StoreID) of
				ok ->
					case UpdateFootprint of
						true ->
							case ar_footprint_record:add(PaddedOffset, Packing, StoreID) of
								ok ->
									ok;
								{error, Reason} ->
									{error, Reason}
							end;
						false ->
							ok
					end;
				{error, Reason} ->
					{error, Reason}
			end;
		{error, Reason} ->
			{error, Reason}
	end.

pick_missing_blocks([{H, WeaveSize, _} | CurrentBI], BlockTXPairs) ->
	{After, Before} = lists:splitwith(fun({BH, _}) -> BH /= H end, BlockTXPairs),
	case Before of
		[] ->
			pick_missing_blocks(CurrentBI, BlockTXPairs);
		_ ->
			{WeaveSize, lists:reverse(After)}
	end.

process_invalid_fetched_chunk(Peer, Byte, State) ->
	%% Not necessarily a malicious peer, it might happen
	%% if the chunk is recent and from a different fork.
	process_invalid_fetched_chunk(Peer, Byte, State, got_invalid_proof_from_peer, []).
process_invalid_fetched_chunk(Peer, Byte, State, Event, ExtraLogs) ->
	#sync_data_state{ weave_size = WeaveSize } = State,
	?LOG_WARNING([{event, Event}, {peer, ar_util:format_peer(Peer)},
			{byte, Byte}, {weave_size, WeaveSize} | ExtraLogs]),
	{noreply, State}.

process_valid_fetched_chunk(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID, disk_pool_threshold = DiskPoolThreshold } = State,
	{Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
	{AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID,
			ChunkEndOffset, Peer, Byte} = Args,
	case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
		false ->
			?LOG_WARNING([{event, got_too_big_proof_from_peer},
					{peer, ar_util:format_peer(Peer)}]),
			decrement_chunk_cache_size(),
			{noreply, State};
		true ->
			case ar_sync_record:is_recorded(Byte + 1, ar_data_sync, StoreID) of
				{true, _} ->
					%% The chunk has been synced by another job already.
					decrement_chunk_cache_size(),
					{noreply, State};
				false ->
					true = AbsoluteEndOffset == AbsoluteTXStartOffset + ChunkEndOffset,
					case AbsoluteEndOffset >= DiskPoolThreshold of
						true ->
							add_chunk_to_disk_pool(
								DataRoot, DataPath, UnpackedChunk, ChunkEndOffset - 1, TXSize),
							decrement_chunk_cache_size(),
							{noreply, State};
						false ->
							pack_and_store_chunk({DataRoot, AbsoluteEndOffset, TXPath, TXRoot,
									DataPath, Packing, ChunkEndOffset, ChunkSize, Chunk,
									UnpackedChunk, none, none}, State)
					end
			end
	end.

pack_and_store_chunk({_, AbsoluteOffset, _, _, _, _, _, _, _, _, _, _},
		#sync_data_state{ disk_pool_threshold = DiskPoolThreshold } = State)
		when AbsoluteOffset > DiskPoolThreshold ->
	%% We do not put data into storage modules unless it is well confirmed.
	decrement_chunk_cache_size(),
	{noreply, State};
pack_and_store_chunk(Args, State) ->
	{DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath, Packing, Offset, ChunkSize, Chunk,
			UnpackedChunk, OriginStoreID, OriginChunkDataKey} = Args,
	#sync_data_state{ packing_map = PackingMap } = State,
	RequiredPacking = get_required_chunk_packing(AbsoluteOffset, ChunkSize, State),
	PackingStatus =
		case {RequiredPacking, Packing} of
			{Packing, Packing} ->
				{ready, {Packing, Chunk}};
			{DifferentPacking, _} ->
				{need_packing, DifferentPacking}
		end,
	case PackingStatus of
		{ready, {StoredPacking, StoredChunk}} ->
			ChunkArgs = {StoredPacking, StoredChunk, AbsoluteOffset, TXRoot, ChunkSize},
			{noreply, store_chunk(ChunkArgs, {StoredPacking, DataPath, Offset, DataRoot,
					TXPath, OriginStoreID, OriginChunkDataKey}, State)};
		{need_packing, RequiredPacking} ->
			case maps:is_key({AbsoluteOffset, RequiredPacking}, PackingMap) of
				true ->
					decrement_chunk_cache_size(),
					{noreply, State};
				false ->
					case ar_packing_server:is_buffer_full() of
						true ->
							ar_util:cast_after(1000, self(), {pack_and_store_chunk, Args}),
							{noreply, State};
						false ->
							{Packing2, Chunk2} =
								case UnpackedChunk of
									none ->
										{Packing, Chunk};
									_ ->
										{unpacked, UnpackedChunk}
								end,
							ar_packing_server:request_repack({AbsoluteOffset, RequiredPacking},
									{RequiredPacking, Packing2, Chunk2, AbsoluteOffset,
										TXRoot, ChunkSize}),
							PackingArgs = {pack_chunk, {RequiredPacking, DataPath,
									Offset, DataRoot, TXPath, OriginStoreID,
									OriginChunkDataKey}},
							{noreply, State#sync_data_state{
								packing_map = PackingMap#{
									{AbsoluteOffset, RequiredPacking} => PackingArgs }}}
					end
			end
	end.

process_store_chunk_queue(#sync_data_state{ store_chunk_queue_len = StartLen } = State) ->
	process_store_chunk_queue(State, StartLen).

process_store_chunk_queue(#sync_data_state{ store_chunk_queue_len = 0 } = State, _StartLen) ->
	State;
process_store_chunk_queue(State, StartLen) ->
	#sync_data_state{ store_chunk_queue = Q, store_chunk_queue_len = Len,
			store_chunk_queue_threshold = Threshold } = State,
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

			decrement_chunk_cache_size(),
			State2 = State#sync_data_state{ store_chunk_queue = Q2,
					store_chunk_queue_len = Len - 1,
					store_chunk_queue_threshold = min(Threshold2 + 1,
							?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD) },
			process_store_chunk_queue(State2, StartLen);
		false ->
			State
	end.

store_chunk(ChunkArgs, Args, State) ->
	%% Let at least N chunks stack up, then write them in the ascending order,
	%% to reduce out-of-order disk writes causing fragmentation.
	#sync_data_state{ store_chunk_queue = Q, store_chunk_queue_len = Len } = State,
	Now = os:system_time(millisecond),
	Offset = element(3, ChunkArgs),
	Q2 = gb_sets:add_element({Offset, Now, make_ref(), ChunkArgs, Args}, Q),
	State2 = State#sync_data_state{ store_chunk_queue = Q2, store_chunk_queue_len = Len + 1 },
	process_store_chunk_queue(State2).

store_chunk2(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = ChunkArgs,
	{_Packing, DataPath, Offset, DataRoot, TXPath, OriginStoreID, OriginChunkDataKey} = Args,
	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
	StartOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
	%% This will fail if DataPath is not a string - which is fine as it serves as a sanity
	%% check that store_chunk2 is called with valid arguments.
	DataPathHash = crypto:hash(sha256, DataPath),
	ShouldStoreInChunkStorage = ar_chunk_storage:is_storage_supported(AbsoluteOffset,
			ChunkSize, Packing),
	CleanRecord =
		case {ShouldStoreInChunkStorage, ar_storage_module:get_packing(StoreID)} of
			{true, {replica_2_9, _}} ->
				%% The 2.9 chunk storage is write-once.
				ok;
			_ ->
				case ar_footprint_record:delete(PaddedOffset, StoreID) of
					ok ->
						ar_sync_record:delete(PaddedOffset, StartOffset, ar_data_sync, StoreID);
					Error ->
						Error
				end
		end,
	case CleanRecord of
		{error, Reason} ->
			log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot, DataPathHash,
					StoreID),
			{error, Reason};
		ok ->
			ChunkDataKey =
				case StoreID == OriginStoreID of
					true ->
						OriginChunkDataKey;
					_ ->
						get_chunk_data_key(DataPathHash)
				end,
			StoreIndex =
				case write_chunk(AbsoluteOffset, ChunkDataKey, Chunk, ChunkSize, DataPath,
						Packing, StoreID) of
					{ok, NewPacking} ->
						{true, NewPacking};
					Error2 ->
						Error2
				end,
			ProcessAlreadyStored =
				case StoreIndex of
					already_stored ->
						case ar_sync_record:is_recorded(PaddedOffset, Packing, ar_data_sync, StoreID) of
							false ->
								invalidate_bad_data_record({AbsoluteOffset, ChunkSize,
										StoreID, chunk_already_stored_but_not_in_sync_record});
							true ->
								case ar_footprint_record:is_recorded(PaddedOffset, StoreID) of
									false ->
										%% Repair the broken footprint record.
										ar_footprint_record:add(PaddedOffset, Packing, StoreID);
									true ->
										ok
								end
						end,
						already_stored;
					Else ->
						Else
				end,
			case ProcessAlreadyStored of
				{true, Packing2} ->
					UpdateFootprintRecord = is_footprint_record_supported(AbsoluteOffset, ChunkSize, Packing2),
					case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey, TXRoot,
							DataRoot, TXPath, ChunkSize, Packing2}, UpdateFootprintRecord, State) of
						ok ->
							ok;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot,
									DataPathHash, StoreID),
							{error, Reason}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot,
							DataPathHash, StoreID),
					{error, Reason}
			end
	end.

log_failed_to_store_chunk(already_stored,
		AbsoluteOffset, Offset, DataRoot, DataPathHash, StoreID) ->
	?LOG_INFO([{event, chunk_already_stored},
			{absolute_end_offset, AbsoluteOffset},
			{relative_offset, Offset},
			{data_path_hash, ar_util:safe_encode(DataPathHash)},
			{data_root, ar_util:safe_encode(DataRoot)},
			{store_id, StoreID}]);
log_failed_to_store_chunk(not_prepared_yet,
		AbsoluteOffset, Offset, DataRoot, DataPathHash, StoreID) ->
	?LOG_WARNING([{event, chunk_not_prepared_yet},
			{absolute_end_offset, AbsoluteOffset},
			{relative_offset, Offset},
			{data_path_hash, ar_util:safe_encode(DataPathHash)},
			{data_root, ar_util:safe_encode(DataRoot)},
			{store_id, StoreID}]);
log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot, DataPathHash, StoreID) ->
	?LOG_ERROR([{event, failed_to_store_chunk},
			{reason, io_lib:format("~p", [Reason])},
			{absolute_end_offset, AbsoluteOffset},
			{relative_offset, Offset},
			{data_path_hash, ar_util:safe_encode(DataPathHash)},
			{data_root, ar_util:safe_encode(DataRoot)},
			{store_id, StoreID}]).

get_required_chunk_packing(_Offset, _ChunkSize, #sync_data_state{ store_id = ?DEFAULT_MODULE }) ->
	unpacked;
get_required_chunk_packing(Offset, ChunkSize, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	IsEarlySmallChunk =
		Offset =< ar_block:strict_data_split_threshold() andalso ChunkSize < ?DATA_CHUNK_SIZE,
	case IsEarlySmallChunk of
		true ->
			unpacked;
		false ->
			case ar_storage_module:get_packing(StoreID) of
				{replica_2_9, _Addr} ->
					unpacked_padded;
				Packing ->
					Packing
			end
	end.

process_disk_pool_item(State, Key, Value) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex,
			data_root_index = DataRootIndex, store_id = StoreID } = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = parse_disk_pool_chunk(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
			PassedBaseValidation, PassedStrictValidation,
			PassedRebaseValidation} = DiskPoolChunk,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = get_data_root_offset(DataRootKey, StoreID),
	InDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{not_found, false} ->
			%% The chunk was either orphaned or never made it to the chain.
			case ets:member(ar_data_sync_state, move_data_root_index_migration_complete) of
				true ->
					ok = ar_kv:delete(DiskPoolChunksIndex, Key),
					ok = delete_chunk_data(ChunkDataKey, StoreID),
					decrease_occupied_disk_pool_size(ChunkSize, DataRootKey);
				false ->
					%% Do not remove the chunk from the disk pool until the data root index
					%% migration is complete, because the data root might still exist in the
					%% old index.
					ok
			end,
			NextCursor = << Key/binary, <<"a">>/binary >>,
			gen_server:cast(self(), process_disk_pool_item),
			State2 = maybe_reset_disk_pool_full_scan_key(Key, State),
			{noreply, State2#sync_data_state{ disk_pool_cursor = NextCursor }};
		{{ok, {TXStartOffset, _TXPath}}, _} ->
			DataRootIndexIterator = data_root_index_iterator_v2(DataRootKey, TXStartOffset + 1,
					DataRootIndex),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			State2 = State#sync_data_state{ disk_pool_cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key,
					PassedBaseValidation, PassedStrictValidation, PassedRebaseValidation},
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, DataRootIndexIterator,
					true, Args}),
			{noreply, State2}
	end.

decrease_occupied_disk_pool_size(Size, DataRootKey) ->
	ets:update_counter(ar_data_sync_state, disk_pool_size, {2, -Size}),
	prometheus_gauge:dec(pending_chunks_size, Size),
	case ets:lookup(ar_disk_pool_data_roots, DataRootKey) of
		[] ->
			ok;
		[{_, {Size2, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots, {DataRootKey,
					{Size2 - Size, Timestamp, TXIDSet}}),
			ok
	end.

maybe_reset_disk_pool_full_scan_key(Key,
		#sync_data_state{ disk_pool_full_scan_start_key = Key } = State) ->
	State#sync_data_state{ disk_pool_full_scan_start_key = none };
maybe_reset_disk_pool_full_scan_key(_Key, State) ->
	State.

parse_disk_pool_chunk(Bin) ->
	case binary_to_term(Bin) of
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, false, false};
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, PassesStrict} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, PassesStrict, false};
		R ->
			R
	end.

delete_disk_pool_chunk(Iterator, Args, State) ->
	#sync_data_state{
			disk_pool_chunks_index = DiskPoolChunksIndex, store_id = StoreID } = State,
	{Offset, _, ChunkSize, _, _, ChunkDataKey, DiskPoolKey, _, _, _} = Args,
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			ok = ar_kv:delete(DiskPoolChunksIndex, DiskPoolKey),
			ok = delete_chunk_data(ChunkDataKey, StoreID),
			DataRootKey = data_root_index_get_key(Iterator),
			decrease_occupied_disk_pool_size(ChunkSize, DataRootKey);
		{TXArgs, Iterator2} ->
			{TXStartOffset, _TXRoot, _TXPath} = TXArgs,
			AbsoluteOffset = TXStartOffset + Offset,
			case get_chunk_metadata(AbsoluteOffset, StoreID) of
				not_found ->
					ok;
				{ok, ChunkArgs} ->
					case element(1, ChunkArgs) of
						ChunkDataKey ->
							PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
							StartOffset = ar_block:get_chunk_padded_offset(
									AbsoluteOffset - ChunkSize),
							ok = ar_footprint_record:delete(PaddedOffset, StoreID),
							ok = ar_sync_record:delete(PaddedOffset, StartOffset, ar_data_sync,
									StoreID),
							case ar_sync_record:is_recorded(PaddedOffset, ar_data_sync) of
								false ->
									ar_events:send(sync_record,
											{global_remove_range, StartOffset, PaddedOffset});
								_ ->
									ok
							end,
							ok = delete_chunk_metadata(AbsoluteOffset, StoreID);
						_ ->
							%% The entry has been written by the 2.5 version thus has
							%% a different key. We do not want to remove chunks from
							%% the existing 2.5 dataset.
							ok
					end
			end,
			delete_disk_pool_chunk(Iterator2, Args, State)
	end.

register_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:add_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

deregister_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:del_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

get_merkle_rebase_threshold() ->
	case ets:lookup(node_state, merkle_rebase_support_threshold) of
		[] ->
			infinity;
		[{_, Threshold}] ->
			Threshold
	end.

process_disk_pool_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude, Args,
		State) ->
	#sync_data_state{ disk_pool_threshold = DiskPoolThreshold } = State,
	{Offset, _, _, DataRoot, DataPathHash, _, _,
			PassedBase, PassedStrictValidation, PassedRebaseValidation} = Args,
	PassedValidation =
		case {AbsoluteOffset >= get_merkle_rebase_threshold(),
				AbsoluteOffset >= ar_block:strict_data_split_threshold(),
				PassedBase, PassedStrictValidation, PassedRebaseValidation} of
			%% At the rebase threshold we relax some of the validation rules so the strict
			%% validation may fail.
			{true, true, _, _, true} ->
				true;
			%% Between the "strict" and "rebase" thresholds the "base" and "strict split"
			%% rules must be followed.
			{false, true, true, true, _} ->
				true;
			%% Before the strict threshold only the base (most relaxed) validation must
			%% pass.
			{false, false, true, _, _} ->
				true;
			_ ->
				false
		end,
	case PassedValidation of
		false ->
			%% When we accept chunks into the disk pool, we do not know where they will
			%% end up on the weave. Therefore, we cannot require all Merkle proofs pass
			%% the strict validation rules taking effect only after
			%% ar_block:strict_data_split_threshold() or allow the merkle tree offset rebases
			%% supported after the yet another special weave threshold.
			%% Instead we note down whether the chunk passes the strict and rebase validations
			%% and take it into account here where the chunk is associated with a global weave
			%% offset.
			?LOG_INFO([{event, disk_pool_chunk_from_bad_split},
					{absolute_end_offset, AbsoluteOffset},
					{merkle_rebase_threshold, get_merkle_rebase_threshold()},
					{strict_data_split_threshold, ar_block:strict_data_split_threshold()},
					{passed_base, PassedBase}, {passed_strict, PassedStrictValidation},
					{passed_rebase, PassedRebaseValidation},
					{relative_offset, Offset},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)}]),
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
					MayConclude, Args}),
			{noreply, State};
		true ->
			case AbsoluteOffset > DiskPoolThreshold of
				true ->
					process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, Args, State);
				false ->
					process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, MayConclude, Args, State)
			end
	end.

process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, Args,
		State) ->
	#sync_data_state{ store_id = StoreID } = State,
	case ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync, StoreID) of
		{true, unpacked} ->
			%% Pass MayConclude as false because we have encountered an offset
			%% above the disk pool threshold => we need to keep the chunk in the
			%% disk pool for now and not pack and move to the offset-based storage.
			%% The motivation is to keep chain reorganisations cheap.
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false, Args}),
			{noreply, State};
		false ->
			{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _, _, _} = Args,
			case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
					TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, false, State) of
				ok ->
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false,
							Args}),
					{noreply, State};
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_index_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)}
			end
	end.

process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
		Args, State) ->
	%% The chunk has received a decent number of confirmations so we put it in storage
	%% module(s). If we have no storage modules configured covering this offset, proceed to
	%% the next offset. If there are several suitable storage modules, send the chunk
	%% to those modules who have not have it synced yet.
	#sync_data_state{ store_id = DefaultStoreID } = State,
	{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _PassedBaseValidation,
			_PassedStrictValidation, _PassedRebaseValidation} = Args,
	FindStorageModules =
		case ar_storage_module:get_all(AbsoluteOffset - ChunkSize + 1) of
			[] ->
				gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
						MayConclude, Args}),
				{noreply, State};
			Modules ->
				[ar_storage_module:id(Module) || Module <- Modules]
		end,
	IsBlacklisted =
		case FindStorageModules of
			{noreply, State2} ->
				{noreply, State2};
			StoreIDs ->
				case ar_tx_blacklist:is_byte_blacklisted(AbsoluteOffset) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_disk_pool_offset(AbsoluteOffset,
								ChunkDataKey, State)};
					false ->
						StoreIDs
				end
		end,
	IsSynced =
		case IsBlacklisted of
			{noreply, State3} ->
				{noreply, State3};
			StoreIDs2 ->
				case filter_storage_modules_by_synced_offset(AbsoluteOffset, StoreIDs2) of
					[] ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_disk_pool_offset(AbsoluteOffset,
								ChunkDataKey, State)};
					StoreIDs3 ->
						StoreIDs3
				end
		end,
	IsProcessed =
		case IsSynced of
			{noreply, State4} ->
				{noreply, State4};
			StoreIDs4 ->
				case is_recently_processed_offset(AbsoluteOffset, ChunkDataKey, State) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						StoreIDs4
				end
		end,
	IsChunkCacheFull =
		case IsProcessed of
			{noreply, State5} ->
				{noreply, State5};
			StoreIDs5 ->
				case is_chunk_cache_full() of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						StoreIDs5
				end
		end,
	case IsChunkCacheFull of
		{noreply, State6} ->
			{noreply, State6};
		StoreIDs6 ->
			case read_chunk(AbsoluteOffset, ChunkDataKey, DefaultStoreID) of
				not_found ->
					?LOG_ERROR([{event, disk_pool_chunk_not_found},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							MayConclude, Args}),
					{noreply, State};
				{error, Reason2} ->
					?LOG_ERROR([{event, failed_to_read_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason2])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)};
				{ok, {Chunk, DataPath}} ->
					increment_chunk_cache_size(),
					Args2 = {DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath, unpacked,
							Offset, ChunkSize, Chunk, Chunk, none, none},
					[gen_server:cast(name(StoreID6), {pack_and_store_chunk, Args2})
						|| StoreID6 <- StoreIDs6],
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							false, Args}),
					{noreply, cache_recently_processed_offset(AbsoluteOffset, ChunkDataKey,
							State)}
			end
	end.

remove_recently_processed_disk_pool_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	case maps:get(Offset, Map, not_found) of
		not_found ->
			State;
		Set ->
			Set2 = sets:del_element(ChunkDataKey, Set),
			Map2 =
				case sets:is_empty(Set2) of
					true ->
						maps:remove(Offset, Map);
					false ->
						maps:put(Offset, Set2, Map)
				end,
			State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }
	end.

is_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	sets:is_element(ChunkDataKey, Set).

cache_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 =
		case sets:is_element(ChunkDataKey, Set) of
			false ->
				ar_util:cast_after(?CACHE_RECENTLY_PROCESSED_DISK_POOL_OFFSET_LIFETIME_MS,
						self(), {remove_recently_processed_disk_pool_offset, Offset,
						ChunkDataKey}),
				maps:put(Offset, sets:add_element(ChunkDataKey, Set), Map);
			true ->
				Map
		end,
	State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }.

filter_storage_modules_by_synced_offset(AbsoluteOffset, [StoreID | StoreIDs]) ->
	case ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync, StoreID) of
		{true, _Packing} ->
			filter_storage_modules_by_synced_offset(AbsoluteOffset, StoreIDs);
		false ->
			[StoreID | filter_storage_modules_by_synced_offset(AbsoluteOffset, StoreIDs)]
	end;
filter_storage_modules_by_synced_offset(_, []) ->
	[].

process_unpacked_chunk(ChunkArgs, Args, State) ->
	{_AbsoluteTXStartOffset, _TXSize, _DataPath, _TXPath, _DataRoot, _Chunk, ChunkID,
			_ChunkEndOffset, Peer, Byte} = Args,
	{_Packing, Chunk, AbsoluteEndOffset, _TXRoot, ChunkSize} = ChunkArgs,
	case validate_chunk_id_size(Chunk, ChunkID, ChunkSize) of
		false ->
			?LOG_DEBUG([{event, invalid_unpacked_fetched_chunk},
					{absolute_end_offset, AbsoluteEndOffset}]),
			decrement_chunk_cache_size(),
			process_invalid_fetched_chunk(Peer, Byte, State);
		true ->
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end.

validate_chunk_id_size(Chunk, ChunkID, ChunkSize) ->
	case ar_tx:generate_chunk_id(Chunk) == ChunkID of
		false ->
			false;
		true ->
			ChunkSize == byte_size(Chunk)
	end.

log_sufficient_disk_space(StoreID) ->
	ar:console("~nThe node has detected available disk space and resumed syncing data "
			"into the storage module ~s.~n", [StoreID]),
	?LOG_INFO([{event, storage_module_resumed_syncing}, {storage_module, StoreID}]).

log_insufficient_disk_space(StoreID) ->
	ar:console("~nThe node has stopped syncing data into the storage module ~s due to "
			"the insufficient disk space.~n", [StoreID]),
	?LOG_INFO([{event, storage_module_stopped_syncing},
			{reason, insufficient_disk_space}, {storage_module, StoreID}]).

data_root_index_iterator_v2(DataRootKey, TXStartOffset, DataRootIndex) ->
	{DataRootKey, TXStartOffset, TXStartOffset, DataRootIndex, 1}.

data_root_index_next_v2({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
data_root_index_next_v2({_, 0, _, _, _}, _Limit) ->
	none;
data_root_index_next_v2(Args, _Limit) ->
	{DataRootKey, TXStartOffset, LatestTXStartOffset, DataRootIndex, Count} = Args,
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
	case ar_kv:get_prev(DataRootIndex, Key) of
		none ->
			none;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartOffset2Size:8, TXStartOffset2:(TXStartOffset2Size * 8) >>, TXPath} ->
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			{{TXStartOffset2, TXRoot, TXPath},
					{DataRootKey, TXStartOffset2, LatestTXStartOffset, DataRootIndex,
							Count + 1}};
		{ok, _, _} ->
			none
	end.

data_root_index_reset({DataRootKey, _, TXStartOffset, DataRootIndex, _}) ->
	{DataRootKey, TXStartOffset, TXStartOffset, DataRootIndex, 1}.

data_root_index_get_key(Iterator) ->
	element(1, Iterator).

data_root_index_iterator(TXRootMap) ->
	{maps:fold(
		fun(TXRoot, Map, Acc) ->
			maps:fold(
				fun(Offset, TXPath, Acc2) ->
					gb_sets:insert({Offset, TXRoot, TXPath}, Acc2)
				end,
				Acc,
				Map
			)
		end,
		gb_sets:new(),
		TXRootMap
	), 0}.

data_root_index_next({_Index, Count}, Limit) when Count >= Limit ->
	none;
data_root_index_next({Index, Count}, _Limit) ->
	case gb_sets:is_empty(Index) of
		true ->
			none;
		false ->
			{Element, Index2} = gb_sets:take_largest(Index),
			{Element, {Index2, Count + 1}}
	end.

record_chunk_cache_size_metric() ->
	case ets:lookup(ar_data_sync_state, chunk_cache_size) of
		[{_, Size}] ->
			prometheus_gauge:set(chunk_cache_size, Size);
		_ ->
			ok
	end.

%% @doc Get data roots for a given offset (>= BlockStartOffset, < BlockEndOffset) from local indices.
%% Return only entries corresponding to non-empty transactions.
%% Return the complete list of entries in the order they appear in the data root index,
%% which corresponds to sorted #tx records in the block.
%% Return {ok, {TXRoot, BlockSize, [{DataRoot, TXSize, TXStartOffset, TXPath}, ...]}}
%% or {error, Reason}.
get_data_roots_for_offset(Offset) ->
	case Offset > get_disk_pool_threshold() of
		true ->
			{error, not_found};
		false ->
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Offset),
			true = Offset >= BlockStart andalso Offset < BlockEnd,
			StoreID = ?DEFAULT_MODULE,
			DB = {data_root_offset_index, StoreID},
			case ar_kv:get(DB, << BlockStart:?OFFSET_KEY_BITSIZE >>) of
				not_found ->
					{error, not_found};
				{ok, Bin} ->
					{TXRoot2, BlockSize, DataRootIndexKeySet} = binary_to_term(Bin),
					true = TXRoot2 == TXRoot,
					{ok, {TXRoot, BlockSize, lists:sort(
						fun({_DataRoot1, _TXSize1, TXStart1, _TXPath1}, {_DataRoot2, _TXSize2, TXStart2, _TXPath2}) ->
							TXStart1 < TXStart2
						end,
						sets:fold(
							fun(<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Acc) ->
								read_data_root_entries(DataRoot, TXSize, BlockEnd, StoreID, Acc)
							end,
						[],
						DataRootIndexKeySet
					))}}
			end
	end.

%% @doc Return true if the data roots for the given block range are synced, false otherwise.
%% Assert the given BlockEnd and TXRoot match the stored values.
are_data_roots_synced(BlockStart, BlockEnd, TXRoot) ->
	DB = {data_root_offset_index, ?DEFAULT_MODULE},
	case ar_kv:get(DB, << BlockStart:?OFFSET_KEY_BITSIZE >>) of
		not_found ->
			false;
		{ok, Bin} ->
			{TXRoot2, BlockSize, _DataRootIndexKeySet} = binary_to_term(Bin),
			true = TXRoot2 == TXRoot,
			true = BlockSize == BlockEnd - BlockStart,
			true
	end.

read_data_root_entries(_DataRoot, _TXSize, 0, _StoreID, Acc) ->
	Acc;
read_data_root_entries(DataRoot, TXSize, Cursor, StoreID, Acc) ->
	Key = data_root_key_v2(DataRoot, TXSize, Cursor - 1),
	case ar_kv:get_prev({data_root_index, StoreID}, Key) of
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartSize:8, TXStart:(TXStartSize * 8) >>, TXPath} ->
			[{DataRoot, TXSize, TXStart, TXPath}
				| read_data_root_entries(DataRoot, TXSize, TXStart, StoreID, Acc)];
		{ok, _, _} ->
			Acc;
		none ->
			Acc
	end.

maybe_run_footprint_record_initialization(State) ->
	#sync_data_state{ store_id = StoreID, migrations_index = MI } = State,
	Packing = ar_storage_module:get_packing(StoreID),
	case Packing of
		{replica_2_9, _Addr} ->
			{FootprintRecordCursor, InitializationComplete} = get_footprint_record_initialization_state(State),
			case InitializationComplete of
				true ->
					ok;
				false ->
					?LOG_INFO([{event, initializing_footprint_record},
							{cursor, FootprintRecordCursor}, {store_id, StoreID}]),
					gen_server:cast(self(), {initialize_footprint_record, FootprintRecordCursor, Packing})
			end;
		_ ->
			ar_kv:put(MI, ?FOOTPRINT_MIGRATION_CURSOR_KEY, <<"complete">>),
			ok
	end.

get_footprint_record_initialization_state(State) ->
	#sync_data_state{ migrations_index = MI } = State,
	case ar_kv:get(MI, ?FOOTPRINT_MIGRATION_CURSOR_KEY) of
		not_found ->
			{0, false};
		{ok, <<"complete">>} ->
			{complete, true};
		{ok, CursorBin} ->
			Cursor = binary:decode_unsigned(CursorBin),
			{Cursor, false}
	end.

%% @doc Initialize the footprint record from the ar_data_sync record.
initialize_footprint_record(complete, _Packing, State) ->
	State;
initialize_footprint_record(Cursor, Packing, State) ->
	#sync_data_state{
		store_id = StoreID,
		range_end = RangeEnd,
		migrations_index = MI
	} = State,
	BatchSize = ?FOOTPRINT_MIGRATION_BATCH_SIZE,

	case ar_sync_record:get_next_synced_interval(Cursor, RangeEnd, Packing, ar_data_sync, StoreID) of
		not_found ->
			ok = ar_kv:put(MI, ?FOOTPRINT_MIGRATION_CURSOR_KEY, <<"complete">>),
			?LOG_INFO([{event, footprint_record_initialized}, {store_id, StoreID}]),
			State;
		{IntervalEnd, IntervalStart} ->
			Cursor2 = max(Cursor, IntervalStart),
			EndPosition = min(Cursor2 + (BatchSize * ?DATA_CHUNK_SIZE), IntervalEnd),
			initialize_footprint_range(Cursor2, EndPosition, Packing, StoreID),
			NewCursor = EndPosition,
			ok = ar_kv:put(MI, ?FOOTPRINT_MIGRATION_CURSOR_KEY, binary:encode_unsigned(NewCursor)),
			gen_server:cast(self(), {initialize_footprint_record, NewCursor, Packing}),
			State
	end.

%% @doc Migrate chunks in the given range to footprint records.
initialize_footprint_range(Start, End, _Packing, _StoreID) when Start >= End ->
	ok;
initialize_footprint_range(Start, End, Packing, StoreID) ->
	ChunkOffset = ar_block:get_chunk_padded_offset(Start + ?DATA_CHUNK_SIZE),
	case ChunkOffset =< End of
		true ->
			ar_footprint_record:add(ChunkOffset, Packing, StoreID),
			initialize_footprint_range(ChunkOffset, End, Packing, StoreID);
		false ->
			ok
	end.