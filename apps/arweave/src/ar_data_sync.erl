-module(ar_data_sync).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2]).
-export([add_block/6, add_chunk/4, get_chunk/1, get_tx_data/1]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_kv.hrl").

-define(INIT_KV(NAME), (ar_kv:init(NAME, ?ROCKSDB_OPTIONS))).
-define(INIT_KV_SEEK(DB), (ar_kv:init_seek(DB, ?ROCKSDB_ITR_OPTIONS))).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

add_block(TXRoot, Height, SizeTaggedTXIDs, WeaveSize, BI, POA) ->
	%% Updates
	%% - #state.sync_tip:
	%%   - tx_roots_by_height
	%%   - confirmed_height (if unconfirmed_tx_roots grows enough)
	%%   - confirmed_tx_root (if unconfirmed_tx_roots grows enough)
	%%   - unconfirmed_tx_roots
	%%   - size_tagged_tx_ids_by_tx_root (if maintain_tx_index is true)
	%%   - block_offset_by_tx_root
	%% - #state.confirmed_size
	%% - #state.sync_record (if #sync_tip.confirmed_height is advanced)
	%% - #state.tx_root_index (if #sync_tip.confirmed_height is advanced)
	%% - #state.chunk_index (if #sync_tip.confirmed_height is advanced & there are new chunks)
	%% - #state.tx_index (if #sync_tip.confirmed_height is advanced & there are new txs)
	gen_server:call(?MODULE, {add_block, TXRoot, Height, SizeTaggedTXIDs, WeaveSize, BI, POA}).

add_chunk(ChunkID, Chunk, Offset, #poa{ tx_path = TXPath } = POA) ->
	case ar_merkle:extract_root(TXPath) of
		{error, invalid_proof} ->
			{error, invalid_proof};
		{ok, TXRoot} ->
			add_chunk(ChunkID, Chunk, Offset, TXRoot, POA)
	end.

add_chunk(ChunkID, Chunk, Offset, TXRoot, POA) ->
	%% If TXRoot is not in #sync_tip.block_offset_by_tx_root and
	%% not in #state.block_offset_index, return {error, tx_root_not_found}.
	%% The global offset for the chunk is Offset + BlockStartOffset.
	case validate_proof(POA, TXRoot, Offset) of
		invalid ->
			{error, invalid_proof};
		valid ->
			%% Update #sync_tip or #state.
			%% In #sync_tip, update:
			%% - chunk_ids_by_tx_root
			%% - tx_roots_with_proofs_by_chunk_id
			%% - chunk_ids_by_tx_id (if maintain_tx_index is true)
			%% - size_tagged_tx_ids_by_tx_root (if maintain_tx_index is true)
			%% In #state, update:
			%% - #state.sync_record
			%% - #state.chunks_index,
			gen_server:call(?MODULE, {add_chunk, ChunkID, Chunk, Offset, TXRoot, POA})
	end.

get_chunk(Offset) ->
	%% Look if the offset is synced in #state.sync_record.
	%% If yes, fetch the chunk with its proof from #state.chunk_index.
	%% If not, return {error, chunk_not_found}.
	gen_server:call(?MODULE, {get_chunk, Offset}).

get_tx_data(TXID) ->
	%% Look if there are chunk identifiers in #sync_tip.chunk_ids_by_tx_id.
	%% If yes, try to reconstruct the transaction data.
	%%   If chunks are missing, return {error, tx_data_not_found}.
	%%   If not, return tx data.
	%% If not, look for the tx offset in #state.tx_index.
	%%   If present, try to reconstruct tx data by fetching chunks by offset
	%%   from state#chunk_index.
	%%   If chunks are missing, return {error, tx_data_not_found}.
	%%   If not, return tx data.
	gen_server:call(?MODULE, {get_tx_data, TXID}).

%% Pick a random (start, end] half-closed interval of
%% ?SYNC_CHUNK_SIZE or smaller which is not synced and sync it. The
%% intervals are chosen from (0, confirmed weave size].
sync_random_chunk() ->
	%% Pick a random not synced interval =< ?SYNC_CHUNK_SIZE.

	%% Starting from its upper bound, request chunks until the interval is synced.
	%%
	%% To request a chunk for the chosen offset:
	%% - lookup the transaction root for the given offset in #state.tx_root_index;
	%% - download chunk via GET /chunk/<offset>;
	%% - compute the chunk offset relative to the block by subtracting block offset
	%%   from the chosen byte;
	%% - validate the proof via validate_proof/3;
	%% - update #state.chunk_index.
	gen_server:call(?MODULE, sync_random_chunk).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(#{ block_index := BI, maintain_tx_index := MaintainTXIndex }) ->
	%% Load state from disk, if any, otherwise construct it from the
	%% block index. Store the new state.
	gen_server:cast(?MODULE, {init, BI}),
	{ok, #state{
			db = ?INIT_KV("kv-data-sync"),
			tx_root_index = ?INIT_KV("kv-tx-root-index"),
			block_offset_index = ?INIT_KV("kv-block-offset-index"),
			tx_index = init_tx_index(MaintainTXIndex),
			maintain_tx_index = MaintainTXIndex}}.

handle_call({add_block, TXRoot, Height, SizeTaggedTXIDs, WeaveSize, BI, POA}, _From, State) ->
	%% extract state fileds
	#state{	db = DB,
		sync_record = SyncRecord,
		sync_tip = SyncTip =
			#sync_tip{
				tx_roots_by_height= TXRootsByHeight,
				confirmed_height = ConfirmedHeight,
				unconfirmed_tx_roots = UnConfirmedTXRoots,
				size_tagged_tx_ids_by_tx_root = SizeTaggedTXIDsByRoot,
				block_offset_by_tx_root = BlockOffsetByTXRoot},
		maintain_tx_index  = MaintainTXIndex,
		chunks_index = ChunksIndex,
		tx_root_index = TXRootIndex,
		tx_index = TXIndex} = State,

	%% update 'tx_roots_by_height'
	TxRootsByHeight0 = map:put(TXRoot, Height, TXRootsByHeight),
	ar_kv:put(DB, tx_roots_by_height, TxRootsByHeight0),
	SyncTip1 = SyncTip#sync_tip{tx_roots_by_height = TxRootsByHeight0},

	%% update 'unconfirmed_tx_roots'
	UnConfirmedTXRoots0 = [TXRoot | UnConfirmedTXRoots],
	SyncTip2 = SyncTip1#sync_tip{unconfirmed_tx_roots = UnConfirmedTXRoots0},

	% update 'confirmed_height' and 'confirmed_tx_root'
	SyncTip3 = if length(UnConfirmedTXRoots0) >= ?TRACK_CONFIRMATIONS ->
			%% 1. Erase synched data from 'orphaned' TX roots
			%% 2. Update 'confirmed_height' and 'confirmed_tx_root'
			SyncTip2#sync_tip{
				confirmed_height = Height,
				confirmed_tx_root = TXRoot};
		true ->
			SyncTip2
	end,

	%% update 'size_tagged_tx_ids_by_tx_root' if 'maintain_tx_index' = true
	SyncTip4 = if MaintainTXIndex ->
			SizeTaggedTXIDsByRoot0 = map:put(TXRoot, SizeTaggedTXIDs, SizeTaggedTXIDsByRoot),
			ar_kv:put(DB, size_tagged_tx_ids_by_tx_root, SizeTaggedTXIDsByRoot0),
			SyncTip3#sync_tip{size_tagged_tx_ids_by_tx_root = SizeTaggedTXIDsByRoot0};
		true ->
			SyncTip3
	end,

	%% update 'block_offset_by_tx_root'
	{_, BlockSize} = hd(lists:reverse(SizeTaggedTXIDs)),
	BlockOffset = WeaveSize - BlockSize,
	BlockOffsetByTXRoot0 = map:put(TXRoot, BlockOffset, BlockOffsetByTXRoot),
	ar_kv:put(DB, block_offset_by_tx_root, BlockOffsetByTXRoot0),
	SyncTip5 = SyncTip4#sync_tip{block_offset_by_tx_root = BlockOffsetByTXRoot0},

	%% update 'sync_record'
	NewSyncRecord =
		if ConfirmedHeight =/= SyncTip5#sync_tip.confirmed_height ->
			TXRootSyncRec =
				sync_record_from_block_index(
					[hd(BI)],
					TXRoot,
					TXIndex,
					TXRootIndex,
					ChunksIndex,
					POA),
			[TXRootSyncRec | SyncRecord];
		true ->
			SyncRecord
	end,

	{reply, ok, State#state{
				 sync_record = NewSyncRecord,
				 confirmed_size = WeaveSize,
				 sync_tip = SyncTip5}};

handle_call({add_chunk, ChunkID, Chunk, Offset, TXRoot, POA}, _From, State) ->
	%% extract state fileds
	#state{
		db = DB,
		sync_record = SyncRecord,
		sync_tip = SyncTip =
			#sync_tip{
				chunk_ids_by_tx_id = ChunkIDsByTXID,
				chunk_ids_by_tx_root = ChunkIDsByTXRoot,
				tx_roots_with_proofs_by_chunk_id = TXRootsPOAByChunkID,
				block_offset_by_tx_root = BlockOffsetByTXRoot},
		maintain_tx_index  = MaintainTXIndex,
		chunks_index = ChunksIndex} = State,

	{Reply, FinalState} =
		case map:get(TXRoot, BlockOffsetByTXRoot, undefined) of
			undefined ->
				{{error, tx_root_not_found}, State};
			Offset when is_number(Offset) ->
				%% update 'chunk_ids_by_tx_root'
				TXRootChunkIDs = map:get(TXRoot, ChunkIDsByTXRoot, []),
				ChunkIDsByTXRoot0 = map:put(TXRoot, [ChunkID | TXRootChunkIDs], ChunkIDsByTXRoot),
				ar_kv:put(DB, chunk_ids_by_tx_root, ChunkIDsByTXRoot0),
				SyncTip1 = SyncTip#sync_tip{chunk_ids_by_tx_root = ChunkIDsByTXRoot0},

				%% update 'tx_roots_with_proofs_by_chunk_id'
				TXRootPOA = map:get(ChunkID, TXRootsPOAByChunkID, []),
				TXRootsPOAByChunkID = map:put(ChunkID, [{TXRoot, POA} | TXRootPOA], TXRootsPOAByChunkID),
				ar_kv:put(DB, tx_roots_with_proofs_by_chunk_id, TXRootsPOAByChunkID),
				SyncTip2 = SyncTip1#sync_tip{tx_roots_with_proofs_by_chunk_id = TXRootsPOAByChunkID},

				%% update 'chunk_ids_by_tx_id'
				SyncTip3 =
					if MaintainTXIndex ->
							%% seek TXID
							{_Offset0, TXID} = ar_kv:seek(erlang:get(<<"itr-tx-index">>), Offset),
							ChunkIDs = map:get(TXID, ChunkIDsByTXID),
							ChunkIDsByTXID0 = map:put(TXID, [ChunkID | ChunkIDs], ChunkIDsByTXID),
							SyncTip2#sync_tip{chunk_ids_by_tx_id = ChunkIDsByTXID0};
						true ->
							SyncTip2
					end,

				%% update 'sync_record' and 'chunks_index'
				SyncRecord0 = map:put(Offset, Offset + byte_size(Chunk), SyncRecord),
				NewChunksIndexEntry = case ar_kv:get(ChunksIndex, Offset) of
					{ChunkIDs0, POA} ->
						{[ChunkID | ChunkIDs0], POA};
					_ ->
						{[ChunkID], POA}
				end,
				ar_kv:put(ChunksIndex, Offset, NewChunksIndexEntry),

				{ok, State#state{sync_record = SyncRecord0, sync_tip = SyncTip3}}
		end,
	{reply, Reply, FinalState};

handle_call({get_chunk, Offset}, _From, State) ->
	%% Look if the offset is synced in #state.sync_record.
	%% If yes, fetch the chunk with its proof from #state.chunk_index.
	%% If not, return {error, chunk_not_found}.
	#state{
		sync_record = SyncRecord,
		chunks_index = ChunksIndex} = State,
	Reply =
		%% seek offset interval
		case get_synched_offset(Offset, SyncRecord) of
			chunk_not_found = Reason ->
				{error, Reason};
			{_Start, EndOffset} ->
				ar_kv:get(ChunksIndex, EndOffset)
		end,
	{reply, Reply, State};

handle_call({get_tx_data, TXID}, _From, State) ->
	% case map:get(TXID, ChunkIDsByTXID, undefined) of
	% 	ChunkIDs = [_|_] ->
	% 		if MissingChunks ->
	% 				{error, not_found};
	% 			true ->
	% 				"construct TX from ChunkIDs"
	% 		end;
	% 	undefined ->
	% 		case ar_kv:get(TXIndex, TXID) of
	% 			Offset ->
	% 				"construct TX from Offset";
	% 			_ ->
	% 				{error, not_found}
	% 		end
	% end,
	{reply, ok, State};

handle_call(sync_random_chunk, _From,
	State = #state{
		sync_record = SyncRecord,
		confirmed_size = ConfirmedSize}) when ConfirmedSize > 0 ->
	UnsynchedInterval =
		if length(SyncRecord) > 0 ->
			EnsureSortedSyncRecord = lists:keysort(2, SyncRecord),
			unsynched_interval(EnsureSortedSyncRecord);
		true ->
			unavailable
		end,

	{Reply, NewState} = get_interval_chunks(UnsynchedInterval, State),

	{reply, Reply, NewState}.

handle_cast({init, _BI}, State = #state{db = DB}) ->
	RestoreFromDisk = load_state_from_disk(DB),
	UpdatedState =
		if RestoreFromDisk ->
				restore_state(DB, State#state{});
			true ->
				%% State#state{sync_record = sync_record_from_block_index(BI)}
				State#state{}
		end,
	init_iterators(UpdatedState),
	{noreply, UpdatedState};

handle_cast(_Msg, State) ->
	{noreply, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

sync_record_from_block_index(BI, TXRoot, TXIndex, TXRootIndex, ChunksIndex, POA) ->
	Intervals = sync_record_from_block_index([], BI),
	compact_intervals(Intervals, TXRoot, TXIndex, TXRootIndex, ChunksIndex, POA).

sync_record_from_block_index(SyncRecord, [{BH, WeaveSize, _} | BI]) ->
	case ar_storage:read_block_shadow(BH) of
		unavailable ->
			sync_record_from_block_index(SyncRecord, BI);
		#block{ txs = TXIDs } ->
			SortedTXIDs = lists:reverse(lists:sort(TXIDs)),
			UpdatedSyncRecord = lists:foldr(
				fun(Interval, Acc) ->
					[Interval | Acc]
				end,
				SyncRecord,
				sync_record_from_txs(SortedTXIDs, WeaveSize)
			),
			sync_record_from_block_index(UpdatedSyncRecord, BI)
	end;
sync_record_from_block_index(SyncRecord, []) ->
	SyncRecord.

sync_record_from_txs(TXIDs, WeaveSize) ->
	sync_record_from_txs(TXIDs, [], WeaveSize).

sync_record_from_txs([TXID | TXIDs], SyncRecord, Offset) ->
	case ar_storage:read_tx(TXID) of
		unavailable ->
			SyncRecord;
		#tx{ format = 1, data_size = DataSize, id = ID } = TX when DataSize > 0 ->
			TXSizedChunkIDs = tx_sized_chunk_ids(TX),
			sync_record_from_txs(
				TXIDs,
				[{Offset - DataSize, Offset, ID, TXSizedChunkIDs} | SyncRecord],
				Offset - DataSize);
		#tx{ format = 2, data_size = DataSize, id = ID } = TX when DataSize > 0 ->
			case filelib:is_file(ar_storage:tx_data_filepath(ID)) of
				true ->
					TXSizedChunkIDs = tx_sized_chunk_ids(TX),
					sync_record_from_txs(
						TXIDs,
						[{Offset - DataSize, Offset, ID, TXSizedChunkIDs} | SyncRecord],
						Offset - DataSize
					);
				false ->
					sync_record_from_txs(TXIDs, SyncRecord, Offset - DataSize)
			end;
		#tx{ data_size = 0 } ->
			sync_record_from_txs(TXIDs, SyncRecord, Offset)
	end;
sync_record_from_txs([], SyncRecord, _Offset) ->
	SyncRecord.

tx_sized_chunk_ids(TX) ->
	ar_tx:sized_chunks_to_sized_chunk_ids(
				ar_tx:chunks_to_size_tagged_chunks(
					ar_tx:chunk_binary(?DATA_CHUNK_SIZE, TX#tx.data)
				)
			).

tx_root_index_and_block_offset_index_from_block_index([{_, WeaveSize, TXRoot} | BI]) ->
	ok;
tx_root_index_and_block_offset_index_from_block_index([]) ->
	ok.

compact_intervals(
	[{Start, End, TXID, ChunkIDs}, {End, NextStart, NextTXID, NextChunkIDs} | Rest],
	TXRoot,
	TXIndex,
	TXRootIndex,
	ChunksIndex,
	POA) ->
		update_records(End, TXRoot, TXIndex, TXRootIndex, ChunksIndex, TXID, ChunkIDs, POA),
		compact_intervals(
			[{Start, NextStart, NextTXID, NextChunkIDs} | Rest],
			TXRoot,
			TXIndex,
			TXRootIndex,
			ChunksIndex,
			POA);
compact_intervals(
	[{Start, End, TXIDs, ChunkIDs} | Rest],
	TXRoot,
	TXIndex,
	TXRootIndex,
	ChunksIndex,
	POA) ->
		update_records(End, TXRoot, TXIndex, TXRootIndex, ChunksIndex, TXIDs, ChunkIDs, POA),
		[{Start, End} | compact_intervals(Rest, TXRoot, TXIndex, TXRootIndex, ChunksIndex, POA)];
compact_intervals([], _TXRoot, _TXIndex, _TXRootIndex, _ChunksIndex, _POA) ->
	[].

update_records(EndOffset, TXRoot, TXIndex, TXRootIndex, ChunksIndex, TXID, ChunkIDs, POA) ->
	ar_kv:put(TXIndex, TXID, EndOffset),
	ar_kv:put(TXRootIndex, EndOffset, TXRoot),
	ar_kv:put(ChunksIndex, EndOffset, {ChunkIDs, POA}).

validate_proof(
	#poa{ tx_path = TXPath, data_path = DataPath, chunk = Chunk },
	TXRoot,
	Offset
) ->
	case ar_merkle:validate_path(TXRoot, Offset, TXPath) of
		false ->
			{error, invalid_proof};
		{DataRoot, StartOffset, _} ->
			case ar_merkle:validate_path(DataRoot, Offset - StartOffset, DataPath) of
				false ->
					{error, invalid_proof};
				{ChunkID, _, _} ->
					case ChunkID == ar_tx:generate_chunk_id(Chunk) of
						true ->
							valid;
						false ->
							{error, invalid_proof}
					end
			end
	end.

load_state_from_disk(DB) ->
	case ar_kv:get(DB, load_state_from_disk) of
		LoadFromDisk when is_boolean(LoadFromDisk) ->
			LoadFromDisk;
		_ ->
			false
	end.

restore_state(DB, State) ->
	State#state{
		sync_record = ar_kv:get(DB, sync_record),
		confirmed_size = ar_kv:get(DB, confirmed_size),
		sync_tip =
			#sync_tip{
				tx_roots_by_height = ar_kv:get(DB, tx_roots_by_height),
				chunk_ids_by_tx_root = ar_kv:get(DB, chunk_ids_by_tx_root),
				tx_roots_with_proofs_by_chunk_id = ar_kv:get(DB, tx_roots_with_proofs_by_chunk_id),
				confirmed_height = ar_kv:get(DB, confirmed_height),
				confirmed_tx_root = ar_kv:get(DB, confirmed_tx_root),
				chunk_ids_by_tx_id = ar_kv:get(DB, chunk_ids_by_tx_id),
				size_tagged_tx_ids_by_tx_root = ar_kv:get(DB, size_tagged_tx_ids_by_tx_root),
				block_offset_by_tx_root = ar_kv:get(DB, block_offset_by_tx_root)
			}
	}.

init_tx_index(true) -> ?INIT_KV("kv-tx-index");
init_tx_index(_)    -> unavailable.

unsynched_interval([]) -> not_found;
unsynched_interval([{_Start, End}, Next = {End, _NextEnd} | Rem]) ->
	unsynched_interval([Next | Rem]);
unsynched_interval([{_Start, End}, {NextStart, _NextEnd} | _Rem]) ->
	{End, NextStart};
unsynched_interval([_| Rem]) -> unsynched_interval(Rem).

get_synched_offset(_Offset, []) -> chunk_not_found;
get_synched_offset(Offset, [{Start, End} = Interval | _Rem])
	when Offset >= Start; Offset < End -> Interval;
get_synched_offset(Offset, [_| Rem]) ->
	get_synched_offset(Offset, Rem).

init_iterators(
	#state{
		db = DB,
		tx_index = TXIndex,
		tx_root_index = TXRootIndex,
		block_offset_index = BlockOffsetIndex,
		maintain_tx_index = MaintainTXIndex}) ->
	erlang:put(<<"itr-data-sync">>, ?INIT_KV_SEEK(DB)),
	erlang:put(<<"itr-tx-root-index">>, ?INIT_KV_SEEK(TXRootIndex)),
	erlang:put(<<"itr-block-offset-index">>, ?INIT_KV_SEEK(BlockOffsetIndex)),
	erlang:put(<<"itr-data-sync">>, ?INIT_KV_SEEK(BlockOffsetIndex)),
	if MaintainTXIndex ->
			erlang:put(<<"itr-tx-index">>, ?INIT_KV_SEEK(TXIndex));
		true ->
			ok
	end,
	ok.

get_interval_chunks(UnsynchedInterval, State) ->
	%% fetch unsyched interval chunks...
	{ok, State}.
