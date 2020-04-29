-module(ar_data_sync).
-behaviour(gen_server).

-export([start_link/1]).

-export([add_block/4, add_chunk/4, get_chunk/1, get_tx_data/1]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

add_block(TXRoot, Height, SizeTaggedTXIDs, WeaveSize) ->
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
	ok.

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
	%% The global offset for the chunk is Offset + BlockOffset.
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
			%% - #state.chunks_index
			ok
	end.

get_chunk(Offset) ->
	%% Look if the offset is synced in #state.sync_record.
	%% If yes, fetch the chunk with its proof from #state.chunk_index.
	%% If not, return {error, chunk_not_found}.
	ok.

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
	ok.

%% Pick a random [start, end) half-closed interval of
%% ?SYNC_CHUNK_SIZE or smaller which is not synced and sync it. The
%% intervals are chosen from [0, confirmed weave size).
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
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(#{ block_index := BI, maintain_tx_index := MaintainTXIndex }) ->
	%% Load state from disk, if any, otherwise construct it from the
	%% block index. Store the new state.
	State = #state{},
	{ok, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

sync_record_from_block_index(BI) ->
	Intervals = sync_record_from_block_index([], BI),
	compact_intervals(Intervals).

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
		#tx{ format = 1, data_size = DataSize } when DataSize > 0 ->
			sync_record_from_txs(
				TXIDs, [{Offset - DataSize, Offset} | SyncRecord], Offset - DataSize);
		#tx{ format = 2, data_size = DataSize, id = ID } when DataSize > 0 ->
			case filelib:is_file(ar_storage:tx_data_filepath(ID)) of
				true ->
					sync_record_from_txs(
						TXIDs, [{Offset - DataSize, Offset} | SyncRecord], Offset - DataSize);
				false ->
					sync_record_from_txs(TXIDs, SyncRecord, Offset - DataSize)
			end;
		#tx{ data_size = 0 } ->
			sync_record_from_txs(TXIDs, SyncRecord, Offset)
	end;
sync_record_from_txs([], SyncRecord, _Offset) ->
	SyncRecord.

tx_root_index_and_block_offset_index_from_block_index([{_, WeaveSize, TXRoot} | BI]) ->
	ok;
tx_root_index_and_block_offset_index_from_block_index([]) ->
	ok.

compact_intervals([{Start, End}, {End, NextStart} | Rest]) ->
	compact_intervals([{Start, NextStart} | Rest]);
compact_intervals([Interval | Rest]) ->
	[Interval | compact_intervals(Rest)];
compact_intervals([]) ->
	[].

validate_proof(#poa{ tx_path = TXPath, data_path = DataPath }, TXRoot, Offset) ->
	case ar_merkle:validate_path(TXRoot, Offset, TXPath) of
		false ->
			{error, invalid_proof};
		{TXRoot, StartOffset, _} ->
			case ar_merkle:extract_root(DataPath) of
				{error, invalid_proof} ->
					{error, invalid_proof};
				{ok, DataRoot} ->
					case ar_merkle:validate_path(DataRoot, Offset - StartOffset, DataPath) of
						false ->
							{error, invalid_proof};
						{DataRoot, _, _} ->
							valid
					end
			end
	end.
