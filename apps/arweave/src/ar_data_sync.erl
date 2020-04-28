-module(ar_data_sync).
-behaviour(gen_server).

-export([start_link/1]).

-export([add_block/4, add_chunk/3, get_chunk/1, get_tx_data/1]).

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
	%% - #state.confirmed_size
	%% - #state.sync_record (if #sync_tip.confirmed_height is advanced)
	%% - #state.tx_root_index (if #sync_tip.confirmed_height is advanced)
	%% - #state.chunk_index (if #sync_tip.confirmed_height is advanced & there are new chunks)
	%% - #state.tx_index (if #sync_tip.confirmed_height is advanced & there are new txs)
	ok.

add_chunk(ChunkID, Chunk, Proof) ->
	%% If the transaction root (part of the proof) is found in
	%% #sync_tip.unconfirmed_tx_roots, validate proof; if valid, update sync_tip:
	%% - chunk_ids_by_tx_root
	%% - tx_roots_with_proofs_by_chunk_id
	%% - chunk_ids_by_tx_id (if maintain_tx_index is true)
	%% - size_tagged_tx_ids_by_tx_root (if maintain_tx_index is true)
	%% If proof is not valid, return {error, invalid_proof}.
	%% Else if the transaction root is found in #state.tx_root_index, validate proof; update:
	%% - #state.sync_record
	%% - #state.chunks_index
	%% Otherwise return {error, tx_root_not_found}.
	ok.

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
