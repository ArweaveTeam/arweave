%%% @doc Per-store packing, storage, indexes, and persisted sync records.
%%% Network sync and ingestion are owned by arweave_sync.
-module(ar_data_sync).
-test_category([fast]).

-behaviour(gen_server).

-export([name/1, start_link/2,
         register_workers/0, join/1,
         add_tip_block/2, add_block/2,
         invalidate_bad_data_record/5, is_chunk_proof_ratio_attractive/3,
         get_chunk/2, get_chunk_data/2, get_chunk_proof/2,
         get_tx_data/1, get_tx_data/2,
         get_tx_offset/1, get_tx_offset_data_in_range/2,
         request_tx_data_removal/3, request_data_removal/4,
        is_disk_space_sufficient/1,
         init_sync_status/1,
         get_chunk_by_byte/2, advance_chunks_index_cursor/1, has_data_root/2,
         read_chunk_with_full_metadata/2, read_chunk_with_datapath/2,
         write_chunk/5, read_data_path/2,
         get_chunk_metadata_range/3, get_merkle_rebase_threshold/0,
         migration_db/1]).

%% Exported for ar_disk_pool
-export([put_chunk_data/3, delete_chunk_data/2, get_chunk_metadata/2,
         delete_chunk_metadata/2, update_chunks_index/3, get_tx_offset/2]).

%% For data-doctor tools
-export([init_kv/2, open_store_dbs/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-export([store_chunk/4, cancel_store_requests/2, validate_fetched_chunk/3,
        validate_chunk_id_size/3]).

-include("ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include("ar_sup.hrl").
-include("ar_poa.hrl").
-include("ar_data_sync.hrl").


%% The migrations_index key that 2.9.6-alpha1 through 2.9.7-alpha1 wrote when
%% they built the footprint record chunk by chunk: <<"complete">> when they
%% finished, a byte cursor while still running. The storage app keeps its own
%% cursor now; this one is only read, so a downgrade to one of those releases
%% still finds it.
-define(LEGACY_FOOTPRINT_MIGRATION_CURSOR_KEY,
        <<"footprint_migration_cursor">>).

%%%===================================================================
%%% Public interface.
%%%===================================================================

name(StoreID) when is_atom(StoreID) ->
    list_to_atom("ar_data_sync_" ++ atom_to_list(StoreID));
name(StoreID) ->
    #store_info{label = Label} = arweave_storage:store_info(StoreID),
    list_to_atom("ar_data_sync_" ++ Label).

start_link(Name, Args) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

%% @doc Register the workers that will be monitored by ar_data_sync_sup.erl.
register_workers() ->
    StorageModuleWorkers = lists:map(
                             fun(StorageModule) ->
                                     #store_info{
                                         id = StoreID, label = StoreLabel
                                     } = arweave_storage:store_info(StorageModule),
                                     Name = list_to_atom("ar_data_sync_" ++ StoreLabel),
                                     ?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, none}])
                             end,
                             arweave_config:storage_modules()
                            ),
    DefaultStorageModuleWorker = ?CHILD_WITH_ARGS(ar_data_sync, worker,
                                                  ar_data_sync_default, [ar_data_sync_default, {?DEFAULT_MODULE, none}]),
    RepackInPlaceWorkers = lists:map(
                             fun({StorageModule, TargetPacking}) ->
                                     #store_info{id = StoreID} =
                                         arweave_storage:store_info(StorageModule),
                                     Name = ar_data_sync:name(StoreID),
                                     ?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, TargetPacking}])
                             end,
                             arweave_config:repack_modules(full)
                            ),
    StorageModuleWorkers ++ [DefaultStorageModuleWorker] ++ RepackInPlaceWorkers.

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool or in the index.
has_data_root(DataRoot, DataSize) ->
    DataRootID = ar_data_roots:id(DataRoot, DataSize),
    case ar_disk_pool:has_data_root(DataRootID) of
        true ->
            true;
        false ->
            ar_data_roots:is_synced(DataRootID, ?DEFAULT_MODULE)
    end.

%% @doc Notify the server the node has joined the network on the given block index.
join(RecentBI) ->
    gen_server:cast(ar_data_sync_default, {join, RecentBI}).

%% @doc Notify the server about the new tip block.
add_tip_block(BlockTXPairs, RecentBI) ->
    gen_server:cast(ar_data_sync_default, {add_tip_block, BlockTXPairs, RecentBI}).

invalidate_bad_data_record(AbsoluteEndOffset, ChunkSize, StoreID, ChunkDataKey, Case) ->
    invalidate_bad_data_record({AbsoluteEndOffset, ChunkSize, StoreID, ChunkDataKey, Case}).

%% @doc Pack and store a chunk via a PID or registered name, replying to the
%% supplied opaque request ID.
store_chunk(undefined, _Args, _ReplyTo, _CacheRef) ->
    {error, not_initialized};
store_chunk(Name, Args, ReplyTo, CacheRef) when is_atom(Name) ->
    store_chunk(whereis(Name), Args, ReplyTo, CacheRef);
store_chunk(Writer, Args, {Client, Ref}, CacheRef) when is_pid(Writer) ->
    case ar_chunk_cache:add_reference(CacheRef, Writer) of
        {ok, StorageCacheRef} ->
            ar_chunk_cache:mark_cached(StorageCacheRef),
            gen_server:cast(
                Writer,
                {store_chunk, Args, {Client, Ref, StorageCacheRef}}
            );
        {error, expired} ->
            case Client of
                none ->
                    ok;
                _ ->
                    Client ! {chunk_store_result, Ref, {error, cancelled}},
                    ok
            end
    end.

%% @doc Discard pending requests from a stopped client before it releases memory.
cancel_store_requests(Writer, Client) ->
    gen_server:call(Writer, {cancel_store_requests, Client}, infinity).

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
        AbsoluteEndOffset :: non_neg_integer(),
        StoreID :: term(),
        Metadata :: term()) -> ok | {error, term()}.
put_chunk_metadata(AbsoluteEndOffset, StoreID,
                   {_ChunkDataKey, _TXRoot, _DataRoot, _TXPath, _Offset, _ChunkSize} = Metadata) ->
    Key = << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>,
    ar_kv:put({chunks_index, StoreID}, Key, term_to_binary(Metadata)).

get_chunk_metadata(AbsoluteEndOffset, StoreID) ->
    case ar_kv:get({chunks_index, StoreID}, << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>) of
        {ok, Value} ->
            {ChunkDataKey, TXRoot, DataRoot, TXPath, _RelativeOffset, ChunkSize} =
                binary_to_term(Value, [safe]),
            {ok, #chunk_metadata{
                    chunk_data_key = ChunkDataKey,
                    tx_root = TXRoot,
                    tx_path = TXPath,
                    data_root = DataRoot,
                    chunk_size = ChunkSize
                   }};
        not_found ->
            not_found
    end.

delete_chunk_metadata(AbsoluteEndOffset, StoreID) ->
    ar_kv:delete({chunks_index, StoreID}, << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>).

%% @doc Return {ok, Map} | {error, Error} where Map maps AbsoluteEndOffset =>
%% #chunk_metadata{} for all the chunk metadata found within the given range
%% (AbsoluteEndOffset >= Start, AbsoluteEndOffset =< End). Empty map if none found.
get_chunk_metadata_range(Start, End, StoreID) ->
    case ar_kv:get_range({chunks_index, StoreID},
                         << Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>) of
        {ok, Map} ->
            {ok, maps:fold(
                   fun(K, V, Acc) ->
                           << Offset:?OFFSET_KEY_BITSIZE >> = K,
                           {ChunkDataKey, TXRoot, DataRoot, TXPath, _RelativeOffset, ChunkSize} =
                               binary_to_term(V, [safe]),
                           maps:put(Offset, #chunk_metadata{
                                               chunk_data_key = ChunkDataKey,
                                               tx_root = TXRoot,
                                               tx_path = TXPath,
                                               data_root = DataRoot,
                                               chunk_size = ChunkSize
                                              }, Acc)
                   end,
                   #{},
                   Map)};
        Error ->
            Error
    end.
delete_chunk_metadata_range(Start, End, State) ->
    #data_sync_state{ chunks_index = ChunksIndex } = State,
    ar_kv:delete_range(ChunksIndex, << (Start + 1):?OFFSET_KEY_BITSIZE >>,
                       << (End + 1):?OFFSET_KEY_BITSIZE >>).


%% @doc Fetch the chunk corresponding to Offset. When Offset is less than or equal to
%% the strict split data threshold, the chunk returned contains the byte with the given
%% Offset (the indexing is 1-based). Otherwise, the chunk returned ends in the same 256 KiB
%% bucket as Offset counting from the first 256 KiB after the strict split data threshold.
%% The strict split data threshold is weave_size of the block preceding the fork 2.5 block.
%%
%% Options:
%%  _________________________________________________________________________________________
%%  packing             | required; spora_2_5 or unpacked or {spora_2_6, <Mining Address>}
%%
%%                          or {replica_2_9, <Mining Address>}
%%  _________________________________________________________________________________________
%%  pack                | if false and a packed chunk is requested but stored unpacked or
%%                      | an unpacked chunk is requested but stored packed, return
%%                      | {error, chunk_not_found} instead of packing/unpacking;
%%                      | true by default;
%%  _________________________________________________________________________________________
%%  bucket_based_offset | does not play a role for the offsets before
%%                      | strict_data_split_threshold (weave_size of the block preceding
%%                      | the fork 2.5 block); if true, return the chunk which ends in
%%                      | the same 256 KiB bucket starting from
%%                      | strict_data_split_threshold where borders belong to the
%%                      | buckets on the left; true by default.
get_chunk(Offset, #{ packing := Packing } = Options) ->
    Pack = maps:get(pack, Options, true),
    RequestOrigin = maps:get(origin, Options, unknown),
    IsRecorded =
        case {RequestOrigin, Pack} of
            {miner, _} ->
                StorageModules = arweave_storage:covering_stores(Offset, any_packing),
                arweave_storage:is_recorded_any(Offset, {ar_data_sync, byte}, StorageModules);
            {_, false} ->
                arweave_storage:is_recorded(
                    Offset,
                    Packing,
                    {ar_data_sync, byte},
                    any_store
                );
            {_, true} ->
                arweave_storage:is_recorded(
                    Offset,
                    any_packing,
                    {ar_data_sync, byte},
                    any_store
                )
        end,
    SeekOffset =
        case maps:get(bucket_based_offset, Options, true) of
            true ->
                arweave_storage:get_chunk_seek_offset(Offset);
            false ->
                Offset
        end,
    case IsRecorded of
        {{true, StoredPacking}, StoreID} ->
            get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                      RequestOrigin);
        {true, StoreID} ->
            UnpackedReply = arweave_storage:is_recorded(
                Offset,
                unpacked,
                {ar_data_sync, byte},
                any_store
            ),
            log_chunk_error(RequestOrigin, chunk_record_not_associated_with_packing,
                            [{store_id, StoreID}, {seek_offset, SeekOffset},
                             {is_recorded_unpacked, io_lib:format("~p", [UnpackedReply])}]),
            {error, chunk_not_found};
        Reply ->
            UnpackedReply = arweave_storage:is_recorded(
                Offset,
                unpacked,
                {ar_data_sync, byte},
                any_store
            ),
            Modules = arweave_storage:covering_stores(Offset, any_packing),
            ModuleIDs = [(arweave_storage:store_info(Module))#store_info.id || Module <- Modules],
            RootRecords = [{ID, arweave_storage:sync_record_exists(
                any_packing, {ar_data_sync, byte}, ID
            )}
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
    IsRecorded = arweave_storage:is_recorded(
        Offset,
        any_packing,
        {ar_data_sync, byte},
        any_store
    ),
    SeekOffset =
        case maps:get(bucket_based_offset, Options, true) of
            true ->
                arweave_storage:get_chunk_seek_offset(Offset);
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
    SizeLimit =
        case arweave_config:get([features, serve_tx_data_without_limits]) of
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
                    Pack = arweave_config:get([features, pack_served_chunks]),
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

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
    gen_server:call(ar_data_sync_default, {add_block, B, SizeTaggedTXs}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID, Ref, ReplyTo) ->
    TXIndex = {tx_index, ?DEFAULT_MODULE},
    case ar_kv:get(TXIndex, TXID) of
        {ok, Value} ->
            {End, Size} = binary_to_term(Value, [safe]),
            remove_range(End - Size, End, Ref, ReplyTo);
        not_found ->
            ?LOG_WARNING([{event, tx_offset_not_found}, {tx, arweave_util:encode(TXID)}]),
            ok;
        {error, Reason} ->
            ?LOG_ERROR([{event, failed_to_fetch_blacklisted_tx_offset},
                        {tx, arweave_util:encode(TXID)}, {reason, Reason}]),
            ok
    end.

%% @doc Request the removal of the given byte range.
request_data_removal(Start, End, Ref, ReplyTo) ->
    remove_range(Start, End, Ref, ReplyTo).


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
        {ok, << AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >>, Value} ->
            {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize} =
                binary_to_term(Value, [safe]),
            Metadata = #chunk_metadata{
                          chunk_data_key = ChunkDataKey,
                          tx_root = TXRoot,
                          tx_path = TXPath,
                          data_root = DataRoot,
                          chunk_size = ChunkSize
                         },
            Offsets = #chunk_offsets{
                         absolute_offset = AbsoluteEndOffset,
                         relative_offset = RelativeOffset
                        },
            {ok, Metadata, Offsets}
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

%% @doc Read the chunk covering `Offset', returning its bytes and full merkle
%% metadata. Resolves the chunk via the `chunks_index', reads from chunk_data_db
%% falling back to chunk_storage, and tolerates the metadata-before-data flush race.
%%
%% Returns:
%%   `{ok, Metadata, Offsets, Chunk}' — found; `data_path' is set.
%%   `no_chunk'                       — nothing stored at or after `Offset'.
%%   `{error, {data_missing, Metadata, Offsets}}' — index entry exists but the
%%                                      chunk data never landed; the caller may
%%                                      invalidate the record.
%%   `{error, {data_read_failed, Reason, Metadata, Offsets}}' — index entry
%%                                      exists but reading the chunk bytes failed;
%%                                      may be transient, so the caller should not
%%                                      invalidate the record.
%%   `{error, {index_read_failed, Reason}}' — the `chunks_index' query itself
%%                                      failed; no metadata is available.
-spec read_chunk_with_full_metadata(Offset, StoreID) ->
          {ok, #chunk_metadata{}, #chunk_offsets{}, binary()}
              | no_chunk
              | {error, {data_missing, #chunk_metadata{}, #chunk_offsets{}}}
              | {error, {data_read_failed, term(), #chunk_metadata{}, #chunk_offsets{}}}
              | {error, {index_read_failed, term()}}
              when Offset :: non_neg_integer(), StoreID :: term().
read_chunk_with_full_metadata(Offset, StoreID) ->
    case get_chunk_by_byte(Offset, StoreID) of
        {error, invalid_iterator} ->
            no_chunk;
        {error, Reason} ->
            {error, {index_read_failed, Reason}};
        {ok, #chunk_metadata{ chunk_data_key = ChunkDataKey } = Metadata,
         #chunk_offsets{ absolute_offset = AbsoluteEndOffset } = Offsets} ->
            case read_chunk_with_datapath(ChunkDataKey, StoreID) of
                {ok, Chunk, DataPath} ->
                    {ok, Metadata#chunk_metadata{ data_path = DataPath }, Offsets, Chunk};
                {stored_elsewhere, DataPath} ->
                    %% Bytes live in chunk_storage; fetch them by offset,
                    %% retrying the same index-before-data race the
                    %% chunk_data_db read above tolerates (the enciphered
                    %% chunk can land after its index entry and data path).
                    case get_chunk_storage_with_retry(AbsoluteEndOffset - 1, StoreID) of
                        {_EndOffset, Chunk} ->
                            {ok, Metadata#chunk_metadata{ data_path = DataPath }, Offsets,
                             Chunk};
                        not_found ->
                            {error, {data_missing, Metadata, Offsets}}
                    end;
                not_found ->
                    {error, {data_missing, Metadata, Offsets}};
                {error, Reason} ->
                    {error, {data_read_failed, Reason, Metadata, Offsets}}
            end
    end.

-define(READ_CHUNK_RETRY_DELAY_MS, 250).
-define(READ_CHUNK_RETRY_ATTEMPTS, 8).

%% @doc Read a stored chunk by its `ChunkDataKey', returning the bytes and data
%% path, tolerating the metadata-before-data flush race. A key carries no offset,
%% so a chunk whose bytes live in chunk_storage can only be reported as
%% `stored_elsewhere'; read by offset via `read_chunk_with_full_metadata/2' to
%% materialize those bytes.
%%
%% Returns:
%%   `{ok, Chunk, DataPath}'        — found inline in chunk_data_db.
%%   `{stored_elsewhere, DataPath}' — chunk lives in chunk_storage; only the data
%%                                    path is available here.
%%   `not_found'                    — no data for this key.
%%   `{error, Reason}'              — storage error.
-spec read_chunk_with_datapath(ChunkDataKey, StoreID) ->
          {ok, binary(), binary()}
              | {stored_elsewhere, binary()}
              | not_found
              | {error, term()}
              when ChunkDataKey :: binary(), StoreID :: term().
read_chunk_with_datapath(ChunkDataKey, StoreID) ->
    do_read_chunk_data(ChunkDataKey, StoreID, ?READ_CHUNK_RETRY_ATTEMPTS).

%% @doc Materialize a chunk from its offset and `ChunkDataKey' without retrying.
%% Serves the `get_chunk/2' path, where chunks are already sync-recorded so a
%% miss is a genuine absence and retrying would only add client-facing latency.
read_chunk(Offset, ChunkDataKey, StoreID) ->
    case do_read_chunk_data(ChunkDataKey, StoreID, 0) of
        {ok, Chunk, DataPath} ->
            {ok, {Chunk, DataPath}};
        {stored_elsewhere, DataPath} ->
            case arweave_storage:get_chunk(Offset - 1, StoreID) of
                not_found ->
                    not_found;
                {_EndOffset, Chunk} ->
                    {ok, {Chunk, DataPath}}
            end;
        Other ->
            Other
    end.

%% @doc Read only the data path stored under `ChunkDataKey', without retrying.
read_data_path(ChunkDataKey, StoreID) ->
    case do_read_chunk_data(ChunkDataKey, StoreID, 0) of
        {ok, _Chunk, DataPath} ->
            {ok, DataPath};
        {stored_elsewhere, DataPath} ->
            {ok, DataPath};
        Other ->
            Other
    end.

%% @doc Read the value stored under `ChunkDataKey' in chunk_data_db, retrying
%% up to Attempts times (0 reads once) in the rare race where the
%% `chunks_index' entry is present before the chunk data lands. Return
%% `{ok, Chunk, DataPath}' when the chunk is stored inline in chunk_data_db,
%% `{stored_elsewhere, DataPath}' when only the data path is stored there (the
%% chunk bytes live in chunk_storage), `not_found' or `{error, Reason}'.
do_read_chunk_data(ChunkDataKey, StoreID, Attempts) ->
    case get_chunk_data_with_retry(ChunkDataKey, StoreID, Attempts) of
        not_found ->
            not_found;
        {ok, Value} ->
            case binary_to_term(Value, [safe]) of
                {Chunk, DataPath} ->
                    {ok, Chunk, DataPath};
                DataPath ->
                    {stored_elsewhere, DataPath}
            end;
        Error ->
            Error
    end.

get_chunk_data_with_retry(ChunkDataKey, StoreID, 0) ->
    get_chunk_data(ChunkDataKey, StoreID);
get_chunk_data_with_retry(ChunkDataKey, StoreID, Attempts) ->
    case get_chunk_data(ChunkDataKey, StoreID) of
        not_found ->
            timer:sleep(?READ_CHUNK_RETRY_DELAY_MS),
            get_chunk_data_with_retry(ChunkDataKey, StoreID, Attempts - 1);
        Other ->
            Other
    end.

%% @doc Retry `arweave_storage:get_chunk/2' in the same `chunks_index'-before-data
%% race as `get_chunk_data_with_retry/2', for chunks whose bytes live in
%% chunk_storage (e.g. replica_2_9): the index entry and `stored_elsewhere'
%% data path can land before the enciphered chunk is written, so a single read
%% would spuriously report `not_found' and the caller would invalidate a record
%% that is merely still landing.
get_chunk_storage_with_retry(Offset, StoreID) ->
    get_chunk_storage_with_retry(Offset, StoreID, ?READ_CHUNK_RETRY_ATTEMPTS).

get_chunk_storage_with_retry(Offset, StoreID, 0) ->
    arweave_storage:get_chunk(Offset, StoreID);
get_chunk_storage_with_retry(Offset, StoreID, Attempts) ->
    case arweave_storage:get_chunk(Offset, StoreID) of
        not_found ->
            timer:sleep(?READ_CHUNK_RETRY_DELAY_MS),
            get_chunk_storage_with_retry(Offset, StoreID, Attempts - 1);
        Other ->
            Other
    end.

write_chunk(Offset, ChunkMetadata, Chunk, Packing, StoreID) ->
    #chunk_metadata{
       chunk_data_key = ChunkDataKey,
       chunk_size = ChunkSize,
       data_path = DataPath
      } = ChunkMetadata,
    write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, StoreID).

%% @doc Release a finished request's cache reference and reply to its client.
finish_store_request({PID, Ref, CacheRef}, Result) ->
    ar_chunk_cache:release(CacheRef),
    case PID of
        none -> ok;
        _ -> PID ! {chunk_store_result, Ref, Result}
    end,
    ok.

migration_db(StoreID) ->
    {migrations_index, StoreID}.

%% @doc Update the weave-size snapshot in #data_sync_state{} and forward it to
%% the matching sync sweeper. The sweeper caches the current disk-pool threshold
%% alongside the weave size when it handles the cast.
set_weave_size(WeaveSize, #data_sync_state{ store_id = StoreID } = State) ->
    arweave_sync:set_weave_size(StoreID, WeaveSize),
    State#data_sync_state{ weave_size = WeaveSize }.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({?DEFAULT_MODULE = StoreID, _}) ->
    ok = arweave_sync:register_store(StoreID),
    %% Trap exit to avoid corrupting any open files on quit..
    process_flag(trap_exit, true),
    [ok, ok, ok] = ar_events:subscribe([node_state, disksup, chunk_copy]),
    State = init_kv(#data_sync_state{}, StoreID),

    StateMap = read_data_sync_state(),
    CurrentBI = maps:get(block_index, StateMap),
    case StateMap of
        #{ disk_pool_threshold := DPT } ->
            ar_disk_pool:set_threshold(DPT);
        _ ->
            ar_disk_pool:update_threshold(CurrentBI)
    end,
    ar_disk_pool:populate_data_roots(
      maps:get(disk_pool_data_roots, StateMap), StoreID),
    WeaveSize = maps:get(weave_size, StateMap),
    %% Initialize the threshold before asking the sweeper to cache both bounds.
    arweave_sync:set_weave_size(StoreID, WeaveSize),
    %% Called for its side effect: publish the initial device-lock sync metric.
    init_sync_status(StoreID),
    State2 = State#data_sync_state{
               block_index = CurrentBI,
               weave_size = WeaveSize,
               store_id = StoreID,
               footprint_limit = ar_footprint_limit:get(StoreID)
              },
    ?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID},
               {range_start, State2#data_sync_state.range_start},
               {range_end, State2#data_sync_state.range_end}]),
    gen_server:cast(self(), store_sync_state),
    gen_server:cast(self(), process_store_chunk_queue),
    {ok, State2};
init({StoreID, RepackInPlacePacking}) ->
    ok = arweave_sync:register_store(StoreID),
    ?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
    %% Trap exit to avoid corrupting any open files on quit..
    process_flag(trap_exit, true),
    [ok, ok, ok] = ar_events:subscribe([node_state, disksup, chunk_copy]),
    {RangeStart2, RangeEnd2} =
        case arweave_storage:store_info(StoreID) of
            #store_info{padded_range = Range} -> Range;
            not_found -> {-1, -1}
        end,
    State0 = #data_sync_state{
                store_id = StoreID,
                footprint_limit = ar_footprint_limit:get(StoreID),
                range_start = RangeStart2,
                range_end = RangeEnd2,
                %% weave_size will be set on join and forwarded to the sync sweeper.
                weave_size = 0
               },
    State1 = init_kv(State0, StoreID),

    case RepackInPlacePacking of
                none ->
                    gen_server:cast(self(), process_store_chunk_queue),
                    %% Called for its side effect: publish the initial device-lock
                    %% sync metric.
                    init_sync_status(StoreID),
                    ar_chunk_copy:start_copy(StoreID),
                    ok = run_footprint_record_initialization(StoreID);
                 _ ->
                    ar_device_lock:set_device_lock_metric(StoreID, sync, off)
             end,
    ?LOG_INFO([{event, ar_data_sync_initialized}, {store_id, StoreID},
               {repack_in_place_packing, case RepackInPlacePacking of
                                             none -> none;
                                             _ -> ar_serialize:encode_packing(RepackInPlacePacking, false)
                                         end}]),
    {ok, State1}.

handle_cast(process_store_chunk_queue, State) ->
    arweave_util:cast_after(200, self(), process_store_chunk_queue),
    {noreply, process_store_chunk_queue(State)};


handle_cast({join, RecentBI}, State) ->
    #data_sync_state{ block_index = CurrentBI, store_id = StoreID } = State,
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
            ok = remove_orphaned_data(State, Offset, PreviousWeaveSize),
            ok = cut_orphaned_storage_modules(Offset, PreviousWeaveSize)
    end,
    BI = ar_block_index:get_list_by_hash(element(1, lists:last(RecentBI))),
    ar_data_roots:repair_data_root_offset_index(BI, StoreID),
    State2 = store_sync_state(
               set_weave_size(WeaveSize,
                              State#data_sync_state{ block_index = RecentBI })),
    {noreply, State2};

handle_cast({cut, Start}, #data_sync_state{ store_id = StoreID,
                                            range_end = End } = State) ->
    case arweave_storage:get_next_interval(
        synced,
        Start,
        End,
        any_packing,
        {ar_data_sync, byte},
        StoreID
    ) of
        not_found ->
            ok;
        _Interval ->
            case arweave_config:get([features, remove_orphaned_storage_module_data]) of
                false ->
                    ar:console("The storage module ~s contains some orphaned data above the "
                               "weave offset ~B. Make sure you are joining the network through "
                               "trusted in-sync peers and restart with "
                               "`features.remove_orphaned_storage_module_data = true` "
                               "(or the legacy `enable remove_orphaned_storage_module_data`).~n",
                               [StoreID, Start]),
                    timer:sleep(2000),
                    init:stop(1);
                true ->
                    ok = delete_chunk_metadata_range(Start, End, State),
                    ok = arweave_storage:cut_sync_record(
                        Start, {ar_chunk_storage, byte}, StoreID
                    ),
                    ok = arweave_storage:cut_sync_record(Start, {ar_data_sync, byte}, StoreID)
            end
    end,
    {noreply, State};

handle_cast({add_tip_block, BlockTXPairs, BI}, State) ->
    #data_sync_state{ store_id = StoreID, weave_size = CurrentWeaveSize,
                      block_index = CurrentBI } = State,
    {BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
    ok = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
    ok = cut_orphaned_storage_modules(BlockStartOffset, CurrentWeaveSize),
    {WeaveSize, AddedDataRootIDs} = lists:foldl(
                                      fun ({_BH, []}, Acc) ->
                                              Acc;
                                          ({_BH, SizeTaggedTXs}, {StartOffset, DataRootIDsAcc}) ->
                                              {ok, DataRootIDs} =
                                                  ar_data_roots:add_block_data_roots(SizeTaggedTXs, StartOffset, StoreID),
                                              ok = ar_data_roots:update_tx_index(SizeTaggedTXs, StartOffset, StoreID),
                                              {StartOffset + element(2, lists:last(SizeTaggedTXs)),
                                               sets:union(DataRootIDsAcc, DataRootIDs)}
                                      end,
                                      {BlockStartOffset, sets:new()},
                                      Blocks
                                     ),
    ar_disk_pool:add_block_data_roots(AddedDataRootIDs),
    ar_disk_pool:update_threshold(BI),
    State2 = store_sync_state(
               set_weave_size(WeaveSize,
                              State#data_sync_state{ block_index = BI })),
    {noreply, State2};

handle_cast({invalidate_bad_data_record, Args}, State) ->
    do_invalidate_bad_data_record(Args),
    {noreply, State};

handle_cast({retry_store_chunk, Ref}, State) ->
    #data_sync_state{packing_map = PackingMap} = State,
    case maps:take(Ref, PackingMap) of
        {{store_retry, Args, ReplyTo}, PackingMap2} ->
            handle_cast({store_chunk, Args, ReplyTo},
                State#data_sync_state{packing_map = PackingMap2});
        error -> {noreply, State}
    end;

handle_cast({store_chunk, Args, ReplyTo}, State) ->
    Outcome = process_store_request(Args, ReplyTo, State),
    {noreply, continue_store_request(ReplyTo, Outcome)};

handle_cast({remove_range, End, Cursor, Ref, PID}, State) when Cursor > End ->
    PID ! {removed_range, Ref},
    {noreply, State};
handle_cast({remove_range, End, Cursor, Ref, PID}, State) ->
    #data_sync_state{ store_id = StoreID } = State,
    case get_chunk_by_byte(Cursor, StoreID) of
        {ok, _Metadata, #chunk_offsets{ absolute_offset = AbsoluteEndOffset }}
          when AbsoluteEndOffset > End ->
            PID ! {removed_range, Ref},
            {noreply, State};
        {ok, #chunk_metadata{ chunk_size = ChunkSize },
         #chunk_offsets{ absolute_offset = AbsoluteEndOffset }} ->
            PaddedStartOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize),
            PaddedOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset),
            %% 1) store updated sync record
            %% 2) remove chunk
            %% 3) update chunks_index
            %%
            %% The order is important - in case the VM crashes,
            %% we will not report false positives to peers,
            %% and the chunk can still be removed upon retry.
            RemoveFromFootprint = arweave_storage:delete_footprint(PaddedOffset, StoreID),
            RemoveFromSyncRecord =
                case RemoveFromFootprint of
                    ok ->
                        arweave_storage:delete_sync_record(PaddedOffset,
                                              PaddedStartOffset, {ar_data_sync, byte}, StoreID);
                    Error ->
                        Error
                end,
            RemoveFromChunkStorage =
                case RemoveFromSyncRecord of
                    ok ->
                        arweave_storage:delete_chunk(PaddedOffset, StoreID);
                    Error2 ->
                        Error2
                end,
            RemoveFromChunksIndex =
                case RemoveFromChunkStorage of
                    ok ->
                        delete_chunk_metadata(AbsoluteEndOffset, StoreID);
                    Error3 ->
                        Error3
                end,
            case RemoveFromChunksIndex of
                ok ->
                    NextCursor = AbsoluteEndOffset + 1,
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

handle_cast({expire, repack, {EndOffset, Packing, Ref}}, State) ->
    #data_sync_state{packing_map = PackingMap} = State,
    Key = {EndOffset, Packing},
    case maps:get(Key, PackingMap, not_found) of
        {pack_chunk, {Ref, {_, DataPath, Offset, DataRoot, _, _, _}}, ReplyTo} ->
            finish_store_request(ReplyTo, {error, packing_timeout}),
            DataPathHash = crypto:hash(sha256, DataPath),
            ?LOG_DEBUG([
                {event, expired_repack_chunk_request},
                {data_path_hash, arweave_util:encode(DataPathHash)},
                {data_root, arweave_util:encode(DataRoot)},
                {relative_offset, Offset}
            ]),
            State2 = State#data_sync_state{
                packing_map = maps:remove(Key, PackingMap)
            },
            {noreply, State2};
        _ ->
            {noreply, State}
    end;

handle_cast(store_sync_state, State) ->
    store_sync_state(State),
    arweave_util:cast_after(?STORE_STATE_FREQUENCY_MS, self(), store_sync_state),
    {noreply, State};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
    {noreply, State}.

handle_call({cancel_store_requests, Client}, _From, State) ->
    #data_sync_state{packing_map = PackingMap, store_chunk_queue = Queue} =
        State,
    PackingMap2 = cancel_packing_requests(Client, PackingMap),
    Queue2 = cancel_queued_chunks(Client, Queue),
    {reply, ok, State#data_sync_state{
        packing_map = PackingMap2,
        store_chunk_queue = Queue2
    }};
handle_call({add_block, B, SizeTaggedTXs}, _From, State) ->
    #data_sync_state{ store_id = StoreID } = State,
    {reply, ar_data_roots:add_block(B, SizeTaggedTXs, StoreID), State};

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
    {reply, ok, State}.

handle_info({chunk, Result, CacheRef}, State) ->
    try
        handle_info({chunk, Result}, State)
    after
        ar_chunk_cache:release(CacheRef)
    end;

handle_info(
    {chunk, {repack_error, {EndOffset, Packing, Ref}, Reason}},
    #data_sync_state{packing_map = Map} = State
) ->
    Key = {EndOffset, Packing},
    case maps:get(Key, Map, undefined) of
        {pack_chunk, {Ref, _}, ReplyTo} ->
            finish_store_request(ReplyTo, {error, Reason}),
            {noreply, State#data_sync_state{
                packing_map = maps:remove(Key, Map)
            }};
        _ ->
            {noreply, State}
    end;

handle_info({event, node_state, {initialized, B}}, State) ->
    {noreply, set_weave_size(B#block.weave_size, State)};

handle_info({event, node_state, {new_tip, B, _PrevB}}, State) ->
    {noreply, set_weave_size(B#block.weave_size, State)};

handle_info({event, node_state, _}, State) ->
    {noreply, State};

%% Local-copy reads have finished; their handoffs retain sync reservations
%% until storage finishes, even as network discovery starts.
handle_info({event, chunk_copy, {complete, StoreID}},
            #data_sync_state{ store_id = StoreID } = State) ->
    %% Start the work-discovery loop; it discovers work and pushes tasks
    %% through the sync pipeline.
    arweave_sync:start_store(StoreID),
    {noreply, State};
handle_info({event, chunk_copy, _}, State) ->
    {noreply, State};

handle_info(
    {chunk, {packed, {EndOffset, RequestedPacking, Ref}, ChunkArgs}}, State
) ->
    #data_sync_state{packing_map = PackingMap} = State,
    Key = {EndOffset, RequestedPacking},
    Packing = element(1, ChunkArgs),
    case maps:get(Key, PackingMap, not_found) of
        {pack_chunk, {Ref, Args}, ReplyTo} when element(1, Args) == Packing ->
            State2 = State#data_sync_state{
                packing_map = maps:remove(Key, PackingMap)
            },
            Outcome = enqueue_chunk(ChunkArgs, Args, ReplyTo, State2),
            {noreply, continue_store_request(ReplyTo, Outcome)};
        _ ->
            {noreply, State}
    end;

handle_info({chunk, _}, State) ->
    {noreply, State};

handle_info({event, disksup, {remaining_disk_space, StoreID, false, Percentage, _Bytes}},
            #data_sync_state{ store_id = StoreID } = State) ->
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
            #data_sync_state{ store_id = StoreID } = State) ->
    MaxDiskPoolBufferMb = arweave_config:get([disk_pool, max_buffer_size]),
    DiskCacheSizeMb = arweave_config:get([gossip, header, cache_size]),
    %% Default values:
    %% max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB = 100_000
    %% disk_cache_size = ?DISK_CACHE_SIZE = 5_120
    %% DiskPoolSize = ~100GB
    %% DisckCacheSize = ~5GB
    %% BufferSize = ~10GB
    DiskPoolSize = MaxDiskPoolBufferMb * ?MiB,
    DiskCacheSize = DiskCacheSizeMb * ?MiB,
    BufferSize = 10_000_000_000,
    RequiredDiskSpace = DiskPoolSize + DiskCacheSize,
    StopThreshold = RequiredDiskSpace + (BufferSize div 2),
    ResumeThreshold = RequiredDiskSpace + BufferSize,
    CurrentStatus = is_disk_space_sufficient(StoreID),
    case Bytes < StopThreshold of
        true ->
            case CurrentStatus of
                false ->
                    ok;
                _ ->
                    log_insufficient_disk_space(StoreID)
            end,
            ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, false});
        false ->
            case Bytes > ResumeThreshold orelse CurrentStatus =/= false of
                true ->
                    case CurrentStatus of
                        false ->
                            log_sufficient_disk_space(StoreID);
                        _ ->
                            ok
                    end,
                    ets:insert(ar_data_sync_state,
                               {{is_disk_space_sufficient, StoreID}, true});
                false ->
                    ok
            end
    end,
    {noreply, State};

handle_info({event, disksup, _}, State) ->
    {noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
    {noreply, State};

handle_info(Message,  #data_sync_state{ store_id = StoreID } = State) ->
    ?LOG_WARNING([{event, unhandled_info}, {store_id, StoreID}, {message, Message}]),
    {noreply, State}.

terminate(Reason, #data_sync_state{ store_id = StoreID } = State) ->
    store_sync_state(State),
    ?LOG_INFO([{event, terminate}, {store_id, StoreID},
               {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

init_sync_status(StoreID) ->
    %% The sweeper checks the live download rate before acquiring its lock.
    ar_device_lock:set_device_lock_metric(StoreID, sync, paused),
    paused.
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

get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID, RequestOrigin) ->
    case do_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                      RequestOrigin) of
        {error, chunk_id_mismatch, MismatchInfo} ->
            %% A chunk-id mismatch is only transient — and so worth retrying — in a
            %% repack-in-place store, where a slot can momentarily hold entropy
            %% (which unpacks to all zeroes) or partially written bytes before the
            %% final chunk lands. Anywhere else a mismatch is genuine corruption, so
            %% invalidate immediately and keep the retry (and its delay) off the
            %% normal read path. Checking only on a mismatch keeps the happy path free.
            case (arweave_storage:store_info(StoreID))#store_info.repack_in_place of
                true ->
                    retry_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking,
                                    StoreID, RequestOrigin, ?READ_CHUNK_RETRY_ATTEMPTS);
                false ->
                    invalidate_after_chunk_id_mismatch(RequestOrigin, MismatchInfo)
            end;
        Result ->
            Result
    end.

%% @doc Re-read after a chunk-id mismatch in a repack-in-place store: a slot can
%% momentarily hold entropy or partially written bytes before the final chunk
%% lands, so wait and retry to let the write catch up. A mismatch that survives
%% the retries is genuine corruption, so invalidate the record.
retry_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                RequestOrigin, 0) ->
    case do_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                      RequestOrigin) of
        {error, chunk_id_mismatch, MismatchInfo} ->
            invalidate_after_chunk_id_mismatch(RequestOrigin, MismatchInfo);
        Result ->
            Result
    end;
retry_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                RequestOrigin, Attempts) ->
    timer:sleep(?READ_CHUNK_RETRY_DELAY_MS),
    case do_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                      RequestOrigin) of
        {error, chunk_id_mismatch, _} ->
            retry_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID,
                            RequestOrigin, Attempts - 1);
        Result ->
            Result
    end.

invalidate_after_chunk_id_mismatch(RequestOrigin, {LogData, InvalidateArgs}) ->
    log_chunk_error(RequestOrigin, get_chunk_invalid_id, LogData),
    invalidate_bad_data_record(InvalidateArgs),
    {error, chunk_not_found}.

do_get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID, RequestOrigin) ->
    case read_chunk_with_metadata(Offset, SeekOffset, StoredPacking, StoreID, true,
                                  RequestOrigin) of
        {error, Reason} ->
            {error, Reason};
        {ok, {Chunk, DataPath}, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath, ChunkDataKey} ->
            ChunkID =
                case validate_fetched_chunk({AbsoluteEndOffset, DataPath, TXPath, TXRoot,
                                             ChunkSize, StoreID, ChunkDataKey, RequestOrigin}) of
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
                          Packing, StoredPacking, AbsoluteEndOffset, TXRoot, Chunk, ChunkSize)
                end,
            case {PackResult, ChunkID} of
                {{error, Reason}, _} ->
                    log_chunk_error(RequestOrigin, failed_to_repack_chunk,
                                    [{packing, ar_serialize:encode_packing(Packing, true)},
                                     {stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
                                     {absolute_end_offset, AbsoluteEndOffset},
                                     {store_id, StoreID},
                                     {error, io_lib:format("~p", [Reason])}]),
                    {error, Reason};
                {{ok, PackedChunk, none}, _} ->
                    %% PackedChunk is the requested format.
                    Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
                               data_path => DataPath, tx_path => TXPath,
                               absolute_end_offset => AbsoluteEndOffset,
                               chunk_size => ChunkSize },
                    {ok, Proof};
                {{ok, PackedChunk, MaybeUnpackedChunk}, none} ->
                    %% PackedChunk is the requested format, but the ChunkID could
                    %% not be determined
                    Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
                               data_path => DataPath, tx_path => TXPath,
                               absolute_end_offset => AbsoluteEndOffset,
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
                               absolute_end_offset => AbsoluteEndOffset,
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
                                    %% The unpacked bytes don't hash to the expected
                                    %% chunk id. During repack-in-place the slot can
                                    %% still hold entropy (which unpacks to all zeroes)
                                    %% or partially written bytes, so return a retryable
                                    %% marker carrying what the caller needs to log and
                                    %% invalidate if the mismatch turns out to be
                                    %% permanent.
                                    LogData =
                                        [{chunk_size, ChunkSize},
                                         {actual_chunk_size, byte_size(MaybeUnpackedChunk)},
                                         {requested_packing,
                                          ar_serialize:encode_packing(Packing, true)},
                                         {stored_packing,
                                          ar_serialize:encode_packing(StoredPacking, true)},
                                         {absolute_end_offset, AbsoluteEndOffset},
                                         {offset, Offset},
                                         {seek_offset, SeekOffset},
                                         {store_id, StoreID},
                                         {expected_chunk_id, arweave_util:encode(ChunkID)},
                                         {chunk_id, arweave_util:encode(ComputedChunkID)},
                                         {actual_chunk, binary:part(MaybeUnpackedChunk, 0,
                                                                    min(32, byte_size(MaybeUnpackedChunk)))}],
                                    InvalidateArgs = {AbsoluteEndOffset, ChunkSize,
                                                      StoreID, ChunkDataKey,
                                                      get_chunk_invalid_id},
                                    {error, chunk_id_mismatch, {LogData, InvalidateArgs}}
                            end
                    end
            end
    end.

get_chunk_proof(Offset, SeekOffset, StoredPacking, StoreID, RequestOrigin) ->
    case read_chunk_with_metadata(
           Offset, SeekOffset, StoredPacking, StoreID, false, RequestOrigin) of
        {error, Reason} ->
            {error, Reason};
        {ok, DataPath, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath, ChunkDataKey} ->
            CheckProof =
                case validate_fetched_chunk({AbsoluteEndOffset, DataPath, TXPath, TXRoot,
                                             ChunkSize, StoreID, ChunkDataKey, false}) of
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
%% {ok, {Chunk, DataPath}, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath}
%%
%% Otherwise, the format is
%% {ok, DataPath, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath}
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
            Modules = arweave_storage:covering_stores(SeekOffset, any_packing),
            ModuleIDs = [(arweave_storage:store_info(Module))#store_info.id || Module <- Modules],
            log_chunk_error(RequestOrigin, failed_to_fetch_chunk_metadata,
                            [{seek_offset, SeekOffset},
                             {store_id, StoreID},
                             {stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
                             {modules_covering_seek_offset, ModuleIDs},
                             {error, io_lib:format("~p", [Err])}]),
            {error, chunk_not_found};
        {ok, #chunk_metadata{ chunk_size = ChunkSize },
         #chunk_offsets{ absolute_offset = AbsoluteEndOffset }}
          when AbsoluteEndOffset - SeekOffset >= ChunkSize ->
            log_chunk_error(RequestOrigin, chunk_offset_mismatch,
                            [{absolute_offset, AbsoluteEndOffset},
                             {seek_offset, SeekOffset},
                             {store_id, StoreID},
                             {stored_packing, ar_serialize:encode_packing(StoredPacking, true)}]),
            {error, chunk_not_found};
        {ok, #chunk_metadata{ chunk_data_key = ChunkDataKey, tx_root = TXRoot,
                              tx_path = TXPath, chunk_size = ChunkSize },
         #chunk_offsets{ absolute_offset = AbsoluteEndOffset }} ->
            ReadResult =
                case ReadChunk of
                    true ->
                        read_chunk(AbsoluteEndOffset, ChunkDataKey, StoreID);
                    _ ->
                        read_data_path(ChunkDataKey, StoreID)
                end,
            case ReadResult of
                not_found ->
                    Modules = arweave_storage:covering_stores(SeekOffset, any_packing),
                    ModuleIDs = [(arweave_storage:store_info(Module))#store_info.id || Module <- Modules],
                    log_chunk_error(RequestOrigin, failed_to_read_chunk_data_path,
                                    [{seek_offset, SeekOffset},
                                     {absolute_offset, AbsoluteEndOffset},
                                     {store_id, StoreID},
                                     {stored_packing,
                                      ar_serialize:encode_packing(StoredPacking, true)},
                                     {modules_covering_seek_offset, ModuleIDs},
                                     {chunk_data_key, arweave_util:encode(ChunkDataKey)},
                                     {read_chunk, ReadChunk}]),
                    invalidate_bad_data_record({AbsoluteEndOffset, ChunkSize, StoreID,
                                                ChunkDataKey, failed_to_read_chunk_data_path}),
                    {error, chunk_not_found};
                {error, Error} ->
                    log_chunk_error(RequestOrigin, failed_to_read_chunk,
                                    [{reason, io_lib:format("~p", [Error])},
                                     {chunk_data_key, arweave_util:encode(ChunkDataKey)},
                                     {absolute_end_offset, Offset}]),
                    {error, failed_to_read_chunk};
                {ok, {Chunk, DataPath}} ->
                    case arweave_storage:is_recorded(Offset, StoredPacking, {ar_data_sync, byte},
                                                    StoreID) of
                        false ->
                            Modules = arweave_storage:covering_stores(SeekOffset, any_packing),
                            ModuleIDs = [(arweave_storage:store_info(Module))#store_info.id || Module <- Modules],
                            RootRecords = [{ID, arweave_storage:sync_record_exists(
                                any_packing, {ar_data_sync, byte}, ID
                            )}
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
                            {ok, {Chunk, DataPath}, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath,
                             ChunkDataKey}
                    end;
                {ok, DataPath} ->
                    {ok, DataPath, AbsoluteEndOffset, TXRoot, ChunkSize, TXPath, ChunkDataKey}
            end
    end.

invalidate_bad_data_record({_AbsoluteEndOffset, _ChunkSize, StoreID, _ObservedChunkDataKey,
                            _Type} = Args) ->
    gen_server:cast(?MODULE:name(StoreID), {invalidate_bad_data_record, Args}).

do_invalidate_bad_data_record({AbsoluteEndOffset, ChunkSize, StoreID, ObservedChunkDataKey,
                               Type}) ->
    T = ar_disk_pool:get_threshold(),
    case AbsoluteEndOffset > T of
        true ->
            %% Do not invalidate fresh records - a reorg may be in progress.
            ok;
        false ->
            case is_stale_invalidation(AbsoluteEndOffset, StoreID, ObservedChunkDataKey) of
                true ->
                    ?LOG_INFO([{event, skipping_stale_chunk_invalidation}, {type, Type},
                               {absolute_end_offset, AbsoluteEndOffset}, {store_id, StoreID}]),
                    ok;
                false ->
                    invalidate_bad_data_record2({AbsoluteEndOffset, ChunkSize, StoreID, Type})
            end
    end.

is_stale_invalidation(_AbsoluteEndOffset, _StoreID, undefined) ->
    false;
is_stale_invalidation(AbsoluteEndOffset, StoreID, ObservedChunkDataKey) ->
    case get_chunk_metadata(AbsoluteEndOffset, StoreID) of
        {ok, #chunk_metadata{ chunk_data_key = CurrentChunkDataKey }} ->
            CurrentChunkDataKey =/= ObservedChunkDataKey;
        not_found ->
            false
    end.

invalidate_bad_data_record2({AbsoluteEndOffset, ChunkSize, StoreID, Type}) ->
    PaddedEndOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset),
    StartOffset = AbsoluteEndOffset - ChunkSize,
    ?LOG_WARNING([{event, invalidating_bad_data_record}, {type, Type},
                  {range_start, StartOffset}, {range_end, PaddedEndOffset},
                  {store_id, StoreID}]),
    case remove_invalid_sync_records(PaddedEndOffset, StartOffset, StoreID) of
        ok ->
            arweave_storage:add_sync_record(
                PaddedEndOffset,
                StartOffset,
                any_packing,
                {invalid_chunks, byte},
                StoreID
            ),
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
    Remove1 = arweave_storage:delete_footprint(PaddedEndOffset, StoreID),
    Remove2 =
        case Remove1 of
            ok ->
                arweave_storage:delete_sync_record(PaddedEndOffset, StartOffset, {ar_data_sync, byte}, StoreID);
            Error ->
                Error
        end,
    IsSmallChunkBeforeThreshold = PaddedEndOffset - StartOffset < ?DATA_CHUNK_SIZE,
    Remove3 =
        case {Remove2, IsSmallChunkBeforeThreshold} of
            {ok, false} ->
                arweave_storage:delete_sync_record(PaddedEndOffset, StartOffset,
                                      {ar_chunk_storage, byte}, StoreID);
            _ ->
                Remove2
        end,
    Remove4 =
        case {Remove3, IsSmallChunkBeforeThreshold} of
            {ok, false} ->
                arweave_storage:delete_entropy_record(PaddedEndOffset, StartOffset, StoreID);
            _ ->
                Remove3
        end,
    case {Remove4, IsSmallChunkBeforeThreshold} of
        {ok, false} ->
            arweave_storage:delete_sync_record(PaddedEndOffset, StartOffset,
                                  {ar_chunk_storage_replica_2_9_1_unpacked, byte}, StoreID);
        _ ->
            Remove4
    end.

delete_invalid_metadata(AbsoluteEndOffset, StoreID) ->
    case get_chunk_metadata(AbsoluteEndOffset, StoreID) of
        not_found ->
            ok;
        {ok, #chunk_metadata{ chunk_data_key = ChunkDataKey }} ->
            delete_chunk_data(ChunkDataKey, StoreID),
            delete_chunk_metadata(AbsoluteEndOffset, StoreID)
    end.

validate_fetched_chunk(Args) ->
    {Offset, DataPath, TXPath, TXRoot, ChunkSize, StoreID, ObservedChunkDataKey,
     RequestOrigin} = Args,
    T = ar_disk_pool:get_threshold(),
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
                                                        ObservedChunkDataKey,
                                                        failed_to_validate_chunk_proofs}),
                            false
                    end;
                {_BlockStart, _BlockEnd, TXRoot2} ->
                    log_chunk_error(RequestOrigin, stored_chunk_invalid_tx_root,
                                    [{end_offset, Offset}, {tx_root, arweave_util:encode(TXRoot2)},
                                     {stored_tx_root, arweave_util:encode(TXRoot)}, {store_id, StoreID}]),
                    invalidate_bad_data_record({Offset, ChunkSize, StoreID,
                                                ObservedChunkDataKey,
                                                stored_chunk_invalid_tx_root}),
                    false
            end
    end.


get_tx_offset(TXIndex, TXID) ->
    case ar_kv:get(TXIndex, TXID) of
        {ok, Value} ->
            {ok, binary_to_term(Value, [safe])};
        not_found ->
            {error, not_found};
        {error, Reason} ->
            ?LOG_ERROR([{event, failed_to_read_tx_offset},
                        {reason, io_lib:format("~p", [Reason])},
                        {tx, arweave_util:encode(TXID)}]),
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
    StorageModules = arweave_storage:intersecting_stores(Start, End, any_packing),
    StoreIDs = [?DEFAULT_MODULE | [(arweave_storage:store_info(M))#store_info.id || M <- StorageModules]],
    RefL = [make_ref() || _ <- StoreIDs],
    PID = spawn(fun() -> ReplyFun(ReplyFun, sets:from_list(RefL)) end),
    lists:foreach(
      fun({StoreID, R}) ->
              gen_server:cast(name(StoreID), {remove_range, End, Start + 1, R, PID})
      end,
      lists:zip(StoreIDs, RefL)
     ).

init_kv(State, StoreID) ->
    ok = open_store_dbs(StoreID),
    ar_disk_pool:move_index(StoreID),
    State#data_sync_state{
      chunks_index = {chunks_index, StoreID},
      chunk_data_db = {chunk_data_db, StoreID},
      tx_index = {tx_index, StoreID},
      tx_offset_index = {tx_offset_index, StoreID}
     }.

open_store_dbs(StoreID) ->
    BasicOpts = [{max_open_files, max_open_files()}],
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
                               ar_data_roots:column_family(BasicOpts ++ BloomFilterOpts),
                               ar_data_roots:keys_column_family(BasicOpts),
                               {"tx_index", BasicOpts ++ BloomFilterOpts},
                               {"tx_offset_index", BasicOpts},
                               ar_disk_pool:column_family(BasicOpts ++ BloomFilterOpts),
                               {"migrations_index", BasicOpts}
                              ],
    #store_info{path = Path} = arweave_storage:store_info(StoreID),
    Dir = filename:join(Path, ?ROCKS_DB_DIR),
    ok = ar_kv:open(#{
                      path => filename:join(Dir, "ar_data_sync_db"),
                      cf_descriptors => ColumnFamilyDescriptors,
                      cf_names => [{ar_data_sync, StoreID}, {chunks_index, StoreID},
                                   ar_data_roots:legacy_db(StoreID),
                                   ar_data_roots:keys_db(StoreID),
                                   {tx_index, StoreID}, {tx_offset_index, StoreID},
                                   ar_disk_pool:old_index_db(StoreID), migration_db(StoreID)]}),
    ok = ar_kv:open(#{
                      path => filename:join(Dir, "ar_data_sync_chunk_db"),
                      name => {chunk_data_db, StoreID},
                      options => ar_kv:db_options(max_open_files())}),
    ok = ar_disk_pool:open_index_db(Dir, StoreID, BloomFilterOpts),
    ok = ar_data_roots:open_index_db(Dir, StoreID, BloomFilterOpts).

%% Test builds force this to 100 — see ar_kv:db_options/1.
-ifdef(AR_TEST).
max_open_files() -> 100.
-else.
max_open_files() -> 10000.
-endif.

read_data_sync_state() ->
    case ar_storage:read_term(data_sync_state) of
        {ok, #{ block_index := RecentBI } = M} ->
            maps:merge(M, #{
                            weave_size => case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end });
        not_found ->
            #{ block_index => [], disk_pool_data_roots => #{}, disk_pool_size => 0,
               weave_size => 0, packing_2_5_threshold => infinity }
    end.

remove_orphaned_data(_State, BlockStartOffset, WeaveSize)
  when BlockStartOffset > WeaveSize ->
    ?LOG_WARNING([{event, skipping_invalid_orphan_range},
                  {block_start_offset, BlockStartOffset},
                  {weave_size, WeaveSize}]),
    ok;
remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
    #data_sync_state{ store_id = StoreID } = State,
    ok = ar_data_roots:remove_tx_index_range(BlockStartOffset, WeaveSize, StoreID),
    {ok, OrphanedDataRoots} =
        ar_data_roots:remove_range(BlockStartOffset, WeaveSize, StoreID),
    ok = delete_chunk_metadata_range(BlockStartOffset, WeaveSize, State),
    ok = arweave_storage:cut_sync_record(
        BlockStartOffset, {ar_chunk_storage, byte}, StoreID
    ),
    ok = arweave_storage:cut_sync_record(BlockStartOffset, {ar_data_sync, byte}, StoreID),
    ar_events:send(sync_record, {global_cut, BlockStartOffset}),
    ar_disk_pool:reset_orphaned_data_roots_timestamps(OrphanedDataRoots),
    ok.

cut_orphaned_storage_modules(BlockStartOffset, WeaveSize)
  when BlockStartOffset >= WeaveSize ->
    ok;
cut_orphaned_storage_modules(BlockStartOffset, _WeaveSize) ->
    lists:foreach(
      fun(Module) ->
              gen_server:cast(name((arweave_storage:store_info(Module))#store_info.id), {cut, BlockStartOffset})
      end,
      arweave_config:storage_modules()),
    ok.

store_sync_state(#data_sync_state{ store_id = ?DEFAULT_MODULE } = State) ->
    #data_sync_state{ block_index = BI } = State,
    DiskPoolDataRoots = ar_disk_pool:get_data_roots(),
    StoredState = #{ block_index => BI, disk_pool_data_roots => DiskPoolDataRoots,
                     %% Storing it for backwards-compatibility.
                     strict_data_split_threshold => arweave_constants:strict_data_split_threshold() },
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

%% @doc Validate fetched proof paths and return protocol metadata to the caller.
validate_fetched_chunk(Peer, Byte, Proof) ->
    SeekByte = arweave_storage:get_chunk_seek_offset(Byte + 1) - 1,
    case validate_proof(SeekByte, Proof, Peer) of
        false -> false;
        {true, ChunkProof} -> fetched_chunk_metadata(valid, Proof, ChunkProof, Peer, Byte);
        {need_unpacking, _, ChunkProof} ->
            fetched_chunk_metadata(needs_unpacking, Proof, ChunkProof, Peer, Byte)
    end.

fetched_chunk_metadata(Status, Proof, ChunkProof, Peer, Byte) ->
    #{data_path := DataPath, tx_path := TXPath, chunk := Chunk,
        packing := Packing} = Proof,
    #chunk_proof{block_start_offset = BlockStartOffset,
        tx_start_offset = TXStartOffset, tx_end_offset = TXEndOffset,
        chunk_end_offset = ChunkEndOffset, chunk_id = ChunkID,
        metadata = #chunk_metadata{tx_root = TXRoot, data_root = DataRoot,
            chunk_size = ChunkSize}} = ChunkProof,
    TXSize = TXEndOffset - TXStartOffset,
    AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
    AbsoluteEndOffset = AbsoluteTXStartOffset + ChunkEndOffset,
    {Status, {Packing, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
        {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
            Chunk, ChunkID, ChunkEndOffset, Peer, Byte}}.

validate_proof(SeekByte, Proof, Peer) ->
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
            case do_additional_validation(ChunkProof2, DataPath, Peer) of
                false ->
                    false;
                true ->
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
            end
    end.

do_additional_validation(ChunkProof, DataPath, Peer) ->
    #chunk_proof{
       seek_byte = SeekByte,
       block_start_offset = BlockStartOffset,
       tx_start_offset = TXStartOffset,
       tx_end_offset = TXEndOffset,
       chunk_start_offset = ChunkStartOffset,
       chunk_end_offset = ChunkEndOffset,
       validate_data_path_ruleset = Ruleset,
       metadata = #chunk_metadata{ data_root = DataRoot }
      } = ChunkProof,
    TXSize = TXEndOffset - TXStartOffset,
    TXRelativeOffset = SeekByte - BlockStartOffset - TXStartOffset,
    case ar_merkle:has_redundant_rebase_marker(
           DataRoot, TXRelativeOffset, TXSize, DataPath, Ruleset) of
        true ->
            log_invalid_fetched_data_path(redundant_rebase_marker, Peer,
                                          [{data_root, arweave_util:encode(DataRoot)},
                                           {offset, TXRelativeOffset}, {tx_size, TXSize}]),
            false;
        false ->
            case ar_merkle:has_positive_leaf_size(TXRelativeOffset, TXSize, DataPath) of
                true ->
                    true;
                false ->
                    log_invalid_fetched_data_path(negative_leaf_size, Peer,
                                                  [{data_root, arweave_util:encode(DataRoot)},
                                                   {offset, TXRelativeOffset}, {tx_size, TXSize},
                                                   {chunk_start_offset, ChunkStartOffset},
                                                   {chunk_end_offset, ChunkEndOffset}]),
                    false
            end
    end.

log_invalid_fetched_data_path(Reason, Peer, Logs) ->
    ?LOG_ERROR([{event, invalid_fetched_data_path}, {reason, Reason},
                {peer, arweave_util:format_peer(Peer)} | Logs]).

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
    AbsoluteEndOffset = BlockStartOffset + BlockRelativeOffset,
    ChunkProof = ar_poa:chunk_proof(ChunkMetadata, BlockStartOffset, BlockEndOffset, AbsoluteEndOffset, ValidateDataPathRuleset),
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
        arweave_storage:is_storage_supported(Offset, ChunkSize, Packing),
    case {ShouldStoreInChunkStorage, is_binary(DataPath)} of
        {true, true} ->
            PaddedOffset = arweave_constants:get_chunk_padded_offset(Offset),
            case put_chunk_with_entropy(PaddedOffset, Chunk, Packing, StoreID) of
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
            PaddedOffset = arweave_constants:get_chunk_padded_offset(Offset),
            put_chunk_with_entropy(PaddedOffset, Chunk, Packing, StoreID);
        {false, true} ->
            case put_chunk_data(ChunkDataKey, StoreID, {Chunk, DataPath}) of
                ok ->
                    arweave_metrics:counter_inc(chunks_stored, [
                                                           arweave_storage:packing_label(Packing),
                                                           (arweave_storage:store_info(StoreID))#store_info.label]),
                    {ok, Packing};
                Error -> Error
            end;
        {false, false} ->
            %% For chunks which are only stored in chunk_data_db, we currently require that
            %% both the Chunk and the DataPath are present.
            {error, invalid_data_path}
    end.

%% @doc Repair missing prepared entropy outside the storage process and retry.
put_chunk_with_entropy(Offset, Chunk, Packing, StoreID) ->
    case arweave_storage:put_chunk(Offset, Chunk, Packing, StoreID) of
        {error, {missing_entropy, RewardAddr}} ->
            case arweave_entropy:generate_chunk(Offset, RewardAddr) of
                {error, _} = Error -> Error;
                Entropy ->
                    arweave_storage:put_chunk(Offset,
                        {chunk_with_entropy, Chunk, Entropy}, Packing, StoreID)
            end;
        Result -> Result
    end.

update_chunks_index(Args, UpdateFootprint, StoreID) ->
    AbsoluteChunkOffset = element(1, Args),
    case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
        true ->
            ok;
        false ->
            update_chunks_index2(Args, UpdateFootprint, StoreID)
    end.

update_chunks_index2(Args, UpdateFootprint, StoreID) ->
    {AbsoluteEndOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
     Packing} = Args,
    Metadata = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
    case put_chunk_metadata(AbsoluteEndOffset, StoreID, Metadata) of
        ok ->
            StartOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize),
            PaddedOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset),
            case arweave_storage:add_sync_record(
                PaddedOffset,
                StartOffset,
                Packing,
                {ar_data_sync, byte},
                StoreID
            ) of
                ok ->
                    case UpdateFootprint of
                        true ->
                            case arweave_storage:add_footprint(PaddedOffset, Packing, StoreID) of
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

process_store_request(
    Args,
    {Client, _, _} = ReplyTo,
    #data_sync_state{store_id = StoreID} = State
) ->
    case Client =:= none orelse is_process_alive(Client) of
        false ->
            {finished, {error, cancelled}, State};
        true ->
            case is_disk_space_sufficient(StoreID) of
                true ->
                    pack_and_store_chunk(Args, ReplyTo, State);
                _ ->
                    retry_store_chunk(30000, Args, ReplyTo, State)
            end
    end.

%% @doc Finish the request or keep its cache reference for pending work.
continue_store_request(ReplyTo, {finished, Result, State}) ->
    finish_store_request(ReplyTo, Result),
    State;
continue_store_request(_ReplyTo, {pending, write, State}) ->
    %% Enqueueing may finish this request and earlier queued requests.
    %% Each write releases its own reference when the queue is drained.
    process_store_chunk_queue(State);
continue_store_request(_ReplyTo, {pending, _Stage, State}) ->
    %% Packing jobs and retries keep the request in packing_map.
    State.

pack_and_store_chunk(
    Args = {_, AbsoluteEndOffset, _, _, _, _, _, _, _, _, _, _},
    ReplyTo,
    #data_sync_state{
        store_id = StoreID,
        footprint_limit = Limit
    } = State
) ->
    case should_skip_chunk(AbsoluteEndOffset, Limit) of
        {true, Reason} ->
            ?LOG_DEBUG([
                {event, skipping_synced_chunk},
                {reason, Reason},
                {absolute_end_offset, AbsoluteEndOffset},
                {store_id, StoreID}
            ]),
            {finished, {skipped, Reason}, State};
        false ->
            pack_and_store_chunk2(Args, ReplyTo, State)
    end.

%% @doc Whether the module must not store the chunk: it is not yet well
%% confirmed, or it lies past the footprints the module keeps.
should_skip_chunk(AbsoluteEndOffset, FootprintLimit) ->
    PaddedOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset),
    IsBeyondLimit = ar_footprint_limit:is_beyond(PaddedOffset, FootprintLimit),
    case {AbsoluteEndOffset > ar_disk_pool:get_threshold(), IsBeyondLimit} of
        {true, _} -> {true, chunk_is_above_disk_pool_threshold};
        {false, true} -> {true, chunk_is_beyond_footprint_limit};
        {false, false} -> false
    end.

pack_and_store_chunk2(Args, ReplyTo, State) ->
    {DataRoot, AbsoluteEndOffset, TXPath, TXRoot, DataPath, Packing, Offset,
        ChunkSize, Chunk, UnpackedChunk, OriginStoreID,
        OriginChunkDataKey} = Args,
    #data_sync_state{store_id = StoreID, packing_map = PackingMap} = State,
    RequiredPacking = get_required_chunk_packing(
        AbsoluteEndOffset, ChunkSize, Packing, State
    ),
    PackingStatus =
        case {RequiredPacking, Packing} of
            {Packing, Packing} ->
                {ready, {Packing, Chunk}};
            {DifferentPacking, _} ->
                {need_packing, DifferentPacking}
        end,
    case PackingStatus of
        {ready, {StoredPacking, StoredChunk}} ->
            ChunkArgs =
                {StoredPacking, StoredChunk, AbsoluteEndOffset, TXRoot,
                    ChunkSize},
            enqueue_chunk(
                ChunkArgs,
                {StoredPacking, DataPath, Offset, DataRoot, TXPath,
                    OriginStoreID, OriginChunkDataKey},
                ReplyTo,
                State
            );
        {need_packing, RequiredPacking} ->
            case
                maps:is_key({AbsoluteEndOffset, RequiredPacking}, PackingMap)
            of
                true ->
                    Reason = chunk_already_being_packed,
                    ?LOG_DEBUG([
                        {event, skipping_synced_chunk},
                        {reason, Reason},
                        {absolute_end_offset, AbsoluteEndOffset},
                        {store_id, StoreID}
                    ]),
                    {finished, {skipped, Reason}, State};
                false ->
                    {_, _, CacheRef} = ReplyTo,
                    {Packing2, Chunk2} =
                        case UnpackedChunk of
                            none -> {Packing, Chunk};
                            _ -> {unpacked, UnpackedChunk}
                        end,
                    Ref = make_ref(),
                    Key = {AbsoluteEndOffset, RequiredPacking},
                    Result = ar_packing_server:request_repack(
                        {AbsoluteEndOffset, RequiredPacking, Ref},
                        self(),
                        {RequiredPacking, Packing2, Chunk2, AbsoluteEndOffset,
                            TXRoot, ChunkSize},
                        CacheRef
                    ),
                    case Result of
                        ok ->
                            PackingArgs =
                                {pack_chunk,
                                    {Ref,
                                        {RequiredPacking, DataPath, Offset,
                                            DataRoot, TXPath, OriginStoreID,
                                            OriginChunkDataKey}},
                                    ReplyTo},
                            {pending, packing, State#data_sync_state{
                                packing_map = PackingMap#{Key => PackingArgs}
                            }};
                        busy ->
                            retry_store_chunk(1000, Args, ReplyTo, State);
                        {error, Reason} ->
                            {finished, {error, Reason}, State}
                    end
            end
    end.

%% @doc Keep retry payloads in the cancellable state, not in timer messages.
retry_store_chunk(Delay, Args, ReplyTo, State) ->
    #data_sync_state{packing_map = PackingMap} = State,
    Ref = make_ref(),
    arweave_util:cast_after(Delay, self(), {retry_store_chunk, Ref}),
    {pending, retry, State#data_sync_state{
        packing_map =
            PackingMap#{Ref => {store_retry, Args, ReplyTo}}
    }}.

cancel_packing_requests(Client, PackingMap) ->
    %% Packing jobs and delayed storage retries share the same reply context.
    maps:filter(
        fun(_Key, {_Type, _Args, ReplyTo}) ->
            not cancel_store_request(Client, ReplyTo)
        end,
        PackingMap
    ).

cancel_queued_chunks(Client, Queue) ->
    Entries = gb_sets:to_list(Queue),
    Remaining = lists:filter(
        fun({_Offset, _Timestamp, _Ref, _ChunkArgs, _Args, ReplyTo}) ->
            not cancel_store_request(Client, ReplyTo)
        end,
        Entries
    ),
    gb_sets:from_list(Remaining).

%% @doc Cancel a matching request and report whether it was cancelled.
cancel_store_request(Client, {Client, _Ref, _CacheRef} = ReplyTo) ->
    finish_store_request(ReplyTo, {error, cancelled}),
    true;
cancel_store_request(_Client, _ReplyTo) ->
    false.

%% @doc Write queued chunks on disk in the ascending offset order. Pop the
%% smallest-offset chunk while the queue holds at least
%% ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD chunks or the oldest entry has been
%% queued for longer than ?STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD.
process_store_chunk_queue(State) ->
    #data_sync_state{store_chunk_queue = Q} = State,
    maybe
        false ?= gb_sets:is_empty(Q),
        Timestamp = element(2, gb_sets:smallest(Q)),
        Now = os:system_time(millisecond),
        true ?= gb_sets:size(Q) >= ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD orelse
            Now - Timestamp > ?STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD,
        {{_Offset, _Timestamp, _Ref, ChunkArgs, Args, ReplyTo}, Q2} =
            gb_sets:take_smallest(Q),
        Result = store_chunk2(ChunkArgs, Args, State),
        case Result of
            stored ->
                ar_chunk_cache:record_completed(
                    State#data_sync_state.store_id
                );
            _ ->
                ok
        end,
        finish_store_request(ReplyTo, Result),
        process_store_chunk_queue(State#data_sync_state{store_chunk_queue = Q2})
    else
        _ ->
            State
    end.

enqueue_chunk(ChunkArgs, Args, ReplyTo, State) ->
    %% Let at least N chunks stack up, then write them in the ascending order,
    %% to reduce out-of-order disk writes causing fragmentation.
    #data_sync_state{store_chunk_queue = Q} = State,
    Now = os:system_time(millisecond),
    Offset = element(3, ChunkArgs),
    Q2 = gb_sets:add_element(
        {Offset, Now, make_ref(), ChunkArgs, Args, ReplyTo}, Q
    ),
    {pending, write, State#data_sync_state{store_chunk_queue = Q2}}.

%% @doc Write the chunk and records, returning the terminal storage outcome.
store_chunk2(ChunkArgs, Args, State) ->
    #data_sync_state{ store_id = StoreID } = State,
    {Packing, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
    {_Packing, DataPath, Offset, DataRoot, TXPath, OriginStoreID, OriginChunkDataKey} = Args,
    PaddedOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset),
    StartOffset = arweave_constants:get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize),
    %% This will fail if DataPath is not a string - which is fine as it serves as a sanity
    %% check that store_chunk2 is called with valid arguments.
    DataPathHash = crypto:hash(sha256, DataPath),
    ShouldStoreInChunkStorage = arweave_storage:is_storage_supported(AbsoluteEndOffset,
                                                                      ChunkSize, Packing),
    #store_info{packing = StorePacking} = arweave_storage:store_info(StoreID),
    CleanRecord =
        case {ShouldStoreInChunkStorage, StorePacking} of
            {true, {replica_2_9, _}} ->
                %% The 2.9 chunk storage is write-once.
                ok;
            _ ->
                case arweave_storage:delete_footprint(PaddedOffset, StoreID) of
                    ok ->
                        arweave_storage:delete_sync_record(PaddedOffset, StartOffset, {ar_data_sync, byte}, StoreID);
                    Error ->
                        Error
                end
        end,
    case CleanRecord of
        {error, Reason} ->
            log_failed_to_store_chunk(Reason, AbsoluteEndOffset, Offset, DataRoot, DataPathHash,
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
                case write_chunk(AbsoluteEndOffset, ChunkDataKey, Chunk, ChunkSize, DataPath,
                                 Packing, StoreID) of
                    {ok, NewPacking} ->
                        {true, NewPacking};
                    Error2 ->
                        Error2
                end,
            ProcessAlreadyStored =
                case StoreIndex of
                    already_stored ->
                        case arweave_storage:is_recorded(
                            PaddedOffset,
                            Packing,
                            {ar_data_sync, byte},
                            StoreID
                        ) of
                            false ->
                                do_invalidate_bad_data_record({AbsoluteEndOffset, ChunkSize,
                                                               StoreID, undefined,
                                                               chunk_already_stored_but_not_in_sync_record});
                            true ->
                                case arweave_storage:is_recorded(
                arweave_storage:get_footprint_offset(PaddedOffset), any_packing,
                {ar_data_sync, footprint}, StoreID) of
                                    false ->
                                        %% Repair the broken footprint record.
                                        arweave_storage:add_footprint(PaddedOffset, Packing, StoreID);
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
                    case update_chunks_index({AbsoluteEndOffset, Offset, ChunkDataKey, TXRoot,
                            DataRoot, TXPath, ChunkSize, Packing2}, true, StoreID) of
                        ok ->
                            stored;
                        {error, Reason} ->
                            log_failed_to_store_chunk(Reason, AbsoluteEndOffset, Offset, DataRoot,
                                                      DataPathHash, StoreID),
                            {error, Reason}
                    end;
                already_stored ->
                    {skipped, already_stored};
                {error, Reason} ->
                    log_failed_to_store_chunk(Reason, AbsoluteEndOffset, Offset, DataRoot,
                                              DataPathHash, StoreID),
                    {error, Reason}
            end
    end.

log_failed_to_store_chunk(already_stored,
                          AbsoluteEndOffset, Offset, DataRoot, DataPathHash, StoreID) ->
    ?LOG_INFO([{event, chunk_already_stored},
               {absolute_end_offset, AbsoluteEndOffset},
               {relative_offset, Offset},
               {data_path_hash, arweave_util:safe_encode(DataPathHash)},
               {data_root, arweave_util:safe_encode(DataRoot)},
               {store_id, StoreID}]);
log_failed_to_store_chunk(not_prepared_yet,
                          AbsoluteEndOffset, Offset, DataRoot, DataPathHash, StoreID) ->
    ?LOG_WARNING([{event, chunk_not_prepared_yet},
                  {absolute_end_offset, AbsoluteEndOffset},
                  {relative_offset, Offset},
                  {data_path_hash, arweave_util:safe_encode(DataPathHash)},
                  {data_root, arweave_util:safe_encode(DataRoot)},
                  {store_id, StoreID}]);
log_failed_to_store_chunk(Reason, AbsoluteEndOffset, Offset, DataRoot, DataPathHash, StoreID) ->
    ?LOG_ERROR([{event, failed_to_store_chunk},
                {reason, io_lib:format("~p", [Reason])},
                {absolute_end_offset, AbsoluteEndOffset},
                {relative_offset, Offset},
                {data_path_hash, arweave_util:safe_encode(DataPathHash)},
                {data_root, arweave_util:safe_encode(DataRoot)},
                {store_id, StoreID}]).

%% @doc Return the packing a chunk arriving with Packing must have to be
%% written into this module.
get_required_chunk_packing(_Offset, _ChunkSize, _Packing,
        #data_sync_state{ store_id = ?DEFAULT_MODULE }) ->
    unpacked;
get_required_chunk_packing(Offset, ChunkSize, Packing, State) ->
    #data_sync_state{ store_id = StoreID } = State,
    IsEarlySmallChunk =
        Offset =< arweave_constants:strict_data_split_threshold() andalso ChunkSize < ?DATA_CHUNK_SIZE,
    case IsEarlySmallChunk of
        true ->
            unpacked;
        false ->
            #store_info{packing = StorePacking} =
                arweave_storage:store_info(StoreID),
            case StorePacking of
                Packing ->
                    %% Already packed for this module: store it as it is.
                    %% A replica.2.9 module takes the unpacked chunk and
                    %% enciphers it with its prepared entropy, but a chunk
                    %% packed for the same address is byte for byte what
                    %% that produces, so unpacking it first (one entropy per
                    %% sub-chunk) is pure cost: a cross-module copy between
                    %% same-address replica.2.9 modules ran at 1/500 of disk
                    %% speed that way, generating entropy for every chunk.
                    Packing;
                {replica_2_9, _Addr} ->
                    unpacked_padded;
                _ ->
                    StorePacking
            end
    end.


get_merkle_rebase_threshold() ->
    ets:lookup_element(node_state, merkle_rebase_support_threshold, 2, infinity).


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
    ?LOG_INFO([{event, storage_module_stopped_syncing},
               {reason, insufficient_disk_space}, {storage_module, StoreID}]).

%% @doc Ask storage to build the store's footprint record, unless the releases
%% that built it chunk by chunk had already finished. Only their completion
%% carries over: their byte cursor says nothing about how far a
%% footprint-ordered walk got, so an interrupted one starts again.
run_footprint_record_initialization(StoreID) ->
    case ar_kv:get(migration_db(StoreID),
                   ?LEGACY_FOOTPRINT_MIGRATION_CURSOR_KEY) of
        {ok, <<"complete">>} ->
            arweave_storage:mark_footprint_record_initialized(StoreID);
        _ ->
            arweave_storage:initialize_footprint_record(StoreID)
    end.

-ifdef(AR_TEST).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc A reader-path invalidation must be dropped when the chunk_data_key the
%% reader observed no longer matches the currently indexed one - a newer write
%% replaced the copy the verdict was formed on. With no observed key (legacy
%% callers) or nothing indexed, there is no fresh copy to protect, so the
%% invalidation proceeds.
is_stale_invalidation_test_() ->
    [
        {"undefined observed key never skips",
         fun() ->
             ?assertEqual(false, is_stale_invalidation(?DATA_CHUNK_SIZE, "s", undefined))
         end},
        {"matching key proceeds",
         with_mocked_chunks_index(<<"k1">>,
             fun() ->
                 ?assertEqual(false, is_stale_invalidation(?DATA_CHUNK_SIZE, "s", <<"k1">>))
             end)},
        {"mismatched key is stale and skips",
         with_mocked_chunks_index(<<"k2">>,
             fun() ->
                 ?assertEqual(true, is_stale_invalidation(?DATA_CHUNK_SIZE, "s", <<"k1">>))
             end)},
        {"missing metadata proceeds",
         with_mocked_chunks_index(not_found,
             fun() ->
                 ?assertEqual(false, is_stale_invalidation(?DATA_CHUNK_SIZE, "s", <<"k1">>))
             end)}
    ].

%% Resolve get_chunk_metadata to a chunk whose chunk_data_key is `CurrentKey',
%% or to `not_found' when `CurrentKey' is the atom `not_found', by mocking the
%% underlying chunks_index read.
with_mocked_chunks_index(CurrentKey, TestFun) ->
    Reply =
        case CurrentKey of
            not_found ->
                not_found;
            _ ->
                {ok, term_to_binary({CurrentKey, <<>>, <<>>, <<>>, 0, ?DATA_CHUNK_SIZE})}
        end,
    ar_test_util:with_mocked(
        [{ar_kv, get, fun(_Name, _Key) -> Reply end}],
        TestFun).


-endif.
