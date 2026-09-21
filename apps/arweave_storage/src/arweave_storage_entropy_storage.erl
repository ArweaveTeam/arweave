-module(arweave_storage_entropy_storage).

-behaviour(gen_server).

-export([
    name/1,
    acquire_semaphore/1,
    release_semaphore/1,
    is_ready/1,
    sync_record_id/0,
    is_entropy_recorded/3,
    get_next_unsynced_interval/3,
    add_record/3,
    add_record_async/4,
    delete_record/2, delete_record/3,
    store_entropy_footprint/2,
    initialize_context/2,
    read_cursor/2,
    write_cursor/2,
    store_entropy/4,
    record_chunk/5
]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

%% Idle time after which the server hibernates and drops its dead heap.
-define(IDLE_HIBERNATE_MS, 10_000).

-record(state, {
    store_id
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, _}) ->
    %% Each footprint write passes 32 8 MiB entropies through this heap.
    %% Hibernating once idle releases the last of them instead of pinning
    %% them for the life of the node.
    gen_server:start_link({local, Name}, ?MODULE, StoreID, [
        {hibernate_after, ?IDLE_HIBERNATE_MS}
    ]).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
    list_to_atom("ar_entropy_storage_" ++ arweave_storage_module:label(StoreID)).

init(StoreID) ->
    ?LOG_INFO([{event, ar_entropy_storage_init}, {name, name(StoreID)}, {store_id, StoreID}]),
    {ok, #state{store_id = StoreID}}.

sync_record_id() ->
    ar_chunk_storage_replica_2_9_5_entropy.

%% @doc Write a footprint supplied as a fold over assembled chunk entropies.
%% The fold only slices existing binaries; generation stays with the caller.
store_entropy_footprint(StoreID, Fold) ->
    gen_server:cast(name(StoreID), {store_entropy_footprint, Fold}).

store_entropy(ChunkEntropy, BucketEndOffset, StoreID, RewardAddr) ->
    case
        catch gen_server:call(
            name(StoreID),
            {store_entropy, ChunkEntropy, BucketEndOffset, StoreID, RewardAddr},
            ?DEFAULT_CALL_TIMEOUT
        )
    of
        {'EXIT', {Reason, {gen_server, call, _}}} ->
            ?LOG_WARNING([
                {event, store_entropy},
                {module, ?MODULE},
                {name, name(StoreID)},
                {store_id, StoreID},
                {bucket_end_offset, BucketEndOffset},
                {reason, Reason}
            ]),
            false;
        Reply ->
            Reply
    end.

is_ready(StoreID) ->
    case catch gen_server:call(name(StoreID), is_ready, ?DEFAULT_CALL_TIMEOUT) of
        {'EXIT', {Reason, {gen_server, call, _}}} ->
            ?LOG_WARNING([
                {event, is_ready_error},
                {module, ?MODULE},
                {name, name(StoreID)},
                {store_id, StoreID},
                {reason, Reason}
            ]),
            false;
        Reply ->
            Reply
    end.

handle_cast({store_entropy_footprint, Fold}, State) ->
    Fold(fun do_store_entropy/5, ok),
    {noreply, State};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_call(is_ready, _From, State) ->
    {reply, true, State};
handle_call(
    {store_entropy, ChunkEntropy, BucketEndOffset, StoreID, RewardAddr},
    _From,
    State
) ->
    #state{store_id = StoreID} = State,
    do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}]),
    {reply, {error, unhandled_call}, State}.

terminate(Reason, State) ->
    ?LOG_INFO([
        {event, terminate},
        {module, ?MODULE},
        {reason, Reason},
        {name, name(State#state.store_id)},
        {store_id, State#state.store_id}
    ]),
    ok.

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

%% @doc Return true if the 2.9 entropy with the given offset is recorded.
is_entropy_recorded(PaddedEndOffset, {replica_2_9, _} = Packing, StoreID) ->
    ChunkBucketStart = arweave_storage:get_chunk_bucket_start(PaddedEndOffset),
    IsRecorded = arweave_storage:is_recorded(
        ChunkBucketStart + 1, Packing, {sync_record_id(), byte}, StoreID
    ),
    case IsRecorded of
        false ->
            %% Included for backwards compatibility with entropy written prior to 2.9.5.
            arweave_storage:is_recorded(
                ChunkBucketStart + 1,
                any_packing,
                {ar_chunk_storage_replica_2_9_1_entropy, byte},
                StoreID
            );
        _ ->
            true
    end;
is_entropy_recorded(_PaddedEndOffset, _Packing, _StoreID) ->
    false.

get_next_unsynced_interval(Offset, Packing, StoreID) ->
    case
        arweave_storage:get_next_interval(
            unsynced,
            Offset,
            infinity,
            Packing,
            {sync_record_id(), byte},
            StoreID
        )
    of
        not_found ->
            %% Included for backwards compatibility with entropy written prior to 2.9.5.
            case
                arweave_storage:get_next_interval(
                    unsynced,
                    Offset,
                    infinity,
                    any_packing,
                    {ar_chunk_storage_replica_2_9_1_entropy, byte},
                    StoreID
                )
            of
                not_found ->
                    not_found;
                Interval ->
                    Interval
            end;
        Interval ->
            Interval
    end.

update_sync_records(IsComplete, PaddedEndOffset, StoreID, RewardAddr) ->
    BucketEnd = arweave_storage:get_chunk_bucket_end(PaddedEndOffset),
    add_record_async(replica_2_9_entropy, BucketEnd, {replica_2_9, RewardAddr}, StoreID),
    arweave_metrics:counter_inc(
        replica_2_9_entropy_stored,
        [arweave_storage_module:label(StoreID)],
        ?DATA_CHUNK_SIZE
    ),
    StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
    case IsComplete of
        true ->
            Packing = {replica_2_9, RewardAddr},

            arweave_metrics:counter_inc(
                chunks_stored,
                [
                    arweave_storage:packing_label(Packing),
                    arweave_storage_module:label(StoreID)
                ]
            ),
            arweave_storage_sync_record:add_async(
                replica_2_9_entropy_with_chunk,
                PaddedEndOffset,
                StartOffset,
                {ar_chunk_storage, byte},
                StoreID
            ),
            arweave_storage_sync_record:add_async(
                replica_2_9_entropy_with_chunk,
                PaddedEndOffset,
                StartOffset,
                {replica_2_9, RewardAddr},
                {ar_data_sync, byte},
                StoreID
            ),
            arweave_storage_footprint_record:add_async(
                replica_2_9_entropy_with_chunk,
                PaddedEndOffset,
                Packing,
                StoreID
            );
        false ->
            ok
    end.

add_record(BucketEndOffset, {replica_2_9, _} = Packing, StoreID) ->
    BucketStartOffset = BucketEndOffset - ?DATA_CHUNK_SIZE,
    arweave_storage:add_sync_record(
        BucketEndOffset, BucketStartOffset, Packing, {sync_record_id(), byte}, StoreID
    ).

add_record_async(Event, BucketEndOffset, {replica_2_9, _} = Packing, StoreID) ->
    BucketStartOffset = BucketEndOffset - ?DATA_CHUNK_SIZE,
    arweave_storage_sync_record:add_async(
        Event,
        BucketEndOffset,
        BucketStartOffset,
        Packing,
        {sync_record_id(), byte},
        StoreID
    ).

delete_record(PaddedEndOffset, StoreID) ->
    BucketStart = arweave_storage:get_chunk_bucket_start(PaddedEndOffset),
    delete_record(BucketStart + ?DATA_CHUNK_SIZE, BucketStart, StoreID).

delete_record(EndOffset, StartOffset, StoreID) ->
    case
        arweave_storage:delete_sync_record(
            EndOffset, StartOffset, {sync_record_id(), byte}, StoreID
        )
    of
        ok ->
            %% Included for backwards compatibility with entropy written prior to 2.9.5.
            arweave_storage:delete_sync_record(
                EndOffset, StartOffset, {ar_chunk_storage_replica_2_9_1_entropy, byte}, StoreID
            );
        Error ->
            Error
    end.

initialize_context(StoreID, Packing) ->
    case Packing of
        {replica_2_9, Addr} ->
            #store_info{effective_range = {ModuleStart, ModuleEnd}} =
                arweave_storage:store_info(StoreID),
            Cursor = read_cursor(StoreID, ModuleStart),
            case Cursor =< ModuleEnd of
                true ->
                    {false, Addr};
                false ->
                    {true, Addr}
            end;
        _ ->
            {true, none}
    end.

read_cursor(StoreID, ModuleStart) ->
    Filepath = arweave_storage:chunk_filepath("prepare_replica_2_9_cursor", StoreID),
    Default = ModuleStart + 1,
    case file:read_file(Filepath) of
        {ok, Bin} ->
            case catch binary_to_term(Bin, [safe]) of
                Cursor when is_integer(Cursor) ->
                    Cursor;
                _ ->
                    Default
            end;
        _ ->
            Default
    end.

write_cursor(Cursor, StoreID) ->
    Filepath = arweave_storage:chunk_filepath("prepare_replica_2_9_cursor", StoreID),
    file:write_file(Filepath, term_to_binary(Cursor)).

record_chunk(Offset, Chunk, StoreID, FileIndex, {IsPrepared, RewardAddr}) when
    is_binary(Chunk)
->
    record_chunk(
        Offset,
        {chunk_with_entropy, Chunk, none},
        StoreID,
        FileIndex,
        {IsPrepared, RewardAddr}
    );
record_chunk(
    PaddedEndOffset,
    {chunk_with_entropy, Chunk, Entropy},
    StoreID,
    FileIndex,
    {IsPrepared, RewardAddr}
) ->
    %% Sanity checks
    PaddedEndOffset = arweave_constants:get_chunk_padded_offset(PaddedEndOffset),
    %% End sanity checks

    Packing = {replica_2_9, RewardAddr},
    StartOffset = arweave_storage:get_chunk_bucket_start(PaddedEndOffset),
    {_ChunkFileStart, Filepath, _Position, _ChunkOffset} =
        arweave_storage:locate_chunk_on_disk(PaddedEndOffset, StoreID),
    acquire_semaphore(Filepath),
    %% Release on every exit path. If the function crashes, without this
    %% the semaphore entry leaks in the supervisor-owned ETS table and every
    %% future writer to this file spins forever. Mirrors do_store_entropy/5.
    try
        do_record_chunk(
            PaddedEndOffset,
            Chunk,
            StoreID,
            FileIndex,
            IsPrepared,
            RewardAddr,
            Packing,
            StartOffset,
            Entropy
        )
    after
        release_semaphore(Filepath)
    end.

do_record_chunk(
    PaddedEndOffset,
    Chunk,
    StoreID,
    FileIndex,
    IsPrepared,
    RewardAddr,
    Packing,
    StartOffset,
    SuppliedEntropy
) ->
    CheckIsChunkStoredAlready =
        arweave_storage:is_recorded(
            PaddedEndOffset,
            any_packing,
            {ar_chunk_storage, byte},
            StoreID
        ),
    CheckIsEntropyRecorded =
        case CheckIsChunkStoredAlready of
            true ->
                {error, already_stored};
            false ->
                is_entropy_recorded(PaddedEndOffset, Packing, StoreID)
        end,
    ReadEntropy =
        case CheckIsEntropyRecorded of
            {error, _} = Error ->
                Error;
            false ->
                case IsPrepared of
                    false ->
                        no_entropy_yet;
                    true when is_binary(SuppliedEntropy) ->
                        {PaddedEndOffset, SuppliedEntropy};
                    true ->
                        missing_entropy
                end;
            true ->
                arweave_storage_chunk_storage:get(StartOffset, StartOffset, StoreID)
        end,
    case ReadEntropy of
        {error, _} = Error2 ->
            Error2;
        not_found ->
            delete_record(PaddedEndOffset, StoreID),
            {error, not_prepared_yet};
        missing_entropy ->
            ?LOG_WARNING([
                {event, missing_entropy},
                {padded_end_offset, PaddedEndOffset},
                {store_id, StoreID},
                {packing, arweave_storage_deps:encode_packing(Packing, true)}
            ]),
            {error, {missing_entropy, RewardAddr}};
        no_entropy_yet ->
            arweave_storage_chunk_storage:record_chunk(
                PaddedEndOffset, Chunk, unpacked_padded, StoreID, FileIndex
            );
        {_EndOffset, Entropy} ->
            PackedChunk = arweave_storage_deps:encipher_replica_2_9_chunk(Chunk, Entropy),
            arweave_storage_chunk_storage:record_chunk(
                PaddedEndOffset, PackedChunk, Packing, StoreID, FileIndex
            )
    end.

do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID, ok) ->
    do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID).

do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID) ->
    %% Sanity checks
    true = byte_size(ChunkEntropy) == ?DATA_CHUNK_SIZE,
    %% End sanity checks

    Byte = arweave_storage:get_chunk_byte_from_bucket_end(BucketEndOffset),
    {ChunkFileStart, Filepath, _Position, _ChunkOffset} =
        arweave_storage:locate_chunk_on_disk(BucketEndOffset, StoreID),
    acquire_semaphore(Filepath),
    try
        do_store_entropy_locked(
            ChunkEntropy,
            BucketEndOffset,
            RewardAddr,
            StoreID,
            Byte,
            Filepath,
            ChunkFileStart
        )
    after
        release_semaphore(Filepath)
    end,
    ok.

do_store_entropy_locked(
    ChunkEntropy,
    BucketEndOffset,
    RewardAddr,
    StoreID,
    Byte,
    Filepath,
    ChunkFileStart
) ->
    %% Classify the bucket under the same file semaphore as record_chunk/5;
    %% otherwise entropy and chunk writes can pass each other and leave both
    %% halves stored without the final replica_2_9 chunk.
    {State, PaddedEndOffset} = classify_entropy_target(BucketEndOffset, Byte, StoreID),
    Result =
        case State of
            unpacked_chunk_already_stored ->
                %% Entropy arrived second: read the waiting chunk, encipher it
                %% with this entropy and store the replica_2_9 chunk.
                StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
                case arweave_storage_chunk_storage:get(Byte, StartOffset, StoreID) of
                    not_found ->
                        {error, not_found};
                    {error, _} = Error ->
                        Error;
                    {_, UnpackedChunk} ->
                        arweave_storage:delete_sync_record(
                            PaddedEndOffset, StartOffset, {ar_data_sync, byte}, StoreID
                        ),
                        arweave_storage:delete_footprint(PaddedEndOffset, StoreID),
                        PackedChunk = arweave_storage_deps:encipher_replica_2_9_chunk(
                            UnpackedChunk, ChunkEntropy
                        ),
                        write_entropy_chunk(
                            PackedChunk,
                            PaddedEndOffset,
                            true,
                            StoreID,
                            ChunkFileStart,
                            RewardAddr
                        )
                end;
            packed_chunk_already_stored ->
                %% A replica_2_9 chunk already occupies this slot. Writing bare entropy
                %% would overwrite it (unpacking to all zeroes), so skip the write and
                %% just record the entropy to stop generation revisiting this bucket.
                update_sync_records(true, PaddedEndOffset, StoreID, RewardAddr);
            no_chunk ->
                %% The entropy for the first sub-chunk of the chunk. The zero-offset does
                %% not have a real meaning, it is set to make sure we pass offset
                %% validation on read.
                write_entropy_chunk(
                    ChunkEntropy,
                    PaddedEndOffset,
                    false,
                    StoreID,
                    ChunkFileStart,
                    RewardAddr
                )
        end,

    case Result of
        {error, Reason} ->
            ?LOG_ERROR([
                {event, failed_to_store_replica_2_9_chunk_entropy},
                {filepath, Filepath},
                {byte, Byte},
                {padded_end_offset, PaddedEndOffset},
                {bucket_end_offset, BucketEndOffset},
                {store_id, StoreID},
                {reason, io_lib:format("~p", [Reason])}
            ]);
        _ ->
            ok
    end,
    ok.

%% @doc Write the entropy (or enciphered chunk) to its slot and record it.
%% `ChunkPresent' is true when a data chunk now occupies the slot, false when
%% only entropy was written.
write_entropy_chunk(
    Chunk,
    PaddedEndOffset,
    ChunkPresent,
    StoreID,
    ChunkFileStart,
    RewardAddr
) ->
    case arweave_storage_chunk_storage:write_chunk(PaddedEndOffset, Chunk, #{}, StoreID) of
        {ok, Filepath} ->
            ets:insert(
                chunk_storage_file_index,
                {{ChunkFileStart, StoreID}, Filepath}
            ),
            update_sync_records(ChunkPresent, PaddedEndOffset, StoreID, RewardAddr);
        Error ->
            Error
    end.

%% @doc Classify the bucket an arriving entropy targets, returning the state and
%% the padded end offset to operate on.
classify_entropy_target(BucketEndOffset, Byte, StoreID) ->
    case
        arweave_storage:is_recorded(
            BucketEndOffset,
            any_packing,
            {ar_chunk_storage, byte},
            StoreID
        )
    of
        false ->
            classify_unpacked_target(BucketEndOffset, Byte, StoreID);
        _ ->
            %% Guard against re-enciphering an already stored replica_2_9 chunk.
            {packed_chunk_already_stored, BucketEndOffset}
    end.

classify_unpacked_target(BucketEndOffset, Byte, StoreID) ->
    case
        arweave_storage:get_interval(
            Byte + 1,
            any_packing,
            {arweave_storage_chunk_storage:sync_record_id(unpacked_padded), byte},
            StoreID
        )
    of
        {_IntervalEnd, IntervalStart} ->
            EndOffset =
                IntervalStart +
                    arweave_util:floor_int(Byte - IntervalStart, ?DATA_CHUNK_SIZE) +
                    ?DATA_CHUNK_SIZE,
            case arweave_storage:get_chunk_bucket_end(EndOffset) == BucketEndOffset of
                true ->
                    {unpacked_chunk_already_stored, EndOffset};
                false ->
                    %% Near the strict data split threshold a single byte cannot pick a
                    %% bucket unambiguously; the recorded chunk is from another bucket.
                    %% The caller already established no replica_2_9 chunk is stored
                    %% here, so there is nothing to encipher.
                    ?LOG_INFO([
                        {event, record_entropy_read_chunk_from_another_bucket},
                        {bucket_end_offset, BucketEndOffset},
                        {chunk_end_offset, EndOffset}
                    ]),
                    {no_chunk, BucketEndOffset}
            end;
        not_found ->
            {no_chunk, BucketEndOffset}
    end.

acquire_semaphore(Filepath) ->
    case ets:insert_new(ar_entropy_storage, {{semaphore, Filepath}}) of
        false ->
            ?LOG_DEBUG([
                {event, details_store_chunk}, {section, waiting_on_semaphore}, {filepath, Filepath}
            ]),
            timer:sleep(20),
            acquire_semaphore(Filepath);
        true ->
            ok
    end.

release_semaphore(Filepath) ->
    ets:delete(ar_entropy_storage, {semaphore, Filepath}).
