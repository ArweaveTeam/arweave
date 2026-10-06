%%% Per-store entropy preparation, cursors and device scheduling.
-module(arweave_entropy_preparation).
-export([shift_entropy_offset/2]).

-behaviour(gen_server).

-export([name/1, register_workers/1, generate_entropies/4]).

-export([
    start_link/2,
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_entropy/include/arweave_entropy_deps.hrl").

-record(state, {
    store_id,
    packing,
    module_start,
    module_end,
    cursor,
    prepare_status = undefined,
    %% Footprints of each partition the module keeps.
    footprint_limit
}).

-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%% Idle time after which the server hibernates and drops its dead heap.
-define(IDLE_HIBERNATE_MS, 10_000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, Packing}) ->
    %% Each footprint passes 32 8 MiB entropies through this heap. Hibernating
    %% once idle releases the last of them instead of pinning them for the
    %% life of the node.
    gen_server:start_link({local, Name}, ?MODULE, {StoreID, Packing}, [
        {hibernate_after, ?IDLE_HIBERNATE_MS}
    ]).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
    #store_info{label = Label} = ?DEP(storage):store_info(StoreID),
    list_to_atom("ar_entropy_gen_" ++ Label).

%% @doc Build preparation workers for configured and repack-in-place stores.
register_workers(Module) ->
    ConfiguredWorkers = lists:filtermap(
        fun(StorageModule) ->
            #store_info{
                id = StoreID, packing = Packing
            } = ?DEP(storage):store_info(StorageModule),

            case is_entropy_packing(Packing) of
                true ->
                    Worker = ?CHILD_WITH_ARGS(
                        Module,
                        worker,
                        Module:name(StoreID),
                        [Module:name(StoreID), {StoreID, Packing}]
                    ),
                    {true, Worker};
                false ->
                    false
            end
        end,
        ?DEP(config):storage_modules()
    ),

    RepackInPlaceWorkers = lists:filtermap(
        fun({StorageModule, ToPacking}) ->
            #store_info{
                id = StoreID, packing = ConfiguredPacking
            } = ?DEP(storage):store_info(StorageModule),
            %% Note: the config validation will prevent a StoreID from being used in both
            %% `storage_modules` and `repack_in_place_storage_modules`, so there's
            %% no risk of a `Name` clash with the workers spawned above.
            IsEntropyPacking =
                (is_entropy_packing(ConfiguredPacking) orelse
                    is_entropy_packing(ToPacking)),

            case IsEntropyPacking of
                true ->
                    Worker = ?CHILD_WITH_ARGS(
                        Module,
                        worker,
                        Module:name(StoreID),
                        [Module:name(StoreID), {StoreID, ToPacking}]
                    ),
                    {true, Worker};
                false ->
                    false
            end
        end,
        ?DEP(config):repack_modules(full)
    ),

    ConfiguredWorkers ++ RepackInPlaceWorkers.

initialize_context(StoreID, Packing) ->
    ?DEP(storage):entropy_context(StoreID, Packing).

is_entropy_packing(unpacked_padded) ->
    true;
is_entropy_packing({replica_2_9, _}) ->
    true;
is_entropy_packing(_) ->
    false.

%% @doc Request footprint entropy from the StoreID's preparation worker.
generate_entropies(StoreID, RewardAddr, BucketEndOffset, ReplyTo) ->
    gen_server:cast(
        name(StoreID),
        {generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}
    ).

init({StoreID, Packing}) ->
    ?LOG_INFO([
        {event, ar_entropy_gen_init},
        {name, name(StoreID)},
        {store_id, StoreID},
        {packing, ?DEP(serialize):encode_packing(Packing, true)}
    ]),

    #store_info{
        packing = ConfiguredPacking,
        effective_range = {ModuleStart, ModuleEnd}
    } = ?DEP(storage):store_info(StoreID),
    %% Sanity checks
    true =
        is_entropy_packing(ConfiguredPacking) orelse
            is_entropy_packing(Packing),
    %% End sanity checks

    PaddedRangeEnd = arweave_lib_constants:get_chunk_bucket_end(ModuleEnd),

    %% Provided Packing will only differ from the StoreID packing when this
    %% module is configured to repack in place.
    IsRepackInPlace = Packing /= ConfiguredPacking,
    State =
        case IsRepackInPlace of
            true ->
                #state{};
            false ->
                %% Only kick of the prepare entropy process if we're not repacking in place.
                Cursor = read_cursor(StoreID, ModuleStart),
                ?LOG_INFO([
                    {event, read_prepare_replica_2_9_cursor},
                    {store_id, StoreID},
                    {cursor, Cursor},
                    {module_start, ModuleStart},
                    {module_end, ModuleEnd},
                    {padded_range_end, PaddedRangeEnd}
                ]),
                PrepareStatus =
                    case initialize_context(StoreID, Packing) of
                        {_IsPrepared, none} ->
                            %% Preparation requires replica_2_9 entropy.
                            ?LOG_ERROR([
                                {event, invalid_packing_for_entropy},
                                {module, ?MODULE},
                                {store_id, StoreID},
                                {packing,
                                    ?DEP(serialize):encode_packing(
                                        Packing, true
                                    )}
                            ]),
                            off;
                        {false, _} ->
                            gen_server:cast(self(), prepare_entropy),
                            paused;
                        {true, _} ->
                            %% Entropy generation is complete
                            complete
                    end,
                ?DEP(device_lock):set_device_lock_metric(
                    StoreID, prepare, PrepareStatus
                ),
                #state{
                    cursor = Cursor,
                    prepare_status = PrepareStatus
                }
        end,

    State2 = State#state{
        store_id = StoreID,
        packing = Packing,
        module_start = ModuleStart,
        module_end = PaddedRangeEnd,
        footprint_limit = ?DEP(footprint_limit):get(StoreID)
    },

    {ok, State2}.

handle_cast(prepare_entropy, State) ->
    #state{store_id = StoreID} = State,
    NewStatus = ?DEP(device_lock):acquire_lock(
        prepare, StoreID, State#state.prepare_status
    ),
    State2 = State#state{prepare_status = NewStatus},
    State3 =
        case NewStatus of
            active ->
                do_prepare_entropy(State2);
            paused ->
                {ok, _} = timer:apply_after(
                    ?DEVICE_LOCK_WAIT, gen_server, cast,
                    [self(), prepare_entropy]
                ),
                State2;
            _ ->
                State2
        end,
    {noreply, State3};
handle_cast({generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}, State) ->
    Entropies = arweave_entropy_generation:generate_entropies(
        RewardAddr, BucketEndOffset
    ),
    ReplyTo ! {entropy, BucketEndOffset, RewardAddr, Entropies},
    {noreply, State};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_call(Call, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}]),
    {reply, {error, unhandled_call}, State}.

handle_info({entropy_generated, _Ref, _Entropy}, State) ->
    ?LOG_WARNING([{event, entropy_generation_timed_out}]),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, State) ->
    ?LOG_INFO([
        {event, terminate},
        {module, ?MODULE},
        {reason, Reason},
        {name, name(State#state.store_id)},
        {store_id, State#state.store_id}
    ]),
    ok.

do_prepare_entropy(State) ->
    #state{
        cursor = Start,
        module_start = ModuleStart,
        module_end = ModuleEnd,
        packing = Packing,
        store_id = StoreID
    } = State,

    {replica_2_9, RewardAddr} = Packing,
    BucketEndOffset = arweave_lib_constants:get_chunk_bucket_end(Start),

    %% Sanity checks:
    BucketEndOffset =
        arweave_lib_constants:get_chunk_bucket_end(BucketEndOffset),
    true =
        (arweave_lib_constants:get_chunk_bucket_start(Start) ==
            arweave_lib_constants:get_chunk_bucket_start(BucketEndOffset)),

    true =
        (max(0, BucketEndOffset - ?DATA_CHUNK_SIZE) ==
            arweave_lib_constants:get_chunk_bucket_start(BucketEndOffset)),

    %% End of sanity checks.

    %% A bucket that is not to be prepared, or a failed entropy generation,
    %% is the outcome itself.
    Outcome =
        maybe
            prepare ?= classify_bucket(BucketEndOffset, State),
            %% Every entropy needed to encipher the chunk at BucketEndOffset.
            [_ | _] =
                Entropies ?=
                    arweave_entropy_generation:generate_entropies(
                        RewardAddr, BucketEndOffset, false
                    ),
            EntropyKeys = arweave_entropy_generation:generate_entropy_keys(
                RewardAddr, BucketEndOffset
            ),
            EntropyOffsets = arweave_entropy_generation:entropy_offsets(
                BucketEndOffset, ModuleEnd
            ),
            ?DEP(storage):store_entropy_footprint(
                StoreID,
                fun(Write, Acc) ->
                    arweave_entropy_generation:map_entropies(
                        Entropies,
                        EntropyOffsets,
                        ModuleStart,
                        EntropyKeys,
                        RewardAddr,
                        Write,
                        [StoreID],
                        Acc
                    )
                end
            )
        end,
    case Outcome of
        complete ->
            ?DEP(device_lock):release_lock(prepare, StoreID),
            ?LOG_INFO([
                {event, storage_module_entropy_preparation_complete},
                {store_id, StoreID}
            ]),
            ?DEP(console):console(
                "The storage module ~s is prepared for 2.9 "
                "replication.~n",
                [StoreID]
            ),
            ?DEP(storage):set_entropy_complete(StoreID),
            ?DEP(device_lock):set_device_lock_metric(
                StoreID, prepare, complete
            ),
            State#state{prepare_status = complete};
        recorded ->
            gen_server:cast(self(), prepare_entropy),
            NextCursor = advance_entropy_offset(
                BucketEndOffset,
                Packing,
                StoreID
            ),
            State#state{cursor = NextCursor};
        beyond_limit ->
            %% The rest of this sector is past the footprint limit: move to
            %% the next sector rather than finish. The module's first bucket
            %% straddles the partition boundary and counts as the previous
            %% partition's last sector, so the first bucket past the limit
            %% can come before any footprint of this partition is prepared.
            %% Once they are, the following sectors are recorded and the
            %% loop crosses them one lookup each until it passes the module
            %% end.
            NextCursor =
                arweave_lib_footprint:get_next_sector_start(BucketEndOffset),
            gen_server:cast(self(), prepare_entropy),
            store_prepare_cursor(NextCursor, StoreID),
            State#state{cursor = NextCursor};
        {error, Error} ->
            %% Retry the footprint rather than record the store as prepared
            %% past it.
            ?LOG_ERROR([
                {event, failed_to_store_entropy},
                {cursor, Start},
                {store_id, StoreID},
                {reason, io_lib:format("~p", [Error])}
            ]),
            {ok, _} = timer:apply_after(
                500, gen_server, cast, [self(), prepare_entropy]),
            State;
        ok ->
            NextCursor = advance_entropy_offset(
                BucketEndOffset,
                Packing,
                StoreID
            ),
            gen_server:cast(self(), prepare_entropy),
            store_prepare_cursor(NextCursor, StoreID),
            State#state{cursor = NextCursor}
    end.

%% @doc What to do with the bucket: nothing more past the module end, move
%% past the rest of a sector beyond the footprint limit, skip a footprint
%% whose entropy is recorded, or prepare it.
classify_bucket(BucketEndOffset, #state{module_end = ModuleEnd}) when
    BucketEndOffset > ModuleEnd
->
    complete;
classify_bucket(BucketEndOffset, State) ->
    #state{
        footprint_limit = Limit,
        packing = Packing,
        store_id = StoreID
    } = State,
    case ?DEP(footprint_limit):is_beyond(BucketEndOffset, Limit) of
        true ->
            beyond_limit;
        false ->
            case
                ?DEP(storage):is_entropy_recorded(
                    BucketEndOffset,
                    Packing,
                    StoreID
                )
            of
                true -> recorded;
                false -> prepare
            end
    end.

store_prepare_cursor(Cursor, StoreID) ->
    case store_cursor(Cursor, StoreID) of
        ok ->
            ok;
        {error, Error} ->
            ?LOG_WARNING([
                {event, failed_to_store_prepare_entropy_cursor},
                {chunk_cursor, Cursor},
                {store_id, StoreID},
                {reason, io_lib:format("~p", [Error])}
            ])
    end.

advance_entropy_offset(BucketEndOffset, Packing, StoreID) ->
    case
        ?DEP(storage):get_next_unsynced_entropy_interval(
            BucketEndOffset, Packing, StoreID
        )
    of
        not_found ->
            BucketEndOffset + ?DATA_CHUNK_SIZE;
        {_, Start} ->
            Start + ?DATA_CHUNK_SIZE
    end.

read_cursor(StoreID, ModuleStart) ->
    ?DEP(storage):read_entropy_cursor(StoreID, ModuleStart).

store_cursor(Cursor, StoreID) ->
    ?DEP(storage):write_entropy_cursor(Cursor, StoreID).




shift_entropy_offset(Offset, SectorCount) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    arweave_lib_constants:get_chunk_bucket_end(Offset + SectorSize * SectorCount).
