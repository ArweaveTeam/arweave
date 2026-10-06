-module(arweave_entropy_preparation).
-include_lib("arweave_entropy/include/arweave_entropy_deps.hrl").
-ifdef(AR_TEST).
-export([is_entropy_packing/1, entropy_offsets2/2, reset_entropy_offset/1, generate_entropies/3, do_prepare_entropy/1, classify_bucket/2, store_prepare_cursor/2, do_generate_entropies/3, take_and_combine_entropy_slices/1, take_and_combine_entropy_slices/3, sanity_check_replica_2_9_entropy_keys/3, sanity_check_replica_2_9_entropy_keys/4, advance_entropy_offset/3, generate_entropy_keys/3, collect_entropies/2, flush_entropy_messages/0, read_cursor/2, store_cursor/2]).
-endif.



-behaviour(gen_server).


-export([name/1, register_workers/1,  initialize_context/2,
         map_entropies/8, entropy_offsets/2,
         generate_entropies/2, generate_entropies/4, generate_entropy_keys/2,
         shift_entropy_offset/2]).


-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).


-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_sup.hrl").

-include_lib("arweave/include/ar_consensus.hrl").



-include_lib("eunit/include/eunit.hrl").


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


%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, Packing}) ->
    gen_server:start_link({local, Name}, ?MODULE, {StoreID, Packing}, []).


%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
    list_to_atom("ar_entropy_gen_" ++ arweave_storage_module:label(StoreID)).


register_workers(Module) ->
    ConfiguredWorkers = lists:filtermap(
                          fun(StorageModule) ->
                                  StoreID = arweave_storage_module:id(StorageModule),
                                  Packing = arweave_storage_module:get_packing(StoreID),

                                  case is_entropy_packing(Packing) of
                                      true ->
                                          Worker = ?CHILD_WITH_ARGS(
                                                      Module, worker, Module:name(StoreID),
                                                      [Module:name(StoreID), {StoreID, Packing}]),
                                          {true, Worker};
                                      false ->
                                          false
                                  end
                          end,
                          ?DEP(config):storage_modules()
                         ),

    RepackInPlaceWorkers = lists:filtermap(
                             fun({StorageModule, ToPacking}) ->
                                     StoreID = arweave_storage_module:id(StorageModule),
                                     ConfiguredPacking = arweave_storage_module:get_packing(StorageModule),
                                     %% Note: the config validation will prevent a StoreID from being used in both
                                     %% `storage_modules` and `repack_in_place_storage_modules`, so there's
                                     %% no risk of a `Name` clash with the workers spawned above.
                                     IsEntropyPacking = (
                                       is_entropy_packing(ConfiguredPacking) orelse is_entropy_packing(ToPacking)
                                      ),
                                     case IsEntropyPacking of
                                         true ->
                                             Worker = ?CHILD_WITH_ARGS(
                                                         Module, worker, Module:name(StoreID),
                                                         [Module:name(StoreID), {StoreID, ToPacking}]),
                                             {true, Worker};
                                         false ->
                                             false
                                     end
                             end,
                             ?DEP(config):repack_modules(full)
                            ),

    ConfiguredWorkers ++ RepackInPlaceWorkers.


-spec initialize_context(arweave_storage_module:store_id(), arweave_storage_chunk_storage:packing()) ->
          {IsPrepared :: boolean(), RewardAddr :: none | ar_wallet:address()}.

initialize_context(StoreID, Packing) ->
    case Packing of
        {replica_2_9, Addr} ->
            {ModuleStart, ModuleEnd} = arweave_storage_module:get_range(StoreID),
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


-spec is_entropy_packing(arweave_storage_chunk_storage:packing()) -> boolean().

is_entropy_packing(unpacked_padded) ->
    true;
is_entropy_packing({replica_2_9, _}) ->
    true;
is_entropy_packing(_) ->
    false.


%% @doc Return a list of all BucketEndOffsets covered by the entropy needed to encipher
%% the chunk at the given offset. The list returned may include offsets that occur before
%% the provided offset. This is expected if Offset does not refer to a sector 0 chunk.
-spec entropy_offsets(non_neg_integer(), non_neg_integer()) -> [non_neg_integer()].

entropy_offsets(Offset, ModuleEnd) ->
    BucketEndOffset = arweave_lib_constants:get_chunk_bucket_end(Offset),
    BucketEndOffset2 = reset_entropy_offset(BucketEndOffset),
    Partition = arweave_lib_replica_2_9:get_entropy_partition(BucketEndOffset),
    {_, EntropyPartitionEnd} = arweave_lib_replica_2_9:get_entropy_partition_range(Partition),
    End = min(EntropyPartitionEnd, ModuleEnd),
    entropy_offsets2(BucketEndOffset2, End).


entropy_offsets2(BucketEndOffset, PaddedPartitionEnd)
  when BucketEndOffset > PaddedPartitionEnd ->
    [];
entropy_offsets2(BucketEndOffset, PaddedPartitionEnd) ->
    NextOffset = shift_entropy_offset(BucketEndOffset, 1),
    [BucketEndOffset | entropy_offsets2(NextOffset, PaddedPartitionEnd)].


%% @doc If we are not at the beginning of the entropy, shift the offset to
%% the left. store_entropy_footprint will traverse the entire 2.9 partition shifting
%% the offset by sector size.
reset_entropy_offset(BucketEndOffset) ->
    %% Sanity checks
    BucketEndOffset = arweave_lib_constants:get_chunk_bucket_end(BucketEndOffset),
    %% End sanity checks
    SliceIndex = arweave_lib_replica_2_9:get_slice_index(BucketEndOffset),
    shift_entropy_offset(BucketEndOffset, -SliceIndex).


shift_entropy_offset(Offset, SectorCount) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    arweave_lib_constants:get_chunk_bucket_end(Offset + SectorSize * SectorCount).


%% @doc Returns a list of 32x 8 MiB entropies. These entropies will need to be sliced
%% and recombined before they can be used. When properly recombined they contain enough
%% entropy to cover 1024 chunks. The chunks covered (aka the "footprint") are distributed
%% throughout the partition
-spec generate_entropies(StoreID :: arweave_storage_module:store_id(),
                         RewardAddr :: ar_wallet:address(),
                         BucketEndOffset :: non_neg_integer(),
                         ReplyTo :: pid()) ->
          ok.

generate_entropies(StoreID, RewardAddr, BucketEndOffset, ReplyTo) ->
    gen_server:cast(name(StoreID), {generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}).


-spec generate_entropies(RewardAddr :: ar_wallet:address(),
                         BucketEndOffset :: non_neg_integer()) ->
          [binary()] | {error, term()}.

generate_entropies(RewardAddr, BucketEndOffset) ->
    generate_entropies(RewardAddr, BucketEndOffset, true).


-spec generate_entropies(RewardAddr :: ar_wallet:address(),
                         BucketEndOffset :: non_neg_integer(),
                         CacheEntropy :: boolean()) ->
          [binary()] | {error, term()}.

generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy) ->
    prometheus_histogram:observe_duration(replica_2_9_entropy_duration_milliseconds, [],
                                          fun() ->
                                                  do_generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy)
                                          end).


map_entropies(_Entropies,
              [],
              _RangeStart,
              _Keys,
              _RewardAddr,
              _Fun,
              _Args,
              Acc) ->
    %% The amount of entropy generated per partition is slightly more than the amount needed.
    %% So at the end of a partition we will have finished processing chunks, but still have
    %% some entropy left. In this case we stop the recursion early and wait for the writes
    %% to complete.
    Acc;
map_entropies(Entropies,
              [BucketEndOffset | EntropyOffsets],
              RangeStart,
              Keys,
              RewardAddr,
              Fun,
              Args,
              Acc) ->

    case take_and_combine_entropy_slices(Entropies) of
        {ChunkEntropy, Rest} ->
            %% Sanity checks
            sanity_check_replica_2_9_entropy_keys(BucketEndOffset, RewardAddr, Keys),
            %% End sanity checks

            Acc2 = case BucketEndOffset > RangeStart of
                       true ->
                           erlang:apply(Fun,
                                        [ChunkEntropy, BucketEndOffset, RewardAddr] ++ Args ++ [Acc]);
                       false ->
                           %% Don't write entropy before the start of the range.
                           Acc
                   end,

            %% Jump to the next sector covered by this entropy.
            map_entropies(
              Rest,
              EntropyOffsets,
              RangeStart,
              Keys,
              RewardAddr,
              Fun,
              Args,
              Acc2)
    end.



init({StoreID, Packing}) ->
    ?LOG_INFO([{event, ar_entropy_gen_init},
               {name, name(StoreID)}, {store_id, StoreID},
               {packing, ?DEP(serialize):encode_packing(Packing, true)}]),

    ConfiguredPacking = arweave_storage_module:get_packing(StoreID),
    %% Sanity checks
    true = is_entropy_packing(ConfiguredPacking) orelse is_entropy_packing(Packing),
    %% End sanity checks

    {ModuleStart, ModuleEnd} = arweave_storage_module:get_range(StoreID),
    PaddedRangeEnd = arweave_lib_constants:get_chunk_bucket_end(ModuleEnd),

    %% Provided Packing will only differ from the StoreID packing when this
    %% module is configured to repack in place.
    IsRepackInPlace = Packing /= ConfiguredPacking,
    State = case IsRepackInPlace of
                true ->
                    #state{};
                false ->
                    %% Only kick of the prepare entropy process if we're not repacking in place.
                    Cursor = read_cursor(StoreID, ModuleStart),
                    ?LOG_INFO([{event, read_prepare_replica_2_9_cursor}, {store_id, StoreID},
                               {cursor, Cursor}, {module_start, ModuleStart},
                               {module_end, ModuleEnd}, {padded_range_end, PaddedRangeEnd}]),
                    PrepareStatus =
                        case initialize_context(StoreID, Packing) of
                            {_IsPrepared, none} ->
                                %% ar_entropy_gen is only used for replica_2_9 packing
                                ?LOG_ERROR([{event, invalid_packing_for_entropy}, {module, ?MODULE},
                                            {store_id, StoreID},
                                            {packing, ?DEP(serialize):encode_packing(Packing, true)}]),
                                off;
                            {false, _} ->
                                gen_server:cast(self(), prepare_entropy),
                                paused;
                            {true, _} ->
                                %% Entropy generation is complete
                                complete
                        end,
                    ar_device_lock:set_device_lock_metric(StoreID, prepare, PrepareStatus),
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
               footprint_limit = ar_footprint_limit:get(StoreID)
              },

    {ok, State2}.


handle_cast(prepare_entropy, State) ->
    #state{ store_id = StoreID } = State,
    NewStatus = ar_device_lock:acquire_lock(prepare, StoreID, State#state.prepare_status),
    State2 = State#state{ prepare_status = NewStatus },
    State3 = case NewStatus of
                 active ->
                     do_prepare_entropy(State2);
                 paused ->
                     ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), prepare_entropy),
                     State2;
                 _ ->
                     State2
             end,
    {noreply, State3};

handle_cast({generate_entropies, RewardAddr, BucketEndOffset, ReplyTo}, State) ->
    Entropies = generate_entropies(RewardAddr, BucketEndOffset),
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
    ?LOG_INFO([{event, terminate},
               {module, ?MODULE},
               {reason, Reason},
               {name, name(State#state.store_id)},
               {store_id, State#state.store_id}]),
    ok.


do_prepare_entropy(State) ->
    #state{
       cursor = Start, module_start = ModuleStart, module_end = ModuleEnd,
       packing = Packing,
       store_id = StoreID
      } = State,

    {replica_2_9, RewardAddr} = Packing,
    BucketEndOffset = arweave_lib_constants:get_chunk_bucket_end(Start),

    %% Sanity checks:
    BucketEndOffset = arweave_lib_constants:get_chunk_bucket_end(BucketEndOffset),
    true = (
      arweave_lib_constants:get_chunk_bucket_start(Start) ==
          arweave_lib_constants:get_chunk_bucket_start(BucketEndOffset)
     ),
    true = (
      max(0, BucketEndOffset - ?DATA_CHUNK_SIZE) ==
          arweave_lib_constants:get_chunk_bucket_start(BucketEndOffset)
     ),
    %% End of sanity checks.

    %% Make sure all prior entropy writes are complete.
    arweave_storage_entropy_storage:is_ready(StoreID),

    %% A bucket that is not to be prepared, or a failed entropy generation,
    %% is the outcome itself.
    Outcome =
        maybe
            prepare ?= classify_bucket(BucketEndOffset, State),
            %% Every entropy needed to encipher the chunk at BucketEndOffset.
            [_ | _] = Entropies ?=
                generate_entropies(RewardAddr, BucketEndOffset, false),
            EntropyKeys = generate_entropy_keys(RewardAddr, BucketEndOffset),
            EntropyOffsets = entropy_offsets(BucketEndOffset, ModuleEnd),
            arweave_storage:store_entropy_footprint(StoreID, fun(Write, Acc) -> map_entropies(Entropies, EntropyOffsets, ModuleStart, EntropyKeys, RewardAddr, Write, [StoreID], Acc) end)
        end,
    case Outcome of
        complete ->
            ar_device_lock:release_lock(prepare, StoreID),
            ?LOG_INFO([{event, storage_module_entropy_preparation_complete},
                       {store_id, StoreID}]),
            ar:console("The storage module ~s is prepared for 2.9 "
                       "replication.~n", [StoreID]),
            arweave_storage_chunk_storage:set_entropy_complete(StoreID),
            ar_device_lock:set_device_lock_metric(StoreID, prepare, complete),
            State#state{ prepare_status = complete };
        recorded ->
            gen_server:cast(self(), prepare_entropy),
            NextCursor = advance_entropy_offset(BucketEndOffset, Packing,
                                                StoreID),
            State#state{ cursor = NextCursor };
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
            State#state{ cursor = NextCursor };
        {error, Error} ->
            ?LOG_WARNING([{event, failed_to_store_entropy},
                          {cursor, Start},
                          {store_id, StoreID},
                          {reason, io_lib:format("~p", [Error])}]),
            ar_util:cast_after(500, self(), prepare_entropy),
            State;
        ok ->
            NextCursor = advance_entropy_offset(BucketEndOffset, Packing,
                                                StoreID),
            gen_server:cast(self(), prepare_entropy),
            store_prepare_cursor(NextCursor, StoreID),
            State#state{ cursor = NextCursor }
    end.


%% @doc What to do with the bucket: nothing more past the module end, move
%% past the rest of a sector beyond the footprint limit, skip a footprint
%% whose entropy is recorded, or prepare it.
classify_bucket(BucketEndOffset, #state{ module_end = ModuleEnd })
        when BucketEndOffset > ModuleEnd ->
    complete;
classify_bucket(BucketEndOffset, State) ->
    #state{ footprint_limit = Limit, packing = Packing,
            store_id = StoreID } = State,
    case ar_footprint_limit:is_beyond(BucketEndOffset, Limit) of
        true ->
            beyond_limit;
        false ->
            case arweave_storage_entropy_storage:is_entropy_recorded(BucketEndOffset,
                                                        Packing, StoreID) of
                true -> recorded;
                false -> prepare
            end
    end.


store_prepare_cursor(Cursor, StoreID) ->
    case store_cursor(Cursor, StoreID) of
        ok ->
            ok;
        {error, Error} ->
            ?LOG_WARNING([{event, failed_to_store_prepare_entropy_cursor},
                          {chunk_cursor, Cursor},
                          {store_id, StoreID},
                          {reason, io_lib:format("~p", [Error])}])
    end.




do_generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy) ->
    SubChunkSize = ?SUB_CHUNK_SIZE,
    EntropyTasks =
        lists:map(
          fun(Offset) ->
                  Ref = make_ref(),
                  ?DEP(packing):request_entropy_generation(
                    Ref, self(), {RewardAddr, BucketEndOffset, Offset, CacheEntropy}),
                  Ref
          end,
          lists:seq(0, ?DATA_CHUNK_SIZE - SubChunkSize, SubChunkSize)),
    Entropies = collect_entropies(EntropyTasks, []),
    case Entropies of
        {error, _Reason} ->
            flush_entropy_messages();
        _ ->
            ok
    end,
    Entropies.


%% @doc Take the first slice of each entropy and combine into a single binary. This binary
%% can be used to encipher a single chunk.
-spec take_and_combine_entropy_slices(Entropies :: [binary()]) ->
          {ChunkEntropy :: binary(),
           RemainingSlicesOfEachEntropy :: [binary()]}.

take_and_combine_entropy_slices(Entropies) ->
    true = ?SUB_CHUNK_COUNT == length(Entropies),
    take_and_combine_entropy_slices(Entropies, [], []).


take_and_combine_entropy_slices([], Acc, RestAcc) ->
    {iolist_to_binary(Acc), lists:reverse(RestAcc)};
take_and_combine_entropy_slices([<<>> | Entropies], _Acc, _RestAcc) ->
    true = lists:all(fun(Entropy) -> Entropy == <<>> end, Entropies),
    {<<>>, []};
take_and_combine_entropy_slices([<<EntropySlice:?SUB_CHUNK_SIZE/binary,
                                   Rest/binary>>
                                | Entropies],
                                Acc,
                                RestAcc) ->
    take_and_combine_entropy_slices(Entropies, [Acc, EntropySlice], [Rest | RestAcc]).


sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, Keys) ->
    sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, 0, Keys).


sanity_check_replica_2_9_entropy_keys(
  _PaddedEndOffset, _RewardAddr, _SubChunkStartOffset, []) ->
    ok;
sanity_check_replica_2_9_entropy_keys(
  PaddedEndOffset, RewardAddr, SubChunkStartOffset, [Key | Keys]) ->
    Key = arweave_lib_replica_2_9:get_entropy_key(RewardAddr, PaddedEndOffset, SubChunkStartOffset),
    SubChunkSize = ?SUB_CHUNK_SIZE,
    sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
                                          RewardAddr,
                                          SubChunkStartOffset + SubChunkSize,
                                          Keys).


advance_entropy_offset(BucketEndOffset, Packing, StoreID) ->
    case arweave_storage_entropy_storage:get_next_unsynced_interval(BucketEndOffset, Packing, StoreID) of
        not_found ->
            BucketEndOffset + ?DATA_CHUNK_SIZE;
        {_, Start} ->
            Start + ?DATA_CHUNK_SIZE
    end.


generate_entropy_keys(RewardAddr, Offset) ->
    generate_entropy_keys(RewardAddr, Offset, 0).


generate_entropy_keys(_RewardAddr, _Offset, SubChunkStart)
  when SubChunkStart == ?DATA_CHUNK_SIZE ->
    [];
generate_entropy_keys(RewardAddr, Offset, SubChunkStart) ->
    SubChunkSize = ?SUB_CHUNK_SIZE,
    [arweave_lib_replica_2_9:get_entropy_key(RewardAddr, Offset, SubChunkStart)
    | generate_entropy_keys(RewardAddr, Offset, SubChunkStart + SubChunkSize)].


collect_entropies([], Acc) ->
    lists:reverse(Acc);
collect_entropies([Ref | Rest], Acc) ->
    receive
        {entropy_generated, Ref, Entropy} ->
            collect_entropies(Rest, [Entropy | Acc])
    after 600_000 ->
            ?LOG_ERROR([{event, entropy_generation_timeout}, {ref, Ref}]),
            {error, timeout}
    end.


flush_entropy_messages() ->
    ?LOG_INFO([{event, flush_entropy_messages}]),
    receive
        {entropy_generated, _, _} ->
            flush_entropy_messages()
    after 0 ->
            ok
    end.


read_cursor(StoreID, ModuleStart) ->
    Filepath = arweave_storage_chunk_storage:get_filepath("prepare_replica_2_9_cursor", StoreID),
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


store_cursor(Cursor, StoreID) ->
    Filepath = arweave_storage_chunk_storage:get_filepath("prepare_replica_2_9_cursor", StoreID),
    file:write_file(Filepath, term_to_binary(Cursor)).


