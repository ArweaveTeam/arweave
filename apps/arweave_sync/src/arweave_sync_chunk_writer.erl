%%% @doc One process per store that checks fetched chunks and hands them to
%%% the store's ar_data_sync process for storage. arweave_sync_runtime_sup
%%% starts one chunk writer per store, under the name name/1.
%%%
%%% Each fetched chunk goes through:
%%% 1. Admit (the fetch worker calls store_fetched_chunk/6): move the chunk's
%%%    cache reference to the chunk writer and queue the chunk for
%%%    validation.
%%% 2. Validate (process_request/3): check the proof the peer sent. A packed
%%%    chunk first goes to the packing server; the chunk ID check then runs on
%%%    the unpacked chunk.
%%% 3. Write (process_valid_fetched_chunk/4): tell the scheduler the chunk is
%%%    unpacked, which frees the footprint's entropy slot. Then add the chunk
%%%    to the disk pool if it ends at or above the disk pool threshold, or
%%%    otherwise hand it to the store's ar_data_sync process.
%%% 4. Finish (finish/3): release the chunk's cache reference and tell the
%%%    scheduler whether the write completed or failed.
%%%
%%% The chunk writer finds the store's ar_data_sync process by name for each
%%% write and monitors it. If that process dies, the writes waiting on it fail,
%%% the scheduler releases their claims, and a later sweep finds the chunks
%%% again.
%%%
%%% The chunk writer's state:
%%% - data_sync: the store's ar_data_sync process, once monitored;
%%% - requests: each chunk in progress, by chunk cache reference, with the
%%%   chunk's task and phase: validating, unpacking, or storing with the
%%%   ar_data_sync process that holds the write;
%%% - unpacking: the chunk offsets with an unpack in progress.
-module(arweave_sync_chunk_writer).
-behaviour(gen_server).
-export([name/1, register_workers/0, start_link/1, store_fetched_chunk/6]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

-record(state, {store_id, data_sync, requests = #{}, unpacking = #{}}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(StoreID) ->
    gen_server:start_link({local, name(StoreID)}, ?MODULE, StoreID, []).

name(?DEFAULT_MODULE) ->
    arweave_sync_chunk_writer_default;
name(StoreID) when is_atom(StoreID) ->
    list_to_atom("arweave_sync_chunk_writer_" ++ atom_to_list(StoreID));
name(StoreID) ->
    #store_info{label = Label} = ?DEP(storage):store_info(StoreID),
    list_to_atom("arweave_sync_chunk_writer_" ++ Label).

%% @doc Return the child specs for one chunk writer per store: the default
%% store and every swept store.
register_workers() ->
    [?CHILD_WITH_ARGS(?MODULE, worker, name(StoreID), [StoreID])
     || StoreID <- [?DEFAULT_MODULE | arweave_sync_sweeper:store_ids()]].

%%%===================================================================
%%% Public interface: admit.
%%%===================================================================

%% @doc Move a fetched chunk and its cache reference to the store's chunk
%% writer.
store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef, CacheRef) ->
    case whereis(name(StoreID)) of
        PID when is_pid(PID) ->
            case ?DEP(chunk_cache):transfer(CacheRef, PID) of
                {ok, OwnedCacheRef} ->
                    gen_server:cast(
                        PID, {admit, {fetched, Peer, Byte, Proof}, TaskRef, OwnedCacheRef}
                    );
                {error, expired} ->
                    failed(TaskRef)
            end;
        undefined ->
            ?DEP(chunk_cache):release(CacheRef),
            failed(TaskRef)
    end.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    {ok, #state{store_id = StoreID}}.

handle_call(ping, _From, State) -> {reply, pong, State}.

handle_cast({admit, Request, TaskRef, Ref}, State) ->
    ?DEP(chunk_cache):mark_cached(Ref),
    Requests = State#state.requests,
    State2 = State#state{requests = Requests#{Ref => {TaskRef, validating}}},
    {noreply, process_request(Ref, Request, State2)};
handle_cast({expire, unpack, Ref}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, _, _}} ->
            {noreply, finish(Ref, {error, unpack_timeout}, State)};
        _ ->
            {noreply, State}
    end.

handle_info({chunk, Result, CacheRef}, State) ->
    try
        handle_info({chunk, Result}, State)
    after
        ?DEP(chunk_cache):release(CacheRef)
    end;
handle_info({retry_unpack, Ref}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, ChunkArgs, _}} -> request_unpack(Ref, ChunkArgs);
        _ -> ok
    end,
    {noreply, State};
handle_info({unpack_rejected, Ref, Reason}, State) ->
    {noreply, finish(Ref, {error, Reason}, State)};
handle_info({chunk_store_result, Ref, Result}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {storing, _}} -> {noreply, finish(Ref, Result, State)};
        _ -> {noreply, State}
    end;
handle_info({'DOWN', _MonitorRef, process, DataSync, _Reason}, State) ->
    {noreply, data_sync_down(DataSync, State)};
handle_info({chunk, {unpacked, Ref, ChunkArgs}}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, _, Args}} ->
            {_, Chunk, _, _, ChunkSize} = ChunkArgs,
            ChunkID = element(7, Args),
            case
                ?DEP(data_sync):validate_chunk_id_size(
                    Chunk, ChunkID, ChunkSize)
            of
                true ->
                    {noreply,
                        process_valid_fetched_chunk(
                            Ref,
                            ChunkArgs,
                            Args,
                            clear_unpacking(Ref, State)
                        )};
                false ->
                    {noreply,
                        reject(
                            Ref,
                            got_invalid_proof_from_peer,
                            element(9, Args),
                            element(10, Args),
                            State
                        )}
            end;
        _ ->
            {noreply, State}
    end;
handle_info({chunk, {unpack_error, Ref, _, Error}}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, _, Args}} ->
            ?DEP(peers):issue_warning(element(9, Args), chunk, Error),
            {noreply,
                reject(
                    Ref,
                    got_invalid_packed_chunk,
                    element(9, Args),
                    element(10, Args),
                    State
                )};
        _ ->
            {noreply, State}
    end.

%%%===================================================================
%%% Validate.
%%%===================================================================

process_request(Ref, {fetched, Peer, Byte, Proof}, State) ->
    case ?DEP(data_sync):validate_fetched_chunk(Peer, Byte, Proof) of
        false ->
            reject(Ref, got_invalid_proof_from_peer, Peer, Byte, State);
        {valid, ChunkArgs, Args} ->
            process_valid_fetched_chunk(Ref, ChunkArgs, Args, State);
        {needs_unpacking, ChunkArgs, Args} ->
            Offset = element(3, ChunkArgs),
            Unpacking = State#state.unpacking,
            case maps:is_key(Offset, Unpacking) of
                true ->
                    finish(Ref, {skipped, chunk_already_being_unpacked}, State);
                false ->
                    request_unpack(Ref, ChunkArgs),
                    set_phase(
                        Ref,
                        {unpacking, ChunkArgs, Args},
                        State#state{unpacking = Unpacking#{Offset => Ref}}
                    )
            end
    end.

request_unpack(Ref, ChunkArgs) ->
    case ?DEP(packing):request_unpack(Ref, self(), ChunkArgs, Ref) of
        busy -> erlang:send_after(1000, self(), {retry_unpack, Ref});
        {error, Reason} -> self() ! {unpack_rejected, Ref, Reason};
        ok -> ok
    end.

reject(Ref, Reason, Peer, Byte, #state{store_id = StoreID} = State) ->
    ?LOG_WARNING([
        {event, skipping_synced_chunk},
        {reason, Reason},
        {peer, arweave_lib_util:format_peer(Peer)},
        {byte, Byte},
        {store_id, StoreID}
    ]),
    finish(Ref, {error, Reason}, State).

%%%===================================================================
%%% Write.
%%%===================================================================

process_valid_fetched_chunk(Ref, ChunkArgs, Args, State) ->
    #state{store_id = StoreID, requests = Requests} = State,
    %% The chunk is unpacked, or never needed unpacking, so its footprint's
    %% entropy slot can go to the next footprint while the write continues.
    {TaskRef, _} = maps:get(Ref, Requests),
    arweave_sync_scheduler:report_unpacked(TaskRef),
    {Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
    {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID, ChunkEndOffset,
        Peer, Byte} = Args,
    maybe
        true ?= ?DEP(data_sync):is_chunk_proof_ratio_attractive(
                ChunkSize, TXSize, DataPath)
            orelse {skipped, got_too_big_proof_from_peer},
        false ?= ?DEP(storage):is_recorded(
                Byte + 1, any_packing, {ar_data_sync, byte}, StoreID)
            =/= false,
        true = AbsoluteEndOffset == AbsoluteTXStartOffset + ChunkEndOffset,
        case AbsoluteEndOffset >= ?DEP(disk_pool):get_threshold() of
            true ->
                Result = ?DEP(disk_pool):add_chunk(
                    DataRoot,
                    DataPath,
                    UnpackedChunk,
                    ChunkEndOffset - 1,
                    TXSize,
                    Peer
                ),
                finish(Ref, disk_pool_result(Result), State);
            false ->
                write_chunk(
                    Ref,
                    {DataRoot, AbsoluteEndOffset, TXPath, TXRoot, DataPath, Packing, ChunkEndOffset,
                        ChunkSize, Chunk, UnpackedChunk, none, none},
                    State
                )
        end
    else
        {skipped, _} = Skipped -> finish(Ref, Skipped, State);
        true -> finish(Ref, {skipped, chunk_already_synced}, State)
    end.

disk_pool_result(ok) -> buffered;
disk_pool_result(temporary) -> buffered;
disk_pool_result({error, _} = Error) -> Error.

write_chunk(Ref, Args, #state{store_id = StoreID} = State) ->
    case whereis(?DEP(data_sync):name(StoreID)) of
        undefined ->
            finish(Ref, {error, ar_data_sync_not_running}, State);
        DataSync ->
            ?DEP(data_sync):store_chunk(DataSync, Args, {self(), Ref}, Ref),
            State2 = monitor_data_sync(DataSync, State),
            set_phase(Ref, {storing, DataSync}, State2)
    end.

monitor_data_sync(DataSync, #state{data_sync = DataSync} = State) ->
    State;
monitor_data_sync(DataSync, State) ->
    monitor(process, DataSync),
    State#state{data_sync = DataSync}.

%% @doc Fail the writes a dead ar_data_sync process held; they died with it.
data_sync_down(DataSync, #state{requests = Requests} = State) ->
    Waiting = [Ref || {Ref, {_, {storing, PID}}} <- maps:to_list(Requests),
        PID =:= DataSync],
    lists:foldl(
        fun(Ref, Acc) -> finish(Ref, {error, ar_data_sync_down}, Acc) end,
        State,
        Waiting
    ).

%%%===================================================================
%%% Finish.
%%%===================================================================

finish(Ref, Result, #state{store_id = StoreID, requests = Requests} = State) ->
    case maps:take(Ref, Requests) of
        error ->
            State;
        {{TaskRef, Phase}, Requests2} ->
            ?DEP(chunk_cache):release(Ref),
            case Result of
                stored ->
                    arweave_sync_scheduler:report_write_completed(StoreID, TaskRef);
                buffered ->
                    arweave_sync_scheduler:report_write_completed(StoreID, TaskRef);
                {_, Reason} ->
                    %% Backend errors can hold arbitrary terms, which must
                    %% not become metric labels.
                    Label =
                        case {Phase, Result} of
                            {{storing, _}, {error, _}} -> chunk_store_failed;
                            _ when is_atom(Reason) -> Reason;
                            _ -> chunk_processing_failed
                        end,
                    arweave_sync_metrics:count_chunk_skipped(Label),
                    failed(TaskRef)
            end,
            State2 = clear_unpacking(Ref, State),
            State2#state{requests = Requests2}
    end.

failed(undefined) -> ok;
failed(TaskRef) -> arweave_sync_scheduler:report_write_failed(TaskRef).

%%%===================================================================
%%% Request state.
%%%===================================================================

set_phase(Ref, Phase, #state{requests = Requests} = State) ->
    {TaskRef, _} = maps:get(Ref, Requests),
    State#state{requests = Requests#{Ref => {TaskRef, Phase}}}.

clear_unpacking(Ref, #state{unpacking = Unpacking} = State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, ChunkArgs, _}} ->
            State#state{unpacking = maps:remove(element(3, ChunkArgs), Unpacking)};
        _ ->
            State
    end.
