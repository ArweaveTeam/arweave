%%% @doc Per-store ownership of fetched-chunk validation and task completion.
-module(arweave_sync_ingest).
-behaviour(gen_server).
-export([start_link/2, store_fetched_chunk/6, writer/1, reset/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("arweave/include/ar.hrl").

-record(state, {store_id, writer, requests = #{}, unpacking = #{}}).

start_link(StoreID, Writer) ->
    gen_server:start_link(?MODULE, {StoreID, Writer}, []).

%% @doc Hand off a fetched chunk with its existing memory reservation.
store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef, CacheRef) ->
    case writer(StoreID) of
        {_, PID} ->
            case (arweave_sync_deps:chunk_cache()):transfer(CacheRef, PID) of
                {ok, OwnedCacheRef} ->
                    gen_server:cast(
                        PID, {admit, {fetched, Peer, Byte, Proof}, TaskRef, OwnedCacheRef}
                    );
                {error, expired} ->
                    failed(TaskRef)
            end;
        undefined ->
            (arweave_sync_deps:chunk_cache()):release(CacheRef),
            failed(TaskRef)
    end.

writer(StoreID) ->
    case ets:lookup(arweave_sync_state, {writer, StoreID}) of
        [{_, Binding}] -> Binding;
        [] -> undefined
    end.

%% @doc Cancel the stopped client's host work before reclaiming its reservations.
reset(StoreID) ->
    %% Stop network admissions before cancelling this client's host work.
    ok = arweave_sync_scheduler:reset_store(StoreID),
    case writer(StoreID) of
        {Writer, PID} ->
            case is_process_alive(Writer) of
                true ->
                    try
                        (arweave_sync_deps:data_sync()):cancel_store_requests(Writer, PID)
                    catch
                        exit:Reason:Stack ->
                            case is_process_alive(Writer) of
                                false -> ok;
                                true -> erlang:raise(exit, Reason, Stack)
                            end
                    end;
                false ->
                    ok
            end;
        undefined ->
            ok
    end,
    ets:delete(arweave_sync_state, {writer, StoreID}),
    ok.

init({StoreID, Writer}) ->
    ok = reset(StoreID),
    %% Publish cleanup identity before opening admissions; init may be interrupted.
    ets:insert(arweave_sync_state, {{writer, StoreID}, {Writer, self()}}),
    {ok, #state{store_id = StoreID, writer = Writer}}.

handle_call(ping, _From, State) -> {reply, pong, State}.

handle_cast({admit, Request, TaskRef, Ref}, State) ->
    (arweave_sync_deps:chunk_cache()):mark_cached(Ref),
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
        (arweave_sync_deps:chunk_cache()):release(CacheRef)
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
        {_, storing} -> {noreply, finish(Ref, Result, State)};
        _ -> {noreply, State}
    end;
handle_info({chunk, {unpacked, Ref, ChunkArgs}}, State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, _, Args}} ->
            {_, Chunk, _, _, ChunkSize} = ChunkArgs,
            ChunkID = element(7, Args),
            case
                (arweave_sync_deps:data_sync()):validate_chunk_id_size(Chunk, ChunkID, ChunkSize)
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
            (arweave_sync_deps:peers()):issue_warning(element(9, Args), chunk, Error),
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

process_request(Ref, {fetched, Peer, Byte, Proof}, State) ->
    case (arweave_sync_deps:data_sync()):validate_fetched_chunk(Peer, Byte, Proof) of
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
    case (arweave_sync_deps:packing()):request_unpack(Ref, self(), ChunkArgs, Ref) of
        busy -> erlang:send_after(1000, self(), {retry_unpack, Ref});
        {error, Reason} -> self() ! {unpack_rejected, Ref, Reason};
        ok -> ok
    end.

process_valid_fetched_chunk(Ref, ChunkArgs, Args, State) ->
    #state{store_id = StoreID} = State,
    {Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
    {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID, ChunkEndOffset,
        Peer, Byte} = Args,
    maybe
        true ?=
            (arweave_sync_deps:data_sync()):is_chunk_proof_ratio_attractive(
                ChunkSize, TXSize, DataPath
            ) orelse {skipped, got_too_big_proof_from_peer},
        false ?=
            (arweave_sync_deps:storage()):is_recorded(
                Byte + 1, any_packing, {ar_data_sync, byte}, StoreID
            ) =/= false,
        true = AbsoluteEndOffset == AbsoluteTXStartOffset + ChunkEndOffset,
        case AbsoluteEndOffset >= (arweave_sync_deps:disk_pool()):get_threshold() of
            true ->
                Result = (arweave_sync_deps:disk_pool()):add_chunk(
                    DataRoot,
                    DataPath,
                    UnpackedChunk,
                    ChunkEndOffset - 1,
                    TXSize,
                    Peer
                ),
                finish(Ref, disk_pool_result(Result), State);
            false ->
                handoff(
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

handoff(Ref, Args, #state{writer = Writer} = State) ->
    (arweave_sync_deps:data_sync()):store_chunk(Writer, Args, {self(), Ref}, Ref),
    set_phase(Ref, storing, State).

set_phase(Ref, Phase, #state{requests = Requests} = State) ->
    {TaskRef, _} = maps:get(Ref, Requests),
    State#state{requests = Requests#{Ref => {TaskRef, Phase}}}.

finish(Ref, Result, #state{store_id = StoreID, requests = Requests} = State) ->
    case maps:take(Ref, Requests) of
        error ->
            State;
        {{TaskRef, Phase}, Requests2} ->
            (arweave_sync_deps:chunk_cache()):release(Ref),
            case Result of
                stored ->
                    arweave_sync_scheduler:task_write_completed(StoreID, TaskRef);
                buffered ->
                    arweave_sync_scheduler:task_write_completed(StoreID, TaskRef);
                {_, Reason} ->
                    %% Backend errors can contain arbitrary data, not metric labels.
                    Label =
                        case {Phase, Result} of
                            {storing, {error, _}} -> chunk_store_failed;
                            _ when is_atom(Reason) -> Reason;
                            _ -> chunk_processing_failed
                        end,
                    arweave_metrics:counter_inc(sync_chunks_skipped, [Label]),
                    failed(TaskRef)
            end,
            State2 = clear_unpacking(Ref, State),
            State2#state{requests = Requests2}
    end.

clear_unpacking(Ref, #state{unpacking = Unpacking} = State) ->
    case maps:get(Ref, State#state.requests, undefined) of
        {_, {unpacking, ChunkArgs, _}} ->
            State#state{unpacking = maps:remove(element(3, ChunkArgs), Unpacking)};
        _ ->
            State
    end.

reject(Ref, Reason, Peer, Byte, #state{store_id = StoreID} = State) ->
    ?LOG_WARNING([
        {event, skipping_synced_chunk},
        {reason, Reason},
        {peer, arweave_util:format_peer(Peer)},
        {byte, Byte},
        {store_id, StoreID}
    ]),
    finish(Ref, {error, Reason}, State).

failed(undefined) -> ok;
failed(TaskRef) -> arweave_sync_scheduler:task_write_failed(TaskRef).
