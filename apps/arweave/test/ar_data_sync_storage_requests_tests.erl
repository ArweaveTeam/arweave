-module(ar_data_sync_storage_requests_tests).
-test_category([fast]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
-include("ar_data_sync.hrl").

%% @doc PID and named-writer handoffs account for each target store separately.
local_handoffs_reserve_each_target_store_test_() ->
    {timeout, 30, fun local_handoffs_reserve_each_target_store/0}.

%% @doc Each local write retains its own cache reference until its writer exits.
local_handoffs_reserve_each_target_store() ->
    ReservedBefore = ar_chunk_cache:reserved_size(),
    FirstPID = start_local_writer(),
    SecondPID = start_local_writer(),
    try
        %% Exercise both PID and registered-name storage handoffs.
        SecondName = ar_data_sync_test_writer,
        true = register(SecondName, SecondPID),
        local_store(FirstPID, store1, first_chunk),
        local_store(SecondName, store2, second_chunk),
        ?assertEqual(ReservedBefore + 2, ar_chunk_cache:reserved_size()),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store2)),
        ?assertMatch({store_chunk, first_chunk, _}, take_local_chunk(FirstPID)),
        ?assertMatch(
            {store_chunk, second_chunk, _},
            take_local_chunk(SecondPID)
        )
    after
        stop_local_writer(FirstPID),
        stop_local_writer(SecondPID)
    end,
    ?assertEqual(
        ok,
        ar_test_await:until(local_writer_cache_released, fun() ->
            ar_chunk_cache:reserved_size() =:= ReservedBefore
        end)
    ).

%% @doc Storage requests return not_initialized for a missing named or undefined
%% writer.
unregistered_local_writer_test() ->
    Name = ar_data_sync_missing_writer,
    ReplyTo = {none, make_ref()},
    CacheRef = make_ref(),
    ?assertEqual(undefined, whereis(Name)),
    ?assertEqual(
        {error, not_initialized},
        ar_data_sync:store_chunk(Name, chunk, ReplyTo, CacheRef)
    ),
    ?assertEqual(
        {error, not_initialized},
        ar_data_sync:store_chunk(undefined, chunk, ReplyTo, CacheRef)
    ).

%% @doc Skipped and rejected requests release their reservation at admission.
store_request_terminal_outcomes_test_() ->
    with_storage(fun store_request_terminal_outcomes/0).

%% @doc Packing expiry and failure release storage and message references.
packing_completion_releases_cache_test_() ->
    with_storage(fun packing_completion_releases_cache/0).

%% @doc Each drained write releases its own reference, preserving pending work.
queued_writes_release_cache_test_() ->
    with_storage(fun queued_writes_release_cache/0, [
        {arweave_storage, is_storage_supported, fun(_, _, _) -> true end},
        {arweave_storage, delete_footprint, fun(_, _) -> ok end},
        {arweave_storage, delete_sync_record, fun(_, _, _, _) -> ok end},
        {arweave_storage, is_recorded, fun(_, _, _, _) -> true end},
        {arweave_storage, add_sync_record, fun(_, _, _, _, _) -> ok end},
        {arweave_storage, add_footprint, fun(_, _, _) -> ok end},
        {arweave_storage, put_chunk, fun
            (?DATA_CHUNK_SIZE, _, Packing, _) ->
                {ok, Packing};
            (Offset, _, _, _) when Offset =:= 2 * ?DATA_CHUNK_SIZE ->
                already_stored;
            (_, _, _, _) ->
                {error, test_write_failed}
        end},
        {ar_tx_blacklist, is_byte_blacklisted, fun(_) -> false end},
        {ar_kv, put, fun(_, _, _) -> ok end}
    ]).

%% @doc Cancelling a client's delayed writes drops their payloads and ignores
%% late retries.
cancel_retry_payloads_test_() ->
    with_storage(fun cancel_retry_payloads/0).

%% @doc Old packing replies and expiry messages cannot complete a replacement
%% request.
late_packing_result_cannot_complete_replacement_test_() ->
    with_storage(fun late_packing_result_cannot_complete_replacement/0).

%% @doc Cancelling one client's writes preserves other clients' packing and
%% queued work.
cancel_only_requesting_client_test_() ->
    with_storage(fun cancel_only_requesting_client/0).

%% @doc Missing entropy is generated before retrying a write; failed generation
%% stops the retry.
missing_entropy_repair_test_() ->
    %% A full chunk above the test split threshold can use chunk storage.
    Offset = 4 * ?DATA_CHUNK_SIZE,
    Chunk = binary:copy(<<42>>, ?DATA_CHUNK_SIZE),
    Entropy = binary:copy(<<7>>, ?DATA_CHUNK_SIZE),
    RewardAddr = <<0:256>>,
    Packing = {replica_2_9, RewardAddr},
    StoreID = test_store,
    ar_test_util:with_mocked(
        [
            {ar_tx_blacklist, is_byte_blacklisted, fun(_) -> false end},
            {arweave_storage, is_storage_supported, fun(_, _, _) -> true end},
            {arweave_storage, put_chunk, fun
                (_, Data, unpacked_padded, _) when is_binary(Data) ->
                    {error, {missing_entropy, RewardAddr}};
                (_, {chunk_with_entropy, _, _}, unpacked_padded, _) ->
                    {ok, Packing}
            end},
            {arweave_entropy, generate_chunk, fun(_, _) -> Entropy end}
        ],
        fun() ->
            Metadata = #chunk_metadata{
                chunk_size = ?DATA_CHUNK_SIZE,
                data_path = none
            },
            ?assertEqual(
                {ok, Packing},
                ar_data_sync:write_chunk(
                    Offset, Metadata, Chunk, unpacked_padded, StoreID
                )
            ),
            ?assert(
                meck:called(
                    arweave_entropy,
                    generate_chunk,
                    [Offset, RewardAddr]
                )
            ),
            ?assert(
                meck:called(
                    arweave_storage,
                    put_chunk,
                    [
                        Offset,
                        {chunk_with_entropy, Chunk, Entropy},
                        unpacked_padded,
                        StoreID
                    ]
                )
            ),
            ?assertEqual(2, meck:num_calls(arweave_storage, put_chunk, '_')),
            %% A failed repair is returned without a retry or a write of bad data.
            meck:reset(arweave_storage),
            meck:expect(
                arweave_entropy,
                generate_chunk,
                fun(_, _) -> {error, timeout} end
            ),
            ?assertEqual(
                {error, timeout},
                ar_data_sync:write_chunk(
                    Offset, Metadata, Chunk, unpacked_padded, StoreID
                )
            ),
            ?assertEqual(1, meck:num_calls(arweave_storage, put_chunk, '_'))
        end
    ).

%% @doc Cancelling a client's delayed writes drops their payloads and ignores
%% late retries.
cancel_retry_payloads() ->
    %% An uninitialized store must defer payloads without posting them again.
    State = #data_sync_state{
        store_id = uninitialized_store, footprint_limit = 2
    },
    {ok, CacheRef} = ar_chunk_cache:reserve(?MODULE),
    ReplyTo = {self(), make_ref(), CacheRef},
    {noreply, Waiting} = ar_data_sync:handle_cast(
        {store_chunk, chunk_args(), ReplyTo}, State
    ),
    [RetryRef] = maps:keys(Waiting#data_sync_state.packing_map),
    ?assert(is_reference(RetryRef)),
    ?assertEqual(active, cache_reference_status(CacheRef)),
    ?assertEqual({retry_store_chunk, RetryRef}, take_timer()),
    {reply, ok, Cancelled} = ar_data_sync:handle_call(
        {cancel_store_requests, self()}, unused, Waiting
    ),
    ?assertEqual(#{}, Cancelled#data_sync_state.packing_map),
    ?assertEqual({error, expired}, cache_reference_status(CacheRef)),
    ?assertEqual(
        {noreply, Cancelled},
        ar_data_sync:handle_cast({retry_store_chunk, RetryRef}, Cancelled)
    ).

%% @doc Skipped chunks and rejected packing requests notify the client and
%% release their cache references.
store_request_terminal_outcomes() ->
    State = #data_sync_state{store_id = ?DEFAULT_MODULE, footprint_limit = 2},
    {ok, SkippedCacheRef} = ar_chunk_cache:reserve(?MODULE),
    SkippedRef = make_ref(),
    %% One chunk past the mocked two-chunk mature-data threshold is skipped.
    Args = setelement(2, chunk_args(), 3 * ?DATA_CHUNK_SIZE),
    ?assertEqual(
        {noreply, State},
        ar_data_sync:handle_cast(
            {store_chunk, Args, {self(), SkippedRef, SkippedCacheRef}}, State
        )
    ),
    ?assertEqual(
        {skipped, chunk_is_above_disk_pool_threshold},
        take_store_result(SkippedRef)
    ),
    ?assertEqual({error, expired}, cache_reference_status(SkippedCacheRef)),
    meck:expect(
        ar_packing_server,
        request_repack,
        fun(_, _, _, _) -> {error, test_packing_failed} end
    ),
    {ok, FailedCacheRef} = ar_chunk_cache:reserve(?MODULE),
    FailedRef = make_ref(),
    ?assertEqual(
        {noreply, State},
        ar_data_sync:handle_cast(
            {store_chunk, chunk_args(), {self(), FailedRef, FailedCacheRef}},
            State
        )
    ),
    ?assertEqual({error, test_packing_failed}, take_store_result(FailedRef)),
    ?assertEqual({error, expired}, cache_reference_status(FailedCacheRef)).

%% @doc Packing timeout, failure and late replies release the appropriate cache
%% references without sending duplicate results.
packing_completion_releases_cache() ->
    State = #data_sync_state{store_id = ?DEFAULT_MODULE, footprint_limit = 2},
    {ok, CacheRef} = ar_chunk_cache:reserve(?MODULE),
    Ref = make_ref(),
    {noreply, Packing} = ar_data_sync:handle_cast(
        {store_chunk, chunk_args(), {self(), Ref, CacheRef}}, State
    ),
    Key = take_packing_request(),
    ?assertEqual(active, cache_reference_status(CacheRef)),
    {ok, MessageCacheRef} = ar_chunk_cache:add_reference(CacheRef, self()),
    {noreply, Expired} = ar_data_sync:handle_cast(
        {expire, repack, Key}, Packing
    ),
    ?assertEqual({error, packing_timeout}, take_store_result(Ref)),
    ?assertEqual({error, expired}, cache_reference_status(CacheRef)),
    ?assertEqual(active, cache_reference_status(MessageCacheRef)),
    %% A late reply releases its message holder without replying a second time.
    ?assertEqual(
        {noreply, Expired},
        ar_data_sync:handle_info(
            {chunk, {repack_error, Key, late_error}, MessageCacheRef}, Expired
        )
    ),
    ?assertEqual({error, expired}, cache_reference_status(MessageCacheRef)),
    ?assertEqual(no_result, take_store_result(Ref)),
    {ok, FailedCacheRef} = ar_chunk_cache:reserve(?MODULE),
    FailedRef = make_ref(),
    {noreply, Packing2} = ar_data_sync:handle_cast(
        {store_chunk, chunk_args(), {self(), FailedRef, FailedCacheRef}},
        Expired
    ),
    Key2 = take_packing_request(),
    {ok, MessageCacheRef2} = ar_chunk_cache:add_reference(
        FailedCacheRef, self()
    ),
    {noreply, Finished} = ar_data_sync:handle_info(
        {chunk, {repack_error, Key2, test_packing_failed}, MessageCacheRef2},
        Packing2
    ),
    ?assertEqual(#{}, Finished#data_sync_state.packing_map),
    ?assertEqual({error, test_packing_failed}, take_store_result(FailedRef)),
    ?assertEqual({error, expired}, cache_reference_status(FailedCacheRef)),
    ?assertEqual({error, expired}, cache_reference_status(MessageCacheRef2)).

%% @doc Drained writes release their references and count only successful writes,
%% while a pending write retains its reference until cancellation.
queued_writes_release_cache() ->
    %% Three aged entries cover success, already-stored and write-error outcomes.
    Reservations = [ar_chunk_cache:reserve(?MODULE) || _ <- lists:seq(1, 3)],
    ?assertMatch([{ok, _}, {ok, _}, {ok, _}], Reservations),
    Replies = [
        {self(), make_ref(), CacheRef}
     || {ok, CacheRef} <- Reservations
    ],
    Entries = lists:zipwith(
        fun(Index, ReplyTo) ->
            Offset = Index * ?DATA_CHUNK_SIZE,
            ChunkArgs = {unpacked, <<0>>, Offset, tx_root, ?DATA_CHUNK_SIZE},
            Args =
                {unpacked, <<0>>, ?DATA_CHUNK_SIZE, data_root, tx_path, none,
                    none},
            {Offset, 0, make_ref(), ChunkArgs, Args, ReplyTo}
        end,
        lists:seq(1, 3),
        Replies
    ),
    State = #data_sync_state{
        store_id = ?DEFAULT_MODULE,
        footprint_limit = 2,
        store_chunk_queue = gb_sets:from_list(Entries)
    },
    %% A fresh fourth entry must remain queued after the aged entries drain.
    meck:expect(ar_disk_pool, get_threshold, fun() -> 4 * ?DATA_CHUNK_SIZE end),
    {ok, PendingCacheRef} = ar_chunk_cache:reserve(?MODULE),
    PendingRef = make_ref(),
    Args = setelement(
        6, setelement(2, chunk_args(), 4 * ?DATA_CHUNK_SIZE), unpacked
    ),
    Completed = ar_chunk_cache:completed(?DEFAULT_MODULE),
    {noreply, Pending} = ar_data_sync:handle_cast(
        {store_chunk, Args, {self(), PendingRef, PendingCacheRef}}, State
    ),
    ?assertEqual(1, gb_sets:size(Pending#data_sync_state.store_chunk_queue)),
    ?assertEqual(
        [stored, {skipped, already_stored}, {error, test_write_failed}],
        [take_store_result(Ref) || {_, Ref, _} <- Replies]
    ),
    ?assertEqual(
        lists:duplicate(3, {error, expired}),
        [cache_reference_status(CacheRef) || {ok, CacheRef} <- Reservations]
    ),
    %% Only the successful write contributes to the completed-chunk count.
    ?assertEqual(Completed + 1, ar_chunk_cache:completed(?DEFAULT_MODULE)),
    ?assertEqual(active, cache_reference_status(PendingCacheRef)),
    ?assertEqual(no_result, take_store_result(PendingRef)),
    {reply, ok, Cancelled} = ar_data_sync:handle_call(
        {cancel_store_requests, self()}, unused, Pending
    ),
    ?assert(gb_sets:is_empty(Cancelled#data_sync_state.store_chunk_queue)),
    ?assertEqual({error, cancelled}, take_store_result(PendingRef)),
    ?assertEqual({error, expired}, cache_reference_status(PendingCacheRef)).

%% @doc Old packing replies and expiry messages cannot complete a replacement
%% request.
late_packing_result_cannot_complete_replacement() ->
    State = #data_sync_state{store_id = ?DEFAULT_MODULE, footprint_limit = 2},
    {noreply, Packing} = ar_data_sync:handle_cast(
        {store_chunk, chunk_args(), {self(), make_ref(), undefined}}, State
    ),
    OldKey = take_packing_request(),
    {reply, ok, Cancelled} = ar_data_sync:handle_call(
        {cancel_store_requests, self()}, unused, Packing
    ),
    NewReplyTo = {self(), make_ref(), undefined},
    {noreply, Replacement} = ar_data_sync:handle_cast(
        {store_chunk, chunk_args(), NewReplyTo}, Cancelled
    ),
    NewKey = take_packing_request(),
    ?assertNotEqual(OldKey, NewKey),
    Chunk = {unpacked, <<0>>, ?DATA_CHUNK_SIZE, tx_root, ?DATA_CHUNK_SIZE},
    ?assertEqual(
        {noreply, Replacement},
        ar_data_sync:handle_info(
            {chunk, {packed, OldKey, Chunk}}, Replacement
        )
    ),
    ?assertEqual(
        {noreply, Replacement},
        ar_data_sync:handle_cast(
            {expire, repack, OldKey}, Replacement
        )
    ),
    {noreply, Queued} = ar_data_sync:handle_info(
        {chunk, {packed, NewKey, Chunk}}, Replacement
    ),
    %% A single chunk remains queued below the test profile's two-chunk flush.
    ?assertEqual(1, gb_sets:size(Queued#data_sync_state.store_chunk_queue)),
    ?assertEqual(#{}, Queued#data_sync_state.packing_map),
    [Entry] = gb_sets:to_list(Queued#data_sync_state.store_chunk_queue),
    ?assertEqual(NewReplyTo, element(6, Entry)).

%% @doc Cancelling one client's writes preserves other clients' packing and
%% queued work.
cancel_only_requesting_client() ->
    Other = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    try
        OwnReply = {self(), make_ref(), undefined},
        OtherReply = {Other, make_ref(), undefined},
        %% Each queue entry carries its client as an opaque sixth field.
        OwnEntry = {1, 0, make_ref(), chunk, args, OwnReply},
        OtherEntry = {2, 0, make_ref(), chunk, args, OtherReply},
        State = #data_sync_state{
            packing_map = #{
                own => {pack_chunk, args, OwnReply},
                other => {store_retry, args, OtherReply}
            },
            store_chunk_queue = gb_sets:from_list([OwnEntry, OtherEntry])
        },
        {reply, ok, Cancelled} = ar_data_sync:handle_call(
            {cancel_store_requests, self()}, unused, State
        ),
        ?assertEqual(
            #{other => {store_retry, args, OtherReply}},
            Cancelled#data_sync_state.packing_map
        ),
        ?assertEqual(
            [OtherEntry],
            gb_sets:to_list(
                Cancelled#data_sync_state.store_chunk_queue
            )
        )
    after
        Other ! stop
    end.

%% @doc Capture storage casts without starting a data-sync process.
start_local_writer() ->
    Parent = self(),
    spawn(fun() -> local_writer_loop(Parent) end).

local_writer_loop(Parent) ->
    receive
        {'$gen_cast', Chunk} ->
            Parent ! {local_chunk, self(), Chunk},
            local_writer_loop(Parent)
    end.

%% @doc Read the storage request delivered to the fake local writer.
take_local_chunk(PID) ->
    receive
        {local_chunk, PID, Chunk} -> Chunk
    end.

%% @doc Stop a fake writer so the cache can reclaim its references.
stop_local_writer(PID) ->
    Ref = erlang:monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', Ref, process, PID, _} -> ok
    end.

%% @doc Reserve a local chunk and hand a separate reference to the writer.
local_store(Writer, StoreID, Args) ->
    {ok, CacheRef} = ar_chunk_cache:reserve(StoreID),
    try
        ar_data_sync:store_chunk(Writer, Args, {none, make_ref()}, CacheRef)
    after
        ar_chunk_cache:release(CacheRef)
    end.

%% @doc Build a storage-request fixture using the default dependency mocks.
with_storage(Test) ->
    with_storage(Test, []).

%% @doc Add dependency mocks while keeping the packing server stopped until
%% the mocks are removed.
with_storage(Test, Mocks) ->
    {setup, Setup, Cleanup, Tests} = ar_test_util:with_mocked(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
            {arweave_util, cast_after, fun(_, PID, Message) ->
                PID ! {'$gen_cast', Message},
                {ok, make_ref()}
            end},
            %% The chunk is below the two-chunk mature-data bound.
            {ar_disk_pool, get_threshold, fun() -> 2 * ?DATA_CHUNK_SIZE end},
            {ar_packing_server, request_repack, fun(Key, _, _, _) ->
                self() ! {packing_requested, Key},
                ok
            end}
        ] ++ Mocks,
        Test
    ),
    {setup,
        fun() ->
            %% Unloading the packing mock must not purge a running server.
            ok = supervisor:terminate_child(ar_packing_sup, ar_packing_server),
            Setup()
        end,
        fun(Modules) ->
            try
                Cleanup(Modules)
            after
                {ok, _} = supervisor:restart_child(
                    ar_packing_sup, ar_packing_server
                )
            end
        end,
        Tests}.

%% @doc Probe whether a cache reference is active without retaining another
%% reference after the check.
cache_reference_status(CacheRef) ->
    case ar_chunk_cache:add_reference(CacheRef, self()) of
        {ok, ProbeRef} ->
            ar_chunk_cache:release(ProbeRef),
            active;
        Error ->
            Error
    end.

%% @doc Read an already-delivered result for Ref, or return no_result.
take_store_result(Ref) ->
    receive
        {chunk_store_result, Ref, Result} -> Result
    after 0 -> no_result
    end.

%% @doc Build the storage-request arguments for one full-sized packed chunk.
chunk_args() ->
    {
        <<0:256>>,
        ?DATA_CHUNK_SIZE,
        tx_path,
        tx_root,
        <<0>>,
        packed,
        ?DATA_CHUNK_SIZE,
        ?DATA_CHUNK_SIZE,
        <<0>>,
        none,
        none,
        none
    }.

%% @doc Receive the retry message captured by the mocked delayed cast.
take_timer() ->
    receive
        {'$gen_cast', {retry_store_chunk, _} = Message} -> Message
    end.

%% @doc Receive the request key captured by the packing mock.
take_packing_request() ->
    receive
        {packing_requested, Key} -> Key
    end.
