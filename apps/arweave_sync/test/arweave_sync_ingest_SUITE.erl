-module(arweave_sync_ingest_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-define(PEER, {127, 0, 0, 1, 1984}).

suite() -> [{timetrap, {seconds, 30}}].
all() ->
    [
        independent_fetched_reservations,
        terminal_outcomes,
        invalid_proof_and_unpack_failure,
        unpack_then_store,
        duplicate_replies,
        ingestion_restart,
        writer_restart,
        disabled_network_preserves_ingestion,
        unpack_timeout,
        invalid_unpacked_chunk,
        unpack_backpressure,
        validated_chunk_skips,
        recent_chunk_uses_disk_pool
    ].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(arweave_sync),
    [{started_apps, Started} | Config].
end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ),
    ok.
init_per_testcase(_, Config) ->
    ar_chunk_cache:create_ets(),
    {ok, _} = ar_chunk_cache:start_link(),
    arweave_sync_deps:override_module(?MODULE),
    persistent_term:put({?MODULE, testcase}, self()),
    persistent_term:put({?MODULE, responses}, #{}),
    Config.
end_per_testcase(_, _) ->
    arweave_sync:deactivate(),
    gen_server:stop(ar_chunk_cache),
    ets:delete(ar_chunk_cache),
    persistent_term:erase({?MODULE, testcase}),
    persistent_term:erase({?MODULE, responses}),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Separate fetched chunks retain independent reservations and
%% task-completion replies.
independent_fetched_reservations(_) ->
    Writer = start_writer(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, valid, undefined),
        {Ingest, LocalRef} = take_request(Writer),
        fetched(
            store1,
            ?PEER,
            0,
            valid,
            TaskRef
        ),
        {Ingest, FetchedRef} = take_request(Writer),
        ?assertEqual(2, ar_chunk_cache:cached_size(store1)),
        ?assertNotEqual(Writer, Ingest),
        %% A chunk that needed no unpacking is reported unpacked on handoff.
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        Ingest ! {chunk_store_result, LocalRef, stored},
        Ingest ! {chunk_store_result, FetchedRef, stored},
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
        ?assertEqual({task_write_completed, TaskRef}, take_task_result())
    after
        stop_writer(Writer)
    end.

%% @doc Skipped and failed writes release cached chunks and report task failure.
terminal_outcomes(_) ->
    Writer = start_writer(store1),
    try
        lists:foreach(
            fun(Result) ->
                TaskRef = {self(), make_ref()},
                fetched(
                    store1,
                    ?PEER,
                    0,
                    valid,
                    TaskRef
                ),
                {Ingest, Ref} = take_request(Writer),
                ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
                Ingest ! {chunk_store_result, Ref, Result},
                ?assertEqual(pong, gen_server:call(Ingest, ping)),
                ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
                ?assertEqual({task_write_failed, TaskRef}, take_task_result())
            end,
            [
                {skipped, already_stored},
                {error, disk_full},
                {error, packing_timeout}
            ]
        )
    after
        stop_writer(Writer)
    end.

%% @doc Invalid proofs and unpack errors fail the task without leaking cached
%% chunks.
invalid_proof_and_unpack_failure(_) ->
    Writer = start_writer(store1),
    try
        BadProofRef = {self(), make_ref()},
        fetched(
            store1,
            ?PEER,
            0,
            false,
            BadProofRef
        ),
        ?assertEqual({task_write_failed, BadProofRef}, take_task_result()),
        BadUnpackRef = {self(), make_ref()},
        fetched(
            store1,
            ?PEER,
            0,
            packed,
            BadUnpackRef
        ),
        {Ingest, Ref, ChunkArgs} = take_unpack(),
        Ingest ! {chunk, {unpack_error, Ref, ChunkArgs, invalid}},
        ?assertEqual({task_write_failed, BadUnpackRef}, take_task_result()),
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc A packed chunk retains its reservation through unpacking and storage,
%% ignoring old timeouts.
unpack_then_store(_) ->
    Writer = start_writer(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(
            store1,
            ?PEER,
            0,
            packed,
            TaskRef
        ),
        {Ingest, Ref, {_, Chunk, Offset, TXRoot, Size}} = take_unpack(),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        Ingest ! {chunk, {unpacked, Ref, {unpacked, Chunk, Offset, TXRoot, Size}}},
        ?assertEqual({Ingest, Ref}, take_request(Writer)),
        %% The footprint's entropy is released as soon as the chunk is unpacked,
        %% before the write completes.
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        %% An old unpack timeout must not expire the later storage phase.
        gen_server:cast(Ingest, {expire, unpack, Ref}),
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        Ingest ! {chunk_store_result, Ref, stored},
        ?assertEqual({task_write_completed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc Duplicate or unknown write replies cannot release another chunk's
%% reservation.
duplicate_replies(_) ->
    Writer = start_writer(store1),
    try
        fetched(store1, ?PEER, 0, valid, undefined),
        {Ingest, FirstRef} = take_request(Writer),
        fetched(store1, ?PEER, 0, valid, undefined),
        {Ingest, SecondRef} = take_request(Writer),
        Ingest ! {chunk_store_result, FirstRef, stored},
        Ingest ! {chunk_store_result, FirstRef, stored},
        Ingest ! {chunk_store_result, make_ref(), stored},
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        Ingest ! {chunk_store_result, SecondRef, stored},
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc Restarting ingestion cancels old writes, frees cache capacity and
%% ignores stale replies.
ingestion_restart(_) ->
    Writer = start_writer(store1),
    try
        fetched(store1, ?PEER, 0, valid, undefined),
        {OldIngest, OldRef} = take_request(Writer),
        Root = whereis(arweave_sync_sup),
        exit(OldIngest, kill),
        ?assertMatch({OldIngest, _}, take_cancel(Writer)),
        ?assertEqual(
            ok,
            ar_test_await:until(ingest_restarted, fun() ->
                case arweave_sync_ingest:writer(store1) of
                    {Writer, PID} ->
                        PID =/= OldIngest andalso
                            (catch gen_server:call(PID, ping)) =:= pong;
                    _ ->
                        false
                end
            end)
        ),
        ?assertEqual(Root, ets:info(arweave_sync_state, owner)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
        fetched(store1, ?PEER, 0, valid, undefined),
        {NewIngest, NewRef} = take_request(Writer),
        NewIngest ! {chunk_store_result, OldRef, stored},
        ?assertEqual(pong, gen_server:call(NewIngest, ping)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        NewIngest ! {chunk_store_result, NewRef, stored},
        ?assertEqual(pong, gen_server:call(NewIngest, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc A writer restart rebuilds its ingestion process without disturbing
%% another store.
writer_restart(_) ->
    Writer = start_writer(store1),
    OtherWriter = start_writer(store2),
    fetched(store1, ?PEER, 0, valid, undefined),
    {OldIngest, _} = take_request(Writer),
    fetched(store2, ?PEER, 0, valid, undefined),
    {OtherIngest, _} = take_request(OtherWriter),
    stop_writer(Writer),
    NewWriter = start_writer(store1),
    try
        ?assertNot(is_process_alive(OldIngest)),
        ?assert(is_process_alive(OtherIngest)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store2)),
        fetched(store1, ?PEER, 0, valid, undefined),
        {NewIngest, Ref} = take_request(NewWriter),
        NewIngest ! {chunk_store_result, Ref, stored},
        ?assertEqual(pong, gen_server:call(NewIngest, ping)),
        ?assertEqual(1, ar_chunk_cache:reserved_size())
    after
        stop_writer(NewWriter),
        stop_writer(OtherWriter)
    end.

%% @doc Ingestion still completes local storage work when network syncing is
%% disabled.
disabled_network_preserves_ingestion(_) ->
    arweave_config:internal_with_test_config(fun() ->
        ok = arweave_config:set([sync, max_download_rate], 0),
        ?assertEqual(ok, arweave_sync:activate()),
        ?assert(is_pid(whereis(arweave_sync_pipeline_sup))),
        Writer = start_writer(store1),
        try
            fetched(store1, ?PEER, 0, valid, undefined),
            {Ingest, Ref} = take_request(Writer),
            ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
            Ingest ! {chunk_store_result, Ref, stored},
            ?assertEqual(pong, gen_server:call(Ingest, ping)),
            ?assertEqual(0, ar_chunk_cache:cached_size(store1))
        after
            stop_writer(Writer)
        end
    end).

%% @doc An unpack timeout releases the reservation and makes a late reply
%% harmless.
unpack_timeout(_) ->
    Writer = start_writer(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {Ingest, Ref, ChunkArgs} = take_unpack(),
        gen_server:cast(Ingest, {expire, unpack, Ref}),
        ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
        Ingest ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual(pong, gen_server:call(Ingest, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc A failed unpacked-chunk validation fails the task and releases its
%% reservation.
invalid_unpacked_chunk(_) ->
    responses(#{valid_unpacked => false}),
    Writer = start_writer(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {Ingest, Ref, ChunkArgs} = take_unpack(),
        Ingest ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc A busy unpacker retries with the same reservation and completes after
%% capacity returns.
unpack_backpressure(_) ->
    responses(#{request_unpack => busy}),
    Writer = start_writer(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {Ingest, Ref, ChunkArgs} = take_unpack(),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        responses(#{}),
        %% Deliver the retry without waiting for the one-second timer.
        Ingest ! {retry_unpack, Ref},
        ?assertEqual({Ingest, Ref, ChunkArgs}, take_unpack()),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        Ingest ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual({Ingest, Ref}, take_request(Writer)),
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        Ingest ! {chunk_store_result, Ref, stored},
        ?assertEqual({task_write_completed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_writer(Writer)
    end.

%% @doc Already-recorded or unacceptable-proof chunks release capacity without
%% further storage.
validated_chunk_skips(_) ->
    Writer = start_writer(store1),
    try
        lists:foreach(
            fun(Responses) ->
                responses(Responses),
                TaskRef = {self(), make_ref()},
                fetched(store1, ?PEER, 0, valid, TaskRef),
                ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
                ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
                ?assertEqual(0, ar_chunk_cache:cached_size(store1))
            end,
            [
                #{proof_ratio => false},
                #{recorded => true},
                #{recorded => {true, unpacked}}
            ]
        )
    after
        stop_writer(Writer)
    end.

%% @doc Recent chunks go through the disk pool and report its success or failure
%% to sync.
recent_chunk_uses_disk_pool(_) ->
    %% End offset one is now at the mature-data bound, not strictly below it.
    Writer = start_writer(store1),
    try
        lists:foreach(
            fun({Result, Expected}) ->
                responses(#{disk_pool_threshold => 1, disk_pool_result => Result}),
                TaskRef = {self(), make_ref()},
                fetched(store1, ?PEER, 0, valid, TaskRef),
                ?assertEqual(
                    {data_root, data_path, <<0>>, 0, 1, ?PEER},
                    receive
                        {disk_pool_chunk, Args} -> Args
                    end
                ),
                ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
                ?assertEqual({Expected, TaskRef}, take_task_result()),
                ?assertEqual(0, ar_chunk_cache:cached_size(store1))
            end,
            [
                {ok, task_write_completed},
                {temporary, task_write_completed},
                {{error, disk_full}, task_write_failed}
            ]
        )
    after
        stop_writer(Writer)
    end.

%%====================================================================
%% Helpers
%%====================================================================

responses(Map) -> persistent_term:put({?MODULE, responses}, Map).
response(Name, Default) ->
    maps:get(Name, persistent_term:get({?MODULE, responses}), Default).

start_writer(StoreID) ->
    Parent = self(),
    PID = spawn(fun() ->
        ok = arweave_sync:register_store(StoreID),
        Parent ! {writer_ready, self()},
        writer_loop(Parent)
    end),
    receive
        {writer_ready, PID} -> PID
    end.

writer_loop(Parent) ->
    receive
        {'$gen_cast', {store_chunk, _, ReplyTo}} ->
            Parent ! {request, self(), ReplyTo},
            writer_loop(Parent);
        {'$gen_call', From, {cancel, Client}} ->
            Parent ! {cancelled, self(), Client, ar_chunk_cache:reserved_size()},
            gen_server:reply(From, ok),
            writer_loop(Parent)
    end.

stop_writer(PID) ->
    Ref = monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', Ref, process, PID, _} -> ok
    end.

take_request(Writer) ->
    receive
        {request, Writer, ReplyTo} -> ReplyTo
    end.
take_cancel(Writer) ->
    receive
        {cancelled, Writer, Client, Size} -> {Client, Size}
    end.
take_task_result() ->
    receive
        {'$gen_cast', Result} -> Result
    end.
take_unpack() ->
    receive
        {unpack, PID, Ref, ChunkArgs} -> {PID, Ref, ChunkArgs}
    end.

store_chunk(Writer, Args, ReplyTo, _CacheRef) ->
    gen_server:cast(Writer, {store_chunk, Args, ReplyTo}).
cancel_store_requests(Writer, Client) -> gen_server:call(Writer, {cancel, Client}).
validate_fetched_chunk(_, _, false) ->
    false;
validate_fetched_chunk(Peer, Byte, Packing) ->
    Status =
        case Packing of
            valid -> valid;
            packed -> needs_unpacking
        end,
    %% A one-byte chunk sits strictly below the two-byte mature-data bound.
    {Status, {Packing, <<0>>, 1, tx_root, 1},
        {0, 1, data_path, tx_path, data_root, <<0>>, id, 1, Peer, Byte}}.
validate_chunk_id_size(_, _, _) -> response(valid_unpacked, true).
is_chunk_proof_ratio_attractive(_, _, _) -> response(proof_ratio, true).
is_recorded(_, any_packing, {ar_data_sync, byte}, _) ->
    response(recorded, false).
get_threshold() -> response(disk_pool_threshold, 2).
add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize, Peer) ->
    persistent_term:get({?MODULE, testcase}) !
        {disk_pool_chunk, {DataRoot, DataPath, Chunk, Offset, TXSize, Peer}},
    response(disk_pool_result, ok).
request_unpack(Ref, ReplyTo, ChunkArgs, _CacheRef) ->
    persistent_term:get({?MODULE, testcase}) ! {unpack, ReplyTo, Ref, ChunkArgs},
    response(request_unpack, ok).
issue_warning(_, chunk, _) -> ok.

fetched(StoreID, Peer, Byte, Proof, TaskRef) ->
    {ok, CacheRef} = ar_chunk_cache:reserve(StoreID),
    arweave_sync_ingest:store_fetched_chunk(
        StoreID,
        Peer,
        Byte,
        Proof,
        TaskRef,
        CacheRef
    ).
chunk_cache() -> ar_chunk_cache.
data_sync() -> ?MODULE.
storage() -> ?MODULE.
disk_pool() -> ?MODULE.
packing() -> ?MODULE.
peers() -> ?MODULE.
clock() -> arweave_sync_test_deps.
events() -> arweave_sync_test_deps.
node() -> arweave_sync_test_deps.
