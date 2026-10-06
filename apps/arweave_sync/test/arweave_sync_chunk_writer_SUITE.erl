-module(arweave_sync_chunk_writer_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-define(PEER, {127, 0, 0, 1, 1984}).

suite() -> [{timetrap, {seconds, 30}}].
all() ->
    [
        independent_fetched_cache_refs,
        terminal_outcomes,
        invalid_proof_and_unpack_failure,
        unpack_then_store,
        duplicate_replies,
        data_sync_down_fails_waiting_writes,
        missing_data_sync_fails_write,
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
    lists:foreach(
        fun(StoreID) ->
            catch gen_server:stop(arweave_sync_chunk_writer:name(StoreID))
        end,
        [store1, store2]
    ),
    gen_server:stop(ar_chunk_cache),
    ets:delete(ar_chunk_cache),
    persistent_term:erase({?MODULE, testcase}),
    persistent_term:erase({?MODULE, responses}),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Separate fetched chunks keep independent cache references and
%% task-completion replies.
independent_fetched_cache_refs(_) ->
    DataSync = start_data_sync(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, valid, undefined),
        {ChunkWriter, LocalRef} = take_request(DataSync),
        fetched(
            store1,
            ?PEER,
            0,
            valid,
            TaskRef
        ),
        {ChunkWriter, FetchedRef} = take_request(DataSync),
        ?assertEqual(2, ar_chunk_cache:cached_size(store1)),
        ?assertNotEqual(DataSync, ChunkWriter),
        %% A chunk that needed no unpacking is reported unpacked on handoff.
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        ChunkWriter ! {chunk_store_result, LocalRef, stored},
        ChunkWriter ! {chunk_store_result, FetchedRef, stored},
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
        ?assertEqual({task_write_completed, TaskRef}, take_task_result())
    after
        stop_data_sync(DataSync)
    end.

%% @doc Skipped and failed writes release cached chunks and report task failure.
terminal_outcomes(_) ->
    DataSync = start_data_sync(store1),
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
                {ChunkWriter, Ref} = take_request(DataSync),
                ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
                ChunkWriter ! {chunk_store_result, Ref, Result},
                ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
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
        stop_data_sync(DataSync)
    end.

%% @doc Invalid proofs and unpack errors fail the task without leaking cached
%% chunks.
invalid_proof_and_unpack_failure(_) ->
    DataSync = start_data_sync(store1),
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
        {ChunkWriter, Ref, ChunkArgs} = take_unpack(),
        ChunkWriter ! {chunk, {unpack_error, Ref, ChunkArgs, invalid}},
        ?assertEqual({task_write_failed, BadUnpackRef}, take_task_result()),
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc A packed chunk keeps its cache reference through unpacking and
%% storage, ignoring old timeouts.
unpack_then_store(_) ->
    DataSync = start_data_sync(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(
            store1,
            ?PEER,
            0,
            packed,
            TaskRef
        ),
        {ChunkWriter, Ref, {_, Chunk, Offset, TXRoot, Size}} = take_unpack(),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        ChunkWriter ! {chunk, {unpacked, Ref, {unpacked, Chunk, Offset, TXRoot, Size}}},
        ?assertEqual({ChunkWriter, Ref}, take_request(DataSync)),
        %% The footprint's entropy is released as soon as the chunk is unpacked,
        %% before the write completes.
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        %% An old unpack timeout must not expire the later storage phase.
        gen_server:cast(ChunkWriter, {expire, unpack, Ref}),
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        ChunkWriter ! {chunk_store_result, Ref, stored},
        ?assertEqual({task_write_completed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc Duplicate or unknown write replies cannot release another chunk's
%% cache reference.
duplicate_replies(_) ->
    DataSync = start_data_sync(store1),
    try
        fetched(store1, ?PEER, 0, valid, undefined),
        {ChunkWriter, FirstRef} = take_request(DataSync),
        fetched(store1, ?PEER, 0, valid, undefined),
        {ChunkWriter, SecondRef} = take_request(DataSync),
        ChunkWriter ! {chunk_store_result, FirstRef, stored},
        ChunkWriter ! {chunk_store_result, FirstRef, stored},
        ChunkWriter ! {chunk_store_result, make_ref(), stored},
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        ChunkWriter ! {chunk_store_result, SecondRef, stored},
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc When a store's ar_data_sync process dies, the writes waiting on it fail
%% and the chunk writer hands later writes to the restarted process, without
%% disturbing another store.
data_sync_down_fails_waiting_writes(_) ->
    DataSync = start_data_sync(store1),
    OtherDataSync = start_data_sync(store2),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, valid, TaskRef),
        {ChunkWriter, _} = take_request(DataSync),
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        fetched(store2, ?PEER, 0, valid, undefined),
        {_, _} = take_request(OtherDataSync),
        stop_data_sync(DataSync),
        ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
        ?assertEqual(1, ar_chunk_cache:cached_size(store2)),
        NewDataSync = start_data_sync(store1),
        try
            NewTaskRef = {self(), make_ref()},
            fetched(store1, ?PEER, 0, valid, NewTaskRef),
            {ChunkWriter, Ref} = take_request(NewDataSync),
            ?assertEqual({task_unpacked, NewTaskRef}, take_task_result()),
            ChunkWriter ! {chunk_store_result, Ref, stored},
            ?assertEqual(
                {task_write_completed, NewTaskRef}, take_task_result()
            )
        after
            stop_data_sync(NewDataSync)
        end
    after
        stop_data_sync(OtherDataSync)
    end.

%% @doc A write with no ar_data_sync process running fails the task and
%% releases its cache reference.
missing_data_sync_fails_write(_) ->
    ChunkWriter = start_chunk_writer(store1),
    TaskRef = {self(), make_ref()},
    fetched(store1, ?PEER, 0, valid, TaskRef),
    ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
    ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
    ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
    ?assertEqual(0, ar_chunk_cache:cached_size(store1)).

%% @doc An unpack timeout releases the cache reference and makes a late reply
%% harmless.
unpack_timeout(_) ->
    DataSync = start_data_sync(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {ChunkWriter, Ref, ChunkArgs} = take_unpack(),
        gen_server:cast(ChunkWriter, {expire, unpack, Ref}),
        ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
        ChunkWriter ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual(pong, gen_server:call(ChunkWriter, ping)),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc A failed unpacked-chunk validation fails the task and releases its
%% cache reference.
invalid_unpacked_chunk(_) ->
    responses(#{valid_unpacked => false}),
    DataSync = start_data_sync(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {ChunkWriter, Ref, ChunkArgs} = take_unpack(),
        ChunkWriter ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual({task_write_failed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc A busy unpacker retries with the same cache reference and completes
%% after capacity returns.
unpack_backpressure(_) ->
    responses(#{request_unpack => busy}),
    DataSync = start_data_sync(store1),
    TaskRef = {self(), make_ref()},
    try
        fetched(store1, ?PEER, 0, packed, TaskRef),
        {ChunkWriter, Ref, ChunkArgs} = take_unpack(),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        responses(#{}),
        %% Deliver the retry without waiting for the one-second timer.
        ChunkWriter ! {retry_unpack, Ref},
        ?assertEqual({ChunkWriter, Ref, ChunkArgs}, take_unpack()),
        ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
        ChunkWriter ! {chunk, {unpacked, Ref, ChunkArgs}},
        ?assertEqual({ChunkWriter, Ref}, take_request(DataSync)),
        ?assertEqual({task_unpacked, TaskRef}, take_task_result()),
        ChunkWriter ! {chunk_store_result, Ref, stored},
        ?assertEqual({task_write_completed, TaskRef}, take_task_result()),
        ?assertEqual(0, ar_chunk_cache:cached_size(store1))
    after
        stop_data_sync(DataSync)
    end.

%% @doc Already-recorded or unacceptable-proof chunks release capacity without
%% further storage.
validated_chunk_skips(_) ->
    DataSync = start_data_sync(store1),
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
        stop_data_sync(DataSync)
    end.

%% @doc Recent chunks go through the disk pool and report its success or failure
%% to sync.
recent_chunk_uses_disk_pool(_) ->
    %% End offset one is now at the mature-data bound, not strictly below it.
    DataSync = start_data_sync(store1),
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
        stop_data_sync(DataSync)
    end.

%%====================================================================
%% Helpers
%%====================================================================

responses(Map) -> persistent_term:put({?MODULE, responses}, Map).
response(Name, Default) ->
    maps:get(Name, persistent_term:get({?MODULE, responses}), Default).

%% @doc Start a stand-in for the store's ar_data_sync process, registered
%% under name/1 below, and make sure the store's chunk writer is running.
start_data_sync(StoreID) ->
    start_chunk_writer(StoreID),
    Parent = self(),
    PID = spawn(fun() ->
        true = register(name(StoreID), self()),
        Parent ! {data_sync_ready, self()},
        data_sync_loop(Parent)
    end),
    receive
        {data_sync_ready, PID} -> PID
    end.

start_chunk_writer(StoreID) ->
    case whereis(arweave_sync_chunk_writer:name(StoreID)) of
        undefined ->
            {ok, PID} = arweave_sync_chunk_writer:start_link(StoreID),
            PID;
        PID ->
            PID
    end.

data_sync_loop(Parent) ->
    receive
        {'$gen_cast', {store_chunk, _, ReplyTo}} ->
            Parent ! {request, self(), ReplyTo},
            data_sync_loop(Parent)
    end.

stop_data_sync(PID) ->
    Ref = monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', Ref, process, PID, _} -> ok
    end.

take_request(DataSync) ->
    receive
        {request, DataSync, ReplyTo} -> ReplyTo
    end.
take_task_result() ->
    receive
        {'$gen_cast', Result} -> Result
    end.
take_unpack() ->
    receive
        {unpack, PID, Ref, ChunkArgs} -> {PID, Ref, ChunkArgs}
    end.

name(StoreID) -> list_to_atom("test_data_sync_" ++ atom_to_list(StoreID)).
store_chunk(DataSync, Args, ReplyTo, _CacheRef) ->
    gen_server:cast(DataSync, {store_chunk, Args, ReplyTo}).
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
    arweave_sync_chunk_writer:store_fetched_chunk(
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
config() -> arweave_config.
metrics() -> arweave_metrics.

store_info(StoreID) -> arweave_storage:store_info(StoreID).
disk_pool() -> ?MODULE.
packing() -> ?MODULE.
peers() -> ?MODULE.
clock() -> arweave_sync_test_deps.
events() -> arweave_sync_test_deps.
node() -> arweave_sync_test_deps.
