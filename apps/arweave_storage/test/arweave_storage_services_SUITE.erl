-module(arweave_storage_services_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        owns_runtime_tables,
        legacy_snapshot_and_wal,
        lifecycle_preserves_chunks_and_records,
        data_sizes_without_mining,
        missing_entropy_repair,
        entropy_cursor_persistence,
        raw_chunk_helpers_preserve_records,
        global_record_aggregation,
        global_footprint_run
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(Name, Config) ->
    Snapshot = arweave_config:internal_snapshot(),
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(Name)),
    %% Each case uses its own on-disk databases; no node or network is started.
    ok = arweave_config:internal_force_config(#{
        [data_dir] => DataDir,
        [log_dir] => filename:join(DataDir, "logs"),
        [storage_modules] => [],
        [repack_modules] => []
    }),
    {ok, KV} = ar_kv_sup:start_link(),
    unlink(KV),
    {ok, Events} = ar_events:start_link(sync_record),
    unlink(Events),
    {ok, ChunkEvents} = ar_events:start_link(chunk_storage),
    unlink(ChunkEvents),
    [
        {config_snapshot, Snapshot},
        {kv, KV},
        {events, Events},
        {chunk_events, ChunkEvents},
        {data_dir, DataDir}
        | Config
    ].

end_per_testcase(_, Config) ->
    arweave_storage:close_chunk_files(?DEFAULT_MODULE),
    ok = arweave_storage:deactivate(),
    gen_server:stop(?config(events, Config)),
    gen_server:stop(?config(chunk_events, Config)),
    gen_server:stop(?config(kv, Config)),
    arweave_config:internal_restore(?config(config_snapshot, Config)).

%% @doc Local additions join the global record, but removal requires a global event.
global_record_aggregation(_) ->
    DiskPoolStart = arweave_constants:partition_size(),
    PartitionStart = DiskPoolStart - ?DATA_CHUNK_SIZE,
    %% Seed four chunks directly; node-driven genesis seeding stays in EUnit.
    WeaveSize = 4 * ?DATA_CHUNK_SIZE,
    Packing = {spora_2_6, <<0:256>>},
    Partition = {0, DiskPoolStart, Packing},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Partition]}),
    PartitionID = arweave_storage_module:id(Partition),
    {ok, _} = arweave_storage:activate(),
    ok = arweave_storage:add_sync_record(
        WeaveSize, 0, Packing, {ar_data_sync, byte}, PartitionID
    ),
    Options = #{format => etf, random_subset => false},

    %% Genesis data only
    ok = ar_test_await:global_sync_record_matches(Options, [{WeaveSize, 0}]),
    ?assertEqual(
        not_found,
        arweave_storage:get_interval(
            DiskPoolStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),

    %% Add a diskpool chunk
    arweave_storage:add_sync_record(
        DiskPoolStart + ?DATA_CHUNK_SIZE,
        DiskPoolStart,
        unpacked,
        {ar_data_sync, byte},
        ?DEFAULT_MODULE
    ),
    ok = ar_test_await:global_sync_record_matches(
        Options,
        [{WeaveSize, 0}, {DiskPoolStart + ?DATA_CHUNK_SIZE, DiskPoolStart}]
    ),
    ?assertEqual(
        {DiskPoolStart + ?DATA_CHUNK_SIZE, DiskPoolStart},
        arweave_storage:get_interval(
            DiskPoolStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),

    %% Remove the diskpool chunk
    arweave_storage:delete_sync_record(
        DiskPoolStart + ?DATA_CHUNK_SIZE,
        DiskPoolStart,
        {ar_data_sync, byte},
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        pong, gen_server:call(ar_events:event_to_process(sync_record), ping)
    ),
    {ok, Binary3} = arweave_storage:get_serialized_sync_record(Options),
    {ok, Global3} = ar_intervals:safe_from_etf(Binary3),
    ?assertEqual(
        [{WeaveSize, 0}, {DiskPoolStart + ?DATA_CHUNK_SIZE, DiskPoolStart}],
        ar_intervals:to_list(Global3)
    ),
    %% We need to explicitly declare global removal
    ar_events:send(
        sync_record,
        {global_remove_range, DiskPoolStart, DiskPoolStart + ?DATA_CHUNK_SIZE}
    ),
    ok = ar_test_await:global_sync_record_matches(Options, [{WeaveSize, 0}]),

    %% Add a storage module chunk
    arweave_storage:add_sync_record(
        PartitionStart + ?DATA_CHUNK_SIZE,
        PartitionStart,
        unpacked,
        {ar_data_sync, byte},
        PartitionID
    ),
    ?assertEqual(
        pong, gen_server:call(ar_events:event_to_process(sync_record), ping)
    ),
    {ok, Binary5} = arweave_storage:get_serialized_sync_record(Options),
    {ok, Global5} = ar_intervals:safe_from_etf(Binary5),

    ?assertEqual(
        [{WeaveSize, 0}, {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart}],
        ar_intervals:to_list(Global5)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_interval(
            DiskPoolStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),
    ?assertEqual(
        {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart},
        arweave_storage:get_interval(
            PartitionStart + 1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),

    %% Remove the storage module chunk
    arweave_storage:delete_sync_record(
        PartitionStart + ?DATA_CHUNK_SIZE,
        PartitionStart,
        {ar_data_sync, byte},
        PartitionID
    ),
    ?assertEqual(
        pong, gen_server:call(ar_events:event_to_process(sync_record), ping)
    ),
    {ok, Binary6} = arweave_storage:get_serialized_sync_record(Options),
    {ok, Global6} = ar_intervals:safe_from_etf(Binary6),
    ?assertEqual(
        [{WeaveSize, 0}, {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart}],
        ar_intervals:to_list(Global6)
    ),
    ar_events:send(
        sync_record,
        {global_remove_range, PartitionStart, PartitionStart + ?DATA_CHUNK_SIZE}
    ),
    ok = ar_test_await:global_sync_record_matches(Options, [{WeaveSize, 0}]),
    ?assertEqual(
        not_found,
        arweave_storage:get_interval(
            DiskPoolStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_interval(
            PartitionStart + 1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),

    %% Add chunk to both diskpool and storage module
    arweave_storage:add_sync_record(
        PartitionStart + ?DATA_CHUNK_SIZE,
        PartitionStart,
        unpacked,
        {ar_data_sync, byte},
        ?DEFAULT_MODULE
    ),
    arweave_storage:add_sync_record(
        PartitionStart + ?DATA_CHUNK_SIZE,
        PartitionStart,
        unpacked,
        {ar_data_sync, byte},
        PartitionID
    ),
    ok = ar_test_await:global_sync_record_matches(
        Options,
        [{WeaveSize, 0}, {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart}]
    ),
    ?assertEqual(
        {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart},
        arweave_storage:get_interval(
            PartitionStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),
    ?assertEqual(
        {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart},
        arweave_storage:get_interval(
            PartitionStart + 1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),

    %% Now remove it from just the diskpool
    arweave_storage:delete_sync_record(
        PartitionStart + ?DATA_CHUNK_SIZE,
        PartitionStart,
        {ar_data_sync, byte},
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        pong, gen_server:call(ar_events:event_to_process(sync_record), ping)
    ),
    {ok, Binary7} = arweave_storage:get_serialized_sync_record(Options),
    {ok, Global7} = ar_intervals:safe_from_etf(Binary7),

    ?assertEqual(
        [{WeaveSize, 0}, {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart}],
        ar_intervals:to_list(Global7)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_interval(
            DiskPoolStart + 1,
            any_packing,
            {ar_data_sync, byte},
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        {WeaveSize, 0},
        arweave_storage:get_interval(
            1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ),
    ?assertEqual(
        {PartitionStart + ?DATA_CHUNK_SIZE, PartitionStart},
        arweave_storage:get_interval(
            PartitionStart + 1, any_packing, {ar_data_sync, byte}, PartitionID
        )
    ).

%% @doc A footprint run joins the global footprint buckets at a cost set by
%% the buckets it spans, not by its length in offsets.
global_footprint_run(_) ->
    {ok, _} = arweave_storage:activate(),
    Pid = whereis(arweave_storage_global_sync_record),
    %% A thousand buckets' worth of footprint offsets.
    RunEnd = 1000 * ?NETWORK_FOOTPRINT_BUCKET_SIZE,
    {reductions, Before} = erlang:process_info(Pid, reductions),
    ok = ar_events:send(
        sync_record, {add_range, 0, RunEnd, ar_data_sync_footprints, #{}}
    ),
    ?assertEqual(
        pong, gen_server:call(ar_events:event_to_process(sync_record), ping)
    ),
    %% The call queues behind the event, so it returns once the run is in.
    ?assertMatch(
        {ok, _},
        arweave_storage:get_serialized_sync_record(#{format => etf, start => 0})
    ),
    {reductions, After} = erlang:process_info(Pid, reductions),
    %% Adding the run offset by offset spends about 44 reductions per offset
    %% (1.6 million here). As one range it spends a few per bucket (about 6,400
    %% here), so one reduction per offset is a wide cap between the two.
    ?assert(After - Before < RunEnd),
    ok = ar_test_await:until(footprint_run_in_buckets, fun() ->
        {ok, Serialized} = arweave_storage:get_serialized_buckets(footprint),
        {BucketSize, Shares} = binary_to_term(Serialized),
        maps:get((RunEnd div 2) div BucketSize, Shares, 0) == 1
    end).

%% @doc Explicit entropy repairs a missing-entropy write without overwriting a
%% competing writer.
missing_entropy_repair(_) ->
    Packing = {replica_2_9, <<0:256>>},
    Module = {0, arweave_constants:partition_size(), Packing},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    {ok, _} = arweave_storage:activate(),
    StoreID = arweave_storage_module:id(Module),
    %% One chunk beyond the test split threshold, with no prepared entropy.
    Offset = 4 * ?DATA_CHUNK_SIZE,
    Chunk = binary:copy(<<42>>, ?DATA_CHUNK_SIZE),
    Entropy = binary:copy(<<7>>, ?DATA_CHUNK_SIZE),
    ok = arweave_storage:set_entropy_complete(StoreID),
    ?assertEqual(
        {error, {missing_entropy, <<0:256>>}},
        arweave_storage:put_chunk(Offset, Chunk, unpacked_padded, StoreID)
    ),
    {_FileStart, Filepath, _, _} =
        arweave_storage:locate_chunk_on_disk(Offset, StoreID),
    ?assertEqual([], ets:lookup(ar_entropy_storage, {semaphore, Filepath})),
    %% Only the pure cipher is stubbed; the storage processes and disk I/O are real.
    meck:new(arweave_storage_deps, [passthrough, no_link]),
    meck:expect(
        arweave_storage_deps,
        encipher_replica_2_9_chunk,
        fun crypto:exor/2
    ),
    try
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                Offset,
                {chunk_with_entropy, Chunk, Entropy},
                unpacked_padded,
                StoreID
            )
        ),
        ?assertEqual(
            {Offset, crypto:exor(Chunk, Entropy)},
            arweave_storage:get_chunk(Offset - 1, StoreID)
        ),
        %% Another writer winning the race must not be overwritten by a retry.
        ?assertEqual(
            {error, already_stored},
            arweave_storage:put_chunk(
                Offset,
                {chunk_with_entropy, Chunk, Entropy},
                unpacked_padded,
                StoreID
            )
        )
    after
        arweave_storage:close_chunk_files(StoreID),
        meck:unload(arweave_storage_deps)
    end.

%% @doc Preparation cursors survive runtime restart and restore the
%% entropy-complete state.
entropy_cursor_persistence(_) ->
    Packing = {replica_2_9, <<0:256>>},
    Module = {0, arweave_constants:partition_size(), Packing},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    {ok, _} = arweave_storage:activate(),
    StoreID = arweave_storage_module:id(Module),
    ?assertEqual(
        {false, <<0:256>>},
        arweave_storage:entropy_context(StoreID, Packing)
    ),
    %% The barrier succeeds even though preparation has not completed.
    ?assert(arweave_storage:await_entropy_writes(StoreID)),
    %% Preparation completes once its cursor passes the effective module end.
    {_, End} = arweave_storage_module:get_range(StoreID),
    ok = arweave_storage:write_entropy_cursor(End + 1, StoreID),
    ok = arweave_storage:deactivate(),
    {ok, _} = arweave_storage:activate(),
    ?assertEqual(End + 1, arweave_storage:read_entropy_cursor(StoreID, 0)),
    ?assertEqual(
        {true, <<0:256>>},
        arweave_storage:entropy_context(StoreID, Packing)
    ).

%% @doc Storage activation owns its workers and tables, and deactivation removes
%% them.
owns_runtime_tables(_) ->
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assertEqual(undefined, whereis(arweave_storage_runtime_sup)),
    {ok, Runtime} = arweave_storage:activate(),
    ?assertEqual({ok, Runtime}, arweave_storage:activate()),
    ?assertMatch(
        [{arweave_storage_runtime_sup, Runtime, supervisor, _}],
        supervisor:which_children(arweave_storage_sup)
    ),
    ?assertEqual(Runtime, ets:info(ar_entropy_storage, owner)),
    ?assertEqual(Runtime, ets:info(arweave_storage_global_sync_record, owner)),
    ?assertEqual(
        whereis(arweave_storage_sync_record_sup),
        ets:info(sync_records, owner)
    ),
    ?assertEqual(
        whereis(arweave_storage_chunk_storage_sup),
        ets:info(chunk_storage_file_index, owner)
    ),
    ?assert(is_pid(whereis(arweave_storage_sync_record:name(?DEFAULT_MODULE)))),
    ?assert(
        is_pid(whereis(arweave_storage_chunk_storage:name(?DEFAULT_MODULE)))
    ),
    ?assertEqual(undefined, whereis(arweave_entropy_runtime_sup)),
    ?assertEqual(undefined, whereis(ar_packing_server)),
    ?assertMatch({ok, _}, arweave_storage:get_serialized_buckets(byte)),
    ?assertMatch({ok, _}, arweave_storage:get_serialized_buckets(footprint)),
    ok = arweave_storage:deactivate(),
    ?assertEqual(undefined, ets:info(sync_records)),
    ?assertEqual(undefined, ets:info(chunk_storage_file_index)),
    ?assertEqual(undefined, ets:info(ar_entropy_storage)),
    ?assertEqual([], supervisor:which_children(arweave_storage_sup)).

%% @doc Legacy snapshots and WAL entries load correctly and survive compressed
%% persistence.
legacy_snapshot_and_wal(Config) ->
    StoreID = ?DEFAULT_MODULE,
    DB = {sync_record, StoreID},
    Dir = filename:join([
        ?config(data_dir, Config),
        "rocksdb",
        "ar_sync_record_db"
    ]),
    ok = ar_kv:open(#{path => Dir, name => DB}),
    %% An old uncompressed snapshot covers one chunk, followed by one WAL add.
    Chunk = ?DATA_CHUNK_SIZE,
    Intervals = ar_intervals:from_list([{Chunk, 0}]),
    Snapshot = {#{ar_data_sync => Intervals}, #{
        {ar_data_sync, unpacked} => Intervals
    }},
    ok = ar_kv:put(DB, <<"sync_records">>, term_to_binary(Snapshot)),
    ok = ar_kv:put(
        DB,
        binary:encode_unsigned(1),
        term_to_binary({{add, unpacked}, {2 * Chunk, Chunk, ar_data_sync}})
    ),
    ok = ar_kv:put(DB, <<"wal">>, binary:encode_unsigned(1)),
    {ok, _} = arweave_storage:activate(),
    ?assertEqual(
        [{2 * Chunk, 0}],
        ar_intervals:to_list(
            arweave_storage_sync_record:get(
                unpacked, {ar_data_sync, byte}, StoreID
            )
        )
    ),
    ?assertEqual(
        true,
        arweave_storage:is_recorded(
            2 * Chunk, unpacked, {ar_data_sync, byte}, StoreID
        )
    ),
    {ok, Serialized} = arweave_storage:get_serialized_sync_record(
        #{format => etf, start => 0}
    ),
    {ok, Advertised} = ar_intervals:safe_from_etf(Serialized),
    ?assertEqual([{2 * Chunk, 0}], ar_intervals:to_list(Advertised)),
    ok = arweave_storage:deactivate(),
    %% The same database now contains the current compressed snapshot.
    {ok, Persisted} = ar_kv:get(DB, <<"sync_records">>),
    ?assertMatch(<<131, 80, _/binary>>, Persisted),
    {ok, _} = arweave_storage:activate(),
    ?assertEqual(
        [{2 * Chunk, 0}],
        ar_intervals:to_list(
            arweave_storage_sync_record:get(
                unpacked, {ar_data_sync, byte}, StoreID
            )
        )
    ).

%% @doc Restarting storage services preserves chunk bytes and sync records on
%% disk.
lifecycle_preserves_chunks_and_records(_) ->
    {Module, Function, Args} = maps:get(start, arweave_storage:child_spec()),
    {ok, Bridge} = apply(Module, Function, Args),
    unlink(Bridge),
    StoreID = ?DEFAULT_MODULE,
    Offset = ?DATA_CHUNK_SIZE,
    Chunk = binary:copy(<<42>>, Offset),
    MetadataOwner = ets:info(arweave_storage, owner),
    try
        ?assertEqual(
            {ok, unpacked},
            arweave_storage:put_chunk(Offset, Chunk, unpacked, StoreID)
        ),
        ok = arweave_storage:add_footprint(Offset, unpacked, StoreID),
        ?assertEqual({Offset, Chunk}, arweave_storage:get_chunk(0, StoreID))
    after
        gen_server:stop(Bridge)
    end,
    ?assertEqual(undefined, whereis(arweave_storage_runtime_sup)),
    ?assertEqual(MetadataOwner, ets:info(arweave_storage, owner)),
    {ok, Bridge2} = apply(Module, Function, Args),
    unlink(Bridge2),
    try
        ?assertEqual({Offset, Chunk}, arweave_storage:get_chunk(0, StoreID)),
        ?assertEqual(
            {true, unpacked},
            arweave_storage:is_recorded(
                arweave_storage:get_footprint_offset(Offset),
                any_packing,
                {ar_data_sync, footprint},
                StoreID
            )
        ),
        ?assertEqual(
            ar_chunk_storage,
            arweave_storage_chunk_storage:sync_record_id(unpacked)
        ),
        ?assertEqual(
            ar_chunk_storage_replica_2_9_1_unpacked,
            arweave_storage_chunk_storage:sync_record_id(unpacked_padded)
        ),
        ?assertEqual(
            ar_chunk_storage_replica_2_9_5_entropy,
            arweave_storage:entropy_sync_record_id()
        )
    after
        gen_server:stop(Bridge2)
    end.

%% @doc Corruption helpers alter bytes but retain the record, and file listing
%% follows the configured data directory.
raw_chunk_helpers_preserve_records(_) ->
    {ok, _} = arweave_storage:activate(),
    StoreID = ?DEFAULT_MODULE,
    Offset = ?DATA_CHUNK_SIZE,
    Chunk = binary:copy(<<42>>, Offset),
    Replacement = binary:copy(<<43>>, Offset),
    ?assertEqual(
        {ok, unpacked},
        arweave_storage:put_chunk(Offset, Chunk, unpacked, StoreID)
    ),
    ?assertMatch(
        {ok, _},
        arweave_storage:internal_write_chunk(Offset, Replacement, StoreID)
    ),
    ?assertEqual({Offset, Replacement}, arweave_storage:get_chunk(0, StoreID)),
    {_Start, Filepath, _, _} = arweave_storage:locate_chunk_on_disk(
        Offset, StoreID
    ),
    ?assertEqual(
        [binary_to_list(Filepath)], arweave_storage:list_chunk_files(StoreID)
    ),
    ?assertEqual(ok, arweave_storage:internal_erase_chunk(Offset, StoreID)),
    ?assertEqual(
        true,
        arweave_storage:is_recorded(
            Offset, any_packing, {ar_chunk_storage, byte}, StoreID
        )
    ),
    ?assertEqual(not_found, arweave_storage:get_chunk(0, StoreID)).

%% @doc Storage publishes recorded data sizes without requiring the mining
%% statistics process.
data_sizes_without_mining(_) ->
    {ok, _} = arweave_storage:activate(),
    ?assertEqual(undefined, whereis(ar_mining_stats)),
    ok = ar_events:subscribe(chunk_storage),
    StoreID = ?DEFAULT_MODULE,
    Chunk = ?DATA_CHUNK_SIZE,
    ok = arweave_storage:add_sync_record(
        Chunk, 0, unpacked, {ar_data_sync, byte}, StoreID
    ),
    Name = arweave_storage_sync_record:name(StoreID),
    gen_server:cast(Name, store_state),
    _ = sys:get_state(Name),
    DataSize = {StoreID, unpacked, undefined, Chunk},
    ?assert(lists:member(DataSize, arweave_storage:get_data_sizes())),
    receive
        {event, chunk_storage, {data_size, DataSize}} -> ok
    after 5000 ->
        ct:fail(data_size_event_missing)
    end.
