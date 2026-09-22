%%% Isolated storage fixtures for Common Test suites.
-module(arweave_storage_ct_util).
-export([
    init_suite/1,
    end_suite/1,
    init_case/2,
    end_case/2,
    with_mocks/2,
    with_disk_stores/4,
    start_sync_record/1,
    add_chunks_to_sync_record/2,
    chunk_samples/2,
    expected_samples/2,
    stop_record/1
]).
-include_lib("arweave/include/ar.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

init_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_suite(Config) ->
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_case(Name, Config) ->
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

end_case(_, Config) ->
    arweave_storage:close_chunk_files(?DEFAULT_MODULE),
    ok = arweave_storage:deactivate(),
    gen_server:stop(?config(events, Config)),
    gen_server:stop(?config(chunk_events, Config)),
    gen_server:stop(?config(kv, Config)),
    arweave_config:internal_restore(?config(config_snapshot, Config)).

stop_record(StoreID) ->
    case whereis(arweave_storage_sync_record:name(StoreID)) of
        undefined -> ok;
        PID -> gen_server:stop(PID)
    end.

with_mocks([], Fun) ->
    Fun();
with_mocks([{Module, Expectations} | Rest], Fun) ->
    meck:new(Module, [passthrough]),
    try
        lists:foreach(
            fun({Name, Implementation}) ->
                meck:expect(Module, Name, Implementation)
            end,
            Expectations
        ),
        with_mocks(Rest, Fun)
    after
        meck:unload(Module)
    end.

expected_samples(not_found, _) ->
    lists:duplicate(9, not_found);
expected_samples(Chunk, Offset) ->
    lists:duplicate(9, {Offset, Chunk}).

chunk_samples(Offset, StoreID) ->
    %% Sample both edges and interior bytes of the same chunk.
    [
        arweave_storage:get_chunk(Byte, StoreID)
     || Byte <- [
            Offset - 1,
            Offset - 2,
            Offset - ?DATA_CHUNK_SIZE,
            Offset - ?DATA_CHUNK_SIZE + 1,
            Offset - ?DATA_CHUNK_SIZE + 2,
            Offset - ?DATA_CHUNK_SIZE div 2,
            Offset - ?DATA_CHUNK_SIZE div 2 + 1,
            Offset - ?DATA_CHUNK_SIZE div 2 - 1,
            Offset - ?DATA_CHUNK_SIZE div 3
        ]
    ].

%% Run Fun with the given stores resolving to storage modules under the case's
%% data dir, so their sync record servers keep a database, covering Range,
%% plus any extra mocks.
with_disk_stores(StoreIDs, {RangeStart, RangeEnd} = Range, ExtraMocks, Fun) ->
    with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun(StoreID) ->
                    case lists:member(StoreID, StoreIDs) of
                        true -> {RangeStart, RangeEnd, unpacked};
                        false -> meck:passthrough([StoreID])
                    end
                end},
                {info, fun(StoreID) ->
                    case lists:member(StoreID, StoreIDs) of
                        true ->
                            #store_info{
                                id = StoreID,
                                padded_range = Range,
                                path = filename:join([
                                    arweave_config:get([data_dir]),
                                    "storage_modules",
                                    atom_to_list(StoreID)
                                ])
                            };
                        false ->
                            meck:passthrough([StoreID])
                    end
                end}
            ]}
            | ExtraMocks
        ],
        Fun
    ).

start_sync_record(StoreID) ->
    arweave_storage_sync_record:start_link(
        arweave_storage_sync_record:name(StoreID), StoreID
    ).

%% Record the chunks, given by end offset and packing, as synced in the
%% store's {ar_data_sync, byte} sync record.
add_chunks_to_sync_record(Chunks, StoreID) ->
    lists:foreach(
        fun({EndOffset, Packing}) ->
            ok = arweave_storage:add_sync_record(
                EndOffset,
                EndOffset - ?DATA_CHUNK_SIZE,
                Packing,
                {ar_data_sync, byte},
                StoreID
            )
        end,
        Chunks
    ).
