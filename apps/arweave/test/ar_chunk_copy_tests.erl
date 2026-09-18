%%% Local-copy admission is independent of network-sync download limits.
-module(ar_chunk_copy_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

%% @doc Local copying still acquires its device lock and dispatches with
%% downloads disabled.
copy_with_downloads_disabled_test_() ->
    %% One full chunk exists only in the source, so copying must dispatch it.
    End = ?DATA_CHUNK_SIZE,
    ar_test_util:with_mocked(copy_source_mocks(End), fun() ->
        arweave_config:internal_with_test_config(fun() ->
            ok = arweave_config:set([sync, max_download_rate], 0),
            ?assertNot(arweave_sync:enabled()),
            ?assertEqual(ok, ar_chunk_copy:start_copy(target_store)),
            ok = ar_test_await:until(local_copy_complete, fun() ->
                meck:called(
                    ar_events, send, [chunk_copy, {complete, target_store}]
                )
            end),
            ?assert(
                meck:called(
                    ar_device_lock, acquire_lock, [sync, target_store, paused]
                )
            ),
            ?assertEqual(
                1,
                meck:num_calls(
                    ar_chunk_copy_worker,
                    run,
                    [{0, End, source_store, target_store}]
                )
            ),
            ?assertNot(arweave_sync:enabled())
        end)
    end).

%% @doc Starting a copy without the copy server returns ignore.
copy_without_server_test_() ->
    without_copy_server(fun() ->
        ?assertEqual(undefined, whereis(ar_chunk_copy)),
        ?assertEqual(ignore, ar_chunk_copy:start_copy(target_store))
    end).

%% @doc Model a source chunk missing from the target without real disk I/O.
copy_source_mocks(End) ->
    %% Background storage workers must still be able to use their real modules.
    [
        {arweave_storage, intersecting_stores, fun
            (0, ChunkEnd, any_packing) when ChunkEnd =:= End ->
                [source_module];
            (Start, ChunkEnd, Packing) ->
                meck:passthrough([Start, ChunkEnd, Packing])
        end},
        {arweave_storage, store_info, fun
            (target_store) ->
                #store_info{id = target_store, padded_range = {0, End}};
            (source_module) ->
                #store_info{id = source_store};
            (Module) ->
                meck:passthrough([Module])
        end},
        {ar_device_lock, set_device_lock_metric, fun
            (target_store, sync, _Status) ->
                ok;
            (StoreID, Mode, Status) ->
                meck:passthrough([StoreID, Mode, Status])
        end},
        {ar_device_lock, acquire_lock, fun
            (sync, target_store, Status) when
                Status =:= paused; Status =:= active
            ->
                active;
            (Mode, StoreID, Status) ->
                meck:passthrough([Mode, StoreID, Status])
        end},
        {ar_disk_pool, get_threshold, fun() -> End end},
        {arweave_storage, get_next_interval, fun
            (
                synced,
                0,
                ChunkEnd,
                any_packing,
                {ar_data_sync, byte},
                source_store
            ) when
                ChunkEnd =:= End
            ->
                {End, 0};
            (
                synced,
                0,
                ChunkEnd,
                any_packing,
                {ar_data_sync, byte},
                StoreID
            ) when
                ChunkEnd =:= End,
                StoreID =:= target_store;
                ChunkEnd =:= End,
                StoreID =:= ?DEFAULT_MODULE
            ->
                not_found;
            (Status, Start, ChunkEnd, Packing, ID, StoreID) ->
                meck:passthrough([
                    Status, Start, ChunkEnd, Packing, ID, StoreID
                ])
        end},
        {ar_chunk_copy_worker, run, fun
            ({0, ChunkEnd, source_store, target_store}) when
                ChunkEnd =:= End
            ->
                ok;
            (Args) ->
                meck:passthrough([Args])
        end},
        {ar_events, send, fun
            (chunk_copy, {complete, target_store}) -> ok;
            (Topic, Event) -> meck:passthrough([Topic, Event])
        end}
    ].

without_copy_server(Test) ->
    %% Only the missing-server test stops this child; disabling downloads uses
    %% the live configuration and leaves the supervision tree untouched.
    {setup,
        fun() ->
            supervisor:terminate_child(ar_data_sync_sup, ar_chunk_copy)
        end,
        fun(_) -> supervisor:restart_child(ar_data_sync_sup, ar_chunk_copy) end,
        Test}.
