-module(ar_data_sync_footprint_migration_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include("ar.hrl").

%% The key of the store's own database that marks its footprint record built.
-define(FOOTPRINT_RECORD_INIT_CURSOR_KEY, <<"footprint_record_init_cursor">>).

footprint_record_rebuilt_on_start_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_footprint_record_rebuilt_on_start/0}.

%% @doc A store that starts without a migrated footprint record rebuilds it
%% from its {ar_data_sync, byte} sync record, and the rebuilt record equals
%% the one the sync path built chunk by chunk.
test_footprint_record_rebuilt_on_start() ->
    Addr = ar_test_node:generate_address(main),
    Packing = ar_test_node:storage_module_packing(Addr, 0),
    %% Ten partitions: the data sits in the first, so the rebuild also walks
    %% and skips partitions without any.
    Module = {0, 10 * ?PARTITION_SIZE, Packing},
    Wallet = ar_test_data_sync:setup_main_node(
        #{ addr => Addr, [storage_modules] => [Module] }),
    #store_info{id = StoreID} = arweave_storage:store_info(Module),
    Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 3)],
    #{ tx := TX } = ar_test_data_sync:make_fixed_data_tx(Wallet, Chunks),
    B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
    Proofs = ar_test_data_sync:post_proofs(main, B, TX, Chunks),
    EndOffsets = lists:sort([Offset || {Offset, _} <- Proofs]),
    %% The last chunk ends at the weave size and stays in the disk pool; the
    %% two before it mature into the store.
    MatureEndOffsets = lists:droplast(EndOffsets),
    lists:foreach(fun(_) ->
        ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [])
    end, lists:seq(1, ?SEARCH_SPACE_UPPER_BOUND_DEPTH)),
    ?assertEqual(ok, ar_test_await:until(chunks_mature,
        fun() -> ar_disk_pool:get_threshold() > lists:max(MatureEndOffsets) end)),
    %% The mature chunks reach the footprint record once enciphered, leaving
    %% no transitional unpacked_padded chunk behind.
    ?assertEqual(ok, ar_test_await:until(chunks_in_footprint_record, fun() ->
        ar_intervals:is_empty(byte_record(unpacked_padded, StoreID)) andalso
            lists:all(
                fun(Offset) ->
                    arweave_storage_footprint_record:is_recorded(Offset, StoreID) =/= false
                end,
                MatureEndOffsets)
    end)),
    Expected = footprint_record_by_packing(Packing, StoreID),
    ?assertNotEqual([], element(2, Expected)),

    %% Lose the record and the migration mark, then restart the store.
    ok = arweave_storage:cut_sync_record(0, {ar_data_sync, footprint}, StoreID),
    ?assertEqual({[], []}, footprint_record_by_packing(Packing, StoreID)),
    ok = ar_kv:delete({sync_record, StoreID}, ?FOOTPRINT_RECORD_INIT_CURSOR_KEY),
    Name = ar_data_sync:name(StoreID),
    OldPID = whereis(Name),
    exit(OldPID, kill),
    ?assertEqual(ok, ar_test_await:until(data_sync_restarted, fun() ->
        case whereis(Name) of
            PID when is_pid(PID), PID =/= OldPID -> true;
            _ -> false
        end
    end)),
    ?assertEqual(ok, ar_test_await:until(footprint_record_initialized,
        fun() -> arweave_storage:is_footprint_record_initialized(StoreID) end)),
    ?assertEqual(Expected, footprint_record_by_packing(Packing, StoreID)).

byte_record(Packing, StoreID) ->
    arweave_storage_sync_record:get(Packing, {ar_data_sync, byte}, StoreID).

footprint_record(Packing, StoreID) ->
    arweave_storage_sync_record:get(Packing, {ar_data_sync, footprint}, StoreID).

%% The store's packing view and the packing-agnostic view, as lists.
footprint_record_by_packing(Packing, StoreID) ->
    {ar_intervals:to_list(footprint_record(any_packing, StoreID)),
        ar_intervals:to_list(footprint_record(Packing, StoreID))}.
