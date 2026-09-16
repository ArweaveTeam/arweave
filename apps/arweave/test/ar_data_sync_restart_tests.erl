-module(ar_data_sync_restart_tests).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_sync.hrl").

storage_module_kill_recovers_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun() -> restart_recovers(module, kill) end}.

default_store_exception_recovers_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT,
        fun() -> restart_recovers(default, exception) end}.

restart_recovers(StoreType, CrashType) ->
    Addr = ar_test_node:generate_address(main),
    Packing = ar_test_node:storage_module_packing(Addr, 0),
    %% Overlapping ranges with different sizes give us two independent writers
    %% for the same chunk, with enough room for genesis and the mined blocks.
    Module = {0, 10 * ?PARTITION_SIZE, Packing},
    OtherModule = {0, 20 * ?PARTITION_SIZE, Packing},
    Wallet = ar_test_data_sync:setup_main_node(
        #{ addr => Addr,
            [storage_modules] => [Module, OtherModule] }),
    StoreID = case StoreType of
        module -> ar_storage_module:id(Module);
        default -> ?DEFAULT_MODULE
    end,
    OtherStoreID = ar_storage_module:id(OtherModule),
    %% The second chunk puts the first strictly below the mature-data bound.
    Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
        crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
    #{ tx := TX } = ar_test_data_sync:make_fixed_data_tx(Wallet, Chunks),
    B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
    [{EndOffset, EncodedProof} | _] =
        ar_test_data_sync:build_proofs(B, TX, Chunks),
    Proof = (ar_test_data_sync:proof_to_expected_fields(EncodedProof))#{
        packing => unpacked },
    lists:foreach(fun(_) ->
        ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [])
    end, lists:seq(1, ?SEARCH_SPACE_UPPER_BOUND_DEPTH)),
    ?assertEqual(ok, ar_test_await:until(chunk_mature,
        fun() -> ar_disk_pool:get_threshold() > EndOffset end)),
    Parent = self(),
    Peer = {1, 1, 1, 1, 1984},
    %% Only replace the network fetch. Proof validation, writes, scheduler,
    %% databases, and the supervised data-sync restart are all real.
    Fetch = fun(#task{ store_id = ID, task_ref = Ref }) ->
        ar_data_sync:store_fetched_chunk(ID, Peer, EndOffset - 1, Proof, Ref),
        Parent ! {handed_off, Ref},
        ar_sync_scheduler:task_fetch_completed(
            Ref, ?DATA_CHUNK_SIZE, #fetch_timing{})
    end,
    ar_test_node:run_with_mocked([main], [{ar_sync_fetch_worker, run, Fetch}],
        fun() ->
            test_restart(StoreID, OtherStoreID, CrashType, Peer, EndOffset,
                Proof)
        end).

test_restart(StoreID, OtherStoreID, CrashType, Peer, EndOffset, Proof) ->
    RootPID = whereis(ar_sup),
    SchedulerPID = whereis(ar_sync_scheduler),
    Name = ar_data_sync:name(StoreID),
    OldPID = whereis(Name),
    OtherPID = whereis(ar_data_sync:name(OtherStoreID)),
    ?assert(is_pid(RootPID)),
    ?assert(is_pid(SchedulerPID)),
    Task = #task{ store_id = StoreID, offset = EndOffset - ?DATA_CHUNK_SIZE,
        sources = [#task_source{ peer = Peer }] },
    ok = sys:suspend(OtherPID),
    try
        ok = sys:suspend(OldPID),
        ar_data_sync:store_fetched_chunk(OtherStoreID, Peer, EndOffset - 1,
            Proof, undefined),
        case CrashType of
            exception ->
                %% Queue a malformed internal message before the real handoff
                %% to exercise terminate/2 as well as supervisor recovery.
                gen_server:cast(OldPID,
                    {store_fetched_chunk, Peer, 0, #{}, undefined});
            kill -> ok
        end,
        ?assertEqual({ok, 1},
            ar_sync_scheduler:claim_and_enqueue(StoreID, [Task])),
        receive {handed_off, _} -> ok end,
        ?assertEqual(ok, ar_test_await:sync_fetches_drained(SchedulerPID)),
        ?assertEqual(1, ar_sync_chunk_cache:size(StoreID)),
        ?assertEqual(1, ar_sync_chunk_cache:size(OtherStoreID)),
        case CrashType of
            kill -> exit(OldPID, kill);
            exception -> ok = sys:resume(OldPID)
        end,
        ?assertEqual(ok, ar_test_await:until(data_sync_restarted, fun() ->
            case whereis(Name) of
                PID when is_pid(PID), PID =/= OldPID ->
                    ets:lookup(ar_data_sync_state,
                        {chunk_cache_owner, StoreID}) =:=
                            [{{chunk_cache_owner, StoreID}, PID}];
                _ -> false
            end
        end)),
        ?assertEqual(RootPID, whereis(ar_sup)),
        ?assertEqual(SchedulerPID, whereis(ar_sync_scheduler)),
        ?assertEqual(OtherPID, whereis(ar_data_sync:name(OtherStoreID))),
        ?assertEqual(0, ar_sync_chunk_cache:size(StoreID)),
        ?assertEqual(1, ar_sync_chunk_cache:size(OtherStoreID)),
        %% The lost write's range must be claimable and writable again.
        ?assertEqual({ok, 1},
            ar_sync_scheduler:claim_and_enqueue(StoreID, [Task])),
        receive {handed_off, _} -> ok end,
        ?assertEqual(ok, ar_test_await:chunk_recorded(main, EndOffset,
            #{ store_id => StoreID })),
        {ok, Metadata, _, StoredChunk} =
            ar_data_sync:read_chunk_with_full_metadata(EndOffset - 1, StoreID),
        {true, StoredPacking} = ar_sync_record:is_recorded(
            EndOffset, ar_data_sync, StoreID),
        ?assertEqual({ok, maps:get(chunk, Proof)}, ar_packing_server:unpack(
            StoredPacking, EndOffset, Metadata#chunk_metadata.tx_root,
            StoredChunk, Metadata#chunk_metadata.chunk_size)),
        ok = sys:resume(OtherPID),
        ?assertEqual(ok, ar_test_await:chunk_recorded(main, EndOffset,
            #{ store_id => OtherStoreID })),
        ?assertEqual(ok, ar_test_await:sync_fetches_drained(SchedulerPID)),
        ?assertEqual(ok, ar_test_await:until(chunk_caches_drained, fun() ->
            ar_sync_chunk_cache:size() =:= 0
        end))
    after
        catch sys:resume(OldPID),
        catch sys:resume(OtherPID)
    end.
