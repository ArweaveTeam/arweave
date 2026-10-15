%%% Tests for data root cleanup and recovery from interrupted removals.
-module(ar_data_roots_tests).
-test_category([fast]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

neighboring_shifted_entries_test_() ->
    with_store(?FUNCTION_NAME, fun(StoreID) ->
        Start = arweave_lib_constants:strict_data_split_threshold() + ?DATA_CHUNK_SIZE,
        BlockSize = ?DATA_CHUNK_SIZE,
        End = Start + BlockSize,
        DataRoot = << 1:256 >>,
        {TXRoot, Tree} = ar_merkle:generate_tree([
            {DataRoot, 10}, {?PADDING_NODE_DATA_ROOT, BlockSize}
        ]),
        TXPath = ar_merkle:generate_path(TXRoot, 0, Tree),
        Shifted = [{DataRoot, 60, Start - 50, TXPath}],
        Neighbor = [{DataRoot, 60, End - 50, TXPath}],
        {ok, _} = ar_data_roots:store_block(Start, BlockSize, TXRoot,
            Shifted, StoreID),
        {ok, _} = ar_data_roots:store_block(End, BlockSize, TXRoot,
            Neighbor, StoreID),
        ?assertEqual({invalid, invalid_tx_start_offset},
            ar_data_roots:verify_stored_block(End, StoreID)),
        ?assertMatch({more, _, 1, [{Start, End, _, removed}]},
            ar_data_roots:verify_stored_blocks(Start, End, 1, StoreID)),
        ?assertEqual({ok, hd(Neighbor)},
            ar_data_roots:get_entry(ar_data_roots:id(DataRoot, 60), StoreID)),
        ?assertEqual({invalid, invalid_tx_start_offset},
            ar_data_roots:verify_stored_block(End, StoreID)),
        ?assertMatch({done, _, 1, [{End, _, _, removed}]},
            ar_data_roots:verify_stored_blocks(End, End + BlockSize,
                10, StoreID)),
        ?assertNot(ar_data_roots:are_synced(End, End + BlockSize,
            TXRoot, StoreID))
    end).

failed_block_key_deletion_test_() ->
    with_store(?FUNCTION_NAME, fun(StoreID) ->
        {TXRoot, BlockSize, _Entries, Shifted} = shifted_block(),
        {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot,
            Shifted, StoreID),
        KeysDB = ar_data_roots:keys_db(StoreID),
        ar_test_util:new_mock(ar_kv, [passthrough]),
        try
            meck:expect(ar_kv, delete,
                fun(DB, _Key) when DB == KeysDB -> {error, injected};
                   (DB, Key) -> meck:passthrough([DB, Key])
                end),
            ?assertEqual({error, injected},
                ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID)),
            %% A resumed removal that fails again names its block.
            ?assertEqual({error, {pending_removal, 0, injected}},
                ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID))
        after
            ar_test_util:unmock_module(ar_kv)
        end,
        ?assert(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
        ?assertEqual({invalid, invalid_total_tx_size},
            ar_data_roots:verify_stored_block(0, StoreID)),
        reopen_store(StoreID),
        ?assertMatch({done, _, 1, [{0, BlockSize, _, removed}]},
            ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID)),
        ?assertNot(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
        ?assertEqual({ok, {TXRoot, BlockSize, Shifted}},
            ar_data_roots:get_removed_block(0, StoreID))
    end).

failed_journal_deletion_preserves_resynced_entries_test_() ->
    with_store(?FUNCTION_NAME, fun(StoreID) ->
        {TXRoot, BlockSize, Entries, Shifted} = shifted_block(),
        {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot,
            Shifted, StoreID),
        MigrationDB = ar_data_sync:migration_db(StoreID),
        ar_test_util:new_mock(ar_kv, [passthrough]),
        try
            meck:expect(ar_kv, delete,
                fun(DB, _Key) when DB == MigrationDB -> {error, injected};
                   (DB, Key) -> meck:passthrough([DB, Key])
                end),
            ?assertEqual({error, injected},
                ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID))
        after
            ar_test_util:unmock_module(ar_kv)
        end,
        ?assertNot(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
        {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot,
            Entries, StoreID),
        reopen_store(StoreID),
        %% Resume pending work even when its block is behind the scan cursor.
        Cursor = BlockSize + 1,
        ?assertMatch({done, Cursor, 1, [{0, BlockSize, _, removed}]},
            ar_data_roots:verify_stored_blocks(Cursor, Cursor, 10, StoreID)),
        ?assert(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
        ?assertEqual(ok, ar_data_roots:verify_stored_block(0, StoreID)),
        ?assertEqual({ok, {TXRoot, BlockSize, Shifted}},
            ar_data_roots:get_removed_block(0, StoreID)),
        ?assertEqual({done, Cursor, 0, []},
            ar_data_roots:verify_stored_blocks(Cursor, Cursor, 10, StoreID))
    end).

missing_removal_backup_test_() ->
    with_store(?FUNCTION_NAME, fun(StoreID) ->
        {TXRoot, BlockSize, _Entries, Shifted} = shifted_block(),
        {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot,
            Shifted, StoreID),
        IndexDB = {data_root_index, StoreID},
        MigrationDB = ar_data_sync:migration_db(StoreID),
        ar_test_util:new_mock(ar_kv, [passthrough]),
        try
            %% Leave the removal pending before any entry is removed.
            meck:expect(ar_kv, delete,
                fun(DB, _Key) when DB == IndexDB -> {error, injected};
                   (DB, Key) -> meck:passthrough([DB, Key])
                end),
            ?assertEqual({error, injected},
                ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID))
        after
            ar_test_util:unmock_module(ar_kv)
        end,
        %% Lose the backup: the entries are read again from the block key.
        ok = ar_kv:delete(MigrationDB, removed_block_key(0)),
        ?assertEqual(not_found, ar_data_roots:get_removed_block(0, StoreID)),
        ?assertMatch({done, _, 1, [{0, BlockSize, _, removed}]},
            ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID)),
        ?assertNot(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
        ?assertEqual({ok, {TXRoot, BlockSize, Shifted}},
            ar_data_roots:get_removed_block(0, StoreID)),
        ?assertEqual([not_found, not_found],
            [ar_data_roots:get_entry(ar_data_roots:id(DataRoot, Size), StoreID)
                || {DataRoot, Size, _, _} <- Shifted])
    end).

failed_removal_write_test_() ->
    %% Fail the backup, initial journal, or phase transition write in turn.
    [with_store(lists:concat([?FUNCTION_NAME, " write ", Write]),
        fun(StoreID) -> test_failed_removal_write(StoreID, Write) end)
        || Write <- [1, 2, 3]].

test_failed_removal_write(StoreID, Write) ->
    {TXRoot, BlockSize, Entries, Shifted} = shifted_block(),
    {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot, Shifted, StoreID),
    MigrationDB = ar_data_sync:migration_db(StoreID),
    Writes = atomics:new(1, []),
    ar_test_util:new_mock(ar_kv, [passthrough]),
    try
        meck:expect(ar_kv, put, fun(DB, Key, Value) ->
            Fail = DB == MigrationDB andalso
                atomics:add_get(Writes, 1, 1) == Write,
            case Fail of
                true -> {error, injected};
                false -> meck:passthrough([DB, Key, Value])
            end
        end),
        ?assertEqual({error, injected},
            ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID))
    after
        ar_test_util:unmock_module(ar_kv)
    end,
    ?assert(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
    reopen_store(StoreID),
    ?assertMatch({done, _, 1, [{0, BlockSize, _, removed}]},
        ar_data_roots:verify_stored_blocks(0, BlockSize, 10, StoreID)),
    ?assertEqual({ok, {TXRoot, BlockSize, Shifted}},
        ar_data_roots:get_removed_block(0, StoreID)),
    ?assertNot(ar_data_roots:are_synced(0, BlockSize, TXRoot, StoreID)),
    {ok, _} = ar_data_roots:store_block(0, BlockSize, TXRoot, Entries, StoreID),
    ?assertEqual(ok, ar_data_roots:verify_stored_block(0, StoreID)),
    ?assertEqual([not_found, not_found],
        [ar_data_roots:get_entry(ar_data_roots:id(DataRoot, Size), StoreID)
            || {DataRoot, Size, _, _} <- Shifted]).

with_store(Name, Test) ->
    {setup, fun open_store/0, fun close_store/1,
        fun(StoreID) ->
            {lists:concat([Name]), {timeout, 30, fun() -> Test(StoreID) end}}
        end}.

open_store() ->
    case ar_kv_sup:start_link() of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    Unique = integer_to_list(erlang:unique_integer([positive])),
    StoreID = {data_roots_removal_test, Unique},
    open_dbs(StoreID),
    StoreID.

open_dbs(StoreID) ->
    [ok = ar_kv:open(#{name => DB,
        path => filename:join(ar_kv:test_db_path(), Name)})
        || {DB, Name} <- store_dbs(StoreID)],
    ok.

close_store(StoreID) ->
    [begin
        ar_kv:test_close(DB),
        ok = ar_kv:test_destroy(Name)
    end || {DB, Name} <- store_dbs(StoreID)],
    ok.

reopen_store(StoreID) ->
    [ar_kv:test_close(DB) || {DB, _} <- store_dbs(StoreID)],
    open_dbs(StoreID).

store_dbs({data_roots_removal_test, Unique} = StoreID) ->
    Prefix = "ar_data_roots_removal_" ++ Unique,
    [{{data_root_index, StoreID}, Prefix ++ "_index"},
     {ar_data_roots:keys_db(StoreID), Prefix ++ "_keys"},
     {ar_data_sync:migration_db(StoreID), Prefix ++ "_migrations"}].

shifted_block() ->
    DataRoot1 = << 1:256 >>,
    DataRoot2 = << 2:256 >>,
    {TXRoot, Tree} = ar_merkle:generate_tree([{DataRoot1, 10}, {DataRoot2, 30}]),
    Path1 = ar_merkle:generate_path(TXRoot, 0, Tree),
    Path2 = ar_merkle:generate_path(TXRoot, 10, Tree),
    Entries = [{DataRoot1, 10, 0, Path1}, {DataRoot2, 20, 10, Path2}],
    Shifted = [{DataRoot1, 5, 5, Path1}, {DataRoot2, 25, 5, Path2}],
    {TXRoot, 30, Entries, Shifted}.

%% The migrations index key under which ar_data_roots keeps a removed block.
removed_block_key(BlockStart) ->
    << "data_root_verification_removed_block",
       BlockStart:?OFFSET_KEY_BITSIZE >>.
