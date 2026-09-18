-module(ar_tx_blacklist_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").

-import(ar_test_node, [
        sign_v1_tx/2, random_v1_data/1]).

%% Mock the refresh interval on every peer and the local node via
%% `test_with_all_nodes_mocked/3': the blacklist gen_server reads
%% `?MODULE:refresh_interval_ms()' on its first `handle_cast', so the mock
%% must be live before any node's arweave app starts.
uses_blacklists_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        [{arweave_constants, height_2_9_6, fun() -> infinity end},
         {ar_tx_blacklist, refresh_interval_ms, fun() -> 2000 end}],
        fun test_uses_blacklists/0,
        ?TEST_NODE_TIMEOUT
    ).

test_uses_blacklists() ->
    {
        BlacklistFiles,
        B0,
        Wallet,
        TXs,
        GoodTXIDs,
        BadTXIDs,
        V1TX,
        GoodOffsets,
        BadOffsets,
        DataTrees,
        {BlocklistPort, BlocklistReservation, BlocklistRoutes}
    } = setup(),
    WhitelistFile = random_filename(),
    ok = file:write_file(WhitelistFile, <<>>),
    RewardAddr = ar_test_node:generate_address(main),
    StorageModules = blacklist_storage_modules(RewardAddr),
    Config = arweave_config:internal_snapshot(),
    try
        ar_test_node:start(#{ b0 => B0, addr => RewardAddr,
            config => #{
                [transactions, blocklist, files] =>
                    [list_to_binary(File) || File <- BlacklistFiles],
                [transactions, allowlist, files] => [list_to_binary(WhitelistFile)],
                [transactions, blocklist, urls] =>
                    blocklist_urls(BlocklistPort),
                [features, pack_served_chunks] => true
            },
            [storage_modules] => ar_test_node:storage_module_configs(StorageModules)
        }),
        %% The node boot restarts ranch, so the stub serving the blocklist
        %% URLs goes up only now, on the port reserved in setup/0.
        {ok, _, BlocklistRef, BlocklistTable} = ar_test_http_server:start(
            BlocklistRoutes, #{ reservation => BlocklistReservation }),
        ar_test_node:connect_to_peer(peer1),
        BadV1TXIDs = [V1TX#tx.id],
        lists:foreach(
            fun({TX, Height}) ->
                ar_test_node:assert_post_tx_to_peer(peer1, TX),
                ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX])),
                case Height == length(TXs) of
                    true ->
                        ar_test_node:assert_post_tx_to_peer(peer1, V1TX),
                        ?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [V1TX]));
                    _ ->
                        ok
                end,
                ar_test_node:mine(peer1),
                upload_data([TX], DataTrees),
                ?assertMatch({ok, _}, ar_test_await:node_height(main, Height))
            end,
            lists:zip(TXs, lists:seq(1, length(TXs)))
        ),
        assert_present_txs(GoodTXIDs),
        assert_present_txs(BadTXIDs), % V2 headers must not be removed.
        assert_removed_txs(BadV1TXIDs),
        assert_present_offsets(GoodOffsets),
        assert_removed_offsets(BadOffsets),
        assert_removed_chunks(StorageModules, BadOffsets),
        assert_does_not_accept_offsets(BadOffsets),
        %% Add a new transaction to the blacklist, add a blacklisted transaction to whitelist.
        ok = file:write_file(lists:nth(3, BlacklistFiles), <<>>),
        ok = file:write_file(WhitelistFile, arweave_util:encode(lists:nth(2, BadTXIDs))),
        ok = file:write_file(lists:nth(4, BlacklistFiles), io_lib:format("~s~n~s",
                [arweave_util:encode(hd(GoodTXIDs)), arweave_util:encode(V1TX#tx.id)])),
        [UnblacklistedOffsets, WhitelistOffsets | BadOffsets2] = BadOffsets,
        RestoredOffsets = [UnblacklistedOffsets, WhitelistOffsets] ++
                [lists:nth(6, lists:reverse(BadOffsets))],
        BadOffsets3 = BadOffsets2 -- [lists:nth(6, lists:reverse(BadOffsets))],
        [_UnblacklistedTXID, _WhitelistTXID | BadTXIDs2] = BadTXIDs,
        %% Expect the transaction data to be resynced.
        assert_present_offsets(RestoredOffsets),
        %% Expect the freshly blacklisted transaction to be erased.
        assert_present_txs([hd(GoodTXIDs)]), % V2 headers must not be removed.
        assert_removed_offsets([hd(GoodOffsets)]),
        assert_does_not_accept_offsets([hd(GoodOffsets)]),
        %% Expect the previously blacklisted transactions to stay blacklisted.
        assert_present_txs(BadTXIDs2), % V2 headers must not be removed.
        assert_removed_txs(BadV1TXIDs),
        assert_removed_offsets(BadOffsets3),
        assert_does_not_accept_offsets(BadOffsets3),
        %% Blacklist the last transaction. Fork the weave. Assert the blacklisted offsets are moved.
        ar_test_node:disconnect_from(peer1),
        TX = ar_test_node:sign_tx(Wallet, #{ data => crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
                last_tx => ar_test_node:get_tx_anchor(peer1) }),
        ar_test_node:assert_post_tx_to_peer(main, TX),
        ar_test_node:mine(),
        {ok, [{_, WeaveSize, _} | _]} = ar_test_await:node_height(main, length(TXs) + 1),
        assert_present_offsets([[WeaveSize]]),
        ok = file:write_file(lists:nth(3, BlacklistFiles), arweave_util:encode(TX#tx.id)),
        assert_removed_offsets([[WeaveSize]]),
        TX2 = sign_v1_tx(Wallet, #{ data => random_v1_data(2 * ?DATA_CHUNK_SIZE),
                last_tx => ar_test_node:get_tx_anchor(peer1) }),
        ar_test_node:assert_post_tx_to_peer(peer1, TX2),
        ar_test_node:mine(peer1),
        ?assertMatch({ok, _}, ar_test_await:node_height(peer1, length(TXs) + 1)),
        ar_test_node:assert_post_tx_to_peer(peer1, TX),
        ar_test_node:mine(peer1),
        ?assertMatch({ok, _}, ar_test_await:node_height(peer1, length(TXs) + 2)),
        ar_test_node:connect_to_peer(peer1),
        {ok, [{_, WeaveSize2, _} | _]} = ar_test_await:node_height(main, length(TXs) + 2),
        assert_removed_offsets([[WeaveSize2]]),
        assert_present_offsets([[WeaveSize]]),
        ok = ar_test_http_server:stop(BlocklistRef, BlocklistTable)
    after
        teardown(Config)
    end.

setup() ->
    {B0, Wallet} = setup(peer1),
    {TXs, DataTrees} = create_txs(Wallet),
    TXIDs = [TX#tx.id || TX <- TXs],
    BadTXIDs = [lists:nth(1, TXIDs), lists:nth(3, TXIDs)],
    V1TX = sign_v1_tx(Wallet, #{ data => random_v1_data(3 * ?DATA_CHUNK_SIZE),
            last_tx => ar_test_node:get_tx_anchor(peer1), reward => ?AR(10000) }),
    DataSizes = [TX#tx.data_size || TX <- TXs],
    S0 = B0#block.block_size,
    [S1, S2, S3, S4, S5, S6, S7, S8 | _] = DataSizes,
    BadOffsets = [S0 + O || O <- [S1, S1 + S2 + S3, % Blacklisted in the file.
            S1 + S2 + S3 + S4 + S5,
            S1 + S2 + S3 + S4 + S5 + S6 + S7]], % Blacklisted in the endpoint.
    BlacklistFiles = create_files([V1TX#tx.id | BadTXIDs],
            [{S0 + S1 + S2 + S3 + ?DATA_CHUNK_SIZE, S0 + S1 + S2 + S3 + ?DATA_CHUNK_SIZE * 2},
                {S0 + S1 + S2 + S3 + S4 + S5,
                        S0 + S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 5},
                % This one just repeats the range of a blacklisted tx:
                {S0 + S1 + S2 + S3 + S4 + S5 + S6, S0 + S1 + S2 + S3 + S4 + S5 + S6 + S7}
            ]),
    BadTXIDs2 = [lists:nth(5, TXIDs), lists:nth(7, TXIDs)], % The endpoint.
    BadTXIDs3 = [lists:nth(4, TXIDs), lists:nth(6, TXIDs)], % Ranges.
    {BlocklistPort, BlocklistReservation} =
        ar_test_http_server:reserve_port(),
    GoodTXIDs = TXIDs -- (BadTXIDs ++ BadTXIDs2 ++ BadTXIDs3),
    BadOffsets2 =
        lists:map(
            fun(TXOffset) ->
                %% Every TX in this test consists of 10 chunks.
                %% Only every second chunk is uploaded in this test
                %% for (originally) blacklisted transactions.
                [TXOffset - ?DATA_CHUNK_SIZE * I || I <- lists:seq(0, 9, 2)]
            end,
            BadOffsets
        ),
    BadOffsets3 = BadOffsets2 ++ [S0 + O || O <- [S1 + S2 + S3 + ?DATA_CHUNK_SIZE * 2,
            S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE,
            S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 2,
            S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 3,
            S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 4,
            S1 + S2 + S3 + S4 + S5 + ?DATA_CHUNK_SIZE * 5]], % Blacklisted as a range.
    GoodOffsets = [S0 + O || O <- [S1 + S2, S1 + S2 + S3 + S4, S1 + S2 + S3 + S4 + S5 + S6,
            S1 + S2 + S3 + S4 + S5 + S6 + S7 + S8]],
    GoodOffsets2 =
        lists:map(
            fun(TXOffset) ->
                %% Every TX in this test consists of 10 chunks.
                [TXOffset - ?DATA_CHUNK_SIZE * I || I <- lists:seq(0, 9)] -- BadOffsets3
            end,
            GoodOffsets
        ),
    {
        BlacklistFiles,
        B0,
        Wallet,
        TXs,
        GoodTXIDs,
        BadTXIDs ++ BadTXIDs2 ++ BadTXIDs3,
        V1TX,
        GoodOffsets2,
        BadOffsets3,
        DataTrees,
        {BlocklistPort, BlocklistReservation, blocklist_routes(BadTXIDs2)}
    }.

%% @doc Routes for the stub serving the blocklist URLs main polls.
blocklist_routes(BadTXIDs) ->
    Encoded = lists:map(fun arweave_util:encode/1, BadTXIDs),
    #{
        %% Serves empty body.
        {<<"GET">>, <<"/empty">>} => {200, #{}, <<>>},
        %% Serves a valid TX ID (one from the BadTXIDs list).
        {<<"GET">>, <<"/good">>} => {200, #{}, hd(Encoded)},
        %% Serves some valid TX IDs (from the BadTXIDs list) and a line
        %% with invalid Base64URL.
        {<<"GET">>, <<"/bad/and/good">>} =>
            {200, #{}, list_to_binary(
                io_lib:format("~s\nbad base64url \n~s\n", Encoded))}
    }.

%% @doc The blocklist URLs served by the stub on Port; see blocklist_routes/1.
blocklist_urls(Port) ->
    Base = <<"http://localhost:", (integer_to_binary(Port))/binary>>,
    [<<Base/binary, "/empty">>,
     <<Base/binary, "/good">>,
     <<Base/binary, "/bad/and/good">>].

setup(Node) ->
    Wallet = {_, Pub} = ar_test_node:remote_call(Node, ar_wallet, new_keyfile, []),
    RewardAddr = ar_wallet:to_address(Pub),
    [B0] = ar_weave:init([{RewardAddr, ?AR(100000000), <<>>}]),
    StorageModules = blacklist_storage_modules(RewardAddr),
    ar_test_node:start_peer(Node, B0, RewardAddr, #{
        [features, pack_served_chunks] => true,
        [storage_modules] => ar_test_node:storage_module_configs(StorageModules)
    }),
    {B0, Wallet}.

blacklist_storage_modules(RewardAddr) ->
    [{0, 30 * ?MiB, {replica_2_9, RewardAddr}}].

create_txs(Wallet) ->
    lists:foldl(
        fun
            (_, {TXs, DataTrees}) ->
                Chunks =
                    lists:sublist(
                        ar_tx:chunk_binary(?DATA_CHUNK_SIZE,
                                crypto:strong_rand_bytes(10 * ?DATA_CHUNK_SIZE)),
                        10
                    ), % Exclude empty chunk created by chunk_to_binary.
                SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
                    ar_tx:chunks_to_size_tagged_chunks(Chunks)
                ),
                {DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
                TX = ar_test_node:sign_tx(Wallet, #{ format => 2, data_root => DataRoot,
                        data_size => 10 * ?DATA_CHUNK_SIZE, last_tx => ar_test_node:get_tx_anchor(peer1),
                        reward => ?AR(10000), denomination => 1 }),
                {[TX | TXs], maps:put(TX#tx.id, {DataTree, Chunks}, DataTrees)}
        end,
        {[], #{}},
        lists:seq(1, 10)
    ).

create_files(BadTXIDs, [{Start1, End1}, {Start2, End2}, {Start3, End3}]) ->
    Files = [
        {random_filename(), <<>>},
        {random_filename(), <<"bad base64url ">>},
        {random_filename(), arweave_util:encode(lists:nth(2, BadTXIDs))},
        {random_filename(),
            list_to_binary(
                io_lib:format(
                    "~s\nbad base64url \n~s\n~s\n~B,~B\n",
                    lists:map(fun arweave_util:encode/1, BadTXIDs) ++ [Start1, End1]
                )
            )},
        {random_filename(), list_to_binary(io_lib:format("~B,~B\n~B,~B",
                [Start2, End2, Start3, End3]))}
    ],
    lists:foreach(
        fun
            ({Filename, Binary}) ->
                ok = file:write_file(Filename, Binary)
        end,
        Files
    ),
    [Filename || {Filename, _} <- Files].

random_filename() ->
    DataDir = ar_test_node:remote_call(
        peer1, arweave_config, get, [[data_dir]]),
    filename:join(DataDir,
        "ar-tx-blacklist-tests-transaction-blacklist-"
        ++
        binary_to_list(arweave_util:encode(crypto:strong_rand_bytes(32)))).

encode_chunk(Proof) ->
    ar_serialize:jsonify(#{
        chunk => arweave_util:encode(maps:get(chunk, Proof)),
        data_path => arweave_util:encode(maps:get(data_path, Proof)),
        data_root => arweave_util:encode(maps:get(data_root, Proof)),
        data_size => integer_to_binary(maps:get(data_size, Proof)),
        offset => integer_to_binary(maps:get(offset, Proof))
    }).

upload_data(TXs, DataTrees) ->
    lists:foreach(
        fun(TX) ->
            #tx{
                id = TXID,
                data_root = DataRoot,
                data_size = DataSize
            } = TX,
            {DataTree, Chunks} = maps:get(TXID, DataTrees),
            ChunkOffsets = lists:zip(Chunks,
                    lists:seq(?DATA_CHUNK_SIZE, 10 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE)),
            UploadChunks = ChunkOffsets,
            lists:foreach(
                fun({Chunk, Offset}) ->
                    DataPath = ar_merkle:generate_path(DataRoot, Offset - 1, DataTree),
                    {ok, {{<<"200">>, _}, _, _, _, _}} =
                        ar_test_node:post_chunk(peer1, encode_chunk(#{
                            data_root => DataRoot,
                            chunk => Chunk,
                            data_path => DataPath,
                            offset => Offset - 1,
                            data_size => DataSize
                        }))
                end,
                UploadChunks
            )
        end,
        TXs
    ).

assert_present_txs(GoodTXIDs) ->
    ?debugFmt("Waiting until these txids are stored: ~p.",
            [[arweave_util:encode(TXID) || TXID <- GoodTXIDs]]),
    ok = ar_test_await:txs_stored(GoodTXIDs),
    ok = ar_test_await:txs_confirmation_data_stored(GoodTXIDs).

assert_removed_txs(BadTXIDs) ->
    ?debugFmt("Waiting until these txids are removed: ~p.",
            [[arweave_util:encode(TXID) || TXID <- BadTXIDs]]),
    ok = ar_test_await:until(blacklist_removed_txs,
        fun() ->
            lists:all(
                fun(TXID) ->
                    {error, not_found} == ar_data_sync:get_tx_data(TXID)
                            %% Do not use ar_storage:read_tx because the
                            %% transaction is temporarily kept in the disk cache,
                            %% even when blacklisted.
                            andalso ar_kv:get(tx_db, TXID) == not_found
                end,
                BadTXIDs
            )
        end,
        30000
    ),
    %% We have to keep the confirmation data even for blacklisted transactions.
    ok = ar_test_await:txs_confirmation_data_stored(BadTXIDs).

assert_present_offsets(GoodOffsets) ->
    ok = ar_test_await:until(blacklist_present_offsets,
        fun() ->
            lists:all(
                fun(Offset) ->
                    case ar_test_node:get_chunk(main, Offset) of
                        {ok, {{<<"200">>, _}, _, _, _, _}} ->
                            true;
                        _ ->
                            ?debugFmt("Waiting until the end offset ~B is stored.", [Offset]),
                            false
                    end
                end,
                lists:flatten(GoodOffsets)
            )
        end
    ).

assert_removed_offsets(BadOffsets) ->
    ok = ar_test_await:until(blacklist_removed_offsets,
        fun() ->
            lists:all(
                fun(Offset) ->
                    case ar_test_node:get_chunk(main, Offset) of
                        {ok, {{<<"404">>, _}, _, _, _, _}} ->
                            true;
                        _ ->
                            ?debugFmt("Waiting until the end offset ~B is removed.", [Offset]),
                            false
                    end
                end,
                lists:flatten(BadOffsets)
            )
        end,
        60000
    ).

assert_removed_chunks(StorageModules, BadOffsets) ->
    PaddedBadOffsets = lists:usort([
        arweave_constants:get_chunk_padded_offset(BadOffset)
        || BadOffset <- lists:flatten(BadOffsets)
    ]),
    CoveredOffsets = [
        Offset
        || Offset <- PaddedBadOffsets,
            lists:any(fun(Module) -> storage_module_covers_offset(Module, Offset) end,
                StorageModules)
    ],
    ?assertEqual(PaddedBadOffsets, CoveredOffsets),
    ok = ar_test_await:until(blacklist_removed_chunks,
        fun() ->
            RemainingOffsets = remaining_stored_offsets(StorageModules, PaddedBadOffsets),
            case RemainingOffsets of
                [] ->
                    true;
                _ ->
                    ?debugFmt("Waiting until blacklisted chunks are removed. "
                            "Remaining offsets: ~p.",
                            [RemainingOffsets]),
                    false
            end
        end,
        60000
    ).

storage_module_covers_offset(Module, Offset) ->
    #store_info{effective_range = {Start, End}} =
        arweave_storage:store_info(Module),
    Start =< Offset andalso Offset < End.

remaining_stored_offsets(StorageModules, PaddedBadOffsets) ->
    lists:usort(lists:flatten([
        remaining_stored_offsets_for_module(Module, PaddedBadOffsets)
        || Module <- StorageModules
    ])).

remaining_stored_offsets_for_module(Module, PaddedBadOffsets) ->
    #store_info{effective_range = {Start, End}} =
        arweave_storage:store_info(Module),
    #store_info{id = StoreID} =
        arweave_storage:store_info(Module),
    Chunks = arweave_storage:get_chunk_range(Start, End - Start, StoreID),
    ChunkOffsets = [Offset || {Offset, _Chunk} <- Chunks],
    [
        Offset
        || Offset <- PaddedBadOffsets,
            lists:member(Offset, ChunkOffsets)
    ].

assert_does_not_accept_offsets(BadOffsets) ->
    ok = ar_test_await:until(blacklist_rejects_offsets,
        fun() ->
            lists:all(
                fun assert_does_not_accept_offset/1,
                lists:flatten(BadOffsets)
            )
        end,
        60000
    ).

assert_does_not_accept_offset(Offset) ->
    case ar_test_node:get_chunk(main, Offset) of
        {ok, {{<<"404">>, _}, _, _, _, _}} ->
            assert_does_not_accept_offset_proof(Offset);
        Response ->
            ?debugFmt("Waiting until main rejects end offset ~B. Response: ~p.",
                    [Offset, Response]),
            false
    end.

assert_does_not_accept_offset_proof(Offset) ->
    case ar_test_node:get_chunk(peer1, Offset) of
        {ok, {{<<"200">>, _}, _, EncodedProof, _, _}} ->
            Proof = decode_chunk(EncodedProof),
            DataPath = maps:get(data_path, Proof),
            {ok, DataRoot} = ar_merkle:extract_root(DataPath),
            RelativeOffset = ar_merkle:extract_note(DataPath),
            Proof2 = Proof#{
                offset => RelativeOffset - 1,
                data_root => DataRoot,
                data_size => 10 * ?DATA_CHUNK_SIZE
            },
            EncodedProof2 = encode_chunk(Proof2),
            %% The node returns 200 but does not store the chunk.
            case ar_test_node:post_chunk(main, EncodedProof2) of
                {ok, {{<<"200">>, _}, _, _, _, _}} ->
                    case ar_test_node:get_chunk(main, Offset) of
                        {ok, {{<<"404">>, _}, _, _, _, _}} ->
                            true;
                        Response ->
                            ?debugFmt("Waiting until main keeps end offset ~B rejected. "
                                    "Response: ~p.",
                                    [Offset, Response]),
                            false
                    end;
                Response ->
                    ?debugFmt("Waiting until main accepts proof for end offset ~B. "
                            "Response: ~p.",
                            [Offset, Response]),
                    false
            end;
        Response ->
            ?debugFmt("Waiting until peer1 serves end offset ~B. Response: ~p.",
                    [Offset, Response]),
            false
    end.

decode_chunk(EncodedProof) ->
    ar_serialize:json_map_to_poa_map(
        jiffy:decode(EncodedProof, [return_maps])
    ).

teardown(Config) ->
    arweave_config:internal_restore(Config).
