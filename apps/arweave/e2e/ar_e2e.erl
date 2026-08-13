-module(ar_e2e).

-export([fixture_dir/1, fixture_dir/2, install_fixture/3, load_wallet_fixture/1,
         write_chunk_fixture/3, load_chunk_fixture/2]).

-export([delayed_print/2, packing_type_to_packing/2,
         restart_node/3,
         start_source_node/3, start_source_node/4,
         source_node_storage_modules/3, source_node_storage_modules/4,
         max_chunk_offset/1, aligned_partition_size/3,
         assert_recall_byte/3,
         assert_block/2,
         assert_chunks/3, assert_chunks/4, assert_no_chunks/2,
         assert_partition_size/3,
         assert_mine_and_validate/3,
         wait_for_chunks_recorded/3]).

-include_lib("ar.hrl").
-include_lib("ar_consensus.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Set to true to update the chunk fixtures.
%% WARNING: ONLY SET TO true IF YOU KNOW WHAT YOU ARE DOING!
-define(UPDATE_CHUNK_FIXTURES, false).

%% Partition size is 2,000,000 bytes, so round up to a full chunk
-define(ALIGNED_PARTITION_SIZE, 2_097_152).

-spec fixture_dir(atom()) -> binary().
fixture_dir(FixtureType) ->
    Dir = filename:dirname(?FILE),
    filename:join([Dir, "fixtures", atom_to_list(FixtureType)]).

-spec fixture_dir(atom(), [binary()]) -> binary().
fixture_dir(FixtureType, SubDirs) ->
    FixtureDir = fixture_dir(FixtureType),
    filename:join([FixtureDir] ++ SubDirs).

-spec install_fixture(binary(), atom(), string()) -> binary().
install_fixture(FilePath, FixtureType, FixtureName) ->
    FixtureDir = fixture_dir(FixtureType),
    ok = filelib:ensure_dir(FixtureDir ++ "/"),
    FixturePath = filename:join([FixtureDir, FixtureName]),
    file:copy(FilePath, FixturePath),
    FixturePath.

-spec load_wallet_fixture(atom()) -> tuple().
load_wallet_fixture(WalletFixture) ->
    WalletName = atom_to_list(WalletFixture),
    FixtureDir = fixture_dir(wallets),
    FixturePath = filename:join([FixtureDir, WalletName ++ ".json"]),
    Wallet = ar_wallet:load_keyfile(FixturePath),
    Address = ar_wallet:to_address(Wallet),
    WalletPath = ar_wallet:wallet_filepath(arweave_util:encode(Address)),
    file:copy(FixturePath, WalletPath),
    ar_wallet:load_keyfile(WalletPath).

-spec write_chunk_fixture(binary(), non_neg_integer(), binary()) -> ok.
write_chunk_fixture(Packing, EndOffset, Chunk) ->
    FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
    ok = filelib:ensure_dir(FixtureDir ++ "/"),
    FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
    file:write_file(FixturePath, Chunk).

-spec load_chunk_fixture(binary(), non_neg_integer()) -> binary().
load_chunk_fixture(Packing, EndOffset) ->
    FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
    FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
    file:read_file(FixturePath).

packing_type_to_packing(PackingType, Address) ->
    case PackingType of
        replica_2_9 -> {replica_2_9, Address};
        spora_2_6 -> {spora_2_6, Address};
        unpacked -> unpacked
    end.

restart_node(Node, Snapshot, Overrides) when is_map(Overrides) ->
    ar_test_node:stop(Node),
    ok = ar_test_node:remote_call(Node, arweave_config, restore,
                                  [Snapshot#{runtime => false}]),
    ok = ar_test_node:remote_call(Node, arweave_config, force_config,
                                  [Overrides]),
    ok = ar_test_node:remote_call(Node, ar, start_dependencies, []),
    ar_test_await:node_joined(Node),
    ok.

start_source_node(Node, PackingType, WalletFixture) ->
    start_source_node(Node, PackingType, WalletFixture, default).

start_source_node(Node, unpacked, _WalletFixture, ModuleSize) ->
    ?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
              [Node, unpacked, _WalletFixture]),
    TempNode = case Node of
                   peer1 -> peer2;
                   peer2 -> peer1
               end,
    {Blocks, _SourceAddr, Chunks} =
        start_source_node(TempNode, spora_2_6, wallet_a),
    {_, StorageModules} = source_node_storage_modules(Node, unpacked, wallet_a, ModuleSize),
    [B0, _, {TX2, _} | _] = Blocks,
    ar_test_node:start_other_node(Node, B0, #{
                                              [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(TempNode))],
                                              [storage_modules] => StorageModules,
                                              [join, auto] => true
                                             }, true),
    InitialSnapshot = ar_test_node:remote_call(
                        Node, arweave_config, snapshot, []),

    ?LOG_INFO("Source node ~p started.", [Node]),

    ok = ar_test_await:http_chunks_recorded(Node, 0, 4*?ALIGNED_PARTITION_SIZE),

    assert_chunks(Node, unpacked, Chunks),

    ?LOG_INFO("Source node ~p assertions passed.", [Node]),

    %% The restart below rejoins with no peers and `start_from_latest_state'.
    %% `/tx/<id>/data' needs both the stored chunk and a tx_index offset lookup
    %% (`ar_data_sync:get_tx_offset/1'); the tx_index entry can lag the chunk, and
    %% if the restart races ahead, the graceful stop persists a tx_index with no
    %% entry for this tx — after restart `get_tx_offset' returns `not_found' and the
    %% endpoint 404s forever with no peer to recover from. `get_tx_data' resolves
    %% and reads locally (no peer proxy), so confirming the endpoint serves here
    %% means the chunk and tx_index are both present before the stop persists them.
    {ok, _} = ar_test_await:http_tx_data(Node, TX2#tx.id),

    ar_test_node:stop(TempNode),

    restart_node(Node, InitialSnapshot, #{
                                          [peers, trusted] => [],
                                          [join, start_from_latest_state] => true,
                                          [storage_modules] => StorageModules,
                                          [join, auto] => true
                                         }),

    %% pack_served_chunks is not enabled but the data is stored unpacked, so we should
    %% return it. After the restart-and-rejoin above the endpoint can transiently
    %% 404 until the stored chunks become servable, so wait for the 200.
    {ok, Data} = ar_test_await:http_tx_data(Node, TX2#tx.id),
    {ok, ExpectedData} = load_chunk_fixture(
                           unpacked, ?ALIGNED_PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE)),
    ExpectedData = arweave_util:decode(Data),

    ?LOG_INFO("Source node ~p restarted.", [Node]),

    {Blocks, not_set, Chunks};
start_source_node(Node, PackingType, WalletFixture, ModuleSize) ->
    ?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
              [Node, PackingType, WalletFixture]),
    {Wallet, StorageModules} = source_node_storage_modules(
                                 Node, PackingType, WalletFixture, ModuleSize),
    RewardAddr = ar_wallet:to_address(Wallet),

    [B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ?ALIGNED_PARTITION_SIZE),

    ExpectedNodeName = ar_test_node:peer_name(Node),
    BaseConfig = #{
                   [peers, trusted] => [],
                   [join, start_from_latest_state] => true,
                   [storage_modules] => StorageModules,
                   [join, auto] => true,
                   [mining, address] => RewardAddr
                  },
    %% For replica_2_9 sources, prepare entropy for every module before any
    %% cross-module copy runs. Otherwise the copy can read a chunk whose
    %% encipher is still pending its module's entropy, get chunk_storage
    %% `not_found' and invalidate a valid record
    %% (ar_chunk_copy_worker:read_and_post_chunk) — dropping overlap chunks and
    %% leaving the partition short. Starting with syncing disabled keeps
    %% ar_chunk_copy from running; once entropy is prepared, restart with sync on.
    case PackingType of
        replica_2_9 ->
            ExpectedNodeName = ar_test_node:start_other_node(
                Node, B0, BaseConfig#{ [sync, max_download_rate] => 0 }, true),
            Snapshot = ar_test_node:remote_call(Node, arweave_config, snapshot, []),
            ar_test_await:all_entropy_prepared(Node),
            restart_node(Node, Snapshot, #{ [sync, max_download_rate] => infinity });
        _ ->
            ExpectedNodeName = ar_test_node:start_other_node(Node, B0, BaseConfig, true)
    end,

    %% Block until every genesis chunk is recorded fully-packed in the
    %% source packing. This also confirms entropy is ready: a chunk only
    %% lands in `{ar_data_sync, {replica_2_9, _}}' once entropy has been
    %% XOR'd into its slot, so recording the whole set means the encipher
    %% pipeline has touched everything mining can sample.
    SourcePacking = packing_type_to_packing(PackingType, RewardAddr),
    wait_for_chunks_recorded(Node, SourcePacking,
                             genesis_chunk_offsets(?ALIGNED_PARTITION_SIZE)),

    ?LOG_INFO("Source node ~p started.", [Node]),

    %% Note: small chunks will be padded to 256 KiB. So B1 actually contains 3 chunks of data
    %% and B2 starts at a chunk boundary and contains 1 chunk of data.
    %%
    %% p1, 2097152 to 2883584
    {TX1, B1} = mine_block(Node, Wallet, floor(2.5 * ?DATA_CHUNK_SIZE), infinity),
    %% p1, 2883584 to 3145728
    {TX2, B2} = mine_block(Node, Wallet, floor(0.75 * ?DATA_CHUNK_SIZE), infinity),
    %% p1 to p2, 3145728 to 5242880
    {TX3, B3} = mine_block(Node, Wallet, ?ALIGNED_PARTITION_SIZE, infinity),
    %% p2, 5242880 to 6291456 (disk pool threshold falls in the middle of p2)
    {TX4, B4} = mine_block(Node, Wallet, floor(0.5 * ?ALIGNED_PARTITION_SIZE), 2 * ?DATA_CHUNK_SIZE),
    %% p3, 6291456 to 8388608 (chunks are stored in disk pool)
    {TX5, B5} = mine_block(Node, Wallet, ?ALIGNED_PARTITION_SIZE, 0),

    %% List of {Block, EndOffset, ChunkSize}
    Chunks = [
              %% PaddedEndOffset: 2359296
              {B1, ?ALIGNED_PARTITION_SIZE + ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE},
              %% PaddedEndOffset: 2621440
              {B1, ?ALIGNED_PARTITION_SIZE + (2*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
              %% PaddedEndOffset: 2883584
              {B1, ?ALIGNED_PARTITION_SIZE + floor(2.5 * ?DATA_CHUNK_SIZE), floor(0.5 * ?DATA_CHUNK_SIZE)},
              %% PaddedEndOffset: 3145728
              {B2, ?ALIGNED_PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE), floor(0.75 * ?DATA_CHUNK_SIZE)},
              %% PaddedEndOffset: 3407872
              {B3, ?ALIGNED_PARTITION_SIZE + (5*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
              %% PaddedEndOffset: 3670016
              {B3, ?ALIGNED_PARTITION_SIZE + (6*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
              %% PaddedEndOffset: 3932160
              {B3, ?ALIGNED_PARTITION_SIZE + (7*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
              %% PaddedEndOffset: 4194304
              {B3, ?ALIGNED_PARTITION_SIZE + (8*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}
             ],

    ?LOG_INFO("Source node ~p blocks mined.", [Node]),

    assert_partition_size(Node, 0, SourcePacking),
    assert_partition_size(Node, 1, SourcePacking),
    ok = ar_test_await:http_chunks_recorded(Node, 0, 4*?ALIGNED_PARTITION_SIZE),
    assert_chunks(Node, SourcePacking, Chunks),

    %% Restart the node to allow it to copy chunks between storage modules.
    ar_test_node:restart(Node),
    ?LOG_INFO("Source node ~p restarted.", [Node]),

    assert_partition_size(Node, 0, SourcePacking),
    assert_partition_size(Node, 1, SourcePacking),
    ok = ar_test_await:http_chunks_recorded(Node, 0, 4*?ALIGNED_PARTITION_SIZE),
    assert_chunks(Node, SourcePacking, Chunks),

    %% pack_served_chunks is not enabled so we shouldn't return unpacked data
    {ok, {{<<"404">>, _}, _, _, _, _}} = ar_http:req(#{
                                                       method => get,
                                                       peer => ar_test_node:peer_ip(Node),
                                                       path => "/tx/" ++ binary_to_list(arweave_util:encode(TX1#tx.id)) ++ "/data"
                                                      }),

    ?LOG_INFO("Source node ~p assertions passed.", [Node]),

    %% One extra (priming) block bumps the depth-3 `partition_upper_bound'
    %% from ~3 MB (Max = 0, only partition 0 mineable) to ~5 MB (Max = 1),
    %% making partition 1 mineable. Without it a sink that lacks partition
    %% 0 deadlocks in `assert_mine_and_validate'.
    SourceHeight = ar_test_node:remote_call(Node, ar_node, get_height, []),
    ar_test_node:mine(Node),
    {ok, _} = ar_test_await:node_height(Node, SourceHeight + 1),
    ?LOG_INFO("Source node ~p priming block mined (height ~p).",
              [Node, SourceHeight + 1]),

    {[B0, {TX1, B1}, {TX2, B2}, {TX3, B3}, {TX4, B4}, {TX5, B5}],
     RewardAddr, Chunks}.

max_chunk_offset(Chunks) ->
    lists:foldl(fun({_, EndOffset, _}, Acc) -> max(Acc, EndOffset) end, 0, Chunks).

aligned_partition_size(Node, Partition, Packing) ->
    StorageModulesList = ar_test_node:remote_call(
                             Node, arweave_config, storage_modules, []),
    RepackInPlaceList = ar_test_node:remote_call(
                             Node, arweave_config, repack_modules, [full]),
    %% Include both regular storage modules and repack_in_place modules.
    %% For repack_in_place modules, use the target packing.
    RepackInPlaceModules = [{ModuleStart, ModuleEnd, TargetPacking}
                            || {{ModuleStart, ModuleEnd, _FromPacking}, TargetPacking} <- RepackInPlaceList],
    AllStorageModules = StorageModulesList ++ RepackInPlaceModules,
    PartitionStart = Partition * ar_block:partition_size(),
    PartitionEnd = (Partition + 1) * ar_block:partition_size(),
    StorageModules = filter_storage_modules_by_partition(
                       PartitionStart, PartitionEnd, AllStorageModules),
    StorageModules2 = filter_storage_modules_by_packing(StorageModules, Packing),
    aligned_partition_size2(StorageModules2, PartitionStart, PartitionEnd, 0).

filter_storage_modules_by_partition(PartitionStart, PartitionEnd, Modules) ->
    lists:filter(fun({ModuleStart, ModuleEnd, _Packing}) ->
                         ModuleStart < PartitionEnd andalso ModuleEnd > PartitionStart
                 end, Modules).

filter_storage_modules_by_packing([{_, _, {replica_2_9, _} = Packing} = Module | Modules], unpacked_padded) ->
    [Module | filter_storage_modules_by_packing(Modules, Packing)];
filter_storage_modules_by_packing([{_, _, Packing} = Module | Modules], Packing) ->
    [Module | filter_storage_modules_by_packing(Modules, Packing)];
filter_storage_modules_by_packing([_Module | Modules], Packing) ->
    filter_storage_modules_by_packing(Modules, Packing);
filter_storage_modules_by_packing([], _Packing) ->
    [].

aligned_partition_size2([{ModuleStart, ModuleEnd, Packing} | Modules], PartitionStart, PartitionEnd, Acc) ->
    Overlap = ar_storage_module:get_overlap(Packing),
    ClippedStart = max(ModuleStart, PartitionStart),
    ClippedEnd = min(ModuleEnd, PartitionEnd),
    AlignedModuleStart = max(0, ar_block:get_chunk_padded_offset(ClippedStart) - ?DATA_CHUNK_SIZE),
    AlignedModuleEnd = ar_block:get_chunk_padded_offset(ClippedEnd + Overlap),
    AlignedModuleSize = AlignedModuleEnd - AlignedModuleStart,
    aligned_partition_size2(Modules, PartitionStart, PartitionEnd, Acc + AlignedModuleSize);
aligned_partition_size2([], _PartitionStart, _PartitionEnd, Acc) ->
    Acc.

source_node_storage_modules(Node, PackingType, WalletFixture) ->
    source_node_storage_modules(Node, PackingType, WalletFixture, default).

source_node_storage_modules(_Node, unpacked, _WalletFixture, ModuleSize) ->
    {not_set, source_node_storage_modules(unpacked, ModuleSize)};
source_node_storage_modules(Node, PackingType, WalletFixture, ModuleSize) ->
    Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [WalletFixture]),
    RewardAddr = ar_wallet:to_address(Wallet),
    SourcePacking = packing_type_to_packing(PackingType, RewardAddr),
    {Wallet, source_node_storage_modules(SourcePacking, ModuleSize)}.

source_node_storage_modules(SourcePacking, default) ->
    Size = ar_block:partition_size(),
    lists:map(fun(I) -> {I * Size, (I + 1) * Size, SourcePacking} end,
        lists:seq(0, 4));

source_node_storage_modules(SourcePacking, small) ->
    Size = ar_block:partition_size() div 4,
    %% Put strict data split threshold inside the first storage module.
    [{0, Size * 2, SourcePacking}
    | lists:map(fun(I) -> {I * Size, (I + 1) * Size, SourcePacking} end,
        lists:seq(2, 19))].

mine_block(Node, Wallet, DataSize, IsTemporary) ->
    WeaveSize = ar_test_node:remote_call(Node, ar_node, get_current_weave_size, []),
    Addr = ar_wallet:to_address(Wallet),
    {TX, Chunks} = generate_tx(Node, Wallet, WeaveSize, DataSize),
    B = ar_test_node:post_and_mine(#{ miner => Node, await_on => Node }, [TX]),

    Addr = B#block.reward_addr,

    Proofs = ar_test_data_sync:post_proofs(Node, B, TX, Chunks, IsTemporary),

    ar_test_data_sync:wait_until_syncs_chunks(Node, Proofs, infinity),
    {TX, B}.

generate_tx(Node, Wallet, WeaveSize, DataSize) ->
    Chunks = generate_chunks(Node, WeaveSize, DataSize, []),
    {DataRoot, _DataTree} = ar_merkle:generate_tree(
                              [{ar_tx:generate_chunk_id(Chunk), Offset} || {Chunk, Offset} <- Chunks]
                             ),
    TX = ar_test_node:sign_tx(Node, Wallet, #{
                                              data_size => DataSize,
                                              data_root => DataRoot
                                             }),
    {TX, [Chunk || {Chunk, _} <- Chunks]}.

generate_chunks(Node, WeaveSize, DataSize, Acc) when DataSize > 0 ->
    ChunkSize = min(DataSize, ?DATA_CHUNK_SIZE),
    EndOffset = (length(Acc) * ?DATA_CHUNK_SIZE) + ChunkSize,
    Chunk = ar_test_node:get_genesis_chunk(WeaveSize + EndOffset),
    generate_chunks(Node, WeaveSize, DataSize - ChunkSize, Acc ++ [{Chunk, EndOffset}]);
generate_chunks(_, _, _, Acc) ->
    Acc.

assert_recall_byte(Node, RangeStart, RangeEnd) when RangeStart > RangeEnd ->
    ok;
assert_recall_byte(Node, RangeStart, RangeEnd) ->
    Options = #{ pack => true, packing => unpacked, origin => miner },
    Result = ar_test_node:remote_call(
               Node, ar_data_sync, get_chunk, [RangeStart + 1, Options]),
    case Result of
        {ok, _} ->
            ?LOG_INFO("Recall byte found at ~p", [RangeStart + 1]),
            assert_recall_byte(Node, RangeStart + 1, RangeEnd);
        Error ->
            ?LOG_ERROR([{event, recall_byte_not_found},
                        {recall_byte, RangeStart},
                        {error, Error}])
    end.
assert_block({spora_2_6, Address}, MinedBlock) ->
    ?assertEqual(Address, MinedBlock#block.reward_addr),
    ?assertEqual(0, MinedBlock#block.packing_difficulty);
assert_block({replica_2_9, Address}, MinedBlock) ->
    ?assertEqual(Address, MinedBlock#block.reward_addr),
    ?assertEqual(?REPLICA_2_9_PACKING_DIFFICULTY, MinedBlock#block.packing_difficulty).


%% @doc Compute the expected aligned size of `PartitionNumber' on `Node'
%% (storage-module-aware) and wait until the partition settles there.
assert_partition_size(Node, PartitionNumber, Packing) ->
    Size = aligned_partition_size(Node, PartitionNumber, Packing),
    ?LOG_INFO("~p: Asserting partition ~p,~p is size ~p",
              [Node, PartitionNumber, ar_serialize:encode_packing(Packing, true), Size]),
    ar_test_await:partition_at_size(Node, PartitionNumber, Packing, Size).


assert_mine_and_validate(MinerNode, ValidatorNode, MinerPacking) ->
    CurrentHeight = max(
                      ar_test_node:remote_call(ValidatorNode, ar_node, get_height, []),
                      ar_test_node:remote_call(MinerNode, ar_node, get_height, [])
                     ),
    {ok, _} = ar_test_await:node_height(ValidatorNode, CurrentHeight),
    {ok, _} = ar_test_await:node_height(MinerNode, CurrentHeight),
    ar_test_node:mine(MinerNode),
    {ok, MinerBI} = ar_test_await:node_height(MinerNode, CurrentHeight + 1),
    {ok, MinerBlock} =
        ar_test_node:http_get_block(element(1, hd(MinerBI)), MinerNode),
    assert_block(MinerPacking, MinerBlock),
    {ok, ValidatorBI} = ar_test_await:node_height(
                          ValidatorNode, MinerBlock#block.height),
    {ok, ValidatorBlock} = ar_test_node:http_get_block(
                             element(1, hd(ValidatorBI)), ValidatorNode),
    MinerBlock = ValidatorBlock.

assert_chunks(Node, Packing, Chunks) ->
    assert_chunks(Node, any, Packing, Chunks).

assert_chunks(Node, RequestPacking, Packing, Chunks) ->
    lists:foreach(fun({Block, EndOffset, ChunkSize}) ->
                          assert_chunk(Node, RequestPacking, Packing, Block, EndOffset, ChunkSize)
                  end, Chunks).

assert_chunk(Node, RequestPacking, Packing, _Block, EndOffset, _ChunkSize)
  when ?UPDATE_CHUNK_FIXTURES =:= true ->
    %% Fixture-update mode: fetch the chunk once and overwrite the
    %% fixture on disk. No polling (any 200 will do) and no comparison.
    ?LOG_ERROR("WARNING: Updating chunk fixture! EndOffset: ~p, Packing: ~p",
               [EndOffset, ar_serialize:encode_packing(Packing, true)]),
    {ok, {{<<"200">>, _}, _, EncodedProof, _, _}} =
        ar_test_node:get_chunk(Node, EndOffset, RequestPacking),
    Proof = ar_serialize:json_map_to_poa_map(jiffy:decode(EncodedProof, [return_maps])),
    write_chunk_fixture(Packing, EndOffset, maps:get(chunk, Proof));
assert_chunk(Node, RequestPacking, Packing, Block, EndOffset, ChunkSize) ->
    ?LOG_INFO("Asserting chunk at offset ~p, size ~p", [EndOffset, ChunkSize]),
    {ok, ExpectedPackedChunk} = load_chunk_fixture(Packing, EndOffset),

    %% An `any' request may briefly return an intermediate packing (e.g.
    %% unpacked_padded before replica_2_9 entropy composition finishes),
    %% so poll until the chunk matches the expected packing.
    {ok, Proof} = ar_test_await:http_chunk_matches(
                    Node, EndOffset, #{chunk => ExpectedPackedChunk},
                    #{packing => RequestPacking}),

    ChunkMetadata = #chunk_metadata{
                       tx_root = Block#block.tx_root,
                       tx_path = maps:get(tx_path, Proof),
                       data_path = maps:get(data_path, Proof)
                      },
    ChunkProof = ar_test_node:remote_call(Node, ar_poa, chunk_proof, [ChunkMetadata, EndOffset - 1]),
    ?LOG_INFO([{chunk_proof, ChunkProof}]),
    {true, _} = ar_test_node:remote_call(Node, ar_poa, validate_paths, [ChunkProof]),
    Chunk = maps:get(chunk, Proof),

    ExpectedSize = byte_size(ExpectedPackedChunk),
    ExpectedSize = byte_size(Chunk),
    ExpectedPackedChunk = Chunk,

    {ok, UnpackedChunk} = ar_packing_server:unpack(
                            Packing, EndOffset, Block#block.tx_root, Chunk, ?DATA_CHUNK_SIZE),
    UnpaddedChunk = ar_packing_server:unpad_chunk(
                      Packing, UnpackedChunk, ChunkSize, byte_size(Chunk)),
    ExpectedUnpackedChunk = ar_test_node:get_genesis_chunk(EndOffset),
    ExpectedUnpackedChunk = UnpaddedChunk.

assert_no_chunks(Node, Chunks) ->
    lists:foreach(fun({_Block, EndOffset, _ChunkSize}) ->
                          assert_no_chunk(Node, EndOffset)
                  end, Chunks).

%% @doc Probe offset for each ?DATA_CHUNK_SIZE slot in a `WeaveSize'-byte
%% genesis weave, as `ChunkEnd - ?DATA_CHUNK_SIZE + 1' (the smallest
%% offset `ar_sync_record:is_recorded' resolves to that chunk).
genesis_chunk_offsets(WeaveSize) ->
    [N * ?DATA_CHUNK_SIZE - ?DATA_CHUNK_SIZE + 1
     || N <- lists:seq(1, WeaveSize div ?DATA_CHUNK_SIZE)].

%% @doc Block until every offset in `ChunkOffsets' is recorded under
%% `{ar_data_sync, Packing}' on `Node', checking each offset in turn.
%% This does not guarantee all offsets are recorded simultaneously, so
%% a `chunk_copy_worker' invalidation of an earlier offset while a later
%% one is pending goes unnoticed; re-verify if whole-set consistency
%% matters.
wait_for_chunks_recorded(Node, Packing, ChunkOffsets) ->
    Opts = #{packing => Packing},
    lists:foreach(
      fun(Offset) ->
              case ar_test_await:chunk_recorded(Node, Offset, Opts) of
                  ok -> ok;
                  {error, {timeout, _}} ->
                      erlang:error({timeout, wait_for_chunks_recorded,
                                    [{node, Node},
                                     {packing, ar_serialize:encode_packing(Packing, true)},
                                     {missing_offset, Offset},
                                     {expected_count, length(ChunkOffsets)}]})
              end
      end, ChunkOffsets),
    ?LOG_INFO([{event, chunks_recorded_in_packing},
               {node, Node},
               {packing, ar_serialize:encode_packing(Packing, true)},
               {count, length(ChunkOffsets)}]),
    ok.

assert_no_chunk(Node, EndOffset) ->
    {ok, {{<<"404">>, _}, _, _, _, _}} = ar_test_node:get_chunk(Node, EndOffset, any).

delayed_print(Format, Args) ->
    %% Print the specific flavor of this test since it isn't captured in the test name.
    %% Delay the print by 1 second to allow the eunit output to be flushed.
    spawn(fun() ->
                  timer:sleep(1000),
                  io:fwrite(user, Format, Args)
          end).

%% --------------------------------------------------------------------------------------------
%% Test Data Generation
%% --------------------------------------------------------------------------------------------
write_wallet_fixtures() ->
    Wallets = [wallet_a, wallet_b, wallet_c, wallet_d],
    lists:foreach(fun(Wallet) ->
                          WalletName = atom_to_list(Wallet),
                          ar_wallet:new_keyfile(?DEFAULT_KEY_TYPE, WalletName),
                          install_fixture(
                            ar_wallet:wallet_filepath(Wallet), wallets, WalletName ++ ".json")
                  end, Wallets),
    ok.
