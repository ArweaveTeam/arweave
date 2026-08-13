%%% @doc Common Test suite for the sync-pack-mine end-to-end scenarios.
%%% `init_per_testcase' restarts both peer BEAMs so each case starts
%%% from a fresh VM state, isolating it from cross-test state-leak
%%% races. Each case does its own source-node setup; the per-case CI
%%% shards parallelize the duplicated cost.
-module(ar_sync_pack_mine_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%%====================================================================
%% Suite metadata
%%====================================================================

suite() ->
    %% 35 min per case: source-node setup (~5 min) plus body (~5-15 min).
    %% Small-module mining cases need the headroom — a single-module sink
    %% can take 10-20 min to mine one block.
    [{timetrap, {minutes, 35}}].

all() ->
    [
     %% replica_2_9 source × 3 sinks
     test_replica_2_9_to_replica_2_9_sync_pack_mine,
     test_replica_2_9_to_spora_2_6_sync_pack_mine,
     test_replica_2_9_to_unpacked_sync_pack_mine,

     %% spora_2_6 source × 3 sinks
     test_spora_2_6_to_replica_2_9_sync_pack_mine,
     test_spora_2_6_to_spora_2_6_sync_pack_mine,
     test_spora_2_6_to_unpacked_sync_pack_mine,

     %% unpacked source × 3 sinks
     test_unpacked_to_replica_2_9_sync_pack_mine,
     test_unpacked_to_spora_2_6_sync_pack_mine,
     test_unpacked_to_unpacked_sync_pack_mine,

     %% Edge cases: unpacked source
     test_unpacked_to_replica_2_9_and_unpacked,
     test_unpacked_to_unpacked_and_replica_2_9,
     test_unpacked_entropy_first_replica_2_9,
     test_unpacked_entropy_last_replica_2_9,

     %% Edge cases: spora_2_6 source
     test_spora_2_6_to_replica_2_9_and_unpacked,
     test_spora_2_6_to_unpacked_and_replica_2_9,
     test_spora_2_6_entropy_first_replica_2_9,
     test_spora_2_6_entropy_last_replica_2_9,

     %% Small module: 3 sources × 2 alignments
     test_unpacked_small_module_aligned,
     test_unpacked_small_module_unaligned,
     test_replica_2_9_small_module_aligned,
     test_replica_2_9_small_module_unaligned,
     test_spora_2_6_small_module_aligned,
     test_spora_2_6_small_module_unaligned,

     %% Large module: 3 sources × 2 alignments
     test_unpacked_large_module_aligned,
     test_unpacked_large_module_unaligned,
     test_replica_2_9_large_module_aligned,
     test_replica_2_9_large_module_unaligned,
     test_spora_2_6_large_module_aligned,
     test_spora_2_6_large_module_unaligned,

     %% Disk-pool threshold pairs
     test_unpacked_to_replica_2_9_disk_pool,
     test_unpacked_to_spora_2_6_disk_pool,
     test_spora_2_6_to_replica_2_9_disk_pool,
     test_spora_2_6_to_spora_2_6_disk_pool,
     test_spora_2_6_to_unpacked_disk_pool
    ].

%%====================================================================
%% Setup / teardown
%%====================================================================

init_per_suite(Config) ->
    case os:getenv("ARWEAVE_PROJECT_ROOT") of
        false -> ok;
        Root -> ok = file:set_cwd(Root)
    end,
    ok = arweave_config:start(),
    ok = arweave_limiter:start(),
    ar_test_runner:start_for_tests(e2e),
    ar_test_node:boot_peers(e2e),
    ar_test_node:wait_for_peers(e2e),
    Config.

end_per_suite(_Config) ->
    ar_test_node:stop_peers(e2e),
    ok.

init_per_testcase(_TestCase, Config) ->
    ar_test_node:restart_peer_beam(peer1),
    ar_test_node:restart_peer_beam(peer2),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test cases — basic sync-pack-mine matrix
%%====================================================================

test_replica_2_9_to_replica_2_9_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(replica_2_9), replica_2_9).
test_replica_2_9_to_spora_2_6_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(replica_2_9), spora_2_6).
test_replica_2_9_to_unpacked_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(replica_2_9), unpacked).

test_spora_2_6_to_replica_2_9_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(spora_2_6), replica_2_9).
test_spora_2_6_to_spora_2_6_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(spora_2_6), spora_2_6).
test_spora_2_6_to_unpacked_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(spora_2_6), unpacked).

test_unpacked_to_replica_2_9_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(unpacked), replica_2_9).
test_unpacked_to_spora_2_6_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(unpacked), spora_2_6).
test_unpacked_to_unpacked_sync_pack_mine(_C) ->
    do_sync_pack_mine(setup_source_node(unpacked), unpacked).

%%====================================================================
%% Test cases — edge cases
%%====================================================================

test_unpacked_to_replica_2_9_and_unpacked(_C) ->
    do_unpacked_and_packed_sync_pack_mine(
      setup_source_node(unpacked), {replica_2_9, unpacked}).
test_unpacked_to_unpacked_and_replica_2_9(_C) ->
    do_unpacked_and_packed_sync_pack_mine(
      setup_source_node(unpacked), {unpacked, replica_2_9}).
test_unpacked_entropy_first_replica_2_9(_C) ->
    do_entropy_first_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).
test_unpacked_entropy_last_replica_2_9(_C) ->
    do_entropy_last_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).

test_spora_2_6_to_replica_2_9_and_unpacked(_C) ->
    do_unpacked_and_packed_sync_pack_mine(
      setup_source_node(spora_2_6), {replica_2_9, unpacked}).
test_spora_2_6_to_unpacked_and_replica_2_9(_C) ->
    do_unpacked_and_packed_sync_pack_mine(
      setup_source_node(spora_2_6), {unpacked, replica_2_9}).
test_spora_2_6_entropy_first_replica_2_9(_C) ->
    do_entropy_first_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).
test_spora_2_6_entropy_last_replica_2_9(_C) ->
    do_entropy_last_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).

%%====================================================================
%% Test cases — small/large module variants
%%====================================================================

test_unpacked_small_module_aligned(_C) ->
    do_small_module_aligned_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).
test_unpacked_small_module_unaligned(_C) ->
    do_small_module_unaligned_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).

test_replica_2_9_small_module_aligned(_C) ->
    do_small_module_aligned_sync_pack_mine(
      setup_source_node(replica_2_9), replica_2_9).
test_replica_2_9_small_module_unaligned(_C) ->
    do_small_module_unaligned_sync_pack_mine(
      setup_source_node(replica_2_9), replica_2_9).

test_spora_2_6_small_module_aligned(_C) ->
    do_small_module_aligned_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).
test_spora_2_6_small_module_unaligned(_C) ->
    do_small_module_unaligned_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).

test_unpacked_large_module_aligned(_C) ->
    do_large_module_aligned_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).
test_unpacked_large_module_unaligned(_C) ->
    do_large_module_unaligned_sync_pack_mine(
      setup_source_node(unpacked), replica_2_9).

test_replica_2_9_large_module_aligned(_C) ->
    do_large_module_aligned_sync_pack_mine(
      setup_source_node(replica_2_9), replica_2_9).
test_replica_2_9_large_module_unaligned(_C) ->
    do_large_module_unaligned_sync_pack_mine(
      setup_source_node(replica_2_9), replica_2_9).

test_spora_2_6_large_module_aligned(_C) ->
    do_large_module_aligned_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).
test_spora_2_6_large_module_unaligned(_C) ->
    do_large_module_unaligned_sync_pack_mine(
      setup_source_node(spora_2_6), replica_2_9).

%%====================================================================
%% Test cases — disk pool threshold
%%====================================================================

test_unpacked_to_replica_2_9_disk_pool(_C) ->
    do_disk_pool_threshold(unpacked, replica_2_9).
test_unpacked_to_spora_2_6_disk_pool(_C) ->
    do_disk_pool_threshold(unpacked, spora_2_6).
test_spora_2_6_to_replica_2_9_disk_pool(_C) ->
    do_disk_pool_threshold(spora_2_6, replica_2_9).
test_spora_2_6_to_spora_2_6_disk_pool(_C) ->
    do_disk_pool_threshold(spora_2_6, spora_2_6).
test_spora_2_6_to_unpacked_disk_pool(_C) ->
    do_disk_pool_threshold(spora_2_6, unpacked).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Set up the source node and return `{Blocks, Chunks, SourcePackingType}'.
setup_source_node(PackingType) ->
    SourceNode = peer1,
    SinkNode = peer2,
    ar_test_node:stop(SinkNode),
    ar_test_node:stop(SourceNode),
    {Blocks, _SourceAddr, Chunks} =
        ar_e2e:start_source_node(SourceNode, PackingType, wallet_a),
    {Blocks, Chunks, PackingType}.

do_sync_pack_mine({Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    SinkPacking = start_sink_node(SinkNode, SourceNode, B0, SinkPackingType),

    RangeStart = ar_block:partition_size(),
    RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

    case SinkPackingType of
        unpacked ->
            ok;
        _ ->
            ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking)
    end.

do_unpacked_and_packed_sync_pack_mine(
  {Blocks, _Chunks, SourcePackingType}, {PackingType1, PackingType2}) ->
    ?LOG_INFO([{event, test_unpacked_and_packed_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType},
               {to_packing_type, {PackingType1, PackingType2}}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    {SinkPacking1, SinkPacking2} = start_sink_node(
                                     SinkNode, SourceNode, B0, PackingType1, PackingType2),

    RangeStart1 = ar_block:partition_size(),
    RangeEnd1 = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking1),

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart1, RangeEnd1),
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking1),
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking2),

    MinablePacking = case PackingType1 of
                         unpacked -> SinkPacking2;
                         _ -> SinkPacking1
                     end,
    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, MinablePacking).

do_entropy_first_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_entropy_first_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    Module = {ar_block:partition_size(), 2 * ar_block:partition_size(), SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    BaseOverrides = #{
                      [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                      [join, start_from_latest_state] => true,
                      [storage_modules] => StorageModules,
                      [join, auto] => true,
                      [mining, address] => SinkAddr,
                      [sync, jobs] => 0
                     },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, BaseOverrides, true),
    SinkSnapshot = ar_test_node:remote_call(
                     SinkNode, arweave_config, snapshot, []),

    RangeStart = ar_block:partition_size(),
    RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, RangeStart, RangeEnd),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),

    DeleteOffset1 = RangeStart + ?DATA_CHUNK_SIZE,
    ar_test_node:remote_call(SinkNode, ar_chunk_storage, delete,
                             [DeleteOffset1, StoreID]),
    DeleteOffset2 = DeleteOffset1 + ?DATA_CHUNK_SIZE,
    ar_test_node:remote_call(SinkNode, ar_chunk_storage, delete_chunk,
                             [DeleteOffset2, StoreID]),

    ar_e2e:restart_node(SinkNode, SinkSnapshot,
                        BaseOverrides#{[sync, jobs] => 100}),

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

do_entropy_last_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_entropy_last_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    Module = {ar_block:partition_size(), 2 * ar_block:partition_size(), SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    BaseOverrides = #{
                      [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                      [join, start_from_latest_state] => true,
                      [storage_modules] => StorageModules,
                      [join, auto] => true,
                      [mining, address] => SinkAddr,
                      [packing, entropy, workers] => 0
                     },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, BaseOverrides, true),
    SinkSnapshot = ar_test_node:remote_call(
                     SinkNode, arweave_config, snapshot, []),

    RangeStart = ar_block:partition_size(),
    RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    ar_e2e:assert_partition_size(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),

    ar_e2e:restart_node(SinkNode, SinkSnapshot,
                        BaseOverrides#{[packing, entropy, workers] => 8}),

    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, RangeStart, RangeEnd),
    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

do_small_module_aligned_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_small_module_aligned_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    Module = {ar_block:partition_size(),
        floor(1.5 * ar_block:partition_size()), SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    %% Sync the second half of partition 1
    Overrides = #{
                  [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                  [join, start_from_latest_state] => true,
                  [storage_modules] => StorageModules,
                  [join, auto] => true,
                  [mining, address] => SinkAddr
                 },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, Overrides, true),

    RangeStart = ar_block:partition_size(),
    RangeEnd = floor(1.5 * ar_block:partition_size()),
    Partition = ar_node:get_partition_number(RangeStart),
    RangeSize = ar_e2e:aligned_partition_size(SinkNode, Partition, SinkPacking),

    ar_test_await:partition_at_size(SinkNode, 1, SinkPacking, RangeSize),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 1, 4)),
    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),

    AlignedStart = arweave_util:floor_int(RangeStart, ?DATA_CHUNK_SIZE),
    AlignedEnd = arweave_util:ceil_int(RangeEnd, ?DATA_CHUNK_SIZE) + ?DATA_CHUNK_SIZE,
    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, AlignedStart, AlignedEnd),
    ok = ar_test_await:entropy_not_prepared(SinkNode, StoreID, AlignedEnd, AlignedEnd + ar_block:partition_size()),

    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

do_small_module_unaligned_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_small_module_unaligned_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    Module = {floor(1.5 * ar_block:partition_size()),
        2 * ar_block:partition_size(), SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    Overrides = #{
                  [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                  [join, start_from_latest_state] => true,
                  [storage_modules] => StorageModules,
                  [join, auto] => true,
                  [mining, address] => SinkAddr
                 },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, Overrides, true),

    RangeStart = floor(1.5 * ar_block:partition_size()),
    RangeEnd = 2 * ar_block:partition_size(),
    Partition = ar_node:get_partition_number(RangeStart),
    RangeSize = ar_e2e:aligned_partition_size(SinkNode, Partition, SinkPacking),

    ar_test_await:partition_at_size(SinkNode, 1, SinkPacking, RangeSize),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 5, 4)),
    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),

    AlignedStart = arweave_util:floor_int(RangeStart, ?DATA_CHUNK_SIZE),
    AlignedEnd = arweave_util:ceil_int(RangeEnd, ?DATA_CHUNK_SIZE) + ?DATA_CHUNK_SIZE,
    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, AlignedStart, AlignedEnd),
    ok = ar_test_await:entropy_not_prepared(SinkNode, StoreID, 0, AlignedStart),

    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

do_large_module_aligned_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_large_module_aligned_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    ModuleSize = 2 * ar_block:partition_size(),
    Module = {0, ModuleSize, SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    Overrides = #{
                  [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                  [join, start_from_latest_state] => true,
                  [storage_modules] => StorageModules,
                  [join, auto] => true,
                  [mining, address] => SinkAddr
                 },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, Overrides, true),

    RangeStart = 0,
    RangeEnd = ModuleSize,

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    %% ar_mining_stats attributes a large storage module's chunks to the
    %% first partition only; assert on partition 0 alone.
    ar_test_await:partition_at_size(SinkNode, 0, SinkPacking, 4456448),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 7, 2)),

    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, RangeStart, RangeEnd),
    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

%% The assertions assume the chain is at height 6, so prime it,
%% branching on source packing:
%%
%%   - Packed source (replica_2_9, spora_2_6): pre-mine on the source
%%     before the sink starts, using its packed data.
%%   - Unpacked source: the source can't mine (no packed entropy), so
%%     run an extra sink-side `assert_mine_and_validate' after the sink
%%     starts.
%%
%% Sink-side priming can't be used for packed sources: their sync runs
%% an unpack+repack-with-sink-address cycle, so the sink can't mine
%% until `http_chunks_recorded' completes — which itself needs height 6.
do_large_module_unaligned_sync_pack_mine(
  {Blocks, Chunks, SourcePackingType}, SinkPackingType) ->
    ?LOG_INFO([{event, test_large_module_unaligned_sync_pack_mine}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
    [B0 | _] = Blocks,
    SourceNode = peer1,
    SinkNode = peer2,

    case SourcePackingType of
        unpacked ->
            ok;
        _ ->
            SourceHeight = ar_test_node:remote_call(
                             SourceNode, ar_node, get_height, []),
            ar_test_node:mine(SourceNode),
            {ok, _} = ar_test_await:node_height(SourceNode, SourceHeight + 1)
    end,

    Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),

    ModuleSize = floor(1.5 * ar_block:partition_size()),
    Module = {ModuleSize, 2 * ModuleSize, SinkPacking},
    StoreID = ar_storage_module:id(Module),
    StorageModules = [ Module ],

    Overrides = #{
                  [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                  [join, start_from_latest_state] => true,
                  [storage_modules] => StorageModules,
                  [join, auto] => true,
                  [mining, address] => SinkAddr
                 },
    SinkPeerName = ar_test_node:peer_name(SinkNode),
    SinkPeerName = ar_test_node:start_other_node(SinkNode, B0, Overrides, true),

    case SourcePackingType of
        unpacked ->
            %% Source couldn't pre-mine — sink primes the chain now.
            ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking);
        _ ->
            ok
    end,

    RangeStart = ModuleSize,
    RangeEnd = 2 * ModuleSize,

    ok = ar_test_await:http_chunks_recorded(SinkNode, RangeStart, RangeEnd),
    ar_test_await:partition_at_size(SinkNode, 1, SinkPacking, 3407872),
    ok = ar_test_await:partition_empty(SinkNode, 0, SinkPacking),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked_padded),
    ok = ar_test_await:partition_empty(SinkNode, 1, unpacked),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 7, 2)),

    ok = ar_test_await:entropy_prepared(SinkNode, StoreID, RangeStart, RangeEnd),
    ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking).

do_disk_pool_threshold(SourcePackingType, SinkPackingType) ->
    ?LOG_INFO([{event, test_disk_pool_threshold}, {module, ?MODULE},
               {from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),

    SourceNode = peer1,
    SinkNode = peer2,

    {Blocks, Chunks, SourcePackingType} = setup_source_node(SourcePackingType),
    [B0 | _] = Blocks,

    SinkPacking = start_sink_node(SinkNode, SourceNode, B0, SinkPackingType),
    ok = ar_test_await:http_chunks_recorded(SinkNode,
                                            ar_block:partition_size(), 4*ar_block:partition_size()),
    %% At height 6 the disk-pool threshold sits at the depth-3 weave
    %% 6291456 (inside partition 3): partitions 1-2 are fully below it,
    %% partition 3 holds only the two chunks below the threshold, and
    %% partition 4 is entirely above it (empty).
    ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
    ar_e2e:assert_partition_size(SinkNode, 2, SinkPacking),
    ar_test_await:partition_at_size(SinkNode, 3, SinkPacking, 2 * ?DATA_CHUNK_SIZE),
    ok = ar_test_await:partition_empty(SinkNode, 4, SinkPacking),
    ok = ar_test_await:http_chunks_not_recorded(SinkNode, 0, ar_block:partition_size()),
    ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

    case SinkPackingType of
        unpacked ->
            ok;
        _ ->
            ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),

            %% At height 7 the threshold moves to the depth-3 weave 8388608
            %% (inside partition 4): partition 3 is now fully below it and
            %% partition 4 holds the two chunks below it.
            ok = ar_test_await:http_chunks_recorded(SinkNode,
                                                    ar_block:partition_size(), 4*ar_block:partition_size()),
            ar_e2e:assert_partition_size(SinkNode, 2, SinkPacking),
            ar_e2e:assert_partition_size(SinkNode, 3, SinkPacking),
            ar_test_await:partition_at_size(SinkNode, 4, SinkPacking, 2 * ?DATA_CHUNK_SIZE),
            ok = ar_test_await:http_chunks_not_recorded(SinkNode, 0, ar_block:partition_size())
    end.

%% @doc Start a sink node with one storage module per partition.
start_sink_node(Node, SourceNode, B0, PackingType) ->
    Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking = ar_e2e:packing_type_to_packing(PackingType, SinkAddr),

    StorageModules = [
                      {ar_block:partition_size(), 2 * ar_block:partition_size(), SinkPacking},
                      {2 * ar_block:partition_size(), 3 * ar_block:partition_size(), SinkPacking},
                      {3 * ar_block:partition_size(), 4 * ar_block:partition_size(), SinkPacking},
                      {4 * ar_block:partition_size(), 5 * ar_block:partition_size(), SinkPacking},
                      {5 * ar_block:partition_size(), 6 * ar_block:partition_size(), SinkPacking},
                      {6 * ar_block:partition_size(), 7 * ar_block:partition_size(), SinkPacking},
                      {10 * ar_block:partition_size(), 11 * ar_block:partition_size(), SinkPacking}
                     ],
    NodePeerName = ar_test_node:peer_name(Node),
    NodePeerName = ar_test_node:start_other_node(Node, B0, #{
                                                             [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                                                             [join, start_from_latest_state] => true,
                                                             [storage_modules] => StorageModules,
                                                             [join, auto] => true,
                                                             [mining, address] => SinkAddr
                                                            }, true),
    SinkPacking.

%% @doc Start a sink node with two storage modules for partition 1.
start_sink_node(Node, SourceNode, B0, PackingType1, PackingType2) ->
    Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
    SinkAddr = ar_wallet:to_address(Wallet),
    SinkPacking1 = ar_e2e:packing_type_to_packing(PackingType1, SinkAddr),
    SinkPacking2 = ar_e2e:packing_type_to_packing(PackingType2, SinkAddr),

    StorageModules = [
                      {ar_block:partition_size(), 2 * ar_block:partition_size(), SinkPacking1},
                      {ar_block:partition_size(), 2 * ar_block:partition_size(), SinkPacking2}
                     ],

    NodePeerName = ar_test_node:peer_name(Node),
    NodePeerName = ar_test_node:start_other_node(Node, B0, #{
                                                             [peers, trusted] => [arweave_util:format_peer(ar_test_node:peer_ip(SourceNode))],
                                                             [join, start_from_latest_state] => true,
                                                             [storage_modules] => StorageModules,
                                                             [join, auto] => true,
                                                             [mining, address] => SinkAddr
                                                            }, true),
    {SinkPacking1, SinkPacking2}.
