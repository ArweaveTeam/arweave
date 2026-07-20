%%% @doc Common Test suite for the repack-mine end-to-end scenarios.
%%%
%%% `init_per_testcase/2' restarts both peer BEAMs before every case so
%%% each starts with fresh Erlang VMs. This isolation avoids a
%%% `get_chunk_invalid_id' race on partition-0 chunks (during the first
%%% `mine_block' of `replica_2_9' source setup) that residual VM-level
%%% state otherwise triggers. The cost is ~2 minutes per case for two
%%% BEAM boots (VM start + RandomX dataset init per peer).
-module(ar_repack_mine_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%%====================================================================
%% Suite metadata
%%====================================================================

suite() ->
    %% 20 min per case: the body runs 7-13 min plus ~2 min peer restart;
    %% the tighter trap surfaces a hung case faster.
    [{timetrap, {minutes, 20}}].

all() ->
    [
     test_replica_2_9_to_replica_2_9,
     test_replica_2_9_to_spora_2_6,
     test_replica_2_9_to_unpacked,

     test_unpacked_to_replica_2_9,
     test_unpacked_to_spora_2_6,
     test_spora_2_6_to_replica_2_9,
     test_spora_2_6_to_spora_2_6,
     test_spora_2_6_to_unpacked
    ].

%%====================================================================
%% Setup / teardown
%%====================================================================

init_per_suite(Config) ->
    %% CT runs from its per-run log directory, but the e2e infrastructure
    %% assumes the project root is CWD (relative `.tmp/data_...' prefixes,
    %% `genesis_data/...' reads, `.tmp/peer*.out' files). `bin/e2e'
    %% exports `ARWEAVE_PROJECT_ROOT' so we can restore it.
    case os:getenv("ARWEAVE_PROJECT_ROOT") of
        false -> ok;
        Root -> ok = file:set_cwd(Root)
    end,
    %% Bring up the support apps `ar_test_node' assumes are running. The
    %% initial peer boot is left to `init_per_testcase' so every case
    %% gets a fresh pair.
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
    %% Fresh peer BEAMs for every case. See module docstring for the
    %% race we're eliminating by doing this.
    ar_test_node:restart_peer_beam(peer1),
    ar_test_node:restart_peer_beam(peer2),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Peers are left up; the next case's `init_per_testcase' (or
    %% `end_per_suite') tears them down, avoiding a double boot cost.
    ok.

%%====================================================================
%% Test cases
%%====================================================================

test_replica_2_9_to_replica_2_9(_Config) ->
    do_repack_mine(replica_2_9, replica_2_9).

test_replica_2_9_to_spora_2_6(_Config) ->
    do_repack_mine(replica_2_9, spora_2_6).

test_replica_2_9_to_unpacked(_Config) ->
    do_repack_mine(replica_2_9, unpacked).

test_unpacked_to_replica_2_9(_Config) ->
    do_repack_mine(unpacked, replica_2_9).

test_unpacked_to_spora_2_6(_Config) ->
    do_repack_mine(unpacked, spora_2_6).

test_spora_2_6_to_replica_2_9(_Config) ->
    do_repack_mine(spora_2_6, replica_2_9).

test_spora_2_6_to_spora_2_6(_Config) ->
    do_repack_mine(spora_2_6, spora_2_6).

test_spora_2_6_to_unpacked(_Config) ->
    do_repack_mine(spora_2_6, unpacked).

%%====================================================================
%% Helpers
%%====================================================================

%% Body shared across all eight cases: bring up a source node packed as
%% `FromPackingType', repack it to `ToPackingType' across two restarts,
%% then mine and validate.
do_repack_mine(FromPackingType, ToPackingType) ->
    ?LOG_INFO([{event, test_repack_mine}, {module, ?MODULE},
               {from_packing_type, FromPackingType}, {to_packing_type, ToPackingType}]),
    ValidatorNode = peer1,
    RepackerNode = peer2,
    ar_test_node:stop(ValidatorNode),
    ar_test_node:stop(RepackerNode),
    {Blocks, _AddrA, Chunks} = ar_e2e:start_source_node(
                                 RepackerNode, FromPackingType, wallet_a),
    RepackerSnapshot = ar_test_node:remote_call(
                         RepackerNode, arweave_config, snapshot, []),

    [B0 | _] = Blocks,
    start_validator_node(ValidatorNode, RepackerNode, B0),

    {WalletB, StorageModules} = ar_e2e:source_node_storage_modules(
                                  RepackerNode, ToPackingType, wallet_b),
    AddrB = case WalletB of
                not_set -> not_set;
                _ -> ar_wallet:to_address(WalletB)
            end,
    ToPacking = ar_e2e:packing_type_to_packing(ToPackingType, AddrB),
    ExistingStorageModuleConfigs = ar_test_node:remote_call(
                                     RepackerNode, arweave_config, get, [[storage_modules]]),
    ExistingStorageModules = [
                              arweave_config:config_to_storage_module(M)
                              || M <- ExistingStorageModuleConfigs
                             ],
    %% For replica_2_9 destinations, mount old and new modules with
    %% sync_jobs=0 first so new modules prepare entropy without
    %% concurrent cross-module sync: otherwise a chunk copied in as
    %% `unpacked_padded' could be read as `not_found' from another new
    %% module and invalidated via `read_range2'. The second restart
    %% re-enables sync once entropy is ready.
    case ToPackingType of
        replica_2_9 ->
            ar_e2e:restart_node(RepackerNode, RepackerSnapshot, #{
                                                                  [storage_modules] => [arweave_config:storage_module_to_config(ConfigModule) || ConfigModule <- ExistingStorageModules ++ StorageModules],
                                                                  [mining, address] => AddrB,
                                                                  [sync, jobs] => 0
                                                                 }),
            ar_test_await:all_entropy_prepared(RepackerNode);
        _ ->
            ok
    end,
    ar_e2e:restart_node(RepackerNode, RepackerSnapshot, #{
                                                          [storage_modules] => [arweave_config:storage_module_to_config(ConfigModule) || ConfigModule <- ExistingStorageModules ++ StorageModules],
                                                          [mining, address] => AddrB
                                                         }),

    ok = ar_test_await:http_chunks_recorded(RepackerNode, 0, 4*ar_block:partition_size()),
    ar_e2e:assert_partition_size(RepackerNode, 0, ToPacking),
    ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking),
    %% Source ends at height 6, putting the disk-pool threshold at the
    %% depth-3 weave 6291456, inside partition 3. Partition 2 is fully
    %% below it; partition 3 holds only the two chunks below the
    %% threshold (the chunk ending at 6029312 crosses into partition 3).
    ar_e2e:assert_partition_size(RepackerNode, 2, ToPacking),
    %% No chunk assertion here: with two storage modules defined the
    %% packing format is ambiguous. The post-second-restart
    %% `assert_chunks' below covers it.
    ar_test_await:partition_at_size(RepackerNode, 3, ToPacking, 2 * ?DATA_CHUNK_SIZE),
    ok = ar_test_await:partition_empty(RepackerNode, 4, ToPacking),

    ar_e2e:restart_node(RepackerNode, RepackerSnapshot, #{
                                                          [storage_modules] => [arweave_config:storage_module_to_config(ConfigModule) || ConfigModule <- StorageModules],
                                                          [mining, address] => AddrB
                                                         }),
    ok = ar_test_await:http_chunks_recorded(RepackerNode, 0, 4*ar_block:partition_size()),
    ar_e2e:assert_partition_size(RepackerNode, 0, ToPacking),
    ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking),
    ar_e2e:assert_partition_size(RepackerNode, 2, ToPacking),
    ar_test_await:partition_at_size(RepackerNode, 3, ToPacking, 2 * ?DATA_CHUNK_SIZE),
    ar_e2e:assert_chunks(RepackerNode, ToPacking, Chunks),
    ok = ar_test_await:partition_empty(RepackerNode, 4, ToPacking),

    case ToPackingType of
        unpacked ->
            ok;
        _ ->
            ar_e2e:assert_mine_and_validate(RepackerNode, ValidatorNode, ToPacking),

            %% Mining one block (height 7) moves the disk-pool threshold
            %% to the depth-3 weave 8388608, inside partition 4.
            %% Partitions 0-3 are now fully below it; partition 4 holds
            %% only the two chunks below the threshold (the chunk ending
            %% at 8126464 crosses into partition 4).
            ok = ar_test_await:http_chunks_recorded(RepackerNode, 0, 4*ar_block:partition_size()),
            ar_e2e:assert_partition_size(RepackerNode, 0, ToPacking),
            ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking),
            ar_e2e:assert_partition_size(RepackerNode, 2, ToPacking),
            ar_e2e:assert_partition_size(RepackerNode, 3, ToPacking),
            ar_test_await:partition_at_size(RepackerNode, 4, ToPacking, 2 * ?DATA_CHUNK_SIZE)
    end.

start_validator_node(ValidatorNode, RepackerNode, B0) ->
    ValidatorPeerName = ar_test_node:peer_name(ValidatorNode),
    ValidatorPeerName = ar_test_node:start_other_node(ValidatorNode, B0, #{
                                                                           [peers, trusted] => [ar_util:format_peer(ar_test_node:peer_ip(RepackerNode))],
                                                                           [join, start_from_latest_state] => true,
                                                                           [join, auto] => true,
                                                                           [storage_modules] => []
                                                                          }, true),
    ok.
