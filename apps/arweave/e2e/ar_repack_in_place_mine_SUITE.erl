%%% @doc Common Test suite for the in-place repack-mine end-to-end
%%% scenarios. `init_per_testcase' restarts both peer BEAMs so each
%%% case starts from a fresh VM state, isolating it from cross-test
%%% state-leak races.
%%%
%%% Repacking in place *from* `replica_2_9' to any format is not
%%% supported, so the matrix here is asymmetric (only 10 of the 18
%%% From×To×ModuleSize combinations are exercised).
-module(ar_repack_in_place_mine_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%====================================================================
%% Suite metadata
%%====================================================================

suite() ->
	[{timetrap, {minutes, 20}}].

all() ->
	[
		test_unpacked_to_replica_2_9_default,
		test_spora_2_6_to_replica_2_9_default,
		test_replica_2_9_to_replica_2_9_default,
		test_replica_2_9_to_unpacked_default,
		test_spora_2_6_to_unpacked_default,

		test_unpacked_to_replica_2_9_small,
		test_spora_2_6_to_replica_2_9_small,
		test_replica_2_9_to_replica_2_9_small,
		test_replica_2_9_to_unpacked_small,
		test_spora_2_6_to_unpacked_small
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
%% Test cases
%%====================================================================

test_unpacked_to_replica_2_9_default(_Config) ->
	do_repack_in_place_mine(unpacked, replica_2_9, default).

test_spora_2_6_to_replica_2_9_default(_Config) ->
	do_repack_in_place_mine(spora_2_6, replica_2_9, default).

test_replica_2_9_to_replica_2_9_default(_Config) ->
	do_repack_in_place_mine(replica_2_9, replica_2_9, default).

test_replica_2_9_to_unpacked_default(_Config) ->
	do_repack_in_place_mine(replica_2_9, unpacked, default).

test_spora_2_6_to_unpacked_default(_Config) ->
	do_repack_in_place_mine(spora_2_6, unpacked, default).

test_unpacked_to_replica_2_9_small(_Config) ->
	do_repack_in_place_mine(unpacked, replica_2_9, small).

test_spora_2_6_to_replica_2_9_small(_Config) ->
	do_repack_in_place_mine(spora_2_6, replica_2_9, small).

test_replica_2_9_to_replica_2_9_small(_Config) ->
	do_repack_in_place_mine(replica_2_9, replica_2_9, small).

test_replica_2_9_to_unpacked_small(_Config) ->
	do_repack_in_place_mine(replica_2_9, unpacked, small).

test_spora_2_6_to_unpacked_small(_Config) ->
	do_repack_in_place_mine(spora_2_6, unpacked, small).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Repack-in-place body shared across all ten cases.
do_repack_in_place_mine(FromPackingType, ToPackingType, ModuleSize) ->
	?LOG_INFO([{event, test_repack_in_place_mine}, {module, ?MODULE},
		{from_packing_type, FromPackingType}, {to_packing_type, ToPackingType},
		{module_size, ModuleSize}]),
	ValidatorNode = peer1,
	RepackerNode = peer2,
	ar_test_node:stop(ValidatorNode),
	ar_test_node:stop(RepackerNode),
	{Blocks, _AddrA, Chunks} = ar_e2e:start_source_node(
		RepackerNode, FromPackingType, wallet_a, ModuleSize),
	RepackerSnapshot = ar_test_node:remote_call(
		RepackerNode, arweave_config, snapshot, []),

	[B0 | _] = Blocks,
	start_validator_node(ValidatorNode, RepackerNode, B0),

	NumModules = case ModuleSize of
		default -> 2;
		small -> 8
	end,

	{WalletB, SourceStorageModules} = ar_e2e:source_node_storage_modules(
		RepackerNode, ToPackingType, wallet_b, ModuleSize),
	AddrB = case WalletB of
		not_set -> not_set;
		_ -> ar_wallet:to_address(WalletB)
	end,
	FinalStorageModules = lists:sublist(SourceStorageModules, NumModules),
	ToPacking = ar_e2e:packing_type_to_packing(ToPackingType, AddrB),
	{_, SourceSideStorageModules} = ar_e2e:source_node_storage_modules(
		RepackerNode, FromPackingType, wallet_a, ModuleSize),
	RepackInPlaceStorageModules = lists:sublist([
		{Module, ToPacking} || Module <- SourceSideStorageModules ], NumModules),

	ar_e2e:restart_node(RepackerNode, RepackerSnapshot, #{
		[storage_modules] => [],
		[repack_modules] => [
			arweave_config:repack_module_to_config(ConfigModule)
			|| ConfigModule <- RepackInPlaceStorageModules
		],
		[mining, address] => not_set
	}),

	ExpectedSize0 = ar_e2e:aligned_partition_size(RepackerNode, 0, ToPacking),
	ar_test_await:partition_at_size(RepackerNode, 0, ToPacking, ExpectedSize0),
	ExpectedSize1 = ar_e2e:aligned_partition_size(RepackerNode, 1, ToPacking),
	ar_test_await:partition_at_size(RepackerNode, 1, ToPacking, ExpectedSize1),

	ar_test_node:stop(RepackerNode),

	DataDir = ar_test_node:remote_call(
		RepackerNode, arweave_config, get, [[data_dir]]),
	lists:foreach(fun({SourceModule, Packing}) ->
		{BucketSize, Bucket, _Packing} = SourceModule,
		SourceID = ar_storage_module:id(SourceModule),
		SourcePath = ar_chunk_storage:get_storage_module_path(DataDir, SourceID),

		TargetModule = {BucketSize, Bucket, Packing},
		TargetID = ar_storage_module:id(TargetModule),
		TargetPath = ar_chunk_storage:get_storage_module_path(DataDir, TargetID),
		ok = file:rename(SourcePath, TargetPath)
	end, RepackInPlaceStorageModules),

	ar_e2e:restart_node(RepackerNode, RepackerSnapshot, #{
		[storage_modules] => [arweave_config:storage_module_to_config(ConfigModule) || ConfigModule <- FinalStorageModules],
		[repack_modules] => [],
		[mining, address] => AddrB
	}),

	ar_e2e:wait_for_chunks_recorded(RepackerNode, ToPacking,
		chunk_probe_offsets(Chunks)),

	ar_e2e:assert_chunks(RepackerNode, ToPacking, Chunks),

	case ToPackingType of
		unpacked ->
			ok;
		_ ->
			ar_e2e:assert_mine_and_validate(RepackerNode, ValidatorNode, ToPacking)
	end.

start_validator_node(ValidatorNode, RepackerNode, B0) ->
	ValidatorPeerName = ar_test_node:peer_name(ValidatorNode),
	ValidatorPeerName = ar_test_node:start_other_node(ValidatorNode, B0, #{
		[peers, trusted] => [ar_util:format_peer(ar_test_node:peer_ip(RepackerNode))],
		[join, start_from_latest_state] => true,
		[join, auto] => true
	}, true),
	ok.

%% @doc Map each `{Block, EndOffset, ChunkSize}' chunk to a probe offset
%% `ar_sync_record:is_recorded/2' resolves to its slot. The chunk's
%% first byte lands inside the padded slot for both full and partial
%% chunks.
chunk_probe_offsets(Chunks) ->
	[EndOffset - ChunkSize + 1 || {_Block, EndOffset, ChunkSize} <- Chunks].
