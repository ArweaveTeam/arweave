-module(ar_packing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(CHUNK_OFFSET, 10*256*1024).
-define(ENCODED_TX_ROOT, <<"9d857DmXbSyhX6bgF7CDMDCl0f__RUjryMMvueFN9wE">>).


% request_test() ->
% 	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

% 	[B0] = ar_weave:init([]),
% 	ar_test_node:start(B0, RewardAddress),

% 	test_full_chunk(),
% 	test_partial_chunk(),
% 	test_full_chunk_repack(),
% 	test_partial_chunk_repack(),
% 	test_invalid_pad(),
% 	test_request_repack(RewardAddress),
% 	test_request_unpack(RewardAddress).

packing_test_() ->
    {setup, 
     fun setup/0, 
     fun teardown/1, 
     [fun test_mix_crc/0,
      fun test_mix_far/0,
      fun test_full_chunk/0,
      fun test_partial_chunk/0,
      fun test_full_chunk_repack/0,
      fun test_partial_chunk_repack/0,
      fun test_invalid_pad/0,
      fun test_request_repack/0,
      fun test_request_unpack/0]}.

setup() ->
    RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),
    [B0] = ar_weave:init([]),
    ar_test_node:start(B0, RewardAddress),
    RewardAddress.

teardown(_) ->
    % optional cleanup code
    ok.

test_mix_crc() ->
	Input1 = << 0:(8*8)>>,
	{ok, RealOutput1} = ar_rxsquared_nif:rsp_mix_entropy_crc32_nif(Input1),
	ExpdOutput1 = << 199,75,103,72,178,6,176,59 >>,
	?assertEqual(ExpdOutput1, RealOutput1),

	Input2 = << 1,2,3,4,5,6,7,8 >>,
	{ok, RealOutput2} = ar_rxsquared_nif:rsp_mix_entropy_crc32_nif(Input2),
	ExpdOutput2 = << 245,142,51,45,188,173,22,249 >>,
	?assertEqual(ExpdOutput2, RealOutput2),
	ok.

test_mix_far() ->
	% divisible
	Input1 = << 11, 12, 21, 22, 31, 32, 41, 42 >>,
	ExodOutput1 = << 11, 21, 31, 41, 12, 22, 32, 42 >>,
	{ok, RealOutput1} = ar_rxsquared_nif:rsp_mix_entropy_far_test_nif(Input1, 2, 1),
	?assertEqual(ExodOutput1, RealOutput1),

	Input2 = << 11, 12, 13, 14, 21, 22, 23, 24 >>,
	ExodOutput2 = << 11, 21, 12, 22, 13, 23, 14, 24 >>,
	{ok, RealOutput2} = ar_rxsquared_nif:rsp_mix_entropy_far_test_nif(Input2, 4, 1),
	?assertEqual(ExodOutput2, RealOutput2),

	Input3 = << 11, 12, 13, 14, 21, 22, 23, 24 >>,
	ExodOutput3 = << 11, 12, 21, 22, 13, 14, 23, 24 >>,
	{ok, RealOutput3} = ar_rxsquared_nif:rsp_mix_entropy_far_test_nif(Input3, 4, 2),
	?assertEqual(ExodOutput3, RealOutput3),

	% not divisible
	Input4 = << 11, 12, 13, 14, 21, 22, 23, 24 >>,
	ExodOutput4 = << 11, 12, 13, 21, 22, 23, 14, 24 >>,
	{ok, RealOutput4} = ar_rxsquared_nif:rsp_mix_entropy_far_test_nif(Input4, 4, 3),
	?assertEqual(ExodOutput4, RealOutput4),
	ok.

test_full_chunk() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.256kb"),
	Spora25Data = ar_test_node:load_fixture("ar_packing_tests/spora25.256kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:pack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora25Data},
		ar_packing_server:pack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora26Data},
		ar_packing_server:pack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, UnpackedData)),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_partial_chunk() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.100kb"),
	Spora25Data = ar_test_node:load_fixture("ar_packing_tests/spora25.100kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:pack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora25Data},
		ar_packing_server:pack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora26Data},
		ar_packing_server:pack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, UnpackedData)),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_full_chunk_repack() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.256kb"),
	Spora25Data = ar_test_node:load_fixture("ar_packing_tests/spora25.256kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, unpacked, 
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, unpacked, 
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, none},
		ar_packing_server:repack(spora_2_5, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, {spora_2_6, RewardAddress}, 
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, {spora_2_6, RewardAddress}, 
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, none},
		ar_packing_server:repack({spora_2_6, RewardAddress}, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_partial_chunk_repack() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.100kb"),
	Spora25Data = ar_test_node:load_fixture("ar_packing_tests/spora25.100kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, none},
		ar_packing_server:repack(spora_2_5, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, none},
		ar_packing_server:repack({spora_2_6, RewardAddress}, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_invalid_pad() ->
	ChunkSize = 100*1024,

	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.256kb"),
	Spora25Data = ar_test_node:load_fixture("ar_packing_tests/spora25.256kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.256kb"),

	ShortUnpackedData = binary:part(UnpackedData, 0, ChunkSize),

	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	?assertEqual(
		{ok, ShortUnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
		"We don't check the pad when unpacking SPoRA 2.5"),
	?assertEqual(
		{error, invalid_padding},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize),
			"We do check the pad when unpacking SPoRA 2.6"),
	?assertEqual(
		{ok, ShortUnpackedData, ShortUnpackedData},
		ar_packing_server:repack(
			unpacked, spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We don't check the pad when repacking from SPoRA 2.5"),
	?assertMatch(
		{ok, _, ShortUnpackedData},
		ar_packing_server:repack(
			{spora_2_6, RewardAddress}, spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We don't check the pad when repacking from SPoRA 2.5"),
	?assertEqual(
		{error, invalid_padding},
		ar_packing_server:repack(
			unpacked, {spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We do check the pad when repacking from SPoRA 2.6"),
	?assertMatch(
		{error, invalid_padding},
		ar_packing_server:repack(
			spora_2_5, {spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We do check the pad when repacking from SPoRA 2.6").

test_request_repack() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.256kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	%% unpacked -> unpacked
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		unpacked,
		unpacked, UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {unpacked, Unpacked1, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked1)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% unpacked -> packed
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress},
		unpacked, UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {{spora_2_6, RewardAddress}, Packed, _, _, _}}} ->
            ?assertEqual(Spora26Data, Packed)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> unpacked
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		unpacked,
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {unpacked, Unpacked2, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked2)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> packed
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress},
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {{spora_2_6, RewardAddress}, Packed2, _, _, _}}} ->
            ?assertEqual(Spora26Data, Packed2)
    after 5000 -> 
        erlang:error(timeout)
    end.

test_request_unpack() ->
	UnpackedData = ar_test_node:load_fixture("ar_packing_tests/unpacked.256kb"),
	Spora26Data = ar_test_node:load_fixture("ar_packing_tests/spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_test_node:load_fixture("ar_packing_tests/address.bin"),

	%% unpacked -> unpacked
	ar_packing_server:request_unpack(?CHUNK_OFFSET, {
		unpacked,
		UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {unpacked, _, {unpacked, Unpacked1, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked1)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> unpacked
	ar_packing_server:request_unpack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {unpacked, _, {{spora_2_6, RewardAddress}, Unpacked2, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked2)
    after 5000 -> 
        erlang:error(timeout)
    end.


packs_chunks_depending_on_packing_threshold_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_9, fun() -> 10 end}],
			fun test_packs_chunks_depending_on_packing_threshold/0).

test_packs_chunks_depending_on_packing_threshold() ->
	MainWallet = ar_wallet:new_keyfile(),
	PeerWallet = ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, []),
	MainAddr = ar_wallet:to_address(MainWallet),
	PeerAddr = ar_wallet:to_address(PeerWallet),
	DataMap =
		lists:foldr(
			fun(Height, Acc) ->
				ChunkCount = 3,
				{DR1, Chunks1} = ar_test_data_sync:generate_random_split(ChunkCount),
				{DR2, Chunks2} = ar_test_data_sync:generate_random_original_split(ChunkCount),
				{DR3, Chunks3} = ar_test_data_sync:generate_random_original_v1_split(),
				maps:put(Height, {{DR1, Chunks1}, {DR2, Chunks2}, {DR3, Chunks3}}, Acc)
			end,
			#{},
			lists:seq(1, 20)
		),
	Wallet = ar_test_data_sync:setup_nodes(#{ addr => MainAddr, peer_addr => PeerAddr }),
	{_LegacyProofs, StrictProofs, V1Proofs} = lists:foldl(
		fun(Height, {Acc1, Acc2, Acc3}) ->
			{{DR1, Chunks1}, {DR2, Chunks2}, {DR3, Chunks3}} = maps:get(Height, DataMap),
			{#tx{ id = TXID1 } = TX1, Chunks1} =
					ar_test_data_sync:tx(Wallet, {fixed_data, DR1, Chunks1}),
			{#tx{ id = TXID2 } = TX2, Chunks2} =
					ar_test_data_sync:tx(Wallet, {fixed_data, DR2, Chunks2}),
			{#tx{ id = TXID3 } = TX3, Chunks3} =
					ar_test_data_sync:tx(Wallet, {fixed_data, DR3, Chunks3}, v1),
			{Miner, Receiver} =
				case rand:uniform(2) == 1 of
					true ->
						{main, peer1};
					false ->
						{peer1, main}
				end,
			?debugFmt("Mining block ~B.~n", [Height]),
			TXs = ar_util:pick_random([TX1, TX2, TX3], 2),
			B = ar_test_node:post_and_mine(#{ miner => Miner, await_on => Receiver }, TXs),
			Acc1_2 =
				case lists:member(TX1, TXs) of
					true ->
						ar_test_data_sync:post_proofs(main, B, TX1, Chunks1),
						maps:put(TXID1,
								ar_test_data_sync:get_records_with_proofs(B, TX1, Chunks1),
								Acc1);
					false ->
						Acc1
				end,
			Acc2_2 =
				case lists:member(TX2, TXs) of
					true ->
						ar_test_data_sync:post_proofs(peer1, B, TX2, Chunks2),
						maps:put(TXID2,
								ar_test_data_sync:get_records_with_proofs(B, TX2, Chunks2),
								Acc2);
					false ->
						Acc2
				end,
			Acc3_2 =
				case lists:member(TX3, TXs) of
					true ->
						maps:put(TXID3,
								ar_test_data_sync:get_records_with_proofs(B, TX3, Chunks3),
								Acc3);
					false ->
						Acc3
				end,
			{Acc1_2, Acc2_2, Acc3_2}
		end,
		{#{}, #{}, #{}},
		lists:seq(1, 20)
	),
	%% Mine some empty blocks on top to force all submitted data to fall below
	%% the disk pool threshold so that the non-default storage modules can sync it.
	lists:foreach(
		fun(_) ->
			{Miner, Receiver} =
				case rand:uniform(2) == 1 of
					true ->
						{main, peer1};
					false ->
						{peer1, main}
				end,
			ar_test_node:post_and_mine(#{ miner => Miner, await_on => Receiver }, [])
		end,
		lists:seq(1, 5)
	),
	BILast = ar_node:get_block_index(),
	LastB = ar_test_node:read_block_when_stored(
			element(1, lists:nth(10, lists:reverse(BILast)))),
	lists:foldl(
		fun(Height, PrevB) ->
			H = element(1, lists:nth(Height + 1, lists:reverse(BILast))),
			B = ar_test_node:read_block_when_stored(H),
			PoA = B#block.poa,
			NonceLimiterInfo = B#block.nonce_limiter_info,
			PartitionUpperBound =
					NonceLimiterInfo#nonce_limiter_info.partition_upper_bound,
			H0 = ar_block:compute_h0(B, PrevB),
			{RecallRange1Start, _} = ar_block:get_recall_range(H0,
					B#block.partition_number, PartitionUpperBound),
			RecallByte =
				case B#block.packing_difficulty of
					0 ->
						RecallRange1Start + B#block.nonce * ?DATA_CHUNK_SIZE;
					_ ->
						RecallRange1Start + (B#block.nonce div 32) * ?DATA_CHUNK_SIZE
				end,
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(RecallByte),
			?debugFmt("Mined a block. "
					"Computed recall byte: ~B, block's recall byte: ~p. "
					"Height: ~B. Previous block: ~s. "
					"Computed search space upper bound: ~B. "
					"Block start: ~B. Block end: ~B. TX root: ~s.",
					[RecallByte, B#block.recall_byte, Height,
					ar_util:encode(PrevB#block.indep_hash), PartitionUpperBound,
					BlockStart, BlockEnd, ar_util:encode(TXRoot)]),
			?assertEqual(RecallByte, B#block.recall_byte),
			SubChunkIndex = ar_block:get_sub_chunk_index(B#block.packing_difficulty,
					B#block.nonce),
			{Packing, PoA2} =
				case B#block.packing_difficulty of
					0 ->
						{{spora_2_6, B#block.reward_addr}, PoA};
					?REPLICA_2_9_PACKING_DIFFICULTY ->
						{ok, #{ chunk := UnpackedChunk }}
							= ar_data_sync:get_chunk(RecallByte + 1,
								#{ packing => unpacked, pack => true }),
						UnpackedChunk2 = ar_packing_server:pad_chunk(UnpackedChunk),
						{{replica_2_9, B#block.reward_addr},
								PoA#poa{ unpacked_chunk = UnpackedChunk2 }};
					_ ->
						{ok, #{ chunk := UnpackedChunk }}
							= ar_data_sync:get_chunk(RecallByte + 1,
								#{ packing => unpacked, pack => true }),
						UnpackedChunk2 = ar_packing_server:pad_chunk(UnpackedChunk),
						{{composite, B#block.reward_addr, B#block.packing_difficulty},
								PoA#poa{ unpacked_chunk = UnpackedChunk2 }}
				end,
			?assertMatch({true, _}, ar_poa:validate({BlockStart, RecallByte, TXRoot,
					BlockEnd - BlockStart, PoA2, Packing, SubChunkIndex, not_set})),
			B
		end,
		LastB,
		lists:seq(10, 20)
	),
	?debugMsg("Asserting synced data with the strict splits."),
	maps:map(
		fun(TXID, [{_, _, Chunks, _} | _]) ->
			ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
			ar_test_node:assert_get_tx_data(main, TXID, ExpectedData),
			ar_test_node:assert_get_tx_data(peer1, TXID, ExpectedData)
		end,
		StrictProofs
	),
	?debugMsg("Asserting synced v1 data."),
	maps:map(
		fun(TXID, [{_, _, Chunks, _} | _]) ->
			ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
			ar_test_node:assert_get_tx_data(main, TXID, ExpectedData),
			ar_test_node:assert_get_tx_data(peer1, TXID, ExpectedData)
		end,
		V1Proofs
	),
	?debugMsg("Asserting synced chunks."),
	ar_test_data_sync:wait_until_syncs_chunks([P || {_, _, _, P} <- lists:flatten(maps:values(StrictProofs))]),
	ar_test_data_sync:wait_until_syncs_chunks([P || {_, _, _, P} <- lists:flatten(maps:values(V1Proofs))]),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, [P || {_, _, _, P} <- lists:flatten(
			maps:values(StrictProofs))], infinity),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, [P || {_, _, _, P} <- lists:flatten(maps:values(V1Proofs))],
			infinity).

