-module(ar_packing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(CHUNK_OFFSET, 10*256*1024).
-define(ENCODED_TX_ROOT, <<"9d857DmXbSyhX6bgF7CDMDCl0f__RUjryMMvueFN9wE">>).
-define(ENCODED_REWARD_ADDRESS, <<"usuW8f-hpzuA4ZFXxiTbv0OXRWZP2HUlJqP-bGMR8g8">>).

load_fixture(Fixture) ->
	Dir = filename:dirname(?FILE),
	FixtureDir = filename:join(Dir, "fixtures/chunks"),
	FixtureFilename = filename:join(FixtureDir, Fixture),
	{ok, Data} = file:read_file(FixtureFilename),
	Data.

% request_test() ->
% 	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
     [fun test_full_chunk/0,
      fun test_partial_chunk/0,
      fun test_full_chunk_repack/0,
      fun test_partial_chunk_repack/0,
      fun test_invalid_pad/0,
      fun test_request_repack/0,
      fun test_request_unpack/0]}.

setup() ->
    RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),
    [B0] = ar_weave:init([]),
    ar_test_node:start(B0, RewardAddress),
    RewardAddress.

teardown(_) ->
    % optional cleanup code
    ok.

test_full_chunk() ->
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
	UnpackedData = load_fixture("unpacked.100kb"),
	Spora25Data = load_fixture("spora25.100kb"),
	Spora26Data = load_fixture("spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
	UnpackedData = load_fixture("unpacked.100kb"),
	Spora25Data = load_fixture("spora25.100kb"),
	Spora26Data = load_fixture("spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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

	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ShortUnpackedData = binary:part(UnpackedData, 0, ChunkSize),

	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

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
		ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end},
				{ar_fork, height_2_6_8, fun() -> 0 end},
				{ar_fork, height_2_7, fun() -> 0 end}],
				fun test_packs_chunks_depending_on_packing_threshold/0).
	
	test_packs_chunks_depending_on_packing_threshold() ->
		MainWallet = ar_wallet:new_keyfile(),
		PeerWallet = ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, []),
		MainAddr = ar_wallet:to_address(MainWallet),
		PeerAddr = ar_wallet:to_address(PeerWallet),
		DataMap =
			lists:foldr(
				fun(Height, Acc) ->
					ChunkCount = 10,
					{DR1, Chunks1} = ar_test_data_sync:generate_random_split(ChunkCount),
					{DR2, Chunks2} = ar_test_data_sync:generate_random_original_split(ChunkCount),
					{DR3, Chunks3} = ar_test_data_sync:generate_random_original_v1_split(),
					maps:put(Height, {{DR1, Chunks1}, {DR2, Chunks2}, {DR3, Chunks3}}, Acc)
				end,
				#{},
				lists:seq(1, 20)
			),
		Wallet = ar_test_data_sync:setup_nodes(MainAddr, PeerAddr),
		{LegacyProofs, StrictProofs, V1Proofs} = lists:foldl(
			fun(Height, {Acc1, Acc2, Acc3}) ->
				{{DR1, Chunks1}, {DR2, Chunks2}, {DR3, Chunks3}} = maps:get(Height, DataMap),
				{#tx{ id = TXID1 } = TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {fixed_data, DR1, Chunks1}),
				{#tx{ id = TXID2 } = TX2, Chunks2} = ar_test_data_sync:tx(Wallet, {fixed_data, DR2, Chunks2}),
				{#tx{ id = TXID3 } = TX3, Chunks3} = ar_test_data_sync:tx(Wallet, {fixed_data, DR3, Chunks3}, v1),
				{Miner, Receiver} =
					case rand:uniform(2) == 1 of
						true ->
							{main, peer1};
						false ->
							{peer1, main}
					end,
				?debugFmt("Mining block ~B, txs: ~s, ~s, ~s; data roots: ~s, ~s, ~s.~n", [Height,
						ar_util:encode(TX1#tx.id), ar_util:encode(TX2#tx.id),
						ar_util:encode(TX3#tx.id), ar_util:encode(TX1#tx.data_root),
						ar_util:encode(TX2#tx.data_root), ar_util:encode(TX3#tx.data_root)]),
				B = ar_test_node:post_and_mine(#{ miner => Miner, await_on => Receiver }, [TX1, TX2, TX3]),
				ar_test_data_sync:post_proofs(main, B, TX1, Chunks1),
				ar_test_data_sync:post_proofs(peer1, B, TX2, Chunks2),
				{maps:put(TXID1, ar_test_data_sync:get_records_with_proofs(B, TX1, Chunks1), Acc1),
						maps:put(TXID2, ar_test_data_sync:get_records_with_proofs(B, TX2, Chunks2), Acc2),
						maps:put(TXID3, ar_test_data_sync:get_records_with_proofs(B, TX3, Chunks3), Acc3)}
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
		LastB = ar_test_node:read_block_when_stored(element(1, lists:nth(10, lists:reverse(BILast)))),
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
				RecallByte = RecallRange1Start + B#block.nonce * ?DATA_CHUNK_SIZE,
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
				?assertMatch({true, _}, ar_poa:validate({BlockStart, RecallByte, TXRoot,
						BlockEnd - BlockStart, PoA,
						{spora_2_6, B#block.reward_addr}, not_set})),
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
				infinity),
		ChunkSize = ?DATA_CHUNK_SIZE,
		maps:map(
			fun(TXID, [{_B, _TX, Chunks, _} | _] = Proofs) ->
				BigChunks = lists:filter(fun(C) -> byte_size(C) == ChunkSize end, Chunks),
				Length = length(Chunks),
				LastSize = byte_size(lists:last(Chunks)),
				SecondLastSize = byte_size(lists:nth(Length - 1, Chunks)),
				DataPathSize = byte_size(maps:get(data_path, element(2, element(4,
						lists:nth(Length - 1, Proofs))))),
				case length(BigChunks) == Length
						orelse (length(BigChunks) + 1 == Length andalso LastSize < ChunkSize)
						orelse (length(BigChunks) + 2 == Length
								andalso LastSize < ChunkSize
								andalso SecondLastSize < ChunkSize
								andalso SecondLastSize >= DataPathSize
								andalso LastSize + SecondLastSize > ChunkSize) of
					true ->
						?debugMsg("Asserting random split which turned out strict."),
						ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
						ar_test_node:assert_get_tx_data(main, TXID, ExpectedData),
						ar_test_node:assert_get_tx_data(peer1, TXID, ExpectedData),
						ar_test_data_sync:wait_until_syncs_chunks([P || {_, _, _, P} <- Proofs]),
						ar_test_data_sync:wait_until_syncs_chunks(peer1, [P || {_, _, _, P} <- Proofs], infinity);
					false ->
						?debugFmt("Asserting random split which turned out NOT strict"
								" and was placed above the strict data split threshold, "
								"TXID: ~s.", [ar_util:encode(TXID)]),
						?debugFmt("Chunk sizes: ~p.", [[byte_size(Chunk) || Chunk <- Chunks]]),
						ar_test_node:assert_data_not_found(main, TXID),
						ar_test_node:assert_data_not_found(peer1, TXID)
				end
			end,
			LegacyProofs
		).
	
