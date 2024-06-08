-module(ar_serialize_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("eunit/include/eunit.hrl").

block_to_binary_test_() ->
	%% Set the mainnet values here because we are using the mainnet fixtures.
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_1_6, fun() -> 95000 end},
			{ar_fork, height_1_7, fun() -> 235200 end},
			{ar_fork, height_1_8, fun() -> 269510 end},
			{ar_fork, height_1_9, fun() -> 315700 end},
			{ar_fork, height_2_0, fun() -> 422250 end},
			{ar_fork, height_2_2, fun() -> 552180 end},
			{ar_fork, height_2_3, fun() -> 591140 end},
			{ar_fork, height_2_4, fun() -> 633720 end},
			{ar_fork, height_2_5, fun() -> 812970 end},
			{ar_fork, height_2_6, fun() -> infinity end}],
		fun test_block_to_binary/0).

test_block_to_binary() ->
	{ok, Cwd} = file:get_cwd(),
	BlockFixtureDir = filename:join(Cwd, "./apps/arweave/test/fixtures/blocks"),
	TXFixtureDir = filename:join(Cwd, "./apps/arweave/test/fixtures/txs"),
	{ok, BlockFixtures} = file:list_dir(BlockFixtureDir),
	test_block_to_binary([filename:join(BlockFixtureDir, Name)
			|| Name <- BlockFixtures], TXFixtureDir).

test_block_to_binary([], _TXFixtureDir) ->
	ok;
test_block_to_binary([Fixture | Fixtures], TXFixtureDir) ->
	{ok, Bin} = file:read_file(Fixture),
	B = ar_storage:migrate_block_record(binary_to_term(Bin)),
	?debugFmt("Block ~s, height ~B.~n", [ar_util:encode(B#block.indep_hash),
			B#block.height]),
	test_block_to_binary(B),
	RandomTags = [crypto:strong_rand_bytes(rand:uniform(2))
			|| _ <- lists:seq(1, rand:uniform(1024))],
	B2 = B#block{ tags = RandomTags },
	test_block_to_binary(B2),
	B3 = B#block{ reward_addr = unclaimed },
	test_block_to_binary(B3),
	{ok, TXFixtures} = file:list_dir(TXFixtureDir),
	TXs =
		lists:foldl(
			fun(TXFixture, Acc) ->
				{ok, TXBin} = file:read_file(filename:join(TXFixtureDir, TXFixture)),
				TX = ar_storage:migrate_tx_record(binary_to_term(TXBin)),
				maps:put(TX#tx.id, TX, Acc)
			end,
			#{},
			TXFixtures),
	BlockTXs = [maps:get(TXID, TXs) || TXID <- B#block.txs],
	B4 = B#block{ txs = BlockTXs },
	test_block_to_binary(B4),
	BlockTXs2 = [case rand:uniform(2) of 1 -> TX#tx.id; _ -> TX end
			|| TX <- BlockTXs],
	B5 = B#block{ txs = BlockTXs2 },
	test_block_to_binary(B5),
	TXIDs = [TX#tx.id || TX <- BlockTXs],
	B6 = B#block{ txs = TXIDs },
	test_block_to_binary(B6),
	test_block_to_binary(Fixtures, TXFixtureDir).

test_block_to_binary(B) ->
	{ok, B2} = ar_serialize:binary_to_block(ar_serialize:block_to_binary(B)),
	?assertEqual(B#block{ txs = [] }, B2#block{ txs = [] }),
	?assertEqual(true, compare_txs(B#block.txs, B2#block.txs)),
	lists:foreach(
		fun	(TX) when is_record(TX, tx)->
				?assertEqual({ok, TX}, ar_serialize:binary_to_tx(ar_serialize:tx_to_binary(TX)));
			(_TXID) ->
				ok
		end,
		B#block.txs
	).

compare_txs([TXID | TXs], [#tx{ id = TXID } | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([#tx{ id = TXID } | TXs], [TXID | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([TXID | TXs], [TXID | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([], []) ->
	true;
compare_txs(_TXs, _TXs2) ->
	false.

block_announcement_to_binary_test() ->
	A = #block_announcement{ indep_hash = crypto:strong_rand_bytes(48),
			previous_block = crypto:strong_rand_bytes(48) },
	?assertEqual({ok, A}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A))),
	A2 = A#block_announcement{ recall_byte = 0 },
	?assertEqual({ok, A2}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A2))),
	A3 = A#block_announcement{ recall_byte = 1000000000000000000000 },
	?assertEqual({ok, A3}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A3))),
	A4 = A3#block_announcement{ tx_prefixes = [crypto:strong_rand_bytes(8)
			|| _ <- lists:seq(1, 1000)] },
	?assertEqual({ok, A4}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A4))),
	A5 = A#block_announcement{ recall_byte2 = 1,
			solution_hash = crypto:strong_rand_bytes(32) },
	?assertEqual({ok, A5}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A5))),
	A6 = A#block_announcement{ recall_byte2 = 1, recall_byte = 2,
			solution_hash = crypto:strong_rand_bytes(32) },
	?assertEqual({ok, A6}, ar_serialize:binary_to_block_announcement(
			ar_serialize:block_announcement_to_binary(A6))).

block_announcement_response_to_binary_test() ->
	A = #block_announcement_response{},
	?assertEqual({ok, A}, ar_serialize:binary_to_block_announcement_response(
			ar_serialize:block_announcement_response_to_binary(A))),
	A2 = A#block_announcement_response{ missing_chunk = true,
			missing_tx_indices = lists:seq(0, 999) },
	?assertEqual({ok, A2}, ar_serialize:binary_to_block_announcement_response(
			ar_serialize:block_announcement_response_to_binary(A2))),
	A3 = A#block_announcement_response{ missing_chunk = true, missing_chunk2 = false,
			missing_tx_indices = lists:seq(0, 1) },
	?assertEqual({ok, A3}, ar_serialize:binary_to_block_announcement_response(
			ar_serialize:block_announcement_response_to_binary(A3))),
	A4 = A#block_announcement_response{ missing_chunk2 = true,
			missing_tx_indices = [731] },
	?assertEqual({ok, A4}, ar_serialize:binary_to_block_announcement_response(
			ar_serialize:block_announcement_response_to_binary(A4))).

poa_map_to_binary_test() ->
	Proof = #{ chunk => crypto:strong_rand_bytes(1), data_path => <<>>,
			tx_path => <<>>, packing => unpacked },
	?assertEqual({ok, Proof},
			ar_serialize:binary_to_poa(ar_serialize:poa_map_to_binary(Proof))),
	Proof2 = Proof#{ chunk => crypto:strong_rand_bytes(256 * 1024) },
	?assertEqual({ok, Proof2},
			ar_serialize:binary_to_poa(ar_serialize:poa_map_to_binary(Proof2))),
	Proof3 = Proof2#{ data_path => crypto:strong_rand_bytes(1024),
			packing => spora_2_5, tx_path => crypto:strong_rand_bytes(1024) },
	?assertEqual({ok, Proof3},
			ar_serialize:binary_to_poa(ar_serialize:poa_map_to_binary(Proof3))),
	Proof4 = Proof3#{ packing => {spora_2_6, crypto:strong_rand_bytes(33)} },
	?assertEqual({ok, Proof4},
			ar_serialize:binary_to_poa(ar_serialize:poa_map_to_binary(Proof4))).

poa_no_chunk_map_to_binary_test() ->
	Proof = #{ data_path => crypto:strong_rand_bytes(500),
		tx_path => crypto:strong_rand_bytes(250) },
	?assertEqual({ok, Proof},
			ar_serialize:binary_to_no_chunk_map(
				ar_serialize:poa_no_chunk_map_to_binary(Proof))).

block_index_to_binary_test() ->
	lists:foreach(
		fun(BI) ->
			?assertEqual({ok, BI}, ar_serialize:binary_to_block_index(
				ar_serialize:block_index_to_binary(BI)))
		end,
		[[], [{crypto:strong_rand_bytes(48), rand:uniform(1000),
				crypto:strong_rand_bytes(32)}],
			[{crypto:strong_rand_bytes(48), 0, <<>>}],
			[{crypto:strong_rand_bytes(48), rand:uniform(1000),
				crypto:strong_rand_bytes(32)} || _ <- lists:seq(1, 1000)]]).

%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> infinity end},
			{ar_fork, height_2_6_8, fun() -> infinity end},
			{ar_fork, height_2_7, fun() -> infinity end}],
		fun test_block_roundtrip/0).

test_block_roundtrip() ->
	[B] = ar_weave:init(),
	TXIDs = [TX#tx.id || TX <- B#block.txs],
	JSONStruct = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	BRes = ar_serialize:json_struct_to_block(JSONStruct),
	?assertEqual(B#block{ txs = TXIDs, size_tagged_txs = [], account_tree = undefined },
			BRes#block{ hash_list = B#block.hash_list, size_tagged_txs = [] }).

%% @doc Convert a new TX into JSON and back, ensure the result is the same.
tx_roundtrip_test() ->
	TXBase = ar_tx:new(<<"test">>),
	TX =
		TXBase#tx{
			format = 2,
			tags = [{<<"Name1">>, <<"Value1">>}],
			data_root = << 0:256 >>,
			signature_type = ?DEFAULT_KEY_TYPE
		},
	JsonTX = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
	?assertEqual(
		TX,
		ar_serialize:json_struct_to_tx(JsonTX)
	).

wallet_list_roundtrip_test_() ->
	{timeout, 30, fun test_wallet_list_roundtrip/0}.

test_wallet_list_roundtrip() ->
	[B] = ar_weave:init(),
	WL = B#block.account_tree,
	JSONWL = ar_serialize:jsonify(
		ar_serialize:wallet_list_to_json_struct(B#block.reward_addr, false, WL)),
	ExpectedWL = ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], WL),
	ActualWL = ar_patricia_tree:foldr(
		fun(K, V, Acc) -> [{K, V} | Acc] end, [], ar_serialize:json_struct_to_wallet_list(JSONWL)
	),
	?assertEqual(ExpectedWL, ActualWL).

block_index_roundtrip_test_() ->
	{timeout, 10, fun test_block_index_roundtrip/0}.

test_block_index_roundtrip() ->
	[B] = ar_weave:init(),
	HL = [B#block.indep_hash, B#block.indep_hash],
	JSONHL = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(HL)),
	HL = ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(JSONHL)),
	BI = [{B#block.indep_hash, 1, <<"Root">>}, {B#block.indep_hash, 2, <<>>}],
	JSONBI = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(BI)),
	BI = ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(JSONBI)).

query_roundtrip_test() ->
	Query = {'equals', <<"TestName">>, <<"TestVal">>},
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
			Query
		)
	),
	?assertEqual({ok, Query}, ar_serialize:json_struct_to_query(QueryJSON)).

candidate_to_json_struct_test() ->

	Test = fun(Candidate) ->
        JSON = ar_serialize:jsonify(ar_serialize:candidate_to_json_struct(Candidate)),
		{ok, JSONStruct} = ar_serialize:json_decode(JSON, [return_maps]),
        CandidateAfter = ar_serialize:json_map_to_candidate(JSONStruct),
        ExpectedCandidate = Candidate#mining_candidate{
            cache_ref = not_set,
            chunk1 = not_set,
            chunk2 = not_set,
            cm_lead_peer = not_set
        },
        ?assertEqual(ExpectedCandidate, CandidateAfter)
    end,

	DefaultCandidate = #mining_candidate{
        cm_diff = {rand:uniform(1024), rand:uniform(1024)},
		cm_h1_list = [
			{crypto:strong_rand_bytes(32), rand:uniform(100)},
			{crypto:strong_rand_bytes(32), rand:uniform(100)},
			{crypto:strong_rand_bytes(32), rand:uniform(100)}
		],
        h0 = crypto:strong_rand_bytes(32),
        h1 = crypto:strong_rand_bytes(32),
        h2 = crypto:strong_rand_bytes(32),
        mining_address = crypto:strong_rand_bytes(32),
        next_seed = crypto:strong_rand_bytes(32),
		next_vdf_difficulty = rand:uniform(100),
        nonce = rand:uniform(100),
        nonce_limiter_output = crypto:strong_rand_bytes(32),
        partition_number = rand:uniform(100),
        partition_number2 = rand:uniform(100),
        partition_upper_bound = rand:uniform(100),
        poa2 = #poa{
			chunk = crypto:strong_rand_bytes(256 * 1024),
			data_path = crypto:strong_rand_bytes(1024),
 			tx_path = crypto:strong_rand_bytes(1024) },
        preimage = crypto:strong_rand_bytes(32),
        seed = crypto:strong_rand_bytes(32),
		session_key = {crypto:strong_rand_bytes(32), rand:uniform(100), rand:uniform(10000)},
        start_interval_number = rand:uniform(100),
        step_number = rand:uniform(100)
    },

	Test(DefaultCandidate),

	%% clear optional fields
	Test(DefaultCandidate#mining_candidate{
		cm_h1_list = [],
		h1 = not_set,
		h2 = not_set,
		nonce = not_set,
		poa2 = not_set,
		preimage = not_set}),

	%% set unserialized fields
	Test(DefaultCandidate#mining_candidate{
		cache_ref = {rand:uniform(100), rand:uniform(100), rand:uniform(100), make_ref()},
		chunk1 = crypto:strong_rand_bytes(256 * 1024),
		chunk2 = crypto:strong_rand_bytes(256 * 1024),
		cm_lead_peer = ar_test_node:peer_ip(main)}).

solution_to_json_struct_test() ->

	Test = fun(Solution) ->
        JSON = ar_serialize:jsonify(ar_serialize:solution_to_json_struct(Solution)),
		{ok, JSONStruct} = ar_serialize:json_decode(JSON, [return_maps]),
        SolutionAfter = ar_serialize:json_map_to_solution(JSONStruct),
        ?assertEqual(Solution, SolutionAfter)
    end,

	DefaultSolution = #mining_solution{
		last_step_checkpoints = [
			crypto:strong_rand_bytes(32),
			crypto:strong_rand_bytes(32),
			crypto:strong_rand_bytes(32)],
		mining_address = crypto:strong_rand_bytes(32),
		next_seed = crypto:strong_rand_bytes(32),
		next_vdf_difficulty = rand:uniform(100),
		nonce = rand:uniform(100),
		nonce_limiter_output = crypto:strong_rand_bytes(32),
		partition_number = rand:uniform(100),
		partition_upper_bound = rand:uniform(100),
		poa1 = #poa{
			chunk = crypto:strong_rand_bytes(256 * 1024),
			data_path = crypto:strong_rand_bytes(1024),
 			tx_path = crypto:strong_rand_bytes(1024) },
		poa2 = #poa{
			chunk = crypto:strong_rand_bytes(256 * 1024),
			data_path = crypto:strong_rand_bytes(1024),
 			tx_path = crypto:strong_rand_bytes(1024) },
		preimage = crypto:strong_rand_bytes(32),
		recall_byte1 = rand:uniform(100),
		recall_byte2 = rand:uniform(100),
		seed = crypto:strong_rand_bytes(32),
		solution_hash = crypto:strong_rand_bytes(32),
		start_interval_number = rand:uniform(100),
		step_number = rand:uniform(100),
		steps = [
			crypto:strong_rand_bytes(32),
			crypto:strong_rand_bytes(32),
			crypto:strong_rand_bytes(32)]
	},

	Test(DefaultSolution),

	%% clear optional fields
	Test(DefaultSolution#mining_solution{
		recall_byte2 = undefined}).

partial_solution_to_json_struct_test() ->
	TestCases = [
		#mining_solution{
			mining_address = <<"a">>,
			next_seed = <<"s">>,
			seed = <<"s">>,
			next_vdf_difficulty = 1,
			nonce = 2,
			partition_number = 10,
			partition_upper_bound = 5001,
			solution_hash = <<"h">>,
			nonce_limiter_output = <<"output">>,
			preimage = <<"pr">>,
			poa1 = #poa{ chunk = <<"c">>, tx_path = <<"t">>, data_path = <<"dpath">> },
			poa2 = #poa{},
			recall_byte1 = 123234234234,
			recall_byte2 = undefined,
			start_interval_number = 23,
			step_number = 1113423423423423423423423432342342342344
		},
		#mining_solution{
			mining_address = <<"a">>,
			next_seed = <<"s">>,
			seed = <<"s">>,
			next_vdf_difficulty = 1,
			nonce = 2,
			partition_number = 10,
			partition_upper_bound = 5001,
			solution_hash = <<"h">>,
			nonce_limiter_output = <<"output">>,
			preimage = <<"pr">>,
			poa1 = #poa{ chunk = <<"c">>, tx_path = <<"t">>, data_path = <<"dpath">> },
			poa2 = #poa{ chunk = <<"chunk2">>, tx_path = <<"t2">>, data_path = <<"d2">> },
			recall_byte1 = 123234234234,
			recall_byte2 = 2,
			start_interval_number = 23,
			step_number = 1113423423423423423423423432342342342344
		}
	],
	lists:foreach(
		fun(Solution) ->
			?assertEqual(Solution,
					ar_serialize:json_map_to_solution(jiffy:decode(ar_serialize:jsonify(
							ar_serialize:solution_to_json_struct(Solution)), [return_maps])))
		end,
		TestCases
	).

partial_solution_response_to_json_struct_test() ->
	TestCases = [
		{#partial_solution_response{}, <<>>, <<>>},
		{#partial_solution_response{ indep_hash = <<"H">>, status = <<"S">>},
				<<"H">>, <<"S">>}
	],
	lists:foreach(
		fun({Case, ExpectedH, ExpectedStatus}) ->
			{Struct} = ar_serialize:dejsonify(ar_serialize:jsonify(
					ar_serialize:partial_solution_response_to_json_struct(Case))),
			?assertEqual(ExpectedH,
					ar_util:decode(proplists:get_value(<<"indep_hash">>, Struct))),
			?assertEqual(ExpectedStatus, proplists:get_value(<<"status">>, Struct))
		end,
		TestCases
	).

jobs_to_json_struct_test() ->
	TestCases = [
		#jobs{}
		#jobs{ seed = <<"a">> },
		#jobs{ jobs = [#job{ output = <<"o">>,
				global_step_number = 1,
				partition_upper_bound = 100 }] },
		#jobs{ jobs = [#job{ output = <<"o2">>,
					global_step_number = 2,
					partition_upper_bound = 100 }, #job{ output = <<"o1">>,
						global_step_number = 1,
						partition_upper_bound = 99 }],
				partial_diff = {12345, 6789},
				seed = <<"gjhgjkghjhg">>,
				next_seed = <<"dfdgfdg">>,
				interval_number = 23,
				next_vdf_difficulty = 32434 }
	],
	lists:foreach(
		fun(Jobs) ->
			?assertEqual(Jobs,
					ar_serialize:json_struct_to_jobs(
						ar_serialize:dejsonify(ar_serialize:jsonify(
							ar_serialize:jobs_to_json_struct(Jobs)))))
		end,
		TestCases
	).
