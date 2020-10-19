-module(ar_block).

-export([
	block_field_size_limit/1,
	verify_dep_hash/2,
	verify_timestamp/1,
	verify_height/2,
	verify_last_retarget/2,
	verify_previous_block/2,
	verify_block_hash_list/2,
	verify_weave_size/3,
	verify_cumulative_diff/2,
	verify_block_hash_list_merkle/3,
	verify_tx_root/1,
	hash_wallet_list/2,
	hash_wallet_list/3,
	hash_wallet_list_without_reward_wallet/2,
	generate_block_data_segment/1,
	generate_block_data_segment/2,
	generate_block_data_segment/3,
	generate_block_data_segment_base/1,
	generate_hash_list_for_block/2,
	generate_tx_root_for_block/1,
	generate_size_tagged_list_from_txs/1,
	generate_tx_tree/1, generate_tx_tree/2,
	compute_hash_list_merkle/2, compute_hash_list_merkle/1,
	test_wallet_list_performance/1,
	poa_to_list/1
]).

-include("ar.hrl").
-include("common.hrl").
-include_lib("eunit/include/eunit.hrl").

hash_wallet_list(Height, RewardAddr, WalletList) ->
	case Height >= ar_fork:height_2_0() of
		true ->
			case Height < ar_fork:height_2_2() of
				true ->
					{hash_wallet_list_pre_2_2(RewardAddr, WalletList), WalletList};
				false ->
					ar_patricia_tree:compute_hash(
						WalletList,
						fun(Addr, {Balance, LastTX}) ->
							ar_deep_hash:hash([Addr, binary:encode_unsigned(Balance), LastTX])
						end
					)
			end
	end.

hash_wallet_list_pre_2_2(RewardAddr, WalletList) ->
	{RewardWallet, NoRewardWalletListHash} =
		hash_wallet_list_without_reward_wallet(RewardAddr, WalletList),
	hash_wallet_list(RewardWallet, NoRewardWalletListHash).

hash_wallet_list_without_reward_wallet(RewardAddr, WalletList) ->
	RewardWallet =
		case ar_patricia_tree:get(RewardAddr, WalletList) of
			not_found ->
				unclaimed;
			{Balance, LastTX} ->
				{RewardAddr, Balance, LastTX}
		end,
	NoRewardWLH = ar_deep_hash:hash(
		ar_patricia_tree:foldr(
			fun(A, {B, Anchor}, Acc) ->
				case A == RewardAddr of
					true ->
						Acc;
					false ->
						[[A, binary:encode_unsigned(B), Anchor] | Acc]
				end
			end,
			[],
			WalletList
		)
	),
	{RewardWallet, NoRewardWLH}.

hash_wallet_list(RewardWallet, NoRewardWalletListHash) ->
	ar_deep_hash:hash([
		NoRewardWalletListHash,
		case RewardWallet of
			unclaimed ->
				<<"unclaimed">>;
			{Address, Balance, Anchor} ->
				[Address, binary:encode_unsigned(Balance), Anchor]
		end
	]).

%% @doc Generate the TX tree and set the TX root for a block.
generate_tx_tree(B) ->
	SizeTaggedTXs = generate_size_tagged_list_from_txs(B#block.txs),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	generate_tx_tree(B, SizeTaggedDataRoots).
generate_tx_tree(B, SizeTaggedTXs) ->
	{Root, Tree} = ar_merkle:generate_tree(SizeTaggedTXs),
	B#block { tx_tree = Tree, tx_root = Root }.

generate_size_tagged_list_from_txs(TXs) ->
	lists:reverse(
		element(2,
			lists:foldl(
				fun(TX, {Pos, List}) ->
					End = Pos + TX#tx.data_size,
					{End, [{{TX#tx.id, get_tx_data_root(TX)}, End} | List]}
				end,
				{0, []},
				lists:sort(TXs)
			)
		)
	).

%% @doc Find the appropriate block hash list for a block, from a
%% block index.
generate_hash_list_for_block(_BlockOrHash, []) -> [];
generate_hash_list_for_block(B, BI) when ?IS_BLOCK(B) ->
	generate_hash_list_for_block(B#block.indep_hash, BI);
generate_hash_list_for_block(Hash, BI) ->
	do_generate_hash_list_for_block(Hash, BI).

do_generate_hash_list_for_block(_, []) ->
	error(cannot_generate_hash_list);
do_generate_hash_list_for_block(IndepHash, [{IndepHash, _, _} | BI]) -> ?BI_TO_BHL(BI);
do_generate_hash_list_for_block(IndepHash, [_ | Rest]) ->
	do_generate_hash_list_for_block(IndepHash, Rest).

%% @doc Given a block checks that the lengths conform to the specified limits.
block_field_size_limit(B = #block { reward_addr = unclaimed }) ->
	block_field_size_limit(B#block { reward_addr = <<>> });
block_field_size_limit(B) ->
	DiffBytesLimit = case ar_fork:height_1_8() of
		H when B#block.height >= H ->
			78;
		_ ->
			10
	end,
	{ChunkSize, DataPathSize} =
		case B#block.poa of
			POA when is_record(POA, poa) ->
				{
					byte_size((B#block.poa)#poa.chunk),
					byte_size((B#block.poa)#poa.data_path)
				};
			_ -> {0, 0}
		end,
	Check = (byte_size(B#block.nonce) =< 512) and
		(byte_size(B#block.previous_block) =< 48) and
		(byte_size(integer_to_binary(B#block.timestamp)) =< 12) and
		(byte_size(integer_to_binary(B#block.last_retarget)) =< 12) and
		(byte_size(integer_to_binary(B#block.diff)) =< DiffBytesLimit) and
		(byte_size(integer_to_binary(B#block.height)) =< 20) and
		(byte_size(B#block.hash) =< 48) and
		(byte_size(B#block.indep_hash) =< 48) and
		(byte_size(B#block.reward_addr) =< 32) and
		(byte_size(list_to_binary(B#block.tags)) =< 2048) and
		(byte_size(integer_to_binary(B#block.weave_size)) =< 64) and
		(byte_size(integer_to_binary(B#block.block_size)) =< 64) and
		(ChunkSize =< ?DATA_CHUNK_SIZE) and
		(DataPathSize =< ?MAX_PATH_SIZE),
	case Check of
		false ->
			?LOG_INFO(
				[
					{event, received_block_with_invalid_field_size},
					{nonce, byte_size(B#block.nonce)},
					{previous_block, byte_size(B#block.previous_block)},
					{timestamp, byte_size(integer_to_binary(B#block.timestamp))},
					{last_retarget, byte_size(integer_to_binary(B#block.last_retarget))},
					{diff, byte_size(integer_to_binary(B#block.diff))},
					{height, byte_size(integer_to_binary(B#block.height))},
					{hash, byte_size(B#block.hash)},
					{indep_hash, byte_size(B#block.indep_hash)},
					{reward_addr, byte_size(B#block.reward_addr)},
					{tags, byte_size(list_to_binary(B#block.tags))},
					{weave_size, byte_size(integer_to_binary(B#block.weave_size))},
					{block_size, byte_size(integer_to_binary(B#block.block_size))}
				]
			);
		_ ->
			ok
	end,
	Check.

compute_hash_list_merkle(B, BI) ->
	NewHeight = B#block.height + 1,
	Fork_2_0 = ar_fork:height_2_0(),
	case NewHeight of
		_ when NewHeight < ?FORK_1_6 ->
			<<>>;
		?FORK_1_6 ->
			ar_unbalanced_merkle:hash_list_to_merkle_root(B#block.hash_list);
		_ when NewHeight < Fork_2_0 ->
			ar_unbalanced_merkle:root(B#block.hash_list_merkle, B#block.indep_hash);
		Fork_2_0 ->
			ar_unbalanced_merkle:block_index_to_merkle_root(BI);
		_ ->
			compute_hash_list_merkle(B)
	end.

compute_hash_list_merkle(B) ->
	ar_unbalanced_merkle:root(
		B#block.hash_list_merkle,
		{B#block.indep_hash, B#block.weave_size, B#block.tx_root},
		fun ar_unbalanced_merkle:hash_block_index_entry/1
	).

%% @doc Generate a block data segment.
%% Block data segment is combined with a nonce to compute a PoW hash.
%% Also, it is combined with a nonce and the corresponding PoW hash
%% to produce the independent hash.
generate_block_data_segment(B) ->
	generate_block_data_segment(
		generate_block_data_segment_base(B),
		B
	).

generate_block_data_segment(BDSBase, B) ->
	generate_block_data_segment(
		BDSBase,
		B#block.hash_list_merkle,
		#{
			timestamp => B#block.timestamp,
			last_retarget => B#block.last_retarget,
			diff => B#block.diff,
			cumulative_diff => B#block.cumulative_diff,
			reward_pool => B#block.reward_pool,
			wallet_list => B#block.wallet_list
		}
	).

generate_block_data_segment(BDSBase, BlockIndexMerkle, TimeDependentParams) ->
	#{
		timestamp := Timestamp,
		last_retarget := LastRetarget,
		diff := Diff,
		cumulative_diff := CDiff,
		reward_pool := RewardPool,
		wallet_list := WalletListHash
	} = TimeDependentParams,
	ar_deep_hash:hash([
		BDSBase,
		integer_to_binary(Timestamp),
		integer_to_binary(LastRetarget),
		integer_to_binary(Diff),
		integer_to_binary(CDiff),
		integer_to_binary(RewardPool),
		WalletListHash,
		BlockIndexMerkle
	]).

%% @doc Generate a hash, which is used to produce a block data segment
%% when combined with the time-dependent parameters, which frequently
%% change during mining - timestamp, last retarget timestamp, difficulty,
%% cumulative difficulty, miner's wallet, reward pool. Also excludes
%% the merkle root of the block index, which is hashed with the rest
%% as the last step, to allow verifiers to quickly validate PoW against
%% the current state.
generate_block_data_segment_base(B) ->
	case B#block.height >= ar_fork:height_2_3() of
		true ->
			ar_deep_hash:hash([
				integer_to_binary(B#block.height),
				B#block.previous_block,
				B#block.tx_root,
				lists:map(fun ar_weave:tx_id/1, B#block.txs),
				integer_to_binary(B#block.block_size),
				integer_to_binary(B#block.weave_size),
				case B#block.reward_addr of
					unclaimed ->
						<<"unclaimed">>;
					_ ->
						B#block.reward_addr
				end,
				ar_tx:tags_to_list(B#block.tags)
			]);
		false ->
			ar_deep_hash:hash([
				integer_to_binary(B#block.height),
				B#block.previous_block,
				B#block.tx_root,
				lists:map(fun ar_weave:tx_id/1, B#block.txs),
				integer_to_binary(B#block.block_size),
				integer_to_binary(B#block.weave_size),
				case B#block.reward_addr of
					unclaimed ->
						<<"unclaimed">>;
					_ ->
						B#block.reward_addr
				end,
				ar_tx:tags_to_list(B#block.tags),
				poa_to_list(B#block.poa)
			])
	end.

poa_to_list(POA) ->
	[
		integer_to_binary(POA#poa.option),
		POA#poa.tx_path,
		POA#poa.data_path,
		POA#poa.chunk
	].

%% @doc Verify the dependent hash of a given block is valid
verify_dep_hash(NewB, BDSHash) ->
	NewB#block.hash == BDSHash.

verify_tx_root(B) ->
	B#block.tx_root == generate_tx_root_for_block(B).

%% @doc Given a list of TXs in various formats, or a block, generate the
%% correct TX merkle tree root.
generate_tx_root_for_block(B) when is_record(B, block) ->
	generate_tx_root_for_block(B#block.txs);
generate_tx_root_for_block(TXIDs = [TXID | _]) when is_binary(TXID) ->
	generate_tx_root_for_block(ar_storage:read_tx(TXIDs));
generate_tx_root_for_block([]) ->
	<<>>;
generate_tx_root_for_block(TXs = [TX | _]) when is_record(TX, tx) ->
	SizeTaggedTXs = generate_size_tagged_list_from_txs(TXs),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	Root.

get_tx_data_root(#tx{ format = 2, data_root = DataRoot }) ->
	DataRoot;
get_tx_data_root(TX) ->
	(ar_tx:generate_chunk_tree(TX))#tx.data_root.

%% @doc Verify the block timestamp is not too far in the future nor too far in
%% the past. We calculate the maximum reasonable clock difference between any
%% two nodes. This is a simplification since there is a chaining effect in the
%% network which we don't take into account. Instead, we assume two nodes can
%% deviate JOIN_CLOCK_TOLERANCE seconds in the opposite direction from each
%% other.
verify_timestamp(B) ->
	CurrentTime = os:system_time(seconds),
	MaxNodesClockDeviation = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	(
		B#block.timestamp =< CurrentTime + MaxNodesClockDeviation
		andalso
		B#block.timestamp >= CurrentTime - lists:sum([
			?MINING_TIMESTAMP_REFRESH_INTERVAL,
			?MAX_BLOCK_PROPAGATION_TIME,
			MaxNodesClockDeviation
		])
	).

%% @doc Verify the height of the new block is the one higher than the
%% current height.
verify_height(NewB, OldB) ->
	NewB#block.height == (OldB#block.height + 1).

%% @doc Verify the retarget timestamp on NewB is correct.
verify_last_retarget(NewB, OldB) ->
	case ar_retarget:is_retarget_height(NewB#block.height) of
		true ->
			NewB#block.last_retarget == NewB#block.timestamp;
		false ->
			NewB#block.last_retarget == OldB#block.last_retarget
	end.

%% @doc Verify that the previous_block hash of the new block is the indep_hash
%% of the current block.
verify_previous_block(NewB, OldB) ->
	OldB#block.indep_hash == NewB#block.previous_block.

%% @doc Verify that the new block's hash_list is the current block's
%% hash_list + indep_hash, until ?FORK_1_6.
verify_block_hash_list(NewB, OldB) when NewB#block.height < ?FORK_1_6 ->
	NewB#block.hash_list == [OldB#block.indep_hash | OldB#block.hash_list];
verify_block_hash_list(_NewB, _OldB) -> true.

verify_weave_size(NewB, OldB, TXs) ->
	NewB#block.weave_size == lists:foldl(
		fun(TX, Acc) ->
			Acc + TX#tx.data_size
		end,
		OldB#block.weave_size,
		TXs
	).

%% @doc Ensure that after the 1.6 release cumulative difficulty is enforced.
verify_cumulative_diff(NewB, OldB) ->
	NewB#block.cumulative_diff ==
		ar_difficulty:next_cumulative_diff(
			OldB#block.cumulative_diff,
			NewB#block.diff,
			NewB#block.height
		).

%% @doc After 1.6 fork check that the given merkle root in a new block is valid.
verify_block_hash_list_merkle(NewB, CurrentB, BI) when NewB#block.height > ?FORK_1_6 ->
	Fork_2_0 = ar_fork:height_2_0(),
	case NewB#block.height of
		H when H < Fork_2_0 ->
			NewB#block.hash_list_merkle ==
				ar_unbalanced_merkle:root(CurrentB#block.hash_list_merkle, CurrentB#block.indep_hash);
		Fork_2_0 ->
			NewB#block.hash_list_merkle == ar_unbalanced_merkle:block_index_to_merkle_root(BI);
		_ ->
			NewB#block.hash_list_merkle ==
				ar_unbalanced_merkle:root(
					CurrentB#block.hash_list_merkle,
					{CurrentB#block.indep_hash, CurrentB#block.weave_size, CurrentB#block.tx_root},
					fun ar_unbalanced_merkle:hash_block_index_entry/1
				)
	end;
verify_block_hash_list_merkle(NewB, _CurrentB, _) when NewB#block.height < ?FORK_1_6 ->
	NewB#block.hash_list_merkle == <<>>;
verify_block_hash_list_merkle(NewB, CurrentB, _) when NewB#block.height == ?FORK_1_6 ->
	NewB#block.hash_list_merkle == ar_unbalanced_merkle:hash_list_to_merkle_root(CurrentB#block.hash_list).

%% Tests: ar_block

hash_list_gen_test() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	ar_node:mine(),
	BI1 = ar_test_node:wait_until_height(1),
	B1 = ar_storage:read_block(hd(BI1)),
	ar_node:mine(),
	BI2 = ar_test_node:wait_until_height(2),
	B2 = ar_storage:read_block(hd(BI2)),
	?assertEqual([B0#block.indep_hash], generate_hash_list_for_block(B1, BI2)),
	?assertEqual([H || {H, _, _} <- BI1], generate_hash_list_for_block(B2#block.indep_hash, BI2)).

test_wallet_list_performance(Length) ->
	io:format("# ~B wallets~n", [Length]),
	io:format("============~n"),
	WL = [random_wallet() || _ <- lists:seq(1, Length)],
	{Time1, T1} =
		timer:tc(
			fun() ->
				lists:foldl(
					fun({A, B, LastTX}, Acc) -> ar_patricia_tree:insert(A, {B, LastTX}, Acc) end,
					ar_patricia_tree:new(),
					WL
				)
			end
		),
	io:format("tree buildup                    | ~f seconds~n", [Time1 / 1000000]),
	{Time2, Binary} =
		timer:tc(
			fun() ->
				ar_serialize:jsonify(ar_serialize:wallet_list_to_json_struct(unclaimed, false, T1))
			end
		),
	io:format("serialization                   | ~f seconds~n", [Time2 / 1000000]),
	io:format("                                | ~B bytes~n", [byte_size(Binary)]),
	{Time3, {_, T2}} =
		timer:tc(
			fun() ->
				ar_patricia_tree:compute_hash(
					T1,
					fun(A, {B, L}) ->
						ar_deep_hash:hash([A, binary:encode_unsigned(B), L])
					end
				)
			end
		),
	io:format("root hash from scratch          | ~f seconds~n", [Time3 / 1000000]),
	{Time4, T3} =
		timer:tc(
			fun() ->
				lists:foldl(
					fun({A, B, LastTX}, Acc) ->
						ar_patricia_tree:insert(A, {B, LastTX}, Acc)
					end,
					T2,
					[random_wallet() || _ <- lists:seq(1, 2000)]
				)
			end
		),
	io:format("2000 inserts                    | ~f seconds~n", [Time4 / 1000000]),
	{Time5, _} =
		timer:tc(
			fun() ->
				ar_patricia_tree:compute_hash(
					T3,
					fun(A, {B, L}) ->
						ar_deep_hash:hash([A, binary:encode_unsigned(B), L])
					end
				)
			end
		),
	io:format("recompute hash after 2k inserts | ~f seconds~n", [Time5 / 1000000]),
	{Time6, T4} =
		timer:tc(
			fun() ->
				{A, B, LastTX} = random_wallet(),
				ar_patricia_tree:insert(A, {B, LastTX}, T2)
			end
		),
	io:format("1 insert                        | ~f seconds~n", [Time6 / 1000000]),
	{Time7, _} =
		timer:tc(
			fun() ->
				ar_patricia_tree:compute_hash(
					T4,
					fun(A, {B, L}) ->
						ar_deep_hash:hash([A, binary:encode_unsigned(B), L])
					end
				)
			end
		),
	io:format("recompute hash after 1 insert   | ~f seconds~n", [Time7 / 1000000]).

random_wallet() ->
	{
		crypto:strong_rand_bytes(32),
		rand:uniform(1000000000000000000),
		crypto:strong_rand_bytes(32)
	}.
