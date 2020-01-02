-module(ar_weave).
-export([init/0, init/1, init/2, init/3, add/1, add/2, add/3, add/4, add/6, add/7, add/11]).
-export([hash/3, indep_hash/1, header_hash/1]).
-export([verify_indep/2]).
-export([generate_block_index/1]).
-export([is_data_on_block_list/2]).
-export([create_genesis_txs/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Utilities for manipulating the ARK weave datastructure.

%% @doc Start a new block list. Optionally takes a list of wallet values
%% for the genesis block.
-ifdef(DEBUG).
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ar_mine:genesis_difficulty(), 0).
init(WalletList, Diff) -> init(WalletList, Diff, 0).
init(WalletList, StartingDiff, RewardPool) ->
	ar_randomx_state:reset(),
	% Generate and dispatch a new data transaction
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = [],
			wallet_list = WalletList,
			block_index = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			votables =
				case ?FORK_2_0 of
					0 -> ar_votable:init();
					_ -> []
				end
		},
	B1TS = B0#block { last_retarget = B0#block.timestamp },
	B1 = B1TS#block { header_hash = header_hash(B1TS) },
	[B1#block { indep_hash = indep_hash(B1) }].
-else.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ar_mine:genesis_difficulty()).
init(WalletList, Diff) -> init(WalletList, Diff, 0).
init(WalletList, StartingDiff, RewardPool) ->
	ar_randomx_state:reset(),
	% Generate and dispatch a new data transaction.
	TXs = read_genesis_txs(),
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = TXs,
			wallet_list = WalletList,
			block_index = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			votables = ar_votable:init()
		},
	B1TS = B0#block { last_retarget = B0#block.timestamp },
	B1 = B1TS#block { header_hash = header_hash(B1TS) },
	[B1#block { indep_hash = indep_hash(B1) }].
-endif.

%% @doc Add a new block to the weave, with assiocated TXs and archive data.
add(Bs) ->
	add(Bs, []).
add(Bs, TXs) ->
	add(Bs, TXs, generate_block_index(Bs)).
add(Bs, TXs, BI) ->
	add(Bs, TXs, BI, <<>>).
add(Bs, TXs, BI, unclaimed) ->
	add(Bs, TXs, BI, <<>>);
add(AllBs = [B|Bs], TXs, BI, RewardAddr) ->
	ar_storage:write_block([ XB || XB <- AllBs, is_record(XB, block) ]),
	POA = ar_poa:generate(B),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			B#block.reward_pool,
			TXs,
			RewardAddr,
			POA,
			B#block.weave_size,
			B#block.height,
			B#block.diff,
			B#block.timestamp
		),
	WalletList = ar_node_utils:apply_mining_reward(
		ar_node_utils:apply_txs(B#block.wallet_list, TXs, length(BI) - 1),
		RewardAddr,
		FinderReward,
		length(BI)
	),
	add([B|Bs], TXs, BI, RewardAddr, RewardPool, WalletList).
add(Bs, TXs, BI, RewardAddr, RewardPool, WalletList) ->
	add(Bs, TXs, BI, RewardAddr, RewardPool, WalletList, []).
add([{Hash,_}|Bs], TXs, BI, RewardAddr, RewardPool, WalletList, Tags) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, BI)|Bs],
		TXs,
		BI,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags
	);
add(Bs, TXs, BI, RewardAddr, RewardPool, WalletList, Tags) ->
	POA = ar_poa:generate(hd(Bs)),
	{Nonce, Timestamp, Diff} = mine(hd(Bs), POA, TXs, RewardAddr, Tags),
	add(
		Bs,
		TXs,
		BI,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		POA,
		Diff,
		Nonce,
		Timestamp
	).
add([{Hash,_}|Bs], RawTXs, BI, RewardAddr, RewardPool, WalletList, Tags, POA, Diff, Nonce, Timestamp) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, BI)|Bs],
		RawTXs,
		BI,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		POA,
		Diff,
		Nonce,
		Timestamp
	);
add([CurrentB|_Bs], RawTXs, BI, RewardAddr, RewardPool, WalletList, Tags, POA, Diff, Nonce, Timestamp) ->
	NewHeight = CurrentB#block.height + 1,
	TXs = [T#tx.id || T <- RawTXs],
	TXRoot = ar_block:generate_tx_root_for_block(RawTXs),
	BlockSize = lists:foldl(
			fun(TX, Acc) ->
				Acc + byte_size(TX#tx.data)
			end,
			0,
			RawTXs
		),
	NewHeight = CurrentB#block.height + 1,
	CDiff =
		case NewHeight >= ?FORK_1_6 of
			true ->
				ar_difficulty:next_cumulative_diff(
					CurrentB#block.cumulative_diff,
					Diff,
					NewHeight
				);
			false ->
				0
		end,
	{MR, NewBI} =
		case NewHeight of
			_ when NewHeight == ?FORK_2_0 ->
				CP = [{H, _}|_] =
					ar_transition:generate_checkpoint(
						[{CurrentB#block.indep_hash, CurrentB#block.weave_size}|CurrentB#block.block_index]),
				ar:info(
					[
						performing_v2_block_index_transition,
						{new_block_index_root, ar_util:encode(H)}
					]
				),
				{H, CP};
			_ when NewHeight > ?FORK_2_0 ->
				{ar_unbalanced_merkle:root(CurrentB#block.block_index_merkle, CurrentB#block.header_hash), BI};
			_ when NewHeight < ?FORK_1_6 ->
				{<<>>, BI};
			_ when NewHeight == ?FORK_1_6 ->
				{ar_unbalanced_merkle:block_block_index_to_merkle_root(CurrentB#block.block_index), BI};
			_ ->
				{ar_unbalanced_merkle:root(CurrentB#block.block_index_merkle, CurrentB#block.indep_hash), BI}
		end,
	NewVotables =
		case NewHeight of
			X when X == ?FORK_2_0 -> ar_votable:init();
			X when X > ?FORK_2_0 -> ar_votable:vote(CurrentB#block.votables);
			_ -> []
		end,
	NewB =
		#block {
			nonce = Nonce,
			previous_block =
				case NewHeight >= ?FORK_2_0 of
					true -> CurrentB#block.header_hash;
					false -> CurrentB#block.indep_hash
				end,
			timestamp = Timestamp,
			last_retarget =
				case ar_retarget:is_retarget_height(NewHeight) of
					true -> Timestamp;
					false -> CurrentB#block.last_retarget
				end,
			diff = Diff,
			cumulative_diff = CDiff,
			height = NewHeight,
			hash = hash(
				ar_block:generate_block_data_segment(
					CurrentB,
					POA,
					RawTXs,
					RewardAddr,
					Timestamp,
					Tags
				),
				Nonce,
				NewHeight
			),
			txs = TXs,
			tx_root = TXRoot,
			block_index = NewBI,
			block_index_merkle = MR,
			wallet_list = WalletList,
			reward_addr = RewardAddr,
			tags = Tags,
			reward_pool = RewardPool,
			weave_size = CurrentB#block.weave_size + BlockSize,
			block_size = BlockSize,
			poa =
				case NewHeight >= ?FORK_2_0 of
					true -> POA;
					false -> undefined
				end,
			votables = NewVotables
		},
	NewBHH = NewB#block { header_hash = header_hash(NewB)},
	[
		NewBHH#block {
			indep_hash = indep_hash(NewBHH)
		}
	|BI].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_block_index(undefined) -> [];
generate_block_index([]) -> [];
generate_block_index(Bs = [B|_]) ->
	generate_block_index(Bs, B#block.height + 1).
generate_block_index([B = #block { block_index = BI }|_], _) when is_list(BI) ->
	[{B#block.indep_hash, B#block.weave_size}|BI];
generate_block_index([], 0) -> [];
generate_block_index([B|Bs], N) when is_record(B, block) ->
	[{B#block.indep_hash, B#block.weave_size}|generate_block_index(Bs, N - 1)];
generate_block_index([Hash|Bs], N) when is_binary(Hash) ->
	[{Hash, 0}|generate_block_index(Bs, N - 1)].

%% @doc Verify a block from a hash list. Hash lists are stored in reverse order
verify_indep(#block{ height = 0 }, []) -> true;
verify_indep(B = #block { height = Height }, BI) ->
	ReversedBI = lists:reverse(BI),
	{ExpectedIndepHash, _} = lists:nth(Height + 1, ReversedBI),
	ComputedIndepHash = indep_hash(B),
	BBI = B#block.block_index,
	ReversedBBI = lists:reverse(BBI),
	case ComputedIndepHash of
		ExpectedIndepHash ->
			true;
		_ ->
			ar:err([
				verify_indep_hash,
				{height, Height},
				{computed_indep_hash, ar_util:encode(ComputedIndepHash)},
				{expected_indep_hash, ar_util:encode(ExpectedIndepHash)},
				{block_index_length, length(BBI)},
				{block_index_latest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(BI, 10)))},
				{block_index_eariest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(ReversedBI, 10)))},
				{block_bi_latest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(BBI, 10)))},
				{block_bi_earliest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(ReversedBBI, 10)))}
			]),
			false
	end.

%% @doc Create the hash of the next block in the list, given a previous block,
%% and the TXs and the nonce.
hash(BDS, Nonce, Height) ->
	HashData = << Nonce/binary, BDS/binary >>,
	case Height >= ar_fork:height_1_7() of
		true ->
			ar_randomx_state:hash(Height, HashData);
		false ->
			crypto:hash(?MINING_HASH_ALG, HashData)
	end.

%% @doc Create an independent hash from a block. Independent hashes
%% verify a block's contents in isolation and are stored in a node's hash list.
indep_hash(#block { height = Height, header_hash = HH }) when Height >= ?FORK_2_0 ->
	HH;
indep_hash(B = #block { height = Height }) when Height >= ?FORK_1_6 ->
	ar_deep_hash:hash([
		B#block.nonce,
		B#block.previous_block,
		integer_to_binary(B#block.timestamp),
		integer_to_binary(B#block.last_retarget),
		integer_to_binary(B#block.diff),
		integer_to_binary(B#block.cumulative_diff),
		integer_to_binary(B#block.height),
		B#block.hash,
		B#block.block_index_merkle,
		[tx_id(TX) || TX <- B#block.txs],
		[[Addr, integer_to_binary(Balance), LastTX]
			||	{Addr, Balance, LastTX} <- B#block.wallet_list],
		case B#block.reward_addr of
			unclaimed -> <<"unclaimed">>;
			_ -> B#block.reward_addr
		end,
		ar_tx:tags_to_list(B#block.tags),
		integer_to_binary(B#block.reward_pool),
		integer_to_binary(B#block.weave_size),
		integer_to_binary(B#block.block_size)
	]);
indep_hash(#block {
		nonce = Nonce,
		previous_block = PrevHash,
		timestamp = TimeStamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = Hash,
		block_index = BI,
		txs = TXs,
		wallet_list = WalletList,
		reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize
	}) ->
	EncodeTX = fun (TX) -> ar_util:encode(tx_id(TX)) end,
	crypto:hash(
		?MINING_HASH_ALG,
		ar_serialize:jsonify(
			{
				[
					{nonce, ar_util:encode(Nonce)},
					{previous_block, ar_util:encode(PrevHash)},
					{timestamp, TimeStamp},
					{last_retarget, LastRetarget},
					{diff, Diff},
					{height, Height},
					{hash, ar_util:encode(Hash)},
					{indep_hash, ar_util:encode(<<>>)},
					{txs, lists:map(EncodeTX, TXs)},
					{block_index, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(BI))},
					{wallet_list,
						lists:map(
							fun({Wallet, Qty, Last}) ->
								{
									[
										{wallet, ar_util:encode(Wallet)},
										{quantity, Qty},
										{last_tx, ar_util:encode(Last)}
									]
								}
							end,
							WalletList
						)
					},
					{reward_addr,
						if RewardAddr == unclaimed -> list_to_binary("unclaimed");
						true -> ar_util:encode(RewardAddr)
						end
					},
					{tags, Tags},
					{reward_pool, RewardPool},
					{weave_size, WeaveSize},
					{block_size, BlockSize}
				]
			}
		)
	).

%% @doc Create an independent hash from a block. Independent hashes
%% verify a block's contents in isolation and are stored in a node's hash list.
header_hash(B) ->
	WLH = ar_block:hash_wallet_list(B#block.wallet_list),
	ar_deep_hash:hash([
		B#block.hash,
		B#block.nonce,
		B#block.previous_block,
		integer_to_binary(B#block.timestamp),
		integer_to_binary(B#block.last_retarget),
		integer_to_binary(B#block.diff),
		integer_to_binary(B#block.height),
		B#block.block_index_merkle,
		B#block.tx_root,
		WLH,
		case B#block.reward_addr of
			unclaimed -> <<"unclaimed">>;
			_ -> B#block.reward_addr
		end,
		ar_tx:tags_to_list(B#block.tags),

		integer_to_binary(B#block.reward_pool),
		integer_to_binary(B#block.weave_size),
		integer_to_binary(B#block.block_size),
		integer_to_binary(B#block.cumulative_diff)
	] ++
	case B#block.height >= ?FORK_2_0 of
		true ->
			lists:map(
				fun({Name, Value}) ->
					[list_to_binary(Name), integer_to_binary(Value)]
				end,
				B#block.votables
			);
		false -> []
	end).

%% @doc Returns the transaction id
tx_id(Id) when is_binary(Id) -> Id;
tx_id(TX) -> TX#tx.id.

%% @doc Spawn a miner and mine the current block synchronously. Used for testing.
%% Returns the nonce to use to add the block to the list.
mine(B, POA, TXs, RewardAddr, Tags) ->
	ar_mine:start(B, POA, TXs, RewardAddr, Tags, self(), []),
	receive
		{work_complete, _BH, TXs, _Hash, _POA, Diff, Nonce, Timestamp, _} ->
			{Nonce, Timestamp, Diff}
	end.

is_data_on_block_list(_, _) -> false.

read_genesis_txs() ->
	{ok, Files} = file:list_dir("data/genesis_txs"),
	lists:foldl(
		fun(F, Acc) ->
			file:copy("data/genesis_txs/" ++ F, ar_meta_db:get(data_dir) ++ "/" ++ ?TX_DIR ++ "/" ++ F),
			[ar_util:decode(hd(string:split(F, ".")))|Acc]
		end,
		[],
		Files
	).

create_genesis_txs() ->
	TXs = lists:map(
		fun({M}) ->
			{Priv, Pub} = ar_wallet:new(),
			LastTx = <<>>,
			Data = unicode:characters_to_binary(M),
			TX = ar_tx:new(Data, 0, LastTx),
			Reward = 0,
			SignedTX = ar_tx:sign_pre_fork_2_0(TX#tx{reward = Reward}, Priv, Pub),
			ar_storage:write_tx(SignedTX),
			SignedTX
		end,
		?GENESIS_BLOCK_MESSAGES
	),
	file:write_file("genesis_wallets.csv", lists:map(fun(T) -> binary_to_list(ar_util:encode(T#tx.id)) ++ "," end, TXs)),
	[T#tx.id || T <- TXs].
