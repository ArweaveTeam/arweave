-module(ar_weave).

-export([init/0, init/1, init/2, init/3, add/2, add/12]).
-export([hash/3, indep_hash/1, indep_hash_post_fork_2_0/1, indep_hash_post_fork_2_0/3]).
-export([verify_indep/2]).
-export([is_data_on_block_list/2]).
-export([create_genesis_txs/0, read_v1_genesis_txs/0]).

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
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = [],
			wallet_list = WalletList,
			wallet_list_hash = ar_block:hash_wallet_list(0, unclaimed, WalletList),
			hash_list = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			poa = #poa{},
			votables =
				case ar_fork:height_2_0() of
					0 -> ar_votable:init();
					_ -> []
				end
		},
	B1 = B0#block { last_retarget = B0#block.timestamp },
	[B1#block { indep_hash = indep_hash(B1) }].
-else.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ar_mine:genesis_difficulty()).
init(WalletList, Diff) -> init(WalletList, Diff, 0).
init(WalletList, StartingDiff, RewardPool) ->
	ar_randomx_state:reset(),
	% Generate and dispatch a new data transaction.
	TXs = read_v1_genesis_txs(),
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = TXs,
			wallet_list = WalletList,
			wallet_list_hash = ar_block:hash_wallet_list(0, unclaimed, WalletList),
			hash_list = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			votables = ar_votable:init(),
			poa = #poa{}
		},
	B1 = B0#block { last_retarget = B0#block.timestamp },
	[B1#block { indep_hash = indep_hash(B1) }].
-endif.

%% @doc Add a new block to the weave, with assiocated TXs and archive data.
%% DEPRECATED - only used in tests, mine blocks in tests instead.
add(Bs, TXs) ->
	add(Bs, TXs, generate_block_index(Bs), []).
add(Bs, TXs, BI, LegacyHL) ->
	add(Bs, TXs, BI, LegacyHL, <<>>).
add(AllBs = [B | Bs], TXs, BI, LegacyHL, RewardAddr) ->
	ar_storage:write_block([XB || XB <- AllBs, is_record(XB, block)]),
	case length(BI) == 1 of
		true ->
			ar_randomx_state:init(BI, []);
		false ->
			noop
	end,
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
	add([B | Bs], TXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList).
add(Bs, TXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList) ->
	add(Bs, TXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList, []).
add([{Hash, _} | Bs], TXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList, Tags) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, BI) | Bs],
		TXs,
		BI,
		LegacyHL,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags
	);
add(Bs, TXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList, Tags) ->
	POA = ar_poa:generate(hd(Bs)),
	{Nonce, Timestamp, Diff} = mine(hd(Bs), POA, TXs, RewardAddr, Tags, BI),
	add(
		Bs,
		TXs,
		BI,
		LegacyHL,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		POA,
		Diff,
		Nonce,
		Timestamp
	).
add([{Hash, _} | Bs], RawTXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList, Tags, POA, Diff, Nonce, Timestamp) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, BI) | Bs],
		RawTXs,
		BI,
		LegacyHL,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		POA,
		Diff,
		Nonce,
		Timestamp
	);
add([CurrentB | _Bs], RawTXs, BI, LegacyHL, RewardAddr, RewardPool, WalletList, Tags, POA, Diff, Nonce, Timestamp) ->
	NewHeight = CurrentB#block.height + 1,
	TXs = [T#tx.id || T <- RawTXs],
	TXRoot = ar_block:generate_tx_root_for_block(RawTXs),
	BlockSize = lists:foldl(
		fun(TX, Acc) ->
			Acc + TX#tx.data_size
		end,
		0,
		RawTXs
	),
	NewHeight = CurrentB#block.height + 1,
	CDiff = ar_difficulty:next_cumulative_diff(
		CurrentB#block.cumulative_diff,
		Diff,
		NewHeight
	),
	MR = ar_block:compute_hash_list_merkle(CurrentB, BI),
	Fork_2_0 = ar_fork:height_2_0(),
	NewVotables =
		case NewHeight of
			X when X == Fork_2_0 -> ar_votable:init();
			X when X > Fork_2_0 -> ar_votable:vote(CurrentB#block.votables);
			_ -> []
		end,
	NewB =
		#block {
			nonce = Nonce,
			previous_block = CurrentB#block.indep_hash,
			timestamp = Timestamp,
			last_retarget =
				case ar_retarget:is_retarget_height(NewHeight) of
					true -> Timestamp;
					false -> CurrentB#block.last_retarget
				end,
			diff = Diff,
			cumulative_diff = CDiff,
			height = NewHeight,
			txs = TXs,
			tx_root = TXRoot,
			hash_list = ?BI_TO_BHL(BI),
			legacy_hash_list = LegacyHL,
			hash_list_merkle = MR,
			wallet_list = WalletList,
			wallet_list_hash = ar_block:hash_wallet_list(NewHeight, RewardAddr, WalletList),
			reward_addr = RewardAddr,
			tags = Tags,
			reward_pool = RewardPool,
			weave_size = CurrentB#block.weave_size + BlockSize,
			block_size = BlockSize,
			poa =
				case NewHeight >= Fork_2_0 of
					true -> POA;
					false -> undefined
				end,
			votables = NewVotables
		},
	Hash = case NewHeight >= Fork_2_0 of
		true ->
			hash(
				ar_block:generate_block_data_segment(NewB),
				NewB#block.nonce,
				NewHeight
			);
		false ->
			hash(
				ar_block:generate_block_data_segment_pre_2_0(
					CurrentB,
					POA,
					RawTXs,
					RewardAddr,
					Timestamp,
					Tags
				),
				Nonce,
				NewHeight
			)
	end,
	[NewB#block { indep_hash = indep_hash(NewB#block { hash = Hash }), hash = Hash } | BI].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_block_index(undefined) -> [];
generate_block_index([]) -> [];
generate_block_index(Blocks) ->
	lists:map(
		fun
			(B) when ?IS_BLOCK(B) ->
				{B#block.indep_hash, B#block.weave_size};
			({H, WS}) -> {H, WS}
		end,
		Blocks
	).

%% @doc Verify a block from a hash list. Hash lists are stored in reverse order
verify_indep(#block{ height = 0 }, []) -> true;
verify_indep(B = #block { height = Height }, BI) ->
	ReversedBI = lists:reverse(BI),
	{ExpectedIndepHash, _} = lists:nth(Height + 1, ReversedBI),
	ComputedIndepHash = indep_hash(B),
	BHL = B#block.hash_list,
	ReversedBHL = lists:reverse(BHL),
	case ComputedIndepHash of
		ExpectedIndepHash ->
			true;
		_ ->
			ar:err([
				verify_indep_hash,
				{height, Height},
				{computed_indep_hash, ar_util:encode(ComputedIndepHash)},
				{expected_indep_hash, ar_util:encode(ExpectedIndepHash)},
				{hash_list_length, length(BI)},
				{hash_list_latest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(BI, 10)))},
				{hash_list_eariest_blocks, lists:map(fun ar_util:encode/1, ?BI_TO_BHL(lists:sublist(ReversedBI, 10)))},
				{block_hash_list_latest_blocks, lists:map(fun ar_util:encode/1, lists:sublist(BHL, 10))},
				{block_hash_list_earlies_blocks, lists:map(fun ar_util:encode/1, lists:sublist(ReversedBHL, 10))}
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
indep_hash(#block { height = Height } = B) ->
	case Height >= ar_fork:height_2_0() of
		true ->
			indep_hash_post_fork_2_0(B);
		false ->
			indep_hash_pre_fork_2_0(B)
	end.

indep_hash_pre_fork_2_0(B = #block { height = Height }) when Height >= ?FORK_1_6 ->
	ar_deep_hash:hash([
		B#block.nonce,
		B#block.previous_block,
		integer_to_binary(B#block.timestamp),
		integer_to_binary(B#block.last_retarget),
		integer_to_binary(B#block.diff),
		integer_to_binary(B#block.cumulative_diff),
		integer_to_binary(B#block.height),
		B#block.hash,
		B#block.hash_list_merkle,
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
indep_hash_pre_fork_2_0(#block {
		nonce = Nonce,
		previous_block = PrevHash,
		timestamp = TimeStamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = Hash,
		hash_list = HL,
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
					{hash_list, lists:map(fun ar_util:encode/1, HL)},
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

indep_hash_post_fork_2_0(B) ->
	BDS = ar_block:generate_block_data_segment(B),
	indep_hash_post_fork_2_0(BDS, B#block.hash, B#block.nonce).

indep_hash_post_fork_2_0(BDS, Hash, Nonce) ->
	ar_deep_hash:hash([BDS, Hash, Nonce]).

%% @doc Returns the transaction id
tx_id(Id) when is_binary(Id) -> Id;
tx_id(TX) -> TX#tx.id.

%% @doc Spawn a miner and mine the current block synchronously. Used for testing.
%% Returns the nonce to use to add the block to the list.
mine(B, POA, TXs, RewardAddr, Tags, BI) ->
	ar_mine:start(B, POA, TXs, RewardAddr, Tags, self(), [], BI),
	receive
		{work_complete, _BH, NewB, TXs, _BDS, _POA, _HashesTried} ->
			{NewB#block.nonce, NewB#block.timestamp, NewB#block.diff}
	end.

is_data_on_block_list(_, _) -> false.

read_v1_genesis_txs() ->
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
