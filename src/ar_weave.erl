-module(ar_weave).
-export([init/0, init/1, init/2, init/3, add/1, add/2, add/3, add/4, add/6, add/7, add/11]).
-export([hash/3, indep_hash/1]).
-export([verify_indep/2]).
-export([calculate_recall_block/2, calculate_recall_block/3]).
-export([generate_hash_list/1]).
-export([is_data_on_block_list/2, is_tx_on_block_list/2]).
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
			hash_list = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool
		},
	B1 = B0#block { last_retarget = B0#block.timestamp },
	[B1#block { indep_hash = indep_hash(B1) }].
-else.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ?DEFAULT_DIFF).
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
			hash_list = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool
		},
	B1 = B0#block { last_retarget = B0#block.timestamp },
	[B1#block { indep_hash = indep_hash(B1) }].
-endif.
%% @doc Add a new block to the weave, with assiocated TXs and archive data.
add(Bs) ->
	add(Bs, []).
add(Bs, TXs) ->
	add(Bs, TXs, generate_hash_list(Bs)).
add(Bs, TXs, HashList) ->
	add(Bs, TXs, HashList, <<>>).
add(Bs, TXs, HashList, unclaimed) ->
	add(Bs, TXs, HashList, <<>>);
add([B|Bs], TXs, HashList, RewardAddr) ->
	RecallHash = ar_util:get_recall_hash(hd([B|Bs]), HashList),
	RecallB = ar_storage:read_block(RecallHash, HashList),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			B#block.reward_pool,
			TXs,
			RewardAddr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				B#block.weave_size,
				B#block.height
			)
		),
	WalletList = ar_node_utils:apply_mining_reward(
		ar_node_utils:apply_txs(B#block.wallet_list, TXs, length(HashList) - 1),
		RewardAddr,
		FinderReward,
		length(HashList)
	),
	add([B|Bs], TXs, HashList, RewardAddr, RewardPool, WalletList).
add(Bs, TXs, HashList, RewardAddr, RewardPool, WalletList) ->
	add(Bs, TXs, HashList, RewardAddr, RewardPool, WalletList, []).
add([Hash|Bs], TXs, HashList, RewardAddr, RewardPool, WalletList, Tags) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, HashList)|Bs],
		TXs,
		HashList,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags
	);
add(Bs, TXs, HashList, RewardAddr, RewardPool, WalletList, Tags) ->
	RecallHash = ar_util:get_recall_hash(hd(Bs), HashList),
	RecallB = ar_storage:read_block(RecallHash, HashList),
	{Nonce, Timestamp, Diff} = mine(hd(Bs), RecallB, TXs, RewardAddr, Tags),
	add(
		Bs,
		TXs,
		HashList,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		RecallB,
		Diff,
		Nonce,
		Timestamp
	).
add([Hash|Bs], RawTXs, HashList, RewardAddr, RewardPool, WalletList, Tags, RecallB, Diff, Nonce, Timestamp) when is_binary(Hash) ->
	add(
		[ar_storage:read_block(Hash, HashList)|Bs],
		RawTXs,
		HashList,
		RewardAddr,
		RewardPool,
		WalletList,
		Tags,
		RecallB,
		Diff,
		Nonce,
		Timestamp
	);
add([CurrentB|_Bs], RawTXs, HashList, RewardAddr, RewardPool, WalletList, Tags, RecallB, Diff, Nonce, Timestamp) ->
	NewHeight = CurrentB#block.height + 1,
	RecallB = ar_node_utils:find_recall_block(HashList),
	TXs = [T#tx.id || T <- RawTXs],
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
			true -> CurrentB#block.cumulative_diff + (Diff * Diff);
			false -> 0
		end,
	MR =
		case NewHeight of
			_ when NewHeight < ?FORK_1_6 ->
				<<>>;
			_ when NewHeight == ?FORK_1_6 ->
				ar_merkle:block_hash_list_to_merkle_root(CurrentB#block.hash_list);
			_ ->
				ar_merkle:root(CurrentB#block.hash_list_merkle, CurrentB#block.indep_hash)
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
			hash = hash(
				ar_block:generate_block_data_segment(
					CurrentB,
					RecallB,
					RawTXs,
					RewardAddr,
					Timestamp,
					Tags
				),
				Nonce,
				NewHeight
			),
			txs = TXs,
			hash_list = HashList,
			hash_list_merkle = MR,
			wallet_list = WalletList,
			reward_addr = RewardAddr,
			tags = Tags,
			reward_pool = RewardPool,
			weave_size = CurrentB#block.weave_size + BlockSize,
			block_size = BlockSize
		},
	[NewB#block { indep_hash = indep_hash(NewB) }|HashList].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_hash_list(undefined) -> [];
generate_hash_list([]) -> [];
generate_hash_list(Bs = [B|_]) ->
	generate_hash_list(Bs, B#block.height + 1).
generate_hash_list([B = #block { hash_list = BHL }|_], _) when is_list(BHL) ->
	[B#block.indep_hash|BHL];
generate_hash_list([], 0) -> [];
generate_hash_list([B|Bs], N) when is_record(B, block) ->
	[B#block.indep_hash|generate_hash_list(Bs, N - 1)];
generate_hash_list([Hash|Bs], N) when is_binary(Hash) ->
	[Hash|generate_hash_list(Bs, N - 1)].

%% @doc Verify a block from a hash list. Hash lists are stored in reverse order
verify_indep(#block{ height = 0 }, []) -> true;
verify_indep(B = #block { height = Height }, HashList) ->
	ComputedIndepHash = indep_hash(B),
	ReversedHashList = lists:reverse(HashList),
	BHL = B#block.hash_list,
	ReversedBHL = lists:reverse(BHL),
	ExpectedIndepHash = lists:nth(Height + 1, ReversedHashList),
	case ComputedIndepHash of
		ExpectedIndepHash ->
			true;
		_ ->
			ar:err([
				verify_indep_hash,
				{height, Height},
				{computed_indep_hash, ar_util:encode(ComputedIndepHash)},
				{expected_indep_hash, ar_util:encode(ExpectedIndepHash)},
				{hash_list_length, length(HashList)},
				{hash_list_latest_blocks, lists:map(fun ar_util:encode/1, lists:sublist(HashList, 10))},
				{hash_list_eariest_blocks, lists:map(fun ar_util:encode/1, lists:sublist(ReversedHashList, 10))},
				{block_hash_list_latest_blocks, lists:map(fun ar_util:encode/1, lists:sublist(BHL, 10))},
				{block_hash_list_earlies_blocks, lists:map(fun ar_util:encode/1, lists:sublist(ReversedBHL, 10))}
			]),
			false
	end.

%% @doc Generate a recall block number from a block or a hash and block height.
calculate_recall_block(Hash, HashList) when is_binary(Hash) ->
	calculate_recall_block(ar_storage:read_block(Hash, HashList), HashList);
calculate_recall_block(B, HashList) when is_record(B, block) ->
	case B#block.height of
		0 -> 0;
		_ -> calculate_recall_block(B#block.indep_hash, B#block.height, HashList)
	end.
calculate_recall_block(IndepHash, Height, _HashList) ->
	binary:decode_unsigned(IndepHash) rem Height.

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
indep_hash(#block {
		nonce = Nonce,
		previous_block = PrevHash,
		timestamp = TimeStamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = Hash,
		hash_list = HashList,
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
					{hash_list, lists:map(fun ar_util:encode/1, HashList)},
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

%% @doc Returns the transaction id
tx_id(Id) when is_binary(Id) -> Id;
tx_id(TX) -> TX#tx.id.

%% @doc Spawn a miner and mine the current block synchronously. Used for testing.
%% Returns the nonce to use to add the block to the list.
mine(B, RecallB, TXs, RewardAddr, Tags) ->
	ar_mine:start(B, RecallB, TXs, RewardAddr, Tags, self(), []),
	receive
		{work_complete, TXs, _Hash, Diff, Nonce, Timestamp} ->
			{Nonce, Timestamp, Diff}
	end.

%% @doc Return whether or not a transaction is found on a block list.
is_tx_on_block_list([], _) -> false;
is_tx_on_block_list([BHL = Hash|Bs], TXID) when is_binary(Hash) ->
	is_tx_on_block_list([ar_storage:read_block(Hash, BHL)|Bs], TXID);
is_tx_on_block_list([#block { txs = TXs }|Bs], TXID) ->
	case lists:member(TXID, TXs) of
		true -> true;
		false -> is_tx_on_block_list(Bs, TXID)
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
			SignedTX = ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub),
			ar_storage:write_tx(SignedTX),
			SignedTX
		end,
		?GENESIS_BLOCK_MESSAGES
	),
	file:write_file("genesis_wallets.csv", lists:map(fun(T) -> binary_to_list(ar_util:encode(T#tx.id)) ++ "," end, TXs)),
	[T#tx.id || T <- TXs].
