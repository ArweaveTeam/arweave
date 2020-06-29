-module(ar_weave).

-export([
	init/0,
	init/1,
	init/2,
	init/3,
	hash/3,
	indep_hash/1,
	indep_hash_post_fork_2_0/1,
	indep_hash_post_fork_2_0/3,
	create_genesis_txs/0,
	read_v1_genesis_txs/0,
	generate_block_index/1,
	tx_id/1
]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Start a new weave. Optionally takes a list of wallets
%% for the genesis block. The function was used to start the original weave.
%% Also, it is used in tests. Currently it's not possible to start a new weave
%% from the command line. The feature was dropped since it requires extra effort
%% to reset the fork heights and update the inflation rewards issuance, to make the
%% new weaves created this way function without the issues solved by the hard forks
%% in the original weave. The genesis transactions of the original weave
%% are defined in read_v1_genesis_txs/0.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ar_mine:genesis_difficulty()).
init(WalletList, Diff) -> init(WalletList, Diff, 0).
init(WalletList, StartingDiff, RewardPool) ->
	ar_randomx_state:reset(),
	WL = ar_patricia_tree:from_proplist([{A, {B, LTX}} || {A, B, LTX} <- WalletList]),
	WLH = element(1, ar_block:hash_wallet_list(0, unclaimed, WL)),
	ok = ar_storage:write_wallet_list(WLH, WL),
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = [],
			wallet_list = WLH,
			hash_list = [],
			diff = StartingDiff,
			weave_size = 0,
			block_size = 0,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			poa = #poa{}
		},
	B1 = B0#block { last_retarget = B0#block.timestamp },
	[B1#block { indep_hash = indep_hash(B1) }].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_block_index(undefined) -> [];
generate_block_index([]) -> [];
generate_block_index(Blocks) ->
	lists:map(
		fun
			(B) when ?IS_BLOCK(B) ->
				ar_util:block_index_entry_from_block(B);
			({H, WS, TXRoot}) ->
				{H, WS, TXRoot}
		end,
		Blocks
	).

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
						ar_patricia_tree:foldr(
							fun({Wallet, Qty, Last}, WL) ->
								[{
									[
										{wallet, ar_util:encode(Wallet)},
										{quantity, Qty},
										{last_tx, ar_util:encode(Last)}
									]
								} | WL]
							end,
							[],
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
			SignedTX = ar_tx:sign_v1(TX#tx{reward = Reward}, Priv, Pub),
			ar_storage:write_tx(SignedTX),
			SignedTX
		end,
		?GENESIS_BLOCK_MESSAGES
	),
	ar_storage:write_file_atomic(
		"genesis_wallets.csv",
		lists:map(fun(T) -> binary_to_list(ar_util:encode(T#tx.id)) ++ "," end, TXs)
	),
	[T#tx.id || T <- TXs].
