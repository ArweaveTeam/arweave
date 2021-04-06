-module(ar_weave).

-export([
	init/0, init/1, init/2, init/3, init/4,
	hash/3,
	indep_hash/1, indep_hash/3, indep_hash/4,
	create_genesis_txs/0,
	read_v1_genesis_txs/0,
	generate_block_index/1,
	tx_id/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

%% @doc Create a genesis block.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, 1).
init(WalletList, Diff) -> init(WalletList, Diff, 0).
init(WalletList, StartingDiff, RewardPool) ->
	init(WalletList, StartingDiff, RewardPool, []).

init(WalletList, StartingDiff, RewardPool, TXs) ->
	WL = ar_patricia_tree:from_proplist([{A, {B, LTX}} || {A, B, LTX} <- WalletList]),
	WLH = element(1, ar_block:hash_wallet_list(0, unclaimed, WL)),
	ok = ar_storage:write_wallet_list(WLH, WL),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	BlockSize =
		case SizeTaggedTXs of
			[] ->
				0;
			_ ->
				element(2, lists:last(SizeTaggedTXs))
		end,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = <<>>,
			previous_block = <<>>,
			hash_list_merkle = <<>>,
			txs = TXs,
			tx_root = TXRoot,
			wallet_list = WLH,
			hash_list = [],
			tags = [],
			diff = StartingDiff,
			cumulative_diff = ar_difficulty:next_cumulative_diff(0, StartingDiff, 0),
			weave_size = BlockSize,
			block_size = BlockSize,
			reward_pool = RewardPool,
			timestamp = os:system_time(seconds),
			poa = #poa{},
			size_tagged_txs = SizeTaggedTXs
		},
	B1 =
		case ar_fork:height_2_5() > 0 of
			true ->
				B0;
			false ->
				B0#block{
					usd_to_ar_rate = ?NEW_WEAVE_USD_TO_AR_RATE,
					scheduled_usd_to_ar_rate = ?NEW_WEAVE_USD_TO_AR_RATE
				}
		end,
	B2 = B1#block { last_retarget = B1#block.timestamp },
	B3 = B2#block { indep_hash = indep_hash(B2) },
	[B3].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
%% @end
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
%% @end
hash(BDS, Nonce, Height) ->
	HashData = << Nonce/binary, BDS/binary >>,
	true = Height >= ar_fork:height_1_7(),
	ar_randomx_state:hash(Height, HashData).

%% @doc Compute the block identifier (also referred to as "independent hash").
indep_hash(B) ->
	BDS = ar_block:generate_block_data_segment(B),
	case B#block.height >= ar_fork:height_2_4() of
		true ->
			indep_hash(BDS, B#block.hash, B#block.nonce, B#block.poa);
		false ->
			indep_hash(BDS, B#block.hash, B#block.nonce)
	end.

indep_hash(BDS, Hash, Nonce, POA) ->
	ar_deep_hash:hash([BDS, Hash, Nonce, ar_block:poa_to_list(POA)]).

indep_hash(BDS, Hash, Nonce) ->
	ar_deep_hash:hash([BDS, Hash, Nonce]).

%% @doc Returns the transaction id
tx_id(Id) when is_binary(Id) -> Id;
tx_id(TX) -> TX#tx.id.

read_v1_genesis_txs() ->
	{ok, Files} = file:list_dir("data/genesis_txs"),
	lists:foldl(
		fun(F, Acc) ->
			file:copy(
				"data/genesis_txs/" ++ F,
				ar_meta_db:get(data_dir) ++ "/" ++ ?TX_DIR ++ "/" ++ F
			),
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
