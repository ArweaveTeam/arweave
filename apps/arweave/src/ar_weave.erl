-module(ar_weave).

-export([init/0, init/1, init/2, init/3, create_mainnet_genesis_txs/0, generate_data/3,
		add_mainnet_v1_genesis_txs/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Create a genesis block. The genesis block includes one transaction with
%% at least one small chunk and the total data size equal to ar_block:strict_data_split_threshold(),
%% to test the code branches dealing with small chunks placed before the threshold.
init() ->
	init([]).

%% @doc Create a genesis block with the given accounts. One system account is added to the
%% list - we use it to sign a transaction included in the genesis block.
init(WalletList) ->
	init(WalletList, 1).

init(WalletList, Diff) ->
	Size = 3 * ?DATA_CHUNK_SIZE, % Matches ?STRICT_DATA_SPLIT_THRESHOLD in tests.
	init(WalletList, Diff, Size).

init(_WalletList, _Diff, GenesisDataSize) when GenesisDataSize > (4 * ?GiB) ->
	erlang:error({size_exceeds_limit, "GenesisDataSize exceeds 4 GiB"});

%% @doc Create a genesis block with the given accounts and difficulty.
init(WalletList, Diff, GenesisDataSize) ->
	{{_, _, _}, {_, _}} = Key = ar_wallet:new_keyfile(),
	TX = create_genesis_tx(Key, GenesisDataSize),
	WalletList2 = WalletList ++ [{ar_wallet:to_address(Key), 0, TX#tx.id}],
	TXs = [TX],
	AccountTree = ar_patricia_tree:from_proplist([{A, {B, LTX}}
			|| {A, B, LTX} <- WalletList2]),
	WLH = element(1, ar_block:hash_wallet_list(AccountTree)),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, 0),
	BlockSize = case SizeTaggedTXs of [] -> 0; _ -> element(2, lists:last(SizeTaggedTXs)) end,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	Timestamp = os:system_time(second),
	B0 =
		#block{
			nonce = <<>>,
			txs = TXs,
			tx_root = TXRoot,
			wallet_list = WLH,
			diff = Diff,
			cumulative_diff = ar_difficulty:next_cumulative_diff(0, Diff, 0),
			weave_size = BlockSize,
			block_size = BlockSize,
			reward_pool = 0,
			timestamp = Timestamp,
			last_retarget = Timestamp,
			size_tagged_txs = SizeTaggedTXs,
			usd_to_ar_rate = ?NEW_WEAVE_USD_TO_AR_RATE,
			scheduled_usd_to_ar_rate = ?NEW_WEAVE_USD_TO_AR_RATE,
			packing_2_5_threshold = 0,
			strict_data_split_threshold = BlockSize,
			account_tree = AccountTree
		},
	B1 =
		case ar_fork:height_2_6() > 0 of
			false ->
				RewardKey = element(2, ar_wallet:new()),
				RewardAddr = ar_wallet:to_address(RewardKey),
				HashRate = ar_difficulty:get_hash_rate_fixed_ratio(B0),
				RewardHistory = [{RewardAddr, HashRate, 10, 1}],
				PricePerGiBMinute = ar_pricing:get_price_per_gib_minute(0, 
						B0#block{ reward_history = RewardHistory, denomination = 1 }),
				B0#block{ hash = crypto:strong_rand_bytes(32),
						nonce = 0, recall_byte = 0, partition_number = 0,
						reward_key = RewardKey, reward_addr = RewardAddr,
						reward = 10,
						recall_byte2 = 0, nonce_limiter_info = #nonce_limiter_info{
								output = crypto:strong_rand_bytes(32),
								seed = crypto:strong_rand_bytes(48),
								partition_upper_bound = BlockSize,
								next_seed = crypto:strong_rand_bytes(48),
								next_partition_upper_bound = BlockSize },
							price_per_gib_minute = PricePerGiBMinute,
							scheduled_price_per_gib_minute = PricePerGiBMinute,
							reward_history = RewardHistory,
							reward_history_hash = ar_rewards:reward_history_hash(0, <<>>,
									RewardHistory)
						};
			true ->
				B0
		end,
	B2 =
		case ar_fork:height_2_7() > 0 of
			false ->
				InitialHistory = get_initial_block_time_history(),
				B1#block{
					merkle_rebase_support_threshold = ar_block:strict_data_split_threshold() * 2,
					chunk_hash = crypto:strong_rand_bytes(32),
					block_time_history = InitialHistory,
					block_time_history_hash = ar_block_time_history:hash(InitialHistory)
				};
			true ->
				B1
		end,
	[B2#block{ indep_hash = ar_block:indep_hash(B2) }].

-ifdef(AR_TEST).
get_initial_block_time_history() ->
	[{1, 1, 1}].
-else.
get_initial_block_time_history() ->
	[{120, 1, 1}].
-endif.

%% @doc: create a genesis transaction with the given key and data size. This is only used
%% in tests and when launching a localnet node.
create_genesis_tx(Key, Size) ->
	{_, {_, Pk}} = Key,
	UnsignedTX =
		(ar_tx:new())#tx{
			owner = Pk,
			reward = 0,
			data = generate_genesis_data(Size),
			data_size = Size,
			target = <<>>,
			quantity = 0,
			tags = [],
			last_tx = <<>>,
			format = 1
		},
	ar_tx:sign_v1(UnsignedTX, Key).

%% @doc: generate binary data to be used as genesis data in tests. That data is incrementing
%% integer data in 4 byte chunks. e.g.
%% <<0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, ...>>
%% This makes it easier to assert correct chunk data in tests.
-spec generate_genesis_data(integer()) -> binary().
generate_genesis_data(DataSize) ->
    FullChunks = DataSize div 4,
    LeftoverBytes = DataSize rem 4,
    IncrementingData = generate_data(0, FullChunks * 4, <<>>),
    add_padding(IncrementingData, LeftoverBytes).

generate_data(CurrentValue, RemainingBytes, Acc) when RemainingBytes >= 4 ->
	Chunk = <<CurrentValue:32/integer>>,
	generate_data(CurrentValue + 1, RemainingBytes - 4, <<Acc/binary, Chunk/binary>>);
generate_data(_, RemainingBytes, Acc) ->
	add_padding(Acc, RemainingBytes).

add_padding(Data, 0) ->
    Data;
add_padding(Data, LeftoverBytes) ->
    Padding = <<16#FF:8, 16#FF:8, 16#FF:8, 16#FF:8>>,
    <<Data/binary, Padding:LeftoverBytes/unit:8>>.

add_mainnet_v1_genesis_txs() ->
	case filelib:is_dir("genesis_data/genesis_txs") of
		true ->
			{ok, Files} = file:list_dir("genesis_data/genesis_txs"),
			{ok, Config} = application:get_env(arweave, config),
			lists:foldl(
				fun(F, Acc) ->
					SourcePath = "genesis_data/genesis_txs/" ++ F,
					TargetPath = Config#config.data_dir ++ "/" ++ ?TX_DIR ++ "/" ++ F,
					file:copy(SourcePath, TargetPath),
					[ar_util:decode(hd(string:split(F, ".")))|Acc]
				end,
				[],
				Files
			);
		false ->
			?LOG_WARNING("genesis_data/genesis_txs directory not found. Node might not index the genesis "
						 "block transactions."),
			[]
	end.


%% @doc Return the mainnet genesis transactions.
create_mainnet_genesis_txs() ->
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
