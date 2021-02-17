%%% @doc Different utility functions for node and node worker.
-module(ar_node_utils).

-export([
	apply_mining_reward/3,
	apply_tx/3,
	apply_txs/3,
	update_wallets/5,
	validate/6,
	calculate_delay/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate and apply mining reward quantities to a wallet list.
apply_mining_reward(WalletList, unclaimed, _Quantity) ->
	WalletList;
apply_mining_reward(WalletList, RewardAddr, Quantity) ->
	alter_wallet(WalletList, RewardAddr, Quantity).

%% @doc Apply a transaction to a wallet list, updating it.
apply_tx(WalletList, unavailable, _) ->
	WalletList;
apply_tx(WalletList, TX, Height) ->
	do_apply_tx(WalletList, TX, Height).

%% @doc Update a wallet list with a set of new transactions.
apply_txs(WalletList, TXs, Height) ->
	lists:foldl(
		fun(TX, Acc) ->
			apply_tx(Acc, TX, Height)
		end,
		WalletList,
		TXs
	).

%% @doc Update the wallets by applying the new transactions and the mining reward.
%% Return the new reward pool and wallets. It is sufficient to provide the source
%% and the destination wallets of the transactions and the reward wallet.
%% @end
update_wallets(NewB, Wallets, RewardPool, Rate, Height) ->
	TXs = NewB#block.txs,
	{FinderReward, NewRewardPool} =
		get_miner_reward_and_endowment_pool({
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			NewB#block.weave_size,
			NewB#block.height,
			NewB#block.diff,
			NewB#block.timestamp,
			Rate
		}),
	UpdatedWallets =
		apply_mining_reward(
			apply_txs(Wallets, TXs, Height),
			NewB#block.reward_addr,
			FinderReward
		),
	{NewRewardPool, UpdatedWallets}.

%% @doc Validate a new block, given the previous block, the block index, the wallets of
%% the source and destination addresses and the reward wallet from the previous block,
%% and the mapping between block hashes and transaction identifiers of the recent blocks.
%% @end
validate(BI, NewB, B, Wallets, BlockAnchors, RecentTXMap) ->
	?LOG_INFO(
		[
			{event, validating_block},
			{hash, ar_util:encode(NewB#block.indep_hash)}
		]
	),
	case timer:tc(
		fun() ->
			do_validate(BI, NewB, B, Wallets, BlockAnchors, RecentTXMap)
		end
	) of
		{TimeTaken, valid} ->
			?LOG_INFO(
				[
					{event, block_validation_successful},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}
				]
			),
			valid;
		{TimeTaken, {invalid, Reason}} ->
			?LOG_INFO(
				[
					{event, block_validation_failed},
					{reason, Reason},
					{hash, ar_util:encode(NewB#block.indep_hash)},
					{time_taken_us, TimeTaken}
				]
			),
			{invalid, Reason}
	end.

%% @doc Return a delay in milliseconds to wait before including a transaction into a block.
%% The delay is computed as base delay + a function of data size with a conservative
%% estimation of the network speed.
%% @end
calculate_delay(Bytes) ->
	BaseDelay = (?BASE_TX_PROPAGATION_DELAY) * 1000,
	NetworkDelay = Bytes * 8 div (?TX_PROPAGATION_BITS_PER_SECOND) * 1000,
	BaseDelay + NetworkDelay.

%%%===================================================================
%%% Private functions.
%%%===================================================================

alter_wallet(Wallets, Target, Adjustment) ->
	case maps:get(Target, Wallets, not_found) of
		not_found ->
			maps:put(Target, {Adjustment, <<>>}, Wallets);
		{Balance, LastTX} ->
			maps:put(Target, {Balance + Adjustment, LastTX}, Wallets)
	end.

do_apply_tx(
		Wallets,
		TX = #tx {
			last_tx = Last,
			owner = From
		},
		Height) ->
	Addr = ar_wallet:to_address(From),
	Fork_1_8 = ar_fork:height_1_8(),
	case {Height, maps:get(Addr, Wallets, not_found)} of
		{H, {_Balance, _LastTX}} when H >= Fork_1_8 ->
			do_apply_tx(Wallets, TX);
		{_, {_Balance, Last}} ->
			do_apply_tx(Wallets, TX);
		_ ->
			Wallets
	end.

do_apply_tx(WalletList, TX) ->
	update_recipient_balance(
		update_sender_balance(WalletList, TX),
		TX
	).

update_sender_balance(
		Wallets,
		#tx {
			id = ID,
			owner = From,
			quantity = Qty,
			reward = Reward
		}) ->
	Addr = ar_wallet:to_address(From),
	case maps:get(Addr, Wallets, not_found) of
		{Balance, _LastTX} ->
			maps:put(Addr, {Balance - (Qty + Reward), ID}, Wallets);
		_ ->
			Wallets
	end.

update_recipient_balance(Wallets, #tx { quantity = 0 }) ->
	Wallets;
update_recipient_balance(
		Wallets,
		#tx {
			target = To,
			quantity = Qty
		}) ->
	case maps:get(To, Wallets, not_found) of
		not_found ->
			maps:put(To, {Qty, <<>>}, Wallets);
		{OldBalance, LastTX} ->
			maps:put(To, {OldBalance + Qty, LastTX}, Wallets)
	end.

do_validate(BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap) ->
	validate_block(height, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}).

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool(Args) ->
	{
		RewardPool,
		TXs,
		RewardAddr,
		WeaveSize,
		Height,
		Diff,
		Timestamp,
		Rate
	} = Args,
	case Height >= ar_fork:height_2_4() of
		true ->
			ar_pricing:get_miner_reward_and_endowment_pool({
				RewardPool,
				TXs,
				RewardAddr,
				WeaveSize,
				Height,
				Timestamp,
				Rate
			});
		false ->
			ar_pricing:get_miner_reward_and_endowment_pool_pre_fork_2_4({
				RewardPool,
				TXs,
				RewardAddr,
				WeaveSize,
				Height,
				Diff,
				Timestamp
			})
	end.

validate_block(height, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_block:verify_height(NewB, OldB) of
		false ->
			{invalid, invalid_height};
		true ->
			validate_block(weave_size, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(
	weave_size,
	{BI, #block{ txs = TXs } = NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
) ->
	case ar_block:verify_weave_size(NewB, OldB, TXs) of
		false ->
			{invalid, invalid_weave_size};
		true ->
			validate_block(previous_block, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(previous_block, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_block:verify_previous_block(NewB, OldB) of
		false ->
			{invalid, invalid_previous_block};
		true ->
			case NewB#block.height >= ar_fork:height_2_4() of
				true ->
					validate_block(spora, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
				false ->
					validate_block(poa, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
			end
	end;
validate_block(spora, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	BDS = ar_block:generate_block_data_segment(NewB),
	#block{
		nonce = Nonce,
		height = Height,
		timestamp = Timestamp,
		diff = Diff,
		previous_block = PrevH,
		poa = POA,
		hash = Hash
	} = NewB,
	UpperBound = ar_mine:get_search_space_upper_bound(BI),
	case ar_mine:validate_spora(BDS, Nonce, Timestamp, Height, Diff, PrevH, UpperBound, POA, BI)
	of
		false ->
			{invalid, invalid_spora};
		{true, Hash} ->
			validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
		{true, _} ->
			{invalid, invalid_spora_hash};
		true ->
			%% The weave is small so far, no solution hash yet.
			validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(
	poa,
	{BI, NewB = #block{ poa = POA }, OldB, Wallets, BlockAnchors, RecentTXMap}
) ->
	case ar_poa:validate(OldB#block.indep_hash, OldB#block.weave_size, BI, POA) of
		false ->
			{invalid, invalid_poa};
		true ->
			validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(difficulty, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_retarget:validate_difficulty(NewB, OldB) of
		false ->
			{invalid, invalid_difficulty};
		true ->
			case NewB#block.height >= ar_fork:height_2_4() of
				false ->
					validate_block(pow, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap});
				true ->
					validate_block(
						independent_hash,
						{BI,  NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
					)
			end
	end;
validate_block(
	pow,
	{
		BI,
		NewB = #block{ nonce = Nonce, height = Height, diff = Diff, poa = POA },
		OldB,
		Wallets,
		BlockAnchors,
		RecentTXMap
	}
) ->
	POW = ar_weave:hash(
		ar_block:generate_block_data_segment(NewB),
		Nonce,
		Height
	),
	case ar_mine:validate(POW, ar_poa:modify_diff(Diff, POA#poa.option, Height), Height) of
		false ->
			{invalid, invalid_pow};
		true ->
			case ar_block:verify_dep_hash(NewB, POW) of
				false ->
					{invalid, invalid_pow_hash};
				true ->
					validate_block(
						independent_hash,
						{BI,  NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
					)
			end
	end;
validate_block(independent_hash, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_weave:indep_hash(NewB) == NewB#block.indep_hash of
		false ->
			{invalid, invalid_independent_hash};
		true ->
			#block{ height = Height } = NewB,
			case Height >= ar_fork:height_2_5() of
				true ->
					validate_block(
						usd_to_ar_rate,
						{BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
					);
				false ->
					validate_block(
						wallet_list,
						{BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
					)
			end
	end;
validate_block(usd_to_ar_rate, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	{USDToARRate, ScheduledUSDToARRate} = ar_pricing:recalculate_usd_to_ar_rate(OldB),
	case NewB#block.usd_to_ar_rate == USDToARRate
			andalso NewB#block.scheduled_usd_to_ar_rate == ScheduledUSDToARRate of
		false ->
			{invalid, invalid_usd_to_ar_rate};
		true ->
			validate_block(wallet_list, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(
	wallet_list,
	{BI, #block{ txs = TXs } = NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
) ->
	RewardPool = OldB#block.reward_pool,
	Height = OldB#block.height,
	Rate =
		case Height >= ar_fork:height_2_5() of
			true ->
				OldB#block.usd_to_ar_rate;
			false ->
				?USD_TO_AR_INITIAL_RATE
		end,
	{_, UpdatedWallets} = update_wallets(NewB, Wallets, RewardPool, Rate, Height),
	case lists:any(fun(TX) -> is_wallet_invalid(TX, UpdatedWallets) end, TXs) of
		true ->
			{invalid, invalid_wallet_list};
		false ->
			validate_block(
				block_field_sizes,
				{BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}
			)
	end;
validate_block(block_field_sizes, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap}) ->
	case ar_block:block_field_size_limit(NewB) of
		false ->
			{invalid, invalid_field_size};
		true ->
			validate_block(txs, {BI, NewB, OldB, Wallets, BlockAnchors, RecentTXMap})
	end;
validate_block(
	txs,
	{
		BI,
		NewB = #block{ timestamp = Timestamp, height = Height, txs = TXs },
		OldB,
		Wallets,
		BlockAnchors,
		RecentTXMap
	}
) ->
	Rate =
		case Height > ar_fork:height_2_5() of
			true ->
				OldB#block.usd_to_ar_rate;
			false ->
				?USD_TO_AR_INITIAL_RATE
		end,
	Args = {TXs, Rate, Height - 1, Timestamp, Wallets, BlockAnchors, RecentTXMap},
	case ar_tx_replay_pool:verify_block_txs(Args) of
		invalid ->
			{invalid, invalid_txs};
		valid ->
			validate_block(tx_root, {BI, NewB, OldB})
	end;
validate_block(tx_root, {BI, NewB, OldB}) ->
	case ar_block:verify_tx_root(NewB) of
		false ->
			{invalid, invalid_tx_root};
		true ->
			validate_block(block_index_root, {BI, NewB, OldB})
	end;
validate_block(block_index_root, {BI, NewB, OldB}) ->
	case ar_block:verify_block_hash_list_merkle(NewB, OldB, BI) of
		false ->
			{invalid, invalid_block_index_root};
		true ->
			validate_block(last_retarget, {NewB, OldB})
	end;
validate_block(last_retarget, {NewB, OldB}) ->
	case ar_block:verify_last_retarget(NewB, OldB) of
		false ->
			{invalid, invalid_last_retarget};
		true ->
			validate_block(cumulative_diff, {NewB, OldB})
	end;
validate_block(cumulative_diff, {NewB, OldB}) ->
	case ar_block:verify_cumulative_diff(NewB, OldB) of
		false ->
			{invalid, invalid_cumulative_difficulty};
		true ->
			valid
	end.

-ifdef(DEBUG).
is_wallet_invalid(#tx{ signature = <<>> }, _Wallets) ->
	false;
is_wallet_invalid(#tx{ owner = Owner }, Wallets) ->
	Address = ar_wallet:to_address(Owner),
	case maps:get(Address, Wallets, not_found) of
		{Balance, LastTX} when Balance >= 0 ->
			case Balance of
				0 ->
					byte_size(LastTX) == 0;
				_ ->
					false
			end;
		_ ->
			true
	end.
-else.
is_wallet_invalid(#tx{ owner = Owner }, Wallets) ->
	Address = ar_wallet:to_address(Owner),
	case maps:get(Address, Wallets, not_found) of
		{Balance, LastTX} when Balance >= 0 ->
			case Balance of
				0 ->
					byte_size(LastTX) == 0;
				_ ->
					false
			end;
		_ ->
			true
	end.
-endif.

%%%===================================================================
%%% Tests.
%%%===================================================================

block_validation_test_() ->
	{timeout, 20, fun test_block_validation/0}.

test_block_validation() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200), <<>>}]),
	ar_test_node:start(B0),
	BI0 = ar_node:get_block_index(),
	BlockAnchors = ar_node:get_block_anchors(),
	RecentTXMap = ar_node:get_recent_txs_map(),
	TX =
		ar_test_node:sign_tx(
			Wallet,
			#{
				reward => ?AR(1),
				data => crypto:strong_rand_bytes(10 * 1024 * 1024)
			}
		),
	ar_test_node:assert_post_tx_to_master(TX),
	ar_node:mine(),
	[{H, _, _} | _] = ar_test_node:wait_until_height(1),
	BShadow = ar_test_node:read_block_when_stored(H),
	B = BShadow#block{ txs = ar_storage:read_tx(BShadow#block.txs) },
	Wallets = #{ ar_wallet:to_address(Pub) => {?AR(200), <<>>} },
	?assertEqual(valid, validate(BI0, B, B0, Wallets, BlockAnchors, RecentTXMap)),
	?assertEqual(
		{invalid, invalid_height},
		validate(BI0, B#block{ height = 2 }, B0, Wallets, BlockAnchors, RecentTXMap)
	),
	?assertEqual(
		{invalid, invalid_weave_size},
		validate(
			BI0,
			B#block{ weave_size = B0#block.weave_size + 1 },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	?assertEqual(
		{invalid, invalid_previous_block},
		validate(
			BI0,
			B#block{ previous_block = B#block.indep_hash },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	?assertEqual(
		{invalid, invalid_difficulty},
		validate(
			BI0,
			B#block{ diff = B0#block.diff - 1 },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	?assertEqual(
		{invalid, invalid_independent_hash},
		validate(
			BI0,
			B#block{ tags = [{<<"N">>, <<"V">>}] },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	?assertEqual(
		{invalid, invalid_wallet_list},
		validate(
			BI0,
			B#block{ txs = [TX#tx{ reward = ?AR(201) }] },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	InvDataRootB = B#block{ tx_root = crypto:strong_rand_bytes(32) },
	?assertEqual(
		{invalid, invalid_tx_root},
		validate(
			BI0,
			InvDataRootB#block{ indep_hash = ar_weave:indep_hash(InvDataRootB) },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	InvLastRetargetB = B#block{ last_retarget = B#block.timestamp },
	?assertEqual(
		{invalid, invalid_difficulty},
		validate(
			BI0,
			InvLastRetargetB#block{ indep_hash = ar_weave:indep_hash(InvLastRetargetB) },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	InvBlockIndexRootB = B#block{ hash_list_merkle = crypto:strong_rand_bytes(32) },
	?assertEqual(
		{invalid, invalid_block_index_root},
		validate(
			BI0,
			InvBlockIndexRootB#block{ indep_hash = ar_weave:indep_hash(InvBlockIndexRootB) },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	InvCDiffB = B#block{ cumulative_diff = B0#block.cumulative_diff * 1000 },
	?assertEqual(
		{invalid, invalid_cumulative_difficulty},
		validate(
			BI0,
			InvCDiffB#block{ indep_hash = ar_weave:indep_hash(InvCDiffB) },
			B0,
			Wallets,
			BlockAnchors,
			RecentTXMap
		)
	),
	BI1 = ar_node:get_block_index(),
	BlockAnchors2 = ar_node:get_block_anchors(),
	RecentTXMap2 = ar_node:get_recent_txs_map(),
	ar_node:mine(),
	[{H2, _, _} | _ ] = ar_test_node:wait_until_height(2),
	BShadow2 = ar_test_node:read_block_when_stored(H2),
	B2 = BShadow2#block{ txs = ar_storage:read_tx(BShadow2#block.txs) },
	?assertEqual(valid, validate(BI1, B2, B, Wallets, BlockAnchors2, RecentTXMap2)),
	?assertEqual(
		{invalid, invalid_spora},
		validate(BI1, B2#block{ poa = #poa{} }, B, Wallets, BlockAnchors2, RecentTXMap2)
	),
	?assertEqual(
		{invalid, invalid_spora_hash},
		validate(BI1, B2#block{ hash = <<>> }, B, Wallets, BlockAnchors2, RecentTXMap2)
	),
	?assertEqual(
		{invalid, invalid_spora_hash},
		validate(BI1, B2#block{ hash = B#block.hash }, B, Wallets, BlockAnchors2, RecentTXMap2)
	),
	PoA = B2#block.poa,
	FakePoA =
		case get_chunk(1) of
			P when P#poa.chunk == PoA#poa.chunk ->
				get_chunk(1 + 256 * 1024);
			P ->
				P
		end,
	?assertEqual(
		{invalid, invalid_spora},
		validate(BI1, B2#block{ poa = FakePoA }, B, Wallets, BlockAnchors2, RecentTXMap2)
	).

get_chunk(Byte) ->
	{ok, {{<<"200">>, _}, _, JSON, _, _}} = ar_test_node:get_chunk(Byte),
	#{
		chunk := Chunk,
		data_path := DataPath,
		tx_path := TXPath
	} = ar_serialize:json_map_to_chunk_proof(jiffy:decode(JSON, [return_maps])),
	#poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath, option = 1 }.
