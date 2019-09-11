%%%
%%% @doc Server to queue ar_node state-changing tasks.
%%%

-module(ar_node_worker).

-export([start/2, stop/1, cast/2, call/2, call/3]).

-include("ar.hrl").

%%%
%%% Public API.
%%%

%% @doc Start a node worker.
start(NPid, SPid) ->
	Pid = spawn(fun() -> server(NPid, SPid) end),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Send an asynchronous task to a node worker. The answer
%% will be sent to the caller.
cast(Pid, Task) ->
	Pid ! {task, Task},
	ok.

%% @doc Send a synchronous task to a node worker. The timeout
%% can be passed, default is 5000 ms.
call(Pid, Task) ->
	call(Pid, Task, 5000).

call(Pid, Task, Timeout) ->
	cast(Pid, Task),
	% TODO mue: Fix, reply is sent to node, need extra way for
	% synchronous calls.
	receive
		{worker, Reply} ->
			Reply
	after
		Timeout ->
			{error, timeout}
	end.

%%%
%%% Server functions.
%%%

%% @doc Main server loop. For every task received, a message back to the ar_node
%% server must be sent, otherwise ar_node server might get stuck.
server(NPid, SPid) ->
	receive
		{task, Task} ->
			try handle(SPid, Task) of
				Reply ->
					NPid ! {worker, Reply},
					server(NPid, SPid)
			catch
				throw:Term ->
					ar:err( [ {'NodeWorkerEXCEPTION', Term } ]),
					NPid ! {worker, {error, Term}},
					server(NPid, SPid);
				exit:Term ->
					ar:err( [ {'NodeWorkerEXIT', Term} ] ),
					NPid ! {worker, {error, Term}},
					server(NPid, SPid);
				error:Term ->
					ar:err( [ {'NodeWorkerERROR', {Term, erlang:get_stacktrace()} } ]),
					NPid ! {worker, {error, Term}},
					server(NPid, SPid)
			end;
		{'DOWN', _, _, _, normal} ->
			%% There is a hidden monitor started in ar_node_utils:fork_recover/3
			server(NPid, SPid);
		stop ->
			ok;
		{ar_node_state, _, _} ->
			%% When an ar_node_state call times out its message may leak here. It can be huge so we avoid logging it.
			server(NPid, SPid);
		Other ->
			ar:warn({ar_node_worker_unknown_msg, Other}),
			server(NPid, SPid)
	end.

%% @doc Handle the server tasks. Return values a sent to the caller. Simple tasks like
%% setter can be done directy, more complex ones are handled as private API functions.
handle(SPid, {gossip_message, Msg}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	handle_gossip(SPid, ar_gossip:recv(GS, Msg));
handle(SPid, {add_tx, TX}) ->
	{ok, StateIn} =
		ar_node_state:lookup(SPid, [gossip, node, txs, waiting_txs, height]),
	case add_tx(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {add_tx_to_mining_pool, TX}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [gossip, txs, waiting_txs, height]),
	case add_tx_to_mining_pool(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx_to_mining_pool};
handle(SPid, {cancel_tx, TXID, Sig}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs]),
	{ok, StateOut} = cancel_tx(StateIn, TXID, Sig),
	ar_node_state:update(SPid, StateOut),
	{ok, cancel_tx};
handle(SPid, {process_new_block, Peer, Height, BShadow, BDS, Recall}) ->
	%% We have a new block. Distribute it to the gossip network. This is only
	%% triggered in the polling mode.
	{ok, StateIn} = ar_node_state:all(SPid),
	GS = maps:get(gossip, StateIn),
	ar_gossip:send(GS, {new_block, Peer, Height, BShadow, BDS, Recall}),
	{ok, process_new_block};
handle(SPid, {work_complete, BH, MinedTXs, Diff, Nonce, Timestamp}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	#{ hash_list := [CurrentBH | _] } = StateIn,
	case BH of
		CurrentBH ->
			case integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) of
				{ok, StateOut} ->
					ar_node_state:update(SPid, StateOut);
				none ->
					ok
			end,
			{ok, work_complete};
		_ ->
			ar:info([ar_node_worker, ignore_mined_block, {reason, accepted_foreign_block}]),
			{ok, ignore}
	end;
handle(SPid, {fork_recovered, BHL, BlockTXPairs}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case recovered_from_fork(StateIn, BHL, BlockTXPairs) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, fork_recovered};
handle(SPid, mine) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	StateOut = ar_node_utils:start_mining(StateIn),
	ar_node_state:update(SPid, StateOut),
	{ok, mine};
handle(SPid, {mine_at_diff, Diff}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	StateOut = ar_node_utils:start_mining(StateIn, Diff),
	ar_node_state:update(SPid, StateOut),
	{ok, mine};
handle(SPid, automine) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	StateOut = ar_node_utils:start_mining(StateIn#{ automine => true }),
	ar_node_state:update(SPid, StateOut),
	{ok, automine};
handle(SPid, {replace_block_list, [Block | _]}) ->
	ar_node_state:update(SPid, [
		{hash_list, [Block#block.indep_hash | Block#block.hash_list]},
		{wallet_list, Block#block.wallet_list},
		{height, Block#block.height}
	]),
	{ok, replace_block_list};
handle(SPid, {set_reward_addr, Addr}) ->
	ar_node_state:update(SPid, [
		{reward_addr, Addr}
	]),
	{ok, set_reward_addr};
handle(SPid, {add_peers, Peers}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	NewGS = ar_gossip:add_peers(GS, Peers),
	ar_node_state:update(SPid, [
		{gossip, NewGS}
	]),
	{ok, add_peers};
handle(SPid, {set_loss_probability, Prob}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	ar_node_state:update(SPid, [
		{gossip, ar_gossip:set_loss_probability(GS, Prob)}
	]),
	{ok, set_loss_probability};
handle(SPid, {set_delay, MaxDelay}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	ar_node_state:update(SPid, [
		{gossip, ar_gossip:set_delay(GS, MaxDelay)}
	]),
	{ok, set_delay};
handle(SPid, {set_xfer_speed, Speed}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	ar_node_state:update(SPid, [
		{gossip, ar_gossip:set_xfer_speed(GS, Speed)}
	]),
	{ok, set_xfer_speed};
handle(SPid, {set_mining_delay, Delay}) ->
	ar_node_state:update(SPid, [
		{mining_delay, Delay}
	]),
	{ok, set_mining_delay};
handle(_SPid, Msg) ->
	{error, {unknown_node_worker_message, Msg}}.

%% @doc Handle the gossip receive results.
handle_gossip(SPid, {NewGS, {new_block, Peer, _Height, BShadow, _BDS, Recall}}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case process_new_block(StateIn#{ gossip => NewGS }, BShadow, Recall, Peer) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ar_node_state:update(SPid, [{gossip, NewGS}])
	end,
	{ok, process_new_block};
handle_gossip(SPid, {NewGS, {add_tx, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [node, txs, waiting_txs, height]),
	case add_tx(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle_gossip(SPid, {NewGS, ignore}) ->
	ar_node_state:update(SPid, [
		{gossip, NewGS}
	]),
	{ok, ignore};
handle_gossip(SPid, {NewGS, UnhandledMsg}) ->
	{ok, NPid} = ar_node_state:lookup(SPid, node),
	ar:info(
		[
			{node, NPid},
			{unhandeled_gossip_msg, UnhandledMsg}
		]
	),
	ar_node_state:update(SPid, [
		{gossip, NewGS}
	]),
	{ok, ignore}.

%%%
%%% Private API functions.
%%%

%% @doc Add new transaction to a server state.
add_tx(StateIn, TX, GS) ->
	#{
		node := Node,
		waiting_txs := WaitingTXs
	} = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	timer:send_after(
		ar_node_utils:calculate_delay(byte_size(TX#tx.data)),
		Node,
		{add_tx_to_mining_pool, TX}
	),
	{ok, [
		{waiting_txs, WaitingTXs ++ [TX]},
		{gossip, NewGS}
	]}.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
add_tx_to_mining_pool(StateIn, TX, NewGS) ->
	#{
		txs := TXs,
		waiting_txs := WaitingTXs
	} = StateIn,
	memsup:start_link(),
	{ok, [
		{txs, TXs ++ [TX]},
		{gossip, NewGS},
		{waiting_txs, WaitingTXs -- [TX]}
	]}.

%% @doc Remove a TX from the pools if the signature is valid.
cancel_tx(StateIn, TXID, Sig) ->
	#{txs := TXs, waiting_txs := WaitingTXs } = StateIn,
	{
		ok,
		[
			{txs, maybe_remove_tx(TXs, TXID, Sig)},
			{waiting_txs, maybe_remove_tx(WaitingTXs, TXID, Sig)}
		]
	}.

%% @doc Find and remove TXs from the state if the given TXID and signature are valid.
maybe_remove_tx(TXs, TXID, Sig) ->
	lists:filter(
		fun(TX) ->
			if TX#tx.id == TXID ->
				% Return false (meaning filter it from the list)
				% for the TX if the sig /does/ verify correctly.
				case ar:d(ar_wallet:verify(TX#tx.owner, TXID, Sig)) of
					true ->
						ar:info(
							[
								{cancelling_tx, ar_util:encode(TXID)},
								{
									on_behalf_of,
									ar_util:encode(ar_wallet:to_address(TX#tx.owner))
								}
							]
						),
						false;
					false -> true
				end;
			true -> true
			end
		end,
		TXs
	).

%% @doc Validate whether a new block is legitimate, then handle it, optionally
%% dropping or starting a fork recovery as appropriate.
process_new_block(#{ hash_list := not_joined }, BShadow, _Recall, _Peer) ->
	ar:info([
		ar_node_worker,
		ignore_block,
		{reason, not_joined},
		{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
	]),
	none;
process_new_block(#{ height := Height } = StateIn, BShadow, Recall, Peer)
		when BShadow#block.height == Height + 1 ->
	#{ hash_list := HashList, wallet_list := WalletList, block_txs_pairs := BlockTXPairs } = StateIn,
	case generate_block_from_shadow(StateIn, BShadow, Recall, Peer) of
		{ok, {NewB, RecallB}} ->
			B = ar_util:get_head_block(HashList),
			StateNew = StateIn#{ wallet_list => NewB#block.wallet_list },
			TXs = NewB#block.txs,
			case ar_node_utils:validate(StateNew, NewB, TXs, B, RecallB) of
				{invalid, Reason} ->
					ar:info([
						{could_not_validate_new_block, ar_util:encode(NewB#block.indep_hash)},
						{reason, Reason}
					]),
					none;
				valid ->
					TXReplayCheck = ar_tx_replay_pool:verify_block_txs(
						TXs,
						NewB#block.diff,
						Height,
						NewB#block.timestamp,
						WalletList,
						BlockTXPairs
					),
					case TXReplayCheck of
						invalid ->
							ar:warn([
								ar_node_worker,
								process_new_block,
								transaction_replay_detected,
								{block_indep_hash, ar_util:encode(NewB#block.indep_hash)},
								{txs, lists:map(fun(TX) -> ar_util:encode(TX#tx.id) end, TXs)}
							]),
							none;
						valid ->
							case whereis(fork_recovery_server) of
								undefined ->
									{ok, ar_node_utils:integrate_new_block(StateNew, NewB, TXs)};
								_ ->
									{ok, ar_node_utils:fork_recover(StateIn, Peer, NewB)}
							end
					end
				end;
		error ->
			none
	end;
process_new_block(#{ height := Height }, BShadow, _Recall, _Peer)
		when BShadow#block.height =< Height ->
	ar:info(
		[
			{ignoring_block_below_current, ar_util:encode(BShadow#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, BShadow#block.height}
		]
	),
	none;
process_new_block(#{ height := Height } = StateIn, BShadow, _Recall, Peer)
		when (BShadow#block.height > Height + 1) ->
	#{ hash_list := HashList, cumulative_diff := CDiff } = StateIn,
	case is_fork_preferable(BShadow, CDiff, HashList) of
		true ->
			case ar_block:reconstruct_hash_list_from_shadow(BShadow#block.hash_list, HashList) of
				{ok, NewHashList} ->
					{ok, ar_node_utils:fork_recover(StateIn, Peer, BShadow#block { hash_list = NewHashList })};
				{error, _} ->
					none
			end;
		false ->
			none
	end.

generate_block_from_shadow(StateIn, BShadow, Recall, Peer) ->
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, aggregate_txs(StateIn)),
	case MissingTXIDs of
		[] ->
			generate_block_from_shadow(StateIn, BShadow, Recall, TXs, Peer);
		_ ->
			ar:info([
				ar_node_worker,
				block_not_accepted,
				{transactions_missing_in_mempool_for_block, ar_util:encode(BShadow#block.indep_hash)},
				{missing_txs, lists:map(fun ar_util:encode/1, MissingTXIDs)}
			]),
			error
	end.

generate_block_from_shadow(StateIn, BShadow, Recall, TXs, Peer) ->
	#{ hash_list := HashList } = StateIn,
	case ar_block:reconstruct_hash_list_from_shadow(BShadow#block.hash_list, HashList) of
		{ok, NewHashList} ->
			generate_block_from_shadow(StateIn, BShadow, Recall, TXs, NewHashList, Peer);
		{error, _} ->
			error
	end.

generate_block_from_shadow(StateIn, BShadow, Recall, TXs, NewHashList, Peer) ->
	#{ hash_list := HashList } = StateIn,
	{RecallIndepHash, _, Key, Nonce} = Recall,
	MaybeRecallB = case ar_block:get_recall_block(Peer, RecallIndepHash, NewHashList, Key, Nonce) of
		unavailable ->
			RecallHash = ar_node_utils:find_recall_hash(BShadow, HashList),
			FetchedRecallB = ar_node_utils:get_full_block(Peer, RecallHash, HashList),
			case ?IS_BLOCK(FetchedRecallB) of
				true ->
					ar_storage:write_full_block(FetchedRecallB),
					FetchedRecallB;
				false ->
					ar:warn(failed_to_get_recall_block),
					unavailable
			end;
		LocalRecallB ->
			LocalRecallB
	end,
	case MaybeRecallB of
		unavailable ->
			error;
		RecallB ->
			NewWalletList = generate_wallet_list_from_shadow(StateIn, BShadow, RecallB, TXs),
			{ok, {BShadow#block{ hash_list = NewHashList, wallet_list = NewWalletList, txs = TXs}, RecallB}}
	end.

pick_txs(TXIDs, TXs) ->
	lists:foldr(
		fun(TXID, {Found, Missing}) ->
			case [TX || TX <- TXs, TX#tx.id == TXID] of
				[] ->
					%% This disk read should almost never be useful. Presumably, the only reason to find some of these
					%% transactions on disk is they had been written prior to the call, what means they are
					%% from an orphaned fork, more than one block behind.
					case ar_storage:read_tx(TXID) of
						unavailable ->
							{Found, [TXID | Missing]};
						TX ->
							{[TX | Found], Missing}
					end;
				[TX | _] ->
					{[TX | Found], Missing}
			end
		end,
		{[], []},
		TXIDs
	).

generate_wallet_list_from_shadow(StateIn, BShadow, RecallB, TXs) ->
	#{
		reward_pool := RewardPool,
		wallet_list := WalletList,
		height := Height
	} = StateIn,
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			RewardPool,
			TXs,
			BShadow#block.reward_addr,
			RecallB#block.block_size,
			BShadow#block.weave_size,
			BShadow#block.height,
			BShadow#block.diff,
			BShadow#block.timestamp
		),
	ar_node_utils:apply_mining_reward(
		ar_node_utils:apply_txs(WalletList, TXs, Height),
		BShadow#block.reward_addr,
		FinderReward,
		BShadow#block.height
	).

%% @doc Verify a new block found by a miner, integrate it.
integrate_block_from_miner(#{ hash_list := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) ->
	#{
		id              := BinID,
		hash_list       := HashList,
		wallet_list     := RawWalletList,
		txs             := TXs,
		gossip          := GS,
		reward_addr     := RewardAddr,
		tags            := Tags,
		reward_pool     := OldPool,
		weave_size      := OldWeaveSize,
		height          := Height,
		block_txs_pairs := BlockTXPairs
	} = StateIn,
	% Calculate the new wallet list (applying TXs and mining rewards).
	RecallB = ar_node_utils:find_recall_block(HashList),
	WeaveSize = OldWeaveSize +
		lists:foldl(
			fun(TX, Acc) ->
				Acc + byte_size(TX#tx.data)
			end,
			0,
			MinedTXs
		),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			OldPool,
			MinedTXs,
			RewardAddr,
			RecallB#block.block_size,
			WeaveSize,
			length(HashList),
			Diff,
			Timestamp
		),
	ar:info(
		[
			calculated_reward_for_mined_block,
			{finder_reward, FinderReward},
			{new_reward_pool, RewardPool},
			{reward_address, RewardAddr},
			{old_reward_pool, OldPool},
			{txs, length(MinedTXs)},
			{recall_block_size, RecallB#block.block_size},
			{weave_size, WeaveSize},
			{length, length(HashList)}
		]
	),
	WalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(RawWalletList, MinedTXs, Height),
			RewardAddr,
			FinderReward,
			length(HashList)
		),
	StateNew = StateIn#{ wallet_list => WalletList },
	%% Build the block record, verify it, and gossip it to the other nodes.
	[NextB | _] = ar_weave:add(
		HashList, MinedTXs, HashList, RewardAddr, RewardPool,
		WalletList, Tags, RecallB, Diff, Nonce, Timestamp),
	B = ar_util:get_head_block(HashList),
	BlockValid = ar_node_utils:validate(
		StateNew,
		NextB,
		MinedTXs,
		B,
		RecallB = ar_node_utils:find_recall_block(HashList)
	),
	case BlockValid of
		{invalid, _} ->
			reject_block_from_miner(StateIn, invalid_block);
		valid ->
			TXReplayCheck = ar_tx_replay_pool:verify_block_txs(
				MinedTXs,
				Diff,
				Height,
				Timestamp,
				RawWalletList,
				BlockTXPairs
			),
			case TXReplayCheck of
				invalid ->
					reject_block_from_miner(StateIn, tx_replay);
				valid ->
					ar_storage:write_full_block(NextB, MinedTXs),
					NewHL = [NextB#block.indep_hash | HashList],
					NewBlockTXPairs = ar_node_utils:update_block_txs_pairs(
						NextB#block.indep_hash,
						[TX#tx.id || TX <- MinedTXs],
						BlockTXPairs
					),
					ValidTXs = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
						NewBlockTXPairs,
						TXs -- MinedTXs,
						NextB#block.diff,
						NextB#block.height,
						NextB#block.wallet_list
					),
					NewHL = [NextB#block.indep_hash | HashList],
					ar_storage:write_block_hash_list(BinID, NewHL),
					ar_miner_log:mined_block(NextB#block.indep_hash),
					ar:report_console(
						[
							{node, self()},
							{accepted_block, NextB#block.height},
							{indep_hash, ar_util:encode(NextB#block.indep_hash)},
							{recall_block, RecallB#block.height},
							{recall_hash, RecallB#block.indep_hash},
							{txs, length(MinedTXs)},
							case is_atom(RewardAddr) of
								true -> {reward_address, unclaimed};
								false -> {reward_address, ar_util:encode(RewardAddr)}
							end
						]
					),
					Recall = {
						RecallB#block.indep_hash,
						RecallB#block.block_size,
						<<>>,
						<<>>
					},
					BDS = generate_block_data_segment(NextB, RecallB),
					{NewGS, _} =
						ar_gossip:send(
							GS,
							{new_block, self(), NextB#block.height, NextB, BDS, Recall}
						),
					{ok, ar_node_utils:reset_miner(
						StateNew#{
							hash_list            => NewHL,
							current              => hd(NewHL),
							gossip               => NewGS,
							txs                  => ValidTXs,
							height               => NextB#block.height,
							reward_pool          => RewardPool,
							diff                 => NextB#block.diff,
							last_retarget        => NextB#block.last_retarget,
							weave_size           => NextB#block.weave_size,
							block_txs_pairs      => NewBlockTXPairs
						}
					)}
			end
	end.

reject_block_from_miner(StateIn, Reason) ->
	ar:err([
		ar_node_worker,
		miner_produced_invalid_block,
		Reason
	]),
	{ok, ar_node_utils:reset_miner(StateIn)}.

%% @doc Generates the data segment for the NextB where the RecallB is the recall
%% block of the previous block to NextB.
generate_block_data_segment(NextB, RecallB) ->
	ar_block:generate_block_data_segment(
		ar_storage:read_block(NextB#block.previous_block, NextB#block.hash_list),
		RecallB,
		lists:map(fun ar_storage:read_tx/1, NextB#block.txs),
		NextB#block.reward_addr,
		NextB#block.timestamp,
		NextB#block.tags
	).

%% @doc Handle executed fork recovery.
recovered_from_fork(#{id := BinID, hash_list := not_joined} = StateIn, BHL, BlockTXPairs) ->
	#{ txs := TXs } = StateIn,
	NewB = ar_storage:read_block(hd(BHL), BHL),
	ar:report_console(
		[
			node_joined_successfully,
			{height, NewB#block.height}
		]
	),
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	ValidTXs = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_storage:write_block_hash_list(BinID, BHL),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			hash_list            => BHL,
			current              => NewB#block.indep_hash,
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			txs                  => ValidTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			block_txs_pairs      => BlockTXPairs
		}
	)};
recovered_from_fork(#{ hash_list := HashList } = StateIn, BHL, BlockTXPairs) ->
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	NewB = ar_storage:read_block(hd(BHL), BHL),
	case is_fork_preferable(NewB, maps:get(cumulative_diff, StateIn), HashList) of
		true ->
			do_recovered_from_fork(StateIn, NewB, BlockTXPairs);
		false ->
			none
	end;
recovered_from_fork(_StateIn, _, _) ->
	none.

do_recovered_from_fork(StateIn, NewB, BlockTXPairs) ->
	#{ id := BinID, txs := TXs } = StateIn,
	ar:report_console(
		[
			fork_recovered_successfully,
			{height, NewB#block.height}
		]
	),
	ar_miner_log:fork_recovered(NewB#block.indep_hash),
	NextBHL = [NewB#block.indep_hash | NewB#block.hash_list],
	ValidTXs = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_storage:write_block_hash_list(BinID, NextBHL),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			hash_list            => NextBHL,
			current              => NewB#block.indep_hash,
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			txs                  => ValidTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			cumulative_diff      => NewB#block.cumulative_diff,
			block_txs_pairs      => BlockTXPairs
		}
	)}.

%% @doc Test whether a new fork is 'preferable' to the current one.
%% The highest cumulated diff is the one with most work performed and should
%% therefor be prefered.
is_fork_preferable(ForkB, _, CurrentBHL) when ForkB#block.height < ?FORK_1_6 ->
	ForkB#block.height > length(CurrentBHL);
is_fork_preferable(ForkB, CurrentCDiff, _) ->
	ForkB#block.cumulative_diff > CurrentCDiff.

%% @doc Aggregates the transactions of a state to one list.
aggregate_txs(#{txs := TXs, waiting_txs := WaitingTXs}) ->
	TXs ++ lists:reverse(WaitingTXs).
