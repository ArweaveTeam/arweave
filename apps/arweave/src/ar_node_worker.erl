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
		{task, Task = {gossip_message, #gs_msg { data = {new_block, _, _, _, _, _}}}} ->
			handle_task(NPid, SPid, Task),
			server(NPid, SPid)
	after 0 ->
		receive
			{task, Task = {gossip_message, #gs_msg { data = {new_tx, _}}}} ->
				handle_task(NPid, SPid, Task),
				server(NPid, SPid)
		after 0 ->
			receive
				{task, Task} ->
					handle_task(NPid, SPid, Task),
					server(NPid, SPid);
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
			end
		end
	end.

%% @doc Safely handle tasks.
handle_task(NPid, SPid, Task) ->
	try handle(SPid, Task) of
		Reply ->
			NPid ! {worker, Reply}
	catch
		throw:Term ->
			ar:err( [ {'NodeWorkerEXCEPTION', Term } ]),
			NPid ! {worker, {error, Term}};
		exit:Term ->
			ar:err( [ {'NodeWorkerEXIT', Term} ] ),
			NPid ! {worker, {error, Term}};
		error:Term:Stacktrace ->
			ar:err( [ {'NodeWorkerERROR', {Term, Stacktrace} } ]),
			NPid ! {worker, {error, Term}}
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
handle(SPid, {move_tx_to_mining_pool, TX}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [gossip, txs, waiting_txs, height]),
	case move_tx_to_mining_pool(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, move_tx_to_mining_pool};
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
handle(SPid, {work_complete, BH, POA, MinedTXs, Diff, Nonce, Timestamp}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	#{ block_index := [ {CurrentBH, _ } | _] } = StateIn,
	case BH of
		CurrentBH ->
			case integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp, POA) of
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
handle(SPid, {fork_recovered, BI, BlockTXPairs}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case recovered_from_fork(StateIn, BI, BlockTXPairs) of
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
		{block_index, [{Block#block.indep_hash, Block#block.weave_size} | Block#block.block_index]},
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
handle_gossip(SPid, {NewGS, {new_block, Peer, _Height, BShadow, Recall}}) ->
	handle_gossip(SPid, {NewGS, {new_block, Peer, _Height, BShadow, <<>>, Recall}});
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
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs]),
	case add_tx(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle_gossip(SPid, {NewGS, {add_waiting_tx, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [waiting_txs]),
	case add_waiting_tx(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_waiting_tx};
handle_gossip(SPid, {NewGS, {move_tx_to_mining_pool, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [waiting_txs, txs]),
	case move_tx_to_mining_pool(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, move_tx_to_mining_pool};
handle_gossip(SPid, {NewGS, {drop_waiting_txs, TXs}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [waiting_txs]),
	case drop_waiting_txs(StateIn, TXs, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, drop_waiting_txs};
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

%% @doc Add the new transaction to the server state.
add_tx(StateIn, TX, GS) ->
	#{ txs := TXs } = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	{ok, [
		{txs, TXs ++ [TX]},
		{gossip, NewGS}
	]}.

%% @doc Add the new waiting transaction to the server state.
add_waiting_tx(StateIn, TX, GS) ->
	#{ waiting_txs := WaitingTXs } = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {add_waiting_tx, TX}),
	{ok, [
		{waiting_txs, WaitingTXs ++ [TX]},
		{gossip, NewGS}
	]}.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
move_tx_to_mining_pool(StateIn, TX, GS) ->
	#{
		txs := TXs,
		waiting_txs := WaitingTXs
	} = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {move_tx_to_mining_pool, TX}),
	{ok, [
		{txs, TXs ++ [TX]},
		{gossip, NewGS},
		{waiting_txs, WaitingTXs -- [TX]}
	]}.

drop_waiting_txs(#{ waiting_txs := WaitingTXs }, DropTXs, GS) ->
	{NewGS, _} = ar_gossip:send(GS, {drop_waiting_txs, DropTXs}),
	DropSet = sets:from_list(lists:map(fun(TX) -> TX#tx.id end, DropTXs)),
	FilteredTXs = lists:filter(
		fun(TX) ->
			not sets:is_element(TX#tx.id, DropSet)
		end,
		WaitingTXs
	),
	{ok, [
		{waiting_txs, FilteredTXs},
		{gossip, NewGS}
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
				case ar_wallet:verify(TX#tx.owner, TXID, Sig) of
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
process_new_block(#{ block_index := not_joined }, BShadow, _Recall, _Peer) ->
	ar:info([
		ar_node_worker,
		ignore_block,
		{reason, not_joined},
		{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
	]),
	none;
process_new_block(#{ height := Height } = StateIn, BShadow, Recall, Peer)
		when BShadow#block.height == Height + 1 ->
	#{ block_index := BI, wallet_list := WalletList, block_txs_pairs := BlockTXPairs } = StateIn,
	case generate_block_from_shadow(StateIn, BShadow, Recall, Peer) of
		{ok, {NewB, RecallB}} ->
			B = ar_util:get_head_block(BI),
			StateNew = StateIn#{ wallet_list => NewB#block.wallet_list },
			TXs = NewB#block.txs,
			case ar_node_utils:validate(StateNew, NewB, TXs, B, RecallB) of
				{invalid, _Reason} ->
					maybe_fork_recover_at_height_plus_one(StateIn, NewB, Peer);
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
								{block_header_hash, ar_util:encode(NewB#block.header_hash)},
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
	#{ block_index := BI, cumulative_diff := CDiff } = StateIn,
	case is_fork_preferable(BShadow, CDiff, BI) of
		true ->
			case ar_block:reconstruct_block_index_from_shadow(BShadow#block.block_index, BI) of
				{ok, NewBI} ->
					{ok, ar_node_utils:fork_recover(StateIn, Peer, BShadow#block { block_index = NewBI })};
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
	#{ block_index := BI } = StateIn,
	case ar_block:reconstruct_block_index_from_shadow(BShadow#block.block_index, BI) of
		{ok, NewBI} ->
			generate_block_from_shadow(StateIn, BShadow, Recall, TXs, NewBI, Peer);
		{error, _} ->
			error
	end.

generate_block_from_shadow(StateIn, BShadow, POA, TXs, NewBI, _Peer) when is_record(POA, poa) ->
	{ok,
		{
			BShadow#block {
				block_index = NewBI,
				wallet_list = generate_wallet_list_from_shadow(StateIn, BShadow, POA, TXs),
				txs = TXs
			},
			POA
		}
	}	;
generate_block_from_shadow(StateIn, BShadow, Recall, TXs, NewBI, Peer) ->
	#{ block_index := BI } = StateIn,
	{RecallIndepHash, Key, Nonce} =
		case Recall of
			B when is_record(B, block) -> {B#block.indep_hash, <<>>, B#block.nonce};
			undefined ->
				{
					ar_util:get_recall_hash(BShadow#block.previous_block, BShadow#block.height - 1, NewBI),
					<<>>,
					<<>>
				};
			{RecallH, _, K, N} ->
				{RecallH, K, N}
		end,
	MaybeRecallB = case ar_block:get_recall_block(Peer, RecallIndepHash, NewBI, Key, Nonce) of
		unavailable ->
			RecallHash = ar_util:get_recall_hash(BShadow, BI),
			FetchedRecallB = ar_node_utils:get_full_block(Peer, RecallHash, BI),
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
			{ok, {BShadow#block{ block_index = NewBI, wallet_list = NewWalletList, txs = TXs}, RecallB}}
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

generate_wallet_list_from_shadow(StateIn, BShadow, POA, TXs) ->
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
			POA,
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

maybe_fork_recover_at_height_plus_one(State, NewB, Peer) ->
	case lists:any(
		fun(TX) ->
			ar_firewall:scan_tx(TX) == reject
		end,
		NewB#block.txs
	) of
		true ->
			%% Disincentivize submission of globally rejected content.
			%% One block is an additional cost for the minority to
			%% make the majority store unwanted content.
			%% A tolerable downside of it is in a scenario when a half
			%% of the network rejects a chunk, the network is going to
			%% be more forky than it could.
			ar:info([
				ar_node_worker,
				new_block_contains_blacklisted_content,
				{indep_hash, ar_util:encode(NewB#block.indep_hash)}
			]),
			none;
		false ->
			#{ cumulative_diff := CDiff, block_index := BI } = State,
			case is_fork_preferable(NewB, CDiff, BI) of
				false ->
					none;
				true ->
					{ok, ar_node_utils:fork_recover(State, Peer, NewB)}
			end
	end.

%% @doc Verify a new block found by a miner, integrate it.
integrate_block_from_miner(#{ block_index := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp, _POA) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp, POA) ->
	#{
		id              := BinID,
		block_index     := BI,
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
			POA,
			WeaveSize,
			length(BI),
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
			{weave_size, WeaveSize},
			{length, length(BI)}
		]
	),
	WalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(RawWalletList, MinedTXs, Height),
			RewardAddr,
			FinderReward,
			length(BI)
		),
	StateNew = StateIn#{ wallet_list => WalletList },
	%% Build the block record, verify it, and gossip it to the other nodes.
	[NextB | _] = ar_weave:add(
		BI, MinedTXs, BI, RewardAddr, RewardPool,
		WalletList, Tags, POA, Diff, Nonce, Timestamp),
	B = ar_util:get_head_block(BI),
	BlockValid = ar_node_utils:validate(
		StateNew,
		NextB,
		MinedTXs,
		B,
		POA
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
					NewBI = [{NextB#block.indep_hash, NextB#block.weave_size} | BI],
					NewBlockTXPairs = ar_node_utils:update_block_txs_pairs(
						NextB#block.indep_hash,
						[TX#tx.id || TX <- MinedTXs],
						BlockTXPairs
					),
					{ValidTXs, InvalidTXs} =
						ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
							NewBlockTXPairs,
							TXs -- MinedTXs,
							NextB#block.diff,
							NextB#block.height,
							NextB#block.wallet_list
						),
					ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
					NewBI2 =
						case NextB#block.height == (?FORK_2_0) of
							true ->
								X = [ {NextB#block.header_hash, NextB#block.weave_size} | NextB#block.block_index ],
								ar:info(
									[
										switching_v1_to_v2_block_index,
										{height, NextB#block.height},
										{block, ar_util:encode(NextB#block.header_hash)}
									]
								),
								X;
							false ->
								[ {NextB#block.indep_hash, NextB#block.weave_size} | BI ]
						end,
					ar_storage:write_block_block_index(BinID, NewBI),
					ar_miner_log:mined_block(NextB#block.indep_hash),
					ar:report_console(
						[
							{node, self()},
							{accepted_block, NextB#block.height},
							{indep_hash, ar_util:encode(NextB#block.indep_hash)},
							{txs, length(MinedTXs)},
							case is_atom(RewardAddr) of
								true -> {reward_address, unclaimed};
								false -> {reward_address, ar_util:encode(RewardAddr)}
							end
						]
					),
					BDS = generate_block_data_segment(NextB, POA),
					{NewGS, _} =
						ar_gossip:send(
							GS,
							{new_block, self(), NextB#block.height, NextB, BDS, POA}
						),
					{ok, ar_node_utils:reset_miner(
						StateNew#{
							block_index          => NewBI2,
							current              => element(1, hd(NewBI2)),
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
generate_block_data_segment(NextB, POA) ->
	ar_block:generate_block_data_segment(
		ar_storage:read_block(NextB#block.previous_block, NextB#block.block_index),
		POA,
		lists:map(fun ar_storage:read_tx/1, NextB#block.txs),
		NextB#block.reward_addr,
		NextB#block.timestamp,
		NextB#block.tags
	).

%% @doc Handle executed fork recovery.
recovered_from_fork(#{id := BinID, block_index := not_joined} = StateIn, BI, BlockTXPairs) ->
	#{ txs := TXs } = StateIn,
	NewB = ar_storage:read_block(element(1, hd(BI)), BI),
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
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
	ar_storage:write_block_block_index(BinID, BI),
	lists:foreach(
		fun({BH, _}) ->
			ar_downloader:add_block(BH, BI, whereis(ar_downloader))
		end,
		BI
	),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => BI,
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
recovered_from_fork(#{ block_index := BBI } = StateIn, BI, BlockTXPairs) ->
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	NewB = ar_storage:read_block(element(1, hd(BI)), BI),
	case is_fork_preferable(NewB, maps:get(cumulative_diff, StateIn), BBI) of
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
	NextBI = [ {NewB#block.indep_hash, NewB#block.weave_size} | NewB#block.block_index],
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
	ar_storage:write_block_block_index(BinID, NextBI),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => NextBI,
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
is_fork_preferable(ForkB, _, CurrentBI) when ForkB#block.height < ?FORK_1_6 ->
	ForkB#block.height > length(CurrentBI);
is_fork_preferable(ForkB, CurrentCDiff, _) ->
	ForkB#block.cumulative_diff > CurrentCDiff.

%% @doc Aggregates the transactions of a state to one list.
aggregate_txs(#{txs := TXs, waiting_txs := WaitingTXs}) ->
	TXs ++ lists:reverse(WaitingTXs).
