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
		ar_node_state:lookup(
			SPid,
			[gossip, node, txs, waiting_txs, potential_txs, height]
		),
	case add_tx(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {add_tx_to_mining_pool, TX}) ->
	{ok, StateIn} = ar_node_state:lookup(
		SPid,
		[gossip, txs, waiting_txs, floating_wallet_list, height]
	),
	case add_tx_to_mining_pool(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx_to_mining_pool};
handle(SPid, {cancel_tx, TXID, Sig}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	{ok, StateOut} = cancel_tx(StateIn, TXID, Sig),
	ar_node_state:update(SPid, StateOut),
	{ok, cancel_tx};
handle(SPid, {process_new_block, Peer, Height, NewB, BDS, Recall}) ->
	%% We have a new block. Distribute it to the gossip network. This is only
	%% triggered in polling mode.
	{ok, StateIn} = ar_node_state:all(SPid),
	GS = maps:get(gossip, StateIn),
	HashList = maps:get(hash_list, StateIn),
	{NewGS, _} = ar_gossip:send(GS, {new_block, Peer, Height, NewB, BDS, Recall}),
	ar_node_state:update(SPid, [{gossip, NewGS}]),
	{RecallIndepHash, _, Key, Nonce} = Recall,
	RecallB = ar_block:get_recall_block(Peer, RecallIndepHash, NewB#block.hash_list, Key, Nonce),
	case process_new_block(StateIn, NewGS, NewB, RecallB, Peer, HashList) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, process_new_block};
handle(SPid, {work_complete, MinedTXs, Diff, Nonce, Timestamp}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, work_complete};
handle(SPid, {fork_recovered, NewHs}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case recovered_from_fork(StateIn, NewHs) of
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
handle_gossip(SPid, {NewGS, {new_block, Peer, _Height, NewB, _BDS, Recall}}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	HashList = maps:get(hash_list, StateIn),
	{RecallIndepHash, _, Key, Nonce} = Recall,
	RecallB = ar_block:get_recall_block(Peer, RecallIndepHash, NewB#block.hash_list, Key, Nonce),
	case process_new_block(StateIn, NewGS, NewB, RecallB, Peer, HashList) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, process_new_block};
handle_gossip(SPid, {NewGS, {add_tx, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [node, txs, waiting_txs, potential_txs, height]),
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
		waiting_txs := WaitingTXs,
		potential_txs := PotentialTXs,
		height := Height
	} = StateIn,
	case ar_node_utils:get_conflicting_txs(aggregate_txs(StateIn), TX) of
		[] ->
			case Height >= ar_fork:height_1_8() of
				true ->
					Node ! {update_floating_wallet_list, TX};
				false ->
					noop
			end,
			{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
			timer:send_after(
				ar_node_utils:calculate_delay(byte_size(TX#tx.data)),
				Node,
				{add_tx_to_mining_pool, TX}
			),
			{ok, [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])},
				{gossip, NewGS}
			]};
		_ ->
			ar_tx_db:put_error_codes(TX#tx.id, ["last_tx_not_valid"]),
			{ok, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])},
				{gossip, GS}
			]}
	end.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
add_tx_to_mining_pool(StateIn, TX, NewGS) ->
	#{
		txs := TXs,
		waiting_txs := WaitingTXs
	} = StateIn,
	memsup:start_link(),
	{_, Mem} = lists:keyfind(system_total_memory, 1, memsup:get_system_memory_data()),
	case (Mem div 4) > byte_size(TX#tx.data) of
		true ->
			NewTXs = TXs ++ [TX],
			{ok, [
				{txs, NewTXs},
				{gossip, NewGS},
				{waiting_txs, WaitingTXs -- [TX]}
			]};
		false ->
			{ok, [
				{gossip, NewGS},
				{waiting_txs, WaitingTXs -- [TX]}
			]}
	end.

%% @doc Remove a TX from the pools if the signature is valid.
cancel_tx(StateIn, TXID, Sig) ->
	#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs } = StateIn,
	{
		ok,
		[
			{txs, maybe_remove_tx(TXs, TXID, Sig)},
			{waiting_txs, maybe_remove_tx(WaitingTXs, TXID, Sig)},
			{potential_txs, maybe_remove_tx(PotentialTXs, TXID, Sig)}
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
%% dropping or starting a fork recoverer as appropriate.
process_new_block(_StateIn, NewGS, NewB, _, _Peer, not_joined) ->
	ar_join:start(ar_gossip:peers(NewGS), NewB),
	none;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, unavailable, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	% This block is at the correct height.
	RecallHash = ar_node_utils:find_recall_hash(NewB, HashList),
	RecallB = ar_node_utils:get_full_block(Peer, RecallHash, HashList),
	case ?IS_BLOCK(RecallB) of
		true ->
			ar_storage:write_full_block(RecallB),
			StateNext = StateIn#{ gossip => NewGS },
			process_new_block(StateNext, NewGS, NewB, RecallB, Peer, HashList);
		false ->
			ar:warn(failed_to_get_recall_block),
			none
	end;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, RecallB, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	%% This block is at the correct height.
	{TXs, MissingTXIDs} = pick_txs(NewB#block.txs, aggregate_txs(StateIn)),
	case MissingTXIDs of
		[] ->
			process_new_block2(StateIn, NewGS, NewB, RecallB, Peer, HashList, TXs);
		_ ->
			ar:info([
				ar_node_worker,
				block_not_accepted,
				{transactions_missing_in_mempool_for_block, ar_util:encode(NewB#block.indep_hash)},
				{missing_txs, lists:map(fun ar_util:encode/1, MissingTXIDs)}
			]),
			{ok, []}
	end;
process_new_block(#{ height := Height }, NewGS, NewB, _RecallB, _Peer, _HashList)
		when NewB#block.height =< Height ->
	% Block is lower than us, ignore it.
	ar:info(
		[
			{ignoring_block_below_current, ar_util:encode(NewB#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, NewB#block.height}
		]
	),
	{ok, [{gossip, NewGS}]};
process_new_block(#{ height := Height, cumulative_diff := CDiff } = StateIn, NewGS, NewB, _RecallB, Peer, HashList)
		when (NewB#block.height > Height + 1) ->
	case is_fork_preferable(NewB, CDiff, HashList) of
		true ->
			{ok, ar_node_utils:fork_recover(StateIn#{ gossip => NewGS }, Peer, NewB)};
		false ->
			none
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

process_new_block2(StateIn, NewGS, NewB, RecallB, Peer, HashList, TXs) ->
	StateNext = StateIn#{ gossip => NewGS },
	#{
		reward_pool := RewardPool,
		wallet_list := WalletList
	} = StateNext,
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				NewB#block.height
			)
		),
	NewWalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(WalletList, TXs),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		),
	StateNew = StateNext#{ wallet_list => NewWalletList },
	% TODO: Setting the state gossip for fork_recover/3 has to be
	% checked. The gossip is already set to NewGS in first function
	% statement. Compare to pre-refactoring.
	StateOut = case ar_node_utils:validate(StateNew, NewB, TXs, ar_util:get_head_block(HashList), RecallB) of
		valid ->
			% The block is legit. Accept it.
			case whereis(fork_recovery_server) of
				undefined -> ar_node_utils:integrate_new_block(StateNew, NewB);
				_		  -> ar_node_utils:fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
			end;
		{invalid, Reason} ->
			ar:info([
				{could_not_validate_new_block, ar_util:encode(NewB#block.indep_hash)},
				{reason, Reason}
			])
	end,
	{ok, StateOut}.

%% @doc Verify a new block found by a miner, integrate it.
integrate_block_from_miner(#{ hash_list := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) ->
	#{
		id            := BinID,
		hash_list     := HashList,
		wallet_list   := RawWalletList,
		txs           := TXs,
		gossip        := GS,
		reward_addr   := RewardAddr,
		tags          := Tags,
		reward_pool   := OldPool,
		weave_size    := OldWeaveSize,
		potential_txs := PotentialTXs,
		height        := Height
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
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				WeaveSize,
				length(HashList)
			)
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
			ar_node_utils:apply_txs(RawWalletList, MinedTXs),
			RewardAddr,
			FinderReward,
			length(HashList)
		),
	% Store the transactions that we know about, but were not mined in
	% this block.
	NotMinedTXs = pick_not_mined_txs(Height, Diff, WalletList, TXs, MinedTXs),
	StateNew = StateIn#{ wallet_list => WalletList },
	% Build the block record, verify it, and gossip it to the other nodes.
	[NextB | _] = ar_weave:add(
		HashList, MinedTXs, HashList, RewardAddr, RewardPool,
		WalletList, Tags, RecallB, Diff, Nonce, Timestamp),
	case ar_node_utils:validate(
			StateNew,
			NextB,
			MinedTXs,
			ar_util:get_head_block(HashList),
			RecallB = ar_node_utils:find_recall_block(HashList)) of
		{invalid, _} ->
			ar:report_console(miner_produced_invalid_block),
			case rand:uniform(5) of
				1 ->
					#{ gossip := StateInGS } = StateIn,
					{ok, ar_node_utils:reset_miner(
						StateIn#{
							gossip		  => StateInGS,
							txs			  => [], % TXs not included in the block
							potential_txs => []
						}
					)};
				_ ->
					{ok, ar_node_utils:reset_miner(StateIn)}
			end;
		valid ->
			ar_storage:write_full_block(NextB, MinedTXs),
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
			lists:foreach(
				fun(MinedTX) ->
					ar:info(
						[
							{successfully_mined_tx_into_block, ar_util:encode(MinedTX#tx.id)},
							{size, byte_size(MinedTX#tx.data)}
						]
					)
				end,
				MinedTXs
			),
			lists:foreach(
				fun(T) ->
					ar_tx_db:ensure_error(T#tx.id)
				end,
				PotentialTXs
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
					txs                  => ar_track_tx_db:remove_bad_txs(NotMinedTXs), % TXs not included in the block
					height               => NextB#block.height,
					floating_wallet_list => ar_node_utils:apply_txs(WalletList, NotMinedTXs),
					reward_pool          => RewardPool,
					potential_txs        => [],
					diff                 => NextB#block.diff,
					last_retarget        => NextB#block.last_retarget,
					weave_size           => NextB#block.weave_size
				}
			)}
	end.

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

pick_not_mined_txs(Height, Diff, WalletList, TXs, MinedTXs) ->
	case Height >= ar_fork:height_1_8() of
		true ->
			{NotMinedTXs, _} = lists:foldl(
				fun(T, {Acc, FloatingWalletList}) ->
					case ar_tx:verify(T, Diff, Height + 2, FloatingWalletList) of
						true ->
							{Acc ++ [T], ar_node_utils:apply_tx(FloatingWalletList, T)};
						false ->
							{Acc, FloatingWalletList}
					end
				end,
				{[], WalletList},
				ar_node_utils:filter_all_out_of_order_txs(WalletList, TXs -- MinedTXs)
			),
			NotMinedTXs;
		false ->
			lists:filter(
				fun(T) ->
					ar_tx:verify(T, Diff, Height + 2, WalletList)
				end,
				ar_node_utils:filter_all_out_of_order_txs(WalletList, TXs -- MinedTXs)
			)
	end.

%% @doc Handle executed fork recovery.
recovered_from_fork(#{ id := BinID, hash_list := not_joined} = StateIn, NewHs) ->
	timer:sleep(15 * 1000), % Waiting for blocks to be written to disk before reading NewB.
	NewB = ar_storage:read_block(hd(NewHs), NewHs),
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
	% ar_cleanup:remove_invalid_blocks(NewHs),
	TXPool = maps:get(txs, StateIn) ++ maps:get(potential_txs, StateIn),
	TXs =
		ar_node_utils:filter_all_out_of_order_txs(
			NewB#block.wallet_list,
			TXPool
		),
	PotentialTXs = TXPool -- TXs,
	ar_storage:write_block_hash_list(BinID, NewHs),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			hash_list            => NewHs,
			current              => NewB#block.indep_hash,
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			floating_wallet_list => NewB#block.wallet_list,
			txs                  => TXs,
			potential_txs        => PotentialTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size
		}
	)};
recovered_from_fork(#{ hash_list := HashList } = StateIn, NewHs) ->
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	NewB = ar_storage:read_block(hd(NewHs), NewHs),
	case is_fork_preferable(NewB, maps:get(cumulative_diff, StateIn), HashList) of
		true ->
			do_recovered_from_fork(StateIn, NewB);
		false ->
			none
	end;
recovered_from_fork(_StateIn, _) ->
	none.

do_recovered_from_fork(StateIn, NewB) ->
	#{ id := BinID } = StateIn,
	ar:report_console(
		[
			fork_recovered_successfully,
			{height, NewB#block.height}
		]
	),
	ar_miner_log:fork_recovered(NewB#block.indep_hash),
	NextBHL = [NewB#block.indep_hash | NewB#block.hash_list],
	% ar_cleanup:remove_invalid_blocks(NewHs),
	TXPool = maps:get(txs, StateIn) ++ maps:get(potential_txs, StateIn),
	TXs =
		ar_node_utils:filter_all_out_of_order_txs(
			NewB#block.wallet_list,
			TXPool
		),
	PotentialTXs = TXPool -- TXs,
	ar_storage:write_block_hash_list(BinID, NextBHL),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			hash_list            => NextBHL,
			current              => NewB#block.indep_hash,
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			floating_wallet_list => NewB#block.wallet_list,
			txs                  => TXs,
			potential_txs        => PotentialTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			cumulative_diff      => NewB#block.cumulative_diff
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
aggregate_txs(#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs}) ->
	TXs ++ WaitingTXs ++ PotentialTXs.
