%%%
%%% @doc Server to queue ar_node state-changing tasks.
%%%

-module(ar_node_worker).

-export([start/2, stop/1, cast/2, call/2, call/3]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%
%%% Public API.
%%%

%% @doc Start a node worker.
start(NPid, SPid) ->
	Pid = spawn(
		fun() ->
			%% The message queue of this process may grow big under load.
			%% The flag makes VM store messages off heap and do not perform
			%% expensive GC on them.
			process_flag(message_queue_data, off_heap),
			server(NPid, SPid)
		end
	),
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
		{task, Task = {gossip_message, #gs_msg { data = {new_block, _, _, _, _}}}} ->
			handle_task(NPid, SPid, Task),
			server(NPid, SPid)
	after 0 ->
		receive
			{task, Task = {gossip_message, #gs_msg { data = {add_waiting_tx, _}}}} ->
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
		ar_node_state:lookup(SPid, [gossip, node, txs, height, mempool_size]),
	case add_tx(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {move_tx_to_mining_pool, TX}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [gossip, txs, height]),
	case move_tx_to_mining_pool(StateIn, TX, maps:get(gossip, StateIn)) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, move_tx_to_mining_pool};
handle(SPid, {cancel_tx, TXID, Sig}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs]),
	{ok, StateOut} = cancel_tx(StateIn, TXID, Sig),
	ar_node_state:update(SPid, StateOut),
	{ok, cancel_tx};
handle(SPid, {process_new_block, Peer, Height, BShadow, BDS}) ->
	%% We have a new block. Distribute it to the gossip network. This is only
	%% triggered in the polling mode.
	{ok, StateIn} = ar_node_state:all(SPid),
	GS = maps:get(gossip, StateIn),
	ar_gossip:send(GS, {new_block, Peer, Height, BShadow, BDS}),
	{ok, process_new_block};
handle(SPid, {work_complete, BaseBH, NewB, MinedTXs, BDS, POA}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	#{ block_index := [{CurrentBH, _, _} | _]} = StateIn,
	case BaseBH of
		CurrentBH ->
			case integrate_block_from_miner(StateIn, NewB, MinedTXs, BDS, POA) of
				{ok, StateOut} ->
					ar_node_state:update(SPid, StateOut);
				none ->
					ok
			end,
			{ok, work_complete};
		_ ->
			ar:info([{event, ignore_mined_block}, {reason, accepted_foreign_block}]),
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
handle(SPid, automine) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	StateOut = ar_node_utils:start_mining(StateIn#{ automine => true }),
	ar_node_state:update(SPid, StateOut),
	{ok, automine};
handle(SPid, {replace_block_list, [Block | _] = Blocks}) ->
	BI = lists:map(fun ar_util:block_index_entry_from_block/1, Blocks),
	BlockTXPairs = lists:map(fun(B) -> {B#block.indep_hash, B#block.txs} end, Blocks),
	ar_node_state:update(SPid, [
		{block_index, BI},
		{block_txs_pairs, BlockTXPairs},
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
handle_gossip(SPid, {NewGS, {new_block, Peer, _Height, BShadow, _BDS}}) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case process_new_block(StateIn#{ gossip => NewGS }, BShadow, Peer) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ar_node_state:update(SPid, [{gossip, NewGS}])
	end,
	{ok, process_new_block};
handle_gossip(SPid, {NewGS, {add_tx, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, mempool_size]),
	case add_tx(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle_gossip(SPid, {NewGS, {add_waiting_tx, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, mempool_size]),
	case add_waiting_tx(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_waiting_tx};
handle_gossip(SPid, {NewGS, {move_tx_to_mining_pool, TX}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs]),
	case move_tx_to_mining_pool(StateIn, TX, NewGS) of
		{ok, StateOut} ->
			ar_node_state:update(SPid, StateOut);
		none ->
			ok
	end,
	{ok, move_tx_to_mining_pool};
handle_gossip(SPid, {NewGS, {drop_waiting_txs, TXs}}) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, mempool_size]),
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
	#{ txs := TXs, mempool_size := MS} = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	{ok, [
		{txs, maps:put(TX#tx.id, {TX, ready_for_mining}, TXs)},
		{gossip, NewGS},
		{mempool_size, ar_node_utils:increase_mempool_size(MS, TX)}
	]}.

%% @doc Add the new waiting transaction to the server state.
add_waiting_tx(StateIn, TX, GS) ->
	#{ txs := WaitingTXs, mempool_size := MS } = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {add_waiting_tx, TX}),
	{ok, [
		{txs, maps:put(TX#tx.id, {TX, waiting}, WaitingTXs)},
		{gossip, NewGS},
		{mempool_size, ar_node_utils:increase_mempool_size(MS, TX)}
	]}.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
move_tx_to_mining_pool(StateIn, TX, GS) ->
	#{ txs := TXs } = StateIn,
	{NewGS, _} = ar_gossip:send(GS, {move_tx_to_mining_pool, TX}),
	{ok, [
		{txs, maps:put(TX#tx.id, {TX, ready_for_mining}, TXs)},
		{gossip, NewGS}
	]}.

drop_waiting_txs(State, DroppedTXs, GS) ->
	#{ txs := TXs, mempool_size := {MempoolHeaderSize, MempoolDataSize} } = State,
	{NewGS, _} = ar_gossip:send(GS, {drop_waiting_txs, DroppedTXs}),
	{UpdatedTXs, DroppedTXMap} = lists:foldl(
		fun(TX, {Acc, DroppedAcc}) ->
			case maps:take(TX#tx.id, Acc) of
				{Value, Map} ->
					{Map, maps:put(TX#tx.id, Value, DroppedAcc)};
				error ->
					{Acc, DroppedAcc}
			end
		end,
		{TXs, maps:new()},
		DroppedTXs
	),
	{DroppedHeaderSize, DroppedDataSize} = ar_node_utils:calculate_mempool_size(DroppedTXMap),
	{ok, [
		{txs, UpdatedTXs},
		{gossip, NewGS},
		{mempool_size,
			{MempoolHeaderSize - DroppedHeaderSize, MempoolDataSize - DroppedDataSize}}
	]}.

%% @doc Remove a TX from the pools if the signature is valid.
cancel_tx(StateIn, TXID, Sig) ->
	#{txs := TXs } = StateIn,
	UpdatedTXs = maybe_remove_tx(TXs, TXID, Sig),
	{
		ok,
		[
			{txs, UpdatedTXs},
			{mempool_size, ar_node_utils:calculate_mempool_size(UpdatedTXs)}
		]
	}.

%% @doc Find and remove TXs from the state if the given TXID and signature are valid.
maybe_remove_tx(TXs, RemoveTXID, Sig) ->
	maps:filter(
		fun(TXID, {TX, _}) ->
			if TXID == RemoveTXID ->
				%% Return false (meaning filter it from the list)
				%% for the TX if the sig /does/ verify correctly.
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
process_new_block(#{ block_index := not_joined }, BShadow, _Peer) ->
	ar:info([
		ar_node_worker,
		ignore_block,
		{reason, not_joined},
		{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
	]),
	none;
process_new_block(#{ height := Height }, BShadow, _Peer)
		when BShadow#block.height =< Height ->
	ar:info(
		[
			{ignoring_block_below_current, ar_util:encode(BShadow#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, BShadow#block.height}
		]
	),
	none;
process_new_block(#{ height := Height } = State, BShadow, Peer)
		when BShadow#block.height >= Height + 1 ->
	ShadowHeight = BShadow#block.height,
	ShadowHL = BShadow#block.hash_list,
	#{ block_index := BI,  block_txs_pairs := BlockTXPairs } = State,
	case get_diverged_block_hashes(ShadowHeight, ShadowHL, BI) of
		{error, no_intersection} ->
			ar:warn([
				{event, new_block_shadow_block_index_no_intersection},
				{block_shadow_hash_list,
					lists:map(fun ar_util:encode/1, lists:sublist(ShadowHL, ?STORE_BLOCKS_BEHIND_CURRENT))},
				{node_hash_list_last_blocks,
					lists:map(fun({H, _, _}) -> ar_util:encode(H) end, lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT))}
			]),
			none;
		{ok, {}} ->
			apply_new_block(State, BShadow#block{ hash_list = ?BI_TO_BHL(BI) });
		{ok, {DivergedHashes, BIBase}} ->
			HeightBase = length(BIBase) - 1,
			{_, BlockTXPairsBase} = lists:split(Height - HeightBase, BlockTXPairs),
			RecoveryHashes = [BShadow#block.indep_hash | DivergedHashes],
			maybe_fork_recover(State, BShadow, Peer, RecoveryHashes, BIBase, BlockTXPairsBase)
	end.

%% Take a block shadow and the hashes of the recent blocks and return
%% {ok, {a list of diverged hashes, the base block index}} - in this case
%% the block becomes a fork recovery target. If the shadow index matches
%% the head of the current state, return {ok, {}} - this block can be then
%% validated against the current state.
%%
%% Return {error, no_intersection} when there is no common block hash within
%% the last ?STORE_BLOCKS_BEHIND_CURRENT hashes.
get_diverged_block_hashes(ShadowHeight, ShadowHL, BI) ->
	ShortShadowHL = lists:sublist(ShadowHL, ?STORE_BLOCKS_BEHIND_CURRENT),
	ShortHL = ?BI_TO_BHL(lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT)),
	case get_diverged_block_hashes(ShortShadowHL, ShortHL) of
		{error, no_intersection} = Error ->
			Error;
		{ok, []} ->
			{ok, {}};
		{ok, DivergedHashes} ->
			ForkDepth = length(DivergedHashes),
			Height = length(BI) - 1,
			{_, BIBase} = lists:split(ForkDepth - (ShadowHeight - Height) + 1, BI),
			{ok, {DivergedHashes, BIBase}}
	end.

get_diverged_block_hashes(ShadowHL, HL) ->
	LastShadowH = lists:last(ShadowHL),
	case lists:member(LastShadowH, HL) of
		true ->
			get_diverged_block_hashes_reversed(
				lists:reverse(ShadowHL),
				lists:dropwhile(fun(H) -> H /= LastShadowH end, lists:reverse(HL))
			);
		false ->
			{error, no_intersection}
	end.

get_diverged_block_hashes_reversed([H | Tail1], [H | Tail2]) ->
	get_diverged_block_hashes_reversed(Tail1, Tail2);
get_diverged_block_hashes_reversed([], []) -> {ok, []};
get_diverged_block_hashes_reversed(DivergedHashes, _) ->
	{ok, lists:reverse(DivergedHashes)}.

apply_new_block(State, BShadow) ->
	#{
		height := Height,
		block_index := BI,
		wallet_list := WalletList,
		block_txs_pairs := BlockTXPairs
	} = State,
	case generate_block_from_shadow(State, BShadow) of
		{ok, NewB} ->
			B = ar_util:get_head_block(BI),
			StateNew = State#{ wallet_list => NewB#block.wallet_list },
			TXs = NewB#block.txs,
			case ar_node_utils:validate(BI, NewB#block.wallet_list, NewB, TXs, B) of
				{invalid, _Reason} ->
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
							{ok, ar_node_utils:integrate_new_block(StateNew, NewB, TXs)}
					end
				end;
		error ->
			none
	end.

generate_block_from_shadow(#{ txs := MempoolTXs } = State, BShadow) ->
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, MempoolTXs),
	case MissingTXIDs of
		[] ->
			generate_block_from_shadow(State, BShadow, TXs);
		_ ->
			ar:info([
				ar_node_worker,
				block_not_accepted,
				{transactions_missing_in_mempool_for_block, ar_util:encode(BShadow#block.indep_hash)},
				{missing_txs, lists:map(fun ar_util:encode/1, MissingTXIDs)}
			]),
			error
	end.

generate_block_from_shadow(State, BShadow, TXs) ->
	{WalletListHash, WalletList} =
		generate_wallet_list_from_shadow(State, BShadow, TXs),
	B = BShadow#block {
		wallet_list = WalletList,
		wallet_list_hash = WalletListHash,
		txs = TXs
	},
	{ok, B}.

pick_txs(TXIDs, TXs) ->
	lists:foldr(
		fun(TXID, {Found, Missing}) ->
			case maps:get(TXID, TXs, tx_not_in_mempool) of
				tx_not_in_mempool ->
					%% This disk read should almost never be useful. Presumably, the only reason to find some of these
					%% transactions on disk is they had been written prior to the call, what means they are
					%% from an orphaned fork, more than one block behind.
					case ar_storage:read_tx(TXID) of
						unavailable ->
							{Found, [TXID | Missing]};
						TX ->
							{[TX | Found], Missing}
					end;
				{TX, _} ->
					{[TX | Found], Missing}
			end
		end,
		{[], []},
		TXIDs
	).

generate_wallet_list_from_shadow(StateIn, BShadow, TXs) ->
	#{
		reward_pool := RewardPool,
		wallet_list := WalletList,
		height := Height
	} = StateIn,
	RewardAddr = BShadow#block.reward_addr,
	NewHeight = BShadow#block.height,
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			RewardPool,
			TXs,
			RewardAddr,
			no_recall,
			BShadow#block.weave_size,
			NewHeight,
			BShadow#block.diff,
			BShadow#block.timestamp
		),
	NewWalletList = ar_node_utils:apply_mining_reward(
		ar_node_utils:apply_txs(WalletList, TXs, Height),
		RewardAddr,
		FinderReward,
		NewHeight
	),
	WalletListHash = ar_block:hash_wallet_list(NewHeight, RewardAddr, NewWalletList),
	{WalletListHash, NewWalletList}.

maybe_fork_recover(State, BShadow, Peer, RecoveryHashes, BI, BlockTXPairs) ->
	#{
		block_index := StateBI,
		cumulative_diff := CDiff,
		node:= Node
	} = State,
	case is_fork_preferable(BShadow, CDiff, StateBI) of
		false ->
			none;
		true ->
			{ok, fork_recover(Node, Peer, RecoveryHashes, BI, BlockTXPairs)}
	end.

fork_recover(Node, Peer, RecoveryHashes, BI, BlockTXPairs) ->
	case {whereis(fork_recovery_server), whereis(join_server)} of
		{undefined, undefined} ->
			PrioritisedPeers = ar_util:unique(Peer) ++
				case whereis(http_bridge_node) of
					undefined -> [];
					BridgePID -> ar_bridge:get_remote_peers(BridgePID)
				end,
			erlang:monitor(
				process,
				PID = ar_fork_recovery:start(
					PrioritisedPeers,
					RecoveryHashes,
					BI,
					Node,
					BlockTXPairs
				)
			),
			case PID of
				undefined -> ok;
				_		  -> erlang:register(fork_recovery_server, PID)
			end;
		{undefined, _} ->
			ok;
		_ ->
			whereis(fork_recovery_server) ! {update_target_hashes, RecoveryHashes, Peer}
	end,
	none.

%% @doc Integrate the block found by us.
integrate_block_from_miner(#{ block_index := not_joined }, _NewB, _MinedTXs, _BDS, _POA) ->
	none;
integrate_block_from_miner(StateIn, NewB, MinedTXs, BDS, _POA) ->
	#{
		block_index      := BI,
		txs              := TXs,
		gossip           := GS,
		block_txs_pairs  := BlockTXPairs
	} = StateIn,
	ar_storage:write_full_block(NewB, MinedTXs),
	NewBI = ar_node_utils:update_block_index(NewB#block{ txs = MinedTXs }, BI),
	NewBlockTXPairs = ar_node_utils:update_block_txs_pairs(NewB, BlockTXPairs),
	ValidTXs =
		ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
			NewBlockTXPairs,
			lists:foldl(
				fun(TX, Acc) ->
					maps:remove(TX#tx.id, Acc)
				end,
				TXs,
				MinedTXs
			),
			NewB#block.diff,
			NewB#block.height,
			NewB#block.wallet_list
		),
	ar_miner_log:mined_block(NewB#block.indep_hash, NewB#block.height),
	ar:info(
		[
			{event, mined_block},
			{indep_hash, ar_util:encode(NewB#block.indep_hash)},
			{txs, length(MinedTXs)}
		]
	),
	{NewGS, _} = ar_gossip:send(
		GS,
		{new_block, self(), NewB#block.height, NewB, BDS}
	),
	lists:foreach(
		fun(TX) ->
			ar_downloader:enqueue_random({tx_data, TX}),
			ar_tx_queue:drop_tx(TX)
		end,
		MinedTXs
	),
	NewState = ar_node_utils:reset_miner(
		StateIn#{
			block_index          => NewBI,
			current              => element(1, hd(NewBI)),
			gossip               => NewGS,
			txs                  => ValidTXs,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			block_txs_pairs      => NewBlockTXPairs,
			wallet_list          => NewB#block.wallet_list,
			mempool_size         => ar_node_utils:calculate_mempool_size(ValidTXs)
		}
	),
	{ok, NewState}.

%% @doc Handle executed fork recovery.
recovered_from_fork(#{ block_index := not_joined } = StateIn, BI, BlockTXPairs) ->
	#{ txs := TXs } = StateIn,
	NewB = ar_storage:read_block(element(1, hd(BI))),
	ar:info(
		[
			{event, node_joined_successfully},
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
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => BI,
			current              => element(1, hd(BI)),
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			txs                  => ValidTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			block_txs_pairs      => BlockTXPairs,
			mempool_size         => ar_node_utils:calculate_mempool_size(ValidTXs)
		}
	)};
recovered_from_fork(#{ block_index := CurrentBI } = StateIn, BI, BlockTXPairs) ->
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	NewB = ar_storage:read_block(element(1, hd(BI))),
	case is_fork_preferable(NewB, maps:get(cumulative_diff, StateIn), CurrentBI) of
		true ->
			do_recovered_from_fork(StateIn, NewB, BI, BlockTXPairs);
		false ->
			none
	end;
recovered_from_fork(_StateIn, _, _) ->
	none.

do_recovered_from_fork(StateIn, NewB, BI, BlockTXPairs) ->
	#{ txs := TXs } = StateIn,
	ar:info(
		[
			{event, fork_recovered_successfully},
			{height, NewB#block.height}
		]
	),
	ar_miner_log:fork_recovered(NewB#block.indep_hash),
	ValidTXs = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => BI,
			current              => element(1, hd(BI)),
			wallet_list          => NewB#block.wallet_list,
			height               => NewB#block.height,
			reward_pool          => NewB#block.reward_pool,
			txs                  => ValidTXs,
			diff                 => NewB#block.diff,
			last_retarget        => NewB#block.last_retarget,
			weave_size           => NewB#block.weave_size,
			cumulative_diff      => NewB#block.cumulative_diff,
			block_txs_pairs      => BlockTXPairs,
			mempool_size         => ar_node_utils:calculate_mempool_size(ValidTXs)
		}
	)}.

%% @doc Test whether a new fork is 'preferable' to the current one.
%% The highest cumulated diff is the one with most work performed and should
%% therefor be prefered.
is_fork_preferable(ForkB, _, CurrentBI) when ForkB#block.height < ?FORK_1_6 ->
	ForkB#block.height > length(CurrentBI);
is_fork_preferable(ForkB, CurrentCDiff, _) ->
	ForkB#block.cumulative_diff > CurrentCDiff.

get_diverged_block_hashes_test_() ->
	?assertEqual(
		{error, no_intersection},
		get_diverged_block_hashes(
			4,
			[bsh1, bsh2, bsh3, bsh4],
			[{h1, 0, <<>>}, {h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]
		)
	),
	?assertEqual(
		{error, no_intersection},
		get_diverged_block_hashes(
			4,
			lists:seq(1, ?STORE_BLOCKS_BEHIND_CURRENT) ++ [h1, h2, h3, h4],
			[{h1, 0, <<>>}, {h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]
		)
	),
	?assertEqual(
		{ok, {}},
		get_diverged_block_hashes(
			4,
			[h1, h2, h3, h4],
			[{h1, 0, <<>>}, {h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]
		)
	),
	?assertEqual(
		{ok, {[h11], [{h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]}},
		get_diverged_block_hashes(
			4,
			[h11, h2, h3, h4],
			[{h1, 0, <<>>}, {h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]
		)
	),
	?assertEqual(
		{ok, {[h1, h2, h13], [{h4, 0, <<>>}]}},
		get_diverged_block_hashes(
			4,
			[h1, h2, h13, h4],
			[{h1, 0, <<>>}, {h2, 0, <<>>}, {h3, 0, <<>>}, {h4, 0, <<>>}]
		)
	).
