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
handle(SPid, {replace_block_list, [Block | _] = Blocks}) ->
	BI = lists:map(fun(B) -> {B#block.indep_hash, B#block.weave_size} end, Blocks),
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
process_new_block(#{ block_index := not_joined }, BShadow, _POA, _Peer) ->
	ar:info([
		ar_node_worker,
		ignore_block,
		{reason, not_joined},
		{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
	]),
	none;
process_new_block(#{ height := Height }, BShadow, _POA, _Peer)
		when BShadow#block.height =< Height ->
	ar:info(
		[
			{ignoring_block_below_current, ar_util:encode(BShadow#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, BShadow#block.height}
		]
	),
	none;
process_new_block(#{ height := Height } = State, BShadow, POA, Peer)
		when BShadow#block.height >= Height + 1 ->
	ShadowHeight = BShadow#block.height,
	ShadowHL = BShadow#block.hash_list,
	#{ block_index := BI, legacy_hash_list := LegacyHL, block_txs_pairs := BlockTXPairs } = State,
	case get_diverged_block_hashes(ShadowHeight, ShadowHL, BI, LegacyHL) of
		{error, no_intersection} ->
			ar:warn([
				{event, new_block_shadow_block_index_no_intersection},
				{block_shadow_hash_list,
					lists:map(fun ar_util:encode/1, lists:sublist(ShadowHL, ?STORE_BLOCKS_BEHIND_CURRENT))},
				{node_hash_list_last_blocks,
					lists:map(fun({H, _}) -> ar_util:encode(H) end, lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT))},
				{legacy_hash_list_last_blocks,
					lists:map(fun ar_util:encode/1, lists:sublist(LegacyHL, ?STORE_BLOCKS_BEHIND_CURRENT))}
			]),
			none;
		{ok, {}} ->
			apply_new_block(State, BShadow#block { hash_list = ?BI_TO_BHL(BI) }, POA, Peer);
		{ok, {DivergedHashes, BIBeforeFork}} ->
			NewBlockTXPairs =
				prepare_block_tx_pairs_for_fork_through_2_0(Height, length(BIBeforeFork) - 1, BlockTXPairs, LegacyHL),
			RecoveryHashes = [BShadow#block.indep_hash | DivergedHashes],
			maybe_fork_recover(State, BShadow, Peer, RecoveryHashes, BIBeforeFork, NewBlockTXPairs)
	end.

%% Take a block shadow and the hashes of the recent blocks and return
%% {ok, {a list of diverged hashes, the base block index}} - in this case
%% the block becomes a fork recovery target. If the shadow index matches
%% the head of the current state, return {ok, {}} - this block can be then
%% validated against the current state.
%%
%% LegacyHL is used when the node is below 2.0 and fork recovers to a
%% target block above 2.0.
%%
%% Return {error, no_intersection} when there is no common block hash within
%% the last ?STORE_BLOCKS_BEHIND_CURRENT hashes.
get_diverged_block_hashes(ShadowHeight, ShadowHL, BI, LegacyHL) ->
	ShortShadowHL = lists:sublist(ShadowHL, ?STORE_BLOCKS_BEHIND_CURRENT),
	ShortHL = ?BI_TO_BHL(lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT)),
	ShortLegacyHL = lists:sublist(LegacyHL, ?STORE_BLOCKS_BEHIND_CURRENT),
	JoinedHL = ar_block:join_v1_v2_hash_list(length(BI), ShortHL, ShortLegacyHL),
	case get_diverged_block_hashes(ShortShadowHL, JoinedHL) of
		{error, no_intersection} = Error ->
			Error;
		{ok, []} ->
			{ok, {}};
		{ok, DivergedHashes} ->
			Fork_2_0 = ar_fork:height_2_0(),
			ForkDepth = length(DivergedHashes),
			Height = length(BI) - 1,
			case ShadowHeight - ForkDepth < Fork_2_0 - 1 andalso ShadowHeight > Fork_2_0 - 1 of
				true ->
					Fork_2_0_Depth = ForkDepth - (ShadowHeight - Fork_2_0),
					{_, HLBeforeFork} = lists:split(Fork_2_0_Depth, LegacyHL),
					{ok, {DivergedHashes, [{H, 0} || H <- HLBeforeFork]}};
				false ->
					{_, BIBeforeFork} = lists:split(ForkDepth - (ShadowHeight - Height) + 1, BI),
					{ok, {DivergedHashes, BIBeforeFork}}
			end
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

apply_new_block(State, BShadow, POA, Peer) ->
	#{ height := Height, block_index := BI, wallet_list := WalletList, block_txs_pairs := BlockTXPairs } = State,
	case generate_block_from_shadow(State, BShadow, POA, Peer) of
		{ok, {NewB, NewPOA}} ->
			B = ar_util:get_head_block(BI),
			StateNew = State#{ wallet_list => NewB#block.wallet_list },
			TXs = NewB#block.txs,
			case ar_node_utils:validate(StateNew, NewB, TXs, B, NewPOA) of
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

generate_block_from_shadow(State, BShadow, POA, Peer) ->
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, aggregate_txs(State)),
	case MissingTXIDs of
		[] ->
			generate_block_from_shadow(State, BShadow, POA, TXs, Peer);
		_ ->
			ar:info([
				ar_node_worker,
				block_not_accepted,
				{transactions_missing_in_mempool_for_block, ar_util:encode(BShadow#block.indep_hash)},
				{missing_txs, lists:map(fun ar_util:encode/1, MissingTXIDs)}
			]),
			error
	end.

generate_block_from_shadow(State, BShadow, POA, TXs, _Peer) when is_record(POA, poa) ->
	{ok,
		{
			BShadow#block {
				wallet_list = generate_wallet_list_from_shadow(State, BShadow, POA, TXs),
				txs = TXs
			},
			POA
		}
	};
generate_block_from_shadow(State, BShadow, Recall, TXs, Peer) ->
	#{ block_index := BI } = State,
	{RecallIndepHash, Key, Nonce} =
		case Recall of
			B when is_record(B, block) -> {B#block.indep_hash, <<>>, B#block.nonce};
			undefined ->
				{
					ar_util:get_recall_hash(BShadow#block.previous_block, BShadow#block.height - 1, BI),
					<<>>,
					<<>>
				};
			{RecallH, _, K, N} ->
				{RecallH, K, N}
		end,
	MaybeRecallB = case ar_block:get_recall_block(Peer, RecallIndepHash, BI, Key, Nonce) of
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
			NewWalletList = generate_wallet_list_from_shadow(State, BShadow, RecallB, TXs),
			{ok, {BShadow#block{ wallet_list = NewWalletList, txs = TXs }, RecallB}}
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

%% Remove the elements above the first fork block from `BlockTXPairs`
%% so that transactions' block anchors be verified during the fork recovery.
%%
%% If the height is above 2.0 (the block hash format has been changed), but the
%% first block(s) to apply during the fork recovery are below 2.0, update
%% `BlockTXPairs` to contain the block hashes in the v1 format.
prepare_block_tx_pairs_for_fork_through_2_0(Height, ForkBase, BlockTXPairs, LegacyHL) ->
	Fork_2_0 = ar_fork:height_2_0(),
	{_AfterFork, BlockTXPairsBeforeFork} = lists:split(Height - ForkBase, BlockTXPairs),
	case Height >= Fork_2_0 - 1 andalso ForkBase < Fork_2_0 of
		true ->
			lists:map(
				fun({{_, TXIDs}, H}) ->
					{H, TXIDs}
				end,
				lists:zip(
					BlockTXPairsBeforeFork,
					lists:sublist(LegacyHL, Fork_2_0 - ForkBase, length(BlockTXPairsBeforeFork))
				)
			);
		false ->
			BlockTXPairsBeforeFork
	end.

maybe_fork_recover(State, BShadow, Peer, RecoveryHashes, BI, BlockTXPairs) ->
	#{
		block_index := StateBI,
		legacy_hash_list := LegacyHL,
		cumulative_diff := CDiff,
		node:= Node
	} = State,
	case is_fork_preferable(BShadow, CDiff, StateBI) of
		false ->
			none;
		true ->
			{ok, fork_recover(Node, Peer, RecoveryHashes, {BI, LegacyHL}, BlockTXPairs)}
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

%% @doc Verify a new block found by the miner, integrate it.
integrate_block_from_miner(#{ block_index := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp, _POA) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp, POA) ->
	#{
		id               := BinID,
		block_index      := BI,
		wallet_list      := RawWalletList,
		txs              := TXs,
		gossip           := GS,
		reward_addr      := RewardAddr,
		tags             := Tags,
		reward_pool      := OldPool,
		weave_size       := OldWeaveSize,
		height           := Height,
		block_txs_pairs  := BlockTXPairs,
		legacy_hash_list := LegacyHL
	} = StateIn,
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
			{event, calculated_reward_for_mined_block},
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
		BI, MinedTXs, BI, LegacyHL, RewardAddr, RewardPool,
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
					{NewBI, NewLegacyHL} = ar_node_utils:update_block_index(NextB, BI, LegacyHL),
					NewBlockTXPairs = ar_node_utils:update_block_txs_pairs(NextB, BlockTXPairs, NewBI),
					{ValidTXs, InvalidTXs} =
						ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
							NewBlockTXPairs,
							TXs -- MinedTXs,
							NextB#block.diff,
							NextB#block.height,
							NextB#block.wallet_list
						),
					ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
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
					BDS = generate_block_data_segment(NextB, POA, BI),
					{NewGS, _} =
						ar_gossip:send(
							GS,
							{new_block, self(), NextB#block.height, NextB, BDS, POA}
						),
					{ok, ar_node_utils:reset_miner(
						StateNew#{
							block_index          => NewBI,
							current              => element(1, hd(NewBI)),
							gossip               => NewGS,
							txs                  => ValidTXs,
							height               => NextB#block.height,
							reward_pool          => RewardPool,
							diff                 => NextB#block.diff,
							last_retarget        => NextB#block.last_retarget,
							weave_size           => NextB#block.weave_size,
							block_txs_pairs      => NewBlockTXPairs,
							legacy_hash_list     => NewLegacyHL
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

generate_block_data_segment(NextB, POA, BI) ->
	ar_block:generate_block_data_segment(
		ar_storage:read_block(NextB#block.previous_block, BI),
		POA,
		lists:map(fun ar_storage:read_tx/1, NextB#block.txs),
		NextB#block.reward_addr,
		NextB#block.timestamp,
		NextB#block.tags
	).

%% @doc Handle executed fork recovery.
recovered_from_fork(#{id := BinID, block_index := not_joined} = StateIn, {BI, LegacyHL}, BlockTXPairs) ->
	#{ txs := TXs } = StateIn,
	NewB = ar_storage:read_block(element(1, hd(BI)), BI),
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
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
	ar_storage:write_block_block_index(BinID, BI),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => BI,
			legacy_hash_list     => LegacyHL,
			current              => element(1, hd(BI)),
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
recovered_from_fork(#{ block_index := CurrentBI } = StateIn, {BI, LegacyHL}, BlockTXPairs) ->
	case whereis(fork_recovery_server) of
		undefined -> ok;
		_		  -> erlang:unregister(fork_recovery_server)
	end,
	NewB = ar_storage:read_block(element(1, hd(BI)), BI),
	case is_fork_preferable(NewB, maps:get(cumulative_diff, StateIn), CurrentBI) of
		true ->
			do_recovered_from_fork(StateIn, NewB, {BI, LegacyHL}, BlockTXPairs);
		false ->
			none
	end;
recovered_from_fork(_StateIn, _, _) ->
	none.

do_recovered_from_fork(StateIn, NewB, {BI, LegacyHL}, BlockTXPairs) ->
	#{ id := BinID, txs := TXs } = StateIn,
	ar:info(
		[
			{event, fork_recovered_successfully},
			{height, NewB#block.height}
		]
	),
	ar_miner_log:fork_recovered(NewB#block.indep_hash),
	{ValidTXs, InvalidTXs} = ar_tx_replay_pool:pick_txs_to_keep_in_mempool(
		BlockTXPairs,
		TXs,
		NewB#block.diff,
		NewB#block.height,
		NewB#block.wallet_list
	),
	ar_node_utils:log_invalid_txs_drop_reason(InvalidTXs),
	ar_storage:write_block_block_index(BinID, BI),
	{ok, ar_node_utils:reset_miner(
		StateIn#{
			block_index          => BI,
			legacy_hash_list     => LegacyHL,
			current              => element(1, hd(BI)),
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

get_diverged_block_hashes_test_() ->
	ar_test_fork:test_on_fork(
		height_2_0,
		5,
		fun() ->
			%% Before 2.0.
			?assertEqual(
				{error, no_intersection},
				get_diverged_block_hashes(
					4,
					[bsh1, bsh2, bsh3, bsh4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}],
					[]
				)
			),
			?assertEqual(
				{error, no_intersection},
				get_diverged_block_hashes(
					4,
					lists:seq(1, ?STORE_BLOCKS_BEHIND_CURRENT) ++ [h1, h2, h3, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}],
					[]
				)
			),
			?assertEqual(
				{ok, {}},
				get_diverged_block_hashes(
					4,
					[h1, h2, h3, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}],
					[]
				)
			),
			?assertEqual(
				{ok, {[h11], [{h2, 0}, {h3, 0}, {h4, 0}]}},
				get_diverged_block_hashes(
					4,
					[h11, h2, h3, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}],
					[]
				)
			),
			?assertEqual(
				{ok, {[h1, h2, h13], [{h4, 0}]}},
				get_diverged_block_hashes(
					4,
					[h1, h2, h13, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}],
					[]
				)
			),
			%% After 2.0.
			?assertEqual(
				{error, no_intersection},
				get_diverged_block_hashes(
					10,
					[bsh1, bsh2, bsh3, bsh4],
					[
						{bsh1, 0}, {bsh2, 0}, {bsh3, 0}, {h4, 0}, {h5, 0},
						{h6, 0}, {h7, 0}, {h8, 0}, {h9, 0}, {h10, 0}
					],
					[]
				)
			),
			?assertEqual(
				{error, no_intersection},
				get_diverged_block_hashes(
					100,
					lists:seq(1, ?STORE_BLOCKS_BEHIND_CURRENT) ++ [h1, h2, h3, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}]
						++ lists:map(fun(N) -> {N, 0} end, lists:seq(1, 96)),
					[]
				)
			),
			?assertEqual(
				{ok, {
					[h11],
					[
						{h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}, {h7, 0},
						{h8, 0}, {h9, 0}, {h10, 0}
					]
				}},
				get_diverged_block_hashes(
					10,
					[h11, h2, h3, h4],
					[
						{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0},
						{h6, 0}, {h7, 0}, {h8, 0}, {h9, 0}, {h10, 0}
					],
					[]
				)
			),
			?assertEqual(
				{ok, {
					[h1, h2, h13],
					[
						{h4, 0}, {h5, 0}, {h6, 0},
						{h7, 0}, {h8, 0}, {h9, 0}, {h10, 0}
					]
				}},
				get_diverged_block_hashes(
					10,
					[h1, h2, h13, h4],
					[
						{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0},
						{h6, 0}, {h7, 0}, {h8, 0}, {h9, 0}, {h10, 0}
					],
					[]
				)
			),
			?assertEqual(
				{ok, {}},
				get_diverged_block_hashes(
					6,
					[h1],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}],
					[lh1, lh2, lh3, lh4, lh5]
				)
			),
			%% Via 2.0.
			?assertEqual(
				{error, no_intersection},
				get_diverged_block_hashes(
					6,
					[h11, h2, h3, h4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}],
					[lh1, lh2, lh3, lh4, lh5, lh6]
				)
			),
			?assertEqual(
				{ok, {[h1, lh21, lh22], [{lh3, 0}, {lh4, 0}, {lh5, 0}]}},
				get_diverged_block_hashes(
					6,
					[h1, lh21, lh22, lh3, lh4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}],
					[lh1, lh2, lh3, lh4, lh5]
				)
			),
			?assertEqual(
				{ok, {[h13, h12, lh21, lh22], [{lh3, 0}, {lh4, 0}, {lh5, 0}]}},
				get_diverged_block_hashes(
					7,
					[h13, h12, lh21, lh22, lh3, lh4],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}],
					[lh1, lh2, lh3, lh4, lh5]
				)
			),
			?assertEqual(
				{ok, {[h11, lh12, lh2, lh31], [{lh4, 0}, {lh5, 0}]}},
				get_diverged_block_hashes(
					6,
					[h11, lh12, lh2, lh31, lh4, lh5],
					[{h1, 0}, {h2, 0}, {h3, 0}, {h4, 0}, {h5, 0}, {h6, 0}],
					[lh1, lh2, lh3, lh4, lh5]
				)
			)
		end
	).

prepare_block_tx_pairs_for_fork_through_2_0_test_() ->
	ar_test_fork:test_on_fork(
		height_2_0,
		5,
		fun() ->
			%% Before 2.0.
			?assertEqual(
				[{h2, [tx2]}],
				prepare_block_tx_pairs_for_fork_through_2_0(
					1,
					0,
					[{h1, [tx1]}, {h2, [tx2]}],
					[]
				)
			),
			%% After 2.0.
			?assertEqual(
				[{h2, [tx2]}],
				prepare_block_tx_pairs_for_fork_through_2_0(
					6,
					5,
					[{h1, [tx1]}, {h2, [tx2]}],
					[lh]
				)
			),
			%% Via 2.0.
			?assertEqual(
				[{lh1, [tx3]}],
				prepare_block_tx_pairs_for_fork_through_2_0(
					6,
					4,
					[{h1, [tx1]}, {h2, [tx2]}, {h3, [tx3]}],
					[lh1, lh2]
				)
			)
		end
	).
