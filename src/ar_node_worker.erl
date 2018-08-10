%% @doc Server to queue ar_node state-changing tasks.
-module(ar_node_worker).

-export([start/1, stop/1, cast/2, call/2, call/3]).

-include("ar.hrl").
-include("ar_node.hrl").

%% @doc Start a node worker.
start(SPid) ->
	Pid = spawn(fun() -> server(SPid) end),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Send an asynchronous task to a node worker. The answer
%% will be sent to the caller.
cast(Pid, Task) ->
	Pid ! {Task, self()},
	ok.

%% @doc Send a synchronous task to a node worker. The timeout
%% can be passed, default is 5000 ms.
call(Pid, Task) ->
	call(Pid, Task, 5000).

call(Pid, Task, Timeout) ->
	cast(Pid, Task),
	receive
		Reply ->
			Reply
	after
		Timeout ->
			{error, timeout}
	end.

%%%
%%% Server functions.
%%%

%% @doc Main server loop.
server(SPid) ->
	receive
		{Task, Sender} ->
			try handle(SPid, Task, Sender) of
				Reply ->
					Sender ! Reply,
					server(SPid)
			catch
				throw:Term ->
					ar:report( [ {'NodeWorkerEXCEPTION', {Term} } ]),
					server(SPid);
				exit:Term ->
					ar:report( [ {'NodeWorkerEXIT', Term} ] ),
					server(SPid);
				error:Term ->
					ar:report( [ {'NodeWorkerERROR', {Term, erlang:get_stacktrace()} } ]),
					server(SPid)
			end;
		stop ->
			ok
	end.

%% @doc Handle the server tasks. Return values a sent to the caller. Simple tasks
%% can be done directy, more complex ones with dependencies from arguments are
%% handled as private API functions. SPid allows to access the state server,
%% inserts have to be atomic.
handle(SPid, {add_tx, TX}, Sender) ->
	add_tx(SPid, TX, Sender),
	{ok, add_tx};
handle(SPid, {add_tx, TX, NewGS}, Sender) ->
	add_tx(SPid, TX, NewGS, Sender),
	{ok, add_tx};
handle(SPid, {process_new_block, NewGS, NewB, RecallB, Peer, HashList}, _Sender) ->
	ar_node_worker:process_new_block(SPid, NewGS, NewB, RecallB, Peer, HashList),
	{ok, process_new_block};
handle(_SPid, Msg, _Sender) ->
	{error, {unknown_node_worker_message, Msg}}.

%%%
%%% Private API functions.
%%%

%% @doc Add new transaction to a server state.
add_tx(SPid, TX, Sender) ->
	{ok, [TXs, WaitingTXs, PotentialTXs]} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			ar_node_state:insert(SPid, [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])}
			]);
		_ ->
			% TODO mue: Space in string atom correct?
			ar_tx_db:put(TX#tx.id, ["last_tx_not_valid "]),
			ar_node_state:insert(SPid, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])}
			])
	end.

add_tx(SPid, TX, NewGS, Sender) ->
	{ok, [TXs, WaitingTXs, PotentialTXs]} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			ar_node_state:insert(SPid, [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])},
				{gossip, NewGS}
			]);
		_ ->
			ar_node_state:insert(SPid, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])},
				{gossip, NewGS}
		])
	end.

%% @doc Validate whether a new block is legitimate, then handle it, optionally
%% dropping or starting a fork recoverer as appropriate.
process_new_block(S, NewGS, NewB, _, _Peer, not_joined) ->
	join_weave(S#state { gossip = NewGS }, NewB),
	S;
process_new_block(RawS1, NewGS, NewB, unavailable, Peer, HashList)
		when NewB#block.height == RawS1#state.height + 1 ->
		% This block is at the correct height.
	RecallHash = ar_node:find_recall_hash(NewB, HashList),
	FullBlock = ar_node:get_full_block(Peer, RecallHash),
	case ?IS_BLOCK(FullBlock) of
		true ->
			% TODO: Cleanup full block -> shadow generation.
			RecallShadow = FullBlock#block { txs = [
													T#tx.id
													||
													T <- FullBlock#block.txs] },
			ar_storage:write_full_block(FullBlock),
			S = RawS1#state { gossip = NewGS },
			process_new_block(S, NewGS, NewB, RecallShadow, Peer, HashList);
		false ->
			ar:d(failed_to_get_recall_block),
			RawS1
	end;
process_new_block(RawS1, NewGS, NewB, RecallB, Peer, HashList)
		when NewB#block.height == RawS1#state.height + 1 ->
		% This block is at the correct height.
	S = RawS1#state { gossip = NewGS },
	% If transaction not found in state or storage, txlist built will be
	% incomplete and will fail in validate
	TXs = lists:foldr(
		fun(T, Acc) ->
			%state contains it
			case [
					TX
					||
					TX <-
						(S#state.txs ++ S#state.waiting_txs ++ S#state.potential_txs),
						TX#tx.id == T ]
			of
				[] ->
					case ar_storage:read_tx(T) of
						unavailable ->
							Acc;
						TX -> [TX | Acc]
					end;
				[TX | _] -> [TX | Acc]
			end
		end,
		[],
		NewB#block.txs
	),
	{FinderReward, _} =
		ar_node:calculate_reward_pool(
			S#state.reward_pool,
			TXs,
			NewB#block.reward_addr,
			ar_node:calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				NewB#block.height
			)
		),
	WalletList =
		ar_node:apply_mining_reward(
			ar_node:apply_txs(S#state.wallet_list, TXs),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		),
	NewS = S#state { wallet_list = WalletList },
	case ar_node:validate(
			NewS,
			NewB,
			TXs,
			ar_util:get_head_block(HashList), RecallB
	) of
		true ->
			% The block is legit. Accept it.
			case whereis(fork_recovery_server) of
				undefined -> integrate_new_block(NewS, NewB);
				_ -> fork_recover(S#state { gossip = NewGS }, Peer, NewB)
			end;
		false ->
			ar:d({could_not_validate_new_block, ar_util:encode(NewB#block.indep_hash)}),
			fork_recover(S#state { gossip = NewGS }, Peer, NewB)
	end;
process_new_block(S, NewGS, NewB, _RecallB, _Peer, _HashList)
		when NewB#block.height =< S#state.height ->
	% Block is lower than us, ignore it.
	ar:report(
		[
			{ignoring_block_below_current, ar_util:encode(NewB#block.indep_hash)},
			{current_height, S#state.height},
			{proposed_block_height, NewB#block.height}
		]
	),
	S#state { gossip = NewGS };
% process_new_block(S, NewGS, NewB, _RecallB, _Peer, _Hashlist)
%		when (NewB#block.height == S#state.height + 1) ->
	% Block is lower than fork recovery height, ignore it.
	% server(S#state { gossip = NewGS });
process_new_block(S, NewGS, NewB, _, Peer, _HashList)
		when (NewB#block.height > S#state.height + 1) ->
	fork_recover(S#state { gossip = NewGS }, Peer, NewB).

%%%
%%% Private functions.
%%%

%% @doc Get the conflicting transaction between state and passed ones.
get_conflicting_txs(STXs, TX) ->
	[
		T
	||
		T <-
			STXs,
			(
				(T#tx.last_tx == TX#tx.last_tx) and
				(T#tx.owner == TX#tx.owner)
			)
	].

%% @doc Recovery from a fork.
fork_recover(
	S = #state{hash_list = HashList}, Peer, NewB) ->
	case {whereis(fork_recovery_server), whereis(join_server)} of
		{undefined, undefined} ->
			PrioritisedPeers = ar_util:unique(Peer) ++
				case whereis(http_bridge_node) of
					undefined -> [];
					BridgePID ->
						ar_bridge:get_remote_peers(BridgePID)
				end,
			erlang:monitor(
				process,
				PID = ar_fork_recovery:start(
					PrioritisedPeers,
					NewB,
					HashList
				)
			),
			case PID of
				undefined -> ok;
				_ -> erlang:register(fork_recovery_server, PID)
			end;
		{undefined, _} ->
			ok;
		_ ->
			whereis(fork_recovery_server)
				! {update_target_block, NewB, ar_util:unique(Peer)}
	end,
	S.

%% @doc We have received a new valid block. Update the node state accordingly.
integrate_new_block(
		S = #state {
			txs = TXs,
			hash_list = HashList,
			wallet_list = WalletList,
			waiting_txs = WaitingTXs,
			potential_txs = PotentialTXs
		},
		NewB) ->
	% Filter completed TXs from the pending list.
	Diff = ar_mine:next_diff(NewB),
	RawKeepNotMinedTXs =
		lists:filter(
			fun(T) ->
				(not ar_weave:is_tx_on_block_list([NewB], T#tx.id)) and
				ar_tx:verify(T, Diff, WalletList)
			end,
			TXs
		),
	NotMinedTXs =
		lists:filter(
			fun(T) ->
				(not ar_weave:is_tx_on_block_list([NewB], T#tx.id))
			end,
			TXs ++ WaitingTXs ++ PotentialTXs
		),
	KeepNotMinedTXs = ar_node:filter_all_out_of_order_txs(
							NewB#block.wallet_list,
							RawKeepNotMinedTXs),
	BlockTXs = (TXs ++ WaitingTXs ++ PotentialTXs) -- NotMinedTXs,
	% Write new block and included TXs to local storage.
	ar_storage:write_tx(BlockTXs),
	ar_storage:write_block(NewB),
	% Recurse over the new block.
	ar:report_console(
		[
			{accepted_foreign_block, ar_util:encode(NewB#block.indep_hash)},
			{height, NewB#block.height}
		]
	),
	%ar:d({new_hash_list, [NewB#block.indep_hash | HashList]}),
	app_search:update_tag_table(NewB),
	lists:foreach(
		fun(T) ->
			ar_tx_db:maybe_add(T#tx.id)
		end,
		PotentialTXs
	),
	RecallB =
		ar_node:get_full_block(
			whereis(http_entrypoint_node),
			ar_node:find_recall_hash(NewB, [NewB#block.indep_hash | HashList])
		),
	case ?IS_BLOCK(RecallB) of
		true ->
			ar_key_db:put(
				RecallB#block.indep_hash,
				[
					{
						ar_block:generate_block_key(
							RecallB,
							NewB#block.previous_block),
						binary:part(NewB#block.indep_hash, 0, 16)
					}
				]
			);
		false -> ok
	end,
	ar_node:reset_miner(
		S#state {
			hash_list = [NewB#block.indep_hash | HashList],
			txs = ar_track_tx_db:remove_bad_txs(KeepNotMinedTXs),
			height = NewB#block.height,
			floating_wallet_list = ar_node:apply_txs(WalletList, TXs),
			reward_pool = NewB#block.reward_pool,
			potential_txs = [],
			diff = NewB#block.diff,
			last_retarget = NewB#block.last_retarget,
			weave_size = NewB#block.weave_size
		}
	).


%% @doc Catch up to the current height.
join_weave(S, NewB) ->
	ar_join:start(ar_gossip:peers(S#state.gossip), NewB).
