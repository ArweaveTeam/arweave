%%%
%%% @doc Server to queue ar_node state-changing tasks.
%%%

-module(ar_node_worker).

-export([start/1, stop/1, cast/2, call/2, call/3]).

-include("ar.hrl").

%%%
%%% Public API.
%%%

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
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case add_tx(StateIn, TX, Sender) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {add_tx, TX, NewGS}, Sender) ->
	{ok, StateIn} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	case add_tx(StateIn, TX, NewGS, Sender) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, add_tx};
handle(SPid, {process_new_block, NewGS, NewB, RecallB, Peer, HashList}, _Sender) ->
	% TODO mue: Reduce to only needed values later, but seem to be pretty much here.
	{ok, StateIn} = ar_node_state:all(SPid),
	case process_new_block(StateIn, NewGS, NewB, RecallB, Peer, HashList) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, process_new_block};
handle(SPid, {replace_block_list, NewBL}, _Sender) ->
	case replace_block_list(NewBL) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, replace_block_list};
handle(_SPid, Msg, _Sender) ->
	{error, {unknown_node_worker_message, Msg}}.

%%%
%%% Private API functions.
%%%

%% @doc Add new transaction to a server state.
add_tx(StateIn, TX, Sender) ->
	#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs} = StateIn,
	case get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			{ok , [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])}
			]};
		_ ->
			% TODO mue: Space in string atom correct?
			ar_tx_db:put(TX#tx.id, ["last_tx_not_valid "]),
			{ok, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])}
			]}
	end.

add_tx(StateIn, TX, NewGS, Sender) ->
	#{txs := TXs, waiting_txs := WaitingTXs, potential_txs := PotentialTXs} = StateIn,
	case get_conflicting_txs(TXs ++ WaitingTXs ++ PotentialTXs, TX) of
		[] ->
			timer:send_after(
				ar_node:calculate_delay(byte_size(TX#tx.data)),
				Sender,
				{apply_tx, TX}
			),
			{ok, [
				{waiting_txs, ar_util:unique([TX | WaitingTXs])},
				{gossip, NewGS}
			]};
		_ ->
			{ok, [
				{potential_txs, ar_util:unique([TX | PotentialTXs])},
				{gossip, NewGS}
			]}
	end.

%% @doc Validate whether a new block is legitimate, then handle it, optionally
%% dropping or starting a fork recoverer as appropriate.
process_new_block(_StateIn, NewGS, NewB, _, _Peer, not_joined) ->
	ar_join:start(ar_gossip:peers(NewGS, NewB)),
	none;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, unavailable, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	% This block is at the correct height.
	RecallHash = find_recall_hash(NewB, HashList),
	FullBlock = get_full_block(Peer, RecallHash),
	case ?IS_BLOCK(FullBlock) of
		true ->
			% TODO: Cleanup full block -> shadow generation.
			RecallShadow = FullBlock#block { txs = [
													T#tx.id
													||
													T <- FullBlock#block.txs] },
			ar_storage:write_full_block(FullBlock),
			StateNext = StateIn#{ gossip => NewGS },
			process_new_block(StateNext, NewGS, NewB, RecallShadow, Peer, HashList);
		false ->
			ar:d(failed_to_get_recall_block),
			none
	end;
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, RecallB, Peer, HashList)
		when NewB#block.height == Height + 1 ->
	% This block is at the correct height.
	StateNext = StateIn#{ gossip => NewGS },
	#{
		txs := TXs,
		waiting_txs := WaitingTXs,
		potential_txs := PotentialTXs,
		rewart_pool := RewardPool,
		wallet_list := WalletList
	} = StateNext,
	% If transaction not found in state or storage, txlist built will be
	% incomplete and will fail in validate
	TXs = lists:foldr(
		fun(T, Acc) ->
			%state contains it
			case [ TX || TX <- (TXs ++ WaitingTXs ++ PotentialTXs), TX#tx.id == T ] of
				[] ->
					case ar_storage:read_tx(T) of
						unavailable -> Acc;
						TX          -> [TX | Acc]
					end;
				[TX | _] ->
					[TX | Acc]
			end
		end,
		[],
		NewB#block.txs
	),
	{FinderReward, _} =
		ar_node:calculate_reward_pool(
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			ar_node:calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				NewB#block.height
			)
		),
	NewWalletList =
		ar_node:apply_mining_reward(
			ar_node:apply_txs(WalletList, TXs),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		),
	StateNew = StateNext#{ wallet_list => NewWalletList },
	% TODO mue: ar_node:validate() has to be moved to ar_node_worker. Also
	% check what values of state are needed. Also setting the state gossip
	% for fork_recover/3 has to be checked. The gossip is already set to
	% NewGS in first function statement. Compare to pre-refactoring.
	case ar_node:validate(
			StateNew,
			NewB,
			TXs,
			ar_util:get_head_block(HashList), RecallB
	) of
		true ->
			% The block is legit. Accept it.
			case whereis(fork_recovery_server) of
				undefined -> integrate_new_block(StateNew, NewB);
				_         -> fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
			end;
		false ->
			ar:d({could_not_validate_new_block, ar_util:encode(NewB#block.indep_hash)}),
			fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
	end;
process_new_block(# {height := Height }, NewGS, NewB, _RecallB, _Peer, _HashList)
		when NewB#block.height =< Height ->
	% Block is lower than us, ignore it.
	ar:report(
		[
			{ignoring_block_below_current, ar_util:encode(NewB#block.indep_hash)},
			{current_height, Height},
			{proposed_block_height, NewB#block.height}
		]
	),
	{ok, [{gossip, NewGS}]};
% process_new_block(S, NewGS, NewB, _RecallB, _Peer, _Hashlist)
%		when (NewB#block.height == S#state.height + 1) ->
	% Block is lower than fork recovery height, ignore it.
	% server(S#state { gossip = NewGS });
process_new_block(#{ height := Height } = StateIn, NewGS, NewB, _RecallB, Peer, _HashList)
		when (NewB#block.height > Height + 1) ->
	fork_recover(StateIn#{ gossip => NewGS }, Peer, NewB).

%% @doc Replace the entire stored block list, regenerating the hash list.
replace_block_list([Block | _]) ->
	{ok, [
		{hash_list, [Block#block.indep_hash | Block#block.hash_list]},
		{wallet_list, Block#block.wallet_list},
		{height, Block#block.height}
	 ]}.

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
fork_recover(#{ hash_list := HashList } = StateIn, Peer, NewB) ->
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
					NewB,
					HashList
				)
			),
			case PID of
				undefined -> ok;
				_         -> erlang:register(fork_recovery_server, PID)
			end;
		{undefined, _} ->
			ok;
		_ ->
			whereis(fork_recovery_server) ! {update_target_block, NewB, ar_util:unique(Peer)}
	end,
	% TODO mue: Check how an unchanged state has to be returned in
	% program flow.
	StateIn.

%% @doc We have received a new valid block. Update the node state accordingly.
integrate_new_block(
		#{
			txs := TXs,
			hash_list := HashList,
			wallet_list := WalletList,
			waiting_txs := WaitingTXs,
			potential_txs := PotentialTXs
		} = StateIn,
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
	% TODO mue: KeepNotMinedTXs is unused.
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
	% ar:d({new_hash_list, [NewB#block.indep_hash | HashList]}),
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
			find_recall_hash(NewB, [NewB#block.indep_hash | HashList])
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
	reset_miner(StateIn#{
		hash_list            => [NewB#block.indep_hash | HashList],
		txs                  => ar_track_tx_db:remove_bad_txs(KeepNotMinedTXs),
		height               => NewB#block.height,
		floating_wallet_list => ar_node:apply_txs(WalletList, TXs),
		reward_pool          => NewB#block.reward_pool,
		potential_txs        => [],
		diff                 => NewB#block.diff,
		last_retarget        => NewB#block.last_retarget,
		weave_size           => NewB#block.weave_size
	}).

%% @doc Kill the old miner, optionally start a new miner, depending on the automine setting.
reset_miner(#{ miner := undefined, automine := false } = StateIn) ->
	StateIn;
reset_miner(#{ miner := undefined, automine := true } = StateIn) ->
	start_mining(StateIn);
reset_miner(#{ miner := Pid, automine := false } = StateIn) ->
	ar_mine:stop(Pid),
	StateIn#{ miner => undefined };
reset_miner(#{ miner := Pid, automine := true } = StateIn) ->
	ar_mine:stop(Pid),
	start_mining(StateIn#{ miner => undefined }).

%% @doc Force a node to start mining, update state.
start_mining(#{hash_list := not_joined} = StateIn) ->
	% We don't have a block list. Wait until we have one before
	% starting to mine.
	StateIn;
start_mining(#{ hash_list := BHL, txs := TXs, reward_addr := RewardAddr, tags := Tags } = StateIn) ->
	case find_recall_block(BHL) of
		unavailable ->
			B = ar_storage:read_block(hd(BHL)),
			RecallHash = find_recall_hash(B, BHL),
			%FullBlock = get_encrypted_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash),
			FullBlock = get_full_block(ar_bridge:get_remote_peers(whereis(http_bridge_node)), RecallHash),
			case FullBlock of
				X when (X == unavailable) or (X == not_found) ->
					ar:report(
						[
							could_not_start_mining,
							could_not_retrieve_recall_block
						]
					);
				_ ->
					case ar_weave:verify_indep(FullBlock, BHL) of
						true ->
							ar_storage:write_full_block(FullBlock),
							ar:report(
								[
									could_not_start_mining,
									stored_recall_block_for_foreign_verification
								]
							);
						false ->
								ar:report(
									[
										could_not_start_mining,
										{received_invalid_recall_block, FullBlock#block.indep_hash}
									]
								)
					end
			end,
			StateIn;
		RecallB ->
			if not is_record(RecallB, block) ->
				ar:report_console([{erroneous_recall_block, RecallB}]);
			true ->
				ar:report([{node_starting_miner, self()}, {recall_block, RecallB#block.height}])
			end,
			RecallBFull = make_full_block(
				RecallB#block.indep_hash
			),
			ar_key_db:put(
				RecallB#block.indep_hash,
				[
					{
						ar_block:generate_block_key(RecallBFull, hd(BHL)),
						binary:part(hd(BHL), 0, 16)
					}
				]
			),
			B = ar_storage:read_block(hd(BHL)),
			Miner =
				ar_mine:start(
					B,
					RecallB,
					TXs,
					RewardAddr,
					Tags
				),
			ar:report([{node, self()}, {started_miner, Miner}]),
			StateIn#{ miner => Miner }
	end.

%% @doc Search a block list for the next recall block.
find_recall_block([Hash]) ->
	ar_storage:read_block(Hash);
find_recall_block(HashList) ->
	Block = ar_storage:read_block(hd(HashList)),
	RecallHash = find_recall_hash(Block, HashList),
	ar_storage:read_block(RecallHash).

%% @doc Return the hash of the next recall block.
find_recall_hash(Block, []) ->
	Block#block.indep_hash;
find_recall_hash(Block, HashList) ->
	lists:nth(1 + ar_weave:calculate_recall_block(Block), lists:reverse(HashList)).

%% @doc Find a block from an ordered block list.
find_block(Hash) when is_binary(Hash) ->
	ar_storage:read_block(Hash).

%% @doc Get a specific full block (a block containing full txs) via
%% blocks indep_hash.
get_full_block(Peers, ID) when is_list(Peers) ->
	% check locally first, if not found ask list of external peers for block
	case ar_storage:read_block(ID) of
		unavailable ->
			lists:foldl(
				fun(Peer, Acc) ->
					case is_atom(Acc) of
						false ->
							Acc;
						true ->
							Full = get_full_block(Peer, ID),
							case is_atom(Full) of
								true  -> Acc;
								false -> Full
							end
					end
				end,
				unavailable,
				Peers
			);
		Block ->
			case make_full_block(ID) of
				unavailable ->
					ar_storage:invalidate_block(Block),
					get_full_block(Peers, ID);
				FinalB -> FinalB
			end
	end;
get_full_block(Pid, ID) when is_pid(Pid) ->
	% Attempt to get block from local storage and add transactions.
	make_full_block(ID);
get_full_block(Host, ID) ->
	% Handle external peer request.
	ar_http_iface:get_full_block(Host, ID).

%% @doc Convert a block with tx references into a full block, that is a block
%% containing the entirety of all its referenced txs.
make_full_block(ID) ->
	case ar_storage:read_block(ID) of
		unavailable ->
			unavailable;
		BlockHeader ->
			FullB =
				BlockHeader#block{
					txs =
						ar_node:get_tx(
							whereis(http_entrypoint_node),
							BlockHeader#block.txs
						)
				},
			case [ NotTX || NotTX <- FullB#block.txs, is_atom(NotTX) ] of
				[] -> FullB;
				_  -> unavailable
			end
	end.

%%%
%%% EOF
%%%

