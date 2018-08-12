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
handle(SPid, {work_complete, MinedTXs, _Hash, Diff, Nonce, Timestamp}, _Sender) ->
	{ok, StateIn} = ar_node_state:all(SPid),
	case integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) of
		{ok, StateOut} ->
			ar_node_state:insert(SPid, StateOut);
		none ->
			ok
	end,
	{ok, work_complete};
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
						TX			-> [TX | Acc]
					end;
				[TX | _] ->
					[TX | Acc]
			end
		end,
		[],
		NewB#block.txs
	),
	{FinderReward, _} =
		calculate_reward_pool(
			RewardPool,
			TXs,
			NewB#block.reward_addr,
			calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				NewB#block.height
			)
		),
	NewWalletList =
		ar_node:apply_mining_reward(
			apply_txs(WalletList, TXs),
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
				_		  -> fork_recover(StateNext#{ gossip => NewGS }, Peer, NewB)
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

%% @doc Verify a new block found by a miner, integrate it.
integrate_block_from_miner(#{ hash_list := not_joined }, _MinedTXs, _Diff, _Nonce, _Timestamp) ->
	none;
integrate_block_from_miner(StateIn, MinedTXs, Diff, Nonce, Timestamp) ->
	#{
		hash_list     := HashList,
		wallet_list   := RawWalletList,
		txs           := TXs,
		gossip        := GS,
		reward_addr   := RewardAddr,
		tags          := Tags,
		reward_pool   := OldPool,
		weave_size    := OldWeaveSize,
		potential_txs := PotentialTXs
	} = StateIn,
	% Calculate the new wallet list (applying TXs and mining rewards).
	RecallB = find_recall_block(HashList),
	WeaveSize = OldWeaveSize +
		lists:foldl(
			fun(TX, Acc) ->
				Acc + byte_size(TX#tx.data)
			end,
			0,
			TXs
		),
	{FinderReward, RewardPool} =
		calculate_reward_pool(
			OldPool,
			MinedTXs,
			RewardAddr,
			calculate_proportion(
				RecallB#block.block_size,
				WeaveSize,
				length(HashList)
			)
		),
	ar:report(
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
		apply_mining_reward(
			apply_txs(RawWalletList, MinedTXs),
			RewardAddr,
			FinderReward,
			length(HashList)
		),
	% Store the transactions that we know about, but were not mined in
	% this block.
	NotMinedTXs =
		lists:filter(
			fun(T) -> ar_tx:verify(T, Diff, WalletList) end,
			filter_all_out_of_order_txs(WalletList, TXs -- MinedTXs)
		),
	StateNew = StateIn#{ wallet_list => WalletList },
	% Build the block record, verify it, and gossip it to the other nodes.
	[NextB | _] = ar_weave:add(
		HashList, MinedTXs, HashList, RewardAddr, RewardPool,
		WalletList, Tags, RecallB, Diff, Nonce, Timestamp),
	case validate(StateNew, NextB, MinedTXs, ar_util:get_head_block(HashList), RecallB = find_recall_block(HashList)) of
		false ->
			ar:report_console(miner_produced_invalid_block),
			case rand:uniform(5) of
				1 ->
					#{ gossip := StateInGS } = StateIn,
					reset_miner(StateIn#{
						gossip		  => StateInGS,
						txs			  => [], % TXs not included in the block
						potential_txs => []
					});
				_ ->
					reset_miner(StateIn)
			end;
		true ->
			ar_storage:write_tx(MinedTXs),
			ar_storage:write_block(NextB),
			app_search:update_tag_table(NextB),
			{NewGS, _} =
				ar_gossip:send(
					GS,
					{
						new_block,
						self(),
						NextB#block.height,
						NextB,
						RecallB
					}
				),
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
					ar:report(
						{successfully_mined_tx_into_block, ar_util:encode(MinedTX#tx.id)}
					)
				end,
				MinedTXs
			),
			lists:foreach(
				fun(T) ->
					ar_tx_db:maybe_add(T#tx.id)
				end,
				PotentialTXs
			),
			reset_miner(
				StateNew#{
					gossip => NewGS,
					hash_list => [NextB#block.indep_hash | HashList],
					txs => ar_track_tx_db:remove_bad_txs(NotMinedTXs), % TXs not included in the block
					height => NextB#block.height,
					floating_wallet_list => apply_txs(WalletList, NotMinedTXs),
					reward_pool => RewardPool,
					potential_txs => [],
					diff => NextB#block.diff,
					last_retarget => NextB#block.last_retarget,
					weave_size => NextB#block.weave_size
				}
			)
	end.

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
				_		  -> erlang:register(fork_recovery_server, PID)
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
		hash_list			 => [NewB#block.indep_hash | HashList],
		txs					 => ar_track_tx_db:remove_bad_txs(KeepNotMinedTXs),
		height				 => NewB#block.height,
		floating_wallet_list => ar_node:apply_txs(WalletList, TXs),
		reward_pool			 => NewB#block.reward_pool,
		potential_txs		 => [],
		diff				 => NewB#block.diff,
		last_retarget		 => NewB#block.last_retarget,
		weave_size			 => NewB#block.weave_size
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

%% @doc Update a wallet list with a set of new transactions.
apply_txs(WalletList, TXs) ->
	lists:sort(
		lists:foldl(
			fun(TX, CurrWalletList) ->
				apply_tx(CurrWalletList, TX)
			end,
			WalletList,
			TXs
		)
	).

%% @doc Apply a transaction to a wallet list, updating it.
%% Critically, filter empty wallets from the list after application.
apply_tx(WalletList, unavailable) ->
	WalletList;
apply_tx(WalletList, TX) ->
	filter_empty_wallets(do_apply_tx(WalletList, TX)).

%% @doc Perform the concrete application of a transaction to
%% a prefiltered wallet list.
do_apply_tx(
		WalletList,
		#tx {
			id = ID,
			owner = From,
			last_tx = Last,
			target = To,
			quantity = Qty,
			reward = Reward
		}) ->
	Addr = ar_wallet:to_address(From),
	case lists:keyfind(Addr, 1, WalletList) of
		{Addr, Balance, Last} ->
			NewWalletList = lists:keyreplace(Addr, 1, WalletList, {Addr, Balance - (Qty + Reward), ID}),
			case lists:keyfind(To, 1, NewWalletList) of
				false ->
					[{To, Qty, <<>>} | NewWalletList];
				{To, OldBalance, LastTX} ->
					lists:keyreplace(To, 1, NewWalletList, {To, OldBalance + Qty, LastTX})
			end;
		_ ->
			WalletList
	end.

%% @doc Takes a wallet list and a set of txs and checks to ensure that the
%% txs can be applied in a given order. The output is the set of all txs
%% that could be applied.
filter_all_out_of_order_txs(WalletList, InTXs) ->
	filter_all_out_of_order_txs(
		WalletList,
		InTXs,
		[]
	).
filter_all_out_of_order_txs(_WalletList, [], OutTXs) ->
	lists:reverse(OutTXs);
filter_all_out_of_order_txs(WalletList, InTXs, OutTXs) ->
	{FloatingWalletList, PassedTXs} =
		filter_out_of_order_txs(
			WalletList,
			InTXs,
			OutTXs
		),
	RemainingInTXs = InTXs -- PassedTXs,
	case PassedTXs of
		[] ->
			lists:reverse(OutTXs);
		OutTXs ->
			lists:reverse(OutTXs);
		_ ->
			filter_all_out_of_order_txs(
				FloatingWalletList,
				RemainingInTXs,
				PassedTXs
			)
	end.

%% @doc Takes a wallet list and a set of txs and checks to ensure that the
%% txs can be iteratively applied. When a tx is encountered that cannot be
%% applied it is disregarded. The return is a tuple containing the output
%% wallet list and the set of applied transactions.
%% Helper function for 'filter_all_out_of_order_txs'.
filter_out_of_order_txs(WalletList, InTXs) ->
	filter_out_of_order_txs(
		WalletList,
		InTXs,
		[]
	).
filter_out_of_order_txs(WalletList, [], OutTXs) ->
	{WalletList, OutTXs};
filter_out_of_order_txs(WalletList, [T | RawTXs], OutTXs) ->
	case ar_tx:check_last_tx(WalletList, T) of
		true ->
			UpdatedWalletList = apply_tx(WalletList, T),
			filter_out_of_order_txs(
				UpdatedWalletList,
				RawTXs,
				[T | OutTXs]
			);
		false ->
			filter_out_of_order_txs(
				WalletList,
				RawTXs,
				OutTXs
			)
	end.

%% @doc Remove wallets with zero balance from a wallet list.
filter_empty_wallets([]) ->
	[];
filter_empty_wallets([{_, 0, <<>>} | WalletList]) ->
	filter_empty_wallets(WalletList);
filter_empty_wallets([Wallet | Rest]) ->
	[Wallet | filter_empty_wallets(Rest)].

%% @doc Calculate the total mining reward for the a block and it's associated TXs.
calculate_reward(Height, Quantity) ->
	erlang:trunc(calculate_static_reward(Height) + Quantity).

%% @doc Calculate the static reward received for mining a given block.
%% This reward portion depends only on block height, not the number of transactions.
calculate_static_reward(Height) when Height =< ?REWARD_DELAY->
	1;
calculate_static_reward(Height) ->
	?AR((0.2 * ?GENESIS_TOKENS * math:pow(2,-(Height-?REWARD_DELAY)/?BLOCK_PER_YEAR) * math:log(2))/?BLOCK_PER_YEAR).

%% @doc Calculate the reward.
calculate_reward_pool(OldPool, TXs, unclaimed, _Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	{0, Pool};
calculate_reward_pool(OldPool, TXs, _RewardAddr, Proportion) ->
	Pool = OldPool + lists:sum(
		lists:map(
			fun calculate_tx_reward/1,
			TXs
		)
	),
	FinderReward = erlang:trunc(Pool * Proportion),
	{FinderReward, Pool - FinderReward}.

%% @doc Given a TX, calculate an appropriate reward.
calculate_tx_reward(#tx { reward = Reward }) ->
	% TDOD mue: Calculation is not calculated, only returned.
	Reward.

%% @doc Calculates the portion of the rewardpool that the miner is entitled
%% to for mining a block with a given recall. The proportion is based on the
%% size of the recall block and the average data stored within the weave.
calculate_proportion(RecallSize, WeaveSize, Height) when (Height == 0)->
	% Genesis block.
	calculate_proportion(
		RecallSize,
		WeaveSize,
		1
	);
calculate_proportion(RecallSize, WeaveSize, Height) when (WeaveSize == 0)->
	% No data stored in the weave.
	calculate_proportion(
		RecallSize,
		1,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize >= (WeaveSize/Height) ->
	% Recall size is larger than the average data stored per block.
	XRaw = ((Height * RecallSize) / WeaveSize) - 1,
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X) / (math:pow(2, X) + 2))
	);
calculate_proportion(RecallSize, WeaveSize, Height) when RecallSize == 0 ->
	% Recall block has no data txs, hence size of zero.
	calculate_proportion(
		1,
		WeaveSize,
		Height
	);
calculate_proportion(RecallSize, WeaveSize, Height) ->
	% Standard recall block, 0 < Recall size < Average block.
	XRaw = -(((Height * WeaveSize) / RecallSize) -1),
	X = min(XRaw, 1023),
	max(
		0.1,
		(math:pow(2, X)/(math:pow(2, X) + 2))
	).

%% @doc Calculate and apply mining reward quantities to a wallet list.
apply_mining_reward(WalletList, unclaimed, _Quantity, _Height) ->
	WalletList;
apply_mining_reward(WalletList, RewardAddr, Quantity, Height) ->
	alter_wallet(WalletList, RewardAddr, calculate_reward(Height, Quantity)).

%%%
%%% EOF
%%%

