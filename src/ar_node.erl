%%%
%%% @doc Blockweave maintaining nodes in the Arweave system.
%%%

-module(ar_node).

-export([start_link/1]).
-export([start/0, start/1, start/2, start/3, start/4, start/5, start/6, start/7]).
-export([stop/1]).

-export([get_blocks/1, get_block/3]).
-export([get_peers/1]).
-export([get_wallet_list/1]).
-export([get_hash_list/1, get_height/1]).
-export([get_trusted_peers/1]).
-export([get_balance/2]).
-export([get_last_tx/2]).
-export([get_pending_txs/1, get_full_pending_txs/1]).
-export([get_current_diff/1, get_diff/1]).
-export([get_floating_wallet_list/1]).
-export([get_waiting_txs/1, get_all_known_txs/1]).
-export([get_current_block_hash/1, get_current_block/1]).
-export([get_reward_addr/1]).
-export([get_reward_pool/1]).
-export([is_joined/1]).

-export([mine/1, mine_at_diff/2, automine/1, truncate/1]).
-export([add_tx/2]).
-export([cancel_tx/3]).
-export([add_peers/2]).
-export([print_reward_addr/0]).

-export([set_reward_addr/2, set_reward_addr_from_file/1, generate_and_set_reward_addr/0]).
-export([set_loss_probability/2, set_delay/2, set_mining_delay/2, set_xfer_speed/2]).

-include("ar.hrl").

%%%
%%% Macros.
%%%

%% @doc Maximum number of blocks to hold at any time.
%% NOTE: This value should be greater than ?RETARGET_BLOCKS + 1
%% in order for the TNT test suite to pass.
-define(MAX_BLOCKS, ?RETARGET_BLOCKS).

%% @doc Ensure this number of the last blocks are not dropped.
-define(KEEP_LAST_BLOCKS, 5).

%% @doc The time to poll peers for a new current block.
-define(POLL_TIME, 60*100).

%%%
%%% Public API.
%%%

%% @doc Start a node, linking to a supervisor process
start_link(Args) ->
	PID = erlang:apply(ar_node, start, Args),
	{ok, PID}.

%% @doc Start a node server loop with a set of optional parameters.
% Peers: the set of PID/IP that the node communicates with
% Blocks: the initial blocks to spawn with, if none, not_joined atom
% MiningDelay: delay in mining, used primarily for network simulation
% RewardAddr: the address in which mining rewards will be attributed with
% AutoJoin: boolean stating if a node should automatically attempt to join
% Diff: starting diff of the network (?DEFAULT_DIFF)
% LastRetarget: timestamp (seconds) stating when difficulty was last changed
start() ->
	start([]).
start(Peers) ->
	start(
		Peers,
		not_joined
	).
start(Peers, Bs) ->
	start(
		Peers,
		Bs,
		0
	).
start(Peers, Bs, MiningDelay) ->
	start(
		Peers,
		Bs,
		MiningDelay,
		unclaimed
	).
start(Peers, HashList, MiningDelay, RewardAddr) ->
	start(
		Peers,
		HashList,
		MiningDelay,
		RewardAddr,
		true
	).
start(Peers, Bs = [B | _], MiningDelay, RewardAddr, AutoJoin)
		when is_record(B, block) ->
	lists:map(
		fun ar_storage:write_block/1,
		Bs
	),
	start(
		Peers,
		[B#block.indep_hash | B#block.hash_list],
		MiningDelay,
		RewardAddr,AutoJoin
	);
start(Peers, HashList, MiningDelay, RewardAddr, AutoJoin) ->
	start(
		Peers,
		HashList,
		MiningDelay,
		RewardAddr,
		AutoJoin,
		?DEFAULT_DIFF
	).
start(Peers, HashList, MiningDelay, RewardAddr, AutoJoin, Diff) ->
	start(
		Peers,
		HashList,
		MiningDelay,
		RewardAddr,
		AutoJoin,
		Diff,
		os:system_time(seconds)
	).
start(Peers, Bs = [B | _], MiningDelay, RewardAddr, AutoJoin, Diff, LastRetarget) when is_record(B, block) ->
	lists:map(
		fun ar_storage:write_block/1,
		Bs
	),
	start(
		Peers,
		[B#block.indep_hash | B#block.hash_list],
		MiningDelay,
		RewardAddr,
		AutoJoin,
		Diff,
		LastRetarget
	);
start(Peers, B, MiningDelay, RewardAddr, AutoJoin, Diff, LastRetarget) when ?IS_BLOCK(B) ->
	start(Peers, B#block.hash_list, MiningDelay, RewardAddr, AutoJoin, Diff, LastRetarget);
start(Peers, HashList, MiningDelay, RewardAddr, AutoJoin, Diff, LastRetarget) ->
	% Spawns the node server process.
	PID = spawn(
		fun() ->
			% Join the node to the network.
			case {HashList, AutoJoin} of
				{not_joined, true} ->
					ar_join:start(self(), Peers);
				_ ->
					do_nothing
			end,
			Gossip =
				ar_gossip:init(
					lists:filter(
						fun is_pid/1,
						Peers
					)
				),
			Wallets = ar_util:wallets_from_hashes(HashList),
			Height = ar_util:height_from_hashes(HashList),
			RewardPool =
				case HashList of
					not_joined -> 0;
					[H | _] -> (ar_storage:read_block(H, HashList))#block.reward_pool
				end,
			WeaveSize =
					case HashList of
						not_joined -> 0;
						[H2 | _] -> (ar_storage:read_block(H2, HashList))#block.weave_size
					end,
			Current =
				case HashList of
					not_joined -> not_joined;
					[C|_] -> C
				end,
			% Start processes, init state, and start server.
			NPid = self(),
			{ok, SPid} = ar_node_state:start(),
			{ok, WPid} = ar_node_worker:start(NPid, SPid),

			ok = ar_node_state:update(SPid, [
				{node, NPid},
				{gossip, Gossip},
				{hash_list, HashList},
				{current, Current},
				{wallet_list, Wallets},
				{floating_wallet_list, Wallets},
				{mining_delay, MiningDelay},
				{reward_addr, RewardAddr},
				{reward_pool, RewardPool},
				{height, Height},
				{trusted_peers, Peers},
				{diff, Diff},
				{last_retarget, LastRetarget},
				{weave_size, WeaveSize}
			]),

			server(SPid, WPid, queue:new())
		end
	),
	ar_http_iface_server:reregister(http_entrypoint_node, PID),
	PID.

%% @doc Stop a node server loop and its subprocesses.
stop(Node) ->
	Node ! stop,
	ok.

%% @doc Get the current top block.
%% If found the result will be a block with tx references, not a full block.
% TODO: standardize return, unavailable | not_found.
get_current_block(Peers) when is_list(Peers) ->
	% ask list of external peers for block
	lists:foldl(
		fun(Peer, Acc) ->
			case is_atom(Acc) of
				false -> Acc;
				true ->
					B = get_current_block(Peer),
					case is_atom(B) of
						true -> Acc;
						false -> B
					end
			end
		end,
		unavailable,
		Peers
	);
get_current_block(Peer) when is_pid(Peer) ->
	Ref = make_ref(),
	Peer ! {get_current_block, self(), Ref},
	receive
		{Ref, block, CurrentBlock} -> CurrentBlock
	after ?LOCAL_NET_TIMEOUT ->
		not_found
	end;
get_current_block(Peer) ->
	% handle external peer request
	ar_http_iface_client:get_current_block(Peer).

%% @doc Return the entire hashlist from a node.
% TODO: Change references to hashlist, not blocklist.
% Code duplication against get_hashlist function.
get_blocks(Node) ->
	Ref = make_ref(),
	Node ! {get_blocks, self(), Ref},
	receive
		{Ref, blocks, Node, Bs} -> Bs
	after ?LOCAL_NET_TIMEOUT ->
		not_found
	end.

%% @doc Get a specific block via blocks indep_hash.
%% If found the result will be a block with tx references, not a full block.
get_block(Peers, ID, BHL) when is_list(Peers) ->
	% ask list of external peers for block
	% ar:d([{getting_block, ar_util:encode(ID)}, {peers, Peers}]),
	case ar_storage:read_block(ID, BHL) of
		unavailable ->
			lists:foldl(
				fun(Peer, Acc) ->
					case is_atom(Acc) of
						false -> Acc;
						true ->
							B = get_block(Peer, ID, BHL),
							case is_atom(B) of
								true -> Acc;
								false -> B
							end
					end
				end,
				unavailable,
				Peers
			);
		Block -> Block
	end;
get_block(Proc, ID, BHL) when is_pid(Proc) ->
	% attempt to get block from nodes local storage
	ar_storage:read_block(ID, BHL);
get_block(Host, ID, BHL) ->
	% handle external peer request
	ar_http_iface_client:get_block(Host, ID, BHL).

%% @doc Convert a block with tx references into a full block, that is a block
%% containing the entirety of all its referenced txs.
% make_full_block(ID, BHL) ->
% 	case ar_storage:read_block(ID, BHL) of
% 		unavailable -> unavailable;
% 		BlockHeader ->
% 			FullB =
% 				BlockHeader#block{
% 					txs =
% 						ar_storage:read_tx(BlockHeader#block.txs)
% 				},
% 			case [ NotTX || NotTX <- FullB#block.txs, is_atom(NotTX) ] of
% 				[] -> FullB;
% 				_ -> unavailable
% 			end
% 	end.

%% @doc Gets the set of all known txs from the node.
%% This set includes those on timeout waiting to distribute around the
%% network, the potentially valid txs as well as those being mined on.
get_all_known_txs(Node) ->
	Ref = make_ref(),
	Node ! {get_all_known_txs, self(), Ref},
	receive
		{Ref, all_known_txs, TXs} -> TXs
		after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Get the set of trusted peers.
%% The set of trusted peers is that in whcih where joined on.
get_trusted_peers(Proc) when is_pid(Proc) ->
	Ref = make_ref(),
	Proc ! {get_trusted_peers, self(), Ref},
	receive
		{Ref, peers, Ps} -> Ps
		after ?LOCAL_NET_TIMEOUT -> []
	end;
get_trusted_peers(_) ->
	unavailable.

%% @doc Get the list of peers from the nodes gossip state.
%% This is the list of peers that node will request blocks/txs from and will
%% distribute its mined blocks to.
get_peers(Proc) when is_pid(Proc) ->
	Ref = make_ref(),
	Proc ! {get_peers, self(), Ref},
	receive
		{Ref, peers, Ps} -> Ps
		after ?LOCAL_NET_TIMEOUT -> []
	end;
get_peers(Host) ->
	case ar_http_iface_client:get_peers(Host) of
		unavailable -> [];
		Peers -> Peers
	end.

%% @doc Get the current wallet list from the node.
%% This wallet list is up to date to the latest block held.
get_wallet_list(Node) ->
	Ref = make_ref(),
	Node ! {get_walletlist, self(), Ref},
	receive
		{Ref, walletlist, WalletList} -> WalletList
		after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Get the current waiting tx list from a node.
get_waiting_txs(Node) ->
	Ref = make_ref(),
	Node ! {get_waiting_txs, self(), Ref},
	receive
		{Ref, waiting_txs, Waiting} -> Waiting
		after ?LOCAL_NET_TIMEOUT -> error(could_not_get_waiting_txs)
	end.

%% @doc Get the current hash list held by the node.
%% This hash list is up to date to the latest block held.
get_hash_list(IP) when not is_pid(IP) ->
	ar_http_iface_client:get_hash_list(IP);
get_hash_list(Node) ->
	Ref = make_ref(),
	Node ! {get_hashlist, self(), Ref},
	receive
		{Ref, hashlist, not_joined} -> [];
		{Ref, hashlist, HashList} -> HashList
		after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Get the current block hash.
get_current_block_hash(Node) ->
	Ref = make_ref(),
	Node ! {get_current_block_hash, self(), Ref},
	receive
		{Ref, current_block_hash, not_joined} -> not_joined;
		{Ref, current_block_hash, Current} -> Current
		after ?LOCAL_NET_TIMEOUT -> unavailable
	end.

%% @doc Return the current height of the blockweave.
get_height(Node) ->
	Ref = make_ref(),
	Node ! {get_height, self(), Ref},
	receive
		{Ref, height, H} -> H
	after ?LOCAL_NET_TIMEOUT -> -1
	end.

%% @doc Check whether self node has joined the weave.
%% Uses hashlist value not_joined as witness.
is_joined(Node) ->
	Ref = make_ref(),
	Node ! {get_hashlist, self(), Ref},
	receive
		{Ref, hashlist, not_joined} -> false;
		{Ref, hashlist, _} -> true
	end.

%% @doc Get the current balance of a given wallet address.
%% The balance returned is in relation to the nodes current wallet list.
get_balance(Node, Addr) when ?IS_ADDR(Addr) ->
	Ref = make_ref(),
	Node ! {get_balance, self(), Ref, Addr},
	receive
		{Ref, balance, Addr, B} -> B
		after ?LOCAL_NET_TIMEOUT -> node_unavailable
	end;
get_balance(Node, WalletID) ->
	get_balance(Node, ar_wallet:to_address(WalletID)).

%% @doc Get the last tx id associated with a given wallet address.
%% Should the wallet not have made a tx the empty binary will be returned.
get_last_tx(Node, Addr) when ?IS_ADDR(Addr) ->
	Ref = make_ref(),
	Node ! {get_last_tx, self(), Ref, Addr},
	receive
		{Ref, last_tx, Addr, LastTX} ->
			{ok, LastTX}
	after ?LOCAL_NET_TIMEOUT ->
		timeout
	end;
get_last_tx(Node, WalletID) ->
	get_last_tx(Node, ar_wallet:to_address(WalletID)).

%% @doc Returns a list of pending transactions.
%% Pending transactions are those that are valid, but not currently actively
%% being mined as they are waiting to be distributed around the network.
get_pending_txs(Node) ->
	Ref = make_ref(),
	Node ! {get_txs, self(), Ref},
	receive
		{Ref, all_txs, Txs} -> [T#tx.id || T <- Txs]
		after ?LOCAL_NET_TIMEOUT -> []
	end.
get_full_pending_txs(Node) ->
	Ref = make_ref(),
	Node ! {get_txs, self(), Ref},
	receive
		{Ref, all_txs, Txs} -> Txs
		after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Returns the floating wallet list held by the node.
%% The floating wallet list is the current wallet list with the txs being
%% mined on applied to it.
get_floating_wallet_list(Node) ->
	Ref = make_ref(),
	Node ! {get_floatingwalletlist, self(), Ref},
	receive
		{Ref, floatingwalletlist, WalletList} -> WalletList
		after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Returns the new difficulty of next mined block.
% TODO: Function name is confusing, returns the new difficulty being mined on,
% not the 'current' diff (that of the latest block)
get_current_diff(Node) ->
	Ref = make_ref(),
	Node ! {get_current_diff, self(), Ref},
	receive
		{Ref, current_diff, Diff} -> Diff
		after ?LOCAL_NET_TIMEOUT -> 1
	end.

%% @doc Returns the difficulty of the last successfully mined block.
%% Returns the difficulty of the current block (not of that being mined).
get_diff(Node) ->
	Ref = make_ref(),
	Node ! {get_diff, self(), Ref},
	receive
		{Ref, diff, Diff} -> Diff
		after ?LOCAL_NET_TIMEOUT -> 1
	end.

%% @doc Get the current rewardpool from the node.
get_reward_pool(Node) ->
	Ref = make_ref(),
	Node ! {get_reward_pool, self(), Ref},
	receive
		{Ref, reward_pool, RewardPool} -> RewardPool
		after ?LOCAL_NET_TIMEOUT -> 0
	end.

%% @doc Get the reward address attributed to the node.
%% This is the wallet address that should the node successfully mine a block
%% the reward will be credited to.
get_reward_addr(Node) ->
	Ref = make_ref(),
	Node ! {get_reward_addr, self(), Ref},
	receive
		{Ref, reward_addr, Addr} -> Addr
	after ?LOCAL_NET_TIMEOUT -> 0
	end.

%% @doc Set the reward address of the node.
%% This is the address mining rewards will be credited to.
set_reward_addr(Node, Addr) ->
	Node ! {set_reward_addr, Addr}.

%% @doc Set the reward address of the node from an Arweave keyfile.
%% This is the address mining rewards will be credited to.
set_reward_addr_from_file(Filepath) ->
	{_Priv, Pub} = ar_wallet:load(Filepath),
	set_reward_addr(whereis(http_entrypoint_node), ar_wallet:to_address(Pub)),
	ar:report(
		[
			{new_reward_address, ar_wallet:to_address(Pub)}
		]
	).

%% @doc Generate a new keyfile and set the reward address of the node to the
%% wallets address.
%% This is the address mining rewards wiwll be credited to.
generate_and_set_reward_addr() ->
	{_Priv, Pub} = ar_wallet:new(),
	set_reward_addr(whereis(http_entrypoint_node), ar_wallet:to_address(Pub)),
	ar:report(
		[
			{new_reward_address, ar_wallet:to_address(Pub)}
		]
	).

%% @doc Pretty print the reward address of the node.
print_reward_addr() ->
	ar_util:encode(get_reward_addr(whereis(http_entrypoint_node))).

%% @doc Trigger a node to start mining a block.
mine(Node) ->
	Node ! mine.

%% @doc Trigger a node to start mining a block at a certain difficulty. This is
%% not used in the original/upstream git repo, but supposedly partners are using
%% it in their forks.
mine_at_diff(Node, Diff) ->
	Node ! {mine_at_diff, Diff}.

%% @doc Trigger a node to mine continually.
automine(Node) ->
	Node ! automine.

%% @doc Cause a node to forget all but the latest block.
%% Used primarily for testing, simulating newly joined node.
truncate(Node) ->
	Node ! truncate.

%% @doc Set the likelihood that a message will be dropped in transmission.
%% Used primarily for testing, simulating packet loss.
set_loss_probability(Node, Prob) ->
	Node ! {set_loss_probability, Prob}.

%% @doc Set the max network latency delay for a node.
%% Used primarily for testing, simulating transmission delays.
set_delay(Node, MaxDelay) ->
	Node ! {set_delay, MaxDelay}.

%% @doc Set the number of milliseconds to wait between hashes.
%% Used primarily for testing, simulating lower hasing power machine.
set_mining_delay(Node, Delay) ->
	Node ! {set_mining_delay, Delay}.

%% @doc Set the number of bytes the node can transfer in a second.
%% Used primarily for testing, simulating node connection strengths.
set_xfer_speed(Node, Speed) ->
	Node ! {set_xfer_speed, Speed}.

%% @doc Add a transaction to the node server loop.
%% If accepted the tx will enter the waiting pool before being mined into the
%% the next block.
%% If the tx contradicts another in the tx mining pool it will be moved to
%% the list of potential txs for potential foreign block verification.
add_tx(GS, TX) when is_record(GS, gs_state) ->
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	NewGS;
add_tx(Node, TX) when is_pid(Node) ->
	Node ! {add_tx, TX},
	ok;
add_tx(Host, TX) ->
	ar_http_iface_client:send_new_tx(Host, TX).

%% @doc remove a TX from the waiting queues, with permission from the owner.
cancel_tx(Node, TXID, Sig) ->
	Node ! {cancel_tx, TXID, Sig}.

%% @doc Request to add a list of peers to the node server loop.
add_peers(Node, Peer) when not is_list(Peer) ->
	add_peers(Node, [Peer]);
add_peers(Node, Peers) ->
	%ar:d([{node, self()}, {requesting_add_peers, Peers}]),
	Node ! {add_peers, Peers},
	ok.

%%%
%%% Server functions.
%%%

%% @doc Main server loop.
server(SPid, WPid, TaskQueue) ->
	receive
		stop ->
			% Stop the node server. First handle all open tasks
			% in the queue synchronously.
			% TODO mue: Possible race condition if worker is
			% currently processing one task! Also check order.
			{ok, Miner} = ar_node_state:lookup(SPid, miner),
			lists:foreach(fun(Task) ->
				ar_node_worker:call(WPid, Task)
			end, queue:to_list(TaskQueue)),
			case Miner of
				undefined -> do_nothing;
				PID		  -> ar_mine:stop(PID)
			end,
			ar_node_worker:stop(WPid),
			ar_node_state:stop(SPid),
			ok;
		{worker, Response} ->
			% Worker finished a task w/o errors.
			case Response of
				{error, Error} ->
					ar:err([{node_worker_error, {error, Error}}]);
				{ok, _} ->
					noop
			end,
			case queue:out(TaskQueue) of
				{empty, TaskQueue} ->
					% Empty queue, nothing to cast.
					server(SPid, WPid, TaskQueue);
				{{value, Task}, NewTaskQueue} ->
					% At least one task in queue, cast it to worker.
					ar_node_worker:cast(WPid, Task),
					server(SPid, WPid, NewTaskQueue)
			end;
		Msg ->
			try handle(SPid, Msg) of
				{task, Task} ->
					% Handler returns worker task to do.
					case queue:is_empty(TaskQueue) of
						true ->
							% Queue is empty, directly cast task to worker.
							ar_node_worker:cast(WPid, Task),
							server(SPid, WPid, TaskQueue);
						false ->
							% Queue contains tasks, so add it.
							NewTaskQueue = queue:in(Task, TaskQueue),
							server(SPid, WPid, NewTaskQueue)
					end;
				ok ->
					% Handler is fine.
					server(SPid, WPid, TaskQueue)
			catch
				throw:Term ->
					ar:report([ {'NodeEXCEPTION', Term} ]),
					server(SPid, WPid, TaskQueue);
				exit:Term ->
					ar:report([ {'NodeEXIT', Term} ]),
					server(SPid, WPid, TaskQueue);
				error:Term ->
					ar:report([ {'NodeERROR', {Term, erlang:get_stacktrace()}} ]),
					server(SPid, WPid, TaskQueue)
			end
	end.

%% @doc Handle the server messages. Returns {task, Task} or ok. First block
%% countains the state changing handler, second block the reading handlers.
handle(_SPid, Msg) when is_record(Msg, gs_msg) ->
	% We have received a gossip mesage. Gossip state manipulation
	% is always a worker task.
	{task, {gossip_message, Msg}};
handle(_SPid, {add_tx, TX}) ->
	{task, {add_tx, TX}};
handle(_SPid, {cancel_tx, TXID, Sig}) ->
	{task, {cancel_tx, TXID, Sig}};
handle(_SPid, {add_peers, Peers}) ->
	{task, {add_peers, Peers}};
handle(_SPid, {add_tx_to_mining_pool, TX}) ->
	{task, {add_tx_to_mining_pool, TX}};
handle(SPid, {update_floating_wallet_list, TX}) ->
	{ok, FloatingWalletList} = ar_node_state:lookup(SPid, floating_wallet_list),
	NewFloatingWalletList = ar_node_utils:apply_tx(FloatingWalletList, TX),
	ar_node_state:update(SPid, [{floating_wallet_list, NewFloatingWalletList}]),
	ar:info("Updating ~p to the floating wallet list", [TX#tx.id]);
handle(_SPid, {new_block, Peer, Height, NewB, BDS, Recall}) ->
	{task, {process_new_block, Peer, Height, NewB, BDS, Recall}};
handle(_SPid, {replace_block_list, NewBL}) ->
	% Replace the entire stored block list, regenerating the hash list.
	{task, {replace_block_list, NewBL}};
handle(_SPid, {set_delay, MaxDelay}) ->
	{task, {set_delay, MaxDelay}};
handle(_SPid, {set_loss_probability, Prob}) ->
	{task, {set_loss_probability, Prob}};
handle(_SPid, {set_mining_delay, Delay}) ->
	{task, {set_mining_delay, Delay}};
handle(_SPid, {set_reward_addr, Addr}) ->
	{task, {set_reward_addr, Addr}};
handle(_SPid, {set_xfer_speed, Speed}) ->
	{task, {set_xfer_speed, Speed}};
handle(SPid, {work_complete, MinedTXs, _Hash, Diff, Nonce, Timestamp}) ->
	% The miner thinks it has found a new block.
	{ok, HashList} = ar_node_state:lookup(SPid, hash_list),
	case HashList of
		not_joined ->
			ok;
		_ ->
			{task, {
				work_complete,
				MinedTXs,
				Diff,
				Nonce,
				Timestamp
			}}
	end;
handle(_SPid, {fork_recovered, NewHs}) ->
	{task, {fork_recovered, NewHs}};
handle(_SPid, mine) ->
	{task, mine};
handle(_SPid, {mine_at_diff, Diff}) ->
	{task, {mine_at_diff, Diff}};
handle(_SPid, automine) ->
	{task, automine};
%% ----- Getters and non-state-changing actions. -----
handle(SPid, {get_current_block, From, Ref}) ->
	{ok, HashList} = ar_node_state:lookup(SPid, hash_list),
	From ! {Ref, block, ar_util:get_head_block(HashList)},
	ok;
handle(SPid, {get_blocks, From, Ref}) ->
	{ok, HashList} = ar_node_state:lookup(SPid, hash_list),
	From ! {Ref, blocks, self(), HashList},
	ok;
handle(SPid, {get_block, From, Ref}) ->
	{ok, HashList} = ar_node_state:lookup(SPid, hash_list),
	From ! {Ref, block, self(), ar_node_utils:find_block(HashList)},
	ok;
handle(SPid, {get_peers, From, Ref}) ->
	{ok, GS} = ar_node_state:lookup(SPid, gossip),
	From ! {Ref, peers, ar_gossip:peers(GS)},
	ok;
handle(SPid, {get_trusted_peers, From, Ref}) ->
	{ok, TrustedPeers} = ar_node_state:lookup(SPid, trusted_peers),
	From ! {Ref, peers, TrustedPeers},
	ok;
handle(SPid, {get_walletlist, From, Ref}) ->
	{ok, WalletList} = ar_node_state:lookup(SPid, wallet_list),
	From ! {Ref, walletlist, WalletList},
	ok;
handle(SPid, {get_hashlist, From, Ref}) ->
	{ok, HashList} = ar_node_state:lookup(SPid, hash_list),
	From ! {Ref, hashlist, HashList},
	ok;
handle(SPid, {get_current_block_hash, From, Ref}) ->
	{ok, Res} = ar_node_state:lookup(SPid, current),
	From ! {Ref, current_block_hash, Res},
	ok;
handle(SPid, {get_height, From, Ref}) ->
	{ok, Height} = ar_node_state:lookup(SPid, height),
	From ! {Ref, height, Height},
	ok;
handle(SPid, {get_balance, From, Ref, WalletID}) ->
	{ok, WalletList} = ar_node_state:lookup(SPid, wallet_list),
	From ! {Ref, balance, WalletID,
		case lists:keyfind(WalletID, 1, WalletList) of
			{WalletID, Balance, _Last} -> Balance;
			false					   -> 0
		end},
	ok;
handle(SPid, {get_last_tx, From, Ref, Addr}) ->
	{ok, WalletList} = ar_node_state:lookup(SPid, wallet_list),
	From ! {Ref, last_tx, Addr,
		case lists:keyfind(Addr, 1, WalletList) of
			{Addr, _Balance, Last} -> Last;
			false				   -> <<>>
		end},
	ok;
handle(SPid, {get_txs, From, Ref}) ->
	{ok, #{ txs := TXs, waiting_txs := WaitingTXs }} = ar_node_state:lookup(SPid, [txs, waiting_txs]),
	From ! {Ref, all_txs, TXs ++ WaitingTXs},
	ok;
handle(SPid, {get_waiting_txs, From, Ref}) ->
	{ok, WaitingTXs} = ar_node_state:lookup(SPid, waiting_txs),
	From ! {Ref, waiting_txs, WaitingTXs},
	ok;
handle(SPid, {get_all_known_txs, From, Ref}) ->
	{ok, #{
		txs           := TXs,
		waiting_txs   := WaitingTXs,
		potential_txs := PotentialTXs
	}} = ar_node_state:lookup(SPid, [txs, waiting_txs, potential_txs]),
	AllTXs = TXs ++ WaitingTXs ++ PotentialTXs,
	From ! {Ref, all_known_txs, AllTXs},
	ok;
handle(SPid, {get_floatingwalletlist, From, Ref}) ->
	{ok, FloatingWalletList} = ar_node_state:lookup(SPid, floating_wallet_list),
	From ! {Ref, floatingwalletlist, FloatingWalletList},
	ok;
handle(SPid, {get_current_diff, From, Ref}) ->
	{ok, #{
		height        := Height,
		diff          := Diff,
		last_retarget := LastRetarget
	}} = ar_node_state:lookup(SPid, [height, diff, last_retarget]),
	From ! {
		Ref,
		current_diff,
		ar_retarget:maybe_retarget(
			Height + 1,
			Diff,
			os:system_time(seconds),
			LastRetarget
		)
	},
	ok;
handle(SPid, {get_diff, From, Ref}) ->
	{ok, Diff} = ar_node_state:lookup(SPid, diff),
	From ! {Ref, diff, Diff},
	ok;
handle(SPid, {get_reward_pool, From, Ref}) ->
	{ok, RewardPool} = ar_node_state:lookup(SPid, reward_pool),
	From ! {Ref, reward_pool, RewardPool},
	ok;
handle(SPid, {get_reward_addr, From, Ref}) ->
	{ok, RewardAddr} = ar_node_state:lookup(SPid, reward_addr),
	From ! {Ref, reward_addr,RewardAddr},
	ok;
%% ----- Server handling. -----
handle(_SPid, {'DOWN', _, _, _, _}) ->
	% Ignore DOWN message.
	ok;
handle(_Spid, {ar_node_state, _, _}) ->
	%% When an ar_node_state call times out its message may leak here. It can be huge so we avoid logging it.
	ok;
handle(_SPid, UnhandledMsg) ->
	ar:warn([ar_node, received_unknown_message, {message, UnhandledMsg}]),
	ok.

%%%
%%% Deprecated or unused.
%%%

%% @doc Get a specific encrypted block via the blocks indep_hash.
%% If the block is found locally an unencrypted block will be returned.
% get_encrypted_block(Peers, ID, BHL) when is_list(Peers) ->
% 	% check locally first, if not found ask list of external peers for
% 	% encrypted block
% 	case ar_storage:read_block(ID, BHL) of
% 		unavailable ->
% 			lists:foldl(
% 				fun(Peer, Acc) ->
% 					case is_atom(Acc) of
% 						false -> Acc;
% 						true ->
% 							B = get_encrypted_block(Peer, ID, BHL),
% 							case is_atom(B) of
% 								true -> Acc;
% 								false -> B
% 							end
% 					end
% 				end,
% 				unavailable,
% 				Peers
% 			);
% 		Block -> Block
% 	end;
% get_encrypted_block(Proc, ID, BHL) when is_pid(Proc) ->
% 	% attempt to get block from local storage
% 	% NB: if found block returned will not be encrypted
% 	ar_storage:read_block(ID, BHL);
% get_encrypted_block(Host, ID, BHL) ->
% 	% handle external peer request
% 	ar_http_iface_client:get_encrypted_block(Host, ID, BHL).

%% @doc Get a specific encrypted full block (a block containing full txs) via
%% the blocks indep_hash.
%% If the block is found locally an unencrypted block will be returned.
% get_encrypted_full_block(Peers, ID, BHL) when is_list(Peers) ->
% 	% check locally first, if not found ask list of external peers for
% 	% encrypted block
% 	case ar_storage:read_block(ID, BHL) of
% 		unavailable ->
% 			lists:foldl(
% 				fun(Peer, Acc) ->
% 					case is_atom(Acc) of
% 						false -> Acc;
% 						true ->
% 							Full = get_encrypted_full_block(Peer, ID, BHL),
% 							case is_atom(Full) of
% 								true -> Acc;
% 								false -> Full
% 							end
% 					end
% 				end,
% 				unavailable,
% 				Peers
% 			);
% 		_Block ->
% 			% make_full_block(ID, BHL)
% 			error(block_hash_list_required_in_context)
% 	end;
% get_encrypted_full_block(Proc, ID, BHL) when is_pid(Proc) ->
% 	% attempt to get block from local storage and make full
% 	% NB: if found block returned will not be encrypted
% 	make_full_block(ID, BHL);
% get_encrypted_full_block(Host, ID, BHL) ->
% 	% handle external peer request
% 	ar_http_iface_client:get_encrypted_full_block(Host, ID, BHL).

%% @doc Reattempts to find a block from a node retrying up to Count times.
%retry_block(_, _, Response, 0) ->
%	Response;
%retry_block(Host, ID, _, Count) ->
%	case get_block(Host, ID) of
%		not_found ->
%			timer:sleep(3000),
%			retry_block(Host, ID, not_found, Count-1);
%		unavailable ->
%			timer:sleep(3000),
%			retry_block(Host, ID, unavailable, Count-1);
%		B -> B
%	end.

%% @doc Reattempts to find an encrypted full block from a node retrying
%% up to Count times.
%% TODO: Nowhere used anymore.
%retry_encrypted_full_block(_, _, Response, 0) ->
%	Response;
%retry_encrypted_full_block(Host, ID, _, Count) ->
%	case get_encrypted_full_block(Host, ID) of
%		not_found ->
%			timer:sleep(3000),
%			retry_encrypted_full_block(Host, ID, not_found, Count-1);
%		unavailable ->
%			timer:sleep(3000),
%			retry_encrypted_full_block(Host, ID, unavailable, Count-1);
%		B -> B
%	end.
