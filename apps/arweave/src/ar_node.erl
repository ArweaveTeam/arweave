-module(ar_node).

-export([
	start_link/1, start/7,
	stop/1,
	get_blocks/1,
	get_block_index/1, is_in_block_index/2, get_height/1,
	get_trusted_peers/1, set_trusted_peers/2,
	get_balance/2,
	get_last_tx/2,
	get_wallets/2,
	get_wallet_list_chunk/3,
	get_current_diff/1, get_diff/1,
	get_pending_txs/1, get_pending_txs/2, get_mined_txs/1, is_a_pending_tx/2,
	get_current_block_hash/1,
	is_joined/1,
	get_block_txs_pairs/1,
	mine/1, automine/1,
	add_tx/2,
	add_peers/2,
	set_reward_addr/2,
	set_loss_probability/2,
	get_mempool_size/1
]).

-include("ar.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start a node, linking to a supervisor process.
start_link(Args) ->
	PID = erlang:apply(ar_node, start, Args),
	{ok, PID}.

%% @doc Start a node server.
start(Peers, BI, MiningDelay, RewardAddr, AutoJoin, Diff, LastRetarget) ->
	PID = spawn_link(
		fun() ->
			case {BI, AutoJoin} of
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
			Height = ar_util:height_from_hashes(BI),
			{RewardPool, WeaveSize, Current, WalletList} =
				case BI of
					not_joined ->
						{0, 0, not_joined, not_set};
					[{H, _, _} | _] ->
						B = ar_storage:read_block(H),
						RecentBlockIndex = lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT),
						{ok, _} =
							ar_wallets:start_link([
								{recent_block_index, RecentBlockIndex},
								{peers, Peers}
							]),
						{B#block.reward_pool, B#block.weave_size, H, B#block.wallet_list}
				end,
			%% Start processes, init state, and start server.
			NPid = self(),
			%% The message queue of this process may grow big under load.
			%% The flag makes VM store messages off heap and do not perform
			%% expensive GC on them.
			process_flag(message_queue_data, off_heap),
			process_flag(trap_exit, true),
			{TXs, MempoolSize} =
				case ar_storage:read_term(mempool) of
					{ok, Mempool} ->
						Mempool;
					not_found ->
						{#{}, {0, 0}};
					{error, Error} ->
						ar:err([{event, failed_to_load_mempool}, {reason, Error}]),
						{#{}, {0, 0}}
				end,
			{ok, _} = ar_data_sync_sup:start_link([{node, self()}]),
			State = #{
				id => crypto:strong_rand_bytes(32),
				node => NPid,
				gossip => Gossip,
				block_index => BI,
				current => Current,
				wallet_list => WalletList,
				mining_delay => MiningDelay,
				reward_addr => RewardAddr,
				reward_pool => RewardPool,
				height => Height,
				trusted_peers => Peers,
				diff => Diff,
				cumulative_diff => 0,
				hash_list_merkle => <<>>,
				tags => [],
				miner => undefined,
				automine => false,
				last_retarget => LastRetarget,
				weave_size => WeaveSize,
				block_txs_pairs => create_block_txs_pairs(BI),
				txs => TXs,
				mempool_size => MempoolSize
			},
			{ok, WPid} = ar_node_worker:start_link(State),
			server(WPid, State)
		end
	),
	ar_http_iface_server:reregister(http_entrypoint_node, PID),
	PID.

%% @doc Stop a node server loop and its subprocesses.
stop(Node) ->
	Node ! stop,
	ok.

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_blocks(Node) ->
	get_block_index(Node).

%% @doc Get pending transactions. This includes:
%% 1. The transactions currently staying in the priority queue.
%% 2. The transactions on timeout waiting to be distributed around the network.
%% 3. The transactions ready to be and being mined.
get_pending_txs(Node) ->
	get_pending_txs(Node, []).

get_pending_txs(Node, Opts) ->
	Ref = make_ref(),
	Node ! {get_pending_txs, Opts, self(), Ref},
	receive
		{Ref, pending_txs, Reply} ->
			Reply
	end.

%% @doc Return true if a tx with the given identifier is pending.
is_a_pending_tx(Node, TXID) ->
	Ref = make_ref(),
	Node ! {is_a_pending_tx, TXID, self(), Ref},
	receive
		{Ref, is_a_pending_tx, Reply} ->
			Reply
	end.

%% @doc Get the list of mined or ready to be mined transactions.
%% The list does _not_ include transactions in the priority queue or
%% those on timeout waiting for network propagation.
get_mined_txs(Node) ->
	Ref = make_ref(),
	Node ! {get_mined_txs, self(), Ref},
	receive
		{Ref, mined_txs, TXs} ->
			TXs
	end.

%% @doc Get trusted peers.
get_trusted_peers(Node) ->
	Ref = make_ref(),
	Node ! {get_trusted_peers, self(), Ref},
	receive
		{Ref, peers, Ps} -> Ps
	end.

%% @doc Set trusted peers.
set_trusted_peers(Proc, Peers) when is_pid(Proc) ->
	Proc ! {set_trusted_peers, Peers}.

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_block_index(Node) ->
	Ref = make_ref(),
	Node ! {get_blockindex, self(), Ref},
	receive
		{Ref, blockindex, not_joined} -> [];
		{Ref, blockindex, BI} -> BI
	end.

%% @doc Return true if the given block hash is found in the block index.
is_in_block_index(Node, H) ->
	Ref = make_ref(),
	Node ! {is_in_block_index, H, self(), Ref},
	receive
		{Ref, is_in_block_index, Reply} -> Reply
	end.

%% @doc Get the current block hash.
get_current_block_hash(Node) ->
	Ref = make_ref(),
	Node ! {get_current_block_hash, self(), Ref},
	receive
		{Ref, current_block_hash, not_joined} -> not_joined;
		{Ref, current_block_hash, Current} -> Current
	end.

%% @doc Return the current height of the blockweave.
get_height(Node) ->
	Ref = make_ref(),
	Node ! {get_height, self(), Ref},
	receive
		{Ref, height, H} -> H
	end.

%% @doc Check whether the node has joined the network.
is_joined(Node) ->
	Ref = make_ref(),
	Node ! {get_current_block_hash, self(), Ref},
	receive
		{Ref, current_block_hash, not_joined} -> false;
		{Ref, current_block_hash, _} -> true
	end.

%% @doc Get the current balance of a given wallet address.
%% The balance returned is in relation to the nodes current wallet list.
get_balance(_Node, Addr) when ?IS_ADDR(Addr) ->
	ar_wallets:get_balance(Addr);
get_balance(Node, WalletID) ->
	get_balance(Node, ar_wallet:to_address(WalletID)).

%% @doc Get the last tx id associated with a given wallet address.
%% Should the wallet not have made a tx the empty binary will be returned.
get_last_tx(_Node, Addr) when ?IS_ADDR(Addr) ->
	{ok, ar_wallets:get_last_tx(Addr)};
get_last_tx(Node, WalletID) ->
	get_last_tx(Node, ar_wallet:to_address(WalletID)).

%% @doc Return a map address => {balance, last tx} for the given addresses.
get_wallets(_Node, Addresses) ->
	ar_wallets:get(Addresses).

%% @doc Return a chunk of wallets from the tree with the given root hash starting
%% from the Cursor address.
get_wallet_list_chunk(_Node, RootHash, Cursor) ->
	ar_wallets:get_chunk(RootHash, Cursor).

%% @doc Returns the estimated future difficulty of the currently mined block.
%% The function name is confusing and needs to be changed.
get_current_diff(Node) ->
	Ref = make_ref(),
	Node ! {get_current_diff, self(), Ref},
	receive
		{Ref, current_diff, Diff} -> Diff
	end.

%% @doc Returns the difficulty of the current block (the last applied one).
get_diff(Node) ->
	Ref = make_ref(),
	Node ! {get_diff, self(), Ref},
	receive
		{Ref, diff, Diff} -> Diff
	end.

%% @doc Returns transaction identifiers from the last ?MAX_TX_ANCHOR_DEPTH
%% blocks grouped by block hash.
get_block_txs_pairs(Node) ->
	Ref = make_ref(),
	Node ! {get_block_txs_pairs, self(), Ref},
	receive
		{Ref, block_txs_pairs, BlockTXPairs} -> {ok, BlockTXPairs}
	end.

%% @doc Set the reward address of the node.
%% This is the address mining rewards will be credited to.
set_reward_addr(Node, Addr) ->
	Node ! {set_reward_addr, Addr}.

%% @doc Trigger a node to start mining a block.
mine(Node) ->
	Node ! mine.

%% @doc Trigger a node to mine continually.
automine(Node) ->
	Node ! automine.

%% @doc Set the likelihood that a message will be dropped in transmission.
%% Used primarily for testing, simulating packet loss.
set_loss_probability(Node, Prob) ->
	Node ! {set_loss_probability, Prob}.

%% @doc Add a transaction to the node server loop.
%% If accepted the tx will enter the waiting pool before being mined into the
%% the next block.
add_tx(GS, TX) when is_record(GS, gs_state) ->
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	NewGS;
add_tx(Node, TX) when is_pid(Node) ->
	Node ! {add_tx, TX},
	ok;
add_tx({Node, Name} = Peer, TX) when is_atom(Node) andalso is_atom(Name) ->
	Peer ! {add_tx, TX},
	ok;
add_tx(Host, TX) ->
	ar_http_iface_client:send_new_tx(Host, TX).

%% @doc Request to add a list of peers to the node server loop.
add_peers(Node, Peer) when not is_list(Peer) ->
	add_peers(Node, [Peer]);
add_peers(Node, Peers) ->
	Node ! {add_peers, Peers},
	ok.

%% @doc Return memory pool size
get_mempool_size(Node) ->
	Ref = make_ref(),
	Node ! {get_mempool_size, self(), Ref},
	receive
		{Ref, get_mempool_size, Size} ->
			Size
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

create_block_txs_pairs(not_joined) ->
	[];
create_block_txs_pairs(BI) ->
	create_block_txs_pairs(recent_blocks, lists:sublist(BI, 2 * ?MAX_TX_ANCHOR_DEPTH)).

create_block_txs_pairs(recent_blocks, []) ->
	[];
create_block_txs_pairs(recent_blocks, [{BH, _, _} | Rest]) ->
	B = ar_storage:read_block(BH),
	TXs = ar_storage:read_tx(B#block.txs),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	[{BH, SizeTaggedTXs} | create_block_txs_pairs(Rest)].

%% @doc Main server loop.
server(WPid, #{ txs := TXs, mempool_size := {MempoolHeaderSize, MempoolDataSize} } = State) ->
	receive
		stop ->
			dump_mempool(State);
		{'EXIT', _, Reason} ->
			dump_mempool(State),
			ar:info([{event, ar_node_terminated}, {reason, Reason}]);
		{sync_mempool_tx, TXID, {TX, Status} = Value} ->
			case maps:get(TXID, TXs, not_found) of
				{ExistingTX, _Status} ->
					UpdatedTXs = maps:put(TXID, {ExistingTX, Status}, TXs),
					server(WPid, State#{ txs => UpdatedTXs });
				not_found ->
					UpdatedTXs = maps:put(TXID, Value, TXs),
					{AddHeaderSize, AddDataSize} = ar_node_worker:tx_mempool_size(TX),
					UpdatedMempoolSize =
						{MempoolHeaderSize + AddHeaderSize, MempoolDataSize + AddDataSize},
					server(WPid, State#{ txs => UpdatedTXs, mempool_size => UpdatedMempoolSize })
			end;
		{sync_dropped_mempool_txs, Map} ->
			{UpdatedTXs, UpdatedMempoolSize} =
				maps:fold(
					fun(TXID, {TX, _Status}, {MapAcc, MempoolSizeAcc} = Acc) ->
						case maps:is_key(TXID, MapAcc) of
							true ->
								{DroppedHeaderSize, DroppedDataSize} =
									ar_node_worker:tx_mempool_size(TX),
								{HeaderSize, DataSize} = MempoolSizeAcc,
								UpdatedMempoolSizeAcc =
									{HeaderSize - DroppedHeaderSize, DataSize - DroppedDataSize},
								{maps:remove(TXID, MapAcc), UpdatedMempoolSizeAcc};
							false ->
								Acc
						end
					end,
					{TXs, {MempoolHeaderSize, MempoolDataSize}},
					Map
				),
			server(WPid, State#{ txs => UpdatedTXs, mempool_size => UpdatedMempoolSize });
		{sync_reward_addr, Addr} ->
			server(WPid, State#{ reward_addr => Addr });
		{sync_trusted_peers, Peers} ->
			server(WPid, State#{ trusted_peers => Peers });
		{sync_state, NewState} ->
			server(WPid, NewState);
		Message ->
			server(WPid, handle(Message, WPid, State))
	end.

dump_mempool(#{ txs := TXs, mempool_size := MempoolSize }) ->
	case ar_storage:write_term(mempool, {TXs, MempoolSize}) of
		ok ->
			ok;
		{error, Reason} ->
			ar:err([{event, failed_to_persist_mempool}, {reason, Reason}])
	end.

handle(Msg, WPid, State) when is_record(Msg, gs_msg) ->
	%% We have received a gossip mesage. Gossip state manipulation is always a worker task.
	gen_server:cast(WPid, {gossip_message, Msg}),
	State;

handle({add_tx, TX}, WPid, State) ->
	gen_server:cast(WPid, {add_tx, TX}),
	State;

handle({add_peers, Peers}, WPid, State) ->
	gen_server:cast(WPid, {add_peers, Peers}),
	State;

handle({new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}, WPid, State) ->
	gen_server:cast(WPid, {process_new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}),
	State;

handle({set_loss_probability, Prob}, WPid, State) ->
	gen_server:cast(WPid, {set_loss_probability, Prob}),
	State;

handle({set_reward_addr, Addr}, WPid, State) ->
	gen_server:cast(WPid, {set_reward_addr, Addr}),
	State;

handle({work_complete, BaseBH, NewB, MinedTXs, BDS, POA, _HashesTried}, WPid, State) ->
	#{ block_index := BI } = State,
	case BI of
		not_joined ->
			do_not_cast;
		_ ->
			gen_server:cast(WPid, {
				work_complete,
				BaseBH,
				NewB,
				MinedTXs,
				BDS,
				POA
			})
	end,
	State;

handle({fork_recovered, BI, BlockTXPairs, BaseH, Timestamp}, WPid, State) ->
	case BaseH of
		no_base_hash ->
			#{ trusted_peers := Peers } = State,
			{ok, _} = ar_wallets:start_link([
				{recent_block_index, lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT)},
				{peers, Peers}
			]);
		_ ->
			do_nothing
	end,
	gen_server:cast(WPid, {fork_recovered, BI, BlockTXPairs, BaseH, Timestamp}),
	State;

handle(mine, WPid, State) ->
	gen_server:cast(WPid, mine),
	State;

handle(automine, WPid, State) ->
	gen_server:cast(WPid, automine),
	State;

handle({get_trusted_peers, From, Ref}, _WPid, #{ trusted_peers := TrustedPeers } = State) ->
	From ! {Ref, peers, TrustedPeers},
	State;

handle({set_trusted_peers, Peers}, WPid, State) ->
	gen_server:cast(WPid, {set_trusted_peers, Peers}),
	State;

handle({get_blockindex, From, Ref}, _WPid, #{ block_index := BI } = State) ->
	From ! {Ref, blockindex, BI},
	State;

handle({is_in_block_index, H, From, Ref}, _WPid, #{ block_index := BI } = State) ->
	Reply =
		case lists:search(fun({BH, _, _}) -> BH == H end, BI) of
			{value, _} ->
				true;
			false ->
				false
		end,
	From ! {Ref, is_in_block_index, Reply},
	State;

handle({get_current_block_hash, From, Ref}, _WPid, #{ current := H } = State) ->
	From ! {Ref, current_block_hash, H},
	State;

handle({get_height, From, Ref}, _WPid, #{ height := Height } = State) ->
	From ! {Ref, height, Height},
	State;

handle({get_pending_txs, Opts, From, Ref}, _WPid, #{ txs := TXs } = State) ->
	Reply =
		case {lists:member(as_map, Opts), lists:member(id_only, Opts)} of
			{true, false} ->
				TXs;
			{true, true} ->
				maps:map(fun(_TXID, _Value) -> no_tx end, TXs);
			{false, true} ->
				maps:keys(TXs);
			{false, false} ->
				maps:fold(
					fun(_, {TX, _}, Acc) ->
						[TX | Acc]
					end,
					[],
					TXs
				)
		end,
	From ! {Ref, pending_txs, Reply},
	State;

handle({is_a_pending_tx, TXID, From, Ref}, _WPid, #{ txs := TXs } = State) ->
	From ! {Ref, is_a_pending_tx, maps:is_key(TXID, TXs)},
	State;

handle({get_mined_txs, From, Ref}, _WPid, #{ txs := TXs } = State) ->
	MinedTXs = maps:fold(
		fun
			(_, {TX, ready_for_mining}, Acc) ->
				[TX | Acc];
			(_, _, Acc) ->
				Acc
		end,
		[],
		TXs
	),
	From ! {Ref, mined_txs, MinedTXs},
	State;

handle({get_current_diff, From, Ref}, _WPid, State) ->
	#{
		height        := Height,
		diff          := Diff,
		last_retarget := LastRetarget
	} = State,
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
	State;

handle({get_diff, From, Ref}, _WPid, #{ diff := Diff } = State) ->
	From ! {Ref, diff, Diff},
	State;

handle({get_block_txs_pairs, From, Ref}, _WPid, State) ->
	#{ block_txs_pairs := BlockTXPairs } = State,
	From ! {Ref, block_txs_pairs, BlockTXPairs},
	State;

handle({get_mempool_size, From, Ref}, _WPid, #{ mempool_size := Size } = State) ->
	From ! {Ref, get_mempool_size, Size},
	State;

handle(UnknownMsg, _WPid, State) ->
	ar:warn([{event, ar_node_received_unknown_message}, {message, UnknownMsg}]),
	State.
