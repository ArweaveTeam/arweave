%%% @doc The module manages the states of wallets (their balances and last transactions)
%%% in different blocks. Since wallet lists are huge, only one copy is stored at any time,
%%% along with the small "diffs", which allow to reconstruct the wallet lists of the previous,
%%% following, and uncle blocks.
-module(ar_wallets).

-export([start_link/1, get/1, get/2, get_chunk/2, get_balance/1, get_balance/2, get_last_tx/1,
		apply_block/2, add_wallets/4, set_current/3, get_size/0]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc Return the map mapping the given addresses to the corresponding wallets
%% from the latest wallet tree.
get(Address) when is_binary(Address) ->
	ar_wallets:get([Address]);
get(Addresses) ->
	gen_server:call(?MODULE, {get, Addresses}, infinity).

%% @doc Return the map mapping the given addresses to the corresponding wallets
%% from the wallet tree with the given root hash.
get(RootHash, Address) when is_binary(Address) ->
	get(RootHash, [Address]);
get(RootHash, Addresses) ->
	gen_server:call(?MODULE, {get, RootHash, Addresses}, infinity).

%% @doc Return the map containing the wallets, up to ?WALLET_LIST_CHUNK_SIZE, starting
%% from the given cursor (first or an address). The wallets are picked in the ascending
%% alphabetical order, from the tree with the given root hash.
get_chunk(RootHash, Cursor) ->
	gen_server:call(?MODULE, {get_chunk, RootHash, Cursor}, infinity).

%% @doc Return balance of the given wallet in the latest wallet tree.
get_balance(Address) ->
	gen_server:call(?MODULE, {get_balance, Address}, infinity).

%% @doc Return balance of the given wallet in the given wallet tree.
get_balance(RootHash, Address) ->
	gen_server:call(?MODULE, {get_balance, RootHash, Address}, infinity).

%% @doc Return the anchor (last_tx) of the given wallet in the latest wallet tree.
get_last_tx(Address) ->
	gen_server:call(?MODULE, {get_last_tx, Address}, infinity).

%% @doc Compute and cache the account tree for the given new block and its previous block.
apply_block(B, PrevB) ->
	gen_server:call(?MODULE, {apply_block, B, PrevB}, infinity).

%% @doc Cache the wallets to be upserted into the tree with the given root hash. Return
%% the root hash of the new wallet tree.
add_wallets(RootHash, Wallets, Height, Denomination) ->
	gen_server:call(?MODULE, {add_wallets, RootHash, Wallets, Height, Denomination}, infinity).

%% @doc Make the wallet tree with the given root hash "the current tree". The current tree
%% is used by get/1, get_balance/1, and get_last_tx/1.
set_current(RootHash, Height, PruneDepth) when is_binary(RootHash) ->
	Call = {set_current, RootHash, Height, PruneDepth},
	gen_server:call(?MODULE, Call, infinity).

%% @doc Return the number of accounts in the latest state.
get_size() ->
	gen_server:call(?MODULE, get_size, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([{blocks, []} | _]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	DAG = ar_diff_dag:new(<<>>, ar_patricia_tree:new(), not_set),
	ar_node_worker ! wallets_ready,
	{ok, DAG};
init([{blocks, Blocks} | Args]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, {init, Blocks, Args}),
	DAG = ar_diff_dag:new(<<>>, ar_patricia_tree:new(), not_set),
	{ok, DAG}.

handle_call({get, Addresses}, _From, DAG) ->
	{reply, get_map(ar_diff_dag:get_sink(DAG), Addresses), DAG};

handle_call({get, RootHash, Addresses}, _From, DAG) ->
	case ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2) of
		{error, _} = Error ->
			{reply, Error, DAG};
		Tree ->
			{reply, get_map(Tree, Addresses), DAG}
	end;

handle_call({get_chunk, RootHash, Cursor}, _From, DAG) ->
	case ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2) of
		{error, not_found} ->
			{reply, {error, root_hash_not_found}, DAG};
		Tree ->
			{NextCursor, Range} = get_account_tree_range(Tree, Cursor),
			{reply, {ok, {NextCursor, Range}}, DAG}
	end;

handle_call(get_size, _From, DAG) ->
	{reply, ar_patricia_tree:size(ar_diff_dag:get_sink(DAG)), DAG};

handle_call({get_balance, Address}, _From, DAG) ->
	case ar_patricia_tree:get(Address, ar_diff_dag:get_sink(DAG)) of
		not_found ->
			{reply, 0, DAG};
		Entry ->
			Denomination = ar_diff_dag:get_sink_metadata(DAG),
			case Entry of
				{Balance, _LastTX} ->
					{reply, ar_pricing:redenominate(Balance, 1, Denomination), DAG};
				{Balance, _LastTX, BaseDenomination, _MiningPermission} ->
					{reply, ar_pricing:redenominate(Balance, BaseDenomination, Denomination),
							DAG}
			end
	end;

handle_call({get_balance, RootHash, Address}, _From, DAG) ->
	case ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2) of
		{error, _} = Error ->
			{reply, Error, DAG};
		Tree ->
			case ar_patricia_tree:get(Address, Tree) of
				not_found ->
					{reply, 0, DAG};
				Entry ->
					Denomination = ar_diff_dag:get_metadata(DAG, RootHash),
					case Entry of
						{Balance, _LastTX} ->
							{reply, ar_pricing:redenominate(Balance, 1, Denomination), DAG};
						{Balance, _LastTX, BaseDenomination, _MiningPermission} ->
							{reply, ar_pricing:redenominate(Balance, BaseDenomination,
									Denomination), DAG}
					end
			end
	end;

handle_call({get_last_tx, Address}, _From, DAG) ->
	{reply,
		case ar_patricia_tree:get(Address, ar_diff_dag:get_sink(DAG)) of
			not_found ->
				<<>>;
			{_Balance, LastTX} ->
				LastTX;
			{_Balance, LastTX, _Denomination, _MiningPermission} ->
				LastTX
		end,
	DAG};

handle_call({apply_block, B, PrevB}, _From, DAG) ->
	{Reply, DAG2} = apply_block(B, PrevB, DAG),
	{reply, Reply, DAG2};

handle_call({add_wallets, RootHash, Wallets, Height, Denomination}, _From, DAG) ->
	Tree = ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2),
	RootHash2 = compute_hash(Tree, Wallets, Height),
	DAG2 = maybe_add_node(DAG, RootHash2, RootHash, Wallets, Denomination),
	{reply, {ok, RootHash2}, DAG2};

handle_call({set_current, RootHash, Height, PruneDepth}, _, DAG) ->
	{reply, ok, set_current(DAG, RootHash, Height, PruneDepth)}.

handle_cast({init, Blocks, [{from_state, SearchDepth}]}, _) ->
	?LOG_DEBUG([{event, init_from_state}, {block_count, length(Blocks)}]),
	case find_local_account_tree(Blocks, SearchDepth) of
		not_found ->
			ar:console("~n~n\tThe local state is missing an account tree, consider joining "
					"the network via the trusted peers.~n"),
			timer:sleep(1000),
			erlang:halt();
		{Skipped, Tree} ->
			Blocks2 = lists:nthtail(Skipped, Blocks),
			initialize_state(Blocks2, Tree)
	end;
handle_cast({init, Blocks, [{from_peers, Peers}]}, _) ->
	B =
		case length(Blocks) >= ?STORE_BLOCKS_BEHIND_CURRENT of
			true ->
				lists:nth(?STORE_BLOCKS_BEHIND_CURRENT, Blocks);
			false ->
				lists:last(Blocks)
		end,
	Tree = get_tree_from_peers(B, Peers),
	initialize_state(Blocks, Tree);

handle_cast(Msg, DAG) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, DAG}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_wallets_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

find_local_account_tree(Blocks, SearchDepth) ->
	find_local_account_tree(Blocks, SearchDepth, 0).

find_local_account_tree(_Blocks, Skipped, Skipped) ->
	not_found;
find_local_account_tree(Blocks, SearchDepth, Skipped) ->
	{IsLast, B} =
		case length(Blocks) >= ?STORE_BLOCKS_BEHIND_CURRENT of
			true ->
				{false, lists:nth(?STORE_BLOCKS_BEHIND_CURRENT, Blocks)};
			false ->
				{true, lists:last(Blocks)}
		end,
	ID = B#block.wallet_list,
	case ar_storage:read_wallet_list(ID) of
		{ok, Tree} ->
			{Skipped, Tree};
		_ ->
			case IsLast of
				true ->
					not_found;
				false ->
					find_local_account_tree(tl(Blocks), SearchDepth, Skipped + 1)
			end
	end.

initialize_state(Blocks, Tree) ->
	InitialDepth = ?STORE_BLOCKS_BEHIND_CURRENT,
	{DAG3, LastB} = lists:foldl(
		fun (B, start) ->
				Height = B#block.height,
				{RootHash, UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree, Height),
				gen_server:cast(ar_storage, {store_account_tree_update, B#block.height,
						RootHash, UpdateMap}),
				RootHash = B#block.wallet_list,
				DAG = ar_diff_dag:new(RootHash, UpdatedTree, B#block.denomination),
				{DAG, B};
			(B, {DAG, PrevB}) ->
				ExpectedRootHash = B#block.wallet_list,
				{{ok, ExpectedRootHash}, DAG2} = apply_block(B, PrevB, DAG),
				{DAG2, B}
		end,
		start,
		lists:reverse(lists:sublist(Blocks, InitialDepth))
	),
	WalletList = LastB#block.wallet_list,
	LastHeight = LastB#block.height,
	DAG4 = set_current(DAG3, WalletList, LastHeight, InitialDepth),
	ar_events:send(node_state, {account_tree_initialized, LastB#block.height}),
	{noreply, DAG4}.

get_tree_from_peers(B, Peers) ->
	ID = B#block.wallet_list,
	ar:console("Downloading the wallet tree, chunk 1.~n", []),
	case ar_http_iface_client:get_wallet_list_chunk(Peers, ID) of
		{ok, {Cursor, Chunk}} ->
			{ok, Tree} = load_wallet_tree_from_peers(
				ID,
				Peers,
				ar_patricia_tree:from_proplist(Chunk),
				Cursor,
				2
			),
			ar:console("Downloaded the wallet tree successfully.~n", []),
			Tree;
		_ ->
			ar:console("Failed to download wallet tree chunk, retrying...~n", []),
			timer:sleep(1000),
			get_tree_from_peers(B, Peers)
	end.

load_wallet_tree_from_peers(_ID, _Peers, Acc, last, _) ->
	{ok, Acc};
load_wallet_tree_from_peers(ID, Peers, Acc, Cursor, N) ->
	ar_util:terminal_clear(),
	ar:console("Downloading the wallet tree, chunk ~B.~n", [N]),
	case ar_http_iface_client:get_wallet_list_chunk(Peers, ID, Cursor) of
		{ok, {NextCursor, Chunk}} ->
			Acc3 =
				lists:foldl(
					fun({K, V}, Acc2) -> ar_patricia_tree:insert(K, V, Acc2)
					end,
					Acc,
					Chunk
				),
			load_wallet_tree_from_peers(ID, Peers, Acc3, NextCursor, N + 1);
		_ ->
			ar:console("Failed to download wallet tree chunk, retrying...~n", []),
			timer:sleep(1000),
			load_wallet_tree_from_peers(ID, Peers, Acc, Cursor, N)
	end.

apply_block(B, PrevB, DAG) ->
	Denomination2 = B#block.denomination,
	RedenominationHeight2 = B#block.redenomination_height,
	case ar_pricing:may_be_redenominate(PrevB) of
		{Denomination2, RedenominationHeight2} ->
			apply_block2(B, PrevB, DAG);
		_ ->
			{{error, invalid_denomination}, DAG}
	end.

apply_block2(B, PrevB, DAG) ->
	RootHash = PrevB#block.wallet_list,
	Tree = ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2),
	TXs = B#block.txs,
	RewardAddr = B#block.reward_addr,
	Addresses = [RewardAddr | ar_tx:get_addresses(TXs)],
	Addresses2 = [ar_rewards:get_oldest_locked_address(PrevB) | Addresses],
	Addresses3 =
		case B#block.double_signing_proof of
			undefined ->
				Addresses2;
			Proof ->
				[ar_wallet:to_address({?DEFAULT_KEY_TYPE, element(1, Proof)}) | Addresses2]
		end,
	Accounts = get_map(Tree, Addresses3),
	case ar_node_utils:update_accounts(B, PrevB, Accounts) of
		{ok, Args} ->
			apply_block2(B, PrevB, Args, Tree, DAG);
		Error ->
			{Error, DAG}
	end.

apply_block2(B, PrevB, Args, Tree, DAG) ->
	Height = B#block.height,
	{EndowmentPool, MinerReward, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier, Accounts} = Args,
	Denomination = PrevB#block.denomination,
	Denomination2 = B#block.denomination,
	EndowmentPool2 = ar_pricing:redenominate(EndowmentPool, Denomination, Denomination2),
	MinerReward2 = ar_pricing:redenominate(MinerReward, Denomination, Denomination2),
	DebtSupply2 = ar_pricing:redenominate(DebtSupply, Denomination, Denomination2),
	case {B#block.reward_pool == EndowmentPool2, B#block.reward == MinerReward2,
			B#block.debt_supply == DebtSupply2,
			B#block.kryder_plus_rate_multiplier_latch == KryderPlusRateMultiplierLatch,
			B#block.kryder_plus_rate_multiplier == KryderPlusRateMultiplier,
			B#block.height >= ar_fork:height_2_6()} of
		{false, _, _, _, _, _} ->
			{{error, invalid_reward_pool}, DAG};
		{true, false, _, _, _, true} ->
			{{error, invalid_miner_reward}, DAG};
		{true, true, false, _, _, true} ->
			{{error, invalid_debt_supply}, DAG};
		{true, true, true, false, _, true} ->
			{{error, invalid_kryder_plus_rate_multiplier_latch}, DAG};
		{true, true, true, true, false, true} ->
			{{error, invalid_kryder_plus_rate_multiplier}, DAG};
		_ ->
			Tree2 = apply_diff(Accounts, Tree),
			{RootHash2, _, UpdateMap} = ar_block:hash_wallet_list(Tree2, Height),
			case B#block.wallet_list == RootHash2 of
				true ->
					RootHash = PrevB#block.wallet_list,
					DAG2 = maybe_add_node(DAG, RootHash2, RootHash, Accounts, Denomination2),
					gen_server:cast(ar_storage, {store_account_tree_update, B#block.height,
							RootHash2, UpdateMap}),
					{{ok, RootHash2}, DAG2};
				false ->
					{{error, invalid_wallet_list}, DAG}
			end
	end.

set_current(DAG, RootHash, Height, PruneDepth) ->
	UpdatedDAG = ar_diff_dag:update_sink(
		ar_diff_dag:move_sink(DAG, RootHash, fun apply_diff/2, fun reverse_diff/2),
		RootHash,
		fun(Tree, Meta) ->
			{RootHash, UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree, Height),
			gen_server:cast(ar_storage, {store_account_tree_update, Height, RootHash,
					UpdateMap}),
			{RootHash, UpdatedTree, Meta}
		end
	),
	Tree = ar_diff_dag:get_sink(UpdatedDAG),
	true = Height >= ar_fork:height_2_2(),
	prometheus_gauge:set(wallet_list_size, ar_patricia_tree:size(Tree)),
	ar_diff_dag:filter(UpdatedDAG, PruneDepth).

apply_diff(Diff, Tree) ->
	maps:fold(
		fun (Addr, remove, Acc) ->
				ar_patricia_tree:delete(Addr, Acc);
			(Addr, {Balance, LastTX}, Acc) ->
				ar_patricia_tree:insert(Addr, {Balance, LastTX}, Acc);
			(Addr, {Balance, LastTX, Denomination, MiningPermission}, Acc) ->
				ar_patricia_tree:insert(Addr,
						{Balance, LastTX, Denomination, MiningPermission}, Acc)
		end,
		Tree,
		Diff
	).

reverse_diff(Diff, Tree) ->
	maps:map(
		fun(Addr, _Value) ->
			case ar_patricia_tree:get(Addr, Tree) of
				not_found ->
					remove;
				Value ->
					Value
			end
		end,
		Diff
	).

get_map(Tree, Addresses) ->
	lists:foldl(
		fun(Addr, Acc) ->
			case ar_patricia_tree:get(Addr, Tree) of
				not_found ->
					Acc;
				Value ->
					maps:put(Addr, Value, Acc)
			end
		end,
		#{},
		Addresses
	).

get_account_tree_range(Tree, Cursor) ->
	Range =
		case Cursor of
			first ->
				ar_patricia_tree:get_range(?WALLET_LIST_CHUNK_SIZE + 1, Tree);
			_ ->
				ar_patricia_tree:get_range(Cursor, ?WALLET_LIST_CHUNK_SIZE + 1, Tree)
		end,
	case length(Range) of
		?WALLET_LIST_CHUNK_SIZE + 1 ->
			{element(1, hd(Range)), tl(Range)};
		_ ->
			{last, Range}
	end.

compute_hash(Tree, Diff, Height) ->
	Tree2 = apply_diff(Diff, Tree),
	true = Height >= ar_fork:height_2_2(),
	element(1, ar_block:hash_wallet_list(Tree2, Height)).

maybe_add_node(DAG, RootHash, RootHash, _Wallets, _Metadata) ->
	%% The wallet list has not changed - there are no transactions
	%% and the miner did not claim the reward.
	DAG;
maybe_add_node(DAG, UpdatedRootHash, RootHash, Wallets, Metadata) ->
	case ar_diff_dag:is_node(DAG, UpdatedRootHash) of
		true ->
			%% The new wallet list is already known from a different fork.
			DAG;
		false ->
			ar_diff_dag:add_node(DAG, UpdatedRootHash, RootHash, Wallets, Metadata)
	end.
