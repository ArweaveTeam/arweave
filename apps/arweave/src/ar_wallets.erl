%%% @doc The module manages the states of wallets (their balances and last transactions)
%%% in different blocks. Since wallet lists are huge, only one copy is stored at any time,
%%% along with the small "diffs", which allow to reconstruct the wallet lists of the previous,
%%% following, and uncle blocks.
-module(ar_wallets).

-export([start_link/1, get/1, get/2, get_chunk/2, get_balance/1, get_balance/2, get_last_tx/1,
		apply_block/5, add_wallets/4, update_wallets/4, set_current/5]).

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

%% @doc Compute and cache the wallet tree for the given new block, provided with
%% the previous block's wallet tree root hash, reward pool, USD to AR exchange rate,
%% and height. Return the root hash of the new wallet tree.
apply_block(NewB, RootHash, RewardPool, Rate, Height) ->
	gen_server:call(?MODULE, {apply_block, NewB, RootHash, RewardPool, Rate, Height},
			infinity).

%% @doc Cache the wallets to be upserted into the tree with the given root hash. Return
%% the root hash of the new wallet tree.
add_wallets(RootHash, Wallets, RewardAddr, Height) ->
	gen_server:call(?MODULE, {add_wallets, RootHash, Wallets, RewardAddr, Height}, infinity).

%% @doc Update the wallets in the tree with the given root hash. Effectively, erase
%% the given root hash from cache. Return the root hash of the updated wallet tree.
update_wallets(RootHash, Wallets, RewardAddr, Height) ->
	gen_server:call(?MODULE, {update_wallets, RootHash, Wallets, RewardAddr, Height},
			infinity).

%% @doc Make the wallet tree with the given root hash "the current tree". The current tree
%% is used by get/1, get_balance/1, and get_last_tx/1.
set_current(PrevRootHash, RootHash, RewardAddr, Height, PruneDepth) when is_binary(RootHash) ->
	Call = {set_current, PrevRootHash, RootHash, RewardAddr, Height, PruneDepth},
	gen_server:call(?MODULE, Call, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([{blocks, []}, {peers, _Peers}]) ->
	process_flag(trap_exit, true),
	DAG = ar_diff_dag:new(<<>>, ar_patricia_tree:new(), not_set),
	ar_node_worker ! wallets_ready,
	{ok, DAG};
init([{blocks, Blocks}, {peers, Peers}]) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, {init, Blocks, Peers}), 
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
			{NextCursor, Range} = ar_storage:get_wallet_list_range(Tree, Cursor),
			{reply, {ok, {NextCursor, Range}}, DAG}
	end;

handle_call({get_balance, Address}, _From, DAG) ->
	{reply,
		case ar_patricia_tree:get(Address, ar_diff_dag:get_sink(DAG)) of
			not_found ->
				0;
			{B, _LastTX} ->
				B
		end,
	DAG};

handle_call({get_balance, RootHash, Address}, _From, DAG) ->
	case ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2) of
		{error, _} = Error ->
			{reply, Error, DAG};
		Tree ->
			{reply,
				case ar_patricia_tree:get(Address, Tree) of
					not_found ->
						0;
					{B, _LastTX} ->
						B
				end,
			DAG}
	end;

handle_call({get_last_tx, Address}, _From, DAG) ->
	{reply,
		case ar_patricia_tree:get(Address, ar_diff_dag:get_sink(DAG)) of
			not_found ->
				<<>>;
			{_Balance, LastTX} ->
				LastTX
		end,
	DAG};

handle_call({apply_block, NewB, RootHash, RewardPool, Rate, Height}, _From, DAG) ->
	{Reply, UpdatedDAG} = apply_block(DAG, NewB, RootHash, RewardPool, Rate, Height),
	{reply, Reply, UpdatedDAG};

handle_call({add_wallets, RootHash, Wallets, RewardAddr, Height}, _From, DAG) ->
	Tree = ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2),
	{UpdatedRootHash, NoRewardWalletHash} =
		compute_hash(Tree, Wallets, not_set, Height, RewardAddr),
	UpdatedDAG = maybe_add_node(DAG, UpdatedRootHash, RootHash, Wallets, NoRewardWalletHash),
	{reply, {ok, UpdatedRootHash}, UpdatedDAG};

handle_call({update_wallets, RootHash, Wallets, RewardAddr, Height}, _From, DAG) ->
	Tree = ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2),
	NoRewardWalletHash = ar_diff_dag:get_metadata(DAG, RootHash),
	{UpdatedRootHash, UpdatedNoRewardWalletHash} =
		compute_hash(Tree, Wallets, NoRewardWalletHash, Height, RewardAddr),
	case UpdatedRootHash of
		RootHash ->
			{reply, {ok, RootHash}, DAG};
		_ ->
			Meta = UpdatedNoRewardWalletHash,
			UpdatedDAG =
				case ar_diff_dag:is_sink(DAG, RootHash) of
					true ->
						maybe_add_node(DAG, UpdatedRootHash, RootHash, Wallets, Meta);
					false ->
						ar_diff_dag:update_leaf_source(
							DAG,
							RootHash,
							fun(Diff, _Meta) ->
								{UpdatedRootHash, maps:merge(Diff, Wallets), Meta}
							end
						)
				end,
			{reply, {ok, UpdatedRootHash}, UpdatedDAG}
	end;

handle_call({set_current, PrevRootHash, RootHash, RewardAddr, Height, PruneDepth}, _, DAG) ->
	{reply, ok, set_current(DAG, PrevRootHash, RootHash, RewardAddr, Height, PruneDepth)}.

handle_cast({write_wallet_list_chunk, RootHash, Cursor, Position}, DAG) ->
	case ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2) of
		{error, not_found} ->
			%% Blocks were mined too fast or IO is too slow - the root hash has been
			%% pruned from DAG.
			ok;
		Tree ->
			{NextCursor, Range} = ar_storage:get_wallet_list_range(Tree, Cursor),
			StoredRange = case NextCursor of last -> [last | Range]; _ -> Range end,
			StoreFun =
				case ar_storage:get_free_space(".") > ?DISK_HEADERS_BUFFER_SIZE of
					false ->
						fun ar_disk_cache:write_wallet_list_chunk/3;
					true ->
						fun ar_storage:write_wallet_list_chunk/3
				end,
			case {StoreFun(RootHash, StoredRange, Position), NextCursor} of
				{ok, last} ->
					ok;
				{ok, _} ->
					NextPosition = Position + ?WALLET_LIST_CHUNK_SIZE,
					Cast = {write_wallet_list_chunk, RootHash, NextCursor, NextPosition},
					gen_server:cast(self(), Cast);
				{{error, _Reason}, _} ->
					ok
			end
	end,
	{noreply, DAG};

handle_cast({init, Blocks, Peers}, _) ->
	InitialDepth = ?STORE_BLOCKS_BEHIND_CURRENT,
	{DAG3, LastB, PrevWalletList} = lists:foldl(
		fun (B, start) ->
				Tree = get_tree(B, Peers),
				{RootHash, UpdatedTree} =
					ar_block:hash_wallet_list(B#block.height, B#block.reward_addr, Tree),
				RootHash = B#block.wallet_list,
				DAG = ar_diff_dag:new(RootHash, UpdatedTree, not_set),
				{DAG, B, <<>>};
			(B, {DAG, PreviousB, _}) ->
				RewardPool = PreviousB#block.reward_pool,
				Height = PreviousB#block.height,
				RootHash = PreviousB#block.wallet_list,
				ExpectedRootHash = B#block.wallet_list,
				Rate = ar_pricing:usd_to_ar_rate(PreviousB),
				{{ok, ExpectedRootHash}, DAG2} =
					apply_block(DAG, B, RootHash, RewardPool, Rate, Height),
				{DAG2, B, PreviousB#block.wallet_list}
		end,
		start,
		lists:reverse(lists:sublist(Blocks, InitialDepth))
	),
	RewardAddr = LastB#block.reward_addr,
	WalletList = LastB#block.wallet_list,
	LastHeight = LastB#block.height,
	DAG4 = set_current(DAG3, PrevWalletList, WalletList, RewardAddr, LastHeight, InitialDepth),
	ar_node_worker ! wallets_ready,
	{noreply, DAG4};

handle_cast(Msg, DAG) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, DAG}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_wallets_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_tree(B, Peers) ->
	ID = B#block.wallet_list,
	case ar_storage:read_wallet_list(ID) of
		{ok, Tree} ->
			Tree;
		_ ->
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
					get_tree(B, Peers)
			end
	end.

load_wallet_tree_from_peers(_ID, _Peers, Acc, last, _) ->
	{ok, Acc};
load_wallet_tree_from_peers(ID, Peers, Acc, Cursor, N) ->
	io:format(os:cmd(clear)),
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

apply_block(DAG, NewB, RootHash, RewardPool, Rate, Height) ->
	Tree = ar_diff_dag:reconstruct(DAG, RootHash, fun apply_diff/2),
	TXs = NewB#block.txs,
	Wallets = get_map(Tree, [NewB#block.reward_addr | ar_tx:get_addresses(TXs)]),
	{NewRewardPool, MinerReward, UpdatedWallets} =
		ar_node_utils:update_wallets(NewB, Wallets, RewardPool,	Rate, Height),
	case {NewB#block.reward_pool == NewRewardPool, NewB#block.reward == MinerReward,
			NewB#block.height >= ar_fork:height_2_6()} of
		{false, _, _} ->
			{{error, invalid_reward_pool}, DAG};
		{true, false, true} ->
			{{error, invalid_miner_reward}, DAG};
		_ ->
			RewardAddr = NewB#block.reward_addr,
			UpdatedTree = apply_diff(UpdatedWallets, Tree),
			{UpdatedRootHash, _} =
				ar_block:hash_wallet_list(Height + 1, RewardAddr, UpdatedTree),
			case NewB#block.wallet_list == UpdatedRootHash
						orelse NewB#block.height < ar_fork:height_2_2() of
				true ->
					UpdatedDAG =
						maybe_add_node(DAG, UpdatedRootHash, RootHash, UpdatedWallets, not_set),
					{{ok, UpdatedRootHash}, UpdatedDAG};
				false ->
					{{error, invalid_wallet_list}, DAG}
			end
	end.

set_current(DAG, PrevRootHash, RootHash, RewardAddr, Height, PruneDepth) ->
	UpdatedDAG = ar_diff_dag:update_sink(
		ar_diff_dag:move_sink(DAG, RootHash, fun apply_diff/2, fun reverse_diff/2),
		RootHash,
		fun(Tree, Meta) ->
			{RootHash, UpdatedTree} = ar_block:hash_wallet_list(Height, RewardAddr, Tree),
			{RootHash, UpdatedTree, Meta}
		end
	),
	Tree = ar_diff_dag:get_sink(UpdatedDAG),
	case Height >= ar_fork:height_2_2() of
		true ->
			gen_server:cast(self(), {write_wallet_list_chunk, RootHash, first, 0});
		false ->
			IsRewardAddrNew =
				case PrevRootHash of
					<<>> ->
						false;
					_ ->
						PrevTree =
							ar_diff_dag:reconstruct(UpdatedDAG, PrevRootHash, fun apply_diff/2),
						ar_patricia_tree:get(RewardAddr, PrevTree) == not_found
				end,
			ok = ar_storage:write_wallet_list(RootHash, RewardAddr, IsRewardAddrNew, Tree)
	end,
	prometheus_gauge:set(wallet_list_size, ar_patricia_tree:size(Tree)),
	ar_diff_dag:filter(UpdatedDAG, PruneDepth).

apply_diff(Diff, Tree) ->
	maps:fold(
		fun (Addr, remove, Acc) ->
				ar_patricia_tree:delete(Addr, Acc);
			(Addr, {Balance, LastTX}, Acc) ->
				ar_patricia_tree:insert(Addr, {Balance, LastTX}, Acc)
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

compute_hash(Tree, Diff, NoRewardRootHash, Height, RewardAddr) ->
	UpdatedTree = apply_diff(Diff, Tree),
	case Height >= ar_fork:height_2_2() of
		true ->
			H = element(1, ar_block:hash_wallet_list(Height, RewardAddr, UpdatedTree)),
			{H, NoRewardRootHash};
		false ->
			compute_hash_pre_fork_2_2(UpdatedTree, Diff, NoRewardRootHash, RewardAddr)
	end.

compute_hash_pre_fork_2_2(Tree, Changes, NoRewardRootHash, RewardAddr) ->
	case NoRewardRootHash /= not_set andalso map_size(maps:without([RewardAddr], Changes)) == 0 of
		true ->
			RewardWallet =
				case maps:get(RewardAddr, Changes, not_found) of
					not_found ->
						unclaimed;
					{Balance, LastTX} ->
						{RewardAddr, Balance, LastTX}
				end,
			{ar_block:hash_wallet_list(RewardWallet, NoRewardRootHash), NoRewardRootHash};
		false ->
			{RW, WLH} = ar_block:hash_wallet_list_without_reward_wallet(RewardAddr, Tree),
			{ar_block:hash_wallet_list(RW, WLH), WLH}
	end.

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
