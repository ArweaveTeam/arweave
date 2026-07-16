%%% @doc Manages the account tree: the per-block mapping from an account's address to its
%%% {balance, last_tx, denomination, mining_permission}, whose root hash is the block's
%%% wallet_list. It holds the account trees of every block in the consensus window - the chain
%%% tip, its recent ancestors, and any forks.
%%%
%%% Only one of those trees is stored in full: a single patricia trie in the shared,
%%% mutable ETS table managed by ar_patricia_tree_ets. That table holds exactly one
%%% representation at a time - it is the "sink" of a diff DAG (ar_diff_dag) whose edges store the
%%% small per-block account diffs needed to reconstruct the previous, following, and uncle
%%% representations. Keeping one full tree plus small diffs (instead of one tree per window
%%% block) trades a little CPU on lookups and reorgs for a large memory saving. Between requests
%%% the sink rests at the current tip. A request that must hash or traverse a non-tip
%%% representation (apply_block/2, add_wallets/4, get_wallet_list_chunk/2 of a non-tip root) runs
%%% inside a snapshot: it transiently repositions the ETS tree to that
%%% representation, does its work, and the snapshot restores the tip exactly before returning.
%%%
%%% Tip reads (get/1, get_balance/1, get_last_tx/1, get_size/0) read the ETS table directly.
%%% Non-tip reads of specific addresses (get/2, get_balance/2) read the current tip from ETS and
%%% overlay the total diff (tip -> target) reconstructed from the diff DAG; the ETS table is not
%%% mutated.
%%%
%%% Hashing a non-tip candidate (apply_block/2, add_wallets/4) needs the whole tree, but only one
%%% tree is stored in ETS, so it runs inside a snapshot. The ETS table is positioned at the
%%% previous block's representation, the block's account diff is applied in place to form the
%%% candidate, and the candidate is hashed - only the touched root-to-leaf paths are re-hashed.
%%% snapshot_restore then rolls the table back to the tip exactly, reinstating every overwritten
%%% node from its recorded pre-snapshot bytes (cached hashes included) - undoing both the
%%% repositioning and the candidate diff in one step and leaving the tip clean.
%%%
%%% Persistence happens on set_current/3: after moving the sink to the new tip we re-hash it,
%%% streaming the content-addressed dirty nodes to ar_storage. Content addressing and incremental
%%% hashing ensure every tip account tree is persisted.
%%%
%%% See ar_patricia_tree_ets for the tree structure and why a patricia trie, and ar_diff_dag for
%%% the sink-and-diffs graph.
-module(ar_account_tree).

-export([start_link/1, get/1, get/2, get_wallet_list_chunk/2, get_balance/1, get_balance/2, get_last_tx/1,
		apply_block/2, add_wallets/4, set_current/3, get_size/0]).

%% Exported for ar_account_tree_persist_tests to exercise the disk -> map -> ets boot path.
-export([load_into_ets/1]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc Return the map mapping the given addresses to the corresponding accounts
%% from the latest account tree.
get(Address) when is_binary(Address) ->
	?MODULE:get([Address]);
get(Addresses) ->
	gen_server:call(?MODULE, {get, Addresses}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the map mapping the given addresses to the corresponding accounts
%% from the account tree with the given root hash.
get(RootHash, Address) when is_binary(Address) ->
	get(RootHash, [Address]);
get(RootHash, Addresses) ->
	gen_server:call(?MODULE, {get, RootHash, Addresses}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the map containing the accounts, up to ?WALLET_LIST_CHUNK_SIZE, starting
%% from the given cursor (first or an address). The accounts are picked in the ascending
%% alphabetical order, from the tree with the given root hash.
get_wallet_list_chunk(RootHash, Cursor) ->
	gen_server:call(?MODULE, {get_wallet_list_chunk, RootHash, Cursor}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return balance of the given account in the latest account tree.
get_balance(Address) ->
	gen_server:call(?MODULE, {get_balance, Address}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return balance of the given account in the given account tree.
get_balance(RootHash, Address) ->
	gen_server:call(?MODULE, {get_balance, RootHash, Address}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the anchor (last_tx) of the given account in the latest account tree.
get_last_tx(Address) ->
	gen_server:call(?MODULE, {get_last_tx, Address}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Compute and cache the account tree for the given new block and its previous block.
apply_block(B, PrevB) ->
	gen_server:call(?MODULE, {apply_block, B, PrevB}, ?DEFAULT_CALL_TIMEOUT).

%% @doc Cache the accounts to be upserted into the tree with the given root hash. Return
%% the root hash of the new account tree.
add_wallets(RootHash, Wallets, Height, Denomination) ->
	gen_server:call(?MODULE, {add_wallets, RootHash, Wallets, Height, Denomination},
			?DEFAULT_CALL_TIMEOUT).

%% @doc Make the account tree with the given root hash "the current tree". The current tree
%% is used by get/1, get_balance/1, and get_last_tx/1.
set_current(RootHash, Height, PruneDepth) when is_binary(RootHash) ->
	Call = {set_current, RootHash, Height, PruneDepth},
	gen_server:call(?MODULE, Call, ?DEFAULT_CALL_TIMEOUT).

%% @doc Return the number of accounts in the latest state.
get_size() ->
	gen_server:call(?MODULE, get_size, ?DEFAULT_CALL_TIMEOUT).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([{blocks, []} | _]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	Tid = ar_patricia_tree_ets:new_named(),
	State = #{ dag => ar_diff_dag:new(<<>>, ets, not_set), sink => <<>>, tid => Tid },
	ar_node_worker ! wallets_ready,
	{ok, State};
init([{blocks, Blocks} | Args]) ->
	%% Trap exit to avoid corrupting any open files on quit.
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, {init, Blocks, Args}),
	Tid = ar_patricia_tree_ets:new_named(),
	State = #{ dag => ar_diff_dag:new(<<>>, ets, not_set), sink => <<>>, tid => Tid },
	{ok, State}.

handle_call(Request, From, State) ->
	%% The observed value must be in native time units. The metric name ends with
	%% _duration_milliseconds, so prometheus converts native to milliseconds on scrape.
	StartTime = erlang:monotonic_time(),
	Result = do_handle_call(Request, From, State),
	arweave_metrics:histogram_observe(account_tree_call_duration_milliseconds,
			[call_label(Request)], erlang:monotonic_time() - StartTime),
	Result.

call_label(Request) when is_atom(Request) ->
	Request;
call_label(Request) ->
	element(1, Request).

do_handle_call({get, Addresses}, _From, State) ->
	{reply, accounts_at_tip(State, Addresses), State};

do_handle_call({get, RootHash, Addresses}, _From, State) ->
	case accounts_at_root(State, RootHash, Addresses) of
		{error, _} = Error ->
			{reply, Error, State};
		{ok, Map} ->
			{reply, Map, State}
	end;

do_handle_call({get_wallet_list_chunk, RootHash, Cursor}, _From, State) ->
	with_known_root(RootHash, State, fun() -> get_wallet_list_chunk(State, RootHash, Cursor) end);

do_handle_call(get_size, _From, State) ->
	{reply, ar_patricia_tree_ets:size(maps:get(tid, State)), State};

do_handle_call({get_balance, Address}, _From, State) ->
	#{ dag := DAG, tid := Tid } = State,
	Reply =
		case ar_patricia_tree_ets:get(Address, Tid) of
			not_found ->
				0;
			Entry ->
				redenominate_balance(Entry, ar_diff_dag:get_sink_metadata(DAG))
		end,
	{reply, Reply, State};

do_handle_call({get_balance, RootHash, Address}, _From, State) ->
	case accounts_at_root(State, RootHash, [Address]) of
		{error, _} = Error ->
			{reply, Error, State};
		{ok, Map} ->
			Reply =
				case maps:get(Address, Map, not_found) of
					not_found ->
						0;
					Entry ->
						Denomination = ar_diff_dag:get_metadata(maps:get(dag, State), RootHash),
						redenominate_balance(Entry, Denomination)
				end,
			{reply, Reply, State}
	end;

do_handle_call({get_last_tx, Address}, _From, State) ->
	{reply,
		case ar_patricia_tree_ets:get(Address, maps:get(tid, State)) of
			not_found ->
				<<>>;
			{_Balance, LastTX} ->
				LastTX;
			{_Balance, LastTX, _Denomination, _MiningPermission} ->
				LastTX
		end,
	State};

do_handle_call({apply_block, B, PrevB}, _From, State) ->
	with_known_root(PrevB#block.wallet_list, State,
			fun() -> apply_block(B, PrevB, State) end);

do_handle_call({add_wallets, RootHash, Wallets, Height, Denomination}, _From, State) ->
	with_known_root(RootHash, State,
			fun() -> add_wallets(State, RootHash, Wallets, Height, Denomination) end);

do_handle_call({set_current, RootHash, Height, PruneDepth}, _, State) ->
	{reply, ok, set_current(State, RootHash, Height, PruneDepth)}.

handle_cast({init, Blocks, Args}, State) ->
	case proplists:get_value(from_state, Args) of
		undefined ->
			Peers = proplists:get_value(from_peers, Args),
			B =
				case length(Blocks) >= ar_block:get_consensus_window_size() of
					true ->
						lists:nth(ar_block:get_consensus_window_size(), Blocks);
					false ->
						lists:last(Blocks)
				end,
			Tree = get_tree_from_peers(B, Peers),
			initialize_state(Blocks, Tree, State);
		SearchDepth ->
			?LOG_DEBUG([{event, init_from_state}, {block_count, length(Blocks)}]),
			CustomDir = proplists:get_value(custom_dir, Args, not_set),
			case find_local_account_tree(Blocks, SearchDepth, CustomDir) of
				not_found ->
					ar:console("~n~n\tThe local state is missing an account tree, consider joining "
							"the network via the trusted peers.~n"),
					timer:sleep(1000),
					init:stop(1);
				{Skipped, Tree} ->
					Blocks2 = lists:nthtail(Skipped, Blocks),
					initialize_state(Blocks2, Tree, State)
			end
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_account_tree_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

find_local_account_tree(Blocks, SearchDepth, CustomDir) ->
	find_local_account_tree(Blocks, SearchDepth, 0, CustomDir).

find_local_account_tree(_Blocks, Skipped, Skipped, _CustomDir) ->
	not_found;
find_local_account_tree(Blocks, SearchDepth, Skipped, CustomDir) ->
	{IsLast, B} =
		case length(Blocks) >= ar_block:get_consensus_window_size() of
			true ->
				{false, lists:nth(ar_block:get_consensus_window_size(), Blocks)};
			false ->
				{true, lists:last(Blocks)}
		end,
	ID = B#block.wallet_list,
	case ar_storage:read_wallet_list(ID, CustomDir) of
		{ok, Tree} ->
			{Skipped, Tree};
		_ ->
			case IsLast of
				true ->
					not_found;
				false ->
					find_local_account_tree(tl(Blocks), SearchDepth, Skipped + 1, CustomDir)
			end
	end.

%% @doc Load the base account tree into ETS, build the diff DAG by applying each block in the
%% consensus window on top of it, then make the last block the current tip. Tree is the
%% map-based ar_patricia_tree returned by the disk/peer loaders; we copy it into the ETS table.
initialize_state(Blocks, BaseTree, State) ->
	InitialDepth = ar_block:get_consensus_window_size(),
	Window = lists:reverse(lists:sublist(Blocks, InitialDepth)),
	[BaseB | RestB] = Window,
	Tid = load_into_ets(BaseTree, maps:get(tid, State)),
	%% Persist the full base tree (every node is dirty on a freshly loaded tree).
	{BaseRoot, _, _} = compute_hash(Tid, #{ sink => ar_storage }),
	BaseRoot = BaseB#block.wallet_list,
	State1 = State#{
		dag => ar_diff_dag:new(BaseRoot, ets, BaseB#block.denomination),
		sink => BaseRoot,
		tid => Tid
	},
	{StateN, LastB} = lists:foldl(
		fun(B, {AccState, PrevB}) ->
			ExpectedRootHash = B#block.wallet_list,
			{{ok, ExpectedRootHash}, AccState2} = apply_block(B, PrevB, AccState),
			{AccState2, B}
		end,
		{State1, BaseB},
		RestB
	),
	State2 = set_current(StateN, LastB#block.wallet_list, LastB#block.height, InitialDepth),
	ar_events:send(node_state, {account_tree_initialized, LastB#block.height}),
	{noreply, State2}.

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

%% @doc Copy a map-based ar_patricia_tree into a fresh ETS account tree, returning its id.
load_into_ets(MapTree) ->
	load_into_ets(MapTree, ar_patricia_tree_ets:new()).

load_into_ets(MapTree, Tid) ->
	ar_patricia_tree:foldr(
		fun(Key, Value, _Acc) -> ar_patricia_tree_ets:insert(Key, Value, Tid) end,
		ok,
		MapTree
	),
	Tid.

apply_block(B, PrevB, State) ->
	Denomination2 = B#block.denomination,
	RedenominationHeight2 = B#block.redenomination_height,
	case ar_pricing:may_be_redenominate(PrevB) of
		{Denomination2, RedenominationHeight2} ->
			apply_block2(B, PrevB, State);
		_ ->
			{{error, invalid_denomination}, State}
	end.

%% Positioning the ETS tree at PrevB and hashing the candidate runs inside a snapshot, so the
%% tip ETS - and the diff DAG sink, which stays at the tip throughout - are left untouched. The
%% candidate is recorded in the DAG as a diff off PrevB. It is persisted only once it becomes
%% the tip via set_current/3.
apply_block2(B, PrevB, State) ->
	Tid = maps:get(tid, State),
	PrevRootHash = PrevB#block.wallet_list,
	Addresses = block_addresses(B, PrevB),
	Outcome = with_snapshot(Tid, fun() ->
		_ = move_sink_to(State, PrevRootHash),
		Accounts = accounts_at_tip(State, Addresses),
		apply_block_outcome(B, PrevB, Accounts, Tid)
	end),
	finalize_apply_block(Outcome, B, PrevRootHash, State).

%% @doc The addresses whose accounts a block may change: the reward address, the senders and
%% recipients of its transactions, the oldest locked reward address, and (when present) the
%% double signing proof's address.
block_addresses(B, PrevB) ->
	Addresses = [B#block.reward_addr | ar_tx:get_addresses(B#block.txs)],
	Addresses2 = [ar_rewards:get_oldest_locked_address(PrevB) | Addresses],
	case B#block.double_signing_proof of
		undefined ->
			Addresses2;
		Proof ->
			[ar_wallet:hash_pub_key(element(1, Proof)) | Addresses2]
	end.

apply_block_outcome(B, PrevB, Accounts, Tid) ->
	case ar_node_utils:update_accounts(B, PrevB, Accounts) of
		{ok, Args} ->
			apply_block_validate(B, PrevB, Args, Tid);
		Error ->
			Error
	end.

apply_block_validate(B, PrevB, Args, Tid) ->
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
			{error, invalid_reward_pool};
		{true, false, _, _, _, true} ->
			{error, invalid_miner_reward};
		{true, true, false, _, _, true} ->
			{error, invalid_debt_supply};
		{true, true, true, false, _, true} ->
			{error, invalid_kryder_plus_rate_multiplier_latch};
		{true, true, true, true, false, true} ->
			{error, invalid_kryder_plus_rate_multiplier};
		_ ->
			apply_diff_ets(Accounts, Tid),
			{RootHash2, _, _} = compute_hash(Tid, #{}),
			{ok, RootHash2, Accounts, Denomination2}
	end.

finalize_apply_block({ok, RootHash2, Accounts, Denomination2}, B, PrevRootHash, State) ->
	case B#block.wallet_list == RootHash2 of
		true ->
			DAG2 = maybe_add_node(maps:get(dag, State), RootHash2, PrevRootHash, Accounts,
					Denomination2),
			{{ok, RootHash2}, State#{ dag := DAG2 }};
		false ->
			{{error, invalid_wallet_list}, State}
	end;
finalize_apply_block({error, _} = Error, _B, _PrevRootHash, State) ->
	{Error, State}.

add_wallets(State, RootHash, Wallets, Height, Denomination) ->
	Tid = maps:get(tid, State),
	true = Height >= ar_fork:height_2_2(),
	RootHash2 = with_snapshot(Tid, fun() ->
		_ = move_sink_to(State, RootHash),
		apply_diff_ets(Wallets, Tid),
		{Root, _, _} = compute_hash(Tid, #{}),
		Root
	end),
	DAG2 = maybe_add_node(maps:get(dag, State), RootHash2, RootHash, Wallets, Denomination),
	{{ok, RootHash2}, State#{ dag := DAG2 }}.

set_current(State, RootHash, Height, PruneDepth) ->
	State1 = move_sink_to(State, RootHash),
	Tid = maps:get(tid, State1),
	{RootHash, _, _} = compute_hash(Tid, #{ sink => ar_storage }),
	true = Height >= ar_fork:height_2_2(),
	arweave_metrics:gauge_set(wallet_list_size, ar_patricia_tree_ets:size(Tid)),
	State1#{ dag := ar_diff_dag:filter(maps:get(dag, State1), PruneDepth) }.

get_wallet_list_chunk(State, RootHash, Cursor) ->
	Range =
		case is_sink(State, RootHash) of
			true ->
				get_account_tree_range(State, Cursor);
			false ->
				with_snapshot(maps:get(tid, State), fun() ->
					_ = move_sink_to(State, RootHash),
					get_account_tree_range(State, Cursor)
				end)
		end,
	{{ok, Range}, State}.

%%%===================================================================
%%% ETS sink / diff DAG helpers.
%%%===================================================================

%% @doc Helper function remove duplication around checking if a RootHash exists before
%% running an operation which relies on it.
with_known_root(RootHash, State, Fun) ->
	case ar_diff_dag:is_node(maps:get(dag, State), RootHash) of
		false ->
			{reply, {error, root_hash_not_found}, State};
		true ->
			{Reply, State2} = Fun(),
			{reply, Reply, State2}
	end.

is_sink(State, RootHash) ->
	maps:get(sink, State) == RootHash.

%% @doc Run Fun with snapshot recording active and restore the ETS table to its pre-call bytes
%% afterwards (cached node hashes included), so positioning the tree at a non-tip representation
%% to hash or traverse it leaves the tip untouched. The diff DAG sink is not moved by the
%% excursion - move_sink_to/2 inside Fun returns a state the caller discards, keeping the
%% original DAG, which stays consistent with the restored tip ETS.
with_snapshot(Tid, Fun) ->
	ar_patricia_tree_ets:snapshot_begin(Tid),
	try
		Fun()
	after
		ar_patricia_tree_ets:snapshot_restore(Tid)
	end.

%% @doc Move the ETS tree (the diff DAG sink) to the representation identified by
%% the given root hash, mutating the ETS table in place. No-op when already there. Precondition:
%% RootHash is a node in the diff DAG - ar_diff_dag:move_sink crashes otherwise. 
move_sink_to(State, RootHash) ->
	case is_sink(State, RootHash) of
		true ->
			State;
		false ->
			#{ dag := DAG, tid := Tid } = State,
			HopCounter = counters:new(1, []),
			DAG2 = ar_diff_dag:move_sink(
				DAG,
				RootHash,
				fun(Diff, Entity) ->
					counters:add(HopCounter, 1, 1),
					apply_diff_ets(Diff, Tid),
					Entity
				end,
				fun(Diff, _Entity) -> reverse_diff_ets(Diff, Tid) end
			),
			arweave_metrics:histogram_observe(account_tree_sink_move_hops, [],
					counters:get(HopCounter, 1)),
			State#{ dag := DAG2, sink := RootHash }
	end.

%% @doc Read the accounts for the given addresses at the current tip (the ETS
%% tree) into a map.
accounts_at_tip(State, Addresses) ->
	Tid = maps:get(tid, State),
	lists:foldl(
		fun(Addr, Acc) ->
			case ar_patricia_tree_ets:get(Addr, Tid) of
				not_found ->
					Acc;
				Value ->
					maps:put(Addr, Value, Acc)
			end
		end,
		#{},
		Addresses
	).

%% @doc Collect the accounts for the given addresses at the representation identified by
%% RootHash by overlaying the total diff (tip -> RootHash) reconstructed from the diff DAG on the
%% current tip read from ETS.
accounts_at_root(State, RootHash, Addresses) ->
	case is_sink(State, RootHash) of
		true ->
			{ok, accounts_at_tip(State, Addresses)};
		false ->
			case ar_diff_dag:reconstruct(maps:get(dag, State), RootHash, fun merge_total_diff/2) of
				{error, _} = Error ->
					Error;
				TotalDiff0 ->
					%% If the diff is 'ets' then there is no diff to combine.
					TotalDiff = case TotalDiff0 of ets -> #{}; _ -> TotalDiff0 end,
					{ok, combine(State, TotalDiff, Addresses)}
			end
	end.

%% @doc Fold function for ar_diff_dag:reconstruct/3 that accumulates the total diff transforming
%% the tip into the target representation. reconstruct seeds the fold with the sink entity, which
%% is the placeholder atom 'ets' (the real tree lives in the ETS table, not the DAG), so the
%% first step replaces it. Later steps merge, with the diff closer to the target winning.
merge_total_diff(Diff, ets) ->
	Diff;
merge_total_diff(Diff, Acc) ->
	maps:merge(Acc, Diff).

%% @doc Build the result map for the requested addresses at the target representation by
%% overlaying the reconstructed tip -> target diff onto the ETS tip: an address the diff touched
%% takes the diff's outcome, one it left alone is unchanged since the tip and is read from ETS.
combine(State, TotalDiff, Addresses) ->
	Tid = maps:get(tid, State),
	lists:foldl(
		fun(Addr, Acc) ->
			case maps:find(Addr, TotalDiff) of
				{ok, remove} ->
					%% Has no account at the target - leave it out of the result.
					Acc;
				{ok, Value} ->
					%% Changed between tip and target - use the target value.
					maps:put(Addr, Value, Acc);
				error ->
					%% Untouched by the diff, so unchanged since the tip - read the live ETS value.
					case ar_patricia_tree_ets:get(Addr, Tid) of
						not_found ->
							Acc;
						Value ->
							maps:put(Addr, Value, Acc)
					end
			end
		end,
		#{},
		Addresses
	).

%% @doc Apply an account diff (Addr => Value | remove) to the ETS table in place.
apply_diff_ets(Diff, Tid) ->
	maps:foreach(
		fun (Addr, remove) ->
				ar_patricia_tree_ets:delete(Addr, Tid);
			(Addr, Value) ->
				ar_patricia_tree_ets:insert(Addr, Value, Tid)
		end,
		Diff
	),
	ok.

%% @doc Build the reverse of a diff against the current ETS state: for every touched address,
%% record its current value, or 'remove' if it is currently absent. Used by ar_diff_dag to
%% keep the DAG reconstructable as the sink moves.
reverse_diff_ets(Diff, Tid) ->
	maps:map(
		fun(Addr, _Value) ->
			case ar_patricia_tree_ets:get(Addr, Tid) of
				not_found ->
					remove;
				Value ->
					Value
			end
		end,
		Diff
	).

compute_hash(Tid, PersistOpts) ->
	{RootHash, Tree, Info} =
		ar_patricia_tree_ets:compute_hash(Tid, ar_block:wallet_list_hash_fun(), PersistOpts),
	arweave_metrics:histogram_observe(account_tree_rehashed_nodes, [],
			maps:get(rehashed_nodes, Info, 0)),
	{RootHash, Tree, Info}.

redenominate_balance({Balance, _LastTX}, Denomination) ->
	ar_pricing:redenominate(Balance, 1, Denomination);
redenominate_balance({Balance, _LastTX, BaseDenomination, _MiningPermission}, Denomination) ->
	ar_pricing:redenominate(Balance, BaseDenomination, Denomination).

get_account_tree_range(State, Cursor) ->
	Tid = maps:get(tid, State),
	Range =
		case Cursor of
			first ->
				ar_patricia_tree_ets:get_range(?WALLET_LIST_CHUNK_SIZE + 1, Tid);
			_ ->
				ar_patricia_tree_ets:get_range(Cursor, ?WALLET_LIST_CHUNK_SIZE + 1, Tid)
		end,
	case length(Range) of
		?WALLET_LIST_CHUNK_SIZE + 1 ->
			{element(1, hd(Range)), tl(Range)};
		_ ->
			{last, Range}
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
