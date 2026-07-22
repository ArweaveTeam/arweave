%%% @doc The ETS-backed variant of ar_patricia_tree_core (see there for the tree structure).
%%% On a production node the tree lives in the named, protected ETS table ar_patricia_tree,
%%% created and owned by the ar_account_tree gen_server (new_named/0). Only the owner can
%%% write, so a stray process cannot corrupt the tip. Any process may still read the table
%%% for diagnostics. Every operation mutates the table in place and returns the table id.
%%% Exactly one account tree is stored in full at a time - the diff DAG sink in
%%% ar_account_tree. Tests and benchmarks create their own unnamed tables with new/0.
-module(ar_patricia_tree_ets).

%% size/1 is a tree accessor here; keep it from clashing with erlang:size/1.
-compile({no_auto_import, [size/1]}).

-behaviour(ar_patricia_tree_core).

-export([new/0, new_named/0, delete_table/1, insert/3, get/2, size/1, compute_hash/2,
		compute_hash/3, foldr/3, is_empty/1, from_proplist/1, delete/2, get_range/2, get_range/3,
		snapshot_begin/1, snapshot_restore/1]).

%% ar_patricia_tree_core backend callbacks.
-export([get_node/2, put_node/3, del_node/2, get_size/1, set_size/2, emit/4, progress_extra/1]).

%% Distinct from any node key (node keys are the atom 'root' or binaries).
-define(SIZE_KEY, '$size').

%% When KV is set, buffer this many node updates in the hashing process and
%% flush them to RocksDB as a single write batch (bounded memory, amortized I/O).
-define(PERSIST_BATCH_SIZE, 5000).

%% Process-dictionary key under which an active snapshot records the pre-snapshot value of
%% every node it overwrites. See snapshot_begin/1.
-define(SNAPSHOT_KEY, '$snapshot').

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Create a fresh, unnamed empty tree owned by the caller. Used by tests and benchmarks.
%% The live node uses the named table created by new_named/0.
new() ->
	init_table(ets:new(ar_patricia_tree, [set, protected])).

%% @doc Create or reset the named, protected ar_patricia_tree table holding the live node's
%% account tree. Only ar_account_tree calls this. The gen_server owns the table, so it alone
%% can write, and the table dies with it. Crashes if the name is taken by another live
%% process.
new_named() ->
	Tid =
		case ets:whereis(ar_patricia_tree) of
			undefined ->
				ets:new(ar_patricia_tree, [set, protected, named_table]);
			_ ->
				ar_patricia_tree
		end,
	init_table(Tid).

init_table(Tid) ->
	ets:delete_all_objects(Tid),
	ets:insert(Tid, {?SIZE_KEY, 0}),
	ets:insert(Tid, {root, {no_parent, gb_sets:new(), no_hash, no_prefix, no_value}}),
	Tid.

%% @doc Delete the underlying ETS table.
delete_table(Tree) ->
	ets:delete(Tree),
	ok.

%% @doc Insert the given value under the given binary key.
insert(Key, Value, Tree) ->
	ar_patricia_tree_core:insert(?MODULE, Key, Value, Tree).

%% @doc Get the value stored under the given key or not_found.
get(Key, Tree) ->
	ar_patricia_tree_core:get_value(?MODULE, Key, Tree).

%% @doc Return the number of values in the tree.
size(Tree) ->
	ar_patricia_tree_core:size(?MODULE, Tree).

%% Recompute the root hash. The third element is #{ rehashed_nodes => N } - the number of dirty
%% nodes this call re-hashed (this variant does not build an UpdateMap); with PersistOpts
%% #{ sink => PID } each node update is streamed to that process in batches (see
%% maybe_persist/4).
-spec compute_hash(ets:tid(), fun()) -> {binary(), ets:tid(), map()}.
compute_hash(Tree, HashFun) ->
	compute_hash(Tree, HashFun, #{}).

-spec compute_hash(ets:tid(), fun(), map()) -> {binary(), ets:tid(), map()}.
compute_hash(Tree, HashFun, PersistOpts) ->
	Sink = maps:get(sink, PersistOpts, undefined),
	persist_init(Sink),
	erlang:put(pt_rehashed_count, 0),
	{RootHash, Tree2, _} = ar_patricia_tree_core:compute_hash(?MODULE, Tree, HashFun, Sink),
	persist_flush(Sink),
	{RootHash, Tree2, #{ rehashed_nodes => erlang:erase(pt_rehashed_count) }}.

%% @doc Traverse the keys in the reversed alphabetical order iteratively applying
%% the given function of a key, a value, and an accumulator.
foldr(Fun, Acc, Tree) ->
	ar_patricia_tree_core:foldr(?MODULE, Fun, Acc, Tree).

%% @doc Return true if the tree stores no values.
is_empty(Tree) ->
	ar_patricia_tree_core:is_empty(?MODULE, Tree).

%% @doc Create a tree from the given list of {Key, Value} pairs.
from_proplist(Proplist) ->
	ar_patricia_tree_core:from_proplist(?MODULE, Proplist).

%% @doc Delete the given key.
delete(Key, Tree) ->
	ar_patricia_tree_core:delete(?MODULE, Key, Tree).

%% @doc Return the list of up to Count key-value tuples collected by traversing the keys
%% in the alphabetical order. The keys in the returned list are sorted in the descending
%% order.
get_range(Count, Tree) ->
	ar_patricia_tree_core:get_range(?MODULE, Count, Tree).

%% @doc Return the list of up to Count key-value tuples collected by traversing the keys
%% in the alphabetical order starting from the Start key. The keys in the returned list
%% are sorted in the descending order. If Start is not a key or Count is not positive,
%% return an empty list.
get_range(Start, Count, Tree) ->
	ar_patricia_tree_core:get_range(?MODULE, Start, Count, Tree).

%% @doc Start recording node writes so a later snapshot_restore/1 returns the table to its
%% current bytes. The recording lives in the process dictionary, so the single owning process
%% can wrap a transient excursion - positioning the tree at another representation to hash or
%% traverse it - and undo it afterwards. Only one snapshot may be active at a time. The
%% returned bytes are identical to the originals, cached node hashes included, so the restore
%% leaves no node dirty. Crashes if a snapshot is already active. That should not happen in
%% practice - nesting would silently corrupt the restore.
snapshot_begin(_Tree) ->
	undefined = erlang:get(?SNAPSHOT_KEY),
	erlang:put(?SNAPSHOT_KEY, #{}),
	ok.

%% @doc Restore every key written since snapshot_begin/1 to its pre-snapshot value (or absence)
%% and stop recording.
snapshot_restore(Tree) ->
	case erlang:erase(?SNAPSHOT_KEY) of
		undefined ->
			ok;
		Recorded ->
			maps:foreach(
				fun	(Key, absent) ->
						ets:delete(Tree, Key);
					(_Key, {value, Object}) ->
						ets:insert(Tree, Object)
				end,
				Recorded),
			ok
	end.

%%%===================================================================
%%% ar_patricia_tree_core backend.
%%%===================================================================

%% @doc Look up a node by key, returning the node tuple or not_found.
get_node(Tree, Key) ->
	case ets:lookup(Tree, Key) of
		[{Key, Node}] ->
			Node;
		[] ->
			not_found
	end.

%% @doc Insert {Key, Node}, first recording the key's pre-snapshot value if a snapshot is
%% active. All node and size writes go through this so they can be undone by snapshot_restore/1.
put_node(Tree, Key, Node) ->
	record(Tree, Key),
	ets:insert(Tree, {Key, Node}),
	Tree.

%% @doc Delete a node, first recording its pre-snapshot value if a snapshot is active.
del_node(Tree, Key) ->
	record(Tree, Key),
	ets:delete(Tree, Key),
	Tree.

get_size(Tree) ->
	ets:lookup_element(Tree, ?SIZE_KEY, 2).

set_size(Tree, Size) ->
	put_node(Tree, ?SIZE_KEY, Size).

%% Stream one node's update to the sink (a no-op when Sink is undefined). Acc is the sink,
%% threaded through unchanged. Called once per re-hashed node, so it also counts them for the
%% rehashed_nodes element of compute_hash/3's result.
emit(Sink, Hash, KeyPrefix, Value) ->
	erlang:put(pt_rehashed_count, erlang:get(pt_rehashed_count) + 1),
	maybe_persist(Sink, Hash, KeyPrefix, Value),
	Sink.

progress_extra(Tree) ->
	io_lib:format(" ets=~BMB", [ets_mem_mb(Tree)]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Record a key's pre-snapshot value the first time it is written within a snapshot. A
%% no-op when no snapshot is active.
record(Tree, Key) ->
	case erlang:get(?SNAPSHOT_KEY) of
		undefined ->
			ok;
		Recorded ->
			case maps:is_key(Key, Recorded) of
				true ->
					ok;
				false ->
					Original =
						case ets:lookup(Tree, Key) of
							[Object] ->
								{value, Object};
							[] ->
								absent
						end,
					erlang:put(?SNAPSHOT_KEY, Recorded#{ Key => Original }),
					ok
			end
	end.

%% Persistence buffer (process dictionary): node updates are accumulated in the hashing
%% process and shipped to the Sink process in batches of ?PERSIST_BATCH_SIZE, so the hashing
%% process holds at most one batch and never blocks on storage I/O. Sink is the PID of a
%% process implementing the node-batch protocol (ar_storage on a live node, a collector in
%% tests/benchmarks), or undefined for no persistence.
persist_init(undefined) ->
	ok;
persist_init(_Sink) ->
	erlang:put(pt_persist_buf, {0, []}),
	ok.

persist_flush(undefined) ->
	ok;
persist_flush(Sink) ->
	{_N, Buf} = erlang:get(pt_persist_buf),
	send_batch(Sink, Buf),
	erlang:erase(pt_persist_buf),
	ok.

%% @doc Buffer one account tree node update and send a batch to the given PID when
%% the buffer is full. The values are sent unserialized.
maybe_persist(undefined, _Hash, _KeyPrefix, _Value) ->
	ok;
maybe_persist(Sink, Hash, KeyPrefix, Value) ->
	Prefix = case KeyPrefix of root -> <<>>; _ -> KeyPrefix end,
	DBKey = << Hash/binary, Prefix/binary >>,
	{N, Buf} = erlang:get(pt_persist_buf),
	Buf2 = [{DBKey, Value} | Buf],
	case N + 1 >= ?PERSIST_BATCH_SIZE of
		true ->
			send_batch(Sink, Buf2),
			erlang:put(pt_persist_buf, {0, []});
		false ->
			erlang:put(pt_persist_buf, {N + 1, Buf2})
	end,
	ok.

%% @doc Send a batch of {Key, Value} node updates to the given process for persistence.
send_batch(_Sink, []) ->
	ok;
send_batch(Sink, Batch) ->
	Sink ! {account_tree_node_batch, Batch},
	ok.

ets_mem_mb(Tree) ->
	(ets:info(Tree, memory) * erlang:system_info(wordsize)) div (1024 * 1024).
