%%% @doc The ETS-backed implementation of ar_patricia_tree (see there for the structure and why a
%%% patricia trie). The whole trie lives in the shared, named ETS table ar_patricia_tree,
%%% created by ar_sup at startup. Every operation mutates that table in place and returns the
%%% table id; new/0 resets it to an empty tree. So exactly one account tree is materialized at a
%%% time - the diff-DAG "sink" in ar_account_tree.
-module(ar_patricia_tree_ets).

%% size/1 is a tree accessor here; keep it from clashing with erlang:size/1.
-compile({no_auto_import, [size/1]}).

-export([new/0, delete_table/1, insert/3, get/2, size/1, compute_hash/2, compute_hash/3,
		foldr/3, is_empty/1, from_proplist/1, delete/2, get_range/2, get_range/3,
		snapshot_begin/1, snapshot_restore/1]).

%% Distinct from any node key (node keys are the atom 'root' or binaries).
-define(SIZE_KEY, '$size').

%% Diagnostic: log a progress line every this many leaves hashed (see progress_* below).
-define(PROGRESS_CHUNK, 1000000).

%% When KV is set, buffer this many node updates in the hashing process and
%% flush them to RocksDB as a single write batch (bounded memory, amortized I/O).
-define(PERSIST_BATCH_SIZE, 5000).

%% Process-dictionary key under which an active snapshot records the pre-snapshot value of
%% every node it overwrites. See snapshot_begin/1.
-define(SNAPSHOT_KEY, '$snapshot').

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Reset the shared ar_patricia_tree table to an empty tree and return its id.
%% The table is normally created by ar_sup; create it here too so the module is usable
%% standalone (e.g. in benchmarks run outside the supervision tree).
new() ->
	Tid =
		case ets:whereis(ar_patricia_tree) of
			undefined ->
				ets:new(ar_patricia_tree, [set, public, named_table]);
			_ ->
				ar_patricia_tree
		end,
	ets:delete_all_objects(Tid),
	ets:insert(Tid, {?SIZE_KEY, 0}),
	ets:insert(Tid, {root, {no_parent, gb_sets:new(), no_hash, no_prefix, no_value}}),
	Tid.

%% @doc Delete the underlying ETS table. Mainly for standalone use (e.g. benchmark
%% teardown); under ar_sup the ar_patricia_tree table is supervisor-owned and normally
%% persists for the lifetime of the node - prefer new/0 to reset it instead.
delete_table(Tree) ->
	ets:delete(Tree),
	ok.

%% @doc Insert the given value under the given binary key.
insert(Key, Value, Tree) when is_binary(Key) ->
	insert(Key, Value, Tree, 1, root).

%% @doc Get the value stored under the given key or not_found.
get(Key, Tree) when is_binary(Key) ->
	case get(Key, Tree, 1) of
		{_, {_, _, _, _, {v, Value}}} ->
			Value;
		not_found ->
			not_found
	end;
get(_Key, _Tree) ->
	not_found.

%% @doc Return the number of values in the tree.
size(Tree) ->
	ets:lookup_element(Tree, ?SIZE_KEY, 2).

%% HashFun has two clauses: HashFun(leaf, {Key, Value}) hashes a leaf, HashFun(node,
%% ChildHashes) combines sibling hashes. The third element is always #{} (this impl does
%% not build an UpdateMap); with PersistOpts #{ sink => PID } each node update is streamed
%% to that process in batches instead (see maybe_persist/4).
-spec compute_hash(ets:tid(), fun()) -> {binary(), ets:tid(), map()}.
compute_hash(Tree, HashFun) ->
	compute_hash(Tree, HashFun, #{}).

-spec compute_hash(ets:tid(), fun(), map()) -> {binary(), ets:tid(), map()}.
compute_hash(Tree, HashFun, PersistOpts) ->
	case size(Tree) of
		0 ->
			{<<>>, Tree, #{}};
		_ ->
			Sink = maps:get(sink, PersistOpts, undefined),
			persist_init(Sink),
			progress_init(Tree),
			RootHash = do_compute_hash(Tree, HashFun, Sink, root),
			progress_finish(Tree),
			persist_flush(Sink),
			{RootHash, Tree, #{}}
	end.

%% @doc Traverse the keys in the reversed alphabetical order iteratively applying
%% the given function of a key, a value, and an accumulator.
foldr(Fun, Acc, Tree) ->
	case is_empty(Tree) of
		true ->
			Acc;
		false ->
			foldr(Fun, Acc, Tree, root)
	end.

%% @doc Return true if the tree stores no values.
is_empty(Tree) ->
	size(Tree) == 0.

%% @doc Create a tree from the given list of {Key, Value} pairs.
from_proplist(Proplist) ->
	lists:foldl(
		fun({Key, Value}, Acc) -> insert(Key, Value, Acc) end,
		new(),
		Proplist
	).

%% @doc Delete the given key.
delete(Key, Tree) ->
	delete(Key, Tree, 1).

%% @doc Return the list of up to Count key-value tuples collected by traversing the keys
%% in the alphabetical order. The keys in the returned list are sorted in the descending
%% order.
get_range(Count, Tree) ->
	Iterator = iterator(Tree),
	get_range(Iterator, Count, 0, []).

%% @doc Return the list of up to Count key-value tuples collected by traversing the keys
%% in the alphabetical order starting from the Start key. The keys in the returned list
%% are sorted in the descending order. If Start is not a key or Count is not positive,
%% return an empty list.
get_range(Start, Count, Tree) when is_binary(Start) ->
	Iterator = iterator_from(Start, Tree),
	get_range(Iterator, Count, 0, []);
get_range(_, _, _) ->
	[].

%% @doc Start recording node writes so a later snapshot_restore/1 returns the table to its
%% current bytes. The recording lives in the process dictionary, so the single owning process
%% can wrap a transient excursion - positioning the tree at another representation to hash or
%% traverse it - and undo it afterwards. Only one snapshot may be active at a time. The
%% returned bytes are identical to the originals, cached node hashes included, so the restore
%% leaves no node dirty.
snapshot_begin(_Tree) ->
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
%%% Private functions.
%%%===================================================================

%% @doc Insert {Key, Node}, first recording the key's pre-snapshot value if a snapshot is
%% active. All node and size writes go through this so they can be undone by snapshot_restore/1.
put_node(Tree, Key, Node) ->
	record(Tree, Key),
	ets:insert(Tree, {Key, Node}),
	ok.

%% @doc Delete a node, first recording its pre-snapshot value if a snapshot is active.
del_node(Tree, Key) ->
	record(Tree, Key),
	ets:delete(Tree, Key),
	ok.

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

%% @doc Look up a node by key, returning the node tuple or not_found.
ets_get(Tree, Key) ->
	case ets:lookup(Tree, Key) of
		[{Key, Node}] ->
			Node;
		[] ->
			not_found
	end.

set_size(Tree, Size) ->
	put_node(Tree, ?SIZE_KEY, Size),
	Tree.

insert(Key, Value, Tree, Level, Parent) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case ets_get(Tree, KeyPrefix) of
		{NodeParent, NodeChildren, NodeHash, NodeSuffix, NodeValue} ->
			{Common, KeySuffix2, NodeSuffix2} = join(KeySuffix, NodeSuffix),
			case {KeySuffix == NodeSuffix, Common == KeySuffix, Common == NodeSuffix} of
				{true, _, _} ->
					Size = size(Tree),
					Size2 =
						case NodeValue of
							no_value ->
								Size + 1;
							_ ->
								Size
						end,
					UpdatedNode = {NodeParent, NodeChildren, no_hash, NodeSuffix, {v, Value}},
					put_node(Tree, KeyPrefix, UpdatedNode),
					set_size(Tree, Size2),
					invalidate_hash(NodeParent, Tree);
				{_, _, true} when KeySuffix > NodeSuffix ->
					insert(Key, Value, Tree, Level + byte_size(NodeSuffix) + 1, KeyPrefix);
				{_, true, _} when KeySuffix < NodeSuffix ->
					{Head, NodeSuffix3} = strip_head(NodeSuffix2),
					UpdatedNodeKey = << KeyPrefix/binary, Common/binary, Head/binary >>,
					PivotChildren = gb_sets:from_list([UpdatedNodeKey]),
					PivotNode = {NodeParent, PivotChildren, no_hash, KeySuffix, {v, Value}},
					UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
					Size = size(Tree),
					put_node(Tree, KeyPrefix, PivotNode),
					put_node(Tree, UpdatedNodeKey, UpdatedNode),
					set_size(Tree, Size + 1),
					update_children_parent(UpdatedNodeKey, NodeChildren, Tree),
					invalidate_hash(NodeParent, Tree);
				{false, false, false} ->
					{KeyHead, KeySuffix3} = strip_head(KeySuffix2),
					NewNodeKey = << KeyPrefix/binary, Common/binary, KeyHead/binary >>,
					NewNode = {KeyPrefix, gb_sets:new(), no_hash, KeySuffix3, {v, Value}},
					{NodeKeyHead, NodeSuffix3} = strip_head(NodeSuffix2),
					UpdatedNodeKey = << KeyPrefix/binary, Common/binary, NodeKeyHead/binary >>,
					UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
					PivotChildren = gb_sets:from_list([NewNodeKey, UpdatedNodeKey]),
					PivotNode = {NodeParent, PivotChildren, no_hash, Common, no_value},
					Size = size(Tree),
					put_node(Tree, NewNodeKey, NewNode),
					put_node(Tree, UpdatedNodeKey, UpdatedNode),
					put_node(Tree, KeyPrefix, PivotNode),
					set_size(Tree, Size + 1),
					update_children_parent(UpdatedNodeKey, NodeChildren, Tree),
					invalidate_hash(NodeParent, Tree)
			end;
		not_found ->
			NewNode = {Parent, gb_sets:new(), no_hash, KeySuffix, {v, Value}},
			{NextParent, Children, _Hash, NextSuffix, ParentValue} = ets_get(Tree, Parent),
			UpdatedChildren = gb_sets:insert(KeyPrefix, Children),
			Size = size(Tree),
			put_node(Tree, KeyPrefix, NewNode),
			put_node(Tree, Parent, {NextParent, UpdatedChildren, no_hash, NextSuffix,
					ParentValue}),
			set_size(Tree, Size + 1),
			invalidate_hash(NextParent, Tree)
	end.

split_by_pos(<<>>, _Pos) ->
	{<<>>, <<>>};
split_by_pos(Binary, Pos) ->
	{binary:part(Binary, {0, Pos}), binary:part(Binary, {Pos, byte_size(Binary) - Pos})}.

join(Binary1, Binary2) ->
	%% Return the longest common prefix and the diverged suffixes of the two binaries.
	PrefixLen = binary:longest_common_prefix([Binary1, Binary2]),
	Prefix = binary:part(Binary1, {0, PrefixLen}),
	Suffix1 = binary:part(Binary1, {PrefixLen, byte_size(Binary1) - PrefixLen}),
	Suffix2 = binary:part(Binary2, {PrefixLen, byte_size(Binary2) - PrefixLen}),
	{Prefix, Suffix1, Suffix2}.

update_children_parent(Key, Children, Tree) ->
	gb_sets:fold(
		fun(ChildKey, Acc) ->
			{_, C, H, S, V} = ets_get(Acc, ChildKey),
			put_node(Acc, ChildKey, {Key, C, H, S, V}),
			Acc
		end,
		Tree,
		Children
	).

invalidate_hash(no_parent, Tree) ->
	Tree;
invalidate_hash(Key, Tree) ->
	{Parent, Children, _Hash, Suffix, Value} = ets_get(Tree, Key),
	put_node(Tree, Key, {Parent, Children, no_hash, Suffix, Value}),
	invalidate_hash(Parent, Tree).

strip_head(Binary) ->
	{binary:part(Binary, {0, 1}), binary:part(Binary, {1, byte_size(Binary) - 1})}.

get(Key, Tree, Level) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case ets_get(Tree, KeyPrefix) of
		not_found ->
			not_found;
		{_, _, _, Suffix, MaybeValue} = NodeData ->
			Len = binary:longest_common_prefix([KeySuffix, Suffix]),
			SuffixSize = byte_size(Suffix),
			case Len < SuffixSize of
				true ->
					not_found;
				false ->
					case KeySuffix == Suffix of
						false ->
							get(Key, Tree, Level + SuffixSize + 1);
						true ->
							case MaybeValue of
								no_value ->
									not_found;
								{v, _Value} ->
									{KeyPrefix, NodeData}
							end
					end
			end
	end.

%% Recursive worker. The tree is mutated in place, so it returns only the node hash. When
%% Sink is a PID each node's {Hash, KeyPrefix} => Value update is streamed to this PID.
do_compute_hash(Tree, HashFun, Sink, KeyPrefix) ->
	{Parent, Children, Hash, Suffix, MaybeValue} = ets_get(Tree, KeyPrefix),
	case Hash of
		no_hash ->
			case gb_sets:is_empty(Children) of
				true ->
					{v, Value} = MaybeValue,
					Key = << KeyPrefix/binary, Suffix/binary >>,
					NewHash = HashFun(leaf, {Key, Value}),
					progress_tick(Tree),
					put_node(Tree, KeyPrefix,
							{Parent, gb_sets:new(), NewHash, Suffix, {v, Value}}),
					maybe_persist(Sink, NewHash, KeyPrefix, {Key, Value}),
					NewHash;
				false ->
					Hashes = gb_sets_foldr(
						fun(Child, HashesAcc) ->
							ChildHash = do_compute_hash(Tree, HashFun, Sink, Child),
							[{ChildHash, Child} | HashesAcc]
						end,
						[],
						Children
					),
					NewHash =
						case MaybeValue of
							{v, Value} ->
								Key = << KeyPrefix/binary, Suffix/binary >>,
								NewHash2 = HashFun(leaf, {Key, Value}),
								Hashes2 = [H || {H, _} <- Hashes],
								NewHash3 = HashFun(node, [NewHash2 | Hashes2]),
								maybe_persist(Sink, NewHash2, KeyPrefix, {Key, Value}),
								maybe_persist(Sink, NewHash3, KeyPrefix,
										[{NewHash2, KeyPrefix} | Hashes]),
								NewHash3;
							no_value ->
								case Hashes of
									[{SingleHash, _}] ->
										maybe_persist(Sink, SingleHash, KeyPrefix,
												Hashes),
										SingleHash;
									_ ->
										Hashes2 = [H || {H, _} <- Hashes],
										NewHash2 = HashFun(node, Hashes2),
										maybe_persist(Sink, NewHash2, KeyPrefix, Hashes),
										NewHash2
								end
						end,
					put_node(Tree, KeyPrefix, {Parent, Children, NewHash, Suffix, MaybeValue}),
					NewHash
			end;
		_ ->
			Hash
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

%% @doc Diagnostic progress logging for compute_hash, gated by the AR_PATRICIA_PROGRESS
%% environment variable (off by default). When enabled, logs a line every ?PROGRESS_CHUNK
%% leaves with elapsed/throughput, total heap memory, GCs since start, and the ETS table
%% memory - intended to pinpoint the non-linear slowdown on large trees. Uses the process
%% dictionary; compute_hash runs in a single process.
progress_init(Tree) ->
	case os:getenv("AR_PATRICIA_PROGRESS") of
		V when V == false; V == ""; V == "0"; V == "false" ->
			erlang:erase(pt_progress),
			erlang:erase(pt_progress_gc0);
		_ ->
			Now = erlang:monotonic_time(millisecond),
			{GCs, _, _} = erlang:statistics(garbage_collection),
			erlang:put(pt_progress, {0, Now, Now}),
			erlang:put(pt_progress_gc0, GCs),
			io:format("[ar_patricia_tree_ets progress] start mem=~BMB ets=~BMB~n",
					[erlang:memory(total) div (1024 * 1024), ets_mem_mb(Tree)])
	end.

progress_tick(Tree) ->
	case erlang:get(pt_progress) of
		undefined ->
			ok;
		{Count, T0, TLast} ->
			Count2 = Count + 1,
			case Count2 rem ?PROGRESS_CHUNK of
				0 ->
					Now = erlang:monotonic_time(millisecond),
					{GCs, _, _} = erlang:statistics(garbage_collection),
					GC0 = erlang:get(pt_progress_gc0),
					ChunkMs = max(1, Now - TLast),
					io:format("[ar_patricia_tree_ets progress] leaves=~B total=~Bms chunk=~Bms "
							"rate=~B/s mem=~BMB gcs=~B ets=~BMB~n",
							[Count2, Now - T0, Now - TLast,
							 (?PROGRESS_CHUNK * 1000) div ChunkMs,
							 erlang:memory(total) div (1024 * 1024), GCs - GC0, ets_mem_mb(Tree)]),
					erlang:put(pt_progress, {Count2, T0, Now});
				_ ->
					erlang:put(pt_progress, {Count2, T0, TLast})
			end
	end.

progress_finish(Tree) ->
	case erlang:get(pt_progress) of
		undefined ->
			ok;
		{Count, T0, _} ->
			Now = erlang:monotonic_time(millisecond),
			{GCs, _, _} = erlang:statistics(garbage_collection),
			GC0 = erlang:get(pt_progress_gc0),
			io:format("[ar_patricia_tree_ets progress] done leaves=~B total=~Bms mem=~BMB "
					"gcs=~B ets=~BMB~n",
					[Count, Now - T0, erlang:memory(total) div (1024 * 1024), GCs - GC0,
					 ets_mem_mb(Tree)]),
			erlang:erase(pt_progress),
			erlang:erase(pt_progress_gc0)
	end.

ets_mem_mb(Tree) ->
	(ets:info(Tree, memory) * erlang:system_info(wordsize)) div (1024 * 1024).

foldr(Fun, Acc, Tree, KeyPrefix) ->
	{_, Children, _, Suffix, MaybeValue} = ets_get(Tree, KeyPrefix),
	case gb_sets:is_empty(Children) of
		true ->
			{v, Value} = MaybeValue,
			Key = << KeyPrefix/binary, Suffix/binary >>,
			Fun(Key, Value, Acc);
		false ->
			Acc2 = gb_sets_foldr(
				fun(Child, ChildrenAcc) ->
					foldr(Fun, ChildrenAcc, Tree, Child)
				end,
				Acc,
				Children
			),
			case MaybeValue of
				{v, Value} ->
					Key = << KeyPrefix/binary, Suffix/binary >>,
					Fun(Key, Value, Acc2);
				_ ->
					Acc2
			end
	end.

gb_sets_foldr(Fun, Acc, G) ->
	lists:foldr(Fun, Acc, gb_sets:to_list(G)).

delete(Key, Tree, Level) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case ets_get(Tree, KeyPrefix) of
		not_found ->
			Tree;
		{Parent, Children, _Hash, Suffix, MaybeValue} ->
			Len = binary:longest_common_prefix([KeySuffix, Suffix]),
			SuffixSize = byte_size(Suffix),
			case Len < SuffixSize of
				true ->
					Tree;
				false ->
					case KeySuffix == Suffix of
						false ->
							delete(Key, Tree, Level + SuffixSize + 1);
						true ->
							case MaybeValue of
								no_value ->
									Tree;
								_ ->
									Size = size(Tree),
									set_size(Tree, Size - 1),
									case gb_sets:is_empty(Children) of
										true ->
											delete2(KeyPrefix, Parent, Tree);
										false ->
											Node2 = {Parent, Children, no_hash, Suffix,
													no_value},
											put_node(Tree, KeyPrefix, Node2),
											invalidate_hash(Parent, Tree)
									end
							end
					end
			end
	end.

delete2(Key, Parent, Tree) ->
	del_node(Tree, Key),
	{ParentParent, ParentChildren, _Hash, Suffix, ParentValue} = ets_get(Tree, Parent),
	ParentChildren2 = gb_sets:del_element(Key, ParentChildren),
	put_node(Tree, Parent, {ParentParent, ParentChildren2, no_hash, Suffix, ParentValue}),
	case {Parent == root, gb_sets:is_empty(ParentChildren2), ParentValue} of
		{false, true, no_value} ->
			delete2(Parent, ParentParent, Tree);
		_ ->
			invalidate_hash(ParentParent, Tree)
	end.

iterator(Tree) ->
	iterator(Tree, root).

iterator(Tree, Key) ->
	{_, Children, _, _, MaybeValue} = NodeData = ets_get(Tree, Key),
	case MaybeValue of
		{v, _Value} ->
			{{Key, NodeData}, Tree};
		no_value ->
			case gb_sets:is_empty(Children) of
				true ->
					none;
				false ->
					iterator(Tree, gb_sets:smallest(Children))
			end
	end.

iterator_from(Start, Tree) ->
	case get(Start, Tree, 1) of
		not_found ->
			none;
		{Prefix, NodeData} ->
			{{Prefix, NodeData}, Tree}
	end.

get_range(_Iterator, Count, Count, List) ->
	List;
get_range(Iterator, Count, Got, List) ->
	case next(Iterator) of
		none ->
			List;
		{{Key, Value}, UpdatedIterator} ->
			get_range(UpdatedIterator, Count, Got + 1, [{Key, Value} | List])
	end.

next({{Prefix, {Parent, Children, _Hash, Suffix, {v, Value}}}, Tree}) ->
	Key = << Prefix/binary, Suffix/binary >>,
	{{Key, Value}, get_next_start_from_children(Prefix, Parent, Children, Tree)};
next(none) ->
	none.

get_next_start_from_children(Key, Parent, Children, Tree) ->
	NextChild =
		case gb_sets:is_empty(Children) of
			true ->
				none;
			false ->
				Child = gb_sets:smallest(Children),
				{Child, ets_get(Tree, Child)}
		end,
	case NextChild of
		none ->
			get_next_start_from_sibling(Key, Parent, Tree);
		_ ->
			{ChildKey, {_, ChildChildren, _, _, MaybeValue}} = NextChild,
			case MaybeValue of
				no_value ->
					get_next_start_from_children(ChildKey, Key, ChildChildren, Tree);
				{v, _} ->
					{NextChild, Tree}
			end
	end.

get_next_start_from_sibling(root, no_parent, _Tree) ->
	none;
get_next_start_from_sibling(Key, Parent, Tree) ->
	{ParentParent, Children, _, _, _} = ets_get(Tree, Parent),
	Iterator = gb_sets:iterator_from(Key, Children),
	Start =
		case gb_sets:next(Iterator) of
			none ->
				none;
			{Key, UpdatedIterator} ->
				gb_sets:next(UpdatedIterator);
			Next ->
				Next
		end,
	case Start of
		none ->
			get_next_start_from_sibling(Parent, ParentParent, Tree);
		{NextSiblingKey, _} ->
			NextSibling = ets_get(Tree, NextSiblingKey),
			{_, NextSiblingChildren, _, _, MaybeValue} = NextSibling,
			case MaybeValue of
				no_value ->
					get_next_start_from_children(NextSiblingKey, Key, NextSiblingChildren,
							Tree);
				{v, _} ->
					{{NextSiblingKey, NextSibling}, Tree}
			end
	end.
