%%% @doc A faithful copy of the pre-ETS `ar_patricia_tree` implementation (the one on
%%% master), kept around so the account-tree benchmark can compare it head-to-head with the
%%% current `ar_patricia_tree` in a single branch (tree_repr => legacy).
-module(ar_patricia_tree_legacy).

-export([new/0, insert/3, get/2, size/1, compute_hash/2, compute_hash/3, foldr/3, is_empty/1,
		from_proplist/1, delete/2, get_range/2, get_range/3]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return a new tree.
new() ->
	#{ root => {no_parent, gb_sets:new(), no_hash, no_prefix, no_value}, size => 0 }.

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
	maps:get(size, Tree).

compute_hash(Tree, HashFun) ->
	compute_hash(Tree, HashFun, #{ return_update_map => true }).

%% @doc Compute the root hash by recursively hashing the tree values. HashFun(leaf,
%% {Key, Value}) hashes a leaf, HashFun(node, ChildHashes) combines siblings. Returns
%% {RootHash, Tree, UpdateMap}; UpdateMap is populated only when PersistOpts has
%% return_update_map => true.
compute_hash(#{ size := 0 } = Tree, _HashFun, _PersistOpts) ->
	{<<>>, Tree, #{}};
compute_hash(Tree, HashFun, PersistOpts) ->
	Persist =
		case maps:get(return_update_map, PersistOpts, false) of
			true -> return_update_map;
			false -> false
		end,
	do_compute_hash(Tree, HashFun, Persist, root, #{}).

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
	maps:get(size, Tree) == 0.

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
%% in the alphabetical order. The keys in the returned list are sorted in the descending order.
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

%%%===================================================================
%%% Private functions.
%%%===================================================================

insert(Key, Value, Tree, Level, Parent) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case maps:get(KeyPrefix, Tree, not_found) of
		{NodeParent, NodeChildren, NodeHash, NodeSuffix, NodeValue} ->
			{Common, KeySuffix2, NodeSuffix2} = join(KeySuffix, NodeSuffix),
			case {KeySuffix == NodeSuffix, Common == KeySuffix, Common == NodeSuffix} of
				{true, _, _} ->
					Size = maps:get(size, Tree),
					Size2 =
						case NodeValue of
							no_value ->
								Size + 1;
							_ ->
								Size
						end,
					UpdatedNode = {NodeParent, NodeChildren, no_hash, NodeSuffix, {v, Value}},
					invalidate_hash(NodeParent,
							Tree#{ KeyPrefix => UpdatedNode, size => Size2 });
				{_, _, true} when KeySuffix > NodeSuffix ->
					insert(Key, Value, Tree, Level + byte_size(NodeSuffix) + 1, KeyPrefix);
				{_, true, _} when KeySuffix < NodeSuffix ->
					{Head, NodeSuffix3} = strip_head(NodeSuffix2),
					UpdatedNodeKey = << KeyPrefix/binary, Common/binary, Head/binary >>,
					PivotChildren = gb_sets:from_list([UpdatedNodeKey]),
					PivotNode = {NodeParent, PivotChildren, no_hash, KeySuffix, {v, Value}},
					UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
					Size = maps:get(size, Tree),
					Tree2 = Tree#{
						KeyPrefix => PivotNode,
						UpdatedNodeKey => UpdatedNode,
						size => Size + 1
					},
					Tree3 = update_children_parent(UpdatedNodeKey, NodeChildren, Tree2),
					invalidate_hash(NodeParent, Tree3);
				{false, false, false} ->
					{KeyHead, KeySuffix3} = strip_head(KeySuffix2),
					NewNodeKey = << KeyPrefix/binary, Common/binary, KeyHead/binary >>,
					NewNode = {KeyPrefix, gb_sets:new(), no_hash, KeySuffix3, {v, Value}},
					{NodeKeyHead, NodeSuffix3} = strip_head(NodeSuffix2),
					UpdatedNodeKey = << KeyPrefix/binary, Common/binary, NodeKeyHead/binary >>,
					UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
					PivotChildren = gb_sets:from_list([NewNodeKey, UpdatedNodeKey]),
					PivotNode = {NodeParent, PivotChildren, no_hash, Common, no_value},
					Size = maps:get(size, Tree),
					Tree2 = Tree#{
						NewNodeKey => NewNode,
						UpdatedNodeKey => UpdatedNode,
						KeyPrefix => PivotNode,
						size => Size + 1
					},
					Tree3 = update_children_parent(UpdatedNodeKey, NodeChildren, Tree2),
					invalidate_hash(NodeParent, Tree3)
			end;
		not_found ->
			NewNode = {Parent, gb_sets:new(), no_hash, KeySuffix, {v, Value}},
			{NextParent, Children, _Hash, NextSuffix, ParentValue} = maps:get(Parent, Tree),
			UpdatedChildren = gb_sets:insert(KeyPrefix, Children),
			Size = maps:get(size, Tree),
			Tree2 = Tree#{
				KeyPrefix => NewNode,
				Parent => {NextParent, UpdatedChildren, no_hash, NextSuffix, ParentValue},
				size => Size + 1
			},
			invalidate_hash(NextParent, Tree2)
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
			{_, C, H, S, V} = maps:get(ChildKey, Acc),
			ChildNode2 = {Key, C, H, S, V},
			Acc#{ ChildKey => ChildNode2 }
		end,
		Tree,
		Children
	).

invalidate_hash(no_parent, Tree) ->
	Tree;
invalidate_hash(Key, Tree) ->
	{Parent, Children, _Hash, Suffix, Value} = maps:get(Key, Tree),
	InvalidatedHashNode = {Parent, Children, no_hash, Suffix, Value},
	invalidate_hash(Parent, Tree#{ Key => InvalidatedHashNode }).

strip_head(Binary) ->
	{binary:part(Binary, {0, 1}), binary:part(Binary, {1, byte_size(Binary) - 1})}.

get(Key, Tree, Level) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case maps:get(KeyPrefix, Tree, not_found) of
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

do_compute_hash(Tree, HashFun, Persist, KeyPrefix, UpdateMap) ->
	{Parent, Children, Hash, Suffix, MaybeValue} = maps:get(KeyPrefix, Tree),
	case Hash of
		no_hash ->
			case gb_sets:is_empty(Children) of
				true ->
					{v, Value} = MaybeValue,
					Key = << KeyPrefix/binary, Suffix/binary >>,
					NewHash = HashFun(leaf, {Key, Value}),
					NewTree = Tree#{
						KeyPrefix => {Parent, gb_sets:new(), NewHash, Suffix, {v, Value}}
					},
					UpdateMap2 = accumulate(Persist, {NewHash, KeyPrefix}, {Key, Value},
							UpdateMap),
					{NewHash, NewTree, UpdateMap2};
				false ->
					{Hashes, UpdatedTree, UpdateMap2} = gb_sets_foldr(
						fun(Child, {HashesAcc, TreeAcc, UpdateMapAcc}) ->
							{ChildHash, TreeAcc2, UpdateMapAcc2} = do_compute_hash(TreeAcc,
									HashFun, Persist, Child, UpdateMapAcc),
							{[{ChildHash, Child} | HashesAcc], TreeAcc2, UpdateMapAcc2}
						end,
						{[], Tree, UpdateMap},
						Children
					),
					{NewHash, UpdateMap3} =
						case MaybeValue of
							{v, Value} ->
								Key = << KeyPrefix/binary, Suffix/binary >>,
								NewHash2 = HashFun(leaf, {Key, Value}),
								Hashes2 = [H || {H, _} <- Hashes],
								NewHash3 = HashFun(node, [NewHash2 | Hashes2]),
								UpdateMapA = accumulate(Persist, {NewHash2, KeyPrefix},
										{Key, Value}, UpdateMap2),
								UpdateMapB = accumulate(Persist, {NewHash3, KeyPrefix},
										[{NewHash2, KeyPrefix} | Hashes], UpdateMapA),
								{NewHash3, UpdateMapB};
							no_value ->
								case Hashes of
									[{SingleHash, _}] ->
										{SingleHash, accumulate(Persist, {SingleHash, KeyPrefix},
												Hashes, UpdateMap2)};
									_ ->
										Hashes2 = [H || {H, _} <- Hashes],
										NewHash2 = HashFun(node, Hashes2),
										{NewHash2, accumulate(Persist, {NewHash2, KeyPrefix},
												Hashes, UpdateMap2)}
								end
						end,
					{NewHash, UpdatedTree#{
						KeyPrefix => {Parent, Children, NewHash, Suffix, MaybeValue}
					}, UpdateMap3}
			end;
		_ ->
			{Hash, Tree, UpdateMap}
	end.

%% @doc Record one node's {Hash, KeyPrefix} => Value update into the UpdateMap, or skip it.
accumulate(return_update_map, Key, Value, Map) ->
	Map#{ Key => Value };
accumulate(false, _Key, _Value, Map) ->
	Map.

foldr(Fun, Acc, Tree, KeyPrefix) ->
	{_, Children, _, Suffix, MaybeValue} = maps:get(KeyPrefix, Tree),
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

%% The master traversal: pull the largest child off the gb_set and recurse. This is the
%% behaviour the current ar_patricia_tree replaced with lists:foldr(gb_sets:to_list(...)).
gb_sets_foldr(Fun, Acc, G) ->
	case gb_sets:is_empty(G) of
		true ->
			Acc;
		false ->
			{Largest, G2} = gb_sets:take_largest(G),
			gb_sets_foldr(Fun, Fun(Largest, Acc), G2)
	end.

delete(Key, Tree, Level) ->
	{KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
	case maps:get(KeyPrefix, Tree, not_found) of
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
									Size = maps:get(size, Tree),
									Tree2 = Tree#{ size => Size - 1 },
									case gb_sets:is_empty(Children) of
										true ->
											delete2(KeyPrefix, Parent, Tree2);
										false ->
											Node2 = {Parent, Children, no_hash, Suffix,
													no_value},
											invalidate_hash(Parent,
													Tree2#{ KeyPrefix => Node2 })
									end
							end
					end
			end
	end.

delete2(Key, Parent, Tree) ->
	Tree2 = maps:remove(Key, Tree),
	{ParentParent, ParentChildren, _Hash, Suffix, ParentValue} = maps:get(Parent, Tree),
	ParentChildren2 = gb_sets:del_element(Key, ParentChildren),
	Tree3 = Tree2#{ Parent => {ParentParent, ParentChildren2, no_hash, Suffix, ParentValue} },
	case {Parent == root, gb_sets:is_empty(ParentChildren2), ParentValue} of
		{false, true, no_value} ->
			delete2(Parent, ParentParent, Tree3);
		_ ->
			invalidate_hash(ParentParent, Tree3)
	end.

iterator(Tree) ->
	iterator(Tree, root).

iterator(Tree, Key) ->
	{_, Children, _, _, MaybeValue} = NodeData = maps:get(Key, Tree),
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
				{Child, maps:get(Child, Tree)}
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
	{ParentParent, Children, _, _, _} = maps:get(Parent, Tree),
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
			NextSibling = maps:get(NextSiblingKey, Tree),
			{_, NextSiblingChildren, _, _, MaybeValue} = NextSibling,
			case MaybeValue of
				no_value ->
					get_next_start_from_children(NextSiblingKey, Key, NextSiblingChildren,
							Tree);
				{v, _} ->
					{{NextSiblingKey, NextSibling}, Tree}
			end
	end.
