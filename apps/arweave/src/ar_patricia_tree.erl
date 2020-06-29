%%% @doc An implementation of a tree closely resembling a merkle patricia tree.
-module(ar_patricia_tree).

-export([
	new/0,
	insert/3,
	get/2,
	size/1,
	compute_hash/2,
	foldr/3,
	is_empty/1,
	from_proplist/1,
	delete/2,
	get_range/2, get_range/3
]).

-include_lib("eunit/include/eunit.hrl").

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

%% @doc Compute the root hash by recursively hashing the tree values.
%% Each key value pair is hashed via the provided hash function. The hashes of the siblings
%% are combined using ar_deep_hash:hash/1. The keys are traversed in the alphabetical order.
compute_hash(#{ size := 0 } = Tree, _HashFun) ->
	{<<>>, Tree};
compute_hash(Tree, HashFun) ->
	compute_hash(Tree, HashFun, root).

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
		fun({Key, Value}, Acc) -> ar_patricia_tree:insert(Key, Value, Acc) end,
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
					invalidate_hash(NodeParent, Tree#{ KeyPrefix => UpdatedNode, size => Size2 });
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

compute_hash(Tree, HashFun, KeyPrefix) ->
	{Parent, Children, Hash, Suffix, MaybeValue} = maps:get(KeyPrefix, Tree),
	case Hash of
		no_hash ->
			case gb_sets:is_empty(Children) of
				true ->
					{v, Value} = MaybeValue,
					Key = << KeyPrefix/binary, Suffix/binary >>,
					NewHash = HashFun(Key, Value),
					NewTree = Tree#{
						KeyPrefix => {Parent, gb_sets:new(), NewHash, Suffix, {v, Value}}
					},
					{NewHash, NewTree};
				false ->
					{Hashes, UpdatedTree} = gb_sets_foldr(
						fun(Child, {HashesAcc, TreeAcc}) ->
							{ChildHash, TreeAcc2} = compute_hash(TreeAcc, HashFun, Child),
							{[ChildHash | HashesAcc], TreeAcc2}
						end,
						{[], Tree},
						Children
					),
					NewHash =
						case MaybeValue of
							{v, Value} ->
								Key = << KeyPrefix/binary, Suffix/binary >>,
								ar_deep_hash:hash([HashFun(Key, Value) | Hashes]);
							no_value ->
								case Hashes of
									[SingleHash] ->
										SingleHash;
									_ ->
										ar_deep_hash:hash(Hashes)
								end
						end,
					{NewHash, UpdatedTree#{
						KeyPrefix => {Parent, Children, NewHash, Suffix, MaybeValue}
					}}
			end;
		_ ->
			{Hash, Tree}
	end.

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
											Node2 = {Parent, Children, no_hash, Suffix, no_value},
											invalidate_hash(Parent, Tree2#{ KeyPrefix => Node2 })
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
					get_next_start_from_children(NextSiblingKey, Key, NextSiblingChildren, Tree);
				{v, _} ->
					{{NextSiblingKey, NextSibling}, Tree}
			end
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

trie_test() ->
	T1 = new(),
	?assertEqual(not_found, get(<<"aaa">>, T1)),
	?assertEqual(true, is_empty(T1)),
	HashFun = fun(K, V) -> crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>) end,
	?assertEqual(<<>>, element(1, compute_hash(T1, HashFun))),
	?assertEqual(true, is_empty(delete(<<"a">>, T1))),
	?assertEqual(0, ar_patricia_tree:size(T1)),
	?assertEqual([], get_range(1, T1)),
	?assertEqual([], get_range(<<>>, 1, T1)),
	?assertEqual([], get_range(0, T1)),
	?assertEqual([], get_range(<<>>, 0, T1)),
	?assertEqual([], get_range(<<"aaa">>, 10, T1)),
	%% a -> a -> 1
	%%      b -> 1
	T1_2 = insert(<<"ab">>, 1, insert(<<"aa">>, 1, T1)),
	?assertEqual(not_found, get(<<"a">>, T1_2)),
	T1_3 = delete(<<"ab">>, delete(<<"aa">>, T1_2)),
	?assertEqual(true, is_empty(T1_3)),
	?assertEqual(<<>>, element(1, compute_hash(T1_3, HashFun))),
	?assertEqual(true, is_empty(delete(<<"a">>, T1_3))),
	?assertEqual(not_found, get(<<"a">>, T1_3)),
	%% aaa -> 1
	T2 = insert(<<"aaa">>, 1, T1_3),
	?assertEqual(false, is_empty(T2)),
	?assertEqual(1, ar_patricia_tree:size(T2)),
	{H2, T2_2} = compute_hash(T2, HashFun),
	{H2_2, _} = compute_hash(T2_2, HashFun),
	?assertEqual(H2, H2_2),
	?assertEqual(1, get(<<"aaa">>, T2)),
	?assertEqual([], get_range(<<>>, 1, T2)),
	?assertEqual([{<<"aaa">>, 1}], get_range(1, T2)),
	?assertEqual([{<<"aaa">>, 1}], get_range(<<"aaa">>, 1, T2)),
	%% aa -> a -> 1
	%%       b -> 2
	T3 = insert(<<"aab">>, 2, T2),
	?assertEqual(2, ar_patricia_tree:size(T3)),
	{H3, _} = compute_hash(T3, HashFun),
	?assertNotEqual(H2, H3),
	{H3_2, _} = compute_hash(insert(<<"aaa">>, 1, insert(<<"aab">>, 2, new())), HashFun),
	?assertEqual(H3, H3_2),
	{H3_3, _} =
		compute_hash(
			insert(<<"aaa">>, 1, insert(<<"aab">>, 2, insert(<<"a">>, 3, new()))),
			HashFun
		),
	{H3_4, _} = compute_hash(insert(<<"a">>, 3, T3), HashFun),
	?assertEqual(H3_3, H3_4),
	?assertEqual(1, get(<<"aaa">>, T3)),
	?assertEqual(2, get(<<"aab">>, T3)),
	?assertEqual([{<<"aaa">>, 1}], get_range(<<"aaa">>, 1, T3)),
	?assertEqual([{<<"aaa">>, 1}], get_range(1, T3)),
	?assertEqual([{<<"aab">>, 2}, {<<"aaa">>, 1}], get_range(<<"aaa">>, 2, T3)),
	?assertEqual([{<<"aab">>, 2}, {<<"aaa">>, 1}], get_range(2, T3)),
	?assertEqual([{<<"aab">>, 2}, {<<"aaa">>, 1}], get_range(<<"aaa">>, 20, T3)),
	?assertEqual([{<<"aab">>, 2}, {<<"aaa">>, 1}], get_range(20, T3)),
	?assertEqual([], get_range(<<"a">>, 2, T3)),
	?assertEqual([], get_range(<<"aa">>, 2, T3)),
	?assertEqual([{<<"aab">>, 2}], get_range(<<"aab">>, 2, T3)),
	?assertEqual([], get_range(<<"aac">>, 2, T3)),
	?assertEqual([], get_range(<<"b">>, 2, T3)),
	T4 = insert(<<"aab">>, 3, T3),
	?assertEqual(2, ar_patricia_tree:size(T4)),
	{H4, _} = compute_hash(T4, HashFun),
	?assertNotEqual(H3, H4),
	?assertEqual(1, get(<<"aaa">>, T4)),
	?assertEqual(3, get(<<"aab">>, T4)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	T5 = insert(<<"ab">>, 2, T4),
	?assertEqual(3, ar_patricia_tree:size(T5)),
	?assertEqual(1, gb_sets:size(element(2, maps:get(root, T5)))),
	{H5, _} = compute_hash(T5, HashFun),
	?assertNotEqual(H4, H5),
	{H5_2, _} =
		compute_hash(
			insert(<<"aab">>, 3, insert(<<"aaa">>, 1, insert(<<"ab">>, 2, new()))),
			HashFun
		),
	?assertEqual(H5, H5_2),
	{_H5_3, T5_2} = compute_hash(insert(<<"aaa">>, 1, new()), HashFun),
	{_H5_4, T5_3} = compute_hash(insert(<<"ab">>, 2, T5_2), HashFun),
	{H5_5, _T5_4} = compute_hash(insert(<<"aab">>, 3, T5_3), HashFun),
	?assertEqual(H5, H5_5),
	?assertEqual(1, get(<<"aaa">>, T5)),
	?assertEqual(3, get(<<"aab">>, T5)),
	?assertEqual(2, get(<<"ab">>, T5)),
	?assertEqual([{<<"ab">>, 2}, {<<"aab">>, 3}], get_range(<<"aab">>, 20, T5)),
	?assertEqual([{<<"aab">>, 3}, {<<"aaa">>, 1}], get_range(2, T5)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	T6 = insert(<<"abc">>, 4, T5),
	?assertEqual(4, ar_patricia_tree:size(T6)),
	?assertEqual(1, gb_sets:size(element(2, maps:get(root, T6)))),
	?assertEqual(1, get(<<"aaa">>, T6)),
	?assertEqual(3, get(<<"aab">>, T6)),
	?assertEqual(2, get(<<"ab">>, T6)),
	?assertEqual(4, get(<<"abc">>, T6)),
	?assertEqual([{<<"abc">>, 4}, {<<"ab">>, 2}, {<<"aab">>, 3}], get_range(<<"aab">>, 20, T6)),
	?assertEqual([{<<"abc">>, 4}], get_range(<<"abc">>, 20, T6)),
	?assertEqual([{<<"abc">>, 4}, {<<"ab">>, 2}], get_range(<<"ab">>, 20, T6)),
	?assertEqual(
		[{<<"abc">>, 4}, {<<"ab">>, 2}, {<<"aab">>, 3}, {<<"aaa">>, 1}],
		get_range(20, T6)
	),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% bcdefj -> 4
	T7 = insert(<<"bcdefj">>, 4, T6),
	?assertEqual(5, ar_patricia_tree:size(T7)),
	?assertEqual(2, gb_sets:size(element(2, maps:get(root, T7)))),
	?assertEqual(1, get(<<"aaa">>, T7)),
	?assertEqual(3, get(<<"aab">>, T7)),
	?assertEqual(4, get(<<"abc">>, T7)),
	?assertEqual(4, get(<<"bcdefj">>, T7)),
	?assertEqual([{<<"bcdefj">>, 4}, {<<"abc">>, 4}, {<<"ab">>, 2}], get_range(<<"ab">>, 3, T7)),
	?assertEqual([], get_range(0, T7)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% bcd -> efj -> 4
	%%        bcd -> 5
	T8 = insert(<<"bcdbcd">>, 5, T7),
	?assertEqual(6, ar_patricia_tree:size(T8)),
	?assertEqual(4, get(<<"bcdefj">>, T8)),
	?assertEqual(5, get(<<"bcdbcd">>, T8)),
	T9 = insert(<<"bcdbcd">>, 6, T8),
	?assertEqual(6, ar_patricia_tree:size(T9)),
	?assertEqual(4, get(<<"bcdefj">>, T9)),
	?assertEqual(6, get(<<"bcdbcd">>, T9)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% bab -> 7
	%% bcd -> efj -> 4
	%%        bcd -> 6
	T10 = insert(<<"bab">>, 7, T9),
	?assertEqual(7, ar_patricia_tree:size(T10)),
	?assertEqual(1, get(<<"aaa">>, T10)),
	?assertEqual(3, get(<<"aab">>, T10)),
	?assertEqual(4, get(<<"abc">>, T10)),
	?assertEqual(4, get(<<"bcdefj">>, T10)),
	?assertEqual(6, get(<<"bcdbcd">>, T10)),
	?assertEqual(7, get(<<"bab">>, T10)),
	?assertEqual(
		[
			{<<"aaa">>, 1}, {<<"aab">>, 3}, {<<"ab">>, 2}, {<<"abc">>, 4}, {<<"bab">>, 7},
			{<<"bcdbcd">>, 6}, {<<"bcdefj">>, 4}
		],
		foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], T10)
	),
	?assertEqual(
		[
			{<<"bcdefj">>, 4},
			{<<"bcdbcd">>, 6},
			{<<"bab">>, 7},
			{<<"abc">>, 4},
			{<<"ab">>, 2},
			{<<"aab">>, 3},
			{<<"aaa">>, 1}
		],
		get_range(<<"aaa">>, 20, T10)
	),
	?assertEqual(
		[
			{<<"bcdefj">>, 4},
			{<<"bcdbcd">>, 6},
			{<<"bab">>, 7},
			{<<"abc">>, 4},
			{<<"ab">>, 2},
			{<<"aab">>, 3},
			{<<"aaa">>, 1}
		],
		get_range(7, T10)
	),
	{H10, _} = compute_hash(T10, HashFun),
	{H10_1, _} = compute_hash(
		insert(
			<<"ab">>, 2,
				insert(<<"abc">>, 4, insert(<<"aab">>, 3, insert(<<"aaa">>, 1, new())))),
		HashFun
	),
	{H10_2, _} = compute_hash(
		insert(<<"bcdefj">>, 4, insert(<<"bab">>, 7, insert(<<"bcdbcd">>, 6, new()))),
		HashFun
	),
	?assertEqual(H10, ar_deep_hash:hash([H10_1, H10_2])),
	{H10_2_1, _} = compute_hash(insert(<<"bab">>, 7, new()), HashFun),
	{H10_2_2, _} = compute_hash(insert(<<"bcdbcd">>, 6, insert(<<"bcdefj">>, 4, new())), HashFun),
	?assertEqual(H10_2, ar_deep_hash:hash([H10_2_1, H10_2_2])),
	?assertNotEqual(H10, element(1, compute_hash(delete(<<"ab">>, T10), HashFun))),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%%      a -> a -> 8
	%% bcd -> efj -> 4
	%%        bcd -> 6
	T11 = insert(<<"baa">>, 8, T10),
	?assertEqual(8, ar_patricia_tree:size(T11)),
	?assertEqual(1, get(<<"aaa">>, T11)),
	?assertEqual(3, get(<<"aab">>, T11)),
	?assertEqual(4, get(<<"abc">>, T11)),
	?assertEqual(4, get(<<"bcdefj">>, T11)),
	?assertEqual(6, get(<<"bcdbcd">>, T11)),
	?assertEqual(7, get(<<"bab">>, T11)),
	?assertEqual(8, get(<<"baa">>, T11)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%%      a -> a -> 8
	%% bcd -> efj -> 4
	%%        bcd -> 6
	%% <<>> -> empty
	T12 = insert(<<>>, empty, T11),
	?assertEqual(9, ar_patricia_tree:size(T12)),
	?assertEqual(1, get(<<"aaa">>, T12)),
	?assertEqual(3, get(<<"aab">>, T12)),
	?assertEqual(4, get(<<"abc">>, T12)),
	?assertEqual(4, get(<<"bcdefj">>, T12)),
	?assertEqual(6, get(<<"bcdbcd">>, T12)),
	?assertEqual(7, get(<<"bab">>, T12)),
	?assertEqual(8, get(<<"baa">>, T12)),
	?assertEqual(empty, get(<<>>, T12)),
	?assertEqual(
		[
			{<<>>, empty}, {<<"aaa">>, 1}, {<<"aab">>, 3}, {<<"ab">>, 2}, {<<"abc">>, 4},
			{<<"baa">>, 8}, {<<"bab">>, 7}, {<<"bcdbcd">>, 6}, {<<"bcdefj">>, 4}
		],
		foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], T12)
	),
	{H12, _} = compute_hash(T12, HashFun),
	T13 = from_proplist([
		{<<"bcdbcd">>, 6}, {<<>>, empty}, {<<"ab">>, 2}, {<<"baa">>, 8}, {<<"aab">>, 3},
		{<<"bab">>, 7}, {<<"aaa">>, 1}, {<<"abc">>, 4}, {<<"bcdefj">>, 4}
	]),
	{H13, _} = compute_hash(T13, HashFun),
	?assertEqual(H12, H13),
	?assertEqual(1, get(<<"aaa">>, T13)),
	?assertEqual(3, get(<<"aab">>, T13)),
	?assertEqual(4, get(<<"abc">>, T13)),
	?assertEqual(4, get(<<"bcdefj">>, T13)),
	?assertEqual(6, get(<<"bcdbcd">>, T13)),
	?assertEqual(7, get(<<"bab">>, T13)),
	?assertEqual(8, get(<<"baa">>, T13)),
	?assertEqual(empty, get(<<>>, T13)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%%      a -> a -> 8
	%% bcd -> efj -> 4
	%%        bc -> 9
	%%              d -> 6
	%% <<>> -> empty
	T14 = insert(<<"bcdbc">>, 9, T13),
	?assertEqual(10, ar_patricia_tree:size(T14)),
	?assertEqual(1, get(<<"aaa">>, T14)),
	?assertEqual(3, get(<<"aab">>, T14)),
	?assertEqual(4, get(<<"abc">>, T14)),
	?assertEqual(4, get(<<"bcdefj">>, T14)),
	?assertEqual(6, get(<<"bcdbcd">>, T14)),
	?assertEqual(7, get(<<"bab">>, T14)),
	?assertEqual(8, get(<<"baa">>, T14)),
	?assertEqual(9, get(<<"bcdbc">>, T14)),
	?assertEqual(empty, get(<<>>, T14)),
	T15 = insert(<<"bcdbc">>, 10, T14),
	?assertEqual(10, ar_patricia_tree:size(T15)),
	?assertEqual(10, get(<<"bcdbc">>, T15)),
	?assertEqual(6, get(<<"bcdbcd">>, T15)),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%% bcd -> efj -> 4
	%%        bc -> 10
	%%              d -> 6
	%% <<>> -> empty
	{H15, T15_2} = compute_hash(T15, HashFun),
	T16 = delete(<<"baa">>, T15_2),
	?assertEqual(1, get(<<"aaa">>, T16)),
	?assertEqual(3, get(<<"aab">>, T16)),
	?assertEqual(4, get(<<"abc">>, T16)),
	?assertEqual(4, get(<<"bcdefj">>, T16)),
	?assertEqual(6, get(<<"bcdbcd">>, T16)),
	?assertEqual(7, get(<<"bab">>, T16)),
	?assertEqual(not_found, get(<<"baa">>, T16)),
	?assertEqual(10, get(<<"bcdbc">>, T16)),
	?assertEqual(empty, get(<<>>, T16)),
	{H16, T16_2} = compute_hash(T16, HashFun),
	?assertNotEqual(H16, H15),
	%% a -> a -> a -> 1
	%%           b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%% bcd -> efj -> 4
	%%        bc -> 10
	%% <<>> -> empty
	T17 = delete(<<"bcdbcd">>, T16_2),
	?assertEqual(1, get(<<"aaa">>, T17)),
	?assertEqual(3, get(<<"aab">>, T17)),
	?assertEqual(4, get(<<"abc">>, T17)),
	?assertEqual(4, get(<<"bcdefj">>, T17)),
	?assertEqual(not_found, get(<<"bcdbcd">>, T17)),
	?assertEqual(7, get(<<"bab">>, T17)),
	?assertEqual(10, get(<<"bcdbc">>, T17)),
	?assertEqual(empty, get(<<>>, T17)),
	{H17, T17_2} = compute_hash(T17, HashFun),
	?assertNotEqual(H17, H16),
	%% a -> a -> b -> 3
	%%      b -> 2
	%%           c -> 4
	%% b -> a -> b -> 7
	%%      a -> a -> 9
	%% bcd -> efj -> 4
	%%        bc -> 10
	%% <<>> -> empty
	T18 = insert(<<"baa">>, 9, delete(<<"aaa">>, T17_2)),
	{H18, _} = compute_hash(T18, HashFun),
	?assertEqual(not_found, get(<<"aaa">>, T18)),
	?assertEqual(3, get(<<"aab">>, T18)),
	?assertEqual(4, get(<<"abc">>, T18)),
	?assertEqual(4, get(<<"bcdefj">>, T18)),
	?assertEqual(9, get(<<"baa">>, T18)),
	?assertEqual(7, get(<<"bab">>, T18)),
	?assertEqual(10, get(<<"bcdbc">>, T18)),
	?assertEqual(empty, get(<<>>, T18)),
	?assertNotEqual(H18, H17),
	?assertEqual([{<<>>, empty}], get_range(<<>>, 1, T18)),
	?assertEqual([{<<>>, empty}], get_range(1, T18)),
	?assertEqual(
		[
			{<<"bcdefj">>, 4}, {<<"bcdbc">>, 10}, {<<"bab">>, 7}, {<<"baa">>, 9}, {<<"abc">>, 4},
			{<<"ab">>, 2}, {<<"aab">>, 3}, {<<>>, empty}
		],
		get_range(<<>>, 20, T18)
	),
	?assertEqual(
		[
			{<<"bcdefj">>, 4},
			{<<"bcdbc">>, 10},
			{<<"bab">>, 7},
			{<<"baa">>, 9},
			{<<"abc">>, 4},
			{<<"ab">>, 2},
			{<<"aab">>, 3},
			{<<>>, empty}
		],
		get_range(8, T18)
	),
	T19 = insert(<<"a">>, 11, T18),
	?assertEqual(11, get(<<"a">>, T19)),
	%% a -> 11
	%%      a -> b -> 3
	%%      b -> c -> 4
	%% b -> a -> b -> 7
	%%      a -> a -> 9
	%% bcd -> efj -> 4
	%%        bc -> 10
	%% <<>> -> empty
	T20 = delete(<<"ab">>, T19),
	?assertEqual(not_found, get(<<"ab">>, T20)),
	?assertEqual(11, get(<<"a">>, T20)),
	?assertEqual(3, get(<<"aab">>, T20)),
	?assertEqual(4, get(<<"abc">>, T20)),
	?assertEqual(4, get(<<"bcdefj">>, T20)),
	?assertEqual(9, get(<<"baa">>, T20)),
	?assertEqual(7, get(<<"bab">>, T20)),
	?assertEqual(10, get(<<"bcdbc">>, T20)),
	?assertEqual(empty, get(<<>>, T20)),
	?assertEqual(8, ar_patricia_tree:size(T20)),
	%% abc -> 1
	%% def -> 1
	T21 = delete(<<"def">>, insert(<<"def">>, 1, insert(<<"abc">>, 1, new()))),
	?assertEqual(not_found, get(<<"def">>, T21)),
	?assertNotEqual(
		element(1,
			compute_hash(insert(<<"aab">>, 1, insert(<<"aaa">>, 1, insert(<<"a">>, 2, new()))),
				HashFun)),
		element(1,
			compute_hash(insert(<<"aab">>, 1, insert(<<"aaa">>, 1, insert(<<"aa">>, 2, new()))),
				HashFun))
	).

stochastic_test() ->
	lists:foreach(
		fun(_Case) ->
			KeyValues = random_key_values(3),
			lists:foldl(
				%% Assert all the permutations of the order of insertion of elements
				%% produce the tree with the same root hash. Assert that each of the
				%% elements removed from the tree after each permutation produces the tree
				%% with the same root hash as the trees produced by building up the tree
				%% without this element.
				fun(Permutation, Acc) ->
					Tree = from_proplist(Permutation),
					Map = maps:from_list(Permutation),
					compare_with_map(Tree, Map),
					SHA256Fun =
						fun(K, V) ->
							crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>)
						end,
					lists:foreach(
						fun({K, V}) ->
							Tree1 = delete(K, Tree),
							M = maps:remove(K, Map),
							compare_with_map(Tree1, M),
							{H1, _} = compute_hash(Tree1, SHA256Fun),
							Tree2 = from_proplist(Permutation -- [{K, V}]),
							{H2, _} = compute_hash(Tree2, SHA256Fun),
							?assertEqual(H1, H2, [{tree1, Tree1}, {tree2, Tree2}])
						end,
						Permutation
					),
					{H, _} = compute_hash(Tree, SHA256Fun),
					case Acc of
						start ->
							do_not_assert;
						_ ->
							?assertEqual(H, Acc)
					end,
					Acc
				end,
				start,
				permutations(KeyValues)
			)
		end,
		lists:seq(1, 1000)
	).

random_key_values(N) ->
	lists:foldl(
		fun(_, Acc) ->
			[{crypto:strong_rand_bytes(5), crypto:strong_rand_bytes(30)} | Acc]
		end,
		[],
		lists:seq(1, N)
	).

compare_with_map(Tree, Map) ->
	?assertEqual(map_size(Map), ar_patricia_tree:size(Tree)),
	maps:map(
		fun(Key, Value) ->
			?assertEqual(Value, get(Key, Tree))
		end,
		Map
	).

permutations([]) ->
	[[]];
permutations(L) ->
	[[KV | T] || KV <- L, T <- permutations(L -- [KV])].
