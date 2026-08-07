%%% @doc The immutable, map-based backend of ar_patricia_tree_core (see there for the tree
%%% structure). Every operation returns a new tree, so it can hold a standalone tree (the
%%% genesis weave, a tree downloaded from peers, JSON serialization, a tree read from disk).
%%% ar_patricia_tree_ets is the mutable ETS variant backing the production account tree.
-module(ar_patricia_tree).
-test_category([fast]).

-behaviour(ar_patricia_tree_core).

-export([new/0, insert/3, get/2, size/1, compute_hash/2, compute_hash/3,
         foldr/3, is_empty/1, from_proplist/1, delete/2, get_range/2, get_range/3]).

%% ar_patricia_tree_core backend callbacks.
-export([get_node/2, put_node/3, del_node/2, get_size/1, set_size/2, emit/4, progress_extra/1]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return a new tree.
new() ->
    #{ root => {no_parent, gb_sets:new(), no_hash, no_prefix, no_value}, size => 0 }.

%% @doc Insert the given value under the given binary key.
insert(Key, Value, Tree) ->
    ar_patricia_tree_core:insert(?MODULE, Key, Value, Tree).

%% @doc Get the value stored under the given key or not_found.
get(Key, Tree) ->
    ar_patricia_tree_core:get_value(?MODULE, Key, Tree).

%% @doc Return the number of values in the tree.
size(Tree) ->
    ar_patricia_tree_core:size(?MODULE, Tree).

%% @doc Return true if the tree stores no values.
is_empty(Tree) ->
    ar_patricia_tree_core:is_empty(?MODULE, Tree).

%% @doc Create a tree from the given list of {Key, Value} pairs.
from_proplist(Proplist) ->
    ar_patricia_tree_core:from_proplist(?MODULE, Proplist).

%% @doc Delete the given key.
delete(Key, Tree) ->
    ar_patricia_tree_core:delete(?MODULE, Key, Tree).

%% @doc Return up to Count key-value tuples from the start of the tree, keys descending.
get_range(Count, Tree) ->
    ar_patricia_tree_core:get_range(?MODULE, Count, Tree).

%% @doc Return up to Count key-value tuples from the Start key, keys descending.
get_range(Start, Count, Tree) ->
    ar_patricia_tree_core:get_range(?MODULE, Start, Count, Tree).

%% @doc Traverse keys in reverse alphabetical order, applying Fun(Key, Value, Acc).
foldr(Fun, Acc, Tree) ->
    ar_patricia_tree_core:foldr(?MODULE, Fun, Acc, Tree).

%% @doc Recompute the root hash. compute_hash/2 returns the UpdateMap; /3 returns it only when
%% PersistOpts has return_update_map => true, otherwise #{}. PersistOpts
%% #{ progress => true } logs progress lines during the computation.
compute_hash(Tree, HashFun) ->
    compute_hash(Tree, HashFun, #{ return_update_map => true }).

compute_hash(Tree, HashFun, PersistOpts) ->
    Acc0 =
        case maps:get(return_update_map, PersistOpts, false) of
            true -> #{};
            false -> no_update_map
        end,
    Progress = maps:get(progress, PersistOpts, false) == true,
    {RootHash, Tree2, Acc} =
        ar_patricia_tree_core:compute_hash(?MODULE, Tree, HashFun, Acc0, Progress),
    UpdateMap = case Acc of no_update_map -> #{}; _ -> Acc end,
    {RootHash, Tree2, UpdateMap}.

%%%===================================================================
%%% ar_patricia_tree_core backend.
%%%===================================================================

get_node(Tree, Key) ->
    maps:get(Key, Tree, not_found).

put_node(Tree, Key, Node) ->
    Tree#{ Key => Node }.

del_node(Tree, Key) ->
    maps:remove(Key, Tree).

get_size(Tree) ->
    maps:get(size, Tree).

set_size(Tree, Size) ->
    Tree#{ size => Size }.

%% Accumulate one node's update into the map, or skip it when no update map was requested.
emit(no_update_map, _Hash, _KeyPrefix, _Value) ->
    no_update_map;
emit(UpdateMap, Hash, KeyPrefix, Value) ->
    UpdateMap#{ {Hash, KeyPrefix} => Value }.

progress_extra(_Tree) ->
    [].

%%%===================================================================
%%% Tests.
%%%===================================================================

trie_test() ->
    T1 = new(),
    ?assertEqual(not_found, get(<<"aaa">>, T1)),
    ?assertEqual(true, is_empty(T1)),
    HashFun = fun
            (leaf, {K, V}) -> crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>);
            (node, Hashes) -> ar_deep_hash:hash(Hashes)
        end,
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
    {H2, T2_2, _} = compute_hash(T2, HashFun),
    {H2_2, _, _} = compute_hash(T2_2, HashFun),
    ?assertEqual(H2, H2_2),
    ?assertEqual(1, get(<<"aaa">>, T2)),
    ?assertEqual([], get_range(<<>>, 1, T2)),
    ?assertEqual([{<<"aaa">>, 1}], get_range(1, T2)),
    ?assertEqual([{<<"aaa">>, 1}], get_range(<<"aaa">>, 1, T2)),
    %% aa -> a -> 1
    %%       b -> 2
    T3 = insert(<<"aab">>, 2, T2),
    ?assertEqual(2, ar_patricia_tree:size(T3)),
    {H3, _, _} = compute_hash(T3, HashFun),
    ?assertNotEqual(H2, H3),
    {H3_2, _, _} = compute_hash(insert(<<"aaa">>, 1, insert(<<"aab">>, 2, new())), HashFun),
    ?assertEqual(H3, H3_2),
    {H3_3, _, _} =
        compute_hash(
          insert(<<"aaa">>, 1, insert(<<"aab">>, 2, insert(<<"a">>, 3, new()))),
          HashFun
         ),
    {H3_4, _, _} = compute_hash(insert(<<"a">>, 3, T3), HashFun),
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
    {H4, _, _} = compute_hash(T4, HashFun),
    ?assertNotEqual(H3, H4),
    ?assertEqual(1, get(<<"aaa">>, T4)),
    ?assertEqual(3, get(<<"aab">>, T4)),
    %% a -> a -> a -> 1
    %%           b -> 3
    %%      b -> 2
    T5 = insert(<<"ab">>, 2, T4),
    ?assertEqual(3, ar_patricia_tree:size(T5)),
    ?assertEqual(1, gb_sets:size(element(2, maps:get(root, T5)))),
    {H5, _, _} = compute_hash(T5, HashFun),
    ?assertNotEqual(H4, H5),
    {H5_2, _, _} =
        compute_hash(
          insert(<<"aab">>, 3, insert(<<"aaa">>, 1, insert(<<"ab">>, 2, new()))),
          HashFun
         ),
    ?assertEqual(H5, H5_2),
    {_H5_3, T5_2, _} = compute_hash(insert(<<"aaa">>, 1, new()), HashFun),
    {_H5_4, T5_3, _} = compute_hash(insert(<<"ab">>, 2, T5_2), HashFun),
    {H5_5, _T5_4, _} = compute_hash(insert(<<"aab">>, 3, T5_3), HashFun),
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
    ?assertEqual([{<<"abc">>, 4}, {<<"ab">>, 2}, {<<"aab">>, 3}],
                 get_range(<<"aab">>, 20, T6)),
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
    ?assertEqual([{<<"bcdefj">>, 4}, {<<"abc">>, 4}, {<<"ab">>, 2}],
                 get_range(<<"ab">>, 3, T7)),
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
    {H10, _, _} = compute_hash(T10, HashFun),
    {H10_1, _, _} = compute_hash(
                      insert(
                        <<"ab">>, 2,
                        insert(<<"abc">>, 4, insert(<<"aab">>, 3, insert(<<"aaa">>, 1, new())))),
                      HashFun
                     ),
    {H10_2, _, _} = compute_hash(
                      insert(<<"bcdefj">>, 4, insert(<<"bab">>, 7, insert(<<"bcdbcd">>, 6, new()))),
                      HashFun
                     ),
    ?assertEqual(H10, ar_deep_hash:hash([H10_1, H10_2])),
    {H10_2_1, _, _} = compute_hash(insert(<<"bab">>, 7, new()), HashFun),
    {H10_2_2, _, _} = compute_hash(insert(<<"bcdbcd">>, 6,
                                          insert(<<"bcdefj">>, 4, new())), HashFun),
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
    {H12, _, _} = compute_hash(T12, HashFun),
    T13 = from_proplist([
                         {<<"bcdbcd">>, 6}, {<<>>, empty}, {<<"ab">>, 2}, {<<"baa">>, 8}, {<<"aab">>, 3},
                         {<<"bab">>, 7}, {<<"aaa">>, 1}, {<<"abc">>, 4}, {<<"bcdefj">>, 4}
                        ]),
    {H13, _, _} = compute_hash(T13, HashFun),
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
    {H15, T15_2, _} = compute_hash(T15, HashFun),
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
    {H16, T16_2, _} = compute_hash(T16, HashFun),
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
    {H17, T17_2, _} = compute_hash(T17, HashFun),
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
    {H18, _, _} = compute_hash(T18, HashFun),
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
    ?assertEqual([{<<"bcdefj">>, 4}, {<<"bcdbc">>, 10}, {<<"bab">>, 7}, {<<"baa">>, 9},
                  {<<"abc">>, 4}, {<<"ab">>, 2}, {<<"aab">>, 3}, {<<>>, empty}],
                 get_range(<<>>, 20, T18)),
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
               compute_hash(insert(<<"aab">>, 1, insert(<<"aaa">>, 1,
                                                        insert(<<"aa">>, 2, new()))), HashFun))
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
                            fun (leaf, {K, V}) ->
                                    crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>);
                                (node, Hashes) ->
                                    ar_deep_hash:hash(Hashes)
                            end,
                        lists:foreach(
                          fun({K, V}) ->
                                  Tree1 = delete(K, Tree),
                                  M = maps:remove(K, Map),
                                  compare_with_map(Tree1, M),
                                  {H1, _, _} = compute_hash(Tree1, SHA256Fun),
                                  Tree2 = from_proplist(Permutation -- [{K, V}]),
                                  {H2, _, _} = compute_hash(Tree2, SHA256Fun),
                                  ?assertEqual(H1, H2, [{tree1, Tree1}, {tree2, Tree2}])
                          end,
                          Permutation
                         ),
                        {H, _, _} = compute_hash(Tree, SHA256Fun),
                        case Acc of
                            start ->
                                do_not_assert;
                            _ ->
                                ?assertEqual(H, Acc)
                        end,
                        H
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
