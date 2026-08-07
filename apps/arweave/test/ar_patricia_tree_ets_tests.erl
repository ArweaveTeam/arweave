%%% @doc Tests asserting ar_patricia_tree_ets behaves identically to ar_patricia_tree
%%% across the whole API (size, is_empty, get, foldr, get_range/2,3, delete, compute_hash
%%% root) and that the two trees are structurally identical (same nodes, children,
%%% suffixes, values). Also checks ar_patricia_tree_legacy, ar_patricia_tree, and
%%% ar_patricia_tree_ets all hash to the same root under the production consensus hash
%%% function. The tests need no test node. Persistence is covered separately by
%%% ar_account_tree_persist_tests.
-module(ar_patricia_tree_ets_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test cases
%%====================================================================

equivalence_test_() ->
    {timeout, 60, fun() -> lists:foreach(fun({N, A}) -> check(N, A) end, cases()) end}.

%% @doc The new account tree implementations (ar_patricia_tree and ar_patricia_tree_ets)
%% hash to the same root as the legacy ar_patricia_tree_legacy.
legacy_equivalence_test_() ->
    {timeout, 60, fun() ->
        lists:foreach(fun({N, A}) -> check_legacy_equivalent(N, A) end, cases())
    end}.

cases() ->
    [
     {empty, []},
     {one_key, [account(<<1:256>>, t1)]},
     {one_key_t2, [account(<<1:256>>, t2)]},
     {empty_key, [account(<<>>, t1), account(<<7:256>>, t2)]},
     {two_shared, [account(pad32(<<1, 2, 3, 4>>), t1), account(pad32(<<1, 2, 3, 9>>), t2)]},
     {shared_prefixes, shared_prefix_accounts()},
     {nested, nested_accounts()},
     {many, many_accounts(100)}
    ].

%% @doc Keys where one key is a prefix of another key, like <<"a">> and <<"ab">>. The
%% prefix key's node holds both a value and children. Real addresses are all 32 bytes long,
%% so such nodes never appear in production, but the tree code still has branches for them.
%% The prefix key <<"ab">> is placed first because check/2 deletes the first key: deleting
%% it must remove the value but keep the node, since <<"abc">> and <<"abd">> still live
%% below it.
nested_accounts() ->
    [
     account(<<"ab">>, t1),      %% has children <<"abc">> and <<"abd">>
     account(<<"a">>, t2),       %% ancestor of every "a" key
     account(<<"abc">>, t1),
     account(<<"abd">>, t2),
     account(<<"b">>, t1),       %% plain leaf
     account(<<"bc">>, t2),      %% has child <<"bcd">>
     account(<<"bcd">>, t1)
    ].

%% @doc A snapshot round trip (snapshot_begin, mutate, compute_hash, snapshot_restore)
%% must leave the table byte-identical to before - every node tuple including its cached
%% hash, plus the size entry - so the tip is never left dirty by hashing a fork. Covers
%% inserts of new keys, updates of existing ones, deletes, node splits, and their mixes,
%% over empty and populated bases.
snapshot_restore_test_() ->
    {timeout, 60, fun() ->
        lists:foreach(fun({N, A, Ops}) -> check_snapshot(N, A, Ops) end, snapshot_cases())
    end}.

%% @doc snapshot_restore must undo the writes even when the work between snapshot_begin
%% and compute_hash throws, the way ar_account_tree:with_snapshot restores in an `after`
%% clause when the wrapped operation fails. Record a base, begin a snapshot, apply some
%% inserts and a delete, raise, restore in an `after`, then assert the table is
%% byte-identical (cached hashes and size included) and still hashes to the original root.
snapshot_restore_on_abort_test_() ->
    {timeout, 30, fun test_snapshot_restore_on_abort/0}.

test_snapshot_restore_on_abort() ->
    HashFun = hash_fun(),
    Accounts = many_accounts(60),
    Existing = element(1, lists:nth(7, Accounts)),
    Fresh = pad32(<<16#AB, 1>>),
    Ets = build(ar_patricia_tree_ets, Accounts),
    {Root0, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    Before = raw_dump(Ets),
    Caught =
        try
            ar_patricia_tree_ets:snapshot_begin(Ets),
            try
                ar_patricia_tree_ets:insert(Fresh, val(Fresh), Ets),
                ar_patricia_tree_ets:insert(Existing, val2(Existing), Ets),
                ar_patricia_tree_ets:delete(element(1, hd(Accounts)), Ets),
                error(aborted_mid_excursion)
            after
                ar_patricia_tree_ets:snapshot_restore(Ets)
            end
        catch
            error:aborted_mid_excursion -> caught
        end,
    ?assertEqual(caught, Caught),
    ?assertEqual(Before, raw_dump(Ets), abort_dump),
    {Root1, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    ?assertEqual(Root0, Root1, abort_root).

%% @doc The same accounts produce the same nodes and the same root hash regardless of
%% insertion order. This is what makes the snapshot restore in ar_account_tree and the
%% consensus root hash well defined.
order_independence_test_() ->
    {timeout, 60, fun() ->
        lists:foreach(fun({N, A}) -> check_order(N, A) end,
                [{shared, shared_prefix_accounts()},
                 {with_empty, [account(<<>>, t2) | shared_prefix_accounts()]},
                 {nested, nested_accounts()},
                 {many, many_accounts(120)}])
    end}.

%% @doc Random fuzz. For every insertion order of a random key set: the root hash is the
%% same, a tree with a key deleted hashes like a tree built without that key, and get/2 and
%% size/1 agree with a plain map. Mirrors ar_patricia_tree:stochastic_test/0. Runs over
%% three kinds of key sets: fully random keys, keys branching off a shared base at several
%% depths (nested_prefix_key_values/1), and keys that are prefixes of one another
%% (strict_prefix_key_values/1).
stochastic_test_() ->
    {timeout, 120, fun() ->
        lists:foreach(fun(_) -> check_stochastic(random_key_values(3)) end, lists:seq(1, 200)),
        lists:foreach(fun(_) -> check_stochastic(nested_prefix_key_values(4)) end,
                lists:seq(1, 150)),
        lists:foreach(fun(_) -> check_stochastic(strict_prefix_key_values(4)) end,
                lists:seq(1, 150))
    end}.

%% @doc Delete edge cases not covered directly elsewhere: deleting an absent key is a
%% no-op (structurally identical to the map-based tree), and deleting every key empties
%% the tree.
delete_edges_test_() ->
    {timeout, 30, fun test_delete_edges/0}.

%% @doc A node with no value and a single child is not hashed itself - the child's hash is
%% passed up unchanged (the [{SingleHash, _}] branch of do_compute_hash). Inserts never
%% create such nodes, because a split always produces two children. Deletes do: removing
%% one of a node's two children leaves the node with one child, and delete does not merge
%% it away. This happens on a live node when a reorg reverts an account creation. Check
%% that a tree with such nodes hashes to the same root as a tree built without the deleted
%% keys, and that later inserts and deletes around them work.
hash_forwarding_test_() ->
    {timeout, 30, fun test_hash_forwarding/0}.

test_hash_forwarding() ->
    HashFun = hash_fun(),
    Kept = pad32(<<1, 2, 3, 4, 5>>),
    Sibling = pad32(<<1, 2, 3, 4, 9>>),   %% differs from Kept in the last byte
    Cousin = pad32(<<1, 2, 9>>),          %% shares only <<1, 2>> with the other two
    Accounts = [account(Kept, t1), account(Sibling, t2), account(Cousin, t1)],
    %% Deleting Cousin leaves the inner node above it with one child and no value.
    Mem1 = ar_patricia_tree:delete(Cousin, build(ar_patricia_tree, Accounts)),
    Ets = build(ar_patricia_tree_ets, Accounts),
    ar_patricia_tree_ets:delete(Cousin, Ets),
    assert_equivalent(one_forwarding_node, Mem1, Ets, [account(Kept, t1), account(Sibling, t2)],
                      HashFun),
    %% Deleting Sibling creates a second such node, right above Kept's leaf.
    Mem2 = ar_patricia_tree:delete(Sibling, Mem1),
    ar_patricia_tree_ets:delete(Sibling, Ets),
    assert_equivalent(two_forwarding_nodes, Mem2, Ets, [account(Kept, t1)], HashFun),
    {RootAfterDeletes, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    DumpAfterDeletes = dump(ar_patricia_tree_ets, Ets),
    %% The root matches a tree that never contained Sibling and Cousin, even though the
    %% shapes differ: after the deletes the tree still has its two extra inner nodes, the
    %% fresh build has none.
    Fresh = build(ar_patricia_tree_ets, [account(Kept, t1)]),
    {RootFresh, _, _} = ar_patricia_tree_ets:compute_hash(Fresh, HashFun, #{}),
    ?assertEqual(RootFresh, RootAfterDeletes, root_canonical),
    ?assertNotEqual(dump(ar_patricia_tree_ets, Fresh), DumpAfterDeletes, shape_differs),
    %% Reinserting Sibling gives the node above Kept a second child again.
    Ets2 = build(ar_patricia_tree_ets, Accounts),
    ar_patricia_tree_ets:delete(Cousin, Ets2),
    ar_patricia_tree_ets:delete(Sibling, Ets2),
    ar_patricia_tree_ets:insert(Sibling, val2(Sibling), Ets2),
    Mem3 = ar_patricia_tree:insert(Sibling, val2(Sibling), Mem2),
    assert_equivalent(reinsert, Mem3, Ets2, [account(Kept, t1), account(Sibling, t2)], HashFun),
    %% Deleting the last key removes the leftover inner nodes too: the table matches a
    %% fresh empty tree.
    Ets3 = build(ar_patricia_tree_ets, Accounts),
    ar_patricia_tree_ets:delete(Cousin, Ets3),
    ar_patricia_tree_ets:delete(Sibling, Ets3),
    ar_patricia_tree_ets:delete(Kept, Ets3),
    ?assert(ar_patricia_tree_ets:is_empty(Ets3)),
    ?assertEqual(<<>>, element(1, ar_patricia_tree_ets:compute_hash(Ets3, HashFun, #{}))),
    DumpCollapsed = raw_dump(Ets3),
    ?assertEqual(raw_dump(ar_patricia_tree_ets:new()), DumpCollapsed, collapse).

snapshot_cases() ->
    K = fun(Bytes) -> pad32(Bytes) end,
    Seventh = element(1, lists:nth(7, many_accounts(60))),
    [
     {noop, many_accounts(40), []},
     {insert_into_empty, [], [{insert, K(<<1, 2>>), val(K(<<1, 2>>))}]},
     {insert_new, many_accounts(60), [{insert, K(<<16#AB, 1>>), val(K(<<16#AB, 1>>))}]},
     {update_existing, many_accounts(60), [{insert, Seventh, val(Seventh)}]},
     {split, [account(K(<<1, 2, 3, 4>>), t1), account(K(<<9, 9>>), t2)],
      [{insert, K(<<1, 2, 3, 9>>), val(K(<<1, 2, 3, 9>>))}]},
     {delete_one, many_accounts(60), [{delete, Seventh}]},
     {delete_all, [account(K(<<1>>), t1), account(K(<<2>>), t2)],
      [{delete, K(<<1>>)}, {delete, K(<<2>>)}]},
     {mixed, shared_prefix_accounts(),
      [{insert, K(<<0, 0, 0, 5>>), val(K(<<0, 0, 0, 5>>))},
       {delete, K(<<255>>)}, {insert, K(<<1, 2>>), val2(K(<<1, 2>>))}]},
     %% Put a value on the inner node <<"aa">>, which sits above <<"aaa">> and <<"aab">>
     %% with no value of its own. Then delete values from nodes that keep their children
     %% (see nested_accounts/0).
     {nested_insert_at_inner, [account(<<"aaa">>, t1), account(<<"aab">>, t2)],
      [{insert, <<"aa">>, val(<<"aa">>)}]},
     {nested_delete_value_keeps_node, nested_accounts(),
      [{delete, <<"ab">>}, {delete, <<"a">>}]},
     %% Delete a key so an inner node is left with a single child (see
     %% hash_forwarding_test_), alone and followed by reinserting the same key.
     {delete_to_forwarding_node, [account(K(<<1, 2, 3, 4>>), t1), account(K(<<1, 2, 3, 9>>), t2),
                                  account(K(<<9>>), t1)],
      [{delete, K(<<1, 2, 3, 9>>)}]},
     {forwarding_node_reinsert, [account(K(<<1, 2, 3, 4>>), t1), account(K(<<1, 2, 3, 9>>), t2)],
      [{delete, K(<<1, 2, 3, 9>>)}, {insert, K(<<1, 2, 3, 9>>), val2(K(<<1, 2, 3, 9>>))}]}
    ].

check_snapshot(Name, Accounts, Ops) ->
    HashFun = hash_fun(),
    Ets = build(ar_patricia_tree_ets, Accounts),
    {Root0, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    Before = raw_dump(Ets),
    ar_patricia_tree_ets:snapshot_begin(Ets),
    apply_ops(Ets, Ops),
    {_, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    ar_patricia_tree_ets:snapshot_restore(Ets),
    ?assertEqual(Before, raw_dump(Ets), {dump, Name}),
    {Root1, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    ?assertEqual(Root0, Root1, {root, Name}).

check_order(Name, Accounts) ->
    HashFun = hash_fun(),
    Orders = [Accounts, lists:reverse(Accounts), lists:keysort(1, Accounts),
              lists:reverse(lists:keysort(1, Accounts)), rotate(Accounts), interleave(Accounts)],
    [Reference | Rest] = [hashed_dump(O, HashFun) || O <- Orders],
    lists:foreach(fun(D) -> ?assertEqual(Reference, D, {order, Name}) end, Rest).

rotate([]) ->
    [];
rotate([H | T]) ->
    T ++ [H].

%% Odd-indexed elements followed by even-indexed - an order distinct from reverse and sort.
interleave(List) ->
    Indexed = lists:zip(lists:seq(1, length(List)), List),
    [X || {I, X} <- Indexed, I rem 2 == 1] ++ [X || {I, X} <- Indexed, I rem 2 == 0].

hashed_dump(Accounts, HashFun) ->
    Ets = build(ar_patricia_tree_ets, Accounts),
    {Root, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    {Root, canon_dump(Ets)}.

apply_ops(Ets, Ops) ->
    lists:foreach(
      fun   ({insert, Key, Value}) -> ar_patricia_tree_ets:insert(Key, Value, Ets);
            ({delete, Key}) -> ar_patricia_tree_ets:delete(Key, Ets)
      end,
      Ops).

%% @doc The full table, hashes included, as a sorted list. Unlike dump/2 this keeps the
%% cached Hash, so an equality check proves the bytes, and therefore the hash caching
%% state, are identical. Used for the snapshot tests, where restore reinstates the exact
%% node tuples.
raw_dump(Ets) ->
    lists:sort(ets:tab2list(Ets)).

%% @doc Like raw_dump but with each node's Children gb_set normalized to a sorted list. A
%% gb_set's internal shape depends on insertion order even for an equal set, so order
%% independence is checked against this normalized view, which still keeps the cached hash.
canon_dump(Ets) ->
    lists:sort([canon_entry(Entry) || Entry <- ets:tab2list(Ets)]).

canon_entry({'$size', Size}) ->
    {'$size', Size};
canon_entry({Key, {Parent, Children, Hash, Suffix, Value}}) ->
    {Key, {Parent, lists:sort(gb_sets:to_list(Children)), Hash, Suffix, Value}}.

val(Addr) ->
    element(2, account(Addr, t1)).

val2(Addr) ->
    element(2, account(Addr, t2)).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Assert the legacy, the map-based, and the ETS-based implementations all hash to
%% the same root under the production consensus hash function.
check_legacy_equivalent(Name, Accounts) ->
    HashFun = ar_block:wallet_list_hash_fun(),
    Legacy = build(ar_patricia_tree_legacy, Accounts),
    Mem = build(ar_patricia_tree, Accounts),
    Ets = build(ar_patricia_tree_ets, Accounts),
    {RootLegacy, _, _} = ar_patricia_tree_legacy:compute_hash(Legacy, HashFun),
    {RootMem, _, _} = ar_patricia_tree:compute_hash(Mem, HashFun),
    {RootEts, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    ?assertEqual(RootLegacy, RootMem, {legacy_vs_mem, Name}),
    ?assertEqual(RootMem, RootEts, {mem_vs_ets, Name}).

%% @doc Assert the map-based and the ETS-based trees agree across the API and are
%% structurally identical.
check(Name, Accounts) ->
    HashFun = hash_fun(),
    Mem = build(ar_patricia_tree, Accounts),
    Ets = build(ar_patricia_tree_ets, Accounts),
    assert_equivalent(Name, Mem, Ets, Accounts, HashFun),
    %% Delete the first key (if any) and assert the trees stay equivalent.
    case Accounts of
        [] ->
            ok;
        [{DelKey, _} | _] ->
            Mem2 = ar_patricia_tree:delete(DelKey, Mem),
            Ets2 = ar_patricia_tree_ets:delete(DelKey, Ets),
            ?assertEqual(not_found, ar_patricia_tree:get(DelKey, Mem2), {del_get_mem, Name}),
            ?assertEqual(not_found, ar_patricia_tree_ets:get(DelKey, Ets2), {del_get_ets, Name}),
            assert_equivalent({Name, after_delete}, Mem2, Ets2,
                              proplists:delete(DelKey, Accounts), HashFun)
    end.

assert_equivalent(Name, Mem, Ets, Accounts, HashFun) ->
    ?assertEqual(ar_patricia_tree:size(Mem), ar_patricia_tree_ets:size(Ets), {size, Name}),
    ?assertEqual(ar_patricia_tree:is_empty(Mem), ar_patricia_tree_ets:is_empty(Ets),
                 {is_empty, Name}),
    %% get/2 for every present key, and an absent key.
    lists:foreach(
      fun({K, V}) ->
              ?assertEqual(V, ar_patricia_tree:get(K, Mem), {get_mem, Name, K}),
              ?assertEqual(V, ar_patricia_tree_ets:get(K, Ets), {get_ets, Name, K})
      end,
      Accounts
     ),
    Absent = <<16#FE, 0:248>>,
    ?assertEqual(not_found, ar_patricia_tree:get(Absent, Mem), {absent_mem, Name}),
    ?assertEqual(not_found, ar_patricia_tree_ets:get(Absent, Ets), {absent_ets, Name}),
    %% foldr/3 yields the same {Key, Value} list.
    Fold = fun(Mod, T) -> Mod:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], T) end,
    ?assertEqual(Fold(ar_patricia_tree, Mem), Fold(ar_patricia_tree_ets, Ets), {foldr, Name}),
    %% get_range/2 (all) and get_range/3 (from each key) agree.
    N = ar_patricia_tree:size(Mem),
    ?assertEqual(ar_patricia_tree:get_range(N + 5, Mem),
                 ar_patricia_tree_ets:get_range(N + 5, Ets), {range_all, Name}),
    lists:foreach(
      fun({K, _}) ->
              ?assertEqual(ar_patricia_tree:get_range(K, N + 5, Mem),
                           ar_patricia_tree_ets:get_range(K, N + 5, Ets), {range_from, Name, K})
      end,
      Accounts
     ),
    %% get_range/3 with absent key agree (e.g. [])
    ?assertEqual(ar_patricia_tree:get_range(Absent, N + 5, Mem),
                 ar_patricia_tree_ets:get_range(Absent, N + 5, Ets), {range_absent, Name}),
    %% get_range/3 with zero count agree (e.g. [])
    ?assertEqual(ar_patricia_tree:get_range(0, Mem),
                 ar_patricia_tree_ets:get_range(0, Ets), {range_zero, Name}),
    %% compute_hash root agrees.
    {RootMem, _, _} = ar_patricia_tree:compute_hash(Mem, HashFun),
    {RootEts, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
    ?assertEqual(RootMem, RootEts, {root, Name}),
    %% The trees are structurally identical (node-by-node, ignoring cached hashes).
    ?assertEqual(dump(ar_patricia_tree, Mem), dump(ar_patricia_tree_ets, Ets),
                 {structure, Name}).

%% @doc A normalized view of the tree: a sorted list of
%% {KeyPrefix, {Parent, SortedChildKeys, Suffix, MaybeValue}}. Drops the cached Hash so it
%% compares regardless of whether compute_hash has run. The root-hash assertion covers the
%% hashes.
dump(Mod, Tree) ->
    lists:sort(
      [
       {Key, {Parent, lists:sort(gb_sets:to_list(Children)), Suffix, MaybeValue}}
       ||
          {Key, {Parent, Children, _Hash, Suffix, MaybeValue}} <- node_entries(Mod, Tree)
      ]
     ).

node_entries(ar_patricia_tree, Tree) ->
    [{K, V} || {K, V} <- maps:to_list(Tree), K =/= size];
node_entries(ar_patricia_tree_ets, Tree) ->
    [{K, V} || {K, V} <- ets:tab2list(Tree), K =/= '$size'].

build(Mod, Accounts) ->
    lists:foldl(fun({K, V}, T) -> Mod:insert(K, V, T) end, Mod:new(), Accounts).

%% @doc A test hash function: sha256 for leaves, ar_deep_hash for inner nodes.
hash_fun() ->
    fun (leaf, {K, V}) -> crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>);
        (node, Hashes) -> ar_deep_hash:hash(Hashes)
    end.

account(Addr, t1) ->
    {Addr, {erlang:phash2(Addr, 1000000000), crypto:hash(sha256, Addr)}};
account(Addr, t2) ->
    {Addr, {erlang:phash2(Addr, 1000000000), crypto:hash(sha256, Addr),
            1 + erlang:phash2(Addr, 10), true}}.

many_accounts(N) ->
    [account(crypto:hash(sha256, <<I:64>>), shape(I)) || I <- lists:seq(1, N)].

shared_prefix_accounts() ->
    [
     account(pad32(<<>>), t1),
     account(pad32(<<0, 0, 0, 1>>), t2),
     account(pad32(<<0, 0, 0, 2>>), t1),
     account(pad32(<<0, 0, 0, 3>>), t2),
     account(pad32(<<0, 0, 9>>), t1),
     account(pad32(<<1, 2>>), t2),
     account(pad32(<<1, 2, 3, 4>>), t1),
     account(pad32(<<255>>), t2)
    ].

pad32(Prefix) when byte_size(Prefix) =< 32 ->
    << Prefix/binary, 0:((32 - byte_size(Prefix)) * 8) >>.

shape(I) when I rem 2 == 0 -> t1;
shape(_I) -> t2.

%% @doc For each insertion-order permutation of KeyValues: build the tree, check get/2 and
%% size/1 match a plain map, and check the root is identical across permutations. For each
%% key, deleting it yields the same root as rebuilding the tree without it.
check_stochastic(KeyValues) ->
    HashFun = hash_fun(),
    lists:foldl(
      fun(Permutation, Acc) ->
              Tree = build(ar_patricia_tree_ets, Permutation),
              compare_with_map(Tree, maps:from_list(Permutation)),
              {H, _, _} = ar_patricia_tree_ets:compute_hash(Tree, HashFun, #{}),
              lists:foreach(
                fun({K, V}) ->
                        WithDelete = build(ar_patricia_tree_ets, Permutation),
                        ar_patricia_tree_ets:delete(K, WithDelete),
                        {H1, _, _} = ar_patricia_tree_ets:compute_hash(WithDelete, HashFun, #{}),
                        Rebuilt = build(ar_patricia_tree_ets, Permutation -- [{K, V}]),
                        {H2, _, _} = ar_patricia_tree_ets:compute_hash(Rebuilt, HashFun, #{}),
                        ?assertEqual(H1, H2, {delete_equiv, K}),
                        ar_patricia_tree_ets:delete_table(WithDelete),
                        ar_patricia_tree_ets:delete_table(Rebuilt)
                end,
                Permutation
               ),
              case Acc of
                  start -> ok;
                  _ -> ?assertEqual(Acc, H, order_root)
              end,
              ar_patricia_tree_ets:delete_table(Tree),
              H
      end,
      start,
      permutations(KeyValues)
     ).

compare_with_map(Tree, Map) ->
    ?assertEqual(map_size(Map), ar_patricia_tree_ets:size(Tree)),
    maps:foreach(fun(K, V) -> ?assertEqual(V, ar_patricia_tree_ets:get(K, Tree)) end, Map).

random_key_values(N) ->
    [{crypto:strong_rand_bytes(5), crypto:strong_rand_bytes(30)} || _ <- lists:seq(1, N)].

%% @doc N distinct keys carved from a shared random base at varying prefix lengths: each
%% key keeps a random-length (1..6 byte) leading slice of the base, then diverges with
%% random bytes. A key sharing five base bytes nests below one that shares only two, so
%% inserting the set splits nodes at several depths rather than at a single branch point.
%% Keys are kept distinct so the map comparison and deletion checks in check_stochastic/1
%% stay well defined.
nested_prefix_key_values(N) ->
    Base = crypto:strong_rand_bytes(6),
    distinct_nested(Base, N, []).

distinct_nested(_Base, 0, Acc) ->
    Acc;
distinct_nested(Base, N, Acc) ->
    Share = 1 + binary:first(crypto:strong_rand_bytes(1)) rem byte_size(Base),
    Key = << (binary:part(Base, 0, Share))/binary, (crypto:strong_rand_bytes(4))/binary >>,
    case lists:keymember(Key, 1, Acc) of
        true -> distinct_nested(Base, N, Acc);
        false -> distinct_nested(Base, N - 1, [{Key, crypto:strong_rand_bytes(30)} | Acc])
    end.

%% @doc N distinct random keys, 1 to 4 bytes long, using only the characters a and b. Only
%% 30 such keys exist, so the chosen set usually includes a key that is a prefix of another
%% chosen key (like <<"a">> and <<"ab">>). The prefix key's node then holds both a value
%% and children, so the fuzz covers inserting and deleting such nodes.
strict_prefix_key_values(N) ->
    Shuffled = [Key || {_, Key} <- lists:sort(
            [{crypto:strong_rand_bytes(4), Key} || Key <- ab_keys()])],
    [{Key, crypto:strong_rand_bytes(30)} || Key <- lists:sublist(Shuffled, N)].

%% @doc All keys of length 1..4 over the characters a and b.
ab_keys() ->
    lists:append([ab_keys(Len) || Len <- lists:seq(1, 4)]).

ab_keys(0) ->
    [<<>>];
ab_keys(Len) ->
    [<< Key/binary, C >> || Key <- ab_keys(Len - 1), C <- "ab"].

permutations([]) ->
    [[]];
permutations(L) ->
    [[KV | T] || KV <- L, T <- permutations(L -- [KV])].

test_delete_edges() ->
    HashFun = hash_fun(),
    Accounts = shared_prefix_accounts(),
    AbsentKey = <<16#FE, 0:248>>,
    %% Deleting an absent key is a no-op - structurally identical to the map-based tree.
    MemAfter = ar_patricia_tree:delete(AbsentKey, build(ar_patricia_tree, Accounts)),
    Ets = build(ar_patricia_tree_ets, Accounts),
    ar_patricia_tree_ets:delete(AbsentKey, Ets),
    ?assertEqual(dump(ar_patricia_tree, MemAfter), dump(ar_patricia_tree_ets, Ets),
                 delete_absent_noop),
    %% Deleting every key empties the tree.
    Ets2 = build(ar_patricia_tree_ets, Accounts),
    lists:foreach(fun({K, _}) -> ar_patricia_tree_ets:delete(K, Ets2) end, Accounts),
    ?assertEqual(0, ar_patricia_tree_ets:size(Ets2), delete_all_size),
    ?assert(ar_patricia_tree_ets:is_empty(Ets2)),
    ?assertEqual(<<>>, element(1, ar_patricia_tree_ets:compute_hash(Ets2, HashFun, #{}))).
