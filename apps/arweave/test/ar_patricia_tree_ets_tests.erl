%%% @doc Fast, node-free equivalence tests asserting ar_patricia_tree_ets behaves identically to
%%% ar_patricia_tree across the whole API (size, is_empty, get, foldr, get_range/2,3, delete,
%%% compute_hash root) and that the two trees are structurally identical (same nodes, children,
%%% suffixes, values), plus a consensus-root check that the frozen reference impl
%%% (ar_patricia_tree_legacy), the in-memory impl, and the ets impl all hash to the same root.
%%% Persistence is covered separately by ar_account_tree_persist_tests (node-based).
-module(ar_patricia_tree_ets_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test cases
%%====================================================================

equivalence_test_() ->
	{timeout, 60, fun() -> lists:foreach(fun({N, A}) -> check(N, A) end, cases()) end}.

%% Check that the new account_tree implementations (ar_patricia_tree and ar_patricia_tree_ets)
%% are equivalent to the legacy implementation (ar_patricia_tree_legacy).
legacy_equivalence_test_() ->
	{timeout, 60, fun() ->
		lists:foreach(fun({N, A}) -> check_legacy_equivalent(N, A) end, cases())
	end}.

cases() ->
	[
		{empty, []},
		{one_key, [acct(<<1:256>>, t1)]},
		{one_key_t2, [acct(<<1:256>>, t2)]},
		{empty_key, [acct(<<>>, t1), acct(<<7:256>>, t2)]},
		{two_shared, [acct(pad32(<<1, 2, 3, 4>>), t1), acct(pad32(<<1, 2, 3, 9>>), t2)]},
		{shared_prefixes, shared_prefix_accounts()},
		{many, many_accounts(100)}
	].

%% An excursion (snapshot_begin, mutate, compute_hash, snapshot_restore) must leave the table
%% byte-identical to before - every node tuple including its cached hash, plus the size entry -
%% so the tip is never left dirty by hashing a fork. Covers inserts of new keys, updates of
%% existing ones, deletes, node splits, and their mixes, over empty and populated bases.
snapshot_restore_test_() ->
	{timeout, 60, fun() ->
		lists:foreach(fun({N, A, Ops}) -> check_snapshot(N, A, Ops) end, snapshot_cases())
	end}.

%% The tree is canonical: the same accounts produce the same nodes and the same root hash
%% regardless of insertion order. This is what makes the snapshot-restore in ar_account_tree and
%% the consensus root hash well defined.
order_independence_test_() ->
	{timeout, 60, fun() ->
		lists:foreach(fun({N, A}) -> check_order(N, A) end,
				[{shared, shared_prefix_accounts()},
				 {with_empty, [acct(<<>>, t2) | shared_prefix_accounts()]},
				 {many, many_accounts(120)}])
	end}.

snapshot_cases() ->
	K = fun(Bytes) -> pad32(Bytes) end,
	Seventh = element(1, lists:nth(7, many_accounts(60))),
	[
		{noop, many_accounts(40), []},
		{insert_into_empty, [], [{insert, K(<<1, 2>>), val(K(<<1, 2>>))}]},
		{insert_new, many_accounts(60), [{insert, K(<<16#AB, 1>>), val(K(<<16#AB, 1>>))}]},
		{update_existing, many_accounts(60), [{insert, Seventh, val(Seventh)}]},
		{split, [acct(K(<<1, 2, 3, 4>>), t1), acct(K(<<9, 9>>), t2)],
				[{insert, K(<<1, 2, 3, 9>>), val(K(<<1, 2, 3, 9>>))}]},
		{delete_one, many_accounts(60), [{delete, Seventh}]},
		{delete_all, [acct(K(<<1>>), t1), acct(K(<<2>>), t2)],
				[{delete, K(<<1>>)}, {delete, K(<<2>>)}]},
		{mixed, shared_prefix_accounts(),
				[{insert, K(<<0, 0, 0, 5>>), val(K(<<0, 0, 0, 5>>))},
				 {delete, K(<<255>>)}, {insert, K(<<1, 2>>), val2(K(<<1, 2>>))}]}
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
		fun	({insert, Key, Value}) -> ar_patricia_tree_ets:insert(Key, Value, Ets);
			({delete, Key}) -> ar_patricia_tree_ets:delete(Key, Ets)
		end,
		Ops).

%% The full table verbatim, hashes included, as a sorted list. Unlike dump/2 this keeps the
%% cached Hash, so an equality check proves the bytes - and therefore the dirty/clean state -
%% are identical. Used for snapshot-restore, where restore reinstates the exact node tuples.
raw_dump(Ets) ->
	lists:sort(ets:tab2list(Ets)).

%% Like raw_dump but with each node's Children gb_set normalized to a sorted list. A gb_set's
%% internal shape depends on insertion order even for an equal set, so canonicity (same accounts
%% in any order) is checked against this normalized view, which still keeps the cached hash.
canon_dump(Ets) ->
	lists:sort([canon_entry(Entry) || Entry <- ets:tab2list(Ets)]).

canon_entry({'$size', Size}) ->
	{'$size', Size};
canon_entry({Key, {Parent, Children, Hash, Suffix, Value}}) ->
	{Key, {Parent, lists:sort(gb_sets:to_list(Children)), Hash, Suffix, Value}}.

val(Addr) ->
	element(2, acct(Addr, t1)).

val2(Addr) ->
	element(2, acct(Addr, t2)).

%%====================================================================
%% Helpers
%%====================================================================

%% Assert the legacy reference, in-memory, and ets impls all hash to the same root under the
%% production consensus hash function.
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

%% Assert the mem and ets impls agree across the API and are structurally identical.
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
	%% compute_hash root agrees.
	{RootMem, _, _} = ar_patricia_tree:compute_hash(Mem, HashFun),
	{RootEts, _, _} = ar_patricia_tree_ets:compute_hash(Ets, HashFun, #{}),
	?assertEqual(RootMem, RootEts, {root, Name}),
	%% The trees are structurally identical (node-by-node, ignoring cached hashes).
	?assertEqual(dump(ar_patricia_tree, Mem), dump(ar_patricia_tree_ets, Ets),
			{structure, Name}).

%% A normalized, hash-state-independent view of the tree: a sorted list of
%% {KeyPrefix, {Parent, SortedChildKeys, Suffix, MaybeValue}}. Drops the cached Hash so it
%% compares regardless of whether compute_hash has run; the root-hash assertion covers the
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

%% Unified hash function (node-free): sha256 leaf, deep_hash node.
hash_fun() ->
	fun	(leaf, {K, V}) -> crypto:hash(sha256, << K/binary, (term_to_binary(V))/binary >>);
		(node, Hashes) -> ar_deep_hash:hash(Hashes)
	end.

acct(Addr, t1) ->
	{Addr, {erlang:phash2(Addr, 1000000000), crypto:hash(sha256, Addr)}};
acct(Addr, t2) ->
	{Addr, {erlang:phash2(Addr, 1000000000), crypto:hash(sha256, Addr),
			1 + erlang:phash2(Addr, 10), true}}.

many_accounts(N) ->
	[acct(crypto:hash(sha256, <<I:64>>), shape(I)) || I <- lists:seq(1, N)].

shared_prefix_accounts() ->
	[
		acct(pad32(<<>>), t1),
		acct(pad32(<<0, 0, 0, 1>>), t2),
		acct(pad32(<<0, 0, 0, 2>>), t1),
		acct(pad32(<<0, 0, 0, 3>>), t2),
		acct(pad32(<<0, 0, 9>>), t1),
		acct(pad32(<<1, 2>>), t2),
		acct(pad32(<<1, 2, 3, 4>>), t1),
		acct(pad32(<<255>>), t2)
	].

pad32(Prefix) when byte_size(Prefix) =< 32 ->
	<< Prefix/binary, 0:((32 - byte_size(Prefix)) * 8) >>.

shape(I) when I rem 2 == 0 -> t1;
shape(_I) -> t2.
