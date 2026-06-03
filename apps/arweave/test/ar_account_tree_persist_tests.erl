%%% @doc Equivalence tests for the two account-tree persistence paths:
%%% the old path (ar_patricia_tree:compute_hash returns an UpdateMap which
%%% ar_storage:store_account_tree_update/3 writes to account_tree_db) and the new path
%%% (ar_patricia_tree_ets:compute_hash with #{sink => Pid} streams the same node updates as
%%% batches to a sink, which here writes them to account_tree_db just as ar_storage does on a
%%% live node). Both must persist the same content-addressed nodes, so the tree restored from
%%% disk (ar_storage:read_wallet_list/1) is identical either way.
%%%
%%% Node-based: relies on the booted test node where account_tree_db is open.
-module(ar_account_tree_persist_tests).

-include_lib("eunit/include/eunit.hrl").

%% Wide bound covering every account_tree_db key (48-byte hash + <=32-byte prefix).
-define(DB_END, <<255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255>>).

%%%===================================================================
%%% Test cases
%%%===================================================================

persist_equivalence_test_() ->
	{timeout, 120, fun test_persist_equivalence/0}.

test_persist_equivalence() ->
	%% Full-tree update: every node is in the update.
	check_full(empty, []),
	check_full(one_key, [acct(<<1:256>>, t1)]),
	check_full(one_key_t2, [acct(<<1:256>>, t2)]),
	check_full(empty_key, [acct(<<>>, t1), acct(<<7:256>>, t2)]),
	check_full(many_keys, many_accounts(60)),
	check_full(shared_prefixes, shared_prefix_accounts()),
	%% Incremental rehash: persist a base, apply some modifications, rehash (dirty-only),
	%% restore - the result must equal the base with the modifications applied.
	check_modify(add_one, many_accounts(60), [acct(<<16#AB, 1:248>>, t2)]),
	check_modify(update_existing, many_accounts(60),
			[acct(crypto:hash(sha256, <<7:64>>), t1)]),    %% same key as many_accounts I=7
	%% Splitting (an insert lands inside an existing node's suffix, splitting it):
	check_modify(just_split, [acct(pad32(<<1, 2, 3, 4>>), t1), acct(pad32(<<9, 9>>), t2)],
			[acct(pad32(<<1, 2, 3, 9>>), t2)]),            %% splits at <<1,2,3>>
	check_modify(nested_split, [acct(pad32(<<1, 2, 3, 4, 5, 6>>), t1)],
			[acct(pad32(<<1, 2, 3, 4, 5, 9>>), t2), acct(pad32(<<1, 2, 3, 9>>), t1),
			 acct(pad32(<<1, 9>>), t2)]),                  %% splits at depths 5, 3, 1
	check_modify(split_and_add, [acct(pad32(<<1, 2, 3, 4>>), t1)],
			[acct(pad32(<<1, 2, 9>>), t2), acct(pad32(<<200>>), t1)]),
	check_modify(split_and_update, shared_prefix_accounts(),
			[acct(pad32(<<0, 0, 0, 1, 5>>), t2),           %% splits the <<0,0,0,1>> leaf
			 acct(pad32(<<255>>), t1)]),                   %% updates existing <<255,...>>
	%% Re-persisting an unchanged (fully cached) tree is a no-op that must still restore.
	check_repersist(repersist_many, many_accounts(60)),
	%% Restoring from disk and re-hashing must reproduce the same root.
	check_roundtrip(roundtrip_empty, []),
	check_roundtrip(roundtrip_many, many_accounts(60)),
	check_roundtrip(roundtrip_shared, shared_prefix_accounts()).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @doc Assert old and new persistence reconstruct the same tree (== the input accounts).
%% Old path: ar_storage:write_wallet_list/2 (hash_wallet_list -> store_account_tree_update).
%% New path: the ets impl streams the same node updates to account_tree_db while hashing.
check_full(Name, Accounts) ->
	HashFun = ar_block:wallet_list_hash_fun(),
	clear_db(),
	RootOld = ar_storage:write_wallet_list(0, build_mem(Accounts)),
	OldAccounts = read(RootOld),
	clear_db(),
	{RootNew, _, _} = persist_ets(build_ets(Accounts), HashFun),
	NewAccounts = read(RootNew),
	?assertEqual(RootOld, RootNew, {root, Name}),
	?assertEqual(sort(Accounts), OldAccounts, {old, Name}),
	?assertEqual(sort(Accounts), NewAccounts, {new, Name}).

%% @doc Persist a base, apply Mods (inserts of new keys and/or updates of existing ones) to
%% the already-hashed tree, rehash (dirty-only) and persist again, then restore. The result
%% must equal the base with Mods applied. Covers updates, splitting, and their mixes.
check_modify(Name, Accounts, Mods) ->
	HashFun = ar_block:wallet_list_hash_fun(),
	Final = apply_mods(Accounts, Mods),
	%% Old path.
	clear_db(),
	_ = ar_storage:write_wallet_list(0, build_mem(Accounts)),
	{_, BaseCached, _} = ar_patricia_tree:compute_hash(build_mem(Accounts), HashFun),
	RootOld = ar_storage:write_wallet_list(1, insert_all(ar_patricia_tree, BaseCached, Mods)),
	OldAccounts = read(RootOld),
	%% New path.
	clear_db(),
	{_, BaseEts, _} = persist_ets(build_ets(Accounts), HashFun),
	{RootNew, _, _} =
		persist_ets(insert_all(ar_patricia_tree_ets, BaseEts, Mods), HashFun),
	NewAccounts = read(RootNew),
	?assertEqual(RootOld, RootNew, {root, Name}),
	?assertEqual(Final, OldAccounts, {old, Name}),
	?assertEqual(Final, NewAccounts, {new, Name}).

%% @doc Re-hashing/persisting a fully-cached tree produces an empty update; the root is
%% unchanged and the tree still restores from disk.
check_repersist(Name, Accounts) ->
	HashFun = ar_block:wallet_list_hash_fun(),
	clear_db(),
	Root = ar_storage:write_wallet_list(0, build_mem(Accounts)),
	{_, Cached, _} = ar_patricia_tree:compute_hash(build_mem(Accounts), HashFun),
	?assertEqual(Root, ar_storage:write_wallet_list(1, Cached), {old_reroot, Name}),
	?assertEqual(sort(Accounts), read(Root), {old, Name}),
	clear_db(),
	{RootEts, CachedEts, _} = persist_ets(build_ets(Accounts), HashFun),
	{RootEts2, _, _} = persist_ets(CachedEts, HashFun),
	?assertEqual(RootEts, RootEts2, {ets_reroot, Name}),
	?assertEqual(Root, RootEts, {old_new_root, Name}),
	?assertEqual(sort(Accounts), read(RootEts), {new, Name}).

%% @doc Restore a tree from disk, re-hash the restored tree, and assert the same root.
check_roundtrip(Name, Accounts) ->
	HashFun = ar_block:wallet_list_hash_fun(),
	clear_db(),
	Root = ar_storage:write_wallet_list(0, build_mem(Accounts)),
	{ok, Restored} = ar_storage:read_wallet_list(Root),
	{Root2, _, _} = ar_patricia_tree:compute_hash(Restored, HashFun),
	?assertEqual(Root, Root2, {roundtrip, Name}).

apply_mods(Accounts, Mods) ->
	Base = maps:from_list(Accounts),
	sort(maps:to_list(lists:foldl(fun({K, V}, Acc) -> Acc#{ K => V } end, Base, Mods))).

insert_all(Mod, Tree, Mods) ->
	lists:foldl(fun({K, V}, T) -> Mod:insert(K, V, T) end, Tree, Mods).

read(RootHash) ->
	{ok, Tree} = ar_storage:read_wallet_list(RootHash),
	sort(ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Tree)).

clear_db() ->
	ar_kv:delete_range(account_tree_db, <<>>, ?DB_END).

%% @doc Hash the ets tree, streaming node-update batches to a sink that writes them to
%% account_tree_db (as ar_storage will on a live node), and block until every batch is written
%% before returning - so the subsequent read/1 sees a complete tree.
persist_ets(Tree, HashFun) ->
	Sink = spawn_link(fun() -> writer_sink_loop(account_tree_db) end),
	Result = ar_patricia_tree_ets:compute_hash(Tree, HashFun, #{ sink => Sink }),
	Sink ! {account_tree_persist_sync, self()},
	receive {account_tree_persist_synced, Sink} -> ok end,
	unlink(Sink),
	exit(Sink, kill),
	Result.

writer_sink_loop(DBName) ->
	receive
		{account_tree_node_batch, Batch} ->
			ok = ar_kv:write_batch(DBName, [{K, term_to_binary(V)} || {K, V} <- Batch]),
			writer_sink_loop(DBName);
		{account_tree_persist_sync, From} ->
			From ! {account_tree_persist_synced, self()},
			writer_sink_loop(DBName)
	end.

build_mem(Accounts) ->
	lists:foldl(fun({K, V}, Acc) -> ar_patricia_tree:insert(K, V, Acc) end,
			ar_patricia_tree:new(), Accounts).

build_ets(Accounts) ->
	lists:foldl(fun({K, V}, Acc) -> ar_patricia_tree_ets:insert(K, V, Acc) end,
			ar_patricia_tree_ets:new(), Accounts).

sort(Accounts) ->
	lists:sort(Accounts).

acct(Addr, t1) ->
	{Addr, {erlang:phash2(Addr, 1000000000), last_tx(Addr)}};
acct(Addr, t2) ->
	{Addr, {erlang:phash2(Addr, 1000000000), last_tx(Addr), 1 + erlang:phash2(Addr, 10), true}}.

last_tx(Addr) ->
	crypto:hash(sha256, Addr).

many_accounts(N) ->
	[acct(crypto:hash(sha256, <<I:64>>), shape(I)) || I <- lists:seq(1, N)].

%% Keys that share a leading prefix to varying degrees (0..3 bytes), plus identical short
%% prefixes, to exercise the radix tree's splitting/merging.
%% Distinct 32-byte keys sharing leading prefixes to varying degrees (note: pad32 zero-
%% pads, so every key below must differ within its first non-padded bytes).
shared_prefix_accounts() ->
	[
		acct(pad32(<<>>), t1),               %% all zeros
		acct(pad32(<<0, 0, 0, 1>>), t2),     %% shares <<0,0,0>> with the next two
		acct(pad32(<<0, 0, 0, 2>>), t1),
		acct(pad32(<<0, 0, 0, 3>>), t2),
		acct(pad32(<<0, 0, 9>>), t1),        %% shares <<0,0>> with the <<0,0,0,_>> group
		acct(pad32(<<1, 2>>), t2),
		acct(pad32(<<1, 2, 3, 4>>), t1),     %% shares <<1,2>> with the previous
		acct(pad32(<<255>>), t2)             %% shares nothing
	].

pad32(Prefix) when byte_size(Prefix) =< 32 ->
	<< Prefix/binary, 0:((32 - byte_size(Prefix)) * 8) >>.

shape(I) when I rem 2 == 0 -> t1;
shape(_I) -> t2.
