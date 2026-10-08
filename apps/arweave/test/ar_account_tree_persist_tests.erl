%%% @doc Equivalence tests for the two account-tree persistence paths. In the old path
%%% ar_patricia_tree:compute_hash returns an UpdateMap which
%%% ar_storage:store_account_tree_update/3 writes to account_tree_db. In the new path
%%% ar_patricia_tree_ets:compute_hash with #{ sink => Pid } streams the same node updates
%%% as batches to a sink, which here writes them to account_tree_db just as ar_storage does
%%% on a live node. Both must persist the same content-addressed nodes, so the tree
%%% restored from disk (ar_storage:read_wallet_list/1) is identical either way.
%%%
%%% The tests rely on the booted test node, where account_tree_db is open.
-module(ar_account_tree_persist_tests).

-include_lib("eunit/include/eunit.hrl").

%% Upper bound above every account_tree_db key (a 48-byte hash followed by a key prefix of
%% up to 32 bytes).
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
    check_full(one_key, [account(<<1:256>>, t1)]),
    check_full(one_key_t2, [account(<<1:256>>, t2)]),
    check_full(empty_key, [account(<<>>, t1), account(<<7:256>>, t2)]),
    check_full(many_keys, many_accounts(60)),
    check_full(shared_prefixes, shared_prefix_accounts()),
    %% A node holding both a value and children is persisted as two records: one for the
    %% value and one for the node pointing at it.
    check_full(nested, nested_accounts()),
    %% Incremental rehash: persist a base, apply some modifications, rehash (dirty-only),
    %% restore - the result must equal the base with the modifications applied.
    check_modify(add_one, many_accounts(60), [account(<<16#AB, 1:248>>, t2)]),
    check_modify(update_existing, many_accounts(60),
                 [account(crypto:hash(sha256, <<7:64>>), t1)]),    %% same key as many_accounts I=7
    %% Splitting (an insert lands inside an existing node's suffix, splitting it):
    check_modify(just_split, [account(pad32(<<1, 2, 3, 4>>), t1), account(pad32(<<9, 9>>), t2)],
                 [account(pad32(<<1, 2, 3, 9>>), t2)]),            %% splits at <<1,2,3>>
    check_modify(nested_split, [account(pad32(<<1, 2, 3, 4, 5, 6>>), t1)],
                 [account(pad32(<<1, 2, 3, 4, 5, 9>>), t2), account(pad32(<<1, 2, 3, 9>>), t1),
                  account(pad32(<<1, 9>>), t2)]),                  %% splits at depths 5, 3, 1
    check_modify(split_and_add, [account(pad32(<<1, 2, 3, 4>>), t1)],
                 [account(pad32(<<1, 2, 9>>), t2), account(pad32(<<200>>), t1)]),
    check_modify(split_and_update, shared_prefix_accounts(),
                 [account(pad32(<<0, 0, 0, 1, 5>>), t2),           %% splits the <<0,0,0,1>> leaf
                  account(pad32(<<255>>), t1)]),                   %% updates existing <<255,...>>
    %% Modify around nodes that hold both a value and children: put a value on an inner
    %% node that has none, and insert a key below an existing leaf.
    check_modify(nested_insert_at_inner, [account(<<"aaa">>, t1), account(<<"aab">>, t2)],
                 [account(<<"aa">>, t2)]),
    check_modify(nested_grow_below_leaf, nested_accounts(), [account(<<"bcde">>, t2)]),
    %% Deletes. The first one leaves an inner node with a single child and no value - such a
    %% node passes its child's hash up instead of hashing (see
    %% ar_patricia_tree_ets_tests:hash_forwarding_test_).
    check_delete(delete_to_forwarding_node,
                 [account(pad32(<<1, 2, 3, 4>>), t1), account(pad32(<<1, 2, 3, 9>>), t2),
                  account(pad32(<<9>>), t1)],
                 [pad32(<<1, 2, 3, 9>>)]),
    check_delete(delete_some, many_accounts(60),
                 [crypto:hash(sha256, <<7:64>>), crypto:hash(sha256, <<8:64>>)]),
    check_delete(delete_nested_values, nested_accounts(), [<<"ab">>, <<"a">>]),
    %% Re-persisting an unchanged (fully cached) tree is a no-op that must still restore.
    check_repersist(repersist_many, many_accounts(60)),
    %% Restoring from disk and re-hashing must reproduce the same root.
    check_roundtrip(roundtrip_empty, []),
    check_roundtrip(roundtrip_many, many_accounts(60)),
    check_roundtrip(roundtrip_shared, shared_prefix_accounts()),
    check_roundtrip(roundtrip_nested, nested_accounts()),
    %% Read from disk into a map tree, load it into a fresh ets table
    %% (ar_account_tree:load_into_ets/1), and reproduce the root.
    check_load_into_ets(load_ets_empty, []),
    check_load_into_ets(load_ets_many, many_accounts(60)),
    check_load_into_ets(load_ets_shared, shared_prefix_accounts()),
    check_load_into_ets(load_ets_nested, nested_accounts()),
    %% Boot path: stream from disk straight into a fresh ets table
    %% (ar_storage:fold_wallet_list/4), and reproduce the root.
    check_stream_into_ets(stream_ets_empty, []),
    check_stream_into_ets(stream_ets_many, many_accounts(60)),
    check_stream_into_ets(stream_ets_shared, shared_prefix_accounts()),
    check_stream_into_ets(stream_ets_nested, nested_accounts()).

%% A delete leaves an inner node with a single child that earlier inserts
%% pushed two levels down without rehashing it. Below the root, such a node
%% takes its child's hash and is not stored. Its parent points at the child
%% directly.
single_child_node_read_test_() ->
    {timeout, 60, fun test_single_child_node_read/0}.

test_single_child_node_read() ->
    HashFun = ar_block:wallet_list_hash_fun(),
    {Dormant, _, Accounts, Root, Tree} = persist_single_child_node(HashFun),
    ar_patricia_tree_ets:delete_table(Tree),
    {DormantAddr, DormantValue} = Dormant,
    ?assertEqual([], single_child_entries()),
    ?assertEqual(Accounts, read(Root)),
    ?assertEqual(DormantValue, ar_storage:read_account(DormantAddr, Root)),
    ?assertEqual({ok, Root}, stream_root(Root, HashFun)),
    %% Absent addresses below, beside and away from the single-child node.
    lists:foreach(
        fun(Addr) ->
            ?assertEqual({0, <<>>}, ar_storage:read_account(Addr, Root))
        end,
        [pad32(<<1, 2, 3, 4, 7>>), pad32(<<1, 2, 3, 8>>), pad32(<<1, 5>>),
         pad32(<<5>>)]
    ).

%% Databases written in the format of earlier releases, which also stored
%% every node below the root with no value and a single child, read back the
%% same as databases written in the current format. Each case applies its
%% steps to an empty tree and persists after every step, once in each format.
storage_formats_read_test_() ->
    {timeout, 120, fun test_storage_formats_read/0}.

test_storage_formats_read() ->
    HashFun = ar_block:wallet_list_hash_fun(),
    %% {Name, Steps, WhetherTheLegacyFormatStoresSingleChildNodes}
    Cases = [
        {no_single_child_nodes, [{insert, many_accounts(60)}], false},
        {value_nodes_with_one_and_several_children,
            [{insert, nested_accounts()}], false},
        {single_child_no_value_node,
            [{insert, shared_prefix_accounts()},
             {delete, [pad32(<<0, 0, 0, 1>>), pad32(<<0, 0, 0, 2>>),
                       pad32(<<0, 0, 0, 3>>)]}], true},
        {single_child_value_and_no_value_nodes,
            [{insert, nested_accounts()}, {delete, [<<"abd">>, <<"b">>]}],
            true},
        {stacked_single_child_nodes,
            [{insert, [account(pad32(<<1, 2, 3, 4, 5>>), t1),
                       account(pad32(<<1, 2, 3, 4, 9>>), t2),
                       account(pad32(<<1, 2, 9>>), t1),
                       account(pad32(<<9>>), t2)]},
             {delete, [pad32(<<1, 2, 9>>)]},
             {delete, [pad32(<<1, 2, 3, 4, 9>>)]}], true},
        %% Earlier releases stored the single-child node here under a prefix
        %% that resolves back to itself, which made reads loop forever.
        {single_child_node_above_moved_leaf,
            [{insert, [account(pad32(<<1, 2, 3, 4, 5>>), t1),
                       account(pad32(<<9>>), t2)]},
             {insert, [account(pad32(<<1, 2, 9, 9, 9>>), t1)]},
             {insert, [account(pad32(<<1, 2, 3, 4, 9>>), t2)]},
             {delete, [pad32(<<1, 2, 3, 4, 9>>)]}], true},
        {single_child_root, [{insert, [account(pad32(<<7>>), t2)]}], true},
        {single_child_root_above_branch,
            [{insert, [account(pad32(<<5, 1>>), t1),
                       account(pad32(<<5, 2>>), t2)]}], true}
    ],
    lists:foreach(
        fun({Name, Steps, HasSingleChildNodes}) ->
            lists:foreach(
                fun(Legacy) ->
                    Case = {Name, {legacy, Legacy}},
                    clear_db(),
                    {Root, Accounts, Deleted} = persist_steps(Steps, HashFun,
                            #{ legacy_single_child_nodes => Legacy }),
                    ?assertEqual(fresh_root(Accounts, HashFun), Root, Case),
                    SingleChildKeys = [Key
                            || {Key, _} <- single_child_entries()],
                    case Legacy of
                        true ->
                            ?assertEqual(HasSingleChildNodes,
                                    SingleChildKeys /= [], Case);
                        false ->
                            ?assertEqual([], SingleChildKeys -- [Root], Case)
                    end,
                    ?assertEqual(Accounts, read(Root), Case),
                    lists:foreach(
                        fun({Addr, Value}) ->
                            ?assertEqual(Value,
                                    ar_storage:read_account(Addr, Root),
                                    {Case, Addr})
                        end,
                        Accounts),
                    lists:foreach(
                        fun(Addr) ->
                            ?assertEqual({0, <<>>},
                                    ar_storage:read_account(Addr, Root),
                                    {Case, Addr})
                        end,
                        [<<16#FE, 0:248>> | Deleted]),
                    ?assertEqual({ok, Root}, stream_root(Root, HashFun), Case)
                end,
                [true, false])
        end,
        Cases).

%% Deleting two keys stacks two single-child nodes above a leaf. The parent
%% of the upper one points past both of them at the leaf.
stacked_single_child_nodes_test_() ->
    {timeout, 60, fun test_stacked_single_child_nodes/0}.

test_stacked_single_child_nodes() ->
    HashFun = ar_block:wallet_list_hash_fun(),
    {KeptAddr, KeptValue} = Kept = account(pad32(<<1, 2, 3, 4, 5>>), t1),
    {SiblingAddr, _} = Sibling = account(pad32(<<1, 2, 3, 4, 9>>), t2),
    {CousinAddr, _} = Cousin = account(pad32(<<1, 2, 9>>), t1),
    Other = account(pad32(<<9>>), t2),
    clear_db(),
    {_, Tree1, _} = persist_ets(build_ets([Kept, Sibling, Cousin, Other]),
            HashFun),
    {_, Tree2, _} = persist_ets(
            delete_all(ar_patricia_tree_ets, Tree1, [CousinAddr]), HashFun),
    {Root, Tree3, _} = persist_ets(
            delete_all(ar_patricia_tree_ets, Tree2, [SiblingAddr]), HashFun),
    ar_patricia_tree_ets:delete_table(Tree3),
    ?assertEqual(fresh_root([Kept, Other], HashFun), Root),
    ?assertEqual([], single_child_entries()),
    %% No node moved after it was stored, so every child reference, including
    %% the one skipping the single-child nodes, names an existing entry.
    ?assertEqual([], unresolved_child_references()),
    ?assertEqual(sort([Kept, Other]), read(Root)),
    ?assertEqual(KeptValue, ar_storage:read_account(KeptAddr, Root)),
    ?assertEqual({0, <<>>}, ar_storage:read_account(SiblingAddr, Root)),
    ?assertEqual({ok, Root}, stream_root(Root, HashFun)).

%% Reinserting the deleted key gives the single-child node a second child
%% again, so it is stored and its parent points at it.
single_child_node_reinsert_test_() ->
    {timeout, 60, fun test_single_child_node_reinsert/0}.

test_single_child_node_reinsert() ->
    HashFun = ar_block:wallet_list_hash_fun(),
    {_, Split2, Accounts, _, Tree} = persist_single_child_node(HashFun),
    {Root, Tree2, _} =
        persist_ets(insert_all(ar_patricia_tree_ets, Tree, [Split2]), HashFun),
    ar_patricia_tree_ets:delete_table(Tree2),
    Accounts2 = sort([Split2 | Accounts]),
    ?assertEqual(fresh_root(Accounts2, HashFun), Root),
    ?assertEqual([], single_child_entries()),
    ?assertEqual(Accounts2, read(Root)),
    ?assertEqual({ok, Root}, stream_root(Root, HashFun)).

%% A root with a single child is the one single-child node that is stored, so
%% that a stored tree is always found under exactly its root hash.
single_child_root_read_test_() ->
    {timeout, 60, fun test_single_child_root_read/0}.

test_single_child_root_read() ->
    HashFun = ar_block:wallet_list_hash_fun(),
    {Addr, Value} = Account = account(pad32(<<7>>), t2),
    clear_db(),
    {Root, Tree, _} = persist_ets(build_ets([Account]), HashFun),
    ar_patricia_tree_ets:delete_table(Tree),
    ?assertEqual(HashFun(leaf, Account), Root),
    ?assertEqual({ok, term_to_binary([{Root, binary:part(Addr, 0, 1)}])},
            ar_kv:get(account_tree_db, Root)),
    ?assertEqual([Account], read(Root)),
    ?assertEqual(Value, ar_storage:read_account(Addr, Root)),
    ?assertEqual({ok, Root}, stream_root(Root, HashFun)).

%% A single-child entry whose child was never stored does not resolve, so
%% reading falls back to the (absent) wallet list files.
single_child_entry_without_child_test_() ->
    {timeout, 60, fun test_single_child_entry_without_child/0}.

test_single_child_entry_without_child() ->
    Hash = crypto:hash(sha384, <<"single_child_entry_without_child">>),
    clear_db(),
    ok = ar_kv:put(account_tree_db, Hash,
            term_to_binary([{Hash, << 1, 2, 3 >>}])),
    ?assertEqual(not_found, ar_storage:read_wallet_list(Hash)).

%% A hash stored only under a longer prefix is not found by its root key, even
%% when the entry just before that key is a single-child node of another hash.
single_child_entry_before_unknown_root_test_() ->
    {timeout, 60, fun test_single_child_entry_before_unknown_root/0}.

test_single_child_entry_before_unknown_root() ->
    PrevHash = << 1:384 >>,
    Hash = << 2:384 >>,
    clear_db(),
    ok = ar_kv:put(account_tree_db, << PrevHash/binary, 1 >>,
            term_to_binary([{PrevHash, << 1, 2 >>}])),
    ok = ar_kv:put(account_tree_db, << Hash/binary, 7 >>,
            term_to_binary(account(pad32(<< 7 >>), t1))),
    ?assertEqual(not_found, ar_storage:read_wallet_list(Hash)).

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @doc Assert the old and the new persistence paths reconstruct the same tree, equal to
%% the input accounts. The old path goes through ar_storage:write_wallet_list/2, the new
%% one streams the node updates to account_tree_db while hashing the ets tree.
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

%% @doc Persist a base, apply Mods (inserts of new keys and updates of existing ones) to
%% the already-hashed tree, rehash and persist again (only the changed paths are re-hashed),
%% then restore. The result must equal the base with Mods applied. Covers updates,
%% splitting, and their mixes.
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

%% @doc Persist a base, delete Keys from the already-hashed tree, rehash and persist again,
%% then restore. The result must equal the base minus Keys.
check_delete(Name, Accounts, Keys) ->
    HashFun = ar_block:wallet_list_hash_fun(),
    Final = sort([{K, V} || {K, V} <- Accounts, not lists:member(K, Keys)]),
    %% Old path.
    clear_db(),
    _ = ar_storage:write_wallet_list(0, build_mem(Accounts)),
    {_, BaseCached, _} = ar_patricia_tree:compute_hash(build_mem(Accounts), HashFun),
    RootOld = ar_storage:write_wallet_list(1, delete_all(ar_patricia_tree, BaseCached, Keys)),
    OldAccounts = read(RootOld),
    %% New path.
    clear_db(),
    {_, BaseEts, _} = persist_ets(build_ets(Accounts), HashFun),
    {RootNew, _, _} = persist_ets(delete_all(ar_patricia_tree_ets, BaseEts, Keys), HashFun),
    NewAccounts = read(RootNew),
    ?assertEqual(RootOld, RootNew, {root, Name}),
    ?assertEqual(Final, OldAccounts, {old, Name}),
    ?assertEqual(Final, NewAccounts, {new, Name}).

delete_all(Mod, Tree, Keys) ->
    lists:foldl(fun(K, T) -> Mod:delete(K, T) end, Tree, Keys).

%% @doc Re-hashing and persisting a fully cached tree produces an empty update. The root
%% is unchanged and the tree still restores from disk.
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

%% @doc The startup path: persist a tip, read it back from disk into a map tree, load that
%% map into a fresh ets table via ar_account_tree:load_into_ets/1, and assert it hashes to
%% the same root.
check_load_into_ets(Name, Accounts) ->
    HashFun = ar_block:wallet_list_hash_fun(),
    clear_db(),
    Root = ar_storage:write_wallet_list(0, build_mem(Accounts)),
    {ok, RestoredMap} = ar_storage:read_wallet_list(Root),
    Tid = ar_account_tree:load_into_ets(RestoredMap),
    {RootEts, _, _} = ar_patricia_tree_ets:compute_hash(Tid, HashFun, #{}),
    ar_patricia_tree_ets:delete_table(Tid),
    ?assertEqual(Root, RootEts, {load_into_ets, Name}).

%% @doc The startup path: persist a tip, stream it from disk straight into a fresh ets
%% table via ar_storage:fold_wallet_list/4, and assert it hashes to the same root.
check_stream_into_ets(Name, Accounts) ->
    HashFun = ar_block:wallet_list_hash_fun(),
    clear_db(),
    Root = ar_storage:write_wallet_list(0, build_mem(Accounts)),
    Tid = ar_patricia_tree_ets:new(),
    {ok, Tid} = ar_storage:fold_wallet_list(Root,
            fun(Key, Value, Tid2) -> ar_patricia_tree_ets:insert(Key, Value, Tid2) end,
            Tid, not_set),
    {RootEts, _, _} = ar_patricia_tree_ets:compute_hash(Tid, HashFun, #{}),
    ar_patricia_tree_ets:delete_table(Tid),
    ?assertEqual(Root, RootEts, {stream_into_ets, Name}).

%% @doc Persist the tree of each step: a dormant account, then two inserts that
%% push it two levels down without rehashing it, then a delete that leaves the
%% node above it with a single child. Return the dormant account, the deleted
%% account, the final accounts, the final root and the final ets tree.
persist_single_child_node(HashFun) ->
    Dormant = account(pad32(<<1, 2, 3, 4, 5>>), t1),
    Other = account(pad32(<<9>>), t2),
    Split1 = account(pad32(<<1, 2, 9, 9, 9>>), t1),
    {Split2Addr, _} = Split2 = account(pad32(<<1, 2, 3, 4, 9>>), t2),
    clear_db(),
    {_, Tree1, _} = persist_ets(build_ets([Dormant, Other]), HashFun),
    {_, Tree2, _} =
        persist_ets(insert_all(ar_patricia_tree_ets, Tree1, [Split1]), HashFun),
    {_, Tree3, _} =
        persist_ets(insert_all(ar_patricia_tree_ets, Tree2, [Split2]), HashFun),
    {Root, Tree4, _} = persist_ets(
            delete_all(ar_patricia_tree_ets, Tree3, [Split2Addr]), HashFun),
    {Dormant, Split2, sort([Dormant, Other, Split1]), Root, Tree4}.

%% @doc Return the account_tree_db entries holding a single-child node.
single_child_entries() ->
    case ar_kv:get_range(account_tree_db, <<>>, ?DB_END) of
        {ok, Entries} ->
            [Entry || {_, Bin} = Entry <- maps:to_list(Entries),
                is_single_child_entry(binary_to_term(Bin))];
        Error ->
            Error
    end.

%% @doc Return the {Hash, Prefix} child references stored in account_tree_db
%% with no entry at exactly that hash and prefix.
unresolved_child_references() ->
    case ar_kv:get_range(account_tree_db, <<>>, ?DB_END) of
        {ok, Entries} ->
            References = lists:append([Children
                    || Bin <- maps:values(Entries),
                    [_ | _] = Children <- [binary_to_term(Bin)]]),
            [{Hash, Prefix} || {Hash, Prefix} <- References,
                not maps:is_key(<< Hash/binary, (db_prefix(Prefix))/binary >>,
                        Entries)];
        Error ->
            Error
    end.

db_prefix(root) ->
    <<>>;
db_prefix(Prefix) ->
    Prefix.

is_single_child_entry([_]) ->
    true;
is_single_child_entry(_) ->
    false.

fresh_root(Accounts, HashFun) ->
    Tree = build_ets(Accounts),
    {Root, _, _} = ar_patricia_tree_ets:compute_hash(Tree, HashFun, #{}),
    ar_patricia_tree_ets:delete_table(Tree),
    Root.

%% @doc Stream the stored tree into a fresh ets table the way the node does on
%% startup and return {ok, RootHash} of the result.
stream_root(Root, HashFun) ->
    Tid = ar_patricia_tree_ets:new(),
    Insert = fun(Key, Value, Tid2) ->
        ar_patricia_tree_ets:insert(Key, Value, Tid2)
    end,
    Result =
        case ar_storage:fold_wallet_list(Root, Insert, Tid, not_set) of
            {ok, Tid} ->
                {RootEts, _, _} =
                    ar_patricia_tree_ets:compute_hash(Tid, HashFun, #{}),
                {ok, RootEts};
            Other ->
                Other
        end,
    ar_patricia_tree_ets:delete_table(Tid),
    Result.

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

%% @doc Apply Steps ({insert, Accounts} or {delete, Keys}) to an empty ets
%% tree, persisting it after every step. Return the final root, the sorted
%% final accounts and the deleted keys.
persist_steps(Steps, HashFun, Opts) ->
    {Root, Tree, Accounts, Deleted} = lists:foldl(
        fun({insert, New}, {_, StepTree, StepAccounts, StepDeleted}) ->
                {StepRoot, StepTree2, _} = persist_ets(
                        insert_all(ar_patricia_tree_ets, StepTree, New),
                        HashFun, Opts),
                {StepRoot, StepTree2,
                 maps:merge(StepAccounts, maps:from_list(New)), StepDeleted};
           ({delete, Keys}, {_, StepTree, StepAccounts, StepDeleted}) ->
                {StepRoot, StepTree2, _} = persist_ets(
                        delete_all(ar_patricia_tree_ets, StepTree, Keys),
                        HashFun, Opts),
                {StepRoot, StepTree2, maps:without(Keys, StepAccounts),
                 StepDeleted ++ Keys}
        end,
        {<<>>, ar_patricia_tree_ets:new(), #{}, []},
        Steps),
    ar_patricia_tree_ets:delete_table(Tree),
    {Root, sort(maps:to_list(Accounts)), Deleted}.

%% @doc Hash the ets tree, streaming node-update batches to a sink that writes them to
%% account_tree_db (as ar_storage will on a live node), and block until every batch is written
%% before returning - so the subsequent read/1 sees a complete tree.
persist_ets(Tree, HashFun) ->
    persist_ets(Tree, HashFun, #{}).

persist_ets(Tree, HashFun, Opts) ->
    Sink = spawn_link(fun() -> writer_sink_loop(account_tree_db) end),
    Result = ar_patricia_tree_ets:compute_hash(Tree, HashFun,
            Opts#{ sink => Sink }),
    Sink ! {account_tree_persist_sync, self()},
    receive {account_tree_persist_synced, Sink} -> ok end,
    unlink(Sink),
    exit(Sink, kill),
    Result.

%% @doc Write node-update batches to DBName, keeping the entries already stored
%% as ar_storage does.
writer_sink_loop(DBName) ->
    receive
        {account_tree_node_batch, Batch} ->
            NewNodes = [{K, term_to_binary(V)} || {K, V} <- Batch,
                    ar_kv:get(DBName, K) == not_found],
            ok = ar_kv:write_batch(DBName, NewNodes),
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

account(Addr, t1) ->
    {Addr, {erlang:phash2(Addr, 1000000000), last_tx(Addr)}};
account(Addr, t2) ->
    {Addr, {erlang:phash2(Addr, 1000000000), last_tx(Addr), 1 + erlang:phash2(Addr, 10), true}}.

last_tx(Addr) ->
    crypto:hash(sha256, Addr).

many_accounts(N) ->
    [account(crypto:hash(sha256, <<I:64>>), shape(I)) || I <- lists:seq(1, N)].

%% @doc Distinct 32-byte keys sharing leading prefixes to varying degrees, to exercise the
%% radix tree's splitting and merging. pad32 zero-pads, so every key below must differ
%% within its first non-padded bytes.
shared_prefix_accounts() ->
    [
     account(pad32(<<>>), t1),               %% all zeros
     account(pad32(<<0, 0, 0, 1>>), t2),     %% shares <<0,0,0>> with the next two
     account(pad32(<<0, 0, 0, 2>>), t1),
     account(pad32(<<0, 0, 0, 3>>), t2),
     account(pad32(<<0, 0, 9>>), t1),        %% shares <<0,0>> with the <<0,0,0,_>> group
     account(pad32(<<1, 2>>), t2),
     account(pad32(<<1, 2, 3, 4>>), t1),     %% shares <<1,2>> with the previous
     account(pad32(<<255>>), t2)             %% shares nothing
    ].

pad32(Prefix) when byte_size(Prefix) =< 32 ->
    << Prefix/binary, 0:((32 - byte_size(Prefix)) * 8) >>.

%% @doc Keys where one key is a prefix of another key, so some nodes hold both a value and
%% children (see ar_patricia_tree_ets_tests:nested_accounts/0).
nested_accounts() ->
    [
     account(<<"ab">>, t1),      %% has children <<"abc">> and <<"abd">>
     account(<<"a">>, t2),       %% ancestor of every "a" key
     account(<<"abc">>, t1),
     account(<<"abd">>, t2),
     account(<<"b">>, t1),
     account(<<"bc">>, t2),      %% has child <<"bcd">>
     account(<<"bcd">>, t1)
    ].

shape(I) when I rem 2 == 0 -> t1;
shape(_I) -> t2.
