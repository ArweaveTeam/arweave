%%% @doc A radix tree with hashed nodes, used for the account tree. Every node caches its
%%% hash and an update invalidates only the hashes on its root-to-leaf path, so recomputing
%%% the root hash costs time proportional to the number of updated keys, not the tree size.
%%%
%%% This module implements the algorithm. Storage is provided by a Backend module
%%% implementing the callbacks below - ar_patricia_tree (map-based) or ar_patricia_tree_ets
%%% (ETS-based).
-module(ar_patricia_tree_core).

-export([insert/4, get_value/3, lookup/3, size/2, is_empty/2, from_proplist/2, delete/3,
         get_range/3, get_range/4, foldr/4, compute_hash/4]).

%% The storage backend. get_node returns the node {Parent, Children, Hash, Suffix, Value}
%% or not_found. put_node, del_node, and set_size return the (possibly new) Tree. emit is
%% called once per hashed node during compute_hash, threading Acc. progress_extra returns
%% extra fields for the progress log.
-callback new() -> Tree :: term().
-callback get_node(Tree :: term(), Key :: term()) -> tuple() | not_found.
-callback put_node(Tree :: term(), Key :: term(), Node :: tuple()) -> Tree :: term().
-callback del_node(Tree :: term(), Key :: term()) -> Tree :: term().
-callback get_size(Tree :: term()) -> non_neg_integer().
-callback set_size(Tree :: term(), Size :: non_neg_integer()) -> Tree :: term().
-callback emit(Acc :: term(), Hash :: binary(), KeyPrefix :: term(), Value :: term()) ->
    Acc :: term().
-callback progress_extra(Tree :: term()) -> iolist().

%% Diagnostic: log a progress line every this many leaves hashed (see progress_* below).
-define(PROGRESS_CHUNK, 1000000).

%%%===================================================================
%%% Public interface (each entry point takes the Backend module).
%%%===================================================================

%% @doc Insert the given value under the given binary key.
insert(Backend, Key, Value, Tree) when is_binary(Key) ->
    insert(Backend, Key, Value, Tree, 1, root).

%% @doc Get the value stored under the given key or not_found.
get_value(Backend, Key, Tree) when is_binary(Key) ->
    case lookup(Backend, Key, Tree, 1) of
        {_, {_, _, _, _, {v, Value}}} ->
            Value;
        not_found ->
            not_found
    end;
get_value(_Backend, _Key, _Tree) ->
    not_found.

%% @doc Return the {KeyPrefix, NodeData} of the node holding Key, or not_found.
lookup(Backend, Key, Tree) ->
    lookup(Backend, Key, Tree, 1).

%% @doc Return the number of values in the tree.
size(Backend, Tree) ->
    Backend:get_size(Tree).

%% @doc Return true if the tree stores no values.
is_empty(Backend, Tree) ->
    Backend:get_size(Tree) == 0.

%% @doc Build a tree from the given list of {Key, Value} pairs.
from_proplist(Backend, Proplist) ->
    lists:foldl(
      fun({Key, Value}, Acc) -> insert(Backend, Key, Value, Acc) end,
      Backend:new(),
      Proplist
     ).

%% @doc Delete the given key.
delete(Backend, Key, Tree) ->
    delete(Backend, Key, Tree, 1).

%% @doc Return up to Count key-value tuples from the start of the tree, keys descending.
get_range(Backend, Count, Tree) ->
    Iterator = iterator(Backend, Tree),
    do_get_range(Backend, Iterator, Count, 0, []).

%% @doc Return up to Count key-value tuples from the Start key, keys descending. If Start is not
%% a key or Count is not positive, return [].
get_range(Backend, Start, Count, Tree) when is_binary(Start) ->
    Iterator = iterator_from(Backend, Start, Tree),
    do_get_range(Backend, Iterator, Count, 0, []);
get_range(_Backend, _Start, _Count, _Tree) ->
    [].

%% @doc Traverse keys in reverse alphabetical order, applying Fun(Key, Value, Acc).
foldr(Backend, Fun, Acc, Tree) ->
    case is_empty(Backend, Tree) of
        true ->
            Acc;
        false ->
            foldr(Backend, Fun, Acc, Tree, root)
    end.

%% @doc Recompute the root hash, re-hashing only the paths invalidated since the previous
%% computation. HashFun(leaf, {Key, Value}) hashes a leaf, HashFun(node, ChildHashes)
%% combines child hashes. Backend:emit is called at every hashed node, threading Acc.
%% Return {RootHash, Tree, Acc}.
compute_hash(Backend, Tree, HashFun, Acc) ->
    case Backend:get_size(Tree) of
        0 ->
            {<<>>, Tree, Acc};
        _ ->
            progress_init(Backend, Tree),
            Result = do_compute_hash(Backend, Tree, HashFun, Acc, root),
            progress_finish(Backend, Tree),
            Result
    end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

insert(Backend, Key, Value, Tree, Level, Parent) ->
    {KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
    case Backend:get_node(Tree, KeyPrefix) of
        {NodeParent, NodeChildren, NodeHash, NodeSuffix, NodeValue} ->
            {Common, KeySuffix2, NodeSuffix2} = join(KeySuffix, NodeSuffix),
            case {KeySuffix == NodeSuffix, Common == KeySuffix, Common == NodeSuffix} of
                {true, _, _} ->
                    Size = Backend:get_size(Tree),
                    Size2 =
                        case NodeValue of
                            no_value ->
                                Size + 1;
                            _ ->
                                Size
                        end,
                    UpdatedNode = {NodeParent, NodeChildren, no_hash, NodeSuffix, {v, Value}},
                    Tree1 = Backend:put_node(Tree, KeyPrefix, UpdatedNode),
                    Tree2 = Backend:set_size(Tree1, Size2),
                    invalidate_hash(Backend, NodeParent, Tree2);
                {_, _, true} when KeySuffix > NodeSuffix ->
                    insert(Backend, Key, Value, Tree, Level + byte_size(NodeSuffix) + 1, KeyPrefix);
                {_, true, _} when KeySuffix < NodeSuffix ->
                    {Head, NodeSuffix3} = strip_head(NodeSuffix2),
                    UpdatedNodeKey = << KeyPrefix/binary, Common/binary, Head/binary >>,
                    PivotChildren = gb_sets:from_list([UpdatedNodeKey]),
                    PivotNode = {NodeParent, PivotChildren, no_hash, KeySuffix, {v, Value}},
                    UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
                    Size = Backend:get_size(Tree),
                    Tree1 = Backend:put_node(Tree, KeyPrefix, PivotNode),
                    Tree2 = Backend:put_node(Tree1, UpdatedNodeKey, UpdatedNode),
                    Tree3 = Backend:set_size(Tree2, Size + 1),
                    Tree4 = update_children_parent(Backend, UpdatedNodeKey, NodeChildren, Tree3),
                    invalidate_hash(Backend, NodeParent, Tree4);
                {false, false, false} ->
                    {KeyHead, KeySuffix3} = strip_head(KeySuffix2),
                    NewNodeKey = << KeyPrefix/binary, Common/binary, KeyHead/binary >>,
                    NewNode = {KeyPrefix, gb_sets:new(), no_hash, KeySuffix3, {v, Value}},
                    {NodeKeyHead, NodeSuffix3} = strip_head(NodeSuffix2),
                    UpdatedNodeKey = << KeyPrefix/binary, Common/binary, NodeKeyHead/binary >>,
                    UpdatedNode = {KeyPrefix, NodeChildren, NodeHash, NodeSuffix3, NodeValue},
                    PivotChildren = gb_sets:from_list([NewNodeKey, UpdatedNodeKey]),
                    PivotNode = {NodeParent, PivotChildren, no_hash, Common, no_value},
                    Size = Backend:get_size(Tree),
                    Tree1 = Backend:put_node(Tree, NewNodeKey, NewNode),
                    Tree2 = Backend:put_node(Tree1, UpdatedNodeKey, UpdatedNode),
                    Tree3 = Backend:put_node(Tree2, KeyPrefix, PivotNode),
                    Tree4 = Backend:set_size(Tree3, Size + 1),
                    Tree5 = update_children_parent(Backend, UpdatedNodeKey, NodeChildren, Tree4),
                    invalidate_hash(Backend, NodeParent, Tree5)
            end;
        not_found ->
            NewNode = {Parent, gb_sets:new(), no_hash, KeySuffix, {v, Value}},
            {NextParent, Children, _Hash, NextSuffix, ParentValue} = Backend:get_node(Tree, Parent),
            UpdatedChildren = gb_sets:insert(KeyPrefix, Children),
            Size = Backend:get_size(Tree),
            Tree1 = Backend:put_node(Tree, KeyPrefix, NewNode),
            Tree2 = Backend:put_node(Tree1, Parent,
                {NextParent, UpdatedChildren, no_hash, NextSuffix, ParentValue}),
            Tree3 = Backend:set_size(Tree2, Size + 1),
            invalidate_hash(Backend, NextParent, Tree3)
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

update_children_parent(Backend, Key, Children, Tree) ->
    gb_sets:fold(
      fun(ChildKey, Acc) ->
              {_, C, H, S, V} = Backend:get_node(Acc, ChildKey),
              Backend:put_node(Acc, ChildKey, {Key, C, H, S, V})
      end,
      Tree,
      Children
     ).

invalidate_hash(_Backend, no_parent, Tree) ->
    Tree;
invalidate_hash(Backend, Key, Tree) ->
    {Parent, Children, _Hash, Suffix, Value} = Backend:get_node(Tree, Key),
    Tree2 = Backend:put_node(Tree, Key, {Parent, Children, no_hash, Suffix, Value}),
    invalidate_hash(Backend, Parent, Tree2).

strip_head(Binary) ->
    {binary:part(Binary, {0, 1}), binary:part(Binary, {1, byte_size(Binary) - 1})}.

lookup(Backend, Key, Tree, Level) ->
    {KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
    case Backend:get_node(Tree, KeyPrefix) of
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
                            lookup(Backend, Key, Tree, Level + SuffixSize + 1);
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

do_compute_hash(Backend, Tree, HashFun, Acc, KeyPrefix) ->
    {Parent, Children, Hash, Suffix, MaybeValue} = Backend:get_node(Tree, KeyPrefix),
    case Hash of
        no_hash ->
            case gb_sets:is_empty(Children) of
                true ->
                    {v, Value} = MaybeValue,
                    Key = << KeyPrefix/binary, Suffix/binary >>,
                    NewHash = HashFun(leaf, {Key, Value}),
                    progress_tick(Backend, Tree),
                    Tree2 = Backend:put_node(Tree, KeyPrefix,
                        {Parent, gb_sets:new(), NewHash, Suffix, {v, Value}}),
                    {NewHash, Tree2, Backend:emit(Acc, NewHash, KeyPrefix, {Key, Value})};
                false ->
                    {Hashes, Tree2, ChildrenAcc} = gb_sets_foldr(
                        fun(Child, {HashesIn, TreeIn, AccIn}) ->
                            {ChildHash, TreeOut, AccOut} = do_compute_hash(
                                Backend, TreeIn, HashFun, AccIn, Child),
                            {[{ChildHash, Child} | HashesIn], TreeOut, AccOut}
                        end,
                        {[], Tree, Acc},
                        Children
                    ),
                    ChildHashes = [H || {H, _} <- Hashes],
                    {NewHash, NewAcc} =
                        case MaybeValue of
                            {v, Value} ->
                                Key = << KeyPrefix/binary, Suffix/binary >>,
                                ValueHash = HashFun(leaf, {Key, Value}),
                                NodeHash = HashFun(node, [ValueHash | ChildHashes]),
                                ValueAcc = Backend:emit(ChildrenAcc, ValueHash, KeyPrefix,
                                        {Key, Value}),
                                NodeAcc = Backend:emit(ValueAcc, NodeHash, KeyPrefix,
                                        [{ValueHash, KeyPrefix} | Hashes]),
                                {NodeHash, NodeAcc};
                            no_value ->
                                case Hashes of
                                    [{SingleHash, _}] ->
                                        {SingleHash, Backend:emit(ChildrenAcc, SingleHash,
                                                KeyPrefix, Hashes)};
                                    _ ->
                                        NodeHash = HashFun(node, ChildHashes),
                                        {NodeHash, Backend:emit(ChildrenAcc, NodeHash,
                                                KeyPrefix, Hashes)}
                                end
                        end,
                    Tree3 = Backend:put_node(Tree2, KeyPrefix,
                        {Parent, Children, NewHash, Suffix, MaybeValue}),
                    {NewHash, Tree3, NewAcc}
            end;
        _ ->
            {Hash, Tree, Acc}
    end.

foldr(Backend, Fun, Acc, Tree, KeyPrefix) ->
    {_, Children, _, Suffix, MaybeValue} = Backend:get_node(Tree, KeyPrefix),
    case gb_sets:is_empty(Children) of
        true ->
            {v, Value} = MaybeValue,
            Key = << KeyPrefix/binary, Suffix/binary >>,
            Fun(Key, Value, Acc);
        false ->
            Acc2 = gb_sets_foldr(
                fun(Child, ChildrenAcc) ->
                        foldr(Backend, Fun, ChildrenAcc, Tree, Child)
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

delete(Backend, Key, Tree, Level) ->
    {KeyPrefix, KeySuffix} = split_by_pos(Key, Level),
    case Backend:get_node(Tree, KeyPrefix) of
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
                            delete(Backend, Key, Tree, Level + SuffixSize + 1);
                        true ->
                            case MaybeValue of
                                no_value ->
                                    Tree;
                                _ ->
                                    Size = Backend:get_size(Tree),
                                    Tree2 = Backend:set_size(Tree, Size - 1),
                                    case gb_sets:is_empty(Children) of
                                        true ->
                                            delete2(Backend, KeyPrefix, Parent, Tree2);
                                        false ->
                                            Node2 = {Parent, Children, no_hash, Suffix,
                                                no_value},
                                            Tree3 = Backend:put_node(Tree2, KeyPrefix, Node2),
                                            invalidate_hash(Backend, Parent, Tree3)
                                    end
                            end
                    end
            end
    end.

delete2(Backend, Key, Parent, Tree) ->
    Tree2 = Backend:del_node(Tree, Key),
    {ParentParent, ParentChildren, _Hash, Suffix, ParentValue} = Backend:get_node(Tree2, Parent),
    ParentChildren2 = gb_sets:del_element(Key, ParentChildren),
    Tree3 = Backend:put_node(Tree2, Parent,
        {ParentParent, ParentChildren2, no_hash, Suffix, ParentValue}),
    case {Parent == root, gb_sets:is_empty(ParentChildren2), ParentValue} of
        {false, true, no_value} ->
            delete2(Backend, Parent, ParentParent, Tree3);
        _ ->
            invalidate_hash(Backend, ParentParent, Tree3)
    end.

iterator(Backend, Tree) ->
    iterator(Backend, Tree, root).

iterator(Backend, Tree, Key) ->
    {_, Children, _, _, MaybeValue} = NodeData = Backend:get_node(Tree, Key),
    case MaybeValue of
        {v, _Value} ->
            {{Key, NodeData}, Tree};
        no_value ->
            case gb_sets:is_empty(Children) of
                true ->
                    none;
                false ->
                    iterator(Backend, Tree, gb_sets:smallest(Children))
            end
    end.

iterator_from(Backend, Start, Tree) ->
    case lookup(Backend, Start, Tree, 1) of
        not_found ->
            none;
        {Prefix, NodeData} ->
            {{Prefix, NodeData}, Tree}
    end.

do_get_range(_Backend, _Iterator, Count, Count, List) ->
    List;
do_get_range(Backend, Iterator, Count, Got, List) ->
    case next(Backend, Iterator) of
        none ->
            List;
        {{Key, Value}, UpdatedIterator} ->
            do_get_range(Backend, UpdatedIterator, Count, Got + 1, [{Key, Value} | List])
    end.

next(Backend, {{Prefix, {Parent, Children, _Hash, Suffix, {v, Value}}}, Tree}) ->
    Key = << Prefix/binary, Suffix/binary >>,
    {{Key, Value}, get_next_start_from_children(Backend, Prefix, Parent, Children, Tree)};
next(_Backend, none) ->
    none.

get_next_start_from_children(Backend, Key, Parent, Children, Tree) ->
    NextChild =
        case gb_sets:is_empty(Children) of
            true ->
                none;
            false ->
                Child = gb_sets:smallest(Children),
                {Child, Backend:get_node(Tree, Child)}
        end,
    case NextChild of
        none ->
            get_next_start_from_sibling(Backend, Key, Parent, Tree);
        _ ->
            {ChildKey, {_, ChildChildren, _, _, MaybeValue}} = NextChild,
            case MaybeValue of
                no_value ->
                    get_next_start_from_children(Backend, ChildKey, Key, ChildChildren, Tree);
                {v, _} ->
                    {NextChild, Tree}
            end
    end.

get_next_start_from_sibling(_Backend, root, no_parent, _Tree) ->
    none;
get_next_start_from_sibling(Backend, Key, Parent, Tree) ->
    {ParentParent, Children, _, _, _} = Backend:get_node(Tree, Parent),
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
            get_next_start_from_sibling(Backend, Parent, ParentParent, Tree);
        {NextSiblingKey, _} ->
            NextSibling = Backend:get_node(Tree, NextSiblingKey),
            {_, NextSiblingChildren, _, _, MaybeValue} = NextSibling,
            case MaybeValue of
                no_value ->
                    get_next_start_from_children(Backend, NextSiblingKey, Key, NextSiblingChildren,
                            Tree);
                {v, _} ->
                    {{NextSiblingKey, NextSibling}, Tree}
            end
    end.

%% @doc Diagnostic progress logging for compute_hash, gated by the AR_PATRICIA_PROGRESS
%% environment variable (off by default). Logs a line every ?PROGRESS_CHUNK leaves. Uses the
%% process dictionary. compute_hash runs in a single process.
progress_init(Backend, Tree) ->
    case os:getenv("AR_PATRICIA_PROGRESS") of
        V when V == false; V == ""; V == "0"; V == "false" ->
            erlang:erase(pt_progress),
            erlang:erase(pt_progress_gc0);
        _ ->
            Now = erlang:monotonic_time(millisecond),
            {GCs, _, _} = erlang:statistics(garbage_collection),
            erlang:put(pt_progress, {0, Now, Now}),
            erlang:put(pt_progress_gc0, GCs),
            io:format("[~s progress] start mem=~BMB~s~n",
                      [Backend, erlang:memory(total) div (1024 * 1024), Backend:progress_extra(Tree)])
    end.

progress_tick(Backend, Tree) ->
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
                    io:format("[~s progress] leaves=~B total=~Bms chunk=~Bms "
                              "rate=~B/s mem=~BMB gcs=~B~s~n",
                              [Backend, Count2, Now - T0, Now - TLast,
                               (?PROGRESS_CHUNK * 1000) div ChunkMs,
                               erlang:memory(total) div (1024 * 1024), GCs - GC0,
                               Backend:progress_extra(Tree)]),
                    erlang:put(pt_progress, {Count2, T0, Now});
                _ ->
                    erlang:put(pt_progress, {Count2, T0, TLast})
            end
    end.

progress_finish(Backend, Tree) ->
    case erlang:get(pt_progress) of
        undefined ->
            ok;
        {Count, T0, _} ->
            Now = erlang:monotonic_time(millisecond),
            {GCs, _, _} = erlang:statistics(garbage_collection),
            GC0 = erlang:get(pt_progress_gc0),
            io:format("[~s progress] done leaves=~B total=~Bms mem=~BMB gcs=~B~s~n",
                      [Backend, Count, Now - T0, erlang:memory(total) div (1024 * 1024), GCs - GC0,
                       Backend:progress_extra(Tree)]),
            erlang:erase(pt_progress),
            erlang:erase(pt_progress_gc0)
    end.
