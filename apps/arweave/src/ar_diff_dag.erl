%%% @doc A directed acyclic graph that keeps ONE big, expensive-to-replicate entity in full -
%%% at the "sink" vertex - plus a small diff on every edge. The entity's value at any other
%%% vertex can be reconstructed on demand, so the graph represents many versions of the entity
%%% while storing only one of them in full.
%%%
%%% Each vertex is one version of the entity; each edge holds the diff between two adjacent
%%% versions. Only the sink holds a full copy. To obtain the entity at some other vertex, walk
%%% from that vertex down to the sink, collect the diffs along the way, and apply them in
%%% reverse order.
%%%
%%% In Arweave this holds the account tree across the blocks of the consensus window (see
%%% ar_account_tree): each vertex is one block's account tree - one kept in full, the rest as
%%% per-block diffs.
-module(ar_diff_dag).
-test_category([fast]).

-export([new/3, get_sink/1, is_sink/2, is_node/2, add_node/5, update_leaf_source/3,
         update_sink/3, get_metadata/2, get_sink_metadata/1, reconstruct/3, move_sink/4,
         filter/2]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Create a new DAG with a sink node under the given identifier storing the given entity.
new(ID, Entity, Metadata) ->
    {#{ ID => {sink, Entity, {0, Metadata}} }, ID, #{ ID => sets:new() }}.

%% @doc Return the entity stored in the sink node.
get_sink(DAG) ->
    ID = element(2, DAG),
    element(2, maps:get(ID, element(1, DAG))).

%% @doc Return true if the given identifier is the identifier of the sink node.
is_sink({_Sinks, ID, _Sources}, ID) ->
    true;
is_sink(_DAG, _ID) ->
    false.

%% @doc Return true if the node with the given identifier exists.
is_node({Sinks, _Sink, _Sources}, ID) ->
    maps:is_key(ID, Sinks).

%% @doc Create a node with an edge connecting the given source and sink identifiers,
%% directed towards the given sink identifier.
%% If the node with the given sink identifier does not exist or the node with the given source
%% identifier already exists, the call fails with a badkey exception.
add_node(DAG, SourceID, SinkID, Diff, Metadata) when SourceID /= SinkID ->
    assert_exists(SinkID, DAG),
    assert_not_exists(SourceID, DAG),
    {Sinks, Sink, Sources} = DAG,
    SinkSources = maps:get(SinkID, Sources, sets:new()),
    UpdatedSources = Sources#{
                              SinkID => sets:add_element(SourceID, SinkSources),
                              SourceID => sets:new()
                             },
    {_ID, _Entity, {Counter, _Metadata}} = maps:get(SinkID, Sinks),
    {Sinks#{ SourceID => {SinkID, Diff, {Counter + 1, Metadata}} }, Sink, UpdatedSources}.

%% @doc Update the given node via the given function of a diff and a metadata, which
%% returns a "new node identifier, new diff, new metadata" triplet. The node must be
%% a source (must have a sink) and a leaf (must be a sink for no node).
%% If the node does not exist or is not a leaf source, the call fails with a badkey exception.
update_leaf_source(DAG, ID, UpdateFun) ->
    assert_exists(ID, DAG),
    assert_not_sink(ID, DAG),
    {#{ ID := {SinkID, Diff, {Counter, Metadata}} } = Sinks, Sink, Sources} = DAG,
    case sets:is_empty(maps:get(ID, Sources, sets:new())) of
        false ->
            error({badkey, ID});
        true ->
            {NewID, UpdatedDiff, UpdatedMetadata} = UpdateFun(Diff, Metadata),
            Sinks2 = maps:remove(ID, Sinks),
            Sources2 = maps:remove(ID, Sources),
            Set = sets:add_element(NewID, sets:del_element(ID, maps:get(SinkID, Sources))),
            Sources3 = Sources2#{ SinkID => Set },
            {Sinks2#{ NewID => {SinkID, UpdatedDiff, {Counter, UpdatedMetadata}} }, Sink,
             Sources3}
    end.

%% @doc Update the sink via the given function of an entity and a metadata, which
%% returns a "new node identifier, new entity, new metadata" triplet.
%% If the node does not exist or is not a sink, the call fails with a badkey exception.
update_sink({Sinks, ID, Sources}, ID, UpdateFun) ->
    #{ ID := {sink, Entity, {Counter, Metadata}} } = Sinks,
    {NewID, NewEntity, NewMetadata} = UpdateFun(Entity, Metadata),
    Sinks2 = maps:remove(ID, Sinks),
    Sinks3 = Sinks2#{ NewID => {sink, NewEntity, {Counter, NewMetadata}} },
    {Set, Sources2} =
        case maps:take(ID, Sources) of
            error ->
                {sets:new(), Sources};
            Update ->
                Update
        end,
    Sinks4 = sets:fold(
               fun(SourceID, Acc) ->
                       {ID, Diff, Meta} = maps:get(SourceID, Acc),
                       Acc#{ SourceID => {NewID, Diff, Meta} }
               end,
               Sinks3,
               Set
              ),
    Sources3 = Sources2#{ NewID => Set },
    {Sinks4, NewID, Sources3};
update_sink(_DAG, ID, _Fun) ->
    error({badkey, ID}).

%% @doc Return metadata stored at the given node. If the node with the given identifier
%% does not exist, the call fails with a badkey exception.
get_metadata(DAG, ID) ->
    element(2, element(3, maps:get(ID, element(1, DAG)))).

%% @doc Return metadata stored at the sink node. If the node with the given identifier
%% does not exist, the call fails with a badkey exception.
get_sink_metadata(DAG) ->
    ID = element(2, DAG),
    get_metadata(DAG, ID).

%% @doc Reconstruct the entity corresponding to the given node using
%% the given diff application function - a function of a diff and an entity.
%% If the node with the given identifier does not exist, returns {error, not_found}.
reconstruct(DAG, ID, ApplyDiffFun) ->
    Sinks = element(1, DAG),
    case maps:is_key(ID, Sinks) of
        true ->
            reconstruct(DAG, ID, ApplyDiffFun, []);
        false ->
            {error, not_found}
    end.

%% @doc Make the given node the sink node. The diffs are reversed
%% according to the given function of a diff and an entity.
%% The new entity is constructed by applying the diffs on the path from the previous
%% sink to the new one using the given diff application function of a diff and an entity.
%% If the node with the given identifier does not exist, the call fails with a badkey exception.
move_sink(DAG, ID, ApplyDiffFun, ReverseDiffFun) ->
    assert_exists(ID, DAG),
    move_sink(DAG, ID, ApplyDiffFun, ReverseDiffFun, []).

%% @doc Remove the nodes further away from the sink than the given distance.
filter({Sinks, ID, Sources}, Depth) ->
    {sink, _Entity, {SinkCounter, _Metadata}} = maps:get(ID, Sinks),
    {ToRemove, Sources2} = filter(maps:iterator(Sinks), SinkCounter, Depth, Sources, sets:new()),
    {UpdatedSinks, UpdatedSources} = sets:fold(
                                       fun(RemoveID, {CurrentSinks, CurrentSources}) ->
                                               #{ RemoveID := {SinkID, _CurrentEntity, _CurrentMetadata} } = CurrentSinks,
                                               CurrentSources2 =
                                                   case sets:is_element(SinkID, ToRemove) of
                                                       false ->
                                                           Set = maps:get(SinkID, CurrentSources, sets:new()),
                                                           maps:put(
                                                             SinkID,
                                                             sets:del_element(RemoveID, Set),
                                                             CurrentSources
                                                            );
                                                       true ->
                                                           CurrentSources
                                                   end,
                                               {maps:remove(RemoveID, CurrentSinks), CurrentSources2}
                                       end,
                                       {Sinks, Sources2},
                                       ToRemove
                                      ),
    {UpdatedSinks, ID, UpdatedSources}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

assert_exists(ID, {Sinks, _, _}) ->
    case maps:is_key(ID, Sinks) of
        true ->
            ok;
        false ->
            error({badkey, ID})
    end.

assert_not_exists(ID, {Sinks, _, _}) ->
    case maps:is_key(ID, Sinks) of
        true ->
            error({badkey, ID});
        false ->
            ok
    end.

assert_not_sink(ID, {_, ID, _}) ->
    error({badkey, ID});
assert_not_sink(_ID, _DAG) ->
    ok.

reconstruct(DAG, ID, ApplyDiffFun, Diffs) ->
    case DAG of
        {#{ ID := {sink, Entity, _Meta} }, ID, _} ->
            lists:foldl(ApplyDiffFun, Entity, Diffs);
        {#{ ID := {SinkID, Diff, _Meta} }, _Sink, _Sinks} ->
            reconstruct(DAG, SinkID, ApplyDiffFun, [Diff | Diffs])
    end.

move_sink(DAG, ID, ApplyDiffFun, ReverseDiffFun, Diffs) ->
    case DAG of
        {#{ ID := {sink, Entity, Metadata} }, ID, _Sources} ->
            {UpdatedSinkID, UpdatedEntity, UpdatedMetadata, UpdatedDAG} = lists:foldl(
                                                                            fun({SinkID, Diff, Meta}, {SourceID, CurrentEntity, CurrentMeta, CurrentDAG}) ->
                                                                                    ReversedDiff = ReverseDiffFun(Diff, CurrentEntity),
                                                                                    {Sinks, _Sink, Sources} = CurrentDAG,
                                                                                    Sinks2 = Sinks#{ SourceID => {SinkID, ReversedDiff, CurrentMeta} },
                                                                                    SourceIDSet2 = sets:del_element(SinkID, maps:get(SourceID, Sources)),
                                                                                    SinkIDSet2 =
                                                                                        sets:add_element(SourceID, maps:get(SinkID, Sources, sets:new())),
                                                                                    Sources2 = Sources#{ SinkID => SinkIDSet2, SourceID => SourceIDSet2 },
                                                                                    {SinkID, ApplyDiffFun(Diff, CurrentEntity), Meta, {Sinks2, SinkID, Sources2}}
                                                                            end,
                                                                            {ID, Entity, Metadata, DAG},
                                                                            Diffs
                                                                           ),
            {UpdatedSinks, UpdatedSinkID, UpdatedSources} = UpdatedDAG,
            UpdatedSinks2 = UpdatedSinks#{
                                          UpdatedSinkID => {sink, UpdatedEntity, UpdatedMetadata}
                                         },
            {UpdatedSinks2, UpdatedSinkID, UpdatedSources};
        {#{ ID := {SinkID, Diff, Metadata} }, _Sink, _Sinks} ->
            move_sink(DAG, SinkID, ApplyDiffFun, ReverseDiffFun, [{ID, Diff, Metadata} | Diffs])
    end.

filter(SinkIterator, SinkCounter, Depth, Sources, ToRemove) ->
    case maps:next(SinkIterator) of
        none ->
            {ToRemove, Sources};
        {ID, {_ID, _Entity, {Counter, _Metadata}}, NextIterator} ->
            case sets:is_element(ID, ToRemove) of
                true ->
                    filter(NextIterator, SinkCounter, Depth, Sources, ToRemove);
                false ->
                    case abs(Counter - SinkCounter) =< Depth of
                        true ->
                            filter(NextIterator, SinkCounter, Depth, Sources, ToRemove);
                        false ->
                            {Sources2, ToRemove2} =
                                extend_with_subtree_identifiers(ID, {Sources, ToRemove}),
                            filter(NextIterator, SinkCounter, Depth, Sources2, ToRemove2)
                    end
            end
    end.

extend_with_subtree_identifiers(ID, {Sources, ToRemove}) ->
    sets:fold(
      fun(RemoveID, Acc) ->
              extend_with_subtree_identifiers(RemoveID, Acc)
      end,
      {maps:remove(ID, Sources), sets:add_element(ID, ToRemove)},
      maps:get(ID, Sources, sets:new())
     ).

%%%===================================================================
%%% Tests.
%%%===================================================================

diff_dag_test() ->
    %% node-1: {0, meta_1}
    DAG1 = new("node-1", 0, meta_1),
    ?assertEqual(0, get_sink(DAG1)),
    ?assertEqual(DAG1, filter(DAG1, 0)),
    ?assertEqual(DAG1, filter(DAG1, 1)),
    ?assertEqual(DAG1, filter(DAG1, 2)),
    ?assertEqual(0, reconstruct(DAG1, "node-1", fun(_Diff, _E) -> not_called end)),
    ?assertEqual(
       {error, not_found},
       reconstruct(DAG1, "node-2", fun(_Diff, _E) -> not_called end)
      ),
    ?assertEqual(meta_1, get_metadata(DAG1, "node-1")),
    %% node-1: {0, meta_1} <- node-2-1: {1, meta_2_1}
    DAG2 = add_node(DAG1, "node-2-1", "node-1", 1, meta_2_1),
    ?assertEqual(0, get_sink(DAG2)),
    ?assertEqual(DAG2, filter(DAG2, 1)),
    ?assertEqual(DAG1, filter(DAG2, 0)),
    ?assertEqual(DAG2, filter(DAG2, 2)),
    ?assertEqual(DAG2, filter(DAG2, 3)),
    ?assertEqual(1, reconstruct(DAG2, "node-2-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(-1, reconstruct(DAG2, "node-2-1", fun(Diff, E) -> E - Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG2, "node-1")),
    ?assertEqual(meta_2_1, get_metadata(DAG2, "node-2-1")),
    %% node-1: {0, meta_1} <- node-2-1: {2, meta_2_2}
    DAG3 = update_leaf_source(DAG2, "node-2-1", fun(D, _M) -> {"node-2-1", D + 1, meta_2_2} end),
    ?assertEqual(0, get_sink(DAG3)),
    ?assertEqual(DAG1, filter(DAG3, 0)),
    ?assertEqual(DAG3, filter(DAG3, 1)),
    ?assertEqual(2, reconstruct(DAG3, "node-2-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG3, "node-2-1")),
    ?assertException(error, {badkey, "node-1"}, update_leaf_source(DAG2, "node-1", no_function)),
    %% node-1: {0, meta_1} <- node-2-2: {1, meta_2_2}
    DAG4 = update_leaf_source(DAG3, "node-2-1", fun(D, M) -> {"node-2-2", D - 1, M} end),
    ?assertEqual(0, get_sink(DAG4)),
    ?assertEqual(1, reconstruct(DAG4, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG4, "node-2-2")),
    ?assertException(error, {badkey, "node-2-1"}, get_metadata(DAG4, "node-2-1")),
    %% node-1: {0, meta_1} <- node-2-2: {1, meta_2_2}
    %%                     <- node-2-3: {2, meta_2_3}
    DAG5 = add_node(DAG4, "node-2-3", "node-1", 2, meta_2_3),
    ?assertEqual(1, reconstruct(DAG5, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(2, reconstruct(DAG5, "node-2-3", fun(Diff, E) -> E + Diff end)),
    %% node-1: {0, meta_1} <- node-2-2: {1, meta_2_2}
    %%                     <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG6 = add_node(DAG5, "node-3-1", "node-2-3", -3, meta_3_1),
    ?assertEqual(1, reconstruct(DAG6, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(-1, reconstruct(DAG6, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(DAG5, filter(DAG6, 1)),
    ?assertEqual(DAG1, filter(DAG6, 0)),
    %% node-1: {-2, meta_1} <- node-2-2: {1, meta_2_2}
    %%                      -> node-2-3: {3, meta_2_3} -> node-3-1: {-1, meta_3_1}
    DAG7 = move_sink(DAG6, "node-3-1", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assertEqual(-1, get_sink(DAG7)),
    ?assertEqual(1, reconstruct(DAG7, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG7, "node-2-2")),
    ?assertEqual(0, reconstruct(DAG7, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG7, "node-1")),
    ?assertEqual(2, reconstruct(DAG7, "node-2-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_3, get_metadata(DAG7, "node-2-3")),
    ?assertEqual(-1, reconstruct(DAG7, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3_1, get_metadata(DAG7, "node-3-1")),
    ?assert(not is_node(filter(DAG7, 0), "node-2-3")),
    ?assert(not is_node(filter(DAG7, 0), "node-1")),
    ?assert(not is_node(filter(DAG7, 0), "node-2-2")),
    ?assert(is_node(filter(DAG7, 0), "node-3-1")),
    ?assert(is_node(filter(DAG7, 1), "node-3-1")),
    ?assert(is_node(filter(DAG7, 1), "node-2-3")),
    ?assert(not is_node(filter(DAG7, 1), "node-1")),
    ?assert(is_node(filter(DAG7, 1), "node-3-1")),
    ?assert(not is_node(filter(DAG7, 1), "node-2-2")),
    ?assertEqual(DAG7, filter(DAG7, 2)),
    ?assertEqual(DAG7, filter(DAG7, 3)),
    %% node-1: {-1, meta_1} -> node-2-2: {1, meta_2_2}
    %%                      <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG9 = move_sink(DAG7, "node-2-2", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assertEqual(1, get_sink(DAG9)),
    ?assertEqual(0, reconstruct(DAG9, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG9, "node-1")),
    ?assertEqual(2, reconstruct(DAG9, "node-2-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_3, get_metadata(DAG9, "node-2-3")),
    ?assertEqual(-1, reconstruct(DAG9, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3_1, get_metadata(DAG9, "node-3-1")),
    %% node-1: {-1, meta_1} -> node-2-2: {1, meta_2_2} <- node-3-2: {10, meta_3_2}
    %%                      <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG10 = add_node(DAG9, "node-3-2", "node-2-2", 10, meta_3_2),
    %% node-1: {-1, meta_1} -> node-2-2: {-10, meta_2_2} -> node-3-2: {11, meta_3_2}
    %%                      <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG11 =
        move_sink(DAG10, "node-3-2", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assertEqual(11, get_sink(DAG11)),
    ?assertEqual(1, reconstruct(DAG11, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG11, "node-2-2")),
    ?assertEqual(0, reconstruct(DAG11, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG11, "node-1")),
    ?assertEqual(2, reconstruct(DAG11, "node-2-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_3, get_metadata(DAG11, "node-2-3")),
    ?assertEqual(-1, reconstruct(DAG11, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3_1, get_metadata(DAG11, "node-3-1")),
    ?assertException(
       error, {badkey, "node-2-2"},
       update_leaf_source(DAG11, "node-2-2", no_function)
      ),
    %% node-1: {-1, meta_1} -> node-2-2: {-10, meta_2_2} -> node-3-2: {12, meta_3_2}
    %%                      <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG12 = update_sink(DAG11, "node-3-2", fun(11, meta_3_2) -> {"node-3-2", 12, meta_3_2} end),
    ?assertEqual(12, get_sink(DAG12)),
    ?assertEqual(meta_3_2, get_metadata(DAG12, "node-3-2")),
    ?assertEqual(2, reconstruct(DAG12, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG12, "node-2-2")),
    ?assertEqual(1, reconstruct(DAG12, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG12, "node-1")),
    ?assertEqual(3, reconstruct(DAG12, "node-2-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_3, get_metadata(DAG12, "node-2-3")),
    ?assertEqual(0, reconstruct(DAG12, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3_1, get_metadata(DAG12, "node-3-1")),
    ?assertException(error, {badkey, "node-2-2"}, update_sink(DAG11, "node-2-2", no_function)),
    ?assertException(error, {badkey, "node-3-1"}, update_sink(DAG11, "node-3-1", no_function)),
    %% node-1: {-1, meta_1} -> node-2-2: {-10, meta_2_2} -> new-node-3-2: {13, meta_3_2}
    %%                      <- node-2-3: {2, meta_2_3} <- node-3-1: {-3, meta_3_1}
    DAG13 =
        update_sink(DAG12, "node-3-2", fun(12, meta_3_2) -> {"new-node-3-2", 13, meta_3_2} end),
    ?assertEqual(13, get_sink(DAG13)),
    ?assertEqual(meta_3_2, get_metadata(DAG13, "new-node-3-2")),
    ?assertException(error, {badkey, "node-3-2"}, get_metadata(DAG13, "node-3-2")),
    ?assertEqual(3, reconstruct(DAG13, "node-2-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_2, get_metadata(DAG13, "node-2-2")),
    ?assertEqual(2, reconstruct(DAG13, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_1, get_metadata(DAG13, "node-1")),
    ?assertEqual(4, reconstruct(DAG13, "node-2-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_2_3, get_metadata(DAG13, "node-2-3")),
    ?assertEqual(1, reconstruct(DAG13, "node-3-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3_1, get_metadata(DAG13, "node-3-1")),
    %% node-1: {0, meta_1} <- node-2: {1, meta_2}
    DAG14 = add_node(new("node-1", 0, meta_1), "node-2", "node-1", 1, meta_2),
    ?assertEqual(0, get_sink(DAG14)),
    ?assertEqual(1, reconstruct(DAG14, "node-2", fun(Diff, E) -> E + Diff end)),
    %% node-1: {-1, meta_1} -> node-2: {1, meta_2}
    DAG15 = move_sink(DAG14, "node-2", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assertEqual(1, get_sink(DAG15)),
    ?assertEqual(0, reconstruct(DAG15, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertException(error, {badkey, "node-2"}, add_node(DAG15, "node-2", "node-1", 1, meta_1)),
    ?assertException(error, {badkey, "node-1"}, add_node(DAG15, "node-1", "node-2", 1, meta_2)).

is_sink_test() ->
    %% node-1 (sink) <- node-2: {1, meta_2}
    DAG1 = new("node-1", 0, meta_1),
    ?assert(is_sink(DAG1, "node-1")),
    ?assert(not is_sink(DAG1, "node-2")),
    DAG2 = add_node(DAG1, "node-2", "node-1", 1, meta_2),
    ?assert(is_sink(DAG2, "node-1")),
    ?assert(not is_sink(DAG2, "node-2")),
    %% After moving the sink to node-2, is_sink follows the sink.
    DAG3 = move_sink(DAG2, "node-2", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assert(is_sink(DAG3, "node-2")),
    ?assert(not is_sink(DAG3, "node-1")).

is_node_test() ->
    %% node-1 (sink) <- node-2: {1, meta_2}
    DAG1 = new("node-1", 0, meta_1),
    ?assert(is_node(DAG1, "node-1")),
    ?assert(not is_node(DAG1, "node-2")),
    ?assert(not is_node(DAG1, "never-existed")),
    DAG2 = add_node(DAG1, "node-2", "node-1", 1, meta_2),
    ?assert(is_node(DAG2, "node-1")),
    ?assert(is_node(DAG2, "node-2")),
    ?assert(not is_node(DAG2, "node-3")),
    %% A node removed via update_leaf_source is no longer a node under its old identifier.
    DAG3 = update_leaf_source(DAG2, "node-2", fun(D, M) -> {"node-2-renamed", D, M} end),
    ?assert(not is_node(DAG3, "node-2")),
    ?assert(is_node(DAG3, "node-2-renamed")).

get_sink_metadata_test() ->
    %% Sink metadata is readable right after new/3.
    DAG1 = new("node-1", 0, meta_1),
    ?assertEqual(meta_1, get_sink_metadata(DAG1)),
    ?assertEqual(get_metadata(DAG1, "node-1"), get_sink_metadata(DAG1)),
    %% Adding a non-sink node does not change which metadata get_sink_metadata returns.
    DAG2 = add_node(DAG1, "node-2", "node-1", 1, meta_2),
    ?assertEqual(meta_1, get_sink_metadata(DAG2)),
    %% Moving the sink makes get_sink_metadata return the new sink's metadata.
    DAG3 = move_sink(DAG2, "node-2", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assertEqual(meta_2, get_sink_metadata(DAG3)),
    %% update_sink replaces the sink metadata.
    DAG4 = update_sink(DAG3, "node-2", fun(E, _M) -> {"node-2", E, meta_2_updated} end),
    ?assertEqual(meta_2_updated, get_sink_metadata(DAG4)).

update_sink_errors_test() ->
    %% node-1 (sink) <- node-2: {1, meta_2}
    DAG = add_node(new("node-1", 0, meta_1), "node-2", "node-1", 1, meta_2),
    %% Calling update_sink on a non-sink source fails with badkey.
    ?assertException(error, {badkey, "node-2"}, update_sink(DAG, "node-2", no_function)),
    %% Calling update_sink on a non-existent node fails with badkey.
    ?assertException(error, {badkey, "node-3"}, update_sink(DAG, "node-3", no_function)),
    %% The successful clause keeps the same counter and rewires sources to the new sink id.
    DAG2 = update_sink(DAG, "node-1", fun(E, _M) -> {"node-1-renamed", E, meta_1_new} end),
    ?assert(is_sink(DAG2, "node-1-renamed")),
    ?assertEqual(meta_1_new, get_sink_metadata(DAG2)),
    ?assertEqual(0, get_sink(DAG2)),
    ?assertEqual(1, reconstruct(DAG2, "node-2", fun(Diff, E) -> E + Diff end)).

reconstruct_errors_test() ->
    DAG = add_node(new("node-1", 0, meta_1), "node-2", "node-1", 5, meta_2),
    %% Unknown identifier returns {error, not_found} without applying any diff.
    ?assertEqual(
       {error, not_found},
       reconstruct(DAG, "missing", fun(_Diff, _E) -> not_called end)
      ),
    %% Reconstructing the sink itself returns the entity untouched (no diffs applied).
    ?assertEqual(0, reconstruct(DAG, "node-1", fun(_Diff, _E) -> not_called end)),
    ?assertEqual(5, reconstruct(DAG, "node-2", fun(Diff, E) -> E + Diff end)).

update_leaf_source_non_leaf_test() ->
    %% node-1 (sink) <- node-2: {1, meta_2} <- node-3: {2, meta_3}
    DAG0 = add_node(new("node-1", 0, meta_1), "node-2", "node-1", 1, meta_2),
    DAG = add_node(DAG0, "node-3", "node-2", 2, meta_3),
    %% node-2 is a source but not a leaf (node-3 points at it) -> badkey.
    ?assertException(error, {badkey, "node-2"}, update_leaf_source(DAG, "node-2", no_function)),
    %% node-1 is the sink (not a source) -> badkey.
    ?assertException(error, {badkey, "node-1"}, update_leaf_source(DAG, "node-1", no_function)),
    %% A non-existent node -> badkey.
    ?assertException(error, {badkey, "node-4"}, update_leaf_source(DAG, "node-4", no_function)),
    %% node-3 is a leaf source -> succeeds.
    DAG2 = update_leaf_source(DAG, "node-3", fun(D, M) -> {"node-3", D + 10, M} end),
    ?assertEqual(13, reconstruct(DAG2, "node-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(meta_3, get_metadata(DAG2, "node-3")).

filter_multi_branch_test() ->
    %% node-1 (sink, counter 0)
    %%   <- node-a1: {c1} <- node-a2: {c2} <- node-a3: {c3}
    %%   <- node-b1: {c1}
    DAG0 = new("node-1", 0, meta_1),
    DAGa1 = add_node(DAG0, "node-a1", "node-1", 1, meta_a1),
    DAGa2 = add_node(DAGa1, "node-a2", "node-a1", 2, meta_a2),
    DAGa3 = add_node(DAGa2, "node-a3", "node-a2", 3, meta_a3),
    DAG = add_node(DAGa3, "node-b1", "node-1", 9, meta_b1),
    %% Depth 0 keeps only the sink.
    F0 = filter(DAG, 0),
    ?assert(is_node(F0, "node-1")),
    ?assert(not is_node(F0, "node-a1")),
    ?assert(not is_node(F0, "node-a2")),
    ?assert(not is_node(F0, "node-a3")),
    ?assert(not is_node(F0, "node-b1")),
    %% Depth 1 keeps the sink and its immediate sources (counter distance 1).
    F1 = filter(DAG, 1),
    ?assert(is_node(F1, "node-1")),
    ?assert(is_node(F1, "node-a1")),
    ?assert(is_node(F1, "node-b1")),
    ?assert(not is_node(F1, "node-a2")),
    ?assert(not is_node(F1, "node-a3")),
    %% Depth 2 reaches node-a2 along the a-branch; node-a3 (distance 3) is pruned.
    F2 = filter(DAG, 2),
    ?assert(is_node(F2, "node-a2")),
    ?assert(not is_node(F2, "node-a3")),
    %% Depth 3 keeps the entire DAG unchanged.
    ?assertEqual(DAG, filter(DAG, 3)),
    ?assertEqual(DAG, filter(DAG, 4)).

filter_prunes_subtree_test() ->
    %% Pruning node-a1 (the closer node) must also drop the whole subtree below it,
    %% exercising extend_with_subtree_identifiers.
    DAG0 = new("node-1", 0, meta_1),
    DAGa1 = add_node(DAG0, "node-a1", "node-1", 1, meta_a1),
    DAGa2 = add_node(DAGa1, "node-a2", "node-a1", 2, meta_a2),
    DAG = add_node(DAGa2, "node-a3", "node-a2", 3, meta_a3),
    F0 = filter(DAG, 0),
    ?assert(not is_node(F0, "node-a1")),
    ?assert(not is_node(F0, "node-a2")),
    ?assert(not is_node(F0, "node-a3")),
    %% The sink's source set no longer references the pruned child.
    ?assertException(error, {badkey, "node-a1"}, get_metadata(F0, "node-a1")),
    %% Re-adding the pruned identifiers must succeed (source set was cleaned up).
    F0b = add_node(F0, "node-a1", "node-1", 7, meta_a1b),
    ?assertEqual(7, reconstruct(F0b, "node-a1", fun(Diff, E) -> E + Diff end)).

move_sink_multi_hop_test() ->
    %% node-1 (sink) <- node-2: {1, meta_2} <- node-3: {2, meta_3} <- node-4: {4, meta_4}
    DAG0 = add_node(new("node-1", 0, meta_1), "node-2", "node-1", 1, meta_2),
    DAG1 = add_node(DAG0, "node-3", "node-2", 2, meta_3),
    DAG = add_node(DAG1, "node-4", "node-3", 4, meta_4),
    ?assertEqual(7, reconstruct(DAG, "node-4", fun(Diff, E) -> E + Diff end)),
    %% Move the sink three hops to node-4.
    DAG2 = move_sink(DAG, "node-4", fun(Diff, E) -> E + Diff end, fun(Diff, _E) -> -Diff end),
    ?assert(is_sink(DAG2, "node-4")),
    ?assertEqual(7, get_sink(DAG2)),
    %% Every original node is still reachable with its original reconstructed value...
    ?assertEqual(0, reconstruct(DAG2, "node-1", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(1, reconstruct(DAG2, "node-2", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(3, reconstruct(DAG2, "node-3", fun(Diff, E) -> E + Diff end)),
    ?assertEqual(7, reconstruct(DAG2, "node-4", fun(Diff, E) -> E + Diff end)),
    %% ...and metadata is preserved across the move.
    ?assertEqual(meta_1, get_metadata(DAG2, "node-1")),
    ?assertEqual(meta_2, get_metadata(DAG2, "node-2")),
    ?assertEqual(meta_3, get_metadata(DAG2, "node-3")),
    ?assertEqual(meta_4, get_metadata(DAG2, "node-4")),
    %% Counters (and therefore filter depth) are preserved: filtering with depth 3
    %% keeps the whole DAG, depth 0 keeps only the new sink.
    ?assertEqual(DAG2, filter(DAG2, 3)),
    F0 = filter(DAG2, 0),
    ?assert(is_node(F0, "node-4")),
    ?assert(not is_node(F0, "node-1")),
    ?assert(not is_node(F0, "node-2")),
    ?assert(not is_node(F0, "node-3")).

new_sink_metadata_test() ->
    %% new/3 stores the sink entity and metadata; get_metadata on the sink returns it.
    DAG = new("node-1", entity_0, meta_1),
    ?assertEqual(entity_0, get_sink(DAG)),
    ?assertEqual(meta_1, get_metadata(DAG, "node-1")),
    ?assertEqual(meta_1, get_sink_metadata(DAG)),
    ?assert(is_sink(DAG, "node-1")),
    ?assert(is_node(DAG, "node-1")).
