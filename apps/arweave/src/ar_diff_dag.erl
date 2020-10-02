%%% @doc A directed acyclic graph with a single sink node. The sink node is supposed
%%% to store some big expensive to replicate entity (e.g., a wallet tree). Edges store
%%% diffs. To compute a representation of the entity corresponding to a particular vertice,
%%% one needs to walk from this vertice down to the sink node, collect all the diffs, and
%%% apply them in the reverse order.
-module(ar_diff_dag).

-export([
	new/3,
	get_sink/1,
	is_sink/2,
	is_node/2,
	add_node/5,
	update_leaf_source/3,
	update_sink/3,
	get_metadata/2,
	reconstruct/3,
	move_sink/4,
	filter/2
]).

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
			{Sinks2#{ NewID => {SinkID, UpdatedDiff, {Counter, UpdatedMetadata}} }, Sink, Sources3}
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

%% @doc Return metadata stored at the given node.
%% If the node with the given identifier does not exist, the call fails with a badkey exception.
get_metadata(DAG, ID) ->
	element(2, element(3, maps:get(ID, element(1, DAG)))).

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
