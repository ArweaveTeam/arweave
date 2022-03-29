-module(ar_block_index).

-export([init/1, update/2, member/1, get_list/1, get_element_by_height/1,
		get_block_bounds/2, get_intersection/2, get_intersection/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Store the given block index in ETS.
init(BI) ->
	init(lists:reverse(BI), 0).

%% @doc Insert the new block index elements from BI and remove the N orphaned ones.
update([], 0) ->
	ok;
update(BI, 0) ->
	{_WeaveSize, Height, _H, _TXRoot} = ets:last(block_index),
	update2(BI, Height + 1);
update(BI, N) ->
	ets:delete(block_index, ets:last(block_index)),
	update(BI, N - 1).

%% @doc Return true if the given block hash is found in the index.
member(H) ->
	member(H, ets:last(block_index)).

%% @doc Return the list of {H, WeaveSize, TXRoot} triplets up to the given Height (including)
%% sorted from latest to earliest.
get_list(Height) ->
	get_list([], ets:first(block_index), -1, Height).

%% @doc Return the {H, WeaveSize, TXRoot} triplet for the given Height or not_found.
get_element_by_height(Height) ->
	case catch ets:slot(block_index, Height) of
		{'EXIT', _} ->
			not_found;
		'$end_of_table' ->
			not_found;
		[{{WeaveSize, Height, H, TXRoot}}] ->
			{H, WeaveSize, TXRoot}
	end.

%% @doc Return {BlockStartOffset, BlockEndOffset, TXRoot} where Offset >= BlockStartOffset,
%% Offset < BlockEndOffset.
get_block_bounds(Offset, BI) ->
	%% The get_block_bounds2 check is not strictly necessary on mainnet because
	%% the given Offset is below the threshold which may be reorganized by construction.
	%% In tests, however, we set SEARCH_SPACE_UPPER_BOUND_DEPTH=2 and have reorgs that
	%% are deeper than that.
	case get_block_bounds2(Offset, BI) of
		not_found ->
			{WeaveSize, Height, _H, TXRoot} = Key = ets:next(block_index, {Offset, n, n, n}),
			case Height of
				0 ->
					{0, WeaveSize, TXRoot};
				_ ->
					{PrevWeaveSize, _, _, _} = ets:prev(block_index, Key),
					{PrevWeaveSize, WeaveSize, TXRoot}
			end;
		Element ->
			Element
	end.

%% @doc Return {Height, {H, WeaveSize, TXRoot}} with the  triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection(Height, _BI) when Height < 0 ->
	no_intersection;
get_intersection(Height, BI) ->
	get_intersection(Height, BI, catch ets:slot(block_index, Height)).

%% @doc Return the {H, WeaveSize, TXRoot} triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection([]) ->
	no_intersection;
get_intersection(BI) ->
	{H, WeaveSize, _TXRoot} = lists:last(BI),
	get_intersection2({H, WeaveSize}, tl(lists:reverse(BI)),
			ets:next(block_index, {WeaveSize - 1, n, n, n})).

%%%===================================================================
%%% Private functions.
%%%===================================================================

init([], _Height) ->
	ok;
init([{H, WeaveSize, TXRoot} | BI], Height) ->
	ets:insert(block_index, {{WeaveSize, Height, H, TXRoot}}),
	init(BI, Height + 1).

update2([], _Height) ->
	ok;
update2([{H, WeaveSize, TXRoot} | BI], Height) ->
	ets:insert(block_index, {{WeaveSize, Height, H, TXRoot}}),
	update2(BI, Height + 1).

member(H, {_, _, H, _}) ->
	true;
member(_H, '$end_of_table') ->
	false;
member(H, Key) ->
	member(H, ets:prev(block_index, Key)).

get_list(BI, '$end_of_table', _Height, _MaxHeight) ->
	BI;
get_list(BI, _Elem, Height, MaxHeight) when Height >= MaxHeight ->
	BI;
get_list(BI, {WeaveSize, _Height, H, TXRoot} = Key, Height, MaxHeight) ->
	get_list([{H, WeaveSize, TXRoot} | BI], ets:next(block_index, Key), Height + 1, MaxHeight).

get_block_bounds2(Offset, [{_, WeaveSize2, TXRoot}, {_, WeaveSize1, _} | _])
		when Offset >= WeaveSize1, Offset < WeaveSize2 ->
	{WeaveSize1, WeaveSize2, TXRoot};
get_block_bounds2(Offset, [_, Element | BI]) ->
	get_block_bounds2(Offset, [Element | BI]);
get_block_bounds2(_Offset, _BI) ->
	not_found.

get_intersection(Height, BI, {'EXIT', _}) ->
	get_intersection(Height - 1, BI, catch ets:slot(block_index, Height - 1));
get_intersection(_Height, _BI, '$end_of_table') ->
	no_intersection;
get_intersection(Height, [{H, _, _} = Elem | _], {_, Height, H, _}) ->
	{Height, Elem};
get_intersection(Height, [{H, _, _} = Elem | _], [{{_, Height, H, _}}]) ->
	{Height, Elem};
get_intersection(Height, [_ | BI], [{Elem}]) ->
	get_intersection(Height - 1, BI, ets:prev(block_index, Elem));
get_intersection(Height, [_ | BI], Elem) ->
	get_intersection(Height - 1, BI, ets:prev(block_index, Elem));
get_intersection(_Height, [], _Elem) ->
	no_intersection.

get_intersection2(_, _, '$end_of_table') ->
	no_intersection;
get_intersection2({_, WeaveSize}, _, {WeaveSize2, _, _, _}) when WeaveSize2 > WeaveSize ->
	no_intersection;
get_intersection2({H, WeaveSize}, BI, {WeaveSize, _, H, TXRoot} = Elem) ->
	get_intersection3(ets:next(block_index, Elem), BI, {H, WeaveSize, TXRoot});
get_intersection2({H, WeaveSize}, BI, {WeaveSize, _, _, _} = Elem) ->
	get_intersection2({H, WeaveSize}, BI, ets:next(block_index, Elem)).

get_intersection3({WeaveSize, _, H, TXRoot} = Key, [{H, WeaveSize, TXRoot} | BI], _Elem) ->
	get_intersection3(ets:next(block_index, Key), BI, {H, WeaveSize, TXRoot});
get_intersection3(_, _, {H, WeaveSize, TXRoot}) ->
	{H, WeaveSize, TXRoot}.
