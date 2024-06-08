-module(ar_block_index).

-export([init/1, update/2, member/1, get_list/1, get_list_by_hash/1, get_element_by_height/1,
		get_block_bounds/1, get_intersection/2, get_intersection/1, get_range/2]).

-include_lib("arweave/include/ar.hrl").

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

%% @doc Return the list of {H, WeaveSize, TXRoot} triplets up to the block with the given
%% hash H (including) sorted from latest to earliest.
get_list_by_hash(H) ->
	get_list_by_hash([], ets:first(block_index), -1, H).

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
get_block_bounds(Offset) ->
	{WeaveSize, Height, _H, TXRoot} = Key = ets:next(block_index, {Offset, n, n, n}),
	case Height of
		0 ->
			{0, WeaveSize, TXRoot};
		_ ->
			{PrevWeaveSize, _, _, _} = ets:prev(block_index, Key),
			{PrevWeaveSize, WeaveSize, TXRoot}
	end.

%% @doc Return {Height, {H, WeaveSize, TXRoot}} with the triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection(Height, _BI) when Height < 0 ->
	no_intersection;
get_intersection(_Height, []) ->
	no_intersection;
get_intersection(Height, BI) ->
	ReverseBI = lists:reverse(BI),
	[{H, _, _} = Elem | ReverseBI2] = ReverseBI,
	case catch ets:slot(block_index, Height) of
		[{{_, Height, H, _} = Entry}] ->
			get_intersection(Height + 1, Elem, ReverseBI2, ets:next(block_index, Entry));
		_ ->
			no_intersection
	end.

%% @doc Return the {H, WeaveSize, TXRoot} triplet present in both
%% the cached block index and the given BI or no_intersection.
get_intersection([]) ->
	no_intersection;
get_intersection(BI) ->
	{H, WeaveSize, _TXRoot} = lists:last(BI),
	get_intersection2({H, WeaveSize}, tl(lists:reverse(BI)),
			ets:next(block_index, {WeaveSize - 1, n, n, n})).

%% @doc Return the list of {H, WeaveSize, TXRoot} for blocks with Height >= Start, =< End,
%% sorted from the largest height to the smallest.
get_range(Start, End) when Start > End ->
	[];
get_range(Start, End) ->
	case catch ets:slot(block_index, Start) of
		[{{WeaveSize, _Height, H, TXRoot} = Entry}] ->
			lists:reverse([{H, WeaveSize, TXRoot}
				| get_range2(Start + 1, End, ets:next(block_index, Entry))]);
		_ ->
			{error, invalid_start}
	end.

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
get_list(BI, {WeaveSize, NextHeight, H, TXRoot} = Key, Height, MaxHeight)
		when NextHeight == Height + 1 ->
	get_list([{H, WeaveSize, TXRoot} | BI], ets:next(block_index, Key), Height + 1, MaxHeight);
get_list(_BI, _Key, _Height, MaxHeight) ->
	%% An extremely unlikely race condition should have occured where some blocks were
	%% orphaned right after we passed some of them here, and new blocks have been added
	%% right before we reached the end of the table.
	get_list(MaxHeight).

get_list_by_hash(BI, '$end_of_table', _Height, _H) ->
	BI;
get_list_by_hash(BI, {WeaveSize, NextHeight, H, TXRoot}, Height, H)
		when NextHeight == Height + 1 ->
	[{H, WeaveSize, TXRoot} | BI];
get_list_by_hash(BI, {WeaveSize, NextHeight, H, TXRoot} = Key, Height, H2)
		when NextHeight == Height + 1 ->
	get_list_by_hash([{H, WeaveSize, TXRoot} | BI], ets:next(block_index, Key), Height + 1,
			H2);
get_list_by_hash(_BI, _Key, _Height, H) ->
	%% An extremely unlikely race condition should have occured where some blocks were
	%% orphaned right after we passed some of them here, and new blocks have been added
	%% right before we reached the end of the table.
	get_list_by_hash(H).

get_intersection(Height, Entry, _ReverseBI, '$end_of_table') ->
	{Height - 1, Entry};
get_intersection(Height, Entry, [], _Entry) ->
	{Height - 1, Entry};
get_intersection(Height, _Entry, [{H, _, _} = Elem | ReverseBI], {_, Height, H, _} = Entry) ->
	get_intersection(Height + 1, Elem, ReverseBI, ets:next(block_index, Entry));
get_intersection(Height, Entry, _ReverseBI, _TableEntry) ->
	{Height - 1, Entry}.

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

get_range2(Start, End, _Elem) when Start > End ->
	[];
get_range2(_Start, _End, '$end_of_table') ->
	[];
get_range2(Start, End, {WeaveSize, _Height, H, TXRoot} = Elem) ->
	[{H, WeaveSize, TXRoot} | get_range2(Start + 1, End, ets:next(block_index, Elem))].
