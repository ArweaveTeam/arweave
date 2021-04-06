%%% @doc The utilities for managing sets of non-overlapping intervals stored in an ETS table.
%%% The API is similar to the one of the ar_intervals module. Keeping the intervals in ETS
%%% is a convenient way to share them between processes, e.g. the mining module can quickly
%%% check whether the given recall byte is synced. ar_intervals, in turn, is helpful
%%% for manipulating multiple sets of intervals, e.g. the syncing process uses it to look for
%%% the intersections between our data and peers' data.
%%% @end
-module(ar_ets_intervals).

-export([
	init_from_gb_set/2,
	add/3,
	delete/3,
	cut/2,
	is_inside/2,
	get_interval_with_byte/2,
	get_next_interval_outside/3
]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Record intervals from the given gb_sets set.
init_from_gb_set(Table, Set) ->
	init_from_gb_set_iterator(Table, gb_sets:iterator(Set)).

%% @doc Record an interval, bytes Start + 1, Start + 2 ... End.
add(Table, End, Start) when End > Start ->
	{End2, Start2, InnerEnds} = find_largest_continuous_interval(Table, End, Start),
	ets:insert(Table, [{End2, Start2}]),
	remove_inner_intervals(Table, InnerEnds, End2).

%% @doc Remove the given interval, bytes Start + 1, Start + 2 ... End.
delete(Table, End, Start) when End > Start ->
	case ets:next(Table, Start) of
		'$end_of_table' ->
			ok;
		End2 ->
			case ets:lookup(Table, End2) of
				[] ->
					%% The key has just been removed, very unlucky timing.
					delete(Table, End, Start);
				[{_End2, Start2}] when Start2 >= End ->
					ok;
				[{End2, Start2}] ->
					Insert =
						case Start2 < Start of
							true ->
								[{Start, Start2}];
							false ->
								[]
						end,
					Insert2 =
						case End2 > End of
							true ->
								[{End2, End} | Insert];
							false ->
								Insert
						end,
					ets:insert(Table, Insert2),
					case End2 > End of
						true ->
							%% We have already inserted {End2, End} above.
							ok;
						false ->
							ets:delete(Table, End2),
							case End2 < End of
								true ->
									delete(Table, End, End2);
								false ->
									ok
							end
					end
			end
	end.

%% @doc Cut the set by removing all the intervals and interval's parts above Offset.
cut(Table, Offset) ->
	case ets:next(Table, Offset) of
		'$end_of_table' ->
			ok;
		End ->
			case ets:lookup(Table, End) of
				[] ->
					%% The key has just been removed, very unlucky timing.
					cut(Table, Offset);
				[{End, Start}] when Start < Offset ->
					ets:insert(Table, [{Offset, Start}]),
					ets:delete(Table, End),
					cut(Table, Offset);
				[{End, _Start}] ->
					ets:delete(Table, End),
					cut(Table, Offset)
			end
	end.

%% @doc Return true if the given offset is inside one of the intervals, including
%% the right bound, excluding the left bound.
%%
%% E.g. a table with byte 1:
%%   add(table, 1, 0),
%%   is_inside(table, 1) == true,
%%   is_inside(table, 0) == false
%% for a table with bytes 3, 4, and 5:
%%   add(table, 5, 2),
%%   is_inside(table, 5) == true,
%%   is_inside(table, 4) == true,
%%   is_inside(table, 3) == true,
%%   is_inside(table, 2) == false.
%% @end
is_inside(Table, Offset) ->
	case ets:next(Table, Offset - 1) of
		'$end_of_table' ->
			false;
		NextOffset ->
			case ets:lookup(Table, NextOffset) of
				[{NextOffset, Start}] ->
					Offset > Start;
				[] ->
					%% The key should have been just removed, unlucky timing.
					is_inside(Table, Offset)
			end
	end.

%% @doc Return the interval containing the given offset, including the right bound,
%% excluding the left bound, or not_found.
%% @end
get_interval_with_byte(Table, Offset) ->
	case ets:next(Table, Offset - 1) of
		'$end_of_table' ->
			not_found;
		NextOffset ->
			case ets:lookup(Table, NextOffset) of
				[{NextOffset, Start}] ->
					case Offset > Start of
						true ->
							{NextOffset, Start};
						false ->
							not_found
					end;
				[] ->
					%% The key should have been just removed, unlucky timing.
					get_interval_with_byte(Table, Offset)
			end
	end.

%% @doc Return the lowest interval outside the recorded set of intervals,
%% strictly above the given Offset, and with the right bound at most RightBound.
%% Return not_found if there are no such intervals.
get_next_interval_outside(_Table, Offset, RightBound) when Offset >= RightBound ->
	not_found;
get_next_interval_outside(Table, Offset, RightBound) ->
	case ets:next(Table, Offset) of
		'$end_of_table' ->
			{RightBound, Offset};
		NextOffset ->
			case ets:lookup(Table, NextOffset) of
				[{NextOffset, Start}] when Start > Offset ->
					{Start, Offset};
				_ ->
					get_next_interval_outside(Table, NextOffset, RightBound)
			end
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

init_from_gb_set_iterator(Table, Iterator) ->
	case gb_sets:next(Iterator) of
		none ->
			ok;
		{{End, Start}, Iterator2} ->
			add(Table, End, Start),
			init_from_gb_set_iterator(Table, Iterator2)
	end.

find_largest_continuous_interval(Table, End, Start) ->
	find_largest_continuous_interval(Table, End, Start, End, Start, []).

find_largest_continuous_interval(Table, End, Start, End2, Start2, InnerEnds) ->
	case ets:next(Table, Start - 1) of
		'$end_of_table' ->
			{End2, Start2, InnerEnds};
		End3 ->
			case ets:lookup(Table, End3) of
				[] ->
					%% The key has just been removed, very unlucky timing.
					find_largest_continuous_interval(Table, End, Start, End2, Start2, InnerEnds);
				[{_End3, Start3}] when Start3 > End ->
					{End2, Start2, InnerEnds};
				[{End3, Start3}] ->
					find_largest_continuous_interval(
						Table,
						End,
						End3 + 1,
						max(End2, End3),
						min(Start2, Start3),
						[End3 | InnerEnds]
					)
			end
	end.

remove_inner_intervals(_Table, [], _End) ->
	ok;
remove_inner_intervals(Table, [End | InnerEnds], End) ->
	remove_inner_intervals(Table, InnerEnds, End);
remove_inner_intervals(Table, [InnerEnd | InnerEnds], End) ->
	ets:delete(Table, InnerEnd),
	remove_inner_intervals(Table, InnerEnds, End).

%%%===================================================================
%%% Tests.
%%%===================================================================

ets_intervals_test() ->
	ets:new(ets_intervals_test, [named_table, ordered_set]),
	assert_is_not_inside(100, 0),
	?assertEqual(ok, cut(ets_intervals_test, 10)),
	?assertEqual(ok, delete(ets_intervals_test, 10, 5)),
	Set = gb_sets:from_list([{1, 0}, {5, 3}, {16, 10}]),
	init_from_gb_set(ets_intervals_test, Set),
	assert_is_inside(1, 0),
	assert_is_not_inside(3, 1),
	assert_is_inside(5, 3),
	assert_is_not_inside(10, 5),
	assert_is_inside(16, 10),
	assert_is_not_inside(20, 16),
	%% 1,0 16,3
	add(ets_intervals_test, 11, 4),
	assert_is_inside(16, 3),
	assert_is_inside(1, 0),
	assert_is_not_inside(3, 1),
	assert_is_not_inside(20, 16),
	%% back to 1,0 5,3 16,10
	delete(ets_intervals_test, 10, 5),
	assert_is_inside(5, 3),
	assert_is_inside(1, 0),
	assert_is_inside(16, 10),
	assert_is_not_inside(3, 1),
	assert_is_not_inside(10, 5),
	assert_is_not_inside(20, 16),
	%% 1,0 5,3 16,10 20,18
	add(ets_intervals_test, 20, 18),
	assert_is_inside(5, 3),
	assert_is_inside(1, 0),
	assert_is_inside(16, 10),
	assert_is_inside(20, 18),
	assert_is_not_inside(3, 1),
	assert_is_not_inside(10, 5),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 20),
	%% 1,0 5,3 8,7 16,10 20,18
	add(ets_intervals_test, 8, 7),
	assert_is_inside(5, 3),
	assert_is_inside(1, 0),
	assert_is_inside(16, 10),
	assert_is_inside(20, 18),
	assert_is_inside(8, 7),
	assert_is_not_inside(3, 1),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 20),
	%% 5,0 8,7 16,10 20,18
	add(ets_intervals_test, 3, 1),
	assert_is_inside(5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(16, 10),
	assert_is_inside(20, 18),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 20),
	%% 5,0 8,7 16,10 20,18
	cut(ets_intervals_test, 22),
	assert_is_inside(5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(16, 10),
	assert_is_inside(20, 18),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 20),
	%% 5,0 8,7 16,10 20,18
	cut(ets_intervals_test, 20),
	assert_is_inside(5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(16, 10),
	assert_is_inside(20, 18),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 20),
	%% 5,0 8,7 16,10 19,18
	cut(ets_intervals_test, 19),
	assert_is_inside(5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(16, 10),
	assert_is_inside(19, 18),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(18, 16),
	assert_is_not_inside(22, 19),
	%% 5,0 8,7 14,10
	cut(ets_intervals_test, 14),
	assert_is_inside(5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(14, 10),
	assert_is_not_inside(7, 5),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(20, 14),
	%% 1,0 8,7 14,10
	delete(ets_intervals_test, 5, 1),
	assert_is_inside(1, 0),
	assert_is_inside(8, 7),
	assert_is_inside(14, 10),
	assert_is_not_inside(7, 1),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(20, 14),
	%% 8,7 14,10
	delete(ets_intervals_test, 5, 0),
	assert_is_inside(8, 7),
	assert_is_inside(14, 10),
	assert_is_not_inside(7, 0),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(20, 14),
	%% 8,7 15,10 30,20
	add(ets_intervals_test, 15, 14),
	add(ets_intervals_test, 30, 20),
	assert_is_inside(8, 7),
	assert_is_inside(15, 10),
	assert_is_inside(30, 20),
	assert_is_not_inside(7, 0),
	assert_is_not_inside(10, 8),
	assert_is_not_inside(20, 15),
	assert_is_not_inside(40, 30),
	%% 8,7 30,25
	delete(ets_intervals_test, 25, 8),
	assert_is_inside(8, 7),
	assert_is_inside(30, 25),
	assert_is_not_inside(25, 8),
	assert_is_not_inside(7, 0),
	assert_is_not_inside(40, 30),
	%% 30,7
	add(ets_intervals_test, 25, 8),
	assert_is_inside(30, 7),
	assert_is_not_inside(7, 0),
	assert_is_not_inside(40, 30),
	%% 12,7 18,16 30,25
	delete(ets_intervals_test, 16, 12),
	delete(ets_intervals_test, 25, 18),
	assert_is_inside(12, 7),
	assert_is_inside(18, 16),
	assert_is_inside(30, 25),
	assert_is_not_inside(16, 12),
	assert_is_not_inside(25, 18),
	assert_is_not_inside(40, 30),
	%% 12,7 21,16 30,25
	add(ets_intervals_test, 21, 18),
	assert_is_inside(12, 7),
	assert_is_inside(21, 16),
	assert_is_inside(30, 25),
	assert_is_not_inside(16, 12),
	assert_is_not_inside(25, 21),
	assert_is_not_inside(40, 30),
	%% 12,7 33,13
	add(ets_intervals_test, 33, 13),
	assert_is_inside(33, 13),
	assert_is_inside(12, 7),
	assert_is_not_inside(13, 12),
	assert_is_not_inside(40, 33),
	%% 12,7 34,13
	add(ets_intervals_test, 34, 13),
	assert_is_inside(34, 13),
	assert_is_inside(12, 7),
	assert_is_not_inside(13, 12),
	assert_is_not_inside(40, 34),
	%% 12,7 35,13
	add(ets_intervals_test, 35, 22),
	assert_is_inside(35, 13),
	assert_is_inside(12, 7),
	assert_is_not_inside(13, 12),
	assert_is_not_inside(40, 35).

assert_is_inside(End, End) ->
	ok;
assert_is_inside(End, Start) ->
	?assertEqual(true, is_inside(ets_intervals_test, Start + 1)),
	assert_is_inside(End, Start + 1).

assert_is_not_inside(End, End) ->
	ok;
assert_is_not_inside(End, Start) ->
	?assertEqual(false, is_inside(ets_intervals_test, Start + 1)),
	assert_is_not_inside(End, Start + 1).
