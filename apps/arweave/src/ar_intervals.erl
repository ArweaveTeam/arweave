%%% @doc A set of non-overlapping intervals.
-module(ar_intervals).

-export([
	new/0,
	add/3,
	delete/3,
	cut/2,
	is_inside/2,
	sum/1,
	union/2,
	serialize/2,
	safe_from_etf/1,
	count/1,
	is_empty/1,
	take_smallest/1,
	take_largest/1,
	largest/1,
	iterator_from/2,
	next/1
]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Create an empty set of intervals.
new() ->
	gb_sets:new().

%% @doc Add a new interval. Intervals are compacted - e.g., (2, 1) and (1, 0) are joined
%% into (2, 0). Also, if two intervals intersect each other, they are joined.
%% @end
add(Intervals, End, Start) when End > Start ->
	Iter = gb_sets:iterator_from({Start - 1, Start - 1}, Intervals),
	add2(Iter, Intervals, End, Start).

%% @doc Remove the given interval from the set.
delete(Intervals, End, Start) ->
	Iter = gb_sets:iterator_from({Start - 1, Start - 1}, Intervals),
	delete2(Iter, Intervals, End, Start).

%% @doc Remove the interval above the given cut. If there is an interval containing
%% the cut, replace it with its part up to the cut.
%% @end
cut(Intervals, Cut) ->
	case gb_sets:size(Intervals) of
		0 ->
			Intervals;
		_ ->
			case gb_sets:take_largest(Intervals) of
				{{_, Start}, UpdatedIntervals} when Start >= Cut ->
					cut(UpdatedIntervals, Cut);
				{{End, Start}, UpdatedIntervals} when End > Cut ->
					gb_sets:add_element({Cut, Start}, UpdatedIntervals);
				_ ->
					Intervals
			end
	end.

%% @doc Return true if the given number is inside one of the intervals, false otherwise.
%% The left bounds of the intervals are excluded from search, the right bounds are included.
%% @end
is_inside(Intervals, Number) ->
	Iter = gb_sets:iterator_from({Number - 1, Number - 1}, Intervals),
	case gb_sets:next(Iter) of
		none ->
			false;
		{{Number, _Start}, _Iter} ->
			true;
		{{_End, Start}, _Iter} when Number > Start ->
			true;
		_ ->
			false
	end.

%% @doc Return the sum of the lenghts of the intervals.
sum(Intervals) ->
	gb_sets:fold(fun({End, Start}, Acc) -> Acc + End - Start end, 0, Intervals).

%% @doc Return the set of intervals consisting of the points of intervals from both sets.
union(I1, I2) ->
	{Longer, Shorter} =
		case gb_sets:size(I1) > gb_sets:size(I2) of
			true ->
				{I1, I2};
			false ->
				{I2, I1}
		end,
	gb_sets:fold(
		fun({End, Start}, Acc) ->
			add(Acc, End, Start)
		end,
		Longer,
		Shorter
	).

%% @doc Serialize a subset of the intervals using the requested format, etf | json.
%% The subset is always smaller than or equal to Limit. If random_subset key is present,
%% the chosen subset is random. Otherwise, the right bound of the first interval is
%% greater than or equal to start.
%% @end
serialize(#{ random_subset := _, limit := Limit, format := Format }, Intervals) ->
	serialize_random_subset(Intervals, Limit, Format);
serialize(#{ start := Start, limit := Limit, format := Format }, Intervals) ->
	serialize_subset(Intervals, Start, Limit, Format).

%% @doc Convert the binary produced by to_etf/2 into the set of intervals.
%% Return {error, invalid} if the binary is not a valid ETF representation of the
%% non-overlapping intervals.
%% @end
safe_from_etf(Binary) ->
	case catch from_etf(Binary) of
		{ok, Intervals} ->
			{ok, Intervals};
		_ ->
			{error, invalid}
	end.

%% @doc Return the number of intervals in the set.
count(Intervals) ->
	gb_sets:size(Intervals).

%% @doc Return true if the set of intervals is empty, false otherwise.
is_empty(Intervals) ->
	gb_sets:is_empty(Intervals).

%% @doc Return {Interval, Intervals2} when Interval is the interval with the smallest
%% right bound and Intervals2 is the set of intervals with this interval removed.
%% @end
take_smallest(Intervals) ->
	gb_sets:take_smallest(Intervals).

%% @doc Return {Interval, Intervals2} when Interval is the interval with the largest
%% right bound and Intervals2 is the set of intervals with this interval removed.
%% @end
take_largest(Intervals) ->
	gb_sets:take_largest(Intervals).

%% @doc A proxy for gb_sets:largest/1.
largest(Intervals) ->
	gb_sets:largest(Intervals).

%% @doc A proxy for gb_sets:iterator_from/2.
iterator_from(Interval, Intervals) ->
	gb_sets:iterator_from(Interval, Intervals).

%% @doc A proxy for gb_sets:next/1.
next(Iterator) ->
	gb_sets:next(Iterator).

%%%===================================================================
%%% Private functions.
%%%===================================================================

add2(Iter, Intervals, End, Start) ->
	case gb_sets:next(Iter) of
		none ->
			gb_sets:add_element({End, Start}, Intervals);
		{{End2, Start2}, Iter2} when End >= Start2 andalso Start =< End2 ->
			End3 = max(End, End2),
			Start3 = min(Start, Start2),
			add2(Iter2, gb_sets:del_element({End2, Start2}, Intervals), End3, Start3);
		_ ->
			gb_sets:add_element({End, Start}, Intervals)
	end.

delete2(Iter, Intervals, End, Start) ->
	case gb_sets:next(Iter) of
		none ->
			Intervals;
		{{End2, Start2}, Iter2} when End >= Start2 andalso Start =< End2 ->
			Intervals2 = gb_sets:del_element({End2, Start2}, Intervals),
			Intervals3 =
				case End2 > End of
					true ->
						gb_sets:insert({End2, End}, Intervals2);
					false ->
						Intervals2
				end,
			Intervals4 =
				case Start > Start2 of
					true ->
						gb_sets:insert({Start, Start2}, Intervals3);
					false ->
						Intervals3
				end,
			delete2(Iter2, Intervals4, End, Start);
		_ ->
			Intervals
	end.

serialize_random_subset(Intervals, Limit, Format) ->
	case gb_sets:is_empty(Intervals) of
		true ->
			serialize_empty(Format);
		false ->
			Iterator = gb_sets:iterator(Intervals),
			InclusionProbability = min(Limit / gb_sets:size(Intervals), 1),
			serialize_random_subset(Iterator, InclusionProbability, [], 0, Limit, Format)
	end.

serialize_empty(etf) ->
	term_to_binary([]);
serialize_empty(json) ->
	jiffy:encode([]).

serialize_random_subset(_Iterator, _Probability, L, Count, Limit, Format) when Count == Limit ->
	serialize_list(L, Format);
serialize_random_subset(Iterator, Probability, L, Count, Limit, Format) ->
	case gb_sets:next(Iterator) of
		none ->
			serialize_list(L, Format);
		{{End, Start}, Iterator2} ->
			PickItem =
				case Probability < 1 of
					true ->
						rand:uniform() < Probability;
					false ->
						true
				end,
			case PickItem of
				false ->
					serialize_random_subset(Iterator2, Probability, L, Count, Limit, Format);
				true ->
					L2 = [serialize_item(End, Start, Format) | L],
					serialize_random_subset(Iterator2, Probability, L2, Count + 1, Limit, Format)
			end
	end.

serialize_list(L, etf) ->
	term_to_binary(L);
serialize_list(L, json) ->
	jiffy:encode(L).

serialize_item(End, Start, etf) ->
	{<< End:256 >>, << Start:256 >>};
serialize_item(End, Start, json) ->
	#{ integer_to_binary(End) => integer_to_binary(Start) }.

serialize_subset(Intervals, Start, Limit, Format) ->
	case gb_sets:is_empty(Intervals) of
		true ->
			serialize_empty(Format);
		false ->
			Iterator = gb_sets:iterator_from({Start, 0}, Intervals),
			serialize_subset(Iterator, [], 0, Limit, Format)
	end.

serialize_subset(_Iterator, L, Count, Limit, Format) when Count == Limit ->
	serialize_list(L, Format);
serialize_subset(Iterator, L, Count, Limit, Format) ->
	case gb_sets:next(Iterator) of
		none ->
			serialize_list(L, Format);
		{{End, Start}, Iterator2} ->
			L2 = [serialize_item(End, Start, Format) | L],
			serialize_subset(Iterator2, L2, Count + 1, Limit, Format)
	end.

from_etf(Binary) ->
	L = binary_to_term(Binary, [safe]),
	from_etf(L, infinity, new()).

from_etf([], _, Intervals) ->
	{ok, Intervals};
from_etf([{<< End:256 >>, << Start:256 >>} | List], R, Intervals)
		when End > Start andalso R > End andalso Start >= 0 ->
	from_etf(List, Start, gb_sets:add_element({End, Start}, Intervals)).

%%%===================================================================
%%% Tests.
%%%===================================================================

intervals_test() ->
	I = new(),
	?assertEqual(0, count(I)),
	?assertEqual(0, sum(I)),
	?assert(not is_inside(I, 0)),
	?assert(not is_inside(I, 1)),
	?assertEqual(<<"[]">>, serialize(#{ random_subset => true, format => json, limit => 1 }, I)),
	?assertEqual(<<"[]">>, serialize(#{ start => 0, format => json, limit => 1 }, I)),
	?assertEqual(<<"[]">>, serialize(#{ start => 1, format => json, limit => 1 }, I)),
	?assertEqual(
		{ok, new()},
		safe_from_etf(serialize(#{ random_subset => true, format => etf, limit => 1 }, I))
	),
	?assertEqual(new(), delete(I, 2, 1)),
	I2 = add(I, 2, 1),
	?assertEqual(1, count(I2)),
	?assertEqual(1, sum(I2)),
	?assert(not is_inside(I2, 0)),
	?assert(not is_inside(I2, 1)),
	?assert(is_inside(I2, 2)),
	?assert(not is_inside(I2, 3)),
	?assertEqual(new(), delete(I2, 2, 1)),
	?assertEqual(new(), delete(I2, 2, 0)),
	?assertEqual(new(), delete(I2, 3, 1)),
	?assertEqual(new(), delete(I2, 3, 0)),
	?assertEqual(new(), cut(I2, 1)),
	?assertEqual(new(), cut(I2, 0)),
	compare(I2, cut(I2, 2)),
	compare(I2, cut(I2, 3)),
	?assertEqual(
		<<"[{\"2\":\"1\"}]">>,
		serialize(#{ random_subset => true, limit => 1, format => json }, I2)
	),
	?assertEqual(
		<<"[]">>,
		serialize(#{ random_subset => true, limit => 0, format => json }, I2)
	),
	?assertEqual(
		<<"[{\"2\":\"1\"}]">>,
		serialize(#{ start => 2, limit => 1, format => json }, I2)
	),
	?assertEqual(
		<<"[]">>,
		serialize(#{ start => 3, limit => 1, format => json }, I2)
	),
	?assertEqual(
		<<"[]">>,
		serialize(#{ start => 2, limit => 0, format => json }, I2)
	),
	{ok, I2_FromETF} =
		safe_from_etf(serialize(#{ format => etf, limit => 1, random_subset => true }, I2)),
	compare(I2, I2_FromETF),
	?assertEqual(
		{ok, new()},
		safe_from_etf(serialize(#{ format => etf, limit => 0, random_subset => true }, I2))
	),
	compare(I2, add(I2, 2, 1)),
	compare(add(new(), 3, 1), add(I2, 3, 1)),
	compare(add(new(), 2, 0), add(I2, 2, 0)),
	I3 = add(I2, 6, 3),
	?assertEqual(2, count(I3)),
	?assertEqual(4, sum(I3)),
	?assert(not is_inside(I3, 0)),
	?assert(not is_inside(I3, 1)),
	?assert(is_inside(I3, 2)),
	?assert(not is_inside(I3, 3)),
	?assert(is_inside(I3, 4)),
	?assert(is_inside(I3, 5)),
	?assert(is_inside(I3, 6)),
	compare(add(add(add(new(), 2, 1), 6, 5), 4, 3), delete(I3, 5, 4)),
	compare(add(new(), 6, 5), delete(I3, 5, 1)),
	compare(add(new(), 10, 0), add(I3, 10, 0)),
	?assertEqual(new(), cut(I3, 1)),
	?assertEqual(new(), cut(I3, 0)),
	?assertEqual(I2, cut(I3, 2)),
	?assertEqual(I2, cut(I3, 3)),
	compare(add(I2, 4, 3), cut(I3, 4)),
	compare(add(I2, 5, 3), cut(I3, 5)),
	compare(I3, cut(I3, 6)),
	?assertEqual(
		<<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
		serialize(#{ random_subset => true, limit => 10, format => json }, I3)
	),
	?assertEqual(
		<<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
		serialize(#{ start => 1, limit => 10, format => json }, I3)
	),
	?assertEqual(
		<<"[{\"2\":\"1\"}]">>,
		serialize(#{ start => 1, limit => 1, format => json }, I3)
	),
	?assertEqual(
		<<"[{\"6\":\"3\"}]">>,
		serialize(#{ start => 3, limit => 10, format => json }, I3)
	),
	{ok, I3_FromETF} =
		safe_from_etf(serialize(#{ format => etf, limit => 10, random_subset => true }, I3)),
	compare(I3, I3_FromETF),
	compare(I3, add(I3, 4, 3)),
	compare(add(new(), 6, 1), add(I3, 3, 1)),
	I4 = add(I3, 7, 6),
	?assertEqual(2, count(I4)),
	?assertEqual(5, sum(I4)),
	?assert(not is_inside(I4, 0)),
	?assert(not is_inside(I4, 1)),
	?assert(is_inside(I4, 2)),
	?assert(not is_inside(I4, 3)),
	?assert(is_inside(I4, 4)),
	?assert(is_inside(I4, 5)),
	?assert(is_inside(I4, 6)),
	?assert(is_inside(I4, 7)),
	?assert(not is_inside(I4, 8)),
	?assertEqual(new(), cut(I4, 1)),
	?assertEqual(new(), cut(I4, 0)),
	compare(add(I2, 5, 3), cut(I4, 5)),
	compare(I4, cut(I4, 7)),
	?assertEqual(
		<<"[{\"7\":\"3\"},{\"2\":\"1\"}]">>,
		serialize(#{ format => json, limit => 10, random_subset => true }, I4)
	),
	{ok, I4_FromETF} =
		safe_from_etf(serialize(#{ limit => count(I4), random_subset => true, format => etf }, I4)),
	compare(I4, I4_FromETF),
	I5 = add(I4, 3, 2),
	?assertEqual(1, count(I5)),
	?assertEqual(6, sum(I5)),
	compare(I5, add(I5, 3, 2)),
	compare(I5, add(I5, 2, 1)),
	compare(add(add(new(), 3, 2), 8, 7), delete(add(add(new(), 4, 2), 8, 6), 7, 3)).

compare(I1, I2) ->
	?assertEqual(
		serialize(#{ format => json, limit => count(I1), start => 0 }, I1),
		serialize(#{ format => json, limit => count(I2), start => 0 }, I2)
	),
	Folded1 = gb_sets:fold(fun({K, V}, Acc) -> [{K, V} | Acc] end, [], I1),
	Folded2 = gb_sets:fold(fun({K, V}, Acc) -> [{K, V} | Acc] end, [], I2),
	?assertEqual(Folded1, Folded2).
