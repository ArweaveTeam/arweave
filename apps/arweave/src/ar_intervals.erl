-module(ar_intervals).

-export([
	new/0,
	add/3,
	cut/2,
	is_inside/2,
	sum/1,
	outerjoin/2,
	get_interval_by_nth_inner_number/2,
	to_etf/2,
	to_json/2,
	safe_from_etf/1
]).

new() ->
	gb_sets:new().

add(Intervals, End, Start) ->
	compact(gb_sets:add_element({End, Start}, Intervals)).

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

is_inside(Intervals, Number) ->
	is_inside2(gb_sets:iterator(Intervals), Number).

sum(Intervals) ->
	gb_sets:fold(fun({End, Start}, Acc) -> Acc + End - Start end, 0, Intervals).

outerjoin(I1, I2) ->
	intersection(inverse(I1), I2).

get_interval_by_nth_inner_number(Intervals, N) ->
	get_interval_by_nth_inner_number(gb_sets:iterator(Intervals), N, 0, infinity).

to_etf(Intervals, Limit) ->
	serialize(Intervals, Limit, etf).

to_json(Intervals, Limit) ->
	serialize(Intervals, Limit, json).

safe_from_etf(Binary) ->
	case catch from_etf(Binary) of
		{ok, Intervals} ->
			{ok, Intervals};
		_ ->
			{error, invalid}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

compact(Intervals) ->
	Iterator = gb_sets:iterator(Intervals),
	case gb_sets:next(Iterator) of
		none ->
			Intervals;
		{Item, UpdatedIterator} ->
			compact(UpdatedIterator, Item, [])
	end.

compact(Iterator, {End, Start}, List) ->
	case gb_sets:next(Iterator) of
		none ->
			gb_sets:from_list([{End, Start} | List]);
		{{End2, End}, UpdatedIterator} ->
			compact(UpdatedIterator, {End2, Start}, List);
		{Item, UpdatedIterator} ->
			compact(UpdatedIterator, Item, [{End, Start} | List])
	end.

is_inside2(Iterator, Number) ->
	case gb_sets:next(Iterator) of
		none ->
			false;
		{{_, Start}, _} when Number =< Start ->
			false;
		{{End, _}, _} when Number =< End ->
			true;
		{_, UpdatedIterator} ->
			is_inside2(UpdatedIterator, Number)
	end.

inverse(Intervals) ->
	inverse(gb_sets:iterator(Intervals), 0, gb_sets:new()).

inverse(Iterator, L, G) ->
	case gb_sets:next(Iterator) of
		none ->
			gb_sets:add_element({infinity, L}, G);
		{{End1, Start1}, I1} ->
			G2 = case Start1 > L of true -> gb_sets:add_element({Start1, L}, G); _ -> G end,
			L2 = End1,
			case gb_sets:next(I1) of
				none ->
					gb_sets:add_element({infinity, L2}, G2);	
				{{End2, Start2}, I2} ->
					inverse(I2, End2, gb_sets:add_element({Start2, End1}, G2))
			end
	end.

intersection(I1, I2) ->
	intersection(gb_sets:iterator(I1), gb_sets:iterator(I2), gb_sets:new()).

intersection(I1, I2, G) ->
	case {gb_sets:next(I1), gb_sets:next(I2)} of
		{none, _} ->
			G;
		{_, none} ->
			G;
		{{{End1, _Start1}, UpdatedI1}, {{_End2, Start2}, _UpdatedI2}} when Start2 >= End1 ->
			intersection(UpdatedI1, I2, G);
		{{{_End1, Start1}, _UpdatedI1}, {{End2, _Start2}, UpdatedI2}} when Start1 >= End2 ->
			intersection(I1, UpdatedI2, G);
		{{{End1, Start1}, UpdatedI1}, {{End2, Start2}, _UpdatedI2}} when End2 >= End1 ->
			intersection(UpdatedI1, I2, gb_sets:add_element({End1, max(Start1, Start2)}, G));
		{{{End1, Start1}, _UpdatedI1}, {{End2, Start2}, UpdatedI2}} when End1 > End2 ->
			intersection(I1, UpdatedI2, gb_sets:add_element({End2, max(Start1, Start2)}, G))
	end.

get_interval_by_nth_inner_number(Iterator, N, L, R) ->
	case gb_sets:next(Iterator) of
		none ->
			{L, N, R};
		{{End, Start}, UpdatedIterator} when N >= End - Start ->
			get_interval_by_nth_inner_number(UpdatedIterator, N - End + Start, End, R);
		{{End, Start}, _UpdatedIterator} ->
			{Start, Start + N, End}
	end.

serialize(Intervals, Limit, Format) ->
	case gb_sets:is_empty(Intervals) of
		true ->
			term_to_binary([]);
		false ->
			Iterator = gb_sets:iterator(Intervals),
			serialize(Iterator, min(Limit / gb_sets:size(Intervals), 1), [], 0, Limit, Format)
	end.

serialize(_Iterator, _Probability, L, Count, Limit, Format) when Count == Limit ->
	serialize_list(L, Format);
serialize(Iterator, Probability, L, Count, Limit, Format) ->
	case gb_sets:next(Iterator) of
		none ->
			serialize_list(L, Format);
		{{End, Start}, UpdatedIterator} ->
			PickItem = case Probability < 1 of
				true ->
					rand:uniform() < Probability;
				false ->
					true
			end,
			case PickItem of
				false ->
					serialize(UpdatedIterator, Probability, L, Count, Limit, Format);
				true ->
					UpdatedL = [serialize_item(End, Start, Format) | L],
					serialize(UpdatedIterator, Probability, UpdatedL, Count + 1, Limit, Format)
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

from_etf(Binary) ->
	L = binary_to_term(Binary, [safe]),
	from_etf(L, infinity, gb_sets:new()).

from_etf([], _, Intervals) ->
	{ok, Intervals};
from_etf([{<< End:256 >>, << Start:256 >>} | List], R, Intervals)
		when End > Start andalso R > End andalso Start >= 0 ->
	from_etf(List, Start, gb_sets:add_element({End, Start}, Intervals)).
