-module(ar_bench_timer).

-export([initialize/0, reset/0, record/3, start/1, stop/1, get_timing_data/0, print_timing_data/0, get_total/1, get_max/1, get_min/1, get_avg/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

record(Key, Fun, Args) ->
    {Time, Result} = timer:tc(Fun, Args),
    update_total(Key, Time),
    Result.

start(Key) ->
	StartTime = erlang:timestamp(),
	ets:insert(start_time, {real_key(Key), StartTime}).

stop(Key) ->
	case ets:lookup(start_time, real_key(Key)) of
		[{_, StartTime}] ->
			EndTime = erlang:timestamp(),
			ElapsedTime = timer:now_diff(EndTime, StartTime),
			% io:format("Elapsed ~p: ~p -> ~p = ~p~n", [Key, StartTime, EndTime, ElapsedTime]),
			update_total(Key, ElapsedTime),
			ElapsedTime;
		[] ->
			% Key not found, throw an error
			{error, {not_started, Key}}
	end.

update_total(Key, ElapsedTime) ->
	ets:update_counter(total_time, real_key(Key), {2, ElapsedTime}, {real_key(Key), 0}).

get_total([]) ->
	0;
get_total(Times) when is_list(Times) ->
	lists:sum(Times);
get_total(Key) ->
    get_total(get_times(Key)).

get_max([]) ->
	0;
get_max(Times) when is_list(Times) ->
    lists:max(Times);
get_max(Key) ->
	get_max(get_times(Key)).

get_min([]) ->
	0;
get_min(Times) when is_list(Times) ->
    lists:min(Times);
get_min(Key) ->
    get_min(get_times(Key)).

get_avg([]) ->
	0;
get_avg(Times) when is_list(Times) ->
	TotalTime = lists:sum(Times),
    case length(Times) of
        0 -> 0;
        N -> TotalTime / N
    end;
get_avg(Key) ->
	get_avg(get_times(Key)).

get_times(Key) ->
	[Match || [Match] <- ets:match(total_time, {{Key, '_'}, '$1'})].
get_timing_keys() ->
    Keys = [Key || {{Key, _PID}, _Value} <- get_timing_data()],
    UniqueKeys = sets:to_list(sets:from_list(Keys)),
	UniqueKeys.
get_timing_data() ->
    ets:tab2list(total_time).
print_timing_data() ->
	lists:foreach(fun(Key) ->
			Seconds = get_total(Key) / 1000000,
			?LOG_ERROR("~p: ~p", [Key, Seconds])
		end, get_timing_keys()).

reset() ->
	ets:delete_all_objects(total_time),
	ets:delete_all_objects(start_time).

initialize() ->
    ets:new(total_time, [set, named_table, public]),
	ets:new(start_time, [set, named_table, public]).

real_key(Key) ->
	{Key, self()}.


