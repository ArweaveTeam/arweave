%%% @doc The Arweave metrics application.
%%%
-module(arweave_metrics).

-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

-export([register/0, cleanup/0, get_status_class/1, record_rate_metric/4]).

%% Safe runtime metric helpers — see the "Safe metric helpers" section below.
-export([gauge_set/2, gauge_set/3, gauge_inc/1, gauge_inc/2, gauge_inc/3,
		gauge_dec/1, gauge_dec/2, gauge_dec/3, gauge_deregister/1,
		gauge_value/1, gauge_value/2,
		counter_inc/1, counter_inc/2, counter_inc/3,
		histogram_observe/2, histogram_observe/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("arweave/include/ar.hrl"). %% FIXME: this is a circular dependency

%% @doc Start the `arweave_metrics' application and its dependencies.
-spec start() -> ok | {error, term()}.
start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, Dependencies} ->
			?LOG_DEBUG("arweave_metrics started dependencies: ~p", [Dependencies]),
			ok;
		Else ->
			Else
	end.

%% @doc Stop the `arweave_metrics' application.
-spec stop() -> ok.
stop() ->
	application:stop(?MODULE).

%% @doc `application' callback. Declare the Arweave metrics, then bring
%% up the supervisor that runs the `arweave_metrics_cache' renderer, and
%% at last register the collectors.
start(_StartType, _StartArgs) ->
    arweave_metrics:register(),
    S = arweave_metrics_sup:start_link(),
    prometheus_registry:register_collector(prometheus_process_collector),
    prometheus_registry:register_collector(arweave_metrics_collector),
    S.

%% @doc `application' callback.
stop(_State) ->
	arweave_metrics:cleanup(),
	ok.

%%% Public interface.
%% @doc Declare Arweave metrics.
register() ->
	lists:foreach(
		fun({MetricType, Definition}) ->
			MetricType:new(Definition)
		end,
		arweave_metrics_definitions:all_metrics()
	),
	%% Release number never changes so just set it here.
	prometheus_gauge:set(arweave_release, ?RELEASE_NUMBER),
	ok.

record_rate_metric(StartTime, Bytes, Metric, Labels) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime =
		erlang:convert_time_unit(EndTime - StartTime,
								native,
								microsecond),
	%% bytes per second
	Rate =
		case ElapsedTime > 0 of
			true -> 1_000_000 * Bytes / ElapsedTime;
			false -> 0
		end,
	histogram_observe(Metric, Labels, Rate).


%% @doc Return the HTTP status class label for cowboy_requests_total and gun_requests_total
%% metrics.
get_status_class({ok, {{Status, _}, _, _, _, _}}) ->
	get_status_class(Status);
get_status_class({error, connection_closed}) ->
	"connection_closed";
get_status_class({error, connect_timeout}) ->
	"connect_timeout";
get_status_class({error, timeout}) ->
	"timeout";
get_status_class({error,{shutdown,timeout}}) ->
	"shutdown_timeout";
get_status_class({error, econnrefused}) ->
	"econnrefused";
get_status_class({error, {shutdown,econnrefused}}) ->
	"shutdown_econnrefused";
get_status_class({error, {shutdown,ehostunreach}}) ->
	"shutdown_ehostunreach";
get_status_class({error, {shutdown,normal}}) ->
	"shutdown_normal";
get_status_class({error, {closed,_}}) ->
	"closed";
get_status_class({error, noproc}) ->
	"noproc";
get_status_class({error, {down,_}}) ->
	"down";
get_status_class({error, {stream_error,_}}) ->
	"stream_error";
get_status_class({error, client_error}) ->
	"client_error";
get_status_class(Data) when is_integer(Data), Data > 0 ->
	integer_to_list(Data);
get_status_class(Data) when is_binary(Data) ->
	case catch binary_to_integer(Data) of
		{_, _} ->
			?LOG_DEBUG([{event, unknown_status}, {status, Data}]),
			"unknown";
		Status ->
			get_status_class(Status)
	end;
get_status_class(Data) when is_atom(Data) ->
	atom_to_list(Data);
get_status_class(Data) ->
	?LOG_DEBUG([{event, unknown_status}, {status, Data}]),
	"unknown".

%%% Safe metric helpers.
%%%
%%% Error-swallowing wrappers around the prometheus runtime API for all
%%% runtime metric writes/reads. The prometheus_* ETS tables can be
%%% transiently absent while the node (re)starts or stops, and an unguarded
%%% crash in a periodic gen_server write can cascade to
%%% reached_max_restart_intensity and halt the BEAM. Metric declarations
%%% (prometheus_*:new/declare) are NOT wrapped — a failed declaration is a bug.

gauge_set(Name, Value) ->
	try prometheus_gauge:set(Name, Value) catch _:_ -> ok end.
gauge_set(Name, Labels, Value) ->
	try prometheus_gauge:set(Name, Labels, Value) catch _:_ -> ok end.

gauge_inc(Name) ->
	try prometheus_gauge:inc(Name) catch _:_ -> ok end.
gauge_inc(Name, Value) ->
	try prometheus_gauge:inc(Name, Value) catch _:_ -> ok end.
gauge_inc(Name, Labels, Value) ->
	try prometheus_gauge:inc(Name, Labels, Value) catch _:_ -> ok end.

gauge_dec(Name) ->
	try prometheus_gauge:dec(Name) catch _:_ -> ok end.
gauge_dec(Name, Value) ->
	try prometheus_gauge:dec(Name, Value) catch _:_ -> ok end.
gauge_dec(Name, Labels, Value) ->
	try prometheus_gauge:dec(Name, Labels, Value) catch _:_ -> ok end.

gauge_deregister(Name) ->
	try prometheus_gauge:deregister(Name) catch _:_ -> ok end.

gauge_value(Name) ->
	try prometheus_gauge:value(Name) catch _:_ -> undefined end.
gauge_value(Name, Labels) ->
	try prometheus_gauge:value(Name, Labels) catch _:_ -> undefined end.

counter_inc(Name) ->
	try prometheus_counter:inc(Name) catch _:_ -> ok end.
counter_inc(Name, Value) ->
	try prometheus_counter:inc(Name, Value) catch _:_ -> ok end.
counter_inc(Name, Labels, Value) ->
	try prometheus_counter:inc(Name, Labels, Value) catch _:_ -> ok end.

histogram_observe(Name, Value) ->
	try prometheus_histogram:observe(Name, Value) catch _:_ -> ok end.
histogram_observe(Name, Labels, Value) ->
	try prometheus_histogram:observe(Name, Labels, Value) catch _:_ -> ok end.

cleanup() ->
	lists:foreach(
		fun({MetricType, Definition}) ->
			Name = proplists:get_value(name, Definition),
			MetricType:deregister(Name)
		end,
		arweave_metrics_definitions:all_metrics()
	),
	ok.
