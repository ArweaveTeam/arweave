%%% @doc Arweave application metrics.
%%%
-module(ar_metrics).

-export([register/0]).

-ifdef(AR_TEST).
-export([cleanup/0]).
-else.
-compile({nowarn_unused_function, [{cleanup, 0}]}).
-endif.

%%% Public interface.
%% @doc Declare Arweave metrics.
register() ->
	lists:foreach(
		fun({MetricType, Definition}) ->
			MetricType:new(Definition)
		end,
		ar_metrics_definitions:all_metrics()
	),
	ok.

cleanup() ->
	lists:foreach(
		fun({MetricType, Definition}) ->
			Name = proplists:get_value(name, Definition),
			MetricType:deregister(Name)
		end,
		ar_metrics_definitions:all_metrics()
	),
	ok.
