-module(arweave_config_spec_short_argument).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, short_argument, 0) of
		true ->
			?LOG_DEBUG("~p is defined", [{Module, short_argument, []}]),
			init2(Module, State);
		false ->
			?LOG_DEBUG("~p is undefined", [{Module, short_argument, []}]),
			{ok, State#{ short_argument => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, short_argument, []) of
		{ok, S} ->
			{ok, State#{ short_argument => S }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
