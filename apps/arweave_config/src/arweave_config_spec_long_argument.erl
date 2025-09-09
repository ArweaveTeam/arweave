-module(arweave_config_spec_long_argument).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, long_argument, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ long_argument => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, long_argument, []) of
		{ok, A} ->
			{ok, State#{ long_argument => A}};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
