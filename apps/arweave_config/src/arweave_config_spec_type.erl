-module(arweave_config_spec_type).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, type, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ type => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, type, []) of
		{ok, T} ->
			{ok, State#{ type=> T }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
