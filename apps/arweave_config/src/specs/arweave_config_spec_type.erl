-module(arweave_config_spec_type).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, type, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ type => default() }}
	end.

fetch(Module, State) ->
	try erlang:apply(Module, type, []) of
		T when is_atom(T) ->
			{ok, State#{ type => T }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
