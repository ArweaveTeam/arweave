-module(arweave_config_spec_legacy).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, legacy, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ legacy => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, legacy, []) of
		{ok, L} ->
			{ok, State#{ legacy => L }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.

