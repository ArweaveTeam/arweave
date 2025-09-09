-module(arweave_config_spec_check).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, check, 2) of
		true ->
			{ok, State#{ check => fun Module:check/2 }};
		false ->
			{ok, State#{ check => default() }}
	end.
