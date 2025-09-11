-module(arweave_config_spec_environment).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, environment, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ environment => default() }}
	end.

fetch(Module, State) ->
	try
		Env = erlang:apply(Module, environment, []),
		check(Module, Env, State)
	catch
		_:R ->
			{error, R}
	end.

check(Module, undefined, State) ->
	{ok, State#{ environment => undefined }};
check(Module, Environment, State) when is_binary(Environment) ->
	{ok, State#{ environment => Environment }};
check(Module, Env, State) ->
	{error, #{
			reason => {invalid, Env},
			module => Module,
			callback => environment,
			state => State
		}
	}.
