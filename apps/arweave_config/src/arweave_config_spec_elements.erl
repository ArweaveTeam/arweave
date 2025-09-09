-module(arweave_config_spec_elements).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> 0.

init(Module, State) ->
	case is_function_exported(Module, elements, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ elements => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, elements, []) of
		{ok, E} when is_integer(E), E >= 0 ->
			{ok, State#{ elements => E}};
		Elsewise ->
			{error, #{
					module => Module,
					callback => elements,
					reason => {bad_value, Elsewise}
				}
			}
	catch
		_:R ->
			{error, R}
	end.
