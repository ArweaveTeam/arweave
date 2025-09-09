-module(arweave_config_spec_short_description).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> <<>>.

init(Module, State) ->
	case is_function_exported(Module, short_description, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ short_description => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, short_description, []) of
		{ok, S} ->
			{ok, State#{ short_description => S }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
