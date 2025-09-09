-module(arweave_config_spec_long_description).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> <<>>.

init(Module, State) ->
	case is_function_exported(Module, long_description, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ long_description => default() }}
	end.

init2(Module, State) ->
	try erlang:apply(Module, long_description, []) of
		{ok, S} ->
			{ok, State#{ long_description => S }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.

