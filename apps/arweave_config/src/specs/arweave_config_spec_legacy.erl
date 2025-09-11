-module(arweave_config_spec_legacy).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Module, State) ->
	case is_function_exported(Module, legacy, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ legacy => default() }}
	end.

fetch(Module, State) ->
	try
		L = erlang:apply(Module, legacy, []),
	    	check(Module, L, State)
	catch
		_:R ->
			{error, R}
	end.

check(_Module, undefined, State) ->
	{ok, State#{ legacy => undefined }};
check(_Module, Legacy, State) when is_atom(Legacy) ->
	{ok, State#{ legacy => Legacy }};
check(Module, Legacy, State) when is_list(Legacy) ->
	check_list(Module, Legacy, Legacy, State);
check(Module, Legacy, State) ->
	{error, #{
			reason => {invalid, Legacy},
			module => Module,
			callback => legacy,
			state => State
		}
	}.

check_list(Module, [], Legacy, State) ->
	{ok, State#{ legacy => Legacy }};
check_list(Module, [Item|Rest], Legacy, State)
	when is_atom(Item); is_binary(Item) ->
		check_list(Module, Rest, Legacy, State);
check_list(Module, Rest, Legacy, State) ->
	{error, #{
			reason => {invalid, Rest},
			module => Module,
			callback => legacy,
			state => State
		}
	}.

