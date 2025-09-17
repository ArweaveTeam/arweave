-module(arweave_config_spec_handle_set).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Map, State) when is_map(Map) ->
	case is_map_key(handle_set, Map) of
		true ->
			Value = maps:get(handle_set, Map),
			{ok, State#{ set => Value }};
		false ->
			{error, #{
					reason => undefined,
					key => handle_set,
					arity => 3,
					map => Map
				}
			}
	end;
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_set, 3) of
		true ->
			{ok, State#{ set => fun Module:handle_set/3 }};
		false ->
			{error, #{
					reason => undefined,
					function => handle_set,
					arity => 3,
					module => Module
				}
			}
	end.
