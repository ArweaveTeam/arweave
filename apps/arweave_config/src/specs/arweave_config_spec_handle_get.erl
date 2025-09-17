-module(arweave_config_spec_handle_get).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Map, State) when is_map(Map) ->
	case is_map_key(handle_get, Map) of
		true ->
			Value = maps:get(handle_get, Map),
			{ok, State#{ get => Value }};
		false ->
			{error, #{
					reason => undefined,
					key => handle_get,
					arity => 1,
					map => Map
				}
			}
		end;
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_get, 1) of
		true ->
			{ok, State#{ get => fun Module:handle_get/1 }};
		false ->
			{error, #{
					reason => undefined,
					function => handle_get,
					arity => 1,
					module => Module
				}
			}
	end.
