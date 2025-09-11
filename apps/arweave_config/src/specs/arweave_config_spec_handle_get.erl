-module(arweave_config_spec_handle_get).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Module, State) ->
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
