-module(arweave_config_spec_handle_set).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Module, State) ->
	case is_function_exported(Module, handle_set, 2) of
		true ->
			{ok, State#{ get => fun Module:handle_set/2 }};
		false ->
			{error, #{
					reason => undefined,
					function => handle_set,
					arity => 2,
					module => Module
				}
			}
	end.
