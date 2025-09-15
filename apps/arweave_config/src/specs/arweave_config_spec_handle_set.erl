-module(arweave_config_spec_handle_set).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Module, State) ->
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
