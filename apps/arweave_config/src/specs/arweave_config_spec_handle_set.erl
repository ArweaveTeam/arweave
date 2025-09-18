%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_spec_handle_set).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ handle_set := Set }, State) when is_function(Set, 3) ->
	{ok, State#{ set => Set }};
init(Map, State) when is_map(Map) ->
	{error, #{
			reason => undefined,
			key => handle_set,
			arity => 3,
			map => Map
		}
	};
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
