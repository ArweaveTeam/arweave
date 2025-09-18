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
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_set, 3) of
		true ->
			{ok, State#{ set => fun Module:handle_set/3 }};
		false ->
			{ok, State}
	end.
