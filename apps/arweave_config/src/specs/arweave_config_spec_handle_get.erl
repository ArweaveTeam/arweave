%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_spec_handle_get).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ handle_get := Get }, State) when is_function(Get, 1) ->
	{ok, State#{ get => Get }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_get, 1) of
		true ->
			{ok, State#{ get => fun Module:handle_get/1 }};
		false ->
			{ok, State}
	end.
