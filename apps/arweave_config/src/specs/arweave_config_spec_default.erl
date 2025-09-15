%%%===================================================================
%%% @doc "Default" specification.
%%% @end
%%%===================================================================
-module(arweave_config_spec_default).
-export([default/0, init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
default() -> undefined.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Module, State) ->
	case is_function_exported(Module, default, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ default => default() }}
	end.

fetch(Module, State) ->
	try Module:default() of
		Default ->
			NewState = State#{ default => Default },
			{ok, NewState};
		Elsewise ->
			{ok, State#{ default => default() }}
	catch
		_:Reason ->
			{ok, State#{ default => default() }}
	end.

